//! `ants roster`: the ants being served, and whether their daemons still beat.

use std::time::Duration;

use ants_daemon::{bus, chains, roster};
use ants_proto::{CHAINS_BUCKET, LimitKind, ROSTER_BUCKET, ROSTER_TTL, RosterEntry, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::{DateTime, Utc};

use crate::avatars::{self, Faces};

/// How fresh an entry is, judged by its last heartbeat ([`roster::is_live`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Live,
    Stale,
}

impl State {
    pub fn of(entry: &RosterEntry, now: DateTime<Utc>) -> Self {
        if roster::is_live(entry, now) { Self::Live } else { Self::Stale }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Stale => "stale",
        }
    }
}

/// Time from `then` to `now`, zero when a clock is ahead.
pub(crate) fn since(then: DateTime<Utc>, now: DateTime<Utc>) -> Duration {
    (now - then).to_std().unwrap_or_default()
}

/// `duration` in its largest whole unit: `7s`, `3m`, `2h`, `5d`.
pub(crate) fn short(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86_400 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86_400),
    }
}

/// The ant's escalation chain, leader first, or `-` for none.
fn chain(entry: &RosterEntry) -> String {
    if entry.escalate_to.is_empty() {
        return "-".to_owned();
    }
    entry.escalate_to.iter().map(UserId::as_str).collect::<Vec<_>>().join(",")
}

/// What the ant is doing: `paused` while an admin or its config pauses it, `fallback` while a fallback answers for its
/// harness, `signed-out` while its harness is,
/// `failing` while its harness keeps failing to start, `rate-limited` or `out-of-quota` while a provider limit holds its
/// messages, else its activity.
pub(crate) fn activity(entry: &RosterEntry) -> &'static str {
    if entry.paused.is_some() {
        return "paused";
    }
    if entry.fallback.is_some() {
        return "fallback";
    }
    match (&entry.signed_out, &entry.failing, &entry.limited, entry.activity) {
        (Some(_), _, _, _) => "signed-out",
        (None, Some(_), _, _) => "failing",
        (None, None, Some(limited), _) => limited.kind.as_str(),
        (None, None, None, activity) => activity.as_str(),
    }
}

/// One line per ant paused (since when, by whom, and how to resume it), whose harness is signed out (since when, and how to sign it in), keeps failing to start (since
/// when, with its last error) or is held back by a provider limit (until when), or a fallback answers for.
pub fn held(entries: &[RosterEntry], now: DateTime<Utc>) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| paused_line(e, now).or_else(|| held_line(e, now)).or_else(|| fallback_line(e, now)))
        .collect()
}

fn paused_line(e: &RosterEntry, now: DateTime<Utc>) -> Option<String> {
    let paused = e.paused.as_ref()?;
    let by = paused.by.as_ref().map_or_else(|| " by its config".to_owned(), |by| format!(" by {by}"));
    Some(format!(
        "{}: paused for {}{by}; it takes no new prompt and messages to {} wait queued. Run `ants ant resume {}`.",
        e.ant,
        short(since(paused.since, now)),
        e.ant,
        e.ant
    ))
}

fn held_line(e: &RosterEntry, now: DateTime<Utc>) -> Option<String> {
    signed_out_line(e, now).or_else(|| failing_line(e, now)).or_else(|| limited_line(e, now))
}

/// What becomes of the messages to the ant of `e` meanwhile: `wait`, unless a fallback answers them.
fn meanwhile(e: &RosterEntry, now: DateTime<Utc>, wait: String) -> String {
    match &e.fallback {
        Some(fallback) => {
            format!(
                "fallback {} answers messages to {} (for {})",
                fallback.name,
                e.ant,
                short(since(fallback.since, now))
            )
        }
        None => wait,
    }
}

fn fallback_line(e: &RosterEntry, now: DateTime<Utc>) -> Option<String> {
    e.fallback.as_ref()?;
    Some(format!("{}: {} until harness {} answers again.", e.ant, meanwhile(e, now, String::new()), e.harness))
}

fn failing_line(e: &RosterEntry, now: DateTime<Utc>) -> Option<String> {
    let failing = e.failing.as_ref()?;
    let wait = format!("messages to {} wait, spending no attempts", e.ant);
    Some(format!(
        "{}: harness {} failing to start for {} ({}); {}. Run `ants harness check --harness {}` on {}.",
        e.ant,
        e.harness,
        short(since(failing.since, now)),
        failing.error,
        meanwhile(e, now, wait),
        e.harness,
        e.machine
    ))
}

fn limited_line(e: &RosterEntry, now: DateTime<Utc>) -> Option<String> {
    let limited = e.limited.as_ref()?;
    let what = match limited.kind {
        LimitKind::Quota => "out of quota",
        LimitKind::RateLimit => "rate limited",
        LimitKind::Unknown => "held back by a provider limit",
    };
    let wait = format!("messages to {} wait, spending no attempts", e.ant);
    Some(format!(
        "{}: harness {} {what} for {}; {}, until {} (in {}).",
        e.ant,
        e.harness,
        short(since(limited.since, now)),
        meanwhile(e, now, wait),
        limited.until.format("%H:%M:%SZ"),
        short(since(now, limited.until))
    ))
}

fn signed_out_line(e: &RosterEntry, now: DateTime<Utc>) -> Option<String> {
    let out = e.signed_out.as_ref()?;
    let how = match &out.sign_in {
        Some(command) => format!("Run `{command}` on {}", e.machine),
        None => format!("Sign {} in on {}", e.harness, e.machine),
    };
    Some(format!(
        "{}: harness {} signed out for {}; {}. {how} as the user the daemon runs as.",
        e.ant,
        e.harness,
        short(since(out.since, now)),
        meanwhile(e, now, format!("messages to {} wait", e.ant)),
    ))
}

/// One line per ant whose machine is low on memory: since when, how much is available and how many harnesses run.
pub fn memory_low(entries: &[RosterEntry], now: DateTime<Utc>) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| {
            let low = e.memory_low.as_ref()?;
            let harnesses = if low.max_harnesses == 1 { "harness runs" } else { "harnesses run" };
            Some(format!(
                "{}: memory low on {} for {} ({} available); idle harnesses stop at once and at most {} {harnesses} \
                 until it recovers.",
                e.ant,
                e.machine,
                short(since(low.since, now)),
                ants_daemon::storage::show_size(low.available),
                low.max_harnesses
            ))
        })
        .collect()
}

/// One line per ant whose machine's config sets some of its shared settings otherwise ([`RosterEntry::drift`]), which
/// the daemon ignores for the shared ones.
pub fn drifted(entries: &[RosterEntry]) -> Vec<String> {
    entries
        .iter()
        .filter(|e| !e.drift.is_empty())
        .map(|e| {
            format!(
                "{}: drifted on {}: its config sets {} otherwise than the ant's shared settings, which serve. Run \
                 `ants doctor` for every machine.",
                e.ant,
                e.machine,
                e.drift.join(", ")
            )
        })
        .collect()
}

/// Why no daemon serves `ant` now, given its roster entry, or None when one does: no entry (with no escalation chain
/// kept for it either, the name may be wrong), a stale entry, a pause, or a harness signed out or held back by a
/// provider limit with no fallback answering for it.
pub fn unserved(ant: &UserId, entry: Option<&RosterEntry>, chain_kept: bool, now: DateTime<Utc>) -> Option<String> {
    let Some(entry) = entry else {
        let hint = if chain_kept { "" } else { " (`ants roster` lists the ants served: check the name)" };
        return Some(format!("{ant} is not in the roster: no daemon serves {ant} now{hint}"));
    };
    if State::of(entry, now) == State::Stale {
        return Some(format!(
            "{ant} is stale in the roster: the daemon on {} last beat {} ago and may not serve {ant} now",
            entry.machine,
            short(since(entry.heartbeat_at, now))
        ));
    }
    if let Some(paused) = paused_line(entry, now) {
        return Some(paused);
    }
    if entry.fallback.is_some() {
        return None;
    }
    held_line(entry, now)
}

/// How long [`unserved_warnings`] reads before giving up, so that an unreachable hub does not hold up a command.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

/// A warning for each of `ants` no daemon serves now ([`unserved`]), read from the roster and the escalation chains
/// kept. None when the roster cannot be read: a lookup is advisory, never a reason to fail.
pub async fn unserved_warnings(js: &jetstream::Context, ants: &[&UserId]) -> Vec<String> {
    if ants.is_empty() {
        return Vec::new();
    }
    match tokio::time::timeout(LOOKUP_TIMEOUT, read_unserved(js, ants)).await {
        Ok(Ok(warnings)) => warnings,
        Ok(Err(e)) => {
            tracing::debug!("not checking the roster: {e:#}");
            Vec::new()
        }
        Err(_) => {
            tracing::debug!("not checking the roster: no answer within {LOOKUP_TIMEOUT:?}");
            Vec::new()
        }
    }
}

async fn read_unserved(js: &jetstream::Context, ants: &[&UserId]) -> Result<Vec<String>> {
    let kv = js.get_key_value(ROSTER_BUCKET).await.context("opening the roster")?;
    let kept = js.get_key_value(CHAINS_BUCKET).await.ok();
    let now = Utc::now();
    let mut warnings = Vec::new();
    for ant in ants {
        let entry = match kv.get(ant.as_str()).await.with_context(|| format!("reading {ant} in the roster"))? {
            Some(value) => {
                let entry: RosterEntry = serde_json::from_slice(&value).with_context(|| format!("parsing {ant}"))?;
                entry.check_key(ant.as_str())?;
                Some(entry)
            }
            None => None,
        };
        // Only chains read and found lacking hint at a misspelt name.
        let chain_kept = match (&entry, &kept) {
            (None, Some(kept)) => chains::read(kept, ant).await.map_or(true, |chain| chain.is_some()),
            _ => true,
        };
        warnings.extend(unserved(ant, entry.as_ref(), chain_kept, now));
    }
    Ok(warnings)
}

/// The roster as a table: one row per ant under a header, columns padded to line up. `-` stands for what an entry
/// leaves out.
pub fn table(entries: &[RosterEntry], now: DateTime<Utc>) -> String {
    let header = [
        "ANT",
        "STATE",
        "ACTIVITY",
        "HARNESSES",
        "HARNESS",
        "AGENT",
        "MACHINE",
        "VERSION",
        "PERMS",
        "HEARTBEAT",
        "UP",
        "CAPABILITIES",
        "ESCALATE_TO",
        "WORKDIR",
    ]
    .map(str::to_owned);
    let rows: Vec<[String; 14]> = std::iter::once(header)
        .chain(entries.iter().map(|e| {
            [
                e.ant.to_string(),
                State::of(e, now).as_str().to_owned(),
                activity(e).to_owned(),
                e.harnesses.to_string(),
                e.harness.clone(),
                e.agent.as_ref().map_or_else(|| "-".to_owned(), |a| format!("{}@{}", a.name, a.version)),
                e.machine.clone(),
                e.version.clone(),
                e.perms.to_string(),
                format!("{} ago", short(since(e.heartbeat_at, now))),
                short(since(e.started_at, now)),
                if e.capabilities.is_empty() { "-".to_owned() } else { e.capabilities.join(",") },
                chain(e),
                e.workdir.clone().unwrap_or_else(|| "-".to_owned()),
            ]
        }))
        .collect();
    let mut widths = [0; 14];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}

/// An entry as one JSON object, with its `state` added, and from the ant's identity record in `faces` when it has one,
/// its `born` and `prompts`, and its `avatar` (the face) and `color`.
pub fn json(entry: &RosterEntry, now: DateTime<Utc>, faces: &Faces) -> Result<String> {
    let mut value = serde_json::to_value(entry)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("state".to_owned(), State::of(entry, now).as_str().into());
        let identity = faces.identity(&entry.ant);
        let avatar = faces.avatar(&entry.ant);
        object.insert("born".to_owned(), serde_json::to_value(identity.map(|i| i.born))?);
        object.insert("avatar".to_owned(), avatar.face().into());
        object.insert("color".to_owned(), avatar.hue_name().into());
        object.insert("prompts".to_owned(), identity.map(|i| i.prompts).into());
    }
    Ok(value.to_string())
}

/// `table` with each ant's face before its row, in its colour, the header indented to line up.
pub fn faced(table: &str, entries: &[RosterEntry], faces: &Faces) -> String {
    let mut lines = table.lines();
    let header = lines.next().map(|h| format!("{:width$} {h}", "", width = ants_proto::FACE_WIDTH));
    let rows = lines.zip(entries).map(|(line, e)| format!("{} {line}", faces.face(&e.ant)));
    header.into_iter().chain(rows).collect::<Vec<_>>().join("\n")
}

/// The entries in the roster, read with a human's credentials.
pub async fn list(js: &jetstream::Context) -> Result<Vec<RosterEntry>> {
    let kv = bus::ensure_roster(js, ROSTER_TTL).await?;
    roster::list_entries(&kv).await
}

/// Print the roster: a table, or with `json` one entry per line.
pub async fn show(nats: &crate::Nats<'_>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-roster").await?;
    let entries = list(&js).await?;
    let faces = Faces::stdout(avatars::read_or_none(&js).await);
    let now = Utc::now();
    if json {
        for entry in &entries {
            println!("{}", self::json(entry, now, &faces)?);
        }
    } else if entries.is_empty() {
        println!("no ants registered");
    } else {
        println!("{}", faced(&table(&entries, now), &entries, &faces));
        if let Some(note) = crate::versions::roster_note(&entries, now) {
            println!("{note}");
        }
        if let Some(note) = crate::versions::harness_note(&entries, now) {
            println!("{note}");
        }
        let collisions = faces.collisions(entries.iter().map(|e| &e.ant));
        let notes = [held(&entries, now), memory_low(&entries, now), drifted(&entries), collisions].concat();
        if !notes.is_empty() {
            println!("\n{}", notes.join("\n"));
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use ants_daemon::roster::Registration;
    use ants_proto::{Activity, HarnessLimit, PermissionsVersion, ROSTER_HEARTBEAT};

    use super::*;
    use crate::auth::tests::Server;

    pub(crate) fn entry(ant: &str, started: i64, beat: i64, now: DateTime<Utc>) -> RosterEntry {
        RosterEntry {
            ant: ant.parse().unwrap(),
            machine: "m1".to_owned(),
            instance: "run-1".to_owned(),
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: vec!["rust".to_owned(), "review".to_owned()],
            whoami: None,
            workdir: Some("/src/app".to_owned()),
            started_at: now - chrono::Duration::seconds(started),
            heartbeat_at: now - chrono::Duration::seconds(beat),
            activity: ants_proto::Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: Vec::new(),
            version: ants_proto::ANTS_VERSION.to_owned(),
            perms: ants_proto::PERMISSIONS_VERSION,
            signed_out: None,
            limited: None,
            failing: None,
            fallback: None,
            memory_low: None,
            model: None,
            paused: None,
            drift: Vec::new(),
        }
    }

    #[test]
    fn a_drifted_ant_is_noted_with_its_machine_and_fields() {
        let now = Utc::now();
        let mut drifted_entry = entry("dev-a", 60, 2, now);
        drifted_entry.drift = vec!["model".to_owned(), "whoami".to_owned()];
        let notes = drifted(&[entry("dev-b", 60, 2, now), drifted_entry]);
        assert_eq!(notes.len(), 1);
        assert!(notes[0].starts_with("dev-a: drifted on m1: its config sets model, whoami otherwise"), "{notes:?}");
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn rows_show_state_and_ages() {
        let now = Utc::now();
        let mut quiet = entry("dev-b", 7300, 25, now);
        quiet.capabilities.clear();
        quiet.workdir = None;
        let mut busy = entry("dev-a", 190, 4, now);
        (busy.activity, busy.handling, busy.harnesses) = (Activity::Working, 1, 2);
        busy.escalate_to = vec!["lead".parse().unwrap(), "dev-b".parse().unwrap()];
        (busy.version, busy.perms) = ("0.2.0".to_owned(), PermissionsVersion(2));
        busy.agent = Some(ants_proto::HarnessAgent { name: "copilot".to_owned(), version: "1.0.3".to_owned() });
        let older = RosterEntry { version: "0.1.0".to_owned(), ..entry("dev-c", 60, 4, now) };
        assert_eq!(
            crate::versions::roster_note(&[busy.clone(), older, quiet.clone()], now).as_deref(),
            Some("mixed ants versions: 0.2.0 (1), 0.1.0 (1); `ants doctor` tells whether credentials may be renewed")
        );
        assert_eq!(crate::versions::roster_note(&[busy.clone(), quiet.clone()], now), None, "a stale one runs nothing");
        let table = table(&[busy, quiet], now);
        let rows: Vec<Vec<&str>> = table.lines().map(|l| l.split_whitespace().collect()).collect();
        assert_eq!(
            rows[0],
            [
                "ANT",
                "STATE",
                "ACTIVITY",
                "HARNESSES",
                "HARNESS",
                "AGENT",
                "MACHINE",
                "VERSION",
                "PERMS",
                "HEARTBEAT",
                "UP",
                "CAPABILITIES",
                "ESCALATE_TO",
                "WORKDIR"
            ]
        );
        assert_eq!(
            rows[1],
            [
                "dev-a",
                "live",
                "working",
                "2",
                "copilot",
                "copilot@1.0.3",
                "m1",
                "0.2.0",
                "2",
                "4s",
                "ago",
                "3m",
                "rust,review",
                "lead,dev-b",
                "/src/app"
            ]
        );
        let perms = ants_proto::PERMISSIONS_VERSION.to_string();
        assert_eq!(
            rows[2],
            [
                "dev-b",
                "stale",
                "idle",
                "0",
                "copilot",
                "-",
                "m1",
                ants_proto::ANTS_VERSION,
                &perms,
                "25s",
                "ago",
                "2h",
                "-",
                "-",
                "-"
            ]
        );
        let columns: Vec<Option<usize>> = table.lines().map(|l| l.find("copilot").or(l.find("HARNESS "))).collect();
        assert!(columns.iter().all(|c| c.is_some() && *c == columns[0]), "{table}");

        let mut out = entry("dev-c", 600, 2, now);
        let since = now - chrono::Duration::seconds(300);
        out.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: Some("copilot login".to_owned()) });
        let mut unnamed = entry("dev-d", 600, 2, now);
        unnamed.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: None });
        let mut limited = entry("dev-f", 600, 2, now);
        let until = now + chrono::Duration::seconds(120);
        limited.limited = Some(HarnessLimit { kind: LimitKind::RateLimit, since: now, until });
        let mut quota = entry("dev-g", 600, 2, now);
        quota.limited = Some(HarnessLimit { kind: LimitKind::Quota, since, until });
        let mut failing = entry("dev-h", 600, 2, now);
        failing.failing = Some(ants_proto::HarnessFailing { since, error: "copilot: not found".to_owned() });
        let entries = [out.clone(), unnamed, entry("dev-e", 600, 2, now), limited, quota, failing];
        let table = super::table(&entries, now);
        let activities: Vec<&str> = table.lines().map(|l| l.split_whitespace().nth(2).unwrap()).collect();
        assert_eq!(
            activities,
            ["ACTIVITY", "signed-out", "signed-out", "idle", "rate-limited", "out-of-quota", "failing"]
        );
        let at = until.format("%H:%M:%SZ");
        assert_eq!(
            held(&entries, now),
            [
                "dev-c: harness copilot signed out for 5m; messages to dev-c wait. Run `copilot login` on m1 as the \
                 user the daemon runs as."
                    .to_owned(),
                "dev-d: harness copilot signed out for 5m; messages to dev-d wait. Sign copilot in on m1 as the user \
                 the daemon runs as."
                    .to_owned(),
                format!(
                    "dev-f: harness copilot rate limited for 0s; messages to dev-f wait, spending no attempts, until \
                     {at} (in 2m)."
                ),
                format!(
                    "dev-g: harness copilot out of quota for 5m; messages to dev-g wait, spending no attempts, until \
                     {at} (in 2m)."
                ),
                "dev-h: harness copilot failing to start for 5m (copilot: not found); messages to dev-h wait, \
                 spending no attempts. Run `ants harness check --harness copilot` on m1."
                    .to_owned(),
            ]
        );
        let json: serde_json::Value = serde_json::from_str(&super::json(&out, now, &Faces::plain()).unwrap()).unwrap();
        assert_eq!(json["signed_out"]["sign_in"].as_str(), Some("copilot login"));
        let mut low = entry("dev-i", 600, 2, now);
        low.memory_low = Some(ants_proto::MemoryLow { since, available: 768 << 20, max_harnesses: 1 });
        assert_eq!(held(std::slice::from_ref(&low), now), Vec::<String>::new(), "messages do not wait");
        assert_eq!(
            memory_low(&[low, out.clone()], now),
            ["dev-i: memory low on m1 for 5m (768.0 MB available); idle harnesses stop at once and at most 1 harness \
              runs until it recovers."]
        );

        let fallback = Some(ants_proto::ServingFallback { name: "claude:opus".to_owned(), since });
        let mut covered = entry("dev-i", 600, 2, now);
        (covered.signed_out, covered.fallback) = (out.signed_out, fallback.clone());
        let mut back = entry("dev-j", 600, 2, now);
        back.fallback = fallback;
        let entries = [covered, back];
        assert_eq!(activity(&entries[0]), "fallback");
        assert_eq!(
            held(&entries, now),
            [
                "dev-i: harness copilot signed out for 5m; fallback claude:opus answers messages to dev-i (for 5m). \
                 Run `copilot login` on m1 as the user the daemon runs as.",
                "dev-j: fallback claude:opus answers messages to dev-j (for 5m) until harness copilot answers again.",
            ]
        );

        let mut led = entry("dev-a", 0, 30, now);
        led.escalate_to = vec!["lead".parse().unwrap()];
        let json: serde_json::Value = serde_json::from_str(&super::json(&led, now, &Faces::plain()).unwrap()).unwrap();
        assert_eq!((json["ant"].as_str(), json["state"].as_str()), (Some("dev-a"), Some("stale")));
        assert_eq!(json["escalate_to"], serde_json::json!(["lead"]));
    }

    #[test]
    fn each_ant_s_face_comes_before_its_row_and_in_its_json() {
        let now = Utc::now();
        let entries = [entry("dev-a", 60, 2, now), entry("dev-b", 60, 2, now)];
        let born = now - chrono::Duration::days(1);
        let mut record = ants_proto::AntIdentity::new("dev-b".parse().unwrap(), "b-id".to_owned(), born);
        record.prompts = 12;
        let identities = avatars::Identities::from([(record.ant.clone(), record)]);
        let faces = Faces::new(identities, avatars::Depth::Plain, ants_proto::Theme::Dark);
        let table = table(&entries, now);
        let lines: Vec<String> = faced(&table, &entries, &faces).lines().map(str::to_owned).collect();
        let (a, b) = (ants_proto::Avatar::derive("dev-a"), ants_proto::Avatar::derive("b-id"));
        assert!(lines[0].starts_with("      ANT "), "{lines:?}");
        assert!(lines[1].starts_with(&format!("{} dev-a ", a.face())), "{lines:?}");
        assert!(lines[2].starts_with(&format!("{} dev-b ", b.face())), "{lines:?}");
        let shown = |e| serde_json::from_str::<serde_json::Value>(&json(e, now, &faces).unwrap()).unwrap();
        let born = shown(&entries[1]);
        assert_eq!((born["avatar"].as_str(), born["color"].as_str()), (Some(&*b.face()), Some(b.hue_name())));
        assert_eq!((born["prompts"].as_u64(), born["born"].is_string()), (Some(12), true));
        let unborn = shown(&entries[0]);
        assert_eq!((unborn["avatar"].as_str(), unborn["born"].is_null()), (Some(&*a.face()), true), "from the name");
    }

    #[test]
    fn unserved_ants_say_why() {
        let now = Utc::now();
        let dev_a: UserId = "dev-a".parse().unwrap();
        assert_eq!(unserved(&dev_a, Some(&entry("dev-a", 60, 2, now)), false, now), None, "a live ant is served");
        assert_eq!(
            unserved(&dev_a, None, true, now).unwrap(),
            "dev-a is not in the roster: no daemon serves dev-a now"
        );
        assert_eq!(
            unserved(&dev_a, None, false, now).unwrap(),
            "dev-a is not in the roster: no daemon serves dev-a now (`ants roster` lists the ants served: check the \
             name)"
        );
        assert_eq!(
            unserved(&dev_a, Some(&entry("dev-a", 600, 300, now)), true, now).unwrap(),
            "dev-a is stale in the roster: the daemon on m1 last beat 5m ago and may not serve dev-a now"
        );
        let mut out = entry("dev-a", 600, 2, now);
        out.signed_out = Some(ants_proto::SignInNeeded { since: now, sign_in: None });
        assert!(unserved(&dev_a, Some(&out), true, now).unwrap().starts_with("dev-a: harness copilot signed out"));
        let mut limited = entry("dev-a", 600, 2, now);
        limited.limited = Some(HarnessLimit { kind: LimitKind::Quota, since: now, until: now });
        assert!(
            unserved(&dev_a, Some(&limited), true, now).unwrap().starts_with("dev-a: harness copilot out of quota")
        );
        limited.fallback = Some(ants_proto::ServingFallback { name: "claude".to_owned(), since: now });
        assert_eq!(unserved(&dev_a, Some(&limited), true, now), None, "a fallback answers");
        limited.paused = Some(ants_proto::Paused { since: now - chrono::Duration::minutes(5), by: None });
        let paused = "dev-a: paused for 5m by its config; it takes no new prompt and messages to dev-a wait queued. \
                      Run `ants ant resume dev-a`.";
        assert_eq!(unserved(&dev_a, Some(&limited), true, now).unwrap(), paused, "a fallback answers no paused ant");
        assert_eq!(activity(&limited), "paused");
        limited.paused.as_mut().unwrap().by = Some("alice".parse().unwrap());
        assert!(held(&[limited], now)[0].starts_with("dev-a: paused for 5m by alice;"));
    }

    /// Read with the human's credentials: an ant registered, one with only its escalation chain kept, one unknown.
    #[tokio::test]
    async fn the_human_learns_which_ants_no_daemon_serves() {
        let Some(server) = Server::start() else { return };
        let (_, human) = server.connect(&UserId::human()).await;
        let ids: Vec<UserId> = ["dev-a", "dev-b", "dev-c"].map(|a| a.parse().unwrap()).into();
        let ants: Vec<&UserId> = ids.iter().collect();
        assert!(unserved_warnings(&human, &ants).await.is_empty(), "no roster to read, no warning");

        bus::ensure_roster(&human, ROSTER_TTL).await.unwrap();
        bus::ensure_chains(&human).await.unwrap();
        let (_, js) = server.connect(&ids[0]).await;
        let kv = bus::ensure_roster(&js, ROSTER_TTL).await.unwrap();
        let _registration = Registration::start(kv, entry("dev-a", 60, 0, Utc::now()), ROSTER_HEARTBEAT).await.unwrap();
        let (_, js) = server.connect(&ids[1]).await;
        ants_daemon::chains::publish(&js, &ids[1], std::slice::from_ref(&ids[0])).await.unwrap();
        assert_eq!(
            unserved_warnings(&human, &ants).await,
            [
                "dev-b is not in the roster: no daemon serves dev-b now",
                "dev-c is not in the roster: no daemon serves dev-c now (`ants roster` lists the ants served: check \
                 the name)"
            ]
        );
    }

    /// With the human's credentials, so it also checks that they read the roster.
    #[tokio::test]
    async fn the_human_lists_registered_ants() {
        let Some(server) = Server::start() else { return };
        let (_, human) = server.connect(&UserId::human()).await;
        assert!(list(&human).await.unwrap().is_empty(), "an empty roster, created on first use");

        let mut registrations = Vec::new();
        let now = Utc::now();
        for ant in ["dev-b", "dev-a"] {
            let (_, js) = server.connect(&ant.parse().unwrap()).await;
            let kv = bus::ensure_roster(&js, ROSTER_TTL).await.unwrap();
            let entry = entry(ant, 60, 0, now);
            registrations.push(Registration::start(kv, entry, ROSTER_HEARTBEAT).await.unwrap());
        }
        let listed = list(&human).await.unwrap();
        let ants: Vec<&str> = listed.iter().map(|e| e.ant.as_str()).collect();
        assert_eq!(ants, ["dev-a", "dev-b"]);
        assert_eq!(listed[0], entry("dev-a", 60, 0, now));

        registrations.remove(0).deregister().await;
        let listed = list(&human).await.unwrap();
        assert_eq!(listed.iter().map(|e| e.ant.as_str()).collect::<Vec<_>>(), ["dev-a"]);
    }
}
