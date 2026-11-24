//! `ants colony`: the ants being served, and whether their daemons still beat.

use ants_daemon::bus::relayed::GuardedKv as _;
use std::time::Duration;

use ants_daemon::records::chains;
use ants_daemon::{bus, colony};
use ants_proto::{CHAINS_BUCKET, COLONY_BUCKET, COLONY_TTL, ColonyEntry, LimitKind, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::{DateTime, Utc};

use crate::ants::avatars::{self, Faces};
use crate::text::{ago, short, since};

/// How fresh an entry is, judged by its last heartbeat ([`colony::is_live`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Live,
    Stale,
}

impl State {
    pub fn of(entry: &ColonyEntry, now: DateTime<Utc>) -> Self {
        if colony::is_live(entry, now) { Self::Live } else { Self::Stale }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Stale => "stale",
        }
    }
}

/// The ant's escalation chain, leader first, or `-` for none.
fn chain(entry: &ColonyEntry) -> String {
    if entry.escalate_to.is_empty() {
        return "-".to_owned();
    }
    entry.escalate_to.iter().map(UserId::as_str).collect::<Vec<_>>().join(",")
}

/// What the ant is doing: `unserved` while its daemon cannot serve it, `paused` while an admin or its config pauses it,
/// `fallback` while a fallback answers for its harness, `signed-out` while its harness is, `failing` while its harness
/// keeps failing to start, `rate-limited` or `out-of-quota` while a provider limit holds its
/// messages, else its activity.
pub(crate) fn activity(entry: &ColonyEntry) -> &'static str {
    if entry.unserved.is_some() {
        return "unserved";
    }
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

/// One line per ant its daemon cannot serve (why, since when, and when it is tried again), paused (since when, by
/// whom, and how to resume it), whose harness is signed out (since when, and how to sign it in), keeps failing to start
/// (since when, with its last error) or is held back by a provider limit (until when), or a fallback answers for.
pub fn held(entries: &[ColonyEntry], now: DateTime<Utc>) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| {
            unserved_line(e, now)
                .or_else(|| paused_line(e, now))
                .or_else(|| held_line(e, now))
                .or_else(|| fallback_line(e, now))
        })
        .collect()
}

fn unserved_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
    let unserved = e.unserved.as_ref()?;
    let retry = match (unserved.retry_at - now).to_std() {
        Ok(wait) if !wait.is_zero() => format!("tried again in {}", short(wait)),
        _ => "tried again now".to_owned(),
    };
    Some(format!(
        "{}: not served on {} for {} ({}); messages to {} wait queued, {retry}.",
        e.ant,
        e.machine,
        short(since(unserved.since, now)),
        unserved.error,
        e.ant
    ))
}

fn paused_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
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

fn held_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
    signed_out_line(e, now).or_else(|| failing_line(e, now)).or_else(|| limited_line(e, now))
}

/// What becomes of the messages to the ant of `e` meanwhile: `wait`, unless a fallback answers them.
fn meanwhile(e: &ColonyEntry, now: DateTime<Utc>, wait: String) -> String {
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

fn fallback_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
    e.fallback.as_ref()?;
    Some(format!("{}: {} until harness {} answers again.", e.ant, meanwhile(e, now, String::new()), e.harness))
}

fn failing_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
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

fn limited_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
    let limited = e.limited.as_ref()?;
    let what = match limited.kind {
        LimitKind::Quota => "out of quota",
        LimitKind::RateLimit => "rate limited",
        LimitKind::OpenFiles => "out of open files on its machine",
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

fn signed_out_line(e: &ColonyEntry, now: DateTime<Utc>) -> Option<String> {
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
pub fn memory_low(entries: &[ColonyEntry], now: DateTime<Utc>) -> Vec<String> {
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
                ants_daemon::monitoring::storage::show_size(low.available),
                low.max_harnesses
            ))
        })
        .collect()
}

/// One line per ant whose sense, its provider, keeps none of its data ([`ColonyEntry::sense_unkept`]): it is served
/// without memories, tasks or brief.
pub fn sense_unkept(entries: &[ColonyEntry]) -> Vec<String> {
    entries
        .iter()
        .filter_map(|e| {
            let why = e.sense_unkept.as_deref()?;
            Some(format!(
                "{}: served on {} without memories, tasks or brief, sense keeping none of its data: {why}.",
                e.ant, e.machine
            ))
        })
        .collect()
}

/// One line per ant whose machine's config sets some of its shared settings otherwise ([`ColonyEntry::drift`]), which
/// the daemon ignores for the shared ones.
fn drifted(entries: &[ColonyEntry]) -> Vec<String> {
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

/// What a colony without an entry for an ant says of it ([`unserved`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Absent {
    /// The colony lists other ants, and an escalation chain is kept for this one.
    Known,
    /// The colony lists other ants, but no escalation chain is kept for this one either: the name may be wrong.
    Unknown,
    /// The colony lists no ant at all: no daemon beats, or the hub's memory store, full, refuses every heartbeat.
    Empty,
}

/// A note for a colony listing no ant at all ([`Absent::Empty`]).
const EMPTY_COLONY: &str = "no daemon beats, or the hub's JetStream memory store is full and refuses their \
                                heartbeats (`ants doctor` shows its usage)";

/// Why no daemon serves `ant` now, given its colony entry, or None when one does: no entry (with no escalation chain
/// kept for it either, the name may be wrong; with no ant in the colony at all, the store may be full: `absent`), a
/// stale entry, a daemon that cannot serve it, a pause, or a harness signed out or held back by a provider limit with no
/// fallback answering for it.
pub fn unserved(ant: &UserId, entry: Option<&ColonyEntry>, absent: Absent, now: DateTime<Utc>) -> Option<String> {
    let Some(entry) = entry else {
        return Some(match absent {
            Absent::Known => format!("{ant} is not in the colony: no daemon serves {ant} now"),
            Absent::Unknown => format!(
                "{ant} is not in the colony: no daemon serves {ant} now (`ants colony` lists the ants served: check the \
                 name)"
            ),
            Absent::Empty => format!("{ant} is not in the colony, which lists no ant: {EMPTY_COLONY}"),
        });
    };
    if State::of(entry, now) == State::Stale {
        return Some(format!(
            "{ant} is stale in the colony: the daemon on {} last beat {} ago and may not serve {ant} now",
            entry.machine,
            short(since(entry.heartbeat_at, now))
        ));
    }
    if let Some(line) = unserved_line(entry, now).or_else(|| paused_line(entry, now)) {
        return Some(line);
    }
    if entry.fallback.is_some() {
        return None;
    }
    held_line(entry, now)
}

/// How long [`unserved_warnings`] reads before giving up, so that an unreachable hub does not hold up a command.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);

/// A warning for each of `ants` no daemon serves now ([`unserved`]), read from the colony and the escalation chains
/// kept. None when the colony cannot be read: a lookup is advisory, never a reason to fail.
pub async fn unserved_warnings(js: &jetstream::Context, ants: &[&UserId]) -> Vec<String> {
    if ants.is_empty() {
        return Vec::new();
    }
    match tokio::time::timeout(LOOKUP_TIMEOUT, read_unserved(js, ants)).await {
        Ok(Ok(warnings)) => warnings,
        Ok(Err(e)) => {
            tracing::debug!("not checking the colony: {e:#}");
            Vec::new()
        }
        Err(_) => {
            tracing::debug!("not checking the colony: no answer within {LOOKUP_TIMEOUT:?}");
            Vec::new()
        }
    }
}

async fn read_unserved(js: &jetstream::Context, ants: &[&UserId]) -> Result<Vec<String>> {
    let kv = js.get_key_value(COLONY_BUCKET).await.context("opening the colony")?;
    let kept =
        js.get_key_value(CHAINS_BUCKET).await.inspect_err(|e| tracing::debug!("{CHAINS_BUCKET} unread: {e}")).ok();
    let now = Utc::now();
    let mut warnings = Vec::new();
    let mut empty = None;
    for ant in ants {
        let entry = match kv.guarded_get(ant.as_str()).await.with_context(|| format!("reading {ant} in the colony"))? {
            Some(value) => {
                let entry: ColonyEntry = serde_json::from_slice(&value).with_context(|| format!("parsing {ant}"))?;
                entry.check_key(ant.as_str())?;
                Some(entry)
            }
            None => None,
        };
        let absent = if entry.is_some() {
            Absent::Known
        } else if *empty.get_or_insert(kv.status().await.is_ok_and(|status| status.values() == 0)) {
            Absent::Empty
        } else {
            // Only chains read and found lacking hint at a misspelt name.
            match &kept {
                Some(kept) if chains::read(kept, ant).await.is_ok_and(|chain| chain.is_none()) => Absent::Unknown,
                _ => Absent::Known,
            }
        };
        warnings.extend(unserved(ant, entry.as_ref(), absent, now));
    }
    Ok(warnings)
}

/// The most characters of a profile the colony table shows.
const PROFILE_WIDTH: usize = 40;

/// The colony as a table: one row per ant under a header, columns padded to line up. `-` stands for what an entry
/// leaves out. The PROFILE column, last, shows the start of each ant's profile, its own words, on one line.
pub fn table(entries: &[ColonyEntry], now: DateTime<Utc>) -> String {
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
        "PROFILE",
    ]
    .map(str::to_owned);
    let rows: Vec<[String; 15]> = std::iter::once(header)
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
                ago(e.heartbeat_at, now),
                short(since(e.started_at, now)),
                if e.capabilities.is_empty() { "-".to_owned() } else { e.capabilities.join(",") },
                chain(e),
                e.workdir.clone().unwrap_or_else(|| "-".to_owned()),
                Some(crate::text::one_line(e.profile.as_deref().unwrap_or_default(), PROFILE_WIDTH))
                    .filter(|profile| !profile.is_empty())
                    .unwrap_or_else(|| "-".to_owned()),
            ]
        }))
        .collect();
    crate::text::columns(&rows)
}

/// An entry as one JSON object, with its `state` added, and from the ant's identity record in `faces` when it has one,
/// its `born`, `prompts` and `sense` (its sense id), and its `avatar` (the face), `color` and `seed`: where the face's
/// seed comes from, `avatar` (set by a human), `sense`, `id` (the record's) or `name` (no record).
pub fn json(entry: &ColonyEntry, now: DateTime<Utc>, faces: &Faces) -> Result<String> {
    let mut value = serde_json::to_value(entry)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("state".to_owned(), State::of(entry, now).as_str().into());
        let identity = faces.identity(&entry.ant);
        let avatar = faces.avatar(&entry.ant);
        object.insert("born".to_owned(), serde_json::to_value(identity.map(|i| i.born))?);
        object.insert("avatar".to_owned(), avatar.face().into());
        object.insert("color".to_owned(), avatar.hue_name().into());
        object.insert("prompts".to_owned(), identity.map(|i| i.prompts).into());
        object.insert(
            "sense".to_owned(),
            identity.and_then(|i| i.sense.as_ref()).map(ants_proto::SenseId::as_str).into(),
        );
        object.insert("seed".to_owned(), identity.map_or("name", |i| i.seed_source().as_str()).into());
    }
    Ok(value.to_string())
}

/// `table` with each ant's face before its row, in its colour, the header indented to line up.
fn faced(table: &str, entries: &[ColonyEntry], faces: &Faces) -> String {
    let mut lines = table.lines();
    let header = lines.next().map(|h| format!("{:width$} {h}", "", width = ants_proto::FACE_WIDTH));
    let rows = lines.zip(entries).map(|(line, e)| format!("{} {line}", faces.face(&e.ant)));
    header.into_iter().chain(rows).collect::<Vec<_>>().join("\n")
}

/// The entries in the colony, read with a human's credentials.
pub async fn list(js: &jetstream::Context) -> Result<Vec<ColonyEntry>> {
    let kv = bus::ensure_colony(js, COLONY_TTL).await?;
    colony::list_entries(&kv).await
}

/// Print the colony: a table, or with `json` one entry per line.
pub async fn show(nats: &crate::connection::Nats<'_>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-colony").await?;
    let entries = list(&js).await?;
    let identities = avatars::read_or_none(&js).await;
    let now = Utc::now();
    let moved: Vec<String> = identities
        .values()
        .filter_map(|i| i.moved.as_ref().map(|mark| crate::ants::export::moved_line(&i.ant, mark, now)))
        .collect();
    let faces = Faces::stdout(identities);
    if json {
        for entry in &entries {
            println!("{}", self::json(entry, now, &faces)?);
        }
    } else if entries.is_empty() {
        println!("no ants registered: {EMPTY_COLONY}");
        if !moved.is_empty() {
            println!("\n{}", moved.join("\n"));
        }
    } else {
        println!("{}", faced(&table(&entries, now), &entries, &faces));
        if let Some(note) = crate::release::versions::colony_note(&entries, now) {
            println!("{note}");
        }
        if let Some(note) = crate::release::versions::harness_note(&entries, now) {
            println!("{note}");
        }
        let collisions = faces.collisions(entries.iter().map(|e| &e.ant));
        let notes = [
            moved,
            held(&entries, now),
            memory_low(&entries, now),
            sense_unkept(&entries),
            drifted(&entries),
            collisions,
        ]
        .concat();
        if !notes.is_empty() {
            println!("\n{}", notes.join("\n"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_daemon::colony::Registration;
    use ants_proto::{Activity, COLONY_HEARTBEAT, HarnessLimit, PermissionsVersion};

    use super::*;
    use crate::test_support::Server;
    use crate::test_support::colony::entry;

    #[test]
    fn an_ant_whose_sense_keeps_none_of_its_data_is_noted_with_why() {
        let now = Utc::now();
        let mut unkept = entry("dev-a", 60, 2, now);
        unkept.sense_unkept = Some("no host credentials on m1".to_owned());
        let notes = sense_unkept(&[entry("dev-b", 60, 2, now), unkept]);
        assert_eq!(
            notes,
            [
                "dev-a: served on m1 without memories, tasks or brief, sense keeping none of its data: no host credentials on m1."
            ]
        );
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

    /// A live ant at work, with all that an entry can tell of one.
    fn busy(now: DateTime<Utc>) -> ColonyEntry {
        let mut busy = entry("dev-a", 190, 4, now);
        (busy.activity, busy.handling, busy.harnesses) = (Activity::Working, 1, 2);
        busy.escalate_to = vec!["lead".parse().unwrap(), "dev-b".parse().unwrap()];
        (busy.version, busy.perms) = ("0.2.0".to_owned(), PermissionsVersion(2));
        busy.agent = Some(ants_proto::HarnessAgent { name: "copilot".to_owned(), version: "1.0.3".to_owned() });
        busy.profile = Some("Good for NATS\nreconnects\tand \u{1b}[31mflaky tests, much more than that.".to_owned());
        busy
    }

    /// A stale ant, with as little as an entry can tell of one.
    fn quiet(now: DateTime<Utc>) -> ColonyEntry {
        let mut quiet = entry("dev-b", 7300, 25, now);
        quiet.capabilities.clear();
        quiet.workdir = None;
        quiet.profile = Some("\u{7}\n".to_owned());
        quiet
    }

    /// dev-c, whose harness has been signed out for five minutes and names how to sign in.
    fn signed_out(now: DateTime<Utc>) -> ColonyEntry {
        let since = now - chrono::Duration::seconds(300);
        let mut out = entry("dev-c", 600, 2, now);
        out.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: Some("copilot login".to_owned()) });
        out
    }

    #[test]
    fn mixed_versions_of_live_ants_are_noted() {
        let now = Utc::now();
        let (busy, quiet) = (busy(now), quiet(now));
        let older = ColonyEntry { version: "0.1.0".to_owned(), ..entry("dev-c", 60, 4, now) };
        assert_eq!(
            crate::release::versions::colony_note(&[busy.clone(), older, quiet.clone()], now).as_deref(),
            Some("mixed ants versions: 0.2.0 (1), 0.1.0 (1); `ants doctor` tells whether credentials may be renewed")
        );
        assert_eq!(crate::release::versions::colony_note(&[busy, quiet], now), None, "a stale one runs nothing");
    }

    #[test]
    fn rows_show_state_and_ages() {
        let now = Utc::now();
        let table = table(&[busy(now), quiet(now)], now);
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
                "WORKDIR",
                "PROFILE"
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
                "/src/app",
                "Good",
                "for",
                "NATS",
                "reconnects",
                "and",
                "[31mflaky",
                "t…"
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
                "-",
                "-"
            ]
        );
        let columns: Vec<Option<usize>> = table.lines().map(|l| l.find("copilot").or(l.find("HARNESS "))).collect();
        assert!(columns.iter().all(|c| c.is_some() && *c == columns[0]), "{table}");
    }

    #[test]
    fn an_ant_whose_harness_cannot_answer_shows_why_and_its_messages_are_told_to_wait() {
        let now = Utc::now();
        let since = now - chrono::Duration::seconds(300);
        let mut unnamed = entry("dev-d", 600, 2, now);
        unnamed.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: None });
        let mut limited = entry("dev-f", 600, 2, now);
        let until = now + chrono::Duration::seconds(120);
        limited.limited = Some(HarnessLimit { kind: LimitKind::RateLimit, since: now, until });
        let mut quota = entry("dev-g", 600, 2, now);
        quota.limited = Some(HarnessLimit { kind: LimitKind::Quota, since, until });
        let mut failing = entry("dev-h", 600, 2, now);
        failing.failing = Some(ants_proto::HarnessFailing { since, error: "copilot: not found".to_owned() });
        let entries = [signed_out(now), unnamed, entry("dev-e", 600, 2, now), limited, quota, failing];
        let table = table(&entries, now);
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
    }

    #[test]
    fn an_ant_low_on_memory_is_noted_and_its_messages_do_not_wait() {
        let now = Utc::now();
        let since = now - chrono::Duration::seconds(300);
        let mut low = entry("dev-i", 600, 2, now);
        low.memory_low = Some(ants_proto::MemoryLow { since, available: 768 << 20, max_harnesses: 1 });
        assert_eq!(held(std::slice::from_ref(&low), now), Vec::<String>::new(), "messages do not wait");
        assert_eq!(
            memory_low(&[low, signed_out(now)], now),
            ["dev-i: memory low on m1 for 5m (768.0 MB available); idle harnesses stop at once and at most 1 harness \
              runs until it recovers."]
        );
    }

    #[test]
    fn a_fallback_answering_shows_as_the_activity_and_among_what_is_held() {
        let now = Utc::now();
        let since = now - chrono::Duration::seconds(300);
        let fallback = Some(ants_proto::ServingFallback { name: "claude:opus".to_owned(), since });
        let mut covered = entry("dev-i", 600, 2, now);
        (covered.signed_out, covered.fallback) = (signed_out(now).signed_out, fallback.clone());
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
    }

    #[test]
    fn json_tells_an_entry_s_state_and_what_it_carries() {
        let now = Utc::now();
        let out = signed_out(now);
        let json: serde_json::Value = serde_json::from_str(&super::json(&out, now, &Faces::plain()).unwrap()).unwrap();
        assert_eq!(json["signed_out"]["sign_in"].as_str(), Some("copilot login"));

        let mut led = entry("dev-a", 0, 30, now);
        led.escalate_to = vec!["lead".parse().unwrap()];
        (led.sage, led.profile) = (Some("sage".parse().unwrap()), Some("Good for NATS.".to_owned()));
        let json: serde_json::Value = serde_json::from_str(&super::json(&led, now, &Faces::plain()).unwrap()).unwrap();
        assert_eq!((json["ant"].as_str(), json["state"].as_str()), (Some("dev-a"), Some("stale")));
        assert_eq!(json["escalate_to"], serde_json::json!(["lead"]));
        assert_eq!((json["sage"].as_str(), json["profile"].as_str()), (Some("sage"), Some("Good for NATS.")));
    }

    #[test]
    fn each_ant_s_face_comes_before_its_row_and_in_its_json() {
        let now = Utc::now();
        let entries = [entry("dev-a", 60, 2, now), entry("dev-b", 60, 2, now)];
        let born = now - chrono::Duration::days(1);
        let mut record = ants_proto::AntIdentity::new("dev-b".parse().unwrap(), "b-id".to_owned(), born);
        record.prompts = 12;
        let sense = ants_proto::SenseId::new();
        record.sense = Some(sense.clone());
        let identities = avatars::Identities::from([(record.ant.clone(), record)]);
        let faces = Faces::new(identities, avatars::Depth::Plain, ants_proto::Theme::Dark);
        let table = table(&entries, now);
        let lines: Vec<String> = faced(&table, &entries, &faces).lines().map(str::to_owned).collect();
        let (a, b) = (ants_proto::Avatar::derive("dev-a"), ants_proto::Avatar::derive(sense.as_str()));
        assert!(lines[0].starts_with("      ANT "), "{lines:?}");
        assert!(lines[1].starts_with(&format!("{} dev-a ", a.face())), "{lines:?}");
        assert!(lines[2].starts_with(&format!("{} dev-b ", b.face())), "{lines:?}");
        let shown = |e| serde_json::from_str::<serde_json::Value>(&json(e, now, &faces).unwrap()).unwrap();
        let born = shown(&entries[1]);
        assert_eq!((born["avatar"].as_str(), born["color"].as_str()), (Some(&*b.face()), Some(b.hue_name())));
        assert_eq!((born["prompts"].as_u64(), born["born"].is_string()), (Some(12), true));
        assert_eq!((born["sense"].as_str(), born["seed"].as_str()), (Some(sense.as_str()), Some("sense")));
        let unborn = shown(&entries[0]);
        assert_eq!((unborn["avatar"].as_str(), unborn["born"].is_null()), (Some(&*a.face()), true), "from the name");
        assert_eq!((unborn["sense"].is_null(), unborn["seed"].as_str()), (true, Some("name")));
    }

    #[test]
    fn unserved_ants_say_why() {
        let now = Utc::now();
        let dev_a: UserId = "dev-a".parse().unwrap();
        assert_eq!(
            unserved(&dev_a, Some(&entry("dev-a", 60, 2, now)), Absent::Unknown, now),
            None,
            "a live ant is served"
        );
        assert_eq!(
            unserved(&dev_a, None, Absent::Known, now).unwrap(),
            "dev-a is not in the colony: no daemon serves dev-a now"
        );
        assert_eq!(
            unserved(&dev_a, None, Absent::Unknown, now).unwrap(),
            "dev-a is not in the colony: no daemon serves dev-a now (`ants colony` lists the ants served: check the \
             name)"
        );
        let empty = unserved(&dev_a, None, Absent::Empty, now).unwrap();
        assert!(empty.starts_with("dev-a is not in the colony, which lists no ant: no daemon beats, or"), "{empty}");
        assert!(empty.contains("memory store is full") && !empty.contains("check the name"), "{empty}");
        assert_eq!(
            unserved(&dev_a, Some(&entry("dev-a", 600, 300, now)), Absent::Known, now).unwrap(),
            "dev-a is stale in the colony: the daemon on m1 last beat 5m ago and may not serve dev-a now"
        );
        let mut out = entry("dev-a", 600, 2, now);
        out.signed_out = Some(ants_proto::SignInNeeded { since: now, sign_in: None });
        assert!(
            unserved(&dev_a, Some(&out), Absent::Known, now).unwrap().starts_with("dev-a: harness copilot signed out")
        );
        let mut limited = entry("dev-a", 600, 2, now);
        limited.limited = Some(HarnessLimit { kind: LimitKind::Quota, since: now, until: now });
        assert!(
            unserved(&dev_a, Some(&limited), Absent::Known, now)
                .unwrap()
                .starts_with("dev-a: harness copilot out of quota")
        );
        limited.fallback = Some(ants_proto::ServingFallback { name: "claude".to_owned(), since: now });
        assert_eq!(unserved(&dev_a, Some(&limited), Absent::Known, now), None, "a fallback answers");
        limited.paused = Some(ants_proto::Paused { since: now - chrono::Duration::minutes(5), by: None });
        let paused = "dev-a: paused for 5m by its config; it takes no new prompt and messages to dev-a wait queued. \
                      Run `ants ant resume dev-a`.";
        assert_eq!(
            unserved(&dev_a, Some(&limited), Absent::Known, now).unwrap(),
            paused,
            "a fallback answers no paused ant"
        );
        assert_eq!(activity(&limited), "paused");
        limited.paused.as_mut().unwrap().by = Some("alice".parse().unwrap());
        assert!(held(&[limited], now)[0].starts_with("dev-a: paused for 5m by alice;"));
    }

    #[test]
    fn an_ant_its_daemon_cannot_serve_says_why_and_when_it_is_tried_again() {
        let now = Utc::now();
        let dev_a: UserId = "dev-a".parse().unwrap();
        let mut e = entry("dev-a", 600, 2, now);
        e.paused = Some(ants_proto::Paused { since: now, by: None });
        e.unserved = Some(ants_proto::Unserved {
            since: now - chrono::Duration::minutes(3),
            error: "the ANTS stream holds its 10000 consumers".to_owned(),
            retry_at: now + chrono::Duration::seconds(40),
        });
        let line = "dev-a: not served on m1 for 3m (the ANTS stream holds its 10000 consumers); messages to dev-a wait \
                    queued, tried again in 40s.";
        assert_eq!(activity(&e), "unserved", "before its pause");
        assert_eq!(held(std::slice::from_ref(&e), now), [line]);
        assert_eq!(unserved(&dev_a, Some(&e), Absent::Known, now).as_deref(), Some(line));
        e.unserved.as_mut().unwrap().retry_at = now - chrono::Duration::seconds(1);
        assert!(held(std::slice::from_ref(&e), now)[0].ends_with("wait queued, tried again now."));
        let table = table(std::slice::from_ref(&e), now);
        assert_eq!(table.lines().nth(1).unwrap().split_whitespace().nth(2), Some("unserved"), "{table}");
        let json: serde_json::Value = serde_json::from_str(&json(&e, now, &Faces::plain()).unwrap()).unwrap();
        assert!(json["unserved"]["retry_at"].is_string() && json["unserved"]["error"].is_string(), "{json}");
    }

    /// Read with the human's credentials: an ant registered, one with only its escalation chain kept, one unknown.
    #[tokio::test]
    async fn the_human_learns_which_ants_no_daemon_serves() {
        let Some(server) = Server::start() else { return };
        let (_, human) = server.connect(&UserId::human()).await;
        let ids: Vec<UserId> = ["dev-a", "dev-b", "dev-c"].map(|a| a.parse().unwrap()).into();
        let ants: Vec<&UserId> = ids.iter().collect();
        assert!(unserved_warnings(&human, &ants).await.is_empty(), "no colony to read, no warning");

        bus::ensure_colony(&human, COLONY_TTL).await.unwrap();
        bus::ensure_chains(&human).await.unwrap();
        let empty = unserved_warnings(&human, &ants[2..]).await;
        assert!(empty[0].starts_with("dev-c is not in the colony, which lists no ant"), "{empty:?}");
        let (_, js) = server.connect(&ids[0]).await;
        let kv = bus::ensure_colony(&js, COLONY_TTL).await.unwrap();
        let _registration = Registration::start(kv, entry("dev-a", 60, 0, Utc::now()), COLONY_HEARTBEAT).await.unwrap();
        let (_, js) = server.connect(&ids[1]).await;
        ants_daemon::records::chains::publish(&js, &ids[1], std::slice::from_ref(&ids[0])).await.unwrap();
        assert_eq!(
            unserved_warnings(&human, &ants).await,
            [
                "dev-b is not in the colony: no daemon serves dev-b now",
                "dev-c is not in the colony: no daemon serves dev-c now (`ants colony` lists the ants served: check \
                 the name)"
            ]
        );
    }

    /// With the human's credentials, so it also checks that they read the colony.
    #[tokio::test]
    async fn the_human_lists_registered_ants() {
        let Some(server) = Server::start() else { return };
        let (_, human) = server.connect(&UserId::human()).await;
        assert!(list(&human).await.unwrap().is_empty(), "an empty colony, created on first use");

        let mut registrations = Vec::new();
        let now = Utc::now();
        for ant in ["dev-b", "dev-a"] {
            let (_, js) = server.connect(&ant.parse().unwrap()).await;
            let kv = bus::ensure_colony(&js, COLONY_TTL).await.unwrap();
            let entry = entry(ant, 60, 0, now);
            registrations.push(Registration::start(kv, entry, COLONY_HEARTBEAT).await.unwrap());
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
