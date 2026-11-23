//! `ants ant show`, and what the ant view of `ants chat` shows the same way: one ant at a glance. Where and how it runs
//! and what holds it back, as its colony entry tells; the messages waiting for it, as `ants queue` lists them; its
//! threads, as `ants thread list --ant` lists them; what it spent that UTC day, as `ants spend` reports it; and its
//! settings and the admin orders to it ([`crate::admin::settings`]).

use std::collections::BTreeMap;

use ants_daemon::limits::caps::Caps;
use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{ColonyEntry, MovedMark, SESSIONS_BUCKET, UserId};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::admin::{orders, settings};
use crate::colony::{self, State, short, since};
use crate::connection::Nats;
use crate::threads::thread;
use crate::{queue, spend};

/// How many of its most recently active threads `ants ant show` lists.
const THREADS_SHOWN: usize = 10;

/// The ant's state in a few words: `live, working`, `stale, idle`, `not in the colony`, or, `moved` (by `ants ant export
/// --move`), `moved` or `moved, draining` while its daemon still serves it.
pub fn state(entry: Option<&ColonyEntry>, moved: Option<&MovedMark>, now: DateTime<Utc>) -> String {
    match (entry, moved) {
        (Some(_), Some(_)) => "moved, draining".to_owned(),
        (None, Some(_)) => "moved".to_owned(),
        (Some(e), None) => format!("{}, {}", State::of(e, now).as_str(), colony::activity(e)),
        (None, None) => "not in the colony".to_owned(),
    }
}

/// Where and how the ant runs, as its colony entry tells: one labelled row each, `-` for what the entry leaves out.
pub fn about(e: &ColonyEntry, now: DateTime<Utc>) -> Vec<(&'static str, String)> {
    let or_none = |list: Vec<String>| if list.is_empty() { "-".to_owned() } else { list.join(", ") };
    vec![
        ("machine", e.machine.clone()),
        ("harness", e.agent.as_ref().map_or_else(|| e.harness.clone(), |a| format!("{} ({a})", e.harness))),
        ("model", e.model.as_ref().map_or_else(|| "harness default".to_owned(), ToString::to_string)),
        ("workdir", e.workdir.clone().unwrap_or_else(|| "-".to_owned())),
        ("capabilities", or_none(e.capabilities.clone())),
        ("escalates to", or_none(e.escalate_to.iter().map(ToString::to_string).collect())),
        ("sage", e.sage.as_ref().map_or_else(|| "-".to_owned(), ToString::to_string)),
        ("harnesses", format!("{} running, {} messages in hand", e.harnesses, e.handling)),
        ("version", format!("{} (permissions {})", e.version, e.perms)),
        ("up", format!("{}, heartbeat {} ago", short(since(e.started_at, now)), short(since(e.heartbeat_at, now)))),
        ("whoami", e.whoami.as_deref().map_or_else(|| "-".to_owned(), |w| w.trim().to_owned())),
        ("profile", e.profile.as_deref().map_or_else(|| "-".to_owned(), |p| p.trim().to_owned())),
    ]
}

/// What holds `ant` back now, one line each: its move to another bus (`moved`), no daemon serving it, a stale entry, a
/// harness signed out, failing to start or held by a provider limit, a fallback answering for it, low memory on its
/// machine, sense keeping none of its data. Empty when nothing does.
pub fn holds(ant: &UserId, entry: Option<&ColonyEntry>, moved: Option<&MovedMark>, now: DateTime<Utc>) -> Vec<String> {
    if let Some(mark) = moved {
        return vec![crate::export::moved_line(ant, mark, now)];
    }
    let Some(e) = entry else { return colony::unserved(ant, None, colony::Absent::Known, now).into_iter().collect() };
    if State::of(e, now) == State::Stale {
        return colony::unserved(ant, Some(e), colony::Absent::Known, now).into_iter().collect();
    }
    let entries = std::slice::from_ref(e);
    let prefix = format!("{ant}: ");
    [colony::held(entries, now), colony::memory_low(entries, now), colony::sense_unkept(entries)]
        .concat()
        .into_iter()
        .map(|line| line.strip_prefix(&prefix).map_or_else(|| line.clone(), str::to_owned))
        .collect()
}

/// `ants ant show` as JSON.
#[derive(Serialize)]
struct Shown {
    ant: UserId,
    /// The colony entry, with its `state` added; None when the colony lists none.
    colony: Option<serde_json::Value>,
    holds: Vec<String>,
    /// None when no daemon of the ant answered.
    queue: Option<Vec<queue::Waiting>>,
    /// The messages waiting in the stream past those listed.
    #[serde(skip_serializing_if = "is_zero")]
    queue_unlisted: usize,
    /// None when it could not be read.
    spend_today: Option<spend::AntReport>,
    threads: Vec<thread::Summary>,
    /// None when never read.
    settings: Option<settings::Read>,
    /// The ant's settings on every machine; none while it has no verified record of them.
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_settings: Option<ants_proto::SharedSettings>,
    /// A record of them that daemons ignore, not verified.
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_settings_unverified: Option<Unverified>,
    /// Why the settings are not read afresh, if they are not.
    #[serde(skip_serializing_if = "Option::is_none")]
    settings_stale: Option<String>,
    orders: Vec<orders::Listed>,
}

#[derive(Serialize)]
struct Unverified {
    why: String,
    record: ants_proto::SharedSettings,
}

impl Unverified {
    /// `shared` as shown: the verified record, or the unverified one with why.
    fn split(shared: Option<settings::SharedRecord>) -> (Option<ants_proto::SharedSettings>, Option<Self>) {
        match shared {
            Some(settings::SharedRecord::Verified(record)) => (Some(record), None),
            Some(settings::SharedRecord::Unverified(record, why)) => (None, Some(Self { why, record })),
            None => (None, None),
        }
    }
}

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde's skip_serializing_if takes a reference")]
fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// Print what is known of `ant`, as text or with `json` one JSON object; `caps` are the ants' caps the local config
/// file sets.
pub async fn show(nats: &Nats<'_>, ant: UserId, caps: &BTreeMap<UserId, Caps>, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; `ants ant show` reads what a human reads");
    }
    let (_, js) = nats.connect("ants-ant").await?;
    let entry = colony::list(&js).await?.into_iter().find(|e| e.ant == ant);
    let queue = queue::read(&js, nats.domain, &ant, me).await?;
    let spend = async {
        let kv = DirectKv::new(&js, nats.domain, SESSIONS_BUCKET).await?;
        let days = spend::window(Utc::now().date_naive(), 1);
        anyhow::Ok(spend::ants(&js, &kv, Some(&ant), &days, caps).await?.into_iter().next())
    };
    let spend = spend.await.inspect_err(|e| tracing::warn!("not showing the spend: {e:#}")).ok().flatten();
    let select = thread::Select {
        ant: Some(ant.clone()),
        owner: None,
        grep: None,
        limit: Some(THREADS_SHOWN),
        escalated: false,
        pinned: false,
        archived: false,
        idle_before: None,
    };
    let (threads, omitted) = thread::listed(nats, None, &select).await?;
    let reading = settings::read(nats, &js, &ant, settings::READ_WAIT).await?;
    let faces = crate::avatars::Faces::stdout(crate::avatars::read_or_none(&js).await);
    let now = Utc::now();
    let moved = faces.identity(&ant).and_then(|i| i.moved.as_ref());
    let holds = holds(&ant, entry.as_ref(), moved, now);
    if json {
        let colony = entry.as_ref().map(|e| colony::json(e, now, &faces)).transpose()?;
        let colony = colony.map(|r| serde_json::from_str(&r)).transpose()?;
        let (queue, queue_unlisted) = queue.map_or((None, 0), |(waiting, more)| (Some(waiting), more));
        let settings::Reading { read, stale, orders, shared } = reading;
        let (shared_settings, shared_settings_unverified) = Unverified::split(shared.map(|s| *s));
        let shown = Shown {
            ant,
            colony,
            holds,
            queue,
            queue_unlisted,
            spend_today: spend,
            threads,
            settings: read,
            shared_settings,
            shared_settings_unverified,
            settings_stale: stale,
            orders,
        };
        println!("{}", serde_json::to_string(&shown)?);
        return Ok(());
    }
    println!("{}: {}", faces.named(&ant), state(entry.as_ref(), moved, now));
    if let Some(e) = &entry {
        for (label, value) in about(e, now) {
            let value = value.lines().collect::<Vec<_>>().join(&format!("\n  {:13} ", ""));
            println!("  {label:13} {value}");
        }
    }
    println!("\nholds:");
    if holds.is_empty() {
        println!("  none");
    }
    for line in &holds {
        println!("  {line}");
    }
    println!("\nqueue (`ants queue {ant}`):");
    match &queue {
        None => println!("  {}", queue::unanswered(&ant)),
        Some((waiting, _)) if waiting.is_empty() => println!("  no messages waiting"),
        Some((waiting, more)) => {
            println!("{}", indent(&queue::table(waiting, now)));
            if *more > 0 {
                println!("  {more} more messages wait in the stream, not listed");
            }
        }
    }
    println!("\nspend today, UTC (`ants spend {ant}`):");
    match &spend {
        Some(report) => println!("{}", indent(&spend::ant_tables(report))),
        None => println!("  not read"),
    }
    println!("\nthreads (`ants thread list --ant {ant}`):");
    if threads.is_empty() {
        println!("  no threads");
    } else {
        println!("{}", indent(&thread::table(&threads)));
    }
    if omitted > 0 {
        println!("  and {omitted} less recently active");
    }
    for line in settings::sections(&ant, &reading, now) {
        println!("{line}");
    }
    Ok(())
}

fn indent(text: &str) -> String {
    text.lines()
        .map(|line| if line.is_empty() { String::new() } else { format!("  {line}") })
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use ants_proto::{AntModel, HarnessAgent, MemoryLow, SignInNeeded};

    use super::*;
    use crate::test_support::colony::entry;

    fn now() -> DateTime<Utc> {
        "2026-01-01T12:00:00Z".parse().unwrap()
    }

    #[test]
    fn about_tells_where_and_how_the_ant_runs() {
        let mut e = entry("dev-a", 7200, 3, now());
        e.agent = Some(HarnessAgent { name: "copilot-agent".to_owned(), version: "1.0.3".to_owned() });
        e.whoami = Some("Maintain the backend.\nReview NATS changes.\n".to_owned());
        let rows: BTreeMap<&str, String> = about(&e, now()).into_iter().collect();
        assert_eq!(rows["harness"], "copilot (copilot-agent 1.0.3)");
        assert_eq!(rows["model"], "harness default");
        assert_eq!(rows["capabilities"], "rust, review");
        assert_eq!(rows["escalates to"], "-");
        assert_eq!(rows["up"], "2h, heartbeat 3s ago");
        assert_eq!(rows["whoami"], "Maintain the backend.\nReview NATS changes.");
        assert_eq!((rows["sage"].as_str(), rows["profile"].as_str()), ("-", "-"));
        e.sage = Some("sage".parse().unwrap());
        e.profile = Some("Good for NATS.\n".to_owned());
        let rows: BTreeMap<&str, String> = about(&e, now()).into_iter().collect();
        assert_eq!((rows["sage"].as_str(), rows["profile"].as_str()), ("sage", "Good for NATS."));
        e.model = AntModel::of(Some("gpt-5".to_owned()), Some("high".to_owned()), None);
        let rows: BTreeMap<&str, String> = about(&e, now()).into_iter().collect();
        assert_eq!(rows["model"], "gpt-5, effort high");
        assert_eq!(state(Some(&e), None, now()), "live, idle");
        assert_eq!(state(None, None, now()), "not in the colony");
        let mark = MovedMark { at: now(), by: "alice".parse().unwrap() };
        assert_eq!(state(Some(&e), Some(&mark), now()), "moved, draining");
        assert_eq!(state(None, Some(&mark), now()), "moved");
    }

    #[test]
    fn holds_name_what_keeps_the_ant_from_answering() {
        let ant: UserId = "dev-a".parse().unwrap();
        assert_eq!(holds(&ant, None, None, now()), ["dev-a is not in the colony: no daemon serves dev-a now"]);
        let stale = entry("dev-a", 7200, 600, now());
        assert!(holds(&ant, Some(&stale), None, now())[0].starts_with("dev-a is stale in the colony"));
        let mut e = entry("dev-a", 7200, 3, now());
        assert!(holds(&ant, Some(&e), None, now()).is_empty());
        e.signed_out = Some(SignInNeeded { since: now() - chrono::Duration::minutes(5), sign_in: None });
        e.memory_low = Some(MemoryLow { since: now(), available: 512 << 20, max_harnesses: 1 });
        e.sense_unkept = Some("no host credentials on m1".to_owned());
        let lines = holds(&ant, Some(&e), None, now());
        assert_eq!(lines.len(), 3, "{lines:?}");
        assert!(lines[0].starts_with("harness copilot signed out for 5m"), "{lines:?}");
        assert!(lines[1].starts_with("memory low on m1"), "{lines:?}");
        assert!(lines[2].starts_with("served on m1 without memories"), "{lines:?}");
        let mark = MovedMark { at: now() - chrono::Duration::minutes(5), by: "alice".parse().unwrap() };
        let lines = holds(&ant, Some(&e), Some(&mark), now());
        assert_eq!(lines.len(), 1, "a moved ant is held by its move alone: {lines:?}");
        assert!(lines[0].starts_with("dev-a was moved to another bus by alice 5m ago"), "{lines:?}");
    }
}
