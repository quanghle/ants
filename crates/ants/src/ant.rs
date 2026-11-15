//! `ants ant show`, and what the ant view of `ants chat` shows the same way: one ant at a glance. Where and how it runs
//! and what holds it back, as its roster entry tells; the messages waiting for it, as `ants queue` lists them; its
//! threads, as `ants thread list --ant` lists them; what it spent that UTC day, as `ants spend` reports it; and its
//! settings and the admin orders to it ([`crate::settings`]).

use std::collections::BTreeMap;

use ants_daemon::caps::Caps;
use ants_daemon::direct_kv::DirectKv;
use ants_proto::{RosterEntry, SESSIONS_BUCKET, UserId};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::roster::{self, State, short, since};
use crate::{orders, queue, settings, spend, thread};

/// How many of its most recently active threads `ants ant show` lists.
const THREADS_SHOWN: usize = 10;

/// The ant's state in a few words: `live, working`, `stale, idle`, or `not in the roster`.
pub fn state(entry: Option<&RosterEntry>, now: DateTime<Utc>) -> String {
    match entry {
        Some(e) => format!("{}, {}", State::of(e, now).as_str(), roster::activity(e)),
        None => "not in the roster".to_owned(),
    }
}

/// Where and how the ant runs, as its roster entry tells: one labelled row each, `-` for what the entry leaves out.
pub fn about(e: &RosterEntry, now: DateTime<Utc>) -> Vec<(&'static str, String)> {
    let or_none = |list: Vec<String>| if list.is_empty() { "-".to_owned() } else { list.join(", ") };
    vec![
        ("machine", e.machine.clone()),
        ("harness", e.agent.as_ref().map_or_else(|| e.harness.clone(), |a| format!("{} ({a})", e.harness))),
        ("model", e.model.as_ref().map_or_else(|| "harness default".to_owned(), ToString::to_string)),
        ("workdir", e.workdir.clone().unwrap_or_else(|| "-".to_owned())),
        ("capabilities", or_none(e.capabilities.clone())),
        ("escalates to", or_none(e.escalate_to.iter().map(ToString::to_string).collect())),
        ("harnesses", format!("{} running, {} messages in hand", e.harnesses, e.handling)),
        ("version", format!("{} (permissions {})", e.version, e.perms)),
        ("up", format!("{}, heartbeat {} ago", short(since(e.started_at, now)), short(since(e.heartbeat_at, now)))),
        ("whoami", e.whoami.as_deref().map_or_else(|| "-".to_owned(), |w| w.trim().to_owned())),
    ]
}

/// What holds `ant` back now, one line each: no daemon serving it, a stale entry, a harness signed out, failing to
/// start or held by a provider limit, a fallback answering for it, low memory on its machine. Empty when nothing
/// does.
pub fn holds(ant: &UserId, entry: Option<&RosterEntry>, now: DateTime<Utc>) -> Vec<String> {
    let Some(e) = entry else { return roster::unserved(ant, None, true, now).into_iter().collect() };
    if State::of(e, now) == State::Stale {
        return roster::unserved(ant, Some(e), true, now).into_iter().collect();
    }
    let entries = std::slice::from_ref(e);
    let prefix = format!("{ant}: ");
    [roster::held(entries, now), roster::memory_low(entries, now)]
        .concat()
        .into_iter()
        .map(|line| line.strip_prefix(&prefix).map_or_else(|| line.clone(), str::to_owned))
        .collect()
}

/// `ants ant show` as JSON.
#[derive(Serialize)]
struct Shown {
    ant: UserId,
    /// The roster entry, with its `state` added; None when the roster lists none.
    roster: Option<serde_json::Value>,
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
    /// Why the settings are not read afresh, if they are not.
    #[serde(skip_serializing_if = "Option::is_none")]
    settings_stale: Option<String>,
    orders: Vec<orders::Listed>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// Print what is known of `ant`, as text or with `json` one JSON object; `caps` are the ants' caps the local config
/// file sets.
pub async fn show(nats: &crate::Nats<'_>, ant: UserId, caps: &BTreeMap<UserId, Caps>, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; `ants ant show` reads what a human reads");
    }
    let (_, js) = nats.connect("ants-ant").await?;
    let entry = roster::list(&js).await?.into_iter().find(|e| e.ant == ant);
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
    let now = Utc::now();
    let holds = holds(&ant, entry.as_ref(), now);
    if json {
        let roster = entry.as_ref().map(|e| roster::json(e, now)).transpose()?;
        let roster = roster.map(|r| serde_json::from_str(&r)).transpose()?;
        let (queue, queue_unlisted) = queue.map_or((None, 0), |(waiting, more)| (Some(waiting), more));
        let settings::Reading { read, stale, orders } = reading;
        let shown = Shown {
            ant,
            roster,
            holds,
            queue,
            queue_unlisted,
            spend_today: spend,
            threads,
            settings: read,
            settings_stale: stale,
            orders,
        };
        println!("{}", serde_json::to_string(&shown)?);
        return Ok(());
    }
    println!("{ant}: {}", state(entry.as_ref(), now));
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
    use crate::roster::tests::entry;

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
        e.model = AntModel::of(Some("gpt-5".to_owned()), Some("high".to_owned()), None);
        let rows: BTreeMap<&str, String> = about(&e, now()).into_iter().collect();
        assert_eq!(rows["model"], "gpt-5, effort high");
        assert_eq!(state(Some(&e), now()), "live, idle");
        assert_eq!(state(None, now()), "not in the roster");
    }

    #[test]
    fn holds_name_what_keeps_the_ant_from_answering() {
        let ant: UserId = "dev-a".parse().unwrap();
        assert_eq!(holds(&ant, None, now()), ["dev-a is not in the roster: no daemon serves dev-a now"]);
        let stale = entry("dev-a", 7200, 600, now());
        assert!(holds(&ant, Some(&stale), now())[0].starts_with("dev-a is stale in the roster"));
        let mut e = entry("dev-a", 7200, 3, now());
        assert!(holds(&ant, Some(&e), now()).is_empty());
        e.signed_out = Some(SignInNeeded { since: now() - chrono::Duration::minutes(5), sign_in: None });
        e.memory_low = Some(MemoryLow { since: now(), available: 512 << 20, max_harnesses: 1 });
        let lines = holds(&ant, Some(&e), now());
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(lines[0].starts_with("harness copilot signed out for 5m"), "{lines:?}");
        assert!(lines[1].starts_with("memory low on m1"), "{lines:?}");
    }
}
