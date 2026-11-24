//! `ants schedule list` and `/schedules` in ants chat: the `[[schedule]]` entries of a machine's config file, each with
//! its timetable, its destination, its next firing and how its last firing went, as the keys its firings leave in
//! [`WEBHOOKS_BUCKET`] tell ([`ants_proto::webhook_seen_key`]); with an ant, those sending to it and the reminders and
//! watchers sense keeps for it, as its memories show them ([`crate::admin::memories`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use ants_daemon::schedule::Schedule;
use ants_proto::{Destination, UserId, WEBHOOK_RETENTION, WEBHOOKS_BUCKET, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::{DateTime, TimeDelta, Utc};
use futures::TryStreamExt as _;
use serde_json::{Value, json};

use crate::connection::Nats;

/// One schedule as listed.
#[derive(Debug, Clone, PartialEq)]
pub struct Listed {
    pub name: UserId,
    /// Its timetable, as `cron "0 3 * * 1-5" in Europe/London` or `every 4h`.
    pub when: String,
    pub to: Destination,
    /// The saved trail its firings start, if they start one.
    pub trail: Option<String>,
    pub catch_up: bool,
    /// None once it fires no more.
    pub next: Option<DateTime<Utc>>,
    /// Its last firing due in the last [`WEBHOOK_RETENTION`]; None before any.
    pub last: Option<Last>,
}

/// The last firing due, and whether it went out.
#[derive(Debug, Clone, PartialEq)]
pub struct Last {
    pub due: DateTime<Utc>,
    /// Some(true) when its key is in [`WEBHOOKS_BUCKET`], Some(false) when not (skipped, held for a paused ant, or
    /// missed while no daemon served the schedule); None when the bucket was not read.
    pub sent: Option<bool>,
}

/// The firings of each schedule that went out in the last [`WEBHOOK_RETENTION`], by their due time in Unix seconds.
pub type Sent = BTreeMap<UserId, BTreeSet<i64>>;

/// `schedules` as listed at `now`, `sent` saying which firings went out, when read.
pub fn listed(schedules: &[Schedule], sent: Option<&Sent>, now: DateTime<Utc>) -> Vec<Listed> {
    let since = now - TimeDelta::from_std(WEBHOOK_RETENTION).unwrap_or(TimeDelta::days(7));
    schedules
        .iter()
        .map(|schedule| {
            let last = schedule.when.last_in(since, now).map(|due| Last {
                due,
                sent: sent.map(|sent| sent.get(&schedule.name).is_some_and(|keys| keys.contains(&due.timestamp()))),
            });
            Listed {
                name: schedule.name.clone(),
                when: schedule.when.to_string(),
                to: schedule.destination.clone(),
                trail: schedule.trail.as_ref().map(|start| start.trail.to_string()),
                catch_up: schedule.catch_up,
                next: schedule.when.next_after(now),
                last,
            }
        })
        .collect()
}

/// The firings of `schedules` that went out, from the keys of [`WEBHOOKS_BUCKET`], which a human's credentials list
/// through the bucket's stream info.
pub async fn sent(js: &jetstream::Context, schedules: &[Schedule]) -> Result<Sent> {
    let mut sent = Sent::new();
    if schedules.is_empty() {
        return Ok(sent);
    }
    let stream = match js.get_stream(subject::kv_stream(WEBHOOKS_BUCKET)).await {
        Ok(stream) => stream,
        Err(e) if crate::node::outbox::not_found(&e) => return Ok(sent),
        Err(e) => return Err(e).context(format!("looking up {WEBHOOKS_BUCKET}")),
    };
    for schedule in schedules {
        let keys: Vec<(String, usize)> = stream
            .info_with_subjects(subject::kv(WEBHOOKS_BUCKET, format_args!("{}.*", schedule.name)))
            .await
            .context("listing the schedules' firings")?
            .try_collect()
            .await
            .context("listing the schedules' firings")?;
        let firings = keys.iter().filter_map(|(subject, _)| {
            let key = subject::kv_key(WEBHOOKS_BUCKET, subject)?;
            key.strip_prefix(schedule.name.as_str())?.strip_prefix('.')?.parse::<i64>().ok()
        });
        sent.insert(schedule.name.clone(), firings.collect());
    }
    Ok(sent)
}

fn stamp(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M UTC").to_string()
}

fn destination(to: &Destination) -> String {
    match to {
        Destination::Ant(ant) => format!("ant {ant}"),
        Destination::Channel(channel) => format!("channel #{channel}"),
    }
}

fn last_text(last: Option<&Last>) -> String {
    match last {
        None => "none in 7d".to_owned(),
        Some(Last { due, sent: Some(true) }) => format!("{} sent", stamp(*due)),
        Some(Last { due, sent: Some(false) }) => format!("{} not sent", stamp(*due)),
        Some(Last { due, sent: None }) => format!("{} (unread)", stamp(*due)),
    }
}

/// The schedules as a table: name, timetable, destination, next firing and the last one.
pub fn table(listed: &[Listed]) -> String {
    if listed.is_empty() {
        return "no schedules\n".to_owned();
    }
    let mut rows = vec![["SCHEDULE", "WHEN", "TO", "NEXT", "LAST"].map(str::to_owned)];
    for schedule in listed {
        let mut to = destination(&schedule.to);
        if let Some(trail) = &schedule.trail {
            let _ = write!(to, ", trail {trail}");
        }
        let mut when = schedule.when.clone();
        if schedule.catch_up {
            when.push_str(", catch_up");
        }
        let next = schedule.next.map_or_else(|| "fires no more".to_owned(), stamp);
        rows.push([schedule.name.to_string(), when, to, next, last_text(schedule.last.as_ref())]);
    }
    crate::text::columns(&rows)
}

/// One schedule as `--json` prints it.
pub fn json_line(schedule: &Listed) -> Value {
    let (to_kind, to) = match &schedule.to {
        Destination::Ant(ant) => ("ant", ant.to_string()),
        Destination::Channel(channel) => ("channel", channel.to_string()),
    };
    json!({
        "kind": "schedule",
        "name": schedule.name,
        "when": schedule.when,
        to_kind: to,
        "trail": schedule.trail,
        "catch_up": schedule.catch_up,
        "next": schedule.next,
        "last_due": schedule.last.as_ref().map(|last| last.due),
        "last_sent": schedule.last.as_ref().and_then(|last| last.sent),
    })
}

/// The schedules of the config file at `path`, those sending to `ant` when given.
pub fn of_config(path: &Path, ant: Option<&UserId>) -> Result<Vec<Schedule>> {
    let loaded = ants_daemon::config::load(path)?;
    Ok(loaded
        .schedules
        .into_iter()
        .filter(|schedule| ant.is_none_or(|ant| schedule.destination == Destination::Ant(ant.clone())))
        .collect())
}

/// The schedules of the config file at `path` listed at `now`, how their last firings went read with `js`, or why
/// not.
pub async fn read(js: &jetstream::Context, path: &Path, ant: Option<&UserId>) -> Result<(Vec<Listed>, Option<String>)> {
    let schedules = of_config(path, ant)?;
    let (sent, unread) = match sent(js, &schedules).await {
        Ok(sent) => (Some(sent), None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };
    Ok((listed(&schedules, sent.as_ref(), Utc::now()), unread))
}

/// The active reminders and watchers sense keeps for `ant`, one line each, as its memories read by an admin order
/// show them; none for an ant whose provider keeps none.
pub async fn reminders(nats: &Nats<'_>, js: &jetstream::Context, ant: &UserId) -> Result<Vec<String>> {
    let memories = crate::admin::memories::read(nats, js, None, ant).await?;
    let Some(provider) = memories.provider else { return Ok(Vec::new()) };
    if let Some(error) = provider.error {
        anyhow::bail!("{error}");
    }
    let entry = provider.memories.into_iter().find(|entry| entry.key == ants_daemon::sense::REMINDERS);
    Ok(entry.map_or_default(|entry| entry.content.lines().map(str::to_owned).collect()))
}

/// `ants schedule list`: the schedules of the config file at `path`, with `ant` those sending to it and the ant's
/// reminders and watchers, as a table or with `json` one JSON object per line.
pub async fn list(nats: &Nats<'_>, path: &Path, ant: Option<&UserId>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-schedules").await?;
    let (listed, unread) = read(&js, path, ant).await?;
    let reminders = match ant {
        Some(ant) => Some(reminders(nats, &js, ant).await.map_err(|e| format!("{e:#}"))),
        None => None,
    };
    if json {
        for schedule in &listed {
            println!("{}", json_line(schedule));
        }
        match (ant, &reminders) {
            (Some(ant), Some(Ok(lines))) => {
                for line in lines {
                    println!("{}", json!({"kind": "reminder", "ant": ant, "line": line}));
                }
            }
            (Some(ant), Some(Err(e))) => println!("{}", json!({"kind": "reminders_unread", "ant": ant, "error": e})),
            _ => {}
        }
        return Ok(());
    }
    print!("{}", table(&listed));
    if let Some(unread) = unread {
        println!("last firings not read: {unread}");
    }
    if let (Some(ant), Some(reminders)) = (ant, reminders) {
        match reminders {
            Ok(lines) if lines.is_empty() => println!("\n{ant} has no active reminders or watchers"),
            Ok(lines) => println!("\nreminders and watchers of {ant}\n{}", lines.join("\n")),
            Err(e) => println!("\nreminders and watchers of {ant} not read: {e}"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
