//! `ants schedule list` and `/schedules` in ants chat: the `[[schedule]]` entries of a machine's config file, each with
//! its timetable, its destination, its next firing and how its last firing went, as the keys its firings leave in
//! [`WEBHOOKS_BUCKET`] tell ([`ants_proto::webhook_seen_key`]); with an ant, those sending to it and the reminders and
//! watchers sense keeps for it, as its memories show them ([`crate::admin::memories`]). The config file is this
//! machine's, or another machine's that an admin order asks the daemon there to list ([`AdminOp::ReadSchedules`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use ants_daemon::schedule::Schedule;
use ants_proto::{
    AdminOp, Destination, ListedSchedule, Machine, SCHEDULES_FRESH, Schedules, UserId, WEBHOOKS_BUCKET, subject,
};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::TryStreamExt as _;
use serde_json::{Value, json};

use crate::admin::orders::{Admin, Asked};
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
    /// Its last firing due in the last [`ants_proto::WEBHOOK_RETENTION`]; None before any.
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

/// The firings of each schedule that went out in the last [`ants_proto::WEBHOOK_RETENTION`], by their due time in Unix seconds.
pub type Sent = BTreeMap<UserId, BTreeSet<i64>>;

/// `listed`, as a daemon lists schedules, with `sent` saying which firings went out, when read.
fn with_sent(listed: Vec<ListedSchedule>, sent: Option<&Sent>) -> Vec<Listed> {
    listed
        .into_iter()
        .map(|schedule| {
            let last = schedule.last_due.map(|due| Last {
                due,
                sent: sent.map(|sent| sent.get(&schedule.name).is_some_and(|keys| keys.contains(&due.timestamp()))),
            });
            Listed {
                name: schedule.name,
                when: schedule.when,
                to: schedule.to,
                trail: schedule.trail,
                catch_up: schedule.catch_up,
                next: schedule.next,
                last,
            }
        })
        .collect()
}

/// The firings of the schedules named `names` that went out, from the keys of [`WEBHOOKS_BUCKET`], which a human's
/// credentials list through the bucket's stream info.
pub async fn sent(js: &jetstream::Context, names: &[&UserId]) -> Result<Sent> {
    let mut sent = Sent::new();
    if names.is_empty() {
        return Ok(sent);
    }
    let stream = match js.get_stream(subject::kv_stream(WEBHOOKS_BUCKET)).await {
        Ok(stream) => stream,
        Err(e) if crate::node::outbox::not_found(&e) => return Ok(sent),
        Err(e) => return Err(e).context(format!("looking up {WEBHOOKS_BUCKET}")),
    };
    for &name in names {
        let keys: Vec<(String, usize)> = stream
            .info_with_subjects(subject::kv(WEBHOOKS_BUCKET, format_args!("{name}.*")))
            .await
            .context("listing the schedules' firings")?
            .try_collect()
            .await
            .context("listing the schedules' firings")?;
        let firings = keys.iter().filter_map(|(subject, _)| {
            let key = subject::kv_key(WEBHOOKS_BUCKET, subject)?;
            key.strip_prefix(name.as_str())?.strip_prefix('.')?.parse::<i64>().ok()
        });
        sent.insert(name.clone(), firings.collect());
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

/// The config file whose schedules to list.
#[derive(Debug, Clone, Copy)]
pub enum Source<'a> {
    /// The config file at this path, on this machine.
    Here(&'a Path),
    /// The config file the daemon of this machine serves, which an admin order asks it to list.
    On(&'a Machine),
}

/// Schedules as listed, with why how their last firings went was not read, and how many the daemon listing them left
/// out.
#[derive(Debug, Clone, PartialEq)]
pub struct Listing {
    pub listed: Vec<Listed>,
    pub unread: Option<String>,
    pub left_out: usize,
}

/// The schedules of the config file at `path` listed now, how their last firings went read with `js`, or why not.
pub async fn read(js: &jetstream::Context, path: &Path, ant: Option<&UserId>) -> Result<Listing> {
    let now = Utc::now();
    let listed = of_config(path, ant)?.iter().map(|schedule| schedule.listed(now)).collect();
    Ok(with_firings(js, listed, 0).await)
}

/// How long the CLI waits for the daemon of another machine to list its schedules.
pub const WAIT: std::time::Duration = std::time::Duration::from_secs(15);

/// The schedules of the config file the daemon of `machine` serves, those sending to `ant` when given, which an admin
/// order `admin` signs asks it to list, waiting up to [`WAIT`]; how their last firings went read with `js`, or why
/// not. The order goes to the daemon through `ant`, else through the first ant the machine records serving.
pub async fn read_on(
    admin: &Admin,
    js: &jetstream::Context,
    machine: &Machine,
    ant: Option<&UserId>,
) -> Result<Listing> {
    let through = match ant {
        Some(ant) => ant.clone(),
        None => crate::ants::machines::list(js)
            .await?
            .into_iter()
            .find(|record| &record.machine == machine)
            .and_then(|record| record.ants.into_iter().next())
            .ok_or_else(|| {
                anyhow!("{machine} records serving no ant, through which an admin order reaches its daemon")
            })?,
    };
    let asked = Asked {
        online: "schedules are read only while it is online",
        fresh: SCHEDULES_FRESH,
        to: "list the schedules of its config file",
        lacking: "schedules",
    };
    let read: Schedules = admin.ask(js, Some(machine.clone()), &through, AdminOp::ReadSchedules, WAIT, &asked).await?;
    let mut listed = read.schedules;
    if let Some(ant) = ant {
        listed.retain(|schedule| schedule.to == Destination::Ant(ant.clone()));
    }
    Ok(with_firings(js, listed, read.left_out).await)
}

/// `listed` with how their last firings went, read with `js`, or why not.
async fn with_firings(js: &jetstream::Context, listed: Vec<ListedSchedule>, left_out: usize) -> Listing {
    let names: Vec<&UserId> = listed.iter().map(|schedule| &schedule.name).collect();
    let (sent, unread) = match sent(js, &names).await {
        Ok(sent) => (Some(sent), None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };
    Listing { listed: with_sent(listed, sent.as_ref()), unread, left_out }
}

/// The schedules of `source`, those sending to `ant` when given, with the admin credentials of `nats` for another
/// machine's.
pub async fn read_from(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    source: Source<'_>,
    ant: Option<&UserId>,
) -> Result<Listing> {
    match source {
        Source::Here(path) => read(js, path, ant).await,
        Source::On(machine) => read_on(&Admin::of(nats)?, js, machine, ant).await,
    }
}

/// The listing as a page: the table, then what was left out or not read.
pub fn page(listing: &Listing) -> String {
    let mut page = table(&listing.listed);
    if listing.left_out > 0 {
        let _ = writeln!(page, "{} more schedules left out", listing.left_out);
    }
    if let Some(unread) = &listing.unread {
        let _ = writeln!(page, "last firings not read: {unread}");
    }
    page
}

/// The active reminders and watchers sense keeps for `ant`, one line each, as its memories read by an admin order
/// to the daemon of `machine`, else of the machine that last recorded serving it, show them; none for an ant whose
/// provider keeps none.
pub async fn reminders(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<&Machine>,
    ant: &UserId,
) -> Result<Vec<String>> {
    let memories = crate::admin::memories::read(nats, js, machine.cloned(), ant).await?;
    let Some(provider) = memories.provider else { return Ok(Vec::new()) };
    if let Some(error) = provider.error {
        anyhow::bail!("{error}");
    }
    let entry = provider.memories.into_iter().find(|entry| entry.key == ants_daemon::sense::REMINDERS);
    Ok(entry.map_or_default(|entry| entry.content.lines().map(str::to_owned).collect()))
}

/// `ants schedule list`: the schedules of the config file of `source`, with `ant` those sending to it and the ant's
/// reminders and watchers, as a table or with `json` one JSON object per line.
pub async fn list(nats: &Nats<'_>, source: Source<'_>, ant: Option<&UserId>, json: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-schedules").await?;
    let listing = read_from(nats, &js, source, ant).await?;
    let machine = match source {
        Source::Here(_) => None,
        Source::On(machine) => Some(machine),
    };
    let reminders = match ant {
        Some(ant) => Some(reminders(nats, &js, machine, ant).await.map_err(|e| format!("{e:#}"))),
        None => None,
    };
    if json {
        for schedule in &listing.listed {
            println!("{}", json_line(schedule));
        }
        if listing.left_out > 0 {
            println!("{}", json!({"kind": "schedules_left_out", "count": listing.left_out}));
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
    print!("{}", page(&listing));
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
