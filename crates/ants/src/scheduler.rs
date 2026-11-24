//! Schedules of `ants daemon`: the `[[schedule]]` entries of its config file ([`Schedule`]), each sending its text on
//! its timetable as a new thread, to an ant or a channel, or asking an ant to start a saved trail on one
//! ([`ants_proto::Envelope::trail_start`]).
//!
//! A schedule sends as a NATS user of its own, with credentials from `ants auth user NAME --webhook --to ANT` or
//! `--channel CHANNEL`, exactly as a webhook does ([`crate::webhook`]): its messages are a webhook's, delivered once
//! per firing. Several daemons serving the same file, or one restarted, never send a firing twice: each firing has
//! its key `<schedule>.<unix time>` in [`ants_proto::WEBHOOKS_BUCKET`] and its `Nats-Msg-Id`. A firing goes out up to
//! [`GRACE`] late, for a daemon restarted or a hub briefly away. One missed by more is skipped, unless the schedule
//! sets `catch_up`: then, once a daemon runs and reaches the hub again, the latest firing missed goes out late, under
//! the same key, and the older ones are skipped. A firing for an ant paused, as its colony entry says, is missed
//! alike: skipped, or with `catch_up` held, a later one held in its place, until the ant resumes and it goes out late.

use std::fmt::Write as _;
use std::time::Duration;

use ants_daemon::schedule::Schedule;
use ants_proto::{JsDomain, WEBHOOK_RETENTION, webhook_seen_key};
use anyhow::{Context as _, Result};
use chrono::{DateTime, TimeDelta, Utc};

use crate::webhook::{Bus, Delivered, Delivery, check_creds};

/// How late a firing may still go out.
const GRACE: Duration = Duration::from_secs(60);
/// How long a firing the hub did not take waits before trying again, within [`GRACE`].
const RETRY: Duration = Duration::from_secs(5);
/// Longest sleep before looking at the clock again, so a suspended machine or a clock set anew is noticed.
const NAP: Duration = Duration::from_secs(30);

/// The schedules with `nats` and `js_domain` where they set none, once their credentials are checked: they name the
/// schedule, are no human's and may send to its destination.
pub fn resolve(schedules: Vec<Schedule>, nats: &str, js_domain: Option<&JsDomain>) -> Result<Vec<Schedule>> {
    schedules
        .into_iter()
        .map(|mut schedule| {
            schedule.nats.get_or_insert_with(|| nats.to_owned());
            if schedule.js_domain.is_none() {
                schedule.js_domain = js_domain.cloned();
            }
            check_creds("schedule", &schedule.name, schedule.creds.as_deref(), &schedule.destination)
                .with_context(|| format!("schedule {}", schedule.name))?;
            Ok(schedule)
        })
        .collect()
}

/// Send `schedule`'s firings until stopped, connected as the schedule, waiting for the hub as long as it takes.
pub async fn run(schedule: Schedule) -> Result<()> {
    let name = &schedule.name;
    let nats = schedule.nats.as_deref().context("no NATS server")?;
    let bus = Bus::connect(name, schedule.creds.as_deref(), nats, schedule.js_domain.as_ref(), "ants-schedule").await?;
    tracing::info!(schedule = %name, when = %schedule.when, to = ?schedule.destination, "schedule ready");
    let grace = TimeDelta::from_std(GRACE)?;
    let now = Utc::now();
    let mut cursor = now - grace;
    // The firing waiting for its paused ant to resume, with `catch_up`.
    let mut held = None;
    if let Some(missed) = missed(&schedule, DateTime::<Utc>::MIN_UTC, now) {
        held = send_unless_paused(&bus, &schedule, missed).await;
    }
    loop {
        let Some(due) = schedule.when.next_after(cursor) else {
            if let Some(held) = held {
                tracing::warn!(schedule = %name, due = %held, "the schedule fires no more; its firing held waits for the ant to resume");
                while paused(&bus, &schedule).await {
                    tokio::time::sleep(NAP).await;
                }
                send(&bus, &schedule, held).await;
            }
            tracing::warn!(schedule = %name, "the schedule fires no more");
            return Ok(());
        };
        if let Some(waiting) = held {
            // Looking again every nap whether the ant resumed, until the next firing is due.
            if Utc::now() < due {
                tokio::time::sleep((due - Utc::now()).to_std().unwrap_or_default().min(NAP)).await;
            }
            if !paused(&bus, &schedule).await {
                send(&bus, &schedule, waiting).await;
                held = None;
            }
            if Utc::now() < due {
                continue;
            }
        }
        sleep_until(due).await;
        let now = Utc::now();
        if now - due > grace {
            match missed(&schedule, cursor, now) {
                Some(missed) => held = send_unless_paused(&bus, &schedule, missed).await.or(held),
                None => tracing::warn!(schedule = %name, %due, "skipping a firing missed by more than {GRACE:?}"),
            }
            cursor = due.max(now - grace);
            continue;
        }
        held = send_unless_paused(&bus, &schedule, due).await.or(held);
        cursor = due;
    }
}

/// Send the firing due at `due` unless the schedule's ant is paused: then return it to hold with `catch_up`, else
/// skip it.
async fn send_unless_paused(bus: &Bus, schedule: &Schedule, due: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if !paused(bus, schedule).await {
        send(bus, schedule, due).await;
        return None;
    }
    let name = &schedule.name;
    if schedule.catch_up {
        tracing::info!(schedule = %name, %due, "holding a firing until its ant resumes");
        return Some(due);
    }
    tracing::warn!(schedule = %name, %due, "skipping a firing: its ant is paused");
    None
}

/// Whether the ant the schedule sends to is paused; a schedule to a channel never waits. An entry that cannot be read
/// counts as not paused.
async fn paused(bus: &Bus, schedule: &Schedule) -> bool {
    let ants_proto::Destination::Ant(ant) = &schedule.destination else { return false };
    bus.paused(ant).await.unwrap_or_else(|e| {
        tracing::debug!(schedule = %schedule.name, error = format!("{e:#}"), "reading whether the ant is paused");
        false
    })
}

/// With `catch_up`, the last firing after `after` and up to `now` (looking back [`WEBHOOK_RETENTION`] at most, so its
/// key, if sent, is still in [`ants_proto::WEBHOOKS_BUCKET`]), if more than [`GRACE`] late.
fn missed(schedule: &Schedule, after: DateTime<Utc>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if !schedule.catch_up {
        return None;
    }
    let after = after.max(now - TimeDelta::from_std(WEBHOOK_RETENTION).ok()?);
    let last = schedule.when.last_in(after, now)?;
    (now - last > TimeDelta::from_std(GRACE).ok()?).then_some(last)
}

/// Wait for `at` on the wall clock.
async fn sleep_until(at: DateTime<Utc>) {
    while let Ok(left) = (at - Utc::now()).to_std()
        && !left.is_zero()
    {
        tokio::time::sleep(left.min(NAP)).await;
    }
}

/// Send the firing due at `due`, trying again while it is at most [`GRACE`] late or, with `catch_up`, until the next
/// firing is due.
async fn send(bus: &Bus, schedule: &Schedule, due: DateTime<Utc>) {
    let name = &schedule.name;
    let grace = TimeDelta::from_std(GRACE).unwrap_or_default();
    let deadline = if schedule.catch_up { schedule.when.next_after(due) } else { Some(due + grace) };
    loop {
        let late = Utc::now() - due > grace;
        match fire(bus, schedule, due, late).await {
            Ok(Delivered { thread, duplicate: false }) => {
                tracing::info!(schedule = %name, %due, late, thread = ?thread, "schedule fired");
                return;
            }
            Ok(Delivered { thread, duplicate: true }) => {
                tracing::info!(schedule = %name, %due, thread = ?thread, "already fired");
                return;
            }
            Err(e)
                if deadline
                    .is_some_and(|deadline| Utc::now() + TimeDelta::from_std(RETRY).unwrap_or_default() > deadline) =>
            {
                tracing::warn!(schedule = %name, %due, error = format!("{e:#}"), "skipping a firing the hub did not take in time");
                return;
            }
            Err(e) => {
                tracing::warn!(schedule = %name, %due, error = format!("{e:#}"), retry_in = ?RETRY, "firing");
                tokio::time::sleep(RETRY).await;
            }
        }
    }
}

/// Send the firing due at `due` as a new thread, unless it went out already; a `late` one says when it was due.
async fn fire(bus: &Bus, schedule: &Schedule, due: DateTime<Utc>, late: bool) -> Result<Delivered> {
    let at = due.timestamp().to_string();
    let mut text = schedule.text.clone();
    if late {
        let note = ants_daemon::sense::when::late_note(due, Utc::now()).unwrap_or_default();
        let _ = write!(text, "\n\n{note}");
    }
    bus.deliver(Delivery {
        from: &schedule.name,
        destination: &schedule.destination,
        mentions: &schedule.mentions,
        owner: schedule.owner.as_ref(),
        allow: schedule.allow.as_deref(),
        key: webhook_seen_key(&schedule.name, &at),
        message_id: format!("schedule.{}.{at}", schedule.name),
        text,
        trail: schedule.trail.as_ref(),
    })
    .await
}

#[cfg(test)]
mod tests;
