//! Schedules of `ants daemon`: the `[[schedule]]` entries of its config file ([`Schedule`]), each sending its text on
//! its timetable as a new thread, to an ant or a channel.
//!
//! A schedule sends as a NATS user of its own, with credentials from `ants auth user NAME --webhook --to ANT` or
//! `--channel CHANNEL`, exactly as a webhook does ([`crate::webhook`]): its messages are a webhook's, delivered once
//! per firing. Several daemons serving the same file, or one restarted, never send a firing twice: each firing has
//! its key `<schedule>.<unix time>` in [`ants_proto::WEBHOOKS_BUCKET`] and its `Nats-Msg-Id`. A firing goes out up to
//! [`GRACE`] late, for a daemon restarted or a hub briefly away. One missed by more is skipped, unless the schedule
//! sets `catch_up`: then, once a daemon runs and reaches the hub again, the latest firing missed goes out late, under
//! the same key, and the older ones are skipped.

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
    if let Some(missed) = missed(&schedule, DateTime::<Utc>::MIN_UTC, now) {
        send(&bus, &schedule, missed).await;
    }
    loop {
        let Some(due) = schedule.when.next_after(cursor) else {
            tracing::warn!(schedule = %name, "the schedule fires no more");
            return Ok(());
        };
        sleep_until(due).await;
        let now = Utc::now();
        if now - due > grace {
            match missed(&schedule, cursor, now) {
                Some(missed) => send(&bus, &schedule, missed).await,
                None => tracing::warn!(schedule = %name, %due, "skipping a firing missed by more than {GRACE:?}"),
            }
            cursor = due.max(now - grace);
            continue;
        }
        send(&bus, &schedule, due).await;
        cursor = due;
    }
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
        let _ = write!(text, "\n\n(Sent late: due {}.)", due.format("%Y-%m-%d %H:%M UTC"));
    }
    bus.deliver(Delivery {
        from: &schedule.name,
        destination: &schedule.destination,
        mentions: &schedule.mentions,
        owner: schedule.owner.as_ref(),
        key: webhook_seen_key(&schedule.name, &at),
        message_id: format!("schedule.{}.{at}", schedule.name),
        text,
    })
    .await
}

#[cfg(test)]
mod tests {
    use ants_daemon::bus;
    use ants_daemon::owners::Owners;
    use ants_daemon::schedule::{Mentions, When};
    use ants_proto::{ChannelId, ChannelPost, Destination, Envelope, UserId, WEBHOOKS_BUCKET, subject};
    use ants_proto::{webhook_marker_key, webhook_seen_key};

    use chrono::Timelike as _;

    use super::*;
    use crate::webhook::tests::{creds, hub, stored};

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn schedule(name: &str, creds: Option<std::path::PathBuf>, destination: Destination) -> Schedule {
        Schedule {
            name: p(name),
            creds,
            nats: None,
            js_domain: None,
            when: When::Every(Duration::from_secs(3_600)),
            destination,
            mentions: Mentions::Nobody,
            owner: None,
            text: "check the nightly build".to_owned(),
            catch_up: false,
        }
    }

    #[test]
    fn credentials_must_be_the_schedule_s_own() {
        let dir = ants_test_support::scratch_dir("schedule-creds");
        let out = dir.join("other.creds");
        let to = Destination::Ant(p("dev-a"));
        crate::auth::init(&dir.join("auth")).unwrap();
        let permissions = ants_proto::Permissions::for_webhook(&p("other"), std::slice::from_ref(&to));
        crate::auth::issue_with(&dir.join("auth"), &p("other"), crate::revoke::Kind::Webhook, permissions, &out)
            .unwrap();
        let err = format!("{:#}", resolve(vec![schedule("nightly", Some(out), to)], "nats://hub", None).unwrap_err());
        assert!(err.contains("schedule nightly") && err.contains("are not schedule nightly's"), "{err}");
        assert!(err.contains("ants auth user nightly --webhook --to dev-a"), "{err}");

        let ok = resolve(vec![schedule("nightly", None, Destination::Ant(p("dev-a")))], "nats://hub", None).unwrap();
        assert_eq!(ok[0].nats.as_deref(), Some("nats://hub"));
    }

    #[test]
    fn catch_up_finds_the_latest_missed_firing_within_a_week() {
        let at = |t: &str| DateTime::parse_from_rfc3339(t).unwrap().with_timezone(&Utc);
        let mut hourly = schedule("pulse", None, Destination::Ant(p("dev-a")));
        let never = DateTime::<Utc>::MIN_UTC;
        assert_eq!(missed(&hourly, never, at("2026-11-14T10:20:00Z")), None, "no catch-up unless asked");
        hourly.catch_up = true;
        assert_eq!(missed(&hourly, never, at("2026-11-14T10:20:00Z")), Some(at("2026-11-14T10:00:00Z")));
        assert_eq!(missed(&hourly, never, at("2026-11-14T10:00:59Z")), None, "within a minute it goes out on time");
        assert_eq!(missed(&hourly, at("2026-11-14T10:00:00Z"), at("2026-11-14T10:20:00Z")), None, "after `after` only");
        let mut yearly = hourly;
        yearly.when = When::Cron("@yearly".parse().unwrap());
        assert_eq!(missed(&yearly, never, at("2027-01-07T10:00:00Z")), Some(at("2027-01-01T00:00:00Z")));
        assert_eq!(missed(&yearly, never, at("2027-01-08T00:00:01Z")), None, "a week back at most");
    }

    #[tokio::test]
    async fn a_missed_firing_is_caught_up_once_across_daemons() {
        let Some((server, js, _)) = hub().await else { return };
        let nightly = p("nightly");
        let to = Destination::Ant(p("dev-a"));
        let creds = creds(&server, &nightly, to.clone());
        let mut schedule = schedule("nightly", Some(creds.clone()), to);
        let due = Utc::now() - TimeDelta::minutes(10);
        let due = due.with_second(0).unwrap().with_nanosecond(0).unwrap();
        schedule.when = When::Cron(format!("{} {} * * *", due.minute(), due.hour()).parse().unwrap());
        schedule.nats = Some(server.url.clone());
        schedule.catch_up = true;

        let daemons = [tokio::spawn(run(schedule.clone())), tokio::spawn(run(schedule.clone()))];
        let inbox = subject::inbox(&p("dev-a"), &nightly);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while stored::<Envelope>(&js, &inbox).await.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        daemons.iter().for_each(|daemon| daemon.abort());
        let messages: Vec<Envelope> = stored(&js, &inbox).await;
        let [message] = &messages[..] else { panic!("one message: {messages:?}") };
        let late = format!("check the nightly build\n\n(Sent late: due {}.)", due.format("%Y-%m-%d %H:%M UTC"));
        assert_eq!(message.text, late);
        let bus = Bus::connect(&nightly, Some(&creds), &server.url, None, "test").await.unwrap();
        assert!(fire(&bus, &schedule, due, false).await.unwrap().duplicate, "the same key as an on-time firing");
    }

    #[tokio::test]
    async fn a_firing_goes_out_once_across_daemons_and_restarts() {
        let Some((server, js, _)) = hub().await else { return };
        let nightly = p("nightly");
        let to = Destination::Ant(p("dev-a"));
        let creds = creds(&server, &nightly, to.clone());
        let mut schedule = schedule("nightly", Some(creds.clone()), to);
        schedule.owner = Some(p("alice"));
        let connect = || Bus::connect(&nightly, Some(&creds), &server.url, None, "test");
        let (a, b) = (connect().await.unwrap(), connect().await.unwrap());
        let due = DateTime::from_timestamp(1_800_000_000, 0).unwrap();

        let (first, second) = tokio::join!(fire(&a, &schedule, due, false), fire(&b, &schedule, due, false));
        let (first, second) = (first.unwrap(), second.unwrap());
        assert_eq!([first.duplicate, second.duplicate].iter().filter(|d| !**d).count(), 1, "one daemon sends it");
        let again = fire(&connect().await.unwrap(), &schedule, due, false).await.unwrap();
        assert!(again.duplicate, "a restarted daemon finds it sent");

        // Within the stream's duplicate window, its Nats-Msg-Id keeps it once even if the bucket forgot it.
        let (_, own) = bus::connect(&server.url, Some(&creds), None, &nightly, "test").await.unwrap();
        let seen = ants_daemon::direct_kv::DirectKv::new(&own, None, WEBHOOKS_BUCKET).await.unwrap();
        seen.delete(webhook_seen_key(&nightly, "1800000000")).await.unwrap();
        assert!(fire(&b, &schedule, due, false).await.unwrap().duplicate);

        let next = fire(&a, &schedule, due + TimeDelta::hours(1), false).await.unwrap();
        assert!(!next.duplicate, "the next firing is a new thread");
        let messages: Vec<Envelope> = stored(&js, &subject::inbox(&p("dev-a"), &nightly)).await;
        let [message, later] = &messages[..] else { panic!("two messages: {messages:?}") };
        assert_ne!(message.thread_id, later.thread_id);
        assert_eq!((message.text.as_str(), message.webhook), ("check the nightly build", true));
        assert_eq!(message.owner.as_ref(), Some(&p("alice")));
        let recorded = Owners::new(js.clone()).recorded(&message.thread_id, &message.to).await.unwrap();
        assert_eq!(recorded, Some(p("alice")));
        let marker =
            seen.get(webhook_marker_key(&nightly)).await.unwrap().expect("daemons take its messages as a webhook's");
        let marker: ants_proto::Serving = serde_json::from_slice(&marker).unwrap();
        assert_eq!(marker.version, ants_proto::ANTS_VERSION, "it publishes its version");
    }

    #[tokio::test]
    async fn a_channel_schedule_mentions_the_members() {
        let Some((server, js, _)) = hub().await else { return };
        let ops: ChannelId = "ops".parse().unwrap();
        let standup = p("standup");
        let creds = creds(&server, &standup, Destination::Channel(ops.clone()));
        let mut schedule = schedule("standup", Some(creds.clone()), Destination::Channel(ops.clone()));
        schedule.mentions = Mentions::Members;
        let bus = Bus::connect(&standup, Some(&creds), &server.url, None, "test").await.unwrap();
        let due = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        assert!(!fire(&bus, &schedule, due, false).await.unwrap().duplicate);
        let posts: Vec<ChannelPost> = stored(&js, &subject::chan(&ops, &standup)).await;
        let [post] = &posts[..] else { panic!("one post: {posts:?}") };
        assert!(post.webhook && post.owner.is_none());
        assert_eq!(post.mentions, [p("dev-a"), p("dev-b")]);
    }
}
