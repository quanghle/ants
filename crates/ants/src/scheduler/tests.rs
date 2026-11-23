//! Tests of schedules: their own credentials, catch-up of missed firings, firings sent once across daemons and
//! restarts, trail schedules, and channel schedules' mentions.

use ants_daemon::bus;
use ants_daemon::records::owners::Owners;
use ants_daemon::schedule::{Mentions, When};
use ants_proto::{ChannelId, ChannelPost, Destination, Envelope, UserId, WEBHOOKS_BUCKET, subject};
use ants_proto::{webhook_marker_key, webhook_seen_key};

use chrono::Timelike as _;

use super::*;
use crate::test_support::webhook::{creds, hub, stored};

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
        trail: None,
        catch_up: false,
        allow: None,
    }
}

#[test]
fn credentials_must_be_the_schedule_s_own() {
    let dir = ants_test_support::scratch_dir("schedule-creds");
    let out = dir.join("other.creds");
    let to = Destination::Ant(p("dev-a"));
    crate::auth::init(&dir.join("auth")).unwrap();
    let permissions = ants_proto::Permissions::for_webhook(&p("other"), std::slice::from_ref(&to));
    crate::auth::issue_with(
        &dir.join("auth"),
        &p("other"),
        crate::credentials::revoke::Kind::Webhook,
        permissions,
        &out,
    )
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
    let inbox = subject::webhook_inbox(&p("dev-a"), &nightly);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while stored::<Envelope>(&js, &inbox).await.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    daemons.iter().for_each(tokio::task::JoinHandle::abort);
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
    schedule.allow = Some(vec!["read:*".to_owned()]);
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
    let seen = ants_daemon::records::direct_kv::DirectKv::new(&own, None, WEBHOOKS_BUCKET).await.unwrap();
    seen.delete(webhook_seen_key(&nightly, "1800000000")).await.unwrap();
    assert!(fire(&b, &schedule, due, false).await.unwrap().duplicate);

    let next = fire(&a, &schedule, due + TimeDelta::hours(1), false).await.unwrap();
    assert!(!next.duplicate, "the next firing is a new thread");
    let messages: Vec<Envelope> = stored(&js, &subject::webhook_inbox(&p("dev-a"), &nightly)).await;
    let [message, later] = &messages[..] else { panic!("two messages: {messages:?}") };
    assert_ne!(message.thread_id, later.thread_id);
    assert_eq!((message.text.as_str(), message.webhook), ("check the nightly build", true));
    assert_eq!(message.owner.as_ref(), Some(&p("alice")));
    assert_eq!(message.allow, schedule.allow, "its threads are narrowed to its rules");
    let recorded = Owners::new(js.clone()).recorded(&message.thread_id, &message.to).await.unwrap();
    assert_eq!(recorded, Some(p("alice")));
    let marker = seen.get(webhook_marker_key(&nightly)).await.unwrap().expect("it publishes its version");
    let marker: ants_proto::Serving = serde_json::from_slice(&marker).unwrap();
    assert_eq!(marker.version, ants_proto::ANTS_VERSION, "it publishes its version");
}

#[tokio::test]
async fn a_trail_schedule_s_firing_asks_its_ant_for_a_run() {
    let Some((server, js, _)) = hub().await else { return };
    let weekly = p("weekly");
    let to = Destination::Ant(p("dev-a"));
    let creds = creds(&server, &weekly, to.clone());
    let mut schedule = schedule("weekly", Some(creds.clone()), to);
    let start = ants_proto::TrailStart {
        trail: "review".parse().unwrap(),
        args: serde_json::json!({"repo": "app"}),
        limits: ants_proto::TrailLimits::DEFAULT,
    };
    schedule.trail = Some(start.clone());
    schedule.owner = Some(p("alice"));
    let bus = Bus::connect(&weekly, Some(&creds), &server.url, None, "test").await.unwrap();
    let due = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
    assert!(!fire(&bus, &schedule, due, false).await.unwrap().duplicate);
    assert!(fire(&bus, &schedule, due, false).await.unwrap().duplicate, "a firing starts one run");
    let messages: Vec<Envelope> = stored(&js, &subject::webhook_inbox(&p("dev-a"), &weekly)).await;
    let [message] = &messages[..] else { panic!("one message: {messages:?}") };
    assert_eq!((message.trail_start.as_deref(), message.webhook), (Some(&start), true));
    assert_eq!(message.owner.as_ref(), Some(&p("alice")));
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
    let posts: Vec<ChannelPost> = stored(&js, &subject::webhook_chan(&ops, &standup)).await;
    let [post] = &posts[..] else { panic!("one post: {posts:?}") };
    assert!(post.webhook && post.owner.is_none() && post.allow.is_none(), "receiving daemons narrow it");
    assert_eq!(post.mentions, [p("dev-a"), p("dev-b")]);
}
