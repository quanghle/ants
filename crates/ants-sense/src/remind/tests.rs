//! Tests of reminders: pending deliveries across restarts, refires and snoozes, recurring deadlines, condition
//! reservations, a command condition's backoff on its digest, the active limit, triggers refused, trusted origins,
//! and stale revisions.

use std::sync::Arc;

use ants_proto::provider::{CallContext, Starter};
use ants_proto::{SenseId, UserId};

use super::watch::UNCHANGED_BEFORE_BACKOFF;
use super::*;
use crate::items::Values;
use crate::testing::{self, One, SharedLog, context, fixture, human, store};

async fn open_store(log: SharedLog, sense: SenseId) -> Arc<dyn Items> {
    Arc::new(store(log, sense, "dev-a".parse().unwrap()).await)
}

fn now() -> DateTime<Utc> {
    "2025-01-01T12:00:00Z".parse().unwrap()
}

fn call(at: DateTime<Utc>, arguments: Value) -> ToolCall {
    let context =
        CallContext { thread: "t1".parse().unwrap(), model: None, machine: "m1".into(), at, ..context(human()) };
    testing::call("remind", arguments, context)
}

async fn create(sense: &Sense, trigger: Value) -> ItemId {
    let answer = remind(sense, &call(now(), json!({"title":"do the work","trigger":trigger}))).await;
    assert!(!answer.is_error, "{answer:?}");
    answer.json["reminder"]["id"].as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn pending_delivery_survives_restart_retries_and_confirmation_retries() {
    let (log, sense_id) = (SharedLog::default(), SenseId::new());
    let items = open_store(log.clone(), sense_id.clone()).await;
    let sense = Sense::with_stores(Arc::new(One::new(items.clone())));
    let id = create(&sense, json!({"kind":"at","at":now()})).await;
    let first = prepare_firing(&*items, &id, now(), "time due").await.unwrap().unwrap();
    assert_eq!(first.id, format!("{id}:1"));
    let retry =
        prepare_firing(&*items, &id, now() + Duration::minutes(20), "different retry reason").await.unwrap().unwrap();
    assert_eq!(first, retry);
    drop(sense);
    drop(items);
    let restarted = open_store(log, sense_id).await;
    let retry = prepare_firing(&*restarted, &id, now() + Duration::minutes(30), "takeover").await.unwrap().unwrap();
    assert_eq!(first, retry);
    assert_eq!(pending_work(&*restarted).await.unwrap().as_slice(), std::slice::from_ref(&first));
    let delivered = now() + Duration::minutes(30);
    confirm_delivered(&*restarted, &id, &first.id, delivered).await.unwrap();
    let revision = get(&*restarted, &id).await.unwrap().revision;
    confirm_delivered(&*restarted, &id, &first.id, delivered + Duration::minutes(5)).await.unwrap();
    assert_eq!(get(&*restarted, &id).await.unwrap().revision, revision);
    assert_eq!(next_due(&*restarted, delivered).await.unwrap(), Some(delivered + Duration::hours(1)));
    assert!(prepare_firing(&*restarted, &id, delivered, "too early").await.unwrap().is_none());
    assert!(pending_work(&*restarted).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_malformed_reminder_hides_itself_alone_from_listings() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let good = create(&sense, json!({"kind":"at","at":now()})).await;
    let malformed = Values::from_iter([("status".to_owned(), json!("active")), ("title".to_owned(), json!(5))]);
    items.write(&[Write::Create { id: ItemId::new(), kind: KIND.into(), values: malformed }], "t1").await.unwrap();
    let firing = prepare_firing(&*items, &good, now(), "time due").await.unwrap().unwrap();
    assert_eq!(pending_work(&*items).await.unwrap(), std::slice::from_ref(&firing));
    assert_eq!(
        due(&*items, now()).await.unwrap().into_iter().map(|due| due.id).collect::<Vec<_>>(),
        std::slice::from_ref(&good)
    );
    let [fired, upcoming] = brief(&*items, now()).await.unwrap();
    assert_eq!(fired.lines.len(), 1, "{:?}", fired.lines);
    assert!(fired.lines[0].starts_with(&format!("- {good} ")), "{}", fired.lines[0]);
    assert!(fired.lines[0].contains(&format!("firing {} at", firing.id)), "{}", fired.lines[0]);
    assert!(upcoming.lines.is_empty(), "{:?}", upcoming.lines);
    create(&sense, json!({"kind":"after","seconds":3600})).await;
}

#[tokio::test]
async fn refires_are_hour_apart_capped_at_three_replace_the_one_before_and_a_late_snooze_says_so_with_a_new_id() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, json!({"kind":"after","seconds":60})).await;
    let first_due = now() + Duration::minutes(1);
    assert!(due(&*items, now()).await.unwrap().is_empty());
    for firing in 1..=4 {
        let at = first_due + Duration::hours(firing - 1);
        let prepared = prepare_firing(&*items, &id, at, "time").await.unwrap().unwrap();
        assert_eq!(prepared.id, format!("{id}:{firing}"));
        let replaced = (firing > 1).then(|| format!("{id}:{}", firing - 1));
        assert_eq!(prepared.replaces, replaced, "a refire replaces the firing before it");
        assert!(!prepared.watched());
        assert_eq!(prepared.reason, "time", "on time, and a refire is late by design");
        confirm_delivered(&*items, &id, &prepared.id, at).await.unwrap();
        assert!(prepare_firing(&*items, &id, at, "early").await.unwrap().is_none());
    }
    assert!(next_due(&*items, first_due + Duration::hours(4)).await.unwrap().is_none());
    let postponed =
        remind(&sense, &call(first_due + Duration::hours(4), json!({"action":"postpone","id":id,"after_seconds":60})))
            .await;
    assert!(!postponed.is_error, "{postponed:?}");
    let firing = prepare_firing(&*items, &id, first_due + Duration::hours(5), "snoozed").await.unwrap().unwrap();
    assert_eq!(firing.id, format!("{id}:5"));
    let due = (first_due + Duration::hours(4) + Duration::seconds(60)).format("%Y-%m-%d %H:%M UTC");
    assert_eq!(firing.reason, format!("snoozed (Sent late: due {due}.)"), "fired past its time");
    let acknowledged = remind(&sense, &call(first_due + Duration::hours(5), json!({"action":"ack","id":id}))).await;
    assert!(!acknowledged.is_error, "{acknowledged:?}");
    assert!(prepare_firing(&*items, &id, first_due + Duration::hours(6), "after ack").await.unwrap().is_none());
}

#[tokio::test]
async fn recurring_utc_deadlines_advance_after_ack_or_exhaustion_and_persist() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, json!({"kind":"daily","time":"13:15","timezone":"UTC"})).await;
    let at = now() + Duration::minutes(75);
    assert_eq!(next_due(&*items, now()).await.unwrap(), Some(at));
    let first = prepare_firing(&*items, &id, at, "daily").await.unwrap().unwrap();
    confirm_delivered(&*items, &id, &first.id, at).await.unwrap();
    assert!(!remind(&sense, &call(at, json!({"action":"ack","id":id}))).await.is_error);
    assert_eq!(next_due(&*items, at).await.unwrap(), Some(at + Duration::days(1)));
    for number in 2..=5 {
        let due = at + Duration::days(1) + Duration::hours(number - 2);
        let firing = prepare_firing(&*items, &id, due, "daily").await.unwrap().unwrap();
        assert_eq!(firing.id, format!("{id}:{number}"));
        confirm_delivered(&*items, &id, &firing.id, due).await.unwrap();
    }
    assert_eq!(next_due(&*items, at + Duration::days(1)).await.unwrap(), Some(at + Duration::days(2)));
    let weekly = Trigger::Weekly { weekday: "wed".into(), time: "12:00".into(), timezone: "UTC".into() };
    assert_eq!(weekly.first(now()).unwrap(), now() + Duration::days(7));
}

#[tokio::test]
async fn condition_reservations_fence_probes_and_persist_backoff_and_expiry() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, json!({"kind":"condition","condition":{"kind":"bd","id":"bd-1","state":"closed"}})).await;
    let check = reserve_condition(&*items, &id, now()).await.unwrap().unwrap();
    assert!(matches!(check.condition, Condition::Bd { .. }));
    assert!(reserve_condition(&*items, &id, now()).await.unwrap().is_none());
    assert_eq!(next_due(&*items, now()).await.unwrap(), Some(now() + Duration::minutes(5)));
    complete_condition(&*items, &check, now(), false, "still open", None).await.unwrap();
    for check_number in 1..10 {
        let at = now() + Duration::minutes(check_number * 5);
        assert!(condition_checked(&*items, &id, at, false, "unchanged").await.unwrap().is_none());
    }
    assert_eq!(next_due(&*items, now()).await.unwrap(), Some(now() + Duration::minutes(75)));
    let stale = reserve_condition(&*items, &id, now() + Duration::minutes(75)).await.unwrap().unwrap();
    assert!(!remind(&sense, &call(now() + Duration::minutes(75), json!({"action":"drop","id":id}))).await.is_error);
    assert!(
        complete_condition(&*items, &stale, now() + Duration::minutes(75), true, "closed", None)
            .await
            .unwrap()
            .is_none()
    );
    let expiring =
        create(&sense, json!({"kind":"condition","condition":{"kind":"file","path":"result","event":"appeared"}}))
            .await;
    assert!(condition_checked(&*items, &expiring, now() + Duration::days(30), true, "exists").await.unwrap().is_none());
    assert_eq!(get(&*items, &expiring).await.unwrap().text("status"), "expired");
    let renewed = remind(&sense, &call(now() + Duration::days(31), json!({"action":"renew","id":expiring}))).await;
    assert!(!renewed.is_error, "{renewed:?}");
    assert_eq!(get(&*items, &expiring).await.unwrap().text("status"), "active");
}

#[tokio::test]
async fn condition_reservation_survives_takeover_and_rejects_an_earlier_probe() {
    let (log, sense_id) = (SharedLog::default(), SenseId::new());
    let first = open_store(log.clone(), sense_id.clone()).await;
    let sense = Sense::with_stores(Arc::new(One::new(first.clone())));
    let id = create(&sense, json!({"kind":"condition","condition":{"kind":"command","command":"true"}})).await;
    let earlier = reserve_condition(&*first, &id, now()).await.unwrap().unwrap();
    let takeover = open_store(log, sense_id).await;
    assert!(reserve_condition(&*takeover, &id, now()).await.unwrap().is_none());
    let later = reserve_condition(&*takeover, &id, now() + Duration::minutes(1)).await.unwrap().unwrap();
    assert!(complete_condition(&*takeover, &earlier, now(), true, "stale", None).await.unwrap().is_none());
    let firing = complete_condition(
        &*takeover,
        &later,
        now() + Duration::minutes(1),
        true,
        "command succeeded",
        Some(json!({"stdout":"never store command output"})),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(firing.id, format!("{id}:1"));
    assert!(!get(&*takeover, &id).await.unwrap().values.contains_key("observation"));
}

fn command_digest(stdout: &str, status: i32) -> Value {
    let mut digest = CommandDigest::default();
    digest.update(stdout.as_bytes());
    digest.finish(Some(status))
}

/// Check command condition `id` when due, seeing `observation`: its unchanged checks then and the wait for the next.
async fn check_command(items: &dyn Items, id: &ItemId, observation: Option<Value>) -> (u64, Duration) {
    let at = Reminder::of(&get(items, id).await.unwrap()).unwrap().next_due.unwrap();
    assert!(condition_observed(items, id, at, false, "not yet", observation).await.unwrap().is_none());
    let reminder = Reminder::of(&get(items, id).await.unwrap()).unwrap();
    (reminder.unchanged_checks.unwrap(), reminder.next_due.unwrap() - at)
}

#[tokio::test]
async fn a_command_condition_backs_off_only_while_its_stdout_and_exit_status_stay_the_same() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, json!({"kind":"condition","condition":{"kind":"command","command":"pr-blocked 7"}})).await;
    let minute = Duration::minutes(1);
    let seen = |stdout, status| Some(command_digest(stdout, status));
    assert_eq!(check_command(&*items, &id, seen("ci: pending", 1)).await, (0, minute), "the first check");
    for check in 1..30 {
        let stdout = if check % 2 == 0 { "ci: pending" } else { "ci: running" };
        assert_eq!(check_command(&*items, &id, seen(stdout, 1)).await, (0, minute), "a changing stdout");
    }
    assert_eq!(check_command(&*items, &id, seen("ci: running", 2)).await, (0, minute), "another exit status");
    for checks in 1..10 {
        assert_eq!(check_command(&*items, &id, seen("ci: running", 2)).await, (checks, minute));
    }
    let backoff = Duration::minutes(30);
    assert_eq!(check_command(&*items, &id, seen("ci: running", 2)).await, (10, backoff), "ten unchanged checks");
    let reminder = Reminder::of(&get(&*items, &id).await.unwrap()).unwrap();
    assert_eq!(reminder.observation, seen("ci: running", 2), "only the digest is kept");
    assert_eq!(check_command(&*items, &id, seen("ci: failed", 2)).await, (0, minute), "a change");
    assert_eq!(check_command(&*items, &id, None).await, (1, minute), "a failed check sees nothing");
    let output = Some(json!({"digest": "ci: failed"}));
    assert_eq!(check_command(&*items, &id, output).await, (2, minute), "an observation of output is dropped");
    let reminder = Reminder::of(&get(&*items, &id).await.unwrap()).unwrap();
    assert_eq!(reminder.observation, seen("ci: failed", 2));
}

#[tokio::test]
async fn matched_conditions_retry_stably_and_snooze_does_not_recheck_the_condition() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id =
        create(&sense, json!({"kind":"condition","condition":{"kind":"file","path":"result","event":"changed"}})).await;
    let baseline = json!({"digest":"first"});
    condition_observed(&*items, &id, now(), false, "baseline", Some(baseline.clone())).await.unwrap();
    let at = now() + Duration::minutes(1);
    let firing =
        condition_observed(&*items, &id, at, true, "changed", Some(json!({"digest":"second"}))).await.unwrap().unwrap();
    assert_eq!(condition_checked(&*items, &id, at, true, "retry").await.unwrap().unwrap(), firing);
    let postponed = remind(&sense, &call(at, json!({"action":"postpone","id":id,"after_seconds":60}))).await;
    assert!(!postponed.is_error, "{postponed:?}");
    let due = due(&*items, at + Duration::minutes(1)).await.unwrap();
    assert_eq!(due[0].condition, None);
    let next = prepare_firing(&*items, &id, at + Duration::minutes(1), "snooze").await.unwrap().unwrap();
    assert_ne!(next.id, firing.id);
}

#[tokio::test]
async fn active_limit_includes_promises_and_renewals_and_resolution_releases_a_slot() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let mut ids = Vec::new();
    for _ in 0..MAX_ACTIVE {
        ids.push(create(&sense, json!({"kind":"after","seconds":60})).await);
    }
    let too_many =
        remind(&sense, &call(now(), json!({"title":"overflow","trigger":{"kind":"after","seconds":60}}))).await;
    assert_eq!(too_many.json["error"], "too_many", "{too_many:?}");
    let promise =
        crate::promise::promise(&sense, &call(now(), json!({"title":"commitment","promisee":"alice","due":now()})))
            .await;
    assert_eq!(promise.json["error"], "too_many", "{promise:?}");
    assert!(items.list(crate::promise::KIND, 100).await.unwrap().is_empty(), "batch rejected atomically");
    assert!(!remind(&sense, &call(now(), json!({"action":"drop","id":ids[0]}))).await.is_error);
    let commitment =
        crate::promise::promise(&sense, &call(now(), json!({"title":"commitment","promisee":"alice","due":now()})))
            .await;
    assert!(!commitment.is_error, "{commitment:?}");
    let promise: ItemId = commitment.json["promise"]["id"].as_str().unwrap().parse().unwrap();
    let reminder: ItemId = commitment.json["promise"]["values"]["reminder"].as_str().unwrap().parse().unwrap();
    assert_eq!(items.history(&promise).await.unwrap()[0].sequence, items.history(&reminder).await.unwrap()[0].sequence);
    let renegotiated = crate::promise::promise(
        &sense,
        &call(now(), json!({"action":"renegotiate","id":promise,"due":now() + Duration::hours(1)})),
    )
    .await;
    assert!(!renegotiated.is_error, "{renegotiated:?}");
    assert_eq!(get(&*items, &reminder).await.unwrap().text("status"), "dropped");
    assert_eq!(active(&*items, now()).await.unwrap().len(), MAX_ACTIVE);
    let promise: ItemId = renegotiated.json["promise"]["id"].as_str().unwrap().parse().unwrap();
    let reminder: ItemId = renegotiated.json["promise"]["values"]["reminder"].as_str().unwrap().parse().unwrap();
    let kept = crate::promise::promise(&sense, &call(now(), json!({"action":"keep","id":promise}))).await;
    assert!(!kept.is_error, "{kept:?}");
    assert_eq!(get(&*items, &reminder).await.unwrap().text("status"), "dropped");
    let later = now() + Duration::days(31);
    for _ in 0..MAX_ACTIVE {
        let answer = remind(&sense, &call(later, json!({"title":"new","trigger":{"kind":"after","seconds":60}}))).await;
        assert!(!answer.is_error, "{answer:?}");
    }
    let renewal = remind(&sense, &call(later, json!({"action":"renew","id":ids[1]}))).await;
    assert_eq!(renewal.json["error"], "too_many", "{renewal:?}");
    assert_eq!(active(&*items, later).await.unwrap().len(), MAX_ACTIVE);
}

#[tokio::test]
async fn a_trigger_giving_no_time_to_fire_at_is_refused_as_invalid() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let end = DateTime::<Utc>::MAX_UTC - Duration::hours(1);
    let late = remind(&sense, &call(end, json!({"title":"too late","trigger":{"kind":"after","seconds":7200}}))).await;
    assert_eq!(late.json["error"], "invalid_trigger", "{late:?}");
    let due = DateTime::<Utc>::MAX_UTC - Duration::days(1);
    let promised =
        crate::promise::promise(&sense, &call(now(), json!({"title":"forever","promisee":"alice","due":due}))).await;
    assert_eq!(promised.json["error"], "invalid_trigger", "{promised:?}");
    assert!(items.list(KIND, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn simultaneous_creations_share_the_active_limit_across_stores() {
    let (log, sense_id) = (SharedLog::default(), SenseId::new());
    let first = open_store(log.clone(), sense_id.clone()).await;
    let first_sense = Sense::with_stores(Arc::new(One::new(first.clone())));
    for _ in 0..MAX_ACTIVE - 1 {
        create(&first_sense, json!({"kind":"after","seconds":60})).await;
    }
    let second = open_store(log, sense_id).await;
    let second_sense = Sense::with_stores(Arc::new(One::new(second)));
    let args = call(now(), json!({"title":"last slot","trigger":{"kind":"after","seconds":60}}));
    let (a, b) = tokio::join!(remind(&first_sense, &args), remind(&second_sense, &args));
    assert_ne!(a.is_error, b.is_error, "{a:?} {b:?}");
    first.sync().await.unwrap();
    assert_eq!(active(&*first, now()).await.unwrap().len(), MAX_ACTIVE);
}

#[tokio::test]
async fn renewal_preserves_a_time_triggers_original_deadline_and_trusted_workdir() {
    struct Workdir;

    impl crate::Workdirs for Workdir {
        fn workdir(&self, _: &CallContext) -> Option<PathBuf> {
            Some("trusted-repo".into())
        }
    }

    let (sense, items) = fixture(SharedLog::default()).await;
    let sense = sense.with_workdirs(Arc::new(Workdir));
    let at = now() + Duration::days(60);
    let id = create(&sense, json!({"kind":"at","at":at})).await;
    assert_eq!(next_due(&*items, now()).await.unwrap(), Some(now() + Duration::days(30)));
    assert!(prepare_firing(&*items, &id, now() + Duration::days(30), "expired").await.unwrap().is_none());
    let renewal = remind(&sense, &call(now() + Duration::days(31), json!({"action":"renew","id":id}))).await;
    assert!(!renewal.is_error, "{renewal:?}");
    assert_eq!(next_due(&*items, now()).await.unwrap(), Some(at));
    assert_eq!(due(&*items, at).await.unwrap()[0].workdir, Some("trusted-repo".into()));
    let acknowledged = remind(&sense, &call(now() + Duration::days(62), json!({"action":"ack","id":id}))).await;
    assert!(!acknowledged.is_error, "{acknowledged:?}");
    assert_eq!(get(&*items, &id).await.unwrap().text("status"), "acknowledged");
}

#[tokio::test]
async fn bad_triggers_are_refused_with_one_terminal_period() {
    let (sense, _) = fixture(SharedLog::default()).await;
    for trigger in [
        json!({"kind":"daily","time":"25:00","timezone":"UTC"}),
        json!({"kind":"weekly","time":"12:00","weekday":"monday","timezone":"UTC"}),
        json!({"kind":"daily","time":"12:00","timezone":"Nowhere/City"}),
        json!({"kind":"at","at":"soon"}),
        json!({"kind":"after","seconds":0}),
        json!({"kind":"condition","condition":{"kind":"gh_pr","repo":"owner/repo","number":0,"state":"merged"}}),
        json!({"kind":"condition","condition":{"kind":"command","command":"true","extra":true}}),
    ] {
        let answer = remind(&sense, &call(now(), json!({"title":"bad","trigger":trigger}))).await;
        assert!(answer.is_error, "{answer:?}");
        assert_eq!(answer.json["error"], "invalid_arguments", "{answer:?}");
        assert!(answer.text.ends_with('.') && !answer.text.ends_with(".."), "{answer:?}");
    }
}

/// Times of day read in the config file's zone, as `[[schedule]]` and `ants remind` read them, each daily or weekly
/// trigger keeping the zone it was made in.
#[tokio::test]
async fn times_are_read_as_ants_reads_them_in_the_config_file_s_time_zone() {
    let (sense, items) = fixture(SharedLog::default()).await;
    sense.read_times_in(&"dev-a".parse().unwrap(), crate::when::zone("Europe/Paris").unwrap());
    let utc = |text: &str| text.parse::<DateTime<Utc>>().unwrap();
    let due = |reminder: Item| Reminder::of(&reminder).unwrap().next_due;
    let daily = create(&sense, json!({"kind":"daily","time":"09:00"})).await;
    assert_eq!(get(&*items, &daily).await.unwrap().values["trigger"]["timezone"], "Europe/Paris");
    assert_eq!(due(get(&*items, &daily).await.unwrap()), Some(utc("2025-01-02T08:00:00Z")));
    let weekly = create(&sense, json!({"kind":"weekly","weekday":"fri","time":"09:00","timezone":"UTC"})).await;
    assert_eq!(due(get(&*items, &weekly).await.unwrap()), Some(utc("2025-01-03T09:00:00Z")));
    let times = [
        ("13:30", "2025-01-01T12:30:00Z"),
        ("2h", "2025-01-01T14:00:00Z"),
        ("2025-01-05T10:00", "2025-01-05T09:00:00Z"),
    ];
    for (text, at) in times {
        let id = create(&sense, json!({"kind":"at","at":text})).await;
        assert_eq!(due(get(&*items, &id).await.unwrap()), Some(utc(at)), "{text}");
    }
    let postpone = json!({"action":"postpone","id":daily,"at":"2025-01-03T10:00"});
    let postponed = remind(&sense, &call(now(), postpone)).await;
    assert!(!postponed.is_error, "{postponed:?}");
    assert_eq!(due(get(&*items, &daily).await.unwrap()), Some(utc("2025-01-03T09:00:00Z")));
}

#[tokio::test]
async fn only_trusted_origins_can_create_or_change_command_triggers() {
    let (sense, _) = fixture(SharedLog::default()).await;
    let command = json!({"kind":"condition","condition":{"kind":"command","command":"true"}});
    let file = json!({"kind":"condition","condition":{"kind":"file","path":"result","event":"appeared"}});
    let command_id = create(&sense, command.clone()).await;
    let file_id = create(&sense, file.clone()).await;
    let user: UserId = "alice".parse().unwrap();
    for (by, trusted) in [
        (Starter::Human { user: user.clone() }, true),
        (Starter::Ant { user: user.clone() }, true),
        (Starter::Provider { name: "test".into() }, true),
        (Starter::Webhook { user: user.clone() }, false),
        (Starter::Review { user }, false),
        (Starter::Unknown, false),
    ] {
        let from_origin = |arguments| {
            let mut request = call(now(), arguments);
            request.context.origin.by = by.clone();
            request
        };
        for arguments in [
            json!({"title":"command","trigger":command}),
            json!({"action":"postpone","id":command_id,"after_seconds":60}),
        ] {
            let answer = remind(&sense, &from_origin(arguments)).await;
            assert_ne!(answer.is_error, trusted, "{by:?}: {answer:?}");
            if !trusted {
                assert_eq!(answer.json["error"], "untrusted", "{by:?}: {answer:?}");
            }
        }
        for arguments in [
            json!({"action":"show","id":command_id}),
            json!({"title":"file","trigger":file}),
            json!({"action":"postpone","id":file_id,"after_seconds":60}),
        ] {
            let answer = remind(&sense, &from_origin(arguments)).await;
            assert!(!answer.is_error, "{by:?}: {answer:?}");
        }
    }
}

#[tokio::test]
async fn stale_revisions_return_the_current_reminder_even_after_acknowledgement() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, json!({"kind":"at","at":now()})).await;
    let current = get(&*items, &id).await.unwrap();
    let stale =
        remind(&sense, &call(now(), json!({"action":"postpone","id":id,"after_seconds":60,"revision":2}))).await;
    assert_eq!(stale.json["error"], "conflict");
    assert_eq!(stale.json["current"], json!(current));
    assert_eq!(stale.json["reminder"], json!(current));
    assert_eq!(get(&*items, &id).await.unwrap(), current);

    let acknowledged = remind(&sense, &call(now(), json!({"action":"ack","id":id,"revision":1}))).await;
    assert!(!acknowledged.is_error, "{acknowledged:?}");
    let current = get(&*items, &id).await.unwrap();
    assert_eq!(current.text("status"), "acknowledged");
    for action in ["ack", "drop", "renew", "show"] {
        let stale = remind(&sense, &call(now(), json!({"action":action,"id":id,"revision":1}))).await;
        assert_eq!(stale.json["error"], "conflict", "{stale:?}");
        assert_eq!(stale.json["current"], json!(current));
        assert_eq!(stale.json["reminder"], json!(current));
    }
    let repeated = remind(&sense, &call(now(), json!({"action":"ack","id":id,"revision":current.revision}))).await;
    assert!(!repeated.is_error, "{repeated:?}");
    assert_eq!(get(&*items, &id).await.unwrap(), current);
}

#[tokio::test]
async fn captured_workdir_provenance_and_source_context_survive_mutations() {
    struct Workdir;

    impl crate::Workdirs for Workdir {
        fn workdir(&self, _: &CallContext) -> Option<PathBuf> {
            Some("trusted-repo".into())
        }
    }

    let (sense, items) = fixture(SharedLog::default()).await;
    let sense = sense.with_workdirs(Arc::new(Workdir));
    let id = create(&sense, json!({"kind":"after","seconds":60})).await;
    let source = call(now(), json!({})).context;
    let before = get(&*items, &id).await.unwrap();
    assert_eq!(before.values["source_context"], json!(source));
    assert_eq!(before.text("workdir_machine"), "m1");
    let mut postponed = call(now(), json!({"action":"postpone","id":id,"after_seconds":600}));
    postponed.context.thread = "t2".parse().unwrap();
    postponed.context.machine = "another-machine".into();
    let result = remind(&sense, &postponed).await;
    assert!(!result.is_error, "{result:?}");
    let after = get(&*items, &id).await.unwrap();
    assert_eq!(after.values["writer"]["machine"], "another-machine");
    assert_eq!(after.text("thread"), source.thread.as_str());
    for name in ["workdir", "workdir_machine", "source_context"] {
        assert_eq!(after.values[name], before.values[name]);
    }

    let promised =
        crate::promise::promise(&sense, &call(now(), json!({"title":"promise","promisee":"alice","due":now()}))).await;
    assert!(!promised.is_error, "{promised:?}");
    let reminder: ItemId = promised.json["promise"]["values"]["reminder"].as_str().unwrap().parse().unwrap();
    let reminder = get(&*items, &reminder).await.unwrap();
    assert_eq!(reminder.text("workdir_machine"), "m1");
    assert_eq!(reminder.text("workdir"), "trusted-repo");
    assert_eq!(reminder.values["source_context"], json!(source));
    assert_eq!(reminder.values["writer"], json!(source));
}

fn watcher(first: bool) -> Value {
    json!({"kind":"watch","run":{"kind":"command","command":"gh pr checks 7"},"interval_seconds":60,"first":first})
}

async fn check(items: &dyn Items, id: &ItemId, at: DateTime<Utc>, checked: Checked) -> Option<Firing> {
    let check = reserve_watch(items, id, at).await.unwrap().expect("due");
    complete_watch(items, &check, at, checked).await.unwrap()
}

fn seen(digest: &str, done: bool) -> Checked {
    Checked::Seen { digest: digest.into(), done }
}

#[tokio::test]
async fn a_watcher_fires_on_each_change_replacing_its_last_firing_and_retires_once_done() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, watcher(false)).await;
    let due = due(&*items, now()).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].watch.as_ref().map(Watch::interval), Some(60));
    assert!(prepare_firing(&*items, &id, now(), "time due").await.unwrap().is_none(), "a watcher fires on checks");

    assert!(check(&*items, &id, now(), seen("a", false)).await.is_none(), "the first check sets the baseline");
    assert!(reserve_watch(&*items, &id, now() + Duration::seconds(59)).await.unwrap().is_none());
    let at = now() + Duration::seconds(60);
    assert!(check(&*items, &id, at, seen("a", false)).await.is_none(), "unchanged");
    assert_eq!(Reminder::of(&get(&*items, &id).await.unwrap()).unwrap().unchanged_checks, Some(1));

    let at = at + Duration::seconds(60);
    let first = check(&*items, &id, at, seen("b", false)).await.expect("changed");
    assert!(first.watched() && !first.last && first.replaces.is_none(), "{first:?}");
    assert!(reserve_watch(&*items, &id, at + Duration::hours(1)).await.unwrap().is_none(), "pending");
    let [fired, _] = rows(&*items, at).await.unwrap();
    assert_eq!((fired[0].1.status.as_str(), fired[0].1.last_fired), ("delivery_pending", Some(at)), "{fired:?}");
    confirm_delivered(&*items, &id, &first.id, at).await.unwrap();
    let reminder = Reminder::of(&get(&*items, &id).await.unwrap()).unwrap();
    assert_eq!((reminder.status, reminder.next_due), (Status::Active, Some(at + Duration::seconds(60))));
    let [fired, upcoming] = brief(&*items, at).await.unwrap();
    assert!(fired.lines.is_empty(), "{:?}", fired.lines);
    assert!(upcoming.lines[0].contains("; last fired "), "{}", upcoming.lines[0]);
    let [_, upcoming] = rows(&*items, at).await.unwrap();
    let row = &upcoming[0].1;
    assert_eq!((row.id.as_str(), row.kind.as_str(), row.status.as_str()), (id.as_str(), "watcher", "upcoming"));
    assert_eq!(row.trigger.as_deref(), Some("watch command gh pr checks 7, every 60 s"));
    assert_eq!((row.next_due, row.last_fired, row.failing_checks), (reminder.next_due, Some(at), None));
    assert!(!remind(&sense, &call(at, json!({"action":"ack","id":id}))).await.is_error);
    assert_eq!(Reminder::of(&get(&*items, &id).await.unwrap()).unwrap().status, Status::Active, "ack keeps it");

    let at = at + Duration::seconds(60);
    let second = check(&*items, &id, at, seen("c", false)).await.expect("changed");
    assert_eq!(second.replaces.as_deref(), Some(first.id.as_str()));
    confirm_delivered(&*items, &id, &second.id, at).await.unwrap();

    let at = at + Duration::seconds(60);
    let last = check(&*items, &id, at, seen("c", true)).await.expect("done fires whatever its digest");
    assert!(last.last, "{last:?}");
    assert_eq!(last.replaces.as_deref(), Some(second.id.as_str()));
    confirm_delivered(&*items, &id, &last.id, at).await.unwrap();
    let reminder = Reminder::of(&get(&*items, &id).await.unwrap()).unwrap();
    assert_eq!((reminder.status, reminder.next_due), (Status::Done, None));
    assert!(active(&*items, at).await.unwrap().is_empty());
}

#[tokio::test]
async fn a_watcher_s_first_check_fires_when_asked_and_failed_checks_back_off_without_firing() {
    let (sense, items) = fixture(SharedLog::default()).await;
    let id = create(&sense, watcher(true)).await;
    let firing = check(&*items, &id, now(), seen("a", false)).await.expect("first");
    confirm_delivered(&*items, &id, &firing.id, now()).await.unwrap();
    let mut at = now();
    for n in 1..=UNCHANGED_BEFORE_BACKOFF {
        at += Duration::seconds(60);
        assert!(check(&*items, &id, at, Checked::Failed("exit 127: gh: not found".into())).await.is_none());
        assert_eq!(Reminder::of(&get(&*items, &id).await.unwrap()).unwrap().failed_checks, Some(n));
    }
    let reminder = Reminder::of(&get(&*items, &id).await.unwrap()).unwrap();
    assert_eq!(reminder.next_due, Some(at + Duration::seconds(watch::DEFAULT_MAX_INTERVAL_SECONDS as i64)));
    let [_, upcoming] = brief(&*items, at).await.unwrap();
    assert!(
        upcoming.lines[0].contains("10 checks failed in a row, the latest because exit 127"),
        "{:?}",
        upcoming.lines
    );
    let [_, upcoming] = rows(&*items, at).await.unwrap();
    let row = &upcoming[0].1;
    assert_eq!(row.failing_checks, Some(UNCHANGED_BEFORE_BACKOFF));
    assert_eq!(row.last_error.as_deref(), Some("exit 127: gh: not found"));
    let at = reminder.next_due.unwrap();
    assert!(check(&*items, &id, at, seen("a", false)).await.is_none(), "the digest of before the failures");
    let reminder = Reminder::of(&get(&*items, &id).await.unwrap()).unwrap();
    assert_eq!((reminder.failed_checks, reminder.check_error), (None, None));
}

#[tokio::test]
async fn watchers_count_within_the_active_limit_and_only_trusted_origins_set_them() {
    let (sense, _) = fixture(SharedLog::default()).await;
    for _ in 0..MAX_ACTIVE {
        create(&sense, watcher(false)).await;
    }
    let too_many = remind(&sense, &call(now(), json!({"title":"overflow","trigger":watcher(false)}))).await;
    assert_eq!(too_many.json["error"], "too_many", "{too_many:?}");

    let (sense, _) = fixture(SharedLog::default()).await;
    let trail = json!({"kind":"watch","run":{"kind":"trail","name":"pr-blocked","args":{"pr":7},"limit_seconds":60}});
    let id = create(&sense, trail.clone()).await;
    for arguments in [
        json!({"title":"watch","trigger":trail}),
        json!({"action":"postpone","id":id,"after_seconds":60}),
        json!({"action":"drop","id":id}),
    ] {
        let mut request = call(now(), arguments);
        request.context.origin.by = Starter::Unknown;
        let answer = remind(&sense, &request).await;
        assert_eq!(answer.json["error"], "untrusted", "{answer:?}");
    }
    for (bad, why) in [
        (json!({"kind":"watch","run":{"kind":"command","command":"true"},"interval_seconds":10}), "interval_seconds"),
        (json!({"kind":"watch","run":{"kind":"trail","name":"t","limit_seconds":301}}), "run.limit_seconds"),
        (
            json!({"kind":"watch","run":{"kind":"command","command":"true"},"interval_seconds":600,
                   "max_interval_seconds":120}),
            "max_interval_seconds",
        ),
    ] {
        let answer = remind(&sense, &call(now(), json!({"title":"bad","trigger":bad}))).await;
        assert!(answer.is_error && answer.text.contains(why), "{answer:?}");
    }
}
