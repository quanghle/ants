use std::sync::Arc;

use ants_proto::provider::{CallContext, Format, Origin, Starter};
use ants_proto::{SenseId, UserId};

use super::*;
use crate::cache::Cache;
use crate::items::Values;
use crate::log::{
    AccountVerifier, AntLog, AppendError, EventLog, LogError, MemoryLog, NoRevocations, Stored, Unsigned,
};
use crate::store::{Store, Stores};

struct One(Arc<dyn Items>);

#[derive(Clone, Default)]
struct SharedLog(Arc<MemoryLog>);

impl EventLog for SharedLog {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected: u64) -> Result<u64, AppendError> {
        self.0.append(sense, payload, expected).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.0.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.0.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        self.0.incarnation()
    }
}

impl Stores for One {
    fn items<'a>(&'a self, _: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}

async fn setup() -> (Sense, Arc<dyn Items>) {
    let items = open_store(SharedLog::default(), SenseId::new()).await;
    (Sense::with_stores(Arc::new(One(items.clone()))), items)
}

async fn open_store(log: SharedLog, sense: SenseId) -> Arc<dyn Items> {
    let log = AntLog::new(
        log,
        Unsigned,
        AccountVerifier::new(vec![], NoRevocations),
        sense,
        "dev-a".parse().unwrap(),
        "m1".parse().unwrap(),
        None,
    )
    .unwrap();
    let store = Store::open(log, Cache::in_memory().unwrap()).await.unwrap();
    Arc::new(tokio::sync::Mutex::new(store))
}

fn now() -> DateTime<Utc> {
    "2025-01-01T12:00:00Z".parse().unwrap()
}

fn call(at: DateTime<Utc>, arguments: Value) -> ToolCall {
    ToolCall {
        name: "remind".into(),
        arguments: arguments.as_object().unwrap().clone(),
        context: CallContext {
            ant: "dev-a".parse().unwrap(),
            identity: "0f1e".into(),
            thread: "t1".parse().unwrap(),
            origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
            harness: "copilot".into(),
            model: None,
            machine: "m1".into(),
            repo: None,
            at,
        },
        format: Format::Json,
    }
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
    let sense = Sense::with_stores(Arc::new(One(items.clone())));
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
    let (sense, items) = setup().await;
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
async fn refires_are_hour_apart_capped_at_three_and_snooze_uses_a_new_id() {
    let (sense, items) = setup().await;
    let id = create(&sense, json!({"kind":"after","seconds":60})).await;
    let first_due = now() + Duration::minutes(1);
    assert!(due(&*items, now()).await.unwrap().is_empty());
    for firing in 1..=4 {
        let at = first_due + Duration::hours(firing - 1);
        let prepared = prepare_firing(&*items, &id, at, "time").await.unwrap().unwrap();
        assert_eq!(prepared.id, format!("{id}:{firing}"));
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
    let acknowledged = remind(&sense, &call(first_due + Duration::hours(5), json!({"action":"ack","id":id}))).await;
    assert!(!acknowledged.is_error, "{acknowledged:?}");
    assert!(prepare_firing(&*items, &id, first_due + Duration::hours(6), "after ack").await.unwrap().is_none());
}

#[tokio::test]
async fn recurring_utc_deadlines_advance_after_ack_or_exhaustion_and_persist() {
    let (sense, items) = setup().await;
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
    let (sense, items) = setup().await;
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
    let sense = Sense::with_stores(Arc::new(One(first.clone())));
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

#[tokio::test]
async fn matched_conditions_retry_stably_and_snooze_does_not_recheck_the_condition() {
    let (sense, items) = setup().await;
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
    let (sense, items) = setup().await;
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
    let (sense, items) = setup().await;
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
    let first_sense = Sense::with_stores(Arc::new(One(first.clone())));
    for _ in 0..MAX_ACTIVE - 1 {
        create(&first_sense, json!({"kind":"after","seconds":60})).await;
    }
    let second = open_store(log, sense_id).await;
    let second_sense = Sense::with_stores(Arc::new(One(second)));
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

    let (sense, items) = setup().await;
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
    let (sense, _) = setup().await;
    for trigger in [
        json!({"kind":"daily","time":"25:00","timezone":"UTC"}),
        json!({"kind":"weekly","time":"12:00","weekday":"monday","timezone":"UTC"}),
        json!({"kind":"daily","time":"12:00","timezone":"Europe/Paris"}),
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

#[tokio::test]
async fn only_trusted_origins_can_create_or_change_command_triggers() {
    let (sense, _) = setup().await;
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
    let (sense, items) = setup().await;
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

    let (sense, items) = setup().await;
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
