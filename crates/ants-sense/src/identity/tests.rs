use std::sync::Arc;

use ants_proto::provider::{BoxFuture, Brief, BriefReason, BriefRequest, Origin, Provider, Starter};
use uuid::Uuid;

use super::*;
use crate::Sense;
use crate::cache::Cache;
use crate::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
use crate::store::{Store, Stores};

fn dev_a() -> UserId {
    "dev-a".parse().unwrap()
}

fn at(time: &str) -> DateTime<Utc> {
    format!("2025-01-01T{time}Z").parse().unwrap()
}

/// The items of `sense` in `log`, its cache in memory.
async fn items(log: &'static MemoryLog, sense: &SenseId) -> Arc<dyn Items> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let log = AntLog::new(log, Unsigned, verifier, sense.clone(), dev_a(), "m1".parse().unwrap(), None).unwrap();
    let store = Store::open(log, Cache::in_memory().unwrap()).await.unwrap();
    Arc::new(tokio::sync::Mutex::new(store))
}

/// The items of a new sense identity, in a log of their own.
async fn fresh() -> Arc<dyn Items> {
    items(Box::leak(Box::default()), &SenseId::new()).await
}

fn event(ant: &str, identity: &str, time: &str, kind: EventKind) -> Event {
    Event { id: Uuid::now_v7(), ant: ant.parse().unwrap(), identity: identity.into(), thread: None, at: at(time), kind }
}

fn context(identity: &str, harness: &str, model: Option<&str>, machine: &str) -> CallContext {
    CallContext {
        ant: dev_a(),
        identity: identity.into(),
        thread: "019a7d2c-0000-7000-8000-000000000001".parse().unwrap(),
        origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
        harness: harness.into(),
        model: model.map(Into::into),
        machine: machine.into(),
        at: at("09:00:00"),
    }
}

async fn revision(items: &dyn Items) -> u64 {
    items.list(CARD, 1).await.unwrap()[0].revision
}

#[test]
fn the_sense_id_is_minted_once_and_kept_in_the_home() {
    let home = ants_test_support::scratch_dir("sense-id");
    assert_eq!(read_id(&home).unwrap(), None);
    let id = sense_id(&home).unwrap();
    assert_eq!(sense_id(&home).unwrap(), id);
    assert_eq!(read_id(&home).unwrap(), Some(id.clone()));
    assert_eq!(std::fs::read_to_string(home.join(".sense/id")).unwrap(), format!("{id}\n"));
    assert_eq!(std::fs::read_dir(home.join(".sense")).unwrap().count(), 1, "nothing left beside it");
    std::fs::write(id_file(&home), "not an id").unwrap();
    assert!(sense_id(&home).is_err(), "a damaged id is not replaced by another");
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn adding_an_ant_makes_its_card_with_its_sense_id_and_identity() {
    let items = fresh().await;
    assert_eq!(card(&*items).await.unwrap(), None);
    let added = record_event(&*items, &event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    assert_eq!(added.sense, items.sense().await);
    assert_eq!(
        (&added.name, added.identity.as_str(), &added.identities[..]),
        (&dev_a(), "id1", &["id1".to_owned()][..])
    );
    assert_eq!(added.born, at("10:00:00"));
    assert!(added.active && !added.returned());
    assert_eq!(card(&*items).await.unwrap(), Some(added));
    let cards = items.list(CARD, 5).await.unwrap();
    assert_eq!((cards.len(), cards[0].created.by.as_str()), (1, BY));
}

#[tokio::test]
async fn an_ant_added_again_under_a_new_identity_is_linked_to_it_and_active_again() {
    let items = fresh().await;
    record_event(&*items, &event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    let retired = record_event(&*items, &event("dev-a", "id1", "11:00:00", EventKind::AntRetired)).await.unwrap();
    assert!(!retired.active && retired.retired_at == Some(at("11:00:00")));

    let back = record_event(&*items, &event("dev-a", "id2", "12:00:00", EventKind::AntAdded)).await.unwrap();
    assert_eq!((back.identity.as_str(), &back.identities[..]), ("id2", &["id1".to_owned(), "id2".to_owned()][..]));
    assert!(back.active && back.retired_at.is_none());
    assert_eq!((back.returned_at, back.born), (Some(at("12:00:00")), at("10:00:00")));

    let told = returned_told(&*items, &dev_a()).await.unwrap();
    assert!(!told.returned());
    assert!(!items.list(CARD, 1).await.unwrap()[0].values.contains_key("returned_at"), "removed, not left null");
}

#[tokio::test]
async fn renames_clones_moves_retirements_and_revivals_are_written_on_the_card() {
    let items = fresh().await;
    let cloned = EventKind::AntCloned { from: "dev-x".parse().unwrap(), from_identity: "idx".into() };
    let cloned = record_event(&*items, &event("dev-a", "id1", "10:00:00", cloned)).await.unwrap();
    let from = cloned.cloned_from.unwrap();
    assert_eq!((from.ant.as_str(), from.identity.as_str(), cloned.born), ("dev-x", "idx", at("10:00:00")));

    let renamed = |from: &str| EventKind::AntRenamed { from: from.parse().unwrap() };
    record_event(&*items, &event("dev-b", "id1", "11:00:00", renamed("dev-a"))).await.unwrap();
    let card = record_event(&*items, &event("dev-c", "id1", "12:00:00", renamed("dev-b"))).await.unwrap();
    assert_eq!(card.name.as_str(), "dev-c");
    assert_eq!(card.earlier_names, [dev_a(), "dev-b".parse().unwrap()]);

    let card = record_event(&*items, &event("dev-c", "id1", "13:00:00", EventKind::AntMoved)).await.unwrap();
    assert_eq!(card.moved_at, Some(at("13:00:00")));
    let card = record_event(&*items, &event("dev-c", "id1", "14:00:00", EventKind::AntRetired)).await.unwrap();
    assert!(!card.active);
    let card = record_event(&*items, &event("dev-c", "id1", "15:00:00", EventKind::AntRevived)).await.unwrap();
    assert!(card.active && card.retired_at.is_none() && !card.returned());
    assert_eq!(card.identities, ["id1"]);
}

#[tokio::test]
async fn an_event_delivered_again_changes_nothing() {
    let items = fresh().await;
    let retired = event("dev-a", "id1", "10:00:00", EventKind::AntRetired);
    record_event(&*items, &retired).await.unwrap();
    record_event(&*items, &event("dev-a", "id1", "11:00:00", EventKind::AntRevived)).await.unwrap();
    let before = revision(&*items).await;
    assert!(record_event(&*items, &retired).await.unwrap().active, "the retirement is not applied twice");
    assert_eq!(revision(&*items).await, before, "nothing written");

    for n in 0..EVENTS_KEPT {
        let time = format!("12:00:{n:02}");
        record_event(&*items, &event("dev-a", "id1", &time, EventKind::AntMoved)).await.unwrap();
    }
    assert_eq!(card(&*items).await.unwrap().unwrap().events.len(), EVENTS_KEPT);
}

#[tokio::test]
async fn sessions_note_the_harnesses_models_and_machines_an_ant_ran_under() {
    let items = fresh().await;
    record_session(&*items, &context("id1", "copilot", None, "studio")).await.unwrap();
    record_session(&*items, &context("id1", "claude", Some("opus"), "studio")).await.unwrap();
    let card = record_session(&*items, &context("id1", "copilot", Some("gpt"), "laptop")).await.unwrap();
    assert_eq!(
        (&card.harnesses[..], &card.models[..]),
        (&["copilot", "claude"].map(String::from)[..], &["opus", "gpt"].map(String::from)[..])
    );
    assert_eq!(
        (&card.machines[..], &card.identities[..]),
        (&["studio", "laptop"].map(String::from)[..], &["id1".to_owned()][..])
    );
    let before = revision(&*items).await;
    record_session(&*items, &context("id1", "claude", Some("opus"), "laptop")).await.unwrap();
    assert_eq!(revision(&*items).await, before, "nothing new, nothing written");
}

#[tokio::test]
async fn two_writers_sharing_a_log_keep_one_card() {
    let (log, sense): (&'static MemoryLog, _) = (Box::leak(Box::default()), SenseId::new());
    let (one, two) = (items(log, &sense).await, items(log, &sense).await);
    record_event(&*one, &event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    let card = record_session(&*two, &context("id1", "copilot", None, "studio")).await.unwrap();
    assert_eq!((card.born, &card.harnesses[..]), (at("10:00:00"), &["copilot".to_owned()][..]), "the first's card");

    record_event(&*one, &event("dev-a", "id1", "11:00:00", EventKind::AntRetired)).await.unwrap();
    let card = record_session(&*two, &context("id1", "claude", None, "studio")).await.unwrap();
    assert!(!card.active && card.harnesses == ["copilot", "claude"], "{card:?}");
    assert_eq!(two.list(CARD, 5).await.unwrap().len(), 1);
    one.sync().await.unwrap();
    assert_eq!(super::card(&*one).await.unwrap(), Some(card));
}

/// The items of dev-a, the same each time.
struct One(Arc<dyn Items>);

impl Stores for One {
    fn items<'a>(&'a self, ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        let items = if *ant == dev_a() { Ok(self.0.clone()) } else { Err(StoreError::NotKept(ant.to_string())) };
        Box::pin(std::future::ready(items))
    }
}

#[tokio::test]
async fn sense_writes_the_card_from_events_and_briefs_and_acks_what_it_does_not_keep() {
    let sense = Sense::with_stores(Arc::new(One(fresh().await)));
    sense.event(event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    let context = context("id1", "copilot", None, "studio");
    let request = BriefRequest { context: context.clone(), budget: 100, reason: BriefReason::Started };
    let brief = sense.brief(request).await.unwrap().text;
    assert!(brief.starts_with("## Identity\ndev-a") && brief.len() <= 100, "{brief}");
    let card = sense.card(&dev_a()).await.unwrap().unwrap();
    assert_eq!((card.born, &card.harnesses[..]), (at("10:00:00"), &["copilot".to_owned()][..]));

    sense.event(event("dev-a", "id1", "11:00:00", EventKind::ThreadDone)).await.unwrap();
    assert_eq!(sense.card(&dev_a()).await.unwrap(), Some(card));
    sense.event(event("dev-b", "id9", "11:00:00", EventKind::AntRetired)).await.unwrap();
    let elsewhere = CallContext { ant: "dev-b".parse().unwrap(), ..context };
    let request = BriefRequest { context: elsewhere, budget: 100, reason: BriefReason::Started };
    assert_eq!(sense.brief(request).await.unwrap(), Brief::default());
}
