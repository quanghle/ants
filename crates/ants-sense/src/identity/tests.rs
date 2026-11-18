use std::sync::Arc;

use ants_proto::provider::{BoxFuture, Brief, BriefReason, BriefRequest, Origin, Provider, Starter};
use uuid::Uuid;

use super::*;
use crate::Sense;
use crate::cache::Cache;
use crate::items::ItemId;
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

/// The items of `sense` in `log` for `ant`, once named `earlier`, cached in `home`, where its chain status is kept.
async fn items_in(
    log: &'static MemoryLog,
    sense: &SenseId,
    ant: &str,
    earlier: Vec<UserId>,
    home: &Path,
) -> Arc<dyn Items> {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let ant = ant.parse().unwrap();
    let log =
        AntLog::new(log, Unsigned, verifier, sense.clone(), ant, "m1".parse().unwrap(), Some(home.to_owned())).unwrap();
    let store = Store::open(log.with_earlier_names(earlier), Cache::open(home).unwrap()).await.unwrap();
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
        repo: None,
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
    std::fs::write(Home::new(&home).id(), "not an id").unwrap();
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
    assert!(back.active && back.retired_at == Some(at("11:00:00")), "the dormancy kept until the brief tells it");
    assert_eq!((back.returned_at, back.born), (Some(at("12:00:00")), at("10:00:00")));

    let told = returned_told(&*items, &dev_a()).await.unwrap();
    assert!(!told.returned() && told.retired_at.is_none());
    let dormancy = Dormancy { from: at("11:00:00"), to: at("12:00:00") };
    assert_eq!(told.dormancies, [dormancy], "kept after the brief told it");
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
    let dormant = Dormancy { from: at("14:00:00"), to: at("14:30:00") };
    assert_eq!(card.dormant(at("14:30:00")).collect::<Vec<_>>(), [dormant], "the current dormancy");
    let card = record_event(&*items, &event("dev-c", "id1", "15:00:00", EventKind::AntRevived)).await.unwrap();
    assert!(card.active && card.returned());
    assert_eq!((card.retired_at, card.returned_at), (Some(at("14:00:00")), Some(at("15:00:00"))));
    assert_eq!(card.identities, ["id1"]);
    let card = record_event(&*items, &event("dev-c", "id1", "16:00:00", EventKind::AntRevived)).await.unwrap();
    assert_eq!(card.returned_at, Some(at("15:00:00")), "a revival of an active ant is no return");
    let dormant = Dormancy { from: at("14:00:00"), to: at("15:00:00") };
    assert_eq!(card.dormant(at("17:00:00")).collect::<Vec<_>>(), [dormant], "ended once");
}

/// The reminders of a dormant ant are paused: its revival puts off the expiry of each still active when it retired by
/// the time it was dormant, once, so a long dormancy expires none of them.
#[tokio::test]
async fn a_revival_puts_off_the_expiry_of_the_reminders_paused() {
    let items = fresh().await;
    let reminder = |status: remind::Status, expires: &str| {
        let writer = context("id1", "claude", None, "laptop");
        let trigger = remind::Trigger::At { at: at("09:00:00") };
        let mut reminder = remind::Reminder::new(
            "check".into(),
            writer.thread.clone(),
            trigger,
            at("09:00:00"),
            at(expires),
            &writer,
            None,
        );
        reminder.status = status;
        Write::Create { id: ItemId::new(), kind: remind::KIND.into(), values: reminder.values() }
    };
    let (active, dropped) = (remind::Status::Active, remind::Status::Dropped);
    let writes = [reminder(active, "12:00:00"), reminder(active, "10:30:00"), reminder(dropped, "12:00:00")];
    items.write(&writes, "test").await.unwrap();
    let expires = async || {
        let mut all = Vec::new();
        for write in &writes {
            let item = items.get(write.id()).await.unwrap().unwrap();
            all.push(remind::Reminder::of(&item).unwrap().expires.unwrap());
        }
        all
    };
    record_event(&*items, &event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    record_event(&*items, &event("dev-a", "id1", "11:00:00", EventKind::AntRetired)).await.unwrap();
    let revived = event("dev-a", "id1", "14:00:00", EventKind::AntRevived);
    record_event(&*items, &revived).await.unwrap();
    let shifted = [at("15:00:00"), at("10:30:00"), at("12:00:00")];
    assert_eq!(expires().await, shifted, "only the active one unexpired at the retirement");
    record_event(&*items, &revived).await.unwrap();
    record_event(&*items, &event("dev-a", "id1", "15:00:00", EventKind::AntRevived)).await.unwrap();
    assert_eq!(expires().await, shifted, "put off once");
}

#[tokio::test]
async fn a_retirement_of_a_dormant_ant_keeps_the_start_of_its_dormancy() {
    let items = fresh().await;
    record_event(&*items, &event("dev-a", "id1", "10:00:00", EventKind::AntRetired)).await.unwrap();
    let card = record_event(&*items, &event("dev-a", "id1", "11:00:00", EventKind::AntRetired)).await.unwrap();
    assert_eq!(card.retired_at, Some(at("10:00:00")));
    let card = record_event(&*items, &event("dev-a", "id1", "12:00:00", EventKind::AntRevived)).await.unwrap();
    assert_eq!(card.dormancies, [Dormancy { from: at("10:00:00"), to: at("12:00:00") }]);
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

#[tokio::test]
async fn a_renamed_ant_keeps_its_card_under_its_new_name_with_its_earlier_one() {
    let (log, sense): (&'static MemoryLog, _) = (Box::leak(Box::default()), SenseId::new());
    let dir = ants_test_support::scratch_dir("sense-renamed");
    let (home_a, home_b) = (dir.join("dev-a"), dir.join("dev-b"));
    let old = items_in(log, &sense, "dev-a", vec![], &home_a).await;
    record_event(&*old, &event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    let before = record_session(&*old, &context("id1", "copilot", None, "studio")).await.unwrap();
    drop(old);

    note_renamed(&home_b, &dev_a()).unwrap();
    note_renamed(&home_b, &dev_a()).unwrap();
    assert_eq!(earlier_names(&home_b).unwrap(), [dev_a()], "listed once");
    let dev_b: UserId = "dev-b".parse().unwrap();
    let in_b = CallContext { ant: dev_b.clone(), ..context("id1", "claude", None, "laptop") };
    for reopened in 0..3 {
        if reopened == 2 {
            std::fs::remove_file(Home::new(&home_b).cache()).unwrap();
        }
        let new = items_in(log, &sense, "dev-b", earlier_names(&home_b).unwrap(), &home_b).await;
        let card = match reopened {
            0 => {
                assert_eq!(super::card(&*new).await.unwrap(), Some(before.clone()), "the card before the rename");
                let card = record_session(&*new, &in_b).await.unwrap();
                record_event(&*new, &event("dev-b", "id1", "11:00:00", EventKind::AntMoved)).await.unwrap();
                let renamed = EventKind::AntRenamed { from: dev_a() };
                let after = record_event(&*new, &event("dev-b", "id1", "11:00:01", renamed)).await.unwrap();
                assert_eq!((&after.name, &after.earlier_names), (&card.name, &card.earlier_names));
                after
            }
            _ => super::card(&*new).await.unwrap().expect("kept"),
        };
        assert_eq!((&card.sense, &card.name, &card.earlier_names), (&sense, &dev_b, &vec![dev_a()]));
        assert_eq!((card.born, &card.identities[..]), (before.born, &["id1".to_owned()][..]));
        assert_eq!(card.harnesses, ["copilot", "claude"]);
        assert_eq!(new.broken().await, None);
        assert_eq!(new.list(CARD, 5).await.unwrap().len(), 1, "no second card");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn an_ant_added_under_a_new_name_with_an_old_home_keeps_its_card() {
    let log: &'static MemoryLog = Box::leak(Box::default());
    let home = ants_test_support::scratch_dir("sense-taken-over");
    let dev_b: UserId = "dev-b".parse().unwrap();
    assert_eq!(take_over(&home, &dev_b).unwrap(), None, "no sense data");
    let sense = sense_id(&home).unwrap();
    assert_eq!(take_over(&home, &dev_b).unwrap(), None, "a sense id, its log never read");
    let old = items_in(log, &sense, "dev-a", vec![], &home).await;
    let before = record_event(&*old, &event("dev-a", "id1", "10:00:00", EventKind::AntAdded)).await.unwrap();
    drop(old);
    assert_eq!(take_over(&home, &dev_a()).unwrap(), None, "added again under its name");
    assert_eq!(earlier_names(&home).unwrap(), []);

    assert_eq!(take_over(&home, &dev_b).unwrap(), Some(dev_a()));
    assert_eq!(take_over(&home, &dev_b).unwrap(), Some(dev_a()));
    assert_eq!(earlier_names(&home).unwrap(), [dev_a()], "listed once");
    let new = items_in(log, &sense, "dev-b", earlier_names(&home).unwrap(), &home).await;
    let card = record_event(&*new, &event("dev-b", "id2", "11:00:00", EventKind::AntAdded)).await.unwrap();
    assert_eq!((&card.name, &card.earlier_names, card.born), (&dev_b, &vec![dev_a()], before.born));
    assert_eq!((&card.identities[..], card.returned()), (&["id1".to_owned(), "id2".to_owned()][..], true));
    assert_eq!(new.broken().await, None);
    drop(new);
    assert_eq!(take_over(&home, &dev_b).unwrap(), None, "its log read under its new name");
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn a_card_made_anew_starts_with_the_earlier_names_of_the_home() {
    let log: &'static MemoryLog = Box::leak(Box::default());
    let home = ants_test_support::scratch_dir("sense-new-bus");
    let (zeta, dev_b): (UserId, UserId) = ("zeta".parse().unwrap(), "dev-b".parse().unwrap());
    let earlier = vec![zeta.clone(), dev_a(), dev_b.clone()];
    let items = items_in(log, &SenseId::new(), "dev-b", earlier, &home).await;
    record_event(&*items, &event("dev-b", "id1", "11:00:00", EventKind::AntMoved)).await.unwrap();
    let renamed = EventKind::AntRenamed { from: dev_a() };
    let card = record_event(&*items, &event("dev-b", "id1", "11:00:01", renamed)).await.unwrap();
    assert_eq!((&card.name, &card.earlier_names), (&dev_b, &vec![zeta, dev_a()]));
    std::fs::remove_dir_all(&home).unwrap();
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
