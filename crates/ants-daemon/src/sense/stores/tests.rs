//! Tests of the sense stores a process keeps: the host link and its retries, items opened on first use, identity
//! records across re-adds, renames and revocations, exports, caches serving while disconnected, and calls sharing a
//! catch-up ([`sharing`]).

use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::provider::{
    AntRef, BriefReason, BriefRequest, CallContext, Event, EventKind, Export, ImportRequest, Origin, Provider, Starter,
};
use ants_proto::{AntIdentity, SENSE_STREAM, Stage};
use ants_sense::Sense;
use ants_sense::identity::Growth;
use ants_sense::{items::ItemValues, store::Write};
use ants_test_support::{NatsServer, eventually, free_port, scratch_dir};
use serde_json::json;

use super::*;
use crate::bus;

mod sharing;

fn dev_a() -> UserId {
    "dev-a".parse().unwrap()
}

fn added(identity: &str) -> Event {
    Event {
        id: uuid::Uuid::now_v7(),
        ant: dev_a(),
        identity: identity.into(),
        thread: None,
        at: chrono::Utc::now(),
        kind: EventKind::AntAdded,
    }
}

/// Serve `ant` from `home` in `stores`, its daemon finding no identity record.
fn serve(stores: &SenseStores, ant: &UserId, home: &Path) {
    stores.serve(ant, home, None);
    stores.identify(ant, None);
}

async fn link(server: &NatsServer) -> Link {
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    Link::new(js, None, vec![], Machine::from_hostname("studio"))
}

#[tokio::test]
async fn nothing_is_kept_without_a_host_connection_and_nothing_reached_while_it_connects() {
    let stores = SenseStores::new();
    let home = scratch_dir("sense-stores-off");
    let kept = async |stores: &SenseStores| stores.items(&dev_a()).await.err().unwrap();
    serve(&stores, &dev_a(), &home);
    assert!(matches!(kept(&stores).await, StoreError::NotKept(_)));
    let first = stores.hosting();
    assert!(matches!(kept(&stores).await, StoreError::Unavailable(_)));
    let second = stores.hosting();
    drop(first);
    assert!(matches!(kept(&stores).await, StoreError::Unavailable(_)), "the next connection's state is left alone");
    drop(second);
    assert!(matches!(kept(&stores).await, StoreError::NotKept(_)));
    let unserved = stores.items(&"dev-b".parse().unwrap()).await.err().unwrap();
    assert!(matches!(unserved, StoreError::NotKept(_)));
    assert!(!home.join(".sense").exists(), "no sense id minted");
    std::fs::remove_dir_all(&home).unwrap();
}

/// Between retries of a failed link, an ant with no cache is kept none of, as with no link, but its events wait in its
/// queue, told again until the link is up, rather than being acked with no effect; its briefs stay empty.
#[tokio::test]
async fn events_wait_between_retries_of_the_link_and_are_kept_once_it_is_up() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores-retrying");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let sense = Sense::with_stores(stores.clone());
    let failed = stores.hosting();
    drop(failed);
    let why = "sense's link with the host credentials stopped: refused";
    stores.unkept(Some(why.to_owned()));
    let retrying = stores.retrying();
    assert!(stores.link_retrying(&dev_a()));
    assert!(!stores.link_retrying(&"dev-b".parse().unwrap()), "an ant not served is not waited for");
    let kept = stores.items(&dev_a()).await.err().unwrap();
    assert!(matches!(&kept, StoreError::NotKept(said) if said == why), "{kept:?}");
    let event = added("id1");
    let waiting = sense.event(event.clone()).await.unwrap_err();
    assert_eq!(waiting.code, ants_proto::provider::RpcError::UNAVAILABLE, "{waiting:?}");
    assert!(sense.brief(brief_request()).await.unwrap().text.is_empty(), "a brief keeps none");
    assert!(!home.join(".sense").exists(), "no sense id minted");

    let hosting = stores.hosting();
    drop(retrying);
    assert!(!stores.link_retrying(&dev_a()), "the next connection replaces the retry");
    assert!(matches!(stores.items(&dev_a()).await.err().unwrap(), StoreError::Unavailable(_)), "the guard left it");
    hosting.up(link(&server).await);
    stores.unkept(None);
    sense.event(event).await.unwrap();
    let card = identity::card(&*stores.items(&dev_a()).await.unwrap()).await.unwrap().unwrap();
    assert_eq!(card.identities, ["id1".to_owned()]);

    drop(hosting);
    let retrying = stores.retrying();
    assert!(stores.items(&dev_a()).await.is_ok(), "the cache serves between retries");
    drop(retrying);
    assert!(!stores.link_retrying(&dev_a()));
    std::fs::remove_dir_all(&home).unwrap();
}

/// Sense's link of its own, as a daemon serving one ant opens it, reaches the ant's data with none of a host
/// connection's other duties, writing no machine record; without it the process keeps none of them, saying why.
#[tokio::test]
async fn sense_s_own_link_reaches_the_data_and_records_no_machine() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores-own-link");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let why = "no host credentials on studio";
    stores.unkept(Some(why.to_owned()));
    let unkept = stores.items(&dev_a()).await.err().unwrap();
    assert!(matches!(&unkept, StoreError::NotKept(said) if said == why), "{unkept:?}");
    stores.unkept(None);
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    let machines = js.create_key_value(crate::hub::machines_config()).await.unwrap();
    let studio = Some(Machine::from_hostname("studio"));
    let host = crate::host::Host::quietly(studio, None, server.url.clone(), None, None, [false].into_iter()).unwrap();
    let checked = async {
        eventually("the link up", async || stores.items(&dev_a()).await.is_ok()).await;
        Sense::with_stores(stores.clone()).event(added("id1")).await.unwrap();
        let messages = js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages;
        assert_eq!(messages, 1);
        let recorded = futures::StreamExt::next(&mut machines.keys().await.unwrap()).await;
        assert!(recorded.is_none(), "no machine record");
    };
    tokio::select! {
        ended = crate::host::run_sense(host, stores.hosting()) => panic!("the link runs until dropped: {ended:?}"),
        () = checked => {}
    }
    std::fs::remove_dir_all(&home).unwrap();
}

/// An ant served again with credentials changed in place, as after a revoke and re-add, opens its items anew to sign
/// with them; served again with the same, it keeps them.
#[test]
fn new_credentials_open_the_items_again() {
    let stores = SenseStores::new();
    let home = scratch_dir("sense-stores-creds");
    let creds = home.join("dev-a.creds");
    std::fs::write(&creds, "first").unwrap();
    let items = |stores: &SenseStores| stores.state().ants[&dev_a()].items.clone();
    stores.serve(&dev_a(), &home, Some(creds.clone()));
    let first = items(&stores);
    stores.serve(&dev_a(), &home, Some(creds.clone()));
    assert!(Arc::ptr_eq(&first, &items(&stores)), "the same credentials keep the items");
    std::fs::write(&creds, "second").unwrap();
    stores.serve(&dev_a(), &home, Some(creds));
    assert!(!Arc::ptr_eq(&first, &items(&stores)), "new credentials open them again");
    std::fs::remove_dir_all(&home).unwrap();
}

/// Whether dev-a's items are open, awaiting the slot the worker retrying pending writes takes a moment each tick.
async fn opened(stores: &SenseStores) -> bool {
    let items = stores.state().ants[&dev_a()].items.clone();
    items.lock().await.is_some()
}

#[tokio::test]
async fn an_ant_s_items_open_on_first_use_with_its_sense_id_and_log() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    assert!(!home.join(".sense").exists(), "nothing opened before the first use");

    let sense = Sense::with_stores(stores.clone());
    sense.event(added("id1")).await.unwrap();
    let id = identity::read_id(&home).unwrap().expect("minted");
    let card = identity::card(&*stores.items(&dev_a()).await.unwrap()).await.unwrap().unwrap();
    assert_eq!((&card.sense, card.identities.as_slice()), (&id, &["id1".to_owned()][..]));
    assert!(home.join(".sense/cache.db").exists());
    let first = stores.items(&dev_a()).await.unwrap();
    assert!(Arc::ptr_eq(&first, &stores.items(&dev_a()).await.unwrap()), "opened once");

    let again = stores.hosting();
    assert!(opened(&stores).await, "cached items survive the next connection starting");
    assert!(
        Arc::ptr_eq(&first, &stores.items(&dev_a()).await.unwrap()),
        "cached items remain reachable while connecting"
    );
    again.up(link(&server).await);
    assert!(
        Arc::ptr_eq(&first, &stores.items(&dev_a()).await.unwrap()),
        "the existing store follows the new connection"
    );
    drop((hosting, first));
    assert!(opened(&stores).await, "the next connection's items are kept");
    drop(again);
    assert!(opened(&stores).await, "cached items remain available without a host link");

    std::fs::remove_file(home.join(".sense/cache.db")).unwrap();
    let elsewhere = SenseStores::new();
    serve(&elsewhere, &dev_a(), &home);
    let hosting = elsewhere.hosting();
    hosting.up(link(&server).await);
    let sense = Sense::with_stores(elsewhere.clone());
    sense.event(added("id2")).await.unwrap();
    let card = identity::card(&*elsewhere.items(&dev_a()).await.unwrap()).await.unwrap().unwrap();
    assert_eq!((&card.sense, card.identities.as_slice()), (&id, &["id1".to_owned(), "id2".to_owned()][..]));
    assert!(card.returned(), "read back from the log, the cache gone");
    assert_eq!(identity::read_id(&home).unwrap(), Some(id));

    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    let messages = js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages;
    assert_eq!(messages, 2);
    std::fs::remove_dir_all(&home).unwrap();
}

/// The identity record names the ant's sense id, so its homes on every machine open one sense identity: the first
/// home's id is written to the record, a home without one takes the record's, also once its id file is removed, and a
/// home keeping another is refused, its items closed, until it is made to keep the record's, unless writes wait there
/// to be appended. A home whose daemon has not read the record opens only under the
/// id it keeps, and an ant whose record and home name none, as a clone, mints its own.
#[tokio::test]
async fn the_identity_record_names_one_sense_identity_for_every_home() {
    let Some(server) = NatsServer::start() else { return };
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    crate::hub::create_identities(&js).await.unwrap();
    let record = Identity::new(&js, None, &dev_a()).await.unwrap();
    let homes = [scratch_dir("sense-named-a"), scratch_dir("sense-named-b"), scratch_dir("sense-named-c")];
    let serving = async |ant: &UserId, home: &Path| {
        let stores = SenseStores::new();
        stores.serve(ant, home, None);
        let hosting = stores.hosting();
        hosting.up(link(&server).await);
        (stores, hosting)
    };
    let identified = async |stores: &SenseStores, ant: &UserId| {
        stores.identify(ant, Some(Identity::new(&js, None, ant).await.unwrap()));
    };

    let (stores, _hosting) = serving(&dev_a(), &homes[0]).await;
    let awaited = stores.items(&dev_a()).await.err().unwrap();
    assert!(matches!(&awaited, StoreError::Unavailable(e) if e.contains("identity record")), "{awaited}");
    assert_eq!(identity::read_id(&homes[0]).unwrap(), None, "nothing minted before the record is read");
    identified(&stores, &dev_a()).await;
    Sense::with_stores(stores.clone()).event(added("id1")).await.unwrap();
    let id = identity::read_id(&homes[0]).unwrap().expect("minted");
    assert_eq!(record.read().await.unwrap().unwrap().sense, Some(id.clone()), "the record names it");

    for removed in [false, true] {
        if removed {
            std::fs::remove_file(Home::new(&homes[1]).id()).unwrap();
        }
        let (stores, _hosting) = serving(&dev_a(), &homes[1]).await;
        identified(&stores, &dev_a()).await;
        let card = identity::card(&*stores.items(&dev_a()).await.unwrap()).await.unwrap().expect("built from the log");
        assert_eq!((&card.sense, card.identities.as_slice()), (&id, &["id1".to_owned()][..]));
        assert_eq!(identity::read_id(&homes[1]).unwrap(), Some(id.clone()), "the home takes the record's");
    }

    let other = SenseId::new();
    identity::keep_id(&homes[2], &other).unwrap();
    let (stores, _hosting) = serving(&dev_a(), &homes[2]).await;
    let opened = stores.items(&dev_a()).await.unwrap();
    assert_eq!(opened.sense().await, other, "the home's id while the record is unread");
    identified(&stores, &dev_a()).await;
    let refused = stores.items(&dev_a()).await.err().unwrap();
    assert!(
        matches!(&refused, StoreError::Unavailable(e) if e.contains(&format!("its identity record names {id}"))
            && e.contains("`ants ant sense-id dev-a --take home`")),
        "{refused}"
    );
    let slot = stores.state().ants[&dev_a()].items.clone();
    assert!(slot.lock().await.is_none(), "the items opened are closed");
    assert_eq!(identity::read_id(&homes[2]).unwrap(), Some(other), "the home is left alone");
    assert_eq!(record.read().await.unwrap().unwrap().sense, Some(id.clone()), "and so is the record");
    std::fs::write(Home::new(&homes[2]).pending(), b"").unwrap();
    let lost = crate::sense::follow_record(&homes[2], &id).unwrap_err().to_string();
    assert!(lost.contains("to be appended"), "writes pending under the home's id refuse the record's: {lost}");
    std::fs::remove_file(Home::new(&homes[2]).pending()).unwrap();
    crate::sense::follow_record(&homes[2], &id).unwrap();
    assert_eq!(stores.items(&dev_a()).await.unwrap().sense().await, id, "settled by taking the record's, unrestarted");

    let dev_b: UserId = "dev-b".parse().unwrap();
    let home_b = scratch_dir("sense-named-clone");
    let (stores, _hosting) = serving(&dev_b, &home_b).await;
    identified(&stores, &dev_b).await;
    let own = stores.items(&dev_b).await.unwrap().sense().await;
    assert_ne!(own, id, "a sense identity of its own");
    let named = Identity::new(&js, None, &dev_b).await.unwrap().read().await.unwrap().unwrap().sense;
    assert_eq!((named, identity::read_id(&home_b).unwrap()), (Some(own.clone()), Some(own)));
    for home in homes.into_iter().chain([home_b]) {
        std::fs::remove_dir_all(home).unwrap();
    }
}

/// An ant revoked and added again in the same home keeps the birth and the growth of its earlier record, which its card
/// kept: its new record starts from them, once. A new ant, as a clone, starts as its record was made.
#[tokio::test]
async fn an_ant_added_again_keeps_its_birth_and_its_growth() {
    let Some(server) = NatsServer::start() else { return };
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    crate::hub::create_identities(&js).await.unwrap();
    let (home, home_b) = (scratch_dir("sense-growth-a"), scratch_dir("sense-growth-b"));
    let record = Identity::new(&js, None, &dev_a()).await.unwrap();
    let born = "2025-01-01T00:00:00Z".parse().unwrap();
    let earlier = AntIdentity { prompts: 42, ..AntIdentity::new(dev_a(), "id1".into(), born) };
    record.replace(&earlier).await.unwrap();
    let opened = async |ant: &UserId, home: &Path, event: Event| {
        let stores = SenseStores::new();
        stores.serve(ant, home, None);
        let hosting = stores.hosting();
        hosting.up(link(&server).await);
        stores.identify(ant, Some(Identity::new(&js, None, ant).await.unwrap()));
        Sense::with_stores(stores.clone()).event(event).await.unwrap();
        let items = stores.items(ant).await.unwrap();
        let card = identity::card(&*items).await.unwrap().unwrap();
        (items, card.growth, hosting)
    };

    let (items, growth, _hosting) = opened(&dev_a(), &home, added("id1")).await;
    assert_eq!(growth, Some(Growth { born, prompts: 42 }), "the card keeps the record's growth once it names it");
    let named = record.read().await.unwrap().unwrap();
    assert_eq!((named.born, named.prompts), (born, 42), "a record naming no sense id with no growth kept is kept");
    for (prompts, sense) in [(100, named.sense.clone()), (500, None)] {
        note_growth(&dev_a(), &*items, &AntIdentity { prompts, sense, ..named.clone() }).await.unwrap();
    }
    let card = identity::card(&*items).await.unwrap().unwrap();
    assert_eq!(card.growth, Some(Growth { born, prompts: 100 }), "told of the record naming its sense id alone");
    drop(items);

    crate::profile::identity::forget(&js, None, &dev_a()).await.unwrap();
    let again = record.ensure().await.unwrap().unwrap();
    assert_eq!((again.prompts, again.stage()), (0, Stage::Callow), "added again, a new record");
    record.count_prompt().await.unwrap();
    let (_items, growth, _hosting) = opened(&dev_a(), &home, added(&again.id)).await;
    let restored = record.read().await.unwrap().unwrap();
    assert_eq!((&restored.id, restored.born, restored.prompts), (&again.id, born, 101), "its birth and growth kept");
    assert_eq!((restored.stage(), &restored.sense), (Stage::Veteran, &named.sense));
    assert_eq!(growth, Some(Growth { born, prompts: 101 }));
    let (_items, _, _hosting) = opened(&dev_a(), &home, added(&again.id)).await;
    assert_eq!(record.read().await.unwrap().unwrap(), restored, "a daemon starting again adds nothing");

    let dev_b: UserId = "dev-b".parse().unwrap();
    let new = Identity::new(&js, None, &dev_b).await.unwrap();
    let made = new.ensure().await.unwrap().unwrap();
    let (_items, growth, _hosting) = opened(&dev_b, &home_b, Event { ant: dev_b.clone(), ..added(&made.id) }).await;
    let started = new.read().await.unwrap().unwrap();
    assert_eq!((started.born, started.prompts, growth), (made.born, 0, Some(Growth { born: made.born, prompts: 0 })));
    for home in [home, home_b] {
        std::fs::remove_dir_all(home).unwrap();
    }
}

/// The prompts counted since the card last noted the record's growth, below the next stage, are noted once the daemon
/// stops serving the ant, so a record made again after a revoke starts from every one of them.
#[tokio::test]
async fn an_ant_added_again_after_its_daemon_stopped_keeps_every_prompt() {
    let Some(server) = NatsServer::start() else { return };
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    crate::hub::create_identities(&js).await.unwrap();
    let home = scratch_dir("sense-growth-stop");
    let record = Identity::new(&js, None, &dev_a()).await.unwrap();
    let made = record.ensure().await.unwrap().unwrap();
    let serving = async || {
        let sense = crate::sense::Sense::new(crate::provider::wake::Wakers::default());
        sense.serve(&dev_a(), &home, None);
        let hosting = sense.stores.hosting();
        hosting.up(link(&server).await);
        sense.identify(&dev_a(), Some(Identity::new(&js, None, &dev_a()).await.unwrap()));
        sense.provider.event(added(&made.id)).await.unwrap();
        (sense, hosting)
    };
    let growth = async |sense: &crate::sense::Sense| {
        let items = sense.stores.items(&dev_a()).await.unwrap();
        identity::card(&*items).await.unwrap().unwrap().growth.map(|growth| growth.prompts)
    };

    let (sense, _hosting) = serving().await;
    assert_eq!(growth(&sense).await, Some(0), "noted on opening");
    for _ in 0..7 {
        record.count_prompt().await.unwrap();
    }
    assert_eq!(growth(&sense).await, Some(0), "a prompt below the next stage notes nothing");
    sense.stopping(&dev_a(), &record).await.unwrap();
    assert_eq!(growth(&sense).await, Some(7), "noted as the daemon stops");
    drop(sense);

    record.delete().await.unwrap();
    record.ensure().await.unwrap().unwrap();
    let (_sense, _hosting) = serving().await;
    assert_eq!(record.read().await.unwrap().unwrap().prompts, 7, "every prompt kept");
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn revoking_a_live_or_crashed_ant_restores_growth_not_yet_on_its_card() {
    let Some(server) = NatsServer::start() else { return };
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    crate::hub::create_identities(&js).await.unwrap();
    let ant = dev_a();
    let record = Identity::new(&js, None, &ant).await.unwrap();
    let serving = async |home: &Path, id: &str| {
        let stores = SenseStores::new();
        stores.serve(&ant, home, None);
        let hosting = stores.hosting();
        hosting.up(link(&server).await);
        stores.identify(&ant, Some(Identity::new(&js, None, &ant).await.unwrap()));
        Sense::with_stores(stores.clone()).event(added(id)).await.unwrap();
        let items = stores.items(&ant).await.unwrap();
        (stores, hosting, items)
    };

    for crashed in [false, true] {
        let home = scratch_dir("sense-growth-revoke");
        let born = "2025-01-01T00:00:00Z".parse().unwrap();
        let earlier = AntIdentity { prompts: 42, ..AntIdentity::new(ant.clone(), "original".into(), born) };
        record.replace(&earlier).await.unwrap();
        let (stores, hosting, items) = serving(&home, &earlier.id).await;
        let sense = items.sense().await;
        for _ in 0..7 {
            record.count_prompt().await.unwrap();
        }
        let card = identity::card(&*items).await.unwrap().unwrap();
        assert_eq!(card.growth, Some(Growth { born, prompts: 42 }), "no stage change");
        let running = if crashed {
            drop((stores, hosting, items));
            std::fs::remove_file(Home::new(&home).cache()).unwrap();
            None
        } else {
            Some((stores, hosting, items))
        };
        crate::profile::identity::forget(&js, None, &ant).await.unwrap();
        assert!(record.read().await.unwrap().is_none());
        drop(running);

        let new_home = scratch_dir("sense-growth-fresh");
        let fresh = record.ensure().await.unwrap().unwrap();
        let fresh_running = serving(&new_home, &fresh.id).await;
        assert_eq!(record.read().await.unwrap().unwrap().prompts, 0, "a different sense identity starts fresh");
        drop(fresh_running);
        crate::profile::identity::forget(&js, None, &ant).await.unwrap();
        std::fs::remove_dir_all(new_home).unwrap();

        let again = record.ensure().await.unwrap().unwrap();
        assert_ne!(again.id, earlier.id);
        record.count_prompt().await.unwrap();
        let (_stores, _hosting, items) = serving(&home, &again.id).await;
        let restored = record.read().await.unwrap().unwrap();
        assert_eq!((restored.id.as_str(), restored.born, restored.prompts), (again.id.as_str(), born, 50));
        assert_eq!(restored.sense, Some(sense));
        assert_eq!(identity::card(&*items).await.unwrap().unwrap().growth, Some(Growth { born, prompts: 50 }));
        drop((_stores, _hosting, items));
        crate::profile::identity::forget(&js, None, &ant).await.unwrap();
        std::fs::remove_dir_all(home).unwrap();
    }
}

struct NamingWarnings(Arc<AtomicUsize>);

impl tracing::Subscriber for NamingWarnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| {
            if field.name() == "message"
                && format!("{value:?}") == "naming the sense identity in the ant's identity record"
            {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// A card that cannot be read holds up only a record naming no sense id, warns once per ant per daemon run, and
/// settles the record on a later call once it reads.
#[tokio::test]
async fn an_unreadable_card_holds_up_only_a_record_naming_no_sense_id() {
    let Some(server) = NatsServer::start() else { return };
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    crate::hub::create_identities(&js).await.unwrap();
    let ants = [dev_a(), "dev-b".parse::<UserId>().unwrap()];
    let homes = ants.each_ref().map(|_| scratch_dir("sense-card-unread"));
    let records =
        [Identity::new(&js, None, &ants[0]).await.unwrap(), Identity::new(&js, None, &ants[1]).await.unwrap()];
    let born = "2025-01-01T00:00:00Z".parse().unwrap();
    for (ant, record) in ants.iter().zip(&records) {
        record
            .replace(&AntIdentity { prompts: 42, ..AntIdentity::new(ant.clone(), "id1".into(), born) })
            .await
            .unwrap();
    }
    let serving = async || {
        let stores = SenseStores::new();
        let hosting = stores.hosting();
        hosting.up(link(&server).await);
        for (ant, home) in ants.iter().zip(&homes) {
            stores.serve(ant, home, None);
            stores.identify(ant, Some(Identity::new(&js, None, ant).await.unwrap()));
        }
        (stores, hosting)
    };
    let settled = |stores: &SenseStores, ant: &UserId| matches!(stores.state().ants[ant].named, Named::Settled);
    let card_born = async |stores: &SenseStores, ant: &UserId, at: &str| {
        let items = stores.items(ant).await.unwrap();
        let card = items.list(identity::CARD, 1).await.unwrap().pop().unwrap();
        let values = serde_json::from_value(json!({ "born": at })).unwrap();
        items.write(&[Write::Change { id: card.id, values, expected: None }], identity::BY).await.unwrap();
    };

    let (stores, hosting) = serving().await;
    for ant in &ants {
        Sense::with_stores(stores.clone()).event(Event { ant: ant.clone(), ..added("id1") }).await.unwrap();
        assert!(settled(&stores, ant));
        card_born(&stores, ant, "not a time").await;
        assert!(identity::card(&*stores.items(ant).await.unwrap()).await.is_err(), "the card cannot be read");
    }
    drop(hosting);

    let warnings = Arc::new(AtomicUsize::new(0));
    let _logged = tracing::subscriber::set_default(NamingWarnings(warnings.clone()));
    let warned = || warnings.load(Ordering::Relaxed);
    let (stores, hosting) = serving().await;
    stores.items(&ants[0]).await.unwrap();
    assert!(settled(&stores, &ants[0]), "a record naming the home's sense id needs no growth of the card");
    assert_eq!(warned(), 0);
    drop(hosting);

    let sense = records[0].read().await.unwrap().unwrap().sense;
    for record in &records {
        record.delete().await.unwrap();
        record.ensure().await.unwrap().unwrap();
    }
    let (stores, hosting) = serving().await;
    for (i, (ant, record)) in ants.iter().zip(&records).enumerate() {
        for _ in 0..3 {
            stores.items(ant).await.unwrap();
            assert!(!settled(&stores, ant), "a record naming no sense id waits for the card's growth");
            assert_eq!(warned(), i + 1, "{ant} warns only on its first failed attempt");
        }
        assert_eq!(record.read().await.unwrap().unwrap().sense, None);
    }
    card_born(&stores, &ants[0], "2025-01-01T00:00:00Z").await;
    stores.items(&ants[0]).await.unwrap();
    assert!(settled(&stores, &ants[0]), "settled on a later call");
    assert_eq!(warned(), 2);
    let restored = records[0].read().await.unwrap().unwrap();
    assert_eq!((restored.born, restored.prompts, restored.sense), (born, 42, sense));
    drop(hosting);

    let (stores, _hosting) = serving().await;
    stores.items(&ants[1]).await.unwrap();
    assert_eq!(warned(), 3, "a new daemon run warns again");
    for home in homes {
        std::fs::remove_dir_all(home).unwrap();
    }
}

/// An ant imported under another name on the same bus, its home holding its sense id and listing its old name, keeps
/// its card, as read again from its log alone.
#[tokio::test]
async fn a_renamed_ant_keeps_its_card() {
    let Some(server) = NatsServer::start() else { return };
    let (home_a, home_b) = (scratch_dir("sense-stores-a"), scratch_dir("sense-stores-b"));
    let dev_b: UserId = "dev-b".parse().unwrap();
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home_a);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    Sense::with_stores(stores.clone()).event(added("id1")).await.unwrap();
    let id = identity::read_id(&home_a).unwrap().expect("minted");
    drop(hosting);

    std::fs::create_dir_all(Home::new(&home_b).dir()).unwrap();
    std::fs::copy(Home::new(&home_a).id(), Home::new(&home_b).id()).unwrap();
    identity::note_renamed(&home_b, &dev_a()).unwrap();
    for reopened in [false, true] {
        let stores = SenseStores::new();
        serve(&stores, &dev_b, &home_b);
        let hosting = stores.hosting();
        hosting.up(link(&server).await);
        let sense = Sense::with_stores(stores.clone());
        if !reopened {
            let moved = Event { ant: dev_b.clone(), kind: EventKind::AntMoved, ..added("id1") };
            sense.event(moved).await.unwrap();
            let renamed = Event { ant: dev_b.clone(), kind: EventKind::AntRenamed { from: dev_a() }, ..added("id1") };
            sense.event(renamed).await.unwrap();
            std::fs::remove_file(home_b.join(".sense/cache.db")).unwrap();
        }
        let card = identity::card(&*stores.items(&dev_b).await.unwrap()).await.unwrap().unwrap();
        assert_eq!((&card.sense, &card.name, &card.earlier_names[..]), (&id, &dev_b, &[dev_a()][..]));
        assert!(card.moved_at.is_some() && card.identities == ["id1"]);
        assert_eq!(stores.items(&dev_b).await.unwrap().broken().await, None);
    }
    std::fs::remove_dir_all(&home_a).unwrap();
    std::fs::remove_dir_all(&home_b).unwrap();
}

/// An ant's sense data, exported on one bus, land on another under the same sense id once the home stops awaiting them;
/// a home of another sense id, or a log holding other events, refuses them.
#[tokio::test]
async fn an_export_moves_the_sense_data_to_another_bus() {
    let (Some(bus_a), Some(bus_b)) = (NatsServer::start(), NatsServer::start()) else { return };
    let (home_a, home_b) = (scratch_dir("sense-export-a"), scratch_dir("sense-export-b"));
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home_a);
    let hosting = stores.hosting();
    hosting.up(link(&bus_a).await);
    let sense = Sense::with_stores(stores.clone());
    sense.event(added("id1")).await.unwrap();
    let mut skill = serde_json::Map::new();
    skill.insert("name".into(), json!("review"));
    skill.insert("description".into(), json!("Review well."));
    skill.insert("files".into(), json!({"SKILL.md": "review well"}));
    let kind = ants_sense::skill::SKILL.into();
    let write = ants_sense::store::Write::Create { id: ants_sense::skill::skill_id("review"), kind, values: skill };
    stores.items(&dev_a()).await.unwrap().write(&[write], "test").await.unwrap();
    std::fs::create_dir_all(Home::new(&home_a).skills().join("stray")).unwrap();
    let exported = sense.export(AntRef { ant: dev_a(), identity: "id1".into() }).await.unwrap();
    assert!(!Home::new(&home_a).skills().join("stray").exists(), "the skill files are those of the log");
    let id = identity::read_id(&home_a).unwrap().unwrap();
    drop(hosting);

    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home_b);
    let hosting = stores.hosting();
    hosting.up(link(&bus_b).await);
    let sense = Sense::with_stores(stores.clone());
    export::expect_import(&home_b).unwrap();
    let awaiting = stores.items(&dev_a()).await.err().unwrap();
    assert!(awaiting.to_string().contains("await their import"), "{awaiting}");
    let request = |export: &Export| ImportRequest { ant: dev_a(), identity: "id1".into(), export: export.clone() };
    sense.import(request(&exported)).await.unwrap();
    assert!(!Home::new(&home_b).awaits_import(), "the home no longer awaits them");
    assert_eq!(identity::read_id(&home_b).unwrap(), Some(id.clone()));
    assert_eq!(std::fs::read_to_string(Home::new(&home_b).skills().join("review/SKILL.md")).unwrap(), "review well");
    let card = identity::card(&*stores.items(&dev_a()).await.unwrap()).await.unwrap().unwrap();
    assert_eq!((&card.sense, card.identities.as_slice()), (&id, &["id1".to_owned()][..]));
    let js = bus::connect(&bus_b.url, None, None, &UserId::human(), "test").await.unwrap().1;
    let messages = js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages;
    assert_eq!(messages, 3, "the events and the anchor");
    sense.import(request(&exported)).await.unwrap();
    assert_eq!(js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages, 3, "taken in already");
    drop(hosting);

    let home_c = scratch_dir("sense-export-c");
    identity::keep_id(&home_c, &SenseId::new()).unwrap();
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home_c);
    let hosting = stores.hosting();
    hosting.up(link(&bus_b).await);
    let refused = Sense::with_stores(stores.clone()).import(request(&exported)).await.unwrap_err();
    assert!(refused.message.contains("keeps sense identity"), "{}", refused.message);
    drop(hosting);

    let Some(bus_c) = NatsServer::start() else { return };
    let home_d = scratch_dir("sense-export-d");
    identity::keep_id(&home_d, &id).unwrap();
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home_d);
    let hosting = stores.hosting();
    hosting.up(link(&bus_c).await);
    let sense = Sense::with_stores(stores.clone());
    sense.event(added("id2")).await.unwrap();
    let refused = sense.import(request(&exported)).await.unwrap_err();
    assert!(refused.message.contains("holds other events"), "{}", refused.message);
    drop(hosting);
    for home in [home_a, home_b, home_c, home_d] {
        std::fs::remove_dir_all(home).unwrap();
    }
}

fn brief_request() -> BriefRequest {
    BriefRequest {
        context: CallContext {
            ant: dev_a(),
            identity: "id1".into(),
            thread: uuid::Uuid::now_v7().to_string().parse().unwrap(),
            origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
            harness: "copilot".into(),
            model: None,
            machine: "studio".into(),
            repo: None,
            at: chrono::Utc::now(),
        },
        budget: 8_000,
        reason: BriefReason::Started,
    }
}

#[tokio::test]
async fn actual_disconnect_serves_cached_briefs_and_reconnect_drains_without_another_read() {
    let Some(mut server) = NatsServer::start_on(free_port()) else { return };
    let home = scratch_dir("sense-stores-offline");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    let link = link(&server).await;
    let client = link.js.client().clone();
    let js = link.js.clone();
    hosting.up(link);
    let sense = Sense::with_stores(stores.clone());
    sense.event(added("id1")).await.unwrap();
    let items = stores.items(&dev_a()).await.unwrap();
    server.stop();
    eventually("host disconnected", async || client.connection_state() != async_nats::connection::State::Connected)
        .await;
    let mut memory = ants_sense::memory::Memory::new(
        ants_sense::memory::MemoryKind::Preference,
        ants_sense::memory::Scope::Own,
        "Cached offline preference".into(),
        "Keep local notes".into(),
        &brief_request().context,
    );
    memory.core = true;
    items.create(ants_sense::memory::MEMORY, memory.values(), "t").await.unwrap();
    assert_eq!(items.pending().await.unwrap(), 1);
    assert!(Arc::ptr_eq(&items, &stores.items(&dev_a()).await.unwrap()));
    let brief = sense.brief(brief_request()).await.unwrap();
    assert!(brief.text.contains("Cached offline preference"), "{}", brief.text);
    let pending = items.pending().await.unwrap();
    server.start_again();
    // pending() is cache-only: the retry worker, not a tool/read, performs the append.
    eventually("pending sense write appended on reconnect", async || items.pending().await.unwrap() == 0).await;
    assert_eq!(js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages, pending as u64 + 1);
    drop((items, hosting));
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn cached_writes_survive_process_restart_and_the_new_host_link_drains_them() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores-restart");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    let items = stores.items(&dev_a()).await.unwrap();
    items.create("note", json!({"title":"online"}).as_object().unwrap().clone(), "t").await.unwrap();
    drop(hosting);
    items.create("note", json!({"title":"offline"}).as_object().unwrap().clone(), "t").await.unwrap();
    assert_eq!(items.pending().await.unwrap(), 1);
    drop((items, stores));

    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let items = stores.items(&dev_a()).await.unwrap();
    assert_eq!(items.list("note", 10).await.unwrap().len(), 2, "opened from durable cache before a host link exists");
    assert_eq!(items.pending().await.unwrap(), 1);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    eventually("restarted pending sense writes appended", async || items.pending().await.unwrap() == 0).await;
    assert!(Arc::ptr_eq(&items, &stores.items(&dev_a()).await.unwrap()));
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    assert_eq!(js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages, 2);
    drop((items, hosting));
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn reconnect_reopens_an_unused_pending_cache_without_waiting_for_a_tool() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores-lazy-restart");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    let first_link = link(&server).await;
    let js = first_link.js.clone();
    hosting.up(first_link);
    let items = stores.items(&dev_a()).await.unwrap();
    drop(hosting);
    items.create("note", json!({"title":"queued"}).as_object().unwrap().clone(), "t").await.unwrap();
    drop((items, stores));

    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    eventually("unused durable outbox reopened and appended", async || {
        js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages == 1
    })
    .await;
    let opened = stores.state().ants[&dev_a()].items.clone();
    let items = opened.lock().await.as_ref().unwrap().clone();
    assert_eq!(items.pending().await.unwrap(), 0);
    drop((items, hosting));
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn a_disconnected_initial_host_opens_the_cache_before_its_log_can_be_opened() {
    let Some(mut server) = NatsServer::start_on(free_port()) else { return };
    let home = scratch_dir("sense-stores-cached-host");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    let items = stores.items(&dev_a()).await.unwrap();
    items.create("note", json!({"title":"online"}).as_object().unwrap().clone(), "t").await.unwrap();
    drop((items, hosting, stores));
    let next = link(&server).await;
    let client = next.js.client().clone();
    server.stop();
    eventually("initial host disconnected", async || {
        client.connection_state() != async_nats::connection::State::Connected
    })
    .await;

    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(next);
    let items = stores.items(&dev_a()).await.unwrap();
    assert_eq!(items.list("note", 10).await.unwrap().len(), 1);
    items.create("note", json!({"title":"offline"}).as_object().unwrap().clone(), "t").await.unwrap();
    assert_eq!(items.pending().await.unwrap(), 1);
    server.start_again();
    eventually("initial offline cache appended on reconnect", async || items.pending().await.unwrap() == 0).await;
    drop((items, hosting));
    std::fs::remove_dir_all(home).unwrap();
}

/// Over a sense stream created anew, items with no writes pending open again from it as a call starts; items with
/// writes pending stay as they are, keeping those writes.
#[tokio::test]
async fn a_new_stream_incarnation_opens_the_items_again_unless_writes_are_pending() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores-incarnation");
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    let first = stores.items(&dev_a()).await.unwrap();
    first.create("note", json!({"title":"online"}).as_object().unwrap().clone(), "t").await.unwrap();
    assert_eq!(first.pending().await.unwrap(), 0);
    js.delete_stream(SENSE_STREAM).await.unwrap();
    let again = stores.hosting();
    again.up(link(&server).await);
    let anew = stores.synced(&dev_a()).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &anew), "opened again over the new stream");
    assert_eq!(anew.list("note", 10).await.unwrap(), [], "built from the new stream");

    drop((first, hosting, again));
    anew.create("note", json!({"title":"queued"}).as_object().unwrap().clone(), "t").await.unwrap();
    assert_eq!(anew.pending().await.unwrap(), 1);
    js.delete_stream(SENSE_STREAM).await.unwrap();
    let third = stores.hosting();
    third.up(link(&server).await);
    assert!(Arc::ptr_eq(&anew, &stores.synced(&dev_a()).await.unwrap()), "the pending write keeps the items");
    assert_eq!(anew.pending().await.unwrap(), 1);
    drop((anew, third));
    std::fs::remove_dir_all(home).unwrap();
}
