use ants_proto::SENSE_STREAM;
use ants_proto::provider::{BriefReason, BriefRequest, CallContext, Event, EventKind, Origin, Provider, Starter};
use ants_sense::Sense;
use ants_test_support::{NatsServer, eventually, free_port, scratch_dir};
use serde_json::json;

use super::*;
use crate::bus;

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

async fn link(server: &NatsServer) -> Link {
    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    Link::new(js, None, vec![], Machine::from_hostname("studio"))
}

#[tokio::test]
async fn nothing_is_kept_without_a_host_connection_and_nothing_reached_while_it_connects() {
    let stores = SenseStores::new();
    let home = scratch_dir("sense-stores-off");
    let kept = async |stores: &SenseStores| stores.items(&dev_a()).await.err().unwrap();
    stores.serve(&dev_a(), &home, None);
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

#[tokio::test]
async fn an_ant_s_items_open_on_first_use_with_its_sense_id_and_log() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores");
    let stores = SenseStores::new();
    stores.serve(&dev_a(), &home, None);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    assert!(!home.join(".sense").exists(), "nothing opened before the first use");

    let sense = Sense::with_stores(stores.clone());
    sense.event(added("id1")).await.unwrap();
    let id = identity::read_id(&home).unwrap().expect("minted");
    let card = sense.card(&dev_a()).await.unwrap().unwrap();
    assert_eq!((&card.sense, card.identities.as_slice()), (&id, &["id1".to_owned()][..]));
    assert!(home.join(".sense/cache.db").exists());
    let first = stores.items(&dev_a()).await.unwrap();
    assert!(Arc::ptr_eq(&first, &stores.items(&dev_a()).await.unwrap()), "opened once");

    let again = stores.hosting();
    let opened = |stores: &SenseStores| stores.state().ants[&dev_a()].items.try_lock().unwrap().is_some();
    assert!(opened(&stores), "cached items survive the next connection starting");
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
    assert!(opened(&stores), "the next connection's items are kept");
    drop(again);
    assert!(opened(&stores), "cached items remain available without a host link");

    std::fs::remove_file(home.join(".sense/cache.db")).unwrap();
    let elsewhere = SenseStores::new();
    elsewhere.serve(&dev_a(), &home, None);
    let hosting = elsewhere.hosting();
    hosting.up(link(&server).await);
    let sense = Sense::with_stores(elsewhere.clone());
    sense.event(added("id2")).await.unwrap();
    let card = sense.card(&dev_a()).await.unwrap().unwrap();
    assert_eq!((&card.sense, card.identities.as_slice()), (&id, &["id1".to_owned(), "id2".to_owned()][..]));
    assert!(card.returned(), "read back from the log, the cache gone");
    assert_eq!(identity::read_id(&home).unwrap(), Some(id));

    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    let messages = js.get_stream(SENSE_STREAM).await.unwrap().info().await.unwrap().state.messages;
    assert_eq!(messages, 2);
    std::fs::remove_dir_all(&home).unwrap();
}

/// An ant imported under another name on the same bus, its home holding its sense id and listing its old name, keeps
/// its card, as read again from its log alone.
#[tokio::test]
async fn a_renamed_ant_keeps_its_card() {
    let Some(server) = NatsServer::start() else { return };
    let (home_a, home_b) = (scratch_dir("sense-stores-a"), scratch_dir("sense-stores-b"));
    let dev_b: UserId = "dev-b".parse().unwrap();
    let stores = SenseStores::new();
    stores.serve(&dev_a(), &home_a, None);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    Sense::with_stores(stores.clone()).event(added("id1")).await.unwrap();
    let id = identity::read_id(&home_a).unwrap().expect("minted");
    drop(hosting);

    std::fs::create_dir_all(identity::id_file(&home_b).parent().unwrap()).unwrap();
    std::fs::copy(identity::id_file(&home_a), identity::id_file(&home_b)).unwrap();
    identity::note_renamed(&home_b, &dev_a()).unwrap();
    for reopened in [false, true] {
        let stores = SenseStores::new();
        stores.serve(&dev_b, &home_b, None);
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
        let card = sense.card(&dev_b).await.unwrap().unwrap();
        assert_eq!((&card.sense, &card.name, &card.earlier_names[..]), (&id, &dev_b, &[dev_a()][..]));
        assert!(card.moved_at.is_some() && card.identities == ["id1"]);
        assert_eq!(stores.items(&dev_b).await.unwrap().broken().await, None);
    }
    std::fs::remove_dir_all(&home_a).unwrap();
    std::fs::remove_dir_all(&home_b).unwrap();
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
    stores.serve(&dev_a(), &home, None);
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
    stores.serve(&dev_a(), &home, None);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    let items = stores.items(&dev_a()).await.unwrap();
    items.create("note", json!({"title":"online"}).as_object().unwrap().clone(), "t").await.unwrap();
    drop(hosting);
    items.create("note", json!({"title":"offline"}).as_object().unwrap().clone(), "t").await.unwrap();
    assert_eq!(items.pending().await.unwrap(), 1);
    drop((items, stores));

    let stores = SenseStores::new();
    stores.serve(&dev_a(), &home, None);
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
    stores.serve(&dev_a(), &home, None);
    let hosting = stores.hosting();
    let first_link = link(&server).await;
    let js = first_link.js.clone();
    hosting.up(first_link);
    let items = stores.items(&dev_a()).await.unwrap();
    drop(hosting);
    items.create("note", json!({"title":"queued"}).as_object().unwrap().clone(), "t").await.unwrap();
    drop((items, stores));

    let stores = SenseStores::new();
    stores.serve(&dev_a(), &home, None);
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
    stores.serve(&dev_a(), &home, None);
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
    stores.serve(&dev_a(), &home, None);
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
