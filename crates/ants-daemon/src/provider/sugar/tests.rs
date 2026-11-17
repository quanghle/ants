use ants_proto::SUGAR_STREAM;
use ants_proto::provider::{Event, EventKind, Provider};
use ants_sugar::Sugar;
use ants_test_support::{NatsServer, scratch_dir};

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
    let stores = SugarStores::new();
    let home = scratch_dir("sugar-stores-off");
    let kept = async |stores: &SugarStores| stores.items(&dev_a()).await.err().unwrap();
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
    assert!(!home.join(".sugar").exists(), "no sugar id minted");
    std::fs::remove_dir_all(&home).unwrap();
}

#[tokio::test]
async fn an_ant_s_items_open_on_first_use_with_its_sugar_id_and_log() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sugar-stores");
    let stores = SugarStores::new();
    stores.serve(&dev_a(), &home, None);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    assert!(!home.join(".sugar").exists(), "nothing opened before the first use");

    let sugar = Sugar::with_stores(stores.clone());
    sugar.event(added("id1")).await.unwrap();
    let id = identity::read_id(&home).unwrap().expect("minted");
    let card = sugar.card(&dev_a()).await.unwrap().unwrap();
    assert_eq!((&card.sugar, card.identities.as_slice()), (&id, &["id1".to_owned()][..]));
    assert!(home.join(".sugar/cache.db").exists());
    let first = stores.items(&dev_a()).await.unwrap();
    assert!(Arc::ptr_eq(&first, &stores.items(&dev_a()).await.unwrap()), "opened once");

    let again = stores.hosting();
    let opened = |stores: &SugarStores| stores.state().ants[&dev_a()].items.try_lock().unwrap().is_some();
    assert!(!opened(&stores), "let go as the next connection starts");
    again.up(link(&server).await);
    assert!(!Arc::ptr_eq(&first, &stores.items(&dev_a()).await.unwrap()), "opened again over the new connection");
    drop((hosting, first));
    assert!(opened(&stores), "the next connection's items are kept");
    drop(again);
    assert!(!opened(&stores));

    std::fs::remove_file(home.join(".sugar/cache.db")).unwrap();
    let elsewhere = SugarStores::new();
    elsewhere.serve(&dev_a(), &home, None);
    let hosting = elsewhere.hosting();
    hosting.up(link(&server).await);
    let sugar = Sugar::with_stores(elsewhere.clone());
    sugar.event(added("id2")).await.unwrap();
    let card = sugar.card(&dev_a()).await.unwrap().unwrap();
    assert_eq!((&card.sugar, card.identities.as_slice()), (&id, &["id1".to_owned(), "id2".to_owned()][..]));
    assert!(card.returned(), "read back from the log, the cache gone");
    assert_eq!(identity::read_id(&home).unwrap(), Some(id));

    let js = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap().1;
    let messages = js.get_stream(SUGAR_STREAM).await.unwrap().info().await.unwrap().state.messages;
    assert_eq!(messages, 2);
    std::fs::remove_dir_all(&home).unwrap();
}
