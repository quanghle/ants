//! Tests of the calls of one ant that come together: they share a catch-up with the log rather than each making one
//! in turn.

use futures::StreamExt as _;

use super::*;

#[tokio::test]
async fn calls_of_an_ant_that_come_together_share_a_catch_up() {
    let Some(server) = NatsServer::start() else { return };
    let home = scratch_dir("sense-stores-sharing");
    let stores = SenseStores::new();
    serve(&stores, &dev_a(), &home);
    let hosting = stores.hosting();
    hosting.up(link(&server).await);
    let items = stores.synced(&dev_a()).await.unwrap();
    items.create("note", json!({"title":"kept"}).as_object().unwrap().clone(), "t").await.unwrap();

    let client = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap().0;
    #[expect(clippy::disallowed_methods, reason = "counting the direct gets asked of the stream")]
    let mut gets = client.subscribe(format!("$JS.API.DIRECT.GET.{SENSE_STREAM}")).await.unwrap();
    client.flush().await.unwrap();
    let ant = dev_a();
    let calls = futures::future::join_all((0..8).map(|_| stores.synced(&ant))).await;
    for call in calls {
        assert!(Arc::ptr_eq(&call.unwrap(), &items));
    }
    // The calls are answered, so the server has their gets already: a message published now reaches the
    // subscription after them, however long a loaded machine takes to deliver either.
    client.publish(format!("$JS.API.DIRECT.GET.{SENSE_STREAM}"), "end".into()).await.unwrap();
    let mut asked = 0;
    while tokio::time::timeout(Duration::from_secs(30), gets.next())
        .await
        .unwrap()
        .is_some_and(|get| get.payload != "end")
    {
        asked += 1;
    }
    // One read for the first call and one shared by those that came during it, or, on a loaded machine where all
    // eight came before the first read went out, the one read shared by all: never a read each.
    assert!((1..=2).contains(&asked), "{asked} reads of the log for eight calls coming together");
    drop((items, hosting));
    std::fs::remove_dir_all(home).unwrap();
}
