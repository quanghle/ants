use ants_daemon::hub;
use ants_proto::{STREAM, UserId, subject};

use super::*;
use crate::auth::{SERVER_CONF, account_key, init, store_key, write_server_conf};
use crate::people::humans::Rights;
use crate::test_support::Server;

/// The state of every stream of the hub holding messages whose info `rights` get: messages, sequences, timestamps and consumers.
async fn states(js: &async_nats::jetstream::Context, rights: &Rights) -> Vec<(String, String)> {
    let mut states = Vec::new();
    let objects = hub::Object::ALL.into_iter().chain([hub::Object::Stream]);
    for object in objects.filter(|o| rights.may_publish(&o.info_subject())) {
        let name = object.stream_name();
        // Some buckets are created only once used, by any connection.
        if let Ok(mut stream) = js.get_stream(&name).await {
            let state = stream.info().await.unwrap().state.clone();
            if state.messages > 0 {
                states.push((name, format!("{state:?}")));
            }
        }
    }
    states.sort();
    states.dedup();
    states
}

/// A hub from before the store account keeps every entry of its streams and buckets, sequences and timestamps
/// included, once moved: stopped, given the server.conf with the store account, and its streams moved into it.
#[tokio::test]
async fn a_hub_moved_to_the_store_account_keeps_every_entry() {
    let Some(mut server) = Server::start_before_store() else { return };
    let human = UserId::human();
    let (client, js) = server.connect(&human).await;
    let rights = Rights::from_creds(Some(&server.creds(&human))).unwrap();
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let dev_a: UserId = "dev-a".parse().unwrap();
    for text in ["one", "two"] {
        js.publish(subject::inbox(&dev_a, &human), text.into()).await.unwrap().await.unwrap();
    }
    hub::ensure_bucket(&js, hub::Object::Read).await.unwrap();
    js.publish(subject::read_marker(&human), "read".into()).await.unwrap().await.unwrap();
    let before = states(&js, &rights).await;
    let names: Vec<&str> = before.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, [STREAM, "KV_ants_read"]);
    let first = js.get_stream(STREAM).await.unwrap().get_raw_message(1).await.unwrap();
    drop((client, js));
    server.stop();

    let team = server.dir().join("auth");
    write_server_conf(&team, &crate::auth::Revocations::default()).unwrap();
    let moved = move_stores(&server.dir().join("js"), &team.join(SERVER_CONF), false).unwrap();
    assert!(moved.streams.contains(&STREAM.to_owned()), "{moved:?}");
    assert_eq!((moved.replaced.len(), moved.left), (0, None));
    let jetstream = server.dir().join("js/jetstream");
    assert!(!jetstream.join(account_key(&team).unwrap()).exists());
    assert!(jetstream.join(store_key(&team).unwrap()).join("streams").join(STREAM).is_dir());

    server.start_again();
    let (_client, js) = server.connect(&human).await;
    assert_eq!(states(&js, &rights).await, before);
    let again = js.get_stream(STREAM).await.unwrap().get_raw_message(1).await.unwrap();
    assert_eq!((again.subject, again.payload, again.time), (first.subject, first.payload, first.time));
}

/// Streams created in the store account before the move fail it, unless replaced; a server.conf without the store
/// account and a directory without JetStream data move nothing.
#[test]
fn streams_already_in_the_store_account_are_replaced_only_when_asked() {
    let dir = ants_test_support::scratch_dir("move-stores");
    let team = dir.join("auth");
    init(&team).unwrap();
    let conf = team.join(SERVER_CONF);
    let jetstream = dir.join("js/jetstream");
    let (from, to) = (jetstream.join(account_key(&team).unwrap()), jetstream.join(store_key(&team).unwrap()));
    for (account, stream, file) in [(&from, "ANTS", "old"), (&from, "KV_a", "old"), (&to, "ANTS", "new")] {
        fs::create_dir_all(account.join("streams").join(stream)).unwrap();
        fs::write(account.join("streams").join(stream).join(file), "").unwrap();
    }
    fs::write(from.join("other"), "").unwrap();

    let e = move_stores(&dir.join("js"), &conf, false).unwrap_err();
    assert!(format!("{e:#}").contains("already holds ANTS") && format!("{e:#}").contains("--replace"), "{e:#}");
    assert!(from.join("streams/ANTS/old").is_file(), "nothing moved");
    let moved = move_stores(&dir.join("js"), &conf, true).unwrap();
    assert_eq!(
        moved,
        Moved { streams: vec!["ANTS".into(), "KV_a".into()], replaced: vec!["ANTS".into()], left: Some(from.clone()) }
    );
    assert!(to.join("streams/ANTS/old").is_file() && !to.join("streams/ANTS/new").exists());
    assert!(to.join("streams/KV_a/old").is_file() && !from.join("streams").exists());
    assert_eq!(move_stores(&dir.join("js"), &conf, false).unwrap().streams, Vec::<String>::new(), "moved already");

    let e = move_stores(&dir, &conf, false).unwrap_err();
    assert!(format!("{e:#}").contains("no jetstream directory"), "{e:#}");
    crate::auth::write_server_conf_before_store(&team).unwrap();
    let e = move_stores(&dir.join("js"), &conf, false).unwrap_err();
    assert!(format!("{e:#}").contains("preloads no store account"), "{e:#}");
    fs::remove_dir_all(dir).unwrap();
}
