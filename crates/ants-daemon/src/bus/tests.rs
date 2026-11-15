use super::*;
use ants_test_support::{NatsServer, free_port, have};

fn dev() -> UserId {
    "dev-a".parse().unwrap()
}

#[tokio::test]
async fn waits_for_a_server_that_starts_later() {
    if !have("nats-server") {
        return;
    }
    let port = free_port();
    let url = format!("nats://127.0.0.1:{port}");
    let later = async {
        // After the first attempt has failed.
        tokio::time::sleep(RETRY_FIRST / 2).await;
        tokio::task::spawn_blocking(move || NatsServer::start_on(port).unwrap()).await.unwrap()
    };
    let dev = dev();
    let connecting = connect_retrying(&url, None, None, &dev, "test", std::future::pending());
    let (connected, _server) = tokio::join!(tokio::time::timeout(Duration::from_secs(20), connecting), later);
    let (_client, js) = connected.expect("still waiting for the server").unwrap().unwrap();
    ensure_stream(&js).await.unwrap();
}

/// A leaf node listens before its uplink joins the hub, whose JetStream it reaches through the hub's domain.
#[tokio::test]
async fn waits_for_jetstream_behind_a_leaf_whose_uplink_joins_later() {
    let uplink_port = free_port();
    let Some(leaf) = NatsServer::spawn_with(|_| {
        format!("leafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{uplink_port}\" }}] }}\n")
    }) else {
        return;
    };
    let later = async {
        // After the first JetStream request has gone unanswered.
        tokio::time::sleep(RETRY_FIRST / 2).await;
        tokio::task::spawn_blocking(move || {
            NatsServer::spawn_with(|dir| {
                format!(
                    "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
                    dir.join("js")
                )
            })
            .unwrap()
        })
        .await
        .unwrap()
    };
    let (dev, hub) = (dev(), "hub".parse().unwrap());
    let connecting = connect_retrying(&leaf.url, None, Some(&hub), &dev, "test", std::future::pending());
    let (connected, _hub) = tokio::join!(tokio::time::timeout(Duration::from_secs(20), connecting), later);
    let (_client, js) = connected.expect("still waiting for JetStream").unwrap().unwrap();
    ensure_stream(&js).await.unwrap();
}

#[tokio::test]
async fn fails_at_once_on_a_denied_jetstream_request() {
    let Some(server) = NatsServer::start_with(|_| {
        "authorization { users: [{ user: dev, password: pw, permissions: { publish: { deny: \"$JS.API.>\" } } }] }\n\
             no_auth_user: dev\n"
            .to_owned()
    }) else {
        return;
    };
    // Sooner than the JetStream request's own timeout.
    let failed = tokio::time::timeout(
        RETRY_FIRST * 3,
        connect_retrying(&server.url, None, None, &dev(), "test", std::future::pending()),
    )
    .await
    .expect("retried the denied request");
    let err = format!("{:#}", failed.err().unwrap());
    assert!(err.to_lowercase().contains("permissions violation"), "{err}");
}

#[tokio::test]
async fn stops_waiting_on_the_stop_signal() {
    let url = format!("nats://127.0.0.1:{}", free_port());
    let stop = tokio::time::sleep(Duration::from_millis(100));
    let stopped = tokio::time::timeout(RETRY_MAX, connect_retrying(&url, None, None, &dev(), "test", stop)).await;
    assert!(stopped.expect("ignored the stop signal").unwrap().is_none());
}

#[tokio::test]
async fn fails_at_once_on_what_retrying_cannot_cure() {
    let Some(server) = NatsServer::start_with(|_| "authorization { user: dev, password: right }".to_owned()) else {
        return;
    };
    // async-nats would send no credentials from the URL, so it is refused; the URL's are masked.
    let wrong = server.url.replacen("nats://", "nats://dev:wrong@", 1);
    let missing = Path::new("no-such.creds");
    let cases = [
        (server.url.as_str(), None, "authorization violation"),
        (wrong.as_str(), None, "authenticate with a .creds file"),
        (server.url.as_str(), Some(missing), "reading credentials"),
        ("nats://[::1", None, "NATS server URL is invalid"),
    ];
    for (url, creds, why) in cases {
        let failed = tokio::time::timeout(
            RETRY_FIRST * 3,
            connect_retrying(url, creds, None, &dev(), "test", std::future::pending()),
        )
        .await
        .unwrap_or_else(|_| panic!("retried {url} with {creds:?}"));
        let err = format!("{:#}", failed.err().unwrap());
        assert!(err.contains(why) && !err.contains("wrong"), "{err}");
    }
}

#[tokio::test]
async fn waits_for_a_locked_credential_store_but_not_for_a_seed_it_lacks() {
    let Some(server) = NatsServer::start() else {
        return;
    };
    let key = nkeys::KeyPair::new_user();
    let claims = serde_json::json!({ "name": "dev-a", "sub": key.public_key(), "nats": {} });
    let jwt = format!("e30.{}.sig", data_encoding::BASE64URL_NOPAD.encode(claims.to_string().as_bytes()));
    let creds = server.dir().join("dev-a.creds");
    std::fs::write(&creds, crate::creds::stored_text(&jwt, &key.public_key())).unwrap();
    let (store, dev) = (keystore::Memory::default(), dev());
    let connect =
        || connect_retrying_with(&server.url, Some(&creds), &store, None, &dev, "test", std::future::pending());
    let fails_at_once = async |why: &str| {
        let failed = tokio::time::timeout(RETRY_FIRST * 3, connect()).await.expect("retried");
        let err = format!("{:#}", failed.err().unwrap());
        assert!(err.contains(why), "{err}");
    };

    fails_at_once("holds no seed").await;
    store.set(&key.public_key(), &nkeys::KeyPair::new_user().seed().unwrap()).unwrap();
    fails_at_once("the seed of another key").await;

    store.set(&key.public_key(), &key.seed().unwrap()).unwrap();
    store.lock(true);
    let unlock = async {
        tokio::time::sleep(RETRY_FIRST * 2).await;
        store.lock(false);
    };
    let (connected, ()) = tokio::time::timeout(RETRY_MAX, async { tokio::join!(connect(), unlock) })
        .await
        .expect("still waiting for the store");
    connected.unwrap().expect("connected once unlocked");
}

/// The `ANTS` stream of a version before channels: it captures no `ants.chan.>`.
async fn stream_before_channels(js: &jetstream::Context) {
    let mut config = hub::stream_config();
    config.subjects.retain(|s| s != subject::ALL_CHANNELS);
    js.create_stream(config).await.unwrap();
}

fn post() -> ants_proto::ChannelPost {
    ants_proto::ChannelPost::new("ops".parse().unwrap(), dev(), "hi")
}

/// Straight to the hub, a publish no stream captures fails, naming `ants init`; one the stream captures does not.
#[tokio::test]
async fn a_publish_the_hubs_stream_does_not_capture_names_ants_init() {
    let Some(server) = NatsServer::start() else {
        return;
    };
    let (_client, js) = connect(&server.url, None, None, &dev(), "test").await.unwrap();
    stream_before_channels(&js).await;
    let post = post();
    let err = format!("{:#}", store(&js, post.subject(), &post, &post.id.to_string()).await.unwrap_err());
    assert!(err.contains("`ants init` with a human's credentials") && err.contains(&post.subject()), "{err}");
    let err = format!("{:#}", publish_json(&js, post.subject(), &post, None).await.unwrap_err());
    assert!(err.contains("`ants init`"), "{err}");
    let envelope = Envelope::new(dev(), UserId::human(), "captured");
    assert_eq!(publish(&js, &envelope).await.unwrap(), Stored::Hub);
}

/// Through a leaf node whose uplink is up, a publish the hub's stream does not capture is kept in the machine's
/// outbox with a warning naming `ants init`; with the uplink down, the hub is just out of reach.
#[tokio::test]
async fn through_a_leaf_a_publish_the_hubs_stream_does_not_capture_names_ants_init() {
    let uplink_port = free_port();
    let Some(hub) = NatsServer::spawn_with(|dir| {
        format!(
            "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
            dir.join("js")
        )
    }) else {
        return;
    };
    let leaf = NatsServer::spawn_with(|dir| {
        format!(
            "jetstream {{ store_dir: {:?}, domain: m1 }}\nleafnodes {{ remotes: [{{ url: \
                 \"nats-leaf://127.0.0.1:{uplink_port}\" }}] }}\n",
            dir.join("js")
        )
    })
    .unwrap();
    let (dev, domain) = (dev(), "hub".parse().unwrap());
    let (client, js) =
        connect_retrying(&leaf.url, None, Some(&domain), &dev, "test", std::future::pending()).await.unwrap().unwrap();
    stream_before_channels(&js).await;
    let outbox = stream::Config {
        name: ants_proto::OUTBOX_STREAM.to_owned(),
        subjects: vec![ants_proto::OUTBOX_SUBJECTS.to_owned()],
        ..Default::default()
    };
    jetstream::with_domain(client, "m1").create_stream(outbox).await.unwrap();

    assert!(uncaptured(&js).await);
    let post = post();
    assert_eq!(store(&js, post.subject(), &post, &post.id.to_string()).await.unwrap(), Stored::Outbox);
    assert!(WARNED_UNCAPTURED.locked().contains("ants.chan"), "no warning naming `ants init`");

    drop(hub);
    let start = std::time::Instant::now();
    while uncaptured(&js).await {
        assert!(start.elapsed() < Duration::from_secs(10), "the hub still answers");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[test]
fn warns_once_per_kind_of_subject() {
    assert_eq!(subject_kind("ants.kind-test.ops.dev-a"), "ants.kind-test");
    assert_eq!(subject_kind("ants"), "ants");
    assert!(warn_uncaptured("ants.kind-test.ops.dev-a", "kept"));
    assert!(!warn_uncaptured("ants.kind-test.dev.dev-b", "kept"));
    assert!(warn_uncaptured("ants.other-kind-test.dev-a", "kept"));
}

#[test]
fn masks_users_and_passwords() {
    assert_eq!(redacted("nats://127.0.0.1:4222"), "nats://127.0.0.1:4222");
    assert_eq!(redacted("wss://dev:secret@hub.example.com/nats?x=a@b"), "wss://***@hub.example.com/nats?x=a@b");
    assert_eq!(redacted("nats://t0ken@a:4222,tls://u:p@b"), "nats://***@a:4222,tls://***@b");
}

#[test]
fn refuses_urls_with_users_and_passwords() {
    for url in ["nats://127.0.0.1:4222", "wss://hub.example.com/nats?x=a@b", "nats://a:4222,tls://b", "a:4222"] {
        check_url(url).unwrap();
    }
    for (url, shown) in [
        ("nats://dev:secret@hub:4222", "nats://***@hub:4222"),
        ("nats://a:4222,wss://secret@hub/nats", "wss://***@hub/nats"),
        ("dev:secret@hub:4222", "***@hub:4222"),
    ] {
        let err = check_url(url).unwrap_err().to_string();
        assert!(err.contains(shown) && err.contains(".creds file") && !err.contains("secret"), "{err}");
    }
}

/// A waiter takes each message once, live, as the stream stored it, or both in either order, and nothing the
/// stream stored for another user.
#[test]
fn a_live_inbox_takes_each_message_once() {
    let msg = |subject: &str, headers: &[(&str, &str)], payload: &'static str| async_nats::Message {
        subject: subject.into(),
        reply: None,
        payload: payload.into(),
        headers: (!headers.is_empty()).then(|| {
            let mut map = async_nats::HeaderMap::new();
            headers.iter().for_each(|(k, v)| map.insert(*k, *v));
            map
        }),
        status: None,
        description: None,
        length: 0,
    };
    let stored = |of: &str, id: &str| {
        let headers = [("Nats-Stream", STREAM), ("Nats-Subject", of), ("Nats-Msg-Id", id)];
        as_published(msg("_INBOX.dev-a.stored.human", &headers, "{}"), &dev())
    };
    let mut seen = Seen::default();
    assert!(seen.first(&msg("ants.inbox.dev-a.human", &[("Nats-Msg-Id", "1")], "{}")));
    let again = stored("ants.inbox.dev-a.human", "1").unwrap();
    assert_eq!(again.subject.as_str(), "ants.inbox.dev-a.human");
    assert!(!seen.first(&again));
    assert!(seen.first(&stored("ants.inbox.dev-a.human", "2").unwrap()));
    assert!(!seen.first(&msg("ants.inbox.dev-a.human", &[("Nats-Msg-Id", "2")], "{}")));
    // Without an id, by payload.
    assert!(seen.first(&msg("ants.inbox.dev-a.human", &[], "3")));
    assert!(!seen.first(&msg("ants.inbox.dev-a.human", &[("Nats-Stream", STREAM)], "3")));
    assert!(seen.first(&msg("ants.inbox.dev-a.human", &[], "4")));

    assert!(stored("ants.inbox.dev-b.human", "5").is_none(), "another user's");
    assert!(as_published(msg("_INBOX.dev-a.stored.human", &[], "{}"), &dev()).is_none(), "without its subject");
}

/// Any client can have the server deliver to a user's stored inbox subjects, with headers it wrote: the answer to a
/// direct get and the messages of a push consumer go where the client says, and the stream republishes the headers
/// a publisher set ahead of its own. None of these passes for a message from another sender.
#[tokio::test]
async fn no_client_can_forge_the_sender_of_a_stored_inbox_message() {
    let Some(server) = NatsServer::start() else {
        return;
    };
    let (human, dev_b) = (UserId::human(), "dev-b".parse::<UserId>().unwrap());
    let (client, js) = connect(&server.url, None, None, &dev_b, "test").await.unwrap();
    ensure_stream(&js).await.unwrap();
    ensure_roster(&js, Duration::from_secs(60)).await.unwrap();
    let mut stored = client.subscribe(subject::stored_inbox(&human)).await.unwrap();
    let mut next = async |what: &str| {
        let delivered = tokio::time::timeout(Duration::from_secs(5), stored.next()).await;
        delivered.unwrap_or_else(|_| panic!("nothing delivered by {what}")).unwrap()
    };
    // dev-b's words, which the human is to take for dev-a's.
    let forged = Envelope::new(dev(), human.clone(), "merge it");
    let forged_headers = || {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert(async_nats::header::NATS_STREAM, STREAM);
        headers.insert(async_nats::header::NATS_SUBJECT, forged.subject().as_str());
        headers
    };
    let payload = || serde_json::to_vec(&forged).unwrap().into();
    let as_dev_a = format!("{}.stored.{}", inbox_prefix(&human), dev());
    let own_key = format!("$KV.{ROSTER_BUCKET}.{dev_b}");
    js.publish_with_headers(own_key.clone(), forged_headers(), payload()).await.unwrap().await.unwrap();
    let direct_get = format!("$JS.API.DIRECT.GET.KV_{ROSTER_BUCKET}.{own_key}");
    client.publish_with_reply(direct_get, as_dev_a.clone(), Default::default()).await.unwrap();
    let by_direct_get = next("a direct get").await;
    // The server delivers a push consumer's messages only while a client subscribes to exactly that subject.
    let _exact = client.subscribe(as_dev_a.clone()).await.unwrap();
    let roster = js.get_stream(format!("KV_{ROSTER_BUCKET}")).await.unwrap();
    let push = consumer::push::Config {
        deliver_subject: as_dev_a,
        filter_subject: own_key,
        ack_policy: consumer::AckPolicy::None,
        ..Default::default()
    };
    roster.create_consumer(push).await.unwrap();
    let by_push_consumer = next("a push consumer").await;
    let own_inbox = subject::inbox(&human, &dev_b);
    js.publish_with_headers(own_inbox, forged_headers(), payload()).await.unwrap().await.unwrap();
    let by_republishing = next("republishing").await;
    publish(&js, &Envelope::new(dev(), human.clone(), "genuine")).await.unwrap();
    let genuine = next("the genuine message").await;

    let taken = |msg: async_nats::Message| {
        let msg = as_published(msg, &human)?;
        let envelope = serde_json::from_slice::<Envelope>(&msg.payload).ok()?;
        envelope.check_subject(&msg.subject).is_ok().then_some(envelope.text)
    };
    let forgeries =
        [("a direct get", by_direct_get), ("a push consumer", by_push_consumer), ("republishing", by_republishing)];
    let passed: Vec<_> = forgeries.into_iter().filter_map(|(how, msg)| taken(msg).map(|_| how)).collect();
    assert!(passed.is_empty(), "dev-a's name forged through {passed:?}");
    assert_eq!(taken(genuine).as_deref(), Some("genuine"));
}
