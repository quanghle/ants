use std::assert_matches;
use std::process::Command;

use ants_daemon::records::{links, owners};
use ants_daemon::stream::notes;
use ants_daemon::stream::reading::Reading;
use ants_daemon::{bus, files};
use ants_proto::{ATTACHMENT_CHUNK, ChannelId, ChannelPost, DeadLetter, Envelope, JsDomain, NotesPost, ThreadId};
use ants_test_support::{NatsServer, free_port};
use async_nats::jetstream::message::{PublishMessage, StreamMessage};
use futures::StreamExt as _;

use super::*;
use crate::node::leaf;
use crate::node::members::EnrollApprover;
use crate::people::humans::Me;
use crate::test_support::{eventually, spawn_hub};
use crate::{auth, channel};

/// The next message a waiter takes.
async fn next(replies: &mut bus::LiveInbox) -> Envelope {
    let msg = tokio::time::timeout(Duration::from_secs(60), replies.next()).await.expect("no message").unwrap();
    let envelope: Envelope = serde_json::from_slice(&msg.payload).unwrap();
    envelope.check_subject(&msg.subject).unwrap();
    envelope
}

async fn messages(js: &Context, name: &str) -> u64 {
    js.get_stream(name).await.unwrap().info().await.unwrap().state.messages
}

/// A real hub and leaf node: what an ant on the leaf sends while the uplink is down waits in the leaf's
/// outbox and lands in the hub's stream once, after the uplink is back, and reaches a live waiter then; what the
/// machine may not send stays out.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one scenario: the uplink down, the outbox filling, then the hub taking it")]
async fn the_hub_takes_what_a_leaf_kept_while_its_uplink_was_down() {
    let (leaf_port, dead_port) = (free_port(), free_port());
    let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else {
        return;
    };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let (human, dev_a, dev_b): (UserId, UserId, UserId) =
        (UserId::human(), "dev-a".parse().unwrap(), "dev-b".parse().unwrap());
    let creds = |p: &UserId| {
        let out = hub.dir().join(format!("{p}.creds"));
        auth::issue(&team, p, Role::by_default(p), Some(&domain), &out).unwrap();
        out
    };
    let connect = async |url: &str, p: &UserId| {
        let (client, mut js) = bus::connect(url, Some(&creds(p)), Some(&domain), p, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(2));
        (client, js)
    };
    let (h, h_js) = connect(&hub.url, &human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    bus::ensure_links(&h_js).await.unwrap();
    bus::ensure_owners(&h_js).await.unwrap();
    h_js.create_stream(ants_daemon::hub::files_config()).await.unwrap();

    let leaf_creds = hub.dir().join("leaf-m1.creds");
    // The CLI runs on m1 too.
    let alice: UserId = "alice".parse().unwrap();
    let on_m1 = auth::members(&[dev_a.clone(), human.clone()], std::slice::from_ref(&alice));
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap();
    let server_conf = team.join(auth::SERVER_CONF);
    let listen = format!("127.0.0.1:{}", free_port());
    let configure = |dir: &Path, port: u16| {
        let uplinks = [format!("nats-leaf://127.0.0.1:{port}")];
        let config = leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &leaf_creds,
            system_creds: None,
            server_conf: &server_conf,
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
    };
    let leaf_node = NatsServer::spawn_with(|dir| {
        configure(dir, leaf_port);
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    let reload = |port: u16| {
        configure(leaf_node.dir(), port);
        let status = Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap();
        assert!(status.success());
    };
    let (_a, a_js) = connect(&leaf_node.url, &dev_a).await;
    let (_b, b_js) = connect(&leaf_node.url, &dev_b).await;
    eventually("the leaf reaches the hub", async || bus::open_stream(&a_js, &dev_a).await.is_ok()).await;
    // The human waits for replies as `ants ask --wait` does, off the machine and on it; on it, core NATS also
    // delivers what the hub never got.
    let (h_leaf, h_leaf_js) = connect(&leaf_node.url, &human).await;
    let mut waiters = [
        bus::caught_up_inbox(&h_leaf, &h_leaf_js, &human, Reading::Gets).await.unwrap(),
        bus::caught_up_inbox(&h, &h_js, &human, Reading::Gets).await.unwrap(),
    ];
    h.flush().await.unwrap();
    h_leaf.flush().await.unwrap();
    // So does dev-a's CLI on the machine, with dev-a's credentials.
    let (a_cli, a_cli_js) = connect(&leaf_node.url, &dev_a).await;
    let mut ant_waiter = bus::caught_up_inbox(&a_cli, &a_cli_js, &dev_a, Reading::Spare).await.unwrap();
    a_cli.flush().await.unwrap();

    // Before the outbox exists, a publish that cannot reach the hub fails outright.
    outbox_missing_fails(&b_js).await;
    let setup = super::connect(&hub.url, Some(&creds(&human)), &human).await.unwrap();
    admit(&setup, &h_js, &m1, &on_m1).await.unwrap();
    sourced(&h_js, &m1, Duration::from_secs(5)).await.unwrap();
    let found = sourcing(&h_js, &m1, &on_m1).await.unwrap();
    assert_eq!(found, Sourcing { hearing: Hearing::Heard, unadmitted: Vec::new() });
    let more = [on_m1.clone(), vec![(dev_b.clone(), Role::Ant)]].concat();
    assert_eq!(sourcing(&h_js, &m1, &more).await.unwrap().unadmitted, std::slice::from_ref(&dev_b));
    // Any human on the machine may set it up again, on the machine, through its leaf node.
    let alice_creds = hub.dir().join("alice.creds");
    auth::issue(&team, &alice, Role::Human, Some(&domain), &alice_creds).unwrap();
    let (_alice, alice_js) = bus::connect(&hub.url, Some(&alice_creds), Some(&domain), &alice, "test").await.unwrap();
    let setup = super::connect(&leaf_node.url, Some(&alice_creds), &alice).await.unwrap();
    admit(&setup, &alice_js, &m1, &on_m1).await.unwrap();

    // Stored directly, and a copy of it in the outbox is dropped as a duplicate.
    let direct = Envelope::new(dev_a.clone(), human.clone(), "direct");
    assert_eq!(bus::publish(&a_js, &direct).await.unwrap(), bus::Stored::Hub);
    let copy =
        PublishMessage::build().payload(serde_json::to_vec(&direct).unwrap().into()).message_id(direct.id.to_string());
    let outboxed = subject::outbox(&direct.subject()).unwrap();
    a_js.send_publish(outboxed, copy).await.unwrap().await.unwrap();
    // So is a copy of a chunk of an attachment stored directly.
    let note = files::reference("note.txt", "text/plain", b"direct").unwrap();
    let note_handle = Envelope::new(dev_a.clone(), human.clone(), "").attachment_handle(note.id);
    assert_eq!(files::put(&a_js, &note_handle, &note, b"direct").await.unwrap(), bus::Stored::Hub);
    let copy = PublishMessage::build().payload("direct".into()).message_id(files::chunk_id(&note_handle, 0));
    a_js.send_publish(note_handle.outbox_chunk_subject(0), copy).await.unwrap().await.unwrap();
    eventually("the copies reach the intake", async || messages(&h_js, INTAKE_STREAM).await == 2).await;
    // Setting it up again recreates a durable consumer the leaf node lost, after what the intake took.
    let leaf_js = jetstream::with_domain(setup.clone(), m1.as_str());
    leaf_js.delete_consumer_from_stream(OUTBOX_CONSUMER, OUTBOX_STREAM).await.unwrap();
    admit(&setup, &alice_js, &m1, &on_m1).await.unwrap();
    leaf_js.get_consumer_from_stream::<consumer::push::Config, _, _>(OUTBOX_CONSUMER, OUTBOX_STREAM).await.unwrap();
    sourced(&h_js, &m1, SOURCE_WAIT).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(messages(&h_js, INTAKE_STREAM).await, 2, "nothing taken twice");

    for replies in &mut waiters {
        assert_eq!(next(replies).await, direct);
    }

    reload(dead_port);
    eventually("the uplink is down", async || bus::open_stream(&a_js, &dev_a).await.is_err()).await;
    // The chunks of an attachment wait in the outbox ahead of the message carrying it.
    let shot_bytes: Vec<u8> = (0..ATTACHMENT_CHUNK + 10).map(|i| i as u8).collect();
    let shot = files::reference("shot.png", "image/png", &shot_bytes).unwrap();
    let mut kept = Envelope::new(dev_a.clone(), human.clone(), "kept");
    kept.attachments = vec![shot.clone()];
    let shot_handle = kept.attachment_handle(shot.id);
    assert_eq!(files::put(&a_js, &shot_handle, &shot, &shot_bytes).await.unwrap(), bus::Stored::Outbox);
    assert_eq!(files::put(&a_js, &shot_handle, &shot, &shot_bytes).await.unwrap(), bus::Stored::Outbox, "again");
    assert_eq!(bus::publish(&a_js, &kept).await.unwrap(), bus::Stored::Outbox);
    // A waiter that never reads back, as a daemon's, gets it only as the hub's stream republishes it.
    let mut live = bus::live_inbox(&h, &human).await.unwrap();
    h.flush().await.unwrap();
    let thread: ThreadId = "t1".parse().unwrap();
    assert!(notes::publish(&a_js, &NotesPost::new(thread.clone(), dev_a.clone(), "notes"), None).await.unwrap());
    let chan = ChannelPost::new("ops".parse().unwrap(), dev_a.clone(), "@dev-b kept");
    let stored = bus::store(&a_js, chan.subject(), &chan, &chan.id.to_string()).await.unwrap();
    assert_eq!(stored, bus::Stored::Outbox);
    let dead = DeadLetter::of_envelope(dev_a.clone(), 1, 5, "harness crashed", direct.clone());
    let stored = bus::store(&a_js, dead.subject(), &dead, &dead.msg_id()).await.unwrap();
    assert_eq!(stored, bus::Stored::Outbox);
    // dev-b may post these itself, but is not admitted from m1.
    let stray = NotesPost::new(thread.clone(), dev_b.clone(), "stray");
    assert!(notes::publish(&b_js, &stray, None).await.unwrap());
    let stray_dead = DeadLetter::of_envelope(dev_b.clone(), 2, 5, "stray", direct.clone());
    let stored = bus::store(&b_js, stray_dead.subject(), &stray_dead, &stray_dead.msg_id()).await.unwrap();
    assert_eq!(stored, bus::Stored::Outbox);
    // With a human on m1, the store takes chunks under every sender, as the stream does inboxes.
    let b_file = files::reference("b.txt", "text/plain", b"from dev-b").unwrap();
    let b_handle = stray.attachment_handle(b_file.id);
    assert_eq!(files::put(&b_js, &b_handle, &b_file, b"from dev-b").await.unwrap(), bus::Stored::Outbox);
    // Links passed on wait in the outbox too; dev-b's stay out of the links bucket like its posts.
    let channel_thread = ThreadId::generate();
    let spun = |from: &UserId, to: &UserId| {
        Envelope::on_thread(channel_thread.clone(), from.clone(), to.clone(), "")
            .with_creator(Some(dev_b.clone()))
            .spun_off(ThreadId::generate())
    };
    links::pass_on(&a_js, &spun(&dev_a, &dev_b)).await.unwrap();
    links::pass_on(&b_js, &spun(&dev_b, &dev_a)).await.unwrap();
    // So do the owners a human records, ahead of the message: a second record of a thread is dropped,
    // and does not hold up those after it.
    let (taken, asked, after) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    owners::record(&h_js, Some(&domain), &taken, &dev_a, &alice).await.unwrap();
    owners::record(&h_leaf_js, Some(&domain), &taken, &dev_a, &human).await.unwrap();
    owners::record(&h_leaf_js, Some(&domain), &asked, &dev_b, &human).await.unwrap();
    let ask = Envelope::on_thread(asked.clone(), human.clone(), dev_b.clone(), "asked").with_owner(Some(human.clone()));
    assert_eq!(bus::publish(&h_leaf_js, &ask).await.unwrap(), bus::Stored::Outbox);
    owners::record(&h_leaf_js, Some(&domain), &after, &dev_a, &human).await.unwrap();
    assert!(messages(&h_js, STREAM).await == 1);

    reload(leaf_port);
    eventually("the intake takes the outbox", async || messages(&h_js, INTAKE_STREAM).await == 17).await;
    eventually("the hub's stream takes what dev-a sent", async || messages(&h_js, STREAM).await == 6).await;
    eventually("the store takes the chunks", async || messages(&h_js, FILES_STREAM).await == 4).await;
    let (stored, bytes) = files::get(&h_js, &shot_handle, Some(&shot)).await.unwrap();
    assert_eq!((stored, bytes), (shot.clone(), shot_bytes));
    assert_eq!(files::get(&h_js, &note_handle, Some(&note)).await.unwrap().1, b"direct");
    assert_eq!(files::get(&h_js, &b_handle, Some(&b_file)).await.unwrap().1, b"from dev-b");
    let ants = h_js.get_stream(STREAM).await.unwrap();
    let last = ants.get_last_raw_message_by_subject(&kept.subject()).await.unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&last.payload).unwrap(), kept);
    ants.get_last_raw_message_by_subject(&subject::notes(&thread, &dev_a)).await.unwrap();
    let last = ants.get_last_raw_message_by_subject(&chan.subject()).await.unwrap();
    assert_eq!(serde_json::from_slice::<ChannelPost>(&last.payload).unwrap(), chan);
    let last = ants.get_last_raw_message_by_subject(&dead.subject()).await.unwrap();
    assert_eq!(serde_json::from_slice::<DeadLetter>(&last.payload).unwrap(), dead);
    assert!(ants.get_last_raw_message_by_subject(&stray.subject()).await.is_err());
    assert!(ants.get_last_raw_message_by_subject(&stray_dead.subject()).await.is_err());
    for replies in &mut waiters {
        assert_eq!(next(replies).await, kept, "a waiter gets the kept message, once the hub stores it");
    }
    assert_eq!(next(&mut live).await, kept, "and one that never reads back");
    let passed_on = async || links::passed_on(&h_js, &channel_thread).await.unwrap();
    eventually("the links bucket takes dev-a's link", async || !passed_on().await.is_empty()).await;
    assert_eq!(passed_on().await, [(dev_a.clone(), dev_b.clone())]);
    let read = owners::Owners::new(h_js.clone());
    eventually("the owners bucket takes the owners", async || {
        read.recorded(&after, &dev_a).await.unwrap() == Some(human.clone())
    })
    .await;
    assert_eq!(read.recorded(&asked, &dev_b).await.unwrap(), Some(human.clone()));
    assert_eq!(read.recorded(&taken, &dev_a).await.unwrap(), Some(alice.clone()), "the first record stays");
    // How a daemon tells a message from an outbox.
    let source = |msg: StreamMessage| msg.headers.get("Nats-Stream-Source").is_some();
    assert!(source(ants.get_last_raw_message_by_subject(&ask.subject()).await.unwrap()));
    let first = ants.get_raw_message(1).await.unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&first.payload).unwrap(), direct);
    assert!(!source(first));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(messages(&h_js, STREAM).await, 6, "stored once");
    assert_eq!(messages(&h_js, FILES_STREAM).await, 4, "each chunk stored once");
    for replies in &mut waiters {
        assert!(futures::FutureExt::now_or_never(replies.next()).is_none(), "each once, live or stored");
    }

    // What the hub stores while the uplink is down never comes live on the leaf; its waiter reads it back.
    reload(dead_port);
    eventually("the uplink is down again", async || bus::open_stream(&a_js, &dev_a).await.is_err()).await;
    let (_b_hub, b_hub_js) = connect(&hub.url, &dev_b).await;
    let away = Envelope::new(dev_b.clone(), human.clone(), "away");
    assert_eq!(bus::publish(&b_hub_js, &away).await.unwrap(), bus::Stored::Hub);
    let away_from_a = Envelope::new(dev_b.clone(), dev_a.clone(), "away");
    assert_eq!(bus::publish(&b_hub_js, &away_from_a).await.unwrap(), bus::Stored::Hub);
    assert_eq!(next(&mut waiters[1]).await, away);
    reload(leaf_port);
    assert_eq!(next(&mut waiters[0]).await, away, "the waiter on the leaf gets it once the uplink is back");
    assert_eq!(next(&mut ant_waiter).await, away_from_a, "and so does an ant's");
    let [on_leaf, on_hub] = &mut waiters;
    let wait = bus::CATCH_UP * 2;
    let (on_leaf, on_hub, ant) = tokio::join!(
        tokio::time::timeout(wait, on_leaf.next()),
        tokio::time::timeout(wait, on_hub.next()),
        tokio::time::timeout(wait, ant_waiter.next())
    );
    assert!(on_leaf.is_err() && on_hub.is_err() && ant.is_err(), "each once, live or read back");

    remove(&h_js, &m1).await.unwrap();
    for name in
        [STREAM.to_owned(), format!("KV_{LINKS_BUCKET}"), format!("KV_{OWNERS_BUCKET}"), FILES_STREAM.to_owned()]
    {
        let info = h_js.get_stream(name).await.unwrap().info().await.unwrap().config.clone();
        assert!(info.sources.unwrap_or_default().is_empty());
    }
}

/// A real hub and leaf node: `ants post --wait` gets each reply once, also those the hub stores while the leaf's
/// uplink is down and those it takes from a leaf's outbox, which never come live, read back with a human's
/// credentials and an ant's; for a post kept in the outbox, only what the hub stores after it.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one scenario: posts kept while the uplink is down, then read back")]
async fn a_post_waiter_reads_back_what_never_comes_live() {
    let (leaf_port, dead_port) = (free_port(), free_port());
    let Some(hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else {
        return;
    };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let (human, dev_a, dev_b): (UserId, UserId, UserId) =
        (UserId::human(), "dev-a".parse().unwrap(), "dev-b".parse().unwrap());
    let connect = async |url: &str, p: &UserId| {
        let creds = hub.dir().join(format!("{p}.creds"));
        auth::issue(&team, p, Role::by_default(p), Some(&domain), &creds).unwrap();
        let (client, mut js) = bus::connect(url, Some(&creds), Some(&domain), p, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(2));
        (client, js)
    };
    let (h, h_js) = connect(&hub.url, &human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_b, b_js) = connect(&hub.url, &dev_b).await;

    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let on_m1 = auth::members(&[dev_a.clone(), human.clone()], &[]);
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap();
    let server_conf = team.join(auth::SERVER_CONF);
    let listen = format!("127.0.0.1:{}", free_port());
    let configure = |dir: &Path, port: u16| {
        let uplinks = [format!("nats-leaf://127.0.0.1:{port}")];
        let config = leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &leaf_creds,
            system_creds: None,
            server_conf: &server_conf,
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
    };
    let leaf_node = NatsServer::spawn_with(|dir| {
        configure(dir, leaf_port);
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    let reload = async |port: u16, up: bool| {
        configure(leaf_node.dir(), port);
        let status = Command::new("kill").arg("-HUP").arg(leaf_node.pid().to_string()).status().unwrap();
        assert!(status.success());
        let (_, js) = connect(&leaf_node.url, &dev_a).await;
        eventually("the uplink goes up or down", async || bus::open_stream(&js, &dev_a).await.is_ok() == up).await;
    };
    let (h_leaf, h_leaf_js) = connect(&leaf_node.url, &human).await;
    let (a_leaf, a_leaf_js) = connect(&leaf_node.url, &dev_a).await;
    eventually("the leaf reaches the hub", async || bus::open_stream(&a_leaf_js, &dev_a).await.is_ok()).await;
    let setup = super::connect(&hub.url, Some(&hub.dir().join("human.creds")), &human).await.unwrap();
    admit(&setup, &h_js, &m1, &on_m1).await.unwrap();
    sourced(&h_js, &m1, Duration::from_secs(5)).await.unwrap();

    // Each poster waits as `ants post --wait` does: subscribed before it posts, reading back after its post.
    let ops: ChannelId = "ops".parse().unwrap();
    let wait = async |client: &async_nats::Client, js: &Context, post: &ChannelPost| {
        let room = client.subscribe(subject::chan_room(&ops)).await.unwrap();
        client.flush().await.unwrap();
        let stored = bus::store_acked(js, post.subject(), post, &post.id.to_string()).await.unwrap();
        let me = Me { id: post.from.clone(), role: Role::by_default(&post.from), assistant_of: None };
        let posts = channel::caught_up_posts(room.into(), js, &me, post, stored.as_ref().map(|ack| ack.sequence));
        (stored.is_some(), Box::pin(channel::thread_posts(posts, post, true)))
    };
    let next = async |waiter: &mut (dyn futures::Stream<Item = ChannelPost> + Unpin)| {
        tokio::time::timeout(Duration::from_secs(60), waiter.next()).await.expect("no post").unwrap()
    };
    let top = ChannelPost::new(ops.clone(), human.clone(), "on the leaf");
    let (stored, mut on_leaf) = wait(&h_leaf, &h_leaf_js, &top).await;
    assert!(stored);
    let ant_top = ChannelPost::new(ops.clone(), dev_a.clone(), "an ant on the leaf");
    let (stored, mut ant_on_leaf) = wait(&a_leaf, &a_leaf_js, &ant_top).await;
    assert!(stored);
    let hub_top = ChannelPost::new(ops.clone(), human.clone(), "on the hub");
    let (stored, mut on_hub) = wait(&h, &h_js, &hub_top).await;
    assert!(stored);

    reload(dead_port, false).await;
    let reply = |to: &ChannelPost, from: &UserId, text: &str| {
        channel::human_post(ops.clone(), Some(to.thread_id.clone()), from.clone(), None, text.to_owned())
    };
    let (away, ant_away) = (reply(&top, &dev_b, "away"), reply(&ant_top, &dev_b, "ant away"));
    for post in [&away, &ant_away] {
        assert_eq!(bus::store(&b_js, post.subject(), post, &post.id.to_string()).await.unwrap(), bus::Stored::Hub);
    }
    // Kept in the outbox, so never published on its subject.
    let outboxed = reply(&hub_top, &dev_a, "from the outbox");
    let stored = bus::store(&a_leaf_js, outboxed.subject(), &outboxed, &outboxed.id.to_string()).await.unwrap();
    assert_eq!(stored, bus::Stored::Outbox);
    let kept = ChannelPost::new(ops.clone(), human.clone(), "kept");
    let (stored, mut on_kept) = wait(&h_leaf, &h_leaf_js, &kept).await;
    assert!(!stored);
    let early = reply(&kept, &dev_b, "before the post reaches the hub");
    bus::store(&b_js, early.subject(), &early, &early.id.to_string()).await.unwrap();

    reload(leaf_port, true).await;
    assert_eq!(next(&mut on_leaf).await, away, "read back by message gets");
    assert_eq!(next(&mut ant_on_leaf).await, ant_away, "read back through a spare reader");
    assert_eq!(next(&mut on_hub).await, outboxed, "read back once the hub takes it from the outbox");
    let ants = h_js.get_stream(STREAM).await.unwrap();
    eventually("the hub takes the kept post", async || {
        ants.get_last_raw_message_by_subject(&kept.subject()).await.is_ok()
    })
    .await;

    reload(dead_port, false).await;
    let late = reply(&kept, &dev_b, "after the post");
    bus::store(&b_js, late.subject(), &late, &late.id.to_string()).await.unwrap();
    reload(leaf_port, true).await;
    assert_eq!(next(&mut on_kept).await, late, "only what the hub stored after the post kept in the outbox");

    let again = channel::CATCH_UP * 2;
    let quiet = async |waiter: &mut (dyn futures::Stream<Item = ChannelPost> + Unpin)| {
        tokio::time::timeout(again, waiter.next()).await.is_err()
    };
    let quiet = tokio::join!(quiet(&mut on_leaf), quiet(&mut ant_on_leaf), quiet(&mut on_hub), quiet(&mut on_kept));
    assert_eq!(quiet, (true, true, true, true), "each once, live or read back");
}

/// Without `js_api_reply_v2` the hub's intake never hears from a leaf's outbox, and setting it up says so.
#[tokio::test]
async fn a_hub_without_js_api_reply_v2_is_reported() {
    let leaf_port = free_port();
    let Some(hub) = spawn_hub(leaf_port, "") else {
        return;
    };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let human = UserId::human();
    let human_creds = hub.dir().join("human.creds");
    auth::issue(&team, &human, Role::Human, Some(&domain), &human_creds).unwrap();
    let (_h, h_js) = bus::connect(&hub.url, Some(&human_creds), Some(&domain), &human, "test").await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    let on_m1 = auth::members(std::slice::from_ref(&human), &[]);
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap();
    let (server_conf, listen) = (team.join(auth::SERVER_CONF), format!("127.0.0.1:{}", free_port()));
    let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
    let _leaf_node = NatsServer::spawn_with(|dir| {
        let config = leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &leaf_creds,
            system_creds: None,
            server_conf: &server_conf,
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();

    let setup = super::connect(&hub.url, Some(&human_creds), &human).await.unwrap();
    eventually("the leaf reaches the hub", async || admit(&setup, &h_js, &m1, &on_m1).await.is_ok()).await;
    let error = sourced(&h_js, &m1, Duration::from_secs(3)).await.unwrap_err().to_string();
    assert!(error.contains("feature_flags { js_api_reply_v2: true }"), "{error}");
    assert_eq!(sourcing(&h_js, &m1, &on_m1).await.unwrap().hearing, Hearing::Silent);
}

#[test]
fn the_intake_hears_from_an_outbox_lately_or_not_since_a_while_or_fails() {
    let m1: Machine = "m1".parse().unwrap();
    let info = |source: serde_json::Value| serde_json::json!({ "sources": [source] });
    let ours = serde_json::json!({ "api": "$JS.m1.API", "deliver": "$JS.S.m1" });
    assert_eq!(
        hearing(&info(serde_json::json!({ "name": OUTBOX_STREAM, "external": ours, "active": 12 })), &m1),
        Hearing::Heard
    );
    let stale = info(serde_json::json!({ "name": OUTBOX_STREAM, "external": ours, "active": 30_000_000_000_i64 }));
    assert_eq!(hearing(&stale, &m1), Hearing::Stale(Duration::from_secs(30)));
    let never = info(serde_json::json!({ "name": OUTBOX_STREAM, "external": ours, "active": -1 }));
    assert_eq!(hearing(&never, &m1), Hearing::Silent);
    let failed = info(serde_json::json!({
        "name": OUTBOX_STREAM, "external": ours, "active": -1, "error": { "code": 503, "description": "boom" },
    }));
    assert_eq!(hearing(&failed, &m1), Hearing::Failed("boom".to_owned()));
    let other = info(serde_json::json!({ "name": OUTBOX_STREAM, "external": { "api": "$JS.m2.API" }, "active": 1 }));
    assert_matches!(hearing(&other, &m1), Hearing::Failed(_));
}

async fn outbox_missing_fails(js: &Context) {
    // dev-b's inbox subject for dev-a has no stream on the leaf while the outbox is missing: ants.out.* is local.
    let envelope = Envelope::new("dev-b".parse().unwrap(), "dev-a".parse().unwrap(), "x");
    let outboxed = subject::outbox(&envelope.subject()).unwrap();
    let message = PublishMessage::build().payload(serde_json::to_vec(&envelope).unwrap().into());
    assert!(js.send_publish(outboxed, message).await.unwrap().await.is_err());
}

fn filters(sources: &[Source]) -> Vec<&str> {
    sources.iter().flat_map(|s| s.subject_transforms.iter().map(|t| t.source.as_str())).collect()
}

#[test]
fn with_humans_on_a_machine_no_two_admissions_overlap() {
    let m1: Machine = "m1".parse().unwrap();
    let ants = ["dev-a".parse().unwrap(), UserId::human(), "dev-b".parse().unwrap()];
    let sources = admissions(&m1, &auth::members(&ants, &[]), 7);
    assert_eq!(
        filters(&sources),
        [
            "ants.in.m1.ants.out.notes.*.dev-a",
            "ants.in.m1.ants.out.chan.*.dev-a",
            "ants.in.m1.ants.out.dead.dev-a",
            "ants.in.m1.ants.out.timeline.*.dev-a",
            "ants.in.m1.ants.out.notes.*.dev-b",
            "ants.in.m1.ants.out.chan.*.dev-b",
            "ants.in.m1.ants.out.dead.dev-b",
            "ants.in.m1.ants.out.timeline.*.dev-b",
            "ants.in.m1.ants.out.inbox.>",
            "ants.in.m1.ants.out.notes.*.human",
            "ants.in.m1.ants.out.chan.*.human",
        ]
    );
    assert!(sources.iter().all(|s| s.start_sequence == Some(7) && from_machine(s, &m1)));
    assert!(!from_machine(&sources[0], &"m".parse().unwrap()));

    // With a second human, the first by name takes every inbox subject; each human posts under its own name.
    let alice: UserId = "alice".parse().unwrap();
    let sources = admissions(&m1, &auth::members(&ants, &[alice.clone(), alice]), 7);
    assert_eq!(
        filters(&sources),
        [
            "ants.in.m1.ants.out.inbox.>",
            "ants.in.m1.ants.out.notes.*.alice",
            "ants.in.m1.ants.out.chan.*.alice",
            "ants.in.m1.ants.out.notes.*.dev-a",
            "ants.in.m1.ants.out.chan.*.dev-a",
            "ants.in.m1.ants.out.dead.dev-a",
            "ants.in.m1.ants.out.timeline.*.dev-a",
            "ants.in.m1.ants.out.notes.*.dev-b",
            "ants.in.m1.ants.out.chan.*.dev-b",
            "ants.in.m1.ants.out.dead.dev-b",
            "ants.in.m1.ants.out.timeline.*.dev-b",
            "ants.in.m1.ants.out.notes.*.human",
            "ants.in.m1.ants.out.chan.*.human",
        ]
    );
}

/// `ants leaf outbox M --remove-ant ANT` after the uplink credentials stopped carrying ANT: an ant the hub still takes
/// from the outbox for, or the team directory kept among the members once, left already; any other is no ant of M.
#[tokio::test]
async fn removing_an_ant_the_uplink_no_longer_carries_takes_one_that_left_already() {
    let Some(hub) = NatsServer::start() else { return };
    let client = async_nats::connect(&hub.url).await.unwrap();
    let js = jetstream::new(client);
    let m1: Machine = "m1".parse().unwrap();
    let p = |s: &str| -> UserId { s.parse().unwrap() };
    let mut config = bus::ensure_stream(&js).await.unwrap().info().await.unwrap().config.clone();
    config.sources = Some(admissions(&m1, &auth::members(&[p("dev-a"), p("dev-b")], &[UserId::human()]), 1));
    js.update_stream(&config).await.unwrap();

    let team = hub.dir().join("auth");
    auth::init(&team).unwrap();
    let on_m1 = auth::members(&[p("dev-b"), p("dev-c")], &[UserId::human()]);
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, None, &hub.dir().join("leaf.creds")).unwrap();
    assert!(members::retired(&team, &m1, &p("dev-c")).unwrap());

    let carried = Members { ants: vec![p("dev-b")], humans: vec![UserId::human()], ..Members::default() };
    let removing = |ants: &[&str]| Change {
        remove: Members { ants: ants.iter().map(|a| p(a)).collect(), ..Members::default() },
        ..Change::default()
    };
    let mut change = removing(&["dev-a", "dev-c", "dev-b"]);
    let left = left_already(&js, &m1, &team, &mut change, Some(&carried)).await.unwrap();
    assert_eq!(left, [p("dev-a"), p("dev-c")], "sourced by the hub, kept by the team directory once");
    let set = change.apply(&m1, Some(carried.clone()), || anyhow::anyhow!("unknown")).unwrap();
    assert_eq!(set, Members { ants: Vec::new(), ..carried.clone() }, "dev-b, carried, removed as ever");

    let mut change = removing(&["dev-x"]);
    assert!(left_already(&js, &m1, &team, &mut change, Some(&carried)).await.unwrap().is_empty());
    let err = change.apply(&m1, Some(carried), || anyhow::anyhow!("unknown")).unwrap_err();
    assert_eq!(err.to_string(), "dev-x is no ant of m1", "never a member");
}

#[test]
fn the_attachments_store_takes_the_chunks_of_every_sender_for_one_human_else_each_ants_own() {
    let m1: Machine = "m1".parse().unwrap();
    let (dev_a, dev_b): (UserId, UserId) = ("dev-a".parse().unwrap(), "dev-b".parse().unwrap());
    let transforms = |members: &[(UserId, Role)]| -> Vec<(UserId, String, String)> {
        file_admissions(&m1, members, 7)
            .into_iter()
            .inspect(|(_, source)| assert!(source.start_sequence == Some(7) && from_machine(source, &m1)))
            .flat_map(|(user, source)| {
                source.subject_transforms.into_iter().map(move |t| (user.clone(), t.source, t.destination))
            })
            .collect()
    };
    let ants = auth::members(&[dev_b.clone(), dev_a.clone(), dev_a.clone()], &[]);
    assert_eq!(
        transforms(&ants),
        [
            (dev_a.clone(), "ants.in.m1.ants.out.files.dev-a.>".to_owned(), "$O.ants_files.C.dev-a.>".to_owned()),
            (dev_b, "ants.in.m1.ants.out.files.dev-b.>".to_owned(), "$O.ants_files.C.dev-b.>".to_owned()),
        ]
    );
    let alice: UserId = "alice".parse().unwrap();
    let with_humans = auth::members(&[dev_a, UserId::human()], std::slice::from_ref(&alice));
    assert_eq!(
        transforms(&with_humans),
        [(alice, "ants.in.m1.ants.out.files.>".to_owned(), "$O.ants_files.C.>".to_owned())]
    );
}

/// A hub and a leaf node stopped and started again on their files, the leaf node after the hub's first request for
/// its outbox, which nobody answers: through the outbox's durable consumer, the hub goes on by itself within seconds,
/// after what it took before the restart, taking nothing twice.
#[tokio::test]
async fn an_outbox_goes_on_after_a_restart_at_once_through_its_durable_consumer() {
    let leaf_port = free_port();
    let Some(mut hub) = spawn_hub(leaf_port, "feature_flags { js_api_reply_v2: true }") else {
        return;
    };
    let team = hub.dir().join("auth");
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let (human, dev_a): (UserId, UserId) = (UserId::human(), "dev-a".parse().unwrap());
    let creds = |p: &UserId| {
        let out = hub.dir().join(format!("{p}.creds"));
        auth::issue(&team, p, Role::by_default(p), Some(&domain), &out).unwrap();
        out
    };
    let (human_creds, a_creds) = (creds(&human), creds(&dev_a));
    let connect = async |url: &str, p: &UserId, c: &Path| {
        let (client, mut js) = bus::connect(url, Some(c), Some(&domain), p, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(2));
        (client, js)
    };
    let (_h, h_js) = connect(&hub.url, &human, &human_creds).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let on_m1 = auth::members(&[dev_a.clone(), human.clone()], &[]);
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    auth::issue_leaf(&team, &m1, &on_m1, &[], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap();
    let (server_conf, listen) = (team.join(auth::SERVER_CONF), format!("127.0.0.1:{}", free_port()));
    let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
    let mut leaf_node = NatsServer::spawn_with(|dir| {
        let config = leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &leaf_creds,
            system_creds: None,
            server_conf: &server_conf,
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    let setup = super::connect(&hub.url, Some(&human_creds), &human).await.unwrap();
    eventually("the leaf reaches the hub", async || admit(&setup, &h_js, &m1, &on_m1).await.is_ok()).await;
    sourced(&h_js, &m1, SOURCE_WAIT).await.unwrap();
    let keep = async |js: &Context, text: &str| {
        let kept = Envelope::new(dev_a.clone(), human.clone(), text);
        let message =
            PublishMessage::build().payload(serde_json::to_vec(&kept).unwrap().into()).message_id(kept.id.to_string());
        js.send_publish(subject::outbox(&kept.subject()).unwrap(), message).await.unwrap().await.unwrap();
    };
    let (_a, a_js) = connect(&leaf_node.url, &dev_a, &a_creds).await;
    keep(&a_js, "before").await;
    eventually("the intake takes it", async || messages(&h_js, INTAKE_STREAM).await == 1).await;

    leaf_node.stop();
    hub.stop();
    hub.start_again();
    // The hub asks for the outbox as it starts, before the leaf node is back.
    tokio::time::sleep(Duration::from_secs(1)).await;
    leaf_node.start_again();
    let (_h, h_js) = connect(&hub.url, &human, &human_creds).await;
    let (_a, a_js) = connect(&leaf_node.url, &dev_a, &a_creds).await;
    eventually("the leaf reaches the hub", async || bus::open_stream(&a_js, &dev_a).await.is_ok()).await;
    keep(&a_js, "after").await;
    sourced(&h_js, &m1, Duration::from_secs(15)).await.unwrap();
    eventually("the intake takes what the leaf kept", async || messages(&h_js, INTAKE_STREAM).await == 2).await;
    eventually("and so does the hub's stream", async || messages(&h_js, STREAM).await == 2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(messages(&h_js, INTAKE_STREAM).await, 2, "nothing taken twice");
}
