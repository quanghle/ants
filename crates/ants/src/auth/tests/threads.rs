//! Who titles a thread, marks it and records its owner.

use super::*;

#[tokio::test]
async fn any_human_titles_a_thread_and_keeps_its_tldr_but_each_marks_only_its_own_threads() {
    use crate::threads::thread_meta::{MISSING, Meta};

    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let thread = ThreadId::generate();
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    let missing = Meta::open(&a_js, None, &alice).await.unwrap().title(&thread).await.unwrap_err();
    assert_eq!(missing.to_string(), MISSING, "no bucket before `ants init`");
    assert!(d_js.create_stream(ants_daemon::hub::threads_config()).await.is_err(), "only a human creates it");
    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let (alice_meta, bob_meta) =
        (Meta::open(&a_js, None, &alice).await.unwrap(), Meta::open(&b_js, None, &bob).await.unwrap());

    assert_eq!(alice_meta.set_title(&thread, "  Release 1.2 ").await.unwrap(), None);
    let had = bob_meta.set_title(&thread, "Release 1.2.1").await.unwrap().unwrap();
    assert_eq!((had.title.as_str(), &had.by), ("Release 1.2", &alice), "bob renames alice's title");
    let title = alice_meta.title(&thread).await.unwrap().unwrap();
    assert_eq!((title.title.as_str(), &title.by), ("Release 1.2.1", &bob));
    assert!(alice_meta.set_title(&thread, "two\nlines").await.is_err());
    let ant = Meta::open(&d_js, None, &dev_a).await.unwrap();
    let read = ant.title(&thread).await.unwrap().unwrap();
    assert_eq!((read.title.as_str(), &read.by), ("Release 1.2.1", &bob), "an ant reads a title by its key");
    assert!(ant.set_title(&thread, "mine").await.is_err(), "but sets none");
    let tldr = ants_proto::ThreadSummary {
        thread_id: thread.clone(),
        text: "Release 1.2.1 is out.".to_owned(),
        by: dev_a.clone(),
        asker: alice.clone(),
        at: chrono::Utc::now(),
        upto: 7,
        tokens: 42,
    };
    alice_meta.put_summary(&tldr).await.unwrap();
    assert_eq!(bob_meta.summary(&thread).await.unwrap(), Some(tldr), "every human reads the TL;DR");
    assert!(ant.summary(&thread).await.is_err(), "an ant reads no TL;DR");
    assert!(alice_meta.set_done(&thread, 1).await.unwrap());
    assert!(bob_meta.clear_done(&thread).await.unwrap().is_some_and(|d| d.by == alice), "bob clears alice's done");
    assert!(alice_meta.set_done(&thread, 2).await.unwrap());
    assert_eq!(ant.done(&thread).await.unwrap().map(|d| d.by), Some(alice.clone()), "an ant reads a done state");
    assert!(ant.set_done(&thread, 3).await.is_err(), "but marks no thread done");
    assert!(ant.clear_done(&thread).await.is_err(), "nor clears one");

    assert!(alice_meta.change_marks(|m| m.pin(&thread)).await.unwrap());
    assert!(!alice_meta.change_marks(|m| m.pin(&thread)).await.unwrap(), "pinned already");
    assert!(bob_meta.change_marks(|m| m.archive(&thread, 1)).await.unwrap());
    assert!(alice_meta.marks().await.unwrap().pinned.contains_key(&thread), "bob's archive is bob's");
    assert!(bob_meta.change_marks(|m| m.unarchive(&thread)).await.unwrap());
    assert!(alice_meta.change_marks(|m| m.unpin(&thread)).await.unwrap());
    assert_eq!(alice_meta.marks().await.unwrap(), ants_proto::ThreadMarks::default());
    assert!(Meta::open(&a_js, None, &bob).await.unwrap().marks().await.is_err(), "alice reads no marks of bob's");
    assert!(Meta::open(&d_js, None, &dev_a).await.unwrap().marks().await.is_err(), "an ant keeps none");

    let (raw, mut errors) = server.watched_with(&alice, &server.creds_as(&alice, Role::Human)).await;
    let forged = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::marks_key(&bob));
    raw.publish(forged.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&forged), "forging bob's marks is denied");
    let title = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::title_key(&thread));
    let (raw, mut errors) = server.watched(&dev_a).await;
    raw.publish(title.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&title), "an ant's title is denied");
    let tldr = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::summary_key(&thread));
    raw.publish(tldr.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&tldr), "an ant's TL;DR is denied");
    let done = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::done_key(&thread));
    raw.publish(done.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&done), "an ant's done state is denied");
    let marking = format!("{done}.mark");
    raw.publish(marking.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&marking), "an ant's marking done is denied");
}

#[tokio::test]
async fn an_ant_s_daemon_reads_what_a_human_did_about_an_escalation() {
    use ants_daemon::records::heed::{Heeded, jetstream_heed};
    use ants_proto::{NotifyPrefs, ReadMarker, Reminders, Snooze, ThreadSnoozes};

    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    let heed = jetstream_heed(d_js, None);
    let [done, read, muted, snoozed] = std::array::from_fn(|_| ThreadId::generate());
    assert_eq!(heed(alice.clone(), done.clone(), vec![5]).await.unwrap(), Heeded::No, "no bucket holds nothing");

    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let reads = a_js.create_key_value(ants_daemon::hub::read_config()).await.unwrap();
    let notify = a_js.create_key_value(ants_daemon::hub::notify_config()).await.unwrap();
    let threads = a_js.get_key_value(ants_proto::THREADS_BUCKET).await.unwrap();
    let now = chrono::Utc::now();
    let put = |kv: &async_nats::jetstream::kv::Store, key: String, value: Vec<u8>| {
        let kv = kv.clone();
        async move { kv.put(key, value.into()).await.unwrap() }
    };
    crate::threads::thread_meta::Meta::open(&a_js, None, &alice).await.unwrap().set_done(&done, 20).await.unwrap();
    let marker = ReadMarker { below: 10, ..ReadMarker::default() };
    put(&reads, alice.to_string(), serde_json::to_vec(&marker).unwrap()).await;
    let mut prefs = NotifyPrefs::default();
    prefs.muted.insert(muted.clone(), now + chrono::TimeDelta::days(1));
    put(&notify, alice.to_string(), serde_json::to_vec(&prefs).unwrap()).await;
    let until = now + chrono::TimeDelta::hours(1);
    let snoozed_threads = ThreadSnoozes([(snoozed.clone(), Snooze { at: now, until })].into());
    let reminders = Reminders { snoozed: snoozed_threads, ..Reminders::default() };
    put(&threads, ants_proto::reminders_key(&alice), serde_json::to_vec(&reminders).unwrap()).await;

    let heeded = |thread: &ThreadId, seq| heed(alice.clone(), thread.clone(), vec![seq]);
    assert_eq!(heeded(&done, 15).await.unwrap(), Heeded::Done);
    assert_eq!(heeded(&read, 5).await.unwrap(), Heeded::Read);
    assert_eq!(heeded(&read, 50).await.unwrap(), Heeded::No);
    assert_eq!(heeded(&muted, 50).await.unwrap(), Heeded::Muted);
    assert_eq!(heeded(&snoozed, 50).await.unwrap(), Heeded::Snoozed(until));
}

#[tokio::test]
async fn an_ant_s_daemon_hears_the_threads_marked_done_through_its_own_consumer_only() {
    use ants_daemon::provider::events::done;

    let Some(server) = Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let stream = d_js.get_stream_no_info(format!("KV_{}", ants_proto::THREADS_BUCKET)).await.unwrap();
    let reader = |name: String, filter: String| jetstream::consumer::pull::Config {
        durable_name: Some(name),
        filter_subject: filter,
        ..Default::default()
    };
    let marks = reader(ants_proto::done_reader(&dev_a), format!("$KV.{}.>", ants_proto::THREADS_BUCKET));
    assert!(stream.create_consumer(marks).await.is_err(), "an ant's consumer reads no other key");
    let states = reader(ants_proto::done_reader(&dev_a), format!("$KV.{}.done.*", ants_proto::THREADS_BUCKET));
    assert!(stream.create_consumer(states).await.is_err(), "nor the done states");
    let filter = format!("$KV.{}.done.*.*", ants_proto::THREADS_BUCKET);
    let not_its_own = reader(ants_proto::done_reader(&dev_b), filter);
    assert!(stream.create_consumer(not_its_own).await.is_err(), "nor is dev-b's dev-a's to use");

    let mut dones = done::consumer(&d_js, &dev_a).await.unwrap();
    let thread = ThreadId::generate();
    let meta = crate::threads::thread_meta::Meta::open(&a_js, None, &alice).await.unwrap();
    meta.change_marks(|m| m.pin(&thread)).await.unwrap();
    meta.set_done(&thread, 4).await.unwrap();
    let message = timeout(Duration::from_secs(5), dones.next()).await.unwrap().unwrap().unwrap();
    let done = format!("$KV.{}.{}.", ants_proto::THREADS_BUCKET, ants_proto::done_key(&thread));
    assert!(message.subject.starts_with(&done), "its own hears the marking done, not the marks: {}", message.subject);
    message.ack().await.unwrap();
}

/// As its retirement stops it, an ant's daemon deletes its own done and channel consumers, no other ant's, so a done
/// state put while it is retired is not told once it is served again, which creates them anew.
#[tokio::test]
async fn an_ant_s_daemon_deletes_its_own_done_and_channel_consumers_on_its_retirement() {
    use ants_daemon::provider::events::done;
    use async_nats::jetstream::ErrorCode;
    use async_nats::jetstream::stream::ConsumerErrorKind;

    let Some(server) = Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    let (_, b_js) = server.connect(&dev_b).await;
    bus::ensure_stream(&a_js).await.unwrap();
    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let threads = format!("KV_{}", ants_proto::THREADS_BUCKET);
    let serve = async |js: &jetstream::Context, ant: &UserId| {
        let stream = bus::open_stream(js, ant).await.unwrap();
        drop(bus::chan_consumer(&stream, ant, WAIT, 5, 1).await.unwrap());
        done::consumer(js, ant).await.unwrap()
    };
    drop(serve(&d_js, &dev_a).await);
    drop(serve(&b_js, &dev_b).await);

    bus::forget_retired(&d_js, &dev_a).await.unwrap();
    for (name, stream) in [(chan_consumer(&dev_a), STREAM), (ants_proto::done_reader(&dev_a), threads.as_str())] {
        let Err(gone) = d_js.delete_consumer_from_stream(&name, stream).await else { panic!("{name} left") };
        assert!(
            matches!(gone.kind(), ConsumerErrorKind::JetStream(e) if e.error_code() == ErrorCode::CONSUMER_NOT_FOUND),
            "{name} deleted: {gone}"
        );
    }
    for (name, stream) in [(chan_consumer(&dev_b), STREAM), (ants_proto::done_reader(&dev_b), threads.as_str())] {
        assert!(d_js.delete_consumer_from_stream(&name, stream).await.is_err(), "{name} is not dev-a's to delete");
        b_js.delete_consumer_from_stream(&name, stream).await.expect("dev-b's own stays");
    }

    let meta = crate::threads::thread_meta::Meta::open(&a_js, None, &alice).await.unwrap();
    let (while_retired, revived) = (ThreadId::generate(), ThreadId::generate());
    meta.set_done(&while_retired, 4).await.unwrap();
    let mut dones = serve(&d_js, &dev_a).await;
    meta.set_done(&revived, 5).await.unwrap();
    let message = timeout(WAIT, dones.next()).await.unwrap().unwrap().unwrap();
    let done = format!("$KV.{}.{}.", ants_proto::THREADS_BUCKET, ants_proto::done_key(&revived));
    assert!(message.subject.starts_with(&done), "only what is marked done once it is served again");
}

#[tokio::test]
async fn revoking_a_retired_ant_cleans_up_only_its_channel_and_done_consumers() {
    use ants_daemon::provider::events::done;
    use ants_proto::{COLONY_BUCKET, COLONY_TTL, THREADS_BUCKET, inbox_consumer};

    let Some(server) = Server::start_with("domain: hub", "") else { return };
    let domain = "hub".parse().unwrap();
    let domain = Some(&domain);
    let team = server.dir().join("auth");
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let threads = format!("KV_{THREADS_BUCKET}");
    let mut inspect = Permissions::for_user(&alice, Role::Human);
    inspect.publish.extend([
        format!("$JS.API.CONSUMER.INFO.{STREAM}.*"),
        format!("$JS.API.CONSUMER.INFO.{threads}.*"),
        format!("$KV.{COLONY_BUCKET}.{dev_a}"),
    ]);
    let human_creds = server.dir().join("alice.creds");
    issue_with(&team, &alice, Role::Human.into(), inspect.with_js_domain(domain), &human_creds).unwrap();
    let human = connect_to(&server.url, &human_creds, domain, &alice).await;
    let colony = bus::ensure_colony(&human, COLONY_TTL).await.unwrap();
    let channel_stream = bus::ensure_stream(&human).await.unwrap();
    let done_stream = human.create_stream(ants_daemon::hub::threads_config()).await.unwrap();

    let first = server.creds_in(&dev_a, domain);
    let first_key = read_creds(&first).unwrap().1.public_key();
    let last = server.creds_in(&dev_a, domain);
    let last_key = read_creds(&last).unwrap().1.public_key();
    let other = server.creds_in(&dev_b, domain);
    for (ant, creds) in [(&dev_a, &first), (&dev_b, &other)] {
        let js = connect_to(&server.url, creds, domain, ant).await;
        let stream = bus::open_stream(&js, ant).await.unwrap();
        drop(bus::inbox_consumer(&stream, ant, WAIT, 5, 1).await.unwrap());
        drop(bus::chan_consumer(&stream, ant, WAIT, 5, 1).await.unwrap());
        drop(done::consumer(&js, ant).await.unwrap());
    }
    let conn = crate::connection::Connection { nats: &server.url, creds: None, js_domain: domain };
    let revoke = async |key: &str| {
        crate::cmd::auth::revoke_credentials(None, vec![key.to_owned()], Vec::new(), false, Some(team.clone()), &conn)
            .await
            .unwrap();
    };
    let check = async |removed: bool| {
        assert_eq!(channel_stream.consumer_info(chan_consumer(&dev_a)).await.is_err(), removed);
        assert_eq!(done_stream.consumer_info(ants_proto::done_reader(&dev_a)).await.is_err(), removed);
        channel_stream.consumer_info(inbox_consumer(&dev_a)).await.unwrap();
        channel_stream.consumer_info(chan_consumer(&dev_b)).await.unwrap();
        done_stream.consumer_info(ants_proto::done_reader(&dev_b)).await.unwrap();
    };
    revoke(&first_key).await;
    check(false).await;

    let beat = async |age| {
        let entry = crate::test_support::colony::entry(dev_a.as_str(), 60, age, chrono::Utc::now());
        colony.put(dev_a.as_str(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    };
    beat(0).await;
    revoke(&last_key).await;
    check(false).await;

    beat(60).await;
    revoke(&last_key).await;
    check(true).await;
    revoke(&last_key).await;
    check(true).await;
}

#[tokio::test]
async fn a_human_adopts_an_ant_s_title_only_on_a_thread_never_titled() {
    use crate::threads::thread_meta::Meta;

    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let meta = Meta::open(&a_js, None, &alice).await.unwrap();
    let (fresh, titled, cleared) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());

    let adopted = meta.adopt_title(&fresh, " Fix the build ", &dev_a).await.unwrap().unwrap();
    assert_eq!((adopted.title.as_str(), &adopted.by), ("Fix the build", &dev_a));
    assert_eq!(meta.title(&fresh).await.unwrap(), Some(adopted));
    assert_eq!(meta.adopt_title(&fresh, "Something else", &dev_a).await.unwrap(), None, "adopted once");
    assert!(meta.adopt_title(&ThreadId::generate(), "two\nlines", &dev_a).await.is_err());

    meta.set_title(&titled, "Release 1.2").await.unwrap();
    assert_eq!(meta.adopt_title(&titled, "Fix the build", &dev_a).await.unwrap(), None);
    assert_eq!(meta.title(&titled).await.unwrap().unwrap().by, alice, "a human's title wins");

    meta.set_title(&cleared, "Release 1.2").await.unwrap();
    meta.clear_title(&cleared).await.unwrap();
    assert_eq!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap(), None);
    assert_eq!(meta.title(&cleared).await.unwrap(), None, "a cleared title stays cleared");

    let ant = Meta::open(&d_js, None, &dev_a).await.unwrap();
    assert!(ant.adopt_title(&ThreadId::generate(), "mine", &dev_a).await.is_err(), "an ant adopts none");
}

#[tokio::test]
async fn only_humans_and_webhooks_to_ants_record_a_thread_s_owner_and_the_first_record_stays() {
    let Some(server) = Server::start() else { return };
    let (human, alice, dev_a, hook, alerts) = (UserId::human(), p("alice"), p("dev-a"), p("github"), p("alerts"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_owners(&a_js).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_owners(&h_js).await.unwrap();
    let issue = |webhook: &UserId, to: Destination| {
        let creds = server.dir().join(format!("{webhook}.creds"));
        issue_with(
            &server.dir().join("auth"),
            webhook,
            crate::credentials::revoke::Kind::Webhook,
            Permissions::for_webhook(webhook, &[to]),
            &creds,
        )
        .unwrap();
        creds
    };
    let hook_creds = issue(&hook, Destination::Ant(dev_a.clone()));
    let alerts_creds = issue(&alerts, Destination::Channel("ops".parse().unwrap()));

    let (asked, delivered, forged) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let dev_b = p("dev-b");
    owners::record(&h_js, None, &asked, &dev_a, &human).await.unwrap();
    owners::record(&h_js, None, &asked, &dev_b, &alice).await.unwrap();
    let hook_js = connect_to(&server.url, &hook_creds, None, &hook).await;
    owners::record(&hook_js, None, &delivered, &dev_a, &alice).await.unwrap();
    owners::record(&hook_js, None, &asked, &dev_a, &alice).await.unwrap();
    assert!(
        owners::record(&hook_js, None, &forged, &dev_b, &alice).await.is_err(),
        "a webhook records for its ants only"
    );
    assert!(owners::record(&a_js, None, &forged, &dev_a, &alice).await.is_err(), "an ant records no owner");
    let alerts_js = connect_to(&server.url, &alerts_creds, None, &alerts).await;
    assert!(owners::record(&alerts_js, None, &forged, &dev_a, &alice).await.is_err(), "nor a webhook to channels");

    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let denied = format!("$KV.{}.{}", ants_proto::OWNERS_BUCKET, ants_proto::owner_key(&forged, &dev_a));
    a_raw.publish(denied.clone(), "alice".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    // A webhook that records owners lists no threads of others, which stream info of the bucket would.
    let (hook_raw, mut hook_errors) = server.watched_with(&hook, &hook_creds).await;
    let info = format!("$JS.API.STREAM.INFO.KV_{}", ants_proto::OWNERS_BUCKET);
    hook_raw.publish(info.clone(), br#"{"subjects_filter":">"}"#[..].into()).await.unwrap();
    let v = next_violation(&hook_raw, &mut hook_errors).await;
    assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");
    let read = owners::Owners::new(a_js);
    assert_eq!(read.recorded(&asked, &dev_a).await.unwrap(), Some(human), "the first record stays");
    assert_eq!(read.recorded(&asked, &dev_b).await.unwrap(), Some(alice.clone()));
    assert_eq!(read.recorded(&delivered, &dev_a).await.unwrap(), Some(alice));
    for to in [&dev_a, &dev_b] {
        assert_eq!(read.recorded(&forged, to).await.unwrap(), None);
    }
    assert_eq!(read.recorded(&delivered, &dev_b).await.unwrap(), None);
}

#[tokio::test]
async fn the_human_s_side_tells_the_owner_the_thread_s_posts_name_not_the_one_an_ant_claims() {
    let Some(server) = Server::start() else { return };
    let (human, alice, dev_a, dev_b, lead) = (UserId::human(), p("alice"), p("dev-a"), p("dev-b"), p("lead"));
    // alice runs the human's side, and gets what goes to a thread with no owner.
    let (h_client, h_js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect(&dev_b).await;
    let (_, lead_js) = server.connect(&lead).await;
    let stream = bus::ensure_stream(&h_js).await.unwrap();
    // The senders' webhook markers are read there.
    h_js.create_key_value(ants_daemon::hub::webhooks_config()).await.unwrap();
    let mut to_human = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    let mut to_alice = h_client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    h_client.flush().await.unwrap();

    // lead starts a thread in #ops, which no human owns; on it dev-b names human as its owner, to dev-a.
    let ops: ChannelId = "ops".parse().unwrap();
    let root = ChannelPost::new(ops.clone(), lead.clone(), "plan the release");
    bus::publish_json(&lead_js, root.subject(), &root, None).await.unwrap();
    let thread = root.thread_id.clone();
    let message =
        Envelope::on_thread(thread.clone(), dev_b.clone(), dev_a.clone(), "relay this").with_owner(Some(human.clone()));
    let message_seq = bus::publish_json(&b_js, message.subject(), &message, None).await.unwrap();
    let post =
        ChannelPost::on_thread(ops, thread.clone(), dev_b.clone(), "@dev-a check").with_owner(Some(human.clone()));
    let post_seq = bus::publish_json(&b_js, post.subject(), &post, None).await.unwrap();

    let humans = Registry::new(alice.clone());
    let threads = Threads::new(
        h_js.clone(),
        alice.clone(),
        DirectKv::new(&h_js, None, ants_proto::SESSIONS_BUCKET).await.unwrap(),
    )
    .reading(Reading::Gets);
    let claims = Claims { humans: &humans, threads: &threads };
    for (consumer, seq) in [(ants_proto::inbox_consumer(&dev_a), message_seq), (chan_consumer(&dev_a), post_seq)] {
        let advisory = dead::MaxDeliveries { stream: STREAM.to_owned(), consumer, stream_seq: seq, deliveries: 5 };
        let (_, letter) = dead::bury_undelivered(&h_js, &stream, &advisory, &claims).await.unwrap().unwrap();
        let notice: Envelope =
            serde_json::from_slice(&timeout(WAIT, to_alice.next()).await.unwrap().unwrap().payload).unwrap();
        let named = (&notice.thread_id, notice.owner, notice.creator);
        assert_eq!(named, (&thread, None, Some(lead.clone())), "as lead's post says");
        if let Some(envelope) = letter.envelope {
            assert_eq!((envelope.owner, envelope.creator), (None, Some(lead.clone())), "kept as checked");
        }
    }
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing goes to the claimed owner");
}
