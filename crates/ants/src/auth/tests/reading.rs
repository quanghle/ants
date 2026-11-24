//! Reading back notes, inboxes and threads through each reader's own consumers.

use super::*;

#[tokio::test]
async fn thread_notes_are_read_back_through_each_reader_own_consumer() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (b, b_js) = server.connect(&dev_b).await;
    let (thread, other): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());

    let first = NotesPost::spend(thread.clone(), dev_a.clone(), "draft", 8, None).unwrap();
    assert!(notes::publish(&a_js, &first, Some(0)).await.unwrap());
    // dev-b spends the same turn without having seen dev-a's post: refused, the notes moved on.
    let stale = NotesPost::spend(thread.clone(), dev_b.clone(), "me first", 8, None).unwrap();
    assert!(!notes::publish(&b_js, &stale, Some(0)).await.unwrap());
    // dev-a may only post on its own subject, so a post naming dev-b as its author is dropped by readers.
    let forged = NotesPost::new(thread.clone(), dev_b.clone(), "dev-b agrees");
    bus::publish_json(&a_js, subject::notes(&thread, &dev_a), &forged, None).await.unwrap();
    notes::publish(&a_js, &NotesPost::new(other, dev_a.clone(), "elsewhere"), None).await.unwrap();

    let seen = notes::read(&b_js, &dev_b, Reading::Consumer, &thread).await.unwrap();
    assert_eq!(seen.posts, std::slice::from_ref(&first));
    let answer = NotesPost::spend(thread.clone(), dev_b.clone(), "agreed", 8, seen.last()).unwrap();
    assert!(notes::publish(&b_js, &answer, Some(seen.last_seq)).await.unwrap());

    // While dev-b's daemon reads through notes-dev-b, dev-b's CLI reads through its spares and the human's by message
    // gets, more at once than there are spares: all read the whole notes, and the daemon's read carries on.
    let daemon = jetstream::consumer::pull::Config {
        name: Some(notes_consumer(&dev_b)),
        filter_subject: subject::notes_of(&thread),
        ack_policy: jetstream::consumer::AckPolicy::None,
        ..Default::default()
    };
    let daemon: jetstream::consumer::PullConsumer = b_js.create_consumer_on_stream(daemon, STREAM).await.unwrap();
    let next = || async {
        let msg = daemon.fetch().max_messages(1).messages().await.unwrap().next().await.unwrap().unwrap();
        serde_json::from_slice::<NotesPost>(&msg.payload).unwrap()
    };
    assert_eq!(next().await, first);
    let reads = (0..6).map(|i| match i % 2 {
        0 => notes::read(&h_js, &human, Reading::Gets, &thread),
        _ => notes::read(&b_js, &dev_b, Reading::Spare, &thread),
    });
    for read in futures::future::join_all(reads).await {
        let read = read.unwrap();
        assert_eq!((&read.posts[..], read.last_seq), (&[first.clone(), answer.clone()][..], seen.last_seq + 2));
    }
    assert_eq!(next().await, forged, "the daemon's read carries on where it was");
    // The human's reads go by batched direct gets, which no ant's credentials allow.
    let batch = format!(r#"{{"batch":2,"next_by_subj":"{}"}}"#, subject::notes_of(&thread));
    let got = h.request(format!("$JS.API.DIRECT.GET.{STREAM}"), batch.clone().into()).await.unwrap();
    assert!(got.headers.unwrap().get("Nats-Num-Pending").is_some(), "a batch");
    let denied = b.request(format!("$JS.API.DIRECT.GET.{STREAM}"), batch.into());
    assert!(!matches!(timeout(Duration::from_secs(1), denied).await, Ok(Ok(_))), "an ant gets no batch");

    // dev-a's notes consumer filters thread notes only, and dev-b's is not dev-a's to use.
    let consumer = |name: String, filter: String| jetstream::consumer::pull::Config {
        name: Some(name),
        filter_subject: filter,
        ..Default::default()
    };
    let into_inbox = consumer(notes_consumer(&dev_a), subject::inbox_of(&dev_b));
    assert!(a_js.create_consumer_on_stream(into_inbox, STREAM).await.is_err());
    let not_its_own = consumer(notes_consumer(&dev_b), subject::notes_of(&thread));
    assert!(a_js.create_consumer_on_stream(not_its_own, STREAM).await.is_err());
    assert!(notes::read(&a_js, &dev_a, Reading::Consumer, &thread).await.is_ok(), "its own works");
    let spare = |name: String| consumer(name, subject::notes_of(&thread));
    let not_its_spare = spare(ants_proto::spare_readers(&notes_consumer(&dev_b)).next().unwrap());
    assert!(a_js.create_consumer_on_stream(not_its_spare, STREAM).await.is_err());
    let into_inbox =
        consumer(ants_proto::spare_readers(&notes_consumer(&dev_a)).next().unwrap(), subject::inbox_of(&dev_b));
    assert!(a_js.create_consumer_on_stream(into_inbox, STREAM).await.is_err());
}

#[tokio::test]
async fn an_ant_cli_reads_back_only_its_own_inbox_through_spares_of_its_inbox_consumer() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    // dev-a's daemon takes its inbox through inbox-dev-a.
    let mut daemon =
        bus::inbox_consumer(&bus::open_stream(&a_js, &dev_a).await.unwrap(), &dev_a, WAIT, 5, 1).await.unwrap();
    let to_a = Envelope::new(dev_b.clone(), dev_a.clone(), "for dev-a");
    bus::publish(&b_js, &to_a).await.unwrap();
    bus::publish(&a_js, &Envelope::new(dev_a.clone(), dev_b.clone(), "for dev-b")).await.unwrap();

    // dev-a's CLI reads its own inbox back, ids and all, more reads at once than there are spares.
    let (inbox, filter) = (ants_proto::inbox_consumer(&dev_a), subject::inbox_of(&dev_a));
    let reads = (0..2 * ants_proto::SPARE_READERS).map(|_| async {
        let mut got = Vec::new();
        let all = jetstream::consumer::DeliverPolicy::All;
        ants_daemon::stream::reading::read_back_messages(&a_js, &inbox, Reading::Spare, &filter, all, |_, _, msg| {
            got.push(msg);
            std::ops::ControlFlow::Continue(())
        })
        .await
        .map(|()| got)
    });
    for read in futures::future::join_all(reads).await {
        let [msg] = &read.unwrap()[..] else { panic!("one message to dev-a") };
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), to_a);
        let id = msg.headers.as_ref().and_then(|h| h.get(async_nats::header::NATS_MESSAGE_ID));
        assert_eq!(id.map(|id| id.as_str().to_owned()), Some(to_a.id.to_string()));
    }
    let msg = timeout(WAIT, daemon.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), to_a, "the daemon still takes it");

    // The spares filter dev-a's own inbox only, and dev-b's are not dev-a's.
    let spare = |of: &UserId, filter: String| jetstream::consumer::pull::Config {
        name: ants_proto::spare_readers(&ants_proto::inbox_consumer(of)).next(),
        filter_subject: filter,
        ..Default::default()
    };
    for (of, filter) in [
        (&dev_a, subject::inbox_of(&dev_b)),
        (&dev_a, subject::ALL_NOTES.to_owned()),
        (&dev_b, subject::inbox_of(&dev_b)),
    ] {
        let config = spare(of, filter.clone());
        assert!(a_js.create_consumer_on_stream(config, STREAM).await.is_err(), "{of}'s spare on {filter}");
    }
}

#[tokio::test]
async fn an_ant_reads_back_its_thread_both_ways_and_only_its_own_sent_messages() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    let (thread, ops): (ThreadId, ChannelId) = (ThreadId::generate(), "ops".parse().unwrap());
    bus::publish(&b_js, &Envelope::on_thread(thread.clone(), dev_b.clone(), dev_a.clone(), "status?")).await.unwrap();
    bus::publish(&a_js, &Envelope::on_thread(thread.clone(), dev_a.clone(), dev_b.clone(), "green")).await.unwrap();
    bus::publish(&a_js, &Envelope::new(dev_a.clone(), dev_b.clone(), "elsewhere")).await.unwrap();
    notes::publish(&b_js, &NotesPost::new(thread.clone(), dev_b.clone(), "noted"), None).await.unwrap();
    let post = ChannelPost::on_thread(ops.clone(), thread.clone(), dev_b.clone(), "shipped");
    bus::publish_json(&b_js, subject::chan(&ops, &dev_b), &post, None).await.unwrap();

    let read = ants_daemon::stream::history::Thread {
        js: a_js.clone(),
        ant: dev_a.clone(),
        thread: thread.clone(),
        channel: Some(ops),
        before: u64::MAX,
        reading: Reading::Consumer,
        home: None,
    };
    assert_eq!(
        read.history()(ants_daemon::stream::history::DEFAULT_BUDGET).await,
        "[ants] The thread's history, read back from ants, oldest first:\n\ndev-b to dev-a:\nstatus?\n\n\
             dev-a to dev-b:\ngreen\n\ndev-b in the thread notes:\nnoted\n\ndev-b in channel ops:\nshipped\n\n\
             [ants] End of the thread's history."
    );

    // The sent reader filters dev-a's own sent messages only, and is not dev-b's to use.
    let reader = |name: String, filter: String| jetstream::consumer::pull::Config {
        name: Some(name),
        filter_subject: filter,
        ..Default::default()
    };
    let sent = ants_proto::sent_reader(&dev_a);
    assert!(a_js.create_consumer_on_stream(reader(sent.clone(), subject::inbox_of(&dev_b)), STREAM).await.is_err());
    assert!(b_js.create_consumer_on_stream(reader(sent, subject::inbox_from(&dev_a)), STREAM).await.is_err());
}

#[tokio::test]
async fn an_ants_daemon_reads_back_its_own_timeline_only() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    let (one, two): (ThreadId, ThreadId) = (ThreadId::generate(), ThreadId::generate());
    for (js, ant, thread) in [(&a_js, &dev_a, &one), (&a_js, &dev_a, &two), (&b_js, &dev_b, &one)] {
        let record = ants_proto::Timeline::new(ant.clone(), thread.clone(), ants_proto::Uuid::now_v7(), Vec::new(), 0);
        bus::publish_json(js, record.subject(), &record, None).await.unwrap();
    }

    let read = async |js: &jetstream::Context, ant: &UserId, filter: String| {
        let (reader, mut read) = (ants_proto::sent_reader(ant), Vec::new());
        let all = jetstream::consumer::DeliverPolicy::All;
        ants_daemon::stream::reading::read_back(js, &reader, Reading::Consumer, &filter, all, |_, _, subject, _| {
            read.push(subject.to_owned());
            std::ops::ControlFlow::Continue(())
        })
        .await
        .map(|()| read)
    };
    let own = read(&a_js, &dev_a, subject::timeline_from(&dev_a)).await.unwrap();
    assert_eq!(own, [subject::timeline(&one, &dev_a), subject::timeline(&two, &dev_a)]);
    let of_one = read(&a_js, &dev_a, subject::timeline(&one, &dev_a)).await.unwrap();
    assert_eq!(of_one, [subject::timeline(&one, &dev_a)]);
    // Not dev-b's timeline, of any thread or of one they share, nor every ant's, nor through dev-b's reader.
    for filter in [subject::timeline_from(&dev_b), subject::timeline(&one, &dev_b), subject::ALL_TIMELINE.to_owned()] {
        assert!(read(&a_js, &dev_a, filter.clone()).await.is_err(), "{filter}");
    }
    assert!(read(&a_js, &dev_b, subject::timeline_from(&dev_b)).await.is_err());
    // Nor does its reader read anything but timelines.
    assert!(read(&a_js, &dev_a, subject::inbox_of(&dev_b)).await.is_err());
}
