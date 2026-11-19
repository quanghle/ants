//! Out of the hub's reach: replies and dead letters kept in the machine's outbox land once the uplink is back.

use super::*;

/// Whether the uplink of [`Uplinked`] reaches the hub.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Link {
    Up,
    Down,
}

/// A hub (JetStream domain `hub`) and a leaf node (domain `m1`) with an outbox, whose uplink can be cut.
struct Uplinked {
    leaf: Server,
    uplink_port: u16,
    dead_port: u16,
    /// The human on the hub.
    hub_js: jetstream::Context,
    /// The hub's JetStream through the leaf.
    through_leaf: jetstream::Context,
    outbox: jetstream::stream::Stream,
}

impl Uplinked {
    /// The hub, for [`Bus::connect`] through [`Uplinked::url`], and its leaf node with the uplink up.
    async fn start() -> Option<(Server, Self)> {
        let (uplink_port, dead_port) = (free_port(), free_port());
        let hub = Server::spawn_with(|dir| {
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
                dir.join("js")
            )
        })?;
        let leaf = Server::spawn_with(|dir| {
            Self::write_uplink(dir, uplink_port);
            format!("jetstream {{ store_dir: {:?}, domain: m1 }}\ninclude \"uplink.conf\"\n", dir.join("js"))
        })
        .unwrap();
        let human = UserId::human();
        let (_, hub_js) = bus::connect(&hub.url, None, None, &human, "test").await.unwrap();
        bus::ensure_stream(&hub_js).await.unwrap();
        let (local, local_js) = bus::connect(&leaf.url, None, None, &human, "test").await.unwrap();
        let outbox = local_js
            .create_stream(jetstream::stream::Config {
                name: ants_proto::OUTBOX_STREAM.to_owned(),
                subjects: vec![ants_proto::OUTBOX_SUBJECTS.to_owned()],
                ..Default::default()
            })
            .await
            .unwrap();
        let mut through_leaf = bus::jetstream(local, Some(&"hub".parse().unwrap()));
        through_leaf.set_timeout(Duration::from_secs(1));
        let uplinked = Self { leaf, uplink_port, dead_port, hub_js, through_leaf, outbox };
        uplinked.until(Link::Up).await;
        Some((hub, uplinked))
    }

    fn write_uplink(dir: &std::path::Path, port: u16) {
        let remote = format!("leafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{port}\" }}] }}\n");
        std::fs::write(dir.join("uplink.conf"), remote).unwrap();
    }

    fn url(&self) -> String {
        self.leaf.url.clone()
    }

    /// Point the uplink at the hub, or at a dead port, and wait until it is so.
    async fn switch(&self, link: Link) {
        let port = match link {
            Link::Up => self.uplink_port,
            Link::Down => self.dead_port,
        };
        Self::write_uplink(self.leaf.dir(), port);
        let reload = std::process::Command::new("kill").arg("-HUP").arg(self.leaf.pid().to_string()).status();
        assert!(reload.unwrap().success());
        self.until(link).await;
    }

    async fn until(&self, link: Link) {
        let up = link == Link::Up;
        timeout(WAIT * 2, async {
            while bus::ensure_stream(&self.through_leaf).await.is_ok() != up {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the uplink is not {}", if up { "up" } else { "down" }));
    }

    async fn kept(&self) -> u64 {
        self.outbox.clone().info().await.unwrap().state.messages
    }

    /// Shorten the hub stream's duplicate window to [`SHORT_WINDOW`], so a test can outwait it.
    async fn shorten_duplicate_window(&self) {
        let mut config = self.hub_js.get_stream(ants_proto::STREAM).await.unwrap().cached_info().config.clone();
        config.duplicate_window = SHORT_WINDOW;
        self.hub_js.update_stream(&config).await.unwrap();
    }
}

/// A duplicate window a test outwaits in three times as long.
const SHORT_WINDOW: Duration = Duration::from_millis(300);

/// dev-a's daemon on a leaf node replies while the leaf's uplink is down: the reply goes to the machine's outbox
/// and the ack fails. Once the uplink is back, the redelivery to a restarted daemon is acked, not answered again.
#[tokio::test]
async fn a_reply_kept_in_the_outbox_answers_its_message_once() {
    let Some((hub, uplinked)) = Uplinked::start().await else { return };
    let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
    let (launch, log, gate) = fake::launch();
    let t1 = ThreadId::generate();
    let ask = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hold a");
    let seq = bus::publish_json(&uplinked.hub_js, ask.subject(), &ask, None).await.unwrap();
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));

    let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let down = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        uplinked.switch(Link::Down).await;
        gate.add_permits(1);
        until("the answer", || log.locked().answered.len() == 1).await;
        // Stopped, as for a restart, after the prompt in flight is handled.
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
    drop(pool);
    assert_eq!(uplinked.kept().await, 2, "the reply and the prompt's end are kept in the outbox");

    uplinked.switch(Link::Up).await;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let check = async {
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(log.locked().answered, ["hold a"], "answered once");
    assert_eq!(
        uplinked.hub_js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().state.messages,
        1,
        "no second reply"
    );
    assert_eq!(bus.unacked().await, 0);
    assert!(!bus.answered.has(&ask).await, "forgotten once acked");
}

/// dev-a's daemon on a leaf node gives up on a message while the leaf's uplink is down: the dead letter goes to the
/// machine's outbox, and the message can be neither terminated nor its owner told. Once the uplink is back, the
/// redelivery to a restarted daemon stores that same dead letter, with the error of the last attempt, tells the
/// human and terminates the message, without another prompt.
#[tokio::test]
async fn a_dead_letter_kept_in_the_outbox_is_buried_once_the_uplink_is_back() {
    Box::pin(dead_letter_kept_in_the_outbox(false)).await;
}

/// As above, with the outbox copy landing long before the redelivery, past the hub's duplicate window: the
/// redelivery finds it and stores no other.
#[tokio::test]
async fn a_dead_letter_kept_in_the_outbox_lands_once_past_the_duplicate_window() {
    Box::pin(dead_letter_kept_in_the_outbox(true)).await;
}

async fn dead_letter_kept_in_the_outbox(past_window: bool) {
    let Some((hub, uplinked)) = Uplinked::start().await else { return };
    if past_window {
        uplinked.shorten_duplicate_window().await;
    }
    let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
    let (launch, log, gate) = fake::launch();
    let t1 = ThreadId::generate();
    let ask = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "doomed a");
    let seq = bus::publish_json(&uplinked.hub_js, ask.subject(), &ask, None).await.unwrap();
    let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
    let stream = uplinked.hub_js.get_stream(ants_proto::STREAM).await.unwrap();

    let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let attempts = MAX_ATTEMPTS as usize;
    gate.add_permits(attempts - 1);
    let down = async {
        until("the last attempt", || log.locked().doomed == attempts).await;
        uplinked.switch(Link::Down).await;
        gate.add_permits(1);
        timeout(WAIT, async {
            while uplinked.kept().await == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the dead letter is kept in the outbox");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
    drop(pool);
    let kept = bus.buried.get(&ask).await.expect("noted on the machine");
    assert_eq!(kept.last_delivery, None, "the extra delivery finishes the burial");
    let kept = kept.dead;
    assert!(kept.error.contains("doomed"), "{}", kept.error);
    assert!(dead::list(&stream).await.unwrap().is_empty(), "not in the hub's stream yet");

    uplinked.switch(Link::Up).await;
    if past_window {
        // As the hub's stream takes the outbox copy once the uplink is back.
        bus::publish_json(&uplinked.hub_js, kept.subject(), &kept, Some(&kept.msg_id())).await.unwrap();
        tokio::time::sleep(SHORT_WINDOW * 3).await;
    }
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let check = async {
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
    assert_eq!(log.locked().doomed, attempts, "not prompted again");
    let letters = dead::list(&stream).await.unwrap();
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(letters[0].1, kept, "the dead letter of the last attempt");
    let notice = stream.get_last_raw_message_by_subject(&subject::inbox(&UserId::human(), &bus.dev)).await;
    let notice: Envelope = serde_json::from_slice(&notice.unwrap().payload).unwrap();
    assert_eq!((&notice.thread_id, notice.in_reply_to), (&t1, Some(ask.id)));
    assert!(notice.text.contains(&format!("ants dead replay {}", letters[0].0)), "{}", notice.text);
    assert_eq!(bus.unacked().await, 0);
    assert!(bus.buried.get(&ask).await.is_none(), "forgotten once terminated");
}

/// dev-a's daemon on a leaf node gives up on a message on the consumer's extra delivery while the leaf's uplink is
/// down: the dead letter goes to the machine's outbox, and no delivery is left to send its notice. Once the uplink
/// is back, the running daemon stores the dead letter in the hub's stream, tells the human and terminates the
/// message, before JetStream gives up on it.
#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_is_buried_once_the_uplink_is_back() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Live)).await;
}

/// As above, to a daemon restarted after JetStream gave up on the message, so a watcher buried it from the
/// advisory: after the outbox copy landed, finding it and telling nobody, or before, under the same message ids.
/// The human gets one notice either way.
#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_is_told_once_after_the_outbox_copy() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted {
        watcher_first: false,
        past_window: false,
    }))
    .await;
}

#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_is_told_once_after_the_watcher_s() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted { watcher_first: true, past_window: false }))
        .await;
}

/// As above, with the daemon restarted past the hub's duplicate window: it finds the letter there and stores no
/// other, and tells only when the watcher did not.
#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_lands_once_past_the_duplicate_window_after_the_outbox_copy() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted { watcher_first: false, past_window: true }))
        .await;
}

#[tokio::test]
async fn a_dead_letter_kept_on_the_extra_delivery_lands_once_past_the_duplicate_window_after_the_watcher_s() {
    Box::pin(dead_letter_kept_on_the_extra_delivery(UplinkBack::Restarted { watcher_first: true, past_window: true }))
        .await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum UplinkBack {
    /// Within the hub's ack wait, with the daemon running.
    Live,
    /// After JetStream gave up on the message and a watcher heard so, `watcher_first` or after the outbox copy
    /// landed, with the daemon restarted, `past_window`: after the hub's duplicate window ran out.
    Restarted { watcher_first: bool, past_window: bool },
}

#[expect(clippy::too_many_lines, reason = "one scenario, run for each way the uplink comes back")]
async fn dead_letter_kept_on_the_extra_delivery(back: UplinkBack) {
    let Some((hub, uplinked)) = Uplinked::start().await else { return };
    let past_window = matches!(back, UplinkBack::Restarted { past_window: true, .. });
    if past_window {
        uplinked.shorten_duplicate_window().await;
    }
    let (bus, _) = Bus::connect(hub, &uplinked.url(), Some(&"hub".parse().unwrap())).await;
    let (launch, log, gate) = fake::launch();
    let t1 = ThreadId::generate();
    let hold = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "hold a");
    let doomed = Envelope::on_thread(t1.clone(), UserId::human(), bus.dev.clone(), "doomed b");
    let hold_seq = bus::publish_json(&uplinked.hub_js, hold.subject(), &hold, None).await.unwrap();
    let seq = bus::publish_json(&uplinked.hub_js, doomed.subject(), &doomed, None).await.unwrap();
    let progress_every = Duration::from_millis(200);
    // Live, the hub still waits for the ack of the extra delivery when the uplink is back.
    let ack_wait = if back == UplinkBack::Live { WAIT * 3 } else { ack_wait(progress_every) };
    let stream = uplinked.hub_js.get_stream(ants_proto::STREAM).await.unwrap();

    // The doomed message's five attempts never finished; the held one, ahead of it on the thread, had one.
    let consumer: PullConsumer = stream
        .create_consumer(jetstream::consumer::pull::Config {
            durable_name: Some(ants_proto::inbox_consumer(&bus.dev)),
            filter_subject: subject::inbox_of(&bus.dev),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            ack_wait: WAIT,
            max_deliver: MAX_DELIVER,
            ..Default::default()
        })
        .await
        .unwrap();
    let fetch = |n| {
        let consumer = consumer.clone();
        async move {
            let batch = consumer.fetch().max_messages(n).expires(WAIT).messages().await.unwrap();
            batch.map(Result::unwrap).collect::<Vec<_>>().await
        }
    };
    let mut first = fetch(2).await;
    let (mut held, mut last) = (first.remove(0), first.remove(0));
    for _ in 1..MAX_ATTEMPTS {
        last.ack_with(AckKind::Nak(None)).await.unwrap();
        last = fetch(1).await.remove(0);
    }
    assert_eq!(last.info().unwrap().delivered, MAX_ATTEMPTS);
    for msg in [&mut held, &mut last] {
        msg.ack_with(AckKind::Nak(None)).await.unwrap();
    }

    let buried = &bus.buried;
    let until_buried = |kept: bool| async move {
        let done = timeout(WAIT, async {
            while buried.undelivered().await.is_empty() == kept {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        done.await.unwrap_or_else(|_| panic!("{}", if kept { "not kept" } else { "not buried" }));
    };
    let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
    let ctrl_c = Arc::new(Semaphore::new(0));
    let down = async {
        until("the prompt to start", || log.locked().starts == 1).await;
        uplinked.switch(Link::Down).await;
        gate.add_permits(1);
        until_buried(true).await;
        let kept = bus.buried.get(&doomed).await.expect("noted on the machine");
        if back == UplinkBack::Live {
            uplinked.switch(Link::Up).await;
            until_buried(false).await;
            // Only the held message is left, whose ack failed while the uplink was down.
            timeout(WAIT, async {
                while bus.unacked().await != 1 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("the doomed message is not terminated");
        }
        ctrl_c.add_permits(1);
        kept
    };
    let ((), kept) = tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
    drop(pool);
    assert!(kept.last_delivery.is_some(), "{kept:?}");
    assert_eq!(kept.dead.stream_seq, seq);

    let notices = || async {
        let mut notices = Vec::new();
        let (subject, mut from) = (subject::inbox(&UserId::human(), &bus.dev), 1);
        while let Ok(msg) = stream.get_first_raw_message_by_subject(&subject, from).await {
            from = msg.sequence + 1;
            let notice: Envelope = serde_json::from_slice(&msg.payload).unwrap();
            notices.extend((notice.in_reply_to == Some(doomed.id)).then_some(notice));
        }
        notices
    };
    if let UplinkBack::Restarted { watcher_first, .. } = back {
        // The watcher's view of JetStream's max-deliveries advisory.
        let advisory = dead::MaxDeliveries {
            stream: ants_proto::STREAM.to_owned(),
            consumer: ants_proto::inbox_consumer(&bus.dev),
            stream_seq: seq,
            deliveries: MAX_DELIVER as u64,
        };
        let humans = Registry::default();
        let sessions = bus::ensure_sessions(&uplinked.hub_js, None, &UserId::human()).await.unwrap();
        let threads = Threads::new(uplinked.hub_js.clone(), UserId::human(), sessions);
        let claims = Claims { humans: &humans, threads: &threads };
        let watch = || dead::bury_undelivered(&uplinked.hub_js, &stream, &advisory, &claims);
        // As the hub's stream takes the outbox copy once the uplink is back.
        let dead_id = kept.dead.msg_id();
        let outbox_copy = || bus::publish_json(&uplinked.hub_js, kept.dead.subject(), &kept.dead, Some(&dead_id));
        if watcher_first {
            let (dead_seq, _) = watch().await.unwrap().expect("buried by the watcher");
            uplinked.switch(Link::Up).await;
            assert_eq!(outbox_copy().await.unwrap(), dead_seq, "the outbox copy is dropped");
        } else {
            uplinked.switch(Link::Up).await;
            outbox_copy().await.unwrap();
            assert_eq!(watch().await.unwrap(), None, "a dead letter already");
            assert!(notices().await.is_empty(), "the watcher sends no notice for the outbox copy");
        }
        if past_window {
            tokio::time::sleep(SHORT_WINDOW * 3).await;
        }

        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let check = async {
            bus.until_acked(hold_seq).await;
            until_buried(false).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        assert_eq!(bus.unacked().await, 0);
    }
    let (letters, notices) = (dead::list(&stream).await.unwrap(), notices().await);
    let prompted = {
        let log = log.locked();
        (log.starts, log.doomed)
    };
    assert_eq!(prompted, (1, 0), "neither prompted again nor the doomed one at all");
    assert_eq!(letters.len(), 1, "{letters:?}");
    assert_eq!(notices.len(), 1, "{notices:?}");
    assert_eq!(notices[0].thread_id, t1);
    assert!(notices[0].text.contains(&format!("ants dead replay {}", letters[0].0)), "{}", notices[0].text);
    assert!(bus.buried.get(&doomed).await.is_none(), "forgotten once terminated");
}
