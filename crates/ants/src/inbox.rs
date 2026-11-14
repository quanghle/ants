//! A human's inbox: the messages to the human in the stream, read by sequence (`STREAM.MSG.GET`), so every device of
//! the human reads all of them, and its [`ReadMarker`] in [`READ_BUCKET`], shared by those devices, which says which
//! it has read on any of them.
//!
//! Showing a message marks it read, unless peeking, for every device at once: the marker is updated with
//! compare-and-set, so devices reading at the same time each keep the other's marks. Nothing is consumed, so a reply
//! one device reads still reaches the others following the inbox. The marker replaced the shared durable consumer
//! `inbox-<name>`; the first read with credentials that write the marker moves the consumer's read state into it and
//! deletes the consumer ([`migrate`]).

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::reading::{self, Reading};
use ants_proto::{Envelope, READ_BUCKET, ReadMarker, STREAM, ThreadId, UserId, inbox_consumer, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::consumer::{DeliverPolicy, PullConsumer};
use async_nats::jetstream::kv::{self, Operation, UpdateErrorKind};
use async_nats::jetstream::stream::ConsumerErrorKind;
use async_nats::jetstream::{self, ErrorCode};
use futures::{FutureExt as _, StreamExt as _};

const BATCH: usize = 256;
/// How often a followed inbox is read again with no message announced, against an announcement missed.
const RESCAN: Duration = Duration::from_secs(30);
/// How many times a mark is tried again after another device moved the marker first.
const CONFLICTS: usize = 16;

/// Which unread messages to take, and whether taking them marks them read.
#[derive(Debug, Clone, Default)]
pub struct Select {
    pub thread: Option<ThreadId>,
    pub limit: Option<usize>,
    /// Show without marking read.
    pub peek: bool,
}

impl Select {
    fn wants(&self, e: &Envelope) -> bool {
        self.thread.as_ref().is_none_or(|t| *t == e.thread_id)
    }
}

/// The read bucket, created when missing. Entries never expire: a marker says what was read until it moves on.
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(READ_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(kv::Config {
        bucket: READ_BUCKET.to_owned(),
        description: "What each human has read of its inbox, on any of its devices".to_owned(),
        history: 1,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await
    .context("creating the read bucket (credentials issued before it need `ants auth user NAME --human` again)")
}

/// A message to the human, unless malformed or forged.
type Message = (u64, Result<Envelope, String>);

pub struct Inbox {
    js: jetstream::Context,
    kv: kv::Store,
    human: UserId,
    /// The first sequence read, once a read started.
    start: Option<u64>,
    /// The sequences of the inbox messages read from `start` on, in order, to move the marker past the read ones.
    seqs: Vec<u64>,
    /// The last sequence read.
    cursor: u64,
    taken: usize,
}

impl Inbox {
    /// The inbox of `human`, whose read state moves from its old inbox consumer into its marker if it has none yet.
    pub async fn open(js: &jetstream::Context, human: &UserId) -> Result<Self> {
        bus::ensure_stream(js).await?;
        let kv = ensure_bucket(js).await?;
        let inbox =
            Self { js: js.clone(), kv, human: human.clone(), start: None, seqs: Vec::new(), cursor: 0, taken: 0 };
        if inbox.kv.entry(human.as_str()).await.context("reading the read marker")?.is_none() {
            migrate(&inbox).await?;
        }
        Ok(inbox)
    }

    fn left(&self, select: &Select) -> usize {
        select.limit.map_or(usize::MAX, |limit| limit.saturating_sub(self.taken))
    }

    /// The marker and its revision, 0 when there is none.
    async fn marker(&self) -> Result<(ReadMarker, u64)> {
        let Some(entry) = self.kv.entry(self.human.as_str()).await.context("reading the read marker")? else {
            return Ok((ReadMarker::default(), 0));
        };
        if entry.operation != Operation::Put {
            return Ok((ReadMarker::default(), entry.revision));
        }
        let marker = serde_json::from_slice(&entry.value).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "a malformed read marker; starting over");
            ReadMarker::default()
        });
        Ok((marker, entry.revision))
    }

    /// Mark the messages at `seqs` read, on top of whatever other devices marked meanwhile.
    async fn mark(&self, seqs: &[u64]) -> Result<()> {
        if seqs.is_empty() {
            return Ok(());
        }
        for _ in 0..CONFLICTS {
            let (mut marker, revision) = self.marker().await?;
            seqs.iter().for_each(|seq| marker.mark(*seq));
            // The sequences read cover the inbox from `start` on only.
            if self.start.is_some_and(|start| marker.below + 1 >= start) {
                marker.compact(self.seqs.iter().copied());
            }
            match self.kv.update(self.human.as_str(), serde_json::to_vec(&marker)?.into(), revision).await {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => continue,
                Err(e) => return Err(e).context("marking read"),
            }
        }
        bail!("marking read: the read marker kept moving")
    }

    /// The messages to the human stored from `from` on.
    async fn read(&mut self, from: u64) -> Result<Vec<Message>> {
        self.start.get_or_insert(from);
        let mut found = Vec::new();
        let filter = subject::inbox_of(&self.human);
        let start = DeliverPolicy::ByStartSequence { start_sequence: from };
        reading::read_back(&self.js, self.human.as_str(), Reading::Gets, &filter, start, |seq, _, subject, payload| {
            found.push((seq, parse(subject, payload)));
            ControlFlow::Continue(())
        })
        .await
        .context("reading the inbox")?;
        for (seq, _) in &found {
            if *seq > self.cursor {
                self.seqs.push(*seq);
                self.cursor = *seq;
            }
        }
        Ok(found)
    }

    /// The selected unread messages available now, oldest first.
    pub async fn unread(&mut self, select: &Select) -> Result<Vec<Envelope>> {
        self.unread_where(select, |_| true).await.map(|found| found.into_iter().map(|(_, e)| e).collect())
    }

    async fn unread_where(
        &mut self,
        select: &Select,
        wanted: impl Fn(&Envelope) -> bool,
    ) -> Result<Vec<(u64, Envelope)>> {
        let (marker, _) = self.marker().await?;
        let mut found = Vec::new();
        let mut marks = Vec::new();
        for (seq, envelope) in self.read(marker.below + 1).await? {
            if marker.is_read(seq) {
                continue;
            }
            match envelope {
                Ok(e) if select.wants(&e) && wanted(&e) && self.left(select) > found.len() => found.push((seq, e)),
                Ok(_) => {}
                Err(e) => {
                    if !select.peek {
                        tracing::warn!(error = %e, seq, "marking a malformed message read");
                        marks.push(seq);
                    }
                }
            }
        }
        self.taken += found.len();
        if !select.peek {
            marks.extend(found.iter().map(|(seq, _)| *seq));
            self.mark(&marks).await?;
        }
        Ok(found)
    }

    /// Wait for new selected messages and hand each to `show`, until `stop` resolves or the limit is reached. Every
    /// device following the inbox shows each new message, even one another device already read.
    pub async fn follow(
        &mut self,
        select: &Select,
        mut show: impl FnMut(&Envelope) -> Result<()>,
        stop: impl Future<Output = ()>,
    ) -> Result<()> {
        tokio::pin!(stop);
        // The stream announces each message it stores on the human's own inboxes.
        let client = self.js.client();
        let mut stored = client.subscribe(subject::stored_inbox(&self.human)).await.context("following the inbox")?;
        if self.start.is_none() {
            // Only what is stored from now on.
            let (marker, _) = self.marker().await?;
            self.read(marker.below + 1).await?;
        }
        let mut rescan = tokio::time::interval(RESCAN);
        rescan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        rescan.tick().await;
        while self.left(select) > 0 {
            let mut marks = Vec::new();
            for (seq, envelope) in self.read(self.cursor + 1).await? {
                match envelope {
                    Ok(e) if select.wants(&e) && self.left(select) > 0 => {
                        self.taken += 1;
                        show(&e)?;
                        marks.push(seq);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, seq, "skipping a malformed message");
                        marks.push(seq);
                    }
                }
            }
            if !select.peek {
                self.mark(&marks).await?;
            }
            if self.left(select) == 0 {
                break;
            }
            tokio::select! {
                _ = &mut stop => break,
                msg = stored.next() => if msg.is_none() { break },
                _ = rescan.tick() => {}
            }
            // One read takes in every message announced so far.
            while let Some(Some(_)) = stored.next().now_or_never() {}
        }
        Ok(())
    }
}

/// The envelope of a message stored on `subject`, unless malformed or its sender forged.
fn parse(subject: &str, payload: &[u8]) -> Result<Envelope, String> {
    let envelope = serde_json::from_slice::<Envelope>(payload).map_err(|e| e.to_string())?;
    envelope.check_subject(subject).map_err(|e| format!("forged sender: {e}"))?;
    Ok(envelope)
}

/// Move the read state of the human's old inbox consumer, if any, into a new marker, and delete the consumer. A message
/// the consumer still has to deliver is unread, and so is every one it delivered past its ack floor when another
/// process holds some of them unacked, as those cannot be told apart from the acked ones. A marker another device put
/// first stays.
async fn migrate(inbox: &Inbox) -> Result<()> {
    let name = inbox_consumer(&inbox.human);
    let mut marker = ReadMarker::default();
    let mut found = true;
    match inbox.js.get_consumer_from_stream(&name, STREAM).await {
        Ok(consumer) => {
            let mut consumer: PullConsumer = consumer;
            let mut held = BTreeSet::new();
            loop {
                let mut batch =
                    consumer.fetch().max_messages(BATCH).messages().await.context("reading the old inbox")?;
                let mut fresh = false;
                while let Some(msg) = batch.next().await {
                    let msg = msg.map_err(|e| anyhow!(e)).context("reading the old inbox")?;
                    fresh |= held.insert(msg.info().map_err(|e| anyhow!(e))?.stream_sequence);
                }
                if !fresh {
                    break;
                }
            }
            let info = consumer.info().await.context("reading the old inbox")?;
            let (floor, delivered) = (info.ack_floor.stream_sequence, info.delivered.stream_sequence);
            // Held elsewhere too: by a CLI showing the inbox right now, or one that died holding it.
            let elsewhere = info.num_ack_pending > held.len();
            marker.below = floor;
            let mut seqs = Vec::new();
            let filter = subject::inbox_of(&inbox.human);
            let start = DeliverPolicy::ByStartSequence { start_sequence: floor + 1 };
            reading::read_back(&inbox.js, inbox.human.as_str(), Reading::Gets, &filter, start, |seq, _, _, _| {
                if seq > delivered {
                    return ControlFlow::Break(());
                }
                seqs.push(seq);
                ControlFlow::Continue(())
            })
            .await
            .context("reading the inbox")?;
            if !elsewhere {
                seqs.iter().filter(|seq| !held.contains(seq)).for_each(|seq| marker.mark(*seq));
            }
            marker.compact(seqs);
        }
        Err(e) if matches!(e.kind(), ConsumerErrorKind::JetStream(e) if e.error_code() == ErrorCode::CONSUMER_NOT_FOUND) =>
        {
            found = false;
        }
        Err(e) => return Err(e).with_context(|| format!("reading consumer {name}")),
    }
    match inbox.kv.update(inbox.human.as_str(), serde_json::to_vec(&marker)?.into(), 0).await {
        Ok(_) => {}
        Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => {}
        Err(e) => return Err(e).context("putting the read marker"),
    }
    if found {
        tracing::info!(consumer = name, below = marker.below, "moved the inbox read state into the read marker");
        if let Err(e) = inbox.js.delete_consumer_from_stream(&name, STREAM).await {
            tracing::debug!(error = %e, consumer = name, "deleting the old inbox consumer");
        }
    }
    Ok(())
}

/// Mark `reply`, already shown by `ants ask --wait`, read in the inbox of `human`. JetStream may store it a moment
/// after it was delivered, so a miss is retried briefly. Returns whether it was found unread.
pub async fn mark_read(js: &jetstream::Context, human: &UserId, reply: &Envelope) -> Result<bool> {
    let mut inbox = Inbox::open(js, human).await?;
    let select = Select { thread: Some(reply.thread_id.clone()), limit: Some(1), peek: false };
    for _ in 0..5 {
        if !inbox.unread_where(&select, |e| e.id == reply.id).await?.is_empty() {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(false)
}

/// A message for the terminal: a header line, then the text indented. A mention notice names its channel, the
/// sender that relayed it if any, and how to answer there.
pub fn render(e: &Envelope) -> String {
    let at = e.sent_at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M");
    let text: Vec<String> = e.text.lines().map(|line| format!("  {line}")).collect();
    let Some(mention) = &e.mention else {
        return format!("{at}  {} on thread {}\n{}\n", e.from, e.thread_id, text.join("\n"));
    };
    let (channel, thread) = (&mention.channel, &e.thread_id);
    let (author, relayed) = match &mention.author {
        Some(author) => (author, format!(" (relayed by {})", e.from)),
        None => (&e.from, String::new()),
    };
    format!(
        "{at}  {author} mentioned {} in #{channel} on thread {thread}{relayed}\n{}\n  (`ants post {channel} --thread \
         {thread} ...` answers on the thread)\n",
        e.to,
        text.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use ants_proto::Role;

    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;

    fn texts(envelopes: &[Envelope]) -> Vec<&str> {
        envelopes.iter().map(|e| e.text.as_str()).collect()
    }

    async fn read(js: &jetstream::Context, select: Select) -> Vec<Envelope> {
        read_as(js, &UserId::human(), select).await
    }

    async fn read_as(js: &jetstream::Context, human: &UserId, select: Select) -> Vec<Envelope> {
        Inbox::open(js, human).await.unwrap().unread(&select).await.unwrap()
    }

    /// A connection of a new device of `human`, with credentials of its own.
    async fn device(server: &Server, human: &UserId, label: &str) -> (async_nats::Client, jetstream::Context) {
        let creds = server.dir().join(format!("{human}-{label}.creds"));
        let permissions = ants_proto::Permissions::for_user(human, Role::Human);
        let team = server.dir().join("auth");
        let device = crate::auth::issue_device(&team, human, Role::Human.into(), permissions, Some(label), &creds);
        assert!(device.unwrap().is_some());
        let (client, mut js) = bus::connect(&server.url, Some(&creds), None, human, "test").await.unwrap();
        js.set_timeout(Duration::from_secs(1));
        (client, js)
    }

    #[test]
    fn a_relayed_mention_notice_shows_the_post_s_author_and_the_relay() {
        let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
        let post = ants_proto::ChannelPost::new("ops".parse().unwrap(), "github".parse().unwrap(), "deploy failed")
            .from_webhook(vec![alice.clone()]);
        let shown = render(&post.relayed_notice(&alice, &dev_a));
        let header = format!("github mentioned alice in #ops on thread {} (relayed by dev-a)\n", post.thread_id);
        assert!(shown.contains(&header), "{shown}");
        assert!(shown.contains("\n  deploy failed\n"), "{shown}");
    }

    /// Runs as the human and dev-a with their own credentials, so it also checks the human's permissions.
    #[tokio::test]
    async fn replies_stay_unread_until_shown() {
        let Some(server) = Server::start() else { return };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        bus::ensure_stream(&human).await.unwrap();
        let (_, agent) = server.connect(&dev_a).await;
        let asks =
            [Envelope::new(UserId::human(), dev_a.clone(), "a"), Envelope::new(UserId::human(), dev_a.clone(), "b")];
        for (ask, text) in [(&asks[0], "one"), (&asks[1], "two"), (&asks[0], "three")] {
            bus::publish(&agent, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        let forged = asks[0].reply(dev_a.clone(), "forged", Role::Human).unwrap();
        let subject = ants_proto::subject::inbox(&UserId::human(), &UserId::human());
        bus::publish_json(&human, subject, &forged, None).await.unwrap();

        let peek = Select { peek: true, ..Default::default() };
        assert_eq!(texts(&read(&human, peek.clone()).await), ["one", "two", "three"]);
        assert_eq!(texts(&read(&human, peek).await), ["one", "two", "three"], "peeking marks nothing read");

        let thread_b = Select { thread: Some(asks[1].thread_id.clone()), ..Default::default() };
        assert_eq!(texts(&read(&human, thread_b).await), ["two"]);
        let first = Select { limit: Some(1), ..Default::default() };
        assert_eq!(texts(&read(&human, first).await), ["one"]);

        let last = asks[0].reply(dev_a.clone(), "four", Role::Human).unwrap();
        bus::publish(&agent, &last).await.unwrap();
        assert!(mark_read(&human, &UserId::human(), &last).await.unwrap(), "ask --wait marks its reply read");
        assert_eq!(texts(&read(&human, Select::default()).await), ["three"]);
        assert!(read(&human, Select::default()).await.is_empty());
        assert!(!mark_read(&human, &UserId::human(), &last).await.unwrap());
    }

    #[tokio::test]
    async fn every_device_of_a_human_gets_every_reply_and_reading_on_one_reads_on_all() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a): (UserId, UserId, UserId) =
            ("alice".parse().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, laptop) = device(&server, &alice, "laptop").await;
        let (_, phone) = device(&server, &alice, "phone").await;
        let (b_client, b_js) = device(&server, &bob, "laptop").await;
        bus::ensure_stream(&laptop).await.unwrap();
        let (_, agent) = server.connect(&dev_a).await;
        let asks = [Envelope::new(alice.clone(), dev_a.clone(), "a"), Envelope::new(alice.clone(), dev_a.clone(), "b")];

        // Both devices follow the inbox; each shows the reply, and the first to show it marks it read for both.
        let follow = async |js: &jetstream::Context| {
            let mut inbox = Inbox::open(js, &alice).await.unwrap();
            assert!(inbox.unread(&Select::default()).await.unwrap().is_empty());
            let mut shown = Vec::new();
            let select = Select { limit: Some(1), ..Default::default() };
            let follow = inbox.follow(
                &select,
                |e| {
                    shown.push(e.text.clone());
                    Ok(())
                },
                std::future::pending(),
            );
            timeout(Duration::from_secs(10), follow).await.expect("no reply followed").unwrap();
            shown
        };
        let send = async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            bus::publish(&agent, &asks[0].reply(dev_a.clone(), "one", Role::Human).unwrap()).await.unwrap();
        };
        let (on_laptop, on_phone, ()) = tokio::join!(follow(&laptop), follow(&phone), send);
        assert_eq!((on_laptop, on_phone), (vec!["one".to_owned()], vec!["one".to_owned()]));
        assert!(read_as(&phone, &alice, Select::default()).await.is_empty(), "read on both");

        // Reading one thread on one device leaves the other unread on both.
        for (ask, text) in [(&asks[0], "two"), (&asks[1], "three"), (&asks[0], "four")] {
            bus::publish(&agent, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        let thread_b = Select { thread: Some(asks[1].thread_id.clone()), ..Default::default() };
        assert_eq!(texts(&read_as(&laptop, &alice, thread_b).await), ["three"]);
        let peek = Select { peek: true, ..Default::default() };
        assert_eq!(texts(&read_as(&phone, &alice, peek).await), ["two", "four"]);
        assert_eq!(texts(&read_as(&phone, &alice, Select::default()).await), ["two", "four"]);
        assert!(read_as(&laptop, &alice, Select::default()).await.is_empty());
        let kv = ensure_bucket(&laptop).await.unwrap();
        let marker: ReadMarker = serde_json::from_slice(&kv.get("alice").await.unwrap().unwrap()).unwrap();
        assert!(marker.read.is_empty(), "every read message is below the marker: {marker:?}");

        // Bob has an inbox and a marker of his own, and reaches neither of Alice's.
        bus::publish(&agent, &Envelope::new(dev_a.clone(), bob.clone(), "for bob")).await.unwrap();
        assert_eq!(texts(&read_as(&b_js, &bob, Select::default()).await), ["for bob"]);
        assert!(Inbox::open(&b_js, &alice).await.is_err(), "bob reads alice's read marker");
        let b_kv = ensure_bucket(&b_js).await.unwrap();
        assert!(b_kv.get("alice").await.is_err());
        assert!(b_kv.put("alice", "{}".into()).await.is_err(), "bob moves alice's read marker");
        let marker: ReadMarker = serde_json::from_slice(&kv.get("alice").await.unwrap().unwrap()).unwrap();
        assert!(marker.below > 0);
        let consumer = jetstream::consumer::pull::Config {
            durable_name: Some(inbox_consumer(&alice)),
            filter_subject: subject::inbox_of(&alice),
            ..Default::default()
        };
        assert!(b_js.create_consumer_on_stream(consumer, STREAM).await.is_err(), "bob reads alice's inbox");
        let mut announced = b_client.subscribe(subject::stored_inbox(&alice)).await.unwrap();
        b_client.flush().await.unwrap();
        bus::publish(&agent, &asks[0].reply(dev_a.clone(), "five", Role::Human).unwrap()).await.unwrap();
        assert!(timeout(Duration::from_millis(500), announced.next()).await.is_err(), "bob hears alice's replies");
    }

    #[tokio::test]
    async fn the_read_state_of_the_old_inbox_consumer_moves_into_the_marker() {
        let Some(server) = Server::start() else { return };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        let stream = bus::ensure_stream(&human).await.unwrap();
        let (_, agent) = server.connect(&dev_a).await;
        let ask = Envelope::new(UserId::human(), dev_a.clone(), "a");
        for text in ["one", "two", "three", "four"] {
            bus::publish(&agent, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        // As the CLI before the marker kept it: "one" and "three" read, "two" handed back, "four" never fetched.
        let name = inbox_consumer(&UserId::human());
        let config = jetstream::consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: subject::inbox_of(&UserId::human()),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            max_deliver: -1,
            max_ack_pending: -1,
            ..Default::default()
        };
        let consumer: PullConsumer = stream.create_consumer(config).await.unwrap();
        let mut batch = consumer.fetch().max_messages(3).messages().await.unwrap();
        for read in [true, false, true] {
            let msg = batch.next().await.unwrap().unwrap();
            let kind = if read { jetstream::AckKind::Ack } else { jetstream::AckKind::Nak(None) };
            msg.double_ack_with(kind).await.unwrap();
        }
        drop(batch);

        assert_eq!(texts(&read(&human, Select { peek: true, ..Default::default() }).await), ["two", "four"]);
        assert!(stream.get_consumer::<jetstream::consumer::pull::Config>(&name).await.is_err(), "deleted");
        assert_eq!(texts(&read(&human, Select::default()).await), ["two", "four"]);
        assert!(read(&human, Select::default()).await.is_empty());
    }
}
