//! Reading messages back from the ANTS stream, oldest first, up to the last one stored as the read starts.
//!
//! An ant's daemon reads through the ant's own reader consumer, named in its credentials with its filter pinned,
//! created for the read and deleted after it, one read at a time. The CLI with the ant's credentials reads through a
//! spare of that reader ([`spare_readers`]) taken by a strict create, so it never deletes a consumer the daemon or
//! another CLI read is reading through. A human gets the messages with no consumer, in batches of direct gets
//! (`$JS.API.DIRECT.GET.ANTS`), one request per batch, and so does an assistant, its daemon and the CLI with its
//! credentials alike, which hold no reader consumers but its dead letters' ([`ants_proto::Permissions::for_assistant`]).

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_proto::{Role, STREAM, Uuid, spare_readers};
use anyhow::{Context as _, Result};
use async_nats::StatusCode;
use async_nats::jetstream::stream::ConsumerCreateStrictErrorKind;
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;

/// How long a reader consumer left behind by an interrupted read lives on.
const INACTIVE: Duration = Duration::from_secs(30);
/// Messages asked for at once.
const BATCH: u64 = 256;
/// How often a read waiting for a spare reader tries them again, until those left behind are gone.
const SPARE_RETRY: Duration = Duration::from_millis(200);

/// How a user reads messages back from the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reading {
    /// Through the user's own reader consumer, created for the read and deleted after it, so one read at a time: an
    /// ant's daemon, whose credentials may not get stream messages by sequence.
    Consumer,
    /// Through a free spare of the user's reader consumer ([`spare_readers`]), taken for the read and deleted after
    /// it: the CLI with an ant's credentials, while the ant's daemon reads through the reader itself.
    Spare,
    /// By getting the messages in batches of direct gets, with no consumer, so any number of reads at once: a
    /// human's, and an assistant's.
    Gets,
}

impl Reading {
    /// How the CLI with credentials in `role` reads, an assistant's when `assistant`.
    pub fn cli(role: Role, assistant: bool) -> Self {
        if role.is_human() || assistant { Self::Gets } else { Self::Spare }
    }

    /// How an ant's daemon reads through its own readers, an assistant's when `assistant`.
    pub fn daemon(assistant: bool) -> Self {
        if assistant { Self::Gets } else { Self::Consumer }
    }

    /// How a read beside the daemon's own reads of the same reader goes, the daemon reading as `self`: through a spare
    /// of the reader, unless by gets.
    #[must_use]
    pub fn beside(self) -> Self {
        if self == Self::Gets { Self::Gets } else { Self::Spare }
    }
}

/// Hand the messages stored on `filter` from `start` on (all, from a stream sequence or from a time), oldest first, to
/// `take` until it breaks, up to the last one stored as the read starts, read as `reading` says, through `reader` or
/// its spares for a consumer. `take` gets each message's stream sequence, the time it was stored, its subject and its
/// payload.
pub async fn read_back(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    start: consumer::DeliverPolicy,
    mut take: impl FnMut(u64, SystemTime, &str, &[u8]) -> ControlFlow<()>,
) -> Result<()> {
    read_back_messages(js, reader, reading, filter, start, |seq, stored, msg| {
        take(seq, stored, &msg.subject, &msg.payload)
    })
    .await
}

/// [`read_back`], handing `take` each whole message, headers included, as it was published (with no reply subject).
pub async fn read_back_messages(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    start: consumer::DeliverPolicy,
    take: impl FnMut(u64, SystemTime, async_nats::Message) -> ControlFlow<()>,
) -> Result<()> {
    read_back_with(js, reader, reading, filter, start, Now(take)).await
}

/// [`read_back_messages`], awaiting `take` on each message before reading on, so it may look something up first.
pub async fn read_back_awaiting(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    start: consumer::DeliverPolicy,
    take: impl AsyncFnMut(u64, SystemTime, async_nats::Message) -> ControlFlow<()>,
) -> Result<()> {
    read_back_with(js, reader, reading, filter, start, Awaiting(take)).await
}

/// What a read hands each message to, its sequence and the time it was stored: the function of
/// [`read_back_messages`] ([`Now`]) or of [`read_back_awaiting`] ([`Awaiting`]).
trait Take {
    fn take(&mut self, seq: u64, stored: SystemTime, msg: async_nats::Message)
    -> impl Future<Output = ControlFlow<()>>;
}

/// A function taking each message at once. Its future borrows nothing, so a read through it stays `Send`.
struct Now<F>(F);

impl<F: FnMut(u64, SystemTime, async_nats::Message) -> ControlFlow<()>> Take for Now<F> {
    fn take(
        &mut self,
        seq: u64,
        stored: SystemTime,
        msg: async_nats::Message,
    ) -> impl Future<Output = ControlFlow<()>> {
        std::future::ready((self.0)(seq, stored, msg))
    }
}

/// An async function, awaited on each message.
struct Awaiting<F>(F);

impl<F: AsyncFnMut(u64, SystemTime, async_nats::Message) -> ControlFlow<()>> Take for Awaiting<F> {
    fn take(
        &mut self,
        seq: u64,
        stored: SystemTime,
        msg: async_nats::Message,
    ) -> impl Future<Output = ControlFlow<()>> {
        (self.0)(seq, stored, msg)
    }
}

async fn read_back_with(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    start: consumer::DeliverPolicy,
    take: impl Take,
) -> Result<()> {
    let config = reader_config(filter, start);
    let (name, consumer) = match reading {
        Reading::Gets => return read_by_gets(js, filter, start, take).await,
        Reading::Consumer => {
            // A consumer left by an interrupted read would carry on where it stopped.
            let _ = js.delete_consumer_from_stream(reader, STREAM).await;
            let config = consumer::pull::Config { name: Some(reader.to_owned()), ..config };
            (reader.to_owned(), js.create_consumer_on_stream(config, STREAM).await?)
        }
        Reading::Spare => take_spare(js, reader, config).await?,
    };
    let read = read_through(&consumer, take).await;
    if let Err(e) = js.delete_consumer_from_stream(&name, STREAM).await {
        tracing::debug!(error = %e, consumer = name, "deleting the reader");
    }
    read
}

/// A reader consumer, or a spare of it, reading `filter` from `start` for a single read. Its state is kept in memory:
/// nats-server may hand a consumer created right after one of the same name was deleted the state that one left on
/// disk (a flush racing the delete), so a read would start where the deleted one stopped and get nothing.
fn reader_config(filter: &str, start: consumer::DeliverPolicy) -> consumer::pull::Config {
    consumer::pull::Config {
        filter_subject: filter.to_owned(),
        deliver_policy: start,
        ack_policy: consumer::AckPolicy::None,
        inactive_threshold: INACTIVE,
        memory_storage: true,
        ..Default::default()
    }
}

/// The stream sequence of the last message stored, read as `reading` says. An ant's credentials get no stream info
/// (it lists every subject stored), so its reader, or a spare of it, filtering `filter` is created to start after the
/// last message, and the sequence it delivered up to is that one's.
pub async fn last_sequence(js: &jetstream::Context, reader: &str, reading: Reading, filter: &str) -> Result<u64> {
    let config = reader_config(filter, consumer::DeliverPolicy::New);
    let (name, consumer) = match reading {
        Reading::Gets => {
            let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
            return Ok(stream.cached_info().state.last_sequence);
        }
        Reading::Consumer => {
            let _ = js.delete_consumer_from_stream(reader, STREAM).await;
            let config = consumer::pull::Config { name: Some(reader.to_owned()), ..config };
            (reader.to_owned(), js.create_consumer_on_stream(config, STREAM).await?)
        }
        Reading::Spare => take_spare(js, reader, config).await?,
    };
    let last = consumer.cached_info().delivered.stream_sequence;
    if let Err(e) = js.delete_consumer_from_stream(&name, STREAM).await {
        tracing::debug!(error = %e, consumer = name, "deleting the reader");
    }
    Ok(last)
}

/// The first free spare of `reader`, created with `config`. A strict create fails on a spare that exists with another
/// configuration, and the description makes this read's its own, so no two reads ever share a spare. When all are
/// taken, they are tried again until one left behind by an interrupted read is gone.
async fn take_spare(
    js: &jetstream::Context,
    reader: &str,
    config: consumer::pull::Config,
) -> Result<(String, consumer::PullConsumer)> {
    let config = consumer::pull::Config { description: Some(format!("read {}", Uuid::now_v7())), ..config };
    let deadline = tokio::time::Instant::now() + INACTIVE + INACTIVE / 2;
    loop {
        for name in spare_readers(reader) {
            let config = consumer::pull::Config { name: Some(name.clone()), ..config.clone() };
            match js.create_consumer_strict_on_stream(config, STREAM).await {
                Ok(consumer) => return Ok((name, consumer)),
                Err(e) if e.kind() == ConsumerCreateStrictErrorKind::AlreadyExists => {}
                Err(e) => return Err(e).with_context(|| format!("creating {name}")),
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("every spare of {reader} stays in use");
        }
        tokio::time::sleep(SPARE_RETRY).await;
    }
}

async fn read_through(consumer: &consumer::PullConsumer, mut take: impl Take) -> Result<()> {
    let mut left = consumer.cached_info().num_pending;
    while left > 0 {
        let mut batch = consumer.fetch().max_messages(left.min(BATCH) as usize).messages().await?;
        let mut got = 0;
        while let Some(msg) = batch.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
            got += 1;
            let Ok(info) = msg.info() else { continue };
            let (seq, stored) = (info.stream_sequence, SystemTime::from(info.published));
            let msg = async_nats::Message { reply: None, ..msg.message };
            if take.take(seq, stored, msg).await.is_break() {
                return Ok(());
            }
        }
        if got == 0 {
            break;
        }
        left = left.saturating_sub(got);
    }
    Ok(())
}

/// Where a read by direct gets goes on from: the first message on the filter at or after `seq`, or stored at or after
/// `since` when nothing was read yet.
struct Position {
    seq: u64,
    since: Option<String>,
}

impl Position {
    /// After the message stored at `seq`.
    fn after(&mut self, seq: u64) {
        (self.seq, self.since) = (seq + 1, None);
    }
}

async fn read_by_gets(
    js: &jetstream::Context,
    filter: &str,
    start: consumer::DeliverPolicy,
    take: impl Take,
) -> Result<()> {
    let at = match start {
        consumer::DeliverPolicy::All => Position { seq: 1, since: None },
        consumer::DeliverPolicy::ByStartSequence { start_sequence } => {
            Position { seq: start_sequence.max(1), since: None }
        }
        consumer::DeliverPolicy::ByStartTime { start_time } => {
            let time = chrono::DateTime::<chrono::Utc>::from(SystemTime::from(start_time));
            Position { seq: 0, since: Some(time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)) }
        }
        other => anyhow::bail!("reading from {other:?}"),
    };
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let last = stream.cached_info().state.last_sequence;
    read_by_batches(js, filter, at, last, take).await
}

/// Messages asked for at once by a direct get, and the bytes they come to at most (a larger message comes alone).
const DIRECT_BATCH: usize = 256;
const DIRECT_BATCH_BYTES: usize = 4 << 20;
/// The headers a direct get adds to each message, which a read hands on without, as the message was published.
const DIRECT_HEADERS: [&str; 6] =
    ["Nats-Stream", "Nats-Subject", "Nats-Sequence", "Nats-Time-Stamp", "Nats-Num-Pending", "Nats-Last-Sequence"];
const NUM_PENDING: &str = "Nats-Num-Pending";

/// A batched direct get: up to `batch` messages on `next_by_subj`, the first at or after `seq`, or stored at or after
/// `start_time`, followed by an end of batch.
#[derive(serde::Serialize)]
struct BatchGet<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    next_by_subj: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<String>,
    batch: usize,
    max_bytes: usize,
}

/// Read up to `last` by batched direct gets (`$JS.API.DIRECT.GET.ANTS`), one request per batch.
async fn read_by_batches(
    js: &jetstream::Context,
    filter: &str,
    mut at: Position,
    last: u64,
    mut take: impl Take,
) -> Result<()> {
    let client = js.client();
    let wait = client.timeout().unwrap_or(Duration::from_secs(10));
    let inbox = client.new_inbox();
    let mut replies = client.subscribe(inbox.clone()).await?;
    while at.seq <= last {
        let request = BatchGet {
            seq: (at.seq > 0).then_some(at.seq),
            next_by_subj: filter,
            start_time: at.since.clone(),
            batch: DIRECT_BATCH,
            max_bytes: DIRECT_BATCH_BYTES,
        };
        let request = async_nats::Request::new().inbox(inbox.clone()).payload(serde_json::to_vec(&request)?.into());
        js.send_request(format!("DIRECT.GET.{STREAM}"), request).await?;
        let mut got = 0;
        loop {
            let Ok(Some(msg)) = tokio::time::timeout(wait, replies.next()).await else {
                anyhow::bail!("no answer to a direct get of {STREAM}");
            };
            match msg.status {
                None => {}
                Some(StatusCode::NOT_FOUND) => return Ok(()),
                Some(status) if status.as_u16() == 204 => {
                    let pending = msg.headers.as_ref().and_then(|h| h.get(NUM_PENDING)).map(|n| n.as_str().parse());
                    if got == 0 || matches!(pending, Some(Ok(0u64))) {
                        return Ok(());
                    }
                    break;
                }
                Some(status) => {
                    anyhow::bail!("a direct get of {STREAM} answered {status} {:?}", msg.description);
                }
            }
            got += 1;
            let msg = jetstream::message::StreamMessage::try_from(msg).map_err(|e| anyhow::anyhow!(e))?;
            if msg.sequence > last {
                return Ok(());
            }
            let mut headers = async_nats::HeaderMap::new();
            for (name, values) in msg.headers.iter() {
                if !DIRECT_HEADERS.iter().any(|h| AsRef::<str>::as_ref(name).eq_ignore_ascii_case(h)) {
                    values.iter().for_each(|value| headers.append(name.clone(), value.clone()));
                }
            }
            let (sequence, stored) = (msg.sequence, msg.time.into());
            at.after(sequence);
            if take.take(sequence, stored, published(msg, headers)).await.is_break() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// `msg` as it was published, with `headers` and no reply subject.
fn published(msg: jetstream::message::StreamMessage, headers: async_nats::HeaderMap) -> async_nats::Message {
    let (subject, payload, headers) = (msg.subject, msg.payload, Some(headers));
    async_nats::Message { subject, reply: None, payload, headers, status: None, description: None, length: 0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ants_proto::{ThreadId, UserId, subject};
    use ants_test_support::NatsServer;

    async fn connect(server: &NatsServer) -> (async_nats::Client, jetstream::Context) {
        let client = async_nats::connect(&server.url).await.unwrap();
        (client.clone(), jetstream::new(client))
    }

    /// Every message `read` gets on `filter` from `start`, with its sequence.
    async fn read(
        js: &jetstream::Context,
        filter: &str,
        start: consumer::DeliverPolicy,
        mut until: impl FnMut(u64) -> bool,
    ) -> Vec<(u64, async_nats::Message)> {
        let mut got = Vec::new();
        read_back_messages(js, "", Reading::Gets, filter, start, |seq, _, msg| {
            got.push((seq, msg));
            if until(seq) { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        })
        .await
        .unwrap();
        got
    }

    /// How many batched direct gets reach the stream.
    async fn count(client: &async_nats::Client) -> impl Fn() -> usize + use<> {
        let counts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut direct = client.subscribe(format!("$JS.API.DIRECT.GET.{STREAM}")).await.unwrap();
        // A round trip, so the server has the subscription before another client's first request.
        jetstream::new(client.clone()).query_account().await.unwrap();
        let tally = counts.clone();
        tokio::spawn(async move {
            while direct.next().await.is_some() {
                tally.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
        move || counts.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// nats-server may hand a consumer the state a deleted one of the same name left on disk (its last flush racing
    /// the delete), as a spare taken right after another read deleted it can be: the read then starts where that one
    /// ended and gets nothing. Here the state such a reader left is put back by hand, so the next read finds it.
    #[tokio::test]
    async fn a_read_through_a_spare_ignores_the_state_a_deleted_one_left_behind() {
        let Some(server) = NatsServer::start() else { return };
        let (_client, js) = connect(&server).await;
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        let thread: ThreadId = "t1".parse().unwrap();
        let notes = subject::notes(&thread, &"dev-a".parse::<UserId>().unwrap());
        for i in 0..3 {
            js.publish(notes.clone(), format!("n{i}").into()).await.unwrap().await.unwrap();
        }
        let reader = "reader-dev-a";
        let spare = spare_readers(reader).next().unwrap();
        let dir = server.dir().join(format!("js/jetstream/$G/streams/{STREAM}/obs/{spare}"));
        let state = dir.join("o.dat");

        // A spare stored on disk as nats-server stores consumers by default, read to the end and flushed.
        let config = consumer::pull::Config {
            name: Some(spare.clone()),
            filter_subject: notes.clone(),
            ack_policy: consumer::AckPolicy::None,
            ..Default::default()
        };
        let old: consumer::PullConsumer = js.create_consumer_on_stream(config, STREAM).await.unwrap();
        let started = std::fs::read(&state).unwrap();
        assert_eq!(old.fetch().max_messages(3).messages().await.unwrap().count().await, 3);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while std::fs::read(&state).unwrap() == started {
            assert!(tokio::time::Instant::now() < deadline, "the read state is flushed");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let left: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (path.clone(), std::fs::read(path).unwrap())
            })
            .collect();
        js.delete_consumer_from_stream(&spare, STREAM).await.unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        for (path, bytes) in left {
            std::fs::write(path, bytes).unwrap();
        }

        let mut got = Vec::new();
        read_back(&js, reader, Reading::Spare, &notes, consumer::DeliverPolicy::All, |_, _, _, payload| {
            got.push(String::from_utf8(payload.to_vec()).unwrap());
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        assert_eq!(got, ["n0", "n1", "n2"]);
    }

    #[tokio::test]
    async fn a_human_reads_thousands_of_messages_in_batches() {
        let Some(server) = NatsServer::start() else { return };
        let (watcher, js) = connect(&server).await;
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        let (human, dev_a): (UserId, UserId) = ("human".parse().unwrap(), "dev-a".parse().unwrap());
        let thread: ThreadId = "t1".parse().unwrap();
        let (to_human, notes) = (subject::inbox(&human, &dev_a), subject::notes(&thread, &dev_a));
        // 1500 messages to the human among as many notes, the first ones before a moment to read from.
        let mut sent = Vec::new();
        let mut moment = None;
        for i in 0..3000u32 {
            if i == 1000 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                moment = Some(SystemTime::now());
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let (to, payload) = if i % 2 == 0 { (&to_human, format!("m{i}")) } else { (&notes, format!("n{i}")) };
            let mut headers = async_nats::HeaderMap::new();
            headers.insert(async_nats::header::NATS_MESSAGE_ID, format!("id-{i}").as_str());
            let ack = js.publish_with_headers(to.clone(), headers, payload.clone().into()).await.unwrap();
            let seq = ack.await.unwrap().sequence;
            if to == &to_human {
                sent.push((seq, payload));
            }
        }
        let all = consumer::DeliverPolicy::All;
        let filter = subject::inbox_of(&human);
        let body = |got: &[(u64, async_nats::Message)]| -> Vec<(u64, String)> {
            got.iter().map(|(seq, msg)| (*seq, String::from_utf8(msg.payload.to_vec()).unwrap())).collect()
        };

        let (_client, new) = connect(&server).await;
        let requests = count(&watcher).await;
        let got = read(&new, &filter, all, |_| false).await;
        assert_eq!(body(&got), sent);
        let (seq, first) = &got[0];
        let headers = first.headers.as_ref().unwrap();
        assert_eq!(first.subject.as_str(), to_human);
        assert_eq!(headers.get(async_nats::header::NATS_MESSAGE_ID).unwrap().as_str(), "id-0", "at {seq}");
        let names: Vec<&str> = headers.iter().map(|(name, _)| name.as_ref()).collect();
        assert_eq!(names, ["Nats-Msg-Id"], "as published");
        watcher.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(requests(), sent.len().div_ceil(DIRECT_BATCH), "one request per batch");

        // From a sequence, from a time, and up to where the taker stops.
        let from = consumer::DeliverPolicy::ByStartSequence { start_sequence: sent[700].0 };
        assert_eq!(body(&read(&new, &filter, from, |_| false).await), sent[700..]);
        let since = consumer::DeliverPolicy::ByStartTime { start_time: moment.unwrap().into() };
        assert_eq!(body(&read(&new, &filter, since, |_| false).await), sent[500..]);
        let stop = sent[300].0;
        assert_eq!(body(&read(&new, &filter, all, |seq| seq == stop).await), sent[..=300]);
        let nothing = consumer::DeliverPolicy::ByStartSequence { start_sequence: sent.last().unwrap().0 + 1 };
        assert!(read(&new, &filter, nothing, |_| false).await.is_empty());
    }
}
