//! Reading messages back from the ANTS stream, oldest first, up to the last one stored as the read starts.
//!
//! An ant's daemon reads through the ant's own reader consumer, named in its credentials with its filter pinned,
//! created for the read and deleted after it, one read at a time. The CLI with the ant's credentials reads through a
//! spare of that reader ([`spare_readers`]) taken by a strict create, so it never deletes a consumer the daemon or
//! another CLI read is reading through. A human gets the messages one by one (`STREAM.MSG.GET`), with no consumer.

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_proto::{Role, STREAM, Uuid, spare_readers};
use anyhow::{Context as _, Result};
use async_nats::jetstream::response::Response;
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
    /// By getting the messages one by one (`STREAM.MSG.GET`), with no consumer, so any number of reads at once: a
    /// human's.
    Gets,
}

impl Reading {
    /// How the CLI with credentials in `role` reads.
    pub fn cli(role: Role) -> Self {
        if role.is_human() { Self::Gets } else { Self::Spare }
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
    let config = consumer::pull::Config {
        filter_subject: filter.to_owned(),
        deliver_policy: start,
        ack_policy: consumer::AckPolicy::None,
        inactive_threshold: INACTIVE,
        ..Default::default()
    };
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
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("creating {name} (credentials issued before spare readers need `ants auth user` again)")
                    });
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("every spare of {reader} stays in use");
        }
        tokio::time::sleep(SPARE_RETRY).await;
    }
}

async fn read_through(
    consumer: &consumer::PullConsumer,
    mut take: impl FnMut(u64, SystemTime, async_nats::Message) -> ControlFlow<()>,
) -> Result<()> {
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
            if take(seq, stored, msg).is_break() {
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

/// A `STREAM.MSG.GET` request for the first message on `next_by_subj` at or after `seq`, or stored at or after
/// `start_time` (nats-server 2.11 or later).
#[derive(serde::Serialize)]
struct NextGet<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    next_by_subj: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_time: Option<String>,
}

#[derive(serde::Deserialize)]
struct Got {
    message: jetstream::stream::RawMessage,
}

async fn read_by_gets(
    js: &jetstream::Context,
    filter: &str,
    start: consumer::DeliverPolicy,
    mut take: impl FnMut(u64, SystemTime, async_nats::Message) -> ControlFlow<()>,
) -> Result<()> {
    let (mut seq, mut since) = match start {
        consumer::DeliverPolicy::All => (1, None),
        consumer::DeliverPolicy::ByStartSequence { start_sequence } => (start_sequence.max(1), None),
        consumer::DeliverPolicy::ByStartTime { start_time } => {
            let time = chrono::DateTime::<chrono::Utc>::from(SystemTime::from(start_time));
            (0, Some(time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)))
        }
        other => anyhow::bail!("reading from {other:?}"),
    };
    let last = js.get_stream(STREAM).await.context("opening the ANTS stream")?.cached_info().state.last_sequence;
    while seq <= last {
        let request = NextGet { seq: (seq > 0).then_some(seq), next_by_subj: filter, start_time: since.take() };
        let got: Response<Got> = js.request(format!("STREAM.MSG.GET.{STREAM}"), &request).await?;
        let msg = match got {
            Response::Ok(got) => {
                jetstream::message::StreamMessage::try_from(got.message).map_err(|e| anyhow::anyhow!(e))?
            }
            Response::Err { error } if error.error_code() == jetstream::ErrorCode::NO_MESSAGE_FOUND => break,
            Response::Err { error } => return Err(error.into()),
        };
        if msg.sequence > last {
            break;
        }
        let (sequence, stored) = (msg.sequence, msg.time.into());
        let msg = async_nats::Message {
            subject: msg.subject,
            reply: None,
            payload: msg.payload,
            headers: Some(msg.headers),
            status: None,
            description: None,
            length: 0,
        };
        if take(sequence, stored, msg).is_break() {
            break;
        }
        seq = sequence + 1;
    }
    Ok(())
}
