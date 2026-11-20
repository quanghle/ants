//! Publishing to the ants stream and storing in the hub, explaining a publish no stream captures.

use std::time::Duration;

use ants_proto::{Envelope, FILES_STREAM, STREAM, UserId, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use async_nats::jetstream::context::PublishErrorKind;

use crate::sync::MutexExt as _;

use super::connect::reach_jetstream;

/// Where [`store`] put a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    /// In the hub's stream.
    Hub,
    /// In the outbox of the machine's leaf node, which the hub's stream takes it from once the uplink is back.
    Outbox,
}

/// Publish an envelope and wait until JetStream has stored it, in the hub's stream or, out of its reach, in the
/// machine's outbox ([`store`]).
pub async fn publish(js: &jetstream::Context, envelope: &Envelope) -> Result<Stored> {
    store(js, envelope.subject(), envelope, &envelope.id.to_string()).await
}

/// Publish `value` as JSON to `subject` in the hub's stream and wait until it is stored. When no stream answers (the
/// leaf node's uplink is down) or none answers in time, store it in the leaf node's outbox instead, under
/// [`subject::outbox`], for the hub to source once the uplink is back. Both copies carry `msg_id`, so the hub's stream
/// keeps one if the first publish did land after all, as long as the second arrives within its duplicate window
/// ([`DUPLICATE_WINDOW`](super::DUPLICATE_WINDOW)).
/// When no stream captures `subject` though the hub's JetStream answers ([`uncaptured`]), the error, or the warning
/// for a message kept in the outbox, names `ants init` as the fix; the warning comes once per kind of subject.
pub async fn store<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Stored> {
    let ack = store_acked(js, subject, value, msg_id).await?;
    Ok(if ack.is_some() { Stored::Hub } else { Stored::Outbox })
}

/// [`store`], returning the hub stream's ack, or `None` when the message went to the machine's outbox.
pub async fn store_acked<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Option<jetstream::publish::PublishAck>> {
    let payload = serde_json::to_vec(value)?;
    let message = || jetstream::message::PublishMessage::build().payload(payload.clone().into()).message_id(msg_id);
    let unreachable = match publish_to_hub(js, &subject, message()).await {
        Ok(ack) => return Ok(Some(ack)),
        Err(e) if out_of_reach(&e) => e,
        Err(e) => return Err(e).context("waiting for JetStream to store the message"),
    };
    let uncaptured = unreachable.kind() == PublishErrorKind::StreamNotFound && uncaptured(js).await;
    let Some(outbox) = subject::outbox(&subject) else {
        return Err(explained(unreachable, &subject, uncaptured)).context("waiting for JetStream to store the message");
    };
    let kept = match js.send_publish(outbox, message()).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    if let Err(e) = kept {
        if uncaptured {
            return Err(explained(unreachable, &subject, uncaptured))
                .context("waiting for JetStream to store the message");
        }
        return Err(anyhow::anyhow!(e)).with_context(|| {
            format!(
                "the hub's stream did not store {subject} ({unreachable}), nor did this machine's outbox \
                 (`ants leaf outbox` sets it up on a leaf node)"
            )
        });
    }
    if uncaptured {
        warn_uncaptured(&subject, "kept in this machine's outbox");
    } else {
        tracing::warn!(%subject, "the hub's stream is out of reach ({unreachable}); kept in this machine's outbox");
    }
    Ok(None)
}

/// After a publish found no stream capturing its subject: whether the hub's JetStream answers all the same, so the hub
/// is in reach and its `ANTS` stream is missing, as when no `ants init` with a human's credentials has run yet. Out of
/// the hub's reach (a leaf node's uplink is down), false. It asks for the info of the connection's own inbox consumer,
/// as [`reach_jetstream`] does, which every user's credentials allow; the user is the one the connection's reply
/// inboxes are named after ([`inbox_prefix`](ants_proto::inbox_prefix)).
pub async fn uncaptured(js: &jetstream::Context) -> bool {
    let inbox = js.client().new_inbox();
    let Some(user) = inbox.strip_prefix("_INBOX.").and_then(|rest| rest.split('.').next()?.parse::<UserId>().ok())
    else {
        return false;
    };
    matches!(tokio::time::timeout(PROBE_TIMEOUT, reach_jetstream(js, &user)).await, Ok(Ok(())))
}

/// How long [`uncaptured`] waits for JetStream: a leaf node whose uplink is down answers at once with no responders.
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// What to do when no stream captures `subject` while the hub is in reach ([`uncaptured`]): the stream missing is the
/// attachments' store for a chunk of an attachment, else the `ANTS` stream.
fn uncaptured_hint(subject: &str) -> String {
    let stream = if subject.starts_with("$O.") { FILES_STREAM } else { STREAM };
    format!(
        "the hub's JetStream answers, but no stream captures {subject}: its {stream} stream is missing; run `ants init` \
         with a human's credentials to create it"
    )
}

/// `e`, the error of a publish to `subject`, with [`uncaptured_hint`] when `uncaptured`.
pub fn explained(e: jetstream::context::PublishError, subject: &str, uncaptured: bool) -> anyhow::Error {
    let e = anyhow::Error::new(e);
    if uncaptured { e.context(uncaptured_hint(subject)) } else { e }
}

/// `e`, the error of a publish to `subject` in the hub's stream, with what to do about it when no stream captures the
/// subject while the hub is in reach ([`uncaptured`]).
pub async fn explain(js: &jetstream::Context, subject: &str, e: jetstream::context::PublishError) -> anyhow::Error {
    let uncaptured = e.kind() == PublishErrorKind::StreamNotFound && uncaptured(js).await;
    explained(e, subject, uncaptured)
}

/// The kinds of subject ([`subject_kind`]) [`warn_uncaptured`] warned about.
pub(super) static WARNED_UNCAPTURED: std::sync::Mutex<std::collections::BTreeSet<String>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

/// The first two tokens of `subject`, such as `ants.chan`: what the `ANTS` stream captures or not.
pub(super) fn subject_kind(subject: &str) -> &str {
    subject.match_indices('.').nth(1).map_or(subject, |(at, _)| &subject[..at])
}

/// Warn that the hub's stream does not capture `subject` ([`uncaptured_hint`]), once per process for each kind of
/// subject ([`subject_kind`]), then only at debug level. True when it warned.
pub(crate) fn warn_uncaptured(subject: &str, done: &str) -> bool {
    let first = WARNED_UNCAPTURED.locked().insert(subject_kind(subject).to_owned());
    if first {
        tracing::warn!(%subject, "{}; {done}", uncaptured_hint(subject));
    } else {
        tracing::debug!(%subject, "the hub's stream does not capture the subject; {done}");
    }
    first
}

/// Publish `value` as JSON to `subject` in the hub's stream only, never the machine's outbox, and wait until it is
/// stored, returning the stream's ack, or `None` while the hub is out of reach (see [`store`]) or its stream does not
/// capture `subject`, which a warning says once ([`uncaptured`]).
pub async fn store_in_hub<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: &str,
) -> Result<Option<jetstream::publish::PublishAck>> {
    let message =
        jetstream::message::PublishMessage::build().payload(serde_json::to_vec(value)?.into()).message_id(msg_id);
    match publish_to_hub(js, &subject, message).await {
        Ok(ack) => Ok(Some(ack)),
        Err(e) if out_of_reach(&e) => {
            if e.kind() == PublishErrorKind::StreamNotFound && uncaptured(js).await {
                warn_uncaptured(&subject, "kept until the hub's stream captures it");
            }
            Ok(None)
        }
        Err(e) => Err(e).context("waiting for JetStream to store the message"),
    }
}

async fn publish_to_hub(
    js: &jetstream::Context,
    subject: &str,
    message: jetstream::message::PublishMessage,
) -> Result<jetstream::publish::PublishAck, jetstream::context::PublishError> {
    js.send_publish(subject.to_owned(), message).await?.await
}

/// Whether publishing failed because no stream answered (the leaf node's uplink is down) or none answered in time.
pub(crate) fn out_of_reach(e: &jetstream::context::PublishError) -> bool {
    matches!(e.kind(), PublishErrorKind::StreamNotFound | PublishErrorKind::TimedOut)
}

/// Publish `value` as JSON and wait until JetStream has stored it, returning its stream sequence. With `msg_id`, a
/// second publish within the duplicate window stores nothing and returns the first one's sequence.
pub async fn publish_json<T: serde::Serialize>(
    js: &jetstream::Context,
    subject: String,
    value: &T,
    msg_id: Option<&str>,
) -> Result<u64> {
    let mut message = jetstream::message::PublishMessage::build().payload(serde_json::to_vec(value)?.into());
    if let Some(id) = msg_id {
        message = message.message_id(id);
    }
    let ack = js.send_publish(subject.clone(), message).await.context("publishing")?.await;
    match ack {
        Ok(ack) => Ok(ack.sequence),
        Err(e) => Err(explain(js, &subject, e).await).context("waiting for JetStream to store the message"),
    }
}
