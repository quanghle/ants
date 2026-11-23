//! The dead letters the hub's stream holds, as `ants dead` lists, shows, replays and drops them.

use std::pin::pin;

use ants_proto::{DeadLetter, Envelope, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::HeaderMap;
use async_nats::jetstream;
use async_nats::jetstream::stream::{self, RawMessageErrorKind};
use futures::{Stream, TryStreamExt as _};

use crate::bus;
use crate::bus::relayed::{self, Read};

/// The messages `stream` holds on `subject`, dead letters by their subject, oldest first: each one's stream sequence
/// with the dead letter read from it, or why it is none.
pub(super) fn letters_on<'a, I>(
    stream: &'a stream::Stream<I>,
    subject: &'a str,
) -> impl Stream<Item = Result<(u64, Result<DeadLetter>)>> + 'a {
    futures::stream::try_unfold(1, move |from| async move {
        match stream.get_first_raw_message_by_subject(subject, from).await {
            Ok(msg) => {
                let dead = letter(&msg.subject, Some(&msg.headers), &msg.payload);
                Ok(Some(((msg.sequence, dead), msg.sequence + 1)))
            }
            Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => Ok(None),
            Err(e) => Err(e).context("reading dead letters"),
        }
    })
}

/// Every dead letter in the stream, oldest first, with its stream sequence. Needs the human's credentials.
pub async fn list<I>(stream: &stream::Stream<I>) -> Result<Vec<(u64, DeadLetter)>> {
    let mut letters = Vec::new();
    let mut stored = pin!(letters_on(stream, subject::DEAD_ALL));
    while let Some((seq, letter)) = stored.try_next().await? {
        match letter {
            Ok(dead) => letters.push((seq, dead)),
            Err(e) => tracing::warn!(seq, error = format!("{e:#}"), "skipping unreadable dead letter"),
        }
    }
    Ok(letters)
}

/// The dead letter at stream sequence `seq`.
pub async fn get<I>(stream: &stream::Stream<I>, seq: u64) -> Result<DeadLetter> {
    let msg = stream.get_raw_message(seq).await.with_context(|| format!("reading message {seq}"))?;
    if !msg.subject.starts_with("ants.dead.") {
        bail!("message {seq} is on {}, not a dead letter", msg.subject);
    }
    letter(&msg.subject, Some(&msg.headers), &msg.payload).with_context(|| format!("dead letter {seq}"))
}

/// The dead letter stored on `subject` with `headers`, as a consumer or a message get reads it. Credentials pin the
/// ant of a dead letter subject, so one naming another ant, or one a stream relayed there, is forged.
pub(super) fn letter(subject: &str, headers: Option<&HeaderMap>, payload: &[u8]) -> Result<DeadLetter> {
    if relayed::stored_relayed(headers, payload, Read::Consumer) {
        bail!("a message a stream relayed onto {subject}, not a dead letter");
    }
    let dead: DeadLetter = serde_json::from_slice(payload)?;
    if dead.subject() != subject {
        bail!("dead letter of {} arrived on {subject}", dead.ant);
    }
    Ok(dead)
}

/// Send the message of dead letter `seq` to its inbox again, as it was, then remove the dead letter. The human's
/// credentials may send under any sender, so only a message to the ant that buried it is replayed: an ant
/// cannot have the human deliver a message of its own making to another ant.
pub async fn replay(js: &jetstream::Context, stream: &stream::Stream, seq: u64) -> Result<Envelope> {
    let dead = get(stream, seq).await?;
    let Some(envelope) = dead.envelope else {
        bail!("dead letter {seq} holds no envelope to replay; `ants dead drop {seq}` removes it");
    };
    if envelope.to != dead.ant {
        bail!(
            "dead letter {seq} of {} holds a message to {}; `ants dead drop {seq}` removes it",
            dead.ant,
            envelope.to
        );
    }
    // The message id makes a second replay after a crash before the removal store nothing.
    bus::publish_json(js, envelope.subject(), &envelope, Some(&format!("replay-{seq}"))).await?;
    stream.delete_message(seq).await.with_context(|| format!("removing dead letter {seq}"))?;
    Ok(envelope)
}

/// Remove dead letter `seq` without replaying it.
pub async fn discard(stream: &stream::Stream, seq: u64) -> Result<DeadLetter> {
    let dead = get(stream, seq).await?;
    stream.delete_message(seq).await.with_context(|| format!("removing dead letter {seq}"))?;
    Ok(dead)
}
