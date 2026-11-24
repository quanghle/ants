//! The human side's backstop: the max-deliveries advisories of JetStream, turned into dead letters for the messages an
//! ant's daemon gave up on without storing one.

use std::pin::pin;
use std::time::Duration;

use ants_proto::{
    ChannelPost, DeadLetter, Envelope, SESSIONS_BUCKET, STREAM, ThreadOrigin, UserId, chan_consumer, inbox_consumer,
    subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use async_nats::jetstream::message::{PublishMessage, StreamMessage};
use async_nats::jetstream::stream::{self, RawMessageErrorKind};
use futures::{StreamExt as _, TryStreamExt as _};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::letters::letters_on;
use super::{Burying, letter_of};
use crate::bus;
use crate::bus::relayed::{self, Read};
use crate::channel::{Reading, Threads};
use crate::daemon::Claims;
use crate::people::humans::Registry;
use crate::records::direct_kv::DirectKv;

/// How often [`watch`] tries again the burials that failed.
const BURY_RETRY: Duration = Duration::from_secs(30);
/// Most failed burials [`watch`] keeps trying; past them, a failure is only logged.
const BURY_RETRIES_KEPT: usize = 10_000;

/// What a max-deliveries advisory of JetStream says (`io.nats.jetstream.advisory.v1.max_deliver`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MaxDeliveries {
    pub stream: String,
    pub consumer: String,
    pub stream_seq: u64,
    pub deliveries: u64,
}

/// The consumers of an ant that dead-letter what they give up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Consumer {
    Inbox,
    Channels,
}

impl MaxDeliveries {
    /// The ant whose inbox or channel consumer gave up, and which one, or `None` for any other consumer.
    fn ant(&self) -> Option<(UserId, Consumer)> {
        if self.stream != STREAM {
            return None;
        }
        let (kind, name) = match (self.consumer.strip_prefix("inbox-"), self.consumer.strip_prefix("chan-")) {
            (Some(name), _) => (Consumer::Inbox, name),
            (_, Some(name)) => (Consumer::Channels, name),
            _ => return None,
        };
        let user: UserId = name.parse().ok()?;
        let own = match kind {
            Consumer::Inbox => inbox_consumer(&user),
            Consumer::Channels => chan_consumer(&user),
        };
        (own == self.consumer).then_some((user, kind))
    }
}

/// Hear the max-deliveries advisories of the stream's consumers and dead-letter each message an ant's inbox or
/// channel consumer stopped delivering, as its daemon would have, handing every new dead letter and its sequence to `buried`
/// and counting it in the ant's metrics ([`crate::monitoring::metrics::Ant::buried`]).
/// Channel threads are read as `reader`, the human, by message gets ([`Reading::Gets`]), so no other read of theirs
/// gets in the way. Runs until the connection closes.
/// Needs the human's credentials.
pub async fn watch(
    client: &async_nats::Client,
    js: &jetstream::Context,
    reader: &UserId,
    mut buried: impl FnMut(u64, &DeadLetter),
) -> Result<()> {
    let mut advisories = relayed::subscribe(client, subject::max_deliveries_advisories())
        .await
        .context("subscribing to max-deliveries advisories")?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    let humans = Registry::new(reader.clone());
    let _watching = humans.watch(js.clone(), crate::people::humans::RETRY);
    // The human's side neither reads nor writes sessions, which its credentials do not allow.
    let sessions = DirectKv::new(js, None, SESSIONS_BUCKET).await?;
    let threads = Threads::new(js.clone(), reader.clone(), sessions).reading(Reading::Gets);
    let claims = Claims { humans: &humans, threads: &threads };
    // The advisories whose burial failed for a while, the hub's store full or its JetStream not answering, tried again
    // every `BURY_RETRY`.
    let mut failed: Vec<MaxDeliveries> = Vec::new();
    let mut retry = tokio::time::interval_at(tokio::time::Instant::now() + BURY_RETRY, BURY_RETRY);
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let advisories = tokio::select! {
            msg = advisories.next() => {
                let Some(msg) = msg else { break };
                match serde_json::from_slice::<MaxDeliveries>(&msg.payload) {
                    Ok(advisory) => vec![advisory],
                    Err(e) => {
                        tracing::warn!(subject = %msg.subject, error = %e, "skipping an unreadable advisory");
                        continue;
                    }
                }
            }
            _ = retry.tick(), if !failed.is_empty() => std::mem::take(&mut failed),
        };
        for advisory in advisories {
            match bury_undelivered(js, &stream, &advisory, &claims).await {
                Ok(Some((seq, dead))) => {
                    crate::monitoring::metrics::ant(&dead.ant).buried();
                    buried(seq, &dead);
                }
                Ok(None) => {}
                Err(e) => {
                    let seq = advisory.stream_seq;
                    let passing = crate::bus::store_refused(&e) || crate::bus::unanswered(&e);
                    if passing && failed.len() < BURY_RETRIES_KEPT && !failed.iter().any(|kept| kept == &advisory) {
                        tracing::warn!(stream_seq = seq, error = format!("{e:#}"), "dead-lettering; trying again");
                        failed.push(advisory);
                    } else {
                        tracing::warn!(stream_seq = seq, error = format!("{e:#}"), "dead-lettering");
                    }
                }
            }
        }
    }
    Ok(())
}

/// Dead-letter the message `advisory` names, unless it is not an ant's inbox message or channel post, is gone, or
/// already is a dead letter. Returns the new dead letter and its stream sequence. The notice goes to the thread's owner,
/// else the default human of `claims`, not up the ant's escalation chain, carrying a level-0 escalation (`direct`).
/// The owner and creator are checked as the
/// ant's daemon checks them: an envelope's against `claims` ([`Claims::sender_claims`]), and a channel post's as the
/// posts of its thread say ([`Threads::origin`]).
pub async fn bury_undelivered(
    js: &jetstream::Context,
    stream: &stream::Stream,
    advisory: &MaxDeliveries,
    claims: &Claims<'_>,
) -> Result<Option<(u64, DeadLetter)>> {
    let Some((ant, consumer)) = advisory.ant().filter(|(user, _)| !claims.humans.is_human(user)) else {
        tracing::debug!(consumer = advisory.consumer, "ignoring an advisory of a consumer that is no ant's inbox");
        return Ok(None);
    };
    let seq = advisory.stream_seq;
    if let Some(dead_seq) = find(stream, &ant, seq).await? {
        tracing::debug!(stream_seq = seq, dead_seq, "already a dead letter");
        return Ok(None);
    }
    let msg = match stream.get_raw_message(seq).await {
        Ok(msg) => msg,
        Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => {
            tracing::warn!(stream_seq = seq, %ant, "the message the daemon gave up on is gone");
            return Ok(None);
        }
        Err(e) => return Err(e).with_context(|| format!("reading message {seq}")),
    };
    let error = format!(
        "no outcome after {} deliveries, and the daemon could not dead-letter it; kept from JetStream's \
         max-deliveries advisory and told to a human directly, past the escalation chain, as the daemon is failing",
        advisory.deliveries
    );
    let (dead, post) = match consumer {
        Consumer::Inbox => {
            if subject::parse_inbox(&msg.subject).is_none_or(|(to, _)| to != ant) {
                bail!("message {seq} is on {}, not in the inbox of {ant}", msg.subject);
            }
            // As the daemon does: an envelope whose sender is forged is kept only as a payload.
            let mut envelope = genuine(&msg, |e: &Envelope| e.check_subject(&msg.subject).is_ok());
            if let Some(envelope) = envelope.as_mut() {
                claims.sender_claims(envelope, seq).await;
            }
            let max_payload = bus::max_payload(&js.client());
            (letter_of(&ant, seq, advisory.deliveries, &error, &msg.payload, envelope.as_ref(), max_payload), None)
        }
        Consumer::Channels => {
            if subject::parse_chan(&msg.subject).is_none() {
                bail!("message {seq} is on {}, not a channel post", msg.subject);
            }
            let mut post = genuine(&msg, |p: &ChannelPost| p.check_subject(&msg.subject).is_ok());
            if let Some(post) = post.as_mut() {
                let origin = claims.threads.origin(post, seq, &claims.humans.humans()).await.unwrap_or_else(|e| {
                    tracing::warn!(thread = %post.thread_id, error = format!("{e:#}"), "reading the thread's origin");
                    ThreadOrigin::default()
                });
                origin.apply(post);
            }
            (DeadLetter::of_payload(ant.clone(), seq, advisory.deliveries, &error, &msg.payload), post)
        }
    };
    // Straight to the hub's stream: the message to bury came from there.
    let message = PublishMessage::build().payload(serde_json::to_vec(&dead)?.into()).message_id(dead.msg_id());
    let ack = js
        .send_publish(dead.subject(), message)
        .await
        .context("publishing")?
        .await
        .context("waiting for JetStream to store the message")?;
    Burying { js, humans: claims.humans, escalator: None }.tell(&dead, ack.sequence, post.as_ref()).await?;
    if ack.duplicate {
        return Ok(None);
    }
    tracing::warn!(stream_seq = seq, dead_seq = ack.sequence, %ant, "dead-lettered from a max-deliveries advisory");
    Ok(Some((ack.sequence, dead)))
}

/// What `msg` holds, read as a `T` on its own subject (`on_subject`), unless a stream relayed it there.
fn genuine<T: DeserializeOwned>(msg: &StreamMessage, on_subject: impl Fn(&T) -> bool) -> Option<T> {
    let relayed = relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer);
    serde_json::from_slice::<T>(&msg.payload).ok().filter(|held| !relayed && on_subject(held))
}

/// The sequence of the dead letter of `ant` kept for stream sequence `seq`, if there is one.
async fn find(stream: &stream::Stream, ant: &UserId, seq: u64) -> Result<Option<u64>> {
    let subject = subject::dead(ant);
    let mut letters = pin!(letters_on(stream, &subject));
    while let Some((dead_seq, letter)) = letters.try_next().await? {
        if letter.is_ok_and(|dead| dead.stream_seq == seq) {
            return Ok(Some(dead_seq));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_advisories_of_ant_inbox_and_channel_consumers_name_an_ant() {
        let json = r#"{"type":"io.nats.jetstream.advisory.v1.max_deliver","id":"x","timestamp":"2026-11-13T00:00:00Z",
            "stream":"ANTS","consumer":"inbox-dev-a","stream_seq":7,"deliveries":6}"#;
        let advisory: MaxDeliveries = serde_json::from_str(json).unwrap();
        assert_eq!((advisory.stream_seq, advisory.deliveries), (7, 6));
        assert_eq!(advisory.ant(), Some(("dev-a".parse().unwrap(), Consumer::Inbox)));
        let chan = MaxDeliveries { consumer: "chan-dev-a".into(), ..advisory.clone() };
        assert_eq!(chan.ant(), Some(("dev-a".parse().unwrap(), Consumer::Channels)));
        for (stream, consumer) in [("ANTS", "notes-dev-a"), ("ANTS", "chanread-dev-a"), ("OTHER", "inbox-dev-a")] {
            let other = MaxDeliveries { stream: stream.into(), consumer: consumer.into(), ..advisory.clone() };
            assert_eq!(other.ant(), None, "{stream} {consumer}");
        }
    }
}
