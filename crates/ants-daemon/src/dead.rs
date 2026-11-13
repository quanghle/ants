//! Dead letters: inbox messages a persona gave up on, kept on `ants.dead.<persona>` until the human replays or drops
//! them.
//!
//! The daemon writes a dead letter itself, with the message in hand: after its last attempt fails, or on the one
//! extra delivery the consumer allows when the last attempt never finished (the daemon stopped, or the ack wait ran
//! out). Watching JetStream's max-deliveries advisory instead would mean fetching the message by sequence, and
//! `STREAM.MSG.GET` takes any sequence, so a persona allowed it could read every inbox.

use ants_proto::{DeadLetter, Envelope, PersonaId, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::{self, RawMessageErrorKind};
use async_nats::jetstream::{self, AckKind};

use crate::bus;

/// Where a delivery stands among the `attempts` a message gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempt {
    /// A failure is retried.
    Early,
    /// A failure makes the message a dead letter.
    Last,
    /// The last attempt never finished: the message is a dead letter without another try.
    Overdue,
}

impl Attempt {
    /// The standing of delivery number `delivered` (from 1).
    pub fn of(delivered: i64, attempts: i64) -> Self {
        match delivered.cmp(&attempts) {
            std::cmp::Ordering::Less => Self::Early,
            std::cmp::Ordering::Equal => Self::Last,
            std::cmp::Ordering::Greater => Self::Overdue,
        }
    }
}

/// Store `msg` as a dead letter of `persona`, tell the human when it was an envelope, then terminate it. Returns the
/// stream sequence of the dead letter. `envelope` is `None` for a payload that is not an envelope.
pub async fn bury(
    js: &jetstream::Context,
    persona: &PersonaId,
    msg: &jetstream::Message,
    envelope: Option<&Envelope>,
    error: &str,
) -> Result<u64> {
    let info = msg.info().map_err(|e| anyhow!(e))?;
    let (seq, deliveries) = (info.stream_sequence, info.delivered.max(0) as u64);
    let dead = match envelope {
        Some(e) => DeadLetter::of_envelope(persona.clone(), seq, deliveries, error, e.clone()),
        None => DeadLetter::of_payload(persona.clone(), seq, deliveries, error, &msg.payload),
    };
    let dead_seq = bus::publish_json(js, dead.subject(), &dead, Some(&dead.msg_id())).await?;
    if let Some(e) = envelope {
        let notice = notice(persona, e, &dead, dead_seq);
        bus::publish_json(js, notice.subject(), &notice, Some(&format!("dead-notice-{seq}"))).await?;
    }
    // Only now: a crash before this redelivers the message, and the message ids above keep the copies single.
    msg.ack_with(AckKind::Term).await.map_err(|e| anyhow!(e))?;
    tracing::warn!(stream_seq = seq, dead_seq, deliveries, error, "dead-lettered");
    Ok(dead_seq)
}

/// The message on the thread that tells the human a message died, so `ants ask --wait` returns too.
fn notice(persona: &PersonaId, envelope: &Envelope, dead: &DeadLetter, dead_seq: u64) -> Envelope {
    let mut notice = Envelope::on_thread(
        envelope.thread_id.clone(),
        persona.clone(),
        PersonaId::human(),
        format!(
            "Gave up on the message from {} after {} deliveries: {}\n`ants dead replay {dead_seq}` sends it again.",
            envelope.from, dead.deliveries, dead.error
        ),
    );
    notice.in_reply_to = Some(envelope.id);
    notice
}

/// Every dead letter in the stream, oldest first, with its stream sequence. Needs the human's credentials.
pub async fn list(stream: &stream::Stream) -> Result<Vec<(u64, DeadLetter)>> {
    let mut letters = Vec::new();
    let mut from = 1;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(subject::DEAD_ALL, from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => return Ok(letters),
            Err(e) => return Err(e).context("reading dead letters"),
        };
        from = msg.sequence + 1;
        match letter(&msg.subject, &msg.payload) {
            Ok(dead) => letters.push((msg.sequence, dead)),
            Err(e) => tracing::warn!(seq = msg.sequence, error = format!("{e:#}"), "skipping unreadable dead letter"),
        }
    }
}

/// The dead letter at stream sequence `seq`.
pub async fn get(stream: &stream::Stream, seq: u64) -> Result<DeadLetter> {
    let msg = stream.get_raw_message(seq).await.with_context(|| format!("reading message {seq}"))?;
    if !msg.subject.starts_with("ants.dead.") {
        bail!("message {seq} is on {}, not a dead letter", msg.subject);
    }
    letter(&msg.subject, &msg.payload).with_context(|| format!("dead letter {seq}"))
}

/// The dead letter stored on `subject`. Credentials pin the persona of a dead letter subject, so one naming another
/// persona is forged.
fn letter(subject: &str, payload: &[u8]) -> Result<DeadLetter> {
    let dead: DeadLetter = serde_json::from_slice(payload)?;
    if dead.subject() != subject {
        bail!("dead letter of {} arrived on {subject}", dead.persona);
    }
    Ok(dead)
}

/// Send the message of dead letter `seq` to its inbox again, as it was, then remove the dead letter. The human's
/// credentials may send under any sender, so only a message to the persona that buried it is replayed: a persona
/// cannot have the human deliver a message of its own making to another persona.
pub async fn replay(js: &jetstream::Context, stream: &stream::Stream, seq: u64) -> Result<Envelope> {
    let dead = get(stream, seq).await?;
    let Some(envelope) = dead.envelope else {
        bail!("dead letter {seq} holds no envelope to replay; `ants dead drop {seq}` removes it");
    };
    if envelope.to != dead.persona {
        bail!(
            "dead letter {seq} of {} holds a message to {}; `ants dead drop {seq}` removes it",
            dead.persona,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_attempt_and_any_after_it_dead_letter() {
        assert_eq!(Attempt::of(1, 5), Attempt::Early);
        assert_eq!(Attempt::of(4, 5), Attempt::Early);
        assert_eq!(Attempt::of(5, 5), Attempt::Last);
        assert_eq!(Attempt::of(6, 5), Attempt::Overdue);
    }

    #[test]
    fn the_notice_goes_to_the_human_on_the_thread() {
        let persona: PersonaId = "dev-a".parse().unwrap();
        let ask = Envelope::new("dev-b".parse().unwrap(), persona.clone(), "build it");
        let dead = DeadLetter::of_envelope(persona.clone(), 3, 5, "harness crashed", ask.clone());
        let notice = notice(&persona, &ask, &dead, 9);
        assert_eq!((notice.from, notice.to), (persona, PersonaId::human()));
        assert_eq!((notice.thread_id, notice.in_reply_to), (ask.thread_id, Some(ask.id)));
        assert!(notice.text.contains("from dev-b after 5 deliveries: harness crashed"), "{}", notice.text);
        assert!(notice.text.contains("ants dead replay 9"), "{}", notice.text);
    }
}
