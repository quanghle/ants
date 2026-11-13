//! Dead letters: inbox messages and channel posts a persona gave up on, kept on `ants.dead.<persona>` until a human
//! replays or drops them. The thread's owner, else the default human, is told. A channel post is kept as a payload,
//! which is not replayed: a human posts again instead.
//!
//! The daemon writes a dead letter itself, with the message in hand: after its last attempt fails, or on the one
//! extra delivery the consumer allows when the last attempt never finished (the daemon stopped, or the ack wait ran
//! out). Watching JetStream's max-deliveries advisory instead would mean fetching the message by sequence, and
//! `STREAM.MSG.GET` takes any sequence, so a persona allowed it could read every inbox.
//!
//! When storing the dead letter fails on that extra delivery too, JetStream delivers the message no more and only
//! publishes a max-deliveries advisory. As a backstop, the human's side ([`watch`]) turns those advisories into dead
//! letters. Advisories are core NATS: one published while no watcher runs is lost, and its message stays in the
//! stream unnoticed until it expires.

use ants_proto::{ChannelPost, DeadLetter, Envelope, PersonaId, STREAM, chan_consumer, inbox_consumer, subject};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{self, RawMessageErrorKind};
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt as _;
use serde::Deserialize;

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

/// [`bury_to`], with `human` as the default human.
pub async fn bury(
    js: &jetstream::Context,
    persona: &PersonaId,
    msg: &jetstream::Message,
    envelope: Option<&Envelope>,
    error: &str,
) -> Result<u64> {
    bury_to(js, persona, msg, envelope, error, &PersonaId::human()).await
}

/// Store `msg` as a dead letter of `persona`, tell the thread's owner (else `human`, the default human) when it was an
/// envelope, then terminate it. Returns the stream sequence of the dead letter. `envelope` is `None` for a payload that
/// is not an envelope.
pub async fn bury_to(
    js: &jetstream::Context,
    persona: &PersonaId,
    msg: &jetstream::Message,
    envelope: Option<&Envelope>,
    error: &str,
    human: &PersonaId,
) -> Result<u64> {
    let info = msg.info().map_err(|e| anyhow!(e))?;
    let (seq, deliveries) = (info.stream_sequence, info.delivered.max(0) as u64);
    let dead = letter_of(persona, seq, deliveries, error, &msg.payload, envelope);
    let (dead_seq, _) = store(js, &dead, None, human).await?;
    // Only now: a crash before this redelivers the message, and the message ids in `store` keep the copies single.
    msg.ack_with(AckKind::Term).await.map_err(|e| anyhow!(e))?;
    tracing::warn!(stream_seq = seq, dead_seq, deliveries, error, "dead-lettered");
    Ok(dead_seq)
}

/// Store channel post `msg` (`post`, checked against its subject) as a dead letter of `persona`, keeping its payload,
/// tell the owner of the post's thread, else `human`, the default human, on that thread, then terminate it. Returns the stream sequence of the dead
/// letter.
pub async fn bury_post(
    js: &jetstream::Context,
    persona: &PersonaId,
    msg: &jetstream::Message,
    post: &ChannelPost,
    error: &str,
    human: &PersonaId,
) -> Result<u64> {
    let info = msg.info().map_err(|e| anyhow!(e))?;
    let (seq, deliveries) = (info.stream_sequence, info.delivered.max(0) as u64);
    let dead = DeadLetter::of_payload(persona.clone(), seq, deliveries, error, &msg.payload);
    let (dead_seq, _) = store(js, &dead, Some(post), human).await?;
    msg.ack_with(AckKind::Term).await.map_err(|e| anyhow!(e))?;
    tracing::warn!(stream_seq = seq, dead_seq, deliveries, error, "dead-lettered a channel post");
    Ok(dead_seq)
}

fn letter_of(
    persona: &PersonaId,
    seq: u64,
    deliveries: u64,
    error: &str,
    payload: &[u8],
    envelope: Option<&Envelope>,
) -> DeadLetter {
    match envelope {
        Some(e) => DeadLetter::of_envelope(persona.clone(), seq, deliveries, error, e.clone()),
        None => DeadLetter::of_payload(persona.clone(), seq, deliveries, error, payload),
    }
}

/// Store `dead`, then tell the thread's owner, else `human`, the default human, when it holds an envelope or is of
/// channel post `post`. Returns the stream sequence of the dead letter, and whether it was stored already: the message
/// ids make a second store within the duplicate window store nothing.
async fn store(
    js: &jetstream::Context,
    dead: &DeadLetter,
    post: Option<&ChannelPost>,
    human: &PersonaId,
) -> Result<(u64, bool)> {
    let message = PublishMessage::build().payload(serde_json::to_vec(dead)?.into()).message_id(dead.msg_id());
    let ack = js
        .send_publish(dead.subject(), message)
        .await
        .context("publishing")?
        .await
        .context("waiting for JetStream to store the message")?;
    if let Some(e) = &dead.envelope {
        let notice = notice(&dead.persona, e, dead, ack.sequence, human);
        let id = format!("dead-notice-{}", dead.stream_seq);
        bus::publish_json(js, notice.subject(), &notice, Some(&id)).await?;
    } else if let Some(post) = post {
        let notice = post_notice(&dead.persona, post, dead, human);
        let id = format!("dead-notice-{}", dead.stream_seq);
        bus::publish_json(js, notice.subject(), &notice, Some(&id)).await?;
    }
    Ok((ack.sequence, ack.duplicate))
}

/// What a max-deliveries advisory of JetStream says (`io.nats.jetstream.advisory.v1.max_deliver`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MaxDeliveries {
    pub stream: String,
    pub consumer: String,
    pub stream_seq: u64,
    pub deliveries: u64,
}

/// The consumers of a persona that dead-letter what they give up on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Consumer {
    Inbox,
    Channels,
}

impl MaxDeliveries {
    /// The persona whose inbox or channel consumer gave up, and which one, or `None` for any other consumer.
    fn persona(&self) -> Option<(PersonaId, Consumer)> {
        if self.stream != STREAM {
            return None;
        }
        let (kind, name) = match (self.consumer.strip_prefix("inbox-"), self.consumer.strip_prefix("chan-")) {
            (Some(name), _) => (Consumer::Inbox, name),
            (_, Some(name)) => (Consumer::Channels, name),
            _ => return None,
        };
        let persona: PersonaId = name.parse().ok()?;
        let own = match kind {
            Consumer::Inbox => inbox_consumer(&persona),
            Consumer::Channels => chan_consumer(&persona),
        };
        (!persona.is_human() && own == self.consumer).then_some((persona, kind))
    }
}

/// Hear the max-deliveries advisories of the stream's consumers and dead-letter each message a persona's inbox or
/// channel consumer stopped delivering, as its daemon would have, handing every new dead letter and its sequence to `buried`.
/// Runs until the connection closes. Needs the human's credentials.
pub async fn watch(
    client: &async_nats::Client,
    js: &jetstream::Context,
    mut buried: impl FnMut(u64, &DeadLetter),
) -> Result<()> {
    let mut advisories = client
        .subscribe(subject::max_deliveries_advisories())
        .await
        .context("subscribing to max-deliveries advisories")?;
    let stream = js.get_stream(STREAM).await.context("opening the ANTS stream")?;
    while let Some(msg) = advisories.next().await {
        let advisory = match serde_json::from_slice::<MaxDeliveries>(&msg.payload) {
            Ok(advisory) => advisory,
            Err(e) => {
                tracing::warn!(subject = %msg.subject, error = %e, "skipping an unreadable advisory");
                continue;
            }
        };
        match bury_undelivered(js, &stream, &advisory).await {
            Ok(Some((seq, dead))) => buried(seq, &dead),
            Ok(None) => {}
            Err(e) => tracing::warn!(stream_seq = advisory.stream_seq, error = format!("{e:#}"), "dead-lettering"),
        }
    }
    Ok(())
}

/// Dead-letter the message `advisory` names, unless it is not a persona's inbox message or channel post, is gone, or
/// already is a dead letter. Returns the new dead letter and its stream sequence.
pub async fn bury_undelivered(
    js: &jetstream::Context,
    stream: &stream::Stream,
    advisory: &MaxDeliveries,
) -> Result<Option<(u64, DeadLetter)>> {
    let Some((persona, consumer)) = advisory.persona() else {
        tracing::debug!(consumer = advisory.consumer, "ignoring an advisory of a consumer that is no persona's inbox");
        return Ok(None);
    };
    let seq = advisory.stream_seq;
    if let Some(dead_seq) = find(stream, &persona, seq).await? {
        tracing::debug!(stream_seq = seq, dead_seq, "already a dead letter");
        return Ok(None);
    }
    let msg = match stream.get_raw_message(seq).await {
        Ok(msg) => msg,
        Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => {
            tracing::warn!(stream_seq = seq, %persona, "the message the daemon gave up on is gone");
            return Ok(None);
        }
        Err(e) => return Err(e).with_context(|| format!("reading message {seq}")),
    };
    let error = format!(
        "no outcome after {} deliveries, and the daemon could not dead-letter it; kept from JetStream's \
         max-deliveries advisory",
        advisory.deliveries
    );
    let (dead, post) = match consumer {
        Consumer::Inbox => {
            if subject::parse_inbox(&msg.subject).is_none_or(|(to, _)| to != persona) {
                bail!("message {seq} is on {}, not in the inbox of {persona}", msg.subject);
            }
            // As the daemon does: an envelope whose sender is forged is kept only as a payload.
            let envelope =
                serde_json::from_slice::<Envelope>(&msg.payload).ok().filter(|e| e.check_subject(&msg.subject).is_ok());
            (letter_of(&persona, seq, advisory.deliveries, &error, &msg.payload, envelope.as_ref()), None)
        }
        Consumer::Channels => {
            if subject::parse_chan(&msg.subject).is_none() {
                bail!("message {seq} is on {}, not a channel post", msg.subject);
            }
            let post = serde_json::from_slice::<ChannelPost>(&msg.payload)
                .ok()
                .filter(|p| p.check_subject(&msg.subject).is_ok());
            (DeadLetter::of_payload(persona.clone(), seq, advisory.deliveries, &error, &msg.payload), post)
        }
    };
    let (dead_seq, duplicate) = store(js, &dead, post.as_ref(), &PersonaId::human()).await?;
    if duplicate {
        return Ok(None);
    }
    tracing::warn!(stream_seq = seq, dead_seq, %persona, "dead-lettered from a max-deliveries advisory");
    Ok(Some((dead_seq, dead)))
}

/// The sequence of the dead letter of `persona` kept for stream sequence `seq`, if there is one.
async fn find(stream: &stream::Stream, persona: &PersonaId, seq: u64) -> Result<Option<u64>> {
    let subject = subject::dead(persona);
    let mut from = 1;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(&subject, from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => return Ok(None),
            Err(e) => return Err(e).context("reading dead letters"),
        };
        from = msg.sequence + 1;
        if letter(&msg.subject, &msg.payload).is_ok_and(|dead| dead.stream_seq == seq) {
            return Ok(Some(msg.sequence));
        }
    }
}

/// The message on the thread that tells its owner, else `human`, the default human, that a message died, so
/// `ants ask --wait` returns too.
fn notice(persona: &PersonaId, envelope: &Envelope, dead: &DeadLetter, dead_seq: u64, human: &PersonaId) -> Envelope {
    let mut notice = envelope.follow_up(
        persona.clone(),
        envelope.owner.clone().unwrap_or_else(|| human.clone()),
        format!(
            "Gave up on the message from {} after {} deliveries: {}\n`ants dead replay {dead_seq}` sends it again.",
            envelope.from, dead.deliveries, dead.error
        ),
    );
    notice.in_reply_to = Some(envelope.id);
    notice
}

/// The message on a channel post's thread that tells the thread's owner, else `human`, the default human, the persona
/// gave up on the post.
fn post_notice(persona: &PersonaId, post: &ChannelPost, dead: &DeadLetter, human: &PersonaId) -> Envelope {
    Envelope::on_thread(
        post.thread_id.clone(),
        persona.clone(),
        post.owner.clone().unwrap_or_else(|| human.clone()),
        format!(
            "Gave up on the post from {} in #{} after {} deliveries: {}\nPosting on the thread again retries it.",
            post.from, post.channel, dead.deliveries, dead.error
        ),
    )
    .with_owner(post.owner.clone())
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
    fn only_advisories_of_persona_inbox_and_channel_consumers_name_a_persona() {
        let json = r#"{"type":"io.nats.jetstream.advisory.v1.max_deliver","id":"x","timestamp":"2026-11-13T00:00:00Z",
            "stream":"ANTS","consumer":"inbox-dev-a","stream_seq":7,"deliveries":6}"#;
        let advisory: MaxDeliveries = serde_json::from_str(json).unwrap();
        assert_eq!((advisory.stream_seq, advisory.deliveries), (7, 6));
        assert_eq!(advisory.persona(), Some(("dev-a".parse().unwrap(), Consumer::Inbox)));
        let chan = MaxDeliveries { consumer: "chan-dev-a".into(), ..advisory.clone() };
        assert_eq!(chan.persona(), Some(("dev-a".parse().unwrap(), Consumer::Channels)));
        for (stream, consumer) in
            [("ANTS", "inbox-human"), ("ANTS", "plan-dev-a"), ("ANTS", "chanread-dev-a"), ("OTHER", "inbox-dev-a")]
        {
            let other = MaxDeliveries { stream: stream.into(), consumer: consumer.into(), ..advisory.clone() };
            assert_eq!(other.persona(), None, "{stream} {consumer}");
        }
    }

    #[test]
    fn the_notice_goes_to_the_owner_else_the_default_human_on_the_thread() {
        let persona: PersonaId = "dev-a".parse().unwrap();
        let alice: PersonaId = "alice".parse().unwrap();
        let ask = Envelope::new("dev-b".parse().unwrap(), persona.clone(), "build it");
        let dead = DeadLetter::of_envelope(persona.clone(), 3, 5, "harness crashed", ask.clone());
        let told = notice(&persona, &ask, &dead, 9, &alice);
        assert_eq!((&told.from, &told.to), (&persona, &alice));
        assert_eq!((told.thread_id, told.in_reply_to), (ask.thread_id.clone(), Some(ask.id)));
        assert!(told.text.contains("from dev-b after 5 deliveries: harness crashed"), "{}", told.text);
        assert!(told.text.contains("ants dead replay 9"), "{}", told.text);
        let owned = ask.with_owner(Some("bob".parse().unwrap()));
        let told = notice(&persona, &owned, &dead, 9, &alice);
        assert_eq!((told.to.as_str(), told.owner), ("bob", owned.owner));
    }

    #[test]
    fn the_notice_of_a_channel_post_goes_to_the_thread_s_owner_else_the_default_human() {
        let (persona, human) = ("dev-a".parse::<PersonaId>().unwrap(), PersonaId::human());
        let post = ChannelPost::new("ops".parse().unwrap(), "dev-b".parse().unwrap(), "@dev-a check");
        let dead = DeadLetter::of_payload(persona.clone(), 3, 5, "harness crashed", b"{}");
        let told = post_notice(&persona, &post, &dead, &human);
        assert_eq!((&told.to, &told.thread_id, &told.owner), (&human, &post.thread_id, &None));
        let owned = post.with_owner(Some("alice".parse().unwrap()));
        let told = post_notice(&persona, &owned, &dead, &human);
        assert_eq!((told.to.as_str(), told.owner), ("alice", owned.owner));
    }
}
