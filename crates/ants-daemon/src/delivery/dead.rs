//! Dead letters: inbox messages and channel posts an ant gave up on, kept on `ants.dead.<ant>` until a human
//! replays or drops them. The ant's daemon escalates them up its chain, in the end to the thread's owner, else the
//! default human ([`crate::escalation`]); the human's side tells the owner directly, with a level-0 `dead_letter`
//! escalation so push sinks and `ants thread list --escalated` see it. A channel post is kept as a payload,
//! which is not replayed: a human posts again instead.
//!
//! The human's side skips the chain on purpose: it only stores what the ant's daemon failed to store, so that daemon
//! is in trouble, which no chain member can fix, and human credentials can neither read nor write the escalation the
//! ant keeps in `ants_sessions` (level, visited ants, renewal, deadline, reminder) that a step up the chain must
//! follow, so no reminder follows its notice either.
//!
//! The daemon writes a dead letter itself, with the message in hand: after its last attempt fails, or on the one
//! extra delivery the consumer allows when the last attempt never finished (the daemon stopped, or the ack wait ran
//! out). Watching JetStream's max-deliveries advisory instead would mean fetching the message by sequence, and
//! `STREAM.MSG.GET` takes any sequence, so an ant allowed it could read every inbox.
//!
//! While a leaf node's uplink is down, the daemon's dead letter goes to the machine's outbox, which the hub's stream
//! takes it from once the uplink is back ([`Burial::Outbox`]). Its notice, which may climb the escalation chain kept in
//! `ants_sessions`, and the message's termination need the hub: they wait for the message's redelivery, which buries the
//! same dead letter, noted on the machine ([`Buried`]), again. On the consumer's extra delivery no redelivery is left:
//! the daemon notes that delivery's ack subject too, and once the hub is in reach, stores the letter there, tells the
//! thread's owner directly and terminates the message itself ([`finish`]). The human's side writes dead letters only
//! from what the hub holds, so straight into its stream.
//!
//! The hub's stream keeps a second copy of a dead letter out by its message id only within its duplicate window (a
//! day, [`crate::bus::DUPLICATE_WINDOW`]). A daemon that finishes a burial later, having been down, first reads its
//! own dead letters back from the hub ([`stored`]) and keeps a letter of the same message found there instead of
//! storing another.
//!
//! When storing the dead letter fails on that extra delivery too, JetStream delivers the message no more and only
//! publishes a max-deliveries advisory. As a backstop, the human's side ([`watch`]) turns those advisories into dead
//! letters. Advisories are core NATS: one published while no watcher runs is lost, and its message stays in the
//! stream unnoticed until it expires.

use crate::bus::relayed::{self, Read};
use std::path::Path;
use std::time::Duration;

use ants_proto::{
    ChannelPost, DeadLetter, Envelope, Escalation, EscalationReason, SESSIONS_BUCKET, STREAM, ThreadOrigin, UserId,
    Uuid, chan_consumer, dead_reader, inbox_consumer, subject,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::HeaderMap;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{self, RawMessageErrorKind};
use async_nats::jetstream::{self, AckKind, consumer};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};

use crate::bus;
use crate::channel::{Reading, Threads};
use crate::daemon::Claims;
use crate::delivery::attempts::Notes;
use crate::escalation::{Escalator, Stuck};
use crate::people::humans::Registry;
use crate::records::direct_kv::DirectKv;

/// How long a dead letter reader left behind by an interrupted read lives on.
const INACTIVE: Duration = Duration::from_secs(30);
/// Dead letters asked for at once.
const BATCH: u64 = 256;
/// One read of dead letters at a time: an ant's reads share its reader.
static READING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

/// What became of a dead letter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Burial {
    /// Stored in the hub's stream at this sequence, its notice sent, and the message terminated.
    Hub(u64),
    /// Kept in the outbox of the machine's leaf node, whose uplink is down; the hub's stream takes it from there. The
    /// notice and the message's termination need the hub: the message stays unacked and comes back once the uplink is.
    Outbox,
}

/// A dead letter kept in the machine's outbox, as [`Buried`] notes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kept {
    #[serde(flatten)]
    pub dead: DeadLetter,
    /// The channel post it is of, as answered, with its thread's owner: the notice goes there.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub post: Option<ChannelPost>,
    /// The ack subject of the delivery that kept it, when the consumer delivers the message no more after that one:
    /// [`finish`] then ends the burial.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_delivery: Option<String>,
}

/// Dead letters a daemon kept in the machine's outbox ([`Burial::Outbox`]) for messages it could not terminate: one
/// file per message, named after its envelope id, holding the dead letter ([`Kept`]), in `<home>/buried` of the ant.
/// The redelivery after the uplink is back, even to a restarted daemon, buries that same dead letter again, with the
/// error of the attempt that failed, without another prompt. One kept on the message's last delivery has no redelivery
/// to wait for: the daemon finishes its burial itself once the hub is in reach ([`finish`]).
pub struct Buried(Notes);

impl Buried {
    pub fn new(home: &Path) -> Self {
        Self(Notes::new(home, "buried"))
    }

    /// The dead letter kept for `message`, if any.
    pub async fn get(&self, message: &Envelope) -> Option<Kept> {
        self.0.get(message).await
    }

    /// Note that `kept`, the dead letter of `message`, waits in the machine's outbox.
    pub async fn add(&self, message: &Envelope, kept: &Kept) -> Result<()> {
        self.0.add(message, kept).await.context("noting the dead letter kept in the outbox")
    }

    /// The dead letters kept on their message's last delivery, with the envelope id of the message.
    pub async fn undelivered(&self) -> Vec<(Uuid, Kept)> {
        let mut kept: Vec<(Uuid, Kept)> = self.0.all().await;
        kept.retain(|(_, kept)| kept.last_delivery.is_some());
        kept
    }

    /// Forget `message` once its dead letter is in the hub's stream and the message terminated.
    pub async fn forget(&self, message: &Envelope) {
        self.0.forget(message).await;
    }

    /// [`Buried::forget`], by the message's envelope id.
    pub async fn forget_id(&self, message: Uuid) {
        self.0.forget_id(message).await;
    }

    /// Remove notes older than `max_age`, by when the stream no longer keeps their messages.
    pub async fn prune(&self, max_age: Duration) {
        self.0.prune(max_age).await;
    }
}

/// [`bury_to`], with `human` as the only human.
pub async fn bury(
    js: &jetstream::Context,
    ant: &UserId,
    msg: &jetstream::Message,
    envelope: Option<&Envelope>,
    error: &str,
) -> Result<Burial> {
    bury_to(js, ant, msg, envelope, error, &Registry::default(), None).await
}

/// Store `msg` as a dead letter of `ant` ([`letter_for`]) and finish its burial ([`bury_letter`]). `envelope` is `None`
/// for a payload that is not an envelope.
pub async fn bury_to(
    js: &jetstream::Context,
    ant: &UserId,
    msg: &jetstream::Message,
    envelope: Option<&Envelope>,
    error: &str,
    humans: &Registry,
    escalator: Option<&Escalator>,
) -> Result<Burial> {
    let dead = letter_for(ant, msg, envelope, error)?;
    bury_letter(js, msg, &dead, None, humans, escalator).await
}

/// The dead letter of `ant` for `msg`, failed with `error`: of `envelope`, or of the message's payload when there is
/// none (not an envelope, or a channel post).
pub fn letter_for(
    ant: &UserId,
    msg: &jetstream::Message,
    envelope: Option<&Envelope>,
    error: &str,
) -> Result<DeadLetter> {
    let info = msg.info().map_err(|e| anyhow!(e))?;
    let (seq, deliveries) = (info.stream_sequence, info.delivered.max(0) as u64);
    Ok(letter_of(ant, seq, deliveries, error, &msg.payload, envelope))
}

/// Store `dead`, the dead letter of `msg`, in the hub's stream, tell the thread's owner (else the default human of
/// `humans`) when it holds an envelope or is of channel post `post` (checked against its subject), through `escalator`
/// when there is one, then terminate `msg`. Out of the hub's reach, as while a leaf node's uplink is down, the dead
/// letter goes to the machine's outbox instead ([`bus::store`]) and the rest waits for the redelivery
/// ([`Burial::Outbox`]); the dead letter's message id keeps one copy in the hub's stream.
pub async fn bury_letter(
    js: &jetstream::Context,
    msg: &jetstream::Message,
    dead: &DeadLetter,
    post: Option<&ChannelPost>,
    humans: &Registry,
    escalator: Option<&Escalator>,
) -> Result<Burial> {
    let Some(ack) = bus::store_acked(js, dead.subject(), dead, &dead.msg_id()).await? else {
        tracing::warn!(stream_seq = dead.stream_seq, error = dead.error, "dead letter kept in this machine's outbox");
        return Ok(Burial::Outbox);
    };
    bury_stored(js, msg, dead, ack.sequence, post, humans, escalator).await
}

/// [`bury_letter`] for `dead`, which an earlier delivery of `msg` kept in the machine's outbox ([`Buried`]): a letter of
/// the message the hub's stream holds already ([`stored`]), as the outbox copy landed long before, is the one the
/// notice names, and no other is stored.
pub async fn bury_kept(
    js: &jetstream::Context,
    msg: &jetstream::Message,
    dead: &DeadLetter,
    post: Option<&ChannelPost>,
    humans: &Registry,
    escalator: Option<&Escalator>,
) -> Result<Burial> {
    match stored(js, dead).await {
        Ok(letters) => {
            if let Some(&(dead_seq, _)) = letters.first() {
                return bury_stored(js, msg, dead, dead_seq, post, humans, escalator).await;
            }
        }
        Err(e) => tracing::warn!(stream_seq = dead.stream_seq, error = format!("{e:#}"), "reading dead letters back"),
    }
    bury_letter(js, msg, dead, post, humans, escalator).await
}

/// Tell about `dead`, the dead letter of `msg` stored at `dead_seq` in the hub's stream, as [`bury_letter`] does, then
/// terminate `msg`.
async fn bury_stored(
    js: &jetstream::Context,
    msg: &jetstream::Message,
    dead: &DeadLetter,
    dead_seq: u64,
    post: Option<&ChannelPost>,
    humans: &Registry,
    escalator: Option<&Escalator>,
) -> Result<Burial> {
    tell(js, dead, dead_seq, post, humans, escalator).await?;
    // Only now: a crash before this redelivers the message, and the message ids keep the copies single.
    msg.ack_with(AckKind::Term).await.map_err(|e| anyhow!(e))?;
    tracing::warn!(
        stream_seq = dead.stream_seq,
        dead_seq,
        deliveries = dead.deliveries,
        error = dead.error,
        "dead-lettered"
    );
    Ok(Burial::Hub(dead_seq))
}

/// Finish the burial of `kept`, a dead letter kept in the machine's outbox on its message's last delivery, once the hub
/// is in reach: store it in the hub's stream (never the outbox again), tell the thread's owner, else the default human
/// of `humans`, then terminate the message. The notice goes there directly, as [`watch`] sends it, under the same
/// message id: the consumer gives up on the message, so a running watcher may bury it too, and whichever notice comes
/// first is the only one. A letter of the message the hub's stream holds already ([`stored`]), the outbox copy or the
/// watcher's, is kept instead of storing another, and the watcher's was told already. Returns the dead letter's stream
/// sequence, or `None` while the hub is out of reach.
pub async fn finish(js: &jetstream::Context, kept: &Kept, humans: &Registry) -> Result<Option<u64>> {
    let Kept { dead, post, last_delivery } = kept;
    let letters = stored(js, dead).await.unwrap_or_else(|e| {
        tracing::debug!(stream_seq = dead.stream_seq, error = format!("{e:#}"), "reading dead letters back");
        Vec::new()
    });
    let dead_seq = match letters.first() {
        Some(&(dead_seq, _)) => dead_seq,
        None => match bus::store_in_hub(js, dead.subject(), dead, &dead.msg_id()).await? {
            Some(ack) => ack.sequence,
            None => return Ok(None),
        },
    };
    // Any letter but this one is the watcher's, which told when it stored it.
    if letters.iter().all(|(_, letter)| letter == dead) {
        tell(js, dead, dead_seq, post.as_ref(), humans, None).await?;
    }
    if let Some(reply) = last_delivery {
        let client = js.client();
        client.publish(reply.clone(), AckKind::Term.into()).await.context("terminating the message")?;
        client.flush().await.context("terminating the message")?;
    }
    tracing::warn!(
        stream_seq = dead.stream_seq,
        dead_seq,
        error = dead.error,
        "dead-lettered from this machine's outbox"
    );
    Ok(Some(dead_seq))
}

/// The letters of the message `dead` is of that the hub's stream holds, oldest first, with their sequences: read back
/// from the dead letters of its ant stored after the message, through the ant's own consumer [`dead_reader`], created
/// for the read and deleted after it.
pub async fn stored(js: &jetstream::Context, dead: &DeadLetter) -> Result<Vec<(u64, DeadLetter)>> {
    let _one = READING.lock().await;
    let name = dead_reader(&dead.ant);
    // A consumer left by an interrupted read would carry on where it stopped; as a rule none is left to delete.
    let _ = js.delete_consumer_from_stream(&name, STREAM).await;
    let config = consumer::pull::Config {
        name: Some(name.clone()),
        filter_subject: dead.subject(),
        deliver_policy: consumer::DeliverPolicy::ByStartSequence { start_sequence: dead.stream_seq + 1 },
        ack_policy: consumer::AckPolicy::None,
        inactive_threshold: INACTIVE,
        // In memory, so it never starts from the state the reader deleted before it left on disk (as a reading
        // reader's, `crate::stream::reading`).
        memory_storage: true,
        ..Default::default()
    };
    let reader: consumer::PullConsumer =
        js.create_consumer_on_stream(config, STREAM).await.context("reading dead letters back")?;
    let letters = read_back(&reader, dead.stream_seq).await;
    if let Err(e) = js.delete_consumer_from_stream(&name, STREAM).await {
        tracing::debug!(error = %e, consumer = name, "deleting the dead letter reader");
    }
    letters
}

async fn read_back(reader: &consumer::PullConsumer, seq: u64) -> Result<Vec<(u64, DeadLetter)>> {
    let mut letters = Vec::new();
    let mut left = reader.cached_info().num_pending;
    while left > 0 {
        let mut batch = reader.fetch().max_messages(left.min(BATCH) as usize).messages().await?;
        let mut got = 0;
        while let Some(msg) = batch.next().await {
            let msg = msg.map_err(|e| anyhow!(e))?;
            got += 1;
            let Ok(info) = msg.info() else { continue };
            if let Some(dead) =
                letter(&msg.subject, msg.headers.as_ref(), &msg.payload).ok().filter(|dead| dead.stream_seq == seq)
            {
                letters.push((info.stream_sequence, dead));
            }
        }
        if got == 0 {
            break;
        }
        left = left.saturating_sub(got);
    }
    Ok(letters)
}

fn letter_of(
    ant: &UserId,
    seq: u64,
    deliveries: u64,
    error: &str,
    payload: &[u8],
    envelope: Option<&Envelope>,
) -> DeadLetter {
    match envelope {
        Some(e) => DeadLetter::of_envelope(ant.clone(), seq, deliveries, error, e.clone()),
        None => DeadLetter::of_payload(ant.clone(), seq, deliveries, error, payload),
    }
}

/// Tell the thread's owner, else the default human of `humans`, that `dead`, stored at stream sequence `dead_seq`,
/// died, when it holds an envelope or is of channel post `post`: up the ant's chain first when there is an
/// `escalator`. The notice's message id keeps a second one out of the stream.
async fn tell(
    js: &jetstream::Context,
    dead: &DeadLetter,
    dead_seq: u64,
    post: Option<&ChannelPost>,
    humans: &Registry,
    escalator: Option<&Escalator>,
) -> Result<()> {
    let (notice, about) = match (&dead.envelope, post) {
        (Some(e), _) => (notice(&dead.ant, e, dead, dead_seq, humans), Some(e)),
        (None, Some(post)) => (post_notice(&dead.ant, post, dead, humans), None),
        (None, None) => return Ok(()),
    };
    let id = format!("dead-notice-{}", dead.stream_seq);
    match escalator {
        Some(escalator) => {
            let stuck = Stuck::new(EscalationReason::DeadLetter, notice, id, about);
            escalator.escalate(Stuck { channel: post.map(|p| p.channel.clone()), ..stuck }).await?;
        }
        None => {
            let notice = direct(notice, humans);
            bus::publish_json(js, notice.subject(), &notice, Some(&id)).await?;
        }
    }
    Ok(())
}

/// `notice` as it goes to a human directly, past the ant's chain: carrying the level-0 `dead_letter` escalation the
/// ant's daemon would start from ([`Escalation::new_from`]), so `ants webhook serve` pushes it and `ants thread list
/// --escalated` lists it. It is the same for every burial of the message, so the notices of the ant's daemon
/// ([`finish`]) and of [`watch`] stay one under their shared message id. No reminder follows: those are kept in the
/// ant's escalation ledger, which the human's side cannot write.
fn direct(notice: Envelope, humans: &Registry) -> Envelope {
    let origin = humans.ant_creator(notice.creator.as_ref());
    let escalation = Escalation::new_from(
        notice.from.clone(),
        notice.thread_id.clone(),
        EscalationReason::DeadLetter,
        origin.as_ref(),
    );
    notice.with_escalation(Some(escalation))
}

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
        (!user.is_human() && own == self.consumer).then_some((user, kind))
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
    let humans = Registry::default();
    let _watching = humans.watch(js.clone(), crate::people::humans::RETRY);
    // The human's side neither reads nor writes sessions, which its credentials do not allow.
    let sessions = DirectKv::new(js, None, SESSIONS_BUCKET).await?;
    let threads = Threads::new(js.clone(), reader.clone(), sessions).reading(Reading::Gets);
    let claims = Claims { humans: &humans, threads: &threads };
    while let Some(msg) = advisories.next().await {
        let advisory = match serde_json::from_slice::<MaxDeliveries>(&msg.payload) {
            Ok(advisory) => advisory,
            Err(e) => {
                tracing::warn!(subject = %msg.subject, error = %e, "skipping an unreadable advisory");
                continue;
            }
        };
        match bury_undelivered(js, &stream, &advisory, &claims).await {
            Ok(Some((seq, dead))) => {
                crate::monitoring::metrics::ant(&dead.ant).buried();
                buried(seq, &dead);
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(stream_seq = advisory.stream_seq, error = format!("{e:#}"), "dead-lettering"),
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
    let Some((ant, consumer)) = advisory.ant() else {
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
            let relayed = relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer);
            let mut envelope = serde_json::from_slice::<Envelope>(&msg.payload)
                .ok()
                .filter(|e| !relayed && e.check_subject(&msg.subject).is_ok());
            if let Some(envelope) = envelope.as_mut() {
                claims.sender_claims(envelope, seq).await;
            }
            (letter_of(&ant, seq, advisory.deliveries, &error, &msg.payload, envelope.as_ref()), None)
        }
        Consumer::Channels => {
            if subject::parse_chan(&msg.subject).is_none() {
                bail!("message {seq} is on {}, not a channel post", msg.subject);
            }
            let relayed = relayed::stored_relayed(Some(&msg.headers), &msg.payload, Read::Consumer);
            let mut post = serde_json::from_slice::<ChannelPost>(&msg.payload)
                .ok()
                .filter(|p| !relayed && p.check_subject(&msg.subject).is_ok());
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
    tell(js, &dead, ack.sequence, post.as_ref(), claims.humans, None).await?;
    if ack.duplicate {
        return Ok(None);
    }
    tracing::warn!(stream_seq = seq, dead_seq = ack.sequence, %ant, "dead-lettered from a max-deliveries advisory");
    Ok(Some((ack.sequence, dead)))
}

/// The sequence of the dead letter of `ant` kept for stream sequence `seq`, if there is one.
async fn find(stream: &stream::Stream, ant: &UserId, seq: u64) -> Result<Option<u64>> {
    let subject = subject::dead(ant);
    let mut from = 1;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(&subject, from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => return Ok(None),
            Err(e) => return Err(e).context("reading dead letters"),
        };
        from = msg.sequence + 1;
        if letter(&msg.subject, Some(&msg.headers), &msg.payload).is_ok_and(|dead| dead.stream_seq == seq) {
            return Ok(Some(msg.sequence));
        }
    }
}

/// The message on the thread that tells its owner, else the default human of `humans`, that a message died, so
/// `ants ask --wait` returns too. An owner that is no human is ignored ([`Registry::human_owner`]).
fn notice(ant: &UserId, envelope: &Envelope, dead: &DeadLetter, dead_seq: u64, humans: &Registry) -> Envelope {
    let owner = humans.human_owner(envelope.owner.as_ref());
    let mut notice = Envelope::on_thread(
        envelope.thread_id.clone(),
        ant.clone(),
        owner.clone().unwrap_or_else(|| humans.default_human().clone()),
        format!(
            "Gave up on the message from {} after {} deliveries: {}\n`ants dead replay {dead_seq}` sends it again.",
            envelope.from, dead.deliveries, dead.error
        ),
    )
    .with_owner(owner)
    .with_creator(humans.ant_creator(envelope.creator.as_ref()))
    .with_origin_thread(envelope.origin_thread.clone())
    .as_notice(ants_proto::NoticeKind::GaveUp);
    notice.in_reply_to = Some(envelope.id);
    notice
}

/// The message on a channel post's thread that tells the thread's owner, else the default human of `humans`, the
/// ant gave up on the post.
fn post_notice(ant: &UserId, post: &ChannelPost, dead: &DeadLetter, humans: &Registry) -> Envelope {
    let owner = humans.human_owner(post.owner.as_ref());
    Envelope::on_thread(
        post.thread_id.clone(),
        ant.clone(),
        owner.clone().unwrap_or_else(|| humans.default_human().clone()),
        format!(
            "Gave up on the post from {} in #{} after {} deliveries: {}\nPosting on the thread again retries it.",
            post.from, post.channel, dead.deliveries, dead.error
        ),
    )
    .with_owner(owner)
    .with_creator(post.thread_creator(&humans.humans()))
    .as_notice(ants_proto::NoticeKind::GaveUp)
}

/// Every dead letter in the stream, oldest first, with its stream sequence. Needs the human's credentials.
pub async fn list<I>(stream: &stream::Stream<I>) -> Result<Vec<(u64, DeadLetter)>> {
    let mut letters = Vec::new();
    let mut from = 1;
    loop {
        let msg = match stream.get_first_raw_message_by_subject(subject::DEAD_ALL, from).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == RawMessageErrorKind::NoMessageFound => return Ok(letters),
            Err(e) => return Err(e).context("reading dead letters"),
        };
        from = msg.sequence + 1;
        match letter(&msg.subject, Some(&msg.headers), &msg.payload) {
            Ok(dead) => letters.push((msg.sequence, dead)),
            Err(e) => tracing::warn!(seq = msg.sequence, error = format!("{e:#}"), "skipping unreadable dead letter"),
        }
    }
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
fn letter(subject: &str, headers: Option<&HeaderMap>, payload: &[u8]) -> Result<DeadLetter> {
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
    fn only_advisories_of_ant_inbox_and_channel_consumers_name_an_ant() {
        let json = r#"{"type":"io.nats.jetstream.advisory.v1.max_deliver","id":"x","timestamp":"2026-11-13T00:00:00Z",
            "stream":"ANTS","consumer":"inbox-dev-a","stream_seq":7,"deliveries":6}"#;
        let advisory: MaxDeliveries = serde_json::from_str(json).unwrap();
        assert_eq!((advisory.stream_seq, advisory.deliveries), (7, 6));
        assert_eq!(advisory.ant(), Some(("dev-a".parse().unwrap(), Consumer::Inbox)));
        let chan = MaxDeliveries { consumer: "chan-dev-a".into(), ..advisory.clone() };
        assert_eq!(chan.ant(), Some(("dev-a".parse().unwrap(), Consumer::Channels)));
        for (stream, consumer) in
            [("ANTS", "inbox-human"), ("ANTS", "notes-dev-a"), ("ANTS", "chanread-dev-a"), ("OTHER", "inbox-dev-a")]
        {
            let other = MaxDeliveries { stream: stream.into(), consumer: consumer.into(), ..advisory.clone() };
            assert_eq!(other.ant(), None, "{stream} {consumer}");
        }
    }

    #[test]
    fn the_notice_goes_to_the_owner_else_the_default_human_on_the_thread() {
        let ant: UserId = "dev-a".parse().unwrap();
        let alice: UserId = "alice".parse().unwrap();
        let humans = Registry::with(alice.clone(), &["bob"]);
        let ask = Envelope::new("dev-b".parse().unwrap(), ant.clone(), "build it");
        let dead = DeadLetter::of_envelope(ant.clone(), 3, 5, "harness crashed", ask.clone());
        let told = notice(&ant, &ask, &dead, 9, &humans);
        assert_eq!((&told.from, &told.to), (&ant, &alice));
        assert_eq!((told.thread_id, told.in_reply_to), (ask.thread_id.clone(), Some(ask.id)));
        assert!(told.text.contains("from dev-b after 5 deliveries: harness crashed"), "{}", told.text);
        assert!(told.text.contains("ants dead replay 9"), "{}", told.text);
        let owned = ask.clone().with_owner(Some("bob".parse().unwrap()));
        let told = notice(&ant, &owned, &dead, 9, &humans);
        assert_eq!((told.to.as_str(), told.owner), ("bob", owned.owner));
        let by_ant = ask.with_owner(Some("dev-c".parse().unwrap()));
        let told = notice(&ant, &by_ant, &dead, 9, &humans);
        assert_eq!((&told.to, told.owner), (&alice, None), "an ant owns no thread");
    }

    #[test]
    fn the_notice_of_a_channel_post_goes_to_the_thread_s_owner_else_the_default_human() {
        let (ant, human) = ("dev-a".parse::<UserId>().unwrap(), UserId::human());
        let humans = Registry::with(human.clone(), &["alice"]);
        let post = ChannelPost::new("ops".parse().unwrap(), "dev-b".parse().unwrap(), "@dev-a check");
        let dead = DeadLetter::of_payload(ant.clone(), 3, 5, "harness crashed", b"{}");
        let told = post_notice(&ant, &post, &dead, &humans);
        assert_eq!((&told.to, &told.thread_id, &told.owner), (&human, &post.thread_id, &None));
        let owned = post.clone().with_owner(Some("alice".parse().unwrap()));
        let told = post_notice(&ant, &owned, &dead, &humans);
        assert_eq!((told.to.as_str(), told.owner), ("alice", owned.owner));
        let by_ant = post.with_owner(Some("dev-c".parse().unwrap()));
        assert_eq!(post_notice(&ant, &by_ant, &dead, &humans).to, human, "an ant owns no thread");
    }

    #[test]
    fn a_notice_sent_directly_carries_a_level_0_dead_letter_escalation_from_the_thread_s_origin() {
        let (ant, lead) = ("dev-a".parse::<UserId>().unwrap(), "lead".parse::<UserId>().unwrap());
        let humans = Registry::with(UserId::human(), &["alice"]);
        let ask = Envelope::new("dev-b".parse().unwrap(), ant.clone(), "build it");
        let dead = DeadLetter::of_envelope(ant.clone(), 3, 5, "harness crashed", ask.clone());
        let told = direct(notice(&ant, &ask, &dead, 9, &humans), &humans);
        let escalation = told.escalation.as_deref().expect("an escalation");
        let fresh = Escalation::new(ant.clone(), ask.thread_id.clone(), EscalationReason::DeadLetter);
        assert_eq!(escalation, &fresh);
        let created = ask.with_creator(Some(lead.clone()));
        let again = direct(notice(&ant, &created, &dead, 9, &humans), &humans);
        let escalation = again.escalation.as_deref().unwrap();
        assert_eq!((escalation.level, &escalation.leader, &escalation.visited), (0, &lead, &vec![ant, lead.clone()]));
    }
}
