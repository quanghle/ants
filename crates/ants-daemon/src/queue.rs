//! The messages waiting for an ant, as `ants queue` and the assistant tool `queue_for_human` list them: those its
//! daemons hold, as their answers to a [`QueueRequest`](ants_proto::QueueRequest) tell, those awaiting redelivery, and
//! the inbox messages and channel posts waking it still in the stream, read from where the answers say its consumers
//! stand.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use ants_proto::{
    CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, ConsumerPlace, Envelope, Humans, MessageMark, QueueReply,
    QueuedMessage, ThreadId, UserId, Uuid, message_mark_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::kv;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::channel::{self, Stored, Wake};
use crate::direct_kv::DirectKv;
use crate::reading::{self, Reading};
use crate::webhooks::Webhooks;

/// A message waiting for an ant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Waiting {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    pub sent_at: DateTime<Utc>,
    pub seq: u64,
    pub running: bool,
    /// How many messages of its thread go before it.
    pub ahead: usize,
    /// Whether the ant's daemon holds it, taken from the stream.
    pub delivered: bool,
    /// Whether it awaits redelivery: delivered, not acked, and back with the server.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub redelivery: bool,
    /// Whether the ant started on it already, in an attempt that failed: it awaits a retry and cannot be withdrawn.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub started: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn_by: Option<UserId>,
    /// The human whose assistant `withdrawn_by` withdrew it, as its daemon checked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn_for: Option<UserId>,
}

/// Messages waiting for an ant, and how many more wait in the stream unlisted.
pub type Listed = (Vec<Waiting>, usize);

/// The messages waiting for `ant`: those its daemons hold, as `answers` list them; those awaiting redelivery, back
/// with the server after a delivery not acked; then up to `undelivered` of the inbox messages and channel posts for it
/// the daemons have not taken from the stream yet, which wake it as `humans` tell. Returns how many more of those there
/// are. `kv`, the attempts bucket, holds the messages' marks; [`withdrawals`] tells which ones are withdrawn.
pub async fn waiting(
    js: &async_nats::jetstream::Context,
    kv: &DirectKv,
    ant: &UserId,
    humans: Humans,
    answers: &[QueueReply],
    undelivered: usize,
) -> Result<Listed> {
    let mut held: Vec<QueuedMessage> = answers.iter().flat_map(|a| a.messages.iter().cloned()).collect();
    held.sort_by_key(|m| m.seq);
    held.dedup_by_key(|m| m.seq);
    let held_seqs: HashSet<u64> = held.iter().map(|m| m.seq).collect();
    let held_posts = held.iter().filter(|m| m.channel.is_some()).count() as u64;
    let held_inbox = held.len() as u64 - held_posts;
    let mut waiting: Vec<Waiting> = held.into_iter().map(held_waiting).collect();
    let mut ahead: HashMap<ThreadId, usize> = HashMap::new();
    for m in &waiting {
        *ahead.entry(m.thread_id.clone()).or_default() += 1;
    }
    let inbox = place(answers, |a| a.inbox);
    let posts = place(answers, |a| a.posts);
    let mut wakes = Wakes::new(js, ant, humans).await;
    let mut found = Vec::new();
    if let Some(inbox) = inbox {
        let missing = inbox.ack_pending.saturating_sub(held_inbox) as usize;
        if missing > 0 && inbox.acked < inbox.delivered {
            let (from, to) = (inbox.acked + 1, Some(inbox.delivered));
            let read = read(js, &subject::inbox_of(ant), from, to, &held_seqs, missing, inbox_message).await;
            found.extend(read.context("reading the messages awaiting redelivery")?);
        }
    }
    if let Some(posts) = posts {
        let missing = posts.ack_pending.saturating_sub(held_posts) as usize;
        if missing > 0 && posts.acked < posts.delivered {
            let (from, to) = (posts.acked + 1, Some(posts.delivered));
            let read = read(js, subject::ALL_CHANNELS, from, to, &held_seqs, usize::MAX, |msg| wakes.candidate(msg));
            let read = read.await.context("reading the posts awaiting redelivery")?;
            found.extend(wakes.waking(read).await.into_iter().take(missing));
        }
    }
    found.sort_by_key(|(seq, _)| *seq);
    for (seq, message) in found {
        let found = mark(kv, ant, &message.id).await?;
        // Acked, or about to be.
        if matches!(found, Some(MessageMark::Finished { .. })) {
            continue;
        }
        let mut redelivered = to_waiting(seq, message, &mut ahead);
        redelivered.redelivery = true;
        match found {
            Some(MessageMark::Started { .. }) => redelivered.started = true,
            Some(MessageMark::Withdrawn { by, on_behalf_of, .. }) => {
                (redelivered.withdrawn_by, redelivered.withdrawn_for) = (Some(by), on_behalf_of);
            }
            _ => {}
        }
        waiting.push(redelivered);
    }
    let mut stored = Vec::new();
    let none = HashSet::new();
    if let Some(inbox) = inbox.filter(|p| p.undelivered > 0) {
        let filter = subject::inbox_of(ant);
        let read = read(js, &filter, inbox.delivered + 1, None, &none, usize::MAX, inbox_message);
        stored.extend(read.await.context("reading the messages waiting in the stream")?);
    }
    if let Some(posts) = posts.filter(|p| p.undelivered > 0) {
        let read =
            read(js, subject::ALL_CHANNELS, posts.delivered + 1, None, &none, usize::MAX, |msg| wakes.candidate(msg));
        let read = read.await.context("reading the posts waiting in the stream")?;
        stored.extend(wakes.waking(read).await);
    }
    stored.sort_by_key(|(seq, _)| *seq);
    let more = stored.len().saturating_sub(undelivered);
    stored.truncate(undelivered);
    waiting.extend(stored.into_iter().map(|(seq, message)| to_waiting(seq, message, &mut ahead)));
    Ok((waiting, more))
}

/// Tell which of the messages `waiting` for `ant`, neither running nor started, are withdrawn, by their marks in `kv`.
pub async fn withdrawals(kv: &DirectKv, ant: &UserId, waiting: &mut [Waiting]) -> Result<()> {
    for message in waiting.iter_mut().filter(|m| !m.running && !m.started && m.withdrawn_by.is_none()) {
        if let Some(MessageMark::Withdrawn { by, on_behalf_of, .. }) = mark(kv, ant, &message.id).await? {
            (message.withdrawn_by, message.withdrawn_for) = (Some(by), on_behalf_of);
        }
    }
    Ok(())
}

/// The mark of message `message` to `ant` in `kv`, the attempts bucket.
pub async fn mark(kv: &DirectKv, ant: &UserId, message: &Uuid) -> Result<Option<MessageMark>> {
    let Some(value) = kv.get(message_mark_key(ant, message)).await? else { return Ok(None) };
    serde_json::from_slice(&value).map(Some).context("reading the message's mark")
}

/// A message a daemon holds, as waiting.
pub fn held_waiting(m: QueuedMessage) -> Waiting {
    Waiting {
        id: m.id,
        thread_id: m.thread_id,
        from: m.from,
        channel: m.channel,
        sent_at: m.sent_at,
        seq: m.seq,
        running: m.running,
        ahead: m.ahead,
        delivered: !m.redelivery,
        redelivery: m.redelivery,
        // A daemon awaits the redelivery of a message only after a failed attempt at it.
        started: m.redelivery,
        withdrawn_by: None,
        withdrawn_for: None,
    }
}

/// Where a message stands: running, withdrawn, or waiting behind others of its thread, in the daemon's hands, awaiting
/// redelivery (or a retry, not withdrawable, after a failed attempt) or still in the stream.
pub fn state(m: &Waiting) -> String {
    if m.running {
        return "running".to_owned();
    }
    if let Some(by) = &m.withdrawn_by {
        return format!("withdrawn by {}", withdrawer(by, m.withdrawn_for.as_ref()));
    }
    let place = if m.ahead == 0 { "next".to_owned() } else { format!("{} ahead", m.ahead) };
    if m.started {
        format!("{place}, awaiting a retry (started; not withdrawable)")
    } else if m.redelivery {
        format!("{place}, awaiting redelivery")
    } else if m.delivered {
        place
    } else {
        format!("{place}, in the stream")
    }
}

/// Who withdrew a message: `by`, or the human `on_behalf_of` via its assistant `by`.
pub fn withdrawer(by: &UserId, on_behalf_of: Option<&UserId>) -> String {
    match on_behalf_of {
        Some(human) => format!("{human} via {by}"),
        None => by.to_string(),
    }
}

/// Where a consumer of the ant stands, as the furthest of the `answers` tells.
fn place(answers: &[QueueReply], of: impl Fn(&QueueReply) -> Option<ConsumerPlace>) -> Option<ConsumerPlace> {
    answers.iter().filter_map(of).reduce(|a, b| ConsumerPlace {
        acked: a.acked.max(b.acked),
        delivered: a.delivered.max(b.delivered),
        undelivered: a.undelivered.max(b.undelivered),
        ack_pending: a.ack_pending.max(b.ack_pending),
    })
}

/// A message for an ant, read from the stream: an inbox message, or a channel post as one.
struct Unheld {
    id: Uuid,
    thread_id: ThreadId,
    from: UserId,
    channel: Option<ChannelId>,
    sent_at: DateTime<Utc>,
}

/// `message`, at `seq`, as a message not held by a daemon, behind the others of its thread counted in `ahead`.
fn to_waiting(seq: u64, message: Unheld, ahead: &mut HashMap<ThreadId, usize>) -> Waiting {
    let before = ahead.entry(message.thread_id.clone()).or_default();
    let waiting = Waiting {
        id: message.id,
        thread_id: message.thread_id,
        from: message.from,
        channel: message.channel,
        sent_at: message.sent_at,
        seq,
        running: false,
        ahead: *before,
        delivered: false,
        redelivery: false,
        started: false,
        withdrawn_by: None,
        withdrawn_for: None,
    };
    *before += 1;
    waiting
}

/// The inbox message in `msg`, when the ant's daemon takes it up: no forged sender, no decision on a tool request.
fn inbox_message(msg: &async_nats::Message) -> Option<Unheld> {
    let envelope = serde_json::from_slice::<Envelope>(&msg.payload).ok()?;
    if envelope.check_subject(&msg.subject).is_err() || envelope.approval.is_some() {
        return None;
    }
    let Envelope { id, thread_id, from, sent_at, .. } = envelope;
    Some(Unheld { id, thread_id, from, channel: None, sent_at })
}

/// The messages on `filter` from stream sequence `from` on, up to `to`, but those at `skip`, which `take` keeps, up to
/// `limit` of them. Read with a function taking each at once, so the read stays `Send`.
async fn read<T>(
    js: &async_nats::jetstream::Context,
    filter: &str,
    from: u64,
    to: Option<u64>,
    skip: &HashSet<u64>,
    limit: usize,
    mut take: impl FnMut(&async_nats::Message) -> Option<T>,
) -> Result<Vec<(u64, T)>> {
    let mut found = Vec::new();
    let start = DeliverPolicy::ByStartSequence { start_sequence: from };
    reading::read_back_messages(js, "", Reading::Gets, filter, start, |seq, _, msg| {
        if to.is_some_and(|to| seq > to) {
            return ControlFlow::Break(());
        }
        if !skip.contains(&seq)
            && let Some(kept) = take(&msg)
        {
            found.push((seq, kept));
            if found.len() >= limit {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    })
    .await?;
    Ok(found)
}

/// Which channel posts wake an ant, as its daemon tells them ([`channel::wake`]), with the channels' entries and the
/// webhooks read once.
struct Wakes {
    ant: UserId,
    humans: Humans,
    channels: Option<kv::Store>,
    entries: HashMap<ChannelId, Option<ChannelEntry>>,
    webhooks: Webhooks,
    checked: HashMap<UserId, bool>,
}

impl Wakes {
    async fn new(js: &async_nats::jetstream::Context, ant: &UserId, humans: Humans) -> Self {
        Self {
            ant: ant.clone(),
            humans,
            channels: js.get_key_value(CHANNELS_BUCKET).await.ok(),
            entries: HashMap::new(),
            webhooks: Webhooks::new(js.clone()),
            checked: HashMap::new(),
        }
    }

    /// The channel post in `msg`, without its text, when it may wake the ant: one of another's mentioning the ant, or
    /// nobody. [`Wakes::waking`] tells.
    fn candidate(&self, msg: &async_nats::Message) -> Option<ChannelPost> {
        let mut post = serde_json::from_slice::<ChannelPost>(&msg.payload).ok()?;
        post.check_subject(&msg.subject).ok()?;
        if post.from == self.ant || !(post.mentions.is_empty() || post.mentions.contains(&self.ant)) {
            return None;
        }
        post.text = String::new();
        post.attachments = Vec::new();
        Some(post)
    }

    /// The `posts` that wake the ant, as messages for it.
    async fn waking(&mut self, posts: Vec<(u64, ChannelPost)>) -> Vec<(u64, Unheld)> {
        let mut woken = Vec::new();
        for (seq, mut post) in posts {
            if post.webhook && !self.humans.contains(&post.from) {
                post.webhook = match self.checked.get(&post.from) {
                    Some(&webhook) => webhook,
                    None => {
                        let webhook = self.webhooks.is_webhook(&post.from).await;
                        *self.checked.entry(post.from.clone()).or_insert(webhook)
                    }
                };
            }
            if !self.entries.contains_key(&post.channel) {
                let entry = match &self.channels {
                    Some(kv) => match channel::load(kv, &post.channel).await {
                        Ok(Stored::Entry(entry, _)) => Some(entry),
                        _ => None,
                    },
                    None => None,
                };
                self.entries.insert(post.channel.clone(), entry);
            }
            let entry = self.entries.get(&post.channel).and_then(Option::as_ref);
            if channel::wake(&self.ant, entry, &post, &self.humans) == Wake::Skip {
                continue;
            }
            let ChannelPost { id, thread_id, from, channel, sent_at, .. } = post;
            woken.push((seq, Unheld { id, thread_id, from, channel: Some(channel), sent_at }));
        }
        woken
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::QueueRequest;
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus;

    #[tokio::test]
    async fn redeliveries_and_posts_waking_the_ant_are_listed_after_the_messages_held() {
        let Some(server) = NatsServer::start() else { return };
        let (alice, ant): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = bus::connect(&server.url, None, None, &alice, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        bus::init_own_buckets(&js).await.unwrap();
        let mut ops = ChannelEntry::new("ops".parse().unwrap());
        ops.members.push(ant.clone());
        let channels = bus::ensure_channels(&js).await.unwrap();
        channels.put(ops.key(), serde_json::to_vec(&ops).unwrap().into()).await.unwrap();
        let kv = DirectKv::new(&js, None, ants_proto::ATTEMPTS_BUCKET).await.unwrap();
        let humans = Humans::from_entries([("alice", ants_proto::HumanEntry::new(alice.clone()))]);

        let running = Envelope::new(alice.clone(), ant.clone(), "running");
        let on_thread = |text| Envelope::on_thread(running.thread_id.clone(), alice.clone(), ant.clone(), text);
        let (failed, retried, finished, later) =
            (on_thread("failed"), on_thread("retried"), on_thread("done"), on_thread("later"));
        let woken = ChannelPost::new(ops.channel.clone(), alice.clone(), "@dev-a look");
        let unwoken = ChannelPost::new("lobby".parse().unwrap(), alice.clone(), "@dev-a look");
        for envelope in [&running, &failed, &retried, &finished] {
            js.publish(envelope.subject(), serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
        }
        for post in [&woken, &unwoken] {
            js.publish(post.subject(), serde_json::to_vec(post).unwrap().into()).await.unwrap().await.unwrap();
        }
        js.publish(later.subject(), serde_json::to_vec(&later).unwrap().into()).await.unwrap().await.unwrap();
        let thread_id = running.thread_id.clone();
        for (envelope, mark) in [
            (&retried, MessageMark::Started { thread_id: thread_id.clone(), at: Utc::now() }),
            (&finished, MessageMark::Finished { thread_id: thread_id.clone(), at: Utc::now() }),
            (&later, MessageMark::Withdrawn { by: alice.clone(), at: Utc::now(), on_behalf_of: None }),
        ] {
            kv.put(message_mark_key(&ant, &envelope.id), serde_json::to_vec(&mark).unwrap()).await.unwrap();
        }

        let held = QueuedMessage {
            id: running.id,
            thread_id: thread_id.clone(),
            from: alice.clone(),
            channel: None,
            sent_at: running.sent_at,
            seq: 1,
            running: true,
            ahead: 0,
            redelivery: false,
        };
        // The messages at 2 to 4 are delivered and not acked; the channel consumer has delivered no post yet.
        let inbox = ConsumerPlace { acked: 0, delivered: 4, undelivered: 1, ack_pending: 4 };
        let posts = ConsumerPlace { acked: 0, delivered: 0, undelivered: 2, ack_pending: 0 };
        let answer = QueueRequest::new(ant.clone(), alice.clone()).answer(vec![held], Some(inbox), Some(posts));
        let (mut listed, more) =
            waiting(&js, &kv, &ant, humans.clone(), std::slice::from_ref(&answer), 10).await.unwrap();
        withdrawals(&kv, &ant, &mut listed).await.unwrap();
        let ids: Vec<Uuid> = listed.iter().map(|m| m.id).collect();
        assert_eq!(ids, [running.id, failed.id, retried.id, woken.id, later.id]);
        let states: Vec<String> = listed.iter().map(state).collect();
        assert_eq!(
            states,
            [
                "running",
                "1 ahead, awaiting redelivery",
                "2 ahead, awaiting a retry (started; not withdrawable)",
                "next, in the stream",
                "withdrawn by alice",
            ]
        );
        assert_eq!(listed[3].channel, Some(ops.channel.clone()));
        assert_eq!(more, 0);

        let (listed, more) = waiting(&js, &kv, &ant, humans, &[answer], 1).await.unwrap();
        assert_eq!(listed.last().map(|m| m.id), Some(woken.id));
        assert_eq!(more, 1, "the later inbox message waits unlisted");
    }
}
