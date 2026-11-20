//! A user's live inbox: the messages to it as they come, each once, caught up from the stream after a gap.

use std::collections::{HashSet, VecDeque};
use std::hash::{Hash as _, Hasher as _};
use std::ops::ControlFlow;
use std::pin::Pin;
use std::time::Duration;

use ants_proto::{STREAM, UserId, subject};
use anyhow::Result;
use async_nats::jetstream;
use async_nats::jetstream::consumer;
use futures::{Stream, StreamExt as _};

use crate::stream::reading::{self, Reading};

use super::relayed;

/// Messages to a user as they come, from [`live_inbox`].
pub type LiveInbox = Pin<Box<dyn Stream<Item = async_nats::Message> + Send>>;

/// How many messages [`live_inbox`] remembers to take each once.
const SEEN: usize = 1024;

/// Messages to `user` as they come, each once, on their inbox subject: published there, and as the hub's stream
/// stored them ([`subject::stored_inbox`]). A message sourced from a leaf's outbox comes only the second way.
/// Subscribed before it returns, so
/// nothing published after that is missed while the connection reaches the hub; [`caught_up_inbox`] also gets what
/// the hub stored meanwhile.
pub async fn live_inbox(client: &async_nats::Client, user: &UserId) -> Result<LiveInbox> {
    inbox(client, user, None).await
}

/// How often [`caught_up_inbox`] reads back what the hub's stream stored.
pub const CATCH_UP: Duration = Duration::from_secs(5);

/// Most messages [`caught_up_inbox`] reads back at once.
const CATCH_UP_BATCH: usize = 100;

/// [`live_inbox`], which also reads back from the hub's stream, every [`CATCH_UP`], the messages stored for `user`
/// since it was called (or since the hub first answered, when out of its reach then): what the hub stored while this
/// connection, or the uplink of the leaf node it goes through, was down never comes live. It reads as `reading` says,
/// as the CLI does with the user's credentials ([`Reading::cli`]): a human or an assistant by message gets, an ant
/// through a spare of its inbox consumer, which filters only its own inbox, so the read never touches the consumer its
/// daemon takes the inbox from.
pub async fn caught_up_inbox(
    client: &async_nats::Client,
    js: &jetstream::Context,
    user: &UserId,
    reading: Reading,
) -> Result<LiveInbox> {
    let read_back = read_back(js.clone(), ants_proto::inbox_consumer(user), reading, subject::inbox_of(user));
    inbox(client, user, Some(read_back)).await
}

async fn inbox(client: &async_nats::Client, user: &UserId, read_back: Option<LiveInbox>) -> Result<LiveInbox> {
    let live = relayed::subscribe(client, subject::inbox_of(user)).await?;
    let live = live.filter(|msg| std::future::ready(!relayed::heard_relayed(msg)));
    #[expect(clippy::disallowed_methods, reason = "the stream's republished messages, which as_published tells")]
    let stored = client.subscribe(subject::stored_inbox(user)).await?;
    let user = user.clone();
    let stored = stored.filter_map(move |msg| std::future::ready(as_published(msg, &user)));
    let mut all: Vec<LiveInbox> = vec![Box::pin(live), Box::pin(stored)];
    all.extend(read_back);
    let mut seen = Seen::default();
    Ok(Box::pin(futures::stream::select_all(all).filter(move |msg| std::future::ready(seen.first(msg)))))
}

/// The messages on `filter` the hub's stream stores from now on, read back every [`CATCH_UP`] as `reading` says,
/// through spares of `reader` for an ant, until dropped.
fn read_back(js: jetstream::Context, reader: String, reading: Reading, filter: String) -> LiveInbox {
    let (found, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut after = None;
        loop {
            for msg in stored_after(&js, &reader, reading, &filter, &mut after).await {
                if found.send(msg).is_err() {
                    return;
                }
            }
            tokio::select! {
                () = found.closed() => return,
                () = tokio::time::sleep(CATCH_UP) => {}
            }
        }
    });
    Box::pin(futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|msg| (msg, rx)) }))
}

/// The messages on `filter` the hub's stream stored after sequence `after`, which moves past them. Without `after`, it
/// becomes the stream's last sequence. Out of the hub's reach, none.
async fn stored_after(
    js: &jetstream::Context,
    reader: &str,
    reading: Reading,
    filter: &str,
    after: &mut Option<u64>,
) -> Vec<async_nats::Message> {
    let Some(last) = *after else {
        match reading::last_sequence(js, reader, reading, filter).await {
            Ok(last) => *after = Some(last),
            Err(e) => tracing::debug!(error = format!("{e:#}"), "cannot read back the inbox yet"),
        }
        return Vec::new();
    };
    let mut found = Vec::new();
    let start = consumer::DeliverPolicy::ByStartSequence { start_sequence: last + 1 };
    let read = reading::read_back_messages(js, reader, reading, filter, start, |seq, _, msg| {
        *after = Some(seq);
        found.push(msg);
        if found.len() < CATCH_UP_BATCH { ControlFlow::Continue(()) } else { ControlFlow::Break(()) }
    })
    .await;
    if let Err(e) = read {
        tracing::debug!(error = format!("{e:#}"), "reading back the inbox");
    }
    found
}

/// A message the hub's stream republished to `user`, back on the inbox subject it was stored under; `None` for anything
/// else. The server also delivers on these subjects for any client that names one, with headers that client stored:
/// the answer to a direct get, and a push consumer's messages, which carry a reply subject. A direct get, like the
/// stream, adds its `Nats-Stream` and `Nats-Subject` headers after those, so a message counts only without a reply
/// subject and with a single value of each: [`STREAM`], and the inbox of `user` from the sender its own subject names.
pub(super) fn as_published(mut msg: async_nats::Message, user: &UserId) -> Option<async_nats::Message> {
    let inbox = subject::parse_stored_inbox(user, &msg.subject)?;
    let headers = msg.headers.as_ref()?;
    let single = |name| match headers.get_all(name).collect::<Vec<_>>()[..] {
        [value] => Some(value.as_str()),
        _ => None,
    };
    let republished = msg.reply.is_none()
        && single(async_nats::header::NATS_STREAM) == Some(STREAM)
        && single(async_nats::header::NATS_SUBJECT) == Some(inbox.as_str());
    if !republished {
        return None;
    }
    msg.subject = inbox.into();
    Some(msg)
}

/// The latest messages taken, by message id, or payload for one without.
#[derive(Default)]
pub(super) struct Seen {
    order: VecDeque<u64>,
    keys: HashSet<u64>,
}

impl Seen {
    /// Whether `msg` is new, then remembered.
    pub(super) fn first(&mut self, msg: &async_nats::Message) -> bool {
        let mut hasher = std::hash::DefaultHasher::new();
        match msg.headers.as_ref().and_then(|h| h.get(async_nats::header::NATS_MESSAGE_ID)) {
            Some(id) => (0u8, id.as_str()).hash(&mut hasher),
            None => (1u8, &msg.payload[..]).hash(&mut hasher),
        }
        let key = hasher.finish();
        if !self.keys.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > SEEN
            && let Some(old) = self.order.pop_front()
        {
            self.keys.remove(&old);
        }
        true
    }
}
