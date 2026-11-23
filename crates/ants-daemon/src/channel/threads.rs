//! Reading channel threads back: the posts an ant has not been shown, who started a thread and who takes part in it.

mod sinks;

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::ops::ControlFlow;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use ants_proto::{
    CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, Humans, ThreadId, ThreadOrigin, UserId, Uuid, chan_reader,
    channel_seen_key, mention_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};

use self::sinks::{Author, Collect, Origin, Part, Root, Sink};
use super::{Reading, Stored, load};
use crate::records::direct_kv::DirectKv;
use crate::records::links;
use crate::records::owners::Owners;
use crate::sync::MutexExt as _;

/// Most earlier posts a prompt shows; older unseen ones are counted instead.
const MAX_CONTEXT: usize = 50;
/// Slack for the clocks of a poster and the server, when a read starts at the time a post was sent, such as a thread's
/// start.
pub const CLOCK_SLACK: Duration = Duration::from_secs(300);
/// Most entries a [`Bounded`] cache of what was read of threads keeps; past it they are read again.
const MAX_ORIGINS: usize = 4096;
/// Most ants, the sender of a message among them, whose link to a channel thread is checked for it.
const MAX_LINK_CHECKS: usize = 32;

/// Whether [`Threads::context`] also reads the earlier posts of a thread the ant has not been shown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Earlier {
    Unseen,
    Omitted,
}

/// The earlier posts on a thread to show with a prompt, and the later ones its turns left go by.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Context {
    pub posts: Vec<ChannelPost>,
    /// Unseen posts left out, the oldest ones.
    pub skipped: usize,
    /// The posts stored on the thread after the one being answered, from a human's last one on: what
    /// [`ChannelPost::turns_left`] goes by.
    pub later: Vec<ChannelPost>,
    /// Whether the post being answered answers one of the ant's own ([`ChannelPost::in_reply_to`]).
    pub answers_own: bool,
    /// Stream sequence of the last post stored on the channel as the read ended, any thread's: a reply spending a turn
    /// is stored only while it is still the last ([`crate::bus::store_expecting`]).
    pub last_seq: u64,
}

/// What was read of threads so far, by key: emptied before an insert that finds [`MAX_ORIGINS`] entries.
struct Bounded<K, V>(Mutex<HashMap<K, V>>);

impl<K, V> Default for Bounded<K, V> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<K: Eq + Hash, V: Clone> Bounded<K, V> {
    fn get(&self, key: &K) -> Option<V> {
        self.0.locked().get(key).cloned()
    }

    fn insert(&self, key: K, value: V) {
        let mut entries = self.0.locked();
        if entries.len() >= MAX_ORIGINS {
            entries.clear();
        }
        entries.insert(key, value);
    }
}

/// Reads channel threads back for an ant, or for the human's side, and keeps which posts the ant was shown.
pub struct Threads {
    js: jetstream::Context,
    ant: UserId,
    /// The sessions bucket, under the ant's own keys.
    seen: DirectKv,
    /// How the reader reads channels back.
    reading: Reading,
    /// One read at a time through the reader's own consumer.
    one_read: tokio::sync::Mutex<()>,
    /// The origins of threads read so far, with the stream sequence each read reached.
    origins: Bounded<(ChannelId, ThreadId), (ThreadOrigin, u64)>,
    /// The channel whose top-level post started each thread looked up so far ([`Threads::origin_of`]); None for a
    /// thread no channel post started.
    roots: Bounded<ThreadId, Option<ChannelId>>,
    /// The senders found taking part in each thread so far ([`Threads::origin_for`]).
    parts: Bounded<(ThreadId, UserId), ()>,
    /// The owners recorded for threads humans and webhooks message ants on.
    owners: Owners,
}

impl Threads {
    pub fn new(js: jetstream::Context, ant: UserId, sessions: DirectKv) -> Self {
        Self {
            js: js.clone(),
            ant,
            seen: sessions,
            reading: Reading::Consumer,
            one_read: tokio::sync::Mutex::new(()),
            origins: Bounded::default(),
            roots: Bounded::default(),
            parts: Bounded::default(),
            owners: Owners::new(js),
        }
    }

    /// Read channels back as `reading` says; an ant's daemon reads through its reader consumer.
    #[must_use]
    pub fn reading(mut self, reading: Reading) -> Self {
        self.reading = reading;
        self
    }

    /// The origin of `thread`, at stream sequence `seq`, when a top-level channel post stored before `seq` started it
    /// ([`Threads::origin`]); None when none did, as for a thread a message started. The top-level post is the first
    /// post of any channel whose id is the thread's, among those stored within [`CLOCK_SLACK`] of the time in it.
    pub async fn origin_of(&self, thread: &ThreadId, seq: u64, humans: &Humans) -> Result<Option<ThreadOrigin>> {
        let Some(channel) = self.root(thread, seq).await? else { return Ok(None) };
        let on_thread = ChannelPost::on_thread(channel, thread.clone(), self.ant.clone(), "");
        self.origin(&on_thread, seq, humans).await.map(Some)
    }

    /// Whether a top-level channel post stored before stream sequence `seq` started `thread` ([`Threads::origin_of`]).
    pub async fn is_channel_thread(&self, thread: &ThreadId, seq: u64) -> Result<bool> {
        Ok(self.root(thread, seq).await?.is_some())
    }

    /// The owner of `thread` that a human's or a webhook's message to `to` named ([`Owners::recorded`]), waiting up to
    /// `within` for a record on its way ([`Owners::wait_for`]).
    pub async fn awaited_owner(&self, thread: &ThreadId, to: &UserId, within: Duration) -> Result<Option<UserId>> {
        self.owners.wait_for(thread, to, within).await
    }

    /// The owner of `thread`, a thread no channel post started, for an ant's message on or linking to it, when `sender`
    /// has a link to it: the one a human's or a webhook's message to it on the thread named ([`Owners::recorded`]),
    /// else the one such a message named to an ant that passed the link on to it, directly or through ants it passed
    /// the link on to in turn ([`links::passed_on`]). With `assisted`, a human and the assistant its grant names, that
    /// human too when the assistant recorded it as the owner its message for it to such an ant named
    /// ([`Owners::assisted`]). None otherwise: no ant names the owner of a message thread it has no link to.
    pub async fn owner_for(
        &self,
        thread: &ThreadId,
        sender: &UserId,
        assisted: Option<(&UserId, &UserId)>,
    ) -> Result<Option<UserId>> {
        let recorded = async |ant: &UserId| match (self.owners.recorded(thread, ant).await?, assisted) {
            (Some(owner), _) => Ok(Some(owner)),
            (None, Some((human, assistant))) => self.owners.assisted(thread, ant, human, assistant).await,
            (None, None) => Ok(None),
        };
        if let Some(owner) = recorded(sender).await? {
            return Ok(Some(owner));
        }
        self.passed_to(thread, sender, recorded).await
    }

    /// The origin of `thread`, at stream sequence `seq`, as [`Threads::origin_of`] reads it, when `sender` has a link
    /// to it: it takes part in the thread ([`Threads::takes_part`]), or an ant that does passed the link on to it,
    /// directly or through ants it passed the link on to in turn ([`links::passed_on`]). None otherwise: no ant links
    /// its thread to a channel thread it has no link to, to take that thread's creator.
    pub(crate) async fn origin_for(
        &self,
        thread: &ThreadId,
        sender: &UserId,
        seq: u64,
        humans: &Humans,
    ) -> Result<Option<ThreadOrigin>> {
        let Some(channel) = self.root(thread, seq).await? else { return Ok(None) };
        let channels = self.js.get_key_value(CHANNELS_BUCKET).await.context("opening the channels")?;
        let Stored::Entry(entry, _) = load(&channels, &channel).await? else { return Ok(None) };
        let at = Part { thread, sender, entry: &entry, humans, upto: seq, found: false };
        let takes_part = async |ant: &UserId| Ok(self.takes_part(&channel, &at, ant).await?.then_some(()));
        if takes_part(sender).await?.is_none() && self.passed_to(thread, sender, takes_part).await?.is_none() {
            return Ok(None);
        }
        let on_thread = ChannelPost::on_thread(channel, thread.clone(), self.ant.clone(), "");
        self.origin(&on_thread, seq, humans).await.map(Some)
    }

    /// Whether `sender` takes part in the thread of `channel` that `at` reads: a member or the messenger of the channel
    /// as the channels bucket holds it now, with a post on the thread stored up to where `at` reads, or woken by one
    /// there ([`ChannelEntry::woken`]).
    async fn takes_part(&self, channel: &ChannelId, at: &Part<'_>, sender: &UserId) -> Result<bool> {
        let mut part = Part { sender, found: false, ..*at };
        let thread = part.thread;
        if !part.entry.is_member(sender) && part.entry.messenger() != *sender {
            return Ok(false);
        }
        if self.parts.get(&(thread.clone(), sender.clone())).is_some() {
            return Ok(true);
        }
        self.scan(&subject::chan_room(channel), from_start(thread), &mut part)
            .await
            .with_context(|| format!("reading thread {thread} of #{channel} for posts of {sender}"))?;
        if part.found {
            self.parts.insert((thread.clone(), sender.clone()), ());
        }
        Ok(part.found)
    }

    /// What `takes_part` finds of the first ant taking part in `thread` that passed the link to the thread on to `to`,
    /// directly or through ants it passed it on to in turn, as the links bucket says ([`links::passed_on`]). At most
    /// [`MAX_LINK_CHECKS`] ants are checked.
    async fn passed_to<T>(
        &self,
        thread: &ThreadId,
        to: &UserId,
        takes_part: impl AsyncFn(&UserId) -> Result<Option<T>>,
    ) -> Result<Option<T>> {
        let links = links::passed_on(&self.js, thread).await?;
        let from = |to: &UserId| links.iter().filter(|(_, t)| t == to).map(|(from, _)| from).collect::<Vec<_>>();
        let mut checked = HashSet::from([to]);
        let mut queue = VecDeque::from(from(to));
        while let Some(ant) = queue.pop_front() {
            if !checked.insert(ant) {
                continue;
            }
            if checked.len() > MAX_LINK_CHECKS {
                break;
            }
            if let Some(found) = takes_part(ant).await? {
                return Ok(Some(found));
            }
            queue.extend(from(ant));
        }
        Ok(None)
    }

    /// The channel whose top-level post, stored before stream sequence `seq`, started `thread`: the first post of any
    /// channel whose id is the thread's, among those stored within [`CLOCK_SLACK`] of the time in it.
    pub(crate) async fn root(&self, thread: &ThreadId, seq: u64) -> Result<Option<ChannelId>> {
        let Some(start) = thread_start(thread) else { return Ok(None) };
        if let Some(channel) = self.roots.get(thread) {
            return Ok(channel);
        }
        let mut root = Root { thread, upto: seq, until: start + 2 * CLOCK_SLACK, channel: None };
        let from = consumer::DeliverPolicy::ByStartTime { start_time: start.into() };
        self.scan(subject::ALL_CHANNELS, from, &mut root)
            .await
            .with_context(|| format!("looking for the top-level post of thread {thread}"))?;
        self.roots.insert(thread.clone(), root.channel.clone());
        Ok(root.channel)
    }

    /// The origin of the thread of `post`, at stream sequence `seq`: who started it, as the posts stored on it up to
    /// `post` say ([`ThreadOrigin`]), not the owner and creator `post` carries. A top-level post is its own; the posts
    /// of any other thread are read from its start once, then on from where the last read of it ended. Each post is
    /// a webhook's as its subject says ([`ChannelPost::check_subject`]).
    pub async fn origin(&self, post: &ChannelPost, seq: u64, humans: &Humans) -> Result<ThreadOrigin> {
        if post.starts_thread() {
            return Ok(ThreadOrigin::of_root(post, humans));
        }
        let key = (post.channel.clone(), post.thread_id.clone());
        let cached = self.origins.get(&key);
        let (origin, start) = match cached {
            Some((origin, read)) if origin.settled() || read >= seq => return Ok(origin),
            Some((origin, read)) => (origin, consumer::DeliverPolicy::ByStartSequence { start_sequence: read + 1 }),
            None => (ThreadOrigin::default(), from_start(&post.thread_id)),
        };
        let mut fold = Origin { thread: &post.thread_id, humans, upto: seq, fold: origin };
        self.scan(&subject::chan_room(&post.channel), start, &mut fold)
            .await
            .with_context(|| format!("reading the origin of thread {} of #{}", post.thread_id, post.channel))?;
        let origin = fold.fold;
        self.origins.insert(key, (origin.clone(), seq));
        Ok(origin)
    }

    /// The posts on the thread of `post`, at stream sequence `seq`, stored after it; with [`Earlier::Unseen`], also those before
    /// it that the ant has not been shown: after the last one shown, or since the thread started, without its own,
    /// and whether `post` answers one of the ant's own ([`Context::answers_own`]), looked up for an ant's post when not
    /// read.
    /// The posts of `humans` renew the thread's turns. The read ends at the last post stored as it starts.
    pub async fn context(&self, post: &ChannelPost, seq: u64, earlier: Earlier, humans: &Humans) -> Result<Context> {
        let mut collect = Collect::new(&self.ant, humans, &post.thread_id, seq, post.in_reply_to);
        let after = consumer::DeliverPolicy::ByStartSequence { start_sequence: seq + 1 };
        let seen = if earlier == Earlier::Unseen && !post.starts_thread() {
            Some(self.last_seen(&post.thread_id).await?)
        } else {
            None
        };
        let whole = seen == Some(None);
        let start = match seen {
            None => after,
            Some(Some(seen)) if seen + 1 >= seq => after,
            Some(Some(seen)) => consumer::DeliverPolicy::ByStartSequence { start_sequence: seen + 1 },
            Some(None) => from_start(&post.thread_id),
        };
        let last_seq = self
            .scan(&subject::chan_room(&post.channel), start, &mut collect)
            .await
            .with_context(|| format!("reading thread {} of #{}", post.thread_id, post.channel))?;
        let unread = collect.answered_own.is_none() && earlier == Earlier::Unseen && !whole;
        // Only an ant's post (no human's, webhook's or assistant's for its human) goes unanswered as one answering
        // the ant's own.
        let by_ant = !post.webhook && post.on_behalf_of.is_none() && !humans.contains(&post.from);
        let mut context = Context { last_seq: last_seq.max(seq), ..collect.done() };
        if let Some(id) = post.in_reply_to.filter(|_| unread && by_ant) {
            context.answers_own = match self.author(post, id, seq).await {
                Ok(author) => author.as_ref() == Some(&self.ant),
                Err(e) => {
                    tracing::warn!(thread = %post.thread_id, error = format!("{e:#}"), "looking up the post answered");
                    false
                }
            };
        }
        Ok(context)
    }

    /// The author of post `id` on the thread of `post`, at stream sequence `seq`, stored before it: read from the time
    /// in `id`, a time-ordered UUID, or else from the thread's start.
    async fn author(&self, post: &ChannelPost, id: Uuid, seq: u64) -> Result<Option<UserId>> {
        let start = match (uuid_start(id), thread_start(&post.thread_id)) {
            (Some(at), started) => {
                let at = started.map_or(at, |started| at.max(started));
                consumer::DeliverPolicy::ByStartTime { start_time: at.into() }
            }
            (None, _) => from_start(&post.thread_id),
        };
        let mut author = Author { thread: &post.thread_id, id, before: seq, from: None };
        self.scan(&subject::chan_room(&post.channel), start, &mut author)
            .await
            .with_context(|| format!("reading post {id} on thread {} of #{}", post.thread_id, post.channel))?;
        Ok(author.from)
    }

    /// Read the channel posts on `filter` from `start` into `sink`, up to the last one stored as the read starts,
    /// returning the stream sequence of the last message read, 0 for none.
    async fn scan(&self, filter: &str, start: consumer::DeliverPolicy, sink: &mut impl Sink) -> Result<u64> {
        let _one = match self.reading {
            Reading::Consumer => Some(self.one_read.lock().await),
            Reading::Spare | Reading::Gets => None,
        };
        let mut last = 0;
        read_back(&self.js, &self.ant, self.reading, filter, start, |seq, stored, subject, post| {
            last = seq;
            match post {
                Ok(post) => sink.take(seq, stored, post),
                Err(error) => {
                    tracing::debug!(subject, error, "dropping a channel post");
                    ControlFlow::Continue(())
                }
            }
        })
        .await?;
        Ok(last)
    }

    /// Stream sequence of the last post on `thread` the ant was shown.
    async fn last_seen(&self, thread: &ThreadId) -> Result<Option<u64>> {
        let value = self.seen.get(channel_seen_key(&self.ant, thread)).await.context("reading the last post seen")?;
        Ok(value.and_then(|v| std::str::from_utf8(&v).ok()?.parse().ok()))
    }

    /// Note that the ant was shown the posts on `thread` up to stream sequence `seq`.
    pub async fn seen(&self, thread: &ThreadId, seq: u64) -> Result<()> {
        if self.last_seen(thread).await?.is_some_and(|seen| seen >= seq) {
            return Ok(());
        }
        let key = channel_seen_key(&self.ant, thread);
        self.seen.put(key, seq.to_string()).await.context("storing the last post seen")?;
        Ok(())
    }

    /// Put the [`ChannelPost::mention_notice`] of `post`, at stream sequence `seq`, in the inbox of each human member
    /// of `entry` it @mentions, once each, when the ant sends its notices ([`ChannelEntry::notifier`]): the ant's own
    /// post, or a webhook's in a channel the ant is the messenger of, whose notices it relays
    /// ([`ChannelPost::relayed_notice`]). The notice's msg id makes JetStream drop a copy stored again soon after, and
    /// a marker in the ant's sessions keys ([`mention_key`]) one stored again later, as on a redelivery after a
    /// restart. Returns the humans notified now.
    pub async fn notify_mentioned(
        &self,
        post: &ChannelPost,
        seq: u64,
        entry: Option<&ChannelEntry>,
        humans: &Humans,
    ) -> Result<Vec<UserId>> {
        let mut notified = Vec::new();
        let Some(entry) = entry.filter(|entry| entry.notifier(post) == self.ant) else { return Ok(notified) };
        for human in entry.notified(post, humans) {
            let key = mention_key(&self.ant, seq, &human);
            if self.seen.get(&key).await.context("reading a mention marker")?.is_some() {
                continue;
            }
            let notice = if post.from == self.ant {
                post.mention_notice(&human)
            } else {
                post.relayed_notice(&human, &self.ant)
            };
            crate::bus::store(&self.js, notice.subject(), &notice, &post.mention_msg_id(&human))
                .await
                .with_context(|| format!("notifying {human} of the mention"))?;
            self.seen.put(key, post.id.to_string()).await.context("storing a mention marker")?;
            notified.push(human);
        }
        Ok(notified)
    }
}

/// Hand the channel posts stored on `filter` from `start` on (all, from a stream sequence or from a time), oldest
/// first, to `take` until it breaks, up to the last one stored as the read starts, as `reader` reads them
/// ([`Reading`]; through [`chan_reader`] for a consumer). `take` gets each post's stream sequence, the time it was
/// stored and its subject, and the post, or why the message is none: not one, or a post whose sender or channel
/// disagrees with its subject.
pub async fn read_back(
    js: &jetstream::Context,
    reader: &UserId,
    reading: Reading,
    filter: &str,
    start: consumer::DeliverPolicy,
    mut take: impl FnMut(u64, SystemTime, &str, Result<ChannelPost, String>) -> ControlFlow<()>,
) -> Result<()> {
    crate::stream::reading::read_back(
        js,
        &chan_reader(reader),
        reading,
        filter,
        start,
        |seq, stored, subject, payload| take(seq, stored, subject, parse_post(subject, payload)),
    )
    .await
}

fn parse_post(subject: &str, payload: &[u8]) -> Result<ChannelPost, String> {
    let post = serde_json::from_slice::<ChannelPost>(payload).map_err(|e| e.to_string())?;
    post.check_subject(subject).map_err(|e| e.to_string())?;
    Ok(post)
}

/// Where a read of `thread` from its start begins: at [`thread_start`], else at the channel's oldest post kept.
pub(crate) fn from_start(thread: &ThreadId) -> consumer::DeliverPolicy {
    match thread_start(thread) {
        Some(time) => consumer::DeliverPolicy::ByStartTime { start_time: time.into() },
        None => consumer::DeliverPolicy::All,
    }
}

/// When `thread` started, a little early, if its id is a time-ordered UUID as a top-level post's is.
pub fn thread_start(thread: &ThreadId) -> Option<SystemTime> {
    uuid_start(Uuid::parse_str(thread.as_str()).ok()?)
}

/// The time in `id`, a little early, if it is a time-ordered UUID.
fn uuid_start(id: Uuid) -> Option<SystemTime> {
    let (secs, nanos) = id.get_timestamp()?.to_unix();
    (SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)).checked_sub(CLOCK_SLACK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::tests::post;

    #[test]
    fn a_thread_named_after_a_post_starts_at_the_post() {
        let root = post("human", "x");
        let (start, sent) = (thread_start(&root.thread_id).unwrap(), SystemTime::from(root.sent_at));
        assert!(start <= sent && sent.duration_since(start).unwrap() <= CLOCK_SLACK * 2);
        assert_eq!(thread_start(&"t1".parse().unwrap()), None);
    }
}
