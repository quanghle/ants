//! Channels as a member's daemon sees them.
//!
//! The daemon watches the channels bucket ([`Channels`]) and takes every channel post through its consumer
//! `chan-<ant>`. A post wakes the ant only when [`wake`] says so; the rest are acked without a prompt. A prompt
//! on a channel thread shows the posts on the thread the ant has not been shown yet ([`Threads::context`]): the
//! stream sequence of the last post shown is kept per thread in the sessions bucket, so a restarted daemon goes on from
//! there. The reply is posted on the same thread, spending one of its turns.
//!
//! A post of a human or a webhook that @mentions nobody goes to the channel's messenger instead, to triage: its answer @mentions
//! the members the post is relevant to, which wakes them ([`triage_prompt`]). Who is a human is the daemon's
//! [`crate::humans::Registry`]'s to say.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use ants_proto::{
    CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, Envelope, Humans, RosterEntry, ThreadId, ThreadOrigin,
    UserId, Uuid, chan_reader, channel_seen_key, mention_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer, kv};
use futures::{StreamExt as _, TryStreamExt as _};

use crate::direct_kv::DirectKv;
use crate::links;
use crate::owners::Owners;
pub use crate::reading::Reading;
use crate::sync::MutexExt as _;
use crate::webhooks::Webhooks;

/// What ants tells a channel's messenger about triaging a post; the messenger's own instructions add to it.
pub const TRIAGE_RULES: &str = "[ants] Triage this post; do not answer or act on the request itself.
1. Read the post, and the thread before it.
2. Decide which members of the channel below the post is relevant to, by their roles and capabilities.
3. Answer with a short post that @mentions exactly the relevant members, each with a few words on why.
4. When no member is relevant, answer with a short post saying so, mentioning nobody.
Only @mentions of members reach anyone: mention no one else. A mention wakes an ant and notifies a human in its \
inbox. An ant not in the roster is offline and gets the post when its daemon starts again.";

/// Most earlier posts a prompt shows; older unseen ones are counted instead.
pub const MAX_CONTEXT: usize = 50;
/// Slack for the clocks of a poster and the server, when a read starts at the time a post was sent, such as a thread's
/// start.
pub const CLOCK_SLACK: Duration = Duration::from_secs(300);
/// Most thread origins kept in memory; past it they are read again.
const MAX_ORIGINS: usize = 4096;
/// Most ants, the sender of a message among them, whose link to a channel thread is checked for it.
const MAX_LINK_CHECKS: usize = 32;

/// The channels in the bucket, kept current by a watch. An entry whose channel disagrees with its key is skipped, and
/// so is a malformed one: neither is a channel.
#[derive(Default)]
pub struct Channels {
    entries: HashMap<ChannelId, ChannelEntry>,
    kv: Option<kv::Store>,
    updates: Option<kv::Watch>,
}

impl Channels {
    /// The channels in `kv`, watched for changes. The watch starts first, so no change made while the current entries
    /// are read is missed.
    pub async fn watch(kv: kv::Store) -> Result<Self> {
        let updates = kv.watch_all().await.context("watching the channels")?;
        let mut channels = Self { entries: HashMap::new(), kv: Some(kv.clone()), updates: Some(updates) };
        let keys: Vec<String> = kv.keys().await.context("listing the channels")?.try_collect().await?;
        for key in keys {
            let value = kv.get(&key).await.with_context(|| format!("reading channel {key}"))?;
            channels.set(&key, value.as_deref());
        }
        Ok(channels)
    }

    pub fn get(&self, channel: &ChannelId) -> Option<&ChannelEntry> {
        self.entries.get(channel)
    }

    /// Read the entry of `channel` again, for a post that may have overtaken a change of it, as a post made right after
    /// its author was added to the channel does.
    pub async fn refresh(&mut self, channel: &ChannelId) {
        let Some(kv) = &self.kv else { return };
        match kv.get(channel.as_str()).await {
            Ok(value) => self.set(channel.as_str(), value.as_deref()),
            Err(e) => tracing::warn!(%channel, error = %e, "reading the channel"),
        }
    }

    /// Apply the next change of the bucket. Never resolves when nothing is watched.
    pub async fn changed(&mut self) {
        let Some(updates) = &mut self.updates else { return std::future::pending().await };
        match updates.next().await {
            Some(Ok(entry)) => {
                let value = (entry.operation == kv::Operation::Put).then_some(&entry.value[..]);
                self.set(&entry.key, value);
            }
            Some(Err(e)) => tracing::warn!(error = %e, "watching the channels"),
            None => {
                tracing::warn!("the watch of the channels ended; channel changes are seen after a restart");
                self.updates = None;
            }
        }
    }

    /// Set the channel of `key` to `value`, or remove it.
    fn set(&mut self, key: &str, value: Option<&[u8]>) {
        let Ok(channel) = key.parse::<ChannelId>() else {
            tracing::warn!(key, "skipping a channel entry under a key that is no channel id");
            return;
        };
        let Some(value) = value else {
            self.entries.remove(&channel);
            return;
        };
        let entry = serde_json::from_slice::<ChannelEntry>(value)
            .map_err(|e| e.to_string())
            .and_then(|entry| entry.check_key(key).map(|()| entry).map_err(|e| e.to_string()));
        match entry {
            Ok(entry) => {
                self.entries.insert(channel, entry);
            }
            Err(error) => {
                tracing::warn!(key, error, "skipping a channel entry");
                self.entries.remove(&channel);
            }
        }
    }
}

/// What is stored under a channel's key.
pub enum Stored {
    Missing,
    /// An entry for the key's channel, at a revision.
    Entry(ChannelEntry, u64),
    /// A malformed entry, or one naming another channel, at a revision: readers skip it.
    Unreadable(u64),
}

/// Read the entry of `channel` from the channels bucket `kv`, with its revision.
pub async fn load(kv: &kv::Store, channel: &ChannelId) -> Result<Stored> {
    let key = channel.as_str();
    let Some(found) = kv.entry(key).await.with_context(|| format!("reading channel {channel}"))? else {
        return Ok(Stored::Missing);
    };
    if found.operation != kv::Operation::Put {
        return Ok(Stored::Missing);
    }
    let entry = serde_json::from_slice::<ChannelEntry>(&found.value)
        .map_err(|e| e.to_string())
        .and_then(|entry| entry.check_key(key).map(|()| entry).map_err(|e| e.to_string()));
    match entry {
        Ok(entry) => Ok(Stored::Entry(entry, found.revision)),
        Err(error) => {
            tracing::warn!(key, error, "skipping a channel entry that is malformed or names another channel");
            Ok(Stored::Unreadable(found.revision))
        }
    }
}

/// Every channel in the channels bucket `kv`, by id. Entries readers skip are left out with a warning.
pub async fn list(kv: &kv::Store) -> Result<Vec<ChannelEntry>> {
    let mut keys: Vec<String> = kv.keys().await.context("listing the channels")?.try_collect().await?;
    keys.sort();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let Ok(channel) = key.parse::<ChannelId>() else {
            tracing::warn!(key, "skipping a channel entry under a key that is no channel id");
            continue;
        };
        if let Stored::Entry(entry, _) = load(kv, &channel).await? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

/// What the prompt of a post in `channel` adds when the ant owns the channel.
pub fn owner_note(channel: &ChannelId) -> String {
    format!(
        "[ants] This ant owns #{channel}: the manage_channel tool adds and removes its members and sets its messenger."
    )
}

/// What a channel post asks of an ant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wake {
    /// Nothing: it is acked without a prompt, and shown with the ant's next prompt on its thread.
    Skip,
    /// An answer: the post @mentions the ant, a member.
    Mentioned,
    /// A triage: the post is a human's or a webhook's, mentions nobody, and the ant is the channel's messenger.
    Triage,
}

/// What `post` asks of `ant`, given the entry of its channel and the `humans`: a prompt only when it @mentions the
/// ant, a member, or when the ant is the channel's messenger and it is a post of a human or a webhook that mentions
/// nobody;
/// never for the ant's own.
pub fn wake(ant: &UserId, entry: Option<&ChannelEntry>, post: &ChannelPost, humans: &Humans) -> Wake {
    let Some(entry) = entry else { return Wake::Skip };
    if post.from == *ant || !entry.woken(post, humans).contains(ant) {
        Wake::Skip
    } else if post.mentions.is_empty() {
        Wake::Triage
    } else {
        Wake::Mentioned
    }
}

/// `post` as an inbox message from its author to `ant`, owned by the thread's owner the post carries, and created by
/// its creator, which the daemon took from the thread's posts ([`Threads::origin`]): what the attempt markers, caps,
/// escalations and tools of the thread go by. A webhook's post stays a webhook's ([`Envelope::webhook`]), and the post's
/// tool rules stay its ([`Envelope::allow`]).
pub fn as_envelope(post: &ChannelPost, ant: &UserId) -> Envelope {
    Envelope {
        id: post.id,
        thread_id: post.thread_id.clone(),
        from: post.from.clone(),
        to: ant.clone(),
        text: post.text.clone(),
        turn_budget: post.turn_budget,
        in_reply_to: None,
        owner: post.owner.clone(),
        creator: post.creator.clone(),
        origin_thread: None,
        escalation: None,
        approval: None,
        webhook: post.webhook,
        allow: post.allow.clone(),
        mention: None,
        sent_at: post.sent_at,
    }
}

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
}

/// Picks the context of a prompt from the posts of a channel, read in stream order.
struct Collect<'a> {
    ant: &'a UserId,
    humans: &'a Humans,
    thread: &'a ThreadId,
    /// Stream sequence of the post being answered: the context ends before it, and the later posts start after it.
    at: u64,
    posts: VecDeque<ChannelPost>,
    skipped: usize,
    later: Vec<ChannelPost>,
}

impl<'a> Collect<'a> {
    fn new(ant: &'a UserId, humans: &'a Humans, thread: &'a ThreadId, at: u64) -> Self {
        Self { ant, humans, thread, at, posts: VecDeque::new(), skipped: 0, later: Vec::new() }
    }

    /// Take the post at stream sequence `seq`.
    fn push(&mut self, seq: u64, post: ChannelPost) {
        if post.thread_id != *self.thread || seq == self.at {
            return;
        }
        if seq > self.at {
            // A human's post gives the thread a fresh budget: the ones before it no longer count.
            if post.by_human(self.humans) {
                self.later.clear();
            }
            self.later.push(post);
            return;
        }
        if post.from == *self.ant {
            return;
        }
        if self.posts.len() == MAX_CONTEXT {
            self.posts.pop_front();
            self.skipped += 1;
        }
        self.posts.push_back(post);
    }

    fn done(self) -> Context {
        Context { posts: self.posts.into(), skipped: self.skipped, later: self.later }
    }
}

/// Takes the posts of a channel, read in stream order.
trait Sink {
    /// Take the post at stream sequence `seq`, stored at `stored`; a break ends the read.
    fn take(&mut self, seq: u64, stored: SystemTime, post: ChannelPost) -> ControlFlow<()>;
}

impl Sink for Collect<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        self.push(seq, post);
        ControlFlow::Continue(())
    }
}

/// Folds the posts of one thread, in order, into its [`ThreadOrigin`] ([`OriginFold::resolve`]): as the posts say,
/// unless the author of the top-level post, no human, marked it as a webhook's and has no webhook marker
/// ([`Webhooks`]); then as if only humans' posts said they were webhooks'.
pub struct OriginFold {
    origin: ThreadOrigin,
    unmarked: ThreadOrigin,
    /// The author of the thread's top-level post, when no human and the post is marked as a webhook's.
    claimed: Option<UserId>,
}

impl OriginFold {
    /// A fold going on from `origin`, that of the posts before.
    pub fn new(origin: ThreadOrigin) -> Self {
        Self { unmarked: origin.clone(), origin, claimed: None }
    }

    /// Take `post`, the next post stored on the thread.
    pub fn push(&mut self, post: ChannelPost, humans: &Humans) {
        let rooted = self.origin.root;
        self.origin.push(&post, humans);
        let human = humans.contains(&post.from);
        if !rooted && self.origin.root && post.webhook && !human {
            self.claimed = Some(post.from.clone());
        }
        self.unmarked.push(&ChannelPost { webhook: post.webhook && human, ..post }, humans);
    }

    /// Whether later posts change nothing.
    pub fn settled(&self) -> bool {
        self.origin.settled() && self.unmarked.settled()
    }

    /// The origin of `thread`, the top-level post's mark as a webhook's checked against its author's marker.
    pub async fn resolve(self, thread: &ThreadId, webhooks: &Webhooks) -> ThreadOrigin {
        match self.claimed {
            Some(author) if !webhooks.is_webhook(&author).await => {
                tracing::warn!(
                    %thread,
                    %author,
                    "taking a top-level post marked as a webhook's for an ant's: the author has no webhook marker"
                );
                self.unmarked
            }
            _ => self.origin,
        }
    }
}

/// Folds the posts of one thread, up to stream sequence `upto`, into its [`ThreadOrigin`] ([`OriginFold`]).
struct Origin<'a> {
    thread: &'a ThreadId,
    humans: &'a Humans,
    upto: u64,
    fold: OriginFold,
}

impl Sink for Origin<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq > self.upto {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread {
            self.fold.push(post, self.humans);
        }
        if self.fold.settled() { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    }
}

/// Finds the channel of the top-level post that starts `thread`, among the posts of every channel stored up to stream
/// sequence `upto` and before `until`.
struct Root<'a> {
    thread: &'a ThreadId,
    upto: u64,
    until: SystemTime,
    channel: Option<ChannelId>,
}

impl Sink for Root<'_> {
    fn take(&mut self, seq: u64, stored: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq > self.upto || stored > self.until {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread && post.starts_thread() {
            self.channel = Some(post.channel);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Finds whether a post on `thread`, stored up to stream sequence `upto`, is by `sender` or wakes it, a member or the
/// messenger of the thread's channel, whose `entry` it is.
#[derive(Clone, Copy)]
struct Part<'a> {
    thread: &'a ThreadId,
    sender: &'a UserId,
    entry: &'a ChannelEntry,
    humans: &'a Humans,
    upto: u64,
    found: bool,
}

impl Sink for Part<'_> {
    fn take(&mut self, seq: u64, _: SystemTime, post: ChannelPost) -> ControlFlow<()> {
        if seq > self.upto {
            return ControlFlow::Break(());
        }
        if post.thread_id == *self.thread
            && (post.from == *self.sender || self.entry.woken(&post, self.humans).contains(self.sender))
        {
            self.found = true;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// The prompt for `post`, which @mentions `ant`, after the `context` of its thread. `left` is the turns the thread
/// keeps after the reply.
pub fn prompt(ant: &UserId, post: &ChannelPost, context: &Context, left: u32) -> String {
    let channel = &post.channel;
    let mut text = format!("[ants] Channel #{channel}, thread {}.\n\n", post.thread_id);
    text.push_str(&context_text(context));
    let _ = write!(text, "[ants] Post from {} mentioning {ant}:\n{}\n\n", post.from, post.text);
    let _ = write!(
        text,
        "[ants] The final answer of this turn is posted to #{channel} on this thread and spends one of its turns \
         ({left} left after it). An @mention in it wakes that member of the channel, or notifies it if a human; \
         mentioning nobody wakes nobody."
    );
    text
}

/// What the prompt of the escalation leader's answer on a thread of `channel` adds: the answer is posted back to the
/// thread, with the fresh turns the leader's answer gave it. `left` is the turns the thread keeps after the post.
pub fn resume_note(channel: &ChannelId, thread: &ThreadId, left: u32) -> String {
    format!(
        "[ants] The escalation's leader answered with fresh turns for thread {thread} of #{channel}. The final answer \
         of this turn is posted to #{channel} on that thread and spends one of its turns ({left} left after it). An \
         @mention in it wakes that member of the channel, or notifies it if a human; mentioning nobody wakes nobody."
    )
}

/// The prompt of the channel's messenger for `post`, a post of a human or a webhook that mentions nobody, after the `context` of
/// its thread: the [`TRIAGE_RULES`], and the members of `entry` as `roster` shows them. `left` is the turns the thread
/// keeps after the answer.
pub fn triage_prompt(
    post: &ChannelPost,
    entry: &ChannelEntry,
    roster: &[RosterEntry],
    humans: &Humans,
    context: &Context,
    left: u32,
) -> String {
    let channel = &post.channel;
    let mut text = format!("[ants] Channel #{channel}, thread {}.\n\n", post.thread_id);
    text.push_str(&context_text(context));
    let kind = if post.webhook { " (a webhook)" } else { "" };
    let _ = write!(text, "[ants] Post from {}{kind} mentioning nobody:\n{}\n\n", post.from, post.text);
    text.push_str(TRIAGE_RULES);
    let _ = write!(text, "\n\n[ants] Members of #{channel}:\n");
    if entry.members.is_empty() {
        text.push_str("(none)\n");
    }
    for member in &entry.members {
        let _ = writeln!(text, "- {}", member_line(member, roster, humans));
    }
    let _ = write!(
        text,
        "\n[ants] The final answer of this turn is posted to #{channel} on this thread and spends one of its turns \
         ({left} left after it)."
    );
    text
}

/// `member` with its role and capabilities from the roster, as a human when it is one of the `humans`, or as offline
/// when the roster lacks it.
fn member_line(member: &UserId, roster: &[RosterEntry], humans: &Humans) -> String {
    if humans.contains(member) {
        return format!("@{member} (a human)");
    }
    let Some(entry) = roster.iter().find(|e| e.ant == *member) else {
        return format!("@{member} (offline: not in the roster)");
    };
    let mut line = format!("@{member}");
    if !entry.capabilities.is_empty() {
        let _ = write!(line, " [{}]", entry.capabilities.join(", "));
    }
    if let Some(whoami) = entry.whoami.as_deref().map(str::trim).filter(|w| !w.is_empty()) {
        let _ = write!(line, ": {}", whoami.replace('\n', " "));
    }
    line
}

fn context_text(context: &Context) -> String {
    if context.posts.is_empty() {
        return String::new();
    }
    let mut text = "[ants] Earlier posts on the thread not shown before".to_owned();
    if context.skipped > 0 {
        let _ = write!(text, " ({} older ones left out)", context.skipped);
    }
    text.push_str(", oldest first:\n");
    for post in &context.posts {
        let _ = writeln!(text, "{}: {}", post.from, post.text);
    }
    text.push('\n');
    text
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
    origins: Mutex<HashMap<(ChannelId, ThreadId), (ThreadOrigin, u64)>>,
    /// The channel whose top-level post started each thread looked up so far ([`Threads::origin_of`]); None for a
    /// thread no channel post started.
    roots: Mutex<HashMap<ThreadId, Option<ChannelId>>>,
    /// Who of the authors of top-level posts marked as webhooks' are webhooks.
    webhooks: Webhooks,
    /// The senders found taking part in each thread so far ([`Threads::origin_for`]).
    parts: Mutex<HashSet<(ThreadId, UserId)>>,
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
            origins: Mutex::default(),
            roots: Mutex::default(),
            webhooks: Webhooks::new(js.clone()),
            parts: Mutex::default(),
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
    /// the link on to in turn ([`links::passed_on`]). None otherwise: no ant names the owner of a message thread it has
    /// no link to.
    pub async fn owner_for(&self, thread: &ThreadId, sender: &UserId) -> Result<Option<UserId>> {
        let recorded = async |ant: &UserId| self.owners.recorded(thread, ant).await;
        if let Some(owner) = recorded(sender).await? {
            return Ok(Some(owner));
        }
        self.passed_to(thread, sender, recorded).await
    }

    /// The origin of `thread`, at stream sequence `seq`, as [`Threads::origin_of`] reads it, when `sender` has a link
    /// to it: it takes part in the thread ([`Threads::takes_part`]), or an ant that does passed the link on to it,
    /// directly or through ants it passed the link on to in turn ([`links::passed_on`]). None otherwise: no ant links
    /// its thread to a channel thread it has no link to, to take that thread's creator.
    pub async fn origin_for(
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
        if self.parts.locked().contains(&(thread.clone(), sender.clone())) {
            return Ok(true);
        }
        self.scan(&subject::chan_room(channel), from_start(thread), &mut part)
            .await
            .with_context(|| format!("reading thread {thread} of #{channel} for posts of {sender}"))?;
        if part.found {
            let mut parts = self.parts.locked();
            if parts.len() >= MAX_ORIGINS {
                parts.clear();
            }
            parts.insert((thread.clone(), sender.clone()));
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
    async fn root(&self, thread: &ThreadId, seq: u64) -> Result<Option<ChannelId>> {
        let Some(start) = thread_start(thread) else { return Ok(None) };
        if let Some(channel) = self.roots.locked().get(thread).cloned() {
            return Ok(channel);
        }
        let mut root = Root { thread, upto: seq, until: start + 2 * CLOCK_SLACK, channel: None };
        let from = consumer::DeliverPolicy::ByStartTime { start_time: start.into() };
        self.scan(subject::ALL_CHANNELS, from, &mut root)
            .await
            .with_context(|| format!("looking for the top-level post of thread {thread}"))?;
        let mut roots = self.roots.locked();
        if roots.len() >= MAX_ORIGINS {
            roots.clear();
        }
        roots.insert(thread.clone(), root.channel.clone());
        Ok(root.channel)
    }

    /// The origin of the thread of `post`, at stream sequence `seq`: who started it, as the posts stored on it up to
    /// `post` say ([`ThreadOrigin`]), not the owner and creator `post` carries. A top-level post is its own, its mark
    /// as a webhook's checked already; the posts of any other thread are read from its start once, then on from where
    /// the last read of it ended. A top-level post marked as a webhook's counts as one only from a human or a webhook
    /// ([`Webhooks`]).
    pub async fn origin(&self, post: &ChannelPost, seq: u64, humans: &Humans) -> Result<ThreadOrigin> {
        if post.starts_thread() {
            return Ok(ThreadOrigin::of_root(post, humans));
        }
        let key = (post.channel.clone(), post.thread_id.clone());
        let cached = self.origins.locked().get(&key).cloned();
        let (origin, start) = match cached {
            Some((origin, read)) if origin.settled() || read >= seq => return Ok(origin),
            Some((origin, read)) => (origin, consumer::DeliverPolicy::ByStartSequence { start_sequence: read + 1 }),
            None => (ThreadOrigin::default(), from_start(&post.thread_id)),
        };
        let mut fold = Origin { thread: &post.thread_id, humans, upto: seq, fold: OriginFold::new(origin) };
        self.scan(&subject::chan_room(&post.channel), start, &mut fold)
            .await
            .with_context(|| format!("reading the origin of thread {} of #{}", post.thread_id, post.channel))?;
        let origin = fold.fold.resolve(&post.thread_id, &self.webhooks).await;
        let mut origins = self.origins.locked();
        if origins.len() >= MAX_ORIGINS {
            origins.clear();
        }
        origins.insert(key, (origin.clone(), seq));
        Ok(origin)
    }

    /// The posts on the thread of `post`, at stream sequence `seq`, stored after it; with [`Earlier::Unseen`], also those before
    /// it that the ant has not been shown: after the last one shown, or since the thread started, without its own.
    /// The posts of `humans` renew the thread's turns. The read ends at the last post stored as it starts.
    pub async fn context(&self, post: &ChannelPost, seq: u64, earlier: Earlier, humans: &Humans) -> Result<Context> {
        let mut collect = Collect::new(&self.ant, humans, &post.thread_id, seq);
        let after = consumer::DeliverPolicy::ByStartSequence { start_sequence: seq + 1 };
        let seen = if earlier == Earlier::Unseen && !post.starts_thread() {
            Some(self.last_seen(&post.thread_id).await?)
        } else {
            None
        };
        let start = match seen {
            None => after,
            Some(Some(seen)) if seen + 1 >= seq => after,
            Some(Some(seen)) => consumer::DeliverPolicy::ByStartSequence { start_sequence: seen + 1 },
            Some(None) => from_start(&post.thread_id),
        };
        self.scan(&subject::chan_room(&post.channel), start, &mut collect)
            .await
            .with_context(|| format!("reading thread {} of #{}", post.thread_id, post.channel))?;
        Ok(collect.done())
    }

    /// Read the channel posts on `filter` from `start` into `sink`, up to the last one stored as the read starts.
    async fn scan(&self, filter: &str, start: consumer::DeliverPolicy, sink: &mut impl Sink) -> Result<()> {
        let _one = match self.reading {
            Reading::Consumer => Some(self.one_read.lock().await),
            Reading::Spare | Reading::Gets => None,
        };
        read_back(&self.js, &self.ant, self.reading, filter, start, |seq, stored, subject, post| match post {
            Ok(post) => sink.take(seq, stored, post),
            Err(error) => {
                tracing::debug!(subject, error, "dropping a channel post");
                ControlFlow::Continue(())
            }
        })
        .await
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
}

impl Threads {
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
    crate::reading::read_back(js, &chan_reader(reader), reading, filter, start, |seq, stored, subject, payload| {
        take(seq, stored, subject, parse_post(subject, payload))
    })
    .await
}

fn parse_post(subject: &str, payload: &[u8]) -> Result<ChannelPost, String> {
    let post = serde_json::from_slice::<ChannelPost>(payload).map_err(|e| e.to_string())?;
    post.check_subject(subject).map_err(|e| e.to_string())?;
    Ok(post)
}

/// Where a read of `thread` from its start begins: at [`thread_start`], else at the channel's oldest post kept.
fn from_start(thread: &ThreadId) -> consumer::DeliverPolicy {
    match thread_start(thread) {
        Some(time) => consumer::DeliverPolicy::ByStartTime { start_time: time.into() },
        None => consumer::DeliverPolicy::All,
    }
}

/// When `thread` started, a little early, if its id is a time-ordered UUID as a top-level post's is.
pub fn thread_start(thread: &ThreadId) -> Option<SystemTime> {
    let (secs, nanos) = Uuid::parse_str(thread.as_str()).ok()?.get_timestamp()?.to_unix();
    (SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)).checked_sub(CLOCK_SLACK)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn ops() -> ChannelEntry {
        let mut entry = ChannelEntry::new("ops".parse().unwrap());
        entry.add_member(p("dev-a"));
        entry.add_member(p("dev-b"));
        entry
    }

    /// The humans: `human`, and alice, from the humans bucket.
    fn humans() -> Humans {
        Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))])
    }

    fn wakes(ant: &UserId, entry: Option<&ChannelEntry>, post: &ChannelPost) -> Wake {
        wake(ant, entry, post, &humans())
    }

    fn post(from: &str, text: &str) -> ChannelPost {
        ChannelPost::new("ops".parse().unwrap(), p(from), text)
    }

    #[test]
    fn only_a_post_mentioning_a_member_wakes_it() {
        let (entry, a) = (ops(), p("dev-a"));
        assert_eq!(wakes(&a, Some(&entry), &post("human", "@dev-a deploy")), Wake::Mentioned);
        assert_eq!(wakes(&a, Some(&entry), &post("dev-b", "@dev-b @dev-a check")), Wake::Mentioned, "ants too");
        assert_eq!(wakes(&a, Some(&entry), &post("human", "@dev-b deploy")), Wake::Skip, "another member");
        assert_eq!(wakes(&a, Some(&entry), &post("human", "deploy")), Wake::Skip, "nobody mentioned");
        assert_eq!(wakes(&a, Some(&entry), &post("dev-a", "@dev-a note to self")), Wake::Skip, "its own post");
        assert_eq!(wakes(&a, None, &post("human", "@dev-a deploy")), Wake::Skip, "no such channel");
        assert_eq!(wakes(&p("dev-c"), Some(&entry), &post("human", "@dev-c deploy")), Wake::Skip, "no member");
        let other = ChannelPost::new("dev".parse().unwrap(), UserId::human(), "@dev-a deploy");
        assert_eq!(wakes(&a, Some(&entry), &other), Wake::Skip, "another channel's entry");
    }

    #[test]
    fn the_messenger_triages_the_human_posts_that_mention_nobody() {
        let (mut entry, messenger) = (ops(), UserId::messenger());
        let untagged = post("human", "the build is red");
        assert_eq!(wakes(&messenger, Some(&entry), &untagged), Wake::Triage, "the default messenger, no member");
        assert_eq!(wakes(&p("dev-a"), Some(&entry), &untagged), Wake::Skip, "members wait for its mentions");
        assert_eq!(wakes(&messenger, Some(&entry), &post("dev-a", "the build is red")), Wake::Skip, "ants' posts");
        assert_eq!(wakes(&messenger, Some(&entry), &post("human", "@dev-a the build")), Wake::Skip, "mentions");
        assert_eq!(wakes(&messenger, Some(&entry), &post("human", "@human the build")), Wake::Skip, "mentions");
        entry.messenger = Some(p("dev-b"));
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &untagged), Wake::Triage, "the assigned one");
        assert_eq!(wakes(&messenger, Some(&entry), &untagged), Wake::Skip, "not the default then");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("human", "@dev-b look")), Wake::Mentioned);
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("alice", "the build")), Wake::Triage, "any human's");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("bob", "the build")), Wake::Skip, "bob is no human");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("dev-a", "@alice the build")), Wake::Skip, "a human");
        let hook = post("github", "@dev-a push").from_webhook(Vec::new());
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &hook), Wake::Triage, "a webhook's");
        assert_eq!(wakes(&p("dev-a"), Some(&entry), &hook), Wake::Skip, "its text mentions nobody");
    }

    #[test]
    fn the_triage_prompt_has_the_rules_and_the_members_with_their_roles() {
        let (mut entry, root) = (ops(), post("human", "the build is red"));
        entry.add_member(p("alice"));
        let now = chrono::Utc::now();
        let a = RosterEntry {
            ant: p("dev-a"),
            machine: "m".to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: vec!["rust".to_owned(), "ci".to_owned()],
            whoami: Some("Keeps the build\ngreen.".to_owned()),
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
            version: None,
            perms: None,
            signed_out: None,
            limited: None,
            failing: None,
            memory_low: None,
        };
        let context = Context { posts: vec![post("dev-b", "earlier")], ..Context::default() };
        let text = triage_prompt(&root, &entry, &[a], &humans(), &context, 7);
        let expected = format!(
            "[ants] Channel #ops, thread {}.\n\n\
             [ants] Earlier posts on the thread not shown before, oldest first:\ndev-b: earlier\n\n\
             [ants] Post from human mentioning nobody:\nthe build is red\n\n\
             {TRIAGE_RULES}\n\n\
             [ants] Members of #ops:\n\
             - @dev-a [rust, ci]: Keeps the build green.\n\
             - @dev-b (offline: not in the roster)\n\
             - @alice (a human)\n\n\
             [ants] The final answer of this turn is posted to #ops on this thread and spends one of its turns (7 \
             left after it).",
            root.thread_id
        );
        assert_eq!(text, expected);
    }

    #[test]
    fn a_post_reads_as_a_message_from_its_author_on_its_thread() {
        let mut ask = post("dev-b", "@dev-a check");
        ask.turn_budget = 3;
        let envelope = as_envelope(&ask, &p("dev-a"));
        assert_eq!((envelope.id, &envelope.thread_id, envelope.turn_budget), (ask.id, &ask.thread_id, 3));
        assert_eq!((envelope.from, envelope.to, envelope.owner), (p("dev-b"), p("dev-a"), None));
        assert_eq!(envelope.text, ask.text);
        let owned = ask.with_owner(Some(p("alice")));
        assert_eq!(as_envelope(&owned, &p("dev-a")).owner, Some(p("alice")), "the thread's owner gets its escalations");
    }

    #[test]
    fn the_context_is_the_latest_posts_of_the_thread_by_others_before_the_post() {
        let (a, root, humans) = (p("dev-a"), post("human", "deploy"), humans());
        let on = |from: &str, text: &str| {
            ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p(from), text)
        };
        let mut collect = Collect::new(&a, &humans, &root.thread_id, 1000);
        collect.push(1, root.clone());
        collect.push(2, post("human", "another thread"));
        collect.push(3, on("dev-a", "its own"));
        for i in 0..MAX_CONTEXT {
            collect.push(4 + i as u64, on("dev-b", &format!("note {i}")));
        }
        collect.push(1000, on("human", "@dev-a the post itself"));
        let context = collect.done();
        assert_eq!(context.skipped, 1, "the root is the oldest, left out");
        assert_eq!(context.posts.len(), MAX_CONTEXT);
        assert_eq!(context.posts[0].text, "note 0");
        assert!(context.posts.iter().all(|p| p.thread_id == root.thread_id && p.from.as_str() == "dev-b"));
        assert!(context.later.is_empty());
    }

    #[test]
    fn the_later_posts_are_the_thread_s_since_the_post_or_the_human_s_last() {
        let (a, root, humans) = (p("dev-a"), post("human", "deploy"), humans());
        let on = |from: &str, text: &str| {
            ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p(from), text)
        };
        let mut collect = Collect::new(&a, &humans, &root.thread_id, 5);
        collect.push(5, root.clone());
        collect.push(6, on("dev-b", "first"));
        collect.push(7, post("human", "another thread"));
        collect.push(8, on("dev-a", "its own counts too"));
        let later = |c: &Collect| c.later.iter().map(|p| p.text.clone()).collect::<Vec<_>>();
        assert_eq!(later(&collect), ["first", "its own counts too"]);
        collect.push(9, on("human", "go on"));
        collect.push(10, on("dev-c", "then"));
        assert_eq!(later(&collect), ["go on", "then"], "the human's post resets the budget");
        collect.push(11, on("alice", "and on"));
        assert_eq!(later(&collect), ["and on"], "any human's does");
        assert!(collect.done().posts.is_empty());
    }

    #[test]
    fn the_prompt_shows_the_thread_then_the_post_and_where_the_reply_goes() {
        let root = post("human", "deploy friday?");
        let ask = ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p("dev-b"), "@dev-a agree?");
        let context = Context { posts: vec![root.clone()], skipped: 2, ..Context::default() };
        let text = prompt(&p("dev-a"), &ask, &context, 6);
        let expected = format!(
            "[ants] Channel #ops, thread {}.\n\n\
             [ants] Earlier posts on the thread not shown before (2 older ones left out), oldest first:\n\
             human: deploy friday?\n\n\
             [ants] Post from dev-b mentioning dev-a:\n@dev-a agree?\n\n\
             [ants] The final answer of this turn is posted to #ops on this thread and spends one of its turns (6 \
             left after it). An @mention in it wakes that member of the channel, or notifies it if a human; \
             mentioning nobody wakes nobody.",
            root.thread_id
        );
        assert_eq!(text, expected);
        let first = prompt(&p("dev-a"), &root, &Context::default(), 7);
        assert!(first.starts_with(&format!("[ants] Channel #ops, thread {}.\n\n[ants] Post from", root.thread_id)));
    }

    #[test]
    fn entries_under_another_key_or_malformed_are_no_channels() {
        let mut channels = Channels::default();
        let entry = ops();
        let json = serde_json::to_vec(&entry).unwrap();
        channels.set("ops", Some(&json));
        assert_eq!(channels.get(&entry.channel), Some(&entry));
        channels.set("dev", Some(&json));
        assert_eq!(channels.get(&"dev".parse().unwrap()), None, "an entry of ops under dev");
        channels.set("ops", Some(b"{"));
        assert_eq!(channels.get(&entry.channel), None, "malformed: the channel is gone");
        channels.set("ops", Some(&json));
        channels.set("ops", None);
        assert_eq!(channels.get(&entry.channel), None, "deleted");
    }

    #[test]
    fn a_thread_named_after_a_post_starts_at_the_post() {
        let root = post("human", "x");
        let (start, sent) = (thread_start(&root.thread_id).unwrap(), SystemTime::from(root.sent_at));
        assert!(start <= sent && sent.duration_since(start).unwrap() <= CLOCK_SLACK * 2);
        assert_eq!(thread_start(&"t1".parse().unwrap()), None);
    }
}
