//! Channels as humans run them: their entries in the KV bucket `ants_channels` (`ants channel`), posts to them as the
//! CLI's human (`ants post`), and reading them back by thread (`ants channel read`).
//!
//! A channel thread's owner is the human whose post started it. A human's top-level post owns its thread. A post on an
//! existing thread (`--thread`) keeps the owner the thread's first posts name ([`thread_origin`]); only when none is
//! known, as on a thread no longer kept or unreachable, does the posting human take it. A thread an ant starts with
//! `ants post` under its own credentials has the ant as its creator, which later posts keep ([`thread_origin`]).
//!
//! Members are ants and humans. A human's post that @mentions a human member puts a notice in that human's inbox
//! ([`notify_mentioned`]); an ant's daemon sends those of the ant's posts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_daemon::bus;
pub use ants_daemon::channel::list;
use ants_daemon::channel::{CLOCK_SLACK, OriginFold, Reading, Stored, load, read_back};
use ants_daemon::webhooks::Webhooks;
use ants_proto::{ChannelEntry, ChannelId, ChannelPost, Humans, ThreadId, ThreadOrigin, UserId, Uuid, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::kv::{self, UpdateErrorKind};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;

use crate::humans::Me;

/// Attempts at a compare-and-set update before giving up on a channel that keeps changing.
const ATTEMPTS: usize = 10;

/// A channel id as given on the command line: `#ops` or `ops`.
pub fn parse_channel(s: &str) -> Result<ChannelId, ants_proto::Error> {
    s.trim_prefix('#').parse()
}

/// A user id as given on the command line, an ant's or a human's: `@dev-a` or `dev-a`; never the channel manager.
pub fn parse_ant(s: &str) -> Result<UserId, String> {
    let ant: UserId = s.trim_prefix('@').parse().map_err(|e: ants_proto::Error| e.to_string())?;
    if ant == UserId::channel_manager() {
        return Err(format!("{ant} is the channel manager, not an ant"));
    }
    Ok(ant)
}

/// The messenger is an ant a daemon serves, as it triages with an agent; `humans` may be members instead.
pub fn check_messenger_not_human(messenger: Option<&UserId>, humans: &Humans) -> Result<()> {
    if let Some(human) = messenger.filter(|m| humans.contains(m)) {
        bail!(
            "{human} is a human, so not the messenger of a channel, which is an ant; `--member {human}` adds a member"
        );
    }
    Ok(())
}

/// A channel's owner is an ant: `humans` override any owner anyway.
pub fn check_owner_not_human(owner: Option<&UserId>, humans: &Humans) -> Result<()> {
    if let Some(human) = owner.filter(|o| humans.contains(o)) {
        bail!("{human} is a human, who needs no ownership: humans change every channel");
    }
    Ok(())
}

/// The ant that triages humans' posts that @mention nobody, and whether it is the default one.
pub fn messenger(entry: &ChannelEntry) -> (UserId, bool) {
    match &entry.messenger {
        Some(m) => (m.clone(), false),
        None => (UserId::messenger(), true),
    }
}

fn messenger_label(entry: &ChannelEntry) -> String {
    match messenger(entry) {
        (m, true) => format!("{m} (default)"),
        (m, false) => m.to_string(),
    }
}

fn members_label(entry: &ChannelEntry) -> String {
    if entry.members.is_empty() {
        "-".to_owned()
    } else {
        entry.members.iter().map(UserId::as_str).collect::<Vec<_>>().join(",")
    }
}

/// One line describing a channel, as `ants channel` prints it after a change.
pub fn describe(entry: &ChannelEntry) -> String {
    let owner = entry.owner.as_ref().map_or_default(|o| format!("; owner {o}"));
    format!("#{}: members {}; messenger {}{owner}", entry.channel, members_label(entry), messenger_label(entry))
}

/// The channels as a table: one row per channel under a header, columns padded to line up. With `unread`, an UNREAD
/// column shows the unread posts of each channel it has, and how many of them @mention the human, `-` for the others.
pub fn table(entries: &[ChannelEntry], unread: Option<&HashMap<ChannelId, reads::Unread>>) -> String {
    let mut header = ["CHANNEL", "MEMBERS", "MESSENGER", "OWNER"].map(str::to_owned).to_vec();
    if unread.is_some() {
        header.push("UNREAD".to_owned());
    }
    let owner = |e: &ChannelEntry| e.owner.as_ref().map_or_else(|| "-".to_owned(), UserId::to_string);
    let unread_label = |e: &ChannelEntry| match unread?.get(&e.channel) {
        None => Some("-".to_owned()),
        Some(u) if u.mentions > 0 => Some(format!("{} (@{})", u.posts, u.mentions)),
        Some(u) => Some(u.posts.to_string()),
    };
    let rows: Vec<Vec<String>> = std::iter::once(header)
        .chain(entries.iter().map(|e| {
            let row = [format!("#{}", e.channel), members_label(e), messenger_label(e), owner(e)];
            row.into_iter().chain(unread_label(e)).collect()
        }))
        .collect();
    let mut widths = vec![0; rows[0].len()];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().zip(&widths).map(|(cell, width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}

/// The users `post` @mentions, other than its author, that it neither wakes nor notifies, not being members of the
/// channel.
pub fn idle_mentions<'a>(post: &'a ChannelPost, entry: Option<&ChannelEntry>) -> Vec<&'a UserId> {
    post.mentions.iter().filter(|m| **m != post.from && entry.is_none_or(|e| !e.is_member(m))).collect()
}

/// Put the notice of `post`, a human's, in the inbox of each human member of `entry` it @mentions
/// ([`ChannelPost::mention_notice`]), with the msg id that stores it once. Returns the humans notified, and warns of
/// those not.
pub async fn notify_mentioned(
    js: &jetstream::Context,
    post: &ChannelPost,
    entry: Option<&ChannelEntry>,
    humans: &Humans,
) -> Vec<UserId> {
    let mut notified = Vec::new();
    for human in entry.map_or_default(|entry| entry.notified(post, humans)) {
        let notice = post.mention_notice(&human);
        match bus::store(js, notice.subject(), &notice, &post.mention_msg_id(&human)).await {
            Ok(_) => notified.push(human),
            Err(e) => tracing::warn!("{human} was not notified of the mention: {e:#}"),
        }
    }
    notified
}

/// The channels bucket, created if missing (the human's credentials may).
pub async fn open(js: &jetstream::Context) -> Result<kv::Store> {
    bus::ensure_channels(js).await
}

/// The entry of `channel`, None when there is none (or only one readers skip).
pub async fn get(kv: &kv::Store, channel: &ChannelId) -> Result<Option<ChannelEntry>> {
    Ok(match load(kv, channel).await? {
        Stored::Entry(entry, _) => Some(entry),
        Stored::Missing | Stored::Unreadable(_) => None,
    })
}

fn encode(entry: &ChannelEntry) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(entry)?)
}

/// Store a new channel; fails when it exists.
pub async fn create(kv: &kv::Store, entry: &ChannelEntry) -> Result<()> {
    match kv.create(entry.key(), encode(entry)?.into()).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == kv::CreateErrorKind::AlreadyExists => {
            bail!("channel #{0} exists; `ants channel set {0}` changes it", entry.channel)
        }
        Err(e) => Err(e).with_context(|| format!("creating channel {}", entry.channel)),
    }
}

/// Apply `change` to the entry of `channel` and store it, unless another writer stored a newer revision in between:
/// then read it again and retry. An entry readers skip is replaced, starting from no members and no messenger. Returns
/// the stored entry and what `change` returned the last time.
pub async fn update<T>(
    kv: &kv::Store,
    channel: &ChannelId,
    mut change: impl FnMut(&mut ChannelEntry) -> T,
) -> Result<(ChannelEntry, T)> {
    for _ in 0..ATTEMPTS {
        let (mut entry, revision) = match load(kv, channel).await? {
            Stored::Missing => bail!("no channel #{channel}; `ants channel create {channel}` makes one"),
            Stored::Entry(entry, revision) => (entry, revision),
            Stored::Unreadable(revision) => {
                tracing::warn!(%channel, "replacing the unreadable entry of the channel");
                (ChannelEntry::new(channel.clone()), revision)
            }
        };
        let changed = change(&mut entry);
        match kv.update(entry.key(), encode(&entry)?.into(), revision).await {
            Ok(_) => return Ok((entry, changed)),
            Err(e) if e.kind() == UpdateErrorKind::WrongLastRevision => {
                tracing::debug!(%channel, "the channel changed meanwhile; retrying");
            }
            Err(e) => return Err(e).with_context(|| format!("updating channel {channel}")),
        }
    }
    bail!("channel #{channel} kept changing; try again")
}

/// Run the channel manager until it fails: connect with `creds`, the channel manager's, retrying until the hub
/// answers, and apply the changes owners ask for ([`ants_daemon::chanadm::serve`]). Meanwhile, publish the version of
/// ants it runs ([`ants_proto::channel_manager_serving_key`]).
pub async fn serve(url: &str, creds: Option<&std::path::Path>, domain: Option<&ants_proto::JsDomain>) -> Result<()> {
    let me = UserId::channel_manager();
    let connecting = bus::connect_retrying(url, creds, domain, &me, "ants-channel-manager", std::future::pending());
    let (client, js) = connecting.await?.context("connecting")?;
    let published = ants_daemon::direct_kv::DirectKv::new(&js, domain, ants_proto::WEBHOOKS_BUCKET).await?;
    let key = ants_proto::channel_manager_serving_key();
    let publishing = crate::versions::keep_putting(published, key, crate::versions::serving().await, || {});
    let humans = ants_daemon::humans::Registry::new(UserId::human());
    let _watching = humans.watch(js.clone(), ants_daemon::humans::RETRY);
    let roster = ants_daemon::mcp::jetstream_roster(js.clone());
    tokio::select! {
        served = ants_daemon::chanadm::serve(client, js, roster, humans) => served,
        () = publishing => unreachable!("it keeps publishing"),
    }
}

/// Remove a channel; fails when there is none. Its posts stay in the stream.
pub async fn delete(kv: &kv::Store, channel: &ChannelId) -> Result<()> {
    if matches!(load(kv, channel).await?, Stored::Missing) {
        bail!("no channel #{channel}");
    }
    kv.delete(channel.as_str()).await.with_context(|| format!("deleting channel {channel}"))
}

/// A post of `from`, owned by `owner`: a top-level one that starts a thread, or with `thread` one on that thread, in
/// reply to the post that started it when the thread is a channel thread.
pub fn human_post(
    channel: ChannelId,
    thread: Option<ThreadId>,
    from: UserId,
    owner: Option<UserId>,
    text: String,
) -> ChannelPost {
    let Some(thread) = thread else { return ChannelPost::new(channel, from, text).with_owner(owner) };
    let root = thread.as_str().parse::<Uuid>().ok();
    let mut post = ChannelPost::on_thread(channel, thread, from, text).with_owner(owner);
    post.in_reply_to = root;
    post
}

/// What `ants post` posts as `me` to `channel`, or on its `thread`. A human owns the thread it starts, and one on an
/// existing thread keeps its owner ([`thread_origin`]), or takes it when none is known; an ant's post owns nothing
/// and keeps a known owner. An ant creates the thread it starts, and a post on an existing thread keeps its creator.
pub async fn compose(
    js: &jetstream::Context,
    me: &Me,
    humans: &Humans,
    channel: ChannelId,
    thread: Option<ThreadId>,
    text: String,
) -> ChannelPost {
    let (known, creator) = match &thread {
        Some(thread) => thread_origin(js, me, &channel, thread, humans).await.unwrap_or_else(|e| {
            tracing::warn!("reading the owner of thread {thread} in #{channel}: {e:#}");
            (None, None)
        }),
        None => (None, (!me.role.is_human()).then(|| me.id.clone())),
    };
    let owner = known.or_else(|| me.role.is_human().then(|| me.id.clone()));
    human_post(channel, thread, me.id.clone(), owner, text).with_creator(creator)
}

/// The owner and creator of `thread` in `channel`, read as `reader` reads channels back ([`read_from`]): as its posts
/// still kept say ([`ThreadOrigin`] with `humans`), or None. A thread named after the post that started it is read from its
/// start; any other from the channel's oldest post kept. A top-level post marked as a webhook's counts as one only from
/// a human or a webhook ([`OriginFold`]).
pub async fn thread_origin(
    js: &jetstream::Context,
    reader: &Me,
    channel: &ChannelId,
    thread: &ThreadId,
    humans: &Humans,
) -> Result<(Option<UserId>, Option<UserId>)> {
    let start = match ants_daemon::channel::thread_start(thread) {
        Some(time) => consumer::DeliverPolicy::ByStartTime { start_time: time.into() },
        None => consumer::DeliverPolicy::All,
    };
    let mut fold = OriginFold::new(ThreadOrigin::default());
    scan(js, reader, channel, start, |_, post| {
        if post.thread_id == *thread {
            fold.push(post, humans);
        }
        if fold.settled() { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    })
    .await?;
    let origin = fold.resolve(thread, &Webhooks::new(js.clone())).await;
    Ok((origin.owner, origin.creator))
}

/// The posts among `posts` on the thread of `post`, other than `post` itself: those of others only with `by_others`.
pub fn thread_posts<S: futures::Stream<Item = ChannelPost>>(
    posts: S,
    post: &ChannelPost,
    by_others: bool,
) -> impl futures::Stream<Item = ChannelPost> + use<S> {
    let (thread, id, from) = (post.thread_id.clone(), post.id, post.from.clone());
    posts.filter(move |reply| {
        std::future::ready(reply.thread_id == thread && reply.id != id && !(by_others && reply.from == from))
    })
}

/// The posts `room`, a subscription to a channel, receives. Forged posts are dropped with a warning.
pub fn live_posts(room: async_nats::Subscriber) -> impl futures::Stream<Item = ChannelPost> {
    room.filter_map(|msg| {
        let post = match serde_json::from_slice::<ChannelPost>(&msg.payload) {
            Ok(post) => match post.check_subject(&msg.subject) {
                Ok(()) => Some(post),
                Err(e) => {
                    tracing::warn!("ignoring a forged channel post: {e}");
                    None
                }
            },
            Err(_) => None,
        };
        std::future::ready(post)
    })
}

/// How often [`caught_up_posts`] reads back what the hub's stream stored.
pub const CATCH_UP: Duration = Duration::from_secs(5);

/// How many posts [`caught_up_posts`] remembers to take each once.
const SEEN: usize = 1024;

/// The posts `room`, a subscription to the channel of `post`, receives ([`live_posts`]), and those the hub's stream
/// stores after `post`, read back every [`CATCH_UP`] as `reader` reads channels back ([`read_from`]), each once:
/// what the hub stores while this connection, or the uplink of the leaf node it goes through, is down never comes
/// live, nor does a post the hub takes from a leaf's outbox. `stored` is the stream sequence of `post`, when the hub
/// stored it; for a post kept in the machine's outbox, the read-back looks for it from [`CLOCK_SLACK`] before it was
/// sent, and takes only what the stream stores after it.
pub fn caught_up_posts(
    room: async_nats::Subscriber,
    js: &jetstream::Context,
    reader: &Me,
    post: &ChannelPost,
    stored: Option<u64>,
) -> impl futures::Stream<Item = ChannelPost> + use<> {
    let since = SystemTime::from(post.sent_at).checked_sub(CLOCK_SLACK).unwrap_or(SystemTime::UNIX_EPOCH);
    let back = ReadBack {
        js: js.clone(),
        reader: reader.clone(),
        room: subject::chan_room(&post.channel),
        own: post.id,
        since,
        after: stored,
    };
    let back = futures::stream::unfold(back, async |mut back| {
        tokio::time::sleep(CATCH_UP).await;
        let posts = back.round().await;
        Some((posts, back))
    })
    .flat_map(futures::stream::iter);
    let mut seen = Seen::default();
    futures::stream::select(live_posts(room), back).filter(move |post| std::future::ready(seen.first(post.id)))
}

/// Where [`caught_up_posts`] reads back.
struct ReadBack {
    js: jetstream::Context,
    reader: Me,
    room: String,
    /// The post waited on.
    own: Uuid,
    /// From when to look for [`ReadBack::own`] while its stream sequence is unknown.
    since: SystemTime,
    /// The stream sequence of the last post read back, from [`ReadBack::own`] on.
    after: Option<u64>,
}

impl ReadBack {
    /// The posts stored after the last one read back, or, until the post waited on shows, after it. Out of the hub's
    /// reach, none.
    async fn round(&mut self) -> Vec<ChannelPost> {
        let start = match self.after {
            Some(seq) => consumer::DeliverPolicy::ByStartSequence { start_sequence: seq + 1 },
            None => consumer::DeliverPolicy::ByStartTime { start_time: self.since.into() },
        };
        let (own, after, mut posts) = (self.own, &mut self.after, Vec::new());
        let reading = Reading::cli(self.reader.role);
        let read = read_back(&self.js, &self.reader.id, reading, &self.room, start, |seq, _, subject, post| {
            match post {
                Ok(post) if after.is_some() => posts.push(post),
                Ok(post) if post.id == own => {}
                Ok(_) => return ControlFlow::Continue(()),
                Err(error) => {
                    tracing::debug!(subject, error, "dropping a channel post read back");
                    if after.is_none() {
                        return ControlFlow::Continue(());
                    }
                }
            }
            *after = Some(seq);
            ControlFlow::Continue(())
        })
        .await;
        if let Err(e) = read {
            tracing::debug!("reading back {}: {e:#}", self.room);
        }
        posts
    }
}

/// The ids of the latest posts taken.
#[derive(Default)]
struct Seen {
    order: VecDeque<Uuid>,
    ids: HashSet<Uuid>,
}

impl Seen {
    /// Whether the post `id` is new, then remembered.
    fn first(&mut self, id: Uuid) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > SEEN
            && let Some(old) = self.order.pop_front()
        {
            self.ids.remove(&old);
        }
        true
    }
}

/// The posts of `channel` stored at stream sequence `from` or after (0: every one kept), oldest first, each with its
/// sequence, read as the CLI reads channels back as `reader` ([`Reading::cli`]): a human by message gets, an ant
/// through a free spare of its reader consumer, taken for the read and deleted after it, so any number of reads, and
/// the ant's daemon's, may run at once. Posts whose sender or channel disagrees with their subject are dropped with a
/// warning.
pub async fn read_from(
    js: &jetstream::Context,
    reader: &Me,
    channel: &ChannelId,
    from: u64,
) -> Result<Vec<(u64, ChannelPost)>> {
    let start = match from {
        0 | 1 => consumer::DeliverPolicy::All,
        from => consumer::DeliverPolicy::ByStartSequence { start_sequence: from },
    };
    let mut posts = Vec::new();
    scan(js, reader, channel, start, |seq, post| {
        posts.push((seq, post));
        ControlFlow::Continue(())
    })
    .await?;
    Ok(posts)
}

/// Hand the posts of `channel` kept in the stream from `start` on, oldest first, each with its sequence, to `take`
/// until it breaks, read as [`read_from`] does.
async fn scan(
    js: &jetstream::Context,
    reader: &Me,
    channel: &ChannelId,
    start: consumer::DeliverPolicy,
    mut take: impl FnMut(u64, ChannelPost) -> ControlFlow<()>,
) -> Result<()> {
    let reading = Reading::cli(reader.role);
    read_back(js, &reader.id, reading, &subject::chan_room(channel), start, |seq, _, subject, post| match post {
        Ok(post) => take(seq, post),
        Err(error) => {
            tracing::warn!(%channel, subject, error, "dropping a channel post");
            ControlFlow::Continue(())
        }
    })
    .await
    .with_context(|| format!("reading channel {channel}"))
}

/// The posts of one thread: the post that started it first, when the stream still has it, then the rest in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Thread {
    pub id: ThreadId,
    pub posts: Vec<ChannelPost>,
}

impl Thread {
    /// Whether the post that started the thread is among its posts.
    pub fn has_start(&self) -> bool {
        self.posts.first().is_some_and(ChannelPost::starts_thread)
    }
}

/// `posts` grouped by thread, the threads in the order their first post came.
pub fn threads(posts: Vec<ChannelPost>) -> Vec<Thread> {
    let mut threads: Vec<Thread> = Vec::new();
    for post in posts {
        match threads.iter_mut().find(|t| t.id == post.thread_id) {
            Some(thread) => thread.posts.push(post),
            None => threads.push(Thread { id: post.thread_id.clone(), posts: vec![post] }),
        }
    }
    for thread in &mut threads {
        if let Some(start) = thread.posts.iter().position(ChannelPost::starts_thread) {
            let start = thread.posts.remove(start);
            thread.posts.insert(0, start);
        }
    }
    threads
}

/// One post at `indent`: time, author and text, the text's further lines indented under it.
fn render_post(out: &mut String, post: &ChannelPost, indent: &str) {
    let time = post.sent_at.format("%Y-%m-%d %H:%M:%S");
    let mut lines = post.text.lines();
    let _ = writeln!(out, "{indent}{time} {}: {}", post.from, lines.next().unwrap_or(""));
    for line in lines {
        let _ = writeln!(out, "{indent}    {line}");
    }
}

/// Threads as `ants channel read` prints them: a header line with the thread id, the post that started it, then the
/// replies indented, and a blank line between threads.
pub fn render(threads: &[Thread]) -> String {
    let mut out = String::new();
    for (i, thread) in threads.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        let _ = writeln!(out, "thread {}", thread.id);
        let replies = if thread.has_start() {
            render_post(&mut out, &thread.posts[0], "");
            &thread.posts[1..]
        } else {
            out.push_str("(the post that started the thread is no longer kept)\n");
            &thread.posts[..]
        };
        for post in replies {
            render_post(&mut out, post, "    ");
        }
    }
    out
}

pub mod reads;
#[cfg(test)]
mod tests;
