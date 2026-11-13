//! Channels as a member's daemon sees them.
//!
//! The daemon watches the channels bucket ([`Channels`]) and takes every channel post through its consumer
//! `chan-<persona>`. A post wakes the persona only when [`wake`] says so; the rest are acked without a prompt. A prompt
//! on a channel thread shows the posts on the thread the persona has not been shown yet ([`Threads::context`]): the
//! stream sequence of the last post shown is kept per thread in the sessions bucket, so a restarted daemon goes on from
//! there. The reply is posted on the same thread, spending one of its turns.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, Envelope, PersonaId, STREAM, ThreadId, Uuid, chan_reader, channel_seen_key,
    subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer, kv};
use futures::{StreamExt as _, TryStreamExt as _};

/// Most earlier posts a prompt shows; older unseen ones are counted instead.
pub const MAX_CONTEXT: usize = 50;
/// How long a reader consumer left behind by an interrupted read lives on.
const INACTIVE: Duration = Duration::from_secs(30);
/// Messages asked for at once.
const BATCH: u64 = 256;
/// Slack for the clocks of the thread's first poster and the server, when a read starts at the thread's start.
const CLOCK_SLACK: Duration = Duration::from_secs(300);

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

/// What a channel post asks of a persona.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wake {
    /// Nothing: it is acked without a prompt, and shown with the persona's next prompt on its thread.
    Skip,
    /// An answer: the post @mentions the persona, a member.
    Mentioned,
}

/// What `post` asks of `persona`, given the entry of its channel: a prompt only when it @mentions the persona, a
/// member, and is not the persona's own.
pub fn wake(persona: &PersonaId, entry: Option<&ChannelEntry>, post: &ChannelPost) -> Wake {
    let Some(entry) = entry else { return Wake::Skip };
    if post.from == *persona || !entry.woken(post).contains(persona) {
        return Wake::Skip;
    }
    Wake::Mentioned
}

/// `post` as an inbox message from its author to `persona`: what the attempt markers, caps and tools of the thread
/// go by.
pub fn as_envelope(post: &ChannelPost, persona: &PersonaId) -> Envelope {
    Envelope {
        id: post.id,
        thread_id: post.thread_id.clone(),
        from: post.from.clone(),
        to: persona.clone(),
        text: post.text.clone(),
        turn_budget: post.turn_budget,
        in_reply_to: None,
        sent_at: post.sent_at,
    }
}

/// The earlier posts on a thread to show with a prompt.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Context {
    pub posts: Vec<ChannelPost>,
    /// Unseen posts left out, the oldest ones.
    pub skipped: usize,
}

/// Picks the context of a prompt from the posts of a channel, read in stream order.
struct Collect<'a> {
    persona: &'a PersonaId,
    thread: &'a ThreadId,
    /// Stream sequence of the post being answered: the context ends before it.
    before: u64,
    posts: VecDeque<ChannelPost>,
    skipped: usize,
}

impl<'a> Collect<'a> {
    fn new(persona: &'a PersonaId, thread: &'a ThreadId, before: u64) -> Self {
        Self { persona, thread, before, posts: VecDeque::new(), skipped: 0 }
    }

    /// Take the post at stream sequence `seq`; false once past the post being answered.
    fn push(&mut self, seq: u64, post: ChannelPost) -> bool {
        if seq >= self.before {
            return false;
        }
        if post.thread_id != *self.thread || post.from == *self.persona {
            return true;
        }
        if self.posts.len() == MAX_CONTEXT {
            self.posts.pop_front();
            self.skipped += 1;
        }
        self.posts.push_back(post);
        true
    }

    fn done(self) -> Context {
        Context { posts: self.posts.into(), skipped: self.skipped }
    }
}

/// The prompt for `post`, which @mentions `persona`, after the `context` of its thread. `left` is the turns the thread
/// keeps after the reply.
pub fn prompt(persona: &PersonaId, post: &ChannelPost, context: &Context, left: u32) -> String {
    let channel = &post.channel;
    let mut text = format!("[ants] Channel #{channel}, thread {}.\n\n", post.thread_id);
    text.push_str(&context_text(context));
    text.push_str(&format!("[ants] Post from {} mentioning {persona}:\n{}\n\n", post.from, post.text));
    text.push_str(&format!(
        "[ants] The final answer of this turn is posted to #{channel} on this thread and spends one of its turns \
         ({left} left after it). An @persona mention in it wakes that member of the channel; mentioning nobody wakes \
         nobody."
    ));
    text
}

fn context_text(context: &Context) -> String {
    if context.posts.is_empty() {
        return String::new();
    }
    let mut text = "[ants] Earlier posts on the thread not shown before".to_owned();
    if context.skipped > 0 {
        text.push_str(&format!(" ({} older ones left out)", context.skipped));
    }
    text.push_str(", oldest first:\n");
    for post in &context.posts {
        text.push_str(&format!("{}: {}\n", post.from, post.text));
    }
    text.push('\n');
    text
}

/// Reads channel threads back for a persona and keeps which posts it was shown.
pub struct Threads {
    js: jetstream::Context,
    persona: PersonaId,
    /// The sessions bucket, under the persona's own keys.
    seen: kv::Store,
    /// One read at a time: they share the reader's consumer.
    reading: tokio::sync::Mutex<()>,
}

impl Threads {
    pub fn new(js: jetstream::Context, persona: PersonaId, sessions: kv::Store) -> Self {
        Self { js, persona, seen: sessions, reading: tokio::sync::Mutex::new(()) }
    }

    /// The posts on the thread of `post`, at stream sequence `seq`, that the persona has not been shown: those after
    /// the last one shown, or since the thread started, up to `post`. Its own posts are left out.
    pub async fn context(&self, post: &ChannelPost, seq: u64) -> Result<Context> {
        let mut collect = Collect::new(&self.persona, &post.thread_id, seq);
        if post.starts_thread() {
            return Ok(collect.done());
        }
        let start = match self.last_seen(&post.thread_id).await? {
            Some(seen) if seen + 1 >= seq => return Ok(collect.done()),
            Some(seen) => consumer::DeliverPolicy::ByStartSequence { start_sequence: seen + 1 },
            None => match thread_start(&post.thread_id) {
                Some(time) => consumer::DeliverPolicy::ByStartTime { start_time: time.into() },
                None => consumer::DeliverPolicy::All,
            },
        };
        let _one = self.reading.lock().await;
        let name = chan_reader(&self.persona);
        // A consumer left by an interrupted read would carry on where it stopped.
        let _ = self.js.delete_consumer_from_stream(&name, STREAM).await;
        let config = consumer::pull::Config {
            name: Some(name.clone()),
            filter_subject: subject::chan_room(&post.channel),
            deliver_policy: start,
            ack_policy: consumer::AckPolicy::None,
            inactive_threshold: INACTIVE,
            ..Default::default()
        };
        let consumer: consumer::PullConsumer = self
            .js
            .create_consumer_on_stream(config, STREAM)
            .await
            .with_context(|| format!("reading thread {} of #{}", post.thread_id, post.channel))?;
        let read = read(&consumer, &mut collect).await;
        if let Err(e) = self.js.delete_consumer_from_stream(&name, STREAM).await {
            tracing::debug!(error = %e, consumer = name, "deleting the channel reader");
        }
        read?;
        Ok(collect.done())
    }

    /// Stream sequence of the last post on `thread` the persona was shown.
    async fn last_seen(&self, thread: &ThreadId) -> Result<Option<u64>> {
        let value =
            self.seen.get(channel_seen_key(&self.persona, thread)).await.context("reading the last post seen")?;
        Ok(value.and_then(|v| std::str::from_utf8(&v).ok()?.parse().ok()))
    }

    /// Note that the persona was shown the posts on `thread` up to stream sequence `seq`.
    pub async fn seen(&self, thread: &ThreadId, seq: u64) -> Result<()> {
        if self.last_seen(thread).await?.is_some_and(|seen| seen >= seq) {
            return Ok(());
        }
        let key = channel_seen_key(&self.persona, thread);
        self.seen.put(key, seq.to_string().into()).await.context("storing the last post seen")?;
        Ok(())
    }
}

async fn read(consumer: &consumer::PullConsumer, collect: &mut Collect<'_>) -> Result<()> {
    let mut left = consumer.cached_info().num_pending;
    while left > 0 {
        let mut batch = consumer.fetch().max_messages(left.min(BATCH) as usize).messages().await?;
        let mut got = 0;
        while let Some(msg) = batch.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
            got += 1;
            let Ok(info) = msg.info() else { continue };
            let seq = info.stream_sequence;
            let post = serde_json::from_slice::<ChannelPost>(&msg.payload)
                .map_err(|e| e.to_string())
                .and_then(|post| post.check_subject(&msg.subject).map(|()| post).map_err(|e| e.to_string()));
            match post {
                Ok(post) => {
                    if !collect.push(seq, post) {
                        return Ok(());
                    }
                }
                Err(error) => tracing::debug!(subject = %msg.subject, error, "dropping a channel post"),
            }
        }
        if got == 0 {
            break;
        }
        left = left.saturating_sub(got);
    }
    Ok(())
}

/// When `thread` started, a little early, if its id is a time-ordered UUID as a top-level post's is.
fn thread_start(thread: &ThreadId) -> Option<SystemTime> {
    let (secs, nanos) = Uuid::parse_str(thread.as_str()).ok()?.get_timestamp()?.to_unix();
    (SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)).checked_sub(CLOCK_SLACK)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PersonaId {
        s.parse().unwrap()
    }

    fn ops() -> ChannelEntry {
        let mut entry = ChannelEntry::new("ops".parse().unwrap());
        entry.add_member(p("dev-a"));
        entry.add_member(p("dev-b"));
        entry
    }

    fn post(from: &str, text: &str) -> ChannelPost {
        ChannelPost::new("ops".parse().unwrap(), p(from), text)
    }

    #[test]
    fn only_a_post_mentioning_a_member_wakes_it() {
        let (entry, a) = (ops(), p("dev-a"));
        assert_eq!(wake(&a, Some(&entry), &post("human", "@dev-a deploy")), Wake::Mentioned);
        assert_eq!(wake(&a, Some(&entry), &post("dev-b", "@dev-b @dev-a check")), Wake::Mentioned, "agents too");
        assert_eq!(wake(&a, Some(&entry), &post("human", "@dev-b deploy")), Wake::Skip, "another member");
        assert_eq!(wake(&a, Some(&entry), &post("human", "deploy")), Wake::Skip, "nobody mentioned");
        assert_eq!(wake(&a, Some(&entry), &post("dev-a", "@dev-a note to self")), Wake::Skip, "its own post");
        assert_eq!(wake(&a, None, &post("human", "@dev-a deploy")), Wake::Skip, "no such channel");
        assert_eq!(wake(&p("dev-c"), Some(&entry), &post("human", "@dev-c deploy")), Wake::Skip, "no member");
        let other = ChannelPost::new("dev".parse().unwrap(), PersonaId::human(), "@dev-a deploy");
        assert_eq!(wake(&a, Some(&entry), &other), Wake::Skip, "another channel's entry");
    }

    #[test]
    fn a_post_reads_as_a_message_from_its_author_on_its_thread() {
        let mut ask = post("dev-b", "@dev-a check");
        ask.turn_budget = 3;
        let envelope = as_envelope(&ask, &p("dev-a"));
        assert_eq!((envelope.id, &envelope.thread_id, envelope.turn_budget), (ask.id, &ask.thread_id, 3));
        assert_eq!((envelope.from, envelope.to, envelope.text), (p("dev-b"), p("dev-a"), ask.text));
    }

    #[test]
    fn the_context_is_the_latest_posts_of_the_thread_by_others_before_the_post() {
        let (a, root) = (p("dev-a"), post("human", "deploy"));
        let on = |from: &str, text: &str| {
            ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p(from), text)
        };
        let mut collect = Collect::new(&a, &root.thread_id, 1000);
        assert!(collect.push(1, root.clone()));
        assert!(collect.push(2, post("human", "another thread")));
        assert!(collect.push(3, on("dev-a", "its own")));
        for i in 0..MAX_CONTEXT {
            assert!(collect.push(4 + i as u64, on("dev-b", &format!("note {i}"))));
        }
        assert!(!collect.push(1000, on("human", "@dev-a the post itself")));
        let context = collect.done();
        assert_eq!(context.skipped, 1, "the root is the oldest, left out");
        assert_eq!(context.posts.len(), MAX_CONTEXT);
        assert_eq!(context.posts[0].text, "note 0");
        assert!(context.posts.iter().all(|p| p.thread_id == root.thread_id && p.from.as_str() == "dev-b"));
    }

    #[test]
    fn the_prompt_shows_the_thread_then_the_post_and_where_the_reply_goes() {
        let root = post("human", "deploy friday?");
        let ask = ChannelPost::on_thread(root.channel.clone(), root.thread_id.clone(), p("dev-b"), "@dev-a agree?");
        let context = Context { posts: vec![root.clone()], skipped: 2 };
        let text = prompt(&p("dev-a"), &ask, &context, 6);
        let expected = format!(
            "[ants] Channel #ops, thread {}.\n\n\
             [ants] Earlier posts on the thread not shown before (2 older ones left out), oldest first:\n\
             human: deploy friday?\n\n\
             [ants] Post from dev-b mentioning dev-a:\n@dev-a agree?\n\n\
             [ants] The final answer of this turn is posted to #ops on this thread and spends one of its turns (6 \
             left after it). An @persona mention in it wakes that member of the channel; mentioning nobody wakes \
             nobody.",
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
