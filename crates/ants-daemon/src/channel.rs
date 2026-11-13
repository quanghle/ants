//! Channels as a member's daemon sees them.
//!
//! The daemon watches the channels bucket ([`Channels`]) and takes every channel post through its consumer
//! `chan-<persona>`. A post wakes the persona only when [`wake`] says so; the rest are acked without a prompt. A prompt
//! on a channel thread shows the posts on the thread the persona has not been shown yet ([`Threads::context`]): the
//! stream sequence of the last post shown is kept per thread in the sessions bucket, so a restarted daemon goes on from
//! there. The reply is posted on the same thread, spending one of its turns.
//!
//! A post of a human that @mentions nobody goes to the channel's messenger instead, to triage: its answer @mentions
//! the members the post is relevant to, which wakes them ([`triage_prompt`]). Who is a human is the daemon's
//! [`crate::humans::Registry`]'s to say.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime};

use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, Envelope, Humans, PersonaId, RosterEntry, STREAM, ThreadId, Uuid,
    chan_reader, channel_seen_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer, kv};
use futures::{StreamExt as _, TryStreamExt as _};

/// What ants tells a channel's messenger about triaging a post; the messenger's own instructions add to it.
pub const TRIAGE_RULES: &str = "[ants] Triage this post; do not answer or act on the request itself.
1. Read the post, and the thread before it.
2. Decide which members of the channel below the post is relevant to, by their roles and capabilities.
3. Answer with a short post that @mentions exactly the relevant members, each with a few words on why.
4. When no member is relevant, answer with a short post saying so, mentioning nobody.
Only @mentions of members wake anyone: mention no one else. A member not in the roster is offline and gets the post \
when its daemon starts again.";

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
    /// A triage: the post is a human's, mentions nobody, and the persona is the channel's messenger.
    Triage,
}

/// What `post` asks of `persona`, given the entry of its channel and the `humans`: a prompt only when it @mentions the
/// persona, a member, or when the persona is the channel's messenger and it is a post of a human that mentions nobody;
/// never for the persona's own.
pub fn wake(persona: &PersonaId, entry: Option<&ChannelEntry>, post: &ChannelPost, humans: &Humans) -> Wake {
    let Some(entry) = entry else { return Wake::Skip };
    if post.from == *persona || !entry.woken(post, humans).contains(persona) {
        Wake::Skip
    } else if post.mentions.is_empty() {
        Wake::Triage
    } else {
        Wake::Mentioned
    }
}

/// `post` as an inbox message from its author to `persona`, owned by the thread's owner the post carries: what the
/// attempt markers, caps, escalations and tools of the thread go by.
pub fn as_envelope(post: &ChannelPost, persona: &PersonaId) -> Envelope {
    Envelope {
        id: post.id,
        thread_id: post.thread_id.clone(),
        from: post.from.clone(),
        to: persona.clone(),
        text: post.text.clone(),
        turn_budget: post.turn_budget,
        in_reply_to: None,
        owner: post.owner.clone(),
        escalation: None,
        approval: None,
        sent_at: post.sent_at,
    }
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
    persona: &'a PersonaId,
    humans: &'a Humans,
    thread: &'a ThreadId,
    /// Stream sequence of the post being answered: the context ends before it, and the later posts start after it.
    at: u64,
    posts: VecDeque<ChannelPost>,
    skipped: usize,
    later: Vec<ChannelPost>,
}

impl<'a> Collect<'a> {
    fn new(persona: &'a PersonaId, humans: &'a Humans, thread: &'a ThreadId, at: u64) -> Self {
        Self { persona, humans, thread, at, posts: VecDeque::new(), skipped: 0, later: Vec::new() }
    }

    /// Take the post at stream sequence `seq`.
    fn push(&mut self, seq: u64, post: ChannelPost) {
        if post.thread_id != *self.thread || seq == self.at {
            return;
        }
        if seq > self.at {
            // A human's post gives the thread a fresh budget: the ones before it no longer count.
            if self.humans.contains(&post.from) {
                self.later.clear();
            }
            self.later.push(post);
            return;
        }
        if post.from == *self.persona {
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

/// What the prompt of the escalation leader's answer on a thread of `channel` adds: the answer is posted back to the
/// thread, with the fresh turns the leader's answer gave it. `left` is the turns the thread keeps after the post.
pub fn resume_note(channel: &ChannelId, thread: &ThreadId, left: u32) -> String {
    format!(
        "[ants] The escalation's leader answered with fresh turns for thread {thread} of #{channel}. The final answer \
         of this turn is posted to #{channel} on that thread and spends one of its turns ({left} left after it). An \
         @persona mention in it wakes that member of the channel; mentioning nobody wakes nobody."
    )
}

/// The prompt of the channel's messenger for `post`, a post of a human that mentions nobody, after the `context` of
/// its thread: the [`TRIAGE_RULES`], and the members of `entry` as `roster` shows them. `left` is the turns the thread
/// keeps after the answer.
pub fn triage_prompt(
    post: &ChannelPost,
    entry: &ChannelEntry,
    roster: &[RosterEntry],
    context: &Context,
    left: u32,
) -> String {
    let channel = &post.channel;
    let mut text = format!("[ants] Channel #{channel}, thread {}.\n\n", post.thread_id);
    text.push_str(&context_text(context));
    text.push_str(&format!("[ants] Post from {} mentioning nobody:\n{}\n\n", post.from, post.text));
    text.push_str(TRIAGE_RULES);
    text.push_str(&format!("\n\n[ants] Members of #{channel}:\n"));
    if entry.members.is_empty() {
        text.push_str("(none)\n");
    }
    for member in &entry.members {
        text.push_str(&format!("- {}\n", member_line(member, roster)));
    }
    text.push_str(&format!(
        "\n[ants] The final answer of this turn is posted to #{channel} on this thread and spends one of its turns \
         ({left} left after it)."
    ));
    text
}

/// `member` with its role and capabilities from the roster, or as offline when the roster lacks it.
fn member_line(member: &PersonaId, roster: &[RosterEntry]) -> String {
    let Some(entry) = roster.iter().find(|e| e.persona == *member) else {
        return format!("@{member} (offline: not in the roster)");
    };
    let mut line = format!("@{member}");
    if !entry.capabilities.is_empty() {
        line.push_str(&format!(" [{}]", entry.capabilities.join(", ")));
    }
    if let Some(whoami) = entry.whoami.as_deref().map(str::trim).filter(|w| !w.is_empty()) {
        line.push_str(&format!(": {}", whoami.replace('\n', " ")));
    }
    line
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

    /// The posts on the thread of `post`, at stream sequence `seq`, stored after it; with `earlier`, also those before
    /// it that the persona has not been shown: after the last one shown, or since the thread started, without its own.
    /// The posts of `humans` renew the thread's turns. The read ends at the last post stored as it starts.
    pub async fn context(&self, post: &ChannelPost, seq: u64, earlier: bool, humans: &Humans) -> Result<Context> {
        let mut collect = Collect::new(&self.persona, humans, &post.thread_id, seq);
        let after = consumer::DeliverPolicy::ByStartSequence { start_sequence: seq + 1 };
        let seen = if earlier && !post.starts_thread() { Some(self.last_seen(&post.thread_id).await?) } else { None };
        let start = match seen {
            None => after,
            Some(Some(seen)) if seen + 1 >= seq => after,
            Some(Some(seen)) => consumer::DeliverPolicy::ByStartSequence { start_sequence: seen + 1 },
            Some(None) => match thread_start(&post.thread_id) {
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
                Ok(post) => collect.push(seq, post),
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
pub fn thread_start(thread: &ThreadId) -> Option<SystemTime> {
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

    /// The humans: `human`, and alice, from the humans bucket.
    fn humans() -> Humans {
        Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))])
    }

    fn wakes(persona: &PersonaId, entry: Option<&ChannelEntry>, post: &ChannelPost) -> Wake {
        wake(persona, entry, post, &humans())
    }

    fn post(from: &str, text: &str) -> ChannelPost {
        ChannelPost::new("ops".parse().unwrap(), p(from), text)
    }

    #[test]
    fn only_a_post_mentioning_a_member_wakes_it() {
        let (entry, a) = (ops(), p("dev-a"));
        assert_eq!(wakes(&a, Some(&entry), &post("human", "@dev-a deploy")), Wake::Mentioned);
        assert_eq!(wakes(&a, Some(&entry), &post("dev-b", "@dev-b @dev-a check")), Wake::Mentioned, "agents too");
        assert_eq!(wakes(&a, Some(&entry), &post("human", "@dev-b deploy")), Wake::Skip, "another member");
        assert_eq!(wakes(&a, Some(&entry), &post("human", "deploy")), Wake::Skip, "nobody mentioned");
        assert_eq!(wakes(&a, Some(&entry), &post("dev-a", "@dev-a note to self")), Wake::Skip, "its own post");
        assert_eq!(wakes(&a, None, &post("human", "@dev-a deploy")), Wake::Skip, "no such channel");
        assert_eq!(wakes(&p("dev-c"), Some(&entry), &post("human", "@dev-c deploy")), Wake::Skip, "no member");
        let other = ChannelPost::new("dev".parse().unwrap(), PersonaId::human(), "@dev-a deploy");
        assert_eq!(wakes(&a, Some(&entry), &other), Wake::Skip, "another channel's entry");
    }

    #[test]
    fn the_messenger_triages_the_human_posts_that_mention_nobody() {
        let (mut entry, messenger) = (ops(), PersonaId::messenger());
        let untagged = post("human", "the build is red");
        assert_eq!(wakes(&messenger, Some(&entry), &untagged), Wake::Triage, "the default messenger, no member");
        assert_eq!(wakes(&p("dev-a"), Some(&entry), &untagged), Wake::Skip, "members wait for its mentions");
        assert_eq!(wakes(&messenger, Some(&entry), &post("dev-a", "the build is red")), Wake::Skip, "agents' posts");
        assert_eq!(wakes(&messenger, Some(&entry), &post("human", "@dev-a the build")), Wake::Skip, "mentions");
        assert_eq!(wakes(&messenger, Some(&entry), &post("human", "@human the build")), Wake::Skip, "mentions");
        entry.messenger = Some(p("dev-b"));
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &untagged), Wake::Triage, "the assigned one");
        assert_eq!(wakes(&messenger, Some(&entry), &untagged), Wake::Skip, "not the default then");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("human", "@dev-b look")), Wake::Mentioned);
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("alice", "the build")), Wake::Triage, "any human's");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("bob", "the build")), Wake::Skip, "bob is no human");
        assert_eq!(wakes(&p("dev-b"), Some(&entry), &post("dev-a", "@alice the build")), Wake::Skip, "a human");
    }

    #[test]
    fn the_triage_prompt_has_the_rules_and_the_members_with_their_roles() {
        let (entry, root) = (ops(), post("human", "the build is red"));
        let now = chrono::Utc::now();
        let a = RosterEntry {
            persona: p("dev-a"),
            machine: "m".to_owned(),
            harness: "copilot".to_owned(),
            capabilities: vec!["rust".to_owned(), "ci".to_owned()],
            whoami: Some("Keeps the build\ngreen.".to_owned()),
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
        };
        let context = Context { posts: vec![post("dev-b", "earlier")], ..Context::default() };
        let text = triage_prompt(&root, &entry, &[a], &context, 7);
        let expected = format!(
            "[ants] Channel #ops, thread {}.\n\n\
             [ants] Earlier posts on the thread not shown before, oldest first:\ndev-b: earlier\n\n\
             [ants] Post from human mentioning nobody:\nthe build is red\n\n\
             {TRIAGE_RULES}\n\n\
             [ants] Members of #ops:\n\
             - @dev-a [rust, ci]: Keeps the build green.\n\
             - @dev-b (offline: not in the roster)\n\n\
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
