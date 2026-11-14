//! Channels as humans run them: their entries in the KV bucket `ants_channels` (`ants channel`), posts to them as the
//! CLI's human (`ants post`), and reading them back by thread (`ants channel read`).
//!
//! A channel thread's owner is the human whose post started it. A human's top-level post owns its thread. A post on an
//! existing thread (`--thread`) keeps the owner the thread's first posts name ([`thread_owner`]); only when none is
//! known, as on a thread no longer kept or unreachable, does the posting human take it.

use std::ops::ControlFlow;

use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{ChannelEntry, ChannelId, ChannelPost, Humans, RosterEntry, STREAM, ThreadId, UserId, Uuid, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::kv::{self, Operation, UpdateErrorKind};
use async_nats::jetstream::{self, consumer};
use futures::{StreamExt as _, TryStreamExt as _};

/// How long a reader consumer left behind by an interrupted read lives on.
const INACTIVE: Duration = Duration::from_secs(30);
/// Messages asked for at once when reading a channel.
const BATCH: u64 = 256;
/// Attempts at a compare-and-set update before giving up on a channel that keeps changing.
const ATTEMPTS: usize = 10;

/// A channel id as given on the command line: `#ops` or `ops`.
pub fn parse_channel(s: &str) -> Result<ChannelId, ants_proto::Error> {
    s.strip_prefix('#').unwrap_or(s).parse()
}

/// An ant id as given on the command line: `@dev-a` or `dev-a`.
pub fn parse_ant(s: &str) -> Result<UserId, ants_proto::Error> {
    s.strip_prefix('@').unwrap_or(s).parse()
}

/// Members and messengers are ants a daemon serves: `humans` read channels instead.
pub fn check_not_human<'a>(ants: impl IntoIterator<Item = &'a UserId>, humans: &Humans) -> Result<()> {
    if let Some(human) = ants.into_iter().find(|p| humans.contains(p)) {
        bail!(
            "{human} is a human, so neither a member nor the messenger of a channel; `ants channel read` shows channel \
             posts"
        );
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
    format!("#{}: members {}; messenger {}", entry.channel, members_label(entry), messenger_label(entry))
}

/// The channels as a table: one row per channel under a header, columns padded to line up.
pub fn table(entries: &[ChannelEntry]) -> String {
    let header = ["CHANNEL", "MEMBERS", "MESSENGER"].map(str::to_owned);
    let rows: Vec<[String; 3]> = std::iter::once(header)
        .chain(entries.iter().map(|e| [format!("#{}", e.channel), members_label(e), messenger_label(e)]))
        .collect();
    let mut widths = [0; 3];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
            cells.join("  ").trim_end().to_owned()
        })
        .collect();
    lines.join("\n")
}

/// Those of `ants` that no entry of the roster serves now.
pub fn unserved<'a>(ants: impl IntoIterator<Item = &'a UserId>, roster: &[RosterEntry]) -> Vec<&'a UserId> {
    ants.into_iter().filter(|p| !roster.iter().any(|e| e.ant == **p)).collect()
}

/// The ants `post` @mentions that it does not wake, not being members of the channel. `humans` are no members
/// but read the channel anyway, so mentions of them are left out.
pub fn idle_mentions<'a>(post: &'a ChannelPost, entry: Option<&ChannelEntry>, humans: &Humans) -> Vec<&'a UserId> {
    post.mentions.iter().filter(|m| !humans.contains(m) && entry.is_none_or(|e| !e.is_member(m))).collect()
}

/// The channels bucket, created if missing (the human's credentials may).
pub async fn open(js: &jetstream::Context) -> Result<kv::Store> {
    bus::ensure_channels(js).await
}

/// What is stored under a channel's key.
enum Stored {
    Missing,
    /// An entry for the key's channel, at a revision.
    Entry(ChannelEntry, u64),
    /// A malformed entry, or one naming another channel, at a revision: readers skip it.
    Unreadable(u64),
}

async fn load(kv: &kv::Store, channel: &ChannelId) -> Result<Stored> {
    let key = channel.as_str();
    let Some(found) = kv.entry(key).await.with_context(|| format!("reading channel {channel}"))? else {
        return Ok(Stored::Missing);
    };
    if found.operation != Operation::Put {
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

/// Remove a channel; fails when there is none. Its posts stay in the stream.
pub async fn delete(kv: &kv::Store, channel: &ChannelId) -> Result<()> {
    if matches!(load(kv, channel).await?, Stored::Missing) {
        bail!("no channel #{channel}");
    }
    kv.delete(channel.as_str()).await.with_context(|| format!("deleting channel {channel}"))
}

/// Every channel, by id. Entries readers skip are left out with a warning.
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
/// existing thread keeps its owner ([`thread_owner`]), or takes it when none is known; an ant's post owns nothing
/// and keeps a known owner.
pub async fn compose(
    js: &jetstream::Context,
    me: &crate::humans::Me,
    humans: &Humans,
    channel: ChannelId,
    thread: Option<ThreadId>,
    text: String,
) -> ChannelPost {
    let known = match &thread {
        Some(thread) => thread_owner(js, &me.id, &channel, thread, humans).await.unwrap_or_else(|e| {
            tracing::warn!("reading the owner of thread {thread} in #{channel}: {e:#}");
            None
        }),
        None => None,
    };
    let owner = known.or_else(|| me.role.is_human().then(|| me.id.clone()));
    human_post(channel, thread, me.id.clone(), owner, text)
}

/// The owner of `thread` in `channel`, read through the reader consumer of `reader`: the one the first of its posts
/// still kept names ([`ChannelPost::thread_owner`] with `humans`), or None when none does. A thread named after the
/// post that started it is read from its start; any other from the channel's oldest post kept.
pub async fn thread_owner(
    js: &jetstream::Context,
    reader: &UserId,
    channel: &ChannelId,
    thread: &ThreadId,
    humans: &Humans,
) -> Result<Option<UserId>> {
    let start = match ants_daemon::channel::thread_start(thread) {
        Some(time) => consumer::DeliverPolicy::ByStartTime { start_time: time.into() },
        None => consumer::DeliverPolicy::All,
    };
    let mut owner = None;
    scan(js, reader, channel, start, |post| {
        owner = (post.thread_id == *thread).then(|| post.thread_owner(humans)).flatten();
        if owner.is_some() { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    })
    .await?;
    Ok(owner)
}

/// Hand each post on `post`'s thread arriving on `sub` (live channel traffic) to `show`, other than `post` itself,
/// until `timeout` passes or `stop` completes. Posts whose sender or channel disagrees with their subject are dropped.
/// Returns how many were shown.
pub async fn follow_thread(
    sub: &mut async_nats::Subscriber,
    post: &ChannelPost,
    timeout: Duration,
    stop: impl Future<Output = ()>,
    mut show: impl FnMut(&ChannelPost),
) -> Result<usize> {
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline, stop);
    let mut shown = 0;
    loop {
        let msg = tokio::select! {
            () = &mut deadline => break,
            () = &mut stop => break,
            msg = sub.next() => match msg {
                Some(msg) => msg,
                None => bail!("connection closed while waiting for replies"),
            },
        };
        let Ok(reply) = serde_json::from_slice::<ChannelPost>(&msg.payload) else { continue };
        if let Err(e) = reply.check_subject(&msg.subject) {
            tracing::warn!("ignoring a forged channel post: {e}");
            continue;
        }
        if reply.thread_id == post.thread_id && reply.id != post.id {
            show(&reply);
            shown += 1;
        }
    }
    Ok(shown)
}

/// The posts of `channel` kept in the stream, oldest first, read through the reader consumer of `reader`, created for
/// the read and deleted after it. Only one read per reader may run at a time. Posts whose sender or channel disagrees
/// with their subject are dropped with a warning.
pub async fn read(js: &jetstream::Context, reader: &UserId, channel: &ChannelId) -> Result<Vec<ChannelPost>> {
    let mut posts = Vec::new();
    scan(js, reader, channel, consumer::DeliverPolicy::All, |post| {
        posts.push(post);
        ControlFlow::Continue(())
    })
    .await?;
    Ok(posts)
}

/// Hand the posts of `channel` kept in the stream from `start` on, oldest first, to `take` until it breaks, read as
/// [`read`] does.
async fn scan(
    js: &jetstream::Context,
    reader: &UserId,
    channel: &ChannelId,
    start: consumer::DeliverPolicy,
    take: impl FnMut(ChannelPost) -> ControlFlow<()>,
) -> Result<()> {
    let name = ants_proto::chan_reader(reader);
    // A consumer left by an interrupted read would carry on where it stopped.
    let _ = js.delete_consumer_from_stream(&name, STREAM).await;
    let config = consumer::pull::Config {
        name: Some(name.clone()),
        filter_subject: subject::chan_room(channel),
        deliver_policy: start,
        ack_policy: consumer::AckPolicy::None,
        inactive_threshold: INACTIVE,
        ..Default::default()
    };
    let consumer: consumer::PullConsumer =
        js.create_consumer_on_stream(config, STREAM).await.with_context(|| format!("reading channel {channel}"))?;
    let scanned = collect(&consumer, channel, take).await;
    if let Err(e) = js.delete_consumer_from_stream(&name, STREAM).await {
        tracing::debug!(error = %e, consumer = name, "deleting the channel reader");
    }
    scanned
}

async fn collect(
    consumer: &consumer::PullConsumer,
    channel: &ChannelId,
    mut take: impl FnMut(ChannelPost) -> ControlFlow<()>,
) -> Result<()> {
    let mut left = consumer.cached_info().num_pending;
    while left > 0 {
        let mut batch = consumer.fetch().max_messages(left.min(BATCH) as usize).messages().await?;
        let mut got = 0;
        while let Some(msg) = batch.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
            got += 1;
            let post = serde_json::from_slice::<ChannelPost>(&msg.payload)
                .map_err(|e| e.to_string())
                .and_then(|post| post.check_subject(&msg.subject).map(|()| post).map_err(|e| e.to_string()));
            match post {
                Ok(post) => {
                    if take(post).is_break() {
                        return Ok(());
                    }
                }
                Err(error) => tracing::warn!(%channel, subject = %msg.subject, error, "dropping a channel post"),
            }
        }
        if got == 0 {
            break;
        }
        left = left.saturating_sub(got);
    }
    Ok(())
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
    out.push_str(&format!("{indent}{time} {}: {}\n", post.from, lines.next().unwrap_or("")));
    for line in lines {
        out.push_str(&format!("{indent}    {line}\n"));
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
        out.push_str(&format!("thread {}\n", thread.id));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::tests::Server;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn ops() -> ChannelId {
        "ops".parse().unwrap()
    }

    #[test]
    fn channels_and_ants_parse_with_or_without_their_sigil() {
        assert_eq!(parse_channel("#ops").unwrap(), ops());
        assert_eq!(parse_channel("ops").unwrap(), ops());
        assert!(parse_channel("##ops").is_err());
        assert!(parse_channel("#").is_err());
        assert!(parse_channel("#Ops").is_err());
        assert_eq!(parse_ant("@dev-a").unwrap(), p("dev-a"));
        assert_eq!(parse_ant("dev-a").unwrap(), p("dev-a"));
        assert!(parse_ant("@").is_err());
        let humans = Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))]);
        assert!(check_not_human([&p("dev-a"), &UserId::human()], &humans).is_err());
        assert!(check_not_human([&p("alice")], &humans).is_err());
        assert!(check_not_human([&p("dev-a")], &humans).is_ok());
    }

    #[test]
    fn the_table_shows_members_and_the_messenger_or_its_default() {
        let mut busy = ChannelEntry::new(ops());
        busy.add_member(p("dev-a"));
        busy.add_member(p("dev-b"));
        busy.messenger = Some(p("triage"));
        let quiet = ChannelEntry::new("dev".parse().unwrap());
        assert_eq!(
            table(&[quiet.clone(), busy.clone()]),
            "CHANNEL  MEMBERS      MESSENGER\n#dev     -            messenger (default)\n#ops     dev-a,dev-b  triage"
        );
        assert_eq!(describe(&quiet), "#dev: members -; messenger messenger (default)");
        assert_eq!(describe(&busy), "#ops: members dev-a,dev-b; messenger triage");
    }

    #[test]
    fn mentions_of_non_members_wake_nobody() {
        let mut entry = ChannelEntry::new(ops());
        entry.add_member(p("dev-a"));
        let post = ChannelPost::new(ops(), UserId::human(), "@dev-a @dev-b @human @alice look");
        let humans = Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))]);
        assert_eq!(idle_mentions(&post, Some(&entry), &humans), [&p("dev-b")]);
        assert_eq!(idle_mentions(&post, None, &humans), [&p("dev-a"), &p("dev-b")]);
        let roster: Vec<RosterEntry> = Vec::new();
        assert_eq!(unserved([&p("dev-a")], &roster), [&p("dev-a")]);
    }

    #[test]
    fn a_post_on_a_thread_answers_the_post_that_started_it() {
        let alice = Some(p("alice"));
        let top = human_post(ops(), None, p("alice"), alice.clone(), "build is red".to_owned());
        assert!(top.starts_thread());
        assert_eq!((&top.from, &top.owner), (&p("alice"), &alice));
        let reply = human_post(ops(), Some(top.thread_id.clone()), p("bob"), alice.clone(), "any news?".to_owned());
        assert_eq!((&reply.thread_id, reply.in_reply_to, reply.starts_thread()), (&top.thread_id, Some(top.id), false));
        assert_eq!(reply.owner, alice);
        let elsewhere = human_post(ops(), Some("t1".parse().unwrap()), p("bob"), None, "hi".to_owned());
        assert_eq!(elsewhere.in_reply_to, None);
    }

    fn at(mut post: ChannelPost, secs: i64) -> ChannelPost {
        post.sent_at = chrono::DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap();
        post
    }

    #[test]
    fn posts_group_by_thread_with_replies_indented() {
        let a = at(ChannelPost::new(ops(), UserId::human(), "build is red @dev-a"), 0);
        let b = at(ChannelPost::new(ops(), p("dev-b"), "deploying"), 1);
        let a1 = at(a.reply(p("dev-a"), "on it\nsee the log", &[], &Humans::default()).unwrap(), 2);
        // A reply stored before the post that started its thread, as one kept in an outbox can be.
        let c = at(ChannelPost::new(ops(), UserId::human(), "late start"), 4);
        let c1 = at(c.reply(p("dev-a"), "early reply", &[], &Humans::default()).unwrap(), 3);
        let orphan = at(ChannelPost::on_thread(ops(), "t1".parse().unwrap(), p("dev-b"), "lost root"), 5);
        let grouped = threads(vec![a.clone(), b.clone(), a1.clone(), c1.clone(), c.clone(), orphan.clone()]);
        let ids: Vec<&ThreadId> = grouped.iter().map(|t| &t.id).collect();
        assert_eq!(ids, [&a.thread_id, &b.thread_id, &c.thread_id, &orphan.thread_id]);
        assert_eq!(grouped[0].posts, [a.clone(), a1]);
        assert_eq!(grouped[2].posts, [c, c1]);
        assert!(!grouped[3].has_start());

        let shown = render(&grouped[..1]);
        assert_eq!(
            shown,
            format!(
                "thread {}\n2027-01-15 08:00:00 human: build is red @dev-a\n    2027-01-15 08:00:02 dev-a: on it\n        see the log\n",
                a.thread_id
            )
        );
        let shown = render(&grouped[3..]);
        assert_eq!(
            shown,
            "thread t1\n(the post that started the thread is no longer kept)\n    2027-01-15 08:00:05 dev-b: lost root\n"
        );
        assert_eq!(render(&grouped[..2]).matches("\n\nthread ").count(), 1, "a blank line between threads");
    }

    /// With the human's credentials, so it also checks what they allow.
    #[tokio::test]
    async fn the_human_manages_channels_and_reads_them_back() {
        let Some(server) = Server::start() else { return };
        let (human, dev_a) = (UserId::human(), p("dev-a"));
        let (client, js) = server.connect(&human).await;
        bus::init_stream(&js).await.unwrap();
        let kv = open(&js).await.unwrap();

        let mut entry = ChannelEntry::new(ops());
        entry.add_member(dev_a.clone());
        create(&kv, &entry).await.unwrap();
        let again = create(&kv, &entry).await.unwrap_err().to_string();
        assert!(again.contains("exists"), "{again}");
        let (stored, added) = update(&kv, &ops(), |e| e.add_member(p("dev-b"))).await.unwrap();
        assert!(added);
        assert_eq!(stored.members, [dev_a.clone(), p("dev-b")]);
        assert_eq!(get(&kv, &ops()).await.unwrap(), Some(stored));
        assert!(update(&kv, &"dev".parse().unwrap(), |_| ()).await.is_err(), "no such channel");

        // An entry naming another channel is skipped, and replaced on the next update.
        let foreign = ChannelEntry::new("dev".parse().unwrap());
        kv.put("qa", encode(&foreign).unwrap().into()).await.unwrap();
        let listed = list(&kv).await.unwrap();
        assert_eq!(listed.iter().map(|e| e.channel.as_str()).collect::<Vec<_>>(), ["ops"]);
        let (fixed, ()) = update(&kv, &"qa".parse().unwrap(), |e| e.messenger = Some(dev_a.clone())).await.unwrap();
        assert_eq!((fixed.channel.as_str(), fixed.members.len()), ("qa", 0));

        // Posting, following the thread live, and reading the channel back through the human's reader.
        let mut sub = client.subscribe(subject::chan_room(&ops())).await.unwrap();
        let top = human_post(ops(), None, human.clone(), Some(human.clone()), "build is red @dev-a".to_owned());
        bus::store(&js, top.subject(), &top, &top.id.to_string()).await.unwrap();
        let (_, a_js) = server.connect(&dev_a).await;
        let answer = top.reply(dev_a.clone(), "on it", &[], &Humans::default()).unwrap();
        bus::publish_json(&a_js, answer.subject(), &answer, None).await.unwrap();
        let mut seen = Vec::new();
        let shown = follow_thread(&mut sub, &top, Duration::from_secs(2), std::future::pending(), |p| {
            seen.push(p.text.clone())
        })
        .await
        .unwrap();
        assert_eq!((shown, seen), (1, vec!["on it".to_owned()]));

        let posts = read(&js, &human, &ops()).await.unwrap();
        assert_eq!(posts, [top, answer]);
        assert!(read(&js, &human, &"dev".parse().unwrap()).await.unwrap().is_empty());

        delete(&kv, &ops()).await.unwrap();
        assert!(delete(&kv, &ops()).await.is_err());
        assert_eq!(get(&kv, &ops()).await.unwrap(), None);
        create(&kv, &entry).await.unwrap();
    }

    /// `ants post` with alice's credentials, then bob's on her thread: each posts under its own name, and the thread
    /// stays alice's.
    #[tokio::test]
    async fn a_human_posts_under_its_own_name_and_keeps_the_thread_it_started() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
        let me = |id: &UserId| crate::humans::Me { id: id.clone(), role: ants_proto::Role::Human };
        let (a_client, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
        bus::init_stream(&a_js).await.unwrap();
        crate::humans::ensure(&a_js).await.unwrap();
        for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
            crate::humans::register(js, &me(human)).await.unwrap();
        }
        let humans = crate::humans::load(&b_js, &me(&bob)).await;
        assert!(humans.contains(&alice) && humans.contains(&bob) && humans.contains(&UserId::human()));

        let mut sub = a_client.subscribe(subject::chan_room(&ops())).await.unwrap();
        a_client.flush().await.unwrap();
        let top = compose(&a_js, &me(&alice), &humans, ops(), None, "build is red".to_owned()).await;
        assert_eq!((&top.from, &top.owner), (&alice, &Some(alice.clone())));
        bus::store(&a_js, top.subject(), &top, &top.id.to_string()).await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(2), sub.next()).await.unwrap().unwrap();
        assert_eq!(got.subject.as_str(), "ants.chan.ops.alice");

        // An ant's answer keeps the owner; so does bob's post on the thread, which reads it back.
        let (_, d_js) = server.connect(&dev_a).await;
        let answer = top.reply(dev_a.clone(), "on it", &[], &humans).unwrap();
        assert_eq!(answer.owner, Some(alice.clone()));
        bus::publish_json(&d_js, answer.subject(), &answer, None).await.unwrap();
        let on = compose(&b_js, &me(&bob), &humans, ops(), Some(top.thread_id.clone()), "any news?".to_owned()).await;
        assert_eq!((&on.from, &on.owner, on.in_reply_to), (&bob, &Some(alice.clone()), Some(top.id)));
        bus::store(&b_js, on.subject(), &on, &on.id.to_string()).await.unwrap();
        assert_eq!(on.subject(), "ants.chan.ops.bob");

        // On a thread nothing names an owner of, bob takes it.
        let lost = compose(&b_js, &me(&bob), &humans, ops(), Some("t1".parse().unwrap()), "hi".to_owned()).await;
        assert_eq!(lost.owner, Some(bob.clone()));
        assert_eq!(read(&b_js, &bob, &ops()).await.unwrap(), [top, answer, on]);
    }
}
