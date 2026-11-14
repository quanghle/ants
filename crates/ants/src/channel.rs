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

use std::ops::ControlFlow;

use ants_daemon::bus;
pub use ants_daemon::channel::list;
use ants_daemon::channel::{OriginFold, Reading, Stored, load, read_back};
use ants_daemon::webhooks::Webhooks;
use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, Humans, RosterEntry, ThreadId, ThreadOrigin, UserId, Uuid, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::kv::{self, UpdateErrorKind};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;

use crate::humans::Me;

/// Attempts at a compare-and-set update before giving up on a channel that keeps changing.
const ATTEMPTS: usize = 10;

/// A channel id as given on the command line: `#ops` or `ops`.
pub fn parse_channel(s: &str) -> Result<ChannelId, ants_proto::Error> {
    s.strip_prefix('#').unwrap_or(s).parse()
}

/// A user id as given on the command line, an ant's or a human's: `@dev-a` or `dev-a`; never the channel manager.
pub fn parse_ant(s: &str) -> Result<UserId, String> {
    let ant: UserId = s.strip_prefix('@').unwrap_or(s).parse().map_err(|e: ants_proto::Error| e.to_string())?;
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
    let owner = entry.owner.as_ref().map(|o| format!("; owner {o}")).unwrap_or_default();
    format!("#{}: members {}; messenger {}{owner}", entry.channel, members_label(entry), messenger_label(entry))
}

/// The channels as a table: one row per channel under a header, columns padded to line up.
pub fn table(entries: &[ChannelEntry]) -> String {
    let header = ["CHANNEL", "MEMBERS", "MESSENGER", "OWNER"].map(str::to_owned);
    let owner = |e: &ChannelEntry| e.owner.as_ref().map_or_else(|| "-".to_owned(), UserId::to_string);
    let rows: Vec<[String; 4]> = std::iter::once(header)
        .chain(entries.iter().map(|e| [format!("#{}", e.channel), members_label(e), messenger_label(e), owner(e)]))
        .collect();
    let mut widths = [0; 4];
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

/// Those of `users` that are not `humans` and that no entry of the roster serves now.
pub fn unserved<'a>(
    users: impl IntoIterator<Item = &'a UserId>,
    roster: &[RosterEntry],
    humans: &Humans,
) -> Vec<&'a UserId> {
    users.into_iter().filter(|p| !humans.contains(p) && !roster.iter().any(|e| e.ant == **p)).collect()
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
    for human in entry.map(|entry| entry.notified(post, humans)).unwrap_or_default() {
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
/// answers, and apply the changes owners ask for ([`ants_daemon::chanadm::serve`]).
pub async fn serve(url: &str, creds: Option<&std::path::Path>, domain: Option<&ants_proto::JsDomain>) -> Result<()> {
    let me = UserId::channel_manager();
    let connecting = bus::connect_retrying(url, creds, domain, &me, "ants-channel-manager", std::future::pending());
    let (client, js) = connecting.await?.context("connecting")?;
    let humans = ants_daemon::humans::Registry::new(UserId::human());
    let _watching = humans.watch(js.clone(), ants_daemon::humans::RETRY);
    let roster = ants_daemon::mcp::jetstream_roster(js.clone());
    ants_daemon::chanadm::serve(client, js, roster, humans).await
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

/// The owner and creator of `thread` in `channel`, read as `reader` reads channels back ([`read`]): as its posts still
/// kept say ([`ThreadOrigin`] with `humans`), or None. A thread named after the post that started it is read from its
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
    scan(js, reader, channel, start, |post| {
        if post.thread_id == *thread {
            fold.push(post, humans);
        }
        if fold.settled() { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
    })
    .await?;
    let origin = fold.resolve(thread, &Webhooks::new(js.clone())).await;
    Ok((origin.owner, origin.creator))
}

/// The posts on the thread of `post` that `room`, a subscription to its channel, receives after it: those of others
/// only with `by_others`. Forged posts are dropped with a warning.
pub fn thread_posts(
    room: async_nats::Subscriber,
    post: &ChannelPost,
    by_others: bool,
) -> impl futures::Stream<Item = ChannelPost> + use<> {
    let (thread, id, from) = (post.thread_id.clone(), post.id, post.from.clone());
    room.filter_map(move |msg| {
        let reply = match serde_json::from_slice::<ChannelPost>(&msg.payload) {
            Ok(reply) => match reply.check_subject(&msg.subject) {
                Ok(()) => {
                    (reply.thread_id == thread && reply.id != id && !(by_others && reply.from == from)).then_some(reply)
                }
                Err(e) => {
                    tracing::warn!("ignoring a forged channel post: {e}");
                    None
                }
            },
            Err(_) => None,
        };
        std::future::ready(reply)
    })
}

/// The posts of `channel` kept in the stream, oldest first, read as `reader` reads channels back ([`Reading`]): a
/// human by message gets, so any number of reads may run at once, an ant through its reader consumer, created for the
/// read and deleted after it, one read at a time. Posts whose sender or channel disagrees with their subject are
/// dropped with a warning.
pub async fn read(js: &jetstream::Context, reader: &Me, channel: &ChannelId) -> Result<Vec<ChannelPost>> {
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
    reader: &Me,
    channel: &ChannelId,
    start: consumer::DeliverPolicy,
    mut take: impl FnMut(ChannelPost) -> ControlFlow<()>,
) -> Result<()> {
    let reading = Reading::of(reader.role);
    read_back(js, &reader.id, reading, &subject::chan_room(channel), start, |_, _, subject, post| match post {
        Ok(post) => take(post),
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
    use std::time::Duration;

    use ants_proto::{STREAM, chan_reader};

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
        assert!(parse_ant("channel-manager").is_err());
        let humans = Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))]);
        assert!(check_messenger_not_human(Some(&UserId::human()), &humans).is_err());
        assert!(check_messenger_not_human(Some(&p("alice")), &humans).is_err());
        assert!(check_messenger_not_human(Some(&p("dev-a")), &humans).is_ok());
        assert!(check_messenger_not_human(None, &humans).is_ok());
    }

    #[test]
    fn the_table_shows_members_and_the_messenger_or_its_default() {
        let mut busy = ChannelEntry::new(ops());
        busy.add_member(p("dev-a"));
        busy.add_member(p("dev-b"));
        busy.messenger = Some(p("triage"));
        busy.owner = Some(p("lead"));
        let quiet = ChannelEntry::new("dev".parse().unwrap());
        assert_eq!(
            table(&[quiet.clone(), busy.clone()]),
            "CHANNEL  MEMBERS      MESSENGER            OWNER\n#dev     -            messenger (default)  -\n\
             #ops     dev-a,dev-b  triage               lead"
        );
        assert_eq!(describe(&quiet), "#dev: members -; messenger messenger (default)");
        assert_eq!(describe(&busy), "#ops: members dev-a,dev-b; messenger triage; owner lead");
    }

    #[test]
    fn mentions_of_non_members_reach_nobody() {
        let mut entry = ChannelEntry::new(ops());
        entry.add_member(p("dev-a"));
        let post = ChannelPost::new(ops(), UserId::human(), "@dev-a @dev-b @human @alice look");
        let humans = Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))]);
        assert_eq!(idle_mentions(&post, Some(&entry)), [&p("dev-b"), &p("alice")], "a human is no member either");
        entry.add_member(p("alice"));
        assert_eq!(idle_mentions(&post, Some(&entry)), [&p("dev-b")], "a human member is notified");
        assert_eq!(idle_mentions(&post, None), [&p("dev-a"), &p("dev-b"), &p("alice")]);
        let roster: Vec<RosterEntry> = Vec::new();
        assert_eq!(unserved([&p("dev-a"), &p("alice")], &roster, &humans), [&p("dev-a")], "a human needs no daemon");
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
        let room = || async { client.subscribe(subject::chan_room(&ops())).await.unwrap() };
        let (following, waiting) = (room().await, room().await);
        let top = human_post(ops(), None, human.clone(), Some(human.clone()), "build is red @dev-a".to_owned());
        bus::store(&js, top.subject(), &top, &top.id.to_string()).await.unwrap();
        let (_, a_js) = server.connect(&dev_a).await;
        let again =
            human_post(ops(), Some(top.thread_id.clone()), human.clone(), Some(human.clone()), "anyone?".into());
        bus::store(&js, again.subject(), &again, &again.id.to_string()).await.unwrap();
        let answer = top.reply(dev_a.clone(), "on it", &[], &Humans::default()).unwrap();
        bus::publish_json(&a_js, answer.subject(), &answer, None).await.unwrap();
        let texts = |posts: Vec<ChannelPost>| posts.into_iter().map(|p| p.text).collect::<Vec<_>>();
        let next_two = thread_posts(following, &top, false).take(2).collect();
        assert_eq!(texts(tokio::time::timeout(Duration::from_secs(2), next_two).await.unwrap()), ["anyone?", "on it"]);
        let mut waiting = Box::pin(thread_posts(waiting, &top, true));
        let next = tokio::time::timeout(Duration::from_secs(2), waiting.next()).await.unwrap().unwrap();
        assert_eq!(next.text, "on it", "waiting skips the poster's own posts");

        let me = Me { id: human.clone(), role: ants_proto::Role::Human };
        let posts = read(&js, &me, &ops()).await.unwrap();
        assert_eq!(posts, [top, again, answer]);
        assert!(read(&js, &me, &"dev".parse().unwrap()).await.unwrap().is_empty());

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
        assert_eq!((lost.owner, lost.creator), (Some(bob.clone()), None));
        assert_eq!(read(&b_js, &me(&bob), &ops()).await.unwrap(), [top.clone(), answer.clone(), on.clone()]);

        // bob's reads get the posts one by one: many at once all read the whole channel, and none touches a reader
        // consumer, here one an older ants is reading through.
        let older = consumer::pull::Config {
            name: Some(chan_reader(&bob)),
            filter_subject: subject::chan_room(&ops()),
            ack_policy: consumer::AckPolicy::None,
            ..Default::default()
        };
        let older: consumer::PullConsumer = b_js.create_consumer_on_stream(older, STREAM).await.unwrap();
        let next = || async { older.fetch().max_messages(1).messages().await.unwrap().next().await.unwrap().unwrap() };
        let post = |msg: jetstream::Message| serde_json::from_slice::<ChannelPost>(&msg.payload).unwrap();
        assert_eq!(post(next().await), top);
        let (as_bob, room) = (me(&bob), ops());
        let reads = futures::future::join_all((0..8).map(|_| read(&b_js, &as_bob, &room))).await;
        for posts in reads {
            assert_eq!(posts.unwrap(), [top.clone(), answer.clone(), on.clone()]);
        }
        assert_eq!(post(next().await), answer, "the older read carries on where it was");

        // An ant creates the thread it starts, and owns none; a human's post on it keeps the creator.
        let ant = crate::humans::Me { id: dev_a.clone(), role: ants_proto::Role::Ant };
        let started = compose(&d_js, &ant, &humans, ops(), None, "@dev-b look".to_owned()).await;
        assert_eq!((&started.creator, &started.owner), (&Some(dev_a.clone()), &None));
        bus::publish_json(&d_js, started.subject(), &started, None).await.unwrap();
        let on = compose(&b_js, &me(&bob), &humans, ops(), Some(started.thread_id.clone()), "ok".to_owned()).await;
        assert_eq!((&on.creator, &on.owner), (&Some(dev_a.clone()), &Some(bob.clone())));
    }

    /// The webhook github, and dev-a, each with its own credentials, start a thread with a post marked as a webhook's
    /// naming alice its owner: bob's post on github's thread keeps alice; on dev-a's, which has no marker, it takes the
    /// thread and keeps dev-a as its creator.
    #[tokio::test]
    async fn a_post_on_a_thread_an_ant_started_as_a_webhook_counts_it_as_the_ant_s() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a, hook) = (p("alice"), p("bob"), p("dev-a"), p("github"));
        let me = |id: &UserId| crate::humans::Me { id: id.clone(), role: ants_proto::Role::Human };
        let (_, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
        bus::init_stream(&a_js).await.unwrap();
        crate::humans::ensure(&a_js).await.unwrap();
        crate::webhook::ensure_bucket(&a_js).await.unwrap();
        for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
            crate::humans::register(js, &me(human)).await.unwrap();
        }
        let humans = crate::humans::load(&b_js, &me(&bob)).await;

        let creds = server.dir().join("github.creds");
        let permissions = ants_proto::Permissions::for_webhook(&hook, &[ants_proto::Destination::Channel(ops())]);
        crate::auth::issue_with(&server.dir().join("auth"), &hook, permissions, &creds).unwrap();
        let (_, h_js) = bus::connect(&server.url, Some(&creds), None, &hook, "test").await.unwrap();
        let markers = h_js.get_key_value(ants_proto::WEBHOOKS_BUCKET).await.unwrap();
        markers.put(ants_proto::webhook_marker_key(&hook), "github".into()).await.unwrap();
        let (_, d_js) = server.connect(&dev_a).await;

        let owner = Some(alice.clone());
        let real =
            ChannelPost::new(ops(), hook.clone(), "build red").with_owner(owner.clone()).from_webhook(Vec::new());
        bus::publish_json(&h_js, real.subject(), &real, None).await.unwrap();
        let forged = ChannelPost::new(ops(), dev_a.clone(), "look").with_owner(owner.clone()).from_webhook(Vec::new());
        bus::publish_json(&d_js, forged.subject(), &forged, None).await.unwrap();

        let as_bob = me(&bob);
        let on = |thread: &ThreadId| compose(&b_js, &as_bob, &humans, ops(), Some(thread.clone()), "ok".to_owned());
        let on_real = on(&real.thread_id).await;
        assert_eq!((on_real.owner, on_real.creator), (owner, None), "the webhook's");
        let on_forged = on(&forged.thread_id).await;
        assert_eq!((on_forged.owner, on_forged.creator), (Some(bob.clone()), Some(dev_a.clone())), "dev-a's");
    }

    /// alice's post and dev-a's, each with its own credentials, mention bob, a member: bob's inbox gets one notice
    /// of each, however often it is sent, and none of a post mentioning bob as no member.
    #[tokio::test]
    async fn a_mention_of_a_human_member_reaches_its_inbox_once() {
        let Some(server) = Server::start() else { return };
        let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
        let me = |id: &UserId| crate::humans::Me { id: id.clone(), role: ants_proto::Role::Human };
        let (_, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
        bus::init_stream(&a_js).await.unwrap();
        crate::humans::ensure(&a_js).await.unwrap();
        for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
            crate::humans::register(js, &me(human)).await.unwrap();
        }
        let humans = crate::humans::load(&a_js, &me(&alice)).await;
        let mut entry = ChannelEntry::new(ops());
        let (_, d_js) = server.connect(&dev_a).await;
        let post = compose(&a_js, &me(&alice), &humans, ops(), None, "@bob @alice @dev-a look".to_owned()).await;
        assert!(notify_mentioned(&a_js, &post, Some(&entry), &humans).await.is_empty(), "bob is no member yet");

        entry.add_member(dev_a.clone());
        entry.add_member(bob.clone());
        for _ in 0..2 {
            assert_eq!(notify_mentioned(&a_js, &post, Some(&entry), &humans).await, std::slice::from_ref(&bob));
        }
        let ant_post = ChannelPost::new(ops(), dev_a.clone(), "@bob done");
        let notice = ant_post.mention_notice(&bob);
        for _ in 0..2 {
            bus::store(&d_js, notice.subject(), &notice, &ant_post.mention_msg_id(&bob)).await.unwrap();
        }

        let mut inbox = crate::inbox::Inbox::open(&b_js, &bob).await.unwrap();
        let unread = inbox.unread(&crate::inbox::Select::default()).await.unwrap();
        inbox.release().await.unwrap();
        let got: Vec<_> = unread.iter().map(|e| (e.from.clone(), e.mention.as_ref().map(|m| m.post))).collect();
        assert_eq!(got, [(alice.clone(), Some(post.id)), (dev_a.clone(), Some(ant_post.id))]);
        let shown = crate::inbox::render(&unread[1]);
        assert!(shown.contains(&format!("dev-a mentioned bob in #ops on thread {}", ant_post.thread_id)), "{shown}");
        assert!(shown.contains(&format!("`ants post ops --thread {} ...`", ant_post.thread_id)), "{shown}");
    }
}
