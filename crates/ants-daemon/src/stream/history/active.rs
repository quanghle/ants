//! Review evidence: the threads of an ant active over a time, each with what was stored on it meanwhile.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::ControlFlow;
use std::time::SystemTime;

use ants_proto::provider::Origin;
use ants_proto::{
    ChannelId, ChannelPost, Envelope, EscalationReason, JsDomain, MergedEnd, MergedRejection, NotesPost,
    SESSIONS_BUCKET, ThreadId, UserId, Uuid, chan_reader, inbox_consumer, notes_consumer, sent_reader, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use serde::Deserialize;

use super::entry::{ENTRY_SHARE, Entry, Place, format};
use super::{sent_turn, timeline};
use crate::provider::origins::OriginStore;
use crate::records::direct_kv::{DirectKv, Lookup};
use crate::stream::reading::{self, Reading};

/// A thread of an ant active over a time, as [`active`] reads it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Active {
    pub thread: ThreadId,
    pub origin: Option<Origin>,
    /// What the ant received and sent on it, its notes and the channel posts on it, as a prompt shows a history.
    pub history: String,
    /// The tool-call timeline the ant stored on it, oldest first: empty when it stored none, or it could not be read.
    pub timeline: String,
    /// The ant's messages, notes posts and channel posts on it, oldest first.
    pub sent: Vec<Sent>,
    /// The entries of its history, each of which reopens it if it was marked done before, oldest first.
    pub reopenings: Vec<Reopening>,
    /// The ant's tool requests rejected on it, as its timeline keeps them: empty when it could not be read.
    pub rejections: Vec<MergedRejection>,
    /// How the ant's prompts on it ended, with their tool calls, as its timeline keeps them: empty when it could not
    /// be read.
    pub ends: Vec<MergedEnd>,
    /// The issues the ant escalated on it: each reason, and tool request, once, however many messages it took. Tool
    /// requests and questions to humans are not counted.
    pub escalations: u32,
}

/// A message the ant sent on a thread, or posted to its notes, as [`active`] read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Sent {
    pub id: Uuid,
    /// The start of its text, on one line, at most [`OPENING_BYTES`] bytes.
    pub opening: String,
}

/// A message, note or channel post stored on a thread, which reopens it if it was marked done before
/// ([`ants_proto::ThreadDone::holds`]), as [`active`] read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reopening {
    /// Its stream sequence.
    pub seq: u64,
    /// When the stream stored it.
    pub at: SystemTime,
    pub from: UserId,
    /// What it is: `message`, `thread note` or `channel post`, prefixed by `unreadable` when only metadata was read.
    pub kind: &'static str,
}

/// The most bytes of a message's text [`Sent::opening`] keeps.
const OPENING_BYTES: usize = 80;

impl Sent {
    fn new(id: Uuid, text: &str) -> Self {
        let line = text.trim().replace(['\n', '\r'], " ");
        let opening = if line.len() <= OPENING_BYTES {
            line
        } else {
            format!("{}…", &line[..line.floor_char_boundary(OPENING_BYTES)])
        };
        Self { id, opening }
    }
}

/// Private records used to gather evidence, without adding inbox links to a channel post.
#[derive(Default)]
pub(crate) struct ReviewRecords<'a> {
    pub js_domain: Option<&'a JsDomain>,
    pub origins: Option<&'a OriginStore>,
}

impl ReviewRecords<'_> {
    async fn origin(&self, thread: &ThreadId) -> Option<Origin> {
        self.origins?.get(thread).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading the origin of a thread");
            None
        })
    }
}

#[derive(Default)]
struct Evidence {
    entries: BTreeMap<u64, Entry>,
    own: BTreeMap<u64, Uuid>,
    unreadable: BTreeMap<u64, Reopening>,
    worked: Option<SystemTime>,
}

/// What a read of review evidence goes by: the ant, how its daemon reads, and the time read, from `since` to `until`.
#[derive(Clone, Copy)]
struct Window<'a> {
    js: &'a jetstream::Context,
    ant: &'a UserId,
    reading: Reading,
    since: SystemTime,
    until: SystemTime,
}

impl Window<'_> {
    /// Where its reads start: at the messages stored at `since`.
    fn start(&self) -> consumer::DeliverPolicy {
        consumer::DeliverPolicy::ByStartTime { start_time: self.since.into() }
    }
}

/// The threads of `ant` active from `since` to `until`, the most recently active first, each with what the ant received
/// and sent on it, its notes and, on the threads the ant worked or posted on in a channel, the channel's posts, from
/// `since` on, as a prompt shows a history within `budget` bytes, the messages, notes posts and channel posts of the
/// ant among them, and the timeline it stored on it meanwhile. Read as its daemon reads ([`Reading::daemon`]).
pub(crate) async fn active(
    js: &jetstream::Context,
    ant: &UserId,
    reading: Reading,
    records: ReviewRecords<'_>,
    (since, until): (SystemTime, SystemTime),
    budget: usize,
) -> Result<Vec<Active>> {
    let window = Window { js, ant, reading, since, until };
    let InboxEvidence { mut threads, unreadable, mut escalated } = inbox_entries(&window).await?;
    // Timeline records are best effort, so a thread is shown without them rather than not at all.
    let mut timelines = timeline::read(js, ant, reading, window.start(), until).await.unwrap_or_else(|e| {
        tracing::warn!(%ant, error = format!("{e:#}"), "reading back the timeline of the active threads");
        BTreeMap::new()
    });
    let mut origins = BTreeMap::new();
    let mut rooms: BTreeMap<ChannelId, BTreeSet<ThreadId>> = BTreeMap::new();
    for (thread, stored) in &timelines {
        let Some(worked) = stored.worked else { continue };
        let origin = records.origin(thread).await;
        if let Some(channel) = origin.as_ref().and_then(|origin| origin.channel.clone()) {
            threads.entry(thread.clone()).or_default().worked = Some(worked);
            rooms.entry(channel).or_default().insert(thread.clone());
        }
        origins.insert(thread.clone(), origin);
    }
    let sessions = DirectKv::new(js, records.js_domain, SESSIONS_BUCKET).await?;
    channel_posts(&window, &sessions, &mut rooms, &mut threads).await;
    known_unreadable_inbox(&records, unreadable, &mut threads, &mut origins).await;
    notes(&window, &mut threads).await?;
    let mut threads: Vec<_> = threads.into_iter().collect();
    threads.sort_by_key(|(_, e)| {
        std::cmp::Reverse(
            e.entries.values().map(|entry| entry.at).chain(e.unreadable.values().map(|r| r.at)).chain(e.worked).max(),
        )
    });
    let mut active = Vec::with_capacity(threads.len());
    for (thread, Evidence { entries, own, unreadable, .. }) in threads {
        let stored = timelines.remove(&thread);
        let timeline = stored.as_ref().map_or_default(|stored| stored.show(ant, budget / ENTRY_SHARE));
        let rejections = stored.as_ref().map_or_default(|stored| stored.rejections().to_vec());
        let ends = stored.map_or_default(|stored| stored.ends().to_vec());
        let escalations = escalated.remove(&thread).map_or(0, |issues| u32::try_from(issues.len()).unwrap_or(u32::MAX));
        let sent = own.into_iter().filter_map(|(seq, id)| entries.get(&seq).map(|e| Sent::new(id, &e.text))).collect();
        let mut reopenings: Vec<_> = entries
            .iter()
            .filter(|(_, entry)| !matches!(entry.place, Place::ChannelLink(..)))
            .map(|(&seq, entry)| Reopening { seq, at: entry.at, from: entry.from.clone(), kind: entry.place.noun() })
            .chain(unreadable.into_values())
            .collect();
        reopenings.sort_unstable_by_key(|r| r.seq);
        let history = format(entries.into_values().collect(), budget);
        let origin = match origins.remove(&thread) {
            Some(origin) => origin,
            None => records.origin(&thread).await,
        };
        active.push(Active { thread, origin, history, timeline, sent, reopenings, rejections, ends, escalations });
    }
    Ok(active)
}

#[derive(Default)]
struct InboxEvidence {
    threads: BTreeMap<ThreadId, Evidence>,
    unreadable: BTreeMap<ThreadId, BTreeMap<u64, Reopening>>,
    escalated: BTreeMap<ThreadId, HashSet<(EscalationReason, Option<Uuid>)>>,
}

/// What `window`'s ant received and sent in it, by thread.
async fn inbox_entries(window: &Window<'_>) -> Result<InboxEvidence> {
    let Window { js, ant, reading, until, .. } = *window;
    let mut inbox = InboxEvidence::default();
    let mut take = |seq, at, subject: &str, payload: &[u8]| {
        if at > until {
            return ControlFlow::Break(());
        }
        match serde_json::from_slice::<Envelope>(payload) {
            Ok(envelope) if envelope.check_subject(subject).is_ok() => {
                let evidence = inbox.threads.entry(envelope.thread_id.clone()).or_default();
                if envelope.from == *ant {
                    evidence.own.insert(seq, envelope.id);
                    if let Some(escalation) = envelope.escalation.as_deref()
                        && escalation.stuck == *ant
                        && !matches!(escalation.reason, EscalationReason::Approval | EscalationReason::Question)
                    {
                        let issue = (escalation.reason, escalation.request_id);
                        inbox.escalated.entry(escalation.thread_id.clone()).or_default().insert(issue);
                    }
                }
                evidence.entries.insert(seq, Entry::received(envelope, at));
            }
            Err(_) => {
                if let Ok(meta) = serde_json::from_slice::<InboxMeta>(payload)
                    && subject::parse_inbox(subject) == Some((meta.to, meta.from.clone()))
                {
                    let entry = Reopening { seq, at, from: meta.from, kind: "unreadable message" };
                    inbox.unreadable.entry(meta.thread_id).or_default().insert(seq, entry);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    };
    let (reader, filter) = (inbox_consumer(ant), subject::inbox_of(ant));
    reading::read_back(js, &reader, reading.beside(), &filter, window.start(), &mut take)
        .await
        .context("reading back the messages received")?;
    let _one = sent_turn(reading).await;
    let (reader, filter) = (sent_reader(ant), subject::inbox_from(ant));
    reading::read_back(js, &reader, reading, &filter, window.start(), &mut take)
        .await
        .context("reading back the messages sent")?;
    Ok(inbox)
}

async fn known_unreadable_inbox(
    records: &ReviewRecords<'_>,
    unreadable: BTreeMap<ThreadId, BTreeMap<u64, Reopening>>,
    threads: &mut BTreeMap<ThreadId, Evidence>,
    origins: &mut BTreeMap<ThreadId, Option<Origin>>,
) {
    for (thread, unreadable) in unreadable {
        match threads.entry(thread) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                entry.get_mut().unreadable.extend(unreadable);
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                let thread = entry.key();
                let origin = match origins.get(thread) {
                    Some(origin) => origin.clone(),
                    None => records.origin(thread).await,
                };
                if let Some(origin) = origin {
                    origins.insert(thread.clone(), Some(origin));
                    entry.insert(Evidence { unreadable, ..Evidence::default() });
                }
            }
        }
    }
}

/// Add the ant's channel posts, followed by posts on the threads it worked or posted on, a read of each channel.
/// MCP posts made during inbox work supply evidence on their source thread, but never reopen it.
async fn channel_posts(
    window: &Window<'_>,
    sessions: &DirectKv,
    rooms: &mut BTreeMap<ChannelId, BTreeSet<ThreadId>>,
    threads: &mut BTreeMap<ThreadId, Evidence>,
) {
    let Window { js, ant, reading, until, .. } = *window;
    let filter = subject::chan_from(ant);
    let mut own = Vec::new();
    let read = crate::channel::read_back(js, ant, reading.beside(), &filter, window.start(), |seq, at, _, post| {
        if at > until {
            return ControlFlow::Break(());
        }
        if let Ok(post) = post
            && post.from == *ant
        {
            own.push((seq, at, post));
        }
        ControlFlow::Continue(())
    })
    .await;
    if let Err(e) = read {
        tracing::warn!(%ant, error = format!("{e:#}"), "reading back the ant's channel posts");
    }
    for (seq, at, post) in own {
        let source = if post.starts_thread() { review_source(sessions, ant, &post).await } else { None };
        let id = post.id;
        let (thread, entry) = match source {
            Some(source) => {
                let place = Place::ChannelLink(post.channel.clone(), post.thread_id.clone());
                (source, Entry { place, ..Entry::posted(post, at) })
            }
            None => {
                rooms.entry(post.channel.clone()).or_default().insert(post.thread_id.clone());
                (post.thread_id.clone(), Entry::posted(post, at))
            }
        };
        let evidence = threads.entry(thread).or_default();
        evidence.own.insert(seq, id);
        evidence.entries.insert(seq, entry);
    }
    for (channel, on) in rooms {
        let room = subject::chan_room(channel);
        let reader = chan_reader(ant);
        let read =
            reading::read_back(js, &reader, reading.beside(), &room, window.start(), |seq, at, subject, payload| {
                if at > until {
                    return ControlFlow::Break(());
                }
                match serde_json::from_slice::<ChannelPost>(payload) {
                    Ok(post) if post.check_subject(subject).is_ok() && on.contains(&post.thread_id) => {
                        if let Some(evidence) = threads.get_mut(&post.thread_id) {
                            evidence.entries.entry(seq).or_insert_with(|| Entry::posted(post, at));
                        }
                    }
                    Err(_) => {
                        if let Ok(meta) = serde_json::from_slice::<ChannelMeta>(payload)
                            && subject::parse_chan(subject) == Some((meta.channel, meta.from.clone()))
                            && on.contains(&meta.thread_id)
                            && let Some(evidence) = threads.get_mut(&meta.thread_id)
                        {
                            let entry = Reopening { seq, at, from: meta.from, kind: "unreadable channel post" };
                            evidence.unreadable.insert(seq, entry);
                        }
                    }
                    _ => {}
                }
                ControlFlow::Continue(())
            })
            .await;
        if let Err(e) = read {
            tracing::warn!(%ant, %channel, error = format!("{e:#}"), "reading back the posts of a channel");
        }
    }
}

/// Add the notes of each of `threads` stored in the `window`, the ant's own among them, and those of them that are
/// unreadable.
async fn notes(window: &Window<'_>, threads: &mut BTreeMap<ThreadId, Evidence>) -> Result<()> {
    let Window { js, ant, reading, until, .. } = *window;
    let reader = notes_consumer(ant);
    for (thread, evidence) in threads {
        let filter = subject::notes_of(thread);
        reading::read_back(js, &reader, reading.beside(), &filter, window.start(), |seq, at, subject, payload| {
            if at > until {
                return ControlFlow::Break(());
            }
            match serde_json::from_slice::<NotesPost>(payload) {
                Ok(post) if post.check_subject(subject).is_ok() => {
                    if post.from == *ant {
                        evidence.own.insert(seq, post.id);
                    }
                    evidence.entries.insert(seq, Entry::note(post, at));
                }
                Err(_) => {
                    if let Some((on, from)) = subject::parse_notes(subject)
                        && on == *thread
                    {
                        let entry = Reopening { seq, at, from, kind: "unreadable thread note" };
                        evidence.unreadable.insert(seq, entry);
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        })
        .await
        .with_context(|| format!("reading back the notes of thread {thread}"))?;
    }
    Ok(())
}

// Do not use any of the text, claimed sender or room of an entry that fails the full decode. A thread id in a
// partially readable inbox message or channel post is only useful on a known thread and with a matching subject.
#[derive(Deserialize)]
struct InboxMeta {
    thread_id: ThreadId,
    to: UserId,
    from: UserId,
}

#[derive(Deserialize)]
struct ChannelMeta {
    thread_id: ThreadId,
    channel: ChannelId,
    from: UserId,
}

async fn review_source(sessions: &DirectKv, ant: &UserId, post: &ChannelPost) -> Option<ThreadId> {
    let key = crate::channel::review_source_key(ant, post.id);
    let found = sessions.lookup(key).await;
    match found {
        Ok(Lookup::Value(value)) => {
            let source: ThreadId = std::str::from_utf8(&value).ok()?.parse().ok()?;
            (source != post.thread_id).then_some(source)
        }
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(post = %post.id, error = format!("{e:#}"), "reading the post's review evidence link");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::stream::history::DEFAULT_BUDGET;
    use crate::stream::history::tests::p;

    #[tokio::test]
    async fn the_threads_active_since_a_time_come_most_recent_first_with_their_notes_and_timeline() {
        let Some(server) = ants_test_support::NatsServer::start() else { return };
        let js = async_nats::jetstream::new(async_nats::connect(&server.url).await.unwrap());
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        let (one, two): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());
        let publish = async |subject: String, payload: Vec<u8>| {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap();
        };
        let message = async |thread: &ThreadId, from: &str, to: &str, text: &str| {
            let envelope = Envelope::on_thread(thread.clone(), p(from), p(to), text);
            publish(envelope.subject(), serde_json::to_vec(&envelope).unwrap()).await;
        };
        message(&one, "alice", "dev-a", "long ago").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let since = SystemTime::now();
        message(&one, "alice", "dev-a", "fix the build").await;
        let note = NotesPost::new(one.clone(), p("dev-a"), "the plan");
        publish(note.subject(), serde_json::to_vec(&note).unwrap()).await;
        message(&two, "dev-a", "bob", "a question").await;
        let escalate = async |text: &str, reason, stuck: &str| {
            let escalation = ants_proto::Escalation::new(p(stuck), two.clone(), reason);
            let envelope =
                Envelope::on_thread(two.clone(), p("dev-a"), p("bob"), text).with_escalation(Some(escalation));
            publish(envelope.subject(), serde_json::to_vec(&envelope).unwrap()).await;
        };
        escalate("capped", EscalationReason::Cap, "dev-a").await;
        escalate("still capped", EscalationReason::Cap, "dev-a").await;
        escalate("a tool request", EscalationReason::Approval, "dev-a").await;
        escalate("passed on", EscalationReason::Asked, "dev-c").await;
        message(&one, "alice", "bob", "not to dev-a").await;
        let record = async |thread: &ThreadId, ant: &str, entries: Vec<ants_proto::TimelineEntry>| {
            let record = ants_proto::Timeline::new(p(ant), thread.clone(), ants_proto::Uuid::nil(), entries, 0);
            publish(record.subject(), serde_json::to_vec(&record).unwrap()).await;
        };
        record(&one, "dev-a", call("cargo build")).await;
        record(&one, "dev-a", ended(ants_proto::PromptEnd::Cancelled)).await;
        let rejection = ants_proto::TimelineRejection {
            request: ants_proto::Uuid::from_u128(9),
            kind: "execute".to_owned(),
            title: "rm -rf target".to_owned(),
            by: ants_proto::DecidedBy::Expiry,
            decider: None,
            via: None,
        };
        let rejected = vec![ants_proto::TimelineEntry::Rejected(rejection)];
        let alone = ants_proto::Timeline::new(p("dev-a"), one.clone(), ants_proto::Uuid::from_u128(8), rejected, 0);
        publish(alone.subject(), serde_json::to_vec(&alone).unwrap()).await;
        record(&one, "bob", call("not dev-a's")).await;
        record(&"t3".parse().unwrap(), "dev-a", call("no thread active")).await;
        let until = SystemTime::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        message(&two, "bob", "dev-a", "too late").await;
        record(&two, "dev-a", call("cargo late")).await;

        let threads = active(&js, &p("dev-a"), Reading::Gets, ReviewRecords::default(), (since, until), DEFAULT_BUDGET)
            .await
            .unwrap();
        assert_eq!(threads.iter().map(|active| active.thread.clone()).collect::<Vec<_>>(), [two, one]);
        assert!(threads[0].history.contains("a question") && !threads[0].history.contains("too late"));
        assert_eq!(threads[0].timeline, "", "{threads:?}");
        let first = &threads[1].history;
        assert!(first.contains("fix the build") && first.contains("the plan"), "{first}");
        assert!(!first.contains("long ago") && !first.contains("not to dev-a"), "{first}");
        let timeline = &threads[1].timeline;
        assert!(timeline.contains("execute: cargo build (completed)\n") && !timeline.contains("dev-a's"), "{timeline}");
        assert!(timeline.contains(" cancelled by a human after 1 s\n"), "{timeline}");
        assert_eq!(threads[1].ends.len(), 1);
        assert!(threads[0].ends.is_empty());
        assert_eq!((threads[0].escalations, threads[1].escalations), (1, 0), "a reason once, tool requests aside");
        assert_eq!(
            timeline.matches("the prompt of message").count(),
            1,
            "a rejection alone shows no prompt: {timeline}"
        );
        let rejected: Vec<_> = threads[1].rejections.iter().map(|r| r.rejection.title.as_str()).collect();
        assert_eq!(rejected, ["rm -rf target"]);
        assert!(threads[0].rejections.is_empty());
        let sent = |active: &Active| active.sent.iter().map(|sent| sent.opening.clone()).collect::<Vec<_>>();
        assert_eq!(
            (sent(&threads[0]), sent(&threads[1])),
            (
                ["a question", "capped", "still capped", "a tool request", "passed on"].map(str::to_owned).to_vec(),
                vec!["the plan".to_owned()]
            )
        );
        assert_eq!(threads[1].sent[0].id, note.id);
        let reopenings: Vec<_> = threads[1].reopenings.iter().map(|r| (r.from.as_str(), r.kind)).collect();
        assert_eq!(reopenings, [("alice", "message"), ("dev-a", "thread note")]);
        assert!(threads[1].reopenings.is_sorted_by_key(|r| r.seq));
    }

    #[tokio::test]
    async fn the_threads_the_ant_posted_on_in_a_channel_are_active_with_the_channels_posts_on_them() {
        let Some(server) = ants_test_support::NatsServer::start() else { return };
        let js = async_nats::jetstream::new(async_nats::connect(&server.url).await.unwrap());
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        let post = async |post: &ChannelPost| {
            js.publish(post.subject(), serde_json::to_vec(post).unwrap().into()).await.unwrap().await.unwrap();
        };
        let (ops, dev): (ChannelId, ChannelId) = ("ops".parse().unwrap(), "dev".parse().unwrap());
        let asked = ChannelPost::new(ops.clone(), p("bob"), "who broke the build?");
        let thread = asked.thread_id.clone();
        post(&ChannelPost::on_thread(ops.clone(), thread.clone(), p("bob"), "long ago")).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let since = SystemTime::now();
        post(&asked).await;
        let answer = ChannelPost::on_thread(ops.clone(), thread.clone(), p("dev-a"), "mine, fixing it");
        post(&answer).await;
        post(&ChannelPost::new(ops.clone(), p("carol"), "another thread")).await;
        post(&ChannelPost::on_thread(dev.clone(), thread.clone(), p("carol"), "a channel dev-a posted none in")).await;
        post(&ChannelPost::on_thread(ops.clone(), thread.clone(), p("bob"), "thanks")).await;
        let until = SystemTime::now();
        tokio::time::sleep(Duration::from_millis(50)).await;
        post(&ChannelPost::on_thread(ops.clone(), thread.clone(), p("bob"), "too late")).await;

        let threads = active(&js, &p("dev-a"), Reading::Gets, ReviewRecords::default(), (since, until), DEFAULT_BUDGET)
            .await
            .unwrap();
        assert_eq!(threads.iter().map(|active| &active.thread).collect::<Vec<_>>(), [&thread]);
        let history = &threads[0].history;
        assert!(history.contains("bob in channel ops:\nwho broke the build?"), "{history}");
        assert!(history.contains("mine, fixing it") && history.contains("thanks"), "{history}");
        for left in ["long ago", "another thread", "posted none in", "too late"] {
            assert!(!history.contains(left), "{left}: {history}");
        }
        assert_eq!(threads[0].sent, [Sent::new(answer.id, "mine, fixing it")]);
        let reopenings: Vec<_> = threads[0].reopenings.iter().map(|r| (r.from.as_str(), r.kind)).collect();
        assert_eq!(reopenings, [("bob", "channel post"), ("dev-a", "channel post"), ("bob", "channel post")]);
    }

    #[tokio::test]
    async fn review_keeps_channels_the_ant_worked_on_without_replying() {
        let Some(server) = ants_test_support::NatsServer::start() else { return };
        let js = async_nats::jetstream::new(async_nats::connect(&server.url).await.unwrap());
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        js.create_key_value(crate::hub::kept_config(SESSIONS_BUCKET)).await.unwrap();
        let ant = p("dev-a");
        let origins = OriginStore::kv(DirectKv::new(&js, None, SESSIONS_BUCKET).await.unwrap(), ant.clone());
        let channel: ChannelId = "ops".parse().unwrap();
        let since = SystemTime::now();
        let wake = ChannelPost::new(channel.clone(), p("bob"), "@dev-a check this");
        let skipped = ChannelPost::new(channel.clone(), p("alice"), "another conversation");
        for post in [&wake, &skipped] {
            crate::bus::store(&js, post.subject(), post, &post.id.to_string()).await.unwrap();
            let origin = Origin {
                by: ants_proto::provider::Starter::Human { user: post.from.clone() },
                channel: Some(channel.clone()),
            };
            origins.put(&post.thread_id, &origin).await.unwrap();
        }
        let turn = ants_proto::Timeline::new(
            ant.clone(),
            wake.thread_id.clone(),
            wake.id,
            ended(ants_proto::PromptEnd::NoReply),
            0,
        );
        crate::bus::store(&js, turn.subject(), &turn, &format!("timeline.{}", turn.message)).await.unwrap();
        let threads = active(
            &js,
            &ant,
            Reading::Consumer,
            ReviewRecords { origins: Some(&origins), ..ReviewRecords::default() },
            (since, SystemTime::now()),
            DEFAULT_BUDGET,
        )
        .await
        .unwrap();
        let [worked] = threads.as_slice() else { panic!("{threads:?}") };
        assert_eq!(worked.thread, wake.thread_id);
        assert!(worked.history.contains("bob in channel ops:\n@dev-a check this"), "{worked:?}");
        assert!(!worked.history.contains("another conversation"));
        assert_eq!(worked.ends.len(), 1);
        assert!(worked.sent.is_empty(), "the ant posted nothing");
    }

    #[tokio::test]
    async fn a_top_level_mcp_post_is_evidence_on_its_inbox_thread_but_not_a_reopening() {
        let Some(server) = ants_test_support::NatsServer::start() else { return };
        let js = async_nats::jetstream::new(async_nats::connect(&server.url).await.unwrap());
        js.create_stream(crate::hub::stream_config()).await.unwrap();
        js.create_key_value(crate::hub::kept_config(SESSIONS_BUCKET)).await.unwrap();
        let since = SystemTime::now();
        let ant = p("dev-a");
        let ask = Envelope::new(p("alice"), ant.clone(), "send a public update");
        crate::bus::publish(&js, &ask).await.unwrap();
        let post = ChannelPost::new("ops".parse().unwrap(), ant.clone(), "build fixed");
        let access = crate::mcp::jetstream_channels(js.clone(), None);
        (access.post)(post.clone(), Some(ask.thread_id.clone())).await.unwrap();
        let answer = ChannelPost::on_thread(post.channel.clone(), post.thread_id.clone(), p("bob"), "thanks");
        crate::bus::store(&js, answer.subject(), &answer, &answer.id.to_string()).await.unwrap();
        let public = js.get_stream(ants_proto::STREAM).await.unwrap().direct_get_last_for_subject(post.subject()).await;
        let public = public.unwrap();
        assert_eq!(serde_json::from_slice::<ChannelPost>(&public.payload).unwrap(), post);
        assert!(!std::str::from_utf8(&public.payload).unwrap().contains(ask.thread_id.as_str()));

        let threads =
            active(&js, &ant, Reading::Consumer, ReviewRecords::default(), (since, SystemTime::now()), DEFAULT_BUDGET)
                .await
                .unwrap();
        let [inbox] = threads.as_slice() else { panic!("{threads:?}") };
        assert_eq!(inbox.thread, ask.thread_id);
        assert!(inbox.history.contains(&format!("started thread {} in channel ops:\nbuild fixed", post.thread_id)));
        assert!(!inbox.history.contains("thanks"), "other channel posts remain on their channel thread");
        assert_eq!(inbox.sent, [Sent::new(post.id, "build fixed")], "reactions use the real channel post id");
        assert_eq!(inbox.reopenings.len(), 1);
        assert_eq!(inbox.reopenings[0].kind, "message");
    }

    /// A timeline's entries: a completed call of `title`.
    fn call(title: &str) -> Vec<ants_proto::TimelineEntry> {
        let call = ants_proto::TimelineCall {
            id: title.to_owned(),
            tool_kind: Some("execute".to_owned()),
            title: title.to_owned(),
            status: Some("completed".to_owned()),
            started: chrono::Utc::now(),
            duration_ms: None,
            paths: Vec::new(),
        };
        vec![ants_proto::TimelineEntry::Tool(call)]
    }

    /// A timeline's entries: a prompt that ended `how` after a second.
    fn ended(how: ants_proto::PromptEnd) -> Vec<ants_proto::TimelineEntry> {
        let end = ants_proto::TimelineEnd {
            how,
            duration_ms: 1_000,
            tokens: 0,
            cost: 0.0,
            asked: 0,
            rebuilt: false,
            calls: std::collections::BTreeMap::new(),
            sense_calls: 0,
        };
        vec![ants_proto::TimelineEntry::Ended(end)]
    }

    #[test]
    fn the_opening_of_a_message_sent_is_one_short_line() {
        let id = ants_proto::Uuid::nil();
        assert_eq!(Sent::new(id, "  fixed\nthe build \n").opening, "fixed the build");
        let long = format!("{}é and more", "x".repeat(OPENING_BYTES - 1));
        assert_eq!(Sent::new(id, &long).opening, format!("{}…", "x".repeat(OPENING_BYTES - 1)));
    }
}
