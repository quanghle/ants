//! `ants thread`: the threads in the ANTS stream, across inboxes (both ways), channel posts, notes, escalations, tool
//! requests and their decisions, and dead letters.
//!
//! Read back by a human one message at a time (`STREAM.MSG.GET`, [`Reading::Gets`]), so nothing is consumed or acked
//! and no consumer is left behind. Only notes subjects name their thread, so a read goes through every message stored
//! in its time window: `--since` bounds it, and `ants thread show` of a thread with a time-ordered id starts at the
//! thread's start.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;
use std::time::{Duration, SystemTime};

use ants_daemon::reading::{self, Reading};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, Envelope, NotesPost, ThreadId, UserId, Uuid,
    subject,
};
use anyhow::{Result, bail};
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::tools;

/// How long the opening message's line in `ants thread list` may be.
const OPENING: usize = 72;

/// What a stored message on a thread is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// An inbox message, between ants or between an ant and a human.
    Message,
    /// An inbox message carrying an escalation.
    Escalation,
    /// An inbox message telling a human member of a channel post that @mentioned it.
    Mention,
    /// A channel post.
    Post,
    /// A post to the thread's notes.
    Note,
    /// A tool request an ant escalated to the humans.
    Approval,
    /// A decision on a tool request: a human's, or an agent's of the requester's chain.
    Decision,
    /// A message an ant gave up on.
    Dead,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Escalation => "escalation",
            Self::Mention => "mention",
            Self::Post => "post",
            Self::Note => "note",
            Self::Approval => "approval",
            Self::Decision => "decision",
            Self::Dead => "dead",
        }
    }
}

/// One stored message on a thread, as `ants thread show` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub seq: u64,
    /// When the stream stored it.
    pub stored_at: DateTime<Utc>,
    pub kind: Kind,
    pub thread_id: ThreadId,
    /// The sender; none for a human's decision on a tool request, which does not say which human decided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<UserId>,
    /// The recipient of an inbox message, the ant a decision or dead letter is for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<UserId>,
    /// The channel of a post or of the post a mention tells of.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    /// The thread's owner as the message names it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// What the kind adds: an escalation's level and chain, a tool request's id, a dead letter's error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub text: String,
    /// The subject it was stored on names another sender (or ant) than the message: the message is forged.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub forged: bool,
}

impl Entry {
    fn new(seq: u64, stored_at: DateTime<Utc>, kind: Kind, thread_id: ThreadId) -> Self {
        Self {
            seq,
            stored_at,
            kind,
            thread_id,
            from: None,
            to: None,
            channel: None,
            owner: None,
            note: None,
            text: String::new(),
            forged: false,
        }
    }

    /// Who sent it to whom: `alice -> dev-a`, `dev-a -> #ops`, `dev-a -> notes`, `(a human) -> dev-a`.
    fn route(&self) -> String {
        let from = self.from.as_ref().map_or("(a human)", UserId::as_str);
        match (self.kind, &self.to, &self.channel) {
            (Kind::Post, _, Some(channel)) => format!("{from} -> #{channel}"),
            (Kind::Note, ..) => format!("{from} -> notes"),
            (Kind::Approval, ..) => format!("{from} -> humans"),
            (_, Some(to), _) => format!("{from} -> {to}"),
            (_, None, _) => from.to_owned(),
        }
    }

    /// The users it was between.
    fn users(&self) -> impl Iterator<Item = &UserId> {
        self.from.iter().chain(&self.to)
    }
}

/// Turns stored messages into [`Entry`]s, in stream order. A human's decision does not name its request's thread, so
/// the threads of the requests read so far are kept to place it.
#[derive(Default)]
pub struct Parser {
    requests: HashMap<(UserId, Uuid), ThreadId>,
}

impl Parser {
    /// The entry of the message stored at `seq` on `subject`, None for one that is not ants' or does not parse.
    pub fn entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        if subject::parse_inbox(subject).is_some() {
            let e: Envelope = serde_json::from_slice(payload).ok()?;
            let forged = e.check_subject(subject).is_err();
            let (kind, note, text) = if let Some(d) = &e.approval {
                let text = format!("{} on {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
                (Kind::Decision, None, text)
            } else if let Some(x) = &e.escalation {
                (Kind::Escalation, Some(crate::escalation_note(x)), e.text)
            } else if e.mention.is_some() {
                (Kind::Mention, None, e.text)
            } else {
                (Kind::Message, None, e.text)
            };
            let channel = e.mention.map(|m| m.channel);
            return Some(Entry {
                from: Some(e.from),
                to: Some(e.to),
                channel,
                owner: e.owner,
                note,
                text,
                forged,
                ..entry(kind, e.thread_id)
            });
        }
        if subject::parse_chan(subject).is_some() {
            let post: ChannelPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            return Some(Entry {
                from: Some(post.from),
                channel: Some(post.channel),
                owner: post.owner,
                note,
                text: post.text,
                forged,
                ..entry(Kind::Post, post.thread_id)
            });
        }
        if subject::parse_notes(subject).is_some() {
            let post: NotesPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            return Some(Entry {
                from: Some(post.from),
                note,
                text: post.text,
                forged,
                ..entry(Kind::Note, post.thread_id)
            });
        }
        if let Some(rest) = subject.strip_prefix("ants.approvals.") {
            if rest.contains('.') {
                let d: ApprovalDecision = serde_json::from_slice(payload).ok()?;
                let thread = self.requests.get(&(d.ant.clone(), d.request_id))?.clone();
                let forged = d.subject() != subject;
                let text = format!("{} on {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
                return Some(Entry { to: Some(d.ant), text, forged, ..entry(Kind::Decision, thread) });
            }
            let r: ApprovalRequest = serde_json::from_slice(payload).ok()?;
            let forged = r.check_subject(subject).is_err();
            if !forged {
                self.requests.insert((r.ant.clone(), r.id), r.thread_id.clone());
            }
            let options = r.options.iter().map(|v| tools::spelling(*v)).collect::<Vec<_>>().join("|");
            let note = Some(format!("{}/{} ({options})", r.ant, r.id));
            let text = format!("{}: {}", r.tool.kind, r.tool.title);
            return Some(Entry { from: Some(r.ant), note, text, forged, ..entry(Kind::Approval, r.thread_id) });
        }
        if let Some(ant) = subject.strip_prefix("ants.dead.") {
            let d: DeadLetter = serde_json::from_slice(payload).ok()?;
            let forged = d.ant.as_str() != ant;
            let note = Some(format!("after {} deliveries: {}", d.deliveries, d.error));
            let e = d.envelope?;
            return Some(Entry {
                from: Some(e.from),
                to: Some(d.ant),
                owner: e.owner,
                note,
                text: e.text,
                forged,
                ..entry(Kind::Dead, e.thread_id)
            });
        }
        None
    }
}

/// One thread as `ants thread list` prints it, from the messages read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Summary {
    pub thread_id: ThreadId,
    /// The thread's owner, as its first message naming one says.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// Every ant and human that sent or received a message on it.
    pub users: BTreeSet<UserId>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub channels: BTreeSet<ChannelId>,
    pub first_at: DateTime<Utc>,
    pub last_at: DateTime<Utc>,
    pub messages: usize,
    /// The first line of the first message read.
    pub opening: String,
    /// The first line matching `--grep`, after its sender.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<String>,
}

/// What `ants thread list` keeps of the threads read.
#[derive(Debug, Default, Clone)]
pub struct Select {
    /// Only threads this user sent or received a message on.
    pub ant: Option<UserId>,
    /// Only threads this human owns.
    pub owner: Option<UserId>,
    /// Only threads with a message whose text has this, whatever the case.
    pub grep: Option<String>,
    /// Only the most recently active threads, this many.
    pub limit: Option<usize>,
}

/// The threads of the entries handed to [`Threads::add`].
#[derive(Debug, Default)]
pub struct Threads {
    by_id: HashMap<ThreadId, Summary>,
    /// `--grep`'s text, lowercased.
    grep: Option<String>,
}

impl Threads {
    pub fn new(grep: Option<&str>) -> Self {
        Self { by_id: HashMap::new(), grep: grep.map(str::to_lowercase) }
    }

    pub fn add(&mut self, entry: &Entry) {
        let summary = self.by_id.entry(entry.thread_id.clone()).or_insert_with(|| Summary {
            thread_id: entry.thread_id.clone(),
            owner: None,
            users: BTreeSet::new(),
            channels: BTreeSet::new(),
            first_at: entry.stored_at,
            last_at: entry.stored_at,
            messages: 0,
            opening: crate::excerpt(&entry.text, OPENING),
            matched: None,
        });
        summary.messages += 1;
        summary.first_at = summary.first_at.min(entry.stored_at);
        summary.last_at = summary.last_at.max(entry.stored_at);
        summary.users.extend(entry.users().cloned());
        summary.channels.extend(entry.channel.iter().cloned());
        if summary.owner.is_none() {
            summary.owner.clone_from(&entry.owner);
        }
        if let (Some(grep), None) = (&self.grep, &summary.matched) {
            summary.matched = entry.text.lines().find(|line| line.to_lowercase().contains(grep.as_str())).map(|line| {
                let from = entry.from.as_ref().map_or("(a human)", UserId::as_str);
                format!("{from}: {}", crate::excerpt(line.trim(), OPENING))
            });
        }
    }

    /// The threads `select` keeps, least recently active first, and how many more it left out for its limit.
    pub fn finish(self, select: &Select) -> (Vec<Summary>, usize) {
        let mut threads: Vec<Summary> = self
            .by_id
            .into_values()
            .filter(|t| select.ant.as_ref().is_none_or(|ant| t.users.contains(ant)))
            .filter(|t| select.owner.is_none() || t.owner == select.owner)
            .filter(|t| select.grep.is_none() || t.matched.is_some())
            .collect();
        threads.sort_by(|a, b| (a.last_at, &a.thread_id).cmp(&(b.last_at, &b.thread_id)));
        let omitted = select.limit.map_or(0, |limit| threads.len().saturating_sub(limit));
        threads.drain(..omitted);
        (threads, omitted)
    }
}

fn time(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// The threads as a table: one row per thread under a header, columns padded to line up, and under a thread
/// matching `--grep` its matching line.
pub fn table(threads: &[Summary]) -> String {
    let header = ["THREAD", "OWNER", "USERS", "FIRST", "LAST", "MSGS", "OPENING"].map(str::to_owned);
    let rows: Vec<[String; 7]> = std::iter::once(header)
        .chain(threads.iter().map(|t| {
            let users = t.users.iter().map(UserId::as_str);
            let channels = t.channels.iter().map(|c| format!("#{c}"));
            [
                t.thread_id.to_string(),
                t.owner.as_ref().map_or("-", UserId::as_str).to_owned(),
                users.map(str::to_owned).chain(channels).collect::<Vec<_>>().join(","),
                time(t.first_at),
                time(t.last_at),
                t.messages.to_string(),
                t.opening.clone(),
            ]
        }))
        .collect();
    let mut widths = [0; 7];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut lines = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
        lines.push(cells.join("  ").trim_end().to_owned());
        if let Some(matched) = i.checked_sub(1).and_then(|i| threads[i].matched.as_ref()) {
            lines.push(format!("    {matched}"));
        }
    }
    lines.join("\n")
}

/// The entries of a thread as `ants thread show` prints them: time, kind, route, note and text, one entry per line,
/// its text's further lines indented under it.
pub fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    for e in entries {
        let forged = if e.forged { " (forged)" } else { "" };
        let note = e.note.as_ref().map(|n| format!(" [{n}]")).unwrap_or_default();
        let mut lines = e.text.lines();
        let first = lines.next().unwrap_or("");
        let kind = e.kind.as_str();
        out.push_str(&format!("{}  {kind:<10}  {}{forged}{note}: {first}\n", time(e.stored_at), e.route()));
        for line in lines {
            out.push_str(&format!("    {line}\n"));
        }
    }
    out
}

/// Where a read of the last `since` starts: everything kept for zero.
fn window(since: Duration) -> consumer::DeliverPolicy {
    match SystemTime::now().checked_sub(since) {
        Some(start) if !since.is_zero() => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        _ => consumer::DeliverPolicy::All,
    }
}

/// Where `ants thread show` reads `thread` from: as `since` says, else the thread's start when its id is
/// time-ordered, else everything kept.
fn show_start(thread: &ThreadId, since: Option<Duration>) -> consumer::DeliverPolicy {
    match (since, ants_daemon::channel::thread_start(thread)) {
        (Some(since), _) => window(since),
        (None, Some(start)) => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        (None, None) => consumer::DeliverPolicy::All,
    }
}

/// Hand `take` the entry of every message stored from `start` on, oldest first.
pub async fn scan(js: &jetstream::Context, start: consumer::DeliverPolicy, mut take: impl FnMut(Entry)) -> Result<()> {
    let mut parser = Parser::default();
    reading::read_back(js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
        match parser.entry(seq, stored.into(), subject, payload) {
            Some(entry) => take(entry),
            None => tracing::debug!(seq, subject, "skipping a message on no thread"),
        }
        ControlFlow::Continue(())
    })
    .await
}

/// Connect as the CLI's human: only a human's credentials get stream messages by sequence.
async fn connect(nats: &crate::Nats<'_>) -> Result<jetstream::Context> {
    if !nats.me.role.is_human() {
        bail!(
            "the credentials of {} are an ant's, which read no other inbox; `ants thread` reads them all with a \
             human's",
            nats.me.id
        );
    }
    Ok(nats.connect("ants-thread").await?.1)
}

/// `ants thread list`: the threads with messages stored within `since`, as `select` keeps them.
pub async fn list(nats: &crate::Nats<'_>, since: Duration, select: &Select, json: bool) -> Result<()> {
    let js = connect(nats).await?;
    let mut threads = Threads::new(select.grep.as_deref());
    scan(&js, window(since), |entry| threads.add(&entry)).await?;
    let (threads, omitted) = threads.finish(select);
    if json {
        for thread in &threads {
            println!("{}", serde_json::to_string(thread)?);
        }
    } else if threads.is_empty() {
        println!("no threads");
    } else {
        println!("{}", table(&threads));
    }
    if omitted > 0 {
        eprintln!("{omitted} less recently active threads left out: raise --limit or narrow --since");
    }
    Ok(())
}

/// `ants thread show THREAD`: every message stored on `thread`, in the order the stream stored them.
pub async fn show(nats: &crate::Nats<'_>, thread: &ThreadId, since: Option<Duration>, json: bool) -> Result<()> {
    let js = connect(nats).await?;
    let mut entries = Vec::new();
    scan(&js, show_start(thread, since), |entry| {
        if entry.thread_id == *thread {
            entries.push(entry);
        }
    })
    .await?;
    if json {
        for entry in &entries {
            println!("{}", serde_json::to_string(entry)?);
        }
    } else if entries.is_empty() {
        println!("no messages on thread {thread}");
    } else {
        let owner = entries.iter().find_map(|e| e.owner.as_ref());
        match owner {
            Some(owner) => println!("thread {thread}, owned by {owner}"),
            None => println!("thread {thread}"),
        }
        print!("{}", render(&entries));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_proto::{ApprovalRequest, Escalation, EscalationReason, ToolCall, Verdict};

    use super::*;

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    fn t(s: &str) -> ThreadId {
        s.parse().unwrap()
    }

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap()
    }

    fn json<T: Serialize>(value: &T) -> Vec<u8> {
        serde_json::to_vec(value).unwrap()
    }

    /// The entries of `messages`, each a subject and a payload, stored a second apart.
    fn entries(messages: &[(String, Vec<u8>)]) -> Vec<Entry> {
        let mut parser = Parser::default();
        let parsed = messages.iter().enumerate();
        parsed
            .filter_map(|(i, (subject, payload))| parser.entry(i as u64 + 1, at(i as i64), subject, payload))
            .collect()
    }

    #[test]
    fn every_kind_of_stored_message_lands_on_its_thread() {
        let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
        let ask = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "fix it").with_owner(Some(alice.clone()));
        let reply = ask.reply(dev_a.clone(), "done\nsee the diff", ants_proto::Role::Human).unwrap();
        let post = ChannelPost::on_thread("ops".parse().unwrap(), t("t1"), dev_b.clone(), "@dev-a nice");
        let note = NotesPost::new(t("t1"), dev_a.clone(), "plan");
        let escalation = Escalation::new(dev_a.clone(), t("t1"), EscalationReason::TurnBudget);
        let up = Envelope::on_thread(t("t1"), dev_a.clone(), dev_b.clone(), "help").with_escalation(Some(escalation));
        let request = ApprovalRequest::new(
            dev_a.clone(),
            t("t1"),
            ToolCall { kind: "execute".into(), title: "rm -rf build".into(), ..Default::default() },
            vec![Verdict::AllowOnce],
            Duration::from_secs(60),
        );
        let decision = request.decide(Verdict::AllowOnce);
        let dead = DeadLetter::of_envelope(dev_a.clone(), 3, 5, "harness died", ask.clone());
        let unknown = ApprovalDecision::new(Uuid::now_v7(), dev_a.clone(), Verdict::Reject);
        let messages = [
            (ask.subject(), json(&ask)),
            (reply.subject(), json(&reply)),
            (post.subject(), json(&post)),
            (note.subject(), json(&note)),
            (up.subject(), json(&up)),
            (request.subject(), json(&request)),
            (decision.subject(), json(&decision)),
            (dead.subject(), json(&dead)),
            // A decision on a request not read, and a payload of no kind, are on no thread known.
            (unknown.subject(), json(&unknown)),
            ("ants.inbox.dev-a.alice".to_owned(), b"not json".to_vec()),
        ];
        let entries = entries(&messages);
        let kinds: Vec<Kind> = entries.iter().map(|e| e.kind).collect();
        let expected = [
            Kind::Message,
            Kind::Message,
            Kind::Post,
            Kind::Note,
            Kind::Escalation,
            Kind::Approval,
            Kind::Decision,
            Kind::Dead,
        ];
        assert_eq!(kinds, expected);
        assert!(entries.iter().all(|e| e.thread_id == t("t1") && !e.forged), "{entries:?}");
        assert_eq!(entries[1].route(), "dev-a -> alice");
        assert_eq!(entries[2].route(), "dev-b -> #ops");
        assert_eq!(entries[3].route(), "dev-a -> notes");
        assert!(entries[4].note.as_deref().unwrap().contains("turn_budget"), "{:?}", entries[4]);
        assert_eq!(entries[6].route(), "(a human) -> dev-a");
        assert!(entries[6].text.starts_with("allow-once on dev-a/"), "{}", entries[6].text);
        assert_eq!(entries[7].note.as_deref(), Some("after 5 deliveries: harness died"));

        let shown = render(&entries[..2]);
        assert!(shown.contains("message     alice -> dev-a: fix it\n"), "{shown}");
        assert!(shown.ends_with("dev-a -> alice: done\n    see the diff\n"), "{shown}");
    }

    #[test]
    fn a_message_on_another_sender_s_subject_is_marked_forged() {
        let e = Envelope::on_thread(t("t1"), p("alice"), p("dev-a"), "hi");
        let entries = entries(&[("ants.inbox.dev-a.mallory".to_owned(), json(&e))]);
        assert!(entries[0].forged);
        assert!(render(&entries).contains("alice -> dev-a (forged): hi"));
    }

    #[test]
    fn threads_sum_up_their_users_owner_times_and_opening() {
        let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
        let one = Envelope::on_thread(t("t1"), alice.clone(), dev_a.clone(), "first line\nmore");
        let two = Envelope::on_thread(t("t1"), dev_a.clone(), dev_b.clone(), "Deploy failed").with_owner(Some(alice));
        let other = ChannelPost::on_thread("ops".parse().unwrap(), t("t2"), dev_b.clone(), "unrelated");
        let three = two.reply(dev_b.clone(), "retrying", ants_proto::Role::Ant).unwrap();
        let messages = [
            (one.subject(), json(&one)),
            (two.subject(), json(&two)),
            (other.subject(), json(&other)),
            (three.subject(), json(&three)),
        ];
        let read = entries(&messages);
        let threads = |select: &Select| {
            let mut threads = Threads::new(select.grep.as_deref());
            read.iter().for_each(|e| threads.add(e));
            threads.finish(select)
        };

        let (all, omitted) = threads(&Select::default());
        assert_eq!(omitted, 0);
        let ids: Vec<&str> = all.iter().map(|t| t.thread_id.as_str()).collect();
        assert_eq!(ids, ["t2", "t1"], "least recently active first");
        let t1 = &all[1];
        assert_eq!(t1.owner, Some(p("alice")));
        assert_eq!(t1.users.iter().map(UserId::as_str).collect::<Vec<_>>(), ["alice", "dev-a", "dev-b"]);
        assert_eq!((t1.first_at, t1.last_at, t1.messages), (at(0), at(3), 3));
        assert_eq!(t1.opening, "first line…");
        assert_eq!(all[0].channels.iter().map(ChannelId::as_str).collect::<Vec<_>>(), ["ops"]);

        let ids = |(threads, _): (Vec<Summary>, usize)| -> Vec<String> {
            threads.into_iter().map(|t| t.thread_id.to_string()).collect()
        };
        assert_eq!(ids(threads(&Select { ant: Some(p("alice")), ..Default::default() })), ["t1"]);
        assert_eq!(ids(threads(&Select { ant: Some(p("dev-b")), ..Default::default() })), ["t2", "t1"]);
        assert_eq!(ids(threads(&Select { owner: Some(p("alice")), ..Default::default() })), ["t1"]);
        assert!(ids(threads(&Select { owner: Some(p("bob")), ..Default::default() })).is_empty());
        let (limited, omitted) = threads(&Select { limit: Some(1), ..Default::default() });
        assert_eq!((limited[0].thread_id.as_str(), omitted), ("t1", 1));

        let (found, _) = threads(&Select { grep: Some("deploy FAILED".into()), ..Default::default() });
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].matched.as_deref(), Some("dev-a: Deploy failed"));
        let table = table(&found);
        assert!(table.starts_with("THREAD  OWNER  USERS"), "{table}");
        assert!(table.ends_with("\n    dev-a: Deploy failed"), "{table}");
    }

    #[test]
    fn show_starts_at_a_time_ordered_thread_s_start_or_reads_everything() {
        assert!(matches!(show_start(&t("t1"), None), consumer::DeliverPolicy::All));
        assert!(matches!(show_start(&ThreadId::generate(), None), consumer::DeliverPolicy::ByStartTime { .. }));
        assert!(matches!(
            show_start(&t("t1"), Some(Duration::from_secs(60))),
            consumer::DeliverPolicy::ByStartTime { .. }
        ));
        assert!(matches!(window(Duration::ZERO), consumer::DeliverPolicy::All));
    }

    #[tokio::test]
    async fn a_human_reads_threads_back_without_consuming_them() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, dev_a) = (p("alice"), p("dev-a"));
        let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        ants_daemon::bus::init_stream(&js).await.unwrap();
        let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it").with_owner(Some(alice.clone()));
        let reply = ask.reply(dev_a.clone(), "built", ants_proto::Role::Human).unwrap();
        let note = NotesPost::new(ask.thread_id.clone(), alice.clone(), "remember the tests");
        let other = Envelope::new(alice.clone(), dev_a.clone(), "another thing");
        for (subject, payload) in [
            (ask.subject(), json(&ask)),
            (reply.subject(), json(&reply)),
            (note.subject(), json(&note)),
            (other.subject(), json(&other)),
        ] {
            js.publish(subject, payload.into()).await.unwrap().await.unwrap();
        }

        let mut threads = Threads::new(None);
        scan(&js, window(Duration::from_secs(3600)), |e| threads.add(&e)).await.unwrap();
        let (threads, _) = threads.finish(&Select::default());
        assert_eq!(threads.len(), 2);
        let built = threads.iter().find(|t| t.thread_id == ask.thread_id).unwrap();
        assert_eq!((built.messages, built.owner.as_ref()), (3, Some(&alice)));

        let mut shown = Vec::new();
        scan(&js, show_start(&ask.thread_id, None), |e| {
            if e.thread_id == ask.thread_id {
                shown.push(e.kind);
            }
        })
        .await
        .unwrap();
        assert_eq!(shown, [Kind::Message, Kind::Message, Kind::Note]);

        let stream = js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().clone();
        assert_eq!((stream.state.messages, stream.state.consumer_count), (4, 0));
    }
}
