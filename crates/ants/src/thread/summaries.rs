//! The threads of `ants thread list`: each one's [`Summary`], gathered entry by entry ([`Threads`]) and selected
//! ([`Select`]).

use std::collections::{BTreeSet, HashMap};

use ants_proto::{Acting, ChannelId, EscalationReason, Humans, Mark, ThreadId, ThreadMarks, UserId};
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::OPENING;
use super::entry::{Entry, Kind};
use super::text::time;

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
    /// With `--escalated`, the escalation that reached a human and no human's message followed: when it first did,
    /// to whom, and the escalation's note.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub escalated: Option<String>,
    /// The thread's title, which any human sets.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The first title an ant suggested for it, and that ant.
    #[serde(skip)]
    pub suggested: Option<(UserId, String)>,
    /// Whether a sealed message between two humans is on it, so no ant's suggested title is stored for it.
    #[serde(skip)]
    pub sealed: bool,
    /// Pinned by the CLI's human.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub pinned: bool,
    /// Archived by the CLI's human.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub archived: bool,
    /// Archived, and a message was stored on it since.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub new_activity: bool,
    /// Archived for having been idle (`auto_archive`), not by hand.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub auto_archived: bool,
    /// Marked done by a human, and no message, post or note stored on it since.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub done: Option<Done>,
    /// The stream sequence of its last message read.
    #[serde(skip)]
    pub last_seq: u64,
    /// Opened by a summary request ([`Envelope::summary`]): a message read on it carries one. Left out of every list.
    ///
    /// [`Envelope::summary`]: ants_proto::Envelope::summary
    #[serde(skip)]
    pub catch_up: bool,
    /// The stream sequence of its last message, post or note read ([`Kind::reopens`]); 0 for none.
    #[serde(skip)]
    pub last_reopening: u64,
}

/// Who marked a thread done, and when ([`ants_proto::ThreadDone`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Done {
    pub by: UserId,
    pub at: DateTime<Utc>,
    /// For whom `by` marked it, as its `on_behalf_of` checked ([`crate::behalf::Checker`]).
    #[serde(skip_serializing_if = "Acting::is_own")]
    pub acting: Acting,
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
    /// Only threads with an open escalation ([`Threads::escalated`]).
    pub escalated: bool,
    /// Only the threads the CLI's human pinned.
    pub pinned: bool,
    /// Only the threads the CLI's human archived; otherwise only those with a message stored since are kept of them.
    pub archived: bool,
    /// Threads with no message stored after this, neither pinned, archived nor kept since, count as archived
    /// ([`ThreadMarks::idle`]).
    pub idle_before: Option<DateTime<Utc>>,
}

/// The threads of the entries handed to [`Threads::add`].
#[derive(Debug, Default)]
pub struct Threads {
    by_id: HashMap<ThreadId, Summary>,
    /// `--grep`'s text, lowercased.
    grep: Option<String>,
    /// With `--escalated`, the team's humans.
    humans: Option<Humans>,
}

impl Threads {
    pub fn new(grep: Option<&str>) -> Self {
        Self { by_id: HashMap::new(), grep: grep.map(str::to_lowercase), humans: None }
    }

    /// Keep each thread's open escalation: the first of the escalations reaching one of `humans` since a human's
    /// last message or post on the thread, other than a tool request's. A human's message on the thread ends it, as
    /// it ends the escalation in the daemon.
    pub fn escalated(self, humans: Humans) -> Self {
        Self { humans: Some(humans), ..self }
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
            opening: crate::text::excerpt(&entry.text, OPENING),
            matched: None,
            escalated: None,
            title: None,
            suggested: None,
            sealed: false,
            pinned: false,
            archived: false,
            new_activity: false,
            auto_archived: false,
            done: None,
            last_seq: entry.seq,
            catch_up: false,
            last_reopening: 0,
        });
        summary.catch_up |= entry.summary.is_some();
        summary.sealed |= entry.sealed.is_some();
        summary.messages += 1;
        summary.last_seq = summary.last_seq.max(entry.seq);
        if entry.kind.reopens() {
            summary.last_reopening = summary.last_reopening.max(entry.seq);
        }
        summary.first_at = summary.first_at.min(entry.stored_at);
        summary.last_at = summary.last_at.max(entry.stored_at);
        summary.users.extend(entry.users().cloned());
        summary.channels.extend(entry.channel.iter().cloned());
        if summary.owner.is_none() {
            summary.owner.clone_from(&entry.owner);
        }
        if summary.suggested.is_none()
            && let (Some(from), Some(title)) = (&entry.from, &entry.suggested_title)
        {
            summary.suggested = Some((from.clone(), title.clone()));
        }
        if let (Some(grep), None, true) = (&self.grep, &summary.matched, entry.matchable()) {
            summary.matched = entry.text.lines().find(|line| line.to_lowercase().contains(grep.as_str())).map(|line| {
                let from = entry.from.as_ref().map_or("(a human)", UserId::as_str);
                format!("{from}: {}", crate::text::excerpt(line.trim(), OPENING))
            });
        }
        if let Some(humans) = self.humans.as_ref().filter(|_| !entry.forged) {
            let human = |user: &Option<UserId>| user.as_ref().is_some_and(|u| humans.contains(u));
            match entry.kind {
                Kind::Escalation if summary.escalated.is_none() && human(&entry.to) => {
                    if entry.reason.is_some_and(|r| r != EscalationReason::Approval) {
                        let note = entry.note.as_deref().unwrap_or_default();
                        summary.escalated = Some(format!("{} {}: {note}", time(entry.stored_at), entry.route()));
                    }
                }
                Kind::Message | Kind::Post if human(&entry.from) => summary.escalated = None,
                _ => {}
            }
        }
    }

    /// Every thread read so far.
    pub fn summaries(&self) -> impl Iterator<Item = &Summary> {
        self.by_id.values()
    }

    pub fn get(&self, thread: &ThreadId) -> Option<&Summary> {
        self.by_id.get(thread)
    }

    /// The threads `select` keeps, as the CLI's human marked them (`marks`), least recently active first and its
    /// pinned ones after the others, and how many more it left out for its limit. Threads opened by summary requests
    /// ([`Summary::catch_up`]) are left out, and so are archived threads, unless a message was stored on one since it
    /// was archived, or `select` asks for archived threads, and threads idle since `select`'s `idle_before`, which
    /// count as archived.
    pub fn finish(self, select: &Select, marks: &ThreadMarks) -> (Vec<Summary>, usize) {
        let mut threads: Vec<Summary> = self
            .by_id
            .into_values()
            .map(|mut t| {
                match marks.mark_at(&t.thread_id, t.last_seq, t.last_at, select.idle_before) {
                    Some(Mark::Pinned) => t.pinned = true,
                    Some(Mark::Archived { new_activity }) => (t.archived, t.new_activity) = (true, new_activity),
                    Some(Mark::Idle) => (t.archived, t.auto_archived) = (true, true),
                    None => {}
                }
                t
            })
            .filter(|t| !t.catch_up)
            .filter(|t| select.ant.as_ref().is_none_or(|ant| t.users.contains(ant)))
            .filter(|t| select.owner.is_none() || t.owner == select.owner)
            .filter(|t| select.grep.is_none() || t.matched.is_some())
            .filter(|t| !select.escalated || t.escalated.is_some())
            .filter(|t| !select.pinned || t.pinned)
            .filter(|t| if select.archived { t.archived } else { !t.archived || t.new_activity })
            .collect();
        threads.sort_by(|a, b| (a.pinned, a.last_at, &a.thread_id).cmp(&(b.pinned, b.last_at, &b.thread_id)));
        let omitted = select.limit.map_or(0, |limit| threads.len().saturating_sub(limit));
        threads.drain(..omitted);
        (threads, omitted)
    }
}
