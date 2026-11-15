//! `ants thread`: the threads in the ANTS stream, across inboxes (both ways), channel posts, notes, escalations, tool
//! requests and their decisions, dead letters, and the tool calls ants stored on their timelines.
//!
//! Read back by a human by message gets (batched direct gets, [`Reading::Gets`]), so nothing is consumed or acked
//! and no consumer is left behind. Only notes subjects name their thread, so a read goes through every message stored
//! in its time window: `--since` bounds it, and `ants thread show` of a thread with a time-ordered id starts at the
//! thread's start.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ants_daemon::channel::CLOCK_SLACK;
use ants_daemon::reactions::Reacted;
use ants_daemon::reading::{self, Reading};
use ants_proto::{
    Acting, Answer, Answered, ApprovalDecision, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, Envelope,
    EscalationReason, Humans, Mark, NotesPost, Question, SummaryRequest, ThreadId, ThreadMarks, Timeline, Timelines,
    UserId, Uuid, subject,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::LastRawMessageErrorKind;
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{attachments, sealed, thread_meta, tools};

pub use message::{MessagePick, recent_messages};

/// How long the opening message's line in `ants thread list` may be.
const OPENING: usize = 72;
/// How far back a start of a thread id looks for the threads of the CLI's user.
pub const RECENT: Duration = Duration::from_hours(7 * 24);
/// How long after its time-ordered id is made a thread's first message may be stored, clocks' skew included, for a start
/// of an id matched among threads of any user.
const LATE: Duration = Duration::from_mins(10);
/// How many threads an error on a start of a thread id lists.
const LISTED: usize = 10;
/// How far back `ants thread list` reads by default.
const SINCE: Duration = Duration::from_hours(24);

/// What a stored message on a thread is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, clap::ValueEnum)]
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
    /// A decision on a tool request: a human's, or an ant's of the requester's chain.
    Decision,
    /// A message an ant gave up on.
    Dead,
    /// A tool call an ant made, from its timeline records: not a message, so not searched.
    #[value(skip)]
    Tool,
}

impl Kind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Escalation => "escalation",
            Self::Mention => "mention",
            Self::Post => "post",
            Self::Note => "note",
            Self::Approval => "approval",
            Self::Decision => "decision",
            Self::Dead => "dead",
            Self::Tool => "tool",
        }
    }

    /// Whether an entry of the kind is a message, channel post or note, which reopens a thread marked done
    /// ([`ants_proto::ThreadDone::holds`]): no tool request, decision on one or dead letter does.
    pub fn reopens(self) -> bool {
        matches!(self, Self::Message | Self::Escalation | Self::Mention | Self::Post | Self::Note)
    }
}

/// The sequence of the last of `entries` that reopens a thread marked done ([`Kind::reopens`]); 0 for none.
pub fn last_reopening(entries: &[Entry]) -> u64 {
    entries.iter().filter(|e| e.kind.reopens()).map(|e| e.seq).max().unwrap_or(0)
}

/// One stored message on a thread, as `ants thread show` prints it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub seq: u64,
    /// The message's id, which `ants react` takes: none for a tool request or a dead letter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
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
    /// The users a channel post @mentions.
    #[serde(skip)]
    pub mentions: Vec<UserId>,
    /// The thread's owner as the message names it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// What the kind adds: an escalation's level and chain, a tool request's id, a dead letter's error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub text: String,
    /// The files attached to an inbox message, channel post, notes post or dead letter, each with its handle.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<attachments::Listed>,
    /// The subject it was stored on names another sender (or ant) than the message: the message is forged.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub forged: bool,
    /// For whom the sender, an assistant, sent an inbox message, as the CLI checked its `on_behalf_of`
    /// ([`crate::behalf::Checker`]): a claim not honoured is forged, and the message the sender's own.
    #[serde(skip_serializing_if = "Acting::is_own")]
    pub acting: Acting,
    /// What the CLI made of a sealed message between two humans: opened, its text the message's; else its text
    /// `[sealed]` or `[forged]` ([`sealed::State::placeholder`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sealed: Option<sealed::State>,
    /// The reactions to the message, each with who reacted so.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reactions: Vec<Reacted>,
    /// The reason of the escalation an inbox message carries, which the note spells out.
    #[serde(skip)]
    pub reason: Option<EscalationReason>,
    /// The stuck ant of the escalation an inbox message carries.
    #[serde(skip)]
    pub stuck: Option<UserId>,
    /// The title an inbox message, channel post or notes post that is not forged suggests for its thread
    /// ([`Envelope::title`], [`ChannelPost::title`], [`NotesPost::title`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_title: Option<String>,
    /// The summary request an inbox message that is not forged carries, or answers ([`Envelope::summary`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryRequest>,
    /// The question an inbox message that is not forged asks, if valid ([`Envelope::asks`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub question: Option<Question>,
    /// The answer an inbox message that is not forged gives a question read before it, if valid
    /// ([`Envelope::answers`]); an invalid one, or one to a question not read, leaves an ordinary message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<Answer>,
    /// A tool call's details, its title the text and its ant the sender.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolCall>,
    /// The ant-to-ant turns an inbox message, channel post or notes post leaves on its thread.
    #[serde(skip)]
    pub turns: Option<u32>,
    /// An inbox message a webhook or schedule sent ([`Envelope::webhook`]).
    #[serde(skip)]
    pub webhook: bool,
    /// The thread an inbox message's thread was spun off ([`Envelope::origin_thread`]).
    #[serde(skip)]
    pub origin: Option<ThreadId>,
}

/// A tool call an ant stored on its timeline, as a [`Kind::Tool`] entry has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolCall {
    /// The call's id, unique within its prompt.
    pub call_id: String,
    /// The message whose prompt made the call.
    pub message: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
}

impl Entry {
    pub(crate) fn new(seq: u64, stored_at: DateTime<Utc>, kind: Kind, thread_id: ThreadId) -> Self {
        Self {
            seq,
            id: None,
            stored_at,
            kind,
            thread_id,
            from: None,
            to: None,
            channel: None,
            mentions: Vec::new(),
            owner: None,
            note: None,
            text: String::new(),
            attachments: Vec::new(),
            forged: false,
            acting: Acting::Own,
            sealed: None,
            reactions: Vec::new(),
            reason: None,
            stuck: None,
            suggested_title: None,
            summary: None,
            question: None,
            answer: None,
            tool: None,
            turns: None,
            webhook: false,
            origin: None,
        }
    }

    /// Whether `--grep` looks at its text: not that of a sealed message the CLI did not open.
    pub fn matchable(&self) -> bool {
        self.sealed.is_none_or(|state| state == sealed::State::Opened)
    }

    /// The entry of a tool call merged from timeline records, the last of them stored at `seq`.
    fn tool(seq: u64, merged: &ants_proto::MergedCall) -> Self {
        let call = &merged.call;
        let tool = ToolCall {
            call_id: call.id.clone(),
            message: merged.message,
            tool_kind: call.tool_kind.clone(),
            status: call.status.clone(),
            duration_ms: call.duration_ms,
            paths: call.paths.clone(),
        };
        let note = [tool.tool_kind.clone(), tool.status.clone(), tool.duration_ms.map(took)];
        let note = note.into_iter().flatten().collect::<Vec<_>>().join(", ");
        Self {
            from: Some(merged.ant.clone()),
            note: (!note.is_empty()).then_some(note),
            text: call.title.clone(),
            tool: Some(tool),
            ..Self::new(seq, call.started, Kind::Tool, merged.thread_id.clone())
        }
    }

    /// What this entry answers `question`, an entry asking one, with: set only on a valid answer
    /// ([`Entry::answer`]).
    pub fn answers<'q>(&self, question: &'q Entry) -> Option<Answered<'q>> {
        let answer = self.answer.as_ref()?;
        (question.id == Some(answer.question)).then_some(())?;
        question.question.as_ref()?.answered(answer)
    }

    /// Who sent it to whom: `alice -> dev-a`, `dev-a -> #ops`, `dev-a -> notes`, `(a human) -> dev-a`; for an
    /// assistant's message for its human, `alice via pa -> dev-a`, or `pa (forged: for alice) -> dev-a`.
    pub(crate) fn route(&self) -> String {
        let from = self.sender();
        match (self.kind, &self.to, &self.channel) {
            (Kind::Post, _, Some(channel)) => format!("{from} -> #{channel}"),
            (Kind::Note, ..) => format!("{from} -> notes"),
            (Kind::Approval, ..) => format!("{from} -> humans"),
            (_, Some(to), _) => format!("{from} -> {to}"),
            (_, None, _) => from.to_owned(),
        }
    }

    /// Who sent it, as [`Acting::label`] names an assistant acting for its human.
    pub(crate) fn sender(&self) -> String {
        self.acting.label(self.from.as_ref().map_or("(a human)", UserId::as_str))
    }

    /// The users it was between.
    fn users(&self) -> impl Iterator<Item = &UserId> {
        self.from.iter().chain(&self.to)
    }
}

/// Turns stored messages into [`Entry`]s, in stream order. A human's decision does not name its request's thread, so
/// the threads of the requests read so far are kept to place it, and an answer names its question by id only, so the
/// questions read so far are kept to check it. A sealed message between two humans is opened with the keyring given
/// ([`Parser::opening`]); without one, it shows as `[sealed]`.
#[derive(Default)]
pub struct Parser {
    requests: HashMap<(UserId, Uuid), ThreadId>,
    questions: HashMap<Uuid, Envelope>,
    keyring: Option<Arc<sealed::Keyring>>,
    behalf: Option<Arc<crate::behalf::Checker>>,
}

impl Parser {
    /// A parser opening sealed messages with `keyring`.
    pub fn opening(keyring: Option<Arc<sealed::Keyring>>) -> Self {
        Self { keyring, ..Self::default() }
    }

    /// The parser, checking the `on_behalf_of` of inbox messages with `behalf`; without it, every such claim is forged.
    pub fn checking(self, behalf: Option<Arc<crate::behalf::Checker>>) -> Self {
        Self { behalf, ..self }
    }

    /// Look the sending device of a sealed message stored on `subject` up unless known ([`sealed::Keyring::learn`]),
    /// and check the claim of an inbox message sent on behalf of a human ([`crate::behalf::Checker::learn`]), before
    /// [`Parser::entry`] takes it, so one from a device added since the keyring was loaded opens.
    pub async fn learn(&self, subject: &str, payload: &[u8]) {
        if let Some(behalf) = &self.behalf
            && subject::parse_inbox(subject).is_some()
            && payload.windows(14).any(|w| w == b"\"on_behalf_of\"")
            && let Ok(envelope) = serde_json::from_slice::<Envelope>(payload)
        {
            Box::pin(behalf.learn(&envelope)).await;
        }
        let Some(keyring) = &self.keyring else { return };
        if subject::parse_inbox(subject).is_none() || !payload.windows(8).any(|w| w == b"\"sealed\"") {
            return;
        }
        if let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) {
            // Boxed: the directory lookup would otherwise swell every read of the stream.
            Box::pin(keyring.learn(&envelope)).await;
        }
    }

    /// [`Parser::entry`], the sending device of a sealed message looked up first ([`Parser::learn`]).
    pub async fn read(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        self.learn(subject, payload).await;
        self.entry(seq, stored_at, subject, payload)
    }

    /// The valid question `e` asks, kept to check later answers against, and its valid answer to a question read
    /// earlier; none of either for a forged message.
    fn question_and_answer(&mut self, e: &Envelope, forged: bool) -> (Option<Question>, Option<Answer>) {
        if forged {
            return (None, None);
        }
        let question = e.asks().cloned();
        if question.is_some() {
            self.questions.insert(e.id, Envelope { text: String::new(), ..e.clone() });
        }
        let asked = e.answer.as_ref().and_then(|a| self.questions.get(&a.question));
        (question, asked.filter(|q| e.answers(q).is_some()).and(e.answer.clone()))
    }

    /// The entry of the envelope stored at `seq` on inbox `subject`, None for one that does not parse.
    fn envelope_entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        let e: Envelope = serde_json::from_slice(payload).ok()?;
        let forged = e.check_subject(subject).is_err();
        let (e, sealed) = sealed::read(e, self.keyring.as_deref());
        let (question, answer) = self.question_and_answer(&e, forged || sealed == Some(sealed::State::Forged));
        let acting = match (&self.behalf, &e.on_behalf_of) {
            (_, None) => Acting::Own,
            (Some(behalf), claim) => behalf.acting(&e.from, claim.as_ref()),
            (None, Some(human)) => Acting::Forged(human.clone()),
        };
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
        Some(Entry {
            id: Some(e.id),
            from: Some(e.from),
            to: Some(e.to),
            channel,
            owner: e.owner,
            note,
            text,
            // Under the handles of the subject it was stored on, a forged one's too: its sender stored them.
            attachments: attachments::stored_on(subject, &e.attachments, stored_at),
            forged,
            acting,
            stuck: e.escalation.as_ref().map(|x| x.stuck.clone()),
            sealed,
            reason: e.escalation.map(|x| x.reason),
            // Only an ant suggests a title, and no ant seals.
            suggested_title: e.title.filter(|_| !forged && sealed.is_none()),
            summary: e.summary.filter(|_| !forged),
            question,
            answer,
            turns: Some(e.turn_budget),
            webhook: e.webhook,
            origin: e.origin_thread,
            ..entry(kind, e.thread_id)
        })
    }

    /// The entry of the message stored at `seq` on `subject`, None for one that is not ants' or does not parse.
    pub fn entry(&mut self, seq: u64, stored_at: DateTime<Utc>, subject: &str, payload: &[u8]) -> Option<Entry> {
        let entry = |kind, thread| Entry::new(seq, stored_at, kind, thread);
        if subject::parse_inbox(subject).is_some() {
            return self.envelope_entry(seq, stored_at, subject, payload);
        }
        if subject::parse_chan(subject).is_some() {
            let post: ChannelPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            return Some(Entry {
                attachments: attachments::stored_on(subject, &post.attachments, stored_at),
                id: Some(post.id),
                from: Some(post.from),
                channel: Some(post.channel),
                mentions: post.mentions,
                owner: post.owner,
                note,
                text: post.text,
                forged,
                suggested_title: post.title.filter(|_| !forged),
                turns: Some(post.turn_budget),
                ..entry(Kind::Post, post.thread_id)
            });
        }
        if subject::parse_notes(subject).is_some() {
            let post: NotesPost = serde_json::from_slice(payload).ok()?;
            let forged = post.check_subject(subject).is_err();
            let note = Some(format!("{} turns left", post.turn_budget));
            return Some(Entry {
                attachments: attachments::stored_on(subject, &post.attachments, stored_at),
                id: Some(post.id),
                from: Some(post.from),
                note,
                text: post.text,
                forged,
                suggested_title: post.title.filter(|_| !forged),
                turns: Some(post.turn_budget),
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
            let text = match &e.sealed {
                Some(sealed) => sealed::size_note(sealed),
                None => e.text.clone(),
            };
            return Some(Entry {
                // Stored under the handles of the message as it was sent.
                attachments: attachments::listed(&e.attachments, |id| e.attachment_handle(id)),
                from: Some(e.from),
                to: Some(d.ant),
                owner: e.owner,
                note,
                text,
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
            opening: crate::excerpt(&entry.text, OPENING),
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
                format!("{from}: {}", crate::excerpt(line.trim(), OPENING))
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

pub(crate) fn time(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// The threads as a table: one row per thread under a header, columns padded to line up, and under a thread
/// matching `--grep` its matching line, under one with an open escalation that escalation. A STATE column (pinned,
/// archived, archived+new for one with a message stored since, or auto-archived for one idle, then done for one
/// marked done, comma-separated) and a TITLE column show only when a thread listed has one.
pub fn table(threads: &[Summary]) -> String {
    let marked = threads.iter().any(|t| t.pinned || t.archived || t.done.is_some());
    let titled = threads.iter().any(|t| t.title.is_some());
    let mut header = vec!["THREAD", "OWNER", "USERS", "FIRST", "LAST", "MSGS"];
    header.extend(marked.then_some("STATE"));
    header.extend(titled.then_some("TITLE"));
    header.push("OPENING");
    let rows: Vec<Vec<String>> = std::iter::once(header.into_iter().map(str::to_owned).collect())
        .chain(threads.iter().map(|t| {
            let users = t.users.iter().map(UserId::as_str);
            let channels = t.channels.iter().map(|c| format!("#{c}"));
            let mut row = vec![
                t.thread_id.to_string(),
                t.owner.as_ref().map_or("-", UserId::as_str).to_owned(),
                users.map(str::to_owned).chain(channels).collect::<Vec<_>>().join(","),
                time(t.first_at),
                time(t.last_at),
                t.messages.to_string(),
            ];
            if marked {
                let states: Vec<&str> = state(t).into_iter().chain(t.done.as_ref().map(|_| "done")).collect();
                row.push(if states.is_empty() { "-".to_owned() } else { states.join(",") });
            }
            if titled {
                row.push(t.title.clone().unwrap_or_else(|| "-".to_owned()));
            }
            row.push(t.opening.clone());
            row
        }))
        .collect();
    let mut widths = vec![0; rows[0].len()];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut lines = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let cells: Vec<String> = row.iter().zip(&widths).map(|(cell, &width)| format!("{cell:width$}")).collect();
        lines.push(cells.join("  ").trim_end().to_owned());
        if let Some(thread) = i.checked_sub(1).map(|i| &threads[i]) {
            for line in thread.matched.iter().chain(&thread.escalated) {
                lines.push(format!("    {line}"));
            }
        }
    }
    lines.join("\n")
}

/// How the CLI's human marked a thread listed, as the STATE column says it.
fn state(thread: &Summary) -> Option<&'static str> {
    match (thread.pinned, thread.archived, thread.new_activity) {
        (true, ..) => Some("pinned"),
        (_, true, _) if thread.auto_archived => Some("auto-archived"),
        (_, true, true) => Some("archived+new"),
        (_, true, false) => Some("archived"),
        _ => None,
    }
}

/// The entries of a thread as `ants thread show` prints them: time, kind, route, note, id and text, one entry per
/// line, its text's further lines, its attachments and the reactions to it indented under it. A question shows its options numbered,
/// whether a typed answer is taken, and the answers to it among `entries` or how to answer it; an answer, the question
/// and the option it picks.
pub fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    for e in entries {
        let text = body(e, entries);
        let mut lines = text.lines();
        let first = lines.next().unwrap_or("");
        let _ = writeln!(out, "{}: {first}", heading(e));
        for line in lines {
            let _ = writeln!(out, "    {line}");
        }
        out.push_str(&attachments::render(&e.attachments, "    "));
        out.push_str(&crate::inbox::render_reactions(&e.reactions, "    "));
    }
    out
}

/// How long a tool call took: `840ms`, `2.1s`, `3m`.
pub(crate) fn took(ms: u64) -> String {
    match ms {
        0..1000 => format!("{ms}ms"),
        1000..60_000 => format!("{:.1}s", Duration::from_millis(ms).as_secs_f64()),
        _ => ants_proto::duration::format(Duration::from_millis(ms)),
    }
}

/// What [`render`] shows of an entry before its text: time, kind, route, note, answer and id.
pub fn heading(e: &Entry) -> String {
    let forged = if e.forged { " (forged)" } else { "" };
    let note = e.note.as_ref().map_or_default(|n| format!(" [{n}]"));
    let answering = e.answer.as_ref().map_or_default(|a| match a.option {
        Some(n) => format!(" [answers {} with option {n}]", a.question),
        None => format!(" [answers {} as typed]", a.question),
    });
    let kind = e.kind.as_str();
    let id = e.id.map_or_default(|id| format!(" (message {id})"));
    format!("{}  {kind:<10}  {}{forged}{note}{answering}{id}", time(e.stored_at), e.route())
}

/// The text [`render`] shows of an entry among `entries`: a question with its options and its answers among them, else
/// the entry's text.
pub fn body(e: &Entry, entries: &[Entry]) -> String {
    match (&e.question, e.id) {
        (Some(question), Some(id)) => {
            let answers = answers_to(e, entries).map(|(by, answered)| answer_line(by, answered)).collect();
            crate::inbox::render_question(id, question, answers)
        }
        _ => e.text.clone(),
    }
}

/// The answers among `entries` to `question`, an entry asking one, oldest first, each with who answered: an option
/// picked or a typed answer, the answer's text.
pub fn answers_to<'a>(question: &'a Entry, entries: &'a [Entry]) -> impl Iterator<Item = (&'a Entry, Answered<'a>)> {
    entries.iter().filter_map(move |e| Some((e, e.answers(question)?)))
}

/// How a question's answer is shown beneath it: who answered with which option, or typed what.
pub(crate) fn answer_line(by: &Entry, answered: Answered<'_>) -> String {
    let from = by.from.as_ref().map_or("(a human)", UserId::as_str);
    match answered {
        Answered::Option(n, option) => format!("answered by {from}: option {n}, {option}"),
        Answered::Typed => format!("answered by {from}, typed: {}", crate::excerpt(&by.text, 72)),
    }
}

/// Where a read of the last `since` starts: everything kept for zero.
pub fn window(since: Duration) -> consumer::DeliverPolicy {
    match SystemTime::now().checked_sub(since) {
        Some(start) if !since.is_zero() => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        _ => consumer::DeliverPolicy::All,
    }
}

/// The time threads last active then or before count as archived, `auto_archive` before `now`; None when
/// `auto_archive` is.
pub fn idle_before(auto_archive: Option<Duration>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    now.checked_sub_signed(chrono::TimeDelta::from_std(auto_archive?).ok()?)
}

/// Where `ants thread show` reads `thread` from: as `since` says, else the thread's start when its id is
/// time-ordered, else everything kept.
pub(crate) fn show_start(thread: &ThreadId, since: Option<Duration>) -> consumer::DeliverPolicy {
    match (since, ants_daemon::channel::thread_start(thread)) {
        (Some(since), _) => window(since),
        (None, Some(start)) => consumer::DeliverPolicy::ByStartTime { start_time: start.into() },
        (None, None) => consumer::DeliverPolicy::All,
    }
}

/// Hand `take` the entry of every message stored from `start` on, oldest first.
pub async fn scan(js: &jetstream::Context, start: consumer::DeliverPolicy, take: impl FnMut(Entry)) -> Result<()> {
    scan_opening(js, start, None, take).await
}

/// [`scan`], opening sealed messages with `keyring`, which looks up a sending device added since it was loaded.
pub async fn scan_opening(
    js: &jetstream::Context,
    start: consumer::DeliverPolicy,
    keyring: Option<Arc<sealed::Keyring>>,
    mut take: impl FnMut(Entry),
) -> Result<()> {
    let mut parser = Parser::opening(keyring);
    reading::read_back_awaiting(js, "", Reading::Gets, subject::ALL, start, async |seq, stored, msg| {
        match parser.read(seq, stored.into(), &msg.subject, &msg.payload).await {
            Some(entry) => take(entry),
            None => tracing::debug!(seq, subject = %msg.subject, "skipping a message on no thread"),
        }
        ControlFlow::Continue(())
    })
    .await
}

/// What a THREAD argument or `--thread` picks: a whole thread id (a UUID), taken as given; a start of one (or a whole
/// id of another form), among the threads the CLI's user took part in lately ([`RECENT`]), else among any user's; or
/// `last`, the thread of the latest message to or from that user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    Id(ThreadId),
    Prefix(String),
    Last,
}

impl std::str::FromStr for Pick {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Self> {
        let text = text.trim().to_ascii_lowercase();
        if text == "last" {
            return Ok(Self::Last);
        }
        if Uuid::try_parse(&text).is_ok_and(|id| id.hyphenated().to_string() == text) {
            return Ok(Self::Id(text.parse()?));
        }
        match text.parse::<ThreadId>() {
            Ok(_) => Ok(Self::Prefix(text)),
            Err(_) => bail!("{text} is no thread id, start of one or `last`"),
        }
    }
}

impl Pick {
    /// The thread this picks for `me`, reading the stream with `js` unless given a whole thread id. Only a human's
    /// credentials get stream messages by sequence, so only a human picks by a start of an id or `last`.
    pub async fn resolve(self, js: &jetstream::Context, me: &crate::humans::Me) -> Result<ThreadId> {
        let prefix = match self {
            Self::Id(thread) => return Ok(thread),
            Self::Prefix(prefix) => Some(prefix),
            Self::Last => None,
        };
        if !me.role.is_human() {
            bail!("the credentials of {} are an ant's, which read no thread back: give the whole thread id", me.id);
        }
        let Some(prefix) = prefix else { return last(js, &me.id).await };
        let own = recent(js, &me.id, window(RECENT)).await?;
        if let Some(thread) = select(&own, &prefix, &me.id)? {
            return Ok(thread);
        }
        select_any(&any_recent(js, &prefix).await?, &own, &prefix, &me.id)
    }
}

/// [`Pick::resolve`] for an optional pick.
pub async fn resolve(pick: Option<Pick>, js: &jetstream::Context, me: &crate::humans::Me) -> Result<Option<ThreadId>> {
    match pick {
        Some(pick) => Ok(Some(pick.resolve(js, me).await?)),
        None => Ok(None),
    }
}

/// The subjects of the messages `user` took part in: its inbox both ways, its channel posts and its notes posts.
fn own_subjects(user: &UserId) -> [String; 4] {
    [subject::inbox_of(user), subject::inbox_from(user), subject::chan_from(user), subject::notes_from(user)]
}

/// The thread of the latest message to or from `user` ([`own_subjects`]), however old, other than a summary
/// request's or reply's: when the last of a subject is one, the latest of the last [`RECENT`] if any.
async fn last(js: &jetstream::Context, user: &UserId) -> Result<ThreadId> {
    let stream = js.get_stream(ants_proto::STREAM).await.context("opening the ANTS stream")?;
    let mut parser = Parser::default();
    let mut latest: Option<Entry> = None;
    let mut catch_up = false;
    for filter in own_subjects(user) {
        let msg = match stream.get_last_raw_message_by_subject(&filter).await {
            Ok(msg) => msg,
            Err(e) if e.kind() == LastRawMessageErrorKind::NoMessageFound => continue,
            Err(e) => return Err(e).with_context(|| format!("reading the last message on {filter}")),
        };
        let stored = DateTime::<Utc>::from(SystemTime::from(msg.time));
        let entry = parser.entry(msg.sequence, stored, &msg.subject, &msg.payload).filter(|e| !e.forged);
        catch_up |= entry.as_ref().is_some_and(|e| e.summary.is_some());
        let entry = entry.filter(|e| e.summary.is_none());
        if let Some(entry) = entry.filter(|e| latest.as_ref().is_none_or(|l| l.seq < e.seq)) {
            latest = Some(entry);
        }
    }
    if catch_up && let Some(recent) = recent(js, user, window(RECENT)).await?.into_iter().next() {
        return Ok(recent.thread_id);
    }
    latest.map(|e| e.thread_id).ok_or_else(|| anyhow!("no message to or from {user} in the stream, so no last thread"))
}

/// The latest message of each thread `user` took part in ([`own_subjects`]) in the last [`RECENT`], most recent first.
pub async fn recent_threads(js: &jetstream::Context, user: &UserId) -> Result<Vec<Entry>> {
    recent(js, user, window(RECENT)).await
}

/// The latest message of each thread `user` took part in ([`own_subjects`]) stored from `start` on, most recent first,
/// summary requests and replies left out.
async fn recent(js: &jetstream::Context, user: &UserId, start: consumer::DeliverPolicy) -> Result<Vec<Entry>> {
    let mut parser = Parser::default();
    let mut latest: HashMap<ThreadId, Entry> = HashMap::new();
    for filter in own_subjects(user) {
        reading::read_back(js, "", Reading::Gets, &filter, start, |seq, stored, subject, payload| {
            let entry = parser.entry(seq, stored.into(), subject, payload).filter(|e| !e.forged && e.summary.is_none());
            if let Some(entry) = entry {
                match latest.get(&entry.thread_id) {
                    Some(kept) if kept.seq > entry.seq => {}
                    _ => _ = latest.insert(entry.thread_id.clone(), entry),
                }
            }
            ControlFlow::Continue(())
        })
        .await?;
    }
    let mut threads: Vec<Entry> = latest.into_values().collect();
    threads.sort_by_key(|e| std::cmp::Reverse(e.seq));
    Ok(threads)
}

/// The latest message of each thread with an id starting with `prefix` stored in the last [`RECENT`], whoever took
/// part in it, most recent first.
async fn any_recent(js: &jetstream::Context, prefix: &str) -> Result<Vec<Entry>> {
    let mut threads: HashMap<ThreadId, Entry> = HashMap::new();
    scan_made(js, prefix, |entry| {
        if entry.thread_id.as_str().starts_with(prefix) {
            threads.insert(entry.thread_id.clone(), entry);
        }
    })
    .await?;
    let mut threads: Vec<Entry> = threads.into_values().collect();
    threads.sort_by_key(|e| std::cmp::Reverse(e.seq));
    Ok(threads)
}

/// Hand `take` the entry of every message, not forged, stored in the last [`RECENT`] when an id starting with `prefix`
/// may have been made, oldest first. For a start of a time-ordered id, only the messages stored while such an id was
/// made, give or take the clocks' skew ([`CLOCK_SLACK`]) and up to [`LATE`] after, are read: a thread's first message,
/// or the message with such an id, is.
async fn scan_made(js: &jetstream::Context, prefix: &str, mut take: impl FnMut(Entry)) -> Result<()> {
    let now = SystemTime::now();
    let floor = now.checked_sub(RECENT).unwrap_or(SystemTime::UNIX_EPOCH);
    let (from, to) = match made_within(prefix) {
        Some((first, last)) => (first.checked_sub(CLOCK_SLACK).unwrap_or(first).max(floor), (last + LATE).min(now)),
        None => (floor, now),
    };
    if from > to {
        return Ok(());
    }
    let mut parser = Parser::default();
    let start = consumer::DeliverPolicy::ByStartTime { start_time: from.into() };
    reading::read_back(js, "", Reading::Gets, subject::ALL, start, |seq, stored, subject, payload| {
        if stored > to {
            return ControlFlow::Break(());
        }
        if let Some(entry) = parser.entry(seq, stored.into(), subject, payload).filter(|e| !e.forged) {
            take(entry);
        }
        ControlFlow::Continue(())
    })
    .await
}

/// The span in which a time-ordered (version 7 UUID) thread id starting with `prefix` was made, from the time its
/// first 12 hex digits carry; None when no such id starts so, or the start leaves the time open.
fn made_within(prefix: &str) -> Option<(SystemTime, SystemTime)> {
    let bytes = prefix.as_bytes();
    if bytes.get(14).is_some_and(|&b| b != b'7') {
        return None;
    }
    let mut digits = String::new();
    for (i, &b) in bytes.iter().take(13).enumerate() {
        match (i, b) {
            (8, b'-') => {}
            (8, _) => return None,
            (_, b) if b.is_ascii_hexdigit() => digits.push(b as char),
            _ => return None,
        }
    }
    if digits.is_empty() {
        return None;
    }
    let millis = |pad: char| {
        let padded: String = digits.chars().chain(std::iter::repeat(pad)).take(12).collect();
        u64::from_str_radix(&padded, 16).ok()
    };
    let at = |ms: u64| SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
    Some((at(millis('0')?), at(millis('f')? + 1)))
}

/// Threads among `threads` whose id is `prefix`, else those starting with it.
fn matching<'a>(threads: &'a [Entry], prefix: &str) -> Vec<&'a Entry> {
    match threads.iter().find(|e| e.thread_id.as_str() == prefix) {
        Some(exact) => vec![exact],
        None => threads.iter().filter(|e| e.thread_id.as_str().starts_with(prefix)).collect(),
    }
}

/// The one thread among `threads` `user` took part in (the latest message of each, most recent first) whose id is
/// `prefix`, else starts with it; None when none does. The error on several lists them.
fn select(threads: &[Entry], prefix: &str, user: &UserId) -> Result<Option<ThreadId>> {
    let found = matching(threads, prefix);
    match found.as_slice() {
        [] => Ok(None),
        [one] => Ok(Some(one.thread_id.clone())),
        _ => bail!(
            "{} threads {user} took part in over the last {} days start with {prefix}; give more of the id:\n{}",
            found.len(),
            RECENT.as_secs() / 86_400,
            listing(found.iter().copied())
        ),
    }
}

/// The one thread among `threads` (any user's, the latest message of each, most recent first) whose id is `prefix`,
/// else starts with it, for a `user` who took part in none of them. The error on none lists `own`, the threads `user`
/// took part in, and the one on several lists those found.
fn select_any(threads: &[Entry], own: &[Entry], prefix: &str, user: &UserId) -> Result<ThreadId> {
    let found = matching(threads, prefix);
    let days = RECENT.as_secs() / 86_400;
    match found.as_slice() {
        [one] => Ok(one.thread_id.clone()),
        [] if own.is_empty() => {
            bail!("no thread over the last {days} days starts with {prefix}: give the whole thread id")
        }
        [] => bail!(
            "no thread over the last {days} days starts with {prefix}; give the whole id of an older one, or pick one \
             of the most recent {user} took part in:\n{}",
            listing(own.iter())
        ),
        _ => bail!(
            "{} threads over the last {days} days start with {prefix}, none of them with {user}; give more of the id:\n{}",
            found.len(),
            listing(found.iter().copied())
        ),
    }
}

/// Threads as an error lists them, one per line: id, time and route of the latest message, and its first line.
fn listing<'a>(threads: impl ExactSizeIterator<Item = &'a Entry>) -> String {
    let more = threads.len().saturating_sub(LISTED);
    let mut lines: Vec<String> = threads
        .take(LISTED)
        .map(|e| format!("  {}  {}  {}: {}", e.thread_id, time(e.stored_at), e.route(), crate::excerpt(&e.text, 48)))
        .collect();
    if more > 0 {
        lines.push(format!("  and {more} more"));
    }
    lines.join("\n")
}

/// Connect as the CLI's human: only a human's credentials get stream messages by sequence.
pub async fn connect(nats: &crate::Nats<'_>) -> Result<jetstream::Context> {
    if !nats.me.role.is_human() {
        bail!(
            "the credentials of {} are an ant's, which read no other inbox; `ants thread` reads them all with a \
             human's",
            nats.me.id
        );
    }
    Ok(nats.connect("ants-thread").await?.1)
}

/// `ants thread list`: the threads with messages stored within `since` (by default [`SINCE`], or for `--pinned` and
/// `--archived` since the earliest thread marked so started, or all the stream keeps for `--archived` when idle
/// threads count as archived), as `select` keeps them, with their titles.
pub async fn list(nats: &crate::Nats<'_>, since: Option<Duration>, select: &Select, json: bool) -> Result<()> {
    let (threads, omitted) = listed(nats, since, select).await?;
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

/// The threads [`list`] lists, and how many less recently active ones `select`'s limit left out.
pub async fn listed(nats: &crate::Nats<'_>, since: Option<Duration>, select: &Select) -> Result<(Vec<Summary>, usize)> {
    let auto_archive = crate::defaults::auto_archive()?;
    let select = &Select { idle_before: idle_before(auto_archive, Utc::now()), ..select.clone() };
    let js = connect(nats).await?;
    let meta = match thread_meta::open(nats, &js).await {
        Ok(meta) => Some(meta),
        Err(e) if !select.pinned && !select.archived => {
            tracing::warn!("not showing titles, pinned or archived threads: {e:#}");
            None
        }
        Err(e) => return Err(e),
    };
    let marks = match &meta {
        Some(meta) => meta.marks().await?,
        None => ThreadMarks::default(),
    };
    let start = match since {
        Some(since) => Some(window(since)),
        None if select.pinned => marked_start(marks.pinned.keys()),
        None if select.archived && auto_archive.is_some() => Some(consumer::DeliverPolicy::All),
        None if select.archived => marked_start(marks.archived.keys()),
        None => Some(window(SINCE)),
    };
    let mut threads = Threads::new(select.grep.as_deref());
    if select.escalated {
        threads = threads.escalated(crate::humans::load(&js, &nats.me).await);
    }
    if let Some(start) = start {
        let keyring = sealed::Keyring::load(nats, &js).await.map(Arc::new);
        scan_opening(&js, start, keyring, |entry| threads.add(&entry)).await?;
    }
    let present: HashMap<ThreadId, DateTime<Utc>> =
        threads.summaries().map(|t| (t.thread_id.clone(), t.last_at)).collect();
    let (mut threads, omitted) = threads.finish(select, &marks);
    if let Some(meta) = &meta {
        let mut titles = meta.titles(threads.iter().map(|t| &t.thread_id)).await?;
        let mut dones = meta.dones(threads.iter().map(|t| &t.thread_id)).await?;
        for thread in &mut threads {
            thread.title = titles.remove(&thread.thread_id).map(|t| t.title);
            let done = dones.remove(&thread.thread_id).filter(|d| d.holds(thread.last_reopening));
            thread.done = done.map(|d| Done { by: d.by, at: d.at });
        }
        adopt_suggested(meta, &mut threads).await;
    }
    if let (Some(meta), Some(start)) = (&meta, &start) {
        prune(meta, &marks, start, &present).await;
    }
    Ok((threads, omitted))
}

/// Forget the marks of the CLI's human and the titles of the threads a read from `start` found no message on though
/// it read every message stored on them ([`covers`]), set more than [`thread_meta::KEPT`] ago, and the human's keeps
/// of threads `present` (each with its last message's time) since active again; a failure is only warned of. Titles
/// are looked through only when the read went back that far.
pub async fn prune(
    meta: &thread_meta::Meta,
    marks: &ThreadMarks,
    start: &consumer::DeliverPolicy,
    present: &HashMap<ThreadId, DateTime<Utc>>,
) {
    let titles = match start {
        consumer::DeliverPolicy::All => true,
        consumer::DeliverPolicy::ByStartTime { start_time } => {
            SystemTime::now().checked_sub(thread_meta::KEPT).is_some_and(|kept| SystemTime::from(*start_time) <= kept)
        }
        _ => false,
    };
    let gone = |thread: &ThreadId| !present.contains_key(thread) && covers(start, thread);
    let active = |thread: &ThreadId, kept| present.get(thread).is_some_and(|last_at| *last_at >= kept);
    match meta.prune(marks, titles, gone, active).await {
        Ok((0, 0)) => {}
        Ok((marks, titles)) => tracing::debug!(marks, titles, "forgot the marks and titles of threads with no message"),
        Err(e) => tracing::warn!("not forgetting the marks and titles of threads with no message: {e:#}"),
    }
}

/// Whether a read from `start` reads every message stored on `thread`: one from the thread's start on, when its id
/// is time-ordered.
fn covers(start: &consumer::DeliverPolicy, thread: &ThreadId) -> bool {
    match start {
        consumer::DeliverPolicy::All => true,
        consumer::DeliverPolicy::ByStartTime { start_time } => {
            ants_daemon::channel::thread_start(thread).is_some_and(|at| at >= SystemTime::from(*start_time))
        }
        _ => false,
    }
}

/// Title each of `threads` without a title as an ant suggested, if it never had one
/// ([`thread_meta::Meta::adopt_title`]) and no sealed message is on it.
async fn adopt_suggested(meta: &thread_meta::Meta, threads: &mut [Summary]) {
    for thread in threads.iter_mut().filter(|t| t.title.is_none() && !t.sealed) {
        let Some((by, text)) = &thread.suggested else { continue };
        match meta.adopt_title(&thread.thread_id, text, by).await {
            Ok(title) => thread.title = title.map(|t| t.title),
            Err(e) => tracing::warn!(thread = %thread.thread_id, "not storing the title {by} suggested: {e:#}"),
        }
    }
}

/// Where to read threads marked so from: the earliest of their starts, or everything kept when an id is not
/// time-ordered; None when no thread is marked so.
fn marked_start<'a>(threads: impl Iterator<Item = &'a ThreadId>) -> Option<consumer::DeliverPolicy> {
    let mut earliest = None;
    for thread in threads {
        let Some(start) = ants_daemon::channel::thread_start(thread) else {
            return Some(consumer::DeliverPolicy::All);
        };
        earliest = Some(earliest.map_or(start, |e: SystemTime| e.min(start)));
    }
    earliest.map(|start| consumer::DeliverPolicy::ByStartTime { start_time: start.into() })
}

/// Every message stored on `thread`, in stream order, with the reactions to each: read from `since` ago, else from the
/// thread's start when its id is time-ordered, else from all the stream keeps. With `tools`, the tool calls its ants
/// stored on their timelines too, each once as its latest record has it ([`timeline`]), placed by when it started.
/// Sealed messages are opened with `keyring`, which looks up a sending device added since it was loaded.
pub async fn history(
    js: &jetstream::Context,
    thread: &ThreadId,
    since: Option<Duration>,
    tools: bool,
    keyring: Option<Arc<sealed::Keyring>>,
) -> Result<Vec<Entry>> {
    history_with(js, thread, since, tools, Parser::opening(keyring)).await
}

/// [`history`], read with `parser`.
pub async fn history_with(
    js: &jetstream::Context,
    thread: &ThreadId,
    since: Option<Duration>,
    tools: bool,
    mut parser: Parser,
) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    let mut timelines = Timelines::default();
    let mut seqs = HashMap::new();
    let start = show_start(thread, since);
    reading::read_back_awaiting(js, "", Reading::Gets, subject::ALL, start, async |seq, stored, msg| {
        let (subject, payload) = (msg.subject.as_str(), &msg.payload[..]);
        if let Some((on, _)) = subject::parse_timeline(subject) {
            if tools && on == *thread {
                add_timeline(&mut timelines, &mut seqs, seq, subject, payload);
            }
        } else if let Some(entry) = parser.read(seq, stored.into(), subject, payload).await
            && entry.thread_id == *thread
        {
            entries.push(entry);
        }
        ControlFlow::Continue(())
    })
    .await?;
    if !timelines.is_empty() {
        let calls = timelines.calls().iter().map(|call| {
            let key = (call.ant.clone(), call.message, call.call.id.clone());
            Entry::tool(seqs.get(&key).copied().unwrap_or_default(), call)
        });
        entries.extend(calls);
        entries.sort_by_key(|e| e.stored_at);
    }
    let ids: Vec<Uuid> = entries.iter().filter_map(|e| e.id).collect();
    let mut reactions = crate::inbox::reactions_or_none(js, &ids).await;
    for entry in &mut entries {
        if let Some(id) = entry.id {
            entry.reactions = reactions.remove(&id).unwrap_or_default();
        }
    }
    Ok(entries)
}

/// The tool calls of the timeline record stored at `seq` on `subject`, each an entry of kind tool as that record has
/// it; none for a record that does not parse or that its subject does not name.
pub fn tool_entries(seq: u64, subject: &str, payload: &[u8]) -> Vec<Entry> {
    let mut timelines = Timelines::default();
    let Ok(record) = serde_json::from_slice::<Timeline>(payload) else { return Vec::new() };
    if !timelines.add(subject, record) {
        return Vec::new();
    }
    timelines.calls().iter().map(|c| Entry::tool(seq, c)).collect()
}

/// Merge the timeline record stored at `seq` on `subject`, noting it as the last of each call it holds.
fn add_timeline(
    timelines: &mut Timelines,
    seqs: &mut HashMap<(UserId, Uuid, String), u64>,
    seq: u64,
    subject: &str,
    payload: &[u8],
) {
    let Ok(record) = serde_json::from_slice::<Timeline>(payload) else {
        tracing::debug!(seq, subject, "skipping a timeline record that does not parse");
        return;
    };
    let calls: Vec<_> = record
        .entries
        .iter()
        .filter_map(|e| match e {
            ants_proto::TimelineEntry::Tool(call) => Some((record.ant.clone(), record.message, call.id.clone())),
            _ => None,
        })
        .collect();
    if timelines.add(subject, record) {
        seqs.extend(calls.into_iter().map(|key| (key, seq)));
    } else {
        tracing::debug!(seq, subject, "skipping a timeline record its subject does not name");
    }
}

/// When the last message stored on `thread` was, reading from the thread's start when its id is time-ordered, else
/// from all the stream keeps; None when none is left.
pub async fn last_activity(js: &jetstream::Context, thread: &ThreadId) -> Result<Option<DateTime<Utc>>> {
    let mut last = None;
    scan(js, show_start(thread, None), |entry| {
        if entry.thread_id == *thread {
            last = Some(entry.stored_at);
        }
    })
    .await?;
    Ok(last)
}

/// `ants thread show THREAD`: every message stored on the thread `pick` picks, in the order the stream stored them,
/// and unless `no_tools` the tool calls its ants made.
pub async fn show(
    nats: &crate::Nats<'_>,
    pick: Pick,
    since: Option<Duration>,
    json: bool,
    no_tools: bool,
) -> Result<()> {
    let auto_archive = crate::defaults::auto_archive()?;
    let js = connect(nats).await?;
    let thread = &pick.resolve(&js, &nats.me).await?;
    let keyring = sealed::Keyring::load(nats, &js).await.map(Arc::new);
    let parser = Parser::opening(keyring).checking(Some(Arc::new(crate::behalf::Checker::new(nats, &js))));
    let entries = history_with(&js, thread, since, !no_tools, parser).await?;
    let sealed = entries.iter().any(|e| e.sealed.is_some());
    let suggested = entries.iter().find_map(|e| Some((e.from.as_ref()?, e.suggested_title.as_deref()?)));
    let suggested = suggested.filter(|_| !sealed);
    let thread_meta::About { title, mark, done } = if json {
        thread_meta::About::default()
    } else {
        let messages: Vec<Entry> = entries.iter().filter(|e| e.kind != Kind::Tool).cloned().collect();
        thread_meta::of_or_none(nats, &js, thread, &messages, suggested, auto_archive).await
    };
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
        if let Some(title) = title {
            let by_ant = entries.iter().any(|e| e.suggested_title.is_some() && e.from.as_ref() == Some(&title.by));
            let suggested = if by_ant { format!(" (suggested by {})", title.by) } else { String::new() };
            println!("title: {}{suggested}", title.title);
        }
        if let Some(done) = done {
            println!("done by {} {}", done.by, time(done.at));
        }
        match mark {
            Some(Mark::Pinned) => println!("pinned"),
            Some(Mark::Archived { new_activity: true }) => println!("archived, with messages since"),
            Some(Mark::Archived { new_activity: false }) => println!("archived"),
            Some(Mark::Idle) => {
                let idle = ants_proto::duration::format(auto_archive.unwrap_or_default());
                println!("auto-archived: no message for {idle} (`ants thread unarchive` keeps it)");
            }
            None => {}
        }
        print!("{}", render(&entries));
    }
    Ok(())
}

mod message;

#[cfg(test)]
mod tests;
