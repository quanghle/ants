//! One stored message on a thread, an [`Entry`], with its [`Kind`].

use ants_daemon::reactions::Reacted;
use ants_proto::{
    Acting, Answer, Answered, ChannelId, EscalationReason, Question, SummaryRequest, ThreadId, UserId, Uuid,
};
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::text::took;
use super::{NoReply, ToolCall};
use crate::{attachments, sealed};

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
    /// A prompt an ant ended with no reply sent, from its timeline records: not a message, so not searched.
    #[value(skip)]
    NoReply,
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
            Self::NoReply => "no reply",
        }
    }

    /// Whether an entry of the kind comes from an ant's timeline records rather than a stored message.
    pub fn on_timeline(self) -> bool {
        matches!(self, Self::Tool | Self::NoReply)
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
    ///
    /// [`Envelope::title`]: ants_proto::Envelope::title
    /// [`ChannelPost::title`]: ants_proto::ChannelPost::title
    /// [`NotesPost::title`]: ants_proto::NotesPost::title
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_title: Option<String>,
    /// The summary request an inbox message that is not forged carries, or answers ([`Envelope::summary`]).
    ///
    /// [`Envelope::summary`]: ants_proto::Envelope::summary
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryRequest>,
    /// The question an inbox message that is not forged asks, if valid ([`Envelope::asks`]).
    ///
    /// [`Envelope::asks`]: ants_proto::Envelope::asks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub question: Option<Question>,
    /// The answer an inbox message that is not forged gives a question read before it, if valid
    /// ([`Envelope::answers`]); an invalid one, or one to a question not read, leaves an ordinary message.
    ///
    /// [`Envelope::answers`]: ants_proto::Envelope::answers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<Answer>,
    /// A tool call's details, its title the text and its ant the sender.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolCall>,
    /// Why a prompt ended with no reply sent, its ant the sender and its final text the text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_reply: Option<NoReply>,
    /// The ant-to-ant turns an inbox message, channel post or notes post leaves on its thread.
    #[serde(skip)]
    pub turns: Option<u32>,
    /// An inbox message a webhook or schedule sent ([`Envelope::webhook`]).
    ///
    /// [`Envelope::webhook`]: ants_proto::Envelope::webhook
    #[serde(skip)]
    pub webhook: bool,
    /// The thread an inbox message's thread was spun off ([`Envelope::origin_thread`]).
    ///
    /// [`Envelope::origin_thread`]: ants_proto::Envelope::origin_thread
    #[serde(skip)]
    pub origin: Option<ThreadId>,
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
            no_reply: None,
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
    pub(super) fn tool(seq: u64, merged: &ants_proto::MergedCall) -> Self {
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

    /// The entry of a prompt that ended with no reply sent, from timeline records, the last of them stored at `seq`.
    pub(super) fn no_reply(seq: u64, merged: &ants_proto::MergedNoReply) -> Self {
        let end = NoReply { message: merged.message, why: merged.why, reason: merged.reason.clone() };
        Self {
            from: Some(merged.ant.clone()),
            text: merged.text.clone(),
            no_reply: Some(end),
            ..Self::new(seq, merged.at, Kind::NoReply, merged.thread_id.clone())
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
            (_, None, _) => from,
        }
    }

    /// Who sent it, as [`Acting::label`] names an assistant acting for its human.
    pub(crate) fn sender(&self) -> String {
        self.acting.label(self.from.as_ref().map_or("(a human)", UserId::as_str))
    }

    /// The users it was between.
    pub(super) fn users(&self) -> impl Iterator<Item = &UserId> {
        self.from.iter().chain(&self.to)
    }
}
