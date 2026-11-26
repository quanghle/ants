//! One stored message on a thread, an [`Entry`], with its [`Kind`], and what an ant stored on its timeline as an entry
//! has it ([`ToolCall`], [`NoReply`], [`PromptEnded`], [`ToolRejection`]), and the watchers' wakes a later wake of
//! theirs withdrew ([`withdrawn_wakes`]).

use ants_proto::{
    Acting, Answer, Answered, ChannelId, DecidedBy, EscalationReason, NoReplyWhy, NoticeKind, Questions, Quote,
    Reacted, SummaryRequest, ThreadId, TimelineEnd, UserId, Uuid, WatchWake,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::attachments;
use crate::sealed;
use crate::text::took;

/// A tool call an ant stored on its timeline, as a [`Kind::Tool`] entry has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    /// The call's id, unique within its prompt.
    pub call_id: String,
    /// The message whose prompt made the call.
    pub message: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
}

/// A prompt an ant ended with no reply sent, from its timeline, as a [`Kind::NoReply`] entry has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoReply {
    /// The message whose prompt ended so.
    pub message: Uuid,
    pub why: NoReplyWhy,
    /// The reason the ant gave to `complete`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// How a prompt of an ant ended and what it took, from its timeline, as a [`Kind::Ended`] entry has it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptEnded {
    /// The message whose prompt ended so.
    pub message: Uuid,
    #[serde(flatten)]
    pub end: TimelineEnd,
}

// The cost is read from JSON, which has no NaN.
impl Eq for PromptEnded {}

/// A tool request that was rejected or expired, as a [`Kind::Rejected`] entry has it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRejection {
    /// The message whose prompt asked for the tool.
    pub message: Uuid,
    pub request: Uuid,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tool_kind: String,
    pub by: DecidedBy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decider: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<UserId>,
}

/// What a stored message on a thread is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    Tool,
    /// A prompt an ant ended with no reply sent, from its timeline records: not a message, so not searched.
    NoReply,
    /// A rejected or expired tool request from an ant's timeline records: not a message, so not searched.
    Rejected,
    /// How a prompt of an ant ended and what it took, from its timeline records: not a message, so not searched.
    Ended,
}

impl Kind {
    /// The kinds that are stored messages, which a search looks through: not those from an ant's timeline records
    /// ([`Kind::on_timeline`]).
    pub const MESSAGES: [Self; 8] = [
        Self::Message,
        Self::Escalation,
        Self::Mention,
        Self::Post,
        Self::Note,
        Self::Approval,
        Self::Decision,
        Self::Dead,
    ];

    pub fn as_str(self) -> &'static str {
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
            Self::Rejected => "rejected",
            Self::Ended => "ended",
        }
    }

    /// Whether an entry of the kind comes from an ant's timeline records rather than a stored message.
    pub fn on_timeline(self) -> bool {
        matches!(self, Self::Tool | Self::NoReply | Self::Rejected | Self::Ended)
    }

    /// Whether an entry of the kind is a message, channel post or note, which reopens a thread marked done
    /// ([`ants_proto::ThreadDone::holds`]): no tool request, decision on one or dead letter does.
    pub fn reopens(self) -> bool {
        matches!(self, Self::Message | Self::Escalation | Self::Mention | Self::Post | Self::Note)
    }
}

/// A kind of stored message by its name ([`Kind::as_str`]), in any case: none of those from an ant's timeline records.
impl std::str::FromStr for Kind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let found = Self::MESSAGES.into_iter().find(|kind| kind.as_str().eq_ignore_ascii_case(s));
        found.ok_or_else(|| format!("unknown kind of message {s:?}"))
    }
}

/// The sequence of the last of `entries` that reopens a thread marked done ([`Kind::reopens`]); 0 for none.
pub fn last_reopening(entries: &[Entry]) -> u64 {
    entries.iter().filter(|e| e.kind.reopens()).map(|e| e.seq).max().unwrap_or(0)
}

/// One stored message on a thread, as a reader shows it. It serializes in full, and reads back as it was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub seq: u64,
    /// The message's id, which a reaction to it names: none for a tool request or a dead letter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    /// When the stream stored it.
    pub stored_at: DateTime<Utc>,
    pub kind: Kind,
    pub thread_id: ThreadId,
    /// The sender; none for a human's decision on a tool request, which does not say which human decided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<UserId>,
    /// The recipient of an inbox message, the ant a decision or dead letter is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<UserId>,
    /// The channel of a post or of the post a mention tells of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    /// The team an inbox message was sent to, which delivered it to its lead ([`Envelope::team`]).
    ///
    /// [`Envelope::team`]: ants_proto::Envelope::team
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<UserId>,
    /// The users a channel post @mentions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<UserId>,
    /// The thread's owner as the message names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// What the kind adds: an escalation's level and chain, a tool request's id, a dead letter's error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub text: String,
    /// The quote of the message or post an inbox message or channel post replies to, as its sender gave it
    /// ([`Envelope::quote`]); a sealed one's only when opened.
    ///
    /// [`Envelope::quote`]: ants_proto::Envelope::quote
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<Quote>,
    /// The files attached to an inbox message, channel post, notes post or dead letter, each with its handle.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<attachments::Listed>,
    /// The subject it was stored on names another sender (or ant) than the message: the message is forged.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub forged: bool,
    /// For whom the sender, an assistant, sent an inbox message, as the reader checked its `on_behalf_of`
    /// ([`crate::parser::Lookups::acting`]): a claim not honoured is forged, and the message the sender's own.
    #[serde(default, skip_serializing_if = "Acting::is_own")]
    pub acting: Acting,
    /// What the reader made of a sealed message between two humans: opened, its text the message's; else its text
    /// `[sealed]` or `[forged]` ([`sealed::State::placeholder`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<sealed::State>,
    /// The reactions to the message, each with who reacted so.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reactions: Vec<Reacted>,
    /// The reason of the escalation an inbox message carries, which the note spells out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<EscalationReason>,
    /// The stuck ant of the escalation an inbox message carries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stuck: Option<UserId>,
    /// The title an inbox message, channel post or notes post that is not forged suggests for its thread
    /// ([`Envelope::title`], [`ChannelPost::title`], [`NotesPost::title`]).
    ///
    /// [`Envelope::title`]: ants_proto::Envelope::title
    /// [`ChannelPost::title`]: ants_proto::ChannelPost::title
    /// [`NotesPost::title`]: ants_proto::NotesPost::title
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_title: Option<String>,
    /// The summary request an inbox message that is not forged carries, or answers ([`Envelope::summary`]).
    ///
    /// [`Envelope::summary`]: ants_proto::Envelope::summary
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryRequest>,
    /// The questions an inbox message that is not forged asks, if valid ([`Envelope::asks`]).
    ///
    /// [`Envelope::asks`]: ants_proto::Envelope::asks
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<Questions>,
    /// The answer an inbox message that is not forged gives a question read before it, if valid
    /// ([`Envelope::answers`]); an invalid one, or one to a question not read, leaves an ordinary message.
    ///
    /// [`Envelope::answers`]: ants_proto::Envelope::answers
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<Answer>,
    /// A tool call's details, its title the text and its ant the sender.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<ToolCall>,
    /// Why a prompt ended with no reply sent, its ant the sender and its final text the text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_reply: Option<NoReply>,
    /// A rejected or expired tool request, its title the text and the requesting ant the sender.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected: Option<Box<ToolRejection>>,
    /// How a prompt ended and what it took, its ant the sender.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended: Option<Box<PromptEnded>>,
    /// The ant-to-ant turns an inbox message, channel post or notes post leaves on its thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turns: Option<u32>,
    /// An inbox message a webhook or schedule sent ([`Envelope::webhook`]).
    ///
    /// [`Envelope::webhook`]: ants_proto::Envelope::webhook
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub webhook: bool,
    /// The provider whose wake request an inbox message that is not forged is ([`Envelope::wake_provider`]).
    ///
    /// [`Envelope::wake_provider`]: ants_proto::Envelope::wake_provider
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// The watcher whose wake an inbox message that is a provider's wake is ([`Envelope::wake_watcher`]): the watcher,
    /// its check's output, and the watcher's earlier wake the daemon withdrew for this one ([`withdrawn_wakes`]).
    ///
    /// [`Envelope::wake_watcher`]: ants_proto::Envelope::wake_watcher
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watcher: Option<Box<WatchWake>>,
    /// The thread an inbox message's thread was spun off ([`Envelope::origin_thread`]).
    ///
    /// [`Envelope::origin_thread`]: ants_proto::Envelope::origin_thread
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<ThreadId>,
    /// The message an inbox message, or the post a channel post, answers ([`Envelope::in_reply_to`],
    /// [`ChannelPost::in_reply_to`]).
    ///
    /// [`Envelope::in_reply_to`]: ants_proto::Envelope::in_reply_to
    /// [`ChannelPost::in_reply_to`]: ants_proto::ChannelPost::in_reply_to
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The daemon notice an inbox message that is not forged is ([`Envelope::notice`]): a line of the system's, not an
    /// ant's answer.
    ///
    /// [`Envelope::notice`]: ants_proto::Envelope::notice
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice: Option<NoticeKind>,
}

impl Entry {
    pub fn new(seq: u64, stored_at: DateTime<Utc>, kind: Kind, thread_id: ThreadId) -> Self {
        Self {
            seq,
            id: None,
            stored_at,
            kind,
            thread_id,
            from: None,
            to: None,
            channel: None,
            team: None,
            mentions: Vec::new(),
            owner: None,
            note: None,
            text: String::new(),
            quote: None,
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
            rejected: None,
            ended: None,
            turns: None,
            webhook: false,
            provider: None,
            watcher: None,
            origin: None,
            in_reply_to: None,
            notice: None,
        }
    }

    /// Whether a search looks at its text: not that of a sealed message the reader did not open.
    pub fn matchable(&self) -> bool {
        self.sealed.is_none_or(|state| state == sealed::State::Opened)
    }

    /// The entry of a tool call merged from timeline records, the last of them stored at `seq`.
    pub fn tool(seq: u64, merged: &ants_proto::MergedCall) -> Self {
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
    pub fn no_reply(seq: u64, merged: &ants_proto::MergedNoReply) -> Self {
        let end = NoReply { message: merged.message, why: merged.why, reason: merged.reason.clone() };
        Self {
            from: Some(merged.ant.clone()),
            text: merged.text.clone(),
            no_reply: Some(end),
            ..Self::new(seq, merged.at, Kind::NoReply, merged.thread_id.clone())
        }
    }

    /// The entry of a rejected tool request merged from timeline records, the last of them stored at `seq`.
    pub fn rejected(seq: u64, merged: &ants_proto::MergedRejection) -> Self {
        let rejection = &merged.rejection;
        let rejected = ToolRejection {
            message: merged.message,
            request: rejection.request,
            tool_kind: rejection.kind.clone(),
            by: rejection.by,
            decider: rejection.decider.clone(),
            via: rejection.via.clone(),
        };
        Self {
            from: Some(merged.ant.clone()),
            text: rejection.title.clone(),
            rejected: Some(Box::new(rejected)),
            ..Self::new(seq, merged.at, Kind::Rejected, merged.thread_id.clone())
        }
    }

    /// The entry of how a prompt ended, from the timeline record stored at `seq`.
    pub fn ended(seq: u64, merged: &ants_proto::MergedEnd) -> Self {
        let ended = PromptEnded { message: merged.message, end: merged.end.clone() };
        Self {
            from: Some(merged.ant.clone()),
            ended: Some(Box::new(ended)),
            ..Self::new(seq, merged.at, Kind::Ended, merged.thread_id.clone())
        }
    }

    /// What this entry answers `question`'s questions, an entry asking them, with, one per question: set only on a
    /// valid answer ([`Entry::answer`]).
    pub fn answers<'q>(&'q self, question: &'q Entry) -> Option<Vec<Answered<'q>>> {
        let answer = self.answer.as_ref()?;
        (question.id == Some(answer.question)).then_some(())?;
        question.question.as_ref()?.answered(answer)
    }

    /// Who sent it to whom: `alice -> dev-a`, `dev-a -> #ops`, `dev-a -> notes`, `(a human) -> dev-a`; for an
    /// assistant's message for its human, `alice via pa -> dev-a`, or `pa (forged: for alice) -> dev-a`.
    pub fn route(&self) -> String {
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
    pub fn sender(&self) -> String {
        self.acting.label(self.from.as_ref().map_or("(a human)", UserId::as_str))
    }

    /// The users it was between.
    pub fn users(&self) -> impl Iterator<Item = &UserId> {
        self.from.iter().chain(&self.to)
    }

    /// The watcher whose wake it is, the ant's to itself, and the ant: none for a forged message.
    fn watcher_wake(&self) -> Option<(&WatchWake, &UserId)> {
        let watcher = self.watcher.as_deref().filter(|_| self.kind == Kind::Message && !self.forged)?;
        let ant = self.from.as_ref().filter(|from| self.to.as_ref() == Some(*from) && self.provider.is_some())?;
        Some((watcher, ant))
    }
}

/// The ids of the watchers' wakes among `entries` that a later wake of the same watcher, on the same thread, withdrew
/// from the ant's queue before the ant took them up ([`WatchWake::replaces`]): a reader hides them. A wake counts
/// only as the ant's to itself and not forged, for the one it names and the one naming it alike.
pub fn withdrawn_wakes(entries: &[Entry]) -> std::collections::HashSet<Uuid> {
    let wakes: std::collections::HashMap<Uuid, (&Entry, &WatchWake, &UserId)> = entries
        .iter()
        .filter_map(|entry| Some((entry.id?, entry, entry.watcher_wake()?)))
        .map(|(id, entry, (watcher, ant))| (id, (entry, watcher, ant)))
        .collect();
    wakes
        .values()
        .filter_map(|(entry, watcher, ant)| {
            let replaced = watcher.replaces?;
            let (earlier, earlier_watcher, earlier_ant) = wakes.get(&replaced)?;
            let same = earlier_watcher.id == watcher.id && earlier_ant == ant && earlier.thread_id == entry.thread_id;
            same.then_some(replaced)
        })
        .collect()
}

/// `entries` without the watchers' wakes a later wake withdrew ([`withdrawn_wakes`]).
pub fn without_withdrawn_wakes(mut entries: Vec<Entry>) -> Vec<Entry> {
    let withdrawn = withdrawn_wakes(&entries);
    if !withdrawn.is_empty() {
        entries.retain(|entry| entry.id.is_none_or(|id| !withdrawn.contains(&id)));
    }
    entries
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// An entry with every field set to other than what [`Entry::new`] leaves it.
    fn full() -> Entry {
        let user = |name: &str| name.parse::<UserId>().unwrap();
        let id = Uuid::from_u128;
        let at = DateTime::from_timestamp(1_700_000_000, 5_000_000).unwrap();
        let thread: ThreadId = "t1".parse().unwrap();
        let attachment = ants_proto::Attachment {
            id: id(9),
            ..ants_proto::Attachment::new("a.txt", "text/plain", 1, "ab".repeat(32)).unwrap()
        };
        let listed = attachments::Listed {
            handle: format!("alice.inbox.dev-a.{}", id(9)).parse().unwrap(),
            attachment,
            unavailable: Some("its id is repeated".into()),
            stored: Some(at),
        };
        let question = ants_proto::Question::new("Ship?", &["yes".to_owned(), "no".to_owned()], true).unwrap();
        let end = serde_json::from_value(json!({ "how": "replied", "duration_ms": 5, "tokens": 7 })).unwrap();
        Entry {
            id: Some(id(1)),
            from: Some(user("alice")),
            to: Some(user("dev-a")),
            channel: Some("ops".parse().unwrap()),
            team: Some(user("backend")),
            mentions: vec![user("bob")],
            owner: Some(user("alice")),
            note: Some("a note".into()),
            text: "hi".into(),
            quote: Some(Quote::new(user("dev-a"), "earlier")),
            attachments: vec![listed],
            forged: true,
            acting: Acting::For(user("carol")),
            sealed: Some(sealed::State::Opened),
            reactions: vec![Reacted::new("eyes".parse().unwrap(), vec![user("bob")])],
            reason: Some(EscalationReason::Cap),
            stuck: Some(user("dev-b")),
            suggested_title: Some("A title".into()),
            summary: Some(SummaryRequest::thread(thread.clone(), 4)),
            question: Some(Questions::new(vec![question]).unwrap()),
            answer: Some(Answer { question: id(2), picks: vec![ants_proto::Pick::option(1)] }),
            tool: Some(ToolCall {
                call_id: "c1".into(),
                message: id(3),
                tool_kind: Some("execute".into()),
                status: Some("completed".into()),
                duration_ms: Some(12),
                paths: vec!["src/lib.rs".into()],
            }),
            no_reply: Some(NoReply { message: id(4), why: NoReplyWhy::Completed, reason: Some("done".into()) }),
            rejected: Some(Box::new(ToolRejection {
                message: id(5),
                request: id(6),
                tool_kind: "execute".into(),
                by: DecidedBy::Chain,
                decider: Some(user("lead")),
                via: Some(user("pa")),
            })),
            ended: Some(Box::new(PromptEnded { message: id(7), end })),
            turns: Some(3),
            webhook: true,
            provider: Some("sense".into()),
            watcher: Some(Box::new(WatchWake {
                id: "rem-1".into(),
                title: "CI of #7".into(),
                done: true,
                output: Some("red".into()),
                cut: true,
                replaces: Some(id(10)),
            })),
            origin: Some("t0".parse().unwrap()),
            in_reply_to: Some(id(8)),
            notice: Some(NoticeKind::Spend),
            ..Entry::new(42, at, Kind::Message, thread)
        }
    }

    #[test]
    fn an_entry_reads_back_from_what_it_serializes_to_with_every_field() {
        let entry = full();
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 36, "a field left out: {json}");
        assert_eq!(json["attachments"][0]["stored"], "2023-11-14T22:13:20.005Z");
        assert_eq!((&json["in_reply_to"], &json["turns"]), (&json!(Uuid::from_u128(8)), &json!(3)));
        assert_eq!(serde_json::from_value::<Entry>(json).unwrap(), entry);

        let bare = Entry::new(1, entry.stored_at, Kind::NoReply, entry.thread_id);
        let json = serde_json::to_value(&bare).unwrap();
        assert_eq!(
            json,
            json!({ "seq": 1, "stored_at": bare.stored_at, "kind": "no_reply", "thread_id": "t1", "text": "" })
        );
        assert_eq!(serde_json::from_value::<Entry>(json).unwrap(), bare);
    }

    /// Watcher `watcher`'s wake `id` from `ant` to `to` on thread `t1`, replacing the wake `replaces`.
    fn wake(id: u128, watcher: &str, ant: &str, to: &str, replaces: Option<u128>) -> Entry {
        let watch = WatchWake {
            id: watcher.into(),
            title: "CI of #7".into(),
            done: false,
            output: None,
            cut: false,
            replaces: replaces.map(Uuid::from_u128),
        };
        Entry {
            id: Some(Uuid::from_u128(id)),
            from: Some(ant.parse().unwrap()),
            to: Some(to.parse().unwrap()),
            provider: Some("sense".into()),
            watcher: Some(Box::new(watch)),
            ..Entry::new(u64::try_from(id).unwrap(), Utc::now(), Kind::Message, "t1".parse().unwrap())
        }
    }

    fn ids(entries: &[Entry]) -> Vec<u128> {
        entries.iter().map(|entry| entry.id.unwrap().as_u128()).collect()
    }

    #[test]
    fn a_watcher_s_wake_hides_the_earlier_wake_it_withdrew_and_only_that() {
        let entries = vec![
            wake(1, "w", "dev-a", "dev-a", None),
            wake(2, "w", "dev-a", "dev-a", Some(1)),
            wake(3, "w", "dev-a", "dev-a", None),
            wake(4, "w", "dev-a", "dev-a", Some(3)),
        ];
        assert_eq!(ids(&without_withdrawn_wakes(entries)), [2, 4]);
        // The daemon names no wake the ant took up already: those stay.
        let taken = vec![wake(1, "w", "dev-a", "dev-a", None), wake(2, "w", "dev-a", "dev-a", None)];
        assert_eq!(ids(&without_withdrawn_wakes(taken)), [1, 2]);
    }

    #[test]
    fn a_wake_names_no_other_watcher_s_wake_nor_one_elsewhere_and_a_forged_one_none() {
        let other_watcher = vec![wake(1, "w", "dev-a", "dev-a", None), wake(2, "v", "dev-a", "dev-a", Some(1))];
        assert_eq!(ids(&without_withdrawn_wakes(other_watcher)), [1, 2]);
        let elsewhere = Entry { thread_id: "t2".parse().unwrap(), ..wake(1, "w", "dev-a", "dev-a", None) };
        assert_eq!(ids(&without_withdrawn_wakes(vec![elsewhere, wake(2, "w", "dev-a", "dev-a", Some(1))])), [1, 2]);
        let forged = Entry { forged: true, ..wake(2, "w", "dev-a", "dev-a", Some(1)) };
        assert_eq!(ids(&without_withdrawn_wakes(vec![wake(1, "w", "dev-a", "dev-a", None), forged])), [1, 2]);
        let from_another = wake(2, "w", "dev-b", "dev-a", Some(1));
        assert_eq!(ids(&without_withdrawn_wakes(vec![wake(1, "w", "dev-a", "dev-a", None), from_another])), [1, 2]);
        let another_ant = wake(2, "w", "dev-b", "dev-b", Some(1));
        assert_eq!(ids(&without_withdrawn_wakes(vec![wake(1, "w", "dev-a", "dev-a", None), another_ant])), [1, 2]);
        let unwoken = Entry { provider: None, ..wake(2, "w", "dev-a", "dev-a", Some(1)) };
        assert_eq!(ids(&without_withdrawn_wakes(vec![wake(1, "w", "dev-a", "dev-a", None), unwoken])), [1, 2]);
        let earlier_forged = Entry { forged: true, ..wake(1, "w", "dev-a", "dev-a", None) };
        assert_eq!(
            ids(&without_withdrawn_wakes(vec![earlier_forged, wake(2, "w", "dev-a", "dev-a", Some(1))])),
            [1, 2]
        );
    }
}
