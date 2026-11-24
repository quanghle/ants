//! Messages: the [`Envelope`] between ants, or between an ant and a human, on [`crate::subject::inbox`]
//! (`ants.inbox.<to>.<from>`), with its replies, follow-ups and summary requests ([`SummaryRequest`]); the
//! [`NotesPost`] on a thread's shared notes ([`crate::subject::notes`]), which the `ANTS` stream keeps too; and the
//! @mentions of a text ([`parse_mentions`]).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Answer, ApprovalDecision, ApprovalRequest, Attachment, DEFAULT_TURN_BUDGET, Error, Escalation, Humans, Questions,
    Role, RunId, Sealed, ThreadId, TrailSpent, TrailStart, TrailStep, UserId, subject,
};
#[cfg(doc)]
use crate::{
    ChannelPost, LINKS_BUCKET, MAX_TITLE, OWNERS_BUCKET, REVIEW_ALLOW, THREADS_BUCKET, ThreadOrigin, ThreadSummary,
    ThreadTitle, Verdict, WEBHOOK_ALLOW, owner_key, summary_key, title_key,
};

mod mention;
mod notes;
mod notice;
mod review;

pub use mention::{Mention, Quote, parse_mentions};
pub use notes::NotesPost;
pub use notice::NoticeKind;
pub use review::{
    FindingOutcome, FindingVerdict, MAX_OUTCOME_REASON, MAX_REVIEW_FINDINGS_BYTES, ReviewCommit, ReviewOutcomes,
};

/// One message between ants, or between an ant and a human.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    pub to: UserId,
    pub text: String,
    /// Ant-to-ant replies left on this thread. At zero the thread goes back to a human: its owner, else the
    /// default one.
    pub turn_budget: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<Uuid>,
    /// The quote of the message `in_reply_to` names, as a human's quoted reply to a message on the thread carries it
    /// ([`Envelope::quoting`]); the recipient's daemon puts it in the prompt. Sealed with the text on a message between
    /// two humans. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<Quote>,
    /// The human who started the thread. Replies and the messages an ant sends on or from the thread keep it. A
    /// daemon takes it as a human or a webhook ([`Envelope::webhook`]) says; from an ant, only as stored state says:
    /// on a channel thread, the thread's posts ([`ThreadOrigin`]); on any other, the thread's record in
    /// [`OWNERS_BUCKET`] for the sender, or for an ant that passed the link to the thread on to it, else that of the
    /// thread it was spun off ([`Envelope::origin_thread`]); else there is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The ant whose top-level channel post started the thread ([`ChannelPost::thread_creator`]), kept like `owner`.
    /// Its escalate_to is the thread's origin chain ([`Escalation::new_from`]); a creator that is a human is none. A
    /// daemon takes it from an ant or a webhook only as the thread's top-level post kept in the stream says
    /// ([`ThreadOrigin`]): on a thread no channel post started, that of the channel thread an ant's message links to
    /// ([`Envelope::origin_thread`]), else there is none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    /// The thread this message's thread was spun off, as when an ant handling a message on it sends one on a new
    /// thread, set while the message has a creator or an owner: the link a daemon checks the `creator` and `owner` of
    /// an ant's message against on a thread no channel post started. Replies and the messages an ant sends on or
    /// from the thread keep it. A daemon takes a linked channel thread's creator and owner only when a top-level
    /// channel post kept in the stream started it and the sender, a member or the messenger of its channel, posted on
    /// it or was woken by a post on it, or such an ant passed the link on to it ([`LINKS_BUCKET`]); a linked message
    /// thread's owner as [`OWNERS_BUCKET`] records it only when a human or webhook messaged the sender on that thread
    /// ([`owner_key`]), or such an ant passed the link on to it; else drops the link.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_thread: Option<ThreadId>,
    /// The escalation the message carries up a chain, or answers. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`]: the daemon sets it with [`Envelope::with_escalation`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<Box<Escalation>>,
    /// An ant's decision on an approval request of `to`, for its daemon rather than its harness. The subject pins the
    /// sender, and the requester takes it only from its own chain ([`Envelope::chain_decision`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<Box<ApprovalDecision>>,
    /// The human the sender, its assistant, sent the message for ([`crate::Acting`]): the sender stays `from`, and a
    /// reader honours the claim only while the human's grant and the sender's credentials say it is the human's
    /// assistant, else takes it as forged and the message as the sender's own. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`]: the sender sets it with [`Envelope::on_behalf`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
    /// A webhook's message, as `ants webhook serve` and schedules send it: its sender is no human, whatever the humans
    /// bucket says, and an answer to it goes to the thread's owner, if any, not to the webhook's inbox, which nobody
    /// reads ([`Envelope::sender_role`]). It goes on [`subject::webhook_inbox`], which only a webhook's credentials
    /// (and humans') publish on, and any other envelope on [`subject::inbox`], which no webhook's do: a reader takes
    /// an envelope whose flag disagrees with its subject as forged ([`Envelope::check_subject`]), so no ant passes for
    /// a webhook, nor a webhook for an ant.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub webhook: bool,
    /// The provider, by name, whose wake request the message is ([`crate::provider::method::WAKE`]): the ant's daemon
    /// sends it as the ant to the ant itself, and an answer to it goes to the thread's owner, else to the default
    /// human, as for any message the ant sends itself. It holds only on a message from the ant to itself
    /// ([`Envelope::wake_provider`]). Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// A message that waits its turn behind the prompt running on its thread rather than steering it, as
    /// `ants ask --queue` and the chat's `/later` send it. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub queue: bool,
    /// The tool rules the thread is narrowed to, as a webhook or schedule that started it, or the message it follows on
    /// from, says: the recipient's daemon allows a tool call unasked only when these and the ant's own `allow` both
    /// allow it, so they never widen what an ant may do and are taken from any sender. A daemon fills in
    /// [`WEBHOOK_ALLOW`] for a webhook's message without them and drops them from a human's. Replies and the messages
    /// an ant sends on or from the thread keep them, so the narrowing follows a thread's spin-offs, until a human
    /// messages the ant on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow: Option<Vec<String>>,
    /// The channel post that @mentioned `to`, a human member of the channel, when the message tells it so
    /// ([`ChannelPost::mention_notice`]): `from` is the post's author, or the channel's messenger relaying a webhook's
    /// post ([`ChannelPost::relayed_notice`]), the thread and text are the post's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mention: Option<Mention>,
    /// The title the sending ant suggests for the thread, as its MCP tool suggest_thread_title took it: a trimmed line
    /// of 1 to [`MAX_TITLE`] characters ([`ThreadTitle::new`]). A human's CLI reading it on a thread whose
    /// [`title_key`] has no entry in [`THREADS_BUCKET`], not even the delete marker a cleared title leaves until its
    /// thread is gone, stores it there by the sender, so a title a human set or cleared wins. The subject pins the
    /// sender, so only an ant that messaged on the thread suggests its title. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// A human's request for a summary of the entries the text carries ([`SummaryRequest`]), or, on an ant's reply to
    /// one, the same request with the tokens its prompt spent. A daemon prompts for a summary only when a human sent
    /// it and clears it from any other sender; its reply carries it back. A reader that knows nothing of it takes
    /// either as an ordinary message. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryRequest>,
    /// 1 to 4 questions the sender asks the recipient, a human, to answer together, picking options of each or typing
    /// an answer ([`Envelope::asking`]). The envelope's id is the question's, and its text repeats each question with
    /// its options numbered. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<Questions>,
    /// The answer to questions the recipient asked the sender on the thread, a pick for each ([`Envelope::answering`]);
    /// the text is the answer in words ([`Questions::answer_text`]). A reader takes an invalid answer
    /// ([`Envelope::answers`]) as an ordinary message. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<Answer>,
    /// The files attached to the message, stored before it under its [`Envelope::attachment_handle`]s. Never copied
    /// by [`Envelope::reply`] or [`Envelope::follow_up`]: a message passing an attachment on stores it again under its
    /// own.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    /// The text, and the suggested title and attachments if any, of a message between two humans, sealed so only
    /// their devices read them ([`Sealed`]); `text` is then empty, and `title` and `attachments` are none. Only a
    /// human seals, and only to another human. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<Box<Sealed>>,
    /// What the message tells of, when the sender's daemon sent it as the ant to tell of the ant's state or of what
    /// happened on the thread, rather than as the ant's answer ([`NoticeKind`]). A reader shows it as a line of the
    /// system's: it makes no thread finished and is unread-worthy, listed or pushed only when it needs a human to act
    /// ([`NoticeKind::needs_action`]). Never copied by [`Envelope::reply`] or [`Envelope::follow_up`]: the daemon sets
    /// it with [`Envelope::as_notice`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notice: Option<NoticeKind>,
    /// The commits the review packet the message carries names, on a message from an ant narrowed to [`REVIEW_ALLOW`]:
    /// the recipient's daemon checks each out for the review under its ant's home. Never copied by [`Envelope::reply`]
    /// or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub review_commits: Vec<ReviewCommit>,
    /// The trail run and call the message is a step of, on the step's own thread: its recipient answers with an
    /// ordinary reply, which the conductor knows by that thread ([`crate::PendingCall::thread`]). Only the
    /// conductor's daemon sets it, with [`Envelope::as_step`]. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail_step: Option<Box<TrailStep>>,
    /// On a reply to a trail step on the step's thread, or its sender's [`NoticeKind::RunSpent`] notice there, what the
    /// runs its sender started with `run_trail` while answering the step spent in all so far ([`TrailSpent`]): the
    /// step's conductor charges what it has not charged yet to the step's run. Never copied by [`Envelope::reply`] or
    /// [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail_spent: Option<TrailSpent>,
    /// The trail run a schedule asks its recipient to start, the message's thread the run's: the recipient's daemon
    /// starts it, without a prompt, only from a webhook's message ([`Envelope::webhook`]). Never copied by
    /// [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail_start: Option<Box<TrailStart>>,
    /// The trail run whose end the message invites its conductor to rate, on the conductor's message to itself on the
    /// run's thread: the conductor's daemon, finding it is that invitation, lets `rate_trail` through the thread's
    /// narrowed tool rules ([`Envelope::allow`]) for its turn, which rates that run alone. Only the conductor's daemon
    /// sets it. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rating_of: Option<RunId>,
    /// On a message from an ant narrowed to [`REVIEW_ALLOW`], the dream whose review it asks for and what is new
    /// since the sender's last review in the verdicts and outcomes of the recipient's earlier findings for it: the
    /// recipient's daemon tells the recipient's sense, which keeps a record of each review. Never copied by
    /// [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_outcomes: Option<ReviewOutcomes>,
    /// The team the message was addressed to, sent on to its lead, `to` ([`crate::TeamDirectory`]): the lead's daemon
    /// opens the prompt saying the message came through the team. The sender sets it; a reader takes it as a claim of
    /// the sender's only. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team: Option<UserId>,
    pub sent_at: DateTime<Utc>,
}

/// What a summary request ([`Envelope::summary`]) asks to summarise: the entries of one thread, a TL;DR stored under
/// the thread's [`summary_key`] in [`THREADS_BUCKET`] ([`ThreadSummary`]), or of every thread with entries since a
/// time, a digest nobody stores. Either covers the entries up to the ANTS stream sequence `upto`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SummaryRequest {
    #[serde(flatten)]
    pub of: SummaryOf,
    /// The ANTS sequence of the last entry the text carries.
    pub upto: u64,
    /// On the reply, the tokens the prompt spent, as the harness reported them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u64>,
}

/// The entries a [`SummaryRequest`] covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryOf {
    /// A TL;DR of one thread.
    Thread(ThreadId),
    /// A digest of every thread with entries since then.
    Since(DateTime<Utc>),
}

impl SummaryRequest {
    /// A TL;DR of `thread` up to sequence `upto`.
    pub fn thread(thread: ThreadId, upto: u64) -> Self {
        Self { of: SummaryOf::Thread(thread), upto, tokens: None }
    }

    /// A digest of every thread with entries since `since`, up to sequence `upto`.
    pub fn since(since: DateTime<Utc>, upto: u64) -> Self {
        Self { of: SummaryOf::Since(since), upto, tokens: None }
    }

    /// The thread a TL;DR is of; none for a digest.
    pub fn thread_id(&self) -> Option<&ThreadId> {
        match &self.of {
            SummaryOf::Thread(thread) => Some(thread),
            SummaryOf::Since(_) => None,
        }
    }
}

impl Envelope {
    /// Start a new thread.
    pub fn new(from: UserId, to: UserId, text: impl Into<String>) -> Self {
        Self::on_thread(ThreadId::generate(), from, to, text)
    }

    /// A message on an existing thread, with a fresh budget and no owner.
    pub fn on_thread(thread_id: ThreadId, from: UserId, to: UserId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            to,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            in_reply_to: None,
            quote: None,
            owner: None,
            creator: None,
            origin_thread: None,
            escalation: None,
            approval: None,
            on_behalf_of: None,
            webhook: false,
            provider: None,
            queue: false,
            allow: None,
            mention: None,
            title: None,
            summary: None,
            question: None,
            answer: None,
            attachments: Vec::new(),
            sealed: None,
            notice: None,
            review_commits: Vec::new(),
            trail_step: None,
            trail_spent: None,
            trail_start: None,
            rating_of: None,
            review_outcomes: None,
            team: None,
            sent_at: Utc::now(),
        }
    }

    /// The envelope as a daemon's notice of `kind` ([`Envelope::notice`]).
    #[must_use]
    pub fn as_notice(self, kind: NoticeKind) -> Self {
        Self { notice: Some(kind), ..self }
    }

    /// The envelope as `step` of a trail run ([`Envelope::trail_step`]).
    #[must_use]
    pub fn as_step(self, step: TrailStep) -> Self {
        Self { trail_step: Some(Box::new(step)), ..self }
    }

    /// The envelope as asking its recipient to start `start` ([`Envelope::trail_start`]).
    #[must_use]
    pub fn starting(self, start: Option<TrailStart>) -> Self {
        Self { trail_start: start.map(Box::new), ..self }
    }

    /// The envelope with `owner` as the human who started its thread.
    #[must_use]
    pub fn with_owner(self, owner: Option<UserId>) -> Self {
        Self { owner, ..self }
    }

    /// The envelope as a reply to message `id`, quoting it with `quote` ([`Envelope::quote`]).
    #[must_use]
    pub fn quoting(self, id: Uuid, quote: Quote) -> Self {
        Self { in_reply_to: Some(id), quote: Some(quote), ..self }
    }

    /// The envelope sent for `human`, by its assistant ([`Envelope::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
    }

    /// The envelope as a webhook's message ([`Envelope::webhook`]).
    #[must_use]
    pub fn from_webhook(self) -> Self {
        Self { webhook: true, ..self }
    }

    /// The envelope as provider `name`'s wake request ([`Envelope::provider`]).
    #[must_use]
    pub fn woken_by(self, name: impl Into<String>) -> Self {
        Self { provider: Some(name.into()), ..self }
    }

    /// The provider whose wake request the envelope is: none unless it is a message from the ant to itself.
    pub fn wake_provider(&self) -> Option<&str> {
        self.provider.as_deref().filter(|_| self.from == self.to)
    }

    /// The role of the sender, given the `humans`: an ant's for a webhook's message, else as `humans` says.
    pub fn sender_role(&self, humans: &Humans) -> Role {
        if self.webhook { Role::Ant } else { humans.role(&self.from) }
    }

    /// The envelope narrowed to the tool rules `allow` ([`Envelope::allow`]).
    #[must_use]
    pub fn with_allow(self, allow: Option<Vec<String>>) -> Self {
        Self { allow, ..self }
    }

    /// The envelope with `creator` as the ant that started its thread with a channel post.
    #[must_use]
    pub fn with_creator(self, creator: Option<UserId>) -> Self {
        Self { creator, ..self }
    }

    /// The envelope linking to `origin_thread`, the thread its creator or owner comes from
    /// ([`Envelope::origin_thread`]): none without a creator or an owner, set first, or on that thread itself.
    #[must_use]
    pub fn with_origin_thread(self, origin_thread: Option<ThreadId>) -> Self {
        let origin_thread = origin_thread.filter(|t| self.links_origin() && *t != self.thread_id);
        Self { origin_thread, ..self }
    }

    /// The envelope carrying `escalation`.
    #[must_use]
    pub fn with_escalation(self, escalation: Option<Escalation>) -> Self {
        Self { escalation: escalation.map(Box::new), ..self }
    }

    /// `from`'s decision on an approval request, to the requesting ant on `thread_id`, the request's thread; made for
    /// a human when the decision says so ([`ApprovalDecision::on_behalf_of`]).
    pub fn decision(thread_id: ThreadId, from: UserId, decision: ApprovalDecision) -> Self {
        let text = format!("{} on approval request {}", decision.verdict.as_str(), decision.request_id);
        let to = decision.ant.clone();
        let on_behalf_of = decision.on_behalf_of.clone();
        Self { approval: Some(Box::new(decision)), on_behalf_of, ..Self::on_thread(thread_id, from, to, text) }
    }

    /// Whether the sender is in `chain`, the escalate_to of the ant the envelope is for. Trust it only once the
    /// envelope passed [`Envelope::check_subject`].
    pub fn is_from_chain(&self, chain: &[UserId]) -> bool {
        chain.contains(&self.from)
    }

    /// The approval decision the envelope carries on `request`, if it comes from `chain`, the requester's escalate_to,
    /// and is for that request. A verdict the request did not offer counts as [`Verdict::Reject`]. Trust it only once
    /// the envelope passed [`Envelope::check_subject`]. A decision sent for a human ([`Envelope::on_behalf_of`]) is
    /// none: it takes effect only once the human confirms it.
    pub fn chain_decision(&self, request: &ApprovalRequest, chain: &[UserId]) -> Option<ApprovalDecision> {
        if self.on_behalf_of.is_some() {
            return None;
        }
        let decision = self.approval.as_ref()?;
        let ours = self.to == request.ant && decision.ant == request.ant;
        (ours && decision.request_id == request.id && self.is_from_chain(chain))
            .then(|| request.decide(decision.verdict))
    }

    /// A message on this envelope's thread, with a fresh budget and the same owner, creator, origin thread and tool
    /// rules.
    #[must_use]
    pub fn follow_up(&self, from: UserId, to: UserId, text: impl Into<String>) -> Self {
        let follow_up = Self::on_thread(self.thread_id.clone(), from, to, text)
            .with_owner(self.owner.clone())
            .with_creator(self.creator.clone())
            .with_allow(self.allow.clone());
        Self { origin_thread: self.origin_thread.clone(), ..follow_up }
    }

    /// The envelope moved to `thread`, spun off this one's: it keeps the link to the thread its creator or owner
    /// comes from ([`Envelope::origin_thread`]), this thread unless it was spun off one itself.
    #[must_use]
    pub fn spun_off(self, thread: ThreadId) -> Self {
        let origin = self.origin_thread.clone().unwrap_or_else(|| self.thread_id.clone());
        let origin_thread = (self.links_origin() && origin != thread).then_some(origin);
        Self { thread_id: thread, origin_thread, ..self }
    }

    /// The thread whose link the envelope passes on to its recipient ([`LINKS_BUCKET`]): the one it was spun off
    /// ([`Envelope::origin_thread`]), else its own while it has a creator or an owner.
    pub fn passes_on(&self) -> Option<&ThreadId> {
        self.origin_thread.as_ref().or_else(|| self.links_origin().then_some(&self.thread_id))
    }

    /// Whether the envelope has a creator or an owner for a link to its origin thread to vouch for.
    fn links_origin(&self) -> bool {
        self.creator.is_some() || self.owner.is_some()
    }

    /// Reply to the sender, whose role is `to`, keeping the thread's owner, creator, origin thread and tool rules.
    /// Replies to a human are free; ant-to-ant replies spend one turn, and fail when none are left, so the caller
    /// escalates to a human instead of looping.
    pub fn reply(&self, from: UserId, text: impl Into<String>, to: Role) -> Result<Self, Error> {
        let turn_budget = if to.is_human() {
            self.turn_budget
        } else {
            self.turn_budget.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(self.thread_id.clone()))?
        };
        Ok(Self {
            id: Uuid::now_v7(),
            thread_id: self.thread_id.clone(),
            to: self.from.clone(),
            from,
            text: text.into(),
            turn_budget,
            in_reply_to: Some(self.id),
            quote: None,
            owner: self.owner.clone(),
            creator: self.creator.clone(),
            origin_thread: self.origin_thread.clone(),
            escalation: None,
            approval: None,
            on_behalf_of: None,
            webhook: false,
            provider: None,
            queue: false,
            allow: self.allow.clone(),
            mention: None,
            title: None,
            summary: None,
            question: None,
            answer: None,
            attachments: Vec::new(),
            sealed: None,
            notice: None,
            review_commits: Vec::new(),
            trail_step: None,
            trail_spent: None,
            trail_start: None,
            rating_of: None,
            review_outcomes: None,
            team: None,
            sent_at: Utc::now(),
        })
    }

    /// The subject the envelope goes on: [`subject::webhook_inbox`] for a webhook's ([`Envelope::webhook`]), else
    /// [`subject::inbox`].
    pub fn subject(&self) -> String {
        if self.webhook { subject::webhook_inbox(&self.to, &self.from) } else { subject::inbox(&self.to, &self.from) }
    }

    /// Check that the envelope arrived on its own [`Envelope::subject`]. Credentials pin the sender token of an inbox
    /// subject, and whether it is a webhook's, so a `from` or a `webhook` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongSubject { subject: subject.to_owned(), from: self.from.clone(), to: self.to.clone() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChannelPost, p};

    #[test]
    fn a_wake_is_the_provider_s_only_on_the_ant_s_own_inbox_and_never_copied() {
        let wake = Envelope::new(p("dev-a"), p("dev-a"), "standup").woken_by("sense");
        assert_eq!(wake.wake_provider(), Some("sense"));
        let claimed = Envelope::new(p("dev-b"), p("dev-a"), "standup").woken_by("sense");
        assert_eq!(claimed.wake_provider(), None);
        assert_eq!(wake.reply(p("dev-a"), "done", Role::Human).unwrap().provider, None);
        assert_eq!(wake.follow_up(p("dev-a"), p("dev-b"), "on").provider, None);
    }

    #[test]
    fn a_notice_is_never_copied_into_a_reply_or_follow_up() {
        let notice = Envelope::new(p("dev-a"), crate::human(), "[ants] spent").as_notice(NoticeKind::Spend);
        assert_eq!(notice.reply(crate::human(), "ok", Role::Ant).unwrap().notice, None);
        assert_eq!(notice.follow_up(p("dev-a"), p("dev-b"), "on").notice, None);
    }

    #[test]
    fn reply_swaps_parties_and_links_the_message() {
        let ask = Envelope::new(crate::human(), p("dev-a"), "hi");
        let reply = ask.reply(p("dev-a"), "hello", Role::Human).unwrap();
        assert_eq!((reply.from.as_str(), reply.to.as_str()), ("dev-a", "human"));
        assert_eq!(reply.thread_id, ask.thread_id);
        assert_eq!(reply.in_reply_to, Some(ask.id));
        assert_eq!(reply.turn_budget, ask.turn_budget, "replying to the human spends no turn");
        assert_eq!(reply.subject(), "ants.inbox.human.dev-a");
    }

    #[test]
    fn a_spun_off_thread_links_to_the_channel_thread_its_creator_comes_from() {
        let (channel_thread, a, b): (ThreadId, ThreadId, ThreadId) =
            ("t0".parse().unwrap(), "t1".parse().unwrap(), "t2".parse().unwrap());
        let woken =
            Envelope::on_thread(channel_thread.clone(), p("lead"), p("dev-a"), "go").with_creator(Some(p("lead")));
        let spun = woken.reply(p("dev-a"), "help", Role::Ant).unwrap().spun_off(a.clone());
        assert_eq!((&spun.thread_id, &spun.origin_thread), (&a, &Some(channel_thread.clone())));
        let answer = spun.reply(p("dev-b"), "done", Role::Ant).unwrap();
        assert_eq!(answer.follow_up(p("dev-a"), p("dev-c"), "x").origin_thread, Some(channel_thread.clone()));
        assert_eq!(answer.clone().spun_off(b).origin_thread, Some(channel_thread.clone()), "still the channel thread");
        assert_eq!(answer.spun_off(channel_thread.clone()).origin_thread, None, "back on the channel thread");
        let uncreated = Envelope::on_thread(channel_thread.clone(), p("lead"), p("dev-a"), "go");
        assert_eq!(uncreated.clone().spun_off(a.clone()).origin_thread, None, "no creator or owner to keep");
        let owned = uncreated.with_owner(Some(p("alice"))).spun_off(a);
        assert_eq!(owned.origin_thread, Some(channel_thread), "the owner's thread");
    }

    #[test]
    fn the_tool_rules_follow_a_thread_and_its_spin_offs_but_not_a_human_s_reply() {
        let rules = Some(vec!["read:*".to_owned()]);
        let fired = Envelope::new(p("nightly"), p("dev-a"), "go").from_webhook().with_allow(rules.clone());
        let ask = fired.reply(p("dev-a"), "help", Role::Ant).unwrap().spun_off(ThreadId::generate());
        assert_eq!(ask.allow, rules);
        assert_eq!(ask.reply(p("dev-b"), "done", Role::Ant).unwrap().allow, rules);
        assert_eq!(ask.follow_up(p("dev-a"), p("dev-c"), "x").allow, rules);
        assert_eq!(Envelope::on_thread(ask.thread_id, p("dev-a"), p("dev-c"), "x").allow, None);
        let json = serde_json::to_string(&Envelope::new(p("dev-a"), p("dev-b"), "x")).unwrap();
        assert!(!json.contains("allow"), "{json}");

        let humans = crate::humans();
        let post = ChannelPost::new("ops".parse().unwrap(), p("github"), "push").with_allow(rules.clone());
        let answer = post.reply(p("dev-a"), "on it", &[], &humans).unwrap();
        assert_eq!(answer.allow, rules);
        assert_eq!(answer.reply(crate::human(), "thanks", &[], &humans).unwrap().allow, None);
    }

    #[test]
    fn mentions_are_user_ids_after_an_at_sign_that_starts_a_word() {
        let ids = |text: &str| parse_mentions(text).into_iter().map(String::from).collect::<Vec<_>>();
        assert_eq!(ids("@dev-a"), ["dev-a"]);
        assert_eq!(
            ids("hey @dev-a, @dev-b. @ops2: (@qa) @rev! @x?; @y;"),
            ["dev-a", "dev-b", "ops2", "qa", "rev", "x", "y"]
        );
        assert_eq!(ids("@dev-b then @dev-a then @dev-b"), ["dev-b", "dev-a"], "first-seen order, no duplicates");
        assert_eq!(ids("\"@dev-a\" [@dev-b] <@dev-c>\n\t@dev-d's"), ["dev-a", "dev-b", "dev-c", "dev-d"]);
        assert!(ids("mail a@b or someone@example.com").is_empty(), "addresses mention nobody");
        assert!(ids("@host.example @Dev @dev_a @a@b @ @@dev-a @-x").is_empty());
        assert_eq!(ids(&format!("@{} @ok", "x".repeat(65))), ["ok"]);
        assert_eq!(ids("end @dev-a."), ["dev-a"]);
    }

    #[test]
    fn notes_posts_spend_the_lower_of_the_message_and_the_notes_budget() {
        let t: ThreadId = "t1".parse().unwrap();
        let first = NotesPost::spend(t.clone(), p("dev-a"), "draft", 3, None).unwrap();
        assert_eq!(first.turn_budget, 2, "empty notes start from the message's budget");
        let second = NotesPost::spend(t.clone(), p("dev-b"), "agreed", 8, Some(&first)).unwrap();
        assert_eq!(second.turn_budget, 1, "the notes' budget is lower");
        let last = NotesPost::spend(t.clone(), p("dev-a"), "done", 1, Some(&second)).unwrap();
        assert_eq!(last.turn_budget, 0);
        let refused = NotesPost::spend(t.clone(), p("dev-b"), "more", 8, Some(&last));
        assert_eq!(refused, Err(Error::BudgetExhausted(t.clone())));
        let human = NotesPost::new(t.clone(), crate::human(), "go on");
        assert_eq!(
            NotesPost::spend(t, p("dev-b"), "more", 8, Some(&human)).unwrap().turn_budget,
            DEFAULT_TURN_BUDGET - 1
        );
    }

    #[test]
    fn ant_replies_spend_the_budget_until_exhausted() {
        let mut msg = Envelope::new(p("dev-a"), p("dev-b"), "plan?");
        msg.turn_budget = 1;
        let reply = msg.reply(p("dev-b"), "draft", Role::Ant).unwrap();
        assert_eq!(reply.turn_budget, 0);
        assert_eq!(reply.reply(p("dev-a"), "again", Role::Ant), Err(Error::BudgetExhausted(msg.thread_id)));
    }

    #[test]
    fn replies_keep_the_owner_and_any_human_s_reply_is_free() {
        let alice = p("alice");
        let ask = Envelope::new(alice.clone(), p("dev-a"), "build it").with_owner(Some(alice.clone()));
        let answer = ask.reply(p("dev-a"), "built", Role::Human).unwrap();
        assert_eq!((answer.to.clone(), answer.owner.clone()), (alice.clone(), Some(alice.clone())));
        assert_eq!(answer.turn_budget, ask.turn_budget, "replying to a human spends no turn, whatever its name");
        let handoff = ask.reply(p("dev-a"), "check it", Role::Human).unwrap();
        let mut handoff = Envelope { to: p("dev-b"), from: p("dev-a"), ..handoff };
        handoff.turn_budget = 3;
        let back = handoff.reply(p("dev-b"), "fine", Role::Ant).unwrap();
        assert_eq!((back.turn_budget, back.owner.clone()), (2, Some(alice.clone())), "an ant's reply spends one");
        let notice = back.follow_up(p("dev-b"), alice.clone(), "over to you");
        assert_eq!((notice.thread_id.clone(), notice.owner.clone()), (ask.thread_id, Some(alice)));
        assert_eq!((notice.turn_budget, notice.in_reply_to), (DEFAULT_TURN_BUDGET, None));
        let ownerless = Envelope::new(p("dev-a"), p("dev-b"), "q");
        assert_eq!(ownerless.reply(p("dev-b"), "a", Role::Ant).unwrap().owner, None);
    }

    #[test]
    fn a_queued_reply_says_so_and_neither_answer_nor_follow_up_carries_it() {
        let plain = Envelope::new(p("alice"), p("dev-a"), "then the docs");
        assert!(!serde_json::to_string(&plain).unwrap().contains("queue"));
        let queued = Envelope { queue: true, ..plain };
        let back: Envelope = serde_json::from_str(&serde_json::to_string(&queued).unwrap()).unwrap();
        assert!(back.queue);
        assert!(!queued.reply(p("dev-a"), "on it", Role::Human).unwrap().queue);
        assert!(!queued.follow_up(p("dev-a"), p("dev-b"), "docs").queue);
    }

    #[test]
    fn the_owner_is_left_out_when_none() {
        let msg = Envelope::new(p("dev-a"), p("dev-b"), "q");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("owner"), "{json}");
        let owned = msg.with_owner(Some(p("alice")));
        let json = serde_json::to_string(&owned).unwrap();
        assert!(json.contains(r#""owner":"alice""#), "{json}");
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), owned);
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let msg = Envelope::new(crate::human(), UserId::messenger(), "route this");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("in_reply_to"));
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
    }
}
