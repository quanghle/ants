use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    Answer, ApprovalDecision, ApprovalRequest, Attachment, ChannelId, DEFAULT_TURN_BUDGET, Error, Escalation, Humans,
    Question, Role, Sealed, ThreadId, UserId, subject,
};
#[cfg(doc)]
use crate::{
    ChannelPost, LINKS_BUCKET, MAX_TITLE, OWNERS_BUCKET, THREADS_BUCKET, ThreadOrigin, ThreadSummary, ThreadTitle,
    Verdict, WEBHOOK_ALLOW, WEBHOOKS_BUCKET, owner_key, summary_key, title_key, webhook_marker_key,
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
    /// A webhook's message, as `ants webhook serve` marks it: its sender is no human, whatever the humans bucket says,
    /// and an answer to it goes to the thread's owner, if any, not to the webhook's inbox, which nobody reads
    /// ([`Envelope::sender_role`]). A daemon takes it only from a sender whose marker is in [`WEBHOOKS_BUCKET`]
    /// ([`webhook_marker_key`]), which only a webhook's credentials write: from any other it is cleared, so no ant
    /// sends an answer to the owner it names.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub webhook: bool,
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
    /// A question the sender asks the recipient, a human, to answer by picking one of its options or typing an answer
    /// ([`Envelope::asking`]). The envelope's id is the question's, and its text repeats the question with the options
    /// numbered. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub question: Option<Question>,
    /// The answer to a question the recipient asked the sender on the thread ([`Envelope::answering`]); the text is the
    /// option picked or the typed answer. A reader takes an invalid answer ([`Envelope::answers`]) as an ordinary
    /// message. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<Answer>,
    /// A human's message steering the ant on the thread, as `ants ask --steer` sends it: when the ant runs a prompt
    /// on the thread, the recipient's daemon cancels it and prompts the same session with this message next, ahead of
    /// the messages waiting there. Honoured only from a human ([`Envelope::sender_role`]) to an ant; any other, a
    /// reader that does not know the field, or a daemon with no prompt running on the thread, handles it as an
    /// ordinary message. Never copied by [`Envelope::reply`] or [`Envelope::follow_up`].
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub steer: bool,
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

/// Where the channel post a mention notice tells of is ([`Envelope::mention`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mention {
    pub channel: ChannelId,
    /// The post's id.
    pub post: Uuid,
    /// The post's author, when the notice is relayed by another ([`ChannelPost::relayed_notice`]): the envelope's
    /// `from` sent it. It is the sender's word only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<UserId>,
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
            owner: None,
            creator: None,
            origin_thread: None,
            escalation: None,
            approval: None,
            webhook: false,
            allow: None,
            mention: None,
            title: None,
            summary: None,
            question: None,
            answer: None,
            steer: false,
            attachments: Vec::new(),
            sealed: None,
            sent_at: Utc::now(),
        }
    }

    /// The envelope steering its recipient ([`Envelope::steer`]).
    #[must_use]
    pub fn steering(self) -> Self {
        Self { steer: true, ..self }
    }

    /// The envelope with `owner` as the human who started its thread.
    #[must_use]
    pub fn with_owner(self, owner: Option<UserId>) -> Self {
        Self { owner, ..self }
    }

    /// The envelope as a webhook's message ([`Envelope::webhook`]).
    #[must_use]
    pub fn from_webhook(self) -> Self {
        Self { webhook: true, ..self }
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

    /// `from`'s decision on an approval request, to the requesting ant on `thread_id`, the request's thread.
    pub fn decision(thread_id: ThreadId, from: UserId, decision: ApprovalDecision) -> Self {
        let text = format!("{} on approval request {}", decision.verdict.as_str(), decision.request_id);
        let to = decision.ant.clone();
        Self { approval: Some(Box::new(decision)), ..Self::on_thread(thread_id, from, to, text) }
    }

    /// Whether the sender is in `chain`, the escalate_to of the ant the envelope is for. Trust it only once the
    /// envelope passed [`Envelope::check_subject`].
    pub fn is_from_chain(&self, chain: &[UserId]) -> bool {
        chain.contains(&self.from)
    }

    /// The approval decision the envelope carries on `request`, if it comes from `chain`, the requester's escalate_to,
    /// and is for that request. A verdict the request did not offer counts as [`Verdict::Reject`]. Trust it only once
    /// the envelope passed [`Envelope::check_subject`].
    pub fn chain_decision(&self, request: &ApprovalRequest, chain: &[UserId]) -> Option<ApprovalDecision> {
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
            owner: self.owner.clone(),
            creator: self.creator.clone(),
            origin_thread: self.origin_thread.clone(),
            escalation: None,
            approval: None,
            webhook: false,
            allow: self.allow.clone(),
            mention: None,
            title: None,
            summary: None,
            question: None,
            answer: None,
            steer: false,
            attachments: Vec::new(),
            sealed: None,
            sent_at: Utc::now(),
        })
    }

    pub fn subject(&self) -> String {
        subject::inbox(&self.to, &self.from)
    }

    /// Check that the envelope arrived on its own [`Envelope::subject`]. Credentials pin the sender token of an inbox
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongSubject { subject: subject.to_owned(), from: self.from.clone(), to: self.to.clone() })
    }
}

/// A post to the notes of a thread, on [`subject::notes`]: one shared board per thread, where the ants on it coordinate
/// together; a post wakes nobody, and each of them can read the whole board.
///
/// Posts spend the thread's turn budget like messages between ants do, and each carries what is left. The post that
/// spends the last turn hands the thread to the human, and the notes take no more ant posts until the human posts
/// there with a fresh budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotesPost {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    pub text: String,
    /// Turns left in the notes after this post.
    pub turn_budget: u32,
    /// The title the posting ant suggests for the thread, as [`Envelope::title`]: an ant's reply posted to the notes
    /// carries the one its MCP tool suggest_thread_title took.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The files attached to the post, stored before it under its [`NotesPost::attachment_handle`]s.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    pub sent_at: DateTime<Utc>,
}

impl NotesPost {
    /// A post with a fresh budget, as the human posts.
    pub fn new(thread_id: ThreadId, from: UserId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            title: None,
            attachments: Vec::new(),
            sent_at: Utc::now(),
        }
    }

    /// An ant's post, spending one turn of what is left: the lower of `budget`, the turns of the message the ant
    /// is handling, and those the notes' `last` post left. Fails when none are left.
    pub fn spend(
        thread_id: ThreadId,
        from: UserId,
        text: impl Into<String>,
        budget: u32,
        last: Option<&NotesPost>,
    ) -> Result<Self, Error> {
        let left = last.map_or(budget, |last| last.turn_budget.min(budget));
        let turn_budget = left.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(thread_id.clone()))?;
        Ok(Self { turn_budget, ..Self::new(thread_id, from, text) })
    }

    pub fn subject(&self) -> String {
        subject::notes(&self.thread_id, &self.from)
    }

    /// Check that the post arrived on its own [`NotesPost::subject`]. Credentials pin the sender token of a notes
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongNotesSubject {
            subject: subject.to_owned(),
            from: self.from.clone(),
            thread: self.thread_id.clone(),
        })
    }
}

/// The users `text` @mentions, in the order of their first mention.
///
/// A mention is `@` and a user id, with the `@` at the start of the text or after whitespace or an opening bracket
/// or quote, so an address such as `a@b` mentions nobody. Punctuation after the id (`,.:;!?)` and the like) ends it;
/// a letter, `_`, `@` or a dot followed by a letter or digit (`@host.example`) means it was no user id.
pub fn parse_mentions(text: &str) -> Vec<UserId> {
    let mut found: Vec<UserId> = Vec::new();
    let mut prev: Option<char> = None;
    for (i, c) in text.char_indices() {
        if c == '@' && prev.is_none_or(|p| p.is_whitespace() || "([{<\"'`".contains(p)) {
            let rest = &text[i + 1..];
            let end =
                rest.find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')).unwrap_or(rest.len());
            let (name, after) = rest.split_at(end);
            let mut after = after.chars();
            let glued = match after.next() {
                Some(c) if c.is_alphanumeric() || c == '_' || c == '@' => true,
                Some('.') => after.next().is_some_and(char::is_alphanumeric),
                _ => false,
            };
            if !glued
                && let Ok(id) = name.parse::<UserId>()
                && !found.contains(&id)
            {
                found.push(id);
            }
        }
        prev = Some(c);
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChannelPost, p};

    #[test]
    fn reply_swaps_parties_and_links_the_message() {
        let ask = Envelope::new(UserId::human(), p("dev-a"), "hi");
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

        let humans = Humans::default();
        let post = ChannelPost::new("ops".parse().unwrap(), p("github"), "push").with_allow(rules.clone());
        let answer = post.reply(p("dev-a"), "on it", &[], &humans).unwrap();
        assert_eq!(answer.allow, rules);
        assert_eq!(answer.reply(UserId::human(), "thanks", &[], &humans).unwrap().allow, None);
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
        let human = NotesPost::new(t.clone(), UserId::human(), "go on");
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
    fn a_steer_is_left_out_when_unset_and_never_carried_on() {
        let plain = Envelope::new(p("alice"), p("dev-a"), "go left");
        assert!(!serde_json::to_string(&plain).unwrap().contains("steer"));
        let steer = plain.steering();
        let json = serde_json::to_string(&steer).unwrap();
        assert!(json.contains(r#""steer":true"#), "{json}");
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), steer);
        assert!(!steer.reply(p("dev-a"), "ok", Role::Human).unwrap().steer);
        assert!(!steer.follow_up(p("dev-a"), p("dev-b"), "x").steer);
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let msg = Envelope::new(UserId::human(), UserId::messenger(), "route this");
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("in_reply_to"));
        assert_eq!(serde_json::from_str::<Envelope>(&json).unwrap(), msg);
    }
}
