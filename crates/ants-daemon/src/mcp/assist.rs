//! The tools an assistant ant acts for its human with ([`ants_proto::AssistantGrant`]): `human_queue` reads what
//! waits in the human's home in ants chat, or one of its threads; `answer_for_human`, `send_for_human`,
//! `post_for_human`, `notes_for_human`, `react_for_human`, `title_for_human` and `done_for_human` answer questions,
//! send messages, post in channels and thread notes, react and title or mark threads done for it; `mark_for_human`
//! and `snooze_for_human` pin, archive and snooze its threads under keys naming the ant
//! ([`ants_proto::assisted_marks_key`], [`ants_proto::assisted_reminders_key`]), never changing the human's own marks
//! and snoozes, which its devices take in only while the grant names the ant and only on threads the human marked or
//! snoozed none of itself; `describe_channel_for_human` sets or clears a channel's description for it, asking the
//! channel manager ([`crate::chanadm`]); `decide_for_human`
//! decides a tool request waiting for it, held by the requesting ant's daemon until the human confirms it or a
//! pre-allow rule of the human covers it ([`crate::approvals`]); `order_for_human` sends an admin order for it, held
//! likewise by the daemon of the ant's machine ([`crate::admin::held`]); `queue_for_human`, `cancel_for_human` and
//! `withdraw_for_human` list an ant's queue, cancel its prompt on a thread and withdraw a message waiting for it,
//! asking the ant's daemons on subjects naming the ant ([`ants_proto::CancelRequest::for_human`]); while none
//! answers, `withdraw_for_human` records the withdrawal of a message of the human's under a key naming the ant instead
//! ([`crate::delivery::attempts::record_withdrawal`]), which the ant's daemon honours before taking the message up.
//!
//! The tools are offered only while the human's grant names the ant ([`Hub::acting_for`]), and each call checks it
//! again, so a withdrawal ends them at once. Everything they publish is the ant's own, carrying `on_behalf_of` naming
//! the human, which readers verify against the grant. The human hears of each act in its inbox, on its activity thread of
//! the day ([`crate::people::behalf::report`]): from the recipient's daemon for a message to an ant, a decision, a cancel or a
//! withdrawal ([`crate::people::behalf`]), else from the ant itself, as for a post, which every daemon sees. Before a message to an ant, the ant records the human as the thread's owner
//! for that ant ([`crate::records::owners::record_for_human`]). Sealed messages open,
//! and messages to humans are sealed, only while the human enrolled the ant as one of its devices
//! ([`crate::secrets::sealing::AssistantSealing`]).

use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use ants_proto::{
    AdminOrder, ApprovalRequest, CancelReply, CancelRequest, Envelope, Humans, QueueRequest, Reaction, ReadMarker,
    ThreadDone, ThreadId, ThreadMarks, ThreadSnoozes, ThreadTitle, UserId, Uuid,
};
use anyhow::Result;
use futures::future::BoxFuture;
use rmcp::handler::server::router::tool::ToolRouter;

use super::Hub;
use super::tools::Tools;
use crate::delivery::attempts::Recorded;
use crate::delivery::queue;
use crate::secrets::sealing::{AssistantSealing, Opening};

mod ants;
mod channels;
mod home;
mod store;
mod threads;

pub use store::jetstream_store;

/// How far back the human's messages and the tool requests waiting for it are read.
const LOOKBACK: Duration = Duration::from_hours(7 * 24);
/// How long the cancel and queue tools wait for a daemon of the ant to answer.
const ANSWER_WAIT: Duration = Duration::from_secs(5);
/// How long they wait on after a first answer, for another daemon of the same ant.
const ANSWER_GRACE: Duration = Duration::from_millis(500);

/// A change of a human's pins and archives, or of its snoozes ([`HumanStore::change_marks`]): whether it changed
/// anything.
pub type Change<T> = Box<dyn Fn(&mut T) -> bool + Send + Sync>;

/// A message as stored, with its stream sequence.
#[derive(Clone, Debug)]
pub struct HumanMessage {
    pub seq: u64,
    pub envelope: Envelope,
}

/// What the assistant tools read and write for the human: its messages, the tool requests waiting for it, its read
/// marker, its threads' titles, done marks and reactions, and its pins, archives and snoozes.
pub trait HumanStore: Send + Sync {
    /// The messages to and from `human` stored since `since`, and those `assistant` sent for it, in the order stored,
    /// each checked against its subject.
    fn messages(&self, human: UserId, assistant: UserId, since: SystemTime)
    -> BoxFuture<'_, Result<Vec<HumanMessage>>>;
    /// The tool requests stored since `since`, and the ids of those a human decided.
    fn approvals(&self, since: SystemTime) -> BoxFuture<'_, Result<(Vec<ApprovalRequest>, HashSet<Uuid>)>>;
    /// Which of `human`'s inbox messages it read.
    fn read_marker(&self, human: UserId) -> BoxFuture<'_, Result<ReadMarker>>;
    /// The done mark of `thread`, if any.
    fn done(&self, thread: ThreadId) -> BoxFuture<'_, Result<Option<ThreadDone>>>;
    /// The stream sequence of the last message stored, which a done mark holds up to.
    fn last_sequence(&self) -> BoxFuture<'_, Result<u64>>;
    fn put_title(&self, title: ThreadTitle) -> BoxFuture<'_, Result<()>>;
    fn put_done(&self, done: ThreadDone) -> BoxFuture<'_, Result<()>>;
    /// `human`'s own pins and archives.
    fn marks(&self, human: UserId) -> BoxFuture<'_, Result<ThreadMarks>>;
    /// `human`'s own snoozes.
    fn snoozes(&self, human: UserId) -> BoxFuture<'_, Result<ThreadSnoozes>>;
    /// Change the pins and archives `assistant` set for `human` with `change`, on top of whatever changed meanwhile.
    /// Whether that changed anything.
    fn change_marks(
        &self,
        human: UserId,
        assistant: UserId,
        change: Change<ThreadMarks>,
    ) -> BoxFuture<'_, Result<bool>>;
    /// Change the snoozes `assistant` set for `human` so.
    fn change_snoozes(
        &self,
        human: UserId,
        assistant: UserId,
        change: Change<ThreadSnoozes>,
    ) -> BoxFuture<'_, Result<bool>>;
    /// Add `by`'s reaction to `message` for `human`, or remove it. Whether anything changed.
    fn react(
        &self,
        by: UserId,
        message: Uuid,
        reaction: Reaction,
        remove: bool,
        human: UserId,
    ) -> BoxFuture<'_, Result<bool>>;
    /// Record `human` as the owner of `thread` that the message `assistant` sends for it to `to` names.
    fn record_owner(&self, thread: ThreadId, to: UserId, human: UserId, assistant: UserId)
    -> BoxFuture<'_, Result<()>>;
    /// The answers of the daemons of the ant to `request`, as many as come within [`ANSWER_WAIT`], and within
    /// [`ANSWER_GRACE`] of the first.
    fn cancel(&self, request: CancelRequest) -> BoxFuture<'_, Result<Vec<CancelReply>>>;
    /// The messages waiting for the ant of `request`, as `ants queue` lists them ([`queue::waiting`]) from the answers
    /// to `request`, gathered as [`HumanStore::cancel`] gathers them, with up to `shown` of those the ant's daemons
    /// have not taken from the stream yet, which wake it as `humans` tell, and how many more of those there are. None
    /// when no daemon of the ant answers.
    fn queue(
        &self,
        request: QueueRequest,
        humans: Humans,
        shown: usize,
    ) -> BoxFuture<'_, Result<Option<queue::Listed>>>;
    /// Record the withdrawal of `message` from the queue of `ant` by `assistant` for `human`, while no daemon of the
    /// ant answers ([`crate::delivery::attempts::record_withdrawal`]).
    fn record_withdrawal(
        &self,
        ant: UserId,
        message: Uuid,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<Recorded>>;
    /// Sign `order` with the ant's credentials and send it to the hub's stream, once stored: the order as sent.
    fn send_order(&self, order: AdminOrder) -> BoxFuture<'_, Result<AdminOrder>>;
    /// The id of the identity record of `ant`, which a read of its memories carries to its provider
    /// ([`crate::profile::identity::id_of`]): None when it cannot be read.
    fn identity_id(&self, ant: UserId) -> BoxFuture<'_, Option<String>>;
}

/// What opens the human's sealed messages and seals those sent for it: [`AssistantSealing`].
pub trait HumanSealing: Send + Sync {
    fn open<'a>(&'a self, human: &'a UserId, envelope: &'a Envelope) -> BoxFuture<'a, Result<Opening>>;
    fn seal<'a>(&'a self, human: &'a UserId, envelope: &'a mut Envelope) -> BoxFuture<'a, Result<()>>;
    /// Seal `title`, set for `human` on a thread between `human` and `other` whose messages are sealed.
    fn seal_title<'a>(
        &'a self,
        human: &'a UserId,
        other: &'a UserId,
        title: &'a mut ThreadTitle,
    ) -> BoxFuture<'a, Result<()>>;
}

impl HumanSealing for AssistantSealing {
    fn open<'a>(&'a self, human: &'a UserId, envelope: &'a Envelope) -> BoxFuture<'a, Result<Opening>> {
        Box::pin(AssistantSealing::open(self, human, envelope))
    }

    fn seal<'a>(&'a self, human: &'a UserId, envelope: &'a mut Envelope) -> BoxFuture<'a, Result<()>> {
        Box::pin(AssistantSealing::seal(self, human, envelope))
    }

    fn seal_title<'a>(
        &'a self,
        human: &'a UserId,
        other: &'a UserId,
        title: &'a mut ThreadTitle,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(AssistantSealing::seal_title(self, human, other, title))
    }
}

/// The human's messages, as read for one call.
struct Messages {
    human: UserId,
    stored: Vec<HumanMessage>,
}

impl Messages {
    fn find(&self, id: Uuid) -> Option<&HumanMessage> {
        self.stored.iter().find(|m| m.envelope.id == id)
    }

    fn on(&self, thread: &ThreadId) -> impl Iterator<Item = &HumanMessage> {
        self.stored.iter().filter(move |m| m.envelope.thread_id == *thread)
    }

    /// Whether `envelope` is the human's own, or one its assistant sent for it.
    fn by_human(&self, envelope: &Envelope) -> bool {
        envelope.from == self.human || envelope.on_behalf_of.as_ref() == Some(&self.human)
    }

    /// The questions to the human not answered yet, by it or for it.
    fn open_questions(&self) -> Vec<&HumanMessage> {
        let asked: Vec<&HumanMessage> =
            self.stored.iter().filter(|m| m.envelope.to == self.human && m.envelope.asks().is_some()).collect();
        let answered: HashSet<Uuid> = self
            .stored
            .iter()
            .filter(|a| self.by_human(&a.envelope))
            .flat_map(|a| asked.iter().filter(|q| a.envelope.answers_as(&q.envelope, &self.human).is_some()))
            .map(|q| q.envelope.id)
            .collect();
        asked.into_iter().filter(|q| !answered.contains(&q.envelope.id)).collect()
    }
}

impl Hub {
    /// The human the ant acts for now: the one its credentials make it the assistant of, while that human's grant
    /// names the ant.
    pub(super) fn assisting_now(&self) -> Option<UserId> {
        let human = self.assisting.get()?;
        (self.humans.assistant(human).as_ref() == Some(&self.ant)).then(|| human.clone())
    }

    /// [`Hub::assisting_now`], and the store to act through, else why the ant may not act for its human.
    fn acting_for(&self) -> Result<(UserId, &dyn HumanStore), String> {
        let Some(human) = self.assisting.get() else {
            return Err(format!("{} is no human's assistant", self.ant));
        };
        if self.assisting_now().is_none() {
            return Err(format!("the grant of {human} no longer names {}: nothing is done for {human}", self.ant));
        }
        let store = self.human_store.get().ok_or("acting for a human is not available here")?;
        Ok((human.clone(), store.as_ref()))
    }

    async fn human_messages(&self, human: &UserId, store: &dyn HumanStore) -> Result<Messages, String> {
        let since = SystemTime::now() - LOOKBACK;
        let stored = store
            .messages(human.clone(), self.ant.clone(), since)
            .await
            .map_err(|e| format!("cannot read the messages of {human}: {e:#}"))?;
        Ok(Messages { human: human.clone(), stored })
    }

    /// Tell `human` in its inbox of an act the ant did for it that no recipient's daemon reports, in a
    /// [`crate::people::behalf::report`]. Failing to, the result says so.
    async fn tell(&self, human: &UserId, act: String) -> String {
        let text = format!("[ants] {}, as the assistant of {human}, {act}", self.ant);
        let notice = crate::people::behalf::report(self.ant.clone(), human, text);
        match self.send(&notice).await {
            Ok(()) => format!(" {human} is told in its inbox."),
            Err(e) => format!(" Telling {human} failed: {e}."),
        }
    }
}

impl Tools {
    /// The tools acting for the human the ant assists.
    pub(super) fn assist_router() -> ToolRouter<Self> {
        Self::assist_home_router()
            + Self::assist_threads_router()
            + Self::assist_ants_router()
            + Self::assist_channels_router()
    }
}
