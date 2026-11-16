//! The tools an assistant ant acts for its human with ([`ants_proto::AssistantGrant`]): `human_queue` reads what
//! waits in the human's home in ants chat, or one of its threads; `answer_for_human`, `send_for_human`,
//! `post_for_human`, `notes_for_human`, `react_for_human`, `title_for_human` and `done_for_human` answer questions,
//! send messages, post in channels and thread notes, react and title or mark threads done for it; `mark_for_human`
//! and `snooze_for_human` pin, archive and snooze its threads under keys naming the ant
//! ([`ants_proto::assisted_marks_key`], [`ants_proto::assisted_snoozed_key`]), never changing the human's own marks
//! and snoozes, which its devices take in only while the grant names the ant and only on threads the human marked or
//! snoozed none of itself; `decide_for_human`
//! decides a tool request waiting for it, held by the requesting ant's daemon until the human confirms it or a
//! pre-allow rule of the human covers it ([`crate::approvals`]); `order_for_human` sends an admin order for it, held
//! likewise by the daemon of the ant's machine ([`crate::admin::held`]); `queue_for_human`, `cancel_for_human` and
//! `withdraw_for_human` list an ant's queue, cancel its prompt on a thread and withdraw a message waiting for it,
//! asking the ant's daemons on subjects naming the ant ([`ants_proto::CancelRequest::for_human`]); while none
//! answers, `withdraw_for_human` records the withdrawal of a message of the human's under a key naming the ant instead
//! ([`crate::attempts::record_withdrawal`]), which the ant's daemon honours before taking the message up.
//!
//! The tools are offered only while the human's grant names the ant ([`Hub::acting_for`]), and each call checks it
//! again, so a withdrawal ends them at once. Everything they publish is the ant's own, carrying `on_behalf_of` naming
//! the human, which readers verify against the grant. The human hears of each act in its inbox: from the recipient's
//! daemon for a message to an ant, a decision, a cancel or a withdrawal ([`crate::behalf`]), else from the ant itself,
//! as for a post, which every daemon sees. Before a message to an ant, the ant records the human as the thread's owner
//! for that ant ([`crate::owners::record_for_human`]). Sealed messages open,
//! and messages to humans are sealed, only while the human enrolled the ant as one of its devices
//! ([`crate::sealing::AssistantSealing`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use ants_proto::{
    ATTEMPTS_BUCKET, Act, AdminOp, AdminOrder, ApprovalDecision, ApprovalRequest, CancelReply, CancelRequest,
    ChannelId, ChannelPost, Choice, Envelope, JsDomain, Machine, MessageMark, NotesPost, QueueReply, QueueRequest,
    QueuedMessage, READ_BUCKET, Reaction, ReadMarker, THREADS_BUCKET, ThreadDone, ThreadId, ThreadMarks, ThreadSnoozes,
    ThreadTitle, UserId, Uuid, Verdict, assisted_marks_key, assisted_snoozed_key, done_key, marks_key, snoozed_key,
    subject, title_key,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{Hub, POST_TRIES, Tools, text};
use crate::attempts::{self, Recorded};
use crate::direct_kv::DirectKv;
use crate::reading::{self, Reading};
use crate::sealing::{AssistantSealing, Opening};
use crate::{behalf, owners, reactions};

/// How far back the human's messages and the tool requests waiting for it are read.
const LOOKBACK: Duration = Duration::from_hours(7 * 24);
/// The most entries of each kind `human_queue` lists, the latest ones.
const SHOWN: usize = 30;
/// How long the cancel and queue tools wait for a daemon of the ant to answer.
const ANSWER_WAIT: Duration = Duration::from_secs(5);
/// How long they wait on after a first answer, for another daemon of the same ant.
const ANSWER_GRACE: Duration = Duration::from_millis(500);
/// How many times a change of the human's marks or snoozes is tried again when its devices changed them meanwhile.
const CONFLICTS: usize = 16;
/// How long `snooze_for_human` snoozes a thread for by default.
const SNOOZE: Duration = Duration::from_hours(1);

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
    /// The answers to `request`, as [`HumanStore::cancel`] gathers them.
    fn queue(&self, request: QueueRequest) -> BoxFuture<'_, Result<Vec<QueueReply>>>;
    /// Record the withdrawal of `message` from the queue of `ant` by `assistant` for `human`, while no daemon of the
    /// ant answers ([`attempts::record_withdrawal`]).
    fn record_withdrawal(
        &self,
        ant: UserId,
        message: Uuid,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<Recorded>>;
    /// Sign `order` with the ant's credentials and send it to the hub's stream, once stored: the order as sent.
    fn send_order(&self, order: AdminOrder) -> BoxFuture<'_, Result<AdminOrder>>;
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

/// The [`HumanStore`] on the hub's JetStream, read as a human reads it, by direct gets; `js_domain` is the domain `js`
/// was made for, and `creds` the ant's credentials, which sign the orders it sends.
pub fn jetstream_store(js: jetstream::Context, js_domain: Option<JsDomain>, creds: PathBuf) -> Arc<dyn HumanStore> {
    Arc::new(JetStreamStore { js, js_domain, creds })
}

struct JetStreamStore {
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    creds: PathBuf,
}

impl JetStreamStore {
    async fn read(&self, filter: &str, since: SystemTime, mut take: impl FnMut(u64, &str, &[u8])) -> Result<()> {
        let start = consumer::DeliverPolicy::ByStartTime { start_time: since.into() };
        reading::read_back(&self.js, "", Reading::Gets, filter, start, |seq, _, subject, payload| {
            take(seq, subject, payload);
            ControlFlow::Continue(())
        })
        .await
    }

    async fn threads(&self) -> Result<DirectKv> {
        DirectKv::new(&self.js, self.js_domain.as_ref(), THREADS_BUCKET).await
    }

    /// The entry under `key` of [`THREADS_BUCKET`], and its stream sequence (0 for none).
    async fn entry<T: Default + DeserializeOwned>(&self, threads: &DirectKv, key: &str) -> Result<(T, u64)> {
        Ok(match threads.lookup_last(key).await? {
            (crate::direct_kv::Lookup::NoBucket, _) => anyhow::bail!("there is no bucket {THREADS_BUCKET}"),
            (crate::direct_kv::Lookup::NoValue, last) => (T::default(), last),
            (crate::direct_kv::Lookup::Value(value), last) => {
                (serde_json::from_slice(&value).unwrap_or_default(), last)
            }
        })
    }

    /// Change the entry under `key` of [`THREADS_BUCKET`] with `change`, by compare-and-set.
    async fn change<T: Default + Serialize + DeserializeOwned>(&self, key: String, change: Change<T>) -> Result<bool> {
        let threads = self.threads().await?;
        for _ in 0..CONFLICTS {
            let (mut value, last) = self.entry::<T>(&threads, &key).await?;
            if !change(&mut value) {
                return Ok(false);
            }
            if threads.update(&key, serde_json::to_vec(&value)?, last).await? {
                return Ok(true);
            }
        }
        anyhow::bail!("storing {key}: it kept changing")
    }

    /// Publish `payload` on `subject` with core NATS, and gather the answers on `reply` that `keep` takes.
    async fn ask<T: DeserializeOwned>(
        &self,
        subject: String,
        reply: String,
        payload: Vec<u8>,
        keep: impl Fn(&T, &str) -> bool,
    ) -> Result<Vec<T>> {
        let client = self.js.client();
        let mut answers = client.subscribe(reply).await?;
        client.publish(subject, payload.into()).await?;
        client.flush().await?;
        let mut deadline = tokio::time::Instant::now() + ANSWER_WAIT;
        let mut found = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, answers.next()).await {
            let Ok(answer) = serde_json::from_slice::<T>(&msg.payload) else { continue };
            if !keep(&answer, &msg.subject) {
                continue;
            }
            if found.is_empty() {
                deadline = deadline.min(tokio::time::Instant::now() + ANSWER_GRACE);
            }
            found.push(answer);
        }
        Ok(found)
    }
}

impl HumanStore for JetStreamStore {
    fn messages(
        &self,
        human: UserId,
        assistant: UserId,
        since: SystemTime,
    ) -> BoxFuture<'_, Result<Vec<HumanMessage>>> {
        Box::pin(async move {
            let mut found = BTreeMap::new();
            for filter in [subject::inbox_of(&human), subject::inbox_from(&human), subject::inbox_from(&assistant)] {
                self.read(&filter, since, |seq, subject, payload| {
                    let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) else { return };
                    let for_human = envelope.from != assistant || envelope.on_behalf_of.as_ref() == Some(&human);
                    if for_human && envelope.check_subject(subject).is_ok() {
                        found.insert(seq, envelope);
                    }
                })
                .await
                .with_context(|| format!("reading {filter}"))?;
            }
            Ok(found.into_iter().map(|(seq, envelope)| HumanMessage { seq, envelope }).collect())
        })
    }

    fn approvals(&self, since: SystemTime) -> BoxFuture<'_, Result<(Vec<ApprovalRequest>, HashSet<Uuid>)>> {
        Box::pin(async move {
            let (mut requests, mut decided) = (Vec::new(), HashSet::new());
            self.read("ants.approvals.>", since, |_, subject, payload| {
                if let Ok(request) = serde_json::from_slice::<ApprovalRequest>(payload)
                    && request.check_subject(subject).is_ok()
                {
                    requests.push(request);
                } else if let Ok(decision) = serde_json::from_slice::<ApprovalDecision>(payload)
                    && subject == decision.subject()
                {
                    decided.insert(decision.request_id);
                }
            })
            .await
            .context("reading the tool requests")?;
            Ok((requests, decided))
        })
    }

    fn read_marker(&self, human: UserId) -> BoxFuture<'_, Result<ReadMarker>> {
        Box::pin(async move {
            let read = DirectKv::new(&self.js, self.js_domain.as_ref(), READ_BUCKET).await?;
            match read.lookup(human.as_str()).await? {
                crate::direct_kv::Lookup::Value(value) => {
                    serde_json::from_slice(&value).with_context(|| format!("reading the read marker of {human}"))
                }
                _ => Ok(ReadMarker::default()),
            }
        })
    }

    fn done(&self, thread: ThreadId) -> BoxFuture<'_, Result<Option<ThreadDone>>> {
        Box::pin(async move {
            match self.threads().await?.lookup(done_key(&thread)).await? {
                crate::direct_kv::Lookup::Value(value) => Ok(serde_json::from_slice(&value).ok()),
                _ => Ok(None),
            }
        })
    }

    fn last_sequence(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(reading::last_sequence(&self.js, "", Reading::Gets, ""))
    }

    fn put_title(&self, title: ThreadTitle) -> BoxFuture<'_, Result<()>> {
        Box::pin(
            async move { self.threads().await?.put(title_key(&title.thread_id), serde_json::to_vec(&title)?).await },
        )
    }

    fn put_done(&self, done: ThreadDone) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { self.threads().await?.put(done_key(&done.thread_id), serde_json::to_vec(&done)?).await })
    }

    fn marks(&self, human: UserId) -> BoxFuture<'_, Result<ThreadMarks>> {
        Box::pin(async move { Ok(self.entry(&self.threads().await?, &marks_key(&human)).await?.0) })
    }

    fn snoozes(&self, human: UserId) -> BoxFuture<'_, Result<ThreadSnoozes>> {
        Box::pin(async move { Ok(self.entry(&self.threads().await?, &snoozed_key(&human)).await?.0) })
    }

    fn change_marks(
        &self,
        human: UserId,
        assistant: UserId,
        change: Change<ThreadMarks>,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(self.change(assisted_marks_key(&human, &assistant), change))
    }

    fn change_snoozes(
        &self,
        human: UserId,
        assistant: UserId,
        change: Change<ThreadSnoozes>,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(self.change(assisted_snoozed_key(&human, &assistant), change))
    }

    fn react(
        &self,
        by: UserId,
        message: Uuid,
        reaction: Reaction,
        remove: bool,
        human: UserId,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let domain = self.js_domain.as_ref();
            if remove {
                reactions::remove(&self.js, domain, &by, &message, &reaction).await
            } else {
                reactions::add_for(&self.js, domain, &by, &message, &reaction, Some(human)).await
            }
        })
    }

    fn record_owner(
        &self,
        thread: ThreadId,
        to: UserId,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<()>> {
        let domain = self.js_domain.as_ref();
        Box::pin(async move { owners::record_for_human(&self.js, domain, &thread, &to, &human, &assistant).await })
    }

    fn cancel(&self, request: CancelRequest) -> BoxFuture<'_, Result<Vec<CancelReply>>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&request)?;
            let keep = |a: &CancelReply, at: &str| a.request_id == request.id && a.check_subject(at).is_ok();
            self.ask(request.subject(), request.reply_subject(), payload, keep).await
        })
    }

    fn queue(&self, request: QueueRequest) -> BoxFuture<'_, Result<Vec<QueueReply>>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&request)?;
            let keep = |a: &QueueReply, at: &str| a.request_id == request.id && a.check_subject(at).is_ok();
            self.ask(request.subject(), request.reply_subject(), payload, keep).await
        })
    }

    fn record_withdrawal(
        &self,
        ant: UserId,
        message: Uuid,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<Recorded>> {
        Box::pin(async move {
            let kv = DirectKv::new(&self.js, self.js_domain.as_ref(), ATTEMPTS_BUCKET).await?;
            attempts::record_withdrawal(&kv, &ant, &message, &assistant, &human).await
        })
    }

    fn send_order(&self, mut order: AdminOrder) -> BoxFuture<'_, Result<AdminOrder>> {
        Box::pin(async move {
            let (jwt, key) = crate::creds::load(&self.creds, crate::keystore::os())?;
            order.credentials = jwt;
            order.signature = data_encoding::BASE64URL_NOPAD.encode(&key.sign(&order.signed())?);
            let message = jetstream::message::PublishMessage::build()
                .payload(serde_json::to_vec(&order)?.into())
                .message_id(order.id.to_string());
            let subject = order.subject();
            let stored = async { self.js.send_publish(subject.clone(), message).await?.await };
            stored.await.with_context(|| format!("sending the admin order on {subject}"))?;
            Ok(order)
        })
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

    /// The tool requests waiting for `human`, or its stand-in, to decide, latest last.
    async fn waiting_requests(&self, human: &UserId, store: &dyn HumanStore) -> Result<Vec<ApprovalRequest>, String> {
        let since = SystemTime::now() - LOOKBACK;
        let (requests, decided) =
            store.approvals(since).await.map_err(|e| format!("cannot read the tool requests: {e:#}"))?;
        let now = chrono::Utc::now();
        let default = self.humans.default_human();
        Ok(requests
            .into_iter()
            .filter(|r| r.human.as_ref().unwrap_or(default) == human || r.stand_in.as_ref() == Some(human))
            .filter(|r| !r.is_expired(now) && !decided.contains(&r.id))
            .collect())
    }

    /// The text of `envelope` as the human's devices show it: a sealed one opened while the human enrolled the ant
    /// as one of its devices, else `[sealed]`, or `[forged]` when it does not open as from its sender.
    async fn shown_text(&self, human: &UserId, envelope: &Envelope) -> String {
        if envelope.sealed.is_none() {
            return super::shown(envelope);
        }
        let Some(sealing) = self.sealing.get() else { return "[sealed]".to_owned() };
        match sealing.open(human, envelope).await {
            Ok(Opening::Opened(body)) => body.text,
            Ok(Opening::Unreadable) => "[sealed]".to_owned(),
            Ok(Opening::Forged(_)) => "[forged]".to_owned(),
            Err(e) => format!("[sealed: {e:#}]"),
        }
    }

    /// Who sent `envelope`: `bob via pb` for a message an assistant sent for its human under a grant standing.
    fn sender(&self, envelope: &Envelope) -> String {
        match &envelope.on_behalf_of {
            Some(human) if self.humans.assistant(human).as_ref() == Some(&envelope.from) => {
                format!("{human} via {}", envelope.from)
            }
            Some(human) => format!("{} (a forged claim to act for {human})", envelope.from),
            None => envelope.from.to_string(),
        }
    }

    /// One line on `m`, with its text cut to its first line unless `whole`.
    async fn line(&self, human: &UserId, m: &HumanMessage, whole: bool) -> String {
        let e = &m.envelope;
        let shown = self.shown_text(human, e).await;
        let shown = if whole { shown } else { behalf::excerpt(&shown) };
        let mut line = format!("- message {} on thread {}, {} to {}: {shown}", e.id, e.thread_id, self.sender(e), e.to);
        if let Some(q) = e.asks() {
            let options: Vec<String> = q.options.iter().enumerate().map(|(n, o)| format!("{}. {o}", n + 1)).collect();
            let typed = if q.free_text { "; a typed answer is taken" } else { "" };
            let _ = write!(line, " [options: {}{typed}]", options.join(", "));
        }
        line
    }

    /// What waits for the human in its home, or the messages of `thread`.
    async fn human_queue(&self, thread: Option<&str>) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        if let Some(thread) = thread.map(str::trim).filter(|t| !t.is_empty()) {
            let thread: ThreadId = thread.parse().map_err(|e| format!("{e}"))?;
            let on: Vec<&HumanMessage> = messages.on(&thread).collect();
            if on.is_empty() {
                return Err(format!("{human} has no message on thread {thread} in the last 7 days"));
            }
            let mut out = format!("Thread {thread} of {human}, oldest first:");
            for m in on.iter().skip(on.len().saturating_sub(SHOWN)) {
                let _ = write!(out, "\n{}", self.line(&human, m, true).await);
            }
            return Ok(out);
        }

        let marker = store.read_marker(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        let questions = messages.open_questions();
        let asked: HashSet<Uuid> = questions.iter().map(|q| q.envelope.id).collect();
        let unread: Vec<&HumanMessage> = messages
            .stored
            .iter()
            .filter(|m| m.envelope.to == human && m.envelope.from != self.ant && !asked.contains(&m.envelope.id))
            .filter(|m| !marker.is_read(m.seq))
            .collect();
        let mut last: HashMap<&ThreadId, u64> = HashMap::new();
        for m in &messages.stored {
            last.insert(&m.envelope.thread_id, m.seq);
        }
        let mut done = HashSet::new();
        for thread in questions.iter().chain(&unread).map(|m| &m.envelope.thread_id).collect::<HashSet<_>>() {
            let mark = store.done(thread.clone()).await.map_err(|e| format!("{e:#}"))?;
            if mark.is_some_and(|d| d.holds(last[thread])) {
                done.insert(thread.clone());
            }
        }
        let open = |m: &&HumanMessage| !done.contains(&m.envelope.thread_id);
        let questions: Vec<&HumanMessage> = questions.into_iter().filter(open).collect();
        let unread: Vec<&HumanMessage> = unread.into_iter().filter(open).collect();
        let requests = self.waiting_requests(&human, store).await?;
        let held: HashSet<Uuid> = messages
            .stored
            .iter()
            .filter(|m| m.envelope.from == self.ant)
            .filter_map(|m| m.envelope.approval.as_ref().map(|d| d.request_id))
            .collect();

        if questions.is_empty() && unread.is_empty() && requests.is_empty() {
            return Ok(format!("Nothing waits for {human}."));
        }
        let mut out = format!("What waits for {human}, latest last:");
        let _ = write!(out, "\nQuestions to {human} ({}), answered with answer_for_human:", questions.len());
        for m in questions.iter().skip(questions.len().saturating_sub(SHOWN)) {
            let _ = write!(out, "\n{}", self.line(&human, m, false).await);
        }
        let _ = write!(out, "\nTool requests ({}), decided with decide_for_human:", requests.len());
        for r in requests.iter().skip(requests.len().saturating_sub(SHOWN)) {
            let options: Vec<&str> = r.options.iter().map(|v| v.as_str()).collect();
            let held = if held.contains(&r.id) { "; decided by this ant, held until confirmed" } else { "" };
            let _ = write!(
                out,
                "\n- {}/{} on thread {}: {} {} [options: {}; expires {}{held}]",
                r.ant,
                r.id,
                r.thread_id,
                r.tool.kind,
                behalf::excerpt(&r.tool.title),
                options.join(", "),
                r.expires_at.format("%Y-%m-%d %H:%M UTC"),
            );
        }
        let _ = write!(out, "\nUnread messages to {human} ({}):", unread.len());
        for m in unread.iter().skip(unread.len().saturating_sub(SHOWN)) {
            let _ = write!(out, "\n{}", self.line(&human, m, false).await);
        }
        Ok(out)
    }

    /// Tell `human` in its inbox of an act the ant did for it that no recipient's daemon reports, on a thread of its
    /// own as [`behalf::sent_notice`] is. Failing to, the result says so.
    async fn tell(&self, human: &UserId, act: String) -> String {
        let text = format!("[ants] {}, as the assistant of {human}, {act}", self.ant);
        let notice = Envelope::new(self.ant.clone(), human.clone(), text).with_owner(Some(human.clone()));
        match self.send(&notice).await {
            Ok(()) => format!(" {human} is told in its inbox."),
            Err(e) => format!(" Telling {human} failed: {e}."),
        }
    }

    async fn answer_for_human(&self, args: AnswerForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let id: Uuid = args.message_id.trim().parse().map_err(|e| format!("message_id: {e}"))?;
        let messages = self.human_messages(&human, store).await?;
        let question =
            messages.open_questions().into_iter().find(|q| q.envelope.id == id).ok_or_else(|| {
                format!("no question to {human} waits as message {id}; human_queue lists those that do")
            })?;
        let question = &question.envelope;
        let answer = Envelope::answering(question, self.ant.clone(), Choice::parse(&args.answer))
            .map_err(|e| format!("{e}"))?
            .on_behalf(Some(human.clone()))
            .with_owner(question.owner.clone());
        self.send(&answer).await?;
        Ok(format!(
            "Answered {}'s question {id} for {human} on thread {} with {:?}; its daemon tells {human}.",
            question.from, question.thread_id, answer.text
        ))
    }

    async fn send_for_human(&self, args: SendForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let to: UserId = args.to.trim().trim_start_matches('@').parse().map_err(|e| format!("{e}"))?;
        if to == human || to == self.ant {
            return Err(format!("{to} is {human} or this ant; send to someone else"));
        }
        let mut envelope = match args.thread.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            Some(thread) => {
                let thread = thread.parse().map_err(|e| format!("{e}"))?;
                Envelope::on_thread(thread, self.ant.clone(), to.clone(), args.text)
            }
            None => Envelope::new(self.ant.clone(), to.clone(), args.text),
        }
        .on_behalf(Some(human.clone()))
        .with_owner(Some(human.clone()));
        let excerpt = behalf::excerpt(&envelope.text);
        if !self.humans.is_human(&to) {
            // Before the message, so the ant's own messages on the thread name the human as their owner.
            let thread = envelope.thread_id.clone();
            if let Err(e) = store.record_owner(thread.clone(), to.clone(), human.clone(), self.ant.clone()).await {
                tracing::warn!(%thread, %to, error = format!("{e:#}"), "recording the thread's owner for the human");
            }
            self.send(&envelope).await?;
            return Ok(format!(
                "Sent {to} message {} on thread {} for {human}; its daemon tells {human}.",
                envelope.id, envelope.thread_id
            ));
        }
        let sealed = !args.plaintext;
        if sealed {
            let sealing = self.sealing.get().ok_or_else(|| {
                format!("{human} has not enrolled {} as one of its devices, so nothing is sealed; nothing sent: set plaintext to send it unsealed", self.ant)
            })?;
            sealing
                .seal(&human, &mut envelope)
                .await
                .map_err(|e| format!("{e:#}; nothing sent: set plaintext to send it unsealed"))?;
        }
        self.send(&envelope).await?;
        let what = if sealed { "a sealed message".to_owned() } else { format!("a message: {excerpt}") };
        let told = self.tell(&human, format!("sent {to} {what} for {human} on thread {}.", envelope.thread_id)).await;
        Ok(format!("Sent {to} message {} on thread {} for {human}.{told}", envelope.id, envelope.thread_id))
    }

    /// Post in a channel the human is a member of, starting a thread or on `thread`, as the human posts there: with a
    /// fresh turn budget.
    async fn post_for_human(&self, args: PostForHuman) -> Result<String, String> {
        let (human, _) = self.acting_for()?;
        let channel: ChannelId = args.channel.trim().trim_start_matches('#').parse().map_err(|e| format!("{e}"))?;
        let access = self.channels.get().ok_or("channels are not served here; post_for_human is unavailable")?;
        let entry = (access.entry)(channel.clone())
            .await
            .map_err(|e| format!("cannot read #{channel}: {e:#}"))?
            .ok_or_else(|| format!("there is no channel #{channel}"))?;
        if !entry.is_member(&human) {
            return Err(format!("{human} is no member of #{channel}; nothing posted"));
        }
        let post = match args.thread.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
            Some(thread) => {
                let thread: ThreadId = thread.parse().map_err(|e| format!("{e}"))?;
                let root = thread.as_str().parse::<Uuid>().ok();
                let mut post = ChannelPost::on_thread(channel.clone(), thread, self.ant.clone(), args.text);
                post.in_reply_to = root;
                post
            }
            None => ChannelPost::new(channel.clone(), self.ant.clone(), args.text),
        }
        .with_owner(Some(human.clone()))
        .on_behalf(Some(human.clone()));
        let excerpt = behalf::excerpt(&post.text);
        (access.post)(post.clone()).await.map_err(|e| format!("not posted: {e:#}"))?;
        let thread = &post.thread_id;
        let told = self.tell(&human, format!("posted to #{channel} on thread {thread} for {human}: {excerpt}")).await;
        Ok(format!("Posted to #{channel} on thread {thread} for {human}.{told}"))
    }

    /// Post to the notes of `thread` as the human posts there: with a fresh turn budget.
    async fn notes_for_human(&self, args: NotesForHuman) -> Result<String, String> {
        let (human, _) = self.acting_for()?;
        let thread: ThreadId = args.thread.trim().parse().map_err(|e| format!("{e}"))?;
        let post = NotesPost::new(thread.clone(), self.ant.clone(), args.text).on_behalf(Some(human.clone()));
        let mut posted = false;
        for _ in 0..POST_TRIES {
            let notes = self.notes.read(&thread).await.map_err(|e| format!("cannot read the thread notes: {e:#}"))?;
            if self.notes.post(&post, notes.last_seq).await.map_err(|e| format!("not posted: {e:#}"))? {
                posted = true;
                break;
            }
        }
        if !posted {
            return Err(format!("the notes of thread {thread} kept changing; nothing posted"));
        }
        let excerpt = behalf::excerpt(&post.text);
        let told = self.tell(&human, format!("posted to the notes of thread {thread} for {human}: {excerpt}")).await;
        Ok(format!("Posted to the notes of thread {thread} for {human} ({} turns left).{told}", post.turn_budget))
    }

    async fn react_for_human(&self, args: ReactForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let reaction: Reaction = args.reaction.trim().parse().map_err(|e| format!("{e}"))?;
        let id: Uuid = args.message_id.trim().parse().map_err(|e| format!("message_id: {e}"))?;
        let messages = self.human_messages(&human, store).await?;
        let message = messages.find(id).ok_or_else(|| format!("{human} has no message {id} in the last 7 days"))?;
        let thread = message.envelope.thread_id.clone();
        let changed = store
            .react(self.ant.clone(), id, reaction.clone(), args.remove, human.clone())
            .await
            .map_err(|e| format!("{e:#}"))?;
        let (did, result) = match (args.remove, changed) {
            (true, false) => return Ok(format!("No reaction {reaction} to message {id} to remove.")),
            (false, false) => return Ok(format!("Already reacted {reaction} to message {id} for {human}.")),
            (true, true) => ("removed its reaction", format!("Removed the reaction {reaction} to message {id}.")),
            (false, true) => ("reacted", format!("Reacted {reaction} to message {id} for {human}.")),
        };
        let told = self.tell(&human, format!("{did} {reaction} to message {id} on thread {thread} for {human}.")).await;
        Ok(format!("{result}{told}"))
    }

    /// The thread `thread` names, if the human has a message on it.
    fn human_thread(&self, messages: &Messages, thread: &str) -> Result<ThreadId, String> {
        let thread: ThreadId = thread.trim().parse().map_err(|e| format!("{e}"))?;
        if messages.on(&thread).next().is_none() {
            return Err(format!("{} has no message on thread {thread} in the last 7 days", messages.human));
        }
        Ok(thread)
    }

    async fn title_for_human(&self, args: TitleForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let mut title = ThreadTitle::new(thread.clone(), &args.title, self.ant.clone())
            .map_err(|e| format!("{e}"))?
            .on_behalf(Some(human.clone()));
        let shown = title.title.clone();
        let sealed = messages.on(&thread).find(|m| m.envelope.sealed.is_some()).map(|m| &m.envelope);
        if let Some(sealed) = sealed {
            let ant = &self.ant;
            let other = [&sealed.from, &sealed.to].into_iter().find(|u| **u != human && *u != ant).cloned();
            let other = other.ok_or_else(|| format!("thread {thread} carries sealed messages with no other party"))?;
            let sealing = self.sealing.get().ok_or_else(|| {
                format!("thread {thread} carries sealed messages, and {human} has not enrolled {ant} as one of its devices, so the title is not sealed; no title is set")
            })?;
            sealing.seal_title(&human, &other, &mut title).await.map_err(|e| format!("{e:#}; no title is set"))?;
        }
        store.put_title(title).await.map_err(|e| format!("{e:#}"))?;
        let what = if sealed.is_some() { "a sealed title".to_owned() } else { format!("{shown:?}") };
        let told = self.tell(&human, format!("titled thread {thread} {what} for {human}.")).await;
        Ok(format!("Titled thread {thread} {shown:?} for {human}.{told}"))
    }

    async fn done_for_human(&self, args: DoneForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let last = store.last_sequence().await.map_err(|e| format!("{e:#}"))?;
        let done = ThreadDone::new(thread.clone(), self.ant.clone(), last).on_behalf(Some(human.clone()));
        store.put_done(done).await.map_err(|e| format!("{e:#}"))?;
        let told = self.tell(&human, format!("marked thread {thread} done for {human}.")).await;
        Ok(format!("Marked thread {thread} done for {human}; a new message on it brings it back.{told}"))
    }

    async fn mark_for_human(&self, args: MarkForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let own = store.marks(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        let marked = match own.mark(&thread, 0) {
            Some(ants_proto::Mark::Pinned) => Some("pinned"),
            Some(_) => Some("archived"),
            None => None,
        };
        if let Some(marked) = marked {
            return Err(format!(
                "{human} {marked} thread {thread} itself: only {human} changes its own marks; nothing changed"
            ));
        }
        let (change, (done, did), already): (Change<ThreadMarks>, _, _) = match args.mark.trim() {
            "pin" => {
                let t = thread.clone();
                (Box::new(move |m| m.pin(&t)), ("Pinned", "pinned"), "is pinned already")
            }
            "archive" => {
                let after = store.last_sequence().await.map_err(|e| format!("{e:#}"))?;
                let t = thread.clone();
                (Box::new(move |m| m.archive(&t, after)), ("Archived", "archived"), "is archived already")
            }
            "unpin" => {
                let t = thread.clone();
                (Box::new(move |m| m.unpin(&t)), ("Unpinned", "unpinned"), "is not pinned")
            }
            "unarchive" => {
                let t = thread.clone();
                (Box::new(move |m| m.unarchive(&t)), ("Unarchived", "unarchived"), "is not archived")
            }
            other => return Err(format!("mark: {other:?} is none of pin, unpin, archive and unarchive")),
        };
        let changed = store.change_marks(human.clone(), self.ant.clone(), change);
        if !changed.await.map_err(|e| format!("{e:#}"))? {
            return Ok(format!("Thread {thread} {already} for {human}; nothing changed."));
        }
        let told = self.tell(&human, format!("{did} thread {thread} for {human}.")).await;
        Ok(format!("{done} thread {thread} for {human}.{told}"))
    }

    async fn snooze_for_human(&self, args: SnoozeForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let messages = self.human_messages(&human, store).await?;
        let thread = self.human_thread(&messages, &args.thread)?;
        let now = chrono::Utc::now();
        let own = store.snoozes(human.clone()).await.map_err(|e| format!("{e:#}"))?;
        if let Some(snooze) = own.get(&thread, now) {
            let until = snooze.until.format("%Y-%m-%d %H:%M UTC");
            return Err(format!(
                "{human} snoozed thread {thread} itself until {until}: only {human} changes its own snoozes; nothing \
                 changed"
            ));
        }
        let t = thread.clone();
        if args.duration.as_deref().map(str::trim) == Some("off") {
            let woke = store.change_snoozes(human.clone(), self.ant.clone(), Box::new(move |s| s.wake(&t, now)));
            if !woke.await.map_err(|e| format!("{e:#}"))? {
                return Ok(format!("Thread {thread} is not snoozed for {human}; nothing changed."));
            }
            let told = self.tell(&human, format!("ended the snooze of thread {thread} for {human}.")).await;
            return Ok(format!("Ended the snooze of thread {thread} for {human}.{told}"));
        }
        let duration = match args.duration.as_deref().map(str::trim) {
            None | Some("") => SNOOZE,
            Some(text) => ants_proto::duration::parse(text).map_err(|e| format!("duration: {e}"))?,
        };
        let duration =
            chrono::TimeDelta::from_std(duration).unwrap_or(ants_proto::SNOOZE_MAX).min(ants_proto::SNOOZE_MAX);
        if duration <= chrono::TimeDelta::zero() {
            return Err("duration: a snooze lasts longer than 0".to_owned());
        }
        let until = now + duration;
        let snoozed = store.change_snoozes(
            human.clone(),
            self.ant.clone(),
            Box::new(move |s| {
                s.snooze(&t, now, until);
                true
            }),
        );
        snoozed.await.map_err(|e| format!("{e:#}"))?;
        let until = until.format("%Y-%m-%d %H:%M UTC");
        let told = self.tell(&human, format!("snoozed thread {thread} on the home of {human} until {until}.")).await;
        Ok(format!(
            "Snoozed thread {thread} on the home of {human} until {until}, or until something for {human} arrives on \
             it.{told}"
        ))
    }

    /// The ant `ant` names, whose queue the assistant reads or acts on for its human.
    fn queued_ant(&self, ant: &str) -> Result<UserId, String> {
        let ant: UserId = ant.trim().trim_start_matches('@').parse().map_err(|e| format!("{e}"))?;
        if self.humans.is_human(&ant) {
            return Err(format!("{ant} is a human; only an ant has a queue"));
        }
        Ok(ant)
    }

    /// Why no daemon of `ant` answered a request for `human`.
    fn unanswered(&self, ant: &UserId, human: &UserId) -> String {
        format!(
            "no daemon serving {ant} answered within {}s (one answers only while the grant of {human} names {})",
            ANSWER_WAIT.as_secs(),
            self.ant
        )
    }

    /// The messages the daemons of `ant` hold, oldest first, and how many more wait in the stream.
    async fn held(
        &self,
        human: &UserId,
        store: &dyn HumanStore,
        ant: &UserId,
    ) -> Result<(Vec<QueuedMessage>, u64), String> {
        let request = QueueRequest::new(ant.clone(), self.ant.clone()).for_human(human.clone());
        let answers = store.queue(request).await.map_err(|e| format!("{e:#}"))?;
        if answers.is_empty() {
            return Err(self.unanswered(ant, human));
        }
        let mut held: Vec<QueuedMessage> = answers.iter().flat_map(|a| a.messages.iter().cloned()).collect();
        held.sort_by_key(|m| m.seq);
        held.dedup_by_key(|m| m.seq);
        Ok((held, answers.iter().filter_map(|a| a.undelivered).max().unwrap_or(0)))
    }

    async fn queue_for_human(&self, args: QueueForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let ant = self.queued_ant(&args.ant)?;
        let (held, undelivered) = self.held(&human, store, &ant).await?;
        let mut out = if held.is_empty() {
            format!("The daemon of {ant} holds no message.")
        } else {
            format!("Messages the daemon of {ant} holds, oldest first:")
        };
        for m in &held {
            let state = if m.running { "running".to_owned() } else { format!("{} ahead on its thread", m.ahead) };
            let at = m.sent_at.format("%Y-%m-%d %H:%M UTC");
            let _ = write!(out, "\n- message {} on thread {} from {}, sent {at}: {state}", m.id, m.thread_id, m.from);
        }
        if undelivered > 0 {
            let _ = write!(out, "\n{undelivered} more wait in the stream, not taken up yet.");
        }
        Ok(out)
    }

    async fn cancel_for_human(&self, args: CancelForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let ant = self.queued_ant(&args.ant)?;
        let thread: ThreadId = args.thread.trim().parse().map_err(|e| format!("{e}"))?;
        let request = CancelRequest::new(ant.clone(), thread.clone(), self.ant.clone()).for_human(human.clone());
        let answers = store.cancel(request).await.map_err(|e| format!("{e:#}"))?;
        if answers.is_empty() {
            return Err(self.unanswered(&ant, &human));
        }
        if !answers.iter().any(|a| a.cancelled) {
            return Err(format!("{ant} has no prompt running on thread {thread}"));
        }
        Ok(format!(
            "Cancelled the prompt of {ant} on thread {thread} for {human}; the partial reply follows on the thread, and \
             the daemon of {ant} tells {human}."
        ))
    }

    async fn withdraw_for_human(&self, args: WithdrawForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let ant = self.queued_ant(&args.ant)?;
        let id: Uuid = args.message_id.trim().parse().map_err(|e| format!("message_id: {e}"))?;
        let held = match self.held(&human, store, &ant).await {
            Ok((held, _)) => held,
            Err(_) => return self.record_withdrawal(&human, store, &ant, id).await,
        };
        let thread = match held.iter().find(|m| m.id == id) {
            Some(m) if m.running => {
                let thread = &m.thread_id;
                return Err(format!(
                    "{ant} started on message {id} already; cancel_for_human stops its prompt on thread {thread}"
                ));
            }
            Some(m) => m.thread_id.clone(),
            None => {
                let messages = self.human_messages(&human, store).await?;
                let sent = messages.find(id).filter(|m| m.envelope.to == ant && messages.by_human(&m.envelope));
                sent.map(|m| m.envelope.thread_id.clone()).ok_or_else(|| {
                    format!(
                        "message {id} is neither held by the daemon of {ant} nor one of {human} to it; \
                         queue_for_human lists those held"
                    )
                })?
            }
        };
        let request =
            CancelRequest::new(ant.clone(), thread, self.ant.clone()).for_human(human.clone()).withdrawing(id);
        let answers = store.cancel(request).await.map_err(|e| format!("{e:#}"))?;
        if answers.iter().any(|a| a.cancelled) {
            return Ok(format!(
                "Withdrew message {id} from the queue of {ant} for {human}; its thread is told once its turn comes, \
                 and the daemon of {ant} tells {human}."
            ));
        }
        match answers.into_iter().find_map(|a| a.mark) {
            Some(MessageMark::Withdrawn { by, on_behalf_of, .. }) => {
                let by = on_behalf_of.map_or_else(|| by.to_string(), |h| format!("{by} for {h}"));
                Ok(format!("Message {id} was withdrawn already, by {by}."))
            }
            Some(MessageMark::Started { thread_id, .. }) => Err(format!(
                "{ant} started on message {id} already; cancel_for_human stops its prompt on thread {thread_id}"
            )),
            Some(MessageMark::Finished { .. }) => Err(format!("{ant} finished message {id} already")),
            None => self.record_withdrawal(&human, store, &ant, id).await,
        }
    }

    /// Withdraw message `id`, one of `human`'s to `ant`, while no daemon of the ant answers: by a record under a key
    /// naming the assistant, which the ant's daemon honours before taking the message up.
    async fn record_withdrawal(
        &self,
        human: &UserId,
        store: &dyn HumanStore,
        ant: &UserId,
        id: Uuid,
    ) -> Result<String, String> {
        let messages = self.human_messages(human, store).await?;
        let Some(sent) = messages.find(id).filter(|m| m.envelope.to == *ant && messages.by_human(&m.envelope)) else {
            return Err(format!(
                "{}; with none serving, only a message of {human} to {ant} is withdrawn",
                self.unanswered(ant, human)
            ));
        };
        let thread = sent.envelope.thread_id.clone();
        let recorded = store.record_withdrawal(ant.clone(), id, human.clone(), self.ant.clone()).await;
        match recorded.map_err(|e| format!("{e:#}"))? {
            Recorded::Holds => Ok(format!(
                "No daemon serving {ant} answered: recorded the withdrawal of message {id} on thread {thread} for \
                 {human}. A daemon of {ant} honours it before {ant} is prompted with the message, while the grant of \
                 {human} names {}, and then tells the thread and {human}.",
                self.ant
            )),
            Recorded::Marked(MessageMark::Withdrawn { by, on_behalf_of, .. }) => {
                let by = on_behalf_of.map_or_else(|| by.to_string(), |h| format!("{by} for {h}"));
                Ok(format!("Message {id} was withdrawn already, by {by}."))
            }
            Recorded::Marked(MessageMark::Started { .. }) => Err(format!(
                "{ant} started on message {id} already; cancel_for_human stops its prompt on thread {thread} once a \
                 daemon of {ant} serves"
            )),
            Recorded::Marked(MessageMark::Finished { .. }) => Err(format!("{ant} finished message {id} already")),
        }
    }

    async fn order_for_human(&self, args: OrderForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let op = match args.op.trim().replace('-', "_").as_str() {
            "pause" => AdminOp::Pause,
            "resume" => AdminOp::Resume,
            "restart" => AdminOp::Restart { now: false },
            "restart_now" => AdminOp::Restart { now: true },
            "read_settings" => AdminOp::ReadSettings,
            "read_memories" => AdminOp::ReadMemories,
            other => {
                return Err(format!(
                    "unknown op {other}: pause, resume, restart, restart_now, read_settings or read_memories"
                ));
            }
        };
        let ant: UserId = args.ant.trim().parse().map_err(|e| format!("ant: {e}"))?;
        let machine: Machine = args.machine.trim().parse().map_err(|e| format!("machine: {e}"))?;
        let (kind, what) = (op.kind(), op.describe());
        let order =
            AdminOrder::new(self.ant.clone(), machine.clone(), ant.clone(), op, "").on_behalf(Some(human.clone()));
        let order = store.send_order(order).await.map_err(|e| format!("{e:#}"))?;
        let act = Act::Order { machine: machine.clone(), ant: ant.clone(), id: order.id }.qualified();
        let told = self
            .tell(
                &human,
                format!(
                    "ordered {what} of {ant} on {machine} for {human}: the daemon of {machine} holds it until {human} \
                     confirms it (`ants assistant confirm {act}`, or `--deny`)."
                ),
            )
            .await;
        Ok(format!(
            "Sent order {act} ({what}) for {human}. The daemon of {machine} carries it out only once {human} confirms \
             it or a pre-allow rule of {human} (`{ant}/order:{kind}`) covers it; until then it is held. `ants ant \
             orders {ant}` shows where it stands.{told}"
        ))
    }

    async fn decide_for_human(&self, args: DecideForHuman) -> Result<String, String> {
        let (human, store) = self.acting_for()?;
        let verdict: Verdict = serde_json::from_value(serde_json::Value::String(args.verdict.trim().replace('-', "_")))
            .map_err(|_| format!("unknown verdict {}: allow_once, allow_always or reject", args.verdict))?;
        let named = args.request.trim();
        let (ant, id) = match named.split_once('/') {
            Some((ant, id)) => (Some(ant.parse::<UserId>().map_err(|e| format!("{e}"))?), id),
            None => (None, named),
        };
        let id: Uuid = id.parse().map_err(|e| format!("request: {e}"))?;
        let requests = self.waiting_requests(&human, store).await?;
        let request = requests
            .iter()
            .find(|r| r.id == id && ant.as_ref().is_none_or(|a| *a == r.ant))
            .ok_or_else(|| format!("no tool request {named} waits for {human}; human_queue lists those that do"))?;
        if verdict != Verdict::Reject && !request.options.contains(&verdict) {
            return Err(format!("{} is not offered for {}/{id}; reject always is", verdict.as_str(), request.ant));
        }
        let decision = request.decide(verdict).on_behalf(Some(human.clone()));
        let envelope =
            Envelope::decision(request.thread_id.clone(), self.ant.clone(), decision).with_owner(Some(human.clone()));
        self.send(&envelope).await?;
        let (ant, verdict) = (&request.ant, verdict.as_str());
        Ok(format!(
            "Sent {verdict} on tool request {ant}/{id} for {human}. It takes effect only once {human} confirms it (`ants \
             assistant confirm {ant}/{id}`) or a pre-allow rule of {human} covers the call; until then it is held, and \
             {ant}'s daemon tells {human}."
        ))
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct HumanQueue {
    /// A thread of the human's to read whole instead of the queue.
    #[serde(default)]
    thread: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct AnswerForHuman {
    /// The id of the question's message, as human_queue lists it.
    message_id: String,
    /// The number of the option picked, or a typed answer where the question takes one.
    answer: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct SendForHuman {
    /// The ant or human to send to.
    to: String,
    text: String,
    /// The thread to send on, such as one human_queue lists; omitted for a new thread.
    #[serde(default)]
    thread: Option<String>,
    /// Send a message to a human unsealed. Without it the message is sealed, which needs the human to have
    /// enrolled this ant as one of its devices.
    #[serde(default)]
    plaintext: bool,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct ReactForHuman {
    /// The id of a message to or from the human.
    message_id: String,
    /// The reaction: 1-64 of lowercase letters, digits and '-', such as `eyes`, `check` or `thumbsup`.
    reaction: String,
    /// Remove the reaction instead of adding it.
    #[serde(default)]
    remove: bool,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct TitleForHuman {
    /// A thread of the human's.
    thread: String,
    /// One line of up to 120 characters.
    title: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct PostForHuman {
    /// A channel the human is a member of, with or without its `#`.
    channel: String,
    text: String,
    /// The thread to post on; none starts a new one.
    #[serde(default)]
    thread: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct NotesForHuman {
    /// The thread whose notes to post to.
    thread: String,
    text: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct DoneForHuman {
    /// A thread of the human's.
    thread: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct MarkForHuman {
    /// A thread of the human's.
    thread: String,
    /// pin, unpin, archive or unarchive.
    mark: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct SnoozeForHuman {
    /// A thread of the human's.
    thread: String,
    /// How long: 30m, 2h, 1d, at most 7d (longer is cut to 7d); 1h by default; `off` ends the snooze.
    #[serde(default)]
    duration: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct DecideForHuman {
    /// The request, as human_queue lists it: `<ant>/<id>`, or its id alone.
    request: String,
    /// allow_once, allow_always or reject.
    verdict: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct QueueForHuman {
    /// The ant whose queue to list.
    ant: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct CancelForHuman {
    /// The ant running the prompt.
    ant: String,
    /// The thread it runs on.
    thread: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct WithdrawForHuman {
    /// The ant the message waits for.
    ant: String,
    /// The id of the message, as queue_for_human lists it, or of a message of the human's to the ant.
    message_id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct OrderForHuman {
    /// The ant to order.
    ant: String,
    /// The machine whose daemon serves the ant, as `ants machines` lists it.
    machine: String,
    /// pause, resume, restart (once the ant is idle), restart_now, read_settings or read_memories.
    op: String,
}

#[tool_router(router = assist_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "As the assistant of a human, read what waits for it in its home in ants chat: the \
        questions ants asked it not yet answered, the tool requests waiting for its decision, and its unread \
        messages, each with its message id and thread, from the last 7 days. With `thread`, read that thread's \
        messages to and from the human instead. A sealed message shows as [sealed] unless the human enrolled this \
        ant as one of its devices.")]
    async fn human_queue(&self, Parameters(args): Parameters<HumanQueue>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.human_queue(args.thread.as_deref()).await))
    }

    #[tool(description = "As the assistant of a human, answer a question an ant asked it (human_queue lists \
        them) for it: `answer` is the number of the option picked, or a typed answer where the question takes one. \
        The answer goes as this ant's, for the human, and the asking ant's daemon tells the human.")]
    async fn answer_for_human(
        &self,
        Parameters(args): Parameters<AnswerForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.answer_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, send a message for it to an ant or another human, on \
        `thread` or a new one, as this ant's, for the human; the human is told. A message to a human is sealed for \
        the recipient's and the human's devices, which needs the human to have enrolled this ant as one of its \
        devices; `plaintext` sends it unsealed. Attachments are not sent for a human.")]
    async fn send_for_human(&self, Parameters(args): Parameters<SendForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.send_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, post for it in a channel it is a member of, on `thread` or \
        starting a new one, as this ant's, for the human, with the fresh turn budget a human's post has; an @mention \
        wakes that member as the human's would. The human is told.")]
    async fn post_for_human(&self, Parameters(args): Parameters<PostForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.post_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, post for it to the notes of a thread, as this ant's, for the \
        human, with the fresh turn budget a human's post gives the notes. The human is told.")]
    async fn notes_for_human(&self, Parameters(args): Parameters<NotesForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.notes_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, add or remove a reaction for it to a message to or from \
        it, shown as the human's via this ant; the human is told.")]
    async fn react_for_human(&self, Parameters(args): Parameters<ReactForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.react_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, set the title of one of its threads for it; the human is \
        told. On a thread carrying sealed messages the title is sealed for the human's and the other party's devices, \
        which needs the human to have enrolled this ant as one of its devices; the notice never quotes it.")]
    async fn title_for_human(&self, Parameters(args): Parameters<TitleForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.title_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, mark one of its threads done for it, which takes it out \
        of the human's home until a new message comes on it; the human is told.")]
    async fn done_for_human(&self, Parameters(args): Parameters<DoneForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.done_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, pin, unpin, archive or unarchive one of its threads for it \
        (`mark`), for all its devices. The marks set so are this ant's, kept apart from the human's own, and count \
        only while the human's grant names this ant and only on threads the human neither pinned nor archived itself; \
        a thread the human marked itself is refused, and the human marking a thread itself takes over. The human is \
        told.")]
    async fn mark_for_human(&self, Parameters(args): Parameters<MarkForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.mark_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, snooze one of its threads off its home for `duration` (1h \
        by default, at most 7d), or until something for the human arrives on it; `off` ends a snooze. The snoozes set \
        so are this ant's, kept apart from the human's own, and count only while the human's grant names this ant; a \
        thread the human snoozed itself is refused, and the human snoozing or waking a thread itself takes over. The \
        human is told.")]
    async fn snooze_for_human(
        &self,
        Parameters(args): Parameters<SnoozeForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.snooze_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, decide a tool request waiting for it (human_queue lists \
        them): `verdict` is allow_once, allow_always or reject. The decision is held by the requesting ant's daemon \
        until the human confirms it or a pre-allow rule of the human covers the call; the human is told and may \
        still decide the request itself.")]
    async fn decide_for_human(
        &self,
        Parameters(args): Parameters<DecideForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.decide_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, list the messages the daemon of an ant holds for it to \
        answer, each with its message id, thread and sender, and how many more wait in the stream. The daemon answers \
        only while the human's grant names this ant.")]
    async fn queue_for_human(&self, Parameters(args): Parameters<QueueForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.queue_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, cancel the prompt an ant runs on a thread for it; the \
        partial reply follows on the thread, and the ant's daemon tells the human.")]
    async fn cancel_for_human(
        &self,
        Parameters(args): Parameters<CancelForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.cancel_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, withdraw a message waiting for an ant (queue_for_human \
        lists them) for it, so the ant is never prompted with it; its thread is told once its turn comes, and the \
        ant's daemon tells the human. While no daemon of the ant serves, only a message of the human's to the ant is \
        withdrawn, by a record the ant's daemon honours before taking the message up.")]
    async fn withdraw_for_human(
        &self,
        Parameters(args): Parameters<WithdrawForHuman>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.withdraw_for_human(args).await))
    }

    #[tool(description = "As the assistant of a human, send an admin order for it to an ant's machine: pause, \
        resume, restart, restart_now, read_settings or read_memories. The machine's daemon holds it until the human \
        confirms it or a pre-allow rule of the human covers it, then carries it out; the human is told.")]
    async fn order_for_human(&self, Parameters(args): Parameters<OrderForHuman>) -> Result<CallToolResult, ErrorData> {
        Ok(text(self.hub.order_for_human(args).await))
    }
}
