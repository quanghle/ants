//! Messages joining the prompt running on their thread, for a harness that takes them into its turn (`_ants/steer`):
//! the last message to join gets the prompt's answer, and those before it, the one the prompt started with first, a
//! notice of where the answer went. A joined turn runs under the tool rules of the prompt's first message, so a message
//! whose rules ([`Envelope::allow`], as its thread's narrowing set them) those are wider than joins nothing: it waits
//! for a prompt of its own under its rules, as a steer the harness refuses does.

use std::collections::HashMap;
use std::fmt;

use ants_proto::{AttachmentHandle, ChannelPost, NoReplyWhy, NoticeKind};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};

use crate::sync::MutexExt as _;

use ants_proto::{Envelope, ThreadId, UserId, Uuid};
use anyhow::Result;
use async_nats::jetstream::{self};

use crate::delivery::attempts::{Given, Reply};
use crate::files::Attached;
use crate::harness::Steer;

use super::prompt::Ran;
use super::{Handling, Inbox, Incoming};

/// The prompts running on threads whose harness takes steers, and where to send the messages joining each.
#[derive(Default)]
pub(super) struct Steers(std::sync::Mutex<HashMap<ThreadId, mpsc::UnboundedSender<Join>>>);

/// A message joining the prompt running on its thread.
pub(super) struct Join {
    pub(super) envelope: Envelope,
    /// The handles of the files attached to it, which the hub stored at `stored`.
    pub(super) carried: Vec<AttachmentHandle>,
    pub(super) stored: Option<DateTime<Utc>>,
    /// The channel post it was made of, with its stream sequence.
    pub(super) post: Option<(ChannelPost, u64)>,
    /// What the prompt is steered with.
    pub(super) text: String,
    /// What came of it; dropped when the prompt failed, the message then waiting its turn again.
    pub(super) joined: oneshot::Sender<Joined>,
}

/// What came of a message joining a running prompt.
pub(super) enum Joined {
    /// The prompt did not take it: it waits for a prompt of its own.
    Refused,
    /// The prompt's answer is its own, to send while the turn of the message the prompt started with lasts: until the
    /// sender drops.
    Answer(Box<Ran>, oneshot::Sender<()>),
    /// A later message joined the prompt too: the sender and id of the next one, which the answer went towards.
    Superseded(UserId, Uuid),
}

impl Steers {
    /// Take the messages joining the prompt about to run on `thread` until the guard drops; they arrive on the receiver.
    pub(super) fn register<'a>(&'a self, thread: &ThreadId) -> (Joinable<'a>, mpsc::UnboundedReceiver<Join>) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.locked().insert(thread.clone(), tx);
        (Joinable { steers: self, thread: thread.clone() }, rx)
    }

    /// Whether the prompt running on `thread` takes messages joining it.
    pub(super) fn takes(&self, thread: &ThreadId) -> bool {
        self.0.locked().get(thread).is_some_and(|tx| !tx.is_closed())
    }

    /// Send `join` to the prompt running on its thread: false when none takes it.
    pub(super) fn join(&self, join: Join) -> bool {
        let steers = self.0.locked();
        steers.get(&join.envelope.thread_id).is_some_and(|tx| tx.send(join).is_ok())
    }
}

pub(super) struct Joinable<'a> {
    steers: &'a Steers,
    thread: ThreadId,
}

impl Drop for Joinable<'_> {
    fn drop(&mut self) {
        self.steers.0.locked().remove(&self.thread);
    }
}

/// A message that steered its thread's prompt but did not join it: it waits for a prompt of its own, as it arrived.
#[derive(Debug)]
pub(super) struct Requeue;

impl fmt::Display for Requeue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the running prompt did not take the message; it waits its turn")
    }
}

impl std::error::Error for Requeue {}

impl Inbox<'_> {
    /// Join the prompt running on the thread of `incoming` with `text`: the prompt's answer to send, with the turn to
    /// hold while sending it, or None when `incoming` was settled as superseded ([`Inbox::superseded`]). A prompt that
    /// refused it or failed hands it back to wait ([`Requeue`]).
    pub(super) async fn joined(
        &self,
        h @ Handling { incoming, msg, seq }: Handling<'_>,
        own: bool,
        cut_off: bool,
        text: String,
    ) -> Result<Option<(Ran, tokio::sync::oneshot::Sender<()>)>> {
        let envelope = &incoming.envelope;
        match self.join(incoming, msg, seq, text).await {
            Some(Joined::Answer(ran, answering)) => Ok(Some((*ran, answering))),
            Some(Joined::Superseded(by, id)) => self.superseded(h, own, (&by, id)).await.map(|()| None),
            Some(Joined::Refused) => {
                if !cut_off {
                    self.marks.attempts.forget(seq).await;
                }
                self.marks.attempts.release(envelope).await;
                Err(Requeue.into())
            }
            // The prompt failed: the retry is warned it was cut off.
            None => {
                self.marks.attempts.retrying(envelope, None).await;
                Err(Requeue.into())
            }
        }
    }

    /// Join the prompt running on the thread of `incoming` with `text`: what came of it, None when the prompt failed.
    pub(super) async fn join(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        text: String,
    ) -> Option<Joined> {
        let envelope = &incoming.envelope;
        let stored = crate::files::stored(msg);
        let text = match Attached::new(self.js.clone(), incoming.attached(), stored) {
            Some(attached) => format!("{text}\n\n{}", attached.show(false).await.lines),
            None => text,
        };
        let carried = incoming.attached().into_iter().map(|(handle, _)| handle).collect();
        let (joined, outcome) = tokio::sync::oneshot::channel();
        let post = incoming.post.as_ref().map(|posted| (posted.post.clone(), seq));
        let join = Join { envelope: envelope.clone(), carried, stored, post, text, joined };
        if !self.running.steers.join(join) {
            return Some(Joined::Refused);
        }
        self.acking(msg, outcome).await.ok()
    }

    /// Hand what came of the prompt run for `incoming` to the messages that joined it ([`Ran::joined`]): its answer to
    /// the last, and to each before it, `incoming` first, the next one it went towards ([`Inbox::superseded`]). The
    /// turn of `incoming` lasts until the answer is sent. A failed prompt is settled as for `incoming` alone, the
    /// messages that joined it waiting again.
    pub(super) async fn hand_on(
        &self,
        h @ Handling { incoming, .. }: Handling<'_>,
        own: bool,
        mut ran: Ran,
        cut_off: bool,
    ) -> Result<()> {
        let mut joined = std::mem::take(&mut ran.joined);
        if ran.cancelled_by.is_none()
            && let Err(e) = ran.reply
        {
            drop(joined);
            self.note_failed(&incoming.envelope.thread_id, &ran.measured, &e, ran.worked).await;
            return self.prompt_failed(h, e, cut_off, ran.worked).await;
        }
        let spend = std::mem::replace(&mut ran.spend, tokio::sync::mpsc::unbounded_channel().1);
        self.record_ended(&incoming.envelope, spend, &mut ran.reached, &mut ran.measured).await;
        // The review request gets only a steering notice; record the actual answer before handing it on.
        if let Ok(answer) = &ran.reply
            && self.asks_review(incoming)
        {
            self.note_review(&incoming.envelope, &self.redactor.redact(&answer.text)).await;
        }
        let last = joined.pop().expect("a message joined");
        let mut by = (last.envelope.from.clone(), last.envelope.id);
        for join in joined.into_iter().rev() {
            let next = (join.envelope.from.clone(), join.envelope.id);
            let _ = join.joined.send(Joined::Superseded(by.0, by.1));
            by = next;
        }
        let (answering, answered) = tokio::sync::oneshot::channel();
        let _ = last.joined.send(Joined::Answer(Box::new(ran), answering));
        let superseded = self.superseded(h, own, (&by.0, by.1)).await;
        let _ = answered.await;
        superseded
    }

    /// Settle `incoming`, the message at stream sequence `seq`, whose prompt `by`'s message `id` steered and took the
    /// answer of. Its sender is told so, unless it is a post, another ant's reply to the ant's own message, or a
    /// message whose answer goes elsewhere than its sender, or the thread has no turns left: those end with no reply
    /// ([`NoReplyWhy::Steered`]). Either outcome is kept on the machine when it cannot be recorded or sent, so the retry
    /// settles the message the same way rather than prompting the harness with it again.
    pub(super) async fn superseded(
        &self,
        h @ Handling { incoming, seq, .. }: Handling<'_>,
        own: bool,
        (by, id): (&UserId, Uuid),
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        tracing::info!(thread = %envelope.thread_id, %by, steered_by = %id, "a later message took the answer");
        let tells = incoming.post.is_none()
            && incoming.resume.is_none()
            && !own
            && !envelope.webhook
            && envelope.from != *self.ant
            && envelope.escalation.is_none();
        let text = format!(
            "[ants] {by} steered the prompt answering this message with message {id}; the answer goes in reply to it."
        );
        let notice = tells.then(|| envelope.reply(self.ant.clone(), text, self.humans.sender_role(envelope)).ok());
        let Some(notice) = notice.flatten() else {
            let reason = format!("message {id} from {by} took the answer");
            let unanswered = Some((NoReplyWhy::Steered, Some(reason)));
            let given = Given { text: String::new(), limit: None, unanswered, tokens: 0, title: None, files: None };
            return Box::pin(self.send_given(h, None, given, false)).await;
        };
        let reply = Reply::Message(Box::new(notice.as_notice(NoticeKind::Steered)));
        self.record_reply(seq, envelope, &reply).await;
        self.deliver(h, &reply).await
    }

    /// Once the prompt ended: turn away the messages still to join it, and note what came of those handed to it.
    pub(super) async fn joins_ended(
        &self,
        joins: Option<(super::steering::Joinable<'_>, tokio::sync::mpsc::UnboundedReceiver<Join>)>,
        mut taking: futures::stream::FuturesUnordered<impl Future<Output = (bool, Join)>>,
        joined: &mut Vec<Join>,
    ) {
        if let Some((joinable, mut rx)) = joins {
            drop(joinable);
            rx.close();
            while let Ok(join) = rx.try_recv() {
                let _ = join.joined.send(Joined::Refused);
            }
        }
        while let Some((took, join)) = futures::StreamExt::next(&mut taking).await {
            self.took(took, join, joined);
        }
    }

    /// Note that the running prompt `took` the message of `join`, or turned it away.
    pub(super) fn took(&self, took: bool, join: Join, joined: &mut Vec<Join>) {
        let envelope = &join.envelope;
        tracing::info!(thread = %envelope.thread_id, from = %envelope.from, took, "steering the running prompt");
        if !took {
            let _ = join.joined.send(Joined::Refused);
            return;
        }
        if let Some(mcp) = self.mcp {
            mcp.steer_to(envelope, join.post.clone(), join.carried.clone(), join.stored);
        }
        joined.push(join);
    }
}

/// The line starting the prompt of a steering message that interrupted the prompt running on its thread.
pub(super) fn steer_note(by: &UserId) -> String {
    format!(
        "[ants] {by} interrupted the previous prompt on this thread with this message; tool calls it had in flight \
         were cancelled, so check what they left before repeating any."
    )
}

/// The line starting a steering message that joined the prompt running on its thread.
pub(super) fn join_note(by: &UserId) -> String {
    format!(
        "[ants] {by} sent this message while the prompt ran, to steer it. The final answer goes in reply to this \
         message, so it answers this message together with what came before it."
    )
}

/// Whether tool rules `running` (None: the ant's own) allow nothing beyond `own`: each of its rules is one of `own`.
fn within(running: Option<&[String]>, own: Option<&[String]>) -> bool {
    match (running, own) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(running), Some(own)) => running.iter().all(|rule| own.contains(rule)),
    }
}

/// Hand `join` to the prompt running for `running` through `steers`: what the harness made of it, or None when the
/// prompt takes no more or runs under tool rules wider than `join`'s, `join` then turned away.
pub(super) fn steer(
    steers: &tokio::sync::mpsc::Sender<Steer>,
    join: Join,
    running: &Envelope,
) -> Option<impl Future<Output = (bool, Join)> + use<>> {
    if !within(running.allow.as_deref(), join.envelope.allow.as_deref()) {
        let (thread, from) = (&join.envelope.thread_id, &join.envelope.from);
        tracing::info!(%thread, %from, "a message under narrower tool rules than the running prompt's waits for its own");
        let _ = join.joined.send(Joined::Refused);
        return None;
    }
    let (accepted, took) = tokio::sync::oneshot::channel();
    if steers.try_send(Steer { text: join.text.clone(), accepted }).is_err() {
        let _ = join.joined.send(Joined::Refused);
        return None;
    }
    Some(async move { (took.await.unwrap_or(false), join) })
}

/// The next message joining the running prompt, once it takes them.
pub(super) async fn next_join(
    joins: &mut Option<(super::steering::Joinable<'_>, tokio::sync::mpsc::UnboundedReceiver<Join>)>,
) -> Option<Join> {
    match joins {
        Some((_, rx)) => rx.recv().await,
        None => std::future::pending().await,
    }
}
