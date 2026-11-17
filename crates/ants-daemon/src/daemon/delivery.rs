//! Delivering the answer: the reply of the kind the message calls for, carrying the files attached to it, stored
//! before the message is acked; or none, for another ant's reply to the ant's own message and for a turn ended by
//! `complete`, whose answer is kept on the thread's timeline instead.

use ants_proto::{
    Attachment, AttachmentHandle, AttachmentScope, ChannelPost, Envelope, Error as ProtoError, EscalationReason,
    NoReplyWhy, NotesPost, Role, State, ThreadId, Timeline, TimelineEntry, UserId, Uuid,
};
use anyhow::Result;
use async_nats::jetstream;

use crate::attempts::Reply;
use crate::bus;
use crate::channel::{Context, Earlier};
use crate::escalation::{self, Route};
use crate::notes;
use crate::status;

use super::{Inbox, Incoming};

impl Inbox<'_> {
    /// Whether `incoming` is another ant's reply to a message or post the ant sent, which is prompted but not answered
    /// back, so that two ants answering each other stop after one reply: given what was read of a post's thread.
    /// Never a message carrying an escalation or an approval request, a webhook's, nor the resume of a channel thread
    /// after an escalation ([`Inbox::always_answered`]).
    pub(super) fn replies_to_own(&self, incoming: &Incoming, read: Option<&Context>) -> bool {
        if self.always_answered(incoming).is_some() {
            return false;
        }
        match (&incoming.post, read) {
            // A post's `in_reply_to` is its author's to set: it counts only when it names a post the ant made.
            (Some(posted), Some(read)) => posted.triage.is_none() && read.answers_own,
            (Some(_), None) => false,
            // `Envelope::reply` addresses the sender of the message it answers (only `Route::Stuck` redirects it, with
            // an escalation), and what an ant sends with send_message, ask_ant or ask_human answers nothing: an ant's
            // message answering one answers a message this ant sent.
            (None, _) => incoming.envelope.in_reply_to.is_some(),
        }
    }

    /// Why `incoming` is always answered, if it is: a human's message, or one an assistant sent for its human, a
    /// webhook's, one carrying an escalation or an approval request, and the resume of a channel thread after an
    /// escalation.
    pub(super) fn always_answered(&self, incoming: &Incoming) -> Option<&'static str> {
        let envelope = &incoming.envelope;
        if envelope.on_behalf_of.is_some() || self.humans.sender_role(envelope).is_human() {
            Some("a human's message")
        } else if envelope.webhook {
            Some("a webhook's message")
        } else if envelope.escalation.is_some() {
            Some("a message carrying an escalation")
        } else if envelope.approval.is_some() {
            Some("an approval request")
        } else if incoming.resume.is_some() {
            Some("the resume of a channel thread after an escalation")
        } else {
            None
        }
    }

    /// End the turn on `incoming`, the message at stream sequence `seq`, with no reply, for `why` and the reason
    /// `complete` gave: its final `text`, redacted, goes to the thread's timeline, then the `limit` the prompt reached
    /// escalates, the posts of a post's thread up to it are noted as shown, and the message is acked. The timeline
    /// record is best effort, but stored before the ack; nothing is noted in `answered`, so a redelivery before the
    /// ack prompts again, told it was cut off.
    pub(super) async fn end_unanswered(
        &self,
        incoming: &Incoming,
        (why, reason): (NoReplyWhy, Option<String>),
        text: String,
        limit: Option<&String>,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        tracing::info!(%thread, from = %envelope.from, ?why, "ending the turn with no reply");
        let entry = TimelineEntry::NoReply { why, reason, text };
        let record = Timeline::new(self.ant.clone(), thread.clone(), envelope.id, vec![entry], 0);
        crate::timeline::publish(self.js, &record).await;
        if let Some(limit) = limit {
            self.escalate(incoming, limit, EscalationReason::Cap, seq, None).await?;
        }
        if incoming.post.is_some()
            && let Err(e) = self.threads.seen(thread, seq).await
        {
            tracing::warn!(%thread, error = format!("{e:#}"), "noting the posts shown");
        }
        self.done(msg, seq).await?;
        status::state(self.client, self.ant, thread, State::Idle);
        Ok(())
    }

    /// The reply carrying `text`, the answer to `incoming`, and the title the turn suggested for the thread, whatever
    /// kind of reply it is: `None` when the answer went up the chain or nowhere instead, and the message is done with.
    pub(super) async fn reply_for(
        &self,
        incoming: &Incoming,
        read: Option<Context>,
        text: String,
        limit: Option<&String>,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<Option<Reply>> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let title = self.mcp.and_then(|mcp| mcp.take_title(thread));
        let reply = match (&incoming.post, read) {
            (Some(posted), Some(read)) => {
                let Some(post) = self.post_reply(incoming, &posted.post, read, &text, msg, seq).await? else {
                    return Ok(None);
                };
                Reply::Post(post)
            }
            _ if let Some(channel) = &incoming.resume => {
                let post = ChannelPost::on_thread(channel.clone(), thread.clone(), self.ant.clone(), text);
                let post = post.with_owner(envelope.owner.clone()).with_creator(envelope.creator.clone());
                Reply::Post(post.spend(envelope.turn_budget)?)
            }
            _ if envelope.webhook || envelope.from == *self.ant => {
                // Nobody reads a webhook's inbox, nor the ant's own for work it handed itself: the answer goes to the
                // thread's owner, else only to the log for a webhook and to the default human for a handoff.
                let owner = self.humans.human_owner(envelope.owner.as_ref());
                let owner = owner.or_else(|| (!envelope.webhook).then(|| self.humans.default_human().clone()));
                let Some(owner) = owner else {
                    if let Some(limit) = limit {
                        self.escalate(incoming, limit, EscalationReason::Cap, seq, None).await?;
                    }
                    tracing::info!(%thread, webhook = %envelope.from, answer = text, "answered a webhook with no owner");
                    self.done(msg, seq).await?;
                    return Ok(None);
                };
                let mut reply = envelope.reply(self.ant.clone(), text, Role::Human)?;
                reply.to = owner;
                Reply::Message(Box::new(reply))
            }
            _ => {
                // A reply on an escalation carries it, so its leader's answer is known as one.
                let mut reply = envelope.reply(self.ant.clone(), text, self.humans.sender_role(envelope))?;
                match escalation::route(self.ant, envelope).map(|(_, route)| route) {
                    Some(Route::Notes) => {
                        let Some(post) = self.notes_reply(incoming, reply.text, title.clone(), msg, seq).await? else {
                            return Ok(None);
                        };
                        Reply::Notes(post)
                    }
                    route => {
                        if let Some(Route::Stuck(stuck)) = route {
                            reply.to = stuck;
                        }
                        Reply::Message(Box::new(reply.with_escalation(envelope.escalation.as_deref().cloned())))
                    }
                }
            }
        };
        Ok(Some(match reply {
            Reply::Message(message) => {
                let (attachments, left_out) = self.reply_files(thread, |id| message.attachment_handle(id)).await;
                let text = with_left_out(message.text, left_out);
                Reply::Message(Box::new(Envelope { title, text, attachments, ..*message }))
            }
            Reply::Post(post) => {
                let (attachments, left_out) = self.reply_files(thread, |id| post.attachment_handle(id)).await;
                let text = with_left_out(post.text, left_out);
                Reply::Post(ChannelPost { title, text, attachments, ..post })
            }
            // Posted with the title and files already.
            Reply::Notes(post) => Reply::Notes(post),
        }))
    }

    /// The post answering the channel post `post` with `text`, given the thread's turns `read` before the prompt:
    /// `None` when the thread's turn budget is spent, the escalation carrying the answer, and the message is done with.
    async fn post_reply(
        &self,
        incoming: &Incoming,
        post: &ChannelPost,
        read: Context,
        text: &str,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<Option<ChannelPost>> {
        // Other members' replies stored during the prompt spent the thread's turns too.
        let humans = self.humans.humans();
        let later = match self.threads.context(post, seq, Earlier::Omitted, &humans).await {
            Ok(now) => now.later,
            Err(e) => {
                let thread = &incoming.envelope.thread_id;
                tracing::warn!(%thread, error = format!("{e:#}"), "rereading the thread's turns");
                read.later
            }
        };
        match post.reply(self.ant.clone(), text, &later, &humans) {
            Ok(post) => Ok(Some(post)),
            Err(ProtoError::BudgetExhausted(_)) => {
                // The answer is not posted: the escalation carries it.
                let why = "Turn budget spent before the answer was posted";
                self.escalate(incoming, why, EscalationReason::TurnBudget, seq, Some(text)).await?;
                bus::double_ack(self.client, msg).await?;
                self.attempts.forget(seq).await;
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// The post of `text` to the notes of the thread of `incoming`, with `title` and the files the session attached:
    /// `None` when the notes are out of turns, the escalation carrying the answer, and the message is done with.
    async fn notes_reply(
        &self,
        incoming: &Incoming,
        text: String,
        title: Option<String>,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<Option<NotesPost>> {
        let thread = &incoming.envelope.thread_id;
        let (store, budget) = (self.notes, incoming.envelope.turn_budget);
        let ant = self.ant.clone();
        let handle_of = |id| AttachmentHandle { from: ant.clone(), scope: AttachmentScope::Notes(thread.clone()), id };
        let (files, left_out) = self.reply_files(thread, handle_of).await;
        let text = with_left_out(text, left_out);
        let posted = notes::spend(store, thread, self.ant, &text, budget, title, files).await?;
        if posted.is_none() {
            let why = "Thread notes out of turns before the answer was posted";
            self.escalate(incoming, why, EscalationReason::Notes, seq, Some(&text)).await?;
            self.done(msg, seq).await?;
        }
        Ok(posted)
    }

    /// Store the files the session of `thread` attached to its final answer under the handles `handle_of` gives,
    /// returning their references, else none and why they are left out.
    async fn reply_files(
        &self,
        thread: &ThreadId,
        handle_of: impl Fn(Uuid) -> AttachmentHandle,
    ) -> (Vec<Attachment>, Option<String>) {
        let Some(mcp) = self.mcp else { return (Vec::new(), None) };
        match mcp.reply_files(thread, handle_of).await {
            Ok(files) => (files, None),
            Err(why) => {
                tracing::warn!(%thread, error = why, "attaching files to the answer");
                (Vec::new(), Some(why))
            }
        }
    }

    /// Store `reply` to `request`, the message at stream sequence `seq`, in the hub's stream or, out of its reach, in
    /// the machine's outbox, then ack the message and forget its marker. The message id, from the request's, makes
    /// JetStream drop a copy an earlier attempt stored within the stream's duplicate window. When the ack fails, the
    /// note in `answered` has the redelivery acked without answering again, even by a restarted daemon. Of a channel
    /// post it also notes that the posts on its thread up to it were shown.
    pub(super) async fn deliver(
        &self,
        incoming: &Incoming,
        reply: &Reply,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<()> {
        let request = &incoming.envelope;
        // Store the reply before acking: a crash in between redelivers rather than loses the message. A post to thread
        // notes is in the notes already.
        if !matches!(reply, Reply::Notes(_)) {
            bus::store(self.js, reply.subject(), reply, &reply_id(self.ant, request)).await?;
        }
        if incoming.post.is_some()
            && let Err(e) = self.threads.seen(&request.thread_id, seq).await
        {
            tracing::warn!(thread = %request.thread_id, error = format!("{e:#}"), "noting the posts shown");
        }
        let noted = self
            .answered
            .add(request, reply)
            .await
            .inspect_err(|e| tracing::warn!(error = format!("{e:#}"), "noting the stored reply"));
        match bus::double_ack(self.client, msg).await {
            Ok(()) => {
                self.attempts.forget(seq).await;
                self.answered.forget(request).await;
            }
            Err(e) if noted.is_ok() => {
                tracing::warn!(thread = %request.thread_id, error = %e, "acking; the redelivery is acked unanswered");
            }
            Err(e) => return Err(e),
        }
        status::state(self.client, self.ant, reply.thread_id(), State::Idle);
        Ok(())
    }
}

/// `text`, the answer, followed by an `[ants]` line saying why the files attached to it are left out, if they are.
fn with_left_out(text: String, left_out: Option<String>) -> String {
    match left_out {
        Some(why) => format!("{text}\n\n[ants] The files attached to this answer are left out: {why}."),
        None => text,
    }
}

/// The message id of `ant`'s reply to `request`: the same for every attempt at it.
pub(super) fn reply_id(ant: &UserId, request: &Envelope) -> String {
    format!("reply-{ant}-{}", request.id)
}
