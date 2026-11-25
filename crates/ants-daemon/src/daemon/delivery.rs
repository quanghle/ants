//! Delivering the answer: the reply of the kind the message calls for, carrying the files attached to it, stored
//! before the message is acked; or none, for another ant's reply to the ant's own message and for a turn ended by
//! `complete`, whose answer is kept on the thread's timeline instead.

use ants_proto::{
    Attachment, AttachmentHandle, AttachmentScope, ChannelEntry, ChannelPost, Envelope, Error as ProtoError,
    EscalationReason, NoReplyWhy, NotesPost, Role, State, ThreadId, Timeline, TimelineEntry, UserId, Uuid, subject,
};
use anyhow::Result;

use crate::bus;
use crate::channel::{Context, Earlier, Threads};
use crate::delivery::attempts::{GivenFiles, Reply, StoredFiles};
use crate::escalation::{self, Route};
use crate::limits::caps::AskedStep;
use crate::monitoring::status;
use crate::people::humans::Registry;
use crate::stream::notes;

use super::{Handling, Inbox, Incoming};

/// Reads of a channel thread a reply spending a turn makes, each after another post reached the channel first, before
/// it is stored with the turns last read.
const SPEND_TRIES: usize = 16;

impl Inbox<'_> {
    /// Whether `incoming` is another ant's reply to a message or post the ant sent, which is prompted but not answered
    /// back, so that two ants answering each other stop after one reply: given what was read of a post's thread.
    /// Never a message carrying an escalation or an approval request, a webhook's, a trail step, nor the resume of a
    /// channel thread after an escalation ([`Inbox::always_answered`]).
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
    /// webhook's, one carrying an escalation or an approval request, a trail step (a re-ask among them, which replies
    /// to the ant's own reply), and the resume of a channel thread after an escalation.
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
        } else if envelope.trail_step.is_some() {
            Some("a trail step")
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
        Handling { incoming, msg, seq }: Handling<'_>,
        (why, reason): (NoReplyWhy, Option<String>),
        text: String,
        limit: Option<&String>,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        tracing::info!(%thread, from = %envelope.from, ?why, "ending the turn with no reply");
        let entry = TimelineEntry::NoReply { why, reason, text };
        let record = Timeline::new(self.ant.clone(), thread.clone(), envelope.id, vec![entry], 0);
        crate::monitoring::timeline::publish(self.js, &record).await;
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

    /// The trail step on the thread of `incoming` whose turn asked a human, when the answer to `incoming`, a human's
    /// message (or one an assistant sent for its human) that is no step, is the step's reply ([`AskedStep`]).
    pub(super) async fn asked_step(&self, incoming: &Incoming) -> Result<Option<AskedStep>> {
        let envelope = &incoming.envelope;
        let human = envelope.on_behalf_of.is_some() || self.humans.sender_role(envelope).is_human();
        if !human || incoming.post.is_some() || incoming.resume.is_some() || envelope.trail_step.is_some() {
            return Ok(None);
        }
        self.ledger.asked_step(&envelope.thread_id).await
    }

    /// Whether `incoming` is a provider's wake on a thread no human owns, outside a channel: its answer stays on the
    /// thread, sent to nobody ([`NoReplyWhy::Unowned`]).
    pub(super) fn unowned_wake(&self, incoming: &Incoming) -> bool {
        let envelope = &incoming.envelope;
        incoming.post.is_none()
            && incoming.resume.is_none()
            && envelope.trail_step.is_none()
            && envelope.wake_provider().is_some()
            && self.humans.human_owner(envelope.owner.as_ref()).is_none()
    }

    /// Who the answer to `incoming` goes to when it goes as a message, outside a channel, as [`Inbox::reply_for`]
    /// addresses it: none for a channel post's or a resumed channel thread's, a trail step's the ant sent itself, a
    /// webhook's with no owner, a provider's wake on a thread no human owns, and one going to an escalation's notes.
    pub(super) fn answered_to(&self, incoming: &Incoming) -> Option<UserId> {
        let envelope = &incoming.envelope;
        if incoming.post.is_some() || incoming.resume.is_some() || self.unowned_wake(incoming) {
            return None;
        }
        if envelope.trail_step.is_some() && envelope.from == *self.ant {
            return None;
        }
        if envelope.webhook || envelope.from == *self.ant {
            let owner = self.humans.human_owner(envelope.owner.as_ref());
            return owner.or_else(|| (!envelope.webhook).then(|| self.humans.default_human().clone()));
        }
        match escalation::route(self.ant, envelope).map(|(_, route)| route) {
            Some(Route::Notes) => None,
            Some(Route::Stuck(stuck)) => Some(stuck),
            _ => Some(envelope.from.clone()),
        }
    }

    /// The reply carrying `text`, the answer to `incoming`, `title`, the one the turn suggested for the thread, and
    /// `files`, those it attached, whatever kind of reply it is: `None` when the answer went up the chain or nowhere
    /// instead, and the message is done with. `files` are stored under the reply's handles, and left as stored, so
    /// the reply made again after a later step failed carries the same ones.
    pub(super) async fn reply_for(
        &self,
        h @ Handling { incoming, msg, seq }: Handling<'_>,
        read: Option<Context>,
        (text, title): (String, Option<String>),
        files: &mut Option<GivenFiles>,
        limit: Option<&String>,
    ) -> Result<Option<Reply>> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let asked = self.asked_step(incoming).await?;
        let reply = match (&incoming.post, read) {
            (Some(posted), Some(read)) => {
                let Some(post) = self.post_reply(h, &posted.post, read, &text).await? else {
                    return Ok(None);
                };
                Reply::Post(Box::new(post))
            }
            // The answer once a human answered what the turn on a trail step asked: the step's reply, to its conductor.
            _ if let Some(asked) = asked => {
                let reply = envelope.reply(self.ant.clone(), text, Role::Human)?;
                let turn_budget = asked.turn_budget.saturating_sub(1);
                let to = asked.conductor;
                Reply::Message(Box::new(Envelope { to, in_reply_to: Some(asked.step), turn_budget, ..reply }))
            }
            _ if let Some(channel) = &incoming.resume => {
                let post = ChannelPost::on_thread(channel.clone(), thread.clone(), self.ant.clone(), text);
                let post = post.with_owner(envelope.owner.clone()).with_creator(envelope.creator.clone());
                Reply::Post(Box::new(post.spend(envelope.turn_budget)?))
            }
            // A step of a trail the ant conducts, sent itself: the answer returns to the run in the ant's inbox.
            _ if envelope.trail_step.is_some() && envelope.from == *self.ant => {
                Reply::Message(Box::new(envelope.reply(self.ant.clone(), text, Role::Ant)?))
            }
            _ if envelope.webhook || envelope.from == *self.ant => {
                // Nobody reads a webhook's inbox, nor the ant's own for work it handed itself: the answer goes to the
                // thread's owner, else only to the log for a webhook and to the default human for a handoff (a
                // provider's wake with no owner ends unanswered before, [`Inbox::unowned_wake`]).
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
                // A reply on an escalation carries it, so its leader's answer is known as one; but not the stuck
                // ant's own, so the leader's turn on it is a reply to its own message, sending nothing back.
                let carried = envelope.escalation.as_deref().filter(|e| e.stuck != *self.ant).cloned();
                let mut reply = envelope.reply(self.ant.clone(), text, self.humans.sender_role(envelope))?;
                match escalation::route(self.ant, envelope).map(|(_, route)| route) {
                    Some(Route::Notes) => {
                        let Some(post) = self.notes_reply(h, reply.text, title.clone(), files).await? else {
                            return Ok(None);
                        };
                        Reply::Notes(post)
                    }
                    route => {
                        if let Some(Route::Stuck(stuck)) = route {
                            reply.to = stuck;
                        }
                        Reply::Message(Box::new(reply.with_escalation(carried)))
                    }
                }
            }
        };
        Ok(Some(match reply {
            Reply::Message(mut message) => {
                if let Some(mcp) = self.mcp {
                    message.trail_spent = Box::pin(mcp.trail_spent(envelope, &message)).await;
                }
                let (attachments, left_out) = self.reply_files(thread, files, |id| message.attachment_handle(id)).await;
                let text = with_left_out(message.text, left_out);
                Reply::Message(Box::new(Envelope { title, text, attachments, ..*message }))
            }
            Reply::Post(post) => {
                let entry = incoming.post.as_ref().and_then(|posted| posted.triage.as_ref());
                let post = Box::pin(self.through_teams(*post, entry)).await;
                let (attachments, left_out) = self.reply_files(thread, files, |id| post.attachment_handle(id)).await;
                let text = with_left_out(post.text, left_out);
                Reply::Post(Box::new(ChannelPost { title, text, attachments, ..post }))
            }
            // Posted with the title and files already.
            Reply::Notes(post) => Reply::Notes(post),
        }))
    }

    /// The post answering the channel post `post` with `text`, given the thread's turns `read` before the prompt:
    /// `None` when the thread's turn budget is spent, the escalation carrying the answer, and the message is done with.
    async fn post_reply(
        &self,
        Handling { incoming, msg, seq }: Handling<'_>,
        post: &ChannelPost,
        read: Context,
        text: &str,
    ) -> Result<Option<ChannelPost>> {
        // Other members' replies stored during the prompt spent the thread's turns too.
        let humans = self.humans.humans();
        let later = match self.threads.context(post, seq, Earlier::Omitted, &humans).await {
            Ok(mut now) => {
                self.fresh_budgets(&mut now.later, &humans);
                now.later
            }
            Err(e) => {
                let thread = &incoming.envelope.thread_id;
                tracing::warn!(%thread, error = format!("{e:#}"), "rereading the thread's turns");
                read.later
            }
        };
        match post.reply(self.ant.clone(), text, &later, &humans) {
            Ok(post) => Ok(Some(post)),
            Err(ProtoError::BudgetExhausted(_)) => {
                self.unposted(Handling { incoming, msg, seq }, text).await?;
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// `post`, an answer posted to a channel, with each team it @mentions standing for the team's lead, as a post of
    /// `post_channel` has it, checked against the channel's members: those of `entry`, else as stored now. An answer
    /// cannot be refused as a tool's post is: a team that cannot stand for its lead stays mentioned, waking nobody,
    /// and an `[ants]` line after the text says why ([`ChannelPost::through_teams_leniently`]). A directory that
    /// cannot be read leaves the post as it is; a channel entry that cannot be read leaves the leads unchecked.
    async fn through_teams(&self, post: ChannelPost, entry: Option<&ChannelEntry>) -> ChannelPost {
        let Some(kv) = self.teams.as_ref().filter(|_| !post.mentions.is_empty()) else { return post };
        let Some(directory) = crate::teams::read_quickly(kv).await else { return post };
        if !post.mentions.iter().any(|mention| directory.is_team(mention)) {
            return post;
        }
        let stored;
        let entry = match entry {
            Some(entry) => Some(entry),
            None => {
                stored = self.stored_entry(&post.channel).await;
                stored.as_ref()
            }
        };
        let (mut post, refused) = post.through_teams_leniently(&directory, entry);
        if refused.is_empty() {
            return post;
        }
        let notes: Vec<String> = refused
            .iter()
            .map(|refusal| format!("[ants] A team this answer mentions wakes nobody: {}.", refusal.why()))
            .collect();
        let (channel, thread) = (&post.channel, &post.thread_id);
        tracing::info!(%channel, %thread, ?notes, "a team the answer mentions wakes nobody");
        post.text = format!("{}\n\n{}", post.text, notes.join("\n"));
        post
    }

    /// Escalate the thread of `incoming`, out of turns before its answer `text` was posted, with the answer, and be
    /// done with the message.
    async fn unposted(&self, Handling { incoming, msg, seq }: Handling<'_>, text: &str) -> Result<()> {
        let why = "Turn budget spent before the answer was posted";
        self.escalate(incoming, why, EscalationReason::TurnBudget, seq, Some(text)).await?;
        bus::double_ack(self.client, msg).await?;
        self.marks.attempts.forget(seq).await;
        Ok(())
    }

    /// Store `post`, the answer to the channel post `answered` of `h`, spending one of the thread's turns: the thread
    /// is read again and the post stored with what is left only while no other post reached the channel since, else
    /// read again, so members answering at once each spend a turn of their own. `None` when the turns ran out first,
    /// the escalation carrying the answer, and the message is done with. One stored already, by an earlier attempt, is
    /// not stored again.
    async fn post_spending(
        &self,
        h @ Handling { incoming, seq, .. }: Handling<'_>,
        answered: &ChannelPost,
        post: ChannelPost,
    ) -> Result<Option<ChannelPost>> {
        let id = reply_id(self.ant, &incoming.envelope);
        let turn_budget = self.escalator.turn_budget();
        let spending = Spending { js: self.js, threads: self.threads, humans: self.humans, turn_budget };
        match spending.store(answered, seq, post, &id).await? {
            Spent::Stored(post) => Ok(Some(post)),
            Spent::Exhausted(post) => {
                self.unposted(h, &post.text).await?;
                Ok(None)
            }
        }
    }

    /// The post of `text` to the notes of the thread of `incoming`, with `title` and `files`, those the turn attached:
    /// `None` when the notes are out of turns, the escalation carrying the answer, and the message is done with.
    async fn notes_reply(
        &self,
        Handling { incoming, msg, seq }: Handling<'_>,
        text: String,
        title: Option<String>,
        files: &mut Option<GivenFiles>,
    ) -> Result<Option<NotesPost>> {
        let thread = &incoming.envelope.thread_id;
        let (store, budget) = (self.notes, incoming.envelope.turn_budget);
        let ant = self.ant.clone();
        let handle_of = |id| AttachmentHandle { from: ant.clone(), scope: AttachmentScope::Notes(thread.clone()), id };
        let (files, left_out) = self.reply_files(thread, files, handle_of).await;
        let text = with_left_out(text, left_out);
        let posted = notes::spend(store, thread, self.ant, &text, budget, title, files).await?;
        if posted.is_none() {
            let why = "Thread notes out of turns before the answer was posted";
            self.escalate(incoming, why, EscalationReason::Notes, seq, Some(&text)).await?;
            self.done(msg, seq).await?;
        }
        Ok(posted)
    }

    /// The references of `files`, those the turn on `thread` attached to its final answer, stored under the handles
    /// `handle_of` gives unless an earlier attempt at the reply stored them there, else none and why they are left
    /// out. Once stored, `files` holds their references.
    async fn reply_files(
        &self,
        thread: &ThreadId,
        files: &mut Option<GivenFiles>,
        handle_of: impl Fn(Uuid) -> AttachmentHandle,
    ) -> (Vec<Attachment>, Option<String>) {
        let Some(given) = files else { return (Vec::new(), None) };
        let under = handle_of(Uuid::nil()).to_string();
        if let Some(stored) = given.stored.as_ref().filter(|stored| stored.under == under) {
            return (stored.files.clone(), None);
        }
        let stored = match self.mcp {
            Some(mcp) => mcp.store_reply_files(&given.dir, &given.paths, handle_of).await,
            None => Err("attachments are not available here".to_owned()),
        };
        match stored {
            Ok(stored) => {
                given.stored = Some(StoredFiles { under, files: stored.clone() });
                (stored, None)
            }
            Err(why) => {
                tracing::warn!(%thread, error = why, "attaching files to the answer");
                (Vec::new(), Some(why))
            }
        }
    }

    /// Note the findings of the sage's answer to `request` and what the review thread spent in all. A fresh human
    /// message renews only its allowance, not this total. Sense keeps only the first answer to a review.
    pub(super) async fn note_review(&self, request: &Envelope, text: &str) {
        let Some(sense) = self.provider.sense.filter(|_| request.review_outcomes.is_some()) else { return };
        let thread = &request.thread_id;
        let spent = self.ledger.thread_total(thread).await.inspect_err(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading what the review thread spent");
        });
        let spent = spent.ok().filter(|spent| spent.tokens > 0 || spent.cost > 0.0);
        if let Err(e) = sense.review_given(self.ant, request, text, spent).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "noting the answer to a review");
        }
    }

    /// Store `reply` to `request`, the message at stream sequence `seq`, in the hub's stream or, out of its reach, in
    /// the machine's outbox, then ack the message and forget its marker. The message id, from the request's, makes
    /// JetStream drop a copy an earlier attempt stored within the stream's duplicate window. When the ack fails, the
    /// note in `answered` has the redelivery acked without answering again, even by a restarted daemon. Of a channel
    /// post it also notes that the posts on its thread up to it were shown, and of an answer to a review an ant asked,
    /// the findings it gives and what its thread spent in all, by the ledger, in the ant's sense
    /// ([`crate::sense::Sense::review_given`]).
    pub(super) async fn deliver(&self, h @ Handling { incoming, msg, seq }: Handling<'_>, reply: &Reply) -> Result<()> {
        let request = &incoming.envelope;
        // Store the reply before acking: a crash in between redelivers rather than loses the message. A post to thread
        // notes is in the notes already; an answer to a channel post spends a turn of its thread as it is stored.
        let spent;
        let reply = match (reply, &incoming.post) {
            (Reply::Notes(_), _) => reply,
            (Reply::Post(post), Some(posted)) => {
                // Boxed: rereading the thread would grow the future of every turn delivering a reply.
                let spending = Box::pin(self.post_spending(h, &posted.post, (**post).clone()));
                let Some(post) = spending.await? else { return Ok(()) };
                spent = Reply::Post(Box::new(post));
                &spent
            }
            _ => {
                bus::store(self.js, reply.subject(), reply, &reply_id(self.ant, request)).await?;
                reply
            }
        };
        // After the store, so that a redelivery, sending the stored reply again, notes the answer it holds.
        if let Reply::Message(answer) = reply
            && answer.notice.is_none()
            && self.asks_review(incoming)
        {
            self.note_review(request, &answer.text).await;
        }
        if incoming.post.is_some()
            && let Err(e) = self.threads.seen(&request.thread_id, seq).await
        {
            tracing::warn!(thread = %request.thread_id, error = format!("{e:#}"), "noting the posts shown");
        }
        let noted = self
            .marks
            .answered
            .add(request, reply)
            .await
            .inspect_err(|e| tracing::warn!(error = format!("{e:#}"), "noting the stored reply"));
        self.marks.answered.forget_unsent(request).await;
        match bus::double_ack(self.client, msg).await {
            Ok(()) => {
                self.marks.attempts.forget(seq).await;
                self.marks.answered.forget(request).await;
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

/// Bytes a reply leaves under the server's `max_payload` for all but its text: the envelope or post around it, its
/// attachments and escalation, the attempt marker recording it, and the headers.
const REPLY_HEADROOM: usize = 16 << 10;

/// Bytes the text of a reply takes at most as JSON on the connection of `client` ([`fit_to`]).
pub(super) fn reply_room(client: &async_nats::Client) -> usize {
    bus::max_payload(client).saturating_sub(REPLY_HEADROOM)
}

/// `text`, the answer, cut to take at most `room` bytes as JSON, ending with an `[ants]` line telling of the cut when
/// it is cut: a reply past the server's `max_payload` could be neither recorded nor sent.
pub(super) fn fit_to(text: String, room: usize) -> String {
    let size = |text: &str| serde_json::to_string(text).map_or(usize::MAX, |json| json.len());
    let mut over = size(&text).saturating_sub(room);
    if over == 0 {
        return text;
    }
    let mut keep = text.len();
    // Each byte cut shrinks the JSON by one or more, its escape included: a few rounds at most.
    loop {
        keep = text.floor_char_boundary(keep.saturating_sub(over));
        let cut = format!(
            "{}\n\n[ants] This answer was cut to fit the bus's max_payload: {keep} of its {} bytes are kept.",
            &text[..keep],
            text.len()
        );
        over = size(&cut).saturating_sub(room);
        if over == 0 || keep == 0 {
            return cut;
        }
    }
}

/// The message id of `ant`'s reply to `request`: the same for every attempt at it.
pub(super) fn reply_id(ant: &UserId, request: &Envelope) -> String {
    format!("reply-{ant}-{}", request.id)
}

/// What storing an ant's post on a channel thread, spending one of the thread's turns, reads and writes through.
pub(super) struct Spending<'a> {
    pub(super) js: &'a async_nats::jetstream::Context,
    pub(super) threads: &'a Threads,
    pub(super) humans: &'a Registry,
    /// The turns a human's post gives the thread ([`Inbox::fresh_budgets`]).
    pub(super) turn_budget: u32,
}

/// What came of storing a post spending a turn.
pub(super) enum Spent {
    /// Stored, with the turns it left the thread.
    Stored(ChannelPost),
    /// Not stored: the thread had no turns left.
    Exhausted(ChannelPost),
}

impl Spending<'_> {
    /// Store `post`, answering the channel post `answered` at stream sequence `seq`, as message `id`, spending one of
    /// the thread's turns: the thread is read again and the post stored with what is left only while no other post
    /// reached the channel since, else read again, so members answering at once each spend a turn of their own. One
    /// stored already under its id is not stored again.
    pub(super) async fn store(
        &self,
        answered: &ChannelPost,
        seq: u64,
        mut post: ChannelPost,
        id: &str,
    ) -> Result<Spent> {
        let thread = &answered.thread_id;
        let filter = subject::chan_room(&answered.channel);
        for _ in 0..SPEND_TRIES {
            let humans = self.humans.humans();
            let read = match self.threads.context(answered, seq, Earlier::Omitted, &humans).await {
                Ok(mut read) => {
                    super::admission::fresh_budgets(&mut read.later, &humans, self.turn_budget);
                    read
                }
                Err(e) => {
                    tracing::warn!(%thread, error = format!("{e:#}"), "rereading the thread's turns; posting");
                    break;
                }
            };
            if read.later.iter().any(|later| later.id == post.id) {
                return Ok(Spent::Stored(post));
            }
            post = match post.clone().spend(answered.turns_left(&read.later, &humans)) {
                Ok(post) => post,
                Err(ProtoError::BudgetExhausted(_)) => return Ok(Spent::Exhausted(post)),
                Err(e) => return Err(e.into()),
            };
            match bus::store_expecting(self.js, post.subject(), &post, id, &filter, read.last_seq).await? {
                bus::Expected::Stored(_) => return Ok(Spent::Stored(post)),
                bus::Expected::Moved => {}
            }
        }
        tracing::warn!(%thread, "the channel kept changing while posting; posting with the turns last read");
        bus::store(self.js, post.subject(), &post, id).await?;
        Ok(Spent::Stored(post))
    }
}

/// What stores the posts the ant makes on the channel thread its turn handles with `post_channel`'s `on_thread`
/// ([`crate::mcp::Mcp::with_thread_posts`]), redacted, spending a turn as its own replies do.
pub(super) fn thread_poster(
    js: async_nats::jetstream::Context,
    threads: std::sync::Arc<Threads>,
    humans: Registry,
    turn_budget: u32,
    redactor: std::sync::Arc<crate::secrets::redact::Redactor>,
) -> crate::mcp::ThreadPoster {
    std::sync::Arc::new(move |answered: ChannelPost, seq, mut post: ChannelPost| {
        let (js, threads, humans) = (js.clone(), threads.clone(), humans.clone());
        redactor.redact_string(&mut post.text);
        Box::pin(async move {
            let spending = Spending { js: &js, threads: &threads, humans: &humans, turn_budget };
            let id = post.id.to_string();
            Ok(match spending.store(&answered, seq, post, &id).await? {
                Spent::Stored(post) => Some(post),
                Spent::Exhausted(_) => None,
            })
        })
    })
}
