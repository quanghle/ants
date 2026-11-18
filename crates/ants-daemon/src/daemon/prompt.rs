//! Prompting the harness with a message: the prompt's text, running it with its status, spend and cancels, and the
//! notices its reply carries, escalating what the ant cannot settle.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;

use ants_proto::{
    CallCount, Envelope, Error as ProtoError, EscalationReason, Hold, Humans, NoReplyWhy, NoticeKind, PromptEnd, Stage,
    State, StatusUpdate, SummaryOf, SummaryRequest, ThreadId, Timeline, TimelineEnd, TimelineEntry, UserId, Uuid,
};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};

use crate::bus;
use crate::channel::{self, Context, Earlier};
use crate::checkouts::Prepared;
use crate::delivery::attempts::{Marker, Reply};
use crate::escalation::{self, Stuck};
use crate::files::Attached;
use crate::harness::{Prompt, SignedOut, Spend, Steer, Steering};
use crate::limits::breaker::StartFailed;
use crate::limits::caps::{Recorded, Warning};
use crate::limits::fallback::OnFallback;
use crate::limits::overflow::ContextOverflow;
use crate::limits::ratelimit::Limited;
use crate::monitoring::status::{self, Status};
use crate::provider::brief::Briefing;
use crate::sync::MutexExt as _;

use super::control::{Cancel, Why};
use super::steering::{Join, Joined, Requeue};
use super::{Inbox, Incoming, Posted, Steered};

/// How many steers wait at most for the harness to take them into the running prompt.
const STEERS: usize = 8;

/// Put before the text of a message whose earlier attempt started and stored no reply.
const CUT_OFF: &str = "[ants] An earlier attempt at this message was cut off before its reply was stored, so tools it \
ran may already have taken effect. The session history may show what ran. Check the current state before repeating \
any action.";

/// What came of running a prompt.
pub(super) struct Ran {
    reply: Result<crate::harness::Reply>,
    /// The human's cancel of the prompt, if one came.
    cancelled_by: Option<Cancel>,
    /// Whether the cancel cut the prompt short.
    cut_by_cancel: bool,
    /// Whether a tool call started, so tools may have run.
    worked: bool,
    /// The cap the prompt's spend reached.
    reached: Option<String>,
    /// The spend still to record.
    spend: tokio::sync::mpsc::UnboundedReceiver<Spend>,
    /// The fallback the prompt ran on, `None` for the ant's own harness.
    fallback: Option<usize>,
    measured: Measured,
    /// The messages that joined the prompt, in order: the last one gets its answer.
    joined: Vec<Join>,
}

/// What a prompt took, noted in the ant's timeline as it ends ([`TimelineEntry::Ended`]).
struct Measured {
    /// The message the prompt ran for, whose timeline the end joins.
    message: Uuid,
    /// When the prompt began: when it got its thread's harness, once told so through `harnessed`.
    began: tokio::time::Instant,
    harnessed: tokio::sync::oneshot::Receiver<tokio::time::Instant>,
    /// How long the prompt ran, once it ended.
    took: std::time::Duration,
    /// The tokens the prompt spent so far, as the harness reported them.
    tokens: u64,
    /// The cost in US dollars the prompt spent so far, as the harness reported it.
    cost: f64,
    /// The prompt's tool calls by kind, once it ended ([`TimelineEnd::calls`]).
    calls: BTreeMap<String, CallCount>,
    /// How many of its tool calls called sense's own tools, once it ended.
    sense_calls: u32,
}

impl Measured {
    /// The measures of the prompt for `message`, beginning now or when told through the sender returned
    /// ([`Prompt::began`]).
    fn new(message: Uuid) -> (Self, tokio::sync::oneshot::Sender<tokio::time::Instant>) {
        let (tx, harnessed) = tokio::sync::oneshot::channel();
        let (began, took) = (tokio::time::Instant::now(), std::time::Duration::ZERO);
        (Self { message, began, harnessed, took, tokens: 0, cost: 0.0, calls: BTreeMap::new(), sense_calls: 0 }, tx)
    }

    /// Note that the prompt ended now, with the tool calls `calls` collected. A wait for a free harness is not the
    /// prompt's.
    fn end(&mut self, calls: &crate::monitoring::timeline::Collector) {
        if let Ok(at) = self.harnessed.try_recv() {
            self.began = at;
        }
        self.took = self.began.elapsed();
        (self.calls, self.sense_calls) = calls.counts();
    }
}

/// The senders of the narrowed messages warned of, so a harness running its tools unasked is warned of once per webhook
/// or schedule, or ant passing a narrowing on, rather than on each prompt.
#[derive(Default)]
pub(super) struct Unnarrowed(pub(super) std::sync::Mutex<HashSet<UserId>>);

impl Unnarrowed {
    /// The most senders kept; past it, the set starts over.
    pub(super) const MAX: usize = 1024;

    /// Whether `sender` was not warned of yet, noting it so.
    pub(super) fn first(&self, sender: &UserId) -> bool {
        let mut warned = self.0.locked();
        if warned.contains(sender) {
            return false;
        }
        if warned.len() >= Self::MAX {
            warned.clear();
        }
        warned.insert(sender.clone())
    }
}

/// The turn of `incoming`, the message its thread handles now, on the ant's MCP server ([`crate::mcp::Mcp::begin`]).
fn begin_turn(mcp: &crate::mcp::Mcp, incoming: &Incoming, msg: &jetstream::Message) -> crate::mcp::Turn {
    let carried = incoming.attached().into_iter().map(|(handle, _)| handle).collect();
    mcp.begin(&incoming.envelope).carrying(carried, crate::files::stored(msg))
}

impl Inbox<'_> {
    /// Settle where the thread of `incoming` came from, stored before its session can call the provider's tools,
    /// whether it starts a prompt or joins one ([`crate::mcp::Mcp::settle_origin`]).
    async fn settle_origin(&self, incoming: &Incoming) -> Result<()> {
        match self.mcp {
            Some(mcp) => mcp.settle_origin(&incoming.envelope, incoming.channel()).await,
            None => Ok(()),
        }
    }

    /// The `[ants]` lines for the thread's branch holding new commits, and for publishing it.
    async fn branch_notices(
        &self,
        envelope: &Envelope,
        msg: &jetstream::Message,
        cancelled: bool,
    ) -> [Option<String>; 2] {
        let thread = &envelope.thread_id;
        let publishing = self.publisher.zip(self.pool.worktrees());
        if let Some((publisher, worktrees)) = publishing {
            publisher.note_opening(worktrees, thread, &envelope.text).await;
        }
        let branch = self.pool.branch_ahead(thread).await;
        let branch_notice = branch.as_ref().map(|branch| {
            format!("[ants] Branch {} holds {} not in {}.", branch.name, branch.commits_text(), branch.outside())
        });
        // What a human cancelled is not published.
        let publish_notice = match (publishing, &branch) {
            (Some((publisher, worktrees)), Some(branch)) if !cancelled => {
                let policy = self.pool.launch().policy();
                self.acking(msg, publisher.publish(worktrees, branch, policy)).await
            }
            _ => None,
        };
        [branch_notice, publish_notice]
    }

    /// Add the prompt just completed to the ant's identity record, telling the ant's sense of the record when the ant
    /// reached its next stage; a failure only warns.
    async fn count_prompt(&self) {
        let Some(identity) = self.identity else { return };
        let record = match identity.count_prompt().await {
            Ok(Some(record)) => record,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "counting the prompt in the ant's identity");
                return;
            }
        };
        if let Some(sense) = self.sense
            && Stage::of(record.prompts.saturating_sub(1)) != record.stage()
            && let Err(e) = sense.grown(self.ant, &record).await
        {
            tracing::warn!(error = format!("{e:#}"), "noting the ant's growth in its sense data");
        }
    }

    /// Answer the message at stream sequence `seq`, given what an earlier attempt at it left. Every attempt after one
    /// that started, whether it failed or was cut off, is told that tools may already have run; one whose reply was
    /// recorded is not prompted again. Another ant's reply to the ant's own message, and a turn ended by `complete`,
    /// are answered with no reply ([`Inbox::end_unanswered`]); the former is prompted even with no turns left.
    pub(super) async fn answer(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        marker: Option<Marker>,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let cut_off = match marker {
            Some(Marker::Replied(reply)) => {
                tracing::info!(%thread, "sending the reply an earlier attempt recorded");
                return self.deliver(incoming, &reply, msg, seq).await;
            }
            Some(Marker::Started) => true,
            None => false,
        };
        // A post's thread has the turns its posts since left, which other members' replies may have spent.
        let humans = self.humans.humans();
        let read = match &incoming.post {
            Some(posted) => Some(self.threads.context(&posted.post, seq, Earlier::Unseen, &humans).await?),
            None => None,
        };
        // Answered with no reply, so it spends no turn.
        let own = self.replies_to_own(incoming, read.as_ref());
        let spent = self.turns_spent(incoming, read.as_ref(), &humans);
        if self.out_of_turns(incoming, spent && !own, msg, seq).await? {
            return Ok(());
        }
        self.settle_origin(incoming).await?;
        if !cut_off {
            // Durable before the harness can run anything.
            self.attempts.start(seq, envelope).await?;
        }
        if let Some(why) = self.unheld_review(incoming) {
            return self.decline_review(incoming, read, &why, msg, seq).await;
        }

        tracing::info!(%thread, from = %envelope.from, cut_off, own, "prompting");
        status::state(self.client, self.ant, thread, State::Working);

        // A message joining a running prompt takes over the turn of the message it started with.
        let joining = incoming.steered == Steered::Joining;
        self.warn_unnarrowed(incoming);
        let _narrowed = (!joining).then(|| self.pool.launch().policy().narrow(thread, envelope.allow.as_deref()));
        let _turn = self.mcp.filter(|_| !joining).map(|mcp| begin_turn(mcp, incoming, msg));
        let _asking = self.approvals.filter(|_| !joining).map(|approvals| approvals.begin(envelope));
        let text = self.prompt_for(incoming, read.as_ref(), &humans, cut_off, own.then_some(spent)).await;
        let (text, _checkouts) = self.review_checkouts(incoming, msg, joining, text).await;
        // The answer of a prompt a message joined is sent while the turn of the one it started with lasts.
        let mut _answering = None;
        let ran = if joining {
            let Some((ran, answering)) = self.joined(incoming, own, msg, seq, cut_off, text).await? else {
                return Ok(());
            };
            _answering = Some(answering);
            ran
        } else {
            let flush =
                tokio::time::interval_at(tokio::time::Instant::now() + status::FLUSH_EVERY, status::FLUSH_EVERY);
            let ran = Box::pin(self.run_prompt(incoming, msg, seq, text, flush)).await;
            if !ran.joined.is_empty() {
                return self.hand_on(incoming, own, ran, msg, seq, cut_off).await;
            }
            ran
        };
        let Ran { reply, cancelled_by, cut_by_cancel, worked, mut reached, spend, fallback, mut measured, .. } = ran;
        // While the turn lasts: it ends with `_turn`.
        let completed = self.completed(incoming);
        let reply = match reply {
            // What a human cancelled is answered as it stands, never tried again.
            Err(e) if cancelled_by.is_some() => {
                tracing::warn!(%thread, error = format!("{e:#}"), "cancelled prompt failed");
                crate::harness::Reply { text: String::new(), cut: None, rebuilt: false }
            }
            Err(e) => {
                self.note_failed(thread, &measured, &e, worked).await;
                return self.prompt_failed(e, incoming, msg, seq, cut_off, worked).await;
            }
            Ok(reply) => reply,
        };
        self.count_prompt().await;
        let fallback_notice = self.served_by(fallback);
        self.record_ended(envelope, spend, &mut reached, &mut measured).await;

        // A human's cancel is no limit: the reply tells of it, and nothing is escalated for it.
        let cut = reply.cut.is_some() && !cut_by_cancel;
        let limit = reply.cut.filter(|_| !cut_by_cancel).or(reached);
        let unanswered = unanswered(completed, own);
        self.note_end(thread, &measured, end_of(cancelled_by.as_ref(), &limit, &unanswered), reply.rebuilt).await;
        let limit_notice =
            limit.as_ref().map(|l| format!("[ants] {l}{}", if cut { "; the prompt was cancelled." } else { "." }));
        let [branch_notice, publish_notice] = self.branch_notices(envelope, msg, cancelled_by.is_some()).await;
        let cancel_notice = cancelled_by.map(|cancel| cancel.notice(self.ant));
        let notice = [fallback_notice, limit_notice, cancel_notice, branch_notice, publish_notice];
        let notice = notice.into_iter().flatten().collect::<Vec<_>>().join("\n");
        let text = [reply.text, notice].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n\n");
        let text = self.redactor.redact(&text).into_owned();
        if let Some(unanswered) = unanswered {
            return self.end_unanswered(incoming, unanswered, text, limit.as_ref(), msg, seq).await;
        }
        let Some(mut reply) = self.reply_for(incoming, read, text, limit.as_ref(), msg, seq).await? else {
            return Ok(());
        };
        if let Reply::Message(message) = &mut reply {
            message.summary = envelope.summary.clone().map(|s| SummaryRequest { tokens: Some(measured.tokens), ..s });
        }
        // Before the reply is recorded, so a redelivery that only resends the reply still escalates; its message id
        // keeps a second escalation out of the stream.
        if let Some(limit) = limit
            && !self.humans.sender_role(envelope).is_human()
        {
            self.escalate(incoming, &limit, EscalationReason::Cap, seq, None).await?;
        }
        // From here on a redelivery sends this reply rather than prompting again. Out of the hub's reach, as while a
        // leaf node's uplink is down, the reply still goes to the machine's outbox, and `answered` keeps it from being
        // prompted again.
        if let Err(e) = self.attempts.replied(seq, envelope, &reply).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "recording the reply");
        }
        self.deliver(incoming, &reply, msg, seq).await
    }

    /// Prompt `text` telling of the checkouts of the commits `incoming` names, with the checkouts, to keep while the
    /// prompt lasts, when it is a review an ant asked for: a message, not a post or a webhook's, narrowed to
    /// [`ants_proto::REVIEW_ALLOW`] ([`crate::checkouts`]). `text` alone for one `joining` a running prompt.
    async fn review_checkouts(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        joining: bool,
        text: String,
    ) -> (String, Option<Prepared>) {
        let review = !joining && self.asks_review(incoming);
        let Some(checkouts) = self.pool.checkouts().filter(|_| review) else { return (text, None) };
        let prepared = self.acking(msg, checkouts.prepare(&incoming.envelope)).await;
        (format!("{text}\n\n{}", prepared.note()), Some(prepared))
    }

    /// Whether `incoming` is a review an ant asked for: a message, not a post or a webhook's, narrowed to
    /// [`ants_proto::REVIEW_ALLOW`].
    pub(super) fn asks_review(&self, incoming: &Incoming) -> bool {
        let envelope = &incoming.envelope;
        incoming.post.is_none()
            && !envelope.webhook
            && !self.humans.sender_role(envelope).is_human()
            && ants_proto::is_review(envelope.allow.as_deref())
    }

    /// Why `incoming` is a review ([`ants_proto::REVIEW_ALLOW`]) that nothing would hold to read-only tools, unless it
    /// joins a prompt running.
    fn unheld_review(&self, incoming: &Incoming) -> Option<String> {
        let review = incoming.steered != Steered::Joining && ants_proto::is_review(incoming.envelope.allow.as_deref());
        review.then(|| self.pool.launch().unheld_review()).flatten()
    }

    /// Answer a review that nothing would hold to read-only tools, `why`, with a notice declining it, unprompted: its
    /// sender then goes on without the review.
    async fn decline_review(
        &self,
        incoming: &Incoming,
        read: Option<Context>,
        why: &str,
        msg: &jetstream::Message,
        seq: u64,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        tracing::warn!(thread = %envelope.thread_id, from = %envelope.from, why, "declining a review");
        let text = format!("[ants] {} declines the review: {why}.", self.ant);
        let Some(reply) = self.reply_for(incoming, read, text, None, msg, seq).await? else { return Ok(()) };
        if let Err(e) = self.attempts.replied(seq, envelope, &reply).await {
            tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "recording the reply");
        }
        self.deliver(incoming, &reply, msg, seq).await
    }

    /// Warn, once per sender, that the tool rules a message not joining a prompt narrows the thread to do not reach a
    /// harness running its tools unasked. A review's do ([`crate::harness::Launch::for_review`]).
    fn warn_unnarrowed(&self, incoming: &Incoming) {
        let (envelope, launch) = (&incoming.envelope, self.pool.launch());
        if incoming.steered != Steered::Joining
            && envelope.allow.as_deref().is_some_and(|allow| !crate::harness::allows_all(allow))
            && !ants_proto::is_review(envelope.allow.as_deref())
            && launch.bypasses_permissions()
            && self.unnarrowed.first(&envelope.from)
        {
            tracing::warn!(
                thread = %envelope.thread_id,
                from = %envelope.from,
                harness = launch.harness_name(),
                "the thread's tool rules are narrowed, but the harness runs its tools unasked: the narrowing does not \
                 apply to them (warned once per sender)"
            );
        }
    }

    /// Join the prompt running on the thread of `incoming` with `text`: the prompt's answer to send, with the turn to
    /// hold while sending it, or None when `incoming` was settled as superseded ([`Inbox::superseded`]). A prompt that
    /// refused it or failed hands it back to wait ([`Requeue`]).
    async fn joined(
        &self,
        incoming: &Incoming,
        own: bool,
        msg: &jetstream::Message,
        seq: u64,
        cut_off: bool,
        text: String,
    ) -> Result<Option<(Ran, tokio::sync::oneshot::Sender<()>)>> {
        let envelope = &incoming.envelope;
        match self.join(incoming, msg, text).await {
            Some(Joined::Answer(ran, answering)) => Ok(Some((*ran, answering))),
            Some(Joined::Superseded(by, id)) => {
                self.superseded(incoming, own, (&by, id), msg, seq).await.map(|()| None)
            }
            Some(Joined::Refused) => {
                if !cut_off {
                    self.attempts.forget(seq).await;
                }
                self.attempts.release(envelope).await;
                Err(Requeue.into())
            }
            // The prompt failed: the retry is warned it was cut off.
            None => {
                self.attempts.retrying(envelope, None).await;
                Err(Requeue.into())
            }
        }
    }

    /// Join the prompt running on the thread of `incoming` with `text`: what came of it, None when the prompt failed.
    async fn join(&self, incoming: &Incoming, msg: &jetstream::Message, text: String) -> Option<Joined> {
        let envelope = &incoming.envelope;
        let stored = crate::files::stored(msg);
        let text = match Attached::new(self.js.clone(), incoming.attached(), stored) {
            Some(attached) => format!("{text}\n\n{}", attached.show(false).await.lines),
            None => text,
        };
        let carried = incoming.attached().into_iter().map(|(handle, _)| handle).collect();
        let (joined, outcome) = tokio::sync::oneshot::channel();
        let join = Join { envelope: envelope.clone(), carried, stored, text, joined };
        if !self.steers.join(join) {
            return Some(Joined::Refused);
        }
        self.acking(msg, outcome).await.ok()
    }

    /// Hand what came of the prompt run for `incoming` to the messages that joined it ([`Ran::joined`]): its answer to
    /// the last, and to each before it, `incoming` first, the next one it went towards ([`Inbox::superseded`]). The
    /// turn of `incoming` lasts until the answer is sent. A failed prompt is settled as for `incoming` alone, the
    /// messages that joined it waiting again.
    async fn hand_on(
        &self,
        incoming: &Incoming,
        own: bool,
        mut ran: Ran,
        msg: &jetstream::Message,
        seq: u64,
        cut_off: bool,
    ) -> Result<()> {
        let mut joined = std::mem::take(&mut ran.joined);
        if ran.cancelled_by.is_none()
            && let Err(e) = ran.reply
        {
            drop(joined);
            self.note_failed(&incoming.envelope.thread_id, &ran.measured, &e, ran.worked).await;
            return self.prompt_failed(e, incoming, msg, seq, cut_off, ran.worked).await;
        }
        let spend = std::mem::replace(&mut ran.spend, tokio::sync::mpsc::unbounded_channel().1);
        self.record_ended(&incoming.envelope, spend, &mut ran.reached, &mut ran.measured).await;
        let last = joined.pop().expect("a message joined");
        let mut by = (last.envelope.from.clone(), last.envelope.id);
        for join in joined.into_iter().rev() {
            let next = (join.envelope.from.clone(), join.envelope.id);
            let _ = join.joined.send(Joined::Superseded(by.0, by.1));
            by = next;
        }
        let (answering, answered) = tokio::sync::oneshot::channel();
        let _ = last.joined.send(Joined::Answer(Box::new(ran), answering));
        let superseded = self.superseded(incoming, own, (&by.0, by.1), msg, seq).await;
        let _ = answered.await;
        superseded
    }

    /// Settle `incoming`, the message at stream sequence `seq`, whose prompt `by`'s message `id` steered and took the
    /// answer of. Its sender is told so, unless it is a post, another ant's reply to the ant's own message, or a
    /// message whose answer goes elsewhere than its sender, or the thread has no turns left: those end with no reply
    /// ([`NoReplyWhy::Steered`]).
    async fn superseded(
        &self,
        incoming: &Incoming,
        own: bool,
        (by, id): (&UserId, Uuid),
        msg: &jetstream::Message,
        seq: u64,
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
            return self
                .end_unanswered(incoming, (NoReplyWhy::Steered, Some(reason)), String::new(), None, msg, seq)
                .await;
        };
        let reply = Reply::Message(Box::new(notice.as_notice(NoticeKind::Steered)));
        if let Err(e) = self.attempts.replied(seq, envelope, &reply).await {
            tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "recording the reply");
        }
        self.deliver(incoming, &reply, msg, seq).await
    }

    /// Note in the ant's timeline on `thread` that the prompt `measured` ended `how`, its session `rebuilt` or not,
    /// with the tool requests its turn asked. Stored before the message is settled; failing to, it only logs.
    async fn note_end(&self, thread: &ThreadId, measured: &Measured, how: PromptEnd, rebuilt: bool) {
        let end = TimelineEnd {
            how,
            duration_ms: u64::try_from(measured.took.as_millis()).unwrap_or(u64::MAX),
            tokens: measured.tokens,
            cost: measured.cost,
            asked: self.approvals.map_or(0, |approvals| approvals.asked(thread)),
            rebuilt,
            calls: measured.calls.clone(),
            sense_calls: measured.sense_calls,
        };
        let entries = vec![TimelineEntry::Ended(end)];
        let record = Timeline::new(self.ant.clone(), thread.clone(), measured.message, entries, 0);
        crate::monitoring::timeline::publish(self.js, &record).await;
    }

    /// Note that the prompt `measured` failed with `e`, after a tool call started if `worked`, when it ran at all: not
    /// when the harness did not start, nor when it turned the prompt away for want of signing in or at a provider
    /// limit before any tool ran.
    async fn note_failed(&self, thread: &ThreadId, measured: &Measured, e: &anyhow::Error, worked: bool) {
        if !e.is::<StartFailed>() && (worked || !(e.is::<SignedOut>() || e.is::<Limited>())) {
            self.note_end(thread, measured, PromptEnd::Failed, false).await;
        }
    }

    /// Settle the message at stream sequence `seq` after its prompt failed with `e`, which is returned unless the
    /// failure was escalated. `worked` says whether tools may have run.
    async fn prompt_failed(
        &self,
        e: anyhow::Error,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        cut_off: bool,
        worked: bool,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        // Nothing ran: the harness turned the prompt away for want of signing in, so the attempt never started, and
        // the message can be withdrawn while it waits.
        if e.is::<SignedOut>() && !cut_off && !worked {
            self.attempts.forget(seq).await;
            self.attempts.release(envelope).await;
            return Err(e);
        }
        // Nothing ran: the harness did not start. Its retry, or the message held for the harness to start, carries no
        // cut-off warning.
        if e.is::<StartFailed>() {
            if !cut_off {
                self.attempts.forget(seq).await;
            }
            return Err(e);
        }
        // A provider limit spends no attempt. The marker stays when tools may have run, to warn the retry.
        if e.is::<Limited>() && !cut_off && !worked {
            self.attempts.forget(seq).await;
            self.attempts.release(envelope).await;
            return Err(e);
        }
        // Held for a sign-in or a provider limit after tools may have run: withdrawable until its retry, as after a
        // failed attempt. A fallback turning it away hands it on at once instead.
        if e.is::<SignedOut>() || e.is::<Limited>() {
            if !e.is::<OnFallback>() {
                let held = if e.is::<SignedOut>() { Hold::SignIn } else { Hold::ProviderLimit };
                self.attempts.retrying(envelope, Some(held)).await;
            }
            return Err(e);
        }
        // Not even a new session holds the prompt: no further attempt can, so the owner decides.
        if e.is::<ContextOverflow>() {
            self.escalate(incoming, &e.to_string(), EscalationReason::ContextWindow, seq, None).await?;
            bus::double_ack(self.client, msg).await?;
            self.attempts.forget(seq).await;
            status::state(self.client, self.ant, &envelope.thread_id, State::Failed);
            return Ok(());
        }
        Err(e)
    }

    /// Whether the thread of `incoming` has no turns left for its answer, given what was read of a post's thread.
    fn turns_spent(&self, incoming: &Incoming, read: Option<&Context>, humans: &Humans) -> bool {
        let envelope = &incoming.envelope;
        let spent = match (&incoming.post, read) {
            (Some(posted), Some(read)) => posted.post.reply(self.ant.clone(), "", &read.later, humans).err(),
            _ => envelope.reply(self.ant.clone(), "", self.humans.sender_role(envelope)).err(),
        };
        matches!(spent, Some(ProtoError::BudgetExhausted(_)))
    }

    /// Escalates the thread instead of prompting when it has no turns (`spent`) or allowance left; true if so.
    async fn out_of_turns(&self, incoming: &Incoming, spent: bool, msg: &jetstream::Message, seq: u64) -> Result<bool> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        if spent {
            // Out of turns: escalate the thread instead of spending another prompt.
            self.escalate(incoming, "Turn budget spent", EscalationReason::TurnBudget, seq, None).await?;
            bus::double_ack(self.client, msg).await?;
            self.attempts.forget(seq).await;
            return Ok(true);
        }
        match self.ledger.reached(thread).await {
            Ok(Some(cap)) => {
                // Out of allowance: the same, until a human or the escalation's leader answers, or the day ends.
                self.escalate(incoming, &cap, EscalationReason::Cap, seq, None).await?;
                bus::double_ack(self.client, msg).await?;
                self.attempts.forget(seq).await;
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(e) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "reading what the thread spent");
                Ok(false)
            }
        }
    }

    /// Whether the session called `complete` in the turn on `incoming`, and why: never for a message always answered,
    /// which the tool refuses, so one taken anyway is ignored.
    fn completed(&self, incoming: &Incoming) -> Option<crate::mcp::Completed> {
        let completed = self.mcp?.take_completed(&incoming.envelope.thread_id)?;
        if let Some(what) = self.always_answered(incoming) {
            let thread = &incoming.envelope.thread_id;
            tracing::warn!(%thread, what, "ignoring complete on a message always answered; replying");
            return None;
        }
        Some(completed)
    }

    /// The prompt for `incoming`, given what was read of a post's thread; `own` when it is another ant's reply to the
    /// ant's own message ([`Inbox::replies_to_own`]), true if the thread has no turns left.
    async fn prompt_for(
        &self,
        incoming: &Incoming,
        read: Option<&Context>,
        humans: &Humans,
        cut_off: bool,
        own: Option<bool>,
    ) -> String {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let late = match self.mcp {
            Some(mcp) => mcp.late_on(thread).await,
            None => 0,
        };
        let text = match (&incoming.post, read) {
            (Some(posted), Some(read)) => self.channel_prompt(posted, read, humans, own.is_some()).await,
            _ => match (&envelope.summary, escalation::group_note(self.ant, envelope)) {
                (Some(summary), _) => format!("{}\n\n{}", summary_note(summary), envelope.text),
                (None, Some(note)) => format!("{note}\n\n{}", envelope.text),
                (None, None) => channel::quoted(envelope.quote.as_ref(), &envelope.text),
            },
        };
        let text = match &incoming.resume {
            Some(channel) => {
                let left = envelope.turn_budget.saturating_sub(1);
                format!("{text}\n\n{}", channel::resume_note(channel, thread, left))
            }
            None => text,
        };
        let text = match self.answer_line(envelope, humans).await {
            Some(line) => format!("{line}\n\n{text}"),
            None if let Some(spent) = own => {
                format!("{}\n\n{text}", own_reply_note(&envelope.from, incoming.post.is_some(), spent))
            }
            None => text,
        };
        let text = prompt_text(&text, cut_off, late);
        match incoming.steered {
            Steered::Interrupted => format!("{}\n\n{text}", steer_note(&envelope.from)),
            Steered::Joining => format!("{}\n\n{text}", join_note(&envelope.from)),
            Steered::No | Steered::Ahead => text,
        }
    }

    /// The `[ants]` line telling that `envelope`, a human's message or one an assistant sent for its human, verified
    /// on arrival, answers a question the ant asked on the thread ([`crate::stream::questions::answer_line`]); none for any
    /// other message, which is prompted as an ordinary one.
    async fn answer_line(&self, envelope: &Envelope, humans: &Humans) -> Option<String> {
        let human = envelope.on_behalf_of.is_some() || envelope.sender_role(humans).is_human();
        if envelope.answer.is_none() || !human {
            return None;
        }
        match crate::stream::questions::asked(self.js, self.ant, self.reading, envelope).await {
            Ok(question) => crate::stream::questions::answer_line(envelope, question.as_ref()?),
            Err(e) => {
                let thread = &envelope.thread_id;
                tracing::warn!(%thread, error = format!("{e:#}"), "reading back the question a message answers");
                None
            }
        }
    }

    /// What asks the ant's provider for its brief, should the prompt for `incoming` be the first of a session: none
    /// without a provider taking brief requests. Asked as the provider's tools are called ([`crate::mcp::Mcp::caller`],
    /// the thread's origin noted as the turn began).
    async fn briefing(&self, incoming: &Incoming) -> Option<Briefing> {
        let mcp = self.mcp?;
        let provider = mcp.provider()?.clone();
        let thread = &incoming.envelope.thread_id;
        let origin = mcp.origin(thread)?;
        Briefing::new(provider, mcp.caller().await, thread.clone(), origin)
    }

    /// What the prompt for `incoming`, on a channel's thread, adds should it be the first of a session: the channel's
    /// description, as the post's entry said, or as stored now for the escalation leader's answer.
    async fn channel_note(&self, incoming: &Incoming) -> Option<String> {
        let (channel, description) = match (&incoming.post, &incoming.resume) {
            (Some(posted), _) => (&posted.post.channel, posted.description.clone()),
            (None, Some(channel)) => (channel, self.stored_description(channel).await),
            (None, None) => return None,
        };
        description.map(|description| channel::description_note(channel, &description))
    }

    async fn stored_description(&self, channel: &ants_proto::ChannelId) -> Option<String> {
        let read = async {
            let kv = self.js.get_key_value(ants_proto::CHANNELS_BUCKET).await?;
            anyhow::Ok(match channel::load(&kv, channel).await? {
                channel::Stored::Entry(entry, _) => entry.description,
                _ => None,
            })
        };
        read.await.unwrap_or_else(|e| {
            tracing::warn!(%channel, error = format!("{e:#}"), "reading the description of a channel");
            None
        })
    }

    /// Run the prompt `text` for `incoming`, streaming its status, recording its spend, renewing the message's ack wait
    /// and cancelling it on request or at a cap.
    async fn run_prompt(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        seq: u64,
        text: String,
        mut flush: tokio::time::Interval,
    ) -> Ran {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let (spend_tx, mut spend) = tokio::sync::mpsc::unbounded_channel();
        let (stop_tx, stop) = tokio::sync::oneshot::channel();
        let (mut stop_tx, mut reached) = (Some(stop_tx), None);
        let (progress_tx, mut streamed) = tokio::sync::mpsc::channel(status::BUFFER);
        let mut status = Status::new(self.client, self.ant, thread, self.redactor);
        let every = self.timeline_every;
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut calls = crate::monitoring::timeline::Collector::new(self.ant, thread, envelope.id, self.redactor);
        let history = self.history_of(incoming, seq);
        let (brief, channel) = tokio::join!(self.briefing(incoming), self.channel_note(incoming));
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        let (steer_tx, steers) = tokio::sync::mpsc::channel(STEERS);
        let (mut measured, began) = Measured::new(envelope.id);
        let prompt = Prompt {
            text,
            limits: self.limits,
            spend: Some(spend_tx),
            stop: Some(stop),
            progress: Some(progress_tx),
            history: Some(history.history()),
            attachments: Attached::new(self.js.clone(), incoming.attached(), crate::files::stored(msg)),
            steering: Some(Steering { ready: ready_tx, steers }),
            brief,
            channel,
            began: Some(began),
        };
        let mut ready = futures::FutureExt::fuse(ready);
        let mut joins = None;
        let mut taking = futures::stream::FuturesUnordered::new();
        let mut joined = Vec::new();
        let (cancellable, mut cancel) = self.cancels.register(thread);
        let (mut cancelled_by, mut cut_by_cancel) = (None, false);
        let prompting = self.metrics.prompting();
        let fallback = self.fallback(tokio::time::Instant::now());
        if let Some(i) = fallback {
            tracing::info!(%thread, fallback = self.pool.fallbacks().get(i).map(String::as_str), "prompting a fallback");
        }
        let prompt = self.telling_mcp_sign_ins(incoming, self.pool.run_on(thread, prompt, fallback));
        tokio::pin!(prompt);
        let mut progress = self.progress();
        // Whether a tool call started, so tools may have run; streamed text, such as a harness's error, runs nothing.
        let mut worked = false;
        let reply = loop {
            tokio::select! {
                r = &mut prompt => break r,
                Some(update) = streamed.recv() => {
                    worked |= matches!(update, StatusUpdate::Tool { .. });
                    calls.push(&update, chrono::Utc::now());
                    status.push(update);
                }
                _ = flush.tick() => status.flush_settled(),
                () = stored_every(&calls, &mut tick) => {
                    if let Some(record) = calls.take() {
                        crate::monitoring::timeline::store(self.js, record);
                    }
                }
                Ok(()) = &mut ready => joins = Some(self.steers.register(thread)),
                Some(join) = next_join(&mut joins) => taking.extend(steer(&steer_tx, join)),
                Some((took, join)) = futures::StreamExt::next(&mut taking) => self.took(took, join, &mut joined),
                Some(by) = cancel.recv(), if cancelled_by.is_none() => {
                    tracing::info!(%thread, by = %by.by, why = ?by.why, "cancelling the prompt");
                    if let Some(stop) = stop_tx.take() {
                        let _ = stop.send(format!("Cancelled by {}", by.by));
                        cut_by_cancel = true;
                    }
                    cancelled_by = Some(by);
                }
                Some(spent) = spend.recv() => {
                    if let Some(cap) = self.record(envelope, &spent, &mut measured).await {
                        if let Some(stop) = stop_tx.take() {
                            let _ = stop.send(cap.clone());
                        }
                        reached = Some(cap);
                    }
                }
                _ = progress.tick() => {
                    if let Err(e) = msg.ack_with(AckKind::Progress).await {
                        tracing::warn!(error = %e, "progress ack");
                    }
                }
            }
        };
        while let Ok(update) = streamed.try_recv() {
            worked |= matches!(update, StatusUpdate::Tool { .. });
            calls.push(&update, chrono::Utc::now());
            status.push(update);
        }
        measured.end(&calls);
        status.flush();
        while let Some(record) = calls.take() {
            crate::monitoring::timeline::store(self.js, record);
        }
        drop(steer_tx);
        self.joins_ended(joins, taking, &mut joined).await;
        drop((cancellable, prompting));
        Ran { reply, cancelled_by, cut_by_cancel, worked, reached, spend, fallback, measured, joined }
    }

    /// The history of the thread of `incoming` before stream sequence `seq`, for a harness rebuilding a lost session.
    fn history_of(&self, incoming: &Incoming, seq: u64) -> crate::stream::history::Thread {
        crate::stream::history::Thread {
            js: self.js.clone(),
            ant: self.ant.clone(),
            thread: incoming.envelope.thread_id.clone(),
            channel: incoming.post.as_ref().map(|posted| posted.post.channel.clone()).or(incoming.resume.clone()),
            before: seq,
            reading: self.reading,
            home: Some(self.home.to_owned()),
        }
    }

    /// Once the prompt ended: turn away the messages still to join it, and note what came of those handed to it.
    async fn joins_ended(
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
    fn took(&self, took: bool, join: Join, joined: &mut Vec<Join>) {
        let envelope = &join.envelope;
        tracing::info!(thread = %envelope.thread_id, from = %envelope.from, took, "steering the running prompt");
        if !took {
            let _ = join.joined.send(Joined::Refused);
            return;
        }
        if let Some(mcp) = self.mcp {
            mcp.steer_to(envelope, join.carried.clone(), join.stored);
        }
        joined.push(join);
    }

    /// The prompt for a channel post, with what was read of its thread: the posts not shown before, and the turns left,
    /// unless the answer is not posted, as for a post answering one of the ant's `own`.
    async fn channel_prompt(&self, posted: &Posted, read: &Context, humans: &Humans, own: bool) -> String {
        let post = &posted.post;
        let left = post.turns_left(&read.later, humans).saturating_sub(1);
        let mut text = match &posted.triage {
            None => channel::prompt(self.ant, post, read, (!own).then_some(left)),
            Some(entry) => {
                let colony = (self.colony)().await.unwrap_or_else(|e| {
                    tracing::warn!(error = format!("{e:#}"), "reading the colony; members show as offline");
                    Vec::new()
                });
                channel::triage_prompt(post, entry, &colony, humans, read, left)
            }
        };
        if posted.owned {
            let _ = write!(text, "\n\n{}", channel::owner_note(&post.channel));
        }
        text
    }

    /// Record the spend reported as a prompt ended, such as its tokens, in `measured`, keeping the first cap it reached
    /// in `reached`.
    async fn record_ended(
        &self,
        envelope: &Envelope,
        mut spend: tokio::sync::mpsc::UnboundedReceiver<Spend>,
        reached: &mut Option<String>,
        measured: &mut Measured,
    ) {
        while let Ok(spent) = spend.try_recv() {
            if let Some(cap) = self.record(envelope, &spent, measured).await {
                reached.get_or_insert(cap);
            }
        }
    }

    /// Count what the harness spent on the thread of `envelope`, warning a human of each cap nearly reached, and add
    /// its tokens and cost to `measured`. Returns the cap reached, if any.
    async fn record(&self, envelope: &Envelope, spent: &Spend, measured: &mut Measured) -> Option<String> {
        let thread = &envelope.thread_id;
        let recorded = self.ledger.record(thread, spent).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing what the thread spent");
            Recorded::default()
        });
        for warning in &recorded.warnings {
            self.warn(envelope, warning).await;
        }
        measured.tokens += spent_tokens(spent);
        measured.cost += recorded.cost;
        recorded.reached
    }

    /// Tell a human that spend on the thread of `envelope` passed the warning share of a cap: the thread's owner, else
    /// the default human, of a thread cap, and the default human of a daily cap. On a thread of its own, so that
    /// `ants ask --wait` on the thread still waits for the answer.
    async fn warn(&self, envelope: &Envelope, warning: &Warning) {
        let thread = &envelope.thread_id;
        let owner = if warning.daily() { None } else { self.humans.human_owner(envelope.owner.as_ref()) };
        let to = owner.unwrap_or_else(|| self.humans.default_human().clone());
        tracing::info!(%thread, %to, cap = warning.key, spent = warning.figure, "spend nearing a cap; warning a human");
        let notice = Envelope::new(self.ant.clone(), to, warning.text(self.ant, thread))
            .as_notice(ants_proto::NoticeKind::Spend);
        if let Err(e) = bus::publish_json(self.js, notice.subject(), &notice, None).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "warning a human of spend nearing a cap");
        }
    }

    /// Escalate the thread for `reason` up its origin chain, in the end to its owner, else the default human
    /// ([`Escalator::escalate`]): `why` it needs a decision, and the message or post it was on, and an `unposted`
    /// answer. Its message id, from the stream sequence `seq` of the message, keeps a redelivery from escalating twice.
    pub(super) async fn escalate(
        &self,
        incoming: &Incoming,
        why: &str,
        reason: EscalationReason,
        seq: u64,
        unposted: Option<&str>,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let human = self.humans.owner_of(envelope);
        let last = match &incoming.post {
            Some(posted) => format!("Last post from {} in #{}", envelope.from, posted.post.channel),
            None => format!("Last message from {}", envelope.from),
        };
        let mut text = format!("{why}; needs a decision. {last}:\n{}", envelope.text);
        if let Some(answer) = unposted {
            let _ = write!(text, "\n\nUnposted answer from {}:\n{answer}", self.ant);
        }
        let mut notice = envelope.follow_up(self.ant.clone(), human, text);
        notice.in_reply_to = Some(envelope.id);
        let id = format!("escalation-{}-{seq}", self.ant);
        let stuck = Stuck::new(reason, notice, id, Some(envelope));
        let channel = incoming.post.as_ref().map(|posted| posted.post.channel.clone());
        let escalated = self.escalator.escalate(Stuck { channel, ..stuck }).await?;
        tracing::info!(thread = %envelope.thread_id, why, %escalated, "escalated");
        Ok(())
    }
}

/// Why a turn that ended sends no reply, if it sends none: the session called `complete`, or the message was another
/// ant's reply to the ant's `own`.
fn unanswered(completed: Option<crate::mcp::Completed>, own: bool) -> Option<(NoReplyWhy, Option<String>)> {
    match completed {
        Some(completed) => Some((NoReplyWhy::Completed, completed.reason)),
        None => own.then_some((NoReplyWhy::ReplyToOwn, None)),
    }
}

/// How a prompt that ran to its end ended: cancelled by a human (not a steering message or restart), cut by a limit
/// or cap, else with no reply or replied.
fn end_of<T, U>(cancelled_by: Option<&Cancel>, limit: &Option<T>, unanswered: &Option<U>) -> PromptEnd {
    if cancelled_by.is_some_and(|cancel| matches!(cancel.why, Why::Cancel | Why::CancelFor(_))) {
        PromptEnd::Cancelled
    } else if limit.is_some() {
        PromptEnd::Capped
    } else if unanswered.is_some() {
        PromptEnd::NoReply
    } else {
        PromptEnd::Replied
    }
}

/// The tokens `spent` reports.
fn spent_tokens(spent: &Spend) -> u64 {
    match spent {
        Spend::Tokens(n) => *n,
        Spend::Cost { .. } => 0,
    }
}

/// The `[ants]` line that turns a human's message carrying `summary` into a request for a summary of the entries its
/// text carries.
fn summary_note(summary: &SummaryRequest) -> String {
    let what = match &summary.of {
        SummaryOf::Thread(thread) => format!(
            "a TL;DR of thread {thread} from the entries below: what was asked, decided and done, and what waits on whom"
        ),
        SummaryOf::Since(since) => format!(
            "a digest of the threads with entries since {} from the entries below: what finished, what is stuck, and \
             what waits on the human",
            since.format("%Y-%m-%d %H:%M UTC")
        ),
    };
    format!(
        "[ants] A human asks for {what}. Answer with at most 120 words of plain text; use no tools and message no \
         one: the reply is the summary."
    )
}

/// The `[ants]` line starting the prompt of `from`'s reply to a message, or a `post`, the ant sent: its answer is not
/// sent back.
pub(super) fn own_reply_note(from: &UserId, post: bool, spent: bool) -> String {
    let (what, going_on) = if post {
        ("a post this ant made", "post_channel on this thread (or send_message)")
    } else {
        ("a message this ant sent", "send_message (or post_channel on a channel)")
    };
    if spent {
        return format!(
            "[ants] {from} replied to {what}; the answer to this prompt is not sent back, and the thread has no turns \
             left: escalate hands it on when it needs more; complete ends the turn."
        );
    }
    format!(
        "[ants] {from} replied to {what}; the answer to this prompt is not sent back. {going_on} continues the \
         exchange; complete ends the turn."
    )
}

/// The line starting the prompt of a steering message that interrupted the prompt running on its thread.
fn steer_note(by: &UserId) -> String {
    format!(
        "[ants] {by} interrupted the previous prompt on this thread with this message; tool calls it had in flight \
         were cancelled, so check what they left before repeating any."
    )
}

/// The line starting a steering message that joined the prompt running on its thread.
fn join_note(by: &UserId) -> String {
    format!(
        "[ants] {by} sent this message while the prompt ran, to steer it. The final answer goes in reply to this \
         message, so it answers this message together with what came before it."
    )
}

/// The prompt for a message: warned when an earlier attempt at it was cut off, and noting the `late` replies to
/// `ask_ant` that wait on its thread for `check_inbox`.
pub(super) fn prompt_text(text: &str, cut_off: bool, late: usize) -> String {
    let late = match late {
        0 => None,
        1 => Some("[ants] 1 late reply to ask_ant is waiting on this thread; call check_inbox to read it.".to_owned()),
        n => Some(format!(
            "[ants] {n} late replies to ask_ant are waiting on this thread; call check_inbox to read them."
        )),
    };
    let cut_off = cut_off.then(|| CUT_OFF.to_owned());
    cut_off.into_iter().chain(late).chain([text.to_owned()]).collect::<Vec<_>>().join("\n\n")
}

/// When the next record of a prompt's tool-call timeline is due: at its next tick once something changed.
async fn stored_every(calls: &crate::monitoring::timeline::Collector<'_>, tick: &mut tokio::time::Interval) {
    if calls.pending() {
        tick.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}

/// The next message joining the running prompt, once it takes them.
/// Hand `join` to the running prompt through `steers`: what the harness made of it, or None when the prompt takes no
/// more, `join` then turned away.
fn steer(steers: &tokio::sync::mpsc::Sender<Steer>, join: Join) -> Option<impl Future<Output = (bool, Join)> + use<>> {
    let (accepted, took) = tokio::sync::oneshot::channel();
    if steers.try_send(Steer { text: join.text.clone(), accepted }).is_err() {
        let _ = join.joined.send(Joined::Refused);
        return None;
    }
    Some(async move { (took.await.unwrap_or(false), join) })
}

async fn next_join(
    joins: &mut Option<(super::steering::Joinable<'_>, tokio::sync::mpsc::UnboundedReceiver<Join>)>,
) -> Option<Join> {
    match joins {
        Some((_, rx)) => rx.recv().await,
        None => std::future::pending().await,
    }
}
