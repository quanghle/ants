//! Prompting the harness with a message: running the prompt ([`text`]) with its status, spend ([`measured`]) and
//! cancels, and the notices its reply carries, escalating what the ant cannot settle.

use std::collections::HashSet;
use std::fmt::Write as _;

use ants_proto::{
    Envelope, Error as ProtoError, EscalationReason, Hold, Humans, NoReplyWhy, PromptEnd, Stage, State, StatusUpdate,
    SummaryRequest, UserId,
};
use anyhow::Result;
use async_nats::jetstream;

use crate::bus;
use crate::channel::{Context, Earlier};
use crate::delivery::attempts::{Marker, Reply};
use crate::escalation::Stuck;
use crate::files::Attached;
use crate::harness::{Prompt, SignedOut, Spend, Steering};
use crate::limits::breaker::StartFailed;
use crate::limits::fallback::OnFallback;
use crate::limits::files;
use crate::limits::overflow::ContextOverflow;
use crate::limits::ratelimit::Limited;
use crate::monitoring::status::{self, Status};
use crate::sync::MutexExt as _;
use crate::worktrees::WorktreeFailed;

use super::control::{Cancel, Why};
use super::delivery::{fit_to, reply_room};
use super::steering::{Join, next_join, steer};
use super::{Handling, Inbox, Incoming, Steered};

mod measured;
pub(super) mod text;

use measured::{Measured, stored_every};

/// How many steers wait at most for the harness to take them into the running prompt.
const STEERS: usize = 8;

/// What came of running a prompt.
pub(super) struct Ran {
    pub(super) reply: Result<crate::harness::Reply>,
    /// The human's cancel of the prompt, if one came.
    pub(super) cancelled_by: Option<Cancel>,
    /// Whether the cancel cut the prompt short.
    pub(super) cut_by_cancel: bool,
    /// Whether a tool call started, so tools may have run.
    pub(super) worked: bool,
    /// The cap the prompt's spend reached.
    pub(super) reached: Option<String>,
    /// The spend still to record.
    pub(super) spend: tokio::sync::mpsc::UnboundedReceiver<Spend>,
    /// The fallback the prompt ran on, `None` for the ant's own harness.
    pub(super) fallback: Option<usize>,
    pub(super) measured: Measured,
    /// The messages that joined the prompt, in order: the last one gets its answer.
    pub(super) joined: Vec<Join>,
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
        h @ Handling { incoming, msg, seq }: Handling<'_>,
        marker: Option<Marker>,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        let thread = &envelope.thread_id;
        let cut_off = match marker {
            Some(Marker::Replied(reply)) => {
                tracing::info!(%thread, "sending the reply an earlier attempt recorded");
                return self.deliver(h, &reply).await;
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
        if self.out_of_turns(h, spent && !own).await? {
            return Ok(());
        }
        self.settle_origin(incoming).await?;
        if !cut_off {
            // Durable before the harness can run anything.
            self.attempts.start(seq, envelope).await?;
        }
        if let Some(why) = self.unheld_review(incoming) {
            return Box::pin(self.decline_review(h, read, &why)).await;
        }

        tracing::info!(%thread, from = %envelope.from, cut_off, own, "prompting");
        status::state(self.client, self.ant, thread, State::Working);

        // A message joining a running prompt takes over the turn of the message it started with.
        let joining = incoming.steered == Steered::Joining;
        self.warn_unnarrowed(incoming);
        let _narrowed = self.narrow_turn(envelope, joining);
        let _turn = self.mcp.filter(|_| !joining).map(|mcp| begin_turn(mcp, incoming, msg));
        let _asking = self.approvals.filter(|_| !joining).map(|approvals| approvals.begin(envelope));
        let text = self.prompt_for(incoming, read.as_ref(), &humans, cut_off, own.then_some(spent)).await;
        let (text, _checkouts) = self.review_checkouts(incoming, msg, joining, text).await;
        // The answer of a prompt a message joined is sent while the turn of the one it started with lasts.
        let mut _answering = None;
        let ran = if joining {
            let Some((ran, answering)) = self.joined(h, own, cut_off, text).await? else {
                return Ok(());
            };
            _answering = Some(answering);
            ran
        } else {
            let flush =
                tokio::time::interval_at(tokio::time::Instant::now() + status::FLUSH_EVERY, status::FLUSH_EVERY);
            let ran = Box::pin(self.run_prompt(h, text, flush)).await;
            if !ran.joined.is_empty() {
                return Box::pin(self.hand_on(h, own, ran, cut_off)).await;
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
                return self.prompt_failed(h, e, cut_off, worked).await;
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
        let text = fit_to(self.redactor.redact(&text).into_owned(), reply_room(self.client));
        if let Some(unanswered) = unanswered {
            return self.end_unanswered(h, unanswered, text, limit.as_ref()).await;
        }
        let Some(mut reply) = Box::pin(self.reply_for(h, read, text, limit.as_ref())).await? else {
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
        self.record_reply(seq, envelope, &reply).await;
        self.deliver(h, &reply).await
    }

    /// Record `reply` to `envelope`, the message at stream sequence `seq`, with its attempt marker, so a redelivery
    /// sends it rather than prompting again; refused, as while the hub's store is full, it is kept on the machine
    /// instead, for the retry to send.
    async fn record_reply(&self, seq: u64, envelope: &Envelope, reply: &Reply) {
        let thread = &envelope.thread_id;
        if let Err(e) = self.attempts.replied(seq, envelope, reply).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "recording the reply");
            if let Err(e) = self.answered.keep_unsent(envelope, reply).await {
                tracing::warn!(%thread, error = format!("{e:#}"), "keeping the reply");
            }
        }
    }

    /// Narrow the tool rules on the thread of `envelope` to those it carries while the returned guard is held, unless
    /// it joins a running prompt, whose rules hold. The turn of a run's rating invitation, which the inbox checked
    /// ([`Envelope::rating_of`]), rates the run whatever the thread's rules.
    fn narrow_turn(&self, envelope: &Envelope, joining: bool) -> Option<crate::permission::Narrowing> {
        let policy = self.pool.launch().policy();
        let narrowed = (!joining).then(|| policy.narrow(&envelope.thread_id, envelope.allow.as_deref()));
        if envelope.rating_of.is_some() {
            // The tool rule letting the invitation rate the run on a narrowed thread.
            policy.widen(&envelope.thread_id, &format!("other:{}", crate::mcp::RATE_TRAIL));
        }
        narrowed
    }

    /// Warn, once per sender, that the tool rules a message not joining a prompt narrows the thread to do not reach a
    /// harness running its tools unasked. A review's do ([`crate::harness::Launch::for_review`]).
    fn warn_unnarrowed(&self, incoming: &Incoming) {
        let (envelope, launch) = (&incoming.envelope, self.pool.launch());
        if incoming.steered != Steered::Joining
            && envelope.allow.as_deref().is_some_and(|allow| !crate::harness::allows_all(allow))
            && !ants_proto::is_review(envelope.allow.as_deref())
            && launch.bypasses_permissions()
            && self.running.unnarrowed.first(&envelope.from)
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

    /// Settle the message at stream sequence `seq` after its prompt failed with `e`, which is returned unless the
    /// failure was escalated. `worked` says whether tools may have run.
    pub(super) async fn prompt_failed(
        &self,
        Handling { incoming, msg, seq }: Handling<'_>,
        e: anyhow::Error,
        cut_off: bool,
        worked: bool,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        // The machine out of open files is no fault of the message: it waits as for a provider limit.
        let e = match files::held(self.pool.launch().harness_name(), &e).filter(|_| !e.is::<OnFallback>()) {
            Some(held) => anyhow::Error::new(held),
            None => e,
        };
        // Nothing ran: the harness turned the prompt away for want of signing in, so the attempt never started, and
        // the message can be withdrawn while it waits.
        if e.is::<SignedOut>() && !cut_off && !worked {
            self.attempts.forget(seq).await;
            self.attempts.release(envelope).await;
            return Err(e);
        }
        // Nothing ran: the harness did not start, or its thread's worktree could not be made. Its retry, or the message
        // held for the harness to start, carries no cut-off warning.
        if e.is::<StartFailed>() || e.is::<WorktreeFailed>() {
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
    async fn out_of_turns(&self, Handling { incoming, msg, seq }: Handling<'_>, spent: bool) -> Result<bool> {
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

    /// Run the prompt `text` for `incoming`, streaming its status, recording its spend, renewing the message's ack wait
    /// and cancelling it on request or at a cap.
    async fn run_prompt(
        &self,
        Handling { incoming, msg, seq }: Handling<'_>,
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
        let every = self.pacing.timeline_every;
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
            from: Some(envelope.from.to_string()),
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
        let (cancellable, mut cancel) = self.running.cancels.register(thread);
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
                Ok(()) = &mut ready => joins = Some(self.running.steers.register(thread)),
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
                _ = progress.tick() => bus::progress(msg).await,
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

    /// Escalate the thread for `reason` up its origin chain, in the end to its owner, else the default human
    /// ([`Escalator::escalate`](crate::escalation::Escalator::escalate)): `why` it needs a decision, and the message or
    /// post it was on, and an `unposted` answer. Its message id, from the stream sequence `seq` of the message, keeps a
    /// redelivery from escalating twice.
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
