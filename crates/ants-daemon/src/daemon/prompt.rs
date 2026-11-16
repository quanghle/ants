//! Prompting the harness with a message: the prompt's text, running it with its status, spend and cancels, and the
//! notices its reply carries, escalating what the ant cannot settle.

use std::collections::HashSet;
use std::fmt::Write as _;

use ants_proto::{
    Envelope, Error as ProtoError, EscalationReason, Hold, Humans, NoReplyWhy, State, StatusUpdate, SummaryOf,
    SummaryRequest, UserId,
};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};

use crate::attempts::{Marker, Reply};
use crate::breaker::StartFailed;
use crate::bus;
use crate::caps::{Recorded, Warning};
use crate::channel::{self, Context, Earlier};
use crate::escalation::{self, Stuck};
use crate::fallback::OnFallback;
use crate::files::Attached;
use crate::harness::{Prompt, SignedOut, Spend};
use crate::overflow::ContextOverflow;
use crate::ratelimit::Limited;
use crate::status::{self, Status};
use crate::sync::MutexExt as _;

use super::control::Cancel;
use super::{Inbox, Incoming, Posted, Steered};

/// Put before the text of a message whose earlier attempt started and stored no reply.
const CUT_OFF: &str = "[ants] An earlier attempt at this message was cut off before its reply was stored, so tools it \
ran may already have taken effect. The session history may show what ran. Check the current state before repeating \
any action.";

/// What came of running a prompt.
struct Ran {
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
    /// The tokens the prompt spent so far, as the harness reported them.
    tokens: u64,
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

impl Inbox<'_> {
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

    /// Add the prompt just completed to the ant's identity record; a failure only warns.
    async fn count_prompt(&self) {
        if let Some(identity) = self.identity
            && let Err(e) = identity.count_prompt().await
        {
            tracing::warn!(error = format!("{e:#}"), "counting the prompt in the ant's identity");
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
        if !cut_off {
            // Durable before the harness can run anything.
            self.attempts.start(seq, envelope).await?;
        }

        tracing::info!(%thread, from = %envelope.from, cut_off, own, "prompting");
        status::state(self.client, self.ant, thread, State::Working);

        let launch = self.pool.launch();
        if envelope.allow.as_deref().is_some_and(|allow| !crate::harness::allows_all(allow))
            && launch.bypasses_permissions()
            && self.unnarrowed.first(&envelope.from)
        {
            tracing::warn!(
                %thread,
                from = %envelope.from,
                harness = launch.harness_name(),
                "the thread's tool rules are narrowed, but the harness runs its tools unasked: the narrowing does not \
                 apply to them (warned once per sender)"
            );
        }
        let _narrowed = launch.policy().narrow(thread, envelope.allow.as_deref());
        let carried = incoming.attached().into_iter().map(|(handle, _)| handle).collect();
        let _turn = self.mcp.map(|mcp| mcp.begin(envelope).carrying(carried, crate::files::stored(msg)));
        let _asking = self.approvals.map(|approvals| approvals.begin(envelope));
        let flush = tokio::time::interval_at(tokio::time::Instant::now() + status::FLUSH_EVERY, status::FLUSH_EVERY);
        let text = self.prompt_for(incoming, read.as_ref(), &humans, cut_off, own.then_some(spent)).await;
        let Ran { reply, cancelled_by, cut_by_cancel, worked, mut reached, spend, fallback, tokens } =
            self.run_prompt(incoming, msg, seq, text, flush).await;
        // While the turn lasts: it ends with `_turn`.
        let completed = self.completed(incoming);
        let reply = match reply {
            // What a human cancelled is answered as it stands, never tried again.
            Err(e) if cancelled_by.is_some() => {
                tracing::warn!(%thread, error = format!("{e:#}"), "cancelled prompt failed");
                crate::harness::Reply { text: String::new(), cut: None }
            }
            Err(e) => return self.prompt_failed(e, incoming, msg, seq, cut_off, worked).await,
            Ok(reply) => reply,
        };
        self.count_prompt().await;
        let fallback_notice = self.served_by(fallback);
        let tokens = tokens + self.record_ended(envelope, spend, &mut reached).await;

        // A human's cancel is no limit: the reply tells of it, and nothing is escalated for it.
        let cut = reply.cut.is_some() && !cut_by_cancel;
        let limit = reply.cut.filter(|_| !cut_by_cancel).or(reached);
        let limit_notice =
            limit.as_ref().map(|l| format!("[ants] {l}{}", if cut { "; the prompt was cancelled." } else { "." }));
        let cancelled = cancelled_by.is_some();
        let cancel_notice = cancelled_by.map(|cancel| cancel.notice(self.ant));
        let [branch_notice, publish_notice] = self.branch_notices(envelope, msg, cancelled).await;
        let notice = [fallback_notice, limit_notice, cancel_notice, branch_notice, publish_notice];
        let notice = notice.into_iter().flatten().collect::<Vec<_>>().join("\n");
        let text = [reply.text, notice].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n\n");
        let text = self.redactor.redact(&text).into_owned();
        let unanswered = match completed {
            Some(completed) => Some((NoReplyWhy::Completed, completed.reason)),
            None => own.then_some((NoReplyWhy::ReplyToOwn, None)),
        };
        if let Some(unanswered) = unanswered {
            return self.end_unanswered(incoming, unanswered, text, limit.as_ref(), msg, seq).await;
        }
        let Some(mut reply) = self.reply_for(incoming, read, text, limit.as_ref(), msg, seq).await? else {
            return Ok(());
        };
        if let Reply::Message(message) = &mut reply {
            message.summary = envelope.summary.clone().map(|s| SummaryRequest { tokens: Some(tokens), ..s });
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
                (None, None) => envelope.text.clone(),
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
            Steered::No | Steered::Ahead => text,
        }
    }

    /// The `[ants]` line telling that `envelope`, a human's message or one an assistant sent for its human, verified
    /// on arrival, answers a question the ant asked on the thread ([`crate::questions::answer_line`]); none for any
    /// other message, which is prompted as an ordinary one.
    async fn answer_line(&self, envelope: &Envelope, humans: &Humans) -> Option<String> {
        let human = envelope.on_behalf_of.is_some() || envelope.sender_role(humans).is_human();
        if envelope.answer.is_none() || !human {
            return None;
        }
        match crate::questions::asked(self.js, self.ant, self.reading, envelope).await {
            Ok(question) => crate::questions::answer_line(envelope, question.as_ref()?),
            Err(e) => {
                let thread = &envelope.thread_id;
                tracing::warn!(%thread, error = format!("{e:#}"), "reading back the question a message answers");
                None
            }
        }
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
        let mut stop_tx = Some(stop_tx);
        let mut reached = None;
        let (progress_tx, mut streamed) = tokio::sync::mpsc::channel(status::BUFFER);
        let mut status = Status::new(self.client, self.ant, thread, self.redactor);
        let every = self.timeline_every;
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut calls = crate::timeline::Collector::new(self.ant, thread, envelope.id, self.redactor);
        let history = crate::history::Thread {
            js: self.js.clone(),
            ant: self.ant.clone(),
            thread: thread.clone(),
            channel: incoming.post.as_ref().map(|posted| posted.post.channel.clone()).or(incoming.resume.clone()),
            before: seq,
            reading: self.reading,
            home: Some(self.home.to_owned()),
        };
        let prompt = Prompt {
            text,
            limits: self.limits,
            spend: Some(spend_tx),
            stop: Some(stop),
            progress: Some(progress_tx),
            history: Some(history.history()),
            attachments: Attached::new(self.js.clone(), incoming.attached(), crate::files::stored(msg)),
        };
        let (cancellable, mut cancel) = self.cancels.register(thread);
        let (mut cancelled_by, mut cut_by_cancel) = (None, false);
        let prompting = self.metrics.prompting();
        let fallback = self.fallback(tokio::time::Instant::now());
        if let Some(i) = fallback {
            tracing::info!(%thread, fallback = self.pool.fallbacks().get(i).map(String::as_str), "prompting a fallback");
        }
        let prompt = self.pool.run_on(thread, prompt, fallback);
        tokio::pin!(prompt);
        let mut progress = self.progress();
        // Whether a tool call started, so tools may have run; streamed text, such as a harness's error, runs nothing.
        let mut worked = false;
        let mut tokens = 0;
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
                        crate::timeline::store(self.js, record);
                    }
                }
                Some(by) = cancel.recv(), if cancelled_by.is_none() => {
                    tracing::info!(%thread, by = %by.by, why = ?by.why, "cancelling the prompt");
                    if let Some(stop) = stop_tx.take() {
                        let _ = stop.send(format!("Cancelled by {}", by.by));
                        cut_by_cancel = true;
                    }
                    cancelled_by = Some(by);
                }
                Some(spent) = spend.recv() => {
                    tokens += spent_tokens(&spent);
                    if let Some(cap) = self.record(envelope, &spent).await {
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
        status.flush();
        while let Some(record) = calls.take() {
            crate::timeline::store(self.js, record);
        }
        drop((cancellable, prompting));
        Ran { reply, cancelled_by, cut_by_cancel, worked, reached, spend, fallback, tokens }
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

    /// Record the spend reported as a prompt ended, such as its tokens, keeping the first cap it reached in `reached`.
    /// The tokens it reported.
    async fn record_ended(
        &self,
        envelope: &Envelope,
        mut spend: tokio::sync::mpsc::UnboundedReceiver<Spend>,
        reached: &mut Option<String>,
    ) -> u64 {
        let mut tokens = 0;
        while let Ok(spent) = spend.try_recv() {
            tokens += spent_tokens(&spent);
            if let Some(cap) = self.record(envelope, &spent).await {
                reached.get_or_insert(cap);
            }
        }
        tokens
    }

    /// Count what the harness spent on the thread of `envelope`, warning a human of each cap nearly reached. Returns
    /// the cap reached, if any.
    async fn record(&self, envelope: &Envelope, spent: &Spend) -> Option<String> {
        let thread = &envelope.thread_id;
        let recorded = self.ledger.record(thread, spent).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing what the thread spent");
            Recorded::default()
        });
        for warning in &recorded.warnings {
            self.warn(envelope, warning).await;
        }
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
        let notice = Envelope::new(self.ant.clone(), to, warning.text(self.ant, thread));
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

/// The line starting the prompt of a human's steer message that interrupted the prompt running on its thread.
fn steer_note(by: &UserId) -> String {
    format!(
        "[ants] {by} interrupted the previous prompt on this thread with this message; tool calls it had in flight \
         were cancelled, so check what they left before repeating any."
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
async fn stored_every(calls: &crate::timeline::Collector<'_>, tick: &mut tokio::time::Interval) {
    if calls.pending() {
        tick.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}
