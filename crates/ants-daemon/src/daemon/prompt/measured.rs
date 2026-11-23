//! What a prompt spent and took: its spend counted against the caps, a human warned of one nearly reached, and its
//! end noted in the ant's timeline.

use std::collections::BTreeMap;

use ants_proto::{CallCount, Envelope, PromptEnd, ThreadId, Timeline, TimelineEnd, TimelineEntry, Uuid};

use crate::bus;
use crate::harness::{SignedOut, Spend};
use crate::limits::breaker::StartFailed;
use crate::limits::caps::{Recorded, Warning};
use crate::limits::ratelimit::Limited;
use crate::worktrees::WorktreeFailed;

use super::super::Inbox;

/// What a prompt took, noted in the ant's timeline as it ends ([`TimelineEntry::Ended`]).
pub(in crate::daemon) struct Measured {
    /// The message the prompt ran for, whose timeline the end joins.
    message: Uuid,
    /// When the prompt began: when it got its thread's harness, once told so through `harnessed`.
    began: tokio::time::Instant,
    harnessed: tokio::sync::oneshot::Receiver<tokio::time::Instant>,
    /// How long the prompt ran, once it ended.
    took: std::time::Duration,
    /// The tokens the prompt spent so far, as the harness reported them.
    pub(super) tokens: u64,
    /// The cost in US dollars the prompt spent so far, as the harness reported it.
    cost: f64,
    /// The prompt's tool calls by kind, once it ended ([`TimelineEnd::calls`]).
    calls: BTreeMap<String, CallCount>,
    /// How many of its tool calls called sense's own tools, once it ended.
    sense_calls: u32,
}

impl Measured {
    /// The measures of the prompt for `message`, beginning now or when told through the sender returned
    /// ([`Prompt::began`](crate::harness::Prompt::began)).
    pub(super) fn new(message: Uuid) -> (Self, tokio::sync::oneshot::Sender<tokio::time::Instant>) {
        let (tx, harnessed) = tokio::sync::oneshot::channel();
        let (began, took) = (tokio::time::Instant::now(), std::time::Duration::ZERO);
        (Self { message, began, harnessed, took, tokens: 0, cost: 0.0, calls: BTreeMap::new(), sense_calls: 0 }, tx)
    }

    /// Note that the prompt ended now, with the tool calls `calls` collected. A wait for a free harness is not the
    /// prompt's.
    pub(super) fn end(&mut self, calls: &crate::monitoring::timeline::Collector) {
        if let Ok(at) = self.harnessed.try_recv() {
            self.began = at;
        }
        self.took = self.began.elapsed();
        (self.calls, self.sense_calls) = calls.counts();
    }
}

impl Inbox<'_> {
    /// Note in the ant's timeline on `thread` that the prompt `measured` ended `how`, its session `rebuilt` or not,
    /// with the tool requests its turn asked. Stored before the message is settled; failing to, it only logs.
    pub(super) async fn note_end(&self, thread: &ThreadId, measured: &Measured, how: PromptEnd, rebuilt: bool) {
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
    /// when the harness did not start or the thread's worktree could not be made, nor when the harness turned the
    /// prompt away for want of signing in or at a provider limit before any tool ran.
    pub(in crate::daemon) async fn note_failed(
        &self,
        thread: &ThreadId,
        measured: &Measured,
        e: &anyhow::Error,
        worked: bool,
    ) {
        if !e.is::<StartFailed>()
            && !e.is::<WorktreeFailed>()
            && (worked || !(e.is::<SignedOut>() || e.is::<Limited>()))
        {
            self.note_end(thread, measured, PromptEnd::Failed, false).await;
        }
    }

    /// Record the spend reported as a prompt ended, such as its tokens, in `measured`, keeping the first cap it reached
    /// in `reached`.
    pub(in crate::daemon) async fn record_ended(
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
    pub(super) async fn record(&self, envelope: &Envelope, spent: &Spend, measured: &mut Measured) -> Option<String> {
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
}

/// The tokens `spent` reports.
fn spent_tokens(spent: &Spend) -> u64 {
    match spent {
        Spend::Tokens(n) => *n,
        Spend::Cost { .. } => 0,
    }
}

/// When the next record of a prompt's tool-call timeline is due: at its next tick once something changed.
pub(super) async fn stored_every(calls: &crate::monitoring::timeline::Collector<'_>, tick: &mut tokio::time::Interval) {
    if calls.pending() {
        tick.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}
