//! Restoring runs from their records: at the daemon's start, and to resume one.

use std::collections::BTreeSet;
use std::sync::Arc;

use ants_proto::{PendingCall, QueuedCall, RunId, RunState, TrailFunction, TrailRun};
use ants_trail::{Run, RunState as Session};
use anyhow::{Context as _, Result};

use super::{Book, Slot, Step, Trails, engine_limits, locked, steps};

impl Trails {
    /// The run of `header` as its records hold it: its stored session, given the answers its journal holds beyond it.
    /// When `resumed`, under the header's limits and without the steps it waited on, which go out anew, but for its
    /// child runs, which resume with it; else waiting on those steps again.
    pub(super) async fn load(&self, header: TrailRun, resumed: bool) -> Result<Arc<Slot>> {
        let store = &self.inner.store;
        let run = header.run.clone();
        let stored = store.session(&run).await?.context("the run has no stored session")?;
        let mut session: Session = serde_json::from_str(&stored.session).context("parsing the run's session")?;
        if resumed {
            session = session.with_limits(engine_limits(&header.limits))?;
        }
        let mut calls: BTreeSet<u32> = session.answered().iter().copied().collect();
        calls.extend(Run::restore(session.clone(), Vec::new()).outstanding().iter().map(|call| call.call));
        let mut journal = Vec::new();
        for call in calls {
            journal.extend(store.entry(&run, call).await?);
        }
        let engine = Run::restore(session, journal);
        let outstanding = |pending: &PendingCall| {
            (!resumed || pending.function == TrailFunction::Trail)
                && engine.outstanding().iter().any(|c| c.call == pending.call)
        };
        let (pending, dropped): (Vec<_>, Vec<_>) = stored.pending.into_iter().partition(outstanding);
        let live = !matches!(header.state, RunState::Stopped { .. });
        {
            let mut threads = locked(&self.inner.threads);
            let calls = pending.iter().map(|call| (call, live)).chain(dropped.iter().map(|call| (call, false)));
            for (call, live) in calls.filter(|(call, _)| call.function != TrailFunction::Trail) {
                let step = if live {
                    Step::Live(run.clone())
                } else {
                    Step::Retired { run: run.clone(), to: call.to.clone() }
                };
                threads.insert(call.thread.clone(), step);
            }
        }
        let queued = queued(&engine);
        let session = stored.session;
        let book = Book { header, session, pending, queued, answers: Vec::new(), late: false, starved: false };
        Ok(Slot::new(engine, book))
    }

    /// Continue the runs the conductor's index lists, their steps' threads known again before a reply to one may
    /// arrive. Each restored run gets a pass once [`Trails::serve`] runs.
    pub async fn restore(&self) {
        let runs = match self.inner.store.runs().await {
            Ok(runs) => runs,
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "reading the trail runs to continue");
                return;
            }
        };
        for run in runs {
            if let Err(e) = self.restore_run(&run).await {
                tracing::warn!(run = %run, error = format!("{e:#}"), "continuing a trail run");
            }
        }
        self.reconcile().await;
    }

    pub(super) async fn restore_run(&self, run: &RunId) -> Result<()> {
        let store = &self.inner.store;
        let header = store.header(run).await?;
        let Some(header) = header.filter(|header| !header.finished()) else {
            return store.list(run, false).await;
        };
        let slot = self.load(header, false).await?;
        locked(&self.inner.runs).insert(run.clone(), slot);
        self.wake(run);
        tracing::info!(run = %run, "trail run continued");
        Ok(())
    }
}

/// The calls `engine` holds back for room, as its session's record lists them.
pub(super) fn queued(engine: &Run) -> Vec<QueuedCall> {
    let queued = engine.queued().iter();
    queued
        .map(|call| QueuedCall { call: call.call, function: steps::function(call.function), args: call.args.clone() })
        .collect()
}
