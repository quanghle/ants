//! Restoring runs from their records: at the daemon's start, and to resume one.

use std::collections::BTreeSet;
use std::sync::Arc;

use ants_proto::{PendingCall, QueuedCall, RunId, RunState, TrailFunction, TrailLeg, TrailRun, TrailSession};
use ants_trail::{Run, RunState as Session};
use anyhow::{Context as _, Result, anyhow, bail};

use super::{Book, Leg, Slot, Step, Trails, driver, engine_limits, locked, steps};

impl Trails {
    /// The run of `header` as its records hold it: its stored session, given the answers its journal holds beyond it.
    /// When `resumed`, under the header's limits and without the steps it waited on, which go out anew, but for its
    /// child runs, which resume with it, those failed on a limit too, or start anew when they left no record; else
    /// waiting on those steps again. The steps that go out anew count again once sent, so the run and its ancestors in
    /// memory are given back what those counted.
    pub(super) async fn load(&self, mut header: TrailRun, resumed: bool) -> Result<Arc<Slot>> {
        let store = &self.inner.store;
        let run = header.run.clone();
        let mut stored = store.session(&run).await?.context("the run has no stored session")?;
        let mut session: Session = serde_json::from_str(&stored.session).context("parsing the run's session")?;
        if header.leg().is_none_or(|leg| leg.first_call != session.first_call()) {
            return self.load_moved(header, stored);
        }
        if resumed {
            session = session.with_limits(engine_limits(&header.limits))?;
        }
        let mut calls: BTreeSet<u32> = session.answered().iter().copied().collect();
        calls.extend(Run::restore(session.clone(), Vec::new()).outstanding().iter().map(|call| call.call));
        if resumed {
            for call in self.rerun_children(&run, &mut stored.pending, session.answered()).await? {
                calls.remove(&call);
            }
        }
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
        if resumed {
            let anew = dropped.iter().filter(|call| engine.outstanding().iter().any(|c| c.call == call.call)).count();
            let anew = u32::try_from(anew).unwrap_or(u32::MAX);
            driver::uncount_steps(&mut header, anew);
            for ancestor in self.ancestors(&header) {
                driver::uncount_steps(&mut ancestor.book().header, anew);
            }
        }
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
        let book = Book {
            header,
            session,
            pending,
            queued,
            left: Vec::new(),
            answers: Vec::new(),
            broke: None,
            late: false,
            starved: false,
        };
        Ok(Slot::new(engine, book))
    }

    /// The run of `header`, which went on to its last leg before storing that leg's session: the leg starts afresh, and
    /// the steps of the leg before, which `stored` still lists, are no longer waited on, and are given up on the run's
    /// next pass, as the goto() may not have given them up before the daemon stopped.
    fn load_moved(&self, header: TrailRun, stored: TrailSession) -> Result<Arc<Slot>> {
        let leg = header.leg().context("the run has no leg")?.clone();
        let TrailLeg { trail, at, script, args, first_call, .. } = leg;
        let (_, engine, session) =
            Leg { trail, script, at, args, first_call }.prepare(&header.limits).map_err(|e| anyhow!(e))?;
        {
            let mut threads = locked(&self.inner.threads);
            for call in stored.pending.iter().filter(|call| call.function != TrailFunction::Trail) {
                threads.insert(call.thread.clone(), Step::Retired { run: header.run.clone(), to: call.to.clone() });
            }
        }
        let book = Book {
            header,
            session,
            pending: Vec::new(),
            queued: Vec::new(),
            left: stored.pending,
            answers: Vec::new(),
            broke: None,
            late: false,
            starved: false,
        };
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

    /// Continue run `run` from its records, listed in the index; a run with no header leaves the index.
    pub(super) async fn restore_run(&self, run: &RunId) -> Result<()> {
        let store = &self.inner.store;
        let header = match store.header(run).await? {
            // Ended before its end was told: the invitation to rate it and its owner's message, both sent again.
            Some(header) if header.finished() => {
                if !self.invite(&header).await {
                    bail!("the invitation to rate the ended run is not stored");
                }
                // A child run's end is told in the notes of its thread, which a repeat would note twice.
                if header.parent.is_none() && !self.report_end(&header).await {
                    bail!("the message telling the run's end is not stored");
                }
                return store.list(run, false).await;
            }
            Some(header) => header,
            None => return store.list(run, false).await,
        };
        let slot = self.load(header, false).await?;
        store.list(run, true).await?;
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
