//! Restoring runs from their records: at the daemon's start, again while it lives for those it could not read, and to
//! resume one.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::{PendingCall, QueuedCall, RunId, RunState, TrailFunction, TrailLeg, TrailRun, TrailSession};
use ants_trail::{Run, RunState as Session};
use anyhow::{Context as _, Result, anyhow, bail};

use super::{Book, Leg, Slot, Step, Trails, driver, engine_limits, locked, steps};

/// How long the daemon waits before trying the runs left unrestored again, at first and at most.
const RESTORE_FIRST: Duration = Duration::from_secs(2);
const RESTORE_MAX: Duration = Duration::from_mins(5);

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
    /// arrive. Each restored run gets a pass once [`Trails::serve`] runs. The runs left unrestored, all of them when
    /// the index cannot be read, are tried again while the daemon lives ([`Trails::restore_again`]), and on demand
    /// ([`Trails::restored`]).
    pub async fn restore(&self) {
        if let Err(e) = self.restore_left().await {
            tracing::warn!(error = format!("{e:#}"), "reading the trail runs to continue");
        }
    }

    /// Restore the runs left unrestored: the index's once it is read, those whose records could not be read until
    /// then. Fails when the index cannot be read.
    async fn restore_left(&self) -> Result<()> {
        let left = match &*locked(&self.inner.unrestored) {
            Unrestored::Index => None,
            Unrestored::Runs(runs) => Some(runs.clone()),
        };
        let runs = match left {
            Some(runs) => runs,
            None => {
                let runs: BTreeSet<RunId> = self.inner.store.runs().await?.into_iter().collect();
                let mut unrestored = locked(&self.inner.unrestored);
                if matches!(*unrestored, Unrestored::Index) {
                    let held = locked(&self.inner.runs);
                    let left = runs.iter().filter(|run| !held.contains_key(run)).cloned().collect();
                    *unrestored = Unrestored::Runs(left);
                }
                runs
            }
        };
        for run in &runs {
            if let Err(e) = self.restore_run(run).await {
                tracing::warn!(run = %run, error = format!("{e:#}"), "continuing a trail run");
            }
        }
        if !runs.is_empty() {
            self.reconcile().await;
        }
        Ok(())
    }

    /// Try the runs left unrestored again, waiting longer each time up to [`RESTORE_MAX`], until none is left.
    pub(super) async fn restore_again(&self) {
        let mut wait = RESTORE_FIRST;
        while !self.restored_all() {
            tokio::time::sleep(wait).await;
            if let Err(e) = self.restore_left().await {
                tracing::warn!(error = format!("{e:#}"), "reading the trail runs to continue, again");
            }
            wait = (wait * 2).min(RESTORE_MAX);
        }
        std::future::pending::<()>().await;
    }

    fn restored_all(&self) -> bool {
        matches!(&*locked(&self.inner.unrestored), Unrestored::Runs(runs) if runs.is_empty())
    }

    /// Whether `run` may be one of the conductor's runs left unrestored: listed in its index, its records not read yet.
    pub(super) fn unrestored(&self, run: &RunId) -> bool {
        match &*locked(&self.inner.unrestored) {
            Unrestored::Index => true,
            Unrestored::Runs(runs) => runs.contains(run),
        }
    }

    /// The run `run` in memory, restored now when it was left unrestored: None when the conductor holds no such run
    /// unfinished; fails while its records, or the index, cannot be read.
    pub(super) async fn restored(&self, run: &RunId) -> Result<Option<Arc<Slot>>> {
        let held = || locked(&self.inner.runs).get(run).cloned();
        if let Some(slot) = held() {
            return Ok(Some(slot));
        }
        if !self.unrestored(run) {
            return Ok(None);
        }
        let index = matches!(*locked(&self.inner.unrestored), Unrestored::Index);
        if index {
            // Boxed: restoring may stop orphaned child runs, which may restore.
            Box::pin(self.restore_left()).await.context("reading the trail runs to continue")?;
        } else {
            self.restore_run(run).await.with_context(|| format!("continuing run {run}"))?;
        }
        match held() {
            Some(slot) => Ok(Some(slot)),
            None if self.unrestored(run) => Err(anyhow!("run {run} is not continued yet: its records were not read")),
            None => Ok(None),
        }
    }

    /// Continue run `run` from its records, listed in the index; a run with no header leaves the index.
    pub(super) async fn restore_run(&self, run: &RunId) -> Result<()> {
        if locked(&self.inner.runs).contains_key(run) {
            self.mark_restored(run);
            return Ok(());
        }
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
                store.list(run, false).await?;
                self.mark_restored(run);
                return Ok(());
            }
            Some(header) => header,
            None => {
                store.list(run, false).await?;
                self.mark_restored(run);
                return Ok(());
            }
        };
        let slot = self.load(header, false).await?;
        store.list(run, true).await?;
        let inserted = {
            let mut runs = locked(&self.inner.runs);
            let vacant = !runs.contains_key(run);
            if vacant {
                runs.insert(run.clone(), slot);
            }
            vacant
        };
        self.mark_restored(run);
        if inserted {
            self.wake(run);
            tracing::info!(run = %run, "trail run continued");
        }
        Ok(())
    }

    fn mark_restored(&self, run: &RunId) {
        if let Unrestored::Runs(runs) = &mut *locked(&self.inner.unrestored) {
            runs.remove(run);
        }
    }
}

/// The conductor's runs its daemon has not continued from their records yet.
#[derive(Default)]
pub(super) enum Unrestored {
    /// All of them: its index is not read yet.
    #[default]
    Index,
    /// Those of its index whose records could not be read yet.
    Runs(BTreeSet<RunId>),
}

/// The calls `engine` holds back for room, as its session's record lists them.
pub(super) fn queued(engine: &Run) -> Vec<QueuedCall> {
    let queued = engine.queued().iter();
    queued
        .map(|call| QueuedCall { call: call.call, function: steps::function(call.function), args: call.args.clone() })
        .collect()
}
