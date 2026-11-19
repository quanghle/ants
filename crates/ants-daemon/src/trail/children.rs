//! Child runs: a trail() call is answered by a run of the saved trail it names, which the conductor starts as a child
//! of the calling run ([`TrailParent`]), with its parent's owner and origin, on a thread of its own. Its result answers
//! the call and its failure raises there; it tells its end on its own thread, sending its owner nothing. Stopping or
//! ending a run stops its children; resuming it resumes them, those that failed on a limit too, whose errors no longer
//! answer their calls, and starts anew those that left no record.
//!
//! A run and its descendants, its tree, share its root's limits: each step and turn a run spends is charged to every
//! ancestor too, so the root's counters hold the tree's, and each call is checked against them. A child's pass stores
//! its ancestors' headers before its own session, so a crash between them counts a step twice, never not at all. A
//! child ends within the wall time its parent has left.

use std::sync::Arc;

use ants_proto::{
    PendingCall, RunId, RunState, TrailLeg, TrailLimits, TrailName, TrailParent, TrailRun, UserId, WaitingStep,
};
use ants_trail::{Answer, Call, Run};
use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{Leg, Slot, Trails, driver, locked, steps};

/// What a run's tree may still spend, as its root's counters and limits say.
pub(super) struct Budget {
    pub(super) limits: TrailLimits,
    /// The steps the tree sent.
    pub(super) steps: u32,
    /// The turns neither spent nor held by a step of the tree.
    pub(super) turns: u32,
}

impl Trails {
    /// Why `call`, a trail() call of the run of `header`, raises before any run starts, counting no step: it names a
    /// trail on the route of the run or of one of its ancestors, any leg of it, which would recurse, or a child run that
    /// cannot start, as of a trail not saved or arguments that do not fit.
    pub(super) async fn refuse_trail(&self, header: &TrailRun, call: &Call) -> Option<String> {
        let (name, args) = match trail_args(&call.args) {
            Ok(called) => called,
            Err(e) => return Some(e),
        };
        let ancestors = self.ancestors(header);
        let mut callers = std::iter::once(header.clone()).chain(ancestors.iter().map(|slot| slot.header()));
        if let Some(caller) = callers.find(|caller| caller.route.iter().any(|leg| leg.trail.as_ref() == Some(&name))) {
            return Some(format!(
                "trail {name} is on the route of run {}, among the calling runs: no trail calls itself",
                caller.run
            ));
        }
        let root = self.root(header);
        if let Some(from) = root.spends_from.as_ref().filter(|from| from.route.contains(&name)) {
            return Some(format!(
                "trail {name} is on the route of run {}, whose step run {} spends from: no trail calls itself",
                from.run, root.run
            ));
        }
        self.child_leg(name, args, &child_limits(header, Utc::now())).await.err()
    }

    /// Start the child run answering `call`, a trail() call of the run of `parent`, unless it started already: an
    /// error to answer the call with when it cannot start.
    pub(super) async fn start_child(&self, parent: &TrailRun, call: &PendingCall) -> Result<Result<(), String>> {
        let Some(run) = call.child() else { return Ok(Err("the call names no child run".to_owned())) };
        if let Some(child) = self.inner.store.header(&run).await? {
            // Started before a restart: in memory again, or ended, when it answers the call now.
            if child.finished() {
                self.answer_parent(&child).await?;
            }
            return Ok(Ok(()));
        }
        let (name, args) = match trail_args(&call.args) {
            Ok(called) => called,
            Err(e) => return Ok(Err(e)),
        };
        let limits = child_limits(parent, Utc::now());
        let (leg, engine, session) = match self.child_leg(name, args, &limits).await {
            Ok(prepared) => prepared,
            Err(e) => return Ok(Err(e)),
        };
        let now = leg.started_at;
        let origin_thread = parent.parent.as_ref().map_or(&parent.thread, |link| &link.origin_thread).clone();
        let header = TrailRun {
            run,
            conductor: parent.conductor.clone(),
            route: vec![leg],
            owner: parent.owner.clone(),
            thread: call.thread.clone(),
            parent: Some(TrailParent { run: parent.run.clone(), call: call.call, origin_thread }),
            spends_from: None,
            origin: parent.origin.clone(),
            limits,
            state: RunState::Running,
            phase: None,
            steps: 0,
            turns: 0,
            started_at: now,
            updated_at: now,
            ended_at: None,
        };
        self.launch(header, engine, session).await?;
        Ok(Ok(()))
    }

    /// The first leg of a child run of the saved trail `name` with `args` as its input under `limits`, its run and its
    /// session as JSON; or why it cannot start.
    async fn child_leg(
        &self,
        name: TrailName,
        args: Value,
        limits: &TrailLimits,
    ) -> Result<(TrailLeg, Run, String), String> {
        let script = self.inner.saved.script(&name).await?;
        Leg { trail: Some(name), script, at: None, args, first_call: 0 }.prepare(limits)
    }

    /// Give back the step counted for a trail() call of the run of `slot` whose child run did not start, to the run and
    /// its ancestors.
    pub(super) fn give_back_step(&self, slot: &Slot) {
        let header = {
            let mut book = slot.book();
            driver::uncount_steps(&mut book.header, 1);
            book.header.clone()
        };
        for ancestor in self.ancestors(&header) {
            driver::uncount_steps(&mut ancestor.book().header, 1);
        }
    }

    /// Answer the call `child`, ended, answers: with its result, or with an error naming its trail and its own, or the
    /// size of a result its parent's journal entry would not hold. Its journal entry is stored when this returns;
    /// nothing when its parent no longer waits on it.
    pub(super) async fn answer_parent(&self, child: &TrailRun) -> Result<()> {
        let Some(link) = &child.parent else { return Ok(()) };
        let name = child.first_trail().map_or_else(|| child.run.to_string(), ToString::to_string);
        let mut answer = match &child.state {
            RunState::Done { result } => Answer::Ok(result.clone()),
            RunState::Failed { error, .. } => Answer::Error(format!("trail {name} failed: {error}")),
            RunState::Running | RunState::Waiting | RunState::Stopped { .. } => return Ok(()),
        };
        let Some(slot) = locked(&self.inner.runs).get(&link.run).cloned() else { return Ok(()) };
        let (header, pending) = {
            let book = slot.book();
            let answered = |call: &&PendingCall| call.call == link.call && call.child().as_ref() == Some(&child.run);
            (book.header.clone(), book.pending.iter().find(answered).cloned())
        };
        let Some(pending) = pending else { return Ok(()) };
        if let Answer::Ok(result) = &answer {
            let bytes = serde_json::to_vec(result).map_or(usize::MAX, |result| result.len());
            if bytes.saturating_add(pending.args.to_string().len()) > driver::MAX_RECORD {
                let max = driver::MAX_RECORD;
                answer = Answer::Error(format!(
                    "trail {name} failed: its result, {bytes} bytes as JSON, outgrew the {max} bytes the call's \
                     journal entry holds"
                ));
            }
        }
        self.resolve_on(&slot, &header, &pending, answer, 0, child.state.failed_on()).await
    }

    /// Stop the child runs answering `calls` for `by`. Returns the steps to other ants they waited on.
    pub(super) async fn stop_children(&self, calls: &[PendingCall], by: &UserId) -> Vec<WaitingStep> {
        let mut waiting = Vec::new();
        for child in calls.iter().filter_map(PendingCall::child) {
            match Box::pin(self.stop(&child, by)).await {
                Ok(steps) => waiting.extend(steps),
                Err(e) => tracing::info!(run = %child, error = e, "a child run not stopped"),
            }
        }
        waiting
    }

    /// Resume, for `by`, the child runs answering `calls` of the run of `parent` that are stopped or failed on a limit,
    /// within the parent's limits; one that ended otherwise meanwhile answers its call.
    pub(super) async fn resume_children(&self, parent: &TrailRun, calls: &[PendingCall], by: &UserId) {
        for run in calls.iter().filter_map(PendingCall::child) {
            let child = match self.inner.store.header(&run).await {
                Ok(Some(child)) => child,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(run = %run, error = format!("{e:#}"), "reading a child run to resume");
                    continue;
                }
            };
            let resumed = match &child.state {
                RunState::Stopped { .. } | RunState::Failed { limit: Some(_), .. } => {
                    let limits = child_limits(parent, child.started_at);
                    Box::pin(self.resume(&run, by, Some(limits))).await
                }
                state if state.finished() => self.answer_parent(&child).await.map_err(|e| format!("{e:#}")),
                _ => Ok(()),
            };
            if let Err(e) = resumed {
                tracing::warn!(run = %run, error = e, "resuming a child run");
            }
        }
    }

    /// The calls among `pending`, those of run `run` its session has not taken an answer to (`answered`), whose child
    /// runs failed on a limit or left no record: resuming the run resumes the former and starts the latter anew, marked
    /// unsent, so the error each answered its call with is deleted from the run's journal.
    pub(super) async fn rerun_children(
        &self,
        run: &RunId,
        pending: &mut [PendingCall],
        answered: &[u32],
    ) -> Result<Vec<u32>> {
        let mut rerun = Vec::new();
        for call in pending.iter_mut().filter(|call| !answered.contains(&call.call)) {
            let Some(child) = call.child() else { continue };
            match self.inner.store.header(&child).await? {
                Some(child) if child.state.failed_on().is_none() => continue,
                Some(_) => {}
                None => call.sent = false,
            }
            self.inner.store.delete_entry(run, call.call).await?;
            rerun.push(call.call);
        }
        Ok(rerun)
    }

    /// After a restart: answer the calls whose child runs ended unanswered, continue those whose children the index
    /// missed, and stop the children whose parents no longer wait on them.
    pub(super) async fn reconcile(&self) {
        let slots: Vec<Arc<Slot>> = locked(&self.inner.runs).values().cloned().collect();
        for slot in slots {
            let (header, calls) = {
                let book = slot.book();
                let calls: Vec<PendingCall> = book.pending.iter().filter(|call| call.sent).cloned().collect();
                (book.header.clone(), calls)
            };
            for call in &calls {
                let Some(child) = call.child() else { continue };
                if locked(&self.inner.runs).contains_key(&child) {
                    continue;
                }
                let reconciled = match self.inner.store.header(&child).await {
                    Ok(Some(ended)) if ended.finished() => self.answer_parent(&ended).await,
                    Ok(Some(_)) => self.restore_run(&child).await,
                    Ok(None) => {
                        let lost = Answer::Error(format!("the child run {child} is lost"));
                        self.resolve(&slot, &header, call, lost, 0).await
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = reconciled {
                    tracing::warn!(run = %header.run, child = %child, error = format!("{e:#}"), "reconciling a child run");
                }
            }
            let orphaned = header.parent.is_some() && !self.awaited(&header);
            if !orphaned || matches!(header.state, RunState::Stopped { .. }) {
                continue;
            }
            if let Err(e) = self.stop(&header.run, &self.inner.ant).await {
                tracing::info!(run = %header.run, error = e, "a child run whose parent ended not stopped");
            }
        }
    }

    /// Whether the parent of the run of `header` is in memory and waits on it.
    fn awaited(&self, header: &TrailRun) -> bool {
        let Some(link) = &header.parent else { return false };
        let Some(parent) = locked(&self.inner.runs).get(&link.run).cloned() else { return false };
        let book = parent.book();
        book.pending.iter().any(|call| call.call == link.call && call.child().as_ref() == Some(&header.run))
    }

    /// The runs in memory from the parent of the run of `header` up to its root, nearest first.
    pub(super) fn ancestors(&self, header: &TrailRun) -> Vec<Arc<Slot>> {
        let runs = locked(&self.inner.runs);
        let mut found: Vec<Arc<Slot>> = Vec::new();
        let mut next = header.parent.as_ref().map(|link| link.run.clone());
        while let Some(slot) = next.and_then(|run| runs.get(&run).cloned()) {
            if found.iter().any(|seen| Arc::ptr_eq(seen, &slot)) {
                break;
            }
            next = slot.book().header.parent.as_ref().map(|link| link.run.clone());
            found.push(slot);
        }
        found
    }

    /// The root of the tree of the run of `header`: its farthest ancestor in memory, else itself.
    fn root(&self, header: &TrailRun) -> TrailRun {
        self.ancestors(header).last().map_or_else(|| header.clone(), |slot| slot.header())
    }

    /// The run whose resume resumes the child run of `header`, stopped by `by`: its farthest ancestor in memory stopped
    /// along with it, else itself, unless the conductor stopped it alone, as its parent's goto() or end does.
    pub(super) fn stopped_root(&self, header: &TrailRun, by: &UserId) -> Option<RunId> {
        let stopped = self
            .ancestors(header)
            .into_iter()
            .map(|slot| slot.header())
            .take_while(|ancestor| matches!(ancestor.state, RunState::Stopped { .. }));
        match stopped.last() {
            Some(ancestor) => Some(ancestor.run),
            None => (*by != self.inner.ant).then(|| header.run.clone()),
        }
    }

    /// The runs in memory of the tree whose root is `root`.
    fn tree(&self, root: &RunId) -> Vec<Arc<Slot>> {
        let runs = locked(&self.inner.runs);
        let parent = |slot: &Slot| slot.book().header.parent.as_ref().map(|link| link.run.clone());
        let rooted = |run: &RunId, slot: &Slot| {
            let (mut at, mut up) = (run.clone(), parent(slot));
            for _ in 0..runs.len() {
                let Some(next) = up else { break };
                up = runs.get(&next).and_then(|slot| parent(slot));
                at = next;
            }
            at == *root
        };
        runs.iter().filter(|(run, slot)| rooted(run, slot)).map(|(_, slot)| slot.clone()).collect()
    }

    /// What the tree of the run of `header` may still spend.
    pub(super) fn budget(&self, header: &TrailRun) -> Budget {
        let root = self.root(header);
        let tree = self.tree(&root.run);
        let reserved: u32 = tree.iter().map(|slot| slot.book().pending.iter().map(driver::reserved).sum::<u32>()).sum();
        let turns = root.limits.turns.saturating_sub(root.turns).saturating_sub(reserved);
        Budget { limits: root.limits, steps: root.steps, turns }
    }

    /// Whether a step of the tree of the run of `header` holds turns it may give back.
    pub(super) fn holds_turns(&self, header: &TrailRun) -> bool {
        let tree = self.tree(&self.root(header).run);
        tree.iter().any(|slot| slot.book().pending.iter().any(|call| driver::reserved(call) > 0))
    }

    /// Charge the `steps` and `turns` the run of `header` spent to each of its ancestors as well.
    pub(super) fn charge(&self, header: &TrailRun, steps: u32, turns: u32) {
        if header.parent.is_none() || steps == 0 && turns == 0 {
            return;
        }
        for slot in self.ancestors(header) {
            let mut book = slot.book();
            driver::count_steps(&mut book.header, steps);
            book.header.turns = book.header.turns.saturating_add(turns);
        }
    }

    /// Store the headers of the ancestors of the run of `header`, which hold what it spent.
    pub(super) async fn save_ancestors(&self, header: &TrailRun) -> Result<()> {
        for slot in self.ancestors(header) {
            self.save_header(&slot).await?;
        }
        Ok(())
    }

    /// Wake the runs of the tree of the run of `header` whose calls wait for turns its steps held.
    pub(super) fn wake_starved(&self, header: &TrailRun) {
        for slot in self.tree(&self.root(header).run) {
            let starved = {
                let mut book = slot.book();
                std::mem::take(&mut book.starved).then(|| book.header.run.clone())
            };
            if let Some(run) = starved {
                self.wake(&run);
            }
        }
    }
}

/// The saved trail the arguments `args` of a trail() call name, and the input they give its run.
fn trail_args(args: &Value) -> Result<(TrailName, Value), String> {
    let args = steps::TrailArgs::of(args)?;
    let name = args.name.parse().map_err(|e| format!("trail() names no trail as {:?}: {e}", args.name))?;
    Ok((name, args.args))
}

/// The limits of a child run of the run of `parent` started at `at`: the parent's, its wall time cut to what the parent
/// has left.
fn child_limits(parent: &TrailRun, at: DateTime<Utc>) -> TrailLimits {
    let wall = chrono::Duration::from_std(parent.limits.wall_time()).unwrap_or(chrono::Duration::MAX);
    let deadline = parent.started_at.checked_add_signed(wall).unwrap_or(DateTime::<Utc>::MAX_UTC);
    let left = u64::try_from((deadline - at).num_milliseconds()).unwrap_or(0);
    TrailLimits { wall_secs: left.div_ceil(1000).clamp(1, TrailLimits::MAX.wall_secs), ..parent.limits }
}
