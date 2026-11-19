//! Runs started from a step: an ant answering a step that starts a run with `run_trail` spends from the step's run.
//!
//! The step's conductor grants each agent() step a share of what the step's run, through its root's limits, has left
//! as the step goes ([`StepGrant`]), and the step carries it ([`StepBounds`]). The grant is held, less what was told
//! spent, while the run waits on the step or while the runs started answering it still run, and given back otherwise:
//! steps out at once share what is left, and the tree's limits bound what they spend together.
//!
//! The recipient's daemon cuts each run started answering the step, and each resume of one, to what the grant leaves
//! after the others: the limits of those still running, the spending of those ended or stopped. It keeps which runs
//! answer the step in its records ([`TrailAnswering`]), and tells what they spent in all, with each reply to the step
//! and with a [`NoticeKind::RunSpent`] notice on the step's thread as each of them ends or stops ([`TrailSpent`]). The
//! conductor charges the part it has not charged yet to the step's run and its ancestors, so a message told twice
//! charges once.

use ants_proto::{
    Envelope, NoticeKind, PendingCall, RunId, RunState, SpendsFrom, StepBounds, StepGrant, ThreadId, TrailAnswering,
    TrailLimits, TrailName, TrailRun, TrailSpent, TrailStep, UserId,
};
use anyhow::Result;
use chrono::{DateTime, Utc};

use super::invite::end_id;
use super::{Book, Slot, Trails, driver};

impl Trails {
    /// `message`, a step of the run of `header` on its thread, carrying the step's grant.
    pub(super) fn bounded(&self, header: &TrailRun, mut message: Envelope) -> Envelope {
        let bounds = self.bounds(header, &message.thread_id);
        if let Some(step) = message.trail_step.as_deref_mut() {
            step.bounds = Some(bounds);
        }
        message
    }

    /// The grant of the step on `thread` of the run of `header`, until the run's wall time ends, taking no trail on the
    /// routes of the run, its ancestors and the runs they spend from.
    fn bounds(&self, header: &TrailRun, thread: &ThreadId) -> StepBounds {
        let mut route: Vec<TrailName> = Vec::new();
        let ancestors = self.ancestors(header).iter().map(|slot| slot.header()).collect::<Vec<_>>();
        for run in std::iter::once(header).chain(&ancestors) {
            let legs = run.route.iter().filter_map(|leg| leg.trail.clone());
            let outer = run.spends_from.iter().flat_map(|from| from.bounds.route.iter().cloned());
            for trail in legs.chain(outer) {
                if !route.contains(&trail) {
                    route.push(trail);
                }
            }
        }
        let grant = header.grants.iter().find(|grant| grant.thread == *thread);
        let wall = chrono::Duration::from_std(header.limits.wall_time()).unwrap_or(chrono::Duration::MAX);
        StepBounds {
            steps: grant.map_or(0, |grant| grant.steps),
            turns: grant.map_or(0, |grant| grant.turns),
            until: header.started_at.checked_add_signed(wall).unwrap_or(DateTime::<Utc>::MAX_UTC),
            route,
        }
    }

    /// Grant each of `steps`, agent() steps the run of `slot` just issued, an equal share among `sharers` of what its
    /// tree has left: steps and turns neither spent nor held by its waiting steps' turn budgets and grants.
    pub(super) fn grant(&self, slot: &Slot, steps: &[(u32, ThreadId)], sharers: u32) {
        let budget = self.budget(&slot.header());
        let sharers = sharers.max(1);
        let steps_left = budget.limits.steps.saturating_sub(budget.steps).saturating_sub(budget.held.0);
        let turns_left = budget.turns.saturating_sub(budget.held.1);
        let (granted_steps, granted_turns) = (steps_left / sharers, turns_left / sharers);
        if granted_steps == 0 && granted_turns == 0 {
            return;
        }
        let mut book = slot.book();
        for (call, thread) in steps {
            book.header.grants.push(StepGrant {
                call: *call,
                thread: thread.clone(),
                steps: granted_steps,
                turns: granted_turns,
                spent: TrailSpent::default(),
            });
        }
    }

    /// Charge what `message`, from `to`, the recipient of the step `call` on the message's thread, tells the runs
    /// answering the step spent in all, to the run of `slot` and its ancestors: the part not charged before. A message
    /// telling less than was charged is an older one, whose count of runs still running is not taken either. True when
    /// the run's header changed, to be stored.
    pub(super) fn take_told(&self, slot: &Slot, call: u32, to: &UserId, message: &Envelope) -> bool {
        let Some(told) = message.trail_spent.filter(|_| message.from == *to) else { return false };
        let (header, steps, turns, changed) = {
            let mut book = slot.book();
            let grants = &mut book.header.grants;
            let at = match grants.iter().position(|grant| grant.thread == message.thread_id) {
                Some(at) => at,
                None => {
                    let thread = message.thread_id.clone();
                    grants.push(StepGrant { call, thread, steps: 0, turns: 0, spent: TrailSpent::default() });
                    grants.len() - 1
                }
            };
            let spent = &mut grants[at].spent;
            let (steps, turns) = (told.steps.saturating_sub(spent.steps), told.turns.saturating_sub(spent.turns));
            let stale = told.steps < spent.steps || told.turns < spent.turns;
            let running = if stale { spent.running } else { told.running };
            let changed = steps > 0 || turns > 0 || running != spent.running;
            *spent = TrailSpent { steps: spent.steps.max(told.steps), turns: spent.turns.max(told.turns), running };
            driver::count_steps(&mut book.header, steps);
            book.header.turns = book.header.turns.saturating_add(turns);
            tidy(&mut book);
            (book.header.clone(), steps, turns, changed)
        };
        self.charge(&header, steps, turns);
        changed
    }

    /// [`Trails::take_told`], the run's header and its ancestors' stored once it changed, before the message is acked.
    pub(super) async fn store_told(&self, slot: &Slot, call: u32, to: &UserId, message: &Envelope) -> Result<()> {
        if !self.take_told(slot, call, to, message) {
            return Ok(());
        }
        let header = slot.header();
        self.save_ancestors(&header).await?;
        self.save_header(slot).await?;
        Ok(())
    }

    /// The limits of a run asking for `limits`, started at `now` answering the step on `thread` that `from` names: cut
    /// to what the step's grant leaves after the runs already started answering it, the run then listed among them.
    /// Refused when the grant leaves no steps, turns or wall time. Called with [`super::Inner::answering`] held.
    pub(super) async fn start_answering(
        &self,
        run: &RunId,
        thread: &ThreadId,
        from: &SpendsFrom,
        limits: TrailLimits,
    ) -> Result<TrailLimits, String> {
        let stored = |e: anyhow::Error| format!("reading the runs answering the step: {e:#}");
        let record = self.inner.store.answering(thread).await.map_err(stored)?;
        let mut record = record
            .filter(|record| (&record.conductor, &record.run, record.call) == (&from.conductor, &from.run, from.call))
            .unwrap_or_else(|| TrailAnswering {
                conductor: from.conductor.clone(),
                run: from.run.clone(),
                call: from.call,
                runs: Vec::new(),
            });
        let left = self.left(&record, None, &from.bounds).await?;
        let limits = within(limits, &left, Utc::now())?;
        record.runs.push(run.clone());
        let stored = |e: anyhow::Error| format!("storing the runs answering the step: {e:#}");
        self.inner.store.put_answering(thread, &record).await.map_err(stored)?;
        Ok(limits)
    }

    /// The limits of the run of `header`, started answering the step `from` names and resumed under `limits`: cut to
    /// what the step's grant leaves after the other runs answering it, its own spending counted in its limits as ever.
    /// Refused when that leaves it nothing beyond what it spent. Called with [`super::Inner::answering`] held.
    pub(super) async fn resume_answering(
        &self,
        header: &TrailRun,
        from: &SpendsFrom,
        limits: TrailLimits,
    ) -> Result<TrailLimits, String> {
        let stored = |e: anyhow::Error| format!("reading the runs answering the step: {e:#}");
        let record = self.inner.store.answering(&header.thread).await.map_err(stored)?;
        let record = record.unwrap_or_else(|| TrailAnswering {
            conductor: from.conductor.clone(),
            run: from.run.clone(),
            call: from.call,
            runs: Vec::new(),
        });
        let left = self.left(&record, Some(&header.run), &from.bounds).await?;
        let wall = u64::try_from((left.until - header.started_at).num_milliseconds()).unwrap_or(0).div_ceil(1000);
        let cut = TrailLimits {
            steps: limits.steps.min(left.steps),
            turns: limits.turns.min(left.turns),
            wall_secs: limits.wall_secs.min(wall),
            ..limits
        };
        let none_left = [
            ("steps", cut.steps <= header.steps),
            ("turns", cut.turns <= header.turns),
            ("wall time", left.until <= Utc::now()),
        ];
        if let Some((limit, _)) = none_left.iter().find(|(_, none)| *none) {
            return Err(format!(
                "the grant of the step of run {} this run answers leaves it no {limit} beyond what it spent",
                from.run
            ));
        }
        Ok(cut)
    }

    /// What `bounds`, a step's grant, leaves after the runs `record` lists, but `but`: the limits of those running or
    /// waiting, the spending of the others.
    async fn left(
        &self,
        record: &TrailAnswering,
        but: Option<&RunId>,
        bounds: &StepBounds,
    ) -> Result<StepBounds, String> {
        let (mut steps, mut turns) = (bounds.steps, bounds.turns);
        for run in record.runs.iter().filter(|run| Some(*run) != but) {
            let Some(header) = self.run(run).await? else { continue };
            let (spent_steps, spent_turns) = if running(&header) {
                (header.limits.steps, header.limits.turns)
            } else {
                (header.steps, header.turns)
            };
            steps = steps.saturating_sub(spent_steps);
            turns = turns.saturating_sub(spent_turns);
        }
        Ok(StepBounds { steps, turns, ..bounds.clone() })
    }

    /// What the runs the ant started answering the step on `thread`, a step of `conductor`'s run `step` names, spent in
    /// all, and how many still run: none when it started none. A run that cannot be read counts as running, spending
    /// nothing yet: a later message tells it.
    pub async fn spent_answering(&self, conductor: &UserId, thread: &ThreadId, step: &TrailStep) -> Option<TrailSpent> {
        let record = match self.inner.store.answering(thread).await {
            Ok(record) => record?,
            Err(e) => {
                tracing::warn!(%thread, error = format!("{e:#}"), "reading the runs answering a step");
                return None;
            }
        };
        if (&record.conductor, &record.run, record.call) != (conductor, &step.run, step.call) {
            return None;
        }
        let mut spent = TrailSpent::default();
        for run in &record.runs {
            match self.run(run).await {
                Ok(Some(header)) => {
                    spent.steps = spent.steps.saturating_add(header.steps);
                    spent.turns = spent.turns.saturating_add(header.turns);
                    spent.running += u32::from(running(&header));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(run = %run, error = e, "reading a run started answering a step");
                    spent.running += 1;
                }
            }
        }
        Some(spent)
    }

    /// [`Trails::tell_spent`] for the run of `header`, stopped, in the background, sent again until stored.
    pub(super) fn tell_spent_stopped(&self, header: TrailRun) {
        if header.spends_from.is_none() {
            return;
        }
        let trails = self.clone();
        tokio::spawn(async move {
            let mut delay = driver::RETRY_FIRST;
            while !trails.tell_spent(&header).await {
                tracing::warn!(run = %header.run, retry_in = ?delay, "telling what a stopped run answering a step spent");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(driver::RETRY_MAX);
            }
        });
    }

    /// Tell the conductor of the step the run of `header`, ended or stopped, was started answering what the runs
    /// answering the step spent, in a notice on the step's thread: true once stored, or when the run answers no step. A
    /// notice not stored is sent again, under the same id.
    pub(super) async fn tell_spent(&self, header: &TrailRun) -> bool {
        let Some(from) = &header.spends_from else { return true };
        let step = TrailStep {
            run: from.run.clone(),
            call: from.call,
            label: None,
            schema: None,
            trail: None,
            script: None,
            tip_key: None,
            bounds: None,
        };
        let Some(spent) = self.spent_answering(&from.conductor, &header.thread, &step).await else { return true };
        let ended = if header.finished() { "ended" } else { "stopped" };
        let text = format!(
            "[ants] Run {}, started answering call {} of run {}, {ended}. The runs started answering the step spent {} \
             steps and {} turns in all; {} still run.",
            header.run, from.call, from.run, spent.steps, spent.turns, spent.running
        );
        let mut notice =
            Envelope::on_thread(header.thread.clone(), self.inner.ant.clone(), from.conductor.clone(), text)
                .with_owner(header.owner.clone())
                .as_notice(NoticeKind::RunSpent);
        notice.trail_spent = Some(spent);
        notice.id = end_id("trail-spent", &header.run, header.ended_at.unwrap_or(header.updated_at));
        match (self.inner.outbox)(notice).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(run = %header.run, error = format!("{e:#}"), "telling what a run answering a step spent");
                false
            }
        }
    }
}

/// Whether the run of `header` runs or waits: neither ended nor stopped.
fn running(header: &TrailRun) -> bool {
    matches!(header.state, RunState::Running | RunState::Waiting)
}

/// Whether `grant` is held: its run waits on its step, or runs answering it still run.
pub(super) fn held(grant: &StepGrant, pending: &[PendingCall]) -> bool {
    grant.spent.running > 0 || pending.iter().any(|call| call.thread == grant.thread)
}

/// The steps and turns `grant` holds of its run's tree, given the calls its run waits on.
pub(super) fn holding(grant: &StepGrant, pending: &[PendingCall]) -> (u32, u32) {
    if !held(grant, pending) {
        return (0, 0);
    }
    (grant.steps.saturating_sub(grant.spent.steps), grant.turns.saturating_sub(grant.spent.turns))
}

/// Forget the grants of `book`'s run given back with nothing told spent.
pub(super) fn tidy(book: &mut Book) {
    let pending = &book.pending;
    book.header.grants.retain(|grant| held(grant, pending) || grant.spent.steps > 0 || grant.spent.turns > 0);
}

/// Why a run of `trail` started answering a step under `bounds` is refused: the trail is on the step's route.
pub fn refused_on_route(trail: &TrailName, step: &TrailStep, bounds: &StepBounds) -> Option<String> {
    bounds.route.contains(trail).then(|| {
        let route = bounds.route.iter().map(TrailName::as_str).collect::<Vec<_>>().join(", ");
        format!(
            "trail {trail} is on the route of run {}, whose step this thread answers ({route}): a run started \
             answering a step takes no trail on its route, as no trail calls itself",
            step.run
        )
    })
}

/// The limits of a run asking for `limits`, started at `now` answering a step whose grant leaves `bounds`: each cut to
/// that; refused when it leaves no steps, turns or wall time.
pub fn within(limits: TrailLimits, bounds: &StepBounds, now: DateTime<Utc>) -> Result<TrailLimits, String> {
    let left = u64::try_from((bounds.until - now).num_milliseconds()).unwrap_or(0).div_ceil(1000);
    let none_left = [("steps", u64::from(bounds.steps)), ("turns", u64::from(bounds.turns)), ("wall time", left)];
    if let Some((limit, _)) = none_left.iter().find(|(_, left)| *left == 0) {
        return Err(format!(
            "the step this thread answers has no {limit} left for a run started answering it: its grant is spent, \
             held by the runs already started answering it, or none"
        ));
    }
    Ok(TrailLimits {
        steps: limits.steps.min(bounds.steps),
        turns: limits.turns.min(bounds.turns),
        wall_secs: limits.wall_secs.min(left),
        ..limits
    })
}

/// The steps and turns the grants of the run of `book` hold.
pub(super) fn held_by(book: &Book) -> (u32, u32) {
    book.header.grants.iter().fold((0, 0), |(steps, turns), grant| {
        let (held_steps, held_turns) = holding(grant, &book.pending);
        (steps.saturating_add(held_steps), turns.saturating_add(held_turns))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(steps: u32, turns: u32, until: DateTime<Utc>) -> StepBounds {
        StepBounds { steps, turns, until, route: vec!["deploy".parse().unwrap(), "review".parse().unwrap()] }
    }

    #[test]
    fn a_run_answering_a_step_takes_at_most_what_the_step_s_grant_leaves() {
        let now = Utc::now();
        let limits = within(TrailLimits::DEFAULT, &bounds(3, 500, now + chrono::Duration::hours(2)), now).unwrap();
        assert_eq!((limits.steps, limits.turns, limits.wall_secs), (3, 200, 7200));
        assert_eq!(limits.concurrency, TrailLimits::DEFAULT.concurrency);
        let asked = TrailLimits { steps: 2, turns: 10, wall_secs: 60, ..TrailLimits::DEFAULT };
        let limits = within(asked, &bounds(3, 500, now + chrono::Duration::hours(2)), now).unwrap();
        assert_eq!((limits.steps, limits.turns, limits.wall_secs), (2, 10, 60), "less asked for stays");
        for (bounds, limit) in [
            (bounds(0, 5, now + chrono::Duration::hours(1)), "steps"),
            (bounds(5, 0, now + chrono::Duration::hours(1)), "turns"),
            (bounds(5, 5, now - chrono::Duration::seconds(1)), "wall time"),
        ] {
            let refused = within(TrailLimits::DEFAULT, &bounds, now).unwrap_err();
            assert!(refused.contains(&format!("no {limit} left")), "{refused}");
        }
    }

    #[test]
    fn a_grant_is_held_while_its_step_waits_or_its_runs_run_less_what_they_spent() {
        let thread = ThreadId::generate();
        let spent = TrailSpent { steps: 2, turns: 5, running: 0 };
        let mut grant = StepGrant { call: 1, thread: thread.clone(), steps: 3, turns: 4, spent };
        let waiting = PendingCall {
            call: 1,
            function: ants_proto::TrailFunction::Agent,
            args: serde_json::Value::Null,
            to: "dev".parse().unwrap(),
            thread,
            message: uuid::Uuid::now_v7(),
            sent_at: Utc::now(),
            sent: true,
            turn_budget: 8,
            retries: 0,
        };
        assert_eq!(holding(&grant, std::slice::from_ref(&waiting)), (1, 0), "less what was spent, never below none");
        assert_eq!(holding(&grant, &[]), (0, 0), "given back once answered with no run running");
        grant.spent.running = 1;
        assert_eq!(holding(&grant, &[]), (1, 0), "held while a run answering it runs");
    }

    #[test]
    fn a_trail_on_the_step_s_route_is_refused_naming_it() {
        let step = TrailStep {
            run: RunId::generate(),
            call: 1,
            label: None,
            schema: None,
            trail: None,
            script: None,
            tip_key: None,
            bounds: None,
        };
        let bounds = bounds(3, 3, Utc::now());
        let refused = refused_on_route(&"review".parse().unwrap(), &step, &bounds).unwrap();
        assert!(refused.contains("deploy, review") && refused.contains(step.run.as_str()), "{refused}");
        assert_eq!(refused_on_route(&"other".parse().unwrap(), &step, &bounds), None);
    }
}
