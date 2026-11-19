//! Runs started from a step: an ant answering a step that starts a run with `run_trail` spends from the step's run.
//! The step carries what its run had left ([`StepBounds`]), the run starts under at most that and takes no trail of
//! the step's route ([`SpendsFrom`]), and what it spends goes back with the replies to the step ([`TrailSpent`]), which
//! the step's conductor charges to the step's run and its ancestors, as a child run's steps and turns are.
//!
//! The step's recipient tells, with each reply to the step, what its runs started answering the step spent since its
//! last reply told; what they spend after the last reply goes untold, within the bounds they started under.

use std::collections::HashMap;

use ants_proto::{
    Envelope, RunId, SpendsFrom, StepBounds, TrailLimits, TrailName, TrailRun, TrailSpent, TrailStep, UserId,
};
use chrono::{DateTime, Utc};

use super::{Slot, Trails, driver, locked};

/// A step answered by runs the conductor started with `run_trail`: its conductor, run and call.
pub(super) type StepKey = (UserId, RunId, u32);

/// The runs started answering a step, and what the replies to the step told of their spending so far.
#[derive(Debug, Clone, Default)]
pub(super) struct Answering {
    runs: Vec<RunId>,
    told: TrailSpent,
}

/// The runs started answering steps, by step.
pub(super) type Answered = HashMap<StepKey, Answering>;

impl Trails {
    /// `message`, a step of the run of `header`, carrying what a run its recipient starts answering it may spend.
    pub(super) fn bounded(&self, header: &TrailRun, mut message: Envelope) -> Envelope {
        if let Some(step) = message.trail_step.as_deref_mut() {
            step.bounds = Some(self.bounds(header));
        }
        message
    }

    /// What a run started answering a step of the run of `header` may spend: what the run's tree has left, until the
    /// run's wall time ends, taking no trail on the routes of the run, its ancestors and the runs they spend from.
    fn bounds(&self, header: &TrailRun) -> StepBounds {
        let budget = self.budget(header);
        let mut route: Vec<TrailName> = Vec::new();
        let ancestors = self.ancestors(header).iter().map(|slot| slot.header()).collect::<Vec<_>>();
        for run in std::iter::once(header).chain(&ancestors) {
            let legs = run.route.iter().filter_map(|leg| leg.trail.clone());
            let outer = run.spends_from.iter().flat_map(|from| from.route.iter().cloned());
            for trail in legs.chain(outer) {
                if !route.contains(&trail) {
                    route.push(trail);
                }
            }
        }
        let wall = chrono::Duration::from_std(header.limits.wall_time()).unwrap_or(chrono::Duration::MAX);
        StepBounds {
            steps: budget.limits.steps.saturating_sub(budget.steps),
            turns: budget.turns,
            until: header.started_at.checked_add_signed(wall).unwrap_or(DateTime::<Utc>::MAX_UTC),
            route,
        }
    }

    /// Keep that `run`, started from the step `from`, spends from it, for the replies to the step to tell.
    pub(super) fn answering(&self, run: &RunId, from: &SpendsFrom) {
        let key = (from.conductor.clone(), from.run.clone(), from.call);
        locked(&self.inner.answered).entry(key).or_default().runs.push(run.clone());
    }

    /// What the runs the ant started answering `step`, of `conductor`'s run, spent since the last reply to it told,
    /// taken as told: none when they spent nothing more. A step whose runs all ended is forgotten once told.
    pub async fn spent_answering(&self, conductor: &UserId, step: &TrailStep) -> Option<TrailSpent> {
        let key = (conductor.clone(), step.run.clone(), step.call);
        let runs = locked(&self.inner.answered).get(&key)?.runs.clone();
        let (mut total, mut ended) = (TrailSpent::default(), true);
        for run in &runs {
            match self.run(run).await {
                Ok(Some(header)) => {
                    total.steps = total.steps.saturating_add(header.steps);
                    total.turns = total.turns.saturating_add(header.turns);
                    ended &= header.finished();
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(run = %run, error = e, "reading a run started answering a step");
                    ended = false;
                }
            }
        }
        let mut answered = locked(&self.inner.answered);
        let entry = answered.get_mut(&key)?;
        let spent = TrailSpent {
            steps: total.steps.saturating_sub(entry.told.steps),
            turns: total.turns.saturating_sub(entry.told.turns),
        };
        entry.told = TrailSpent { steps: total.steps.max(entry.told.steps), turns: total.turns.max(entry.told.turns) };
        if ended && entry.runs.len() == runs.len() {
            answered.remove(&key);
        }
        (!spent.is_none()).then_some(spent)
    }

    /// Charge `spent`, what runs started answering a step of the run of `slot` spent, to the run and its ancestors.
    pub(super) fn charge_spent(&self, slot: &Slot, header: &TrailRun, spent: TrailSpent) {
        if spent.is_none() {
            return;
        }
        {
            let mut book = slot.book();
            driver::count_steps(&mut book.header, spent.steps);
            book.header.turns = book.header.turns.saturating_add(spent.turns);
        }
        self.charge(header, spent.steps, spent.turns);
    }
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

/// The limits of a run asking for `limits`, started at `now` answering a step under `bounds`: each cut to what the step's
/// run has left; refused when it has no steps, turns or wall time left.
pub fn within(limits: TrailLimits, bounds: &StepBounds, now: DateTime<Utc>) -> Result<TrailLimits, String> {
    let left = u64::try_from((bounds.until - now).num_milliseconds()).unwrap_or(0).div_ceil(1000);
    let none_left = [("steps", u64::from(bounds.steps)), ("turns", u64::from(bounds.turns)), ("wall time", left)];
    if let Some((limit, _)) = none_left.iter().find(|(_, left)| *left == 0) {
        return Err(format!(
            "the run whose step this thread answers has no {limit} left, which a run started answering it spends from"
        ));
    }
    Ok(TrailLimits {
        steps: limits.steps.min(bounds.steps),
        turns: limits.turns.min(bounds.turns),
        wall_secs: limits.wall_secs.min(left),
        ..limits
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds(steps: u32, turns: u32, until: DateTime<Utc>) -> StepBounds {
        StepBounds { steps, turns, until, route: vec!["deploy".parse().unwrap(), "review".parse().unwrap()] }
    }

    #[test]
    fn a_run_answering_a_step_takes_at_most_what_the_step_s_run_has_left() {
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
