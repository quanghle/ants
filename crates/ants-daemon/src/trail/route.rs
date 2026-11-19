//! A run's route: a goto() call ends the run's leg where the script makes it and starts the next, in the trail it
//! names (the leg's own without one) at the waypoint it names (the trail's start without one), with its state as the
//! leg's input. The run keeps its id, conductor, owner, origin, thread, limits and spending; the leg it left is given
//! up as a stop gives up a run's steps. The conductor refuses a goto() it cannot follow, raising in the script, where
//! it can take another exit: one to a trail or waypoint that does not exist, to one the run entered before, or, for a
//! run an untrusted thread started, to an inline script.

use ants_proto::{TrailLeg, TrailName, TrailRun};
use ants_trail::{Call, Function, Run};
use anyhow::Result;

use super::{Leg, Slot, Trails, driver, steps};

impl Trails {
    /// Take the goto() calls the run of `slot`, `engine`, waits on, in call order: each refused is answered with why,
    /// raising in the script, until one starts the next leg. Whether there was one, for the run to compute again.
    pub(super) async fn take_gotos(&self, slot: &Slot, engine: &mut Run) -> Result<bool> {
        let mut gotos: Vec<Call> =
            engine.outstanding().iter().filter(|call| call.function == Function::Goto).cloned().collect();
        gotos.sort_by_key(|call| call.call);
        let header = slot.header();
        for call in &gotos {
            match self.next_leg(&header, engine, call).await {
                Ok(next) => {
                    self.go_on(slot, engine, next).await?;
                    return Ok(true);
                }
                Err(why) => {
                    tracing::info!(run = %header.run, call = call.call, why, "a goto() refused");
                    let entry = self.raise(&header.run, call, why).await?;
                    engine.answer(entry.call, entry.answer)?;
                }
            }
        }
        Ok(!gotos.is_empty())
    }

    /// The leg `call`, a goto() of the run of `header`, `engine`, leads to, its run and its session as JSON; or why the
    /// conductor refuses it.
    async fn next_leg(&self, header: &TrailRun, engine: &Run, call: &Call) -> Result<(TrailLeg, Run, String), String> {
        let args = steps::GotoArgs::of(&call.args)?;
        let current = header.leg().ok_or("the run is on no leg")?;
        let (trail, script) = match args.trail {
            None => (current.trail.clone(), current.script.clone()),
            Some(name) => {
                let name: TrailName = name.parse().map_err(|e| format!("goto() names no trail as {name:?}: {e}"))?;
                let script = self.inner.saved.script(&name).await?;
                (Some(name), script)
            }
        };
        if trail.is_none() && !header.origin.trusted() {
            return Err("a run started from a webhook's thread goes only to saved trails".to_owned());
        }
        let first_call = engine.state().next_call();
        let next = Leg { trail, script, at: args.at, args: args.state, first_call }.prepare(&header.limits)?;
        let leg = &next.0;
        if let Some(entered) = header.route.iter().find(|entered| entered.trail == leg.trail && entered.at == leg.at) {
            let entered = place(entered);
            return Err(format!("the run entered {entered} before; a run enters a trail at each waypoint only once"));
        }
        let mut moved = header.clone();
        moved.route.push(leg.clone());
        let size = serde_json::to_vec(&moved).map_or(usize::MAX, |moved| moved.len());
        if size > driver::MAX_RECORD {
            let max = driver::MAX_RECORD;
            return Err(format!("the run's record has no room for another leg: {size} bytes, past the {max} it holds"));
        }
        Ok(next)
    }

    /// End the leg the run of `slot`, `engine`, is on, and go on with `next`: the leg, its run and session. The steps
    /// of the leg ended are given up once the run's header lists the next.
    async fn go_on(&self, slot: &Slot, engine: &mut Run, next: (TrailLeg, Run, String)) -> Result<()> {
        let (leg, run, session) = next;
        tracing::info!(run = %slot.header().run, trail = ?leg.trail, at = ?leg.at, "a trail run goes on to its next leg");
        let (header, dropped) = {
            let mut book = slot.book();
            book.header.route.push(leg);
            book.header.phase = None;
            book.session = session;
            book.queued.clear();
            book.answers.clear();
            book.broke = None;
            let dropped = std::mem::take(&mut book.pending);
            book.left.extend(dropped.iter().cloned());
            (book.header.clone(), dropped)
        };
        for call in &dropped {
            self.retire(&header.run, call);
        }
        *engine = run;
        // The header first: a session stored for a leg its header does not list yet would be one no restart finds. The
        // session last: until it is stored, a restart finds the steps left in the session before, and gives them up.
        let stored = async {
            self.save_header(slot).await?;
            self.abandon_left(slot).await;
            self.save_session(slot).await
        };
        let stored = stored.await;
        if stored.is_ok() {
            self.report_route(&header).await;
        }
        self.wake_starved(&header);
        stored
    }

    /// Give up the steps of the leg the run of `slot` left, once its stored header lists the next: done again after a
    /// restart, giving up a step twice withdraws it once and tells it dropped again.
    pub(super) async fn give_up_left(&self, slot: &Slot) -> Result<()> {
        if slot.book().left.is_empty() {
            return Ok(());
        }
        self.save_header(slot).await?;
        self.abandon_left(slot).await;
        Ok(())
    }

    /// Give up the steps of the leg the run of `slot` left, its stored header listing the next.
    async fn abandon_left(&self, slot: &Slot) {
        let (header, left) = {
            let mut book = slot.book();
            (book.header.clone(), std::mem::take(&mut book.left))
        };
        self.abandon(&header, &left).await;
    }
}

/// Where `leg` entered its trail, for telling.
fn place(leg: &TrailLeg) -> String {
    let trail = leg.trail.as_ref().map_or_else(|| "its inline script".to_owned(), |trail| format!("trail {trail}"));
    match &leg.at {
        Some(at) => format!("{trail} at waypoint {at}"),
        None => format!("{trail} at its start"),
    }
}
