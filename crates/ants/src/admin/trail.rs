//! `ants ant stop-trail` and `ants ant resume-trail`: an admin's order to stop or resume a trail run an ant conducts
//! ([`AdminOp::StopTrail`], [`AdminOp::ResumeTrail`]), which the daemon of the ant's machine carries out as the
//! conductor answers `ants trail stop` and `ants trail resume` ([`ants_daemon::admin::trail`]); an order to a machine
//! whose daemon is away waits for it.

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{AdminOp, AdminOrder, AdminOutcome, Machine, OrderState, TrailOutcome, UserId};
use anyhow::{Result, bail};

use crate::admin::orders::Admin;
use crate::cmd::trail::runs::{self, Pick};
use crate::cmd::trail::{LimitArgs, control};
use crate::connection::Nats;

/// Where an order about a run goes: the run, of `ant`, on `machine`, else the machine that last recorded serving it,
/// waiting up to `wait` for the outcome.
pub(crate) struct Ordered<'a> {
    pub(crate) ant: &'a UserId,
    pub(crate) run: &'a Pick,
    pub(crate) machine: Option<Machine>,
    pub(crate) wait: Duration,
}

/// `ants ant stop-trail ANT RUN`: order the run stopped and print how it went.
pub(crate) async fn stop(nats: &Nats<'_>, ordered: Ordered<'_>) -> Result<()> {
    let admin = Admin::of(nats)?;
    let (_, js) = nats.connect("ants-ant-stop-trail").await?;
    let header = runs::pick_of(&js, ordered.ant, ordered.run).await?;
    if header.finished() {
        bail!("run {} of {} has ended: {}", header.run, header.conductor, header.state.name());
    }
    let op = AdminOp::StopTrail { run: header.run };
    let (order, outcome) = admin.send(&js, ordered.machine, ordered.ant, op, ordered.wait).await?;
    print!("{}", told(&order, outcome.as_ref())?);
    Ok(())
}

/// `ants ant resume-trail ANT RUN`: order the run resumed, under the limits given over its own, and print how it went.
pub(crate) async fn resume(nats: &Nats<'_>, ordered: Ordered<'_>, limits: LimitArgs) -> Result<()> {
    let admin = Admin::of(nats)?;
    let (_, js) = nats.connect("ants-ant-resume-trail").await?;
    let header = runs::pick_of(&js, ordered.ant, ordered.run).await?;
    if let Some(ended) = control::ended(&header) {
        bail!("run {} of {} has ended: {ended}", header.run, header.conductor);
    }
    let limits = if limits.given() { Some(limits.over(header.limits)?) } else { None };
    let op = AdminOp::ResumeTrail { run: header.run, limits };
    let (order, outcome) = admin.send(&js, ordered.machine, ordered.ant, op, ordered.wait).await?;
    print!("{}", told(&order, outcome.as_ref())?);
    Ok(())
}

/// What came of `order`, a stop or resume of a run, in lines: the steps a stop left in other ants' queues with the
/// command withdrawing each; or why it was refused, as the error.
fn told(order: &AdminOrder, outcome: Option<&AdminOutcome>) -> Result<String> {
    let (ant, machine) = (&order.ant, &order.machine);
    let (verb, run) = match &order.op {
        AdminOp::StopTrail { run } => ("stop", run),
        AdminOp::ResumeTrail { run, .. } => ("resume", run),
        other => bail!("order {} is no order about a trail run: {}", order.id, other.describe()),
    };
    let Some(outcome) = outcome.filter(|o| !matches!(o.state, OrderState::Pending | OrderState::Held)) else {
        return Ok(format!(
            "the order to {verb} run {run} of {ant} waits for the daemon of {machine}: `ants ant orders {ant}` tells \
             when it is decided\n"
        ));
    };
    if outcome.state == OrderState::Refused {
        let reason = outcome.reason.as_deref().unwrap_or("no reason given");
        bail!("{machine} refused to {verb} run {run} of {ant}: {reason}");
    }
    let mut out = String::new();
    match &order.op {
        AdminOp::ResumeTrail { limits: Some(limits), .. } => {
            let _ = writeln!(out, "resumed run {run} of {ant} on {machine} under {limits}");
        }
        AdminOp::ResumeTrail { limits: None, .. } => {
            let _ = writeln!(out, "resumed run {run} of {ant} on {machine}");
        }
        _ => {
            let _ = writeln!(
                out,
                "stopped run {run} of {ant} on {machine}: no further step goes out until `ants ant resume-trail {ant} \
                 {run}`, which sends the steps waited on anew"
            );
            let answer = outcome.result.clone().and_then(|result| serde_json::from_value(result).ok());
            if let Some(TrailOutcome::Stopped { waiting, .. }) = answer
                && !waiting.is_empty()
            {
                out.push_str("steps still waiting in the queues of other ants (withdrawing one not started yet):\n");
                for step in &waiting {
                    let (to, message) = (&step.to, step.message);
                    let _ = writeln!(out, "  {to} message {message}: ants withdraw {to} {message}");
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use ants_proto::{RunId, TrailLimits, Uuid, WaitingStep};

    use super::*;

    fn p(name: &str) -> UserId {
        name.parse().unwrap()
    }

    #[test]
    fn tells_what_the_order_stopped_or_resumed_or_why_not() {
        let run = RunId::generate();
        let order = |op| AdminOrder::new(p("alice"), "m1".parse().unwrap(), p("lead"), op, "");
        let stop = order(AdminOp::StopTrail { run: run.clone() });
        let decided = |order: &AdminOrder, state, reason: Option<&str>, result: Option<TrailOutcome>| {
            let result = result.map(|r| serde_json::to_value(r).unwrap());
            AdminOutcome::of(order, state, reason.map(Into::into), result)
        };
        let message = Uuid::now_v7();
        let waiting = vec![WaitingStep { to: p("dev-a"), message }];
        let stopped =
            decided(&stop, OrderState::Applied, None, Some(TrailOutcome::Stopped { run: run.clone(), waiting }));
        assert_eq!(
            told(&stop, Some(&stopped)).unwrap(),
            format!(
                "stopped run {run} of lead on m1: no further step goes out until `ants ant resume-trail lead {run}`, \
                 which sends the steps waited on anew\nsteps still waiting in the queues of other ants (withdrawing \
                 one not started yet):\n  dev-a message {message}: ants withdraw dev-a {message}\n"
            )
        );
        let limits = TrailLimits { steps: 100, wall_secs: 2 * 86_400, ..TrailLimits::DEFAULT };
        let resume = order(AdminOp::ResumeTrail { run: run.clone(), limits: Some(limits) });
        let resumed = decided(&resume, OrderState::Applied, None, Some(TrailOutcome::Resumed { run: run.clone() }));
        assert_eq!(
            told(&resume, Some(&resumed)).unwrap(),
            format!("resumed run {run} of lead on m1 under 100 steps, 4 at once, 200 turns, 2d of wall time\n")
        );
        let refused = decided(&resume, OrderState::Refused, Some("run is not stopped"), None);
        let error = told(&resume, Some(&refused)).unwrap_err().to_string();
        assert_eq!(error, format!("m1 refused to resume run {run} of lead: run is not stopped"));
        let held = decided(&stop, OrderState::Held, None, None);
        for pending in [None, Some(&held)] {
            assert!(told(&stop, pending).unwrap().starts_with(&format!("the order to stop run {run} of lead waits")));
        }
    }
}
