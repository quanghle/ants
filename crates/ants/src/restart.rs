//! `ants ant restart`: an admin's order to restart an ant's harnesses ([`AdminOp::Restart`]), which the daemon serving
//! the ant carries out ([`ants_daemon::admin::restart`]).

use std::fmt::Write as _;
use std::time::Duration;

use ants_daemon::admin::restart::Restarted;
use ants_proto::{AdminOp, AdminOrder, AdminOutcome, Machine, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;
use serde_json::json;

use crate::connection::Nats;
use crate::orders::Admin;

/// How long to wait for the outcome before telling the order is pending.
pub const WAIT: Duration = Duration::from_secs(10);

/// `ants ant restart ANT`: order the daemon of `machine`, else of the machine that last recorded serving `ant`, to
/// restart its harnesses, cancelling their prompts if `now`, and tell what came of it.
pub async fn run(nats: &Nats<'_>, ant: &UserId, now: bool, machine: Option<Machine>, json: bool) -> Result<()> {
    let admin = Admin::of(nats)?;
    let (_, js) = nats.connect("ants-restart").await?;
    let (order, outcome) = send(&admin, &js, machine, ant, now).await?;
    if json {
        let printed = match &outcome {
            Some(outcome) => serde_json::to_value(outcome)?,
            None => json!({"id": order.id, "machine": order.machine, "ant": order.ant, "state": OrderState::Pending}),
        };
        println!("{printed}");
        if let Some(outcome) = outcome.filter(|o| o.state == OrderState::Refused) {
            bail!("{}", refusal(&outcome));
        }
        return Ok(());
    }
    println!("{}", told(&order, outcome)?);
    Ok(())
}

/// Send the order to restart `ant`'s harnesses as `admin`, waiting up to [`WAIT`] for its outcome.
pub async fn send(
    admin: &Admin,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    now: bool,
) -> Result<(AdminOrder, Option<AdminOutcome>)> {
    admin.send(js, machine, ant, AdminOp::Restart { now }, WAIT).await
}

/// What came of `order` in a line, or the reason it was refused as the error.
pub fn told(order: &AdminOrder, outcome: Option<AdminOutcome>) -> Result<String> {
    let (ant, machine) = (&order.ant, &order.machine);
    let Some(outcome) = outcome else {
        return Ok(format!(
            "restart of {ant} sent to {machine}, pending until its daemon takes the order (`ants ant orders {ant}`)"
        ));
    };
    if outcome.state == OrderState::Refused {
        bail!("{}", refusal(&outcome));
    }
    let restarted: Restarted =
        outcome.result.and_then(|result| serde_json::from_value(result).ok()).unwrap_or_default();
    let Restarted { stopped, after_prompt, cancelled } = restarted;
    let mut line = format!("restarting {ant} on {machine}: {stopped} idle {} stopped", harnesses(stopped));
    if after_prompt > 0 {
        let _ = write!(line, ", {after_prompt} working {} once the prompt ends", stop(after_prompt));
    }
    if cancelled > 0 {
        let _ = write!(line, " ({cancelled} cancelled)");
    }
    Ok(line)
}

fn refusal(outcome: &AdminOutcome) -> String {
    let reason = outcome.reason.as_deref().unwrap_or("no reason given");
    format!("{} refused to restart {}: {reason}", outcome.machine, outcome.ant)
}

fn harnesses(n: usize) -> &'static str {
    if n == 1 { "harness" } else { "harnesses" }
}

fn stop(n: usize) -> &'static str {
    if n == 1 { "stops" } else { "stop" }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(now: bool) -> AdminOrder {
        let (admin, machine, ant) = ("alice".parse().unwrap(), "m1".parse().unwrap(), "dev-a".parse().unwrap());
        AdminOrder::new(admin, machine, ant, AdminOp::Restart { now }, String::new())
    }

    #[test]
    fn a_restart_tells_what_it_stopped_or_why_not() {
        let now = order(true);
        let result = serde_json::to_value(Restarted { stopped: 1, after_prompt: 2, cancelled: 2 }).unwrap();
        let applied = AdminOutcome::of(&now, OrderState::Applied, None, Some(result));
        assert_eq!(
            told(&now, Some(applied)).unwrap(),
            "restarting dev-a on m1: 1 idle harness stopped, 2 working stop once the prompt ends (2 cancelled)"
        );
        let idle = AdminOutcome::of(
            &now,
            OrderState::Applied,
            None,
            Some(serde_json::to_value(Restarted::default()).unwrap()),
        );
        assert_eq!(told(&now, Some(idle)).unwrap(), "restarting dev-a on m1: 0 idle harnesses stopped");
        let reason = "machine m1 serves no ant dev-a".to_owned();
        let refused = AdminOutcome::of(&now, OrderState::Refused, Some(reason), None);
        let err = told(&now, Some(refused)).unwrap_err().to_string();
        assert_eq!(err, "m1 refused to restart dev-a: machine m1 serves no ant dev-a");
        assert!(told(&order(false), None).unwrap().contains("pending until its daemon takes the order"));
    }
}
