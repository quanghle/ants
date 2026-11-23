//! `ants ant restart`: an admin's order to restart an ant's harnesses ([`AdminOp::Restart`]), which the daemon serving
//! the ant carries out ([`ants_daemon::admin::restart`]).

use std::fmt::Write as _;
use std::time::Duration;

use ants_daemon::admin::restart::Restarted;
use ants_proto::{AdminOp, AdminOutcome, Machine, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;
use serde_json::json;

use crate::admin::orders::{self, Admin, Sent};
use crate::connection::Nats;

/// How long to wait for the outcome before telling the order is pending.
pub const WAIT: Duration = Duration::from_secs(10);

/// `ants ant restart ANT`: order the daemon of `machine`, else of the machine that last recorded serving `ant`, to
/// restart its harnesses, cancelling their prompts if `now`, and tell what came of it.
pub async fn run(nats: &Nats<'_>, ant: &UserId, now: bool, machine: Option<Machine>, json: bool) -> Result<()> {
    let admin = Admin::of(nats)?;
    let (_, js) = nats.connect("ants-restart").await?;
    let sent = send(&admin, &js, machine, ant, now).await?;
    if json {
        let printed = match &sent {
            Sent::Applied(_, outcome) | Sent::Refused(_, outcome) => serde_json::to_value(outcome)?,
            Sent::Pending(order) => {
                json!({"id": order.id, "machine": order.machine, "ant": order.ant, "state": OrderState::Pending})
            }
        };
        println!("{printed}");
        if let Sent::Refused(_, outcome) = &sent {
            bail!("{}", refusal(outcome));
        }
        return Ok(());
    }
    println!("{}", told(sent)?);
    Ok(())
}

/// Send the order to restart `ant`'s harnesses as `admin`, waiting up to [`WAIT`] for its outcome.
pub async fn send(
    admin: &Admin,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    now: bool,
) -> Result<Sent> {
    admin.send(js, machine, ant, AdminOp::Restart { now }, WAIT).await
}

/// What came of the restart order `sent` in a line, or the reason it was refused as the error.
pub fn told(sent: Sent) -> Result<String> {
    let (order, outcome) = match sent {
        Sent::Pending(order) => {
            let (ant, machine) = (&order.ant, &order.machine);
            return Ok(format!(
                "restart of {ant} sent to {machine}, pending until its daemon takes the order (`ants ant orders {ant}`)"
            ));
        }
        Sent::Refused(_, outcome) => bail!("{}", refusal(&outcome)),
        Sent::Applied(order, outcome) => (order, outcome),
    };
    let (ant, machine) = (&order.ant, &order.machine);
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
    format!("{} refused to restart {}: {}", outcome.machine, outcome.ant, orders::reason(&outcome.reason))
}

fn harnesses(n: usize) -> &'static str {
    if n == 1 { "harness" } else { "harnesses" }
}

fn stop(n: usize) -> &'static str {
    if n == 1 { "stops" } else { "stop" }
}

#[cfg(test)]
mod tests {
    use ants_proto::AdminOrder;

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
            told(Sent::of(now.clone(), Some(applied))).unwrap(),
            "restarting dev-a on m1: 1 idle harness stopped, 2 working stop once the prompt ends (2 cancelled)"
        );
        let idle = AdminOutcome::of(
            &now,
            OrderState::Applied,
            None,
            Some(serde_json::to_value(Restarted::default()).unwrap()),
        );
        assert_eq!(
            told(Sent::of(now.clone(), Some(idle))).unwrap(),
            "restarting dev-a on m1: 0 idle harnesses stopped"
        );
        let reason = "machine m1 serves no ant dev-a".to_owned();
        let refused = AdminOutcome::of(&now, OrderState::Refused, Some(reason), None);
        let err = told(Sent::of(now, Some(refused))).unwrap_err().to_string();
        assert_eq!(err, "m1 refused to restart dev-a: machine m1 serves no ant dev-a");
        assert!(told(Sent::of(order(false), None)).unwrap().contains("pending until its daemon takes the order"));
    }
}
