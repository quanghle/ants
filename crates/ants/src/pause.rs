//! `ants ant pause` and `ants ant resume`: an admin order pausing an ant, which then takes no new prompt and keeps
//! the messages to it queued, or resuming it ([`AdminOp::Pause`], [`AdminOp::Resume`]); the ant view of `ants chat`
//! sends them too.

use std::time::Duration;

use ants_proto::{AdminOp, AdminOrder, AdminOutcome, Machine, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;

use crate::orders::Admin;

/// How long the ant view waits for the order's outcome.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// Pause `ant`, or resume it, on `machine` or the machine serving it, and print how it went.
pub(crate) async fn run(
    nats: &crate::Nats<'_>,
    ant: &UserId,
    machine: Option<Machine>,
    paused: bool,
    wait: Duration,
) -> Result<()> {
    let admin = Admin::of(nats)?;
    let (_, js) = nats.connect(if paused { "ants-ant-pause" } else { "ants-ant-resume" }).await?;
    println!("{}", order(&admin, &js, ant, machine, paused, wait).await?);
    Ok(())
}

/// Send the order pausing or resuming `ant`, waiting up to `wait` for its outcome: the line telling how it went, or
/// why it was refused.
pub(crate) async fn order(
    admin: &Admin,
    js: &jetstream::Context,
    ant: &UserId,
    machine: Option<Machine>,
    paused: bool,
    wait: Duration,
) -> Result<String> {
    let op = if paused { AdminOp::Pause } else { AdminOp::Resume };
    let (order, outcome) = admin.send(js, machine, ant, op, wait).await?;
    said(&order, outcome.as_ref(), paused)
}

fn said(order: &AdminOrder, outcome: Option<&AdminOutcome>, paused: bool) -> Result<String> {
    let (ant, machine) = (&order.ant, &order.machine);
    let verb = if paused { "pause" } else { "resume" };
    match outcome.map(|o| (o.state, o.reason.as_deref())) {
        None | Some((OrderState::Pending | OrderState::Held, _)) => Ok(format!(
            "the order to {verb} {ant} waits for the daemon of {machine}: `ants ant orders {ant}` tells when it is \
             decided"
        )),
        Some((OrderState::Applied, _)) if paused => Ok(format!(
            "{ant} paused on {machine}: it takes no new prompt and the messages to it wait queued until `ants ant \
             resume {ant}`"
        )),
        Some((OrderState::Applied, _)) => Ok(format!("{ant} resumed on {machine}")),
        Some((OrderState::Refused, reason)) => {
            bail!("{machine} refused to {verb} {ant}: {}", reason.unwrap_or("no reason given"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(name: &str) -> UserId {
        name.parse().unwrap()
    }

    #[test]
    fn tells_how_the_order_went() {
        let order = AdminOrder::new(p("alice"), "m1".parse().unwrap(), p("dev-a"), AdminOp::Pause, "");
        let outcome = |state, reason: Option<&str>| AdminOutcome::of(&order, state, reason.map(Into::into), None);
        let said = |outcome: Option<&AdminOutcome>, paused| said(&order, outcome, paused);
        assert_eq!(
            said(Some(&outcome(OrderState::Applied, None)), true).unwrap(),
            "dev-a paused on m1: it takes no new prompt and the messages to it wait queued until `ants ant resume dev-a`"
        );
        assert_eq!(said(Some(&outcome(OrderState::Applied, None)), false).unwrap(), "dev-a resumed on m1");
        let refused = said(Some(&outcome(OrderState::Refused, Some("no config"))), true).unwrap_err();
        assert_eq!(refused.to_string(), "m1 refused to pause dev-a: no config");
        assert!(said(None, false).unwrap().starts_with("the order to resume dev-a waits for the daemon of m1"));
    }
}
