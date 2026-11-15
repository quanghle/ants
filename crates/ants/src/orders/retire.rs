//! `ants ant retire ANT`: the retire order ([`AdminOp::Retire`]) to the daemon of the ant's machine, which runs the
//! `ants retire` flow there for any ant: its `[[ant]]` table goes, its key is revoked by `ants signer` on the order and
//! its credentials are deleted; its home stays.

use std::time::Duration;

use ants_proto::{AdminOp, Machine, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;

use super::Admin;
use crate::Nats;
use crate::retire::stop_note;

/// How long `ants ant retire` waits for the outcome by default: the daemon waits up to 5m for the ant to stop, then
/// up to 2m for `ants signer`.
pub const WAIT: Duration = Duration::from_mins(8);

/// `ants ant retire`: send the order and wait up to `wait` for its outcome.
pub async fn run(nats: &Nats<'_>, ant: &UserId, machine: Option<Machine>, wait: Duration) -> Result<()> {
    let (_, js) = nats.connect("ants-ant-retire").await?;
    println!("{}", retire(&Admin::of(nats)?, &js, ant, machine, wait).await?);
    Ok(())
}

/// Send the retire order for `ant`, signed by `admin`, to `machine`, else to the machine that last recorded serving it, and wait up to
/// `wait` for its outcome: what it did, a line each, or that it is still pending. Fails when the daemon refused it.
pub async fn retire(
    admin: &Admin,
    js: &jetstream::Context,
    ant: &UserId,
    machine: Option<Machine>,
    wait: Duration,
) -> Result<String> {
    let (order, outcome) = admin.send(js, machine, ant, AdminOp::Retire, wait).await?;
    let Some(outcome) = outcome else { return Ok(super::pending_note(&order)) };
    if outcome.state != OrderState::Applied {
        bail!("the daemon of {} refused to retire {ant}: {}", order.machine, super::reason(&outcome));
    }
    let result = outcome.result.unwrap_or_default();
    let mut lines = vec![format!("{ant} retired on {}.", order.machine)];
    lines.push(stop_note(ant, result["stopped"].as_bool().unwrap_or(false)));
    let revoked: Vec<&str> = result["revoked"].as_array().into_iter().flatten().filter_map(|k| k.as_str()).collect();
    if !revoked.is_empty() {
        lines.push(format!("Revoked {}.", revoked.join(", ")));
    }
    if let Some(error) = result["push_error"].as_str() {
        lines.push(format!(
            "Pushing the revocation failed ({error}): `ants auth push` where the account key is pushes it."
        ));
    }
    if let Some(home) = result["home"].as_str() {
        lines.push(format!("Its home {home} stays on {}.", order.machine));
    }
    Ok(lines.join("\n"))
}
