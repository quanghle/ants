//! `ants ant retire ANT`: the retire order ([`AdminOp::Retire`]) to the daemon of the ant's machine, which runs the
//! `ants retire` flow there for any ant: its `[[ant]]` table goes, its key is revoked by `ants signer` on the order and
//! its credentials are deleted; its home stays. An ant leading a team is refused before the order is sent, and a
//! retired ant leaves its team: `ants signer` takes it off as it revokes the key, and the CLI makes sure.

use std::time::Duration;

use ants_proto::{AdminOp, JsDomain, Machine, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;

use super::{Admin, Sent};
use crate::connection::Nats;
use crate::signing::retire::stop_note;

/// How long `ants ant retire` waits for the outcome by default: the daemon waits up to 5m for the ant to stop, then
/// up to 2m for `ants signer`.
pub const WAIT: Duration = Duration::from_mins(8);

/// `ants ant retire`: send the order and wait up to `wait` for its outcome.
pub async fn run(nats: &Nats<'_>, ant: &UserId, machine: Option<Machine>, wait: Duration) -> Result<()> {
    let (_, js) = nats.connect("ants-ant-retire").await?;
    println!("{}", retire(&Admin::of(nats)?, &js, nats.domain, ant, machine, wait).await?);
    Ok(())
}

/// Send the retire order for `ant`, signed by `admin`, to `machine`, else to the machine that last recorded serving it, and wait up to
/// `wait` for its outcome: what it did, a line each, or that it is still pending. Fails when the daemon refused it; once retired, the ant leaves its team in the directory of `js`, of the
/// JetStream domain `js_domain`.
pub async fn retire(
    admin: &Admin,
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    ant: &UserId,
    machine: Option<Machine>,
    wait: Duration,
) -> Result<String> {
    let (order, outcome) = match admin.send(js, machine, ant, AdminOp::Retire, wait).await? {
        Sent::Pending(order) => return Ok(super::pending_note(&order)),
        Sent::Refused(order, outcome) => {
            bail!("the daemon of {} refused to retire {ant}: {}", order.machine, super::reason(&outcome.reason))
        }
        Sent::Applied(order, outcome) => (order, outcome),
    };
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
    lines.extend(crate::teams::drop_ant_or_say(js, js_domain, ant).await.map(|line| format!("{}.", capital(&line))));
    Ok(lines.join("\n"))
}

/// `line` with its first letter in capitals, as the lines of an outcome start.
fn capital(line: &str) -> String {
    let mut chars = line.chars();
    chars.next().map_or_default(|first| first.to_uppercase().chain(chars).collect())
}
