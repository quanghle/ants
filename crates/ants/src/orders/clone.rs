//! `ants ant clone SRC NEW`: the clone order ([`AdminOp::Clone`]) to the daemon of SRC's machine, which pauses SRC,
//! copies its home, sessions, thread history and worktrees to NEW, has NEW's key signed by `ants signer` on the order,
//! appends NEW's `[[ant]]` table, a copy of SRC's with the settings given over it, and resumes SRC.

use std::time::Duration;

use ants_proto::{AdminOp, Machine, OrderState, UserId};
use anyhow::{Result, bail};
use serde_json::Value;

use super::Admin;
use crate::Nats;
use crate::serve::AntOptions;

/// `ants ant clone`: send the order and wait up to `wait` for its outcome.
pub async fn run(
    nats: &Nats<'_>,
    src: &UserId,
    new: &UserId,
    machine: Option<Machine>,
    options: &AntOptions,
    wait: Duration,
) -> Result<()> {
    if new.is_human() {
        bail!("{new} is a human's name: an ant's name is lowercase");
    }
    let settings = super::add::settings(new, options)?;
    let (_, js) = nats.connect("ants-ant-clone").await?;
    let op = AdminOp::Clone { new: new.clone(), settings };
    let (order, outcome) = Admin::of(nats)?.send(&js, machine, src, op, wait).await?;
    let Some(outcome) = outcome else {
        println!("{}", super::pending_note(&order));
        return Ok(());
    };
    if outcome.state != OrderState::Applied {
        bail!("the daemon of {} refused to clone {src} as {new}: {}", order.machine, super::reason(&outcome));
    }
    println!("{}", summary(src, new, &order.machine, &outcome.result.unwrap_or_default()));
    Ok(())
}

/// What the clone did, a line each.
fn summary(src: &UserId, new: &UserId, machine: &Machine, result: &Value) -> String {
    let mut lines = vec![format!("{src} cloned as {new} on {machine}; its daemon starts {new}.")];
    let list =
        |key: &str| -> Vec<&str> { result[key].as_array().into_iter().flatten().filter_map(Value::as_str).collect() };
    if let Some(home) = result["home"].as_str() {
        lines.push(format!("Home: {home}."));
    }
    let (forked, restarted) = (list("forked"), list("restarted"));
    if !forked.is_empty() {
        lines.push(format!("Sessions forked: {}.", forked.join(", ")));
    }
    if !restarted.is_empty() {
        lines.push(format!("Sessions starting over from the thread's history: {}.", restarted.join(", ")));
    }
    let worktrees = list("worktrees");
    if !worktrees.is_empty() {
        lines.push(format!("Worktree branches: {}.", worktrees.join(", ")));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests;
