//! `ants ant clone SRC NEW`: the clone order ([`AdminOp::Clone`]) to the daemon of SRC's machine, which pauses SRC,
//! copies its home, sessions, thread history and worktrees to NEW, has NEW's key signed by `ants signer` on the order,
//! appends NEW's `[[ant]]` table, a copy of SRC's with the settings given over it, and resumes SRC.

use std::time::Duration;

use ants_proto::{AdminOp, Machine, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;
use clap_complete::engine::ArgValueCandidates;
use serde_json::{Map, Value};

use super::Admin;
use crate::completions;
use crate::connection::Nats;
use crate::serve::AntOptions;

/// The arguments of `ants ant clone`, which `/clone` of `ants chat` takes too.
#[derive(clap::Args, Debug)]
pub struct Args {
    #[arg(add = ArgValueCandidates::new(completions::ants))]
    pub src: UserId,
    pub new: UserId,
    /// The machine running SRC (default: the one that last recorded serving it).
    #[arg(long)]
    pub machine: Option<Machine>,
    #[command(flatten)]
    pub options: Box<AntOptions>,
    /// How long to wait for the daemon's outcome (a duration such as 90s, 10m or 1h).
    #[arg(long, value_name = "DURATION", default_value = "40m", value_parser = ants_proto::duration::parse)]
    pub wait: Duration,
}

/// `ants ant clone`: send the order and wait up to `wait` for its outcome.
pub async fn run(nats: &Nats<'_>, args: Args) -> Result<()> {
    let settings = super::add::settings(&args.new, &args.options)?;
    let (_, js) = nats.connect("ants-ant-clone").await?;
    println!("{}", clone(&Admin::of(nats)?, &js, &args.src, &args.new, args.machine, settings, args.wait).await?);
    Ok(())
}

/// Send the order cloning `src` as `new` with `settings` over its own, signed by `admin`, to `machine`, else to the
/// machine that last recorded serving `src`, and wait up to `wait` for its outcome: what it did, a line each, or that
/// it is still pending. Fails when the daemon refused it.
pub async fn clone(
    admin: &Admin,
    js: &jetstream::Context,
    src: &UserId,
    new: &UserId,
    machine: Option<Machine>,
    settings: Map<String, Value>,
    wait: Duration,
) -> Result<String> {
    let op = AdminOp::Clone { new: new.clone(), settings };
    let (order, outcome) = admin.send(js, machine, src, op, wait).await?;
    let Some(outcome) = outcome else { return Ok(super::pending_note(&order)) };
    if outcome.state != OrderState::Applied {
        bail!("the daemon of {} refused to clone {src} as {new}: {}", order.machine, super::reason(&outcome));
    }
    Ok(summary(src, new, &order.machine, &outcome.result.unwrap_or_default()))
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
