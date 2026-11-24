//! `ants ant add NAME --on MACHINE`: the add order ([`AdminOp::Add`]) to the daemon of a machine, which has the ant's
//! key signed by `ants signer` on the order and appends its `[[ant]]` table with the settings given.

use std::path::PathBuf;
use std::time::Duration;

use ants_daemon::config::{self, Ant};
use ants_proto::{AdminOp, Machine, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use serde_json::{Map, Value};

use super::{Admin, Sent};
use crate::connection::Nats;
use crate::serve::AntOptions;

/// The arguments of `ants ant add`, which `/add` of `ants chat` takes too.
#[derive(clap::Args, Debug)]
pub struct Args {
    pub name: UserId,
    /// The machine to run the ant on.
    #[arg(long = "on", value_name = "MACHINE")]
    pub on: Machine,
    #[command(flatten)]
    pub options: Box<AntOptions>,
    /// How long to wait for the daemon's outcome (a duration such as 90s, 10m or 1h).
    #[arg(long, value_name = "DURATION", default_value = "3m", value_parser = ants_proto::duration::parse)]
    pub wait: Duration,
}

/// The settings of an `[[ant]]` table named `name` that `options` give, as an add or clone order carries them: no
/// name, and a workdir only when given.
pub fn settings(name: &UserId, options: &AntOptions) -> Result<Map<String, Value>> {
    let ant = options.apply(Ant::new(name.clone(), PathBuf::new()));
    let entry = config::entry(&ant)?;
    let parsed: toml::Table = toml::from_str(&entry).context("the settings given")?;
    let table = parsed.get("ant").and_then(|ants| ants.as_array()?.first()?.as_table().cloned()).unwrap_or_default();
    let Value::Object(mut settings) = serde_json::to_value(table)? else { bail!("the settings are no table") };
    settings.remove("name");
    if settings.get("workdir").and_then(Value::as_str) == Some("") {
        settings.remove("workdir");
    }
    Ok(settings)
}

/// `ants ant add`: send the order and wait up to `wait` for its outcome.
pub async fn run(nats: &Nats<'_>, args: Args) -> Result<()> {
    let settings = settings(&args.name, &args.options)?;
    let (_, js) = nats.connect("ants-ant-add").await?;
    println!("{}", add(&Admin::of(nats)?, &js, &args.name, args.on, settings, args.wait).await?);
    Ok(())
}

/// Send the add order for `name` with `settings`, signed by `admin`, to `machine`, and wait up to `wait` for its
/// outcome: what it did, or that it is still pending. Fails when the daemon refused it.
pub async fn add(
    admin: &Admin,
    js: &jetstream::Context,
    name: &UserId,
    machine: Machine,
    settings: Map<String, Value>,
    wait: Duration,
) -> Result<String> {
    let op = AdminOp::Add { settings, assistant_of: None };
    let outcome = match admin.send(js, Some(machine.clone()), name, op, wait).await? {
        Sent::Pending(order) => return Ok(super::pending_note(&order)),
        Sent::Refused(_, outcome) => {
            bail!("the daemon of {machine} refused to add {name}: {}", super::reason(&outcome.reason))
        }
        Sent::Applied(_, outcome) => outcome,
    };
    let key = outcome.result.as_ref().and_then(|r| r["key"].as_str()).map(|k| format!(" with key {k}"));
    Ok(format!("{name} added on {machine}{}; its daemon starts it.", key.unwrap_or_default()))
}

#[cfg(test)]
mod tests;
