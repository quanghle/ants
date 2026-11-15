//! `ants ant add NAME --on MACHINE`: the add order ([`AdminOp::Add`]) to the daemon of a machine, which has the ant's
//! key signed by `ants signer` on the order and appends its `[[ant]]` table with the settings given.

use std::path::PathBuf;
use std::time::Duration;

use ants_daemon::config::{self, Ant};
use ants_proto::{AdminOp, Machine, OrderState, UserId};
use anyhow::{Context as _, Result, bail};
use serde_json::{Map, Value};

use crate::Nats;
use crate::serve::AntOptions;

/// The settings of an `[[ant]]` table named `name` that `options` give, as an add order carries them: no name, and a
/// workdir only when given.
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
pub async fn run(nats: &Nats<'_>, name: &UserId, machine: Machine, options: &AntOptions, wait: Duration) -> Result<()> {
    if name.is_human() {
        bail!("{name} is a human's name: an ant's name is lowercase");
    }
    let settings = settings(name, options)?;
    let (_, js) = nats.connect("ants-ant-add").await?;
    let (order, outcome) =
        super::send(nats, &js, Some(machine.clone()), name, AdminOp::Add { settings, assistant_of: None }, wait)
            .await?;
    let Some(outcome) = outcome else {
        println!("{}", super::pending_note(&order));
        return Ok(());
    };
    match outcome.state {
        OrderState::Applied => {
            let key = outcome.result.as_ref().and_then(|r| r["key"].as_str()).map(|k| format!(" with key {k}"));
            println!("{name} added on {machine}{}; its daemon starts it.", key.unwrap_or_default());
            Ok(())
        }
        _ => bail!("the daemon of {machine} refused to add {name}: {}", super::reason(&outcome)),
    }
}

#[cfg(test)]
pub(crate) mod tests;
