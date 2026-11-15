//! `ants ant`: one ant at a glance.

use std::path::PathBuf;

use ants_proto::UserId;
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::settings::{self, Assignment};
use crate::{ant, completions, orders};

#[derive(Subcommand, Debug)]
pub(crate) enum AntCommand {
    /// Show ANT as the ant view of `ants chat` does: where and how it runs (machine, harness, agent, model, workdir,
    /// capabilities, escalation chain, whoami) and what holds it back, as its roster entry tells; the messages waiting
    /// for it (`ants queue`), what it spent that UTC day (`ants spend`), its most recently active threads (`ants
    /// thread list --ant`), its settings as the config file of its machine has them, secrets hidden, and the latest
    /// admin orders to it (`ants ant orders`). Needs a human's credentials, and for the queue a daemon of the ant to
    /// answer. With an admin's credentials, reads the settings afresh with an admin order, else shows them as last
    /// read.
    Show {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object: the roster entry with its state, holds, queue, spend_today, threads, settings and orders.
        #[arg(long)]
        json: bool,
        /// Config file with the ants' caps (default: ~/.ants/config.toml, if it exists).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
    },
    /// List the admin orders to ANT, oldest first: who sent what, when, and whether its machine's daemon applied or
    /// refused it, and why, or whether it still waits for the daemon (pending). Needs a human's credentials.
    Orders {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object per order.
        #[arg(long)]
        json: bool,
    },
    /// Change settings of ANT in the config file of the machine serving it, with an admin order: read them first, as
    /// `ants ant show` does, and expect each field to hold the value read, so the daemon refuses the order, naming the
    /// field, should one have changed meanwhile. Prints the order's id, then waits for its outcome. A field of the
    /// ant's `[[ant]]` table, or of the `[harnesses]` table every ant of the machine shares (`harnesses.NAME.KEY`).
    /// Needs a human device's admin credentials.
    Set {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// KEY=VALUE: KEY a TOML key (`model`, `caps.daily`, `env.RUST_LOG`, `harnesses.copilot.command`), VALUE a
        /// TOML value (`3`, `true`, `["read"]`, `{ A = "1" }`), a bare word reading as a string; `KEY=` removes the
        /// field.
        #[arg(required = true, value_name = "KEY=VALUE")]
        changes: Vec<Assignment>,
        /// Print the order's id and return without waiting for its outcome (`ants ant orders` tells it).
        #[arg(long)]
        no_wait: bool,
    },
}

pub(crate) async fn run(nats: &crate::Nats<'_>, command: AntCommand) -> Result<()> {
    match command {
        AntCommand::Show { ant, json, config } => {
            let path = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            let caps = ants_daemon::config::Summary::load(&path)?.caps;
            ant::show(nats, ant, &caps, json).await
        }
        AntCommand::Orders { ant, json } => orders::show(nats, &ant, json).await,
        AntCommand::Set { ant, changes, no_wait } => {
            let wait = if no_wait { std::time::Duration::ZERO } else { settings::SET_WAIT };
            settings::set(nats, &ant, &changes, wait).await
        }
    }
}
