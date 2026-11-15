//! `ants ant`: one ant at a glance.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{Machine, UserId};
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::serve::AntOptions;
use crate::{ant, completions, orders};

#[derive(Subcommand, Debug)]
pub(crate) enum AntCommand {
    /// Show ANT as the ant view of `ants chat` does: where and how it runs (machine, harness, agent, model, workdir,
    /// capabilities, escalation chain, whoami) and what holds it back, as its roster entry tells; the messages waiting
    /// for it (`ants queue`), what it spent that UTC day (`ants spend`) and its most recently active threads (`ants
    /// thread list --ant`). Needs a human's credentials, and for the queue a daemon of the ant to answer.
    Show {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object: the roster entry with its state, holds, queue, spend_today and threads.
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
    /// Add the ant NAME on MACHINE by admin order: its daemon generates the ant's key, has `ants signer` sign it on
    /// the order, writes its credentials, appends its `[[ant]]` table with the settings given and starts it. Paths
    /// are MACHINE's; the workdir defaults to the ant's home, ~/.ants/NAME there. Needs a human device's admin
    /// credentials, and `ants signer` running.
    Add {
        name: UserId,
        /// The machine to run the ant on.
        #[arg(long = "on", value_name = "MACHINE")]
        on: Machine,
        #[command(flatten)]
        options: Box<AntOptions>,
        /// How long to wait for the daemon's outcome (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", default_value = "3m", value_parser = ants_proto::duration::parse)]
        wait: Duration,
    },
    /// Retire ANT by admin order, templated or not: its machine's daemon removes its `[[ant]]` table, which stops
    /// it, has `ants signer` revoke its key on the order and deletes its credentials; its home stays. Needs a human
    /// device's admin credentials, and `ants signer` running.
    Retire {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// The machine running the ant (default: the one that last recorded serving it).
        #[arg(long = "on", value_name = "MACHINE")]
        on: Option<Machine>,
        /// How long to wait for the daemon's outcome (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", default_value = "8m", value_parser = ants_proto::duration::parse)]
        wait: Duration,
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
        AntCommand::Add { name, on, options, wait } => orders::add::run(nats, &name, on, &options, wait).await,
        AntCommand::Retire { ant, on, wait } => orders::retire::run(nats, &ant, on, wait).await,
    }
}
