//! `ants ant`: one ant at a glance.

use std::path::PathBuf;

use ants_proto::UserId;
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

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
    }
}
