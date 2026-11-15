//! `ants ant`: one ant at a glance.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{Machine, UserId};
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::{ant, completions, orders, pause, restart};

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
    /// Order ANT's daemon to restart its harnesses: idle ones stop at once, working ones once their current prompt
    /// ends, and each thread's next prompt resumes its session in a new harness. Waits up to 10 s for the outcome, else
    /// the order stays pending until the daemon takes it (`ants ant orders`). Needs a human device's admin
    /// credentials.
    Restart {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// Cancel the prompts running too, as `ants cancel` does, and stop their harnesses at once.
        #[arg(long)]
        now: bool,
        /// The machine whose daemon serves ANT (default: the machine that last recorded serving it).
        #[arg(long)]
        machine: Option<Machine>,
        /// The outcome as one JSON object.
        #[arg(long)]
        json: bool,
    },
    /// Pause ANT: an admin order has the daemon of its machine set `paused = true` in its config, after which it
    /// takes no new prompt and the messages to it wait queued, prompts already running going on to their end. Needs
    /// admin credentials, as every order does.
    Pause {
        #[command(flatten)]
        order: Order,
    },
    /// Resume ANT paused by `ants ant pause` or its config: it takes the messages queued for it in turn.
    Resume {
        #[command(flatten)]
        order: Order,
    },
}

/// Where an order goes, and how long to wait for its outcome.
#[derive(clap::Args, Debug)]
pub(crate) struct Order {
    #[arg(add = ArgValueCandidates::new(completions::ants))]
    ant: UserId,
    /// The machine whose daemon serves ANT (default: the one that last recorded serving it).
    #[arg(long)]
    machine: Option<Machine>,
    /// How long to wait for the daemon to apply or refuse the order; a pending order stays until it does.
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "10s")]
    wait: Duration,
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
        AntCommand::Restart { ant, now, machine, json } => restart::run(nats, &ant, now, machine, json).await,
        AntCommand::Pause { order: Order { ant, machine, wait } } => pause::run(nats, &ant, machine, true, wait).await,
        AntCommand::Resume { order: Order { ant, machine, wait } } => {
            pause::run(nats, &ant, machine, false, wait).await
        }
    }
}
