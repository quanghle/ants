//! `ants ant`: one ant at a glance.

use std::path::PathBuf;

use ants_proto::{Machine, UserId};
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::{ant, avatars, completions, memories, orders, restart};

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
    /// Print ANT's avatar: its pixel-art head, one-line face, parts and colour, when it was born and how it grew with
    /// the prompts it completed. --seed or --reset and --color or --auto-color change it in the ant's identity record
    /// by compare-and-set, the same for every machine and viewer. Needs a human's credentials.
    Avatar {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// Draw the avatar from this seed instead of the ant's id, such as when two ants look alike.
        #[arg(long, conflicts_with = "reset")]
        seed: Option<String>,
        /// Draw the avatar from the ant's id again.
        #[arg(long)]
        reset: bool,
        /// Colour the avatar in this hue: red, orange, yellow, lime, green, mint, cyan, azure, blue, violet, magenta or
        /// rose.
        #[arg(long, conflicts_with = "auto_color")]
        color: Option<String>,
        /// Colour the avatar in the hue its seed picks again.
        #[arg(long)]
        auto_color: bool,
    },
    /// Show ANT's memories, which an admin order asks the daemon of its machine to read while online: its core
    /// memories (whoami, memory, tasks), which a set order edits, and its harness's memory files (the `memory_files`
    /// globs of its harness) and bd memories (with BD_DB in its environment), read-only and size-capped. Needs a
    /// human device's admin credentials.
    Memories {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object: core, harness, workdir, files, bd and skipped.
        #[arg(long)]
        json: bool,
        /// The machine whose daemon serves ANT (default: the machine that last recorded serving it).
        #[arg(long)]
        machine: Option<Machine>,
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
        AntCommand::Avatar { ant, seed, reset, color, auto_color } => {
            let change = avatars::Change {
                seed: if reset { Some(None) } else { seed.map(Some) },
                color: if auto_color { Some(None) } else { color.map(Some) },
            };
            avatars::show(nats, &ant, change).await
        }
        AntCommand::Memories { ant, json, machine } => memories::show(nats, &ant, machine, json).await,
        AntCommand::Restart { ant, now, machine, json } => restart::run(nats, &ant, now, machine, json).await,
    }
}
