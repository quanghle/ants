//! `ants ant`: one ant at a glance.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{Machine, UserId};
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::admin::settings::{self, Assignment};
use crate::admin::{mcp, memories, orders, pause, restart};
use crate::serve::AntOptions;
use crate::{ant, avatars, completions, export};

#[derive(Subcommand, Debug)]
pub(crate) enum AntCommand {
    /// Show ANT as the ant view of `ants chat` does: where and how it runs (machine, harness, agent, model, workdir,
    /// capabilities, escalation chain, sage, whoami, the profile the ant wrote of itself) and what holds it back, as
    /// its colony entry tells; the messages waiting for it (`ants queue`), what it spent that UTC day (`ants spend`),
    /// its most recently active threads (`ants thread list --ant`), its settings as the config file of its machine
    /// has them, secrets hidden, and the latest admin orders to it (`ants ant orders`). Needs a human's credentials,
    /// and for the queue a daemon of the ant to answer. With an admin's credentials, reads the settings afresh with
    /// an admin order, else shows them as last read.
    Show {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object: the colony entry with its state, holds, queue, spend_today, threads, settings and orders.
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
    /// Clone SRC as the new ant NEW by admin order, on SRC's machine: its daemon pauses SRC until its prompts end,
    /// copies its home, its `[[ant]]` table with the settings given over it, the escalation chain with it, and each
    /// thread's session (forked when the harness can, else starting over from the thread's history), history and
    /// worktree (on the branch ants/NEW/THREAD, with its uncommitted changes), has `ants signer` sign NEW's key on the
    /// order, starts NEW and resumes SRC. Not copied: queued messages, channel memberships, references and spend
    /// counters. Paths are the machine's; the home defaults to ~/.ants/NEW there. Needs a human device's admin
    /// credentials, and `ants signer` running.
    Clone {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        src: UserId,
        new: UserId,
        /// The machine running SRC (default: the one that last recorded serving it).
        #[arg(long)]
        machine: Option<Machine>,
        #[command(flatten)]
        options: Box<AntOptions>,
        /// How long to wait for the daemon's outcome (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", default_value = "40m", value_parser = ants_proto::duration::parse)]
        wait: Duration,
    },
    /// Retire ANT by admin order, templated or not: its machine's daemon removes its `[[ant]]` table, which stops
    /// it, has `ants signer` revoke its key on the order and deletes its credentials; its home stays. Needs a human
    /// device's admin credentials, and `ants signer` running.
    Retire {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// The machine running the ant (default: the one that last recorded serving it).
        #[arg(long)]
        machine: Option<Machine>,
        /// How long to wait for the daemon's outcome (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", default_value = "8m", value_parser = ants_proto::duration::parse)]
        wait: Duration,
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
    /// Print ANT's avatar: its pixel-art head, one-line face, parts and colour, when it was born and how it grew with
    /// the prompts it completed. --seed or --reset and --color or --auto-color change it in the ant's identity record
    /// by compare-and-set, the same for every machine and viewer. Needs a human's credentials.
    Avatar {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// Draw the avatar from this seed instead of the ant's sense id (else its record's id), such as when two ants
        /// look alike.
        #[arg(long, conflicts_with = "reset")]
        seed: Option<String>,
        /// Draw the avatar from the ant's sense id (else its record's id) again.
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
    /// memories (whoami, memory, tasks), which a set order edits, the memories view of its provider when it serves one
    /// (sense's: the ant's identity, persona, self-model, relationships, skills, last dream and core and top memories),
    /// and its harness's memory files (the `memory_files` globs of its harness) and bd memories (with BD_DB in its
    /// environment), read-only and size-capped. Needs a human device's admin credentials.
    Memories {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object: core, harness, workdir, files, bd, provider and skipped.
        #[arg(long)]
        json: bool,
        /// The machine whose daemon serves ANT (default: the machine that last recorded serving it).
        #[arg(long)]
        machine: Option<Machine>,
    },
    /// Show the MCP servers of ANT's harness and how each stands (connected, pending, needs_auth, failed, disabled),
    /// which an admin order asks the daemon of its machine to read while online, with a session of the harness opened
    /// for that alone. Protocols claude, codex and copilot-sdk report them; others do not. With --sign-in, sign the
    /// harness in to a server asking for OAuth: prints the address to open in a browser. Needs a human device's admin
    /// credentials.
    Mcp {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// One JSON object: the report (harness, protocol, servers, truncated, read_at), or with --sign-in what it did
        /// (server, authorization_url, expires_at).
        #[arg(long)]
        json: bool,
        /// The machine whose daemon serves ANT (default: the machine that last recorded serving it).
        #[arg(long)]
        machine: Option<Machine>,
        /// Sign the harness in to this MCP server. The harness keeps the credentials in its own store, which the
        /// ant's sessions share; running ones may need `ants ant restart ANT` to use them.
        #[arg(long, value_name = "SERVER")]
        sign_in: Option<String>,
        /// The address a browser on another machine came back to after signing in, which fails to load there: hand
        /// it to the harness waiting for it.
        #[arg(long, value_name = "URL", requires = "sign_in")]
        callback: Option<String>,
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
    /// Export ANT to a file another bus imports it from (`ants ant import`): its identity record (id, birth, avatar,
    /// prompts), the settings that make it (whoami, harness, model, reasoning_effort, context, fallback) and its
    /// home's memory files, packed between prompts, and the part of its provider when the provider exports one, which
    /// an admin order asks the daemon of this machine for (admin credentials; --no-provider leaves it out). Leaves out
    /// its task database, other settings, credentials, environment, workdir, worktrees, sessions and history. Plain
    /// JSON, not encrypted. Runs on the machine whose config file lists ANT, with a human's credentials.
    Export {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// The file to write (default: ./ANT.ant); never overwritten.
        #[arg(short, long)]
        out: Option<PathBuf>,
        /// Mark ANT moved once exported: its daemon drains it and leaves the colony, and no daemon starts it until
        /// `ants ant unseal`.
        #[arg(long = "move")]
        seal: bool,
        /// Config file listing ANT (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
        /// Leave out the part of ANT's provider, which otherwise the daemon of this machine asks the provider for.
        #[arg(long)]
        no_provider: bool,
    },
    /// Import an ant from a file `ants ant export` wrote, as `ants add` adds one: create its identity record on this
    /// bus, refused when an ant there holds its name or its id, list it in the config file with the exported settings
    /// (the flags given win), give it a new key and restore its memories into its empty home, with no tasks. Harness
    /// memory files are left out, with a warning, when the ant runs another harness here. The part of a provider the
    /// export carries goes to the ant's provider here by an admin order to the daemon of this machine, of a provider
    /// of the same name only; when it cannot, a warning says why and --provider-only tries again. Needs a human's
    /// credentials, admin ones for the provider's part.
    Import(Box<crate::export::ImportArgs>),
    /// Clear the moved mark `ants ant export --move` set on ANT, so a daemon listing it serves it again.
    Unseal {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
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

pub(crate) async fn run(nats: &crate::connection::Nats<'_>, command: AntCommand) -> Result<()> {
    match command {
        AntCommand::Show { ant, json, config } => {
            let path = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            let caps = ants_daemon::config::Summary::load(&path)?.caps;
            Box::pin(ant::show(nats, ant, &caps, json)).await
        }
        AntCommand::Orders { ant, json } => orders::show(nats, &ant, json).await,
        AntCommand::Add { name, on, options, wait } => orders::add::run(nats, &name, on, &options, wait).await,
        AntCommand::Clone { src, new, machine, options, wait } => {
            orders::clone::run(nats, &src, &new, machine, &options, wait).await
        }
        AntCommand::Retire { ant, machine, wait } => orders::retire::run(nats, &ant, machine, wait).await,
        AntCommand::Set { ant, changes, no_wait } => {
            let wait = if no_wait { std::time::Duration::ZERO } else { settings::SET_WAIT };
            settings::set(nats, &ant, &changes, wait).await
        }
        AntCommand::Avatar { ant, seed, reset, color, auto_color } => {
            let change = avatars::Change {
                seed: if reset { Some(None) } else { seed.map(Some) },
                color: if auto_color { Some(None) } else { color.map(Some) },
            };
            avatars::show(nats, &ant, change).await
        }
        AntCommand::Memories { ant, json, machine } => memories::show(nats, &ant, machine, json).await,
        AntCommand::Mcp { ant, json, machine, sign_in, callback } => {
            mcp::show(nats, &ant, machine, json, sign_in, callback).await
        }
        AntCommand::Restart { ant, now, machine, json } => restart::run(nats, &ant, now, machine, json).await,
        AntCommand::Pause { order: Order { ant, machine, wait } } => pause::run(nats, &ant, machine, true, wait).await,
        AntCommand::Resume { order: Order { ant, machine, wait } } => {
            pause::run(nats, &ant, machine, false, wait).await
        }
        AntCommand::Export { ant, out, seal, config, no_provider } => {
            Box::pin(export::export(nats, &ant, out, seal, config, !no_provider)).await
        }
        AntCommand::Import(args) => Box::pin(export::import(nats, *args)).await,
        AntCommand::Unseal { ant } => export::unseal(nats, &ant).await,
    }
}
