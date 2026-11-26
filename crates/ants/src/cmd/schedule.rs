//! `ants schedule list`: the schedules of the machine's config file, or of another machine's, as `/schedules` in
//! `ants chat` shows them.

use std::path::PathBuf;

use ants_proto::{Machine, UserId};
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::completions;
use crate::connection::Nats;
use crate::schedules;

#[derive(Subcommand, Debug)]
pub(crate) enum ScheduleCommand {
    /// List the schedules: each one's timetable, destination, next firing (UTC) and its last firing in the last 7
    /// days, sent or not (skipped, held for a paused ant, or missed while no daemon served it).
    List {
        /// Only the schedules sending to this ant, then the reminders and watchers sense keeps for it, as `ants ant
        /// memories` reads them (with admin credentials).
        #[arg(long, add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// One JSON object per line: each schedule (`"kind": "schedule"`), then with --ant each reminder or watcher
        /// (`"kind": "reminder"`).
        #[arg(long)]
        json: bool,
        /// Config file of the machine's ants (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
        /// The machine whose config file to list instead of this one's, which an admin order asks the daemon there to
        /// list while online (with admin device credentials).
        #[arg(long)]
        machine: Option<Machine>,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: ScheduleCommand) -> Result<()> {
    match command {
        ScheduleCommand::List { ant, json, config: _, machine: Some(machine) } => {
            schedules::list(nats, schedules::Source::On(&machine), ant.as_ref(), json).await
        }
        ScheduleCommand::List { ant, json, config, machine: None } => {
            let path = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            schedules::list(nats, schedules::Source::Here(&path), ant.as_ref(), json).await
        }
    }
}
