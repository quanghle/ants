//! `ants schedule list`: the schedules of the machine's config file, as `/schedules` in `ants chat` shows them.

use std::path::PathBuf;

use ants_proto::UserId;
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
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: ScheduleCommand) -> Result<()> {
    match command {
        ScheduleCommand::List { ant, json, config } => {
            let path = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            schedules::list(nats, &path, ant.as_ref(), json).await
        }
    }
}
