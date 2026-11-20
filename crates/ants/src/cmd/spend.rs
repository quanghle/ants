//! `ants spend`: what ants spent, against the caps of the config file.

use std::path::PathBuf;

use ants_proto::UserId;
use anyhow::Result;
use clap_complete::engine::ArgValueCandidates;

use crate::connection::Connection;
use crate::threads::thread;
use crate::{completions, spend};

/// `ants spend`.
#[derive(clap::Args, Debug)]
pub(crate) struct SpendArgs {
    /// Only this ant.
    #[arg(add = ArgValueCandidates::new(completions::ants))]
    pub(crate) ant: Option<UserId>,
    /// What the ant, or each ant on the thread, spent on it since a human's last message there and in all: its id, a
    /// unique start of one, or `last`.
    #[arg(long, value_name = "THREAD")]
    #[arg(add = ArgValueCandidates::new(completions::threads))]
    pub(crate) thread: Option<thread::Pick>,
    /// UTC days to report, today included.
    #[arg(long, default_value_t = spend::DEFAULT_DAYS, value_parser = clap::value_parser!(u32).range(1..=366),
          conflicts_with = "thread")]
    pub(crate) days: u32,
    /// One JSON object per line: per ant, or with --thread per ant on the thread.
    #[arg(long)]
    pub(crate) json: bool,
    /// Config file with the ants' caps (default: ~/.ants/config.toml, if it exists).
    #[arg(long, env = "ANTS_CONFIG")]
    pub(crate) config: Option<PathBuf>,
}

pub(crate) async fn run(conn: &Connection<'_>, args: SpendArgs) -> Result<()> {
    let SpendArgs { ant, thread, days, json, config } = args;
    let path = match config {
        Some(path) => path,
        None => ants_daemon::config::default_path()?,
    };
    let caps = ants_daemon::config::Summary::load(&path)?.caps;
    spend::show(&conn.nats()?, &spend::Select { ant, thread, days }, &caps, json).await
}
