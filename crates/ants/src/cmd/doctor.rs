//! `ants doctor`: check the setup of this machine.

use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::auth::team_dir;
use crate::connection::Connection;
use crate::{auth, doctor, webhook};

/// `ants doctor`.
#[derive(clap::Args, Debug)]
pub(crate) struct DoctorArgs {
    /// Config file of the machine's ants (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub(crate) config: Option<PathBuf>,
    /// Leaf node configuration (default: the one `ants leaf config` writes, ~/.config/ants/leaf.conf).
    #[arg(long)]
    pub(crate) leaf_config: Option<PathBuf>,
    /// Skip starting each ant's harness for an ACP handshake, which may take up to a minute.
    #[arg(long)]
    pub(crate) no_harness: bool,
    /// Print one JSON object per check.
    #[arg(long)]
    pub(crate) json: bool,
    /// Team directory of `ants auth` (default: ~/.config/ants/auth); where it holds the team keys, the hub is asked
    /// whether it keeps pushed accounts across restarts.
    #[arg(long = "dir", env = "ANTS_AUTH_DIR")]
    pub(crate) team: Option<PathBuf>,
    /// Config file of `ants webhook serve` (default: ~/.ants/webhooks.toml), checked when it is there.
    #[arg(long)]
    pub(crate) webhook_config: Option<PathBuf>,
}

pub(crate) async fn run(conn: &Connection<'_>, args: DoctorArgs) -> Result<()> {
    let DoctorArgs { config, leaf_config, no_harness, json, team, webhook_config } = args;
    let setup = doctor::Setup {
        nats: conn.nats.to_owned(),
        creds: conn.creds.map(Path::to_path_buf),
        js_domain: conn.js_domain.cloned(),
        config: match config {
            Some(path) => path,
            None => ants_daemon::config::default_path()?,
        },
        leaf_config: match leaf_config {
            Some(path) => path,
            None => crate::node::leaf::default_path()?,
        },
        hub_config: auth::default_dir()?.with_file_name("nats.conf"),
        harnesses: !no_harness,
        team: team_dir(team)?,
        webhooks: match webhook_config {
            Some(path) => path,
            None => webhook::default_path()?,
        },
        home: std::env::home_dir(),
        worker: doctor::trail_worker(std::env::home_dir()),
    };
    let checks = Box::pin(doctor::run(&setup)).await;
    for check in &checks {
        if json { println!("{}", serde_json::to_string(check)?) } else { println!("{check}") }
    }
    let failed = checks.iter().filter(|c| c.status == doctor::Status::Fail).count();
    match failed {
        0 => Ok(()),
        _ => anyhow::bail!("{failed} of {} checks failed", checks.len()),
    }
}
