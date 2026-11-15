//! `ants doctor`: check the setup of this machine.

use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::{Connection, auth, doctor, team_dir, webhook};

pub(crate) async fn run(
    config: Option<PathBuf>,
    leaf_config: Option<PathBuf>,
    no_harness: bool,
    json: bool,
    team: Option<PathBuf>,
    webhook_config: Option<PathBuf>,
    conn: &Connection<'_>,
) -> Result<()> {
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
            None => auth::default_dir()?.with_file_name("leaf.conf"),
        },
        harnesses: !no_harness,
        team: team_dir(team)?,
        webhooks: match webhook_config {
            Some(path) => path,
            None => webhook::default_path()?,
        },
        home: std::env::home_dir(),
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
