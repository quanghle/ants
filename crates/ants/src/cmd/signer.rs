//! `ants signer`: sign the credentials of the ants manager ants ask for.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::{Connection, renew, signer, team_dir, upgrade};

pub(crate) async fn run(
    config: Option<PathBuf>,
    approval_timeout: Duration,
    dir: Option<PathBuf>,
    upgrade: upgrade::UpgradeArgs,
    conn: &Connection<'_>,
) -> Result<()> {
    let domain = conn.js_domain;
    let dir = team_dir(dir)?;
    let config = match config {
        Some(path) => path,
        None => ants_daemon::config::default_path()?,
    };
    let templates = ants_daemon::config::signer_templates(&config)?;
    let names: Vec<_> = templates.iter().map(|t| t.name.as_str()).collect();
    eprintln!(
        "signing the ants manager ants ask for, within the templates of {} ({}); Ctrl-C stops",
        config.display(),
        if names.is_empty() { "none yet: every request goes to a human".to_owned() } else { names.join(", ") }
    );
    let options =
        signer::Options { config: &config, approval_timeout, reconnect: renew::AUTO_RECONNECT, tick: signer::TICK };
    let stop = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let serving = signer::serve(conn.nats, &dir, domain, &options, signer::print, stop);
    Box::pin(upgrade::or_exit(serving, &upgrade)).await
}
