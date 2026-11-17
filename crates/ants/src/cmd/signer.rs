//! `ants signer`: sign the credentials of the ants manager ants ask for, and answer ants' reactions requests.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::auth::team_dir;
use crate::connection::Connection;
use crate::{renew, signer, upgrade};

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
        "signing the ants manager ants ask for, within the templates of {} ({}), and answering ants' reactions \
         requests; Ctrl-C stops",
        config.display(),
        if names.is_empty() { "none yet: every request goes to a human".to_owned() } else { names.join(", ") }
    );
    match ants_daemon::config::registration::signer_registration(&config)? {
        Some(registration) => eprintln!(
            "registering humans' devices after a GitHub sign-in (OAuth App {}), signed at once for {}",
            registration.client_id,
            policy(&registration)
        ),
        None => eprintln!("refusing registrations: {} has no [registration] table", config.display()),
    }
    let options =
        signer::Options { config: &config, approval_timeout, reconnect: renew::AUTO_RECONNECT, tick: signer::TICK };
    let stop = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let serving = signer::serve(conn.nats, &dir, domain, &options, signer::print, stop);
    Box::pin(upgrade::or_exit(serving, &upgrade)).await
}

/// Whom `registration` signs for at once, in words.
fn policy(registration: &ants_daemon::config::registration::Registration) -> String {
    let mut whom: Vec<String> = registration.orgs.iter().map(|o| format!("org {o}")).collect();
    whom.extend(registration.teams.iter().map(|t| format!("team {t}")));
    whom.extend(registration.email_domains.iter().map(|d| format!("@{d}")));
    if whom.is_empty() { "nobody: every registration goes to a human".to_owned() } else { whom.join(", ") }
}
