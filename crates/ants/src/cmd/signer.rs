//! `ants signer`: sign the credentials of the ants manager ants ask for, and answer ants' reactions requests.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::auth::team_dir;
use crate::connection::Connection;
use crate::credentials::approver;
use crate::release::upgrade;
use crate::signing::signer;

/// `ants signer`.
#[derive(clap::Args, Debug)]
pub(crate) struct SignerArgs {
    /// Config file whose [[ant_template]] tables are the policy; the rest of it is ignored
    /// (default: ~/.ants/config.toml).
    #[expect(rustdoc::broken_intra_doc_links, reason = "clap help text: brackets name config tables")]
    #[arg(long, env = "ANTS_CONFIG")]
    pub(crate) config: Option<PathBuf>,
    /// How long a human has to decide on a request outside the policy, at most until the manager stops waiting,
    /// or on a registration (`ants login`) outside the [registration] table, at most until its request expires.
    #[expect(rustdoc::broken_intra_doc_links, reason = "clap help text: brackets name config tables")]
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "15m")]
    pub(crate) approval_timeout: Duration,
    /// Auth directory holding the signing keys (default: ~/.config/ants/auth).
    #[arg(long, env = "ANTS_AUTH_DIR")]
    pub(crate) dir: Option<PathBuf>,
    #[command(flatten)]
    pub(crate) upgrade: upgrade::UpgradeArgs,
}

pub(crate) async fn run(conn: &Connection<'_>, args: SignerArgs) -> Result<()> {
    let SignerArgs { config, approval_timeout, dir, upgrade } = args;
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
        signer::Options { config: &config, approval_timeout, reconnect: approver::AUTO_RECONNECT, tick: signer::TICK };
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
