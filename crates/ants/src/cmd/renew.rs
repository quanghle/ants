//! `ants renew`: ask for the credentials of a file renewed, and use them once approved.

use std::path::PathBuf;

use ants_daemon::secrets::creds::Kind;
use ants_daemon::secrets::keystore;
use ants_proto::ENROLL_REPEAT;
use anyhow::{Context as _, Result, bail};
use nkeys::KeyPair;

use crate::auth;
use crate::connection::Connection;
use crate::credentials::keys;
use crate::credentials::renew::{Renewal, device_xkey, reload, renewable, request, same_file};

#[derive(clap::Args, Debug)]
pub(crate) struct RenewArgs {
    /// Credentials file to renew (default: --creds, the CLI's). Those of a leaf node's uplink need a human's in
    /// --creds to carry the request.
    pub(crate) file: Option<PathBuf>,
    /// Config file of the daemon to reload when one of its ants uses the file (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub(crate) config: Option<PathBuf>,
    /// Configuration of the machine's leaf node, from `ants leaf config` (default: ~/.config/ants/leaf.conf, when it
    /// exists): restarted when its uplink uses the file.
    #[arg(long, value_name = "FILE")]
    pub(crate) leaf_config: Option<PathBuf>,
}

/// Run `ants renew` with the global `--nats` and `--creds`: ask for the credentials of the file renewed, wait for
/// the approval, replace the file and reload the daemon or the leaf node using it.
pub(crate) async fn run(conn: &Connection<'_>, args: RenewArgs) -> Result<()> {
    let (nats, cli_creds) = (conn.nats, conn.creds);
    let path = match (&args.file, cli_creds) {
        (Some(file), _) => std::path::absolute(file)?,
        (None, Some(creds)) => std::path::absolute(creds)?,
        (None, None) => bail!("name the credentials file to renew: `ants renew FILE`"),
    };
    let (jwt, _) = auth::read_creds(&path)?;
    let (_, finding) = renewable(&path, &jwt)?;
    let carrier = match &finding.kind {
        Kind::Leaf { .. } => {
            let carrier = cli_creds.filter(|c| args.file.is_some() && !same_file(c, &path)).with_context(|| {
                format!(
                    "uplink credentials connect only as a leaf node: carry the request with a human's credentials, \
                     `ants renew {} --creds HUMAN.creds`",
                    path.display()
                )
            })?;
            std::path::absolute(carrier)?
        }
        _ => path.clone(),
    };
    let user = KeyPair::new_user();
    let approver = if finding.registered {
        "`ants signer` on the machine holding the account key answers for a device registered with `ants login`"
            .to_owned()
    } else {
        format!("approve with `ants enroll renew {}` on the machine holding the account key", finding.name)
    };
    eprintln!(
        "asking to renew {} ({}) for the new key {}: {approver}; waiting (Ctrl-C stops)",
        finding.holder(),
        path.display(),
        user.public_key(),
    );
    let stop = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let store = keystore::os();
    let xkey = device_xkey(&path, &finding, store)?;
    let ask = match (&finding.xkey, &xkey) {
        (None, Some(xkey)) => Some(auth::device_key(xkey)?),
        _ => None,
    };
    let renewal = Renewal { creds: &path, user: &user, xkey: ask.as_ref() };
    let jwt = request(nats, &carrier, renewal, store, ENROLL_REPEAT, stop).await?;
    keys::replace(&path, &jwt, &user, xkey.as_ref(), store, nats).await?;
    println!("{} renewed: {}, key {}", path.display(), finding.holder(), user.public_key());
    reload(&path, args.config.as_deref(), args.leaf_config.as_deref())
}
