//! Renewing credentials over NATS: where credentials are used, `ants renew` generates a new user NKey and asks for
//! the credentials renewed for it, signing the request with the key of the credentials it renews ([`request`]); on
//! the machine holding the account key, `ants enroll renew` signs a user JWT for the new key with the same name, kind,
//! members, device and JetStream domain, and the permissions their kind has in this version of ants
//! ([`auth::renew`], [`crate::credentials::approver::renewals`]). The holder replaces its `.creds` file at once,
//! keeping the new seed where the earlier one was, in the file or in the OS credential store
//! ([`crate::credentials::keys::replace`]), and reloads what uses it ([`reload`]).
//!
//! As with enrollment, requests are core NATS messages, repeated every [`ants_proto::ENROLL_REPEAT`] until an answer
//! comes, and the answer carries only a JWT, useless without the seed that never leaves the holder. Only credentials
//! still valid and not revoked may ask: expired ones cannot connect, and are issued again with `ants auth` on the
//! machine holding the account keys.
//!
//! The uplink credentials of a leaf node connect as no client: a human's credentials carry their request
//! (`ants renew LEAF.creds --creds HUMAN.creds`).

use ants_daemon::bus::relayed;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Summary};
use ants_daemon::secrets::creds::{self, Finding, Kind};
use ants_daemon::secrets::keystore::Store;
use ants_daemon::{bus, colony};
use ants_proto::{DeviceKey, RenewReply, RenewRequest, UserId, subject};
use anyhow::{Context as _, Result, bail};
use futures::StreamExt as _;
use nkeys::{KeyPair, XKey};

use crate::auth;
use crate::credentials::keys;
use crate::node::{leaf, natsd};
use crate::service::{Action, Outcome, Service, Services, failure};

/// The encryption key the renewed credentials of a human at `path` are to keep: the one the file holds, or a new one
/// for credentials binding none, which the renewal asks to bind. None for others', and for credentials binding a key
/// whose seed the file does not hold.
pub(crate) fn device_xkey(path: &Path, finding: &Finding, store: &dyn Store) -> Result<Option<XKey>> {
    if finding.kind != Kind::Human {
        return Ok(None);
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    match &finding.xkey {
        None => Ok(Some(XKey::new())),
        Some(_) => keys::held_xkey(&text, path, store),
    }
}

/// The claims and findings of `jwt`, the credentials at `path`, if they may ask for their renewal now; else an error
/// naming how to renew them on the machine holding the account keys.
pub(crate) fn renewable(path: &Path, jwt: &str) -> Result<(auth::Claims, Finding)> {
    let claims = auth::verified(jwt).with_context(|| path.display().to_string())?;
    let finding = Finding::from_jwt(jwt).with_context(|| path.display().to_string())?;
    let now = auth::now()?;
    if finding.renewable(now) {
        return Ok((claims, finding));
    }
    let why = match &finding.kind {
        Kind::Enrollment { .. } | Kind::LeafSystem { .. } | Kind::LeafStore { .. } | Kind::Bootstrap => {
            "are not renewed over NATS"
        }
        _ => "expired, and expired credentials connect no more",
    };
    bail!(
        "{} ({}) {why}: renew them on the machine holding the account keys, `{}`",
        finding.holder(),
        path.display(),
        finding.renew().join("`, then `")
    )
}

/// Credentials to renew: those at `creds`, for the public key of `user`, asking for a human's credentials without an
/// encryption key to bind `xkey`, if given.
pub struct Renewal<'a> {
    pub creds: &'a Path,
    pub user: &'a KeyPair,
    pub xkey: Option<&'a DeviceKey>,
}

/// Ask for the `renewal` of credentials over the server at `url`, connected with the credentials at `carrier` (the
/// renewed ones themselves, but for a leaf node's uplink), each reading a seed it keeps in `store` rather than in
/// itself: repeat the signed request every `repeat` until an answer for the key arrives, the credentials expire or
/// `stop` completes. Returns the user JWT of the answer, checked to be the account's for that key, with the same name,
/// kind, device and JetStream domain, and the encryption key the credentials bind or the renewal's `xkey`; fails with
/// the reason of an answer refusing it (from `ants signer`, for credentials a registration granted).
pub async fn request(
    url: &str,
    carrier: &Path,
    Renewal { creds, user, xkey }: Renewal<'_>,
    store: &dyn Store,
    repeat: Duration,
    stop: impl Future<Output = ()>,
) -> Result<String> {
    let (current_jwt, current_key) = creds::load(creds, store)?;
    let (current, finding) = renewable(creds, &current_jwt)?;
    let (carrier_jwt, _) = creds::load(carrier, store)?;
    let carrier_name = auth::verified(&carrier_jwt).with_context(|| carrier.display().to_string())?.name;
    let carrier_user: UserId =
        carrier_name.parse().with_context(|| format!("{} connect as no client", carrier.display()))?;
    let (client, _) = bus::connect_with(url, Some(carrier), store, None, &carrier_user, "ants-renew").await?;
    let mut replies = relayed::subscribe(&client, subject::renew_reply(&finding.name)).await?;
    let public_key = user.public_key();
    let mut request = RenewRequest::new(&finding.name, &public_key, colony::machine().await);
    request.current = current_jwt;
    request.xkey = xkey.cloned();
    request.signature = auth::sign(&current_key, &request.signed())?;
    let payload = serde_json::to_vec(&request)?;
    let left = current.exp.map(|exp| Duration::from_secs(exp.saturating_sub(auth::now().unwrap_or(exp))));
    let expiry = async {
        match left {
            Some(left) => tokio::time::sleep(left.min(Duration::from_hours(365 * 24))).await,
            None => std::future::pending().await,
        }
    };
    let mut repeats = tokio::time::interval(repeat);
    tokio::pin!(expiry, stop);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                client.publish(request.subject(), payload.clone().into()).await?;
                client.flush().await?;
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let reply = match serde_json::from_slice::<RenewReply>(&msg.payload) {
                    Ok(reply) if reply.name == finding.name && reply.public_key == public_key => reply,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "ignoring a malformed renewal answer");
                        continue;
                    }
                };
                if let Some(reason) = reply.refused {
                    bail!("ants signer refused to renew {}: {reason}", finding.holder());
                }
                let xkey = finding.xkey.as_ref().or(xkey);
                match check_renewed(&reply.jwt, &current, &finding, &public_key, xkey) {
                    Ok(()) => return Ok(reply.jwt),
                    Err(e) => tracing::warn!(error = format!("{e:#}"), "ignoring a renewal answer"),
                }
            }
            () = &mut expiry => bail!("the credentials {} expired without an answer", creds.display()),
            () = &mut stop => bail!("stopped before an answer came"),
        }
    }
}

/// Check that `jwt` renews the credentials of `current` and `finding` for `public_key`: signed by the same account,
/// with the same name, kind, device and JetStream domain, binding the encryption key `xkey`.
fn check_renewed(
    jwt: &str,
    current: &auth::Claims,
    finding: &Finding,
    public_key: &str,
    xkey: Option<&DeviceKey>,
) -> Result<()> {
    let claims = auth::verified(jwt)?;
    if claims.iss != current.iss {
        bail!("signed by {}, not by the account {}", claims.iss, current.iss);
    }
    if claims.sub != public_key || claims.name != current.name {
        bail!("issued to {} ({}), not to {} ({public_key})", claims.name, claims.sub, current.name);
    }
    let renewed = Finding::from_jwt(jwt)?;
    if !finding.same_credentials(&renewed) {
        bail!("would make {} into {}", finding.holder(), renewed.holder());
    }
    if renewed.xkey.as_ref() != xkey {
        bail!("binds the encryption key {:?}, not {xkey:?}", renewed.xkey);
    }
    Ok(())
}

/// The configuration of the machine's leaf node, at `path` or the default one when it exists, and the path of its
/// uplink credentials.
fn leaf_uplink(path: Option<&Path>) -> Option<(PathBuf, PathBuf)> {
    let path = match path {
        Some(path) => path.to_owned(),
        None => crate::node::leaf::default_path().ok()?,
    };
    let (_, creds) = leaf::read_uplink(&path).ok()?;
    Some((path, creds))
}

pub(crate) fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => std::path::absolute(a).ok() == std::path::absolute(b).ok(),
    }
}

/// Reload what uses the credentials at `path`, which a NATS connection reads only when it connects: the daemon of the
/// config file at `config` (or the default one) when one of its ants does, the leaf node of `leaf_config` (or the
/// default one) when they are its uplink's, connecting the uplink again ([`leaf::use_renewed_uplink`]).
pub(crate) fn reload(path: &Path, config: Option<&Path>, leaf_config: Option<&Path>) -> Result<()> {
    let config = match config {
        Some(config) => config.to_owned(),
        None => config::default_path()?,
    };
    let summary = Summary::load(&config)?;
    let ants: Vec<&UserId> =
        summary.ants.iter().filter(|(_, c)| c.as_deref().is_some_and(|c| same_file(c, path))).map(|(a, _)| a).collect();
    let uplink = leaf_uplink(leaf_config).filter(|(_, uplink)| same_file(uplink, path));
    let mut services = Services::local();
    if !ants.is_empty() {
        let ants = ants.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
        match services.apply(Service::Daemon, Action::Reload, &config) {
            Outcome::Done(command) => println!("daemon reloaded to serve {ants} with them ({command})"),
            Outcome::NotRunning => println!("the daemon service is not running; it uses them once started"),
            outcome => println!(
                "{}serve {ants} with them: reload `ants daemon` with SIGHUP, or its service ({})",
                failure(outcome),
                crate::service::manual(Service::Daemon, Action::Reload)
            ),
        }
    }
    if let Some((conf, _)) = &uplink {
        println!("{}", leaf::use_renewed_uplink(conf, |action| natsd::apply(conf, action)));
    }
    if ants.is_empty() && uplink.is_none() {
        println!(
            "restart what uses {} to use them: a NATS connection reads them only when it connects",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
