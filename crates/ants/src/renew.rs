//! Renewing credentials over NATS: where credentials are used, `ants renew` generates a new user NKey and asks for
//! the credentials renewed for it, signing the request with the key of the credentials it renews ([`request`]); on
//! the machine holding the account key, `ants enroll renew` signs a user JWT for the new key with the same name, kind,
//! members, device and JetStream domain, and the permissions their kind has in this version of ants
//! ([`auth::renew`]). The holder replaces its `.creds` file at once, keeping the new seed where the earlier one was, in
//! the file or in the OS credential store ([`keys::replace`]), and reloads what uses it ([`run`]).
//!
//! As with enrollment, requests are core NATS messages, repeated every [`ENROLL_REPEAT`] until an answer comes, and
//! the answer carries only a JWT, useless without the seed that never leaves the holder. Only credentials still valid
//! and not revoked may ask: expired ones cannot connect, and are issued again with `ants auth` on the machine holding
//! the team keys.
//!
//! The uplink credentials of a leaf node connect as no client: a human's credentials carry their request
//! (`ants renew LEAF.creds --creds HUMAN.creds`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Summary};
use ants_daemon::creds::{self, Finding, Kind};
use ants_daemon::keystore::{self, Store};
use ants_daemon::{bus, colony};
use ants_proto::{DeviceKey, ENROLL_REPEAT, JsDomain, RenewReply, RenewRequest, UserId, subject};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use futures::StreamExt as _;
use nkeys::{KeyPair, XKey};

use crate::enroll::Approver;
use crate::service::{Action, Outcome, Service, Services, failure};
use crate::{auth, keys, leaf, natsd, revoke};

#[derive(clap::Args, Debug)]
pub struct RenewArgs {
    /// Credentials file to renew (default: --creds, the CLI's). Those of a leaf node's uplink need a human's in
    /// --creds to carry the request.
    pub file: Option<PathBuf>,
    /// Config file of the daemon to reload when one of its ants uses the file (default: ~/.ants/config.toml).
    #[arg(long, env = "ANTS_CONFIG")]
    pub config: Option<PathBuf>,
    /// Configuration of the machine's leaf node, from `ants leaf config` (default: ~/.config/ants/leaf.conf, when it
    /// exists): restarted when its uplink uses the file.
    #[arg(long, value_name = "FILE")]
    pub leaf_config: Option<PathBuf>,
}

/// Run `ants renew` with the global `--nats` and `--creds`: ask for the credentials of the file renewed, wait for
/// the approval, replace the file and reload the daemon or the leaf node using it.
pub async fn run(args: RenewArgs, nats: &str, cli_creds: Option<&Path>) -> Result<()> {
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

/// The encryption key the renewed credentials of a human at `path` are to keep: the one the file holds, or a new one
/// for credentials binding none, which the renewal asks to bind. None for others', and for credentials binding a key
/// whose seed the file does not hold.
fn device_xkey(path: &Path, finding: &Finding, store: &dyn Store) -> Result<Option<XKey>> {
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
/// naming how to renew them on the machine holding the team keys.
fn renewable(path: &Path, jwt: &str) -> Result<(auth::Claims, Finding)> {
    let claims = auth::verified(jwt).with_context(|| path.display().to_string())?;
    let finding = Finding::from_jwt(jwt).with_context(|| path.display().to_string())?;
    let now = auth::now()?;
    if finding.renewable(now) {
        return Ok((claims, finding));
    }
    let why = match &finding.kind {
        Kind::Enrollment { .. } | Kind::LeafSystem { .. } | Kind::Bootstrap => "are not renewed over NATS",
        _ => "expired, and expired credentials connect no more",
    };
    bail!(
        "{} ({}) {why}: renew them on the machine holding the team keys, `{}`",
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
    let mut replies = client.subscribe(subject::renew_reply(&finding.name)).await?;
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
        None => auth::default_dir().ok()?.with_file_name("leaf.conf"),
    };
    let (_, creds) = leaf::read_uplink(&path).ok()?;
    Some((path, creds))
}

fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => std::path::absolute(a).ok() == std::path::absolute(b).ok(),
    }
}

/// Reload what uses the credentials at `path`, which a NATS connection reads only when it connects: the daemon of the
/// config file at `config` (or the default one) when one of its ants does, the leaf node of `leaf_config` (or the
/// default one) when they are its uplink's, connecting the uplink again ([`leaf::use_renewed_uplink`]).
fn reload(path: &Path, config: Option<&Path>, leaf_config: Option<&Path>) -> Result<()> {
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

/// How long `ants enroll renew --auto` keeps one connection, under the lifetime of the credentials it issues itself
/// for it ([`auth::approver_creds`]).
pub const AUTO_RECONNECT: Duration = Duration::from_mins(5);

impl Approver {
    /// Check that `request` proves credentials of the team's account named as it says, unexpired and not revoked:
    /// signed with their key. Returns their claims.
    fn check_renewal(&self, request: &RenewRequest) -> Result<auth::Claims> {
        auth::check_user_key(&request.public_key)?;
        let current = auth::verified(&request.current).context("the credentials to renew")?;
        if current.iss != self.account {
            bail!("credentials issued by {}, not by the team's account", current.iss);
        }
        if current.name != request.name {
            bail!("credentials of {}, not {}", current.name, request.name);
        }
        if current.exp.is_some_and(|exp| exp <= auth::now().unwrap_or(u64::MAX)) {
            bail!("expired credentials");
        }
        if self.revoked.contains(&current.sub) {
            bail!("revoked credentials");
        }
        auth::check_signature(&current.sub, &request.signed(), &request.signature).context("the credentials")?;
        Ok(current)
    }

    /// A checked renewal request of `msg`, with the claims of the credentials it renews, or why it is ignored.
    pub(crate) fn checked_renewal(&self, msg: &async_nats::Message) -> Result<(RenewRequest, auth::Claims)> {
        let request: RenewRequest = serde_json::from_slice(&msg.payload).context("a malformed renewal request")?;
        request.check_subject(&msg.subject)?;
        let current = self.check_renewal(&request).with_context(|| format!("renewal request of {}", request.name))?;
        Ok((request, current))
    }

    /// A checked renewal request of `msg` for `ants enroll renew`, or why it is ignored: credentials a registration
    /// granted are `ants signer`'s to renew ([`revoke::registered`]).
    pub(crate) fn renewal(&self, msg: &async_nats::Message) -> Result<RenewRequest> {
        let (request, current) = self.checked_renewal(msg)?;
        if revoke::registered(&revoke::issued(&self.dir)?, &current.sub).is_some() {
            bail!(
                "renewal request of {}: a registration granted the credentials, which ants signer renews",
                request.name
            );
        }
        Ok(request)
    }

    /// The renewal requests heard, for `name` or for every name: the latest of each name and key, oldest first.
    pub async fn renewals(&self, name: Option<&str>) -> Result<Vec<RenewRequest>> {
        let filter = name.map_or_else(|| subject::ALL_RENEW_REQUESTS.to_owned(), subject::renew);
        let mut requests = self.client.subscribe(filter).await?;
        self.client.flush().await?;
        let mut heard: Vec<RenewRequest> = Vec::new();
        let end = tokio::time::sleep(self.listen);
        tokio::pin!(end);
        loop {
            tokio::select! {
                msg = requests.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    match self.renewal(&msg) {
                        Ok(request) => {
                            heard.retain(|r| (&r.name, &r.public_key) != (&request.name, &request.public_key));
                            heard.push(request);
                        }
                        Err(e) => tracing::warn!("ignoring a renewal request: {e:#}"),
                    }
                }
                () = &mut end => break,
            }
        }
        requests.unsubscribe().await?;
        heard.sort_by_key(|r| r.sent_at);
        Ok(heard)
    }

    /// Print the renewal requests heard.
    pub async fn list_renewals(&self) -> Result<()> {
        let heard = self.renewals(None).await?;
        if heard.is_empty() {
            println!("no renewal requests heard in {}s", self.listen.as_secs());
        }
        for r in heard {
            let age = (Utc::now() - r.sent_at).num_seconds().max(0);
            let holder = Finding::from_jwt(&r.current).map_or_else(|_| r.name.clone(), |f| f.holder());
            println!("{}  {holder}  new key {}  host {}  asked {age}s ago", r.name, r.public_key, r.host);
        }
        Ok(())
    }

    /// Renew the credentials `name` asks for, with the account key in `dir` ([`auth::renew`]), and send them.
    /// `public_key` picks the request when several holders ask for `name`.
    pub async fn renew(
        &self,
        dir: &Path,
        name: &str,
        public_key: Option<&str>,
    ) -> Result<(RenewRequest, auth::Renewed)> {
        let heard = self.renewals(Some(name)).await?;
        let request = match (public_key, heard.as_slice()) {
            (_, []) => bail!(
                "no renewal request for {name} heard in {}s; run `ants renew FILE` where its credentials are and \
                 leave it waiting",
                self.listen.as_secs()
            ),
            (Some(key), heard) => heard
                .iter()
                .find(|r| r.public_key == key)
                .with_context(|| format!("no renewal request for {name} from {key} heard"))?,
            (None, [one]) => one,
            (None, several) => {
                let keys: Vec<String> = several.iter().map(|r| format!("{} (host {})", r.public_key, r.host)).collect();
                bail!("several holders ask to renew {name}; pick one with --public-key: {}", keys.join(", "))
            }
        };
        let renewed = auth::renew(dir, &request.current, &request.public_key, request.xkey.as_ref())?;
        let reply = RenewReply::signed(name, &request.public_key, renewed.jwt.clone());
        // An answer lost on the way leaves the holder asking: answer again while it does.
        for _ in 0..3 {
            self.answer_renewal(&reply).await?;
            let still = self.renewals(Some(name)).await?;
            if !still.iter().any(|r| r.public_key == request.public_key) {
                return Ok((request.clone(), renewed));
            }
        }
        bail!("{name} on {} keeps asking; the answer did not get through", request.host)
    }

    pub(crate) async fn answer_renewal(&self, reply: &RenewReply) -> Result<()> {
        self.client.publish(reply.subject(), serde_json::to_vec(reply)?.into()).await?;
        self.client.flush().await?;
        Ok(())
    }
}

/// `ants enroll renew --auto`: renew every credentials asking ([`auth::renew`]) as their requests arrive, over the
/// server at `url`, with the account key in `dir`, until `stop` completes. Each run connects afresh every
/// [`AUTO_RECONNECT`], with credentials it issues itself, and reads the revocations again. Calls `renewed` with what
/// each renewal did.
pub async fn auto(
    url: &str,
    dir: &Path,
    js_domain: Option<&JsDomain>,
    listen: Duration,
    reconnect: Duration,
    mut renewed: impl FnMut(&RenewRequest, &auth::Renewed),
    stop: impl Future<Output = ()>,
) -> Result<()> {
    // The answer to each name and key: a holder that missed it asks again and gets the same.
    let mut answered: HashMap<(String, String), RenewReply> = HashMap::new();
    tokio::pin!(stop);
    loop {
        let approver = Approver::connect(url, dir, js_domain, listen).await?;
        let mut requests = approver.client.subscribe(subject::ALL_RENEW_REQUESTS).await?;
        approver.client.flush().await?;
        let until = tokio::time::sleep(reconnect);
        tokio::pin!(until);
        loop {
            tokio::select! {
                msg = requests.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    let request = match approver.renewal(&msg) {
                        Ok(request) => request,
                        Err(e) => {
                            tracing::warn!("ignoring a renewal request: {e:#}");
                            continue;
                        }
                    };
                    let key = (request.name.clone(), request.public_key.clone());
                    if let Some(reply) = answered.get(&key) {
                        approver.answer_renewal(reply).await?;
                        continue;
                    }
                    match auth::renew(dir, &request.current, &request.public_key, request.xkey.as_ref()) {
                        Ok(done) => {
                            let reply = RenewReply::signed(&request.name, &request.public_key, done.jwt.clone());
                            approver.answer_renewal(&reply).await?;
                            answered.insert(key, reply);
                            renewed(&request, &done);
                        }
                        Err(e) => tracing::warn!(name = %request.name, "not renewing: {e:#}"),
                    }
                }
                () = &mut until => break,
                () = &mut stop => return Ok(()),
            }
        }
    }
}

/// Print what renewing `request` did, and how to retire the earlier key.
pub fn print_renewed(request: &RenewRequest, renewed: &auth::Renewed) {
    println!(
        "{} renewed for {} (host {}); once the new credentials are in use, retire the earlier key: \
         `ants auth revoke --key {} --earlier`, then `ants auth push`",
        renewed.finding.holder(),
        request.public_key,
        request.host,
        renewed.previous
    );
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use ants_proto::{Role, UserId};
    use tokio::time::timeout;

    use super::*;
    use crate::members::EnrollApprover;
    use crate::revoke;
    use crate::test_support::Server;

    const WAIT: Duration = Duration::from_secs(5);
    const REPEAT: Duration = Duration::from_millis(100);
    const LISTEN: Duration = Duration::from_millis(400);

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn an_approved_renewal_gives_working_credentials_of_the_same_kind() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let creds = server.creds_as(&p("dev-a"), Role::Ant);
        let (old_jwt, old_key) = auth::read_creds(&creds).unwrap();
        let before = Finding::from_jwt(&old_jwt).unwrap();
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
        let user = KeyPair::new_user();

        let renewing = Renewal { creds: &creds, user: &user, xkey: None };
        let asking = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending());
        let approving = async {
            let heard = approver.renewals(None).await.unwrap();
            assert_eq!(heard.len(), 1, "{heard:?}");
            assert_eq!((heard[0].name.as_str(), heard[0].public_key.as_str()), ("dev-a", user.public_key().as_str()));
            approver.renew(&team, "dev-a", None).await.unwrap().1
        };
        let (jwt, renewed) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
        let jwt = jwt.unwrap();
        assert_eq!(jwt, renewed.jwt);
        assert_eq!(renewed.previous, old_key.public_key());
        assert!(before.same_credentials(&renewed.finding));

        auth::replace_creds(&creds, &jwt, &user, None).unwrap();
        let (renewed_jwt, key) = auth::read_creds(&creds).unwrap();
        assert_eq!((renewed_jwt, key.public_key()), (jwt, user.public_key()));
        let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("dev-a"), "test").await.unwrap();
        client.flush().await.unwrap();
        let line = revoke::issued(&team).unwrap().pop().unwrap();
        assert_eq!((line.key, line.renewed_from), (user.public_key(), Some(old_key.public_key())));
    }

    #[tokio::test]
    async fn renewing_stored_credentials_keeps_the_new_seed_in_the_store() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let store = keystore::Memory::default();
        let creds = server.creds_as(&p("dev-a"), Role::Ant);
        let Ok(keys::Moved::Now(old_key)) = keys::store_seed(&creds, &store, &server.url).await else { panic!() };
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
        let renewal = async |user: &KeyPair| {
            let renewing = Renewal { creds: &creds, user, xkey: None };
            let asking = request(&server.url, &creds, renewing, &store, REPEAT, pending());
            let approving = async {
                while approver.renewals(None).await.unwrap().is_empty() {}
                approver.renew(&team, "dev-a", None).await.unwrap()
            };
            let (jwt, _) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
            jwt.unwrap()
        };

        let refused = KeyPair::new_user();
        let jwt = renewal(&refused).await;
        let before = std::fs::read(&creds).unwrap();
        store.refuse_writes();
        let err = format!("{:#}", keys::replace(&creds, &jwt, &refused, None, &store, &server.url).await.unwrap_err());
        assert!(err.contains("stay as they were") && err.contains("denied"), "{err}");
        assert_eq!((std::fs::read(&creds).unwrap(), store.keys()), (before, vec![old_key.clone()]));
        store.allow_writes();

        let user = KeyPair::new_user();
        let jwt = renewal(&user).await;
        keys::replace(&creds, &jwt, &user, None, &store, &server.url).await.unwrap();
        let text = std::fs::read_to_string(&creds).unwrap();
        assert_eq!(creds::seed_of(&text), Some(creds::Seed::Stored(&user.public_key())));
        assert_eq!(store.keys(), [user.public_key()], "the earlier seed {old_key} goes");
        let (client, _) =
            bus::connect_with(&server.url, Some(&creds), &store, None, &p("dev-a"), "test").await.unwrap();
        client.flush().await.unwrap();
    }

    #[tokio::test]
    async fn renewing_a_human_device_without_an_encryption_key_binds_a_new_one_and_keeps_it() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let store = keystore::Memory::default();
        let creds = server.dir().join("alice.creds");
        let first = KeyPair::new_user();
        let rights = ants_proto::Permissions::for_user(&p("alice"), Role::Human);
        let human = revoke::Kind::Human;
        let (jwt, _) = auth::sign_device(&team, &p("alice"), human, rights, None, &first.public_key(), None).unwrap();
        auth::replace_creds(&creds, &jwt, &first, None).unwrap();
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
        let renewal = async |user: &KeyPair, xkey: Option<&DeviceKey>| {
            let renewing = Renewal { creds: &creds, user, xkey };
            let asking = request(&server.url, &creds, renewing, &store, REPEAT, pending());
            let approving = async {
                while approver.renewals(None).await.unwrap().is_empty() {}
                approver.renew(&team, "alice", None).await.unwrap()
            };
            let (jwt, _) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
            jwt.unwrap()
        };

        let finding = Finding::from_jwt(&jwt).unwrap();
        let xkey = device_xkey(&creds, &finding, &store).unwrap().expect("a new key for a human's without one");
        let asked = auth::device_key(&xkey).unwrap();
        let user = KeyPair::new_user();
        let jwt = renewal(&user, Some(&asked)).await;
        let renewed = Finding::from_jwt(&jwt).unwrap();
        assert_eq!(renewed.xkey.as_ref(), Some(&asked));
        let current = auth::verified(&auth::read_creds(&creds).unwrap().0).unwrap();
        assert!(check_renewed(&jwt, &current, &finding, &user.public_key(), None).is_err(), "an unasked key");
        keys::replace(&creds, &jwt, &user, Some(&xkey), &store, &server.url).await.unwrap();
        assert_eq!(creds::load_xkey(&creds, &store).unwrap().map(|x| x.public_key()), Some(xkey.public_key()));

        let kept = device_xkey(&creds, &renewed, &store).unwrap().expect("the key the file holds");
        assert_eq!(kept.public_key(), xkey.public_key());
        let again = KeyPair::new_user();
        let jwt = renewal(&again, None).await;
        assert_eq!(Finding::from_jwt(&jwt).unwrap().xkey, Some(asked));
        keys::replace(&creds, &jwt, &again, Some(&kept), &store, &server.url).await.unwrap();
        assert_eq!(creds::load_xkey(&creds, &store).unwrap().map(|x| x.public_key()), Some(xkey.public_key()));
        let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("alice"), "test").await.unwrap();
        client.flush().await.unwrap();
    }

    #[tokio::test]
    async fn auto_approval_renews_asking_credentials_and_answers_a_repeat_alike() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let creds = server.creds_as(&p("alice"), Role::Human);
        let user = KeyPair::new_user();
        let (done_tx, mut done) = tokio::sync::mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let approving = auto(
            &server.url,
            &team,
            None,
            LISTEN,
            Duration::from_millis(300),
            move |request, renewed| done_tx.send((request.name.clone(), renewed.jwt.clone())).unwrap(),
            async move {
                let _ = stop_rx.await;
            },
        );
        let asking = async {
            let renewing = Renewal { creds: &creds, user: &user, xkey: None };
            let first = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending()).await.unwrap();
            // Asking again with the same key, as a holder that missed the answer, gets the same, signed once.
            let renewing = Renewal { creds: &creds, user: &user, xkey: None };
            let again = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending()).await.unwrap();
            stop_tx.send(()).unwrap();
            (first, again)
        };
        let (approved, (first, again)) =
            timeout(WAIT * 2, Box::pin(async { tokio::join!(approving, asking) })).await.unwrap();
        approved.unwrap();
        assert_eq!(first, again);
        assert_eq!(done.recv().await.unwrap(), ("alice".to_owned(), first.clone()));
        assert!(done.try_recv().is_err(), "renewed once");
        auth::replace_creds(&creds, &first, &user, None).unwrap();
        let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("alice"), "test").await.unwrap();
        client.flush().await.unwrap();
    }

    /// Publish `request` with the credentials at `creds`, as a holder would, a few times.
    async fn forge(server: &Server, creds: &Path, request: &RenewRequest) {
        let client = async_nats::ConnectOptions::with_credentials_file(creds).await.unwrap().connect(&server.url).await;
        let client = client.unwrap();
        for _ in 0..6 {
            client.publish(request.subject(), serde_json::to_vec(request).unwrap().into()).await.unwrap();
            client.flush().await.unwrap();
            tokio::time::sleep(REPEAT).await;
        }
    }

    #[tokio::test]
    async fn requests_must_prove_valid_unrevoked_credentials_of_their_name() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let alice = server.creds_as(&p("alice"), Role::Human);
        let (alice_jwt, alice_key) = auth::read_creds(&alice).unwrap();
        let dev_a = server.creds_as(&p("dev-a"), Role::Ant);
        let (dev_a_jwt, _) = auth::read_creds(&dev_a).unwrap();
        let revoked = server.dir().join("dev-b.creds");
        auth::issue(&team, &p("dev-b"), Role::Ant, None, &revoked).unwrap();
        let (revoked_jwt, revoked_key) = auth::read_creds(&revoked).unwrap();
        revoke::revoke(&team, Some("dev-b"), &[]).unwrap();
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();

        let signed = |name: &str, current: &str, key: &KeyPair| {
            let mut request = RenewRequest::new(name, KeyPair::new_user().public_key(), "host");
            request.current = current.to_owned();
            request.signature = auth::sign(key, &request.signed()).unwrap();
            request
        };
        let stranger = KeyPair::new_user();
        let good = signed("alice", &alice_jwt, &alice_key);
        let mut tampered = good.clone();
        tampered.public_key = KeyPair::new_user().public_key();
        let forged = [
            // Another's credentials, signed by a key that is not theirs.
            signed("dev-a", &dev_a_jwt, &stranger),
            // Alice's credentials renamed: they are dev-a's no more than the key signing them is.
            signed("dev-a", &alice_jwt, &alice_key),
            tampered,
            signed("dev-b", &revoked_jwt, &revoked_key),
        ];
        let sending = async {
            for request in forged.iter().chain([&good]) {
                forge(&server, &alice, request).await;
            }
        };
        let listening = async {
            let mut heard = Vec::new();
            while !heard.contains(&good) {
                heard.extend(approver.renewals(None).await.unwrap());
            }
            heard
        };
        let ((), heard) = timeout(WAIT * 4, Box::pin(async { tokio::join!(sending, listening) })).await.unwrap();
        assert!(!heard.is_empty() && heard.iter().all(|r| *r == good), "{heard:?}");
        // The revoked credentials are refused by the signing too, whatever hears them.
        let err = auth::renew(&team, &revoked_jwt, &KeyPair::new_user().public_key(), None).unwrap_err();
        assert!(err.to_string().contains("revoked"), "{err:#}");
    }

    #[tokio::test]
    async fn the_holder_ignores_an_answer_changing_what_its_credentials_are() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let creds = server.creds_as(&p("dev-a"), Role::Ant);
        let user = KeyPair::new_user();
        // The approver's connection, which may answer.
        let client = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap().client;
        let renewing = Renewal { creds: &creds, user: &user, xkey: None };
        let asking = request(&server.url, &creds, renewing, keystore::os(), REPEAT, pending());
        let answering = async {
            tokio::time::sleep(REPEAT * 3).await;
            let human = auth::sign_user(&team, &p("dev-a"), Role::Human, None, &user.public_key()).unwrap();
            let other = auth::sign_user(&team, &p("dev-b"), Role::Ant, None, &user.public_key()).unwrap();
            for jwt in [human, other] {
                let reply = RenewReply::signed("dev-a", &user.public_key(), jwt);
                client.publish(reply.subject(), serde_json::to_vec(&reply).unwrap().into()).await.unwrap();
            }
            client.flush().await.unwrap();
            tokio::time::sleep(REPEAT * 3).await;
        };
        tokio::select! {
            jwt = asking => panic!("took an answer changing the credentials: {jwt:?}"),
            () = answering => {}
        }
    }

    #[tokio::test]
    async fn a_leaf_node_s_uplink_is_renewed_through_a_human_s_credentials() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let m1: ants_proto::Machine = "m1".parse().unwrap();
        let uplink = server.dir().join("leaf-m1.creds");
        let members = auth::members(&[p("dev-a")], &[p("alice")]);
        auth::issue_leaf(&team, &m1, &members, &[], EnrollApprover::With, None, &uplink).unwrap();
        let before = Finding::from_jwt(&auth::read_creds(&uplink).unwrap().0).unwrap();
        let alice = server.creds_as(&p("alice"), Role::Human);
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
        let user = KeyPair::new_user();
        let renewing = Renewal { creds: &uplink, user: &user, xkey: None };
        let asking = request(&server.url, &alice, renewing, keystore::os(), REPEAT, pending());
        let approving = approver.renew(&team, "leaf-m1", None);
        let (jwt, renewed) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
        let (_, renewed) = renewed.unwrap();
        assert_eq!(jwt.unwrap(), renewed.jwt);
        assert!(before.same_credentials(&renewed.finding), "{:?}", renewed.finding.kind);
        auth::check_leaf(&auth::account_key(&team).unwrap(), &renewed.jwt, &m1).unwrap();
    }

    #[test]
    fn expired_and_enrollment_credentials_name_their_renewal_on_the_team_key_machine() {
        let dir = ants_test_support::scratch_dir("renewable");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let enrollment = dir.join("enroll-dev-a.creds");
        auth::issue_enrollment(&team, &p("dev-a"), Duration::from_secs(60), &enrollment).unwrap();
        let (jwt, _) = auth::read_creds(&enrollment).unwrap();
        let err = renewable(&enrollment, &jwt).unwrap_err().to_string();
        assert!(err.contains("are not renewed over NATS") && err.contains("`ants auth enroll dev-a`"), "{err}");
        let ant = dir.join("dev-a.creds");
        auth::issue(&team, &p("dev-a"), Role::Ant, None, &ant).unwrap();
        let (jwt, _) = auth::read_creds(&ant).unwrap();
        assert!(renewable(&ant, &jwt).is_ok());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
