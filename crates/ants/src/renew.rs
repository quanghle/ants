//! Renewing credentials over NATS: where credentials are used, `ants renew` generates a new user NKey and asks for
//! the credentials renewed for it, signing the request with the key of the credentials it renews ([`request`]); on
//! the machine holding the account key, `ants enroll renew` signs a user JWT for the new key with the same name, kind,
//! members, device and JetStream domain, and the permissions their kind has in this version of ants
//! ([`auth::renew`]). The holder replaces its `.creds` file at once and reloads what uses it ([`run`]).
//!
//! As with enrollment, requests are core NATS messages, repeated every [`ENROLL_REPEAT`] until an answer comes, and
//! the answer carries only a JWT, useless without the seed that never leaves the holder. Only credentials still valid,
//! not revoked, and issued with permissions version [`RENEW_SINCE`] or later, whose subjects they need, may ask:
//! expired ones cannot connect, and are issued again with `ants auth` on the machine holding the team keys.
//!
//! The uplink credentials of a leaf node connect as no client: a human's credentials carry their request
//! (`ants renew LEAF.creds --creds HUMAN.creds`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Summary};
use ants_daemon::creds::{Expiry, Finding, Kind};
use ants_daemon::{bus, roster};
use ants_proto::{ENROLL_REPEAT, JsDomain, RENEW_SINCE, RenewReply, RenewRequest, UserId, subject};
use anyhow::{Context as _, Result, bail};
use chrono::Utc;
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::enroll::Approver;
use crate::service::{Action, Outcome, Service, Services, failure};
use crate::{auth, leaf};

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
    warn_old_uplink(args.leaf_config.as_deref(), &path);
    eprintln!(
        "asking to renew {} ({}) for the new key {}: approve with `ants enroll renew {}` on the machine holding the \
         account key; waiting (Ctrl-C stops)",
        finding.holder(),
        path.display(),
        user.public_key(),
        finding.name
    );
    let stop = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let jwt = request(nats, &carrier, &path, &user, ENROLL_REPEAT, stop).await?;
    auth::replace_creds(&path, &jwt, &user)?;
    println!("{} renewed: {}, key {}", path.display(), finding.holder(), user.public_key());
    reload(&path, args.config.as_deref(), args.leaf_config.as_deref())
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
    let why = match (&finding.kind, finding.expiry(now), finding.version) {
        (Kind::Enrollment { .. } | Kind::LeafSystem { .. }, _, _) => "are not renewed over NATS".to_owned(),
        (_, Expiry::Expired, _) => "expired, and expired credentials connect no more".to_owned(),
        (_, _, Some(version)) => {
            format!("carry permissions version {version}, older than renewal over NATS (version {RENEW_SINCE})")
        }
        (_, _, None) => format!("carry no permissions version, older than renewal over NATS (version {RENEW_SINCE})"),
    };
    bail!(
        "{} ({}) {why}: renew them on the machine holding the team keys, `{}`",
        finding.holder(),
        path.display(),
        finding.renew().join("`, then `")
    )
}

/// Ask for the credentials at `creds` renewed for the public key of `user`, over the server at `url`, connected with
/// the credentials at `carrier` (`creds` themselves, but for a leaf node's uplink): repeat the signed request every
/// `repeat` until an answer for the key arrives, the credentials expire or `stop` completes. Returns the user JWT of
/// the answer, checked to be the account's for that key, with the same name, kind, device and JetStream domain.
pub async fn request(
    url: &str,
    carrier: &Path,
    creds: &Path,
    user: &KeyPair,
    repeat: Duration,
    stop: impl Future<Output = ()>,
) -> Result<String> {
    let (current_jwt, current_key) = auth::read_creds(creds)?;
    let (current, finding) = renewable(creds, &current_jwt)?;
    let (carrier_jwt, _) = auth::read_creds(carrier)?;
    let carrier_name = auth::verified(&carrier_jwt).with_context(|| carrier.display().to_string())?.name;
    let carrier_user: UserId =
        carrier_name.parse().with_context(|| format!("{} connect as no client", carrier.display()))?;
    let (client, _) = bus::connect(url, Some(carrier), None, &carrier_user, "ants-renew").await?;
    let mut replies = client.subscribe(subject::renew_reply(&finding.name)).await?;
    let public_key = user.public_key();
    let mut request = RenewRequest::new(&finding.name, &public_key, roster::machine().await);
    request.current = current_jwt;
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
                match check_renewed(&reply.jwt, &current, &finding, &public_key) {
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
/// with the same name, kind, device and JetStream domain.
fn check_renewed(jwt: &str, current: &auth::Claims, finding: &Finding, public_key: &str) -> Result<()> {
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
    Ok(())
}

/// Warn when the uplink of the machine's leaf node, from the configuration at `leaf_config` or the default one,
/// cannot carry renewal requests, unless it is what `renewing` names.
fn warn_old_uplink(leaf_config: Option<&Path>, renewing: &Path) {
    let Some((conf, uplink)) = leaf_uplink(leaf_config) else { return };
    if same_file(&uplink, renewing) {
        return;
    }
    let finding = auth::read_creds(&uplink).ok().and_then(|(jwt, _)| Finding::from_jwt(&jwt).ok());
    if let Some(finding) = finding.filter(|f| f.version.is_none_or(|v| v < RENEW_SINCE)) {
        eprintln!(
            "warning: the leaf node of {} carries no renewal request with uplink credentials {} older than \
             permissions version {RENEW_SINCE}; connect to the hub with --nats, or renew them first: `{}`",
            conf.display(),
            uplink.display(),
            finding.renew().join("`, then `")
        );
    }
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
/// default one) when they are its uplink's.
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
        match services.apply(Service::Nats, Action::Restart, conf) {
            Outcome::Done(command) => println!("leaf node restarted to use them ({command})"),
            Outcome::NotRunning => println!("the leaf node service is not running; it uses them once started"),
            outcome => println!(
                "{}restart the leaf node to use them ({}); SIGHUP does not read them again",
                failure(outcome),
                crate::service::manual(Service::Nats, Action::Restart)
            ),
        }
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
    /// signed with their key.
    fn check_renewal(&self, request: &RenewRequest) -> Result<()> {
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
        auth::check_signature(&current.sub, &request.signed(), &request.signature).context("the credentials")
    }

    /// A checked renewal request of `msg`, or why it is ignored.
    fn renewal(&self, msg: &async_nats::Message) -> Result<RenewRequest> {
        let request: RenewRequest = serde_json::from_slice(&msg.payload).context("a malformed renewal request")?;
        request.check_subject(&msg.subject)?;
        self.check_renewal(&request).with_context(|| format!("renewal request of {}", request.name))?;
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
        let renewed = auth::renew(dir, &request.current, &request.public_key)?;
        let reply =
            RenewReply { name: name.to_owned(), public_key: request.public_key.clone(), jwt: renewed.jwt.clone() };
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

    async fn answer_renewal(&self, reply: &RenewReply) -> Result<()> {
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
                    match auth::renew(dir, &request.current, &request.public_key) {
                        Ok(done) => {
                            let reply = RenewReply {
                                name: request.name.clone(),
                                public_key: request.public_key.clone(),
                                jwt: done.jwt.clone(),
                            };
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
         `ants auth revoke --key {}`, then `ants auth push`",
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
    use crate::auth::tests::Server;
    use crate::revoke;

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

        let asking = request(&server.url, &creds, &creds, &user, REPEAT, pending());
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

        auth::replace_creds(&creds, &jwt, &user).unwrap();
        let (renewed_jwt, key) = auth::read_creds(&creds).unwrap();
        assert_eq!((renewed_jwt, key.public_key()), (jwt, user.public_key()));
        let (client, _) = bus::connect(&server.url, Some(&creds), None, &p("dev-a"), "test").await.unwrap();
        client.flush().await.unwrap();
        let line = revoke::issued(&team).unwrap().pop().unwrap();
        assert_eq!((line.key, line.renewed_from), (user.public_key(), Some(old_key.public_key())));
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
            let first = request(&server.url, &creds, &creds, &user, REPEAT, pending()).await.unwrap();
            // Asking again with the same key, as a holder that missed the answer, gets the same, signed once.
            let again = request(&server.url, &creds, &creds, &user, REPEAT, pending()).await.unwrap();
            stop_tx.send(()).unwrap();
            (first, again)
        };
        let (approved, (first, again)) =
            timeout(WAIT * 2, Box::pin(async { tokio::join!(approving, asking) })).await.unwrap();
        approved.unwrap();
        assert_eq!(first, again);
        assert_eq!(done.recv().await.unwrap(), ("alice".to_owned(), first.clone()));
        assert!(done.try_recv().is_err(), "renewed once");
        auth::replace_creds(&creds, &first, &user).unwrap();
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
        let err = auth::renew(&team, &revoked_jwt, &KeyPair::new_user().public_key()).unwrap_err();
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
        let asking = request(&server.url, &creds, &creds, &user, REPEAT, pending());
        let answering = async {
            tokio::time::sleep(REPEAT * 3).await;
            let human = auth::sign_user(&team, &p("dev-a"), Role::Human, None, &user.public_key()).unwrap();
            let other = auth::sign_user(&team, &p("dev-b"), Role::Ant, None, &user.public_key()).unwrap();
            for jwt in [human, other] {
                let reply = RenewReply { name: "dev-a".to_owned(), public_key: user.public_key(), jwt };
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
        auth::issue_leaf(&team, &m1, &members, &[], true, None, &uplink).unwrap();
        let before = Finding::from_jwt(&auth::read_creds(&uplink).unwrap().0).unwrap();
        let alice = server.creds_as(&p("alice"), Role::Human);
        let approver = Approver::connect(&server.url, &team, None, LISTEN).await.unwrap();
        let user = KeyPair::new_user();
        let asking = request(&server.url, &alice, &uplink, &user, REPEAT, pending());
        let approving = approver.renew(&team, "leaf-m1", None);
        let (jwt, renewed) = timeout(WAIT * 2, Box::pin(async { tokio::join!(asking, approving) })).await.unwrap();
        let (_, renewed) = renewed.unwrap();
        assert_eq!(jwt.unwrap(), renewed.jwt);
        assert!(before.same_credentials(&renewed.finding), "{:?}", renewed.finding.kind);
        auth::check_leaf(&auth::account_key(&team).unwrap(), &renewed.jwt, &m1).unwrap();
    }

    #[test]
    fn expired_enrollment_and_older_credentials_name_their_renewal_on_the_team_key_machine() {
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
