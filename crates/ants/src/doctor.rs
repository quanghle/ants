//! `ants doctor`: check a machine's ants setup end to end, reporting each check with the command that fixes it.
//!
//! It changes nothing: the hub's stream and buckets are looked up ([`hub::check`]), not created when missing as the
//! first connection of other commands does, and only the requests the credentials
//! allow are made, so none just times out. Every network check has a timeout; a harness handshake has its own.

use std::fmt;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Ant};
use ants_daemon::creds::{self, Expiry, Kind, Seed, VersionStatus};
use ants_daemon::harness::{self, Harnesses, Readiness};
use ants_daemon::keystore::{self, Store};
use ants_daemon::storage::{self, Usage};
use ants_daemon::{Launch, PermissionPolicy, bus, hub, reactions};
use ants_proto::{
    INTAKE_STREAM, JsDomain, Machine, PERMISSIONS_VERSION, ReactionChange, ReactionsRequest, ReactionsResponder,
    STREAM, UserId, Uuid, inbox_consumer,
};
use async_nats::jetstream;
use hyper::StatusCode;
use serde::Serialize;

use crate::bridge::{self, Probe};
use crate::humans::{Me, Rights};
use crate::service::{Action, Service, Services, State, Unit};
use crate::webhook::{self, SecretSource};
use crate::{auth, backup, leaf, notify, outbox, revoke, versions};

/// How long a network check may take.
pub const NETWORK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Warn,
    Fail,
    /// The check could not run, for the reason its detail gives.
    Skip,
}

/// The outcome of one check, with the commands fixing a warning or a failure.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Check {
    pub check: String,
    pub status: Status,
    pub detail: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fix: Vec<String>,
}

impl Check {
    fn new(check: impl Into<String>, status: Status, detail: impl Into<String>, fix: Vec<String>) -> Self {
        Self { check: check.into(), status, detail: detail.into(), fix }
    }

    fn ok(check: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(check, Status::Ok, detail, Vec::new())
    }

    fn skip(check: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::new(check, Status::Skip, detail, Vec::new())
    }

    fn warn(check: impl Into<String>, detail: impl Into<String>, fix: Vec<String>) -> Self {
        Self::new(check, Status::Warn, detail, fix)
    }

    fn fail(check: impl Into<String>, detail: impl Into<String>, fix: Vec<String>) -> Self {
        Self::new(check, Status::Fail, detail, fix)
    }
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = match self.status {
            Status::Ok => "ok",
            Status::Warn => "warn",
            Status::Fail => "fail",
            Status::Skip => "skip",
        };
        write!(f, "{status:<5} {}: {}", self.check, self.detail)?;
        for fix in &self.fix {
            write!(f, "\n      fix: {fix}")?;
        }
        Ok(())
    }
}

/// What to check: the connection the CLI resolved, and the machine's files.
pub struct Setup {
    pub nats: String,
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// The config file of the machine's ants.
    pub config: PathBuf,
    /// The leaf node configuration of `ants leaf config`.
    pub leaf_config: PathBuf,
    /// The local hub's nats-server configuration of `ants setup`, whose service is checked when it is there and the
    /// leaf node configuration is not.
    pub hub_config: PathBuf,
    /// Whether to start each harness for an ACP handshake.
    pub harnesses: bool,
    /// The team directory of `ants auth`, whose system account key, when it is there, asks the hub how it keeps
    /// pushed accounts.
    pub team: PathBuf,
    /// The config file of `ants webhook serve`, checked when it is there.
    pub webhooks: PathBuf,
    /// The home directory whose installed services are checked; None for none.
    pub home: Option<PathBuf>,
}

/// Run every check, in order: the config file, the CLI's credentials, the server, JetStream, the hub's stream and
/// buckets, the versions of ants running across the team, the JetStream storage, each ant's credentials, harness,
/// workdir and reactions responder, the hub's account resolver, the team keys' backup, the leaf node and outbox, the installed services, the
/// webhooks' config file and notification sinks, and the Dev Tunnel and HTTPS bridge services installed.
pub async fn run(setup: &Setup) -> Vec<Check> {
    let now = auth::now().unwrap_or_default();
    let mut checks = Vec::new();
    let loaded = check_config(&setup.config, &mut checks);
    let identity = check_cli_creds(setup, now, &mut checks);
    let leaf = Leaf::read(&setup.leaf_config);
    let ants = loaded.as_ref().map_or_default(|l| l.ants.as_slice());
    let reach = reach(setup, identity.as_ref(), leaf.as_ref().and_then(|l| l.as_ref().ok()));
    let ants_checked = async {
        match &loaded {
            Some(loaded) => check_ants(&loaded.ants, &loaded.harnesses, setup, now).await,
            None => vec![Check::skip("ants", "no config file read: no ants to check")],
        }
    };
    let through_leaf =
        leaf.as_ref().and_then(|l| l.as_ref().ok()).is_some_and(|l| same_address(&setup.nats, &l.listen));
    let resolver = check_resolver(setup, through_leaf);
    let reactions = check_reactions(ants, setup, NETWORK_TIMEOUT);
    let ((reached, connected), ants_checked, resolver, reactions) =
        tokio::join!(reach, ants_checked, resolver, reactions);
    checks.extend(reached);
    let settings = loaded.as_ref().map_or_default(|l| l.storage);
    checks.extend(check_storage(connected.as_ref(), setup, settings).await);
    checks.extend(ants_checked);
    checks.extend(check_shared_workdirs(ants, &setup.config));
    checks.extend(reactions);
    if let Some(loaded) = &loaded {
        let webhooks = setup.webhooks.exists().then(|| webhook::Config::read(&setup.webhooks).ok()).flatten();
        checks.extend(check_bypassed(loaded, webhooks.as_ref().map_or_default(|c| c.webhooks.as_slice())));
    }
    checks.push(resolver);
    checks.extend(check_backup(&setup.team));
    let human = identity.as_ref().map(|i| &i.me).filter(|me| me.role.is_human() && !me.id.is_human());
    checks.extend(check_leaf(setup, leaf, ants, human, connected.as_ref(), now).await);
    let (services, transports) = {
        let mut services = Services::under(setup.home.clone());
        (check_services(setup, &mut services), installed_transports(&mut services))
    };
    checks.extend(services);
    checks.extend(check_webhooks(&setup.webhooks).await);
    checks.extend(check_transports(transports, now).await);
    checks
}

/// Who the CLI connects as.
struct Identity {
    me: Me,
    rights: Rights,
    /// None without credentials.
    finding: Option<creds::Finding>,
}

/// A connection to the server, as [`Identity`].
struct Connected {
    js: jetstream::Context,
    rights: Rights,
}

fn check_config(path: &Path, checks: &mut Vec<Check>) -> Option<config::Loaded> {
    if !path.exists() {
        checks.push(Check::skip("config", format!("no config file at {}: no ants on this machine", path.display())));
        return None;
    }
    match config::load(path) {
        Ok(loaded) => {
            let names: Vec<&str> = loaded.ants.iter().map(|ant| ant.name.as_str()).collect();
            checks.push(Check::ok("config", format!("{} lists {}", path.display(), names.join(", "))));
            Some(loaded)
        }
        Err(e) => {
            let mut fix = vec![format!("correct {}; `ants daemon --config {0}` refuses it as it is", path.display())];
            if harness::Unset::caused(&e) {
                let builtin = Harnesses::builtin().names().collect::<Vec<_>>().join(", ");
                fix.insert(
                    0,
                    format!(
                        "add `harness = \"NAME\"` at the top of {}, NAME a built-in harness ({builtin}) or a \
                         [harnesses.NAME] table; `ants setup` lists the ones installed here",
                        path.display()
                    ),
                );
            }
            checks.push(Check::fail("config", format!("{e:#}"), fix));
            None
        }
    }
}

fn check_cli_creds(setup: &Setup, now: u64, checks: &mut Vec<Check>) -> Option<Identity> {
    let check = "credentials";
    let Some(path) = &setup.creds else {
        checks.push(Check::skip(check, "none set (--creds, ANTS_CREDS or `creds` in [cli]): connecting as no one"));
        let me = Me::from_creds(None).ok()?;
        return Some(Identity { me, rights: Rights::from_creds(None).ok()?, finding: None });
    };
    let read = creds::check(path).and_then(|finding| {
        Ok(Identity {
            me: Me::from_creds(Some(path))?,
            rights: Rights::from_creds(Some(path))?,
            finding: Some(finding),
        })
    });
    match read {
        Ok(identity) => {
            let finding = identity.finding.as_ref()?;
            checks.push(check_creds(check, path, finding, now));
            checks.extend(check_key("credentials key", path, finding.renew(), keystore::os()));
            checks.extend(check_xkey("credentials xkey", path, finding, keystore::os()));
            Some(identity)
        }
        Err(e) => {
            let fix = format!(
                "ants auth user HUMAN --human on the machine holding the team keys, then name the file in `creds` of \
                 [cli] in {}",
                setup.config.display()
            );
            checks.push(Check::fail(check, format!("{e:#}"), vec![fix]));
            None
        }
    }
}

/// The check of credentials at `path`: their holder, permissions version and expiry ([`creds::Finding`]), failing
/// once expired and warning as [`creds::Finding::warnings`] does, with the commands renewing them.
fn check_creds(check: &str, path: &Path, finding: &creds::Finding, now: u64) -> Check {
    let version = format!("permissions version {}", finding.version);
    let at = |exp: Option<u64>| exp.map_or_default(creds::time);
    let mut problems = Vec::new();
    match finding.version_status() {
        VersionStatus::Current => {}
        VersionStatus::Outdated => problems.push(format!(
            "{version}, older than version {PERMISSIONS_VERSION} of this ants: requests their role allows now may be \
             refused"
        )),
        VersionStatus::Newer => {
            let detail = format!("{version}, newer than version {PERMISSIONS_VERSION} of this ants");
            return Check::warn(
                check,
                format!("{} ({}): {detail}", finding.holder(), path.display()),
                vec!["upgrade ants".to_owned()],
            );
        }
    }
    let expiry = finding.expiry(now);
    match expiry {
        Expiry::Expired => problems.push(format!("expired at {}", at(finding.expires_at))),
        Expiry::Soon if !matches!(finding.kind, Kind::Enrollment { .. }) => {
            problems.push(format!("expire at {}", at(finding.expires_at)));
        }
        _ => {}
    }
    let holder = format!("{} ({})", finding.holder(), path.display());
    if problems.is_empty() {
        let expires = match finding.expires_at {
            Some(exp) => format!("expiring at {}", creds::time(exp)),
            None => "never expiring".to_owned(),
        };
        return Check::ok(check, format!("{holder}: {version}, {expires}"));
    }
    let when = if finding.version_status() == VersionStatus::Outdated { finding.renew_when() } else { "" };
    let status = if expiry == Expiry::Expired { Status::Fail } else { Status::Warn };
    if finding.renewable(now) {
        let detail = format!(
            "{holder}: {}; renew them over NATS, approved on the machine holding the account key{when}",
            problems.join("; ")
        );
        return Check::new(check, status, detail, finding.renew_over_nats(path));
    }
    let detail = format!("{holder}: {}; renew them on the machine holding the team keys{when}", problems.join("; "));
    Check::new(check, status, detail, finding.renew())
}

/// The check of the seed that the credentials at `path` keep in `store` in their place, when they name a stored key:
/// that the store answers, holds it, and that it is the key of their user JWT. `issue` has them issued again. Never
/// shows the seed.
fn check_key(check: &str, path: &Path, issue: Vec<String>, store: &dyn Store) -> Option<Check> {
    let text = std::fs::read_to_string(path).ok()?;
    let Some(Seed::Stored(key)) = creds::seed_of(&text) else { return None };
    Some(match creds::load(path, store) {
        Ok(_) => Check::ok(check, format!("{key} ({}): its seed is in the OS credential store", path.display())),
        Err(e) => unloaded(check, &e, issue),
    })
}

/// The check of the encryption key of a human's credentials at `path`, of `finding`: a user JWT binding none warns,
/// with `ants renew` to add one; a seed missing, unreadable or of another key than the JWT binds fails, as in
/// [`check_key`]. None for others' credentials. Never shows the seed.
fn check_xkey(check: &str, path: &Path, finding: &creds::Finding, store: &dyn Store) -> Option<Check> {
    if finding.kind != Kind::Human {
        return None;
    }
    let Some(bound) = &finding.xkey else {
        let detail = format!(
            "{} ({}) bind no encryption key, so no message can be sealed for this device",
            finding.holder(),
            path.display()
        );
        return Some(Check::warn(check, detail, finding.renew_over_nats(path)));
    };
    let stored = std::fs::read_to_string(path).is_ok_and(|text| matches!(creds::xkey_of(&text), Some(Seed::Stored(_))));
    let held = if stored { "the OS credential store" } else { "the file" };
    Some(match creds::load_xkey(path, store) {
        Ok(_) => Check::ok(check, format!("{bound} ({}): its seed is in {held}", path.display())),
        Err(e) => unloaded(check, &e, finding.renew()),
    })
}

/// A failed check of a seed that did not load: unlocking the OS credential store when it did not answer, else
/// restoring the seed or `issue` again.
fn unloaded(check: &str, e: &anyhow::Error, issue: Vec<String>) -> Check {
    if keystore::is_unavailable(e) {
        let fix = "unlock the OS credential store (log in), then check again; services started before then wait for it";
        return Check::fail(check, format!("{e:#}"), vec![fix.to_owned()]);
    }
    let restore = "restore the seed from a backup of the OS credential store, or issue the credentials again:";
    Check::fail(check, format!("{e:#}"), std::iter::once(restore.to_owned()).chain(issue).collect())
}

/// Connect, then check JetStream and the hub's stream and buckets.
async fn reach(setup: &Setup, identity: Option<&Identity>, leaf: Option<&Leaf>) -> (Vec<Check>, Option<Connected>) {
    let Some(identity) = identity else {
        return (vec![Check::skip("server", "no credentials to connect with")], None);
    };
    let me = &identity.me;
    let through_leaf = leaf.filter(|leaf| same_address(&setup.nats, &leaf.listen));
    let client = match connect(&setup.nats, setup.creds.as_deref(), &me.id).await {
        Ok(client) => client,
        Err(e) => return (vec![unreached(setup, identity, through_leaf, e)], None),
    };
    let info = client.server_info();
    let via = if through_leaf.is_some() { "the leaf node " } else { "" };
    let connected = format!(
        "connected to {via}{} (server {}, nats-server {}) as {}",
        setup.nats, info.server_name, info.version, me.id
    );
    let (major, minor) = MIN_NATS_SERVER;
    let mut checks = vec![if supported_server(&info.version) {
        Check::ok("server", connected)
    } else {
        let detail = format!("{connected}: ants needs nats-server {major}.{minor} or later");
        Check::fail("server", detail, vec![format!("upgrade nats-server to {major}.{minor} or later")])
    }];
    let mut js = bus::jetstream(client, setup.js_domain.as_ref());
    js.set_timeout(NETWORK_TIMEOUT);
    let answered = check_jetstream(setup, identity, &js, leaf, through_leaf.is_some()).await;
    let reached = answered.status == Status::Ok;
    checks.push(answered);
    if !reached {
        checks.push(Check::skip("hub", "JetStream does not answer"));
        checks.push(Check::skip("consumers", "JetStream does not answer"));
        checks.push(Check::skip("versions", "JetStream does not answer"));
        checks.push(Check::skip("harness versions", "JetStream does not answer"));
        return (checks, None);
    }
    checks.extend(check_hub(&js, identity).await);
    checks.extend(check_consumers(&js, identity).await);
    checks.push(check_versions(&js, identity).await);
    checks.push(check_harness_versions(&js).await);
    (checks, Some(Connected { js, rights: identity.rights.clone() }))
}

/// The oldest nats-server, major and minor version, ants runs on.
const MIN_NATS_SERVER: (u32, u32) = (2, 15);

/// Whether nats-server `version` is [`MIN_NATS_SERVER`] or later; a version that does not parse passes.
fn supported_server(version: &str) -> bool {
    let mut parts = version.split(['.', '-']).map(|part| part.parse::<u32>().ok());
    match (parts.next().flatten(), parts.next().flatten()) {
        (Some(major), Some(minor)) => (major, minor) >= MIN_NATS_SERVER,
        _ => true,
    }
}

/// Connect to `url` with `creds` as `user`, as the CLI does, within [`NETWORK_TIMEOUT`].
async fn connect(url: &str, creds: Option<&Path>, user: &UserId) -> Result<async_nats::Client, Unreached> {
    bus::check_url(url).map_err(|e| Unreached::Other(format!("{e:#}")))?;
    let mut options = async_nats::ConnectOptions::new()
        .name("ants-doctor")
        .connection_timeout(NETWORK_TIMEOUT)
        .custom_inbox_prefix(ants_proto::inbox_prefix(user))
        .event_callback(|_| async {});
    if let Some(path) = creds {
        options = bus::authenticate(options, path, keystore::os()).map_err(|e| Unreached::NoKey(format!("{e:#}")))?;
    }
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, options.connect(url)).await {
        Ok(Ok(client)) => Ok(client),
        Ok(Err(e)) => Err(match e.kind() {
            async_nats::ConnectErrorKind::AuthorizationViolation | async_nats::ConnectErrorKind::Authentication => {
                Unreached::Refused(e.to_string())
            }
            async_nats::ConnectErrorKind::Io
            | async_nats::ConnectErrorKind::TimedOut
            | async_nats::ConnectErrorKind::Dns => Unreached::Unreachable(e.to_string()),
            _ => Unreached::Other(e.to_string()),
        }),
        Err(_) => Err(Unreached::Unreachable(format!("no connection within {}s", (NETWORK_TIMEOUT * 2).as_secs()))),
    }
}

enum Unreached {
    /// The server refused the credentials.
    Refused(String),
    /// No server answered.
    Unreachable(String),
    /// The credentials' seed could not be read.
    NoKey(String),
    Other(String),
}

fn unreached(setup: &Setup, identity: &Identity, through_leaf: Option<&Leaf>, e: Unreached) -> Check {
    let check = "server";
    let url = &setup.nats;
    let set_nats = format!(
        "check that a server runs there, or point --nats, ANTS_NATS_URL or `nats` of [cli] in {} at one",
        setup.config.display()
    );
    match e {
        Unreached::Refused(e) => {
            let fix = match &identity.finding {
                Some(finding) => finding.renew(),
                None => vec!["ants auth user HUMAN --human, then --creds FILE".to_owned()],
            };
            let detail = format!("{url} refused the credentials ({e}): revoked, expired or of another team");
            Check::fail(check, detail, fix)
        }
        Unreached::Unreachable(e) => match through_leaf {
            Some(leaf) => {
                let fix = match Services::local().state(Service::Nats, &leaf.path) {
                    State::Stopped(start) => start,
                    _ => format!(
                        "nats-server -c {}, or scripts/install-units.sh to run it as a service",
                        leaf.path.display()
                    ),
                };
                Check::fail(check, format!("the leaf node at {url} does not answer: {e}"), vec![fix])
            }
            None => Check::fail(check, format!("{url} does not answer: {e}"), vec![set_nats]),
        },
        Unreached::NoKey(e) => Check::skip(check, format!("no seed to connect with: {e}")),
        Unreached::Other(e) => Check::fail(check, format!("connecting to {url}: {e}"), vec![set_nats]),
    }
}

/// Whether JetStream answers in the domain the CLI uses, the hub's: not the leaf node's own, and one the credentials
/// were issued for.
async fn check_jetstream(
    setup: &Setup,
    identity: &Identity,
    js: &jetstream::Context,
    leaf: Option<&Leaf>,
    through_leaf: bool,
) -> Check {
    let check = "jetstream";
    let domain = setup.js_domain.as_ref();
    let config = setup.config.display();
    let issued = identity.finding.as_ref().and_then(|f| f.js_domain.as_ref());
    let hub_domain = issued.or(leaf.and_then(|l| l.hub_domain.as_ref())).map_or("HUB".to_owned(), ToString::to_string);
    let set_domain = format!("--js-domain {hub_domain}, or js_domain = \"{hub_domain}\" in {config}");
    match (issued, domain, &identity.finding) {
        (Some(issued), Some(used), _) if issued != used => {
            let detail = format!("the credentials were issued for JetStream domain {issued}, the CLI uses {used}");
            return Check::fail(check, detail, vec![set_domain]);
        }
        (None, Some(used), Some(finding)) => {
            let detail = format!("the credentials were issued for no JetStream domain, the CLI uses {used}");
            let fix = finding.renew().into_iter().take(1).map(|c| format!("{c} --js-domain {used}")).collect();
            return Check::fail(check, detail, fix);
        }
        _ => {}
    }
    if let (Some(leaf), true, None) = (leaf, through_leaf, domain) {
        let detail = format!(
            "no JetStream domain set: requests reach the leaf node's own JetStream (domain {}), not the hub's",
            leaf.machine
        );
        return Check::fail(check, detail, vec![set_domain]);
    }
    let me = &identity.me.id;
    let rights = &identity.rights;
    let in_domain = domain.map_or("without a domain".to_owned(), |d| format!("in domain {d}"));
    let probe = format!("$JS.API.CONSUMER.INFO.{STREAM}.{}", inbox_consumer(me));
    let account = rights.may_publish("$JS.API.INFO");
    if !rights.may_publish(&probe) && !account {
        return Check::skip(check, "the credentials may ask JetStream nothing ants checks with");
    }
    if rights.may_publish(&probe)
        && let Err(e) = bus::reach_jetstream(js, me).await
    {
        let fix = match (domain, through_leaf) {
            (None, _) => set_domain,
            (Some(_), true) => "check the leaf node's uplink: its log, and `ants doctor` on the hub".to_owned(),
            (Some(d), false) => format!("check that the server's JetStream runs in domain {d}, or {set_domain}"),
        };
        return Check::fail(check, format!("no JetStream answers {in_domain}: {e}"), vec![fix]);
    }
    if !account {
        return Check::ok(check, format!("answers {in_domain}"));
    }
    match js.query_account().await {
        Ok(info) => {
            let answering = info.domain.filter(|d| !d.is_empty());
            if let (Some(leaf), Some(answering)) = (leaf, &answering)
                && answering == leaf.machine.as_str()
            {
                let detail =
                    format!("the leaf node's own JetStream (domain {answering}) answers, not the hub's: {in_domain}");
                return Check::fail(check, detail, vec![set_domain]);
            }
            match answering {
                Some(answering) if domain.is_none() => {
                    Check::ok(check, format!("answers {in_domain}, that of domain {answering}"))
                }
                _ => Check::ok(check, format!("answers {in_domain}")),
            }
        }
        Err(e) => Check::fail(check, format!("no JetStream answers {in_domain}: {e}"), vec![set_domain]),
    }
}

/// The hub's stream and buckets, as [`hub::check`] finds them with the credentials' rights.
async fn check_hub(js: &jetstream::Context, identity: &Identity) -> Vec<Check> {
    let check = "hub";
    if !identity.me.role.is_human() {
        return vec![Check::skip(check, "an ant's credentials get no stream info: run `ants doctor` as a human")];
    }
    let may = |subject: &str| identity.rights.may_publish(subject);
    let findings = match tokio::time::timeout(NETWORK_TIMEOUT * 2, hub::check(js, may)).await {
        Ok(findings) => findings,
        Err(_) => return vec![Check::warn(check, "no stream info within 10s", vec!["ants init".to_owned()])],
    };
    let unseen: Vec<String> =
        hub::Object::ALL.into_iter().filter(|o| !may(&o.info_subject())).map(|o| o.to_string()).collect();
    if findings.is_empty() {
        let checked = hub::Object::ALL.len() - unseen.len();
        let mut detail = format!("stream {STREAM} and {} buckets present", checked.saturating_sub(1));
        if !unseen.is_empty() {
            let _ = write!(detail, " ({} not checked: the credentials get no stream info of it)", unseen.join(", "));
        }
        return vec![Check::ok(check, detail)];
    }
    findings
        .iter()
        .map(|finding| {
            let fix = if finding.rights().iter().all(|s| may(s)) {
                "ants init".to_owned()
            } else {
                "ants init with a human's current credentials (`ants auth user NAME --human`)".to_owned()
            };
            match finding.problem {
                hub::Problem::Unchecked(_) | hub::Problem::ShortWindow { .. } => {
                    Check::warn(check, finding.to_string(), vec![fix])
                }
                _ => Check::fail(check, finding.to_string(), vec![fix]),
            }
        })
        .collect()
}

/// The consumers of the `ANTS` stream, and of each bucket every daemon watches ([`hub::Object::watched`]), against the
/// most each takes ([`consumer_limit`]): past that, creating one fails, so an ant cannot read its inbox, or a daemon
/// cannot watch the bucket. Warns from four fifths of it. Those whose stream info the credentials do not get are left
/// out, the stream with a skip.
async fn check_consumers(js: &jetstream::Context, identity: &Identity) -> Vec<Check> {
    let check = "consumers";
    let objects = std::iter::once(hub::Object::Stream).chain(hub::Object::ALL.into_iter().filter(|o| o.watched()));
    let (seen, unseen): (Vec<_>, Vec<_>) =
        objects.partition(|object| identity.rights.may_publish(&object.info_subject()));
    let mut checks = Vec::new();
    if unseen.contains(&hub::Object::Stream) {
        let detail = format!("the credentials get no stream info of {STREAM}: run `ants doctor` as a human");
        checks.push(Check::skip(check, detail));
    }
    let infos = seen.into_iter().map(async |object| (object, js.get_stream(object.stream_name()).await));
    for (object, info) in futures::future::join_all(infos).await {
        checks.push(match info {
            Ok(stream) => {
                let info = stream.cached_info();
                consumers(object, info.state.consumer_count, consumer_limit(&info.config))
            }
            Err(e) => Check::skip(check, format!("no stream info of {}: {e}", object.stream_name())),
        });
    }
    checks
}

/// JetStream storage against its limits ([`storage::measure`]): the hub's, and the leaf node's when the CLI connects
/// through one. Warns past the warning share of `settings` (its default when 0), naming the streams using most when
/// the credentials list them.
async fn check_storage(connected: Option<&Connected>, setup: &Setup, settings: storage::Settings) -> Vec<Check> {
    let check = "storage";
    let Some(Connected { js, rights }) = connected else {
        return vec![Check::skip(check, "JetStream does not answer")];
    };
    if !rights.may_publish("$JS.API.INFO") {
        return vec![Check::skip(check, "the credentials may not read the account's JetStream info")];
    }
    let measured = tokio::time::timeout(NETWORK_TIMEOUT * 2, storage::measure(js, setup.js_domain.as_ref(), &settings));
    let usages = match measured.await {
        Ok(Ok(usages)) => usages,
        Ok(Err(e)) => return vec![Check::skip(check, format!("{e:#}"))],
        Err(_) => return vec![Check::skip(check, "no account info in time")],
    };
    let warn_percent = Some(settings.warn_percent).filter(|p| *p > 0).unwrap_or(storage::DEFAULT_WARN_PERCENT);
    let mut checks = Vec::new();
    for usage in &usages {
        let mut largest = Vec::new();
        if usage.over(warn_percent) && rights.may_publish("$JS.API.STREAM.LIST") {
            largest = storage::largest(js, usage, 3).await.unwrap_or_default();
        }
        checks.push(storage_check(usage, &largest, warn_percent, &setup.config));
    }
    checks
}

fn storage_check(usage: &Usage, largest: &[(String, u64)], warn_percent: u8, config: &Path) -> Check {
    let check = "storage";
    if usage.over(warn_percent) {
        let mut detail = format!("{usage}, past {warn_percent}%: once full, JetStream refuses new messages");
        if let Some(streams) = storage::using_most(largest) {
            let _ = write!(detail, "; using most: {streams}");
        }
        return Check::warn(check, detail, vec![usage.fix()]);
    }
    if usage.limit.is_none() {
        let setting = usage.store.setting();
        let detail =
            format!("{usage}: set {setting} in [storage] of {} to be warned before it fills", config.display());
        return Check::ok(check, detail);
    }
    Check::ok(check, usage.to_string())
}

/// The most consumers a stream configured as `config` takes: its `max_consumers`, else the 1000 nats-server gives a
/// stream by default (`default_max_consumers` in its configuration may change that).
fn consumer_limit(config: &jetstream::stream::Config) -> usize {
    usize::try_from(config.max_consumers).ok().filter(|&max| max > 0).unwrap_or(1000)
}

fn consumers(object: hub::Object, count: usize, limit: usize) -> Check {
    let check = "consumers";
    let name = object.stream_name();
    let detail = format!("{count} on stream {name}, which takes {limit}");
    if count.saturating_mul(5) < limit.saturating_mul(4) {
        return Check::ok(check, detail);
    }
    let raise = format!("nats stream edit {name} --max-consumers {}", limit.saturating_mul(2));
    let full = match object {
        hub::Object::Stream => "no ant can open a consumer to read its inbox".to_owned(),
        _ => format!("no daemon can watch {object}"),
    };
    Check::warn(check, format!("{detail}: once full, {full}"), vec![raise])
}

/// The versions of ants every daemon, webhook, schedule and channel manager runs ([`versions::running`]), and whether
/// credentials renewed with this ants suit them all.
async fn check_versions(js: &jetstream::Context, identity: &Identity) -> Check {
    let check = "versions";
    if !identity.me.role.is_human() {
        return Check::skip(check, "an ant's credentials read no versions: run `ants doctor` as a human");
    }
    let may = |subject: &str| identity.rights.may_publish(subject);
    let now = chrono::Utc::now();
    let running = match tokio::time::timeout(NETWORK_TIMEOUT * 2, versions::running(js, may, now)).await {
        Ok(Ok(running)) => running,
        Ok(Err(e)) => return Check::warn(check, format!("{e:#}"), Vec::new()),
        Err(_) => return Check::warn(check, "no versions read within 10s", Vec::new()),
    };
    let mut notes: Vec<String> =
        versions::mixed(&running.components).map(|v| format!("mixed: {v}")).into_iter().collect();
    notes.extend(running.unseen);
    let notes = if notes.is_empty() { String::new() } else { format!(" ({})", notes.join("; ")) };
    let renewing = "renewing ant, webhook, channel manager and leaf credentials";
    match versions::renewal(&running.components) {
        versions::Renewal::Allowed => Check::ok(
            check,
            format!(
                "{} running components at permissions version {PERMISSIONS_VERSION} or newer: {renewing} is safe{notes}",
                running.components.len()
            ),
        ),
        versions::Renewal::Behind(behind) => Check::warn(
            check,
            format!("older than permissions version {PERMISSIONS_VERSION}: {}{notes}", behind.join(", ")),
            vec![format!("upgrade ants where they run and restart them before {renewing}")],
        ),
        versions::Renewal::Ahead(ahead) => Check::warn(
            check,
            format!("newer than permissions version {PERMISSIONS_VERSION} of this ants: {}{notes}", ahead.join(", ")),
            vec![format!("upgrade ants here and on the machine holding the team keys before {renewing}")],
        ),
    }
}

/// Whether live ants run the same harness agent at different versions on different machines, as their roster entries
/// name it ([`versions::harness_drift`]).
async fn check_harness_versions(js: &jetstream::Context) -> Check {
    use anyhow::Context as _;
    let read = async {
        let kv = js.get_key_value(ants_proto::ROSTER_BUCKET).await.context("opening the roster")?;
        ants_daemon::roster::list_entries(&kv).await
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok(entries)) => harness_versions(&entries, chrono::Utc::now()),
        Ok(Err(e)) => Check::skip("harness versions", format!("no roster read: {e:#}")),
        Err(_) => Check::skip("harness versions", "no roster read within 10s"),
    }
}

fn harness_versions(entries: &[ants_proto::RosterEntry], now: chrono::DateTime<chrono::Utc>) -> Check {
    let check = "harness versions";
    let drift = versions::harness_drift(entries, now);
    if !drift.is_empty() {
        return Check::warn(
            check,
            format!("different versions across machines: {}", drift.join("; ")),
            vec![
                "upgrade the older harness agents where they run; an ant's roster entry shows the new version once its \
                 harness starts again"
                    .to_owned(),
            ],
        );
    }
    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    let named = entries.iter().filter(|e| ants_daemon::roster::is_live(e, now)).filter_map(|e| e.agent.as_ref());
    for agent in named {
        *counts.entry(agent.to_string()).or_default() += 1;
    }
    if counts.is_empty() {
        return Check::ok(
            check,
            "no live ant names its harness agent yet: none started a harness since its daemon did",
        );
    }
    let running: Vec<String> = counts.into_iter().map(|(agent, n)| format!("{agent} ({n})")).collect();
    Check::ok(check, format!("live ants run {}", running.join(", ")))
}

/// Whether the hub keeps the accounts `ants auth push` sends it across restarts, as only a `full` resolver does
/// ([`revoke::keeps_accounts`]): asked as a user of the team's system account, which only the machine holding the team
/// keys can sign.
async fn check_resolver(setup: &Setup, through_leaf: bool) -> Check {
    let check = "hub resolver";
    let conf = setup.team.join(auth::SERVER_CONF);
    if !auth::holds_team(&setup.team) {
        let detail = format!(
            "no team keys in {}: only the team's system account can ask, so run `ants doctor` where `ants auth` runs",
            setup.team.display()
        );
        return Check::skip(check, detail);
    }
    if through_leaf {
        return Check::skip(check, "the CLI connects to the leaf node: point --nats at the hub to check it");
    }
    let client = match tokio::time::timeout(NETWORK_TIMEOUT * 2, revoke::connect_system(&setup.nats, &setup.team)).await
    {
        Err(_) => return Check::skip(check, format!("{} does not answer", setup.nats)),
        Ok(Ok(client)) => client,
        Ok(Err(e)) => {
            let refused = e.chain().any(|e| {
                e.downcast_ref::<async_nats::ConnectError>().is_some_and(|e| {
                    matches!(
                        e.kind(),
                        async_nats::ConnectErrorKind::AuthorizationViolation
                            | async_nats::ConnectErrorKind::Authentication
                    )
                })
            });
            if !refused {
                return Check::skip(check, format!("connecting to {} as the system account: {e:#}", setup.nats));
            }
            let detail = format!(
                "{} refuses the team's system account ({e:#}): its server.conf is of another team or lacks \
                 the team's system account, and `ants auth push` fails",
                setup.nats
            );
            return Check::warn(check, detail, vec![copy_server_conf(&conf, "reload it (SIGHUP)")]);
        }
    };
    let server = client.server_info().server_name;
    match tokio::time::timeout(NETWORK_TIMEOUT, revoke::keeps_accounts(&client)).await {
        Ok(Some(true)) => {
            Check::ok(check, format!("server {server} keeps pushed accounts across restarts (full resolver)"))
        }
        Ok(Some(false)) => Check::warn(
            check,
            format!(
                "server {server} keeps pushed accounts in memory only: a restart brings back the account of its \
                 server.conf, revocations pushed since included, and leaf nodes offline during a push miss it"
            ),
            vec![
                format!(
                    "add `resolver {{ type: full, dir: \"/ABSOLUTE/DIR\" }}` after `include \"{}\"` in the hub's \
                     nats-server configuration and restart it (a reload does not switch resolvers)",
                    auth::SERVER_CONF
                ),
                copy_server_conf(&conf, "reload it (SIGHUP) until then"),
            ],
        ),
        _ => Check::skip(check, format!("server {server} did not say how it keeps pushed accounts")),
    }
}

fn copy_server_conf(conf: &Path, then: &str) -> String {
    format!("copy {} next to the hub's nats-server configuration and {then}", conf.display())
}

/// Whether a backup of `ants auth backup` holds the team keys of `team` and every key issued and revoked since
/// ([`backup::state`]); none without team keys.
fn check_backup(team: &Path) -> Option<Check> {
    if !auth::holds_team(team) {
        return None;
    }
    let check = "team keys backup";
    let dir = match auth::default_dir() {
        Ok(default) if default == team => String::new(),
        _ => format!(" --dir {}", team.display()),
    };
    let fix = vec![format!("ants auth backup FILE{dir}, then keep FILE off this machine")];
    let last = |record: &backup::Record| format!("{} ({})", creds::time(record.at), record.file.display());
    Some(match backup::state(team) {
        Err(e) => Check::warn(check, format!("{e:#}"), fix),
        Ok(backup::State::None) => Check::warn(
            check,
            format!(
                "no backup of the team keys in {}: without them no credentials can be issued, renewed or revoked \
                 again",
                team.display()
            ),
            fix,
        ),
        Ok(backup::State::Current(record)) => Check::ok(check, format!("backed up {}", last(&record))),
        Ok(backup::State::Behind { record, issued, revoked }) => {
            let mut since = Vec::new();
            match issued {
                0 => {}
                1 => since.push("1 issued.jsonl entry (a key issued, renewed or relabelled)".to_owned()),
                2.. => since.push(format!("{issued} issued.jsonl entries (keys issued, renewed or relabelled)")),
                _ => since.push("issued.jsonl changes".to_owned()),
            }
            if revoked {
                since.push("revocations".to_owned());
            }
            Check::warn(check, format!("the last backup, {}, predates {}", last(&record), since.join(" and ")), fix)
        }
    })
}

/// Each ant's credentials, and, unless told not to, whether its harness is installed and signed in: one handshake per
/// harness and environment, all at once.
async fn check_ants(ants: &[Ant], harnesses: &Harnesses, setup: &Setup, now: u64) -> Vec<Check> {
    let config = setup.config.display();
    // The harness's name, the ant's environment, the harness and its launch.
    type Started = (String, Vec<(String, String)>, std::sync::Arc<ants_daemon::Harness>, Launch);
    let mut launches: Vec<Started> = Vec::new();
    let mut picked = Vec::with_capacity(ants.len());
    for ant in ants {
        let name = ant.harness.as_deref().unwrap_or_default();
        let env: Vec<(String, String)> = ant.env.clone().into_iter().collect();
        let index = match launches.iter().position(|(n, e, ..)| n == name && *e == env) {
            Some(index) => Some(index),
            None => harnesses.get(name).ok().map(|harness| {
                let cwd = Some(ant.workdir.clone())
                    .filter(|dir| dir.is_dir())
                    .or_else(|| std::env::current_dir().ok())
                    .unwrap_or_else(|| PathBuf::from("/"));
                let launch =
                    Launch::new(harness.clone(), cwd, env.clone(), PermissionPolicy::reject_all(), Default::default());
                launches.push((name.to_owned(), env, harness, launch));
                launches.len() - 1
            }),
        };
        picked.push(index);
    }
    let handshakes = if setup.harnesses {
        futures::future::join_all(launches.iter().map(|(.., launch)| harness::handshake(launch))).await
    } else {
        Vec::new()
    };
    let mut checks = Vec::new();
    for (ant, index) in ants.iter().zip(picked) {
        checks.push(check_ant_creds(ant, now));
        if let Some(path) = &ant.creds {
            checks.extend(check_key(&format!("ant {} key", ant.name), path, vec![issue(ant)], keystore::os()));
        }
        let check = format!("ant {} harness", ant.name);
        let (handshake, installed) = match index {
            _ if !setup.harnesses => {
                checks.push(Check::skip(check, "--no-harness"));
                continue;
            }
            Some(index) => (&handshakes[index], &launches[index].2),
            None => {
                checks.push(Check::fail(check, "unknown harness", vec![format!("correct {config}")]));
                continue;
            }
        };
        let harness = &handshake.harness;
        let recheck = format!("ants harness check --harness {harness} --config {config}");
        checks.push(match &handshake.readiness {
            Readiness::Ready { agent } => Check::ok(check, format!("{harness}: {agent}, signed in")),
            Readiness::Missing => {
                let command = installed.command().display();
                let detail = format!("{harness}: {command} is not installed (not found on PATH)");
                let file = installed.file().map_or_else(|| config.to_string(), |file| file.display().to_string());
                let fix = format!("install {command}, or set the `command` of [harnesses.{harness}] in {file}");
                Check::fail(check, detail, vec![fix])
            }
            Readiness::SignedOut { detail } => {
                let sign_in = handshake.sign_in.clone().unwrap_or_else(|| format!("sign {harness} in"));
                Check::fail(check, format!("{harness} is signed out: {detail}"), vec![sign_in, recheck])
            }
            Readiness::Failed { detail } => {
                Check::fail(check, format!("{harness} ACP handshake: {detail}"), vec![recheck])
            }
        });
    }
    checks
}

/// The commands issuing the credentials of `ant` again.
fn issue(ant: &Ant) -> String {
    let name = &ant.name;
    let domain = ant.js_domain.as_ref().map_or_default(|d| format!(" --js-domain {d}"));
    format!("ants auth user {name}{domain} on the machine holding the team keys, or ants add {name}")
}

/// An ant's credentials: an ant's of its name, issued for the JetStream domain it uses.
fn check_ant_creds(ant: &Ant, now: u64) -> Check {
    let check = format!("ant {} credentials", ant.name);
    let name = &ant.name;
    let issue = issue(ant);
    let Some(path) = &ant.creds else {
        return Check::skip(check, "none in the config file: the ant connects as no one");
    };
    let finding = match creds::check(path) {
        Ok(finding) => finding,
        Err(e) => return Check::fail(check, format!("{e:#}"), vec![issue]),
    };
    if finding.kind != Kind::Ant || finding.name != name.as_str() {
        let detail = format!("{} are the credentials of {}, not of ant {name}", path.display(), finding.holder());
        return Check::fail(check, detail, vec![issue]);
    }
    if let Some(used) = &ant.js_domain
        && finding.js_domain.as_ref() != Some(used)
    {
        let detail = format!("{} were not issued for JetStream domain {used}, which the ant uses", path.display());
        return Check::fail(check, detail, vec![issue]);
    }
    check_creds(&check, path, &finding, now)
}

/// A warning for each ant whose threads run at once in one git work tree, worktrees turned off, where they may
/// clobber each other's edits.
fn check_shared_workdirs(ants: &[Ant], config: &Path) -> Vec<Check> {
    ants.iter()
        .filter(|ant| ants_daemon::worktrees::shares_git_workdir(&ant.workdir, ant.max_harnesses, ant.worktrees))
        .map(|ant| {
            let detail = format!(
                "up to {} threads at once edit the git work tree of {}",
                ant.max_harnesses,
                ant.workdir.display()
            );
            let fix = format!(
                "worktrees = true in the [[ant]] named {} of {}, giving each thread its own git worktree",
                ant.name,
                config.display()
            );
            Check::warn(format!("ant {} workdir", ant.name), detail, vec![fix])
        })
        .collect()
}

/// Whether something answers each ant's reactions requests, which its MCP list_reactions and react make, and which
/// kind of process does: asked as the ant, where its daemon connects, about the nil id, which no ant reads, so the
/// answers are unknown and nothing changes. An ant without credentials, on an open server, asks no one: its daemon
/// answers them itself.
async fn check_reactions(ants: &[Ant], setup: &Setup, wait: Duration) -> Vec<Check> {
    futures::future::join_all(ants.iter().map(|ant| check_ant_reactions(ant, setup, wait))).await
}

async fn check_ant_reactions(ant: &Ant, setup: &Setup, wait: Duration) -> Check {
    let check = format!("ant {} reactions", ant.name);
    let Some(creds) = &ant.creds else {
        return Check::ok(
            check,
            "no credentials, so an open server: the daemon answers list_reactions and react itself",
        );
    };
    let url = ant.nats.as_deref().unwrap_or(&setup.nats);
    let client = match connect(url, Some(creds), &ant.name).await {
        Ok(client) => client,
        Err(Unreached::Refused(e) | Unreached::Unreachable(e) | Unreached::NoKey(e) | Unreached::Other(e)) => {
            return Check::skip(check, format!("not connected to {url} as {}: {e}", ant.name));
        }
    };
    let ask = async |request: ReactionsRequest| reactions::request_within(&client, &request, wait).await;
    let remove = ReactionChange { reaction: "eyes".parse().expect("a reaction"), remove: true };
    let (listed, changed) = tokio::join!(
        ask(ReactionsRequest::new(ant.name.clone(), Uuid::nil())),
        ask(ReactionsRequest::change(ant.name.clone(), Uuid::nil(), remove)),
    );
    let (listed, changed) = match (listed, changed) {
        (Ok(listed), Ok(changed)) => (listed, changed),
        (Err(e), _) | (_, Err(e)) => return Check::skip(check, format!("asking as {}: {e:#}", ant.name)),
    };
    let within = format!("within {}s", wait.as_secs());
    let signer = "ants signer on the machine holding the account key (scripts/install-units.sh --signer runs it as a \
                  service)"
        .to_owned();
    match (listed, changed) {
        (listed, Some(changed)) => {
            let mut by = vec![responder(changed.responder)];
            by.extend(listed.map(|l| responder(l.responder)).filter(|l| !by.contains(l)));
            Check::ok(check, format!("{} answers list_reactions and react", by.join(" and ")))
        }
        (Some(listed), None) => {
            let detail = format!(
                "{} answers list_reactions, but nothing answers react {within}: the ant's react fails",
                responder(listed.responder)
            );
            Check::warn(check, detail, vec![signer])
        }
        (None, None) => {
            let detail = format!("nothing answers its reactions requests {within}: list_reactions and react fail");
            let webhook = "or, for list_reactions only, ants webhook serve with a [[human]] entry".to_owned();
            Check::warn(check, detail, vec![signer, webhook])
        }
    }
}

/// The kind of process that answered a reactions request, as a user-facing name.
fn responder(responder: Option<ReactionsResponder>) -> &'static str {
    match responder {
        Some(ReactionsResponder::Signer) => "ants signer",
        Some(ReactionsResponder::Webhook) => "ants webhook serve",
        None => "an older ants",
    }
}

/// A warning for each ant whose harness runs its tools unasked while the ant gates them: with allow rules or tool
/// approvals by its `escalate_to` ants, or as the destination of schedules or webhooks narrowing the threads they
/// start.
fn check_bypassed(loaded: &config::Loaded, webhooks: &[webhook::Webhook]) -> Vec<Check> {
    let mut checks = Vec::new();
    for ant in &loaded.ants {
        let Some(harness) = ant.harness.as_deref().and_then(|name| loaded.harnesses.get(name).ok()) else { continue };
        if !harness.bypasses_permissions() {
            continue;
        }
        let check = format!("ant {} tool permissions", ant.name);
        let fix = vec![harness.gated_fix()];
        let allow = if ant.allow_tools { vec!["*".to_owned()] } else { ant.allow.clone() };
        if let Some(detail) = harness.unenforced(&allow, !ant.escalate_to.is_empty()) {
            checks.push(Check::warn(check.clone(), detail, fix.clone()));
        }
        let to_ant = |destination: &ants_proto::Destination| matches!(destination, ants_proto::Destination::Ant(to) if *to == ant.name);
        let narrowing: Vec<String> = loaded
            .schedules
            .iter()
            .filter(|s| to_ant(&s.destination) && s.allow.as_deref().is_none_or(|a| !harness::allows_all(a)))
            .map(|s| format!("schedule {}", s.name))
            .chain(
                webhooks
                    .iter()
                    .filter(|w| to_ant(&w.destination) && w.narrows())
                    .map(|w| format!("webhook {}", w.name)),
            )
            .collect();
        if !narrowing.is_empty() {
            let detail = format!(
                "harness {harness} runs its tools unasked, so the tool rules {} narrow its threads to do not apply",
                narrowing.join(", ")
            );
            checks.push(Check::warn(check, detail, fix));
        }
    }
    checks
}

/// The machine's leaf node, as `ants leaf config` wrote its configuration.
struct Leaf {
    path: PathBuf,
    text: String,
    machine: Machine,
    uplink: PathBuf,
    listen: String,
    /// The hub's JetStream domain, as the uplink credentials carry it.
    hub_domain: Option<JsDomain>,
}

impl Leaf {
    /// None without a configuration at `path`.
    fn read(path: &Path) -> Option<anyhow::Result<Self>> {
        if !path.exists() {
            return None;
        }
        Some((|| {
            let text = std::fs::read_to_string(path)?;
            let (machine, uplink) = leaf::read_uplink(path)?;
            let listen = leaf::read_listen(path)?;
            let hub_domain = creds::check(&uplink).ok().and_then(|f| f.js_domain);
            Ok(Self { path: path.to_owned(), text, machine, uplink, listen, hub_domain })
        })())
    }
}

/// The leaf node's system account credentials, which carry the revocations pushed to the hub.
fn check_leaf_system(
    setup: &Setup,
    leaf: &Leaf,
    uplink: &anyhow::Result<creds::Finding>,
    generic: String,
    now: u64,
) -> Check {
    let path = setup.leaf_config.display();
    let machine = &leaf.machine;
    let system = auth::leaf_system_creds(&leaf.uplink);
    let rewrite = format!("ants leaf config {machine} --uplink URL ... (picks up {})", system.display());
    if !system.is_file() {
        let detail = format!(
            "no system account credentials at {}: revocations pushed to the hub do not reach this leaf",
            system.display()
        );
        let issue = uplink.as_ref().ok().and_then(|f| f.renew().into_iter().next()).unwrap_or(generic);
        Check::warn("leaf system uplink", detail, vec![issue, rewrite])
    } else if !leaf.text.contains(&*system.to_string_lossy()) {
        let detail =
            format!("{path} does not use {}: revocations pushed to the hub do not reach this leaf", system.display());
        Check::warn("leaf system uplink", detail, vec![rewrite])
    } else {
        match creds::check(&system) {
            Ok(finding) if finding.kind == (Kind::LeafSystem { machine: machine.clone() }) => {
                check_creds("leaf system uplink", &system, &finding, now)
            }
            Ok(finding) => Check::fail(
                "leaf system uplink",
                format!("{} are the credentials of {}", system.display(), finding.holder()),
                vec![generic],
            ),
            Err(e) => Check::fail("leaf system uplink", format!("{e:#}"), vec![generic]),
        }
    }
}

/// The leaf node: its configuration, uplink and system account credentials, whether it listens and the CLI connects
/// to it, and whether the hub takes from its outbox what the machine's ants and humans send.
async fn check_leaf(
    setup: &Setup,
    leaf: Option<anyhow::Result<Leaf>>,
    ants: &[Ant],
    human: Option<&Me>,
    connected: Option<&Connected>,
    now: u64,
) -> Vec<Check> {
    let path = setup.leaf_config.display();
    let leaf = match leaf {
        None => return vec![Check::skip("leaf", format!("no leaf node configuration at {path}"))],
        Some(Err(e)) => {
            let fix = "ants leaf config MACHINE --uplink URL".to_owned();
            return vec![Check::fail("leaf", format!("{e:#}"), vec![fix])];
        }
        Some(Ok(leaf)) => leaf,
    };
    let machine = &leaf.machine;
    let mut checks = vec![Check::ok("leaf", format!("{path}: machine {machine}, listening on {}", leaf.listen))];

    let uplink = creds::check(&leaf.uplink);
    let generic =
        format!("ants auth leaf {machine} --ant ANT... --human HUMAN..., then ants leaf config {machine} ...");
    let mut members = None;
    checks.push(match &uplink {
        Err(e) => Check::fail("leaf uplink", format!("{e:#}"), vec![generic.clone()]),
        Ok(finding) => match &finding.kind {
            Kind::Leaf { machine: m, ants: carried, humans, .. } if m == machine => {
                // The ants and the human connecting through the leaf node, the daemon's default server.
                let through = |url: Option<&str>| same_address(url.unwrap_or(leaf::DEFAULT_LISTEN), &leaf.listen);
                let missing: Vec<&UserId> = ants
                    .iter()
                    .filter(|ant| through(ant.nats.as_deref()))
                    .map(|ant| &ant.name)
                    .chain(human.filter(|_| through(Some(&setup.nats))).map(|me| &me.id))
                    .filter(|user| !carried.contains(user) && !humans.contains(user))
                    .collect();
                members = Some(auth::members(carried, humans));
                if missing.is_empty() {
                    check_creds("leaf uplink", &leaf.uplink, finding, now)
                } else {
                    let mut renewed = finding.clone();
                    if let Kind::Leaf { ants, humans, .. } = &mut renewed.kind {
                        for user in &missing {
                            if human.is_some_and(|me| &me.id == *user) {
                                humans.push((*user).clone());
                            } else {
                                ants.push((*user).clone());
                            }
                        }
                    }
                    let names: Vec<&str> = missing.iter().map(|u| u.as_str()).collect();
                    let detail = format!(
                        "{} carry no rights for {} of this machine: what they send does not cross the uplink",
                        leaf.uplink.display(),
                        names.join(", ")
                    );
                    Check::fail("leaf uplink", detail, renewed.renew())
                }
            }
            _ => {
                let detail = format!(
                    "{} are the credentials of {}, not the uplink of {machine}",
                    leaf.uplink.display(),
                    finding.holder()
                );
                Check::fail("leaf uplink", detail, vec![generic.clone()])
            }
        },
    });

    checks.push(check_leaf_system(setup, &leaf, &uplink, generic, now));

    let listening = tokio::time::timeout(NETWORK_TIMEOUT, tokio::net::TcpStream::connect(leaf.listen.as_str())).await;
    checks.push(match listening {
        Ok(Ok(_)) if same_address(&setup.nats, &leaf.listen) => {
            Check::ok("leaf node", format!("listening on {}, where the CLI connects", leaf.listen))
        }
        Ok(Ok(_)) => Check::warn(
            "leaf node",
            format!("listening on {}, but the CLI connects to {}", leaf.listen, setup.nats),
            vec![format!("nats = \"nats://{}\" in [cli] of {}", leaf.listen, setup.config.display())],
        ),
        failed => {
            let error = match failed {
                Ok(Err(e)) => e.to_string(),
                _ => format!("no answer within {}s", NETWORK_TIMEOUT.as_secs()),
            };
            let start = match Services::local().state(Service::Nats, &leaf.path) {
                State::Stopped(start) => start,
                _ => format!("nats-server -c {path}, or scripts/install-units.sh to run it as a service"),
            };
            Check::fail("leaf node", format!("nothing listens on {}: {error}", leaf.listen), vec![start])
        }
    });

    checks.push(check_outbox(machine, members.as_deref(), uplink.as_ref().ok(), connected).await);
    checks
}

/// Whether the hub sources the outbox of `machine` and takes from it what `members` send ([`outbox::sourcing`]).
async fn check_outbox(
    machine: &Machine,
    members: Option<&[(UserId, ants_proto::Role)]>,
    uplink: Option<&creds::Finding>,
    connected: Option<&Connected>,
) -> Check {
    let check = "outbox";
    let Some(connected) = connected else { return Check::skip(check, "the hub is not reached") };
    let Some(members) = members.filter(|m| !m.is_empty()) else {
        return Check::skip(check, "the uplink credentials name no ants or humans of the machine");
    };
    let rights = &connected.rights;
    let infos = [format!("$JS.API.STREAM.INFO.{INTAKE_STREAM}"), format!("$JS.API.STREAM.INFO.{STREAM}")];
    if !infos.iter().all(|s| rights.may_publish(s)) {
        return Check::skip(check, "only a human's credentials get the stream info telling it");
    }
    let admit = uplink
        .and_then(|f| f.renew().into_iter().nth(1))
        .unwrap_or_else(|| format!("ants leaf outbox {machine} --ant ANT... --human HUMAN..."));
    let sourcing = tokio::time::timeout(NETWORK_TIMEOUT * 2, outbox::sourcing(&connected.js, machine, members)).await;
    match sourcing {
        Err(_) => Check::warn(check, "no stream info within 10s", vec![admit]),
        Ok(Err(e)) => Check::warn(check, format!("{e:#}"), vec![admit]),
        Ok(Ok(sourcing)) => match (sourcing.hearing, sourcing.unadmitted.as_slice()) {
            (outbox::Hearing::Failed(e), _) => {
                Check::warn(check, format!("the hub does not source the outbox of {machine}: {e}"), vec![admit])
            }
            (_, [_, ..]) => {
                let names: Vec<&str> = sourcing.unadmitted.iter().map(UserId::as_str).collect();
                let detail = format!(
                    "the hub's stream takes nothing from the outbox of {machine} for {}: what they send while the \
                     uplink is down stays there",
                    names.join(", ")
                );
                Check::warn(check, detail, vec![admit])
            }
            (outbox::Hearing::Silent, []) => Check::warn(
                check,
                format!("the hub sources the outbox of {machine} but has not heard from it"),
                vec![
                    "feature_flags { js_api_reply_v2: true } in the hub's nats-server configuration, then restart it"
                        .to_owned(),
                ],
            ),
            (outbox::Hearing::Heard, []) => {
                let names: Vec<&str> = members.iter().map(|(user, _)| user.as_str()).collect();
                Check::ok(check, format!("the hub takes from the outbox of {machine} for {}", names.join(", ")))
            }
        },
    }
}

/// The daemon service of the config file, the nats-server service of the leaf node configuration, and that of the
/// local hub's configuration when there is no leaf node one, as scripts/install-units.sh installs them.
fn check_services(setup: &Setup, services: &mut Services) -> Vec<Check> {
    let daemon = ("daemon service", Service::Daemon, &setup.config, "--config");
    let nats = ("leaf node service", Service::Nats, &setup.leaf_config, "--nats-conf");
    // `ants setup` runs a local hub only on a machine with no leaf node, from the same unit.
    let hub = ("hub service", Service::Nats, &setup.hub_config, "--nats-conf");
    let hub = (setup.hub_config.exists() && !setup.leaf_config.exists()).then_some(hub);
    // Only a machine with the webhooks' config file serves them.
    let webhook = ("webhook service", Service::Webhook, &setup.webhooks, "--webhook --webhook-config");
    let webhook = setup.webhooks.exists().then_some(webhook);
    [Some(daemon), Some(nats), hub, webhook]
        .into_iter()
        .flatten()
        .map(|(check, service, file, flag)| {
            let install = format!("scripts/install-units.sh {flag} {}", file.display());
            let file_shown = file.display();
            if !file.exists() {
                return Check::skip(check, format!("no {file_shown}"));
            }
            match services.state(service, file) {
                State::Unsupported => Check::skip(check, "no service manager ants installs units for here"),
                State::NotInstalled => Check::warn(check, format!("none installed to run {file_shown}"), vec![install]),
                State::Other(unit) => {
                    Check::warn(check, format!("{} runs another file than {file_shown}", unit.display()), vec![install])
                }
                State::Stopped(start) => {
                    Check::fail(check, format!("installed for {file_shown}, not running"), vec![start])
                }
                State::Running => Check::ok(check, format!("running {file_shown}")),
                State::Unknown(command, e) => Check::warn(check, format!("{command}: {e}"), vec![install]),
            }
        })
        .collect()
}

/// The webhooks' config file at `path`, when it is there: its secret files, present and private to their owner, the
/// file itself as `ants webhook serve` reads it, the programs of its exec sinks, and whether `public_url` reaches it.
async fn check_webhooks(path: &Path) -> Vec<Check> {
    if !path.exists() {
        return Vec::new();
    }
    let shown = path.display();
    let refused = || format!("correct {shown}; `ants webhook serve --config {shown}` refuses it as it is");
    let outline = match webhook::outline(path) {
        Ok(outline) => outline,
        Err(e) => return vec![Check::fail("webhooks", format!("{e:#}"), vec![refused()])],
    };
    let mut checks = Vec::new();
    let mut unset = Vec::new();
    for (what, source) in &outline.secrets {
        match source {
            SecretSource::File(file) => {
                let fix = format!("write the secret into {} (mode 0600)", file.display());
                match private_file(file, fix) {
                    Ok(_) => {}
                    Err(problem) => checks.push(problem.named("webhook secrets", what, Status::Fail)),
                }
            }
            SecretSource::Env(var) if std::env::var_os(var).is_none() => unset.push((what, var)),
            SecretSource::Env(_) => {}
        }
    }
    let files = outline.secrets.iter().filter(|(_, s)| matches!(s, SecretSource::File(_))).count();
    if checks.is_empty() && files > 0 {
        checks.push(Check::ok("webhook secrets", format!("{files} secret files of {shown}, each with mode 0600")));
    }
    for (what, var) in &unset {
        let detail = format!("{what}: {var} is not set here, only the environment of `ants webhook serve` may hold it");
        checks.push(Check::warn("webhook secrets", detail, vec![format!("export {var}, or name a secret_file")]));
    }
    if checks.iter().any(|c| c.status != Status::Ok) {
        checks.push(Check::skip("webhooks", format!("{shown} not read: its secrets are not all readable here")));
    } else {
        checks.push(match webhook::Config::read(path) {
            Ok(config) => {
                let humans: Vec<&str> = config.humans.iter().map(|h| h.name.as_str()).collect();
                let hooks = config.webhooks.len();
                match humans.as_slice() {
                    [] => Check::ok("webhooks", format!("{shown} lists {hooks} webhooks and no human to notify")),
                    _ => Check::ok(
                        "webhooks",
                        format!(
                            "{shown} lists {hooks} webhooks and notifies {}; `ants webhook notify-test HUMAN` tries \
                             each sink",
                            humans.join(", ")
                        ),
                    ),
                }
            }
            Err(e) => Check::fail("webhooks", format!("{e:#}"), vec![refused()]),
        });
    }
    checks.extend(outline.commands.iter().map(|(what, program)| check_command(what, program)));
    if let Some(public_url) = &outline.public_url {
        checks.push(check_public_url(public_url).await);
    }
    checks
}

/// Whether the program of an exec sink is there to run: an executable file at its path, or one on `PATH` here.
fn check_command(what: &str, program: &str) -> Check {
    let check = "notify commands";
    let executable = |path: &Path| {
        let Ok(metadata) = std::fs::metadata(path) else { return false };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        metadata.is_file()
    };
    if program.contains(std::path::is_separator) {
        return if executable(Path::new(program)) {
            Check::ok(check, format!("{what}: {program} is executable"))
        } else {
            Check::fail(
                check,
                format!("{what}: {program} is no executable file"),
                vec![format!("put the program at {program} and chmod +x it, or correct command")],
            )
        };
    }
    let path = std::env::var_os("PATH").unwrap_or_default();
    match std::env::split_paths(&path).map(|dir| dir.join(program)).find(|p| executable(p)) {
        Some(found) => Check::ok(check, format!("{what}: {program} is {} here", found.display())),
        None => Check::warn(
            check,
            format!("{what}: {program} is not on PATH here"),
            vec![format!("give the full path of {program} in command")],
        ),
    }
}

/// Whether `public_url`, the approval links' address, reaches `ants webhook serve`: its page for a link without a
/// query.
async fn check_public_url(public_url: &str) -> Check {
    let check = "public_url";
    let url = format!("{}{}", public_url.trim_end_matches('/'), notify::APPROVAL_PATH);
    let fix = || {
        vec![format!("route {public_url} to `ants webhook serve` (its listen address and path), or correct public_url")]
    };
    match notify::probe_links(public_url, NETWORK_TIMEOUT).await {
        Ok((StatusCode::BAD_REQUEST, page)) if page.contains("The link is incomplete.") => {
            Check::ok(check, format!("{url} answers as `ants webhook serve` does"))
        }
        Ok((status, _)) => {
            Check::warn(check, format!("{url} answered {status}, not as `ants webhook serve` does"), fix())
        }
        Err(e) => Check::warn(check, format!("{url}: {e:#}; approval links from notices may not open"), fix()),
    }
}

/// Why a secret file does not do: its check's detail and fix, and how bad it is.
struct Problem {
    detail: String,
    fix: String,
    /// The file is there, readable by others.
    exposed: bool,
}

impl Problem {
    /// The check `check` of the file holding `what`: `exposed` warns or fails as `status` says.
    fn named(self, check: &str, what: &str, exposed: Status) -> Check {
        let status = if self.exposed { exposed } else { Status::Fail };
        Check::new(check, status, format!("{what}: {}", self.detail), vec![self.fix])
    }
}

/// The modification time of `file`, a regular file only its owner may read; else what to do about it, `write`
/// when it is missing.
fn private_file(file: &Path, write: String) -> Result<std::fs::Metadata, Problem> {
    let shown = file.display();
    let metadata = match std::fs::metadata(file) {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) => return Err(Problem { detail: format!("{shown} is not a file"), fix: write, exposed: false }),
        Err(e) => return Err(Problem { detail: format!("{shown}: {e}"), fix: write, exposed: false }),
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            let detail = format!("{shown} has mode {mode:04o}, readable by others");
            return Err(Problem { detail, fix: format!("chmod 600 {shown}"), exposed: true });
        }
    }
    Ok(metadata)
}

/// A Dev Tunnel or HTTPS bridge service installed, read before any check waits.
struct Transport {
    service: Service,
    state: State,
    unit: Unit,
    restart: String,
}

/// The Dev Tunnel and HTTPS bridge services installed.
fn installed_transports(services: &mut Services) -> Vec<Transport> {
    [Service::DevTunnel, Service::DevTunnelHost, Service::Bridge, Service::BridgeServe]
        .into_iter()
        .filter_map(|service| {
            let unit = services.unit(service)?;
            let restart = services.by_hand(service, Action::Restart)?;
            Some(Transport { service, state: services.status(service), unit, restart })
        })
        .collect()
}

/// When a Dev Tunnel login has this little left, warn.
const LOGIN_WARN: Duration = Duration::from_hours(24);
/// When a Dev Tunnel connect token has this little left, warn.
const TOKEN_WARN: Duration = Duration::from_hours(6);
/// How long a Dev Tunnel connect token lasts, for a token file whose expiry cannot be read.
const TOKEN_LIFE: Duration = Duration::from_hours(24);

/// The Dev Tunnel and HTTPS bridge services `transports`: whether each runs, the Dev Tunnel login or connect token,
/// and whether each bridge answers and takes its token. Nothing without such services.
async fn check_transports(transports: Vec<Transport>, now: u64) -> Vec<Check> {
    let mut checks = Vec::new();
    let mut login = None;
    for Transport { service, state, unit, restart } in &transports {
        let (check, what) = match service {
            Service::DevTunnel => ("devtunnel service", "devtunnel connect"),
            Service::DevTunnelHost => ("devtunnel host service", "devtunnel host"),
            Service::Bridge => ("bridge service", "ants bridge connect"),
            _ => ("bridge serve service", "ants bridge serve"),
        };
        checks.push(match state {
            State::Running => Check::ok(check, format!("running `{what}`")),
            State::Stopped(start) => {
                Check::fail(check, format!("`{what}` installed, not running"), vec![start.clone()])
            }
            State::Unknown(command, e) => Check::warn(check, format!("{command}: {e}"), Vec::new()),
            _ => Check::skip(check, "not installed"),
        });
        match service {
            Service::DevTunnel | Service::DevTunnelHost => {
                let program = unit.args.first().map_or_default(PathBuf::from);
                if !program.is_file() {
                    let detail = format!("no devtunnel at {}", program.display());
                    let fix = "scripts/install-units.sh again with --devtunnel-bin PATH".to_owned();
                    checks.push(Check::fail("devtunnel", detail, vec![fix]));
                    continue;
                }
                let tunnel = unit.args.get(2).map_or("TUNNEL", String::as_str);
                match (&unit.stdin, *service) {
                    (Some(token), Service::DevTunnel) => {
                        checks.push(check_devtunnel_token(token, tunnel, restart, now));
                    }
                    _ => {
                        login.get_or_insert((program, restart.clone()));
                    }
                }
            }
            _ => checks.extend(check_bridge(*service, unit, restart).await),
        }
    }
    if let Some((program, restart)) = login {
        checks.push(check_devtunnel_login(&program, &restart, now).await);
    }
    checks
}

/// The Dev Tunnel login of `devtunnel user show`, run with `program`: there, and not expiring within
/// [`LOGIN_WARN`].
async fn check_devtunnel_login(program: &Path, restart: &str, now: u64) -> Check {
    let check = "devtunnel login";
    let fix =
        || vec![format!("devtunnel user login -g -d (GitHub, device code; -e for Microsoft Entra), then {restart}")];
    let mut command = tokio::process::Command::new(program);
    command.args(["user", "show", "--json"]).stdin(std::process::Stdio::null()).kill_on_drop(true);
    let output = match tokio::time::timeout(NETWORK_TIMEOUT, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => return Check::fail(check, format!("running {}: {e}", program.display()), fix()),
        Err(_) => {
            let detail = format!("`devtunnel user show` gave no answer within {}s", NETWORK_TIMEOUT.as_secs());
            return Check::warn(check, detail, Vec::new());
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let shown: serde_json::Value = match serde_json::from_str(stdout.trim()) {
        Ok(shown) => shown,
        Err(_) => {
            let said = if stdout.trim().is_empty() { String::from_utf8_lossy(&output.stderr) } else { stdout };
            let detail = format!("`devtunnel user show` said: {}", said.trim());
            return Check::warn(check, detail, fix());
        }
    };
    let status = shown.get("status").and_then(|s| s.as_str()).unwrap_or_default();
    if !status.eq_ignore_ascii_case("logged in") {
        let status = if status.is_empty() { "no login" } else { status };
        return Check::fail(check, format!("{status}: the tunnel goes down at its next connection"), fix());
    }
    let user = ["username", "userName", "user", "name"]
        .iter()
        .find_map(|key| shown.get(key).and_then(|v| v.as_str()))
        .map_or(String::new(), |user| format!(" as {user}"));
    match expiry(&shown) {
        Some(expires) if expires <= now => Check::fail(check, format!("logged in{user}, expired"), fix()),
        Some(expires) if expires - now < LOGIN_WARN.as_secs() => {
            Check::warn(check, format!("logged in{user}, expiring in {}", left(expires - now)), fix())
        }
        Some(expires) => Check::ok(check, format!("logged in{user} for {}", left(expires - now))),
        None => Check::ok(check, format!("logged in{user}; a login lasts some days, then devtunnel user login again")),
    }
}

/// The connect token of `tunnel` in `file`, read by the devtunnel service at each start: there, private to its owner
/// and not expiring within [`TOKEN_WARN`] (its `exp`, or [`TOKEN_LIFE`] after it was written).
fn check_devtunnel_token(file: &Path, tunnel: &str, restart: &str, now: u64) -> Check {
    let check = "devtunnel token";
    let shown = file.display();
    let renew =
        format!("devtunnel token {tunnel} --scopes connect, its token into {shown} (mode 0600), then {restart}");
    let metadata = match private_file(file, renew.clone()) {
        Ok(metadata) => metadata,
        Err(problem) => return problem.named(check, "the connect token", Status::Warn),
    };
    let token = std::fs::read_to_string(file).unwrap_or_default();
    let (expires, said) = match jwt_expiry(token.trim()) {
        Some(expires) => (expires, format!("expiring in {}", left(expires.saturating_sub(now)))),
        None => {
            let written = metadata.modified().ok().and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok());
            let written = written.map_or(0, |t| t.as_secs());
            let age = left(now.saturating_sub(written));
            (written + TOKEN_LIFE.as_secs(), format!("written {age} ago, and a connect token lasts a day"))
        }
    };
    match expires.saturating_sub(now) {
        0 => Check::fail(check, format!("{shown}: expired"), vec![renew]),
        rest if rest < TOKEN_WARN.as_secs() => Check::warn(check, format!("{shown}: {said}"), vec![renew]),
        _ => Check::ok(check, format!("{shown}: {said}")),
    }
}

/// The `exp` of a JSON Web Token.
fn jwt_expiry(token: &str) -> Option<u64> {
    let mut parts = token.split('.');
    let (_, payload, _) = (parts.next()?, parts.next()?, parts.next()?);
    let payload = data_encoding::BASE64URL_NOPAD.decode(payload.trim_end_matches('=').as_bytes()).ok()?;
    serde_json::from_slice::<serde_json::Value>(&payload).ok()?.get("exp")?.as_u64()
}

/// The time, as seconds since the epoch, of a field of `value` whose name speaks of expiry.
fn expiry(value: &serde_json::Value) -> Option<u64> {
    let object = value.as_object()?;
    object.iter().find_map(|(key, value)| match value {
        serde_json::Value::Object(_) => expiry(value),
        _ if !key.to_ascii_lowercase().contains("expir") => None,
        serde_json::Value::String(text) => {
            chrono::DateTime::parse_from_rfc3339(text).ok().and_then(|t| u64::try_from(t.timestamp()).ok())
        }
        serde_json::Value::Number(n) => n.as_u64().map(|t| if t > 100_000_000_000 { t / 1000 } else { t }),
        _ => None,
    })
}

/// `seconds`, rounded down, as minutes, hours or days.
fn left(seconds: u64) -> String {
    match seconds / 60 {
        minutes @ 0..60 => format!("{minutes}m"),
        minutes @ 60..2880 => format!("{}h", minutes / 60),
        minutes => format!("{} days", minutes / 1440),
    }
}

/// The bridge of `service`, `ants bridge connect` or `ants bridge serve` run as `unit`: its token file, and whether
/// the bridge answers as one taking it.
async fn check_bridge(service: Service, unit: &Unit, restart: &str) -> Vec<Check> {
    let serving = service == Service::BridgeServe;
    let (token_check, url_check) =
        if serving { ("bridge serve token", "bridge serve url") } else { ("bridge token", "bridge url") };
    let mut checks = Vec::new();
    let token = match unit.flag("--token-file") {
        Some(file) => {
            let file = Path::new(file);
            let write = if serving {
                format!("write a long random token into {} (mode 0600), then {restart}", file.display())
            } else {
                format!(
                    "the token of the hub's `ants bridge serve` into {} (mode 0600), then {restart}",
                    file.display()
                )
            };
            match private_file(file, write) {
                Ok(_) => checks.push(Check::ok(token_check, format!("{}, mode 0600", file.display()))),
                Err(problem) => checks.push(problem.named(token_check, "the bridge token", Status::Warn)),
            }
            bridge::token(None, Some(file)).ok().flatten()
        }
        None if serving => {
            let detail = "none: anyone reaching the bridge may carry a connection to the hub";
            checks.push(Check::warn(
                token_check,
                detail,
                vec!["scripts/install-units.sh with --bridge-token FILE".to_owned()],
            ));
            None
        }
        None => None,
    };
    let url = if serving {
        format!("http://{}", unit.flag("--listen").unwrap_or(bridge::DEFAULT_SERVE_LISTEN))
    } else {
        match bridge_url(&unit.args) {
            Some(url) => url.to_owned(),
            None => {
                checks.push(Check::fail(url_check, "the service names no bridge URL", Vec::new()));
                return checks;
            }
        }
    };
    let remote = bridge::Remote::new(&url, token.as_deref(), &[], unit.flag("--proxy"));
    let remote = match (remote, unit.env("SSL_CERT_FILE")) {
        (Ok(remote), Some(ca)) => remote.trusting(Path::new(ca)),
        (remote, _) => remote,
    };
    let probed = match remote {
        Ok(remote) => remote.probe(NETWORK_TIMEOUT).await,
        Err(e) => Err(e),
    };
    let unreachable = if serving {
        restart.to_owned()
    } else {
        format!("check that the hub's `ants bridge serve` runs and is reached at {url}, through the proxy if any")
    };
    checks.push(match probed {
        Ok(Probe::Answers) => Check::ok(url_check, format!("{url} answers as a bridge taking the token")),
        Ok(Probe::Refused(answer)) => {
            let fix = match unit.flag("--token-file") {
                Some(file) => {
                    format!("the same token in {file} as the hub's `ants bridge serve` takes, then {restart}")
                }
                None => "scripts/install-units.sh with --bridge-token FILE holding the hub's bridge token".to_owned(),
            };
            Check::fail(url_check, format!("{url} refuses the token: {answer}"), vec![fix])
        }
        Ok(Probe::Other(status, answer)) => Check::fail(
            url_check,
            format!("{url} answered {status} {answer:?}, not as a bridge does"),
            vec![unreachable],
        ),
        Err(e) => Check::fail(url_check, format!("{url}: {e:#}"), vec![unreachable]),
    });
    checks
}

/// The URL `ants bridge connect` runs with, among its arguments `args`.
fn bridge_url(args: &[String]) -> Option<&str> {
    let start = args.iter().position(|a| a == "connect")? + 1;
    let mut rest = args[start..].iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "--token-file" | "--token" | "--proxy" | "--header" | "--listen" => {
                rest.next();
            }
            a if a.starts_with('-') => {}
            url => return Some(url),
        }
    }
    None
}

/// Whether the NATS URL `url` names the host and port `listen`, a loopback address.
fn same_address(url: &str, listen: &str) -> bool {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = rest.split(['/', '?', ',']).next().unwrap_or_default();
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(']') || authority.starts_with('[') => (host, port),
        _ => (authority, "4222"),
    };
    let loopback = |h: &str| matches!(h, "localhost" | "127.0.0.1" | "[::1]" | "::1");
    let Some((listen_host, listen_port)) = listen.rsplit_once(':') else { return false };
    port == listen_port && (host == listen_host || loopback(host) && loopback(listen_host))
}

#[cfg(test)]
mod tests;
