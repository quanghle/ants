//! `ants doctor`: check a machine's ants setup end to end, reporting each check with the command that fixes it.
//!
//! It changes nothing: the hub's stream and buckets are compared with what this version expects ([`hub::check`]),
//! not brought up to date as the first connection of other commands does, and only the requests the credentials
//! allow are made, so none just times out. Every network check has a timeout; a harness handshake has its own.

use std::fmt;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::config::{self, Ant};
use ants_daemon::creds::{self, Expiry, Kind, Seed, VersionStatus};
use ants_daemon::harness::{self, DEFAULT_HARNESS, Harnesses, Readiness};
use ants_daemon::keystore::{self, Store};
use ants_daemon::{Launch, PermissionPolicy, bus, hub};
use ants_proto::{INTAKE_STREAM, JsDomain, Machine, PERMISSIONS_VERSION, STREAM, UserId, inbox_consumer};
use async_nats::jetstream;
use serde::Serialize;

use crate::humans::{Me, Rights};
use crate::service::{Service, Services, State};
use crate::{auth, leaf, outbox, revoke, versions};

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
    /// Whether to start each harness for an ACP handshake.
    pub harnesses: bool,
    /// The team directory of `ants auth`, whose system account key, when it is there, asks the hub how it keeps
    /// pushed accounts.
    pub team: PathBuf,
}

/// Run every check, in order: the config file, the CLI's credentials, the server, JetStream, the hub's stream and
/// buckets, the versions of ants running across the team, each ant's credentials, harness and workdir, the hub's
/// account resolver, the leaf node and outbox, and the installed services.
pub async fn run(setup: &Setup) -> Vec<Check> {
    let now = auth::now().unwrap_or_default();
    let mut checks = Vec::new();
    let loaded = check_config(&setup.config, &mut checks);
    let identity = check_cli_creds(setup, now, &mut checks);
    let leaf = Leaf::read(&setup.leaf_config);
    let ants = loaded.as_ref().map(|l| l.ants.as_slice()).unwrap_or_default();
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
    let ((reached, connected), ants_checked, resolver) = tokio::join!(reach, ants_checked, resolver);
    checks.extend(reached);
    checks.extend(ants_checked);
    checks.extend(check_shared_workdirs(ants, &setup.config));
    checks.push(resolver);
    let human = identity.as_ref().map(|i| &i.me).filter(|me| me.role.is_human() && !me.id.is_human());
    checks.extend(check_leaf(setup, leaf, ants, human, connected.as_ref(), now).await);
    checks.extend(check_services(setup, &mut Services::local()));
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
            let fix = format!("correct {}; `ants daemon --config {0}` refuses it as it is", path.display());
            checks.push(Check::fail("config", format!("{e:#}"), vec![fix]));
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
    let version = match finding.version {
        Some(version) => format!("permissions version {version}"),
        None => "no permissions version".to_owned(),
    };
    let at = |exp: Option<u64>| exp.map(creds::time).unwrap_or_default();
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
            problems.push(format!("expire at {}", at(finding.expires_at)))
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
        Err(e) if keystore::is_unavailable(&e) => {
            let fix = "unlock the OS credential store (log in), then check again; services started before then wait                        for it";
            Check::fail(check, format!("{e:#}"), vec![fix.to_owned()])
        }
        Err(e) => {
            let restore = "restore the seed from a backup of the OS credential store, or issue the credentials again:";
            Check::fail(check, format!("{e:#}"), std::iter::once(restore.to_owned()).chain(issue).collect())
        }
    })
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
    let mut checks = vec![Check::ok(
        "server",
        format!(
            "connected to {via}{} (server {}, nats-server {}) as {}",
            setup.nats, info.server_name, info.version, me.id
        ),
    )];
    let mut js = bus::jetstream(client, setup.js_domain.as_ref());
    js.set_timeout(NETWORK_TIMEOUT);
    let answered = check_jetstream(setup, identity, &js, leaf, through_leaf.is_some()).await;
    let reached = answered.status == Status::Ok;
    checks.push(answered);
    if !reached {
        checks.push(Check::skip("hub", "JetStream does not answer"));
        checks.push(Check::skip("consumers", "JetStream does not answer"));
        checks.push(Check::skip("versions", "JetStream does not answer"));
        return (checks, None);
    }
    checks.extend(check_hub(&js, identity).await);
    checks.push(check_consumers(&js, identity).await);
    checks.push(check_versions(&js, identity).await);
    (checks, Some(Connected { js, rights: identity.rights.clone() }))
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
    let hub_domain = issued.or(leaf.and_then(|l| l.hub_domain.as_ref())).map_or("HUB".to_owned(), |d| d.to_string());
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
        let mut detail = format!("stream {STREAM} and {} buckets up to date", checked.saturating_sub(1));
        if !unseen.is_empty() {
            let _ = write!(detail, " ({} not checked: the credentials get no stream info of it)", unseen.join(", "));
        }
        return vec![Check::ok(check, detail)];
    }
    findings
        .iter()
        .map(|finding| {
            let fix = match finding.rights().iter().all(|s| may(s)) {
                true => "ants init".to_owned(),
                false => "ants init with a human's current credentials (`ants auth user NAME --human`)".to_owned(),
            };
            match finding.problem {
                hub::Problem::Unchecked(_) => Check::warn(check, finding.to_string(), vec![fix]),
                _ => Check::fail(check, finding.to_string(), vec![fix]),
            }
        })
        .collect()
}

/// The consumers of the `ANTS` stream against the most it takes ([`consumer_limit`]): past that, creating one fails,
/// so an ant cannot read its inbox. Warns from four fifths of it.
async fn check_consumers(js: &jetstream::Context, identity: &Identity) -> Check {
    let check = "consumers";
    if !identity.rights.may_publish(&hub::Object::Stream.info_subject()) {
        return Check::skip(
            check,
            format!("the credentials get no stream info of {STREAM}: run `ants doctor` as a human"),
        );
    }
    let info = match js.get_stream(STREAM).await {
        Ok(stream) => stream.cached_info().clone(),
        Err(e) => return Check::skip(check, format!("no stream info of {STREAM}: {e}")),
    };
    consumers(info.state.consumer_count, consumer_limit(&info.config))
}

/// The most consumers a stream configured as `config` takes: its `max_consumers`, else the 1000 nats-server 2.15 and
/// later give a stream by default (`default_max_consumers` in its configuration may change that); None for no limit.
fn consumer_limit(config: &jetstream::stream::Config) -> Option<usize> {
    if let Ok(max) = usize::try_from(config.max_consumers)
        && max > 0
    {
        return Some(max);
    }
    let version = config.metadata.get("_nats.ver")?;
    let mut parts = version.split(['.', '-']).map(|part| part.parse::<u32>().unwrap_or(0));
    let (major, minor) = (parts.next()?, parts.next()?);
    ((major, minor) >= (2, 15)).then_some(1000)
}

fn consumers(count: usize, limit: Option<usize>) -> Check {
    let check = "consumers";
    let Some(limit) = limit else {
        return Check::ok(check, format!("{count} on stream {STREAM}, which takes any number"));
    };
    let detail = format!("{count} on stream {STREAM}, which takes {limit}");
    if count.saturating_mul(5) < limit.saturating_mul(4) {
        return Check::ok(check, detail);
    }
    let raise = match usize::try_from(hub::MAX_CONSUMERS).is_ok_and(|max| limit < max) {
        true => "ants init".to_owned(),
        false => format!("nats stream edit {STREAM} --max-consumers {}", limit.saturating_mul(2)),
    };
    Check::warn(check, format!("{detail}: once full, no ant can open a consumer to read its inbox"), vec![raise])
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
                "{} refuses the team's system account ({e:#}): its server.conf is older than revocations or of \
                 another team, and `ants auth push` fails",
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

/// Each ant's credentials, and, unless told not to, whether its harness is installed and signed in: one handshake per
/// harness and environment, all at once.
async fn check_ants(ants: &[Ant], harnesses: &Harnesses, setup: &Setup, now: u64) -> Vec<Check> {
    let config = setup.config.display();
    // The harness's name, the ant's environment, the harness and its launch.
    type Started = (String, Vec<(String, String)>, std::sync::Arc<ants_daemon::Harness>, Launch);
    let mut launches: Vec<Started> = Vec::new();
    let mut picked = Vec::with_capacity(ants.len());
    for ant in ants {
        let name = ant.harness.as_deref().unwrap_or(DEFAULT_HARNESS);
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
    let handshakes = match setup.harnesses {
        true => futures::future::join_all(launches.iter().map(|(.., launch)| harness::handshake(launch))).await,
        false => Vec::new(),
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
                let fix = format!("install {command}, or set the `command` of [harnesses.{harness}] in {config}");
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
    let domain = ant.js_domain.as_ref().map(|d| format!(" --js-domain {d}")).unwrap_or_default();
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

/// A warning for each ant whose threads run at once in one git work tree, where they may clobber each other's edits.
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
                            match human.is_some_and(|me| &me.id == *user) {
                                true => humans.push((*user).clone()),
                                false => ants.push((*user).clone()),
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

    let system = auth::leaf_system_creds(&leaf.uplink);
    let rewrite = format!("ants leaf config {machine} --uplink URL ... (picks up {})", system.display());
    checks.push(if !system.is_file() {
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
    });

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
                ["feature_flags { js_api_reply_v2: true } in the hub's nats-server configuration, then restart it"
                    .to_owned()]
                .into_iter()
                .chain(
                    uplink
                        .and_then(|f| f.renew().into_iter().next())
                        .map(|c| format!("{c}, for uplink credentials issued before store and forward")),
                )
                .collect(),
            ),
            (outbox::Hearing::Heard, []) => {
                let names: Vec<&str> = members.iter().map(|(user, _)| user.as_str()).collect();
                Check::ok(check, format!("the hub takes from the outbox of {machine} for {}", names.join(", ")))
            }
        },
    }
}

/// The daemon service of the config file and the nats-server service of the leaf node configuration, as
/// scripts/install-units.sh installs them.
fn check_services(setup: &Setup, services: &mut Services) -> Vec<Check> {
    let daemon = ("daemon service", Service::Daemon, &setup.config, "--config");
    let nats = ("leaf node service", Service::Nats, &setup.leaf_config, "--nats-conf");
    [daemon, nats]
        .into_iter()
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
mod tests {
    use ants_proto::Role;

    use super::*;
    use crate::auth::tests::Server;

    fn setup(server: &Server, creds: Option<PathBuf>) -> Setup {
        Setup {
            nats: server.url.clone(),
            creds,
            js_domain: None,
            config: server.dir().join("no-config.toml"),
            leaf_config: server.dir().join("no-leaf.conf"),
            harnesses: false,
            team: server.dir().join("no-team"),
        }
    }

    fn named<'a>(checks: &'a [Check], name: &str) -> Vec<&'a Check> {
        checks.iter().filter(|c| c.check == name).collect()
    }

    #[tokio::test]
    async fn a_fresh_hub_is_missing_its_stream_until_init() {
        let Some(server) = Server::start() else { return };
        let alice = "alice".parse::<UserId>().unwrap();
        let setup = setup(&server, Some(server.creds_as(&alice, Role::Human)));

        let checks = Box::pin(run(&setup)).await;
        assert_eq!(named(&checks, "server")[0].status, Status::Ok, "{checks:#?}");
        assert_eq!(named(&checks, "jetstream")[0].status, Status::Ok, "{checks:#?}");
        let hub = named(&checks, "hub");
        let stream = hub.iter().find(|c| c.detail == format!("stream {STREAM} is missing")).expect("missing stream");
        assert_eq!((stream.status, stream.fix.as_slice()), (Status::Fail, ["ants init".to_owned()].as_slice()));
        assert_eq!(named(&checks, "config")[0].status, Status::Skip);
        assert_eq!(named(&checks, "leaf")[0].status, Status::Skip);

        let (_, js) = server.connect_as(&alice, Role::Human).await;
        let rights = Rights::from_creds(setup.creds.as_deref()).unwrap();
        hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
        let checks = Box::pin(run(&setup)).await;
        assert!(checks.iter().all(|c| matches!(c.status, Status::Ok | Status::Skip)), "{checks:#?}");
        let hub = named(&checks, "hub");
        assert_eq!(hub.len(), 1, "{checks:#?}");
        assert!(hub[0].detail.contains("presence not checked"), "{}", hub[0]);
    }

    #[test]
    fn consumers_warn_near_the_limit_the_hub_enforces() {
        let mut config = jetstream::stream::Config::default();
        assert_eq!(consumer_limit(&config), None, "no limit before nats-server 2.15");
        config.metadata.insert("_nats.ver".to_owned(), "2.14.9".to_owned());
        assert_eq!(consumer_limit(&config), None);
        config.metadata.insert("_nats.ver".to_owned(), "2.15.0-beta.1".to_owned());
        config.max_consumers = -1;
        assert_eq!(consumer_limit(&config), Some(1000), "-1 leaves the default");
        config.max_consumers = hub::MAX_CONSUMERS;
        assert_eq!(consumer_limit(&config), Some(10_000));

        assert_eq!(consumers(799, Some(1000)).status, Status::Ok);
        let near = consumers(800, Some(1000));
        assert_eq!((near.status, near.fix.as_slice()), (Status::Warn, ["ants init".to_owned()].as_slice()));
        let full = consumers(9_000, Some(10_000));
        assert_eq!(full.fix, [format!("nats stream edit {STREAM} --max-consumers 20000")]);
        assert_eq!(consumers(5_000, None).status, Status::Ok);
    }

    #[tokio::test]
    async fn the_versions_check_waits_for_every_component_to_allow_renewal() {
        let Some(server) = Server::start() else { return };
        let alice = "alice".parse::<UserId>().unwrap();
        let setup = setup(&server, Some(server.creds_as(&alice, Role::Human)));
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        let rights = Rights::from_creds(setup.creds.as_deref()).unwrap();
        hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
        let versions =
            async || Box::pin(run(&setup)).await.into_iter().find(|c| c.check == "versions").expect("a versions check");

        let dev_a = "dev-a".parse::<UserId>().unwrap();
        let (_, a_js) = server.connect_as(&dev_a, Role::Ant).await;
        let roster = a_js.get_key_value(ants_proto::ROSTER_BUCKET).await.unwrap();
        let now = chrono::Utc::now();
        let mut entry = serde_json::json!({
            "ant": "dev-a", "machine": "m1", "harness": "copilot", "started_at": now, "heartbeat_at": now,
        });
        roster.put("dev-a", serde_json::to_vec(&entry).unwrap().into()).await.unwrap();

        let manager = UserId::channel_manager();
        let creds = server.dir().join("channel-manager.creds");
        auth::issue_channel_manager(&server.dir().join("auth"), None, &creds).unwrap();
        let (_, m_js) = bus::connect(&server.url, Some(&creds), None, &manager, "test").await.unwrap();
        let published = ants_daemon::direct_kv::DirectKv::new(&m_js, None, ants_proto::WEBHOOKS_BUCKET).await.unwrap();
        let key = ants_proto::channel_manager_serving_key();
        published.put(&key, versions::serving().await).await.expect("the channel manager publishes its version");

        let hook = "github".parse::<UserId>().unwrap();
        let creds = server.dir().join("github.creds");
        let to = [ants_proto::Destination::Ant(dev_a.clone())];
        let permissions = ants_proto::Permissions::for_webhook(&hook, &to);
        auth::issue_with(&server.dir().join("auth"), &hook, crate::revoke::Kind::Webhook, permissions, &creds).unwrap();
        let _bus = crate::webhook::Bus::connect(&hook, Some(&creds), &server.url, None, "test").await.unwrap();

        let may = |s: &str| rights.may_publish(s);
        for _ in 0..50 {
            let running = versions::running(&js, may, chrono::Utc::now()).await.unwrap();
            if running.components.iter().any(|c| c.what.starts_with("webhook github on")) {
                assert_eq!(running.components.len(), 3, "{running:?}");
                assert!(running.unseen.is_empty(), "{running:?}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let check = versions().await;
        assert_eq!(check.status, Status::Warn, "{check}");
        let old = "the daemon of dev-a on m1 (no version published";
        assert!(check.detail.starts_with(&format!("older than permissions version {PERMISSIONS_VERSION}: {old}")));
        assert!(check.detail.contains("mixed: "), "{check}");
        assert!(!check.detail.contains("the channel manager on") && !check.detail.contains("webhook github on"));
        assert!(check.fix[0].starts_with("upgrade ants where they run"), "{check}");

        entry["version"] = ants_proto::ANTS_VERSION.into();
        entry["perms"] = PERMISSIONS_VERSION.0.into();
        roster.put("dev-a", serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
        let check = versions().await;
        assert_eq!(check.status, Status::Ok, "{check}");
        assert!(check.detail.starts_with("3 running components"), "{check}");
    }

    #[tokio::test]
    async fn an_ants_credentials_skip_the_hub_check() {
        let Some(server) = Server::start() else { return };
        let ant = "dev-a".parse::<UserId>().unwrap();
        let checks = Box::pin(run(&setup(&server, Some(server.creds_as(&ant, Role::Ant))))).await;
        assert_eq!(named(&checks, "jetstream")[0].status, Status::Ok, "{checks:#?}");
        assert_eq!(named(&checks, "hub")[0].status, Status::Skip, "{checks:#?}");
    }

    #[tokio::test]
    async fn a_missing_harness_fails_with_its_fix() {
        let dir = ants_test_support::scratch_dir("doctor-harness").canonicalize().unwrap();
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"ghost\"\n\
             [harnesses.ghost]\ncommand = \"/nonexistent-ants-bin/acp\"\n",
        )
        .unwrap();
        let loaded = config::load(&config).unwrap();
        let setup = Setup {
            nats: "nats://127.0.0.1:1".to_owned(),
            creds: None,
            js_domain: None,
            config: config.clone(),
            leaf_config: dir.join("leaf.conf"),
            harnesses: true,
            team: dir.join("no-team"),
        };
        let checks = check_ants(&loaded.ants, &loaded.harnesses, &setup, 0).await;
        let harness = named(&checks, "ant dev-a harness")[0];
        assert_eq!(harness.status, Status::Fail, "{checks:#?}");
        assert!(harness.detail.contains("/nonexistent-ants-bin/acp is not installed"), "{harness}");
        let fix = format!(
            "install /nonexistent-ants-bin/acp, or set the `command` of [harnesses.ghost] in {}",
            config.display()
        );
        assert_eq!(harness.fix, [fix]);
        assert_eq!(named(&checks, "ant dev-a credentials")[0].status, Status::Skip);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn ants_sharing_a_git_workdir_without_worktrees_are_warned() {
        let dir = ants_test_support::scratch_dir("doctor-shared-workdir").canonicalize().unwrap();
        std::fs::create_dir(dir.join(".git")).unwrap();
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            "messenger = false\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nworktrees = true\n\
             [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nmax_harnesses = 1\n",
        )
        .unwrap();
        let loaded = config::load(&config).unwrap();
        let checks = check_shared_workdirs(&loaded.ants, &config);
        assert_eq!(
            checks.iter().map(|c| (c.check.as_str(), c.status)).collect::<Vec<_>>(),
            [("ant dev-a workdir", Status::Warn)]
        );
        let fix = format!("worktrees = true in the [[ant]] named dev-a of {}", config.display());
        assert!(checks[0].fix[0].starts_with(&fix), "{}", checks[0]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn a_leaf_reports_an_ant_its_uplink_misses_and_a_node_not_listening() {
        let dir = ants_test_support::scratch_dir("doctor-leaf").canonicalize().unwrap();
        auth::init(&dir).unwrap();
        let machine: Machine = "m1".parse().unwrap();
        let uplink = dir.join("leaf-m1.creds");
        auth::issue_leaf(&dir, &machine, &[("dev-a".parse().unwrap(), Role::Ant)], &[], false, None, &uplink).unwrap();
        let leaf_config = dir.join("leaf.conf");
        let uplinks = ["nats-leaf://127.0.0.1:7422".to_owned()];
        let leaf = leaf::Leaf {
            machine: &machine,
            hub_domain: None,
            uplinks: &uplinks,
            creds: &uplink,
            system_creds: None,
            server_conf: &dir.join(auth::SERVER_CONF),
            ca_file: None,
            proxy: None,
            listen: "127.0.0.1:1",
        };
        leaf::write(&leaf, &leaf_config).unwrap();
        let config = dir.join("config.toml");
        std::fs::write(
            &config,
            "messenger = false\nnats = \"nats://127.0.0.1:1\"\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n",
        )
        .unwrap();
        let loaded = config::load(&config).unwrap();
        let setup = Setup {
            nats: "nats://127.0.0.1:1".to_owned(),
            creds: None,
            js_domain: None,
            config,
            leaf_config: leaf_config.clone(),
            harnesses: false,
            team: dir.clone(),
        };

        let checks = check_leaf(&setup, Leaf::read(&leaf_config), &loaded.ants, None, None, 0).await;
        assert_eq!(named(&checks, "leaf")[0].status, Status::Ok, "{checks:#?}");
        let carried = named(&checks, "leaf uplink")[0];
        assert_eq!(carried.status, Status::Fail, "{carried}");
        assert!(carried.detail.contains("no rights for dev-b"), "{carried}");
        assert!(carried.fix[0].starts_with("ants auth leaf m1") && carried.fix[0].contains("--ant dev-b"), "{carried}");
        assert_eq!(named(&checks, "leaf system uplink")[0].status, Status::Warn, "{checks:#?}");
        let node = named(&checks, "leaf node")[0];
        assert_eq!(node.status, Status::Fail, "{node}");
        assert!(node.detail.starts_with("nothing listens on 127.0.0.1:1"), "{node}");
        assert_eq!(named(&checks, "outbox")[0].status, Status::Skip, "{checks:#?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// With the team keys, the hub is asked whether it keeps pushed accounts: a warning without a `full` resolver, and
    /// for a hub of another team, which refuses the system account.
    #[tokio::test]
    async fn the_hub_resolver_check_warns_of_a_hub_keeping_pushed_accounts_in_memory() {
        for full in [false, true] {
            let Some(hub) = ants_test_support::NatsServer::spawn_with(|dir| {
                auth::init(&dir.join("auth")).unwrap();
                let resolver = match full {
                    true => format!("resolver {{ type: full, dir: {:?} }}\n", dir.join("jwt")),
                    false => String::new(),
                };
                format!("include \"auth/{}\"\n{resolver}", auth::SERVER_CONF)
            }) else {
                return;
            };
            let setup = Setup {
                nats: hub.url.clone(),
                creds: None,
                js_domain: None,
                config: hub.dir().join("no-config.toml"),
                leaf_config: hub.dir().join("no-leaf.conf"),
                harnesses: false,
                team: hub.dir().join("auth"),
            };
            let check = check_resolver(&setup, false).await;
            match full {
                true => assert_eq!(check.status, Status::Ok, "{check}"),
                false => {
                    assert_eq!(check.status, Status::Warn, "{check}");
                    assert!(check.fix[0].starts_with("add `resolver { type: full"), "{check}");
                }
            }
            assert_eq!(check_resolver(&setup, true).await.status, Status::Skip, "through the leaf node");

            auth::init(&hub.dir().join("other")).unwrap();
            let other = Setup { team: hub.dir().join("other"), ..setup };
            let check = check_resolver(&other, false).await;
            assert_eq!(check.status, Status::Warn, "{check}");
            assert!(check.detail.contains("refuses the team's system account"), "{check}");
            let none = Setup { team: hub.dir().join("no-team"), ..other };
            assert_eq!(check_resolver(&none, false).await.status, Status::Skip);
        }
    }

    #[test]
    fn the_key_check_finds_a_stored_seed_locked_missing_or_of_another_key() {
        let dir = ants_test_support::scratch_dir("doctor-key");
        let team = dir.join("team");
        auth::init(&team).unwrap();
        let user = nkeys::KeyPair::new_user();
        let (key, seed) = (user.public_key(), user.seed().unwrap());
        let jwt = auth::sign_user(&team, &"dev-a".parse().unwrap(), Role::Ant, None, &key).unwrap();
        let path = dir.join("dev-a.creds");
        let store = keystore::Memory::default();
        let check = |store: &keystore::Memory| {
            let check = check_key("ant dev-a key", &path, vec!["ants add dev-a".to_owned()], store).unwrap();
            let shown = check.to_string();
            assert!(!shown.contains(&seed), "{shown}");
            (check.status, shown)
        };

        auth::write_creds(&path, &jwt, &user).unwrap();
        assert!(check_key("ant dev-a key", &path, Vec::new(), &store).is_none(), "the seed is in the file");
        std::fs::write(&path, creds::stored_text(&jwt, &key)).unwrap();
        store.set(&key, &seed).unwrap();
        assert_eq!(check(&store).0, Status::Ok);
        store.lock(true);
        let (status, shown) = check(&store);
        assert!(status == Status::Fail && shown.contains("locked") && shown.contains("unlock"), "{shown}");
        store.lock(false);
        store.set(&key, &auth::seed(&nkeys::KeyPair::new_user()).unwrap()).unwrap();
        let (status, shown) = check(&store);
        assert!(status == Status::Fail && shown.contains("another key") && shown.contains("ants add"), "{shown}");
        store.delete(&key).unwrap();
        let (status, shown) = check(&store);
        assert!(status == Status::Fail && shown.contains("holds no seed") && shown.contains("backup"), "{shown}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_check_renders_on_one_line_with_its_fixes_indented() {
        let check = Check::fail("hub", "stream ANTS is missing", vec!["ants init".to_owned()]);
        assert_eq!(check.to_string(), "fail  hub: stream ANTS is missing\n      fix: ants init");
        assert_eq!(Check::ok("server", "connected").to_string(), "ok    server: connected");
        assert_eq!(
            serde_json::to_string(&check).unwrap(),
            r#"{"check":"hub","status":"fail","detail":"stream ANTS is missing","fix":["ants init"]}"#
        );
        assert_eq!(
            serde_json::to_string(&Check::skip("leaf", "none")).unwrap(),
            r#"{"check":"leaf","status":"skip","detail":"none"}"#
        );
    }

    #[test]
    fn the_cli_targets_the_leaf_by_its_listen_address() {
        assert!(same_address("nats://127.0.0.1:4222", "127.0.0.1:4222"));
        assert!(same_address("nats://localhost:4222", "127.0.0.1:4222"));
        assert!(same_address("127.0.0.1", "127.0.0.1:4222"));
        assert!(!same_address("nats://127.0.0.1:4223", "127.0.0.1:4222"));
        assert!(!same_address("wss://hub.example/nats", "127.0.0.1:4222"));
    }
}
