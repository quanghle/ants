//! Reaching the server: the connection as the CLI's credentials, JetStream, the hub's stream, buckets and consumers,
//! the JetStream storage, and how the hub's account resolver keeps pushed accounts.

use std::fmt::Write as _;
use std::path::Path;

use ants_daemon::monitoring::storage::{self, Usage};
use ants_daemon::secrets::keystore;
use ants_daemon::{bus, hub};
use ants_proto::{STREAM, UserId, inbox_consumer};
use async_nats::jetstream;

use super::colony::{check_avatars, check_drift, check_harness_versions, check_versions, read_colony};
use super::leaf_node::Leaf;
use super::services::start_server;
use super::transports::same_address;
use super::{Check, Connected, Identity, NETWORK_TIMEOUT, Setup, Status};
use crate::auth;
use crate::credentials::revoke;

/// Connect, then check JetStream and the hub's stream and buckets. `managed` is the nats-server configuration the
/// daemon runs itself ([`start_server`]).
pub(super) async fn reach(
    setup: &Setup,
    identity: Option<&Identity>,
    leaf: Option<&Leaf>,
    managed: Option<&Path>,
) -> (Vec<Check>, Option<Connected>) {
    let Some(identity) = identity else {
        return (vec![Check::skip("server", "no credentials to connect with")], None);
    };
    let me = &identity.me;
    let through_leaf = leaf.filter(|leaf| same_address(&setup.nats, &leaf.listen));
    let client = match connect(&setup.nats, setup.creds.as_deref(), &me.id).await {
        Ok(client) => client,
        Err(e) => return (vec![unreached(setup, identity, through_leaf, managed, e)], None),
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
    let colony = read_colony(&js).await;
    checks.push(check_harness_versions(&colony));
    checks.extend(check_avatars(&js, identity).await);
    checks.push(check_drift(&js, identity).await);
    (checks, Some(Connected { js, rights: identity.rights.clone(), colony }))
}

/// The oldest nats-server, major and minor version, ants runs on.
pub(super) const MIN_NATS_SERVER: (u32, u32) = (2, 15);

/// Whether nats-server `version` is [`MIN_NATS_SERVER`] or later; a version that does not parse passes.
pub(super) fn supported_server(version: &str) -> bool {
    let mut parts = version.split(['.', '-']).map(|part| part.parse::<u32>().ok());
    match (parts.next().flatten(), parts.next().flatten()) {
        (Some(major), Some(minor)) => (major, minor) >= MIN_NATS_SERVER,
        _ => true,
    }
}

/// Connect to `url` with `creds` as `user`, as the CLI does, within [`NETWORK_TIMEOUT`].
pub(super) async fn connect(url: &str, creds: Option<&Path>, user: &UserId) -> Result<async_nats::Client, Unreached> {
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

pub(super) enum Unreached {
    /// The server refused the credentials.
    Refused(String),
    /// No server answered.
    Unreachable(String),
    /// The credentials' seed could not be read.
    NoKey(String),
    Other(String),
}

pub(super) fn unreached(
    setup: &Setup,
    identity: &Identity,
    through_leaf: Option<&Leaf>,
    managed: Option<&Path>,
    e: Unreached,
) -> Check {
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
                let fix = start_server(setup, &leaf.path, managed);
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
pub(super) async fn check_jetstream(
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
            let detail = format!("the credentials were issued for JetStream domain {issued}; the CLI uses {used}");
            return Check::fail(check, detail, vec![set_domain]);
        }
        (None, Some(used), Some(finding)) => {
            let detail = format!("the credentials were issued for no JetStream domain; the CLI uses {used}");
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
pub(super) async fn check_hub(js: &jetstream::Context, identity: &Identity) -> Vec<Check> {
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
            let them = if unseen.len() == 1 { "it" } else { "them" };
            let _ =
                write!(detail, " ({} not checked: the credentials get no stream info of {them})", unseen.join(", "));
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
                hub::Problem::Unchecked(_) => Check::warn(check, finding.to_string(), vec![fix]),
                _ => Check::fail(check, finding.to_string(), vec![fix]),
            }
        })
        .collect()
}

/// The consumers of the `ANTS` stream, and of each bucket every daemon watches ([`hub::Object::watched`]), against the
/// most each takes ([`consumer_limit`]): past that, creating one fails, so an ant cannot read its inbox, or a daemon
/// cannot watch the bucket. Warns from four fifths of it. Those whose stream info the credentials do not get are left
/// out, the stream with a skip.
pub(super) async fn check_consumers(js: &jetstream::Context, identity: &Identity) -> Vec<Check> {
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
pub(super) async fn check_storage(
    connected: Option<&Connected>,
    setup: &Setup,
    settings: storage::Settings,
) -> Vec<Check> {
    let check = "storage";
    let Some(Connected { js, rights, .. }) = connected else {
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

pub(super) fn storage_check(usage: &Usage, largest: &[(String, u64)], warn_percent: u8, config: &Path) -> Check {
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
pub(super) fn consumer_limit(config: &jetstream::stream::Config) -> usize {
    usize::try_from(config.max_consumers).ok().filter(|&max| max > 0).unwrap_or(1000)
}

pub(super) fn consumers(object: hub::Object, count: usize, limit: usize) -> Check {
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

/// Whether the hub keeps the accounts `ants auth push` sends it across restarts, as only a `full` resolver does
/// ([`revoke::keeps_accounts`]): asked as a user of the team's system account, which only the machine holding the team
/// keys can sign.
pub(super) async fn check_resolver(setup: &Setup, through_leaf: bool) -> Check {
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

pub(super) fn copy_server_conf(conf: &Path, then: &str) -> String {
    format!("copy {} next to the hub's nats-server configuration and {then}", conf.display())
}
