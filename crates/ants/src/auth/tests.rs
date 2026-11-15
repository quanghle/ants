#![allow(clippy::too_many_lines)]

use std::assert_matches;

use ants_daemon::bus;
use ants_daemon::channel::{Channels, Earlier, Reading, Threads};
use ants_daemon::daemon::Claims;
use ants_daemon::dead::{self, Attempt};
use ants_daemon::direct_kv::DirectKv;
use ants_daemon::humans::Registry;
use ants_daemon::roster::Registration;
use ants_daemon::webhooks::Webhooks;
use ants_daemon::{notes, owners};
use ants_proto::{
    ChannelEntry, ChannelId, ChannelPost, DeadLetter, Destination, Envelope, NotesPost, RosterEntry, STREAM, ThreadId,
    attempt_key, chan_consumer, chan_reader, inbox_prefix, link_key, notes_consumer, session_key, subject,
};
use ants_test_support::NatsServer;
use async_nats::jetstream::AckKind;
use async_nats::{Client, Event, jetstream};
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::*;

const WAIT: Duration = Duration::from_secs(5);

/// A nats-server trusting a fresh team, whose keys are in its directory.
pub(crate) struct Server(NatsServer);

impl std::ops::Deref for Server {
    type Target = NatsServer;
    fn deref(&self) -> &NatsServer {
        &self.0
    }
}

impl Server {
    /// None when nats-server is not installed.
    pub(crate) fn start() -> Option<Self> {
        Self::start_with("", "")
    }

    /// A server trusting a fresh team, with `jetstream` added to its JetStream block and `extra` to its
    /// configuration.
    fn start_with(jetstream: &str, extra: &str) -> Option<Self> {
        NatsServer::spawn_with(|dir| {
            init(&dir.join("auth")).unwrap();
            let store = dir.join("js");
            format!("jetstream {{ store_dir: {store:?}\n{jetstream} }}\ninclude \"auth/{SERVER_CONF}\"\n{extra}")
        })
        .map(Self)
    }

    fn creds(&self, user: &UserId) -> PathBuf {
        self.creds_in(user, None)
    }

    /// Credentials of `user` issued for `domain`, as `ants auth user` without `--human` does.
    fn creds_in(&self, user: &UserId, domain: Option<&JsDomain>) -> PathBuf {
        let suffix = domain.map_or_default(|d| format!("-{d}"));
        let out = self.dir().join(format!("{user}{suffix}.creds"));
        issue(&self.dir().join("auth"), user, role(user, false), domain, &out).unwrap();
        out
    }

    pub(crate) async fn connect(&self, user: &UserId) -> (Client, jetstream::Context) {
        self.connect_as(user, role(user, false)).await
    }

    /// Credentials of `user` issued in `role`.
    pub(crate) fn creds_as(&self, user: &UserId, role: Role) -> PathBuf {
        let creds = self.dir().join(format!("{user}-{role:?}.creds"));
        issue(&self.dir().join("auth"), user, role, None, &creds).unwrap();
        creds
    }

    /// A connection with credentials of `user` issued in `role`.
    pub(crate) async fn connect_as(&self, user: &UserId, role: Role) -> (Client, jetstream::Context) {
        let creds = self.creds_as(user, role);
        let (client, mut js) = bus::connect(&self.url, Some(&creds), None, user, "test").await.unwrap();
        // Denied JetStream requests only time out.
        js.set_timeout(Duration::from_secs(1));
        (client, js)
    }

    /// A connection that reports the server errors it gets, such as permission violations.
    async fn watched(&self, user: &UserId) -> (Client, mpsc::UnboundedReceiver<String>) {
        self.watched_with(user, &self.creds(user)).await
    }

    /// A connection with `creds` that reports the server errors it gets.
    async fn watched_with(&self, user: &UserId, creds: &Path) -> (Client, mpsc::UnboundedReceiver<String>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let client = async_nats::ConnectOptions::with_credentials_file(creds)
            .await
            .unwrap()
            .custom_inbox_prefix(inbox_prefix(user))
            .event_callback(move |event| {
                let tx = tx.clone();
                async move {
                    if let Event::ServerError(e) = event {
                        let _ = tx.send(e.to_string());
                    }
                }
            })
            .connect(&self.url)
            .await
            .unwrap();
        (client, rx)
    }
}

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

#[test]
fn enrollment_credentials_issued_with_a_zero_ttl_never_expire() {
    let dir = ants_test_support::scratch_dir("enroll-ttl");
    let (team, out, dev_a) = (dir.join("auth"), dir.join("enroll-dev-a.creds"), p("dev-a"));
    init(&team).unwrap();
    let issued = |ttl| {
        let exp = issue_enrollment(&team, &dev_a, ttl, &out)?;
        let text = std::fs::read_to_string(&out).unwrap();
        anyhow::Ok((exp, verified(creds_jwt(&text).unwrap()).unwrap().exp))
    };
    assert_eq!(issued(Duration::ZERO).unwrap(), (None, None));
    let (exp, claimed) = issued(Duration::from_secs(60)).unwrap();
    assert!(exp.is_some_and(|exp| exp >= now().unwrap() + 59) && claimed == exp, "{exp:?} {claimed:?}");
    let err = issued(Duration::from_millis(500)).unwrap_err().to_string();
    assert!(err.contains("under a second"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_renewed_uplink_keeps_its_key_and_rights_and_a_revoked_one_is_not_renewed() {
    let dir = ants_test_support::scratch_dir("extend-leaf");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    issue_leaf(&team, &m1, &[(p("dev-a"), Role::Ant)], &[], EnrollApprover::Without, None, &out).unwrap();
    let (jwt, key) = read_creds(&out).unwrap();
    let renewed = extend_leaf(&team, &jwt, &m1, Permissions::for_leaf(&m1, [(&p("dev-c"), Role::Ant)])).unwrap();
    assert!(allows_all(&renewed, &jwt).unwrap() && !allows_all(&jwt, &renewed).unwrap());
    assert_eq!(check_leaf(&account_key(&team).unwrap(), &renewed, &m1).unwrap().sub, key.public_key());
    let none = || Permissions { publish: vec![], subscribe: vec![] };
    let ant = sign_user(&team, &p("leaf-m1"), Role::Ant, None, &KeyPair::new_user().public_key()).unwrap();
    assert!(extend_leaf(&team, &ant, &m1, none()).is_err(), "an ant named leaf-m1 is no leaf");
    revoke::revoke(&team, Some("leaf-m1"), &[]).unwrap();
    let err = extend_leaf(&team, &jwt, &m1, none()).unwrap_err().to_string();
    assert!(err.contains("revoked"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn renewed_credentials_keep_what_they_are_for_a_new_key() {
    use ants_daemon::creds;
    let dir = ants_test_support::scratch_dir("renew");
    let team = dir.join("auth");
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    let out = |name: &str| team.join(format!("{name}.creds"));
    let renewed = |name: &str| {
        let (jwt, key) = read_creds(&out(name)).unwrap();
        let new = KeyPair::new_user();
        let renewed = renew(&team, &jwt, &new.public_key(), None).unwrap();
        assert!(creds::Finding::from_jwt(&jwt).unwrap().same_credentials(&renewed.finding), "{name}");
        assert_eq!(renewed.finding.version, PERMISSIONS_VERSION, "{name}");
        assert_eq!(
            (verified(&renewed.jwt).unwrap().sub, renewed.previous.as_str()),
            (new.public_key(), &*key.public_key())
        );
        let line = revoke::issued(&team).unwrap().pop().unwrap();
        assert_eq!((line.key, line.renewed_from), (new.public_key(), Some(key.public_key())), "{name}");
        renewed
    };
    issue(&team, &p("dev-a"), Role::Ant, Some(&hub), &out("dev-a")).unwrap();
    assert_eq!(renewed("dev-a").finding.js_domain, Some(hub.clone()));
    let device = issue_device(
        &team,
        &p("alice"),
        Kind::Human,
        Permissions::for_user(&p("alice"), Role::Human),
        Some("laptop"),
        &out("alice"),
    )
    .unwrap();
    assert_eq!(renewed("alice").finding.device, device);
    let devices = revoke::devices(&team, "alice").unwrap();
    assert_eq!(devices.len(), 1, "one device, renewed: {devices:?}");
    assert_eq!(devices[0].label.as_deref(), Some("laptop"));
    let destinations = [Destination::Ant(p("dev-a")), Destination::Channel("ops".parse().unwrap())];
    let hook = Permissions::for_webhook(&p("hook"), &destinations);
    issue_with(&team, &p("hook"), Kind::Webhook, hook, &out("hook")).unwrap();
    renewed("hook");
    let observer = Permissions::for_observer(&p("audit")).with_js_domain(Some(&hub));
    issue_with(&team, &p("audit"), Kind::Observer, observer, &out("audit")).unwrap();
    let audit = renewed("audit");
    assert_eq!((&audit.finding.kind, audit.finding.js_domain.as_ref()), (&creds::Kind::Observer, Some(&hub)));
    assert_eq!(audit.finding.renew(), ["ants auth user audit --observer --js-domain hub"]);
    assert_eq!(revoke::issued(&team).unwrap().pop().unwrap().kind, Kind::Observer);
    issue_channel_manager(&team, None, &out("channel-manager")).unwrap();
    renewed("channel-manager");
    let m1: Machine = "m1".parse().unwrap();
    let members = members(&[p("dev-a"), p("dev-b")], &[p("alice")]);
    issue_leaf(&team, &m1, &members, &[p("hook")], EnrollApprover::With, Some(&hub), &out("leaf-m1")).unwrap();
    let leaf = renewed("leaf-m1");
    check_leaf(&account_key(&team).unwrap(), &leaf.jwt, &m1).unwrap();
    let uplink = allowed(&verified_body(&leaf.jwt).unwrap().1);
    let react = subject::ALL_REACT_REQUESTS.to_owned();
    assert!(uplink.subscribe.contains(&react), "`ants signer` on the machine answers ants' reactions requests");
    let kept = crate::members::issued(&team, &m1).unwrap().unwrap();
    assert_eq!(
        kept,
        crate::members::Members::new(&members, &[p("hook")], EnrollApprover::With),
        "the registry keeps its members"
    );

    // A lifetime carries over: as long from now as the earlier credentials had from their issue.
    let account = load(&team.join(ACCOUNT_SEED)).unwrap();
    let key = KeyPair::new_user().public_key();
    let ant = Permissions::for_user(&p("dev-c"), Role::Ant);
    let expiring = user_jwt(&account, &key, "dev-c", ant, json!({}), Some(now().unwrap() + 3600)).unwrap();
    let exp = verified(&renew(&team, &expiring, &KeyPair::new_user().public_key(), None).unwrap().jwt).unwrap().exp;
    assert!(exp.is_some_and(|exp| exp.abs_diff(now().unwrap() + 3600) <= 2), "{exp:?}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_human_device_binds_its_encryption_key_and_renewal_keeps_it_or_adds_one() {
    use ants_daemon::{creds, keystore};
    let dir = ants_test_support::scratch_dir("renew-xkey");
    let team = dir.join("auth");
    init(&team).unwrap();
    let out = |name: &str| dir.join(format!("{name}.creds"));
    let human = || Permissions::for_user(&p("alice"), Role::Human);
    let new = || KeyPair::new_user().public_key();
    let bound = |jwt: &str| creds::Finding::from_jwt(jwt).unwrap().xkey;
    let registered = || revoke::issued(&team).unwrap().pop().unwrap().xkey;

    issue_device(&team, &p("alice"), Kind::Human, human(), Some("laptop"), &out("alice")).unwrap();
    let (jwt, _) = read_creds(&out("alice")).unwrap();
    let xkey = creds::load_xkey(&out("alice"), &keystore::Memory::default()).unwrap().unwrap();
    let laptop = device_key(&xkey).unwrap();
    assert_eq!((bound(&jwt), registered()), (Some(laptop.clone()), Some(laptop.clone())));
    let renewed = renew(&team, &jwt, &new(), None).unwrap();
    assert_eq!((bound(&renewed.jwt), registered()), (Some(laptop.clone()), Some(laptop.clone())), "kept");
    let other = device_key(&XKey::new()).unwrap();
    let err = renew(&team, &jwt, &new(), Some(&other)).unwrap_err().to_string();
    assert!(err.contains("already"), "{err}");

    let (unbound, _) = sign_device(&team, &p("alice"), Kind::Human, human(), None, &new(), None).unwrap();
    assert_eq!(bound(&unbound), None, "signed for a key generated elsewhere, without --xkey");
    let added = renew(&team, &unbound, &new(), Some(&other)).unwrap();
    assert_eq!((bound(&added.jwt), registered()), (Some(other.clone()), Some(other.clone())));
    let devices = revoke::devices(&team, "alice").unwrap();
    assert_eq!(devices.iter().map(|d| d.xkey.clone()).collect::<Vec<_>>(), [Some(laptop), Some(other.clone())]);

    issue(&team, &p("dev-a"), Role::Ant, None, &out("dev-a")).unwrap();
    let (ant, _) = read_creds(&out("dev-a")).unwrap();
    assert_eq!(bound(&ant), None);
    assert!(creds::xkey_of(&std::fs::read_to_string(out("dev-a")).unwrap()).is_none(), "an ant holds no xkey");
    let err = renew(&team, &ant, &new(), Some(&other)).unwrap_err().to_string();
    assert!(err.contains("only a human's"), "{err}");
    let ant_rights = Permissions::for_user(&p("dev-b"), Role::Ant);
    assert!(sign_device(&team, &p("dev-b"), Kind::Ant, ant_rights, None, &new(), Some(&other)).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn credentials_expired_revoked_foreign_or_of_no_renewable_kind_are_not_renewed() {
    let dir = ants_test_support::scratch_dir("renew-refused");
    let (team, other) = (dir.join("auth"), dir.join("other"));
    init(&team).unwrap();
    init(&other).unwrap();
    let out = |name: &str| dir.join(format!("{name}.creds"));
    let jwt = |name: &str| read_creds(&out(name)).unwrap();
    let refused = |jwt: &str, key: &str| renew(&team, jwt, key, None).unwrap_err().to_string();
    let new = || KeyPair::new_user().public_key();

    issue(&team, &p("dev-a"), Role::Ant, None, &out("dev-a")).unwrap();
    let (dev_a, dev_a_key) = jwt("dev-a");
    assert!(refused(&dev_a, &dev_a_key.public_key()).contains("generate a new one"));
    assert!(refused(&dev_a, &account_key(&team).unwrap()).contains("not the public key of a user"));
    issue(&team, &p("dev-b"), Role::Ant, None, &out("dev-b")).unwrap();
    let (_, dev_b_key) = jwt("dev-b");
    let err = refused(&dev_a, &dev_b_key.public_key());
    assert!(err.contains("issued to other credentials"), "a key already dev-b's: {err}");
    let stolen = new();
    revoke::revoke(&team, None, std::slice::from_ref(&stolen)).unwrap();
    assert!(refused(&dev_a, &stolen).contains("revoked"));

    issue(&other, &p("dev-a"), Role::Ant, None, &out("foreign")).unwrap();
    assert!(refused(&jwt("foreign").0, &new()).contains("not by the team's account"));

    let account = load(&team.join(ACCOUNT_SEED)).unwrap();
    let ant = Permissions::for_user(&p("dev-c"), Role::Ant);
    let expired = user_jwt(&account, &new(), "dev-c", ant, json!({}), Some(now().unwrap() - 1)).unwrap();
    assert!(refused(&expired, &new()).contains("expired"));

    issue_enrollment(&team, &p("dev-c"), Duration::from_secs(60), &out("enroll-dev-c")).unwrap();
    assert!(refused(&jwt("enroll-dev-c").0, &new()).contains("cannot be renewed"));
    let m1: Machine = "m1".parse().unwrap();
    issue_leaf(&team, &m1, &[(p("dev-a"), Role::Ant)], &[], EnrollApprover::Without, None, &out("leaf-m1")).unwrap();
    let system = read_creds(&leaf_system_creds(&out("leaf-m1"))).unwrap().0;
    assert!(refused(&system, &new()).contains("team's account"), "the system account's");

    revoke::revoke(&team, Some("dev-a"), &[]).unwrap();
    assert!(refused(&dev_a, &new()).contains("revoked"));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn issued_credentials_carry_the_current_permissions_version_and_name_their_renewal() {
    use ants_daemon::creds::{self, VersionStatus};
    let dir = ants_test_support::scratch_dir("creds-check");
    let team = dir.join("auth");
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    let renewal = |name: &str| {
        let finding = creds::check(&team.join(format!("{name}.creds"))).unwrap();
        assert_eq!(finding.version_status(), VersionStatus::Current, "{name}");
        assert!(
            finding.warnings(None, now().unwrap()).is_empty(),
            "{name}: {:?}",
            finding.warnings(None, now().unwrap())
        );
        finding.renew()
    };
    let out = |name: &str| team.join(format!("{name}.creds"));
    issue(&team, &p("dev-a"), Role::Ant, Some(&hub), &out("dev-a")).unwrap();
    assert_eq!(renewal("dev-a"), ["ants auth user dev-a --js-domain hub"]);
    issue(&team, &p("alice"), Role::Human, None, &out("alice")).unwrap();
    assert_eq!(renewal("alice"), ["ants auth user alice --human"]);
    let destinations = [Destination::Ant(p("dev-a")), Destination::Channel("ops".parse().unwrap())];
    let hook = Permissions::for_webhook(&p("hook"), &destinations);
    issue_with(&team, &p("hook"), Kind::Webhook, hook, &out("hook")).unwrap();
    assert_eq!(renewal("hook"), ["ants auth user hook --webhook --to dev-a --channel ops"]);
    issue_channel_manager(&team, None, &out("channel-manager")).unwrap();
    assert_eq!(renewal("channel-manager"), ["ants auth channel-manager"]);
    let m1: Machine = "m1".parse().unwrap();
    let members = members(&[p("dev-a")], &[p("alice")]);
    issue_leaf(&team, &m1, &members, &[p("hook")], EnrollApprover::With, Some(&hub), &out("leaf-m1")).unwrap();
    assert_eq!(
        renewal("leaf-m1"),
        [
            "ants auth leaf m1 --ant dev-a --human alice --webhook hook --enroll-approver --js-domain hub",
            "ants leaf outbox m1 --ant dev-a --human alice --js-domain hub",
        ]
    );
    let system = creds::check(&leaf_system_creds(&out("leaf-m1"))).unwrap();
    assert_eq!(
        (system.kind.clone(), system.version_status()),
        (creds::Kind::LeafSystem { machine: m1.clone() }, VersionStatus::Current)
    );
    let (jwt, _) = read_creds(&out("leaf-m1")).unwrap();
    let more = Permissions::for_leaf(&m1, [(&p("dev-c"), Role::Ant)]).with_uplink_js_domain(Some(&hub));
    let renewed = creds::Finding::from_jwt(&extend_leaf(&team, &jwt, &m1, more).unwrap()).unwrap();
    assert_eq!(renewed.version_status(), VersionStatus::Current);
    assert!(renewed.renew()[0].starts_with("ants auth leaf m1 --ant dev-a --ant dev-c --human alice"), "{renewed:?}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// Issue `out`, uplink credentials of `machine` for the hub's `domain`, for as many ants as they may carry; returns
/// the ants and the error refusing one more.
pub(crate) fn uplink_for_most_ants(
    team: &Path,
    machine: &Machine,
    domain: Option<&JsDomain>,
    out: &Path,
) -> (Vec<UserId>, String) {
    let mut ants = Vec::new();
    loop {
        ants.push(p(&format!("dev-{}", ants.len())));
        if let Err(e) = issue_leaf(team, machine, &members(&ants, &[]), &[], EnrollApprover::Without, domain, out) {
            ants.pop();
            return (ants, e.to_string());
        }
    }
}

#[test]
fn uplink_credentials_a_hub_would_refuse_are_neither_issued_nor_renewed() {
    let dir = ants_test_support::scratch_dir("uplink-size");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let domain: JsDomain = "hub".parse().unwrap();
    let (ants, err) = uplink_for_most_ants(&team, &m1, Some(&domain), &out);
    eprintln!("{} ants fit the uplink of m1 (domain {domain})", ants.len());
    assert!(ants.len() >= 48, "only {} ants fit", ants.len());
    assert!(err.contains("max_control_line 16384") && err.contains("another leaf node"), "{err}");
    let (jwt, _) = read_creds(&out).unwrap();
    assert!(jwt.len() <= MAX_UPLINK_JWT, "the refused credentials are not written");
    // As long a name as that of the ant refused.
    let more = Permissions::for_leaf(&m1, [(&p("dev-xx"), Role::Ant)]).with_uplink_js_domain(Some(&domain));
    let err = extend_leaf(&team, &jwt, &m1, more).unwrap_err().to_string();
    assert!(err.contains("the uplink credentials of m1 would take"), "{err}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// Each ant added behind a leaf node (`request_ant`, enrollment) grows its uplink JWT by the same amount, about
/// 4.7 KB for a six-character name, 5 KB with the hub's JetStream domain, whose plain JetStream API and KV forms it
/// leaves out: no permission is shared between two ants, and none is a pattern matching an ant not yet added.
#[test]
fn each_ant_added_grows_the_uplink_by_the_same_measure() {
    let dir = ants_test_support::scratch_dir("uplink-growth");
    let (team, out, m1) = (dir.join("auth"), dir.join("leaf-m1.creds"), "m1".parse::<Machine>().unwrap());
    init(&team).unwrap();
    let hub: JsDomain = "hub".parse().unwrap();
    for (domain, least, most) in [(None, 4_200, 5_000), (Some(&hub), 4_800, 5_400)] {
        issue_leaf(&team, &m1, &members(&[p("dev-00")], &[]), &[], EnrollApprover::Without, domain, &out).unwrap();
        let (mut jwt, _) = read_creds(&out).unwrap();
        let mut growth = Vec::new();
        for i in 1..=5 {
            let more =
                Permissions::for_leaf(&m1, [(&p(&format!("dev-{i:02}")), Role::Ant)]).with_uplink_js_domain(domain);
            let renewed = extend_leaf(&team, &jwt, &m1, more).unwrap();
            growth.push(renewed.len() - jwt.len());
            jwt = renewed;
        }
        if domain.is_some() {
            let publish = allowed(&verified_body(&jwt).unwrap().1).publish;
            let plain = publish.iter().find(|s| s.starts_with("$JS.API.") || s.starts_with("$KV."));
            assert_eq!(plain, None, "the hub denies the plain forms on a leaf connection of another domain");
        }
        let (min, max) = (*growth.iter().min().unwrap(), *growth.iter().max().unwrap());
        eprintln!("uplink of m1 (domain {domain:?}): {} bytes, each ant adds {growth:?}", jwt.len());
        assert!(max - min <= 4, "ants of names of one length grow it alike: {growth:?}");
        assert!((least..=most).contains(&max), "{max} bytes per ant, domain {domain:?}");
        let room = (MAX_UPLINK_JWT - jwt.len()) / max + 6;
        assert!(room >= if domain.is_some() { 48 } else { 54 }, "{room} ants fit");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// The server handles operations in order, so the next violation names the first denied one.
/// Returns once the server has registered the subscriptions of `js`'s client: async-nats' flush only writes them
/// out, so another client's publish could otherwise get there first, and be missed.
async fn subscribed(js: &jetstream::Context, user: &UserId) {
    bus::open_stream(js, user).await.unwrap();
}

async fn next_violation(client: &Client, errors: &mut mpsc::UnboundedReceiver<String>) -> String {
    client.flush().await.unwrap();
    timeout(WAIT, errors.recv()).await.expect("no violation").unwrap()
}

#[tokio::test]
async fn credentials_confine_each_user_to_its_role() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));

    assert!(bus::connect(&server.url, None, None, &human, "anonymous").await.is_err(), "credentials are required");

    let (_, a_js) = server.connect(&dev_a).await;
    assert!(bus::open_stream(&a_js, &dev_a).await.is_err(), "only the human creates the stream");
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    // dev-a finds JetStream on connecting as the daemon does, without the stream's info.
    let a_creds = server.creds(&dev_a);
    let connecting = bus::connect_retrying(&server.url, Some(&a_creds), None, &dev_a, "test", std::future::pending());
    timeout(WAIT, connecting).await.expect("dev-a reaches JetStream").unwrap().expect("not stopped");

    let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    h_client.flush().await.unwrap();
    let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
    bus::publish(&h_js, &ask).await.unwrap();

    // dev-a serves its inbox the way the daemon does, and replies.
    let stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
    let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
    let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
    let reply = ask.reply(dev_a.clone(), "hello", Role::Human).unwrap();
    bus::publish(&a_js, &reply).await.unwrap();
    msg.double_ack().await.unwrap();
    let got = timeout(WAIT, human_inbox.next()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&got.payload).unwrap(), reply);

    // dev-a cannot read dev-b's inbox, through a consumer or the stream.
    assert!(bus::inbox_consumer(&stream, &dev_b, WAIT, 5, 2).await.is_err());
    assert!(stream.get_raw_message(1).await.is_err());

    // dev-a keeps its sessions under its own keys, and cannot touch dev-b's.
    let thread: ThreadId = "t1".parse().unwrap();
    assert!(bus::ensure_sessions(&a_js, None, &dev_a).await.is_err(), "only the human creates the bucket");
    assert!(bus::ensure_attempts(&a_js, None, &dev_a).await.is_err(), "only the human creates the bucket");
    bus::init_own_buckets(&h_js).await.unwrap();
    let sessions = bus::ensure_sessions(&a_js, None, &dev_a).await.unwrap();
    sessions.put(session_key(&dev_a, &thread), "s1").await.unwrap();
    assert_eq!(sessions.get(session_key(&dev_a, &thread)).await.unwrap().as_deref(), Some(&b"s1"[..]));
    assert!(sessions.put(session_key(&dev_b, &thread), "s1").await.is_err());
    assert!(sessions.get(session_key(&dev_b, &thread)).await.is_err());
    // Except that it reads dev-b's daily spend, for pool caps, though it cannot write it.
    let day = ants_proto::daily_spend_key(&dev_b, "2026-11-13");
    assert_eq!(sessions.get(&day).await.unwrap(), None);
    assert!(sessions.put(&day, "{}").await.is_err());

    // So it does with the markers of its attempts.
    let attempts = bus::ensure_attempts(&a_js, None, &dev_a).await.unwrap();
    attempts.put(attempt_key(&dev_a, 1), "started").await.unwrap();
    assert_eq!(attempts.get(attempt_key(&dev_a, 1)).await.unwrap().as_deref(), Some(&b"started"[..]));
    attempts.delete(attempt_key(&dev_a, 1)).await.unwrap();
    assert_eq!(attempts.get(attempt_key(&dev_a, 1)).await.unwrap(), None);
    assert!(attempts.put(attempt_key(&dev_b, 1), "started").await.is_err());
    assert!(attempts.get(attempt_key(&dev_b, 1)).await.is_err());
    // A human withdraws a message waiting for dev-a, and reads what became of one, but no attempt marker; dev-a
    // marks only its own messages.
    let marks = DirectKv::new(&h_js, None, ants_proto::ATTEMPTS_BUCKET).await.unwrap();
    let (queued, taken) = (ants_proto::Uuid::now_v7(), ants_proto::Uuid::now_v7());
    let mark = |ant: &UserId, message| ants_proto::message_mark_key(ant, message);
    assert!(marks.create(mark(&dev_a, &queued), "withdrawn").await.unwrap());
    assert!(!attempts.create(mark(&dev_a, &queued), "started").await.unwrap(), "withdrawn first");
    assert!(attempts.create(mark(&dev_a, &taken), "started").await.unwrap());
    assert_eq!(marks.get(mark(&dev_a, &taken)).await.unwrap().as_deref(), Some(&b"started"[..]));
    assert!(marks.put(attempt_key(&dev_a, 2), "started").await.is_err());
    assert!(marks.get(attempt_key(&dev_a, 1)).await.is_err());
    assert!(attempts.create(mark(&dev_b, &queued), "started").await.is_err());
    // Watching the messages it sent for those that keep waiting, dev-a reads them back through its own reader and
    // what became of them, and marks those told of under its own keys; the human only under its own.
    let (sent, all) = (subject::inbox_from(&dev_a), jetstream::consumer::DeliverPolicy::All);
    let read_sent = async |ant: &UserId, filter: &str| {
        let (reader, mut read) = (ants_proto::sent_reader(ant), Vec::new());
        let reading = ants_daemon::reading::Reading::Consumer;
        ants_daemon::reading::read_back(&a_js, &reader, reading, filter, all, |seq, _, _, _| {
            read.push(seq);
            std::ops::ControlFlow::Continue(())
        })
        .await
        .map(|()| read)
    };
    assert_eq!(read_sent(&dev_a, &sent).await.unwrap().len(), 1, "its reply");
    assert!(read_sent(&dev_a, &subject::inbox_from(&dev_b)).await.is_err());
    assert!(read_sent(&dev_b, &subject::inbox_from(&dev_b)).await.is_err());
    assert_eq!(attempts.get(mark(&dev_b, &queued)).await.unwrap(), None);
    let told = |user: &UserId, message| ants_proto::unserved_key(user, message);
    assert!(attempts.create(told(&dev_a, &queued), "{}").await.unwrap());
    assert!(!attempts.create(told(&dev_a, &queued), "{}").await.unwrap(), "told once");
    assert!(attempts.create(told(&dev_b, &queued), "{}").await.is_err());
    assert!(marks.create(told(&human, &queued), "{}").await.unwrap());
    assert!(marks.create(told(&dev_a, &taken), "{}").await.is_err());
    assert!(marks.get(told(&dev_a, &queued)).await.is_err());

    // Nor list their keys, which a stream info request with a subjects filter would, as of the owners bucket.
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let filter = br#"{"subjects_filter":">"}"#;
    for bucket in [ants_proto::SESSIONS_BUCKET, ants_proto::ATTEMPTS_BUCKET, ants_proto::OWNERS_BUCKET] {
        let info = format!("$JS.API.STREAM.INFO.KV_{bucket}");
        a_raw.publish(info.clone(), filter[..].into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");
    }
    // Nor the subjects stored in the stream: who messages whom, approval requests and dead letters.
    let info = format!("$JS.API.STREAM.INFO.{STREAM}");
    a_raw.publish(info.clone(), filter[..].into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");

    // Nor speak for dev-b, or listen to others.
    a_raw.publish(subject::status(&dev_a, &thread), "ok".into()).await.unwrap();
    a_raw.publish(subject::status(&dev_b, &thread), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains("Publish to \"ants.status.dev-b.t1\""), "{v}");
    a_raw.publish(subject::approvals(&dev_b), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains("Publish to \"ants.approvals.dev-b\""), "{v}");
    a_raw.publish(subject::outbox(&subject::dead(&dev_a)).unwrap(), "ok".into()).await.unwrap();
    for spoof in [subject::dead(&dev_b), subject::outbox(&subject::dead(&dev_b)).unwrap()] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }
    // It sends only under its own name: not as the human, nor as dev-b to the human, nor as the hub's stream
    // republishing what dev-b sent.
    a_raw.publish(subject::inbox(&dev_a, &dev_a), "ok".into()).await.unwrap();
    let republished = format!("{}.stored.{dev_b}", inbox_prefix(&human));
    for spoof in [subject::inbox(&dev_b, &human), subject::inbox(&human, &dev_b), republished] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }
    // It posts to thread notes only under its own name too.
    let thread: ThreadId = "t2".parse().unwrap();
    a_raw.publish(subject::notes(&thread, &dev_a), "ok".into()).await.unwrap();
    for spoof in [subject::notes(&thread, &dev_b), subject::notes(&thread, &human)] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }
    // dev-a hears the human's decisions on its approval requests, but cannot decide them itself.
    let request = ants_proto::Uuid::now_v7();
    let mut decisions = a_raw.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
    a_raw.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"ants.approvals.dev-a.{request}\"")), "{v}");
    h_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
    let got = timeout(WAIT, decisions.next()).await.unwrap().unwrap();
    assert_eq!(&got.payload[..], b"allow");
    // It reads back the decision stored on its own request, and on no one else's.
    // The human's publish is not acknowledged, so the stream may store it a moment after dev-a hears it.
    let stored = timeout(WAIT, async {
        loop {
            match stream.direct_get_last_for_subject(subject::approval_decision(&dev_a, &request)).await {
                Ok(stored) => break stored,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(&stored.payload[..], b"allow");
    let of_b = format!("$JS.API.DIRECT.GET.{STREAM}.{}", subject::approval_decision(&dev_b, &request));
    a_raw.publish(of_b.clone(), "".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{of_b}\"")), "{v}");
    // Only a human asks dev-a to cancel a prompt; dev-a hears its own cancel requests, and answers as itself only.
    let mut cancels = a_raw.subscribe(subject::cancel_of(&dev_a)).await.unwrap();
    let mut answers = h_client.subscribe(subject::cancelled(&dev_a, &thread)).await.unwrap();
    h_client.flush().await.unwrap();
    a_raw.publish(subject::cancel(&dev_a, &thread), "self".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::cancel(&dev_a, &thread))), "{v}");
    h_client.publish(subject::cancel(&dev_a, &thread), "cancel".into()).await.unwrap();
    let got = timeout(WAIT, cancels.next()).await.unwrap().unwrap();
    assert_eq!(&got.payload[..], b"cancel");
    a_raw.publish(subject::cancelled(&dev_a, &thread), "done".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, answers.next()).await.unwrap().unwrap().payload[..], b"done");
    a_raw.publish(subject::cancelled(&dev_b, &thread), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::cancelled(&dev_b, &thread))), "{v}");
    let _sub = a_raw.subscribe(subject::cancel_of(&dev_b)).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Subscription to \"{}\"", subject::cancel_of(&dev_b))), "{v}");
    // Likewise only a human asks for dev-a's queue, and dev-a answers as itself only.
    let mut asks = a_raw.subscribe(subject::queue(&dev_a)).await.unwrap();
    let mut listed = h_client.subscribe(subject::queued(&dev_a)).await.unwrap();
    h_client.flush().await.unwrap();
    a_raw.publish(subject::queue(&dev_a), "self".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::queue(&dev_a))), "{v}");
    h_client.publish(subject::queue(&dev_a), "queue".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, asks.next()).await.unwrap().unwrap().payload[..], b"queue");
    a_raw.publish(subject::queued(&dev_a), "listed".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, listed.next()).await.unwrap().unwrap().payload[..], b"listed");
    a_raw.publish(subject::queued(&dev_b), "spoof".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{}\"", subject::queued(&dev_b))), "{v}");
    let _sub = a_raw.subscribe(subject::queue(&dev_b)).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Subscription to \"{}\"", subject::queue(&dev_b))), "{v}");
    // Its own inbox it may watch live; a denial would be the next violation below.
    let _own = a_raw.subscribe(subject::inbox_of(&dev_a)).await.unwrap();
    let _own = a_raw.subscribe(subject::stored_inbox(&dev_a)).await.unwrap();
    for denied in
        [subject::inbox_of(&dev_b), subject::stored_inbox(&dev_b), "_INBOX.>".to_owned(), subject::ALL.to_owned()]
    {
        let _sub = a_raw.subscribe(denied.clone()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
}

#[tokio::test]
async fn every_human_has_the_same_rights_and_reads_only_its_own_inbox() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    assert_eq!(
        (role(&alice, true), role(&alice, false), role(&UserId::human(), false)),
        (Role::Human, Role::Ant, Role::Human)
    );
    let (a_client, a_js) = server.connect_as(&alice, role(&alice, true)).await;
    let (b_client, b_js) = server.connect_as(&bob, role(&bob, true)).await;
    let (d_client, d_js) = server.connect(&dev_a).await;
    let (_, carol_js) = server.connect_as(&p("carol"), role(&p("carol"), false)).await;

    // Either human creates the stream and the buckets; a name issued without --human does not.
    assert!(bus::ensure_stream(&carol_js).await.is_err(), "carol is no human");
    assert!(crate::humans::ensure(&d_js).await.is_err(), "only a human creates the humans bucket");
    bus::ensure_stream(&a_js).await.unwrap();
    let b_stream = bus::ensure_stream(&b_js).await.unwrap();
    bus::ensure_roster(&b_js, ants_proto::ROSTER_TTL).await.unwrap();
    bus::init_own_buckets(&a_js).await.unwrap();
    bus::init_own_buckets(&b_js).await.unwrap();
    bus::ensure_channels(&a_js).await.unwrap();
    let a_humans = crate::humans::ensure(&a_js).await.unwrap();
    let b_humans = crate::humans::ensure(&b_js).await.unwrap();

    // Each human writes the humans bucket; ants read it, but never write it.
    for (kv, human) in [(&a_humans, &alice), (&b_humans, &bob)] {
        let entry = ants_proto::HumanEntry::new(human.clone());
        kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    }
    let d_humans = crate::humans::ensure(&d_js).await.unwrap();
    let listed: ants_proto::HumanEntry = serde_json::from_slice(&d_humans.get("bob").await.unwrap().unwrap()).unwrap();
    assert_eq!((listed.check_key("bob"), listed.human), (Ok(()), bob.clone()));
    let keys: Vec<String> = d_humans.keys().await.unwrap().try_collect().await.unwrap();
    assert_eq!(keys, ["alice", "bob"]);
    let forged = serde_json::to_vec(&ants_proto::HumanEntry::new(dev_a.clone())).unwrap();
    assert!(d_humans.put("dev-a", forged.into()).await.is_err(), "an ant cannot list itself");
    assert!(d_humans.delete("alice").await.is_err());

    // Both watch all traffic and read dead letters.
    let mut a_all = a_client.subscribe(subject::ALL).await.unwrap();
    let mut b_all = b_client.subscribe(subject::ALL).await.unwrap();
    for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
        subscribed(js, human).await;
    }
    assert!(dead::list(&b_stream).await.unwrap().is_empty());

    // Each reads its replies in its own inbox, never in the other's.
    let (to_alice, to_bob) =
        (Envelope::new(dev_a.clone(), alice.clone(), "a"), Envelope::new(dev_a.clone(), bob.clone(), "b"));
    bus::publish(&d_js, &to_alice).await.unwrap();
    bus::publish(&d_js, &to_bob).await.unwrap();
    for all in [&mut a_all, &mut b_all] {
        assert_eq!(timeout(WAIT, all.next()).await.unwrap().unwrap().subject.as_str(), to_alice.subject());
    }
    for (js, human, other, sent) in [(&a_js, &alice, &bob, &to_alice), (&b_js, &bob, &alice, &to_bob)] {
        let theirs = async { crate::inbox::Inbox::open(js, other).await?.unread(&Default::default()).await };
        assert!(theirs.await.is_err(), "{human} reads {other}'s inbox");
        let mut inbox = crate::inbox::Inbox::open(js, human).await.unwrap();
        let unread = inbox.unread(&crate::inbox::Select::default()).await.unwrap();
        assert_eq!(unread.as_slice(), std::slice::from_ref(sent));
    }

    // Either human decides dev-a's approval requests.
    let request = ants_proto::Uuid::now_v7();
    let mut decisions = d_client.subscribe(subject::approval_decision(&dev_a, &request)).await.unwrap();
    subscribed(&d_js, &dev_a).await;
    a_client.publish(subject::approval_decision(&dev_a, &request), "allow".into()).await.unwrap();
    a_client.flush().await.unwrap();
    b_client.publish(subject::approval_decision(&dev_a, &request), "reject".into()).await.unwrap();
    // Published on different connections, so heard in either order.
    let mut heard = Vec::new();
    for _ in 0..2 {
        heard.push(timeout(WAIT, decisions.next()).await.unwrap().unwrap().payload);
    }
    heard.sort();
    assert_eq!(heard, [&b"allow"[..], b"reject"]);
}

#[tokio::test]
async fn only_a_human_creates_and_reads_its_own_notification_consumer() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let config = |of: &UserId| jetstream::consumer::pull::Config {
        durable_name: Some(ants_proto::notify_consumer(of)),
        filter_subjects: vec![subject::ALL_APPROVAL_REQUESTS.to_owned(), subject::inbox_of(of)],
        ..Default::default()
    };
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let a_stream = a_js.get_stream(STREAM).await.unwrap();
    let mine: jetstream::consumer::PullConsumer = a_stream.create_consumer(config(&alice)).await.unwrap();
    let one = jetstream::consumer::pull::Config { filter_subjects: vec![subject::inbox_of(&alice)], ..config(&alice) };
    a_stream.create_consumer::<jetstream::consumer::pull::Config>(one).await.unwrap();
    let (_, d_js) = server.connect(&dev_a).await;
    bus::publish_json(&d_js, subject::inbox(&alice, &dev_a), &Envelope::new(dev_a.clone(), alice.clone(), "hi"), None)
        .await
        .unwrap();
    let msg = mine.messages().await.unwrap().next().await.unwrap().unwrap();
    msg.ack().await.unwrap();

    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    let b_stream = b_js.get_stream(STREAM).await.unwrap();
    assert!(b_stream.create_consumer::<jetstream::consumer::pull::Config>(config(&alice)).await.is_err());
    let d_stream = d_js.get_stream_no_info(STREAM).await.unwrap();
    assert!(d_stream.create_consumer::<jetstream::consumer::pull::Config>(config(&dev_a)).await.is_err());
}

#[tokio::test]
async fn thread_notes_are_read_back_through_each_reader_own_consumer() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (b, b_js) = server.connect(&dev_b).await;
    let (thread, other): (ThreadId, ThreadId) = ("t1".parse().unwrap(), "t2".parse().unwrap());

    let first = NotesPost::spend(thread.clone(), dev_a.clone(), "draft", 8, None).unwrap();
    assert!(notes::publish(&a_js, &first, Some(0)).await.unwrap());
    // dev-b spends the same turn without having seen dev-a's post: refused, the notes moved on.
    let stale = NotesPost::spend(thread.clone(), dev_b.clone(), "me first", 8, None).unwrap();
    assert!(!notes::publish(&b_js, &stale, Some(0)).await.unwrap());
    // dev-a may only post on its own subject, so a post naming dev-b as its author is dropped by readers.
    let forged = NotesPost::new(thread.clone(), dev_b.clone(), "dev-b agrees");
    bus::publish_json(&a_js, subject::notes(&thread, &dev_a), &forged, None).await.unwrap();
    notes::publish(&a_js, &NotesPost::new(other, dev_a.clone(), "elsewhere"), None).await.unwrap();

    let seen = notes::read(&b_js, &dev_b, Reading::Consumer, &thread).await.unwrap();
    assert_eq!(seen.posts, std::slice::from_ref(&first));
    let answer = NotesPost::spend(thread.clone(), dev_b.clone(), "agreed", 8, seen.last()).unwrap();
    assert!(notes::publish(&b_js, &answer, Some(seen.last_seq)).await.unwrap());

    // While dev-b's daemon reads through notes-dev-b, dev-b's CLI reads through its spares and the human's by message
    // gets, more at once than there are spares: all read the whole notes, and the daemon's read carries on.
    let daemon = jetstream::consumer::pull::Config {
        name: Some(notes_consumer(&dev_b)),
        filter_subject: subject::notes_of(&thread),
        ack_policy: jetstream::consumer::AckPolicy::None,
        ..Default::default()
    };
    let daemon: jetstream::consumer::PullConsumer = b_js.create_consumer_on_stream(daemon, STREAM).await.unwrap();
    let next = || async {
        let msg = daemon.fetch().max_messages(1).messages().await.unwrap().next().await.unwrap().unwrap();
        serde_json::from_slice::<NotesPost>(&msg.payload).unwrap()
    };
    assert_eq!(next().await, first);
    let reads = (0..6).map(|i| match i % 2 {
        0 => notes::read(&h_js, &human, Reading::Gets, &thread),
        _ => notes::read(&b_js, &dev_b, Reading::Spare, &thread),
    });
    for read in futures::future::join_all(reads).await {
        let read = read.unwrap();
        assert_eq!((&read.posts[..], read.last_seq), (&[first.clone(), answer.clone()][..], seen.last_seq + 2));
    }
    assert_eq!(next().await, forged, "the daemon's read carries on where it was");
    // The human's reads go by batched direct gets, which no ant's credentials allow.
    let batch = format!(r#"{{"batch":2,"next_by_subj":"{}"}}"#, subject::notes_of(&thread));
    let got = h.request(format!("$JS.API.DIRECT.GET.{STREAM}"), batch.clone().into()).await.unwrap();
    assert!(got.headers.unwrap().get("Nats-Num-Pending").is_some(), "a batch");
    let denied = b.request(format!("$JS.API.DIRECT.GET.{STREAM}"), batch.into());
    assert!(!matches!(timeout(Duration::from_secs(1), denied).await, Ok(Ok(_))), "an ant gets no batch");

    // dev-a's notes consumer filters thread notes only, and dev-b's is not dev-a's to use.
    let consumer = |name: String, filter: String| jetstream::consumer::pull::Config {
        name: Some(name),
        filter_subject: filter,
        ..Default::default()
    };
    let into_inbox = consumer(notes_consumer(&dev_a), subject::inbox_of(&dev_b));
    assert!(a_js.create_consumer_on_stream(into_inbox, STREAM).await.is_err());
    let not_its_own = consumer(notes_consumer(&dev_b), subject::notes_of(&thread));
    assert!(a_js.create_consumer_on_stream(not_its_own, STREAM).await.is_err());
    assert!(notes::read(&a_js, &dev_a, Reading::Consumer, &thread).await.is_ok(), "its own works");
    let spare = |name: String| consumer(name, subject::notes_of(&thread));
    let not_its_spare = spare(ants_proto::spare_readers(&notes_consumer(&dev_b)).next().unwrap());
    assert!(a_js.create_consumer_on_stream(not_its_spare, STREAM).await.is_err());
    let into_inbox =
        consumer(ants_proto::spare_readers(&notes_consumer(&dev_a)).next().unwrap(), subject::inbox_of(&dev_b));
    assert!(a_js.create_consumer_on_stream(into_inbox, STREAM).await.is_err());
}

#[tokio::test]
async fn an_ant_cli_reads_back_only_its_own_inbox_through_spares_of_its_inbox_consumer() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    // dev-a's daemon takes its inbox through inbox-dev-a.
    let mut daemon =
        bus::inbox_consumer(&bus::open_stream(&a_js, &dev_a).await.unwrap(), &dev_a, WAIT, 5, 1).await.unwrap();
    let to_a = Envelope::new(dev_b.clone(), dev_a.clone(), "for dev-a");
    bus::publish(&b_js, &to_a).await.unwrap();
    bus::publish(&a_js, &Envelope::new(dev_a.clone(), dev_b.clone(), "for dev-b")).await.unwrap();

    // dev-a's CLI reads its own inbox back, ids and all, more reads at once than there are spares.
    let (inbox, filter) = (ants_proto::inbox_consumer(&dev_a), subject::inbox_of(&dev_a));
    let reads = (0..2 * ants_proto::SPARE_READERS).map(|_| async {
        let mut got = Vec::new();
        let all = jetstream::consumer::DeliverPolicy::All;
        ants_daemon::reading::read_back_messages(&a_js, &inbox, Reading::Spare, &filter, all, |_, _, msg| {
            got.push(msg);
            std::ops::ControlFlow::Continue(())
        })
        .await
        .map(|()| got)
    });
    for read in futures::future::join_all(reads).await {
        let [msg] = &read.unwrap()[..] else { panic!("one message to dev-a") };
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), to_a);
        let id = msg.headers.as_ref().and_then(|h| h.get(async_nats::header::NATS_MESSAGE_ID));
        assert_eq!(id.map(|id| id.as_str().to_owned()), Some(to_a.id.to_string()));
    }
    let msg = timeout(WAIT, daemon.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), to_a, "the daemon still takes it");

    // The spares filter dev-a's own inbox only, and dev-b's are not dev-a's.
    let spare = |of: &UserId, filter: String| jetstream::consumer::pull::Config {
        name: ants_proto::spare_readers(&ants_proto::inbox_consumer(of)).next(),
        filter_subject: filter,
        ..Default::default()
    };
    for (of, filter) in [
        (&dev_a, subject::inbox_of(&dev_b)),
        (&dev_a, subject::ALL_NOTES.to_owned()),
        (&dev_b, subject::inbox_of(&dev_b)),
    ] {
        let config = spare(of, filter.clone());
        assert!(a_js.create_consumer_on_stream(config, STREAM).await.is_err(), "{of}'s spare on {filter}");
    }
}

#[tokio::test]
async fn an_ant_reads_back_its_thread_both_ways_and_only_its_own_sent_messages() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    let (thread, ops): (ThreadId, ChannelId) = (ThreadId::generate(), "ops".parse().unwrap());
    bus::publish(&b_js, &Envelope::on_thread(thread.clone(), dev_b.clone(), dev_a.clone(), "status?")).await.unwrap();
    bus::publish(&a_js, &Envelope::on_thread(thread.clone(), dev_a.clone(), dev_b.clone(), "green")).await.unwrap();
    bus::publish(&a_js, &Envelope::new(dev_a.clone(), dev_b.clone(), "elsewhere")).await.unwrap();
    notes::publish(&b_js, &NotesPost::new(thread.clone(), dev_b.clone(), "noted"), None).await.unwrap();
    let post = ChannelPost::on_thread(ops.clone(), thread.clone(), dev_b.clone(), "shipped");
    bus::publish_json(&b_js, subject::chan(&ops, &dev_b), &post, None).await.unwrap();

    let read = ants_daemon::history::Thread {
        js: a_js.clone(),
        ant: dev_a.clone(),
        thread: thread.clone(),
        channel: Some(ops),
        before: u64::MAX,
    };
    assert_eq!(
        read.history()(ants_daemon::history::DEFAULT_BUDGET).await,
        "[ants] The thread's history, read back from ants, oldest first:\n\ndev-b to dev-a:\nstatus?\n\n\
             dev-a to dev-b:\ngreen\n\ndev-b in the thread notes:\nnoted\n\ndev-b in channel ops:\nshipped\n\n\
             [ants] End of the thread's history."
    );

    // The sent reader filters dev-a's own sent messages only, and is not dev-b's to use.
    let reader = |name: String, filter: String| jetstream::consumer::pull::Config {
        name: Some(name),
        filter_subject: filter,
        ..Default::default()
    };
    let sent = ants_proto::sent_reader(&dev_a);
    assert!(a_js.create_consumer_on_stream(reader(sent.clone(), subject::inbox_of(&dev_b)), STREAM).await.is_err());
    assert!(b_js.create_consumer_on_stream(reader(sent, subject::inbox_from(&dev_a)), STREAM).await.is_err());
}

#[tokio::test]
async fn ants_write_their_own_roster_entry_and_everyone_reads_the_roster() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (UserId::human(), p("dev-a"));
    let ttl = ants_proto::ROSTER_TTL;

    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_roster(&a_js, ttl).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&human).await;
    let h_kv = bus::ensure_roster(&h_js, ttl).await.unwrap();

    let a_kv = bus::ensure_roster(&a_js, ttl).await.unwrap();
    let now = chrono::Utc::now();
    let entry = RosterEntry {
        ant: dev_a.clone(),
        machine: "m1".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: vec!["rust".to_owned()],
        whoami: None,
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
    };
    let registration = Registration::start(a_kv.clone(), entry.clone(), ants_proto::ROSTER_HEARTBEAT).await.unwrap();
    assert!(a_kv.put("dev-b", "spoof".into()).await.is_err(), "dev-a cannot list dev-b");
    // Ants read the roster, to route requests by capability.
    let (_b, b_js) = server.connect(&p("dev-b")).await;
    let b_kv = bus::ensure_roster(&b_js, ttl).await.unwrap();
    // dev-b may write its own key only, and an entry there naming dev-a is skipped.
    let forged = RosterEntry { capabilities: vec!["deploy".to_owned()], ..entry.clone() };
    b_kv.put("dev-b", serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
    for kv in [&b_kv, &h_kv] {
        assert_eq!(ants_daemon::roster::list_entries(kv).await.unwrap(), std::slice::from_ref(&entry));
    }
    b_kv.delete("dev-b").await.unwrap();

    let listed: RosterEntry = serde_json::from_slice(&h_kv.get("dev-a").await.unwrap().unwrap()).unwrap();
    assert_eq!(listed, entry);
    let keys: Vec<String> = h_kv.keys().await.unwrap().try_collect().await.unwrap();
    assert_eq!(keys, ["dev-a"]);
    let mut watch = h_kv.watch_with_history(">").await.unwrap();
    let seen = timeout(WAIT, watch.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(seen.key, "dev-a");
    assert!(h_kv.put("dev-a", "spoof".into()).await.is_err(), "the human does not write the roster");

    registration.deregister().await;
    assert!(h_kv.get("dev-a").await.unwrap().is_none());
}

#[tokio::test]
async fn ants_warn_a_human_once_per_crossing_of_a_storage_limit() {
    use ants_daemon::storage::{self, Settings, Watcher};
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    bus::init_own_buckets(&h_js).await.unwrap();
    h_js.publish(subject::inbox(&dev_a, &human), vec![b'x'; 4096].into()).await.unwrap().await.unwrap();
    let used = storage::measure(&h_js, None, &Settings::default()).await.unwrap()[0].used;
    let watcher = async |ant: &UserId, max_file_store: u64| {
        let (_, js) = server.connect(ant).await;
        Watcher {
            notes: bus::ensure_sessions(&js, None, ant).await.unwrap(),
            js,
            hub_domain: None,
            ant: ant.clone(),
            human: human.clone(),
            settings: Settings { max_file_store: Some(max_file_store), ..Settings::default() },
            server: String::new(),
        }
    };
    let (a, b) = (watcher(&dev_a, used + used / 10).await, watcher(&dev_b, used + used / 10).await);
    let mut warnings = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    h_client.flush().await.unwrap();
    let note = ants_proto::storage_warning_key(None, "file");

    tokio::join!(a.check(), b.check());
    a.check().await;
    let got = timeout(WAIT, warnings.next()).await.unwrap().unwrap();
    let warning: Envelope = serde_json::from_slice(&got.payload).unwrap();
    assert_eq!(warning.escalation.map(|e| e.reason), Some(ants_proto::EscalationReason::Storage));
    assert!(warning.text.contains("JetStream file store of the hub") && warning.text.contains("Using most: "));
    assert!(timeout(Duration::from_millis(300), warnings.next()).await.is_err(), "one warning per crossing");
    assert!(a.notes.get(&note).await.unwrap().is_some());

    // Well under the share the note goes, and the next crossing warns again.
    watcher(&dev_a, used * 100).await.check().await;
    assert!(a.notes.get(&note).await.unwrap().is_none());
    b.check().await;
    timeout(WAIT, warnings.next()).await.expect("a warning of the next crossing");
}

#[tokio::test]
async fn ants_keep_their_own_escalation_chain_and_read_everyone_s() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, lead) = (UserId::human(), p("dev-a"), p("lead"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_chains(&a_js).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&human).await;
    let h_kv = bus::ensure_chains(&h_js).await.unwrap();

    let (_l, l_js) = server.connect(&lead).await;
    ants_daemon::chains::publish(&l_js, &lead, std::slice::from_ref(&dev_a)).await.unwrap();
    let a_kv = bus::ensure_chains(&a_js).await.unwrap();
    assert_eq!(ants_daemon::chains::read(&a_kv, &lead).await.unwrap(), Some(vec![dev_a.clone()]));
    assert_eq!(ants_daemon::chains::read(&h_kv, &lead).await.unwrap(), Some(vec![dev_a.clone()]));

    // dev-a writes its own key only, and an entry there naming lead is skipped.
    assert!(a_kv.put("lead", "spoof".into()).await.is_err(), "dev-a cannot rewrite lead's chain");
    let forged = ants_proto::ChainEntry::new(lead.clone(), vec![human.clone()]);
    a_kv.put("dev-a", serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
    assert_eq!(ants_daemon::chains::read(&h_kv, &dev_a).await.unwrap(), None);
    assert!(h_kv.put("lead", "spoof".into()).await.is_err(), "the human does not write chains");
}

#[tokio::test]
async fn ants_record_the_links_they_pass_on_and_read_everyone_s() {
    let Some(server) = Server::start() else { return };
    let (dev_a, dev_b, lead) = (p("dev-a"), p("dev-b"), p("lead"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_links(&a_js).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&UserId::human()).await;
    let h_kv = bus::ensure_links(&h_js).await.unwrap();

    let origin = ThreadId::generate();
    let spin = |from: &UserId, to: &UserId| {
        Envelope::on_thread(origin.clone(), from.clone(), to.clone(), "")
            .with_creator(Some(lead.clone()))
            .spun_off(ThreadId::generate())
    };
    let spun = spin(&dev_a, &dev_b);
    ants_daemon::links::pass_on(&a_js, &spun).await.unwrap();
    let a_kv = a_js.get_key_value(ants_proto::LINKS_BUCKET).await.unwrap();
    for forged in [link_key(&lead, &origin, &dev_b), link_key(&dev_b, &origin, &dev_a)] {
        assert!(a_kv.put(forged, "spoof".into()).await.is_err(), "dev-a records its own links only");
    }
    assert!(h_kv.put(link_key(&lead, &origin, &dev_a), "spoof".into()).await.is_err(), "the human records none");

    let (_b, b_js) = server.connect(&dev_b).await;
    for js in [&a_js, &b_js, &h_js] {
        assert_eq!(ants_daemon::links::passed_on(js, &origin).await.unwrap(), [(dev_a.clone(), dev_b.clone())]);
    }
    assert!(ants_daemon::links::passed_on(&b_js, &ThreadId::generate()).await.unwrap().is_empty());
    let passed = ants_daemon::links::Passed { from: dev_a, to: dev_b, on: Some(spun.thread_id) };
    assert_eq!(ants_daemon::links::passed(&h_js, &origin).await.unwrap(), [passed], "with the thread it went on");
}

#[tokio::test]
async fn senders_store_their_own_attachments_and_ants_read_those_of_the_messages_they_read() {
    use ants_daemon::files;
    use ants_proto::{AttachmentHandle, AttachmentScope};

    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b, dev_c) = (UserId::human(), p("dev-a"), p("dev-b"), p("dev-c"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(a_js.create_stream(ants_daemon::hub::files_config()).await.is_err(), "only a human creates it");
    let (_h, h_js) = server.connect(&human).await;
    h_js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
    let (_b, b_js) = server.connect(&dev_b).await;
    let (_c, c_js) = server.connect(&dev_c).await;

    let store = async |js: &jetstream::Context, from: &UserId, scope: AttachmentScope| {
        let attachment = files::reference("report.txt", "text/plain", b"findings").unwrap();
        let handle = AttachmentHandle { from: from.clone(), scope, id: attachment.id };
        files::put(js, &handle, &attachment, b"findings").await.unwrap();
        handle
    };
    let to_b = store(&a_js, &dev_a, AttachmentScope::Inbox(dev_b.clone())).await;
    let notes = store(&a_js, &dev_a, AttachmentScope::Notes(ThreadId::generate())).await;
    let channel: ChannelId = "ops".parse().unwrap();
    let chan = store(&b_js, &dev_b, AttachmentScope::Chan(channel)).await;
    let from_human = store(&h_js, &human, AttachmentScope::Inbox(dev_a.clone())).await;
    for (js, handle) in [(&b_js, &to_b), (&a_js, &to_b), (&h_js, &to_b), (&c_js, &notes), (&c_js, &chan)] {
        assert_eq!(files::get(js, handle, None).await.unwrap().1, b"findings", "{handle}");
    }
    assert_eq!(files::get(&a_js, &from_human, None).await.unwrap().1, b"findings");
    assert!(files::get(&c_js, &to_b, None).await.is_err(), "dev-c reads no messages between dev-a and dev-b");

    let (raw, mut errors) = server.watched(&dev_c).await;
    let theirs = format!("$JS.API.DIRECT.GET.{}.{}", ants_proto::FILES_STREAM, to_b.chunk_subject(0));
    raw.publish(theirs.clone(), "".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&theirs), "reading others' attachments is denied");
    let forged = AttachmentHandle { from: dev_a.clone(), ..to_b.clone() }.chunk_subject(1);
    raw.publish(forged.clone(), "x".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&forged), "storing as another sender is denied");
}

#[tokio::test]
async fn humans_write_only_their_own_reactions_and_ants_change_theirs_through_ants_signer() {
    use ants_daemon::reactions::{self, Answering, Reacted};
    use ants_proto::{ReactionChange, ReactionsOutcome, ReactionsRequest};

    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (eyes, check): (ants_proto::Reaction, ants_proto::Reaction) =
        ("eyes".parse().unwrap(), "check".parse().unwrap());
    let (a_client, a_js) = server.connect(&dev_a).await;
    assert!(a_js.create_stream(ants_daemon::hub::reactions_config()).await.is_err(), "only a human creates it");
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
    let (to_a, between) = (
        Envelope::new(human.clone(), dev_a.clone(), "for dev-a"),
        Envelope::new(human.clone(), dev_b.clone(), "for dev-b"),
    );
    for envelope in [&to_a, &between] {
        bus::publish(&h_js, envelope).await.unwrap();
    }
    let message = to_a.id;
    let mut heard = h_client.subscribe(subject::ALL).await.unwrap();
    h_client.flush().await.unwrap();

    assert!(reactions::add(&h_js, None, &human, &message, &eyes).await.unwrap());
    assert!(reactions::add(&h_js, None, &dev_a, &message, &check).await.is_err(), "a human reacts as nobody else");
    // dev-a neither writes reactions, not even its own, nor reads them.
    assert!(reactions::add(&a_js, None, &dev_a, &message, &eyes).await.is_err());
    assert!(reactions::load(&a_js, &[message]).await.is_err(), "an ant reads no reactions");
    let (raw, mut errors) = server.watched(&dev_a).await;
    for key in [format!("dev-a.{message}.x"), format!("dev-b.{message}.x")] {
        let forged = format!("$KV.ants_reactions.{key}");
        raw.publish(forged.clone(), "{}".into()).await.unwrap();
        assert!(next_violation(&raw, &mut errors).await.contains(&forged), "a direct reaction is denied");
    }
    let theirs = format!("$KV.ants_reactions.{human}.{message}.eyes");
    let mut delete = async_nats::HeaderMap::new();
    delete.insert("KV-Operation", "DEL");
    raw.publish_with_headers(theirs.clone(), delete, "".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&theirs), "removing another's reaction is denied");
    let quiet = timeout(Duration::from_millis(300), heard.next()).await;
    assert!(quiet.is_err(), "a reaction is no message: {quiet:?}");

    // `ants signer`'s credentials answer an ant's requests, for the messages it reads only.
    let approver =
        crate::enroll::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, WAIT).await.unwrap();
    let serving = tokio::spawn(reactions::serve(
        approver.client.clone(),
        approver.hub().clone(),
        None,
        Answering::ListsAndChanges,
    ));
    let change = |message, reaction: &ants_proto::Reaction, remove| {
        ReactionsRequest::change(dev_a.clone(), message, ReactionChange { reaction: reaction.clone(), remove })
    };
    let applied = |changed| Some(ReactionsOutcome::Applied { changed });
    assert_eq!(reactions::request(&a_client, &change(message, &eyes, false)).await.unwrap(), applied(true));
    assert_eq!(reactions::request(&a_client, &change(message, &check, false)).await.unwrap(), applied(true));
    let unread = reactions::request(&a_client, &change(between.id, &eyes, false)).await.unwrap();
    assert_eq!(unread, Some(ReactionsOutcome::Unknown), "dev-a reads no message to dev-b");
    let expected = vec![
        Reacted { reaction: check.clone(), by: vec![dev_a.clone()] },
        Reacted { reaction: eyes.clone(), by: vec![dev_a.clone(), human.clone()] },
    ];
    let listed = reactions::ask(&a_client, &dev_a, message).await.unwrap();
    assert_eq!(listed, Some(ReactionsOutcome::Listed { reactions: expected.clone() }));
    assert_eq!(reactions::load(&h_js, &[message]).await.unwrap()[&message], expected);
    assert!(!reactions::load(&h_js, &[between.id]).await.unwrap().contains_key(&between.id));
    assert_eq!(reactions::request(&a_client, &change(message, &eyes, true)).await.unwrap(), applied(true));
    assert_eq!(reactions::request(&a_client, &change(message, &eyes, true)).await.unwrap(), applied(false));
    let all = reactions::load(&h_js, &[message]).await.unwrap();
    assert_eq!(all[&message][1], Reacted { reaction: eyes, by: vec![human] });
    serving.abort();
}

#[tokio::test]
async fn any_human_titles_a_thread_and_keeps_its_tldr_but_each_marks_only_its_own_threads() {
    use crate::thread_meta::{MISSING, Meta};

    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let thread = ThreadId::generate();
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    let missing = Meta::open(&a_js, None, &alice).await.unwrap().title(&thread).await.unwrap_err();
    assert_eq!(missing.to_string(), MISSING, "no bucket before `ants init`");
    assert!(d_js.create_stream(ants_daemon::hub::threads_config()).await.is_err(), "only a human creates it");
    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let (alice_meta, bob_meta) =
        (Meta::open(&a_js, None, &alice).await.unwrap(), Meta::open(&b_js, None, &bob).await.unwrap());

    assert_eq!(alice_meta.set_title(&thread, "  Release 1.2 ").await.unwrap(), None);
    let had = bob_meta.set_title(&thread, "Release 1.2.1").await.unwrap().unwrap();
    assert_eq!((had.title.as_str(), &had.by), ("Release 1.2", &alice), "bob renames alice's title");
    let title = alice_meta.title(&thread).await.unwrap().unwrap();
    assert_eq!((title.title.as_str(), &title.by), ("Release 1.2.1", &bob));
    assert!(alice_meta.set_title(&thread, "two\nlines").await.is_err());
    let ant = Meta::open(&d_js, None, &dev_a).await.unwrap();
    assert!(ant.title(&thread).await.is_err(), "an ant reads no title");
    assert!(ant.set_title(&thread, "mine").await.is_err(), "nor sets one");
    let tldr = ants_proto::ThreadSummary {
        thread_id: thread.clone(),
        text: "Release 1.2.1 is out.".to_owned(),
        by: dev_a.clone(),
        asker: alice.clone(),
        at: chrono::Utc::now(),
        upto: 7,
        tokens: 42,
    };
    assert!(alice_meta.put_summary(&tldr).await.unwrap());
    assert_eq!(bob_meta.summary(&thread).await.unwrap(), Some(tldr), "every human reads the TL;DR");
    assert!(ant.summary(&thread).await.is_err(), "an ant reads no TL;DR");
    assert!(alice_meta.set_done(&thread, 1).await.unwrap());
    assert!(bob_meta.clear_done(&thread).await.unwrap().is_some_and(|d| d.by == alice), "bob clears alice's done");
    assert!(ant.done(&thread).await.is_err(), "an ant reads no done state");
    assert!(ant.set_done(&thread, 1).await.is_err(), "nor marks a thread done");

    assert!(alice_meta.change_marks(|m| m.pin(&thread)).await.unwrap());
    assert!(!alice_meta.change_marks(|m| m.pin(&thread)).await.unwrap(), "pinned already");
    assert!(bob_meta.change_marks(|m| m.archive(&thread, 1)).await.unwrap());
    assert!(alice_meta.marks().await.unwrap().pinned.contains_key(&thread), "bob's archive is bob's");
    assert!(bob_meta.change_marks(|m| m.unarchive(&thread)).await.unwrap());
    assert!(alice_meta.change_marks(|m| m.unpin(&thread)).await.unwrap());
    assert_eq!(alice_meta.marks().await.unwrap(), ants_proto::ThreadMarks::default());
    assert!(Meta::open(&a_js, None, &bob).await.unwrap().marks().await.is_err(), "alice reads no marks of bob's");
    assert!(Meta::open(&d_js, None, &dev_a).await.unwrap().marks().await.is_err(), "an ant keeps none");

    let (raw, mut errors) = server.watched_with(&alice, &server.creds_as(&alice, Role::Human)).await;
    let forged = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::marks_key(&bob));
    raw.publish(forged.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&forged), "forging bob's marks is denied");
    let title = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::title_key(&thread));
    let (raw, mut errors) = server.watched(&dev_a).await;
    raw.publish(title.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&title), "an ant's title is denied");
    let tldr = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::summary_key(&thread));
    raw.publish(tldr.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&tldr), "an ant's TL;DR is denied");
    let done = format!("$KV.{}.{}", ants_proto::THREADS_BUCKET, ants_proto::done_key(&thread));
    raw.publish(done.clone(), "{}".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&done), "an ant's done state is denied");
}

#[tokio::test]
async fn a_human_adopts_an_ant_s_title_only_on_a_thread_never_titled() {
    use crate::thread_meta::Meta;

    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    a_js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let meta = Meta::open(&a_js, None, &alice).await.unwrap();
    let (fresh, titled, cleared) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());

    let adopted = meta.adopt_title(&fresh, " Fix the build ", &dev_a).await.unwrap().unwrap();
    assert_eq!((adopted.title.as_str(), &adopted.by), ("Fix the build", &dev_a));
    assert_eq!(meta.title(&fresh).await.unwrap(), Some(adopted));
    assert_eq!(meta.adopt_title(&fresh, "Something else", &dev_a).await.unwrap(), None, "adopted once");
    assert!(meta.adopt_title(&ThreadId::generate(), "two\nlines", &dev_a).await.is_err());

    meta.set_title(&titled, "Release 1.2").await.unwrap();
    assert_eq!(meta.adopt_title(&titled, "Fix the build", &dev_a).await.unwrap(), None);
    assert_eq!(meta.title(&titled).await.unwrap().unwrap().by, alice, "a human's title wins");

    meta.set_title(&cleared, "Release 1.2").await.unwrap();
    meta.clear_title(&cleared).await.unwrap();
    assert_eq!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap(), None);
    assert_eq!(meta.title(&cleared).await.unwrap(), None, "a cleared title stays cleared");

    let ant = Meta::open(&d_js, None, &dev_a).await.unwrap();
    assert!(ant.adopt_title(&ThreadId::generate(), "mine", &dev_a).await.is_err(), "an ant adopts none");
}

#[tokio::test]
async fn only_humans_and_webhooks_to_ants_record_a_thread_s_owner_and_the_first_record_stays() {
    let Some(server) = Server::start() else { return };
    let (human, alice, dev_a, hook, alerts) = (UserId::human(), p("alice"), p("dev-a"), p("github"), p("alerts"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_owners(&a_js).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&human).await;
    bus::ensure_owners(&h_js).await.unwrap();
    let issue = |webhook: &UserId, to: Destination| {
        let creds = server.dir().join(format!("{webhook}.creds"));
        issue_with(
            &server.dir().join("auth"),
            webhook,
            crate::revoke::Kind::Webhook,
            Permissions::for_webhook(webhook, &[to]),
            &creds,
        )
        .unwrap();
        creds
    };
    let hook_creds = issue(&hook, Destination::Ant(dev_a.clone()));
    let alerts_creds = issue(&alerts, Destination::Channel("ops".parse().unwrap()));

    let (asked, delivered, forged) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let dev_b = p("dev-b");
    owners::record(&h_js, None, &asked, &dev_a, &human).await.unwrap();
    owners::record(&h_js, None, &asked, &dev_b, &alice).await.unwrap();
    let hook_js = connect_to(&server.url, &hook_creds, None, &hook).await;
    owners::record(&hook_js, None, &delivered, &dev_a, &alice).await.unwrap();
    owners::record(&hook_js, None, &asked, &dev_a, &alice).await.unwrap();
    assert!(
        owners::record(&hook_js, None, &forged, &dev_b, &alice).await.is_err(),
        "a webhook records for its ants only"
    );
    assert!(owners::record(&a_js, None, &forged, &dev_a, &alice).await.is_err(), "an ant records no owner");
    let alerts_js = connect_to(&server.url, &alerts_creds, None, &alerts).await;
    assert!(owners::record(&alerts_js, None, &forged, &dev_a, &alice).await.is_err(), "nor a webhook to channels");

    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let denied = format!("$KV.{}.{}", ants_proto::OWNERS_BUCKET, ants_proto::owner_key(&forged, &dev_a));
    a_raw.publish(denied.clone(), "alice".into()).await.unwrap();
    let v = next_violation(&a_raw, &mut errors).await;
    assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    // A webhook that records owners lists no threads of others, which stream info of the bucket would.
    let (hook_raw, mut hook_errors) = server.watched_with(&hook, &hook_creds).await;
    let info = format!("$JS.API.STREAM.INFO.KV_{}", ants_proto::OWNERS_BUCKET);
    hook_raw.publish(info.clone(), br#"{"subjects_filter":">"}"#[..].into()).await.unwrap();
    let v = next_violation(&hook_raw, &mut hook_errors).await;
    assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");
    let read = owners::Owners::new(a_js);
    assert_eq!(read.recorded(&asked, &dev_a).await.unwrap(), Some(human), "the first record stays");
    assert_eq!(read.recorded(&asked, &dev_b).await.unwrap(), Some(alice.clone()));
    assert_eq!(read.recorded(&delivered, &dev_a).await.unwrap(), Some(alice));
    for to in [&dev_a, &dev_b] {
        assert_eq!(read.recorded(&forged, to).await.unwrap(), None);
    }
    assert_eq!(read.recorded(&delivered, &dev_b).await.unwrap(), None);
}

#[tokio::test]
async fn a_webhook_posts_as_itself_to_its_destinations_only_and_reads_no_message() {
    let Some(server) = Server::start() else { return };
    let (hook, dev_a, ops) = (p("github"), p("dev-a"), "ops".parse::<ChannelId>().unwrap());
    let creds = server.dir().join("github.creds");
    let destinations = [Destination::Ant(dev_a.clone()), Destination::Channel(ops.clone())];
    issue_with(
        &server.dir().join("auth"),
        &hook,
        crate::revoke::Kind::Webhook,
        Permissions::for_webhook(&hook, &destinations),
        &creds,
    )
    .unwrap();
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    crate::webhook::ensure_bucket(&h_js).await.unwrap();

    let (raw, mut errors) = server.watched_with(&hook, &creds).await;
    for allowed in [subject::inbox(&dev_a, &hook), subject::chan(&ops, &hook), "$KV.ants_webhooks.github.x".to_owned()]
    {
        raw.publish(allowed, "ok".into()).await.unwrap();
    }
    for denied in [
        subject::inbox(&p("dev-b"), &hook),
        subject::inbox(&dev_a, &p("dev-b")),
        subject::chan(&"dev".parse().unwrap(), &hook),
        "ants.out.inbox.dev-a.github".to_owned(),
        "$KV.ants_channels.ops".to_owned(),
        "$KV.ants_humans.github".to_owned(),
        "$KV.ants_webhooks.gitlab.x".to_owned(),
        subject::notes(&"t1".parse().unwrap(), &hook),
    ] {
        raw.publish(denied.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    for denied in [subject::ALL, "_INBOX.dev-a.>"] {
        let _sub = raw.subscribe(denied).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
    // Nor does it list the subjects of the stream or the bucket: who messages whom, other webhooks' events.
    for info in ["$JS.API.STREAM.INFO.ANTS", "$JS.API.STREAM.INFO.KV_ants_webhooks"] {
        raw.publish(info, br#"{"subjects_filter":">"}"#[..].into()).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{info}\"")), "{v}");
    }

    // Yet it finds JetStream on connecting, and records and reads its own events by key, as `ants webhook serve`.
    let connecting = bus::connect_retrying(&server.url, Some(&creds), None, &hook, "test", std::future::pending());
    let (_, mut hook_js) = timeout(WAIT, connecting).await.expect("github reaches JetStream").unwrap().unwrap();
    hook_js.set_timeout(Duration::from_secs(1));
    let seen = DirectKv::new(&hook_js, None, ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    assert!(seen.exists(&hook).await.unwrap());
    let key = ants_proto::webhook_seen_key(&hook, "e1");
    assert!(seen.create(&key, "first").await.unwrap());
    assert!(!seen.create(&key, "again").await.unwrap(), "an event is recorded once");
    assert_eq!(seen.get(&key).await.unwrap().as_deref(), Some(&b"first"[..]));
    seen.delete(&key).await.unwrap();
    assert!(seen.create(&key, "after").await.unwrap(), "a deleted record is no record");
    assert_eq!(seen.get(&key).await.unwrap().as_deref(), Some(&b"after"[..]));
    assert!(seen.create(ants_proto::webhook_seen_key(&p("gitlab"), "e1"), "x").await.is_err());
    assert!(seen.get(ants_proto::webhook_seen_key(&p("gitlab"), "e1")).await.is_err());

    // Its marker tells ants it is a webhook; they read no event it delivered, and put no marker of their own.
    raw.publish(ants_proto::webhook_marker_subject(&hook), "github".into()).await.unwrap();
    raw.flush().await.unwrap();
    let (_, a_js) = server.connect(&dev_a).await;
    let bucket = a_js.get_stream_no_info(format!("KV_{}", ants_proto::WEBHOOKS_BUCKET)).await.unwrap();
    let marker = bucket.direct_get_last_for_subject(ants_proto::webhook_marker_subject(&hook)).await.unwrap();
    assert_eq!(&marker.payload[..], b"github");
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    for denied in [
        "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.github.x".to_owned(),
        ants_proto::webhook_marker_subject(&dev_a),
    ] {
        a_raw.publish(denied.clone(), "".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
}

#[tokio::test]
async fn an_observer_reads_approvals_dead_letters_status_and_the_roster_and_publishes_nothing() {
    use ants_proto::Feed;
    use async_nats::jetstream::consumer;
    let Some(server) = Server::start() else { return };
    let (audit, dev_a, dev_b) = (p("audit"), p("dev-a"), p("dev-b"));
    let creds = server.dir().join("audit.creds");
    let team = server.dir().join("auth");
    issue_with(&team, &audit, crate::revoke::Kind::Observer, Permissions::for_observer(&audit), &creds).unwrap();
    let text = std::fs::read_to_string(&creds).unwrap();
    assert_eq!(ants_daemon::creds::Finding::from_creds(&text).unwrap().kind, ants_daemon::creds::Kind::Observer);
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    bus::ensure_roster(&h_js, Duration::from_secs(60)).await.unwrap();
    let (a, a_js) = server.connect(&dev_a).await;
    let (_, b_js) = server.connect(&dev_b).await;
    let request = ants_proto::Uuid::now_v7();
    let stored = [
        (&a_js, subject::approvals(&dev_a)),
        (&h_js, subject::approval_decision(&dev_a, &request)),
        (&a_js, subject::dead(&dev_a)),
        (&b_js, subject::inbox(&dev_a, &dev_b)),
        (&a_js, subject::notes(&ThreadId::generate(), &dev_a)),
    ];
    for (js, subject) in &stored {
        js.publish(subject.clone(), "event".into()).await.unwrap().await.unwrap();
    }
    a_js.get_key_value(ants_proto::ROSTER_BUCKET).await.unwrap().put("dev-a", "{}".into()).await.unwrap();

    // It finds JetStream on connecting, reads each feed through its own consumer and watches the roster.
    let connecting = bus::connect_retrying(&server.url, Some(&creds), None, &audit, "test", std::future::pending());
    let (o, mut o_js) = timeout(WAIT, connecting).await.expect("audit reaches JetStream").unwrap().unwrap();
    o_js.set_timeout(Duration::from_secs(1));
    for (feed, want) in [
        (Feed::ApprovalRequests, subject::approvals(&dev_a)),
        (Feed::ApprovalDecisions, subject::approval_decision(&dev_a, &request)),
        (Feed::DeadLetters, subject::dead(&dev_a)),
    ] {
        let name = feed.consumer(&audit);
        let config = consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: feed.filter().to_owned(),
            ..Default::default()
        };
        let reader: consumer::PullConsumer = o_js.create_consumer_on_stream(config, STREAM).await.unwrap();
        let msgs: Vec<_> = reader.fetch().max_messages(10).messages().await.unwrap().try_collect().await.unwrap();
        let subjects: Vec<&str> = msgs.iter().map(|m| m.subject.as_str()).collect();
        assert_eq!(subjects, [want.as_str()], "{feed:?}");
        msgs[0].double_ack().await.unwrap();
        let again: consumer::PullConsumer = o_js.get_consumer_from_stream(&name, STREAM).await.unwrap();
        assert_eq!(again.cached_info().num_ack_pending, 0, "{feed:?}");
        o_js.delete_consumer_from_stream(&name, STREAM).await.unwrap();
    }
    let roster = o_js.get_key_value(ants_proto::ROSTER_BUCKET).await.unwrap();
    assert_eq!(roster.get("dev-a").await.unwrap().as_deref(), Some(&b"{}"[..]));
    let mut watch = roster.watch_all().await.unwrap();
    a_js.get_key_value(ants_proto::ROSTER_BUCKET).await.unwrap().put("dev-a", "{\"a\":1}".into()).await.unwrap();
    let entry = timeout(WAIT, watch.next()).await.expect("no roster entry").unwrap().unwrap();
    assert_eq!(entry.key, "dev-a");
    let mut status = o.subscribe(subject::ALL_STATUS).await.unwrap();
    o.flush().await.unwrap();
    let thread = ThreadId::generate();
    a.publish(subject::status(&dev_a, &thread), "{}".into()).await.unwrap();
    let heard = timeout(WAIT, status.next()).await.expect("no status").unwrap();
    assert_eq!(heard.subject.as_str(), subject::status(&dev_a, &thread));

    // It sends nothing, decides nothing, writes no bucket, and reads no conversation.
    let (raw, mut errors) = server.watched_with(&audit, &creds).await;
    for denied in [
        subject::inbox(&dev_a, &audit),
        subject::inbox(&dev_a, &dev_b),
        subject::outbox(&subject::inbox(&dev_a, &audit)).unwrap(),
        subject::notes(&thread, &audit),
        subject::chan(&"ops".parse().unwrap(), &audit),
        subject::approvals(&audit),
        subject::approval_decision(&dev_a, &request),
        subject::dead(&dev_a),
        subject::status(&audit, &thread),
        subject::cancel(&dev_a, &thread),
        subject::queue(&dev_a),
        format!("$KV.ants_attempts.{}", ants_proto::message_mark_key(&dev_a, &request)),
        subject::renew("dev-a"),
        "$KV.ants_roster.dev-a".to_owned(),
        "$KV.ants_humans.audit".to_owned(),
        "$JS.API.STREAM.INFO.ANTS".to_owned(),
        "$JS.API.STREAM.MSG.GET.ANTS".to_owned(),
        "$JS.API.DIRECT.GET.ANTS".to_owned(),
        format!("$JS.API.DIRECT.GET.ANTS.{}", subject::inbox(&dev_a, &dev_b)),
        // Consumers with another filter, or none in the subject, or another's name.
        format!("$JS.API.CONSUMER.CREATE.ANTS.{}.ants.inbox.>", Feed::DeadLetters.consumer(&audit)),
        "$JS.API.CONSUMER.CREATE.ANTS".to_owned(),
        format!("$JS.API.CONSUMER.CREATE.ANTS.{}", Feed::DeadLetters.consumer(&audit)),
        format!("$JS.API.CONSUMER.CREATE.ANTS.inbox-dev-a.{}", subject::inbox_of(&dev_a)),
        format!("$JS.API.CONSUMER.MSG.NEXT.ANTS.{}", ants_proto::inbox_consumer(&dev_a)),
        "$JS.API.CONSUMER.CREATE.KV_ants_sessions".to_owned(),
        "$JS.API.DIRECT.GET.KV_ants_presence.>".to_owned(),
        "$JS.API.STREAM.INFO.KV_ants_presence".to_owned(),
    ] {
        raw.publish(denied.clone(), "{}".into()).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    for denied in
        [subject::ALL, "ants.inbox.>", subject::ALL_NOTES, subject::ALL_CHANNELS, "ants.cancel.>", "_INBOX.dev-a.>"]
    {
        let _sub = raw.subscribe(denied).await.unwrap();
        let v = next_violation(&raw, &mut errors).await;
        assert!(v.contains(&format!("Subscription to \"{denied}\"")), "{v}");
    }
}

#[tokio::test]
async fn a_hook_runs_once_per_dead_letter_from_where_its_consumer_left_off() {
    use ants_proto::Feed;
    use async_nats::jetstream::consumer;
    let Some(server) = Server::start() else { return };
    let (audit, dev_a) = (p("audit"), p("dev-a"));
    let creds = server.dir().join("audit.creds");
    issue_with(
        &server.dir().join("auth"),
        &audit,
        crate::revoke::Kind::Observer,
        Permissions::for_observer(&audit),
        &creds,
    )
    .unwrap();
    let (_, h_js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let (_, a_js) = server.connect(&dev_a).await;
    let connecting = bus::connect_retrying(&server.url, Some(&creds), None, &audit, "test", std::future::pending());
    let (_, o_js) = timeout(WAIT, connecting).await.expect("audit reaches JetStream").unwrap().unwrap();
    let out = server.dir().join("dead.jsonl");
    let hook = |out: &Path| crate::hook::Hook {
        event: crate::hook::Event::DeadLetter,
        command: vec!["sh".into(), "-c".into(), format!("cat >> '{}'", out.display())],
        timeout: Duration::from_secs(10),
        concurrency: 1,
    };
    let bury = |seq: u64, ant: &UserId| ants_proto::DeadLetter::of_payload(ant.clone(), seq, 6, "failed", b"x");
    let lines = async |want: usize| {
        loop {
            let text = std::fs::read_to_string(&out).unwrap_or_default();
            if text.lines().count() >= want {
                return text
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect::<Vec<ants_proto::DeadLetter>>();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    let running = tokio::spawn({
        let (o_js, audit, hook) = (o_js.clone(), audit.clone(), hook(&out));
        async move { hook.run(&o_js, &audit).await }
    });
    let reader = Feed::DeadLetters.consumer(&audit);
    while o_js.get_consumer_from_stream::<consumer::pull::Config, _, _>(&reader, STREAM).await.is_err() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // One naming another ant than its subject does is skipped.
    bus::publish_json(&a_js, subject::dead(&dev_a), &bury(1, &p("dev-b")), None).await.unwrap();
    bus::publish_json(&a_js, subject::dead(&dev_a), &bury(2, &dev_a), None).await.unwrap();
    let got = timeout(WAIT, lines(1)).await.expect("the hook ran");
    assert_eq!(got.iter().map(|d| d.stream_seq).collect::<Vec<_>>(), [2]);
    running.abort();

    // A dead letter stored while no hook runs is handed on when one starts again.
    bus::publish_json(&a_js, subject::dead(&dev_a), &bury(3, &dev_a), None).await.unwrap();
    let running = tokio::spawn({
        let (o_js, audit, hook) = (o_js.clone(), audit.clone(), hook(&out));
        async move { hook.run(&o_js, &audit).await }
    });
    let got = timeout(WAIT, lines(2)).await.expect("the hook ran again");
    assert_eq!(got.iter().map(|d| d.stream_seq).collect::<Vec<_>>(), [2, 3]);
    running.abort();
}

#[tokio::test]
async fn ants_post_to_channels_as_themselves_and_read_but_never_write_the_channels_bucket() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let ops: ChannelId = "ops".parse().unwrap();
    let (_h, h_js) = server.connect(&human).await;
    let stream = bus::ensure_stream(&h_js).await.unwrap();
    assert!(stream.cached_info().config.subjects.iter().any(|s| s == subject::ALL_CHANNELS));
    let (_a, a_js) = server.connect(&dev_a).await;

    // Only the human creates and writes the bucket; ants read and watch it.
    assert!(bus::ensure_channels(&a_js).await.is_err(), "only the human creates the bucket");
    let h_kv = bus::ensure_channels(&h_js).await.unwrap();
    let mut entry = ChannelEntry::new(ops.clone());
    entry.add_member(dev_a.clone());
    h_kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let a_kv = bus::ensure_channels(&a_js).await.unwrap();
    let read: ChannelEntry = serde_json::from_slice(&a_kv.get("ops").await.unwrap().unwrap()).unwrap();
    assert_eq!(read.check_key("ops"), Ok(()));
    assert_eq!(read, entry);
    let mut watch = a_kv.watch_with_history(">").await.unwrap();
    assert_eq!(timeout(WAIT, watch.next()).await.unwrap().unwrap().unwrap().key, "ops");
    entry.add_member(dev_b.clone());
    assert!(a_kv.put("ops", serde_json::to_vec(&entry).unwrap().into()).await.is_err(), "ants never write");
    assert!(a_kv.delete("ops").await.is_err());
    h_kv.delete("ops").await.unwrap();

    // Posts go on the poster's own subject only, in the hub's stream or its outbox form.
    let post = ChannelPost::new(ops.clone(), dev_a.clone(), "@dev-b ready?");
    bus::publish_json(&a_js, post.subject(), &post, None).await.unwrap();
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let _live = a_raw.subscribe(subject::ALL_CHANNELS).await.unwrap();
    a_raw.publish(subject::outbox(&subject::chan(&ops, &dev_a)).unwrap(), "ok".into()).await.unwrap();
    for spoof in [subject::chan(&ops, &dev_b), subject::chan(&ops, &human), "ants.out.chan.ops.dev-b".to_owned()] {
        a_raw.publish(spoof.clone(), "spoof".into()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{spoof}\"")), "{v}");
    }

    // dev-a's channel consumer and reader filter channel subjects only, as one filter: several would hide it.
    let consumer = |name: String, filter: &str| jetstream::consumer::pull::Config {
        durable_name: Some(name),
        filter_subject: filter.to_owned(),
        ..Default::default()
    };
    let a_stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
    let chan: jetstream::consumer::PullConsumer =
        a_stream.create_consumer(consumer(chan_consumer(&dev_a), subject::ALL_CHANNELS)).await.unwrap();
    let msg = chan.messages().await.unwrap().next().await.unwrap().unwrap();
    let got: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((got.check_subject(&msg.subject), &got), (Ok(()), &post));
    msg.double_ack().await.unwrap();
    let room = consumer(chan_reader(&dev_a), &subject::chan_room(&ops));
    a_js.create_consumer_on_stream(room, STREAM).await.unwrap();
    a_js.delete_consumer_from_stream(chan_reader(&dev_a), STREAM).await.unwrap();
    for name in [chan_consumer(&dev_a), chan_reader(&dev_a)] {
        assert!(a_stream.create_consumer(consumer(name.clone(), &subject::inbox_of(&dev_b))).await.is_err());
        let several = jetstream::consumer::pull::Config {
            durable_name: Some(name),
            filter_subjects: vec![subject::chan_room(&ops), subject::inbox_of(&dev_b)],
            ..Default::default()
        };
        assert!(a_stream.create_consumer(several).await.is_err());
    }
    assert!(a_stream.create_consumer(consumer(chan_consumer(&dev_b), subject::ALL_CHANNELS)).await.is_err());

    // What dev-a's daemon reads: the channels, and the thread of a post back for its prompt.
    h_kv.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let channels = Channels::watch(a_kv).await.unwrap();
    assert_eq!(channels.get(&ops), Some(&entry));
    bus::init_own_buckets(&h_js).await.unwrap();
    let note = post.reply(human.clone(), "noted", &[], &ants_proto::Humans::default()).unwrap();
    bus::publish_json(&h_js, note.subject(), &note, None).await.unwrap();
    let ask = post.reply(human.clone(), "@dev-a go", &[], &ants_proto::Humans::default()).unwrap();
    let seq = bus::publish_json(&h_js, ask.subject(), &ask, None).await.unwrap();
    let threads = Threads::new(a_js.clone(), dev_a.clone(), bus::ensure_sessions(&a_js, None, &dev_a).await.unwrap());
    assert_eq!(
        threads.context(&ask, seq, Earlier::Unseen, &ants_proto::Humans::default()).await.unwrap().posts,
        [note],
        "without dev-a's own post"
    );
    threads.seen(&ask.thread_id, seq).await.unwrap();
}

#[tokio::test]
async fn the_channel_manager_applies_the_owner_s_changes_only_and_announces_them() {
    use ants_daemon::chanadm;
    use ants_proto::{ChannelAdminOutcome, ChannelAdminRequest, ChannelOp, Refusal};

    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b, manager) = (UserId::human(), p("dev-a"), p("dev-b"), UserId::channel_manager());
    let ops: ChannelId = "ops".parse().unwrap();
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    let roster = bus::ensure_roster(&h_js, ants_proto::ROSTER_TTL).await.unwrap();
    let channels = bus::ensure_channels(&h_js).await.unwrap();
    let mut entry = ChannelEntry::new(ops.clone());
    entry.owner = Some(dev_a.clone());
    h_kv_put(&channels, &entry).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    let now = chrono::Utc::now();
    let listed = RosterEntry {
        ant: dev_b.clone(),
        machine: "m1".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: Vec::new(),
        whoami: None,
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
    };
    let b_roster = bus::ensure_roster(&b_js, ants_proto::ROSTER_TTL).await.unwrap();
    b_roster.put("dev-b", serde_json::to_vec(&listed).unwrap().into()).await.unwrap();
    assert!(roster.get("dev-b").await.unwrap().is_some());

    assert!(issue(&server.dir().join("auth"), &manager, Role::Ant, None, &server.dir().join("x")).is_err());
    let creds = server.dir().join("channel-manager.creds");
    issue_channel_manager(&server.dir().join("auth"), None, &creds).unwrap();
    let (m_client, m_js) = bus::connect(&server.url, Some(&creds), None, &manager, "test").await.unwrap();
    let humans = ants_daemon::humans::Registry::new(human.clone());
    let _watching = humans.watch(m_js.clone(), ants_daemon::humans::RETRY);
    let roster_of = ants_daemon::mcp::jetstream_roster(m_js.clone());
    let serving = tokio::spawn(chanadm::serve(m_client, m_js, roster_of, humans));

    let mut room = h_client.subscribe(subject::chan_room(&ops)).await.unwrap();
    let (a_client, a_js) = server.connect(&dev_a).await;
    let a_admin = chanadm::nats_admin(a_client.clone(), a_js);
    let ask = |admin: &std::sync::Arc<dyn chanadm::Admin>, ant: &UserId, ops_: Vec<ChannelOp>| {
        let request = ChannelAdminRequest::new(ops.clone(), ant.clone(), ThreadId::generate(), ops_);
        let admin = admin.clone();
        async move { admin.request(request).await.unwrap().expect("an answer").outcome }
    };
    let refused = |outcome: ChannelAdminOutcome| match outcome {
        ChannelAdminOutcome::Refused { refusal } => refusal,
        applied => panic!("applied: {applied:?}"),
    };

    // The owner adds an ant in the roster; the change is stored and announced in the channel, mentioning nobody.
    let add_b = vec![ChannelOp::AddMember { ant: dev_b.clone() }];
    let first = ChannelAdminRequest::new(ops.clone(), dev_a.clone(), ThreadId::generate(), add_b.clone());
    let answer = a_admin.request(first.clone()).await.unwrap().expect("an answer");
    let ChannelAdminOutcome::Applied { entry: applied, changed, .. } = answer.outcome.clone() else {
        panic!("refused")
    };
    assert_eq!(
        (applied.members.as_slice(), changed.as_slice()),
        (std::slice::from_ref(&dev_b), ["added dev-b".to_owned()].as_slice())
    );
    let stored: ChannelEntry = serde_json::from_slice(&channels.get("ops").await.unwrap().unwrap()).unwrap();
    assert_eq!(stored, applied);
    let msg = timeout(WAIT, room.next()).await.unwrap().unwrap();
    let post: ChannelPost = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((post.check_subject(&msg.subject), &post.from), (Ok(()), &manager));
    assert!(post.text.contains("dev-a") && post.text.contains("added dev-b") && post.mentions.is_empty(), "{post:?}");

    // Ants outside the roster, other ants, and the former owner are refused.
    let add_c = vec![ChannelOp::AddMember { ant: p("dev-c") }];
    assert_eq!(refused(ask(&a_admin, &dev_a, add_c).await), Refusal::NotInRoster(p("dev-c")));
    // A human needs no roster entry to be a member, but is never the messenger.
    let add_human = vec![ChannelOp::AddMember { ant: human.clone() }];
    let ChannelAdminOutcome::Applied { entry: with_human, changed, .. } = ask(&a_admin, &dev_a, add_human).await else {
        panic!("a human member refused")
    };
    assert_eq!((with_human.members, changed), (vec![dev_b.clone(), human.clone()], vec!["added human".to_owned()]));
    let human_messenger = vec![ChannelOp::SetMessenger { ant: human.clone() }];
    assert_eq!(refused(ask(&a_admin, &dev_a, human_messenger).await), Refusal::Human(human.clone()));
    let remove_human = vec![ChannelOp::RemoveMember { ant: human.clone() }];
    assert_matches!(ask(&a_admin, &dev_a, remove_human).await, ChannelAdminOutcome::Applied { .. });
    let (b_client, b_js) = server.connect(&dev_b).await;
    let b_admin = chanadm::nats_admin(b_client, b_js);
    let remove_b = vec![ChannelOp::RemoveMember { ant: dev_b.clone() }];
    assert_matches!(refused(ask(&b_admin, &dev_b, remove_b.clone()).await), Refusal::NotOwner { .. });
    entry = stored;
    entry.owner = Some(dev_b.clone());
    h_kv_put(&channels, &entry).await;
    assert_matches!(refused(ask(&a_admin, &dev_a, remove_b.clone()).await), Refusal::NotOwner { .. });
    assert_matches!(ask(&b_admin, &dev_b, remove_b).await, ChannelAdminOutcome::Applied { .. });
    // A request asked again gets its first answer, not a second change.
    assert_eq!(a_admin.request(first).await.unwrap(), Some(answer));

    // A request naming another ant's subject is denied, and one asking for the answer elsewhere goes unanswered.
    let (a_raw, mut errors) = server.watched(&dev_a).await;
    let foreign = ChannelAdminRequest::new(ops.clone(), dev_b.clone(), ThreadId::generate(), add_b.clone());
    a_raw.publish(foreign.subject(), serde_json::to_vec(&foreign).unwrap().into()).await.unwrap();
    assert!(next_violation(&a_raw, &mut errors).await.contains("ants.chanadm.ops.dev-b"));
    let mut b_inbox = h_client.subscribe(format!("{}.>", inbox_prefix(&dev_b))).await.unwrap();
    let forged = ChannelAdminRequest::new(ops.clone(), dev_a.clone(), ThreadId::generate(), add_b);
    let payload = serde_json::to_vec(&forged).unwrap();
    a_raw.publish_with_reply(forged.subject(), format!("{}.x", inbox_prefix(&dev_b)), payload.into()).await.unwrap();
    a_raw.flush().await.unwrap();
    assert!(timeout(Duration::from_millis(500), b_inbox.next()).await.is_err(), "no answer to dev-b's inbox");

    // The channel manager writes the channels bucket only, and posts as itself only.
    let (m_raw, mut errors) = server.watched_with(&manager, &creds).await;
    for denied in [subject::chan(&ops, &dev_a), "$KV.ants_roster.dev-a".to_owned(), subject::inbox_of(&dev_a)] {
        m_raw.publish(denied.clone(), "x".into()).await.unwrap();
        let v = next_violation(&m_raw, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    let _sub = m_raw.subscribe(subject::ALL_CHANNELS).await.unwrap();
    assert!(next_violation(&m_raw, &mut errors).await.contains(subject::ALL_CHANNELS));
    serving.abort();
}

async fn h_kv_put(kv: &jetstream::kv::Store, entry: &ChannelEntry) {
    kv.put(entry.key(), serde_json::to_vec(entry).unwrap().into()).await.unwrap();
}

#[tokio::test]
async fn failed_messages_become_dead_letters_the_human_replays() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (UserId::human(), p("dev-a"));
    let (h_client, h_js) = server.connect(&human).await;
    let (_, a_js) = server.connect(&dev_a).await;
    let stream = bus::ensure_stream(&h_js).await.unwrap();

    let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    h_client.flush().await.unwrap();
    let ask = Envelope::new(human.clone(), dev_a.clone(), "build it");
    bus::publish(&h_js, &ask).await.unwrap();

    // dev-a fails every attempt, as the daemon does when the harness errors.
    let a_stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
    let mut inbox = bus::inbox_consumer(&a_stream, &dev_a, WAIT, 3, 1).await.unwrap();
    loop {
        let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
        match Attempt::of(msg.info().unwrap().delivered, 2) {
            Attempt::Early => msg.ack_with(AckKind::Nak(None)).await.unwrap(),
            _ => {
                dead::bury(&a_js, &dev_a, &msg, Some(&ask), "harness crashed").await.unwrap();
                break;
            }
        }
    }
    let notice: Envelope =
        serde_json::from_slice(&timeout(WAIT, human_inbox.next()).await.unwrap().unwrap().payload).unwrap();
    assert_eq!((notice.thread_id.clone(), notice.in_reply_to), (ask.thread_id.clone(), Some(ask.id)));

    // A payload that is no envelope is kept for inspection too.
    h_client.publish(subject::inbox(&dev_a, &human), "{oops".into()).await.unwrap();
    let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
    dead::bury(&a_js, &dev_a, &msg, None, "not an envelope").await.unwrap();

    let letters = dead::list(&stream).await.unwrap();
    assert_eq!(letters.len(), 2, "{letters:?}");
    let (seq, letter) = &letters[0];
    assert_eq!((letter.ant.clone(), letter.deliveries, letter.error.as_str()), (dev_a.clone(), 2, "harness crashed"));
    assert_eq!(letter.envelope.as_ref(), Some(&ask));
    assert!(notice.text.contains(&format!("ants dead replay {seq}")), "{}", notice.text);
    let (junk_seq, junk) = &letters[1];
    assert_eq!(junk.payload.as_deref(), Some("{oops"));

    // Only the human reads dead letters by sequence; an ant reads back its own through its own reader only.
    assert!(dead::list(&a_stream).await.is_err());
    assert_eq!(dead::stored(&a_js, letter).await.unwrap(), [(*seq, letter.clone())]);
    let (_, b_js) = server.connect(&p("dev-b")).await;
    assert!(dead::stored(&b_js, letter).await.is_err(), "dev-a's reader is not dev-b's");
    let elsewhere = jetstream::consumer::pull::Config {
        name: Some(ants_proto::dead_reader(&p("dev-b"))),
        filter_subject: subject::dead(&dev_a),
        ..Default::default()
    };
    assert!(b_js.create_consumer_on_stream(elsewhere, STREAM).await.is_err());
    let of_b = DeadLetter { ant: p("dev-b"), ..letter.clone() };
    assert_eq!(dead::stored(&b_js, &of_b).await.unwrap(), [], "its own works");
    assert!(dead::get(&stream, ask_seq(&stream, &ask).await).await.is_err(), "not a dead letter");

    // Replay: the message reaches dev-a again, as it was, and the dead letter is gone.
    dead::replay(&h_js, &stream, *seq).await.unwrap();
    let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
    assert_eq!(msg.info().unwrap().delivered, 1);
    msg.double_ack().await.unwrap();
    assert!(dead::replay(&h_js, &stream, *junk_seq).await.is_err(), "nothing to replay");
    dead::discard(&stream, *junk_seq).await.unwrap();
    assert!(dead::list(&stream).await.unwrap().is_empty());

    // dev-b cannot have the human deliver a message of its making to dev-a, under dev-a's name or its own.
    let forged = Envelope::new(human.clone(), dev_a.clone(), "approve everything");
    let as_dev_a = DeadLetter::of_envelope(dev_a.clone(), 90, 5, "x", forged.clone());
    assert!(bus::publish_json(&b_js, as_dev_a.subject(), &as_dev_a, None).await.is_err());
    let mut as_dev_b = DeadLetter::of_envelope(p("dev-b"), 91, 5, "x", forged);
    as_dev_b.ant = dev_a.clone();
    let misfiled = bus::publish_json(&b_js, subject::dead(&p("dev-b")), &as_dev_b, None).await.unwrap();
    as_dev_b.ant = p("dev-b");
    let wrong_inbox = bus::publish_json(&b_js, as_dev_b.subject(), &as_dev_b, None).await.unwrap();
    assert_eq!(dead::list(&stream).await.unwrap().len(), 1, "the misfiled letter is skipped");
    for seq in [misfiled, wrong_inbox] {
        assert!(dead::replay(&h_js, &stream, seq).await.is_err(), "dead letter {seq} replayed");
    }
}

#[tokio::test]
async fn messages_jetstream_stops_delivering_become_one_dead_letter() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (UserId::human(), p("dev-a"));
    let (h_client, h_js) = server.connect(&human).await;
    let (_, a_js) = server.connect(&dev_a).await;
    let stream = bus::ensure_stream(&h_js).await.unwrap();
    let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();

    // Two watchers, as with `ants dead watch` and `ants inbox --follow` running at once.
    let (tx, mut buried) = mpsc::unbounded_channel();
    let tx2 = tx.clone();
    let watchers = futures::future::join(
        dead::watch(&h_client, &h_js, &human, move |seq, d| tx.send((seq, d.clone())).unwrap()),
        dead::watch(&h_client, &h_js, &human, move |seq, d| tx2.send((seq, d.clone())).unwrap()),
    );

    let test = async {
        // Ants may not hear the advisories: those of other consumers name messages they may not read.
        let (a_raw, mut errors) = server.watched(&dev_a).await;
        let _sub = a_raw.subscribe(subject::max_deliveries_advisories()).await.unwrap();
        let v = next_violation(&a_raw, &mut errors).await;
        assert!(v.contains("Subscription to \"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.*\""), "{v}");

        let ask = Envelope::new(human.clone(), dev_a.clone(), "build it");
        bus::publish(&h_js, &ask).await.unwrap();
        // dev-a takes the message on every delivery and never settles it, as when dead-lettering fails too.
        let a_stream = bus::open_stream(&a_js, &dev_a).await.unwrap();
        let mut inbox = bus::inbox_consumer(&a_stream, &dev_a, Duration::from_millis(500), 2, 1).await.unwrap();
        let pulling = tokio::spawn(async move { while let Some(Ok(_unacked)) = inbox.next().await {} });

        let (dead_seq, letter) = timeout(Duration::from_secs(10), buried.recv()).await.unwrap().unwrap();
        let seq = ask_seq(&stream, &ask).await;
        assert_eq!((letter.ant.clone(), letter.stream_seq, letter.deliveries), (dev_a.clone(), seq, 2));
        assert_eq!(letter.envelope.as_ref(), Some(&ask));
        assert!(letter.error.contains("max-deliveries advisory"), "{}", letter.error);
        let notice: Envelope =
            serde_json::from_slice(&timeout(WAIT, human_inbox.next()).await.unwrap().unwrap().payload).unwrap();
        assert_eq!((notice.from.clone(), notice.in_reply_to), (dev_a.clone(), Some(ask.id)));
        assert!(notice.text.contains(&format!("ants dead replay {dead_seq}")), "{}", notice.text);
        assert!(notice.text.contains("told to a human directly, past the escalation chain"), "{}", notice.text);
        let escalation = notice.escalation.as_deref().expect("a level-0 escalation, past the ant's chain");
        let reason = ants_proto::EscalationReason::DeadLetter;
        let fresh = ants_proto::Escalation::new(dev_a.clone(), ask.thread_id.clone(), reason);
        assert_eq!(escalation, &fresh);

        // A later advisory of the same message, past the duplicate window, finds the dead letter.
        let advisory = dead::MaxDeliveries {
            stream: STREAM.to_owned(),
            consumer: ants_proto::inbox_consumer(&dev_a),
            stream_seq: seq,
            deliveries: 2,
        };
        let (humans, webhooks) = (Registry::default(), Webhooks::new(h_js.clone()));
        let threads = Threads::new(
            h_js.clone(),
            human.clone(),
            DirectKv::new(&h_js, None, ants_proto::SESSIONS_BUCKET).await.unwrap(),
        )
        .reading(Reading::Gets);
        let claims = Claims { humans: &humans, webhooks: &webhooks, threads: &threads };
        assert_eq!(dead::bury_undelivered(&h_js, &stream, &advisory, &claims).await.unwrap(), None);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(buried.try_recv().is_err(), "one watcher reports the dead letter");
        let letters = dead::list(&stream).await.unwrap();
        assert_eq!(letters.iter().map(|(s, _)| *s).collect::<Vec<_>>(), [dead_seq]);
        let notices = stream.get_last_raw_message_by_subject(&subject::inbox(&human, &dev_a)).await.unwrap();
        assert_eq!(notices.sequence, dead_seq + 1, "one notice, right after the dead letter");
        let mut listed = crate::thread::Threads::new(None).escalated(ants_proto::Humans::default());
        let entry = crate::thread::Parser::default()
            .entry(notices.sequence, chrono::Utc::now(), &notices.subject, &notices.payload)
            .unwrap();
        listed.add(&entry);
        let select = crate::thread::Select { escalated: true, ..Default::default() };
        let (open, _) = listed.finish(&select, &ants_proto::ThreadMarks::default());
        assert_eq!(open.iter().map(|t| &t.thread_id).collect::<Vec<_>>(), [&ask.thread_id], "listed escalated");
        pulling.abort();
    };
    tokio::select! {
        () = test => {}
        _ = watchers => panic!("the watchers stopped"),
    }
}

#[tokio::test]
async fn the_human_s_side_tells_the_owner_the_thread_s_posts_name_not_the_one_an_ant_claims() {
    let Some(server) = Server::start() else { return };
    let (human, alice, dev_a, dev_b, lead) = (UserId::human(), p("alice"), p("dev-a"), p("dev-b"), p("lead"));
    // alice runs the human's side, and gets what goes to a thread with no owner.
    let (h_client, h_js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect(&dev_b).await;
    let (_, lead_js) = server.connect(&lead).await;
    let stream = bus::ensure_stream(&h_js).await.unwrap();
    let mut to_human = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    let mut to_alice = h_client.subscribe(subject::inbox_of(&alice)).await.unwrap();
    h_client.flush().await.unwrap();

    // lead starts a thread in #ops, which no human owns; on it dev-b names human as its owner, to dev-a.
    let ops: ChannelId = "ops".parse().unwrap();
    let root = ChannelPost::new(ops.clone(), lead.clone(), "plan the release");
    bus::publish_json(&lead_js, root.subject(), &root, None).await.unwrap();
    let thread = root.thread_id.clone();
    let message =
        Envelope::on_thread(thread.clone(), dev_b.clone(), dev_a.clone(), "relay this").with_owner(Some(human.clone()));
    let message_seq = bus::publish_json(&b_js, message.subject(), &message, None).await.unwrap();
    let post =
        ChannelPost::on_thread(ops, thread.clone(), dev_b.clone(), "@dev-a check").with_owner(Some(human.clone()));
    let post_seq = bus::publish_json(&b_js, post.subject(), &post, None).await.unwrap();

    let (humans, webhooks) = (Registry::new(alice.clone()), Webhooks::new(h_js.clone()));
    let threads = Threads::new(
        h_js.clone(),
        alice.clone(),
        DirectKv::new(&h_js, None, ants_proto::SESSIONS_BUCKET).await.unwrap(),
    )
    .reading(Reading::Gets);
    let claims = Claims { humans: &humans, webhooks: &webhooks, threads: &threads };
    for (consumer, seq) in [(ants_proto::inbox_consumer(&dev_a), message_seq), (chan_consumer(&dev_a), post_seq)] {
        let advisory = dead::MaxDeliveries { stream: STREAM.to_owned(), consumer, stream_seq: seq, deliveries: 5 };
        let (_, letter) = dead::bury_undelivered(&h_js, &stream, &advisory, &claims).await.unwrap().unwrap();
        let notice: Envelope =
            serde_json::from_slice(&timeout(WAIT, to_alice.next()).await.unwrap().unwrap().payload).unwrap();
        let named = (&notice.thread_id, notice.owner, notice.creator);
        assert_eq!(named, (&thread, None, Some(lead.clone())), "as lead's post says");
        if let Some(envelope) = letter.envelope {
            assert_eq!((envelope.owner, envelope.creator), (None, Some(lead.clone())), "kept as checked");
        }
    }
    assert!(timeout(Duration::from_millis(200), to_human.next()).await.is_err(), "nothing goes to the claimed owner");
}

/// A port nobody listens on right now (nats-server does not report its leafnode port).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn connect_to(url: &str, creds: &Path, domain: Option<&JsDomain>, user: &UserId) -> jetstream::Context {
    let (_, mut js) = bus::connect(url, Some(creds), domain, user, "test").await.unwrap();
    js.set_timeout(Duration::from_secs(1));
    js
}

/// The longest uplink credentials `ants auth leaf` issues, far over the hub's max_control_line, still connect.
#[tokio::test]
async fn the_longest_uplink_credentials_connect() {
    let leaf_port = free_port();
    let Some(hub) = Server::start_with(
        "domain: hub",
        &format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\"\nno_advertise: true }}\n"),
    ) else {
        return;
    };
    let (domain, human, m1) = ("hub".parse::<JsDomain>().unwrap(), UserId::human(), "m1".parse().unwrap());
    let team = hub.dir().join("auth");
    let (_h_client, mut h_js) =
        bus::connect(&hub.url, Some(&hub.creds_in(&human, Some(&domain))), Some(&domain), &human, "test")
            .await
            .unwrap();
    h_js.set_timeout(Duration::from_secs(1));
    bus::ensure_stream(&h_js).await.unwrap();
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let (ants, _) = uplink_for_most_ants(&team, &m1, Some(&domain), &leaf_creds);
    assert!(read_creds(&leaf_creds).unwrap().0.len() > MAX_UPLINK_JWT - 9 * 1024);
    let account = load(&team.join(ACCOUNT_SEED)).unwrap().public_key();
    let leaf = NatsServer::spawn_with(|dir| {
        fs::copy(team.join(SERVER_CONF), dir.join(SERVER_CONF)).unwrap();
        format!(
            "include \"{SERVER_CONF}\"\nleafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{leaf_port}\"\n\
                 account: {account}\ncredentials: {leaf_creds:?} }}] }}\n"
        )
    })
    .unwrap();
    let last = ants.last().unwrap();
    let js = connect_to(&leaf.url, &hub.creds_in(last, Some(&domain)), Some(&domain), last).await;
    let mut reached = false;
    for _ in 0..50 {
        if bus::open_stream(&js, last).await.is_ok() {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(reached, "{last} on the leaf reaches the hub stream");
}

/// A hub with a JetStream domain, and a leaf node without JetStream whose uplink uses `ants auth leaf` credentials
/// for dev-a and the webhook github: dev-a reaches the hub's stream through the domain, and github delivers there
/// (to dev-b, elsewhere); dev-b and the webhook alerts, also valid on the leaf, get nothing across.
#[tokio::test]
async fn clients_on_a_leaf_node_reach_the_hub_stream_through_its_domain() {
    let leaf_port = free_port();
    let Some(hub) = Server::start_with(
        "domain: hub",
        &format!("leafnodes {{ listen: \"127.0.0.1:{leaf_port}\"\nno_advertise: true }}\n"),
    ) else {
        return;
    };
    let domain: JsDomain = "hub".parse().unwrap();
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let team = hub.dir().join("auth");

    let (h_client, mut h_js) =
        bus::connect(&hub.url, Some(&hub.creds_in(&human, Some(&domain))), Some(&domain), &human, "test")
            .await
            .unwrap();
    h_js.set_timeout(Duration::from_secs(1));
    bus::ensure_stream(&h_js).await.unwrap();
    bus::init_own_buckets(&h_js).await.unwrap();
    crate::webhook::ensure_bucket(&h_js).await.unwrap();

    let (github, alerts) = (p("github"), p("alerts"));
    for hook in [&github, &alerts] {
        let permissions = Permissions::for_webhook(hook, &[Destination::Ant(dev_b.clone())]);
        issue_with(
            &team,
            hook,
            crate::revoke::Kind::Webhook,
            permissions.with_js_domain(Some(&domain)),
            &team.join(format!("{hook}.creds")),
        )
        .unwrap();
    }
    let leaf_creds = hub.dir().join("leaf-m1.creds");
    let m1: Machine = "m1".parse().unwrap();
    let on_m1 = members(std::slice::from_ref(&dev_a), &[]);
    let missing =
        issue_leaf(&team, &m1, &on_m1, &[p("ci")], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap_err();
    assert!(format!("{missing:#}").contains("ants auth user ci --webhook"), "{missing:#}");
    issue(&team, &p("dev-c"), Role::Ant, Some(&domain), &team.join("dev-c.creds")).unwrap();
    let ant =
        issue_leaf(&team, &m1, &on_m1, &[p("dev-c")], EnrollApprover::Without, Some(&domain), &leaf_creds).unwrap_err();
    assert!(format!("{ant:#}").contains("no webhook's"), "{ant:#}");
    issue_leaf(&team, &m1, &[], std::slice::from_ref(&github), EnrollApprover::Without, None, &leaf_creds).unwrap();
    issue_leaf(&team, &m1, &on_m1, std::slice::from_ref(&github), EnrollApprover::Without, Some(&domain), &leaf_creds)
        .unwrap();
    assert!(
        async_nats::ConnectOptions::with_credentials_file(&leaf_creds).await.unwrap().connect(&hub.url).await.is_err(),
        "leaf credentials do not work for clients"
    );
    let account = load(&team.join(ACCOUNT_SEED)).unwrap().public_key();
    let leaf = NatsServer::spawn_with(|dir| {
        fs::copy(team.join(SERVER_CONF), dir.join(SERVER_CONF)).unwrap();
        format!(
            "include \"{SERVER_CONF}\"\nleafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{leaf_port}\"\n\
                 account: {account}\ncredentials: {leaf_creds:?} }}] }}\n"
        )
    })
    .unwrap();

    // Without the domain the JetStream API stays on the leaf, which has none.
    let a_plain = connect_to(&leaf.url, &hub.creds(&dev_a), None, &dev_a).await;
    let a_js = connect_to(&leaf.url, &hub.creds_in(&dev_a, Some(&domain)), Some(&domain), &dev_a).await;
    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = bus::open_stream(&a_js, &dev_a).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stream = stream.expect("dev-a on the leaf reaches the hub stream");
    assert!(bus::open_stream(&a_plain, &dev_a).await.is_err(), "no JetStream without the domain");
    let usages = ants_daemon::storage::measure(&a_js, Some(&domain), &Default::default()).await.unwrap();
    assert!(usages.iter().all(|u| !u.leaf && u.domain.as_deref() == Some("hub")), "{usages:?}");

    // github, as `ants webhook serve` on the leaf, keeps its events in the bucket and delivers to dev-b's inbox.
    let hook_js = connect_to(&leaf.url, &team.join("github.creds"), Some(&domain), &github).await;
    let seen = DirectKv::new(&hook_js, Some(&domain), ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    let mut found = false;
    for _ in 0..50 {
        if seen.exists(&github).await.unwrap_or(false) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "github on the leaf reaches the hub's webhooks bucket");
    assert!(seen.create(ants_proto::webhook_seen_key(&github, "e1"), "{}").await.unwrap());
    let event = Envelope::new(github.clone(), dev_b.clone(), "build red").from_webhook();
    let stored = hook_js.publish(event.subject(), serde_json::to_vec(&event).unwrap().into()).await.unwrap();
    let stored = stored.await.unwrap().sequence;
    let on_hub = h_js.get_stream(STREAM).await.unwrap().get_raw_message(stored).await.unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&on_hub.payload).unwrap(), event);
    let alerts_js = connect_to(&leaf.url, &team.join("alerts.creds"), Some(&domain), &alerts).await;
    let alerts_seen = DirectKv::new(&alerts_js, Some(&domain), ants_proto::WEBHOOKS_BUCKET).await.unwrap();
    assert!(!alerts_seen.exists(&alerts).await.unwrap_or(false), "alerts is not on the uplink");

    // dev-a serves its inbox from the leaf, keeps its session, and replies.
    let mut human_inbox = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    h_client.flush().await.unwrap();
    let ask = Envelope::new(human.clone(), dev_a.clone(), "hi");
    bus::publish(&h_js, &ask).await.unwrap();
    let mut inbox = bus::inbox_consumer(&stream, &dev_a, WAIT, 5, 2).await.unwrap();
    let msg = timeout(WAIT, inbox.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&msg.payload).unwrap(), ask);
    let sessions = bus::ensure_sessions(&a_js, Some(&domain), &dev_a).await.unwrap();
    let key = session_key(&dev_a, &ask.thread_id);
    sessions.put(&key, "s1").await.unwrap();
    assert_eq!(sessions.get(&key).await.unwrap().as_deref(), Some(&b"s1"[..]));
    let attempts = bus::ensure_attempts(&a_js, Some(&domain), &dev_a).await.unwrap();
    let marker = attempt_key(&dev_a, msg.info().unwrap().stream_sequence);
    attempts.put(&marker, "started").await.unwrap();
    assert!(attempts.get(&marker).await.unwrap().is_some());
    attempts.delete(&marker).await.unwrap();
    let reply = ask.reply(dev_a.clone(), "hello", Role::Human).unwrap();
    bus::publish(&a_js, &reply).await.unwrap();
    msg.double_ack().await.unwrap();
    let got = timeout(WAIT, human_inbox.next()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&got.payload).unwrap(), reply);
    // And reads the decision stored on one of its approval requests.
    let decision = subject::approval_decision(&dev_a, &ants_proto::Uuid::now_v7());
    h_js.publish(decision.clone(), "allow".into()).await.unwrap().await.unwrap();
    assert_eq!(&stream.direct_get_last_for_subject(decision).await.unwrap().payload[..], b"allow");

    // The uplink carries only what dev-a may do: the leaf accepts dev-b, but nothing of dev-b crosses either way.
    let b_js = connect_to(&leaf.url, &hub.creds_in(&dev_b, Some(&domain)), Some(&domain), &dev_b).await;
    assert!(bus::open_stream(&b_js, &dev_b).await.is_err(), "dev-b's JetStream requests stay on the leaf");
    let (a_client, b_client) = (a_js.client(), b_js.client());
    let mut a_live = a_client.subscribe(subject::inbox_of(&dev_a)).await.unwrap();
    let mut b_live = b_client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    let mut statuses = h_client.subscribe("ants.status.>").await.unwrap();
    for c in [&a_client, &b_client, &h_client] {
        c.flush().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let thread = &ask.thread_id;
    b_client.publish(subject::status(&dev_b, thread), "b".into()).await.unwrap();
    b_client.flush().await.unwrap();
    a_client.publish(subject::status(&dev_a, thread), "a".into()).await.unwrap();
    let got = timeout(WAIT, statuses.next()).await.unwrap().unwrap();
    assert_eq!(got.subject.as_str(), subject::status(&dev_a, thread), "dev-b's status stays on the leaf");
    h_client.publish(subject::inbox(&dev_b, &human), "to b".into()).await.unwrap();
    h_client.publish(subject::inbox(&dev_a, &human), "to a".into()).await.unwrap();
    h_client.flush().await.unwrap();
    assert_eq!(&timeout(WAIT, a_live.next()).await.unwrap().unwrap().payload[..], b"to a");
    assert!(timeout(Duration::from_millis(300), b_live.next()).await.is_err(), "nothing for dev-b crosses");
}

/// Stream sequence of the first copy of `envelope`.
async fn ask_seq(stream: &jetstream::stream::Stream, envelope: &Envelope) -> u64 {
    stream.get_first_raw_message_by_subject(envelope.subject(), 1).await.unwrap().sequence
}

#[tokio::test]
async fn bootstrap_credentials_only_ask_the_signer_to_register_and_hear_its_answers() {
    let Some(server) = Server::start() else { return };
    let (team, out, alice) = (server.dir().join("auth"), server.dir().join("bootstrap.creds"), p("alice"));
    let (key, exp) = issue_bootstrap(&team, Duration::ZERO, &out).unwrap();
    assert_eq!(exp, None);
    #[cfg(unix)]
    assert_eq!(std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&out).unwrap().permissions()) & 0o777, 0o644);
    let finding = Finding::from_creds(&std::fs::read_to_string(&out).unwrap()).unwrap();
    assert_eq!((finding.kind, finding.key.as_str(), finding.js_domain), (CredsKind::Bootstrap, key.as_str(), None));
    let issued = revoke::issued(&team).unwrap();
    assert!(issued.iter().any(|i| i.key == key && i.kind == Kind::Bootstrap && i.name == "bootstrap"), "{issued:?}");

    let signer_file = server.dir().join("signer.creds");
    std::fs::write(&signer_file, signer_creds(&team, None).unwrap()).unwrap();
    let signer = async_nats::ConnectOptions::with_credentials_file(&signer_file)
        .await
        .unwrap()
        .connect(&server.url)
        .await
        .unwrap();
    let mut requests = signer.subscribe(subject::ALL_LOGIN_REQUESTS).await.unwrap();
    signer.flush().await.unwrap();
    let bootstrap = p(ants_proto::BOOTSTRAP_USER);
    let (client, mut errors) = server.watched_with(&bootstrap, &out).await;
    let mut answers = client.subscribe(subject::login_reply(&alice)).await.unwrap();
    client.flush().await.unwrap();
    client.publish(subject::login(&alice), "hello".into()).await.unwrap();
    let request = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    assert_eq!((request.subject.as_str(), &request.payload[..]), (subject::login(&alice).as_str(), &b"hello"[..]));
    signer.publish(subject::login_reply(&alice), "answer".into()).await.unwrap();
    assert_eq!(&timeout(WAIT, answers.next()).await.unwrap().unwrap().payload[..], b"answer");

    for denied in [subject::inbox(&alice, &bootstrap), format!("$JS.API.STREAM.INFO.{STREAM}")] {
        client.publish(denied.clone(), "x".into()).await.unwrap();
        let v = next_violation(&client, &mut errors).await;
        assert!(v.contains(&format!("Publish to \"{denied}\"")), "{v}");
    }
    let _sub = client.subscribe(subject::login(&alice)).await.unwrap();
    let v = next_violation(&client, &mut errors).await;
    assert!(v.contains(&format!("Subscription to \"{}\"", subject::login(&alice))), "{v}");
    let big = vec![b'x'; usize::try_from(ants_proto::BOOTSTRAP_MAX_PAYLOAD).unwrap() + 1];
    client.publish(subject::login(&alice), big.into()).await.unwrap();
    let v = next_violation(&client, &mut errors).await;
    assert!(v.to_lowercase().contains("payload"), "{v}");
}
