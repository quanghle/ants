//! Tests of revocation: names and devices revoked, renewals listed, connections dropped, resolvers keeping revocations,
//! GitHub bindings and grants, predecessors, and leaf uplinks.

use tokio::sync::mpsc;
use tokio::time::timeout;

use super::*;
use crate::test_support::Server;

const WAIT: Duration = Duration::from_secs(5);

#[test]
fn revoking_a_name_takes_all_its_keys_and_drops_revocations_once_expired() {
    let dir = ants_test_support::scratch_dir("revoke");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let alice: ants_proto::UserId = "alice".parse().unwrap();
    let human = |n: &str| {
        let out = team.join(format!("alice-{n}.creds"));
        auth::issue(&team, &alice, ants_proto::Role::Human, None, &out).unwrap();
        auth::verified(auth::creds_jwt(&fs::read_to_string(out).unwrap()).unwrap()).unwrap().sub
    };
    let (laptop, phone) = (human("laptop"), human("phone"));

    let err = revoke(&team, Some("bob"), &[]).unwrap_err().to_string();
    assert!(err.contains("no credentials of bob"), "{err}");
    assert!(revoke(&team, None, &["not-a-key".to_owned()]).is_err());
    let mut keys: Vec<String> = revoke(&team, Some("alice"), &[]).unwrap().into_iter().map(|r| r.key).collect();
    keys.sort();
    let mut expected = vec![laptop.clone(), phone];
    expected.sort();
    assert_eq!(keys, expected);
    let conf = fs::read_to_string(team.join(auth::SERVER_CONF)).unwrap();
    let account = conf.lines().find_map(|l| l.trim().strip_prefix(&format!("{}: ", auth::account_key(&team).unwrap())));
    let body = account.unwrap().split('.').nth(1).unwrap();
    let claims: serde_json::Value =
        serde_json::from_slice(&data_encoding::BASE64URL_NOPAD.decode(body.as_bytes()).unwrap()).unwrap();
    assert_eq!(claims["nats"]["revocations"].as_object().unwrap().len(), 2, "{claims}");

    // A key of credentials that expired needs no revocation: it is dropped on the next change.
    let enrollment = team.join("enroll.creds");
    auth::issue_enrollment(&team, &"dev-c".parse().unwrap(), Duration::from_secs(600), &enrollment).unwrap();
    let revoked_now = revoke(&team, Some("enroll-dev-c"), std::slice::from_ref(&laptop)).unwrap();
    assert_eq!(revoked_now.len(), 2);
    assert_eq!(revoked(&team).unwrap().len(), 3, "laptop's revocation is replaced, not repeated");
    apply_at(&team, auth::now().unwrap() + 601).unwrap();
    let left: Vec<Option<String>> = revoked(&team).unwrap().into_iter().map(|r| r.name).collect();
    assert_eq!(left, vec![Some("alice".to_owned()); 2]);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn the_listing_names_the_renewal_of_credentials_in_use_that_are_outdated_or_expire_soon() {
    let dir = ants_test_support::scratch_dir("issued");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let ant =
        |name: &str, out: &Path| auth::issue(&team, &name.parse().unwrap(), ants_proto::Role::Ant, None, out).unwrap();
    ant("dev-a", &dir.join("dev-a-old.creds"));
    ant("dev-a", &team.join("dev-a.creds"));
    ant("dev-b", &dir.join("dev-b.creds"));
    ant("dev-c", &team.join("dev-c.creds"));
    auth::issue_enrollment(&team, &"dev-d".parse().unwrap(), Duration::from_secs(60), &dir.join("e.creds")).unwrap();
    let now = auth::now().unwrap();
    // dev-a's keys carry an older permissions version; dev-b's expires in a day.
    let lines: String = issued(&team)
        .unwrap()
        .into_iter()
        .map(|i| match i.name.as_str() {
            "dev-a" => Issued { perms: ants_proto::PermissionsVersion(0), ..i },
            "dev-b" => Issued { expires_at: Some(now + 86_400), ..i },
            _ => i,
        })
        .map(|i| serde_json::to_string(&i).unwrap() + "\n")
        .collect();
    fs::write(team.join(ISSUED), lines).unwrap();

    let listed = listing(&team, now).unwrap();
    let summary: Vec<(&str, bool, bool, bool, Vec<String>)> =
        listed.iter().map(|l| (l.issued.name.as_str(), l.reissued, l.outdated, l.expiring, l.renew.clone())).collect();
    assert_eq!(
        summary,
        [
            ("dev-a", true, true, false, vec![]),
            ("dev-a", false, true, false, vec!["ants auth user dev-a".to_owned()]),
            ("dev-b", false, false, true, vec!["ants auth user dev-b [--js-domain DOMAIN]".to_owned()]),
            ("dev-c", false, false, false, vec![]),
            ("enroll-dev-d", false, false, true, vec![]),
        ]
    );
    fs::remove_dir_all(dir).unwrap();
}

/// A connection with `creds` to `url` that reports when the server closes it.
async fn watched(url: &str, creds: &Path) -> Result<(async_nats::Client, mpsc::UnboundedReceiver<()>)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let client = async_nats::ConnectOptions::with_credentials_file(creds)
        .await?
        .event_callback(move |event| {
            let tx = tx.clone();
            async move {
                if let async_nats::Event::Disconnected = event {
                    let _ = tx.send(());
                }
            }
        })
        .connect(url)
        .await?;
    Ok((client, rx))
}

#[tokio::test]
async fn revoked_credentials_lose_their_connection_and_are_refused_after_a_push_or_a_reload() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let issue = |user: &str, file: &str| {
        let out = team.join(file);
        auth::issue(&team, &user.parse().unwrap(), ants_proto::Role::Ant, None, &out).unwrap();
        out
    };
    let (a, b) = (issue("dev-a", "dev-a.creds"), issue("dev-b", "dev-b.creds"));
    let (a_client, mut a_closed) = watched(&server.url, &a).await.unwrap();
    let (b_client, mut b_closed) = watched(&server.url, &b).await.unwrap();
    for client in [&a_client, &b_client] {
        client.flush().await.unwrap();
    }

    // Pushed: dev-a's connection is closed at once and its credentials refused; dev-b's are untouched.
    revoke(&team, Some("dev-a"), &[]).unwrap();
    push(&server.url, &team).await.unwrap();
    timeout(WAIT, a_closed.recv()).await.expect("dev-a's connection stays open").unwrap();
    assert!(watched(&server.url, &a).await.is_err(), "dev-a's credentials still work");
    b_client.flush().await.unwrap();
    assert!(b_closed.try_recv().is_err(), "dev-b's connection was closed");
    // New credentials of the name are not revoked.
    let (renewed, _) = watched(&server.url, &issue("dev-a", "dev-a-new.creds")).await.unwrap();

    // Without a push, the server reloading server.conf applies the revocation too.
    revoke(&team, Some("dev-b"), &[]).unwrap();
    let reload = std::process::Command::new("kill").arg("-HUP").arg(server.pid().to_string()).status();
    assert!(reload.unwrap().success());
    timeout(WAIT, b_closed.recv()).await.expect("dev-b's connection stays open").unwrap();
    assert!(watched(&server.url, &b).await.is_err(), "dev-b's credentials still work");
    assert!(watched(&server.url, &a).await.is_err(), "dev-a's revocation stays after the reload");
    renewed.flush().await.unwrap();

    // An unreachable server leaves the revocations stored.
    let port = ants_test_support::free_port();
    assert!(push(&format!("nats://127.0.0.1:{port}"), &team).await.is_err());
    assert_eq!(revoked(&team).unwrap().len(), 2);
}

/// A hub whose files outlive its process, to restart it: the team's directory, the hub's own copy of server.conf
/// as copied when it was set up, and, with a `full` resolver, the resolver's directory.
struct Hub {
    files: std::path::PathBuf,
    full: bool,
}

impl Hub {
    fn new(full: bool) -> Self {
        let files = ants_test_support::scratch_dir("hub-files");
        auth::init(&files.join("auth")).unwrap();
        fs::create_dir(files.join("hub")).unwrap();
        fs::copy(files.join("auth").join(auth::SERVER_CONF), files.join("hub").join(auth::SERVER_CONF)).unwrap();
        Self { files, full }
    }

    fn team(&self) -> std::path::PathBuf {
        self.files.join("auth")
    }

    /// None when nats-server is not installed.
    fn start(&self) -> Option<ants_test_support::NatsServer> {
        ants_test_support::NatsServer::spawn_with(|_| {
            // Both directories are in target/test-tmp; includes are relative to the configuration.
            let files = self.files.file_name().unwrap().to_string_lossy();
            let resolver = if self.full {
                format!("resolver {{ type: full, dir: {:?} }}\n", self.files.join("jwt"))
            } else {
                String::new()
            };
            format!("include \"../{files}/hub/{}\"\n{resolver}", auth::SERVER_CONF)
        })
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.files);
    }
}

/// Only a hub with a `full` resolver keeps a revocation pushed to it once restarted on the server.conf it had
/// before, and says so; a newer server.conf still replaces the account it keeps.
#[tokio::test]
async fn only_a_hub_with_a_full_resolver_keeps_a_pushed_revocation_across_restarts() {
    for full in [false, true] {
        let hub = Hub::new(full);
        let team = hub.team();
        let issue = |user: &str| {
            let out = team.join(format!("{user}.creds"));
            auth::issue(&team, &user.parse().unwrap(), ants_proto::Role::Ant, None, &out).unwrap();
            out
        };
        let (a, b) = (issue("dev-a"), issue("dev-b"));
        let Some(server) = hub.start() else { return };
        // A MEMORY resolver skips the update of an account with no connection.
        let (_b_client, _) = watched(&server.url, &b).await.unwrap();
        // Account JWTs carry their issue time in seconds, and the later one is kept.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        revoke(&team, Some("dev-a"), &[]).unwrap();
        assert_eq!(push(&server.url, &team).await.unwrap().kept, Some(full));
        assert!(watched(&server.url, &a).await.is_err(), "dev-a's credentials still work");

        drop(server);
        let server = hub.start().unwrap();
        assert_eq!(watched(&server.url, &a).await.is_err(), full, "dev-a's revocation kept with full = {full}");
        assert!(watched(&server.url, &b).await.is_ok(), "dev-b's credentials are refused");
        if !full {
            continue;
        }

        revoke(&team, Some("dev-b"), &[]).unwrap();
        fs::copy(team.join(auth::SERVER_CONF), hub.files.join("hub").join(auth::SERVER_CONF)).unwrap();
        drop(server);
        let server = hub.start().unwrap();
        assert!(watched(&server.url, &b).await.is_err(), "the newer server.conf's revocation is not applied");
        assert!(watched(&server.url, &a).await.is_err(), "dev-a's revocation is lost");
    }
}

#[tokio::test]
async fn a_human_s_devices_are_listed_relabelled_and_revoked_one_by_one() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let alice: ants_proto::UserId = "alice".parse().unwrap();
    let issue = |label: &str, file: &str| {
        let out = team.join(file);
        let permissions = ants_proto::Permissions::for_user(&alice, ants_proto::Role::Human);
        let device = auth::issue_device(&team, &alice, Kind::Human, permissions, Some(label), &out).unwrap();
        (out, device.unwrap())
    };
    let (laptop, laptop_id) = issue("Laptop", "alice-laptop.creds");
    let (phone, phone_id) = issue("Phone", "alice-phone.creds");
    let (tablet, tablet_id) = issue("Phone", "alice-tablet.creds");
    let text = fs::read_to_string(&laptop).unwrap();
    let claims = crate::people::humans::creds_claims(&text).unwrap();
    assert_eq!(claims["nats"]["tags"][0], laptop_id.tag(), "the JWT carries the device id");
    assert!(
        auth::issue_device(
            &team,
            &alice,
            Kind::Ant,
            ants_proto::Permissions::for_user(&alice, ants_proto::Role::Ant),
            Some("x"),
            &team.join("x.creds")
        )
        .is_err()
    );

    let listed = devices(&team, "alice").unwrap();
    let ids: Vec<ants_proto::DeviceId> = listed.iter().map(|d| d.device).collect();
    assert_eq!(ids, [laptop_id, phone_id, tablet_id]);
    let err = device_keys(&team, "alice", &["Phone".to_owned()]).unwrap_err().to_string();
    assert!(err.contains(&phone_id.to_string()) && err.contains(&tablet_id.to_string()), "{err}");
    assert!(device_keys(&team, "alice", &["Desk".to_owned()]).is_err());
    assert!(rename(&team, "alice", "Laptop", "").is_err());
    let renamed = rename(&team, "alice", &tablet_id.to_string(), "Tablet").unwrap();
    assert_eq!((renamed.device, renamed.label.as_deref()), (tablet_id, Some("Tablet")));
    let labels: Vec<Option<String>> = devices(&team, "alice").unwrap().into_iter().map(|d| d.label).collect();
    assert_eq!(labels, [Some("Laptop".into()), Some("Phone".into()), Some("Tablet".into())]);

    // Revoking the phone, by its label now unique, closes its connection only.
    let connected = async |creds: &Path| {
        let (client, closed) = watched(&server.url, creds).await.unwrap();
        client.flush().await.unwrap();
        (client, closed)
    };
    let (_phone_client, mut phone_closed) = connected(&phone).await;
    let (laptop_client, mut laptop_closed) = connected(&laptop).await;
    let keys = device_keys(&team, "alice", &["Phone".to_owned()]).unwrap();
    assert_eq!(revoke(&team, None, &keys).unwrap().len(), 1);
    push(&server.url, &team).await.unwrap();
    timeout(WAIT, phone_closed.recv()).await.expect("the phone's connection stays open").unwrap();
    assert!(watched(&server.url, &phone).await.is_err());
    laptop_client.flush().await.unwrap();
    assert!(laptop_closed.try_recv().is_err(), "the laptop's connection was closed");
    assert!(watched(&server.url, &tablet).await.is_ok());
    let revoked: Vec<bool> = devices(&team, "alice").unwrap().into_iter().map(|d| d.revoked).collect();
    assert_eq!(revoked, [false, true, false]);
}

#[test]
fn a_github_binding_and_its_grant_are_listed_with_the_device_and_kept_on_renewal() {
    use ants_daemon::config::registration::{GithubAccount, Grant};
    let dir = ants_test_support::scratch_dir("binding");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let alice: ants_proto::UserId = "alice".parse().unwrap();
    let new = auth::NewDevice {
        label: Some("Laptop"),
        github: Some(GithubAccount { id: 42, login: "octocat".into() }),
        grant: Some(Grant::Org("acme".into())),
        ..auth::NewDevice::default()
    };
    let key = nkeys::KeyPair::new_user();
    let permissions = ants_proto::Permissions::for_user(&alice, ants_proto::Role::Human);
    let (jwt, device) =
        auth::sign_device_with(&team, &alice, Kind::Human, permissions, &new, &key.public_key(), None).unwrap();

    let listed = devices(&team, "alice").unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!((listed[0].github.as_deref(), listed[0].grant.as_ref()), (Some("octocat"), new.grant.as_ref()));
    auth::renew(&team, &jwt, &nkeys::KeyPair::new_user().public_key(), None).unwrap();
    let renewed = issued(&team).unwrap().pop().unwrap();
    assert_eq!(renewed.device, device);
    assert_eq!((renewed.github, renewed.grant), (new.github, new.grant));
}

#[test]
fn revoking_a_name_withdraws_its_approval_grants_but_revoking_a_device_does_not() {
    use ants_daemon::config::registration::{GithubAccount, Grant};
    let dir = ants_test_support::scratch_dir("withdraw");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let carol: ants_proto::UserId = "carol".parse().unwrap();
    let sign = |label| {
        let new = auth::NewDevice {
            label: Some(label),
            github: Some(GithubAccount { id: 7, login: "mallory".into() }),
            grant: Some(Grant::Approval("alice".into())),
            ..auth::NewDevice::default()
        };
        let key = nkeys::KeyPair::new_user().public_key();
        let permissions = ants_proto::Permissions::for_user(&carol, ants_proto::Role::Human);
        auth::sign_device_with(&team, &carol, Kind::Human, permissions, &new, &key, None).unwrap();
    };
    sign("laptop");
    sign("phone");
    let approval = Some(Grant::Approval("alice".into()));
    assert_eq!(standing_approval(&issued(&team).unwrap(), "carol", 7), approval);
    assert_eq!(standing_approval(&issued(&team).unwrap(), "carol", 8), None, "another GitHub user");
    assert_eq!(standing_approval(&issued(&team).unwrap(), "bob", 7), None, "another name");

    let keys = device_keys(&team, "carol", &["laptop".to_owned()]).unwrap();
    revoke(&team, None, &keys).unwrap();
    assert_eq!(standing_approval(&issued(&team).unwrap(), "carol", 7), approval, "one device revoked");

    revoke(&team, Some("carol"), &[]).unwrap();
    let after = issued(&team).unwrap();
    assert_eq!(standing_approval(&after, "carol", 7), None);
    let binding = GithubAccount { id: 7, login: "mallory".into() };
    assert!(after.iter().all(|i| i.github.as_ref() == Some(&binding)), "the binding stays");
    rename(&team, "carol", "phone", "Phone").unwrap();
    assert_eq!(standing_approval(&issued(&team).unwrap(), "carol", 7), None, "a relabelled device stays withdrawn");
}

/// `--earlier` follows a key back through the keys it was renewed from, live ones only, and none of another holder's.
#[test]
fn the_keys_a_key_was_renewed_from_are_its_live_predecessors() {
    let dir = ants_test_support::scratch_dir("renewed-from");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let ant = |name: &str| {
        let out = dir.join(format!("{name}.creds"));
        auth::issue(&team, &name.parse().unwrap(), ants_proto::Role::Ant, None, &out).unwrap();
        auth::read_creds(&out).unwrap()
    };
    let renewed = |jwt: &str| {
        let key = nkeys::KeyPair::new_user();
        (auth::renew(&team, jwt, &key.public_key(), None).unwrap().jwt, key.public_key())
    };
    let (jwt, first) = ant("dev-a");
    let first = first.public_key();
    let (jwt, second) = renewed(&jwt);
    let (_, third) = renewed(&jwt);
    ant("dev-b");

    let mut earlier = renewed_from(&team, std::slice::from_ref(&third)).unwrap();
    earlier.sort();
    let mut expected = vec![first.clone(), second.clone()];
    expected.sort();
    assert_eq!(earlier, expected);
    assert!(renewed_from(&team, std::slice::from_ref(&first)).unwrap().is_empty());
    // What a renewal's hint revokes for the key it replaced reaches back, never forward to the new key.
    assert_eq!(renewed_from(&team, std::slice::from_ref(&second)).unwrap(), std::slice::from_ref(&first));
    revoke(&team, None, std::slice::from_ref(&first)).unwrap();
    assert_eq!(renewed_from(&team, std::slice::from_ref(&third)).unwrap(), [second]);

    let mut keys = vec![third];
    keys.extend(renewed_from(&team, &keys).unwrap());
    revoke(&team, None, &keys).unwrap();
    let live = crate::signing::signer::live(&team).unwrap();
    assert!(!live.iter().any(|i| i.name == "dev-a"), "{live:?}");
    assert!(live.iter().any(|i| i.name == "dev-b"));
    fs::remove_dir_all(dir).unwrap();
}

/// The revoked keys of the account `key` in the `server.conf` of `team`.
fn revoked_in(team: &Path, key: &str) -> BTreeSet<String> {
    let conf = fs::read_to_string(team.join(auth::SERVER_CONF)).unwrap();
    let jwt = conf.lines().find_map(|l| l.trim().strip_prefix(&format!("{key}: ")).map(str::to_owned)).unwrap();
    let body = jwt.split('.').nth(1).unwrap();
    let claims: serde_json::Value =
        serde_json::from_slice(&data_encoding::BASE64URL_NOPAD.decode(body.as_bytes()).unwrap()).unwrap();
    claims["nats"]["revocations"].as_object().map_or_default(|r| r.keys().cloned().collect())
}

#[test]
fn a_leaf_s_store_and_system_uplinks_are_recorded_and_revoked_with_its_uplink_in_their_accounts() {
    use ants_daemon::secrets::creds;
    let dir = ants_test_support::scratch_dir("revoke-leaf-store");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let m1: ants_proto::Machine = "m1".parse().unwrap();
    let uplink = dir.join("leaf-m1.creds");
    let members = [("dev-a".parse().unwrap(), ants_proto::Role::Ant)];
    let approver = crate::node::members::EnrollApprover::Without;
    auth::issue_leaf(&team, &m1, &members, &[], approver, None, &uplink).unwrap();
    let uplink_finding = creds::check(&uplink).unwrap();
    let store_finding = creds::check(&auth::leaf_store_creds(&uplink)).unwrap();
    assert_eq!(store_finding.kind, creds::Kind::LeafStore { machine: m1.clone() });
    assert_eq!(store_finding.expires_at, uplink_finding.expires_at, "the uplink's lifetime");
    let system_finding = creds::check(&auth::leaf_system_creds(&uplink)).unwrap();
    assert_eq!(system_finding.kind, creds::Kind::LeafSystem { machine: m1 });
    assert_eq!(system_finding.expires_at, uplink_finding.expires_at, "the uplink's lifetime");

    let issued = issued(&team).unwrap();
    let of = |kind: Kind| issued.iter().find(|i| i.kind == kind).unwrap();
    let (up, store, system) = (of(Kind::Leaf), of(Kind::LeafStore), of(Kind::LeafSystem));
    assert_eq!((up.key.as_str(), store.key.as_str()), (uplink_finding.key.as_str(), store_finding.key.as_str()));
    assert_eq!(system.key, system_finding.key);
    assert_eq!((store.name.as_str(), system.name.as_str()), ("leaf-m1", "leaf-m1"));
    assert_eq!((store.expires_at, system.expires_at), (up.expires_at, up.expires_at));
    let listed = listing(&team, auth::now().unwrap()).unwrap();
    assert!(listed.iter().any(|l| l.issued.key == store.key && !l.revoked), "{listed:?}");

    let mut keys: Vec<String> = revoke(&team, Some("leaf-m1"), &[]).unwrap().into_iter().map(|r| r.key).collect();
    keys.sort();
    let mut expected = vec![up.key.clone(), store.key.clone(), system.key.clone()];
    expected.sort();
    assert_eq!(keys, expected);
    let (team_account, store_account) = (auth::account_key(&team).unwrap(), auth::store_key(&team).unwrap());
    let system_seed = fs::read_to_string(team.join(auth::SYSTEM_SEED)).unwrap();
    let system_account = nkeys::KeyPair::from_seed(system_seed.trim()).unwrap().public_key();
    assert_eq!(revoked_in(&team, &store_account), BTreeSet::from([store.key.clone()]));
    assert_eq!(revoked_in(&team, &system_account), BTreeSet::from([system.key.clone()]));
    assert_eq!(revoked_in(&team, &team_account), BTreeSet::from([up.key.clone()]));
    // A key the registry does not know is revoked in all three accounts.
    let unknown = nkeys::KeyPair::new_user().public_key();
    revoke(&team, None, std::slice::from_ref(&unknown)).unwrap();
    for account in [&store_account, &system_account, &team_account] {
        assert!(revoked_in(&team, account).contains(&unknown));
    }
    let listed = listing(&team, auth::now().unwrap()).unwrap();
    assert!(listed.iter().any(|l| l.issued.key == store.key && l.revoked), "{listed:?}");
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn revoking_a_leaf_s_uplink_by_name_drops_its_store_and_system_account_uplinks_at_the_hub() {
    use crate::node::leaf;
    let leaf_port = ants_test_support::free_port();
    let Some(hub) = crate::test_support::spawn_hub(leaf_port, "") else { return };
    let team = hub.dir().join("auth");
    let (m1, domain): (ants_proto::Machine, ants_proto::JsDomain) = ("m1".parse().unwrap(), "hub".parse().unwrap());
    let creds = hub.dir().join("leaf-m1.creds");
    let members = [("dev-a".parse().unwrap(), ants_proto::Role::Ant)];
    let approver = crate::node::members::EnrollApprover::Without;
    auth::issue_leaf(&team, &m1, &members, &[], approver, Some(&domain), &creds).unwrap();
    let server_conf = team.join(auth::SERVER_CONF);
    let system_creds = auth::leaf_system_creds(&creds);
    let listen = format!("127.0.0.1:{}", ants_test_support::free_port());
    let uplinks = [format!("nats-leaf://127.0.0.1:{leaf_port}")];
    let _leaf = ants_test_support::NatsServer::spawn_with(|dir| {
        let config = leaf::Leaf {
            machine: &m1,
            hub_domain: Some(&domain),
            uplinks: &uplinks,
            creds: &creds,
            system_creds: Some(&system_creds),
            server_conf: &server_conf,
            ca_file: None,
            proxy: None,
            listen: &listen,
        };
        leaf::write(&config, &dir.join("leaf.conf")).unwrap();
        "include \"leaf.conf\"\n".to_owned()
    })
    .unwrap();
    let system = connect_system(&hub.url, &team).await.unwrap();
    let (team_account, store_account) = (auth::account_key(&team).unwrap(), auth::store_key(&team).unwrap());
    let system_seed = fs::read_to_string(team.join(auth::SYSTEM_SEED)).unwrap();
    let system_account = nkeys::KeyPair::from_seed(system_seed.trim()).unwrap().public_key();
    let leaf_accounts = async || -> Vec<String> {
        let Ok(answer) = relayed::request(&system, "$SYS.REQ.SERVER.PING.LEAFZ", "".into()).await else {
            return Vec::new();
        };
        let answer: serde_json::Value = serde_json::from_slice(&answer.payload).unwrap_or_default();
        let leafs = answer["data"]["leafs"].as_array().map_or_default(Vec::as_slice);
        leafs.iter().filter_map(|l| l["account"].as_str().map(str::to_owned)).collect()
    };
    crate::test_support::eventually("the three uplinks connected", async || {
        let accounts = leaf_accounts().await;
        [&team_account, &store_account, &system_account].into_iter().all(|a| accounts.contains(a))
    })
    .await;

    revoke(&team, Some("leaf-m1"), &[]).unwrap();
    push(&hub.url, &team).await.unwrap();
    crate::test_support::eventually("the three uplinks dropped", async || leaf_accounts().await.is_empty()).await;
    // The leaf node keeps trying, refused.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(leaf_accounts().await, Vec::<String>::new());
}
