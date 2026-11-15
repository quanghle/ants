use tokio::sync::mpsc;
use tokio::time::timeout;

use super::*;
use crate::auth::tests::Server;

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
    auth::issue_enrollment(&team, &"dev-c".parse().unwrap(), Duration::from_secs(1), &enrollment).unwrap();
    let revoked_now = revoke(&team, Some("enroll-dev-c"), std::slice::from_ref(&laptop)).unwrap();
    assert_eq!(revoked_now.len(), 2);
    assert_eq!(revoked(&team).unwrap().len(), 3, "laptop's revocation is replaced, not repeated");
    std::thread::sleep(Duration::from_secs(2));
    apply(&team).unwrap();
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
    let claims = crate::humans::creds_claims(&text).unwrap();
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
