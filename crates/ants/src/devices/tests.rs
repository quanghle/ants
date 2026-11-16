use ants_daemon::devices::verify;
use ants_proto::{DeviceKey, Permissions, Role};
use data_encoding::BASE64URL_NOPAD;
use nkeys::KeyPair;
use serde_json::Value;

use super::*;
use crate::auth::tests::Server;

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

/// Issue credentials of `user` in `role` from the team in `team`, as `ants auth user` does, returning their file's
/// text.
fn issue(team: &Path, user: &UserId, role: Role) -> String {
    let out = team.join(format!("{user}-{role:?}.creds"));
    let permissions = Permissions::for_user(user, role);
    auth::issue_device(team, user, role.into(), permissions, None, &out).unwrap();
    std::fs::read_to_string(out).unwrap()
}

fn account(team: &Path) -> KeyPair {
    let seed = std::fs::read_to_string(team.join(auth::ACCOUNT_SEED)).unwrap();
    KeyPair::from_seed(seed.trim()).unwrap()
}

/// `claims` as a JWT signed by `issuer`, whatever they say of their issuer.
fn signed(issuer: &KeyPair, claims: &Value) -> String {
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(&serde_json::to_vec(claims).unwrap()));
    let signature = issuer.sign(signed.as_bytes()).unwrap();
    format!("{signed}.{}", BASE64URL_NOPAD.encode(&signature))
}

#[test]
fn a_reader_takes_a_key_only_from_a_jwt_it_verifies() {
    let dir = ants_test_support::scratch_dir("devices-verify");
    let (team, other_team) = (dir.join("auth"), dir.join("other"));
    auth::init(&team).unwrap();
    auth::init(&other_team).unwrap();
    let (alice, account) = (p("alice"), account(&team));
    let own = Own::from_creds_text(&issue(&team, &alice, Role::Human)).expect("a human's credentials bind a key");
    let (_, claims) = auth::verified_body(&own.jwt).unwrap();
    let now = auth::now().unwrap();
    let ok = |jwt: &str| verify(&account.public_key(), &alice, &own.device, jwt, now);
    let verified = ok(&own.jwt).unwrap();
    assert_eq!((verified.human.clone(), verified.device), (alice.clone(), own.device));
    assert_eq!(verified.user_key, claims["sub"]);
    assert_eq!(verified.key.tag(), Finding::from_jwt(&own.jwt).unwrap().xkey.unwrap().tag());

    let other = Own::from_creds_text(&issue(&other_team, &alice, Role::Human)).unwrap();
    let theirs = verify(&account.public_key(), &alice, &other.device, &other.jwt, now).unwrap_err();
    assert!(theirs.contains("not by the team's account"), "{theirs}");
    let reason = |claims: &Value, by: &KeyPair| ok(&signed(by, claims)).unwrap_err();
    let mut forged = claims.clone();
    forged["name"] = "bob".into();
    let tampered = {
        let (header, rest) = own.jwt.split_once('.').unwrap();
        let signature = rest.split_once('.').unwrap().1;
        let body = BASE64URL_NOPAD.encode(&serde_json::to_vec(&forged).unwrap());
        format!("{header}.{body}.{signature}")
    };
    assert!(ok(&tampered).unwrap_err().contains("not signed by its issuer"));
    assert_eq!(reason(&forged, &account), "names bob, not alice");
    let mut expired = claims.clone();
    expired["exp"] = now.into();
    assert_eq!(reason(&expired, &account), format!("expired at {now}"));
    let mut keyless = claims.clone();
    let tags = keyless["nats"]["tags"].as_array_mut().unwrap();
    tags.retain(|t| !t.as_str().unwrap().starts_with(DeviceKey::TAG));
    assert_eq!(reason(&keyless, &account), "binds no encryption key");
    let elsewhere = verify(&account.public_key(), &alice, &DeviceId::generate(), &own.jwt, now).unwrap_err();
    assert_eq!(elsewhere, format!("for device {}", own.device));
    let mut not_a_user = claims;
    not_a_user["sub"] = account.public_key().into();
    assert!(reason(&not_a_user, &account).contains("not the public key of a user"));

    assert!(auth::check_not_reserved(&p("revoked")).is_err(), "no human puts the revocation records");
    let ant = issue(&team, &alice, Role::Ant);
    assert_eq!(Own::from_creds_text(&ant), None, "an ant's credentials bind no key");
    let jwt = auth::creds_jwt(&ant).unwrap();
    assert_eq!(ok(jwt).unwrap_err(), "not a human's credentials");
}

#[test]
fn revocations_of_humans_assistants_ants_and_unknown_keys_are_recorded_and_no_others() {
    let dir = ants_test_support::scratch_dir("devices-revocations");
    let team = dir.join("auth");
    auth::init(&team).unwrap();
    let human = Own::from_creds_text(&issue(&team, &p("alice"), Role::Human)).unwrap();
    let human_key = auth::verified(&human.jwt).unwrap().sub;
    let ant = crate::humans::creds_claims(&issue(&team, &p("dev-a"), Role::Ant)).unwrap();
    let ant_key = ant["sub"].as_str().unwrap().to_owned();
    let host = dir.join("host-m1.creds");
    auth::issue_host(&team, &"m1".parse().unwrap(), None, &host).unwrap();
    let host_key = auth::read_creds(&host).unwrap().1.public_key();
    let out = dir.join("pa.creds");
    let new = auth::NewDevice { assistant_of: Some(p("alice")), ..auth::NewDevice::default() };
    let rights = Permissions::for_assistant(&p("pa"), &p("alice"));
    auth::issue_device_with(&team, &p("pa"), revoke::Kind::Assistant, rights, &new, &out).unwrap();
    let assistant_key = auth::read_creds(&out).unwrap().1.public_key();
    let unknown = KeyPair::new_user().public_key();
    assert!(device_revocations(&team).unwrap().is_empty());
    revoke::revoke(&team, Some("dev-a"), std::slice::from_ref(&unknown)).unwrap();
    revoke::revoke(&team, Some("alice"), &[]).unwrap();
    revoke::revoke(&team, Some("pa"), &[]).unwrap();
    revoke::revoke(&team, Some("host-m1"), &[]).unwrap();
    let mut recorded: Vec<String> = device_revocations(&team).unwrap().into_iter().map(|r| r.key).collect();
    recorded.sort();
    assert!(!recorded.contains(&host_key), "a host's key signs nothing readers check");
    let mut expected = vec![unknown, human_key, assistant_key, ant_key];
    expected.sort();
    assert_eq!(recorded, expected);
}

/// Each human puts its own device's entry and no other's, nor a revocation record; an ant reads none of it; a reader
/// skips the entries it cannot verify and those of revoked keys, which only `ants auth push` records.
#[tokio::test]
async fn humans_publish_their_devices_and_readers_take_only_verified_unrevoked_keys() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let account = auth::account_key(&team).unwrap();
    let (alice, bob) = (p("alice"), p("bob"));
    let alice_creds = server.creds_as(&alice, Role::Human);
    let bob_creds = server.creds_as(&bob, Role::Human);
    let (alice_own, bob_own) =
        (Own::from_creds(Some(&alice_creds)).unwrap().unwrap(), Own::from_creds(Some(&bob_creds)).unwrap().unwrap());

    // Connecting puts the device's entry, once.
    let nats = crate::Nats::new(&server.url, Some(&alice_creds), None).unwrap();
    assert_eq!(nats.account.as_deref(), Some(account.as_str()));
    let (_, alice_js) = nats.connect("test").await.unwrap();
    let directory = Directory::open(&alice_js, &account).await.unwrap();
    let Lookup::Verified(found) = directory.device(&alice, &alice_own.device).await.unwrap() else {
        panic!("alice's device is not in the directory")
    };
    assert_eq!(found.device, alice_own.device);
    assert!(!publish(&alice_js, &alice_own).await.unwrap(), "unchanged, so not put again");
    let (_, bob_js) = bus::connect(&server.url, Some(&bob_creds), None, &bob, "test").await.unwrap();
    assert!(publish(&bob_js, &bob_own).await.unwrap());

    let devices = directory.devices(&[alice.clone(), bob.clone()]).await.unwrap();
    let mut verified: Vec<(UserId, DeviceId)> = devices.verified.iter().map(|d| (d.human.clone(), d.device)).collect();
    verified.sort();
    assert_eq!(verified, [(alice.clone(), alice_own.device), (bob.clone(), bob_own.device)]);
    assert!(devices.skipped.is_empty(), "{:?}", devices.skipped);
    assert_eq!(directory.devices(std::slice::from_ref(&bob)).await.unwrap().of(&alice).count(), 0);

    // Alice puts no entry of bob's, nor a revocation record, so she cannot swap or drop his key.
    let (_, alice_raw) = server.connect_as(&alice, Role::Human).await;
    let kv = alice_raw.get_key_value(DEVICES_BUCKET).await.unwrap();
    let bob_entry = device_entry_key(&bob, &bob_own.device);
    assert!(kv.put(&bob_entry, alice_own.jwt.clone().into()).await.is_err(), "alice put an entry of bob's");
    let bob_key = auth::verified(&bob_own.jwt).unwrap().sub;
    assert!(kv.put(revoked_device_key(&bob_key), "{}".into()).await.is_err(), "alice revoked bob's key");
    // What she puts under her own name that is not her device's JWT is skipped.
    let decoy = DeviceId::generate();
    kv.put(device_entry_key(&alice, &decoy), bob_own.jwt.clone().into()).await.unwrap();
    kv.put(format!("{alice}.not-a-device"), alice_own.jwt.clone().into()).await.unwrap();
    let devices = directory.devices(std::slice::from_ref(&alice)).await.unwrap();
    assert_eq!(devices.verified.iter().map(|d| d.device).collect::<Vec<_>>(), [alice_own.device]);
    let mut skipped: Vec<String> = devices.skipped.iter().map(ToString::to_string).collect();
    skipped.sort();
    assert_eq!(
        skipped,
        [
            format!("{DEVICES_BUCKET} entry alice.{decoy}: names bob, not alice"),
            format!("{DEVICES_BUCKET} entry alice.not-a-device: not the key of a device's entry"),
        ]
    );
    assert!(matches!(directory.device(&alice, &decoy).await.unwrap(), Lookup::Skipped(_)));
    assert_eq!(directory.device(&bob, &decoy).await.unwrap(), Lookup::Missing);

    // An ant reads none of it.
    let (_, ant_js) = server.connect_as(&p("dev-a"), Role::Ant).await;
    assert!(ant_js.get_key_value(DEVICES_BUCKET).await.is_err());
    assert!(Directory::open(&ant_js, &account).await.is_err());

    // Revoking bob's credentials and pushing records the revocation, once; readers then skip his device.
    revoke::revoke(&team, Some("bob"), &[]).unwrap();
    crate::cmd::auth::push(&server.url, None, &team).await.unwrap();
    assert_eq!(record_revocations(&server.url, None, &team).await.unwrap(), Vec::<String>::new(), "recorded already");
    let record: DeviceRevocation =
        serde_json::from_slice(&kv.get(revoked_device_key(&bob_key)).await.unwrap().unwrap()).unwrap();
    assert_eq!(record.key, bob_key);
    let devices = directory.devices(&[alice.clone(), bob.clone()]).await.unwrap();
    assert_eq!(devices.of(&bob).count(), 0);
    let revoked = format!("{DEVICES_BUCKET} entry {bob_entry}: its credentials {bob_key} are revoked");
    assert!(devices.skipped.iter().any(|s| s.to_string() == revoked), "{:?}", devices.skipped);
    let Lookup::Skipped(skipped) = directory.device(&bob, &bob_own.device).await.unwrap() else { panic!() };
    assert_eq!(skipped.to_string(), revoked);

    // A hub out of reach leaves the records to the next push.
    let port = ants_test_support::free_port();
    let unreachable = format!("nats://127.0.0.1:{port}");
    revoke::revoke(&team, None, &[KeyPair::new_user().public_key()]).unwrap();
    assert!(record_revocations(&unreachable, None, &team).await.is_err());
    assert_eq!(record_revocations(&server.url, None, &team).await.unwrap().len(), 1);
}

/// Before any human put a key, the directory is empty, not an error.
#[tokio::test]
async fn a_directory_without_its_bucket_is_empty() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let account = auth::account_key(&server.dir().join("auth")).unwrap();
    let directory = Directory::open(&js, &account).await.unwrap();
    assert_eq!(directory.devices(std::slice::from_ref(&alice)).await.unwrap(), Devices::default());
    assert_eq!(directory.device(&alice, &DeviceId::generate()).await.unwrap(), Lookup::Missing);
}

/// Revoking an ant by name deletes its identity, so an ant later given the name is born afresh; a bucket not created
/// yet holds nothing to delete.
#[tokio::test]
async fn revoking_an_ant_by_name_deletes_its_identity() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    issue(&team, &dev_a, Role::Ant);
    forget_identity(&server.url, None, &team, "dev-a").await.unwrap();

    let (_, js) = server.connect_as(&p("alice"), Role::Human).await;
    let kv = js.create_key_value(ants_daemon::hub::identities_config()).await.unwrap();
    for ant in [&dev_a, &dev_b] {
        let record = ants_proto::AntIdentity::new(ant.clone(), format!("{ant}-id"), chrono::Utc::now());
        kv.put(ant.as_str(), serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    }
    crate::cmd::auth::revoke_credentials(
        Some("dev-a".to_owned()),
        vec![],
        vec![],
        false,
        Some(team),
        &crate::Connection { nats: &server.url, creds: None, js_domain: None },
    )
    .await
    .unwrap();
    let read = DirectKv::whole(&js, None, IDENTITIES_BUCKET).await.unwrap();
    assert!(read.get("dev-a").await.unwrap().is_none(), "dev-a's identity is left");
    assert!(read.get("dev-b").await.unwrap().is_some(), "dev-b's identity is gone");
}
