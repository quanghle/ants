//! Tests of a device's credentials: the NKey of a raw public key, which JWTs a device connects with, the bootstrap
//! credentials it registers with, and the answers its registration and renewal take.

use ants_proto::{LoginAnswer, LoginReply, LoginRequest, PERMISSIONS_VERSION, PermissionsVersion, RenewReply, WEB_TAG};
use chrono::Utc;
use data_encoding::BASE64URL_NOPAD;
use nkeys::{KeyPair, XKey};
use serde_json::json;

use super::registration::{Registration, Step, device_page};
use super::renewal::{Renewal, Renewed};
use super::*;
use crate::transport::Message;

const GITHUB: &str = "https://github.com";

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

/// A user JWT for `user`, signed by `account`, carrying `claims` under `nats` besides its type.
fn jwt(account: &KeyPair, user: &str, name: &str, nats: serde_json::Value, exp: Option<i64>) -> String {
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let mut nats = nats;
    nats["type"] = json!("user");
    let now = Utc::now().timestamp();
    let mut claims = json!({ "sub": user, "iss": account.public_key(), "name": name, "iat": now, "nats": nats });
    if let Some(exp) = exp {
        claims["exp"] = json!(now + exp);
    }
    let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
    let signature = BASE64URL_NOPAD.encode(&account.sign(format!("{header}.{body}").as_bytes()).unwrap());
    format!("{header}.{body}.{signature}")
}

/// The tags of web credentials of `device`, at `version`.
fn web_tags(device: DeviceId, version: PermissionsVersion) -> serde_json::Value {
    json!([WEB_TAG, device.tag(), version.tag()])
}

struct Team {
    account: KeyPair,
    bootstrap: Bootstrap,
}

impl Team {
    fn new() -> Self {
        let account = KeyPair::new_account();
        let key = KeyPair::new_user();
        let jwt = jwt(&account, &key.public_key(), ants_proto::BOOTSTRAP_USER, json!({}), None);
        let creds = format!(
            "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
             -----BEGIN USER NKEY SEED-----\n{}\n------END USER NKEY SEED------\n",
            key.seed().unwrap()
        );
        Self { bootstrap: Bootstrap::read(&creds).unwrap(), account }
    }

    /// Web credentials of alice's `device` for `key`, for a day.
    fn web(&self, key: &KeyPair, device: DeviceId) -> String {
        let nats = json!({ "tags": web_tags(device, PERMISSIONS_VERSION), "pub": { "allow": ["ants.renew.alice"] } });
        jwt(&self.account, &key.public_key(), "alice", nats, Some(86_400))
    }
}

#[test]
fn the_nkey_of_a_raw_public_key_is_the_one_nkeys_gives() {
    let key = KeyPair::new_user();
    let raw = &BASE32_NOPAD.decode(key.public_key().as_bytes()).unwrap()[1..33];
    assert_eq!(user_key(raw).unwrap(), key.public_key());
    assert!(matches!(user_key(&raw[1..]), Err(Error::Invalid(_))));
}

#[test]
fn a_device_connects_only_with_unexpired_web_credentials_of_its_key_of_no_newer_permissions() {
    let team = Team::new();
    let (key, device) = (KeyPair::new_user(), DeviceId::generate());
    let account = team.account.public_key();
    let good = team.web(&key, device);

    let credentials = Credentials::read(&good, &key.public_key(), Some(&account)).unwrap();
    assert_eq!((&credentials.human, credentials.device, &credentials.domain), (&p("alice"), Some(device), &None));
    assert_eq!(credentials.account, account);
    let (issued, expires) = (credentials.issued_at.unwrap(), credentials.expires_at.unwrap());
    assert_eq!(credentials.renew_at(), Some(issued + (expires - issued) * 2 / 3));
    assert_eq!(credentials.revoke_command(), format!("ants auth revoke --key {}", key.public_key()));
    let session = credentials.session();
    assert_eq!((session.account.as_deref(), session.name.as_str()), (Some(account.as_str()), CONNECTION_NAME));
    assert_eq!(Credentials::read(&good, &key.public_key(), None).unwrap(), credentials, "the issuer it names");

    let refused = |jwt: &str, key: &str, account: &str| Credentials::read(jwt, key, Some(account)).unwrap_err();
    assert!(matches!(refused(&good, &KeyPair::new_user().public_key(), &account), Error::Credentials(_)));
    assert!(matches!(refused(&good, &key.public_key(), &KeyPair::new_account().public_key()), Error::Credentials(_)));

    let tags = json!({ "tags": [device.tag(), PERMISSIONS_VERSION.tag()] });
    let not_web = jwt(&team.account, &key.public_key(), "alice", tags, None);
    let Error::Credentials(why) = refused(&not_web, &key.public_key(), &account) else { panic!() };
    assert!(why.contains("--web"), "{why}");

    let newer = PermissionsVersion(PERMISSIONS_VERSION.0 + 1);
    let newer = jwt(&team.account, &key.public_key(), "alice", json!({ "tags": web_tags(device, newer) }), None);
    let ours = PERMISSIONS_VERSION;
    let theirs = PermissionsVersion(ours.0 + 1);
    assert_eq!(refused(&newer, &key.public_key(), &account), Error::NewerPermissions { theirs, ours });

    let tags = web_tags(device, PERMISSIONS_VERSION);
    let expired = jwt(&team.account, &key.public_key(), "alice", json!({ "tags": tags }), Some(-1));
    assert!(matches!(refused(&expired, &key.public_key(), &account), Error::Credentials(_)));

    let nats =
        json!({ "tags": tags, "pub": { "allow": ["ants.renew.alice", "$JS.hub.API.DIRECT.GET.KV_ants_colony"] } });
    let in_hub = jwt(&team.account, &key.public_key(), "alice", nats, None);
    let credentials = Credentials::read(&in_hub, &key.public_key(), Some(&account)).unwrap();
    assert_eq!(credentials.renew_at(), None, "never expiring");
    assert_eq!(credentials.domain.map(|d| d.to_string()), Some("hub".to_owned()));
}

#[test]
fn without_authentication_a_device_is_the_human_it_names() {
    let session = unauthenticated("alice".parse().unwrap());
    assert_eq!((session.human.as_str(), session.domain, session.account), ("alice", None, None));
    assert_eq!(session.capabilities, Capabilities::all());
    assert!(session.capabilities.teams_write && !session.capabilities.stale_permissions);
}

#[test]
fn what_credentials_let_a_page_do_is_what_their_rights_allow_whatever_their_version() {
    let team = Team::new();
    let (key, device) = (KeyPair::new_user(), DeviceId::generate());
    let account = team.account.public_key();
    let read = |nats: serde_json::Value| {
        let jwt = jwt(&team.account, &key.public_key(), "alice", nats, None);
        Credentials::read(&jwt, &key.public_key(), Some(&account)).unwrap().capabilities
    };
    let rights =
        |rights: &Permissions| json!({ "pub": { "allow": rights.publish }, "sub": { "allow": rights.subscribe } });
    let tagged = |mut nats: serde_json::Value, version| {
        nats["tags"] = web_tags(device, version);
        nats
    };

    let web = Permissions::for_web(&p("alice"));
    let all = read(tagged(rights(&web), PERMISSIONS_VERSION));
    let expected = Capabilities { permissions_version: Some(PERMISSIONS_VERSION), ..Capabilities::all() };
    assert_eq!(all, expected);
    assert_eq!(credentials_session(&team, &key, device, &web).capabilities, expected, "carried by the session");

    let hub: JsDomain = "hub".parse().unwrap();
    let in_hub = read(tagged(rights(&web.clone().with_js_domain(Some(&hub))), PERMISSIONS_VERSION));
    assert_eq!(in_hub, expected, "the requests in the hub's domain");

    let without = |dropped: &[&str]| Permissions {
        publish: web.publish.iter().filter(|s| !dropped.iter().any(|d| s.contains(d))).cloned().collect(),
        subscribe: web.subscribe.clone(),
    };
    let reads_only = read(tagged(rights(&without(&["$KV.ants_teams"])), PERMISSIONS_VERSION));
    assert_eq!((reads_only.teams_read, reads_only.teams_write), (true, false));
    let no_teams = read(tagged(rights(&without(&["ants_teams"])), PERMISSIONS_VERSION));
    assert_eq!((no_teams.teams_read, no_teams.teams_write), (false, false));
    let no_identities = read(tagged(rights(&without(&["KV_ants_identities"])), PERMISSIONS_VERSION));
    assert_eq!((no_identities.teams_read, no_identities.teams_write), (true, false));

    let older = read(tagged(rights(&web), PermissionsVersion(0)));
    assert_eq!((older.permissions_version, older.stale_permissions), (Some(PermissionsVersion(0)), true));
    assert!(older.teams_write, "rights tell, not the version");
    let mut unversioned = rights(&web);
    unversioned["tags"] = json!([WEB_TAG, device.tag()]);
    let unversioned = read(unversioned);
    assert_eq!((unversioned.permissions_version, unversioned.stale_permissions), (None, true));
    let listing_none = read(tagged(json!({}), PERMISSIONS_VERSION));
    assert!(listing_none.teams_write && listing_none.memories_read, "a JWT listing no subjects allows them all");
}

/// The session of web credentials of `device` for `key` with `rights`.
fn credentials_session(team: &Team, key: &KeyPair, device: DeviceId, rights: &Permissions) -> Session {
    let nats = json!({
        "tags": web_tags(device, PERMISSIONS_VERSION),
        "pub": { "allow": rights.publish },
        "sub": { "allow": rights.subscribe },
    });
    let jwt = jwt(&team.account, &key.public_key(), "alice", nats, None);
    Credentials::read(&jwt, &key.public_key(), Some(&team.account.public_key())).unwrap().session()
}

#[test]
fn bootstrap_credentials_read_only_when_they_are_the_team_s_bootstrap_ones_with_their_seed() {
    let team = Team::new();
    assert_eq!(team.bootstrap.account(), team.account.public_key());
    let nonce = b"nonce";
    let signature = BASE64URL_NOPAD.decode(team.bootstrap.sign(nonce).unwrap().as_bytes()).unwrap();
    KeyPair::from_public_key(&team.bootstrap.public_key()).unwrap().verify(nonce, &signature).unwrap();

    let creds = |jwt: &str, seed: &str| {
        format!(
            "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
             -----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
        )
    };
    let key = KeyPair::new_user();
    let alice = jwt(&team.account, &key.public_key(), "alice", json!({}), None);
    let Err(Error::Bootstrap(why)) = Bootstrap::read(&creds(&alice, &key.seed().unwrap())) else { panic!() };
    assert!(why.contains("not the bus's bootstrap"), "{why}");
    let other = KeyPair::new_user().seed().unwrap();
    assert!(Bootstrap::read(&creds(team.bootstrap.jwt(), &other)).is_err(), "a seed of another key");
    assert!(Bootstrap::read("no creds").is_err());
}

#[test]
fn the_device_page_is_the_https_one_of_the_trusted_github() {
    assert_eq!(device_page("https://github.com").unwrap(), "https://github.com/login/device");
    assert_eq!(device_page("HTTPS://GitHub.Example:8443/").unwrap(), "https://github.example:8443/login/device");
    for wrong in ["http://github.com", "https://github.com/login", "github.com", "https://", "https://a b"] {
        assert!(matches!(device_page(wrong), Err(Error::Invalid(_))), "{wrong}");
    }
}

/// The signer's answer to `registration`, sealed to its reply key.
fn answering(registration: &Registration, answer: &LoginAnswer) -> Message {
    let request: LoginRequest = serde_json::from_slice(&registration.request().payload).unwrap();
    let (signer, to) = (XKey::new(), XKey::from_public_key(&request.reply_xkey).unwrap());
    let sealed = signer.seal(&serde_json::to_vec(answer).unwrap(), &to).unwrap();
    let reply = LoginReply {
        name: request.name,
        public_key: request.public_key,
        sender: signer.public_key(),
        sealed: BASE64URL_NOPAD.encode(&sealed),
    };
    Message::new(reply.subject(), serde_json::to_vec(&reply).unwrap())
}

#[test]
fn a_registration_shows_only_the_trusted_page_and_takes_only_the_device_s_credentials() {
    let team = Team::new();
    let key = KeyPair::new_user();
    let label = Some(" ".to_owned());
    let mut registration = Registration::new(p("alice"), key.public_key(), label, &team.bootstrap, GITHUB).unwrap();
    registration.sign(BASE64URL_NOPAD.encode(&key.sign(&registration.signed()).unwrap()));
    let request: LoginRequest = serde_json::from_slice(&registration.request().payload).unwrap();
    assert!(request.web && request.xkey.is_none() && request.label.is_none());
    let signature = BASE64URL_NOPAD.decode(request.signature.as_bytes()).unwrap();
    key.verify(&request.signed(), &signature).unwrap();
    assert_eq!(registration.request().subject, "ants.login.alice");
    assert_eq!(registration.replies(), "ants.login.alice.reply");

    let expires_at = Utc::now();
    let verify = |url: &str| LoginAnswer::Verify { url: url.into(), user_code: "CODE".into(), expires_at };
    let answer = |answer: &LoginAnswer| registration.answer(&answering(&registration, answer));
    let page = "https://github.com/login/device".to_owned();
    let shown = Step::Verify { page: page.clone(), user_code: "CODE".into(), expires_at };
    assert_eq!(answer(&verify("https://GITHUB.com/login/device")), Some(Ok(shown)));
    assert_eq!(answer(&verify("https://github.evil/login/device")), Some(Err(Error::NotShown { trusted: page })));
    let waiting = LoginAnswer::Waiting { login: "octocat".into(), reason: "no member".into() };
    assert!(matches!(answer(&waiting), Some(Ok(Step::Waiting { .. }))));
    let refused = LoginAnswer::Refused { reason: "no".into() };
    assert_eq!(answer(&refused), Some(Err(Error::Refused("no".into()))));

    let device = DeviceId::generate();
    let signed = |jwt: String| answer(&LoginAnswer::Signed { jwt });
    let Some(Ok(Step::Signed { credentials, .. })) = signed(team.web(&key, device)) else { panic!() };
    assert_eq!((credentials.device, credentials.human), (Some(device), p("alice")));
    assert_eq!(signed(team.web(&KeyPair::new_user(), device)), None, "another key's");
    assert_eq!(signed(Team::new().web(&key, device)), None, "another account's");
    let nats = json!({ "tags": web_tags(device, PERMISSIONS_VERSION) });
    assert_eq!(signed(jwt(&team.account, &key.public_key(), "bob", nats, None)), None, "another human's");

    let mut other = answering(&registration, &refused);
    let mut reply: LoginReply = serde_json::from_slice(&other.payload).unwrap();
    reply.public_key = KeyPair::new_user().public_key();
    other.payload = serde_json::to_vec(&reply).unwrap();
    assert_eq!(registration.answer(&other), None, "an answer to another request");
    let sealed_elsewhere = Registration::new(p("alice"), key.public_key(), None, &team.bootstrap, GITHUB).unwrap();
    assert_eq!(registration.answer(&answering(&sealed_elsewhere, &refused)), None, "one it cannot open");
}

#[test]
fn a_renewal_takes_only_the_new_key_s_credentials_of_the_same_human_and_account() {
    let team = Team::new();
    let (key, next, device) = (KeyPair::new_user(), KeyPair::new_user(), DeviceId::generate());
    let jwt_now = team.web(&key, device);
    let current = Credentials::read(&jwt_now, &key.public_key(), None).unwrap();
    let mut renewal = Renewal::new(&current, &jwt_now, next.public_key(), "a browser".into());
    renewal.sign(BASE64URL_NOPAD.encode(&key.sign(&renewal.signed()).unwrap()));
    let request: ants_proto::RenewRequest = serde_json::from_slice(&renewal.request().payload).unwrap();
    assert_eq!((request.current.as_str(), request.public_key.as_str()), (jwt_now.as_str(), next.public_key().as_str()));
    let signature = BASE64URL_NOPAD.decode(request.signature.as_bytes()).unwrap();
    key.verify(&request.signed(), &signature).unwrap();
    assert_eq!(
        (renewal.request().subject.as_str(), renewal.replies()),
        ("ants.renew.alice", "ants.renew.alice.reply".into())
    );

    let reply = |public_key: String, jwt: String, refused: Option<&str>| {
        let reply = RenewReply { name: "alice".into(), public_key, jwt, refused: refused.map(str::to_owned) };
        renewal.answer(&Message::new(renewal.replies(), serde_json::to_vec(&reply).unwrap()))
    };
    let renewed = team.web(&next, device);
    let Some(Ok(Renewed { credentials, .. })) = reply(next.public_key(), renewed.clone(), None) else { panic!() };
    assert_eq!((credentials.public_key, credentials.device), (next.public_key(), Some(device)));
    assert_eq!(reply(KeyPair::new_user().public_key(), renewed, None), None, "an answer to another key");
    assert_eq!(reply(next.public_key(), String::new(), Some("revoked")), Some(Err(Error::Refused("revoked".into()))));
    assert_eq!(reply(next.public_key(), Team::new().web(&next, device), None), None, "another account's");
    assert_eq!(reply(next.public_key(), team.web(&key, device), None), None, "the old key's");
}

#[test]
fn errors_tell_their_kind_to_the_page() {
    let error = Error::NotShown { trusted: "https://github.com/login/device".into() };
    let shown = json!({ "kind": "not_shown", "detail": { "trusted": "https://github.com/login/device" } });
    assert_eq!(serde_json::to_value(&error).unwrap(), shown);
    assert_eq!(serde_json::to_value(Error::Expired).unwrap(), json!({ "kind": "expired" }));
}
