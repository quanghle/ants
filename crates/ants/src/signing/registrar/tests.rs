//! Tests of device registration: GitHub users under the policy, human decisions outside it, web devices, renewals
//! while the grant holds, and the sweep revoking members who left.

use std::future::pending;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ants_daemon::bus;
use ants_daemon::secrets::creds::{Finding, Kind as CredsKind};
use ants_daemon::secrets::keystore;
use ants_proto::{AntRequest, DeviceKey, LoginReply, Role, UserId, Verdict, subject};
use async_nats::jetstream::stream::Stream;
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;
use nkeys::KeyPair;
use tokio::time::timeout;

use super::offboard::{Offboarded, Offboarder};
use super::*;
use crate::credentials::{issued, renew, revoke};
use crate::signing::signer;
use crate::test_support::Server;
use crate::test_support::github::fake;
use crate::{auth, tools};

const WAIT: Duration = Duration::from_secs(20);
const REPEAT: Duration = Duration::from_millis(200);

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

type Users = &'static [(u64, &'static str, &'static str, bool)];

/// A fake GitHub where the nth sign-in is of the nth of `users` (id, login, verified email, member of acme):
/// each device flow is pending at its first poll.
async fn github(users: Users) -> String {
    fake(Arc::new(signing_in(users))).await
}

/// The routes of [`github`].
fn signing_in(users: Users) -> impl Fn(&str, Option<&str>, &str) -> (u16, String) + Send + Sync + 'static {
    let flows = Arc::new(Mutex::new((0_usize, Vec::<String>::new())));
    move |what, auth, body| {
        let token = auth.and_then(|a| a.strip_prefix("Bearer tok-")).and_then(|n| n.parse::<usize>().ok());
        let user = token.and_then(|n| users.get(n));
        match (what, user) {
            ("POST /login/device/code", _) if body.contains("client_id=Iv1.test") => {
                let mut flows = flows.lock().unwrap();
                flows.0 += 1;
                let n = flows.0 - 1;
                let code = format!(
                    r#"{{"device_code":"dc-{n}","user_code":"CODE-{n}","verification_uri":"https://github.example/login/device","expires_in":900,"interval":0}}"#
                );
                (200, code)
            }
            ("POST /login/oauth/access_token", _) => {
                let code = body.split('&').find_map(|kv| kv.strip_prefix("device_code=")).unwrap().to_owned();
                let mut flows = flows.lock().unwrap();
                if flows.1.contains(&code) {
                    let n = code.trim_start_matches("dc-");
                    (
                        200,
                        format!(r#"{{"access_token":"tok-{n}","token_type":"bearer","scope":"read:org,user:email"}}"#),
                    )
                } else {
                    flows.1.push(code);
                    (200, r#"{"error":"authorization_pending"}"#.to_owned())
                }
            }
            ("GET /user", Some((id, login, _, _))) => (200, format!(r#"{{"id":{id},"login":"{login}"}}"#)),
            ("GET /user/emails", Some((_, _, email, _))) => {
                (200, format!(r#"[{{"email":"{email}","verified":true}},{{"email":"x@acme.dev","verified":false}}]"#))
            }
            ("GET /user/memberships/orgs/acme", Some((.., true))) => (200, r#"{"state":"active"}"#.to_owned()),
            _ => (404, r#"{"message":"Not Found"}"#.to_owned()),
        }
    }
}

/// A fake GitHub as [`github`], where the signer's token `signer` also looks `users` up by id, and finds those
/// of them members of acme while `still` holds.
async fn github_checking(users: Users, still: Arc<AtomicBool>) -> String {
    let signing_in = signing_in(users);
    fake(Arc::new(move |what, auth, body| {
        if auth != Some("Bearer signer") {
            return signing_in(what, auth, body);
        }
        let id = what.strip_prefix("GET /user/").and_then(|id| id.parse::<u64>().ok());
        if let Some((id, login, ..)) = users.iter().find(|u| Some(u.0) == id) {
            return (200, format!(r#"{{"id":{id},"login":"{login}"}}"#));
        }
        let login = what.strip_prefix("GET /orgs/acme/members/");
        match users.iter().find(|u| Some(u.1) == login) {
            Some((.., true)) if still.load(Ordering::SeqCst) => (204, String::new()),
            _ => (404, r#"{"message":"Not Found"}"#.to_owned()),
        }
    }))
    .await
}

/// A machine asking to register a device of `name`, with the bootstrap credentials' `client`.
struct Device {
    key: KeyPair,
    reply: XKey,
    request: LoginRequest,
}

impl Device {
    fn new(name: &str) -> Self {
        let (key, reply) = (KeyPair::new_user(), XKey::new());
        let mut request = LoginRequest::new(p(name), key.public_key(), reply.public_key());
        request.label = Some("laptop".to_owned());
        request.xkey = Some(XKey::new().public_key().parse::<DeviceKey>().unwrap());
        request.signature = BASE64URL_NOPAD.encode(&key.sign(&request.signed()).unwrap());
        Self { key, reply, request }
    }

    /// As [`Device::new`], asking for a web device, which names no encryption key.
    fn web(name: &str) -> Self {
        let mut device = Self::new(name);
        device.request.web = true;
        device.request.xkey = None;
        device.request.label = Some("browser".to_owned());
        device.sign();
        device
    }

    fn sign(&mut self) {
        self.request.signature = BASE64URL_NOPAD.encode(&self.key.sign(&self.request.signed()).unwrap());
    }

    /// Ask every [`REPEAT`] until an answer settles the request or `within` passes: the answers opened.
    async fn ask(&self, client: &async_nats::Client, within: Duration) -> Vec<LoginAnswer> {
        let mut replies = client.subscribe(subject::login_reply(&self.request.name)).await.unwrap();
        let payload = serde_json::to_vec(&self.request).unwrap();
        let mut answers = Vec::new();
        let _ = timeout(within, async {
            let mut repeat = tokio::time::interval(REPEAT);
            loop {
                tokio::select! {
                    _ = repeat.tick() => client.publish(self.request.subject(), payload.clone().into()).await.unwrap(),
                    msg = replies.next() => {
                        let reply: LoginReply = serde_json::from_slice(&msg.unwrap().payload).unwrap();
                        if !reply.answers(&self.request.public_key) {
                            continue;
                        }
                        let sender = XKey::from_public_key(&reply.sender).unwrap();
                        let sealed = BASE64URL_NOPAD.decode(reply.sealed.as_bytes()).unwrap();
                        let answer: LoginAnswer = serde_json::from_slice(&self.reply.open(&sealed, &sender).unwrap()).unwrap();
                        let settled = answer.is_final();
                        answers.push(answer);
                        if settled {
                            return;
                        }
                    }
                }
            }
        })
        .await;
        answers
    }
}

fn refusal(answers: &[LoginAnswer]) -> String {
    match answers.last() {
        Some(LoginAnswer::Refused { reason }) => reason.clone(),
        other => panic!("no refusal: {other:?} of {answers:?}"),
    }
}

/// The `[registration]` table of a config file, for the fake GitHub at `url`, signing members of acme for 7 days.
fn policy(url: &str) -> String {
    format!(
        "[registration]\nclient_id = \"Iv1.test\"\norgs = [\"acme\"]\nttl = \"7d\"\ngithub_url = \"{url}\"\napi_url = \"{url}\"\n"
    )
}

/// A connection to `server` with new bootstrap credentials of the team `team`.
async fn bootstrap(server: &Server, team: &Path) -> async_nats::Client {
    let creds = server.dir().join("bootstrap.creds");
    auth::issue_bootstrap(team, Duration::ZERO, &creds).unwrap();
    async_nats::ConnectOptions::with_credentials_file(&creds).await.unwrap().connect(&server.url).await.unwrap()
}

/// Run `asking` while `ants signer` answers over `server` for the team `team` with the policy of `config`.
async fn registering(server: &Server, team: &Path, config: &Path, asking: impl Future<Output = ()>) {
    let options = signer::Options { config, approval_timeout: WAIT, reconnect: WAIT * 3, tick: REPEAT };
    let report = |_: &AntRequest, _: &signer::Event| {};
    let serving = Box::pin(signer::serve(&server.url, team, None, &options, report, pending()));
    let both = async {
        tokio::select! {
            r = serving => panic!("the signer stopped: {r:?}"),
            () = asking => {}
        }
    };
    Box::pin(timeout(WAIT * 4, both)).await.unwrap();
}

/// Decide, as the default human with `js` on `stream`, `verdict` on the next approval request of the signer,
/// which asks about GitHub user `login`.
async fn decide(js: &async_nats::jetstream::Context, stream: &Stream, login: &str, verdict: Verdict) {
    let asked = loop {
        let mut pending = Vec::new();
        for (_, request) in tools::requests(stream).await.unwrap() {
            if tools::decision(stream, &request).await.unwrap().is_none() {
                pending.push(request);
            }
        }
        if let Some(request) = pending.pop() {
            break request;
        }
        tokio::time::sleep(REPEAT).await;
    };
    assert_eq!(asked.ant, UserId::signer());
    assert!(asked.tool.title.contains(&format!("GitHub user {login} ")), "{}", asked.tool.title);
    tools::answer(js, stream, &asked.id.to_string(), verdict, Utc::now()).await.unwrap();
}

/// Whether `answers` tell the device to wait for a human.
fn waited(answers: &[LoginAnswer]) -> bool {
    answers.iter().any(|a| matches!(a, LoginAnswer::Waiting { .. }))
}

fn signed(answers: &[LoginAnswer]) -> bool {
    matches!(answers.last(), Some(LoginAnswer::Signed { .. }))
}

/// The credentials file of `device`, signed `answers`.
fn creds(server: &Server, device: &Device, answers: &[LoginAnswer]) -> PathBuf {
    let Some(LoginAnswer::Signed { jwt }) = answers.last() else { panic!("not signed: {answers:?}") };
    let path = server.dir().join(format!("{}.creds", device.key.public_key()));
    auth::write_creds(&path, jwt, &device.key).unwrap();
    path
}

/// Ask with `ants renew` for the credentials at `path` renewed: the new key, and the JWT or why it was refused.
async fn renewing(server: &Server, path: &Path) -> (KeyPair, Result<String>) {
    let user = KeyPair::new_user();
    let renewal = renew::Renewal { creds: path, user: &user, xkey: None };
    let renewed = renew::request(&server.url, path, renewal, keystore::os(), REPEAT, pending()).await;
    (user, renewed)
}

/// The renewal request of the credentials at `path`, as the signer and `ants enroll renew` hear it.
fn renewal_msg(path: &Path) -> async_nats::Message {
    let (jwt, key) = auth::read_creds(path).unwrap();
    let mut request = RenewRequest::new(&auth::verified(&jwt).unwrap().name, KeyPair::new_user().public_key(), "h");
    request.current = jwt;
    request.signature = auth::sign(&key, &request.signed()).unwrap();
    let payload = hyper::body::Bytes::from(serde_json::to_vec(&request).unwrap());
    let (subject, length) = (request.subject().into(), payload.len());
    async_nats::Message { subject, reply: None, payload, headers: None, status: None, description: None, length }
}

#[tokio::test]
async fn a_github_user_under_the_policy_registers_a_device_and_holds_the_name() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let url = github(&[(1, "octocat", "octo@example.com", true), (1, "octocat", "octo@example.com", true)]).await;
    let config = server.dir().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let client = bootstrap(&server, &team).await;

    let asking = async {
        let reason = refusal(&Device::new("alice").ask(&client, WAIT).await);
        assert!(reason.contains("registration is not enabled"), "{reason}");
        std::fs::write(&config, policy(&url)).unwrap();
        let alice = Device::new("alice");
        let answers = alice.ask(&client, WAIT).await;
        let Some((LoginAnswer::Signed { jwt }, waits)) = answers.split_last() else { panic!("{answers:?}") };
        assert!(!waits.is_empty(), "{answers:?}");
        for wait in waits {
            assert!(
                matches!(wait, LoginAnswer::Verify { user_code, url, .. } if user_code == "CODE-0" && url.ends_with("/login/device")),
                "{wait:?}"
            );
        }
        let finding = Finding::from_jwt(jwt).unwrap();
        assert_eq!((&finding.kind, finding.key.as_str()), (&CredsKind::Human, alice.key.public_key().as_str()));
        assert_eq!(finding.xkey, alice.request.xkey);
        assert!(finding.registered, "marked for the holder's renewal hints");
        let claims = auth::verified(jwt).unwrap();
        assert_eq!(claims.name, "alice");
        let ttl = claims.exp.unwrap() - auth::now().unwrap();
        assert!(ttl > 7 * 86_400 - 60 && ttl <= 7 * 86_400, "{ttl}");
        let record = issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!(record.github, Some(GithubAccount { id: 1, login: "octocat".into() }));
        assert_eq!((record.grant, record.label.as_deref()), (Some(Grant::Org("acme".into())), Some("laptop")));

        let reason = refusal(&Device::new("bob").ask(&client, WAIT).await);
        assert!(reason.contains("octocat is bound to alice"), "{reason}");
        let reason = refusal(&Device::new("human").ask(&client, WAIT).await);
        assert!(reason.contains("default human"), "{reason}");

        let mut forged = Device::new("dave");
        forged.request.label = Some("other".into());
        assert_eq!(forged.ask(&client, REPEAT * 5).await, [], "a request its key did not sign is answered");
    };
    registering(&server, &team, &config, asking).await;
}

#[tokio::test]
async fn outside_the_policy_a_human_decides_and_an_approval_stands_until_the_name_is_revoked() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    const MALLORY: (u64, &str, &str, bool) = (2, "mallory", "m@evil.example", false);
    let url = github(&[MALLORY, MALLORY, (3, "eve", "eve@evil.example", false), MALLORY]).await;
    let config = server.dir().join("config.toml");
    std::fs::write(&config, policy(&url)).unwrap();
    let client = bootstrap(&server, &team).await;
    let (_, js) = server.connect_as(&UserId::human(), Role::Human).await;
    let stream = bus::ensure_stream(&js).await.unwrap();
    let register = async |name: &str| Device::new(name).ask(&client, WAIT).await;

    let asking = async {
        let (answers, ()) = tokio::join!(register("carol"), decide(&js, &stream, "mallory", Verdict::AllowOnce));
        assert!(waited(&answers) && signed(&answers), "{answers:?}");
        let record = issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!(record.github, Some(GithubAccount { id: 2, login: "mallory".into() }));
        assert_eq!((record.name.as_str(), record.grant), ("carol", Some(Grant::Approval("human".into()))));
        let carol = Device::new("carol");
        let answers = carol.ask(&client, WAIT).await;
        assert!(!waited(&answers) && signed(&answers), "the approval stands for another device: {answers:?}");
        let path = creds(&server, &carol, &answers);
        let (key, renewed) = renewing(&server, &path).await;
        assert_eq!(auth::verified(&renewed.unwrap()).unwrap().sub, key.public_key(), "the approval renews");

        let (answers, ()) = tokio::join!(register("dave"), decide(&js, &stream, "eve", Verdict::Reject));
        assert!(waited(&answers), "{answers:?}");
        assert!(refusal(&answers).contains("a human rejected the registration"), "{answers:?}");
        assert!(!issued::issued(&team).unwrap().iter().any(|i| i.name == "dave"), "a rejected human is not signed");

        revoke::revoke(&team, Some("carol"), &[]).unwrap();
        let (answers, ()) = tokio::join!(register("carol"), decide(&js, &stream, "mallory", Verdict::Reject));
        assert!(waited(&answers), "revoking the name withdraws the approval: {answers:?}");
        assert!(refusal(&answers).contains("a human rejected"), "{answers:?}");
    };
    registering(&server, &team, &config, asking).await;
}

#[tokio::test]
async fn the_signer_renews_a_registered_device_while_its_grant_holds_and_revokes_a_member_who_left() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let still = Arc::new(AtomicBool::new(true));
    let url =
        github_checking(&[(1, "octocat", "o@example.com", true), (3, "erin", "e@acme.example", false)], still.clone())
            .await;
    let token = server.dir().join("github-token");
    std::fs::write(&token, "signer").unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let config = server.dir().join("config.toml");
    let table = format!("{}email_domains = [\"acme.example\"]\ntoken_file = \"github-token\"\n", policy(&url));
    std::fs::write(&config, table).unwrap();
    let client = bootstrap(&server, &team).await;

    let asking = async {
        let alice = Device::new("alice");
        let path = creds(&server, &alice, &alice.ask(&client, WAIT).await);
        let approver = Approver::connect(&server.url, &team, None, REPEAT).await.unwrap();
        let err = approver.renewal(&renewal_msg(&path)).unwrap_err().to_string();
        assert!(err.contains("ants signer renews"), "`ants enroll renew` leaves it to the signer: {err}");

        let (key, renewed) = renewing(&server, &path).await;
        let renewed = renewed.unwrap();
        assert_eq!(auth::verified(&renewed).unwrap().sub, key.public_key());
        assert!(Finding::from_jwt(&renewed).unwrap().registered, "still marked");
        let record = issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!(record.renewed_from, Some(alice.key.public_key()));
        assert_eq!((record.github.map(|g| g.id), record.grant), (Some(1), Some(Grant::Org("acme".into()))));

        still.store(false, Ordering::SeqCst);
        let path = server.dir().join("alice-renewed.creds");
        auth::write_creds(&path, &renewed, &key).unwrap();
        let err = format!("{:#}", renewing(&server, &path).await.1.unwrap_err());
        assert!(err.contains("octocat is no member") && err.contains("every key of alice is revoked"), "{err}");
        let revoked = revoke::revoked(&team).unwrap();
        for key in [alice.key.public_key(), key.public_key()] {
            assert!(revoked.iter().any(|r| r.key == key), "{key} not revoked: {revoked:?}");
        }

        let erin = Device::new("erin");
        let path = creds(&server, &erin, &erin.ask(&client, WAIT).await);
        let err = format!("{:#}", renewing(&server, &path).await.1.unwrap_err());
        assert!(err.contains("email address in acme.example") && err.contains("`ants login`"), "{err}");
        assert!(!revoke::revoked(&team).unwrap().iter().any(|r| r.key == erin.key.public_key()));
        let offboarded = offboarded(&team);
        assert_eq!(offboarded.len(), 1, "{offboarded:?}");
        assert_eq!((offboarded[0].name.as_str(), offboarded[0].by), ("alice", Offboarder::Renewal));
    };
    registering(&server, &team, &config, asking).await;
}

#[tokio::test]
async fn a_web_device_registers_with_web_credentials_and_keeps_them_through_renewal() {
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let still = Arc::new(AtomicBool::new(true));
    let url = github_checking(&[(1, "octocat", "o@example.com", true); 3], still).await;
    let token = server.dir().join("github-token");
    std::fs::write(&token, "signer").unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let config = server.dir().join("config.toml");
    std::fs::write(&config, format!("{}web_ttl = \"2d\"\ntoken_file = \"github-token\"\n", policy(&url))).unwrap();
    let client = bootstrap(&server, &team).await;
    const WEB_TTL: u64 = 2 * 86_400;
    let lifetime = |jwt: &str| auth::verified(jwt).unwrap().exp.unwrap() - auth::now().unwrap();
    let web_rights = ants_proto::Permissions::for_web(&p("alice"));
    let rights = |jwt: &str| {
        let nats = auth::verified_body(jwt).unwrap().1["nats"].take();
        let list = |allow: &serde_json::Value| serde_json::from_value::<Vec<String>>(allow.clone()).unwrap();
        ants_proto::Permissions { publish: list(&nats["pub"]["allow"]), subscribe: list(&nats["sub"]["allow"]) }
    };

    let asking = async {
        let mut binding = Device::web("alice");
        binding.request.xkey = Some(XKey::new().public_key().parse::<DeviceKey>().unwrap());
        binding.sign();
        let reason = refusal(&binding.ask(&client, WAIT).await);
        assert!(reason.contains("a web device binds no encryption key"), "{reason}");
        assert!(issued::issued(&team).unwrap().iter().all(|i| i.name != "alice"), "nothing signed");

        let alice = Device::web("alice");
        let answers = alice.ask(&client, WAIT).await;
        let path = creds(&server, &alice, &answers);
        let (jwt, _) = auth::read_creds(&path).unwrap();
        let finding = Finding::from_jwt(&jwt).unwrap();
        assert_eq!((&finding.kind, finding.web, finding.admin), (&CredsKind::Human, true, false));
        assert_eq!((finding.xkey.as_ref(), finding.registered), (None, true));
        assert_eq!(rights(&jwt), web_rights);
        let ttl = lifetime(&jwt);
        assert!(ttl > WEB_TTL - 60 && ttl <= WEB_TTL, "the web lifetime, not the 7 d of other devices: {ttl}");
        let record = issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!((record.web, record.admin, record.label.as_deref()), (true, false, Some("browser")));
        assert_eq!(record.grant, Some(Grant::Org("acme".into())));

        let (key, renewed) = renewing(&server, &path).await;
        let renewed = renewed.unwrap();
        let again = Finding::from_jwt(&renewed).unwrap();
        assert_eq!((again.key.as_str(), again.web, again.registered), (key.public_key().as_str(), true, true));
        assert!(finding.same_credentials(&again) && again.xkey.is_none() && !again.admin);
        assert_eq!(rights(&renewed), web_rights);
        let ttl = lifetime(&renewed);
        assert!(ttl > WEB_TTL - 60 && ttl <= WEB_TTL, "{ttl}");
        let record = issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!((record.web, record.renewed_from), (true, Some(alice.key.public_key())));
        let devices = issued::devices(&team, "alice").unwrap();
        assert_eq!(devices.iter().map(|d| (d.web, d.key.clone())).collect::<Vec<_>>(), [(true, key.public_key())]);

        let laptop = Device::new("alice");
        let answers = laptop.ask(&client, WAIT).await;
        let Some(LoginAnswer::Signed { jwt }) = answers.last() else { panic!("{answers:?}") };
        let full = Finding::from_jwt(jwt).unwrap();
        assert!(!full.web && full.xkey.is_some(), "another device of the human is no web device");
        let ttl = lifetime(jwt);
        assert!(ttl > 7 * 86_400 - 60, "{ttl}");
    };
    registering(&server, &team, &config, asking).await;
}

/// Publish `request` on `transport`, again each [`REPEAT`] without an answer, until `answer` makes one of a message on
/// `replies` that settles it (`settled`): every answer made, in order.
async fn asked<T>(
    transport: &ants_test_support::NatsTransport,
    request: &ants_client::transport::Message,
    replies: String,
    answer: impl Fn(&ants_client::transport::Message) -> Option<T>,
    settled: impl Fn(&T) -> bool,
) -> Vec<T> {
    use ants_client::transport::Transport as _;
    let mut replies = transport.subscribe(replies).await.unwrap();
    transport.publish(request.clone()).await.unwrap();
    let mut answers = Vec::new();
    loop {
        match replies.next(Some(REPEAT)).await {
            Ok(message) => {
                let Some(answered) = answer(&message) else { continue };
                let done = settled(&answered);
                answers.push(answered);
                if done {
                    return answers;
                }
            }
            Err(ants_client::transport::TransportError::TimedOut) => transport.publish(request.clone()).await.unwrap(),
            Err(e) => panic!("{e}"),
        }
    }
}

#[tokio::test]
async fn a_page_registers_and_renews_its_device_with_the_requests_of_ants_client() {
    use ants_client::device::registration::{Registration, Step};
    use ants_client::device::renewal::Renewal;
    use ants_client::device::{Bootstrap, Error as DeviceError};
    use ants_client::transport::{Connect, Transport as _};
    use ants_test_support::NatsTransport;

    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let url = github_checking(&[(1, "octocat", "o@example.com", true); 3], Arc::new(AtomicBool::new(true))).await;
    let token = server.dir().join("github-token");
    std::fs::write(&token, "signer").unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let config = server.dir().join("config.toml");
    std::fs::write(&config, format!("{}token_file = \"github-token\"\n", policy(&url))).unwrap();
    let creds = server.dir().join("bootstrap.creds");
    auth::issue_bootstrap(&team, Duration::ZERO, &creds).unwrap();
    let bootstrap = Bootstrap::read(&std::fs::read_to_string(&creds).unwrap()).unwrap();
    assert_eq!(bootstrap.account(), auth::account_key(&team).unwrap());
    let carrier = NatsTransport::new(&server.url, Some(&creds));
    carrier.connect(Connect { name: "ants-web".into(), inbox_prefix: "_INBOX".into() }).await.unwrap();
    let alice = p("alice");

    let asking = async {
        let register = async |key: &KeyPair, github: &str| {
            let label = Some("browser".to_owned());
            let mut registration =
                Registration::new(alice.clone(), key.public_key(), label, &bootstrap, github).unwrap();
            registration.sign(BASE64URL_NOPAD.encode(&key.sign(&registration.signed()).unwrap()));
            let settled =
                |step: &Result<Step, DeviceError>| !matches!(step, Ok(Step::Verify { .. } | Step::Waiting { .. }));
            asked(&carrier, &registration.request(), registration.replies(), |m| registration.answer(m), settled).await
        };

        // A page trusting github.com shows no page of another GitHub.
        let phished = register(&KeyPair::new_user(), "https://github.com").await;
        let trusted = "https://github.com/login/device".to_owned();
        assert_eq!(phished, [Err(DeviceError::NotShown { trusted })]);

        // Trusting the team's, it shows that one's page and code, then takes the JWT of its key.
        let key = KeyPair::new_user();
        let mut steps = register(&key, "https://GitHub.example/").await;
        let Some(Ok(Step::Signed { jwt, credentials })) = steps.pop() else { panic!("not signed: {steps:?}") };
        let page = "https://github.example/login/device".to_owned();
        assert!(
            matches!(&steps[..], [Ok(Step::Verify { page: shown, user_code, .. })] if *shown == page && user_code.starts_with("CODE-")),
            "{steps:?}"
        );
        assert_eq!((&credentials.human, credentials.public_key.as_str()), (&alice, key.public_key().as_str()));
        assert_eq!((credentials.account.as_str(), &credentials.domain), (bootstrap.account(), &None));
        let device = credentials.device.expect("a device");
        assert_eq!(credentials.revoke_command(), format!("ants auth revoke --key {}", key.public_key()));
        let record = crate::credentials::issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!((record.web, record.device, record.label.as_deref()), (true, Some(device), Some("browser")));

        // Connected as the device, it asks for its credentials renewed for a new key, signed by the current one.
        let path = server.dir().join("alice-web.creds");
        auth::write_creds(&path, &jwt, &key).unwrap();
        let page = NatsTransport::new(&server.url, Some(&path));
        page.connect(Connect { name: "ants-web".into(), inbox_prefix: ants_proto::inbox_prefix(&alice) })
            .await
            .unwrap();
        let next = KeyPair::new_user();
        let mut renewal = Renewal::new(&credentials, &jwt, next.public_key(), "a browser".into());
        renewal.sign(BASE64URL_NOPAD.encode(&key.sign(&renewal.signed()).unwrap()));
        let mut renewed = asked(&page, &renewal.request(), renewal.replies(), |m| renewal.answer(m), |_| true).await;
        let renewed = renewed.pop().unwrap().unwrap();
        let again = renewed.credentials;
        assert_eq!(
            (&again.human, again.public_key.as_str(), again.device),
            (&alice, next.public_key().as_str(), Some(device))
        );
        assert!(again.expires_at >= credentials.expires_at);
        let record = crate::credentials::issued::issued(&team).unwrap().pop().unwrap();
        assert_eq!((record.web, record.renewed_from), (true, Some(key.public_key())));
        assert!(page.server_errors().is_empty() && carrier.server_errors().is_empty(), "nothing refused");
    };
    registering(&server, &team, &config, asking).await;
}

/// The lines of [`OFFBOARDED`] in `team`.
fn offboarded(team: &Path) -> Vec<Offboarded> {
    let text = std::fs::read_to_string(team.join(OFFBOARDED)).unwrap_or_default();
    text.lines().map(|line| serde_json::from_str(line).unwrap()).collect()
}

#[tokio::test]
async fn the_sweep_revokes_a_member_who_left_only_when_github_definitely_says_so() {
    const USERS: Users = &[(1, "octocat", "o@example.com", true), (2, "bobby", "b@example.com", true)];
    let Some(server) = Server::start() else { return };
    let team = server.dir().join("auth");
    let flag = || Arc::new(AtomicBool::new(true));
    let (member, visible, answering) = (flag(), flag(), flag());
    let signing_in = signing_in(USERS);
    let url = fake(Arc::new({
        let (member, visible, answering) = (member.clone(), visible.clone(), answering.clone());
        move |what: &str, auth: Option<&str>, body: &str| {
            if auth != Some("Bearer signer") {
                return signing_in(what, auth, body);
            }
            let on = |flag: &AtomicBool| flag.load(Ordering::SeqCst);
            let not_found = (404, r#"{"message":"Not Found"}"#.to_owned());
            match what {
                "GET /orgs/acme" if on(&visible) => (200, r#"{"login":"acme"}"#.to_owned()),
                "GET /user/1" => (200, r#"{"id":1,"login":"octocat"}"#.to_owned()),
                "GET /user/2" => (200, r#"{"id":2,"login":"bobby"}"#.to_owned()),
                "GET /orgs/acme/members/octocat" if on(&member) => (204, String::new()),
                "GET /orgs/acme/members/bobby" if on(&answering) => (204, String::new()),
                "GET /orgs/acme/members/bobby" => (502, "Bad Gateway".to_owned()),
                _ => not_found,
            }
        }
    }))
    .await;
    let token = server.dir().join("github-token");
    std::fs::write(&token, "signer").unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&token, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let config = server.dir().join("config.toml");
    std::fs::write(&config, format!("{}token_file = \"github-token\"\nsweep = \"1s\"\n", policy(&url))).unwrap();
    let client = bootstrap(&server, &team).await;
    let revoked = |device: &Device| revoke::revoked(&team).unwrap().iter().any(|r| r.key == device.key.public_key());

    let asking = async {
        let (alice, bob) = (Device::new("alice"), Device::new("bob"));
        assert!(signed(&alice.ask(&client, WAIT).await) && signed(&bob.ask(&client, WAIT).await));
        tokio::time::sleep(SWEEP_PACE * 3).await;
        assert!(!revoked(&alice) && !revoked(&bob), "members are kept");

        // GitHub hides the organization from the signer's token: a missing membership tells nothing.
        visible.store(false, Ordering::SeqCst);
        member.store(false, Ordering::SeqCst);
        tokio::time::sleep(SWEEP_PACE * 3).await;
        assert!(!revoked(&alice) && !revoked(&bob), "a sweep the token cannot see the policy for revokes nobody");

        // GitHub fails for bobby: kept, while octocat, definitely gone, is revoked.
        visible.store(true, Ordering::SeqCst);
        answering.store(false, Ordering::SeqCst);
        timeout(WAIT, async {
            while !revoked(&alice) {
                tokio::time::sleep(REPEAT).await;
            }
        })
        .await
        .expect("the sweep revokes alice");
        tokio::time::sleep(SWEEP_PACE * 3).await;
        assert!(!revoked(&bob), "a member GitHub could not check is kept");
        let offboarded = offboarded(&team);
        let [line] = offboarded.as_slice() else { panic!("{offboarded:?}") };
        assert_eq!((line.name.as_str(), line.by, line.github.id), ("alice", Offboarder::Sweep, 1));
        assert_eq!(line.keys, [alice.key.public_key()]);
        assert!(line.reason.contains("octocat is no member"), "{}", line.reason);
    };
    registering(&server, &team, &config, asking).await;
}

#[test]
fn the_sweep_is_off_without_a_token_or_an_interval() {
    let dir = ants_test_support::scratch_dir("registrar-sweep");
    let config = dir.join("config.toml");
    let registrar = |config| Registrar::new("nats://127.0.0.1:1", &dir, config, None, WAIT, print).start_sweep();
    for (table, why) in [
        (String::new(), "no [registration] table"),
        (policy("http://127.0.0.1:1"), "no GitHub token"),
        (format!("{}token_store = \"t\"\nsweep = \"0\"\n", policy("http://127.0.0.1:1")), "sweep is 0"),
        (
            "[registration]\nclient_id = \"x\"\nemail_domains = [\"acme.example\"]\n".to_owned(),
            "no organization or team",
        ),
    ] {
        std::fs::write(&config, &table).unwrap();
        let Err(err) = registrar(&config) else { panic!("a sweep runs for {table}") };
        assert!(err.contains(why), "{err}");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
