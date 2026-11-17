use std::future::pending;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ants_daemon::bus;
use ants_daemon::creds::{Finding, Kind as CredsKind};
use ants_daemon::keystore;
use ants_proto::{AntRequest, DeviceKey, LoginReply, Role, UserId, Verdict, subject};
use async_nats::jetstream::stream::Stream;
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;
use nkeys::KeyPair;
use tokio::time::timeout;

use super::offboard::{Offboarded, Offboarder};
use super::*;
use crate::test_support::Server;
use crate::test_support::github::fake;
use crate::{auth, renew, revoke, signer, tools};

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
        let record = revoke::issued(&team).unwrap().pop().unwrap();
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
        let record = revoke::issued(&team).unwrap().pop().unwrap();
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
        assert!(!revoke::issued(&team).unwrap().iter().any(|i| i.name == "dave"), "a rejected human is not signed");

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
        let record = revoke::issued(&team).unwrap().pop().unwrap();
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
