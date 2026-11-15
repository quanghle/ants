//! Self-service registration of humans' devices, answered by `ants signer`: a machine running `ants login` asks with
//! the team's public bootstrap credentials ([`LoginRequest`] on [`subject::login`]), the signer runs a GitHub device
//! flow for it ([`Github`]), tells the machine where to sign in, reads who signed in, and signs a human's credentials
//! for the request's key when the GitHub user falls under the signer's `[registration]` policy and may hold the name
//! asked for ([`binding`]).
//!
//! Requests are core NATS messages and stored nowhere: the machine repeats its request every
//! [`ants_proto::LOGIN_REPEAT`] until an answer settles it, and the signer answers each repeat of a key with what it
//! has for it, sealed to the request's one-off X25519 key ([`LoginReply`]). One device flow runs per key, and at most
//! [`MAX_REGISTRATIONS`] at once. The user's GitHub token stays in the task that reads the user with it.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use ants_daemon::config::registration::{GithubAccount, GithubUser, Grant, signer_registration};
use ants_proto::{JsDomain, LoginAnswer, LoginReply, LoginRequest, MAX_REGISTRATIONS, Permissions, Role};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use data_encoding::BASE64URL_NOPAD;
use nkeys::XKey;
use tokio::sync::mpsc;

use crate::auth;
use crate::binding::{self, Binder};
use crate::github::Github;
use crate::revoke::{self, Kind};
use crate::signer::AbortOnDrop;

/// What a registration's task found out.
enum Progress {
    /// The device flow began: sign in at `url` with `user_code` until `expires_at`.
    Verify { url: String, user_code: String, expires_at: DateTime<Utc> },
    /// Who signed in, or why no one did.
    SignedIn(Result<GithubUser>),
}

/// What the task of the registration of a key found out, for [`Registrar::progressed`].
pub struct Update(String, Progress);

/// One registration, by the key of the device asking.
struct Flow {
    /// The latest repeat of the request, whose reply key the answers are sealed to.
    request: LoginRequest,
    answer: Option<LoginAnswer>,
    /// The task running the device flow, until it reports who signed in.
    task: Option<AbortOnDrop>,
}

/// What the registrar did with a request.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Began a GitHub sign-in.
    Started,
    /// Signed for GitHub user `login`, under `grant`.
    Signed { login: String, grant: Grant },
    /// Refused, and why.
    Refused(String),
}

/// Print what the registrar did with `request`.
pub fn print(request: &LoginRequest, event: &Event) {
    let asked = format!("registration of {} (key {})", request.name, request.public_key);
    match event {
        Event::Started => println!("{asked}: waiting for a GitHub sign-in"),
        Event::Signed { login, grant } => println!("signed {asked}: GitHub user {login}, {grant}"),
        Event::Refused(reason) => println!("refused {asked}: {reason}"),
    }
}

/// The registrations `ants signer` runs, kept across its reconnections.
pub struct Registrar<'a> {
    dir: &'a Path,
    /// The config file whose `[registration]` table is the policy, read for each request.
    config: &'a Path,
    js_domain: Option<&'a JsDomain>,
    /// The signer's key sealing the answers.
    xkey: XKey,
    flows: HashMap<String, Flow>,
    progress: (mpsc::UnboundedSender<Update>, mpsc::UnboundedReceiver<Update>),
    report: fn(&LoginRequest, &Event),
}

impl<'a> Registrar<'a> {
    pub fn new(
        dir: &'a Path,
        config: &'a Path,
        js_domain: Option<&'a JsDomain>,
        report: fn(&LoginRequest, &Event),
    ) -> Self {
        let progress = mpsc::unbounded_channel();
        Self { dir, config, js_domain, xkey: XKey::new(), flows: HashMap::new(), progress, report }
    }

    /// The next progress of a registration's task, for [`Registrar::progressed`].
    pub async fn next(&mut self) -> Update {
        self.progress.1.recv().await.expect("the registrar holds a sender")
    }

    /// Answer the registration request `msg`, if it is one: a repeat with what its key has, a new one by beginning
    /// a GitHub sign-in or refusing.
    pub async fn heard(&mut self, client: &async_nats::Client, msg: &async_nats::Message) -> Result<()> {
        let request = match serde_json::from_slice::<LoginRequest>(&msg.payload) {
            Ok(request) => request,
            Err(e) => {
                tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed registration request");
                return Ok(());
            }
        };
        if let Err(e) = request.check_subject(&msg.subject).map_err(anyhow::Error::from).and_then(|()| check(&request))
        {
            tracing::warn!(name = %request.name, key = %request.public_key, "ignoring a registration request: {e:#}");
            return Ok(());
        }
        let key = request.public_key.clone();
        if let Some(flow) = self.flows.get_mut(&key) {
            let first = &flow.request;
            if (&first.name, &first.label, &first.xkey) != (&request.name, &request.label, &request.xkey) {
                tracing::warn!(key = %key, "ignoring a registration request unlike the earlier one of its key");
                return Ok(());
            }
            flow.request = request;
            return self.answer(client, &key).await;
        }
        let answer = match self.start(&request) {
            Ok(task) => {
                (self.report)(&request, &Event::Started);
                self.flows.insert(key, Flow { request, answer: None, task: Some(task) });
                return Ok(());
            }
            Err(e) => LoginAnswer::Refused { reason: format!("{e:#}") },
        };
        self.settle(client, request, answer).await
    }

    /// Begin a GitHub sign-in for `request`, if the policy allows one: a task reporting its [`Progress`].
    fn start(&self, request: &LoginRequest) -> Result<AbortOnDrop> {
        let registration = signer_registration(self.config)
            .context("the signer's registration policy does not load")?
            .context("registration is not enabled on this team")?;
        let running = self.flows.values().filter(|f| f.task.is_some()).count();
        if running >= MAX_REGISTRATIONS {
            bail!("the signer is busy with {running} registrations: try again in a few minutes");
        }
        let issued = revoke::issued(self.dir)?;
        if issued.iter().any(|i| i.key == request.public_key)
            || revoke::revoked(self.dir)?.iter().any(|r| r.key == request.public_key)
        {
            bail!("the key {} was issued already: generate a new one", request.public_key);
        }
        binding::check_name(&issued, &request.name)?;
        let github = Github::new(Some(&registration))?;
        let (key, tx) = (request.public_key.clone(), self.progress.0.clone());
        let left = (request.expires_at - Utc::now()).to_std().unwrap_or_default();
        let (until, expires_at) = (tokio::time::Instant::now() + left, request.expires_at);
        Ok(AbortOnDrop(tokio::spawn(async move {
            let signed_in = async {
                let code = github.device_code(&registration.client_id).await?;
                let code_expires = Utc::now() + Duration::from_secs(code.expires_in);
                let verify = Progress::Verify {
                    url: code.verification_uri.clone(),
                    user_code: code.user_code.clone(),
                    expires_at: code_expires.min(expires_at),
                };
                let _ = tx.send(Update(key.clone(), verify));
                let token = tokio::time::timeout_at(until, github.signed_in(&registration.client_id, &code))
                    .await
                    .map_err(|_| anyhow!("no one signed in to GitHub in time"))??;
                github.user(&token, &registration).await
            };
            let signed_in = signed_in.await;
            let _ = tx.send(Update(key, Progress::SignedIn(signed_in)));
        })))
    }

    /// Act on what the task of the registration of `key` found out, and answer.
    pub async fn progressed(&mut self, client: &async_nats::Client, Update(key, progress): Update) -> Result<()> {
        let Some(flow) = self.flows.get_mut(&key) else { return Ok(()) };
        let user = match progress {
            Progress::Verify { url, user_code, expires_at } => {
                flow.answer = Some(LoginAnswer::Verify { url, user_code, expires_at });
                return self.answer(client, &key).await;
            }
            Progress::SignedIn(user) => user,
        };
        let request = flow.request.clone();
        let answer = match user.and_then(|user| self.sign(&request, &user)) {
            Ok((jwt, login, grant)) => {
                (self.report)(&request, &Event::Signed { login, grant });
                LoginAnswer::Signed { jwt }
            }
            Err(e) => LoginAnswer::Refused { reason: format!("{e:#}") },
        };
        self.settle(client, request, answer).await
    }

    /// Sign a human's credentials for `request`, of the GitHub user `user`, if the policy grants them and the name is
    /// the user's to hold: the JWT, the user's login and the grant.
    fn sign(&self, request: &LoginRequest, user: &GithubUser) -> Result<(String, String, Grant)> {
        let registration =
            signer_registration(self.config)?.context("registration is no longer enabled on this team")?;
        let Some(grant) = registration.grant(user) else {
            bail!("GitHub user {} is outside the team's registration policy", user.login);
        };
        let name = &request.name;
        let account = GithubAccount { id: user.id, login: user.login.clone() };
        binding::check_in(self.dir, name, &account, Binder::Signer)?;
        let expires_at = auth::now()? + registration.ttl.as_secs();
        let new = auth::NewDevice {
            label: request.label.as_deref(),
            github: Some(account),
            grant: Some(grant.clone()),
            expires_at: Some(expires_at),
        };
        let permissions = Permissions::for_user(name, Role::Human).with_js_domain(self.js_domain);
        let (jwt, _) = auth::sign_device_with(
            self.dir,
            name,
            Kind::Human,
            permissions,
            &new,
            &request.public_key,
            request.xkey.as_ref(),
        )
        .context("signing failed")?;
        Ok((jwt, user.login.clone(), grant))
    }

    /// Record `answer`, which settles `request`, and send it.
    async fn settle(&mut self, client: &async_nats::Client, request: LoginRequest, answer: LoginAnswer) -> Result<()> {
        if let LoginAnswer::Refused { reason } = &answer {
            (self.report)(&request, &Event::Refused(reason.clone()));
        }
        let key = request.public_key.clone();
        self.flows.insert(key.clone(), Flow { request, answer: Some(answer), task: None });
        self.answer(client, &key).await
    }

    /// Send what the registration of `key` has to say, if anything, sealed to its latest request's reply key.
    async fn answer(&self, client: &async_nats::Client, key: &str) -> Result<()> {
        let Some(Flow { request, answer: Some(answer), .. }) = self.flows.get(key) else { return Ok(()) };
        let recipient = XKey::from_public_key(&request.reply_xkey).map_err(|e| anyhow!("the reply key: {e}"))?;
        let sealed = self.xkey.seal(&serde_json::to_vec(answer)?, &recipient).map_err(|e| anyhow!("sealing: {e}"))?;
        let reply = LoginReply {
            name: request.name.clone(),
            public_key: request.public_key.clone(),
            sender: self.xkey.public_key(),
            sealed: BASE64URL_NOPAD.encode(&sealed),
        };
        client.publish(reply.subject(), serde_json::to_vec(&reply)?.into()).await?;
        client.flush().await?;
        Ok(())
    }

    /// Drop the registrations whose requests expired, stopping their device flows.
    pub fn expire(&mut self) {
        let now = Utc::now();
        self.flows.retain(|_, flow| flow.request.expires_at > now);
    }
}

/// Check that `request` is the device's: for a well-formed key and reply key, signed by the key, and still good.
fn check(request: &LoginRequest) -> Result<()> {
    auth::check_user_key(&request.public_key)?;
    XKey::from_public_key(&request.reply_xkey).map_err(|e| anyhow!("the reply key {}: {e}", request.reply_xkey))?;
    if !request.is_live(Utc::now()) {
        bail!("the request expired at {} or was made for too long", request.expires_at);
    }
    auth::check_signature(&request.public_key, &request.signed(), &request.signature)
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::{Arc, Mutex};

    use ants_daemon::creds::{Finding, Kind as CredsKind};
    use ants_proto::{AntRequest, DeviceKey, UserId, subject};
    use futures::StreamExt as _;
    use nkeys::KeyPair;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;
    use crate::github::tests::fake;
    use crate::signer;

    const WAIT: Duration = Duration::from_secs(20);
    const REPEAT: Duration = Duration::from_millis(200);

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// A fake GitHub where the nth sign-in is of the nth of `users` (id, login, verified email, member of acme):
    /// each device flow is pending at its first poll.
    async fn github(users: &'static [(u64, &'static str, &'static str, bool)]) -> String {
        let flows = Arc::new(Mutex::new((0_usize, Vec::<String>::new())));
        fake(Arc::new(move |what, auth, body| {
            let token = auth.and_then(|a| a.strip_prefix("Bearer tok-")).and_then(|n| n.parse::<usize>().ok());
            let user = token.and_then(|n| users.get(n));
            match (what, user) {
                ("POST /login/device/code", _) if body.contains("client_id=Iv1.test") => {
                    let mut flows = flows.lock().unwrap();
                    flows.0 += 1;
                    let n = flows.0 - 1;
                    let code = format!(r#"{{"device_code":"dc-{n}","user_code":"CODE-{n}","verification_uri":"https://github.example/login/device","expires_in":900,"interval":0}}"#);
                    (200, code)
                }
                ("POST /login/oauth/access_token", _) => {
                    let code = body.split('&').find_map(|kv| kv.strip_prefix("device_code=")).unwrap().to_owned();
                    let mut flows = flows.lock().unwrap();
                    if flows.1.contains(&code) {
                        let n = code.trim_start_matches("dc-");
                        (200, format!(r#"{{"access_token":"tok-{n}","token_type":"bearer","scope":"read:org,user:email"}}"#))
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

    #[tokio::test]
    async fn a_github_user_under_the_policy_registers_a_device_and_holds_the_name() {
        let Some(server) = Server::start() else { return };
        let team = server.dir().join("auth");
        let url = github(&[
            (1, "octocat", "octo@example.com", true),
            (2, "mallory", "m@evil.example", false),
            (1, "octocat", "octo@example.com", true),
        ])
        .await;
        let config = server.dir().join("config.toml");
        let table = format!(
            "[registration]\nclient_id = \"Iv1.test\"\norgs = [\"acme\"]\nttl = \"7d\"\ngithub_url = \"{url}\"\napi_url = \"{url}\"\n"
        );
        std::fs::write(&config, "").unwrap();
        let bootstrap = server.dir().join("bootstrap.creds");
        auth::issue_bootstrap(&team, Duration::ZERO, &bootstrap).unwrap();
        let client = async_nats::ConnectOptions::with_credentials_file(&bootstrap)
            .await
            .unwrap()
            .connect(&server.url)
            .await
            .unwrap();

        let options = signer::Options { config: &config, approval_timeout: WAIT, reconnect: WAIT * 3, tick: REPEAT };
        let serving = Box::pin(signer::serve(
            &server.url,
            &team,
            None,
            &options,
            |_: &AntRequest, _: &signer::Event| {},
            pending(),
        ));
        let asking = async {
            let reason = refusal(&Device::new("alice").ask(&client, WAIT).await);
            assert!(reason.contains("registration is not enabled"), "{reason}");
            std::fs::write(&config, table).unwrap();
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
            let claims = auth::verified(jwt).unwrap();
            assert_eq!(claims.name, "alice");
            let ttl = claims.exp.unwrap() - auth::now().unwrap();
            assert!(ttl > 7 * 86_400 - 60 && ttl <= 7 * 86_400, "{ttl}");
            let record = revoke::issued(&team).unwrap().pop().unwrap();
            assert_eq!(record.github, Some(GithubAccount { id: 1, login: "octocat".into() }));
            assert_eq!((record.grant, record.label.as_deref()), (Some(Grant::Org("acme".into())), Some("laptop")));

            let reason = refusal(&Device::new("carol").ask(&client, WAIT).await);
            assert!(reason.contains("mallory is outside the team's registration policy"), "{reason}");
            let reason = refusal(&Device::new("bob").ask(&client, WAIT).await);
            assert!(reason.contains("octocat is bound to alice"), "{reason}");
            let reason = refusal(&Device::new("human").ask(&client, WAIT).await);
            assert!(reason.contains("default human"), "{reason}");

            let mut forged = Device::new("dave");
            forged.request.label = Some("other".into());
            assert_eq!(forged.ask(&client, REPEAT * 5).await, [], "a request its key did not sign is answered");
        };
        timeout(WAIT * 4, async {
            tokio::select! {
                r = serving => panic!("the signer stopped: {r:?}"),
                () = asking => {}
            }
        })
        .await
        .unwrap();
    }
}
