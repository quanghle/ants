//! Self-service registration of humans' devices, answered by `ants signer`: a machine running `ants login` asks with
//! the team's public bootstrap credentials ([`LoginRequest`] on [`subject::login`]), the signer runs a GitHub device
//! flow for it ([`Github`]), tells the machine where to sign in, reads who signed in, and signs a human's credentials
//! for the request's key when the GitHub user falls under the signer's `[registration]` policy and may hold the name
//! asked for ([`binding`]). A user outside the policy waits for the humans: the signer asks them with an approval
//! request of the reserved user [`UserId::signer`], and signs once one allows it. The approval stands for that user
//! and name until every key of the name is revoked ([`revoke::standing_approval`]), so the user's other devices sign
//! at once.
//!
//! Requests are core NATS messages and stored nowhere: the machine repeats its request every
//! [`ants_proto::LOGIN_REPEAT`] until an answer settles it, and the signer answers each repeat of a key with what it
//! has for it, sealed to the request's one-off X25519 key ([`LoginReply`]). One device flow runs per key, and at most
//! [`MAX_REGISTRATIONS`] at once. The user's GitHub token stays in the task that reads the user with it.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use ants_daemon::config::registration::{GithubAccount, GithubUser, Grant, signer_registration};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, JsDomain, LOGIN_REPEAT, LoginAnswer, LoginReply, LoginRequest,
    MAX_REGISTRATIONS, Permissions, Role, STREAM, ThreadId, ToolCall, UserId, Verdict,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use chrono::{DateTime, Utc};
use data_encoding::BASE64URL_NOPAD;
use nkeys::XKey;
use tokio::sync::mpsc;

use crate::auth;
use crate::binding::{self, Binder};
use crate::enroll::Approver;
use crate::github::Github;
use crate::revoke::{self, Kind};
use crate::signer::AbortOnDrop;

/// Most registrations waiting for a human at once; more GitHub users outside the policy are refused.
pub const MAX_WAITING_REGISTRATIONS: usize = 3;
/// Least time a request must have left for a human to be asked about it.
const LEAST_ASKING: Duration = Duration::from_secs(10);

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
    /// The approval request asking the humans about the GitHub user who signed in, outside the policy, until a human
    /// decides or it expires.
    asking: Option<Asking>,
}

/// A GitHub user outside the registration policy, and the approval request asking the humans about it.
struct Asking {
    account: GithubAccount,
    approval: ApprovalRequest,
}

/// What the registrar did with a request.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Began a GitHub sign-in.
    Started,
    /// Asked the humans with approval request `id` about GitHub user `login`, outside the policy.
    Asked { login: String, id: ants_proto::Uuid },
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
        Event::Asked { login, id } => println!(
            "asked the humans about {asked}, as GitHub user {login} is outside the registration policy: \
             `ants tools approve|reject {}/{id}`",
            UserId::signer()
        ),
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
    /// How long a human has to decide on a registration outside the policy, at most until its request expires.
    approval_timeout: Duration,
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
        approval_timeout: Duration,
        report: fn(&LoginRequest, &Event),
    ) -> Self {
        let progress = mpsc::unbounded_channel();
        let flows = HashMap::new();
        Self { dir, config, js_domain, approval_timeout, xkey: XKey::new(), flows, progress, report }
    }

    /// The next progress of a registration's task, for [`Registrar::progressed`].
    pub async fn next(&mut self) -> Update {
        self.progress.1.recv().await.expect("the registrar holds a sender")
    }

    /// Answer the registration request `msg`, if it is one: a repeat with what its key has, a new one by beginning
    /// a GitHub sign-in or refusing.
    pub async fn heard(&mut self, approver: &Approver, msg: &async_nats::Message) -> Result<()> {
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
            return self.answer(approver, &key).await;
        }
        let answer = match self.start(&request) {
            Ok(task) => {
                (self.report)(&request, &Event::Started);
                self.flows.insert(key, Flow { request, answer: None, task: Some(task), asking: None });
                return Ok(());
            }
            Err(e) => LoginAnswer::Refused { reason: format!("{e:#}") },
        };
        self.settle(approver, request, answer).await
    }

    /// Begin a GitHub sign-in for `request`, if the policy allows one: a task reporting its [`Progress`].
    fn start(&self, request: &LoginRequest) -> Result<AbortOnDrop> {
        let registration = signer_registration(self.config)
            .context("the signer's registration policy does not load")?
            .context("registration is not enabled on this team")?;
        let running = self.flows.values().filter(|f| f.task.is_some() || f.asking.is_some()).count();
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
    pub async fn progressed(&mut self, approver: &Approver, Update(key, progress): Update) -> Result<()> {
        let Some(flow) = self.flows.get_mut(&key) else { return Ok(()) };
        let user = match progress {
            Progress::Verify { url, user_code, expires_at } => {
                flow.answer = Some(LoginAnswer::Verify { url, user_code, expires_at });
                return self.answer(approver, &key).await;
            }
            Progress::SignedIn(user) => user,
        };
        flow.task = None;
        let request = flow.request.clone();
        let granted = user.and_then(|user| Ok((self.grant(&request, &user)?, user)));
        let answer = match granted {
            Ok((Some(grant), user)) => self.signed(&request, &user.account(), grant),
            Ok((None, user)) => match self.ask_humans(approver, &request, user.account()).await {
                Ok(()) => return self.answer(approver, &key).await,
                Err(e) => LoginAnswer::Refused { reason: format!("{e:#}") },
            },
            Err(e) => LoginAnswer::Refused { reason: format!("{e:#}") },
        };
        self.settle(approver, request, answer).await
    }

    /// What grants the GitHub user `user` the credentials `request` asks for: the policy, reread, or an approval
    /// that stands for the user and name; none when a human must decide. Fails when the name is not the user's to
    /// hold.
    fn grant(&self, request: &LoginRequest, user: &GithubUser) -> Result<Option<Grant>> {
        let registration =
            signer_registration(self.config)?.context("registration is no longer enabled on this team")?;
        let issued = revoke::issued(self.dir)?;
        binding::check(&issued, &request.name, &user.account(), Binder::Signer)?;
        Ok(registration.grant(user).or_else(|| revoke::standing_approval(&issued, request.name.as_str(), user.id)))
    }

    /// Ask the humans whether the GitHub user `account`, outside the policy, may have the credentials `request` asks
    /// for, with an approval request of [`UserId::signer`] on a thread of its own, until a human decides or it expires:
    /// [`Registrar::approval_timeout`] at most, and never past the request. The registration waits meanwhile.
    async fn ask_humans(&mut self, approver: &Approver, request: &LoginRequest, account: GithubAccount) -> Result<()> {
        let waiting = self.flows.values().filter(|f| f.asking.is_some()).count();
        if waiting >= MAX_WAITING_REGISTRATIONS {
            bail!(
                "GitHub user {} is outside the team's registration policy, and {waiting} registrations wait for a \
                 human already: try again later",
                account.login
            );
        }
        let left = (request.expires_at - Utc::now()).to_std().unwrap_or_default().saturating_sub(LOGIN_REPEAT);
        let timeout = self.approval_timeout.min(left);
        if timeout < LEAST_ASKING {
            bail!(
                "GitHub user {} is outside the team's registration policy, and too little time is left to ask a human",
                account.login
            );
        }
        let approval = approval_request(request, &account, timeout);
        approver
            .hub()
            .publish(approval.subject(), serde_json::to_vec(&approval)?.into())
            .await
            .context("publishing the approval request")?
            .await
            .context("waiting for JetStream to store the approval request")?;
        (self.report)(request, &Event::Asked { login: account.login.clone(), id: approval.id });
        let reason = "outside the team's registration policy: a human of the team decides".to_owned();
        let flow = self.flows.get_mut(&request.public_key).context("the registration")?;
        flow.answer = Some(LoginAnswer::Waiting { login: account.login.clone(), reason });
        flow.asking = Some(Asking { account, approval });
        Ok(())
    }

    /// Act on the humans' decisions stored on the registrations asked about: sign those a human allowed, refuse
    /// those a human rejected or no human decided on in time.
    pub async fn look(&mut self, approver: &Approver) -> Result<()> {
        let asked: Vec<String> =
            self.flows.iter().filter(|(_, f)| f.asking.is_some()).map(|(k, _)| k.clone()).collect();
        if asked.is_empty() {
            return Ok(());
        }
        let stream = approver.hub().get_stream_no_info(STREAM).await.context("opening the ANTS stream")?;
        let now = Utc::now();
        for key in asked {
            let Some(Flow { request, asking: Some(asking), .. }) = self.flows.get(&key) else { continue };
            let approval = &asking.approval;
            let decision = match stream.direct_get_last_for_subject(approval.decision_subject()).await {
                Ok(msg) => serde_json::from_slice::<ApprovalDecision>(&msg.payload)
                    .ok()
                    .filter(|d| d.request_id == approval.id && d.ant == approval.ant),
                Err(e) if e.kind() == DirectGetErrorKind::NotFound => None,
                Err(e) => {
                    tracing::warn!(request = %approval.id, error = %e, "reading the decision");
                    None
                }
            };
            let answer = match decision {
                Some(d) => match approval.decide(d.verdict).verdict {
                    Verdict::AllowOnce | Verdict::AllowAlways => {
                        let by = d.by.map_or_else(|| "a human".to_owned(), |by| by.to_string());
                        self.signed(request, &asking.account, Grant::Approval(by))
                    }
                    Verdict::Reject => LoginAnswer::Refused { reason: "a human rejected the registration".to_owned() },
                },
                None if approval.is_expired(now) => {
                    LoginAnswer::Refused { reason: "no human decided on the registration in time".to_owned() }
                }
                None => continue,
            };
            let request = request.clone();
            self.settle(approver, request, answer).await?;
        }
        Ok(())
    }

    /// The answer signing `request` for the GitHub user `account` under `grant` ([`Registrar::sign`]), or refusing
    /// it.
    fn signed(&self, request: &LoginRequest, account: &GithubAccount, grant: Grant) -> LoginAnswer {
        match self.sign(request, account, grant.clone()) {
            Ok(jwt) => {
                (self.report)(request, &Event::Signed { login: account.login.clone(), grant });
                LoginAnswer::Signed { jwt }
            }
            Err(e) => LoginAnswer::Refused { reason: format!("{e:#}") },
        }
    }

    /// Sign a human's credentials for `request`, of the GitHub user `account`, under `grant`, if registration is still
    /// enabled and the name is the user's to hold: the JWT.
    fn sign(&self, request: &LoginRequest, account: &GithubAccount, grant: Grant) -> Result<String> {
        let registration =
            signer_registration(self.config)?.context("registration is no longer enabled on this team")?;
        let name = &request.name;
        binding::check_in(self.dir, name, account, Binder::Signer)?;
        let expires_at = auth::now()? + registration.ttl.as_secs();
        let new = auth::NewDevice {
            label: request.label.as_deref(),
            github: Some(account.clone()),
            grant: Some(grant),
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
        Ok(jwt)
    }

    /// Record `answer`, which settles `request`, and send it.
    async fn settle(&mut self, approver: &Approver, request: LoginRequest, answer: LoginAnswer) -> Result<()> {
        if let LoginAnswer::Refused { reason } = &answer {
            (self.report)(&request, &Event::Refused(reason.clone()));
        }
        let key = request.public_key.clone();
        self.flows.insert(key.clone(), Flow { request, answer: Some(answer), task: None, asking: None });
        self.answer(approver, &key).await
    }

    /// Send what the registration of `key` has to say, if anything, sealed to its latest request's reply key.
    async fn answer(&self, approver: &Approver, key: &str) -> Result<()> {
        let Some(Flow { request, answer: Some(answer), .. }) = self.flows.get(key) else { return Ok(()) };
        let recipient = XKey::from_public_key(&request.reply_xkey).map_err(|e| anyhow!("the reply key: {e}"))?;
        let sealed = self.xkey.seal(&serde_json::to_vec(answer)?, &recipient).map_err(|e| anyhow!("sealing: {e}"))?;
        let reply = LoginReply {
            name: request.name.clone(),
            public_key: request.public_key.clone(),
            sender: self.xkey.public_key(),
            sealed: BASE64URL_NOPAD.encode(&sealed),
        };
        approver.client.publish(reply.subject(), serde_json::to_vec(&reply)?.into()).await?;
        approver.client.flush().await?;
        Ok(())
    }

    /// Drop the registrations whose requests expired, stopping their device flows.
    pub fn expire(&mut self) {
        let now = Utc::now();
        self.flows.retain(|_, flow| flow.request.expires_at > now);
    }
}

/// The approval request asking the humans whether the GitHub user `account`, outside the registration policy, may
/// register a device of the human `request` names, for `timeout`: one of [`UserId::signer`], on a thread of its own,
/// offering to allow once or reject.
fn approval_request(request: &LoginRequest, account: &GithubAccount, timeout: Duration) -> ApprovalRequest {
    let (name, login, id) = (&request.name, &account.login, account.id);
    let title = format!("register human {name}: GitHub user {login} (id {id}), outside the registration policy");
    let input = serde_json::json!({
        "name": name,
        "github": { "login": login, "id": id },
        "label": request.label,
        "key": request.public_key,
    });
    let tool = ToolCall { kind: "other".to_owned(), title, locations: Vec::new(), input: Some(input.to_string()) };
    ApprovalRequest::new(UserId::signer(), ThreadId::generate(), tool, vec![Verdict::AllowOnce], timeout)
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
    use async_nats::jetstream::stream::Stream;
    use futures::StreamExt as _;
    use nkeys::KeyPair;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;
    use crate::github::tests::fake;
    use crate::{bus, signer, tools};

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
            let answers = register("carol").await;
            assert!(!waited(&answers) && signed(&answers), "the approval stands for another device: {answers:?}");

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
}
