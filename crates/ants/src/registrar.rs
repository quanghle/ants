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
//!
//! The signer also answers the renewal requests ([`RenewRequest`]) of the credentials a registration granted
//! ([`revoke::registered`]), which `ants enroll renew` leaves to it: an organization or team grant renews only while
//! the signer's own GitHub token finds the user still a member under the policy, reread; else the signer refuses,
//! revokes every key of the name and pushes the account. An approval grant renews while it stands; an email-domain
//! grant, which only the user's token shows, is refused, naming `ants login`.
//!
//! Between renewals, a membership sweep checks again every `[registration] sweep` the GitHub users of the live
//! credentials an organization or team granted ([`swept`]), with the signer's token, one user at a time, leaving
//! [`SWEEP_RESERVE`] requests of GitHub's rate limit to registrations and renewals: it revokes every key of a name whose
//! user GitHub definitely finds outside the policy, or gone, as a refused renewal does ([`Registrar::offboard`]), and
//! keeps it when GitHub cannot tell. It runs only while the signer's token sees every organization and team of the
//! policy ([`Github::sees_policy`]). Each name revoked so is recorded in [`OFFBOARDED`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use ants_daemon::config::registration::{GithubAccount, GithubUser, Grant, signer_registration};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, JsDomain, LOGIN_REPEAT, LoginAnswer, LoginReply, LoginRequest,
    MAX_REGISTRATIONS, Permissions, RenewReply, RenewRequest, Role, STREAM, ThreadId, ToolCall, UserId, Verdict,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use chrono::{DateTime, Utc};
use data_encoding::BASE64URL_NOPAD;
use nkeys::XKey;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::binding::{self, Binder};
use crate::enroll::Approver;
use crate::github::Github;
use crate::revoke::{self, Kind};
use crate::signer::AbortOnDrop;
use crate::{auth, devices};

/// Most registrations waiting for a human at once; more GitHub users outside the policy are refused.
pub const MAX_WAITING_REGISTRATIONS: usize = 3;
/// Least time a request must have left for a human to be asked about it.
const LEAST_ASKING: Duration = Duration::from_secs(10);
/// How long a renewal's answer is kept for the repeats of its request, and its membership check at most runs.
const RENEWAL_KEPT: Duration = Duration::from_mins(10);
/// Pause between the GitHub users a membership sweep checks.
const SWEEP_PACE: Duration = Duration::from_secs(1);
/// How long until the signer looks again whether a membership sweep may run, when one may not.
const SWEEP_RECHECK: Duration = Duration::from_mins(10);
/// Requests of the GitHub API rate limit's window a membership sweep leaves to registrations and renewals.
const SWEEP_RESERVE: u64 = 100;
/// The record, in the team directory, of the names whose keys the signer revoked as their GitHub users left the
/// registration policy: one [`Offboarded`] per line.
pub const OFFBOARDED: &str = "offboarded.jsonl";

/// What a registration's task found out.
enum Progress {
    /// The device flow began: sign in at `url` with `user_code` until `expires_at`.
    Verify { url: String, user_code: String, expires_at: DateTime<Utc> },
    /// Who signed in, or why no one did.
    SignedIn(Result<GithubUser>),
    /// For a renewal of `name`: the GitHub user of the credentials as the signer's token finds it now, and what of
    /// the policy grants it (none when no organization or team of it does); none when GitHub has no such user any
    /// more. Fails when GitHub cannot tell.
    Member { name: String, found: Result<Option<(GithubAccount, Option<Grant>)>> },
    /// For the membership sweep, the same of the GitHub user `account` the live credentials of `name` are bound to.
    Swept { name: String, account: GithubAccount, found: Result<Option<(GithubAccount, Option<Grant>)>> },
}

/// The membership sweep.
struct Sweep {
    /// When to look next whether a sweep is due.
    next: Instant,
    /// The running sweep, reporting [`Progress::Swept`] for each user it checks.
    task: Option<AbortOnDrop>,
    /// Why the sweep may not run, as printed last.
    off: Option<String>,
}

/// What revoked a name as its GitHub user left the registration policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Offboarder {
    /// A renewal refused.
    Renewal,
    /// The membership sweep.
    Sweep,
}

/// A line of [`OFFBOARDED`]: the keys of `name`, bound to the GitHub user `github`, revoked at `at` (seconds since the
/// epoch) by `by`, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offboarded {
    pub at: u64,
    pub name: String,
    pub github: GithubAccount,
    pub by: Offboarder,
    pub reason: String,
    pub keys: Vec<String>,
}

/// What the task of the registration (or renewal) of a key found out, for [`Registrar::progressed`].
pub struct Update(String, Progress);

/// A renewal of credentials a registration granted, by name and new key.
enum Renewal {
    /// The signer checks the memberships of the GitHub user of the credentials, in `_task`.
    Checking { request: RenewRequest, account: GithubAccount, _task: AbortOnDrop },
    /// The answer, which a repeat of the request gets again.
    Answered(RenewReply),
}

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

/// The registrations and renewals `ants signer` runs, kept across its reconnections.
pub struct Registrar<'a> {
    /// The server the account is pushed to, after a revocation.
    url: &'a str,
    dir: &'a Path,
    /// The config file whose `[registration]` table is the policy, read for each request.
    config: &'a Path,
    js_domain: Option<&'a JsDomain>,
    /// How long a human has to decide on a registration outside the policy, at most until its request expires.
    approval_timeout: Duration,
    /// The signer's key sealing the answers.
    xkey: XKey,
    flows: HashMap<String, Flow>,
    /// The renewals by name and new key, since when.
    renewals: HashMap<(String, String), (Instant, Renewal)>,
    progress: (mpsc::UnboundedSender<Update>, mpsc::UnboundedReceiver<Update>),
    sweep: Sweep,
    report: fn(&LoginRequest, &Event),
}

impl<'a> Registrar<'a> {
    pub fn new(
        url: &'a str,
        dir: &'a Path,
        config: &'a Path,
        js_domain: Option<&'a JsDomain>,
        approval_timeout: Duration,
        report: fn(&LoginRequest, &Event),
    ) -> Self {
        let progress = mpsc::unbounded_channel();
        let (flows, renewals) = (HashMap::new(), HashMap::new());
        let xkey = XKey::new();
        let sweep = Sweep { next: Instant::now(), task: None, off: None };
        Self { url, dir, config, js_domain, approval_timeout, xkey, flows, renewals, progress, sweep, report }
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
        let progress = match progress {
            Progress::Member { name, found } => return self.member(approver, (name, key), found).await,
            Progress::Swept { name, account, found } => {
                self.swept(&name, &account, found).await;
                return Ok(());
            }
            progress => progress,
        };
        let Some(flow) = self.flows.get_mut(&key) else { return Ok(()) };
        let user = match progress {
            Progress::Verify { url, user_code, expires_at } => {
                flow.answer = Some(LoginAnswer::Verify { url, user_code, expires_at });
                return self.answer(approver, &key).await;
            }
            Progress::SignedIn(user) => user,
            Progress::Member { .. } | Progress::Swept { .. } => unreachable!("handled above"),
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
            ..auth::NewDevice::default()
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

    /// Drop the registrations whose requests expired, stopping their device flows, and the renewals older than
    /// [`RENEWAL_KEPT`], stopping their membership checks.
    pub fn expire(&mut self) {
        let now = Utc::now();
        self.flows.retain(|_, flow| flow.request.expires_at > now);
        self.renewals.retain(|_, (since, _)| since.elapsed() < RENEWAL_KEPT);
    }

    /// Answer the renewal request `msg` if it is one of credentials a registration granted ([`revoke::registered`]):
    /// a repeat with the answer it had, a new one by renewing them while their grant holds, else refusing. An
    /// organization or team grant is checked again on GitHub first, by a task reporting [`Progress::Member`].
    pub async fn renewal(&mut self, approver: &Approver, msg: &async_nats::Message) -> Result<()> {
        let (request, current) = match approver.checked_renewal(msg) {
            Ok(checked) => checked,
            Err(e) => {
                tracing::warn!("ignoring a renewal request: {e:#}");
                return Ok(());
            }
        };
        let key = (request.name.clone(), request.public_key.clone());
        match self.renewals.get(&key) {
            Some((_, Renewal::Answered(reply))) => return approver.answer_renewal(reply).await,
            Some((_, Renewal::Checking { .. })) => return Ok(()),
            None => {}
        }
        let issued = revoke::issued(self.dir)?;
        let Some(record) = revoke::registered(&issued, &current.sub) else { return Ok(()) };
        let (Some(account), Some(grant)) = (record.github.clone(), record.grant.clone()) else { return Ok(()) };
        let name = request.name.as_str();
        let reply = match grant {
            Grant::Approval(_) => match revoke::standing_approval(&issued, name, account.id) {
                Some(grant) => self.renewed(&request, account, grant),
                None => self.refused(
                    &request,
                    "the approval that registered the device was withdrawn: register it again with `ants login`".into(),
                ),
            },
            Grant::EmailDomain(domain) => self.refused(
                &request,
                format!(
                    "a verified email address in {domain} registered the device, which only signing in again shows: \
                     register it again with `ants login`"
                ),
            ),
            Grant::Org(_) | Grant::Team(_) => {
                let checking = self.renewals.values().filter(|(_, r)| matches!(r, Renewal::Checking { .. })).count();
                if checking >= MAX_REGISTRATIONS {
                    // The holder asks again.
                    return Ok(());
                }
                match self.check_member(&request, &account) {
                    Ok(task) => {
                        print_renewal(&request, &format!("checking the memberships of GitHub user {}", account.login));
                        let checking = Renewal::Checking { request, account, _task: task };
                        self.renewals.insert(key, (Instant::now(), checking));
                        return Ok(());
                    }
                    Err(e) => self.refused(&request, format!("{e:#}: try again later")),
                }
            }
        };
        self.renewals.insert(key, (Instant::now(), Renewal::Answered(reply.clone())));
        approver.answer_renewal(&reply).await
    }

    /// Begin checking on GitHub, with the signer's token, whether the GitHub user `account` is still a member under
    /// the policy, for the renewal `request`: a task reporting [`Progress::Member`].
    fn check_member(&self, request: &RenewRequest, account: &GithubAccount) -> Result<AbortOnDrop> {
        let registration =
            signer_registration(self.config).context("the signer's registration policy does not load")?.with_context(
                || format!("registration is no longer enabled on this team to check GitHub user {}", account.login),
            )?;
        let github = Github::new(Some(&registration))?;
        let (id, tx) = (account.id, self.progress.0.clone());
        let (key, name) = (request.public_key.clone(), request.name.clone());
        Ok(AbortOnDrop(tokio::spawn(async move {
            let found = github.member(id, &registration).await.map(|user| {
                user.map(|user| {
                    let grant = registration.grant(&user);
                    (user.account(), grant)
                })
            });
            let _ = tx.send(Update(key, Progress::Member { name, found }));
        })))
    }

    /// Act on what the membership check of the renewal `key` found, and answer: renew the credentials under the
    /// policy's grant; refuse, then revoke every key of the name and push the account, when the user is no member, or
    /// no GitHub user any more; refuse alone when GitHub could not tell. The refusal goes first, as the push closes
    /// the asking connection.
    async fn member(
        &mut self,
        approver: &Approver,
        key: (String, String),
        found: Result<Option<(GithubAccount, Option<Grant>)>>,
    ) -> Result<()> {
        let Some((since, Renewal::Checking { request, account, .. })) = self.renewals.remove(&key) else {
            return Ok(());
        };
        let left = |why: &str| format!("{why}: every key of {} is revoked", request.name);
        let (reply, revoking) = match found {
            Ok(Some((now, Some(grant)))) => (self.renewed(&request, now, grant), None),
            Ok(found) => {
                let why = gone(&account, found.as_ref().map(|(now, _)| now));
                (self.refused(&request, left(&why)), Some(why))
            }
            Err(e) => {
                let why = format!("the memberships of GitHub user {} could not be checked ({e:#})", account.login);
                (self.refused(&request, format!("{why}: try again later")), None)
            }
        };
        self.renewals.insert(key, (since, Renewal::Answered(reply.clone())));
        approver.answer_renewal(&reply).await?;
        if let Some(why) = revoking {
            let what = self.offboard(&request.name, &account, &why, Offboarder::Renewal).await;
            print_renewal(&request, &what);
        }
        Ok(())
    }

    /// The answer renewing the credentials of `request` for the GitHub user `account` under `grant`
    /// ([`auth::renew_registered`]), or refusing it.
    fn renewed(&self, request: &RenewRequest, account: GithubAccount, grant: Grant) -> RenewReply {
        let (login, why) = (account.login.clone(), grant.to_string());
        let renewed = auth::renew_registered(
            self.dir,
            &request.current,
            &request.public_key,
            request.xkey.as_ref(),
            account,
            grant,
        );
        match renewed {
            Ok(renewed) => {
                print_renewal(request, &format!("renewed: GitHub user {login}, {why}"));
                RenewReply::signed(&request.name, &request.public_key, renewed.jwt)
            }
            Err(e) => self.refused(request, format!("{e:#}")),
        }
    }

    /// The answer refusing to renew the credentials of `request`, and why.
    fn refused(&self, request: &RenewRequest, reason: String) -> RenewReply {
        print_renewal(request, &format!("refused: {reason}"));
        RenewReply::refused(&request.name, &request.public_key, reason)
    }

    /// Revoke every key of `name`, bound to the GitHub user `account`, which left the registration policy for the
    /// reason `why`, as `ants auth revoke` does: record it in [`OFFBOARDED`], push the account, and record the revoked
    /// devices in the hub's directory ([`devices::record_revocations`]). Returns what happened, in words.
    async fn offboard(&self, name: &str, account: &GithubAccount, why: &str, by: Offboarder) -> String {
        let revoked = match revoke::revoke(self.dir, Some(name), &[]) {
            Ok(revoked) => revoked,
            Err(e) => return format!("revoking the keys of {name} failed ({e:#}): `ants auth revoke {name}`"),
        };
        let keys: Vec<String> = revoked.into_iter().map(|r| r.key).collect();
        let n = keys.len();
        tracing::warn!(name, github = %account.login, by = ?by, keys = n, "revoked as {why}");
        let at = auth::now().unwrap_or_default();
        let line = Offboarded { at, name: name.to_owned(), github: account.clone(), by, reason: why.to_owned(), keys };
        if let Err(e) = record_offboarded(self.dir, &line) {
            tracing::warn!(name, "recording the revocation in {OFFBOARDED}: {e:#}");
        }
        let pushed = match revoke::push(self.url, self.dir).await {
            Ok(_) => match devices::record_revocations(self.url, self.js_domain, self.dir).await {
                Ok(_) => String::new(),
                Err(e) => format!(", but recording the revoked devices failed ({e:#}): `ants auth push`"),
            },
            Err(e) => format!(", but pushing the account failed ({e:#}): `ants auth push`"),
        };
        format!("{n} keys of {name} revoked{pushed}")
    }

    /// Begin a membership sweep if one is due and may run: a task checking on GitHub, one at a time, the users the
    /// live credentials an organization or team granted are bound to ([`swept`]), reporting [`Progress::Swept`] for
    /// each. A sweep runs every `[registration] sweep`, the first at once; while one may not, the signer says why once
    /// and looks again every [`SWEEP_RECHECK`].
    pub fn sweep(&mut self) {
        let now = Instant::now();
        if now < self.sweep.next || self.sweep.task.as_ref().is_some_and(|task| !task.0.is_finished()) {
            return;
        }
        match self.start_sweep() {
            Ok((every, task)) => {
                if self.sweep.off.take().is_some() {
                    print_sweep(&format!("on, every {}", ants_proto::duration::format(every)));
                }
                (self.sweep.next, self.sweep.task) = (now + every, task);
            }
            Err(why) => {
                if self.sweep.off.as_ref() != Some(&why) {
                    print_sweep(&format!("off: {why}"));
                }
                (self.sweep.next, self.sweep.task, self.sweep.off) = (now + SWEEP_RECHECK, None, Some(why));
            }
        }
    }

    /// The task of a membership sweep, if there is anyone to check, and when the next is due; else why none may run.
    fn start_sweep(&self) -> Result<(Duration, Option<AbortOnDrop>), String> {
        let registration = match signer_registration(self.config) {
            Ok(Some(registration)) => registration,
            Ok(None) => return Err(format!("{} has no [registration] table", self.config.display())),
            Err(e) => return Err(format!("the registration policy does not load ({e:#})")),
        };
        if registration.sweep.is_zero() {
            return Err("[registration] sweep is 0".to_owned());
        }
        if registration.orgs.is_empty() && registration.teams.is_empty() {
            return Err("the registration policy names no organization or team".to_owned());
        }
        let github = Github::new(Some(&registration)).map_err(|e| format!("{e:#}"))?;
        if !github.has_token() {
            return Err(
                "no GitHub token of the signer reads memberships ([registration] token_file or token_store)".to_owned()
            );
        }
        let users = swept(self.dir).map_err(|e| format!("{e:#}"))?;
        if users.is_empty() {
            return Ok((registration.sweep, None));
        }
        let (every, tx) = (registration.sweep, self.progress.0.clone());
        Ok((
            every,
            Some(AbortOnDrop(tokio::spawn(async move {
                let rate_wait = async || {
                    if let Some(wait) = github.rate_wait(SWEEP_RESERVE) {
                        tracing::info!("membership sweep: waiting {wait:?} for GitHub's rate limit");
                        tokio::time::sleep(wait).await;
                    }
                };
                rate_wait().await;
                if let Err(e) = github.sees_policy(&registration).await {
                    print_sweep(&format!("skipped, as the signer's token cannot tell who is a member: {e:#}"));
                    return;
                }
                print_sweep(&format!("checking the GitHub users of {} names", users.len()));
                for (i, (name, account)) in users.into_iter().enumerate() {
                    if i > 0 {
                        tokio::time::sleep(SWEEP_PACE).await;
                    }
                    rate_wait().await;
                    let found = github.member(account.id, &registration).await.map(|user| {
                        user.map(|user| {
                            let grant = registration.grant(&user);
                            (user.account(), grant)
                        })
                    });
                    let _ = tx.send(Update(name.clone(), Progress::Swept { name, account, found }));
                }
            }))),
        ))
    }

    /// Act on what the membership sweep found of the GitHub user `account` the live credentials of `name` are bound
    /// to: revoke every key of the name ([`Registrar::offboard`]) when GitHub definitely finds the user outside the
    /// policy, or gone, and the sweep still covers the name; keep it when GitHub could not tell.
    async fn swept(&self, name: &str, account: &GithubAccount, found: Result<Option<(GithubAccount, Option<Grant>)>>) {
        let why = match found {
            Ok(Some((_, Some(_)))) => return,
            Ok(found) => gone(account, found.as_ref().map(|(now, _)| now)),
            Err(e) => {
                print_sweep(&format!(
                    "{name} kept, as the memberships of GitHub user {} could not be checked ({e:#})",
                    account.login
                ));
                return;
            }
        };
        let still = signer_registration(self.config).ok().flatten().is_some_and(|r| !r.sweep.is_zero())
            && swept(self.dir).is_ok_and(|users| users.get(name).is_some_and(|bound| bound.id == account.id));
        if still {
            let what = self.offboard(name, account, &why, Offboarder::Sweep).await;
            print_sweep(&format!("{why}: {what}"));
        }
    }
}

/// Why the GitHub user `account` falls outside the registration policy: GitHub finds the user, as `now`, no member of
/// its organizations or teams, or no user any more.
fn gone(account: &GithubAccount, now: Option<&GithubAccount>) -> String {
    match now {
        Some(now) => format!("GitHub user {} is no member of the team's organizations or teams any more", now.login),
        None => format!("GitHub has no user {} (id {}) any more", account.login, account.id),
    }
}

/// The names a membership sweep covers, each with the GitHub user it is bound to: those with live credentials (neither
/// revoked nor expired) an organization or team of the policy granted.
fn swept(dir: &Path) -> Result<BTreeMap<String, GithubAccount>> {
    let revoked: HashSet<String> = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
    let now = auth::now()?;
    let mut latest: HashMap<String, revoke::Issued> = HashMap::new();
    for one in revoke::issued(dir)? {
        latest.insert(one.key.clone(), one);
    }
    let live = latest.into_values().filter(|i| !revoked.contains(&i.key) && i.expires_at.is_none_or(|exp| exp > now));
    let granted = live.filter(|i| matches!(i.grant, Some(Grant::Org(_) | Grant::Team(_))));
    Ok(granted.filter_map(|i| Some((i.name, i.github?))).collect())
}

/// Append `line` to [`OFFBOARDED`] in `dir`.
fn record_offboarded(dir: &Path, line: &Offboarded) -> Result<()> {
    let path = dir.join(OFFBOARDED);
    let mut text = serde_json::to_string(line)?;
    text.push('\n');
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(text.as_bytes()).with_context(|| format!("writing {}", path.display()))
}

/// Print what the membership sweep did.
fn print_sweep(what: &str) {
    println!("membership sweep: {what}");
}

/// Print what the signer did with the renewal `request`.
fn print_renewal(request: &RenewRequest, what: &str) {
    println!("renewal of {} (new key {}, host {}): {what}", request.name, request.public_key, request.host);
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use ants_daemon::creds::{Finding, Kind as CredsKind};
    use ants_daemon::keystore;
    use ants_proto::{AntRequest, DeviceKey, UserId, subject};
    use async_nats::jetstream::stream::Stream;
    use futures::StreamExt as _;
    use nkeys::KeyPair;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;
    use crate::github::tests::fake;
    use crate::{bus, renew, signer, tools};

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
                            format!(
                                r#"{{"access_token":"tok-{n}","token_type":"bearer","scope":"read:org,user:email"}}"#
                            ),
                        )
                    } else {
                        flows.1.push(code);
                        (200, r#"{"error":"authorization_pending"}"#.to_owned())
                    }
                }
                ("GET /user", Some((id, login, _, _))) => (200, format!(r#"{{"id":{id},"login":"{login}"}}"#)),
                ("GET /user/emails", Some((_, _, email, _))) => (
                    200,
                    format!(r#"[{{"email":"{email}","verified":true}},{{"email":"x@acme.dev","verified":false}}]"#),
                ),
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
        let renewed = renew::request(&server.url, path, path, &user, None, keystore::os(), REPEAT, pending()).await;
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
        let url = github_checking(
            &[(1, "octocat", "o@example.com", true), (3, "erin", "e@acme.example", false)],
            still.clone(),
        )
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
        let revoked =
            |device: &Device| revoke::revoked(&team).unwrap().iter().any(|r| r.key == device.key.public_key());

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
}
