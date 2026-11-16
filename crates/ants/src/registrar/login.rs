//! Registrations: checking a login request, running the GitHub device flow for its key, granting under the policy
//! or asking the humans, and answering with credentials signed for the key or a refusal.

use std::time::Duration;

use ants_daemon::config::registration::{GithubAccount, GithubUser, Grant, signer_registration};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, LOGIN_REPEAT, LoginAnswer, LoginReply, LoginRequest, MAX_REGISTRATIONS,
    Permissions, Role, STREAM, ThreadId, ToolCall, UserId, Verdict,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use chrono::Utc;
use data_encoding::BASE64URL_NOPAD;
use nkeys::XKey;

use super::{Asking, Event, Flow, LEAST_ASKING, MAX_WAITING_REGISTRATIONS, Progress, Registrar, Update};
use crate::auth;
use crate::binding::{self, Binder};
use crate::enroll::Approver;
use crate::github::Github;
use crate::revoke::{self, Kind};
use crate::signer::AbortOnDrop;

impl Registrar<'_> {
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

    /// What grants the GitHub user `user` the credentials `request` asks for: the policy, reread, or an approval
    /// that stands for the user and name; none when a human must decide. Fails when the name is not the user's to
    /// hold.
    pub(super) fn grant(&self, request: &LoginRequest, user: &GithubUser) -> Result<Option<Grant>> {
        let registration =
            signer_registration(self.config)?.context("registration is no longer enabled on this team")?;
        let issued = revoke::issued(self.dir)?;
        binding::check(&issued, &request.name, &user.account(), Binder::Signer)?;
        Ok(registration.grant(user).or_else(|| revoke::standing_approval(&issued, request.name.as_str(), user.id)))
    }

    /// Ask the humans whether the GitHub user `account`, outside the policy, may have the credentials `request` asks
    /// for, with an approval request of [`UserId::signer`] on a thread of its own, until a human decides or it expires:
    /// [`Registrar::approval_timeout`] at most, and never past the request. The registration waits meanwhile.
    pub(super) async fn ask_humans(
        &mut self,
        approver: &Approver,
        request: &LoginRequest,
        account: GithubAccount,
    ) -> Result<()> {
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
    pub(super) fn signed(&self, request: &LoginRequest, account: &GithubAccount, grant: Grant) -> LoginAnswer {
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
    pub(super) async fn settle(
        &mut self,
        approver: &Approver,
        request: LoginRequest,
        answer: LoginAnswer,
    ) -> Result<()> {
        if let LoginAnswer::Refused { reason } = &answer {
            (self.report)(&request, &Event::Refused(reason.clone()));
        }
        let key = request.public_key.clone();
        self.flows.insert(key.clone(), Flow { request, answer: Some(answer), task: None, asking: None });
        self.answer(approver, &key).await
    }

    /// Send what the registration of `key` has to say, if anything, sealed to its latest request's reply key.
    pub(super) async fn answer(&self, approver: &Approver, key: &str) -> Result<()> {
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
}

/// The approval request asking the humans whether the GitHub user `account`, outside the registration policy, may
/// register a device of the human `request` names, for `timeout`: one of [`UserId::signer`], on a thread of its own,
/// offering to allow once or reject.
pub(super) fn approval_request(request: &LoginRequest, account: &GithubAccount, timeout: Duration) -> ApprovalRequest {
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
pub(super) fn check(request: &LoginRequest) -> Result<()> {
    auth::check_user_key(&request.public_key)?;
    XKey::from_public_key(&request.reply_xkey).map_err(|e| anyhow!("the reply key {}: {e}", request.reply_xkey))?;
    if !request.is_live(Utc::now()) {
        bail!("the request expired at {} or was made for too long", request.expires_at);
    }
    auth::check_signature(&request.public_key, &request.signed(), &request.signature)
}
