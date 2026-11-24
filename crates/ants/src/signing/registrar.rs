//! Self-service registration of humans' devices, answered by `ants signer`: a machine running `ants login` asks with
//! the bus's public bootstrap credentials ([`LoginRequest`] on [`subject::login`](ants_proto::subject::login)), the
//! signer runs a GitHub device flow for it ([`Github`]), tells the machine where to sign in, reads who signed in, and
//! signs a human's credentials for the request's key when the GitHub user falls under the signer's `[registration]`
//! policy and may hold the name asked for ([`binding`]). A user outside the policy waits for the humans: the signer
//! asks them with an approval request of the reserved user [`UserId::signer`], and signs once one allows it. The
//! approval stands for that user and name until every key of the name is revoked ([`issued::standing_approval`]), so
//! the user's other devices sign at once.
//!
//! Requests are core NATS messages and stored nowhere: the machine repeats its request every
//! [`ants_proto::LOGIN_REPEAT`] until an answer settles it, and the signer answers each repeat of a key with what it
//! has for it, sealed to the request's one-off X25519 key ([`LoginReply`]). One device flow runs per key, and at most
//! [`MAX_REGISTRATIONS`] at once. The user's GitHub token stays in the task that reads the user with it.
//!
//! The signer also answers the renewal requests ([`RenewRequest`]) of the credentials a registration granted
//! ([`issued::registered`]), which `ants enroll renew` leaves to it: an organization or team grant renews only while
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
//!
//! [`binding`]: crate::signing::binding
//! [`issued::standing_approval`]: crate::credentials::issued::standing_approval
//! [`LoginReply`]: ants_proto::LoginReply
//! [`MAX_REGISTRATIONS`]: ants_proto::MAX_REGISTRATIONS
//! [`issued::registered`]: crate::credentials::issued::registered
//! [`swept`]: offboard::swept
//! [`Github`]: crate::signing::github::Github
//! [`Github::sees_policy`]: crate::signing::github::Github::sees_policy

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use ants_daemon::config::registration::{GithubAccount, GithubUser, Grant};
use ants_proto::{ApprovalRequest, JsDomain, LoginAnswer, LoginRequest, RenewReply, RenewRequest, UserId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use nkeys::XKey;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::credentials::approver::Approver;
use crate::signing::signer::AbortOnDrop;

mod login;
mod offboard;
mod renewal;

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
/// The record, in the auth directory, of the names whose keys the signer revoked as their GitHub users left the
/// registration policy: one [`Offboarded`] per line.
///
/// [`Offboarded`]: offboard::Offboarded
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
    let web = if request.web { " as a web device" } else { "" };
    let asked = format!("registration of {}{web} (key {})", request.name, request.public_key);
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

    /// Drop the registrations whose requests expired, stopping their device flows, and the renewals older than
    /// [`RENEWAL_KEPT`], stopping their membership checks.
    pub fn expire(&mut self) {
        let now = Utc::now();
        self.flows.retain(|_, flow| flow.request.expires_at > now);
        self.renewals.retain(|_, (since, _)| since.elapsed() < RENEWAL_KEPT);
    }
}

#[cfg(test)]
mod tests;
