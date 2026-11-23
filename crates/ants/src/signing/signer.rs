//! Ants added by manager ants, signed on the machine holding the account key: a manager asks for an ant from a
//! template over NATS ([`ask`], behind its `request_ant` tool), and `ants signer` there ([`serve`]) signs its
//! credentials when the request keeps within the signer's own copy of the template's policy, asks the humans through
//! the approvals path otherwise, and refuses what it can never sign. A manager retires an ant it added the same way
//! ([`retire`], behind its `retire_ant` tool): the signer revokes the ant's keys ([`revoke_added`]) and pushes the
//! account to the hub, so the name and its place under the template's `max_ants` are free again.
//!
//! Requests are core NATS messages and stored nowhere, as enrollment requests are ([`crate::credentials::enroll`]): the manager's
//! machine repeats its request every [`ENROLL_REPEAT`] until an answer comes or it stops waiting
//! ([`AntRequest::expires_at`]), and the signer answers each repeat of a request it decided the same way.
//!
//! The signer answers ants' reactions requests as well ([`reactions::serve`]): an ant's credentials neither read nor
//! write the reactions bucket, so the signer lists the reactions to a message for the ant, and adds or removes the
//! ant's own, once it found the ant reads the message, and ants' trail saves ([`keeper::serve`]): an ant's credentials
//! write no saved trail, whose key names no saver, so the signer saves a trail for the ant as its own, and replaces or
//! removes only one the ant saved. It also registers humans' devices after a GitHub sign-in, and
//! renews the credentials such registrations granted ([`crate::signing::registrar`]), and answers the enrollment requests a
//! machine's daemon sends carrying an admin's order to add or retire an ant there ([`ordered`]). Each time it connects,
//! it records its machine as the one holding the account key ([`AccountHolder`]), for `ants chat` and `ants machines`
//! elsewhere to name it.
//!
//! The signer trusts only what it can check itself: the request is signed by the manager's credentials of the team's
//! account, and its template is read from the signer's own config file ([`config::signer_templates`]), never taken
//! from the manager's machine. How many ants a template has is counted from the keys the team issued
//! ([`issued::Issued::template`]), live ones only.

use ants_daemon::bus::relayed;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::Duration;

use ants_daemon::colony;
use ants_daemon::config;
use ants_daemon::profile::template::{AntTemplate, Request};
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::records::reactions;
use ants_daemon::trail::keeper;
use ants_proto::{
    ACCOUNT_HOLDER_KEY, ANTS_VERSION, AccountHolder, AntReply, AntRequest, ApprovalDecision, ApprovalRequest,
    ENROLL_REPEAT, JsDomain, MACHINES_BUCKET, Permissions, STREAM, ThreadId, ToolCall, UserId, Verdict, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;

use crate::auth;
use crate::credentials::enroll::Approver;
use crate::credentials::issued;
use crate::credentials::revoke;
use crate::node::{members, outbox};
use crate::signing::registrar::{self, Registrar};

mod asking;
mod ordered;

pub use asking::{Requester, Retired, ask, retire};

/// Most requests of one manager waiting for a human at once; more are refused.
pub const MAX_WAITING: usize = 3;
/// How often the signer looks for the humans' decisions on the requests it asked them about.
pub const TICK: Duration = Duration::from_secs(1);
/// Least time a request must have left for a human to be asked about it.
const LEAST_ASKING: Duration = Duration::from_secs(10);

/// How `ants signer` runs.
pub struct Options<'a> {
    /// The config file whose `[[ant_template]]` tables are the policy.
    pub config: &'a Path,
    /// How long a human has to decide on a request outside the policy, at most until the manager stops waiting, or on
    /// a registration outside the registration policy, at most until its request expires.
    pub approval_timeout: Duration,
    /// How long it stays connected with one set of the credentials it issues itself ([`crate::credentials::renew::AUTO_RECONNECT`]).
    pub reconnect: Duration,
    /// How often it looks for decisions ([`TICK`]).
    pub tick: Duration,
}

/// What the signer did with a request.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// Signed, within the policy (`human: false`) or on a human's approval.
    Approved { human: bool },
    /// Refused, and why.
    Refused(String),
    /// Asked the humans with approval request `id`, as the request is outside the policy for the reason given.
    Asked { id: ants_proto::Uuid, why: String },
    /// Retired, revoking `revoked` keys, the push of the account to the hub failing with `push_error` if it did.
    Retired { revoked: usize, push_error: Option<String> },
}

/// What approved a request the signer signs: the template's policy, or a human.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ApprovedBy {
    Policy,
    Human,
}

/// Print what the signer did with `request`.
pub fn print(request: &AntRequest, event: &Event) {
    if request.retire {
        let asked = format!(
            "{} retiring {} (key {}, host {})",
            request.manager, request.name, request.public_key, request.host
        );
        match event {
            Event::Retired { revoked, push_error: None } => println!("retired for {asked}: {revoked} keys revoked"),
            Event::Retired { revoked, push_error: Some(e) } => println!(
                "retired for {asked}: {revoked} keys revoked, but pushing the account failed ({e}): `ants auth push`"
            ),
            Event::Refused(reason) => println!("refused {asked}: {reason}"),
            other => println!("{asked}: {other:?}"),
        }
        return;
    }
    let asked = format!(
        "{} for {} from template {} (key {}, host {})",
        request.manager, request.name, request.template, request.public_key, request.host
    );
    match event {
        Event::Approved { human: false } => println!("signed {asked}: within the template's policy"),
        Event::Approved { human: true } => println!("signed {asked}: a human approved"),
        Event::Refused(reason) => println!("refused {asked}: {reason}"),
        Event::Asked { id, why } => {
            println!("asked the humans about {asked}, as {why}: `ants tools approve|reject {}/{id}`", request.manager);
        }
        Event::Retired { .. } => println!("retired {asked}"),
    }
}

/// What [`Signer::judge`] makes of a request.
enum Judgement {
    Approve,
    Ask(String),
    Refuse(String),
}

/// A request a human is asked about, and the approval request asking.
struct Waiting {
    request: AntRequest,
    approval: ApprovalRequest,
}

/// `ants signer`: answer the ant requests of manager ants ([`AntRequest`]) over the server at `url`, signing with the
/// account key in the team directory `dir`, ants' reactions requests ([`reactions::serve`]) and trail saves
/// ([`keeper::serve`]), until `stop`
/// completes. Each run connects afresh every
/// [`Options::reconnect`], with credentials it issues itself, and reads the revocations again; the templates are read
/// for each request. Calls `report` with what it did with each request.
pub async fn serve(
    url: &str,
    dir: &Path,
    js_domain: Option<&JsDomain>,
    options: &Options<'_>,
    mut report: impl FnMut(&AntRequest, &Event),
    stop: impl Future<Output = ()>,
) -> Result<()> {
    let mut signer = Signer { url, dir, options, answered: HashMap::new(), waiting: Vec::new() };
    let mut registrar = Registrar::new(url, dir, options.config, js_domain, options.approval_timeout, registrar::print);
    let mut ordered = ordered::Ordered::default();
    let holder = holder(options.config).await;
    tokio::pin!(stop);
    loop {
        let approver = Approver::connect_signer(url, dir, js_domain, crate::credentials::enroll::LISTEN).await?;
        record_holder(&approver, js_domain, &holder).await;
        let mut requests = relayed::subscribe(&approver.client, subject::ALL_ANT_REQUESTS).await?;
        let mut logins = relayed::subscribe(&approver.client, subject::ALL_LOGIN_REQUESTS).await?;
        let mut renewals = relayed::subscribe(&approver.client, subject::ALL_RENEW_REQUESTS).await?;
        let mut enrollments = relayed::subscribe(&approver.client, subject::ALL_ENROLL_REQUESTS).await?;
        let answering = reactions::serve(
            approver.client.clone(),
            approver.hub().clone(),
            js_domain.cloned(),
            reactions::Answering::ListsAndChanges,
        );
        let _answering = AbortOnDrop(tokio::spawn(async move {
            if let Err(e) = answering.await {
                tracing::warn!("answering reactions requests until the next reconnect: {e:#}");
            }
        }));
        let keeping = keeper::serve(approver.client.clone(), approver.hub().clone(), js_domain.cloned());
        let _keeping = AbortOnDrop(tokio::spawn(async move {
            if let Err(e) = keeping.await {
                tracing::warn!("answering trail saves until the next reconnect: {e:#}");
            }
        }));
        approver.client.flush().await?;
        let until = tokio::time::sleep(options.reconnect);
        let mut ticks = tokio::time::interval(options.tick);
        tokio::pin!(until);
        loop {
            tokio::select! {
                msg = requests.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    signer.heard(&approver, &msg, &mut report).await?;
                }
                msg = logins.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    registrar.heard(&approver, &msg).await?;
                }
                msg = renewals.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    registrar.renewal(&approver, &msg).await?;
                }
                msg = enrollments.next() => {
                    let msg = msg.context("the NATS connection closed")?;
                    ordered.heard(&approver, url, &msg).await?;
                }
                update = registrar.next() => registrar.progressed(&approver, update).await?,
                _ = ticks.tick() => {
                    registrar.expire();
                    registrar.sweep();
                    registrar.look(&approver).await?;
                    signer.look(&approver, &mut report).await?;
                }
                () = &mut until => break,
                () = &mut stop => return Ok(()),
            }
        }
    }
}

/// The record of this machine holding the account key: its name as the `[host]` table of the config file at `config`
/// names it, else as its top-level `machine` does if that is a machine name, else the first label of its host name.
async fn holder(config: &Path) -> AccountHolder {
    let hostname = colony::machine().await;
    let host = config::Summary::load(config).map_or_else(|_| config::HostSettings::default(), |summary| summary.host);
    AccountHolder {
        machine: host.machine_or(&hostname),
        hostname,
        version: ANTS_VERSION.to_owned(),
        updated_at: Utc::now(),
    }
}

/// Put `holder` in [`MACHINES_BUCKET`], as of now, for machines without the account key to name this one; a failure is
/// only warned about, as answering requests goes on without it.
async fn record_holder(approver: &Approver, js_domain: Option<&JsDomain>, holder: &AccountHolder) {
    let holder = AccountHolder { updated_at: Utc::now(), ..holder.clone() };
    let put = async {
        let kv = DirectKv::new(approver.hub(), js_domain, MACHINES_BUCKET).await?;
        kv.put(ACCOUNT_HOLDER_KEY, serde_json::to_vec(&holder)?).await
    };
    if let Err(e) = put.await {
        tracing::warn!(
            error = format!("{e:#}"),
            "recording where the account key is failed; `ants init` creates the {MACHINES_BUCKET} bucket"
        );
    }
}

pub(crate) struct AbortOnDrop(pub(crate) tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Signer<'a> {
    url: &'a str,
    dir: &'a Path,
    options: &'a Options<'a>,
    /// The answer to each manager, ant, key and whether to retire it: a machine that missed it asks again and gets
    /// the same.
    answered: HashMap<Key, (AntReply, DateTime<Utc>)>,
    waiting: Vec<Waiting>,
}

type Key = (UserId, UserId, String, bool);

fn key(request: &AntRequest) -> Key {
    (request.manager.clone(), request.name.clone(), request.public_key.clone(), request.retire)
}

impl Signer<'_> {
    async fn heard(
        &mut self,
        approver: &Approver,
        msg: &async_nats::Message,
        report: &mut impl FnMut(&AntRequest, &Event),
    ) -> Result<()> {
        let request = match serde_json::from_slice::<AntRequest>(&msg.payload) {
            Ok(request) => request,
            Err(e) => {
                tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed ant request");
                return Ok(());
            }
        };
        if let Err(e) =
            request.check_subject(&msg.subject).map_err(anyhow::Error::from).and_then(|()| check(approver, &request))
        {
            tracing::warn!(manager = %request.manager, ant = %request.name, "ignoring an ant request: {e:#}");
            return Ok(());
        }
        if let Some((reply, _)) = self.answered.get(&key(&request)) {
            return answer(approver, reply).await;
        }
        if self.waiting.iter().any(|w| key(&w.request) == key(&request)) {
            return Ok(());
        }
        if request.retire {
            return self.retire(approver, &request, report).await;
        }
        let judgement = match self.judge(&request) {
            Ok(judgement) => judgement,
            Err(e) => Judgement::Refuse(format!("{e:#}")),
        };
        match judgement {
            Judgement::Approve => self.sign(approver, &request, ApprovedBy::Policy, report).await,
            Judgement::Refuse(reason) => self.refuse(approver, &request, reason, report).await,
            Judgement::Ask(why) => self.ask_humans(approver, request, why, report).await,
        }
    }

    /// Whether `request`, proven the manager's ([`check`]), may be signed: never when its name is no ant's to take,
    /// at once when it keeps within the policy, else on a human's approval, saying why.
    fn judge(&self, request: &AntRequest) -> Result<Judgement> {
        let (manager, name) = (&request.manager, &request.name);
        let refuse = |why: String| Ok(Judgement::Refuse(why));
        if name.is_human() || *name == *manager {
            return refuse(format!("{name} cannot be an ant added by {manager}"));
        }
        if let Err(e) = auth::check_not_reserved(name) {
            return refuse(format!("{e:#}"));
        }
        let asked: Request = match serde_json::from_str(&request.request) {
            Ok(asked) => asked,
            Err(e) => return refuse(format!("the settings asked for do not read as a template request: {e}")),
        };
        // Before a human is asked: no approval makes room on an uplink the hub would refuse.
        if let Err(e) = self.uplink(request) {
            return refuse(format!("{e:#}"));
        }
        let issued = live(self.dir)?;
        if issued.iter().any(|i| i.name == manager.as_str() && i.template.is_some()) {
            return refuse(format!("{manager} was added from a template, and added ants never add ants"));
        }
        if let Some(other) = issued.iter().find(|i| i.name == name.as_str() && i.key != request.public_key) {
            return refuse(format!("the team holds credentials named {name} already (key {})", other.key));
        }
        let waiting = self.waiting.iter().filter(|w| w.request.manager == *manager).count();
        let templates = match config::signer_templates(self.options.config) {
            Ok(templates) => templates,
            Err(e) => return self.ask_or_refuse(waiting, format!("the signer's templates do not load: {e:#}")),
        };
        match policy(&templates, &issued, request, &asked) {
            Ok(()) => Ok(Judgement::Approve),
            Err(why) => self.ask_or_refuse(waiting, why),
        }
    }

    fn ask_or_refuse(&self, waiting: usize, why: String) -> Result<Judgement> {
        if waiting >= MAX_WAITING {
            return Ok(Judgement::Refuse(format!(
                "{why}, and {waiting} requests of the manager wait for a human already"
            )));
        }
        Ok(Judgement::Ask(why))
    }

    /// Sign the credentials `request` asks for, renew the uplink of the leaf node it came through to carry the ant,
    /// and answer.
    async fn sign(
        &mut self,
        approver: &Approver,
        request: &AntRequest,
        by: ApprovedBy,
        report: &mut impl FnMut(&AntRequest, &Event),
    ) -> Result<()> {
        let signed = self.signed(approver, request).await;
        let (manager, name, public_key) = (request.manager.clone(), request.name.clone(), request.public_key.clone());
        let reply = match signed {
            Ok((jwt, leaf_jwt)) => {
                report(request, &Event::Approved { human: by == ApprovedBy::Human });
                AntReply::Approved { manager, name, public_key, jwt, leaf_jwt }
            }
            Err(e) => {
                let reason = format!("signing failed: {e:#}");
                report(request, &Event::Refused(reason.clone()));
                AntReply::Refused { manager, name, public_key, reason }
            }
        };
        answer(approver, &reply).await?;
        self.answered.insert(key(request), (reply, request.expires_at));
        Ok(())
    }

    /// The uplink JWT of the leaf node `request` came through, renewed to carry the ant too; refused when the hub
    /// would not take it.
    fn uplink(&self, request: &AntRequest) -> Result<Option<String>> {
        let Some(leaf) = &request.leaf else { return Ok(None) };
        let domain = ants_daemon::secrets::creds::Finding::from_jwt(&request.credentials)?.js_domain;
        let name = &request.name;
        let more = Permissions::for_leaf(&leaf.machine, [(name, ants_proto::Role::Ant)])
            .with_uplink_js_domain(domain.as_ref());
        auth::extend_leaf(self.dir, &leaf.jwt, &leaf.machine, more)
            .map(Some)
            .with_context(|| format!("the uplink of the leaf node {} cannot carry {name}", leaf.machine))
    }

    async fn signed(&self, approver: &Approver, request: &AntRequest) -> Result<(String, Option<String>)> {
        // The ant runs where its manager does, so it reaches JetStream the same way.
        let manager = ants_daemon::secrets::creds::Finding::from_jwt(&request.credentials)?;
        let (name, domain) = (&request.name, manager.js_domain.as_ref());
        // First, as it records nothing: an uplink the hub would refuse leaves no ant signed.
        let renewed = self.uplink(request)?;
        let jwt = auth::sign_added(self.dir, name, domain, &request.public_key, &request.template, &request.manager)?;
        let (Some(leaf), Some(renewed)) = (&request.leaf, renewed) else {
            return Ok((jwt, None));
        };
        members::enrolled(self.dir, &leaf.machine, name)
            .with_context(|| format!("keeping {name} among the members of {}", leaf.machine))?;
        outbox::admit_ant(approver.hub(), &leaf.machine, name)
            .await
            .with_context(|| format!("admitting {name} from the outbox of {}", leaf.machine))?;
        Ok((jwt, Some(renewed)))
    }

    /// Retire the ant `request` names, if its manager added it: revoke its live keys, push the account to the hub
    /// when any were, drop it from the leaf node the request came through ([`ordered::drop_from_leaf`]), and answer.
    async fn retire(
        &mut self,
        approver: &Approver,
        request: &AntRequest,
        report: &mut impl FnMut(&AntRequest, &Event),
    ) -> Result<()> {
        let (name, public_key) = (&request.name, Some(request.public_key.as_str()));
        let revoked = match revoke_added(self.dir, name, Some(&request.manager), public_key) {
            Ok(revoked) => revoked,
            Err(e) => return self.refuse(approver, request, format!("{e:#}"), report).await,
        };
        let push_error = if revoked.is_empty() {
            None
        } else {
            crate::credentials::push::push(self.url, self.dir).await.err().map(|e| format!("{e:#}"))
        };
        if push_error.is_none() {
            super::retire::clean_consumers(self.url, approver.js_domain(), self.dir, name).await;
        }
        report(request, &Event::Retired { revoked: revoked.len(), push_error: push_error.clone() });
        let (leaf_jwt, leaf_error) = match &request.leaf {
            Some(leaf) => {
                match ordered::drop_from_leaf(approver, &leaf.jwt, &leaf.machine, name, ants_proto::LeafRole::Ant).await
                {
                    Ok(jwt) => (jwt, None),
                    Err(e) => (None, Some(format!("{e:#}"))),
                }
            }
            None => (None, None),
        };
        let (manager, name, public_key) = (request.manager.clone(), request.name.clone(), request.public_key.clone());
        let revoked = revoked.into_iter().map(|r| r.key).collect();
        let reply = AntReply::Retired { manager, name, public_key, revoked, push_error, leaf_jwt, leaf_error };
        answer(approver, &reply).await?;
        self.answered.insert(key(request), (reply, request.expires_at));
        Ok(())
    }

    async fn refuse(
        &mut self,
        approver: &Approver,
        request: &AntRequest,
        reason: String,
        report: &mut impl FnMut(&AntRequest, &Event),
    ) -> Result<()> {
        report(request, &Event::Refused(reason.clone()));
        let (manager, name, public_key) = (request.manager.clone(), request.name.clone(), request.public_key.clone());
        let reply = AntReply::Refused { manager, name, public_key, reason };
        answer(approver, &reply).await?;
        self.answered.insert(key(request), (reply, request.expires_at));
        Ok(())
    }

    /// Ask the humans about `request` with an approval request of its manager, on the manager's thread, until a
    /// human decides or it expires: [`Options::approval_timeout`] at most, and never past the manager's wait.
    async fn ask_humans(
        &mut self,
        approver: &Approver,
        request: AntRequest,
        why: String,
        report: &mut impl FnMut(&AntRequest, &Event),
    ) -> Result<()> {
        let left = (request.expires_at - Utc::now()).to_std().unwrap_or_default().saturating_sub(ENROLL_REPEAT);
        let timeout = self.options.approval_timeout.min(left);
        if timeout < LEAST_ASKING {
            let reason = format!("{why}, and too little time is left to ask a human");
            return self.refuse(approver, &request, reason, report).await;
        }
        let approval = approval_request(&request, &why, timeout);
        approver
            .hub()
            .publish(approval.subject(), serde_json::to_vec(&approval)?.into())
            .await
            .context("publishing the approval request")?
            .await
            .context("waiting for JetStream to store the approval request")?;
        report(&request, &Event::Asked { id: approval.id, why });
        self.waiting.push(Waiting { request, approval });
        Ok(())
    }

    /// Act on the humans' decisions stored on the requests asked about, and drop answers no machine waits for.
    async fn look(&mut self, approver: &Approver, report: &mut impl FnMut(&AntRequest, &Event)) -> Result<()> {
        let now = Utc::now();
        self.answered.retain(|_, (_, expires_at)| *expires_at > now);
        if self.waiting.is_empty() {
            return Ok(());
        }
        let stream = approver.hub().get_stream_no_info(STREAM).await.context("opening the ANTS stream")?;
        for waiting in std::mem::take(&mut self.waiting) {
            let decision = match stream.direct_get_last_for_subject(waiting.approval.decision_subject()).await {
                Ok(msg) if relayed::stored_relayed(Some(&msg.headers), &msg.payload, relayed::Read::DirectGet) => {
                    tracing::warn!(request = %waiting.approval.id, "ignoring a decision a stream relayed");
                    None
                }
                Ok(msg) => serde_json::from_slice::<ApprovalDecision>(&msg.payload)
                    .ok()
                    .filter(|d| d.request_id == waiting.approval.id && d.ant == waiting.approval.ant),
                Err(e) if e.kind() == DirectGetErrorKind::NotFound => None,
                Err(e) => {
                    tracing::warn!(request = %waiting.approval.id, error = %e, "reading the decision");
                    None
                }
            };
            match decision.map(|d| waiting.approval.decide(d.verdict).verdict) {
                Some(Verdict::AllowOnce | Verdict::AllowAlways) => {
                    self.sign(approver, &waiting.request, ApprovedBy::Human, report).await?;
                }
                Some(Verdict::Reject) => {
                    self.refuse(approver, &waiting.request, "a human rejected the request".to_owned(), report).await?;
                }
                None if waiting.approval.is_expired(now) => {
                    let reason = "no human decided on the request in time".to_owned();
                    self.refuse(approver, &waiting.request, reason, report).await?;
                }
                None => self.waiting.push(waiting),
            }
        }
        Ok(())
    }
}

/// Check that `request` is its manager's: signed by the manager's credentials of the team's account, unexpired and
/// unrevoked, for a well-formed key, still waited for, and if it names a leaf node, signed by the leaf's uplink
/// credentials too.
fn check(approver: &Approver, request: &AntRequest) -> Result<()> {
    auth::check_user_key(&request.public_key)?;
    if request.expires_at <= Utc::now() {
        bail!("the manager stopped waiting at {}", request.expires_at);
    }
    let signed = request.signed();
    let manager = &request.manager;
    let claims = auth::verified(&request.credentials).context("the manager's credentials")?;
    if claims.iss != approver.account {
        bail!("credentials issued by {}, not by the team's account", claims.iss);
    }
    if claims.name != manager.as_str() {
        bail!("credentials of {}, not of {manager}", claims.name);
    }
    if claims.exp.is_some_and(|exp| exp <= auth::now().unwrap_or(u64::MAX)) {
        bail!("expired credentials of {manager}");
    }
    if approver.revoked.contains(&claims.sub) {
        bail!("revoked credentials of {manager}");
    }
    auth::check_signature(&claims.sub, &signed, &request.signature).context("the manager's credentials")?;
    if let Some(leaf) = &request.leaf {
        let uplink = auth::check_leaf(&approver.account, &leaf.jwt, &leaf.machine)
            .with_context(|| format!("the uplink credentials of {}", leaf.machine))?;
        if approver.revoked.contains(&uplink.sub) {
            bail!("revoked uplink credentials of {}", leaf.machine);
        }
        auth::check_signature(&uplink.sub, &signed, &leaf.signature)
            .with_context(|| format!("the uplink credentials of {}", leaf.machine))?;
    }
    Ok(())
}

/// Revoke the live keys of ant `name`, added from a template (by `manager` when given), and write `server.conf`
/// again ([`revoke::revoke`]); returns the keys revoked, none when none is live. Given the `key` of the ant's
/// credentials, only it and the keys issued before it go (those it renewed): keys issued after it, for the ant added
/// again, stay. Fails, revoking nothing, when the team never issued `key` (or with none, any key of `name`) for an ant
/// added so, or holds live keys of `name` issued otherwise.
pub fn revoke_added(
    dir: &Path,
    name: &UserId,
    manager: Option<&UserId>,
    key: Option<&str>,
) -> Result<Vec<revoke::Revoked>> {
    let added = |i: &issued::Issued| {
        i.name == name.as_str() && i.template.is_some() && manager.is_none_or(|m| i.added_by.as_ref() == Some(m))
    };
    let by = manager.map_or_default(|m| format!(" by {m}"));
    let issued = issued::issued(dir)?;
    let before = match key {
        Some(key) => {
            issued.iter().filter(|i| i.key == key && added(i)).map(|i| i.issued_at).max().with_context(|| {
                format!("the team issued no key {key} of {name} as an ant added from a template{by}")
            })?
        }
        None if issued.iter().any(added) => u64::MAX,
        None => bail!("the team issued no credentials of {name} as an ant added from a template{by}"),
    };
    let live: Vec<issued::Issued> =
        live(dir)?.into_iter().filter(|i| i.name == name.as_str() && i.issued_at <= before).collect();
    if let Some(other) = live.iter().find(|i| !added(i)) {
        bail!("the team holds credentials of {name} not issued for an ant added{by} (key {})", other.key);
    }
    if live.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<String> = live.into_iter().map(|i| i.key).collect();
    revoke::revoke(dir, None, &keys)
}

/// The keys the team issued that are neither revoked nor expired.
pub(crate) fn live(dir: &Path) -> Result<Vec<issued::Issued>> {
    let revoked: BTreeSet<String> = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
    let now = auth::now()?;
    let issued = issued::issued(dir)?;
    Ok(issued.into_iter().filter(|i| !revoked.contains(&i.key) && i.expires_at.is_none_or(|exp| exp > now)).collect())
}

/// Whether `request`, asking for `asked`, keeps within the policy of `templates`: its template lists the manager, has
/// room under its `max_ants` among the live keys `issued`, and bounds what is asked; else why not.
fn policy(
    templates: &[AntTemplate],
    issued: &[issued::Issued],
    request: &AntRequest,
    asked: &Request,
) -> Result<(), String> {
    let (manager, name, template) = (&request.manager, &request.name, request.template.as_str());
    let t = templates
        .iter()
        .find(|t| t.name == template)
        .ok_or_else(|| format!("the signer's config has no ant template {template}"))?;
    if !t.managers.contains(manager) {
        return Err(format!("{manager} is not among the managers of template {template} in the signer's config"));
    }
    let added: BTreeSet<&str> = issued
        .iter()
        .filter(|i| i.template.as_deref() == Some(template) && i.name != name.as_str())
        .map(|i| i.name.as_str())
        .collect();
    if added.len() >= t.max_ants {
        return Err(format!("template {template} has {} ants already, its max_ants", added.len()));
    }
    let canonical = |dir: &Path| Ok(dir.canonicalize().unwrap_or_else(|_| dir.to_owned()));
    t.resolve_with(manager, name, asked, canonical).map(drop).map_err(|e| format!("{e:#}"))
}

/// The approval request asking the humans about `request`, outside the policy for `why`, for `timeout`: one of its
/// manager's, on the manager's thread, offering to allow once or reject.
fn approval_request(request: &AntRequest, why: &str, timeout: Duration) -> ApprovalRequest {
    let title = format!("request_ant: add ant {} from template {} ({why})", request.name, request.template);
    let input = serde_json::json!({
        "name": request.name,
        "template": request.template,
        "reason": request.reason,
        "overrides": serde_json::from_str::<serde_json::Value>(&request.request).unwrap_or_default(),
        "host": request.host,
        "leaf": request.leaf.as_ref().map(|l| &l.machine),
    });
    let tool = ToolCall { kind: "other".to_owned(), title, locations: Vec::new(), input: Some(input.to_string()) };
    let thread: ThreadId = request.thread_id.clone();
    ApprovalRequest::new(request.manager.clone(), thread, tool, vec![Verdict::AllowOnce], timeout)
}

async fn answer(approver: &Approver, reply: &AntReply) -> Result<()> {
    approver.client.publish(reply.subject(), serde_json::to_vec(reply)?.into()).await?;
    approver.client.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::path::PathBuf;

    use ants_daemon::bus;
    use ants_proto::Role;
    use nkeys::KeyPair;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::asking::Signed;
    use super::*;
    use crate::credentials::enroll::Leaf;
    use crate::test_support::Server;
    use crate::tools;

    const WAIT: Duration = Duration::from_secs(20);
    const REPEAT: Duration = Duration::from_millis(200);

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    /// A config file on the signer's machine with template `worker` for manager `lead`, of at most `max_ants`.
    fn templates(server: &Server, max_ants: usize) -> PathBuf {
        let src = server.dir().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let path = server.dir().join("config.toml");
        let text = format!(
            "[[ant_template]]\nname = \"worker\"\nmanagers = [\"lead\"]\nmax_ants = {max_ants}\nworkdirs = [{src:?}]\n\
             models = {{ \"gpt-5\" = \"high\" }}\n"
        );
        std::fs::write(&path, text).unwrap();
        path
    }

    /// Run `asking` while `ants signer` answers over `server` with the templates of `config`, and return its output
    /// with what the signer did.
    async fn signing<T>(server: &Server, config: &Path, asking: impl Future<Output = T>) -> (T, Vec<(UserId, Event)>) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let options = Options { config, approval_timeout: WAIT, reconnect: WAIT * 3, tick: REPEAT };
        let report = move |r: &AntRequest, e: &Event| drop(tx.send((r.name.clone(), e.clone())));
        let team = server.dir().join("auth");
        let serving = Box::pin(serve(&server.url, &team, None, &options, report, pending()));
        let out = timeout(WAIT * 2, async {
            tokio::select! {
                r = serving => panic!("the signer stopped: {r:?}"),
                out = asking => out,
            }
        })
        .await
        .unwrap();
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        (out, events)
    }

    struct Manager {
        creds: PathBuf,
        client: async_nats::Client,
    }

    impl Manager {
        async fn new(server: &Server) -> Self {
            let lead = p("lead");
            let creds = server.creds_as(&lead, Role::Ant);
            let (client, _) = bus::connect(&server.url, Some(&creds), None, &lead, "test").await.unwrap();
            Self { creds, client }
        }

        async fn ask(&self, name: &str, request: &str, user: &KeyPair) -> Result<Signed> {
            let until = Utc::now() + WAIT;
            let thread = ThreadId::generate();
            let (jwt, key) = auth::read_creds(&self.creds).unwrap();
            let (template, reason) = ("worker", "more hands");
            let requester = Requester { client: &self.client, jwt: &jwt, key: &key, leaf: None, repeat: REPEAT, until };
            ask(&requester, template, request, reason, &thread, &p(name), user).await
        }
    }

    #[tokio::test]
    async fn a_request_within_the_policy_is_signed_at_once_and_counted() {
        let Some(server) = Server::start() else { return };
        let (team, config) = (server.dir().join("auth"), templates(&server, 1));
        let lead = Manager::new(&server).await;
        let user = KeyPair::new_user();

        let (signed, events) = signing(&server, &config, lead.ask("lead-a", r#"{"model":"gpt-5"}"#, &user)).await;
        let signed = signed.unwrap();
        assert_eq!(events, [(p("lead-a"), Event::Approved { human: false })]);
        assert_eq!(signed.leaf_jwt, None);
        let issued = issued::issued(&team).unwrap();
        let record = issued.iter().find(|i| i.name == "lead-a").unwrap();
        assert_eq!((record.key.as_str(), record.template.as_deref()), (user.public_key().as_str(), Some("worker")));
        assert_eq!(record.added_by, Some(p("lead")));
        let out = server.dir().join("lead-a.creds");
        auth::write_creds(&out, &signed.jwt, &user).unwrap();
        let (client, _) = bus::connect(&server.url, Some(&out), None, &p("lead-a"), "test").await.unwrap();
        client.flush().await.unwrap();

        // The same request again, as from a machine that missed the answer, gets the same credentials.
        let (again, events) = signing(&server, &config, lead.ask("lead-a", r#"{"model":"gpt-5"}"#, &user)).await;
        assert_eq!(auth::verified(&again.unwrap().jwt).unwrap().sub, user.public_key());
        assert_eq!(events, [(p("lead-a"), Event::Approved { human: false })], "signed anew by a new signer");

        let other = KeyPair::new_user();
        let (taken, _) = signing(&server, &config, lead.ask("lead-a", "{}", &other)).await;
        let err = format!("{:#}", taken.unwrap_err());
        assert!(err.contains("the signer refused lead-a") && err.contains("holds credentials named lead-a"), "{err}");

        // An ant added from a template never adds ants, whatever the signer's templates say.
        auth::sign_added(&team, &p("lead"), None, &KeyPair::new_user().public_key(), "worker", &p("boss")).unwrap();
        let (added, _) = signing(&server, &config, lead.ask("lead-b", "{}", &KeyPair::new_user())).await;
        let err = format!("{:#}", added.unwrap_err());
        assert!(err.contains("added ants never add ants"), "{err}");
    }

    #[tokio::test]
    async fn the_signer_records_its_machine_as_holding_the_account_key_for_humans_to_read() {
        let Some(server) = Server::start() else { return };
        let config = server.dir().join("config.toml");
        std::fs::write(&config, "[host]\nmachine = \"keybox\"\n").unwrap();
        let (_, h_js) = server.connect_as(&p("alice"), Role::Human).await;
        h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
        let (recorded, _) = signing(&server, &config, async {
            loop {
                if let Some(holder) = crate::machines::holder(&h_js).await.unwrap() {
                    break holder;
                }
                tokio::time::sleep(REPEAT).await;
            }
        })
        .await;
        assert_eq!(recorded.machine, "keybox".parse::<ants_proto::Machine>().unwrap());
        assert_eq!((recorded.hostname, recorded.version), (colony::machine().await, ANTS_VERSION.to_owned()));
        assert_eq!(crate::machines::list(&h_js).await.unwrap(), [], "the record is no machine's");
        let as_human = DirectKv::new(&h_js, None, MACHINES_BUCKET).await.unwrap();
        assert!(as_human.put(ACCOUNT_HOLDER_KEY, b"{}".to_vec()).await.is_err(), "a human only reads it");
    }

    #[tokio::test]
    async fn the_signer_changes_and_lists_the_reactions_of_ants_to_the_messages_they_read() {
        use ants_proto::{Envelope, ReactionChange, ReactionsOutcome, ReactionsRequest};
        let Some(server) = Server::start() else { return };
        let config = templates(&server, 1);
        let (human, dev_a) = (UserId::human(), p("dev-a"));
        let (_, h_js) = server.connect_as(&human, Role::Human).await;
        bus::ensure_stream(&h_js).await.unwrap();
        h_js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
        let to_a = Envelope::new(human, dev_a.clone(), "for dev-a");
        bus::publish(&h_js, &to_a).await.unwrap();
        let (a_client, _) = server.connect_as(&dev_a, Role::Ant).await;

        let change = ReactionChange::new("eyes".parse().unwrap(), false);
        let (outcomes, _) = signing(&server, &config, async {
            let changed =
                reactions::request(&a_client, &ReactionsRequest::change(dev_a.clone(), to_a.id, change)).await;
            (changed.unwrap(), reactions::ask(&a_client, &dev_a, to_a.id).await.unwrap())
        })
        .await;
        assert_eq!(outcomes.0, Some(ReactionsOutcome::Applied { changed: true }));
        let reacted = ants_proto::Reacted::new("eyes".parse().unwrap(), vec![dev_a]);
        assert_eq!(outcomes.1, Some(ReactionsOutcome::Listed { reactions: vec![reacted] }));
    }

    #[tokio::test]
    async fn outside_the_policy_a_human_approves_or_rejects() {
        let Some(server) = Server::start() else { return };
        let (team, config) = (server.dir().join("auth"), templates(&server, 1));
        let human = p("alice");
        let (_, js) = server.connect_as(&human, Role::Human).await;
        let stream = bus::ensure_stream(&js).await.unwrap();
        let lead = Manager::new(&server).await;
        // The template's one ant, from an earlier request.
        auth::sign_added(&team, &p("lead-x"), None, &KeyPair::new_user().public_key(), "worker", &p("lead")).unwrap();

        let decide = async |verdict| {
            let asked = loop {
                let stored = tools::requests(&stream).await.unwrap();
                let mut pending = Vec::new();
                for (_, request) in stored {
                    if tools::decision(&stream, &request).await.unwrap().is_none() {
                        pending.push(request);
                    }
                }
                if let Some(request) = pending.pop() {
                    break request;
                }
                tokio::time::sleep(REPEAT).await;
            };
            assert_eq!(asked.ant, p("lead"));
            assert!(asked.tool.title.contains("max_ants"), "{}", asked.tool.title);
            tools::answer(&js, &stream, &asked.id.to_string(), verdict, Utc::now()).await.unwrap();
        };

        let user = KeyPair::new_user();
        let asking = async { tokio::join!(lead.ask("lead-a", "{}", &user), decide(Verdict::AllowOnce)).0 };
        let (signed, events) = signing(&server, &config, Box::pin(asking)).await;
        assert_eq!(auth::verified(&signed.unwrap().jwt).unwrap().sub, user.public_key());
        assert!(matches!(&events[..], [(_, Event::Asked { .. }), (_, Event::Approved { human: true })]), "{events:?}");

        let other = KeyPair::new_user();
        let asking = async { tokio::join!(lead.ask("lead-b", "{}", &other), decide(Verdict::Reject)).0 };
        let (rejected, events) = signing(&server, &config, Box::pin(asking)).await;
        let err = format!("{:#}", rejected.unwrap_err());
        assert!(err.contains("a human rejected the request"), "{err}");
        assert!(matches!(&events[..], [(_, Event::Asked { .. }), (_, Event::Refused(_))]), "{events:?}");
        assert!(!issued::issued(&team).unwrap().iter().any(|i| i.name == "lead-b"), "a rejected ant is not signed");
    }

    /// Behind a leaf node whose uplink the hub would refuse with one more ant, a request is refused at once, before
    /// any human is asked, with the reason; nothing is signed or kept for the ant.
    #[tokio::test]
    async fn an_ant_the_uplink_has_no_room_for_is_refused_before_a_human_is_asked() {
        let Some(server) = Server::start() else { return };
        let (team, config) = (server.dir().join("auth"), templates(&server, 1));
        // Outside the policy: the template's one ant is there already, so only a human could approve.
        auth::sign_added(&team, &p("lead-x"), None, &KeyPair::new_user().public_key(), "worker", &p("lead")).unwrap();
        let m1: ants_proto::Machine = "m1".parse().unwrap();
        let uplink = server.dir().join("leaf-m1.creds");
        crate::test_support::uplink_for_most_ants(&team, &m1, None, &uplink);
        let before = std::fs::read_to_string(&uplink).unwrap();
        let lead = Manager::new(&server).await;
        let leaf = Leaf { machine: m1, creds: uplink.clone() };

        let asking = async {
            let (jwt, key) = auth::read_creds(&lead.creds).unwrap();
            let (thread, until) = (ThreadId::generate(), Utc::now() + WAIT);
            let user = KeyPair::new_user();
            let leaf = Some(&leaf);
            let requester = Requester { client: &lead.client, jwt: &jwt, key: &key, leaf, repeat: REPEAT, until };
            ask(&requester, "worker", "{}", "more hands", &thread, &p("lead-a"), &user).await
        };
        let (refused, events) = signing(&server, &config, asking).await;
        let err = format!("{:#}", refused.unwrap_err());
        assert!(err.contains("the signer refused lead-a: the uplink of the leaf node m1 cannot carry lead-a"), "{err}");
        assert!(err.contains("would take") && err.contains("another leaf node"), "{err}");
        assert!(matches!(&events[..], [(_, Event::Refused(_))]), "no human asked: {events:?}");
        assert!(!issued::issued(&team).unwrap().iter().any(|i| i.name == "lead-a"), "nothing signed");
        assert_eq!(std::fs::read_to_string(&uplink).unwrap(), before);
    }
}
