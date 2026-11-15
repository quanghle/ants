//! Ants added by manager ants, signed on the machine holding the account key: a manager asks for an ant from a
//! template over NATS ([`ask`], behind its `request_ant` tool), and `ants signer` there ([`serve`]) signs its
//! credentials when the request keeps within the signer's own copy of the template's policy, asks the humans through
//! the approvals path otherwise, and refuses what it can never sign. A manager retires an ant it added the same way
//! ([`retire`], behind its `retire_ant` tool): the signer revokes the ant's keys ([`revoke_added`]) and pushes the
//! account to the hub, so the name and its place under the template's `max_ants` are free again.
//!
//! Requests are core NATS messages and stored nowhere, as enrollment requests are ([`crate::enroll`]): the manager's
//! machine repeats its request every [`ENROLL_REPEAT`] until an answer comes or it stops waiting
//! ([`AntRequest::expires_at`]), and the signer answers each repeat of a request it decided the same way.
//!
//! The signer answers ants' reactions requests as well ([`reactions::serve`]): an ant's credentials neither read nor
//! write the reactions bucket, so the signer lists the reactions to a message for the ant, and adds or removes the
//! ant's own, once it found the ant reads the message.
//!
//! The signer trusts only what it can check itself: the request is signed by the manager's credentials of the team's
//! account, and its template is read from the signer's own config file ([`config::signer_templates`]), never taken
//! from the manager's machine. How many ants a template has is counted from the keys the team issued
//! ([`revoke::Issued::template`]), live ones only.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::time::Duration;

use ants_daemon::config;
use ants_daemon::reactions;
use ants_daemon::roster;
use ants_daemon::template::{AntTemplate, Request};
use ants_proto::{
    AntReply, AntRequest, ApprovalDecision, ApprovalRequest, ENROLL_REPEAT, EnrollLeaf, JsDomain, Permissions, STREAM,
    ThreadId, ToolCall, UserId, Verdict, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::stream::DirectGetErrorKind;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::enroll::{Approver, Leaf};
use crate::registrar::{self, Registrar};
use crate::{auth, members, outbox, revoke};

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
    /// How long it stays connected with one set of the credentials it issues itself ([`crate::renew::AUTO_RECONNECT`]).
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
/// account key in the team directory `dir`, and ants' reactions requests ([`reactions::serve`]), until `stop`
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
    let mut registrar = Registrar::new(dir, options.config, js_domain, options.approval_timeout, registrar::print);
    tokio::pin!(stop);
    loop {
        let approver = Approver::connect_signer(url, dir, js_domain, crate::enroll::LISTEN).await?;
        let mut requests = approver.client.subscribe(subject::ALL_ANT_REQUESTS).await?;
        let mut logins = approver.client.subscribe(subject::ALL_LOGIN_REQUESTS).await?;
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
                update = registrar.next() => registrar.progressed(&approver, update).await?,
                _ = ticks.tick() => {
                    registrar.expire();
                    registrar.look(&approver).await?;
                    signer.look(&approver, &mut report).await?;
                }
                () = &mut until => break,
                () = &mut stop => return Ok(()),
            }
        }
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
        let domain = ants_daemon::creds::Finding::from_jwt(&request.credentials)?.js_domain;
        let name = &request.name;
        let more =
            Permissions::for_leaf(&leaf.machine, [(name, ants_proto::Role::Ant)]).with_js_domain(domain.as_ref());
        auth::extend_leaf(self.dir, &leaf.jwt, &leaf.machine, more)
            .map(Some)
            .with_context(|| format!("the uplink of the leaf node {} cannot carry {name}", leaf.machine))
    }

    async fn signed(&self, approver: &Approver, request: &AntRequest) -> Result<(String, Option<String>)> {
        // The ant runs where its manager does, so it reaches JetStream the same way.
        let manager = ants_daemon::creds::Finding::from_jwt(&request.credentials)?;
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
    /// when any were, and answer.
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
            crate::revoke::push(self.url, self.dir).await.err().map(|e| format!("{e:#}"))
        };
        report(request, &Event::Retired { revoked: revoked.len(), push_error: push_error.clone() });
        let (manager, name, public_key) = (request.manager.clone(), request.name.clone(), request.public_key.clone());
        let revoked = revoked.into_iter().map(|r| r.key).collect();
        let reply = AntReply::Retired { manager, name, public_key, revoked, push_error };
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
    let added = |i: &revoke::Issued| {
        i.name == name.as_str() && i.template.is_some() && manager.is_none_or(|m| i.added_by.as_ref() == Some(m))
    };
    let by = manager.map_or_default(|m| format!(" by {m}"));
    let issued = revoke::issued(dir)?;
    let before = match key {
        Some(key) => {
            issued.iter().filter(|i| i.key == key && added(i)).map(|i| i.issued_at).max().with_context(|| {
                format!("the team issued no key {key} of {name} as an ant added from a template{by}")
            })?
        }
        None if issued.iter().any(added) => u64::MAX,
        None => bail!("the team issued no credentials of {name} as an ant added from a template{by}"),
    };
    let live: Vec<revoke::Issued> =
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
fn live(dir: &Path) -> Result<Vec<revoke::Issued>> {
    let revoked: BTreeSet<String> = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
    let now = auth::now()?;
    let issued = revoke::issued(dir)?;
    Ok(issued.into_iter().filter(|i| !revoked.contains(&i.key) && i.expires_at.is_none_or(|exp| exp > now)).collect())
}

/// Whether `request`, asking for `asked`, keeps within the policy of `templates`: its template lists the manager, has
/// room under its `max_ants` among the live keys `issued`, and bounds what is asked; else why not.
fn policy(
    templates: &[AntTemplate],
    issued: &[revoke::Issued],
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

/// What [`ask`] got: the ant's user JWT, and for a request through a leaf node, the renewed uplink JWT of the leaf.
#[derive(Debug)]
pub struct Signed {
    pub jwt: String,
    pub leaf_jwt: Option<String>,
}

/// Ask, over the manager's connection `client`, as the manager its credentials' user JWT `jwt` and key `key` name, for
/// ant `name` from `template` with the settings `request` (JSON), for the public key of `user`, through the machine's
/// `leaf` node if it has one:
/// repeat the request every `repeat` until an answer for the key arrives or `until` passes. Returns the ant's user
/// JWT of an approval, checked to be the account's for that key and ant, and the leaf's renewed uplink JWT, checked
/// likewise; a refusal is an error carrying the signer's reason.
#[allow(clippy::too_many_arguments)]
pub async fn ask(
    client: &async_nats::Client,
    jwt: &str,
    key: &KeyPair,
    template: &str,
    request: &str,
    reason: &str,
    thread: &ThreadId,
    name: &UserId,
    user: &KeyPair,
    leaf: Option<&Leaf>,
    repeat: Duration,
    until: DateTime<Utc>,
) -> Result<Signed> {
    let claims = auth::verified(jwt).context("the manager's credentials")?;
    let manager: UserId = claims.name.parse().with_context(|| format!("credentials named {}", claims.name))?;
    let mut replies = client.subscribe(subject::antreq_reply(&manager)).await?;
    let public_key = user.public_key();
    let host = roster::machine().await;
    let mut asking = AntRequest::new(
        manager.clone(),
        name.clone(),
        template,
        request,
        reason,
        thread.clone(),
        &public_key,
        host,
        until,
    );
    asking.credentials = jwt.to_owned();
    let mut uplink: Option<KeyPair> = None;
    let mut repeats = tokio::time::interval(repeat);
    let left = (until - Utc::now()).to_std().unwrap_or_default();
    let deadline = tokio::time::sleep(left);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                if let Some(leaf) = leaf {
                    let (jwt, key) = auth::read_creds(&leaf.creds).context("the leaf node's uplink credentials")?;
                    asking.leaf = Some(EnrollLeaf { machine: leaf.machine.clone(), jwt, signature: String::new() });
                    let signature = auth::sign(&key, &asking.signed())?;
                    asking.leaf.as_mut().expect("the leaf just set").signature = signature;
                    uplink = Some(key);
                }
                asking.signature = auth::sign(key, &asking.signed())?;
                client.publish(asking.subject(), serde_json::to_vec(&asking)?.into()).await?;
                client.flush().await?;
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let reply = match serde_json::from_slice::<AntReply>(&msg.payload) {
                    Ok(reply) if reply.manager() == &manager && reply.answers(name, &public_key) => reply,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "ignoring a malformed answer to an ant request");
                        continue;
                    }
                };
                match reply {
                    AntReply::Approved { jwt, leaf_jwt, .. } => {
                        match check_signed(&claims.iss, &jwt, name, &public_key, leaf_jwt.as_deref(), leaf, uplink.as_ref()) {
                            Ok(()) => return Ok(Signed { jwt, leaf_jwt }),
                            Err(e) => tracing::warn!(error = format!("{e:#}"), "ignoring an approval of an ant request"),
                        }
                    }
                    AntReply::Refused { reason, .. } => bail!("the signer refused {name}: {reason}"),
                    AntReply::Retired { .. } => continue,
                }
            }
            () = &mut deadline => bail!("no answer from the signer (`ants signer` on the machine holding the team's account key) in time"),
        }
    }
}

/// What [`retire`] got: the keys the signer revoked, and the error of pushing them to the hub, if it failed.
#[derive(Debug)]
pub struct Retired {
    pub revoked: Vec<String>,
    pub push_error: Option<String>,
}

/// Ask, over the manager's connection `client`, as the manager its credentials' user JWT `jwt` and key `key` name, to
/// retire ant `name`, added from `template`, whose credentials on this machine have `public_key`: repeat the request
/// every `repeat` until an answer arrives or `until` passes. A refusal is an error carrying the signer's reason.
#[allow(clippy::too_many_arguments)]
pub async fn retire(
    client: &async_nats::Client,
    jwt: &str,
    key: &KeyPair,
    template: &str,
    thread: &ThreadId,
    name: &UserId,
    public_key: &str,
    repeat: Duration,
    until: DateTime<Utc>,
) -> Result<Retired> {
    let claims = auth::verified(jwt).context("the manager's credentials")?;
    let manager: UserId = claims.name.parse().with_context(|| format!("credentials named {}", claims.name))?;
    let mut replies = client.subscribe(subject::antreq_reply(&manager)).await?;
    let host = roster::machine().await;
    let mut asking =
        AntRequest::new(manager.clone(), name.clone(), template, "{}", "", thread.clone(), public_key, host, until);
    asking.retire = true;
    asking.credentials = jwt.to_owned();
    asking.signature = auth::sign(key, &asking.signed())?;
    let mut repeats = tokio::time::interval(repeat);
    let left = (until - Utc::now()).to_std().unwrap_or_default();
    let deadline = tokio::time::sleep(left);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = repeats.tick() => {
                client.publish(asking.subject(), serde_json::to_vec(&asking)?.into()).await?;
                client.flush().await?;
            }
            msg = replies.next() => {
                let msg = msg.context("the NATS connection closed")?;
                let reply = match serde_json::from_slice::<AntReply>(&msg.payload) {
                    Ok(reply) if reply.manager() == &manager && reply.answers(name, public_key) => reply,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, "ignoring a malformed answer to an ant request");
                        continue;
                    }
                };
                match reply {
                    AntReply::Retired { revoked, push_error, .. } => return Ok(Retired { revoked, push_error }),
                    AntReply::Refused { reason, .. } => bail!("the signer refused to retire {name}: {reason}"),
                    AntReply::Approved { .. } => continue,
                }
            }
            () = &mut deadline => bail!("no answer from the signer (`ants signer` on the machine holding the team's account key) in time"),
        }
    }
}

/// Check that `jwt` is the user JWT of ant `name` for `public_key`, signed by `account`, and that `leaf_jwt` is there
/// only when the request named `leaf`, and is then the account's for the uplink key `uplink` and the leaf's machine.
fn check_signed(
    account: &str,
    jwt: &str,
    name: &UserId,
    public_key: &str,
    leaf_jwt: Option<&str>,
    leaf: Option<&Leaf>,
    uplink: Option<&KeyPair>,
) -> Result<()> {
    let claims = auth::verified(jwt)?;
    if claims.iss != account {
        bail!("signed by {}, not by the account {account}", claims.iss);
    }
    if claims.sub != public_key || claims.name != name.as_str() {
        bail!("issued to {} ({}), not to {name} ({public_key})", claims.name, claims.sub);
    }
    match (leaf_jwt, leaf, uplink) {
        (None, None, _) => Ok(()),
        (None, Some(leaf), _) => bail!("no renewed uplink credentials for the leaf node {}", leaf.machine),
        (Some(jwt), Some(leaf), Some(uplink)) => {
            let claims = auth::check_leaf(account, jwt, &leaf.machine).context("the renewed uplink credentials")?;
            if claims.sub != uplink.public_key() {
                bail!("the renewed uplink credentials are for the key {}, not for this leaf node's", claims.sub);
            }
            Ok(())
        }
        (Some(_), _, _) => bail!("renews uplink credentials no leaf node asked for"),
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::path::PathBuf;

    use ants_daemon::bus;
    use ants_proto::Role;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::*;
    use crate::auth::tests::Server;
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
            ask(&self.client, &jwt, &key, template, request, reason, &thread, &p(name), user, None, REPEAT, until).await
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
        let issued = revoke::issued(&team).unwrap();
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

        let change = ReactionChange { reaction: "eyes".parse().unwrap(), remove: false };
        let (outcomes, _) = signing(&server, &config, async {
            let changed =
                reactions::request(&a_client, &ReactionsRequest::change(dev_a.clone(), to_a.id, change)).await;
            (changed.unwrap(), reactions::ask(&a_client, &dev_a, to_a.id).await.unwrap())
        })
        .await;
        assert_eq!(outcomes.0, Some(ReactionsOutcome::Applied { changed: true }));
        let reacted = ants_proto::Reacted { reaction: "eyes".parse().unwrap(), by: vec![dev_a] };
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
        assert!(!revoke::issued(&team).unwrap().iter().any(|i| i.name == "lead-b"), "a rejected ant is not signed");
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
        auth::tests::uplink_for_most_ants(&team, &m1, None, &uplink);
        let before = std::fs::read_to_string(&uplink).unwrap();
        let lead = Manager::new(&server).await;
        let leaf = Leaf { machine: m1, creds: uplink.clone() };

        let asking = async {
            let (jwt, key) = auth::read_creds(&lead.creds).unwrap();
            let (thread, until) = (ThreadId::generate(), Utc::now() + WAIT);
            let user = KeyPair::new_user();
            let leaf = Some(&leaf);
            ask(
                &lead.client,
                &jwt,
                &key,
                "worker",
                "{}",
                "more hands",
                &thread,
                &p("lead-a"),
                &user,
                leaf,
                REPEAT,
                until,
            )
            .await
        };
        let (refused, events) = signing(&server, &config, asking).await;
        let err = format!("{:#}", refused.unwrap_err());
        assert!(err.contains("the signer refused lead-a: the uplink of the leaf node m1 cannot carry lead-a"), "{err}");
        assert!(err.contains("would take") && err.contains("another leaf node"), "{err}");
        assert!(matches!(&events[..], [(_, Event::Refused(_))]), "no human asked: {events:?}");
        assert!(!revoke::issued(&team).unwrap().iter().any(|i| i.name == "lead-a"), "nothing signed");
        assert_eq!(std::fs::read_to_string(&uplink).unwrap(), before);
    }
}
