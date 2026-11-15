//! Enrollment requests carrying an admin's order ([`EnrollRequest::order`]): the daemon of the order's machine, carrying
//! out an order to add an ant on it ([`AdminOp::Add`]) or to retire one ([`AdminOp::Retire`]), asks with its host
//! credentials, and the signer answers on the admin's word, with no human asked again.
//!
//! The signer trusts neither the machine nor the order without checking both: the request must prove the machine's
//! live host credentials of the team's account (a request failing that gets no answer, as an enrollment request
//! failing its proof does), and the order must verify as an admin's ([`AdminOrder::verify`]) at the time the signer
//! hears it, for that machine and the ant asked about. An order adds one key only: another key asking on the same
//! order is refused, and a retirement revokes only keys of an ant that was there when the admin sent the order.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use ants_daemon::creds::{Finding, Kind as CredsKind};
use ants_proto::{AdminOp, AdminOrder, EnrollReply, EnrollRequest, Machine, ORDER_CLOCK_SKEW, Uuid, host_user};
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, TimeDelta, Utc};

use super::live;
use crate::auth;
use crate::enroll::Approver;
use crate::revoke::{self, Kind};

/// How long the signer answers a repeat of a request the same way, without deciding again.
const REMEMBER: TimeDelta = TimeDelta::minutes(10);

/// The requests carrying orders that the signer answered.
#[derive(Default)]
pub(super) struct Ordered {
    answered: HashMap<(Uuid, String), (EnrollReply, DateTime<Utc>)>,
}

impl Ordered {
    /// Answer `msg` if it is an enrollment request carrying an order its machine proves; ignore it otherwise.
    pub(super) async fn heard(&mut self, approver: &Approver, url: &str, msg: &async_nats::Message) -> Result<()> {
        let Ok(request) = serde_json::from_slice::<EnrollRequest>(&msg.payload) else { return Ok(()) };
        let Some(order) = request.order.as_deref() else { return Ok(()) };
        let now = Utc::now();
        self.answered.retain(|_, (_, at)| *at + REMEMBER > now);
        let proven = request
            .check_subject(&msg.subject)
            .map_err(anyhow::Error::from)
            .and_then(|()| host(approver, &request, now));
        let machine = match proven {
            Ok(machine) => machine,
            Err(e) => {
                tracing::warn!(ant = %request.ant, order = %order.id, "ignoring an ordered enrollment request: {e:#}");
                return Ok(());
            }
        };
        let id = (order.id, request.public_key.clone());
        if let Some((reply, _)) = self.answered.get(&id) {
            return answer(approver, reply).await;
        }
        let reply = match decide(approver, url, &request, order, &machine, now).await {
            Ok(reply) => reply,
            Err(e) => EnrollReply::Rejected {
                ant: request.ant.clone(),
                public_key: Some(request.public_key.clone()),
                reason: Some(format!("{e:#}")),
            },
        };
        print(order, &reply);
        answer(approver, &reply).await?;
        self.answered.insert(id, (reply, now));
        Ok(())
    }
}

/// The machine whose live host credentials of the team's account `request` proves, sent no further from `now` than
/// [`ORDER_CLOCK_SKEW`].
fn host(approver: &Approver, request: &EnrollRequest, now: DateTime<Utc>) -> Result<Machine> {
    auth::check_user_key(&request.public_key)?;
    let claims = auth::verified(&request.enrollment).context("the host credentials")?;
    if claims.iss != approver.account {
        bail!("credentials issued by {}, not by the team's account", claims.iss);
    }
    let CredsKind::Host { machine } = Finding::from_jwt(&request.enrollment)?.kind else {
        bail!("credentials of {}, not a machine's host credentials", claims.name);
    };
    if claims.exp.is_some_and(|exp| exp <= auth::now().unwrap_or(u64::MAX)) {
        bail!("expired host credentials of {machine}");
    }
    let revoked: BTreeSet<String> = revoke::revoked(&approver.dir)?.into_iter().map(|r| r.key).collect();
    if revoked.contains(&claims.sub) || approver.revoked.contains(&claims.sub) {
        bail!("revoked host credentials of {machine}");
    }
    let name = host_user(&machine);
    if !revoke::issued(&approver.dir)?.iter().any(|i| i.key == claims.sub && i.kind == Kind::Host && i.name == name) {
        bail!("host credentials of {machine} the team never issued");
    }
    auth::check_signature(&claims.sub, &request.signed(), &request.signature).context("the host credentials")?;
    if (request.sent_at - now).abs() > ORDER_CLOCK_SKEW {
        bail!("sent at {}, too far from now", request.sent_at);
    }
    Ok(machine)
}

/// What to answer `request`, which `machine` proved, carrying `order`: refused, with the reason, unless the order is an
/// admin's to add or retire the ant asked about on that machine.
async fn decide(
    approver: &Approver,
    url: &str,
    request: &EnrollRequest,
    order: &AdminOrder,
    machine: &Machine,
    now: DateTime<Utc>,
) -> Result<EnrollReply> {
    if request.leaf.is_some() {
        bail!("an order travels with no leaf node's uplink");
    }
    let dir = &approver.dir;
    let revoked: BTreeSet<String> = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
    order
        .verify(&order.subject(), [approver.account.as_str()], |key| revoked.contains(key), now)
        .context("the order")?;
    if order.machine != *machine {
        bail!("the order is for {}, not {machine}", order.machine);
    }
    if order.ant != request.ant {
        bail!("the order is about {}, not {}", order.ant, request.ant);
    }
    let leaf = auth::leaf_user(machine);
    if live(dir)?.iter().any(|i| i.kind == Kind::Leaf && i.name == leaf) {
        bail!("{machine} is a leaf node's machine: adding or retiring an ant there by order is not supported yet");
    }
    match &order.op {
        AdminOp::Add { .. } => add(dir, request, order, &revoked),
        AdminOp::Retire => retire(dir, url, request, order).await,
        op => bail!("an order to {} asks nothing of the signer", op.describe()),
    }
}

/// Sign the key `request` asks for, of the ant the order adds: never a key revoked or issued to other credentials than
/// the ant's on an order, nor another key on the same order, nor while the team holds live credentials of the name
/// for another key.
fn add(dir: &Path, request: &EnrollRequest, order: &AdminOrder, revoked: &BTreeSet<String>) -> Result<EnrollReply> {
    let (ant, key) = (&request.ant, request.public_key.as_str());
    if ant.is_human() {
        bail!("{ant} is a human's name, not an ant's");
    }
    auth::check_not_reserved(ant)?;
    if revoked.contains(key) {
        bail!("the key {key} is revoked");
    }
    let issued = revoke::issued(dir)?;
    if let Some(other) = issued.iter().find(|i| i.order == Some(order.id) && i.key != key) {
        bail!("the order added {ant} with the key {} already", other.key);
    }
    // A key an earlier order added the ant with may ask again, as a machine that missed the answer does.
    if let Some(other) = issued.iter().find(|i| i.key == key && (i.order.is_none() || i.name != ant.as_str())) {
        bail!("the key {key} was issued to {} already", other.name);
    }
    if let Some(other) = live(dir)?.iter().find(|i| i.name == ant.as_str() && i.key != key) {
        bail!("the team holds credentials named {ant} already (key {})", other.key);
    }
    let domain = Finding::from_jwt(&request.enrollment)?.js_domain;
    let jwt = auth::sign_ordered(dir, ant, domain.as_ref(), key, order.id)?;
    Ok(EnrollReply::Approved { ant: ant.clone(), public_key: key.to_owned(), jwt, leaf_jwt: None })
}

/// Revoke the ant's key `request` names and the live keys of the name issued before it, which it renewed, pushing the
/// account to the hub when any was live: only an ant's or an assistant's key, whose first key (before renewals) was
/// issued no later than the admin sent the order, so that the order retires no ant added again since.
async fn retire(dir: &Path, url: &str, request: &EnrollRequest, order: &AdminOrder) -> Result<EnrollReply> {
    let (ant, key) = (&request.ant, request.public_key.as_str());
    let issued = revoke::issued(dir)?;
    let of_ant = |key: &str| {
        issued
            .iter()
            .filter(|i| i.key == key && i.name == ant.as_str() && matches!(i.kind, Kind::Ant | Kind::Assistant))
            .min_by_key(|i| i.issued_at)
    };
    let asked = of_ant(key).with_context(|| format!("the team issued no key {key} of the ant {ant}"))?;
    let mut first = asked;
    for _ in 0..issued.len() {
        match first.renewed_from.as_deref().and_then(of_ant) {
            Some(earlier) if earlier.issued_at <= first.issued_at => first = earlier,
            _ => break,
        }
    }
    let sent = u64::try_from((order.sent_at + ORDER_CLOCK_SKEW).timestamp()).unwrap_or_default();
    if first.issued_at > sent {
        bail!("{ant} was added again (key {}) after the order was sent", first.key);
    }
    let keys: Vec<String> = live(dir)?
        .into_iter()
        .filter(|i| i.name == ant.as_str() && i.issued_at <= asked.issued_at)
        .map(|i| i.key)
        .collect();
    let revoked = if keys.is_empty() { Vec::new() } else { revoke::revoke(dir, None, &keys)? };
    let push_error =
        if revoked.is_empty() { None } else { revoke::push(url, dir).await.err().map(|e| format!("{e:#}")) };
    let revoked = revoked.into_iter().map(|r| r.key).collect();
    Ok(EnrollReply::Retired { ant: ant.clone(), public_key: key.to_owned(), revoked, push_error })
}

async fn answer(approver: &Approver, reply: &EnrollReply) -> Result<()> {
    approver.client.publish(reply.subject(), serde_json::to_vec(reply)?.into()).await?;
    approver.client.flush().await?;
    Ok(())
}

fn print(order: &AdminOrder, reply: &EnrollReply) {
    let asked = format!("{} on {} by {}'s order {}", order.ant, order.machine, order.admin, order.id);
    match reply {
        EnrollReply::Approved { .. } => println!("signed {asked}"),
        EnrollReply::Rejected { reason, .. } => {
            println!("refused {asked}: {}", reason.as_deref().unwrap_or("no reason"));
        }
        EnrollReply::Retired { revoked, push_error: None, .. } => {
            println!("retired {asked}: {} keys revoked", revoked.len());
        }
        EnrollReply::Retired { revoked, push_error: Some(e), .. } => println!(
            "retired {asked}: {} keys revoked, but pushing the account failed: {e}; run `ants auth push`",
            revoked.len()
        ),
    }
}

#[cfg(test)]
mod tests;
