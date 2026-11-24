//! Enrollment requests carrying an admin's order ([`EnrollRequest::order`]): the daemon of the order's machine, carrying
//! out an order to add an ant on it ([`AdminOp::Add`]), to clone one under a new name ([`AdminOp::Clone`]) or to
//! retire one ([`AdminOp::Retire`]), asks with its host credentials, and the signer answers on the admin's word, with
//! no human asked again.
//!
//! The signer trusts neither the machine nor the order without checking both: the request must prove the machine's
//! live host credentials of the bus's account (a request failing that gets no answer, as an enrollment request
//! failing its proof does), and the order must verify as an admin's ([`AdminOrder::verify`]) at the time the signer
//! hears it, for that machine and the ant asked about: the ant an order adds or retires, or the clone a clone order
//! names, never the ant it clones. An order adds one key only: another key asking on the same order is refused, and a
//! retirement revokes only keys of an ant that was there when the admin sent the order. An order never grants host
//! credentials ([`EnrollRequest::host_creds`]): only a human approving does. An add order naming a human the ant is to
//! assist ([`AdminOp::Add`]'s `assistant_of`) has the key signed as that human's assistant only when the human sent
//! it, and as `ants auth user ANT --assistant-of HUMAN` would.
//!
//! On a machine behind a leaf node, the request of an add or clone order also carries the leaf's live uplink
//! credentials, which sign it too: the signer renews them to carry the ant, keeps the ant among the leaf's members and
//! admits it from the machine's outbox, as for an ant enrolling through the leaf. The request of a retire order there
//! carries them too: once the ant's keys are revoked, the signer renews them without the ant, drops it from the leaf's
//! members and from the machine's outbox sourcing, the retirement standing even when that fails.

use std::collections::{BTreeSet, HashMap};

use ants_daemon::secrets::creds::{Finding, Kind as CredsKind};
use ants_proto::{
    AdminOp, AdminOrder, EnrollLeaf, EnrollReply, EnrollRequest, LeafRole, Machine, ORDER_CLOCK_SKEW, Permissions,
    UserId, Uuid, host_user,
};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, TimeDelta, Utc};

use super::live;
use crate::auth;
use crate::credentials::approver::Approver;
use crate::credentials::issued::{self, Kind};
use crate::credentials::push;
use crate::credentials::revoke;
use crate::node::{members, outbox};

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

/// The machine whose live host credentials of the bus's account `request` proves, sent no further from `now` than
/// [`ORDER_CLOCK_SKEW`].
fn host(approver: &Approver, request: &EnrollRequest, now: DateTime<Utc>) -> Result<Machine> {
    auth::check_user_key(&request.public_key)?;
    let claims = auth::verified(&request.enrollment).context("the host credentials")?;
    if claims.iss != approver.account {
        bail!("credentials issued by {}, not by the bus's account", claims.iss);
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
    if !issued::issued(&approver.dir)?.iter().any(|i| i.key == claims.sub && i.kind == Kind::Host && i.name == name) {
        bail!("host credentials of {machine} the bus never issued");
    }
    auth::check_signature(&claims.sub, &request.signed(), &request.signature).context("the host credentials")?;
    if (request.sent_at - now).abs() > ORDER_CLOCK_SKEW {
        bail!("sent at {}, too far from now", request.sent_at);
    }
    Ok(machine)
}

/// What to answer `request`, which `machine` proved, carrying `order`: refused, with the reason, unless the order is an
/// admin's to add, clone as or retire the ant asked about on that machine.
async fn decide(
    approver: &Approver,
    url: &str,
    request: &EnrollRequest,
    order: &AdminOrder,
    machine: &Machine,
    now: DateTime<Utc>,
) -> Result<EnrollReply> {
    if request.host_creds.is_some() {
        bail!("an order grants no host credentials: the machine asking holds its own");
    }
    let dir = &approver.dir;
    let revoked: BTreeSet<String> = revoke::revoked(dir)?.into_iter().map(|r| r.key).collect();
    order
        .verify(&order.subject(), [approver.account.as_str()], |key| revoked.contains(key), now)
        .context("the order")?;
    if order.machine != *machine {
        bail!("the order is for {}, not {machine}", order.machine);
    }
    let about = match &order.op {
        AdminOp::Clone { new, .. } => new,
        _ => &order.ant,
    };
    if *about != request.ant {
        bail!("the order is about {about}, not {}", request.ant);
    }
    // A retirement stands on the host's proof alone: uplink credentials failing their checks are only left as they are.
    if let Some(leaf) = &request.leaf
        && !matches!(order.op, AdminOp::Retire)
    {
        check_leaf(approver, request, leaf, machine, &revoked)?;
    }
    match &order.op {
        AdminOp::Add { assistant_of: Some(human), .. } => {
            if order.admin != *human {
                bail!("only {human} adds an assistant of {human}: the order is {}'s", order.admin);
            }
            auth::check_assistant(dir, &request.ant, human)?;
            add(approver, request, order, machine, &revoked, Some(human)).await
        }
        AdminOp::Add { .. } | AdminOp::Clone { .. } => add(approver, request, order, machine, &revoked, None).await,
        AdminOp::Retire => retire(approver, url, request, order, machine, &revoked).await,
        op => bail!("an order to {} asks nothing of the signer", op.describe()),
    }
}

/// Check that `leaf`, the uplink credentials `request` carries, are the live ones of the leaf node of `machine`, whose
/// host asks, and that their key signed the request too.
fn check_leaf(
    approver: &Approver,
    request: &EnrollRequest,
    leaf: &EnrollLeaf,
    machine: &Machine,
    revoked: &BTreeSet<String>,
) -> Result<()> {
    if leaf.machine != *machine {
        bail!("the request carries the uplink credentials of {}, not of {machine}", leaf.machine);
    }
    let uplink = auth::check_leaf(&approver.account, &leaf.jwt, machine)
        .with_context(|| format!("the uplink credentials of {machine}"))?;
    if revoked.contains(&uplink.sub) || approver.revoked.contains(&uplink.sub) {
        bail!("revoked uplink credentials of {machine}");
    }
    auth::check_signature(&uplink.sub, &request.signed(), &leaf.signature)
        .with_context(|| format!("the uplink credentials of {machine}"))
}

/// Sign the key `request` asks for, of the ant the order adds on `machine` or the clone it makes there, under no name a
/// team holds ([`crate::teams::check_no_team`]): never a key revoked or issued to other
/// credentials than the ant's on an order, nor another key on the same order, nor while the team holds live
/// credentials of the name for another key. Behind a leaf node, also renew its uplink credentials the request carries
/// to carry the ant, first, so that an uplink the hub would refuse leaves no ant signed; keep the ant among the leaf's
/// members and admit it from the machine's outbox. With `assistant_of`, the key is the assistant's of that human.
async fn add(
    approver: &Approver,
    request: &EnrollRequest,
    order: &AdminOrder,
    machine: &Machine,
    revoked: &BTreeSet<String>,
    assistant_of: Option<&UserId>,
) -> Result<EnrollReply> {
    let dir = approver.dir.as_path();
    let (ant, key) = (&request.ant, request.public_key.as_str());
    auth::check_not_reserved(ant)?;
    crate::teams::check_no_team(approver.hub(), approver.js_domain(), ant).await?;
    if revoked.contains(key) {
        bail!("the key {key} is revoked");
    }
    let issued = issued::issued(dir)?;
    if let Some(other) = issued.iter().find(|i| i.order == Some(order.id) && i.key != key) {
        bail!("the order added {ant} with the key {} already", other.key);
    }
    // A key an earlier order added the ant with may ask again, as a machine that missed the answer does.
    if let Some(other) = issued.iter().find(|i| i.key == key && (i.order.is_none() || i.name != ant.as_str())) {
        bail!("the key {key} was issued to {} already", other.name);
    }
    if let Some(other) = live(dir)?.iter().find(|i| i.name == ant.as_str() && i.key != key) {
        bail!("the bus holds credentials named {ant} already (key {})", other.key);
    }
    let domain = Finding::from_jwt(&request.enrollment)?.js_domain;
    let leaf_jwt = match &request.leaf {
        Some(leaf) => {
            let role = assistant_of.map_or(LeafRole::Ant, LeafRole::AssistantOf);
            let more = Permissions::for_leaf(machine, [(ant, role)]).with_uplink_js_domain(domain.as_ref());
            let renewed = auth::extend_leaf(dir, &leaf.jwt, machine, more)
                .with_context(|| format!("the uplink of the leaf node {machine} cannot carry {ant}"))?;
            Some(renewed)
        }
        None if live(dir)?.iter().any(|i| i.kind == Kind::Leaf && i.name == auth::leaf_user(machine)) => bail!(
            "{machine} runs a leaf node, and the request carries none of its uplink credentials to renew for {ant}: \
             its daemon finds them through ~/.config/ants/leaf.conf"
        ),
        None => None,
    };
    let jwt = auth::sign_ordered(dir, ant, domain.as_ref(), key, order.id, assistant_of)?;
    if leaf_jwt.is_some() {
        members::enrolled(dir, machine, ant)
            .with_context(|| format!("keeping {ant} among the members of {machine}"))?;
        // The key is signed already: a failed admission leaves it to `ants leaf outbox` rather than refusing.
        match outbox::admit_ant(approver.hub(), machine, ant).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::info!(%machine, %ant, "the hub does not source the machine's outbox: `ants leaf outbox` sets it up");
            }
            Err(e) => {
                tracing::warn!(%machine, %ant, "admitting the ant from the machine's outbox failed: {e:#}; `ants leaf outbox` admits it");
            }
        }
    }
    Ok(EnrollReply::Approved { ant: ant.clone(), public_key: key.to_owned(), jwt, leaf_jwt, host_jwt: None })
}

/// Revoke the ant's key `request` names and the live keys of the name issued before it, which it renewed, pushing the
/// account to the hub when any was live: only an ant's or an assistant's key, whose first key (before renewals) was
/// issued no later than the admin sent the order, so that the order retires no ant added again since. Then, behind a
/// leaf node, drop the ant from it ([`drop_from_leaf`]) with the uplink credentials the request carries, answering
/// with them renewed, or why they were not. The ant leaves its team ([`crate::teams::drop_ant`]).
async fn retire(
    approver: &Approver,
    url: &str,
    request: &EnrollRequest,
    order: &AdminOrder,
    machine: &Machine,
    revoked: &BTreeSet<String>,
) -> Result<EnrollReply> {
    let dir = approver.dir.as_path();
    let (ant, key) = (&request.ant, request.public_key.as_str());
    let issued = issued::issued(dir)?;
    let of_ant = |key: &str| {
        issued
            .iter()
            .filter(|i| i.key == key && i.name == ant.as_str() && matches!(i.kind, Kind::Ant | Kind::Assistant))
            .min_by_key(|i| i.issued_at)
    };
    let asked = of_ant(key).with_context(|| format!("the bus issued no key {key} of the ant {ant}"))?;
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
    let role = asked.assistant_of.as_ref().map_or(LeafRole::Ant, LeafRole::AssistantOf);
    let newly = if keys.is_empty() { Vec::new() } else { revoke::revoke(dir, None, &keys)? };
    let push_error = if newly.is_empty() { None } else { push::push(url, dir).await.err().map(|e| format!("{e:#}")) };
    if push_error.is_none() {
        crate::signing::retire::clean_consumers(url, approver.js_domain(), dir, ant).await;
    }
    let leaf = match &request.leaf {
        Some(leaf) => match check_leaf(approver, request, leaf, machine, revoked) {
            Ok(()) => drop_from_leaf(approver, &leaf.jwt, machine, ant, role).await,
            Err(e) => Err(e),
        },
        None if live(dir)?.iter().any(|i| i.kind == Kind::Leaf && i.name == auth::leaf_user(machine)) => Err(anyhow!(
            "{machine} runs a leaf node, and the request carries none of its uplink credentials to renew without {ant}"
        )),
        None => Ok(None),
    };
    let (leaf_jwt, leaf_error) = match leaf {
        Ok(jwt) => (jwt, None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };
    if let Some(line) = crate::teams::drop_ant_or_say(approver.hub(), approver.js_domain(), ant).await {
        tracing::info!(%ant, "{line}");
    }
    let revoked = newly.into_iter().map(|r| r.key).collect();
    Ok(EnrollReply::Retired { ant: ant.clone(), public_key: key.to_owned(), revoked, push_error, leaf_jwt, leaf_error })
}

/// Drop `ant`, retired, in `role`, from the leaf node of `machine` whose uplink user JWT is `jwt`: renew the JWT for
/// the same key without what it allows only for the ant ([`auth::shrink_leaf`]), drop the ant from the leaf's members
/// in the registry and from what the hub takes from the machine's outbox. Returns the renewed JWT, none when it
/// allowed nothing only for the ant. A failure to drop the outbox sourcing is only logged: `ants leaf outbox` drops it.
pub(super) async fn drop_from_leaf(
    approver: &Approver,
    jwt: &str,
    machine: &Machine,
    ant: &UserId,
    role: LeafRole<'_>,
) -> Result<Option<String>> {
    let dir = approver.dir.as_path();
    let shrunk = auth::shrink_leaf(dir, jwt, machine, ant, role)
        .with_context(|| format!("renewing the uplink of the leaf node {machine} without {ant}"))?;
    members::retired(dir, machine, ant).with_context(|| format!("dropping {ant} from the members of {machine}"))?;
    match outbox::drop_ant(approver.hub(), machine, ant).await {
        Ok(true) => {}
        Ok(false) => tracing::info!(%machine, %ant, "the hub does not source the machine's outbox"),
        Err(e) => tracing::warn!(
            %machine, %ant,
            "dropping the ant from the machine's outbox sourcing failed: {e:#}; `ants leaf outbox` drops it"
        ),
    }
    Ok(shrunk)
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
        EnrollReply::Retired { revoked, push_error, leaf_jwt, leaf_error, .. } => {
            let pushed = match push_error {
                Some(e) => format!(", but pushing the account failed: {e}; run `ants auth push`"),
                None => String::new(),
            };
            let uplink = match (leaf_jwt, leaf_error) {
                (Some(_), _) => format!("; the uplink of {} renewed without it", order.machine),
                (None, Some(e)) => format!("; the uplink of {} left as it is: {e}", order.machine),
                (None, None) => String::new(),
            };
            println!("retired {asked}: {} keys revoked{pushed}{uplink}", revoked.len());
        }
    }
}

#[cfg(test)]
mod tests;
