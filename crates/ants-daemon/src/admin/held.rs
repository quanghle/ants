//! The orders an assistant sent for its human ([`AdminOrder::on_behalf_of`]): verified as any order, held
//! ([`OrderState::Held`]) until the human consents, and only then carried out (`super::apply`).
//!
//! An order held is acked once its held outcome is stored, and kept, its credentials left out, in the machine's list
//! of held orders ([`held_orders_key`]), which the daemon reads back on start. The human's consent is its signed
//! answer to the order ([`Act::Order`]) or a pre-allow rule covering the order's kind ([`covers_order`]), by
//! credentials that carry [`ADMIN_TAG`] and are not revoked, while the human's grant names the assistant and the
//! assistant's credentials are not revoked either: `review` looks at every held order whenever a human's consent
//! records or grants change ([`Registry::consents`], [`Registry::withdrawals`]), an order is held, or every
//! [`RECHECK`]. It replaces the held outcome with the order's applied or refused one: refused when the human denies
//! it, its grant no longer names the assistant, or [`ORDER_HOLD`] passed.

use std::sync::Arc;
use std::time::Duration;

#[cfg(doc)]
use ants_proto::ADMIN_TAG;
use ants_proto::{
    Act, ActAnswer, AdminOrder, AdminOutcome, JsDomain, ORDER_HOLD, OrderState, covers_order, held_orders_key,
    revoked_device_key, verify_user_jwt,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};

use super::{Orders, Stores, Turn, refused};
use crate::people::humans::Registry;
use crate::records::direct_kv::{DirectKv, Lookup};

/// How often the held orders are looked at besides when something changes: for those whose hold ran out.
pub const RECHECK: Duration = Duration::from_secs(60);

/// The most orders a machine holds at once; another is refused.
pub const MAX_HELD: usize = 64;

/// Why an order an assistant may not send is refused.
pub const NOT_FOR_ASSISTANT: &str = "an assistant's order may not add, retire or clone an ant";

/// What deciding the held orders shares with taking new ones.
#[derive(Debug, Clone)]
pub struct Hold {
    /// The humans' grants and consent records, kept current by a watch of the humans bucket.
    pub humans: Registry,
    /// Held while an order is decided: one at a time, whether taken or held, but for what an order waits for aside
    /// ([`super::Turn::aside`]).
    deciding: Arc<Mutex<()>>,
    /// Held while an order adds, retires or clones an ant, from before it takes its turn ([`Hold::deciding`]).
    changing: Arc<Mutex<()>>,
    /// Notified when an order is held.
    held: Arc<Notify>,
}

impl Hold {
    pub fn new(humans: Registry) -> Self {
        Self { humans, deciding: Arc::default(), changing: Arc::default(), held: Arc::default() }
    }

    /// Decide one order at a time, until the guard drops.
    pub(super) async fn deciding(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.deciding).lock_owned().await
    }

    /// Add, retire or clone one ant at a time, until the guard drops.
    pub(super) async fn changing(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.changing.lock().await
    }

    /// Have the held orders looked at, one being added.
    pub(super) fn added(&self) {
        self.held.notify_one();
    }
}

impl Default for Hold {
    fn default() -> Self {
        Self::new(Registry::listed())
    }
}

/// The machine's held orders ([`held_orders_key`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Held {
    #[serde(default)]
    orders: Vec<HeldOrder>,
}

/// An order held, as verified.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct HeldOrder {
    /// The order, without its credentials.
    order: AdminOrder,
    /// The public key of the assistant's credentials that signed it, whose revocation drops it.
    signer: String,
}

/// Hold `order`, verified, signed by the key `signer`: its outcome, held, to be recorded, or refused.
pub(super) async fn hold(
    machines: &DirectKv,
    orders: &Orders,
    order: &AdminOrder,
    signer: &str,
) -> Result<AdminOutcome> {
    if !order.op.for_assistant() {
        return Ok(refused(order, NOT_FOR_ASSISTANT));
    }
    let key = held_orders_key(&orders.machine);
    let mut held = read(machines, &key).await?;
    if !held.orders.iter().any(|h| h.order.id == order.id) {
        if held.orders.len() >= MAX_HELD {
            return Ok(refused(order, &format!("{MAX_HELD} orders of assistants are held already")));
        }
        let order = AdminOrder { credentials: String::new(), ..order.clone() };
        held.orders.push(HeldOrder { order, signer: signer.to_owned() });
        machines.put(&key, serde_json::to_vec(&held)?).await?;
    }
    Ok(AdminOutcome::of(order, OrderState::Held, None, None))
}

/// The held orders stored under `key`; none when there is nothing, or nothing that reads.
async fn read(machines: &DirectKv, key: &str) -> Result<Held> {
    Ok(match machines.lookup(key).await? {
        Lookup::Value(value) => serde_json::from_slice(&value).unwrap_or_else(|e| {
            tracing::warn!(key, error = %e, "dropping a held orders list that does not read");
            Held::default()
        }),
        Lookup::NoValue | Lookup::NoBucket => Held::default(),
    })
}

/// The orders a machine's list of held orders ([`held_orders_key`]) holds, without their credentials; none when it does
/// not read.
pub fn held_orders(value: &[u8]) -> Vec<AdminOrder> {
    serde_json::from_slice::<Held>(value).map_or_default(|held| held.orders.into_iter().map(|h| h.order).collect())
}

/// Decide the held orders whenever something changes, for as long as the connection `js` lasts: until the watch of the
/// humans' bucket has read it, which it never does while the hub has none, only by their age ([`consent`]).
pub(super) async fn keep(js: &jetstream::Context, js_domain: Option<&JsDomain>, orders: &Orders) {
    let humans = &orders.hold.humans;
    let (mut consents, mut withdrawals) = (humans.consents(), humans.withdrawals());
    let mut loaded = humans.is_loaded();
    let read = humans.loaded();
    tokio::pin!(read);
    loop {
        let reviewed = async {
            let stores = Stores::open(js, js_domain).await?;
            let machines = DirectKv::new(js, js_domain, ants_proto::MACHINES_BUCKET).await?;
            Box::pin(review(js, &machines, &stores, orders, Utc::now())).await
        };
        if let Err(e) = reviewed.await {
            tracing::warn!(machine = %orders.machine, error = format!("{e:#}"), "deciding the held admin orders");
        }
        tokio::select! {
            _ = consents.recv() => {}
            _ = withdrawals.recv() => {}
            () = orders.hold.held.notified() => {}
            () = tokio::time::sleep(RECHECK) => {}
            () = &mut read, if !loaded => loaded = true,
        }
    }
}

/// Decide each held order the human consented to, denied, or can consent to no longer at `now`, recording its outcome
/// in place of the held one; keep the rest.
async fn review(
    js: &jetstream::Context,
    machines: &DirectKv,
    stores: &Stores,
    orders: &Orders,
    now: DateTime<Utc>,
) -> Result<()> {
    let _deciding = orders.hold.deciding().await;
    let key = held_orders_key(&orders.machine);
    let held = read(machines, &key).await?;
    let mut kept = Vec::new();
    for h in &held.orders {
        let order = &h.order;
        let outcome_key = order.outcome_key();
        if let Lookup::Value(value) = machines.lookup(&outcome_key).await? {
            let state = serde_json::from_slice::<AdminOutcome>(&value).map(|o| o.state);
            if state.is_ok_and(|state| state != OrderState::Held) {
                continue;
            }
        }
        let outcome = match consent(&orders.hold.humans, h, now) {
            Consent::Waiting => {
                kept.push(h.clone());
                continue;
            }
            Consent::Refused(reason) => refused(order, &reason),
            Consent::Given { jwt, how } => match consenter(stores, orders, h, &jwt, now).await? {
                Some(reason) => refused(order, &reason),
                None => {
                    tracing::info!(id = %order.id, human = ?order.on_behalf_of, how, "carrying out an assistant's order");
                    super::apply(js, stores, orders, &Turn::within(&orders.hold), order, now).await?
                }
            },
        };
        machines.put(&outcome_key, serde_json::to_vec(&outcome)?).await?;
        tracing::info!(
            key = outcome_key,
            admin = %order.admin,
            human = ?order.on_behalf_of,
            state = outcome.state.as_str(),
            reason = outcome.reason.as_deref(),
            "held admin order decided"
        );
    }
    if kept != held.orders {
        machines.put(&key, serde_json::to_vec(&Held { orders: kept })?).await.context("storing the held orders")?;
    }
    Ok(())
}

/// Where the consent of the human of a held order stands.
#[derive(Debug, PartialEq, Eq)]
enum Consent {
    Waiting,
    /// Refused, for this reason.
    Refused(String),
    /// Consented to by the record signed under the user JWT `jwt`, `how` naming it.
    Given {
        jwt: String,
        how: String,
    },
}

/// Where the consent of the human of `held` stands at `now`, by its records `humans` keeps. Until `humans` has read
/// them, no grant stands, which does not mean that one was withdrawn: the order then only expires.
fn consent(humans: &Registry, held: &HeldOrder, now: DateTime<Utc>) -> Consent {
    let order = &held.order;
    let Some(human) = &order.on_behalf_of else { return Consent::Refused("no human named".to_owned()) };
    let expired = || {
        (now > order.sent_at + ORDER_HOLD)
            .then(|| Consent::Refused(format!("{human} did not confirm it within {} days", ORDER_HOLD.num_days())))
    };
    if !humans.is_loaded() {
        return expired().unwrap_or(Consent::Waiting);
    }
    let assistant = &order.admin;
    if humans.assistant(human).as_ref() != Some(assistant) {
        return Consent::Refused(format!("withdrawn: the grant of {human} no longer names {assistant}"));
    }
    let act = Act::Order { machine: order.machine.clone(), ant: order.ant.clone(), id: order.id };
    match humans.answer(human, assistant, &act) {
        Some(ActAnswer::Deny) => return Consent::Refused(format!("denied by {human}")),
        Some(ActAnswer::Confirm) => {
            if let Some(confirmations) = humans.confirmations(human) {
                return Consent::Given { jwt: confirmations.jwt, how: "confirmed".to_owned() };
            }
        }
        None => {}
    }
    if let Some(rules) = humans.pre_allow(human, assistant)
        && let Some(rule) = rules.rules_for(&order.ant).find(|rule| covers_order(rule, &order.op))
    {
        return Consent::Given { how: format!("pre-allowed by {rule}"), jwt: rules.jwt.clone() };
    }
    expired().unwrap_or(Consent::Waiting)
}

/// Why the consent signed under `jwt` to `held` does not count at `now`, if it does not: the credentials of the human
/// carry no admin role, or they or the assistant's are revoked.
async fn consenter(
    stores: &Stores,
    orders: &Orders,
    held: &HeldOrder,
    jwt: &str,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    let human = held.order.on_behalf_of.as_ref().context("an order held for no human")?;
    let at = u64::try_from(now.timestamp()).unwrap_or_default();
    let user = match verify_user_jwt(jwt, orders.accounts.iter().map(String::as_str), at) {
        Ok(user) if user.id == *human => user,
        Ok(user) => return Ok(Some(format!("the consent of {human} is signed by {}", user.id))),
        Err(e) => return Ok(Some(format!("the credentials of {human}'s consent: {e}"))),
    };
    if !user.is_admin() {
        return Ok(Some(format!("{human}, who consented, is no admin: its credentials carry no admin role")));
    }
    for (whose, key) in [(human, &user.public_key), (&held.order.admin, &held.signer)] {
        if revoked(stores, key).await? {
            return Ok(Some(format!("the credentials of {whose} are revoked ({key})")));
        }
    }
    Ok(None)
}

async fn revoked(stores: &Stores, key: &str) -> Result<bool> {
    Ok(matches!(stores.devices.lookup(revoked_device_key(key)).await?, Lookup::Value(_)))
}

#[cfg(test)]
mod tests;
