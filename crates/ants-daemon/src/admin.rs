//! The admin orders to a machine's ants ([`AdminOrder`]), which its host connection ([`crate::host`]) takes from the
//! hub's stream through the machine's own consumer ([`admin_consumer`]), one at a time and in the order they were
//! stored. Each it verifies ([`AdminOrder::verify`]) and decides once, recording the outcome in [`MACHINES_BUCKET`]
//! under the key of the machine and ant of the subject it came on and the order's id ([`admin_outcome_key`]), and acks
//! it only after the outcome is stored. A message whose order already has an outcome, a replay, it acks without
//! deciding again. It refuses every order while the config sets `[admin] remote = false`.
//!
//! An order an admin signed it carries out ([`apply`]) with the module of its op: [`set`] edits the ant's settings in
//! the config file the daemon serves ([`ConfigFile`]), which the daemon reloads at once; [`add`] adds an ant to it,
//! its credentials signed by `ants signer` on the order, and [`retire`] removes one, its credentials revoked so.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::{
    AdminOp, AdminOrder, AdminOutcome, DEVICES_BUCKET, JsDomain, MACHINES_BUCKET, Machine, OrderState, STREAM, UserId,
    Uuid, admin_consumer, admin_outcome_key, revoked_device_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer, pull};
use async_nats::jetstream::{self, AckKind};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde_json::Value;
use tokio::sync::Notify;

use crate::bus;
use crate::direct_kv::{DirectKv, Lookup};

mod add;
mod ask;
mod retire;
mod set;

/// How long an order may stay in hand unacked before the stream delivers it again.
const ACK_WAIT: Duration = Duration::from_secs(60);
/// How often an order in hand for long tells the stream so ([`in_progress`]).
const PROGRESS: Duration = Duration::from_secs(20);
/// How long before the stream delivers again an order whose outcome could not be stored.
const RETRY_ORDER: Duration = Duration::from_secs(5);
const RETRY_FIRST: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(300);

/// Why every order is refused while the config sets `[admin] remote = false` (requirement: a machine may opt out).
pub const REMOTE_OFF: &str = "this machine's config sets [admin] remote = false";
/// Why an order is refused that the daemon cannot carry out yet.
pub const NOT_SUPPORTED: &str = "not supported yet";

/// Why an order to edit the config is refused by a daemon serving no config file.
pub const NO_CONFIG: &str = "the daemon serves no config file to edit";

/// What the host connection of a machine needs to decide the orders to its ants.
#[derive(Debug, Clone)]
pub struct Orders {
    pub machine: Machine,
    /// The config's `[admin] remote`: false refuses every order.
    pub remote: bool,
    /// The accounts whose admins' orders count: the issuer of the host credentials. None without credentials, which
    /// refuses every order.
    pub accounts: Vec<String>,
    /// The config file the daemon serves whole, which orders edit; none refuses those.
    pub config: Option<ConfigFile>,
    /// The host credentials, which ask `ants signer` to sign an added ant's key or revoke a retired one's, and beside
    /// which an added ant's credentials go; none refuses those orders.
    pub creds: Option<PathBuf>,
    /// The hub's JetStream domain, where a retirement reads the roster.
    pub js_domain: Option<JsDomain>,
}

/// The config file a daemon serves whole, as the orders to its machine's ants edit it, and how to have the daemon
/// reload it at once after an edit rather than at its next check of the file.
#[derive(Debug, Clone)]
pub struct ConfigFile {
    pub path: PathBuf,
    /// Notified once per edit written.
    pub edited: Arc<Notify>,
}

impl PartialEq for ConfigFile {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path && Arc::ptr_eq(&self.edited, &other.edited)
    }
}

impl Eq for ConfigFile {}

/// Take the orders to the machine's ants and record their outcomes, for as long as the connection `js` lasts:
/// [`take`] again, after a pause, whenever it stops. `js_domain` is the domain `js` was made for.
pub async fn serve(js: jetstream::Context, js_domain: Option<JsDomain>, orders: Orders) {
    let mut delay = RETRY_FIRST;
    loop {
        match take(&js, js_domain.as_ref(), &orders).await {
            Ok(()) => delay = RETRY_FIRST,
            Err(e) => {
                tracing::warn!(
                    machine = %orders.machine, error = format!("{e:#}"), retry_in = ?delay,
                    "taking the admin orders failed; `ants init` with a human's credentials sets up the stream"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(RETRY_MAX);
            }
        }
    }
}

/// Take the orders through the machine's consumer, created if missing, until its messages stop.
async fn take(js: &jetstream::Context, js_domain: Option<&JsDomain>, orders: &Orders) -> Result<()> {
    let name = admin_consumer(&orders.machine);
    let stream = js.get_stream_no_info(STREAM).await.context("naming the ANTS stream")?;
    let consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: subject::admin_orders(&orders.machine),
            ack_policy: AckPolicy::Explicit,
            ack_wait: ACK_WAIT,
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .with_context(|| format!("creating consumer {name}"))?;
    let mut messages = consumer.messages().await.with_context(|| format!("reading consumer {name}"))?;
    let outcomes = DirectKv::new(js, js_domain, MACHINES_BUCKET).await?;
    let devices = DirectKv::new(js, js_domain, DEVICES_BUCKET).await?;
    tracing::debug!(machine = %orders.machine, "taking admin orders");
    while let Some(msg) = messages.next().await {
        let msg = msg.with_context(|| format!("reading consumer {name}"))?;
        match handle(js, &outcomes, &devices, orders, &msg).await {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!(
                    subject = %msg.subject, error = format!("{e:#}"), retry_in = ?RETRY_ORDER,
                    "recording an admin order's outcome failed"
                );
                if let Err(e) = msg.ack_with(AckKind::Nak(Some(RETRY_ORDER))).await {
                    tracing::debug!(error = %e, "naking the order; the stream delivers it again after its ack wait");
                }
            }
        }
    }
    Ok(())
}

/// Decide the order in `msg` unless it has an outcome, record the outcome, and ack it.
async fn handle(
    js: &jetstream::Context,
    outcomes: &DirectKv,
    devices: &DirectKv,
    orders: &Orders,
    msg: &jetstream::Message,
) -> Result<()> {
    let stored_at = msg.info().map(|info| DateTime::<Utc>::from(std::time::SystemTime::from(info.published)));
    let msg_id = msg.headers.as_ref().and_then(|h| h.get("Nats-Msg-Id")).map(|id| id.as_str().to_owned());
    match read(&msg.subject, &msg.payload, msg_id.as_deref(), &orders.machine) {
        Taken::Unnamed(why) => tracing::warn!(subject = %msg.subject, "dropping an admin order: {why}"),
        taken => {
            let key = taken.key();
            if matches!(outcomes.lookup(&key).await?, Lookup::Value(_)) {
                tracing::debug!(key, "the admin order has an outcome already: a replay");
            } else {
                let outcome = match taken {
                    Taken::Order { order, machine, ant } => {
                        let stored_at = stored_at.unwrap_or_else(|_| Utc::now());
                        let deciding = decide(js, devices, orders, &msg.subject, &order, stored_at);
                        let mut outcome = in_progress(msg, deciding).await?;
                        (outcome.machine, outcome.ant) = (machine, ant);
                        outcome
                    }
                    Taken::Unreadable(outcome) => *outcome,
                    Taken::Unnamed(_) => unreachable!("handled above"),
                };
                let recorded = outcomes.create(&key, serde_json::to_vec(&outcome)?).await?;
                tracing::info!(
                    key,
                    recorded,
                    admin = outcome.admin.as_ref().map(tracing::field::display),
                    state = outcome.state.as_str(),
                    reason = outcome.reason.as_deref(),
                    "admin order decided"
                );
            }
        }
    }
    bus::double_ack(&js.client(), msg).await
}

/// `work` on the order in `msg`, telling the stream every [`PROGRESS`] that it is in hand, so that an order taking
/// longer than [`ACK_WAIT`] (an ant to stop, the signer to answer) is not delivered again meanwhile.
async fn in_progress<T>(msg: &jetstream::Message, work: impl Future<Output = T>) -> T {
    tokio::pin!(work);
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS, PROGRESS);
    loop {
        tokio::select! {
            done = &mut work => return done,
            _ = ticks.tick() => {
                if let Err(e) = msg.ack_with(AckKind::Progress).await {
                    tracing::debug!(error = %e, "telling the stream the order is in hand");
                }
            }
        }
    }
}

/// What a message of the machine's consumer holds.
#[derive(Debug)]
enum Taken {
    /// An order, with the machine and ant of the subject it came on, under which its outcome is recorded whatever
    /// the order names.
    Order { order: Box<AdminOrder>, machine: Machine, ant: UserId },
    /// No order that reads, with an id: its refusal.
    Unreadable(Box<AdminOutcome>),
    /// Nothing to record an outcome under: why.
    Unnamed(String),
}

impl Taken {
    fn key(&self) -> String {
        match self {
            Self::Order { order, machine, ant } => admin_outcome_key(machine, ant, &order.id),
            Self::Unreadable(outcome) => outcome.key(),
            Self::Unnamed(_) => String::new(),
        }
    }
}

/// Read what came on `subject` to the consumer of `machine`, as `payload` with the message id `msg_id`.
fn read(subject: &str, payload: &[u8], msg_id: Option<&str>, machine: &Machine) -> Taken {
    let Some((to, ant)) = subject::parse_admin_order(subject) else {
        return Taken::Unnamed("not an admin order's subject".to_owned());
    };
    if to != *machine {
        return Taken::Unnamed(format!("an order to machine {to}, not {machine}"));
    }
    match serde_json::from_slice::<AdminOrder>(payload) {
        Ok(order) => Taken::Order { order: Box::new(order), machine: to, ant },
        Err(e) => {
            let named = serde_json::from_slice::<Value>(payload).ok().and_then(|v| v["id"].as_str()?.parse().ok());
            match named.or_else(|| msg_id?.parse::<Uuid>().ok()) {
                Some(id) => {
                    Taken::Unreadable(Box::new(AdminOutcome::unreadable(id, to, ant, format!("unreadable order: {e}"))))
                }
                None => Taken::Unnamed(format!("an unreadable order without an id: {e}")),
            }
        }
    }
}

/// The outcome of `order`, taken from `subject` and stored at `stored_at`: refused unless the config allows remote
/// administration and an admin of the team signed it with credentials not revoked, as [`devices`] records them;
/// else what carrying it out ([`apply`]) gave.
async fn decide(
    js: &jetstream::Context,
    devices: &DirectKv,
    orders: &Orders,
    subject: &str,
    order: &AdminOrder,
    stored_at: DateTime<Utc>,
) -> Result<AdminOutcome> {
    let refused = |reason: &str| AdminOutcome::of(order, OrderState::Refused, Some(reason.to_owned()), None);
    if !orders.remote {
        return Ok(refused(REMOTE_OFF));
    }
    if orders.accounts.is_empty() {
        return Ok(refused("the machine's host connection has no credentials whose account vouches for an admin"));
    }
    let revoked = match order.signing_key() {
        Some(key) => match devices.lookup(revoked_device_key(&key)).await? {
            Lookup::Value(_) => true,
            Lookup::NoValue | Lookup::NoBucket => false,
        },
        None => false,
    };
    let accounts = orders.accounts.iter().map(String::as_str);
    match order.verify(subject, accounts, |_| revoked, stored_at) {
        Err(e) => Ok(refused(&e.to_string())),
        Ok(_) => Ok(apply(js, orders, order).await),
    }
}

/// Carry out `order`, which an admin signed, with the module of its op, which refuses it when it cannot.
async fn apply(js: &jetstream::Context, orders: &Orders, order: &AdminOrder) -> AdminOutcome {
    match &order.op {
        AdminOp::Set { changes } => set::apply(orders, order, changes),
        AdminOp::Pause => refused(order, NOT_SUPPORTED),
        AdminOp::Resume => refused(order, NOT_SUPPORTED),
        AdminOp::Restart { .. } => refused(order, NOT_SUPPORTED),
        AdminOp::Add { settings } => add::apply(js, orders, order, settings).await,
        AdminOp::Retire => retire::apply(js, orders, order).await,
        AdminOp::ReadMemories => refused(order, NOT_SUPPORTED),
    }
}

/// `order` applied, decided now, with what its op returns, if anything.
fn applied(order: &AdminOrder, result: Option<Value>) -> AdminOutcome {
    AdminOutcome::of(order, OrderState::Applied, None, result)
}

/// `order` refused for `reason`, decided now.
fn refused(order: &AdminOrder, reason: &str) -> AdminOutcome {
    AdminOutcome::of(order, OrderState::Refused, Some(reason.to_owned()), None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(s: &str) -> Machine {
        s.parse().unwrap()
    }

    #[test]
    fn an_order_is_recorded_under_the_subject_it_came_on() {
        let order = AdminOrder::new(
            "alice".parse().unwrap(),
            m("m2"),
            "dev-b".parse().unwrap(),
            ants_proto::AdminOp::Pause,
            "",
        );
        let payload = serde_json::to_vec(&order).unwrap();
        let taken = read("ants.admin.m1.dev-a", &payload, None, &m("m1"));
        assert_eq!(taken.key(), format!("m1.dev-a.{}", order.id), "never another machine's key");
        assert!(matches!(read("ants.admin.m2.dev-a", &payload, None, &m("m1")), Taken::Unnamed(_)));

        let id = Uuid::now_v7();
        let garbled = serde_json::to_vec(&serde_json::json!({ "id": id, "op": "nonsense" })).unwrap();
        let Taken::Unreadable(outcome) = read("ants.admin.m1.dev-a", &garbled, None, &m("m1")) else { panic!() };
        assert_eq!((outcome.key(), outcome.state), (format!("m1.dev-a.{id}"), OrderState::Refused));
        let by_header = read("ants.admin.m1.dev-a", b"{", Some(&id.to_string()), &m("m1"));
        assert_eq!(by_header.key(), format!("m1.dev-a.{id}"));
        assert!(matches!(read("ants.admin.m1.dev-a", b"{", None, &m("m1")), Taken::Unnamed(_)));
    }
}
