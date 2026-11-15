//! Admin orders: what an admin, a human whose user JWT carries [`ADMIN_TAG`], asks the daemon of an ant's machine to
//! do to the ant ([`AdminOrder`]), and what came of it ([`AdminOutcome`]).
//!
//! - An order travels on [`subject::admin_order`] (`ants.admin.<machine>.<ant>`), which the `ANTS` stream keeps, so an
//!   order to a machine whose daemon is down waits there, for up to the stream's retention, and the machine's daemon
//!   takes it with its host credentials through its own consumer ([`admin_consumer`]) when it is back. Only humans
//!   publish orders; no ant reads or sends one.
//! - The order carries the admin's user JWT and the signature of what it says ([`AdminOrder::signed`]) by the key the
//!   JWT is for: the daemon checks both against the account of its own credentials ([`AdminOrder::verify`]), so no one
//!   but an admin's credentials makes an order a daemon carries out, and an order carrying anything the daemon does not
//!   know fails its signature check rather than being carried out in part.
//! - An order may wait days for its machine: it is not refused for its age. The admin's credentials must not have
//!   expired, nor have been revoked, when the stream stored it ([`AdminOrder::verify`] takes that time, which the server
//!   sets), and its `sent_at` may not be further ahead of that than [`ORDER_CLOCK_SKEW`]. The stream drops a second
//!   copy carrying the same `Nats-Msg-Id` (the order's id) within its duplicate window; the daemon refuses a replay by
//!   the order's id, any later: it records each order's outcome once ([`admin_outcome_key`]) and acks a message whose
//!   order has one without carrying it out again.
//! - The outcome of each order, applied or refused with the reason, the machine's daemon records in
//!   [`MACHINES_BUCKET`], beside the machine's record, under [`admin_outcome_key`], which only the machine's host
//!   credentials write: humans read every outcome, so the orders of an ant ([`subject::admin_outcomes_of`]); an order
//!   without one waits, pending.
//! - An assistant does not send orders: its credentials do not publish them. Confirming an assistant's order on behalf
//!   of its human will add who it acts for to the signed lines, which a daemon not knowing the field fails to verify.

use std::fmt::Write as _;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

#[cfg(doc)]
use crate::{ADMIN_TAG, MACHINES_BUCKET, admin_consumer};
use crate::{JwtError, Machine, UserId, VerifiedUser, subject, verified_jwt, verify_admin};

/// How far ahead of the time the stream stored it an order may say it was sent.
pub const ORDER_CLOCK_SKEW: TimeDelta = TimeDelta::minutes(5);

/// What the first of the lines an admin signs says ([`AdminOrder::signed`]), so a signature of anything else is no
/// order's.
pub const ORDER_SIGNING_CONTEXT: &str = "ants-admin-order";

/// An admin's order to the daemon of `machine` about `ant`, signed by the admin's credentials. See the
/// [module docs](self).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminOrder {
    /// A version 7 UUID, the key of the order's outcome and its `Nats-Msg-Id`.
    pub id: Uuid,
    /// The admin, as its user JWT names it.
    pub admin: UserId,
    /// The machine whose daemon serves the ant, or is to serve it ([`AdminOp::Add`]).
    pub machine: Machine,
    pub ant: UserId,
    pub op: AdminOp,
    /// When the admin's CLI sent it.
    pub sent_at: DateTime<Utc>,
    /// The user JWT of the admin's credentials, which binds the key that signed to the admin.
    pub credentials: String,
    /// Signature of [`AdminOrder::signed`] by the key `credentials` is for, base64url without padding.
    pub signature: String,
}

/// What an [`AdminOrder`] asks for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdminOp {
    /// Change fields of the ant's settings, each only if it holds the value expected, all or none.
    Set {
        changes: Vec<FieldChange>,
    },
    /// Take no new prompt, keeping the ant's messages queued.
    Pause,
    Resume,
    /// Stop the ant's harnesses: idle ones at once, working ones after their current prompt, or at once with `now`.
    Restart {
        #[serde(default)]
        now: bool,
    },
    /// Add the ant on the machine with these settings: the keys of its `[[ant]]` table in the config file, its name the
    /// order's ant.
    Add {
        #[serde(default)]
        settings: Map<String, Value>,
    },
    /// Drain the ant, revoke and delete its credentials, keeping its home.
    Retire,
    /// Read the ant's core and harness memories, which the outcome's `result` carries.
    ReadMemories,
}

impl AdminOp {
    /// The op in a few words, as an ant's orders list it.
    pub fn describe(&self) -> String {
        match self {
            Self::Set { changes } => {
                let fields: Vec<String> = changes.iter().map(FieldChange::describe).collect();
                format!("set {}", fields.join(", "))
            }
            Self::Pause => "pause".to_owned(),
            Self::Resume => "resume".to_owned(),
            Self::Restart { now: false } => "restart".to_owned(),
            Self::Restart { now: true } => "restart now".to_owned(),
            Self::Add { .. } => "add".to_owned(),
            Self::Retire => "retire".to_owned(),
            Self::ReadMemories => "read memories".to_owned(),
        }
    }
}

/// A change of one field of the ant's settings in an [`AdminOp::Set`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldChange {
    /// The keys leading to the field from the ant's `[[ant]]` table: `["model"]`, `["caps", "daily"]`.
    pub field: Vec<String>,
    /// The value the field must hold for the change to apply; none when the field must be absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<Value>,
    /// The new value; none to remove the field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

impl FieldChange {
    /// The change in a few words: `model = "gpt-5"`, `caps.daily removed`.
    pub fn describe(&self) -> String {
        let field = self.field.join(".");
        match &self.value {
            Some(value) => format!("{field} = {value}"),
            None => format!("{field} removed"),
        }
    }
}

impl AdminOrder {
    /// The order of `op` about `ant` on `machine` by `admin`, sent now by the credentials whose user JWT is
    /// `credentials`, to be completed with its signature.
    pub fn new(admin: UserId, machine: Machine, ant: UserId, op: AdminOp, credentials: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            admin,
            machine,
            ant,
            op,
            sent_at: Utc::now(),
            credentials: credentials.into(),
            signature: String::new(),
        }
    }

    /// The subject it travels on.
    pub fn subject(&self) -> String {
        subject::admin_order(&self.machine, &self.ant)
    }

    /// The key of its outcome in [`MACHINES_BUCKET`].
    pub fn outcome_key(&self) -> String {
        admin_outcome_key(&self.machine, &self.ant, &self.id)
    }

    /// What the admin's key signs: lines joined by `\n`, [`ORDER_SIGNING_CONTEXT`], the id, the admin, the machine, the
    /// ant, `sent_at` in RFC 3339 with nanoseconds and `Z`, and the op as canonical JSON ([`canonical_json`]). Every
    /// field but the JWT, which the signing key is checked against, and the signature.
    pub fn signed(&self) -> Vec<u8> {
        let sent_at = self.sent_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let op = canonical_json(&serde_json::to_value(&self.op).expect("an op serializes"));
        let id = self.id.to_string();
        [ORDER_SIGNING_CONTEXT, &id, self.admin.as_str(), self.machine.as_str(), self.ant.as_str(), &sent_at, &op]
            .join("\n")
            .into_bytes()
    }

    /// The public key (`U...`) of the credentials that signed, as their JWT says before anything checks it: the key
    /// whose revocation record to look up for [`AdminOrder::verify`].
    pub fn signing_key(&self) -> Option<String> {
        verified_jwt(&self.credentials).ok().map(|(claims, _)| claims.sub)
    }

    /// Check the order, taken from `subject`, which the stream stored at `stored_at`, for the daemon of a team whose
    /// accounts are `accounts` (the issuers of the daemon's credentials): on its machine's and ant's subject, signed by
    /// the key of credentials of the admin it names that an account signed, that carry [`ADMIN_TAG`], that had not
    /// expired when it was stored and whose key `revoked` (looked up in the revocation records) does not say is revoked,
    /// and sent no further ahead of `stored_at` than [`ORDER_CLOCK_SKEW`].
    pub fn verify<'a>(
        &self,
        subject: &str,
        accounts: impl IntoIterator<Item = &'a str>,
        revoked: impl FnOnce(&str) -> bool,
        stored_at: DateTime<Utc>,
    ) -> Result<VerifiedUser, OrderError> {
        if subject != self.subject() {
            return Err(OrderError::WrongSubject(subject.to_owned()));
        }
        let at = u64::try_from(stored_at.timestamp()).unwrap_or_default();
        let admin = verify_admin(&self.credentials, accounts, &self.signed(), &self.signature, at)?;
        if admin.id != self.admin {
            return Err(OrderError::NotTheAdmin { named: admin.id, admin: self.admin.clone() });
        }
        if revoked(&admin.public_key) {
            return Err(OrderError::Revoked(admin.public_key));
        }
        if self.sent_at > stored_at + ORDER_CLOCK_SKEW {
            return Err(OrderError::Ahead(self.sent_at));
        }
        Ok(admin)
    }
}

/// Why a daemon refuses an order ([`AdminOrder::verify`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OrderError {
    #[error("taken from {0}, not the subject of the order's machine and ant")]
    WrongSubject(String),
    #[error("the admin's credentials: {0}")]
    Credentials(#[from] JwtError),
    #[error("signed by the credentials of {named}, not of {admin}")]
    NotTheAdmin { named: UserId, admin: UserId },
    #[error("signed by revoked credentials ({0})")]
    Revoked(String),
    #[error("sent at {0}, ahead of the time it was stored")]
    Ahead(DateTime<Utc>),
}

/// Where an order stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    /// Waiting in the stream for its machine's daemon, which has recorded no outcome yet.
    Pending,
    Applied,
    Refused,
}

impl OrderState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Applied => "applied",
            Self::Refused => "refused",
        }
    }
}

/// What came of an order, which its machine's daemon records once, under [`admin_outcome_key`] in
/// [`MACHINES_BUCKET`]: who ordered what, when, and whether it was applied. What it names of the order, the daemon
/// verified only when it applied it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminOutcome {
    pub id: Uuid,
    pub machine: Machine,
    pub ant: UserId,
    /// The admin the order names; none when it could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin: Option<UserId>,
    /// What the order asked for; none when it could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<AdminOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    /// Applied or refused: an order with no outcome is pending.
    pub state: OrderState,
    /// Why it was refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// What the op returns, such as the memories read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// When the daemon decided.
    pub decided_at: DateTime<Utc>,
}

impl AdminOutcome {
    /// The outcome `state` of `order`, decided now.
    pub fn of(order: &AdminOrder, state: OrderState, reason: Option<String>, result: Option<Value>) -> Self {
        Self {
            id: order.id,
            machine: order.machine.clone(),
            ant: order.ant.clone(),
            admin: Some(order.admin.clone()),
            op: Some(order.op.clone()),
            sent_at: Some(order.sent_at),
            state,
            reason,
            result,
            decided_at: Utc::now(),
        }
    }

    /// The refusal, decided now, of order `id` to `ant` on `machine`, which could not be read: `reason` says why.
    pub fn unreadable(id: Uuid, machine: Machine, ant: UserId, reason: String) -> Self {
        Self {
            id,
            machine,
            ant,
            admin: None,
            op: None,
            sent_at: None,
            state: OrderState::Refused,
            reason: Some(reason),
            result: None,
            decided_at: Utc::now(),
        }
    }

    pub fn key(&self) -> String {
        admin_outcome_key(&self.machine, &self.ant, &self.id)
    }
}

/// Key in [`MACHINES_BUCKET`] of the outcome of order `id` to `ant` on `machine`: `<machine>.<ant>.<id>`, under the
/// machine's name, which its host credentials alone write, and beside its record, the key of its name alone.
pub fn admin_outcome_key(machine: &Machine, ant: &UserId, id: &Uuid) -> String {
    format!("{machine}.{ant}.{id}")
}

/// The machine, ant and order id an outcome key names ([`admin_outcome_key`]), if it is one.
pub fn admin_outcome_of(key: &str) -> Option<(Machine, UserId, Uuid)> {
    let mut parts = key.split('.');
    let (machine, ant, id) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    Some((machine.parse().ok()?, ant.parse().ok()?, id.parse().ok()?))
}

/// `value` as JSON text with no whitespace and the keys of every object sorted, whatever order the value holds them
/// in: what an admin signs of an op ([`AdminOrder::signed`]).
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let _ = write!(out, "{}:", Value::String(key.clone()));
                write_canonical(item, out);
            }
            out.push('}');
        }
        scalar => {
            let _ = write!(out, "{scalar}");
        }
    }
}

#[cfg(test)]
mod tests;
