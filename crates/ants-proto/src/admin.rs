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
//! - An assistant sends orders for its human ([`AdminOrder::on_behalf_of`]), signed by its own credentials, whose user
//!   JWT names the human ([`crate::AssistantOf`]). The daemon holds such an order ([`OrderState::Held`]) and carries it
//!   out only once the human, an admin, confirms it ([`crate::Act::Order`]) or a pre-allow rule of the human covers it
//!   ([`covers_order`]), while the human's grant names the assistant; a denial, a withdrawal or [`ORDER_HOLD`] passing
//!   refuses it. It never adds, retires or clones an ant for an assistant. Who it acts for is a signed line appended
//!   only when there is one, so the signatures of other orders stay what they were; a daemon not knowing the field
//!   fails to verify such an order.

use std::fmt::Write as _;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

#[cfg(doc)]
use crate::{ADMIN_TAG, MACHINES_BUCKET, Memories, admin_consumer};
use crate::{
    JwtError, Machine, RunId, TrailLimits, UserId, VerifiedUser, subject, verified_jwt, verify_admin,
    verify_assistant_of,
};

mod payload;
pub use payload::{ADMIN_PAYLOADS, AdminPayload, MAX_ADMIN_PAYLOAD, ProviderExport};

/// How far ahead of the time the stream stored it an order may say it was sent.
pub const ORDER_CLOCK_SKEW: TimeDelta = TimeDelta::minutes(5);

/// How long after it was sent an assistant's order waits for its human's consent before the daemon refuses it.
pub const ORDER_HOLD: TimeDelta = TimeDelta::days(7);

/// The pre-allow rule ([`crate::pre_allow_rule`]) prefix of an order kind: `order:pause` covers the pause orders of
/// an assistant, `order:*` all of them ([`covers_order`]).
pub const ORDER_RULE: &str = "order:";

/// The kinds of the orders an assistant may send for its human ([`AdminOp::for_assistant`]).
pub const ASSISTANT_ORDER_KINDS: [&str; 6] = ["set", "pause", "resume", "restart", "read_memories", "read_settings"];

/// What the first of the lines an admin signs says ([`AdminOrder::signed`]), so a signature of anything else is no
/// order's.
pub const ORDER_SIGNING_CONTEXT: &str = "ants-admin-order";

/// An admin's order to the daemon of `machine` about `ant`, signed by the admin's credentials. See the
/// [module docs](self).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminOrder {
    /// A version 7 UUID, the key of the order's outcome and its `Nats-Msg-Id`.
    pub id: Uuid,
    /// The admin, as its user JWT names it; for an order of an assistant, the assistant.
    pub admin: UserId,
    /// The human an assistant sent the order for, whose consent it waits for. See the [module docs](self).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
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
    /// order's ant; with `assistant_of`, as the assistant of that human, who must be the order's admin.
    Add {
        #[serde(default)]
        settings: Map<String, Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        assistant_of: Option<UserId>,
    },
    /// Drain the ant, revoke and delete its credentials, keeping its home.
    Retire,
    /// Read the ant's core and harness memories and, given `identity`, the id of its identity record, the memories view
    /// of its provider, which the outcome's `result` carries ([`Memories`]).
    ReadMemories {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        identity: Option<String>,
    },
    /// Export the ant, whose identity record's id is `identity`, from its provider, which stores the export for the
    /// admin to read: the outcome's `result` says where ([`ProviderExport`]).
    ExportProvider {
        identity: String,
    },
    /// Hand the ant's provider the export of an ant ([`crate::provider::Export`]) stored at `payload`, for the ant
    /// whose identity record's id is `identity`.
    ImportProvider {
        identity: String,
        payload: AdminPayload,
    },
    /// Read the ant's settings as the machine's config file has them, which the outcome's `result` carries
    /// ([`Settings`]).
    ReadSettings,
    /// Read the MCP servers the ant's harness runs and how each stands, which the outcome's `result` carries
    /// ([`McpReport`]).
    ReadMcp,
    /// Sign the ant's harness in to its MCP server `server` with OAuth, the outcome's `result` carrying the address to
    /// open in a browser ([`McpSignIn`]); with `callback`, hand the harness waiting the address the browser came back
    /// to, which a browser on another machine than the ant's cannot reach.
    McpSignIn {
        server: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        callback: Option<String>,
    },
    /// Clone the ant, on its machine, as the ant `new`: its home, settings (these overriding them), thread sessions,
    /// thread history and worktrees, under a key of its own.
    Clone {
        new: UserId,
        #[serde(default, skip_serializing_if = "Map::is_empty")]
        settings: Map<String, Value>,
    },
    /// Stop the trail run `run` the ant conducts, as a human's request to the conductor does
    /// ([`crate::TrailAction::Stop`]); the outcome's `result` carries the conductor's answer
    /// ([`crate::TrailOutcome::Stopped`]), naming the steps left in other ants' queues.
    StopTrail {
        run: RunId,
    },
    /// Resume the trail run `run` the ant conducts, stopped or failed on a limit, under `limits` when given, as a
    /// human's request to the conductor does ([`crate::TrailAction::Resume`]).
    ResumeTrail {
        run: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limits: Option<TrailLimits>,
    },
}

impl AdminOp {
    /// The op's `kind`, as it travels: `set`, `read_memories`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Set { .. } => "set",
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Restart { .. } => "restart",
            Self::Add { .. } => "add",
            Self::Retire => "retire",
            Self::ReadMemories { .. } => "read_memories",
            Self::ExportProvider { .. } => "export_provider",
            Self::ImportProvider { .. } => "import_provider",
            Self::ReadSettings => "read_settings",
            Self::ReadMcp => "read_mcp",
            Self::McpSignIn { .. } => "mcp_sign_in",
            Self::Clone { .. } => "clone",
            Self::StopTrail { .. } => "stop_trail",
            Self::ResumeTrail { .. } => "resume_trail",
        }
    }

    /// Whether an assistant may order it for its human: anything but adding, retiring or cloning an ant, reading or
    /// signing in its MCP servers, which `ants signer` signs for an admin's own order only, exporting or importing
    /// what its provider holds of it, or stopping or resuming a trail run it conducts.
    pub fn for_assistant(&self) -> bool {
        !matches!(
            self,
            Self::Add { .. }
                | Self::Retire
                | Self::Clone { .. }
                | Self::ReadMcp
                | Self::McpSignIn { .. }
                | Self::ExportProvider { .. }
                | Self::ImportProvider { .. }
                | Self::StopTrail { .. }
                | Self::ResumeTrail { .. }
        )
    }

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
            Self::Add { assistant_of: None, .. } => "add".to_owned(),
            Self::Add { assistant_of: Some(human), .. } => format!("add as the assistant of {human}"),
            Self::Retire => "retire".to_owned(),
            Self::ReadMemories { .. } => "read memories".to_owned(),
            Self::ExportProvider { .. } => "export from its provider".to_owned(),
            Self::ImportProvider { payload, .. } => {
                format!("import an export of {} bytes into its provider", payload.size)
            }
            Self::ReadSettings => "read settings".to_owned(),
            Self::ReadMcp => "read MCP servers".to_owned(),
            Self::McpSignIn { server, callback: None } => format!("sign in to MCP server {server}"),
            Self::McpSignIn { server, callback: Some(_) } => format!("complete the sign-in to MCP server {server}"),
            Self::Clone { new, .. } => format!("clone as {new}"),
            Self::StopTrail { run } => format!("stop trail run {run}"),
            Self::ResumeTrail { run, limits: None } => format!("resume trail run {run}"),
            Self::ResumeTrail { run, limits: Some(limits) } => format!("resume trail run {run} under {limits}"),
        }
    }
}

/// A change of one field of the ant's settings in an [`AdminOp::Set`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldChange {
    /// The keys leading to the field from the ant's `[[ant]]` table: `["model"]`, `["caps", "daily"]`; or, first
    /// [`HARNESSES_FIELD`], from the config file's top level: `["harnesses", "copilot", "command"]`.
    pub field: Vec<String>,
    /// The value the field must hold for the change to apply; none when the field must be absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<Value>,
    /// In place of `expected`, for a value a read of the settings hid as a secret ([`Settings::hidden`]): the digest
    /// that read gave of the value the field must hold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_digest: Option<String>,
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
            on_behalf_of: None,
            machine,
            ant,
            op,
            sent_at: Utc::now(),
            credentials: credentials.into(),
            signature: String::new(),
        }
    }

    /// The order sent by an assistant for `human`.
    #[must_use]
    pub fn on_behalf(mut self, human: Option<UserId>) -> Self {
        self.on_behalf_of = human;
        self
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
    /// ant, `sent_at` in RFC 3339 with nanoseconds and `Z`, the op as canonical JSON ([`canonical_json`]), and, only
    /// when there is one, `on_behalf_of`. Every field but the JWT, which the signing key is checked against, and the
    /// signature.
    pub fn signed(&self) -> Vec<u8> {
        let sent_at = self.sent_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let op = canonical_json(&serde_json::to_value(&self.op).expect("an op serializes"));
        let id = self.id.to_string();
        let mut lines = vec![
            ORDER_SIGNING_CONTEXT,
            &id,
            self.admin.as_str(),
            self.machine.as_str(),
            self.ant.as_str(),
            &sent_at,
            &op,
        ];
        lines.extend(self.on_behalf_of.as_ref().map(UserId::as_str));
        lines.join("\n").into_bytes()
    }

    /// The public key (`U...`) of the credentials that signed, as their JWT says before anything checks it: the key
    /// whose revocation record to look up for [`AdminOrder::verify`].
    pub fn signing_key(&self) -> Option<String> {
        verified_jwt(&self.credentials).ok().map(|(claims, _)| claims.sub)
    }

    /// Check the order, taken from `subject`, which the stream stored at `stored_at`, for the daemon of a team whose
    /// accounts are `accounts` (the issuers of the daemon's credentials): on its machine's and ant's subject, signed by
    /// the key of credentials of the admin it names that an account signed, that carry [`ADMIN_TAG`] (for an order
    /// [`AdminOrder::on_behalf_of`] a human, that make the admin the human's assistant instead), that had not expired
    /// when it was stored and whose key `revoked` (looked up in the revocation records) does not say is revoked, and
    /// sent no further ahead of `stored_at` than [`ORDER_CLOCK_SKEW`]. Whether the human's grant stands, the caller
    /// checks.
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
        let admin = match &self.on_behalf_of {
            None => verify_admin(&self.credentials, accounts, &self.signed(), &self.signature, at)?,
            Some(human) => {
                let assistant = verify_assistant_of(&self.credentials, accounts, human, at)?;
                assistant.check_signature(&self.signed(), &self.signature)?;
                assistant
            }
        };
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
    /// An assistant's order, which its machine's daemon holds until the human consents ([`AdminOrder::on_behalf_of`]).
    Held,
    Applied,
    Refused,
}

impl OrderState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Held => "held",
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
    /// The human the admin, an assistant, sent it for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
    /// What the order asked for; none when it could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<AdminOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at: Option<DateTime<Utc>>,
    /// Held, applied or refused: an order with no outcome is pending. A held order's outcome is replaced once decided.
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
            on_behalf_of: order.on_behalf_of.clone(),
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
            on_behalf_of: None,
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

/// The first key of a field naming the `[harnesses]` table of the config file, which every ant of the machine shares,
/// rather than a key of the ant's `[[ant]]` table, which has none of that name.
pub const HARNESSES_FIELD: &str = "harnesses";

/// An ant's settings as its machine's config file has them: what the outcome of an [`AdminOp::ReadSettings`] order
/// carries as its `result`, and what an admin's changes expect ([`Settings::change`]).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct Settings {
    /// The keys of the ant's `[[ant]]` table and, under [`HARNESSES_FIELD`], the file's `[harnesses]` table, if any.
    /// A string holding a secret reads `[redacted]` where the secret is.
    pub fields: Map<String, Value>,
    /// The fields holding a secret, and those holding them, with the digest of each one's value.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hidden: Vec<Hidden>,
}

/// A field of [`Settings`] whose value holds a secret, so reads otherwise than it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hidden {
    pub field: Vec<String>,
    /// What a change expecting the value the field holds carries as its `expected_digest`.
    pub digest: String,
}

impl Settings {
    /// The value `field` holds, as read; none when absent.
    pub fn get(&self, field: &[String]) -> Option<&Value> {
        let (first, rest) = field.split_first()?;
        rest.iter().try_fold(self.fields.get(first)?, |value, key| value.as_object()?.get(key))
    }

    /// The digest of the value of `field`, if it holds a secret.
    pub fn digest(&self, field: &[String]) -> Option<&str> {
        self.hidden.iter().find(|h| h.field == field).map(|h| h.digest.as_str())
    }

    /// The change of `field` to `value`, or its removal with none, expecting the value read: refused, should the
    /// field hold another by the time the order is applied.
    pub fn change(&self, field: Vec<String>, value: Option<Value>) -> FieldChange {
        match self.digest(&field) {
            Some(digest) => FieldChange { expected: None, expected_digest: Some(digest.to_owned()), field, value },
            None => FieldChange { expected: self.get(&field).cloned(), expected_digest: None, field, value },
        }
    }

    /// Every field holding something other than a table, as the keys leading to it, in order: those of tables
    /// stand for their fields, an empty one for itself.
    pub fn leaves(&self) -> Vec<(Vec<String>, &Value)> {
        fn walk<'a>(path: &mut Vec<String>, value: &'a Value, out: &mut Vec<(Vec<String>, &'a Value)>) {
            match value {
                Value::Object(map) if !map.is_empty() => {
                    for (key, value) in map {
                        path.push(key.clone());
                        walk(path, value, out);
                        path.pop();
                    }
                }
                _ => out.push((path.clone(), value)),
            }
        }
        let mut out = Vec::new();
        for (key, value) in &self.fields {
            walk(&mut vec![key.clone()], value, &mut out);
        }
        out
    }
}

/// Key in [`MACHINES_BUCKET`] of the outcome of order `id` to `ant` on `machine`: `<machine>.<ant>.<id>`, under the
/// machine's name, which its host credentials alone write, and beside its record, the key of its name alone.
pub fn admin_outcome_key(machine: &Machine, ant: &UserId, id: &Uuid) -> String {
    format!("{machine}.{ant}.{id}")
}

/// Key in [`MACHINES_BUCKET`] of the assistants' orders the daemon of `machine` holds ([`OrderState::Held`]), for it
/// to read back on start: `<machine>.held`, two tokens, apart from the machine's record and its orders' outcomes.
pub fn held_orders_key(machine: &Machine) -> String {
    format!("{machine}.held")
}

/// Whether the pre-allow rule `rule` ([`crate::pre_allow_rule`]'s second part) covers an order of `op`: `order:*` or
/// `order:<kind>` ([`AdminOp::kind`]). No tool rule, `*` among them, covers an order.
pub fn covers_order(rule: &str, op: &AdminOp) -> bool {
    rule.strip_prefix(ORDER_RULE).is_some_and(|kind| kind == "*" || kind == op.kind())
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
