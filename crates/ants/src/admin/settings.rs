//! An ant's settings, as the config file of the machine serving it has them: read with an admin order
//! ([`AdminOp::ReadSettings`]) whose outcome every human reads, else as last read; and changed with one
//! ([`AdminOp::Set`]) expecting the values read, so the daemon refuses it should a field have changed meanwhile
//! (`ants ant show`, `ants ant set`, the settings pane of the ant view of `ants chat`). The ant's shared settings
//! ([`SHARED_FIELDS`]), the same on every machine, are read from their record on the bus ([`shared_of`]), verified as
//! daemons verify it, and a change of one expects the record's value; a record daemons would ignore is shown marked
//! and otherwise left out.

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use ants_daemon::config::edit;
use ants_daemon::profile::shared::{Bucket, Found};
use ants_daemon::records::direct_kv::DirectKv;
#[cfg(doc)]
use ants_proto::SHARED_FIELDS;
use ants_proto::{
    ADMIN_TAG, AdminOp, DEVICES_BUCKET, IDENTITIES_BUCKET, Machine, OrderState, Settings, SharedSettings, UserId, Uuid,
    is_shared_field,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::admin::orders::{self, Admin, Listed, Sent};
use crate::auth;
use crate::colony::{short, since};
use crate::connection::Nats;

/// How long a read of the settings waits for the daemon of the ant's machine before taking the last read.
pub const READ_WAIT: Duration = Duration::from_secs(5);

/// How many of the latest admin orders to an ant `ants ant show` lists.
const ORDERS_SHOWN: usize = 10;

/// How long `ants ant set` waits for the outcome of its order.
pub const SET_WAIT: Duration = Duration::from_secs(10);

/// A change asked for as `KEY=VALUE`: the field `KEY` names, dotted as a TOML key (`model`, `caps.daily`,
/// `env.RUST_LOG`, `harnesses.copilot.command`), set to `VALUE` read as a TOML value, else as a string; or, with
/// nothing after `=`, removed.
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub field: Vec<String>,
    /// None to remove the field.
    pub value: Option<Value>,
}

impl FromStr for Assignment {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Self> {
        let at = equals(text).with_context(|| format!("{text:?} sets nothing: KEY=VALUE, or KEY= to remove"))?;
        let (key, value) = (text[..at].trim(), text[at + 1..].trim());
        let field = edit::parse_field(key).with_context(|| format!("{key:?} is no setting's key"))?;
        let value = match value {
            "" => None,
            value => Some(match edit::parse_value(value) {
                Ok(value) => value,
                // A bare word reads as a string: `model=gpt-5`.
                Err(_) if !value.starts_with(['"', '\'', '[', '{']) => Value::String(value.to_owned()),
                Err(e) => return Err(e).with_context(|| format!("{value} is no TOML value")),
            }),
        };
        Ok(Self { field, value })
    }
}

/// Where the first `=` outside quotes is.
fn equals(text: &str) -> Option<usize> {
    let mut quote = None;
    for (at, c) in text.char_indices() {
        match (quote, c) {
            (None, '"' | '\'') => quote = Some(c),
            (Some(q), _) if c == q => quote = None,
            (None, '=') => return Some(at),
            _ => {}
        }
    }
    None
}

/// The order making `assignments` to the settings `read` has, each expecting the value read: that of the ant's
/// `shared` settings for one of those, when it has a record of them. The one place changes asked for become an order.
pub fn order(read: &Settings, shared: Option<&SharedSettings>, assignments: &[Assignment]) -> AdminOp {
    let changes = assignments
        .iter()
        .map(|a| match (a.field.as_slice(), shared) {
            ([key], Some(shared)) if is_shared_field(key) => ants_proto::FieldChange {
                field: a.field.clone(),
                expected: shared.fields.get(key).cloned(),
                expected_digest: None,
                value: a.value.clone(),
            },
            _ => read.change(a.field.clone(), a.value.clone()),
        })
        .collect();
    AdminOp::Set { changes }
}

/// An ant's shared settings as read on the bus.
#[derive(Debug, Clone, PartialEq)]
pub enum SharedRecord {
    /// Verified as daemons verify it: the settings every machine serves.
    Verified(SharedSettings),
    /// A record daemons ignore, and why: not signed by credentials of the ant, signed by revoked ones, or put back
    /// after it was written, until a machine holding the ant's credentials writes it anew.
    Unverified(SharedSettings, String),
}

impl SharedRecord {
    pub fn record(&self) -> &SharedSettings {
        match self {
            Self::Verified(record) | Self::Unverified(record, _) => record,
        }
    }

    pub fn verified(&self) -> Option<&SharedSettings> {
        match self {
            Self::Verified(record) => Some(record),
            Self::Unverified(..) => None,
        }
    }

    /// Why daemons ignore the record, if they do.
    pub fn unverified(&self) -> Option<&str> {
        match self {
            Self::Verified(_) => None,
            Self::Unverified(_, why) => Some(why),
        }
    }
}

/// The shared settings of `ant`, read with a human's credentials and verified against their team account as daemons
/// verify them ([`ants_daemon::profile::shared::read`]); none while it has no record of them.
pub async fn shared_of(nats: &Nats<'_>, js: &jetstream::Context, ant: &UserId) -> Result<Option<SharedRecord>> {
    let identities = DirectKv::whole(js, nats.domain, IDENTITIES_BUCKET).await?;
    let devices = DirectKv::new(js, nats.domain, DEVICES_BUCKET).await?;
    let accounts: Vec<String> = nats.account.iter().cloned().collect();
    let bucket = Bucket { identities: &identities, devices: &devices, accounts: &accounts };
    Ok(match ants_daemon::profile::shared::read(bucket, ant).await.context("reading the ant's shared settings")? {
        Found::Record(record, _) => Some(SharedRecord::Verified(record)),
        Found::Unverified(record, why, _) => Some(SharedRecord::Unverified(*record, why.to_string())),
        Found::Malformed(why, _) => bail!("the ant's shared settings: {why}"),
        Found::None(_) | Found::NoBucket => None,
    })
}

/// One `KEY = VALUE` line per field of `shared`.
pub fn shared_lines(shared: &SharedSettings) -> Vec<String> {
    shared.fields.iter().map(|(key, value)| format!("{key} = {}", edit::toml_text(value))).collect()
}

/// The settings as a read order's outcome carries them.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Read {
    /// The machine whose config file has them.
    pub machine: Machine,
    /// The read order.
    pub order: Uuid,
    pub read_at: DateTime<Utc>,
    pub settings: Settings,
}

impl Read {
    /// The last read of the settings among `orders`, oldest first, that the daemon applied.
    pub fn last(orders: &[Listed]) -> Option<Self> {
        orders.iter().rev().find_map(|o| {
            if o.op != Some(AdminOp::ReadSettings) || o.state != OrderState::Applied {
                return None;
            }
            let settings = serde_json::from_value(o.result.clone()?).ok()?;
            Some(Self { machine: o.machine.clone(), order: o.id, read_at: o.decided_at?, settings })
        })
    }

    /// One `KEY = VALUE` line per field, as TOML has them, `(secret)` after those hiding one.
    pub fn lines(&self) -> Vec<String> {
        self.settings
            .leaves()
            .into_iter()
            .map(|(field, value)| {
                let secret = if self.settings.digest(&field).is_some() { "  (secret)" } else { "" };
                format!("{} = {}{secret}", edit::field_text(&field), edit::toml_text(value))
            })
            .collect()
    }
}

/// The settings read and the orders to the ant.
#[derive(Debug, Clone, Default)]
pub struct Reading {
    /// Afresh, or with `stale` set, as last read; none when never read.
    pub read: Option<Read>,
    /// Why the settings are not read afresh, if they are not.
    pub stale: Option<String>,
    /// The orders to the ant, oldest first.
    pub orders: Vec<Listed>,
    /// The ant's shared settings; none while it has none, or they could not be read.
    pub shared: Option<Box<SharedRecord>>,
}

impl Reading {
    /// The ant's shared settings, verified: those every machine serves.
    pub fn verified_shared(&self) -> Option<&SharedSettings> {
        self.shared.as_deref().and_then(SharedRecord::verified)
    }
}

/// The settings and orders sections of `ants ant show`, as lines.
pub fn sections(ant: &UserId, reading: &Reading, now: DateTime<Utc>) -> Vec<String> {
    let mut out = vec![String::new(), format!("settings (`ants ant set {ant} KEY=VALUE...`):")];
    if let Some(shared) = reading.shared.as_deref() {
        let record = shared.record();
        let ago = short(since(record.updated_at, now));
        let by = record.by.as_ref().map_or_else(String::new, |by| format!(" by {by}"));
        let set = format!("set{by} on {} {ago} ago", record.machine);
        out.push(match shared.unverified() {
            None => format!("  shared, the ant's on every machine: {set}"),
            Some(why) => format!("  shared, NOT VERIFIED ({why}): {set}; daemons ignore it"),
        });
        out.extend(shared_lines(record).into_iter().map(|line| format!("  {line}")));
    }
    match &reading.read {
        Some(read) => {
            let ago = short(since(read.read_at, now));
            out.push(format!("  as config.toml on {} has them, read {ago} ago", read.machine));
            let shared = reading.verified_shared().is_some();
            for line in read.lines() {
                let key = line.split([' ', '.']).next().unwrap_or_default();
                let ignored = if shared && is_shared_field(key) { "  (ignored: shared)" } else { "" };
                out.push(format!("  {line}{ignored}"));
            }
        }
        None => out.push("  never read".to_owned()),
    }
    out.extend(reading.stale.iter().map(|stale| format!("  {stale}")));
    out.extend([String::new(), format!("admin orders (`ants ant orders {ant}`):")]);
    let shown = &reading.orders[reading.orders.len().saturating_sub(ORDERS_SHOWN)..];
    if shown.is_empty() {
        out.push("  none".to_owned());
    } else {
        out.extend(orders::table(shown, now).lines().map(|line| format!("  {line}")));
    }
    if reading.orders.len() > shown.len() {
        out.push(format!("  and {} earlier", reading.orders.len() - shown.len()));
    }
    out
}

/// Whether the credentials of `nats` sign admin orders: a human's device's, carrying the admin role.
pub fn may_order(nats: &Nats<'_>) -> bool {
    match nats.creds {
        Some(creds) if nats.me.role.is_human() && nats.device.is_some() => admin_tagged(creds),
        _ => false,
    }
}

fn admin_tagged(creds: &Path) -> bool {
    let Ok((jwt, _)) = auth::read_creds(creds) else { return false };
    let Ok((_, claims)) = ants_proto::verified_jwt(&jwt) else { return false };
    claims["nats"]["tags"].as_array().is_some_and(|tags| tags.iter().any(|tag| tag.as_str() == Some(ADMIN_TAG)))
}

/// Read `ant`'s settings: with an admin's credentials, afresh, waiting up to `wait` for the daemon of its machine;
/// else, or when that daemon does not answer in time, refuses or is away, as last read. A read still pending is not
/// sent again: while none was ever applied, its outcome is waited for up to `wait`.
pub async fn read(nats: &Nats<'_>, js: &jetstream::Context, ant: &UserId, wait: Duration) -> Result<Reading> {
    let mut orders = orders::orders_of(js, ant).await?;
    let now: DateTime<Utc> = SystemTime::now().into();
    let waiting = orders.iter().rev().find(|o| o.state == OrderState::Pending && o.op == Some(AdminOp::ReadSettings));
    let stale = if !may_order(nats) {
        Some("as last read: an admin's credentials read them afresh".to_owned())
    } else if let Some(waiting) = waiting.filter(|_| Read::last(&orders).is_some()) {
        let ago = waiting.sent_at.map_or_else(String::new, |at| format!(" sent {} ago", short(since(at, now))));
        Some(format!("a read{ago} waits for the daemon of {}", waiting.machine))
    } else if let Some(waiting) = waiting {
        let (id, machine) = (waiting.id, waiting.machine.clone());
        orders = decided(js, ant, id, wait).await?;
        match orders.iter().find(|o| o.id == id) {
            Some(o) if o.state == OrderState::Applied => None,
            Some(o) if o.state != OrderState::Pending => {
                Some(format!("the daemon of {machine} refused the read: {}", orders::reason(&o.reason)))
            }
            _ => Some(format!("the read waits for the daemon of {machine}")),
        }
    } else {
        let sent = async { Admin::of(nats)?.send(js, None, ant, AdminOp::ReadSettings, wait).await };
        match sent.await {
            Ok(sent) => {
                orders = orders::orders_of(js, ant).await?;
                match sent {
                    Sent::Applied(..) => None,
                    Sent::Refused(_, o) => {
                        Some(format!("the daemon of {} refused the read: {}", o.machine, orders::reason(&o.reason)))
                    }
                    Sent::Pending(order) => Some(format!("the read waits for the daemon of {}", order.machine)),
                }
            }
            Err(e) => Some(format!("{e:#}")),
        }
    };
    let shared = shared_of(nats, js, ant)
        .await
        .inspect_err(|e| tracing::debug!("no shared settings: {e:#}"))
        .ok()
        .flatten()
        .map(Box::new);
    Ok(Reading { read: Read::last(&orders), stale, orders, shared })
}

/// The orders to `ant` once the order `id` is decided, or as they stand after `wait`.
async fn decided(js: &jetstream::Context, ant: &UserId, id: Uuid, wait: Duration) -> Result<Vec<Listed>> {
    let started = tokio::time::Instant::now();
    loop {
        let orders = orders::orders_of(js, ant).await?;
        let pending = orders.iter().any(|o| o.id == id && o.state == OrderState::Pending);
        if !pending || started.elapsed() >= wait {
            return Ok(orders);
        }
        tokio::time::sleep(orders::POLL).await;
    }
}

/// `ants ant set ANT KEY=VALUE...`: read `ant`'s settings, send the order of `assignments` expecting the values read,
/// and wait up to `wait` for its outcome.
pub async fn set(nats: &Nats<'_>, ant: &UserId, assignments: &[Assignment], wait: Duration) -> Result<()> {
    if !may_order(nats) {
        bail!("`ants ant set` needs a human device's admin credentials (`ants auth user NAME --human --admin`)");
    }
    let (_, js) = nats.connect("ants-ant-set").await?;
    let reading = read(nats, &js, ant, READ_WAIT).await?;
    let Some(read) = reading.read else {
        let why = reading.stale.unwrap_or_default();
        bail!("{ant}'s settings were never read, so no change can expect their values: {why}");
    };
    if let Some(stale) = &reading.stale {
        let ago = short(since(read.read_at, SystemTime::now().into()));
        eprintln!("expecting the settings as read {ago} ago: {stale}");
    }
    if let Some(why) = reading.shared.as_deref().and_then(SharedRecord::unverified) {
        eprintln!("ignoring {ant}'s shared settings, not verified ({why}): expecting the values of its config");
    }
    let op = order(&read.settings, reading.shared.as_deref().and_then(SharedRecord::verified), assignments);
    let sent = Admin::of(nats)?.send(&js, Some(read.machine.clone()), ant, op, wait).await?;
    let (Sent::Pending(order) | Sent::Applied(order, _) | Sent::Refused(order, _)) = &sent;
    println!("order {} sent to the daemon of {}", order.id, read.machine);
    match sent {
        Sent::Applied(..) => println!("applied"),
        Sent::Refused(_, o) => bail!("refused: {}", orders::reason(&o.reason)),
        Sent::Pending(_) if wait.is_zero() => {}
        Sent::Pending(_) => {
            println!("pending: the daemon of {} has not taken it yet; `ants ant orders {ant}` tells", read.machine);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
