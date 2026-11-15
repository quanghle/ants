//! Watching the JetStream storage the ants depend on: the hub's, and that of the machine's leaf node, which keeps the
//! outbox. A full store refuses every message: webhooks answer 503, and ants keep messages in outboxes or retry them.
//!
//! One ant of each daemon process per server ([`Watcher::server`]) reads the account's JetStream usage every
//! [`CHECK_EVERY`] ([`measure`]) and notes it for the metrics ([`crate::metrics::stored`]). Past
//! [`Settings::warn_percent`] of a store's limit, it warns the default human on a thread of its own, carrying a
//! level-0 [`EscalationReason::Storage`] escalation so that `ants webhook serve` pushes it, and names the streams
//! using most ([`largest`]). One warning goes out per crossing, whichever daemons see it and however often they
//! restart: the daemon that creates the shared note [`storage_warning_key`] warns, and the note is deleted once usage
//! falls [`HYSTERESIS`] points below the share, so that the next crossing warns again.
//!
//! A store's limit is the account's, when it has one; else, for the hub, `max_file_store` and `max_memory_store` of
//! [`Settings`], and for a leaf node the limits `ants leaf config` writes. With no limit known, the usage is only
//! reported.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use ants_proto::{Envelope, Escalation, EscalationReason, JsDomain, UserId, storage_warning_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, account::Account, stream::StorageType};
use futures::TryStreamExt as _;
use serde::{Deserialize, Deserializer};
use tokio::time::Instant;

use crate::bus;
use crate::direct_kv::{DirectKv, Lookup};

/// How often the usage is read.
pub const CHECK_EVERY: Duration = Duration::from_mins(5);
/// How long after starting the usage is first read.
const FIRST_CHECK: Duration = Duration::from_secs(30);
/// The default [`Settings::warn_percent`].
pub const DEFAULT_WARN_PERCENT: u8 = 80;
/// Points below the warning share usage falls to before a later crossing warns again.
pub const HYSTERESIS: u64 = 5;
/// The limits of the leaf node's JetStream that `ants leaf config` writes.
pub const LEAF_FILE_STORE: u64 = 1 << 30;
pub const LEAF_MEMORY_STORE: u64 = 64 << 20;
/// Streams a warning names.
const LARGEST: usize = 3;

/// The `[storage]` table of the config file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Settings {
    /// The share of a store's limit, in percent, past which the humans are warned; 0 for never.
    pub warn_percent: u8,
    /// The hub's `max_file_store` and `max_memory_store`, which its account's JetStream info does not tell.
    #[serde(deserialize_with = "size")]
    pub max_file_store: Option<u64>,
    #[serde(deserialize_with = "size")]
    pub max_memory_store: Option<u64>,
}

impl Default for Settings {
    fn default() -> Self {
        Self { warn_percent: DEFAULT_WARN_PERCENT, max_file_store: None, max_memory_store: None }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        if self.warn_percent >= 100 {
            anyhow::bail!("[storage] warn_percent must be below 100 (0 for no warning)");
        }
        Ok(())
    }
}

fn size<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Size {
        Bytes(u64),
        Text(String),
    }
    match Size::deserialize(deserializer)? {
        Size::Bytes(bytes) => Ok(Some(bytes)),
        Size::Text(text) => parse_size(&text).map(Some).map_err(serde::de::Error::custom),
    }
}

/// A size as nats-server reads `max_file_store`: bytes, or a number with `K`, `M`, `G` or `T`, alone or followed by
/// `B`, each 1024 times the one before.
pub fn parse_size(text: &str) -> Result<u64, String> {
    let invalid = || format!("invalid size {text:?} (such as 1073741824, 512MB or 10GB)");
    let digits = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let (number, unit) = text.split_at(digits);
    let number: u64 = number.parse().map_err(|_| invalid())?;
    let shift = match unit.trim_start().to_ascii_uppercase().as_str() {
        "" | "B" => 0,
        "K" | "KB" => 10,
        "M" | "MB" => 20,
        "G" | "GB" => 30,
        "T" | "TB" => 40,
        _ => return Err(invalid()),
    };
    number.checked_mul(1 << shift).ok_or_else(invalid)
}

/// `bytes` in the largest unit [`parse_size`] reads that keeps a whole number in front, to one decimal.
pub fn show_size(bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let at = (0..units.len()).rev().find(|i| bytes >> (10 * i) > 0).unwrap_or(0);
    if at == 0 {
        return format!("{bytes} B");
    }
    let value = bytes as f64 / (1u64 << (10 * at)) as f64;
    format!("{value:.1} {}", units[at])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    File,
    Memory,
}

impl Store {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Memory => "memory",
        }
    }

    /// Its key in [`Settings`], as in nats-server's jetstream block.
    pub fn setting(self) -> &'static str {
        match self {
            Self::File => "max_file_store",
            Self::Memory => "max_memory_store",
        }
    }

    fn holds(self, storage: StorageType) -> bool {
        matches!((self, storage), (Self::File, StorageType::File) | (Self::Memory, StorageType::Memory))
    }
}

/// What one JetStream store holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Usage {
    /// The domain of the JetStream that answered.
    pub domain: Option<String>,
    /// Whether it is the leaf node's own JetStream, not the hub's.
    pub leaf: bool,
    pub store: Store,
    pub used: u64,
    pub limit: Option<u64>,
}

impl Usage {
    /// The share of its limit used, in percent, when the limit is known.
    pub fn percent(&self) -> Option<u64> {
        let limit = self.limit.filter(|limit| *limit > 0)?;
        Some((u128::from(self.used) * 100 / u128::from(limit)) as u64)
    }

    /// Whether it is past `warn_percent` of its limit; never when that is 0.
    pub fn over(&self, warn_percent: u8) -> bool {
        warn_percent > 0 && self.percent().is_some_and(|p| p >= u64::from(warn_percent))
    }

    /// Whether it is far enough under `warn_percent` that passing it again warns again.
    fn under(&self, warn_percent: u8) -> bool {
        self.percent().is_some_and(|p| p + HYSTERESIS < u64::from(warn_percent))
    }

    /// What to do about it filling up.
    pub fn fix(&self) -> String {
        match self.leaf {
            true => format!(
                "check the leaf node's uplink: its outbox keeps what the hub has not taken; or raise {} in the leaf's \
                 jetstream block",
                self.store.setting()
            ),
            false => format!(
                "raise {0} in the hub's jetstream block, and in [storage] of the ants config, or remove what is no \
                 longer needed (`ants dead list` shows dead letters)",
                self.store.setting()
            ),
        }
    }

    /// The warning to the humans, naming the `largest` streams.
    pub fn warning(&self, largest: &[(String, u64)]) -> String {
        let using_most = using_most(largest).map(|streams| format!(" Using most: {streams}.")).unwrap_or_default();
        format!(
            "{self}.{using_most} Once full, JetStream refuses new messages: webhooks answer 503, and ants keep messages in outboxes or \
             retry them. To fix: {}.",
            self.fix()
        )
    }
}

/// The `largest` streams with their sizes, such as `ANTS 7.9 GB, KV_ants_sessions 12.0 MB`; None for none.
pub fn using_most(largest: &[(String, u64)]) -> Option<String> {
    let streams: Vec<String> = largest.iter().map(|(name, bytes)| format!("{name} {}", show_size(*bytes))).collect();
    (!streams.is_empty()).then(|| streams.join(", "))
}

impl fmt::Display for Usage {
    /// Such as `JetStream file store of the hub (domain hub): 8.0 GB of 10.0 GB, 80%`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let whose = if self.leaf { "the leaf node" } else { "the hub" };
        write!(f, "JetStream {} store of {whose}", self.store.as_str())?;
        if let Some(domain) = &self.domain {
            write!(f, " (domain {domain})")?;
        }
        write!(f, ": {}", show_size(self.used))?;
        match (self.limit, self.percent()) {
            (Some(limit), Some(percent)) => write!(f, " of {}, {percent}%", show_size(limit)),
            _ => write!(f, ", limit unknown"),
        }
    }
}

/// The usage of the hub's JetStream, through `js` (of `hub_domain`), and of the leaf node's the client of `js`
/// connects to, when it has one: the JetStream answering without a domain, in another domain. Fails when neither
/// answers.
pub async fn measure(
    js: &jetstream::Context,
    hub_domain: Option<&JsDomain>,
    settings: &Settings,
) -> Result<Vec<Usage>> {
    let plain = jetstream::new(js.client());
    let (hub, own) = tokio::join!(js.query_account(), plain.query_account());
    let mut found = Vec::new();
    let hub = hub.map(|account| found.extend(usages(&account, false, settings)));
    if let Ok(account) = own
        && domain(&account).is_some_and(|d| Some(d) != hub_domain.map(JsDomain::as_str))
    {
        found.extend(usages(&account, true, settings));
    }
    match hub {
        Err(e) if found.is_empty() => Err(e).context("reading the account's JetStream info"),
        _ => Ok(found),
    }
}

fn domain(account: &Account) -> Option<&str> {
    account.domain.as_deref().filter(|d| !d.is_empty())
}

fn usages(account: &Account, leaf: bool, settings: &Settings) -> Vec<Usage> {
    let limit = |own: Option<i64>, leaf_limit: u64, configured: Option<u64>| {
        let own = own.and_then(|n| u64::try_from(n).ok()).filter(|n| *n > 0);
        own.or(if leaf { Some(leaf_limit) } else { configured })
    };
    let usage = |store, used, limit| Usage { domain: domain(account).map(str::to_owned), leaf, store, used, limit };
    let limits = &account.limits;
    let file = usage(Store::File, account.storage, limit(limits.max_storage, LEAF_FILE_STORE, settings.max_file_store));
    let memory =
        usage(Store::Memory, account.memory, limit(limits.max_memory, LEAF_MEMORY_STORE, settings.max_memory_store));
    let mut found = vec![file];
    if memory.used > 0 || memory.limit.is_some() {
        found.push(memory);
    }
    found
}

/// The `n` streams of `usage`'s store holding the most bytes, largest first, listed through `js` as [`measure`] got
/// it.
pub async fn largest(js: &jetstream::Context, usage: &Usage, n: usize) -> Result<Vec<(String, u64)>> {
    let js = if usage.leaf { jetstream::new(js.client()) } else { js.clone() };
    let mut sizes = Vec::new();
    let mut streams = js.streams();
    while let Some(info) = streams.try_next().await.context("listing the streams")? {
        if usage.store.holds(info.config.storage) {
            sizes.push((info.config.name, info.state.bytes));
        }
    }
    sizes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    sizes.truncate(n);
    Ok(sizes)
}

/// When the ants of this process last read the usage, by [`Watcher::server`].
static CHECKED: Mutex<BTreeMap<String, Instant>> = Mutex::new(BTreeMap::new());

/// Whether this ant reads the usage of `server` now: none of the process's ants did in the last half `every`.
fn claim(server: &str, every: Duration) -> bool {
    let now = Instant::now();
    let mut checked = CHECKED.lock().unwrap();
    if checked.get(server).is_some_and(|at| now.duration_since(*at) < every / 2) {
        return false;
    }
    checked.insert(server.to_owned(), now);
    true
}

/// An ant's daemon watching the storage.
pub struct Watcher {
    pub js: jetstream::Context,
    pub hub_domain: Option<JsDomain>,
    /// The sessions bucket, which keeps the shared notes of the warnings given.
    pub notes: DirectKv,
    pub ant: UserId,
    /// Who gets the warnings.
    pub human: UserId,
    pub settings: Settings,
    /// The server the ant connects to and its domain, the same for the ants of the process that see the same stores.
    pub server: String,
}

impl Watcher {
    /// Check every `every`, while no other ant of the process does.
    pub async fn watch(&self, every: Duration) {
        let mut tick = tokio::time::interval_at(Instant::now() + FIRST_CHECK.min(every), every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if claim(&self.server, every) {
                self.check().await;
            }
        }
    }

    /// Read the usage, note it for the metrics, and warn of, or forget, each crossing.
    pub async fn check(&self) {
        let usages = match measure(&self.js, self.hub_domain.as_ref(), &self.settings).await {
            Ok(usages) => usages,
            Err(e) => {
                tracing::debug!(error = format!("{e:#}"), "not watching the JetStream storage");
                return;
            }
        };
        for usage in &usages {
            crate::metrics::stored(usage.domain.as_deref(), usage.store.as_str(), usage.used, usage.limit);
            if let Err(e) = self.settle(usage).await {
                tracing::warn!(error = format!("{e:#}"), "{usage}; warning a human of it next time");
            }
        }
    }

    async fn settle(&self, usage: &Usage) -> Result<()> {
        let percent = self.settings.warn_percent;
        let key = storage_warning_key(usage.domain.as_deref(), usage.store.as_str());
        if usage.over(percent) {
            if let (Lookup::Value(_), _) = self.notes.lookup_last(&key).await? {
                return Ok(());
            }
            let note = serde_json::json!({ "by": self.ant, "percent": usage.percent(), "at": chrono::Utc::now() });
            if !self.notes.create(&key, serde_json::to_vec(&note)?).await? {
                return Ok(());
            }
            tracing::warn!("{usage}; warning {}", self.human);
            if let Err(e) = self.warn(usage).await {
                let _ = self.notes.delete(&key).await;
                return Err(e);
            }
        } else if usage.under(percent) && matches!(self.notes.lookup(&key).await?, Lookup::Value(_)) {
            tracing::info!("{usage}; back under {percent}%");
            self.notes.delete(&key).await?;
        }
        Ok(())
    }

    async fn warn(&self, usage: &Usage) -> Result<()> {
        let largest = largest(&self.js, usage, LARGEST).await.unwrap_or_else(|e| {
            tracing::debug!(error = format!("{e:#}"), "naming the largest streams");
            Vec::new()
        });
        let notice = Envelope::new(self.ant.clone(), self.human.clone(), usage.warning(&largest));
        let escalation = Escalation::new(self.ant.clone(), notice.thread_id.clone(), EscalationReason::Storage);
        let notice = notice.with_escalation(Some(escalation));
        bus::publish_json(&self.js, notice.subject(), &notice, None).await.context("warning a human")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_read_as_nats_server_reads_them_and_show_back() {
        assert_eq!(parse_size("1073741824"), Ok(1 << 30));
        assert_eq!(parse_size("64MB"), Ok(64 << 20));
        assert_eq!(parse_size("10g"), Ok(10 << 30));
        assert_eq!(parse_size("2 TB"), Ok(2 << 40));
        for bad in ["", "GB", "10 parsecs", "-1", "99999999999T"] {
            assert!(parse_size(bad).is_err(), "{bad:?}");
        }
        assert_eq!(show_size(512), "512 B");
        assert_eq!(show_size(64 << 20), "64.0 MB");
        assert_eq!(show_size((10 << 30) + (1 << 29)), "10.5 GB");
    }

    #[test]
    fn a_limit_comes_from_the_account_else_the_config_or_the_leaf_s_defaults() {
        let account = |max_storage: Option<i64>, domain: &str| Account {
            storage: 900,
            memory: 0,
            domain: Some(domain.to_owned()).filter(|d| !d.is_empty()),
            limits: async_nats::jetstream::account::Limits { max_storage, ..Default::default() },
            ..Default::default()
        };
        let configured = Settings { max_file_store: Some(1000), ..Settings::default() };
        let hub = usages(&account(None, "hub"), false, &configured);
        assert_eq!(hub.len(), 1, "no memory store in use or limited: {hub:?}");
        assert_eq!((hub[0].limit, hub[0].percent()), (Some(1000), Some(90)));
        assert_eq!(usages(&account(Some(9000), "hub"), false, &configured)[0].percent(), Some(10), "the account's");
        assert_eq!(usages(&account(None, ""), false, &Settings::default())[0].limit, None, "unknown");
        let leaf = usages(&account(None, "m1"), true, &configured);
        assert_eq!(leaf.iter().map(|u| u.limit).collect::<Vec<_>>(), [Some(LEAF_FILE_STORE), Some(LEAF_MEMORY_STORE)]);
        assert_eq!(leaf[0].domain.as_deref(), Some("m1"));
    }

    #[test]
    fn a_warning_comes_past_the_share_and_again_only_after_falling_well_under_it() {
        let usage = |used| Usage { domain: None, leaf: false, store: Store::File, used, limit: Some(100) };
        assert!(usage(80).over(80) && !usage(79).over(80) && !usage(100).over(0));
        assert!(!usage(76).under(80) && !usage(75).under(80) && usage(74).under(80));
        assert!(!Usage { limit: None, ..usage(100) }.over(80));
        let warning = usage(85).warning(&[("ANTS".to_owned(), 80), ("KV_ants_sessions".to_owned(), 5)]);
        assert!(warning.starts_with("JetStream file store of the hub: 85 B of 100 B, 85%. Using most: ANTS 80 B,"));
        assert!(warning.contains("raise max_file_store"), "{warning}");
        let leaf = Usage { domain: Some("m1".to_owned()), leaf: true, ..usage(85) };
        assert!(leaf.warning(&[]).contains("of the leaf node (domain m1)"));
        assert!(leaf.fix().contains("uplink"));
    }
}
