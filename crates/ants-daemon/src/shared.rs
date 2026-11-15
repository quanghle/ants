//! An ant's shared settings ([`SharedSettings`]): its role settings and core memories ([`SHARED_FIELDS`]), kept in
//! [`IDENTITIES_BUCKET`] so the ant is the same on every machine. A machine's host connection ([`crate::host`]) reads
//! those of the ants its daemon serves every [`POLL_EVERY`], creating those still missing from the machine's config,
//! and keeps them in a [`Shared`]; the daemon serves each ant with them in place of its config's ([`Shared::overlaid`])
//! and reloads when they change. The fields the machine's config sets otherwise are its drift, which the ant's roster
//! entry and the machine's record report.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(doc)]
use ants_proto::{IDENTITIES_BUCKET, SHARED_FIELDS};
use ants_proto::{Machine, SharedSettings, UserId};
use anyhow::{Context as _, Result};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::config::Ant;
use crate::direct_kv::{DirectKv, Lookup};
use crate::sync::MutexExt as _;

/// How often the host connection reads the shared settings of the machine's ants.
pub const POLL_EVERY: Duration = Duration::from_secs(10);

/// The shared settings, typed as an ant's table holds them: what a record must parse as to be applied.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Roles {
    pub whoami: Option<String>,
    pub memory: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub model: Option<String>,
    pub reasoning_effort: Option<String>,
    pub context: Option<String>,
    #[serde(default)]
    pub fallback: Vec<String>,
    #[serde(default)]
    pub allow: Vec<String>,
    #[serde(default)]
    pub escalate_to: Vec<UserId>,
}

impl Roles {
    /// The settings `fields` hold: each a key of [`SHARED_FIELDS`] with a value of its type.
    pub fn of(fields: &Map<String, Value>) -> Result<Self> {
        serde_json::from_value(Value::Object(fields.clone())).context("shared settings")
    }

    /// `ant` with these settings in place of its own.
    pub fn overlay(self, ant: Ant) -> Ant {
        Ant {
            whoami: self.whoami,
            memory: self.memory,
            capabilities: self.capabilities,
            model: self.model,
            reasoning_effort: self.reasoning_effort,
            context: self.context,
            fallback: self.fallback,
            allow: self.allow,
            escalate_to: self.escalate_to,
            ..ant
        }
    }
}

/// The shared settings `ant` sets, as its machine's config gives them: those holding no default.
pub fn local_fields(ant: &Ant) -> Map<String, Value> {
    let mut fields = Map::new();
    let texts = [
        ("whoami", &ant.whoami),
        ("memory", &ant.memory),
        ("model", &ant.model),
        ("reasoning_effort", &ant.reasoning_effort),
        ("context", &ant.context),
    ];
    for (key, text) in texts {
        if let Some(text) = text {
            fields.insert(key.to_owned(), Value::from(text.as_str()));
        }
    }
    let escalate_to: Vec<String> = ant.escalate_to.iter().map(ToString::to_string).collect();
    for (key, list) in [
        ("capabilities", &ant.capabilities),
        ("fallback", &ant.fallback),
        ("allow", &ant.allow),
        ("escalate_to", &escalate_to),
    ] {
        if !list.is_empty() {
            fields.insert(key.to_owned(), Value::from(list.clone()));
        }
    }
    fields
}

/// The shared settings of the ants a daemon serves, as its host connection last read them, and the machine's own
/// values of them; cloned, the same.
#[derive(Debug, Clone)]
pub struct Shared(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    /// Bumped whenever a record read differs from the one kept.
    version: watch::Sender<u64>,
    /// Whether the host connection read them once.
    polled: watch::Sender<bool>,
}

#[derive(Debug, Default)]
struct State {
    records: BTreeMap<UserId, SharedSettings>,
    /// The shared settings each ant's table in the machine's config sets ([`local_fields`]).
    local: BTreeMap<UserId, Map<String, Value>>,
}

impl Default for Shared {
    fn default() -> Self {
        Self(Arc::new(Inner { state: Mutex::default(), version: watch::channel(0).0, polled: watch::channel(false).0 }))
    }
}

impl Shared {
    /// What changes whenever a record kept changes.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.version.subscribe()
    }

    pub fn record(&self, ant: &UserId) -> Option<SharedSettings> {
        self.0.state.locked().records.get(ant).cloned()
    }

    /// Keep `record`, telling subscribers when it differs from the one kept.
    pub fn store(&self, record: SharedSettings) {
        let mut state = self.0.state.locked();
        if state.records.get(&record.ant) == Some(&record) {
            return;
        }
        state.records.insert(record.ant.clone(), record);
        drop(state);
        self.0.version.send_modify(|v| *v += 1);
    }

    /// Forget the record of `ant`, which has none any more, telling subscribers when one was kept.
    pub fn forget(&self, ant: &UserId) {
        if self.0.state.locked().records.remove(ant).is_some() {
            self.0.version.send_modify(|v| *v += 1);
        }
    }

    /// Note the shared settings of `ants`, as the machine's config lists them, the built-in messenger left out: those
    /// the host connection creates missing records with, and tells drift by.
    pub fn listed(&self, ants: &[Ant]) {
        let local = ants.iter().filter(|ant| !ant.builtin).map(|ant| (ant.name.clone(), local_fields(ant))).collect();
        self.0.state.locked().local = local;
    }

    /// The shared settings the machine's config sets for `ant`; none for an ant it does not list.
    pub fn local(&self, ant: &UserId) -> Option<Map<String, Value>> {
        self.0.state.locked().local.get(ant).cloned()
    }

    /// `ant` with its shared settings in place of its own, and the fields its own set otherwise (its drift); none
    /// while it has no shared settings, or ones that do not parse, warned.
    pub fn overlaid(&self, ant: &Ant) -> Option<(Ant, Vec<String>)> {
        let record = self.record(&ant.name)?;
        match Roles::of(&record.fields) {
            Ok(roles) => Some((roles.overlay(ant.clone()), record.drift(&local_fields(ant)))),
            Err(e) => {
                tracing::warn!(
                    ant = %ant.name, error = format!("{e:#}"),
                    "ignoring the ant's shared settings, which do not parse; serving it with its config's"
                );
                None
            }
        }
    }

    /// By ant listed, the fields the machine's config sets otherwise than the ant's shared settings.
    pub fn drift(&self) -> BTreeMap<UserId, Vec<String>> {
        let state = self.0.state.locked();
        state
            .local
            .iter()
            .filter_map(|(ant, local)| Some((ant.clone(), state.records.get(ant)?.drift(local))))
            .filter(|(_, drift)| !drift.is_empty())
            .collect()
    }

    /// Note that the host connection read the records once.
    pub fn polled(&self) {
        self.0.polled.send_replace(true);
    }

    /// Wait up to `timeout` for the host connection's first read; whether it came.
    pub async fn wait_polled(&self, timeout: Duration) -> bool {
        let mut polled = self.0.polled.subscribe();
        tokio::time::timeout(timeout, polled.wait_for(|polled| *polled)).await.is_ok_and(|r| r.is_ok())
    }
}

/// What a read of an ant's shared settings found, with the stream sequence of the key's last message (0 for none),
/// which a compare-and-set expects.
pub enum Found {
    Record(SharedSettings, u64),
    /// A value that is no record of the ant's shared settings: why.
    Malformed(String, u64),
    None(u64),
    NoBucket,
}

/// The shared settings of `ant` in `kv`.
pub async fn read(kv: &DirectKv, ant: &UserId) -> Result<Found> {
    let key = SharedSettings::key_of(ant);
    Ok(match kv.lookup_last(&key).await? {
        (Lookup::Value(bytes), last) => match serde_json::from_slice::<SharedSettings>(&bytes) {
            Ok(record) => match record.check_key(&key) {
                Ok(()) => Found::Record(record, last),
                Err(e) => Found::Malformed(e.to_string(), last),
            },
            Err(e) => Found::Malformed(format!("not shared settings: {e}"), last),
        },
        (Lookup::NoValue, last) => Found::None(last),
        (Lookup::NoBucket, _) => Found::NoBucket,
    })
}

/// Read the shared settings of `ants` into `shared`, creating as `machine` those missing from the machine's config.
/// Failures are logged; the next poll tries again.
pub async fn refresh(kv: &DirectKv, machine: &Machine, shared: &Shared, ants: &[UserId]) {
    for ant in ants.iter().filter(|ant| **ant != UserId::messenger()) {
        if let Err(e) = refresh_one(kv, machine, shared, ant).await {
            tracing::warn!(%ant, error = format!("{e:#}"), "reading the ant's shared settings failed");
        }
    }
}

async fn refresh_one(kv: &DirectKv, machine: &Machine, shared: &Shared, ant: &UserId) -> Result<()> {
    for _ in 0..2 {
        match read(kv, ant).await? {
            Found::Record(record, _) => {
                shared.store(record);
                return Ok(());
            }
            Found::NoBucket => return Ok(()),
            Found::Malformed(why, _) => {
                shared.forget(ant);
                tracing::warn!(%ant, why, "ignoring the ant's shared settings; an admin's settings order writes them anew");
                return Ok(());
            }
            Found::None(_) => {
                shared.forget(ant);
                let Some(fields) = shared.local(ant) else { return Ok(()) };
                let record = SharedSettings {
                    ant: ant.clone(),
                    fields,
                    machine: machine.clone(),
                    by: None,
                    updated_at: chrono::Utc::now(),
                };
                if kv.create(record.key(), serde_json::to_vec(&record)?).await? {
                    tracing::info!(%ant, "created the ant's shared settings from this machine's config");
                    shared.store(record);
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn ant(name: &str) -> Ant {
        Ant::new(name.parse().unwrap(), "/".into())
    }

    fn record(ant: &str, fields: Value) -> SharedSettings {
        SharedSettings {
            ant: ant.parse().unwrap(),
            fields: fields.as_object().unwrap().clone(),
            machine: "m1".parse().unwrap(),
            by: None,
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn the_shared_settings_replace_the_config_s_and_tell_drift() {
        let mut dev_a = ant("dev-a");
        dev_a.model = Some("gpt-4".to_owned());
        dev_a.whoami = Some("a tester".to_owned());
        dev_a.tasks = Some("bd".to_owned());
        dev_a.capabilities = vec!["rust".to_owned()];
        assert_eq!(
            Value::Object(local_fields(&dev_a)),
            json!({ "model": "gpt-4", "whoami": "a tester", "capabilities": ["rust"] }),
            "tasks stays the machine's"
        );
        let shared = Shared::default();
        let mut changes = shared.subscribe();
        shared.listed(std::slice::from_ref(&dev_a));
        assert!(shared.overlaid(&dev_a).is_none(), "no record: the config holds");
        assert!(!changes.has_changed().unwrap(), "listing ants changes no record");

        let fields = json!({ "model": "gpt-5", "capabilities": ["rust"], "escalate_to": ["lead"] });
        shared.store(record("dev-a", fields.clone()));
        assert!(changes.has_changed().unwrap());
        changes.mark_unchanged();
        let (overlaid, drift) = shared.overlaid(&dev_a).unwrap();
        assert_eq!(overlaid.model.as_deref(), Some("gpt-5"));
        assert_eq!(overlaid.whoami, None, "a field the record leaves out takes its default");
        assert_eq!(overlaid.tasks.as_deref(), Some("bd"));
        assert_eq!(overlaid.escalate_to, ["lead".parse::<UserId>().unwrap()]);
        assert_eq!(drift, ["model", "whoami"]);
        assert_eq!(shared.drift(), BTreeMap::from([(dev_a.name.clone(), vec!["model".into(), "whoami".into()])]));

        let mut same = record("dev-a", fields);
        same.updated_at = shared.record(&dev_a.name).unwrap().updated_at;
        shared.store(same);
        assert!(!changes.has_changed().unwrap(), "the same record read again changes nothing");

        shared.store(record("dev-a", json!({ "model": 5 })));
        assert!(shared.overlaid(&dev_a).is_none(), "a record that does not parse is ignored");
        assert!(Roles::of(json!({ "workdir": "/" }).as_object().unwrap()).is_err());
    }
}
