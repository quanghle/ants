//! An ant's shared settings ([`SharedSettings`]): its role settings and core memories ([`SHARED_FIELDS`]), kept in
//! [`IDENTITIES_BUCKET`] so the ant is the same on every machine. A machine's host connection ([`crate::host`]) reads
//! those of the ants its daemon serves every [`POLL_EVERY`], creating those still missing from the machine's config,
//! and keeps them in a [`Shared`]; the daemon serves each ant with them in place of its config's ([`Shared::overlaid`])
//! and reloads when they change. The fields the machine's config sets otherwise are its drift, which the ant's roster
//! entry and the machine's record report.
//!
//! As any machine's host credentials may write any ant's record, a record is signed with the ant's own credentials,
//! which only the machines serving the ant hold ([`sign`]); a daemon whose team has credentials applies only a record so
//! signed, and writes its config's settings over any other ([`read`], [`refresh`]).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(doc)]
use ants_proto::{IDENTITIES_BUCKET, SHARED_FIELDS};
use ants_proto::{JsDomain, Machine, SharedSettings, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use data_encoding::BASE64URL_NOPAD;
use nkeys::KeyPair;
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

/// The fields of an ant's shared settings its machine's config sets otherwise (its drift); a clone shares it, so the
/// running ant's roster entry sees it change. Equal to any other, so that a change of drift alone restarts no ant.
#[derive(Debug, Clone)]
pub struct Drift(Arc<watch::Sender<Vec<String>>>);

impl Drift {
    pub fn new(fields: Vec<String>) -> Self {
        Self(Arc::new(watch::Sender::new(fields)))
    }

    pub fn get(&self) -> Vec<String> {
        self.0.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<Vec<String>> {
        self.0.subscribe()
    }

    /// Take the drift of `listed`, the ant's config as listed anew.
    pub fn replace(&self, listed: &Self) {
        let listed = listed.get();
        self.0.send_if_modified(|fields| {
            if *fields == listed {
                return false;
            }
            *fields = listed;
            true
        });
    }
}

impl Default for Drift {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl PartialEq for Drift {
    fn eq(&self, _: &Self) -> bool {
        true
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
    /// The credentials each ant's table names, which sign its records.
    creds: BTreeMap<UserId, PathBuf>,
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
        let ants = || ants.iter().filter(|ant| !ant.builtin);
        let local = ants().map(|ant| (ant.name.clone(), local_fields(ant))).collect();
        let creds = ants().filter_map(|ant| Some((ant.name.clone(), ant.creds.clone()?))).collect();
        let mut state = self.0.state.locked();
        state.local = local;
        state.creds = creds;
    }

    /// The shared settings the machine's config sets for `ant`; none for an ant it does not list.
    pub fn local(&self, ant: &UserId) -> Option<Map<String, Value>> {
        self.0.state.locked().local.get(ant).cloned()
    }

    /// The credentials the machine's config names for `ant`.
    pub fn creds(&self, ant: &UserId) -> Option<PathBuf> {
        self.0.state.locked().creds.get(ant).cloned()
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

/// Sign `record` with the key of the ant's credentials `jwt`.
pub fn sign(record: &mut SharedSettings, jwt: &str, key: &KeyPair) -> Result<()> {
    let signature = key.sign(&record.signed()).map_err(|e| anyhow!("signing the shared settings: {e}"))?;
    record.credentials = Some(jwt.to_owned());
    record.signature = Some(BASE64URL_NOPAD.encode(&signature));
    Ok(())
}

/// Sign `record` with the ant's credentials at `creds`; without any, leave it unsigned where `accounts` is empty (a
/// server without authentication), else fail.
pub fn sign_as(record: &mut SharedSettings, creds: Option<&Path>, accounts: &[String]) -> Result<()> {
    match creds {
        Some(creds) => {
            let (jwt, key) = crate::creds::load(creds, crate::keystore::os())?;
            sign(record, &jwt, &key)
        }
        None if accounts.is_empty() => Ok(()),
        None => bail!("no credentials of {} on this machine sign its shared settings", record.ant),
    }
}

/// What a read of an ant's shared settings found, with the stream sequence of the key's last message (0 for none),
/// which a compare-and-set expects.
pub enum Found {
    Record(SharedSettings, u64),
    /// A value that is no record of the ant's shared settings: why.
    Malformed(String, u64),
    /// A record not signed by credentials of the ant: why.
    Unverified(String, u64),
    None(u64),
    NoBucket,
}

/// The shared settings of `ant` in `kv`, verified against `accounts`, the team account's public keys
/// ([`SharedSettings::verify`]); with none, a server without authentication, any record holds.
pub async fn read(kv: &DirectKv, ant: &UserId, accounts: &[String]) -> Result<Found> {
    let key = SharedSettings::key_of(ant);
    Ok(match kv.lookup_stored(&key).await? {
        (Lookup::Value(bytes), last, stored) => match serde_json::from_slice::<SharedSettings>(&bytes) {
            Ok(record) => match record.check_key(&key) {
                Err(e) => Found::Malformed(e.to_string(), last),
                Ok(()) if accounts.is_empty() => Found::Record(record, last),
                Ok(()) => {
                    let stored = stored.map_or_else(Utc::now, DateTime::<Utc>::from);
                    match record.verify(accounts.iter().map(String::as_str), stored) {
                        Ok(_) => Found::Record(record, last),
                        Err(e) => Found::Unverified(e.to_string(), last),
                    }
                }
            },
            Err(e) => Found::Malformed(format!("not shared settings: {e}"), last),
        },
        (Lookup::NoValue, last, _) => Found::None(last),
        (Lookup::NoBucket, ..) => Found::NoBucket,
    })
}

/// Read the shared settings of `ants` into `shared`, verified against `accounts` ([`read`]). As `writer`, a machine
/// whose credentials may write them, create those missing from the machine's config, and write it over those not
/// signed by the ant's credentials. Failures are logged; the next poll tries again.
pub async fn refresh(kv: &DirectKv, writer: Option<&Machine>, shared: &Shared, ants: &[UserId], accounts: &[String]) {
    for ant in ants.iter().filter(|ant| **ant != UserId::messenger()) {
        if let Err(e) = refresh_one(kv, writer, shared, ant, accounts).await {
            tracing::warn!(%ant, error = format!("{e:#}"), "reading the ant's shared settings failed");
        }
    }
}

async fn refresh_one(
    kv: &DirectKv,
    writer: Option<&Machine>,
    shared: &Shared,
    ant: &UserId,
    accounts: &[String],
) -> Result<()> {
    for _ in 0..2 {
        // The sequence of the unverified record to write over; none to create the record.
        let over = match read(kv, ant, accounts).await? {
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
            Found::Unverified(why, last) => {
                shared.forget(ant);
                tracing::warn!(%ant, why, "ignoring the ant's shared settings, not signed by its credentials");
                Some(last)
            }
            Found::None(_) => {
                shared.forget(ant);
                None
            }
        };
        let (Some(machine), Some(fields)) = (writer, shared.local(ant)) else { return Ok(()) };
        let creds = shared.creds(ant);
        if creds.is_none() && !accounts.is_empty() {
            tracing::debug!(%ant, "no credentials of the ant sign its shared settings; leaving them to another machine");
            return Ok(());
        }
        let mut record = SharedSettings::new(ant.clone(), fields, machine.clone(), None);
        sign_as(&mut record, creds.as_deref(), accounts)?;
        let value = serde_json::to_vec(&record)?;
        let written = match over {
            Some(last) => kv.update(record.key(), value, last).await?,
            None => kv.create(record.key(), value).await?,
        };
        if written {
            tracing::info!(%ant, "wrote the ant's shared settings from this machine's config");
            shared.store(record);
            return Ok(());
        }
    }
    Ok(())
}

/// Where a daemon without a host connection reads the shared settings of its ants: a server, connected to as one of
/// them, which may read every ant's ([`follow`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Via {
    pub nats: String,
    pub creds: Option<PathBuf>,
    pub js_domain: Option<JsDomain>,
    /// The ant connected as.
    pub user: UserId,
    /// The ants whose shared settings to read there.
    pub ants: Vec<UserId>,
}

/// Keep `shared` current with the shared settings of the ants of `vias`, read every [`POLL_EVERY`] over a connection
/// as one of them ([`refresh`]), until `vias` closes: for a daemon without a host connection. Ant credentials only read
/// the records, verified against their account; over a server without authentication, missing ones are created from
/// the machine's config as the host connection would.
pub async fn follow(shared: Shared, mut vias: watch::Receiver<Vec<Via>>) {
    let machine = Machine::from_hostname(&crate::roster::machine().await);
    let mut connected: Vec<(Via, DirectKv, Vec<String>)> = Vec::new();
    loop {
        let wanted = vias.borrow_and_update().clone();
        connected.retain(|(via, ..)| wanted.contains(via));
        for via in wanted {
            if connected.iter().any(|(c, ..)| *c == via) {
                continue;
            }
            match reader(&via).await {
                Ok((kv, accounts)) => connected.push((via, kv, accounts)),
                Err(e) => tracing::warn!(
                    nats = %via.nats, as_ant = %via.user, error = format!("{e:#}"),
                    "connecting to read the shared settings failed; trying again"
                ),
            }
        }
        for (via, kv, accounts) in &connected {
            let writer = via.creds.is_none().then_some(&machine);
            refresh(kv, writer, &shared, &via.ants, accounts).await;
        }
        shared.polled();
        tokio::select! {
            () = tokio::time::sleep(POLL_EVERY) => {}
            changed = vias.changed() => if changed.is_err() { return },
        }
    }
}

/// The identities bucket over a connection as `via` names it, and the team account its credentials verify records
/// against.
async fn reader(via: &Via) -> Result<(DirectKv, Vec<String>)> {
    let accounts = via.creds.as_deref().map(crate::creds::account).transpose()?.into_iter().collect();
    let domain = via.js_domain.as_ref();
    let (_client, js) = crate::bus::connect(&via.nats, via.creds.as_deref(), domain, &via.user, "shared").await?;
    let kv = DirectKv::whole(&js, domain, ants_proto::IDENTITIES_BUCKET).await?;
    Ok((kv, accounts))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn ant(name: &str) -> Ant {
        Ant::new(name.parse().unwrap(), "/".into())
    }

    fn record(ant: &str, fields: Value) -> SharedSettings {
        SharedSettings::new(ant.parse().unwrap(), fields.as_object().unwrap().clone(), "m1".parse().unwrap(), None)
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
