//! An ant's shared settings ([`SharedSettings`]): its role settings and core memories ([`SHARED_FIELDS`]), kept in
//! [`IDENTITIES_BUCKET`] so the ant is the same on every machine. A machine's host connection ([`crate::host`]) reads
//! those of the ants its daemon serves every [`POLL_EVERY`], creating those still missing from the machine's config,
//! and keeps them in a [`Shared`]; the daemon serves each ant with them in place of its config's ([`Shared::overlaid`])
//! and reloads when they change. The fields the machine's config sets otherwise are its drift, which the ant's roster
//! entry and the machine's record report.
//!
//! As any machine's host credentials may write any ant's record, a record is signed with the ant's own credentials,
//! which only the machines serving the ant hold ([`Signer`]); a daemon whose team has credentials applies only a record
//! so signed, and writes its config's settings over any other ([`read`], [`refresh`]). It refuses a record signed with
//! revoked credentials, and one the ant signed that was put back after it was replaced: stored at another stream
//! sequence than the one it was signed for ([`write`], [`SharedSettings::verify`]); over a replay it writes back the
//! settings it kept.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[cfg(doc)]
use ants_proto::SHARED_FIELDS;
use ants_proto::{
    DEVICES_BUCKET, IDENTITIES_BUCKET, JsDomain, Machine, SettingsError, SharedSettings, UserId, revoked_device_key,
};
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

/// What signs an ant's shared settings: the key of the ant's credentials, with their JWT; nothing on a server without
/// authentication.
pub struct Signer(Option<(String, KeyPair)>);

impl Signer {
    /// The ant's credentials `jwt`, whose key is `key`.
    pub fn new(jwt: &str, key: KeyPair) -> Self {
        Self(Some((jwt.to_owned(), key)))
    }

    /// The credentials of `ant` at `creds`; without any, nothing where `accounts` is empty (a server without
    /// authentication), else fail.
    pub fn load(ant: &UserId, creds: Option<&Path>, accounts: &[String]) -> Result<Self> {
        match creds {
            Some(creds) => {
                let (jwt, key) = crate::creds::load(creds, crate::keystore::os())?;
                Ok(Self::new(&jwt, key))
            }
            None if accounts.is_empty() => Ok(Self(None)),
            None => bail!("no credentials of {ant} on this machine sign its shared settings"),
        }
    }

    /// The public key that signs, if any.
    pub fn key(&self) -> Option<String> {
        self.0.as_ref().map(|(_, key)| key.public_key())
    }

    fn sign(&self, record: &mut SharedSettings) -> Result<()> {
        let Some((jwt, key)) = &self.0 else { return Ok(()) };
        let signature = key.sign(&record.signed()).map_err(|e| anyhow!("signing the shared settings: {e}"))?;
        record.credentials = Some(jwt.clone());
        record.signature = Some(BASE64URL_NOPAD.encode(&signature));
        Ok(())
    }
}

/// Store `record` in `identities`, signed by `signer` for the stream sequence it is stored at
/// ([`SharedSettings::sequence`], [`DirectKv::put_sequenced`]), if the key's last message is still the one at stream
/// sequence `last` (0: none); with no `last`, whatever the key holds. Whether it stored the record, which then holds
/// its sequence and signature: false when the key changed meanwhile.
pub async fn write(
    identities: &DirectKv,
    record: &mut SharedSettings,
    signer: &Signer,
    last: Option<u64>,
) -> Result<bool> {
    let key = record.key();
    identities
        .put_sequenced(&key, last, |sequence| {
            record.sequence = sequence;
            signer.sign(record)?;
            Ok(serde_json::to_vec(&*record)?)
        })
        .await
        .context("writing the shared settings")
}

/// What a read of an ant's shared settings found, with the stream sequence of the key's last message (0 for none),
/// which a compare-and-set expects.
pub enum Found {
    Record(SharedSettings, u64),
    /// A value that is no record of the ant's shared settings: why.
    Malformed(String, u64),
    /// A record readers do not apply, and why: not signed by credentials of the ant, signed by revoked ones, or put
    /// back after it was written.
    Unverified(Box<SharedSettings>, SettingsError, u64),
    None(u64),
    NoBucket,
}

/// Where the shared settings are read and checked.
#[derive(Clone, Copy)]
pub struct Bucket<'a> {
    /// [`IDENTITIES_BUCKET`].
    pub identities: &'a DirectKv,
    /// [`DEVICES_BUCKET`], whose revocation records name the keys of credentials that sign no record.
    pub devices: &'a DirectKv,
    /// The team account's public keys, which sign the ants' credentials; none on a server without authentication.
    pub accounts: &'a [String],
}

/// The shared settings of `ant` in `bucket`, verified ([`SharedSettings::verify`]); without accounts, a server without
/// authentication, any record holds.
pub async fn read(bucket: Bucket<'_>, ant: &UserId) -> Result<Found> {
    let key = SharedSettings::key_of(ant);
    let (bytes, last, stored) = match bucket.identities.lookup_stored(&key).await? {
        (Lookup::Value(bytes), last, stored) => (bytes, last, stored),
        (Lookup::NoValue, last, _) => return Ok(Found::None(last)),
        (Lookup::NoBucket, ..) => return Ok(Found::NoBucket),
    };
    let record = match serde_json::from_slice::<SharedSettings>(&bytes) {
        Ok(record) => record,
        Err(e) => return Ok(Found::Malformed(format!("not shared settings: {e}"), last)),
    };
    if let Err(e) = record.check_key(&key) {
        return Ok(Found::Malformed(e.to_string(), last));
    }
    if bucket.accounts.is_empty() {
        return Ok(Found::Record(record, last));
    }
    let revoked = match record.signing_key() {
        Some(key) => matches!(bucket.devices.lookup(revoked_device_key(&key)).await?, Lookup::Value(_)),
        None => false,
    };
    let stored = stored.map_or_else(Utc::now, DateTime::<Utc>::from);
    Ok(match record.verify(bucket.accounts.iter().map(String::as_str), |_| revoked, stored, last) {
        Ok(_) => Found::Record(record, last),
        Err(e) => Found::Unverified(Box::new(record), e, last),
    })
}

/// Read the shared settings of `ants` into `shared`, verified ([`read`]). As `writer`, a machine whose credentials may
/// write them, create those missing from the machine's config, and write over those not verified: the settings kept
/// before a record put back after it was replaced, else the machine's config's. Failures are logged; the next poll
/// tries again.
pub async fn refresh(bucket: Bucket<'_>, writer: Option<&Machine>, shared: &Shared, ants: &[UserId]) {
    for ant in ants.iter().filter(|ant| **ant != UserId::messenger()) {
        if let Err(e) = refresh_one(bucket, writer, shared, ant).await {
            tracing::warn!(%ant, error = format!("{e:#}"), "reading the ant's shared settings failed");
        }
    }
}

async fn refresh_one(bucket: Bucket<'_>, writer: Option<&Machine>, shared: &Shared, ant: &UserId) -> Result<()> {
    for _ in 0..2 {
        // The sequence of the key's last message, which the record written expects, why the record there is refused,
        // and the settings kept to write back over a record put back.
        let (last, why, kept) = match read(bucket, ant).await? {
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
            Found::Unverified(_, why, last) => {
                tracing::warn!(%ant, why = %why, "ignoring the ant's shared settings");
                let kept = shared.record(ant).filter(|_| why.is_replay());
                if kept.is_none() {
                    shared.forget(ant);
                }
                (last, Some(why), kept)
            }
            Found::None(last) => {
                shared.forget(ant);
                (last, None, None)
            }
        };
        let Some(machine) = writer else { return Ok(()) };
        let (fields, by) = match kept {
            Some(kept) => (kept.fields, kept.by),
            None => match shared.local(ant) {
                Some(fields) => (fields, None),
                None => return Ok(()),
            },
        };
        let creds = shared.creds(ant);
        if creds.is_none() && !bucket.accounts.is_empty() {
            tracing::debug!(%ant, "no credentials of the ant sign its shared settings; leaving them to another machine");
            return Ok(());
        }
        let signer = Signer::load(ant, creds.as_deref(), bucket.accounts)?;
        if let Some(SettingsError::Revoked(key)) = &why
            && signer.key().as_ref() == Some(key)
        {
            tracing::warn!(%ant, key, "the ant's credentials on this machine are revoked: not writing its shared settings");
            return Ok(());
        }
        let mut record = SharedSettings::new(ant.clone(), fields, machine.clone(), by);
        if write(bucket.identities, &mut record, &signer, Some(last)).await? {
            tracing::info!(%ant, "wrote the ant's shared settings");
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
/// the machine's config as the host connection would, naming `machine` as their writer, else the first label of the
/// host name.
pub async fn follow(shared: Shared, mut vias: watch::Receiver<Vec<Via>>, machine: Option<Machine>) {
    let machine = match machine {
        Some(machine) => machine,
        None => Machine::from_hostname(&crate::roster::machine().await),
    };
    let mut connected: Vec<(Via, Reader)> = Vec::new();
    loop {
        let wanted = vias.borrow_and_update().clone();
        connected.retain(|(via, ..)| wanted.contains(via));
        for via in wanted {
            if connected.iter().any(|(c, ..)| *c == via) {
                continue;
            }
            match Reader::open(&via).await {
                Ok(reader) => connected.push((via, reader)),
                Err(e) => tracing::warn!(
                    nats = %via.nats, as_ant = %via.user, error = format!("{e:#}"),
                    "connecting to read the shared settings failed; trying again"
                ),
            }
        }
        for (via, reader) in &connected {
            let writer = via.creds.is_none().then_some(&machine);
            refresh(reader.bucket(), writer, &shared, &via.ants).await;
        }
        shared.polled();
        tokio::select! {
            () = tokio::time::sleep(POLL_EVERY) => {}
            changed = vias.changed() => if changed.is_err() { return },
        }
    }
}

/// The buckets over a connection as a [`Via`] names it, and the team account its credentials verify records against.
struct Reader {
    identities: DirectKv,
    devices: DirectKv,
    accounts: Vec<String>,
}

impl Reader {
    async fn open(via: &Via) -> Result<Self> {
        let accounts = via.creds.as_deref().map(crate::creds::account).transpose()?.into_iter().collect();
        let domain = via.js_domain.as_ref();
        let (_client, js) = crate::bus::connect(&via.nats, via.creds.as_deref(), domain, &via.user, "shared").await?;
        let identities = DirectKv::whole(&js, domain, IDENTITIES_BUCKET).await?;
        let devices = DirectKv::new(&js, domain, DEVICES_BUCKET).await?;
        Ok(Self { identities, devices, accounts })
    }

    fn bucket(&self) -> Bucket<'_> {
        Bucket { identities: &self.identities, devices: &self.devices, accounts: &self.accounts }
    }
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

    #[tokio::test]
    async fn without_a_host_connection_the_records_created_name_the_machine_given() {
        let Some(server) = ants_test_support::NatsServer::start() else { return };
        let js = async_nats::jetstream::new(async_nats::connect(&server.url).await.unwrap());
        js.create_key_value(crate::hub::identities_config()).await.unwrap();
        let mut dev_a = ant("dev-a");
        dev_a.model = Some("gpt-4".to_owned());
        let shared = Shared::default();
        shared.listed(std::slice::from_ref(&dev_a));
        let user = dev_a.name.clone();
        let via = Via { nats: server.url.clone(), creds: None, js_domain: None, user: user.clone(), ants: vec![user] };
        let (vias, watched) = watch::channel(vec![via]);
        let following = tokio::spawn(follow(shared.clone(), watched, Some("dev-vm1".parse().unwrap())));
        assert!(shared.wait_polled(Duration::from_secs(10)).await, "the follower reads the shared settings");
        assert_eq!(shared.record(&dev_a.name).unwrap().machine.as_str(), "dev-vm1");
        drop(vias);
        tokio::time::timeout(Duration::from_secs(10), following).await.unwrap().unwrap();
    }
}
