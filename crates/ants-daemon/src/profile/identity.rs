//! The ant's identity record ([`AntIdentity`]) in [`IDENTITIES_BUCKET`]: created by its daemon on start when absent, so
//! the first daemon to start the ant anywhere sets its id and birth, and its completed prompts counted by
//! compare-and-set by the daemon serving it, so a standby daemon taking it over carries on the same count. A daemon
//! serving the ant with built-in sense names its sense identity there when the record names none
//! ([`Identity::name_sense`]), so every machine opens the same one, a record made again for the ant starting from the
//! birth and growth its sense identity kept. A record marked moved (`ants ant export --move`) keeps every daemon from
//! serving the ant until `ants ant unseal` clears the mark: the daemon serving it drains it as on a stop, and a daemon
//! starting it waits ([`Identity::moved`], [`Identity::unsealed`]).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::{AntIdentity, IDENTITIES_BUCKET, JsDomain, SenseId, SharedSettings, UserId, subject};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use data_encoding::HEXLOWER;
use ring::rand::{SecureRandom as _, SystemRandom};
use serde::{Deserialize, Serialize};

use crate::records::direct_kv::{DirectKv, Lookup};
use crate::sync::MutexExt as _;

/// How often a compare-and-set is tried again when another writer changed the record in between.
const ATTEMPTS: usize = 5;
/// How often a daemon reads the ant's record for the moved mark, while serving it or waiting for the mark to go.
pub const MOVED_EVERY: Duration = Duration::from_secs(30);

/// The id of the identity record of `ant` ([`AntIdentity::id`]), which its provider knows it by: what a read of its
/// memories ([`ants_proto::AdminOp::ReadMemories`]) carries. None, the daemon noting the provider's memories unread,
/// when it cannot be read.
pub async fn id_of(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Option<String> {
    let identity = Identity::new(js, js_domain, ant).await.ok()?;
    identity.read().await.ok()?.map(|record| record.id)
}

/// Forget an ant revoked by name, keeping the final birth and count of its sense identity on the hub.
pub async fn forget(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<()> {
    let kv = DirectKv::new(js, js_domain, IDENTITIES_BUCKET).await?;
    let mut expected = None;
    for _ in 0..ATTEMPTS {
        let (found, last) = kv.lookup_last(ant.as_str()).await?;
        match found {
            Lookup::NoBucket => return Ok(()),
            Lookup::NoValue => return kv.delete(SharedSettings::key_of(ant)).await,
            Lookup::Value(value) => {
                let record = parse(ant.as_str(), &value)?;
                if expected.as_ref().is_some_and(|id| id != &record.id) {
                    bail!("the identity record of {ant} was replaced while being revoked");
                }
                if forget_at(&kv, &record, last).await? {
                    return kv.delete(SharedSettings::key_of(ant)).await;
                }
                expected = Some(record.id);
            }
        }
    }
    bail!("the identity of {ant} kept changing while being revoked")
}

/// Persist growth before a compare-and-delete; a count written in between makes the caller retry with the new count.
async fn forget_at(kv: &DirectKv, record: &AntIdentity, last: u64) -> Result<bool> {
    if let Some(sense) = &record.sense {
        let key = subject::revoked_growth_key(sense);
        let growth = Growth { born: record.born, prompts: record.prompts };
        let mut saved = false;
        for _ in 0..ATTEMPTS {
            let (found, last) = kv.lookup_last(&key).await?;
            let previous = Growth::from_lookup(found)?;
            let latest = previous.map_or(growth, |previous| previous.merge(growth));
            if previous == Some(latest) || kv.update(&key, serde_json::to_vec(&latest)?, last).await? {
                saved = true;
                break;
            }
        }
        if !saved {
            bail!("the saved growth of {sense} kept changing while being revoked");
        }
    }
    kv.delete_expecting(record.key(), last, None).await
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Growth {
    born: DateTime<Utc>,
    prompts: u64,
}

impl Growth {
    fn from_lookup(found: Lookup) -> Result<Option<Self>> {
        match found {
            Lookup::Value(value) => Ok(Some(serde_json::from_slice(&value).context("reading revoked sense growth")?)),
            Lookup::NoValue => Ok(None),
            Lookup::NoBucket => bail!("the {IDENTITIES_BUCKET} bucket is missing"),
        }
    }

    fn merge(self, other: Self) -> Self {
        Self { born: self.born.min(other.born), prompts: self.prompts.max(other.prompts) }
    }
}

fn parse(key: &str, value: &[u8]) -> Result<AntIdentity> {
    let record: AntIdentity =
        serde_json::from_slice(value).with_context(|| format!("reading the identity of {key}"))?;
    record.check_key(key)?;
    Ok(record)
}

/// One ant's identity record, read and written by the credentials of the ant, which read every ant's record by direct
/// gets naming no subject and write only its own.
#[derive(Clone)]
pub struct Identity {
    kv: DirectKv,
    ant: UserId,
    /// The record's id once read ([`Identity::id`]), shared by clones.
    id: Arc<Mutex<Option<String>>>,
}

impl Identity {
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<Self> {
        let kv = DirectKv::whole(js, js_domain, IDENTITIES_BUCKET).await?;
        Ok(Self { kv, ant: ant.clone(), id: Arc::default() })
    }

    /// The id of the ant's record ([`AntIdentity::id`]), read once and kept until the record is replaced or deleted
    /// here: it stays while a daemon serves the ant. None while the record is absent.
    pub async fn id(&self) -> Result<Option<String>> {
        if let Some(id) = self.id.locked().clone() {
            return Ok(Some(id));
        }
        let id = self.read().await?.map(|record| record.id);
        self.id.locked().clone_from(&id);
        Ok(id)
    }

    /// The ant's record, created first when absent: None when the bucket is missing (`ants init` creates it).
    pub async fn ensure(&self) -> Result<Option<AntIdentity>> {
        self.change(None, |_, _| {}).await
    }

    /// Add a completed prompt to the record, creating it first when absent: the record as stored, None when the bucket
    /// is missing.
    pub async fn count_prompt(&self) -> Result<Option<AntIdentity>> {
        self.change(None, |record, _| record.prompts += 1).await
    }

    /// Name `sense` as the ant's sense identity in the record, creating the record first when absent, unless it names
    /// one already: the first writer wins. A record naming none yet, as one made again after a revoke, starts from
    /// `earlier`, the birth and the prompt count the sense identity kept of the ant's earlier record: its birth the
    /// earlier one, and those prompts added to its own. The record as stored, naming the sense identity that won; None
    /// when the bucket is missing.
    pub async fn name_sense(
        &self,
        sense: &SenseId,
        earlier: Option<(DateTime<Utc>, u64)>,
    ) -> Result<Option<AntIdentity>> {
        self.name_sense_from(sense, earlier, false).await
    }

    /// Name the sense id after reading its card, also using any newer growth kept by a revoke. Read that growth for
    /// the record about to be named on each compare-and-set attempt, including if the record was replaced meanwhile.
    pub(crate) async fn resume_sense(
        &self,
        sense: &SenseId,
        earlier: Option<(DateTime<Utc>, u64)>,
    ) -> Result<Option<AntIdentity>> {
        self.name_sense_from(sense, earlier, true).await
    }

    async fn name_sense_from(
        &self,
        sense: &SenseId,
        earlier: Option<(DateTime<Utc>, u64)>,
        resume: bool,
    ) -> Result<Option<AntIdentity>> {
        self.change(resume.then_some(sense), |record, saved| {
            if record.sense.is_some() {
                return;
            }
            let mut growth = earlier.map(|(born, prompts)| Growth { born, prompts });
            if let Some(saved) = saved {
                growth = Some(growth.map_or(saved, |card| card.merge(saved)));
            }
            record.sense = Some(sense.clone());
            if let Some(Growth { born, prompts }) = growth {
                (record.born, record.prompts) = (record.born.min(born), record.prompts + prompts);
            }
        })
        .await
    }

    /// The record of `ant`, any ant's: None when absent, or when the bucket is missing.
    pub async fn of(&self, ant: &UserId) -> Result<Option<AntIdentity>> {
        let Lookup::Value(value) = self.kv.lookup(ant.as_str()).await? else { return Ok(None) };
        let record: AntIdentity =
            serde_json::from_slice(&value).with_context(|| format!("reading the identity of {ant}"))?;
        record.check_key(ant.as_str())?;
        Ok(Some(record))
    }

    /// Store `record` as the ant's, in place of any: a clone's, made by its clone order ([`crate::clone`]).
    pub async fn replace(&self, record: &AntIdentity) -> Result<()> {
        record.check_key(self.ant.as_str())?;
        self.id.locked().take();
        self.kv.put(self.ant.as_str(), serde_json::to_vec(record)?).await
    }

    /// Delete the ant's record, as a failed clone order does with the one it stored.
    pub async fn delete(&self) -> Result<()> {
        self.id.locked().take();
        self.kv.delete(self.ant.as_str()).await
    }

    /// The record as stored, without creating it: None when absent or when the bucket is missing.
    pub async fn read(&self) -> Result<Option<AntIdentity>> {
        let key = self.ant.as_str();
        match self.kv.lookup(key).await? {
            Lookup::NoBucket | Lookup::NoValue => Ok(None),
            Lookup::Value(value) => parse(key, &value).map(Some),
        }
    }

    /// Whether the record is marked moved; a record that cannot be read counts as not moved, which the warning says.
    pub async fn is_moved(&self) -> bool {
        match self.read().await {
            Ok(record) => record.is_some_and(|r| r.moved.is_some()),
            Err(e) => {
                tracing::warn!(ant = %self.ant, error = format!("{e:#}"), "reading the ant's identity for a move");
                false
            }
        }
    }

    /// Return once the record is marked moved, reading it every `every`.
    pub async fn moved(&self, every: Duration) {
        loop {
            tokio::time::sleep(every).await;
            if self.is_moved().await {
                return;
            }
        }
    }

    /// Return once the record is not marked moved, reading it every `every`; at once when it is not.
    pub async fn unsealed(&self, every: Duration) {
        let mut told = false;
        while self.is_moved().await {
            if !told {
                tracing::warn!(
                    ant = %self.ant,
                    "the ant is marked moved to another bus: not serving it until `ants ant unseal` or `ants auth revoke`"
                );
                told = true;
            }
            tokio::time::sleep(every).await;
        }
        if told {
            tracing::info!(ant = %self.ant, "the ant's moved mark is gone; serving it");
        }
    }

    /// Store the record as `change` makes it, by compare-and-set on the one read, a new one when absent; unchanged, a
    /// record found is not stored again.
    async fn change(
        &self,
        resume: Option<&SenseId>,
        change: impl Fn(&mut AntIdentity, Option<Growth>),
    ) -> Result<Option<AntIdentity>> {
        let key = self.ant.as_str();
        for _ in 0..ATTEMPTS {
            let (found, last) = self.kv.lookup_last(key).await?;
            let (mut record, existed) = match found {
                Lookup::NoBucket => return Ok(None),
                Lookup::NoValue => (born(&self.ant)?, false),
                Lookup::Value(value) => (parse(key, &value)?, true),
            };
            let saved = match resume {
                Some(sense) if record.sense.is_none() => {
                    Growth::from_lookup(self.kv.lookup(subject::revoked_growth_key(sense)).await?)?
                }
                _ => None,
            };
            let before = record.clone();
            change(&mut record, saved);
            if existed && record == before {
                return Ok(Some(record));
            }
            let value = serde_json::to_vec(&record)?;
            if self.kv.update(key, value, last).await? {
                return Ok(Some(record));
            }
        }
        bail!("the identity of {key} kept changing while being updated")
    }
}

/// A record of `ant` born now, with a new random id.
pub fn born(ant: &UserId) -> Result<AntIdentity> {
    let mut id = [0; 16];
    SystemRandom::new().fill(&mut id).map_err(|_| anyhow::anyhow!("no random id"))?;
    Ok(AntIdentity::new(ant.clone(), HEXLOWER.encode(&id), Utc::now()))
}

#[cfg(test)]
mod tests;
