//! The ant's identity record ([`AntIdentity`]) in [`IDENTITIES_BUCKET`]: created by its daemon on start when absent, so
//! the first daemon to start the ant anywhere sets its id and birth, and its completed prompts counted by
//! compare-and-set by the daemon serving it, so a standby daemon taking it over carries on the same count. A record
//! marked moved (`ants ant export --move`) keeps every daemon from serving the ant until `ants ant unseal` clears the
//! mark: the daemon serving it drains it as on a stop, and a daemon starting it waits ([`Identity::moved`],
//! [`Identity::unsealed`]).

use std::time::Duration;

use ants_proto::{AntIdentity, IDENTITIES_BUCKET, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use data_encoding::HEXLOWER;
use ring::rand::{SecureRandom as _, SystemRandom};

use crate::records::direct_kv::{DirectKv, Lookup};

/// How often a compare-and-set is tried again when another writer changed the record in between.
const ATTEMPTS: usize = 5;
/// How often a daemon reads the ant's record for the moved mark, while serving it or waiting for the mark to go.
pub const MOVED_EVERY: Duration = Duration::from_secs(30);

/// One ant's identity record, read and written by the credentials of the ant, which read every ant's record by direct
/// gets naming no subject and write only its own.
#[derive(Clone)]
pub struct Identity {
    kv: DirectKv,
    ant: UserId,
}

impl Identity {
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<Self> {
        Ok(Self { kv: DirectKv::whole(js, js_domain, IDENTITIES_BUCKET).await?, ant: ant.clone() })
    }

    /// The ant's record, created first when absent: None when the bucket is missing (`ants init` creates it).
    pub async fn ensure(&self) -> Result<Option<AntIdentity>> {
        self.change(|_| {}).await
    }

    /// Add a completed prompt to the record, creating it first when absent: the record as stored, None when the bucket
    /// is missing.
    pub async fn count_prompt(&self) -> Result<Option<AntIdentity>> {
        self.change(|record| record.prompts += 1).await
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
        self.kv.put(self.ant.as_str(), serde_json::to_vec(record)?).await
    }

    /// Delete the ant's record, as a failed clone order does with the one it stored.
    pub async fn delete(&self) -> Result<()> {
        self.kv.delete(self.ant.as_str()).await
    }

    /// The record as stored, without creating it: None when absent or when the bucket is missing.
    pub async fn read(&self) -> Result<Option<AntIdentity>> {
        let key = self.ant.as_str();
        match self.kv.lookup_last(key).await?.0 {
            Lookup::NoBucket | Lookup::NoValue => Ok(None),
            Lookup::Value(value) => {
                let record: AntIdentity =
                    serde_json::from_slice(&value).with_context(|| format!("reading the identity of {key}"))?;
                record.check_key(key)?;
                Ok(Some(record))
            }
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
    async fn change(&self, change: impl Fn(&mut AntIdentity)) -> Result<Option<AntIdentity>> {
        let key = self.ant.as_str();
        for _ in 0..ATTEMPTS {
            let (found, last) = self.kv.lookup_last(key).await?;
            let (mut record, existed) = match found {
                Lookup::NoBucket => return Ok(None),
                Lookup::NoValue => (born(&self.ant)?, false),
                Lookup::Value(value) => {
                    let record: AntIdentity =
                        serde_json::from_slice(&value).with_context(|| format!("reading the identity of {key}"))?;
                    record.check_key(key)?;
                    (record, true)
                }
            };
            let before = record.clone();
            change(&mut record);
            if existed && record == before {
                return Ok(Some(record));
            }
            let value = serde_json::to_vec(&record)?;
            let stored =
                if existed { self.kv.update(key, value, last).await? } else { self.kv.create(key, value).await? };
            if stored {
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
