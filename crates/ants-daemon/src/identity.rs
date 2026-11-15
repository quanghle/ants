//! The ant's identity record ([`AntIdentity`]) in [`IDENTITIES_BUCKET`]: created by its daemon on start when absent, so
//! the first daemon to start the ant anywhere sets its id and birth, and its completed prompts counted by
//! compare-and-set by the daemon serving it, so a standby daemon taking it over carries on the same count.

use ants_proto::{AntIdentity, IDENTITIES_BUCKET, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use chrono::Utc;
use data_encoding::HEXLOWER;
use ring::rand::{SecureRandom as _, SystemRandom};

use crate::direct_kv::{DirectKv, Lookup};

/// How often a compare-and-set is tried again when another writer changed the record in between.
const ATTEMPTS: usize = 5;

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

    /// Store the record as `change` makes it, by compare-and-set on the one read, a new one when absent; unchanged, a
    /// record found is not stored again.
    async fn change(&self, change: impl Fn(&mut AntIdentity)) -> Result<Option<AntIdentity>> {
        let key = self.ant.as_str();
        for _ in 0..ATTEMPTS {
            let (found, last) = self.kv.lookup_last(key).await?;
            let (mut record, existed) = match found {
                Lookup::NoBucket => return Ok(None),
                Lookup::NoValue => (self.born()?, false),
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

    /// A record of the ant born now, with a new random id.
    fn born(&self) -> Result<AntIdentity> {
        let mut id = [0; 16];
        SystemRandom::new().fill(&mut id).map_err(|_| anyhow::anyhow!("no random id"))?;
        Ok(AntIdentity::new(self.ant.clone(), HEXLOWER.encode(&id), Utc::now()))
    }
}

#[cfg(test)]
mod tests;
