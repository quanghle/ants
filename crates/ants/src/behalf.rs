//! What the CLI makes of an act's `on_behalf_of` ([`Acting`]): honoured only while the human's grant names the
//! publisher and the publisher's record names the human ([`ants_daemon::behalf::Records`]), else forged, the act then
//! being the publisher's own. Each human's grant and each assistant's record is read once, when first needed.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use ants_daemon::behalf::Records;
use ants_daemon::sync::MutexExt as _;
use ants_proto::{Acting, Envelope, Grants, HUMANS_BUCKET, UserId, assistant_grant_key};
use async_nats::jetstream;
use chrono::Utc;

/// Checks the claims of the acts the CLI shows, caching what it read.
pub struct Checker {
    js: jetstream::Context,
    account: Option<String>,
    records: Records,
    grants: Mutex<Grants>,
    /// The humans whose grant was read.
    read: Mutex<HashSet<UserId>>,
    /// What each publisher's claim for each human came to.
    known: Mutex<HashMap<(UserId, UserId), Acting>>,
}

impl Checker {
    /// A checker reading with `js` as the credentials of `nats`: without an account, which verifies grants and
    /// records, every claim is forged.
    pub fn new(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Self {
        let records = Records::new(js.clone(), nats.domain.cloned(), nats.account.clone());
        Self {
            js: js.clone(),
            account: nats.account.clone(),
            records,
            grants: Mutex::default(),
            read: Mutex::default(),
            known: Mutex::default(),
        }
    }

    /// Check the claim of `envelope` unless known, before [`Checker::acting`] takes it.
    pub async fn learn(&self, envelope: &Envelope) {
        self.check(&envelope.from, envelope.on_behalf_of.as_ref()).await;
    }

    /// What `publisher`'s act claiming `claim` comes to, read unless known.
    pub async fn check(&self, publisher: &UserId, claim: Option<&UserId>) -> Acting {
        let Some(human) = claim else { return Acting::Own };
        let key = (publisher.clone(), human.clone());
        if let Some(known) = self.known.locked().get(&key) {
            return known.clone();
        }
        self.read_grant(human).await;
        let grants = self.grants.locked().clone();
        let acting = self.records.acting(&grants, publisher, claim).await;
        self.known.locked().insert(key, acting.clone());
        acting
    }

    /// What `publisher`'s act claiming `claim` came to, as checked before ([`Checker::check`]): a claim not checked
    /// is forged.
    pub fn acting(&self, publisher: &UserId, claim: Option<&UserId>) -> Acting {
        let Some(human) = claim else { return Acting::Own };
        let known = self.known.locked().get(&(publisher.clone(), human.clone())).cloned();
        known.unwrap_or_else(|| Acting::Forged(human.clone()))
    }

    /// Read `human`'s grant into the grants, once.
    async fn read_grant(&self, human: &UserId) {
        let Some(account) = &self.account else { return };
        if !self.read.locked().insert(human.clone()) {
            return;
        }
        let key = assistant_grant_key(human);
        let value = async {
            let kv = self.js.get_key_value(HUMANS_BUCKET).await?;
            anyhow::Ok(kv.get(&key).await?)
        };
        let value = value.await.unwrap_or_else(|e| {
            tracing::warn!(%human, "reading the assistant grant: {e:#}");
            None
        });
        let applied = self.grants.locked().apply(&key, value.as_deref(), account, Utc::now());
        if let Some(refused) = applied.and_then(|a| a.refused) {
            tracing::warn!(%human, "not honouring the assistant grant: {refused}");
        }
    }
}
