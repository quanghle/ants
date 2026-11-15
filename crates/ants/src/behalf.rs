//! What the CLI makes of an act's `on_behalf_of` ([`Acting`]): honoured only while the human's grant names the
//! publisher and the publisher's record names the human ([`ants_daemon::behalf::Records`]), else forged, the act then
//! being the publisher's own. Each human's grant and each assistant's record is read once, when first needed, and
//! again on a change of a grant or a revocation while a long-running reader follows them ([`Checker::follow`]).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_daemon::behalf::Records;
use ants_daemon::sync::MutexExt as _;
use ants_proto::{
    ASSISTANT_GRANT, Acting, DEVICES_BUCKET, Envelope, Grants, HUMANS_BUCKET, REVOKED_DEVICES, UserId,
    assistant_grant_key,
};
use async_nats::jetstream::{self, kv};
use chrono::Utc;
use futures::StreamExt as _;
use tokio::task::JoinHandle;

/// How often a following checker reads every claim it checked again when it cannot watch the grants or the
/// revocations, as an ant's credentials cannot watch the revocations; and how soon it watches again after a watch
/// ended.
pub const REFRESH: Duration = Duration::from_secs(60);

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

    /// Read again the grant of every human read and the record behind every claim checked, so a grant withdrawn or
    /// credentials revoked since make the claims forged from now on.
    pub async fn refresh(&self) {
        let humans = std::mem::take(&mut *self.read.locked());
        for human in &humans {
            self.read_grant(human).await;
        }
        let claims: Vec<(UserId, UserId)> = self.known.locked().keys().cloned().collect();
        let grants = self.grants.locked().clone();
        for (publisher, human) in claims {
            let acting = self.records.acting(&grants, &publisher, Some(&human)).await;
            self.known.locked().insert((publisher, human), acting);
        }
    }

    /// Keep the checks fresh while the handle returned lives: [`Checker::refresh`] on every change of an assistant
    /// grant and every revocation of credentials, as watched; every [`REFRESH`] instead when either watch is
    /// refused or ends.
    pub fn follow(self: &Arc<Self>) -> Following {
        Following(tokio::spawn(Arc::clone(self).keep_fresh(REFRESH)))
    }

    async fn keep_fresh(self: Arc<Self>, period: Duration) {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut grants = self.watch(HUMANS_BUCKET, &format!("*.{ASSISTANT_GRANT}")).await;
        let mut revoked = self.watch(DEVICES_BUCKET, &format!("{REVOKED_DEVICES}.*")).await;
        loop {
            tokio::select! {
                change = next(&mut grants) => if !matches!(change, Some(Ok(_))) { grants = None },
                change = next(&mut revoked) => if !matches!(change, Some(Ok(_))) { revoked = None },
                _ = tick.tick() => {
                    if grants.is_some() && revoked.is_some() {
                        continue;
                    }
                    if grants.is_none() {
                        grants = self.watch(HUMANS_BUCKET, &format!("*.{ASSISTANT_GRANT}")).await;
                    }
                    if revoked.is_none() {
                        revoked = self.watch(DEVICES_BUCKET, &format!("{REVOKED_DEVICES}.*")).await;
                    }
                }
            }
            self.refresh().await;
        }
    }

    /// A watch of the keys of `bucket` matching `keys` from now on, unless refused.
    async fn watch(&self, bucket: &str, keys: &str) -> Option<kv::Watch> {
        let watch = async { anyhow::Ok(self.js.get_key_value(bucket).await?.watch(keys).await?) };
        watch.await.inspect_err(|e| tracing::debug!(bucket, "not watching {keys}: {e:#}")).ok()
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

/// The next change `watch` delivers; never without one.
async fn next(watch: &mut Option<kv::Watch>) -> Option<Result<kv::Entry, kv::WatcherError>> {
    match watch {
        Some(watch) => watch.next().await,
        None => std::future::pending().await,
    }
}

/// Keeps a checker's checks fresh ([`Checker::follow`]) until dropped.
pub struct Following(JoinHandle<()>);

impl Drop for Following {
    fn drop(&mut self) {
        self.0.abort();
    }
}
