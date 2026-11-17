//! What the CLI makes of an act's `on_behalf_of` ([`Acting`]): honoured only while the human's grant names the
//! publisher and the publisher's record names the human ([`ants_daemon::behalf::Records`]), else forged, the act then
//! being the publisher's own. Each human's grant and each assistant's record is read once, when first needed, and
//! again on a change of a grant or a revocation while a long-running reader follows them ([`Checker::follow`]).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_daemon::behalf::Records;
use ants_daemon::direct_kv::{DirectKv, Lookup};
use ants_daemon::reactions::{Change, Reactions};
use ants_daemon::sync::MutexExt as _;
use ants_proto::{
    ASSISTANT_GRANT, Acting, DEVICES_BUCKET, Envelope, Grants, HUMANS_BUCKET, JsDomain, REACTIONS_BUCKET,
    REVOKED_DEVICES, Reaction, ReactionEntry, UserId, Uuid, assistant_grant_key, reaction_key,
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
    js_domain: Option<JsDomain>,
    account: Option<String>,
    records: Records,
    /// The human each user's record makes it the assistant of, as read: only an assistant's reactions are read
    /// for a claim.
    assistants: Mutex<HashMap<UserId, Option<UserId>>>,
    grants: Mutex<Grants>,
    /// The humans whose grant was read.
    read: Mutex<HashSet<UserId>>,
    /// What each publisher's claim for each human came to.
    known: Mutex<HashMap<(UserId, UserId), Acting>>,
    /// Whether a following checker started its watches, or found them refused: a change from then on is delivered.
    watching: tokio::sync::watch::Sender<bool>,
}

impl Checker {
    /// A checker reading with `js` as the credentials of `nats`: without an account, which verifies grants and
    /// records, every claim is forged.
    pub fn new(nats: &crate::Nats<'_>, js: &jetstream::Context) -> Self {
        let records = Records::new(js.clone(), nats.domain.cloned(), nats.account.clone());
        Self {
            js: js.clone(),
            js_domain: nats.domain.cloned(),
            account: nats.account.clone(),
            records,
            assistants: Mutex::default(),
            grants: Mutex::default(),
            read: Mutex::default(),
            known: Mutex::default(),
            watching: tokio::sync::watch::Sender::new(false),
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

    /// The assistant acting for `human` now, if any: the one its grant names, whose record names the human.
    pub async fn assistant(&self, human: &UserId) -> Option<UserId> {
        self.read_grant(human).await;
        let assistant = self.grants.locked().designated(human).cloned()?;
        matches!(self.check(&assistant, Some(human)).await, Acting::For(_)).then_some(assistant)
    }

    /// Who `publisher`'s act claiming `claim` is by, as [`Acting::label`] names it once checked ([`Checker::check`]).
    pub async fn label(&self, publisher: &UserId, claim: Option<&UserId>) -> String {
        self.check(publisher, claim).await.label(publisher.as_str())
    }

    /// What `publisher`'s act claiming `claim` came to, as checked before ([`Checker::check`]): a claim not checked
    /// is forged.
    pub fn acting(&self, publisher: &UserId, claim: Option<&UserId>) -> Acting {
        let Some(human) = claim else { return Acting::Own };
        let known = self.known.locked().get(&(publisher.clone(), human.clone())).cloned();
        known.unwrap_or_else(|| Acting::Forged(human.clone()))
    }

    /// What the reactions in `reactions` came to: the claim of each by an assistant, which only its entry carries,
    /// is checked ([`Checker::check`]) into [`ants_proto::Reacted::acting`]. Another reactor's reaction is its own,
    /// unread.
    pub async fn reactions(&self, reactions: &mut Reactions) {
        for (message, reacted) in reactions.iter_mut() {
            for r in reacted.iter_mut() {
                for by in &r.by {
                    let acting = self.reaction(by, message, &r.reaction).await;
                    if !acting.is_own() {
                        r.acting.insert(by.clone(), acting);
                    }
                }
            }
        }
    }

    /// `change` with what a reaction it adds came to, as [`Checker::reactions`] checks it.
    pub async fn change(&self, mut change: Change) -> Change {
        if !change.removed {
            change.acting = self.reaction(&change.by, &change.message, &change.reaction).await;
        }
        change
    }

    /// What `by`'s `reaction` to `message` came to.
    async fn reaction(&self, by: &UserId, message: &Uuid, reaction: &Reaction) -> Acting {
        if self.assistant_of(by).await.is_none() {
            return Acting::Own;
        }
        let entry = async {
            let kv = DirectKv::new(&self.js, self.js_domain.as_ref(), REACTIONS_BUCKET).await?;
            let Lookup::Value(value) = kv.lookup(reaction_key(by, message, reaction)).await? else { return Ok(None) };
            anyhow::Ok(Some(serde_json::from_slice::<ReactionEntry>(&value)?))
        };
        let claim = entry.await.unwrap_or_else(|e| {
            tracing::warn!(%by, %message, "reading a reaction: {e:#}");
            None
        });
        self.check(by, claim.and_then(|e| e.on_behalf_of).as_ref()).await
    }

    /// The human `user`'s record makes it the assistant of, read unless known.
    async fn assistant_of(&self, user: &UserId) -> Option<UserId> {
        if let Some(known) = self.assistants.locked().get(user) {
            return known.clone();
        }
        let of = self.records.assistant_of(user).await;
        self.assistants.locked().insert(user.clone(), of.clone());
        of
    }

    /// Read again the grant of every human read and the record behind every claim checked, so a grant withdrawn or
    /// credentials revoked since make the claims forged from now on.
    pub async fn refresh(&self) {
        self.assistants.locked().clear();
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

    /// Wait until following started its watches ([`Checker::follow`]).
    #[cfg(test)]
    pub async fn watching(&self) {
        let _ = self.watching.subscribe().wait_for(|watching| *watching).await;
    }

    async fn keep_fresh(self: Arc<Self>, period: Duration) {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let mut grants = self.watch(HUMANS_BUCKET, &format!("*.{ASSISTANT_GRANT}")).await;
        let mut revoked = self.watch(DEVICES_BUCKET, &format!("{REVOKED_DEVICES}.*")).await;
        self.watching.send_replace(true);
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
