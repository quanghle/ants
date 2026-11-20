//! The team's humans as a daemon sees them.
//!
//! A human is a user listed in the [`HUMANS_BUCKET`], which only humans write; `human` and the daemon's default
//! human (config `human`) are humans without an entry. The daemon keeps the bucket's humans in a [`Registry`], kept
//! current by a watch ([`Registry::watch`]). Until the bucket exists only the humans without an entry are known; the
//! daemon looks for the bucket again every little while, since an ant may read it but not create it.
//!
//! The same watch keeps the humans' assistant grants ([`ants_proto::AssistantGrant`], under `<human>.assistant`),
//! verified for the team's account ([`Registry::verifying_grants`]): a withdrawal holds as soon as the watch delivers
//! it, and is announced to whoever listens ([`Registry::withdrawals`]).
//!
//! It keeps too each human's consent to what its assistant does for it ([`ants_proto::Consents`]): its signed answers
//! to the assistant's acts (`<human>.confirmed`) and its pre-allow rules (`<human>.preallow`), announced as they
//! change ([`Registry::consents`]).

use crate::bus::relayed::GuardedKv as _;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use ants_proto::{
    Act, ActAnswer, Confirmations, ConsentKey, Consents, Envelope, Grants, HUMANS_BUCKET, HumanEntry, Humans, PreAllow,
    Role, UserId, VerifiedGrant, assistant_grant_of,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, context::KeyValueErrorKind, kv};
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use crate::sync::RwLockExt as _;

/// How long the daemon waits to look for the bucket again, when it is missing or its watch ended.
pub const RETRY: Duration = Duration::from_secs(30);

/// How many withdrawals a listener may lag behind before it misses some ([`Registry::withdrawals`]).
const WITHDRAWALS: usize = 64;

/// The humans of the team, shared by the inbox loop and the MCP server.
#[derive(Debug, Clone)]
pub struct Registry {
    humans: Arc<RwLock<Humans>>,
    default: UserId,
    assistants: Arc<Assistants>,
}

/// The assistant grants a [`Registry`] keeps, behind one pointer for clones of it to share.
#[derive(Debug)]
struct Assistants {
    grants: RwLock<Grants>,
    /// The team's account, which signs the credentials of the devices whose grants count; none: no grant does.
    account: Option<String>,
    withdrawals: broadcast::Sender<Withdrawal>,
    /// The humans' answers to their assistants' acts and their pre-allow rules, verified as the grants are.
    consents: RwLock<Consents>,
    /// Each human whose consent records changed.
    consented: broadcast::Sender<UserId>,
    /// Whether the watch has read the bucket once ([`Registry::loaded`]).
    loaded: watch::Sender<bool>,
}

impl Assistants {
    fn new(account: Option<String>) -> Self {
        Self {
            grants: RwLock::default(),
            account,
            withdrawals: broadcast::channel(WITHDRAWALS).0,
            consents: RwLock::default(),
            consented: broadcast::channel(WITHDRAWALS).0,
            loaded: watch::channel(false).0,
        }
    }
}

/// That `human` stopped designating `assistant` as its assistant: cleared, replaced, deleted or refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Withdrawal {
    pub human: UserId,
    pub assistant: UserId,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new(UserId::human())
    }
}

impl Registry {
    /// A registry knowing only the humans without an entry: `human` and `default`, the default human.
    pub fn new(default: UserId) -> Self {
        let registry = Self { humans: Arc::default(), default, assistants: Arc::new(Assistants::new(None)) };
        registry.replace(registry.base());
        registry
    }

    /// The registry, honouring the assistant grants that devices of the team of `account` (`A...`, the issuer of the
    /// reader's credentials) signed; without an account, none.
    #[must_use]
    pub fn verifying_grants(mut self, account: Option<String>) -> Self {
        self.assistants = Arc::new(Assistants::new(account));
        self
    }

    /// The ant `human` designates as its assistant by a grant standing now. Its acts count for the human only while
    /// its own user JWT names the human too ([`Registry::honours_assistant`]).
    pub fn assistant(&self, human: &UserId) -> Option<UserId> {
        self.assistants.grants.read_locked().designated(human).cloned()
    }

    /// Whether `assistant`, whose user JWT's assistant_of tag names `assistant_of` (as the reader verified it), acts
    /// for `human` now ([`ants_proto::VerifiedGrant::honours`]).
    pub fn honours_assistant(&self, human: &UserId, assistant: &UserId, assistant_of: Option<&UserId>) -> bool {
        self.assistants.grants.read_locked().honours(human, assistant, assistant_of)
    }

    /// The record of `human` standing: a grant, or a withdrawal.
    pub fn grant(&self, human: &UserId) -> Option<VerifiedGrant> {
        self.assistants.grants.read_locked().grant(human).cloned()
    }

    /// Each assistant that stops being one from now on, as the watch sees it: whatever it did for its human that
    /// is not confirmed yet no longer counts.
    pub fn withdrawals(&self) -> broadcast::Receiver<Withdrawal> {
        self.assistants.withdrawals.subscribe()
    }

    /// The answer of `human`, by its signed record standing now, to `act` its assistant `assistant` made for it.
    pub fn answer(&self, human: &UserId, assistant: &UserId, act: &Act) -> Option<ActAnswer> {
        self.assistants.consents.read_locked().answer(human, assistant, act)
    }

    /// The answers of `human` standing now, as signed: the credentials that signed carry what else the human is.
    pub fn confirmations(&self, human: &UserId) -> Option<Confirmations> {
        self.assistants.consents.read_locked().confirmations(human).cloned()
    }

    /// The pre-allow rules `human` signed for its assistant `assistant`, standing now.
    pub fn pre_allow(&self, human: &UserId, assistant: &UserId) -> Option<PreAllow> {
        self.assistants.consents.read_locked().pre_allow(human, assistant).cloned()
    }

    /// Each human whose answers or pre-allow rules change from now on, as the watch sees it; a listener lagging
    /// behind should look again at every human it waits for.
    pub fn consents(&self) -> broadcast::Receiver<UserId> {
        self.assistants.consented.subscribe()
    }

    /// Wait until the watch has read the bucket once ([`Registry::watch`]): until then, no grant stands, which does not
    /// mean that a grant was withdrawn.
    pub async fn loaded(&self) {
        let mut loaded = self.assistants.loaded.subscribe();
        let _ = loaded.wait_for(|loaded| *loaded).await;
    }

    /// Whether the watch has read the bucket once ([`Registry::loaded`]).
    pub fn is_loaded(&self) -> bool {
        *self.assistants.loaded.borrow()
    }

    /// The human who gets what is meant for the owner of a thread that has none.
    pub fn default_human(&self) -> &UserId {
        &self.default
    }

    /// `owner`, the owner a message names for its thread, when it is a human. Any ant can name any owner, and an
    /// ant's daemon answers what goes to the owner with a fresh turn budget: a thread an ant owned would never
    /// run out of turns. So an owner that is no human is no owner.
    pub fn human_owner(&self, owner: Option<&UserId>) -> Option<UserId> {
        owner.filter(|owner| self.is_human(owner)).cloned()
    }

    /// `creator`, the creator a message names for its thread, when it is no human: only an ant creates a thread.
    pub fn ant_creator(&self, creator: Option<&UserId>) -> Option<UserId> {
        creator.filter(|creator| !self.is_human(creator)).cloned()
    }

    /// The human who gets what is meant for the owner `owner` names: it when a human ([`Registry::human_owner`]),
    /// else the default human.
    pub fn owner_or_default(&self, owner: Option<&UserId>) -> UserId {
        self.human_owner(owner).unwrap_or_else(|| self.default.clone())
    }

    /// The human who gets the escalations of `envelope`'s thread: its owner when a human, else the default human.
    pub fn owner_of(&self, envelope: &Envelope) -> UserId {
        self.owner_or_default(envelope.owner.as_ref())
    }

    pub fn role(&self, user: &UserId) -> Role {
        self.read().role(user)
    }

    pub fn is_human(&self, user: &UserId) -> bool {
        self.role(user).is_human()
    }

    /// The role of the sender of `envelope`: an ant's for a webhook's message ([`Envelope::sender_role`]).
    pub fn sender_role(&self, envelope: &Envelope) -> Role {
        envelope.sender_role(&self.read())
    }

    /// The humans known now.
    pub fn humans(&self) -> Humans {
        self.read().clone()
    }

    /// Keep the registry current with the bucket, until the returned handle drops: the bucket is opened (never
    /// created), read and watched; when it is missing or its watch ends, it is looked for again after `retry`.
    pub fn watch(&self, js: jetstream::Context, retry: Duration) -> Watching {
        let registry = self.clone();
        Watching(tokio::spawn(async move {
            loop {
                match js.get_key_value(HUMANS_BUCKET).await {
                    Ok(kv) => match registry.follow(&kv).await {
                        Ok(()) => tracing::warn!("the watch of the humans ended; watching again"),
                        Err(e) => tracing::warn!(error = format!("{e:#}"), "watching the humans"),
                    },
                    Err(e) if e.kind() == KeyValueErrorKind::GetBucket => {
                        tracing::debug!(error = %e, "no humans bucket; only the humans without an entry are known");
                    }
                    Err(e) => tracing::warn!(error = %e, "opening the humans bucket"),
                }
                tokio::time::sleep(retry).await;
            }
        }))
    }

    /// Read the humans of `kv` in place of the known ones, then apply its changes until the watch ends. The watch
    /// starts first, so no change made while the entries are read is missed.
    async fn follow(&self, kv: &kv::Store) -> Result<()> {
        let mut updates = kv.guarded_watch(">").await.context("watching the humans")?;
        let keys: Vec<String> = kv.keys().await.context("listing the humans")?.try_collect().await?;
        let mut humans = self.base();
        let (mut granting, mut consenting) = (Vec::new(), Vec::new());
        for key in keys {
            let value = kv.guarded_get(&key).await.with_context(|| format!("reading human {key}"))?;
            if let Some(human) = assistant_grant_of(&key) {
                granting.push(human);
                self.set_grant(&key, value.as_deref());
            } else if ConsentKey::parse(&key).is_some() {
                self.set_consent(&key, value.as_deref());
                consenting.push(key);
            } else {
                self.set(&mut humans, &key, value.as_deref());
            }
        }
        self.replace(humans);
        // Grants deleted while the bucket was not watched.
        let gone: Vec<UserId> = self.assistants.grants.read_locked().designations().map(|(h, _)| h.clone()).collect();
        for human in gone.into_iter().filter(|h| !granting.contains(h)) {
            self.set_grant(&ants_proto::assistant_grant_key(&human), None);
        }
        // Consent records deleted while the bucket was not watched.
        let consenting_now: Vec<UserId> = self.assistants.consents.read_locked().humans().cloned().collect();
        for human in consenting_now {
            for key in [ants_proto::confirmations_key(&human), ants_proto::pre_allow_key(&human)] {
                if !consenting.contains(&key) {
                    self.set_consent(&key, None);
                }
            }
        }
        self.assistants.loaded.send_replace(true);
        while let Some(entry) = updates.next().await {
            let entry = entry.context("watching the humans")?;
            let value = (entry.operation == kv::Operation::Put).then_some(&entry.value[..]);
            if assistant_grant_of(&entry.key).is_some() {
                self.set_grant(&entry.key, value);
            } else if ConsentKey::parse(&entry.key).is_some() {
                self.set_consent(&entry.key, value);
            } else {
                let mut humans = self.humans.write_locked();
                self.set(&mut humans, &entry.key, value);
            }
        }
        Ok(())
    }

    /// Apply `value`, what the grant key `key` holds now, to the grants, announcing the assistant it withdraws.
    fn set_grant(&self, key: &str, value: Option<&[u8]>) {
        let Some(account) = &self.assistants.account else { return };
        let applied = self.assistants.grants.write_locked().apply(key, value, account, chrono::Utc::now());
        let Some(applied) = applied else { return };
        if let Some(refused) = &applied.refused {
            tracing::warn!(key, error = %refused, "not honouring an assistant grant");
        }
        if let Some(assistant) = applied.withdrawn() {
            tracing::info!(human = %applied.human, %assistant, "assistant withdrawn");
            // Fails only with no one listening.
            let _ = self
                .assistants
                .withdrawals
                .send(Withdrawal { human: applied.human.clone(), assistant: assistant.clone() });
        }
    }

    /// Apply `value`, what the consent key `key` holds now, to the consent records, announcing whose changed.
    fn set_consent(&self, key: &str, value: Option<&[u8]>) {
        let Some(account) = &self.assistants.account else { return };
        let applied = self.assistants.consents.write_locked().apply(key, value, account, chrono::Utc::now());
        let Some(applied) = applied else { return };
        if let Some(refused) = &applied.refused {
            tracing::warn!(key, error = %refused, "not honouring a human's consent to its assistant's acts");
        }
        // Fails only with no one listening.
        let _ = self.assistants.consented.send(applied.human);
    }

    /// Set the human of `key` in `humans` to `value`, or remove it. A malformed entry, or one naming another human
    /// than its key, is skipped and names no human.
    fn set(&self, humans: &mut Humans, key: &str, value: Option<&[u8]>) {
        // Another key under a human's name, not its entry.
        if key.contains('.') {
            return;
        }
        let Ok(human) = key.parse::<UserId>() else {
            tracing::warn!(key, "skipping a human entry under a key that is no user id");
            return;
        };
        let added = value.map(|value| {
            serde_json::from_slice::<HumanEntry>(value)
                .map_err(|e| e.to_string())
                .and_then(|entry| humans.add(key, entry).map_err(|e| e.to_string()))
        });
        match added {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                tracing::warn!(key, error, "skipping a human entry");
                self.remove(humans, &human);
            }
            None => self.remove(humans, &human),
        }
    }

    fn remove(&self, humans: &mut Humans, human: &UserId) {
        if *human != self.default {
            humans.remove(human);
        }
    }

    /// The humans without an entry.
    fn base(&self) -> Humans {
        let mut humans = Humans::default();
        let _ = humans.add(self.default.as_str(), HumanEntry { human: self.default.clone(), registered_at: None });
        humans
    }

    fn replace(&self, humans: Humans) {
        *self.humans.write_locked() = humans;
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Humans> {
        self.humans.read_locked()
    }

    /// A registry of `default` and `humans`, as if the bucket listed them.
    #[cfg(test)]
    pub fn with(default: UserId, humans: &[&str]) -> Self {
        let registry = Self::new(default);
        let mut known = registry.base();
        for name in humans {
            let entry = HumanEntry::new(name.parse().unwrap());
            known.add(name, entry).unwrap();
        }
        registry.replace(known);
        registry.assistants.loaded.send_replace(true);
        registry
    }

    /// Apply `value`, what the grant or consent key `key` holds now, as the watch would.
    #[cfg(test)]
    pub fn put_record(&self, key: &str, value: Option<&[u8]>) {
        self.assistants.loaded.send_replace(true);
        if assistant_grant_of(key).is_some() {
            self.set_grant(key, value);
        } else {
            self.set_consent(key, value);
        }
    }
}

/// A device of a human of a team of its own, signing records as the human's devices do.
#[cfg(test)]
pub struct TestDevice {
    account: nkeys::KeyPair,
    key: nkeys::KeyPair,
    id: ants_proto::DeviceId,
}

#[cfg(test)]
impl Default for TestDevice {
    fn default() -> Self {
        let (account, key) = (nkeys::KeyPair::new_account(), nkeys::KeyPair::new_user());
        Self { account, key, id: ants_proto::DeviceId::generate() }
    }
}

#[cfg(test)]
impl TestDevice {
    /// The team's account.
    pub fn account(&self) -> String {
        self.account.public_key()
    }

    /// The user JWT of the device's credentials, for `human`.
    pub fn jwt(&self, human: &UserId) -> String {
        self.signed_jwt(human, &self.id.tag())
    }

    /// The user JWT of the credentials of `assistant` as the assistant of `human`, signed by the team's account.
    pub fn assistant_jwt(&self, assistant: &UserId, human: &UserId) -> String {
        self.signed_jwt(assistant, &ants_proto::AssistantOf::tag(human))
    }

    fn signed_jwt(&self, name: &UserId, tag: &str) -> String {
        use data_encoding::BASE64URL_NOPAD;
        let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
        let claims = serde_json::json!({
            "sub": self.key.public_key(), "iss": self.account.public_key(), "name": name,
            "nats": { "type": "user", "tags": [tag] },
        });
        let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
        let signature = BASE64URL_NOPAD.encode(&self.account.sign(format!("{header}.{body}").as_bytes()).unwrap());
        format!("{header}.{body}.{signature}")
    }

    /// The signature of `signed`, base64url without padding.
    pub fn sign(&self, signed: &[u8]) -> String {
        data_encoding::BASE64URL_NOPAD.encode(&self.key.sign(signed).unwrap())
    }
}

/// The account whose devices' grants a reader with the credentials file `creds` honours
/// ([`Registry::verifying_grants`]): the one that signed them. None without credentials, or with a warning when they
/// cannot be read.
pub fn account(creds: Option<&std::path::Path>) -> Option<String> {
    let creds = creds?;
    crate::secrets::creds::account(creds)
        .inspect_err(|e| tracing::warn!(error = format!("{e:#}"), "honouring no assistant grant"))
        .ok()
}

/// The watch of [`Registry::watch`], stopped when dropped.
pub struct Watching(JoinHandle<()>);

impl Drop for Watching {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str) -> UserId {
        name.parse().unwrap()
    }

    #[test]
    fn humans_without_an_entry() {
        let registry = Registry::new(user("alice"));
        assert!(registry.is_human(&UserId::human()));
        assert!(registry.is_human(&user("alice")));
        assert!(!registry.is_human(&user("bob")));
    }

    #[test]
    fn escalations_go_to_the_owner_else_the_default_human() {
        let registry = Registry::with(user("alice"), &["carol"]);
        let envelope = Envelope::new(user("bob"), user("coder"), "hi");
        assert_eq!(registry.owner_of(&envelope), user("alice"));
        let owned = envelope.clone().with_owner(Some(user("carol")));
        assert_eq!(registry.owner_of(&owned), user("carol"));
        let by_ant = envelope.with_owner(Some(user("dev-a")));
        assert_eq!(registry.owner_of(&by_ant), user("alice"), "an ant owns no thread");
        assert_eq!(registry.human_owner(by_ant.owner.as_ref()), None);
    }

    #[test]
    fn entries_add_and_remove_humans() {
        let registry = Registry::new(user("alice"));
        let mut humans = registry.base();
        let entry = |name: &str| serde_json::to_vec(&HumanEntry::new(user(name))).unwrap();
        registry.set(&mut humans, "carol", Some(&entry("carol")));
        registry.set(&mut humans, "dave", Some(&entry("carol")));
        registry.set(&mut humans, "erin", Some(b"not json"));
        assert!(humans.contains(&user("carol")));
        assert!(!humans.contains(&user("dave")), "an entry naming another human is forged");
        assert!(!humans.contains(&user("erin")));
        registry.set(&mut humans, "carol", None);
        registry.set(&mut humans, "alice", None);
        assert!(!humans.contains(&user("carol")));
        assert!(humans.contains(&user("alice")), "the default human stays a human");
    }
}
