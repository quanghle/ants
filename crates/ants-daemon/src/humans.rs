//! The team's humans as a daemon sees them.
//!
//! A human is a user listed in the [`HUMANS_BUCKET`], which only humans write; `human` and the daemon's default
//! human (config `human`) are humans without an entry. The daemon keeps the bucket's humans in a [`Registry`], kept
//! current by a watch ([`Registry::watch`]). Until the bucket exists only the humans without an entry are known; the
//! daemon looks for the bucket again every little while, since an ant may read it but not create it.

use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use ants_proto::{Envelope, HUMANS_BUCKET, HumanEntry, Humans, Role, UserId};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, context::KeyValueErrorKind, kv};
use futures::{StreamExt as _, TryStreamExt as _};
use tokio::task::JoinHandle;

/// How long the daemon waits to look for the bucket again, when it is missing or its watch ended.
pub const RETRY: Duration = Duration::from_secs(30);

/// The humans of the team, shared by the inbox loop and the MCP server.
#[derive(Debug, Clone)]
pub struct Registry {
    humans: Arc<RwLock<Humans>>,
    default: UserId,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new(UserId::human())
    }
}

impl Registry {
    /// A registry knowing only the humans without an entry: `human` and `default`, the default human.
    pub fn new(default: UserId) -> Self {
        let registry = Self { humans: Arc::default(), default };
        registry.replace(registry.base());
        registry
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
        let mut updates = kv.watch_all().await.context("watching the humans")?;
        let keys: Vec<String> = kv.keys().await.context("listing the humans")?.try_collect().await?;
        let mut humans = self.base();
        for key in keys {
            let value = kv.get(&key).await.with_context(|| format!("reading human {key}"))?;
            self.set(&mut humans, &key, value.as_deref());
        }
        self.replace(humans);
        while let Some(entry) = updates.next().await {
            let entry = entry.context("watching the humans")?;
            let value = (entry.operation == kv::Operation::Put).then_some(&entry.value[..]);
            let mut humans = self.humans.write().unwrap_or_else(PoisonError::into_inner);
            self.set(&mut humans, &entry.key, value);
        }
        Ok(())
    }

    /// Set the human of `key` in `humans` to `value`, or remove it. A malformed entry, or one naming another human
    /// than its key, is skipped and names no human.
    fn set(&self, humans: &mut Humans, key: &str, value: Option<&[u8]>) {
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
        *self.humans.write().unwrap_or_else(PoisonError::into_inner) = humans;
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Humans> {
        self.humans.read().unwrap_or_else(PoisonError::into_inner)
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
        registry
    }
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
