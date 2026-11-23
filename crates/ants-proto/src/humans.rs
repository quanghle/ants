//! Who is a human: a human's [`HumanEntry`] in [`crate::HUMANS_BUCKET`], which only humans write; the team's [`Humans`]
//! read from it, which give a sender its [`crate::Role`]; and whether a user JWT's publish allowances are a human's
//! ([`writes_human_entries`]).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Error, HUMANS_BUCKET, Role, UserId, subject};

/// A human's entry in the [`HUMANS_BUCKET`], under the key of its user id. Only humans write it.
///
/// Readers should treat unknown fields as additions, not errors, and skip an entry naming another human than its key
/// ([`HumanEntry::check_key`], [`Humans::add`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanEntry {
    pub human: UserId,
    /// When the human was registered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registered_at: Option<DateTime<Utc>>,
}

impl HumanEntry {
    /// The entry of `human`, registered now.
    pub fn new(human: UserId) -> Self {
        Self { human, registered_at: Some(Utc::now()) }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> &str {
        self.human.as_str()
    }

    /// Check that the entry was read from its own key; a reader skips one naming another human.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "human entry", user: self.human.clone(), at: format!("key {key}") })
    }
}

/// The humans of a team: [`UserId::human`], always, and every user [`HUMANS_BUCKET`] lists under its own key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Humans(std::collections::BTreeSet<UserId>);

impl Default for Humans {
    fn default() -> Self {
        Self([UserId::human()].into())
    }
}

impl Humans {
    /// The default human and the humans of `entries`, as pairs of a KV key and the entry read from it. Entries naming
    /// another human than their key are skipped.
    pub fn from_entries<'a>(entries: impl IntoIterator<Item = (&'a str, HumanEntry)>) -> Self {
        let mut humans = Self::default();
        for (key, entry) in entries {
            let _ = humans.add(key, entry);
        }
        humans
    }

    /// Add the human of `entry`, read from `key`; an entry naming another human is refused and changes nothing.
    pub fn add(&mut self, key: &str, entry: HumanEntry) -> Result<(), Error> {
        entry.check_key(key)?;
        self.0.insert(entry.human);
        Ok(())
    }

    /// Drop `human`, whose entry was deleted. The default human stays a human.
    pub fn remove(&mut self, human: &UserId) {
        if !human.is_human() {
            self.0.remove(human);
        }
    }

    pub fn contains(&self, user: &UserId) -> bool {
        self.0.contains(user)
    }

    /// The role of `user`: [`Role::Human`] for a human, else [`Role::Ant`].
    pub fn role(&self, user: &UserId) -> Role {
        if self.contains(user) { Role::Human } else { Role::Ant }
    }

    pub fn iter(&self) -> impl Iterator<Item = &UserId> {
        self.0.iter()
    }
}

/// Whether the publish allowances `publish` of a user JWT are a human's: they write every human's entry in
/// [`HUMANS_BUCKET`] (`$KV.ants_humans.*`), as no other credentials do.
pub fn writes_human_entries<'a>(publish: impl IntoIterator<Item = &'a str>) -> bool {
    let entries = subject::kv(HUMANS_BUCKET, "*");
    publish.into_iter().any(|s| s == entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{p, subject};

    #[test]
    fn humans_are_the_default_one_and_those_listed_under_their_own_key() {
        let entry = HumanEntry::new(p("alice"));
        let json = serde_json::to_string(&entry).unwrap();
        assert_eq!(serde_json::from_str::<HumanEntry>(&json).unwrap(), entry);
        let read = |json: &str| serde_json::from_str::<HumanEntry>(json).unwrap();
        let bob = read(r#"{"human":"bob","team":"ops"}"#);
        assert_eq!(bob.registered_at, None);
        assert_eq!(bob.check_key("bob"), Ok(()));
        assert_eq!(bob.check_key("dev-a").unwrap_err().to_string(), "human entry of bob found at key dev-a");
        assert_eq!(subject::human_entry(&p("bob")), "$KV.ants_humans.bob");

        let mut humans = Humans::from_entries([("alice", entry), ("dev-a", read(r#"{"human":"carol"}"#))]);
        assert!(humans.contains(&UserId::human()), "the default human needs no entry");
        assert!(humans.contains(&p("alice")));
        assert!(!humans.contains(&p("dev-a")) && !humans.contains(&p("carol")), "a mismatched entry is skipped");
        assert_eq!((humans.role(&p("alice")), humans.role(&p("dev-a"))), (Role::Human, Role::Ant));
        assert!(humans.add("dev-b", bob.clone()).is_err() && !humans.contains(&p("bob")));
        humans.add("bob", bob).unwrap();
        humans.remove(&p("bob"));
        humans.remove(&UserId::human());
        assert_eq!(humans.iter().map(UserId::as_str).collect::<Vec<_>>(), ["alice", "human"]);
    }
}
