use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[cfg(doc)]
use crate::IDENTITIES_BUCKET;
use crate::{Error, Machine, UserId};

/// The keys of an ant's `[[ant]]` table that are the ant's own rather than its machine's: its role and core memories
/// (`whoami`, `memory`), which [`SharedSettings`] holds for every machine serving it. The rest (workdir, env, harness,
/// `tasks` and the others) stays the machine's config.
pub const SHARED_FIELDS: [&str; 9] =
    ["whoami", "memory", "capabilities", "model", "reasoning_effort", "context", "fallback", "allow", "escalate_to"];

/// Whether `key`, the first key of a field of an ant's table, is one of [`SHARED_FIELDS`].
pub fn is_shared_field(key: &str) -> bool {
    SHARED_FIELDS.contains(&key)
}

/// An ant's shared settings in [`IDENTITIES_BUCKET`], under [`SharedSettings::key_of`] its name: the ant's role
/// settings ([`SHARED_FIELDS`]), the same on every machine. A machine's host connection creates it, when absent, from
/// the ant's table in its config file, with a KV create, so across machines the first creator wins; an admin's
/// settings order changes it by compare-and-set. The daemon serving the ant runs it with these values in place of its
/// config's. Only host credentials write it; humans and ants read it. Readers should treat unknown fields as
/// additions, not errors.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SharedSettings {
    pub ant: UserId,
    /// The settings, by key of the ant's table, valued as admin orders carry them; a key left out stands for the
    /// setting's default.
    #[serde(default)]
    pub fields: Map<String, Value>,
    /// The machine whose daemon last wrote it.
    pub machine: Machine,
    /// The admin whose order last changed it; none while it holds what a machine's config seeded it with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<UserId>,
    pub updated_at: DateTime<Utc>,
}

impl SharedSettings {
    /// The key of `ant`'s shared settings: `<ant>.settings`, which no ant's credentials and no human's write.
    pub fn key_of(ant: &UserId) -> String {
        format!("{ant}.settings")
    }

    pub fn key(&self) -> String {
        Self::key_of(&self.ant)
    }

    /// Check that the record was read from its own key.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "shared settings", user: self.ant.clone(), at: format!("key {key}") })
    }

    /// The fields of `local`, a machine's values of the shared settings, that differ from these: the machine's drift.
    /// A field `local` leaves out is no drift. Sorted.
    pub fn drift(&self, local: &Map<String, Value>) -> Vec<String> {
        let mut drift: Vec<String> = local
            .iter()
            .filter(|(key, value)| self.fields.get(*key) != Some(value))
            .map(|(key, _)| key.clone())
            .collect();
        drift.sort();
        drift
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn drift_names_the_fields_the_machine_sets_otherwise() {
        let shared = SharedSettings {
            ant: "dev-a".parse().unwrap(),
            fields: json!({ "model": "gpt-5", "capabilities": ["rust"] }).as_object().unwrap().clone(),
            machine: "m1".parse().unwrap(),
            by: None,
            updated_at: Utc::now(),
        };
        assert_eq!(shared.key(), "dev-a.settings");
        let local = json!({ "model": "gpt-4", "capabilities": ["rust"], "whoami": "a dev" });
        assert_eq!(shared.drift(local.as_object().unwrap()), ["model", "whoami"]);
        assert!(shared.drift(&Map::new()).is_empty(), "a machine setting nothing drifts in nothing");
        assert!(shared.check_key("dev-a.settings").is_ok());
        assert!(shared.check_key("dev-b.settings").is_err());
        assert!(is_shared_field("escalate_to") && !is_shared_field("tasks") && !is_shared_field("workdir"));
    }
}
