//! Machines: a machine's [`MachineRecord`] in [`crate::MACHINES_BUCKET`], the ants its daemon serves, put with the
//! machine's host credentials; the [`AccountHolder`] record naming where the team's account key is; and the names of a
//! machine's host connection ([`host_user`], [`host_inbox_prefix`], [`admin_consumer`]).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::{ANTS_VERSION, COLONY_BUCKET, MACHINES_BUCKET, subject};
use crate::{Machine, UserId};

/// A machine's record in [`MACHINES_BUCKET`], under the key of its name: the ants its daemon serves. The daemon puts
/// it with the machine's host credentials ([`host_user`]) on start and whenever the ants it serves change, and never
/// deletes it, so an ant whose daemon stopped, gone from the [`COLONY_BUCKET`], still has a machine. Readers should
/// treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineRecord {
    pub machine: Machine,
    /// The ants the daemon serves, sorted, the built-in messenger left out.
    #[serde(default)]
    pub ants: Vec<UserId>,
    /// Host name of the machine, as the colony entries of its ants name it.
    #[serde(default)]
    pub hostname: String,
    /// The version of ants the daemon runs ([`ANTS_VERSION`]).
    #[serde(default)]
    pub version: String,
    /// When the daemon put the record.
    pub updated_at: DateTime<Utc>,
    /// By ant, the settings the machine's config sets otherwise than the ant's [`crate::SharedSettings`]: drift to
    /// mend in that config. Missing when none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub drift: BTreeMap<UserId, Vec<String>>,
}

impl MachineRecord {
    pub fn key(&self) -> &str {
        self.machine.as_str()
    }
}

/// The key in [`MACHINES_BUCKET`] of the [`AccountHolder`] record: no machine's name starts with `_`, so no machine's
/// host credentials write it, and [`subject::machine_records`] readers skip it.
pub const ACCOUNT_HOLDER_KEY: &str = "_account";

/// Where the team's account key is, in [`MACHINES_BUCKET`] under [`ACCOUNT_HOLDER_KEY`]: `ants signer` puts it, with
/// credentials it signs with that key, each time it connects, so that machines without the key can name the one that
/// answers enrollment and renewal requests. Only credentials signed there write it ([`crate::Permissions::for_account_holder`]);
/// humans read it. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountHolder {
    /// The machine's name, as its `[host]` table names it, else its config's top-level `machine` if that is a machine
    /// name, else the first label of its host name.
    pub machine: Machine,
    /// Host name of the machine.
    #[serde(default)]
    pub hostname: String,
    /// The version of ants the signer runs ([`ANTS_VERSION`]).
    #[serde(default)]
    pub version: String,
    /// When the signer last put the record: it does so every time it connects.
    pub updated_at: DateTime<Utc>,
}

/// The name in the user JWT of a machine's host credentials (`ants auth host`), with which its daemon writes its own
/// [`MachineRecord`] ([`crate::Permissions::for_host`]).
pub fn host_user(machine: &Machine) -> String {
    format!("host-{machine}")
}

/// Inbox prefix of a machine's host connection: apart from every user's (`_INBOX.<user>`), so no user hears its
/// replies, whatever its name.
pub fn host_inbox_prefix(machine: &Machine) -> String {
    format!("_INBOX_HOST.{machine}")
}

/// Durable consumer of the `ANTS` stream a machine's host connection takes the admin orders to the machine's ants from
/// ([`subject::admin_orders`]).
pub fn admin_consumer(machine: &Machine) -> String {
    format!("admin-{machine}")
}

/// The machines whose records list `ant`, the latest put first: where it is served, or was last.
pub fn machines_of<'a>(records: &'a [MachineRecord], ant: &UserId) -> Vec<&'a MachineRecord> {
    let mut found: Vec<&MachineRecord> = records.iter().filter(|r| r.ants.contains(ant)).collect();
    found.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then_with(|| a.machine.cmp(&b.machine)));
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(machine: &str, ants: &[&str], at: i64) -> MachineRecord {
        MachineRecord {
            machine: machine.parse().unwrap(),
            ants: ants.iter().map(|a| a.parse().unwrap()).collect(),
            hostname: format!("{machine}.local"),
            version: "0.1.0".to_owned(),
            updated_at: DateTime::from_timestamp(at, 0).unwrap(),
            drift: BTreeMap::new(),
        }
    }

    #[test]
    fn finds_the_machines_of_an_ant_latest_first() {
        let records = [record("m1", &["dev-a", "dev-b"], 10), record("m2", &["dev-a"], 20), record("m3", &[], 30)];
        let dev_a: Vec<&str> = machines_of(&records, &"dev-a".parse().unwrap()).iter().map(|r| r.key()).collect();
        assert_eq!(dev_a, ["m2", "m1"]);
        assert!(machines_of(&records, &"dev-c".parse().unwrap()).is_empty());
    }

    #[test]
    fn a_record_reads_with_fields_missing_or_added() {
        let text = r#"{"machine":"m1","updated_at":"2025-01-01T00:00:00Z","later":1}"#;
        let record: MachineRecord = serde_json::from_str(text).unwrap();
        assert_eq!((record.key(), record.ants.len()), ("m1", 0));
    }
}
