//! An ant's shared settings ([`SharedSettings`]): its role and core memories ([`SHARED_FIELDS`]), the same on every
//! machine serving it, in [`crate::IDENTITIES_BUCKET`], put by host connections and admins' settings orders, signed
//! with the ant's own credentials for the stream sequence and bucket [`incarnation`] it is stored at, so a reader
//! refuses one put back ([`SharedSettings::verify`]).

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[cfg(doc)]
use crate::IDENTITIES_BUCKET;
use crate::{Error, JwtError, Machine, UserId, VerifiedUser, canonical_json, verified_jwt, verify_user_jwt};

/// The first line of what an ant's key signs of its shared settings ([`SharedSettings::signed`]).
pub const SETTINGS_SIGNING_CONTEXT: &str = "ants-shared-settings";

/// The key of [`IDENTITIES_BUCKET`] whose last message marks the bucket's incarnation ([`incarnation`]), written by
/// `ants init` when absent and anew by `ants init --identities-restored`. No ant's name: ant names hold no `_`.
pub const INCARNATION_KEY: &str = "_incarnation";

/// The incarnation of [`IDENTITIES_BUCKET`] its marker ([`INCARNATION_KEY`]) names, stored at stream sequence
/// `sequence` at `stored`: `<sequence>@<RFC 3339 time with nanoseconds>`. Writing the marker starts a new
/// incarnation, which no later write reproduces, as the server sets both the sequence and the time; a stream restored
/// from a backup takes back the backup's marker, so that records written after the backup verify there again until the
/// marker is written anew.
pub fn incarnation(sequence: u64, stored: DateTime<Utc>) -> String {
    format!("{sequence}@{}", stored.to_rfc3339_opts(SecondsFormat::Nanos, true))
}

/// The keys of an ant's `[[ant]]` table that are the ant's own rather than its machine's: its role and core memories
/// (`whoami`, `memory`) and the ant reviewing it (`sage`), which [`SharedSettings`] holds for every machine serving it.
/// The rest (workdir, env, harness, `tasks` and the others) stays the machine's config. Of an ant added from a
/// template, [`SharedSettings`] holds only those a request to the template chooses: `fallback`, `allow` and
/// `escalate_to` follow the template, and `sage` the config's top-level one.
pub const SHARED_FIELDS: [&str; 10] = [
    "whoami",
    "memory",
    "capabilities",
    "model",
    "reasoning_effort",
    "context",
    "fallback",
    "allow",
    "escalate_to",
    "sage",
];

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
///
/// As any machine's host credentials may write any ant's record, its writer signs it with credentials of the ant
/// itself, which only the machines serving the ant hold ([`SharedSettings::credentials`]); a daemon of a bus with
/// credentials applies only a record it verifies ([`SharedSettings::verify`]).
///
/// A record the ant once signed stays signed, so a host may put back one replaced since (a replay). The bucket keeps
/// one revision per key, so a reader cannot check a record against its predecessor; instead the record is signed for
/// the stream sequence it is stored at ([`SharedSettings::sequence`]), which its writer fixes by publishing it only if
/// the stream's last message is the one before. Stream sequences are never reused, so a record put back is stored at
/// another sequence than the one it was signed for, and every reader refuses it, whether or not it read the record
/// replacing it. As a bucket created again, or restored from a backup, may reach the sequences of the records of the
/// one before, the record is signed for the bucket's incarnation too ([`SharedSettings::incarnation`]), and readers
/// refuse every record in a bucket without one. A record signed with a key revoked since (a revocation record in
/// [`crate::DEVICES_BUCKET`]) counts no more, whenever it was stored.
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
    /// The sequence of the bucket's stream the record was written to be stored at: its writer publishes it only if the
    /// stream's last message is at the one before. Stored at any other, it was put back ([`SharedSettings::verify`]).
    pub sequence: u64,
    /// The incarnation of the bucket the record was written in ([`incarnation`]): its marker's when written, empty
    /// without one on a server without authentication.
    pub incarnation: String,
    /// The user JWT of the ant's credentials whose key signed the record; none on a server without authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
    /// The signature of [`SharedSettings::signed`] by the key of `credentials`, base64url without padding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// Why a reader does not apply an ant's shared settings ([`SharedSettings::verify`]).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    #[error("signed by no credentials of the ant")]
    Unsigned,
    #[error("the signing credentials: {0}")]
    Credentials(#[from] JwtError),
    #[error("signed by the credentials of {named}, not of {ant}")]
    NotTheAnt { named: UserId, ant: UserId },
    #[error("signed by revoked credentials ({0})")]
    Revoked(String),
    #[error("signed to be stored at sequence {signed} but stored at {stored}: put back after it was written")]
    PutBack { signed: u64, stored: u64 },
    #[error(
        "the bucket has no incarnation marker ({INCARNATION_KEY}), as when created otherwise than by `ants init`, \
         which writes it"
    )]
    NoIncarnation,
    #[error(
        "signed in incarnation {signed} of the bucket, now in {current}: written before the bucket was restored or \
         created again"
    )]
    OtherIncarnation { signed: String, current: String },
}

impl SettingsError {
    /// Whether the record is one the ant signed, put back after it was replaced, or written in an incarnation of the
    /// bucket before this one: a newer one stands for it.
    pub fn is_replay(&self) -> bool {
        matches!(self, Self::PutBack { .. } | Self::OtherIncarnation { .. })
    }
}

impl SharedSettings {
    /// The record of `ant` holding `fields`, written now on `machine`, by `by` when an admin's order changed it,
    /// unsigned and for no sequence yet.
    pub fn new(ant: UserId, fields: Map<String, Value>, machine: Machine, by: Option<UserId>) -> Self {
        let (sequence, incarnation, updated_at) = (0, String::new(), Utc::now());
        Self { ant, fields, machine, by, updated_at, sequence, incarnation, credentials: None, signature: None }
    }

    /// The key of `ant`'s shared settings: `<ant>.settings`, which no ant's credentials and no human's write.
    pub fn key_of(ant: &UserId) -> String {
        format!("{ant}.settings")
    }

    pub fn key(&self) -> String {
        Self::key_of(&self.ant)
    }

    /// What the ant's key signs: lines joined by `\n`, [`SETTINGS_SIGNING_CONTEXT`], the ant, the fields as canonical
    /// JSON ([`canonical_json`]), the machine, the admin or an empty line, `updated_at` in RFC 3339 with nanoseconds
    /// and `Z`, the sequence in decimal and the incarnation. Every field but the JWT, which the signing key is checked
    /// against, and the signature.
    pub fn signed(&self) -> Vec<u8> {
        let fields = canonical_json(&Value::Object(self.fields.clone()));
        let updated_at = self.updated_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let by = self.by.as_ref().map_or("", UserId::as_str);
        let sequence = self.sequence.to_string();
        let machine = self.machine.as_str();
        [SETTINGS_SIGNING_CONTEXT, self.ant.as_str(), &fields, machine, by, &updated_at, &sequence, &self.incarnation]
            .join("\n")
            .into_bytes()
    }

    /// The public key (`U...`) of the credentials that signed, as their JWT says before anything checks it: the key
    /// whose revocation record to look up for [`SharedSettings::verify`].
    pub fn signing_key(&self) -> Option<String> {
        verified_jwt(self.credentials.as_deref()?).ok().map(|(claims, _)| claims.sub)
    }

    /// Check that the record, which the stream stored at `stored_at` as its message `stored` (the stream sequence), is
    /// signed by the key of credentials of the ant that one of `accounts` (the bus's account's public keys) signed,
    /// that had not expired when it was stored and whose key `revoked` (looked up in the revocation records) does not
    /// say is revoked: the credentials of a machine serving the ant. It must be stored at the sequence it was signed
    /// for, so that a record replaced since is not put back, in `incarnation`, the bucket's ([`incarnation`]; none
    /// without its marker), so that no record of an incarnation before is. Returns the signing user.
    pub fn verify<'a>(
        &self,
        accounts: impl IntoIterator<Item = &'a str>,
        revoked: impl FnOnce(&str) -> bool,
        stored_at: DateTime<Utc>,
        stored: u64,
        incarnation: Option<&str>,
    ) -> Result<VerifiedUser, SettingsError> {
        let (Some(jwt), Some(signature)) = (&self.credentials, &self.signature) else {
            return Err(SettingsError::Unsigned);
        };
        let at = u64::try_from(stored_at.timestamp()).unwrap_or_default();
        let user = verify_user_jwt(jwt, accounts, at)?;
        if user.id != self.ant {
            return Err(SettingsError::NotTheAnt { named: user.id, ant: self.ant.clone() });
        }
        user.check_signature(&self.signed(), signature)?;
        if revoked(&user.public_key) {
            return Err(SettingsError::Revoked(user.public_key));
        }
        let Some(incarnation) = incarnation else { return Err(SettingsError::NoIncarnation) };
        if self.incarnation != incarnation {
            let (signed, current) = (self.incarnation.clone(), incarnation.to_owned());
            return Err(SettingsError::OtherIncarnation { signed, current });
        }
        if self.sequence != stored {
            return Err(SettingsError::PutBack { signed: self.sequence, stored });
        }
        Ok(user)
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

    const INCARNATION: &str = "1@2026-11-16T00:00:00.000000000Z";

    #[test]
    fn drift_names_the_fields_the_machine_sets_otherwise() {
        let shared = SharedSettings {
            ant: "dev-a".parse().unwrap(),
            fields: json!({ "model": "gpt-5", "capabilities": ["rust"] }).as_object().unwrap().clone(),
            machine: "m1".parse().unwrap(),
            by: None,
            updated_at: Utc::now(),
            sequence: 1,
            incarnation: INCARNATION.to_owned(),
            credentials: None,
            signature: None,
        };
        assert_eq!(shared.key(), "dev-a.settings");
        let local = json!({ "model": "gpt-4", "capabilities": ["rust"], "whoami": "a dev" });
        assert_eq!(shared.drift(local.as_object().unwrap()), ["model", "whoami"]);
        assert!(shared.drift(&Map::new()).is_empty(), "a machine setting nothing drifts in nothing");
        assert!(shared.check_key("dev-a.settings").is_ok());
        assert!(shared.check_key("dev-b.settings").is_err());
        assert!(is_shared_field("escalate_to") && !is_shared_field("tasks") && !is_shared_field("workdir"));
    }

    #[test]
    fn only_a_record_signed_by_the_ant_s_credentials_verifies() {
        use data_encoding::BASE64URL_NOPAD;
        use nkeys::KeyPair;

        let account = KeyPair::new_account();
        let (dev_a, dev_b) = (KeyPair::new_user(), KeyPair::new_user());
        let mut record = SharedSettings {
            ant: "dev-a".parse().unwrap(),
            fields: json!({ "model": "gpt-5" }).as_object().unwrap().clone(),
            machine: "m1".parse().unwrap(),
            by: Some("alice".parse().unwrap()),
            updated_at: Utc::now(),
            sequence: 7,
            incarnation: INCARNATION.to_owned(),
            credentials: None,
            signature: None,
        };
        let accounts = [account.public_key()];
        let accounts = || accounts.iter().map(String::as_str);
        let now = Utc::now();
        assert_eq!(record.verify(accounts(), |_| false, now, 7, Some(INCARNATION)), Err(SettingsError::Unsigned));
        let sign = |record: &mut SharedSettings, jwt: String, key: &KeyPair| {
            record.credentials = Some(jwt);
            record.signature = Some(BASE64URL_NOPAD.encode(&key.sign(&record.signed()).unwrap()));
        };
        sign(&mut record, crate::test_support::user_jwt(&account, &dev_a, "dev-a", &[], None), &dev_a);
        assert_eq!(record.verify(accounts(), |_| false, now, 7, Some(INCARNATION)).unwrap().id, record.ant);

        let mut changed = record.clone();
        changed.fields.insert("allow".to_owned(), json!(["execute"]));
        assert!(matches!(
            changed.verify(accounts(), |_| false, now, 7, Some(INCARNATION)),
            Err(SettingsError::Credentials(JwtError::BadSignature(_)))
        ));

        let mut other = record.clone();
        sign(&mut other, crate::test_support::user_jwt(&account, &dev_b, "dev-b", &[], None), &dev_b);
        assert!(
            matches!(
                other.verify(accounts(), |_| false, now, 7, Some(INCARNATION)),
                Err(SettingsError::NotTheAnt { .. })
            ),
            "another ant's"
        );

        let foreign = KeyPair::new_account();
        let mut forged = record.clone();
        sign(&mut forged, crate::test_support::user_jwt(&foreign, &dev_a, "dev-a", &[], None), &dev_a);
        assert!(matches!(
            forged.verify(accounts(), |_| false, now, 7, Some(INCARNATION)),
            Err(SettingsError::Credentials(JwtError::ForeignAccount(_)))
        ));

        let mut expired = record;
        expired.updated_at = DateTime::from_timestamp(999, 0).unwrap();
        sign(&mut expired, crate::test_support::user_jwt(&account, &dev_a, "dev-a", &[], Some(1_000)), &dev_a);
        assert!(expired.verify(accounts(), |_| false, now, 7, Some(INCARNATION)).is_err());
        let then = DateTime::from_timestamp(999, 0).unwrap();
        assert!(
            expired.verify(accounts(), |_| false, then, 7, Some(INCARNATION)).is_ok(),
            "expiry counts when the record was stored"
        );
    }

    #[test]
    fn a_record_put_back_at_another_sequence_or_signed_with_a_revoked_key_does_not_verify() {
        use data_encoding::BASE64URL_NOPAD;
        use nkeys::KeyPair;

        let (account, dev_a) = (KeyPair::new_account(), KeyPair::new_user());
        let mut record = SharedSettings {
            sequence: 41,
            incarnation: INCARNATION.to_owned(),
            credentials: Some(crate::test_support::user_jwt(&account, &dev_a, "dev-a", &[], None)),
            ..SharedSettings::new("dev-a".parse().unwrap(), Map::new(), "m1".parse().unwrap(), None)
        };
        record.signature = Some(BASE64URL_NOPAD.encode(&dev_a.sign(&record.signed()).unwrap()));
        assert_eq!(record.signing_key(), Some(dev_a.public_key()));
        let accounts = [account.public_key()];
        let accounts = || accounts.iter().map(String::as_str);
        let verify =
            |revoked: bool, stored| record.verify(accounts(), |_| revoked, Utc::now(), stored, Some(INCARNATION));

        assert!(verify(false, 41).is_ok(), "stored where signed for");
        let replayed = verify(false, 57);
        assert_eq!(replayed, Err(SettingsError::PutBack { signed: 41, stored: 57 }), "put back, however soon");
        assert!(replayed.unwrap_err().is_replay());
        let mut moved = record.clone();
        moved.sequence = 57;
        let moved = moved.verify(accounts(), |_| false, Utc::now(), 57, Some(INCARNATION));
        assert!(matches!(moved, Err(SettingsError::Credentials(JwtError::BadSignature(_)))), "the sequence is signed");
        let revoked = verify(true, 41);
        assert_eq!(revoked, Err(SettingsError::Revoked(dev_a.public_key())));
        assert!(!revoked.unwrap_err().is_replay());
    }

    #[test]
    fn a_record_of_another_incarnation_of_the_bucket_or_of_one_without_a_marker_does_not_verify() {
        use data_encoding::BASE64URL_NOPAD;
        use nkeys::KeyPair;

        let (account, dev_a) = (KeyPair::new_account(), KeyPair::new_user());
        let restored = DateTime::from_timestamp(1_000, 5).unwrap();
        let mut record = SharedSettings {
            sequence: 41,
            incarnation: incarnation(3, restored),
            credentials: Some(crate::test_support::user_jwt(&account, &dev_a, "dev-a", &[], None)),
            ..SharedSettings::new("dev-a".parse().unwrap(), Map::new(), "m1".parse().unwrap(), None)
        };
        record.signature = Some(BASE64URL_NOPAD.encode(&dev_a.sign(&record.signed()).unwrap()));
        let accounts = [account.public_key()];
        let verify = |incarnation: Option<&str>| {
            record.verify(accounts.iter().map(String::as_str), |_| false, Utc::now(), 41, incarnation)
        };

        assert_eq!(record.incarnation, "3@1970-01-01T00:16:40.000000005Z");
        assert!(verify(Some(&incarnation(3, restored))).is_ok());
        let marked_anew = incarnation(3, restored + chrono::Duration::nanoseconds(1));
        let other = verify(Some(&marked_anew));
        assert!(matches!(&other, Err(SettingsError::OtherIncarnation { .. })), "{other:?}");
        assert!(other.unwrap_err().is_replay(), "the settings kept stand for it");
        assert!(matches!(verify(Some(&incarnation(4, restored))), Err(SettingsError::OtherIncarnation { .. })));
        assert_eq!(verify(None), Err(SettingsError::NoIncarnation));
        let mut moved = record.clone();
        moved.incarnation = marked_anew.clone();
        let moved = moved.verify(accounts.iter().map(String::as_str), |_| false, Utc::now(), 41, Some(&marked_anew));
        assert!(
            matches!(moved, Err(SettingsError::Credentials(JwtError::BadSignature(_)))),
            "the incarnation is signed"
        );
    }
}
