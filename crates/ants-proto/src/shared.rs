use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[cfg(doc)]
use crate::IDENTITIES_BUCKET;
use crate::{Error, JwtError, Machine, UserId, VerifiedUser, canonical_json, verified_jwt, verify_user_jwt};

/// The first line of what an ant's key signs of its shared settings ([`SharedSettings::signed`]).
pub const SETTINGS_SIGNING_CONTEXT: &str = "ants-shared-settings";

/// How far apart the time an ant's shared settings were signed (`updated_at`) and the time the stream stored them may
/// be: past it, a reader takes the record for one put back after it was replaced ([`SharedSettings::verify`]).
pub const SETTINGS_CLOCK_SKEW: TimeDelta = TimeDelta::minutes(5);

/// The keys of an ant's `[[ant]]` table that are the ant's own rather than its machine's: its role and core memories
/// (`whoami`, `memory`), which [`SharedSettings`] holds for every machine serving it. The rest (workdir, env, harness,
/// `tasks` and the others) stays the machine's config. Of an ant added from a template, [`SharedSettings`] holds only
/// those a request to the template chooses: `fallback`, `allow` and `escalate_to` follow the template.
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
///
/// As any machine's host credentials may write any ant's record, its writer signs it with credentials of the ant
/// itself, which only the machines serving the ant hold ([`SharedSettings::credentials`]); a daemon of a team with
/// credentials applies only a record it verifies ([`SharedSettings::verify`]).
///
/// A record the ant once signed stays valid, so a host may put back one replaced since (a replay). The bucket keeps
/// one revision per key, so a reader cannot check a record against its predecessor; instead a record counts only if
/// the stream stored it within [`SETTINGS_CLOCK_SKEW`] of when it was signed, which refuses a record put back later
/// than that after its signing, whoever reads it. Within that window a reader that kept the record replacing it
/// refuses it too (the daemon's), but a reader that never saw it replaced (a daemon starting, the CLI) takes it. A
/// record signed with a key revoked since (a revocation record in [`crate::DEVICES_BUCKET`]) counts no more, whenever
/// it was stored.
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
    #[error(
        "signed at {signed} but stored at {stored}: put back after it was replaced, or signed on a clock off from the \
         hub's"
    )]
    Stale { signed: DateTime<Utc>, stored: DateTime<Utc> },
    #[error("put back after the record replacing it was read")]
    Replayed,
}

impl SettingsError {
    /// Whether the record is one the ant signed, put back after it was replaced: a newer one stands for it.
    pub fn is_replay(&self) -> bool {
        matches!(self, Self::Stale { .. } | Self::Replayed)
    }
}

impl SharedSettings {
    /// The record of `ant` holding `fields`, written now on `machine`, by `by` when an admin's order changed it,
    /// unsigned.
    pub fn new(ant: UserId, fields: Map<String, Value>, machine: Machine, by: Option<UserId>) -> Self {
        Self { ant, fields, machine, by, updated_at: Utc::now(), credentials: None, signature: None }
    }

    /// The key of `ant`'s shared settings: `<ant>.settings`, which no ant's credentials and no human's write.
    pub fn key_of(ant: &UserId) -> String {
        format!("{ant}.settings")
    }

    pub fn key(&self) -> String {
        Self::key_of(&self.ant)
    }

    /// What the ant's key signs: lines joined by `\n`, [`SETTINGS_SIGNING_CONTEXT`], the ant, the fields as canonical
    /// JSON ([`canonical_json`]), the machine, the admin or an empty line, and `updated_at` in RFC 3339 with
    /// nanoseconds and `Z`. Every field but the JWT, which the signing key is checked against, and the signature.
    pub fn signed(&self) -> Vec<u8> {
        let fields = canonical_json(&Value::Object(self.fields.clone()));
        let updated_at = self.updated_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let by = self.by.as_ref().map_or("", UserId::as_str);
        [SETTINGS_SIGNING_CONTEXT, self.ant.as_str(), &fields, self.machine.as_str(), by, &updated_at]
            .join("\n")
            .into_bytes()
    }

    /// The public key (`U...`) of the credentials that signed, as their JWT says before anything checks it: the key
    /// whose revocation record to look up for [`SharedSettings::verify`].
    pub fn signing_key(&self) -> Option<String> {
        verified_jwt(self.credentials.as_deref()?).ok().map(|(claims, _)| claims.sub)
    }

    /// Check that the record, which the stream stored at `stored_at`, is signed by the key of credentials of the ant
    /// that one of `accounts` (the team account's public keys) signed, that had not expired when it was stored and
    /// whose key `revoked` (looked up in the revocation records) does not say is revoked: the credentials of a machine
    /// serving the ant. It must have been stored within [`SETTINGS_CLOCK_SKEW`] of `updated_at`, so that a record
    /// signed long before is not put back. Returns the signing user.
    pub fn verify<'a>(
        &self,
        accounts: impl IntoIterator<Item = &'a str>,
        revoked: impl FnOnce(&str) -> bool,
        stored_at: DateTime<Utc>,
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
        if (stored_at - self.updated_at).abs() > SETTINGS_CLOCK_SKEW {
            return Err(SettingsError::Stale { signed: self.updated_at, stored: stored_at });
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

    #[test]
    fn drift_names_the_fields_the_machine_sets_otherwise() {
        let shared = SharedSettings {
            ant: "dev-a".parse().unwrap(),
            fields: json!({ "model": "gpt-5", "capabilities": ["rust"] }).as_object().unwrap().clone(),
            machine: "m1".parse().unwrap(),
            by: None,
            updated_at: Utc::now(),
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
            credentials: None,
            signature: None,
        };
        let accounts = [account.public_key()];
        let accounts = || accounts.iter().map(String::as_str);
        let now = Utc::now();
        assert_eq!(record.verify(accounts(), |_| false, now), Err(SettingsError::Unsigned));
        let sign = |record: &mut SharedSettings, jwt: String, key: &KeyPair| {
            record.credentials = Some(jwt);
            record.signature = Some(BASE64URL_NOPAD.encode(&key.sign(&record.signed()).unwrap()));
        };
        sign(&mut record, crate::roles::tests::user_jwt(&account, &dev_a, "dev-a", &[], None), &dev_a);
        assert_eq!(record.verify(accounts(), |_| false, now).unwrap().id, record.ant);

        let mut changed = record.clone();
        changed.fields.insert("allow".to_owned(), json!(["execute"]));
        assert!(matches!(
            changed.verify(accounts(), |_| false, now),
            Err(SettingsError::Credentials(JwtError::BadSignature(_)))
        ));

        let mut other = record.clone();
        sign(&mut other, crate::roles::tests::user_jwt(&account, &dev_b, "dev-b", &[], None), &dev_b);
        assert!(
            matches!(other.verify(accounts(), |_| false, now), Err(SettingsError::NotTheAnt { .. })),
            "another ant's"
        );

        let foreign = KeyPair::new_account();
        let mut forged = record.clone();
        sign(&mut forged, crate::roles::tests::user_jwt(&foreign, &dev_a, "dev-a", &[], None), &dev_a);
        assert!(matches!(
            forged.verify(accounts(), |_| false, now),
            Err(SettingsError::Credentials(JwtError::ForeignAccount(_)))
        ));

        let mut expired = record;
        expired.updated_at = DateTime::from_timestamp(999, 0).unwrap();
        sign(&mut expired, crate::roles::tests::user_jwt(&account, &dev_a, "dev-a", &[], Some(1_000)), &dev_a);
        assert!(expired.verify(accounts(), |_| false, now).is_err());
        let then = DateTime::from_timestamp(999, 0).unwrap();
        assert!(expired.verify(accounts(), |_| false, then).is_ok(), "expiry counts when the record was stored");
    }

    #[test]
    fn a_record_put_back_long_after_its_signing_or_signed_with_a_revoked_key_does_not_verify() {
        use data_encoding::BASE64URL_NOPAD;
        use nkeys::KeyPair;

        let (account, dev_a) = (KeyPair::new_account(), KeyPair::new_user());
        let signed_at = Utc::now() - TimeDelta::days(30);
        let mut record = SharedSettings {
            updated_at: signed_at,
            credentials: Some(crate::roles::tests::user_jwt(&account, &dev_a, "dev-a", &[], None)),
            ..SharedSettings::new("dev-a".parse().unwrap(), Map::new(), "m1".parse().unwrap(), None)
        };
        record.signature = Some(BASE64URL_NOPAD.encode(&dev_a.sign(&record.signed()).unwrap()));
        assert_eq!(record.signing_key(), Some(dev_a.public_key()));
        let accounts = [account.public_key()];
        let verify =
            |revoked: bool, stored_at| record.verify(accounts.iter().map(String::as_str), |_| revoked, stored_at);

        assert!(verify(false, signed_at + TimeDelta::seconds(1)).is_ok(), "stored as signed");
        assert!(verify(false, signed_at - TimeDelta::minutes(4)).is_ok(), "the clocks' skew is allowed for");
        let replayed = verify(false, Utc::now());
        assert!(matches!(replayed, Err(SettingsError::Stale { .. })), "{replayed:?}");
        assert!(replayed.unwrap_err().is_replay());
        assert!(matches!(verify(false, signed_at - TimeDelta::minutes(6)), Err(SettingsError::Stale { .. })));
        let revoked = verify(true, signed_at + TimeDelta::seconds(1));
        assert_eq!(revoked, Err(SettingsError::Revoked(dev_a.public_key())));
        assert!(!revoked.unwrap_err().is_replay());
    }
}
