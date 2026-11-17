//! An event of a sense identity's log, and the message storing it.

use ants_proto::{Machine, SenseId, UserId, canonical_json, verified_jwt};
use chrono::{DateTime, SecondsFormat, Utc};
use data_encoding::HEXLOWER;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// The first line of what an event's signature signs ([`Event::signed`]), so that no signature over other bytes of the
/// ant's key passes for an event's.
pub const EVENT_SIGNING_CONTEXT: &str = "ants-sense-event-v1";
/// The kind of the event starting the events of an ant's new name in its log, its data `{"from": <the name before>}`:
/// written by the daemon serving the ant under its new name before its first event ([`super::AntLog::try_append`]).
pub const RENAMED: &str = "ant_renamed";

/// An event of the log of the sense identity `sense`, as stored, JSON-encoded, in its message. Its position in the log
/// (`index`) and the hash of the message before it (`prev`) chain it to the events before; neither the stream's
/// sequence nor its incarnation is part of it, so a log copied whole elsewhere verifies as well.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub sense: SenseId,
    /// The event's position in the log, from 1, one more than the event before.
    pub index: u64,
    /// The SHA-256, lowercase hex, of the message storing the event before ([`Stored::sha256`]); none for the first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,
    pub at: DateTime<Utc>,
    /// The ant whose sense data the event is, whose credentials sign it.
    pub ant: UserId,
    /// The machine whose daemon wrote it.
    pub machine: Machine,
    /// What the event is, such as an item's change, which tells how to read `data`.
    pub kind: String,
    #[serde(default)]
    pub data: Value,
    /// The user JWT of the ant's credentials whose key signed the event; none on a server without authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<String>,
    /// The signature of [`Event::signed`] by the key of `credentials`, base64url without padding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl Event {
    /// What the ant's key signs: lines joined by `\n`, [`EVENT_SIGNING_CONTEXT`], the sense id, the index in decimal,
    /// `prev` or an empty line, `at` in RFC 3339 with nanoseconds and `Z`, the ant, the machine, the kind and the data
    /// as canonical JSON ([`canonical_json`]). Every field but the JWT, which the signing key is checked against, and
    /// the signature.
    pub fn signed(&self) -> Vec<u8> {
        let index = self.index.to_string();
        let at = self.at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let data = canonical_json(&self.data);
        let prev = self.prev.as_deref().unwrap_or_default();
        [
            EVENT_SIGNING_CONTEXT,
            self.sense.as_str(),
            &index,
            prev,
            &at,
            self.ant.as_str(),
            self.machine.as_str(),
            &self.kind,
            &data,
        ]
        .join("\n")
        .into_bytes()
    }

    /// The public key (`U...`) of the credentials that signed, as their JWT says before anything checks it.
    pub fn signing_key(&self) -> Option<String> {
        verified_jwt(self.credentials.as_deref()?).ok().map(|(claims, _)| claims.sub)
    }
}

/// A message of a sense identity's log: the stream sequence it is stored at, when the stream stored it, and its
/// payload, an [`Event`] if it is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub sequence: u64,
    pub stored_at: DateTime<Utc>,
    pub payload: Vec<u8>,
}

impl Stored {
    /// The SHA-256 of the payload, lowercase hex: what the next event's `prev` holds.
    pub fn sha256(&self) -> String {
        sha256(&self.payload)
    }

    pub fn event(&self) -> Result<Event, serde_json::Error> {
        serde_json::from_slice(&self.payload)
    }
}

/// The SHA-256 of `bytes`, lowercase hex.
pub fn sha256(bytes: &[u8]) -> String {
    HEXLOWER.encode(&Sha256::digest(bytes))
}
