//! Sugar's event log on the bus: the stream keeping it ([`SUGAR_STREAM`]) and the id of a sugar identity
//! ([`SugarId`]), whose subject ([`subject::sugar`]) holds the identity's events.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::Permissions;
use crate::{Error, subject};

/// JetStream stream keeping the event log of each sugar identity, one subject per identity ([`subject::sugar`]). Only
/// host credentials reach it ([`Permissions::for_host`]): the daemon serving an ant creates it when missing, appends
/// each event expecting the last sequence of the identity's subject, and reads it by direct gets. No ant's, human's or
/// service's credentials read or write it, and its subjects sit outside `ants.>`, which every human hears.
pub const SUGAR_STREAM: &str = "ANTS_SUGAR";

/// The alphabet of a ULID: Crockford's base32.
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// The id of a sugar identity: a ULID (26 characters of Crockford's base32, uppercase, the first 0-7), minted by the
/// daemon when an ant first uses sugar and kept in the ant's home, `.sugar/id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SugarId(String);

impl SugarId {
    /// A new id: a ULID of the time now, with the random bits of a UUID v7.
    pub fn new() -> Self {
        let value = Uuid::now_v7().as_u128();
        Self((0..26).map(|i| char::from(CROCKFORD[((value >> (125 - 5 * i)) & 31) as usize])).collect())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SugarId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for SugarId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        let valid = s.len() == 26 && s.bytes().all(|b| CROCKFORD.contains(&b)) && s.as_bytes()[0] <= b'7';
        if valid { Ok(Self(s.to_owned())) } else { Err(Error::InvalidSugarId(s.to_owned())) }
    }
}

impl TryFrom<String> for SugarId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self, Error> {
        s.parse()
    }
}

impl From<SugarId> for String {
    fn from(id: SugarId) -> String {
        id.0
    }
}

impl fmt::Display for SugarId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The subjects [`SUGAR_STREAM`] captures.
pub const SUGAR_SUBJECTS: &str = subject::ALL_SUGAR;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_id_is_a_ulid_that_parses_back_and_sorts_by_time() {
        let first = SugarId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = SugarId::new();
        assert_eq!(first.as_str().parse::<SugarId>().unwrap(), first);
        assert!(first < second, "{first} {second}");
        assert_eq!(subject::parse_sugar(&subject::sugar(&first)), Some(first));
    }

    #[test]
    fn only_ulids_are_sugar_ids() {
        "01J9ZQ3C5V8W2X4Y6Z7A8B9C0D".parse::<SugarId>().unwrap();
        for bad in [
            "",
            "01j9zq3c5v8w2x4y6z7a8b9c0d",
            "81J9ZQ3C5V8W2X4Y6Z7A8B9C0D",
            "01J9ZQ3C5V8W2X4Y6Z7A8B9C0",
            "01J9ZQ3C5V8W2X4Y6Z7A8B9C0I",
        ] {
            assert!(bad.parse::<SugarId>().is_err(), "{bad}");
        }
        assert!(serde_json::from_str::<SugarId>("\"x.y\"").is_err());
        assert_eq!(subject::parse_sugar("ants_sugar.nope"), None);
    }
}
