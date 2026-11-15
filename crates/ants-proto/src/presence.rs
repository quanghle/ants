use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::PRESENCE_BUCKET;
use crate::UserId;

/// The id of one device of a human: each `ants auth user NAME --human` issues credentials for a new device, whose user
/// JWT carries its id as the tag [`DeviceId::tag`]. Generated (a UUIDv7), never chosen and never reused; a label
/// for display is kept beside it where the credentials were issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeviceId(Uuid);

impl DeviceId {
    /// Prefix of the user JWT tag carrying the device id.
    pub const TAG: &str = "ants-device:";

    pub fn generate() -> Self {
        Self(Uuid::now_v7())
    }

    /// The user JWT tag carrying the id.
    pub fn tag(&self) -> String {
        format!("{}{}", Self::TAG, self.0)
    }

    /// The device id among a user JWT's `tags`, if any.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        tags.into_iter().find_map(|tag| tag.strip_prefix(Self::TAG)?.parse().ok())
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DeviceId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

/// The public X25519 encryption key of one device of a human: the public key of an nkeys xkey (`X...`), generated on
/// the device beside its NKey and carried in its user JWT as the tag [`DeviceKey::tag`], so the account's signature
/// binds it to that human and device. Messages sealed for the device are opened with its seed, which never leaves the
/// device. Only the form is checked here; the key's checksum is checked where it is used.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DeviceKey(String);

impl DeviceKey {
    /// Prefix of the user JWT tag carrying the key.
    pub const TAG: &str = "ants-xkey:";

    /// The user JWT tag carrying the key.
    pub fn tag(&self) -> String {
        format!("{}{}", Self::TAG, self.0)
    }

    /// The key among a user JWT's `tags`, if any, in either case: tools that lowercase a JWT's tags lose nothing of
    /// a base32 key.
    pub fn from_tags<'a>(tags: impl IntoIterator<Item = &'a str>) -> Option<Self> {
        tags.into_iter().find_map(|tag| tag.strip_prefix(Self::TAG)?.to_ascii_uppercase().parse().ok())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeviceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for DeviceKey {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let base32 = |c: char| c.is_ascii_uppercase() || ('2'..='7').contains(&c);
        if s.len() == 56 && s.starts_with('X') && s.chars().all(base32) {
            Ok(Self(s.to_owned()))
        } else {
            Err(crate::Error::InvalidDeviceKey(s.to_owned()))
        }
    }
}

impl TryFrom<String> for DeviceKey {
    type Error = crate::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<DeviceKey> for String {
    fn from(key: DeviceKey) -> Self {
        key.0
    }
}

/// What a device of a human follows as it comes, so it needs no push of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Following {
    /// Every message to the human (`ants inbox --follow`), escalations among them.
    Inbox,
    /// The tool requests of every ant (`ants tools list --follow`, or `ants inbox --follow` as text on every thread).
    Approvals,
}

impl Following {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Approvals => "approvals",
        }
    }
}

/// Key in [`PRESENCE_BUCKET`] of `device` of `human` following `what`. Keys start with the human, so its credentials
/// can be confined to its own.
pub fn presence_key(human: &UserId, device: &DeviceId, what: Following) -> String {
    format!("{human}.{device}.{}", what.as_str())
}

/// That a device of a human follows something ([`Following`]) right now, kept in [`PRESENCE_BUCKET`] under
/// [`presence_key`] while it does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presence {
    pub human: UserId,
    pub device: DeviceId,
    pub following: Following,
    /// When the device started following.
    pub since: DateTime<Utc>,
}

impl Presence {
    pub fn new(human: UserId, device: DeviceId, following: Following) -> Self {
        Self { human, device, following, since: Utc::now() }
    }

    pub fn key(&self) -> String {
        presence_key(&self.human, &self.device, self.following)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_id_travels_as_a_user_jwt_tag() {
        let device = DeviceId::generate();
        assert_ne!(device, DeviceId::generate(), "never reused");
        let tag = device.tag();
        assert!(tag.starts_with("ants-device:"));
        assert_eq!(DeviceId::from_tags(["team", tag.as_str()]), Some(device));
        assert_eq!(DeviceId::from_tags(["ants-device:not-a-uuid"]), None);
        assert_eq!(device.to_string().parse::<DeviceId>().unwrap(), device);
    }

    #[test]
    fn a_device_key_travels_as_a_user_jwt_tag_and_only_in_its_form() {
        let public = "XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB";
        let key: DeviceKey = public.parse().unwrap();
        assert_eq!(key.tag(), format!("ants-xkey:{public}"));
        let tags = ["ants-perms:9".to_owned(), key.tag()];
        assert_eq!(DeviceKey::from_tags(tags.iter().map(String::as_str)), Some(key.clone()));
        assert_eq!(DeviceKey::from_tags([key.tag().to_lowercase().as_str()]), Some(key.clone()));
        assert_eq!(DeviceKey::from_tags(["ants-perms:9"]), None);
        for wrong in
            ["", "UBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB", &public[..55], &public.to_lowercase()]
        {
            assert!(wrong.parse::<DeviceKey>().is_err(), "{wrong}");
        }
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(serde_json::from_str::<DeviceKey>(&json).unwrap(), key);
        assert!(serde_json::from_str::<DeviceKey>("\"X1\"").is_err());
    }
}
