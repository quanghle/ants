//! What makes an ant the same ant on every machine: its [`AntIdentity`] record in [`crate::IDENTITIES_BUCKET`], and the
//! [`Avatar`] its seed derives, a pixel-art head that grows with the ant's [`Stage`] and a one-line ASCII face, which
//! the CLI draws.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::IDENTITIES_BUCKET;
use crate::{Error, SenseId, UserId};

mod avatar;
mod motion;
mod palette;

pub use avatar::{Avatar, Cell, FACE_WIDTH, Grid, HEAD_COLUMNS, HEAD_ROWS, HUE_NAMES, Pixel, hue_named};
pub use motion::{Frame, Mood, Motion, Side};
pub use palette::{Palette, Rgb, Theme};

/// An ant's record in [`IDENTITIES_BUCKET`], under the key of its name: what makes it the same ant on every machine.
/// Its daemon creates it on start only when absent, so across machines the first creator wins and later ones keep it;
/// the daemon serving the ant then adds each completed prompt by compare-and-set, keeping the other fields. Humans set
/// and clear `avatar` and `color` (`ants ant avatar`), and `moved` (`ants ant export --move`, `ants ant unseal`), by
/// compare-and-set too. A daemon serving the ant with built-in sense names its sense identity in `sense` when the record
/// names none, by compare-and-set, so the first writer wins and every machine opens the same sense identity.
/// `ants ant import` creates it on another bus from an export, by a KV create refused when a record holds its name or
/// its id, so no bus holds two copies of one ant. `ants auth revoke NAME` of the ant deletes it, so a later ant of that
/// name gets a new record, which names the sense identity of the home again. Readers should treat unknown fields as
/// additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntIdentity {
    pub ant: UserId,
    /// 128 random bits in lowercase hex, set at birth; the avatar's seed unless `avatar` or `sense` sets one.
    pub id: String,
    /// When the record was created.
    pub born: DateTime<Utc>,
    /// The avatar's seed a human set, in place of `sense` and `id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    /// The avatar's colour a human set, one of [`HUE_NAMES`], in place of the seed's hue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    /// The prompts the ant completed since its birth.
    #[serde(default)]
    pub prompts: u64,
    /// Set when the ant was moved to another bus, by whom and when; no daemon serves it while set: the daemon serving it
    /// drains it as on a stop and leaves the colony, and a daemon starting it waits for the mark to go.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub moved: Option<MovedMark>,
    /// The ant this one was cloned from, when it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloned_from: Option<ClonedFrom>,
    /// The ant's sense identity, once a daemon serving it with built-in sense named it: the avatar's seed unless
    /// `avatar` sets one, so the ant keeps its face across records of that identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sense: Option<SenseId>,
}

/// When and by whom an ant was moved off its bus ([`AntIdentity::moved`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MovedMark {
    pub at: DateTime<Utc>,
    pub by: UserId,
}

/// The ant a clone was made from ([`AntIdentity::cloned_from`]): its name and id then, and when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClonedFrom {
    pub ant: UserId,
    pub id: String,
    pub at: DateTime<Utc>,
}

impl AntIdentity {
    /// A record for `ant` born at `born` with `id`, before any prompt.
    pub fn new(ant: UserId, id: String, born: DateTime<Utc>) -> Self {
        Self { ant, id, born, avatar: None, color: None, prompts: 0, moved: None, cloned_from: None, sense: None }
    }

    pub fn key(&self) -> &str {
        self.ant.as_str()
    }

    /// Check that the record was read from its own key: credentials pin each ant to its own key, while the body is
    /// whatever the writer put there.
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key == self.key() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "identity record", user: self.ant.clone(), at: format!("key {key}") })
    }

    /// The avatar's seed: the one a human set, else the sense id, else the record's id.
    pub fn seed(&self) -> &str {
        self.avatar.as_deref().or(self.sense.as_ref().map(SenseId::as_str)).unwrap_or(&self.id)
    }

    /// Where [`Self::seed`] comes from.
    pub fn seed_source(&self) -> SeedSource {
        if self.avatar.is_some() {
            SeedSource::Set
        } else if self.sense.is_some() {
            SeedSource::Sense
        } else {
            SeedSource::Id
        }
    }

    pub fn stage(&self) -> Stage {
        Stage::of(self.prompts)
    }
}

/// Where an ant's avatar seed comes from ([`AntIdentity::seed`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedSource {
    /// The seed a human set (`avatar`).
    Set,
    /// The sense identity (`sense`).
    Sense,
    /// The record's id (`id`).
    Id,
}

impl SeedSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Set => "avatar",
            Self::Sense => "sense",
            Self::Id => "id",
        }
    }
}

/// FNV-1a 64 of `bytes`: stable across builds and platforms, as std's hashers are not.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3))
}

/// An ant's growth, by the prompts it completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stage {
    /// Below 10 prompts: pale, as a newly emerged ant is.
    Callow,
    /// From 10: full colour.
    Worker,
    /// From 100: a scar across the right cheek.
    Veteran,
    /// From 1000: a crown between the antennae.
    Elder,
}

impl Stage {
    pub fn of(prompts: u64) -> Self {
        match prompts {
            0..10 => Self::Callow,
            10..100 => Self::Worker,
            100..1000 => Self::Veteran,
            _ => Self::Elder,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Callow => "callow",
            Self::Worker => "worker",
            Self::Veteran => "veteran",
            Self::Elder => "elder",
        }
    }

    /// Prompts to reach the next stage; None for the last.
    pub fn next_at(self) -> Option<u64> {
        match self {
            Self::Callow => Some(10),
            Self::Worker => Some(100),
            Self::Veteran => Some(1000),
            Self::Elder => None,
        }
    }
}

#[cfg(test)]
mod tests;
