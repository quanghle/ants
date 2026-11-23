//! Trail tips: what an ant answering a step of a saved trail learnt, left for the ants answering the trail's later
//! steps ([`TrailTip`]), and the votes of the ants that used one ([`TipVote`]), kept in [`TRAIL_TIPS_BUCKET`]. A
//! [`TipBoard`] ranks a trail's tips by their net votes, halved every [`TIP_HALF_LIFE`] since last used, and retires
//! those a human retired, voted wrong more than helpful, or unused for [`TIP_UNUSED`].

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::{
    Error, MAX_TITLE, RunId, TipId, TrailName, UserId, trail_tip_key, trail_tip_retired_key, trail_tip_vote_key,
};
#[cfg(doc)]
use crate::{TRAIL_TIPS_BUCKET, TrailStep};

mod board;

pub use board::{Ranked, Retirement, Similarity, Tally, TipBoard, TipStatus, search_words, tip_words};

/// Most characters a tip's text has.
pub const MAX_TIP_TEXT: usize = 1000;
/// Most characters a vote's reason has.
pub const MAX_TIP_REASON: usize = 300;
/// Most tips a step's message carries.
pub const MAX_TIPS_CARRIED: usize = 3;
/// Most bytes the tips a step's message carries add to it.
pub const MAX_TIPS_BLOCK: usize = 2048;
/// Tips a search finds unless asked for another number.
pub const DEFAULT_TIPS_FOUND: usize = 10;
/// Most tips a search finds.
pub const MAX_TIPS_FOUND: usize = 50;
/// Most live tips a trail has: no tip is left on a trail having as many.
pub const MAX_LIVE_TIPS: usize = 200;
/// The time over which a tip's score halves, from its last use.
pub const TIP_HALF_LIFE: Duration = Duration::from_hours(30 * 24);
/// How long a tip stays live unused: neither written nor voted helpful for as long, it is retired.
pub const TIP_UNUSED: Duration = Duration::from_hours(90 * 24);
/// The share of a new tip's words ([`tip_words`]) a live tip of the same tip key has in common with it to be similar.
pub const TIP_SIMILARITY: f64 = 0.6;
/// The share of a trail-wide tip's words ([`tip_words`]) a step's prompt has, sharing at least
/// [`TIP_RELEVANT_WORDS`] of them, for the step to carry the tip.
pub const TIP_RELEVANCE: f64 = 0.3;
/// The fewest of a trail-wide tip's words ([`tip_words`]) a step's prompt has for the step to carry the tip.
pub const TIP_RELEVANT_WORDS: usize = 2;

/// A tip in [`TRAIL_TIPS_BUCKET`], under [`TrailTip::key`]: what an ant answering a step of a saved trail learnt, for
/// the ants answering the trail's later steps. Its writer creates it once; a human retires it under another key
/// ([`TrailTip::retired_key`]). Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailTip {
    pub trail: TrailName,
    pub id: TipId,
    /// The ant that left it.
    pub writer: UserId,
    pub text: String,
    /// The tip key of the step it was left on ([`TrailStep::tip_key`]), whose steps carry it first; none for a tip
    /// on the whole trail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tip_key: Option<String>,
    /// The run of the step it was left on.
    pub run: RunId,
    /// The [`script_sha256`] of the script of the step it was left on.
    pub script: String,
    pub written_at: DateTime<Utc>,
    /// Who retired it, as a [`TipBoard`] reads it from [`TrailTip::retired_key`]: what a record says here is
    /// never taken, as its writer puts the record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retired: Option<TipRetired>,
}

/// A human's retiring of a tip, under the tip's [`TrailTip::retired_key`]. Readers should treat unknown fields as
/// additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TipRetired {
    pub by: UserId,
    pub at: DateTime<Utc>,
}

impl TrailTip {
    /// Its key in [`TRAIL_TIPS_BUCKET`] ([`trail_tip_key`]).
    pub fn key(&self) -> String {
        trail_tip_key(&self.trail, &self.writer, &self.id)
    }

    /// The key of its retirement by a human ([`trail_tip_retired_key`]), which no ant writes.
    pub fn retired_key(&self) -> String {
        trail_tip_retired_key(&self.trail, &self.writer, &self.id)
    }

    /// The SHA-256 of its text, in lowercase hex: the text a vote on it judged ([`TipVote::text`]).
    pub fn text_sha256(&self) -> String {
        script_sha256(&self.text)
    }

    /// The key of `voter`'s vote on it ([`trail_tip_vote_key`]).
    pub fn vote_key(&self, voter: &UserId) -> String {
        trail_tip_vote_key(&self.trail, &self.writer, &self.id, voter)
    }

    /// Check what a reader takes from the bucket: the tip is stored under its own key and within the bounds of
    /// [`TrailTip::check`].
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key != self.key() {
            return Err(Error::ForeignTip { kind: "tip", key: self.key(), at: key.to_owned() });
        }
        self.check()
    }

    /// Check the tip's bounds: a text of 1 to [`MAX_TIP_TEXT`] characters, not blank, and a tip key passing
    /// [`check_tip_key`].
    pub fn check(&self) -> Result<(), Error> {
        if self.text.trim().is_empty() || self.text.chars().count() > MAX_TIP_TEXT {
            return Err(Error::InvalidTip(format!("a tip's text has 1-{MAX_TIP_TEXT} characters")));
        }
        self.tip_key.as_deref().map_or(Ok(()), check_tip_key)
    }
}

/// Check a tip key, an `agent()` call's `tips=` ([`TrailStep::tip_key`]): one line of 1 to [`MAX_TITLE`] characters,
/// not blank.
pub fn check_tip_key(key: &str) -> Result<(), Error> {
    if key.trim().is_empty() || key.contains(['\n', '\r']) || key.chars().count() > MAX_TITLE {
        return Err(Error::InvalidTip(format!("a tip key is one line of 1-{MAX_TITLE} characters")));
    }
    Ok(())
}

/// What an ant that used a tip says of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TipVerdict {
    Helpful,
    /// Right once, no longer.
    Stale,
    Wrong,
}

impl TipVerdict {
    pub const ALL: [Self; 3] = [Self::Helpful, Self::Stale, Self::Wrong];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Helpful => "helpful",
            Self::Stale => "stale",
            Self::Wrong => "wrong",
        }
    }
}

impl fmt::Display for TipVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TipVerdict {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::ALL
            .into_iter()
            .find(|verdict| verdict.as_str() == s)
            .ok_or_else(|| Error::InvalidTipVote(format!("no verdict {s:?}: use helpful, stale or wrong")))
    }
}

/// An ant's vote on a tip in [`TRAIL_TIPS_BUCKET`], under the tip's [`TrailTip::vote_key`] for the voter: voting
/// again replaces it. It counts only for the text it judged ([`TipVote::text`]). Readers should treat unknown fields
/// as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TipVote {
    pub voter: UserId,
    pub verdict: TipVerdict,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The [`TrailTip::text_sha256`] of the tip's text it judged: a tip its writer puts again with other text keeps
    /// none of the votes cast on the earlier one.
    pub text: String,
    /// The [`script_sha256`] of the script of the step the voter answered, when it voted answering a step of the
    /// tip's trail: a helpful vote with the current script's shows the tip still holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    pub at: DateTime<Utc>,
}

impl TipVote {
    /// Check what a reader takes from the bucket: the vote is stored under the key of its voter's vote on `tip`
    /// ([`TrailTip::vote_key`]), judged `tip`'s text ([`TipVote::text`]) and is within the bounds of
    /// [`TipVote::check`].
    pub fn check_key(&self, tip: &TrailTip, key: &str) -> Result<(), Error> {
        let own = tip.vote_key(&self.voter);
        if key != own {
            return Err(Error::ForeignTip { kind: "vote", key: own, at: key.to_owned() });
        }
        if self.text != tip.text_sha256() {
            return Err(Error::InvalidTipVote(format!("the vote at {key} judged another text than the tip's")));
        }
        self.check()
    }

    /// Check the vote's bounds: a reason of at most [`MAX_TIP_REASON`] characters.
    pub fn check(&self) -> Result<(), Error> {
        if self.reason.as_ref().is_some_and(|reason| reason.chars().count() > MAX_TIP_REASON) {
            return Err(Error::InvalidTipVote(format!("a reason has at most {MAX_TIP_REASON} characters")));
        }
        Ok(())
    }
}

/// The SHA-256 of a trail's script, in lowercase hex: which script a step ran ([`TrailStep::script`]), a tip was left
/// on ([`TrailTip::script`]) and a vote was cast on ([`TipVote::script`]).
pub fn script_sha256(script: &str) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(script.as_bytes()))
}
