//! What a dream asks the ant to judge ([`Finding`]), and the ant's verdicts on them ([`Verdict`]).

use std::fmt::{self, Write as _};

use ants_proto::TrailName;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Advice, TRAIL};
use crate::items::ItemId;

/// What a finding is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    /// A live memory voted both helpful and stale, voted on since the last dream.
    Contradiction,
    NeedsReview,
    /// Near duplicates found by the last maintenance run.
    Duplicates,
    /// An episode written since the last dream, whose lessons are to be distilled.
    Distill,
    /// A pending persona or skill proposal ([`proposals`](super::gather::proposals)).
    Proposal,
    /// A finding of the ant's sage ([`Advice`]).
    Sage,
    /// No persona after some threads ([`identity_findings`](super::identity_findings)).
    NoPersona,
    /// No self-model after some threads.
    NoSelfModel,
    /// No relationship record of a human or ant who started some threads.
    NoRelationship,
    /// A self-model unchanged for many active days, in which the ant worked with someone or somewhere new.
    StaleSelfModel,
    /// A persona, self-model or skill still as a clone inherited it.
    Inherited,
    /// A vital sign of the work turned sharply for the worse ([`vitals`](super::vitals)).
    Vitals,
    /// A kind of the findings a sage gave other ants, mostly rejected ([`practice_findings`](super::practice_findings)).
    RejectedKind,
    /// Findings a sage gave other ants, applied and later undone.
    UndoneFindings,
    /// A sage's follow-ups often did not name an eligible earlier finding.
    UnlinkedFollowUps,
    /// An ant that left most of a sage's findings unanswered.
    UnansweredReviews,
    /// No relationship record of an ant a sage reviewed some times.
    NoReviewedRelationship,
    /// An ant whose reviews by a sage cost much, with none of their findings applied.
    CostlyReviews,
}

impl FindingKind {
    /// Whether it is about the ant's identity, gathered by [`identity_findings`](super::identity_findings).
    pub(super) fn identity(self) -> bool {
        matches!(
            self,
            Self::NoPersona | Self::NoSelfModel | Self::NoRelationship | Self::StaleSelfModel | Self::Inherited
        )
    }

    /// Whether it is about a sage's practice of reviewing, gathered by [`practice_findings`](super::practice_findings).
    pub(super) fn practice(self) -> bool {
        matches!(
            self,
            Self::RejectedKind
                | Self::UndoneFindings
                | Self::UnlinkedFollowUps
                | Self::UnansweredReviews
                | Self::NoReviewedRelationship
                | Self::CostlyReviews
        )
    }
}

impl fmt::Display for FindingKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Contradiction => "contradiction",
            Self::NeedsReview => "needs_review",
            Self::Duplicates => "duplicates",
            Self::Distill => "distill",
            Self::Proposal => "proposal",
            Self::Sage => "sage",
            Self::NoPersona => "no_persona",
            Self::NoSelfModel => "no_self_model",
            Self::NoRelationship => "no_relationship",
            Self::StaleSelfModel => "stale_self_model",
            Self::Inherited => "inherited",
            Self::Vitals => "vitals",
            Self::RejectedKind => "rejected_kind",
            Self::UndoneFindings => "undone_findings",
            Self::UnlinkedFollowUps => "unlinked_follow_ups",
            Self::UnansweredReviews => "unanswered_reviews",
            Self::NoReviewedRelationship => "no_reviewed_relationship",
            Self::CostlyReviews => "costly_reviews",
        })
    }
}

/// Something a dream asks the ant to judge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    /// `<dream>.<n>`: the dream that first offered it, and its place there.
    pub id: String,
    pub kind: FindingKind,
    pub text: String,
    /// The items it is about: memories, proposals, or the persona, self-model or a skill.
    #[serde(default)]
    pub items: Vec<ItemId>,
    /// Whether it rests on untrusted text: an untrusted memory or thread.
    #[serde(default)]
    pub untrusted: bool,
    /// Its kind and what it is about, `<kind>:<about>`, the same in every dream gathering it; none for a sage's. A
    /// rejected finding rests under it ([`Rested`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// The count it rests on, for a finding about a count rather than items alone: a rejected one rests until it
    /// differs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
    /// A sage's finding: its own kind, its evidence and the action it suggests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advice: Option<Advice>,
    /// Whether it is a sage's finding of an answer come after the ant was woken with the packet alone
    /// ([`Review::late`](super::Review::late)): the dreams offering it show it in a block of its own, as the late answer to the dream that
    /// gave it.
    #[serde(default, skip_serializing_if = "is_false")]
    pub late: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

impl Finding {
    /// The number of the dream that first offered it.
    pub fn dream(&self) -> u64 {
        self.id.split_once('.').and_then(|(number, _)| number.parse().ok()).unwrap_or_default()
    }

    /// What it is about, as its key tells, such as `self` or `human:alice`; none for a sage's.
    pub fn about(&self) -> Option<&str> {
        self.key.as_deref()?.split_once(':').map(|(_, about)| about)
    }

    /// Whether it is a sage's [`TRAIL`] finding, suggesting a saved trail.
    pub fn suggests_trail(&self) -> bool {
        self.advice.as_ref().is_some_and(|advice| advice.kind == TRAIL)
    }
}

/// How the ant judged a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Judgement {
    Applied,
    Rejected,
    Deferred,
}

impl fmt::Display for Judgement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Deferred => "deferred",
        })
    }
}

/// The ant's verdict on a finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    pub finding: String,
    pub verdict: Judgement,
    pub reason: String,
    /// With [`Judgement::Applied`]: the items written or changed.
    #[serde(default)]
    pub items: Vec<ItemId>,
    /// With [`Judgement::Applied`]: the trails saved on the dream thread for the finding ([`Dream::trails`](super::Dream::trails)).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trails: Vec<TrailName>,
    pub at: DateTime<Utc>,
}

impl Verdict {
    /// What it names, as ` (items a, b) (trails c)`: nothing when it names nothing.
    pub fn named(&self) -> String {
        let mut named = String::new();
        if !self.items.is_empty() {
            let items: Vec<_> = self.items.iter().map(ItemId::as_str).collect();
            let _ = write!(named, " (items {})", items.join(", "));
        }
        if !self.trails.is_empty() {
            let trails: Vec<_> = self.trails.iter().map(TrailName::as_str).collect();
            let _ = write!(named, " (trails {})", trails.join(", "));
        }
        named
    }
}

/// A trail saved on a dream thread ([`trail_saved`](super::count::trail_saved)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedTrail {
    pub name: TrailName,
    /// The finding the save named as the one it answers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finding: Option<String>,
    pub at: DateTime<Utc>,
}

/// A finding the ant rejected, or applied on a sage's practice ([`FindingKind::practice`]), kept with the record of
/// the last dream: no dream gathers it again until what it is about changed since the verdict (one of its items, other
/// than by a read, or the count it rests on) or [`REST_DAYS`](super::REST_DAYS) of the ant's active time passed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rested {
    /// Its [`Finding::key`].
    pub key: String,
    /// Its id when rejected.
    pub finding: String,
    /// When it was rejected, or applied.
    pub at: DateTime<Utc>,
    /// The count it rested on ([`Finding::count`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<u64>,
}
