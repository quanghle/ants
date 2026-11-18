//! Dreams (requirements ants-x39hk.12.3 to ants-x39hk.12.7): at most once a day, when the ant has had no prompt running
//! for a while and activity since its last review, the daemon serving it begins a dream ([`begin`]) and sends its
//! review packet ([`packet`]): to the ant's sage when one serves it ([`Recipient::Sage`]), as a message from the ant on
//! a review thread of its own ([`Review`]), else, as when the sage did not answer within [`SAGE_TIMEOUT_HOURS`] or
//! answered with nothing readable, to the ant itself on a new dream thread with the packet alone
//! ([`Dream::wake_due`]). The sage's findings prompt the ant on the review thread inside a marked block
//! ([`answered`]); an answer without findings wakes nobody, and the findings of one come after the ant was woken with
//! the packet alone are offered in the next dream ([`Review::late`]).
//!
//! sense runs no model: it only gathers what a review judges and records what the ant decides. Each dream is an item of
//! kind [`DREAM`], numbered, the last one named by a record of kind [`DREAMS`], so a daemon taking over dreams no sooner.
//! Its findings, each with an id the ant answers by (`<dream>.<n>`), are the sage's when it answered with some, with
//! those carried from earlier dreams, and otherwise the packet's own: contradicting votes, memories needing review,
//! near duplicates, episodes to distill and pending persona and skill proposals made
//! [`proposal::CONFIRM_AFTER_HOURS`] or more before ([`proposals`], [`crate::proposal`]), then what the ant's identity
//! lacks for the work it did, from sense data alone ([`identity_findings`]), then, for a sage, how the findings it gave
//! other ants fared, from the records of its reviews alone ([`practice_findings`]), then the vital signs of the work
//! turned sharply for the worse ([`vitals`]). The ant answers each with
//! `sense_dream` ([`dream`]) as applied (naming the items it wrote), rejected or deferred, with a reason, applied
//! confirming a proposal and rejected discarding it; the deferred and unanswered come again in the next dream while
//! they still hold. A rejected finding rests ([`Rested`]), as does an applied one on a sage's practice, which no
//! write of the sage changes: no dream gathers it again until what it is about changed
//! since the verdict or [`REST_DAYS`] of the ant's active time passed. A ripe proposal no dream offered makes a dream
//! due ([`proposal_due`]). Writes on the dream thread through the tools that change memories or records count
//! toward [`MAX_DREAM_WRITES`], and are untrusted while a finding resting on untrusted text is open ([`counted`],
//! [`untrusted_on`]); persona and skill proposals are refused on untrusted ground ([`untrusted`]). Finishing a dream,
//! or beginning the next one, writes its diary: an episode memory summarizing its verdicts. Each dream keeps the vital
//! signs of the work it reviews, measured by the daemon as it begins ([`Signs`]).
//!
//! A sage's own dream covers the reviews it was asked as threads too ([`Covered::review`]), but its packet lists them by
//! their records ([`reviewing()`]), leaving out their text, the packets of other ants' work: so they make nothing of
//! the dream untrusted, but for a finding on its practice quoting the reasons the ants reviewed gave for rejecting its
//! findings ([`practice_findings`]). Each record keeps what its review thread spent ([`ReviewSpend`]), which the sage's
//! tally shows a review and a finding still holding ([`ReviewCost`]).

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use ants_proto::provider::{CallContext, ToolCall};
use ants_proto::{ReviewOutcomes, ThreadId, UserId};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::Sense;
use crate::identity;
use crate::items::{ItemId, Revision, Values};
use crate::memory::{self, MAX_BODY_BYTES, MAX_TITLE_CHARS, MEMORY, Memory, MemoryItem, MemoryKind, Scope, Status};
use crate::proposal;
use crate::store::{Items, StoreError, Write, read_modify_write};
use crate::tools::{Answer, Handler};

mod identity_findings;
mod packet;
mod practice_findings;
mod review;
pub(crate) mod reviews;
mod tool;
mod vitals;

pub use identity_findings::{
    OWN_AFTER_THREADS, PERSONA_AFTER_THREADS, RELATIONSHIP_AFTER_THREADS, SELF_MODEL_AFTER_THREADS, STALE_ACTIVE_DAYS,
};
pub use packet::{
    Action, Earlier, Identity, MIN_THREAD_BYTES, OUTCOME_DAYS, PACKET_BUDGET, REVIEWING_SHARE, Reader, Section,
    TRACK_RECORD_DAYS, Thread, VERDICTS_SHARE, compose, packet, reviewing, track_record,
};
pub use practice_findings::{
    COSTLY_REVIEW_COST_MICROS, COSTLY_REVIEW_TOKENS, COSTLY_REVIEWS, JUDGED_FINDINGS, PRACTICE_DAYS, QUOTED_REASONS,
    REJECTED_FINDINGS, RELATIONSHIP_AFTER_REVIEWS, UNANSWERED_PERCENT, UNANSWERED_REVIEWS, UNDONE_FINDINGS,
};
pub use review::{
    Advice, Evidence, FOLLOW_UP, FollowUp, Parsed, REVIEW_BEGIN, REVIEW_END, Review, Reviewed, answered, parse,
    review_sent, untrusted_on,
};
pub use reviews::{
    AntTally, FindingTally, KindTally, REVIEW, ReviewCost, ReviewRecord, ReviewSpend, ReviewTally, ReviewedFinding,
    review_answered, review_id, review_received, review_record, review_records, review_tally, tally_lines,
};
pub use tool::{dream, schema};
pub use vitals::{
    CANCELLED_FINDING, CAPPED_FINDING, Ends, FAILED_MIN_CALLS, FAILED_SHARE_FACTOR, FAILED_SHARE_FLOOR,
    REJECTED_FINDING, Signs, TREND, Tally, before, changes, trend,
};

/// The kind of each dream's item ([`Dream`]).
pub const DREAM: &str = "dream";
/// The kind of the record naming the last dream.
pub const DREAMS: &str = "dreams";
/// The hours from one dream to the next, at least.
pub const DREAM_EVERY_HOURS: i64 = 24;
/// The minutes with no prompt running before a dream begins.
pub const IDLE_MINUTES: i64 = 30;
/// The hours the sage has to answer a review before the ant is woken with the packet alone.
pub const SAGE_TIMEOUT_HOURS: i64 = 6;
/// The most findings a dream gathers, and the most a sage's answer gives.
pub const MAX_FINDINGS: usize = 20;
/// The most writes through sense's tools a dream thread makes ([`counted`]).
pub const MAX_DREAM_WRITES: usize = 20;
/// The tools whose writes on a dream thread count toward [`MAX_DREAM_WRITES`]: those writing memories, and
/// `sense_identity` when it replaces a record ([`writes`]).
pub const COUNTED: &[&str] = &["remember", "vote", "identity"];
/// The days of the ant's active time a rejected finding rests unless what it is about changes ([`Rested`]).
pub const REST_DAYS: i64 = 14;

/// Who sense changes dream items for ([`crate::items::Stamp::by`]).
pub(crate) const BY: &str = "sense:dream";
/// The name of the record of the last dream ([`ItemId::named`]).
const RECORD: &str = "last";

/// Who reads a new dream's packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recipient {
    /// The ant's sage, sent the packet as a message from the ant; its findings are awaited for
    /// [`SAGE_TIMEOUT_HOURS`].
    Sage(UserId),
    /// The ant itself, woken with the packet alone.
    Ant,
}

/// Who reads the packet of a dream of an ant whose sage, if it names one, is `served`: the sage when served, else the
/// ant (requirement ants-x39hk.12.7). An ant with no sage, as where reviews are off, dreams on the packet alone.
pub fn recipient(served: Option<UserId>) -> Recipient {
    served.map_or(Recipient::Ant, Recipient::Sage)
}

/// Where a dream stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Its packet went to the sage, whose findings are awaited.
    Sent,
    /// The sage answered: its findings prompt the ant on the review thread; none wake nobody.
    Answered,
    /// The ant is woken with the packet alone: it has no sage served, or the sage left the review unanswered or
    /// answered with nothing readable.
    Fallback,
    /// Finished by the ant or closed by the next dream: its diary is written.
    Done,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sent => "sent",
            Self::Answered => "answered",
            Self::Fallback => "fallback",
            Self::Done => "done",
        })
    }
}

/// A thread a packet covers, whether its origin is untrusted (a webhook relaying the outside, or an ant asking a review)
/// and whether it is a review the ant was asked as another ant's sage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Covered {
    pub thread: ThreadId,
    #[serde(default)]
    pub untrusted: bool,
    /// A review asked of the ant: the packet lists it by its record ([`reviewing()`]), its text, another ant's work,
    /// left out.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub review: bool,
}

impl Covered {
    /// Whether the packet holds its text and that text is untrusted: a review's is left out.
    pub fn untrusted_text(&self) -> bool {
        self.untrusted && !self.review
    }
}

/// A commit a packet names, so its reader checks the code out as the threads left it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// The repository's normalized git remote ([`ants_proto::provider::normalize_remote`]).
    pub repo: String,
    pub commit: String,
    /// The thread whose worktree is at it; none for the ant's workdir.
    #[serde(default)]
    pub thread: Option<ThreadId>,
}

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
    /// A pending persona or skill proposal ([`proposals`]).
    Proposal,
    /// A finding of the ant's sage ([`Advice`]).
    Sage,
    /// No persona after some threads ([`identity_findings`]).
    NoPersona,
    /// No self-model after some threads.
    NoSelfModel,
    /// No relationship record of a human or ant who started some threads.
    NoRelationship,
    /// A self-model unchanged for many active days, in which the ant worked with someone or somewhere new.
    StaleSelfModel,
    /// A persona, self-model or skill still as a clone inherited it.
    Inherited,
    /// A vital sign of the work turned sharply for the worse ([`vitals`]).
    Vitals,
    /// A kind of the findings a sage gave other ants, mostly rejected ([`practice_findings`]).
    RejectedKind,
    /// Findings a sage gave other ants, applied and later undone.
    UndoneFindings,
    /// An ant that left most of a sage's findings unanswered.
    UnansweredReviews,
    /// No relationship record of an ant a sage reviewed some times.
    NoReviewedRelationship,
    /// An ant whose reviews by a sage cost much, with none of their findings applied.
    CostlyReviews,
}

impl FindingKind {
    /// Whether it is about the ant's identity, gathered by [`identity_findings`].
    fn identity(self) -> bool {
        matches!(
            self,
            Self::NoPersona | Self::NoSelfModel | Self::NoRelationship | Self::StaleSelfModel | Self::Inherited
        )
    }

    /// Whether it is about a sage's practice of reviewing, gathered by [`practice_findings`].
    fn practice(self) -> bool {
        matches!(
            self,
            Self::RejectedKind
                | Self::UndoneFindings
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
    pub at: DateTime<Utc>,
}

/// A finding the ant rejected, or applied on a sage's practice ([`FindingKind::practice`]), kept with the record of
/// the last dream: no dream gathers it again until what it is about changed since the verdict (one of its items, other
/// than by a read, or the count it rests on) or [`REST_DAYS`] of the ant's active time passed.
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

/// The record of the last dream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    number: u64,
    rested: Vec<Rested>,
}

/// A dream, as its item holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dream {
    pub number: u64,
    /// When it began.
    pub at: DateTime<Utc>,
    /// Where its review starts: the last dream's beginning, else a day before it.
    pub since: DateTime<Utc>,
    pub state: State,
    /// The sage its packet went to.
    pub sage: Option<UserId>,
    /// Its review by the sage, when the packet went to one.
    #[serde(default)]
    pub review: Option<Review>,
    /// The dream thread the ant was woken on: the review thread when the sage's findings prompted it.
    pub thread: Option<ThreadId>,
    #[serde(default)]
    pub threads: Vec<Covered>,
    #[serde(default)]
    pub commits: Vec<Commit>,
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default)]
    pub verdicts: Vec<Verdict>,
    /// The items written on the dream thread ([`counted`]), one per write.
    #[serde(default)]
    pub writes: Vec<ItemId>,
    /// Its diary episode, once written.
    pub diary: Option<ItemId>,
    /// The vital signs of the work it reviews, as the daemon measured them when it began: none for a dream before
    /// they were kept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vitals: Option<Signs>,
}

impl Dream {
    /// The id of the item of dream `number`.
    pub fn id(number: u64) -> ItemId {
        ItemId::named(DREAM, &number.to_string())
    }

    /// What its review message tells the sage beside the packet ([`ants_proto::Envelope::review_outcomes`]): the dream
    /// and what is new in the verdicts and outcomes of the sage's earlier findings; none without a review.
    pub fn review_outcomes(&self) -> Option<ReviewOutcomes> {
        let review = self.review.as_ref()?;
        Some(ReviewOutcomes { dream: self.number, findings: review.outcomes.clone() })
    }

    fn values(&self) -> Values {
        match serde_json::to_value(self) {
            Ok(Value::Object(values)) => values,
            _ => unreachable!("a dream is an object"),
        }
    }

    /// The write changing it, at `revision`, into `after`.
    pub(crate) fn change(&self, after: &Self, revision: u64) -> Write {
        let values = crate::items::changed(&self.values(), after.values());
        Write::Change { id: Self::id(after.number), values, expected: Some(revision) }
    }

    fn of(item: &crate::items::Item) -> Result<Self, StoreError> {
        serde_json::from_value(Value::Object(item.values.clone()))
            .map_err(|e| StoreError::Unavailable(format!("unreadable dream {}: {e}", item.id)))
    }

    /// Whether the ant is to be woken on a new dream thread at `now` with the packet alone: at once without a sage,
    /// else once the sage left its review unanswered for [`SAGE_TIMEOUT_HOURS`]. The sage's answer prompts the ant
    /// itself, on the review thread.
    pub fn wake_due(&self, now: DateTime<Utc>) -> bool {
        self.thread.is_none()
            && match self.state {
                State::Fallback => true,
                State::Sent => now >= self.at + Duration::hours(SAGE_TIMEOUT_HOURS),
                State::Answered | State::Done => false,
            }
    }

    /// The review packet still to send the sage at `now`: before its time to answer runs out.
    pub fn review_due(&self, now: DateTime<Utc>) -> Option<&Review> {
        self.review
            .as_ref()
            .filter(|review| !review.sent && self.state == State::Sent && self.deadline().is_some_and(|by| now < by))
    }

    /// The threads of the dream itself, which are no work for the next one to review: the thread the ant was woken
    /// on and the review thread.
    pub fn own_threads(&self) -> impl Iterator<Item = &ThreadId> {
        self.thread.iter().chain(self.review.as_ref().map(|review| &review.thread))
    }

    /// Its open findings resting on untrusted text: unanswered or deferred.
    pub fn untrusted_open(&self) -> impl Iterator<Item = &Finding> {
        self.open().filter(|finding| finding.untrusted)
    }

    /// When the sage's time to answer runs out, while its findings are awaited.
    pub fn deadline(&self) -> Option<DateTime<Utc>> {
        (self.state == State::Sent && self.thread.is_none()).then(|| self.at + Duration::hours(SAGE_TIMEOUT_HOURS))
    }

    /// When the next dream may begin.
    pub fn next(&self) -> DateTime<Utc> {
        self.at + Duration::hours(DREAM_EVERY_HOURS)
    }

    /// Its number and how many findings it has, applied, rejected, deferred and unanswered: the title of its diary.
    pub fn summary(&self) -> String {
        let count = |judgement| self.verdicts.iter().filter(|verdict| verdict.verdict == judgement).count();
        let answered = self.findings.iter().filter(|finding| self.verdict(&finding.id).is_some()).count();
        format!(
            "Dream {}: {} findings, {} applied, {} rejected, {} deferred, {} unanswered",
            self.number,
            self.findings.len(),
            count(Judgement::Applied),
            count(Judgement::Rejected),
            count(Judgement::Deferred),
            self.findings.len() - answered,
        )
    }

    /// The verdict on finding `id`, if any.
    pub fn verdict(&self, id: &str) -> Option<&Verdict> {
        self.verdicts.iter().find(|verdict| verdict.finding == id)
    }

    /// The findings to offer again: those deferred or unanswered.
    fn open(&self) -> impl Iterator<Item = &Finding> {
        self.findings
            .iter()
            .filter(|finding| self.verdict(&finding.id).is_none_or(|verdict| verdict.verdict == Judgement::Deferred))
    }

    /// The findings with a key to rest: those the ant rejected, and those on a sage's practice it applied, whose ground
    /// no write of the ant changes.
    fn resting(&self) -> impl Iterator<Item = Rested> {
        self.verdicts.iter().filter_map(|verdict| {
            let finding = self.findings.iter().find(|finding| finding.id == verdict.finding)?;
            let rests = match verdict.verdict {
                Judgement::Rejected => true,
                Judgement::Applied => finding.kind.practice(),
                Judgement::Deferred => false,
            };
            rests.then_some(())?;
            Some(Rested {
                key: finding.key.clone()?,
                finding: finding.id.clone(),
                at: verdict.at,
                count: finding.count,
            })
        })
    }
}

fn record_id() -> ItemId {
    ItemId::named(DREAMS, RECORD)
}

/// The record of the last dream and the revision of its item; none before the first.
async fn record(items: &dyn Items) -> Result<Option<(Record, u64)>, StoreError> {
    let Some(item) = items.get(&record_id()).await? else { return Ok(None) };
    let record = serde_json::from_value(Value::Object(item.values))
        .map_err(|e| StoreError::Unavailable(format!("unreadable record of the last dream: {e}")))?;
    Ok(Some((record, item.revision)))
}

/// Dream `number` and the revision of its item.
pub async fn get(items: &dyn Items, number: u64) -> Result<Option<(Dream, u64)>, StoreError> {
    match items.get(&Dream::id(number)).await? {
        Some(item) => Ok(Some((Dream::of(&item)?, item.revision))),
        None => Ok(None),
    }
}

/// The last dream, and the revision of its item; none before the first.
pub async fn last(items: &dyn Items) -> Result<Option<(Dream, u64)>, StoreError> {
    match record(items).await? {
        Some((record, _)) => get(items, record.number).await,
        None => Ok(None),
    }
}

/// Where the review of a dream beginning at `at` after `previous` starts: the previous one's beginning, else a day
/// before.
pub fn since(previous: Option<&Dream>, at: DateTime<Utc>) -> DateTime<Utc> {
    previous.map_or_else(|| at - Duration::hours(DREAM_EVERY_HOURS), |previous| previous.at)
}

/// What a dream begins with, gathered by the daemon.
#[derive(Debug, Clone)]
pub struct Start<'a> {
    pub at: DateTime<Utc>,
    pub recipient: Recipient,
    pub threads: Vec<Covered>,
    pub commits: Vec<Commit>,
    /// The context the diary of the last dream is written in when the ant left it unfinished.
    pub writer: &'a CallContext,
    /// The vital signs of the work on each of `threads`.
    pub vitals: Vec<(ThreadId, Signs)>,
}

/// Begin the next dream as `start` says, unless one began within [`DREAM_EVERY_HOURS`]: its findings gathered, the
/// last dream closed with its diary if the ant left it unfinished, and the findings rejected in it rested, in one
/// write. A thread covered that a review record names is a review ([`Covered::review`]), whatever its origin. Returns
/// the new dream; none when one began meanwhile.
pub async fn begin(items: &dyn Items, start: Start<'_>) -> Result<Option<Dream>, StoreError> {
    let card = identity::card(items).await?;
    let ages = &memory::Ages::new(start.at, card.iter().flat_map(|card| card.dormant(start.at)));
    let start = &start;
    read_modify_write(|| async move {
        let record = record(items).await?;
        let previous = match &record {
            Some((record, _)) => get(items, record.number).await?,
            None => None,
        };
        if previous.as_ref().is_some_and(|(previous, _)| previous.next() > start.at) {
            return Ok(None);
        }
        let number = record.as_ref().map_or(1, |(record, _)| record.number + 1);
        let last = previous.as_ref().map(|(previous, _)| previous);
        let since = since(last, start.at);
        let mut rested = record.as_ref().map_or_else(Vec::new, |(record, _)| record.rested.clone());
        for resting in last.into_iter().flat_map(Dream::resting) {
            rested.retain(|rested| rested.key != resting.key);
            rested.push(resting);
        }
        let reviews = review_records(items, usize::MAX).await?;
        let findings = findings(items, last, number, start, &reviews, &mut rested, ages).await?;
        let mut writes = Vec::new();
        if let Some((previous, revision)) = previous
            && previous.state != State::Done
        {
            writes.extend(close(previous, revision, start.writer, start.at));
        }
        let (state, sage, review) = match &start.recipient {
            Recipient::Sage(sage) => {
                let outcomes = packet::outcomes(items, number, ages, sage).await?;
                (State::Sent, Some(sage.clone()), Some(Review::new(outcomes)))
            }
            Recipient::Ant => (State::Fallback, None, None),
        };
        let dream = Dream {
            number,
            at: start.at,
            since,
            state,
            sage,
            review,
            thread: None,
            threads: start
                .threads
                .iter()
                .map(|covered| Covered {
                    review: covered.review || reviews.iter().any(|review| review.thread == covered.thread),
                    ..covered.clone()
                })
                .collect(),
            commits: start.commits.clone(),
            findings,
            verdicts: Vec::new(),
            writes: Vec::new(),
            diary: None,
            vitals: Some(Signs::sum(start.vitals.iter().map(|(_, signs)| signs))),
        };
        writes.push(Write::Put {
            id: Dream::id(number),
            kind: DREAM.into(),
            values: dream.values(),
            expected: Some(0),
        });
        let expected = Some(record.map_or(0, |(_, revision)| revision));
        let values = crate::items::values_of(&Record { number, rested });
        writes.push(Write::Put { id: record_id(), kind: DREAMS.into(), values, expected });
        items.write(&writes, BY).await?;
        Ok(Some(dream))
    })
    .await
}

/// Note that the ant was woken on `thread` for dream `number`: past the sage's time, its dream falls back to the
/// packet alone.
pub async fn woken(items: &dyn Items, number: u64, thread: &ThreadId) -> Result<Dream, StoreError> {
    read_modify_write(|| async move {
        let (mut dream, revision) = get(items, number).await?.ok_or_else(|| StoreError::NotFound(Dream::id(number)))?;
        if dream.thread.is_some() {
            return Ok(dream);
        }
        dream.thread = Some(thread.clone());
        if dream.state == State::Sent {
            dream.state = State::Fallback;
        }
        let mut values = Values::new();
        values.insert("thread".into(), json!(dream.thread));
        values.insert("state".into(), json!(dream.state));
        items.change(&Dream::id(number), values, Some(revision), BY).await?;
        Ok(dream)
    })
    .await
}

/// A persona or skill proposal awaiting the ant's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub id: ItemId,
    /// What it proposes, as a finding states it.
    pub text: String,
    pub untrusted: bool,
}

/// The pending persona and skill proposals a dream beginning at `at` offers: those made [`proposal::CONFIRM_AFTER_HOURS`] or
/// more before, each offered as a finding until the ant answers it.
async fn proposals(items: &dyn Items, at: DateTime<Utc>) -> Result<Vec<Proposal>, StoreError> {
    Ok(proposal::pending(items)
        .await?
        .into_iter()
        .filter(|found| found.proposal.ripe() <= at)
        .map(|found| Proposal { text: found.finding(), untrusted: !found.proposal.writer.trusted(), id: found.item.id })
        .collect())
}

/// Whether a pending proposal ripe at `now` awaits a dream: one the `last` dream did not offer, so a dream is due for
/// it even when no thread was active.
pub async fn proposal_due(items: &dyn Items, last: Option<&Dream>, now: DateTime<Utc>) -> Result<bool, StoreError> {
    let offered = |id: &ItemId| last.is_some_and(|last| last.findings.iter().any(|finding| finding.items.contains(id)));
    Ok(proposal::pending(items).await?.iter().any(|found| found.proposal.ripe() <= now && !offered(found.id())))
}

/// Finding `id` (`<dream>.<n>`) of the dream that offered it, or whose sage's answer gave it late ([`Review::late`]);
/// none when no dream has it.
pub async fn finding(items: &dyn Items, id: &str) -> Result<Option<Finding>, StoreError> {
    let Some(number) = id.split_once('.').and_then(|(number, _)| number.parse().ok()) else { return Ok(None) };
    let Some((dream, _)) = get(items, number).await? else { return Ok(None) };
    let late = dream.review.into_iter().flat_map(|review| review.late);
    Ok(dream.findings.into_iter().chain(late).find(|finding| finding.id == id))
}

/// Why a persona or skill proposal made in `context`, in answer to `finding` if it names one, rests on untrusted text;
/// none when it does not (requirements ants-x39hk.6.5 and ants-x39hk.12.5). It does when the thread came from a
/// webhook or asks a review, or when the finding it names is untrusted ([`Finding::untrusted`]). On the thread of a
/// dream whose packet holds untrusted text, an untrusted finding or the history of an untrusted thread, it does too
/// unless it names a finding of that dream: one naming none, or a finding the dream does not hold, may answer the
/// untrusted text. The reviews a sage's dream covers hold none: their text is left out ([`Covered::untrusted_text`]).
pub async fn untrusted(
    items: &dyn Items,
    context: &CallContext,
    finding: Option<&str>,
) -> Result<Option<String>, StoreError> {
    if !context.trusted() {
        return Ok(Some("the thread came from a webhook or asks a review".into()));
    }
    let on = items.list_by(DREAM, "thread", Some(&json!(context.thread)), 1).await?;
    let dream = on.first().map(Dream::of).transpose()?;
    if let Some(id) = finding {
        let held = dream.as_ref().and_then(|dream| dream.findings.iter().find(|finding| finding.id == id));
        let untrusted = match held {
            Some(held) => held.untrusted,
            None => self::finding(items, id).await?.is_some_and(|finding| finding.untrusted),
        };
        if untrusted {
            return Ok(Some(format!("finding {id} rests on untrusted text")));
        }
        if held.is_some() || dream.is_none() {
            return Ok(None);
        }
    }
    let Some(dream) = dream else { return Ok(None) };
    let answers = match finding {
        Some(id) => format!("finding {id} is not one of its findings"),
        None => "the proposal names none of its findings in `finding`".into(),
    };
    if let Some(finding) = dream.findings.iter().find(|finding| finding.untrusted) {
        return Ok(Some(format!("dream {} offers untrusted finding {} and {answers}", dream.number, finding.id)));
    }
    if dream.threads.iter().any(Covered::untrusted_text) {
        return Ok(Some(format!(
            "the packet of dream {} holds the history of an untrusted thread and {answers}",
            dream.number
        )));
    }
    Ok(None)
}

/// A finding gathered for a dream, before it is numbered.
///
/// A finding is about its items, or about a subject without items, and may rest on a count: [`Gathered::key`] names
/// it the same in every dream, so a rejected one rests ([`Rested`]) until an item changes or the count differs.
struct Gathered {
    kind: FindingKind,
    /// What it is about: its items' ids joined by commas, or a subject naming it without items.
    about: String,
    items: Vec<ItemId>,
    count: Option<u64>,
    text: String,
    untrusted: bool,
}

impl Gathered {
    /// A finding of `kind` about `items`.
    fn on(kind: FindingKind, items: Vec<ItemId>, text: String, untrusted: bool) -> Self {
        let about = items.iter().map(ItemId::as_str).collect::<Vec<_>>().join(",");
        Self { kind, about, items, count: None, text, untrusted }
    }

    fn key(&self) -> String {
        format!("{}:{}", self.kind, self.about)
    }

    /// Whether what it is about changed since `rested` was rejected: the count it rests on, or one of its items other
    /// than by a read ([`memory::Access`]).
    async fn changed(&self, items: &dyn Items, rested: &Rested) -> Result<bool, StoreError> {
        if self.count != rested.count {
            return Ok(true);
        }
        for id in &self.items {
            let history = items.history(id).await?;
            let changed = |revision: &Revision| {
                revision.at > rested.at && revision.change.after.keys().any(|value| value != "access")
            };
            if history.iter().any(changed) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// The findings of dream `number`, beginning as `start` says after `previous`: the findings of the sage's answer to
/// the review of `previous` come after the ant was woken with the packet alone ([`Review::late`]), the open findings of
/// `previous` that still hold, then the packet's own on what they do not cover yet, those on the identity of the ant
/// first ([`identity_findings`]), which stay whatever a sage answers as the open ones do ([`review::answered`]), then
/// those on memories and proposals, then those on its practice as a sage from the records of the `reviews` it made
/// ([`practice_findings`]), then those on the vital signs of the work ([`vitals`]), up to [`MAX_FINDINGS`]. An ant both
/// worked with and reviewed has one finding on its relationship record, the identity's, telling of both. An open
/// finding on the identity, the practice or the vital signs holds while its ground does, taking the text, count and
/// trust gathered now. One of the `rested` stays out while what it is about is unchanged and the ant was active less
/// than [`REST_DAYS`] since the verdict, as `ages` counts; `rested` keeps those resting on.
async fn findings(
    items: &dyn Items,
    previous: Option<&Dream>,
    number: u64,
    start: &Start<'_>,
    reviews: &[ReviewRecord],
    rested: &mut Vec<Rested>,
    ages: &memory::Ages,
) -> Result<Vec<Finding>, StoreError> {
    let proposals = proposals(items, start.at).await?;
    let memories: Vec<_> = items.list(MEMORY, usize::MAX).await?.into_iter().filter_map(MemoryItem::of).collect();
    let live = |id: &ItemId| memories.iter().find(|found| found.id() == id).filter(|f| f.memory.status.live());
    let identity_found = identity_findings::gather(items, &start.writer.ant, reviews).await?;
    let mut practice = practice_findings::gather(items, reviews, ages).await?;
    let folded = |reviewed: &Gathered| {
        reviewed.kind == FindingKind::NoReviewedRelationship
            && identity_found
                .iter()
                .any(|worked| worked.kind == FindingKind::NoRelationship && worked.about == reviewed.about)
    };
    practice.retain(|reviewed| !folded(reviewed));
    let vital = vitals::gather(&start.vitals, &vitals::trend(items, number).await?);
    let late = previous.and_then(|previous| previous.review.as_ref()).map(|review| review.late.iter());
    let mut found: Vec<Finding> = late.into_iter().flatten().cloned().collect();
    for open in previous.into_iter().flat_map(Dream::open) {
        let mut open = open.clone();
        let holds = match open.kind {
            FindingKind::Sage => true,
            FindingKind::Proposal => open.items.iter().all(|id| proposals.iter().any(|proposal| &proposal.id == id)),
            FindingKind::NeedsReview => {
                open.items.iter().all(|id| live(id).is_some_and(|f| f.memory.status == Status::NeedsReview))
            }
            kind if kind.identity() || kind.practice() || kind == FindingKind::Vitals => {
                let mut regathered = identity_found.iter().chain(&practice).chain(&vital);
                let now = regathered.find(|gathered| open.key.as_ref() == Some(&gathered.key()));
                if let Some(now) = now {
                    (open.text, open.items, open.count) = (now.text.clone(), now.items.clone(), now.count);
                    open.untrusted = now.untrusted;
                }
                now.is_some()
            }
            _ => open.items.iter().all(|id| live(id).is_some()),
        };
        if holds {
            found.push(open);
        }
    }
    let diary = previous.and_then(|previous| previous.diary.as_ref());
    let mut fresh = identity_found;
    fresh.extend(gather(items, &memories, since(previous, start.at), diary, proposals).await?);
    fresh.extend(practice);
    fresh.extend(vital);
    let covered: BTreeSet<_> = found.iter().filter_map(|finding| finding.key.clone()).collect();
    let resting = |at| ages.since(at) < Duration::days(REST_DAYS);
    let mut n = 0;
    for gathered in fresh {
        if found.len() >= MAX_FINDINGS {
            break;
        }
        let key = gathered.key();
        if covered.contains(&key) {
            continue;
        }
        if let Some(index) = rested.iter().position(|rested| rested.key == key) {
            if resting(rested[index].at) && !gathered.changed(items, &rested[index]).await? {
                continue;
            }
            rested.remove(index);
        }
        n += 1;
        found.push(Finding {
            id: format!("{number}.{n}"),
            kind: gathered.kind,
            text: gathered.text,
            items: gathered.items,
            untrusted: gathered.untrusted,
            key: Some(key),
            count: gathered.count,
            advice: None,
        });
    }
    found.truncate(MAX_FINDINGS);
    rested.retain(|rested| resting(rested.at));
    Ok(found)
}

/// The packet's own findings on `memories` and `proposals`, for a dream reviewing from `since`, the last dream's
/// `diary` left out of those to distill.
async fn gather(
    items: &dyn Items,
    memories: &[MemoryItem],
    since: DateTime<Utc>,
    diary: Option<&ItemId>,
    proposals: Vec<Proposal>,
) -> Result<Vec<Gathered>, StoreError> {
    let mut fresh = Vec::new();
    let mine = |found: &MemoryItem| found.memory.trust == memory::Trust::Untrusted;
    for found in memories.iter().filter(|found| found.memory.status.live()) {
        let (memory, votes) = (&found.memory, &found.memory.votes);
        if votes.helpful > 0 && votes.stale > 0 && found.item.updated.at >= since {
            let text = format!(
                "Memory {} \"{}\" is voted helpful {} and stale {} times: check it, then vote, supersede or keep it.",
                found.id(),
                memory.title,
                votes.helpful,
                votes.stale
            );
            fresh.push(Gathered::on(FindingKind::Contradiction, vec![found.id().clone()], text, mine(found)));
        }
    }
    for found in memory::needs_review(items).await? {
        let why = if found.memory.citation_warnings.is_empty() { "" } else { " (a citation changed)" };
        let text = format!(
            "Memory {} \"{}\" needs review{why}: check it, then vote it helpful, supersede it or vote it wrong.",
            found.id(),
            found.memory.title
        );
        fresh.push(Gathered::on(FindingKind::NeedsReview, vec![found.id().clone()], text, mine(&found)));
    }
    for cluster in memory::duplicates(items).await? {
        let named: Vec<_> = cluster.iter().map(|found| format!("{} \"{}\"", found.id(), found.memory.title)).collect();
        let text = format!(
            "Memories {} are near duplicates: keep one, superseding it with what all hold, and vote the others wrong.",
            named.join(", ")
        );
        let ids = cluster.iter().map(|found| found.id().clone()).collect();
        fresh.push(Gathered::on(FindingKind::Duplicates, ids, text, cluster.iter().any(mine)));
    }
    let mut episodes: Vec<_> = memories
        .iter()
        .filter(|found| found.memory.kind == MemoryKind::Episode && found.memory.status.live())
        .filter(|found| found.item.created.at >= since && Some(found.id()) != diary)
        .collect();
    episodes.sort_by_key(|found| found.item.created.at);
    for found in episodes {
        let text = format!(
            "Episode {} \"{}\": distill what it teaches into lessons, facts or decisions with sense_remember.",
            found.id(),
            found.memory.title
        );
        fresh.push(Gathered::on(FindingKind::Distill, vec![found.id().clone()], text, mine(found)));
    }
    for proposal in proposals {
        fresh.push(Gathered::on(FindingKind::Proposal, vec![proposal.id], proposal.text, proposal.untrusted));
    }
    Ok(fresh)
}

/// The writes closing `dream`, at `revision`, with its diary written in `context` at `at`.
fn close(mut dream: Dream, revision: u64, context: &CallContext, at: DateTime<Utc>) -> [Write; 2] {
    let id = ItemId::new();
    let context = CallContext { at, ..context.clone() };
    let diary = diary(&dream, &context);
    dream.state = State::Done;
    dream.diary = Some(id.clone());
    let mut values = Values::new();
    values.insert("state".into(), json!(dream.state));
    values.insert("diary".into(), json!(dream.diary));
    values.insert("verdicts".into(), json!(dream.verdicts));
    [diary.create(id), Write::Change { id: Dream::id(dream.number), values, expected: Some(revision) }]
}

/// The diary episode of `dream`, written in `context`: how many findings it had, the verdict on each and the items
/// written, within a memory's limits.
fn diary(dream: &Dream, context: &CallContext) -> Memory {
    let title = dream.summary();
    let mut lines = vec![format!(
        "Reviewed {} threads since {} UTC; {} of {MAX_DREAM_WRITES} writes made.",
        dream.threads.len(),
        dream.since.format("%Y-%m-%d %H:%M"),
        dream.writes.len()
    )];
    for finding in &dream.findings {
        let mut line = format!("- {} {}: ", finding.id, finding.kind);
        match dream.verdict(&finding.id) {
            Some(verdict) => {
                let _ = write!(line, "{}: {}", verdict.verdict, verdict.reason);
                if !verdict.items.is_empty() {
                    let items: Vec<_> = verdict.items.iter().map(ItemId::as_str).collect();
                    let _ = write!(line, " (items {})", items.join(", "));
                }
            }
            None => line.push_str("unanswered"),
        }
        lines.push(line);
    }
    let mut body = String::new();
    for (shown, line) in lines.iter().enumerate() {
        let more = format!("\n… {} more", lines.len() - shown);
        if body.len() + 1 + line.len() + more.len() > MAX_BODY_BYTES {
            body.push_str(&more);
            break;
        }
        if shown > 0 {
            body.push('\n');
        }
        body.push_str(line);
    }
    let title: String = title.chars().take(MAX_TITLE_CHARS).collect();
    Memory::new(MemoryKind::Episode, Scope::Own, title, body, context)
}

/// Whether `call`, of a tool of [`COUNTED`], writes a memory or a record: a `sense_identity` call writes one only
/// with `self_model` or `relationship`.
fn writes(call: &ToolCall) -> bool {
    call.name != "identity"
        || ["self_model", "relationship"].iter().any(|record| call.arguments.get(*record).is_some_and(|v| !v.is_null()))
}

/// Answer `call` of a tool writing memories or records with `handler`, counting it toward [`MAX_DREAM_WRITES`] when
/// made on the last dream's thread, refused past them. A memory or record written there while a finding resting on
/// untrusted text is open is stored as untrusted, as quoting outside content: the thread cannot tell which finding a
/// write answers.
pub(crate) async fn counted(sense: &Sense, handler: Handler, call: &ToolCall) -> Answer {
    if !writes(call) {
        return handler(sense, call).await;
    }
    let thread = &call.context.thread;
    let Ok(items) = sense.items(&call.context.ant).await else { return handler(sense, call).await };
    let on_thread = |dream: &Dream| dream.thread.as_ref() == Some(thread);
    if !matches!(last(&*items).await, Ok(Some((dream, _))) if on_thread(&dream)) {
        return handler(sense, call).await;
    }
    // One write of a dream thread of the ant at a time, so concurrent calls cannot pass the cap together.
    let ant = sense.ant(&call.context.ant);
    let _turn = ant.dream_writes.lock().await;
    let dream = match last(&*items).await {
        Ok(Some((dream, _))) if on_thread(&dream) => dream,
        _ => return handler(sense, call).await,
    };
    if dream.writes.len() >= MAX_DREAM_WRITES {
        let message = format!(
            "Dream {} made its {MAX_DREAM_WRITES} writes, the most a dream makes: answer the findings left with \
             sense_dream as deferred, so the next dream offers them again.",
            dream.number
        );
        return Answer::error("dream_cap", message);
    }
    let untrusted: Vec<_> = dream.untrusted_open().map(|finding| finding.id.as_str()).collect();
    let quotable = matches!(call.name.as_str(), "remember" | "identity");
    let mut answer = if !untrusted.is_empty() && quotable && !call.arguments.contains_key("quotes") {
        let mut quoting = call.clone();
        quoting.arguments.insert("quotes".into(), json!("external"));
        let mut answer = handler(sense, &quoting).await;
        if !answer.is_error {
            let _ = write!(
                answer.text,
                "\nStored as untrusted: dream {} holds untrusted findings still open ({}).",
                dream.number,
                untrusted.join(", ")
            );
        }
        answer
    } else {
        handler(sense, call).await
    };
    if answer.is_error {
        return answer;
    }
    let Some(written) = answer.json["id"].as_str().and_then(|id| id.parse::<ItemId>().ok()) else { return answer };
    match count_write(&*items, dream.number, written).await {
        Ok(writes) => {
            let _ = write!(answer.text, "\nDream {}: {writes} of {MAX_DREAM_WRITES} writes made.", dream.number);
            answer.json["dream_writes"] = json!(writes);
        }
        Err(e) => {
            let _ = write!(answer.text, "\nWarning: this write did not count toward dream {}: {e}.", dream.number);
        }
    }
    answer
}

/// Count a write of `id` on the thread of dream `number`; returns the writes counted.
async fn count_write(items: &dyn Items, number: u64, id: ItemId) -> Result<usize, StoreError> {
    let id = &id;
    read_modify_write(|| async move {
        let (mut dream, revision) = get(items, number).await?.ok_or_else(|| StoreError::NotFound(Dream::id(number)))?;
        dream.writes.push(id.clone());
        let mut values = Values::new();
        values.insert("writes".into(), json!(dream.writes));
        items.change(&Dream::id(number), values, Some(revision), BY).await?;
        Ok(dream.writes.len())
    })
    .await
}

#[cfg(test)]
mod tests;
