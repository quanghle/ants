//! Dreams (requirements ants-x39hk.12.3 to ants-x39hk.12.7): at most once a day, when the ant has had no prompt running
//! for a while and activity since its last review, the daemon serving it begins a dream ([`begin`]) and sends its
//! review packet ([`packet`]): to the ant's sage when one serves it ([`Recipient::Sage`]), as a message from the ant on
//! a review thread of its own ([`Review`]), else, as when the sage did not answer within [`SAGE_TIMEOUT_HOURS`] or
//! answered with nothing readable, to the ant itself on a new dream thread with the packet alone
//! ([`Dream::wake_due`]). The sage's findings prompt the ant on the review thread inside a marked block
//! ([`answered`]); an answer without findings wakes nobody, and the findings of one come after the ant was woken with
//! the packet alone are offered in the next dream ([`Review::late`]), up to [`MAX_LATE_FINDINGS`], in a marked block
//! of their own. The packet's findings a sage's answer leaves unshown are held for the next dream ([`Dream::held`]).
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
//! [`Dream::untrusted_open`]); persona and skill proposals, and trails saved, are refused on untrusted ground ([`ground`]). A
//! sage's [`TRAIL`] finding is applied by saving the trail it suggests, which the daemon records on the dream thread
//! ([`trail_saved`]), counted as a write, and naming it in the verdict's trails; it holds while the trail stands in the
//! colony's saved trails the daemon reads ([`ColonyTrails`]), which a sage's packet lists. Finishing a dream, or
//! beginning the next one, writes its diary: an episode memory summarizing its verdicts. Each dream keeps the vital
//! signs of the work it reviews, measured by the daemon as it begins ([`Signs`]).
//!
//! A sage's own dream covers the reviews it was asked as threads too ([`Covered::review`]), but its packet lists them by
//! their records ([`reviewing()`]), leaving out their text, the packets of other ants' work: so they make nothing of
//! the dream untrusted, but for a finding on its practice quoting the reasons the ants reviewed gave for rejecting its
//! findings ([`practice_findings`]). Each record keeps what its review thread spent ([`ReviewSpend`]), which the sage's
//! tally shows a review and a finding still holding ([`ReviewCost`]).

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use ants_proto::provider::CallContext;
use ants_proto::{ReviewOutcomes, ThreadId, UserId};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::identity;
use crate::items::{ItemId, ItemValues, Revision, readable};
use crate::memory::{self, MAX_BODY_BYTES, MAX_TITLE_CHARS, MEMORY, Memory, MemoryItem, MemoryKind, Scope, Status};
use crate::proposal;
use crate::store::{Items, StoreError, Write, read_modify_write};

mod count;
mod finding;
mod gather;
mod ground;
mod identity_findings;
mod packet;
mod practice_findings;
mod review;
pub(crate) mod reviews;
mod tool;
mod vitals;

use count::capped;
pub(crate) use count::counted;
pub use count::{TrailGround, TrailRefused};
pub use finding::{Finding, FindingKind, Judgement, Rested, SavedTrail, Verdict};
use gather::Gathered;
pub use gather::{Start, begin, proposal_due};
pub(crate) use ground::{Making, Refused, ground};
pub(crate) use identity_findings::{
    OWN_AFTER_THREADS, PERSONA_AFTER_THREADS, RELATIONSHIP_AFTER_THREADS, SELF_MODEL_AFTER_THREADS, STALE_ACTIVE_DAYS,
};
pub(crate) use packet::finding_line;
pub use packet::{Action, ColonyTrail, ColonyTrails, PACKET_BUDGET, Reader, Thread, packet};
pub(crate) use review::{Advice, FOLLOW_UP, MAX_TRAIL_FINDINGS, REVIEW_END, TRAIL, parse};
pub use review::{REVIEW_BEGIN, Review, Reviewed, answered, review_sent};
pub(crate) use reviews::{FindingTally, REVIEW, ReviewTally, ReviewedFinding, review_tally, tally_lines};
pub use reviews::{ReviewRecord, ReviewSpend, review_answered, review_received, review_record, review_records};
pub(crate) use tool::{dream, schema};
pub use vitals::{Signs, Tally};
pub(crate) use vitals::{TREND, before, changes, trend};
#[cfg(test)]
pub(crate) use {
    count::trail_saved,
    ground::untrusted,
    packet::{
        Earlier, Identity, OUTCOME_DAYS, Section, Standing, TRACK_RECORD_DAYS, TRAILS_SHARE, compose, reviewing,
        track_record,
    },
    practice_findings::{
        COSTLY_REVIEW_COST_MICROS, COSTLY_REVIEW_TOKENS, JUDGED_FINDINGS, PRACTICE_DAYS, QUOTED_REASONS,
        REJECTED_FINDINGS,
    },
    review::{Evidence, FollowUp, MAX_LATE_FINDINGS},
    reviews::{AntTally, KindTally, ReviewCost},
    vitals::{CANCELLED_FINDING, CAPPED_FINDING, Ends, FAILED_MIN_CALLS, FAILED_SHARE_FLOOR, REJECTED_FINDING},
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
/// The dreams before the last on whose review thread a sage's answer is dropped, having come after the next dream
/// began.
pub const DROPPED_ANSWER_DREAMS: u64 = 5;
/// The most writes through sense's tools a dream thread makes ([`counted`]).
pub const MAX_DREAM_WRITES: usize = 20;
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

/// The record of the last dream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    number: u64,
    rested: Vec<Rested>,
}

impl ItemValues for Record {
    const KIND: &str = "record of the last dream";
}

impl ItemValues for Dream {
    const KIND: &str = DREAM;
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
    /// The trails saved on the dream thread ([`trail_saved`]), each counted as a write.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trails: Vec<SavedTrail>,
    /// The packet's own findings a sage's answer left unshown, neither on the identity nor offered again
    /// ([`review::answered`]), numbered after those it keeps: unanswered, the next dream offers them again before the
    /// others it offers again ([`Dream::again`]).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub held: Vec<Finding>,
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
    /// and a fixed batch of changed verdicts and outcomes of the sage's earlier findings, with the ids eligible for
    /// follow-ups; none without a review.
    pub fn review_outcomes(&self) -> Option<ReviewOutcomes> {
        let review = self.review.as_ref()?;
        Some(ReviewOutcomes {
            dream: self.number,
            findings: review.outcomes.clone(),
            earlier_findings: review.earlier_findings.keys().cloned().collect(),
        })
    }

    /// The write changing it, at `revision`, into `after`.
    pub(crate) fn change(&self, after: &Self, revision: u64) -> Write {
        let values = crate::items::changed(&self.values(), after.values());
        Write::Change { id: Self::id(after.number), values, expected: Some(revision) }
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

    /// The writes made on its thread, toward [`MAX_DREAM_WRITES`]: the items written and the trails saved.
    pub fn writes_made(&self) -> usize {
        self.writes.len() + self.trails.len()
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
    pub fn open(&self) -> impl Iterator<Item = &Finding> {
        self.findings
            .iter()
            .filter(|finding| self.verdict(&finding.id).is_none_or(|verdict| verdict.verdict == Judgement::Deferred))
    }

    /// The findings the next dream offers again while they hold: those held unshown first ([`Dream::held`]), then
    /// those deferred or unanswered.
    pub fn again(&self) -> impl Iterator<Item = &Finding> {
        self.held.iter().chain(self.open())
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
    Ok(Some((Record::of(&item)?, item.revision)))
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

/// Every dream, the first first; one unreadable left out with a warning.
pub async fn all(items: &dyn Items) -> Result<Vec<Dream>, StoreError> {
    let found = items.list(DREAM, usize::MAX).await?;
    let mut dreams: Vec<Dream> = readable(found, Dream::of).into_iter().map(|(_, dream)| dream).collect();
    dreams.sort_by_key(|dream| dream.number);
    Ok(dreams)
}

/// How many findings rest with the record of the last dream ([`Rested`]); none before the first.
pub async fn rested_count(items: &dyn Items) -> Result<usize, StoreError> {
    Ok(record(items).await?.map_or(0, |(record, _)| record.rested.len()))
}

/// Where the review of a dream beginning at `at` after `previous` starts: the previous one's beginning, else a day
/// before.
pub fn since(previous: Option<&Dream>, at: DateTime<Utc>) -> DateTime<Utc> {
    previous.map_or_else(|| at - Duration::hours(DREAM_EVERY_HOURS), |previous| previous.at)
}

/// Note that the ant was woken on `thread` for dream `number`: past the sage's time, its dream falls back to the
/// packet alone.
pub async fn woken(items: &dyn Items, number: u64, thread: &ThreadId) -> Result<Dream, StoreError> {
    let woken = |dream: &mut Dream| {
        if dream.thread.is_none() {
            dream.thread = Some(thread.clone());
            if dream.state == State::Sent {
                dream.state = State::Fallback;
            }
        }
    };
    Ok(update(items, number, woken).await?.0)
}

/// Dream `number` after `change`, which tells what else it made of it, made on the revision read and again when
/// another writer changed the dream meanwhile ([`read_modify_write`]).
pub(crate) async fn update<T>(
    items: &dyn Items,
    number: u64,
    change: impl Fn(&mut Dream) -> T,
) -> Result<(Dream, T), StoreError> {
    let change = &change;
    read_modify_write(|| async move {
        let (before, revision) = get(items, number).await?.ok_or_else(|| StoreError::NotFound(Dream::id(number)))?;
        let mut after = before.clone();
        let made = change(&mut after);
        rewrite(items, &before, &after, revision).await?;
        Ok((after, made))
    })
    .await
}

/// Write `before`, a dream read at `revision`, as `after`: only the values it changes, nothing when none.
pub(crate) async fn rewrite(items: &dyn Items, before: &Dream, after: &Dream, revision: u64) -> Result<(), StoreError> {
    if before != after {
        items.write(&[before.change(after, revision)], BY).await?;
    }
    Ok(())
}

/// Finding `id` (`<dream>.<n>`) of the dream that offered it, or whose sage's answer gave it late ([`Review::late`]);
/// none when no dream has it.
pub async fn finding(items: &dyn Items, id: &str) -> Result<Option<Finding>, StoreError> {
    let Some(number) = id.split_once('.').and_then(|(number, _)| number.parse().ok()) else { return Ok(None) };
    let Some((dream, _)) = get(items, number).await? else { return Ok(None) };
    let late = dream.review.into_iter().flat_map(|review| review.late);
    Ok(dream.findings.into_iter().chain(dream.held).chain(late).find(|finding| finding.id == id))
}

/// The writes closing `dream`, `before` as read at `revision`, with its diary written in `context` at `at`.
fn close(before: &Dream, mut dream: Dream, revision: u64, context: &CallContext, at: DateTime<Utc>) -> [Write; 2] {
    let id = ItemId::new();
    let context = CallContext { at, ..context.clone() };
    let diary = diary(&dream, &context);
    dream.state = State::Done;
    dream.diary = Some(id.clone());
    [diary.create(id), before.change(&dream, revision)]
}

/// The diary episode of `dream`, written in `context`: how many findings it had, the verdict on each and the items
/// written and trails saved, within a memory's limits.
fn diary(dream: &Dream, context: &CallContext) -> Memory {
    let title = dream.summary();
    let mut lines = vec![format!(
        "Reviewed {} threads since {} UTC; {} of {MAX_DREAM_WRITES} writes made.",
        dream.threads.len(),
        dream.since.format("%Y-%m-%d %H:%M"),
        dream.writes_made()
    )];
    for finding in &dream.findings {
        let mut line = format!("- {} {}: ", finding.id, finding.kind);
        match dream.verdict(&finding.id) {
            Some(verdict) => {
                let _ = write!(line, "{}: {}{}", verdict.verdict, verdict.reason, verdict.named());
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

#[cfg(test)]
mod tests;
