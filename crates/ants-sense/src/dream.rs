//! Dreams (requirements ants-x39hk.12.3 to ants-x39hk.12.7): at most once a day, when the ant has had no prompt running
//! for a while and activity since its last review, the daemon serving it begins a dream ([`begin`]) and sends its
//! review packet ([`packet`]): to the ant's sage when one serves it ([`Recipient::Sage`]), as a message from the ant on
//! a review thread of its own ([`Review`]), else, as when the sage did not answer within [`SAGE_TIMEOUT_HOURS`] or
//! answered with nothing readable, to the ant itself on a new dream thread with the packet alone
//! ([`Dream::wake_due`]). The sage's findings prompt the ant on the review thread inside a marked block
//! ([`answered`]); an answer without findings wakes nobody.
//!
//! sense runs no model: it only gathers what a review judges and records what the ant decides. Each dream is an item of
//! kind [`DREAM`], numbered, the last one named by a record of kind [`DREAMS`], so a daemon taking over dreams no sooner.
//! Its findings, each with an id the ant answers by (`<dream>.<n>`), are the sage's when it answered with some, with
//! those carried from earlier dreams, and otherwise the packet's own: contradicting votes, memories needing review,
//! near duplicates, episodes to distill and pending persona and skill proposals ([`proposals`]). The ant answers each
//! with `sense_dream` ([`dream`]) as applied (naming the items it wrote), rejected or deferred, with a reason; the
//! deferred and unanswered come again in the next dream while they still hold. Writes on the dream thread through the
//! tools that change memories count toward [`MAX_DREAM_WRITES`], and are untrusted while a finding resting on
//! untrusted text is open ([`counted`], [`untrusted_on`]). Finishing a dream, or beginning the next one, writes its
//! diary: an episode memory summarizing its verdicts.

use std::collections::BTreeSet;
use std::fmt::{self, Write as _};

use ants_proto::provider::{CallContext, ToolCall};
use ants_proto::{ThreadId, UserId};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::Sense;
use crate::items::{ItemId, Values};
use crate::memory::{self, MAX_BODY_BYTES, MAX_TITLE_CHARS, MEMORY, Memory, MemoryItem, MemoryKind, Scope, Status};
use crate::store::{Items, StoreError, Write};
use crate::tools::{Answer, Handler};

mod packet;
mod review;
mod tool;

pub use packet::{MIN_THREAD_BYTES, PACKET_BUDGET, Reader, Thread, compose, packet};
pub use review::{
    Advice, Evidence, Parsed, REVIEW_BEGIN, REVIEW_END, Review, Reviewed, answered, parse, review_sent, untrusted_on,
};
pub use tool::{dream, schema};

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
/// The tools whose writes on a dream thread count toward [`MAX_DREAM_WRITES`]: those writing memories.
pub const COUNTED: &[&str] = &["remember", "vote"];
/// The most characters of a verdict's reason.
pub const MAX_REASON_CHARS: usize = 500;

/// Who sense changes dream items for ([`crate::items::Stamp::by`]).
const BY: &str = "sense:dream";
/// The name of the record of the last dream ([`ItemId::named`]).
const RECORD: &str = "last";
/// How many times a write of a dream is tried again on items changed meanwhile.
const TRIES: usize = 3;

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

/// A thread a packet covers, and whether its origin is untrusted (a webhook relaying the outside).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Covered {
    pub thread: ThreadId,
    #[serde(default)]
    pub untrusted: bool,
}

/// A commit a packet names, so its reader checks the code out as the threads left it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    /// The repository's normalized git remote ([`memory::normalize_remote`]).
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
    /// The memories or proposals it is about.
    #[serde(default)]
    pub items: Vec<ItemId>,
    /// Whether it rests on untrusted text: an untrusted memory or thread.
    #[serde(default)]
    pub untrusted: bool,
    /// A sage's finding: its own kind, its evidence and the action it suggests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub advice: Option<Advice>,
}

impl Finding {
    /// The number of the dream that first offered it.
    pub fn dream(&self) -> u64 {
        self.id.split_once('.').and_then(|(number, _)| number.parse().ok()).unwrap_or_default()
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
}

impl Dream {
    /// The id of the item of dream `number`.
    pub fn id(number: u64) -> ItemId {
        ItemId::named(DREAM, &number.to_string())
    }

    fn values(&self) -> Values {
        match serde_json::to_value(self) {
            Ok(Value::Object(values)) => values,
            _ => unreachable!("a dream is an object"),
        }
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
}

fn record_id() -> ItemId {
    ItemId::named(DREAMS, RECORD)
}

/// The number of the last dream and the revision of its record; none before the first.
async fn record(items: &dyn Items) -> Result<Option<(u64, u64)>, StoreError> {
    let Some(item) = items.get(&record_id()).await? else { return Ok(None) };
    Ok(item.values.get("number").and_then(Value::as_u64).map(|number| (number, item.revision)))
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
        Some((number, _)) => get(items, number).await,
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
}

/// Begin the next dream as `start` says, unless one began within [`DREAM_EVERY_HOURS`]: its findings gathered, the
/// last dream closed with its diary if the ant left it unfinished, in one write. Returns the new dream; none when one
/// began meanwhile.
pub async fn begin(items: &dyn Items, start: Start<'_>) -> Result<Option<Dream>, StoreError> {
    for _ in 0..TRIES {
        let record = record(items).await?;
        let previous = match record {
            Some((number, _)) => get(items, number).await?,
            None => None,
        };
        if previous.as_ref().is_some_and(|(previous, _)| previous.next() > start.at) {
            return Ok(None);
        }
        let number = record.map_or(1, |(number, _)| number + 1);
        let since = since(previous.as_ref().map(|(previous, _)| previous), start.at);
        let findings = findings(items, previous.as_ref().map(|(previous, _)| previous), number, since).await?;
        let mut writes = Vec::new();
        if let Some((previous, revision)) = previous
            && previous.state != State::Done
        {
            writes.extend(close(previous, revision, start.writer, start.at));
        }
        let (state, sage, review) = match &start.recipient {
            Recipient::Sage(sage) => (State::Sent, Some(sage.clone()), Some(Review::new())),
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
            threads: start.threads.clone(),
            commits: start.commits.clone(),
            findings,
            verdicts: Vec::new(),
            writes: Vec::new(),
            diary: None,
        };
        writes.push(Write::Put {
            id: Dream::id(number),
            kind: DREAM.into(),
            values: dream.values(),
            expected: Some(0),
        });
        let mut values = Values::new();
        values.insert("number".into(), json!(number));
        let expected = Some(record.map_or(0, |(_, revision)| revision));
        writes.push(Write::Put { id: record_id(), kind: DREAMS.into(), values, expected });
        match items.write(&writes, BY).await {
            Ok(_) => return Ok(Some(dream)),
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Err(StoreError::Unavailable("the dreams kept changing meanwhile".into()))
}

/// Note that the ant was woken on `thread` for dream `number`: past the sage's time, its dream falls back to the
/// packet alone.
pub async fn woken(items: &dyn Items, number: u64, thread: &ThreadId) -> Result<Dream, StoreError> {
    for _ in 0..TRIES {
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
        match items.change(&Dream::id(number), values, Some(revision), BY).await {
            Ok(_) => return Ok(dream),
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Err(StoreError::Unavailable(format!("dream {number} kept changing meanwhile")))
}

/// A persona or skill proposal awaiting the ant's verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub id: ItemId,
    /// What it proposes, as a finding states it.
    pub text: String,
    pub untrusted: bool,
}

/// The pending persona and skill proposals, each offered as a finding until the ant answers it: none while
/// `sense_persona` and `sense_skill` keep none.
async fn proposals(_items: &dyn Items) -> Result<Vec<Proposal>, StoreError> {
    Ok(Vec::new())
}

/// The findings of dream `number`, reviewing from `since`: the open findings of `previous` that still hold, then the
/// packet's own on what they do not cover yet, up to [`MAX_FINDINGS`].
async fn findings(
    items: &dyn Items,
    previous: Option<&Dream>,
    number: u64,
    since: DateTime<Utc>,
) -> Result<Vec<Finding>, StoreError> {
    let memories: Vec<_> = items.list(MEMORY, usize::MAX).await?.into_iter().filter_map(MemoryItem::of).collect();
    let live = |id: &ItemId| memories.iter().find(|found| found.id() == id).filter(|f| f.memory.status.live());
    let mut found: Vec<Finding> = Vec::new();
    for open in previous.into_iter().flat_map(Dream::open) {
        let holds = match open.kind {
            FindingKind::Sage | FindingKind::Proposal => true,
            FindingKind::NeedsReview => {
                open.items.iter().all(|id| live(id).is_some_and(|f| f.memory.status == Status::NeedsReview))
            }
            _ => open.items.iter().all(|id| live(id).is_some()),
        };
        if holds {
            found.push(open.clone());
        }
    }
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
            fresh.push((FindingKind::Contradiction, vec![found.id().clone()], text, mine(found)));
        }
    }
    for found in memory::needs_review(items).await? {
        let why = if found.memory.citation_warnings.is_empty() { "" } else { " (a citation changed)" };
        let text = format!(
            "Memory {} \"{}\" needs review{why}: check it, then vote it helpful, supersede it or vote it wrong.",
            found.id(),
            found.memory.title
        );
        fresh.push((FindingKind::NeedsReview, vec![found.id().clone()], text, mine(&found)));
    }
    for cluster in memory::duplicates(items).await? {
        let named: Vec<_> = cluster.iter().map(|found| format!("{} \"{}\"", found.id(), found.memory.title)).collect();
        let text = format!(
            "Memories {} are near duplicates: keep one, superseding it with what all hold, and vote the others wrong.",
            named.join(", ")
        );
        let untrusted = cluster.iter().any(mine);
        fresh.push((
            FindingKind::Duplicates,
            cluster.iter().map(|found| found.id().clone()).collect(),
            text,
            untrusted,
        ));
    }
    let diary = previous.and_then(|previous| previous.diary.as_ref());
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
        fresh.push((FindingKind::Distill, vec![found.id().clone()], text, mine(found)));
    }
    for proposal in proposals(items).await? {
        fresh.push((FindingKind::Proposal, vec![proposal.id], proposal.text, proposal.untrusted));
    }
    let covered: BTreeSet<_> = found.iter().map(|finding| (finding.kind, finding.items.clone())).collect();
    let mut n = 0;
    for (kind, ids, text, untrusted) in fresh {
        if found.len() >= MAX_FINDINGS {
            break;
        }
        if !covered.contains(&(kind, ids.clone())) {
            n += 1;
            found.push(Finding { id: format!("{number}.{n}"), kind, text, items: ids, untrusted, advice: None });
        }
    }
    found.truncate(MAX_FINDINGS);
    Ok(found)
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
    let count = |judgement| dream.verdicts.iter().filter(|verdict| verdict.verdict == judgement).count();
    let answered = dream.findings.iter().filter(|finding| dream.verdict(&finding.id).is_some()).count();
    let title = format!(
        "Dream {}: {} findings, {} applied, {} rejected, {} deferred, {} unanswered",
        dream.number,
        dream.findings.len(),
        count(Judgement::Applied),
        count(Judgement::Rejected),
        count(Judgement::Deferred),
        dream.findings.len() - answered,
    );
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

/// Answer `call` of a tool writing memories with `handler`, counting it toward [`MAX_DREAM_WRITES`] when made on the
/// last dream's thread, refused past them. A memory or record written there while a finding resting on untrusted text
/// is open is stored as untrusted, as quoting outside content: the thread cannot tell which finding a write answers.
pub(crate) async fn counted(sense: &Sense, handler: Handler, call: &ToolCall) -> Answer {
    let thread = &call.context.thread;
    let Ok(items) = sense.items(&call.context.ant).await else { return handler(sense, call).await };
    let on_thread = |dream: &Dream| dream.thread.as_ref() == Some(thread);
    if !matches!(last(&*items).await, Ok(Some((dream, _))) if on_thread(&dream)) {
        return handler(sense, call).await;
    }
    // One write of a dream thread at a time, so concurrent calls cannot pass the cap together.
    let _turn = sense.dream_writes.lock().await;
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
    let mut answer = if !untrusted.is_empty() && call.name == "remember" && !call.arguments.contains_key("quotes") {
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
    for _ in 0..TRIES {
        let (mut dream, revision) = get(items, number).await?.ok_or_else(|| StoreError::NotFound(Dream::id(number)))?;
        dream.writes.push(id.clone());
        let mut values = Values::new();
        values.insert("writes".into(), json!(dream.writes));
        match items.change(&Dream::id(number), values, Some(revision), BY).await {
            Ok(_) => return Ok(dream.writes.len()),
            Err(StoreError::Conflict { .. }) => {}
            Err(e) => return Err(e),
        }
    }
    Err(StoreError::Unavailable(format!("dream {number} kept changing meanwhile")))
}

#[cfg(test)]
mod tests;
