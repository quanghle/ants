//! The earlier findings section of a sage's review packet: what the sage reading found in the ant's work over its last
//! [`TRACK_RECORD_DAYS`] active days, the latest reviews first, each finding with its suggested action, the ant's
//! verdict and reason and, for one applied, what became of what the ant wrote or voted on for it ([`outcome`]) and the
//! ant's vital signs over the dreams before it was applied and since, side by side ([`Vitals`]); then a tally by kind.
//! The sage keeps nothing between reviews: its record of an ant lives in the ant's dreams and comes back
//! in each packet, holding the findings of that sage alone. It takes at most a share of the packet's budget
//! ([`TRACK_RECORD_SHARE`]), the earlier reviews' findings left out first, and shrinks to its tally for room before the
//! identity is cut to titles.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use ants_proto::{FindingOutcome, FindingVerdict, UserId};
use chrono::{DateTime, Duration, Utc};

use crate::dream::vitals::percent;
use crate::dream::{Dream, Finding, FindingKind, FindingTally, Judgement, Signs, TREND, Verdict, get};
use crate::items::ItemId;
use crate::memory::{self, Memory, Scope, Status};
use crate::proposal::{self, ProposalItem};
use crate::records::RecordItem;
use crate::store::{Items, StoreError};

/// The days of the ant's active time, its dormancies not counted, a sage's earlier findings reach back over.
pub const TRACK_RECORD_DAYS: i64 = 30;
/// The share of a packet's budget the sage's earlier findings take at most: one part in this many.
pub const TRACK_RECORD_SHARE: usize = 8;

/// The earlier findings section of a sage's packet ([`track_record`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrackRecord {
    /// Its heading and what it holds.
    pub head: String,
    /// Each finding, the latest reviews first, with its verdict and what became of what was written for it: the first
    /// of each review under the review's heading.
    pub findings: Vec<String>,
    /// The tally by kind, or that there were no findings.
    pub tally: String,
}

impl TrackRecord {
    /// Its text, keeping the first `kept` findings.
    pub fn text(&self, kept: usize) -> String {
        let mut text = self.head.clone();
        for finding in self.findings.iter().take(kept) {
            text.push_str(finding);
        }
        text.push_str(&self.left_out(kept));
        text.push_str(&self.tally);
        text
    }

    /// The most findings it keeps within `bytes`; none when its heading and tally alone take more.
    pub(super) fn within(&self, bytes: usize) -> usize {
        let fixed = self.head.len() + self.tally.len();
        let mut kept = 0;
        let mut taken = fixed;
        for finding in &self.findings {
            if taken + finding.len() + self.left_out(kept + 1).len() > bytes {
                break;
            }
            taken += finding.len();
            kept += 1;
        }
        kept
    }

    fn left_out(&self, kept: usize) -> String {
        match self.findings.len().saturating_sub(kept) {
            0 => String::new(),
            1 => "\n1 earlier finding left out for room.\n".into(),
            left => format!("\n{left} earlier findings left out for room.\n"),
        }
    }
}

/// The earlier findings section of the packet of `dream` for `sage`, reviewing `ant`, whose items `items` holds: the
/// findings `sage` gave in the dreams that began within [`TRACK_RECORD_DAYS`] of the ant's active time before
/// `dream`, each with the last verdict on it, in the dream that offered it or one offering it again.
pub async fn track_record(
    items: &dyn Items,
    dream: &Dream,
    sage: &UserId,
    ant: &UserId,
) -> Result<TrackRecord, StoreError> {
    let card = crate::identity::card(items).await?;
    let ages = memory::Ages::new(dream.at, card.iter().flat_map(|card| card.dormant(dream.at)));
    let dreams = earlier(items, dream.number, &ages).await?;
    let verdicts = last_verdicts(&dreams);
    let mut vitals = Vitals::new(items, dream, &dreams);
    let mut record = TrackRecord {
        head: format!(
            "\n## Earlier findings of {sage}\n\nWhat {sage} found in the work of {ant} over its last \
             {TRACK_RECORD_DAYS} active days, the latest reviews first: each finding with its suggested action, the \
             verdict of {ant} and, for one applied, what became of what {ant} wrote or voted on for it and the vital \
             signs of {ant} over the dreams before it was applied and since, side by side.\n"
        ),
        ..TrackRecord::default()
    };
    let mut tallies: BTreeMap<String, FindingTally> = BTreeMap::new();
    for earlier in dreams.iter().filter(|earlier| earlier.sage.as_ref() == Some(sage)) {
        for (n, (finding, _)) in own(earlier).enumerate() {
            let found = verdicts.get(finding.id.as_str()).copied();
            let verdict = found.map(|(_, verdict)| verdict);
            let (mut written, mut signs) = (Vec::new(), None);
            if let Some((number, applied)) = found.filter(|(_, verdict)| verdict.verdict == Judgement::Applied) {
                for id in &applied.items {
                    written.push(outcome(items, id, applied.at).await?);
                }
                signs = vitals.around(number).await?;
            }
            let holds = written.iter().all(|(_, holds)| *holds);
            tallies.entry(kind(finding)).or_default().add(verdict.map(|verdict| verdict.verdict), holds);
            let mut entry = String::new();
            if n == 0 {
                let (number, day) = (earlier.number, earlier.at.format("%Y-%m-%d"));
                let _ = write!(entry, "\n### Review of dream {number}, {day}\n\n");
            }
            let again = dream.findings.iter().any(|offered| offered.id == finding.id);
            entry.push_str(&finding_entry(finding, verdict, &written, signs.as_deref(), again));
            record.findings.push(entry);
        }
    }
    if tallies.is_empty() {
        record.tally = format!("\nNo findings of {sage} in the last {TRACK_RECORD_DAYS} active days of {ant}.\n");
        return Ok(record);
    }
    let _ = write!(record.tally, "\n### Tally of the last {TRACK_RECORD_DAYS} active days\n\n");
    for (kind, tally) in &tallies {
        let _ = writeln!(record.tally, "- {kind}: {}.", tally.counts());
    }
    if tallies.len() > 1 {
        let all = tallies.values().fold(FindingTally::default(), FindingTally::plus);
        let _ = writeln!(record.tally, "- all: {}.", all.counts());
    }
    Ok(record)
}

/// The dreams before dream `number` that began within [`TRACK_RECORD_DAYS`] of the ant's active time, by `ages`, the
/// latest first.
async fn earlier(items: &dyn Items, number: u64, ages: &memory::Ages) -> Result<Vec<Dream>, StoreError> {
    let mut dreams = Vec::new();
    for number in (1..number).rev() {
        let Some((earlier, _)) = get(items, number).await? else { continue };
        if ages.since(earlier.at) > Duration::days(TRACK_RECORD_DAYS) {
            break;
        }
        dreams.push(earlier);
    }
    Ok(dreams)
}

/// The last verdict on each finding of `dreams`, the latest first, in the dream that offered it or one offering it
/// again, with the number of that dream.
fn last_verdicts(dreams: &[Dream]) -> HashMap<&str, (u64, &Verdict)> {
    let mut verdicts = HashMap::new();
    for earlier in dreams.iter().rev() {
        for verdict in &earlier.verdicts {
            verdicts.insert(verdict.finding.as_str(), (earlier.number, verdict));
        }
    }
    verdicts
}

/// The vital signs of the ant's dreams, by number, each read once: those of the packet's dream and of the dreams the
/// track record covers known from the start.
struct Vitals<'a> {
    items: &'a dyn Items,
    /// The number of the packet's dream, the last whose signs count as since.
    last: u64,
    known: BTreeMap<u64, Option<Signs>>,
}

impl<'a> Vitals<'a> {
    fn new(items: &'a dyn Items, dream: &Dream, earlier: &[Dream]) -> Self {
        let known = earlier.iter().chain([dream]).map(|dream| (dream.number, dream.vitals.clone())).collect();
        Self { items, last: dream.number, known }
    }

    async fn of(&mut self, number: u64) -> Result<Option<Signs>, StoreError> {
        if let Some(signs) = self.known.get(&number) {
            return Ok(signs.clone());
        }
        let signs = get(self.items, number).await?.and_then(|(dream, _)| dream.vitals);
        self.known.insert(number, signs.clone());
        Ok(signs)
    }

    /// The line of the signs of a finding applied in dream `number`: those of the [`TREND`] dreams up to it, the work
    /// before it was applied, beside those of the [`TREND`] dreams after it up to the packet's; none when none of them
    /// kept any.
    async fn around(&mut self, number: u64) -> Result<Option<String>, StoreError> {
        let mut before = Vec::new();
        for earlier in number.saturating_sub(TREND - 1).max(1)..=number {
            before.extend(self.of(earlier).await?.map(|signs| (earlier, signs)));
        }
        let mut since = Vec::new();
        for later in number + 1..=self.last.min(number + TREND) {
            since.extend(self.of(later).await?.map(|signs| (later, signs)));
        }
        if before.is_empty() && since.is_empty() {
            return Ok(None);
        }
        Ok(Some(signs_line(&before, &since)))
    }
}

/// A sign beside an applied finding: its label and its value.
type Sign = (&'static str, fn(&Signs) -> String);

/// The signs beside an applied finding.
const SIGNS: &[Sign] = &[
    ("threads", |signs| signs.threads.to_string()),
    ("prompts cancelled", |signs| signs.prompts.cancelled.to_string()),
    ("tool calls failed", |signs| signs.all_calls().failed_share().map_or_else(|| "-".to_owned(), percent)),
    ("tool requests rejected", |signs| signs.rejected.to_string()),
    ("tokens a thread", |signs| {
        if signs.threads == 0 { "-".to_owned() } else { (signs.tokens / u64::from(signs.threads)).to_string() }
    }),
];

/// The signs of the dreams `before` an applied finding beside those `since`, numbers alone, each dream's in turn.
fn signs_line(before: &[(u64, Signs)], since: &[(u64, Signs)]) -> String {
    let side = |dreams: &[(u64, Signs)], value: &dyn Fn(u64, &Signs) -> String| {
        if dreams.is_empty() {
            return "none".to_owned();
        }
        dreams.iter().map(|(number, signs)| value(*number, signs)).collect::<Vec<_>>().join(", ")
    };
    let numbers = |dreams| side(dreams, &|number, _| number.to_string());
    let mut line = format!("  Signs of dreams {} before it was applied | {} since:", numbers(before), numbers(since));
    let signs: Vec<_> = SIGNS
        .iter()
        .map(|(label, value)| {
            let value = |_, signs: &Signs| value(signs);
            format!("{label} {} | {}", side(before, &value), side(since, &value))
        })
        .collect();
    let _ = writeln!(line, " {}", signs.join("; "));
    line
}

/// The findings the sage gave in the review of `dream`, each with its id in the sage's record of the review, numbered
/// as the answer gave them: those of an answer in time, as the dream offered them, then those of a late one
/// ([`Review::late`]).
///
/// [`Review::late`]: crate::dream::Review::late
fn own(dream: &Dream) -> impl Iterator<Item = (&Finding, String)> {
    let number = dream.number;
    let in_time =
        dream.findings.iter().filter(move |finding| finding.kind == FindingKind::Sage && finding.dream() == number);
    let late = dream
        .review
        .iter()
        .flat_map(|review| &review.late)
        .zip(1..)
        .map(move |(finding, n)| (finding, format!("{number}.{n}")));
    in_time.map(|finding| (finding, finding.id.clone())).chain(late)
}

/// What the review of dream `number`, by `ages` of the ant's active time, tells `sage` of its earlier findings for the
/// ant: those of the dreams [`track_record`] covers whose last verdict, or for one applied whether what was written
/// for it still holds ([`outcome`]), is new since the ant's earlier reviews by `sage` told them, the oldest first, each
/// by its id in the sage's record of its review ([`own`]).
pub(in crate::dream) async fn outcomes(
    items: &dyn Items,
    number: u64,
    ages: &memory::Ages,
    sage: &UserId,
) -> Result<Vec<FindingOutcome>, StoreError> {
    let dreams = earlier(items, number, ages).await?;
    let verdicts = last_verdicts(&dreams);
    let by_sage: Vec<_> = dreams.iter().rev().filter(|earlier| earlier.sage.as_ref() == Some(sage)).collect();
    let mut told = HashMap::new();
    for review in by_sage.iter().filter_map(|earlier| earlier.review.as_ref()).filter(|review| review.sent) {
        for outcome in &review.outcomes {
            told.insert(outcome.finding.as_str(), (outcome.verdict, outcome.holds));
        }
    }
    let mut outcomes = Vec::new();
    for (finding, told_as) in by_sage.into_iter().flat_map(own) {
        let Some((_, verdict)) = verdicts.get(finding.id.as_str()) else { continue };
        let mut holds = verdict.verdict == Judgement::Applied;
        if holds {
            for id in &verdict.items {
                if !outcome(items, id, verdict.at).await?.1 {
                    holds = false;
                    break;
                }
            }
        }
        let verdict = match verdict.verdict {
            Judgement::Applied => FindingVerdict::Applied,
            Judgement::Rejected => FindingVerdict::Rejected,
            Judgement::Deferred => FindingVerdict::Deferred,
        };
        if told.get(told_as.as_str()) != Some(&(verdict, holds)) {
            outcomes.push(FindingOutcome { finding: told_as, verdict, holds });
        }
    }
    Ok(outcomes)
}

/// `finding` as the section shows it, with the last `verdict` on it, what became of the items `written` for it and the
/// line of the vital `signs` around it; `again` when the dream of the packet offers it again.
fn finding_entry(
    finding: &Finding,
    verdict: Option<&Verdict>,
    written: &[(String, bool)],
    signs: Option<&str>,
    again: bool,
) -> String {
    let mut entry = format!("- {} ({}): {}\n", finding.id, kind(finding), finding.text);
    if let Some(advice) = &finding.advice {
        let _ = writeln!(entry, "  Suggested action: {}", advice.action);
    }
    match verdict {
        Some(verdict) => {
            let _ = write!(entry, "  Verdict: {}: {}", verdict.verdict, verdict.reason);
        }
        None => entry.push_str("  Verdict: unanswered"),
    }
    if again {
        entry.push_str(" (offered again below)");
    }
    entry.push('\n');
    if !written.is_empty() {
        let lines: Vec<_> = written.iter().map(|(line, _)| line.as_str()).collect();
        let _ = writeln!(entry, "  Written: {}", lines.join("; "));
    }
    entry.push_str(signs.unwrap_or_default());
    entry
}

/// The kind of `finding` as the section names it: the sage's own, such as `lesson`, or `no_persona` for one of the
/// packet's it passed on, else its kind in the dream.
fn kind(finding: &Finding) -> String {
    finding.advice.as_ref().map_or_else(|| finding.kind.to_string(), |advice| advice.kind.clone())
}

/// What became of item `id`, written or voted on for a finding applied at `at`: a line telling it, and whether it still
/// holds, nothing of it undone since: a memory neither voted stale or wrong nor retired since, a proposal neither
/// rejected nor superseded, the item still kept. A memory superseded and a record replaced since hold: so they grow.
async fn outcome(items: &dyn Items, id: &ItemId, at: DateTime<Utc>) -> Result<(String, bool), StoreError> {
    let Some(item) = items.get(id).await? else { return Ok((format!("item {id}: no longer kept"), false)) };
    if let Some(found) = ProposalItem::of(item.clone()) {
        let status = found.proposal.status;
        let line = format!("proposal {id} ({}): {}", found.proposal.kind, status.name());
        return Ok((line, matches!(status, proposal::Status::Pending | proposal::Status::Confirmed)));
    }
    let then = items.at(id, at).await?;
    if let Some(memory) = Memory::of(&item) {
        let before = then.as_ref().and_then(Memory::of).map(|memory| memory.status);
        let mut line = format!("memory {id} \"{}\": {}", memory.title, memory.status.name());
        if let Some(by) = &memory.superseded_by {
            let _ = write!(line, " by {by}");
        }
        if before.is_some_and(|before| before != memory.status) {
            line.push_str(" since the verdict");
        }
        if let Some(access) = memory.access {
            let days = if access.days == 1 { "1 day".to_owned() } else { format!("{} days", access.days) };
            let _ = write!(line, ", read on {days}, last {}", access.last.format("%Y-%m-%d"));
        }
        let votes = &memory.votes;
        let voted: Vec<_> = [("helpful", votes.helpful), ("stale", votes.stale), ("wrong", votes.wrong)]
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .map(|(vote, count)| format!("{vote} {count}"))
            .collect();
        if !voted.is_empty() {
            let _ = write!(line, ", voted {}", voted.join(", "));
        }
        let holds = matches!(memory.status, Status::Active | Status::Superseded) || before == Some(memory.status);
        return Ok((line, holds));
    }
    let since = match then {
        Some(then) if then.revision != item.revision => "replaced since the verdict",
        Some(_) => "unchanged since the verdict",
        None => "written since the verdict",
    };
    let kind = item.kind.clone();
    let line = match RecordItem::of(item) {
        Some(found) if found.record.scope == Scope::Own => format!("self-model {id}: {since}"),
        Some(found) => format!("relationship record {id} ({}): {since}", found.record.scope),
        None => format!("{kind} {id}: {since}"),
    };
    Ok((line, true))
}
