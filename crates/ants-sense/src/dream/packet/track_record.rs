//! The earlier findings section of a sage's review packet: what the sage reading found in the ant's work over its last
//! [`TRACK_RECORD_DAYS`] active days, the latest reviews first, each finding with its suggested action, the ant's
//! verdict and reason and, for one applied, what became of what the ant wrote or voted on for it ([`outcome`]); then a
//! tally by kind. The sage keeps nothing between reviews: its record of an ant lives in the ant's dreams and comes back
//! in each packet, holding the findings of that sage alone. It takes at most a share of the packet's budget
//! ([`TRACK_RECORD_SHARE`]), the earlier reviews' findings left out first, and shrinks to its tally for room before the
//! identity is cut to titles.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use ants_proto::{FindingOutcome, FindingVerdict, UserId};
use chrono::{DateTime, Duration, Utc};

use crate::dream::{Dream, Finding, FindingKind, FindingTally, Judgement, Verdict, get};
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
    let mut record = TrackRecord {
        head: format!(
            "\n## Earlier findings of {sage}\n\nWhat {sage} found in the work of {ant} over its last \
             {TRACK_RECORD_DAYS} active days, the latest reviews first: each finding with its suggested action, the \
             verdict of {ant} and, for one applied, what became of what {ant} wrote or voted on for it.\n"
        ),
        ..TrackRecord::default()
    };
    let mut tallies: BTreeMap<String, FindingTally> = BTreeMap::new();
    for earlier in dreams.iter().filter(|earlier| earlier.sage.as_ref() == Some(sage)) {
        for (n, finding) in own(earlier).enumerate() {
            let verdict = verdicts.get(finding.id.as_str()).copied();
            let mut written = Vec::new();
            if let Some(applied) = verdict.filter(|verdict| verdict.verdict == Judgement::Applied) {
                for id in &applied.items {
                    written.push(outcome(items, id, applied.at).await?);
                }
            }
            let holds = written.iter().all(|(_, holds)| *holds);
            tallies.entry(kind(finding)).or_default().add(verdict.map(|verdict| verdict.verdict), holds);
            let mut entry = String::new();
            if n == 0 {
                let (number, day) = (earlier.number, earlier.at.format("%Y-%m-%d"));
                let _ = write!(entry, "\n### Review of dream {number}, {day}\n\n");
            }
            let again = dream.findings.iter().any(|offered| offered.id == finding.id);
            entry.push_str(&finding_entry(finding, verdict, &written, again));
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
/// again.
fn last_verdicts(dreams: &[Dream]) -> HashMap<&str, &Verdict> {
    let mut verdicts = HashMap::new();
    for earlier in dreams.iter().rev() {
        for verdict in &earlier.verdicts {
            verdicts.insert(verdict.finding.as_str(), verdict);
        }
    }
    verdicts
}

/// The findings the sage gave in the review of `dream`.
fn own(dream: &Dream) -> impl Iterator<Item = &Finding> {
    dream.findings.iter().filter(|finding| finding.kind == FindingKind::Sage && finding.dream() == dream.number)
}

/// What the review of dream `number`, by `ages` of the ant's active time, tells `sage` of its earlier findings for the
/// ant: those of the dreams [`track_record`] covers whose last verdict, or for one applied whether what was written
/// for it still holds ([`outcome`]), is new since the ant's earlier reviews by `sage` told them, the oldest first.
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
    for finding in by_sage.into_iter().flat_map(own) {
        let Some(verdict) = verdicts.get(finding.id.as_str()) else { continue };
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
        if told.get(finding.id.as_str()) != Some(&(verdict, holds)) {
            outcomes.push(FindingOutcome { finding: finding.id.clone(), verdict, holds });
        }
    }
    Ok(outcomes)
}

/// `finding` as the section shows it, with the last `verdict` on it and what became of the items `written` for it;
/// `again` when the dream of the packet offers it again.
fn finding_entry(finding: &Finding, verdict: Option<&Verdict>, written: &[(String, bool)], again: bool) -> String {
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
