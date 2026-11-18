//! The earlier findings section of a sage's review packet: what the sage reading found in the ant's work over its last
//! [`TRACK_RECORD_DAYS`] active days, the latest reviews first, each finding with its suggested action, the ant's
//! verdict and reason and, for one applied, what became of what the ant wrote or voted on for it ([`outcome`]); then a
//! tally by kind. The sage keeps nothing between reviews: its record of an ant lives in the ant's dreams and comes back
//! in each packet, holding the findings of that sage alone. It takes at most a share of the packet's budget
//! ([`TRACK_RECORD_SHARE`]), the earlier reviews' findings left out first, and shrinks to its tally for room before the
//! identity is cut to titles.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use ants_proto::UserId;
use chrono::{DateTime, Duration, Utc};

use crate::dream::{Dream, Finding, FindingKind, Judgement, Verdict, get};
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
    let mut dreams = Vec::new();
    for number in (1..dream.number).rev() {
        let Some((earlier, _)) = get(items, number).await? else { continue };
        if ages.since(earlier.at) > Duration::days(TRACK_RECORD_DAYS) {
            break;
        }
        dreams.push(earlier);
    }
    let mut verdicts: HashMap<&str, &Verdict> = HashMap::new();
    for earlier in dreams.iter().rev() {
        for verdict in &earlier.verdicts {
            verdicts.insert(&verdict.finding, verdict);
        }
    }
    let mut record = TrackRecord {
        head: format!(
            "\n## Earlier findings of {sage}\n\nWhat {sage} found in the work of {ant} over its last \
             {TRACK_RECORD_DAYS} active days, the latest reviews first: each finding with its suggested action, the \
             verdict of {ant} and, for one applied, what became of what {ant} wrote or voted on for it.\n"
        ),
        ..TrackRecord::default()
    };
    let mut tallies: BTreeMap<&str, Tally> = BTreeMap::new();
    for earlier in dreams.iter().filter(|earlier| earlier.sage.as_ref() == Some(sage)) {
        let own = earlier.findings.iter().filter(|f| f.kind == FindingKind::Sage && f.dream() == earlier.number);
        for (n, finding) in own.enumerate() {
            let verdict = verdicts.get(finding.id.as_str()).copied();
            let mut written = Vec::new();
            if let Some(applied) = verdict.filter(|verdict| verdict.verdict == Judgement::Applied) {
                for id in &applied.items {
                    written.push(outcome(items, id, applied.at).await?);
                }
            }
            let holds = written.iter().all(|(_, holds)| *holds);
            let kind = finding.advice.as_ref().map_or("sage", |advice| advice.kind.as_str());
            tallies.entry(kind).or_default().add(verdict.map(|verdict| verdict.verdict), holds);
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
        record.tally.push_str(&tally.line(kind));
    }
    if tallies.len() > 1 {
        let all = tallies.values().fold(Tally::default(), Tally::plus);
        record.tally.push_str(&all.line("all"));
    }
    Ok(record)
}

/// `finding` as the section shows it, with the last `verdict` on it and what became of the items `written` for it;
/// `again` when the dream of the packet offers it again.
fn finding_entry(finding: &Finding, verdict: Option<&Verdict>, written: &[(String, bool)], again: bool) -> String {
    let kind = finding.advice.as_ref().map_or("sage", |advice| advice.kind.as_str());
    let mut entry = format!("- {} ({kind}): {}\n", finding.id, finding.text);
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

/// The findings of one kind and what the ant made of them.
#[derive(Debug, Clone, Copy, Default)]
struct Tally {
    made: usize,
    applied: usize,
    /// The applied that still hold ([`outcome`]).
    holding: usize,
    rejected: usize,
    deferred: usize,
    unanswered: usize,
}

impl Tally {
    /// Count a finding, with the last `verdict` on it, applied with what still `holds`.
    fn add(&mut self, verdict: Option<Judgement>, holds: bool) {
        self.made += 1;
        match verdict {
            Some(Judgement::Applied) => {
                self.applied += 1;
                self.holding += usize::from(holds);
            }
            Some(Judgement::Rejected) => self.rejected += 1,
            Some(Judgement::Deferred) => self.deferred += 1,
            None => self.unanswered += 1,
        }
    }

    fn plus(self, other: &Self) -> Self {
        Self {
            made: self.made + other.made,
            applied: self.applied + other.applied,
            holding: self.holding + other.holding,
            rejected: self.rejected + other.rejected,
            deferred: self.deferred + other.deferred,
            unanswered: self.unanswered + other.unanswered,
        }
    }

    fn line(&self, kind: &str) -> String {
        format!(
            "- {kind}: {} made, {} applied ({} still holding), {} rejected, {} deferred, {} unanswered.\n",
            self.made, self.applied, self.holding, self.rejected, self.deferred, self.unanswered
        )
    }
}
