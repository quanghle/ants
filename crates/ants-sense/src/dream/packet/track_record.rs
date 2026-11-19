//! The earlier findings section of a sage's review packet: what the sage reading found in the ant's work over its last
//! [`TRACK_RECORD_DAYS`] active days, the latest reviews first, each finding with its suggested action, the ant's
//! verdict and reason and, for one applied, what became of what the ant wrote or voted on for it ([`outcome`]) and of
//! the trails it saved for it ([`ColonyTrails`]), and the ant's vital signs over the dreams before it was applied and
//! since, side by side ([`Vitals`]); then a tally by kind.
//! The sage keeps nothing between reviews: its record of an ant lives in the ant's dreams and comes back
//! in each packet, holding the findings of that sage alone. It takes at most a share of the packet's budget
//! ([`TRACK_RECORD_SHARE`]), the earlier reviews' findings left out first, and shrinks to its tally for room before the
//! identity is cut to titles. The verdicts and outcomes the ant's reviews tell the sage's records reach further back:
//! [`OUTCOME_DAYS`] ([`review_history`]).

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use ants_proto::{FindingOutcome, FindingVerdict, MAX_REVIEW_FINDINGS_BYTES, UserId};
use chrono::{DateTime, Duration, Utc};

use super::{ColonyTrails, Section};
use crate::dream::reviews::told_reason;
use crate::dream::vitals::percent;
use crate::dream::{Dream, Finding, FindingKind, FindingTally, Judgement, Signs, TREND, Verdict, get};
use crate::items::ItemId;
use crate::memory::{self, Memory, Scope, Status};
use crate::proposal::{self, ProposalItem};
use crate::records::RecordItem;
use crate::store::{Items, StoreError};

/// The days of the ant's active time, its dormancies not counted, a sage's earlier findings reach back over.
pub const TRACK_RECORD_DAYS: i64 = 30;
/// The days of the ant's active time, its dormancies not counted, over which the verdicts on a sage's findings, and
/// whether an applied one still holds, are told to the sage's records (`review_outcomes`): further than the packet
/// shows.
pub const OUTCOME_DAYS: i64 = 90;
/// The share of a packet's budget the sage's earlier findings take at most: one part in this many.
pub const TRACK_RECORD_SHARE: usize = 8;

/// The earlier findings section of the packet of `dream` for `sage`, reviewing `ant`, whose items `items` holds: the
/// findings `sage` gave in the dreams that began within [`TRACK_RECORD_DAYS`] of the ant's active time before
/// `dream`, each by its id in the sage's record of its review ([`own`]) and with the last verdict on it, in the dream
/// that offered it or one offering it again, and the chain of follow-ups: a [`FOLLOW_UP`] with the finding it follows,
/// a finding followed up with its follow-ups. An entry each, the first of each review under the review's heading. An
/// applied finding holds while what was written for it holds and the trails saved for it stand in the colony's saved
/// `trails` ([`super::Standing::holds`]), by its items alone where those are not known.
///
/// [`FOLLOW_UP`]: crate::dream::FOLLOW_UP
pub async fn track_record(
    items: &dyn Items,
    dream: &Dream,
    sage: &UserId,
    ant: &UserId,
    trails: &ColonyTrails,
) -> Result<Section, StoreError> {
    let dreams = covered(items, dream).await?;
    let verdicts = last_verdicts(&dreams);
    let by_sage: Vec<_> = dreams.iter().filter(|earlier| earlier.sage.as_ref() == Some(sage)).collect();
    let mut followed: HashMap<String, Vec<String>> = HashMap::new();
    for (finding, told_as) in by_sage.iter().flat_map(|earlier| own(earlier)) {
        let follows = finding.advice.as_ref().and_then(|advice| advice.follows.as_ref());
        if let Some(follows) = follows.filter(|follows| follows.finding.is_some()) {
            followed.entry(follows.named.clone()).or_default().push(told_as);
        }
    }
    let mut vitals = Vitals::new(items, dream, &dreams);
    let mut record = Section {
        head: format!(
            "\n## Earlier findings of {sage}\n\nWhat {sage} found in the work of {ant} over its last \
             {TRACK_RECORD_DAYS} active days, the latest reviews first: each finding with its suggested action, the \
             verdict of {ant} and, for one applied, what became of what {ant} wrote or voted on for it, the ratings of \
             the trails it saved for it and the vital signs of {ant} over the dreams before it was applied and since, \
             side by side; a follow_up with the finding it follows, and a finding followed up with its follow-ups. An \
             applied finding still holds while what was written stands and each trail saved for it is still saved, \
             rated failed no more often than worked.\n"
        ),
        noun: "earlier finding",
        ..Section::default()
    };
    let mut tallies: BTreeMap<String, FindingTally> = BTreeMap::new();
    for earlier in by_sage {
        for (n, (finding, told_as)) in own(earlier).enumerate() {
            let found = verdicts.get(finding.id.as_str()).copied();
            let verdict = found.map(|(_, verdict)| verdict);
            let (mut written, mut saved, mut signs) = (Vec::new(), None, None);
            if let Some((number, applied)) = found.filter(|(_, verdict)| verdict.verdict == Judgement::Applied) {
                for id in &applied.items {
                    written.push(outcome(items, id, applied.at).await?);
                }
                if !applied.trails.is_empty() {
                    saved = Some(trails.named(&applied.trails));
                }
                signs = vitals.around(number).await?;
            }
            let holds = written.iter().all(|(_, holds)| *holds)
                && saved.as_ref().is_none_or(|(_, holds)| holds.unwrap_or(true));
            let saved = saved.map(|(line, _)| line);
            tallies.entry(kind(finding)).or_default().add(verdict.map(|verdict| verdict.verdict), holds);
            let mut entry = String::new();
            if n == 0 {
                let (number, day) = (earlier.number, earlier.at.format("%Y-%m-%d"));
                let _ = write!(entry, "\n### Review of dream {number}, {day}\n\n");
            }
            let again = dream.findings.iter().any(|offered| offered.id == finding.id);
            let shown = Shown { written: &written, saved: saved.as_deref(), signs: signs.as_deref() };
            entry.push_str(&finding_entry((finding, &told_as), verdict, shown, again));
            if let Some(follow_ups) = followed.get(&told_as) {
                let _ = writeln!(entry, "  Followed up by {}.", follow_ups.join(", "));
            }
            record.entries.push(entry);
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

/// The dreams the earlier findings section of the packet of `dream` covers: those before it that began within
/// [`TRACK_RECORD_DAYS`] of the ant's active time, its dormancies not counted, the latest first.
async fn covered(items: &dyn Items, dream: &Dream) -> Result<Vec<Dream>, StoreError> {
    let card = crate::identity::card(items).await?;
    let ages = memory::Ages::new(dream.at, card.iter().flat_map(|card| card.dormant(dream.at)));
    earlier(items, dream.number, &ages, TRACK_RECORD_DAYS).await
}

/// The dreams before dream `number` that began within `days` of the ant's active time, by `ages`, the latest first.
async fn earlier(items: &dyn Items, number: u64, ages: &memory::Ages, days: i64) -> Result<Vec<Dream>, StoreError> {
    let mut dreams = Vec::new();
    for number in (1..number).rev() {
        let Some((earlier, _)) = get(items, number).await? else { continue };
        if ages.since(earlier.at) > Duration::days(days) {
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
/// ant: a bounded batch of changed outcomes over [`OUTCOME_DAYS`] (oldest first), and follow-up targets over
/// [`TRACK_RECORD_DAYS`], the same window as the packet's earlier findings. Both use the sage's numbering ([`own`]).
/// An applied finding naming trails holds as [`track_record`] judges it by the colony's saved `trails`; where those
/// are not known, as last told, or by its items alone when never told.
pub(in crate::dream) async fn review_history(
    items: &dyn Items,
    number: u64,
    ages: &memory::Ages,
    sage: &UserId,
    trails: &ColonyTrails,
) -> Result<(Vec<FindingOutcome>, BTreeMap<String, String>), StoreError> {
    let dreams = earlier(items, number, ages, OUTCOME_DAYS).await?;
    let verdicts = last_verdicts(&dreams);
    let by_sage: Vec<_> = dreams.iter().rev().filter(|earlier| earlier.sage.as_ref() == Some(sage)).collect();
    let mut told = HashMap::new();
    // The original findings and their latest verdicts are the backlog; only acknowledged batches subtract from it.
    for review in by_sage.iter().filter_map(|earlier| earlier.review.as_ref()).filter(|review| review.sent) {
        for outcome in &review.outcomes {
            told.insert(outcome.finding.as_str(), (outcome.verdict, outcome.holds));
        }
    }
    let (mut outcomes, mut targets) = (Vec::new(), BTreeMap::new());
    let mut budget = Some(MAX_REVIEW_FINDINGS_BYTES - "[]".len());
    for earlier in by_sage {
        let eligible = ages.since(earlier.at) <= Duration::days(TRACK_RECORD_DAYS);
        for (finding, told_as) in own(earlier) {
            if eligible {
                targets.insert(told_as.clone(), finding.id.clone());
            }
            let Some(left) = budget else { continue };
            let Some((_, verdict)) = verdicts.get(finding.id.as_str()) else { continue };
            let mut holds = verdict.verdict == Judgement::Applied;
            if holds && !verdict.trails.is_empty() {
                let told_holds = told.get(told_as.as_str()).map(|(_, holds)| *holds);
                holds = trails.named(&verdict.trails).1.or(told_holds).unwrap_or(true);
            }
            if holds {
                for id in &verdict.items {
                    if !outcome(items, id, verdict.at).await?.1 {
                        holds = false;
                        break;
                    }
                }
            }
            let judgement = match verdict.verdict {
                Judgement::Applied => FindingVerdict::Applied,
                Judgement::Rejected => FindingVerdict::Rejected,
                Judgement::Deferred => FindingVerdict::Deferred,
            };
            if told.get(told_as.as_str()) == Some(&(judgement, holds)) {
                continue;
            }
            let finding =
                FindingOutcome { finding: told_as, verdict: judgement, holds, reason: told_reason(&verdict.reason) };
            let bytes = serde_json::to_vec(&finding).expect("a finding outcome is JSON").len()
                + usize::from(!outcomes.is_empty());
            budget = left.checked_sub(bytes);
            if budget.is_some() {
                outcomes.push(finding);
            }
        }
    }
    Ok((outcomes, targets))
}

/// What an entry of the section shows of what became of an applied finding.
struct Shown<'a> {
    /// What became of the items written for it, each with whether it holds ([`outcome`]).
    written: &'a [(String, bool)],
    /// The line of the trails saved for it, as they stand ([`ColonyTrails`]).
    saved: Option<&'a str>,
    /// The line of the vital signs around it ([`Vitals::around`]).
    signs: Option<&'a str>,
}

/// `finding` as the section shows it, by the id it was `told_as` in the sage's record of its review, with the last
/// `verdict` on it and what was `shown` of it once applied; `again` when the dream of the packet offers it again.
fn finding_entry(
    (finding, told_as): (&Finding, &str),
    verdict: Option<&Verdict>,
    shown: Shown<'_>,
    again: bool,
) -> String {
    let follows = finding.advice.as_ref().map_or_default(|advice| {
        advice.follows_marks(|follows| follows.finding.is_some().then_some(follows.named.as_str()))
    });
    let mut entry = format!("- {told_as} ({}{follows}): {}\n", kind(finding), finding.text);
    if let Some(advice) = &finding.advice {
        let _ = writeln!(entry, "  Suggested action: {}", advice.action);
    }
    match verdict {
        Some(verdict) => {
            let _ = write!(entry, "  Verdict: {}: {}", verdict.verdict, verdict.reason);
        }
        None => entry.push_str("  Verdict: unanswered"),
    }
    if again && finding.id == told_as {
        entry.push_str(" (offered again below)");
    } else if again {
        let _ = write!(entry, " (offered again below as {})", finding.id);
    }
    entry.push('\n');
    if !shown.written.is_empty() {
        let lines: Vec<_> = shown.written.iter().map(|(line, _)| line.as_str()).collect();
        let _ = writeln!(entry, "  Written: {}", lines.join("; "));
    }
    entry.push_str(shown.saved.unwrap_or_default());
    entry.push_str(shown.signs.unwrap_or_default());
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
