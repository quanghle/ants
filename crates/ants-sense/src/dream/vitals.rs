//! The vital signs of an ant's work over a dream's period ([`Signs`]): the daemon measures them on each thread the dream
//! covers as it begins ([`Start::vitals`]) and the dream keeps their sum ([`Dream::vitals`]), so sense holds the trend
//! for as long as its log lives, with nothing written between dreams. The packet shows them beside those of the dreams
//! before ([`trend`]), with a line per thread ([`section`]), the memories view and growth report each sign's change
//! against their mean ([`changes`]), and a sign turning sharply for the worse is a finding
//! ([`gather`]): [`CANCELLED_FINDING`] prompts cancelled by a human, [`CAPPED_FINDING`] cut by a cap,
//! [`REJECTED_FINDING`] tool requests rejected, or a share of failed tool calls at least [`FAILED_SHARE_FACTOR`] times
//! the mean of the dreams before, and at least [`FAILED_SHARE_FLOOR`], over [`FAILED_MIN_CALLS`] calls or more.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::Range;

use ants_proto::ThreadId;
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use super::{Dream, Start};
use super::{FindingKind, Gathered, get};
use crate::store::{Items, StoreError};

/// The prompts cancelled by a human in a dream's period that make a finding.
pub const CANCELLED_FINDING: u32 = 3;
/// The prompts cut by a cap in a dream's period that make a finding.
pub const CAPPED_FINDING: u32 = 2;
/// The tool requests rejected, by a human or a chain, in a dream's period that make a finding.
pub const REJECTED_FINDING: u32 = 3;
/// How many times the mean share of failed tool calls of the dreams before a period's share makes a finding.
pub const FAILED_SHARE_FACTOR: f64 = 2.0;
/// The least share of failed tool calls in a dream's period that makes a finding, however low the mean of the dreams
/// before: after dreams with no failed call, the mean is zero, and any failure would be twice it. A first guess, to
/// tune on the signs of real ants.
pub const FAILED_SHARE_FLOOR: f64 = 0.1;
/// The fewest tool calls in a dream's period whose failed share makes a finding.
pub const FAILED_MIN_CALLS: u32 = 20;
/// The most dreams before whose signs a packet shows and a period's failed share is compared with.
pub const TREND: u64 = 5;

/// The vital signs of an ant's work, on a thread or over a dream's period: what the daemon measured of it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Signs {
    /// The threads worked: 1 for a thread's own.
    #[serde(skip_serializing_if = "is_zero")]
    pub threads: u32,
    /// The threads marked done.
    #[serde(skip_serializing_if = "is_zero")]
    pub done: u32,
    /// The prompts, by how they ended.
    #[serde(skip_serializing_if = "Ends::is_empty")]
    pub prompts: Ends,
    /// The tool calls, by ACP kind: `other` for those without one.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub calls: BTreeMap<String, Tally>,
    /// The time prompts took, in milliseconds.
    #[serde(skip_serializing_if = "is_zero")]
    pub prompt_ms: u64,
    /// The tokens the harness reported.
    #[serde(skip_serializing_if = "is_zero")]
    pub tokens: u64,
    /// The cost the harness reported, in millionths of its unit (US dollars for Claude).
    #[serde(skip_serializing_if = "is_zero")]
    pub cost_micros: u64,
    /// The tool requests asked of humans.
    #[serde(skip_serializing_if = "is_zero")]
    pub asked: u32,
    /// The tool requests rejected by a human or a chain.
    #[serde(skip_serializing_if = "is_zero")]
    pub rejected: u32,
    /// The tool requests that expired unanswered.
    #[serde(skip_serializing_if = "is_zero")]
    pub expired: u32,
    /// The escalations sent but tool requests and questions to a human.
    #[serde(skip_serializing_if = "is_zero")]
    pub escalations: u32,
    /// The prompts run in a session rebuilt, as the last was lost or out of room.
    #[serde(skip_serializing_if = "is_zero")]
    pub rebuilt: u32,
    /// The calls of sense's own tools.
    #[serde(skip_serializing_if = "is_zero")]
    pub sense_calls: u32,
}

/// Prompts by how they ended.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Ends {
    #[serde(skip_serializing_if = "is_zero")]
    pub replied: u32,
    #[serde(skip_serializing_if = "is_zero")]
    pub no_reply: u32,
    /// Cancelled by a human.
    #[serde(skip_serializing_if = "is_zero")]
    pub cancelled: u32,
    /// Cut by a prompt limit or a spending cap.
    #[serde(skip_serializing_if = "is_zero")]
    pub capped: u32,
    #[serde(skip_serializing_if = "is_zero")]
    pub failed: u32,
}

/// Tool calls, and how many of them failed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Tally {
    pub calls: u32,
    #[serde(skip_serializing_if = "is_zero")]
    pub failed: u32,
}

fn is_zero<T: Default + PartialEq>(n: &T) -> bool {
    *n == T::default()
}

impl Ends {
    pub fn total(&self) -> u32 {
        self.replied + self.no_reply + self.cancelled + self.capped + self.failed
    }

    fn is_empty(&self) -> bool {
        self.total() == 0
    }

    fn add(&mut self, other: &Self) {
        self.replied += other.replied;
        self.no_reply += other.no_reply;
        self.cancelled += other.cancelled;
        self.capped += other.capped;
        self.failed += other.failed;
    }
}

impl Tally {
    fn add(&mut self, other: &Self) {
        self.calls += other.calls;
        self.failed += other.failed;
    }

    /// The share of the calls that failed; none without calls.
    pub(super) fn failed_share(&self) -> Option<f64> {
        (self.calls > 0).then(|| f64::from(self.failed) / f64::from(self.calls))
    }
}

impl Signs {
    /// The sum of `signs`.
    pub fn sum<'a>(signs: impl IntoIterator<Item = &'a Self>) -> Self {
        let mut sum = Self::default();
        for signs in signs {
            sum.add(signs);
        }
        sum
    }

    /// Add `other` to these.
    pub fn add(&mut self, other: &Self) {
        self.threads += other.threads;
        self.done += other.done;
        self.prompts.add(&other.prompts);
        for (kind, tally) in &other.calls {
            self.calls.entry(kind.clone()).or_default().add(tally);
        }
        self.prompt_ms += other.prompt_ms;
        self.tokens += other.tokens;
        self.cost_micros += other.cost_micros;
        self.asked += other.asked;
        self.rejected += other.rejected;
        self.expired += other.expired;
        self.escalations += other.escalations;
        self.rebuilt += other.rebuilt;
        self.sense_calls += other.sense_calls;
    }

    /// These signs on one line, as a packet shows a thread's.
    pub fn line(&self) -> String {
        line(self)
    }

    /// The tool calls of every kind.
    pub fn all_calls(&self) -> Tally {
        let mut all = Tally::default();
        for tally in self.calls.values() {
            all.add(tally);
        }
        all
    }
}

/// The vital signs of the dreams before dream `number` that kept some, at most [`TREND`], oldest first, each with its
/// number.
pub async fn trend(items: &dyn Items, number: u64) -> Result<Vec<(u64, Signs)>, StoreError> {
    let mut trend = Vec::new();
    for earlier in before(number) {
        if let Some((dream, _)) = get(items, earlier).await?
            && let Some(vitals) = dream.vitals
        {
            trend.push((earlier, vitals));
        }
    }
    Ok(trend)
}

/// The numbers of the dreams before dream `number` whose vital signs make its trend: the last [`TREND`].
pub fn before(number: u64) -> Range<u64> {
    number.saturating_sub(TREND).max(1)..number
}

/// The findings on the vital signs of `threads`, the threads a dream covers, against the `trend` of the dreams before:
/// each about a sign turned sharply for the worse, naming the threads it counts.
pub(super) fn gather(threads: &[(ThreadId, Signs)], trend: &[(u64, Signs)]) -> Vec<Gathered> {
    let period = Signs::sum(threads.iter().map(|(_, signs)| signs));
    let named = |count: fn(&Signs) -> u32| {
        let named: Vec<_> = threads
            .iter()
            .filter_map(|(thread, signs)| Some((thread, count(signs))).filter(|(_, n)| *n > 0))
            .map(|(thread, n)| format!("{n} on thread {thread}"))
            .collect();
        named.join(", ")
    };
    let finding = |about: &str, text: String| Gathered {
        kind: FindingKind::Vitals,
        about: about.into(),
        items: Vec::new(),
        count: None,
        text,
        untrusted: false,
    };
    let mut found = Vec::new();
    let cancelled = period.prompts.cancelled;
    if cancelled >= CANCELLED_FINDING {
        let text = format!(
            "{cancelled} prompts were cancelled by a human ({}): find what made a human stop the work and remember \
             the lesson with sense_remember.",
            named(|signs| signs.prompts.cancelled)
        );
        found.push(finding("cancelled", text));
    }
    let capped = period.prompts.capped;
    if capped >= CAPPED_FINDING {
        let text = format!(
            "{capped} prompts were cut by a cap ({}): find what spent the turns, time or cost, and remember how to \
             work within the caps with sense_remember.",
            named(|signs| signs.prompts.capped)
        );
        found.push(finding("capped", text));
    }
    let rejected = period.rejected;
    if rejected >= REJECTED_FINDING {
        let text = format!(
            "{rejected} tool requests were rejected ({}): find what the requests had in common and remember what \
             to do instead with sense_remember.",
            named(|signs| signs.rejected)
        );
        found.push(finding("rejected", text));
    }
    let calls = period.all_calls();
    let shares: Vec<_> = trend.iter().filter_map(|(_, signs)| signs.all_calls().failed_share()).collect();
    if calls.calls >= FAILED_MIN_CALLS
        && calls.failed > 0
        && let Some(share) = calls.failed_share()
        && !shares.is_empty()
    {
        let mean = shares.iter().sum::<f64>() / shares.len() as f64;
        if share >= FAILED_SHARE_FLOOR && share >= FAILED_SHARE_FACTOR * mean {
            let text = format!(
                "{} of {} tool calls failed, {}, against {} in the {} dreams before ({}): find what the failed calls \
                 had in common and remember how to avoid it with sense_remember.",
                calls.failed,
                calls.calls,
                percent(share),
                percent(mean),
                shares.len(),
                named(|signs| signs.all_calls().failed)
            );
            found.push(finding("failed_calls", text));
        }
    }
    found
}

/// The vital-signs section of the packet of dream `number`, of `signs`, appended to `text`: beside the `trend` of the
/// dreams before, then the tool calls by kind, then, with `per_thread`, a line for each of `threads`.
pub(super) fn section<'a>(
    text: &mut String,
    number: u64,
    signs: &Signs,
    trend: &[(u64, Signs)],
    threads: impl IntoIterator<Item = (&'a ThreadId, &'a Signs)>,
    per_thread: bool,
) {
    text.push_str("\n## Vital signs\n\n");
    let columns: Vec<_> = trend.iter().map(|(number, signs)| (*number, signs)).chain([(number, signs)]).collect();
    text.push_str("| |");
    for (number, _) in &columns {
        let _ = write!(text, " dream {number} |");
    }
    text.push_str("\n|---|");
    text.push_str(&"---:|".repeat(columns.len()));
    text.push('\n');
    for row in ROWS {
        let cells: Vec<_> = columns.iter().map(|(_, signs)| row.cell(signs)).collect();
        if cells.iter().all(String::is_empty) {
            continue;
        }
        let _ = write!(text, "| {} |", row.label);
        for cell in &cells {
            let _ = write!(text, " {} |", if cell.is_empty() { "0" } else { cell });
        }
        text.push('\n');
    }
    if !signs.calls.is_empty() {
        let kinds: Vec<_> = signs.calls.iter().map(|(kind, tally)| format!("{kind} {}", tally_text(tally))).collect();
        let _ = writeln!(text, "\nTool calls by kind: {}.", kinds.join(", "));
    }
    if !per_thread {
        return;
    }
    let mut first = true;
    for (thread, signs) in threads {
        if first {
            text.push_str("\nBy thread:\n");
            first = false;
        }
        let _ = writeln!(text, "- {thread}: {}.", line(signs));
    }
}

/// A row of the vital-signs table: a sign's label, its measure, none when the signs give it none, and how it shows.
struct Row {
    label: &'static str,
    measure: fn(&Signs) -> Option<f64>,
    unit: Unit,
}

/// How a sign shows.
#[derive(Clone, Copy)]
enum Unit {
    Count,
    /// Milliseconds, as a duration.
    Millis,
    /// Millionths, as a cost.
    Micros,
    /// The share of the tool calls that failed.
    Failed,
}

/// The rows of the vital-signs table.
const ROWS: &[Row] = &[
    Row { label: "threads worked", measure: |signs| Some(signs.threads.into()), unit: Unit::Count },
    Row { label: "threads marked done", measure: |signs| Some(signs.done.into()), unit: Unit::Count },
    Row { label: "prompts replied", measure: |signs| Some(signs.prompts.replied.into()), unit: Unit::Count },
    Row { label: "prompts with no reply", measure: |signs| Some(signs.prompts.no_reply.into()), unit: Unit::Count },
    Row {
        label: "prompts cancelled by a human",
        measure: |signs| Some(signs.prompts.cancelled.into()),
        unit: Unit::Count,
    },
    Row { label: "prompts cut by a cap", measure: |signs| Some(signs.prompts.capped.into()), unit: Unit::Count },
    Row { label: "prompts failed", measure: |signs| Some(signs.prompts.failed.into()), unit: Unit::Count },
    Row { label: "time in prompts", measure: |signs| Some(signs.prompt_ms as f64), unit: Unit::Millis },
    Row { label: "tool calls", measure: |signs| Some(signs.all_calls().calls.into()), unit: Unit::Count },
    Row { label: "tool calls failed", measure: |signs| signs.all_calls().failed_share(), unit: Unit::Failed },
    Row { label: "tokens", measure: |signs| Some(signs.tokens as f64), unit: Unit::Count },
    Row { label: "cost", measure: |signs| Some(signs.cost_micros as f64), unit: Unit::Micros },
    Row { label: "tool requests asked of humans", measure: |signs| Some(signs.asked.into()), unit: Unit::Count },
    Row { label: "tool requests rejected", measure: |signs| Some(signs.rejected.into()), unit: Unit::Count },
    Row { label: "tool requests expired", measure: |signs| Some(signs.expired.into()), unit: Unit::Count },
    Row { label: "escalations", measure: |signs| Some(signs.escalations.into()), unit: Unit::Count },
    Row { label: "sessions rebuilt", measure: |signs| Some(signs.rebuilt.into()), unit: Unit::Count },
    Row { label: "sense tool calls", measure: |signs| Some(signs.sense_calls.into()), unit: Unit::Count },
];

impl Row {
    /// The cell of `signs` in the table: empty when nothing counts.
    fn cell(&self, signs: &Signs) -> String {
        if let Unit::Failed = self.unit {
            let all = signs.all_calls();
            return all
                .failed_share()
                .filter(|_| all.failed > 0)
                .map_or_default(|share| format!("{} ({})", all.failed, percent(share)));
        }
        (self.measure)(signs).filter(|value| *value > 0.0).map_or_default(|value| self.show(value))
    }

    fn show(&self, value: f64) -> String {
        match self.unit {
            Unit::Count if value.fract() == 0.0 => format!("{value:.0}"),
            Unit::Count => format!("{value:.1}"),
            Unit::Millis => duration(value.round() as u64),
            Unit::Micros => cost(value.round() as u64),
            Unit::Failed => percent(value),
        }
    }

    /// The change of `now` against `mean`: in points for a share, else in percent of the mean; none from a mean of
    /// zero.
    fn change(&self, now: f64, mean: f64) -> Option<String> {
        // Adding zero turns a rounded -0 into 0, which shows as +0.
        match self.unit {
            Unit::Failed => Some(format!("{:+} points", ((now - mean) * 100.0).round() + 0.0)),
            _ if mean == 0.0 => None,
            _ => Some(format!("{:+}%", ((now - mean) / mean * 100.0).round() + 0.0)),
        }
    }
}

/// Each of `signs` that counts, now or in the `trend` of the dreams before, with its change against its mean there,
/// taken over the dreams that measure it: "the mean of the 2 dreams before: tool calls 30 (mean 20, +50%); …". None
/// without a dream before or a sign that counts.
pub fn changes(signs: &Signs, trend: &[(u64, Signs)]) -> Option<String> {
    if trend.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for row in ROWS {
        let Some(now) = (row.measure)(signs) else { continue };
        let before: Vec<_> = trend.iter().filter_map(|(_, signs)| (row.measure)(signs)).collect();
        let mean = (!before.is_empty()).then(|| before.iter().sum::<f64>() / before.len() as f64);
        if now == 0.0 && mean.is_none_or(|mean| mean == 0.0) {
            continue;
        }
        let shown = match mean {
            None => row.show(now),
            Some(mean) => match row.change(now, mean) {
                Some(change) => format!("{} (mean {}, {change})", row.show(now), row.show(mean)),
                None => format!("{} (mean {})", row.show(now), row.show(mean)),
            },
        };
        parts.push(format!("{} {shown}", row.label));
    }
    if parts.is_empty() {
        return None;
    }
    let before =
        if trend.len() == 1 { "the dream before".to_owned() } else { format!("the {} dreams before", trend.len()) };
    Some(format!("the mean of {before}: {}", parts.join("; ")))
}

/// Signs on one line, a thread's or a period's: what counts of them.
fn line(signs: &Signs) -> String {
    let mut parts = Vec::new();
    if signs.threads > 1 {
        parts.push(counted(signs.threads, "thread worked", "threads worked"));
    }
    let ends = &signs.prompts;
    if ends.total() > 0 {
        let how = [
            (ends.replied, "replied"),
            (ends.no_reply, "with no reply"),
            (ends.cancelled, "cancelled by a human"),
            (ends.capped, "cut by a cap"),
            (ends.failed, "failed"),
        ];
        let how: Vec<_> = how.iter().filter(|(n, _)| *n > 0).map(|(n, how)| format!("{n} {how}")).collect();
        parts.push(format!("{} ({})", counted(ends.total(), "prompt", "prompts"), how.join(", ")));
    }
    let calls = signs.all_calls();
    if calls.calls > 0 {
        parts.push(format!("{}{}", counted(calls.calls, "tool call", "tool calls"), failed_text(calls.failed)));
    }
    if signs.prompt_ms > 0 {
        parts.push(format!("{} in prompts", duration(signs.prompt_ms)));
    }
    if signs.tokens > 0 {
        parts.push(format!("{} tokens", signs.tokens));
    }
    if signs.cost_micros > 0 {
        parts.push(format!("cost {}", cost(signs.cost_micros)));
    }
    let counts = [
        (signs.asked, "tool request asked", "tool requests asked"),
        (signs.rejected, "rejected", "rejected"),
        (signs.expired, "expired", "expired"),
        (signs.escalations, "escalation", "escalations"),
        (signs.rebuilt, "session rebuilt", "sessions rebuilt"),
        (signs.sense_calls, "sense tool call", "sense tool calls"),
    ];
    parts.extend(counts.iter().filter(|(n, ..)| *n > 0).map(|(n, one, many)| counted(*n, one, many)));
    if signs.threads > 1 {
        parts.extend((signs.done > 0).then(|| counted(signs.done, "marked done", "marked done")));
    } else if signs.done > 0 {
        parts.push("marked done".into());
    }
    if parts.is_empty() {
        return "no prompt measured".into();
    }
    parts.join(", ")
}

/// `n` of what is `one` or `many`.
fn counted(n: u32, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

fn tally_text(tally: &Tally) -> String {
    format!("{}{}", tally.calls, failed_text(tally.failed))
}

fn failed_text(failed: u32) -> String {
    if failed == 0 { String::new() } else { format!(" ({failed} failed)") }
}

pub(super) fn percent(share: f64) -> String {
    format!("{:.0}%", share * 100.0)
}

pub(super) fn cost(micros: u64) -> String {
    format!("{:.2}", micros as f64 / 1e6)
}

/// `ms` milliseconds as hours and minutes, minutes and seconds, or seconds.
fn duration(ms: u64) -> String {
    let seconds = ms / 1000;
    match (seconds / 3600, seconds / 60 % 60, seconds % 60) {
        (0, 0, s) => format!("{s}s"),
        (0, m, s) => format!("{m}m {s:02}s"),
        (h, m, _) => format!("{h}h {m:02}m"),
    }
}
