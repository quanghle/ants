//! The growth report of sense's memories view ([`super`]): how the ant's identity grew, counted from its sense data at
//! each read and writing nothing, to judge the numbers the dreams' identity findings and the brief go by. Counts only,
//! over the last [`WEEKS`] weeks and the [`TOP`] people and repositories of the most threads, so it stays small however
//! long the ant works:
//!
//! - its threads of work, from their records ([`crate::thread::experience::work_records`]): done, started again,
//!   expired, its active days, who started them and where, and the prompts it completed;
//! - by week: the threads started and done, the active days, the calls of sense's tools that wrote
//!   ([`crate::thread::calls`]) and the verdicts given in dreams;
//! - its memories by kind and status, with their votes and reads in full;
//! - its dreams, and their findings by kind, each by the last verdict given on it in any dream, then the vital signs
//!   of the periods of the last [`TREND`] dreams keeping them, all calls of sense's tools among them, and the change of
//!   the last one's against the mean of the dreams before ([`crate::dream::changes`]);
//! - as a sage, the tally of the reviews it made of other ants ([`crate::dream::reviews`]): their findings by what
//!   came of them, of the [`TOP`] kinds and ants of the most; none for an ant that reviewed nobody;
//! - its persona and self-model, with their revisions, the self-model's age in active days, and the proposals;
//! - the people who started its threads, by how many each, against its relationship records;
//! - its skills, with their revisions and proposals;
//! - the thresholds of the dreams' identity findings and of the brief's experience and memories.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;

use ants_proto::UserId;
use ants_proto::provider::Starter;
use chrono::{DateTime, Duration, NaiveDate, Utc};

use crate::brief::{TOP_EXPERIENCE, TOP_MEMORIES};
use crate::dream::{
    self as dream, Dream, FindingKind, Judgement, OWN_AFTER_THREADS, PERSONA_AFTER_THREADS, RELATIONSHIP_AFTER_THREADS,
    ReviewTally, SELF_MODEL_AFTER_THREADS, STALE_ACTIVE_DAYS, State, TREND, review_tally,
};
use crate::fork::INHERITED;
use crate::identity::{Card, card};
use crate::items::{Item, ItemValues, readable};
use crate::memory::{MEMORY, Memory, MemoryKind, Status, Trust};
use crate::persona::{self, PERSONA};
use crate::proposal::{self, Confirmation, PROPOSAL, Proposal, ProposalItem, time};
use crate::records::{self, RELATIONSHIP, RecordItem};
use crate::skill::{SKILL, Skill};
use crate::store::{Items, StoreError};
use crate::thread::calls::{self, Calls, week};
use crate::thread::experience::{Experience, experience, experience_since, work_records};
use crate::thread::record::Record;

/// The key of the report's entry.
pub const KEY: &str = "growth report";
/// What the report reads for an ant sense keeps no data of.
pub const NO_DATA: &str = "no sense data";

/// The weeks shown, the current one last.
const WEEKS: i64 = 8;
/// The most people and repositories named.
const TOP: usize = 10;
/// The upper bounds of the buckets of people by the threads they started; the last bucket has none.
const BUCKETS: [u32; 5] = [1, 2, 4, 8, 16];
const STATUSES: [Status; 5] = [Status::Active, Status::NeedsReview, Status::Superseded, Status::Wrong, Status::Retired];
const PROPOSALS: [proposal::Status; 4] =
    [proposal::Status::Pending, proposal::Status::Confirmed, proposal::Status::Rejected, proposal::Status::Superseded];

/// What the report counts.
struct Data {
    card: Option<Card>,
    records: Vec<Record>,
    worked: Option<Experience>,
    /// The ant's names, the earlier ones too.
    own: Vec<UserId>,
    calls: Vec<Calls>,
    memories: Vec<Memory>,
    dreams: Vec<Dream>,
    rested: usize,
    /// The tally of the reviews it made as a sage.
    reviews: Option<ReviewTally>,
    persona: Option<Item>,
    proposals: Vec<Proposal>,
    self_model: Option<RecordItem>,
    relationships: Vec<RecordItem>,
    /// The confirmed skills, and whether each is inherited.
    skills: Vec<(Skill, bool)>,
}

impl Data {
    async fn read(items: &dyn Items, ant: &UserId, since: DateTime<Utc>) -> Result<Self, StoreError> {
        let mut own = items.earlier_names().await;
        own.push(ant.clone());
        let proposals = items.list(PROPOSAL, usize::MAX).await?.into_iter().filter_map(ProposalItem::of);
        let skills = items.list(SKILL, usize::MAX).await?;
        Ok(Self {
            card: card(items).await?,
            records: work_records(items).await?,
            worked: experience(items, ant, usize::MAX).await?,
            own,
            calls: calls::calls(items, since).await?,
            memories: readable(items.list(MEMORY, usize::MAX).await?, Memory::of)
                .into_iter()
                .map(|(_, memory)| memory)
                .collect(),
            dreams: dream::all(items).await?,
            rested: dream::rested_count(items).await?,
            reviews: review_tally(items, TOP).await?,
            persona: persona::current(items).await?,
            proposals: proposals.map(|found| found.proposal).collect(),
            self_model: records::self_model(items).await?,
            relationships: items.list(RELATIONSHIP, usize::MAX).await?.into_iter().filter_map(RecordItem::of).collect(),
            skills: skills
                .iter()
                .filter_map(|item| Some((Skill::of(item)?, item.values.contains_key(INHERITED))))
                .collect(),
        })
    }

    fn empty(&self) -> bool {
        self.card.is_none()
            && self.records.is_empty()
            && self.calls.is_empty()
            && self.memories.is_empty()
            && self.dreams.is_empty()
            && self.reviews.is_none()
            && self.persona.is_none()
            && self.proposals.is_empty()
            && self.self_model.is_none()
            && self.relationships.is_empty()
            && self.skills.is_empty()
    }
}

/// The growth report of `ant` in `items` at `now`; none when sense keeps nothing it counts.
pub async fn report(items: &dyn Items, ant: &UserId, now: DateTime<Utc>) -> Result<Option<String>, StoreError> {
    let since = first_week(now);
    let data = Data::read(items, ant, since).await?;
    if data.empty() {
        return Ok(None);
    }
    let mut text = String::new();
    threads(&mut text, &data);
    weeks(&mut text, &data, since);
    memories(&mut text, &data.memories);
    dreams_lines(&mut text, &data);
    reviews_lines(&mut text, data.reviews.as_ref());
    persona_line(&mut text, &data);
    self_model_line(&mut text, items, ant, data.self_model.as_ref()).await?;
    people(&mut text, &data);
    skills_line(&mut text, &data);
    thresholds(&mut text);
    Ok(Some(text.trim_end().to_owned()))
}

/// The Monday starting the first of the [`WEEKS`] weeks ending with that of `now`.
fn first_week(now: DateTime<Utc>) -> DateTime<Utc> {
    let monday = week(now) - Duration::weeks(WEEKS - 1);
    monday.and_hms_opt(0, 0, 0).map_or(now, |midnight| midnight.and_utc())
}

/// `counts` named, those of none left out, joined by commas.
fn counted<'a>(counts: impl IntoIterator<Item = (&'a str, usize)>) -> String {
    let named: Vec<_> = counts.into_iter().filter(|(_, n)| *n > 0).map(|(name, n)| format!("{n} {name}")).collect();
    named.join(", ")
}

fn day(at: DateTime<Utc>) -> NaiveDate {
    at.date_naive()
}

fn threads(text: &mut String, data: &Data) {
    let records = &data.records;
    if let Some(worked) = &data.worked {
        let again = records.iter().filter(|record| record.restarted.is_some()).count();
        let expired = records.iter().filter(|record| record.expired.is_some()).count();
        let first = records.iter().map(|record| record.started).min();
        let last = records.iter().flat_map(|r| [Some(r.started), r.restarted, r.done]).flatten().max();
        let _ = write!(
            text,
            "Threads: {} in {} repositories, {} done, {again} started again, {expired} expired; {} active days",
            worked.threads,
            worked.repos.len(),
            worked.done,
            worked.active_days
        );
        if let (Some(first), Some(last)) = (first, last) {
            let _ = write!(text, ", {} to {}", day(first), day(last));
        }
        text.push_str(".\n");
        let started = |of: &dyn Fn(&Starter) -> bool| records.iter().filter(|record| of(&record.origin.by)).count();
        let own = |by: &Starter| matches!(by, Starter::Ant { user } if data.own.contains(user));
        let by = counted([
            ("by humans", started(&|by| matches!(by, Starter::Human { .. }))),
            ("by other ants", started(&|by| matches!(by, Starter::Ant { .. }) && !own(by))),
            ("by itself", started(&own)),
            ("by webhooks", started(&|by| matches!(by, Starter::Webhook { .. }))),
            ("by providers", started(&|by| matches!(by, Starter::Provider { .. }))),
        ]);
        let _ = writeln!(text, "  Started {by}.");
        if !worked.repos.is_empty() {
            let repos: Vec<_> =
                worked.repos.iter().take(TOP).map(|tally| format!("{} {}", tally.scope, tally.threads)).collect();
            let _ = writeln!(text, "  Threads by repository: {}{}.", repos.join(", "), more(worked.repos.len()));
        }
    } else {
        text.push_str("Threads: none recorded.\n");
    }
    if let Some(growth) = data.card.as_ref().and_then(|card| card.growth) {
        let _ = writeln!(text, "  Prompts completed: {} since {}.", growth.prompts, day(growth.born));
    }
}

/// How many of `count` are not named, past the first [`TOP`].
fn more(count: usize) -> String {
    let more = count.saturating_sub(TOP);
    if more > 0 { format!(", {more} more") } else { String::new() }
}

/// One week of the report.
#[derive(Default)]
struct Week {
    started: usize,
    done: usize,
    days: BTreeSet<NaiveDate>,
    calls: Vec<(&'static str, u32)>,
    verdicts: usize,
}

/// The week of `at` in `weeks`, its day counted active; none before `since`.
fn active(weeks: &mut BTreeMap<NaiveDate, Week>, at: DateTime<Utc>, since: DateTime<Utc>) -> Option<&mut Week> {
    (at >= since).then(|| {
        let shown = weeks.entry(week(at)).or_default();
        shown.days.insert(day(at));
        shown
    })
}

fn weeks(text: &mut String, data: &Data, since: DateTime<Utc>) {
    let mut weeks: BTreeMap<NaiveDate, Week> = BTreeMap::new();
    for record in &data.records {
        if let Some(shown) = active(&mut weeks, record.started, since) {
            shown.started += 1;
        }
        if let Some(restarted) = record.restarted {
            active(&mut weeks, restarted, since);
        }
        if let Some(shown) = record.done.and_then(|done| active(&mut weeks, done, since)) {
            shown.done += 1;
        }
    }
    for calls in &data.calls {
        weeks.entry(calls.week).or_default().calls.push((calls.tool, calls.calls));
    }
    for verdict in data.dreams.iter().flat_map(|dream| &dream.verdicts).filter(|verdict| verdict.at >= since) {
        weeks.entry(week(verdict.at)).or_default().verdicts += 1;
    }
    let _ = writeln!(
        text,
        "By week from Monday (UTC), the last {WEEKS}: threads started and done, active days, the calls of sense's tools \
         that wrote, the verdicts given in dreams."
    );
    if weeks.is_empty() {
        text.push_str("  None active.\n");
    }
    for (monday, week) in &weeks {
        let mut parts = Vec::new();
        let threads = counted([("started", week.started), ("done", week.done), ("active days", week.days.len())]);
        if !threads.is_empty() {
            parts.push(format!("threads {threads}"));
        }
        if !week.calls.is_empty() {
            let calls: Vec<_> = week.calls.iter().map(|(tool, n)| format!("{tool} {n}")).collect();
            parts.push(format!("calls {}", calls.join(", ")));
        }
        if week.verdicts > 0 {
            parts.push(format!("verdicts {}", week.verdicts));
        }
        let _ = writeln!(text, "  {monday}: {}.", parts.join("; "));
    }
    text.push_str(
        "  Calls not kept: sense_brief, searches and other reads write nothing, nor does a call changing nothing; \
         sense_recall reading a memory in full counts once a day a memory, sense_dream only as its verdicts; the vital \
         signs of dreams count all calls of sense's tools, not by tool.\n",
    );
}

fn memories(text: &mut String, memories: &[Memory]) {
    if memories.is_empty() {
        text.push_str("Memories: none.\n");
        return;
    }
    let count = |of: &dyn Fn(&Memory) -> bool| memories.iter().filter(|memory| of(memory)).count();
    let _ = write!(text, "Memories: {}", memories.len());
    let kinds = counted([
        ("core", count(&|memory| memory.core)),
        ("untrusted", count(&|memory| memory.trust == Trust::Untrusted)),
        ("inherited", count(&|memory| memory.inherited.is_some())),
    ]);
    if !kinds.is_empty() {
        let _ = write!(text, ", {kinds}");
    }
    let reads: u64 = memories.iter().filter_map(|memory| memory.access).map(|access| u64::from(access.days)).sum();
    let _ = writeln!(text, "; {}; {reads} reads in full, once a day a memory.", votes(memories.iter()));
    for kind in MemoryKind::ALL {
        let of: Vec<_> = memories.iter().filter(|memory| memory.kind == kind).collect();
        if of.is_empty() {
            continue;
        }
        let statuses =
            counted(STATUSES.map(|status| (status.name(), of.iter().filter(|memory| memory.status == status).count())));
        let _ = writeln!(text, "  {kind}: {} ({statuses}); {}.", of.len(), votes(of.iter().copied()));
    }
}

/// The votes counted on `memories`, in all.
fn votes<'a>(memories: impl Iterator<Item = &'a Memory>) -> String {
    let (mut helpful, mut stale, mut wrong) = (0, 0, 0);
    for memory in memories {
        helpful += memory.votes.helpful;
        stale += memory.votes.stale;
        wrong += memory.votes.wrong;
    }
    format!("votes {helpful} helpful, {stale} stale, {wrong} wrong")
}

fn dreams_lines(text: &mut String, data: &Data) {
    let dreams = &data.dreams;
    if dreams.is_empty() {
        text.push_str("Dreams: none.\n");
        return;
    }
    let states = [State::Sent, State::Answered, State::Fallback, State::Done]
        .map(|state| (state, dreams.iter().filter(|dream| dream.state == state).count()));
    let states: Vec<_> = states.iter().filter(|(_, n)| *n > 0).map(|(state, n)| format!("{n} {state}")).collect();
    let sage = dreams.iter().filter(|dream| dream.sage.is_some()).count();
    let writes: usize = dreams.iter().map(|dream| dream.writes.len()).sum();
    let _ = writeln!(
        text,
        "Dreams: {} ({}), {sage} sent to a sage; {writes} writes on dream threads.",
        dreams.len(),
        states.join(", ")
    );
    let mut judged: BTreeMap<&str, (FindingKind, Option<Judgement>)> = BTreeMap::new();
    for dream in dreams {
        for finding in &dream.findings {
            let last = judged.entry(&finding.id).or_insert((finding.kind, None));
            last.0 = finding.kind;
            if let Some(verdict) = dream.verdict(&finding.id) {
                last.1 = Some(verdict.verdict);
            }
        }
    }
    let mut kinds: BTreeMap<FindingKind, [usize; 4]> = BTreeMap::new();
    for (kind, verdict) in judged.values() {
        let at = match verdict {
            Some(Judgement::Applied) => 0,
            Some(Judgement::Rejected) => 1,
            Some(Judgement::Deferred) => 2,
            None => 3,
        };
        kinds.entry(*kind).or_default()[at] += 1;
    }
    let judgements = |counts: &[usize; 4]| {
        counted([("applied", counts[0]), ("rejected", counts[1]), ("deferred", counts[2]), ("unanswered", counts[3])])
    };
    let mut all = [0; 4];
    for counts in kinds.values() {
        for (total, n) in all.iter_mut().zip(counts) {
            *total += n;
        }
    }
    let _ = write!(text, "  Findings: {}", judged.len());
    if !judged.is_empty() {
        let _ = write!(text, " ({})", judgements(&all));
    }
    let _ = writeln!(text, ", each by its last verdict; {} rejected ones resting.", data.rested);
    for (kind, counts) in &kinds {
        let _ = writeln!(text, "  {kind}: {} ({}).", counts.iter().sum::<usize>(), judgements(counts));
    }
    let kept: Vec<_> = dreams.iter().filter_map(|dream| Some((dream, dream.vitals.as_ref()?))).collect();
    if kept.is_empty() {
        return;
    }
    let _ = writeln!(text, "  Vital signs of their periods, the last {TREND} dreams keeping them:");
    let shown = usize::try_from(TREND).unwrap_or(usize::MAX);
    for (dream, signs) in &kept[kept.len().saturating_sub(shown)..] {
        let _ = writeln!(text, "  dream {}, began {}: {}.", dream.number, day(dream.at), signs.line());
    }
    let Some((last, signs)) = kept.last() else { return };
    let before = crate::dream::before(last.number);
    let trend: Vec<_> = (kept.iter())
        .filter(|(dream, _)| before.contains(&dream.number))
        .map(|(dream, signs)| (dream.number, (*signs).clone()))
        .collect();
    if let Some(changes) = crate::dream::changes(signs, &trend) {
        let _ = writeln!(text, "  dream {} against {changes}.", last.number);
    }
}

/// The tally of the reviews the ant made as a sage, by kind and by ant; nothing when it made none.
fn reviews_lines(text: &mut String, tally: Option<&ReviewTally>) {
    let Some(tally) = tally else { return };
    let _ = writeln!(
        text,
        "Reviews as a sage: {} of {} ants, the last {}; findings {}.",
        tally.reviews,
        tally.ants,
        day(tally.last),
        tally.findings.counts()
    );
    if let Some(cost) = &tally.cost {
        let _ = writeln!(text, "  Spend: {}.", cost.line());
    }
    for kind in &tally.by_kind {
        let _ = writeln!(text, "  {} findings: {}.", kind.kind, kind.findings.counts());
    }
    let shown: u32 = tally.by_kind.iter().map(|kind| kind.findings.made).sum();
    if tally.findings.made > shown {
        let _ = writeln!(text, "  {} findings of other kinds.", tally.findings.made - shown);
    }
    for ant in &tally.by_ant {
        let _ = writeln!(
            text,
            "  {}: {} reviews, the last {}; findings {}{}.",
            ant.ant,
            ant.reviews,
            day(ant.last),
            ant.findings.counts(),
            ant.cost.map_or_default(|cost| format!("; spend {}", cost.line()))
        );
    }
    let shown = u32::try_from(tally.by_ant.len()).unwrap_or(u32::MAX);
    if tally.ants > shown {
        let _ = writeln!(text, "  {} more ants.", tally.ants - shown);
    }
}

/// The proposals of `data` of `kind`, by status.
fn proposals(data: &Data, kind: &str) -> String {
    let of: Vec<_> = data.proposals.iter().filter(|proposal| proposal.kind == kind).collect();
    if of.is_empty() {
        return "no proposals".to_owned();
    }
    let statuses =
        counted(PROPOSALS.map(|status| (status.name(), of.iter().filter(|found| found.status == status).count())));
    format!("{} proposals ({statuses})", of.len())
}

fn persona_line(text: &mut String, data: &Data) {
    let proposed = proposals(data, PERSONA);
    let Some(item) = &data.persona else {
        let _ = writeln!(text, "Persona: none; {proposed}.");
        return;
    };
    let confirmed = Confirmation::of(&item.values).confirmed;
    let confirmed = confirmed.map_or_else(|| "never confirmed".to_owned(), |at| format!("confirmed {}", time(at)));
    let inherited = if item.values.contains_key(INHERITED) { ", inherited" } else { "" };
    let _ = writeln!(text, "Persona: revision {}, {confirmed}{inherited}; {proposed}.", item.revision);
}

/// The self-model, with the threads and active days since it was written: its age in active days.
async fn self_model_line(
    text: &mut String,
    items: &dyn Items,
    ant: &UserId,
    model: Option<&RecordItem>,
) -> Result<(), StoreError> {
    let Some(model) = model else {
        text.push_str("Self-model: none.\n");
        return Ok(());
    };
    let written = model.item.updated.at;
    let lately = experience_since(items, ant, written, 0).await?;
    let (threads, days) = lately.map_or((0, 0), |lately| (lately.threads, lately.active_days));
    let inherited = if model.record.inherited.is_some() { ", inherited" } else { "" };
    let _ = writeln!(
        text,
        "Self-model: revision {}, written {}{inherited}; since then {threads} threads on {days} active days.",
        model.item.revision,
        time(written)
    );
    Ok(())
}

fn people(text: &mut String, data: &Data) {
    let people = data.worked.as_ref().map_or(&[][..], |worked| &worked.people[..]);
    let records: HashMap<String, u64> =
        data.relationships.iter().map(|found| (found.record.scope.to_string(), found.item.revision)).collect();
    let scopes: Vec<_> = people.iter().map(|tally| tally.scope.to_string()).collect();
    let with = scopes.iter().filter(|scope| records.contains_key(*scope)).count();
    let often: Vec<_> =
        people.iter().zip(&scopes).filter(|(tally, _)| tally.threads >= RELATIONSHIP_AFTER_THREADS).collect();
    let often_with = often.iter().filter(|(_, scope)| records.contains_key(*scope)).count();
    let others = records.keys().filter(|scope| !scopes.contains(scope)).count();
    let _ = writeln!(
        text,
        "People: {} started its threads, {with} with a relationship record; of the {} who started \
         {RELATIONSHIP_AFTER_THREADS} or more, {often_with} with one; {others} records of others.",
        people.len(),
        often.len()
    );
    if people.is_empty() {
        return;
    }
    let mut buckets = [0; BUCKETS.len() + 1];
    for tally in people {
        buckets[BUCKETS.iter().position(|bound| tally.threads <= *bound).unwrap_or(BUCKETS.len())] += 1;
    }
    let mut by = Vec::new();
    let mut from = 1;
    for (at, n) in buckets.into_iter().enumerate() {
        let range = match BUCKETS.get(at) {
            Some(&to) if to == from => to.to_string(),
            Some(&to) => format!("{from}-{to}"),
            None => format!("{from}+"),
        };
        if n > 0 {
            by.push(format!("{n} started {range}"));
        }
        from = BUCKETS.get(at).map_or(from, |to| to + 1);
    }
    let _ = writeln!(text, "  By threads started: {}.", by.join(", "));
    for (tally, scope) in people.iter().zip(&scopes).take(TOP) {
        let record =
            records.get(scope).map_or_else(|| "no record".to_owned(), |revision| format!("record revision {revision}"));
        let _ = writeln!(text, "  {scope}: {} threads, {record}.", tally.threads);
    }
    if people.len() > TOP {
        let _ = writeln!(text, "  {} more.", people.len() - TOP);
    }
}

fn skills_line(text: &mut String, data: &Data) {
    let proposed = proposals(data, SKILL);
    if data.skills.is_empty() {
        let _ = writeln!(text, "Skills: none; {proposed}.");
        return;
    }
    let revisions: u64 = data.skills.iter().map(|(skill, _)| skill.revision).sum();
    let inherited = data.skills.iter().filter(|(_, inherited)| *inherited).count();
    let _ = writeln!(text, "Skills: {}, {revisions} revisions, {inherited} inherited; {proposed}.", data.skills.len());
}

fn thresholds(text: &mut String) {
    let _ = writeln!(
        text,
        "Thresholds: a dream finds no persona after {PERSONA_AFTER_THREADS} threads, no self-model after \
         {SELF_MODEL_AFTER_THREADS}, no relationship record of one who started {RELATIONSHIP_AFTER_THREADS}, a \
         self-model stale after {STALE_ACTIVE_DAYS} active days, an inheritance to own after {OWN_AFTER_THREADS} \
         threads; the brief names {TOP_EXPERIENCE} people and repositories and {TOP_MEMORIES} memories."
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ants_proto::provider::{CallContext, Event, EventKind, Origin};
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::Sense;
    use crate::dream::{self, ColonyTrails, Ends, Recipient, Signs, Start};
    use crate::identity::{self, Growth};
    use crate::items::Values;
    use crate::memory;
    use crate::memory::tests::id_of;
    use crate::skill::skill_id;
    use crate::store::Write;
    use crate::testing::{context, human, sense, tool};
    use crate::thread::record::record_event;
    use crate::tools::{Answer, Handler};
    use serde_json::Value;

    fn name(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn on(thread: u8) -> CallContext {
        let thread = format!("019a7d2c-0000-7000-8000-0000000000{thread:02}").parse().unwrap();
        CallContext { thread, ..context(human()) }
    }

    async fn told(items: &dyn Items, id: u128, thread: u8, at: DateTime<Utc>, kind: EventKind) {
        let thread = Some(on(thread).thread);
        let event = Event { id: Uuid::from_u128(id), ant: name("dev-a"), identity: "0f1e".into(), thread, at, kind };
        record_event(items, &event).await.unwrap().unwrap();
    }

    fn started(by: &str, repo: Option<&str>) -> EventKind {
        let by = match by.split_once(':') {
            Some(("ant", user)) => Starter::Ant { user: name(user) },
            _ => Starter::Human { user: name(by) },
        };
        EventKind::ThreadStarted { origin: Origin { by, channel: None }, repo: repo.map(Into::into) }
    }

    async fn call(sense: &Sense, handler: Handler, arguments: Value, thread: u8) -> Answer {
        tool(sense, handler, arguments, on(thread)).await
    }

    /// Five threads, the first done two weeks before `start`, the others started then; returns when the first began.
    async fn threads_told(items: &dyn Items, start: DateTime<Utc>) -> DateTime<Utc> {
        let (r, s) = (Some("github.com/o/r"), Some("github.com/o/s"));
        let earlier = (week(start) - Duration::weeks(2)).and_hms_opt(9, 0, 0).unwrap().and_utc();
        let told_threads = [
            (1, earlier, started("alice", r)),
            (1, earlier + Duration::days(1), EventKind::ThreadDone),
            (2, start, started("alice", r)),
            (3, start, started("alice", None)),
            (4, start, started("ant:dev-b", s)),
        ];
        for (id, (thread, at, kind)) in (1..).zip(told_threads) {
            told(items, id, thread, at, kind).await;
        }
        earlier
    }

    /// Three memories, one core and read in full, one voted helpful and stale.
    async fn remembered(sense: &Sense) {
        let remember = |kind: &str, title: &str, core: bool| json!({"kind": kind, "title": title, "body": format!("{title}, in a body."), "core": core});
        let core = id_of(&call(sense, memory::remember, remember("lesson", "Always rebase", true), 1).await);
        let slow = id_of(&call(sense, memory::remember, remember("lesson", "Tests run slow", false), 1).await);
        id_of(&call(sense, memory::remember, remember("episode", "Shipped the parser", false), 2).await);
        for (vote, thread) in [("helpful", 2), ("stale", 3)] {
            let voted = call(sense, memory::vote, json!({"id": slow, "vote": vote, "reason": "Seen."}), thread).await;
            assert!(!voted.is_error, "{voted:?}");
        }
        assert!(!call(sense, memory::recall, json!({"id": core}), 3).await.is_error);
    }

    /// A persona with a proposal of a new voice, a self-model, two relationship records and a skill.
    async fn identity_written(sense: &Sense, items: &dyn Items) {
        let mut persona = Values::new();
        persona.insert("voice".into(), json!("Plain."));
        persona.insert("confirmed".into(), json!("2025-01-01T12:00:00Z"));
        persona.insert("reason".into(), json!("Asked for."));
        let mut skill = Values::new();
        skill.insert("name".into(), json!("release"));
        skill.insert("description".into(), json!("Cut a release."));
        skill.insert("files".into(), json!({"SKILL.md": "---\nname: release\ndescription: Cut a release.\n---\n"}));
        let writes = [
            Write::Create { id: persona::persona_id(), kind: PERSONA.into(), values: persona },
            Write::Create { id: skill_id("release"), kind: SKILL.into(), values: skill },
        ];
        items.write(&writes, "test").await.unwrap();
        for arguments in [
            json!({"voice": "Terse.", "reason": "Shorter."}),
            json!({"self_model": "Careful.", "title": "A careful reviewer"}),
            json!({"relationship": "Wants tests first.", "who": "human:alice", "title": "Alice: tests first"}),
            json!({"relationship": "Reviews on Fridays.", "who": "human:carol", "title": "Carol: Fridays"}),
        ] {
            assert!(!call(sense, identity::identity, arguments, 4).await.is_error);
        }
    }

    /// A dream applying the persona's proposal, rejecting the contradiction, deferring the memory to review and
    /// leaving the episode to distill unanswered, then the next one offering the last two again.
    async fn dreamt(sense: &Sense, items: &dyn Items) {
        let writer = on(9);
        let begun = |writer| Start {
            at: Utc::now(),
            recipient: Recipient::Ant,
            threads: vec![],
            commits: vec![],
            writer,
            vitals: vec![],
            trails: &ColonyTrails::UNREAD,
        };
        let prompts = Ends { replied: 2, ..Ends::default() };
        let vitals = vec![(on(1).thread, Signs { threads: 1, prompts, sense_calls: 5, ..Signs::default() })];
        let first = dream::begin(items, Start { vitals, ..begun(&writer) }).await.unwrap().unwrap();
        dream::woken(items, 1, &on(7).thread).await.unwrap();
        let verdicts: Vec<_> = (first.findings.iter())
            .filter_map(|finding| {
                let verdict = match finding.kind {
                    FindingKind::Proposal => "applied",
                    FindingKind::Contradiction => "rejected",
                    FindingKind::NeedsReview => "deferred",
                    _ => return None,
                };
                Some(json!({"finding": finding.id, "verdict": verdict, "reason": "Judged.", "items": finding.items}))
            })
            .collect();
        let answered = call(sense, dream::dream, json!({"verdicts": verdicts, "done": true}), 7).await;
        assert!(!answered.is_error, "{answered:?}");
        let next = Start { at: Utc::now() + Duration::hours(25), ..begun(&writer) };
        let next = dream::begin(items, next).await.unwrap().unwrap();
        assert_eq!(next.findings.len(), 2, "the deferred and unanswered come again: {:?}", next.findings);
    }

    #[tokio::test]
    async fn the_report_counts_how_the_identity_grew() {
        let (sense, kept) = sense().await;
        let items: Arc<dyn Items> = kept.clone();
        let ant = name("dev-a");
        assert_eq!(report(&*items, &ant, Utc::now()).await.unwrap(), None, "no sense data, no report");

        let earlier = threads_told(&*items, Utc::now()).await;
        identity::record_session(&*items, &context(human())).await.unwrap();
        identity::record_growth(&*items, &ant, Growth { born: earlier, prompts: 42 }).await.unwrap();
        remembered(&sense).await;
        identity_written(&sense, &*items).await;
        told(&*items, 6, 5, Utc::now(), started("bob", None)).await;
        dreamt(&sense, &*items).await;

        let now = Utc::now();
        let report = report(&*items, &ant, now).await.unwrap().unwrap();
        let confirmed = Confirmation::of(&persona::current(&*items).await.unwrap().unwrap().values).confirmed.unwrap();
        let written = records::self_model(&*items).await.unwrap().unwrap().item.updated.at;
        let expected = format!(
            "Threads: 5 in 2 repositories, 1 done, 0 started again, 0 expired; 3 active days, {first} to {last}.
  Started 4 by humans, 1 by other ants.
  Threads by repository: repo:github.com/o/r 2, repo:github.com/o/s 1.
  Prompts completed: 42 since {first}.
By week from Monday (UTC), the last 8: threads started and done, active days, the calls of sense's tools that \
             wrote, the verdicts given in dreams.
  {earlier_week}: threads 1 started, 1 done, 2 active days.
  {this_week}: threads 4 started, 1 active days; calls remember 3, recall 1, vote 2, identity 4; verdicts 3.
  Calls not kept: sense_brief, searches and other reads write nothing, nor does a call changing nothing; \
             sense_recall reading a memory in full counts once a day a memory, sense_dream only as its verdicts; the \
             vital signs of dreams count all calls of sense's tools, not by tool.
Memories: 4, 1 core; votes 1 helpful, 1 stale, 0 wrong; 1 reads in full, once a day a memory.
  lesson: 2 (1 active, 1 needs_review); votes 1 helpful, 1 stale, 0 wrong.
  episode: 2 (2 active); votes 0 helpful, 0 stale, 0 wrong.
Dreams: 2 (1 fallback, 1 done), 0 sent to a sage; 0 writes on dream threads.
  Findings: 4 (1 applied, 1 rejected, 1 deferred, 1 unanswered), each by its last verdict; 1 rejected ones resting.
  contradiction: 1 (1 rejected).
  needs_review: 1 (1 deferred).
  distill: 1 (1 unanswered).
  proposal: 1 (1 applied).
  Vital signs of their periods, the last 5 dreams keeping them:
  dream 1, began {first_dream}: 2 prompts (2 replied), 5 sense tool calls.
  dream 2, began {next_dream}: no prompt measured.
  dream 2 against the mean of the dream before: threads worked 0 (mean 1, -100%); prompts replied 0 (mean 2, \
             -100%); sense tool calls 0 (mean 5, -100%).
Persona: revision 2, confirmed {confirmed}; 1 proposals (1 confirmed).
Self-model: revision 1, written {written}; since then 1 threads on 1 active days.
People: 3 started its threads, 1 with a relationship record; of the 1 who started 3 or more, 1 with one; 1 \
             records of others.
  By threads started: 2 started 1, 1 started 3-4.
  human:alice: 3 threads, record revision 1.
  human:bob: 1 threads, no record.
  ant:dev-b: 1 threads, no record.
Skills: 1, 1 revisions, 0 inherited; no proposals.
Thresholds: a dream finds no persona after 5 threads, no self-model after 5, no relationship record of one who \
             started 3, a self-model stale after 30 active days, an inheritance to own after 5 threads; the brief \
             names 3 people and repositories and 10 memories.",
            first = day(earlier),
            last = day(now),
            earlier_week = week(earlier),
            this_week = week(now),
            first_dream = day(now),
            next_dream = day(now + Duration::hours(25)),
            confirmed = time(confirmed),
            written = time(written),
        );
        assert_eq!(report, expected);
    }

    #[test]
    fn the_reviews_name_the_top_kinds_and_ants_and_count_the_rest() {
        let made = |made| dream::FindingTally { made, unanswered: made, ..Default::default() };
        let at = DateTime::from_timestamp(0, 0).unwrap();
        let tally = ReviewTally {
            reviews: 13,
            ants: 12,
            last: at,
            findings: made(5),
            cost: None,
            by_kind: vec![dream::KindTally { kind: "lesson".into(), findings: made(3) }],
            by_ant: (0..TOP)
                .map(|n| {
                    let ant = name(&format!("dev-{n}"));
                    dream::AntTally { ant, reviews: 1, last: at, findings: made(0), cost: None }
                })
                .collect(),
        };
        let mut text = String::new();
        reviews_lines(&mut text, Some(&tally));
        let lines: Vec<_> = text.lines().collect();
        assert_eq!(lines.len(), 1 + 2 + TOP + 1, "{text}");
        assert_eq!(lines[2], "  2 findings of other kinds.");
        assert_eq!(lines.last(), Some(&"  2 more ants."));
    }
}
