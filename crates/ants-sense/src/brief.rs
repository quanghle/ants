//! The brief (requirements ants-x39hk.8.1 and ants-x39hk.8.6): what an ant reads first in each session, and again on
//! demand with `sense_brief`. Its sections come in a fixed order ([`sections`]): warnings when the ant's log is
//! broken or over its size limit ([`SIZE_LIMIT`]), the identity with the experience counted from the ant's thread
//! records ([`thread::experience`]), the persona, the self-model, the thread's resume register, the unacknowledged
//! handoffs, open commitments, fired and upcoming reminders, claimed and ready tasks, the relationship records of the
//! thread's participants, the core and top memories, the skills index, and the core loop of tools.
//!
//! The brief stays within its byte budget ([`fit`]), cutting what other calls show before the ant's identity
//! ([`Order`]): memories and skills are shortened to their titles first, then the work sections, memories and skills
//! are cut, the longest first; then the core loop is shortened to the tool names; and only then are the relationship
//! records, the self-model and the persona cut. Each cut section ends with a line naming the tool call that shows the
//! rest, and the shortened core loop points to the tools' descriptions; the warnings and the identity are never cut.

use std::fmt::Write as _;

use ants_proto::UserId;
use ants_proto::provider::{BRIEF_BUDGET, BoxFuture, CallContext, ToolCall};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::identity::{self, Card};
use crate::items::{Item, ItemId};
use crate::log::{Break, SIZE_LIMIT, mib};
use crate::memory::{self, MemoryItem, Ranked, Scope, Status, Trust, framed, participants, snippet, thread_repo};
use crate::records::{self, RecordItem};
use crate::skill::{self, Skill};
use crate::store::{Items, StoreError};
use crate::thread::experience::Experience;
use crate::thread::{self, Handoff, Register};
use crate::{Answer, Sense, persona, promise, remind, task};

/// The most unacknowledged handoffs a brief shows.
pub const MAX_HANDOFFS: usize = 5;
/// The most memories past the core ones a brief shows.
pub const TOP_MEMORIES: usize = 10;
/// What the line ending a cut section starts with, the tool call showing the rest after it.
pub const MORE: &str = "… more: ";
/// What the line telling the ant of its return starts with.
pub const RETURNED: &str = "Returned ";

/// The most people and the most repositories the identity names: those of the ant's most threads.
pub const TOP_EXPERIENCE: usize = 3;

/// The most characters of a break's reason in the warning.
const REASON_CHARS: usize = 160;
/// The most characters of a person or repository the identity names, more than any name has.
const SCOPE_CHARS: usize = 72;

/// The core loop of tools: each tool and when to use it.
const CORE_LOOP: &[(&str, &str)] = &[
    ("checkpoint", "after each step, replace this thread's resume register from the revision shown"),
    ("task", "list ready tasks; claim a task, then heartbeat, close or release using the claim token"),
    ("promise", "record commitments with promisees and due times; keep, break, release or renegotiate commitments"),
    ("remind", "set, acknowledge, postpone or drop time and condition reminders"),
    ("recall", "before deciding, search the memories; with an id, read one in full"),
    ("remember", "keep a fact, lesson, decision, preference or gotcha worth knowing later"),
    ("vote", "say whether a recalled memory helped, is stale or is wrong"),
    (
        "identity",
        "show who the ant is; replace the self-model or a relationship record from the revision shown; propose a \
         change of values, voice, boundaries or working style, which a later dream confirms",
    ),
    ("skill", "read a skill; propose a new, changed or removed one, which a later dream confirms"),
    ("handoff", "hand the work over as the thread's last write"),
    ("brief", "show this brief again"),
];

/// When a section shortens and is cut over the brief's budget: each in turn, until the brief fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Order {
    /// The work sections, memories and skills, shown again by their tools.
    Work,
    /// The core loop, which each tool's description tells again.
    Help,
    Relationships,
    SelfModel,
    Persona,
}

impl Order {
    const ALL: [Self; 5] = [Self::Work, Self::Help, Self::Relationships, Self::SelfModel, Self::Persona];
}

/// A part of the brief: its heading, its body, and how it shortens.
#[derive(Debug, Clone)]
struct Section {
    heading: String,
    body: String,
    /// The body shortened to its titles, or to its tools' names.
    titles: Option<String>,
    /// Whether the body is shown in the block of recalled memories ([`framed`]).
    framed: bool,
    /// The tool call showing what a cut leaves out; none for a section never cut.
    more: Option<String>,
    cut: bool,
    order: Order,
}

impl Section {
    /// A section kept whole.
    fn whole(heading: String, body: String) -> Self {
        Self { heading, body, titles: None, framed: false, more: None, cut: false, order: Order::Work }
    }

    /// A section cut when the brief is over its budget, `more` showing the rest.
    fn cut_with(heading: &str, body: String, more: &str) -> Self {
        Self { more: Some(more.to_owned()), ..Self::whole(heading.to_owned(), body) }
    }

    fn render(&self) -> String {
        let mut out = self.heading.clone();
        if !self.body.is_empty() {
            out.push('\n');
            if self.framed {
                out.push_str(&framed(&self.body));
            } else {
                out.push_str(&self.body);
            }
        }
        if let (true, Some(more)) = (self.cut, &self.more) {
            let _ = write!(out, "\n{MORE}{more}");
        }
        out
    }

    /// Show the memories by their titles alone.
    fn shorten(&mut self) {
        if let Some(titles) = self.titles.take() {
            self.body = titles;
            self.cut = true;
        }
    }

    /// Cut the body so the section takes at most `target` bytes, or as few as it can; false when it cannot shrink.
    fn cut_to(&mut self, target: usize) -> bool {
        let Some(more) = &self.more else { return false };
        let before = (self.render().len(), self.body.clone(), self.cut);
        let frame = if self.framed { framed("").len() } else { 0 };
        let around = self.heading.len() + 1 + MORE.len() + more.len() + 1 + frame;
        self.body = cut_text(&self.body, target.saturating_sub(around));
        self.cut = true;
        if self.render().len() < before.0 {
            return true;
        }
        (_, self.body, self.cut) = before;
        false
    }
}

/// `text` within `max` bytes: up to its last line break in the second half of them, else on a character boundary.
fn cut_text(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let end = text.floor_char_boundary(max);
    let kept = &text[..end];
    let kept = match kept.rfind('\n') {
        Some(line) if line >= end / 2 => &kept[..line],
        _ => kept,
    };
    kept.trim_end().to_owned()
}

/// `sections` joined within `budget` bytes: whole when they fit; else, [`Order`] by order until they fit, with the
/// sections of each shortened to their titles, then the longest of them cut, down to the next longest or as much as
/// needed. Sections kept whole that do not fit alone are cut on a character boundary.
fn fit(mut sections: Vec<Section>, budget: usize) -> String {
    let joined = |sections: &[Section]| sections.iter().map(Section::render).collect::<Vec<_>>().join("\n\n");
    let mut stuck = vec![false; sections.len()];
    for order in Order::ALL {
        if joined(&sections).len() <= budget {
            break;
        }
        sections.iter_mut().filter(|section| section.order == order).for_each(Section::shorten);
        loop {
            let total = joined(&sections).len();
            let Some(over) = total.checked_sub(budget).filter(|over| *over > 0) else { break };
            let lengths: Vec<_> = sections.iter().map(|section| section.render().len()).collect();
            let cuttable = |i: &usize| !stuck[*i] && sections[*i].order == order && sections[*i].more.is_some();
            let Some(longest) = (0..sections.len()).filter(cuttable).max_by_key(|i| lengths[*i]) else { break };
            let next = (0..sections.len()).filter(cuttable).filter(|i| *i != longest).map(|i| lengths[i]).max();
            let length = lengths[longest];
            let mut target = length.saturating_sub(over).max(next.unwrap_or(0));
            if target >= length {
                target = length.saturating_sub(over);
            }
            if !sections[longest].cut_to(target) {
                stuck[longest] = true;
            }
        }
    }
    let text = joined(&sections);
    text[..text.floor_char_boundary(budget)].trim_end().to_owned()
}

/// The brief of the thread of `context` within `budget` bytes, from `items` and `card`, the ant's card. Once it tells
/// the ant of its return ([`Card::returned`]), the card no longer asks to.
pub async fn compose(
    items: &dyn Items,
    context: &CallContext,
    card: Option<&Card>,
    budget: usize,
) -> Result<String, StoreError> {
    let text = fit(sections(items, context, card).await?, budget);
    if card.is_some_and(Card::returned) && text.lines().any(|line| line.starts_with(RETURNED)) {
        // Failing to note it only tells the ant again in the next brief.
        let _ = identity::returned_told(items, &context.ant).await;
    }
    Ok(text)
}

/// The sections of the brief, in order.
async fn sections(items: &dyn Items, context: &CallContext, card: Option<&Card>) -> Result<Vec<Section>, StoreError> {
    let mut sections = Vec::new();
    sections.extend(items.broken().await.as_ref().map(warning));
    sections.extend(size_warning(items.log_bytes().await));
    let experience = thread::experience::experience(items, &context.ant, TOP_EXPERIENCE).await?;
    sections.push(identity_section(card, context, experience.as_ref()));
    sections.push(persona_section(persona::current(items).await?.as_ref()));
    sections.push(self_model_section(records::self_model(items).await?.as_ref()));
    sections.push(register_section(thread::register(items, &context.thread).await?.as_ref()));
    let mut handoffs = thread::unacknowledged(items, MAX_HANDOFFS + 1).await?;
    let more = handoffs.len() > MAX_HANDOFFS;
    handoffs.truncate(MAX_HANDOFFS);
    sections.extend(handoffs_section(&handoffs, more));
    if card.is_some_and(Card::returned)
        && let Some(last) = thread::last_handoff(items).await?
        && !handoffs.iter().any(|handoff| handoff.id == last.id)
    {
        sections.push(Section::cut_with("## Last handoff", last.text().trim_end().to_owned(), thread::READ));
    }
    sections.extend(commitments_section(items, context.at).await?);
    sections.extend(reminders_sections(items, context.at).await?);
    sections.extend(tasks_sections(items, context).await?);
    sections.extend(relationships_section(&records::relationships(items, &participants(context)).await?));
    let episodes: Vec<_> = handoffs.into_iter().filter_map(|handoff| handoff.episode).collect();
    sections.extend(memories_section(items, context, &episodes).await?);
    sections.extend(skills_section(&skill::skills(items).await?));
    sections.push(core_loop());
    Ok(sections)
}

fn warning(broken: &Break) -> Section {
    let reason: String = broken.reason.chars().take(REASON_CHARS).collect();
    let line = format!(
        "Warning: this ant's sense log breaks at event {} ({reason}); what follows may miss data. `ants doctor` \
         reports the break.",
        broken.index
    );
    Section::whole(line, String::new())
}

/// A warning when the log holds over [`SIZE_LIMIT`] bytes.
fn size_warning(bytes: u64) -> Option<Section> {
    if bytes <= SIZE_LIMIT {
        return None;
    }
    let line = format!(
        "Warning: this ant's sense log holds {}, over the {} MiB limit, and keeps every change for good. Write less: \
         supersede or vote on memories rather than remembering near duplicates, and checkpoint only when the register \
         changes. `ants doctor` reports the size.",
        mib(bytes),
        SIZE_LIMIT >> 20
    );
    Some(Section::whole(line, String::new()))
}

fn identity_section(card: Option<&Card>, context: &CallContext, experience: Option<&Experience>) -> Section {
    let mut body = identity_text(card, &context.ant, experience);
    if let Some(card) = card
        && let Some(at) = card.returned_at
    {
        let how = match card.retired_at {
            Some(since) => format!(", dormant since {}: back after a retirement", since.format("%Y-%m-%d")),
            None => ": added again under a new ants identity, as after a revoke and re-add".to_owned(),
        };
        let _ = write!(
            body,
            "\n{RETURNED}{}{how}. The register, the last handoff and the open commitments below, overdue ones marked, \
             tell where the work stood.",
            at.format("%Y-%m-%d")
        );
    }
    Section::whole("## Identity".to_owned(), body)
}

/// The ant's `card`, or the name of `ant` while it has none, with the lines of `experience` and whether it is
/// dormant: the identity the brief and `sense_identity` show.
pub(crate) fn identity_text(card: Option<&Card>, ant: &UserId, experience: Option<&Experience>) -> String {
    let experience = experience.map_or_default(|experience| format!("\n{}", experience_lines(experience)));
    let Some(card) = card else {
        return format!("{ant}, with no sense card yet.{experience}");
    };
    let mut body = format!("{} (sense identity {}), born {}.", card.name, card.sense, card.born.format("%Y-%m-%d"));
    if !card.earlier_names.is_empty() {
        let names: Vec<_> = card.earlier_names.iter().map(ToString::to_string).collect();
        let _ = write!(body, " Earlier names: {}.", names.join(", "));
    }
    if let Some(from) = &card.cloned_from {
        let _ = write!(body, " Cloned from {}.", from.ant);
    }
    for (label, values) in [("Harnesses", &card.harnesses), ("Models", &card.models), ("Machines", &card.machines)] {
        if !values.is_empty() {
            let _ = write!(body, "\n{label}: {}.", values.join(", "));
        }
    }
    body.push_str(&experience);
    if !card.active
        && let Some(at) = card.retired_at
    {
        let _ = write!(
            body,
            "\nDormant since {}: retired, its reminders paused and its commitments open until revived.",
            at.format("%Y-%m-%d")
        );
    }
    body
}

fn persona_section(found: Option<&Item>) -> Section {
    match found {
        Some(found) => Section {
            order: Order::Persona,
            ..Section::cut_with("## Persona", persona::text(&found.values), "sense_identity")
        },
        None => {
            let body = "No persona yet: propose values, voice, boundaries or working style with sense_identity.";
            Section::whole("## Persona".to_owned(), body.to_owned())
        }
    }
}

/// The skills by name and description.
fn skills_section(skills: &[Skill]) -> Option<Section> {
    if skills.is_empty() {
        return None;
    }
    let lines: Vec<_> = skills.iter().map(|skill| format!("- {}: {}", skill.name, skill.description)).collect();
    let names: Vec<_> = skills.iter().map(|skill| format!("- {}", skill.name)).collect();
    let mut section =
        Section::cut_with("## Skills (sense_skill name=<name> reads one)", lines.join("\n"), "sense_skill");
    section.titles = Some(names.join("\n"));
    Some(section)
}

fn self_model_section(found: Option<&RecordItem>) -> Section {
    let Some(found) = found else {
        let body =
            "No self-model yet: write one with sense_identity, giving self_model and a title, without a revision.";
        return Section::whole("## Self-model".to_owned(), body.to_owned());
    };
    let more = format!("sense_recall id={}", found.id());
    let mut section = Section { order: Order::SelfModel, ..Section::cut_with("## Self-model", found.text(), &more) };
    (section.framed, section.titles) = (true, Some(found.head()));
    section
}

/// The lines of `experience`: the threads and their days, then the people and the repositories of the most threads.
pub(crate) fn experience_lines(experience: &Experience) -> String {
    let plural = |count: u32, noun: &str| if count == 1 { format!("1 {noun}") } else { format!("{count} {noun}s") };
    let mut lines = format!(
        "Experience: {}, {} done, on {}.",
        plural(experience.threads, "thread"),
        experience.done,
        plural(experience.active_days, "active day")
    );
    for (label, tallies) in [("Most threads with", &experience.people), ("Most threads in", &experience.repos)] {
        let named: Vec<_> = tallies
            .iter()
            .map(|tally| {
                let mut scope = tally.scope.to_string();
                if scope.chars().count() > SCOPE_CHARS {
                    scope = scope.chars().take(SCOPE_CHARS - 1).chain(['…']).collect();
                }
                format!("{scope} ({}, last {})", tally.threads, tally.last.format("%Y-%m-%d"))
            })
            .collect();
        if !named.is_empty() {
            let _ = write!(lines, "\n{label}: {}.", named.join(", "));
        }
    }
    lines
}

/// The relationship records of the thread's participants.
fn relationships_section(found: &[RecordItem]) -> Option<Section> {
    if found.is_empty() {
        return None;
    }
    let body: Vec<_> = found.iter().map(RecordItem::text).collect();
    let titles: Vec<_> = found.iter().map(RecordItem::head).collect();
    let mut section = Section {
        order: Order::Relationships,
        ..Section::cut_with("## Relationships", body.join("\n"), "sense_recall id=<id>")
    };
    (section.framed, section.titles) = (true, Some(titles.join("\n")));
    Some(section)
}

fn register_section(register: Option<&Register>) -> Section {
    match register {
        Some(register) => Section::cut_with("## This thread", register.text().trim_end().to_owned(), thread::READ),
        None => {
            let body = "No resume register yet: start one with sense_checkpoint, without a revision.";
            Section::whole("## This thread".to_owned(), body.to_owned())
        }
    }
}

/// The section of `handoffs`, ending with the tool call showing the rest when there are `more`.
fn handoffs_section(handoffs: &[Handoff], more: bool) -> Option<Section> {
    if handoffs.is_empty() {
        return None;
    }
    let texts: Vec<_> = handoffs.iter().map(|handoff| handoff.text().trim_end().to_owned()).collect();
    let heading = "## Unacknowledged handoffs (a checkpoint acknowledges them)";
    let mut section = Section::cut_with(heading, texts.join("\n"), thread::READ);
    section.cut = more;
    Some(section)
}

async fn commitments_section(items: &dyn Items, now: DateTime<Utc>) -> Result<Option<Section>, StoreError> {
    let promises = promise::open(items, now).await?;
    let lines = promises
        .iter()
        .map(|item| {
            let overdue =
                if item.text("due").parse::<DateTime<Utc>>().is_ok_and(|due| due <= now) { ", overdue" } else { "" };
            format!(
                "- {} (revision {}, open{overdue}): {} — to {}, due {}; source thread {}",
                item.id,
                item.revision,
                item.text("title"),
                item.text("promisee"),
                item.text("due"),
                item.text("source_thread")
            )
        })
        .collect();
    Ok(work_section("## Open commitments", lines, "sense_promise action=list"))
}

async fn reminders_sections(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<Section>, StoreError> {
    let (mut fired, mut upcoming) = (Vec::new(), Vec::new());
    for item in remind::active(items, now).await? {
        let is_fired = item.values.get("occurrence_firings").and_then(Value::as_u64).unwrap_or(0) > 0;
        let mut line = format!(
            "- {} (revision {}, {}): {}",
            item.id,
            item.revision,
            if is_fired { "fired" } else { "upcoming" },
            item.text("title")
        );
        if is_fired {
            let pending = item.values.get("pending");
            let firing = pending.or_else(|| item.values.get("delivered"));
            if let Some(firing) = firing {
                let _ = write!(
                    line,
                    "; firing {} at {}",
                    firing["id"].as_str().unwrap_or_default(),
                    firing["at"].as_str().unwrap_or_default()
                );
            }
            line.push_str(if pending.is_some() { "; delivery pending" } else { "; awaiting acknowledgement" });
        }
        if let Some(next) = item.values.get("next_due").and_then(Value::as_str) {
            let _ = write!(line, "; next {next}");
        }
        if !is_fired && let Some(trigger) = item.values.get("trigger") {
            let _ = write!(line, "; trigger {trigger}");
        }
        let _ = write!(line, "; thread {}", item.text("thread"));
        if let Some(promise) = item.values.get("promise").and_then(Value::as_str) {
            let _ = write!(line, "; commitment {promise}");
        }
        if is_fired {
            fired.push(line);
        } else {
            upcoming.push(line);
        }
    }
    Ok([
        work_section("## Fired reminders", fired, "sense_remind action=list"),
        work_section("## Upcoming reminders", upcoming, "sense_remind action=list"),
    ]
    .into_iter()
    .flatten()
    .collect())
}

async fn tasks_sections(items: &dyn Items, context: &CallContext) -> Result<Vec<Section>, StoreError> {
    let mut claimed = Vec::new();
    for found in task::claimed(items, &context.thread, usize::MAX).await? {
        let mut line = task_line(&found);
        if found.task.lease.as_ref().is_some_and(|lease| lease.expires_at <= context.at) {
            let _ = write!(line, "; lease expired; claim again: sense_task action=claim id={}", found.id());
        } else if let Some(token) = task::fencing_token(items, &found).await? {
            let _ = write!(line, "; claim token {token}");
        }
        claimed.push(line);
    }
    let ready = task::ready(items, usize::MAX).await?.iter().map(task_line).collect();
    Ok([
        work_section("## Claimed tasks (this thread)", claimed, "sense_task action=list limit=100"),
        work_section("## Ready tasks", ready, "sense_task action=ready limit=100"),
    ]
    .into_iter()
    .flatten()
    .collect())
}

fn task_line(found: &task::TaskItem) -> String {
    let mut line = format!("- {}", found.text());
    if let Some(reference) = &found.task.external_ref {
        let _ = write!(line, "; external {reference}");
    }
    line
}

fn work_section(heading: &str, lines: Vec<String>, more: &str) -> Option<Section> {
    (!lines.is_empty()).then(|| Section::cut_with(heading, lines.join("\n"), more))
}

/// The core memories, then the top memories of the thread's scopes but the `episodes` of the handoffs shown.
async fn memories_section(
    items: &dyn Items,
    context: &CallContext,
    episodes: &[ItemId],
) -> Result<Option<Section>, StoreError> {
    let core = memory::core(items).await?;
    let mut scopes = vec![Scope::Own];
    scopes.extend(thread_repo(context));
    scopes.extend(participants(context));
    let top = memory::top(items, &scopes, TOP_MEMORIES + core.len() + episodes.len(), context.at).await?;
    let shown = |ranked: &Ranked| ranked.found.memory.core || episodes.contains(ranked.found.id());
    let top: Vec<Ranked> = top.into_iter().filter(|ranked| !shown(ranked)).take(TOP_MEMORIES).collect();
    if core.is_empty() && top.is_empty() {
        return Ok(None);
    }
    let (mut body, mut titles) = (Vec::new(), Vec::new());
    for (label, found) in [("Core:", core.iter().collect::<Vec<_>>()), ("Top:", top.iter().map(|r| &r.found).collect())]
    {
        if found.is_empty() {
            continue;
        }
        body.push(label.to_owned());
        titles.push(label.to_owned());
        for found in found {
            let full = if found.memory.core {
                found.memory.body.replace('\n', "\n  ")
            } else {
                snippet(&found.memory.body, None)
            };
            body.push(format!("{}\n  {full}", head(found)));
            titles.push(head(found));
        }
    }
    let mut section =
        Section::cut_with("## Memories", body.join("\n"), "sense_recall, or sense_recall id=<id> for one in full");
    (section.framed, section.titles) = (true, Some(titles.join("\n")));
    Ok(Some(section))
}

/// A memory's id, kind, scope, importance and what to mind, then its title.
fn head(found: &MemoryItem) -> String {
    let memory = &found.memory;
    let mut flags = String::new();
    if memory.status == Status::NeedsReview {
        flags.push_str(", needs review");
    }
    if memory.trust == Trust::Untrusted {
        flags.push_str(", untrusted");
    }
    let mut head = format!(
        "- {} ({}, {}, importance {}{flags}): {}",
        found.id(),
        memory.kind,
        memory.scope,
        memory.importance,
        memory.title
    );
    for warning in &memory.citation_warnings {
        if let Some(citation) = memory.citations.get(warning.citation) {
            let _ = write!(head, "\n  Warning: {citation}: {}.", warning.reason);
        }
    }
    head
}

/// The core loop, shortened to the tools' names over the brief's budget.
fn core_loop() -> Section {
    let lines: Vec<_> = CORE_LOOP.iter().map(|(tool, when)| format!("- sense_{tool}: {when}.")).collect();
    let names: Vec<_> = CORE_LOOP.iter().map(|(tool, _)| format!("sense_{tool}")).collect();
    let names = format!("{}: each tool's description says when to use it.", names.join(", "));
    Section { titles: Some(names), order: Order::Help, ..Section::whole("## Core loop".to_owned(), lines.join("\n")) }
}

/// Answers a `sense_brief` call: the brief of the thread, within the budget of a session's.
pub fn brief<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move {
        let context = &call.context;
        let composed = async {
            let items = sense.items(&context.ant).await?;
            if let Some(workdir) = sense.workdir(context) {
                memory::recheck_citations(&*items, &workdir).await?;
            }
            // Files failing to be written are written again at the next brief.
            let _ = crate::skill::materialize_for(sense, &context.ant).await;
            let card = identity::card(&*items).await?;
            compose(&*items, context, card.as_ref(), BRIEF_BUDGET as usize).await
        };
        match composed.await {
            Ok(text) => Answer { json: json!({"brief": text}), text, is_error: false },
            Err(e) => e.into(),
        }
    })
}

#[cfg(test)]
mod tests;
