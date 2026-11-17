//! The brief (requirements ants-x39hk.8.1 and ants-x39hk.8.2): what an ant reads first in each session, and again on
//! demand with `sugar_brief`. Its sections come in a fixed order ([`sections`]): a warning when the ant's log is
//! broken, the identity, the thread's resume register, the unacknowledged handoffs, the core and top memories, and the
//! core loop of tools.
//!
//! The brief stays within its byte budget ([`fit`]): memories are shortened to their titles first, then the longest
//! section is cut, again and again, each cut ending with a line naming the tool call that shows the rest.

use std::fmt::Write as _;

use ants_proto::provider::{BRIEF_BUDGET, BoxFuture, CallContext, ToolCall};
use serde_json::json;

use crate::identity::{self, Card};
use crate::items::ItemId;
use crate::log::Break;
use crate::memory::{self, MemoryItem, Ranked, Scope, Status, Trust, framed, participants, snippet};
use crate::store::{Items, StoreError};
use crate::thread::{self, Handoff, Register};
use crate::{Answer, Sugar};

/// The most unacknowledged handoffs a brief shows.
pub const MAX_HANDOFFS: usize = 5;
/// The most memories past the core ones a brief shows.
pub const TOP_MEMORIES: usize = 10;
/// What the line ending a cut section starts with, the tool call showing the rest after it.
pub const MORE: &str = "… more: ";
/// What the line telling the ant of its return starts with.
pub const RETURNED: &str = "Returned ";

/// The most characters of a break's reason in the warning.
const REASON_CHARS: usize = 160;

/// The core loop of tools: each tool and when to use it.
const CORE_LOOP: &[(&str, &str)] = &[
    ("checkpoint", "after each step, replace this thread's resume register from the revision shown"),
    ("recall", "before deciding, search the memories; with an id, read one in full"),
    ("remember", "keep a fact, lesson, decision, preference or gotcha worth knowing later"),
    ("vote", "say whether a recalled memory helped, is stale or is wrong"),
    ("handoff", "hand the work over as the thread's last write"),
    ("brief", "show this brief again"),
];

/// A part of the brief: its heading, its body, and how it shortens.
#[derive(Debug, Clone)]
struct Section {
    heading: String,
    body: String,
    /// The body with its memories shortened to their titles.
    titles: Option<String>,
    /// Whether the body is shown in the block of recalled memories ([`framed`]).
    framed: bool,
    /// The tool call showing what a cut leaves out; none for a section never cut.
    more: Option<String>,
    cut: bool,
}

impl Section {
    /// A section kept whole.
    fn whole(heading: String, body: String) -> Self {
        Self { heading, body, titles: None, framed: false, more: None, cut: false }
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

/// `sections` joined within `budget` bytes: whole when they fit; else with memories shortened to their titles; then
/// with the longest section cut, down to the next longest or as much as needed, until they fit. Sections kept whole
/// that do not fit alone are cut on a character boundary.
fn fit(mut sections: Vec<Section>, budget: usize) -> String {
    let joined = |sections: &[Section]| sections.iter().map(Section::render).collect::<Vec<_>>().join("\n\n");
    if joined(&sections).len() > budget {
        sections.iter_mut().for_each(Section::shorten);
    }
    let mut stuck = vec![false; sections.len()];
    loop {
        let total = joined(&sections).len();
        let Some(over) = total.checked_sub(budget).filter(|over| *over > 0) else { break };
        let lengths: Vec<_> = sections.iter().map(|section| section.render().len()).collect();
        let cuttable = |i: &usize| !stuck[*i] && sections[*i].more.is_some();
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

/// The sections of the brief, in order. The commitments (overdue first), the reminders (fired and upcoming), the
/// claimed and ready tasks and the relationship notes of the thread's participants come after the handoffs once sugar
/// keeps them, and the skills index after the memories.
async fn sections(items: &dyn Items, context: &CallContext, card: Option<&Card>) -> Result<Vec<Section>, StoreError> {
    let mut sections = Vec::new();
    sections.extend(items.broken().await.as_ref().map(warning));
    sections.push(identity_section(card, context));
    sections.push(register_section(thread::register(items, &context.thread).await?.as_ref()));
    let handoffs = thread::unacknowledged(items, MAX_HANDOFFS).await?;
    sections.extend(handoffs_section(&handoffs));
    let episodes: Vec<_> = handoffs.into_iter().filter_map(|handoff| handoff.episode).collect();
    sections.extend(memories_section(items, context, &episodes).await?);
    sections.push(core_loop());
    Ok(sections)
}

fn warning(broken: &Break) -> Section {
    let reason: String = broken.reason.chars().take(REASON_CHARS).collect();
    let line = format!(
        "Warning: this ant's sugar log breaks at event {} ({reason}); what follows may miss data. `ants doctor` \
         reports the break.",
        broken.index
    );
    Section::whole(line, String::new())
}

fn identity_section(card: Option<&Card>, context: &CallContext) -> Section {
    let heading = "## Identity".to_owned();
    let Some(card) = card else {
        return Section::whole(heading, format!("{}, with no sugar card yet.", context.ant));
    };
    let mut body = format!("{} (sugar identity {}), born {}.", card.name, card.sugar, card.born.format("%Y-%m-%d"));
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
    if let Some(at) = card.retired_at {
        let _ = write!(body, "\nRetired since {}.", at.format("%Y-%m-%d"));
    }
    if let Some(at) = card.returned_at {
        let _ = write!(
            body,
            "\n{RETURNED}{}: added again under a new ants identity, as after a revoke and re-add. The register and \
             handoffs below tell where the work stood.",
            at.format("%Y-%m-%d")
        );
    }
    Section::whole(heading, body)
}

fn register_section(register: Option<&Register>) -> Section {
    let body = match register {
        Some(register) => register.text().trim_end().to_owned(),
        None => "No resume register yet: start one with sugar_checkpoint, without a revision.".to_owned(),
    };
    Section::cut_with("## This thread", body, "sugar_checkpoint without a revision is refused with the whole register")
}

fn handoffs_section(handoffs: &[Handoff]) -> Option<Section> {
    if handoffs.is_empty() {
        return None;
    }
    let texts: Vec<_> = handoffs
        .iter()
        .map(|handoff| match &handoff.episode {
            Some(episode) => format!("{}Episode memory: {episode}", handoff.text()),
            None => handoff.text().trim_end().to_owned(),
        })
        .collect();
    let heading = "## Unacknowledged handoffs (a checkpoint acknowledges them)";
    Some(Section::cut_with(heading, texts.join("\n"), "sugar_recall query=\"Handoff of thread\""))
}

/// The core memories, then the top memories of the thread's scopes but the `episodes` of the handoffs shown.
async fn memories_section(
    items: &dyn Items,
    context: &CallContext,
    episodes: &[ItemId],
) -> Result<Option<Section>, StoreError> {
    let core = memory::core(items).await?;
    let mut scopes = vec![Scope::Own];
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
        Section::cut_with("## Memories", body.join("\n"), "sugar_recall, or sugar_recall id=<id> for one in full");
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
    format!(
        "- {} ({}, {}, importance {}{flags}): {}",
        found.id(),
        memory.kind,
        memory.scope,
        memory.importance,
        memory.title
    )
}

fn core_loop() -> Section {
    let lines: Vec<_> = CORE_LOOP.iter().map(|(tool, when)| format!("- sugar_{tool}: {when}.")).collect();
    Section::whole("## Core loop".to_owned(), lines.join("\n"))
}

/// Answers a `sugar_brief` call: the brief of the thread, within the budget of a session's.
pub fn brief<'a>(sugar: &'a Sugar, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move {
        let context = &call.context;
        let composed = async {
            let items = sugar.items(&context.ant).await?;
            let card = identity::card(&*items).await?;
            compose(&*items, context, card.as_ref(), BRIEF_BUDGET as usize).await
        };
        match composed.await {
            Ok(text) => Answer { json: json!({"brief": text}), text, is_error: false },
            Err(e) => Answer::error("unavailable", format!("The brief is not available: {e}.")),
        }
    })
}

#[cfg(test)]
mod tests;
