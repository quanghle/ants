//! What an ant keeps of each thread to pick its work up again: the thread's resume register (`sense_checkpoint`,
//! requirement ants-x39hk.8.3) and its handoffs (`sense_handoff`, ants-x39hk.8.4).
//!
//! A thread has one resume register, an item of kind [`CHECKPOINT`] whose id is named after the thread
//! ([`ItemId::named`]), so writers starting it at once start the same item. A checkpoint replaces the whole register
//! by compare-and-set on its revision: the ant passes the revision it read (none for the thread's first), and a
//! checkpoint from another revision is refused with the current register.
//!
//! A handoff is an item of kind [`HANDOFF`], written in one event with the episode memory it links ([`EPISODE`]). It
//! stays unacknowledged, shown in every brief of the ant ([`unacknowledged`]), until a later checkpoint on its thread,
//! or a checkpoint on another thread naming it in `acknowledges`, sets its [`ACKNOWLEDGED`] value.
//!
//! Every text passes the sanitizer, and each item carries its writer, taken from the call's context, never from text.

use std::fmt::Write as _;

use ants_proto::ThreadId;
use ants_proto::provider::{BoxFuture, CallContext, ToolCall};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::items::{Item, ItemId, Stamp, Values};
use crate::memory::{MAX_BODY_BYTES, MAX_TITLE_CHARS, Memory, MemoryKind, Scope, WRITER, writer};
use crate::sanitize::{Limit, Lines, sanitize};
use crate::store::{Items, StoreError, Write};
use crate::{Answer, Sense};

/// The kind of a thread's resume register.
pub const CHECKPOINT: &str = "checkpoint";
/// The kind of a handoff.
pub const HANDOFF: &str = "handoff";
/// The value of a register or handoff naming its thread.
pub const THREAD: &str = "thread";
/// The value of a handoff naming its episode memory.
pub const EPISODE: &str = "episode";
/// The value of an acknowledged handoff: the thread and time of the checkpoint acknowledging it.
pub const ACKNOWLEDGED: &str = "acknowledged";

/// The most characters of the goal, the step, the next action, and of each entry of a list.
const TEXT_MAX: usize = 2_000;
/// The most characters of a branch or a file.
const LINE_MAX: usize = 500;
/// The most entries of a list.
const ENTRIES_MAX: usize = 50;

/// A thread's resume register, as its last checkpoint left it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Register {
    pub id: ItemId,
    pub revision: u64,
    pub thread: ThreadId,
    pub goal: Option<String>,
    /// The current step.
    pub step: Option<String>,
    /// The next action.
    pub next: Option<String>,
    pub blockers: Vec<String>,
    pub branch: Option<String>,
    /// The open questions.
    pub questions: Vec<String>,
    pub updated: Stamp,
}

impl Register {
    /// The register `item` holds; none for an item of another kind.
    pub fn of(item: &Item) -> Option<Self> {
        (item.kind == CHECKPOINT).then_some(())?;
        Some(Self {
            id: item.id.clone(),
            revision: item.revision,
            thread: item.text(THREAD).parse().ok()?,
            goal: text(item, "goal"),
            step: text(item, "step"),
            next: text(item, "next"),
            blockers: texts(item, "blockers"),
            branch: text(item, "branch"),
            questions: texts(item, "questions"),
            updated: item.updated.clone(),
        })
    }

    /// The register as brief text.
    pub fn text(&self) -> String {
        let at = self.updated.at.to_rfc3339_opts(SecondsFormat::Secs, true);
        let mut out = format!("Resume register, revision {} (checkpointed {at}):\n", self.revision);
        for (label, value) in [("Goal", &self.goal), ("Step", &self.step), ("Next", &self.next)] {
            if let Some(value) = value {
                let _ = writeln!(out, "{label}: {value}");
            }
        }
        list(&mut out, "Blockers", &self.blockers);
        if let Some(branch) = &self.branch {
            let _ = writeln!(out, "Branch: {branch}");
        }
        list(&mut out, "Open questions", &self.questions);
        out
    }
}

/// A handoff: the work as a thread handed it over.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Handoff {
    pub id: ItemId,
    pub thread: ThreadId,
    #[serde(flatten)]
    pub work: Work,
    /// Its episode memory.
    pub episode: Option<ItemId>,
    pub acknowledged: Option<Acknowledgement>,
    pub created: Stamp,
}

/// The work handed over: what was done, is in flight and comes next, the promises made, the decisions taken and the
/// files touched.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Work {
    pub done: Vec<String>,
    pub in_flight: Vec<String>,
    pub next: Vec<String>,
    pub promises: Vec<String>,
    pub decisions: Vec<String>,
    pub files: Vec<String>,
}

impl Work {
    const NAMES: [&str; 6] = ["done", "in_flight", "next", "promises", "decisions", "files"];
    const LABELS: [&str; 6] = ["Done", "In flight", "Next", "Promises", "Decisions", "Files"];

    fn lists(&self) -> [&Vec<String>; 6] {
        [&self.done, &self.in_flight, &self.next, &self.promises, &self.decisions, &self.files]
    }

    fn lists_mut(&mut self) -> [&mut Vec<String>; 6] {
        [&mut self.done, &mut self.in_flight, &mut self.next, &mut self.promises, &mut self.decisions, &mut self.files]
    }

    fn of(item: &Item) -> Self {
        let mut work = Self::default();
        for (name, list) in Self::NAMES.into_iter().zip(work.lists_mut()) {
            *list = texts(item, name);
        }
        work
    }

    /// The work as brief text, a list under its label each, the empty ones left out.
    pub fn text(&self) -> String {
        let mut out = String::new();
        for (label, entries) in Self::LABELS.into_iter().zip(self.lists()) {
            list(&mut out, label, entries);
        }
        out
    }
}

/// The checkpoint acknowledging a handoff: its thread and time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Acknowledgement {
    pub thread: ThreadId,
    pub at: DateTime<Utc>,
}

impl Handoff {
    /// The handoff `item` holds; none for an item of another kind.
    pub fn of(item: &Item) -> Option<Self> {
        (item.kind == HANDOFF).then_some(())?;
        Some(Self {
            id: item.id.clone(),
            thread: item.text(THREAD).parse().ok()?,
            work: Work::of(item),
            episode: item.text(EPISODE).parse().ok(),
            acknowledged: item.values.get(ACKNOWLEDGED).cloned().and_then(|value| serde_json::from_value(value).ok()),
            created: item.created.clone(),
        })
    }

    /// The handoff as brief text.
    pub fn text(&self) -> String {
        let at = self.created.at.to_rfc3339_opts(SecondsFormat::Secs, true);
        let mut out = format!("Handoff {} of thread {} ({at}):\n", self.id, self.thread);
        out.push_str(&self.work.text());
        out
    }
}

/// The resume register of `thread`, if any.
pub async fn register(items: &dyn Items, thread: &ThreadId) -> Result<Option<Register>, StoreError> {
    Ok(items.get(&register_id(thread)).await?.as_ref().and_then(Register::of))
}

/// The ant's unacknowledged handoffs, the last first, up to `limit`.
pub async fn unacknowledged(items: &dyn Items, limit: usize) -> Result<Vec<Handoff>, StoreError> {
    Ok(items.list_by(HANDOFF, ACKNOWLEDGED, None, limit).await?.iter().filter_map(Handoff::of).collect())
}

fn register_id(thread: &ThreadId) -> ItemId {
    ItemId::named(CHECKPOINT, thread.as_str())
}

/// `sense_checkpoint`: replace the thread's resume register, then acknowledge the unacknowledged handoffs of the
/// thread and those named in `acknowledges`.
pub fn checkpoint<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { checkpoint_call(sense, call).await.unwrap_or_else(|refusal| refusal) })
}

async fn checkpoint_call(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let mut args = Arguments { map: &call.arguments, redacted: 0 };
    let mut values = Values::new();
    for name in ["goal", "step", "next"] {
        values.insert(name.into(), args.text(name, Lines::Multi, TEXT_MAX)?.into());
    }
    values.insert("blockers".into(), listed(args.texts("blockers", Lines::Multi, TEXT_MAX)?));
    values.insert("branch".into(), args.text("branch", Lines::Single, LINE_MAX)?.into());
    values.insert("questions".into(), listed(args.texts("questions", Lines::Multi, TEXT_MAX)?));
    let revision = match call.arguments.get("revision") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_u64().ok_or_else(|| invalid("revision", "a revision number"))?),
    };
    let named = args.ids("acknowledges")?;
    let context = &call.context;
    values.insert(THREAD.into(), context.thread.as_str().into());
    values.insert(WRITER.into(), writer(context));

    let items = sense.items(&context.ant).await.map_err(refused)?;
    let mut acknowledge = Vec::new();
    for id in &named {
        let handoff = items.get(id).await.map_err(refused)?.filter(|item| item.kind == HANDOFF);
        let handoff = handoff.ok_or_else(|| Answer::error("not_found", format!("No handoff {id}.")))?;
        acknowledge.push(handoff);
    }

    let by = context.thread.as_str();
    let item = match items.put(&register_id(&context.thread), CHECKPOINT, values, Some(revision.unwrap_or(0)), by).await
    {
        Ok(item) => item,
        Err(StoreError::Conflict { current, .. }) => return Err(conflict(revision, &current)),
        Err(StoreError::NotFound(_)) => {
            let message = "This thread has no resume register: checkpoint without a revision to start one.";
            return Err(Answer::error("not_found", message.into()));
        }
        Err(e) => return Err(refused(e)),
    };
    let register = Register::of(&item).ok_or_else(|| refused(StoreError::Unavailable("unreadable register".into())))?;

    acknowledge.extend(items.list_by(HANDOFF, THREAD, Some(&Value::from(by)), usize::MAX).await.map_err(refused)?);
    acknowledge.retain(|handoff| !handoff.values.contains_key(ACKNOWLEDGED));
    acknowledge.sort_by(|a, b| a.id.cmp(&b.id));
    acknowledge.dedup_by(|a, b| a.id == b.id);
    let acknowledged = json!({"thread": context.thread, "at": context.at});
    let mut ids = Vec::new();
    for handoff in acknowledge {
        let values = Values::from_iter([(ACKNOWLEDGED.to_owned(), acknowledged.clone())]);
        if let Err(e) = items.change(&handoff.id, values, None, by).await {
            let message = format!(
                "Checkpoint saved at revision {}, but acknowledging handoff {} failed: {e}",
                register.revision, handoff.id
            );
            return Err(Answer::error("unavailable", message));
        }
        ids.push(handoff.id);
    }

    let mut text = format!("Checkpoint saved: revision {} of this thread's resume register.", register.revision);
    if !ids.is_empty() {
        let ids: Vec<_> = ids.iter().map(ItemId::as_str).collect();
        let _ = write!(text, "\nAcknowledged handoffs: {}.", ids.join(", "));
    }
    text.push_str(&args.redactions());
    let json = json!({"register": register, "acknowledged": ids, "redacted": args.redacted});
    Ok(Answer { text, json, is_error: false })
}

/// The refusal of a checkpoint from `revision` of the register, at another: the current register comes with it.
fn conflict(revision: Option<u64>, current: &Item) -> Answer {
    let (code, message) = match revision {
        None => (
            "revision_required",
            format!(
                "This thread's resume register exists, at revision {}: pass revision {0} to replace it, keeping what \
                 still holds.",
                current.revision
            ),
        ),
        Some(revision) => (
            "conflict",
            format!(
                "The resume register is at revision {}, not {revision}: checkpoint again from revision {0}, keeping \
                 what still holds.",
                current.revision
            ),
        ),
    };
    let register = Register::of(current);
    let mut answer = Answer::error(code, message);
    if let Some(register) = register {
        answer.text = format!("{}\n\n{}", answer.text, register.text());
        answer.json["register"] = json!(register);
    }
    answer
}

/// `sense_handoff`: record the work as this thread hands it over, with an episode memory.
pub fn handoff<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { handoff_call(sense, call).await.unwrap_or_else(|refusal| refusal) })
}

async fn handoff_call(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let mut args = Arguments { map: &call.arguments, redacted: 0 };
    let context = &call.context;
    let mut work = Work::default();
    for (name, list) in Work::NAMES.into_iter().zip(work.lists_mut()) {
        let (lines, max) = if name == "files" { (Lines::Single, LINE_MAX) } else { (Lines::Multi, TEXT_MAX) };
        *list = args.texts(name, lines, max)?;
    }
    if work.lists().iter().all(|list| list.is_empty()) {
        let message = "A handoff needs an entry: done, in flight, next, promises, decisions or files.";
        return Err(Answer::error("empty", message.into()));
    }

    let items = sense.items(&context.ant).await.map_err(refused)?;
    let by = context.thread.as_str();
    let episode = ItemId::new();
    let mut values = Values::new();
    for (name, entries) in Work::NAMES.into_iter().zip(work.lists()) {
        values.insert(name.into(), listed(entries.clone()));
    }
    values.insert(THREAD.into(), by.into());
    values.insert(EPISODE.into(), episode.as_str().into());
    values.insert(WRITER.into(), writer(context));
    let writes = [
        episode_memory(context, &work).create(episode.clone()),
        Write::Create { id: ItemId::new(), kind: HANDOFF.into(), values },
    ];
    let item = items.write(&writes, by).await.map_err(refused)?.remove(1);
    let handoff = Handoff::of(&item).ok_or_else(|| refused(StoreError::Unavailable("unreadable handoff".into())))?;

    let mut text = format!(
        "Handoff {} recorded, with episode memory {}. Every brief shows the handoff until a checkpoint on this \
         thread, or one naming the handoff in acknowledges, acknowledges it.",
        handoff.id, episode
    );
    text.push_str(&args.redactions());
    let json = json!({"handoff": handoff, "redacted": args.redacted});
    Ok(Answer { text, json, is_error: false })
}

/// The episode memory of the handoff of `work`, written with it. Unlike `sense_remember`, it is not checked for
/// duplicates: each handoff keeps its own.
fn episode_memory(context: &CallContext, work: &Work) -> Memory {
    let summary = [&work.done, &work.next, &work.in_flight, &work.promises, &work.decisions]
        .into_iter()
        .flatten()
        .next()
        .map_or("files touched", String::as_str);
    let title = truncate_chars(&format!("Handoff of thread {}: {summary}", context.thread), MAX_TITLE_CHARS);
    let body = truncate_bytes(&work.text(), MAX_BODY_BYTES);
    let clean = |text: &str, lines, limit| sanitize(text, lines, limit).map(|clean| clean.text);
    let title = clean(&title, Lines::Single, Limit::Chars(MAX_TITLE_CHARS)).unwrap_or(title);
    let body = clean(&body, Lines::Multi, Limit::Bytes(MAX_BODY_BYTES)).unwrap_or(body);
    Memory::new(MemoryKind::Episode, Scope::Own, title, body, context)
}

/// A tool's arguments as sanitized texts, counting the secrets redacted.
struct Arguments<'a> {
    map: &'a Map<String, Value>,
    redacted: usize,
}

impl Arguments<'_> {
    /// The text `name`, sanitized; none when missing, null or blank.
    fn text(&mut self, name: &str, lines: Lines, max: usize) -> Result<Option<String>, Answer> {
        match self.map.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(text)) => self.clean(name, text, lines, max),
            Some(_) => Err(invalid(name, "a text")),
        }
    }

    /// The list of texts `name`, each sanitized, the blank ones left out.
    fn texts(&mut self, name: &str, lines: Lines, max: usize) -> Result<Vec<String>, Answer> {
        let entries = match self.map.get(name) {
            None | Some(Value::Null) => return Ok(Vec::new()),
            Some(Value::Array(entries)) => entries,
            Some(_) => return Err(invalid(name, "a list of texts")),
        };
        if entries.len() > ENTRIES_MAX {
            let message = format!("{name} has {} entries, more than {ENTRIES_MAX}.", entries.len());
            return Err(Answer::error("too_long", message));
        }
        let mut texts = Vec::new();
        for entry in entries {
            let entry = entry.as_str().ok_or_else(|| invalid(name, "a list of texts"))?;
            texts.extend(self.clean(name, entry, lines, max)?);
        }
        Ok(texts)
    }

    /// The list of item ids `name`.
    fn ids(&self, name: &str) -> Result<Vec<ItemId>, Answer> {
        let entries = match self.map.get(name) {
            None | Some(Value::Null) => return Ok(Vec::new()),
            Some(Value::Array(entries)) if entries.len() <= ENTRIES_MAX => entries,
            Some(_) => return Err(invalid(name, &format!("a list of at most {ENTRIES_MAX} ids"))),
        };
        let id = |entry: &Value| entry.as_str().and_then(|id| id.parse().ok());
        entries.iter().map(|entry| id(entry).ok_or_else(|| invalid(name, "a list of ids"))).collect()
    }

    fn clean(&mut self, name: &str, text: &str, lines: Lines, max: usize) -> Result<Option<String>, Answer> {
        let clean = sanitize(text, lines, Limit::Chars(max))
            .map_err(|refused| Answer::error("too_long", format!("{name} is too long: {refused}.")))?;
        self.redacted += clean.redactions.iter().map(|redaction| redaction.count).sum::<usize>();
        let text = clean.text.trim();
        Ok((!text.is_empty()).then(|| text.to_owned()))
    }

    fn redactions(&self) -> String {
        match self.redacted {
            0 => String::new(),
            1 => "\nRedacted 1 secret.".into(),
            n => format!("\nRedacted {n} secrets."),
        }
    }
}

fn invalid(name: &str, expected: &str) -> Answer {
    Answer::error("invalid", format!("{name} must be {expected}."))
}

/// The refusal of a call that found no item, or no data.
fn refused(e: StoreError) -> Answer {
    match e {
        StoreError::NotFound(id) => Answer::error("not_found", format!("No item {id}.")),
        e @ StoreError::Unavailable(_) => Answer::error("unavailable", format!("{e}.")),
        e => Answer::error("unavailable", format!("sense's data is not available: {e}.")),
    }
}

/// `entries` as a value: none when empty.
fn listed(entries: Vec<String>) -> Value {
    if entries.is_empty() { Value::Null } else { entries.into() }
}

fn text(item: &Item, name: &str) -> Option<String> {
    item.values.get(name).and_then(Value::as_str).map(str::to_owned)
}

fn texts(item: &Item, name: &str) -> Vec<String> {
    let entries = item.values.get(name).and_then(Value::as_array).into_iter().flatten();
    entries.filter_map(|entry| entry.as_str().map(str::to_owned)).collect()
}

/// Add `entries` to `out` under `label`, one a line, unless there are none.
fn list(out: &mut String, label: &str, entries: &[String]) {
    if !entries.is_empty() {
        let _ = writeln!(out, "{label}:");
        for entry in entries {
            let _ = writeln!(out, "- {entry}");
        }
    }
}

fn truncate_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    text.chars().take(max - 1).chain(['…']).collect()
}

fn truncate_bytes(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max - '…'.len_utf8();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests;
