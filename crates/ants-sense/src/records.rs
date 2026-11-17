//! The ant's self-model and its relationship records (requirement ants-x39hk.6.6): what it holds true of itself, and of
//! each human or ant it works with. Unlike memories, which pile up and are superseded, each is one record replaced
//! whole by compare-and-set on its revision, its earlier revisions kept in the log: the self-model, an item of kind
//! [`SELF_MODEL`], and one item of kind [`RELATIONSHIP`] per person, each id named after its scope ([`record_id`]) so
//! writers starting one at once start the same item.
//!
//! The ant writes them with `sense_remember`, its `kind` `self_model` or `relationship` ([`remember`]); they take
//! effect at once, with no dream to confirm them. The brief shows the self-model after the identity and the records of
//! the thread's participants after the tasks; `sense_recall` lists those of the people it searches and reads one by id.

use std::fmt::Write as _;

use ants_proto::provider::{CallContext, ToolCall};
use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::Sense;
use crate::items::{Item, ItemId};
use crate::memory::{
    MAX_TITLE_CHARS, Scope, Trust, holds_recalled_block, holds_recalled_marker, invalid, redacted, sanitized,
    unavailable,
};
use crate::sanitize::{Limit, sanitize_body, sanitize_title};
use crate::store::{Items, StoreError};
use crate::tools::Answer;

/// The kind of the ant's self-model; an ant has one.
pub const SELF_MODEL: &str = "self_model";
/// The kind of a relationship record; an ant has one per human or ant.
pub const RELATIONSHIP: &str = "relationship";
/// The most bytes of a record's body: each shows in the briefs it applies to.
pub const MAX_RECORD_BYTES: usize = 2_000;

/// The values of a self-model or relationship record, by their names in the item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    /// `self` for the self-model, `human:<name>` or `ant:<name>` for a relationship.
    pub scope: Scope,
    /// A line summing it up, at most [`MAX_TITLE_CHARS`] characters, sanitized.
    pub title: String,
    /// At most [`MAX_RECORD_BYTES`] bytes, sanitized.
    pub body: String,
    /// Who wrote this revision, where and when, as ants told for the call: never taken from the ant's text.
    pub writer: CallContext,
    pub trust: Trust,
}

/// A record and the item holding it.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordItem {
    pub item: Item,
    pub record: Record,
}

impl RecordItem {
    /// The record `item` holds; none when it holds none.
    pub fn of(item: Item) -> Option<Self> {
        if item.kind != SELF_MODEL && item.kind != RELATIONSHIP {
            return None;
        }
        let record = serde_json::from_value(Value::Object(item.values.clone())).ok()?;
        Some(Self { item, record })
    }

    pub fn id(&self) -> &ItemId {
        &self.item.id
    }

    /// Its id, scope, revision and whether it is untrusted, then its title.
    pub fn head(&self) -> String {
        let untrusted = if self.record.trust == Trust::Untrusted { ", untrusted" } else { "" };
        format!(
            "- {} ({}, revision {}{untrusted}): {}",
            self.id(),
            self.record.scope,
            self.item.revision,
            self.record.title
        )
    }

    /// [`RecordItem::head`], then its body, indented.
    pub fn text(&self) -> String {
        format!("{}\n  {}", self.head(), self.record.body.replace('\n', "\n  "))
    }

    /// It in full: what it is, its text and who wrote this revision.
    pub fn full(&self) -> String {
        let record = &self.record;
        let writer = &record.writer;
        let model = writer.model.as_ref().map_or_else(String::new, |model| format!(", model {model}"));
        format!(
            "{} ({}, {}), trust {}, revision {}\n{}\n{}\nWritten {} by {} (identity {}) in thread {}, with {}{model}, \
             on {}.",
            self.id(),
            self.item.kind,
            record.scope,
            record.trust.name(),
            self.item.revision,
            record.title,
            record.body,
            writer.at.to_rfc3339_opts(SecondsFormat::Secs, true),
            writer.ant,
            writer.identity,
            writer.thread,
            writer.harness,
            writer.machine
        )
    }

    /// Its values with its id, kind, revision and times.
    pub fn json(&self) -> Value {
        let mut json = Value::Object(self.item.values.clone());
        json["id"] = json!(self.id());
        json["kind"] = json!(self.item.kind);
        json["revision"] = json!(self.item.revision);
        json["created"] = json!(self.item.created.at);
        json["updated"] = json!(self.item.updated.at);
        json
    }
}

/// The id of the record of `scope`: the self-model for `self`, else the relationship record of a human or ant; none
/// for a repository.
pub fn record_id(scope: &Scope) -> Option<ItemId> {
    match scope {
        Scope::Own => Some(ItemId::named(SELF_MODEL, "self")),
        Scope::Human(_) | Scope::Ant(_) => Some(ItemId::named(RELATIONSHIP, &scope.to_string())),
        Scope::Repo(_) => None,
    }
}

/// The ant's self-model in `items`; none before it wrote one.
pub async fn self_model(items: &dyn Items) -> Result<Option<RecordItem>, StoreError> {
    let id = record_id(&Scope::Own).expect("a self-model");
    Ok(items.get(&id).await?.and_then(RecordItem::of))
}

/// The relationship records in `items` of the people among `scopes`, in their order.
pub async fn relationships(items: &dyn Items, scopes: &[Scope]) -> Result<Vec<RecordItem>, StoreError> {
    let mut found = Vec::new();
    for scope in scopes.iter().filter(|scope| matches!(scope, Scope::Human(_) | Scope::Ant(_))) {
        let id = record_id(scope).expect("a person's record");
        found.extend(items.get(&id).await?.and_then(RecordItem::of));
    }
    Ok(found)
}

/// Whether `kind`, the kind a `sense_remember` call names, is a record's rather than a memory's.
pub fn is_record(kind: &str) -> bool {
    kind == SELF_MODEL || kind == RELATIONSHIP
}

/// Answers a `sense_remember` call of `kind` [`SELF_MODEL`] or [`RELATIONSHIP`]: replace the record from the revision
/// read, none or 0 for the first.
pub(crate) async fn remember(sense: &Sense, call: &ToolCall, kind: &str) -> Result<Answer, Answer> {
    let arguments = &call.arguments;
    let scope = match (kind, text(arguments, "scope")?.map(str::parse::<Scope>).transpose().map_err(invalid)?) {
        (SELF_MODEL, None | Some(Scope::Own)) => Scope::Own,
        (SELF_MODEL, Some(_)) => return Err(invalid("The self-model's scope is self.".into())),
        (_, Some(scope @ (Scope::Human(_) | Scope::Ant(_)))) => scope,
        (_, _) => return Err(invalid("A relationship record's scope is human:<name> or ant:<name>.".into())),
    };
    let title = text(arguments, "title")?;
    let title =
        sanitized("title", title, sanitize_title(title.unwrap_or_default().trim(), Limit::Chars(MAX_TITLE_CHARS)))?;
    let body = text(arguments, "body")?;
    let body = sanitized("body", body, sanitize_body(body.unwrap_or_default().trim(), Limit::Bytes(MAX_RECORD_BYTES)))?;
    if holds_recalled_block(&title.text) || holds_recalled_block(&body.text) {
        let message = "Recalled memories are not stored again: write what was learned in new words.";
        return Err(Answer::error("recalled_block", message.into()));
    }
    let revision = match arguments.get("revision") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_u64().ok_or_else(|| invalid("revision must be a revision number.".into()))?),
    };
    let context = &call.context;
    let untrusted = if !context.trusted() {
        Some("the thread came from a webhook")
    } else if let Some(quotes) = text(arguments, "quotes")? {
        match quotes {
            "recalled" => Some("it quotes recalled memories"),
            "external" => Some("it quotes outside content"),
            other => return Err(invalid(format!("quotes must be recalled or external, not {other:?}."))),
        }
    } else if holds_recalled_marker(&title.text) || holds_recalled_marker(&body.text) {
        Some("it quotes recalled memories")
    } else {
        None
    };
    let mut redactions = title.redactions.clone();
    redactions.extend(body.redactions.iter().copied());
    let record = Record {
        scope,
        title: title.text,
        body: body.text,
        writer: context.clone(),
        trust: if untrusted.is_some() { Trust::Untrusted } else { Trust::Own },
    };
    let values = match serde_json::to_value(&record) {
        Ok(Value::Object(values)) => values,
        _ => unreachable!("a record is an object"),
    };

    let items = sense.items(&context.ant).await.map_err(unavailable)?;
    let id = record_id(&record.scope).expect("a record's scope");
    let by = context.thread.as_str();
    let item = match items.put(&id, kind, values, Some(revision.unwrap_or(0)), by).await {
        Ok(item) => item,
        Err(StoreError::Conflict { current, .. }) => return Err(conflict(revision, *current)),
        Err(StoreError::NotFound(_)) => {
            let message = format!(
                "Nothing to replace: {} is not written yet; write it without a revision to start it.",
                name(&record.scope)
            );
            return Err(Answer::error("not_found", message));
        }
        Err(e) => return Err(unavailable(e)),
    };
    let (redacted_text, redacted_json) = redacted(&redactions);
    let mut text = format!("Saved {} {id}: revision {}.", name(&record.scope), item.revision);
    if let Some(why) = untrusted {
        let _ = write!(text, "\nStored as untrusted: {why}.");
    }
    text.push_str(&redacted_text);
    let json = json!({
        "id": id,
        "kind": kind,
        "revision": item.revision,
        "scope": record.scope,
        "trust": record.trust,
        "redactions": redacted_json,
    });
    Ok(Answer { text, json, is_error: false })
}

/// What the record of `scope` is called.
fn name(scope: &Scope) -> String {
    match scope {
        Scope::Own => "the self-model".into(),
        scope => format!("the relationship record of {scope}"),
    }
}

fn text<'a>(arguments: &'a Map<String, Value>, name: &str) -> Result<Option<&'a str>, Answer> {
    match arguments.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(invalid(format!("{name} must be a string."))),
    }
}

/// The refusal of a write from `revision`, none standing for the first, of the record now `current`, which it carries.
fn conflict(revision: Option<u64>, current: Item) -> Answer {
    let Some(current) = RecordItem::of(current) else {
        return Answer::error("conflict", "The record changed meanwhile: try again.".into());
    };
    let (name, at) = (name(&current.record.scope), current.item.revision);
    let (code, message) = match revision {
        None | Some(0) => (
            "revision_required",
            format!("Revision {at} of {name} exists: pass revision {at} to replace it, keeping what still holds."),
        ),
        Some(revision) => (
            "conflict",
            format!(
                "Revision {at} of {name} is the latest, not {revision}: write it again from revision {at}, keeping \
                 what still holds."
            ),
        ),
    };
    let mut answer = Answer::error(code, message);
    answer.text = format!("{}\n{}", answer.text, crate::memory::framed(&current.text()));
    answer.json["record"] = current.json();
    answer
}

#[cfg(test)]
mod tests;
