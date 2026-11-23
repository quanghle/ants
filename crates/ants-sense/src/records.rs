//! The ant's self-model and its relationship records (requirement ants-x39hk.6.6): what it holds true of itself, and of
//! each human or ant it works with. Unlike memories, which pile up and are superseded, each is one record replaced
//! whole by compare-and-set on its revision, its earlier revisions kept in the log: the self-model, an item of kind
//! [`SELF_MODEL`], and one item of kind [`RELATIONSHIP`] per person, each id named after its scope ([`record_id`]) so
//! writers starting one at once start the same item.
//!
//! The ant writes them with `sense_identity`, giving `self_model`, or `relationship` and `who` (`replace`); they
//! take effect at once, with no dream to confirm them. `sense_identity` shows the self-model and the relationship
//! records by title, and with `history` and `who` every revision of one (`history`); the brief shows the self-model
//! after the identity and the records of the thread's participants after the tasks; `sense_recall` lists those of the
//! people it searches and reads one by id.

use std::fmt::Write as _;

use ants_proto::SenseId;
use ants_proto::provider::CallContext;
use chrono::SecondsFormat;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::items::{Item, ItemId, ItemValues};
use crate::memory::{
    MAX_TITLE_CHARS, Quotes, Scope, Trust, framed, holds_recalled_block, holds_recalled_marker, redacted, sanitized,
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
    /// The sense identity a clone inherited it from ([`crate::fork`]), until the clone writes it anew.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inherited: Option<SenseId>,
}

impl ItemValues for Record {
    const KIND: &str = "record";
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
        let record = Record::of(&item).ok()?;
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
        let mut text = format!(
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
        );
        if let Some(from) = &record.inherited {
            let _ = write!(text, "\nInherited from sense identity {from} on clone.");
        }
        text
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

/// A change of the record of `scope`, replacing it whole: what a `sense_identity` call with `self_model`, or
/// `relationship` and `who`, gives.
pub(crate) struct Change<'a> {
    /// `self` for the self-model, `human:<name>` or `ant:<name>` for a relationship.
    pub scope: Scope,
    pub title: Option<&'a str>,
    pub body: &'a str,
    /// The revision replaced: none or 0 for the first.
    pub revision: Option<u64>,
    pub quotes: Option<Quotes>,
}

/// Replace the record of `change` from the revision read, for the ant of `context`.
pub(crate) async fn replace(items: &dyn Items, context: &CallContext, change: Change<'_>) -> Result<Answer, Answer> {
    let kind = match change.scope {
        Scope::Own => SELF_MODEL,
        Scope::Human(_) | Scope::Ant(_) => RELATIONSHIP,
        Scope::Repo(_) => return Err(Answer::invalid("A record is of self, a human or an ant.")),
    };
    let title = change.title;
    let title =
        sanitized("title", title, sanitize_title(title.unwrap_or_default().trim(), Limit::Chars(MAX_TITLE_CHARS)))?;
    let body = sanitized(kind, Some(change.body), sanitize_body(change.body.trim(), Limit::Bytes(MAX_RECORD_BYTES)))?;
    if holds_recalled_block(&title.text) || holds_recalled_block(&body.text) {
        let message = "Recalled memories are not stored again: write what was learned in new words.";
        return Err(Answer::error("recalled_block", message.into()));
    }
    let revision = change.revision;
    let untrusted = if !context.trusted() {
        Some("the thread came from a webhook")
    } else if let Some(quotes) = change.quotes {
        Some(quotes.why())
    } else if holds_recalled_marker(&title.text) || holds_recalled_marker(&body.text) {
        Some("it quotes recalled memories")
    } else {
        None
    };
    let mut redactions = title.redactions.clone();
    redactions.extend(body.redactions.iter().copied());
    let record = Record {
        scope: change.scope,
        title: title.text,
        body: body.text,
        writer: context.clone(),
        trust: if untrusted.is_some() { Trust::Untrusted } else { Trust::Own },
        inherited: None,
    };
    let mut values = record.values();
    values.insert(crate::fork::INHERITED.into(), Value::Null);

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
        Err(e) => return Err(e.into()),
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

/// Every revision of the record of `scope` kept in the log, the newest first, each with when, where and by whom it was
/// written: what a `sense_identity` call with `history` and `who` shows.
pub(crate) async fn history(items: &dyn Items, scope: &Scope) -> Result<Answer, Answer> {
    let Some(id) = record_id(scope) else {
        return Err(Answer::invalid(
            "A record is of self, a human or an ant: who is self, human:<name> or ant:<name>.",
        ));
    };
    let versions = crate::proposal::versions(items, &id).await?;
    let records: Vec<(u64, Record)> = versions
        .into_iter()
        .filter_map(|(revision, _, values)| Some((revision, serde_json::from_value(Value::Object(values)).ok()?)))
        .collect();
    let (name, mut json) = (name(scope), Vec::new());
    if records.is_empty() {
        let text = format!("No revision of {name} yet.");
        return Ok(Answer { text, json: json!({"id": id, "scope": scope, "versions": []}), is_error: false });
    }
    let mut lines = Vec::new();
    for (revision, record) in records.iter().rev() {
        let writer = &record.writer;
        let untrusted = if record.trust == Trust::Untrusted { ", untrusted" } else { "" };
        let mut line = format!(
            "- Revision {revision}, {} by {} in thread {}{untrusted}: {}",
            crate::proposal::time(writer.at),
            writer.ant,
            writer.thread,
            record.title
        );
        if let Some(from) = &record.inherited {
            let _ = write!(line, " (inherited from sense identity {from} on clone)");
        }
        let _ = write!(line, "\n  {}", record.body.replace('\n', "\n  "));
        lines.push(line);
        let mut version = serde_json::to_value(record).expect("a record is an object");
        version["revision"] = json!(revision);
        json.push(version);
    }
    let count = match records.len() {
        1 => "1 revision".to_owned(),
        n => format!("{n} revisions"),
    };
    let text = format!("{count} of {name}, the newest first:\n{}", framed(&lines.join("\n")));
    Ok(Answer { text, json: json!({"id": id, "scope": scope, "versions": json}), is_error: false })
}

/// What the record of `scope` is called.
fn name(scope: &Scope) -> String {
    match scope {
        Scope::Own => "the self-model".into(),
        scope => format!("the relationship record of {scope}"),
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
    answer.text = format!("{}\n{}", answer.text, framed(&current.text()));
    answer.json["record"] = current.json();
    answer
}

#[cfg(test)]
mod tests;
