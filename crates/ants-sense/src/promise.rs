//! Personal commitments and their earlier terms. Renegotiating closes the old commitment and creates its
//! replacement in the same event, preserving both the earlier item and an explicit history on the replacement.

use std::path::Path;

use ants_proto::ThreadId;
use ants_proto::provider::{BoxFuture, CallContext, ToolCall};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::brief::Listing;
use crate::items::{Item, ItemId, Values, changed, readable, stamp};
use crate::sanitize::{Limit, Lines, sanitize};
use crate::store::{Items, StoreError, Write};
use crate::{Answer, Sense, remind};

pub const KIND: &str = "promise";
/// The most characters of a commitment's title.
pub const MAX_TITLE_CHARS: usize = 2_000;
/// The most characters naming a commitment's promisee.
pub const MAX_PROMISEE_CHARS: usize = 500;
/// The most characters of the reason a commitment is resolved for.
pub const MAX_RESOLUTION_CHARS: usize = 2_000;

/// The tool call listing the open commitments, which the brief's section of them ends with when cut.
const LIST: &str = "sense_promise action=list";

/// A commitment, as its item keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Promise {
    pub title: String,
    pub promisee: String,
    pub due: DateTime<Utc>,
    pub status: Status,
    /// The thread it was made in.
    pub source_thread: ThreadId,
    /// The context of the latest tool call changing it.
    pub writer: CallContext,
    /// The terms of the commitments it renegotiates, the earliest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<Earlier>,
    /// The commitment it renegotiates.
    pub supersedes: Option<ItemId>,
    /// The reminder of its due time.
    pub reminder: Option<ItemId>,
    /// Why it was resolved, and when.
    pub reason: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    /// The commitment renegotiating it.
    pub replacement: Option<ItemId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    Kept,
    Broken,
    Released,
    Renegotiated,
}

impl Status {
    pub fn name(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Kept => "kept",
            Self::Broken => "broken",
            Self::Released => "released",
            Self::Renegotiated => "renegotiated",
        }
    }
}

/// The terms of a commitment renegotiated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Earlier {
    pub id: ItemId,
    pub title: String,
    pub promisee: String,
    pub due: DateTime<Utc>,
    pub source_thread: ThreadId,
    pub renegotiated_at: DateTime<Utc>,
    pub reason: Option<String>,
}

impl Promise {
    /// The commitment `item` holds.
    pub fn of(item: &Item) -> Result<Self, StoreError> {
        serde_json::from_value(Value::Object(item.values.clone()))
            .map_err(|error| StoreError::Unavailable(format!("invalid commitment {}: {error}", item.id)))
    }

    /// The values of its item, null for those it lacks.
    pub fn values(&self) -> Values {
        crate::items::values_of(self)
    }

    /// Whether it is open past its due time.
    pub fn overdue(&self, now: DateTime<Utc>) -> bool {
        self.status == Status::Open && self.due <= now
    }
}

/// Open commitments, overdue first, then by due time and creation time.
pub async fn open(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<(Item, Promise)>, StoreError> {
    let mut promises =
        readable(items.list_by(KIND, "status", Some(&json!(Status::Open)), usize::MAX).await?, Promise::of);
    promises.sort_by_key(|(item, promise)| (promise.due > now, promise.due, item.created.at, item.id.clone()));
    Ok(promises)
}

/// Whether an open commitment, commitment `id` alone when given, is past its due time.
pub async fn overdue(items: &dyn Items, id: Option<&ItemId>, now: DateTime<Utc>) -> Result<bool, StoreError> {
    let open = open(items, now).await?;
    Ok(open.iter().any(|(item, promise)| id.is_none_or(|id| *id == item.id) && promise.overdue(now)))
}

/// The brief's lines of the open commitments.
pub(crate) async fn brief(items: &dyn Items, now: DateTime<Utc>) -> Result<Listing, StoreError> {
    let lines = open(items, now)
        .await?
        .iter()
        .map(|(item, promise)| {
            format!(
                "- {} (revision {}, open{}): {} — to {}, due {}; source thread {}",
                item.id,
                item.revision,
                if promise.overdue(now) { ", overdue" } else { "" },
                promise.title,
                promise.promisee,
                stamp(promise.due),
                promise.source_thread
            )
        })
        .collect();
    Ok(Listing { lines, more: LIST })
}

pub fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "action": {"enum": ["create", "list", "show", "keep", "break", "release", "renegotiate"],
                       "description": "Without arguments, list open commitments, overdue first."},
            "id": {"type": "string"},
            "title": {"type": "string", "minLength": 1, "maxLength": MAX_TITLE_CHARS},
            "promisee": {"type": "string", "minLength": 1, "maxLength": MAX_PROMISEE_CHARS},
            "due": {"type": "string", "format": "date-time", "description": "RFC 3339 due time, normalized to UTC."},
            "revision": {"type": "integer", "minimum": 1, "description": "Optional compare-and-set revision."},
            "reason": {"type": "string", "maxLength": MAX_RESOLUTION_CHARS}
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Create,
    List,
    Show,
    Keep,
    Break,
    Release,
    Renegotiate,
}

/// The arguments of a `sense_promise` call.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    action: Option<Action>,
    id: Option<ItemId>,
    title: Option<String>,
    promisee: Option<String>,
    due: Option<DateTime<Utc>>,
    revision: Option<u64>,
    reason: Option<String>,
}

pub fn promise<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { handle(sense, call).await.unwrap_or_else(|answer| answer) })
}

async fn handle(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let request: Request = crate::tools::arguments(call)?;
    let action = request.action.unwrap_or(match (&request.title, &request.id) {
        (Some(_), _) => Action::Create,
        (None, Some(_)) => Action::Show,
        (None, None) => Action::List,
    });
    if request.revision == Some(0) {
        return Err(Answer::invalid("revision must be positive."));
    }
    let items = sense.items(&call.context.ant).await.map_err(refused)?;
    let now = call.context.at;
    let workdir = sense.workdir(&call.context);
    if action == Action::List {
        let promises = open(&*items, now).await.map_err(refused)?;
        let text = if promises.is_empty() {
            "No open commitments.".into()
        } else {
            promises.iter().map(|(item, promise)| line(item, promise, now)).collect::<Vec<_>>().join("\n")
        };
        let promises: Vec<_> = promises.into_iter().map(|(item, _)| item).collect();
        return Ok(Answer { text, json: json!({"promises": promises}), is_error: false });
    }
    if action == Action::Create {
        let writes = creation(ItemId::new(), terms(call, &request, None)?, now, workdir.as_deref())?;
        let item = remind::write_creations(&*items, &writes, call.context.thread.as_str(), now)
            .await
            .map_err(refused)?
            .remove(0);
        return answer(item, now);
    }
    let id = request.id.as_ref().ok_or_else(|| Answer::invalid("id must name a commitment."))?;
    let item = items
        .get(id)
        .await
        .map_err(refused)?
        .filter(|item| item.kind == KIND)
        .ok_or_else(|| Answer::error("not_found", format!("No commitment {id}.")))?;
    if let Some(revision) = request.revision
        && revision != item.revision
    {
        return Err(refused(StoreError::Conflict { expected: revision, current: Box::new(item) }));
    }
    if action == Action::Show {
        return answer(item, now);
    }
    let promise = Promise::of(&item).map_err(refused)?;
    if promise.status != Status::Open {
        return Err(Answer::error("closed", format!("Commitment {id} is {}.", promise.status.name())));
    }
    resolve(&*items, call, &request, &item, promise, action, workdir.as_deref()).await
}

async fn resolve(
    items: &dyn Items,
    call: &ToolCall,
    request: &Request,
    item: &Item,
    promise: Promise,
    action: Action,
    workdir: Option<&Path>,
) -> Result<Answer, Answer> {
    let now = call.context.at;
    let reason = text(request.reason.as_deref(), "reason", MAX_RESOLUTION_CHARS)?;
    let mut resolved = promise.clone();
    resolved.reason.clone_from(&reason);
    resolved.resolved_at = Some(now);
    resolved.writer = call.context.clone();
    resolved.status = match action {
        Action::Keep => Status::Kept,
        Action::Break => Status::Broken,
        Action::Release => Status::Released,
        Action::Renegotiate => Status::Renegotiated,
        Action::Create | Action::List | Action::Show => return Err(Answer::invalid("No resolution of a commitment.")),
    };
    let mut writes = Vec::new();
    if let Some(reminder) = &promise.reminder {
        writes.extend(remind::dropped(items, reminder).await.map_err(refused)?);
    }
    if action == Action::Renegotiate {
        let mut replacement = terms(call, request, Some(&promise))?;
        replacement.history = promise.history.clone();
        replacement.history.push(Earlier {
            id: item.id.clone(),
            title: promise.title,
            promisee: promise.promisee,
            due: promise.due,
            source_thread: promise.source_thread,
            renegotiated_at: now,
            reason,
        });
        replacement.supersedes = Some(item.id.clone());
        let replacement_id = ItemId::new();
        resolved.replacement = Some(replacement_id.clone());
        writes.extend(creation(replacement_id, replacement, now, workdir)?);
    }
    let values = changed(&Promise::of(item).map_err(refused)?.values(), resolved.values());
    writes.insert(0, Write::Change { id: item.id.clone(), values, expected: Some(item.revision) });
    let mut written =
        remind::write_creations(items, &writes, call.context.thread.as_str(), now).await.map_err(refused)?;
    let previous = written.remove(0);
    if action == Action::Renegotiate {
        let replacement = written.into_iter().find(|item| item.kind == KIND).expect("replacement promise");
        let mut result = answer(replacement, now)?;
        result.json["previous"] = json!(previous);
        return Ok(result);
    }
    answer(previous, now)
}

/// The creation of commitment `id` holding `promise`, with its reminder.
fn creation(
    id: ItemId,
    mut promise: Promise,
    now: DateTime<Utc>,
    workdir: Option<&Path>,
) -> Result<Vec<Write>, Answer> {
    let reminder =
        remind::for_commitment(&id, &promise.title, promise.due, &promise.writer, workdir, now).map_err(refused)?;
    promise.reminder = Some(reminder.id().clone());
    Ok(vec![Write::Create { id, kind: KIND.into(), values: promise.values() }, reminder])
}

/// The terms of an open commitment made by `call`, those it does not give the `previous` commitment's.
fn terms(call: &ToolCall, request: &Request, previous: Option<&Promise>) -> Result<Promise, Answer> {
    let given = |given: &Option<String>, name: &str, max: usize, previous: Option<&String>| {
        text(given.as_deref(), name, max)?
            .or_else(|| previous.cloned())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Answer::invalid(format!("{name} must be nonempty.")))
    };
    let title = given(&request.title, "title", MAX_TITLE_CHARS, previous.map(|promise| &promise.title))?;
    let promisee = given(&request.promisee, "promisee", MAX_PROMISEE_CHARS, previous.map(|promise| &promise.promisee))?;
    let due = request
        .due
        .or_else(|| previous.map(|promise| promise.due))
        .ok_or_else(|| Answer::invalid("due must be an RFC 3339 time."))?;
    Ok(Promise {
        title,
        promisee,
        due,
        status: Status::Open,
        source_thread: call.context.thread.clone(),
        writer: call.context.clone(),
        history: Vec::new(),
        supersedes: None,
        reminder: None,
        reason: None,
        resolved_at: None,
        replacement: None,
    })
}

/// The text `given` for argument `name`, sanitized to a line of at most `max` characters.
fn text(given: Option<&str>, name: &str, max: usize) -> Result<Option<String>, Answer> {
    given
        .map(|text| sanitize(text, Lines::Single, Limit::Chars(max)).map(|text| text.text))
        .transpose()
        .map_err(|e| Answer::invalid(format!("{name}: {e}.")))
}

fn line(item: &Item, promise: &Promise, now: DateTime<Utc>) -> String {
    format!(
        "{} [{}{}] {} — to {}, due {}",
        item.id,
        promise.status.name(),
        if promise.overdue(now) { ", overdue" } else { "" },
        promise.title,
        promise.promisee,
        stamp(promise.due)
    )
}

fn answer(item: Item, now: DateTime<Utc>) -> Result<Answer, Answer> {
    let text = line(&item, &Promise::of(&item).map_err(refused)?, now);
    Ok(Answer { text, json: json!({"promise": item}), is_error: false })
}

/// The refusal of a call its store failed, showing the commitment found at another revision.
fn refused(error: StoreError) -> Answer {
    let mut answer = Answer::from(error);
    if answer.json["current"]["kind"] == KIND {
        answer.json["promise"] = answer.json["current"].clone();
    }
    answer
}

#[cfg(test)]
mod tests;
