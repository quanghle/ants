//! Personal commitments and their earlier terms. Renegotiating closes the old commitment and creates its
//! replacement in the same event, preserving both the earlier item and an explicit history on the replacement.

use ants_proto::provider::{BoxFuture, ToolCall};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::items::{Item, ItemId, Values};
use crate::memory::{WRITER, writer};
use crate::sanitize::{Limit, Lines, sanitize};
use crate::store::{Items, StoreError, Write};
use crate::{Answer, Sense};

pub const KIND: &str = "promise";
/// The most characters of a commitment's title.
pub const MAX_TITLE_CHARS: usize = 2_000;
/// The most characters naming a commitment's promisee.
pub const MAX_PROMISEE_CHARS: usize = 500;
/// The most characters of the reason a commitment is resolved for.
pub const MAX_RESOLUTION_CHARS: usize = 2_000;

/// Open commitments, overdue first, then by due time and creation time.
pub async fn open(items: &dyn Items, now: DateTime<Utc>) -> Result<Vec<Item>, StoreError> {
    let mut promises = items.list_by(KIND, "status", Some(&json!("open")), usize::MAX).await?;
    promises.sort_by_key(|item| {
        let due = item.text("due").parse::<DateTime<Utc>>().unwrap_or(DateTime::<Utc>::MAX_UTC);
        (due > now, due, item.created.at, item.id.clone())
    });
    Ok(promises)
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
    if action == Action::List {
        let promises = open(&*items, call.context.at).await.map_err(refused)?;
        let text = if promises.is_empty() {
            "No open commitments.".into()
        } else {
            promises.iter().map(|item| line(item, call.context.at)).collect::<Vec<_>>().join("\n")
        };
        return Ok(Answer { text, json: json!({"promises": promises}), is_error: false });
    }
    if action == Action::Create {
        let values = terms(call, &request, None)?;
        let id = ItemId::new();
        let writes = creation(id, values, call.context.at, sense.workdir(&call.context))?;
        let item = crate::remind::write_creations(&*items, &writes, call.context.thread.as_str(), call.context.at)
            .await
            .map_err(refused)?
            .remove(0);
        return Ok(answer(item, call.context.at));
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
        return Ok(answer(item, call.context.at));
    }
    if item.text("status") != "open" {
        return Err(Answer::error("closed", format!("Commitment {id} is {}.", item.text("status"))));
    }
    resolve(&*items, call, &request, item, action, sense.workdir(&call.context)).await
}

async fn resolve(
    items: &dyn Items,
    call: &ToolCall,
    request: &Request,
    item: Item,
    action: Action,
    workdir: Option<std::path::PathBuf>,
) -> Result<Answer, Answer> {
    let id = item.id.clone();
    let reason = text(request.reason.as_deref(), "reason", MAX_RESOLUTION_CHARS)?;
    let mut changed = Values::new();
    changed.insert("reason".into(), reason.clone().into());
    changed.insert("resolved_at".into(), json!(call.context.at));
    changed.insert(WRITER.into(), writer(&call.context));
    let status = match action {
        Action::Keep => "kept",
        Action::Break => "broken",
        Action::Release => "released",
        Action::Renegotiate => "renegotiated",
        Action::Create | Action::List | Action::Show => return Err(Answer::invalid("No resolution of a commitment.")),
    };
    changed.insert("status".into(), status.into());
    let mut writes = vec![Write::Change { id: id.clone(), values: changed, expected: Some(item.revision) }];
    if let Some(reminder) = item.values.get("reminder").and_then(Value::as_str)
        && let Ok(reminder) = reminder.parse::<ItemId>()
        && let Some(reminder) = items.get(&reminder).await.map_err(refused)?
        && reminder.kind == crate::remind::KIND
    {
        writes.push(Write::Change {
            id: reminder.id,
            values: json!({"status":"dropped","next_due":null,"pending":null}).as_object().unwrap().clone(),
            expected: Some(reminder.revision),
        });
    }
    if action == Action::Renegotiate {
        let mut replacement = terms(call, request, Some(&item))?;
        let mut history = item.values.get("history").and_then(Value::as_array).cloned().unwrap_or_default();
        history.push(json!({
            "id": item.id, "title": item.text("title"), "promisee": item.text("promisee"),
            "due": item.values.get("due"), "source_thread": item.text("source_thread"),
            "renegotiated_at": call.context.at, "reason": reason
        }));
        replacement.insert("history".into(), history.into());
        replacement.insert("supersedes".into(), json!(id));
        let replacement_id = ItemId::new();
        if let Write::Change { values, .. } = &mut writes[0] {
            values.insert("replacement".into(), json!(replacement_id));
        }
        writes.extend(creation(replacement_id, replacement, call.context.at, workdir)?);
    }
    let mut changed = crate::remind::write_creations(items, &writes, call.context.thread.as_str(), call.context.at)
        .await
        .map_err(refused)?;
    let previous = changed.remove(0);
    if action == Action::Renegotiate {
        let replacement = changed.into_iter().find(|item| item.kind == KIND).expect("replacement promise");
        let mut result = answer(replacement, call.context.at);
        result.json["previous"] = json!(previous);
        return Ok(result);
    }
    Ok(answer(previous, call.context.at))
}

fn creation(
    id: ItemId,
    mut values: Values,
    now: DateTime<Utc>,
    workdir: Option<std::path::PathBuf>,
) -> Result<Vec<Write>, Answer> {
    let mut reminder = crate::remind::promise_creation(&id, &values, now).map_err(refused)?;
    if let Some(workdir) = workdir
        && let Write::Create { values: reminder_values, .. } = &mut reminder
    {
        reminder_values.insert("workdir".into(), workdir.to_string_lossy().as_ref().into());
        if let Some(machine) = values.get(WRITER).and_then(|context| context.get("machine")) {
            reminder_values.insert("workdir_machine".into(), machine.clone());
        }
    }
    values.insert("reminder".into(), json!(reminder.id()));
    Ok(vec![Write::Create { id, kind: KIND.into(), values }, reminder])
}

fn terms(call: &ToolCall, request: &Request, previous: Option<&Item>) -> Result<Values, Answer> {
    let mut values = Values::new();
    let given = [("title", &request.title, MAX_TITLE_CHARS), ("promisee", &request.promisee, MAX_PROMISEE_CHARS)];
    for (name, given, max) in given {
        let value = text(given.as_deref(), name, max)?
            .or_else(|| previous.map(|item| item.text(name).to_owned()))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Answer::invalid(format!("{name} must be nonempty.")))?;
        values.insert(name.into(), value.into());
    }
    let due = request
        .due
        .or_else(|| previous.and_then(|item| item.text("due").parse().ok()))
        .ok_or_else(|| Answer::invalid("due must be an RFC 3339 time."))?;
    values.insert("due".into(), json!(due));
    values.insert("status".into(), "open".into());
    values.insert("source_thread".into(), call.context.thread.as_str().into());
    values.insert(WRITER.into(), writer(&call.context));
    Ok(values)
}

/// The text `given` for argument `name`, sanitized to a line of at most `max` characters.
fn text(given: Option<&str>, name: &str, max: usize) -> Result<Option<String>, Answer> {
    given
        .map(|text| sanitize(text, Lines::Single, Limit::Chars(max)).map(|text| text.text))
        .transpose()
        .map_err(|e| Answer::invalid(format!("{name}: {e}.")))
}

fn line(item: &Item, now: DateTime<Utc>) -> String {
    let overdue = item.text("due").parse::<DateTime<Utc>>().is_ok_and(|due| due <= now);
    format!(
        "{} [{}{}] {} — to {}, due {}",
        item.id,
        item.text("status"),
        if overdue && item.text("status") == "open" { ", overdue" } else { "" },
        item.text("title"),
        item.text("promisee"),
        item.text("due")
    )
}

fn answer(item: Item, now: DateTime<Utc>) -> Answer {
    Answer { text: line(&item, now), json: json!({"promise": item}), is_error: false }
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
