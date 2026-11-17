//! Personal commitments and their earlier terms. Renegotiating closes the old commitment and creates its
//! replacement in the same event, preserving both the earlier item and an explicit history on the replacement.

use ants_proto::provider::{BoxFuture, ToolCall};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::items::{Item, ItemId, Values};
use crate::memory::{WRITER, writer};
use crate::sanitize::{Limit, Lines, sanitize};
use crate::store::{Items, StoreError, Write};
use crate::{Answer, Sense};

pub const KIND: &str = "promise";

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
            "title": {"type": "string", "minLength": 1, "maxLength": 2000},
            "promisee": {"type": "string", "minLength": 1, "maxLength": 500},
            "due": {"type": "string", "format": "date-time", "description": "RFC 3339 due time, normalized to UTC."},
            "revision": {"type": "integer", "minimum": 1, "description": "Optional compare-and-set revision."},
            "reason": {"type": "string", "maxLength": 2000}
        }
    })
}

pub fn promise<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { handle(sense, call).await.unwrap_or_else(|answer| answer) })
}

async fn handle(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let args = &call.arguments;
    for key in args.keys() {
        if !["action", "id", "title", "promisee", "due", "revision", "reason"].contains(&key.as_str()) {
            return Err(invalid(&format!("Unknown promise argument {key:?}.")));
        }
    }
    let action = match args.get("action") {
        Some(Value::String(action)) => action.as_str(),
        None => {
            if args.contains_key("title") {
                "create"
            } else if args.contains_key("id") {
                "show"
            } else {
                "list"
            }
        }
        _ => return Err(invalid("action must be a string.")),
    };
    let items = sense.items(&call.context.ant).await.map_err(refused)?;
    if action == "list" {
        let promises = open(&*items, call.context.at).await.map_err(refused)?;
        let text = if promises.is_empty() {
            "No open commitments.".into()
        } else {
            promises.iter().map(|item| line(item, call.context.at)).collect::<Vec<_>>().join("\n")
        };
        return Ok(Answer { text, json: json!({"promises": promises}), is_error: false });
    }
    if action == "create" {
        let values = terms(call, None)?;
        let id = ItemId::new();
        let writes = creation(id, values, call.context.at, sense.workdir(&call.context))?;
        let item = crate::remind::write_creations(&*items, &writes, call.context.thread.as_str(), call.context.at)
            .await
            .map_err(refused)?
            .remove(0);
        return Ok(answer(item, call.context.at));
    }
    let id: ItemId = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("id must name a commitment."))?
        .parse()
        .map_err(|_| invalid("id must be an item id."))?;
    let item = items
        .get(&id)
        .await
        .map_err(refused)?
        .filter(|item| item.kind == KIND)
        .ok_or_else(|| Answer::error("not_found", format!("No commitment {id}.")))?;
    if let Some(revision) = args.get("revision") {
        let revision = revision.as_u64().filter(|n| *n > 0).ok_or_else(|| invalid("revision must be positive."))?;
        if revision != item.revision {
            return Err(refused(StoreError::Conflict { expected: revision, current: Box::new(item) }));
        }
    }
    if action == "show" {
        return Ok(answer(item, call.context.at));
    }
    if item.text("status") != "open" {
        return Err(Answer::error("closed", format!("Commitment {id} is {}.", item.text("status"))));
    }
    resolve(&*items, call, item, action, sense.workdir(&call.context)).await
}

async fn resolve(
    items: &dyn Items,
    call: &ToolCall,
    item: Item,
    action: &str,
    workdir: Option<std::path::PathBuf>,
) -> Result<Answer, Answer> {
    let id = item.id.clone();
    let args = &call.arguments;
    let reason = optional_text(args.get("reason"), "reason", 2000)?;
    let mut changed = Values::new();
    changed.insert("reason".into(), reason.clone().into());
    changed.insert("resolved_at".into(), json!(call.context.at));
    changed.insert(WRITER.into(), writer(&call.context));
    let status = match action {
        "keep" => "kept",
        "break" => "broken",
        "release" => "released",
        "renegotiate" => "renegotiated",
        _ => return Err(invalid("Unknown promise action.")),
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
    if action == "renegotiate" {
        let mut replacement = terms(call, Some(&item))?;
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
    if action == "renegotiate" {
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

fn terms(call: &ToolCall, previous: Option<&Item>) -> Result<Values, Answer> {
    let args = &call.arguments;
    let mut values = Values::new();
    for (name, max) in [("title", 2000), ("promisee", 500)] {
        let value = optional_text(args.get(name), name, max)?
            .or_else(|| previous.map(|item| item.text(name).to_owned()))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| invalid(&format!("{name} must be nonempty.")))?;
        values.insert(name.into(), value.into());
    }
    let due = match args.get("due") {
        Some(Value::String(due)) => Some(due.as_str()),
        None => previous.map(|item| item.text("due")),
        _ => return Err(invalid("due must be an RFC 3339 time.")),
    }
    .ok_or_else(|| invalid("due must be an RFC 3339 time."))?
    .parse::<DateTime<Utc>>()
    .map_err(|_| invalid("due must be an RFC 3339 time."))?;
    values.insert("due".into(), json!(due));
    values.insert("status".into(), "open".into());
    values.insert("source_thread".into(), call.context.thread.as_str().into());
    values.insert(WRITER.into(), writer(&call.context));
    Ok(values)
}

fn optional_text(value: Option<&Value>, name: &str, max: usize) -> Result<Option<String>, Answer> {
    match value {
        None => Ok(None),
        Some(Value::String(text)) => sanitize(text, Lines::Single, Limit::Chars(max))
            .map(|text| Some(text.text))
            .map_err(|e| invalid(&format!("{name}: {e}"))),
        _ => Err(invalid(&format!("{name} must be a string."))),
    }
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

fn invalid(message: &str) -> Answer {
    Answer::error("invalid_arguments", message.into())
}

fn refused(error: StoreError) -> Answer {
    let code = if matches!(error, StoreError::Conflict { .. }) { "conflict" } else { "unavailable" };
    let mut answer = Answer::error(code, format!("Commitment not changed: {error}."));
    if let StoreError::Conflict { current, .. } = error {
        if current.kind == KIND {
            answer.json["promise"] = json!(current);
        }
        answer.json["current"] = json!(current);
    }
    answer
}

#[cfg(test)]
mod tests;
