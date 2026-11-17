use crate::memory::{WRITER, writer};
use crate::sanitize::{Limit, Lines, sanitize};

use super::*;

pub(super) fn schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,
        "properties":{
            "action":{"enum":["create","list","show","ack","postpone","drop","renew"]},
            "id":{"type":"string"},
            "title":{"type":"string","minLength":1,"maxLength":2000},
            "trigger":trigger::schema(),
            "at":{"type":"string","format":"date-time","description":"Postpone until this RFC 3339 time."},
            "after_seconds":{"type":"integer","minimum":1,"maximum":31536000,"description":"Postpone by this duration."},
            "revision":{"type":"integer","minimum":1}
        }
    })
}

pub(super) async fn handle(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    for key in call.arguments.keys() {
        if !["action", "id", "title", "trigger", "at", "after_seconds", "revision"].contains(&key.as_str()) {
            return Err(invalid(&format!("Unknown reminder argument {key:?}.")));
        }
    }
    let args = &call.arguments;
    let action = match args.get("action") {
        Some(Value::String(action)) => action.as_str(),
        None if args.contains_key("trigger") => "create",
        None if args.contains_key("id") => "show",
        None => "list",
        _ => return Err(invalid("action must be a string.")),
    };
    let items = sense.items(&call.context.ant).await.map_err(refused)?;
    let now = call.context.at;
    let by = call.context.thread.as_str();
    if action == "list" {
        let reminders = active(&*items, now).await.map_err(refused)?;
        let text = if reminders.is_empty() {
            "No active reminders.".into()
        } else {
            reminders.iter().map(line).collect::<Vec<_>>().join("\n")
        };
        return Ok(Answer { text, json: json!({"reminders":reminders}), is_error: false });
    }
    if action == "create" {
        return create(sense, &*items, call).await;
    }
    let id = args
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("id must name a reminder."))?
        .parse::<ItemId>()
        .map_err(|_| invalid("id must be an item id."))?;
    let item = get(&*items, &id).await.map_err(refused)?;
    if let Some(revision) = args.get("revision") {
        let revision =
            revision.as_u64().filter(|revision| *revision > 0).ok_or_else(|| invalid("Invalid revision."))?;
        if revision != item.revision {
            return Err(refused(StoreError::Conflict { expected: revision, current: Box::new(item) }));
        }
    }
    if action == "show" {
        return Ok(answer(item));
    }
    deny_webhook_command(call, &trigger(&item).map_err(refused)?)?;
    if action == "ack" && item.text("status") == "acknowledged" {
        return Ok(answer(item));
    }
    let changed = mutation(&item, call, action)?;
    let writes = [Write::Change { id: item.id.clone(), values: changed, expected: Some(item.revision) }];
    let item = write_creations(&*items, &writes, by, now).await.map_err(refused)?.remove(0);
    Ok(answer(item))
}

async fn create(sense: &Sense, items: &dyn Items, call: &ToolCall) -> Result<Answer, Answer> {
    let args = &call.arguments;
    let now = call.context.at;
    let by = call.context.thread.as_str();
    let title = args.get("title").and_then(Value::as_str).ok_or_else(|| invalid("title is required."))?;
    let title = sanitize(title, Lines::Single, Limit::Chars(2000)).map_err(|error| invalid(&error.to_string()))?;
    if title.text.trim().is_empty() {
        return Err(invalid("title must be nonempty."));
    }
    let trigger: Trigger = serde_json::from_value(args.get("trigger").cloned().unwrap_or_default())
        .map_err(|error| invalid(&format!("Invalid trigger: {error}")))?;
    trigger.validate().map_err(|error| invalid(&error))?;
    deny_webhook_command(call, &trigger)?;
    let at = trigger.first(now).map_err(refused)?;
    let mut values = values(json!({
        "title":title.text,"thread":by,"trigger":trigger,"status":"active","next_due":at,"first_due":at,
        "number":0,"occurrence_firings":0,WRITER:writer(&call.context),"source_context":writer(&call.context),
        "expires":add(now, Duration::days(CONDITION_DAYS)).map_err(refused)?
    }));
    if trigger.condition().is_some() {
        values.insert("unchanged_checks".into(), 0.into());
    }
    if let Some(workdir) = sense.workdir(&call.context) {
        values.insert("workdir".into(), workdir.to_string_lossy().as_ref().into());
        values.insert("workdir_machine".into(), call.context.machine.as_str().into());
    }
    let writes = [Write::Create { id: ItemId::new(), kind: KIND.into(), values }];
    Ok(answer(write_creations(items, &writes, by, now).await.map_err(refused)?.remove(0)))
}

fn mutation(item: &Item, call: &ToolCall, action: &str) -> Result<Values, Answer> {
    let args = &call.arguments;
    let now = call.context.at;
    let mut changed = values(json!({WRITER:writer(&call.context)}));
    match action {
        "ack" => {
            if !matches!(item.text("status"), "active" | "exhausted" | "expired") {
                return Err(invalid("Only an undropped reminder can be acknowledged."));
            }
            changed.insert("pending".into(), Value::Null);
            changed.insert("acknowledged_at".into(), json!(now));
            changed.insert("occurrence_firings".into(), 0.into());
            let next =
                if expired(item, now) { None } else { trigger(item).map_err(refused)?.next(now).map_err(refused)? };
            match next {
                Some(next) => {
                    changed.insert("next_due".into(), json!(next));
                    changed.insert("status".into(), "active".into());
                }
                None => {
                    changed.insert("next_due".into(), Value::Null);
                    changed.insert("status".into(), "acknowledged".into());
                }
            }
        }
        "drop" => {
            changed.insert("pending".into(), Value::Null);
            changed.insert("next_due".into(), Value::Null);
            changed.insert("status".into(), "dropped".into());
        }
        "postpone" => {
            if !matches!(item.text("status"), "active" | "exhausted") || expired(item, now) {
                return Err(invalid("Only an active, unexpired reminder can be postponed."));
            }
            let at = match (args.get("at"), args.get("after_seconds")) {
                (Some(Value::String(at)), None) => {
                    at.parse::<DateTime<Utc>>().map_err(|_| invalid("at must be an RFC 3339 time."))?
                }
                (None, Some(seconds)) => {
                    let seconds = seconds
                        .as_u64()
                        .filter(|seconds| *seconds > 0 && *seconds <= 31_536_000)
                        .ok_or_else(|| invalid("after_seconds must be between 1 and 31536000."))?;
                    add(now, Duration::seconds(seconds as i64)).map_err(refused)?
                }
                _ => return Err(invalid("Supply exactly one of at or after_seconds.")),
            };
            if at <= now {
                return Err(invalid("Postponement time must be in the future."));
            }
            changed.extend(values(json!({
                "status":"active","pending":null,"next_due":at,"occurrence_firings":0,"snoozed":true
            })));
        }
        "renew" => {
            if !matches!(item.text("status"), "active" | "expired") {
                return Err(invalid("Only an active or expired reminder can be renewed."));
            }
            changed.insert("expires".into(), json!(renewed_expiry(item, now).map_err(refused)?));
            if item.text("status") == "expired" || expired(item, now) {
                let trigger = trigger(item).map_err(refused)?;
                let next = match trigger {
                    Trigger::After { .. } => date(item, "first_due").unwrap_or(now),
                    _ => trigger.first(now).map_err(refused)?,
                };
                changed.extend(values(json!({
                    "status":"active","next_due":next,"unchanged_checks":0,
                    "occurrence_firings":0,"snoozed":null,"check_started":null
                })));
            }
        }
        _ => return Err(invalid("Unknown reminder action.")),
    }
    Ok(changed)
}

fn line(item: &Item) -> String {
    format!(
        "{} [{}] {}{}",
        item.id,
        item.text("status"),
        item.text("title"),
        date(item, "next_due").map_or_default(|at| format!(" — next {at}"))
    )
}

fn answer(item: Item) -> Answer {
    Answer { text: line(&item), json: json!({"reminder":item}), is_error: false }
}

fn invalid(message: &str) -> Answer {
    Answer::error("invalid_arguments", message.into())
}

fn deny_webhook_command(call: &ToolCall, trigger: &Trigger) -> Result<(), Answer> {
    if trigger.condition().is_some_and(|condition| condition["kind"] == "command")
        && matches!(call.context.origin.by, ants_proto::provider::Starter::Webhook { .. })
    {
        return Err(Answer::error("untrusted", "Webhook threads cannot change command triggers.".into()));
    }
    Ok(())
}

fn refused(error: StoreError) -> Answer {
    let code = match error {
        StoreError::NotFound(_) => "not_found",
        StoreError::Conflict { .. } => "conflict",
        _ => "unavailable",
    };
    let mut answer = Answer::error(code, format!("Reminder not changed: {error}."));
    if let StoreError::Conflict { current, .. } = error {
        if current.kind == KIND {
            answer.json["reminder"] = json!(current);
        }
        answer.json["current"] = json!(current);
    }
    answer
}
