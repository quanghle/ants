use crate::memory::{WRITER, writer};
use crate::sanitize::{Limit, Lines, sanitize};

use super::*;

/// The most characters of a reminder's title.
const MAX_TITLE_CHARS: usize = 2_000;
/// The longest postponement given in seconds: a year.
const MAX_POSTPONE_SECONDS: u64 = 365 * 24 * 60 * 60;

pub(super) fn schema() -> Value {
    json!({
        "type":"object","additionalProperties":false,
        "properties":{
            "action":{"enum":["create","list","show","ack","postpone","drop","renew"]},
            "id":{"type":"string"},
            "title":{"type":"string","minLength":1,"maxLength":MAX_TITLE_CHARS},
            "trigger":trigger::schema(),
            "at":{"type":"string","format":"date-time","description":"Postpone until this RFC 3339 time."},
            "after_seconds":{
                "type":"integer","minimum":1,"maximum":MAX_POSTPONE_SECONDS,"description":"Postpone by this duration."
            },
            "revision":{"type":"integer","minimum":1}
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Create,
    List,
    Show,
    Ack,
    Postpone,
    Drop,
    Renew,
}

/// The arguments of a `sense_remind` call.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    action: Option<Action>,
    id: Option<ItemId>,
    title: Option<String>,
    trigger: Option<Trigger>,
    at: Option<DateTime<Utc>>,
    after_seconds: Option<u64>,
    revision: Option<u64>,
}

pub(super) async fn handle(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let request: Request = crate::tools::arguments(call)?;
    let action = request.action.unwrap_or(match (&request.trigger, &request.id) {
        (Some(_), _) => Action::Create,
        (None, Some(_)) => Action::Show,
        (None, None) => Action::List,
    });
    if request.revision == Some(0) {
        return Err(Answer::invalid("revision must be positive."));
    }
    let items = sense.items(&call.context.ant).await.map_err(refused)?;
    let now = call.context.at;
    let by = call.context.thread.as_str();
    if action == Action::List {
        let reminders = active(&*items, now).await.map_err(refused)?;
        let text = if reminders.is_empty() {
            "No active reminders.".into()
        } else {
            reminders.iter().map(line).collect::<Vec<_>>().join("\n")
        };
        return Ok(Answer { text, json: json!({"reminders":reminders}), is_error: false });
    }
    if action == Action::Create {
        return create(sense, &*items, call, request).await;
    }
    let id = request.id.as_ref().ok_or_else(|| Answer::invalid("id must name a reminder."))?;
    let item = get(&*items, id).await.map_err(refused)?;
    if let Some(revision) = request.revision
        && revision != item.revision
    {
        return Err(refused(StoreError::Conflict { expected: revision, current: Box::new(item) }));
    }
    if action == Action::Show {
        return Ok(answer(item));
    }
    deny_webhook_command(call, &trigger(&item).map_err(refused)?)?;
    if action == Action::Ack && item.text("status") == "acknowledged" {
        return Ok(answer(item));
    }
    let changed = mutation(&item, call, &request, action)?;
    let writes = [Write::Change { id: item.id.clone(), values: changed, expected: Some(item.revision) }];
    let item = write_creations(&*items, &writes, by, now).await.map_err(refused)?.remove(0);
    Ok(answer(item))
}

async fn create(sense: &Sense, items: &dyn Items, call: &ToolCall, request: Request) -> Result<Answer, Answer> {
    let now = call.context.at;
    let by = call.context.thread.as_str();
    let title = request.title.as_deref().ok_or_else(|| Answer::invalid("title is required."))?;
    let title = sanitize(title, Lines::Single, Limit::Chars(MAX_TITLE_CHARS))
        .map_err(|error| Answer::invalid(format!("title: {error}.")))?;
    if title.text.trim().is_empty() {
        return Err(Answer::invalid("title must be nonempty."));
    }
    let trigger = request.trigger.ok_or_else(|| Answer::invalid("trigger is required."))?;
    trigger.validate().map_err(|error| Answer::invalid(format!("trigger: {error}.")))?;
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

fn mutation(item: &Item, call: &ToolCall, request: &Request, action: Action) -> Result<Values, Answer> {
    let now = call.context.at;
    let mut changed = values(json!({WRITER:writer(&call.context)}));
    match action {
        Action::Ack => {
            if !matches!(item.text("status"), "active" | "exhausted" | "expired") {
                return Err(Answer::invalid("Only an undropped reminder can be acknowledged."));
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
        Action::Drop => {
            changed.insert("pending".into(), Value::Null);
            changed.insert("next_due".into(), Value::Null);
            changed.insert("status".into(), "dropped".into());
        }
        Action::Postpone => {
            if !matches!(item.text("status"), "active" | "exhausted") || expired(item, now) {
                return Err(Answer::invalid("Only an active, unexpired reminder can be postponed."));
            }
            let at = match (request.at, request.after_seconds) {
                (Some(at), None) => at,
                (None, Some(seconds)) => {
                    if !(1..=MAX_POSTPONE_SECONDS).contains(&seconds) {
                        let message = format!("after_seconds must be between 1 and {MAX_POSTPONE_SECONDS}.");
                        return Err(Answer::invalid(message));
                    }
                    add(now, Duration::seconds(seconds as i64)).map_err(refused)?
                }
                _ => return Err(Answer::invalid("Supply exactly one of at or after_seconds.")),
            };
            if at <= now {
                return Err(Answer::invalid("Postponement time must be in the future."));
            }
            changed.extend(values(json!({
                "status":"active","pending":null,"next_due":at,"occurrence_firings":0,"snoozed":true
            })));
        }
        Action::Renew => {
            if !matches!(item.text("status"), "active" | "expired") {
                return Err(Answer::invalid("Only an active or expired reminder can be renewed."));
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
        Action::Create | Action::List | Action::Show => return Err(Answer::invalid("No change of a reminder.")),
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

fn deny_webhook_command(call: &ToolCall, trigger: &Trigger) -> Result<(), Answer> {
    if trigger.condition().is_some_and(|condition| condition["kind"] == "command")
        && matches!(call.context.origin.by, ants_proto::provider::Starter::Webhook { .. })
    {
        return Err(Answer::error("untrusted", "Webhook threads cannot change command triggers.".into()));
    }
    Ok(())
}

/// The refusal of a call its store failed, showing the reminder found at another revision.
fn refused(error: StoreError) -> Answer {
    let mut answer = Answer::from(error);
    if answer.json["current"]["kind"] == KIND {
        answer.json["reminder"] = answer.json["current"].clone();
    }
    answer
}
