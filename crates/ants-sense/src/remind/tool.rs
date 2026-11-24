//! The `sense_remind` tool: creating, listing, showing, acknowledging, postponing, dropping and renewing an ant's
//! reminders, each change guarded by the reminder's revision.

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
            "at":{"type":"string","description":format!("Postpone until: {}.", crate::when::FORMS)},
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
    /// Read by [`trigger::read`].
    trigger: Option<Value>,
    at: Option<String>,
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
            reminders.iter().map(|(item, reminder)| line(item, reminder)).collect::<Vec<_>>().join("\n")
        };
        let reminders: Vec<_> = reminders.into_iter().map(|(item, _)| item).collect();
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
    let reminder = Reminder::of(&item).map_err(refused)?;
    if action == Action::Show {
        return answer(item);
    }
    deny_untrusted_command(call, &reminder.trigger)?;
    if action == Action::Ack && reminder.status == Status::Acknowledged {
        return answer(item);
    }
    let zone = sense.timezone(&call.context.ant);
    let changed = mutation(reminder, call, &request, action, &zone)?;
    let writes = [change_to(&item, &changed).map_err(refused)?];
    answer(write_creations(&*items, &writes, by, now).await.map_err(refused)?.remove(0))
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
    let trigger = trigger::read(trigger, now, &sense.timezone(&call.context.ant))
        .map_err(|error| Answer::invalid(format!("trigger: {}.", error.trim_end_matches('.'))))?;
    trigger.validate().map_err(|error| Answer::invalid(format!("trigger: {}.", error.trim_end_matches('.'))))?;
    deny_untrusted_command(call, &trigger)?;
    let at = trigger.first(now).map_err(refused)?;
    let expires = add(now, Duration::days(CONDITION_DAYS)).map_err(refused)?;
    let workdir = sense.workdir(&call.context);
    let thread = call.context.thread.clone();
    let reminder = Reminder::new(title.text, thread, trigger, at, expires, &call.context, workdir.as_deref());
    let writes = [Write::Create { id: ItemId::new(), kind: KIND.into(), values: reminder.values() }];
    answer(write_creations(items, &writes, by, now).await.map_err(refused)?.remove(0))
}

fn mutation(
    mut reminder: Reminder,
    call: &ToolCall,
    request: &Request,
    action: Action,
    zone: &crate::when::TimeZone,
) -> Result<Reminder, Answer> {
    let now = call.context.at;
    let expired = reminder.expired(now);
    reminder.writer = call.context.clone();
    match action {
        Action::Ack => {
            if !matches!(reminder.status, Status::Active | Status::Exhausted | Status::Expired) {
                return Err(Answer::invalid("Only an undropped reminder can be acknowledged."));
            }
            reminder.acknowledged_at = Some(now);
            if reminder.trigger.watch().is_some() {
                return Ok(reminder);
            }
            reminder.pending = None;
            reminder.occurrence_firings = 0;
            let next = if expired { None } else { reminder.trigger.next(now).map_err(refused)? };
            reminder.next_due = next;
            reminder.status = if next.is_some() { Status::Active } else { Status::Acknowledged };
        }
        Action::Drop => reminder.drop_it(),
        Action::Postpone => {
            if !matches!(reminder.status, Status::Active | Status::Exhausted) || expired {
                return Err(Answer::invalid("Only an active, unexpired reminder can be postponed."));
            }
            let at = match (&request.at, request.after_seconds) {
                (Some(at), None) => crate::when::parse(at, now, &crate::when::Zone::Named(zone.clone()))
                    .map_err(|error| Answer::invalid(format!("at: {error}.")))?,
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
            reminder.status = Status::Active;
            reminder.pending = None;
            reminder.next_due = Some(at);
            reminder.occurrence_firings = 0;
            reminder.snoozed = reminder.trigger.watch().is_none();
        }
        Action::Renew => {
            if !matches!(reminder.status, Status::Active | Status::Expired) {
                return Err(Answer::invalid("Only an active or expired reminder can be renewed."));
            }
            reminder.expires = Some(reminder.renewed_expiry(now).map_err(refused)?);
            if reminder.status == Status::Expired || expired {
                let next = match reminder.trigger {
                    Trigger::After { .. } => reminder.first_due.unwrap_or(now),
                    _ => reminder.trigger.first(now).map_err(refused)?,
                };
                reminder.status = Status::Active;
                reminder.next_due = Some(next);
                reminder.unchanged_checks = Some(0);
                reminder.occurrence_firings = 0;
                reminder.snoozed = false;
                reminder.check_started = None;
                reminder.failed_checks = None;
                reminder.check_error = None;
            }
        }
        Action::Create | Action::List | Action::Show => return Err(Answer::invalid("No change of a reminder.")),
    }
    Ok(reminder)
}

fn line(item: &Item, reminder: &Reminder) -> String {
    format!(
        "{} [{}] {}{}",
        item.id,
        reminder.status.name(),
        reminder.title,
        reminder.next_due.map_or_default(|at| format!(" — next {at}"))
    )
}

fn answer(item: Item) -> Result<Answer, Answer> {
    let text = line(&item, &Reminder::of(&item).map_err(refused)?);
    Ok(Answer { text, json: json!({"reminder":item}), is_error: false })
}

fn deny_untrusted_command(call: &ToolCall, trigger: &Trigger) -> Result<(), Answer> {
    if matches!(trigger.condition(), Some(Condition::Command { .. })) && !call.context.origin.trusted() {
        return Err(Answer::error("untrusted", "Untrusted threads cannot create or change command triggers.".into()));
    }
    if trigger.watch().is_some() && !call.context.origin.trusted() {
        return Err(Answer::error("untrusted", "Untrusted threads cannot create or change watchers.".into()));
    }
    Ok(())
}

/// The refusal of a call, showing the reminder found at another revision.
fn refused(error: impl Into<Answer>) -> Answer {
    crate::tools::refused_as(error, KIND, "reminder")
}
