use chrono::{DateTime, Datelike, Duration, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::store::StoreError;

/// Reminder time zones are deliberately UTC, so a restart or host timezone change cannot move a firing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trigger {
    At { at: DateTime<Utc> },
    After { seconds: u64 },
    Daily { time: String, timezone: String },
    Weekly { weekday: String, time: String, timezone: String },
    Condition { condition: Value },
}

impl Trigger {
    pub fn condition(&self) -> Option<&Value> {
        match self {
            Self::Condition { condition } => Some(condition),
            _ => None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::At { .. } => Ok(()),
            Self::After { seconds } => {
                if *seconds == 0 || *seconds > 31_536_000 {
                    return Err("after.seconds must be between 1 and 31536000.".into());
                }
                Ok(())
            }
            Self::Daily { time, timezone } | Self::Weekly { time, timezone, .. } => {
                clock(time)?;
                if timezone != "UTC" {
                    return Err("Recurring reminders require timezone UTC.".into());
                }
                if let Self::Weekly { weekday, .. } = self {
                    weekday_number(weekday)?;
                }
                Ok(())
            }
            Self::Condition { condition } => validate_condition(condition),
        }
    }

    pub(crate) fn first(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, StoreError> {
        match self {
            Self::At { at } => Ok(*at),
            Self::After { seconds } => super::add(now, Duration::seconds(*seconds as i64)),
            Self::Condition { .. } => Ok(now),
            _ => self.next(now)?.ok_or_else(|| StoreError::Unavailable("no recurring deadline".into())),
        }
    }

    pub(crate) fn next(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, StoreError> {
        let (time, weekday) = match self {
            Self::Daily { time, .. } => (time, None),
            Self::Weekly { time, weekday, .. } => {
                (time, Some(weekday_number(weekday).map_err(StoreError::Unavailable)?))
            }
            _ => return Ok(None),
        };
        let time = clock(time).map_err(StoreError::Unavailable)?;
        for days in 0..=7 {
            let date = super::add(now, Duration::days(days))?.date_naive();
            if weekday.is_some_and(|wanted| date.weekday().num_days_from_monday() != wanted) {
                continue;
            }
            let candidate = date.and_time(time).and_utc();
            if candidate > now {
                return Ok(Some(candidate));
            }
        }
        Err(StoreError::Unavailable("no recurring deadline".into()))
    }
}

fn clock(time: &str) -> Result<NaiveTime, String> {
    if time.len() != 5 || time.as_bytes()[2] != b':' {
        return Err("Recurring time must be HH:MM in UTC.".into());
    }
    NaiveTime::parse_from_str(time, "%H:%M").map_err(|_| "Recurring time must be HH:MM in UTC.".into())
}

fn weekday_number(day: &str) -> Result<u32, String> {
    ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        .iter()
        .position(|candidate| *candidate == day)
        .map(|day| day as u32)
        .ok_or_else(|| "weekday must be mon, tue, wed, thu, fri, sat or sun.".into())
}

fn validate_condition(condition: &Value) -> Result<(), String> {
    let map = condition.as_object().ok_or("condition must be an object.")?;
    let kind = map.get("kind").and_then(Value::as_str).ok_or("condition.kind must be a string.")?;
    let keys: &[&str] = match kind {
        "gh_pr" => &["kind", "repo", "number", "state"],
        "gh_workflow" => &["kind", "repo", "run"],
        "bd" => &["kind", "id", "state"],
        "git" => &["kind", "commit", "reference"],
        "file" => &["kind", "path", "event"],
        "command" => &["kind", "command"],
        "sense" => &["kind", "event", "id"],
        _ => return Err("Unknown condition.kind.".into()),
    };
    if map.keys().any(|key| !keys.contains(&key.as_str())) {
        return Err("Unknown condition field.".into());
    }
    let text = |name: &str| -> Result<&str, String> {
        let value = map.get(name).and_then(Value::as_str).ok_or_else(|| format!("condition.{name} is required."))?;
        if value.trim().is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
            return Err(format!("condition.{name} must be a nonempty single-line string up to 4096 bytes."));
        }
        let clean =
            crate::sanitize::sanitize(value, crate::sanitize::Lines::Single, crate::sanitize::Limit::Bytes(4096))
                .map_err(|error| error.to_string())?;
        if clean.text != value {
            return Err(format!("condition.{name} contains hidden characters or secrets."));
        }
        Ok(value)
    };
    let one_of = |name: &str, allowed: &[&str]| -> Result<(), String> {
        if !allowed.contains(&text(name)?) {
            return Err(format!("Invalid condition.{name}."));
        }
        Ok(())
    };
    match kind {
        "gh_pr" | "gh_workflow" => {
            let repo = text("repo")?;
            if repo.split('/').count() != 2 || repo.split('/').any(|part| part.is_empty() || part.starts_with('-')) {
                return Err("condition.repo must be owner/repository.".into());
            }
            let number = if kind == "gh_pr" { "number" } else { "run" };
            if map.get(number).and_then(Value::as_u64).is_none_or(|number| number == 0) {
                return Err(format!("condition.{number} must be positive."));
            }
            if kind == "gh_pr" {
                one_of("state", &["merged", "closed", "reviewed"])?;
            }
        }
        "bd" => {
            text("id")?;
            one_of("state", &["closed", "ready"])?;
        }
        "git" => {
            text("commit")?;
            text("reference")?;
        }
        "file" => {
            text("path")?;
            one_of("event", &["appeared", "changed"])?;
        }
        "command" => {
            text("command")?;
        }
        "sense" => {
            one_of("event", &["commitment_overdue", "lease_expired"])?;
            if map.contains_key("id") {
                text("id")?.parse::<crate::items::ItemId>().map_err(|_| "condition.id must be an item id.")?;
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

pub(super) fn schema() -> Value {
    let text = json!({"type":"string","minLength":1,"maxLength":4096});
    let condition = |kind: &str, fields: Value, required: &[&str]| {
        let mut properties = fields.as_object().unwrap().clone();
        properties.insert("kind".into(), json!({"const":kind}));
        let required = std::iter::once("kind").chain(required.iter().copied()).collect::<Vec<_>>();
        json!({"type":"object","additionalProperties":false,"properties":properties,"required":required})
    };
    let condition = json!({"oneOf":[
        condition("gh_pr",json!({"repo":text,"number":{"type":"integer","minimum":1},
            "state":{"enum":["merged","closed","reviewed"]}}),&["repo","number","state"]),
        condition("gh_workflow",json!({"repo":text,"run":{"type":"integer","minimum":1}}),&["repo","run"]),
        condition("bd",json!({"id":text,"state":{"enum":["closed","ready"]}}),&["id","state"]),
        condition("git",json!({"commit":text,"reference":text}),&["commit","reference"]),
        condition("file",json!({"path":text,"event":{"enum":["appeared","changed"]}}),&["path","event"]),
        condition("command",json!({"command":text}),&["command"]),
        condition("sense",json!({"event":{"enum":["commitment_overdue","lease_expired"]},"id":text}),&["event"])
    ]});
    json!({"oneOf":[
        {"type":"object","additionalProperties":false,"required":["kind","at"],
         "properties":{"kind":{"const":"at"},"at":{"type":"string","format":"date-time"}}},
        {"type":"object","additionalProperties":false,"required":["kind","seconds"],
         "properties":{"kind":{"const":"after"},"seconds":{"type":"integer","minimum":1,"maximum":31536000}}},
        {"type":"object","additionalProperties":false,"required":["kind","time","timezone"],
         "properties":{"kind":{"const":"daily"},"time":{"type":"string","pattern":"^([01][0-9]|2[0-3]):[0-5][0-9]$"},
                       "timezone":{"const":"UTC"}}},
        {"type":"object","additionalProperties":false,"required":["kind","weekday","time","timezone"],
         "properties":{"kind":{"const":"weekly"},"weekday":{"enum":["mon","tue","wed","thu","fri","sat","sun"]},
                       "time":{"type":"string","pattern":"^([01][0-9]|2[0-3]):[0-5][0-9]$"},"timezone":{"const":"UTC"}}},
        {"type":"object","additionalProperties":false,"required":["kind","condition"],
         "properties":{"kind":{"const":"condition"},"condition":condition}}
    ]})
}
