use chrono::{DateTime, Datelike, Duration, NaiveTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{Condition, RemindError};

/// The most seconds of an `after` trigger: a year.
pub const MAX_AFTER_SECONDS: u64 = 365 * 24 * 60 * 60;

/// Reminder time zones are deliberately UTC, so a restart or host timezone change cannot move a firing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trigger {
    At { at: DateTime<Utc> },
    After { seconds: u64 },
    Daily { time: String, timezone: String },
    Weekly { weekday: String, time: String, timezone: String },
    Condition { condition: Condition },
}

impl Trigger {
    pub fn condition(&self) -> Option<&Condition> {
        match self {
            Self::Condition { condition } => Some(condition),
            _ => None,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::At { .. } => Ok(()),
            Self::After { seconds } => {
                if !(1..=MAX_AFTER_SECONDS).contains(seconds) {
                    return Err(format!("after.seconds must be between 1 and {MAX_AFTER_SECONDS}."));
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
            Self::Condition { condition } => condition.validate(),
        }
    }

    pub(crate) fn first(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, RemindError> {
        match self {
            Self::At { at } => Ok(*at),
            Self::After { seconds } => super::add(now, Duration::seconds(*seconds as i64)),
            Self::Condition { .. } => Ok(now),
            _ => self.next(now)?.ok_or_else(no_time),
        }
    }

    pub(crate) fn next(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, RemindError> {
        let (time, weekday) = match self {
            Self::Daily { time, .. } => (time, None),
            Self::Weekly { time, weekday, .. } => (time, Some(weekday_number(weekday).map_err(RemindError::Trigger)?)),
            _ => return Ok(None),
        };
        let time = clock(time).map_err(RemindError::Trigger)?;
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
        Err(no_time())
    }
}

fn no_time() -> RemindError {
    RemindError::Trigger("The trigger has no time to come.".into())
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

pub(super) fn schema() -> Value {
    let condition = super::condition::schema();
    json!({"oneOf":[
        {"type":"object","additionalProperties":false,"required":["kind","at"],
         "properties":{"kind":{"const":"at"},"at":{"type":"string","format":"date-time"}}},
        {"type":"object","additionalProperties":false,"required":["kind","seconds"],
         "properties":{"kind":{"const":"after"},"seconds":{"type":"integer","minimum":1,"maximum":MAX_AFTER_SECONDS}}},
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
