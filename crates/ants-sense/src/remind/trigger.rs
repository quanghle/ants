//! When a reminder fires ([`Trigger`]): at a time, after a delay, daily or weekly at a time of day in a time zone, on
//! a condition, or, a watcher, on each change of what it watches; its reading from a `sense_remind` call ([`read`]),
//! its validation, its schema in the tool, and its first and next firing times.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{Condition, RemindError, Watch};
use crate::when::{self, Zone};

/// The most seconds of an `after` trigger: a year.
pub const MAX_AFTER_SECONDS: u64 = 365 * 24 * 60 * 60;

/// A daily or weekly trigger keeps the zone it was made in, so neither a restart, a host's time zone nor a change of
/// the config file's `timezone` moves its firings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Trigger {
    At {
        at: DateTime<Utc>,
    },
    After {
        seconds: u64,
    },
    Daily {
        time: String,
        timezone: String,
    },
    Weekly {
        weekday: String,
        time: String,
        timezone: String,
    },
    Condition {
        condition: Condition,
    },
    /// A watcher: armed until dropped, expired or done, firing on each change of its check's result.
    Watch(Watch),
}

impl Trigger {
    pub fn condition(&self) -> Option<&Condition> {
        match self {
            Self::Condition { condition } => Some(condition),
            _ => None,
        }
    }

    pub fn watch(&self) -> Option<&Watch> {
        match self {
            Self::Watch(watch) => Some(watch),
            _ => None,
        }
    }

    /// When it fires, in a line: `at 2025-01-01T12:00:00Z`, `after 300 s`, `daily at 09:00 Europe/London`,
    /// `weekly on mon at 09:00 UTC`, `condition {…}`, `watch command gh run list, every 60 s`.
    pub fn summary(&self) -> String {
        match self {
            Self::At { at } => format!("at {}", crate::items::stamp(*at)),
            Self::After { seconds } => format!("after {seconds} s"),
            Self::Daily { time, timezone } => format!("daily at {time} {timezone}"),
            Self::Weekly { weekday, time, timezone } => format!("weekly on {weekday} at {time} {timezone}"),
            Self::Condition { condition } => format!("condition {}", json!(condition)),
            Self::Watch(watch) => {
                let run = match &watch.run {
                    super::Watched::Command { command } => format!("command {command}"),
                    super::Watched::Trail { name, .. } => format!("trail {name}"),
                };
                let every = watch.interval_seconds.unwrap_or(super::watch::MIN_INTERVAL_SECONDS);
                format!("watch {run}, every {every} s")
            }
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
                when::clock(time)?;
                when::zone(timezone)?;
                if let Self::Weekly { weekday, .. } = self {
                    weekday_number(weekday)?;
                }
                Ok(())
            }
            Self::Condition { condition } => condition.validate(),
            Self::Watch(watch) => watch.validate(),
        }
    }

    pub(crate) fn first(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, RemindError> {
        match self {
            Self::At { at } => Ok(*at),
            Self::After { seconds } => super::add(now, Duration::seconds(*seconds as i64)),
            Self::Condition { .. } | Self::Watch(_) => Ok(now),
            _ => self.next(now)?.ok_or_else(no_time),
        }
    }

    pub(crate) fn next(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, RemindError> {
        let (time, weekday, timezone) = match self {
            Self::Daily { time, timezone } => (time, None, timezone),
            Self::Weekly { time, weekday, timezone } => {
                (time, Some(weekday_number(weekday).map_err(RemindError::Trigger)?), timezone)
            }
            _ => return Ok(None),
        };
        let time = when::clock(time).map_err(RemindError::Trigger)?;
        let zone = Zone::Named(when::zone(timezone).map_err(RemindError::Trigger)?);
        zone.next(time, weekday, now).map(Some).ok_or_else(no_time)
    }
}

/// The trigger `value` of a `sense_remind` call made at `now`, its times read as [`when::parse`] reads them in `zone`,
/// the config file's: an `at` given as text, and the zone of a daily or weekly one naming none.
pub(crate) fn read(mut value: Value, now: DateTime<Utc>, zone: &when::TimeZone) -> Result<Trigger, String> {
    if let Value::Object(fields) = &mut value {
        match fields.get("kind").and_then(Value::as_str) {
            Some("at") => {
                if let Some(Value::String(text)) = fields.get("at") {
                    let at = when::parse(text, now, &Zone::Named(zone.clone())).map_err(|e| format!("at: {e}"))?;
                    fields.insert("at".into(), json!(at));
                }
            }
            Some("daily" | "weekly") if !fields.contains_key("timezone") => {
                fields.insert("timezone".into(), json!(when::name(zone)));
            }
            _ => {}
        }
    }
    serde_json::from_value(value).map_err(|e| e.to_string())
}

fn no_time() -> RemindError {
    RemindError::Trigger("The trigger has no time to come.".into())
}

fn weekday_number(day: &str) -> Result<chrono::Weekday, String> {
    use chrono::Weekday::{Fri, Mon, Sat, Sun, Thu, Tue, Wed};
    let days = [("mon", Mon), ("tue", Tue), ("wed", Wed), ("thu", Thu), ("fri", Fri), ("sat", Sat), ("sun", Sun)];
    days.iter()
        .find(|(name, _)| *name == day)
        .map(|(_, weekday)| *weekday)
        .ok_or_else(|| "weekday must be mon, tue, wed, thu, fri, sat or sun.".into())
}

pub(super) fn schema() -> Value {
    let condition = super::condition::schema();
    let watch = super::watch::schema();
    let at = format!("When: {}; local times in the config file's time zone.", when::FORMS);
    let timezone = "An IANA time zone such as Europe/London; default the config file's time zone, else UTC.";
    json!({"oneOf":[
        {"type":"object","additionalProperties":false,"required":["kind","at"],
         "properties":{"kind":{"const":"at"},"at":{"type":"string","description":at}}},
        {"type":"object","additionalProperties":false,"required":["kind","seconds"],
         "properties":{"kind":{"const":"after"},"seconds":{"type":"integer","minimum":1,"maximum":MAX_AFTER_SECONDS}}},
        {"type":"object","additionalProperties":false,"required":["kind","time"],
         "properties":{"kind":{"const":"daily"},"time":{"type":"string","pattern":"^([01][0-9]|2[0-3]):[0-5][0-9]$"},
                       "timezone":{"type":"string","description":timezone}}},
        {"type":"object","additionalProperties":false,"required":["kind","weekday","time"],
         "properties":{"kind":{"const":"weekly"},"weekday":{"enum":["mon","tue","wed","thu","fri","sat","sun"]},
                       "time":{"type":"string","pattern":"^([01][0-9]|2[0-3]):[0-5][0-9]$"},
                       "timezone":{"type":"string","description":timezone}}},
        {"type":"object","additionalProperties":false,"required":["kind","condition"],
         "description":"Checked at most once a minute (every 5 for gh and bd), every 30 after 10 checks seeing no \
                        change, a command's the same stdout and exit status.",
         "properties":{"kind":{"const":"condition"},"condition":condition}},
        watch
    ]})
}
