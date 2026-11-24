//! Times as ants read them: the one reading of a `[[schedule]]`'s time zone and clock, a `sense_remind` trigger's
//! time and `ants remind`'s, so the same text gives the same instant in the three ([`parse`]), and the note a firing
//! sent late carries ([`late_note`]).
//!
//! A time of day is read in a zone's local time, daylight saving time included: a local time skipped when the clocks
//! go forward is the instant the clocks jump (02:30 on a night that goes from 02:00 to 03:00 is 03:00), and a local
//! time repeated when they go back is its first occurrence.

use chrono::{DateTime, Datelike as _, Local, LocalResult, NaiveDateTime, NaiveTime, TimeDelta, TimeZone as _, Utc};
use jiff::tz::AmbiguousOffset;

pub use jiff::tz::TimeZone;

/// How late a firing may go out before it says it is late ([`late_note`]).
pub const LATE: TimeDelta = TimeDelta::minutes(1);

/// What [`parse`] reads, for its error and the help of each surface taking it.
pub const FORMS: &str =
    "a duration such as 90m or 1d2h from now, the next HH:MM, YYYY-MM-DDTHH:MM, or an RFC 3339 time";

/// The longest a day is searched forward to leave a gap in local time.
const GAP_MINUTES: i64 = 24 * 60;

/// Where a time of day is read: a time zone of the IANA database, or the local clock of the machine.
#[derive(Debug, Clone, PartialEq)]
pub enum Zone {
    Named(TimeZone),
    /// The machine's local clock, as `ants remind` reads its human's times.
    Local,
}

impl From<TimeZone> for Zone {
    fn from(zone: TimeZone) -> Self {
        Self::Named(zone)
    }
}

impl Zone {
    /// The instant local time `at` is in the zone: in a gap the end of the gap, in a fold the first occurrence.
    pub fn instant(&self, at: NaiveDateTime) -> Option<DateTime<Utc>> {
        match self {
            Self::Named(zone) => instant(zone, at),
            Self::Local => (0..=GAP_MINUTES).find_map(|minutes| {
                match Local.from_local_datetime(&(at + TimeDelta::minutes(minutes))) {
                    LocalResult::Single(at) | LocalResult::Ambiguous(at, _) => Some(at.with_timezone(&Utc)),
                    LocalResult::None => None,
                }
            }),
        }
    }

    /// The local time of instant `t` in the zone.
    pub fn local(&self, t: DateTime<Utc>) -> Option<NaiveDateTime> {
        match self {
            Self::Named(zone) => local(zone, t),
            Self::Local => Some(t.with_timezone(&Local).naive_local()),
        }
    }

    /// The first instant strictly after `now` at local time `time`, on `weekday` when given; None when there is none
    /// within a week.
    pub fn next(&self, time: NaiveTime, weekday: Option<chrono::Weekday>, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let today = self.local(now)?.date();
        (0..=8).find_map(|days| {
            let date = today.checked_add_signed(TimeDelta::days(days))?;
            if weekday.is_some_and(|weekday| date.weekday() != weekday) {
                return None;
            }
            self.instant(date.and_time(time)).filter(|at| *at > now)
        })
    }
}

/// The time zone named `name` in the IANA time zone database (`/usr/share/zoneinfo` or `TZDIR` on Unix, a bundled
/// copy on Windows), `UTC` included.
pub fn zone(name: &str) -> Result<TimeZone, String> {
    if name == "UTC" {
        return Ok(TimeZone::UTC);
    }
    TimeZone::get(name).map_err(|e| format!("timezone {name:?}: unknown time zone ({e})"))
}

/// The name of `zone`, as [`zone`] reads it back.
pub fn name(zone: &TimeZone) -> &str {
    zone.iana_name().unwrap_or("UTC")
}

/// A time of day written `HH:MM`.
pub fn clock(text: &str) -> Result<NaiveTime, String> {
    let wrong = || format!("{text:?} is no time of day HH:MM");
    if text.len() != 5 || text.as_bytes()[2] != b':' {
        return Err(wrong());
    }
    NaiveTime::parse_from_str(text, "%H:%M").map_err(|_| wrong())
}

/// The instant `text` names from `now`, local times read in `zone`: a duration ([`ants_proto::duration`]) from now,
/// the next time of day `HH:MM`, a date and time `YYYY-MM-DDTHH:MM`, or an RFC 3339 time, which names its own offset.
/// Whether a past instant does is for the caller to say.
pub fn parse(text: &str, now: DateTime<Utc>, zone: &Zone) -> Result<DateTime<Utc>, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err(format!("no time given: {FORMS}"));
    }
    if let Ok(time) = clock(text) {
        return zone.next(time, None, now).ok_or_else(|| format!("{text}: no such local time to come"));
    }
    if let Ok(at) = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M") {
        return zone.instant(at).ok_or_else(|| format!("{text}: no such local time"));
    }
    if let Ok(at) = DateTime::parse_from_rfc3339(text) {
        return Ok(at.with_timezone(&Utc));
    }
    match ants_proto::duration::parse(text) {
        Ok(duration) => TimeDelta::from_std(duration)
            .ok()
            .and_then(|duration| now.checked_add_signed(duration))
            .ok_or_else(|| format!("{text}: too long")),
        Err(_) => Err(format!("{text:?} is no time: {FORMS}")),
    }
}

/// The note a firing due at `due` and sent at `at` ends with, when later than [`LATE`]: as a `[[schedule]]`'s, a
/// sense reminder's and a pushed `ants remind`'s say it.
pub fn late_note(due: DateTime<Utc>, at: DateTime<Utc>) -> Option<String> {
    (at - due > LATE).then(|| format!("(Sent late: due {}.)", due.format("%Y-%m-%d %H:%M UTC")))
}

/// The instant local time `at` is in `zone` ([`Zone::instant`]).
pub fn instant(zone: &TimeZone, at: NaiveDateTime) -> Option<DateTime<Utc>> {
    if *zone == TimeZone::UTC {
        return Some(at.and_utc());
    }
    let civil = jiff::civil::DateTime::new(
        i16::try_from(at.year()).ok()?,
        i8::try_from(at.month()).ok()?,
        i8::try_from(at.day()).ok()?,
        i8::try_from(chrono::Timelike::hour(&at)).ok()?,
        i8::try_from(chrono::Timelike::minute(&at)).ok()?,
        i8::try_from(chrono::Timelike::second(&at)).ok()?,
        0,
    )
    .ok()?;
    let instant = match zone.to_ambiguous_timestamp(civil).offset() {
        AmbiguousOffset::Unambiguous { offset } | AmbiguousOffset::Fold { before: offset, .. } => {
            offset.to_timestamp(civil).ok()?
        }
        // Read with the offset after the gap, `at` falls just before the transition that opens it.
        AmbiguousOffset::Gap { after, .. } => zone.following(after.to_timestamp(civil).ok()?).next()?.timestamp(),
    };
    DateTime::from_timestamp(instant.as_second(), instant.subsec_nanosecond().try_into().ok()?)
}

/// The local time of instant `t` in `zone`.
pub fn local(zone: &TimeZone, t: DateTime<Utc>) -> Option<NaiveDateTime> {
    if *zone == TimeZone::UTC {
        return Some(t.naive_utc());
    }
    let offset = zone.to_offset(jiff::Timestamp::from_second(t.timestamp()).ok()?);
    Some(t.naive_utc() + TimeDelta::seconds(offset.seconds().into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    #[test]
    fn a_time_is_a_duration_the_next_time_of_day_a_local_date_and_time_or_an_rfc_3339_time() {
        let now = utc("2027-01-15T08:00:00Z");
        let at_utc = Zone::Named(TimeZone::UTC);
        assert_eq!(parse("90m", now, &at_utc), Ok(now + TimeDelta::minutes(90)));
        assert_eq!(parse("09:30", now, &at_utc), Ok(utc("2027-01-15T09:30:00Z")), "later today");
        assert_eq!(parse("07:00", now, &at_utc), Ok(utc("2027-01-16T07:00:00Z")), "tomorrow");
        let east = Zone::Named(TimeZone::fixed(jiff::tz::offset(2)));
        assert_eq!(parse("09:30", now, &east), Ok(utc("2027-01-16T07:30:00Z")), "in the zone");
        assert_eq!(parse("2027-01-18T08:00", now, &east), Ok(utc("2027-01-18T06:00:00Z")));
        assert_eq!(parse("2027-01-18T08:00:00+01:00", now, &east), Ok(utc("2027-01-18T07:00:00Z")), "its own offset");
        assert!(parse("", now, &at_utc).is_err());
        assert!(parse("soon", now, &at_utc).unwrap_err().contains("HH:MM"));
        assert!(clock("9:30").is_err() && clock("24:00").is_err());
    }

    #[test]
    fn a_local_time_skipped_is_the_jump_and_one_repeated_its_first_occurrence() {
        let london = Zone::Named(zone("Europe/London").unwrap());
        let spring = "2027-03-28T01:30:00".parse::<NaiveDateTime>().unwrap();
        assert_eq!(london.instant(spring), Some(utc("2027-03-28T01:00:00Z")), "01:30 is skipped: the jump to 02:00");
        let autumn = "2027-10-31T01:30:00".parse::<NaiveDateTime>().unwrap();
        assert_eq!(london.instant(autumn), Some(utc("2027-10-31T00:30:00Z")), "the first 01:30");
        let monday = london.next(clock("09:00").unwrap(), Some(chrono::Weekday::Mon), utc("2027-07-01T00:00:00Z"));
        assert_eq!(monday, Some(utc("2027-07-05T08:00:00Z")), "summer time");
        assert!(zone("Nowhere/City").is_err());
        assert_eq!(name(&zone("UTC").unwrap()), "UTC");
    }

    #[test]
    fn a_firing_later_than_a_minute_says_when_it_was_due() {
        let due = utc("2027-01-15T08:00:00Z");
        assert_eq!(late_note(due, due + LATE), None);
        let late = late_note(due, due + TimeDelta::hours(2));
        assert_eq!(late.as_deref(), Some("(Sent late: due 2027-01-15 08:00 UTC.)"));
    }
}
