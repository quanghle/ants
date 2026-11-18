//! Schedules of the daemon config file: prompts sent on a timetable, each as a new thread to an ant or a channel, or
//! saved trails an ant starts on one.
//!
//! ```toml
//! [[schedule]]
//! name = "nightly"                  # sends as this user: `ants auth user nightly --webhook --to dev-a`
//! creds = "~/.config/ants/auth/nightly.creds"
//! cron = "0 3 * * 1-5"              # minute hour day-of-month month day-of-week
//! timezone = "Europe/London"        # optional: the zone cron reads in; the file's top-level timezone, then UTC
//! ant = "dev-a"                     # or channel = "ops", with mention = [...] or broadcast = true
//! owner = "alice"                   # optional human owner of the threads it starts
//! text = "Audit the dependencies and report what changed."
//! catch_up = true                   # optional: once a daemon runs again, send the latest firing it missed
//! allow = ["read:*", "execute:cargo audit*"]  # optional: the tool rules its threads are narrowed to
//!
//! [[schedule]]
//! name = "pulse"
//! every = "15m"                     # at whole multiples of 15 minutes since 1970-01-01 00:00 UTC
//! channel = "ops"
//! text = "Anything stuck?"
//!
//! [[schedule]]
//! name = "weekly-review"
//! cron = "0 9 * * 1"
//! ant = "lead"                      # the conductor of the runs
//! trail = "review-open-prs"         # instead of text: start this saved trail ([crate::trail])
//! args = { repo = "acme/app" }      # optional: the run's JSON input
//! limits = { steps = 100, wall = "2d" }  # optional: what each run may spend (steps, at_once, turns, wall)
//! ```
//!
//! A firing time depends only on the schedule and the clock, never on when a daemon started, so every daemon serving
//! the schedule computes the same firings and can tell them apart.
//!
//! A cron timetable reads in its time zone's local time, daylight saving time included: a local time skipped when the
//! clocks go forward fires once, at the instant the clocks jump (02:30 on a night that goes from 02:00 to 03:00 fires
//! at 03:00), and a local time repeated when they go back fires once, at its first occurrence.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{ChannelId, Destination, JsDomain, TrailName, TrailStart, UserId};
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Datelike as _, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike as _, Utc};
use jiff::tz::{AmbiguousOffset, TimeZone};
use serde::Deserialize;
use serde_json::Value;

/// Shortest `every`.
pub const MIN_EVERY: Duration = Duration::from_secs(60);
/// How far ahead [`Cron::next_after`] looks: past it, a timetable such as `0 0 30 2 *` never fires.
const CRON_HORIZON_DAYS: i64 = 5 * 366;

/// One `[[schedule]]` as the file writes it.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub name: UserId,
    creds: Option<PathBuf>,
    nats: Option<String>,
    js_domain: Option<JsDomain>,
    cron: Option<String>,
    timezone: Option<String>,
    every: Option<String>,
    ant: Option<UserId>,
    channel: Option<ChannelId>,
    mention: Option<Vec<UserId>>,
    #[serde(default)]
    broadcast: bool,
    owner: Option<UserId>,
    text: Option<String>,
    trail: Option<TrailName>,
    args: Option<serde_json::Map<String, Value>>,
    limits: Option<crate::trail::AskedLimits>,
    #[serde(default)]
    catch_up: bool,
    allow: Option<Vec<String>>,
}

/// Whom a channel post mentions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mentions {
    /// Nobody: the channel's messenger triages it.
    Nobody,
    Listed(Vec<UserId>),
    /// Every member at the time of the post.
    Members,
}

/// One `[[schedule]]`, checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Schedule {
    /// The user the schedule sends as, and the name its firings are known by.
    pub name: UserId,
    pub creds: Option<PathBuf>,
    /// NATS server; the file's top-level `nats`, then `--nats`, when absent.
    pub nats: Option<String>,
    /// JetStream domain of the hub; the file's top-level `js_domain`, then `--js-domain`, when absent.
    pub js_domain: Option<JsDomain>,
    pub when: When,
    pub destination: Destination,
    pub mentions: Mentions,
    pub owner: Option<UserId>,
    /// The prompt; for a trail schedule, the line its message shows.
    pub text: String,
    /// The saved trail a firing starts, with the ant as its conductor and under the limits the entry gives, rather
    /// than prompting the ant.
    pub trail: Option<TrailStart>,
    /// Send the latest firing missed while no daemon could, late, rather than skip it.
    pub catch_up: bool,
    /// The tool rules the threads it starts are narrowed to; None for [`ants_proto::WEBHOOK_ALLOW`].
    pub allow: Option<Vec<String>>,
}

impl Schedule {
    /// Check `entry`, reading its cron in `timezone` (UTC when None) unless it names its own, and resolving its
    /// credentials path with `resolve`.
    pub fn from_entry(entry: Entry, timezone: Option<&TimeZone>, resolve: impl Fn(&Path) -> PathBuf) -> Result<Self> {
        if entry.name.is_human() {
            bail!("the default human is no schedule");
        }
        if ants_proto::BuiltinAnt::from_id(entry.name.as_str()).is_some() {
            bail!("{} is reserved for a built-in ant", entry.name);
        }
        let when = match (entry.cron, entry.every) {
            (Some(cron), None) => {
                let zone = match entry.timezone {
                    Some(name) => time_zone(&name)?,
                    None => timezone.cloned().unwrap_or(TimeZone::UTC),
                };
                When::Cron(cron.parse::<Cron>()?.in_zone(zone))
            }
            (None, Some(_)) if entry.timezone.is_some() => bail!("timezone goes with cron only"),
            (None, Some(every)) => {
                let every = ants_proto::duration::parse(&every).map_err(|e| anyhow::anyhow!("every: {e}"))?;
                if every < MIN_EVERY {
                    bail!("every: {} is shorter than 1m", ants_proto::duration::format(every));
                }
                When::Every(every)
            }
            _ => bail!("give exactly one of cron and every"),
        };
        let destination = match (entry.ant, entry.channel) {
            (Some(ant), None) => Destination::Ant(ant),
            (None, Some(channel)) => Destination::Channel(channel),
            _ => bail!("give exactly one of ant and channel"),
        };
        let mentions = match (entry.mention, entry.broadcast) {
            (Some(_), true) => bail!("mention and broadcast exclude each other"),
            (None, false) => Mentions::Nobody,
            _ if matches!(destination, Destination::Ant(_)) => bail!("mention and broadcast go with a channel only"),
            (Some(mentions), false) => Mentions::Listed(mentions),
            (None, true) => Mentions::Members,
        };
        let (text, trail) = match (entry.text, entry.trail) {
            (Some(text), None) if entry.args.is_some() => bail!("args go with trail only, not text: {text:?}"),
            (Some(text), None) if entry.limits.is_some() => bail!("limits go with trail only, not text: {text:?}"),
            (Some(text), None) if text.trim().is_empty() => bail!("empty text"),
            (Some(text), None) => (text, None),
            (None, Some(_)) if !matches!(destination, Destination::Ant(_)) => {
                bail!("a trail schedule names the ant conducting its runs, not a channel")
            }
            (None, Some(_)) if entry.allow.is_some() => {
                bail!("allow goes with text only: a trail's steps are narrowed as a webhook's threads are")
            }
            (None, Some(trail)) => {
                let args = entry.args.map_or(Value::Null, Value::Object);
                let limits = entry.limits.unwrap_or_default().limits().map_err(|e| anyhow::anyhow!("limits: {e}"))?;
                (format!("Scheduled run of trail {trail}."), Some(TrailStart { trail, args, limits }))
            }
            _ => bail!("give exactly one of text and trail"),
        };
        if let Some(url) = &entry.nats {
            crate::bus::check_url(url)?;
        }
        if let Some(allow) = &entry.allow {
            crate::permission::Allowlist::parse(allow)?;
        }
        Ok(Self {
            name: entry.name,
            creds: entry.creds.as_deref().map(resolve),
            nats: entry.nats,
            js_domain: entry.js_domain,
            when,
            destination,
            mentions,
            owner: entry.owner,
            text,
            trail,
            catch_up: entry.catch_up,
            allow: entry.allow,
        })
    }
}

/// The time zone named `name` in the IANA time zone database.
pub fn time_zone(name: &str) -> Result<TimeZone> {
    TimeZone::get(name).map_err(|e| anyhow::anyhow!("timezone {name:?}: unknown time zone ({e})"))
}

/// When a schedule fires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum When {
    Cron(Cron),
    /// At every whole multiple of the duration since the Unix epoch.
    Every(Duration),
}

impl When {
    /// The first firing strictly after `t`; None when there is none within years.
    pub fn next_after(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::Cron(cron) => cron.next_after(t),
            Self::Every(every) => {
                let step = i64::try_from(every.as_secs()).ok().filter(|s| *s > 0)?;
                let next = t.timestamp().div_euclid(step).checked_add(1)?.checked_mul(step)?;
                DateTime::from_timestamp(next, 0)
            }
        }
    }

    /// The last firing after `after` and at or before `upto`.
    pub fn last_in(&self, after: DateTime<Utc>, upto: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let last = match self {
            Self::Cron(cron) => {
                let mut last = None;
                let mut t = after;
                while let Some(next) = cron.next_after(t).filter(|next| *next <= upto) {
                    last = Some(next);
                    t = next;
                }
                last
            }
            Self::Every(every) => {
                let step = i64::try_from(every.as_secs()).ok().filter(|s| *s > 0)?;
                DateTime::from_timestamp(upto.timestamp().div_euclid(step).checked_mul(step)?, 0)
            }
        };
        last.filter(|last| *last > after)
    }
}

impl fmt::Display for When {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cron(cron) if cron.zone == TimeZone::UTC => write!(f, "cron {:?}", cron.text),
            Self::Cron(cron) => {
                write!(f, "cron {:?} in {}", cron.text, cron.zone.iana_name().unwrap_or("a time zone"))
            }
            Self::Every(every) => write!(f, "every {}", ants_proto::duration::format(*every)),
        }
    }
}

/// A five-field cron timetable, in UTC unless [`Cron::in_zone`] says otherwise: minute (0-59), hour (0-23), day of month (1-31), month (1-12 or jan-dec) and
/// day of week (0-7 or sun-sat, 0 and 7 both Sunday). A field is `*` or a comma-separated list of values, ranges
/// `a-b` and steps `*/n`, `a-b/n` or `a/n`. When both day fields are restricted, a day matching either one fires, as
/// in Vixie cron. `@yearly` (`@annually`), `@monthly`, `@weekly`, `@daily` (`@midnight`) and `@hourly` stand for
/// their usual timetables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cron {
    text: String,
    minutes: u64,
    hours: u64,
    days: u64,
    months: u64,
    weekdays: u64,
    /// The day-of-month field starts with `*`.
    any_day: bool,
    /// The day-of-week field starts with `*`.
    any_weekday: bool,
    /// The zone whose local time the fields read in.
    zone: TimeZone,
}

const MONTHS: [&str; 12] = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const WEEKDAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

impl std::str::FromStr for Cron {
    type Err = anyhow::Error;

    fn from_str(text: &str) -> Result<Self> {
        let shown = text;
        let expanded = match text.trim() {
            "@yearly" | "@annually" => "0 0 1 1 *",
            "@monthly" => "0 0 1 * *",
            "@weekly" => "0 0 * * 0",
            "@daily" | "@midnight" => "0 0 * * *",
            "@hourly" => "0 * * * *",
            other if other.starts_with('@') => bail!("cron {shown:?}: unknown {other}"),
            other => other,
        };
        let fields: Vec<&str> = expanded.split_whitespace().collect();
        let [minute, hour, day, month, weekday] = fields[..] else {
            bail!(
                "cron {shown:?}: expected 5 fields (minute hour day-of-month month day-of-week), found {}",
                fields.len()
            );
        };
        let parse = |field: &str, what: &str, min: u32, max: u32, names: &[&str], first_name: u32| {
            field_bits(field, min, max, names, first_name).with_context(|| format!("cron {shown:?}: {what} {field:?}"))
        };
        let mut weekdays = parse(weekday, "day of week", 0, 7, &WEEKDAYS, 0)?;
        if weekdays & (1 << 7) != 0 {
            weekdays = (weekdays & !(1 << 7)) | 1;
        }
        let cron = Self {
            text: text.trim().to_owned(),
            minutes: parse(minute, "minute", 0, 59, &[], 0)?,
            hours: parse(hour, "hour", 0, 23, &[], 0)?,
            days: parse(day, "day of month", 1, 31, &[], 0)?,
            months: parse(month, "month", 1, 12, &MONTHS, 1)?,
            weekdays,
            any_day: day.starts_with('*'),
            any_weekday: weekday.starts_with('*'),
            zone: TimeZone::UTC,
        };
        if cron.next_after(DateTime::UNIX_EPOCH).is_none() {
            bail!("cron {shown:?}: never fires");
        }
        Ok(cron)
    }
}

/// The values of one cron field as bits.
fn field_bits(field: &str, min: u32, max: u32, names: &[&str], first_name: u32) -> Result<u64> {
    let value = |text: &str| -> Result<u32> {
        let lower = text.to_ascii_lowercase();
        let n = match names.iter().position(|name| *name == lower) {
            Some(i) => first_name + i as u32,
            None => text.parse().ok().with_context(|| format!("{text:?} is no number"))?,
        };
        if !(min..=max).contains(&n) {
            bail!("{n} is out of {min}-{max}");
        }
        Ok(n)
    };
    let mut bits = 0;
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step.parse().ok().filter(|s| *s > 0).with_context(|| format!("step {step:?}"))?;
                (range, Some(step))
            }
            None => (part, None),
        };
        let (from, to) = match range {
            "*" => (min, max),
            "" => bail!("an empty value"),
            _ => match range.split_once('-') {
                Some((a, b)) => (value(a)?, value(b)?),
                None if step.is_some() => (value(range)?, max),
                None => (value(range)?, value(range)?),
            },
        };
        if from > to {
            bail!("range {range} runs backwards");
        }
        for n in (from..=to).step_by(step.unwrap_or(1) as usize) {
            bits |= 1 << n;
        }
    }
    Ok(bits)
}

impl Cron {
    /// The timetable read in `zone`'s local time.
    #[must_use]
    pub fn in_zone(self, zone: TimeZone) -> Self {
        Self { zone, ..self }
    }

    /// The instant local time `at` fires at in the timetable's zone: in a gap, when the clocks go forward, the end of
    /// the gap; in a fold, when they go back, the first occurrence.
    fn instant(&self, at: NaiveDateTime) -> Option<DateTime<Utc>> {
        if self.zone == TimeZone::UTC {
            return Some(at.and_utc());
        }
        let civil = jiff::civil::DateTime::new(
            i16::try_from(at.year()).ok()?,
            i8::try_from(at.month()).ok()?,
            i8::try_from(at.day()).ok()?,
            i8::try_from(at.hour()).ok()?,
            i8::try_from(at.minute()).ok()?,
            0,
            0,
        )
        .ok()?;
        let instant = match self.zone.to_ambiguous_timestamp(civil).offset() {
            AmbiguousOffset::Unambiguous { offset } | AmbiguousOffset::Fold { before: offset, .. } => {
                offset.to_timestamp(civil).ok()?
            }
            // Read with the offset after the gap, `at` falls just before the transition that opens it.
            AmbiguousOffset::Gap { after, .. } => {
                self.zone.following(after.to_timestamp(civil).ok()?).next()?.timestamp()
            }
        };
        DateTime::from_timestamp(instant.as_second(), instant.subsec_nanosecond().try_into().ok()?)
    }

    /// The local time of instant `t` in the timetable's zone.
    fn local(&self, t: DateTime<Utc>) -> Option<NaiveDateTime> {
        if self.zone == TimeZone::UTC {
            return Some(t.naive_utc());
        }
        let offset = self.zone.to_offset(jiff::Timestamp::from_second(t.timestamp()).ok()?);
        Some(t.naive_utc() + TimeDelta::seconds(offset.seconds().into()))
    }

    /// The first instant strictly after `t` the timetable fires at; None when there is none within years.
    pub fn next_after(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Local times only grow with the instants they fire at, and none up to `t`'s own fires after `t`.
        let start = self.local(t)?.with_second(0)?.with_nanosecond(0)? + TimeDelta::minutes(1);
        let limit = start + TimeDelta::days(CRON_HORIZON_DAYS);
        let mut at = start;
        let has = |bits: u64, n: u32| bits & (1 << n) != 0;
        while at <= limit {
            let date = at.date();
            if !has(self.months, date.month()) {
                let (year, month) =
                    if date.month() == 12 { (date.year() + 1, 1) } else { (date.year(), date.month() + 1) };
                at = NaiveDate::from_ymd_opt(year, month, 1)?.and_time(NaiveTime::MIN);
                continue;
            }
            let day = has(self.days, date.day());
            let weekday = has(self.weekdays, date.weekday().num_days_from_sunday());
            let day_matches = if self.any_day || self.any_weekday { day && weekday } else { day || weekday };
            if !day_matches {
                at = date.succ_opt()?.and_time(NaiveTime::MIN);
                continue;
            }
            if !has(self.hours, at.hour()) {
                at = at.with_minute(0)? + TimeDelta::hours(1);
                continue;
            }
            if !has(self.minutes, at.minute()) {
                at += TimeDelta::minutes(1);
                continue;
            }
            match self.instant(at)? {
                // A repeated local time whose first occurrence is past.
                instant if instant <= t => at += TimeDelta::minutes(1),
                instant => return Some(instant),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
    }

    fn next(cron: &str, after: &str) -> String {
        let cron: Cron = cron.parse().unwrap();
        cron.next_after(at(after)).unwrap().to_rfc3339()
    }

    #[test]
    fn cron_finds_the_next_firing() {
        // 2026-11-14 is a Saturday.
        assert_eq!(next("* * * * *", "2026-11-14T10:20:30Z"), "2026-11-14T10:21:00+00:00");
        assert_eq!(next("* * * * *", "2026-11-14T10:20:00Z"), "2026-11-14T10:21:00+00:00", "strictly after");
        assert_eq!(next("*/15 * * * *", "2026-11-14T10:20:00Z"), "2026-11-14T10:30:00+00:00");
        assert_eq!(next("0 3 * * *", "2026-11-14T10:20:00Z"), "2026-11-15T03:00:00+00:00");
        assert_eq!(next("0 3 * * 1-5", "2026-11-14T10:20:00Z"), "2026-11-16T03:00:00+00:00", "Monday");
        assert_eq!(next("30 9 * * SUN", "2026-11-14T10:20:00Z"), "2026-11-15T09:30:00+00:00");
        assert_eq!(next("0 0 * * 7", "2026-11-14T10:20:00Z"), "2026-11-15T00:00:00+00:00", "7 is Sunday");
        assert_eq!(next("0 0 1 * *", "2026-12-14T10:20:00Z"), "2027-01-01T00:00:00+00:00", "across a year");
        assert_eq!(next("0 12 29 feb *", "2026-11-14T10:20:00Z"), "2028-02-29T12:00:00+00:00", "a leap day");
        assert_eq!(next("5,10-12 8-9/1 * jan,Mar *", "2026-11-14T10:20:00Z"), "2027-01-01T08:05:00+00:00");
        assert_eq!(next("10/20 * * * *", "2026-11-14T10:31:00Z"), "2026-11-14T10:50:00+00:00");
        // Both day fields restricted: either one fires; one of them `*`: both must hold.
        assert_eq!(next("0 0 20 * mon", "2026-11-14T10:20:00Z"), "2026-11-16T00:00:00+00:00");
        assert_eq!(next("0 0 */1 * mon", "2026-11-14T10:20:00Z"), "2026-11-16T00:00:00+00:00");
        assert_eq!(next("0 0 13 * fri", "2026-11-14T10:20:00Z"), "2026-11-20T00:00:00+00:00", "a 13th or a Friday");
        assert_eq!(next("0 0 13 * *", "2026-11-14T10:20:00Z"), "2026-12-13T00:00:00+00:00");
        assert_eq!(next("@hourly", "2026-11-14T10:20:00Z"), "2026-11-14T11:00:00+00:00");
        assert_eq!(next("@weekly", "2026-11-14T10:20:00Z"), "2026-11-15T00:00:00+00:00");
        assert_eq!(next("@yearly", "2026-11-14T10:20:00Z"), "2027-01-01T00:00:00+00:00");
    }

    fn zoned(cron: &str, zone: &str) -> When {
        When::Cron(cron.parse::<Cron>().unwrap().in_zone(time_zone(zone).unwrap()))
    }

    /// The firings of `when` after `after`, `n` of them.
    fn firings(when: &When, after: &str, n: usize) -> Vec<DateTime<Utc>> {
        std::iter::successors(when.next_after(at(after)), |t| when.next_after(*t)).take(n).collect()
    }

    #[test]
    fn cron_reads_in_its_time_zone_and_fires_once_across_daylight_saving() {
        let ny = |cron| zoned(cron, "America/New_York");
        // 2026-03-08 02:00 EST jumps to 03:00 EDT (07:00 UTC): a skipped time fires as the clocks jump, once.
        let gap = [at("2026-03-08T07:00:00Z"), at("2026-03-09T06:30:00Z")];
        assert_eq!(firings(&ny("30 2 * * *"), "2026-03-07T12:00:00Z", 2), gap);
        assert_eq!(firings(&ny("*/15 2 * * *"), "2026-03-07T12:00:00Z", 2), [gap[0], at("2026-03-09T06:00:00Z")]);
        let hourly = [at("2026-03-08T06:00:00Z"), at("2026-03-08T07:00:00Z"), at("2026-03-08T08:00:00Z")];
        assert_eq!(firings(&ny("0 * * * *"), "2026-03-08T05:30:00Z", 3), hourly, "02:00 and 03:00 fire as one");
        // 2026-11-01 02:00 EDT goes back to 01:00 EST (06:00 UTC): a repeated time fires at its first occurrence.
        let fold = [at("2026-11-01T05:30:00Z"), at("2026-11-02T06:30:00Z")];
        assert_eq!(firings(&ny("30 1 * * *"), "2026-10-31T12:00:00Z", 2), fold);
        let hourly = [at("2026-11-01T05:00:00Z"), at("2026-11-01T07:00:00Z")];
        assert_eq!(firings(&ny("0 * * * *"), "2026-11-01T04:30:00Z", 2), hourly, "01:00 EST does not fire again");
        assert_eq!(ny("30 1 * * *").next_after(at("2026-11-01T06:15:00Z")), Some(fold[1]), "from inside the fold");
        assert_eq!(ny("30 1 * * *").last_in(at("2026-11-01T05:30:00Z"), at("2026-11-01T06:45:00Z")), None);

        // Europe/London: 2026-03-29 01:00 GMT jumps to 02:00 BST, 2026-10-25 02:00 BST goes back to 01:00 GMT.
        let london = zoned("30 1 * * *", "Europe/London");
        assert_eq!(
            firings(&london, "2026-03-28T12:00:00Z", 2),
            [at("2026-03-29T01:00:00Z"), at("2026-03-30T00:30:00Z")]
        );
        assert_eq!(
            firings(&london, "2026-10-24T12:00:00Z", 2),
            [at("2026-10-25T00:30:00Z"), at("2026-10-26T01:30:00Z")]
        );

        // Asia/Kolkata keeps +05:30 all year.
        let kolkata = zoned("0 9 * * 1-5", "Asia/Kolkata");
        assert_eq!(
            firings(&kolkata, "2026-11-14T10:20:00Z", 2),
            [at("2026-11-16T03:30:00Z"), at("2026-11-17T03:30:00Z")]
        );
        assert_eq!(kolkata.to_string(), "cron \"0 9 * * 1-5\" in Asia/Kolkata");
    }

    #[test]
    fn a_bad_cron_says_what_is_wrong() {
        for (cron, why) in [
            ("* * * *", "expected 5 fields"),
            ("60 * * * *", "minute \"60\": 60 is out of 0-59"),
            ("* 24 * * *", "hour"),
            ("* * 0 * *", "day of month"),
            ("* * * 13 *", "month"),
            ("* * * * 8", "day of week"),
            ("* * * * mon-sun", "runs backwards"),
            ("*/0 * * * *", "step"),
            ("1,,2 * * * *", "an empty value"),
            ("x * * * *", "\"x\" is no number"),
            ("0 0 30 feb *", "never fires"),
            ("@often", "unknown @often"),
        ] {
            let e = format!("{:#}", cron.parse::<Cron>().unwrap_err());
            assert!(e.contains(why), "{cron}: {e}");
        }
    }

    #[test]
    fn every_fires_at_multiples_since_the_epoch() {
        let every = When::Every(Duration::from_mins(15));
        assert_eq!(every.next_after(at("2026-11-14T10:20:30Z")), Some(at("2026-11-14T10:30:00Z")));
        assert_eq!(every.next_after(at("2026-11-14T10:30:00Z")), Some(at("2026-11-14T10:45:00Z")), "strictly after");
        let daily = When::Every(Duration::from_hours(24));
        assert_eq!(daily.next_after(at("2026-11-14T23:59:59Z")), Some(at("2026-11-15T00:00:00Z")));
    }

    #[test]
    fn the_last_firing_in_a_span_is_found() {
        let every = When::Every(Duration::from_mins(15));
        let last = |when: &When, after: &str, upto: &str| when.last_in(at(after), at(upto));
        assert_eq!(last(&every, "2026-11-14T08:00:00Z", "2026-11-14T10:20:30Z"), Some(at("2026-11-14T10:15:00Z")));
        assert_eq!(last(&every, "2026-11-14T08:00:00Z", "2026-11-14T10:15:00Z"), Some(at("2026-11-14T10:15:00Z")));
        assert_eq!(last(&every, "2026-11-14T10:15:00Z", "2026-11-14T10:29:59Z"), None, "after is excluded");
        let weekdays = When::Cron("0 3 * * 1-5".parse().unwrap());
        // 2026-11-14 is a Saturday: the last weekday firing before it is Friday's.
        assert_eq!(last(&weekdays, "2026-11-07T10:00:00Z", "2026-11-15T10:00:00Z"), Some(at("2026-11-13T03:00:00Z")));
        assert_eq!(last(&weekdays, "2026-11-13T03:00:00Z", "2026-11-15T10:00:00Z"), None);
        assert_eq!(last(&weekdays, "2026-11-13T02:59:00Z", "2026-11-13T03:00:00Z"), Some(at("2026-11-13T03:00:00Z")));
        let minutely = When::Cron("* * * * *".parse().unwrap());
        assert_eq!(last(&minutely, "2026-11-07T10:00:00Z", "2026-11-14T10:20:30Z"), Some(at("2026-11-14T10:20:00Z")));
    }

    fn entry(text: &str) -> Result<Schedule> {
        let entry: Entry = toml::from_str(text)?;
        Schedule::from_entry(entry, None, |p| Path::new("/etc/ants").join(p))
    }

    #[test]
    fn an_entry_names_one_timetable_and_one_destination() {
        let s =
            entry("name = \"nightly\"\ncreds = \"n.creds\"\ncron = \"0 3 * * *\"\nant = \"dev-a\"\ntext = \"go\"\n")
                .unwrap();
        assert_eq!(s.creds.as_deref(), Some(Path::new("/etc/ants/n.creds")));
        assert_eq!((s.destination, s.mentions), (Destination::Ant("dev-a".parse().unwrap()), Mentions::Nobody));
        assert!(!s.catch_up, "no catch-up unless asked");
        let s = entry(
            "name = \"pulse\"\nevery = \"1h\"\nchannel = \"ops\"\nbroadcast = true\ntext = \"t\"\ncatch_up = true\n",
        )
        .unwrap();
        assert_eq!((s.when, s.mentions, s.catch_up), (When::Every(Duration::from_secs(3600)), Mentions::Members, true));

        let base = "name = \"pulse\"\ntext = \"t\"\n";
        for (extra, why) in [
            ("ant = \"dev-a\"", "exactly one of cron and every"),
            ("ant = \"dev-a\"\ncron = \"@daily\"\nevery = \"1h\"", "exactly one of cron and every"),
            ("cron = \"@daily\"", "exactly one of ant and channel"),
            ("cron = \"@daily\"\nant = \"dev-a\"\nchannel = \"ops\"", "exactly one of ant and channel"),
            ("cron = \"@daily\"\nant = \"dev-a\"\nbroadcast = true", "with a channel only"),
            ("cron = \"@daily\"\nchannel = \"ops\"\nmention = []\nbroadcast = true", "exclude each other"),
            ("every = \"30s\"\nant = \"dev-a\"", "shorter than 1m"),
            ("every = \"15\"\nant = \"dev-a\"", "every:"),
            ("cron = \"@daily\"\nant = \"dev-a\"\nnats = \"nats://u:p@hub\"", "NATS URL"),
            ("cron = \"@daily\"\nant = \"dev-a\"\nat = \"3am\"", "unknown field `at`"),
            ("cron = \"@daily\"\nant = \"dev-a\"\ntimezone = \"Nowhere\"", "timezone \"Nowhere\": unknown time zone"),
            ("every = \"1h\"\nant = \"dev-a\"\ntimezone = \"UTC\"", "timezone goes with cron only"),
        ] {
            let e = format!("{:#}", entry(&format!("{base}{extra}\n")).unwrap_err());
            assert!(e.contains(why), "{extra}: {e}");
        }
        let e = entry("name = \"p\"\ntext = \" \"\ncron = \"@daily\"\nant = \"dev-a\"\n").unwrap_err();
        assert!(e.to_string().contains("empty text"), "{e}");
        let e = entry("name = \"human\"\ntext = \"t\"\ncron = \"@daily\"\nant = \"dev-a\"\n").unwrap_err();
        assert!(e.to_string().contains("no schedule"), "{e}");
    }

    #[test]
    fn an_entry_starts_a_saved_trail_with_its_args() {
        let base = "name = \"weekly\"\ncron = \"@weekly\"\nant = \"dev-a\"\n";
        let s = entry(&format!("{base}trail = \"review\"\nargs = {{ repo = \"app\", depth = 2 }}\n")).unwrap();
        let trail = s.trail.unwrap();
        assert_eq!(trail.trail.as_str(), "review");
        assert_eq!(trail.args, serde_json::json!({"repo": "app", "depth": 2}));
        assert!(s.text.contains("review"), "{}", s.text);
        assert_eq!(trail.limits, ants_proto::TrailLimits::DEFAULT, "no limits: the defaults");
        let s = entry(&format!("{base}trail = \"review\"\n")).unwrap();
        assert_eq!(s.trail.unwrap().args, serde_json::Value::Null, "no args");
        let s = entry(&format!("{base}trail = \"review\"\nlimits = {{ steps = 100, at_once = 8, wall = \"2d\" }}\n"));
        let asked = ants_proto::TrailLimits {
            steps: 100,
            concurrency: 8,
            wall_secs: 2 * 86_400,
            ..ants_proto::TrailLimits::DEFAULT
        };
        assert_eq!(s.unwrap().trail.unwrap().limits, asked, "the limits given, the others their defaults");

        for (extra, why) in [
            ("", "exactly one of text and trail"),
            ("text = \"t\"\ntrail = \"review\"", "exactly one of text and trail"),
            ("text = \"t\"\nargs = { a = 1 }", "args go with trail only"),
            ("trail = \"Review!\"", "trail"),
            ("trail = \"review\"\nallow = [\"read\"]", "allow"),
            ("text = \"t\"\nlimits = { steps = 9 }", "limits go with trail only"),
            ("trail = \"review\"\nlimits = { steps = 501 }", "limits: steps is over the most a run may take, 500"),
            ("trail = \"review\"\nlimits = { wall = \"8d\" }", "limits: wall is over the most a run may take, 7d"),
            ("trail = \"review\"\nlimits = { at_once = 0 }", "limits: at_once must be more than 0"),
            ("trail = \"review\"\nlimits = { stepz = 9 }", "unknown field `stepz`"),
        ] {
            let e = format!("{:#}", entry(&format!("{base}{extra}\n")).unwrap_err());
            assert!(e.contains(why), "{extra}: {e}");
        }
        let e = entry("name = \"w\"\ncron = \"@weekly\"\nchannel = \"ops\"\ntrail = \"review\"\n").unwrap_err();
        assert!(format!("{e:#}").contains("not a channel"), "{e:#}");
    }
}
