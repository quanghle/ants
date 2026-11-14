//! Schedules of the daemon config file: prompts sent on a timetable, each as a new thread to an ant or a channel.
//!
//! ```toml
//! [[schedule]]
//! name = "nightly"                  # sends as this user: `ants auth user nightly --webhook --to dev-a`
//! creds = "~/.config/ants/auth/nightly.creds"
//! cron = "0 3 * * 1-5"              # minute hour day-of-month month day-of-week, in UTC
//! ant = "dev-a"                     # or channel = "ops", with mention = [...] or broadcast = true
//! owner = "alice"                   # optional human owner of the threads it starts
//! text = "Audit the dependencies and report what changed."
//! catch_up = true                   # optional: once a daemon runs again, send the latest firing it missed
//!
//! [[schedule]]
//! name = "pulse"
//! every = "15m"                     # at whole multiples of 15 minutes since 1970-01-01 00:00 UTC
//! channel = "ops"
//! text = "Anything stuck?"
//! ```
//!
//! A firing time depends only on the schedule and the clock, never on when a daemon started, so every daemon serving
//! the schedule computes the same firings and can tell them apart.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_proto::{ChannelId, Destination, JsDomain, UserId};
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Datelike as _, NaiveDate, NaiveTime, TimeDelta, Timelike as _, Utc};
use serde::Deserialize;

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
    every: Option<String>,
    ant: Option<UserId>,
    channel: Option<ChannelId>,
    mention: Option<Vec<UserId>>,
    #[serde(default)]
    broadcast: bool,
    owner: Option<UserId>,
    text: String,
    #[serde(default)]
    catch_up: bool,
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
    pub text: String,
    /// Send the latest firing missed while no daemon could, late, rather than skip it.
    pub catch_up: bool,
}

impl Schedule {
    /// Check `entry`, resolving its credentials path with `resolve`.
    pub fn from_entry(entry: Entry, resolve: impl Fn(&Path) -> PathBuf) -> Result<Self> {
        if entry.name.is_human() {
            bail!("the default human is no schedule");
        }
        let when = match (entry.cron, entry.every) {
            (Some(cron), None) => When::Cron(cron.parse()?),
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
        if entry.text.trim().is_empty() {
            bail!("empty text");
        }
        if let Some(url) = &entry.nats {
            crate::bus::check_url(url)?;
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
            text: entry.text,
            catch_up: entry.catch_up,
        })
    }
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
            Self::Cron(cron) => write!(f, "cron {:?}", cron.text),
            Self::Every(every) => write!(f, "every {}", ants_proto::duration::format(*every)),
        }
    }
}

/// A five-field cron timetable, in UTC: minute (0-59), hour (0-23), day of month (1-31), month (1-12 or jan-dec) and
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
    /// The first minute strictly after `t` the timetable fires at; None when there is none within years.
    pub fn next_after(&self, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let start = t.naive_utc().with_second(0)?.with_nanosecond(0)? + TimeDelta::minutes(1);
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
            return Some(at.and_utc());
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
        let every = When::Every(Duration::from_secs(15 * 60));
        assert_eq!(every.next_after(at("2026-11-14T10:20:30Z")), Some(at("2026-11-14T10:30:00Z")));
        assert_eq!(every.next_after(at("2026-11-14T10:30:00Z")), Some(at("2026-11-14T10:45:00Z")), "strictly after");
        let daily = When::Every(Duration::from_secs(24 * 60 * 60));
        assert_eq!(daily.next_after(at("2026-11-14T23:59:59Z")), Some(at("2026-11-15T00:00:00Z")));
    }

    #[test]
    fn the_last_firing_in_a_span_is_found() {
        let every = When::Every(Duration::from_secs(15 * 60));
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
        Schedule::from_entry(entry, |p| Path::new("/etc/ants").join(p))
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
        ] {
            let e = format!("{:#}", entry(&format!("{base}{extra}\n")).unwrap_err());
            assert!(e.contains(why), "{extra}: {e}");
        }
        let e = entry("name = \"p\"\ntext = \" \"\ncron = \"@daily\"\nant = \"dev-a\"\n").unwrap_err();
        assert!(e.to_string().contains("empty text"), "{e}");
        let e = entry("name = \"human\"\ntext = \"t\"\ncron = \"@daily\"\nant = \"dev-a\"\n").unwrap_err();
        assert!(e.to_string().contains("no schedule"), "{e}");
    }
}
