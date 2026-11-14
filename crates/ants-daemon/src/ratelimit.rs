//! Provider limits a harness runs into: a rate limit or an overloaded provider, which passes within minutes, and an
//! exhausted quota, which may take hours or a plan change. A message that meets one waits in hand, spending no
//! attempt, and is tried again after a backoff, sooner when the error says when; an exhausted quota is told to a human
//! once until a message gets past it.

use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use ants_proto::{Envelope, HarnessLimit, LimitKind, UserId};
use chrono::{DateTime, Utc};
use tokio::time::Instant;

use crate::harness::Harness;
use crate::humans::Registry;

/// Phrases of a harness's error or stderr, lowercase, that mean a rate limit or an overloaded provider, whatever the
/// harness; a definition adds its own ([`Harness::rate_limited`]).
const RATE_LIMITED_TEXTS: &[&str] = &[
    "rate limit",
    "rate_limit",
    "rate-limit",
    "ratelimit",
    "rate exceeded",
    "too many requests",
    "overloaded",
    "resource_exhausted",
    "resource exhausted",
    "exhausted your capacity",
    "capacity constraints",
    "is at capacity",
    "experiencing high load",
];

/// Phrases that mean an exhausted quota or usage allowance, whatever the harness; a definition adds its own
/// ([`Harness::quota_exhausted`]).
const QUOTA_TEXTS: &[&str] = &[
    "insufficient_quota",
    "quota exceeded",
    "quota_exceeded",
    "exceeded your current quota",
    "exceeded your quota",
    "out of quota",
    "no quota",
    "usage limit",
    "hit your limit",
    "spending limit",
    "credit balance is too low",
    "out of credits",
];

/// HTTP statuses of a rate limit and of an overloaded provider, counted only after a word that makes them one
/// ([`STATUS_WORDS`]), so that a number in a path or a timestamp is none.
const STATUSES: &[&str] = &["429", "529"];
const STATUS_WORDS: &[&str] = &["status", "status code", "status_code", "error", "code", "http", "rejected"];

/// Phrases after which an error may say how long to wait, as `30`, `23.4s`, `2h27m3s` or `4 days 20 hours`.
const HINTS: &[&str] = &[
    "retry-after",
    "retry after",
    "retry_after",
    "retryafter",
    "retrydelay",
    "retry_delay",
    "retry in",
    "try again in",
    "reset after",
    "resets after",
    "reset in",
    "resets in",
];

/// An exhausted quota said to be back within this long is waited for quietly, as a rate limit.
const SHORT_QUOTA: Duration = Duration::from_mins(5);

/// The longest wait a hint is read as.
const MAX_HINT: Duration = Duration::from_hours(7 * 24);

/// The most of what the harness said that a notice quotes.
const DETAIL_CHARS: usize = 500;

/// A session or prompt a harness turned away for a provider limit. Nothing ran, unless the prompt streamed work first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limited {
    pub harness: String,
    pub kind: LimitKind,
    /// How long the error says to wait, if it says.
    pub retry_after: Option<Duration>,
    /// What the harness said.
    pub detail: String,
}

impl fmt::Display for Limited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            LimitKind::Quota => write!(f, "{} is out of quota: {}", self.harness, self.detail),
            _ => write!(f, "{} is rate limited: {}", self.harness, self.detail),
        }
    }
}

impl std::error::Error for Limited {}

/// What `detail`, an error from `harness`, says of a provider limit, if it reports one.
pub fn classify(harness: &Harness, detail: &str) -> Option<Limited> {
    let text = detail.to_lowercase();
    let says = |common: &[&str], own: &[String]| {
        common.iter().any(|phrase| text.contains(phrase))
            || own.iter().any(|phrase| text.contains(&phrase.trim().to_lowercase()))
    };
    let quota = says(QUOTA_TEXTS, &harness.quota_exhausted);
    if !quota && !says(RATE_LIMITED_TEXTS, &harness.rate_limited) && !has_status(&text) {
        return None;
    }
    let retry_after = retry_after(&text, Utc::now());
    let kind = if quota && retry_after.is_none_or(|wait| wait > SHORT_QUOTA) {
        LimitKind::Quota
    } else {
        LimitKind::RateLimit
    };
    Some(Limited { harness: harness.name().to_owned(), kind, retry_after, detail: detail.to_owned() })
}

/// Whether `text` holds a rate-limit status as a whole number after a word such as `error` or `status`.
fn has_status(text: &str) -> bool {
    STATUSES.iter().any(|status| {
        text.match_indices(status).any(|(at, _)| {
            let (before, after) = (&text[..at], &text[at + status.len()..]);
            let whole = !before.ends_with(|c: char| c.is_alphanumeric() || c == '.')
                && !after.starts_with(|c: char| c.is_alphanumeric());
            let word = before.trim_end_matches(|c: char| c.is_whitespace() || ":\"'(=[".contains(c));
            whole && STATUS_WORDS.iter().any(|w| word.ends_with(w))
        })
    })
}

/// How long `text`, lowercase, says to wait: a duration after a hint ([`HINTS`]), else a reset time as Unix seconds
/// after `|` (`usage limit reached|1767225600`), at most [`MAX_HINT`].
fn retry_after(text: &str, now: DateTime<Utc>) -> Option<Duration> {
    let hinted = HINTS.iter().flat_map(|hint| text.match_indices(hint).map(|(at, _)| &text[at + hint.len()..]));
    let wait = hinted.filter_map(duration_at).next().or_else(|| {
        text.match_indices('|').find_map(|(at, _)| {
            let digits = &text[at + 1..];
            let digits = &digits[..digits.find(|c: char| !c.is_ascii_digit()).unwrap_or(digits.len())];
            let reset = DateTime::from_timestamp(digits.parse().ok().filter(|_| digits.len() == 10)?, 0)?;
            Some((reset - now).to_std().unwrap_or_default())
        })
    })?;
    Some(wait.min(MAX_HINT))
}

/// The duration `text` starts with, past any punctuation: numbers each followed by a unit, or one bare number of
/// seconds.
fn duration_at(text: &str) -> Option<Duration> {
    let mut rest = text.trim_start_matches(|c: char| !c.is_alphanumeric());
    let mut seconds = None;
    loop {
        let number = &rest[..rest.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(rest.len())];
        let Ok(n) = number.trim_end_matches('.').parse::<f64>() else { break };
        let after = rest[number.len()..].trim_start();
        let unit = &after[..after.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(after.len())];
        let scale = match unit {
            "" if seconds.is_none() => 1.0,
            "ms" | "msec" | "millisecond" | "milliseconds" => 0.001,
            "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
            "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3600.0,
            "d" | "day" | "days" => 86_400.0,
            _ => break,
        };
        seconds = Some(seconds.unwrap_or(0.0) + n * scale);
        if unit.is_empty() {
            break;
        }
        rest = after[unit.len()..].trim_start_matches([' ', ',']);
        rest = rest.strip_prefix("and ").unwrap_or(rest);
    }
    Duration::try_from_secs_f64(seconds?).ok()
}

/// How long messages wait after a limit: `first` after the first one since a message got past it, doubling with each
/// one after, at most `max`. A hint the error gives replaces the doubling, within the same bounds.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl Backoff {
    fn wait(&self, hits: u32, hint: Option<Duration>) -> Duration {
        let doubled = || self.first.saturating_mul(2u32.saturating_pow(hits.saturating_sub(1)));
        hint.unwrap_or_else(doubled).clamp(self.first.min(Duration::from_secs(1)), self.max)
    }
}

/// The backoff of each kind of limit.
#[derive(Debug, Clone, Copy)]
pub struct Backoffs {
    pub rate_limit: Backoff,
    pub quota: Backoff,
}

impl Backoffs {
    fn of(&self, kind: LimitKind) -> &Backoff {
        match kind {
            LimitKind::Quota => &self.quota,
            _ => &self.rate_limit,
        }
    }
}

/// 30 s doubling to 10 min for a rate limit, 15 min doubling to 2 h for an exhausted quota.
pub const BACKOFFS: Backoffs = Backoffs {
    rate_limit: Backoff { first: Duration::from_secs(30), max: Duration::from_mins(10) },
    quota: Backoff { first: Duration::from_mins(15), max: Duration::from_hours(2) },
};

/// Whether messages wait out a provider limit, and since when the harness has reported limits.
#[derive(Default)]
pub struct Throttle(Mutex<Period>);

#[derive(Default)]
struct Period {
    /// Prompts start again then; None while they run.
    until: Option<(Instant, DateTime<Utc>)>,
    /// When the first limit came since a message got past one.
    since: Option<DateTime<Utc>>,
    /// The waits since then, each a step of the backoff.
    hits: u32,
    /// A human has been told of an exhausted quota since then.
    told: bool,
}

/// How a limit holds the messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// What the roster entry shows.
    pub shown: HarnessLimit,
    /// Whether a human is to be told of it: an exhausted quota, first since a message got past a limit.
    pub tell: bool,
}

impl Throttle {
    /// Note that the harness reported `limited`: prompts wait out the backoff of `backoffs`. A limit met while they
    /// wait already only makes them wait longer when its hint says so.
    pub fn hit(&self, limited: &Limited, backoffs: &Backoffs) -> Hold {
        let mut period = self.period();
        let (now, wall) = (Instant::now(), Utc::now());
        let since = *period.since.get_or_insert(wall);
        let waiting = period.until.filter(|(at, _)| *at > now);
        if waiting.is_none() {
            period.hits += 1;
        }
        let wait = backoffs.of(limited.kind).wait(period.hits, limited.retry_after);
        let next = (now + wait, wall + chrono::Duration::from_std(wait).unwrap_or(chrono::Duration::MAX));
        let until = match waiting {
            Some(waiting) if limited.retry_after.is_none_or(|_| waiting.0 >= next.0) => waiting,
            _ => next,
        };
        period.until = Some(until);
        let tell = limited.kind == LimitKind::Quota && !std::mem::replace(&mut period.told, true);
        Hold { shown: HarnessLimit { kind: limited.kind, since, until: until.1 }, tell }
    }

    /// Whether prompts wait.
    pub fn is_held(&self) -> bool {
        self.period().until.is_some()
    }

    /// When prompts start again, while they wait.
    pub fn until(&self) -> Option<Instant> {
        self.period().until.map(|(at, _)| at)
    }

    /// Let prompts start again if their wait is over. Returns whether they may.
    pub fn lift(&self) -> bool {
        let mut period = self.period();
        if period.until.is_some_and(|(at, _)| at > Instant::now()) {
            return false;
        }
        period.until = None;
        true
    }

    /// Telling a human failed: the next limit of the same kind tries again.
    pub fn untold(&self) {
        self.period().told = false;
    }

    /// A message was handled without a limit: the next one starts the backoff over and is told again.
    pub fn settled(&self) {
        let mut period = self.period();
        if period.until.is_none() {
            *period = Period::default();
        }
    }

    fn period(&self) -> std::sync::MutexGuard<'_, Period> {
        self.0.lock().expect("throttle")
    }
}

/// The message on the thread of `envelope`, which found the quota of `ant`'s harness on `machine` exhausted, that
/// tells its owner, else the default human of `humans`, that messages wait and when they are tried again, at most
/// `max` apart. A reply to `envelope` unless it stands for a channel post.
#[allow(clippy::too_many_arguments)]
pub fn notice(
    ant: &UserId,
    envelope: &Envelope,
    post: bool,
    limited: &Limited,
    shown: &HarnessLimit,
    machine: &str,
    max: Duration,
    humans: &Registry,
) -> Envelope {
    let mut detail: String = limited.detail.trim().chars().take(DETAIL_CHARS).collect();
    if limited.detail.trim().chars().nth(DETAIL_CHARS).is_some() {
        detail.push('…');
    }
    let harness = &limited.harness;
    let until = shown.until.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let max = match max.as_secs() {
        s if s >= 3600 && s % 3600 == 0 => format!("{} h", s / 3600),
        s if s >= 60 && s % 60 == 0 => format!("{} min", s / 60),
        _ => format!("{max:?}"),
    };
    let text = format!(
        "[ants] {ant} cannot answer: harness {harness} on {machine} is out of quota ({detail}).\nMessages to {ant} \
         wait meanwhile, spending no attempts; they are tried again at {until}, then at most {max} apart until the \
         quota is back. Raising the provider plan's limit, or a quota reset, lets them through."
    );
    let owner = humans.human_owner(envelope.owner.as_ref());
    let mut notice = Envelope::on_thread(
        envelope.thread_id.clone(),
        ant.clone(),
        owner.clone().unwrap_or_else(|| humans.default_human().clone()),
        text,
    )
    .with_owner(owner)
    .with_creator(humans.ant_creator(envelope.creator.as_ref()))
    .with_origin_thread(envelope.origin_thread.clone());
    if !post {
        notice.in_reply_to = Some(envelope.id);
    }
    notice
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::Harnesses;

    fn classified(harness: &str, detail: &str) -> Option<(LimitKind, Option<Duration>)> {
        let harness = Harnesses::builtin().get(harness).unwrap();
        classify(&harness, detail).map(|l| (l.kind, l.retry_after))
    }

    #[test]
    fn provider_limits_are_told_apart_from_other_errors() {
        use LimitKind::{Quota, RateLimit};
        let secs = |s| Some(Duration::from_secs(s));
        let cases = [
            (
                "claude",
                r#"Internal error: API Error: 529 {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
                Some((RateLimit, None)),
            ),
            ("claude", "Internal error: API Error: Request rejected (429)", Some((RateLimit, None))),
            ("claude", "Internal error: You've hit your session limit · resets 3:45pm", Some((Quota, None))),
            ("claude", "Claude AI usage limit reached|4102444800", Some((Quota, Some(MAX_HINT)))),
            (
                "copilot",
                "Failed to get response from the AI model; retried 5 times. Last error: CAPIError: 429 429 Too Many \
                 Requests",
                Some((RateLimit, None)),
            ),
            ("copilot", "Sorry, you have been rate-limited. Please wait 2 minutes.", Some((RateLimit, None))),
            ("copilot", "You have exceeded your premium request allowance", Some((Quota, None))),
            (
                "antigravity",
                "Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 2h27m3s.",
                Some((RateLimit, secs(2 * 3600 + 27 * 60 + 3))),
            ),
            (
                "antigravity",
                r#"Quota exceeded for quota metric 'requests per minute'. "retryDelay": "23s""#,
                Some((RateLimit, secs(23))),
            ),
            (
                "copilot",
                "You've hit your usage limit. Upgrade to Pro or try again in 4 days 20 hours 9 minutes.",
                Some((Quota, secs(((4 * 24 + 20) * 60 + 9) * 60))),
            ),
            ("copilot", "Error: insufficient_quota; retry-after: 30", Some((RateLimit, secs(30)))),
            ("copilot", "Internal error: connection reset by peer", None),
            ("copilot", "Process exited: 2026-11-15T03:51:14.529 read line 429 of /src/429/main.rs", None),
            ("copilot", "Internal error: doomed", None),
        ];
        for (harness, detail, expected) in cases {
            assert_eq!(classified(harness, detail), expected, "{detail}");
        }
    }

    #[test]
    fn a_definition_adds_its_own_phrases() {
        let defined = crate::harness::parse_definitions(
            "[harnesses.custom]\ncommand = \"x\"\nrate_limited = [\"Slow Down\"]\nquota_exhausted = [\"plan cap\"]",
        )
        .unwrap();
        let harnesses = Harnesses::builtin().define(defined).unwrap();
        let custom = harnesses.get("custom").unwrap();
        assert_eq!(classify(&custom, "please SLOW DOWN").map(|l| l.kind), Some(LimitKind::RateLimit));
        assert_eq!(classify(&custom, "plan cap reached").map(|l| l.kind), Some(LimitKind::Quota));
        let copilot = harnesses.get("copilot").unwrap();
        assert_eq!(classify(&copilot, "please slow down"), None, "only the defining harness's phrases");
    }

    #[test]
    fn durations_are_read_in_their_common_forms() {
        let at = |text: &str| duration_at(text).map(|d| d.as_secs_f64());
        assert_eq!(at(": 30\n"), Some(30.0));
        assert_eq!(at(" 23.5s."), Some(23.5));
        assert_eq!(at(" 500ms"), Some(0.5));
        assert_eq!(at(" 1h 30m and 5 seconds"), Some(5405.0));
        assert_eq!(at(" a moment"), None);
        assert_eq!(at(" 3pm"), None);
        let now = "2026-01-01T00:00:00Z".parse().unwrap();
        assert_eq!(retry_after("limit reached|1767229200", now), Some(Duration::from_secs(3600)));
        assert_eq!(retry_after("retry in 30 days", now), Some(MAX_HINT));
    }

    #[test]
    fn waits_double_to_their_cap_and_a_quota_is_told_once() {
        let backoffs = Backoffs {
            rate_limit: Backoff { first: Duration::from_secs(30), max: Duration::from_secs(100) },
            quota: Backoff { first: Duration::from_secs(600), max: Duration::from_secs(3600) },
        };
        let limited = |kind, retry_after| Limited { harness: "h".to_owned(), kind, retry_after, detail: String::new() };
        let rate = limited(LimitKind::RateLimit, None);
        let throttle = Throttle::default();
        let waited = |hold: &Hold| (hold.shown.until - Utc::now()).num_seconds();

        let first = throttle.hit(&rate, &backoffs);
        assert!(throttle.is_held() && !first.tell, "a rate limit is waited out quietly");
        assert!((28..=30).contains(&waited(&first)), "{first:?}");
        assert_eq!(throttle.hit(&rate, &backoffs).shown.until, first.shown.until, "met while waiting: no new step");
        let hinted = throttle.hit(&limited(LimitKind::RateLimit, Some(Duration::from_secs(50))), &backoffs);
        assert!((48..=50).contains(&waited(&hinted)), "a later hint waits longer");
        assert!(!throttle.lift(), "not yet");

        let lifted = |throttle: &Throttle| {
            throttle.0.lock().unwrap().until = Some((Instant::now(), Utc::now()));
            assert!(throttle.lift() && !throttle.is_held());
        };
        lifted(&throttle);
        assert!((58..=60).contains(&waited(&throttle.hit(&rate, &backoffs))), "doubled");
        lifted(&throttle);
        assert!((98..=100).contains(&waited(&throttle.hit(&rate, &backoffs))), "at most the cap");

        lifted(&throttle);
        let quota = limited(LimitKind::Quota, None);
        let out = throttle.hit(&quota, &backoffs);
        assert!(out.tell && out.shown.kind == LimitKind::Quota && out.shown.since == first.shown.since);
        assert!((3598..=3600).contains(&waited(&out)), "the fourth step, at most the quota's cap");
        assert!(!throttle.hit(&quota, &backoffs).tell, "told once");
        throttle.untold();
        assert!(throttle.hit(&quota, &backoffs).tell, "telling failed: tell again");

        throttle.settled();
        assert!(throttle.is_held(), "a message handled while waiting ends nothing");
        lifted(&throttle);
        throttle.settled();
        let again = throttle.hit(&quota, &backoffs);
        assert!(again.tell && again.shown.since > first.shown.since, "a new period, told again");
        assert!((598..=600).contains(&waited(&again)), "the backoff starts over");
    }
}
