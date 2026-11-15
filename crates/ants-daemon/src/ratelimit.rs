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
use crate::sync::MutexExt as _;

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

/// How a harness that ends a failed prompt as a reply, not an ACP error, starts each piece of it: Copilot CLI's
/// `Error:` and `Info:` lines, Antigravity's `⚠ API error` ones, an `API Error:`.
const ERROR_MARKERS: &[&str] = &["Error:", "API Error:", "Info:", "⚠"];

/// The longest piece of a reply read as an error rather than as part of an answer.
const ERROR_CHARS: usize = 4096;

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
    let text = detail.to_lowercase().replace('\u{2019}', "'");
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

/// Whether `chunk`, a piece of a streamed reply, reads as a harness's error or notice rather than as part of an answer:
/// short and starting with one of [`ERROR_MARKERS`]. A reply made only of such pieces is the harness's report of a
/// failed prompt.
pub fn is_error_chunk(chunk: &str) -> bool {
    let chunk = chunk.trim_start();
    ERROR_MARKERS.iter().any(|marker| chunk.starts_with(marker)) && chunk.chars().nth(ERROR_CHARS).is_none()
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
    /// The wait is for an exhausted quota.
    quota: bool,
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
        period.quota = limited.kind == LimitKind::Quota || waiting.is_some() && period.quota;
        let tell = limited.kind == LimitKind::Quota && !std::mem::replace(&mut period.told, true);
        Hold { shown: HarnessLimit { kind: limited.kind, since, until: until.1 }, tell }
    }

    /// Whether prompts wait.
    pub fn is_held(&self) -> bool {
        self.period().until.is_some()
    }

    /// Whether prompts wait out an exhausted quota, so they may go to a fallback ([`crate::fallback`]) meanwhile.
    pub fn is_quota_held(&self) -> bool {
        let period = self.period();
        period.until.is_some() && period.quota
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
        self.0.locked()
    }
}

/// The message on the thread of `envelope`, which found the quota of `ant`'s harness on `machine` exhausted, that
/// tells its owner, else the default human of `humans`, that messages wait, or go to the ant's `fallbacks`
/// ([`crate::fallback`]), and when the harness is tried again, at most `max` apart. A reply to `envelope` unless it
/// stands for a channel post.
#[allow(clippy::too_many_arguments)]
pub fn notice(
    ant: &UserId,
    envelope: &Envelope,
    post: bool,
    limited: &Limited,
    shown: &HarnessLimit,
    machine: &str,
    max: Duration,
    fallbacks: &[String],
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
    let back = format!("{harness} once its quota is back, tried at {until}, then at most {max} apart");
    let meanwhile = crate::fallback::meanwhile(ant, fallbacks, &back).unwrap_or_else(|| {
        format!(
            "Messages to {ant} wait meanwhile, spending no attempts; they are tried again at {until}, then at most \
             {max} apart until the quota is back."
        )
    });
    let text = format!(
        "[ants] {ant} cannot answer{own}: harness {harness} on {machine} is out of quota ({detail}).\n{meanwhile} \
         Raising the provider plan's limit, or a quota reset, lets them through.",
        own = crate::fallback::own(fallbacks),
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

    /// Claude's adapter's ACP error, as its `Display` shows it: the message, then the data.
    fn acp_error(message: &str, kind: &str) -> String {
        format!("Internal error: {message}: {{\n  \"errorKind\": \"{kind}\"\n}}")
    }

    /// One attempt of Antigravity's reply when Gemini answers 429, as `agy` 1.5.8 streams it.
    fn gemini_429(attempt: u32, retry: &str, quota_id: &str, delay: &str) -> String {
        format!(
            "\n⚠ API error (attempt {attempt}): Error 429, Message: You exceeded your current quota, please check \
             your plan and billing details. For more information on this error, head to: \
             https://ai.google.dev/gemini-api/docs/rate-limits.\n* Quota exceeded for metric: \
             generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 10, model: \
             gemini-2.5-pro\nPlease retry in {retry}., Status: RESOURCE_EXHAUSTED, Details: \
             [map[@type:type.googleapis.com/google.rpc.QuotaFailure violations:[map[quotaDimensions:map[location:global \
             model:gemini-2.5-pro] quotaId:{quota_id} \
             quotaMetric:generativelanguage.googleapis.com/generate_content_free_tier_requests quotaValue:10]]] \
             map[@type:type.googleapis.com/google.rpc.Help links:[map[description:Learn more about Gemini API quotas \
             url:https://ai.google.dev/gemini-api/docs/rate-limits]]] map[@type:type.googleapis.com/google.rpc.RetryInfo \
             retryDelay:{delay}]]\n"
        )
    }

    const AGY_TERMINATED: &str = "\n⚠ Agent execution terminated due to error.\n";

    /// Copilot CLI's last reply chunk when every retry of a prompt failed.
    fn copilot_failed(waited: &str, last: &str) -> String {
        format!(
            "Error: Failed to get response from the AI model; retried 5 times (total retry wait time: {waited} \
             seconds) Last error: {last}"
        )
    }

    /// What the harnesses said when a fake provider turned their prompts away (claude-agent-acp 0.106.0 with Claude
    /// Code 2.1.325, Copilot CLI 1.0.112, agy 1.5.8), and what Copilot CLI's runtime says of GitHub's limits.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn provider_limits_are_told_apart_from_other_errors() {
        use LimitKind::{Quota, RateLimit};
        let secs = |s| Some(Duration::from_secs(s));
        let agy = |retry, quota_id, delay| {
            let mut reply: String = (1..=8).map(|n| gemini_429(n, retry, quota_id, delay)).collect();
            reply.push_str(AGY_TERMINATED);
            reply
        };
        let cases = [
            (
                "claude",
                acp_error(
                    "API Error: Repeated 529 Overloaded errors. The API is at capacity — this is usually temporary. Try \
                     again in a moment. If it persists, check your inference gateway (127.0.0.1:18765).",
                    "server_error",
                ),
                Some((RateLimit, None)),
            ),
            (
                "claude",
                acp_error(
                    "API Error: Request rejected (429) · This request would exceed the rate limit for your \
                     organization (00000000-0000-0000-0000-000000000000) of 50,000 input tokens per minute. For \
                     details, refer to: https://docs.claude.com/en/api/rate-limits. You can see the response headers \
                     for current usage. Please reduce the prompt length or the maximum tokens requested, or try again \
                     later.",
                    "rate_limit",
                ),
                Some((RateLimit, None)),
            ),
            (
                "claude",
                acp_error(
                    "API Error: Request rejected (429) · This request would exceed your account's rate limit. Please \
                     try again later.",
                    "rate_limit",
                ),
                Some((RateLimit, None)),
            ),
            ("claude", acp_error("You've hit your 5-hour limit · resets 6:43am (Asia/Tokyo)", "rate_limit"), Some((Quota, None))),
            ("claude", acp_error("You\u{2019}ve reached your Fable limit for this week.", "rate_limit"), Some((Quota, None))),
            ("claude", acp_error("Credit balance is too low", "billing_error"), Some((Quota, None))),
            ("claude", acp_error("spend limit reached · resets Aug 8", "billing_error"), Some((Quota, None))),
            ("claude", "Claude AI usage limit reached|4102444800".to_owned(), Some((Quota, Some(MAX_HINT)))),
            ("claude", acp_error("API Error: 500 Internal server error", "server_error"), None),
            (
                "copilot",
                format!("Info: Request failed due to a transient API error. Retrying...{}", copilot_failed("16.07", "529 Overloaded")),
                Some((RateLimit, None)),
            ),
            (
                "copilot",
                copilot_failed(
                    "108.55",
                    "429 Rate limit reached for gpt-5 in organization org-test on tokens per min (TPM): Limit 30000, \
                     Used 29000, Requested 2000. Please try again in 20s. Visit \
                     https://platform.openai.com/account/rate-limits to learn more.",
                ),
                Some((RateLimit, secs(20))),
            ),
            (
                "copilot",
                copilot_failed(
                    "56.70",
                    "429 You exceeded your current quota, please check your plan and billing details. For more \
                     information on this error, read the docs: \
                     https://platform.openai.com/docs/guides/error-codes/api-errors.",
                ),
                Some((Quota, None)),
            ),
            (
                "copilot",
                "You've hit the rate limit for this model. Please switch models or wait for your limit to reset (current \
                 limit resets in ~12 minutes)."
                    .to_owned(),
                Some((RateLimit, secs(12 * 60))),
            ),
            (
                "copilot",
                "You've reached your weekly rate limit. (current limit resets in ~2 days)".to_owned(),
                Some((Quota, secs(2 * 86_400))),
            ),
            ("copilot", "You've reached the spending limit for this session. Start a new session to continue.".to_owned(), Some((Quota, None))),
            ("copilot", "You've used all your Copilot Free chat requests for the month.".to_owned(), Some((Quota, None))),
            ("copilot", "No remaining quota for premium requests".to_owned(), Some((Quota, None))),
            ("copilot", copilot_failed("3.20", "500 Internal Server Error"), None),
            (
                "antigravity",
                agy("23.4s", "GenerateRequestsPerMinutePerProjectPerModel-FreeTier", "23s"),
                Some((RateLimit, secs(23))),
            ),
            (
                "antigravity",
                agy("7h3m2s", "GenerateRequestsPerDayPerProjectPerModel-FreeTier", "25382s"),
                Some((Quota, secs(25_382))),
            ),
            (
                "antigravity",
                format!(
                    "{}{AGY_TERMINATED}",
                    "\n⚠ API error (attempt 1): Error 503, Message: The model is overloaded. Please try again later., \
                     Status: UNAVAILABLE, Details: []\n"
                ),
                Some((RateLimit, None)),
            ),
            (
                "antigravity",
                "You have exhausted your capacity on this model. Your quota will reset after 2h27m3s.".to_owned(),
                Some((RateLimit, secs(2 * 3600 + 27 * 60 + 3))),
            ),
            ("antigravity", "Your AI credits balance is too low to continue.".to_owned(), Some((Quota, None))),
            ("copilot", "Error: insufficient_quota; retry-after: 30".to_owned(), Some((RateLimit, secs(30)))),
            ("copilot", "Internal error: connection reset by peer".to_owned(), None),
            ("copilot", "Process exited: 2026-11-15T03:51:14.529 read line 429 of /src/429/main.rs".to_owned(), None),
        ];
        for (harness, detail, expected) in cases {
            assert_eq!(classified(harness, &detail), expected, "{detail}");
        }
    }

    /// A reply is read as the harness's error only when every piece of it is an error or notice line, so an answer
    /// that discusses rate limits stays an answer.
    #[test]
    fn error_lines_are_told_from_answers() {
        let errors = [
            "Info: Request failed due to a transient API error. Retrying...".to_owned(),
            copilot_failed("16.07", "529 Overloaded"),
            gemini_429(1, "23.4s", "GenerateRequestsPerMinutePerProjectPerModel-FreeTier", "23s"),
            AGY_TERMINATED.to_owned(),
            "API Error: Request rejected (429)".to_owned(),
        ];
        for chunk in &errors {
            assert!(is_error_chunk(chunk), "{chunk}");
        }
        // An answer streams in pieces, few of which start like an error line.
        for chunk in ["Error", ": a rate limit of 429 means", "The API returned `Error: 429 Too Many Requests`"] {
            assert!(!is_error_chunk(chunk), "{chunk}");
        }
        assert!(!is_error_chunk(&format!("Error: {}", "rate limit ".repeat(ERROR_CHARS))), "too long for an error");
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
        assert!(!throttle.is_quota_held(), "a rate limit sends nothing to a fallback");
        assert!((28..=30).contains(&waited(&first)), "{first:?}");
        assert_eq!(throttle.hit(&rate, &backoffs).shown.until, first.shown.until, "met while waiting: no new step");
        let hinted = throttle.hit(&limited(LimitKind::RateLimit, Some(Duration::from_secs(50))), &backoffs);
        assert!((48..=50).contains(&waited(&hinted)), "a later hint waits longer");
        assert!(!throttle.lift(), "not yet");

        let lifted = |throttle: &Throttle| {
            throttle.0.locked().until = Some((Instant::now(), Utc::now()));
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
        assert!(throttle.is_quota_held());
        throttle.hit(&rate, &backoffs);
        assert!(throttle.is_quota_held(), "a rate limit met while the quota is waited out");
        assert!((3598..=3600).contains(&waited(&out)), "the fourth step, at most the quota's cap");
        assert!(!throttle.hit(&quota, &backoffs).tell, "told once");
        throttle.untold();
        assert!(throttle.hit(&quota, &backoffs).tell, "telling failed: tell again");

        throttle.settled();
        assert!(throttle.is_held(), "a message handled while waiting ends nothing");
        lifted(&throttle);
        assert!(!throttle.is_quota_held());
        throttle.settled();
        let again = throttle.hit(&quota, &backoffs);
        assert!(again.tell && again.shown.since > first.shown.since, "a new period, told again");
        assert!((598..=600).contains(&waited(&again)), "the backoff starts over");
    }
}
