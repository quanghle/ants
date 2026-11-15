//! An ant whose harness keeps failing to start, for a reason other than signing in or a provider limit: not
//! installed, a broken install, a bad harness definition or ant settings, a crash before a session opens. After
//! [`TRIP_FAILURES`] such failures on at least [`TRIP_THREADS`] threads within [`WINDOW`], a breaker opens: messages
//! wait, spending no attempts, a human is told once, and the messages go on once a check starts the harness.

use std::collections::{HashSet, VecDeque};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ants_proto::{Envelope, Escalation, EscalationReason, HarnessFailing, ThreadId, UserId};
use chrono::{DateTime, Utc};

use crate::humans::Registry;
use crate::sync::MutexExt as _;

/// Failures to start the harness that open the breaker, within [`WINDOW`] and on at least [`TRIP_THREADS`] threads.
pub const TRIP_FAILURES: usize = 3;
/// Threads the failures that open the breaker come from, at least, so one thread's trouble does not hold the others.
pub const TRIP_THREADS: usize = 2;
/// How far back failures to start the harness count.
pub const WINDOW: Duration = Duration::from_mins(10);

/// The most of the harness's error that the roster entry and a notice quote.
const DETAIL_CHARS: usize = 500;

/// A harness that failed to start or to open a session, for a reason other than signing in or a provider limit.
/// Nothing ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartFailed {
    pub harness: String,
    /// What went wrong.
    pub detail: String,
}

impl fmt::Display for StartFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for StartFailed {}

/// The harness's recent failures to start, and whether they hold the messages.
#[derive(Default)]
pub struct Breaker(Mutex<Period>);

#[derive(Default)]
struct Period {
    /// Failures to start the harness while the breaker is closed, oldest first, with their thread.
    failures: VecDeque<(Instant, ThreadId)>,
    /// Messages wait until a check starts the harness.
    open: bool,
    /// A check started the harness since the breaker opened, and no message got past the start since: the next
    /// failure opens it again at once.
    probation: bool,
    /// When the breaker opened first since a message last got past the start.
    since: Option<DateTime<Utc>>,
    /// The harness's last error.
    error: String,
    /// A human has been told since then.
    told: bool,
}

/// Messages wait for the harness to start ([`Breaker::failed`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hold {
    /// What the roster entry shows.
    pub shown: HarnessFailing,
    /// No human has been told yet: the caller tells one.
    pub tell: bool,
}

impl Breaker {
    /// Whether messages wait for a check to start the harness.
    pub fn is_open(&self) -> bool {
        self.period().open
    }

    /// Note that the harness failed to start for a message on `thread`, with `error`. Returns how messages are held
    /// when the breaker is open, now or already; None while they go on, each failure spending its attempt.
    pub fn failed(&self, thread: &ThreadId, error: &str) -> Option<Hold> {
        self.failed_at(thread, error, Instant::now())
    }

    fn failed_at(&self, thread: &ThreadId, error: &str, now: Instant) -> Option<Hold> {
        let mut period = self.period();
        period.error = excerpt(error);
        if !period.open {
            period.failures.retain(|(at, _)| now.saturating_duration_since(*at) < WINDOW);
            period.failures.push_back((now, thread.clone()));
            let threads: HashSet<&ThreadId> = period.failures.iter().map(|(_, t)| t).collect();
            let trips = period.probation || (period.failures.len() >= TRIP_FAILURES && threads.len() >= TRIP_THREADS);
            if !trips {
                return None;
            }
            period.open = true;
            period.failures.clear();
        }
        let since = *period.since.get_or_insert_with(Utc::now);
        let tell = !std::mem::replace(&mut period.told, true);
        Some(Hold { shown: HarnessFailing { since, error: period.error.clone() }, tell })
    }

    /// A check could not start the harness either, with `error`: what the roster entry shows now, while the breaker is
    /// open.
    pub fn still_failing(&self, error: &str) -> Option<HarnessFailing> {
        let mut period = self.period();
        period.error = excerpt(error);
        let since = period.since.filter(|_| period.open)?;
        Some(HarnessFailing { since, error: period.error.clone() })
    }

    /// Telling a human failed: the next message that finds the breaker open tries again.
    pub fn untold(&self) {
        self.period().told = false;
    }

    /// A check started the harness: messages go on. Until one gets past the start ([`Breaker::settled`]), the next
    /// failure holds them again at once, telling nobody again.
    pub fn closed(&self) {
        let mut period = self.period();
        period.open = false;
        period.probation = true;
    }

    /// A message was handled without the harness failing to start: earlier failures no longer count, and the next
    /// time the breaker opens, a human is told.
    pub fn settled(&self) {
        let mut period = self.period();
        if !period.open {
            *period = Period::default();
        }
    }

    fn period(&self) -> std::sync::MutexGuard<'_, Period> {
        self.0.locked()
    }
}

fn excerpt(text: &str) -> String {
    let mut excerpt: String = text.trim().chars().take(DETAIL_CHARS).collect();
    if text.trim().chars().nth(DETAIL_CHARS).is_some() {
        excerpt.push('…');
    }
    excerpt
}

/// The message on the thread of `envelope`, held because the harness of `ant` on `machine` keeps failing to start
/// with `error`, that tells its owner, else the default human of `humans`, how to check the harness. It carries a
/// level-0 `harness` escalation, so `ants webhook serve` pushes it. A reply to `envelope` unless it stands for a
/// channel post.
pub fn notice(
    ant: &UserId,
    envelope: &Envelope,
    post: bool,
    failed: &StartFailed,
    machine: &str,
    every: Duration,
    humans: &Registry,
) -> Envelope {
    let harness = &failed.harness;
    let detail = excerpt(&failed.detail);
    let text = format!(
        "[ants] {ant} cannot answer: harness {harness} on {machine} keeps failing to start ({detail}).\nRun `ants \
         harness check --harness {harness}` on {machine} as the user the ants daemon runs as, then fix the harness's \
         install, its [harnesses.{harness}] definition or the settings of {ant} in the config file. Messages to {ant} \
         wait meanwhile, spending no attempts, and are answered once a check of the harness, every {every:?}, starts \
         it."
    );
    let owner = humans.human_owner(envelope.owner.as_ref());
    let creator = humans.ant_creator(envelope.creator.as_ref());
    let escalation =
        Escalation::new_from(ant.clone(), envelope.thread_id.clone(), EscalationReason::Harness, creator.as_ref());
    let mut notice = Envelope::on_thread(
        envelope.thread_id.clone(),
        ant.clone(),
        owner.clone().unwrap_or_else(|| humans.default_human().clone()),
        text,
    )
    .with_owner(owner)
    .with_creator(creator)
    .with_origin_thread(envelope.origin_thread.clone())
    .with_escalation(Some(escalation));
    if !post {
        notice.in_reply_to = Some(envelope.id);
    }
    notice
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_failures_across_threads_within_the_window_hold_the_messages() {
        let breaker = Breaker::default();
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        let start = Instant::now();
        let at = |mins| start + Duration::from_mins(mins);
        assert_eq!(breaker.failed_at(&t1, "a", at(0)), None);
        assert_eq!(breaker.failed_at(&t1, "b", at(1)), None);
        assert_eq!(breaker.failed_at(&t1, "c", at(2)), None, "one thread alone holds nothing");
        assert_eq!(breaker.failed_at(&t2, "d", at(11)), None, "the first failures fell out of the window");
        assert!(!breaker.is_open());

        let hold = breaker.failed_at(&t2, "e", at(11)).expect("three failures on two threads within the window");
        assert!(breaker.is_open() && hold.tell);
        assert_eq!(hold.shown.error, "e");
        let again = breaker.failed_at(&t1, "f", at(13)).expect("held while open");
        assert!(!again.tell, "told once");
        assert_eq!((again.shown.since, again.shown.error.as_str()), (hold.shown.since, "f"));
        assert_eq!(breaker.still_failing("g").map(|f| f.error), Some("g".to_owned()));

        breaker.closed();
        assert!(!breaker.is_open());
        assert_eq!(breaker.still_failing("h"), None, "messages go on");
        let reopened = breaker.failed_at(&t1, "i", at(14)).expect("a failure right after a check opens it at once");
        assert_eq!((reopened.tell, reopened.shown.since), (false, hold.shown.since), "the same period");
        breaker.untold();
        assert!(breaker.failed_at(&t1, "j", at(14)).expect("open").tell, "telling failed: tell again");

        breaker.settled();
        assert!(breaker.is_open(), "a message handled while open does not close it");
        breaker.closed();
        breaker.settled();
        assert_eq!(breaker.failed_at(&t1, "k", at(15)), None, "a message got past the start: counting starts over");
    }
}
