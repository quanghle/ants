//! An ant whose harness is signed out: its messages wait, spending no attempts, a human is told once how to sign the
//! harness in, and the messages go on once a handshake with it succeeds.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_proto::{Envelope, UserId};
use chrono::{DateTime, Utc};

use crate::harness::SignedOut;
use crate::humans::Registry;
use crate::sync::MutexExt as _;

/// How often a daemon whose harness is signed out checks it ([`crate::harness::handshake`]).
pub const CHECK_EVERY: Duration = Duration::from_secs(60);

/// The most of what the harness said that a notice quotes.
const DETAIL_CHARS: usize = 500;

/// Whether the ant's harness is signed out, and whether a human has been told since it last worked.
#[derive(Default)]
pub struct SignIn(Mutex<Period>);

#[derive(Default)]
struct Period {
    /// The harness said it is signed out, and no handshake with it has succeeded since: messages wait.
    out: bool,
    /// When the harness said so first since it last answered, in milliseconds since the epoch.
    since: Option<u64>,
    /// A human has been told since then.
    told: bool,
}

impl SignIn {
    /// Whether messages wait for the harness to be signed in.
    pub fn is_out(&self) -> bool {
        self.period().out
    }

    /// When the harness said it is signed out first since it last answered, while messages wait for it.
    pub fn out_since(&self) -> Option<DateTime<Utc>> {
        let period = self.period();
        period.since.filter(|_| period.out).and_then(|ms| DateTime::from_timestamp_millis(ms as i64))
    }

    /// Note that the harness said it is signed out: messages wait until a handshake succeeds. Returns when that started,
    /// in milliseconds since the epoch, when no human has been told of it yet; the caller tells one.
    pub fn out(&self) -> Option<u64> {
        let mut period = self.period();
        period.out = true;
        let since = *period.since.get_or_insert_with(now_ms);
        (!std::mem::replace(&mut period.told, true)).then_some(since)
    }

    /// Telling a human failed: the next message that finds the harness signed out tries again.
    pub fn untold(&self) {
        self.period().told = false;
    }

    /// A handshake with the harness succeeded: messages go on. A message that finds it signed out still tells nobody
    /// again until one is handled past its sign-in ([`SignIn::settled`]).
    pub fn back(&self) {
        self.period().out = false;
    }

    /// A message was handled without the harness saying it is signed out: the next time it does, a human is told.
    pub fn settled(&self) {
        let mut period = self.period();
        if !period.out {
            *period = Period::default();
        }
    }

    fn period(&self) -> std::sync::MutexGuard<'_, Period> {
        self.0.locked()
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// The message on the thread of `envelope`, which found the harness of `ant` on `machine` signed out, that tells its
/// owner, else the default human of `humans`, how to sign the harness in. A reply to `envelope` unless it stands for a
/// channel post.
pub fn notice(
    ant: &UserId,
    envelope: &Envelope,
    post: bool,
    signed_out: &SignedOut,
    machine: &str,
    every: Duration,
    humans: &Registry,
) -> Envelope {
    let SignedOut { harness, sign_in, detail } = signed_out;
    let mut detail: String = detail.trim().chars().take(DETAIL_CHARS).collect();
    if signed_out.detail.trim().chars().nth(DETAIL_CHARS).is_some() {
        detail.push('…');
    }
    let how = match sign_in {
        Some(command) => format!("Run `{command}` on {machine}"),
        None => format!("Sign {harness} in on {machine}"),
    };
    let text = format!(
        "[ants] {ant} cannot answer: harness {harness} on {machine} is not signed in ({detail}).\n{how} as the user \
         the ants daemon runs as. Messages to {ant} wait meanwhile, spending no attempts, and are answered once a \
         check of the harness, every {every:?}, finds it signed in."
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

    #[test]
    fn a_human_is_told_once_per_signed_out_period() {
        let sign_in = SignIn::default();
        let since = sign_in.out().expect("first time: tell");
        assert!(sign_in.is_out());
        assert_eq!(sign_in.out_since().map(|t| t.timestamp_millis() as u64), Some(since));
        assert_eq!(sign_in.out(), None, "told already");
        sign_in.untold();
        assert_eq!(sign_in.out(), Some(since), "telling failed: tell again, of the same period");

        sign_in.back();
        assert!(!sign_in.is_out());
        assert_eq!(sign_in.out_since(), None, "messages go on");
        assert_eq!(sign_in.out(), None, "a handshake alone does not end the period");
        assert_eq!(sign_in.out_since().map(|t| t.timestamp_millis() as u64), Some(since), "of the same period");
        sign_in.settled();
        assert!(sign_in.is_out(), "a message handled while out does not end it either");

        sign_in.back();
        sign_in.settled();
        assert!(sign_in.out().is_some(), "signed out again after a message got past sign-in: tell again");
    }
}
