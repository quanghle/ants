//! What the human an escalation reached did about it since, which ends or holds back the reminders of it
//! ([`crate::escalation`]): marked the thread done after it, read it or a reminder of it, muted the thread, or
//! snoozed it. Read by direct gets, one key at a time, the ant's credentials allowing no other read of those buckets.

use std::sync::Arc;

use ants_proto::{
    JsDomain, NOTIFY_BUCKET, NotifyPrefs, READ_BUCKET, ReadMarker, Reminders, THREADS_BUCKET, ThreadDone, ThreadId,
    ThreadSnoozes, UserId, done_key, reminders_key,
};
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;

use crate::records::direct_kv::{DirectKv, Lookup};

/// What the human did about the escalation of a thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heeded {
    /// Nothing that stops a reminder.
    No,
    /// Marked the thread done after the escalation's latest message reached it.
    Done,
    /// Read the escalation or a reminder of it.
    Read,
    /// Muted the thread's notifications.
    Muted,
    /// Snoozed the thread until then.
    Snoozed(DateTime<Utc>),
}

/// Looks up what `human` did about the escalation on `thread` that reached it at the hub stream's sequences `seqs`.
pub type Heed = Arc<dyn Fn(UserId, ThreadId, Vec<u64>) -> BoxFuture<'static, Result<Heeded>> + Send + Sync>;

/// [`Heed`] on the hub's JetStream: the thread's done state and the snoozes in the human's own
/// [`Reminders`] in [`THREADS_BUCKET`], its
/// inbox [`ReadMarker`] in [`READ_BUCKET`] and its [`NotifyPrefs`] in [`NOTIFY_BUCKET`]. A bucket missing holds
/// nothing.
pub fn jetstream_heed(js: jetstream::Context, js_domain: Option<JsDomain>) -> Heed {
    Arc::new(move |human, thread, seqs| {
        let (js, js_domain) = (js.clone(), js_domain.clone());
        Box::pin(async move {
            let kv = |bucket| {
                let (js, js_domain) = (js.clone(), js_domain.clone());
                async move { DirectKv::new(&js, js_domain.as_ref(), bucket).await }
            };
            let threads = kv(THREADS_BUCKET).await?;
            let done = entry::<ThreadDone>(&threads, done_key(&thread)).await?;
            let read = entry::<ReadMarker>(&kv(READ_BUCKET).await?, human.as_str()).await?;
            let prefs = entry::<NotifyPrefs>(&kv(NOTIFY_BUCKET).await?, human.as_str()).await?;
            let snoozes = entry::<Reminders>(&threads, reminders_key(&human)).await?.map(|r| r.snoozed);
            let state = State { done, read, prefs, snoozes };
            Ok(state.heeded(&thread, &seqs, Utc::now()))
        })
    })
}

/// The entry under `key`, if any that parses.
async fn entry<T: DeserializeOwned>(kv: &DirectKv, key: impl AsRef<str>) -> Result<Option<T>> {
    Ok(match kv.lookup(key).await? {
        Lookup::Value(value) => serde_json::from_slice(&value).ok(),
        Lookup::NoValue | Lookup::NoBucket => None,
    })
}

/// What a human set that bears on the reminders of a thread's escalation.
#[derive(Debug, Default)]
pub struct State {
    pub done: Option<ThreadDone>,
    pub read: Option<ReadMarker>,
    pub prefs: Option<NotifyPrefs>,
    pub snoozes: Option<ThreadSnoozes>,
}

impl State {
    /// What it says the human did at `now` about the escalation of `thread` that reached it at `seqs`. Done and read
    /// need a sequence: an escalation kept in the machine's outbox has none.
    pub fn heeded(&self, thread: &ThreadId, seqs: &[u64], now: DateTime<Utc>) -> Heeded {
        let latest = seqs.iter().max();
        let done = self.done.as_ref().filter(|d| d.thread_id == *thread);
        if let (Some(done), Some(latest)) = (done, latest)
            && *latest <= done.after
        {
            return Heeded::Done;
        }
        if self.read.as_ref().is_some_and(|read| seqs.iter().any(|seq| read.is_read(*seq))) {
            return Heeded::Read;
        }
        if self.prefs.as_ref().is_some_and(|prefs| prefs.is_muted(thread, now)) {
            return Heeded::Muted;
        }
        match self.snoozes.as_ref().and_then(|snoozes| snoozes.get(thread, now)) {
            Some(snooze) => Heeded::Snoozed(snooze.until),
            None => Heeded::No,
        }
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{Snooze, UserId};
    use chrono::TimeDelta;

    use super::*;

    #[test]
    fn done_after_the_latest_read_muted_or_snoozed_each_heed_the_escalation() {
        let (thread, now) = (ThreadId::generate(), Utc::now());
        let done = |after| Some(ThreadDone::new(thread.clone(), UserId::human(), after));
        let heeded = |state: State, seqs: &[u64]| state.heeded(&thread, seqs, now);
        assert_eq!(heeded(State::default(), &[3]), Heeded::No);
        assert_eq!(heeded(State { done: done(5), ..State::default() }, &[3, 5]), Heeded::Done);
        assert_eq!(heeded(State { done: done(4), ..State::default() }, &[3, 5]), Heeded::No, "before the latest");
        assert_eq!(heeded(State { done: done(4), ..State::default() }, &[]), Heeded::No, "no sequence");

        let read = ReadMarker { below: 2, read: [5].into(), unread: Default::default() };
        assert_eq!(heeded(State { read: Some(read.clone()), ..State::default() }, &[3, 5]), Heeded::Read);
        assert_eq!(heeded(State { read: Some(read), ..State::default() }, &[3, 4]), Heeded::No);

        let mut prefs = NotifyPrefs::default();
        prefs.muted.insert(thread.clone(), now + TimeDelta::days(1));
        assert_eq!(heeded(State { prefs: Some(prefs.clone()), ..State::default() }, &[3]), Heeded::Muted);
        prefs.muted.insert(thread.clone(), now - TimeDelta::days(1));
        assert_eq!(heeded(State { prefs: Some(prefs), ..State::default() }, &[3]), Heeded::No, "the mute ended");

        let until = now + TimeDelta::hours(2);
        let snoozes = ThreadSnoozes([(thread.clone(), Snooze { at: now, until })].into());
        assert_eq!(heeded(State { snoozes: Some(snoozes), ..State::default() }, &[3]), Heeded::Snoozed(until));
    }
}
