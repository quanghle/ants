//! What the daemon is doing, as its colony entry shows it.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::{
    Activity, ColonyEntry, HarnessAgent, HarnessFailing, HarnessLimit, MemoryLow, Paused, ServingFallback, SignInNeeded,
};
use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::sync::MutexExt as _;

/// What the daemon is doing, shared by the inbox loop and the heartbeats: the messages being handled, the live
/// harnesses, the agent the latest harness start named, whether the harness is signed out, held back by a provider
/// limit or failing to start, the fallback answering meanwhile, whether memory is low, whether the ant is paused, its
/// drift, the profile it published and why sense keeps none of its data.
#[derive(Debug, Default)]
pub struct Workload {
    handling: AtomicUsize,
    harnesses: AtomicUsize,
    shown: Mutex<Shown>,
    /// When the last message handled ended.
    worked: Mutex<Option<DateTime<Utc>>>,
    /// Wakes the heartbeats when the ant turns idle or working, its harness signed out or in or started with another
    /// agent, a provider limit starts or ends, the harness's start failures hold messages or no longer do, a fallback
    /// starts or stops answering, memory turns low or recovers, the ant is paused or resumed, or its drift, profile or
    /// why sense keeps none of its data changes, so the colony shows it before the next beat.
    changed: Notify,
}

/// The fields of a [`ColonyEntry`] a [`Workload`] holds as they are shown, each named as in the entry.
#[derive(Debug, Default, Clone)]
struct Shown {
    agent: Option<HarnessAgent>,
    signed_out: Option<SignInNeeded>,
    limited: Option<HarnessLimit>,
    failing: Option<HarnessFailing>,
    fallback: Option<ServingFallback>,
    memory_low: Option<MemoryLow>,
    paused: Option<Paused>,
    drift: Vec<String>,
    profile: Option<String>,
    sense_unkept: Option<String>,
}

impl Shown {
    /// Write every field into `entry`.
    fn stamp(self, entry: &mut ColonyEntry) {
        let Self { agent, signed_out, limited, failing, fallback, memory_low, paused, drift, profile, sense_unkept } =
            self;
        entry.agent = agent;
        entry.signed_out = signed_out;
        entry.limited = limited;
        entry.failing = failing;
        entry.fallback = fallback;
        entry.memory_low = memory_low;
        entry.paused = paused;
        entry.drift = drift;
        entry.profile = profile;
        entry.sense_unkept = sense_unkept;
    }
}

impl Workload {
    /// Count a message as handled until the returned guard drops.
    pub fn working(&self) -> Working<'_> {
        if self.handling.fetch_add(1, Ordering::Relaxed) == 0 {
            self.changed.notify_one();
        }
        Working(self)
    }

    pub fn set_harnesses(&self, live: usize) {
        self.harnesses.store(live, Ordering::Relaxed);
    }

    /// Set the field of [`Shown`] that `field` picks to `value`, waking the heartbeats if that changes it.
    fn set<T: PartialEq>(&self, field: impl FnOnce(&mut Shown) -> &mut T, value: T) {
        let mut shown = self.shown.locked();
        let current = field(&mut shown);
        if *current != value {
            *current = value;
            self.changed.notify_one();
        }
    }

    /// Note the agent a harness just started with named itself as, `None` when it did not name itself.
    pub fn set_agent(&self, agent: Option<HarnessAgent>) {
        self.set(|shown| &mut shown.agent, agent);
    }

    pub fn agent(&self) -> Option<HarnessAgent> {
        self.shown.locked().agent.clone()
    }

    /// Note the harness as signed out, or with `None` as signed in.
    pub fn set_signed_out(&self, signed_out: Option<SignInNeeded>) {
        self.set(|shown| &mut shown.signed_out, signed_out);
    }

    pub fn signed_out(&self) -> Option<SignInNeeded> {
        self.shown.locked().signed_out.clone()
    }

    /// Note the provider limit messages wait out, or with `None` that they no longer wait.
    pub fn set_limited(&self, limited: Option<HarnessLimit>) {
        self.set(|shown| &mut shown.limited, limited);
    }

    pub fn limited(&self) -> Option<HarnessLimit> {
        self.shown.locked().limited.clone()
    }

    /// Note that the harness's start failures hold the messages, or with `None` that they no longer do.
    pub fn set_failing(&self, failing: Option<HarnessFailing>) {
        self.set(|shown| &mut shown.failing, failing);
    }

    pub fn failing(&self) -> Option<HarnessFailing> {
        self.shown.locked().failing.clone()
    }

    /// Note the fallback answering for the ant's own harness, or with `None` that the ant's own answers again.
    pub fn set_fallback(&self, fallback: Option<ServingFallback>) {
        self.set(|shown| &mut shown.fallback, fallback);
    }

    pub fn fallback(&self) -> Option<ServingFallback> {
        self.shown.locked().fallback.clone()
    }

    /// Note that memory is low, or with `None` that it is not. Only turning low or recovering wakes the heartbeats.
    pub fn set_memory_low(&self, low: Option<MemoryLow>) {
        let mut shown = self.shown.locked();
        let changed = shown.memory_low.is_some() != low.is_some();
        shown.memory_low = low;
        if changed {
            self.changed.notify_one();
        }
    }

    pub fn memory_low(&self) -> Option<MemoryLow> {
        self.shown.locked().memory_low.clone()
    }

    /// Note the ant as paused, or with `None` as resumed.
    pub fn set_paused(&self, paused: Option<Paused>) {
        self.set(|shown| &mut shown.paused, paused);
    }

    pub fn paused(&self) -> Option<Paused> {
        self.shown.locked().paused.clone()
    }

    /// Note the fields of the ant's shared settings its machine's config sets otherwise.
    pub fn set_drift(&self, drift: Vec<String>) {
        self.set(|shown| &mut shown.drift, drift);
    }

    pub fn drift(&self) -> Vec<String> {
        self.shown.locked().drift.clone()
    }

    /// Note the profile the ant's confirmed persona holds, or with `None` that it holds none.
    pub fn set_profile(&self, profile: Option<String>) {
        self.set(|shown| &mut shown.profile, profile);
    }

    pub fn profile(&self) -> Option<String> {
        self.shown.locked().profile.clone()
    }

    /// Note why sense, the ant's provider, keeps none of its data here, or with `None` that it keeps them.
    pub fn set_sense_unkept(&self, why: Option<String>) {
        self.set(|shown| &mut shown.sense_unkept, why);
    }

    pub fn sense_unkept(&self) -> Option<String> {
        self.shown.locked().sense_unkept.clone()
    }

    pub fn handling(&self) -> usize {
        self.handling.load(Ordering::Relaxed)
    }

    /// When the last message handled ended; none before the first.
    pub fn worked(&self) -> Option<DateTime<Utc>> {
        *self.worked.locked()
    }

    pub fn harnesses(&self) -> usize {
        self.harnesses.load(Ordering::Relaxed)
    }

    pub fn activity(&self) -> Activity {
        if self.handling() == 0 { Activity::Idle } else { Activity::Working }
    }

    /// Write the current workload into `entry`.
    pub fn stamp(&self, entry: &mut ColonyEntry) {
        let count = |n: usize| u32::try_from(n).unwrap_or(u32::MAX);
        entry.activity = self.activity();
        entry.handling = count(self.handling());
        entry.harnesses = count(self.harnesses());
        let shown = self.shown.locked().clone();
        shown.stamp(entry);
    }

    /// Whether `entry` shows the current activity and every field a change of which wakes the heartbeats; the counts
    /// of messages in hand and of harnesses are not compared.
    pub(super) fn shown_by(&self, entry: &ColonyEntry) -> bool {
        let mut stamped = entry.clone();
        let shown = self.shown.locked().clone();
        shown.stamp(&mut stamped);
        entry.activity == self.activity() && stamped == *entry
    }

    /// Resolves once something the colony shows changed: at once if it did since the last wait.
    pub(super) async fn changed(&self) {
        self.changed.notified().await;
    }
}

/// A message being handled, counted in its [`Workload`] while alive.
pub struct Working<'a>(&'a Workload);

impl Drop for Working<'_> {
    fn drop(&mut self) {
        *self.0.worked.locked() = Some(Utc::now());
        if self.0.handling.fetch_sub(1, Ordering::Relaxed) == 1 {
            self.0.changed.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::colony::tests::entry;

    #[test]
    fn workload_counts_messages_in_hand_and_signals_turns() {
        let workload = Workload::default();
        let mut stamped = entry("dev-a", &[]);
        workload.set_harnesses(2);
        workload.stamp(&mut stamped);
        assert_eq!((stamped.activity, stamped.handling, stamped.harnesses), (Activity::Idle, 0, 2));

        let first = workload.working();
        let second = workload.working();
        workload.stamp(&mut stamped);
        assert_eq!((stamped.activity, stamped.handling), (Activity::Working, 2));
        drop(first);
        assert_eq!(workload.activity(), Activity::Working);
        drop(second);
        assert_eq!(workload.activity(), Activity::Idle);

        // Turning working and turning idle each leave a wakeup, which a later waiter takes at once.
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some());

        let out = SignInNeeded { since: Utc::now(), sign_in: Some("copilot login".to_owned()) };
        workload.set_signed_out(Some(out.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "signing out shows at once");
        workload.set_signed_out(Some(out.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "nothing new");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.signed_out, Some(out));
        workload.set_signed_out(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "signing in shows at once");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.signed_out, None);

        let agent = HarnessAgent { name: "copilot".to_owned(), version: "1.0.3".to_owned() };
        workload.set_agent(Some(agent.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a new agent shows at once");
        workload.set_agent(Some(agent.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "the same agent again");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.agent, Some(agent));

        let now = Utc::now();
        let limited = HarnessLimit { kind: ants_proto::LimitKind::Quota, since: now, until: now };
        workload.set_limited(Some(limited.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a limit shows at once");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.limited, Some(limited));
        workload.set_limited(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "its end too");

        let fallback = ServingFallback { name: "claude:opus".to_owned(), since: now };
        workload.set_fallback(Some(fallback.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a fallback shows at once");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.fallback, Some(fallback));

        let low = MemoryLow { since: now, available: 900 << 20, max_harnesses: 1 };
        workload.set_memory_low(Some(low.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "low memory shows at once");
        let lower = MemoryLow { available: 800 << 20, ..low };
        workload.set_memory_low(Some(lower.clone()));
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "a new reading waits a beat");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.memory_low, Some(lower));
        workload.set_memory_low(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "recovering too");

        let profile = Some("Good for Rust daemons.".to_owned());
        workload.set_profile(profile.clone());
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "a profile shows at once");
        workload.set_profile(profile.clone());
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "the same profile again");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.profile, profile);
        workload.set_profile(None);
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "its drop too");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.profile, None);

        let why = Some("no host credentials".to_owned());
        workload.set_sense_unkept(why.clone());
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_some(), "sense keeping none shows");
        workload.set_sense_unkept(why.clone());
        assert!(futures::FutureExt::now_or_never(workload.changed.notified()).is_none(), "the same reason again");
        workload.stamp(&mut stamped);
        assert_eq!(stamped.sense_unkept, why);
        workload.set_sense_unkept(None);
        workload.stamp(&mut stamped);
        assert_eq!(stamped.sense_unkept, None);
    }
}
