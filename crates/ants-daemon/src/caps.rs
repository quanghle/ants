//! Token and cost caps per thread and per ant, counted from the spend harnesses report.
//!
//! Cost comes from ACP `usage_update` notifications, which carry the cumulative cost of a session; the increase over
//! the last reading of the thread's session is what a prompt spent. A reading below the last one, or from another
//! session, means the harness counts from zero again, so all of it is new. Tokens come from the usage in ACP prompt
//! responses, one prompt's worth each. A harness that reports neither, such as Copilot CLI, is never capped.
//!
//! What a thread spent since a human's last message on it is kept in the KV bucket `ants_sessions` under
//! `<ant>.<thread>.spend`, and what the ant spent in a UTC day under `<ant>.day.<YYYY-MM-DD>`, so both
//! survive harness and daemon restarts. A message from any human gives the thread a fresh allowance; the ant's
//! allowance comes back at 00:00 UTC. Spend is kept whether or not the ant has caps, for `ants spend` to report.
//!
//! Spend first passing a share of a cap short of it gives a [`Warning`], once per thread allowance or UTC day: that it
//! was given is kept with the spend, so restarts and later prompts do not give it again.
//!
//! The thread's escalation ([`Pending`]) is kept with its spend, so its level survives restarts too, as is the
//! reminder of one that reached a human ([`Reminder`]), and the threads whose escalation has a deadline or a reminder
//! due, with the earlier of the two, under `<ant>.escalations.deadlines`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::{
    ChannelId, Escalation, ThreadId, UserId, daily_spend_key, escalation_deadlines_key, thread_spend_key,
};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::direct_kv::DirectKv;
use crate::harness::Spend;

/// Spend caps; None is no cap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct Caps {
    /// Tokens a thread may spend between messages from a human.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from a human, in the harness's unit (USD for Claude).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_cost: Option<f64>,
    /// Tokens the ant may spend in a UTC day.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_tokens: Option<u64>,
    /// Cost the ant may spend in a UTC day.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub daily_cost: Option<f64>,
}

impl Caps {
    fn any(&self) -> bool {
        self.thread_tokens.is_some()
            || self.thread_cost.is_some()
            || self.daily_tokens.is_some()
            || self.daily_cost.is_some()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ThreadSpend {
    /// The session of the last cost reading, and the reading.
    session: Option<String>,
    reading: Option<f64>,
    tokens: u64,
    cost: f64,
    /// The thread's escalation, from when it first went anywhere until a human's message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    escalation: Option<Pending>,
    /// The reminder of the thread's escalation that reached a human, until a human's message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reminder: Option<Reminder>,
    #[serde(default, skip_serializing_if = "Warned::none")]
    warned: Warned,
}

/// What the ant spent in a UTC day, stored as [`Spent`] is, with the warnings given.
#[derive(Debug, Default, Serialize, Deserialize)]
struct DaySpend {
    tokens: u64,
    cost: f64,
    #[serde(default, skip_serializing_if = "Warned::none")]
    warned: Warned,
}

/// The caps whose warning was given.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct Warned {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    tokens: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    cost: bool,
}

impl Warned {
    fn none(&self) -> bool {
        !self.tokens && !self.cost
    }
}

/// Spend that passed the warning share of a cap, short of the cap.
#[derive(Debug, Clone, PartialEq)]
pub struct Warning {
    /// The cap's config key, such as `max_daily_cost`.
    pub key: &'static str,
    /// What was spent against the cap, as `ants spend` shows it: `850/1000 tokens` or `4.10/5.00`.
    pub figure: String,
    /// The share of the cap spent, in percent, rounded down.
    pub percent: u64,
}

impl Warning {
    /// Whether it is about the ant's daily allowance rather than the thread's.
    pub fn daily(&self) -> bool {
        self.key.starts_with("max_daily_")
    }

    /// The notice telling a human of it, for `ant` spending on `thread`.
    pub fn text(&self, ant: &UserId, thread: &ThreadId) -> String {
        let Self { key, figure, percent } = self;
        let raise = format!(
            "To raise the cap, set {key} of {ant} in the config file of the daemon serving it and reload the daemon \
             (SIGHUP)."
        );
        if self.daily() {
            format!(
                "[ants] {ant} spent {figure} ({percent}%) of {key} today (UTC), last on thread {thread}. At the cap, \
                 its prompts are cancelled and escalated until 00:00 UTC. {raise} `ants spend {ant}` shows its spend."
            )
        } else {
            format!(
                "[ants] {ant} spent {figure} ({percent}%) of {key} on thread {thread} since a human's last message \
                 there. At the cap, its prompt there is cancelled and the thread escalated. A human's message on the \
                 thread gives it a fresh allowance. {raise} `ants spend {ant} --thread {thread}` shows its spend."
            )
        }
    }
}

/// What counting a harness's report found: the cap reached, if any, and the warnings due.
#[derive(Debug, Default, PartialEq)]
pub struct Recorded {
    pub reached: Option<String>,
    pub warnings: Vec<Warning>,
}

/// Where a thread's escalation stands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pending {
    /// The escalation as it last went out.
    pub escalation: Escalation,
    /// Whom it went to, its leader first; empty when it went to the thread's owner, else the default human.
    #[serde(default)]
    pub to: Vec<UserId>,
    /// The leader's message that gave the thread a fresh budget and allowance at this level, if one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renewed_by: Option<Uuid>,
    /// When it goes one level higher unless its leader answers first, in Unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<i64>,
    /// The thread's owner, a human, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The ant that created the thread with a channel post, if one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    /// The channel thread the thread was spun off, whose creator it has
    /// ([`ants_proto::Envelope::origin_thread`]), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_thread: Option<ThreadId>,
    /// What the escalation is about, for the escalations that follow it up.
    #[serde(default)]
    pub summary: String,
    /// The channel of the thread, when the ant got stuck on a post there: the leader's answer is posted back to it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
}

impl Pending {
    /// Whether it went to the thread's owner rather than up a chain.
    pub fn at_owner(&self) -> bool {
        self.to.is_empty()
    }
}

/// When to remind a human of a thread's escalation that reached it, until a human's message on the thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reminder {
    /// The escalation as it last reached the human.
    pub escalation: Escalation,
    /// The human it reached: the thread's owner, else the default human.
    pub to: UserId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creator: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_thread: Option<ThreadId>,
    /// What the escalation is about.
    #[serde(default)]
    pub summary: String,
    /// When an escalation of the thread first reached a human, in Unix milliseconds.
    pub since: i64,
    /// When the next reminder is due, in Unix milliseconds.
    pub at: i64,
    /// Reminders sent so far.
    #[serde(default)]
    pub sent: u32,
}

/// Tokens and cost spent: by the ant in a UTC day, or by a thread since a human's last message on it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Spent {
    pub tokens: u64,
    pub cost: f64,
}

impl std::ops::AddAssign for Spent {
    fn add_assign(&mut self, other: Self) {
        self.tokens += other.tokens;
        self.cost += other.cost;
    }
}

/// What `ant` spent on `day` (`YYYY-MM-DD`, UTC), read from [`ants_proto::SESSIONS_BUCKET`]; None when nothing is
/// stored.
pub async fn day_spent(kv: &DirectKv, ant: &UserId, day: &str) -> Result<Option<Spent>> {
    read(kv, daily_spend_key(ant, day)).await
}

/// What `ant` spent on `thread` since a human's last message on it, read from [`ants_proto::SESSIONS_BUCKET`]; None
/// when nothing is stored.
pub async fn thread_spent(kv: &DirectKv, ant: &UserId, thread: &ThreadId) -> Result<Option<Spent>> {
    let spent: Option<ThreadSpend> = read(kv, thread_spend_key(ant, thread)).await?;
    Ok(spent.map(|s| Spent { tokens: s.tokens, cost: s.cost }))
}

async fn read<T: for<'de> Deserialize<'de>>(kv: &DirectKv, key: String) -> Result<Option<T>> {
    match kv.get(&key).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).with_context(|| format!("parsing {key}"))?)),
        None => Ok(None),
    }
}

#[derive(Clone)]
enum Store {
    Kv(Box<DirectKv>),
    Memory(Arc<Mutex<HashMap<String, Vec<u8>>>>),
}

impl Store {
    async fn get<T: for<'de> Deserialize<'de> + Default>(&self, key: String) -> Result<T> {
        let bytes = match self {
            Self::Kv(kv) => kv.get(&key).await?,
            Self::Memory(map) => map.lock().expect("spend store").get(&key).cloned(),
        };
        match bytes {
            Some(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("parsing {key}")),
            None => Ok(T::default()),
        }
    }

    async fn put<T: Serialize>(&self, key: String, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        match self {
            Self::Kv(kv) => {
                kv.put(&key, bytes).await?;
            }
            Self::Memory(map) => {
                map.lock().expect("spend store").insert(key, bytes);
            }
        }
        Ok(())
    }
}

/// What the ant and its threads spent, against their caps.
pub struct Ledger {
    caps: Caps,
    ant: UserId,
    store: Store,
    /// The share of a cap, in percent, past which spend gives a [`Warning`]; 0 for none.
    warn_percent: u8,
    /// The ant's spend of the day it is for; held while it is read or stored, so updates do not cross.
    day: tokio::sync::Mutex<Option<(String, DaySpend)>>,
    /// The escalation deadlines by thread, once read; held while a thread's spend is read and stored.
    threads: tokio::sync::Mutex<Option<BTreeMap<ThreadId, i64>>>,
}

impl Ledger {
    /// Spend kept in the KV bucket [`ants_proto::SESSIONS_BUCKET`].
    pub fn kv(kv: DirectKv, ant: UserId, caps: Caps) -> Self {
        Self::with_store(Store::Kv(Box::new(kv)), ant, caps)
    }

    /// Spend kept in this process only.
    pub fn memory(ant: UserId, caps: Caps) -> Self {
        Self::with_store(Store::Memory(Arc::default()), ant, caps)
    }

    fn with_store(store: Store, ant: UserId, caps: Caps) -> Self {
        let (day, threads) = (tokio::sync::Mutex::default(), tokio::sync::Mutex::default());
        Self { caps, ant, store, warn_percent: 0, day, threads }
    }

    /// Give a [`Warning`] once spend passes `percent` of a cap; 0 for never.
    pub fn warn_at(self, percent: u8) -> Self {
        Self { warn_percent: percent, ..self }
    }

    /// The thread starts over from nothing spent.
    pub async fn fresh_allowance(&self, thread: &ThreadId) -> Result<()> {
        if !self.caps.any() {
            return Ok(());
        }
        self.update(thread, |spent| (forget_spend(spent), ())).await
    }

    /// A message from a human: the thread starts over from nothing spent, and its escalation is over, with its
    /// reminder. Returns whether it had either.
    pub async fn reset(&self, thread: &ThreadId) -> Result<bool> {
        self.update(thread, |spent| {
            let escalated = spent.escalation.take().is_some() | spent.reminder.take().is_some();
            (forget_spend(spent) || escalated, escalated)
        })
        .await
    }

    /// The reminder of the thread's escalation, if it has one.
    pub async fn reminder(&self, thread: &ThreadId) -> Result<Option<Reminder>> {
        let spent: ThreadSpend = self.store.get(thread_spend_key(&self.ant, thread)).await?;
        Ok(spent.reminder)
    }

    /// Remind of the thread's escalation as `reminder` says, keeping when one first reached a human and the reminders
    /// sent from the reminder the thread has, if any.
    pub async fn arm_reminder(&self, thread: &ThreadId, reminder: Reminder) -> Result<()> {
        self.update(thread, |spent| {
            let kept = spent.reminder.as_ref().map(|r| (r.since, r.sent));
            let (since, sent) = kept.unwrap_or((reminder.since, reminder.sent));
            spent.reminder = Some(Reminder { since, sent, ..reminder });
            (true, ())
        })
        .await
    }

    /// A reminder of the thread's escalation was sent: the next one is due `next`, or none is for None. A thread whose
    /// escalation a human ended meanwhile stays without one.
    pub async fn reminded(&self, thread: &ThreadId, next: Option<i64>) -> Result<()> {
        self.update(thread, |spent| match next {
            Some(at) => match &mut spent.reminder {
                Some(r) => {
                    (r.sent, r.at) = (r.sent + 1, at);
                    (true, ())
                }
                None => (false, ()),
            },
            None => (spent.reminder.take().is_some(), ()),
        })
        .await
    }

    /// The thread's escalation, if it has one.
    pub async fn pending(&self, thread: &ThreadId) -> Result<Option<Pending>> {
        let spent: ThreadSpend = self.store.get(thread_spend_key(&self.ant, thread)).await?;
        Ok(spent.escalation)
    }

    /// Keep `pending` as the thread's escalation.
    pub async fn set_pending(&self, thread: &ThreadId, pending: Option<Pending>) -> Result<()> {
        self.update(thread, |spent| {
            let changed = spent.escalation != pending;
            spent.escalation = pending;
            (changed, ())
        })
        .await
    }

    /// A message from `leader` answering the thread's escalation: the first one at the escalation's level gives the
    /// thread a fresh allowance and ends the deadline. Returns whether `by`, that message, did (again, when it is
    /// delivered again).
    pub async fn renew(&self, thread: &ThreadId, leader: &UserId, by: Uuid) -> Result<bool> {
        self.update(thread, |spent| match &mut spent.escalation {
            Some(p) if p.escalation.is_leader(leader) && !p.at_owner() && p.renewed_by.is_none_or(|id| id == by) => {
                let changed = p.renewed_by != Some(by) || p.deadline.is_some();
                (p.renewed_by, p.deadline) = (Some(by), None);
                (forget_spend(spent) || changed, true)
            }
            _ => (false, false),
        })
        .await
    }

    /// A member sent the thread's escalation up to `escalation`, a level above the thread's, with members `to`: it
    /// becomes the thread's, waiting for its leader until `deadline`. Returns whether it did; an escalation a human
    /// ended, or one at or below the thread's level, stays as it is.
    pub async fn climbed(
        &self,
        thread: &ThreadId,
        escalation: &Escalation,
        to: Vec<UserId>,
        deadline: Option<i64>,
    ) -> Result<bool> {
        self.update(thread, |spent| match &mut spent.escalation {
            Some(p) if escalation.level > p.escalation.level => {
                (p.escalation, p.to, p.renewed_by, p.deadline) = (escalation.clone(), to, None, deadline);
                (true, true)
            }
            _ => (false, false),
        })
        .await
    }

    /// The escalation deadlines and reminders due by thread, the earlier of the two. One may be stale:
    /// [`Ledger::recheck`] drops it.
    pub async fn deadlines(&self) -> Result<BTreeMap<ThreadId, i64>> {
        let mut index = self.threads.lock().await;
        Ok(self.load_index(&mut index).await?.clone())
    }

    /// Bring the thread's deadline in [`Ledger::deadlines`] in line with its escalation and reminder.
    pub async fn recheck(&self, thread: &ThreadId) -> Result<()> {
        self.update(thread, |_| (false, ())).await
    }

    /// Read, change with `change` and store the thread's spend, when `change` says it changed it, keeping the deadlines
    /// in line: the earlier of the escalation's deadline and its reminder's. A deadline is listed before its escalation
    /// is stored, and unlisted after, so a crash in between leaves a stale entry rather than a lost one.
    async fn update<R>(&self, thread: &ThreadId, change: impl FnOnce(&mut ThreadSpend) -> (bool, R)) -> Result<R> {
        let mut index = self.threads.lock().await;
        let index = self.load_index(&mut index).await?;
        let key = thread_spend_key(&self.ant, thread);
        let mut spent: ThreadSpend = self.store.get(key.clone()).await?;
        let (changed, result) = change(&mut spent);
        let deadline = spent.escalation.as_ref().and_then(|p| p.deadline);
        let deadline = deadline.into_iter().chain(spent.reminder.as_ref().map(|r| r.at)).min();
        let listed = index.get(thread).copied();
        if let Some(deadline) = deadline.filter(|d| Some(*d) != listed) {
            index.insert(thread.clone(), deadline);
            self.store.put(escalation_deadlines_key(&self.ant), index).await?;
        }
        if changed {
            self.store.put(key, &spent).await?;
        }
        if deadline.is_none() && index.remove(thread).is_some() {
            self.store.put(escalation_deadlines_key(&self.ant), index).await?;
        }
        Ok(result)
    }

    async fn load_index<'a>(
        &self,
        index: &'a mut Option<BTreeMap<ThreadId, i64>>,
    ) -> Result<&'a mut BTreeMap<ThreadId, i64>> {
        if index.is_none() {
            *index = Some(self.store.get(escalation_deadlines_key(&self.ant)).await?);
        }
        Ok(index.as_mut().expect("loaded"))
    }

    /// The cap the thread or the ant has reached, if any.
    pub async fn reached(&self, thread: &ThreadId) -> Result<Option<String>> {
        self.reached_on(thread, &today()).await
    }

    /// Count what a harness reported for the thread. Returns the cap reached, if any, and the warnings due.
    pub async fn record(&self, thread: &ThreadId, spend: &Spend) -> Result<Recorded> {
        self.record_on(thread, spend, &today()).await
    }

    async fn reached_on(&self, thread: &ThreadId, today: &str) -> Result<Option<String>> {
        if !self.caps.any() {
            return Ok(None);
        }
        let spent: ThreadSpend = self.store.get(thread_spend_key(&self.ant, thread)).await?;
        let mut day = self.day.lock().await;
        let daily = self.load_day(&mut day, today).await?;
        Ok(self.check(&spent, daily))
    }

    /// Counted whether or not the ant has caps, so `ants spend` reports it.
    async fn record_on(&self, thread: &ThreadId, spend: &Spend, today: &str) -> Result<Recorded> {
        let caps = self.caps;
        let (spent, tokens, cost, mut warnings) = self
            .update(thread, |spent| {
                let (tokens, cost) = match spend {
                    Spend::Tokens(n) => (*n, 0.0),
                    Spend::Cost { session, total } => (0, cost_increase(spent, session, *total)),
                };
                spent.tokens += tokens;
                spent.cost += cost;
                let total = Spent { tokens: spent.tokens, cost: spent.cost };
                let (tokens_cap, cost_cap) =
                    (("max_thread_tokens", caps.thread_tokens), ("max_thread_cost", caps.thread_cost));
                let warnings = self.warnings(total, tokens_cap, cost_cap, &mut spent.warned);
                let total = ThreadSpend { tokens: total.tokens, cost: total.cost, ..ThreadSpend::default() };
                (true, (total, tokens, cost, warnings))
            })
            .await?;
        crate::metrics::ant(&self.ant).spent(tokens, cost);

        let mut day = self.day.lock().await;
        let daily = self.load_day(&mut day, today).await?;
        if tokens > 0 || cost > 0.0 {
            daily.tokens += tokens;
            daily.cost += cost;
            let total = Spent { tokens: daily.tokens, cost: daily.cost };
            let (tokens_cap, cost_cap) = (("max_daily_tokens", caps.daily_tokens), ("max_daily_cost", caps.daily_cost));
            warnings.extend(self.warnings(total, tokens_cap, cost_cap, &mut daily.warned));
            self.store.put(daily_spend_key(&self.ant, today), daily).await?;
        }
        Ok(Recorded { reached: self.check(&spent, daily), warnings })
    }

    /// The warnings `spent` is due against a token and a cost cap, each named by its config key: one for each cap it
    /// passed the warning share of for the first time, short of the cap, noted in `warned`. A cap passed at once is
    /// noted without a warning, as reaching it tells of it.
    fn warnings(
        &self,
        spent: Spent,
        (tokens_key, tokens_cap): (&'static str, Option<u64>),
        (cost_key, cost_cap): (&'static str, Option<f64>),
        warned: &mut Warned,
    ) -> Vec<Warning> {
        let percent = u64::from(self.warn_percent);
        let mut warnings = Vec::new();
        if percent == 0 {
            return warnings;
        }
        if let Some(cap) = tokens_cap
            && !warned.tokens
            && u128::from(spent.tokens) * 100 >= u128::from(cap) * u128::from(percent)
        {
            warned.tokens = true;
            if spent.tokens < cap {
                let figure = format!("{}/{cap} tokens", spent.tokens);
                let percent = (u128::from(spent.tokens) * 100 / u128::from(cap)) as u64;
                warnings.push(Warning { key: tokens_key, figure, percent });
            }
        }
        if let Some(cap) = cost_cap
            && !warned.cost
            && spent.cost * 100.0 >= cap * percent as f64
        {
            warned.cost = true;
            if spent.cost < cap {
                let figure = format!("{:.2}/{cap:.2}", spent.cost);
                warnings.push(Warning { key: cost_key, figure, percent: (spent.cost / cap * 100.0) as u64 });
            }
        }
        warnings
    }

    /// The ant's spend on `today`, read from the store when the day held is another one.
    async fn load_day<'a>(&self, day: &'a mut Option<(String, DaySpend)>, today: &str) -> Result<&'a mut DaySpend> {
        if day.as_ref().is_none_or(|(d, _)| d != today) {
            let spent = self.store.get(daily_spend_key(&self.ant, today)).await?;
            *day = Some((today.to_owned(), spent));
        }
        Ok(&mut day.as_mut().expect("loaded").1)
    }

    fn check(&self, thread: &ThreadSpend, day: &DaySpend) -> Option<String> {
        let caps = &self.caps;
        let since = "since a human's last message";
        let resets = "today; resets at 00:00 UTC";
        let ant = &self.ant;
        if let Some(cap) = caps.thread_tokens.filter(|cap| thread.tokens >= *cap) {
            return Some(format!("Thread token cap reached ({} of {cap} tokens {since})", thread.tokens));
        }
        if let Some(cap) = caps.thread_cost.filter(|cap| thread.cost >= *cap) {
            return Some(format!("Thread cost cap reached ({:.2} of {cap:.2} {since})", thread.cost));
        }
        if let Some(cap) = caps.daily_tokens.filter(|cap| day.tokens >= *cap) {
            return Some(format!("Daily token cap of {ant} reached ({} of {cap} tokens {resets})", day.tokens));
        }
        if let Some(cap) = caps.daily_cost.filter(|cap| day.cost >= *cap) {
            return Some(format!("Daily cost cap of {ant} reached ({:.2} of {cap:.2} {resets})", day.cost));
        }
        None
    }
}

/// Forget what the thread spent, and the warnings given of it. Returns whether it had spent anything.
fn forget_spend(spent: &mut ThreadSpend) -> bool {
    let had = spent.tokens > 0 || spent.cost > 0.0 || !spent.warned.none();
    (spent.tokens, spent.cost, spent.warned) = (0, 0.0, Warned::default());
    had
}

/// What a cumulative cost reading of `session` adds to the thread's spend, keeping it as the last reading.
fn cost_increase(spent: &mut ThreadSpend, session: &SessionId, total: f64) -> f64 {
    let session = session.to_string();
    let increase = match spent.reading {
        Some(last) if spent.session.as_ref() == Some(&session) && total >= last => total - last,
        _ => total,
    };
    (spent.session, spent.reading) = (Some(session), Some(total));
    increase
}

fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: &str = "2026-11-13";

    fn ant() -> UserId {
        "dev-a".parse().unwrap()
    }

    fn thread(n: u8) -> ThreadId {
        format!("t{n}").parse().unwrap()
    }

    fn cost(session: &str, total: f64) -> Spend {
        Spend::Cost { session: SessionId::new(session), total }
    }

    #[tokio::test]
    async fn cost_readings_count_what_each_adds() {
        let caps = Caps { thread_cost: Some(1.0), ..Caps::default() };
        let ledger = Ledger::memory(ant(), caps);
        let t = thread(1);
        assert_eq!(ledger.record_on(&t, &cost("s1", 0.25), DAY).await.unwrap().reached, None);
        assert_eq!(ledger.record_on(&t, &cost("s1", 0.5), DAY).await.unwrap().reached, None);
        // The harness restarted and counts from zero again: 0.25 + 0.25 + 0.25.
        assert_eq!(ledger.record_on(&t, &cost("s1", 0.25), DAY).await.unwrap().reached, None);
        let reached =
            ledger.record_on(&t, &cost("s2", 0.3), DAY).await.unwrap().reached.expect("a new session adds it all");
        assert!(reached.starts_with("Thread cost cap reached (1.05 of 1.00"), "{reached}");
        assert_eq!(ledger.reached_on(&thread(2), DAY).await.unwrap(), None, "other threads have their own");
    }

    #[tokio::test]
    async fn spend_is_kept_without_caps() {
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), Caps::default());
        assert_eq!(ledger.record_on(&thread(1), &Spend::Tokens(70), DAY).await.unwrap().reached, None);
        assert_eq!(ledger.record_on(&thread(1), &cost("s1", 0.5), DAY).await.unwrap().reached, None);
        let day: Spent = store.get(daily_spend_key(&ant(), DAY)).await.unwrap();
        assert_eq!(day, Spent { tokens: 70, cost: 0.5 });
        let spent: ThreadSpend = store.get(thread_spend_key(&ant(), &thread(1))).await.unwrap();
        assert_eq!((spent.tokens, spent.cost), (70, 0.5));
    }

    #[tokio::test]
    async fn a_message_from_the_human_frees_the_thread_but_not_the_ant() {
        let caps = Caps { thread_tokens: Some(100), daily_tokens: Some(250), ..Caps::default() };
        let ledger = Ledger::memory(ant(), caps);
        let (t1, t2) = (thread(1), thread(2));
        let reached = ledger.record_on(&t1, &Spend::Tokens(120), DAY).await.unwrap().reached.unwrap();
        assert!(reached.starts_with("Thread token cap reached (120 of 100"), "{reached}");
        ledger.fresh_allowance(&t1).await.unwrap();
        assert_eq!(ledger.reached_on(&t1, DAY).await.unwrap(), None);
        assert_eq!(ledger.record_on(&t2, &Spend::Tokens(90), DAY).await.unwrap().reached, None);
        let reached = ledger.record_on(&t1, &Spend::Tokens(50), DAY).await.unwrap().reached.unwrap();
        assert!(reached.starts_with("Daily token cap of dev-a reached (260 of 250"), "{reached}");
        ledger.fresh_allowance(&t1).await.unwrap();
        assert!(ledger.reached_on(&t1, DAY).await.unwrap().is_some(), "the daily cap holds until the day ends");
    }

    #[tokio::test]
    async fn spend_survives_a_restart_and_the_daily_allowance_comes_back_the_next_day() {
        let caps = Caps { thread_cost: Some(10.0), daily_cost: Some(1.2), ..Caps::default() };
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), caps);
        ledger.record_on(&thread(1), &cost("s1", 0.75), DAY).await.unwrap();

        let ledger = Ledger::with_store(store, ant(), caps);
        let reached = ledger.record_on(&thread(1), &cost("s1", 1.0), DAY).await.unwrap().reached;
        assert_eq!(reached, None, "only the 0.25 added since the stored reading counts");
        let reached = ledger.record_on(&thread(2), &cost("s2", 0.5), DAY).await.unwrap().reached.unwrap();
        assert!(reached.starts_with("Daily cost cap of dev-a reached (1.50 of 1.20"), "{reached}");
        assert_eq!(ledger.reached_on(&thread(2), "2026-11-14").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_warning_comes_once_per_allowance_and_day_and_survives_a_restart() {
        let caps = Caps { thread_tokens: Some(100), daily_cost: Some(2.0), ..Caps::default() };
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), caps).warn_at(80);
        let (t1, t2) = (thread(1), thread(2));
        let recorded = ledger.record_on(&t1, &Spend::Tokens(79), DAY).await.unwrap();
        assert_eq!(recorded, Recorded::default(), "short of the warning share");
        let recorded = ledger.record_on(&t1, &Spend::Tokens(6), DAY).await.unwrap();
        let warning = Warning { key: "max_thread_tokens", figure: "85/100 tokens".to_owned(), percent: 85 };
        assert_eq!(recorded, Recorded { reached: None, warnings: vec![warning.clone()] });
        assert!(!warning.daily());
        let text = warning.text(&ant(), &t1);
        assert!(text.starts_with("[ants] dev-a spent 85/100 tokens (85%) of max_thread_tokens on thread t1"), "{text}");
        assert!(text.contains("set max_thread_tokens of dev-a") && text.contains("`ants spend dev-a --thread t1`"));

        let ledger = Ledger::with_store(store.clone(), ant(), caps).warn_at(80);
        let recorded = ledger.record_on(&t1, &Spend::Tokens(5), DAY).await.unwrap();
        assert!(recorded.warnings.is_empty(), "warned already, also after a restart");
        ledger.fresh_allowance(&t1).await.unwrap();
        let recorded = ledger.record_on(&t1, &Spend::Tokens(90), DAY).await.unwrap();
        assert_eq!(recorded.warnings.len(), 1, "a fresh allowance warns again");

        let recorded = ledger.record_on(&t1, &cost("s1", 1.7), DAY).await.unwrap();
        let warning = Warning { key: "max_daily_cost", figure: "1.70/2.00".to_owned(), percent: 85 };
        assert_eq!(recorded.warnings, std::slice::from_ref(&warning));
        assert!(warning.daily() && warning.text(&ant(), &t1).contains("today (UTC), last on thread t1"));
        assert!(ledger.record_on(&t2, &cost("s2", 0.1), DAY).await.unwrap().warnings.is_empty(), "once a day");
        let day: Spent = store.get(daily_spend_key(&ant(), DAY)).await.unwrap();
        assert_eq!(day.tokens, 180, "`ants spend` still reads the day");
        let next = ledger.record_on(&t2, &cost("s2", 1.8), "2026-11-14").await.unwrap();
        assert_eq!(next.warnings.len(), 1, "a new day warns again");

        // Passing the cap at once: reaching it tells of it, so no warning, and none later either.
        let recorded = ledger.record_on(&t2, &Spend::Tokens(150), DAY).await.unwrap();
        assert!(recorded.reached.is_some() && recorded.warnings.is_empty(), "{recorded:?}");

        let silent = Ledger::with_store(Store::Memory(Arc::default()), ant(), caps).warn_at(0);
        assert!(silent.record_on(&t1, &Spend::Tokens(90), DAY).await.unwrap().warnings.is_empty(), "0: never");
    }

    fn escalated(to: &[&str], deadline: Option<i64>) -> Pending {
        let start = Escalation::new(ant(), thread(1), ants_proto::EscalationReason::TurnBudget);
        let chain: Vec<UserId> = ["lead", "dev-b"].iter().map(|p| p.parse().unwrap()).collect();
        let ants_proto::Hop::Chain { escalation, .. } = start.up(&chain, 3) else { unreachable!() };
        let to = to.iter().map(|p| p.parse().unwrap()).collect();
        let summary = "stuck".to_owned();
        Pending {
            escalation,
            to,
            renewed_by: None,
            deadline,
            owner: None,
            creator: None,
            origin_thread: None,
            summary,
            channel: None,
        }
    }

    #[tokio::test]
    async fn spend_stored_before_escalations_still_loads() {
        let store = Store::Memory(Arc::default());
        let old = serde_json::json!({"session": "s1", "reading": 0.5, "tokens": 0, "cost": 0.5});
        store.put(thread_spend_key(&ant(), &thread(1)), &old).await.unwrap();
        let ledger = Ledger::with_store(store, ant(), Caps { thread_cost: Some(0.5), ..Caps::default() });
        assert_eq!(ledger.pending(&thread(1)).await.unwrap(), None);
        assert!(ledger.reached_on(&thread(1), DAY).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn the_leader_renews_once_per_level_and_a_human_ends_the_escalation() {
        let caps = Caps { thread_tokens: Some(100), ..Caps::default() };
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), caps);
        let (t, lead, dev_b) = (thread(1), "lead".parse().unwrap(), "dev-b".parse().unwrap());
        ledger.record_on(&t, &Spend::Tokens(120), DAY).await.unwrap();
        ledger.set_pending(&t, Some(escalated(&["lead", "dev-b"], Some(5)))).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 5)]));

        let (answer, again) = (Uuid::new_v4(), Uuid::new_v4());
        assert!(!ledger.renew(&t, &dev_b, answer).await.unwrap(), "only the leader renews");
        assert!(ledger.renew(&t, &lead, answer).await.unwrap());
        assert_eq!(ledger.reached_on(&t, DAY).await.unwrap(), None, "a fresh allowance");
        assert!(ledger.deadlines().await.unwrap().is_empty(), "the deadline is over");
        assert!(ledger.renew(&t, &lead, answer).await.unwrap(), "a redelivery of the answer still renews");
        assert!(!ledger.renew(&t, &lead, again).await.unwrap(), "once per level");

        // A restart keeps the level and that its renewal was used.
        let ledger = Ledger::with_store(store, ant(), caps);
        let pending = ledger.pending(&t).await.unwrap().unwrap();
        assert_eq!((pending.escalation.level, pending.renewed_by), (1, Some(answer)));
        assert!(ledger.reset(&t).await.unwrap(), "a human ends it");
        assert_eq!(ledger.pending(&t).await.unwrap(), None);
        assert!(!ledger.reset(&t).await.unwrap());

        ledger.set_pending(&t, Some(escalated(&[], None))).await.unwrap();
        assert!(!ledger.renew(&t, &lead, again).await.unwrap(), "once with the owner, only a human answers");
    }

    #[tokio::test]
    async fn a_stale_deadline_is_dropped_on_a_recheck() {
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), Caps::default());
        let (t1, t2) = (thread(1), thread(2));
        ledger.set_pending(&t1, Some(escalated(&["lead"], Some(7)))).await.unwrap();
        ledger.set_pending(&t2, Some(escalated(&["lead"], Some(9)))).await.unwrap();
        // As a crash between listing the deadline and storing the escalation leaves it.
        store.put(thread_spend_key(&ant(), &t2), &ThreadSpend::default()).await.unwrap();

        let ledger = Ledger::with_store(store, ant(), Caps::default());
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t1.clone(), 7), (t2.clone(), 9)]));
        ledger.recheck(&t2).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t1, 7)]));
    }

    #[tokio::test]
    async fn a_reminder_is_due_before_a_later_deadline_keeps_its_start_and_ends_with_the_escalation() {
        let ledger = Ledger::memory(ant(), Caps::default());
        let t = thread(1);
        let pending = escalated(&["lead"], Some(9));
        let reminder = |since, at| Reminder {
            escalation: pending.escalation.clone(),
            to: UserId::human(),
            owner: None,
            creator: None,
            origin_thread: None,
            summary: String::new(),
            since,
            at,
            sent: 0,
        };
        ledger.set_pending(&t, Some(pending.clone())).await.unwrap();
        ledger.arm_reminder(&t, reminder(1, 5)).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 5)]), "the earlier one");
        ledger.reminded(&t, Some(20)).await.unwrap();
        assert_eq!(ledger.deadlines().await.unwrap(), BTreeMap::from([(t.clone(), 9)]));

        // Reaching a human again postpones the reminder, still counting from the first time.
        ledger.arm_reminder(&t, reminder(15, 30)).await.unwrap();
        let armed = ledger.reminder(&t).await.unwrap().unwrap();
        assert_eq!((armed.since, armed.at, armed.sent), (1, 30, 1));

        assert!(ledger.reset(&t).await.unwrap(), "a human ends it");
        assert_eq!(ledger.reminder(&t).await.unwrap(), None);
        assert!(ledger.deadlines().await.unwrap().is_empty());
        ledger.reminded(&t, Some(40)).await.unwrap();
        assert_eq!(ledger.reminder(&t).await.unwrap(), None, "a reminder sent meanwhile does not bring it back");
    }
}
