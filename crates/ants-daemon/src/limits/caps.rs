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
//! allowance comes back at 00:00 UTC. Spend is kept whether or not the ant has caps, for `ants spend` to report. The
//! thread's key also keeps what it spent in all, which no fresh allowance starts over: what a sage's review thread
//! cost ([`Ledger::thread_total`]).
//!
//! Spend first passing a share of a cap short of it gives a [`Warning`], once per thread allowance or UTC day: that it
//! was given is kept with the spend, so restarts and later prompts do not give it again.
//!
//! The ants added from one ant template ([`crate::profile::template`]) may share a pool: daily caps on what they spend together
//! ([`Pool`]), on top of each one's own caps. The pool's spend is the sum of its members' daily spend, read from the
//! same keys, so it holds across the daemons serving them; its warning, given by one member, is noted in that
//! member's daily spend, and no member gives it again that day.
//!
//! The thread's escalation ([`Pending`]) is kept with its spend, so its level survives restarts too, as is the
//! reminder of one that reached a human ([`Reminder`]), and the threads whose escalation has a deadline or a reminder
//! due, with the earlier of the two, under `<ant>.escalations.deadlines`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::{ThreadId, UserId, Uuid, daily_spend_key, escalation_deadlines_key, thread_spend_key};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

use crate::harness::{Harness, Reported, Spend};
use crate::records::direct_kv::DirectKv;
use crate::sync::{MutexExt as _, RwLockExt as _};

mod escalations;
mod warning;

pub use escalations::{Ended, Pending, Reminder, Sent};
pub use warning::Warning;

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
    /// The caps set, by config key, with what each counts.
    pub fn counted(&self) -> Vec<(&'static str, Reported)> {
        let tokens = [("max_thread_tokens", self.thread_tokens), ("max_daily_tokens", self.daily_tokens)];
        let cost = [("max_thread_cost", self.thread_cost), ("max_daily_cost", self.daily_cost)];
        let tokens = tokens.into_iter().filter_map(|(key, cap)| cap.map(|_| (key, Reported::Tokens)));
        tokens.chain(cost.into_iter().filter_map(|(key, cap)| cap.map(|_| (key, Reported::Cost)))).collect()
    }

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
    /// What the thread spent in all, which a fresh allowance leaves alone, unlike `tokens` and `cost`.
    total: Spent,
    /// The thread's escalation, from when it first went anywhere until a human's message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    escalation: Option<Pending>,
    /// The reminder of the thread's escalation that reached a human, until a human's message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reminder: Option<Reminder>,
    /// The human the thread's escalation reached when no reminder was armed for it, until a human's message: whom to
    /// tell should another human answer it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    reached: Option<UserId>,
    #[serde(default, skip_serializing_if = "Warned::none")]
    warned: Warned,
    /// The tool rules the thread is narrowed to ([`ants_proto::Envelope::allow`]), until a human's message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    narrowed: Option<Vec<String>>,
    /// The trail step whose turn asked a human, until the answer after the human's goes to the step's conductor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    asked: Option<AskedStep>,
}

/// A trail step on a thread whose turn asked a human with ask_human, so that it sent the step no reply: the ant's
/// answer once the human answered goes to the step's conductor as the step's reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AskedStep {
    /// The ant conducting the step's run.
    pub conductor: UserId,
    /// The step's message, which the answer replies to.
    pub step: Uuid,
    /// The turns the step's thread had left as the step came.
    pub turn_budget: u32,
}

/// What the ant spent in a UTC day, stored as [`Spent`] is, with the warnings given.
#[derive(Debug, Default, Serialize, Deserialize)]
struct DaySpend {
    tokens: u64,
    cost: f64,
    #[serde(default, skip_serializing_if = "Warned::none")]
    warned: Warned,
    /// The pool caps whose warning was given, by this ant or another member.
    #[serde(default, skip_serializing_if = "Warned::none")]
    pool_warned: Warned,
}

/// Why caps of an ant, its own `caps` and those of its template's `pool`, never trip on `harnesses`, its own and its
/// fallbacks': one line for each harness not reporting what one of them counts ([`Harness::uncounted`]), with the
/// harness.
pub fn uncounted<'a>(
    caps: &Caps,
    pool: Option<&Pool>,
    harnesses: impl IntoIterator<Item = &'a Harness>,
) -> Vec<(String, &'a Harness)> {
    let mut counted = caps.counted();
    if let Some(pool) = pool {
        counted.extend(pool.daily_tokens.map(|_| ("pool_daily_tokens", Reported::Tokens)));
        counted.extend(pool.daily_cost.map(|_| ("pool_daily_cost", Reported::Cost)));
    }
    let mut seen = std::collections::BTreeSet::new();
    harnesses
        .into_iter()
        .filter(|harness| seen.insert(harness.name().to_owned()))
        .filter_map(|harness| harness.uncounted(&counted).map(|why| (why, harness)))
        .collect()
}

/// Daily caps on what the ants added from one template spend together.
#[derive(Debug, Clone, PartialEq)]
pub struct Pool {
    /// The template's name.
    pub template: String,
    /// Tokens its ants may spend together in a UTC day.
    pub daily_tokens: Option<u64>,
    /// Cost its ants may spend together in a UTC day.
    pub daily_cost: Option<f64>,
    /// Its ants.
    pub members: Members,
}

/// The ants of a [`Pool`]; a clone shares the list, so a running ant sees it change ([`Members::replace`]). Equal to
/// any other, so that a change of members alone does not restart the ants it concerns.
#[derive(Debug, Clone, Default)]
pub struct Members(Arc<std::sync::RwLock<Vec<UserId>>>);

impl Members {
    pub fn new(members: Vec<UserId>) -> Self {
        Self(Arc::new(std::sync::RwLock::new(members)))
    }

    pub fn get(&self) -> Vec<UserId> {
        self.0.read_locked().clone()
    }

    /// Take the list of `other`, for every clone of this one.
    pub fn replace(&self, other: &Self) {
        let new = other.get();
        *self.0.write_locked() = new;
    }
}

impl PartialEq for Members {
    fn eq(&self, _: &Self) -> bool {
        true
    }
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

/// A limit or cap a prompt reached, and whether 00:00 UTC renews it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limit {
    /// What was reached, as the `[ants]` notice tells it.
    pub why: String,
    /// A daily or pool cap, renewed at 00:00 UTC: not a thread cap, which only a human's or the leader's message
    /// renews, nor a limit of the prompt's own, such as its timeout.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub daily: bool,
}

impl Limit {
    /// A limit no day renews: the prompt's own, such as its timeout or tool calls, or a thread cap.
    pub fn of_prompt(why: String) -> Self {
        Self { why, daily: false }
    }

    fn daily(why: String) -> Self {
        Self { why, daily: true }
    }
}

impl std::fmt::Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.why)
    }
}

/// What counting a harness's report found: the cap reached, if any, the warnings due, and the cost in US dollars the
/// report added.
#[derive(Debug, Default, PartialEq)]
pub struct Recorded {
    pub reached: Option<Limit>,
    pub warnings: Vec<Warning>,
    pub cost: f64,
}

/// Tokens and cost spent: by the ant in a UTC day, or by a thread, since a human's last message on it or in all.
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
    let key = daily_spend_key(ant, day);
    parse(&key, kv.get(&key).await?)
}

/// What `ant` spent on `thread` since a human's last message on it, read from [`ants_proto::SESSIONS_BUCKET`]; None
/// when nothing is stored.
pub async fn thread_spent(kv: &DirectKv, ant: &UserId, thread: &ThreadId) -> Result<Option<Spent>> {
    Ok(thread_spent_and_total(kv, ant, thread).await?.map(|(since, _)| since))
}

/// What `ant` spent on `thread` since a human's last message on it, then in all ([`Ledger::thread_total`]), read from
/// [`ants_proto::SESSIONS_BUCKET`]; None when nothing is stored.
pub async fn thread_spent_and_total(kv: &DirectKv, ant: &UserId, thread: &ThreadId) -> Result<Option<(Spent, Spent)>> {
    let key = thread_spend_key(ant, thread);
    let spent: Option<ThreadSpend> = parse(&key, kv.get(&key).await?)?;
    Ok(spent.map(|s| (Spent { tokens: s.tokens, cost: s.cost }, s.total)))
}

/// The value stored under `key`, of which `bytes` were read; None when nothing is stored.
fn parse<T: for<'de> Deserialize<'de>>(key: &str, bytes: Option<Vec<u8>>) -> Result<Option<T>> {
    bytes.map(|bytes| serde_json::from_slice(&bytes).with_context(|| format!("parsing {key}"))).transpose()
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
            Self::Memory(map) => map.locked().get(&key).cloned(),
        };
        Ok(parse(&key, bytes)?.unwrap_or_default())
    }

    async fn put<T: Serialize>(&self, key: String, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        match self {
            Self::Kv(kv) => {
                kv.put(&key, bytes).await?;
            }
            Self::Memory(map) => {
                map.locked().insert(key, bytes);
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
    /// The pool the ant spends from, if any.
    pool: Option<Pool>,
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
        Self { caps, ant, store, warn_percent: 0, pool: None, day, threads }
    }

    /// Give a [`Warning`] once spend passes `percent` of a cap; 0 for never.
    #[must_use]
    pub fn warn_at(self, percent: u8) -> Self {
        Self { warn_percent: percent, ..self }
    }

    /// Hold the ant to the daily caps of `pool` too.
    #[must_use]
    pub fn with_pool(self, pool: Option<Pool>) -> Self {
        Self { pool: pool.filter(|p| p.daily_tokens.is_some() || p.daily_cost.is_some()), ..self }
    }

    /// The thread starts over from nothing spent.
    pub async fn fresh_allowance(&self, thread: &ThreadId) -> Result<()> {
        if !self.caps.any() {
            return Ok(());
        }
        self.update(thread, |spent| (forget_spend(spent), ())).await
    }

    /// A message from a human: the thread starts over from nothing spent, its escalation is over, with its reminder
    /// and the human it reached, and its tool rules are no longer narrowed. Returns what the escalation had.
    pub async fn reset(&self, thread: &ThreadId) -> Result<Ended> {
        self.update(thread, |spent| {
            let ended = Ended {
                escalation: spent.escalation.take(),
                reminder: spent.reminder.take(),
                reached: spent.reached.take(),
            };
            let narrowed = spent.narrowed.take().is_some();
            (forget_spend(spent) || ended.any() || narrowed, ended)
        })
        .await
    }

    /// The tool rules the thread is narrowed to: those it was narrowed to first, until a human's message
    /// ([`Ledger::reset`]), else `carried`, which a message on it carries and which it is narrowed to from then on.
    pub async fn narrowed(&self, thread: &ThreadId, carried: Option<Vec<String>>) -> Result<Option<Vec<String>>> {
        self.update(thread, |spent| match (&spent.narrowed, carried) {
            (Some(rules), _) => (false, Some(rules.clone())),
            (None, Some(rules)) => {
                spent.narrowed = Some(rules.clone());
                (true, Some(rules))
            }
            (None, None) => (false, None),
        })
        .await
    }

    /// Keep `step`, the trail step on the thread whose turn asked a human, until [`Ledger::step_answered`]: a human's
    /// message leaves it ([`Ledger::reset`]).
    pub async fn step_asked(&self, thread: &ThreadId, step: AskedStep) -> Result<()> {
        self.update(thread, |spent| {
            let changed = spent.asked.as_ref() != Some(&step);
            spent.asked = Some(step);
            (changed, ())
        })
        .await
    }

    /// The trail step on the thread whose turn asked a human, if one waits for the answer after the human's.
    pub async fn asked_step(&self, thread: &ThreadId) -> Result<Option<AskedStep>> {
        Ok(self.thread(thread).await?.asked)
    }

    /// The trail step `step` on the thread, which a turn asked a human about, is answered.
    pub async fn step_answered(&self, thread: &ThreadId, step: Uuid) -> Result<()> {
        self.update(thread, |spent| {
            let answered = spent.asked.as_ref().is_some_and(|asked| asked.step == step);
            if answered {
                spent.asked = None;
            }
            (answered, ())
        })
        .await
    }

    /// What the ant spent on the thread in all, every prompt and across restarts, whatever humans' messages and
    /// leaders' answers on it gave it fresh allowances.
    pub async fn thread_total(&self, thread: &ThreadId) -> Result<Spent> {
        Ok(self.thread(thread).await?.total)
    }

    /// What is stored of the thread: its spend, its escalation and its reminder.
    async fn thread(&self, thread: &ThreadId) -> Result<ThreadSpend> {
        self.store.get(thread_spend_key(&self.ant, thread)).await
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
    pub async fn reached(&self, thread: &ThreadId) -> Result<Option<Limit>> {
        self.reached_on(thread, &today()).await
    }

    /// Count what a harness reported for the thread. Returns the cap reached, if any, and the warnings due.
    pub async fn record(&self, thread: &ThreadId, spend: &Spend) -> Result<Recorded> {
        self.record_on(thread, spend, &today()).await
    }

    async fn reached_on(&self, thread: &ThreadId, today: &str) -> Result<Option<Limit>> {
        if !self.caps.any() && self.pool.is_none() {
            return Ok(None);
        }
        let spent = self.thread(thread).await?;
        let mut day = self.day.lock().await;
        let daily = self.load_day(&mut day, today).await?;
        let pool = self.pool_spent(daily, today).await;
        let all = self.day_check(daily).or_else(|| pool.and_then(|(pool, spent, _)| check_pool(pool, spent)));
        crate::monitoring::metrics::ant(&self.ant).capped(today, all.is_some());
        Ok(self.thread_check(&spent).or(all))
    }

    /// [`Ledger::record`] on the UTC day `today` (`YYYY-MM-DD`). Counted whether or not the ant has caps, so
    /// `ants spend` reports it.
    pub async fn record_on(&self, thread: &ThreadId, spend: &Spend, today: &str) -> Result<Recorded> {
        let caps = self.caps;
        let (spent, tokens, cost, mut warnings) = self
            .update(thread, |spent| {
                let (tokens, cost) = match spend {
                    Spend::Tokens(n) => (*n, 0.0),
                    Spend::Cost { session, total } => (0, cost_increase(spent, session, *total)),
                };
                spent.tokens += tokens;
                spent.cost += cost;
                spent.total += Spent { tokens, cost };
                let total = Spent { tokens: spent.tokens, cost: spent.cost };
                let (tokens_cap, cost_cap) =
                    (("max_thread_tokens", caps.thread_tokens), ("max_thread_cost", caps.thread_cost));
                let warnings = self.warnings(total, tokens_cap, cost_cap, &mut spent.warned);
                let total = ThreadSpend { tokens: total.tokens, cost: total.cost, ..ThreadSpend::default() };
                (true, (total, tokens, cost, warnings))
            })
            .await?;
        crate::monitoring::metrics::ant(&self.ant).spent(tokens, cost);

        let mut day = self.day.lock().await;
        let daily = self.load_day(&mut day, today).await?;
        let pool = if tokens > 0 || cost > 0.0 {
            daily.tokens += tokens;
            daily.cost += cost;
            let total = Spent { tokens: daily.tokens, cost: daily.cost };
            let (tokens_cap, cost_cap) = (("max_daily_tokens", caps.daily_tokens), ("max_daily_cost", caps.daily_cost));
            warnings.extend(self.warnings(total, tokens_cap, cost_cap, &mut daily.warned));
            let pool = self.pool_spent(daily, today).await;
            if let Some((pool, total, mut warned)) = pool {
                let (tokens_cap, cost_cap) =
                    (("pool_daily_tokens", pool.daily_tokens), ("pool_daily_cost", pool.daily_cost));
                let pool_warnings = self.warnings(total, tokens_cap, cost_cap, &mut warned);
                daily.pool_warned = warned;
                warnings.extend(pool_warnings.into_iter().map(|w| Warning { pool: Some(pool.template.clone()), ..w }));
            }
            self.store.put(daily_spend_key(&self.ant, today), daily).await?;
            pool
        } else {
            self.pool_spent(daily, today).await
        };
        let all = self.day_check(daily).or_else(|| pool.and_then(|(pool, spent, _)| check_pool(pool, spent)));
        crate::monitoring::metrics::ant(&self.ant).capped(today, all.is_some());
        Ok(Recorded { reached: self.thread_check(&spent).or(all), warnings, cost })
    }

    /// What the ant's pool spent on `today`, `own` being the ant's share, and the pool's warnings given by any member.
    /// None without a pool, or when a member's spend cannot be read: the pool then holds no ant back.
    async fn pool_spent(&self, own: &DaySpend, today: &str) -> Option<(&Pool, Spent, Warned)> {
        let pool = self.pool.as_ref()?;
        let mut total = Spent { tokens: own.tokens, cost: own.cost };
        let mut warned = own.pool_warned;
        for member in pool.members.get().iter().filter(|m| **m != self.ant) {
            let spent: DaySpend = match self.store.get(daily_spend_key(member, today)).await {
                Ok(spent) => spent,
                Err(e) => {
                    tracing::warn!(
                        ant = %self.ant, %member, template = pool.template,
                        "reading the daily spend of a member of the pool: {e:#}; not holding the ant to the pool caps"
                    );
                    return None;
                }
            };
            total += Spent { tokens: spent.tokens, cost: spent.cost };
            warned.tokens |= spent.pool_warned.tokens;
            warned.cost |= spent.pool_warned.cost;
        }
        Some((pool, total, warned))
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
                warnings.push(Warning { key: tokens_key, pool: None, figure, percent });
            }
        }
        if let Some(cap) = cost_cap
            && !warned.cost
            && spent.cost * 100.0 >= cap * percent as f64
        {
            warned.cost = true;
            if spent.cost < cap {
                let figure = format!("{:.2}/{cap:.2}", spent.cost);
                let percent = (spent.cost / cap * 100.0) as u64;
                warnings.push(Warning { key: cost_key, pool: None, figure, percent });
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

    /// The thread cap `thread` reached, if any.
    fn thread_check(&self, thread: &ThreadSpend) -> Option<Limit> {
        let caps = &self.caps;
        let since = "since a human's last message";
        if let Some(cap) = caps.thread_tokens.filter(|cap| thread.tokens >= *cap) {
            return Some(Limit::of_prompt(format!(
                "Thread token cap reached ({} of {cap} tokens {since})",
                thread.tokens
            )));
        }
        if let Some(cap) = caps.thread_cost.filter(|cap| thread.cost >= *cap) {
            return Some(Limit::of_prompt(format!("Thread cost cap reached ({:.2} of {cap:.2} {since})", thread.cost)));
        }
        None
    }

    /// The daily cap of the ant `day` reached, if any, holding all its threads.
    fn day_check(&self, day: &DaySpend) -> Option<Limit> {
        let caps = &self.caps;
        let resets = "today; resets at 00:00 UTC";
        let ant = &self.ant;
        if let Some(cap) = caps.daily_tokens.filter(|cap| day.tokens >= *cap) {
            return Some(Limit::daily(format!(
                "Daily token cap of {ant} reached ({} of {cap} tokens {resets})",
                day.tokens
            )));
        }
        if let Some(cap) = caps.daily_cost.filter(|cap| day.cost >= *cap) {
            return Some(Limit::daily(format!(
                "Daily cost cap of {ant} reached ({:.2} of {cap:.2} {resets})",
                day.cost
            )));
        }
        None
    }
}

/// The pool cap `spent` reached, if any.
fn check_pool(pool: &Pool, spent: Spent) -> Option<Limit> {
    let template = &pool.template;
    let by = "spent by its ants today; resets at 00:00 UTC";
    if let Some(cap) = pool.daily_tokens.filter(|cap| spent.tokens >= *cap) {
        return Some(Limit::daily(format!(
            "Pool daily token cap of template {template} reached ({} of {cap} tokens {by})",
            spent.tokens
        )));
    }
    if let Some(cap) = pool.daily_cost.filter(|cap| spent.cost >= *cap) {
        return Some(Limit::daily(format!(
            "Pool daily cost cap of template {template} reached ({:.2} of {cap:.2} {by})",
            spent.cost
        )));
    }
    None
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

pub(crate) fn today() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) const DAY: &str = "2026-11-13";

    pub(super) fn ant() -> UserId {
        "dev-a".parse().unwrap()
    }

    pub(super) fn thread(n: u8) -> ThreadId {
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
        assert!(reached.why.starts_with("Thread cost cap reached (1.05 of 1.00") && !reached.daily, "{reached:?}");
        assert_eq!(ledger.reached_on(&thread(2), DAY).await.unwrap(), None, "other threads have their own");
    }

    #[test]
    fn caps_a_harness_does_not_report_are_named_once_per_harness() {
        let harnesses = crate::harness::Harnesses::builtin();
        let ran: Vec<_> = ["claude", "copilot", "codex", "copilot"].map(|name| harnesses.get(name).unwrap()).into();
        let caps = Caps { thread_tokens: Some(10), daily_cost: Some(1.0), ..Caps::default() };
        let pool =
            Pool { template: "t".to_owned(), daily_tokens: None, daily_cost: Some(5.0), members: Members::default() };
        let found: Vec<_> = uncounted(&caps, Some(&pool), ran.iter().map(|h| &**h))
            .into_iter()
            .map(|(why, harness)| (why, harness.name().to_owned()))
            .collect();
        assert_eq!(
            found,
            [
                (
                    "harness copilot reports neither tokens nor cost, so max_thread_tokens, max_daily_cost and \
                     pool_daily_cost never trip on it"
                        .to_owned(),
                    "copilot".to_owned()
                ),
                (
                    "harness codex reports no cost, so max_daily_cost and pool_daily_cost never trip on it".to_owned(),
                    "codex".to_owned()
                ),
            ]
        );
        assert!(uncounted(&Caps::default(), None, ran.iter().map(|h| &**h)).is_empty(), "no caps");
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
        assert_eq!(ledger.thread_total(&thread(1)).await.unwrap(), Spent { tokens: 70, cost: 0.5 });
        assert_eq!(ledger.thread_total(&thread(2)).await.unwrap(), Spent::default(), "nothing spent on it");
    }

    /// A human's message or a leader's answer gives the thread a fresh allowance, its caps and `ants spend` counting
    /// from nothing again, but leaves what it spent in all.
    #[tokio::test]
    async fn a_fresh_allowance_leaves_the_thread_s_total() {
        let caps = Caps { thread_tokens: Some(100), ..Caps::default() };
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), caps);
        let t = thread(1);
        ledger.record_on(&t, &Spend::Tokens(120), DAY).await.unwrap();
        ledger.record_on(&t, &cost("s1", 0.25), DAY).await.unwrap();
        ledger.reset(&t).await.unwrap();
        assert_eq!(ledger.reached_on(&t, DAY).await.unwrap(), None, "a human's message frees the thread");
        ledger.record_on(&t, &Spend::Tokens(30), DAY).await.unwrap();
        ledger.fresh_allowance(&t).await.unwrap();
        ledger.record_on(&t, &cost("s1", 0.75), DAY).await.unwrap();

        let ledger = Ledger::with_store(store, ant(), caps);
        assert_eq!(ledger.thread_total(&t).await.unwrap(), Spent { tokens: 150, cost: 0.75 }, "after a restart too");
        let kv = ledger.store.get::<ThreadSpend>(thread_spend_key(&ant(), &t)).await.unwrap();
        assert_eq!((kv.tokens, kv.cost), (0, 0.5), "the allowance counts from the last one");
    }

    #[tokio::test]
    async fn a_message_from_the_human_frees_the_thread_but_not_the_ant() {
        let caps = Caps { thread_tokens: Some(100), daily_tokens: Some(250), ..Caps::default() };
        let ledger = Ledger::memory(ant(), caps);
        let (t1, t2) = (thread(1), thread(2));
        let reached = ledger.record_on(&t1, &Spend::Tokens(120), DAY).await.unwrap().reached.unwrap();
        assert!(reached.why.starts_with("Thread token cap reached (120 of 100") && !reached.daily, "{reached:?}");
        ledger.fresh_allowance(&t1).await.unwrap();
        assert_eq!(ledger.reached_on(&t1, DAY).await.unwrap(), None);
        assert_eq!(ledger.record_on(&t2, &Spend::Tokens(90), DAY).await.unwrap().reached, None);
        let reached = ledger.record_on(&t1, &Spend::Tokens(50), DAY).await.unwrap().reached.unwrap();
        assert!(
            reached.why.starts_with("Daily token cap of dev-a reached (260 of 250") && reached.daily,
            "{reached:?}"
        );
        ledger.fresh_allowance(&t1).await.unwrap();
        assert!(ledger.reached_on(&t1, DAY).await.unwrap().is_some(), "the daily cap holds until the day ends");
    }

    #[tokio::test]
    async fn a_thread_stays_narrowed_across_a_restart_until_a_human_messages() {
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), ant(), Caps::default());
        let (read, all) = (Some(vec!["read".to_owned()]), Some(vec!["*".to_owned()]));
        assert_eq!(ledger.narrowed(&thread(1), None).await.unwrap(), None);
        assert_eq!(ledger.narrowed(&thread(1), read.clone()).await.unwrap(), read);

        let ledger = Ledger::with_store(store, ant(), Caps::default());
        assert_eq!(ledger.narrowed(&thread(1), None).await.unwrap(), read, "kept without a message's rules");
        assert_eq!(ledger.narrowed(&thread(1), all.clone()).await.unwrap(), read, "the first rules hold");
        assert_eq!(ledger.narrowed(&thread(2), None).await.unwrap(), None);
        assert!(!ledger.reset(&thread(1)).await.unwrap().any(), "no escalation ended");
        assert_eq!(ledger.narrowed(&thread(1), None).await.unwrap(), None);
        assert_eq!(ledger.narrowed(&thread(1), all.clone()).await.unwrap(), all);
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
        assert!(
            reached.why.starts_with("Daily cost cap of dev-a reached (1.50 of 1.20") && reached.daily,
            "{reached:?}"
        );
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
        let warning = Warning { key: "max_thread_tokens", pool: None, figure: "85/100 tokens".to_owned(), percent: 85 };
        assert_eq!(recorded, Recorded { reached: None, warnings: vec![warning.clone()], cost: 0.0 });
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
        assert!((recorded.cost - 1.7).abs() < 1e-9, "the cost the report added: {}", recorded.cost);
        let warning = Warning { key: "max_daily_cost", pool: None, figure: "1.70/2.00".to_owned(), percent: 85 };
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

    #[tokio::test]
    async fn a_pool_caps_its_members_together_across_ledgers_and_warns_once() {
        let (a, b): (UserId, UserId) = ("lead-a".parse().unwrap(), "lead-b".parse().unwrap());
        let pool = Pool {
            template: "worker".to_owned(),
            daily_tokens: Some(100),
            daily_cost: None,
            members: Members::new(vec![a.clone(), b.clone()]),
        };
        let store = Store::Memory(Arc::default());
        let ledger = |ant: &UserId| {
            Ledger::with_store(store.clone(), ant.clone(), Caps::default()).warn_at(80).with_pool(Some(pool.clone()))
        };
        let (la, lb) = (ledger(&a), ledger(&b));
        assert_eq!(la.record_on(&thread(1), &Spend::Tokens(50), DAY).await.unwrap(), Recorded::default());
        let recorded = lb.record_on(&thread(2), &Spend::Tokens(35), DAY).await.unwrap();
        let warning = Warning {
            key: "pool_daily_tokens",
            pool: Some("worker".to_owned()),
            figure: "85/100 tokens".to_owned(),
            percent: 85,
        };
        assert_eq!(recorded, Recorded { reached: None, warnings: vec![warning.clone()], cost: 0.0 });
        assert!(warning.daily());
        let text = warning.text(&b, &thread(2));
        assert!(text.starts_with("[ants] The ants added from template worker spent 85/100 tokens (85%)"), "{text}");
        assert!(text.contains("set pool_daily_tokens of [[ant_template]] worker"), "{text}");
        assert!(la.record_on(&thread(1), &Spend::Tokens(5), DAY).await.unwrap().warnings.is_empty(), "warned by b");

        let reached = lb.record_on(&thread(2), &Spend::Tokens(10), DAY).await.unwrap().reached.unwrap();
        assert_eq!(
            reached.why,
            "Pool daily token cap of template worker reached (100 of 100 tokens spent by its ants today; resets at 00:00 UTC)"
        );
        assert!(reached.daily, "renewed at 00:00 UTC");
        assert!(la.reached_on(&thread(3), DAY).await.unwrap().is_some(), "every member is held back");
        assert_eq!(la.reached_on(&thread(3), "2026-11-14").await.unwrap(), None, "until the next day");
        let restarted = ledger(&a);
        assert!(restarted.reached_on(&thread(3), DAY).await.unwrap().is_some(), "also after a restart");

        pool.members.replace(&Members::new(vec![a.clone()]));
        assert_eq!(la.reached_on(&thread(3), DAY).await.unwrap(), None, "a member that left no longer counts");
        let alone = Ledger::with_store(store.clone(), a.clone(), Caps::default());
        assert_eq!(alone.reached_on(&thread(3), DAY).await.unwrap(), None, "no pool, no pool cap");
    }
}
