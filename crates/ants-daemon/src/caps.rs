//! Token and cost caps per thread and per persona, counted from the spend harnesses report.
//!
//! Cost comes from ACP `usage_update` notifications, which carry the cumulative cost of a session; the increase over
//! the last reading of the thread's session is what a prompt spent. A reading below the last one, or from another
//! session, means the harness counts from zero again, so all of it is new. Tokens come from the usage in ACP prompt
//! responses, one prompt's worth each. A harness that reports neither, such as Copilot CLI, is never capped.
//!
//! What a thread spent since a human's last message on it is kept in the KV bucket `ants_sessions` under
//! `<persona>.<thread>.spend`, and what the persona spent in a UTC day under `<persona>.day.<YYYY-MM-DD>`, so both
//! survive harness and daemon restarts. A message from any human gives the thread a fresh allowance; the persona's
//! allowance comes back at 00:00 UTC.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::SessionId;
use ants_proto::{PersonaId, ThreadId, daily_spend_key, thread_spend_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream::kv;
use serde::{Deserialize, Serialize};

use crate::harness::Spend;

/// Spend caps; None is no cap.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Caps {
    /// Tokens a thread may spend between messages from a human.
    pub thread_tokens: Option<u64>,
    /// Cost a thread may spend between messages from a human, in the harness's unit (USD for Claude).
    pub thread_cost: Option<f64>,
    /// Tokens the persona may spend in a UTC day.
    pub daily_tokens: Option<u64>,
    /// Cost the persona may spend in a UTC day.
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct DaySpend {
    tokens: u64,
    cost: f64,
}

#[derive(Clone)]
enum Store {
    Kv(Box<kv::Store>),
    Memory(Arc<Mutex<HashMap<String, Vec<u8>>>>),
}

impl Store {
    async fn get<T: for<'de> Deserialize<'de> + Default>(&self, key: String) -> Result<T> {
        let bytes = match self {
            Self::Kv(kv) => kv.get(&key).await.with_context(|| format!("reading {key}"))?.map(|b| b.to_vec()),
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
                kv.put(&key, bytes.into()).await.with_context(|| format!("storing {key}"))?;
            }
            Self::Memory(map) => {
                map.lock().expect("spend store").insert(key, bytes);
            }
        }
        Ok(())
    }
}

/// What the persona and its threads spent, against their caps.
pub struct Ledger {
    caps: Caps,
    persona: PersonaId,
    store: Store,
    /// The persona's spend of the day it is for; held while it is read or stored, so updates do not cross.
    day: tokio::sync::Mutex<Option<(String, DaySpend)>>,
}

impl Ledger {
    /// Spend kept in the KV bucket [`ants_proto::SESSIONS_BUCKET`].
    pub fn kv(kv: kv::Store, persona: PersonaId, caps: Caps) -> Self {
        Self::with_store(Store::Kv(Box::new(kv)), persona, caps)
    }

    /// Spend kept in this process only.
    pub fn memory(persona: PersonaId, caps: Caps) -> Self {
        Self::with_store(Store::Memory(Arc::default()), persona, caps)
    }

    fn with_store(store: Store, persona: PersonaId, caps: Caps) -> Self {
        Self { caps, persona, store, day: tokio::sync::Mutex::default() }
    }

    /// A message from a human: the thread starts over from nothing spent.
    pub async fn fresh_allowance(&self, thread: &ThreadId) -> Result<()> {
        if !self.caps.any() {
            return Ok(());
        }
        let key = thread_spend_key(&self.persona, thread);
        let mut spent: ThreadSpend = self.store.get(key.clone()).await?;
        if spent.tokens > 0 || spent.cost > 0.0 {
            (spent.tokens, spent.cost) = (0, 0.0);
            self.store.put(key, &spent).await?;
        }
        Ok(())
    }

    /// The cap the thread or the persona has reached, if any.
    pub async fn reached(&self, thread: &ThreadId) -> Result<Option<String>> {
        self.reached_on(thread, &today()).await
    }

    /// Count what a harness reported for the thread. Returns the cap reached, if any.
    pub async fn record(&self, thread: &ThreadId, spend: &Spend) -> Result<Option<String>> {
        self.record_on(thread, spend, &today()).await
    }

    async fn reached_on(&self, thread: &ThreadId, today: &str) -> Result<Option<String>> {
        if !self.caps.any() {
            return Ok(None);
        }
        let spent: ThreadSpend = self.store.get(thread_spend_key(&self.persona, thread)).await?;
        let mut day = self.day.lock().await;
        let daily = self.load_day(&mut day, today).await?;
        Ok(self.check(&spent, daily))
    }

    async fn record_on(&self, thread: &ThreadId, spend: &Spend, today: &str) -> Result<Option<String>> {
        if !self.caps.any() {
            return Ok(None);
        }
        let key = thread_spend_key(&self.persona, thread);
        let mut spent: ThreadSpend = self.store.get(key.clone()).await?;
        let (tokens, cost) = match spend {
            Spend::Tokens(n) => (*n, 0.0),
            Spend::Cost { session, total } => (0, cost_increase(&mut spent, session, *total)),
        };
        spent.tokens += tokens;
        spent.cost += cost;
        self.store.put(key, &spent).await?;

        let mut day = self.day.lock().await;
        let daily = self.load_day(&mut day, today).await?;
        if tokens > 0 || cost > 0.0 {
            daily.tokens += tokens;
            daily.cost += cost;
            self.store.put(daily_spend_key(&self.persona, today), daily).await?;
        }
        Ok(self.check(&spent, daily))
    }

    /// The persona's spend on `today`, read from the store when the day held is another one.
    async fn load_day<'a>(&self, day: &'a mut Option<(String, DaySpend)>, today: &str) -> Result<&'a mut DaySpend> {
        if day.as_ref().is_none_or(|(d, _)| d != today) {
            let spent = self.store.get(daily_spend_key(&self.persona, today)).await?;
            *day = Some((today.to_owned(), spent));
        }
        Ok(&mut day.as_mut().expect("loaded").1)
    }

    fn check(&self, thread: &ThreadSpend, day: &DaySpend) -> Option<String> {
        let caps = &self.caps;
        let since = "since a human's last message";
        let resets = "today; resets at 00:00 UTC";
        let persona = &self.persona;
        if let Some(cap) = caps.thread_tokens.filter(|cap| thread.tokens >= *cap) {
            return Some(format!("Thread token cap reached ({} of {cap} tokens {since})", thread.tokens));
        }
        if let Some(cap) = caps.thread_cost.filter(|cap| thread.cost >= *cap) {
            return Some(format!("Thread cost cap reached ({:.2} of {cap:.2} {since})", thread.cost));
        }
        if let Some(cap) = caps.daily_tokens.filter(|cap| day.tokens >= *cap) {
            return Some(format!("Daily token cap of {persona} reached ({} of {cap} tokens {resets})", day.tokens));
        }
        if let Some(cap) = caps.daily_cost.filter(|cap| day.cost >= *cap) {
            return Some(format!("Daily cost cap of {persona} reached ({:.2} of {cap:.2} {resets})", day.cost));
        }
        None
    }
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

    fn persona() -> PersonaId {
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
        let ledger = Ledger::memory(persona(), caps);
        let t = thread(1);
        assert_eq!(ledger.record_on(&t, &cost("s1", 0.25), DAY).await.unwrap(), None);
        assert_eq!(ledger.record_on(&t, &cost("s1", 0.5), DAY).await.unwrap(), None);
        // The harness restarted and counts from zero again: 0.25 + 0.25 + 0.25.
        assert_eq!(ledger.record_on(&t, &cost("s1", 0.25), DAY).await.unwrap(), None);
        let reached = ledger.record_on(&t, &cost("s2", 0.3), DAY).await.unwrap().expect("a new session adds it all");
        assert!(reached.starts_with("Thread cost cap reached (1.05 of 1.00"), "{reached}");
        assert_eq!(ledger.reached_on(&thread(2), DAY).await.unwrap(), None, "other threads have their own");
    }

    #[tokio::test]
    async fn a_message_from_the_human_frees_the_thread_but_not_the_persona() {
        let caps = Caps { thread_tokens: Some(100), daily_tokens: Some(250), ..Caps::default() };
        let ledger = Ledger::memory(persona(), caps);
        let (t1, t2) = (thread(1), thread(2));
        let reached = ledger.record_on(&t1, &Spend::Tokens(120), DAY).await.unwrap().unwrap();
        assert!(reached.starts_with("Thread token cap reached (120 of 100"), "{reached}");
        ledger.fresh_allowance(&t1).await.unwrap();
        assert_eq!(ledger.reached_on(&t1, DAY).await.unwrap(), None);
        assert_eq!(ledger.record_on(&t2, &Spend::Tokens(90), DAY).await.unwrap(), None);
        let reached = ledger.record_on(&t1, &Spend::Tokens(50), DAY).await.unwrap().unwrap();
        assert!(reached.starts_with("Daily token cap of dev-a reached (260 of 250"), "{reached}");
        ledger.fresh_allowance(&t1).await.unwrap();
        assert!(ledger.reached_on(&t1, DAY).await.unwrap().is_some(), "the daily cap holds until the day ends");
    }

    #[tokio::test]
    async fn spend_survives_a_restart_and_the_daily_allowance_comes_back_the_next_day() {
        let caps = Caps { thread_cost: Some(10.0), daily_cost: Some(1.2), ..Caps::default() };
        let store = Store::Memory(Arc::default());
        let ledger = Ledger::with_store(store.clone(), persona(), caps);
        ledger.record_on(&thread(1), &cost("s1", 0.75), DAY).await.unwrap();

        let ledger = Ledger::with_store(store, persona(), caps);
        let reached = ledger.record_on(&thread(1), &cost("s1", 1.0), DAY).await.unwrap();
        assert_eq!(reached, None, "only the 0.25 added since the stored reading counts");
        let reached = ledger.record_on(&thread(2), &cost("s2", 0.5), DAY).await.unwrap().unwrap();
        assert!(reached.starts_with("Daily cost cap of dev-a reached (1.50 of 1.20"), "{reached}");
        assert_eq!(ledger.reached_on(&thread(2), "2026-11-14").await.unwrap(), None);
    }
}
