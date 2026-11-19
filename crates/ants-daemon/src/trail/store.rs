//! A run's records in [`TRAIL_RUNS_BUCKET`], under the conductor's own keys: its header ([`TrailRun`]), its session
//! ([`TrailSession`]), one journal entry per answered call, the index of the conductor's runs not finished
//! ([`TrailRunIndex`]), which the daemon reads at start, as an ant cannot list the bucket's keys, and what it keeps of
//! each step's thread ([`TrailStepThread`]), under the step's thread and the run's.

use ants_proto::{
    JsDomain, RunId, TRAIL_RUNS_BUCKET, ThreadId, TrailRun, TrailRunIndex, TrailSession, TrailStepThread, UserId,
    trail_call_key, trail_run_key, trail_runs_index_key, trail_session_key, trail_step_key, trail_thread_step_key,
};
use ants_trail::JournalEntry;
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::records::direct_kv::DirectKv;

pub(super) struct Store {
    kv: DirectKv,
    ant: UserId,
    /// Held while the index is read and written back.
    index: tokio::sync::Mutex<()>,
}

impl Store {
    pub(super) async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>, ant: UserId) -> Result<Self> {
        let kv = DirectKv::new(js, js_domain, TRAIL_RUNS_BUCKET).await?;
        Ok(Self { kv, ant, index: tokio::sync::Mutex::new(()) })
    }

    async fn get<T: DeserializeOwned>(&self, key: String) -> Result<Option<T>> {
        let Some(value) = self.kv.get(&key).await.with_context(|| format!("reading {key}"))? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&value).with_context(|| format!("parsing {key}"))?))
    }

    async fn put(&self, key: String, value: &impl Serialize) -> Result<()> {
        self.put_bytes(key, serde_json::to_vec(value)?).await
    }

    pub(super) async fn put_bytes(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.kv.put(&key, value).await.with_context(|| format!("storing {key}"))
    }

    pub(super) async fn header(&self, run: &RunId) -> Result<Option<TrailRun>> {
        let header: Option<TrailRun> = self.get(trail_run_key(&self.ant, run)).await?;
        Ok(header.filter(|header| header.conductor == self.ant && header.run == *run))
    }

    pub(super) async fn put_header(&self, header: &TrailRun) -> Result<()> {
        self.put(header.key(), header).await
    }

    pub(super) async fn session(&self, run: &RunId) -> Result<Option<TrailSession>> {
        self.get(trail_session_key(&self.ant, run)).await
    }

    pub(super) fn session_key(&self, run: &RunId) -> String {
        trail_session_key(&self.ant, run)
    }

    pub(super) async fn entry(&self, run: &RunId, call: u32) -> Result<Option<JournalEntry>> {
        let entry: Option<JournalEntry> = self.get(trail_call_key(&self.ant, run, call)).await?;
        Ok(entry.filter(|entry| entry.call == call))
    }

    pub(super) async fn put_entry(&self, run: &RunId, entry: &JournalEntry) -> Result<()> {
        self.put(trail_call_key(&self.ant, run, entry.call), entry).await
    }

    pub(super) async fn delete_entry(&self, run: &RunId, call: u32) -> Result<()> {
        self.kv.delete(trail_call_key(&self.ant, run, call)).await
    }

    /// What the conductor keeps of the step thread `thread`.
    pub(super) async fn step(&self, thread: &ThreadId) -> Result<Option<TrailStepThread>> {
        self.get(trail_step_key(&self.ant, thread)).await
    }

    /// Keep `step`, the step thread `thread`, under its own key and under its run's thread, where a human reads the
    /// steps of that thread alone.
    pub(super) async fn put_step(&self, thread: &ThreadId, step: &TrailStepThread) -> Result<()> {
        self.put(trail_step_key(&self.ant, thread), step).await?;
        self.put(trail_thread_step_key(&self.ant, &step.run_thread, thread), step).await
    }

    /// The runs of the index: those started or resumed and not finished.
    pub(super) async fn runs(&self) -> Result<Vec<RunId>> {
        let index: Option<TrailRunIndex> = self.get(trail_runs_index_key(&self.ant)).await?;
        Ok(index.unwrap_or_default().runs)
    }

    /// Add `run` to the index, or remove it.
    pub(super) async fn list(&self, run: &RunId, listed: bool) -> Result<()> {
        let _held = self.index.lock().await;
        let mut runs = self.runs().await?;
        if runs.contains(run) == listed {
            return Ok(());
        }
        if listed {
            runs.push(run.clone());
        } else {
            runs.retain(|r| r != run);
        }
        self.put(trail_runs_index_key(&self.ant), &TrailRunIndex { runs }).await
    }
}
