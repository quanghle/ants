//! Whom what would reach a human goes to first: the human's assistant, while one stands for it and can act.
//!
//! Stuck escalations ([`crate::escalation`]), ask_human questions and tool requests ([`crate::approvals`]) that would
//! reach a human go first to the ant its grant designates as its assistant, when that ant's record names the human
//! (the grant honours it) and a daemon serves it now: in the colony, live, not paused, failing, signed out nor held
//! back by a provider limit ([`crate::mcp::why_unserved`]). Otherwise they go to the human directly.

use std::sync::Arc;
use std::time::Duration;

use ants_proto::UserId;
use futures::future::BoxFuture;

use crate::mcp::{Colony, why_unserved};
use crate::people::behalf::Records;
use crate::people::humans::Registry;

/// Reads the human an assistant's record names, as [`Records::assistant_of`] does.
pub type RecordOf = Arc<dyn Fn(UserId) -> BoxFuture<'static, Option<UserId>> + Send + Sync>;

/// Finds the assistant that gets first what would reach a human.
#[derive(Clone)]
pub struct AssistantFirst {
    humans: Registry,
    colony: Colony,
    record: RecordOf,
}

impl AssistantFirst {
    pub fn new(humans: Registry, colony: Colony, record: RecordOf) -> Self {
        Self { humans, colony, record }
    }

    /// The assistants' records read through `records`.
    pub fn with_records(humans: Registry, colony: Colony, records: Records) -> Self {
        let record: RecordOf = Arc::new(move |assistant| {
            let records = records.clone();
            Box::pin(async move { records.assistant_of(&assistant).await })
        });
        Self::new(humans, colony, record)
    }

    /// The assistant of `human` that gets first what would reach `human` from `from`, if one stands for it and can act
    /// now; never `from` itself, nor a human.
    pub async fn of(&self, human: &UserId, from: &UserId) -> Option<UserId> {
        if !self.humans.is_human(human) {
            return None;
        }
        let assistant = self.humans.assistant(human)?;
        if assistant == *from || self.humans.is_human(&assistant) {
            return None;
        }
        let record = (self.record)(assistant.clone()).await;
        if !self.humans.honours_assistant(human, &assistant, record.as_ref()) {
            tracing::debug!(%human, %assistant, "the assistant does not act for the human now; going to the human");
            return None;
        }
        self.acts(&assistant).await.then_some(assistant)
    }

    /// Whether a daemon serves `assistant` now; a colony that cannot be read says no.
    async fn acts(&self, assistant: &UserId) -> bool {
        match (self.colony)().await {
            Ok(entries) => {
                let entry = entries.iter().find(|e| e.ant == *assistant);
                match why_unserved(assistant, entry, chrono::Utc::now(), Duration::ZERO) {
                    None => true,
                    Some(why) => {
                        tracing::info!(%assistant, why, "the assistant cannot act now; going to the human");
                        false
                    }
                }
            }
            Err(e) => {
                tracing::warn!(%assistant, error = format!("{e:#}"), "reading the colony; going to the human");
                false
            }
        }
    }
}

/// A colony entry of `ant` that a daemon serves now.
#[cfg(test)]
pub fn live(ant: &UserId) -> ants_proto::ColonyEntry {
    let now = chrono::Utc::now();
    ants_proto::ColonyEntry {
        ant: ant.clone(),
        machine: "m1".to_owned(),
        instance: "run-1".to_owned(),
        harness: "copilot".to_owned(),
        agent: None,
        capabilities: Vec::new(),
        whoami: None,
        workdir: None,
        started_at: now,
        heartbeat_at: now,
        activity: ants_proto::Activity::Idle,
        handling: 0,
        harnesses: 0,
        escalate_to: Vec::new(),
        sage: None,
        version: ants_proto::ANTS_VERSION.to_owned(),
        perms: ants_proto::PERMISSIONS_VERSION,
        signed_out: None,
        limited: None,
        failing: None,
        fallback: None,
        memory_low: None,
        model: None,
        paused: None,
        drift: Vec::new(),
        mcp: false,
    }
}
