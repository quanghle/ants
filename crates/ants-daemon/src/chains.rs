//! Escalation chains kept past the roster. A roster entry expires [`ants_proto::ROSTER_TTL`] after its daemon stops,
//! so each daemon also puts its persona's `escalate_to` in the KV bucket [`CHAINS_BUCKET`] on start, whose entries
//! never expire: an escalation climbs past a leader that is offline through the chain kept there.

use std::sync::Arc;

use ants_proto::{CHAINS_BUCKET, ChainEntry, PersonaId};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, kv};
use futures::future::BoxFuture;

/// Reads the escalation chain of a persona: None when none is kept for it.
pub type Chains = Arc<dyn Fn(PersonaId) -> BoxFuture<'static, Result<Option<Vec<PersonaId>>>> + Send + Sync>;

/// Reads chains from the chains bucket; while the bucket does not exist, none is kept.
pub fn jetstream_chains(js: jetstream::Context) -> Chains {
    Arc::new(move |persona| {
        let js = js.clone();
        Box::pin(async move {
            match js.get_key_value(CHAINS_BUCKET).await {
                Ok(kv) => read(&kv, &persona).await,
                Err(e) => {
                    tracing::debug!(error = %e, "opening the chains bucket");
                    Ok(None)
                }
            }
        })
    })
}

/// The chain kept for `persona` in `kv`. An entry naming another persona than its key is forged, so it counts as none.
pub async fn read(kv: &kv::Store, persona: &PersonaId) -> Result<Option<Vec<PersonaId>>> {
    let key = persona.as_str();
    let Some(value) = kv.get(key).await.with_context(|| format!("reading the chain of {persona}"))? else {
        return Ok(None);
    };
    let entry: ChainEntry =
        serde_json::from_slice(&value).with_context(|| format!("parsing the chain of {persona}"))?;
    if let Err(e) = entry.check_key(key) {
        tracing::warn!(key, error = %e, "skipping a chain entry for another persona");
        return Ok(None);
    }
    Ok(Some(entry.escalate_to))
}

/// Keep `escalate_to` as the chain of `persona` in the chains bucket, unless it is kept already. A persona that never
/// had a chain puts nothing.
pub async fn publish(js: &jetstream::Context, persona: &PersonaId, escalate_to: &[PersonaId]) -> Result<()> {
    let kv = js
        .get_key_value(CHAINS_BUCKET)
        .await
        .context("opening the chains bucket (with credentials, `ants init` as the human creates it)")?;
    match read(&kv, persona).await {
        Ok(Some(chain)) if chain == escalate_to => return Ok(()),
        Ok(None) if escalate_to.is_empty() => return Ok(()),
        Ok(_) => {}
        Err(e) => tracing::warn!(error = format!("{e:#}"), "reading the chain kept; putting it again"),
    }
    let entry = ChainEntry::new(persona.clone(), escalate_to.to_vec());
    kv.put(entry.key(), serde_json::to_vec(&entry)?.into()).await.context("keeping the escalation chain")?;
    Ok(())
}
