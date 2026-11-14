//! Links to channel threads, passed on from ant to ant. A message spun off a channel thread links to it
//! ([`Envelope::origin_thread`]), and a daemon takes that thread's creator for an ant's message only when the
//! sender's link is verifiable: the sender takes part in the channel thread, or an ant with a verifiable link passed
//! the link on to it. Each daemon records the links its ant passes on in [`LINKS_BUCKET`], under keys only the ant's
//! credentials write ([`link_key`]), and reads back who passed on the link to a thread ([`passed_on`]).

use std::time::Duration;

use ants_proto::{Envelope, LINKS_BUCKET, ThreadId, UserId, link_key, links_to, parse_link_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;

/// How long a read of the links waits for each record the bucket said it has.
const WAIT: Duration = Duration::from_secs(5);
/// Most records read for one channel thread.
const MAX_LINKS: u64 = 1024;

/// Record that the sender of `envelope` passed the link it carries to a channel thread, if any, on to its recipient.
pub async fn pass_on(js: &jetstream::Context, envelope: &Envelope) -> Result<()> {
    let Some(thread) = envelope.origin_thread.as_ref() else { return Ok(()) };
    let kv = js
        .get_key_value(LINKS_BUCKET)
        .await
        .context("opening the links bucket (with credentials, `ants init` as the human creates it)")?;
    let key = link_key(&envelope.from, thread, &envelope.to);
    kv.put(key, envelope.thread_id.to_string().into()).await.context("recording the link passed on")?;
    Ok(())
}

/// Who passed the link to channel thread `thread` on to whom, as the links bucket keeps it: none while there is no
/// bucket.
pub async fn passed_on(js: &jetstream::Context, thread: &ThreadId) -> Result<Vec<(UserId, UserId)>> {
    let kv = match js.get_key_value(LINKS_BUCKET).await {
        Ok(kv) => kv,
        Err(e) => {
            tracing::debug!(error = %e, "opening the links bucket");
            return Ok(Vec::new());
        }
    };
    let consumer = kv
        .stream
        .create_consumer(consumer::push::OrderedConfig {
            deliver_subject: js.client().new_inbox(),
            filter_subject: format!("{}{}", kv.prefix, links_to(thread)),
            headers_only: true,
            deliver_policy: consumer::DeliverPolicy::LastPerSubject,
            replay_policy: consumer::ReplayPolicy::Instant,
            ..Default::default()
        })
        .await
        .with_context(|| format!("reading the links to thread {thread}"))?;
    let pending = consumer.cached_info().num_pending.min(MAX_LINKS);
    let mut messages = consumer.messages().await.with_context(|| format!("reading the links to thread {thread}"))?;
    let mut links = Vec::new();
    for _ in 0..pending {
        let msg = tokio::time::timeout(WAIT, messages.next())
            .await
            .with_context(|| format!("waiting for the links to thread {thread}"))?
            .with_context(|| format!("the links to thread {thread} ended early"))?
            .map_err(|e| anyhow::anyhow!(e))?;
        let removed = msg.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
        if removed {
            continue;
        }
        if let Some(link) = msg.subject.strip_prefix(kv.prefix.as_str()).and_then(|key| parse_link_key(key, thread)) {
            links.push(link);
        }
    }
    Ok(links)
}
