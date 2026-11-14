//! Links to threads, passed on from ant to ant. A message spun off a thread links to it ([`Envelope::origin_thread`]),
//! and a daemon takes that thread's creator or owner for an ant's message only when the sender's link is verifiable:
//! the sender takes part in the thread, or an ant with a verifiable link passed the link on to it, spinning a thread
//! off it or messaging the ant on it. Each daemon records the links its ant passes on in [`LINKS_BUCKET`], under keys
//! only the ant's credentials write ([`link_key`]), and reads back who passed on the link to a thread ([`passed_on`]).
//! While the hub is out of reach, the machine's outbox keeps the record until the bucket sources it
//! ([`link_outbox`]).

use std::time::Duration;

use ants_proto::{Envelope, LINKS_BUCKET, ThreadId, UserId, link_key, link_outbox, links_to, parse_link_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;

/// How long a read of the links waits for each record the bucket said it has.
const WAIT: Duration = Duration::from_secs(5);
/// Most records read for one thread.
const MAX_LINKS: u64 = 1024;

/// Record that the sender of `envelope` passed the link it carries to a thread, if any ([`Envelope::passes_on`]), on
/// to its recipient: in the links bucket or, while the hub is out of reach, in the machine's outbox, which the bucket
/// sources once the uplink is back ([`link_outbox`]). Resolves once either holds it, so a message sent after it never
/// outruns its link.
pub async fn pass_on(js: &jetstream::Context, envelope: &Envelope) -> Result<()> {
    let Some(thread) = envelope.passes_on() else { return Ok(()) };
    let value = envelope.thread_id.to_string();
    let in_hub = async {
        let kv = js
            .get_key_value(LINKS_BUCKET)
            .await
            .context("opening the links bucket (with credentials, `ants init` as the human creates it)")?;
        kv.put(link_key(&envelope.from, thread, &envelope.to), value.clone().into())
            .await
            .context("recording the link")?;
        anyhow::Ok(())
    };
    let unreachable = match in_hub.await {
        Ok(()) => return Ok(()),
        Err(e) => e,
    };
    let kept = match js.publish(link_outbox(&envelope.from, thread, &envelope.to), value.into()).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    if let Err(e) = kept {
        return Err(anyhow::anyhow!(e)).with_context(|| {
            format!("the hub did not record the link passed on ({unreachable:#}), nor did this machine's outbox")
        });
    }
    tracing::warn!(
        to = %envelope.to,
        error = format!("{unreachable:#}"),
        "the hub is out of reach; the link passed on is kept in this machine's outbox"
    );
    Ok(())
}

/// [`pass_on`], logging a failure: the message goes out anyway, its recipient's own messages linking to the thread
/// losing its creator and owner, as they would before the links bucket or credentials allowing it existed.
pub async fn passing_on(js: &jetstream::Context, envelope: &Envelope) {
    if let Err(e) = pass_on(js, envelope).await {
        tracing::warn!(
            thread = ?envelope.passes_on(),
            to = %envelope.to,
            error = format!("{e:#}"),
            "recording the link passed on; messages the recipient links to the thread lose its creator and owner"
        );
    }
}

/// Who passed the link to `thread` on to whom, as the links bucket keeps it: none while there is no bucket.
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
