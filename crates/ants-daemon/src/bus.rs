//! NATS connection and the JetStream objects ants relies on.

use std::time::Duration;

use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, AckKind};

mod buckets;
mod connect;
mod live;
mod publish;
mod pull;
pub mod relayed;

pub use buckets::{
    chan_consumer, ensure_attempts, ensure_chains, ensure_channels, ensure_colony, ensure_links, ensure_owners,
    ensure_sessions, ensure_stream, forget_retired, inbox_consumer, init_own_buckets, open_stream,
};
pub use connect::{
    authenticate, check_url, connect, connect_retrying, connect_retrying_with, connect_watched, connect_with,
    jetstream, reach_jetstream, unanswered, worth_retrying,
};
pub use live::{CATCH_UP, LiveInbox, caught_up_inbox, live_inbox};
pub use publish::{
    Stored, explain, explain_full, explained, publish, publish_json, store, store_acked, store_in_hub, uncaptured,
};
pub(crate) use publish::{out_of_reach, warn_uncaptured};
pub use pull::{IDLE_HEARTBEAT, Pull, pull, pull_anew};

/// How long the stream keeps messages, the sessions bucket a thread's session, and the attempts bucket a marker: a
/// message can be redelivered for as long as the stream keeps it.
pub const RETENTION: Duration = Duration::from_hours(30 * 24);

/// How long the `ANTS` stream drops a message repeating the `Nats-Msg-Id` of one it stored. A copy kept in a leaf's
/// outbox lands up to about two and a half minutes after the uplink is back, so it may follow its twin stored
/// directly by the length of an outage plus that: a day covers outages overnight.
pub const DUPLICATE_WINDOW: Duration = Duration::from_hours(24);

/// Whether `e` or one of its causes is JetStream refusing a write for want of room in its store: "insufficient
/// resources", err_code 10023.
pub fn is_store_full(e: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(e), |e| e.source())
        .any(|e| e.downcast_ref::<jetstream::Error>().is_some_and(|e| e.error_code() == jetstream::ErrorCode(10023)))
}

/// Ack `msg` and wait until the server confirms it. [`jetstream::Message::double_ack`] takes the "no responders"
/// status the client gets while a leaf node's uplink is down for a confirmation; this does not.
pub async fn double_ack(client: &async_nats::Client, msg: &jetstream::Message) -> Result<()> {
    let reply = msg.reply.clone().context("acking a message that is not from JetStream")?;
    relayed::request(client, reply, jetstream::AckKind::Ack.into()).await.context("acking")?;
    Ok(())
}

/// Tell the server `msg` is still being worked on, so it does not redeliver it yet; a failure only logs.
pub async fn progress(msg: &jetstream::Message) {
    if let Err(e) = msg.ack_with(AckKind::Progress).await {
        tracing::warn!(error = %e, "progress ack");
    }
}

/// Wait for `work`, telling the server every `every` meanwhile that `msg` is still being worked on.
pub async fn in_progress<T>(msg: &jetstream::Message, every: Duration, work: impl Future<Output = T>) -> T {
    tokio::pin!(work);
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    loop {
        tokio::select! {
            done = &mut work => return done,
            _ = ticks.tick() => progress(msg).await,
        }
    }
}

/// Have the server deliver `msg` again, after `delay` if any, else at once; a failure only logs, the message then
/// coming back after its ack wait.
pub async fn nak(msg: &jetstream::Message, delay: Option<Duration>) {
    if let Err(e) = msg.ack_with(AckKind::Nak(delay)).await {
        tracing::warn!(error = %e, "nak");
    }
}

/// Have the server never deliver `msg` again; a failure only logs.
pub async fn term(msg: &jetstream::Message) {
    if let Err(e) = msg.ack_with(AckKind::Term).await {
        tracing::warn!(error = %e, "terminating the message");
    }
}

#[cfg(test)]
mod tests;
