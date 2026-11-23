//! How a reply to `ask_ant` reaches the call waiting for it. The asks waiting on a reply are kept in
//! `<home>/mcp.json`, with the replies that came after their call gave up, so both survive a restart. A reply reaches
//! the call off the live subscription to the inbox ([`Hub::route`]) or from the inbox consumer ([`Hub::claim`]). One
//! that comes too late waits for `check_inbox` ([`Hub::take_late`]) and is prompted on its thread once its turn comes
//! there, unless `check_inbox` took it first ([`Hub::late_turn`]).

use std::collections::HashMap;
use std::time::Duration;

use ants_proto::{Envelope, ThreadId, UserId, Uuid};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use super::Hub;

/// Asks remembered for their replies; the oldest are forgotten first.
const MAX_ASKS: usize = 1024;
/// Late replies kept for `check_inbox`, and those taken or prompted since; the oldest are dropped first.
const MAX_LATE: usize = 256;
/// Replies taken by a waiting call that the inbox consumer has not handed over yet.
const MAX_ROUTED: usize = 1024;

/// What `<home>/mcp.json` keeps.
#[derive(Default, Serialize, Deserialize)]
pub(super) struct Stored {
    /// Asks whose reply the inbox consumer has not handed over yet, oldest first, with whom they went to.
    pub(super) asks: Vec<(Uuid, UserId)>,
    /// Replies that came after their `ask_ant` call gave up, oldest first.
    pub(super) late: Vec<Envelope>,
    /// Replies a waiting call took, oldest first, until the inbox consumer hands them over.
    #[serde(default)]
    pub(super) routed: Vec<Uuid>,
    /// Late replies `check_inbox` took, oldest first: their prompt is skipped.
    #[serde(default)]
    pub(super) taken: Vec<Uuid>,
    /// Late replies whose prompt began, oldest first: a redelivery is prompted again.
    #[serde(default)]
    pub(super) prompted: Vec<Uuid>,
}

impl Stored {
    fn asked(&self, ask: Uuid) -> Option<&UserId> {
        self.asks.iter().find(|(id, _)| *id == ask).map(|(_, to)| to)
    }

    fn forget(&mut self, ask: Uuid) {
        self.asks.retain(|(id, _)| *id != ask);
    }

    fn is_late(&self, reply: Uuid) -> bool {
        self.late.iter().any(|e| e.id == reply) || self.prompted.contains(&reply)
    }
}

/// What a message from the inbox consumer is to the asks ([`Mcp::claim`](super::Mcp::claim)).
#[derive(Debug, PartialEq, Eq)]
pub enum Claim {
    /// An ordinary message.
    No,
    /// The reply of a call, handed over, or a late one `check_inbox` took: acked, never prompted.
    Taken,
    /// A reply that came after its call gave up, kept for `check_inbox`: prompted on its thread when its turn comes
    /// there, so it is not acked now.
    Late,
}

/// Push `id` onto `ids`, dropping the oldest beyond `max`.
fn push_bounded(ids: &mut Vec<Uuid>, id: Uuid, max: usize) {
    ids.push(id);
    let excess = ids.len().saturating_sub(max);
    ids.drain(..excess);
}

pub(super) struct Asks {
    pub(super) stored: Stored,
    /// `ask_ant` calls waiting, by the id of their ask.
    pub(super) waiting: HashMap<Uuid, oneshot::Sender<Envelope>>,
}

impl Hub {
    /// Send `envelope` and wait up to `wait` (or until `cancelled`) for its reply.
    pub(super) async fn ask(
        &self,
        envelope: Envelope,
        wait: Duration,
        cancelled: impl Future<Output = ()>,
    ) -> Result<Envelope, String> {
        let (tx, mut rx) = oneshot::channel();
        {
            let mut asks = self.asks.lock().await;
            asks.stored.asks.push((envelope.id, envelope.to.clone()));
            let excess = asks.stored.asks.len().saturating_sub(MAX_ASKS);
            asks.stored.asks.drain(..excess);
            asks.waiting.insert(envelope.id, tx);
            // Remembered before it is sent, so a reply after a restart is not taken for a new message.
            if let Err(e) = self.save(&asks.stored).await {
                asks.stored.forget(envelope.id);
                asks.waiting.remove(&envelope.id);
                return Err(format!("not sent: {e:#}"));
            }
        }
        if let Err(e) = self.send(&envelope).await {
            let mut asks = self.asks.lock().await;
            asks.waiting.remove(&envelope.id);
            asks.stored.forget(envelope.id);
            return Err(e);
        }
        tokio::select! {
            reply = &mut rx => if let Ok(reply) = reply { return Ok(reply) },
            () = tokio::time::sleep(wait) => {}
            () = cancelled => {}
        }
        // Routing sends under the lock, so once the waiter is gone a reply is either in `rx` already or kept for later.
        self.asks.lock().await.waiting.remove(&envelope.id);
        rx.try_recv().map_err(|_| {
            format!(
                "no reply from {} within {}s; a reply that comes later shows up in check_inbox, and once this turn \
                 ends is prompted on this thread",
                envelope.to,
                wait.as_secs()
            )
        })
    }

    /// Hand a reply from the live subscription to the call waiting for it.
    pub(super) async fn route(&self, envelope: Envelope) {
        let Some(ask) = envelope.in_reply_to else { return };
        let mut asks = self.asks.lock().await;
        if asks.stored.asked(ask) != Some(&envelope.from) {
            return;
        }
        let Some(waiter) = asks.waiting.remove(&ask) else { return };
        let id = envelope.id;
        if waiter.send(envelope).is_ok() {
            push_bounded(&mut asks.stored.routed, id, MAX_ROUTED);
            // Kept so that after a restart the inbox consumer's copy is not shown again; losing it only risks that.
            if let Err(e) = self.save(&asks.stored).await {
                tracing::warn!(error = format!("{e:#}"), "saving MCP state");
            }
        }
    }

    pub(super) async fn claim(&self, envelope: &Envelope) -> Result<Claim> {
        let Some(ask) = envelope.in_reply_to else { return Ok(Claim::No) };
        let mut asks = self.asks.lock().await;
        if asks.stored.taken.contains(&envelope.id) {
            return Ok(Claim::Taken);
        }
        if asks.stored.is_late(envelope.id) {
            return Ok(Claim::Late);
        }
        if asks.stored.asked(ask) != Some(&envelope.from) {
            return Ok(Claim::No);
        }
        asks.stored.forget(ask);
        let routed = asks.stored.routed.len();
        asks.stored.routed.retain(|id| *id != envelope.id);
        let taken = asks.stored.routed.len() < routed
            || asks.waiting.remove(&ask).is_some_and(|waiter| waiter.send(envelope.clone()).is_ok());
        if taken {
            // Saving only shortens the list of asks; a failure leaves an entry that never matches again.
            if let Err(e) = self.save(&asks.stored).await {
                tracing::warn!(error = format!("{e:#}"), "saving MCP state");
            }
            return Ok(Claim::Taken);
        }
        tracing::info!(thread = %envelope.thread_id, from = %envelope.from, "keeping a late reply for check_inbox and a prompt");
        asks.stored.late.push(envelope.clone());
        let excess = asks.stored.late.len().saturating_sub(MAX_LATE);
        asks.stored.late.drain(..excess);
        if let Err(e) = self.save(&asks.stored).await {
            // Not acked, so it comes again; it must still count as a reply then.
            asks.stored.late.retain(|e| e.id != envelope.id);
            asks.stored.asks.push((ask, envelope.from.clone()));
            return Err(e);
        }
        Ok(Claim::Late)
    }

    /// Whether the late reply `envelope` is still to be prompted as its turn comes: not when `check_inbox` took it.
    /// From now on `check_inbox` no longer shows it, and a redelivery is prompted again.
    pub(super) async fn late_turn(&self, envelope: &Envelope) -> Result<bool> {
        let mut asks = self.asks.lock().await;
        if asks.stored.taken.contains(&envelope.id) {
            return Ok(false);
        }
        if asks.stored.prompted.contains(&envelope.id) {
            return Ok(true);
        }
        let Some(at) = asks.stored.late.iter().position(|e| e.id == envelope.id) else {
            // Dropped from the full list unread: prompted all the same.
            return Ok(true);
        };
        let kept = asks.stored.late.remove(at);
        push_bounded(&mut asks.stored.prompted, envelope.id, MAX_LATE);
        if let Err(e) = self.save(&asks.stored).await {
            asks.stored.prompted.retain(|id| *id != envelope.id);
            asks.stored.late.insert(at, kept);
            return Err(e);
        }
        Ok(true)
    }

    /// Take the late replies on `thread`, or on every thread.
    pub(super) async fn take_late(&self, thread: Option<&ThreadId>) -> Result<Vec<Envelope>> {
        let mut asks = self.asks.lock().await;
        let (taken, kept) =
            std::mem::take(&mut asks.stored.late).into_iter().partition(|e| thread.is_none_or(|t| &e.thread_id == t));
        asks.stored.late = kept;
        let ids: Vec<Uuid> = taken.iter().map(|e| e.id).collect();
        for id in &ids {
            push_bounded(&mut asks.stored.taken, *id, MAX_LATE);
        }
        if let Err(e) = self.save(&asks.stored).await {
            asks.stored.taken.retain(|id| !ids.contains(id));
            asks.stored.late.extend(taken);
            return Err(e);
        }
        Ok(taken)
    }

    async fn save(&self, stored: &Stored) -> Result<()> {
        let tmp = self.state_path.with_extension("json.tmp");
        tokio::fs::write(&tmp, serde_json::to_vec(stored)?).await?;
        tokio::fs::rename(&tmp, &self.state_path)
            .await
            .with_context(|| format!("writing {}", self.state_path.display()))
    }
}
