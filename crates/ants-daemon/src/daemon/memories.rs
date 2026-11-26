//! The memories any human reads of the ant, its web credentials too, with no admin order ([`MemoriesRequest`] on
//! [`subject::memories`]): the daemon serving the ant answers on [`subject::memoried`] with its core memories as served
//! and its provider's memories view ([`read_shown`]), and refuses where the config sets `[admin] remote = false`. Only
//! the daemon serving the ant hears these, so a standby daemon answers none, and nothing is stored.

use std::time::Duration;

use ants_proto::provider::Connected;
use ants_proto::{CoreMemories, MemoriesOutcome, MemoriesReply, MemoriesRequest, UserId, subject};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;

use crate::admin::memories::{Of, read_shown};
use crate::bus::relayed;
use crate::people::humans::Registry;

/// How long a failed subscription waits before the next try.
const RESUBSCRIBE: Duration = Duration::from_secs(5);
/// Most requests answered at once: a further one waits for one of those to be answered.
const ANSWERING: usize = 4;

/// Refused where the config sets `[admin] remote = false`.
pub(crate) const NOT_REMOTE: &str = "the machine's config refuses remote admin ([admin] remote = false)";

/// What the daemon serving `ant` answers memories requests with.
pub(crate) struct Shown<'a, P> {
    pub client: &'a async_nats::Client,
    pub ant: &'a UserId,
    /// The ant's core memories, as served.
    pub core: &'a CoreMemories,
    /// The config's `[admin] remote`: false refuses every request.
    pub remote: bool,
    pub humans: &'a Registry,
    /// The ant's provider, connected, with the id of the ant's identity record, which the provider knows it by.
    pub provider: P,
}

impl<P: AsyncFn() -> (Option<Connected>, Option<String>)> Shown<'_, P> {
    /// Answer the memories requests for the ant, never ending: several at once, so a slow provider holds up no other.
    pub(crate) async fn hear(&self) {
        let subject = subject::memories(self.ant);
        loop {
            let mut requests = match relayed::subscribe(self.client, subject.clone()).await {
                Ok(requests) => requests,
                Err(e) => {
                    tracing::warn!(error = %e, subject, "hearing memories requests");
                    tokio::time::sleep(RESUBSCRIBE).await;
                    continue;
                }
            };
            let mut answering = FuturesUnordered::new();
            loop {
                tokio::select! {
                    Some(()) = answering.next() => {}
                    message = requests.next(), if answering.len() < ANSWERING => match message {
                        Some(message) => answering.push(self.answer(message)),
                        None => break,
                    },
                }
            }
            while answering.next().await.is_some() {}
        }
    }

    async fn answer(&self, message: async_nats::Message) {
        let request: MemoriesRequest = match serde_json::from_slice(&message.payload) {
            Ok(request) => request,
            Err(e) => {
                tracing::warn!(error = %e, "an unreadable memories request");
                return;
            }
        };
        if let Err(e) = request.check_subject(message.subject.as_str()) {
            tracing::warn!(error = %e, "ignoring a forged memories request");
            return;
        }
        let by = &request.by;
        let outcome = if !self.remote {
            MemoriesOutcome::Refused { error: NOT_REMOTE.to_owned() }
        } else if !self.humans.is_human(by) {
            MemoriesOutcome::Refused { error: format!("{by} is no human of the colony") }
        } else {
            tracing::info!(%by, "reading the ant's memories");
            let (connected, identity) = (self.provider)().await;
            let of = connected.as_ref().map(|connected| Of { connected, identity: identity.as_deref() });
            MemoriesOutcome::Read(read_shown(self.ant, self.core.clone(), of).await)
        };
        self.reply(request.answer(outcome)).await;
    }

    async fn reply(&self, reply: MemoriesReply) {
        let published = match serde_json::to_vec(&reply) {
            Ok(payload) => self.client.publish(reply.subject(), payload.into()).await.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(error) = published {
            tracing::warn!(error, request = %reply.request_id, "answering a memories request");
        }
    }
}

#[cfg(test)]
mod tests;
