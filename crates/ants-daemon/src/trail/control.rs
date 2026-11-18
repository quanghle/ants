//! The conductor's control subject ([`subject::trail`]): humans asking it to start, stop and resume runs. Admin orders
//! to stop and resume one ([`crate::admin::trail`]) are carried out the same way ([`Trails::act`]).

use ants_proto::{TrailAction, TrailOutcome, TrailRequest, UserId, subject};
use futures::StreamExt as _;

use super::Trails;

/// How long a failed subscription waits before the next try.
const RESUBSCRIBE: std::time::Duration = std::time::Duration::from_secs(5);

impl Trails {
    /// Answer the requests on the conductor's control subject, never ending.
    pub(super) async fn hear(&self) {
        let subject = subject::trail(&self.inner.ant);
        loop {
            let mut requests = match self.inner.client.subscribe(subject.clone()).await {
                Ok(requests) => requests,
                Err(e) => {
                    tracing::warn!(error = %e, subject, "hearing trail requests");
                    tokio::time::sleep(RESUBSCRIBE).await;
                    continue;
                }
            };
            while let Some(message) = requests.next().await {
                self.answer(&message).await;
            }
        }
    }

    async fn answer(&self, message: &async_nats::Message) {
        let request: TrailRequest = match serde_json::from_slice(&message.payload) {
            Ok(request) => request,
            Err(e) => {
                tracing::warn!(error = %e, "an unreadable trail request");
                return;
            }
        };
        let by = &request.by;
        let outcome = match request.check_subject(message.subject.as_str()) {
            Ok(()) if !self.inner.humans.is_human(by) => {
                TrailOutcome::Refused { error: format!("{by} is no human of the colony") }
            }
            Ok(()) => self.act(by, &request.action).await,
            Err(e) => TrailOutcome::Refused { error: e.to_string() },
        };
        let reply = request.answer(outcome);
        let published = match serde_json::to_vec(&reply) {
            Ok(payload) => self.inner.client.publish(reply.subject(), payload.into()).await.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        if let Err(error) = published {
            tracing::warn!(error, request = %request.id, "answering a trail request");
        }
    }

    /// Carry out `action` for `by`, a human: what a request on the control subject asks, or an admin's order.
    pub(crate) async fn act(&self, by: &UserId, action: &TrailAction) -> TrailOutcome {
        let done = match action {
            TrailAction::Start { trail, args, thread, limits } => self
                .start_for(by, trail.clone(), args.clone(), thread.clone(), *limits)
                .await
                .map(|run| TrailOutcome::Started { run }),
            TrailAction::Stop { run } => {
                self.stop(run, by).await.map(|waiting| TrailOutcome::Stopped { run: run.clone(), waiting })
            }
            TrailAction::Resume { run, limits } => {
                self.resume(run, by, *limits).await.map(|()| TrailOutcome::Resumed { run: run.clone() })
            }
        };
        done.unwrap_or_else(|error| TrailOutcome::Refused { error })
    }
}
