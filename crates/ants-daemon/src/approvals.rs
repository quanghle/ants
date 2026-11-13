//! Escalating tool requests to the human over NATS.
//!
//! The daemon subscribes to `ants.approvals.<persona>.<request id>`, then publishes the [`ApprovalRequest`] to
//! `ants.approvals.<persona>`, where the stream keeps it, so an `ants approve` started later still finds it. The human
//! publishes the [`ApprovalDecision`] to the request's own subject, which only the human may write to. No decision
//! before `expires_at` rejects the request.

use std::time::Duration;

use ants_proto::{ApprovalDecision, ApprovalRequest, PersonaId, State, ThreadId, ToolCall, Verdict};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use futures::StreamExt as _;
use futures::future::BoxFuture;

use crate::permission::Approver;

/// How long a tool request waits for the human by default.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Asks the human on `ants.approvals.<persona>`.
#[derive(Clone)]
pub struct NatsApprover {
    client: async_nats::Client,
    js: jetstream::Context,
    persona: PersonaId,
    timeout: Duration,
}

impl NatsApprover {
    pub fn new(client: async_nats::Client, js: jetstream::Context, persona: PersonaId, timeout: Duration) -> Self {
        Self { client, js, persona, timeout }
    }

    async fn ask(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> Result<Verdict> {
        let request = ApprovalRequest::new(self.persona.clone(), thread, tool, options, self.timeout);
        // Subscribe first, so a quick answer is not missed.
        let mut decisions = self.client.subscribe(request.decision_subject()).await?;
        self.js
            .publish(request.subject(), serde_json::to_vec(&request)?.into())
            .await
            .context("publishing the approval request")?
            .await
            .context("waiting for JetStream to store the approval request")?;
        tracing::info!(request = %request.id, thread = %request.thread_id, title = %request.tool.title, "waiting for approval");
        crate::status::state(&self.client, &self.persona, &request.thread_id, State::WaitingApproval);
        let wait = async {
            while let Some(msg) = decisions.next().await {
                match serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
                    Ok(d) if d.request_id == request.id && d.persona == request.persona => {
                        // A verdict the harness did not offer counts as a rejection.
                        return request.decide(d.verdict).verdict;
                    }
                    Ok(_) => tracing::warn!(subject = %msg.subject, "ignoring a decision for another request"),
                    Err(e) => tracing::warn!(error = %e, subject = %msg.subject, "ignoring a malformed decision"),
                }
            }
            Verdict::Reject
        };
        let verdict = tokio::time::timeout(self.timeout, wait).await.unwrap_or_else(|_| {
            tracing::info!(request = %request.id, "approval request expired; rejecting");
            Verdict::Reject
        });
        crate::status::state(&self.client, &self.persona, &request.thread_id, State::Working);
        Ok(verdict)
    }
}

impl Approver for NatsApprover {
    fn decide(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> BoxFuture<'static, Verdict> {
        let this = self.clone();
        Box::pin(async move {
            this.ask(thread, tool, options).await.unwrap_or_else(|e| {
                tracing::warn!(error = format!("{e:#}"), "escalating a tool request failed; rejecting it");
                Verdict::Reject
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::subject;
    use ants_test_support::NatsServer as Server;
    use tokio::time::timeout;

    use super::*;
    use crate::bus;

    fn tool() -> ToolCall {
        ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() }
    }

    #[tokio::test]
    async fn the_human_decides_and_silence_rejects() {
        let Some(server) = Server::start() else { return };
        let persona: PersonaId = "dev-a".parse().unwrap();
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &persona, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &PersonaId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&persona)).await.unwrap();
        let mut statuses = human.subscribe(subject::status(&persona, &thread)).await.unwrap();
        human.flush().await.unwrap();

        let approver = NatsApprover::new(client.clone(), js.clone(), persona.clone(), Duration::from_secs(10));
        let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options));
        let msg = timeout(Duration::from_secs(5), requests.next()).await.unwrap().unwrap();
        let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((&request.persona, &request.thread_id, &request.tool), (&persona, &thread, &tool()));
        assert_eq!(request.options, [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);
        let status = timeout(Duration::from_secs(5), statuses.next()).await.unwrap().unwrap();
        let status: ants_proto::StatusEvent = serde_json::from_slice(&status.payload).unwrap();
        assert_eq!(status.update, ants_proto::StatusUpdate::State { state: State::WaitingApproval });

        let decision = request.decide(Verdict::AllowAlways);
        human_js
            .publish(decision.subject(), serde_json::to_vec(&decision).unwrap().into())
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(timeout(Duration::from_secs(5), decided).await.unwrap().unwrap(), Verdict::AllowAlways);

        // Both the request and the decision stay in the stream for a late `ants approve`.
        let stream = js.get_stream(ants_proto::STREAM).await.unwrap();
        assert_eq!(stream.get_last_raw_message_by_subject(&request.subject()).await.unwrap().sequence, 1);
        assert!(stream.get_last_raw_message_by_subject(&decision.subject()).await.is_ok());

        let impatient = NatsApprover::new(client, js, persona, Duration::from_millis(300));
        assert_eq!(impatient.decide(thread, tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);
    }
}
