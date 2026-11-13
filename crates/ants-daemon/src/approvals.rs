//! Escalating tool requests to the humans over NATS.
//!
//! The daemon subscribes to `ants.approvals.<persona>.<request id>`, then publishes the [`ApprovalRequest`] to
//! `ants.approvals.<persona>`, where the stream keeps it, so an `ants approve` started later still finds it. Any human
//! publishes the [`ApprovalDecision`] to the request's own subject, which only humans may write to; the first decision
//! stored wins. No decision before `expires_at` rejects the request.
//!
//! A decision heard on the subject is only a sign that one was published: core NATS delivers a publish the stream then
//! refuses, as when `ants approve` loses to another human's decision, and the two may arrive in either order. So the
//! persona acts on the decision the stream stored on the subject, read back with a direct get of its last message
//! (`$JS.API.DIRECT.GET.ANTS.<decision subject>`, which credentials allow for the persona's own requests only); while
//! none is stored it looks again every [`RECHECK`]. `ants approve` stores a decision only while the subject holds none,
//! so the last stored is the first. The stored decision is not republished to a subject of its own: a persona can point
//! a push consumer at any subject outside the stream, and nats-server does not check its deliver subject against the
//! persona's permissions, so only a subject the stream captures, or a reply to the persona's own request, cannot be
//! forged. A stream without direct gets, from before them, or credentials without the permission make the persona act
//! on the first decision it hears, as it used to, with a warning: `ants init` and `ants auth user` upgrade them.

use std::time::Duration;

use ants_proto::{ApprovalDecision, ApprovalRequest, PersonaId, STREAM, State, ThreadId, ToolCall, Verdict};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use async_nats::jetstream::stream::DirectGetErrorKind;
use futures::StreamExt as _;
use futures::future::BoxFuture;

use crate::permission::Approver;

/// How long a tool request waits for a human by default.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How often the persona looks for the stored decision after hearing one that is not stored (yet).
pub const RECHECK: Duration = Duration::from_millis(250);

/// What the stream holds on a request's decision subject.
enum Stored {
    Decision(ApprovalDecision),
    /// Nothing, or nothing that decides the request.
    None,
    /// The stream cannot be asked: no direct gets, or credentials without them.
    Unknown(String),
}

/// Asks the humans on `ants.approvals.<persona>`.
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
            // The first decision heard for the request, acted on only if the stream cannot be asked.
            let mut heard: Option<ApprovalDecision> = None;
            loop {
                let next = match heard {
                    // Until the stream holds a decision, it is asked again every RECHECK.
                    Some(_) => tokio::time::timeout(RECHECK, decisions.next()).await.ok(),
                    None => Some(decisions.next().await),
                };
                match next {
                    Some(Some(msg)) => match decided(&request, &msg.payload) {
                        Some(d) => {
                            heard.get_or_insert(d);
                        }
                        None => {
                            tracing::warn!(subject = %msg.subject, "ignoring a malformed decision or one for another request")
                        }
                    },
                    Some(None) => return Verdict::Reject,
                    None => {}
                }
                let Some(first) = &heard else { continue };
                let decision = match self.stored(&request).await {
                    Stored::Decision(d) => d,
                    Stored::None => continue,
                    Stored::Unknown(error) => {
                        tracing::warn!(
                            request = %request.id,
                            error,
                            "cannot read the stored decision; taking the first one heard (`ants init` and \
                             `ants auth user` as a human upgrade the stream and the credentials)"
                        );
                        first.clone()
                    }
                };
                // A verdict the harness did not offer counts as a rejection.
                return request.decide(decision.verdict).verdict;
            }
        };
        let verdict = tokio::time::timeout(self.timeout, wait).await.unwrap_or_else(|_| {
            tracing::info!(request = %request.id, "approval request expired; rejecting");
            Verdict::Reject
        });
        crate::status::state(&self.client, &self.persona, &request.thread_id, State::Working);
        Ok(verdict)
    }

    /// The decision the stream stored on `request`'s decision subject: its last message, which is also its first, as
    /// `ants approve` stores one only while the subject holds none.
    async fn stored(&self, request: &ApprovalRequest) -> Stored {
        let stream = match self.js.get_stream_no_info(STREAM).await {
            Ok(stream) => stream,
            Err(e) => return Stored::Unknown(e.to_string()),
        };
        match stream.direct_get_last_for_subject(request.decision_subject()).await {
            Ok(msg) => match decided(request, &msg.payload) {
                Some(decision) => Stored::Decision(decision),
                None => {
                    tracing::warn!(request = %request.id, "ignoring a stored decision, malformed or for another request");
                    Stored::None
                }
            },
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => Stored::None,
            Err(e) => Stored::Unknown(e.to_string()),
        }
    }
}

/// `payload` as a decision on `request`, if it is one.
fn decided(request: &ApprovalRequest, payload: &[u8]) -> Option<ApprovalDecision> {
    serde_json::from_slice::<ApprovalDecision>(payload)
        .ok()
        .filter(|d| d.request_id == request.id && d.persona == request.persona)
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

    /// Long enough for a busy machine running the whole suite.
    const WAIT: Duration = Duration::from_secs(30);

    /// Returns once the server has registered the subscriptions of `js`'s client: async-nats' flush only writes them
    /// out, so another client's publish could otherwise get there first, and be missed.
    async fn subscribed(js: &jetstream::Context) {
        js.get_stream(ants_proto::STREAM).await.unwrap();
    }

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
        subscribed(&human_js).await;

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

    /// A decision the stream refuses, as `ants approve` loses to another human's, still reaches the persona.
    async fn publish_refused(human: &async_nats::Client, decision: &ApprovalDecision) {
        let mut headers = async_nats::HeaderMap::new();
        // Nothing is stored on the subject yet, so the stream refuses this.
        headers.insert(async_nats::header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, "1");
        let payload = serde_json::to_vec(decision).unwrap().into();
        human.publish_with_headers(decision.subject(), headers, payload).await.unwrap();
        human.flush().await.unwrap();
    }

    #[tokio::test]
    async fn the_persona_acts_on_the_stored_decision_not_the_first_heard() {
        let Some(server) = Server::start() else { return };
        let persona: PersonaId = "dev-a".parse().unwrap();
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &persona, "test").await.unwrap();
        let stream = bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &PersonaId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&persona)).await.unwrap();
        subscribed(&human_js).await;
        let mut next_request = async || {
            let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
            serde_json::from_slice::<ApprovalRequest>(&msg.payload).unwrap()
        };

        let approver = NatsApprover::new(client.clone(), js.clone(), persona.clone(), WAIT);
        let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options.clone()));
        let request = next_request().await;
        let mut heard = client.subscribe(request.decision_subject()).await.unwrap();
        subscribed(&js).await;
        publish_refused(&human, &request.decide(Verdict::AllowOnce)).await;
        timeout(WAIT, heard.next()).await.unwrap().unwrap();
        tokio::time::sleep(RECHECK * 2).await;
        assert!(!decided.is_finished(), "a decision heard but not stored decides nothing");
        let winner = request.decide(Verdict::AllowAlways);
        human_js.publish(winner.subject(), serde_json::to_vec(&winner).unwrap().into()).await.unwrap().await.unwrap();
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);
        let stored = stream.direct_get_last_for_subject(winner.subject()).await.unwrap();
        assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), winner);

        // A decision that is never stored leaves the request to expire.
        let impatient = NatsApprover::new(client.clone(), js.clone(), persona.clone(), Duration::from_secs(1));
        let decided = tokio::spawn(impatient.decide(thread.clone(), tool(), options.clone()));
        publish_refused(&human, &next_request().await.decide(Verdict::AllowOnce)).await;
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::Reject);

        // A stream from before direct gets leaves only the first decision heard.
        let mut config = stream.cached_info().config.clone();
        config.allow_direct = false;
        human_js.update_stream(&config).await.unwrap();
        let decided = tokio::spawn(approver.decide(thread, tool(), options));
        publish_refused(&human, &next_request().await.decide(Verdict::AllowOnce)).await;
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    }
}
