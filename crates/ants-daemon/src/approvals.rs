//! Escalating tool requests to the humans, and to the ant's chain, over NATS.
//!
//! The daemon subscribes to `ants.approvals.<ant>.<request id>`, then publishes the [`ApprovalRequest`] to
//! `ants.approvals.<ant>`, where the stream keeps it, so an `ants tools list` started later still finds it. Any human
//! publishes the [`ApprovalDecision`] to the request's own subject, which only humans may write to; the first decision
//! stored wins. No decision before `expires_at` rejects the request.
//!
//! A decision heard on the subject is only a sign that one was published: core NATS delivers a publish the stream then
//! refuses, as when `ants tools approve` loses to another human's decision, and the two may arrive in either order. So
//! the ant acts on the decision the stream stored on the subject, read back with a direct get of its last message
//! (`$JS.API.DIRECT.GET.ANTS.<decision subject>`, which credentials allow for the ant's own requests only); while
//! none is stored it looks again every [`RECHECK`]. `ants tools approve` and `reject` store a decision only while the
//! subject holds none, so the last stored is the first. The stored decision is not republished to a subject of its
//! own: an ant can point a push consumer at any subject outside the stream, and nats-server does not check its deliver
//! subject against the ant's permissions, so only a subject the stream captures, or a reply to the ant's own request,
//! cannot be forged. A stream without direct gets, from before them, or credentials without the permission make the ant
//! act on the first decision it hears, as it used to, with a warning: `ants init` and `ants auth user` upgrade them.
//!
//! # The ant's chain
//!
//! With an `escalate_to`, the request also goes to every ant of it, on `ants.inbox.<member>.<ant>`, as an
//! approval escalation at level 1 whose text shows the tool call ([`NatsApprover::with_chain`]). Any member decides
//! with the MCP tool `decide_approval`, which sends an envelope carrying the decision to
//! `ants.inbox.<ant>.<member>`: the subject pins the member, so ant credentials need no rights on another
//! ant's approvals. The ant takes such a decision only from its own `escalate_to` and for a request still
//! waiting ([`Envelope::chain_decision`]); it hears it on a core subscription to its inbox while waiting, and the inbox
//! consumer, which delivers it again later, acks it without prompting ([`NatsApprover::offer`]). The first decision
//! wins: a member's, unless a human's is stored by the time the member's arrives. The escalation does not climb further
//! nor wait in `ants_sessions`: the request has its own time limit, and only the ant's own chain may decide it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::{
    ApprovalDecision, ApprovalRequest, Envelope, Escalation, Hop, STREAM, State, ThreadId, ToolCall, UserId, Uuid,
    Verdict,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use async_nats::jetstream::stream::DirectGetErrorKind;
use futures::StreamExt as _;
use futures::future::BoxFuture;
use tokio::sync::mpsc;

use crate::bus;
use crate::escalation::Chain;
use crate::permission::Approver;

/// How long a tool request waits for a human by default.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How often the ant looks for the stored decision after hearing one that is not stored (yet).
pub const RECHECK: Duration = Duration::from_millis(250);

/// What the stream holds on a request's decision subject.
enum Stored {
    Decision(ApprovalDecision),
    /// Nothing, or nothing that decides the request.
    None,
    /// The stream cannot be asked: no direct gets, or credentials without them.
    Unknown(String),
}

/// A decision on a waiting request from an agent of the ant's chain, and who sent it.
type FromChain = (UserId, ApprovalDecision);

/// The requests waiting for a decision, by id, with where a chain decision on each goes.
type Waiting = Arc<Mutex<HashMap<Uuid, (ApprovalRequest, mpsc::UnboundedSender<FromChain>)>>>;

/// Asks the humans on `ants.approvals.<ant>`, and the ant's chain.
#[derive(Clone)]
pub struct NatsApprover {
    client: async_nats::Client,
    js: jetstream::Context,
    ant: UserId,
    timeout: Duration,
    /// The ant's escalate_to and its hop limit; an empty chain asks only the humans.
    chain: Chain,
    waiting: Waiting,
}

/// Removes a request from [`NatsApprover::waiting`] once it is decided, expired or given up.
struct Pending<'a> {
    waiting: &'a Waiting,
    request: Uuid,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.waiting.lock().expect("waiting requests").remove(&self.request);
    }
}

/// Something heard while a request waits.
enum Heard {
    /// A message on the request's decision subject.
    Human(async_nats::Message),
    /// A decision from an agent of the ant's chain.
    Chain(FromChain),
    /// The subscription to the decision subject ended.
    Closed,
    Nothing,
}

impl NatsApprover {
    pub fn new(client: async_nats::Client, js: jetstream::Context, ant: UserId, timeout: Duration) -> Self {
        Self { client, js, ant, timeout, chain: Chain::default(), waiting: Waiting::default() }
    }

    /// The same, also asking every ant of `chain`'s escalate_to, which may decide too.
    pub fn with_chain(self, chain: Chain) -> Self {
        Self { chain, ..self }
    }

    /// Hand `envelope`, which carries an approval decision and arrived on its own subject
    /// ([`Envelope::check_subject`]), to the request it decides, if that one still waits and the sender is in the
    /// ant's escalate_to. Anything else is ignored: a late decision, or one from outside the chain. Never prompt
    /// with it.
    pub fn offer(&self, envelope: &Envelope) {
        let Some(decision) = envelope.approval.as_deref() else { return };
        let waiting = self.waiting.lock().expect("waiting requests");
        let Some((request, chain_decided)) = waiting.get(&decision.request_id) else {
            let (from, request) = (&envelope.from, decision.request_id);
            tracing::debug!(%from, %request, "ignoring a decision on a request no longer waiting");
            return;
        };
        match envelope.chain_decision(request, &self.chain.escalate_to) {
            Some(decided) => {
                let _ = chain_decided.send((envelope.from.clone(), decided));
            }
            None => tracing::warn!(
                from = %envelope.from,
                request = %decision.request_id,
                "ignoring an approval decision from outside the ant's escalate_to"
            ),
        }
    }

    /// Send `request` to every ant of the chain, as an approval escalation showing the tool call. Returns whom it
    /// went to: none when the chain is empty or allows no hop.
    async fn ask_chain(&self, request: &ApprovalRequest) -> Result<Vec<UserId>> {
        let Hop::Chain { escalation, to } =
            Escalation::of_request(request).up(&self.chain.escalate_to, self.chain.max_hops)
        else {
            return Ok(Vec::new());
        };
        let text = chain_text(request, &to);
        for member in &to {
            let envelope =
                Envelope::on_thread(request.thread_id.clone(), self.ant.clone(), member.clone(), text.clone())
                    .with_escalation(Some(escalation.clone()));
            let id = format!("approval-{}-{}-{member}", self.ant, request.id);
            bus::store(&self.js, envelope.subject(), &envelope, &id).await?;
        }
        Ok(to)
    }

    async fn ask(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> Result<Verdict> {
        let request = ApprovalRequest::new(self.ant.clone(), thread, tool, options, self.timeout);
        // Subscribe first, so a quick answer is not missed.
        let mut decisions = self.client.subscribe(request.decision_subject()).await?;
        let mut inbox = match self.chain.escalate_to.is_empty() {
            true => None,
            false => Some(bus::live_inbox(&self.client, &self.ant).await?),
        };
        let (chain_decided, mut from_chain) = mpsc::unbounded_channel();
        self.waiting.lock().expect("waiting requests").insert(request.id, (request.clone(), chain_decided));
        let _pending = Pending { waiting: &self.waiting, request: request.id };
        self.js
            .publish(request.subject(), serde_json::to_vec(&request)?.into())
            .await
            .context("publishing the approval request")?
            .await
            .context("waiting for JetStream to store the approval request")?;
        tracing::info!(request = %request.id, thread = %request.thread_id, title = %request.tool.title, "waiting for approval");
        match self.ask_chain(&request).await {
            Ok(to) if to.is_empty() => {}
            Ok(to) => tracing::info!(request = %request.id, ?to, "asked the ant's chain too"),
            Err(e) => tracing::warn!(request = %request.id, error = format!("{e:#}"), "asking the ant's chain"),
        }
        crate::status::state(&self.client, &self.ant, &request.thread_id, State::WaitingApproval);
        let wait = async {
            // The first decision heard for the request, acted on only if the stream cannot be asked.
            let mut heard: Option<ApprovalDecision> = None;
            loop {
                // Until the stream holds a decision heard, it is asked again every RECHECK.
                let recheck = heard.is_some();
                let next = tokio::select! {
                    msg = decisions.next() => msg.map_or(Heard::Closed, Heard::Human),
                    msg = next_message(&mut inbox) => match msg {
                        Some(msg) => self.chain_decision(&request, &msg).map_or(Heard::Nothing, Heard::Chain),
                        None => {
                            inbox = None;
                            Heard::Nothing
                        }
                    },
                    Some(decided) = from_chain.recv() => Heard::Chain(decided),
                    () = tokio::time::sleep(RECHECK), if recheck => Heard::Nothing,
                };
                match next {
                    Heard::Human(msg) => match decided(&request, &msg.payload) {
                        Some(d) => {
                            heard.get_or_insert(d);
                        }
                        None => {
                            tracing::warn!(subject = %msg.subject, "ignoring a malformed decision or one for another request")
                        }
                    },
                    Heard::Chain((from, decision)) => {
                        // A human's decision stored by now came first.
                        let human = match self.stored(&request).await {
                            Stored::Decision(d) => Some(d),
                            Stored::Unknown(_) => heard.clone(),
                            Stored::None => None,
                        };
                        if let Some(human) = human {
                            return request.decide(human.verdict).verdict;
                        }
                        let verdict = decision.verdict.as_str();
                        tracing::info!(request = %request.id, %from, verdict, "decided by the ant's chain");
                        return decision.verdict;
                    }
                    Heard::Closed => return Verdict::Reject,
                    Heard::Nothing => {}
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
        crate::status::state(&self.client, &self.ant, &request.thread_id, State::Working);
        Ok(verdict)
    }

    /// The decision the stream stored on `request`'s decision subject: its last message, which is also its first, as
    /// `ants tools approve` and `reject` store one only while the subject holds none.
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

    /// The decision on `request` that `msg`, heard on the ant's inbox, carries from an agent of its chain, if any.
    fn chain_decision(&self, request: &ApprovalRequest, msg: &async_nats::Message) -> Option<FromChain> {
        let envelope = serde_json::from_slice::<Envelope>(&msg.payload).ok().filter(|e| e.approval.is_some())?;
        if let Err(e) = envelope.check_subject(&msg.subject) {
            tracing::warn!(error = %e, "ignoring an approval decision with a forged sender");
            return None;
        }
        let decided = envelope.chain_decision(request, &self.chain.escalate_to);
        if decided.is_none() && envelope.approval.as_ref().is_some_and(|d| d.request_id == request.id) {
            let (from, request) = (&envelope.from, request.id);
            tracing::warn!(%from, %request, "ignoring an approval decision from outside the ant's escalate_to");
        }
        Some((envelope.from, decided?))
    }
}

/// The next message of `subscriber`, if there is one; without one, never.
async fn next_message(subscriber: &mut Option<bus::LiveInbox>) -> Option<async_nats::Message> {
    match subscriber {
        Some(subscriber) => subscriber.next().await,
        None => std::future::pending().await,
    }
}

/// What every ant of the chain, `to`, gets about `request`: how to decide it, and the tool call.
fn chain_text(request: &ApprovalRequest, to: &[UserId]) -> String {
    let verdicts: Vec<&str> = request.options.iter().map(|v| v.as_str()).collect();
    let to: Vec<&str> = to.iter().map(UserId::as_str).collect();
    let (ant, id, thread) = (&request.ant, request.id, &request.thread_id);
    let mut text = format!(
        "[ants] Approval request {id} on thread {thread}: {ant} asks to run a tool outside its allowlist. Sent to \
         {}; any of them decides with the decide_approval tool ({}). Humans may decide too, with `ants tools \
         approve {ant}/{id}` or `ants tools reject {ant}/{id}`; the first decision wins. Without one by {} the \
         request is rejected.\n\nTool: {} ({})",
        to.join(", "),
        verdicts.join(", "),
        request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        request.tool.title,
        request.tool.kind,
    );
    if !request.tool.locations.is_empty() {
        text.push_str(&format!("\nFiles: {}", request.tool.locations.join(", ")));
    }
    if let Some(input) = &request.tool.input {
        text.push_str(&format!("\nInput: {input}"));
    }
    text
}

/// `payload` as a decision on `request`, if it is one.
fn decided(request: &ApprovalRequest, payload: &[u8]) -> Option<ApprovalDecision> {
    serde_json::from_slice::<ApprovalDecision>(payload)
        .ok()
        .filter(|d| d.request_id == request.id && d.ant == request.ant)
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
    use ants_proto::{EscalationReason, subject};
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
        let ant: UserId = "dev-a".parse().unwrap();
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
        let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
        subscribed(&human_js).await;

        let approver = NatsApprover::new(client.clone(), js.clone(), ant.clone(), Duration::from_secs(10));
        let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options));
        let msg = timeout(Duration::from_secs(5), requests.next()).await.unwrap().unwrap();
        let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((&request.ant, &request.thread_id, &request.tool), (&ant, &thread, &tool()));
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

        // Both the request and the decision stay in the stream for a late `ants tools list`.
        let stream = js.get_stream(ants_proto::STREAM).await.unwrap();
        assert_eq!(stream.get_last_raw_message_by_subject(&request.subject()).await.unwrap().sequence, 1);
        assert!(stream.get_last_raw_message_by_subject(&decision.subject()).await.is_ok());

        let impatient = NatsApprover::new(client, js, ant, Duration::from_millis(300));
        assert_eq!(impatient.decide(thread, tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);
    }

    /// A decision the stream refuses, as `ants tools approve` loses to another human's, still reaches the ant.
    async fn publish_refused(human: &async_nats::Client, decision: &ApprovalDecision) {
        let mut headers = async_nats::HeaderMap::new();
        // Nothing is stored on the subject yet, so the stream refuses this.
        headers.insert(async_nats::header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, "1");
        let payload = serde_json::to_vec(decision).unwrap().into();
        human.publish_with_headers(decision.subject(), headers, payload).await.unwrap();
        human.flush().await.unwrap();
    }

    #[tokio::test]
    async fn the_ant_acts_on_the_stored_decision_not_the_first_heard() {
        let Some(server) = Server::start() else { return };
        let ant: UserId = "dev-a".parse().unwrap();
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
        let stream = bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
        subscribed(&human_js).await;
        let mut next_request = async || {
            let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
            serde_json::from_slice::<ApprovalRequest>(&msg.payload).unwrap()
        };

        let approver = NatsApprover::new(client.clone(), js.clone(), ant.clone(), WAIT);
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
        let impatient = NatsApprover::new(client.clone(), js.clone(), ant.clone(), Duration::from_secs(1));
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

    fn chain(escalate_to: &[&UserId]) -> Chain {
        Chain { escalate_to: escalate_to.iter().map(|p| (*p).clone()).collect(), ..Chain::default() }
    }

    /// Publish `envelope` on `subject` and wait for the stream to store it.
    async fn send(js: &jetstream::Context, subject: String, envelope: &Envelope) {
        js.publish(subject, serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
    }

    #[tokio::test]
    async fn the_chain_hears_the_request_and_only_its_decision_counts() {
        let Some(server) = Server::start() else { return };
        let (ant, lead, other): (UserId, UserId, UserId) =
            ("dev-a".parse().unwrap(), "lead".parse().unwrap(), "dev-c".parse().unwrap());
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
        let mut asked = human.subscribe(subject::inbox_of(&lead)).await.unwrap();
        subscribed(&human_js).await;

        let approver = NatsApprover::new(client, js, ant.clone(), WAIT).with_chain(chain(&[&lead]));
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), vec![Verdict::AllowOnce]));
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
        let msg = timeout(WAIT, asked.next()).await.unwrap().unwrap();
        let notice: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        notice.check_subject(&msg.subject).unwrap();
        assert_eq!((&notice.from, &notice.thread_id), (&ant, &thread));
        let escalation = notice.escalation.as_deref().unwrap();
        assert_eq!(
            (escalation.reason, escalation.request_id, escalation.level),
            (EscalationReason::Approval, Some(request.id), 1)
        );
        assert!(notice.text.contains("decide_approval") && notice.text.contains("git push"), "{}", notice.text);

        // Outside the chain, forged, or for another request: ignored.
        let decide =
            |from: &UserId, decision: ApprovalDecision| Envelope::decision(thread.clone(), from.clone(), decision);
        let foreign = decide(&other, request.decide(Verdict::AllowOnce));
        send(&human_js, foreign.subject(), &foreign).await;
        let forged = decide(&lead, request.decide(Verdict::AllowOnce));
        send(&human_js, foreign.subject(), &forged).await;
        let elsewhere = decide(&lead, ApprovalDecision::new(Uuid::new_v4(), ant.clone(), Verdict::AllowOnce));
        send(&human_js, elsewhere.subject(), &elsewhere).await;
        tokio::time::sleep(RECHECK * 2).await;
        assert!(!decided.is_finished(), "no decision from the chain yet");

        let lead_says = decide(&lead, request.decide(Verdict::AllowOnce));
        send(&human_js, lead_says.subject(), &lead_says).await;
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
        assert!(approver.waiting.lock().unwrap().is_empty(), "nothing waits once decided");
        approver.offer(&lead_says);
    }

    #[tokio::test]
    async fn the_first_decision_wins_whoever_makes_it() {
        let Some(server) = Server::start() else { return };
        let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
        bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
        subscribed(&human_js).await;
        let approver = NatsApprover::new(client, js, ant.clone(), WAIT).with_chain(chain(&[&lead]));
        let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
        let decide = |request: &ApprovalRequest, verdict| {
            Envelope::decision(thread.clone(), lead.clone(), request.decide(verdict))
        };

        // The inbox consumer hands over the lead's decision, and a human's comes after it.
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options.clone()));
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
        approver.offer(&Envelope::decision(
            thread.clone(),
            "dev-c".parse().unwrap(),
            request.decide(Verdict::AllowAlways),
        ));
        approver.offer(&decide(&request, Verdict::AllowOnce));
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
        let late = request.decide(Verdict::Reject);
        human_js.publish(late.subject(), serde_json::to_vec(&late).unwrap().into()).await.unwrap().await.unwrap();

        // A human's decision stored by the time the lead's arrives wins. The runtime is blocked while another stores
        // it, so the approver gets both at once.
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options));
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
        let (url, human_says) = (server.url.clone(), request.decide(Verdict::AllowAlways));
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let (_human, js) = bus::connect(&url, None, None, &UserId::human(), "human-2").await.unwrap();
                js.publish(human_says.subject(), serde_json::to_vec(&human_says).unwrap().into())
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            })
        })
        .join()
        .unwrap();
        approver.offer(&decide(&request, Verdict::AllowOnce));
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);
    }
}
