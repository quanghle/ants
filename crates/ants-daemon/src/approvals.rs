//! Escalating tool requests to the humans, and to the ant's chain, over NATS.
//!
//! The daemon subscribes to `ants.approvals.<ant>.<request id>`, then publishes the [`ApprovalRequest`] to
//! `ants.approvals.<ant>`, where the stream keeps it, so an `ants tools list` started later still finds it. Any human
//! publishes the [`ApprovalDecision`] to the request's own subject, which only humans may write to; the first decision
//! stored wins. No decision before `expires_at` rejects the request, and tells the thread's owner, else the default
//! human, on a thread of its own ([`NatsApprover::begin`]): the request vanishes from `ants tools list`, and the notice
//! says how to retry, wait longer or allow such calls. The notice carries the request's approval escalation, so `ants
//! webhook serve` pushes it to that human's sinks as it does any escalation reaching the human's inbox. The ants of the
//! chain that were asked are told too, on the request's thread, but a human among them already got the notice.
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
use std::fmt::Write as _;
use std::mem::DropGuard;
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
use crate::humans::Registry;
use crate::permission::Approver;

/// How long a tool request waits for a human by default.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_mins(5);

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

/// A decision on a waiting request from an ant of the ant's chain, and who sent it.
type FromChain = (UserId, ApprovalDecision);

/// The requests waiting for a decision, by id, with where a chain decision on each goes.
type Waiting = Arc<Mutex<HashMap<Uuid, (ApprovalRequest, mpsc::UnboundedSender<FromChain>)>>>;

/// The message being answered on each thread, naming whom a request of its turn that expires is told of.
type Answering = Arc<Mutex<HashMap<ThreadId, Envelope>>>;

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
    /// Who is a human, and who is told of an expired request on a thread with no owner.
    humans: Registry,
    answering: Answering,
}

/// Something heard while a request waits.
enum Heard {
    /// A message on the request's decision subject.
    Human(async_nats::Message),
    /// A decision from an ant of the ant's chain.
    Chain(FromChain),
    /// The subscription to the decision subject ended.
    Closed,
    Nothing,
}

impl NatsApprover {
    pub fn new(client: async_nats::Client, js: jetstream::Context, ant: UserId, timeout: Duration) -> Self {
        let (chain, waiting, humans, answering) = Default::default();
        Self { client, js, ant, timeout, chain, waiting, humans, answering }
    }

    /// The same, also asking every ant of `chain`'s escalate_to, which may decide too.
    pub fn with_chain(self, chain: Chain) -> Self {
        Self { chain, ..self }
    }

    /// The same, telling the thread's owner when a human, else the default human of `humans`, of an expired request.
    pub fn with_humans(self, humans: Registry) -> Self {
        Self { humans, ..self }
    }

    /// Note that the ant answers `envelope` on its thread until the returned guard drops: a tool request of the turn
    /// that expires unanswered is told to the owner it names, and carries its creator and the thread it links to.
    pub fn begin(&self, envelope: &Envelope) -> Turn {
        let thread = envelope.thread_id.clone();
        self.answering.lock().expect("answering").insert(thread.clone(), envelope.clone());
        Turn { answering: self.answering.clone(), thread }
    }

    /// Tell the thread's owner, else the default human, and the ants of the chain `asked` about it, that `request`
    /// expired unanswered and was rejected. Stored before the ant goes on, so before the message being answered is
    /// acked; failing to, it only warns.
    async fn tell_expired(&self, request: &ApprovalRequest, asked: &[UserId]) {
        let answering = self.answering.lock().expect("answering").get(&request.thread_id).cloned();
        let notice = expired_notice(&self.ant, request, self.timeout, answering.as_ref(), &self.humans);
        let id = format!("approval-expired-{}-{}", self.ant, request.id);
        let allow = answering.and_then(|e| e.allow);
        let members: Vec<_> = asked
            .iter()
            .filter(|member| **member != notice.to)
            .map(|member| {
                let told = expired_member_notice(&self.ant, request, self.timeout, member).with_allow(allow.clone());
                (told, format!("{id}-{member}"))
            })
            .collect();
        for (told, id) in std::iter::once((notice, id)).chain(members) {
            match bus::store(&self.js, told.subject(), &told, &id).await {
                Ok(_) => {
                    crate::status::escalated(&self.client, &told);
                    tracing::info!(request = %request.id, to = %told.to, "told of the expired approval request")
                }
                Err(e) => tracing::warn!(
                    request = %request.id,
                    to = %told.to,
                    error = format!("{e:#}"),
                    "telling of the expired approval request"
                ),
            }
        }
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

    /// Send `request` to every ant of the chain, as an approval escalation showing the tool call, with the tool rules
    /// of the message the ant answers on the thread. Returns whom it went to: none when the chain is empty or allows
    /// no hop.
    async fn ask_chain(&self, request: &ApprovalRequest) -> Result<Vec<UserId>> {
        let Hop::Chain { escalation, to } =
            Escalation::of_request(request).up(&self.chain.escalate_to, self.chain.max_hops)
        else {
            return Ok(Vec::new());
        };
        let text = chain_text(request, &to);
        let answering = self.answering.lock().expect("answering").get(&request.thread_id).map(|e| e.allow.clone());
        for member in &to {
            let envelope =
                Envelope::on_thread(request.thread_id.clone(), self.ant.clone(), member.clone(), text.clone())
                    .with_escalation(Some(escalation.clone()))
                    .with_allow(answering.clone().flatten());
            let id = format!("approval-{}-{}-{member}", self.ant, request.id);
            bus::store(&self.js, envelope.subject(), &envelope, &id).await?;
            crate::status::escalated(&self.client, &envelope);
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
        // Leaves the waiting map once decided, expired or given up.
        let _pending = DropGuard::new(request.id, |id| {
            self.waiting.lock().expect("waiting requests").remove(&id);
        });
        self.js
            .publish(request.subject(), serde_json::to_vec(&request)?.into())
            .await
            .context("publishing the approval request")?
            .await
            .context("waiting for JetStream to store the approval request")?;
        tracing::info!(request = %request.id, thread = %request.thread_id, title = %request.tool.title, "waiting for approval");
        let asked = match self.ask_chain(&request).await {
            Ok(to) => {
                if !to.is_empty() {
                    tracing::info!(request = %request.id, ?to, "asked the ant's chain too");
                }
                to
            }
            Err(e) => {
                tracing::warn!(request = %request.id, error = format!("{e:#}"), "asking the ant's chain");
                Vec::new()
            }
        };
        let waits = ants_proto::StatusUpdate::Approval { request_id: request.id };
        crate::status::publish(&self.client, &self.ant, &request.thread_id, waits);
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
        let verdict = match tokio::time::timeout(self.timeout, wait).await {
            Ok(verdict) => verdict,
            Err(_) => {
                tracing::info!(request = %request.id, "approval request expired; rejecting");
                self.tell_expired(&request, &asked).await;
                Verdict::Reject
            }
        };
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

    /// The decision on `request` that `msg`, heard on the ant's inbox, carries from an ant of its chain, if any.
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

/// See [`NatsApprover::begin`].
pub struct Turn {
    answering: Answering,
    thread: ThreadId,
}

impl Drop for Turn {
    fn drop(&mut self) {
        self.answering.lock().expect("answering").remove(&self.thread);
    }
}

/// The message on `request`'s thread that tells its owner, else the default human of `humans`, that the request
/// expired unanswered after `timeout` and was rejected. `answering` is the message the ant was answering, naming the
/// thread's owner and creator. On a thread of its own, naming the request's, so that `ants ask --wait` on the request's
/// thread still waits for the ant's reply that follows. It carries the request's escalation, for `ants webhook serve`
/// to push it.
fn expired_notice(
    ant: &UserId,
    request: &ApprovalRequest,
    timeout: Duration,
    answering: Option<&Envelope>,
    humans: &Registry,
) -> Envelope {
    let owner = humans.human_owner(answering.and_then(|e| e.owner.as_ref()));
    let (id, thread, tool) = (request.id, &request.thread_id, &request.tool);
    let mut text = format!(
        "[ants] Tool request {ant}/{id} on thread {thread} expired unanswered after {} and was rejected: {} ({}). \
         {ant} goes on without it.",
        ants_proto::duration::format(timeout),
        tool.title,
        tool.kind,
    );
    if !tool.locations.is_empty() {
        let _ = write!(text, "\nFiles: {}", tool.locations.join(", "));
    }
    let _ = write!(
        text,
        "\nTo retry, ask again with `ants ask --thread {thread} @{ant} TEXT` and decide the new request in time \
         (`ants tools list -f`). To wait longer, raise {ant}'s approval_timeout; to let such calls run unasked, add \
         an allow rule matching them."
    );
    let to = owner.clone().unwrap_or_else(|| humans.default_human().clone());
    Envelope::new(ant.clone(), to, text)
        .with_owner(owner)
        .with_creator(answering.and_then(|e| humans.ant_creator(e.creator.as_ref())))
        .with_origin_thread(answering.and_then(|e| e.origin_thread.clone()))
        .with_escalation(Some(Escalation::of_request(request)))
}

/// The message on `request`'s thread that tells `member`, an ant of the chain asked to decide it, that it expired
/// unanswered after `timeout` and was rejected. It carries no escalation: there is nothing left to decide.
fn expired_member_notice(ant: &UserId, request: &ApprovalRequest, timeout: Duration, member: &UserId) -> Envelope {
    let (id, thread, tool) = (request.id, &request.thread_id, &request.tool);
    let text = format!(
        "[ants] Approval request {id} on thread {thread} expired unanswered after {} and was rejected: {} ({}). \
         {ant} goes on without it; decide_approval no longer applies to it, and no answer is needed.",
        ants_proto::duration::format(timeout),
        tool.title,
        tool.kind,
    );
    Envelope::on_thread(thread.clone(), ant.clone(), member.clone(), text)
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
        let _ = write!(text, "\nFiles: {}", request.tool.locations.join(", "));
    }
    if let Some(input) = &request.tool.input {
        let _ = write!(text, "\nInput: {input}");
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
        let stream = bus::ensure_stream(&js).await.unwrap();
        let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
        let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
        let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
        subscribed(&human_js).await;

        let approver = NatsApprover::new(client.clone(), js.clone(), ant.clone(), Duration::from_secs(10));
        let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
        let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options));
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((&request.ant, &request.thread_id, &request.tool), (&ant, &thread, &tool()));
        assert_eq!(request.options, [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);
        for expected in [
            ants_proto::StatusUpdate::Approval { request_id: request.id },
            ants_proto::StatusUpdate::State { state: State::WaitingApproval },
        ] {
            let status = timeout(WAIT, statuses.next()).await.unwrap().unwrap();
            let status: ants_proto::StatusEvent = serde_json::from_slice(&status.payload).unwrap();
            assert_eq!(status.update, expected);
        }

        let decision = request.decide(Verdict::AllowAlways);
        human_js
            .publish(decision.subject(), serde_json::to_vec(&decision).unwrap().into())
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);

        // Both the request and the decision stay in the stream for a late `ants tools list`.
        assert_eq!(stream.get_last_raw_message_by_subject(&request.subject()).await.unwrap().sequence, 1);
        assert!(stream.get_last_raw_message_by_subject(&decision.subject()).await.is_ok());

        // Silence rejects, and the thread's owner is told before the ant goes on.
        let alice: UserId = "alice".parse().unwrap();
        let humans = Registry::with(UserId::human(), &["alice"]);
        let impatient =
            NatsApprover::new(client, js, ant.clone(), Duration::from_millis(300)).with_humans(humans.clone());
        let asked =
            Envelope::on_thread(thread.clone(), alice.clone(), ant.clone(), "push").with_owner(Some(alice.clone()));
        let turn = impatient.begin(&asked);
        assert_eq!(impatient.decide(thread.clone(), tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);
        let stored = stream.direct_get_last_for_subject(subject::inbox(&alice, &ant)).await.unwrap();
        let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
        assert_eq!((&told.from, &told.owner), (&ant, &Some(alice.clone())));
        assert_ne!(told.thread_id, thread, "on a thread of its own, for `ants ask --wait` to wait on");
        assert!(told.text.contains("expired unanswered") && told.text.contains("git push"), "{}", told.text);
        let escalation = told.escalation.as_deref().map(|e| (e.reason, &e.thread_id));
        assert_eq!(escalation, Some((EscalationReason::Approval, &thread)), "pushed as escalations are");

        // With the turn over, the default human is told.
        drop(turn);
        assert_eq!(impatient.decide(thread, tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);
        let stored = stream.direct_get_last_for_subject(subject::inbox(&UserId::human(), &ant)).await.unwrap();
        let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
        assert_eq!((&told.to, &told.owner), (&UserId::human(), &None));
    }

    #[test]
    fn an_expired_request_is_told_to_the_owner_else_the_default_human() {
        let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
        let humans = Registry::with(UserId::human(), &["alice"]);
        let thread: ThreadId = "t1".parse().unwrap();
        let tool = ToolCall { locations: vec!["src/main.rs".into()], ..tool() };
        let request =
            ApprovalRequest::new(ant.clone(), thread.clone(), tool, vec![Verdict::AllowOnce], DEFAULT_TIMEOUT);
        let asked = Envelope::on_thread(thread.clone(), lead.clone(), ant.clone(), "push")
            .with_owner(Some("alice".parse().unwrap()))
            .with_creator(Some(lead.clone()))
            .with_origin_thread(Some("t0".parse().unwrap()));
        let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, Some(&asked), &humans);
        assert_eq!((told.to.as_str(), &told.owner, &told.creator), ("alice", &asked.owner, &Some(lead)));
        assert_ne!(told.thread_id, thread);
        assert_eq!((&told.origin_thread, told.in_reply_to), (&asked.origin_thread, None));
        for part in [
            format!("Tool request dev-a/{} on thread t1 expired unanswered after 5m", request.id),
            "git push (execute)".into(),
            "Files: src/main.rs".into(),
            "`ants ask --thread t1 @dev-a TEXT`".into(),
            "approval_timeout".into(),
            "allow rule".into(),
        ] {
            assert!(told.text.contains(&part), "{part}: {}", told.text);
        }
        let by_ant = asked.with_owner(Some("dev-c".parse().unwrap()));
        let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, Some(&by_ant), &humans);
        assert_eq!((&told.to, &told.owner), (&UserId::human(), &None), "an ant owns no thread");
        let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, None, &humans);
        assert_eq!((&told.to, &told.owner, &told.creator), (&UserId::human(), &None, &None));
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
    async fn an_expired_request_is_told_to_the_chain_asked_and_pushed_to_the_human_once() {
        let Some(server) = Server::start() else { return };
        let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
        let thread: ThreadId = "t1".parse().unwrap();
        let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
        let mut stream = bus::ensure_stream(&js).await.unwrap();

        // A human in the chain, as a misconfigured escalate_to may hold, is told once, as the default human.
        let human = UserId::human();
        let approver =
            NatsApprover::new(client, js, ant.clone(), Duration::from_millis(300)).with_chain(chain(&[&lead, &human]));
        assert_eq!(approver.decide(thread.clone(), tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);

        let stored = stream.direct_get_last_for_subject(subject::inbox(&lead, &ant)).await.unwrap();
        let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
        assert_eq!((&told.thread_id, &told.escalation), (&thread, &None), "on the request's thread, nothing to decide");
        assert!(told.text.contains("expired unanswered") && told.text.contains("git push"), "{}", told.text);

        let to_human = stream.get_last_raw_message_by_subject(&subject::inbox(&human, &ant)).await.unwrap();
        let told: Envelope = serde_json::from_slice(&to_human.payload).unwrap();
        assert!(told.text.contains("expired unanswered"), "{}", told.text);
        let escalation = told.escalation.as_deref().expect("an escalation, for push sinks");
        assert_eq!((escalation.reason, &escalation.thread_id), (EscalationReason::Approval, &thread));
        let info = stream.info().await.unwrap();
        let (first, last) = (info.state.first_sequence, info.state.last_sequence);
        let mut to_human = 0;
        for seq in first..=last {
            let msg = stream.get_raw_message(seq).await.unwrap();
            to_human += usize::from(msg.subject.as_str() == subject::inbox(&human, &ant));
        }
        assert_eq!(to_human, 2, "the chain's request and the expiry notice, not one more");
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
