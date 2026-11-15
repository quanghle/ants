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
//! cannot be forged. While the stream cannot be asked, the ant waits: a decision it cannot read decides nothing.
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
//!
//! However a request is settled, by a human, an ant of the chain or its expiry, the ant tells it on the thread's status
//! as a `decided` update before it goes on: observers, which read no inbox, learn of a member's decision that way.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::mem::DropGuard;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ants_proto::{
    ApprovalDecision, ApprovalRequest, DecidedBy, Envelope, Escalation, Hop, STREAM, State, StatusUpdate, ThreadId,
    ToolCall, UserId, Uuid, Verdict,
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
use crate::sync::MutexExt as _;

/// How long a tool request waits for a human by default.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_mins(5);

/// How often the ant looks for the stored decision after hearing one that is not stored (yet).
pub const RECHECK: Duration = Duration::from_millis(250);

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
    #[must_use]
    pub fn with_chain(self, chain: Chain) -> Self {
        Self { chain, ..self }
    }

    /// The same, telling the thread's owner when a human, else the default human of `humans`, of an expired request.
    #[must_use]
    pub fn with_humans(self, humans: Registry) -> Self {
        Self { humans, ..self }
    }

    /// Note that the ant answers `envelope` on its thread until the returned guard drops: a tool request of the turn
    /// that expires unanswered is told to the owner it names, and carries its creator and the thread it links to.
    pub fn begin(&self, envelope: &Envelope) -> Turn {
        let thread = envelope.thread_id.clone();
        self.answering.locked().insert(thread.clone(), envelope.clone());
        Turn { answering: self.answering.clone(), thread }
    }

    /// Tell the thread's owner, else the default human, and the ants of the chain `asked` about it, that `request`
    /// expired unanswered and was rejected. Stored before the ant goes on, so before the message being answered is
    /// acked; failing to, it only warns.
    async fn tell_expired(&self, request: &ApprovalRequest, asked: &[UserId]) {
        let answering = self.answering.locked().get(&request.thread_id).cloned();
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
                    tracing::info!(request = %request.id, to = %told.to, "told of the expired approval request");
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
        let waiting = self.waiting.locked();
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
        let answering = self.answering.locked().get(&request.thread_id).map(|e| e.allow.clone());
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

    /// [`Self::ask_chain`], logging whom it asked or why it could not: those it asked.
    async fn ask_chain_logged(&self, request: &ApprovalRequest) -> Vec<UserId> {
        match self.ask_chain(request).await {
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
        }
    }

    async fn store(&self, request: &ApprovalRequest) -> Result<()> {
        self.js
            .publish(request.subject(), serde_json::to_vec(request)?.into())
            .await
            .context("publishing the approval request")?
            .await
            .context("waiting for JetStream to store the approval request")?;
        tracing::info!(request = %request.id, thread = %request.thread_id, title = %request.tool.title, "waiting for approval");
        Ok(())
    }

    async fn ask(&self, thread: ThreadId, tool: ToolCall, options: Vec<Verdict>) -> Result<Verdict> {
        let human = {
            let answering = self.answering.locked();
            self.humans.owner_or_default(answering.get(&thread).and_then(|e| e.owner.as_ref()))
        };
        let request = ApprovalRequest::new(self.ant.clone(), thread, tool, options, self.timeout)
            .for_human(Some(human), self.chain.stand_in.clone());
        // Subscribe first, so a quick answer is not missed.
        let mut decisions = self.client.subscribe(request.decision_subject()).await?;
        let mut inbox = if self.chain.escalate_to.is_empty() {
            None
        } else {
            Some(bus::live_inbox(&self.client, &self.ant).await?)
        };
        let (chain_decided, mut from_chain) = mpsc::unbounded_channel();
        self.waiting.locked().insert(request.id, (request.clone(), chain_decided));
        // Leaves the waiting map once decided, expired or given up.
        let _pending = DropGuard::new(request.id, |id| {
            self.waiting.locked().remove(&id);
        });
        self.store(&request).await?;
        let asked = self.ask_chain_logged(&request).await;
        let waits = StatusUpdate::Approval { request_id: request.id };
        crate::status::publish(&self.client, &self.ant, &request.thread_id, waits);
        crate::status::state(&self.client, &self.ant, &request.thread_id, State::WaitingApproval);
        // The verdict, who gave it, and the ant of the chain that did; None once nothing more can be heard.
        let wait = async {
            // Whether a decision was heard for the request, and a failure to read the stored one was warned of.
            let (mut heard, mut warned) = (false, false);
            loop {
                // Until the stream holds a decision heard, it is asked again every RECHECK.
                let recheck = heard;
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
                        Some(_) => heard = true,
                        None => {
                            tracing::warn!(subject = %msg.subject, "ignoring a malformed decision or one for another request");
                        }
                    },
                    Heard::Chain((from, decision)) => {
                        // A human's decision stored by now came first.
                        let human = self.stored(&request).await.unwrap_or_else(|e| {
                            tracing::warn!(request = %request.id, error = format!("{e:#}"), "reading the stored decision");
                            None
                        });
                        if let Some(human) = human {
                            self.tell_decided(&request, &human).await;
                            return Some((request.decide(human.verdict).verdict, DecidedBy::Human, human.by));
                        }
                        let verdict = decision.verdict.as_str();
                        tracing::info!(request = %request.id, %from, verdict, "decided by the ant's chain");
                        return Some((decision.verdict, DecidedBy::Chain, Some(from)));
                    }
                    Heard::Closed => return None,
                    Heard::Nothing => {}
                }
                if !heard {
                    continue;
                }
                let decision = match self.stored(&request).await {
                    Ok(Some(d)) => d,
                    Ok(None) => continue,
                    Err(e) => {
                        if !std::mem::replace(&mut warned, true) {
                            tracing::warn!(request = %request.id, error = format!("{e:#}"), "reading the stored decision");
                        }
                        continue;
                    }
                };
                self.tell_decided(&request, &decision).await;
                // A verdict the harness did not offer counts as a rejection.
                return Some((request.decide(decision.verdict).verdict, DecidedBy::Human, decision.by));
            }
        };
        let decided = match tokio::time::timeout(self.timeout, wait).await {
            Ok(decided) => decided,
            Err(_) => {
                tracing::info!(request = %request.id, "approval request expired; rejecting");
                self.tell_expired(&request, &asked).await;
                Some((Verdict::Reject, DecidedBy::Expiry, None))
            }
        };
        let verdict = decided.as_ref().map_or(Verdict::Reject, |(verdict, ..)| *verdict);
        if let Some((verdict, by, decider)) = decided {
            let decided = StatusUpdate::Decided { request_id: request.id, verdict, by, decider };
            crate::status::publish(&self.client, &self.ant, &request.thread_id, decided);
        }
        crate::status::state(&self.client, &self.ant, &request.thread_id, State::Working);
        Ok(verdict)
    }

    /// Tell the human `request` waited for that another human, its stand-in say, decided it ([`decided_notice`]).
    /// Failing to, it only warns.
    async fn tell_decided(&self, request: &ApprovalRequest, decision: &ApprovalDecision) {
        let Some(notice) = decided_notice(request, decision) else { return };
        let id = format!("approval-decided-{}-{}", self.ant, request.id);
        match bus::store(&self.js, notice.subject(), &notice, &id).await {
            Ok(_) => tracing::info!(request = %request.id, to = %notice.to, "told of the decision of another human"),
            Err(e) => tracing::warn!(
                request = %request.id,
                to = %notice.to,
                error = format!("{e:#}"),
                "telling of the decision of another human"
            ),
        }
    }

    /// The decision the stream stored on `request`'s decision subject: its last message, which is also its first, as
    /// `ants tools approve` and `reject` store one only while the subject holds none. None while it holds nothing that
    /// decides the request.
    async fn stored(&self, request: &ApprovalRequest) -> Result<Option<ApprovalDecision>> {
        let stream = self.js.get_stream_no_info(STREAM).await?;
        match stream.direct_get_last_for_subject(request.decision_subject()).await {
            Ok(msg) => {
                let decision = decided(request, &msg.payload);
                if decision.is_none() {
                    tracing::warn!(request = %request.id, "ignoring a stored decision, malformed or for another request");
                }
                Ok(decision)
            }
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("reading the stored decision"),
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
        self.answering.locked().remove(&self.thread);
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

/// The message that tells the human `request` waited for that the human `decision` names, another one, decided it: on
/// a thread of its own, as [`expired_notice`] is, and carrying no escalation, as nothing is left to decide. None for a
/// decision naming no human, or the one the request waited for.
fn decided_notice(request: &ApprovalRequest, decision: &ApprovalDecision) -> Option<Envelope> {
    let (Some(human), Some(by)) = (&request.human, &decision.by) else { return None };
    if by == human {
        return None;
    }
    let (ant, id, thread, tool) = (&request.ant, request.id, &request.thread_id, &request.tool);
    let text = format!(
        "[ants] {by} decided tool request {ant}/{id} on thread {thread} in place of {human}: {}. Tool: {} ({}).",
        request.decide(decision.verdict).verdict.as_str(),
        tool.title,
        tool.kind,
    );
    Some(Envelope::new(ant.clone(), human.clone(), text).with_owner(Some(human.clone())))
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
mod tests;
