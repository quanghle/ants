//! `escalate`, sending the thread up its origin chain when the ant is stuck on it (see [`crate::escalation`]), and
//! `decide_approval`, answering the tool request of another ant that came up the chain (see [`crate::approvals`]).

use ants_proto::{ApprovalDecision, Envelope, EscalationReason, ThreadId, UserId, Verdict};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::tools::{Tools, text};
use crate::escalation::{self, Escalated, Stuck};
use crate::sync::MutexExt as _;

#[derive(Deserialize, schemars::JsonSchema)]
struct Escalate {
    /// What blocks the thread and what decision or help it needs.
    issue: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct DecideApproval {
    /// `allow_once`, `allow_always` or `reject`.
    verdict: String,
}

impl Hub {
    /// Escalate `thread` for `issue`: up its origin chain or, for the leader of the escalation group the thread's
    /// message is from, one level above that escalation. Returns where it went.
    async fn escalate(&self, thread: &ThreadId, issue: String) -> Result<String, String> {
        let current = self.sessions.locked().current.get(thread).cloned();
        let current =
            current.ok_or("no message is being handled on this thread; escalations go out only while one is")?;
        if let Some(e) = current.escalation.as_deref().filter(|e| e.reason == EscalationReason::Approval)
            && e.stuck != self.ant
        {
            return Err(format!(
                "the message being handled is {}'s approval request, which goes no higher: decide it with \
                 decide_approval (reject when unsure), or leave it to {}'s owner",
                e.stuck, e.stuck
            ));
        }
        if let Some(e) = escalation::group(&self.ant, &current)
            && !e.is_leader(&self.ant)
        {
            let (leader, stuck) = (&e.leader, &e.stuck);
            return Err(format!(
                "only {leader} escalates the escalation of {stuck}'s thread {thread}; post what blocks it to the \
                 thread notes (post_notes) for {leader} to weigh"
            ));
        }
        let human = self.humans.owner_of(&current);
        let text =
            format!("{} asks for help: {issue}\n\nLast message from {}:\n{}", self.ant, current.from, current.text);
        let mut notice = current.follow_up(self.ant.clone(), human.clone(), text);
        notice.in_reply_to = Some(current.id);
        let Some(escalator) = &self.escalator else {
            self.send(&notice).await?;
            return Ok(format!("Escalated thread {thread} to {human}, the thread's owner: no escalation chain here."));
        };
        // From the message being handled, so asking again while on it sends nothing twice.
        let id = format!("asked-{}-{}", self.ant, current.id);
        let stuck = Stuck::new(EscalationReason::Asked, notice, id, Some(&current));
        let (escalated, why) =
            escalator.escalate_explained(stuck).await.map_err(|e| format!("not escalated: {e:#}"))?;
        tracing::info!(%thread, %escalated, "asked for help; escalated");
        Ok(match (escalated, why) {
            (Escalated::Chain { level, leader, to }, _) => {
                let to = to.iter().map(UserId::as_str).collect::<Vec<_>>().join(", ");
                format!(
                    "Escalated thread {thread} to level {level}: sent to {to}; {leader} leads and answers on this \
                     thread. The members coordinate in its thread notes (read_notes)."
                )
            }
            (Escalated::Owner(human), Some(why)) => format!("Escalated thread {thread} to its owner {human}: {why}"),
            (Escalated::Owner(human), None) => format!("Escalated thread {thread} to its owner {human}."),
        })
    }

    /// Send the ant's `verdict` on the approval request that the message `thread` is handling escalates, to the
    /// ant that made it, on its inbox.
    async fn decide_approval(&self, thread: &ThreadId, verdict: &str) -> Result<String, String> {
        let verdict: Verdict = serde_json::from_value(serde_json::Value::String(verdict.trim().replace('-', "_")))
            .map_err(|_| format!("unknown verdict {verdict}: allow_once, allow_always or reject"))?;
        let current = self.sessions.locked().current.get(thread).cloned();
        let current =
            current.ok_or("no message is being handled on this thread; decisions go out only while one is")?;
        let request = current.escalation.as_deref().filter(|e| e.reason == EscalationReason::Approval);
        let (escalation, id) = request
            .and_then(|e| Some((e, e.request_id?)))
            .ok_or("the message being handled is no approval request; decide_approval answers one sent up the chain")?;
        if escalation.stuck == self.ant {
            return Err("the approval request is this ant's own; its chain or a human decides it".to_owned());
        }
        let decision = ApprovalDecision::new(id, escalation.stuck.clone(), verdict);
        let envelope = Envelope::decision(escalation.thread_id.clone(), self.ant.clone(), decision)
            .with_owner(current.owner.clone())
            .with_creator(current.creator.clone());
        self.send(&envelope).await?;
        Ok(format!(
            "Sent {} on approval request {id} to {}; the first decision wins, a human's included.",
            verdict.as_str(),
            escalation.stuck
        ))
    }
}

#[tool_router(router = escalation_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Escalate the current thread when stuck on it: `issue` says what blocks it and what is \
        needed. It goes up the thread's origin chain one level at a time: the escalation chain of the ant that \
        created the thread with a channel post, else this ant's, to the escalate_to ants (the first leads and \
        answers on the thread), and to the thread's owner when no chain is left. On an escalation \
        group's message only its leader escalates, one level higher; the other members post to the thread notes. \
        Returns where it went.")]
    async fn escalate(
        &self,
        Parameters(args): Parameters<Escalate>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.escalate(&thread, args.issue).await))
    }

    #[tool(description = "Decide the approval request that the message being handled brings up the chain: another \
        ant asks to run a tool outside its allowlist, shown in the message. `verdict` is allow_once, allow_always \
        or reject, and goes to that ant; the first decision wins, a human's included, and none before the \
        request expires rejects it.")]
    async fn decide_approval(
        &self,
        Parameters(args): Parameters<DecideApproval>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.decide_approval(&thread, &args.verdict).await))
    }
}
