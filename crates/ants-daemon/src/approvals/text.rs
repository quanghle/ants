//! The notices and texts of tool request escalation: what the chain and the assistant asked to decide a request get,
//! and what tells of one expired or decided by another human.

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{ApprovalDecision, ApprovalRequest, Envelope, Escalation, UserId, Verdict};

use crate::people::humans::Registry;

/// The message on `request`'s thread that tells its owner, else the default human of `humans`, that the request
/// expired unanswered after `timeout` and was rejected. `answering` is the message the ant was answering, naming the
/// thread's owner and creator. On a thread of its own, naming the request's, so that `ants ask --wait` on the request's
/// thread still waits for the ant's reply that follows. It carries the request's escalation, for `ants webhook serve`
/// to push it.
pub(super) fn expired_notice(
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
pub(super) fn decided_notice(request: &ApprovalRequest, decision: &ApprovalDecision) -> Option<Envelope> {
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
    Some(
        Envelope::new(ant.clone(), human.clone(), text)
            .with_owner(Some(human.clone()))
            .as_notice(ants_proto::NoticeKind::InPlace),
    )
}

/// The message on `request`'s thread that tells `member`, an ant of the chain asked to decide it, that it expired
/// unanswered after `timeout` and was rejected. It carries no escalation: there is nothing left to decide.
pub(super) fn expired_member_notice(
    ant: &UserId,
    request: &ApprovalRequest,
    timeout: Duration,
    member: &UserId,
) -> Envelope {
    let (id, thread, tool) = (request.id, &request.thread_id, &request.tool);
    let text = format!(
        "[ants] Approval request {id} on thread {thread} expired unanswered after {} and was rejected: {} ({}). \
         {ant} goes on without it; decide_approval no longer applies to it, and no answer is needed.",
        ants_proto::duration::format(timeout),
        tool.title,
        tool.kind,
    );
    Envelope::on_thread(thread.clone(), ant.clone(), member.clone(), text)
        .as_notice(ants_proto::NoticeKind::RequestExpired)
}

/// What every ant of the chain, `to`, gets about `request`: how to decide it, and the tool call.
pub(super) fn chain_text(request: &ApprovalRequest, to: &[UserId]) -> String {
    let verdicts: Vec<&str> = request.options.iter().map(|v| v.as_str()).collect();
    let to: Vec<&str> = to.iter().map(UserId::as_str).collect();
    let (ant, id, thread) = (&request.ant, request.id, &request.thread_id);
    let mut text = format!(
        "[ants] Approval request {id} on thread {thread}: {ant} asks to run a tool outside its allowlist. Sent to \
         {}; any of them decides with the decide_approval tool ({}). Humans may decide too, with `ants tools \
         approve {ant}/{id}` or `ants tools reject {ant}/{id}`; the first decision wins. Without one by {} the \
         request is rejected.",
        to.join(", "),
        verdicts.join(", "),
        request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    );
    tool_lines(&mut text, request);
    text
}

/// What the assistant of `human`, the human `request` waits for, gets about it: how to decide it for the human, and
/// the tool call.
pub(super) fn assistant_text(request: &ApprovalRequest, assistant: &UserId, human: &UserId) -> String {
    let verdicts: Vec<&str> = request.options.iter().map(|v| v.as_str()).collect();
    let (ant, id, thread) = (&request.ant, request.id, &request.thread_id);
    let mut text = format!(
        "[ants] Tool request {ant}/{id} on thread {thread}: {ant} asks to run a tool outside its allowlist and waits \
         for {human}, who has the request too; it comes to {assistant}, the assistant of {human}, first. Decide it \
         for {human} with decide_for_human {ant}/{id} ({}), which takes effect once {human} confirms it or a \
         pre-allow rule covers it, or leave it to {human}. Without a decision by {} the request is rejected.",
        verdicts.join(", "),
        request.expires_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    );
    tool_lines(&mut text, request);
    text
}

/// Add to `text` the tool call of `request`.
fn tool_lines(text: &mut String, request: &ApprovalRequest) {
    let _ = write!(text, "\n\nTool: {} ({})", request.tool.title, request.tool.kind);
    if !request.tool.locations.is_empty() {
        let _ = write!(text, "\nFiles: {}", request.tool.locations.join(", "));
    }
    if let Some(input) = &request.tool.input {
        let _ = write!(text, "\nInput: {input}");
    }
    if let Some(always) = request.label(Verdict::AllowAlways) {
        let _ = write!(text, "\nallow_always: {always}");
    }
}
