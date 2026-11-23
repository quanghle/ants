//! Documents written elsewhere read as wire types: the examples of docs/hooks.md, and what newer versions may add.

use super::*;

/// The documents of docs/hooks.md, with its elisions filled in.
#[test]
fn hooks_doc_examples_parse() {
    let request: ApprovalRequest = parse(
        r#"{"id": "019a7d2e-0000-7000-8000-00000000000a", "ant": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001",
            "tool": {"kind": "execute", "title": "rm -rf target", "locations": ["/src/app"],
                     "input": "{\"command\":\"rm -rf target\"}"},
            "options": ["allow_once", "allow_always", "reject"],
            "labels": {"allow_always": "Yes, and don't ask again for rm commands in /src/app"},
            "requested_at": "2025-01-01T12:00:00Z", "expires_at": "2025-01-01T12:05:00Z"}"#,
    );
    assert_eq!(request.tool, tool_call());
    assert_eq!(request.label(Verdict::AllowAlways), Some("Yes, and don't ask again for rm commands in /src/app"));
    let approval: ApprovalDecision = parse(
        r#"{"request_id": "019a7d2e-0000-7000-8000-00000000000a", "ant": "dev-a", "verdict": "allow_once",
            "decided_at": "2025-01-01T12:01:00Z"}"#,
    );
    assert_eq!(approval, decision());
    let dead: DeadLetter = parse(
        r#"{"ant": "dev-a", "stream_seq": 4211, "deliveries": 6, "error": "harness exited", "died_at": "2025-01-01T12:00:00Z",
            "envelope": {"id": "019a7d2e-0000-7000-8000-000000000001", "thread_id": "019a7d2c-0000-7000-8000-000000000001",
                         "from": "alice", "to": "dev-a", "text": "fix the build", "turn_budget": 8,
                         "sent_at": "2025-01-01T12:00:00Z"}}"#,
    );
    assert_eq!(dead.envelope, Some(plain_envelope()));
    let escalated: StatusEvent = parse(
        r#"{"ant": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "type": "escalated", "to": "lead",
            "escalation": {"stuck": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "reason": "turn_budget",
                           "level": 1, "leader": "lead", "visited": ["dev-a"]},
            "at": "2025-01-01T12:00:00Z"}"#,
    );
    let StatusUpdate::Escalated { to, escalation } = escalated.update else { panic!("not an escalated update") };
    assert_eq!((to, escalation.reason, escalation.request_id), (user("lead"), EscalationReason::TurnBudget, None));
    let decided: StatusEvent = parse(
        r#"{"ant": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "type": "decided",
            "request_id": "019a7d2e-0000-7000-8000-00000000000a", "verdict": "reject", "by": "human",
            "at": "2025-01-01T12:00:00Z"}"#,
    );
    let StatusUpdate::Decided { verdict, by, decider, .. } = decided.update else { panic!("not a decided update") };
    assert_eq!((verdict, by, decider), (Verdict::Reject, DecidedBy::Human, None));
}

/// What newer versions may add: unknown fields are ignored, and unknown values of the open enumerations read as
/// `unknown`.
#[test]
fn newer_documents_parse() {
    let colony: ColonyEntry = parse(
        r#"{"ant": "dev-a", "machine": "studio.local", "instance": "019a7d2b-0000-7000-8000-000000000001",
            "harness": "copilot", "capabilities": [], "started_at": "2025-01-01T12:00:00Z",
            "heartbeat_at": "2025-01-01T12:01:00Z", "activity": "napping", "handling": 0, "harnesses": 0,
            "version": "9.0.0", "perms": 9, "mood": "great",
            "limited": {"kind": "solar_flare", "since": "2025-01-01T12:00:00Z", "until": "2025-01-01T12:05:00Z"}}"#,
    );
    assert_eq!(colony.activity, Activity::Unknown);
    assert_eq!(colony.limited.map(|l| l.kind), Some(LimitKind::Unknown));
    let escalation: Escalation = parse(
        r#"{"stuck": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "reason": "moon_phase", "level": 0,
            "leader": "dev-a", "visited": ["dev-a"], "urgency": "high"}"#,
    );
    assert_eq!(escalation.reason, EscalationReason::Unknown);
}
