//! Golden snapshots of the JSON form of every ants-proto wire type, pinning the current wire form.
//!
//! Peers share these documents, so renaming or dropping a field, or a variant, is a wire change even when every round
//! trip still passes. Each value is fully populated, every optional field set, so each field name is pinned. A changed
//! snapshot is a wire change: review it with `cargo insta review`.

#![allow(clippy::too_many_lines)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use ants_proto::*;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Snapshot `value` as `name`, and check it reads back as itself.
fn snap<T: Serialize + DeserializeOwned + PartialEq + Debug>(name: &str, value: &T) {
    insta::assert_json_snapshot!(name, value);
    let json = serde_json::to_string(value).expect("serializes");
    let back: T = serde_json::from_str(&json).unwrap_or_else(|e| panic!("{name} does not read back: {e}\n{json}"));
    assert_eq!(&back, value, "{name} reads back as another value");
}

fn parse<T: DeserializeOwned>(json: &str) -> T {
    serde_json::from_str(json).unwrap_or_else(|e| panic!("does not parse: {e}\n{json}"))
}

fn id(n: u128) -> Uuid {
    Uuid::from_u128(0x019a7d2e_0000_7000_8000_000000000000 | n)
}

fn user(name: &str) -> UserId {
    name.parse().unwrap()
}

fn thread() -> ThreadId {
    "019a7d2c-0000-7000-8000-000000000001".parse().unwrap()
}

fn other_thread() -> ThreadId {
    "019a7d2c-0000-7000-8000-000000000002".parse().unwrap()
}

fn channel() -> ChannelId {
    "backend".parse().unwrap()
}

fn machine() -> Machine {
    "studio".parse().unwrap()
}

fn device() -> DeviceId {
    "019a7d2a-0000-7000-8000-000000000001".parse().unwrap()
}

fn at(minute: u32) -> DateTime<Utc> {
    format!("2025-01-01T12:{minute:02}:00Z").parse().unwrap()
}

fn attachment() -> Attachment {
    Attachment {
        id: id(20),
        name: "build log.txt".to_owned(),
        mime: "text/plain".to_owned(),
        size: 1234,
        sha256: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".to_owned(),
    }
}

fn escalation() -> Escalation {
    Escalation {
        stuck: user("dev-a"),
        thread_id: thread(),
        reason: EscalationReason::Approval,
        request_id: Some(id(10)),
        level: 1,
        leader: user("lead"),
        visited: vec![user("dev-a"), user("lead")],
        stand_in: None,
        away: None,
    }
}

fn decision() -> ApprovalDecision {
    ApprovalDecision {
        request_id: id(10),
        ant: user("dev-a"),
        verdict: Verdict::AllowOnce,
        decided_at: at(1),
        by: None,
    }
}

fn mention() -> Mention {
    Mention { channel: channel(), post: id(3), author: Some(user("github")) }
}

fn envelope() -> Envelope {
    Envelope {
        id: id(1),
        thread_id: thread(),
        from: user("alice"),
        to: user("dev-a"),
        text: "fix the build".to_owned(),
        turn_budget: 8,
        in_reply_to: Some(id(2)),
        owner: Some(user("alice")),
        creator: Some(user("planner")),
        origin_thread: Some(other_thread()),
        escalation: Some(Box::new(escalation())),
        approval: Some(Box::new(decision())),
        webhook: true,
        allow: Some(vec!["read:*".to_owned(), "other:send_message".to_owned()]),
        mention: Some(mention()),
        title: Some("Fix the build".to_owned()),
        question: Some(Question {
            text: "Ship the fix?".to_owned(),
            options: vec!["yes".to_owned(), "not yet".to_owned()],
            free_text: false,
        }),
        answer: Some(Answer { question: id(4), option: Some(2) }),
        steer: true,
        attachments: vec![attachment()],
        sent_at: at(0),
    }
}

/// [`envelope`] with none of its optional fields: a plain message.
fn plain_envelope() -> Envelope {
    Envelope {
        in_reply_to: None,
        owner: None,
        creator: None,
        origin_thread: None,
        escalation: None,
        approval: None,
        webhook: false,
        allow: None,
        mention: None,
        title: None,
        question: None,
        answer: None,
        steer: false,
        attachments: Vec::new(),
        ..envelope()
    }
}

fn tool_call() -> ToolCall {
    ToolCall {
        kind: "execute".to_owned(),
        title: "rm -rf target".to_owned(),
        locations: vec!["/src/app".to_owned()],
        input: Some(r#"{"command":"rm -rf target"}"#.to_owned()),
    }
}

fn channel_entry() -> ChannelEntry {
    ChannelEntry {
        channel: channel(),
        members: vec![user("dev-a"), user("alice")],
        messenger: Some(user("triage")),
        owner: Some(user("lead")),
    }
}

fn enroll_leaf() -> EnrollLeaf {
    EnrollLeaf { machine: machine(), jwt: "eyJ0eXAiOiJKV1QifQ.leaf".to_owned(), signature: "c2lnLWxlYWY".to_owned() }
}

#[test]
fn identifiers() {
    snap(
        "identifiers",
        &(
            user("dev-a"),
            thread(),
            channel(),
            "eyes".parse::<Reaction>().unwrap(),
            "hub".parse::<JsDomain>().unwrap(),
            machine(),
            device(),
            PermissionsVersion(1),
        ),
    );
}

#[test]
fn serving() {
    snap(
        "serving",
        &Serving { machine: "studio.local".to_owned(), version: "0.1.0".to_owned(), perms: PermissionsVersion(1) },
    );
}

#[test]
fn roster() {
    snap(
        "roster_entry",
        &RosterEntry {
            ant: user("dev-a"),
            machine: "studio.local".to_owned(),
            instance: "019a7d2b-0000-7000-8000-000000000001".to_owned(),
            harness: "copilot".to_owned(),
            agent: Some(HarnessAgent { name: "copilot".to_owned(), version: "1.2.3".to_owned() }),
            capabilities: vec!["rust".to_owned(), "frontend".to_owned()],
            whoami: Some("Backend developer".to_owned()),
            workdir: Some("/src/app".to_owned()),
            started_at: at(0),
            heartbeat_at: at(1),
            activity: Activity::Working,
            handling: 1,
            harnesses: 2,
            escalate_to: vec![user("lead"), user("cto")],
            version: "0.1.0".to_owned(),
            perms: PermissionsVersion(1),
            signed_out: Some(SignInNeeded { since: at(2), sign_in: Some("copilot login".to_owned()) }),
            limited: Some(HarnessLimit { kind: LimitKind::RateLimit, since: at(3), until: at(8) }),
            failing: Some(HarnessFailing { since: at(4), error: "harness exited".to_owned() }),
            fallback: Some(ServingFallback { name: "claude:sonnet".to_owned(), since: at(5) }),
            memory_low: Some(MemoryLow { since: at(5), available: 805_306_368, max_harnesses: 1 }),
        },
    );
    snap("activity_variants", &[Activity::Idle, Activity::Working, Activity::Unknown]);
    snap("limit_kind_variants", &[LimitKind::RateLimit, LimitKind::Quota, LimitKind::Unknown]);
}

#[test]
fn kv_entries() {
    snap("chain_entry", &ChainEntry { ant: user("dev-a"), escalate_to: vec![user("lead")], updated_at: at(0) });
    snap("human_entry", &HumanEntry { human: user("alice"), registered_at: Some(at(0)) });
    snap(
        "reaction_entry",
        &ReactionEntry { message: id(1), by: user("alice"), reaction: "eyes".parse().unwrap(), at: at(0) },
    );
    let request = ReactionsRequest { id: id(1), ant: user("dev-a"), message: id(2), requested_at: at(0), change: None };
    snap("reactions_request", &request);
    for (kind, remove) in [("react", false), ("unreact", true)] {
        let change = Some(ReactionChange { reaction: "eyes".parse().unwrap(), remove });
        snap(&format!("reactions_request_{kind}"), &ReactionsRequest { change, ..request.clone() });
    }
    let reacted = Reacted { reaction: "eyes".parse().unwrap(), by: vec![user("alice"), user("dev-b")] };
    for (kind, outcome) in [
        ("listed", ReactionsOutcome::Listed { reactions: vec![reacted] }),
        ("applied", ReactionsOutcome::Applied { changed: true }),
        ("unknown", ReactionsOutcome::Unknown),
    ] {
        let reply = ReactionsReply { request_id: id(1), ant: user("dev-a"), message: id(2), outcome };
        snap(&format!("reactions_reply_{kind}"), &reply);
    }
    snap("read_marker", &ReadMarker { below: 4200, read: BTreeSet::from([4203, 4207]) });
    snap(
        "thread_title",
        &ThreadTitle { thread_id: thread(), title: "Release 0.2".to_owned(), by: user("alice"), at: at(0) },
    );
    snap("thread_done", &ThreadDone { thread_id: thread(), by: user("alice"), at: at(0), after: 4211 });
    snap(
        "thread_marks",
        &ThreadMarks {
            pinned: BTreeMap::from([(thread(), at(0))]),
            archived: BTreeMap::from([(other_thread(), Archived { at: at(1), after: 4211 })]),
            kept: BTreeMap::from([(thread(), at(2))]),
        },
    );
    snap("notify_prefs", &NotifyPrefs { snoozed_until: Some(at(30)), muted: BTreeMap::from([(thread(), at(45))]) });
    snap(
        "held_notices",
        &HeldNotices {
            since: at(0),
            counts: BTreeMap::from([("approval".to_owned(), 2), ("completion".to_owned(), 1)]),
        },
    );
    snap("following_variants", &[Following::Inbox, Following::Approvals]);
    snap("presence", &Presence { human: user("alice"), device: device(), following: Following::Inbox, since: at(0) });
}

#[test]
fn envelopes() {
    snap("envelope_full", &envelope());
    let message = plain_envelope();
    snap("envelope_message", &message);
    snap(
        "envelope_reply",
        &Envelope {
            from: user("dev-a"),
            to: user("alice"),
            in_reply_to: Some(id(1)),
            owner: Some(user("alice")),
            ..message.clone()
        },
    );
    snap(
        "envelope_escalation",
        &Envelope {
            from: user("dev-a"),
            to: user("lead"),
            escalation: Some(Box::new(escalation())),
            owner: Some(user("alice")),
            ..message.clone()
        },
    );
    snap(
        "envelope_decision",
        &Envelope { from: user("lead"), to: user("dev-a"), approval: Some(Box::new(decision())), ..message.clone() },
    );
    snap(
        "envelope_webhook",
        &Envelope {
            from: user("github"),
            webhook: true,
            owner: Some(user("alice")),
            allow: Some(WEBHOOK_ALLOW.iter().map(|rule| (*rule).to_owned()).collect()),
            ..message.clone()
        },
    );
    snap("envelope_steer", &Envelope { owner: Some(user("alice")), steer: true, ..message.clone() });
    snap(
        "envelope_mention_notice",
        &Envelope { from: user("dev-b"), to: user("alice"), mention: Some(mention()), ..message },
    );
    snap("mention", &mention());
}

#[test]
fn escalations() {
    snap("escalation", &escalation());
    let relayed = Escalation {
        stand_in: Some(user("bob")),
        away: Some(Away { human: user("alice"), ant: user("lead") }),
        ..escalation()
    };
    snap("escalation_relayed", &relayed);
    snap(
        "escalation_reason_variants",
        &[
            EscalationReason::TurnBudget,
            EscalationReason::Cap,
            EscalationReason::Notes,
            EscalationReason::DeadLetter,
            EscalationReason::Approval,
            EscalationReason::Asked,
            EscalationReason::ContextWindow,
            EscalationReason::Harness,
            EscalationReason::Storage,
            EscalationReason::Unknown,
        ],
    );
}

#[test]
fn notes() {
    snap(
        "notes_post",
        &NotesPost {
            id: id(1),
            thread_id: thread(),
            from: user("dev-a"),
            text: "taking the parser".to_owned(),
            turn_budget: 7,
            title: Some("Rewrite the parser".to_owned()),
            attachments: vec![attachment()],
            sent_at: at(0),
        },
    );
}

#[test]
fn channels() {
    snap("channel_entry", &channel_entry());
    let ops = vec![
        ChannelOp::AddMember { ant: user("dev-b") },
        ChannelOp::RemoveMember { ant: user("dev-c") },
        ChannelOp::SetMessenger { ant: user("triage") },
        ChannelOp::ClearMessenger,
    ];
    snap("channel_op_variants", &ops);
    snap(
        "channel_admin_request",
        &ChannelAdminRequest {
            id: id(1),
            channel: channel(),
            ant: user("lead"),
            thread_id: thread(),
            ops,
            requested_at: at(0),
            expires_at: at(1),
        },
    );
    snap(
        "channel_admin_reply_applied",
        &ChannelAdminReply {
            request_id: id(1),
            outcome: ChannelAdminOutcome::Applied {
                entry: channel_entry(),
                revision: 12,
                changed: vec!["added dev-b".to_owned()],
            },
        },
    );
    snap(
        "channel_admin_reply_refused",
        &ChannelAdminReply {
            request_id: id(1),
            outcome: ChannelAdminOutcome::Refused { refusal: Refusal::NotOwner { owner: Some(user("lead")) } },
        },
    );
    snap(
        "refusal_variants",
        &[
            Refusal::NotOwner { owner: Some(user("lead")) },
            Refusal::NoSuchChannel,
            Refusal::Unreadable,
            Refusal::Human(user("alice")),
            Refusal::NotInRoster(user("ghost")),
            Refusal::Expired,
            Refusal::Busy,
            Refusal::Invalid("no change asked for".to_owned()),
        ],
    );
    snap(
        "channel_post",
        &ChannelPost {
            id: id(2),
            channel: channel(),
            thread_id: thread(),
            from: user("github"),
            text: "@dev-a CI failed".to_owned(),
            mentions: vec![user("dev-a")],
            turn_budget: 8,
            in_reply_to: Some(id(1)),
            owner: Some(user("alice")),
            creator: Some(user("planner")),
            webhook: true,
            allow: Some(vec!["read:*".to_owned()]),
            title: Some("Fix CI".to_owned()),
            attachments: vec![attachment()],
            sent_at: at(0),
        },
    );
}

#[test]
fn dead_letters() {
    snap(
        "dead_letter_full",
        &DeadLetter {
            ant: user("dev-a"),
            stream_seq: 4211,
            deliveries: 6,
            error: "harness exited".to_owned(),
            died_at: at(0),
            envelope: Some(envelope()),
            payload: Some("{\"id\":".to_owned()),
        },
    );
}

#[test]
fn approvals() {
    snap("verdict_variants", &[Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);
    snap("tool_call", &tool_call());
    let request = ApprovalRequest {
        id: id(10),
        ant: user("dev-a"),
        thread_id: thread(),
        tool: tool_call(),
        options: vec![Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject],
        requested_at: at(0),
        expires_at: at(5),
        human: None,
        stand_in: None,
    };
    snap("approval_request", &request);
    snap("approval_request_for_human", &request.for_human(Some(user("alice")), Some(user("bob"))));
    snap("approval_decision", &decision());
    snap("approval_decision_by", &decision().by(Some(user("bob"))));
}

#[test]
fn enrollment() {
    snap(
        "enroll_request",
        &EnrollRequest {
            ant: user("dev-a"),
            public_key: "UABC".to_owned(),
            host: "laptop.local".to_owned(),
            sent_at: at(0),
            enrollment: "eyJ0eXAiOiJKV1QifQ.enroll".to_owned(),
            signature: "c2lnLWVucm9sbA".to_owned(),
            leaf: Some(enroll_leaf()),
        },
    );
    snap("enroll_leaf", &enroll_leaf());
    snap(
        "enroll_reply_variants",
        &[
            EnrollReply::Approved {
                ant: user("dev-a"),
                public_key: "UABC".to_owned(),
                jwt: "eyJ0eXAiOiJKV1QifQ.user".to_owned(),
                leaf_jwt: Some("eyJ0eXAiOiJKV1QifQ.leaf".to_owned()),
            },
            EnrollReply::Rejected {
                ant: user("dev-a"),
                public_key: Some("UABC".to_owned()),
                reason: Some("unknown machine".to_owned()),
            },
        ],
    );
    snap(
        "renew_request",
        &RenewRequest {
            name: "dev-a".to_owned(),
            public_key: "UDEF".to_owned(),
            host: "laptop.local".to_owned(),
            sent_at: at(0),
            current: "eyJ0eXAiOiJKV1QifQ.current".to_owned(),
            xkey: None,
            signature: "c2lnLXJlbmV3".to_owned(),
        },
    );
    snap(
        "renew_request_xkey",
        &RenewRequest {
            name: "alice".to_owned(),
            public_key: "UDEF".to_owned(),
            host: "laptop.local".to_owned(),
            sent_at: at(0),
            current: "eyJ0eXAiOiJKV1QifQ.current".to_owned(),
            xkey: Some("XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap()),
            signature: "c2lnLXJlbmV3".to_owned(),
        },
    );
    snap("renew_reply", &RenewReply::signed("dev-a", "UDEF", "eyJ0eXAiOiJKV1QifQ.renewed".to_owned()));
    snap(
        "renew_reply_refused",
        &RenewReply::refused("alice", "UDEF", "GitHub user octocat is no member of acme any more".to_owned()),
    );
}

#[test]
fn login() {
    snap(
        "login_request",
        &LoginRequest {
            name: user("alice"),
            public_key: "UDEF".to_owned(),
            reply_xkey: "XREPLY".to_owned(),
            xkey: Some("XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap()),
            label: Some("Alice's laptop".to_owned()),
            made_at: at(0),
            expires_at: at(15),
            signature: "c2lnLWxvZ2lu".to_owned(),
        },
    );
    snap(
        "login_reply",
        &LoginReply {
            name: user("alice"),
            public_key: "UDEF".to_owned(),
            sender: "XSIGNER".to_owned(),
            sealed: "c2VhbGVk".to_owned(),
        },
    );
    snap(
        "login_answer_variants",
        &vec![
            LoginAnswer::Verify {
                url: "https://github.com/login/device".to_owned(),
                user_code: "WDJB-MJHT".to_owned(),
                expires_at: at(15),
            },
            LoginAnswer::Waiting { login: "octocat".to_owned(), reason: "outside the registration policy".to_owned() },
            LoginAnswer::Refused { reason: "alice is bound to another GitHub user".to_owned() },
            LoginAnswer::Signed { jwt: "eyJ0eXAiOiJKV1QifQ.alice".to_owned() },
        ],
    );
}

#[test]
fn ant_requests() {
    snap(
        "ant_request",
        &AntRequest {
            manager: user("lead"),
            name: user("dev-c"),
            template: "rust-dev".to_owned(),
            request: r#"{"whoami":"Parser work"}"#.to_owned(),
            reason: "the parser needs a hand".to_owned(),
            thread_id: thread(),
            public_key: "UGHI".to_owned(),
            host: "studio.local".to_owned(),
            sent_at: at(0),
            expires_at: at(10),
            credentials: "eyJ0eXAiOiJKV1QifQ.manager".to_owned(),
            signature: "c2lnLWFudA".to_owned(),
            leaf: Some(enroll_leaf()),
            retire: true,
        },
    );
    snap(
        "ant_reply_variants",
        &[
            AntReply::Approved {
                manager: user("lead"),
                name: user("dev-c"),
                public_key: "UGHI".to_owned(),
                jwt: "eyJ0eXAiOiJKV1QifQ.user".to_owned(),
                leaf_jwt: Some("eyJ0eXAiOiJKV1QifQ.leaf".to_owned()),
            },
            AntReply::Refused {
                manager: user("lead"),
                name: user("dev-c"),
                public_key: "UGHI".to_owned(),
                reason: "outside the template".to_owned(),
            },
            AntReply::Retired {
                manager: user("lead"),
                name: user("dev-c"),
                public_key: "UGHI".to_owned(),
                revoked: vec!["UGHI".to_owned()],
                push_error: Some("hub unreachable".to_owned()),
            },
        ],
    );
}

#[test]
fn status() {
    snap("state_variants", &[State::Working, State::WaitingApproval, State::Idle, State::Failed, State::SignedOut]);
    snap("decided_by_variants", &[DecidedBy::Human, DecidedBy::Chain, DecidedBy::Expiry]);
    snap("plan_step", &PlanStep { content: "write tests".to_owned(), status: "in_progress".to_owned() });
    let updates = [
        ("state", StatusUpdate::State { state: State::WaitingApproval }),
        ("message", StatusUpdate::Message { text: "Done: ".to_owned() }),
        ("thought", StatusUpdate::Thought { text: "The parser".to_owned() }),
        (
            "tool",
            StatusUpdate::Tool {
                id: "call-1".to_owned(),
                title: Some("cargo test".to_owned()),
                tool_kind: Some("execute".to_owned()),
                status: Some("in_progress".to_owned()),
                paths: Some(vec!["crates/ants/src/main.rs".to_owned()]),
            },
        ),
        (
            "plan",
            StatusUpdate::Plan {
                steps: vec![
                    PlanStep { content: "read".to_owned(), status: "completed".to_owned() },
                    PlanStep { content: "fix".to_owned(), status: "pending".to_owned() },
                ],
            },
        ),
        ("queued", StatusUpdate::Queued { message: id(1), ahead: 2 }),
        ("approval", StatusUpdate::Approval { request_id: id(10) }),
        ("escalated", StatusUpdate::Escalated { to: user("lead"), escalation: escalation() }),
        (
            "decided",
            StatusUpdate::Decided {
                request_id: id(10),
                verdict: Verdict::AllowOnce,
                by: DecidedBy::Chain,
                decider: Some(user("lead")),
            },
        ),
    ];
    for (kind, update) in updates {
        let event = StatusEvent { ant: user("dev-a"), thread_id: thread(), update, at: at(0) };
        snap(&format!("status_event_{kind}"), &event);
    }
}

#[test]
fn timeline() {
    let call = TimelineCall {
        id: "call-1".to_owned(),
        tool_kind: Some("execute".to_owned()),
        title: "cargo test".to_owned(),
        status: Some("completed".to_owned()),
        started: at(0),
        duration_ms: Some(2100),
        paths: vec!["crates/ants/src/main.rs".to_owned()],
    };
    let plan =
        TimelineEntry::Plan { steps: vec![PlanStep { content: "fix".to_owned(), status: "in_progress".to_owned() }] };
    let record = Timeline {
        ant: user("dev-a"),
        thread_id: thread(),
        message: id(1),
        entries: vec![TimelineEntry::Tool(call), plan],
        omitted: 3,
        at: at(1),
    };
    snap("timeline", &record);
}

#[test]
fn cancel_and_queue() {
    snap(
        "cancel_request",
        &CancelRequest { id: id(1), ant: user("dev-a"), thread_id: thread(), by: user("alice"), sent_at: at(0) },
    );
    snap("cancel_reply", &CancelReply { request_id: id(1), ant: user("dev-a"), thread_id: thread(), cancelled: true });
    snap("queue_request", &QueueRequest { id: id(1), ant: user("dev-a"), by: user("alice"), sent_at: at(0) });
    let queued = QueuedMessage {
        id: id(2),
        thread_id: thread(),
        from: user("alice"),
        channel: Some(channel()),
        sent_at: at(0),
        seq: 4211,
        running: true,
        ahead: 0,
    };
    snap("queued_message", &queued);
    snap(
        "queue_reply",
        &QueueReply {
            request_id: id(1),
            ant: user("dev-a"),
            messages: vec![queued],
            delivered: Some(4211),
            undelivered: Some(3),
        },
    );
    snap(
        "message_mark_variants",
        &[
            MessageMark::Started { thread_id: thread(), at: at(0) },
            MessageMark::Finished { thread_id: thread(), at: at(1) },
            MessageMark::Withdrawn { by: user("alice"), at: at(2) },
        ],
    );
}

/// The documents of docs/hooks.md, with its elisions filled in.
#[test]
fn hooks_doc_examples_parse() {
    let request: ApprovalRequest = parse(
        r#"{"id": "019a7d2e-0000-7000-8000-00000000000a", "ant": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001",
            "tool": {"kind": "execute", "title": "rm -rf target", "locations": ["/src/app"],
                     "input": "{\"command\":\"rm -rf target\"}"},
            "options": ["allow_once", "allow_always", "reject"],
            "requested_at": "2025-01-01T12:00:00Z", "expires_at": "2025-01-01T12:05:00Z"}"#,
    );
    assert_eq!(request.tool, tool_call());
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
    let roster: RosterEntry = parse(
        r#"{"ant": "dev-a", "machine": "studio.local", "instance": "019a7d2b-0000-7000-8000-000000000001",
            "harness": "copilot", "capabilities": [], "started_at": "2025-01-01T12:00:00Z",
            "heartbeat_at": "2025-01-01T12:01:00Z", "activity": "napping", "handling": 0, "harnesses": 0,
            "version": "9.0.0", "perms": 9, "mood": "great",
            "limited": {"kind": "solar_flare", "since": "2025-01-01T12:00:00Z", "until": "2025-01-01T12:05:00Z"}}"#,
    );
    assert_eq!(roster.activity, Activity::Unknown);
    assert_eq!(roster.limited.map(|l| l.kind), Some(LimitKind::Unknown));
    let escalation: Escalation = parse(
        r#"{"stuck": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "reason": "moon_phase", "level": 0,
            "leader": "dev-a", "visited": ["dev-a"], "urgency": "high"}"#,
    );
    assert_eq!(escalation.reason, EscalationReason::Unknown);
}
