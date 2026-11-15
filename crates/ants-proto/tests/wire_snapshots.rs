//! Golden snapshots of the JSON form of every ants-proto wire type, and parse checks of documents older versions wrote.
//!
//! Peers of different versions share these documents, so renaming or dropping a field, or a variant, breaks the older
//! ones even when every round trip still passes. Each value is fully populated, every optional field set, so each
//! field name is pinned. A changed snapshot is a wire change: review it with `cargo insta review`, and keep older
//! peers reading what newer ones write.

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
        sent_at: at(0),
    }
}

/// [`envelope`] with none of its optional fields: a plain message, as the first versions wrote one.
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
            PermissionsVersion(4),
        ),
    );
}

#[test]
fn serving() {
    snap(
        "serving",
        &Serving { machine: "studio.local".to_owned(), version: "0.1.0".to_owned(), perms: PermissionsVersion(4) },
    );
}

#[test]
fn roster() {
    snap(
        "roster_entry",
        &RosterEntry {
            ant: user("dev-a"),
            machine: "studio.local".to_owned(),
            instance: Some("019a7d2b-0000-7000-8000-000000000001".to_owned()),
            harness: "copilot".to_owned(),
            agent: Some(HarnessAgent { name: "copilot".to_owned(), version: "1.2.3".to_owned() }),
            capabilities: vec!["rust".to_owned(), "frontend".to_owned()],
            whoami: Some("Backend developer".to_owned()),
            workdir: Some("/src/app".to_owned()),
            started_at: at(0),
            heartbeat_at: at(1),
            activity: Some(Activity::Working),
            handling: Some(1),
            harnesses: Some(2),
            escalate_to: vec![user("lead"), user("cto")],
            version: Some("0.1.0".to_owned()),
            perms: Some(PermissionsVersion(4)),
            signed_out: Some(SignInNeeded { since: at(2), sign_in: Some("copilot login".to_owned()) }),
            limited: Some(HarnessLimit { kind: LimitKind::RateLimit, since: at(3), until: at(8) }),
            failing: Some(HarnessFailing { since: at(4), error: "harness exited".to_owned() }),
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
    snap("read_marker", &ReadMarker { below: 4200, read: BTreeSet::from([4203, 4207]) });
    snap(
        "thread_title",
        &ThreadTitle { thread_id: thread(), title: "Release 0.2".to_owned(), by: user("alice"), at: at(0) },
    );
    snap(
        "thread_marks",
        &ThreadMarks {
            pinned: BTreeMap::from([(thread(), at(0))]),
            archived: BTreeMap::from([(other_thread(), Archived { at: at(1), after: 4211 })]),
        },
    );
    snap("notify_prefs", &NotifyPrefs { snoozed_until: Some(at(30)), muted: BTreeMap::from([(thread(), at(45))]) });
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
            signature: "c2lnLXJlbmV3".to_owned(),
        },
    );
    snap(
        "renew_reply",
        &RenewReply {
            name: "dev-a".to_owned(),
            public_key: "UDEF".to_owned(),
            jwt: "eyJ0eXAiOiJKV1QifQ.renewed".to_owned(),
        },
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
    ];
    for (kind, update) in updates {
        let event = StatusEvent { ant: user("dev-a"), thread_id: thread(), update, at: at(0) };
        snap(&format!("status_event_{kind}"), &event);
    }
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
}

/// Documents in the shape older versions wrote, before the fields they lack were added, still parse, the missing
/// fields taking their defaults.
#[test]
fn older_documents_parse() {
    // The first envelope: no owner, creator, origin thread, escalation, approval, webhook flag, tool rules or mention.
    let message: Envelope = parse(
        r#"{"id": "019a7d2e-0000-7000-8000-000000000001", "thread_id": "019a7d2c-0000-7000-8000-000000000001",
            "from": "alice", "to": "dev-a", "text": "fix the build", "turn_budget": 8, "sent_at": "2025-01-01T12:00:00Z"}"#,
    );
    assert_eq!(message, plain_envelope());
    let notice: Mention = parse(r#"{"channel": "backend", "post": "019a7d2e-0000-7000-8000-000000000003"}"#);
    assert_eq!(notice.author, None);

    // The first roster entry: no instance, agent, whoami, activity, counts, chain, versions or harness troubles.
    let roster: RosterEntry = parse(
        r#"{"ant": "dev-a", "machine": "studio.local", "harness": "copilot",
            "started_at": "2025-01-01T12:00:00Z", "heartbeat_at": "2025-01-01T12:01:00Z"}"#,
    );
    assert_eq!(
        roster,
        RosterEntry {
            ant: user("dev-a"),
            machine: "studio.local".to_owned(),
            instance: None,
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: Vec::new(),
            whoami: None,
            workdir: None,
            started_at: at(0),
            heartbeat_at: at(1),
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: Vec::new(),
            version: None,
            perms: None,
            signed_out: None,
            limited: None,
            failing: None,
        }
    );
    let signed_out: SignInNeeded = parse(r#"{"since": "2025-01-01T12:00:00Z"}"#);
    assert_eq!(signed_out.sign_in, None);
    let chain: ChainEntry = parse(r#"{"ant": "dev-a", "updated_at": "2025-01-01T12:00:00Z"}"#);
    assert!(chain.escalate_to.is_empty());
    let human: HumanEntry = parse(r#"{"human": "alice"}"#);
    assert_eq!(human.registered_at, None);

    // What each human keeps, from before any of its fields were set.
    assert_eq!(parse::<ReadMarker>("{}"), ReadMarker::default());
    assert_eq!(parse::<ThreadMarks>("{}"), ThreadMarks::default());
    assert_eq!(parse::<NotifyPrefs>("{}"), NotifyPrefs::default());

    // Channels before members, messengers and owners; posts before mentions, owners, creators, webhooks and tool rules.
    let entry: ChannelEntry = parse(r#"{"channel": "backend"}"#);
    assert_eq!(entry, ChannelEntry::new(channel()));
    let post: ChannelPost = parse(
        r#"{"id": "019a7d2e-0000-7000-8000-000000000002", "channel": "backend",
            "thread_id": "019a7d2c-0000-7000-8000-000000000001", "from": "alice", "text": "hello", "turn_budget": 8,
            "sent_at": "2025-01-01T12:00:00Z"}"#,
    );
    assert!(post.mentions.is_empty() && post.in_reply_to.is_none() && post.owner.is_none());
    assert!(post.creator.is_none() && !post.webhook && post.allow.is_none());
    let applied: ChannelAdminReply = parse(
        r#"{"request_id": "019a7d2e-0000-7000-8000-000000000001",
            "outcome": {"outcome": "applied", "entry": {"channel": "backend"}, "revision": 3}}"#,
    );
    let ChannelAdminOutcome::Applied { changed, .. } = applied.outcome else { panic!("not applied") };
    assert!(changed.is_empty());

    // Escalations without a tool request, tool calls without locations or input, dead letters of neither kind.
    let asked: Escalation = parse(
        r#"{"stuck": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "reason": "asked", "level": 0,
            "leader": "dev-a", "visited": ["dev-a"]}"#,
    );
    assert_eq!(asked.request_id, None);
    let tool: ToolCall = parse(r#"{"kind": "read", "title": "README.md"}"#);
    assert!(tool.locations.is_empty() && tool.input.is_none());
    let dead: DeadLetter = parse(
        r#"{"ant": "dev-a", "stream_seq": 1, "deliveries": 6, "error": "boom", "died_at": "2025-01-01T12:00:00Z"}"#,
    );
    assert!(dead.envelope.is_none() && dead.payload.is_none());

    // Enrollment and ant requests before leaf nodes and retirement; their answers without a leaf JWT or a reason.
    let enroll: EnrollRequest = parse(
        r#"{"ant": "dev-a", "public_key": "UABC", "host": "laptop.local", "sent_at": "2025-01-01T12:00:00Z",
            "enrollment": "jwt", "signature": "sig"}"#,
    );
    assert_eq!(enroll.leaf, None);
    let approved: EnrollReply = parse(r#"{"outcome": "approved", "ant": "dev-a", "public_key": "UABC", "jwt": "jwt"}"#);
    assert!(matches!(approved, EnrollReply::Approved { leaf_jwt: None, .. }));
    let rejected: EnrollReply = parse(r#"{"outcome": "rejected", "ant": "dev-a", "public_key": null}"#);
    assert!(matches!(rejected, EnrollReply::Rejected { public_key: None, reason: None, .. }));
    let request: AntRequest = parse(
        r#"{"manager": "lead", "name": "dev-c", "template": "rust-dev", "request": "{}", "reason": "help",
            "thread_id": "019a7d2c-0000-7000-8000-000000000001", "public_key": "UGHI", "host": "studio.local",
            "sent_at": "2025-01-01T12:00:00Z", "expires_at": "2025-01-01T12:10:00Z", "credentials": "jwt",
            "signature": "sig"}"#,
    );
    assert!(request.leaf.is_none() && !request.retire);
    let approved: AntReply =
        parse(r#"{"outcome": "approved", "manager": "lead", "name": "dev-c", "public_key": "UGHI", "jwt": "jwt"}"#);
    assert!(matches!(approved, AntReply::Approved { leaf_jwt: None, .. }));
    let retired: AntReply =
        parse(r#"{"outcome": "retired", "manager": "lead", "name": "dev-c", "public_key": "UGHI", "revoked": []}"#);
    assert!(matches!(retired, AntReply::Retired { push_error: None, .. }));

    // A tool update naming only its call; a queue answer before the consumer's state and channel posts were in it.
    let tool: StatusEvent = parse(
        r#"{"ant": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "type": "tool", "id": "call-1",
            "at": "2025-01-01T12:00:00Z"}"#,
    );
    assert_eq!(tool.update, StatusUpdate::Tool { id: "call-1".to_owned(), title: None, tool_kind: None, status: None });
    let queue: QueueReply = parse(
        r#"{"request_id": "019a7d2e-0000-7000-8000-000000000001", "ant": "dev-a",
            "messages": [{"id": "019a7d2e-0000-7000-8000-000000000002", "thread_id": "019a7d2c-0000-7000-8000-000000000001",
                          "from": "alice", "sent_at": "2025-01-01T12:00:00Z", "seq": 4211, "running": false, "ahead": 1}]}"#,
    );
    assert!(queue.delivered.is_none() && queue.undelivered.is_none() && queue.messages[0].channel.is_none());
}

/// What newer versions may add: unknown fields are ignored, and unknown values of the open enumerations read as
/// `unknown`.
#[test]
fn newer_documents_parse() {
    let roster: RosterEntry = parse(
        r#"{"ant": "dev-a", "machine": "studio.local", "harness": "copilot", "started_at": "2025-01-01T12:00:00Z",
            "heartbeat_at": "2025-01-01T12:01:00Z", "activity": "napping", "mood": "great",
            "limited": {"kind": "solar_flare", "since": "2025-01-01T12:00:00Z", "until": "2025-01-01T12:05:00Z"}}"#,
    );
    assert_eq!(roster.activity, Some(Activity::Unknown));
    assert_eq!(roster.limited.map(|l| l.kind), Some(LimitKind::Unknown));
    let escalation: Escalation = parse(
        r#"{"stuck": "dev-a", "thread_id": "019a7d2c-0000-7000-8000-000000000001", "reason": "moon_phase", "level": 0,
            "leader": "dev-a", "visited": ["dev-a"], "urgency": "high"}"#,
    );
    assert_eq!(escalation.reason, EscalationReason::Unknown);
}
