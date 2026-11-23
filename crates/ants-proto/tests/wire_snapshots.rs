//! Golden snapshots of the JSON form of every ants-proto wire type, pinning the current wire form.
//!
//! Peers share these documents, so renaming or dropping a field, or a variant, is a wire change even when every round
//! trip still passes. Each value is fully populated, every optional field set, so each field name is pinned. A changed
//! snapshot is a wire change: review it with `cargo insta review`.
//!
//! The tests are in one submodule per area; the fixtures and the snapshot helpers stay here, as a snapshot's file is
//! named after the module that asserts it.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use ants_proto::*;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

#[path = "wire_snapshots/enrollment.rs"]
mod enrollment;
#[path = "wire_snapshots/messages.rs"]
mod messages;
#[path = "wire_snapshots/providers.rs"]
mod providers;
#[path = "wire_snapshots/reading.rs"]
mod reading;
#[path = "wire_snapshots/records.rs"]
mod records;
#[path = "wire_snapshots/status.rs"]
mod status;
#[path = "wire_snapshots/trails.rs"]
mod trails;

/// Snapshot `value` as `name`, and check it reads back as itself.
fn snap<T: Serialize + DeserializeOwned + PartialEq + Debug>(name: &str, value: &T) {
    insta::assert_json_snapshot!(name, value);
    let json = serde_json::to_string(value).expect("serializes");
    let back: T = serde_json::from_str(&json).unwrap_or_else(|e| panic!("{name} does not read back: {e}\n{json}"));
    assert_eq!(&back, value, "{name} reads back as another value");
}

/// Snapshot the text `line` as `name`.
fn snap_line(name: &str, line: &str) {
    insta::assert_snapshot!(name, line);
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
        sealed: None,
    }
}

fn sealed() -> Sealed {
    Sealed {
        v: SEALED_VERSION,
        device: device(),
        keys: vec![
            SealedKey { device: device(), key: "eGt2MQ==".to_owned() },
            SealedKey { device: "019a7d2a-0000-7000-8000-000000000002".parse().unwrap(), key: "eGt2MQ==".to_owned() },
        ],
        body: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
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
        assistant_of: None,
    }
}

fn decision() -> ApprovalDecision {
    ApprovalDecision {
        request_id: id(10),
        ant: user("dev-a"),
        verdict: Verdict::AllowOnce,
        decided_at: at(1),
        by: None,
        on_behalf_of: None,
    }
}

fn mention() -> Mention {
    Mention { channel: channel(), post: id(3), author: Some(user("github")) }
}

fn quote() -> Quote {
    Quote::new(user("dev-b"), "the build fails on main")
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
        quote: Some(quote()),
        owner: Some(user("alice")),
        creator: Some(user("planner")),
        origin_thread: Some(other_thread()),
        escalation: Some(Box::new(escalation())),
        approval: Some(Box::new(decision())),
        on_behalf_of: Some(user("alice")),
        webhook: true,
        provider: Some("sense".to_owned()),
        queue: true,
        allow: Some(vec!["read:*".to_owned(), "other:send_message".to_owned()]),
        mention: Some(mention()),
        title: Some("Fix the build".to_owned()),
        summary: Some(SummaryRequest { tokens: Some(1834), ..SummaryRequest::thread(other_thread(), 4211) }),
        question: Some(Questions {
            questions: vec![
                Question {
                    text: "Ship the fix?".to_owned(),
                    header: Some("Ship".to_owned()),
                    options: vec![
                        QuestionOption { label: "yes".to_owned(), description: Some("tag and release".to_owned()) },
                        QuestionOption { label: "not yet".to_owned(), description: None },
                    ],
                    multi_select: false,
                    free_text: false,
                },
                Question {
                    text: "Which regions?".to_owned(),
                    header: None,
                    options: vec![
                        QuestionOption { label: "eu".to_owned(), description: None },
                        QuestionOption { label: "us".to_owned(), description: None },
                    ],
                    multi_select: true,
                    free_text: true,
                },
            ],
        }),
        answer: Some(Answer {
            question: id(4),
            picks: vec![Pick::Options(vec![1, 2]), Pick::Typed("ap only".to_owned())],
        }),
        attachments: vec![attachment()],
        sealed: Some(Box::new(sealed())),
        notice: Some(NoticeKind::SignedOut),
        review_commits: vec![ReviewCommit {
            repo: "github.com/acme/app".to_owned(),
            commit: "4b825dc642cb6eb9a060e54bf8d69288fbee4904".to_owned(),
            thread: Some(other_thread()),
            local: Some("/home/dev/app/.git".to_owned()),
        }],
        trail_step: Some(Box::new(trail_step())),
        trail_spent: Some(TrailSpent { steps: 3, turns: 7, running: 1 }),
        trail_start: Some(Box::new(TrailStart {
            trail: "nightly-audit".parse().unwrap(),
            args: serde_json::json!({"repo": "app"}),
            limits: TrailLimits { steps: 100, wall_secs: 2 * 24 * 3600, ..TrailLimits::DEFAULT },
        })),
        rating_of: Some(run()),
        review_outcomes: Some(ReviewOutcomes {
            dream: 7,
            findings: vec![
                FindingOutcome {
                    finding: "5.1".to_owned(),
                    verdict: FindingVerdict::Applied,
                    holds: true,
                    reason: String::new(),
                },
                FindingOutcome {
                    finding: "5.2".to_owned(),
                    verdict: FindingVerdict::Rejected,
                    holds: false,
                    reason: "Not borne out by the threads cited.".to_owned(),
                },
            ],
            earlier_findings: vec!["5.1".to_owned(), "5.2".to_owned()],
        }),
        sent_at: at(0),
    }
}

/// [`envelope`] with none of its optional fields: a plain message.
fn plain_envelope() -> Envelope {
    Envelope {
        in_reply_to: None,
        quote: None,
        owner: None,
        creator: None,
        origin_thread: None,
        escalation: None,
        approval: None,
        on_behalf_of: None,
        webhook: false,
        provider: None,
        queue: false,
        allow: None,
        mention: None,
        title: None,
        summary: None,
        question: None,
        answer: None,
        attachments: Vec::new(),
        sealed: None,
        notice: None,
        review_commits: Vec::new(),
        trail_step: None,
        trail_spent: None,
        trail_start: None,
        rating_of: None,
        review_outcomes: None,
        ..envelope()
    }
}

fn run() -> RunId {
    "019a7d2f-0000-7000-8000-000000000001".parse().unwrap()
}

fn trail_step() -> TrailStep {
    TrailStep {
        run: run(),
        call: 3,
        label: Some("review".to_owned()),
        schema: Some(serde_json::json!({"properties": {"ok": {"type": "boolean"}}, "type": "object"})),
        trail: Some("release".parse().unwrap()),
        script: Some(script_sha256("agent('dev-a', 'build it')")),
        tip_key: Some("review".to_owned()),
        bounds: Some(StepBounds {
            steps: 12,
            turns: 40,
            until: at(9),
            route: vec!["triage".parse().unwrap(), "release".parse().unwrap()],
        }),
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
        description: Some("Incidents and deploys of the shop".to_owned()),
    }
}

fn enroll_leaf() -> EnrollLeaf {
    EnrollLeaf { machine: machine(), jwt: "eyJ0eXAiOiJKV1QifQ.leaf".to_owned(), signature: "c2lnLWxlYWY".to_owned() }
}
