//! Golden snapshots of the JSON form of every ants-proto wire type, pinning the current wire form.
//!
//! Peers share these documents, so renaming or dropping a field, or a variant, is a wire change even when every round
//! trip still passes. Each value is fully populated, every optional field set, so each field name is pinned. A changed
//! snapshot is a wire change: review it with `cargo insta review`.

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
fn colony() {
    snap(
        "colony_entry",
        &ColonyEntry {
            ant: user("dev-a"),
            machine: "studio.local".to_owned(),
            instance: "019a7d2b-0000-7000-8000-000000000001".to_owned(),
            harness: "copilot".to_owned(),
            agent: Some(HarnessAgent { name: "copilot".to_owned(), version: "1.2.3".to_owned() }),
            capabilities: vec!["rust".to_owned(), "frontend".to_owned()],
            whoami: Some("Backend developer".to_owned()),
            profile: Some("Good for Rust services and their NATS wiring.".to_owned()),
            sense_unkept: Some("no host credentials on a server with authentication".to_owned()),
            workdir: Some("/src/app".to_owned()),
            started_at: at(0),
            heartbeat_at: at(1),
            activity: Activity::Working,
            handling: 1,
            harnesses: 2,
            escalate_to: vec![user("lead"), user("cto")],
            sage: Some(user("sage")),
            version: "0.1.0".to_owned(),
            perms: PermissionsVersion(1),
            signed_out: Some(SignInNeeded { since: at(2), sign_in: Some("copilot login".to_owned()) }),
            limited: Some(HarnessLimit { kind: LimitKind::RateLimit, since: at(3), until: at(8) }),
            failing: Some(HarnessFailing { since: at(4), error: "harness exited".to_owned() }),
            fallback: Some(ServingFallback { name: "claude:sonnet".to_owned(), since: at(5) }),
            memory_low: Some(MemoryLow { since: at(5), available: 805_306_368, max_harnesses: 1 }),
            model: Some(AntModel {
                model: Some("gpt-5".to_owned()),
                reasoning_effort: Some("high".to_owned()),
                context: Some("1m".to_owned()),
            }),
            paused: Some(Paused { since: at(6), by: Some(user("alice")) }),
            drift: Vec::new(),
            mcp: true,
        },
    );
    snap("activity_variants", &[Activity::Idle, Activity::Working, Activity::Unknown]);
    snap("limit_kind_variants", &[LimitKind::RateLimit, LimitKind::Quota, LimitKind::Unknown]);
}

#[test]
fn reminders() {
    snap(
        "reminders",
        &Reminders {
            remind: BTreeMap::from([
                (
                    thread(),
                    Reminder { at: at(0), due: at(30), message: Some(id(7)), note: "check CI".into(), pushed: true },
                ),
                (
                    other_thread(),
                    Reminder { at: at(1), due: at(45), message: None, note: String::new(), pushed: false },
                ),
            ]),
            snoozed: ThreadSnoozes(BTreeMap::from([(thread(), Snooze { at: at(2), until: at(50) })])),
        },
    );
}

#[test]
fn ant_identity() {
    snap(
        "ant_identity",
        &AntIdentity {
            avatar: Some("lead".to_owned()),
            color: Some("azure".to_owned()),
            prompts: 42,
            moved: Some(MovedMark { at: at(2), by: user("alice") }),
            cloned_from: Some(ClonedFrom {
                ant: user("dev-b"),
                id: "fedcba9876543210fedcba9876543210".to_owned(),
                at: at(1),
            }),
            sense: Some("01JD8X5G3T7Q9R2M4N6P8S0V1W".parse().unwrap()),
            ..AntIdentity::new(user("dev-a"), "0123456789abcdef0123456789abcdef".to_owned(), at(0))
        },
    );
}

#[test]
fn kv_entries() {
    snap("chain_entry", &ChainEntry { ant: user("dev-a"), escalate_to: vec![user("lead")], updated_at: at(0) });
    snap("human_entry", &HumanEntry { human: user("alice"), registered_at: Some(at(0)) });
    snap(
        "reaction_entry",
        &ReactionEntry {
            message: id(1),
            by: user("alice"),
            reaction: "eyes".parse().unwrap(),
            at: at(0),
            on_behalf_of: None,
        },
    );
    let reaction = ReactionEntry::new(user("asst"), id(1), "eyes".parse().unwrap());
    snap("reaction_entry_on_behalf", &ReactionEntry { at: at(0), ..reaction.on_behalf(Some(user("alice"))) });
    let request = ReactionsRequest { id: id(1), ant: user("dev-a"), message: id(2), requested_at: at(0), change: None };
    snap("reactions_request", &request);
    for (kind, remove) in [("react", false), ("unreact", true)] {
        let change = Some(ReactionChange::new("eyes".parse().unwrap(), remove));
        snap(&format!("reactions_request_{kind}"), &ReactionsRequest { change, ..request.clone() });
    }
    let reacted = Reacted::new("eyes".parse().unwrap(), vec![user("alice"), user("dev-b")]);
    for (kind, outcome, responder) in [
        ("listed", ReactionsOutcome::Listed { reactions: vec![reacted] }, ReactionsResponder::Webhook),
        ("applied", ReactionsOutcome::Applied { changed: true }, ReactionsResponder::Signer),
        ("unknown", ReactionsOutcome::Unknown, ReactionsResponder::Signer),
    ] {
        let reply = ReactionsReply { request_id: id(1), ant: user("dev-a"), message: id(2), outcome, responder };
        snap(&format!("reactions_reply_{kind}"), &reply);
    }
    let unread = BTreeSet::from([4198]);
    snap("read_marker", &ReadMarker { below: 4200, read: BTreeSet::from([4203, 4207]), unread });
    let marker = ReadMarker { below: 4200, read: BTreeSet::from([4207]), ..ReadMarker::default() };
    snap("channel_read_markers", &ChannelReadMarkers { channels: BTreeMap::from([(channel(), marker)]) });
    snap(
        "thread_title",
        &ThreadTitle {
            thread_id: thread(),
            title: "Release 0.2".to_owned(),
            by: user("alice"),
            at: at(0),
            sealed: None,
            on_behalf_of: None,
        },
    );
    snap(
        "thread_title_sealed",
        &ThreadTitle {
            thread_id: thread(),
            title: String::new(),
            by: user("alice"),
            at: at(0),
            sealed: Some(Box::new(sealed())),
            on_behalf_of: None,
        },
    );
    snap("sealed_title", &SealedTitle { thread_id: thread(), by: user("alice"), title: "Lunch".to_owned() });
    let done = ThreadDone { thread_id: thread(), by: user("alice"), at: at(0), after: 4211, on_behalf_of: None };
    snap("thread_done", &done);
    let on_behalf = ThreadDone { by: user("asst"), ..done }.on_behalf(Some(user("alice")));
    snap("thread_done_on_behalf", &on_behalf);
    snap(
        "thread_summary",
        &ThreadSummary {
            thread_id: thread(),
            text: "Asked for a release; the build is fixed; the tag waits on alice.".to_owned(),
            by: user("messenger"),
            asker: user("alice"),
            at: at(0),
            upto: 4211,
            tokens: 1834,
        },
    );
    snap(
        "thread_marks",
        &ThreadMarks {
            pinned: BTreeMap::from([(thread(), at(0))]),
            archived: BTreeMap::from([(other_thread(), Archived { at: at(1), after: 4211 })]),
            kept: BTreeMap::from([(thread(), at(2))]),
        },
    );
    snap(
        "thread_snoozes",
        &ThreadSnoozes(BTreeMap::from([
            (thread(), Snooze { at: at(0), until: at(30) }),
            (other_thread(), Snooze { at: at(1), until: at(45) }),
        ])),
    );
    let channels = BTreeMap::from([("ops".parse().unwrap(), ChannelNotify::Mentions)]);
    let muted = BTreeMap::from([(thread(), at(45))]);
    snap("notify_prefs", &NotifyPrefs { snoozed_until: Some(at(30)), muted, channels });
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
    snap("notice_kind_mcp_sign_in", &NoticeKind::McpSignIn);
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
        "envelope_wake",
        &Envelope { from: user("dev-a"), to: user("dev-a"), provider: Some("sense".to_owned()), ..message.clone() },
    );
    snap(
        "envelope_mention_notice",
        &Envelope { from: user("dev-b"), to: user("alice"), mention: Some(mention()), ..message.clone() },
    );
    let digest = SummaryRequest::since(at(0), 4211);
    snap(
        "envelope_summary_request",
        &Envelope { from: user("alice"), to: user("messenger"), summary: Some(digest.clone()), ..message.clone() },
    );
    snap(
        "envelope_summary_reply",
        &Envelope {
            from: user("messenger"),
            to: user("alice"),
            in_reply_to: Some(id(1)),
            summary: Some(SummaryRequest { tokens: Some(1834), ..digest }),
            ..message.clone()
        },
    );
    snap(
        "envelope_sealed",
        &Envelope {
            from: user("alice"),
            to: user("bob"),
            text: String::new(),
            owner: Some(user("alice")),
            sealed: Some(Box::new(sealed())),
            ..message
        },
    );
    snap(
        "sealed_body",
        &SealedBody {
            from: user("alice"),
            to: user("bob"),
            thread_id: thread(),
            id: id(1),
            text: "lunch?".to_owned(),
            title: Some("Lunch".to_owned()),
            quote: Some(Quote::new(user("bob"), "noon?")),
            attachments: vec![Attachment {
                size: 1250,
                sealed: Some(SealedAttachment { nonce: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned() }),
                ..attachment()
            }],
        },
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
    let assistant = Escalation {
        reason: EscalationReason::Question,
        request_id: None,
        leader: user("pa"),
        visited: vec![user("dev-a"), user("pa")],
        assistant_of: Some(user("alice")),
        ..escalation()
    };
    snap("escalation_to_assistant", &assistant);
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
            EscalationReason::Question,
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
            on_behalf_of: Some(user("alice")),
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
        ChannelOp::SetDescription { description: "Incidents and deploys of the shop".to_owned() },
        ChannelOp::ClearDescription,
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
            on_behalf_of: Some(user("alice")),
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
            Refusal::NotInColony(user("ghost")),
            Refusal::Expired,
            Refusal::Busy,
            Refusal::Invalid("no change asked for".to_owned()),
            Refusal::Description,
            Refusal::NotForHuman,
            Refusal::NotAssistant(user("alice")),
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
            quote: Some(quote()),
            owner: Some(user("alice")),
            creator: Some(user("planner")),
            webhook: true,
            queue: true,
            allow: Some(vec!["read:*".to_owned()]),
            title: Some("Fix CI".to_owned()),
            attachments: vec![attachment()],
            on_behalf_of: Some(user("alice")),
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
        labels: BTreeMap::new(),
        requested_at: at(0),
        expires_at: at(5),
        human: None,
        stand_in: None,
    };
    snap("approval_request", &request);
    let always = "Yes, and don't ask again for git push commands in /work";
    snap("approval_request_labelled", &request.clone().labelled([(Verdict::AllowAlways, always.to_owned())].into()));
    snap("approval_request_for_human", &request.for_human(Some(user("alice")), Some(user("bob"))));
    snap("approval_decision", &decision());
    snap("approval_decision_by", &decision().by(Some(user("bob"))));
    snap("approval_decision_on_behalf", &decision().on_behalf(Some(user("alice"))));
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
            order: None,
            host_creds: Some(EnrollHost { machine: machine(), public_key: "UHOST".to_owned() }),
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
                host_jwt: Some("eyJ0eXAiOiJKV1QifQ.host".to_owned()),
            },
            EnrollReply::Rejected {
                ant: user("dev-a"),
                public_key: Some("UABC".to_owned()),
                reason: Some("unknown machine".to_owned()),
            },
            EnrollReply::Retired {
                ant: user("dev-a"),
                public_key: "UABC".to_owned(),
                revoked: vec!["UABC".to_owned()],
                push_error: None,
                leaf_jwt: Some("eyJ0eXAiOiJKV1QifQ.leaf".to_owned()),
                leaf_error: None,
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
                leaf_jwt: None,
                leaf_error: Some("no uplink of m1".to_owned()),
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
            "asked",
            StatusUpdate::Asked {
                to: user("alice"),
                message: id(11),
                question: Question::new("Ship it?", &["yes".to_owned(), "not yet".to_owned()], false).unwrap().into(),
            },
        ),
        (
            "decided",
            StatusUpdate::Decided {
                request_id: id(10),
                verdict: Verdict::AllowOnce,
                by: DecidedBy::Chain,
                decider: Some(user("lead")),
                via: None,
            },
        ),
        (
            "step",
            StatusUpdate::Step {
                run: run(),
                call: 3,
                label: Some("review".to_owned()),
                to: user("dev-a"),
                step_thread: other_thread(),
                stage: StepStage::Answered,
            },
        ),
        (
            "leg",
            StatusUpdate::Leg {
                run: run(),
                leg: 1,
                trail: Some("release".parse().unwrap()),
                waypoint: Some("review".to_owned()),
            },
        ),
        ("leg_inline", StatusUpdate::Leg { run: run(), leg: 0, trail: None, waypoint: None }),
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
    let ends = vec![
        TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, reason: None, text: "Thanks, noted.".to_owned() },
        TimelineEntry::NoReply {
            why: NoReplyWhy::Completed,
            reason: Some("nothing to add".to_owned()),
            text: String::new(),
        },
    ];
    snap("timeline_no_reply", &Timeline { entries: ends, omitted: 0, ..record.clone() });
    let calls = [("execute", CallCount { calls: 240, failed: 12 }), ("other", CallCount { calls: 3, failed: 0 })];
    let ended = TimelineEnd {
        how: PromptEnd::Capped,
        duration_ms: 95_000,
        tokens: 41_200,
        cost: 0.37,
        asked: 2,
        rebuilt: true,
        calls: calls.map(|(kind, count)| (kind.to_owned(), count)).into(),
        sense_calls: 2,
    };
    snap("timeline_ended", &Timeline { entries: vec![TimelineEntry::Ended(ended)], omitted: 0, ..record.clone() });
    let replied = TimelineEnd {
        how: PromptEnd::Replied,
        duration_ms: 800,
        tokens: 0,
        cost: 0.0,
        asked: 0,
        rebuilt: false,
        calls: std::collections::BTreeMap::new(),
        sense_calls: 0,
    };
    snap("timeline_ended_bare", &Timeline { entries: vec![TimelineEntry::Ended(replied)], omitted: 0, ..record });
}

#[test]
fn cancel_and_queue() {
    let cancel = CancelRequest {
        id: id(1),
        ant: user("dev-a"),
        thread_id: thread(),
        by: user("alice"),
        sent_at: at(0),
        on_behalf_of: None,
        withdraw: None,
    };
    snap("cancel_request", &cancel);
    let withdrawal =
        CancelRequest { by: user("pa"), on_behalf_of: Some(user("alice")), withdraw: Some(id(2)), ..cancel };
    snap("cancel_request_withdrawal_for_human", &withdrawal);
    let reply = CancelReply { request_id: id(1), ant: user("dev-a"), thread_id: thread(), cancelled: true, mark: None };
    snap("cancel_reply", &reply);
    let found = MessageMark::Started { thread_id: thread(), at: at(1) };
    snap("cancel_reply_withdrawal_refused", &CancelReply { cancelled: false, mark: Some(found), ..reply });
    let queue = QueueRequest { id: id(1), ant: user("dev-a"), by: user("alice"), sent_at: at(0), on_behalf_of: None };
    snap("queue_request", &queue);
    snap("queue_request_for_human", &QueueRequest { by: user("pa"), on_behalf_of: Some(user("alice")), ..queue });
    let queued = QueuedMessage {
        id: id(2),
        thread_id: thread(),
        from: user("alice"),
        channel: Some(channel()),
        sent_at: at(0),
        seq: 4211,
        running: true,
        ahead: 0,
        redelivery: false,
    };
    snap("queued_message", &queued);
    let reply =
        QueueReply { request_id: id(1), ant: user("dev-a"), messages: vec![queued.clone()], inbox: None, posts: None };
    snap("queue_reply", &reply);
    let retry = QueuedMessage { channel: None, seq: 4209, running: false, redelivery: true, ..queued };
    let inbox = ConsumerPlace { acked: 4208, delivered: 4211, undelivered: 3, ack_pending: 2 };
    let posts = ConsumerPlace { acked: 4210, delivered: 4210, undelivered: 0, ack_pending: 0 };
    let consumers = QueueReply { messages: vec![retry], inbox: Some(inbox), posts: Some(posts), ..reply };
    snap("queue_reply_with_consumers", &consumers);
    snap(
        "message_mark_variants",
        &[
            MessageMark::Started { thread_id: thread(), at: at(0) },
            MessageMark::Retrying { thread_id: thread(), at: at(0), held: None },
            MessageMark::Retrying { thread_id: thread(), at: at(0), held: Some(Hold::SignIn) },
            MessageMark::Finished { thread_id: thread(), at: at(1) },
            MessageMark::Withdrawn { by: user("alice"), at: at(2), on_behalf_of: None, held: None },
            MessageMark::Withdrawn { by: user("pa"), at: at(3), on_behalf_of: Some(user("alice")), held: None },
            MessageMark::Withdrawn {
                by: user("alice"),
                at: at(4),
                on_behalf_of: None,
                held: Some(Hold::ProviderLimit),
            },
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

fn call_context(by: provider::Starter) -> provider::CallContext {
    provider::CallContext {
        ant: user("dev-a"),
        identity: "0f1e2d3c4b5a69788796a5b4c3d2e1f0".into(),
        thread: thread(),
        origin: provider::Origin { by, channel: Some(channel()) },
        harness: "copilot".into(),
        model: Some("gpt-5".into()),
        machine: "studio".into(),
        repo: Some("github.com/owner/repo".into()),
        at: at(1),
    }
}

#[test]
fn provider_protocol() {
    use provider::jsonrpc::{Message, Notification, Request, RequestId, Response};
    use provider::*;
    use serde_json::json;

    let info = |name: &str| Info { name: name.into(), version: "0.2.0".into() };
    let params = InitializeParams::new(info("ants"), Capabilities { tools: true, wake: true, ..Capabilities::NONE });
    snap("provider_initialize_params", &params);
    let result = params.accept(&[PROTOCOL_VERSION], info("sense"), Capabilities::ALL).unwrap();
    snap("provider_initialize_result", &result);

    // Params are kept as JSON, whose keys keep their order only where serde_json preserves it: these are sorted.
    let ant = AntRef { ant: user("dev-a"), identity: "0f1e".into() };
    snap("provider_request", &Request::new(RequestId::Number(7), method::MEMORIES, &ant).unwrap());
    snap(
        "provider_request_string_id",
        &Request::new(RequestId::String("w-1".into()), method::TOOLS_LIST, &()).unwrap(),
    );
    snap("provider_notification", &Notification::new("log", &json!({"level": "info"})).unwrap());
    snap("provider_response_result", &Response::new(RequestId::Number(7), Ok(Ack {})));
    let refused = RpcError::unsupported_version(&[2], &[PROTOCOL_VERSION]);
    snap("provider_response_error", &Response::new(RequestId::Number(7), Err::<Ack, _>(refused)));
    snap("provider_response_unread", &Response::unread(RpcError::parse_error("expected value")));
    let line = Message::Response(Response::new(RequestId::Number(1), Ok(Woken { thread: thread() }))).to_line();
    insta::assert_snapshot!("provider_message_line", line);

    let tools = ToolList {
        tools: vec![Tool {
            name: "recall".into(),
            description: "Search memories.".into(),
            input_schema: json!({"properties": {"query": {"type": "string"}}, "type": "object"}),
        }],
    };
    snap("provider_tool_list", &tools);
    let call = ToolCall {
        name: "recall".into(),
        arguments: json!({"query": "deploy"}).as_object().unwrap().clone(),
        context: call_context(Starter::Human { user: user("alice") }),
        format: Format::Json,
    };
    snap("provider_tool_call", &call);
    let starters = vec![
        Starter::Human { user: user("alice") },
        Starter::Ant { user: user("dev-b") },
        Starter::Webhook { user: user("github") },
        Starter::Provider { name: "sense".into() },
    ];
    snap("provider_starter_variants", &starters);
    snap("provider_tool_result_text", &ToolResult { output: Output::Text("2 memories".into()), is_error: false });
    snap("provider_tool_result_json", &ToolResult { output: Output::Json(json!({"memories": []})), is_error: false });
    snap("provider_tool_result_error", &ToolResult { output: Output::Text("no such memory".into()), is_error: true });

    let brief = BriefRequest {
        context: call_context(Starter::Webhook { user: user("github") }),
        budget: BRIEF_BUDGET,
        reason: BriefReason::Rebuilt,
    };
    snap("provider_brief_request", &brief);
    snap("provider_brief", &Brief { text: "Goal: ship the release.".into() });

    let kinds = vec![
        EventKind::AntAdded,
        EventKind::AntRenamed { from: user("dev-old") },
        EventKind::AntMoved,
        EventKind::AntCloned { from: user("dev-b"), from_identity: "a1b2".into() },
        EventKind::AntRetired,
        EventKind::AntRevived,
        EventKind::ThreadStarted {
            origin: provider::Origin { by: Starter::Ant { user: user("dev-b") }, channel: None },
            repo: Some("github.com/o/r".into()),
        },
        EventKind::ThreadDone,
        EventKind::ThreadExpired,
        EventKind::SessionLost,
        EventKind::SessionRebuilt { reason: Rebuilt::Overflow },
    ];
    let events: Vec<Event> = kinds
        .into_iter()
        .zip(1..)
        .map(|(kind, n)| Event {
            id: id(n),
            ant: user("dev-a"),
            identity: "0f1e".into(),
            thread: Some(thread()),
            at: at(2),
            kind,
        })
        .collect();
    snap("provider_event_variants", &events);
    snap("provider_ack", &Ack {});

    snap(
        "provider_wake_request",
        &WakeRequest {
            ant: user("dev-a"),
            thread: Some(thread()),
            text: "The reminder is due.".into(),
            id: "rem-1".into(),
        },
    );
    snap("provider_woken", &Woken { thread: other_thread() });
    snap("provider_ant_ref", &ant);
    let memories =
        MemoriesView { memories: vec![MemoryEntry { key: "lesson/deploy".into(), content: "Tag first.".into() }] };
    snap("provider_memories_view", &memories);
    let export = Export { provider: "sense".into(), data: json!({"memories": 1}) };
    snap("provider_export", &export);
    snap("provider_import_request", &ImportRequest { ant: ant.ant, identity: ant.identity, export });
}

#[test]
fn provider_wake_refusals() {
    use provider::Woken;
    use provider::jsonrpc::{RequestId, Response, RpcError};

    let unknown = RpcError::new(RpcError::UNKNOWN_THREAD, format!("thread {} is no thread of ant dev-a", thread()));
    snap("provider_wake_unknown_thread", &Response::new(RequestId::Number(8), Err::<Woken, _>(unknown)));
}

#[test]
fn admin_orders_of_providers() {
    snap("admin_op_read_memories", &AdminOp::ReadMemories { identity: Some("0f1e".into()) });
    let payload = AdminPayload { id: id(9), machine: machine(), size: 300_000, sha256: "ab".repeat(32) };
    snap("admin_op_export_provider", &AdminOp::ExportProvider { identity: "0f1e".into() });
    snap("admin_op_import_provider", &AdminOp::ImportProvider { identity: "0f1e".into(), payload: payload.clone() });
    snap("admin_provider_export", &ProviderExport { provider: Some("sense".into()), payload: Some(payload) });
    let provider = ProviderMemories {
        provider: "sense".into(),
        memories: vec![BdMemory { key: "lesson/deploy".into(), content: "Tag first.".into(), truncated: true }],
        error: Some("provider sense did not answer within 10s".into()),
    };
    let memories = Memories {
        core: CoreMemories { whoami: Some("A reviewer.".into()), ..CoreMemories::default() },
        provider: Some(provider),
        skipped: vec![Skipped { what: "provider memory big".into(), why: "over 64 KiB".into() }],
        ..Memories::default()
    };
    snap("admin_memories_of_a_provider", &memories);
}

#[test]
fn admin_orders_of_trails() {
    snap("admin_op_stop_trail", &AdminOp::StopTrail { run: run() });
    let limits = TrailLimits { steps: 100, wall_secs: 2 * 24 * 3600, ..TrailLimits::DEFAULT };
    snap("admin_op_resume_trail", &AdminOp::ResumeTrail { run: run(), limits: Some(limits) });
}

#[test]
fn trail_child_runs() {
    let child = TrailRun {
        run: "019a7d2e-0000-7000-8000-000000000007".parse().unwrap(),
        conductor: user("planner"),
        route: vec![TrailLeg {
            trail: Some("review".parse().unwrap()),
            local: false,
            at: None,
            script: "await agent('review it', ant='dev-a')".to_owned(),
            args: serde_json::json!({"pr": 7}),
            first_call: 0,
            started_at: at(4),
            steps: 1,
        }],
        owner: Some(user("alice")),
        thread: other_thread(),
        parent: Some(TrailParent { run: run(), call: 2, origin_thread: thread() }),
        spends_from: None,
        grants: Vec::new(),
        origin: provider::Origin { by: provider::Starter::Human { user: user("alice") }, channel: Some(channel()) },
        limits: TrailLimits { steps: 20, ..TrailLimits::DEFAULT },
        state: RunState::Running,
        phase: None,
        steps: 1,
        turns: 1,
        started_at: at(4),
        updated_at: at(5),
        ended_at: None,
    };
    snap("trail_run_child", &child);
    let call = PendingCall {
        call: 2,
        function: TrailFunction::Trail,
        args: serde_json::json!({"args": {"pr": 7}, "name": "review"}),
        to: user("planner"),
        thread: other_thread(),
        message: id(7),
        sent_at: at(4),
        sent: true,
        turn_budget: 0,
        retries: 0,
    };
    assert_eq!(call.child(), Some(child.run), "a trail() call's message is its child run's id");
    snap("trail_child_call", &call);
}

/// A run that went from triage's start to release's review waypoint.
fn routed_run() -> TrailRun {
    TrailRun {
        run: run(),
        conductor: user("planner"),
        route: vec![
            TrailLeg {
                trail: Some("triage".parse().unwrap()),
                local: false,
                at: None,
                script: "await goto('release', at='review', state={'version': '1.2.0'})".to_owned(),
                args: serde_json::json!({"bug": 7}),
                first_call: 0,
                started_at: at(0),
                steps: 2,
            },
            TrailLeg {
                trail: Some("release".parse().unwrap()),
                local: false,
                at: Some("review".to_owned()),
                script: "async def review(state):\n    return await agent(state['version'], ant='dev-a')".to_owned(),
                args: serde_json::json!({"version": "1.2.0"}),
                first_call: 3,
                started_at: at(2),
                steps: 1,
            },
        ],
        owner: Some(user("alice")),
        thread: thread(),
        parent: None,
        spends_from: None,
        grants: Vec::new(),
        origin: provider::Origin { by: provider::Starter::Human { user: user("alice") }, channel: Some(channel()) },
        limits: TrailLimits { steps: 20, ..TrailLimits::DEFAULT },
        state: RunState::Done { result: serde_json::json!({"tag": "v1.2.0"}) },
        phase: Some("review".to_owned()),
        steps: 3,
        turns: 5,
        started_at: at(0),
        updated_at: at(9),
        ended_at: Some(at(9)),
    }
}

#[test]
fn trails() {
    snap(
        "envelope_step",
        &Envelope { from: user("planner"), to: user("dev-a"), ..plain_envelope().as_step(trail_step()) },
    );
    let saved = SavedTrail {
        name: "release".parse().unwrap(),
        description: "Cut a release".to_owned(),
        phases: vec!["build".to_owned(), "review".to_owned()],
        waypoints: vec!["build".to_owned(), "review".to_owned()],
        script: "agent('dev-a', 'build it')".to_owned(),
        saved_by: Saver::Human { user: user("alice") },
        saved_from: None,
        saved_at: at(0),
    };
    snap("saved_trail", &saved);
    snap(
        "saved_trail_by_ant",
        &SavedTrail { saved_by: Saver::Ant { user: user("dev-a") }, saved_from: Some(run()), ..saved },
    );
    let header = routed_run();
    assert_eq!(header.route_text(), "triage -> release@review");
    snap("trail_run", &header);
    let mut newer = serde_json::to_value(&header).unwrap();
    newer["paused_at"] = serde_json::json!("2025-01-01T12:05:00Z");
    assert_eq!(serde_json::from_value::<TrailRun>(newer).unwrap(), header, "unknown fields are additions");
    let states = [
        RunState::Running,
        RunState::Waiting,
        RunState::Stopped { by: user("alice") },
        RunState::Failed { error: "steps limit of 20 reached".to_owned(), limit: None },
    ];
    let runs: Vec<_> = states.into_iter().map(|state| TrailRun { state, ended_at: None, ..header.clone() }).collect();
    snap("trail_run_states", &runs);
    snap(
        "trail_session",
        &TrailSession {
            session: r#"{"monty":"dump"}"#.to_owned(),
            pending: vec![PendingCall {
                call: 3,
                function: TrailFunction::Agent,
                args: serde_json::json!(["dev-a", "review it"]),
                to: user("dev-a"),
                thread: other_thread(),
                message: id(5),
                sent_at: at(4),
                sent: true,
                turn_budget: 4,
                retries: 1,
            }],
            queued: vec![QueuedCall { call: 4, function: TrailFunction::Human, args: serde_json::json!(["ship it?"]) }],
            anew: vec![2],
        },
    );
    let args = serde_json::json!(["dev-a", "build it"]);
    let answer = |call, answer| JournalEntry { call, function: TrailFunction::Agent, args: args.clone(), answer };
    let refused = JournalEntry {
        call: 3,
        function: TrailFunction::Goto,
        args: serde_json::json!({"state": 7, "trail": "triage"}),
        answer: CallAnswer::Error("the run entered trail triage at its start before".to_owned()),
    };
    snap(
        "journal_entries",
        &[
            answer(1, CallAnswer::Ok(serde_json::json!({"ok": true}))),
            answer(2, CallAnswer::Error("dev-a is not an ant".to_owned())),
            refused,
        ],
    );
    snap("trail_run_index", &TrailRunIndex { runs: vec![run()] });
    snap(
        "trail_tip",
        &TrailTip {
            trail: "release".parse().unwrap(),
            id: "k3f7q2ab".parse().unwrap(),
            writer: user("dev-a"),
            text: "Run the migrations before the smoke tests.".to_owned(),
            tip_key: Some("review".to_owned()),
            run: run(),
            script: script_sha256("agent('dev-a', 'build it')"),
            written_at: at(3),
            retired: Some(TipRetired { by: user("alice"), at: at(9) }),
        },
    );
    snap("tip_retired", &TipRetired { by: user("alice"), at: at(9) });
    snap(
        "tip_vote",
        &TipVote {
            voter: user("dev-b"),
            verdict: TipVerdict::Stale,
            reason: Some("the smoke tests now run the migrations".to_owned()),
            text: script_sha256("Run the migrations before the smoke tests."),
            script: Some(script_sha256("agent('dev-a', 'build it')")),
            at: at(5),
        },
    );
    let limits: TrailLimits = parse(r#"{"steps": 10}"#);
    assert_eq!(limits, TrailLimits { steps: 10, ..TrailLimits::DEFAULT }, "missing limits take their defaults");
}

#[test]
fn trail_ratings() {
    snap(
        "trail_rating",
        &TrailRating {
            trail: "release".parse().unwrap(),
            conductor: user("dev-a"),
            run: run(),
            outcome: RatingOutcome::Partly,
            task: "release 2.1 with the migration".to_owned(),
            why: "built and tagged, but the smoke tests needed a rerun".to_owned(),
            script: script_sha256("agent('dev-a', 'build it')"),
            untrusted: false,
            at: at(6),
        },
    );
}

#[test]
fn trail_control() {
    let request = |action| TrailRequest { id: id(1), ant: user("planner"), by: user("alice"), sent_at: at(0), action };
    snap(
        "trail_requests",
        &[
            request(TrailAction::Start {
                trail: TrailSource::Saved("release".parse().unwrap()),
                args: serde_json::json!({"version": "1.2.0"}),
                thread: thread(),
                limits: TrailLimits { steps: 20, ..TrailLimits::DEFAULT },
            }),
            request(TrailAction::Start {
                trail: TrailSource::Script("agent('dev-a', 'build it')".to_owned()),
                args: serde_json::Value::Null,
                thread: thread(),
                limits: TrailLimits::DEFAULT,
            }),
            request(TrailAction::Stop { run: run() }),
            request(TrailAction::Resume {
                run: run(),
                limits: Some(TrailLimits { steps: 40, ..TrailLimits::DEFAULT }),
            }),
        ],
    );
    let reply = |outcome| TrailReply { request_id: id(1), ant: user("planner"), outcome };
    snap(
        "trail_replies",
        &[
            reply(TrailOutcome::Started { run: run() }),
            reply(TrailOutcome::Stopped {
                run: run(),
                waiting: vec![WaitingStep { to: user("dev-a"), message: id(5) }],
            }),
            reply(TrailOutcome::Resumed { run: run() }),
            reply(TrailOutcome::Stopping { run: run() }),
            reply(TrailOutcome::Refused { error: "no saved trail release".to_owned() }),
        ],
    );
}

#[test]
fn trail_run_limits() {
    use ants_proto::RunLimit;
    let limits =
        [RunLimit::Steps, RunLimit::Turns, RunLimit::Wall, RunLimit::Compute, RunLimit::Session, RunLimit::Memory];
    let failed = limits.map(|limit| RunState::Failed { error: format!("past its {limit} limit"), limit: Some(limit) });
    snap("trail_run_failed_on_limits", &failed.to_vec());
}

#[test]
fn trail_run_spending_from_a_step() {
    let spends_from = SpendsFrom {
        conductor: user("lead"),
        run: "019a7d2e-0000-7000-8000-000000000009".parse().unwrap(),
        call: 4,
        bounds: StepBounds { steps: 6, turns: 40, until: at(9), route: vec!["deploy".parse().unwrap()] },
    };
    let grant = StepGrant {
        call: 2,
        thread: "019a7d2e-0000-7000-8000-00000000000a".parse().unwrap(),
        steps: 3,
        turns: 20,
        spent: TrailSpent { steps: 1, turns: 2, running: 1 },
    };
    let run = TrailRun { spends_from: Some(spends_from), grants: vec![grant], ..routed_run() };
    snap("trail_run_spending_from_a_step", &run);
}

#[test]
fn trail_answering() {
    let answering = TrailAnswering {
        conductor: user("lead"),
        run: "019a7d2e-0000-7000-8000-000000000009".parse().unwrap(),
        call: 4,
        runs: vec!["019a7d2e-0000-7000-8000-00000000000b".parse().unwrap()],
    };
    snap("trail_answering", &answering);
}
