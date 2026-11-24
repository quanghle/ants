//! Snapshots of what a daemon tells of its work and is asked about it: status events, timeline records, cancel and
//! queue requests.

use super::*;

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

/// What observers hear of the bus besides an ant's work: a trail run's end, an admin order settled, a team changed.
#[test]
fn observed_status() {
    snap("run_end_variants", &[RunEnd::Done, RunEnd::Failed, RunEnd::Stopped]);
    snap("team_change_variants", &[TeamChange::Created, TeamChange::Changed, TeamChange::Deleted]);
    let updates = [
        (
            "run_ended",
            StatusUpdate::RunEnded {
                run: run(),
                trail: Some("release".parse().unwrap()),
                parent: Some("019a7d2f-0000-7000-8000-000000000002".parse().unwrap()),
                state: RunEnd::Failed,
                error: Some("ValueError: no tests".to_owned()),
                limit: Some(RunLimit::Turns),
                by: None,
                steps: 4,
                turns: 9,
            },
        ),
        (
            "run_ended_stopped",
            StatusUpdate::RunEnded {
                run: run(),
                trail: None,
                parent: None,
                state: RunEnd::Stopped,
                error: None,
                limit: None,
                by: Some(user("alice")),
                steps: 1,
                turns: 2,
            },
        ),
        (
            "order_settled",
            StatusUpdate::OrderSettled {
                order: id(20),
                machine: machine(),
                op: Some("restart".to_owned()),
                admin: Some(user("pa")),
                on_behalf_of: Some(user("alice")),
                state: OrderState::Refused,
                reason: Some("denied by alice".to_owned()),
            },
        ),
        (
            "team_changed",
            StatusUpdate::TeamChanged {
                team: user("backend"),
                change: TeamChange::Created,
                entry: Some(Box::new(Team {
                    team: user("backend"),
                    description: Some("The backend team.".to_owned()),
                    members: vec![user("dev-a")],
                    lead: Some(user("dev-a")),
                    owner: Some(user("alice")),
                    avatar: Some("seed".to_owned()),
                    color: Some("teal".to_owned()),
                    created_by: user("lead"),
                    created_at: at(0),
                })),
                parent: Some(user("eng")),
            },
        ),
        (
            "team_changed_deleted",
            StatusUpdate::TeamChanged { team: user("backend"), change: TeamChange::Deleted, entry: None, parent: None },
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
