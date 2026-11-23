//! Snapshots of what is sent on a thread or a channel: envelopes, escalations, notes, channel posts and entries, dead
//! letters and approvals.

use super::*;

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
