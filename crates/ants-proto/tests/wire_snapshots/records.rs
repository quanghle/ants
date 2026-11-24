//! Snapshots of the identifiers, the tags of a user JWT and the records daemons and humans keep: serving, colony
//! entries, reminders, an ant's identity and the entries of the KV buckets.

use super::*;

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
fn user_jwt_tags() {
    let xkey: DeviceKey = "XBLFEIK7UCHY5XKH7LGNQ4JZNVOZEWMIHPZH3LP4JUBJ6KQZDVBEWHWB".parse().unwrap();
    snap(
        "user_jwt_tags",
        &vec![
            PermissionsVersion(1).tag(),
            device().tag(),
            xkey.tag(),
            REGISTERED_TAG.to_owned(),
            ADMIN_TAG.to_owned(),
            WEB_TAG.to_owned(),
            AssistantOf::tag(&user("alice")),
        ],
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
            unserved: Some(Unserved {
                since: at(4),
                error: "the ANTS stream holds its 10000 consumers".to_owned(),
                retry_at: at(7),
            }),
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
fn teams() {
    let team = |id: &str, lead: &str, members: &[&str]| Team {
        team: user(id),
        description: Some(format!("The {id} team.")),
        members: members.iter().map(|m| user(m)).collect(),
        lead: Some(user(lead)),
        owner: Some(user("alice")),
        avatar: Some("seed".to_owned()),
        color: Some("teal".to_owned()),
        created_by: user("alice"),
        created_at: at(0),
    };
    snap(
        "team_directory",
        &TeamDirectory { teams: vec![team("eng", "lead", &["lead", "backend"]), team("backend", "dev-a", &["dev-a"])] },
    );
    let ops = vec![
        TeamOp::AddMember { member: user("dev-b") },
        TeamOp::RemoveMember { member: user("dev-c") },
        TeamOp::SetLead { ant: user("dev-b") },
        TeamOp::SetDescription { description: "Services.".to_owned() },
        TeamOp::ClearDescription,
        TeamOp::CreateTeam { team: user("infra"), lead: user("dev-d"), description: Some("Machines.".to_owned()) },
        TeamOp::DeleteTeam { team: user("infra") },
    ];
    snap(
        "team_admin_request",
        &TeamAdminRequest {
            id: id(1),
            ant: user("lead"),
            team: user("backend"),
            ops,
            last: 42,
            thread_id: Some(thread()),
            requested_at: at(0),
            expires_at: at(1),
        },
    );
    for (name, outcome) in [
        ("team_admin_reply_applied", TeamAdminOutcome::Applied { last: 43, changed: vec!["added dev-b".to_owned()] }),
        ("team_admin_reply_changed", TeamAdminOutcome::Changed),
        (
            "team_admin_reply_refused",
            TeamAdminOutcome::Refused { refusal: TeamRefusal::OnTeam { member: user("dev-b"), team: user("eng") } },
        ),
    ] {
        snap(name, &TeamAdminReply { request_id: id(1), ant: user("lead"), outcome });
    }
}
