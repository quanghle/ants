//! Cancel requests, and an assistant: what it does for its human on the subjects naming it, with its ant's rights
//! and a human's under its own name.

use super::*;

#[test]
fn humans_ask_and_only_the_ant_hears_and_answers_cancel_requests() {
    let t: ThreadId = "t1".parse().unwrap();
    let request = CancelRequest::new(p("dev-a"), t, UserId::human());
    assert_eq!(
        (request.subject(), request.reply_subject()),
        ("ants.cancel.dev-a.t1".into(), "ants.cancelled.dev-a.t1".into())
    );
    assert_eq!(request.check_subject("ants.cancel.dev-a.t1"), Ok(()));
    assert!(request.check_subject("ants.cancel.dev-a.t2").is_err(), "another thread");
    let forged = request.check_subject("ants.cancel.dev-b.t1").unwrap_err();
    assert_eq!(forged.to_string(), "cancel request of dev-a found at ants.cancel.dev-b.t1 (thread t1)");
    let json = serde_json::to_string(&request).unwrap();
    assert_eq!(serde_json::from_str::<CancelRequest>(&json).unwrap(), request);
    let reply = request.answer(true);
    assert_eq!((reply.request_id, reply.cancelled), (request.id, true));
    assert_eq!(reply.subject(), request.reply_subject());
    assert_eq!(reply.check_subject("ants.cancelled.dev-a.t1"), Ok(()));
    assert!(reply.check_subject("ants.cancelled.dev-b.t1").is_err());
    let json = serde_json::to_string(&reply).unwrap();
    assert_eq!(serde_json::from_str::<CancelReply>(&json).unwrap(), reply);

    let human = Permissions::for_user(&UserId::human(), Role::Human);
    assert!(human.publish.iter().any(|s| s == subject::ALL_CANCELS));
    assert!(human.subscribe.iter().any(|s| s == subject::ALL), "the answers");
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert_eq!(
        a.subscribe.iter().filter(|s| s.starts_with("ants.cancel")).collect::<Vec<_>>(),
        ["ants.cancel.dev-a.>"]
    );
    assert_eq!(
        a.publish.iter().filter(|s| s.starts_with("ants.cancel")).collect::<Vec<_>>(),
        ["ants.cancelled.dev-a.*"]
    );
    assert_eq!(subject::outbox(&request.subject()), None, "never stored, so never buffered");
}

/// An assistant's cancel, withdrawal and queue requests for its human go on subjects naming it, and its owner records
/// under keys naming its human: no other credentials publish or write there, so readers know who acted.
#[test]
fn only_an_assistant_acts_for_its_human_on_the_subjects_naming_it() {
    let (t, alice, pa) = ("t1".parse::<ThreadId>().unwrap(), p("alice"), p("pa"));
    let cancel = CancelRequest::new(p("dev-a"), t.clone(), pa.clone()).for_human(alice.clone());
    assert_eq!(cancel.subject(), "ants.cancel.dev-a.t1.pa");
    assert_eq!(cancel.reply_subject(), "ants.cancelled.dev-a.t1");
    assert!(cancel.check_subject("ants.cancel.dev-a.t1").is_err(), "a request for a human names its sender");
    let forged = CancelRequest { by: p("pb"), ..cancel.clone() };
    assert!(forged.check_subject("ants.cancel.dev-a.t1.pa").is_err(), "and its `by` is the sender named");
    let withdrawal = CancelRequest::new(p("dev-a"), t.clone(), pa.clone()).withdrawing(Uuid::now_v7());
    assert_eq!(withdrawal.subject(), "ants.cancel.dev-a.t1.pa", "a withdrawal never reaches a daemon cancelling");
    let queue = QueueRequest::new(p("dev-a"), pa.clone()).for_human(alice.clone());
    assert_eq!((queue.subject(), queue.reply_subject()), ("ants.queue.dev-a.pa".into(), "ants.queued.dev-a".into()));
    let owner = format!("$KV.ants_owners.{}", assisted_owner_key(&t, &p("dev-a"), &alice, &pa));

    let assistant = Permissions::for_assistant(&pa, &alice);
    let acts = [cancel.subject(), queue.subject(), owner];
    for s in &acts {
        assert!(assistant.publish.iter().any(|have| covers(have, s)), "{s} missing from {:?}", assistant.publish);
    }
    let domain: JsDomain = "hub".parse().unwrap();
    let m1: Machine = "m1".parse().unwrap();
    let others = [
        Permissions::for_user(&alice, Role::Human),
        Permissions::for_user(&p("dev-a"), Role::Ant),
        Permissions::for_user(&p("pb"), Role::Ant),
        Permissions::for_assistant(&p("pb"), &p("bob")),
        Permissions::for_assistant(&pa, &alice).with_js_domain(Some(&domain)),
        Permissions::for_host(&m1),
        Permissions::for_channel_manager(),
        Permissions::for_enrollment(&p("dev-a")),
        Permissions::for_webhook(&p("github"), &[Destination::Ant(p("dev-a"))]),
        Permissions::for_observer(&p("watcher")),
        Permissions::for_enroll_approver(),
        Permissions::for_reactions_responder(),
        Permissions::for_trail_keeper(),
        Permissions::for_device_revoker(),
        Permissions::for_bootstrap(),
        Permissions::for_account_holder(),
        Permissions::for_registrar(),
    ];
    for (n, other) in others.iter().enumerate() {
        for s in &acts {
            let held = other.publish.iter().any(|have| covers(have, s));
            assert_eq!(held, n == 4, "credentials {n} publish {s}");
        }
    }
    // A leaf uplink carries them only for the assistant member.
    // Requests naming pa cross the uplink of pa as any human's assistant, whose daemons check for whom it may act;
    // the owner keys of alice only that of pa as hers.
    let bob = p("bob");
    for (role, carries) in [
        (LeafRole::AssistantOf(&alice), [true; 3]),
        (LeafRole::Ant, [false; 3]),
        (LeafRole::AssistantOf(&bob), [true, true, false]),
    ] {
        let leaf = Permissions::for_leaf(&m1, [(&pa, role)]);
        for (s, carries) in acts.iter().zip(carries) {
            assert_eq!(leaf.publish.iter().any(|have| covers(have, s)), carries, "a leaf uplink carrying {s}");
        }
    }
    // The pins, archives and snoozes it sets for its human under keys naming it, never its human's own, and the
    // withdrawals it records under keys naming it.
    let own = ["marks", "reminders"].map(|k| format!("$KV.ants_threads.{k}.alice"));
    let marks = ["marks", "reminders"].map(|k| format!("$KV.ants_threads.{k}.alice.pa"));
    let withdrawal = format!("$KV.ants_attempts.{}", assisted_withdrawal_key(&p("dev-a"), &Uuid::now_v7(), &pa));
    for (n, other) in others.iter().enumerate() {
        let held = |s: &String| other.publish.iter().any(|have| covers(have, s));
        assert!(own.iter().all(|s| held(s) == (n == 0)), "credentials {n} set alice's own marks");
        assert!(marks.iter().all(|s| held(s) == (n == 0 || n == 4)), "credentials {n} set pa's marks for alice");
        assert_eq!(held(&withdrawal), n == 1 || n == 4, "credentials {n} record pa's withdrawal");
    }
    assert!(marks.iter().all(|s| assistant.publish.contains(s)));
    for own in ["marks", "reminders"].map(|k| format!("ants_threads.{k}.pa")) {
        assert!(!assistant.publish.iter().any(|s| s.contains(&own)), "keeps {own}");
    }
    for (role, carries) in
        [(LeafRole::AssistantOf(&alice), true), (LeafRole::Ant, false), (LeafRole::AssistantOf(&bob), false)]
    {
        let leaf = Permissions::for_leaf(&m1, [(&pa, role)]);
        for s in &marks {
            assert_eq!(leaf.publish.iter().any(|have| covers(have, s)), carries, "a leaf uplink carrying {s}");
        }
        let any_assistant = !matches!(role, LeafRole::Ant);
        assert_eq!(leaf.publish.iter().any(|have| covers(have, &withdrawal)), any_assistant, "{withdrawal}");
    }
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    for heard in [cancel.subject(), "ants.cancel.dev-a.t1".to_owned(), queue.subject(), "ants.queue.dev-a".to_owned()] {
        assert!(a.subscribe.iter().any(|have| covers(have, &heard)), "the ant's daemon hears {heard}");
    }
}

#[test]
fn an_assistant_keeps_its_ant_rights_and_a_humans_under_its_own_name() {
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    let has = |s: &str| assistant.publish.iter().any(|have| have == s);
    let ant = Permissions::for_user(&p("pa"), Role::Ant);
    // It reads by direct gets: of an ant's readers it keeps only its dead letters'.
    let readers = ["notes-pa", "chanread-pa", "sentread-pa", "inbox-pa_", "notes-pa_", "chanread-pa_", "notify-pa"];
    let through_reader = |s: &str| s.split('.').any(|token| readers.iter().any(|r| token.starts_with(r)));
    for s in &ant.publish {
        if through_reader(s) {
            assert!(!assistant.publish.iter().any(|have| covers(have, s)), "reader right {s} kept");
        } else {
            assert!(assistant.publish.iter().any(|have| covers(have, s)), "ant right {s} missing");
        }
    }
    assert!(!assistant.publish.iter().any(|s| through_reader(s)), "no reader of its own: {:?}", assistant.publish);
    for s in ["$JS.API.CONSUMER.MSG.NEXT.ANTS.inbox-pa", "$JS.API.CONSUMER.MSG.NEXT.ANTS.chan-pa"] {
        assert!(has(s), "{s} missing");
    }
    assert!(assistant.publish.iter().any(|s| s.contains(".deadread-pa")), "reads its dead letters");
    for s in &ant.subscribe {
        assert!(assistant.subscribe.iter().any(|have| covers(have, s)), "ant subscription {s} missing");
    }
    assert!(assistant.subscribe.iter().any(|s| s == subject::ALL));
    for s in [
        "$JS.API.STREAM.MSG.GET.ANTS",
        "$JS.API.DIRECT.GET.ANTS",
        "$JS.API.STREAM.INFO.ANTS",
        "$JS.API.STREAM.INFO.KV_ants_devices",
        "$JS.API.CONSUMER.CREATE.KV_ants_devices",
        "$KV.ants_threads.title.*",
        "$KV.ants_threads.done.*",
        "$KV.ants_reactions.pa.>",
        "$KV.ants_read.pa",
        "$JS.API.DIRECT.GET.KV_ants_devices.>",
        "$JS.API.DIRECT.GET.OBJ_ants_files.$O.ants_files.C.>",
        "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice",
        "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice.chan",
        "$JS.API.DIRECT.GET.KV_ants_attempts.$KV.ants_attempts.alice.unserved.*",
    ] {
        assert!(assistant.publish.iter().any(|have| covers(have, s)), "{s} missing from {:?}", assistant.publish);
    }
    let human = Permissions::for_user(&p("alice"), Role::Human);
    // A human's sending under any name, acts readers could not tell from its human's, and changes to the team.
    let human_only = [
        "ants.inbox.>",
        "ants.out.inbox.>",
        "$O.ants_files.C.>",
        "ants.out.files.>",
        "ants.approvals.*.*",
        "ants.cancel.*.*",
        "ants.queue.*",
        "ants.trail.*",
        "$KV.ants_attempts.*.msg.*",
        "$KV.ants_owners.*.*",
        "ants.out.owners.*.*",
        "$KV.ants_humans.*",
        "$KV.ants_channels.>",
        "$KV.ants_teams.directory",
        "$JS.API.STREAM.UPDATE.ANTS",
        "$JS.API.STREAM.MSG.DELETE.ANTS",
        "$JS.API.STREAM.CREATE.KV_ants_read",
    ];
    for s in human_only {
        assert!(human.publish.iter().any(|have| have == s), "{s} is a human's");
        assert!(!assistant.publish.iter().any(|have| covers(have, s)), "{s} is not an assistant's");
    }
    // Of its human's keys it only reads but the pins, archives and snoozes it sets for it; it moves no device's key,
    // presence or notifications.
    for s in &assistant.publish {
        let key = s.strip_prefix("$JS.API.DIRECT.GET.").and_then(|get| get.split_once('.')).map_or(s.as_str(), |g| g.1);
        let owner = s == "$KV.ants_owners.*.*.alice.pa";
        let marks = ["$KV.ants_threads.marks.alice.pa", "$KV.ants_threads.reminders.alice.pa"].contains(&s.as_str());
        assert!(owner || marks || s.starts_with("$JS.API.DIRECT.GET.") || !s.contains(".alice"), "writes {s}");
        assert!(!key.starts_with("$KV.ants_reactions.alice"), "reads {s}");
        for bucket in ["ants_devices", "ants_presence", "ants_notify", "ants_humans"] {
            // Save the record of its own credentials, which readers check its acts for its human against.
            let record = s == "$KV.ants_devices.pa.assistant";
            assert!(record || !s.starts_with(&format!("$KV.{bucket}.")), "puts {s}");
        }
    }
    assert!(has("$KV.ants_devices.pa.assistant"));
    assert!(!ant.publish.iter().any(|s| s.starts_with("$KV.ants_devices.")), "an ant keeps no record");
    let unique: std::collections::HashSet<&String> = assistant.publish.iter().collect();
    assert_eq!(unique.len(), assistant.publish.len(), "no subject twice");
}
