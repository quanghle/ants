use chrono::Utc;

use super::*;
use crate::{
    ANTS_VERSION, AntReply, AntRequest, CancelReply, CancelRequest, ChainEntry, ChannelEntry, ChannelOp, DeviceId,
    HumanEntry, Humans, Refusal, SPARE_READERS, Serving, ThreadId, channel_manager_serving_key, p,
};

#[test]
fn credentials_ask_only_for_their_own_renewal_and_leaf_nodes_carry_any() {
    let (dev_a, alice) = (p("dev-a"), p("alice"));
    let ant = Permissions::for_user(&dev_a, Role::Ant);
    let renewals = |list: &[String]| -> Vec<String> {
        list.iter().filter(|s| s.starts_with("ants.renew.") || *s == subject::ALL).cloned().collect()
    };
    assert_eq!(renewals(&ant.publish), ["ants.renew.dev-a"]);
    assert_eq!(renewals(&ant.subscribe), ["ants.renew.dev-a.reply"]);
    // A human carries the renewal of its machine's leaf node uplink, and hears every answer anyway.
    let human = Permissions::for_user(&alice, Role::Human);
    assert_eq!(renewals(&human.publish), [subject::ALL_RENEW_REQUESTS]);
    assert_eq!(renewals(&human.subscribe), [subject::ALL]);
    let manager = Permissions::for_channel_manager();
    assert_eq!(
        (renewals(&manager.publish), renewals(&manager.subscribe)),
        (vec!["ants.renew.channel-manager".to_owned()], vec!["ants.renew.channel-manager.reply".to_owned()])
    );
    let enrollment = Permissions::for_enrollment(&dev_a);
    assert!(renewals(&enrollment.publish).is_empty() && renewals(&enrollment.subscribe).is_empty());
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&dev_a, Role::Ant), (&alice, Role::Human)]);
    assert_eq!(leaf.publish.iter().filter(|s| *s == subject::ALL_RENEW_REQUESTS).count(), 1);
    assert!(leaf.subscribe.iter().any(|s| s == subject::ALL_RENEW_REPLIES));
    let approver = Permissions::for_enroll_approver();
    assert!(approver.subscribe.iter().any(|s| s == subject::ALL_RENEW_REQUESTS));
    assert!(approver.publish.iter().any(|s| s == subject::ALL_RENEW_REPLIES));
    assert!(!approver.publish.iter().any(|s| s == subject::ALL_RENEW_REQUESTS));
}

#[test]
fn ants_ask_only_for_their_own_ant_requests_and_the_approver_answers_them() {
    let (lead, other) = (p("lead"), p("dev-b"));
    let ant = Permissions::for_user(&lead, Role::Ant);
    let asks =
        |list: &[String]| -> Vec<String> { list.iter().filter(|s| s.starts_with("ants.antreq.")).cloned().collect() };
    assert_eq!(asks(&ant.publish), ["ants.antreq.lead"]);
    assert_eq!(asks(&ant.subscribe), ["ants.antreq.lead.reply"]);
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&lead, Role::Ant), (&other, Role::Ant)]);
    assert_eq!(asks(&leaf.publish), ["ants.antreq.lead", "ants.antreq.dev-b"]);
    let approver = Permissions::for_enroll_approver();
    assert_eq!(asks(&approver.subscribe), [subject::ALL_ANT_REQUESTS]);
    assert_eq!(asks(&approver.publish), [subject::ALL_ANT_REPLIES]);
    assert!(approver.publish.iter().any(|s| s == subject::ALL_APPROVAL_REQUESTS));
    assert!(
        !approver.publish.iter().any(|s| s == subject::ALL_APPROVAL_DECISIONS),
        "only humans and the escalation chain decide"
    );
    let request = AntRequest::new(
        lead.clone(),
        p("lead-w1"),
        "worker",
        "{}",
        "more hands",
        "t1".parse().unwrap(),
        "UKEY",
        "host",
        Utc::now(),
    );
    assert_eq!(request.subject(), "ants.antreq.lead");
    assert!(request.check_subject("ants.antreq.dev-b").is_err());
    let mut moved = request.clone();
    moved.reason = "more hands\n".into();
    assert_ne!(moved.signed(), request.signed());
    let retiring = AntRequest { retire: true, ..request.clone() };
    assert_ne!(retiring.signed(), request.signed(), "an addition never passes for a retirement");
    let reply = AntReply::Refused { manager: lead, name: p("lead-w1"), public_key: "UKEY".into(), reason: "no".into() };
    assert_eq!(reply.subject(), "ants.antreq.lead.reply");
    assert!(reply.answers(&p("lead-w1"), "UKEY") && !reply.answers(&p("lead-w2"), "UKEY"));
}

#[test]
fn a_js_domain_adds_the_domain_form_of_the_jetstream_api() {
    let domain: JsDomain = "hub".parse().unwrap();
    let plain = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert_eq!(plain.clone().with_js_domain(None), plain);
    let Permissions { publish, subscribe } = plain.clone().with_js_domain(Some(&domain));
    assert_eq!(subscribe, plain.subscribe);
    assert_eq!(publish[..plain.publish.len()], plain.publish, "the hub checks the mapped subjects too");
    for s in [
        "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.inbox-dev-a",
        "$JS.hub.API.DIRECT.GET.KV_ants_sessions.$KV.ants_sessions.dev-a.>",
        "$JS.hub.API.$KV.ants_sessions.dev-a.>",
        "$JS.hub.API.$KV.ants_chains.dev-a",
        "$JS.hub.API.DIRECT.GET.KV_ants_chains.>",
        "$JS.hub.API.$KV.ants_roster.dev-a",
        "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.notes-dev-a",
        "$JS.ACK.ANTS.inbox-dev-a.>",
        // The v2 form of nats-server 2.16, whatever the domain.
        "$JS.ACK.*.*.ANTS.inbox-dev-a.>",
        "ants.notes.*.dev-a",
    ] {
        assert!(publish.iter().any(|have| have == s), "{s} missing from {publish:?}");
    }
    assert!("hub.x".parse::<JsDomain>().is_err());
}

#[test]
fn leaf_permissions_cover_each_user_once() {
    let (human, a, b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let m1: Machine = "m1".parse().unwrap();
    let all = [(&a, Role::Ant), (&b, Role::Ant), (&human, Role::Human)];
    let leaf = Permissions::for_leaf(&m1, all);
    for (user, role) in all {
        let own = Permissions::for_user(user, role);
        let crossing = own.publish.iter().filter(|s| !s.starts_with("ants.out."));
        assert!(crossing.into_iter().all(|s| leaf.publish.contains(s)), "{user} publish");
        assert!(own.subscribe.iter().all(|s| leaf.subscribe.contains(s)), "{user} subscribe");
    }
    assert_eq!(leaf.publish.iter().filter(|s| *s == "$JS.API.STREAM.INFO.ANTS").count(), 1);
    assert!(leaf.publish.iter().all(|s| !s.starts_with("ants.out.")), "the outbox stays on the leaf");
    // The hub may source the outbox of m1, and the leaf deliver it on m1's subjects only.
    let only_a = Permissions::for_leaf(&m1, [(&a, Role::Ant)]);
    let own = Permissions::for_user(&a, Role::Ant);
    let extra: Vec<_> = only_a.publish.iter().filter(|s| !own.publish.contains(s)).collect();
    assert_eq!(
        extra,
        [
            // Already allowed by `ants.inbox.*.dev-a`, `ants.notes.*.dev-a`, `ants.chan.*.dev-a` and
            // `ants.chanadm.*.dev-a`.
            "ants.inbox.dev-a.dev-a",
            "ants.notes.dev-a.dev-a",
            "ants.chan.dev-a.dev-a",
            "ants.chanadm.dev-a.dev-a",
            "$JS.S.m1.>",
            "$JSC.R.m1.*.ANTS_OUT.>",
            "_INBOX.human.leaf.m1.>",
            // Asking for a new ant's credentials, and for any renewal on the machine.
            "ants.enroll.*",
            "ants.renew.*",
        ]
    );
    let mut extra: Vec<_> = only_a.subscribe.iter().filter(|s| !own.subscribe.contains(s)).collect();
    assert_eq!(extra.pop().map(String::as_str), Some("ants.renew.*.reply"), "hearing the renewals");
    assert_eq!(extra.pop().map(String::as_str), Some("ants.enroll.*.reply"), "hearing the answer");
    let fc = ["$JS.FC.ANTS_OUT.>", "$JS.FC.m1.*.ANTS_OUT.>"];
    assert!(fc.iter().all(|s| extra.contains(&&s.to_string())), "both forms in m1's domain: {extra:?}");
    assert!(extra.iter().all(|s| s.starts_with("$JS.m1.API.") || fc.contains(&s.as_str())), "{extra:?}");
    assert!(extra.iter().all(|s| s.contains(".ANTS_OUT")), "{extra:?}");
    // Never hearing the requests of other machines, nor answering them, without a human (who watches all).
    assert!(!only_a.subscribe.iter().any(|s| s == subject::ALL_ENROLL_REQUESTS || s == ">" || s == "ants.>"));
    assert!(!leaf.publish.iter().any(|s| s == subject::ALL_ENROLL_REPLIES || s == ">" || s == "ants.>"));
}

#[test]
fn outboxes_take_inbox_notes_channel_and_dead_letter_subjects_only() {
    let t: ThreadId = "t1".parse().unwrap();
    let c: ChannelId = "ops".parse().unwrap();
    assert_eq!(subject::outbox(&subject::inbox(&p("dev-a"), &p("dev-b"))).unwrap(), "ants.out.inbox.dev-a.dev-b");
    assert_eq!(subject::outbox(&subject::notes(&t, &p("dev-a"))).unwrap(), "ants.out.notes.t1.dev-a");
    assert_eq!(subject::outbox(&subject::chan(&c, &p("dev-a"))).unwrap(), "ants.out.chan.ops.dev-a");
    assert_eq!(subject::outbox(&subject::dead(&p("dev-a"))).unwrap(), "ants.out.dead.dev-a");
    for other in [subject::approvals(&p("dev-a")), subject::roster(&p("dev-a")), "ants.outbox.x".to_owned()] {
        assert_eq!(subject::outbox(&other), None, "{other}");
    }
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    for s in ["ants.out.inbox.*.dev-a", "ants.out.notes.*.dev-a", "ants.out.chan.*.dev-a", "ants.out.dead.dev-a"] {
        assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
    }
    assert!(!a.publish.iter().any(|s| s.starts_with("ants.out.approvals")));
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    assert!(human.publish.iter().any(|s| s == "ants.out.inbox.>"));
    assert!(human.publish.iter().any(|s| s == "ants.out.chan.*.human"));
    assert!(!human.publish.iter().any(|s| s.starts_with("ants.out.dead")), "a human's dead letters need the hub");
}

#[test]
fn the_hub_admits_from_a_machine_what_its_users_may_send() {
    let m1: Machine = "m1".parse().unwrap();
    assert_eq!(
        m1.admitted(&p("dev-a"), Role::Ant, true),
        [
            ("ants.in.m1.ants.out.inbox.*.dev-a".to_owned(), "ants.inbox.{{wildcard(1)}}.dev-a".to_owned()),
            ("ants.in.m1.ants.out.notes.*.dev-a".to_owned(), "ants.notes.{{wildcard(1)}}.dev-a".to_owned()),
            ("ants.in.m1.ants.out.chan.*.dev-a".to_owned(), "ants.chan.{{wildcard(1)}}.dev-a".to_owned()),
            ("ants.in.m1.ants.out.dead.dev-a".to_owned(), "ants.dead.dev-a".to_owned()),
        ]
    );
    let human = m1.admitted(&UserId::human(), Role::Human, true);
    assert_eq!(human[0], ("ants.in.m1.ants.out.inbox.>".to_owned(), "ants.inbox.>".to_owned()));
    assert!(human.iter().all(|(filter, _)| !filter.contains(".dead.")), "no dead letters of a human");
    let rest: Vec<_> = m1.admitted(&p("dev-a"), Role::Ant, false).into_iter().map(|(filter, _)| filter).collect();
    assert_eq!(
        rest,
        ["ants.in.m1.ants.out.notes.*.dev-a", "ants.in.m1.ants.out.chan.*.dev-a", "ants.in.m1.ants.out.dead.dev-a"],
        "no inbox subjects"
    );
    assert_eq!(
        m1.admitted_owners(),
        ("ants.in.m1.ants.out.owners.*.*".to_owned(), "$KV.ants_owners.{{wildcard(1)}}.{{wildcard(2)}}".to_owned())
    );
    assert_eq!(subject::intake(&m1), "ants.in.m1.>");
}

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
        ["ants.cancel.dev-a.*"]
    );
    assert_eq!(
        a.publish.iter().filter(|s| s.starts_with("ants.cancel")).collect::<Vec<_>>(),
        ["ants.cancelled.dev-a.*"]
    );
    assert_eq!(subject::outbox(&request.subject()), None, "never stored, so never buffered");
}

#[test]
fn a_webhook_may_post_only_to_its_destinations_and_keep_only_its_own_events() {
    let hook = p("github");
    let ops: ChannelId = "ops".parse().unwrap();
    let perms = Permissions::for_webhook(&hook, &[Destination::Ant(p("dev-a")), Destination::Channel(ops)]);
    assert_eq!(
        perms.publish,
        [
            "ants.inbox.dev-a.github",
            "ants.chan.ops.github",
            "$JS.API.DIRECT.GET.KV_ants_channels.$KV.ants_channels.ops",
            "$JS.API.CONSUMER.INFO.ANTS.inbox-github",
            "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.github.>",
            "$KV.ants_webhooks.github.>",
            "$KV.ants_owners.*.dev-a",
            "ants.renew.github",
        ]
    );
    assert_eq!(perms.subscribe, ["_INBOX.github.>", "ants.renew.github.reply"]);
    let to_channel = Permissions::for_webhook(&hook, &[Destination::Channel("ops".parse().unwrap())]);
    assert!(!to_channel.publish.iter().any(|s| s.contains("ants_owners")), "posts' owners come from the posts");
    let domain: JsDomain = "hub".parse().unwrap();
    let in_domain = perms.with_js_domain(Some(&domain));
    assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_webhooks.github.>"));
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    assert!(human.publish.iter().any(|s| s == "$JS.API.STREAM.CREATE.KV_ants_webhooks"), "ants init creates it");
}

#[test]
fn an_observer_publishes_nothing_but_reads_its_feeds_status_and_the_roster() {
    let perms = Permissions::for_observer(&p("audit"));
    let ants: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("ants.")).collect();
    assert_eq!(ants, ["ants.renew.audit"], "no message, approval decision or cancel request");
    assert!(!perms.publish.iter().any(|s| s.starts_with("$KV.")), "no bucket writes");
    let creates: Vec<&str> =
        perms.publish.iter().filter_map(|s| s.strip_prefix("$JS.API.CONSUMER.CREATE.ANTS.")).collect();
    assert_eq!(
        creates,
        [
            "observe-requests-audit.ants.approvals.*",
            "observe-decisions-audit.ants.approvals.*.*",
            "observe-dead-audit.ants.dead.>"
        ]
    );
    assert!(!perms.publish.iter().any(|s| s.contains("STREAM.INFO.ANTS") || s.contains("DIRECT.GET.ANTS")));
    assert!(!perms.publish.iter().any(|s| s.contains("STREAM.MSG.GET")));
    let buckets: Vec<&String> = perms.publish.iter().filter(|s| s.contains("KV_")).collect();
    assert!(buckets.iter().all(|s| s.contains("KV_ants_roster")), "{buckets:?}");
    let acks: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("$JS.ACK.")).collect();
    assert_eq!(acks.len(), 6, "v1 and v2 acks of its own consumers only: {acks:?}");
    assert!(acks.iter().all(|s| s.contains(".ANTS.observe-") && s.ends_with("-audit.>")), "{acks:?}");
    let fc: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("$JS.FC.")).collect();
    assert_eq!(fc, ["$JS.FC.KV_ants_roster.>", "$JS.FC.*.*.KV_ants_roster.>"]);
    assert_eq!(
        perms.subscribe,
        [
            "_INBOX.audit.>",
            "ants.approvals.*",
            "ants.approvals.*.*",
            "ants.dead.>",
            "ants.status.*.*",
            "ants.renew.audit.reply"
        ]
    );
}

#[test]
fn only_humans_get_the_info_of_the_stream_and_of_the_webhooks_bucket() {
    let infos = |perms: &Permissions| -> Vec<String> {
        perms
            .publish
            .iter()
            .filter(|s| s.ends_with(".STREAM.INFO.ANTS") || s.ends_with(".STREAM.INFO.KV_ants_webhooks"))
            .cloned()
            .collect()
    };
    let hook = Permissions::for_webhook(&p("github"), &[Destination::Ant(p("dev-a"))]);
    for perms in [Permissions::for_user(&p("dev-a"), Role::Ant), hook, Permissions::for_channel_manager()] {
        let domain: JsDomain = "hub".parse().unwrap();
        assert_eq!(infos(&perms.with_js_domain(Some(&domain))), Vec::<String>::new());
    }
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    assert_eq!(infos(&human), ["$JS.API.STREAM.INFO.ANTS", "$JS.API.STREAM.INFO.KV_ants_webhooks"]);
    // Each finds JetStream on connecting through the info of a consumer named after it.
    let manager = Permissions::for_channel_manager();
    assert!(manager.publish.iter().any(|s| s == "$JS.API.CONSUMER.INFO.ANTS.inbox-channel-manager"));
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(ant.publish.iter().any(|s| s == "$JS.API.CONSUMER.INFO.ANTS.inbox-dev-a"));
}

#[test]
fn channels_are_readable_by_ants_and_written_by_humans_only() {
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    for s in [
        "ants.chan.*.dev-a",
        "ants.out.chan.*.dev-a",
        "$JS.API.STREAM.INFO.KV_ants_channels",
        "$JS.API.DIRECT.GET.KV_ants_channels.>",
        "$JS.API.CONSUMER.CREATE.KV_ants_channels",
        "$JS.API.CONSUMER.CREATE.ANTS.chan-dev-a.ants.chan.>",
        "$JS.API.CONSUMER.CREATE.ANTS.chanread-dev-a.ants.chan.>",
        "$JS.ACK.ANTS.chan-dev-a.>",
        "$JS.ACK.*.*.ANTS.chan-dev-a.>",
    ] {
        assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
    }
    assert!(a.subscribe.iter().any(|s| s == subject::ALL_CHANNELS));
    assert!(!a.publish.iter().any(|s| s.contains("$KV.ants_channels") || s.contains("CREATE.KV_ants_channels.")));
    assert!(!a.publish.iter().any(|s| s == "ants.chan.>" || s == "$JS.API.CONSUMER.CREATE.ANTS.chan-dev-a"));
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    for s in ["ants.chan.*.human", "$KV.ants_channels.>", "$JS.API.STREAM.CREATE.KV_ants_channels"] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let domain: JsDomain = "hub".parse().unwrap();
    let in_domain = human.with_js_domain(Some(&domain));
    assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_channels.>"));
    let alice = Permissions::for_user(&p("alice"), Role::Human);
    for s in ["ants.chan.*.alice", "$KV.ants_channels.>"] {
        assert!(alice.publish.iter().any(|have| have == s), "any human: {s} missing from {:?}", alice.publish);
    }
}

#[test]
fn only_ants_get_spare_readers_pinned_like_their_own() {
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert_eq!(
        spare_readers("chanread-dev-a").collect::<Vec<_>>(),
        ["chanread-dev-a_1", "chanread-dev-a_2", "chanread-dev-a_3"]
    );
    for (reader, filter) in
        [("inbox-dev-a", "ants.inbox.dev-a.*"), ("notes-dev-a", "ants.notes.>"), ("chanread-dev-a", "ants.chan.>")]
    {
        for spare in spare_readers(reader) {
            for s in [
                format!("$JS.API.CONSUMER.CREATE.ANTS.{spare}.{filter}"),
                format!("$JS.API.CONSUMER.MSG.NEXT.ANTS.{spare}"),
                format!("$JS.API.CONSUMER.DELETE.ANTS.{spare}"),
            ] {
                assert!(a.publish.contains(&s), "{s} missing from {:?}", a.publish);
            }
        }
    }
    let spares = a.publish.iter().filter(|s| s.contains("-dev-a_")).count();
    assert_eq!(spares, 3 * 3 * SPARE_READERS, "no spare with any other filter, and none acks");
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    assert!(!human.publish.iter().any(|s| s.contains("-human_")));
    let named_like = |spare: String| spare.strip_prefix("chanread-").is_some_and(|user| user.parse::<UserId>().is_ok());
    assert!(!spare_readers("chanread-dev-a").any(named_like), "no spare is another user's reader");
}

#[test]
fn only_the_owner_changes_members_and_messenger_and_never_the_owner() {
    let mut humans = Humans::default();
    humans.add("alice", HumanEntry::new(p("alice"))).unwrap();
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.add_member(p("dev-a"));
    let ops = [
        ChannelOp::AddMember { ant: p("dev-b") },
        ChannelOp::AddMember { ant: p("dev-a") },
        ChannelOp::RemoveMember { ant: p("dev-a") },
        ChannelOp::SetMessenger { ant: p("dev-c") },
    ];
    let before = entry.clone();
    assert_eq!(entry.apply(&p("lead"), &ops, &humans), Err(Refusal::NotOwner { owner: None }));
    entry.owner = Some(p("lead"));
    let refused = entry.apply(&p("dev-a"), &ops, &humans).unwrap_err();
    assert_eq!(refused.to_string(), "only the channel's owner may change it; its owner is lead");
    let human = [ChannelOp::AddMember { ant: p("dev-d") }, ChannelOp::SetMessenger { ant: p("alice") }];
    assert_eq!(entry.apply(&p("lead"), &human, &humans), Err(Refusal::Human(p("alice"))));
    assert_eq!(entry, ChannelEntry { owner: Some(p("lead")), ..before }, "a refusal changes nothing");
    let member = [ChannelOp::AddMember { ant: p("alice") }];
    assert_eq!(entry.apply(&p("lead"), &member, &humans).unwrap(), ["added alice"], "a human may be a member");
    let member = [ChannelOp::RemoveMember { ant: p("alice") }];
    assert_eq!(entry.apply(&p("lead"), &member, &humans).unwrap(), ["removed alice"]);

    let changed = entry.apply(&p("lead"), &ops, &humans).unwrap();
    assert_eq!(changed, ["added dev-b", "removed dev-a", "messenger dev-c"]);
    assert_eq!((entry.members.as_slice(), entry.messenger.as_ref()), ([p("dev-b")].as_slice(), Some(&p("dev-c"))));
    assert_eq!(entry.owner, Some(p("lead")));
    assert!(entry.apply(&p("lead"), &ops[..1], &humans).unwrap().is_empty(), "nothing to change");
    let cleared = entry.apply(&p("lead"), &[ChannelOp::ClearMessenger], &humans).unwrap();
    assert_eq!((cleared, entry.messenger.as_ref()), (vec!["messenger back to the default messenger".to_owned()], None));
    let json = serde_json::to_string(&entry).unwrap();
    assert!(json.contains(r#""owner":"lead""#), "{json}");
    assert!(!serde_json::to_string(&ChannelEntry::new(entry.channel.clone())).unwrap().contains("owner"));
}

#[test]
fn the_channel_manager_hears_requests_and_writes_channels_only() {
    let manager = Permissions::for_channel_manager();
    assert_eq!(manager.subscribe, ["ants.chanadm.*.*", "_INBOX.channel-manager.>", "ants.renew.channel-manager.reply"]);
    for s in [
        "ants.chan.*.channel-manager",
        "$KV.ants_channels.>",
        "$JS.API.DIRECT.GET.KV_ants_channels.>",
        "$JS.API.DIRECT.GET.KV_ants_roster.>",
        "$JS.API.CONSUMER.CREATE.KV_ants_humans",
    ] {
        assert!(manager.publish.iter().any(|have| have == s), "{s} missing from {:?}", manager.publish);
    }
    let writes: Vec<_> = manager.publish.iter().filter(|s| s.starts_with("$KV.") || s.starts_with("ants.")).collect();
    assert_eq!(
        writes,
        [
            "ants.chan.*.channel-manager",
            "$KV.ants_channels.>",
            "$KV.ants_webhooks.channel-manager.serve",
            "ants.renew.channel-manager"
        ]
    );
    assert!(!manager.publish.iter().any(|s| s.starts_with("_INBOX") || s.contains("STREAM.CREATE")));
    // Humans read its version; ants, which read only markers (`*.webhook`), do not take it for a webhook.
    let read = "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.channel-manager.serve";
    assert!(Permissions::for_user(&UserId::human(), Role::Human).publish.iter().any(|s| s == read));
    assert!(!channel_manager_serving_key().ends_with(&format!(".{WEBHOOK_MARKER}")));
    let marker = Serving::here("m1");
    let json = serde_json::to_string(&marker).unwrap();
    assert_eq!(json, format!(r#"{{"machine":"m1","version":"{ANTS_VERSION}","perms":{PERMISSIONS_VERSION}}}"#));
    assert_eq!(serde_json::from_str::<Serving>(&json).unwrap(), marker);

    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(a.publish.iter().any(|s| s == "ants.chanadm.*.dev-a"));
    assert!(!a.publish.iter().any(|s| s.starts_with("ants.out.chanadm")), "no outbox form");
    assert!(!a.subscribe.iter().any(|s| s.starts_with("ants.chanadm")));
}

#[test]
fn every_human_has_the_human_rights_and_only_humans_write_the_humans_bucket() {
    let alice = Permissions::for_user(&p("alice"), Role::Human);
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    let rename = |s: &String| s.replace(".human", ".alice").replace("-human", "-alice");
    assert_eq!(alice.publish, human.publish.iter().map(rename).collect::<Vec<_>>(), "the same rights by any name");
    assert_eq!(alice.subscribe, human.subscribe.iter().map(rename).collect::<Vec<_>>());
    for s in [
        "ants.inbox.>",
        "$JS.API.STREAM.CREATE.ANTS",
        "$JS.API.STREAM.MSG.GET.ANTS",
        "$JS.API.DIRECT.GET.ANTS",
        "ants.approvals.*.*",
        "ants.dead.>",
        "$JS.API.STREAM.CREATE.KV_ants_humans",
        "$KV.ants_humans.>",
        "$JS.API.CONSUMER.CREATE.ANTS.inbox-alice.ants.inbox.alice.*",
        "$JS.API.CONSUMER.CREATE.ANTS.notes-alice.ants.notes.>",
    ] {
        assert!(alice.publish.iter().any(|have| have == s), "{s} missing from {:?}", alice.publish);
    }
    assert!(alice.subscribe.iter().any(|s| s == subject::ALL));

    let reads = [
        "$JS.API.STREAM.INFO.KV_ants_humans",
        "$JS.API.DIRECT.GET.KV_ants_humans.>",
        "$JS.API.CONSUMER.CREATE.KV_ants_humans",
        "$JS.FC.KV_ants_humans.>",
        "$JS.FC.*.*.KV_ants_humans.>",
    ];
    let named_human = Permissions::for_user(&UserId::human(), Role::Ant);
    for ant in [Permissions::for_user(&p("dev-a"), Role::Ant), named_human] {
        for s in reads {
            assert!(ant.publish.iter().any(|have| have == s), "{s} missing from {:?}", ant.publish);
        }
        assert!(
            !ant.publish.iter().any(|s| s.contains("$KV.ants_humans") || s.contains("STREAM.CREATE.KV_ants_humans"))
        );
        let human_only = ["ants.inbox.>", "ants.dead.>", "$JS.API.DIRECT.GET.ANTS"];
        assert!(!ant.publish.iter().any(|s| human_only.contains(&s.as_str())), "no human rights");
    }

    let m1: Machine = "m1".parse().unwrap();
    assert_eq!(m1.setup_inbox(&p("alice")), "_INBOX.alice.leaf.m1");
    let leaf = Permissions::for_leaf(&m1, [(&p("alice"), Role::Human), (&p("dev-a"), Role::Ant)]);
    for s in ["_INBOX.alice.leaf.m1.>", "_INBOX.human.leaf.m1.>", "$KV.ants_humans.>"] {
        assert!(leaf.publish.iter().any(|have| have == s), "{s} missing from {:?}", leaf.publish);
    }
}

#[test]
fn humans_keep_only_their_own_holds_and_held_notices_and_ants_neither() {
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in [
        "$KV.ants_notify.alice",
        "$JS.API.DIRECT.GET.KV_ants_notify.$KV.ants_notify.alice",
        "$KV.ants_notify.held.alice",
        "$JS.API.DIRECT.GET.KV_ants_notify.$KV.ants_notify.held.alice",
    ] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let notify: Vec<&String> = human.publish.iter().filter(|s| s.contains("$KV.ants_notify")).collect();
    assert!(notify.iter().all(|s| s.ends_with(".alice") && !s.contains('*') && !s.contains('>')), "{notify:?}");
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(!ant.publish.iter().any(|s| s.contains("ants_notify")));
}

#[test]
fn humans_set_every_title_and_done_state_but_only_their_own_pins_and_archives_and_ants_neither() {
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in [
        "$KV.ants_threads.title.*",
        "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.title.*",
        "$KV.ants_threads.done.*",
        "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.done.*",
        "$KV.ants_threads.marks.alice",
        "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.marks.alice",
        "$JS.API.STREAM.CREATE.KV_ants_threads",
    ] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let threads: Vec<&String> = human.publish.iter().filter(|s| s.contains("ants_threads")).collect();
    assert!(threads.iter().all(|s| !s.contains("marks") || s.ends_with("marks.alice")), "{threads:?}");
    assert!(!threads.iter().any(|s| s.ends_with("ants_threads.>") || s.contains("CONSUMER")), "{threads:?}");
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let observer = Permissions::for_observer(&p("watcher"));
    for none in [ant, observer] {
        assert!(!none.publish.iter().chain(&none.subscribe).any(|s| s.contains("ants_threads")));
    }
}

#[test]
fn everyone_writes_only_their_own_reactions_and_only_humans_read_others() {
    let own = "$KV.ants_reactions.dev-a.>";
    let others = ["$JS.API.DIRECT.GET.KV_ants_reactions.>", "$JS.API.CONSUMER.CREATE.KV_ants_reactions"];
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(ant.publish.iter().any(|s| s == own), "{:?}", ant.publish);
    assert!(ant.publish.iter().any(|s| s == "$JS.API.DIRECT.GET.KV_ants_reactions.$KV.ants_reactions.dev-a.>"));
    let reactions: Vec<&String> = ant.publish.iter().filter(|s| s.contains("ants_reactions")).collect();
    assert_eq!(reactions.len(), 2, "nothing of others' reactions: {reactions:?}");
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert!(human.publish.iter().any(|s| s == "$KV.ants_reactions.alice.>"));
    assert!(!human.publish.iter().any(|s| s == "$KV.ants_reactions.>" || s == "$KV.ants_reactions.*.>"));
    for s in others.into_iter().chain(["$JS.API.STREAM.CREATE.KV_ants_reactions"]) {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let observer = Permissions::for_observer(&p("watcher"));
    assert!(!observer.publish.iter().chain(&observer.subscribe).any(|s| s.contains("ants_reactions")));

    // Ants ask for the reactions to messages they read only as themselves, and only humans answer.
    let asks = |list: &[String]| -> Vec<String> {
        list.iter().filter(|s| s.starts_with("ants.reactions.")).cloned().collect()
    };
    assert_eq!(asks(&ant.publish), ["ants.reactions.dev-a"]);
    assert!(ant.subscribe.iter().any(|s| s == "_INBOX.dev-a.>"), "hears the answers in its reply inboxes");
    assert!(human.publish.iter().any(|s| s == subject::ALL_REACTIONS_REPLIES), "only humans answer");
    for other in [&observer, &Permissions::for_user(&p("dev-b"), Role::Ant)] {
        assert!(!other.publish.iter().any(|s| s.starts_with("_INBOX.*")), "{:?}", other.publish);
    }
    assert!(human.subscribe.iter().any(|s| s == subject::ALL), "hears the requests");
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&p("dev-a"), Role::Ant), (&p("alice"), Role::Human)]);
    assert!(leaf.publish.iter().any(|s| s == "ants.reactions.dev-a"), "an ant on a leaf asks through its uplink");
    assert!(leaf.publish.iter().any(|s| s == subject::ALL_REACTIONS_REPLIES), "and a human there answers");
    assert!(leaf.subscribe.iter().any(|s| s == "_INBOX.dev-a.>"), "and its answers come down");
}

#[test]
fn ants_write_only_their_own_chain_and_read_every_one() {
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    for s in ["$KV.ants_chains.dev-a", "$JS.API.DIRECT.GET.KV_ants_chains.>", "$JS.API.STREAM.INFO.KV_ants_chains"] {
        assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
    }
    assert!(!a.publish.iter().any(|s| s.starts_with("$KV.ants_chains.") && s != "$KV.ants_chains.dev-a"));
    assert!(!a.publish.iter().any(|s| s.contains("CREATE.KV_ants_chains")), "only humans create the bucket");
    let human = Permissions::for_user(&UserId::human(), Role::Human);
    assert!(human.publish.iter().any(|s| s == "$JS.API.STREAM.CREATE.KV_ants_chains"));
    assert!(!human.publish.iter().any(|s| s.starts_with("$KV.ants_chains.")), "only the ant writes its chain");

    let entry = ChainEntry::new(p("dev-a"), vec![p("lead"), p("dev-b")]);
    assert_eq!(subject::chain(&entry.ant), "$KV.ants_chains.dev-a");
    let json = serde_json::to_string(&entry).unwrap();
    assert_eq!(serde_json::from_str::<ChainEntry>(&json).unwrap(), entry);
    let none = ChainEntry::new(p("dev-a"), Vec::new());
    assert!(!serde_json::to_string(&none).unwrap().contains("escalate_to"));
    assert!(entry.check_key("dev-a").is_ok());
    assert!(entry.check_key("dev-b").is_err(), "credentials vouch for the key only");
}

#[test]
fn the_permissions_version_travels_as_a_user_jwt_tag() {
    let tag = PERMISSIONS_VERSION.tag();
    assert_eq!(tag, format!("ants-perms:{}", PERMISSIONS_VERSION.0));
    let device = DeviceId::generate().tag();
    assert_eq!(PermissionsVersion::from_tags([device.as_str(), tag.as_str()]), Some(PERMISSIONS_VERSION));
    assert_eq!(PermissionsVersion::from_tags([device.as_str()]), None);
    assert_eq!(PermissionsVersion::from_tags(["ants-perms:x"]), None);
    assert!(PermissionsVersion(0) < PERMISSIONS_VERSION);
}
