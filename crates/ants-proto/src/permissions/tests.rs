use chrono::Utc;

use super::*;
use crate::{
    ANTS_VERSION, AdminPayload, AntReply, AntRequest, AskedAnt, CancelReply, CancelRequest, ChainEntry, ChannelEntry,
    ChannelId, ChannelOp, DeviceId, ENROLL_INBOX, FILES_STREAM, HumanEntry, Humans, JsDomain, MAX_CHANNEL_DESCRIPTION,
    Machine, QueueRequest, Refusal, Role, SENSE_STREAM, SPARE_READERS, STREAM, Serving, ThreadId, UserId, Uuid,
    WEBHOOK_MARKER, assisted_owner_key, assisted_withdrawal_key, channel_manager_serving_key, p, spare_readers,
    subject,
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
    let asked = AskedAnt {
        name: p("lead-w1"),
        template: "worker".to_owned(),
        request: "{}".to_owned(),
        reason: "more hands".to_owned(),
    };
    let request = AntRequest::new(lead.clone(), asked, "t1".parse().unwrap(), "UKEY", "host", Utc::now());
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
        "$JS.hub.API.$KV.ants_colony.dev-a",
        "$JS.hub.API.CONSUMER.MSG.NEXT.ANTS.notes-dev-a",
        "$JS.ACK.ANTS.inbox-dev-a.>",
        // The v2 form of nats-server 2.16, whatever the domain.
        "$JS.ACK.*.*.ANTS.inbox-dev-a.>",
        "ants.notes.*.dev-a",
    ] {
        assert!(publish.iter().any(|have| have == s), "{s} missing from {publish:?}");
    }
    assert!("hub.x".parse::<JsDomain>().is_err());

    // A human's leaf outbox rights name any domain already, so the hub's form takes no more room; the families a
    // human holds whole take one wildcard each, the plain forms staying as they are.
    let human = Permissions::for_user(&p("alice"), Role::Human);
    let in_domain = human.clone().with_js_domain(Some(&domain)).publish;
    assert_eq!(in_domain[..human.publish.len()], human.publish);
    assert!(in_domain.iter().any(|s| s == "$JS.*.API.STREAM.INFO.ANTS_OUT"));
    assert!(in_domain.iter().any(|s| s == "$JS.*.API.CONSUMER.*.ANTS_OUT.ANTS_IN"));
    // What a leaf node maps its own domain to, for the CLI run on the machine.
    assert!(in_domain.iter().any(|s| s == "$JS.API.CONSUMER.*.ANTS_OUT.ANTS_IN"));
    for family in ["STREAM.INFO.*", "STREAM.CREATE.*", "STREAM.UPDATE.*", "CONSUMER.CREATE.*"] {
        assert!(in_domain.iter().any(|s| *s == format!("$JS.hub.API.{family}")), "{family}");
    }
    for s in ["$JS.hub.API.STREAM.INFO.ANTS_IN", "$JS.hub.API.STREAM.INFO.ANTS_OUT", "$JS.hub.API.CONSUMER.CREATE.ANTS"]
    {
        assert!(!in_domain.iter().any(|have| have == s), "{s}");
    }
    assert!(in_domain.iter().any(|s| s == "$JS.hub.API.STREAM.MSG.GET.ANTS"), "other families stay listed");
    // An ant lacks some of each family, and an assistant some stream lookups: theirs stay listed.
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant).with_js_domain(Some(&domain)).publish;
    assert!(!ant.iter().any(|s| s.starts_with("$JS.hub.API.STREAM.") && s.ends_with(".*")));
    assert!(!ant.iter().any(|s| s == "$JS.hub.API.CONSUMER.CREATE.*"));
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice")).with_js_domain(Some(&domain)).publish;
    assert!(assistant.iter().any(|s| s == "$JS.hub.API.STREAM.INFO.ANTS"));
    assert!(!assistant.iter().any(|s| s == "$JS.hub.API.STREAM.INFO.*"));
}

/// A family's domain wildcard grants nothing through a leaf node, whose uplink checks the domain form unmapped: no
/// credentials hold a right of the family a human's lack, but for creating [`SENSE_STREAM`], which hosts hold alone. A
/// human on a leaf node may create it through the uplink, yet only with the config every host creates it with: a host
/// writes nothing in a stream it could not create with that config itself, which nats-server refuses for a stream of
/// another config.
#[test]
fn no_credentials_hold_a_jetstream_family_right_a_human_lacks() {
    let (human, a, pa) = (UserId::human(), p("dev-a"), p("pa"));
    let m1: Machine = "m1".parse().unwrap();
    let most = Permissions::for_user(&human, Role::Human).publish;
    let families = ["STREAM.INFO.*", "STREAM.CREATE.*", "STREAM.UPDATE.*", "CONSUMER.CREATE.*"];
    for (kind, rights) in [
        ("ant", Permissions::for_user(&a, Role::Ant)),
        ("assistant", Permissions::for_assistant(&pa, &human)),
        (
            "leaf",
            Permissions::for_leaf(
                &m1,
                [(&a, LeafRole::Ant), (&human, LeafRole::Human), (&pa, LeafRole::AssistantOf(&human))],
            ),
        ),
        ("host", Permissions::for_host(&m1)),
        ("channel manager", Permissions::for_channel_manager()),
        ("enrollment", Permissions::for_enrollment(&a)),
        ("webhook", Permissions::for_webhook(&p("hook"), &[])),
        ("observer", Permissions::for_observer(&p("watch"))),
        ("enroll approver", Permissions::for_enroll_approver()),
        ("reactions responder", Permissions::for_reactions_responder()),
        ("device revoker", Permissions::for_device_revoker()),
        ("bootstrap", Permissions::for_bootstrap()),
        ("registrar", Permissions::for_registrar()),
    ] {
        for s in &rights.publish {
            let plain = s.strip_prefix("$JS.API.").map(str::to_owned).or_else(|| {
                let rest = s.strip_prefix("$JS.")?;
                Some(rest.split_once(".API.")?.1.to_owned())
            });
            let Some(plain) = plain else { continue };
            if families.iter().any(|family| covers(family, &plain)) && plain != format!("STREAM.CREATE.{SENSE_STREAM}")
            {
                assert!(most.contains(&format!("$JS.API.{plain}")), "{kind} holds {s}");
            }
        }
    }
}

/// async-nats recreates an ordered consumer after a reconnect or missed heartbeats by deleting the old one and creating
/// the next under a name: credentials creating a bucket's ordered consumers may do both, or the watch stalls.
#[test]
fn ordered_consumers_of_a_bucket_can_be_recreated_under_a_name() {
    let (human, a, pa) = (UserId::human(), p("dev-a"), p("pa"));
    let m1: Machine = "m1".parse().unwrap();
    for (kind, rights) in [
        ("human", Permissions::for_user(&human, Role::Human)),
        ("ant", Permissions::for_user(&a, Role::Ant)),
        ("assistant", Permissions::for_assistant(&pa, &human)),
        ("host", Permissions::for_host(&m1)),
        ("channel manager", Permissions::for_channel_manager()),
        ("observer", Permissions::for_observer(&p("watch"))),
        ("reactions responder", Permissions::for_reactions_responder()),
    ] {
        let streams: Vec<&str> =
            rights.publish.iter().filter_map(|s| s.strip_prefix("$JS.API.CONSUMER.CREATE.KV_")).collect();
        let streams: Vec<&str> = streams.into_iter().filter(|s| !s.contains('.')).collect();
        assert!(!streams.is_empty(), "{kind} creates no ordered consumer of a bucket");
        for stream in streams {
            let recreating = [
                format!("$JS.API.CONSUMER.CREATE.KV_{stream}.KhJx2c_1.$KV.{stream}.>"),
                format!("$JS.API.CONSUMER.CREATE.KV_{stream}.KhJx2c_2"),
                format!("$JS.API.CONSUMER.DELETE.KV_{stream}.KhJx2c_1"),
            ];
            for s in recreating {
                assert!(rights.publish.iter().any(|have| covers(have, &s)), "{kind} may not publish {s}");
            }
        }
    }
}

#[test]
fn a_leaf_carries_an_assistant_s_rights_as_it_would_an_ant_s() {
    let (alice, pa) = (p("alice"), p("pa"));
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&pa, LeafRole::AssistantOf(&alice))]);
    let own = Permissions::for_assistant(&pa, &alice);
    for s in own.publish.iter().filter(|s| !s.starts_with("ants.out.")) {
        assert!(leaf.publish.contains(s), "{s}");
    }
    assert!(own.subscribe.iter().all(|s| leaf.subscribe.contains(s)));
    // The literals the hub stream's interest needs, as for an ant and for the admin orders it sends; no outbox to set
    // up.
    for s in ["ants.inbox.pa.pa", "ants.chan.pa.pa", "ants.chanadm.pa.pa", "ants.timeline.pa.pa", "ants.admin.pa.pa"] {
        assert!(leaf.publish.iter().any(|have| have == s), "{s}");
    }
    assert!(!leaf.publish.contains(&format!("{}.>", m1.setup_inbox(&pa))));
    let ant = Permissions::for_leaf(&m1, [(&pa, LeafRole::Ant)]);
    assert!(!ant.publish.iter().any(|s| s.starts_with("ants.admin.")), "an ant sends no admin order");
    assert_eq!(Permissions::for_leaf(&m1, [(&pa, Role::Ant)]), Permissions::for_leaf(&m1, [(&pa, LeafRole::Ant)]));
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
            // Already allowed by `ants.inbox.*.dev-a`, `ants.notes.*.dev-a`, `ants.chan.*.dev-a`,
            // `ants.chanadm.*.dev-a` and `ants.timeline.*.dev-a`.
            "ants.inbox.dev-a.dev-a",
            "ants.notes.dev-a.dev-a",
            "ants.chan.dev-a.dev-a",
            "ants.chanadm.dev-a.dev-a",
            "ants.timeline.dev-a.dev-a",
            // The machine's host connection.
            "$KV.ants_machines.m1",
            "$JS.API.DIRECT.GET.KV_ants_machines.$KV.ants_machines.m1.>",
            "$KV.ants_machines.m1.>",
            // The shared settings of the ants it serves, and the incarnation of their bucket.
            "$JS.API.DIRECT.GET.KV_ants_identities.$KV.ants_identities.*.settings",
            "$JS.API.DIRECT.GET.KV_ants_identities.$KV.ants_identities._incarnation",
            "$KV.ants_identities.*.settings",
            // Its revocation reads are already an ant's.
            "$JS.API.CONSUMER.INFO.ANTS.admin-m1",
            "$JS.API.CONSUMER.CREATE.ANTS.admin-m1.ants.admin.m1.>",
            "$JS.API.CONSUMER.MSG.NEXT.ANTS.admin-m1",
            "$JS.API.DIRECT.GET.KV_ants_colony.$KV.ants_colony.*",
            // Asking for a new ant's credentials, its own or on an admin's order.
            "ants.enroll.*",
            // Sense's event logs, which only hosts reach.
            "$JS.API.STREAM.CREATE.ANTS_SENSE",
            "ants_sense.*",
            "$JS.API.DIRECT.GET.ANTS_SENSE",
            "$JS.API.DIRECT.GET.ANTS_SENSE.ants_sense.*",
            // The payloads of its admin orders.
            "$O.ants_files.C._admin.*.m1.>",
            "$JS.API.DIRECT.GET.OBJ_ants_files.$O.ants_files.C._admin.*.m1.>",
            "$JS.ACK.ANTS.admin-m1.>",
            "$JS.ACK.*.*.ANTS.admin-m1.>",
            "$JS.S.m1.>",
            "$JSC.R.m1.*.ANTS_OUT.>",
            "_INBOX.human.leaf.m1.>",
            // Asking for any renewal on the machine.
            "ants.renew.*",
        ]
    );
    let mut extra: Vec<_> = only_a.subscribe.iter().filter(|s| !own.subscribe.contains(s)).collect();
    assert_eq!(extra.pop().map(String::as_str), Some("ants.renew.*.reply"), "hearing the renewals");
    assert_eq!(extra.remove(0), "_INBOX_HOST.m1.>", "hearing the host connection's replies");
    assert_eq!(extra.remove(0), "ants.enroll.*.reply", "hearing the answer");
    let fc = ["$JS.FC.ANTS_OUT.>", "$JS.FC.m1.*.ANTS_OUT.>"];
    assert!(fc.iter().all(|s| extra.contains(&&s.to_string())), "both forms in m1's domain: {extra:?}");
    assert!(extra.iter().all(|s| s.starts_with("$JS.m1.API.") || fc.contains(&s.as_str())), "{extra:?}");
    assert!(extra.iter().all(|s| s.contains(".ANTS_OUT")), "{extra:?}");
    for op in ["INFO", "RESET"] {
        let durable = format!("$JS.m1.API.CONSUMER.{op}.ANTS_OUT.ANTS_IN");
        assert!(extra.contains(&&durable), "the outbox's durable consumer: {extra:?}");
    }
    // Never hearing the requests of other machines, nor answering them, without a human (who watches all).
    assert!(!only_a.subscribe.iter().any(|s| s == subject::ALL_ENROLL_REQUESTS || s == ">" || s == "ants.>"));
    assert!(!leaf.publish.iter().any(|s| s == subject::ALL_ENROLL_REPLIES || s == ">" || s == "ants.>"));
}

#[test]
fn outboxes_take_inbox_notes_channel_dead_letter_and_timeline_subjects_only() {
    let t: ThreadId = "t1".parse().unwrap();
    let c: ChannelId = "ops".parse().unwrap();
    assert_eq!(subject::outbox(&subject::inbox(&p("dev-a"), &p("dev-b"))).unwrap(), "ants.out.inbox.dev-a.dev-b");
    assert_eq!(subject::outbox(&subject::notes(&t, &p("dev-a"))).unwrap(), "ants.out.notes.t1.dev-a");
    assert_eq!(subject::outbox(&subject::chan(&c, &p("dev-a"))).unwrap(), "ants.out.chan.ops.dev-a");
    assert_eq!(subject::outbox(&subject::dead(&p("dev-a"))).unwrap(), "ants.out.dead.dev-a");
    for other in [subject::approvals(&p("dev-a")), subject::colony(&p("dev-a")), "ants.outbox.x".to_owned()] {
        assert_eq!(subject::outbox(&other), None, "{other}");
    }
    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert_eq!(subject::outbox(&subject::timeline(&t, &p("dev-a"))).unwrap(), "ants.out.timeline.t1.dev-a");
    let own = ["ants.out.inbox.*.dev-a", "ants.out.notes.*.dev-a", "ants.out.chan.*.dev-a", "ants.out.dead.dev-a"];
    for s in own.into_iter().chain(["ants.out.timeline.*.dev-a"]) {
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
            ("ants.in.m1.ants.out.timeline.*.dev-a".to_owned(), "ants.timeline.{{wildcard(1)}}.dev-a".to_owned()),
        ]
    );
    let human = m1.admitted(&UserId::human(), Role::Human, true);
    assert_eq!(human[0], ("ants.in.m1.ants.out.inbox.>".to_owned(), "ants.inbox.>".to_owned()));
    assert!(human.iter().all(|(filter, _)| !filter.contains(".dead.")), "no dead letters of a human");
    assert!(human.iter().all(|(filter, _)| !filter.contains(".timeline.")), "no timeline of a human");
    let rest: Vec<_> = m1.admitted(&p("dev-a"), Role::Ant, false).into_iter().map(|(filter, _)| filter).collect();
    assert_eq!(
        rest,
        [
            "ants.in.m1.ants.out.notes.*.dev-a",
            "ants.in.m1.ants.out.chan.*.dev-a",
            "ants.in.m1.ants.out.dead.dev-a",
            "ants.in.m1.ants.out.timeline.*.dev-a",
        ],
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
fn an_observer_publishes_nothing_but_reads_its_feeds_status_and_the_colony() {
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
    assert!(buckets.iter().all(|s| s.contains("KV_ants_colony")), "{buckets:?}");
    let acks: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("$JS.ACK.")).collect();
    assert_eq!(acks.len(), 6, "v1 and v2 acks of its own consumers only: {acks:?}");
    assert!(acks.iter().all(|s| s.contains(".ANTS.observe-") && s.ends_with("-audit.>")), "{acks:?}");
    let fc: Vec<&String> = perms.publish.iter().filter(|s| s.starts_with("$JS.FC.")).collect();
    assert_eq!(fc, ["$JS.FC.KV_ants_colony.>", "$JS.FC.*.*.KV_ants_colony.>"]);
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
        "$JS.API.CONSUMER.DELETE.ANTS.chan-dev-a",
    ] {
        assert!(a.publish.iter().any(|have| have == s), "{s} missing from {:?}", a.publish);
    }
    assert!(a.subscribe.iter().any(|s| s == subject::ALL_CHANNELS));
    assert!(!a.publish.iter().any(|s| s.contains("$KV.ants_channels") || s.contains("STREAM.CREATE.KV_ants_channels")));
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
    assert_eq!(spare_readers("chanread-dev-a").collect::<Vec<_>>(), ["chanread-dev-a_1", "chanread-dev-a_2"]);
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
fn only_a_human_or_its_assistant_describes_a_channel_and_never_with_other_changes() {
    let humans = Humans::default();
    let mut entry = ChannelEntry::new("ops".parse().unwrap());
    entry.owner = Some(p("lead"));
    let describe = [ChannelOp::SetDescription { description: "  Incidents of the shop\n".to_owned() }];
    assert_eq!(entry.apply(&p("lead"), &describe, &humans), Err(Refusal::Description), "not even the owner");
    let mixed = [describe[0].clone(), ChannelOp::AddMember { ant: p("dev-a") }];
    assert_eq!(entry.apply(&p("lead"), &mixed, &humans), Err(Refusal::Description));
    assert_eq!(entry.apply_for_human(&mixed), Err(Refusal::NotForHuman));
    assert_eq!(entry.description, None, "a refusal changes nothing");

    assert_eq!(entry.apply_for_human(&describe).unwrap(), [r#"description set to "Incidents of the shop""#]);
    assert_eq!(entry.description.as_deref(), Some("Incidents of the shop"));
    assert!(entry.apply_for_human(&describe).unwrap().is_empty(), "nothing to change");
    let long = [ChannelOp::SetDescription { description: "x".repeat(MAX_CHANNEL_DESCRIPTION + 1) }];
    assert!(matches!(entry.apply_for_human(&long), Err(Refusal::Invalid(_))));
    let blank = [ChannelOp::SetDescription { description: " ".to_owned() }];
    assert_eq!(entry.apply_for_human(&blank).unwrap(), ["description cleared"], "a blank one clears it");
    assert_eq!(entry.description, None);
    assert!(entry.apply_for_human(&[ChannelOp::ClearDescription]).unwrap().is_empty());
    assert!(!serde_json::to_string(&entry).unwrap().contains("description"));
}

#[test]
fn the_channel_manager_hears_requests_and_writes_channels_only() {
    let manager = Permissions::for_channel_manager();
    assert_eq!(manager.subscribe, ["ants.chanadm.*.*", "_INBOX.channel-manager.>", "ants.renew.channel-manager.reply"]);
    for s in [
        "ants.chan.*.channel-manager",
        "$KV.ants_channels.>",
        "$JS.API.DIRECT.GET.KV_ants_channels.>",
        "$JS.API.DIRECT.GET.KV_ants_colony.>",
        "$JS.API.CONSUMER.CREATE.KV_ants_humans",
        // The records it checks an assistant's request for its human against.
        "$JS.API.DIRECT.GET.KV_ants_devices.$KV.ants_devices.*.assistant",
        "$JS.API.DIRECT.GET.KV_ants_devices.$KV.ants_devices.revoked.*",
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
        "$KV.ants_humans.*",
        "$KV.ants_humans.alice.>",
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
    for s in ["_INBOX.alice.leaf.m1.>", "_INBOX.human.leaf.m1.>", "$KV.ants_humans.*", "$KV.ants_humans.alice.>"] {
        assert!(leaf.publish.iter().any(|have| have == s), "{s} missing from {:?}", leaf.publish);
    }
}

#[test]
fn humans_keep_only_their_own_read_markers_and_ants_only_get_inbox_ones() {
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in [
        "$KV.ants_read.alice",
        "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice",
        "$KV.ants_read.alice.chan",
        "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice.chan",
    ] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let read: Vec<&String> = human.publish.iter().filter(|s| s.contains("$KV.ants_read")).collect();
    assert!(read.iter().all(|s| (s.ends_with(".alice") || s.ends_with(".alice.chan")) && !s.contains('>')), "{read:?}");
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let read: Vec<&String> = ant.publish.iter().filter(|s| s.contains("ants_read")).collect();
    assert_eq!(read, ["$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.*"], "an ant gets a human's inbox marker only");
    assert!(!covers(read[0], "$JS.API.DIRECT.GET.KV_ants_read.$KV.ants_read.alice.chan"));
}

#[test]
fn humans_keep_only_their_own_holds_and_held_notices_and_ants_only_get_holds() {
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
    let notify: Vec<&String> = ant.publish.iter().filter(|s| s.contains("ants_notify")).collect();
    assert_eq!(notify, ["$JS.API.DIRECT.GET.KV_ants_notify.$KV.ants_notify.*"], "an ant gets a human's holds only");
    assert!(!covers(notify[0], "$JS.API.DIRECT.GET.KV_ants_notify.$KV.ants_notify.held.alice"));
}

#[test]
fn humans_set_every_title_tldr_and_done_state_but_only_their_own_pins_and_archives_and_ants_only_read_some() {
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in [
        "$KV.ants_threads.title.*",
        "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.title.*",
        "$KV.ants_threads.summary.*",
        "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.summary.*",
        "$KV.ants_threads.done.*",
        "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.done.*",
        "$JS.API.STREAM.CREATE.KV_ants_threads",
    ] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    // Its own marks and snoozes, and those its assistants set for it, and no other human's.
    for key in ["marks.alice", "reminders.alice", "marks.alice.pa", "reminders.alice.pb"] {
        for s in
            [format!("$KV.ants_threads.{key}"), format!("$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.{key}")]
        {
            assert!(human.publish.iter().any(|have| covers(have, &s)), "{s} missing from {:?}", human.publish);
            let bob = s.replace("alice", "bob");
            assert!(!human.publish.iter().any(|have| covers(have, &bob)), "{bob}");
        }
    }
    let threads: Vec<&String> = human.publish.iter().filter(|s| s.contains("ants_threads")).collect();
    assert!(!threads.iter().any(|s| s.ends_with("ants_threads.>") || s.contains("CONSUMER")), "{threads:?}");
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let threads: Vec<&String> =
        ant.publish.iter().chain(&ant.subscribe).filter(|s| s.contains("ants_threads")).collect();
    let mut reads: Vec<String> = ["title.*", "done.*", "reminders.*"]
        .map(|k| format!("$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.{k}"))
        .into();
    reads.extend([
        "$JS.API.CONSUMER.CREATE.KV_ants_threads.done-dev-a.$KV.ants_threads.done.*".to_owned(),
        "$JS.API.CONSUMER.MSG.NEXT.KV_ants_threads.done-dev-a".to_owned(),
    ]);
    reads.extend(ack_subjects("KV_ants_threads", "done-dev-a"));
    reads.push("$JS.API.CONSUMER.DELETE.KV_ants_threads.done-dev-a".to_owned());
    assert_eq!(
        threads,
        reads.iter().collect::<Vec<_>>(),
        "an ant only reads titles, done states and own reminders, and hears of done states through its own consumer, \
         which it deletes on its retirement"
    );
    let assisted = "$JS.API.DIRECT.GET.KV_ants_threads.$KV.ants_threads.reminders.alice.pa";
    assert!(!threads.iter().any(|s| covers(s, assisted)), "nor snoozes an assistant set");
    let observer = Permissions::for_observer(&p("watcher"));
    assert!(!observer.publish.iter().chain(&observer.subscribe).any(|s| s.contains("ants_threads")));
}

#[test]
fn humans_write_only_their_own_reactions_ants_ask_and_only_the_responder_writes_others() {
    let others = ["$JS.API.DIRECT.GET.KV_ants_reactions.>", "$JS.API.CONSUMER.CREATE.KV_ants_reactions"];
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let reactions: Vec<&String> =
        ant.publish.iter().chain(&ant.subscribe).filter(|s| s.contains("ants_reactions")).collect();
    assert!(reactions.is_empty(), "an ant neither reads nor writes reactions: {reactions:?}");
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert!(human.publish.iter().any(|s| s == "$KV.ants_reactions.alice.>"));
    assert!(!human.publish.iter().any(|s| s == "$KV.ants_reactions.>" || s == "$KV.ants_reactions.*.>"));
    for s in others.into_iter().chain(["$JS.API.STREAM.CREATE.KV_ants_reactions"]) {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let responder = Permissions::for_reactions_responder();
    for s in others.into_iter().chain(["$KV.ants_reactions.>", "$JS.API.DIRECT.GET.ANTS", "$JS.API.STREAM.INFO.ANTS"]) {
        assert!(responder.publish.iter().any(|have| have == s), "{s} missing from {:?}", responder.publish);
    }
    assert!(!responder.publish.iter().any(|s| s.contains("STREAM.CREATE") || s.starts_with("ants.")));
    assert_eq!(responder.subscribe[..2], [subject::ALL_REACTIONS_REQUESTS, subject::ALL_REACT_REQUESTS]);
    let observer = Permissions::for_observer(&p("watcher"));
    assert!(!observer.publish.iter().chain(&observer.subscribe).any(|s| s.contains("ants_reactions")));

    // Ants ask about the reactions to messages they read only as themselves, and only humans and the responder
    // answer.
    let asks = |list: &[String]| -> Vec<String> {
        list.iter().filter(|s| s.starts_with("ants.reactions.")).cloned().collect()
    };
    assert_eq!(asks(&ant.publish), ["ants.reactions.dev-a", "ants.reactions.dev-a.react"]);
    assert!(ant.subscribe.iter().any(|s| s == "_INBOX.dev-a.>"), "hears the answers in its reply inboxes");
    for answering in [&human, &responder] {
        assert!(answering.publish.iter().any(|s| s == subject::ALL_REACTIONS_REPLIES), "{:?}", answering.publish);
    }
    for other in [&observer, &Permissions::for_user(&p("dev-b"), Role::Ant)] {
        assert!(!other.publish.iter().any(|s| s.starts_with("_INBOX.*")), "{:?}", other.publish);
    }
    assert!(human.subscribe.iter().any(|s| s == subject::ALL), "hears the requests");
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&p("dev-a"), Role::Ant), (&p("alice"), Role::Human)]);
    assert!(leaf.publish.iter().any(|s| s == "ants.reactions.dev-a"), "an ant on a leaf asks through its uplink");
    assert!(leaf.publish.iter().any(|s| s == "ants.reactions.dev-a.react"));
    assert!(leaf.publish.iter().any(|s| s == subject::ALL_REACTIONS_REPLIES), "and a human there answers");
    assert!(leaf.subscribe.iter().any(|s| s == "_INBOX.dev-a.>"), "and its answers come down");
}

#[test]
fn humans_read_every_device_key_put_only_their_own_and_only_the_revoker_records_revocations() {
    let devices = |perms: &Permissions| -> Vec<String> {
        perms.publish.iter().chain(&perms.subscribe).filter(|s| s.contains("ants_devices")).cloned().collect()
    };
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in [
        "$JS.API.STREAM.INFO.KV_ants_devices",
        "$JS.API.STREAM.CREATE.KV_ants_devices",
        "$JS.API.DIRECT.GET.KV_ants_devices.>",
        "$JS.API.CONSUMER.CREATE.KV_ants_devices",
        "$KV.ants_devices.alice.>",
    ] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    let puts: Vec<String> = devices(&human).into_iter().filter(|s| s.starts_with("$KV.")).collect();
    assert_eq!(puts, ["$KV.ants_devices.alice.>"], "only its own devices, no revocation record");
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&p("alice"), Role::Human), (&p("dev-a"), Role::Ant)]);
    assert!(leaf.publish.iter().any(|s| s == "$KV.ants_devices.alice.>"), "a human on a leaf puts its own");

    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let observer = Permissions::for_observer(&p("watcher"));
    let webhook = Permissions::for_webhook(&p("hook"), &[Destination::Ant(p("dev-a"))]);
    // An ant and the channel manager read only the assistants' records and the revocations, to check what an
    // assistant does for its human.
    for checker in [ant, Permissions::for_channel_manager()] {
        assert_eq!(
            devices(&checker),
            [
                "$JS.API.DIRECT.GET.KV_ants_devices.$KV.ants_devices.*.assistant",
                "$JS.API.DIRECT.GET.KV_ants_devices.$KV.ants_devices.revoked.*",
            ]
        );
    }
    for none in [observer, webhook, Permissions::for_reactions_responder()] {
        assert_eq!(devices(&none), Vec::<String>::new());
    }

    let revoker = Permissions::for_device_revoker();
    let puts: Vec<&String> = revoker.publish.iter().filter(|s| s.starts_with("$KV.")).collect();
    assert_eq!(puts, ["$KV.ants_devices.revoked.*", "$KV.ants_identities.*", "$KV.ants_identities.*.settings"]);
    assert!(revoker.publish.iter().any(|s| s == "$JS.API.STREAM.CREATE.KV_ants_devices"));
    assert!(!revoker.publish.iter().any(|s| s.starts_with("ants.") || s.contains("CONSUMER")));
    assert_eq!(revoker.subscribe, [format!("{ENROLL_INBOX}.>")]);
}

#[test]
fn each_ant_writes_only_its_own_identity_and_humans_read_all() {
    let identities = |perms: &Permissions| -> Vec<String> {
        perms.publish.iter().chain(&perms.subscribe).filter(|s| s.contains("ants_identities")).cloned().collect()
    };
    assert_eq!(
        identities(&Permissions::for_user(&p("dev-a"), Role::Ant)),
        ["$JS.API.DIRECT.GET.KV_ants_identities", "$KV.ants_identities.dev-a"]
    );
    assert_eq!(
        identities(&Permissions::for_user(&p("alice"), Role::Human)),
        ["$JS.API.STREAM.CREATE.KV_ants_identities", "$JS.API.DIRECT.GET.KV_ants_identities", "$KV.ants_identities.*"]
    );
    // An assistant reads them as its human does, and writes only its own.
    assert_eq!(
        identities(&Permissions::for_assistant(&p("pa"), &p("alice"))),
        ["$JS.API.DIRECT.GET.KV_ants_identities", "$KV.ants_identities.pa"]
    );
    let observer = Permissions::for_observer(&p("watcher"));
    let webhook = Permissions::for_webhook(&p("hook"), &[Destination::Ant(p("dev-a"))]);
    for none in [observer, webhook, Permissions::for_channel_manager()] {
        assert_eq!(identities(&none), Vec::<String>::new());
    }
    // A host reads and writes the shared settings of any ant, and reads the bucket's incarnation marker, nothing else.
    assert_eq!(
        identities(&Permissions::for_host(&"m1".parse().unwrap())),
        [
            "$JS.API.DIRECT.GET.KV_ants_identities.$KV.ants_identities.*.settings",
            "$JS.API.DIRECT.GET.KV_ants_identities.$KV.ants_identities._incarnation",
            "$KV.ants_identities.*.settings"
        ]
    );
}

#[test]
fn humans_save_trails_every_user_reads_and_each_ant_writes_only_the_runs_it_conducts() {
    let trails = |perms: &Permissions| -> Vec<String> {
        perms.publish.iter().chain(&perms.subscribe).filter(|s| s.contains("ants_trail")).cloned().collect()
    };
    assert_eq!(
        trails(&Permissions::for_user(&p("dev-a"), Role::Ant)),
        [
            "$JS.API.DIRECT.GET.KV_ants_trails",
            "$JS.API.DIRECT.GET.KV_ants_trail_runs.$KV.ants_trail_runs.dev-a.>",
            "$KV.ants_trail_runs.dev-a.>"
        ]
    );
    assert_eq!(
        trails(&Permissions::for_user(&p("alice"), Role::Human)),
        [
            "$JS.API.DIRECT.GET.KV_ants_trails",
            "$JS.API.STREAM.CREATE.KV_ants_trails",
            "$JS.API.STREAM.INFO.KV_ants_trails",
            "$KV.ants_trails.*",
            "$JS.API.STREAM.CREATE.KV_ants_trail_runs",
            "$JS.API.STREAM.INFO.KV_ants_trail_runs",
            "$JS.API.DIRECT.GET.KV_ants_trail_runs"
        ]
    );
    let may = |perms: &Permissions, s: &str| perms.publish.iter().any(|have| covers(have, s));
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let run: crate::RunId = "r1".parse().unwrap();
    for denied in ["$KV.ants_trails.deploy", "$KV.ants_trail_runs.dev-b.r1", "$KV.ants_trail_runs.dev-a"] {
        assert!(!may(&ant, denied), "an ant may publish {denied}");
    }
    assert!(!may(&ant, "$JS.API.DIRECT.GET.KV_ants_trail_runs.$KV.ants_trail_runs.dev-b.r1"));
    for key in [crate::trail_run_key(&p("dev-a"), &run), crate::trail_call_key(&p("dev-a"), &run, 3)] {
        assert!(may(&ant, &format!("$KV.ants_trail_runs.{key}")), "{key}");
    }
    // An assistant conducts trails as an ant does, and saves none, which would name its human as their author.
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    assert!(!may(&assistant, "$KV.ants_trails.deploy"));
    assert!(may(&assistant, "$KV.ants_trail_runs.pa.r1.session"));
    let observer = Permissions::for_observer(&p("watcher"));
    let webhook = Permissions::for_webhook(&p("hook"), &[Destination::Ant(p("dev-a"))]);
    for none in [observer, webhook, Permissions::for_channel_manager(), Permissions::for_host(&"m1".parse().unwrap())] {
        assert_eq!(trails(&none), Vec::<String>::new());
    }
}

#[test]
fn humans_ask_and_only_the_conductor_hears_and_answers_trail_requests() {
    let control = |perms: &Permissions| -> (Vec<String>, Vec<String>) {
        let of = |rights: &[String]| rights.iter().filter(|s| s.starts_with("ants.trail")).cloned().collect();
        (of(&perms.publish), of(&perms.subscribe))
    };
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert_eq!(control(&ant), (vec!["ants.trailed.dev-a".to_owned()], vec!["ants.trail.dev-a".to_owned()]));
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert_eq!(control(&human).0, [subject::ALL_TRAIL_REQUESTS]);
    assert!(human.subscribe.iter().any(|s| s == subject::ALL), "the answers");
    // Its request names no one the ant's daemon could check, so an assistant asks none.
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    assert!(!assistant.publish.iter().any(|have| covers(have, "ants.trail.dev-a")));
    let leaf = Permissions::for_leaf(&"m1".parse().unwrap(), [(&p("dev-a"), LeafRole::Ant)]);
    assert!(leaf.subscribe.iter().any(|have| covers(have, "ants.trail.dev-a")));
    assert!(leaf.publish.iter().any(|have| covers(have, "ants.trailed.dev-a")));
    assert_eq!(subject::outbox(&subject::trail(&p("dev-a"))), None, "never stored, so never buffered");
}

#[test]
fn users_write_only_their_own_attachments_and_ants_read_those_of_the_messages_they_read() {
    let files = |perms: &Permissions| -> Vec<String> {
        perms.publish.iter().chain(&perms.subscribe).filter(|s| s.contains("ants_files")).cloned().collect()
    };
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let get = "$JS.API.DIRECT.GET.OBJ_ants_files.$O.ants_files.C";
    assert_eq!(
        files(&ant),
        [
            "$O.ants_files.C.dev-a.>".to_owned(),
            format!("{get}.*.inbox.dev-a.>"),
            format!("{get}.dev-a.inbox.>"),
            format!("{get}.*.notes.>"),
            format!("{get}.*.chan.>"),
        ],
        "no stream info or consumer, which would show the attachments between others"
    );
    let outbox: Vec<&String> = ant.publish.iter().filter(|s| s.starts_with("ants.out.files.")).collect();
    assert_eq!(outbox, ["ants.out.files.dev-a.>"]);

    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in [
        "$O.ants_files.C.>",
        "ants.out.files.>",
        "$JS.API.STREAM.INFO.OBJ_ants_files",
        "$JS.API.STREAM.CREATE.OBJ_ants_files",
        "$JS.API.STREAM.UPDATE.OBJ_ants_files",
        "$JS.API.DIRECT.GET.OBJ_ants_files.$O.ants_files.C.>",
    ] {
        assert!(human.publish.iter().any(|have| have == s), "{s} missing from {:?}", human.publish);
    }
    // Admitting an ant new to a leaf node into the store, from the leaf's outbox.
    let approver = Permissions::for_enroll_approver();
    assert_eq!(
        files(&approver),
        ["$JS.API.STREAM.INFO.OBJ_ants_files", "$JS.API.STREAM.UPDATE.OBJ_ants_files"],
        "no chunk"
    );
    let domain: JsDomain = "hub".parse().unwrap();
    let ant_in_domain = ant.clone().with_js_domain(Some(&domain));
    assert!(
        ant_in_domain.publish.iter().any(|s| s == "$JS.hub.API.DIRECT.GET.OBJ_ants_files.$O.ants_files.C.*.notes.>")
    );
    let m1: Machine = "m1".parse().unwrap();
    let leaf = Permissions::for_leaf(&m1, [(&p("dev-a"), Role::Ant)]);
    assert!(leaf.publish.iter().any(|s| s == "$O.ants_files.C.dev-a.>"), "an ant on a leaf stores through the uplink");
    assert!(!leaf.publish.iter().any(|s| s.starts_with("ants.out.files.")), "the outbox forms stay on the leaf");
    let observer = Permissions::for_observer(&p("watcher"));
    let hook = Permissions::for_webhook(&p("gh"), &[Destination::Ant(p("dev-a"))]);
    for none in [&observer, &hook, &Permissions::for_channel_manager(), &Permissions::for_enrollment(&p("dev-a"))] {
        assert!(files(none).is_empty() && !none.publish.iter().any(|s| s.contains("out.files")), "{none:?}");
    }
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
fn only_the_signer_records_where_the_account_key_is() {
    let holder = Permissions::for_account_holder();
    assert_eq!(holder.publish, ["$KV.ants_machines._account"]);
    assert!("_account".parse::<Machine>().is_err(), "no machine's host credentials write it");
    let in_domain = holder.with_js_domain(Some(&"hub".parse().unwrap()));
    assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_machines._account"));
    for (who, role) in [("alice", Role::Human), ("dev-a", Role::Ant)] {
        let user = Permissions::for_user(&p(who), role);
        assert!(!user.publish.iter().any(|s| s.contains("_account")), "{who}");
    }
}

#[test]
fn a_host_writes_only_its_own_machine_record_and_takes_only_its_own_orders() {
    let m1: Machine = "m1".parse().unwrap();
    let host = Permissions::for_host(&m1);
    for s in ["$KV.ants_machines.m1", "$JS.API.CONSUMER.CREATE.ANTS.admin-m1.ants.admin.m1.>"] {
        assert!(host.publish.iter().any(|have| have == s), "{s} missing from {:?}", host.publish);
    }
    assert!(host.publish.iter().all(|s| !s.contains("ants_machines.*") && !s.contains("ants_machines.>")));
    // It records the outcomes of its own machine's orders only, and reads revocations, which it never writes.
    let outcomes: Vec<_> = host.publish.iter().filter(|s| s.contains("ants_machines.m1.")).collect();
    assert_eq!(outcomes, ["$JS.API.DIRECT.GET.KV_ants_machines.$KV.ants_machines.m1.>", "$KV.ants_machines.m1.>"]);
    assert!(host.publish.iter().all(|s| !s.starts_with("$KV.ants_devices")), "{:?}", host.publish);
    assert_eq!(subject::admin_outcomes(&m1), "$KV.ants_machines.m1.>");
    // Past its own subjects, it asks the signer to enroll or retire an ant on an admin's order, and reads the colony.
    let ants: Vec<_> = host.publish.iter().filter(|s| s.starts_with("ants.")).collect();
    assert_eq!(ants, [subject::ALL_ENROLL_REQUESTS]);
    // Of streams' info, it reads only the humans', whose consent to their assistants' orders it watches, and creates
    // only sense's.
    let infos: Vec<_> = host.publish.iter().filter(|s| s.contains("STREAM.")).collect();
    assert_eq!(infos, ["$JS.API.STREAM.CREATE.ANTS_SENSE", "$JS.API.STREAM.INFO.KV_ants_humans"]);
    assert!(host.publish.iter().all(|s| !s.starts_with("$KV.ants_humans")), "it never writes the humans");
    assert!(host.publish.iter().any(|s| s == "$JS.API.DIRECT.GET.KV_ants_colony.$KV.ants_colony.*"));
    assert!(!host.publish.iter().any(|s| s.starts_with("$KV.ants_colony")), "it reads the colony only");
    assert_eq!(host.subscribe, ["_INBOX_HOST.m1.>", subject::ALL_ENROLL_REPLIES]);
    let in_domain = Permissions::for_host(&m1).with_js_domain(Some(&"hub".parse().unwrap()));
    assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_machines.m1"));
    // No ant gets any of it, and a human reads every record and writes none.
    for user in [p("dev-a"), p("host-m1")] {
        let ant = Permissions::for_user(&user, Role::Ant);
        assert!(!ant.publish.iter().any(|s| s.contains("ants_machines") || s.contains("admin-")), "{user}");
        assert!(!ant.subscribe.iter().any(|s| s.starts_with("_INBOX_HOST")), "{user}");
    }
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for s in ["$JS.API.STREAM.CREATE.KV_ants_machines", "$JS.API.DIRECT.GET.KV_ants_machines"] {
        assert!(human.publish.iter().any(|have| have == s), "{s}");
    }
    assert!(!human.publish.iter().any(|s| s.starts_with("$KV.ants_machines") || s.contains("KV_ants_machines.")));
    // A human sends orders to any machine's ants, as does an assistant for its human, which daemons hold for the
    // human's confirmation; an ant sends none.
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    for rights in [&human, &assistant] {
        assert!(rights.publish.iter().any(|s| s == "ants.admin.*.*"));
    }
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(!ant.publish.iter().any(|s| s.starts_with("ants.admin") || s == ">"), "{:?}", ant.publish);
    assert_eq!(subject::machine_record(&m1), "$KV.ants_machines.m1");
    assert_eq!(subject::admin_orders(&m1), "ants.admin.m1.>");
}

#[test]
fn a_host_stores_and_reads_only_its_own_admin_payloads_which_no_ant_reaches() {
    let (m1, m2): (Machine, Machine) = ("m1".parse().unwrap(), "m2".parse().unwrap());
    let payload = |machine: &Machine| AdminPayload {
        id: Uuid::now_v7(),
        machine: machine.clone(),
        size: 1,
        sha256: String::new(),
    };
    let (own, other) = (payload(&m1).chunk_subject(0), payload(&m2).chunk_subject(0));
    let get = |chunk: &str| format!("$JS.API.DIRECT.GET.{FILES_STREAM}.{chunk}");
    let can = |rights: &Permissions, s: &str| rights.publish.iter().any(|have| covers(have, s));
    let host = Permissions::for_host(&m1);
    assert!(can(&host, &own) && can(&host, &get(&own)), "{:?}", host.publish);
    assert!(!can(&host, &other) && !can(&host, &get(&other)));
    assert!(!can(&host, "$O.ants_files.C.dev-a.inbox.alice.x.0"), "no attachment of an ant's");
    // A human stores and reads any; no ant does either, though an ant may be named like a machine.
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert!(can(&human, &own) && can(&human, &get(&own)));
    for ant in [p("dev-a"), p("m1")] {
        let ant = Permissions::for_user(&ant, Role::Ant);
        assert!(!can(&ant, &own) && !can(&ant, &get(&own)), "{:?}", ant.publish);
    }
}

#[test]
fn ants_store_and_read_back_only_their_own_timeline() {
    let timeline =
        |list: &[String]| -> Vec<String> { list.iter().filter(|s| s.contains("timeline")).cloned().collect() };
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert_eq!(
        timeline(&ant.publish),
        [
            "ants.timeline.*.dev-a",
            "ants.out.timeline.*.dev-a",
            "$JS.API.CONSUMER.CREATE.ANTS.sentread-dev-a.ants.timeline.*.dev-a",
        ]
    );
    assert!(timeline(&ant.subscribe).is_empty());
    assert!(!ant.subscribe.iter().any(|s| s == subject::ALL || s == ">"));
    // A human stores none, and reads them all as it reads every stored message.
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert!(timeline(&human.publish).is_empty());
    assert!(human.subscribe.iter().any(|s| s == subject::ALL));
    assert!(human.publish.iter().any(|s| s == &format!("$JS.API.STREAM.MSG.GET.{STREAM}")));
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

#[test]
fn only_bootstrap_credentials_ask_to_register_and_only_the_signer_hears_them() {
    let bootstrap = Permissions::for_bootstrap();
    assert_eq!(bootstrap.publish, [subject::ALL_LOGIN_REQUESTS]);
    assert_eq!(bootstrap.subscribe, [subject::ALL_LOGIN_REPLIES]);
    let registrar = Permissions::for_registrar();
    assert_eq!((registrar.publish, registrar.subscribe), (bootstrap.subscribe, bootstrap.publish));
    let (dev_a, alice): (UserId, UserId) = ("dev-a".parse().unwrap(), "alice".parse().unwrap());
    let m1: Machine = "m1".parse().unwrap();
    let mut leaf = Permissions::for_leaf(&m1, [(&dev_a, Role::Ant), (&alice, Role::Human)]);
    leaf.publish.extend(Permissions::for_enroll_approver().publish);
    leaf.subscribe.extend(Permissions::for_enroll_approver().subscribe);
    let others = [Permissions::for_user(&dev_a, Role::Ant), Permissions::for_user(&alice, Role::Human), leaf];
    for permissions in others {
        let subjects = permissions.publish.iter().chain(&permissions.subscribe);
        assert!(subjects.clone().all(|s| !s.starts_with("ants.login.") && s != ">"), "{subjects:?}");
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

/// Sense's event logs: host credentials, and a leaf node's uplink carrying them, create the stream, append to and read
/// any identity's subject by direct gets, and nothing more; no other credentials reach any of it, and no human hears
/// it.
#[test]
fn only_hosts_reach_sense_event_logs() {
    let (human, a, pa) = (UserId::human(), p("dev-a"), p("pa"));
    let m1: Machine = "m1".parse().unwrap();
    let domain: JsDomain = "hub".parse().unwrap();
    let id: crate::SenseId = "01J9ZQ3C5V8W2X4Y6Z7A8B9C0D".parse().unwrap();
    let log = subject::sense(&id);
    let reaches = |rights: &[String]| -> Vec<String> {
        rights
            .iter()
            .filter(|s| s.contains(SENSE_STREAM) || covers(s, &log) || covers(s, subject::ALL_SENSE))
            .cloned()
            .collect()
    };
    let host = Permissions::for_host(&m1);
    let want = [
        "$JS.API.STREAM.CREATE.ANTS_SENSE",
        "ants_sense.*",
        "$JS.API.DIRECT.GET.ANTS_SENSE",
        "$JS.API.DIRECT.GET.ANTS_SENSE.ants_sense.*",
    ];
    assert_eq!(reaches(&host.publish), want);
    assert!(reaches(&host.subscribe).is_empty(), "replies come to the host's inbox");
    let in_domain = Permissions::for_host(&m1).with_js_domain(Some(&domain)).publish;
    assert!(in_domain.iter().any(|s| s == "$JS.hub.API.DIRECT.GET.ANTS_SENSE.ants_sense.*"));
    assert!(in_domain.iter().any(|s| s == "$JS.hub.API.STREAM.CREATE.ANTS_SENSE"));
    let uplink = Permissions::for_leaf(&m1, [(&a, LeafRole::Ant), (&human, LeafRole::Human)])
        .with_uplink_js_domain(Some(&domain));
    for s in ["$JS.hub.API.STREAM.CREATE.ANTS_SENSE", "ants_sense.*", "$JS.hub.API.DIRECT.GET.ANTS_SENSE"] {
        assert!(uplink.publish.iter().any(|have| have == s), "{s} missing from the uplink");
    }
    for (kind, rights) in [
        ("ant", Permissions::for_user(&a, Role::Ant)),
        ("human", Permissions::for_user(&human, Role::Human)),
        ("assistant", Permissions::for_assistant(&pa, &human)),
        ("channel manager", Permissions::for_channel_manager()),
        ("enrollment", Permissions::for_enrollment(&a)),
        ("webhook", Permissions::for_webhook(&p("hook"), &[])),
        ("observer", Permissions::for_observer(&p("watch"))),
        ("enroll approver", Permissions::for_enroll_approver()),
        ("reactions responder", Permissions::for_reactions_responder()),
        ("device revoker", Permissions::for_device_revoker()),
        ("bootstrap", Permissions::for_bootstrap()),
        ("registrar", Permissions::for_registrar()),
    ] {
        let rights = rights.with_js_domain(Some(&domain));
        assert_eq!(reaches(&rights.publish), Vec::<String>::new(), "{kind} publishes");
        assert_eq!(reaches(&rights.subscribe), Vec::<String>::new(), "{kind} subscribes");
        for s in rights.publish.iter().filter(|s| s.starts_with("$JS.")) {
            for api in [
                "STREAM.CREATE",
                "STREAM.UPDATE",
                "STREAM.INFO",
                "STREAM.DELETE",
                "STREAM.PURGE",
                "DIRECT.GET",
                "STREAM.MSG.GET",
                "CONSUMER.CREATE",
                "CONSUMER.DURABLE.CREATE",
            ] {
                for prefix in ["$JS.API", "$JS.hub.API"] {
                    // A family's domain wildcard reaches the stream only through a host's rights
                    // (`no_credentials_hold_a_jetstream_family_right_a_human_lacks`).
                    if prefix != "$JS.API"
                        && (api.starts_with("STREAM.")
                            && !api.contains("DELETE")
                            && !api.contains("PURGE")
                            && !api.contains("MSG")
                            || api == "CONSUMER.CREATE")
                    {
                        continue;
                    }
                    let right = format!("{prefix}.{api}.{SENSE_STREAM}");
                    assert!(!covers(s, &right) && !covers(s, &format!("{right}.{log}")), "{kind} holds {s}");
                }
            }
        }
    }
}
