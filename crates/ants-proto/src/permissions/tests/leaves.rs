//! A leaf node's uplink: the rights of the users it carries, each once, what its outboxes take and what the hub
//! admits from a machine.

use super::*;

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
    assert!(!leaf.publish.iter().any(|s| s.contains(".leaf.")), "no outbox to set up");
    let ant = Permissions::for_leaf(&m1, [(&pa, LeafRole::Ant)]);
    assert!(!ant.publish.iter().any(|s| s.starts_with("ants.admin.")), "an ant sends no admin order");
    assert_eq!(Permissions::for_leaf(&m1, [(&pa, Role::Ant)]), Permissions::for_leaf(&m1, [(&pa, LeafRole::Ant)]));
}

#[test]
fn leaf_permissions_cover_each_user_once() {
    let (human, a, b) = (crate::human(), p("dev-a"), p("dev-b"));
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
            // Asking for any renewal on the machine.
            "ants.renew.*",
        ]
    );
    let mut extra: Vec<_> = only_a.subscribe.iter().filter(|s| !own.subscribe.contains(s)).collect();
    assert_eq!(extra.pop().map(String::as_str), Some("ants.renew.*.reply"), "hearing the renewals");
    assert_eq!(extra.remove(0), "_INBOX_HOST.m1.>", "hearing the host connection's replies");
    assert_eq!(extra, ["ants.enroll.*.reply"], "hearing the answer, and nothing of the outbox");
    // The store uplink lets the hub source the outbox of m1, and the leaf deliver it on m1's subjects only.
    let store = Permissions::for_leaf_store(&m1);
    assert_eq!(store.publish, ["$JS.S.m1.>", "$JSC.R.m1.*.ANTS_OUT.>", "_R_.>"]);
    let extra: Vec<_> = store.subscribe.iter().collect();
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
    let human = Permissions::for_user(&crate::human(), Role::Human);
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
    let human = m1.admitted(&crate::human(), Role::Human, true);
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
