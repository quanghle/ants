//! What each writer keeps for itself: attachments, escalation chains, the account key's place, a host's machine
//! record, orders and admin payloads, and an ant's timeline.

use super::*;

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
