//! The buckets humans write: the humans, read markers, holds, thread titles and states, reactions and device keys,
//! each human its own entries.

use super::*;

#[test]
fn every_human_has_the_human_rights_and_only_humans_write_the_humans_bucket() {
    let alice = Permissions::for_user(&p("alice"), Role::Human);
    let human = Permissions::for_user(&crate::human(), Role::Human);
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
        "$JS.API.CONSUMER.CREATE.ANTS.inbox-alice.ants.inbox.alice.>",
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
    let named_human = Permissions::for_user(&crate::human(), Role::Ant);
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
    let leaf = Permissions::for_leaf(&m1, [(&p("alice"), Role::Human), (&p("dev-a"), Role::Ant)]);
    for s in ["$KV.ants_humans.*", "$KV.ants_humans.alice.>"] {
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
        "$KV.ants_threads.done.>",
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
        "$JS.API.CONSUMER.CREATE.KV_ants_threads.done-dev-a.$KV.ants_threads.done.*.*".to_owned(),
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
    assert_eq!(
        puts,
        [
            "$KV.ants_devices.revoked.*",
            "$KV.ants_identities.*",
            "$KV.ants_identities.growth.*",
            "$KV.ants_identities.*.settings"
        ]
    );
    assert!(!revoker.publish.iter().any(|s| s.contains("STREAM.CREATE") || s.contains("STREAM.UPDATE")));
    assert!(!revoker.publish.iter().any(|s| s.starts_with("ants.") || s.contains("CONSUMER")));
    assert_eq!(revoker.subscribe, [format!("{ENROLL_INBOX}.>")]);
}
