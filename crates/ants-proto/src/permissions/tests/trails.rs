//! Identities and trails: each ant writes its own identity, runs, tips, votes and ratings, humans save trails and
//! ask their conductors.

use super::*;

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
        [
            "$JS.API.STREAM.CREATE.KV_ants_identities",
            "$JS.API.DIRECT.GET.KV_ants_identities",
            "$KV.ants_identities.*",
            "$JS.API.STREAM.UPDATE.KV_ants_identities"
        ]
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
fn humans_save_trails_ants_ask_the_keeper_and_each_ant_writes_only_the_runs_it_conducts() {
    let trails = |perms: &Permissions| -> Vec<String> {
        perms.publish.iter().chain(&perms.subscribe).filter(|s| s.contains("ants_trail")).cloned().collect()
    };
    assert_eq!(
        trails(&Permissions::for_user(&p("dev-a"), Role::Ant)),
        [
            "$JS.API.DIRECT.GET.KV_ants_trails",
            "$JS.API.DIRECT.GET.KV_ants_trail_tips",
            "$JS.API.DIRECT.GET.KV_ants_trail_ratings",
            "$JS.API.DIRECT.GET.KV_ants_trail_runs.$KV.ants_trail_runs.dev-a.>",
            "$JS.API.CONSUMER.CREATE.KV_ants_trail_runs.runsread-dev-a.$KV.ants_trail_runs.dev-a.>",
            "$JS.API.CONSUMER.MSG.NEXT.KV_ants_trail_runs.runsread-dev-a",
            "$JS.API.CONSUMER.DELETE.KV_ants_trail_runs.runsread-dev-a",
            "$KV.ants_trail_runs.dev-a.>",
            "$KV.ants_trail_tips.*.dev-a.>",
            "$KV.ants_trail_ratings.*.dev-a.*"
        ]
    );
    assert_eq!(
        trails(&Permissions::for_user(&p("alice"), Role::Human)),
        [
            "$JS.API.DIRECT.GET.KV_ants_trails",
            "$JS.API.DIRECT.GET.KV_ants_trail_tips",
            "$JS.API.DIRECT.GET.KV_ants_trail_ratings",
            "$JS.API.STREAM.CREATE.KV_ants_trails",
            "$JS.API.STREAM.INFO.KV_ants_trails",
            "$KV.ants_trails.*",
            "$JS.API.STREAM.CREATE.KV_ants_trail_runs",
            "$JS.API.STREAM.INFO.KV_ants_trail_runs",
            "$JS.API.DIRECT.GET.KV_ants_trail_runs",
            "$JS.API.STREAM.CREATE.KV_ants_trail_tips",
            "$JS.API.STREAM.INFO.KV_ants_trail_tips",
            "$KV.ants_trail_tips.>",
            "$JS.API.STREAM.PURGE.KV_ants_trail_tips",
            "$JS.API.STREAM.CREATE.KV_ants_trail_ratings",
            "$JS.API.STREAM.INFO.KV_ants_trail_ratings",
            "$JS.API.STREAM.PURGE.KV_ants_trail_ratings",
            "$JS.API.STREAM.UPDATE.KV_ants_trails",
            "$JS.API.STREAM.UPDATE.KV_ants_trail_runs",
            "$JS.API.STREAM.UPDATE.KV_ants_trail_tips",
            "$JS.API.STREAM.UPDATE.KV_ants_trail_ratings"
        ]
    );
    let may = |perms: &Permissions, s: &str| perms.publish.iter().any(|have| covers(have, s));
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let run: crate::RunId = "r1".parse().unwrap();
    for denied in ["$KV.ants_trail_runs.dev-b.r1", "$KV.ants_trail_runs.dev-a"] {
        assert!(!may(&ant, denied), "an ant may publish {denied}");
    }
    // No trail's key, as no subject names its saver: an ant asks the keeper, on its own subject only.
    let domain: crate::JsDomain = "hub".parse().unwrap();
    let in_domain = ant.clone().with_js_domain(Some(&domain));
    assert!(!may(&ant, "$KV.ants_trails.deploy") && !may(&in_domain, "$JS.hub.API.$KV.ants_trails.deploy"));
    assert!(may(&ant, "ants.savetrail.dev-a") && !may(&ant, "ants.savetrail.dev-b"));
    assert!(ant.subscribe.iter().any(|have| covers(have, "_INBOX.dev-a.savetrail")));
    assert!(!may(&ant, "_INBOX.dev-b.savetrail"));
    // Only the keeper, `ants signer`, writes any trail for an ant and answers it.
    let keeper = Permissions::for_trail_keeper();
    for allowed in ["$KV.ants_trails.deploy", "_INBOX.dev-a.savetrail", "$JS.API.DIRECT.GET.KV_ants_trails"] {
        assert!(may(&keeper, allowed), "the keeper may not publish {allowed}");
    }
    assert!(keeper.subscribe.iter().any(|have| covers(have, "ants.savetrail.dev-a")));
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert!(!may(&human, "_INBOX.dev-a.savetrail"), "a human answers no trail save");
    assert!(!may(&ant, "$JS.API.DIRECT.GET.KV_ants_trail_runs.$KV.ants_trail_runs.dev-b.r1"));
    for key in [crate::trail_run_key(&p("dev-a"), &run), crate::trail_call_key(&p("dev-a"), &run, 3)] {
        assert!(may(&ant, &format!("$KV.ants_trail_runs.{key}")), "{key}");
    }
    // An assistant conducts and saves trails as an ant does, as itself, never writing one as its human.
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    assert!(!may(&assistant, "$KV.ants_trails.deploy"));
    assert!(may(&assistant, "ants.savetrail.pa"));
    assert!(may(&assistant, "$KV.ants_trail_runs.pa.r1.session"));
    let observer = Permissions::for_observer(&p("watcher"));
    let webhook = Permissions::for_webhook(&p("hook"), &[Destination::Ant(p("dev-a"))]);
    for none in [observer, webhook, Permissions::for_channel_manager(), Permissions::for_host(&"m1".parse().unwrap())] {
        assert_eq!(trails(&none), Vec::<String>::new());
    }
}

#[test]
fn each_ant_leaves_its_own_tips_and_votes_and_only_humans_retire_or_purge_them() {
    let may = |perms: &Permissions, s: &str| perms.publish.iter().any(|have| covers(have, s));
    let (own_tip, others_tip) =
        ("$KV.ants_trail_tips.deploy.dev-a.k3f7q2ab", "$KV.ants_trail_tips.deploy.dev-b.k3f7q2ab");
    let (own_vote, others_vote) =
        ("$KV.ants_trail_tips.deploy.dev-a.dev-b.k3f7q2ab", "$KV.ants_trail_tips.deploy.dev-b.dev-a.k3f7q2ab");
    let purge = "$JS.API.STREAM.PURGE.KV_ants_trail_tips";
    let retire = "$KV.ants_trail_tips.deploy._retired.dev-a.k3f7q2ab";
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(may(&ant, own_tip) && may(&ant, own_vote));
    for denied in [others_tip, others_vote, purge, retire] {
        assert!(!may(&ant, denied), "an ant may publish {denied}");
    }
    let human = Permissions::for_user(&p("alice"), Role::Human);
    for allowed in [own_tip, others_tip, own_vote, purge, retire] {
        assert!(may(&human, allowed), "a human may not publish {allowed}");
    }
    // An assistant votes and leaves tips as itself, and retires or purges none for its human.
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    assert!(may(&assistant, "$KV.ants_trail_tips.deploy.pa.k3f7q2ab"));
    assert!(may(&assistant, "$KV.ants_trail_tips.deploy.pa.dev-a.k3f7q2ab"));
    assert!(may(&assistant, "$JS.API.DIRECT.GET.KV_ants_trail_tips"));
    for denied in [own_tip, others_vote, purge, retire] {
        assert!(!may(&assistant, denied), "an assistant may publish {denied}");
    }
    let domain: JsDomain = "hub".parse().unwrap();
    let leaf = ant.with_js_domain(Some(&domain));
    assert!(may(&leaf, "$JS.hub.API.DIRECT.GET.KV_ants_trail_tips"));
    assert!(may(&leaf, &format!("$JS.hub.API.{own_tip}")) && !may(&leaf, &format!("$JS.hub.API.{others_tip}")));
}

#[test]
fn each_ant_rates_only_its_own_runs_and_humans_only_purge_ratings() {
    let may = |perms: &Permissions, s: &str| perms.publish.iter().any(|have| covers(have, s));
    let run = "019a7d2f-0000-7000-8000-000000000001";
    let (own, others) =
        (format!("$KV.ants_trail_ratings.deploy.dev-a.{run}"), format!("$KV.ants_trail_ratings.deploy.dev-b.{run}"));
    let (deeper, purge) = (format!("{own}.x"), "$JS.API.STREAM.PURGE.KV_ants_trail_ratings");
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(may(&ant, &own) && may(&ant, "$JS.API.DIRECT.GET.KV_ants_trail_ratings"));
    for denied in [&others, &deeper, purge] {
        assert!(!may(&ant, denied), "an ant may publish {denied}");
    }
    let human = Permissions::for_user(&p("alice"), Role::Human);
    assert!(may(&human, purge) && !may(&human, &own), "a human purges ratings and writes none");
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    assert!(may(&assistant, &format!("$KV.ants_trail_ratings.deploy.pa.{run}")));
    for denied in [own.as_str(), purge, "$JS.API.STREAM.CREATE.KV_ants_trail_ratings"] {
        assert!(!may(&assistant, denied), "an assistant may publish {denied}");
    }
    let leaf = ant.with_js_domain(Some(&"hub".parse().unwrap()));
    assert!(may(&leaf, "$JS.hub.API.DIRECT.GET.KV_ants_trail_ratings"));
    assert!(may(&leaf, &format!("$JS.hub.API.{own}")) && !may(&leaf, &format!("$JS.hub.API.{others}")));
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
