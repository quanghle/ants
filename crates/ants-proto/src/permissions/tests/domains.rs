//! The JetStream API under a domain: the domain forms credentials hold beside the plain ones, and the ordered
//! consumers of a bucket they recreate.

use super::*;

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

    // A human's leaf outbox rights name any domain already, so the hub's form takes no more room; the others are
    // listed one by one, the plain forms staying as they are.
    let human = Permissions::for_user(&p("alice"), Role::Human);
    let in_domain = human.clone().with_js_domain(Some(&domain)).publish;
    assert_eq!(in_domain[..human.publish.len()], human.publish);
    assert!(in_domain.iter().any(|s| s == "$JS.*.API.STREAM.INFO.ANTS_OUT"));
    assert!(in_domain.iter().any(|s| s == "$JS.*.API.CONSUMER.*.ANTS_OUT.ANTS_IN"));
    // What a leaf node maps its own domain to, for the CLI run on the machine.
    assert!(in_domain.iter().any(|s| s == "$JS.API.CONSUMER.*.ANTS_OUT.ANTS_IN"));
    assert!(!in_domain.iter().any(|s| s == "$JS.hub.API.STREAM.INFO.ANTS_OUT"), "covered by the wildcard");
    for s in ["$JS.hub.API.STREAM.INFO.ANTS", "$JS.hub.API.STREAM.MSG.GET.ANTS"] {
        assert!(in_domain.iter().any(|have| have == s), "{s}");
    }
}

/// The store account maps a domain form of the JetStream API onto the plain one past the team account's import, where
/// no user permissions are checked: every domain form credentials hold is one of a plain right they hold too.
#[test]
fn every_domain_form_is_that_of_a_plain_right() {
    let (human, a, pa) = (UserId::human(), p("dev-a"), p("pa"));
    let m1: Machine = "m1".parse().unwrap();
    let domain: JsDomain = "hub".parse().unwrap();
    for (kind, rights) in [
        ("human", Permissions::for_user(&human, Role::Human)),
        ("ant", Permissions::for_user(&a, Role::Ant)),
        ("assistant", Permissions::for_assistant(&pa, &human)),
        ("host", Permissions::for_host(&m1)),
        ("channel manager", Permissions::for_channel_manager()),
        ("enrollment", Permissions::for_enrollment(&a)),
        ("webhook", Permissions::for_webhook(&p("hook"), &[])),
        ("observer", Permissions::for_observer(&p("watch"))),
        ("enroll approver", Permissions::for_enroll_approver()),
        ("reactions responder", Permissions::for_reactions_responder()),
        ("trail keeper", Permissions::for_trail_keeper()),
        ("device revoker", Permissions::for_device_revoker()),
        ("bootstrap", Permissions::for_bootstrap()),
        ("registrar", Permissions::for_registrar()),
    ] {
        let rights = rights.with_js_domain(Some(&domain)).publish;
        for s in &rights {
            let Some(plain) = s.strip_prefix("$JS.").and_then(|rest| Some(rest.split_once(".API.")?.1)) else {
                continue;
            };
            let plain = if plain.starts_with("$KV.") { plain.to_owned() } else { format!("$JS.API.{plain}") };
            assert!(rights.iter().any(|have| covers(have, &plain)), "{kind} holds {s} without {plain}");
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
