//! Sense's event logs, which only hosts reach, and the hub's streams taking what a leaf's members store.

use super::*;

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
        ("trail keeper", Permissions::for_trail_keeper()),
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

/// How nats-server's `ReverseMatch` sees `allowance` against `interest`, a subject ending in `>`: `None` when the
/// allowance publishes nothing under it, else whether it finds the allowance. Below the interest's tokens it descends
/// only through literal ones, so a `*` before the allowance's last token hides it.
fn reverse_matched(allowance: &str, interest: &str) -> Option<bool> {
    let prefix: Vec<&str> = interest.strip_suffix(".>").expect("an interest ending in .>").split('.').collect();
    let tokens: Vec<&str> = allowance.split('.').collect();
    for (i, want) in prefix.iter().enumerate() {
        match tokens.get(i) {
            Some(&">") => return Some(true),
            Some(t) if *t == "*" || t == want => {}
            _ => return None,
        }
    }
    let below = tokens.get(prefix.len()..).filter(|below| !below.is_empty())?;
    Some(below[..below.len() - 1].iter().all(|t| *t != "*"))
}

/// The hub sends a leaf the interest of the streams in the bus's account (one per subject the store account exports)
/// only if the leaf's uplink allows publishing a subject `ReverseMatch` finds under it, once it checks, as after an
/// update of the account: for each one a member stores under, there is such a subject, whatever its role.
#[test]
fn the_hub_s_streams_take_what_a_leaf_s_members_store() {
    let m1: Machine = "m1".parse().unwrap();
    let (alice, dev_a, pa) = (p("alice"), p("dev-a"), p("pa"));
    let mut interests: Vec<&str> = crate::STREAM_SUBJECTS.to_vec();
    interests.extend(["$KV.>", "$O.>"]);
    for (role, leaf) in [
        ("a human", Permissions::for_leaf(&m1, [(&alice, LeafRole::Human)])),
        ("an ant", Permissions::for_leaf(&m1, [(&dev_a, LeafRole::Ant)])),
        ("an assistant", Permissions::for_leaf(&m1, [(&pa, LeafRole::AssistantOf(&alice))])),
    ] {
        for interest in &interests {
            let seen: Vec<_> = leaf.publish.iter().filter_map(|s| Some((s, reverse_matched(s, interest)?))).collect();
            assert!(
                seen.is_empty() || seen.iter().any(|(_, found)| *found),
                "{role} stores under {interest} on {:?} only, none of which nats-server finds",
                seen.iter().map(|(s, _)| s).collect::<Vec<_>>()
            );
        }
    }
    assert_eq!(reverse_matched("ants.approvals.*.*", "ants.approvals.>"), Some(false));
    assert_eq!(reverse_matched("ants.approvals.alice.alice", "ants.approvals.>"), Some(true));
    assert_eq!(reverse_matched("ants.approvals.alice", "ants.approvals.>"), Some(true));
    assert_eq!(reverse_matched("ants.inbox.>", "ants.inbox.>"), Some(true));
    assert_eq!(reverse_matched("ants.notes.*.alice", "ants.inbox.>"), None);
    assert_eq!(reverse_matched("ants.approvals", "ants.approvals.>"), None);
}
