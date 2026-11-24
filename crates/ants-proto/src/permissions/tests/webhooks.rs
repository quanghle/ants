//! Webhooks and observers, which send or read little, and whose stream and consumer info each kind of credentials
//! gets.

use super::*;

#[test]
fn a_webhook_may_post_only_to_its_destinations_and_keep_only_its_own_events() {
    let hook = p("github");
    let ops: ChannelId = "ops".parse().unwrap();
    let perms = Permissions::for_webhook(&hook, &[Destination::Ant(p("dev-a")), Destination::Channel(ops)]);
    assert_eq!(
        perms.publish,
        [
            "ants.inbox.dev-a.github.webhook",
            "ants.chan.ops.github.webhook",
            "$JS.API.DIRECT.GET.KV_ants_channels.$KV.ants_channels.ops",
            "$JS.API.CONSUMER.INFO.ANTS.inbox-github",
            "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.github.>",
            "$KV.ants_webhooks.github.>",
            "$KV.ants_owners.*.dev-a",
            "ants.renew.github",
        ]
    );
    assert_eq!(perms.subscribe, ["_INBOX.github.>", "ants.renew.github.reply"]);
    // Its subjects say it is a webhook, as the issuing human's credentials do: no ant's credentials publish on them,
    // nor its on an ant's.
    let ant = Permissions::for_user(&hook, Role::Ant);
    let webhooks = |subject: &str| subject::is_webhooks(subject);
    assert!(
        perms.publish.iter().filter(|s| s.starts_with("ants.")).all(|s| s.starts_with("ants.renew.") || webhooks(s))
    );
    assert!(!ant.publish.iter().any(|s| s.ends_with(".webhook") || s.ends_with(".>") && s.starts_with("ants.inbox")));
    let to_channel = Permissions::for_webhook(&hook, &[Destination::Channel("ops".parse().unwrap())]);
    assert!(!to_channel.publish.iter().any(|s| s.contains("ants_owners")), "posts' owners come from the posts");
    let domain: JsDomain = "hub".parse().unwrap();
    let in_domain = perms.with_js_domain(Some(&domain));
    assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.$KV.ants_webhooks.github.>"));
    let human = Permissions::for_user(&crate::human(), Role::Human);
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
    let human = Permissions::for_user(&crate::human(), Role::Human);
    assert_eq!(infos(&human), ["$JS.API.STREAM.INFO.ANTS", "$JS.API.STREAM.INFO.KV_ants_webhooks"]);
    // Each finds JetStream on connecting through the info of a consumer named after it.
    let manager = Permissions::for_channel_manager();
    assert!(manager.publish.iter().any(|s| s == "$JS.API.CONSUMER.INFO.ANTS.inbox-channel-manager"));
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(ant.publish.iter().any(|s| s == "$JS.API.CONSUMER.INFO.ANTS.inbox-dev-a"));
}

#[test]
fn an_ant_reads_where_its_own_inbox_and_channel_consumers_stand_and_no_other_s() {
    let infos = |perms: &Permissions| -> Vec<String> {
        let mut infos: Vec<String> =
            perms.publish.iter().filter(|s| s.starts_with("$JS.API.CONSUMER.INFO.ANTS.")).cloned().collect();
        infos.sort();
        infos
    };
    let want = ["$JS.API.CONSUMER.INFO.ANTS.chan-dev-a", "$JS.API.CONSUMER.INFO.ANTS.inbox-dev-a"];
    assert_eq!(infos(&Permissions::for_user(&p("dev-a"), Role::Ant)), want);
    let domain: JsDomain = "hub".parse().unwrap();
    let in_domain = Permissions::for_user(&p("dev-a"), Role::Ant).with_js_domain(Some(&domain));
    assert!(in_domain.publish.iter().any(|s| s == "$JS.hub.API.CONSUMER.INFO.ANTS.chan-dev-a"));
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    for s in ["$JS.API.CONSUMER.INFO.ANTS.chan-pa", "$JS.API.CONSUMER.INFO.ANTS.inbox-pa"] {
        assert!(assistant.publish.iter().any(|have| have == s), "{s} missing from {:?}", assistant.publish);
    }
}
