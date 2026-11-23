//! Channels: who reads and writes their entries, changes their members, messenger and description, and what the
//! channel manager does.

use super::*;

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
        [("inbox-dev-a", "ants.inbox.dev-a.>"), ("notes-dev-a", "ants.notes.>"), ("chanread-dev-a", "ants.chan.>")]
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
    assert_eq!(
        (cleared, entry.messenger.as_ref()),
        (vec!["messenger back to the default system-messenger".to_owned()], None)
    );
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
    for bad in ["evil\u{202e}gnp.exe", "red \u{1b}[31m", "\u{2066}isolated"] {
        let bidi = [ChannelOp::SetDescription { description: bad.to_owned() }];
        let Err(Refusal::Invalid(why)) = entry.apply_for_human(&bidi) else { panic!("{bad:?} taken") };
        assert!(why.contains("without control characters but whitespace, or characters reordering text"), "{why}");
    }
    let lines = [ChannelOp::SetDescription { description: "Incidents\n\tand upkeep".to_owned() }];
    assert_eq!(entry.apply_for_human(&lines).unwrap().len(), 1, "line breaks and tabs stay");
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
    // Humans read its version.
    let read = "$JS.API.DIRECT.GET.KV_ants_webhooks.$KV.ants_webhooks.channel-manager.serve";
    assert!(Permissions::for_user(&UserId::human(), Role::Human).publish.iter().any(|s| s == read));
    let marker = Serving::here("m1");
    let json = serde_json::to_string(&marker).unwrap();
    assert_eq!(json, format!(r#"{{"machine":"m1","version":"{ANTS_VERSION}","perms":{PERMISSIONS_VERSION}}}"#));
    assert_eq!(serde_json::from_str::<Serving>(&json).unwrap(), marker);

    let a = Permissions::for_user(&p("dev-a"), Role::Ant);
    assert!(a.publish.iter().any(|s| s == "ants.chanadm.*.dev-a"));
    assert!(!a.publish.iter().any(|s| s.starts_with("ants.out.chanadm")), "no outbox form");
    assert!(!a.subscribe.iter().any(|s| s.starts_with("ants.chanadm")));
}
