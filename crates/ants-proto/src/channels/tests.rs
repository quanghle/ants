use super::*;
use crate::{HumanEntry, Role, mention_key, p};

#[test]
fn channel_posts_start_threads_and_are_checked_against_their_subject() {
    let c: ChannelId = "ops".parse().unwrap();
    let top = ChannelPost::new(c.clone(), UserId::human(), "@dev-a, @dev-b: deploy? cc @dev-a");
    assert!(top.starts_thread());
    assert_eq!(top.thread_id.as_str(), top.id.to_string());
    assert_eq!(top.mentions, [p("dev-a"), p("dev-b")]);
    assert!(top.mentions(&p("dev-b")) && !top.mentions(&p("dev-c")));
    assert_eq!((top.turn_budget, top.in_reply_to), (DEFAULT_TURN_BUDGET, None));
    assert_eq!(top.subject(), "ants.chan.ops.human");
    assert_eq!(subject::parse_chan(&top.subject()), Some((c.clone(), UserId::human())));
    assert_eq!(top.check_subject("ants.chan.ops.human"), Ok(()));
    let err = top.check_subject("ants.chan.ops.dev-a").unwrap_err();
    assert_eq!(err.to_string(), "channel post from human in channel ops arrived on ants.chan.ops.dev-a");
    assert!(top.check_subject("ants.chan.dev.human").is_err(), "posted to another channel");

    let two = ChannelPost::new(c.clone(), UserId::human(), "and this");
    assert_ne!(two.thread_id, top.thread_id, "each top-level post starts its own thread");
    assert!(two.mentions.is_empty());
    let on = ChannelPost::on_thread(c, top.thread_id.clone(), UserId::human(), "more");
    assert!(!on.starts_thread());
    assert_eq!(on.thread_id, top.thread_id);

    let json = serde_json::to_string(&two).unwrap();
    assert!(!json.contains("mentions") && !json.contains("in_reply_to"), "{json}");
    assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), two);
    let json = serde_json::to_string(&top).unwrap();
    assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), top);
}

#[test]
fn every_ant_channel_post_spends_a_turn() {
    let humans = Humans::default();
    let c: ChannelId = "ops".parse().unwrap();
    let ask = ChannelPost::new(c.clone(), UserId::human(), "@dev-a deploy?");
    let answer = ask.reply(p("dev-a"), "done, @human", &[], &humans).unwrap();
    assert_eq!((answer.channel.clone(), answer.thread_id.clone()), (c.clone(), ask.thread_id.clone()));
    assert_eq!(answer.in_reply_to, Some(ask.id));
    assert_eq!(answer.turn_budget, DEFAULT_TURN_BUDGET - 1, "waking nobody spends a turn too");
    let mut handoff = ask.reply(p("dev-a"), "@dev-b check it; @dev-a will wait", &[], &humans).unwrap();
    assert_eq!(handoff.turn_budget, DEFAULT_TURN_BUDGET - 1);
    handoff.turn_budget = 1;
    let back = handoff.reply(p("dev-b"), "@dev-a fine", &[], &humans).unwrap();
    assert_eq!(back.turn_budget, 0);
    let spent = Err(Error::BudgetExhausted(ask.thread_id));
    assert_eq!(back.reply(p("dev-a"), "@dev-b again", &[], &humans), spent);
    assert_eq!(back.reply(p("dev-a"), "@human over to you", &[], &humans), spent);
    let human = back.reply(UserId::human(), "@dev-a @dev-b go on", &[], &humans).unwrap();
    assert_eq!(human.turn_budget, DEFAULT_TURN_BUDGET, "the human's post gives a fresh budget");
    assert_eq!(
        handoff.reply(p("dev-c"), "late", &[back.clone(), human], &humans).unwrap().turn_budget,
        7,
        "reset since"
    );
    assert_eq!(handoff.reply(p("dev-c"), "late", &[back], &humans), spent, "an older post spends the thread's turns");
    let own = ChannelPost::new(c, p("dev-a"), "@dev-b status?").spend(3).unwrap();
    assert!(own.starts_thread());
    assert_eq!(own.turn_budget, 2);
}

#[test]
fn ants_woken_by_one_post_spend_the_thread_s_turns_together() {
    let humans = Humans::default();
    let c: ChannelId = "ops".parse().unwrap();
    let mut ask = ChannelPost::new(c.clone(), UserId::human(), "@dev-a @dev-b @dev-c review");
    ask.turn_budget = 4;
    let a = ask.reply(p("dev-a"), "@dev-b @dev-c fine", &[], &humans).unwrap();
    let b = ask.reply(p("dev-b"), "@dev-a @dev-c fine", std::slice::from_ref(&a), &humans).unwrap();
    let other = ChannelPost::on_thread(c, "other".parse().unwrap(), p("dev-z"), "elsewhere");
    let c_ = ask.reply(p("dev-c"), "@dev-a @dev-b fine", &[a.clone(), other, b.clone()], &humans).unwrap();
    assert_eq!([a.turn_budget, b.turn_budget, c_.turn_budget], [3, 2, 1], "one budget, run down across branches");
    let a2 = a.reply(p("dev-a"), "@dev-b again", &[b.clone(), c_.clone()], &humans).unwrap();
    assert_eq!(a2.turn_budget, 0);
    // Every post of the thread is spent out: no branch gets another turn.
    let thread = [ask, a, b, c_, a2];
    for (i, post) in thread.iter().enumerate() {
        let spent = Err(Error::BudgetExhausted(post.thread_id.clone()));
        assert_eq!(post.reply(p("dev-b"), "more", &thread[i + 1..], &humans), spent, "after {}", post.text);
    }
}

#[test]
fn channel_entries_list_members_and_wake_those_mentioned() {
    let humans = Humans::default();
    let c: ChannelId = "ops".parse().unwrap();
    let mut entry = ChannelEntry::new(c.clone());
    assert!(entry.add_member(p("dev-a")) && entry.add_member(p("dev-b")) && entry.add_member(p("dev-c")));
    assert!(!entry.add_member(p("dev-a")), "already a member");
    assert!(entry.remove_member(&p("dev-c")) && !entry.remove_member(&p("dev-c")));
    assert_eq!(entry.members, [p("dev-a"), p("dev-b")]);
    assert!(entry.is_member(&p("dev-a")) && !entry.is_member(&p("dev-c")));
    assert_eq!(entry.key(), "ops");
    assert_eq!(entry.check_key("ops"), Ok(()));
    assert_eq!(entry.check_key("dev").unwrap_err().to_string(), "channel entry of channel ops found at key dev");

    let json = serde_json::to_string(&entry).unwrap();
    assert!(!json.contains("messenger"), "{json}");
    assert_eq!(serde_json::from_str::<ChannelEntry>(&json).unwrap(), entry);
    let minimal: ChannelEntry = serde_json::from_str(r#"{"channel":"ops","topic":"new"}"#).unwrap();
    assert_eq!(minimal, ChannelEntry::new(c.clone()));

    let human = UserId::human();
    let untagged = ChannelPost::new(c.clone(), human.clone(), "anyone?");
    assert_eq!(entry.woken(&untagged, &humans), [UserId::messenger()], "the default messenger triages");
    entry.messenger = Some(p("dev-c"));
    assert_eq!(entry.woken(&untagged, &humans), [p("dev-c")], "the assigned one, member or not");
    assert!(entry.woken(&ChannelPost::new(c.clone(), p("dev-a"), "anyone?"), &humans).is_empty(), "ants never fan out");
    let tagged = ChannelPost::new(c, p("dev-a"), "@dev-b @dev-c @dev-a @human look");
    assert_eq!(entry.woken(&tagged, &humans), [p("dev-b")], "members only, not the author or the human");
    assert!(entry.woken(&ChannelPost::new("dev".parse().unwrap(), human, "@dev-a"), &humans).is_empty());
}

#[test]
fn a_mention_notifies_human_members_and_wakes_ant_members() {
    let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
    let mut humans = Humans::default();
    for h in [&alice, &bob] {
        humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
    }
    let mut entry = ChannelEntry::new(c.clone());
    for member in [p("dev-a"), p("dev-b"), alice.clone()] {
        entry.add_member(member);
    }
    let mixed = ChannelPost::new(c.clone(), p("dev-a"), "@alice @bob @dev-b @dev-c look").with_owner(Some(bob.clone()));
    assert_eq!(entry.woken(&mixed, &humans), [p("dev-b")]);
    assert_eq!(entry.notified(&mixed, &humans), std::slice::from_ref(&alice), "members only: bob is none");
    let human_only = ChannelPost::new(c.clone(), p("dev-a"), "@alice see");
    assert!(entry.woken(&human_only, &humans).is_empty(), "no ant, nor the messenger");
    let by_alice = ChannelPost::new(c.clone(), alice.clone(), "@alice @dev-b note");
    assert!(entry.notified(&by_alice, &humans).is_empty(), "not its author");
    assert_eq!(entry.woken(&by_alice, &humans), [p("dev-b")]);
    assert!(entry.notified(&ChannelPost::new("dev".parse().unwrap(), p("dev-a"), "@alice"), &humans).is_empty());

    let notice = mixed.mention_notice(&alice);
    assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&p("dev-a"), &alice, &mixed.thread_id));
    assert_eq!((notice.text.as_str(), notice.turn_budget), (mixed.text.as_str(), mixed.turn_budget));
    assert_eq!((&notice.owner, &notice.creator), (&None, &None), "a notice claims no thread");
    assert_eq!(notice.mention, Some(Mention { channel: c, post: mixed.id, author: None }));
    assert_eq!(notice.subject(), subject::inbox(&alice, &p("dev-a")));
    assert_eq!(mixed.mention_msg_id(&alice), format!("mention.{}.alice", mixed.id));
    assert_eq!(mention_key(&p("dev-a"), 7, &alice), "dev-a.mention.7.alice");

    let json = serde_json::to_value(&notice).unwrap();
    assert_eq!(json["mention"]["channel"], "ops");
    let mut old = json;
    old.as_object_mut().unwrap().remove("mention");
    let read: Envelope = serde_json::from_value(old).unwrap();
    assert_eq!(read, Envelope { mention: None, ..notice }, "an envelope from before mentions reads as a message");
    let plain = serde_json::to_string(&Envelope::new(p("dev-a"), alice, "hi")).unwrap();
    assert!(!plain.contains("mention"), "{plain}");
}

#[test]
fn the_messenger_relays_the_mention_notices_of_a_webhook_s_post() {
    let (c, alice) = ("ops".parse::<ChannelId>().unwrap(), p("alice"));
    let mut humans = Humans::default();
    humans.add("alice", HumanEntry::new(alice.clone())).unwrap();
    let mut entry = ChannelEntry::new(c.clone());
    entry.add_member(alice.clone());
    let hook = ChannelPost::new(c.clone(), p("github"), "deploy failed").from_webhook(vec![alice.clone()]);
    assert_eq!(entry.notified(&hook, &humans), std::slice::from_ref(&alice));
    assert_eq!(entry.notifier(&hook), UserId::messenger());
    entry.messenger = Some(p("dev-a"));
    assert_eq!(entry.notifier(&hook), p("dev-a"));
    let own = ChannelPost::new(c.clone(), p("dev-b"), "@alice look");
    assert_eq!(entry.notifier(&own), p("dev-b"), "an ant's or a human's post: its author");

    let notice = hook.relayed_notice(&alice, &p("dev-a"));
    assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&p("dev-a"), &alice, &hook.thread_id));
    assert_eq!((notice.text.as_str(), notice.webhook), (hook.text.as_str(), false));
    assert_eq!(notice.mention, Some(Mention { channel: c, post: hook.id, author: Some(p("github")) }));
    assert_eq!(notice.subject(), subject::inbox(&alice, &p("dev-a")));
    let json = serde_json::to_value(&notice).unwrap();
    assert_eq!(json["mention"]["author"], "github");
    assert!(serde_json::to_value(hook.mention_notice(&alice)).unwrap()["mention"].get("author").is_none());
}

#[test]
fn any_human_s_posts_count_as_human_and_threads_keep_the_owner_who_started_them() {
    let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
    let mut humans = Humans::default();
    for h in [&alice, &bob] {
        humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
    }
    let mut entry = ChannelEntry::new(c.clone());
    entry.add_member(p("dev-a"));
    let top = ChannelPost::new(c.clone(), alice.clone(), "the build is red").with_owner(Some(alice.clone()));
    assert_eq!(entry.woken(&top, &humans), [UserId::messenger()], "alice's post goes to the messenger");
    assert!(entry.woken(&top, &Humans::default()).is_empty(), "unless alice is no human");
    let at_alice = ChannelPost::new(c.clone(), p("dev-a"), "@alice @dev-a look");
    assert!(entry.woken(&at_alice, &humans).is_empty() && !at_alice.wakes_ants(&humans));

    let answer = top.reply(p("dev-a"), "@dev-a on it", &[], &humans).unwrap();
    assert_eq!((answer.owner.as_ref(), answer.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET - 1));
    let mut spent = answer.reply(p("dev-b"), "@dev-a done", &[], &humans).unwrap();
    spent.turn_budget = 1;
    let by_bob = spent.reply(bob.clone(), "@dev-a again", &[], &humans).unwrap();
    assert_eq!((by_bob.owner.as_ref(), by_bob.turn_budget), (Some(&alice), DEFAULT_TURN_BUDGET), "fresh, alice's");
    assert_eq!(spent.turns_left(std::slice::from_ref(&by_bob), &humans), DEFAULT_TURN_BUDGET);

    // A thread from before owners: its human author owns it, else the first human to answer on it.
    let old = ChannelPost::new(c.clone(), alice.clone(), "old");
    assert_eq!(old.reply(p("dev-a"), "a", &[], &humans).unwrap().owner, Some(alice.clone()));
    let ant_post = ChannelPost::new(c, p("dev-a"), "@dev-b status?");
    assert_eq!(ant_post.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);
    assert_eq!(ant_post.reply(bob.clone(), "b", &[], &humans).unwrap().owner, Some(bob));
    // Only a human owns a thread: an owner an ant names is ignored.
    let named = ChannelPost::new("ops".parse().unwrap(), p("dev-a"), "@dev-b go").with_owner(Some(p("dev-a")));
    assert_eq!(named.thread_owner(&humans), None);
    assert_eq!(named.reply(p("dev-b"), "b", &[], &humans).unwrap().owner, None);

    let json = serde_json::to_string(&old).unwrap();
    assert!(!json.contains("owner"), "{json}");
    assert!(serde_json::to_string(&top).unwrap().contains(r#""owner":"alice""#));
    assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), old, "posts without an owner still read");
}

#[test]
fn a_webhook_s_post_that_mentions_nobody_is_triaged_and_its_mentions_come_from_the_ingress() {
    let humans = Humans::default();
    let c: ChannelId = "ops".parse().unwrap();
    let mut entry = ChannelEntry::new(c.clone());
    entry.add_member(p("dev-a"));
    let hook = p("github");
    let untagged = ChannelPost::new(c.clone(), hook.clone(), "@dev-a push to main").from_webhook(Vec::new());
    assert!(untagged.mentions.is_empty(), "never parsed from the text");
    assert!(untagged.to_triage(&humans));
    assert_eq!(entry.woken(&untagged, &humans), [UserId::messenger()]);
    let tagged = ChannelPost::new(c, hook, "push").from_webhook(vec![p("dev-a"), p("dev-b")]);
    assert_eq!(entry.woken(&tagged, &humans), [p("dev-a")], "members only");

    let json = serde_json::to_string(&untagged).unwrap();
    assert!(json.contains(r#""webhook":true"#), "{json}");
    assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap(), untagged);
    let reply = untagged.reply(p("dev-a"), "on it", &[], &humans).unwrap();
    assert!(!reply.webhook && !serde_json::to_string(&reply).unwrap().contains("webhook"), "replies are no webhook's");
}

#[test]
fn a_webhook_under_a_human_s_name_is_no_human() {
    let (c, bob) = ("ops".parse::<ChannelId>().unwrap(), p("bob"));
    let mut humans = Humans::default();
    humans.add(bob.as_str(), HumanEntry::new(bob.clone())).unwrap();
    let hook = ChannelPost::new(c.clone(), bob.clone(), "red").from_webhook(Vec::new());
    assert!(!hook.by_human(&humans) && ChannelPost::new(c.clone(), bob.clone(), "hi").by_human(&humans));
    assert_eq!(hook.thread_owner(&humans), None, "it owns no thread");
    let mut spent = hook.clone();
    spent.turn_budget = 1;
    let again = ChannelPost::on_thread(c, hook.thread_id, bob.clone(), "red").from_webhook(Vec::new());
    assert_eq!(spent.turns_left(&[again], &humans), 1, "nor renews its turns");

    let message = Envelope::new(bob.clone(), p("dev-a"), "red");
    assert_eq!(
        (message.sender_role(&humans), message.clone().from_webhook().sender_role(&humans)),
        (Role::Human, Role::Ant)
    );
    assert!(!serde_json::to_string(&message).unwrap().contains("webhook"));
    let json = serde_json::to_string(&message.clone().from_webhook()).unwrap();
    assert!(json.contains(r#""webhook":true"#), "{json}");
    assert!(serde_json::from_str::<Envelope>(&json).unwrap().webhook);
    let reply = message.from_webhook().reply(p("dev-a"), "on it", Role::Human).unwrap();
    assert!(!reply.webhook, "replies are no webhook's");
}

#[test]
fn a_thread_s_origin_comes_from_its_posts_not_from_what_a_later_post_carries() {
    let (c, alice, bob) = ("ops".parse::<ChannelId>().unwrap(), p("alice"), p("bob"));
    let mut humans = Humans::default();
    for h in [&alice, &bob] {
        humans.add(h.as_str(), HumanEntry::new(h.clone())).unwrap();
    }
    let on = |from: &UserId, thread: &ThreadId| ChannelPost::on_thread(c.clone(), thread.clone(), from.clone(), "x");
    let root = ChannelPost::new(c.clone(), p("lead"), "@dev-a fix it").with_owner(Some(bob.clone()));
    let thread = root.thread_id.clone();
    let forged = on(&p("dev-a"), &thread).with_owner(Some(bob.clone())).with_creator(Some(p("boss")));
    let mut origin = ThreadOrigin::default();
    origin.push(&root, &humans);
    assert_eq!((&origin.owner, &origin.creator, origin.root), (&None, &Some(p("lead")), true), "an ant owns none");
    origin.push(&forged, &humans);
    // A later post with the thread's id as its own only claims to start it.
    let mut fake = on(&p("dev-b"), &thread);
    fake.id = root.id;
    origin.push(&fake, &humans);
    assert_eq!((&origin.owner, &origin.creator, origin.settled()), (&None, &Some(p("lead")), false));
    // The first human to post owns the thread.
    origin.push(&on(&alice, &thread), &humans);
    origin.push(&on(&bob, &thread), &humans);
    assert_eq!((&origin.owner, origin.settled()), (&Some(alice.clone()), true));
    let mut post = forged.clone();
    origin.apply(&mut post);
    assert_eq!((post.owner, post.creator), (Some(alice.clone()), Some(p("lead"))));

    // With its top-level post no longer kept, the thread has no creator.
    let mut lost = ThreadOrigin::default();
    lost.push(&forged, &humans);
    assert_eq!((&lost.owner, &lost.creator, lost.root), (&None, &None, false));
    // A webhook's top-level post names the owner its config does, a human; a human's owns its thread.
    let hook = ChannelPost::new(c.clone(), p("ci"), "red").from_webhook(Vec::new());
    let of = |post: ChannelPost| {
        let origin = ThreadOrigin::of_root(&post, &humans);
        (origin.owner, origin.creator)
    };
    assert_eq!(of(hook.clone().with_owner(Some(alice.clone()))), (Some(alice.clone()), None));
    assert_eq!(of(hook.with_owner(Some(p("dev-a")))), (None, None));
    assert_eq!(of(ChannelPost::new(c, bob.clone(), "hi").with_owner(Some(alice))), (Some(bob), None));
}

#[test]
fn threads_keep_the_ant_that_created_them_and_only_its_own_post_makes_it_so() {
    let (c, alice) = ("ops".parse::<ChannelId>().unwrap(), p("alice"));
    let mut humans = Humans::default();
    humans.add(alice.as_str(), HumanEntry::new(alice.clone())).unwrap();
    // A top-level post's creator is its author, whatever it says.
    let top = ChannelPost::new(c.clone(), p("lead"), "@dev-a fix it").with_creator(Some(p("boss")));
    assert_eq!(top.thread_creator(&humans), Some(p("lead")));
    assert_eq!(ChannelPost::new(c.clone(), alice.clone(), "hi").thread_creator(&humans), None, "humans own");
    let hook = ChannelPost::new(c.clone(), p("ci"), "red").from_webhook(vec![p("dev-a")]);
    assert_eq!(hook.thread_creator(&humans), None, "a webhook is no ant");
    let manager = ChannelPost::new(c.clone(), UserId::channel_manager(), "dev-b joined");
    assert_eq!(manager.thread_creator(&humans), None, "nor is the channel manager");
    let answer = top.reply(p("dev-a"), "@dev-b help", &[], &humans).unwrap();
    assert_eq!((answer.creator.as_ref(), answer.owner.as_ref()), (Some(&p("lead")), None));
    let by_alice = answer.reply(alice.clone(), "@dev-a go on", &[], &humans).unwrap();
    assert_eq!((by_alice.creator.as_ref(), by_alice.owner.as_ref()), (Some(&p("lead")), Some(&alice)));
    assert_eq!(by_alice.reply(p("dev-b"), "ok", &[], &humans).unwrap().creator, Some(p("lead")));
    // A later post keeps the creator it carries, unless a human.
    let named = ChannelPost::on_thread(c, top.thread_id.clone(), p("dev-b"), "x").with_creator(Some(alice.clone()));
    assert_eq!(named.thread_creator(&humans), None);

    // Messages on or from the thread keep it too.
    let msg = Envelope::new(p("lead"), p("dev-a"), "q").with_creator(Some(p("lead")));
    assert_eq!(msg.reply(p("dev-a"), "a", Role::Ant).unwrap().creator, Some(p("lead")));
    assert_eq!(msg.follow_up(p("dev-a"), p("human"), "n").creator, Some(p("lead")));

    let json = serde_json::to_string(&ChannelPost::new("ops".parse().unwrap(), p("lead"), "x")).unwrap();
    assert!(!json.contains("creator"), "{json}");
    assert!(serde_json::to_string(&top).unwrap().contains(r#""creator":"boss""#));
    assert_eq!(serde_json::from_str::<ChannelPost>(&json).unwrap().creator, None, "older posts read");
    let older = r#"{"id":"0190a5c4-1d2e-7f00-8000-000000000001","thread_id":"t1","from":"human","to":"dev-a",
            "text":"hi","turn_budget":8,"sent_at":"2026-01-01T00:00:00Z"}"#;
    assert_eq!(serde_json::from_str::<Envelope>(older).unwrap().creator, None);
}
