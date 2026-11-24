//! Tests of channels as humans run them: sigils, the table, mentions, posts on threads, reading back by thread, webhook
//! threads, and read markers.

use std::time::Duration;

use ants_proto::{ChannelNotify, ChannelReadMarkers, STREAM, chan_reader};

use super::*;
use crate::test_support::Server;

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn ops() -> ChannelId {
    "ops".parse().unwrap()
}

async fn read(js: &jetstream::Context, reader: &Me, channel: &ChannelId) -> Result<Vec<ChannelPost>> {
    Ok(read_from(js, reader, channel, 0).await?.into_iter().map(|(_, post)| post).collect())
}

#[test]
fn channels_and_ants_parse_with_or_without_their_sigil() {
    assert_eq!(parse_channel("#ops").unwrap(), ops());
    assert_eq!(parse_channel("ops").unwrap(), ops());
    assert!(parse_channel("##ops").is_err());
    assert!(parse_channel("#").is_err());
    assert!(parse_channel("#Ops").is_err());
    assert_eq!(parse_ant("@dev-a").unwrap(), p("dev-a"));
    assert_eq!(parse_ant("dev-a").unwrap(), p("dev-a"));
    assert!(parse_ant("@").is_err());
    assert!(parse_ant("channel-manager").is_err());
    let humans = Humans::from_entries([("alice", ants_proto::HumanEntry::new(p("alice")))]);
    assert!(check_messenger_not_human(Some(&UserId::human()), &humans).is_err());
    assert!(check_messenger_not_human(Some(&p("alice")), &humans).is_err());
    assert!(check_messenger_not_human(Some(&p("dev-a")), &humans).is_ok());
    assert!(check_messenger_not_human(None, &humans).is_ok());
}

#[test]
fn the_table_shows_members_and_the_messenger_or_its_default() {
    let mut busy = ChannelEntry::new(ops());
    busy.add_member(p("dev-a"));
    busy.add_member(p("dev-b"));
    busy.messenger = Some(p("triage"));
    busy.owner = Some(p("lead"));
    let quiet = ChannelEntry::new("dev".parse().unwrap());
    assert_eq!(
        table(&[quiet.clone(), busy.clone()], None),
        "CHANNEL  MEMBERS      MESSENGER                   OWNER\n#dev     -            system-messenger (default)  -\n\
             #ops     dev-a,dev-b  triage                      lead"
    );
    let unread = HashMap::from([(ops(), reads::Unread { posts: 3, mentions: 1, other: 4 })]);
    assert_eq!(
        table(&[quiet.clone(), busy.clone()], Some(&unread)),
        "CHANNEL  MEMBERS      MESSENGER                   OWNER  UNREAD\n\
             #dev     -            system-messenger (default)  -      -\n\
             #ops     dev-a,dev-b  triage                      lead   3 (@1) +4 other"
    );
    assert_eq!(describe(&quiet), "#dev: members -; messenger system-messenger (default)");
    assert_eq!(describe(&busy), "#ops: members dev-a,dev-b; messenger triage; owner lead");

    busy.set_description(Some("Incidents and deploys of the shop, on call around the clock")).unwrap();
    assert_eq!(
        table(&[quiet, busy.clone()], None),
        "CHANNEL  MEMBERS      MESSENGER                   OWNER  DESCRIPTION\n\
         #dev     -            system-messenger (default)  -      -\n\
         #ops     dev-a,dev-b  triage                      lead   Incidents and deploys of the shop, on ca…"
    );
    assert_eq!(
        describe(&busy),
        "#ops: members dev-a,dev-b; messenger triage; owner lead\n\
         description: Incidents and deploys of the shop, on call around the clock"
    );
}

#[test]
fn mentions_of_non_members_reach_nobody() {
    let mut entry = ChannelEntry::new(ops());
    entry.add_member(p("dev-a"));
    let post = ChannelPost::new(ops(), UserId::human(), "@dev-a @dev-b @human @alice look");
    assert_eq!(idle_mentions(&post, Some(&entry)), [&p("dev-b"), &p("alice")], "a human is no member either");
    entry.add_member(p("alice"));
    assert_eq!(idle_mentions(&post, Some(&entry)), [&p("dev-b")], "a human member is notified");
    assert_eq!(idle_mentions(&post, None), [&p("dev-a"), &p("dev-b"), &p("alice")]);
}

#[test]
fn a_post_on_a_thread_answers_the_post_that_started_it() {
    let alice = Some(p("alice"));
    let top = human_post(ops(), None, p("alice"), alice.clone(), "build is red".to_owned());
    assert!(top.starts_thread());
    assert_eq!((&top.from, &top.owner), (&p("alice"), &alice));
    let reply = human_post(ops(), Some(top.thread_id.clone()), p("bob"), alice.clone(), "any news?".to_owned());
    assert_eq!((&reply.thread_id, reply.in_reply_to, reply.starts_thread()), (&top.thread_id, Some(top.id), false));
    assert_eq!(reply.owner, alice);
    let elsewhere = human_post(ops(), Some("t1".parse().unwrap()), p("bob"), None, "hi".to_owned());
    assert_eq!(elsewhere.in_reply_to, None);
}

fn at(mut post: ChannelPost, secs: i64) -> ChannelPost {
    post.sent_at = chrono::DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap();
    post
}

#[test]
fn posts_and_descriptions_show_without_control_or_reordering_characters() {
    use crate::test_support::hostile::{HOSTILE, assert_printable};
    let start = ChannelPost::new(ops(), p("dev-a"), HOSTILE);
    let reply = start.reply(p("dev-b"), HOSTILE, &[], &Humans::default()).unwrap();
    let shown = render(&threads(vec![start, reply]), &|post| post.from.to_string());
    assert_printable(&shown);
    assert!(shown.contains(" dev-a: [2J]0;pwned]52;c;cHduZWQ=spoof31m\n    line two\n"), "{shown}");
    assert!(shown.contains(" dev-b: [2J]0;pwned]52;c;cHduZWQ=spoof31m\n        line two\n"), "{shown}");
    let mut entry = ChannelEntry::new(ops());
    entry.description = Some(HOSTILE.to_owned());
    let listed = table(&[entry], None);
    assert_printable(&listed);
    assert!(listed.ends_with(" [2J]0;pwned]52;c;cHduZWQ=spoof31m…"), "{listed}");
}

#[test]
fn posts_group_by_thread_with_replies_indented() {
    let a = at(ChannelPost::new(ops(), UserId::human(), "build is red @dev-a"), 0);
    let b = at(ChannelPost::new(ops(), p("dev-b"), "deploying"), 1);
    let a1 = at(a.reply(p("dev-a"), "on it\nsee the log", &[], &Humans::default()).unwrap(), 2);
    // A reply stored before the post that started its thread, as one kept in an outbox can be.
    let c = at(ChannelPost::new(ops(), UserId::human(), "late start"), 4);
    let c1 = at(c.reply(p("dev-a"), "early reply", &[], &Humans::default()).unwrap(), 3);
    let orphan = at(ChannelPost::on_thread(ops(), "t1".parse().unwrap(), p("dev-b"), "lost root"), 5);
    let grouped = threads(vec![a.clone(), b.clone(), a1.clone(), c1.clone(), c.clone(), orphan.clone()]);
    let ids: Vec<&ThreadId> = grouped.iter().map(|t| &t.id).collect();
    assert_eq!(ids, [&orphan.thread_id, &a.thread_id, &b.thread_id, &c.thread_id], "one started before all first");
    let grouped = [&grouped[1..], &grouped[..1]].concat();
    assert_eq!(grouped[0].posts, [a.clone(), a1]);
    assert_eq!(grouped[2].posts, [c, c1]);
    assert!(!grouped[3].has_start());

    let from = |post: &ChannelPost| post.from.to_string();
    let shown = render(&grouped[..1], &from);
    assert_eq!(
        shown,
        format!(
            "thread {}\n2027-01-15 08:00:00 human: build is red @dev-a\n    2027-01-15 08:00:02 dev-a: on it\n        see the log\n",
            a.thread_id
        )
    );
    let shown = render(&grouped[3..], &from);
    assert_eq!(
        shown,
        "thread t1\n(the post that started the thread is no longer kept)\n    2027-01-15 08:00:05 dev-b: lost root\n"
    );
    assert_eq!(render(&grouped[..2], &from).matches("\n\nthread ").count(), 1, "a blank line between threads");
    let alice = |post: &ChannelPost| ants_proto::Acting::For("alice".parse().unwrap()).label(post.from.as_str());
    assert!(render(&grouped[3..], &alice).contains(" alice via dev-b: lost root\n"));
}

#[test]
fn archived_and_idle_threads_are_left_out_by_their_last_post() {
    let (done, quiet, busy) = (
        at(ChannelPost::new(ops(), UserId::human(), "fixed"), 0),
        at(ChannelPost::new(ops(), UserId::human(), "anyone?"), 1),
        at(ChannelPost::new(ops(), UserId::human(), "deploying"), 2),
    );
    let mut archive = Archive { idle_before: Some(quiet.sent_at), ..Archive::default() };
    archive.marks.archive(&done.thread_id, 3);
    archive.marks.archive(&busy.thread_id, 3);
    let reply = at(busy.reply(p("dev-a"), "done", &[], &Humans::default()).unwrap(), 4);
    let posts = [(1, &done), (2, &quiet), (3, &busy), (4, &reply)];
    let hidden = archive.hidden(posts);
    assert_eq!(hidden, HashSet::from([done.thread_id, quiet.thread_id]), "busy has a post since it was archived");
}

/// With the human's credentials, so it also checks what they allow.
#[tokio::test]
async fn the_human_manages_channels_and_reads_them_back() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (UserId::human(), p("dev-a"));
    let (client, js) = server.connect(&human).await;
    bus::ensure_stream(&js).await.unwrap();
    let kv = open(&js).await.unwrap();

    let mut entry = ChannelEntry::new(ops());
    entry.add_member(dev_a.clone());
    create(&kv, &entry).await.unwrap();
    let again = create(&kv, &entry).await.unwrap_err().to_string();
    assert!(again.contains("exists"), "{again}");
    let (stored, added) = update(&kv, &ops(), |e| e.add_member(p("dev-b"))).await.unwrap();
    assert!(added);
    assert_eq!(stored.members, [dev_a.clone(), p("dev-b")]);
    assert_eq!(get(&kv, &ops()).await.unwrap(), Some(stored));
    assert!(update(&kv, &"dev".parse().unwrap(), |_| ()).await.is_err(), "no such channel");

    // An entry naming another channel is skipped, and replaced on the next update.
    let foreign = ChannelEntry::new("dev".parse().unwrap());
    kv.put("qa", encode(&foreign).unwrap().into()).await.unwrap();
    let listed = list(&kv).await.unwrap();
    assert_eq!(listed.iter().map(|e| e.channel.as_str()).collect::<Vec<_>>(), ["ops"]);
    let (fixed, ()) = update(&kv, &"qa".parse().unwrap(), |e| e.messenger = Some(dev_a.clone())).await.unwrap();
    assert_eq!((fixed.channel.as_str(), fixed.members.len()), ("qa", 0));

    // Posting, following the thread live, and reading the channel back through the human's reader.
    let room = || async { client.subscribe(subject::chan_room(&ops())).await.unwrap() };
    let (following, waiting) = (room().await, room().await);
    let top = human_post(ops(), None, human.clone(), Some(human.clone()), "build is red @dev-a".to_owned());
    bus::store(&js, top.subject(), &top, &top.id.to_string()).await.unwrap();
    let (_, a_js) = server.connect(&dev_a).await;
    let again = human_post(ops(), Some(top.thread_id.clone()), human.clone(), Some(human.clone()), "anyone?".into());
    bus::store(&js, again.subject(), &again, &again.id.to_string()).await.unwrap();
    let answer = top.reply(dev_a.clone(), "on it", &[], &Humans::default()).unwrap();
    bus::publish_json(&a_js, answer.subject(), &answer, None).await.unwrap();
    let texts = |posts: Vec<ChannelPost>| posts.into_iter().map(|p| p.text).collect::<Vec<_>>();
    let next_two = thread_posts(live_posts(following.into()), &top, false).take(2).collect();
    assert_eq!(texts(tokio::time::timeout(Duration::from_secs(2), next_two).await.unwrap()), ["anyone?", "on it"]);
    let mut waiting = Box::pin(thread_posts(live_posts(waiting.into()), &top, true));
    let next = tokio::time::timeout(Duration::from_secs(2), waiting.next()).await.unwrap().unwrap();
    assert_eq!(next.text, "on it", "waiting skips the poster's own posts");

    let me = Me { id: human.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let posts = read(&js, &me, &ops()).await.unwrap();
    assert_eq!(posts, [top, again, answer]);
    assert!(read(&js, &me, &"dev".parse().unwrap()).await.unwrap().is_empty());

    delete(&kv, &ops()).await.unwrap();
    assert!(delete(&kv, &ops()).await.is_err());
    assert_eq!(get(&kv, &ops()).await.unwrap(), None);
    create(&kv, &entry).await.unwrap();
}

/// `ants post` with alice's credentials, then bob's on her thread: each posts under its own name, and the thread
/// stays alice's.
#[tokio::test]
async fn a_human_posts_under_its_own_name_and_keeps_the_thread_it_started() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let me =
        |id: &UserId| crate::people::humans::Me { id: id.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let (a_client, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
    bus::ensure_stream(&a_js).await.unwrap();
    crate::people::humans::ensure(&a_js).await.unwrap();
    for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
        crate::people::humans::register(js, &me(human)).await.unwrap();
    }
    let humans = crate::people::humans::load(&b_js, &me(&bob)).await;
    assert!(humans.contains(&alice) && humans.contains(&bob) && humans.contains(&UserId::human()));

    let mut sub = a_client.subscribe(subject::chan_room(&ops())).await.unwrap();
    a_client.flush().await.unwrap();
    let top = compose(&a_js, &me(&alice), &humans, ops(), None, "build is red".to_owned()).await;
    assert_eq!((&top.from, &top.owner), (&alice, &Some(alice.clone())));
    bus::store(&a_js, top.subject(), &top, &top.id.to_string()).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(2), sub.next()).await.unwrap().unwrap();
    assert_eq!(got.subject.as_str(), "ants.chan.ops.alice");

    // An ant's answer keeps the owner; so does bob's post on the thread, which reads it back.
    let (_, d_js) = server.connect(&dev_a).await;
    let answer = top.reply(dev_a.clone(), "on it", &[], &humans).unwrap();
    assert_eq!(answer.owner, Some(alice.clone()));
    bus::publish_json(&d_js, answer.subject(), &answer, None).await.unwrap();
    let on = compose(&b_js, &me(&bob), &humans, ops(), Some(top.thread_id.clone()), "any news?".to_owned()).await;
    assert_eq!((&on.from, &on.owner, on.in_reply_to), (&bob, &Some(alice.clone()), Some(top.id)));
    bus::store(&b_js, on.subject(), &on, &on.id.to_string()).await.unwrap();
    assert_eq!(on.subject(), "ants.chan.ops.bob");

    // On a thread nothing names an owner of, bob takes it.
    let lost = compose(&b_js, &me(&bob), &humans, ops(), Some("t1".parse().unwrap()), "hi".to_owned()).await;
    assert_eq!((lost.owner, lost.creator), (Some(bob.clone()), None));
    assert_eq!(read(&b_js, &me(&bob), &ops()).await.unwrap(), [top.clone(), answer.clone(), on.clone()]);

    // bob's reads get the posts one by one: many at once all read the whole channel.
    let reader = |user: &UserId| consumer::pull::Config {
        name: Some(chan_reader(user)),
        filter_subject: subject::chan_room(&ops()),
        ack_policy: consumer::AckPolicy::None,
        ..Default::default()
    };
    let next = |c: &consumer::PullConsumer| {
        let c = c.clone();
        async move { c.fetch().max_messages(1).messages().await.unwrap().next().await.unwrap().unwrap() }
    };
    let post = |msg: jetstream::Message| serde_json::from_slice::<ChannelPost>(&msg.payload).unwrap();
    let (as_bob, room) = (me(&bob), ops());
    let reads = futures::future::join_all((0..8).map(|_| read(&b_js, &as_bob, &room))).await;
    for posts in reads {
        assert_eq!(posts.unwrap(), [top.clone(), answer.clone(), on.clone()]);
    }

    // dev-a's CLI reads through spares of dev-a's reader: more reads at once than it has spares all read the whole
    // channel, and the read of dev-a's daemon, through the reader itself, carries on.
    let daemon: consumer::PullConsumer = d_js.create_consumer_on_stream(reader(&dev_a), STREAM).await.unwrap();
    assert_eq!(post(next(&daemon).await), top);
    let as_dev_a = crate::people::humans::Me { id: dev_a.clone(), role: ants_proto::Role::Ant, assistant_of: None };
    let reads = futures::future::join_all((0..8).map(|_| read(&d_js, &as_dev_a, &room))).await;
    for posts in reads {
        assert_eq!(posts.unwrap(), [top.clone(), answer.clone(), on.clone()]);
    }
    assert_eq!(post(next(&daemon).await), answer, "the daemon's read carries on where it was");
    for spare in ants_proto::spare_readers(&chan_reader(&dev_a)) {
        let check =
            consumer::pull::Config { name: Some(spare.clone()), description: Some("check".into()), ..reader(&dev_a) };
        let left = d_js.create_consumer_strict_on_stream::<consumer::pull::Config, _>(check, STREAM).await;
        assert!(left.is_ok(), "{spare} is deleted after its read: {left:?}");
        d_js.delete_consumer_from_stream(&spare, STREAM).await.unwrap();
    }

    // An ant creates the thread it starts, and owns none; a human's post on it keeps the creator.
    let ant = crate::people::humans::Me { id: dev_a.clone(), role: ants_proto::Role::Ant, assistant_of: None };
    let started = compose(&d_js, &ant, &humans, ops(), None, "@dev-b look".to_owned()).await;
    assert_eq!((&started.creator, &started.owner), (&Some(dev_a.clone()), &None));
    bus::publish_json(&d_js, started.subject(), &started, None).await.unwrap();
    let on = compose(&b_js, &me(&bob), &humans, ops(), Some(started.thread_id.clone()), "ok".to_owned()).await;
    assert_eq!((&on.creator, &on.owner), (&Some(dev_a.clone()), &Some(bob.clone())));
}

/// The webhook github, with its own credentials and no marker, starts a thread with a post naming alice its owner:
/// bob's post on it keeps alice. dev-a cannot post as a webhook: its credentials refuse a webhook's subject.
#[tokio::test]
async fn a_post_on_a_webhook_s_thread_keeps_its_owner_and_no_ant_posts_as_a_webhook() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a, hook) = (p("alice"), p("bob"), p("dev-a"), p("github"));
    let me =
        |id: &UserId| crate::people::humans::Me { id: id.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let (_, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
    bus::ensure_stream(&a_js).await.unwrap();
    crate::people::humans::ensure(&a_js).await.unwrap();
    crate::test_support::webhook::ensure_bucket(&a_js).await.unwrap();
    for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
        crate::people::humans::register(js, &me(human)).await.unwrap();
    }
    let humans = crate::people::humans::load(&b_js, &me(&bob)).await;

    let creds = server.dir().join("github.creds");
    let permissions = ants_proto::Permissions::for_webhook(&hook, &[ants_proto::Destination::Channel(ops())]);
    crate::test_support::issue_with(
        &server.dir().join("auth"),
        &hook,
        crate::credentials::issued::Kind::Webhook,
        permissions,
        &creds,
    )
    .unwrap();
    let (_, h_js) = bus::connect(&server.url, Some(&creds), None, &hook, "test").await.unwrap();
    let (_, d_js) = server.connect(&dev_a).await;

    let owner = Some(alice.clone());
    let real = ChannelPost::new(ops(), hook.clone(), "build red").with_owner(owner.clone()).from_webhook(Vec::new());
    bus::publish_json(&h_js, real.subject(), &real, None).await.unwrap();
    let forged = ChannelPost::new(ops(), dev_a.clone(), "look").with_owner(owner.clone()).from_webhook(Vec::new());
    assert_eq!(forged.subject(), ants_proto::subject::webhook_chan(&ops(), &dev_a));
    assert!(bus::publish_json(&d_js, forged.subject(), &forged, None).await.is_err(), "dev-a posts as a webhook");

    let as_bob = me(&bob);
    let on = |thread: &ThreadId| compose(&b_js, &as_bob, &humans, ops(), Some(thread.clone()), "ok".to_owned());
    let on_real = on(&real.thread_id).await;
    assert_eq!((on_real.owner, on_real.creator), (owner, None), "the webhook's");
}

/// alice's post and dev-a's, each with its own credentials, mention bob, a member: bob's inbox gets one notice
/// of each, however often it is sent, and none of a post mentioning bob as no member.
#[tokio::test]
async fn a_mention_of_a_human_member_reaches_its_inbox_once() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let me =
        |id: &UserId| crate::people::humans::Me { id: id.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let (_, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
    bus::ensure_stream(&a_js).await.unwrap();
    crate::people::humans::ensure(&a_js).await.unwrap();
    for (js, human) in [(&a_js, &alice), (&b_js, &bob)] {
        crate::people::humans::register(js, &me(human)).await.unwrap();
    }
    let humans = crate::people::humans::load(&a_js, &me(&alice)).await;
    let mut entry = ChannelEntry::new(ops());
    let (_, d_js) = server.connect(&dev_a).await;
    let post = compose(&a_js, &me(&alice), &humans, ops(), None, "@bob @alice @dev-a look".to_owned()).await;
    assert!(notify_mentioned(&a_js, &post, Some(&entry), &humans).await.is_empty(), "bob is no member yet");

    entry.add_member(dev_a.clone());
    entry.add_member(bob.clone());
    for _ in 0..2 {
        assert_eq!(notify_mentioned(&a_js, &post, Some(&entry), &humans).await, std::slice::from_ref(&bob));
    }
    let ant_post = ChannelPost::new(ops(), dev_a.clone(), "@bob done");
    let notice = ant_post.mention_notice(&bob);
    for _ in 0..2 {
        bus::store(&d_js, notice.subject(), &notice, &ant_post.mention_msg_id(&bob)).await.unwrap();
    }

    let mut inbox = crate::messages::inbox::Inbox::open(&b_js, &bob).await.unwrap();
    let unread = inbox.unread(&crate::messages::inbox::Select::default()).await.unwrap();
    let got: Vec<_> = unread.iter().map(|e| (e.from.clone(), e.mention.as_ref().map(|m| m.post))).collect();
    assert_eq!(got, [(alice.clone(), Some(post.id)), (dev_a.clone(), Some(ant_post.id))]);
    let shown = crate::messages::inbox::render(&unread[1], &ants_proto::Acting::Own, &[], None);
    assert!(shown.contains(&format!("dev-a mentioned bob in #ops on thread {}", ant_post.thread_id)), "{shown}");
    assert!(shown.contains(&format!("`ants post ops --thread {} ...`", ant_post.thread_id)), "{shown}");
}

/// `ants channel read --thread` reads from the thread's start on, and `--limit` from the latest posts back only as far
/// as enough threads started: a thread started before those read comes first, so is never among the last.
#[tokio::test]
async fn reads_of_a_thread_or_the_latest_threads_read_from_where_they_start() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    bus::ensure_stream(&js).await.unwrap();
    let me = Me { id: alice.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let store = async |post: &ChannelPost| bus::store(&js, post.subject(), post, &post.id.to_string()).await.unwrap();
    let old = ChannelPost::new(ops(), alice.clone(), "old");
    store(&old).await;
    for i in 0..2 * LATEST {
        store(&ChannelPost::new("dev".parse().unwrap(), alice.clone(), format!("filler {i}"))).await;
    }
    let (b, c) = (ChannelPost::new(ops(), alice.clone(), "b"), ChannelPost::new(ops(), alice.clone(), "c"));
    store(&b).await;
    let late = old.reply(alice.clone(), "late reply", &[], &Humans::default()).unwrap();
    store(&late).await;
    store(&c).await;

    let started =
        |n: usize| move |posts: &[(u64, ChannelPost)]| posts.iter().filter(|(_, p)| p.starts_thread()).count() >= n;
    let latest = read_latest(&js, &me, &ops(), started(2)).await.unwrap();
    assert!(latest.from > 1, "not every post read");
    let ids: Vec<Uuid> = latest.posts.iter().map(|(_, p)| p.id).collect();
    assert_eq!(ids, [b.id, late.id, c.id]);
    let grouped = threads(latest.posts.into_iter().map(|(_, p)| p).collect());
    assert_eq!(grouped.iter().map(|t| &t.id).collect::<Vec<_>>(), [&old.thread_id, &b.thread_id, &c.thread_id]);
    let all = read_latest(&js, &me, &ops(), started(3)).await.unwrap();
    assert_eq!((all.from, all.posts.len(), all.seqs.len()), (0, 4, 4), "read further back, to the oldest post");

    let thread = read_thread(&js, &me, &ops(), &old.thread_id).await.unwrap();
    assert_eq!(thread.posts.iter().map(|(_, p)| p.id).collect::<Vec<_>>(), [old.id, late.id]);
    assert_eq!((thread.from, thread.seqs.len()), (thread.posts[0].0, 4), "every post from its start read, one kept");
    let orphan = ChannelPost::on_thread(ops(), "t1".parse().unwrap(), alice.clone(), "lost root");
    store(&orphan).await;
    let thread = read_thread(&js, &me, &ops(), &orphan.thread_id).await.unwrap();
    assert_eq!((thread.from, thread.posts.len()), (0, 1), "no start in its id: read from the oldest post");
}

/// alice's channel read markers, with her credentials: shared by her devices, kept per channel, and counting her
/// unread posts from what her marker covers on; neither bob nor an ant reads or writes hers.
#[tokio::test]
async fn a_human_marks_channel_posts_read_for_itself_only() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a) = (p("alice"), p("bob"), p("dev-a"));
    let (_, a_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, phone_js) = server.connect_as(&alice, ants_proto::Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, ants_proto::Role::Human).await;
    let (_, d_js) = server.connect(&dev_a).await;
    bus::ensure_stream(&a_js).await.unwrap();
    let as_alice = Me { id: alice.clone(), role: ants_proto::Role::Human, assistant_of: None };
    let look = ChannelPost::new(ops(), dev_a.clone(), "@alice look");
    let mine = ChannelPost::new(ops(), alice.clone(), "on it");
    let fixed = ChannelPost::new(ops(), dev_a.clone(), "fixed");
    let asked = ChannelPost::new(ops(), bob.clone(), "and the docs?");
    bus::publish_json(&d_js, look.subject(), &look, None).await.unwrap();
    bus::store(&a_js, mine.subject(), &mine, &mine.id.to_string()).await.unwrap();
    bus::publish_json(&d_js, fixed.subject(), &fixed, None).await.unwrap();
    bus::store(&b_js, asked.subject(), &asked, &asked.id.to_string()).await.unwrap();
    let seqs: Vec<u64> = read_from(&a_js, &as_alice, &ops(), 0).await.unwrap().into_iter().map(|(s, _)| s).collect();
    assert_eq!(seqs.len(), 4);
    let from_second = read_from(&a_js, &as_alice, &ops(), seqs[1]).await.unwrap();
    assert_eq!(from_second.into_iter().map(|(_, p)| p).collect::<Vec<_>>(), [mine, fixed, asked]);

    let (laptop, phone) = (
        reads::ChannelReads::open(&a_js, &alice).await.unwrap(),
        reads::ChannelReads::open(&phone_js, &alice).await.unwrap(),
    );
    let (none, revision) = laptop.markers().await.unwrap();
    assert_eq!((&none, revision), (&ChannelReadMarkers::default(), 0));
    let unread_at = |markers: ChannelReadMarkers, level| {
        let (js, me) = (a_js.clone(), as_alice.clone());
        let humans = ants_proto::Humans::from_entries([("bob", ants_proto::HumanEntry::new(p("bob")))]);
        async move { reads::unread(&js, &me, &humans, &markers, &ops(), level, None).await.unwrap() }
    };
    let unread = |markers| unread_at(markers, ChannelNotify::default());
    let counted = reads::Unread { posts: 2, mentions: 1, other: 1 };
    assert_eq!(unread(none.clone()).await, counted, "alice's own post is read, dev-a's unmentioning one apart");
    let all = reads::Unread { posts: 3, mentions: 1, other: 0 };
    assert_eq!(unread_at(none.clone(), ChannelNotify::All).await, all, "dev-a's chatter counts too");
    let mentions = reads::Unread { posts: 1, mentions: 1, other: 2 };
    assert_eq!(unread_at(none.clone(), ChannelNotify::Mentions).await, mentions, "bob's post goes apart too");
    laptop.change(|m| [2, 3].into_iter().for_each(|i| m.mark(&ops(), seqs[i]))).await.unwrap();
    let marked = phone.change(|m| m.mark(&ops(), seqs[0])).await.unwrap();
    assert!(marked.is_read(&ops(), seqs[0]) && marked.is_read(&ops(), seqs[2]), "the phone keeps the laptop's mark");
    assert_eq!(laptop.markers().await.unwrap().0, marked);
    assert_eq!(unread(marked).await, reads::Unread::default());

    let theirs = reads::ChannelReads::open(&b_js, &alice).await.unwrap();
    assert!(theirs.markers().await.is_err(), "bob reads alice's channel read markers");
    assert!(theirs.change(|m| m.mark_through(&ops(), 9)).await.is_err());
    let ants = async { reads::ChannelReads::open(&d_js, &dev_a).await?.markers().await };
    assert!(ants.await.is_err(), "an ant has channel read markers");
}
