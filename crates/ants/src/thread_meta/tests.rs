use std::collections::HashSet;

use ants_proto::{Envelope, Role, Uuid};
use async_nats::jetstream::consumer::DeliverPolicy;

use super::*;
use crate::thread::{Select, Threads, prune, scan};

/// A time-ordered thread id made `days` ago.
fn made_ago(days: i64) -> ThreadId {
    let ms = (Utc::now() - chrono::Duration::days(days)).timestamp_millis() as u64;
    let random = Uuid::now_v7().as_u128() as u64 & 0xffff_ffff_ffff;
    format!("{:08x}-{:04x}-7000-8000-{random:012x}", ms >> 16, ms & 0xffff).parse().unwrap()
}

#[tokio::test]
async fn an_archived_thread_comes_back_flagged_once_a_message_is_stored_on_it() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let meta = Meta::open(&js, None, &alice).await.unwrap();
    let ask = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let thread = &ask.thread_id;
    let store = async |envelope: &Envelope| {
        let payload = serde_json::to_vec(envelope).unwrap();
        js.publish(envelope.subject(), payload.into()).await.unwrap().await.unwrap();
    };
    store(&ask).await;
    let listed = async |select: &Select| {
        let mut threads = Threads::new(None);
        let start = async_nats::jetstream::consumer::DeliverPolicy::All;
        scan(&js, start, |e| threads.add(&e)).await.unwrap();
        threads.finish(select, &meta.marks().await.unwrap()).0
    };

    let after = last_sequence(&js).await.unwrap();
    assert!(meta.change_marks(|m| m.archive(thread, after)).await.unwrap());
    assert!(!meta.change_marks(|m| m.archive(thread, after)).await.unwrap(), "archived already");
    assert!(listed(&Select::default()).await.is_empty(), "hidden by default");
    let archived = listed(&Select { archived: true, ..Default::default() }).await;
    assert!(archived[0].archived && !archived[0].new_activity);

    store(&ask.reply(dev_a, "built", Role::Human).unwrap()).await;
    let back = listed(&Select::default()).await;
    assert_eq!(back.len(), 1);
    assert!(back[0].archived && back[0].new_activity, "still archived, but flagged");
    assert_eq!(js.get_stream(STREAM).await.unwrap().cached_info().state.messages, 2, "nothing deleted");

    let after = last_sequence(&js).await.unwrap();
    assert!(meta.change_marks(|m| m.archive(thread, after)).await.unwrap(), "archiving again moves on");
    assert!(listed(&Select::default()).await.is_empty());
    assert!(meta.change_marks(|m| m.pin(thread)).await.unwrap());
    let pinned = listed(&Select { pinned: true, ..Default::default() }).await;
    assert!(pinned[0].pinned && !pinned[0].archived, "pinning unarchives");

    assert!(meta.change_marks(|m| m.unpin(thread)).await.unwrap());
    let last_at = thread::last_activity(&js, thread).await.unwrap().unwrap();
    let idle = Select { idle_before: Some(last_at), ..Default::default() };
    assert!(listed(&idle).await.is_empty(), "idle since its last message");
    assert!(listed(&Select { archived: true, ..idle.clone() }).await[0].auto_archived);
    assert!(meta.change_marks(|m| m.keep(thread)).await.unwrap());
    assert!(!listed(&idle).await[0].archived, "kept");
    assert_eq!(thread::last_activity(&js, &ThreadId::generate()).await.unwrap(), None);
}

#[tokio::test]
async fn a_thread_done_for_every_human_reopens_once_a_message_is_stored_on_it() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, bob, dev_a) = ("alice".parse::<UserId>().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let (meta, bob_meta) = (Meta::open(&js, None, &alice).await.unwrap(), Meta::open(&b_js, None, &bob).await.unwrap());
    let ask = Envelope::new(alice.clone(), dev_a, "build it");
    let thread = &ask.thread_id;
    let store = async |envelope: &Envelope| {
        let payload = serde_json::to_vec(envelope).unwrap();
        js.publish(envelope.subject(), payload.into()).await.unwrap().await.unwrap();
    };
    store(&ask).await;

    let last = last_reopening(&js, thread).await.unwrap();
    assert!(last > 0);
    assert!(mark_done(&meta, thread, true, last).await.unwrap());
    assert!(!mark_done(&bob_meta, thread, true, last).await.unwrap(), "done already, for bob too");
    let done = bob_meta.done(thread).await.unwrap().unwrap();
    assert_eq!((&done.by, done.after), (&alice, last));
    assert!(meta.dones([thread, &ThreadId::generate()]).await.unwrap().contains_key(thread));

    store(&ask.reply(ask.to.clone(), "built", Role::Human).unwrap()).await;
    let later = last_reopening(&js, thread).await.unwrap();
    assert!(!meta.done(thread).await.unwrap().unwrap().holds(later), "a reply reopens it");
    assert!(!mark_done(&meta, thread, false, later).await.unwrap(), "reopened: nothing to clear");
    assert_eq!(meta.done(thread).await.unwrap(), None, "the stale state is gone though");
    assert!(mark_done(&bob_meta, thread, true, later).await.unwrap());
    assert!(mark_done(&meta, thread, false, later).await.unwrap(), "alice clears bob's");
}

#[tokio::test]
async fn marks_and_titles_of_threads_with_no_message_left_are_forgotten_once_old() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let meta = Meta::open(&js, None, &alice).await.unwrap();
    let (gone, alive, fresh, renamed) = (made_ago(40), made_ago(40), made_ago(40), made_ago(40));
    let unordered: ThreadId = "deploy-1".parse().unwrap();
    let ask = Envelope { thread_id: alive.clone(), ..Envelope::new(alice.clone(), dev_a, "still here") };
    js.publish(ask.subject(), serde_json::to_vec(&ask).unwrap().into()).await.unwrap().await.unwrap();
    let long_ago = Utc::now() - KEPT - chrono::Duration::days(1);
    let old_title = |thread: &ThreadId| {
        let title = ThreadTitle { at: long_ago, ..ThreadTitle::new(thread.clone(), "old", alice.clone()).unwrap() };
        serde_json::to_vec(&title).unwrap()
    };
    for thread in [&gone, &alive, &renamed, &unordered] {
        meta.kv.put(title_key(thread), old_title(thread)).await.unwrap();
    }
    for thread in [&gone, &alive] {
        let done = ThreadDone { at: long_ago, ..ThreadDone::new(thread.clone(), alice.clone(), 0) };
        meta.kv.put(done_key(thread), serde_json::to_vec(&done).unwrap()).await.unwrap();
    }
    meta.set_title(&fresh, "new").await.unwrap();
    meta.change_marks(|m| {
        for thread in [&gone, &alive, &unordered] {
            m.pinned.insert(thread.clone(), long_ago);
        }
        m.archived.insert(renamed.clone(), ants_proto::Archived { at: long_ago, after: 0 });
        m.kept.insert(alive.clone(), Utc::now() - chrono::Duration::days(1));
        m.kept.insert(fresh.clone(), Utc::now());
        m.kept.insert(gone.clone(), long_ago);
        m.pin(&fresh)
    })
    .await
    .unwrap();
    let present = async |start: DeliverPolicy| {
        let mut threads = Threads::new(None);
        scan(&js, start, |e| threads.add(&e)).await.unwrap();
        threads.summaries().map(|t| (t.thread_id.clone(), t.last_at)).collect::<HashMap<_, _>>()
    };
    let (last, revision) = meta.kv.lookup_last(title_key(&renamed)).await.unwrap();
    assert!(matches!(last, Lookup::Value(_)));
    meta.set_title(&renamed, "renamed meanwhile").await.unwrap();
    let deleted = meta.kv.delete_expecting(title_key(&renamed), revision, None).await.unwrap();
    assert!(!deleted, "a title set again stays");

    let start = crate::thread::window(Duration::from_hours(50 * 24));
    prune(&meta, &meta.marks().await.unwrap(), &start, &present(start).await).await;
    let marks = meta.marks().await.unwrap();
    let pinned: HashSet<&ThreadId> = marks.pinned.keys().collect();
    assert_eq!(pinned, HashSet::from([&alive, &fresh, &unordered]), "the old pin of a thread gone goes");
    assert!(marks.archived.is_empty(), "so does an old archive");
    assert!(marks.kept.is_empty(), "and keeps of threads gone or with a message since");
    assert_eq!(meta.title(&gone).await.unwrap(), None, "and an old title");
    assert_eq!(meta.done(&gone).await.unwrap(), None, "and an old done state");
    assert!(meta.done(&alive).await.unwrap().is_some(), "a thread with a message keeps its done state");
    assert_eq!(meta.title(&alive).await.unwrap().unwrap().title, "old", "a thread with a message keeps its title");
    assert_eq!(meta.title(&fresh).await.unwrap().unwrap().title, "new", "a recent title stays");
    assert_eq!(meta.title(&renamed).await.unwrap().unwrap().title, "renamed meanwhile");
    assert!(meta.title(&unordered).await.unwrap().is_some(), "a read from a time leaves an unordered id be");

    let start = crate::thread::window(Duration::from_hours(24));
    prune(&meta, &meta.marks().await.unwrap(), &start, &present(start).await).await;
    assert!(meta.marks().await.unwrap().pinned.contains_key(&unordered), "nor does a read of one day");

    prune(&meta, &meta.marks().await.unwrap(), &DeliverPolicy::All, &present(DeliverPolicy::All).await).await;
    assert_eq!(meta.title(&unordered).await.unwrap(), None, "a read of everything kept does");
    let pinned: Vec<ThreadId> = meta.marks().await.unwrap().pinned.into_keys().collect();
    assert_eq!(HashSet::from_iter(pinned), HashSet::from([alive, fresh]));
}

/// A cleared title's delete marker keeps out an ant's suggestion while its thread has messages, and goes with the
/// thread, as a pruned title's does, leaving no key behind.
#[tokio::test]
async fn the_markers_of_cleared_and_pruned_titles_go_with_their_threads() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = ("alice".parse::<UserId>().unwrap(), "dev-a".parse::<UserId>().unwrap());
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let meta = Meta::open(&js, None, &alice).await.unwrap();
    let (cleared, titled, alive) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    for thread in [&cleared, &titled, &alive] {
        meta.set_title(thread, "Release 1.2").await.unwrap();
    }
    meta.clear_title(&cleared).await.unwrap();
    meta.clear_title(&alive).await.unwrap();
    assert_eq!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap(), None, "the clear wins");

    let gone = |thread: &ThreadId| *thread != alive;
    assert_eq!(meta.prune_titles(Utc::now() - KEPT, &gone).await.unwrap(), 0, "recent ones stay");
    let later = Utc::now() + chrono::Duration::seconds(1);
    assert_eq!(meta.prune_titles(later, &gone).await.unwrap(), 2);
    assert_eq!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap(), None, "a marker for now");
    let left = [title_key(&alive)];
    for _ in 0..100 {
        if meta.kv.keys("title.*").await.unwrap() == left {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(meta.kv.keys("title.*").await.unwrap(), left, "the markers of threads gone expire");
    assert!(meta.adopt_title(&cleared, "Fix the build", &dev_a).await.unwrap().is_some());
    assert_eq!(meta.adopt_title(&alive, "Fix the build", &dev_a).await.unwrap(), None, "a live thread's clear wins");
}

/// The title of a thread between two humans whose messages are sealed is sealed for both: each opens it, a
/// third human reads `[sealed]` and may not title it, and the thread keeps no ant's suggestion.
#[tokio::test]
async fn the_title_of_a_sealed_thread_is_sealed_for_both_humans() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let humans: Vec<UserId> = ["alice", "bob", "carol"].iter().map(|h| h.parse().unwrap()).collect();
    let creds: Vec<_> = humans.iter().map(|h| server.creds_as(h, Role::Human)).collect();
    let nats: Vec<_> = creds.iter().map(|c| crate::Nats::new(&server.url, Some(c), None).unwrap()).collect();
    let mut js = Vec::new();
    for n in &nats {
        js.push(n.connect("test").await.unwrap().1);
    }
    let _ = js[0].create_stream(ants_daemon::hub::threads_config()).await;
    let mut ask = Envelope::new(humans[0].clone(), humans[1].clone(), "lunch?");
    Sealer::new(&nats[0], &js[0], &humans[1], "").await.unwrap().seal(&mut ask, &mut []).unwrap();
    ants_daemon::bus::publish(&js[0], &ask).await.unwrap();
    let thread = &ask.thread_id;
    let dev_a: UserId = "dev-a".parse().unwrap();
    let (_, ant_js) = server.connect_as(&dev_a, Role::Ant).await;
    let mut noted = Envelope::on_thread(thread.clone(), dev_a, humans[0].clone(), "noted");
    noted.title = Some("Lunch order".to_owned());
    ants_daemon::bus::publish(&ant_js, &noted).await.unwrap();

    let sealer = title_sealer(&Sealing::of(&nats[0]), &js[0], thread).await.unwrap().expect("a sealed thread");
    let mut title = ThreadTitle::new(thread.clone(), "Team lunch", humans[0].clone()).unwrap();
    sealer.seal_title(&mut title).unwrap();
    assert_eq!(open(&nats[0], &js[0]).await.unwrap().put_title(&title).await.unwrap(), None);
    let meta = open(&nats[0], &js[0]).await.unwrap();
    let Lookup::Value(stored) = meta.kv.lookup(title_key(thread)).await.unwrap() else { panic!("no title") };
    assert!(!String::from_utf8_lossy(&stored).contains("lunch"), "stored sealed");
    for i in 0..2 {
        let title = open(&nats[i], &js[i]).await.unwrap().title(thread).await.unwrap().unwrap();
        assert_eq!((title.title.as_str(), &title.by, title.sealed), ("Team lunch", &humans[0], None));
    }
    let unopened = |meta: Meta| async move { meta.title(thread).await.unwrap().unwrap().title };
    assert_eq!(unopened(open(&nats[2], &js[2]).await.unwrap()).await, "[sealed]");
    assert_eq!(unopened(Meta::open(&js[1], None, &humans[1]).await.unwrap()).await, "[sealed]");
    let refused = title_sealer(&Sealing::of(&nats[2]), &js[2], thread).await.err().unwrap().to_string();
    assert!(refused.contains("only they title it"), "{refused}");
    assert!(title_sealer(&Sealing::of(&nats[0]), &js[0], &ThreadId::generate()).await.unwrap().is_none());

    let mut threads = Threads::new(None);
    scan(&js[0], async_nats::jetstream::consumer::DeliverPolicy::All, |e| threads.add(&e)).await.unwrap();
    let summary = threads.summaries().find(|t| t.thread_id == *thread).unwrap();
    assert!(summary.sealed && summary.suggested.is_some(), "an ant's suggestion stays unstored");
}
