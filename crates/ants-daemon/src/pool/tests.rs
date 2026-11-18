use std::sync::Mutex;

use ants_proto::provider::{BriefReason, Origin, Starter};
use tokio::sync::{Semaphore, mpsc, oneshot};

use super::fake::Sessions;
use super::*;
use crate::harness::{PromptLimits, Spend};
use crate::limits::overflow::ContextOverflow;
use crate::provider::brief::Briefing;
use crate::provider::brief::fake::{Answer, Fake};
use crate::provider::context::Caller;

const IDLE: Duration = Duration::from_secs(60);

fn fake_pool(max_live: usize, sessions: Sessions) -> (Pool, Arc<Mutex<fake::Log>>, Arc<Semaphore>) {
    let (launch, log, gate) = fake::launch_with(sessions);
    (Pool::new(launch, SessionStore::memory(), max_live, IDLE), log, gate)
}

fn thread(n: u8) -> ThreadId {
    format!("t{n}").parse().unwrap()
}

/// Let spawned harness tasks run until they block.
async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn each_thread_works_in_its_own_worktree_until_its_session_expires() {
    let Some(repo) = crate::test_support::repo("pool-worktrees") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees(worktrees);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    pool.evict_idle(Instant::now() + IDLE * 2);
    pool.prompt(&thread(1), "c").await.unwrap();
    let (t1, t2) = (home.join("worktrees/t1"), home.join("worktrees/t2"));
    assert_eq!(log.locked().cwds, [t1.clone(), t2.clone(), t1.clone()], "session/load in the same worktree");
    crate::test_support::commit(&t2, "work of t2");
    assert_eq!(pool.branch_ahead(&thread(1)).await, None);
    assert_eq!(pool.branch_ahead(&thread(2)).await.map(|b| (b.name, b.commits)), Some(("ants/dev-a/t2".to_owned(), 1)));
    assert_eq!(pool.prune_worktrees().await.removed, 0, "sessions still stored");
    let SessionStore::Memory(sessions) = &store else { unreachable!() };
    sessions.locked().clear();
    let pruned = pool.prune_worktrees().await;
    assert_eq!(pruned.removed, 1, "only the thread without a live harness");
    assert_eq!(pruned.kept.iter().map(|b| &b.thread).collect::<Vec<_>>(), [&thread(2)], "its branch holds a commit");
    assert!(t1.is_dir() && !t2.exists());
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn worktrees_left_behind_are_pruned_while_threads_share_the_workdir() {
    let Some(repo) = crate::test_support::repo("pool-worktrees-off") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    worktrees.checkout(&thread(1)).await.unwrap();
    worktrees.checkout(&thread(2)).await.unwrap();
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let left = Worktrees::left_behind(&"dev-a".parse().unwrap(), &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees_left_behind(left);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(3), "b").await.unwrap();
    assert_eq!(log.locked().cwds, [repo.clone(), repo.clone()], "threads share the workdir");
    crate::test_support::commit(&home.join("worktrees/t1"), "work of t1");
    assert_eq!(pool.branch_ahead(&thread(1)).await, None, "the thread works in the workdir");
    assert_eq!(pool.prune_worktrees().await.removed, 1, "only the thread with no session and no live harness");
    let SessionStore::Memory(sessions) = &store else { unreachable!() };
    sessions.locked().clear();
    pool.evict_idle(Instant::now() + IDLE * 2);
    assert_eq!(pool.prune_worktrees().await.removed, 1);
    assert!(!home.join("worktrees/t1").exists() && !home.join("worktrees/t3").exists());
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_thread_whose_worktree_fails_shares_the_workdir() {
    let Some(repo) = crate::test_support::repo("pool-worktrees-fail") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    crate::test_support::run(&repo, &["branch", "ants/dev-a/t1"]);
    crate::test_support::run(&repo, &["switch", "--quiet", "ants/dev-a/t1"]);
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 4, IDLE).with_worktrees(worktrees);
    assert_eq!(pool.prompt(&thread(1), "a").await.unwrap(), "s1: a", "its branch is checked out elsewhere");
    assert_eq!(log.locked().cwds, std::slice::from_ref(&repo));
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_thread_under_review_works_among_its_checkouts() {
    let Some(repo) = crate::test_support::repo("pool-checkouts") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let worktrees = Worktrees::find(&"sage".parse().unwrap(), &repo, &home).await.unwrap();
    let checkouts = Checkouts::new(&home);
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 4, IDLE)
        .with_worktrees(worktrees)
        .with_checkouts(checkouts.clone());
    let review = ants_proto::Envelope::on_thread(thread(1), "dev-a".parse().unwrap(), "sage".parse().unwrap(), "r");
    let prepared = pool.checkouts().unwrap().prepare(&review).await;
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    assert_eq!(log.locked().cwds, [home.join("reviews/t1"), home.join("worktrees/t2")]);
    drop(prepared);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_thread_keeps_its_harness_between_messages() {
    let (pool, log, _) = fake_pool(4, Sessions::Loadable);
    assert_eq!(pool.prompt(&thread(1), "one").await.unwrap(), "s1: one");
    assert_eq!(pool.prompt(&thread(1), "two").await.unwrap(), "s1: two");
    assert_eq!(pool.prompt(&thread(2), "three").await.unwrap(), "s2: three");
    let log = log.locked();
    assert_eq!((log.starts, log.new_sessions), (2, 2));
}

#[tokio::test]
async fn a_harness_start_tells_the_colony_its_agent() {
    let (launch, _, _) = fake::launch();
    let workload = Arc::new(Workload::default());
    let pool = Pool::new(launch, SessionStore::memory(), 4, IDLE).reporting_to(workload.clone());
    assert_eq!(workload.agent(), None, "no harness started yet");
    pool.prompt(&thread(1), "a").await.unwrap();
    let fake = ants_proto::HarnessAgent { name: "fake".to_owned(), version: "1.0".to_owned() };
    assert_eq!(workload.agent(), Some(fake));
}

#[tokio::test]
async fn the_cap_stops_the_least_recently_used_and_it_resumes() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    assert_eq!(pool.live(), 1);
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s1: c", "no replayed history in the reply");
    let log = log.locked();
    assert_eq!((log.starts, log.new_sessions, log.loaded.as_slice()), (3, 2, &["s1".to_owned()][..]));
}

#[tokio::test]
async fn a_review_runs_in_a_session_of_its_own_on_the_thread_s_session() {
    let (pool, log, _) = fake_pool(4, Sessions::Loadable);
    pool.prompt(&thread(1), "a").await.unwrap();
    let review = ants_proto::REVIEW_ALLOW.map(str::to_owned);
    let narrowed = pool.launch().policy().narrow(&thread(1), Some(&review));
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s1: c");
    drop(narrowed);
    pool.prompt(&thread(1), "d").await.unwrap();
    let log = log.locked();
    assert_eq!((log.starts, log.loaded.as_slice()), (3, &["s1".to_owned(), "s1".to_owned()][..]));
}

#[tokio::test]
async fn idle_harnesses_stop_and_resume_on_the_next_message() {
    let (pool, log, _) = fake_pool(4, Sessions::Loadable);
    pool.prompt(&thread(1), "a").await.unwrap();
    assert_eq!(pool.evict_idle(Instant::now()), 0);
    assert_eq!(pool.evict_idle(Instant::now() + IDLE + Duration::from_secs(1)), 1);
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    assert_eq!(log.locked().loaded, ["s1"]);
}

#[tokio::test]
async fn a_harness_without_session_load_starts_over() {
    let (pool, log, _) = fake_pool(1, Sessions::NewOnly);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    let told = "[ants] The earlier session of this thread could not be resumed, so this one starts over, without \
                    the context of the thread.\n\nc";
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), format!("s3: {told}"));
    assert!(log.locked().loaded.is_empty());
}

#[tokio::test]
async fn threads_run_concurrently_up_to_the_cap() {
    let (pool, log, gate) = fake_pool(2, Sessions::Loadable);
    let (t1, t2) = (thread(1), thread(2));
    let (a, b) = (pool.prompt(&t1, "hold a"), pool.prompt(&t2, "hold b"));
    let release = async {
        settle().await;
        assert_eq!(log.locked().starts, 2, "both threads started before either finished");
        gate.add_permits(2);
    };
    let (a, b, ()) = tokio::join!(a, b, release);
    assert_eq!((a.unwrap(), b.unwrap()), ("s1: hold a".to_owned(), "s2: hold b".to_owned()));
}

#[tokio::test]
async fn a_busy_harness_is_never_stopped() {
    let (pool, log, gate) = fake_pool(1, Sessions::Loadable);
    let (t1, t2) = (thread(1), thread(2));
    let busy = pool.prompt(&t1, "hold a");
    let others = async {
        settle().await;
        assert_eq!(pool.evict_idle(Instant::now() + IDLE * 2), 0, "idle eviction skips a busy harness");
        let waiting = pool.prompt(&t2, "b");
        let release = async {
            settle().await;
            assert_eq!(log.locked().starts, 1, "a new thread waits instead of stopping a busy harness");
            gate.add_permits(1);
        };
        let (reply, ()) = tokio::join!(waiting, release);
        reply.unwrap()
    };
    let (a, b) = tokio::join!(busy, others);
    assert_eq!((a.unwrap().as_str(), b.as_str()), ("s1: hold a", "s2: b"));
    assert_eq!(pool.live(), 1);
}

#[tokio::test]
async fn a_prompt_waiting_for_a_free_harness_is_told_when_it_gets_one() {
    let (pool, _, gate) = fake_pool(1, Sessions::Loadable);
    let (t1, t2) = (thread(1), thread(2));
    let busy = pool.prompt(&t1, "hold a");
    let others = async {
        settle().await;
        let (began_tx, mut began) = oneshot::channel();
        let waiting = pool.run(&t2, Prompt { began: Some(began_tx), ..Prompt::new("b") });
        let release = async {
            settle().await;
            assert!(began.try_recv().is_err(), "not while it waits");
            let released = tokio::time::Instant::now();
            gate.add_permits(1);
            released
        };
        let (reply, released) = tokio::join!(waiting, release);
        assert_eq!(reply.unwrap().text, "s2: b");
        assert!(began.try_recv().unwrap() >= released);
    };
    let (a, ()) = tokio::join!(busy, others);
    assert_eq!(a.unwrap(), "s1: hold a");
}

#[tokio::test]
async fn a_restart_stops_idle_harnesses_at_once_and_busy_ones_after_their_prompt() {
    let (pool, log, gate) = fake_pool(4, Sessions::Loadable);
    let (t1, t2) = (thread(1), thread(2));
    pool.prompt(&t1, "a").await.unwrap();
    let busy = pool.prompt(&t2, "hold b");
    let restart = async {
        settle().await;
        assert_eq!(pool.restart(), Restarting { stopped: 1, working: vec![t2.clone()] });
        assert_eq!(pool.live(), 1, "the busy harness runs on");
        let next = pool.prompt(&t2, "c");
        let release = async {
            settle().await;
            assert_eq!(log.locked().starts, 2, "the next prompt waits for the busy harness to stop");
            gate.add_permits(1);
        };
        let (reply, ()) = tokio::join!(next, release);
        reply.unwrap()
    };
    let (b, c) = tokio::join!(busy, restart);
    assert_eq!((b.unwrap().as_str(), c.as_str()), ("s2: hold b", "s2: c"));
    assert_eq!(pool.prompt(&t1, "d").await.unwrap(), "s1: d");
    let log = log.locked();
    assert_eq!(log.starts, 4, "each thread on a new harness");
    assert_eq!(log.loaded, ["s2", "s1"], "resuming its session");
}

#[tokio::test]
async fn low_memory_stops_idle_harnesses_at_once_and_lowers_the_cap_until_it_recovers() {
    let (launch, log, gate) = fake::launch();
    let available = Arc::new(Mutex::new(8u64 << 30));
    let read = available.clone();
    let probe: pressure::Probe =
        Arc::new(move || Some(pressure::Memory { available: *read.locked(), total: 16 << 30 }));
    let settings = pressure::Settings { min_available: pressure::Threshold::Bytes(1 << 30), max_harnesses: 1 };
    let workload = Arc::new(Workload::default());
    let pool = Pool::new(launch, SessionStore::memory(), 3, IDLE)
        .reporting_to(workload.clone())
        .with_memory_pressure(settings, probe);
    for n in 1..=3 {
        pool.prompt(&thread(n), "a").await.unwrap();
    }
    assert_eq!(pool.evict_idle(Instant::now()), 0, "with memory to spare, harnesses wait out the idle timeout");
    assert_eq!(workload.memory_low(), None);
    *available.locked() = 512 << 20;
    let (t1, t3) = (thread(1), thread(3));
    let busy = pool.prompt(&t3, "hold c");
    let others = async {
        settle().await;
        assert_eq!(pool.evict_idle(Instant::now()), 2, "idle harnesses stop at once, the busy one stays");
        let low = workload.memory_low().expect("the colony shows low memory");
        assert_eq!((low.available, low.max_harnesses), (512 << 20, 1));
        let waiting = pool.prompt(&t1, "d");
        let release = async {
            settle().await;
            assert_eq!(log.locked().starts, 3, "a new thread waits under the lower cap");
            gate.add_permits(1);
        };
        let (reply, ()) = tokio::join!(waiting, release);
        reply.unwrap()
    };
    let (c, d) = tokio::join!(busy, others);
    assert_eq!((c.unwrap().as_str(), d.as_str()), ("s3: hold c", "s1: d"));
    assert_eq!(pool.live(), 1);
    *available.locked() = 1200 << 20;
    assert_eq!(pool.evict_idle(Instant::now()), 1, "still low just above the threshold");
    assert!(workload.memory_low().is_some());
    *available.locked() = 2 << 30;
    assert_eq!(pool.evict_idle(Instant::now()), 0);
    assert_eq!(workload.memory_low(), None, "recovered");
    pool.prompt(&thread(1), "e").await.unwrap();
    pool.prompt(&thread(2), "f").await.unwrap();
    assert_eq!(pool.live(), 2, "the full cap again");
}

#[tokio::test]
async fn a_restarted_pool_resumes_the_stored_session() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.shutdown().await;

    let pool = Pool::new(launch, store, 4, IDLE);
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    let log = log.locked();
    assert_eq!((log.new_sessions, log.loaded.as_slice()), (1, &["s1".to_owned()][..]));
}

fn with_history(text: &str) -> Prompt {
    let history = crate::stream::history::from_fn(|budget| async move { format!("HISTORY {budget}") });
    Prompt { history: Some(history), ..Prompt::new(text) }
}

#[tokio::test]
async fn a_session_that_cannot_be_resumed_starts_over_from_the_thread_history() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    pool.shutdown().await;
    log.locked().lose_sessions = true;

    let pool = Pool::new(launch, store.clone(), 4, IDLE);
    let seeded = "[ants] The earlier session of this thread could not be resumed, so this one starts over, its \
                      context rebuilt from the thread's history.\n\nHISTORY 65536\n\nb";
    assert_eq!(pool.run(&thread(1), with_history("b")).await.unwrap().text, format!("s2: {seeded}"));
    assert_eq!(pool.run(&thread(1), with_history("c")).await.unwrap().text, "s2: c", "only the first prompt");
    assert_eq!(store.get(&thread(1)).await.unwrap().map(|s| s.session), Some(SessionId::new("s2")));
    assert_eq!(log.locked().prompts, ["a", seeded, "c"], "a session started anew is not seeded");
}

#[tokio::test]
async fn a_session_lost_is_told_lost_then_rebuilt() {
    use crate::provider::events::{Lifecycle, tests};
    let provider = tests::Recording::taking_events();
    let outbox = tests::outbox_to(provider.clone()).await;
    let home = ants_test_support::scratch_dir("pool-lifecycle");
    let ant: UserId = "ant".parse().unwrap();
    let lifecycle = Lifecycle::open(&outbox, &ant, "id-1", &home).await;
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE).with_lifecycle(lifecycle.clone());
    assert!(!pool.run(&thread(1), Prompt::new("a")).await.unwrap().rebuilt);
    pool.shutdown().await;
    log.locked().lose_sessions = true;

    let pool = Pool::new(launch, store, 4, IDLE).with_lifecycle(lifecycle);
    assert!(pool.run(&thread(1), Prompt::new("b")).await.unwrap().rebuilt, "the reply tells of the rebuilt session");
    assert!(!pool.run(&thread(1), Prompt::new("c")).await.unwrap().rebuilt, "rebuilt once");
    tests::took(&provider, 3).await;
    let lost = [EventKind::AntAdded, EventKind::SessionLost, EventKind::SessionRebuilt { reason: Rebuilt::Lost }];
    assert_eq!(provider.kinds(), lost, "a new session is not one lost");
    assert!(provider.got.locked()[1..].iter().all(|event| event.thread == Some(thread(1))));
    std::fs::remove_dir_all(home).unwrap();
}

/// A clone's session is forked in its working directory where the harness advertises session/fork; one stored as
/// not forked starts over, seeded from the thread's history.
#[tokio::test]
async fn a_clone_s_session_is_forked_else_starts_over_from_the_thread_history() {
    let (launch, log, _) = fake::launch();
    let session = SessionId::new("s9");
    assert_eq!(crate::harness::fork(&launch, &session).await.unwrap(), None, "no fork advertised");
    log.locked().forks = true;
    let forked = crate::harness::fork(&launch.clone().in_dir(std::path::PathBuf::from("/w")), &session).await.unwrap();
    assert_eq!(forked, Some(SessionId::new("f1")));
    assert_eq!(log.locked().forked, [("s9".to_owned(), std::path::PathBuf::from("/w"))]);

    let store = SessionStore::memory();
    let stored = Stored { session, harness: "test".to_owned(), cloned: true };
    store.put(&thread(1), stored).await.unwrap();
    let pool = Pool::new(launch, store.clone(), 4, IDLE);
    let reply = pool.run(&thread(1), with_history("b")).await.unwrap().text;
    assert!(reply.starts_with("s1: [ants] This ant is a clone of another"), "{reply}");
    assert!(reply.ends_with("HISTORY 65536\n\nb"), "{reply}");
    assert!(log.locked().loaded.is_empty(), "the session cloned from is not loaded");
    assert_eq!(
        store.get(&thread(1)).await.unwrap().map(|s| (s.session, s.cloned)),
        Some((SessionId::new("s1"), false))
    );
}

#[tokio::test]
async fn a_restarted_pool_resumes_a_session_only_in_the_harness_it_ran_in() {
    let (launch, log, _) = fake::launch();
    let (spare, spare_log, _) = fake::launch();
    let fallbacks = vec![("spare".to_owned(), spare.named("spare"))];
    let store = SessionStore::memory();
    let restart = || Pool::new(launch.clone(), store.clone(), 4, IDLE).with_fallbacks(fallbacks.clone());
    let pool = restart();
    pool.run_on(&thread(1), with_history("a"), Some(0)).await.unwrap();
    pool.shutdown().await;
    let ran_in =
        |harness: &str| Some(Stored { session: SessionId::new("s1"), harness: harness.to_owned(), cloned: false });
    assert_eq!(store.get(&thread(1)).await.unwrap(), ran_in("spare"));

    let pool = restart();
    assert_eq!(pool.run_on(&thread(1), with_history("b"), Some(0)).await.unwrap().text, "s1: b");
    assert_eq!(spare_log.locked().loaded, ["s1"], "resumed on the fallback it ran in");
    pool.shutdown().await;

    let pool = restart();
    let reply = pool.run(&thread(1), with_history("c")).await.unwrap().text;
    assert!(reply.starts_with("s1: [ants] The earlier session of this thread ran in another harness"), "{reply}");
    assert!(log.locked().loaded.is_empty(), "the fallback's session is not loaded in the own harness");
    assert_eq!(store.get(&thread(1)).await.unwrap(), ran_in("test"));
}

#[tokio::test]
async fn a_prompt_that_outgrows_the_context_window_runs_again_in_a_new_session_once() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    let reply = pool.run(&thread(1), with_history("overflow")).await.unwrap();
    let seeded = "[ants] The session of this thread outgrew the model's context window, so this one starts over, \
                      its context rebuilt from the thread's history.\n\nHISTORY 32768\n\noverflow";
    assert_eq!(reply, Reply { text: format!("s2: {seeded}"), cut: None, rebuilt: true });
    assert_eq!(
        store.get(&thread(1)).await.unwrap().map(|s| s.session),
        Some(SessionId::new("s2")),
        "resumed from now on"
    );
    assert_eq!(pool.run(&thread(1), with_history("b")).await.unwrap().text, "s2: b");
    assert_eq!(log.locked().starts, 1, "the same harness");

    // A prompt that outgrows even a new session fails for good, without a third try.
    let err = pool.run(&thread(1), with_history("overflows")).await.unwrap_err();
    let overflow = err.downcast_ref::<ContextOverflow>().expect("a context overflow");
    assert_eq!(overflow.harness, "test");
    assert!(overflow.detail.contains("Prompt is too long"), "{}", overflow.detail);
    let log = log.locked();
    assert_eq!((log.prompted, log.new_sessions), (6, 3));
    assert!(log.prompts[5].contains("outgrew the model's context window"), "{}", log.prompts[5]);
}

#[tokio::test]
async fn ant_and_tool_instructions_reach_every_prompt_and_resumed_session() {
    let (launch, log, _) = fake::launch();
    let mut ant = crate::config::Ant::new("dev-a".parse().unwrap(), env!("CARGO_MANIFEST_DIR").into());
    ant.whoami = Some("Maintain the Rust backend.".to_owned());
    ant.memory = Some("Use notes.".to_owned());
    ant.tasks = Some("Use tickets.".to_owned());
    ant.harness = Some("copilot".to_owned());
    let config = ant.into_config("nats://unused", &Default::default()).unwrap();
    let instructions =
        "[ants role dev-a]\nMaintain the Rust backend.\n\n[ants memory]\nUse notes.\n\n[ants tasks]\nUse tickets.";
    let pool = Pool::new(launch.with_instructions(config.harness.instructions), SessionStore::memory(), 1, IDLE);
    for (thread_id, text, session) in [(1, "one", "s1"), (1, "two", "s1"), (2, "three", "s2"), (1, "four", "s1")] {
        assert_eq!(
            pool.prompt(&thread(thread_id), text).await.unwrap(),
            format!("{session}: {instructions}\n\n{text}")
        );
    }
    assert_eq!(log.locked().loaded, ["s1"]);
    pool.shutdown().await;
}

#[tokio::test]
async fn a_prompt_past_its_time_limit_is_cancelled_and_the_session_goes_on() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let limits = PromptLimits { timeout: Some(Duration::from_millis(50)), max_tool_calls: None };
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("stall") }).await.unwrap();
    let cut = "Prompt time limit reached (50ms)".to_owned();
    assert_eq!(reply, Reply { text: "partial".to_owned(), cut: Some(cut), rebuilt: false });
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    let log = log.locked();
    assert_eq!((log.starts, log.cancelled), (1, 1));
}

#[tokio::test]
async fn a_prompt_past_its_tool_call_limit_is_cancelled() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let limits = PromptLimits { timeout: None, max_tool_calls: Some(2) };
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 2") }).await.unwrap();
    assert_eq!(reply.cut, None, "up to the limit is fine");
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 3 stall") }).await.unwrap();
    assert_eq!(reply.cut.as_deref(), Some("Tool call limit reached (2 tool calls in one prompt)"));
    assert_eq!(log.locked().cancelled, 1);
}

#[tokio::test]
async fn spend_is_reported_and_a_stop_cancels_the_prompt() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let (spend, mut spent) = mpsc::unbounded_channel();
    let (stop, stopped) = oneshot::channel();
    let prompt = Prompt { spend: Some(spend), stop: Some(stopped), ..Prompt::new("cost 0.5 stall") };
    let watch = async {
        assert_eq!(spent.recv().await, Some(Spend::Cost { session: SessionId::new("s1"), total: 0.5 }));
        stop.send("Enough".to_owned()).unwrap();
    };
    let t1 = thread(1);
    let (reply, ()) = tokio::join!(pool.run(&t1, prompt), watch);
    assert_eq!(reply.unwrap().cut.as_deref(), Some("Enough"));
    assert_eq!(log.locked().cancelled, 1);

    let (spend, mut spent) = mpsc::unbounded_channel();
    pool.run(&thread(1), Prompt { spend: Some(spend), ..Prompt::new("tokens 42") }).await.unwrap();
    let cost = Spend::Cost { session: SessionId::new("s1"), total: 0.5 };
    assert_eq!(spent.try_recv(), Ok(cost), "the last cost comes again as the next prompt starts");
    assert_eq!(spent.try_recv(), Ok(Spend::Tokens(42)));
}

/// A prompt for thread 1 carrying the briefing of a fake provider answering `answer`.
async fn briefed(answer: Answer, text: &str) -> (Prompt, Arc<Fake>) {
    let (provider, fake) = crate::provider::brief::fake::connected(answer, true).await;
    let caller = Caller { ant: "dev-a".parse().unwrap(), identity: "id-a".to_owned(), machine: "m1".to_owned() };
    let origin = Origin { by: Starter::Human { user: ants_proto::UserId::human() }, channel: None };
    let brief = Briefing::new(provider, Ok(caller), thread(1), origin);
    (Prompt { brief, ..with_history(text) }, fake)
}

fn reasons(fake: &Fake) -> Vec<BriefReason> {
    fake.asked.locked().iter().map(|request| request.reason).collect()
}

#[tokio::test]
async fn the_brief_follows_the_instructions_when_a_session_starts_or_is_loaded_or_rebuilt() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let launch = launch.with_instructions("[ants memory]\nUse notes.".to_owned());
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    let brief = Answer::Text("Working on the parser.".to_owned());
    let (prompt, fake) = briefed(brief.clone(), "a").await;
    let instructions = "[ants memory]\nUse notes.\n\n[ants brief from fake]\nWorking on the parser.";
    assert_eq!(pool.run(&thread(1), prompt).await.unwrap().text, format!("s1: {instructions}\n\na"));
    let (prompt, _) = briefed(brief.clone(), "b").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    assert_eq!(reply, "s1: [ants memory]\nUse notes.\n\nb", "only the first prompt of a session");
    pool.evict_idle(Instant::now() + IDLE * 2);
    let (prompt, loaded) = briefed(brief.clone(), "c").await;
    assert_eq!(pool.run(&thread(1), prompt).await.unwrap().text, format!("s1: {instructions}\n\nc"));
    assert_eq!((reasons(&fake), reasons(&loaded)), (vec![BriefReason::Started], vec![BriefReason::Loaded]));
    pool.shutdown().await;

    log.locked().lose_sessions = true;
    let pool = Pool::new(launch, store, 4, IDLE);
    let (prompt, rebuilt) = briefed(brief, "d").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    assert!(reply.starts_with(&format!("s2: {instructions}\n\n[ants] The earlier session")), "{reply}");
    assert!(reply.ends_with("HISTORY 65536\n\nd"), "{reply}");
    assert_eq!(reasons(&rebuilt), [BriefReason::Rebuilt]);
}

#[tokio::test]
async fn a_session_rebuilt_after_a_context_overflow_is_briefed_again() {
    let (pool, _, _) = fake_pool(4, Sessions::Loadable);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    let (prompt, fake) = briefed(Answer::Text("Brief.".to_owned()), "overflow").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    let brief = "[ants brief from fake]\nBrief.";
    assert!(reply.starts_with(&format!("s2: {brief}\n\n[ants] The session of this thread outgrew")), "{reply}");
    assert_eq!(reasons(&fake), [BriefReason::Rebuilt], "the first session was not opened by this prompt");
}

#[tokio::test]
async fn a_channel_s_description_opens_each_session_of_its_thread_after_the_brief() {
    let (pool, _, _) = fake_pool(4, Sessions::Loadable);
    let described =
        |text: &str, description: &str| Prompt { channel: Some(description.to_owned()), ..with_history(text) };
    assert_eq!(pool.run(&thread(1), described("a", "Ops.")).await.unwrap().text, "s1: Ops.\n\na");
    let reply = pool.run(&thread(1), described("b", "Ops, changed.")).await.unwrap().text;
    assert_eq!(reply, "s1: b", "a running session keeps the description it started with");
    pool.evict_idle(Instant::now() + IDLE * 2);
    let reply = pool.run(&thread(1), described("c", "Ops, changed.")).await.unwrap().text;
    assert_eq!(reply, "s1: Ops, changed.\n\nc", "a loaded session takes the description of now");
    let (prompt, _) = briefed(Answer::Text("Brief.".to_owned()), "overflow").await;
    let reply = pool.run(&thread(1), Prompt { channel: Some("Ops.".to_owned()), ..prompt }).await.unwrap().text;
    let opening = "[ants brief from fake]\nBrief.\n\nOps.";
    assert!(reply.starts_with(&format!("s2: {opening}\n\n[ants] The session of this thread outgrew")), "{reply}");
}

#[tokio::test(start_paused = true)]
async fn a_brief_not_sent_in_time_leaves_a_line_and_the_prompt_goes_on() {
    let (pool, _, _) = fake_pool(1, Sessions::Loadable);
    let (prompt, _) = briefed(Answer::Never, "a").await;
    let reply = pool.run(&thread(1), prompt).await.unwrap().text;
    assert_eq!(reply, "s1: [ants] The fake brief is left out: no answer within 5s.\n\na");
}
