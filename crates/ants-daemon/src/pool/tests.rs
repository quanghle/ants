use std::sync::Mutex;

use tokio::sync::{Semaphore, mpsc, oneshot};

use super::*;
use crate::harness::{PromptLimits, Spend};
use crate::overflow::ContextOverflow;

const IDLE: Duration = Duration::from_secs(60);

fn fake_pool(max_live: usize, can_load: bool) -> (Pool, Arc<Mutex<fake::Log>>, Arc<Semaphore>) {
    let (launch, log, gate) = fake::launch(can_load);
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
    let Some(repo) = crate::worktrees::tests::repo("pool-worktrees") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch(true);
    let store = SessionStore::memory();
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees(worktrees);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    pool.evict_idle(Instant::now() + IDLE * 2);
    pool.prompt(&thread(1), "c").await.unwrap();
    let (t1, t2) = (home.join("worktrees/t1"), home.join("worktrees/t2"));
    assert_eq!(log.locked().cwds, [t1.clone(), t2.clone(), t1.clone()], "session/load in the same worktree");
    crate::worktrees::tests::commit(&t2, "work of t2");
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
    let Some(repo) = crate::worktrees::tests::repo("pool-worktrees-off") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    worktrees.checkout(&thread(1)).await.unwrap();
    worktrees.checkout(&thread(2)).await.unwrap();
    let (launch, log, _) = fake::launch(true);
    let store = SessionStore::memory();
    let left = Worktrees::left_behind(&"dev-a".parse().unwrap(), &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees_left_behind(left);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(3), "b").await.unwrap();
    assert_eq!(log.locked().cwds, [repo.clone(), repo.clone()], "threads share the workdir");
    crate::worktrees::tests::commit(&home.join("worktrees/t1"), "work of t1");
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
    let Some(repo) = crate::worktrees::tests::repo("pool-worktrees-fail") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch(true);
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    crate::worktrees::tests::run(&repo, &["branch", "ants/dev-a/t1"]);
    crate::worktrees::tests::run(&repo, &["switch", "--quiet", "ants/dev-a/t1"]);
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 4, IDLE).with_worktrees(worktrees);
    assert_eq!(pool.prompt(&thread(1), "a").await.unwrap(), "s1: a", "its branch is checked out elsewhere");
    assert_eq!(log.locked().cwds, std::slice::from_ref(&repo));
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_thread_keeps_its_harness_between_messages() {
    let (pool, log, _) = fake_pool(4, true);
    assert_eq!(pool.prompt(&thread(1), "one").await.unwrap(), "s1: one");
    assert_eq!(pool.prompt(&thread(1), "two").await.unwrap(), "s1: two");
    assert_eq!(pool.prompt(&thread(2), "three").await.unwrap(), "s2: three");
    let log = log.locked();
    assert_eq!((log.starts, log.new_sessions), (2, 2));
}

#[tokio::test]
async fn a_harness_start_tells_the_roster_its_agent() {
    let (launch, _, _) = fake::launch(true);
    let workload = Arc::new(Workload::default());
    let pool = Pool::new(launch, SessionStore::memory(), 4, IDLE).reporting_to(workload.clone());
    assert_eq!(workload.agent(), None, "no harness started yet");
    pool.prompt(&thread(1), "a").await.unwrap();
    let fake = ants_proto::HarnessAgent { name: "fake".to_owned(), version: "1.0".to_owned() };
    assert_eq!(workload.agent(), Some(fake));
}

#[tokio::test]
async fn the_cap_stops_the_least_recently_used_and_it_resumes() {
    let (pool, log, _) = fake_pool(1, true);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    assert_eq!(pool.live(), 1);
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s1: c", "no replayed history in the reply");
    let log = log.locked();
    assert_eq!((log.starts, log.new_sessions, log.loaded.as_slice()), (3, 2, &["s1".to_owned()][..]));
}

#[tokio::test]
async fn idle_harnesses_stop_and_resume_on_the_next_message() {
    let (pool, log, _) = fake_pool(4, true);
    pool.prompt(&thread(1), "a").await.unwrap();
    assert_eq!(pool.evict_idle(Instant::now()), 0);
    assert_eq!(pool.evict_idle(Instant::now() + IDLE + Duration::from_secs(1)), 1);
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    assert_eq!(log.locked().loaded, ["s1"]);
}

#[tokio::test]
async fn a_harness_without_session_load_starts_over() {
    let (pool, log, _) = fake_pool(1, false);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    let told = "[ants] The earlier session of this thread could not be resumed, so this one starts over, without \
                    the context of the thread.\n\nc";
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), format!("s3: {told}"));
    assert!(log.locked().loaded.is_empty());
}

#[tokio::test]
async fn threads_run_concurrently_up_to_the_cap() {
    let (pool, log, gate) = fake_pool(2, true);
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
    let (pool, log, gate) = fake_pool(1, true);
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
async fn a_restarted_pool_resumes_the_stored_session() {
    let (launch, log, _) = fake::launch(true);
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
    let history = crate::history::from_fn(|budget| async move { format!("HISTORY {budget}") });
    Prompt { history: Some(history), ..Prompt::new(text) }
}

#[tokio::test]
async fn a_session_that_cannot_be_resumed_starts_over_from_the_thread_history() {
    let (launch, log, _) = fake::launch(true);
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
    assert_eq!(store.get(&thread(1)).await.unwrap(), Some(SessionId::new("s2")));
    assert_eq!(log.locked().prompts, ["a", seeded, "c"], "a session started anew is not seeded");
}

#[tokio::test]
async fn a_prompt_that_outgrows_the_context_window_runs_again_in_a_new_session_once() {
    let (launch, log, _) = fake::launch(true);
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    let reply = pool.run(&thread(1), with_history("overflow")).await.unwrap();
    let seeded = "[ants] The session of this thread outgrew the model's context window, so this one starts over, \
                      its context rebuilt from the thread's history.\n\nHISTORY 32768\n\noverflow";
    assert_eq!(reply, Reply { text: format!("s2: {seeded}"), cut: None });
    assert_eq!(store.get(&thread(1)).await.unwrap(), Some(SessionId::new("s2")), "resumed from now on");
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
    let (launch, log, _) = fake::launch(true);
    let mut ant = crate::config::Ant::new("dev-a".parse().unwrap(), env!("CARGO_MANIFEST_DIR").into());
    ant.whoami = Some("Maintain the Rust backend.".to_owned());
    ant.memory = Some("Use notes.".to_owned());
    ant.tasks = Some("Use tickets.".to_owned());
    let config = ant.into_config("nats://unused", &Default::default()).unwrap();
    let instructions =
        "[ants role dev-a]\nMaintain the Rust backend.\n\n[ants memory]\nUse notes.\n\n[ants tasks]\nUse tickets.";
    let pool = Pool::new(launch.with_instructions(config.instructions), SessionStore::memory(), 1, IDLE);
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
    let (pool, log, _) = fake_pool(1, true);
    let limits = PromptLimits { timeout: Some(Duration::from_millis(50)), max_tool_calls: None };
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("stall") }).await.unwrap();
    let cut = "Prompt time limit reached (50ms)".to_owned();
    assert_eq!(reply, Reply { text: "partial".to_owned(), cut: Some(cut) });
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    let log = log.locked();
    assert_eq!((log.starts, log.cancelled), (1, 1));
}

#[tokio::test]
async fn a_prompt_past_its_tool_call_limit_is_cancelled() {
    let (pool, log, _) = fake_pool(1, true);
    let limits = PromptLimits { timeout: None, max_tool_calls: Some(2) };
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 2") }).await.unwrap();
    assert_eq!(reply.cut, None, "up to the limit is fine");
    let reply = pool.run(&thread(1), Prompt { limits, ..Prompt::new("tools 3 stall") }).await.unwrap();
    assert_eq!(reply.cut.as_deref(), Some("Tool call limit reached (2 tool calls in one prompt)"));
    assert_eq!(log.locked().cancelled, 1);
}

#[tokio::test]
async fn spend_is_reported_and_a_stop_cancels_the_prompt() {
    let (pool, log, _) = fake_pool(1, true);
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
