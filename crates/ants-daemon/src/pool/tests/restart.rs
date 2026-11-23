//! Restarts: a pool asked to restart stops its harnesses, and a restarted one resumes the stored sessions.

use super::*;

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
    let ran_in = |harness: &str| {
        Some(Stored { session: SessionId::new("s1"), harness: harness.to_owned(), cwd: "/".into(), cloned: false })
    };
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
