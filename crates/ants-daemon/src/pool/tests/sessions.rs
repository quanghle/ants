//! A thread's session: started, resumed, forked for a clone or a review, started over from the thread's history
//! when lost or outgrown, and given the ant's instructions.

use super::*;

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
async fn a_harness_without_session_load_starts_over() {
    let (pool, log, _) = fake_pool(1, Sessions::NewOnly);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    let told = "[ants] The earlier session of this thread could not be resumed, so this one starts over, without \
                    the context of the thread.\n\n[ants message]\nc";
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), format!("s3: {told}"));
    assert!(log.locked().loaded.is_empty());
}

#[tokio::test]
async fn a_session_that_cannot_be_resumed_starts_over_from_the_thread_history() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    pool.shutdown().await;
    log.locked().knobs.lose_sessions = true;

    let pool = Pool::new(launch, store.clone(), 4, IDLE);
    let seeded = "[ants] The earlier session of this thread could not be resumed, so this one starts over, its \
                      context rebuilt from the thread's history.\n\nHISTORY 65536\n\n[ants message]\nb";
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
    log.locked().knobs.lose_sessions = true;

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
    log.locked().knobs.forks = true;
    let forked = crate::harness::fork(&launch.clone().in_dir(std::path::PathBuf::from("/w")), &session).await.unwrap();
    assert_eq!(forked, Some(SessionId::new("f1")));
    assert_eq!(log.locked().forked, [("s9".to_owned(), std::path::PathBuf::from("/w"))]);

    let store = SessionStore::memory();
    let stored = Stored { session, harness: "test".to_owned(), cwd: Some("/".into()), cloned: true };
    store.put(&thread(1), stored).await.unwrap();
    let pool = Pool::new(launch, store.clone(), 4, IDLE);
    let reply = pool.run(&thread(1), with_history("b")).await.unwrap().text;
    assert!(reply.starts_with("s1: [ants] This ant is a clone of another"), "{reply}");
    assert!(reply.ends_with("HISTORY 65536\n\n[ants message]\nb"), "{reply}");
    assert!(log.locked().loaded.is_empty(), "the session cloned from is not loaded");
    assert_eq!(
        store.get(&thread(1)).await.unwrap().map(|s| (s.session, s.cloned)),
        Some((SessionId::new("s1"), false))
    );
}

#[tokio::test]
async fn a_prompt_that_outgrows_the_context_window_runs_again_in_a_new_session_once() {
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.run(&thread(1), with_history("a")).await.unwrap();
    let reply = pool.run(&thread(1), with_history("overflow")).await.unwrap();
    let seeded = "[ants] The session of this thread outgrew the model's context window, so this one starts over, \
                      its context rebuilt from the thread's history.\n\nHISTORY 32768\n\n[ants message]\noverflow";
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
    ant.human = Some(ants_test_support::human());
    let config = ant.into_config("nats://unused", &Default::default()).unwrap();
    let instructions =
        "[ants role dev-a]\nMaintain the Rust backend.\n\n[ants memory]\nUse notes.\n\n[ants tasks]\nUse tickets.";
    let pool = Pool::new(launch.with_instructions(config.harness.instructions), SessionStore::memory(), 1, IDLE);
    for (thread_id, text, session) in [(1, "one", "s1"), (1, "two", "s1"), (2, "three", "s2"), (1, "four", "s1")] {
        assert_eq!(
            pool.prompt(&thread(thread_id), text).await.unwrap(),
            format!("{session}: {instructions}\n\n[ants message]\n{text}")
        );
    }
    assert_eq!(log.locked().loaded, ["s1"]);
    pool.shutdown().await;
}
