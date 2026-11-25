//! The cap on live harnesses: threads keep theirs, the least recently used and idle ones stop and resume, busy ones
//! never stop, and low memory lowers the cap.

use super::*;

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
async fn idle_harnesses_stop_and_resume_on_the_next_message() {
    let (pool, log, _) = fake_pool(4, Sessions::Loadable);
    pool.prompt(&thread(1), "a").await.unwrap();
    assert_eq!(pool.evict_idle(Instant::now()), 0);
    assert_eq!(pool.evict_idle(Instant::now() + IDLE + Duration::from_secs(1)), 1);
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b");
    assert_eq!(log.locked().loaded, ["s1"]);
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
    // Both harnesses start at once in the blocking pool, so either may open its session first.
    let (a, b) = (a.unwrap(), b.unwrap());
    let ((a_session, a), (b_session, b)) = (a.split_once(": ").unwrap(), b.split_once(": ").unwrap());
    assert_eq!((a, b), ("hold a", "hold b"));
    assert_ne!(a_session, b_session, "each thread has a session of its own");
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
async fn busy_threads_past_the_cap_run_their_messages_back_to_back() {
    const PROMPTS: usize = 20;
    let (pool, log, _) = fake_pool(2, Sessions::Loadable);
    let threads: Vec<ThreadId> = (1..=6).map(thread).collect();
    let left: Mutex<HashMap<ThreadId, usize>> = Mutex::new(threads.iter().map(|t| (t.clone(), PROMPTS)).collect());
    // As the inbox does once a message is handled: the threads with messages left in hand.
    let tell = || pool.set_in_hand(left.locked().iter().filter(|(_, n)| **n > 0).map(|(t, _)| t.clone()).collect());
    tell();
    let run = async |t: &ThreadId| {
        for i in 0..PROMPTS {
            pool.prompt(t, &format!("{t} {i}")).await.unwrap();
            *left.locked().get_mut(t).unwrap() -= 1;
            // Answering, while the threads waiting for a harness try to take one.
            settle().await;
            tell();
        }
    };
    futures::future::join_all(threads.iter().map(run)).await;
    let log = log.locked();
    assert_eq!(log.answered.len(), threads.len() * PROMPTS, "every prompt answered");
    let bursts = threads.len() * PROMPTS.div_ceil(BURST);
    assert!(log.starts <= bursts + threads.len(), "{} harness starts for {} prompts", log.starts, log.answered.len());
}

#[tokio::test]
async fn a_thread_with_nothing_left_in_hand_gives_up_its_idle_harness() {
    let (pool, log, _) = fake_pool(1, Sessions::Loadable);
    let (t1, t2) = (thread(1), thread(2));
    pool.set_in_hand([t1.clone(), t2.clone()].into());
    pool.prompt(&t1, "a").await.unwrap();
    let waiting = pool.prompt(&t2, "b");
    let release = async {
        settle().await;
        assert_eq!(log.locked().starts, 1, "the harness is kept for the thread's next message");
        pool.set_in_hand([t2.clone()].into());
    };
    let (reply, ()) = tokio::join!(waiting, release);
    assert_eq!(reply.unwrap(), "s2: b");
    assert_eq!(log.locked().starts, 2);
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
