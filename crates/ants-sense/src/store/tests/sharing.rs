//! Tests of a store its ant's calls share: catch-ups shared by the calls that came before them, and reads serving the
//! cache while another call has its turn at the log.

use std::time::Instant;

use tokio::sync::Semaphore;

use super::*;
use crate::log::AppendError;

/// A log counting its reads, whose reads and appends each wait for a permit of their gate.
struct Gated {
    log: MemoryLog,
    reads: AtomicUsize,
    reading: Semaphore,
    appending: Semaphore,
}

impl Gated {
    fn open() -> Self {
        let free = Semaphore::MAX_PERMITS;
        Self {
            log: MemoryLog::default(),
            reads: AtomicUsize::new(0),
            reading: Semaphore::new(free),
            appending: Semaphore::new(free),
        }
    }

    fn reads(&self) -> usize {
        self.reads.swap(0, Ordering::Relaxed)
    }

    /// Make the turns at `gate` wait until permits are added.
    fn close(gate: &Semaphore) {
        gate.forget_permits(Semaphore::MAX_PERMITS);
    }
}

impl EventLog for Gated {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.appending.acquire().await.unwrap().forget();
        self.log.append(sense, payload, expected_last).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.reading.acquire().await.unwrap().forget();
        self.log.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.log.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        self.log.incarnation()
    }
}

/// Longer than a clock's step, so that what comes after it comes later.
const TICK: std::time::Duration = std::time::Duration::from_millis(2);

/// Ends after more turns of the runtime than a call served from the cache takes.
async fn many_turns() {
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_catch_up_is_shared_by_the_calls_that_came_before_it_started() {
    let (log, sense) = (Gated::open(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    log.reads();
    let asked = Instant::now();
    std::thread::sleep(TICK);
    store.sync().await.unwrap();
    let once = log.reads();
    assert!(once > 0);
    store.sync_since(asked).await.unwrap();
    assert_eq!(log.reads(), 0, "the catch-up started since is the call's own");
    store.sync_since(Instant::now()).await.unwrap();
    assert_eq!(log.reads(), once, "none started since: the call catches up");

    // What another writer appended before a call came is in its catch-up, shared or not.
    let mut other = open(&log.log, &sense, None).await;
    other.create("note", note("theirs"), "t").await.unwrap();
    let asked = Instant::now();
    store.sync_since(asked).await.unwrap();
    assert_eq!(titles(&store), ["theirs"]);
}

#[tokio::test]
async fn calls_waiting_for_a_catch_up_share_the_next_one() {
    let (log, sense) = (Gated::open(), SenseId::new());
    let shared = Shared::new(open(&log, &sense, None).await);
    log.reads();
    Gated::close(&log.reading);
    let first = shared.sync_since(Instant::now());
    let mut first = std::pin::pin!(first);
    assert!(poll_once(&mut first).await.is_none(), "the first waits for the log");
    std::thread::sleep(TICK);
    // These come while the first reads the log: one of them reads it again, for all of them.
    let rest = async {
        let asked = Instant::now();
        tokio::join!(
            shared.sync_since(asked),
            shared.sync_since(asked),
            shared.sync_since(asked),
            shared.sync_since(asked)
        )
    };
    let mut rest = std::pin::pin!(rest);
    assert!(poll_once(&mut rest).await.is_none());
    std::thread::sleep(TICK);
    log.reading.add_permits(Semaphore::MAX_PERMITS);
    first.await.unwrap();
    let (a, b, c, d) = rest.await;
    for shared in [a, b, c, d] {
        shared.unwrap();
    }
    assert_eq!(log.reads(), 2, "one read for the first call, one for the four that came during it");
}

/// Poll `future` once: its output when it ended.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Option<F::Output> {
    std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match std::pin::Pin::new(&mut *future).poll(cx) {
            std::task::Poll::Ready(output) => Some(output),
            std::task::Poll::Pending => None,
        })
    })
    .await
}

#[tokio::test]
async fn a_read_serves_the_cache_while_another_call_has_its_turn_at_the_log() {
    let (log, sense) = (Gated::open(), SenseId::new());
    let shared = Shared::new(open(&log, &sense, None).await);
    let kept = shared.create("note", note("kept"), "t").await.unwrap();
    let bytes = shared.log_bytes().await;
    assert!(bytes > 0);

    Gated::close(&log.appending);
    let writing = shared.create("note", note("coming"), "t");
    let mut writing = std::pin::pin!(writing);
    let read = async {
        let listed = shared.list("note", 10).await.unwrap();
        let found = shared.search("kept", None, 10).await.unwrap();
        let history = shared.history(&kept.id).await.unwrap();
        (listed, found.len(), history.len(), shared.pending().await.unwrap(), shared.log_bytes().await)
    };
    let read = tokio::select! {
        biased;
        _ = &mut writing => panic!("the append waits for its gate"),
        read = read => read,
        () = many_turns() => panic!("the reads wait for the other call's append"),
    };
    assert_eq!(read, (vec![kept.clone()], 1, 1, 0, bytes), "the items as the log left them before the append");
    assert_eq!((shared.sense().await, shared.broken().await), (sense.clone(), None));

    log.appending.add_permits(Semaphore::MAX_PERMITS);
    let coming = writing.await.unwrap();
    assert_eq!(shared.list("note", 10).await.unwrap(), [coming, kept]);
    assert!(shared.log_bytes().await > bytes, "what the log told is kept as each turn ends");
}
