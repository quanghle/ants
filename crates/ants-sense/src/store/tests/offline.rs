use std::sync::atomic::AtomicBool;

use super::*;
use crate::log::{AppendError, Signature, Signer};

#[derive(Default)]
struct Fallible {
    log: MemoryLog,
    offline: AtomicBool,
    lose_ack: AtomicBool,
    rejection: std::sync::Mutex<Option<&'static str>>,
}

impl Fallible {
    fn online(&self, online: bool) {
        self.offline.store(!online, Ordering::SeqCst);
    }

    fn available(&self) -> Result<(), LogError> {
        if self.offline.load(Ordering::SeqCst) { Err(LogError("hub unavailable".into())) } else { Ok(()) }
    }
}

impl EventLog for Fallible {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.available()?;
        if let Some(rejection) = *self.rejection.lock().unwrap() {
            return Err(LogError(rejection.into()).into());
        }
        let sequence = self.log.append(sense, payload, expected_last).await?;
        if self.lose_ack.swap(false, Ordering::SeqCst) {
            self.online(false);
            return Err(LogError("publish acknowledgment timed out".into()).into());
        }
        Ok(sequence)
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.available()?;
        self.log.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.available()?;
        self.log.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        self.log.incarnation()
    }
}

#[tokio::test]
async fn offline_batches_keep_order_and_cas_across_restart_then_join_the_verified_log() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let home = ants_test_support::scratch_dir("sense-offline-order");
    let mut store = open(&log, &sense, Some(&home)).await.with_machine("m1".parse().unwrap());
    let one = store.create("note", note("one"), "t").await.unwrap();
    log.online(false);
    let two = ItemId::new();
    store
        .write(
            &[
                Write::Change { id: one.id.clone(), values: note("changed"), expected: Some(1) },
                Write::Create { id: two.clone(), kind: "note".into(), values: note("two") },
            ],
            "t",
        )
        .await
        .unwrap();
    store.change(&one.id, note("last"), Some(2), "t").await.unwrap();
    assert!(matches!(
        store.change(&one.id, note("stale"), Some(1), "t").await,
        Err(StoreError::Conflict { expected: 1, .. })
    ));
    assert_eq!(store.pending().unwrap(), 2);
    assert_eq!(store.search("last", None, 10).unwrap().len(), 1);
    assert_eq!(store.history(&one.id).unwrap().iter().map(|revision| revision.sequence).collect::<Vec<_>>(), [1, 0, 0]);
    let local = store.get(&one.id).unwrap();
    drop(store);

    let mut store = open(&log, &sense, Some(&home)).await;
    assert_eq!(store.get(&one.id).unwrap().unwrap().text("title"), "last");
    assert_eq!(store.get(&two).unwrap().unwrap().text("title"), "two");
    assert_eq!(store.pending().unwrap(), 2);
    log.online(true);
    store.sync().await.unwrap();
    assert_eq!(store.get(&one.id).unwrap(), local, "acknowledgment preserves the accepted local change's stamp");
    assert_eq!(store.pending().unwrap(), 0);
    assert!(!crate::cache::cache_file(&home).with_extension("pending").exists());
    let history = store.history(&one.id).unwrap();
    assert_eq!(history.iter().map(|revision| revision.sequence).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(store.history(&two).unwrap()[0].sequence, 2);
    let mut chain = ant_log(&log, &sense);
    assert_eq!(chain.catch_up().await.unwrap().len(), 3);
    assert!(chain.chain().broken().is_none());
    drop(store);
    let rebuilt = open(&log, &sense, None).await;
    assert_eq!(titles(&rebuilt), ["last", "two"]);
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn an_uncertain_ack_is_recognized_after_restart_without_appending_twice() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let home = ants_test_support::scratch_dir("sense-offline-ack");
    let mut store = open(&log, &sense, Some(&home)).await;
    log.lose_ack.store(true, Ordering::SeqCst);
    let item = store.create("note", note("one"), "t").await.unwrap();
    store.change(&item.id, note("two"), Some(1), "t").await.unwrap();
    assert_eq!(store.pending().unwrap(), 2);
    drop(store);
    let mut store = open(&log, &sense, Some(&home)).await;
    log.online(true);
    store.sync().await.unwrap();
    assert_eq!(log.log.read_after(&sense, 0, 10).await.unwrap().len(), 2);
    assert_eq!(store.get(&item.id).unwrap().unwrap().text("title"), "two");
    assert_eq!(store.history(&item.id).unwrap().len(), 2);
    assert_eq!(store.pending().unwrap(), 0);
    drop(store);
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn a_remote_guard_conflict_retains_the_entire_atomic_batch_and_reports_it_after_restart() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let home = ants_test_support::scratch_dir("sense-offline-conflict");
    let mut store = open(&log, &sense, Some(&home)).await;
    let guard = store.create("note", note("guard"), "t").await.unwrap();
    log.online(false);
    let id = ItemId::new();
    store
        .write(
            &[
                Write::Create { id: id.clone(), kind: "note".into(), values: note("local") },
                Write::Change { id: guard.id.clone(), values: Values::new(), expected: Some(1) },
            ],
            "t",
        )
        .await
        .unwrap();
    log.online(true);
    let mut other = open(&log, &sense, None).await;
    other.change(&guard.id, note("remote"), Some(1), "u").await.unwrap();
    let error = store.sync().await.unwrap_err();
    assert!(error.to_string().contains("remote guard item"), "{error}");
    assert_eq!(store.pending().unwrap(), 1);
    assert_eq!(store.get(&id).unwrap(), None, "none of the conflicted batch applies");
    assert_eq!(store.get(&guard.id).unwrap().unwrap().text("title"), "remote");
    assert_eq!(log.last_sequence(&sense).await.unwrap(), 2);
    drop(store);
    let mut store = open(&log, &sense, Some(&home)).await;
    assert_eq!(store.pending().unwrap(), 1);
    assert!(store.sync().await.unwrap_err().to_string().contains("local write retained"));
    assert!(store.create("note", note("blocked"), "t").await.is_err());
    drop(store);
    std::fs::remove_dir_all(home).unwrap();
}

#[tokio::test]
async fn confirmed_writes_never_create_offline_fencing_sequences_or_queue_new_effects() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    let item = store.create("note", note("one"), "t").await.unwrap();
    log.online(false);
    let writes = [Write::Change { id: item.id.clone(), values: note("claim"), expected: Some(1) }];
    assert!(store.write_confirmed(&writes, "t").await.is_err());
    assert_eq!(store.pending().unwrap(), 0);
    assert_eq!(store.get(&item.id).unwrap(), Some(item));
}

#[tokio::test]
async fn authentication_signing_and_integrity_failures_never_queue_writes() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    *log.rejection.lock().unwrap() = Some("authorization violation: request timed out");
    assert!(store.create("note", note("forbidden"), "t").await.is_err());
    assert_eq!(store.pending().unwrap(), 0);
    *log.rejection.lock().unwrap() = None;
    let item = store.create("note", note("valid"), "t").await.unwrap();
    let sequence = store.history(&item.id).unwrap()[0].sequence;
    log.log.rewrite(sequence, Some(b"not an event".to_vec()));
    let mut corrupt = open(&log, &sense, None).await;
    assert!(corrupt.log().chain().broken().is_some());
    log.online(false);
    assert!(corrupt.create("note", note("corrupt"), "t").await.is_err());
    assert_eq!(corrupt.pending().unwrap(), 0);

    struct BrokenSigner;
    impl Signer for BrokenSigner {
        fn sign(&self, _: &[u8]) -> Result<Option<Signature>, String> {
            Err("key service timed out".into())
        }
    }
    log.online(true);
    let log = AntLog::new(
        &log,
        BrokenSigner,
        AccountVerifier::new(vec![], NoRevocations),
        SenseId::new(),
        dev_a(),
        "m1".parse().unwrap(),
        None,
    )
    .unwrap();
    let mut store = Store::open(log, Cache::in_memory().unwrap()).await.unwrap();
    assert!(store.create("note", note("unsigned"), "t").await.is_err());
    assert_eq!(store.pending().unwrap(), 0);
}

#[tokio::test]
async fn reads_through_items_replay_pending_writes_when_the_hub_returns() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let store = tokio::sync::Mutex::new(open(&log, &sense, None).await);
    log.online(false);
    let item = store.create("note", note("cached"), "t").await.unwrap();
    assert_eq!(store.list("note", 10).await.unwrap(), std::slice::from_ref(&item));
    assert_eq!(store.pending().await.unwrap(), 1);
    log.online(true);
    assert_eq!(store.get(&item.id).await.unwrap().unwrap().text("title"), "cached");
    assert_eq!(store.pending().await.unwrap(), 0);
    assert_eq!(store.history(&item.id).await.unwrap()[0].sequence, 1);
}

#[tokio::test]
async fn signer_failures_are_not_hidden_behind_an_existing_offline_queue() {
    struct SwitchSigner<'a>(&'a AtomicBool);
    impl Signer for SwitchSigner<'_> {
        fn sign(&self, _: &[u8]) -> Result<Option<Signature>, String> {
            if self.0.load(Ordering::SeqCst) { Err("signing service timed out".into()) } else { Ok(None) }
        }
    }
    let (log, sense, failed) = (Fallible::default(), SenseId::new(), AtomicBool::new(false));
    let ant_log = AntLog::new(
        &log,
        SwitchSigner(&failed),
        AccountVerifier::new(vec![], NoRevocations),
        sense,
        dev_a(),
        "m1".parse().unwrap(),
        None,
    )
    .unwrap();
    let mut store = Store::open(ant_log, Cache::in_memory().unwrap()).await.unwrap();
    log.online(false);
    store.create("note", note("kept"), "t").await.unwrap();
    failed.store(true, Ordering::SeqCst);
    let error = store.create("note", note("refused"), "t").await.unwrap_err();
    assert!(error.to_string().contains("signing:"), "{error}");
    assert_eq!(store.pending().unwrap(), 1);
    assert_eq!(store.list("note", 10).unwrap().len(), 1);
}

#[tokio::test]
async fn reconnect_appends_the_nonconflicting_prefix_before_reporting_a_later_conflict() {
    let (log, sense) = (Fallible::default(), SenseId::new());
    let mut store = open(&log, &sense, None).await;
    let item = store.create("note", note("base"), "t").await.unwrap();
    log.online(false);
    let first = store.create("note", note("independent"), "t").await.unwrap();
    store.change(&item.id, note("local"), Some(1), "t").await.unwrap();
    log.online(true);
    let mut other = open(&log, &sense, None).await;
    other.change(&item.id, note("remote"), Some(1), "u").await.unwrap();
    assert!(store.sync().await.unwrap_err().to_string().contains("local write retained"));
    assert_eq!(store.pending().unwrap(), 1);
    assert_eq!(store.history(&first.id).unwrap()[0].sequence, 3);
    assert_eq!(log.last_sequence(&sense).await.unwrap(), 3);
}
