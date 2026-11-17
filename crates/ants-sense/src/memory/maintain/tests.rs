use std::sync::Arc;

use ants_proto::provider::{CallContext, Event, EventKind};
use chrono::{DateTime, Duration, Utc};

use super::*;
use crate::memory::tests::{Kept, context, human, memory, sense};
use crate::memory::{Access, Votes};

fn written(at: DateTime<Utc>) -> CallContext {
    CallContext { at, ..context(human()) }
}

fn new(title: &str, body: &str, at: DateTime<Utc>) -> Memory {
    Memory::new(MemoryKind::Lesson, Scope::Own, title.into(), body.into(), &written(at))
}

async fn store(kept: &Arc<Kept>, memory: &Memory) -> ItemId {
    let id = ItemId::new();
    let items: Arc<dyn Items> = kept.clone();
    items.write(&[memory.create(id.clone())], "test").await.unwrap();
    id
}

fn actions(run: &Maintenance) -> Vec<(ItemId, Action)> {
    run.changes.iter().map(|change| (change.memory.clone(), change.action)).collect()
}

#[test]
fn salience_fades_with_disuse_and_rises_with_use() {
    let at: DateTime<Utc> = "2025-01-01T00:00:00Z".parse().unwrap();
    let close = |a: f64, b: f64| assert!((a - b).abs() < 1e-9, "{a} != {b}");
    let mut memory = new("Port", "Deploy uses 8080", at);
    let ages = |days: i64| Ages::new(at + Duration::days(days), []);
    close(salience(&memory, &ages(0)), 1.0);
    close(salience(&memory, &ages(90)), DECAY_FLOOR);
    memory.importance = 5;
    close(salience(&memory, &ages(30)), 5.0 / 3.0 / 2.0);
    memory.importance = DEFAULT_IMPORTANCE;
    memory.access = Some(Access { days: 20, last: at + Duration::days(30) });
    close(salience(&memory, &ages(60)), 0.5 * (1.0 + ACCESS_WEIGHT * f64::from(MAX_ACCESS_DAYS)));
    memory.votes = Votes { helpful: 1, stale: 1, ..Votes::default() };
    memory.access = None;
    close(salience(&memory, &ages(0)), (1.0 + HELPFUL_WEIGHT) / (1.0 + STALE_WEIGHT));
}

#[test]
fn ages_count_no_dormant_time() {
    let day = |n: i64| "2025-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap() + Duration::days(n);
    let span = |from: i64, to: i64| Dormancy { from: day(from), to: day(to) };
    let ages = Ages::new(day(100), [span(60, 70), span(20, 30), span(25, 40), span(90, 150), span(50, 50)]);
    let days = |since: i64| ages.since(day(since)).num_days();
    assert_eq!(days(0), 100 - 20 - 10 - 10, "overlapping spans counted once, the ongoing one up to now");
    assert_eq!(days(35), 65 - 5 - 10 - 10, "a span begun before counted from the start");
    assert_eq!(days(95), 0);
    assert_eq!(days(120), 0, "a start past now");
    assert_eq!(Ages::new(day(100), []).since(day(10)).num_days(), 90);
}

/// A dormant ant used no memory: maintenance after its revival counts only the days it was active in the ages of its
/// memories, over its dormancies kept on its card.
#[tokio::test]
async fn memories_age_only_while_the_ant_is_active() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    // The store's clock stamps every write now.
    let start = Utc::now();
    let day = |n: i64| start + Duration::days(n);
    let lifecycle = |n: i64, kind: EventKind| Event {
        id: uuid::Uuid::now_v7(),
        ant: "dev-a".parse().unwrap(),
        identity: "id1".into(),
        thread: None,
        at: day(n),
        kind,
    };
    for (n, kind) in [
        (-50, EventKind::AntAdded),
        (-35, EventKind::AntRetired),
        (-30, EventKind::AntRevived),
        (60, EventKind::AntRetired),
        (260, EventKind::AntRevived),
    ] {
        identity::record_event(&*items, &lifecycle(n, kind)).await.unwrap();
    }
    identity::returned_told(&*items, &"dev-a".parse().unwrap()).await.unwrap();

    let rested = store(&kept, &new("Rested", "Deploy uses 8080", day(0))).await;
    let core = store(&kept, &Memory { core: true, ..new("Core", "Answer briefly", day(0)) }).await;
    let review = Memory { status: Status::NeedsReview, ..new("Review", "Tests take long", day(0)) };
    let review = store(&kept, &review).await;
    let disused = store(&kept, &new("Disused", "Builds need the cache", day(-45))).await;
    let core_disused = store(&kept, &Memory { core: true, ..new("Idle", "Lint first", day(-45)) }).await;

    let now = day(261) + Duration::hours(1);
    let run = maintain(&*items, None, now).await.unwrap().unwrap();
    assert_eq!(actions(&run), [(disused.clone(), Action::Decayed), (core_disused.clone(), Action::Demoted)]);
    assert!(run.changes[0].reason.ends_with("unused for 101 days"), "{run:?}");
    assert_eq!(run.changes[1].reason, "core, unused for 101 days", "5 dormant days before, 200 since");
    assert_eq!(memory(&kept, &rested).await.status, Status::Active, "61 active days idle");
    assert_eq!(memory(&kept, &review).await.status, Status::NeedsReview, "untouched for 61 active days");
    assert!(memory(&kept, &core).await.core);

    let later = maintain(&*items, None, day(291) + Duration::hours(1)).await.unwrap().unwrap();
    let mut changed = actions(&later);
    changed.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = vec![
        (rested, Action::Decayed),
        (core, Action::Demoted),
        (review, Action::Retired),
        (disused, Action::Retired),
        (core_disused, Action::Decayed),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(changed, expected, "91 active days on");
}

#[tokio::test]
async fn disused_memories_decay_and_those_left_needing_review_retire() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    // Past the days of retirement since the store's clock stamped every write.
    let now = Utc::now() + Duration::days(RETIRE_AFTER_DAYS + 1) + Duration::hours(1);
    let old = now - Duration::days(100);
    let disused = store(&kept, &new("Old", "Deploy used 8080", old)).await;
    let important = store(&kept, &Memory { importance: 5, ..new("Key", "Releases need a tag", old) }).await;
    let mut read = new("Read", "Builds need the cache", old);
    read.access = Some(Access { days: 1, last: now - Duration::days(10) });
    let read = store(&kept, &read).await;
    let fresh = store(&kept, &new("Fresh", "Lint before pushing", now - Duration::days(1))).await;
    let core = store(&kept, &Memory { core: true, ..new("Core", "Answer briefly", now - Duration::days(80)) }).await;
    let review = Memory { status: Status::NeedsReview, ..new("Review", "Tests take long", now - Duration::days(1)) };
    let review = store(&kept, &review).await;

    let early = maintain(&*items, None, now - Duration::days(2)).await.unwrap().unwrap();
    assert_eq!(actions(&early), [(disused.clone(), Action::Decayed)], "retirement waits until 90 days untouched");
    let reason = &early.changes[0].reason;
    assert!(reason.starts_with("salience 0.10") && reason.ends_with("below 0.125: unused for 98 days"), "{reason}");
    assert_eq!(memory(&kept, &disused).await.status, Status::NeedsReview);
    let history = items.history(&disused).await.unwrap();
    let change = &history.last().unwrap().change;
    assert_eq!((change.by.as_str(), &change.after["status"]), (BY, &json!("needs_review")));
    assert_eq!(change.before.as_ref().unwrap()["status"], json!("active"));

    let run = maintain(&*items, None, now).await.unwrap().unwrap();
    let mut retired = actions(&run);
    retired.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = vec![(disused.clone(), Action::Retired), (review.clone(), Action::Retired)];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(retired, expected, "a decayed memory too, untouched since by the store's clock");
    assert!(run.changes.iter().all(|change| change.reason == "needed review, untouched for 91 days"), "{run:?}");
    for id in [&disused, &review] {
        assert_eq!(memory(&kept, id).await.status, Status::Retired, "{id}");
    }
    for id in [&important, &read, &fresh, &core] {
        assert_eq!(memory(&kept, id).await.status, Status::Active, "{id}");
    }

    let recorded = items.history(&record_id()).await.unwrap();
    assert_eq!(recorded.len(), 2, "one record, changed by each run");
    assert_eq!(recorded[1].change.before.as_ref().unwrap()["changes"][0]["action"], "decayed");
    assert_eq!(last_maintenance(&*items).await.unwrap(), Some(run));
    assert_eq!(next_maintenance(&*items).await.unwrap(), Some(now + Duration::hours(MAINTENANCE_EVERY_HOURS)));
    assert_eq!(maintain(&*items, None, now + Duration::hours(23)).await.unwrap(), None, "once a day");
}

#[tokio::test]
async fn a_run_changes_at_most_its_cap_leaving_the_rest_for_the_next() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now() + Duration::days(10);
    for n in 0..=MAX_CHANGES {
        store(&kept, &new(&format!("Old {n}"), &format!("Fact number {n}"), now - Duration::days(100))).await;
    }
    let run = maintain(&*items, None, now).await.unwrap().unwrap();
    assert_eq!((run.changes.len(), run.left), (MAX_CHANGES, 1));
    let next = maintain(&*items, None, now + Duration::days(1)).await.unwrap().unwrap();
    assert_eq!((next.changes.len(), next.left), (1, 0));
    assert_eq!(needs_review(&*items).await.unwrap().len(), MAX_CHANGES + 1);
}

#[tokio::test]
async fn core_memories_are_promoted_by_use_within_their_budget_and_demoted_by_disuse() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let used = |title: &str, bytes: usize, helpful: u32, days: u32| {
        let mut memory = new(title, &"u".repeat(bytes - title.len()), now - Duration::days(30));
        memory.votes = Votes { helpful, confirmed: Some(now - Duration::days(2)), ..Votes::default() };
        memory.access = (days > 0).then(|| Access { days, last: now - Duration::days(1) });
        memory
    };
    let idle = Memory { core: true, ..new("Idle", &"i".repeat(1_496), now - Duration::days(95)) };
    let idle = store(&kept, &idle).await;
    let kept_core = store(&kept, &Memory { core: true, ..used("Kept", 300, 1, 0) }).await;
    let most = store(&kept, &used("Most", 1_000, 2, 4)).await;
    let too_big = store(&kept, &used("Big", 800, 5, 0)).await;
    let small = store(&kept, &used("Small", 200, 0, 5)).await;
    let rare = store(&kept, &used("Rare", 100, 2, 2)).await;
    let mut others = Vec::new();
    for memory in [
        Memory { scope: Scope::Repo("github.com/o/r".into()), ..used("Repo", 100, 9, 0) },
        Memory { trust: Trust::Untrusted, ..used("Hook", 100, 9, 0) },
        Memory { kind: MemoryKind::Episode, ..used("Episode", 100, 9, 0) },
        Memory { status: Status::NeedsReview, ..used("Review", 100, 9, 0) },
    ] {
        others.push(store(&kept, &memory).await);
    }

    let run = maintain(&*items, None, now).await.unwrap().unwrap();
    assert_eq!(
        actions(&run),
        [(idle, Action::Demoted), (most.clone(), Action::Promoted), (small.clone(), Action::Promoted)]
    );
    assert_eq!(run.changes[0].reason, "core, unused for 95 days");
    assert_eq!(run.changes[1].reason, "used 6 times, last 1 days ago; core memories take 1300 of 2000 bytes");
    assert_eq!(run.changes[2].reason, "used 5 times, last 1 days ago; core memories take 1500 of 2000 bytes");
    let core: Vec<_> = crate::memory::core(&*items).await.unwrap().into_iter().map(|found| found.item.id).collect();
    assert_eq!(core.len(), 3);
    assert!([&kept_core, &most, &small].iter().all(|id| core.contains(id)), "{core:?}");
    for id in others.iter().chain([&too_big, &rare]) {
        assert!(!memory(&kept, id).await.core, "{id}");
    }
}

#[tokio::test]
async fn near_duplicates_of_one_scope_are_clustered_for_review() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let at = now - Duration::days(1);
    let body = "Deploy uses port 8080 on staging hosts";
    let a = store(&kept, &new("Deploys", body, at)).await;
    let b = store(&kept, &new("Deploys", "Deploy uses port 9090 on staging hosts", at)).await;
    let title = format!("{body} and many more words in a title far longer than the others");
    let c = store(&kept, &new(&title, body, at)).await;
    store(&kept, &Memory { scope: Scope::Human("alice".parse().unwrap()), ..new("Deploys", body, at) }).await;
    store(&kept, &Memory { kind: MemoryKind::Episode, ..new("Deploys", body, at) }).await;
    store(&kept, &Memory { status: Status::Wrong, ..new("Deploys", body, at) }).await;
    let d = store(&kept, &new("Pasta", "Pasta wants salted water", at)).await;
    let e = store(&kept, &new("Pasta", "Pasta wants salted water, always", at)).await;
    store(&kept, &new("Cache", "Builds need the cache", at)).await;

    let run = maintain(&*items, None, now).await.unwrap().unwrap();
    let mut expected = vec![vec![a, b, c], vec![d, e.clone()]];
    for cluster in &mut expected {
        cluster.sort();
    }
    expected.sort();
    assert_eq!(run.duplicates, expected, "c shares a's digest, its long title notwithstanding");
    assert!(run.changes.is_empty());

    items.change(&e, json!({"status": "wrong"}).as_object().unwrap().clone(), None, "test").await.unwrap();
    let clusters = duplicates(&*items).await.unwrap();
    let ids: Vec<Vec<ItemId>> =
        clusters.iter().map(|cluster| cluster.iter().map(|found| found.item.id.clone()).collect()).collect();
    let pasta = expected.iter().position(|cluster| cluster.contains(&e)).unwrap();
    assert_eq!(ids, [expected[1 - pasta].clone()], "a cluster left with one live memory is none");
}
