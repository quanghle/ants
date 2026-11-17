use ants_proto::ThreadId;
use ants_sense::dream::{self, Commit, Thread};
use ants_sense::proposal;
use chrono::DateTime;

use super::super::dreams::Dreams;
use super::*;

#[derive(Default)]
struct Fake {
    idle: Mutex<Option<DateTime<Utc>>>,
    threads: Mutex<Vec<Thread>>,
    reads: AtomicUsize,
}

impl Dreaming for Fake {
    fn idle_since(&self) -> Option<DateTime<Utc>> {
        *self.idle.locked()
    }

    fn threads(&self, _: DateTime<Utc>, _: DateTime<Utc>) -> BoxFuture<'_, Result<Vec<Thread>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let threads = self.threads.locked().clone();
        Box::pin(async move { Ok(threads) })
    }

    fn commits<'a>(&'a self, _: &'a [ThreadId]) -> BoxFuture<'a, Vec<Commit>> {
        let commit = Commit { repo: "github.com/ants/dreams".into(), commit: "c0ffee".into(), thread: None };
        Box::pin(async move { vec![commit] })
    }
}

impl Fake {
    fn set(&self, idle: Option<DateTime<Utc>>, threads: &[(&str, bool)]) {
        *self.idle.locked() = idle;
        *self.threads.locked() = threads
            .iter()
            .map(|(id, untrusted)| Thread {
                id: id.parse().unwrap(),
                untrusted: *untrusted,
                started_by: "human alice".into(),
                text: format!("the work of {id}"),
                timeline: format!("the calls of {id}"),
            })
            .collect();
    }
}

fn dreaming(fake: &Arc<Fake>) -> Settings {
    Settings { dreams: Some(Dreams::new(fake.clone())), ..settings() }
}

async fn last(items: &dyn Items) -> Option<dream::Dream> {
    dream::last(items).await.unwrap().map(|(dream, _)| dream)
}

#[tokio::test]
async fn a_dream_waits_for_an_idle_ant_with_activity_and_comes_once_a_day() {
    let items = open(SharedLog::default(), SenseId::new()).await;
    let fake = Arc::new(Fake::default());
    let settings = dreaming(&fake);
    let host = Wakes::default();
    let now = Utc::now();
    let half_hour = chrono::Duration::minutes(30);

    fake.set(None, &[("worked-thread", false)]);
    let next = cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap().unwrap();
    assert!(next >= now + half_hour && next < now + chrono::Duration::minutes(31), "a running prompt waits");
    fake.set(Some(now - chrono::Duration::minutes(10)), &[("worked-thread", false)]);
    let next = cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap().unwrap();
    assert_eq!(next, now + chrono::Duration::minutes(20), "the ant has 30 idle minutes first");
    assert_eq!(fake.reads.load(Ordering::SeqCst), 0);
    assert!(host.requests.locked().is_empty() && last(&*items).await.is_none());

    let idle = now - chrono::Duration::minutes(40);
    fake.set(Some(idle), &[]);
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    cycle(&Sense::new(), &*items, &host, &settings, now + chrono::Duration::hours(1)).await.unwrap();
    assert_eq!(fake.reads.load(Ordering::SeqCst), 1, "a quiet ant is looked at once until a prompt runs");
    assert!(host.requests.locked().is_empty() && last(&*items).await.is_none(), "no activity, no dream");

    fake.set(Some(idle), &[("worked-thread", false), ("hooked-thread", true)]);
    *fake.idle.locked() = Some(idle + chrono::Duration::seconds(1));
    let next = cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap().unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].thread.is_none(), "a dream has a thread of its own");
    assert!(requests[0].id.starts_with("dream-1-"), "{}", requests[0].id);
    let parts = ["# Dream 1", "the work of worked-thread", "the calls of worked-thread", "### Thread hooked-thread"];
    for part in parts.into_iter().chain(["untrusted", "c0ffee"]) {
        assert!(requests[0].text.contains(part), "{part} in {}", requests[0].text);
    }
    let first = last(&*items).await.unwrap();
    assert_eq!((first.number, first.state), (1, dream::State::Fallback));
    assert!(first.thread.is_some());
    assert!(next <= now + chrono::Duration::hours(24) + chrono::Duration::minutes(1), "the next dream in a day");

    let later = now + chrono::Duration::hours(23);
    *fake.idle.locked() = Some(later - chrono::Duration::hours(1));
    cycle(&Sense::new(), &*items, &host, &settings, later).await.unwrap();
    assert_eq!(host.requests.locked().len(), 1, "once a day");

    let day_later = now + chrono::Duration::hours(25);
    let own = first.thread.as_ref().unwrap().as_str();
    fake.set(Some(day_later - chrono::Duration::hours(1)), &[(own, false)]);
    cycle(&Sense::new(), &*items, &host, &settings, day_later).await.unwrap();
    assert_eq!(host.requests.locked().len(), 1, "a dream's own thread is no work to review");
    fake.set(Some(day_later - chrono::Duration::minutes(59)), &[(own, false), ("next-thread", false)]);
    cycle(&Sense::new(), &*items, &host, &settings, day_later).await.unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].id.starts_with("dream-2-") && requests[1].text.contains("next-thread"));
    assert!(!requests[1].text.contains(&format!("### Thread {own}")));
    assert_eq!(last(&*items).await.unwrap().number, 2);
}

#[tokio::test]
async fn a_takeover_wakes_a_dream_begun_but_not_woken_and_begins_none_again() {
    let log = SharedLog::default();
    let identity = SenseId::new();
    let first = open(log.clone(), identity.clone()).await;
    let fake = Arc::new(Fake::default());
    let now = Utc::now();
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    let host = Wakes::default();
    host.errors.locked().push_back(RpcError::unavailable("disconnected"));
    let next = cycle(&Sense::new(), &*first, &host, &dreaming(&fake), now).await.unwrap().unwrap();
    assert!(next <= now + chrono::Duration::minutes(2), "a failed wake retries soon");
    let begun = last(&*first).await.unwrap();
    assert!(begun.thread.is_none());
    drop(first);

    let takeover = open(log, identity).await;
    let fake = Arc::new(Fake::default());
    fake.set(Some(now), &[("worked-thread", false), ("newer-thread", false)]);
    let minute_later = now + chrono::Duration::minutes(1);
    cycle(&Sense::new(), &*takeover, &host, &dreaming(&fake), minute_later).await.unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].id, requests[1].id, "the same wake, deduplicated by the host");
    assert!(requests[1].text.contains("worked-thread"));
    assert!(!requests[1].text.contains("newer-thread"), "the dream covers the threads it began with");
    let woken = last(&*takeover).await.unwrap();
    assert_eq!(woken.number, 1);
    assert!(woken.thread.is_some());

    let hours_later = now + chrono::Duration::hours(2);
    *fake.idle.locked() = Some(now + chrono::Duration::hours(1));
    cycle(&Sense::new(), &*takeover, &host, &dreaming(&fake), hours_later).await.unwrap();
    assert_eq!(host.requests.locked().len(), 2, "no second dream within a day");
}

#[tokio::test]
async fn a_proposal_ripe_for_confirmation_calls_for_a_dream_without_new_work() {
    let items = open(SharedLog::default(), SenseId::new()).await;
    let fake = Arc::new(Fake::default());
    let settings = dreaming(&fake);
    let host = Wakes::default();
    let now = Utc::now();
    fake.set(Some(now - chrono::Duration::hours(1)), &[]);
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    assert!(last(&*items).await.is_none(), "no activity, no dream");

    let writer = CallContext { at: now - chrono::Duration::hours(13), ..call(json!({})).context };
    let made = proposal::Proposal {
        target: ants_sense::persona::persona_id(),
        kind: ants_sense::persona::PERSONA.into(),
        summary: "Voice: plain.".into(),
        version: serde_json::Map::new(),
        base: 0,
        reason: "Users skim.".into(),
        status: proposal::Status::Pending,
        writer,
        finding: None,
        superseded_by: None,
        decided: None,
    };
    proposal::propose(&*items, made).await.unwrap();
    cycle(&Sense::new(), &*items, &host, &settings, now + chrono::Duration::minutes(1)).await.unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 1, "a quiet ant dreams for its proposal");
    assert!(requests[0].text.contains("Voice: plain."), "{}", requests[0].text);
}
