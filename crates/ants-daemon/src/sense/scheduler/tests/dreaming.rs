use ants_proto::{ColonyEntry, Envelope, NoticeKind, ThreadId};
use ants_sense::dream::{self, Commit, REVIEW_BEGIN, Review, Thread};
use ants_sense::proposal;
use chrono::DateTime;

use super::super::dreams::{Dreams, review_answer, serving_sage};
use super::*;

#[derive(Default)]
struct Fake {
    idle: Mutex<Option<DateTime<Utc>>>,
    threads: Mutex<Vec<Thread>>,
    reads: AtomicUsize,
    /// The sage served, if any.
    sage: Mutex<Option<UserId>>,
    /// The review packets sent: to whom, as what, holding what.
    reviews: Mutex<Vec<(UserId, Review, String)>>,
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

    fn sage(&self) -> BoxFuture<'_, Result<Option<UserId>>> {
        let sage = self.sage.locked().clone();
        Box::pin(async move { Ok(sage) })
    }

    fn review<'a>(
        &'a self,
        sage: &'a UserId,
        review: &'a Review,
        _: &'a [Commit],
        text: String,
    ) -> BoxFuture<'a, Result<()>> {
        self.reviews.locked().push((sage.clone(), review.clone(), text));
        Box::pin(async { Ok(()) })
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
                actions: Vec::new(),
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

fn sage() -> UserId {
    "sage".parse().unwrap()
}

/// The sage's reply to the review packet `review` holding `text`.
fn reply(review: &Review, text: &str) -> Envelope {
    let mut reply = Envelope::on_thread(review.thread.clone(), sage(), ant(), text);
    reply.in_reply_to = Some(review.message);
    reply.allow = Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec());
    reply
}

#[tokio::test]
async fn a_served_sage_reviews_the_packet_on_a_thread_of_its_own_and_its_findings_prompt_the_ant() {
    let items = open(SharedLog::default(), SenseId::new()).await;
    let fake = Arc::new(Fake::default());
    let settings = dreaming(&fake);
    let host = Wakes::default();
    let now = Utc::now();
    *fake.sage.locked() = Some(sage());
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false), ("hooked-thread", true)]);

    let next = cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap().unwrap();
    assert!(host.requests.locked().is_empty(), "the ant waits for its sage");
    let begun = last(&*items).await.unwrap();
    assert_eq!(next, begun.at + chrono::Duration::hours(dream::SAGE_TIMEOUT_HOURS));
    let (to, review, text) = fake.reviews.locked()[0].clone();
    assert_eq!(to, sage());
    assert!(text.starts_with(&format!("# Review of dream 1 of {}", ant())), "{text}");
    for part in ["messages no one", "```json", "the work of worked-thread", "### Thread hooked-thread", "c0ffee"] {
        assert!(text.contains(part), "{part} in {text}");
    }
    cycle(&Sense::new(), &*items, &host, &settings, now + chrono::Duration::hours(1)).await.unwrap();
    assert_eq!(fake.reviews.locked().len(), 1, "sent once");

    let findings = json!({"findings": [
        {"kind": "lesson", "finding": "Pin the toolchain", "evidence": {"threads": ["worked-thread"]}, "action": "Remember it"},
        {"kind": "mistake", "finding": "Trusted the hook", "evidence": {"threads": ["hooked-thread"]}, "action": "Check"},
    ]});
    let mut answer = reply(&review, &format!("```json\n{findings}\n```"));
    let mut stranger = Envelope { from: "mallory".parse().unwrap(), ..answer.clone() };
    assert_eq!(review_answer(&*items, &mut stranger).await.unwrap(), None, "only the sage answers a review");
    assert_eq!(review_answer(&*items, &mut answer).await.unwrap(), Some(true));
    assert!(answer.text.starts_with("# Dream 1") && answer.text.contains(REVIEW_BEGIN), "{}", answer.text);
    assert!(answer.text.contains("1.2 (sage: mistake, untrusted): Trusted the hook"), "{}", answer.text);
    assert_eq!(answer.allow, None, "the ant answers its findings unnarrowed");

    let timeout = now + chrono::Duration::hours(dream::SAGE_TIMEOUT_HOURS + 1);
    *fake.idle.locked() = Some(timeout - chrono::Duration::hours(1));
    cycle(&Sense::new(), &*items, &host, &settings, timeout).await.unwrap();
    assert!(host.requests.locked().is_empty(), "the findings woke the ant already");

    let day_later = now + chrono::Duration::hours(25);
    fake.set(Some(day_later - chrono::Duration::hours(1)), &[(review.thread.as_str(), false)]);
    cycle(&Sense::new(), &*items, &host, &settings, day_later).await.unwrap();
    assert_eq!(last(&*items).await.unwrap().number, 1, "the review thread is no work to review");
}

#[tokio::test]
async fn without_findings_nobody_wakes_and_without_an_answer_the_ant_dreams_on_the_packet_alone() {
    let items = open(SharedLog::default(), SenseId::new()).await;
    let fake = Arc::new(Fake::default());
    let settings = dreaming(&fake);
    let host = Wakes::default();
    let now = Utc::now();
    *fake.sage.locked() = Some(sage());
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    let review = fake.reviews.locked()[0].1.clone();
    let mut waiting = Envelope { notice: Some(NoticeKind::Quota), ..reply(&review, "out of quota") };
    assert_eq!(
        review_answer(&*items, &mut waiting).await.unwrap(),
        Some(false),
        "a sage out of quota may answer later"
    );
    let mut empty = reply(&review, "Nothing worth a look.\n```json\n{\"findings\": []}\n```");
    assert_eq!(review_answer(&*items, &mut empty).await.unwrap(), Some(false));
    cycle(&Sense::new(), &*items, &host, &settings, now + chrono::Duration::hours(7)).await.unwrap();
    assert!(host.requests.locked().is_empty(), "an answer without findings wakes nobody");

    let items = open(SharedLog::default(), SenseId::new()).await;
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    let review = fake.reviews.locked()[1].1.clone();
    let timeout = last(&*items).await.unwrap().at + chrono::Duration::hours(dream::SAGE_TIMEOUT_HOURS);
    cycle(&Sense::new(), &*items, &host, &settings, timeout - chrono::Duration::minutes(1)).await.unwrap();
    assert!(host.requests.locked().is_empty());
    cycle(&Sense::new(), &*items, &host, &settings, timeout).await.unwrap();
    let requests = host.requests.locked().clone();
    assert_eq!(requests.len(), 1, "six hours unanswered");
    assert!(requests[0].text.starts_with("# Dream 1") && requests[0].text.contains("sense_remember"));
    let mut late = reply(&review, "```json\n[{\"kind\": \"lesson\"}]\n```");
    assert_eq!(review_answer(&*items, &mut late).await.unwrap(), Some(false), "a late answer wakes nobody");

    let items = open(SharedLog::default(), SenseId::new()).await;
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    let review = fake.reviews.locked()[2].1.clone();
    let mut gave_up = Envelope { notice: Some(NoticeKind::GaveUp), ..reply(&review, "gave up") };
    assert_eq!(review_answer(&*items, &mut gave_up).await.unwrap(), Some(false));
    cycle(&Sense::new(), &*items, &host, &settings, now + chrono::Duration::minutes(1)).await.unwrap();
    assert_eq!(host.requests.locked().len(), 2, "a sage giving up leaves the ant the packet at once");
}

#[tokio::test]
async fn an_ant_without_a_sage_serving_it_or_its_own_sage_dreams_on_the_packet_alone() {
    let now = Utc::now();
    for sage in [None, Some(ant())] {
        let items = open(SharedLog::default(), SenseId::new()).await;
        let fake = Arc::new(Fake::default());
        *fake.sage.locked() = sage;
        fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
        let host = Wakes::default();
        cycle(&Sense::new(), &*items, &host, &dreaming(&fake), now).await.unwrap();
        assert!(fake.reviews.locked().is_empty());
        assert_eq!(host.requests.locked().len(), 1, "woken at once");
        let begun = last(&*items).await.unwrap();
        assert_eq!((begun.sage, begun.review, begun.state), (None, None, dream::State::Fallback));
    }
}

#[test]
fn an_ant_a_live_ant_names_as_its_sage_sends_its_own_sage_no_review_packet() {
    let now = Utc::now();
    let listed = |ant: &str, machine: &str, sage: Option<&str>, beat: DateTime<Utc>| -> ColonyEntry {
        serde_json::from_value(json!({
            "ant": ant, "machine": machine, "instance": "run-1", "harness": "copilot", "capabilities": [],
            "started_at": beat, "heartbeat_at": beat, "activity": "idle", "handling": 0, "harnesses": 0,
            "version": "0.1.0", "perms": 1, "sage": sage,
        }))
        .unwrap()
    };
    let (lead, sage) = ("lead".parse::<UserId>().unwrap(), UserId::sage());
    let serving = |entries: &[ColonyEntry]| serving_sage(&lead, sage.clone(), entries, now);
    let (own, served) = (listed("lead", "m1", Some("sage"), now), listed("sage", "m1", None, now));
    assert_eq!(serving(&[own.clone(), served.clone()]), Some(sage.clone()), "no other ant names lead");
    let other = listed("dev-b", "m1", Some("sage"), now);
    assert_eq!(serving(&[other, own.clone(), served.clone()]), Some(sage.clone()));

    let elsewhere = listed("dev-a", "m2", Some("lead"), now);
    assert_eq!(
        serving(&[elsewhere, own.clone(), served.clone()]),
        None,
        "dev-a, whose config on another machine or shared settings name lead, makes lead a sage"
    );
    let stale = listed("dev-a", "m2", Some("lead"), now - chrono::Duration::minutes(5));
    assert_eq!(serving(&[stale, own.clone(), served]), Some(sage.clone()), "an ant no daemon serves now makes none");
    assert_eq!(serving(&[own]), None, "no daemon serves the sage");
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

#[tokio::test]
async fn a_review_names_the_git_directory_its_commits_are_in() {
    let Some(repo) = crate::test_support::repo("dreams-review-commits") else { return };
    let worktree = repo.parent().unwrap().join("worktree");
    crate::test_support::run(&repo, &["worktree", "add", "--quiet", "-b", "t1", worktree.to_str().unwrap()]);
    let commit = |thread: Option<&str>| Commit {
        repo: "github.com/ants/dreams".into(),
        commit: "c0ffee".into(),
        thread: thread.map(|thread| thread.parse().unwrap()),
    };
    assert_eq!(super::super::dreams::review_commits(&worktree, &[]).await, []);

    let named = super::super::dreams::review_commits(&worktree, &[commit(None), commit(Some("t1"))]).await;

    let local = repo.join(".git").display().to_string();
    assert_eq!(named.iter().map(|c| c.local.as_deref()).collect::<Vec<_>>(), [Some(&*local), Some(&*local)]);
    assert_eq!(named[1].thread, Some("t1".parse().unwrap()));
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}
