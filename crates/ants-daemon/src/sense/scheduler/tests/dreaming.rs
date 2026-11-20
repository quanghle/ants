use ants_proto::{
    ColonyEntry, Envelope, FindingOutcome, FindingVerdict, NoticeKind, ReviewCommit, ReviewOutcomes, ThreadId,
};
use ants_sense::dream::{self, ColonyTrail, ColonyTrails, Commit, Dream, REVIEW_BEGIN, Review, Signs, Tally, Thread};
use ants_sense::proposal;
use chrono::DateTime;

use super::super::dreams::{Dreams, fit_review, review_answer, review_given, review_received, serving_sage};
use super::*;

#[derive(Default)]
struct Fake {
    idle: Mutex<Option<DateTime<Utc>>>,
    threads: Mutex<Vec<Thread>>,
    reads: AtomicUsize,
    /// The configured sage, if any.
    sage: Mutex<Option<UserId>>,
    colony: Mutex<Vec<ColonyEntry>>,
    /// The review packets sent: to whom, as what, holding what.
    reviews: Mutex<Vec<(UserId, Review, String)>>,
    /// Whether sending a review packet fails.
    unsent: std::sync::atomic::AtomicBool,
    /// The colony's saved trails; none when they cannot be read.
    trails: Mutex<Option<Vec<ColonyTrail>>>,
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

    fn sage(&self) -> Option<UserId> {
        self.sage.locked().clone()
    }

    fn colony(&self) -> BoxFuture<'_, Result<Vec<ColonyEntry>>> {
        let entries = self.colony.locked().clone();
        Box::pin(async move { Ok(entries) })
    }

    fn trails(&self) -> BoxFuture<'_, ColonyTrails> {
        let trails = self.trails.locked().clone().map_or(ColonyTrails::UNREAD, ColonyTrails::read);
        Box::pin(async move { trails })
    }

    fn review<'a>(&'a self, sage: &'a UserId, dream: &'a Dream, text: String) -> BoxFuture<'a, Result<()>> {
        if self.unsent.load(Ordering::SeqCst) {
            return Box::pin(async { Err(anyhow::anyhow!("the review packet was not sent")) });
        }
        self.reviews.locked().push((sage.clone(), dream.review.clone().unwrap(), text));
        Box::pin(async { Ok(()) })
    }
}

impl Fake {
    fn serve_sage(&self, now: DateTime<Utc>) {
        *self.sage.locked() = Some(sage());
        self.colony.locked().push(listed(sage().as_str(), "m1", None, now));
    }

    fn set(&self, idle: Option<DateTime<Utc>>, threads: &[(&str, bool)]) {
        *self.idle.locked() = idle;
        *self.threads.locked() = threads
            .iter()
            .map(|(id, untrusted)| Thread {
                id: id.parse().unwrap(),
                untrusted: *untrusted,
                review: false,
                started_by: "human alice".into(),
                text: format!("the work of {id}"),
                timeline: format!("the calls of {id}"),
                actions: Vec::new(),
                signs: Signs { threads: 1, ..Signs::default() },
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
async fn a_refused_wake_holds_back_neither_maintenance_nor_the_dream() {
    let (sense, stores) = setup().await;
    let id = create(&sense, json!({"kind":"at","at":Utc::now()})).await;
    let fake = Arc::new(Fake::default());
    let now = Utc::now();
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    let host = Wakes::default();
    let refused = RpcError::unavailable("ant dev-scheduler was woken 60 times within the hour");
    host.errors.locked().extend(std::iter::repeat_n(refused, 10));

    let error = cycle(&sense, &*stores.items, &host, &dreaming(&fake), now).await.unwrap_err();
    assert!(format!("{error:#}").contains("60 times within the hour"), "{error:#}");
    assert!(memory::last_maintenance(&*stores.items).await.unwrap().is_some(), "maintenance ran");
    assert!(last(&*stores.items).await.unwrap().thread.is_none(), "the dream began, its wake refused");
    assert_eq!(host.requests.locked().len(), 2, "the reminder's wake, then the dream's");
    assert_eq!(remind::pending_work(&*stores.items).await.unwrap()[0].id, format!("{id}:1"));

    host.errors.locked().clear();
    cycle(&sense, &*stores.items, &host, &dreaming(&fake), now + chrono::Duration::minutes(1)).await.unwrap();
    assert_eq!(host.requests.locked()[2].id, format!("{id}:1"));
    assert!(remind::pending_work(&*stores.items).await.unwrap().is_empty());
    assert!(stores.items.get(&id).await.unwrap().unwrap().values.contains_key("delivered"));
    assert!(last(&*stores.items).await.unwrap().thread.is_some(), "the dream woken too");
}

#[tokio::test]
async fn a_dreams_vital_signs_move_with_the_ant_and_nothing_is_written_for_them_between_dreams() {
    let (log, identity) = (SharedLog::default(), SenseId::new());
    let first = open(log.clone(), identity.clone()).await;
    let fake = Arc::new(Fake::default());
    let (now, host) = (Utc::now(), Wakes::default());
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    let mut signs = fake.threads.locked()[0].signs.clone();
    signs.prompts.cancelled = 1;
    signs.prompts.capped = 1;
    signs.calls.insert("execute".into(), Tally { calls: 10, failed: 3 });
    signs.rejected = 1;
    fake.threads.locked()[0].signs = signs.clone();
    cycle(&Sense::new(), &*first, &host, &dreaming(&fake), now).await.unwrap();
    assert_eq!(last(&*first).await.unwrap().vitals, Some(signs));

    let written = log.0.last_sequence(&identity).await.unwrap();
    for hours in [1, 12, 23] {
        let at = now + chrono::Duration::hours(hours);
        *fake.idle.locked() = Some(at - chrono::Duration::hours(1));
        cycle(&Sense::new(), &*first, &host, &dreaming(&fake), at).await.unwrap();
    }
    assert_eq!(log.0.last_sequence(&identity).await.unwrap(), written, "nothing written between dreams");
    drop(first);

    let takeover = open(log, identity).await;
    let fake = Arc::new(Fake::default());
    let day_later = now + chrono::Duration::hours(25);
    fake.set(Some(day_later - chrono::Duration::hours(1)), &[("next-thread", false)]);
    fake.threads.locked()[0].signs.prompts.cancelled = 2;
    cycle(&Sense::new(), &*takeover, &host, &dreaming(&fake), day_later).await.unwrap();
    let requests = host.requests.locked().clone();
    let packet = &requests.last().unwrap().text;
    assert!(requests.last().unwrap().id.starts_with("dream-2-"), "{packet}");
    for row in
        ["| | dream 1 | dream 2 |", "| prompts cancelled by a human | 1 | 2 |", "| tool calls failed | 3 (30%) | 0 |"]
    {
        assert!(packet.contains(row), "{row} in {packet}");
    }
}

fn sage() -> UserId {
    "sage".parse().unwrap()
}

fn listed(ant: &str, machine: &str, sage: Option<&str>, beat: DateTime<Utc>) -> ColonyEntry {
    serde_json::from_value(json!({
        "ant": ant, "machine": machine, "instance": "run-1", "harness": "copilot", "capabilities": [],
        "started_at": beat, "heartbeat_at": beat, "activity": "idle", "handling": 0, "harnesses": 0,
        "version": "0.1.0", "perms": 1, "sage": sage,
    }))
    .unwrap()
}

/// The sage's reply to the review packet `review` holding `text`.
fn reply(review: &Review, text: &str) -> Envelope {
    let mut reply = Envelope::on_thread(review.thread.clone(), sage(), ant(), text);
    reply.in_reply_to = Some(review.message);
    reply.allow = Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec());
    reply
}

#[test]
fn a_review_fits_the_bus_even_when_the_non_outcome_fields_encode_large() {
    let text = "\u{1}🤔\"\\".repeat(90_000);
    let mut envelope = Envelope::on_thread(ThreadId::generate(), ant(), sage(), text.clone());
    let outcomes = ReviewOutcomes {
        dream: 2,
        findings: vec![FindingOutcome {
            finding: "1.1".into(),
            verdict: FindingVerdict::Applied,
            holds: true,
            reason: "🤔 \"kept\"".into(),
        }],
        earlier_findings: vec!["1.1".into()],
    };
    envelope.review_outcomes = Some(outcomes.clone());
    fit_review(&mut envelope).unwrap();
    assert!(serde_json::to_vec(&envelope).unwrap().len() <= 1024 * 1024 - 8 * 1024);
    assert!(text.starts_with(&envelope.text) && !envelope.text.is_empty() && envelope.text.len() < text.len());
    assert_eq!(envelope.review_outcomes, Some(outcomes.clone()));
    let text_after = envelope.text.clone();

    let commit =
        ReviewCommit { repo: "github.com/ants/dreams".into(), commit: "c0ffee".into(), thread: None, local: None };
    envelope.review_commits = vec![commit.clone(), ReviewCommit { local: Some(text), ..commit.clone() }];
    fit_review(&mut envelope).unwrap();
    assert!(serde_json::to_vec(&envelope).unwrap().len() <= 1024 * 1024 - 8 * 1024);
    assert_eq!(envelope.review_commits, [commit]);
    assert_eq!(envelope.text, text_after);
    assert_eq!(envelope.review_outcomes, Some(outcomes));
}

#[tokio::test]
async fn a_served_sage_reviews_the_packet_on_a_thread_of_its_own_and_its_findings_prompt_the_ant() {
    let items = open(SharedLog::default(), SenseId::new()).await;
    let sense = Sense::new();
    let fake = Arc::new(Fake::default());
    let settings = dreaming(&fake);
    let host = Wakes::default();
    let now = Utc::now();
    fake.serve_sage(now);
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false), ("hooked-thread", true)]);
    let counts = ants_proto::Counts { worked: 2, partly: 0, failed: 1 };
    let trail =
        ColonyTrail { name: "release-notes".parse().unwrap(), description: "Notes".into(), counts, saved_at: now };
    *fake.trails.locked() = Some(vec![trail]);

    let next = cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap().unwrap();
    assert!(host.requests.locked().is_empty(), "the ant waits for its sage");
    let begun = last(&*items).await.unwrap();
    assert_eq!(next, begun.at + chrono::Duration::hours(dream::SAGE_TIMEOUT_HOURS));
    let (to, review, text) = fake.reviews.locked()[0].clone();
    assert_eq!(to, sage());
    assert!(text.starts_with(&format!("# Review of dream 1 of {}", ant())), "{text}");
    let listed = "- release-notes (rated 2 worked, 0 partly, 1 failed): Notes";
    for part in
        ["messages no one", "```json", "the work of worked-thread", "### Thread hooked-thread", "c0ffee", listed]
    {
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
    assert_eq!(review_answer(&sense, &*items, &mut stranger).await.unwrap(), None, "only the sage answers a review");
    assert_eq!(review_answer(&sense, &*items, &mut answer).await.unwrap(), Some(true));
    assert!(answer.text.starts_with("# Dream 1") && answer.text.contains(REVIEW_BEGIN), "{}", answer.text);
    assert!(answer.text.contains("1.2 (sage: mistake, untrusted): Trusted the hook"), "{}", answer.text);
    assert_eq!(answer.allow, None, "the ant answers its findings unnarrowed");

    let sage_items = open(SharedLog::default(), SenseId::new()).await;
    let mut asked = Envelope::on_thread(review.thread.clone(), ant(), sage(), text.clone());
    review_received(&*sage_items, &asked).await.unwrap();
    assert!(dream::review_records(&*sage_items, 10).await.unwrap().is_empty(), "a message telling no dream");
    asked.review_outcomes = begun.review_outcomes();
    review_received(&*sage_items, &asked).await.unwrap();
    let spent = dream::ReviewSpend { tokens: 40_000, cost_micros: 250_000 };
    review_given(&*sage_items, &asked, &format!("```json\n{findings}\n```"), Some(spent)).await.unwrap();
    let record = dream::review_record(&*sage_items, &ant(), 1).await.unwrap().unwrap();
    let kinds: Vec<_> = record.findings.iter().map(|finding| finding.kind.as_str()).collect();
    assert_eq!((&record.thread, kinds.as_slice()), (&review.thread, ["lesson", "mistake"].as_slice()));
    assert_eq!(record.spent, Some(spent), "what the review thread spent");

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
    let sense = Sense::new();
    let fake = Arc::new(Fake::default());
    let settings = dreaming(&fake);
    let host = Wakes::default();
    let now = Utc::now();
    fake.serve_sage(now);
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    let review = fake.reviews.locked()[0].1.clone();
    let mut waiting = Envelope { notice: Some(NoticeKind::Quota), ..reply(&review, "out of quota") };
    assert_eq!(
        review_answer(&sense, &*items, &mut waiting).await.unwrap(),
        Some(false),
        "a sage out of quota may answer later"
    );
    let mut empty = reply(&review, "Nothing worth a look.\n```json\n{\"findings\": []}\n```");
    assert_eq!(review_answer(&sense, &*items, &mut empty).await.unwrap(), Some(false));
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
    let pinned = json!([
        {"kind": "lesson", "finding": "Pin the toolchain", "evidence": {"threads": ["worked-thread"]}, "action": "Pin it"},
    ]);
    let mut late = reply(&review, &format!("```json\n{pinned}\n```"));
    assert_eq!(review_answer(&sense, &*items, &mut late).await.unwrap(), Some(false), "a late answer wakes nobody");
    assert_eq!(last(&*items).await.unwrap().review.unwrap().late.len(), 1, "kept for the next dream");
    let day_later = now + chrono::Duration::hours(25);
    fake.colony.locked()[0].heartbeat_at = day_later;
    fake.set(Some(day_later - chrono::Duration::hours(1)), &[("next-thread", false)]);
    cycle(&Sense::new(), &*items, &host, &settings, day_later).await.unwrap();
    let next = last(&*items).await.unwrap();
    assert_eq!(next.number, 2);
    assert!(next.findings.iter().any(|finding| finding.text == "Pin the toolchain"), "{:?}", next.findings);
    let mut older = reply(&review, &format!("```json\n{pinned}\n```"));
    assert_eq!(
        review_answer(&sense, &*items, &mut older).await.unwrap(),
        Some(false),
        "dropped after the next dream began"
    );
    for day in 2..=dream::DROPPED_ANSWER_DREAMS + 1 {
        let at = day_later + chrono::Duration::hours(25 * (day as i64 - 1));
        fake.colony.locked()[0].heartbeat_at = at;
        let worked = format!("thread-{day}");
        fake.set(Some(at - chrono::Duration::hours(1)), &[(worked.as_str(), false)]);
        for _woken_then_begun in 0..2 {
            cycle(&Sense::new(), &*items, &host, &settings, at).await.unwrap();
        }
        let number = last(&*items).await.unwrap().number;
        assert_eq!(number, day + 1);
        let mut older = reply(&review, &format!("```json\n{pinned}\n```"));
        let taken = review_answer(&sense, &*items, &mut older).await.unwrap();
        let dropped = (number - 1 <= dream::DROPPED_ANSWER_DREAMS).then_some(false);
        assert_eq!(taken, dropped, "an answer to the review of dream 1 in dream {number}");
    }

    let woken = host.requests.locked().len();
    let items = open(SharedLog::default(), SenseId::new()).await;
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
    let review = fake.reviews.locked().last().unwrap().1.clone();
    let mut gave_up = Envelope { notice: Some(NoticeKind::GaveUp), ..reply(&review, "gave up") };
    assert_eq!(review_answer(&sense, &*items, &mut gave_up).await.unwrap(), Some(false));
    cycle(&Sense::new(), &*items, &host, &settings, now + chrono::Duration::minutes(1)).await.unwrap();
    assert_eq!(host.requests.locked().len(), woken + 1, "a sage giving up leaves the ant the packet at once");
}

#[tokio::test(start_paused = true)]
async fn a_sage_s_decline_wakes_the_dreamer_on_the_packet_alone() {
    let now = Utc::now();
    let (sense, stores) = setup().await;
    let fake = Arc::new(Fake::default());
    let host = Arc::new(Wakes::default());
    fake.serve_sage(now);
    fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
    let running = start(sense.clone(), host.clone(), dreaming(&fake));
    settle().await;
    let review = fake.reviews.locked()[0].1.clone();
    assert!(host.requests.locked().is_empty(), "the ant waits for the review");

    let mut decline = reply(&review, "[ants] sage declines the review: a sage is reviewed by no one.");
    assert_eq!(
        review_answer(&sense, &*stores.items, &mut decline).await.unwrap(),
        Some(false),
        "no prompt on the review thread"
    );
    settle().await;
    running.stop().await;
    let requests = host.requests.locked();
    assert_eq!(requests.len(), 1, "woken before the review deadline");
    assert!(requests[0].thread.is_none() && requests[0].text.contains("the work of worked-thread"));
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

#[tokio::test]
async fn a_review_sent_again_rechecks_who_is_a_sage_and_whether_the_recipient_is_served() {
    let now = Utc::now();
    for (named, served) in [(false, true), (true, true), (false, false)] {
        let items = open(SharedLog::default(), SenseId::new()).await;
        let fake = Arc::new(Fake::default());
        let settings = dreaming(&fake);
        let host = Wakes::default();
        fake.serve_sage(now);
        fake.set(Some(now - chrono::Duration::hours(1)), &[("worked-thread", false)]);
        fake.unsent.store(true, Ordering::SeqCst);
        cycle(&Sense::new(), &*items, &host, &settings, now).await.unwrap();
        let begun = last(&*items).await.unwrap();
        assert_eq!(begun.state, dream::State::Sent);
        assert!(!begun.review.unwrap().sent);

        fake.unsent.store(false, Ordering::SeqCst);
        let again = now + chrono::Duration::minutes(1);
        {
            let mut entries = fake.colony.locked();
            entries[0].heartbeat_at = again;
            if !served {
                entries[0].paused = Some(ants_proto::Paused { since: again, by: None });
            }
            if named {
                entries.push(listed("dev-b", "m2", Some(ant().as_str()), again));
            }
        }
        cycle(&Sense::new(), &*items, &host, &settings, again).await.unwrap();
        cycle(&Sense::new(), &*items, &host, &settings, again + chrono::Duration::minutes(1)).await.unwrap();
        let looked = last(&*items).await.unwrap();
        let requests = host.requests.locked().clone();
        if !named && served {
            assert_eq!(fake.reviews.locked().len(), 1, "sent again, once");
            assert!(requests.is_empty(), "the ant waits for its sage");
            assert!(looked.review.unwrap().sent);
            continue;
        }
        assert!(fake.reviews.locked().is_empty(), "no review packet for named={named}, served={served}");
        assert_eq!(requests.len(), 1, "woken at once, once");
        assert!(requests[0].text.starts_with("# Dream 1"), "{}", requests[0].text);
        assert_eq!(looked.state, dream::State::Fallback);
        assert!(looked.thread.is_some());
    }
}

#[test]
fn an_ant_a_live_ant_names_as_its_sage_sends_its_own_sage_no_review_packet() {
    let now = Utc::now();
    let (lead, sage) = ("lead".parse::<UserId>().unwrap(), UserId::sage());
    let serving = |entries: &[ColonyEntry]| serving_sage(&lead, &sage, entries, now);
    let (own, served) = (listed("lead", "m1", Some(sage.as_str()), now), listed(sage.as_str(), "m1", None, now));
    assert!(serving(&[own.clone(), served.clone()]), "no other ant names lead");
    let other = listed("dev-b", "m1", Some(sage.as_str()), now);
    assert!(serving(&[other, own.clone(), served.clone()]));

    let elsewhere = listed("dev-a", "m2", Some("lead"), now);
    assert!(
        !serving(&[elsewhere, own.clone(), served.clone()]),
        "dev-a, whose config on another machine or shared settings name lead, makes lead a sage"
    );
    let stale = listed("dev-a", "m2", Some("lead"), now - chrono::Duration::minutes(5));
    assert!(serving(&[stale, own.clone(), served]), "an ant no daemon serves now makes none");
    assert!(!serving(&[own]), "no daemon serves the sage");
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
