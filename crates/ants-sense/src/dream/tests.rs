//! Tests of dreams: when one begins and closes, a sage's unanswered review, findings gathered, rejected and resting,
//! verdicts, the write cap and the packet's budget; with fixtures for the submodules.

use std::sync::Arc;

use ants_proto::provider::{CallContext, Event, EventKind, Format, Output, Provider as _, ToolCall};
use serde_json::json;

use super::*;
use crate::Sense;
use crate::items::Values;
use crate::memory::tests::memory;
use crate::memory::{Memory, Votes};
use crate::testing::{Kept, context, events, human, sense, tool};
use crate::tools::Answer;

fn on(thread: u8) -> CallContext {
    let thread = format!("019a7d2c-0000-7000-8000-0000000000{thread:02}").parse().unwrap();
    CallContext { thread, ..context(human()) }
}

/// The ant of the tests, reading its packet alone.
fn ant() -> Reader {
    Reader::Ant(context(human()).ant)
}

fn start(at: DateTime<Utc>, writer: &CallContext) -> Start<'_> {
    let trails = &ColonyTrails::UNREAD;
    Start {
        at,
        recipient: Recipient::Ant,
        threads: Vec::new(),
        commits: Vec::new(),
        writer,
        vitals: Vec::new(),
        trails,
    }
}

fn review_request(dream: u64) -> ReviewOutcomes {
    ReviewOutcomes { dream, findings: Vec::new(), earlier_findings: Vec::new() }
}

async fn store(kept: &Arc<Kept>, memory: &Memory) -> ItemId {
    let id = ItemId::new();
    let items: Arc<dyn Items> = kept.clone();
    items.write(&[memory.create(id.clone())], "test").await.unwrap();
    id
}

fn lesson(title: &str) -> Memory {
    Memory::new(MemoryKind::Lesson, Scope::Own, title.into(), format!("{title}, in a body."), &context(human()))
}

async fn answer(sense: &Sense, thread: u8, arguments: serde_json::Value) -> Answer {
    tool(sense, dream, arguments, on(thread)).await
}

fn error(answer: &Answer) -> &str {
    assert!(answer.is_error, "{answer:?}");
    answer.json["error"].as_str().unwrap()
}

#[tokio::test]
async fn a_dream_begins_at_most_once_a_day_and_the_next_closes_it_with_its_diary() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let writer = on(9);
    let now = Utc::now();

    let first = begin(&*items, start(now, &writer)).await.unwrap().unwrap();
    assert_eq!((first.number, first.state, first.since), (1, State::Fallback, now - Duration::hours(24)));
    assert!(first.wake_due(now) && first.deadline().is_none());
    assert_eq!(begin(&*items, start(now + Duration::hours(23), &writer)).await.unwrap(), None);
    let woken = woken(&*items, 1, &on(1).thread).await.unwrap();
    assert!(!woken.wake_due(now));

    let later = now + Duration::hours(24);
    let second = begin(&*items, start(later, &writer)).await.unwrap().unwrap();
    assert_eq!((second.number, second.since), (2, now));
    let (first, _) = get(&*items, 1).await.unwrap().unwrap();
    assert_eq!(first.state, State::Done, "the ant left it unfinished");
    let diary = memory(&kept, first.diary.as_ref().unwrap()).await;
    assert_eq!((diary.kind, diary.writer.at), (MemoryKind::Episode, later));
    assert!(diary.title.starts_with("Dream 1: 0 findings"), "{}", diary.title);
    assert_eq!(last(&*items).await.unwrap().unwrap().0.number, 2);
}

#[tokio::test]
async fn a_review_the_sage_leaves_unanswered_falls_back_to_the_ant_after_six_hours() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let writer = on(9);
    let now = Utc::now();
    let sage: UserId = "sage".parse().unwrap();
    assert_eq!(recipient(Some(sage.clone())), Recipient::Sage(sage.clone()));
    assert_eq!(recipient(None), Recipient::Ant);

    let start = Start { recipient: Recipient::Sage(sage.clone()), ..start(now, &writer) };
    let sent = begin(&*items, start).await.unwrap().unwrap();
    assert_eq!((sent.state, sent.sage.as_ref()), (State::Sent, Some(&sage)));
    let timeout = now + Duration::hours(SAGE_TIMEOUT_HOURS);
    assert_eq!(sent.deadline(), Some(timeout));
    assert!(!sent.wake_due(timeout - Duration::seconds(1)) && sent.wake_due(timeout));

    let woken = woken(&*items, 1, &on(1).thread).await.unwrap();
    assert_eq!((woken.state, woken.deadline()), (State::Fallback, None));
    assert_eq!(get(&*items, 1).await.unwrap().unwrap().0, woken);
}

#[tokio::test]
async fn findings_gather_what_needs_judging_and_the_open_ones_come_again() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let writer = on(9);
    let votes = Votes { helpful: 1, stale: 1, ..Votes::default() };
    let disputed = store(&kept, &Memory { votes, ..lesson("Disputed") }).await;
    let doubted = store(&kept, &Memory { status: Status::NeedsReview, ..lesson("Doubted") }).await;
    let episode = Memory { kind: MemoryKind::Episode, ..lesson("Shipped the parser") };
    let episode = store(&kept, &episode).await;
    let untrusted = Memory { trust: memory::Trust::Untrusted, status: Status::NeedsReview, ..lesson("Relayed") };
    let untrusted = store(&kept, &untrusted).await;
    let now = Utc::now();

    let first = begin(&*items, start(now, &writer)).await.unwrap().unwrap();
    let found: Vec<_> =
        first.findings.iter().map(|f| (f.id.as_str(), f.kind, f.items[0].clone(), f.untrusted)).collect();
    assert_eq!(
        found,
        [
            ("1.1", FindingKind::Contradiction, disputed.clone(), false),
            ("1.2", FindingKind::NeedsReview, doubted.clone(), false),
            ("1.3", FindingKind::NeedsReview, untrusted.clone(), true),
            ("1.4", FindingKind::Distill, episode.clone(), false),
        ]
    );
    let shown = packet(&*items, &first, &ant(), &[], &ColonyTrails::UNREAD).await.unwrap();
    assert!(shown.contains("- 1.3 (needs_review, untrusted): Memory"), "{shown}");
    woken(&*items, 1, &on(1).thread).await.unwrap();
    let verdicts = json!({"verdicts": [
        {"finding": "1.1", "verdict": "applied", "reason": "Voted it helpful again.", "items": [disputed]},
        {"finding": "1.2", "verdict": "deferred", "reason": "Needs the next release."},
        {"finding": "1.3", "verdict": "rejected", "reason": "Outside text."},
    ], "done": true});
    let done = answer(&sense, 1, verdicts).await;
    assert!(!done.is_error, "{done:?}");
    assert!(done.text.contains("1 unanswered findings come again"), "{}", done.text);
    let diary: ItemId = done.json["diary"].as_str().unwrap().parse().unwrap();
    let written = memory(&kept, &diary).await;
    assert!(written.title.starts_with("Dream 1: 4 findings, 1 applied, 1 rejected, 1 deferred, 1 unanswered"));
    assert!(written.body.contains("1.2 needs_review: deferred: Needs the next release."), "{}", written.body);

    let second = begin(&*items, start(now + Duration::hours(24), &writer)).await.unwrap().unwrap();
    let found: Vec<_> = second.findings.iter().map(|f| (f.id.as_str(), f.kind)).collect();
    assert_eq!(
        found,
        [("1.2", FindingKind::NeedsReview), ("1.4", FindingKind::Distill)],
        "the deferred and unanswered come again, the applied and rejected not, nor the diary to distill"
    );
    let packet = packet(&*items, &second, &ant(), &[], &ColonyTrails::UNREAD).await.unwrap();
    assert!(packet.contains("## Verdicts on dream 1"), "{packet}");
    assert!(packet.contains("- 1.4 (distill): unanswered"), "{packet}");
    assert!(packet.contains("- 1.2 (needs_review, offered again): Memory"), "{packet}");
}

/// Begin dream `number` at `at` and reject its `finding` on its thread.
async fn reject(sense: &Sense, items: &dyn Items, number: u64, at: DateTime<Utc>, finding: &str) -> Dream {
    let writer = on(9);
    let dream = begin(items, start(at, &writer)).await.unwrap().unwrap();
    assert_eq!(dream.number, number);
    let thread = u8::try_from(number).unwrap();
    woken(items, number, &on(thread).thread).await.unwrap();
    let verdicts = json!({"verdicts": [{"finding": finding, "verdict": "rejected", "reason": "Still holds."}]});
    let answered = answer(sense, thread, verdicts).await;
    assert!(!answered.is_error, "{answered:?}");
    dream
}

async fn rested(items: &dyn Items) -> Vec<String> {
    let (record, _) = record(items).await.unwrap().unwrap();
    record.rested.into_iter().map(|rested| rested.finding).collect()
}

fn ids(dream: &Dream) -> Vec<(&str, &ItemId)> {
    dream.findings.iter().map(|finding| (finding.id.as_str(), &finding.items[0])).collect()
}

#[tokio::test]
async fn a_rejected_finding_rests_until_what_it_is_about_changes() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let doubted = store(&kept, &Memory { status: Status::NeedsReview, ..lesson("Doubted") }).await;
    let other = store(&kept, &Memory { status: Status::NeedsReview, ..lesson("Questioned") }).await;
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);

    let first = reject(&sense, &*items, 1, day(0), "1.1").await;
    assert_eq!(ids(&first), [("1.1", &doubted), ("1.2", &other)]);
    assert_eq!(first.findings[0].key, Some(format!("needs_review:{doubted}")));
    let read = tool(&sense, memory::recall, json!({"id": doubted}), on(1)).await;
    assert!(!read.is_error && memory(&kept, &doubted).await.access.is_some(), "{read:?}");

    let second = reject(&sense, &*items, 2, day(1), "1.2").await;
    assert_eq!(ids(&second), [("1.2", &other)], "the rejected rests though read since; the unanswered comes again");
    assert_eq!(rested(&*items).await, ["1.1"]);
    let third = begin(&*items, start(day(2), &on(9))).await.unwrap().unwrap();
    assert!(third.findings.is_empty(), "{:?}", third.findings);

    let mut values = Values::new();
    values.insert("importance".into(), json!(4));
    items.change(&doubted, values, None, "test").await.unwrap();
    let fourth = begin(&*items, start(day(3), &on(9))).await.unwrap().unwrap();
    assert_eq!(ids(&fourth), [("4.1", &doubted)], "changed since the verdict; the other still rests");
    assert_eq!(rested(&*items).await, ["1.2"]);
}

#[tokio::test]
async fn a_rejected_finding_comes_again_after_fourteen_active_days() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let doubted = store(&kept, &Memory { status: Status::NeedsReview, ..lesson("Doubted") }).await;
    // The store's clock stamps the verdict now.
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let lifecycle = |n: i64, kind: EventKind| Event {
        id: uuid::Uuid::now_v7(),
        ant: "dev-a".parse().unwrap(),
        identity: "id1".into(),
        thread: None,
        at: day(n),
        kind,
    };
    for (n, kind) in [(-10, EventKind::AntAdded), (3, EventKind::AntRetired), (8, EventKind::AntRevived)] {
        identity::record_event(&*items, &lifecycle(n, kind)).await.unwrap();
    }

    reject(&sense, &*items, 1, day(0), "1.1").await;
    let resting = begin(&*items, start(day(15), &on(9))).await.unwrap().unwrap();
    assert!(resting.findings.is_empty(), "10 active days since the verdict: {:?}", resting.findings);
    let back = begin(&*items, start(day(20), &on(9))).await.unwrap().unwrap();
    assert_eq!(ids(&back), [("3.1", &doubted)], "15 active days since the verdict");
    assert!(rested(&*items).await.is_empty());
}

#[tokio::test]
async fn verdicts_are_taken_on_the_dream_thread_checked_and_replaced() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    assert_eq!(error(&answer(&sense, 1, json!({})).await), "no_dream");
    store(&kept, &Memory { status: Status::NeedsReview, ..lesson("Doubted") }).await;
    begin(&*items, start(Utc::now(), &on(9))).await.unwrap().unwrap();
    assert_eq!(error(&answer(&sense, 1, json!({})).await), "not_dream_thread", "not woken yet");
    woken(&*items, 1, &on(1).thread).await.unwrap();
    assert_eq!(error(&answer(&sense, 2, json!({})).await), "not_dream_thread");

    let shown = answer(&sense, 1, json!({})).await;
    assert!(shown.text.contains("- 1.1 (needs_review): Memory"), "{}", shown.text);
    let one = |verdict: serde_json::Value| json!({"verdicts": [verdict]});
    let refusals = [
        (one(json!({"finding": "1.9", "verdict": "rejected", "reason": "No."})), "not_found"),
        (one(json!({"finding": "1.1", "verdict": "applied", "reason": "Done."})), "invalid_arguments"),
        (
            one(json!({"finding": "1.1", "verdict": "applied", "reason": "Done.", "items": [ItemId::new()]})),
            "not_found",
        ),
        (one(json!({"finding": "1.1", "verdict": "rejected", "reason": " "})), "invalid_arguments"),
        (one(json!({"finding": "1.1", "verdict": "maybe", "reason": "Hm."})), "invalid_arguments"),
        (one(json!({"finding": "1.1", "verdict": "rejected", "reason": "x".repeat(501)})), "too_long"),
    ];
    for (arguments, code) in refusals {
        assert_eq!(error(&answer(&sense, 1, arguments.clone()).await), code, "{arguments}");
    }

    let deferred = answer(&sense, 1, one(json!({"finding": "1.1", "verdict": "deferred", "reason": "Later."}))).await;
    assert!(deferred.text.contains("1 verdicts recorded; every finding is answered"), "{}", deferred.text);
    let rejected = answer(&sense, 1, one(json!({"finding": "1.1", "verdict": "rejected", "reason": "Fine."}))).await;
    assert_eq!(rejected.json["verdicts"].as_array().unwrap().len(), 1, "{rejected:?}");
    assert_eq!(rejected.json["verdicts"][0]["verdict"], "rejected");
    assert!(!answer(&sense, 1, json!({"done": true})).await.is_error);
    assert_eq!(error(&answer(&sense, 1, json!({})).await), "done");
}

/// A call of tool `name` on thread `thread` with `arguments`.
fn called(name: &str, thread: u8, arguments: &serde_json::Value) -> ToolCall {
    let arguments = arguments.as_object().unwrap().clone();
    ToolCall { name: name.into(), arguments, context: on(thread), format: Format::Json }
}

fn remember(thread: u8, n: usize) -> ToolCall {
    let arguments = json!({"kind": "fact", "title": format!("Fact {n}"), "body": format!("Body {n}."), "force": true});
    called("remember", thread, &arguments)
}

#[tokio::test]
async fn writes_on_a_dream_thread_are_counted_and_refused_past_the_cap() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    begin(&*items, start(Utc::now(), &on(9))).await.unwrap().unwrap();
    woken(&*items, 1, &on(1).thread).await.unwrap();
    let json = |result: ants_proto::provider::ToolResult| match result.output {
        Output::Json(json) => (json, result.is_error),
        Output::Text(text) => panic!("{text}"),
    };
    let mut written = Vec::new();
    for n in 0..MAX_DREAM_WRITES {
        let call = match n {
            0 => called("identity", 1, &json!({"self_model": "Careful.", "title": "Me"})),
            n => remember(1, n),
        };
        let (answer, refused) = json(sense.call_tool(call).await.unwrap());
        assert!(!refused, "{answer}");
        assert_eq!(answer["dream_writes"], n + 1);
        written.push(answer["id"].as_str().unwrap().parse::<ItemId>().unwrap());
    }
    let (answer, refused) = json(sense.call_tool(remember(1, MAX_DREAM_WRITES)).await.unwrap());
    assert!(refused && answer["error"] == "dream_cap", "{answer}");
    let record = called("identity", 1, &json!({"self_model": "Bolder.", "title": "Me", "revision": 1}));
    let (answer, refused) = json(sense.call_tool(record).await.unwrap());
    assert!(refused && answer["error"] == "dream_cap", "a record is a write: {answer}");
    let (answer, refused) = json(sense.call_tool(called("identity", 1, &json!({}))).await.unwrap());
    assert!(!refused && answer["self_model"]["body"] == "Careful.", "showing the identity writes nothing: {answer}");
    let (answer, refused) = json(sense.call_tool(remember(2, MAX_DREAM_WRITES)).await.unwrap());
    assert!(!refused && answer.get("dream_writes").is_none(), "other threads write freely: {answer}");
    assert_eq!(get(&*items, 1).await.unwrap().unwrap().0.writes, written);
}

fn finding(n: usize) -> Finding {
    let text = format!("Finding number {n}, about something worth a look.");
    Finding {
        id: format!("1.{n}"),
        kind: FindingKind::NeedsReview,
        text,
        items: Vec::new(),
        untrusted: false,
        key: None,
        count: None,
        advice: None,
        late: false,
    }
}

fn thread(n: u8, bytes: usize, untrusted: bool) -> packet::Thread {
    let line = format!("message {n} of a thread\n");
    let text = line.repeat(bytes / line.len());
    let (started_by, timeline) = ("alice".into(), String::new());
    packet::Thread {
        id: on(n).thread,
        untrusted,
        review: false,
        started_by,
        text,
        timeline,
        actions: Vec::new(),
        signs: Signs::default(),
    }
}

/// The first dream, covering threads 1 to `threads` (2 untrusted), with `findings` findings.
fn reviewing(threads: u8, findings: usize) -> Dream {
    let now = Utc::now();
    Dream {
        number: 1,
        at: now,
        since: now - Duration::hours(24),
        state: State::Fallback,
        sage: None,
        review: None,
        thread: None,
        threads: (1..=threads).map(|n| Covered { thread: on(n).thread, untrusted: n == 2, review: false }).collect(),
        commits: vec![Commit { repo: "github.com/o/r".into(), commit: "abc123".into(), thread: Some(on(1).thread) }],
        findings: (1..=findings).map(finding).collect(),
        verdicts: Vec::new(),
        writes: Vec::new(),
        trails: Vec::new(),
        held: Vec::new(),
        diary: None,
        vitals: None,
    }
}

#[test]
fn a_packet_over_its_budget_trims_threads_to_their_latest_then_leaves_out_threads_then_findings() {
    let dream = reviewing(3, 5);
    let threads = [thread(1, 3_000, false), thread(2, 3_000, true), thread(3, 3_000, false)];
    let whole = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, PACKET_BUDGET);
    assert!(!whole.contains("left out"), "{whole}");
    assert!(whole.contains("github.com/o/r at abc123, in the worktree of thread"), "{whole}");
    assert!(whole.contains(&format!("### Thread {}, started by alice (untrusted", on(2).thread)), "{whole}");

    let budget = whole.len() - 2_000;
    let trimmed = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, budget);
    assert!(trimmed.len() <= budget && trimmed.len() > budget - 100, "{} of {budget}", trimmed.len());
    assert_eq!(trimmed.matches("earlier bytes left out for room]").count(), 3, "{trimmed}");
    assert!(trimmed.ends_with("message 3 of a thread\n"), "the latest kept");

    let budget = whole.len() - 6_400;
    let dropped = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, budget);
    assert!(dropped.len() <= budget, "{} of {budget}", dropped.len());
    let left_out = format!("Left out for room, the least recently active: threads {}.", on(3).thread);
    assert!(dropped.contains(&left_out) && dropped.contains("message 2 of a thread"), "{dropped}");

    let least = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, 0);
    assert!(least.contains("5 more findings left out") && !least.contains("message 1"), "{least}");
    let line = format!("{}\n", packet::finding_line(&finding(1), 1));
    let budget = least.len() + 3 * line.len() + 10;
    let cut = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, budget);
    assert!(cut.len() <= budget && !cut.contains("message 1"), "{cut}");
    assert!(cut.contains("Finding number 3") && !cut.contains("Finding number 4"), "{cut}");
    assert!(cut.contains("2 more findings left out for room: the next dream offers them again."), "{cut}");
}

#[test]
fn over_its_budget_the_verdicts_on_the_last_dream_shrink_to_their_tally_before_the_reviews_made() {
    let dream = Dream { number: 2, ..reviewing(3, 5) };
    let threads = [thread(1, 3_000, false), thread(2, 3_000, true), thread(3, 3_000, false)];
    let identity = Identity {
        whole: format!("\n### Persona\n\n{}\n", "Voice: plain.\n".repeat(300)),
        titles: "\nCut to titles for room.\n\n### Persona\n\nFields: Voice.\n".into(),
    };
    let last = |findings: usize, bytes: usize| {
        let at = Utc::now();
        let verdict = |finding: &Finding| {
            let reason = "x".repeat(bytes);
            Verdict {
                finding: finding.id.clone(),
                verdict: Judgement::Applied,
                reason,
                items: Vec::new(),
                trails: Vec::new(),
                at,
            }
        };
        let mut last = reviewing(0, findings);
        last.verdicts = last.findings.iter().take(findings - 1).map(verdict).collect();
        last
    };
    let tally = |findings: usize| {
        format!("\nDream 1: {findings} findings, {} applied, 0 rejected, 0 deferred, 1 unanswered.\n", findings - 1)
    };

    let long = last(30, 400);
    let shared = compose(&dream, Earlier::Last(Some(&long)), &[], &ant(), &identity, &threads, PACKET_BUDGET);
    assert!(shared.contains("- 1.18 (needs_review): applied") && !shared.contains("- 1.19 ("), "an eighth: {shared}");
    assert!(shared.contains("\n12 verdicts left out for room.\n") && shared.contains(&tally(30)), "{shared}");
    assert!(shared.contains("message 3") && shared.contains(&identity.whole), "{shared}");

    let previous = last(10, 30);
    let reviews = Section {
        head: "\n## Reviews made\n\n".into(),
        entries: vec![format!("- 1 of dev-b: {}\n", "y".repeat(100))],
        tally: "\n1 review in all.\n".into(),
        noun: "earlier review",
    };
    let earlier = Earlier::Reviewing(Some(&previous), &reviews);
    let within = |budget| compose(&dream, earlier, &[], &ant(), &identity, &threads, budget);
    let whole = within(PACKET_BUDGET);
    assert!(whole.contains("- 1.10 (needs_review): unanswered\n") && !whole.contains("left out for room."), "{whole}");
    assert!(whole.find("## Verdicts on dream 1").unwrap() < whole.find("## Reviews made").unwrap(), "{whole}");

    let bare = within(whole.len() - 8_500);
    assert!(!bare.contains(" of a thread") && bare.contains("- 1.10 (needs_review)"), "{bare}");

    let shrunk = within(bare.len() - 1);
    assert!(shrunk.contains("- 1.9 (needs_review)") && !shrunk.contains("- 1.10 ("), "{shrunk}");
    assert!(shrunk.contains("\n1 verdict left out for room.\n") && shrunk.contains("- 1 of dev-b:"), "{shrunk}");

    let left_out = "\n10 verdicts left out for room.\n";
    let listed = whole.find("- 1.1 (").unwrap()..whole.find(&tally(10)).unwrap();
    let tallied = within(bare.len() - listed.len() + left_out.len());
    assert!(tallied.contains(left_out) && tallied.contains(&tally(10)), "{tallied}");
    assert!(tallied.contains("- 1 of dev-b:") && tallied.contains(&identity.whole), "{tallied}");

    let fewer = within(tallied.len() - 1);
    assert!(fewer.contains("1 earlier review left out for room.") && fewer.contains(&identity.whole), "{fewer}");
}

#[test]
fn a_thread_shows_its_timeline_after_its_history_the_latest_of_both_kept_for_room() {
    let dream = reviewing(2, 0);
    let lines =
        |what: &str, n: u8, count: usize| (0..count).map(|i| format!("{what} {i:03} of thread {n}\n")).collect();
    let threads: Vec<_> = (1..=2)
        .map(|n| packet::Thread {
            text: lines("message", n, 100),
            timeline: lines("call", n, 100),
            ..thread(n, 0, n == 2)
        })
        .collect();
    let long = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, PACKET_BUDGET);
    assert!(!long.contains("left out"), "{long}");
    let untrusted = format!("### Thread {}, started by alice (untrusted", on(2).thread);
    let (first, second) = long.split_once(&untrusted).unwrap();
    for (section, n) in [(first, 1), (second, 2)] {
        let (history, timeline) = section.split_once("\n#### Timeline\n\n").unwrap();
        assert!(history.contains(&format!("message 099 of thread {n}")) && !history.contains("call 0"), "{section}");
        assert!(timeline.starts_with(&format!("call 000 of thread {n}\n")), "{section}");
        assert!(timeline.contains(&format!("call 099 of thread {n}")), "{section}");
    }
    let bare = packet::Thread { timeline: String::new(), ..threads[0].clone() };
    assert!(
        !compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &[bare], PACKET_BUDGET)
            .contains("#### Timeline"),
        "no section without one"
    );

    // A shorter timeline stays whole while the history gives way.
    let short: Vec<_> =
        threads.iter().map(|t| packet::Thread { timeline: lines("call", 0, 30), ..t.clone() }).collect();
    let whole = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &short, PACKET_BUDGET);
    let budget = whole.len() - 2_000;
    let trimmed = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &short, budget);
    assert!(trimmed.len() <= budget, "{} of {budget}", trimmed.len());
    assert_eq!(trimmed.matches("earlier bytes left out for room]\nmessage").count(), 2, "{trimmed}");
    assert_eq!(trimmed.matches("call 000 of thread 0").count(), 2, "{trimmed}");

    // Both long, each keeps its latest half.
    let budget = long.len() - 6_000;
    let trimmed = compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, budget);
    assert!(trimmed.len() <= budget, "{} of {budget}", trimmed.len());
    assert_eq!(trimmed.matches("earlier bytes left out for room]").count(), 4, "{trimmed}");
    assert!(!trimmed.contains("call 000 of thread 1") && !trimmed.contains("message 000 of thread 1"), "{trimmed}");
    assert!(trimmed.contains("message 099 of thread 1\n\n#### Timeline"), "{trimmed}");
    assert!(trimmed.ends_with("call 099 of thread 2\n"), "the latest kept: {trimmed}");
}

#[test]
fn a_thread_shows_what_was_done_with_it_besides_writing_before_its_history() {
    let dream = reviewing(1, 0);
    let at = |minute: i64| DateTime::from_timestamp(1_794_646_800 + minute * 60, 0).unwrap();
    let message = ants_proto::Uuid::from_u128(7);
    let actions = vec![
        Action::Rejected { at: at(5), tool: "execute: rm -rf target".into(), by: Some("alice".into()) },
        Action::Rejected { at: at(20), tool: "fetch: example.com".into(), by: None },
        Action::Reaction { reaction: "thumbsup".into(), by: "bob".into(), message, opening: "fixed the build".into() },
        Action::Done { at: at(30), by: "alice via asst".into() },
        Action::Reopened { at: at(40), by: "a channel post from bob".into() },
    ];
    let threads = [
        packet::Thread { actions, ..thread(1, 200, false) },
        packet::Thread { actions: vec![Action::Undone { at: at(45) }], ..thread(2, 200, false) },
    ];
    for reader in [ant(), Reader::Sage(on(1).ant)] {
        let packet = compose(&dream, Earlier::Last(None), &[], &reader, &Identity::default(), &threads, PACKET_BUDGET);
        let lines = format!(
            "### Thread {}, started by alice\n\n\
             - A tool request rejected by alice at 2026-11-14 09:05 UTC: execute: rm -rf target.\n\
             - A tool request expired unanswered at 2026-11-14 09:20 UTC, so rejected: fetch: example.com.\n\
             - bob reacted thumbsup to message {message}, \"fixed the build\".\n\
             - Marked done by alice via asst at 2026-11-14 09:30 UTC.\n\
             - Reopened by a channel post from bob at 2026-11-14 09:40 UTC, so no longer done.\n\n\
             message 1 of a thread\n",
            on(1).thread
        );
        assert!(packet.contains(&lines), "{packet}");
        assert!(packet.contains("- Explicitly marked undone at 2026-11-14 09:45 UTC."), "{packet}");
    }
    let bare = compose(
        &dream,
        Earlier::Last(None),
        &[],
        &ant(),
        &Identity::default(),
        &[thread(1, 200, false)],
        PACKET_BUDGET,
    );
    assert!(bare.contains("started by alice\n\nmessage 1 of a thread\n"), "no lines without actions: {bare}");
    assert!(!bare.contains("marked undone"), "{bare}");
}

/// The ids of the open findings resting on untrusted text of the last dream when `thread` is its dream thread.
async fn untrusted_on(items: &dyn Items, thread: &ThreadId) -> Result<Vec<String>, StoreError> {
    let Some((dream, _)) = last(items).await? else { return Ok(Vec::new()) };
    if dream.thread.as_ref() != Some(thread) {
        return Ok(Vec::new());
    }
    Ok(dream.untrusted_open().map(|finding| finding.id.clone()).collect())
}

mod colony_trails;
mod identity_findings;
mod identity_section;
mod review;
mod reviews;
mod sage_dream;
mod track_record;
mod trail_findings;
mod vitals;
