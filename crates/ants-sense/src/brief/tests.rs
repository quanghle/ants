//! Tests of the brief: section order, the thread's repository memories, an empty ant, cache syncs and log failures,
//! cuts within the budget, handoffs, returns and revivals; with fixtures for the submodules.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ants_proto::SenseId;
use ants_proto::provider::{
    BriefReason, BriefRequest, Event, EventKind, Format, Origin, Output, Provider, Starter, ToolResult,
};
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::log::{
    AccountVerifier, AntLog, AppendError, EventLog, LogError, MemoryLog, NoRevocations, Stored, Unsigned,
};
use crate::memory::{NOTE, RECALLED_BEGIN, RECALLED_END};
use crate::testing::{self, One, human, sense, store};
use crate::tools::Handler;

mod citations;
mod cuts;
mod experience;
mod persona;
mod records;
mod work;

/// sense keeping the items of its one ant in `log`.
async fn sense_on(log: impl EventLog + Send + Sync + 'static, sense: SenseId) -> (Sense, Arc<dyn Items>) {
    let items: Arc<dyn Items> = Arc::new(store(log, sense, "dev-a".parse().unwrap()).await);
    (Sense::with_stores(Arc::new(One::new(items.clone()))), items)
}

const THREAD: &str = "019a7d2c-0000-7000-8000-000000000001";
const OTHER: &str = "019a7d2c-0000-7000-8000-000000000002";
const THIRD: &str = "019a7d2c-0000-7000-8000-000000000003";

fn context(thread: &str) -> CallContext {
    CallContext { thread: thread.parse().unwrap(), ..testing::context(human()) }
}

/// The answer of `handler` to a call on `thread` with `arguments`, which must not be refused.
async fn call(sense: &Sense, handler: Handler, thread: &str, arguments: Value) -> Answer {
    let answer = handler(sense, &testing::call("tool", arguments, context(thread))).await;
    assert!(!answer.is_error, "{answer:?}");
    answer
}

/// The brief of a session of the thread, within `budget`.
async fn session_brief(sense: &Sense, budget: u32) -> String {
    let request = BriefRequest { context: context(THREAD), budget, reason: BriefReason::Started };
    sense.brief(request).await.unwrap().text
}

/// sense with every section of a brief: a register, a handoff of another thread, a core memory and another memory.
async fn filled() -> (Sense, Arc<dyn Items>) {
    let (sense, items) = sense().await;
    let register = json!({"goal": "Ship the parser", "next": "Write the tests", "blockers": ["CI is red"]});
    call(&sense, thread::checkpoint, THREAD, register).await;
    call(&sense, thread::handoff, OTHER, json!({"done": ["Lexer"], "next": ["Parser"]})).await;
    let core = json!({"kind": "preference", "title": "Short commits", "body": "Keep each commit small.", "core": true});
    call(&sense, memory::remember, THREAD, core).await;
    let fact = json!({"kind": "fact", "title": "Tokens are spans", "body": "The lexer yields byte spans."});
    call(&sense, memory::remember, THREAD, fact).await;
    (sense, items)
}

/// Thread records of the ant of the briefs: two threads alice started in github.com/o/r, one of them done, and one
/// ant dev-b started, on two days.
async fn worked(sense: &Sense) {
    let alice = || Starter::Human { user: "alice".parse().unwrap() };
    let started = |by, repo: Option<&str>| EventKind::ThreadStarted {
        origin: Origin { by, channel: None },
        repo: repo.map(Into::into),
    };
    let told = [
        (THREAD, "2025-01-01T09:00:00Z", started(alice(), Some("github.com/o/r"))),
        (THREAD, "2025-01-01T11:00:00Z", EventKind::ThreadDone),
        (OTHER, "2025-01-02T09:00:00Z", started(alice(), Some("github.com/o/r"))),
        (THIRD, "2025-01-02T10:00:00Z", started(Starter::Ant { user: "dev-b".parse().unwrap() }, None)),
    ];
    for (thread, at, kind) in told {
        sense.event(Event { thread: Some(thread.parse().unwrap()), ..lifecycle(kind, at) }).await.unwrap();
    }
}

fn position(brief: &str, text: &str) -> usize {
    brief.find(text).unwrap_or_else(|| panic!("{text:?} not in:\n{brief}"))
}

#[tokio::test]
async fn sections_come_in_order_with_memories_framed() {
    let (sense, _) = filled().await;
    let brief = session_brief(&sense, 8_000).await;
    let headings = ["## Identity", "## This thread", "## Unacknowledged handoffs", "## Memories", "## Core loop"];
    let at: Vec<_> = headings.iter().map(|heading| position(&brief, heading)).collect();
    assert!(at.is_sorted(), "{brief}");
    assert!(!brief.contains(MORE) && !brief.starts_with("Warning"), "{brief}");
    position(&brief, "dev-a (sense identity ");
    position(&brief, "Harnesses: copilot.");
    position(&brief, "Goal: Ship the parser");
    position(&brief, "- Lexer");
    let memories = &brief[at[3]..at[4]];
    let begin = position(memories, RECALLED_BEGIN);
    let note = position(memories, NOTE);
    assert!(begin < note && note < position(memories, RECALLED_END), "{memories}");
    assert!(position(memories, "Core:") < position(memories, "Keep each commit small."), "{memories}");
    assert!(position(memories, "Top:") < position(memories, "Tokens are spans"), "{memories}");
    assert_eq!(memories.matches("Short commits").count(), 1, "a core memory is not listed again: {memories}");
    assert!(!memories.contains("(episode"), "the episode of a handoff shown is not listed again: {memories}");
    for (tool, _) in CORE_LOOP {
        assert!(crate::TOOLS.iter().any(|spec| spec.name == *tool), "{tool}");
        position(&brief[at[4]..], &format!("- sense_{tool}: "));
    }
}

#[tokio::test]
async fn the_memories_include_those_of_the_thread_s_repository() {
    let (sense, _) = sense().await;
    for (scope, title) in [("repo:github.com/o/r", "Run make check"), ("repo:github.com/o/other", "Run npm test")] {
        let fact = json!({"kind": "fact", "scope": scope, "title": title, "body": title});
        call(&sense, memory::remember, THREAD, fact).await;
    }
    let context = CallContext { repo: Some("github.com/o/r".into()), ..context(THREAD) };
    let request = BriefRequest { context, budget: 8_000, reason: BriefReason::Started };
    let brief = sense.brief(request).await.unwrap().text;
    position(&brief[position(&brief, "## Memories")..], "Run make check");
    assert!(!brief.contains("Run npm test"), "{brief}");
    assert!(!session_brief(&sense, 8_000).await.contains("Run make check"), "none outside the repository");
}

#[tokio::test]
async fn an_empty_ant_is_told_to_start_a_register_and_the_tool_shows_the_brief_again() {
    let (sense, _) = sense().await;
    let brief = session_brief(&sense, 8_000).await;
    position(&brief, "No resume register yet");
    assert!(!brief.contains("## Memories") && !brief.contains("handoffs"), "{brief}");
    for heading in
        ["## Open commitments", "## Fired reminders", "## Upcoming reminders", "## Claimed tasks", "## Ready tasks"]
    {
        assert!(!brief.contains(heading), "{brief}");
    }
    let (sense, _) = filled().await;
    let brief = session_brief(&sense, 8_000).await;
    let call = ToolCall {
        name: "brief".into(),
        arguments: serde_json::Map::new(),
        context: context(THREAD),
        format: Format::Text,
    };
    let ToolResult { output, is_error } = sense.call_tool(call).await.unwrap();
    assert!(!is_error);
    assert_eq!(output, Output::Text(brief));
}

#[tokio::test]
async fn a_call_writing_wakes_the_schedule_of_its_ant_alone_and_one_reading_none() {
    let (sense, _) = sense().await;
    let mut changes = sense.changes(&"dev-a".parse().unwrap());
    let others = sense.changes(&"dev-b".parse().unwrap());
    let tool = |name: &str, arguments: Value| ToolCall {
        name: name.into(),
        arguments: arguments.as_object().unwrap().clone(),
        context: context(THREAD),
        format: Format::Json,
    };
    let remember = tool("remember", json!({"kind":"fact","title":"Parser spans","body":"Tokens carry byte spans."}));
    assert!(!sense.call_tool(tool("brief", json!({}))).await.unwrap().is_error);
    assert!(!changes.has_changed().unwrap(), "a read wakes no schedule");
    assert!(!sense.call_tool(remember.clone()).await.unwrap().is_error);
    assert!(changes.has_changed().unwrap());
    assert!(!others.has_changed().unwrap(), "the write was of dev-a's items");
    changes.borrow_and_update();
    assert!(sense.call_tool(remember).await.unwrap().is_error, "duplicate memory");
    assert!(!changes.has_changed().unwrap(), "refused calls do not change the schedule");
}

/// A log counting its reads, one each time a store caught up with it catches up again, and failing them with
/// `failure` when one is set.
#[derive(Default)]
struct Counting {
    log: MemoryLog,
    reads: Arc<AtomicUsize>,
    failure: Arc<std::sync::Mutex<Option<LogError>>>,
}

impl EventLog for Counting {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.log.append(sense, payload, expected_last).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(failure) = self.failure.lock().unwrap().clone() {
            return Err(failure);
        }
        self.log.read_after(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.log.last_sequence(sense).await
    }

    fn incarnation(&self) -> &str {
        self.log.incarnation()
    }
}

#[tokio::test]
async fn a_brief_syncs_the_log_once_and_serves_its_reads_from_the_cache() {
    let reads = Arc::new(AtomicUsize::new(0));
    let (sense, _) = sense_on(Counting { reads: reads.clone(), ..Counting::default() }, SenseId::new()).await;
    call(&sense, thread::checkpoint, THREAD, json!({"goal": "Ship the parser", "next": "Write the tests"})).await;
    call(&sense, thread::handoff, OTHER, json!({"done": ["Lexer"], "next": ["Parser"]})).await;
    let core = json!({"kind": "preference", "title": "Short commits", "body": "Keep each commit small.", "core": true});
    call(&sense, memory::remember, THREAD, core).await;
    session_brief(&sense, 8_000).await;
    let before = reads.load(Ordering::SeqCst);
    let brief = session_brief(&sense, 8_000).await;
    assert!(brief.contains("Ship the parser") && brief.contains("Short commits"), "{brief}");
    assert_eq!(reads.load(Ordering::SeqCst) - before, 1);
    let call = ToolCall {
        name: "brief".into(),
        arguments: serde_json::Map::new(),
        context: context(THREAD),
        format: Format::Text,
    };
    assert!(!sense.call_tool(call).await.unwrap().is_error);
    assert_eq!(reads.load(Ordering::SeqCst) - before, 2, "a tool call syncs once too");
}

#[tokio::test]
async fn a_log_failing_other_than_out_of_reach_fails_the_calls_reading_the_cache() {
    let failure = Arc::new(std::sync::Mutex::new(None));
    let (sense, _) = sense_on(Counting { failure: failure.clone(), ..Counting::default() }, SenseId::new()).await;
    call(&sense, thread::checkpoint, THREAD, json!({"goal": "Ship the parser", "next": "Write the tests"})).await;
    let recall = || ToolCall {
        name: "recall".into(),
        arguments: json!({"query": "parser"}).as_object().unwrap().clone(),
        context: context(THREAD),
        format: Format::Text,
    };
    let brief = || sense.brief(BriefRequest { context: context(THREAD), budget: 8_000, reason: BriefReason::Started });
    *failure.lock().unwrap() = Some(LogError::unreachable("hub timed out"));
    assert!(brief().await.unwrap().text.contains("Ship the parser"), "offline, the cache serves");
    assert!(!sense.call_tool(recall()).await.unwrap().is_error);
    *failure.lock().unwrap() = Some(LogError::refused("permission denied"));
    let error = brief().await.unwrap_err();
    assert!(error.message.contains("permission denied"), "{error:?}");
    let answer = sense.call_tool(recall()).await.unwrap();
    assert!(answer.is_error && matches!(&answer.output, Output::Text(text) if text.contains("permission denied")));
}

#[tokio::test]
async fn memories_shorten_to_their_titles_before_any_section_is_cut() {
    let (sense, _) = filled().await;
    let long = json!({"kind": "lesson", "title": "Long lesson", "body": "word ".repeat(19), "core": true});
    call(&sense, memory::remember, THREAD, long).await;
    let full = session_brief(&sense, 8_000).await;
    let brief = session_brief(&sense, u32::try_from(full.len()).unwrap() - 1).await;
    assert!(brief.len() < full.len());
    assert!(!brief.contains("Keep each commit small.") && !brief.contains("byte spans"), "{brief}");
    position(&brief, "): Long lesson");
    position(&brief, "): Short commits");
    let memories = &brief[position(&brief, "## Memories")..position(&brief, "## Core loop")];
    position(memories, RECALLED_END);
    assert_eq!(brief.matches(MORE).count(), 1, "{brief}");
    position(memories, "… more: sense_recall, or sense_recall id=<id> for one in full");
}

#[tokio::test]
async fn the_longest_section_is_cut_naming_the_tool_call_showing_the_rest() {
    let (sense, _) = filled().await;
    let blockers: Vec<_> = (0..20).map(|i| format!("Blocker {i}: {}", "wait ".repeat(30))).collect();
    let register = json!({"goal": "Ship the parser", "blockers": blockers, "revision": 1});
    call(&sense, thread::checkpoint, THREAD, register).await;
    let full = session_brief(&sense, 8_000).await;
    let budget = full.len() - 1_000;
    let brief = session_brief(&sense, u32::try_from(budget).unwrap()).await;
    assert!(brief.len() <= budget, "{} > {budget}", brief.len());
    let this = &brief[position(&brief, "## This thread")..position(&brief, "## Unacknowledged")];
    position(this, "Goal: Ship the parser");
    let hint = "… more: sense_checkpoint without arguments";
    assert!(this.trim_end().ends_with(hint), "{this}");
    position(&brief, "- Lexer");
    position(&brief, "- sense_brief: ");

    let read = call(&sense, thread::checkpoint, THREAD, json!({})).await;
    assert_eq!(read.json["register"]["blockers"].as_array().unwrap().len(), 20, "the whole register");
    assert!(read.text.contains(blockers[19].trim_end()), "{}", read.text);
    let after = session_brief(&sense, u32::try_from(budget).unwrap()).await;
    assert_eq!(after, brief, "reading changes nothing");
}

#[tokio::test]
async fn handoffs_past_the_brief_s_are_named_with_the_tool_call_showing_them_all() {
    let (sense, _) = filled().await;
    let mut episodes = Vec::new();
    for i in 0..MAX_HANDOFFS {
        let answer = call(&sense, thread::handoff, OTHER, json!({"next": [format!("Step {i}")]})).await;
        episodes.push(answer.json["handoff"]["episode"].as_str().unwrap().to_owned());
    }
    let brief = session_brief(&sense, 8_000).await;
    let handoffs = &brief[position(&brief, "## Unacknowledged")..position(&brief, "## Memories")];
    assert!(!handoffs.contains("- Lexer"), "the oldest is past the brief's: {handoffs}");
    assert!(handoffs.trim_end().ends_with("… more: sense_checkpoint without arguments"), "{handoffs}");

    let read = call(&sense, thread::checkpoint, THREAD, json!({})).await;
    let shown = read.json["handoffs"].as_array().unwrap();
    assert_eq!(shown.len(), MAX_HANDOFFS + 1);
    position(&read.text, "- Lexer");
    position(&read.text, "Goal: Ship the parser");
    for episode in &episodes {
        position(&read.text, &format!("Episode memory: {episode}"));
    }
    let first = session_brief(&sense, 8_000).await;
    assert_eq!(first, brief, "reading acknowledges nothing");
}

#[test]
fn fitting_cuts_the_longest_section_down_to_the_next_then_both() {
    let sections = || {
        vec![
            Section::whole("# whole".into(), "w".repeat(50)),
            Section::cut_with("# long", "l\n".repeat(500), "long_more"),
            Section::cut_with("# mid", "m\n".repeat(200), "mid_more"),
        ]
    };
    let lengths: Vec<_> = sections().iter().map(|section| section.render().len()).collect();
    let fitted = fit(sections(), lengths.iter().sum::<usize>() + 4 - 300);
    assert!(fitted.contains("… more: long_more") && !fitted.contains("mid_more"), "{fitted}");
    let fitted = fit(sections(), 600);
    assert!(fitted.len() <= 600 && fitted.contains("long_more") && fitted.contains("mid_more"), "{fitted}");
    assert!(fitted.starts_with(&format!("# whole\n{}", "w".repeat(50))));
}

#[tokio::test]
async fn no_budget_is_ever_exceeded_even_with_multibyte_text() {
    let (sense, items) = filled().await;
    let blockers: Vec<_> = (0..10).map(|i| format!("{i}é{}", "日本語🦀".repeat(40))).collect();
    let register = json!({"goal": "é".repeat(300), "blockers": blockers, "revision": 1});
    call(&sense, thread::checkpoint, THREAD, register).await;
    let body = "Ünïcödé 🦀 ".repeat(40);
    call(&sense, memory::remember, THREAD, json!({"kind": "gotcha", "title": "Crabs 🦀", "body": body})).await;
    worked(&sense).await;
    let card = identity::card(&*items).await.unwrap();
    for budget in (0..4_000).step_by(97).chain([8_000]) {
        let brief = compose(&*items, &context(THREAD), card.as_ref(), budget).await.unwrap();
        assert!(brief.len() <= budget, "{} > {budget}", brief.len());
        if budget >= 1_500 {
            position(&brief, "## Core loop");
            assert_eq!(brief.matches(RECALLED_BEGIN).count(), brief.matches(RECALLED_END).count(), "{brief}");
        }
    }
}

#[tokio::test]
async fn a_return_is_told_once() {
    let (sense, items) = sense().await;
    session_brief(&sense, 8_000).await;
    let added = Event {
        id: Uuid::now_v7(),
        ant: "dev-a".parse().unwrap(),
        identity: "a1b2".into(),
        thread: None,
        at: "2025-02-01T12:00:00Z".parse().unwrap(),
        kind: EventKind::AntAdded,
    };
    sense.event(added).await.unwrap();
    assert!(identity::card(&*items).await.unwrap().unwrap().returned());
    let brief = session_brief(&sense, 8_000).await;
    position(&brief, "Returned 2025-02-01: added again under a new ants identity");
    assert!(!identity::card(&*items).await.unwrap().unwrap().returned());
    assert!(!session_brief(&sense, 8_000).await.contains(RETURNED));
}

/// A lifecycle event of the ant of the briefs at `at`.
fn lifecycle(kind: EventKind, at: &str) -> Event {
    Event {
        id: Uuid::now_v7(),
        ant: "dev-a".parse().unwrap(),
        identity: "0f1e".into(),
        thread: None,
        at: at.parse().unwrap(),
        kind,
    }
}

/// A dormant ant's brief says so; once revived, its next brief tells of its dormancy, its last handoff, acknowledged
/// before it retired, and its overdue commitment, once.
#[tokio::test]
async fn a_revival_is_told_with_the_last_handoff_and_the_open_commitments() {
    let (sense, _) = sense().await;
    session_brief(&sense, 8_000).await;
    call(&sense, thread::handoff, OTHER, json!({"done": ["Lexer"], "next": ["Parser"]})).await;
    call(&sense, thread::checkpoint, OTHER, json!({"goal": "Ship the parser"})).await;
    let due = "2025-01-01T10:00:00Z";
    call(&sense, promise::promise, OTHER, json!({"title": "Deliver the parser", "promisee": "alice", "due": due}))
        .await;
    sense.event(lifecycle(EventKind::AntRetired, "2025-01-02T12:00:00Z")).await.unwrap();
    let dormant = session_brief(&sense, 8_000).await;
    position(
        &dormant,
        "Dormant since 2025-01-02: retired, its reminders paused and its commitments open until revived.",
    );
    assert!(!dormant.contains("## Last handoff") && !dormant.contains("## Unacknowledged handoffs"), "{dormant}");

    sense.event(lifecycle(EventKind::AntRevived, "2025-02-01T12:00:00Z")).await.unwrap();
    let brief = session_brief(&sense, 8_000).await;
    position(&brief, "Returned 2025-02-01, dormant since 2025-01-02: back after a retirement.");
    assert!(!brief.contains("Dormant since"), "{brief}");
    let last = position(&brief, "## Last handoff");
    assert!(last < position(&brief, "- Lexer") && last < position(&brief, "## Open commitments"), "{brief}");
    position(&brief, "Deliver the parser — to alice, due 2025-01-01T10:00:00Z");
    position(&brief, ", overdue)");
    let next = session_brief(&sense, 8_000).await;
    assert!(!next.contains(RETURNED) && !next.contains("## Last handoff") && !next.contains("dormant"), "{next}");
}

#[tokio::test]
async fn a_broken_log_is_warned_of_first() {
    let (log, id) = (MemoryLog::default(), SenseId::new());
    log.append(&id, b"not an event".to_vec(), 0).await.unwrap();
    let (sense, _) = sense_on(log, id).await;
    let brief = session_brief(&sense, 8_000).await;
    let first = brief.lines().next().unwrap();
    assert!(first.starts_with("Warning: this ant's sense log breaks at event 1 (not an event"), "{brief}");
    assert!(first.ends_with("`ants doctor` reports the break."), "{first}");
}

#[tokio::test]
async fn a_log_over_its_size_limit_is_warned_of() {
    assert!(size_warning(SIZE_LIMIT).is_none());
    let line = size_warning(SIZE_LIMIT + 1).unwrap().render();
    assert!(line.starts_with("Warning: this ant's sense log holds 32.0 MiB (33554433 bytes), over the 32 MiB limit"));

    let (log, id) = (MemoryLog::default(), SenseId::new());
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let ant = "dev-a".parse().unwrap();
    let mut writer = AntLog::new(&log, Unsigned, verifier, id.clone(), ant, "m1".parse().unwrap(), None).unwrap();
    writer.append("padding", json!("a".repeat(usize::try_from(SIZE_LIMIT).unwrap()))).await.unwrap();
    let (sense, _) = sense_on(log, id).await;
    let brief = session_brief(&sense, 8_000).await;
    let first = brief.lines().next().unwrap();
    assert!(first.starts_with("Warning: this ant's sense log holds 32.0 MiB ("), "{first}");
    assert!(first.ends_with("`ants doctor` reports the size."), "{first}");
}
