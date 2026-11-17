use std::sync::Arc;

use ants_proto::provider::{
    BriefReason, BriefRequest, Event, EventKind, Format, Origin, Output, Provider, Starter, ToolResult,
};
use ants_proto::{SenseId, UserId};
use serde_json::{Value, json};
use uuid::Uuid;

use super::*;
use crate::cache::Cache;
use crate::log::{AccountVerifier, AntLog, EventLog, MemoryLog, NoRevocations, Unsigned};
use crate::memory::{NOTE, RECALLED_BEGIN, RECALLED_END};
use crate::store::{Store, Stores};
use crate::tools::Handler;

/// The items of one ant, the same at each call.
struct One(Arc<dyn Items>);

impl Stores for One {
    fn items<'a>(&'a self, _ant: &'a UserId) -> BoxFuture<'a, Result<Arc<dyn Items>, StoreError>> {
        let items = self.0.clone();
        Box::pin(async move { Ok(items) })
    }
}

/// sense keeping the items of its one ant in `log`.
async fn sense_on(log: MemoryLog, sense: SenseId) -> (Sense, Arc<dyn Items>) {
    let verifier = AccountVerifier::new(vec![], NoRevocations);
    let log = AntLog::new(log, Unsigned, verifier, sense, "dev-a".parse().unwrap(), "m1".parse().unwrap(), None);
    let store = Store::open(log.unwrap(), Cache::in_memory().unwrap()).await.unwrap();
    let items: Arc<dyn Items> = Arc::new(tokio::sync::Mutex::new(store));
    (Sense::with_stores(Arc::new(One(items.clone()))), items)
}

async fn sense() -> (Sense, Arc<dyn Items>) {
    sense_on(MemoryLog::default(), SenseId::new()).await
}

const THREAD: &str = "019a7d2c-0000-7000-8000-000000000001";
const OTHER: &str = "019a7d2c-0000-7000-8000-000000000002";

fn context(thread: &str) -> CallContext {
    CallContext {
        ant: "dev-a".parse().unwrap(),
        identity: "0f1e".into(),
        thread: thread.parse().unwrap(),
        origin: Origin { by: Starter::Human { user: "alice".parse().unwrap() }, channel: None },
        harness: "copilot".into(),
        model: Some("gpt".into()),
        machine: "studio".into(),
        at: "2025-01-01T12:00:00Z".parse().unwrap(),
    }
}

/// The answer of `handler` to a call on `thread` with `arguments`, which must not be refused.
async fn call(sense: &Sense, handler: Handler, thread: &str, arguments: Value) -> Answer {
    let arguments = arguments.as_object().unwrap().clone();
    let call = ToolCall { name: "tool".into(), arguments, context: context(thread), format: Format::Json };
    let answer = handler(sense, &call).await;
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
async fn an_empty_ant_is_told_to_start_a_register_and_the_tool_shows_the_brief_again() {
    let (sense, _) = sense().await;
    let brief = session_brief(&sense, 8_000).await;
    position(&brief, "No resume register yet");
    assert!(!brief.contains("## Memories") && !brief.contains("handoffs"), "{brief}");
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
    let hint = "… more: sense_checkpoint without a revision is refused with the whole register";
    assert!(this.trim_end().ends_with(hint), "{this}");
    position(&brief, "- Lexer");
    position(&brief, "- sense_brief: ");
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
