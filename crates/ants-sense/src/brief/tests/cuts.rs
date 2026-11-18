use super::*;
use crate::items::Values;
use crate::persona::{FIELDS, MAX_FIELD_BYTES, PERSONA, persona_id};
use crate::store::Write;

/// sense with long work sections, records of the ant and of alice, who started the thread, a full persona, and the
/// experience of threads.
async fn crowded() -> (Sense, Arc<dyn Items>) {
    let (sense, items) = filled().await;
    worked(&sense).await;
    let blockers: Vec<_> = (0..10).map(|i| format!("Blocker {i}: {}", "wait ".repeat(30))).collect();
    let register = json!({"goal": "Ship the parser", "blockers": blockers, "revision": 1});
    call(&sense, thread::checkpoint, THREAD, register).await;
    for i in 0..15 {
        let title = format!("Ready task {i}: {}", "polish ".repeat(12));
        call(&sense, task::task, THREAD, json!({"action": "create", "title": title})).await;
    }
    let commitment = json!({"title": "Send the notes", "promisee": "alice", "due": "2025-01-03T12:00:00Z"});
    call(&sense, promise::promise, THREAD, commitment).await;
    let model = json!({"self_model": "careful ".repeat(120), "title": "Knows parsers"});
    call(&sense, identity::identity, THREAD, model).await;
    let relationship =
        json!({"relationship": "patient ".repeat(100), "who": "human:alice", "title": "Works with Alice"});
    call(&sense, identity::identity, THREAD, relationship).await;
    let mut persona = Values::new();
    for (field, _) in FIELDS {
        let value = format!("{field} {}", "steady ".repeat(70));
        persona.insert((*field).into(), json!(value[..value.floor_char_boundary(MAX_FIELD_BYTES)].trim_end()));
    }
    items.write(&[Write::Create { id: persona_id(), kind: PERSONA.into(), values: persona }], "test").await.unwrap();
    (sense, items)
}

/// The section of `brief` under `heading`, up to the next.
fn section<'a>(brief: &'a str, heading: &str) -> &'a str {
    let start = position(brief, heading);
    let end = brief[start..].find("\n\n## ").map_or(brief.len(), |end| start + end);
    &brief[start..end]
}

#[tokio::test]
async fn over_its_budget_the_brief_cuts_the_work_and_keeps_a_full_persona_and_the_records_whole() {
    let (sense, _) = crowded().await;
    let full = session_brief(&sense, 64_000).await;
    assert!(full.len() > 8_000, "{}", full.len());
    let persona = section(&full, "## Persona");
    assert!(persona.len() > 4 * (MAX_FIELD_BYTES - 10), "{persona}");
    let brief = session_brief(&sense, 8_000).await;
    assert!(brief.len() <= 8_000);
    position(section(&brief, "## Identity"), "Most threads in: repo:github.com/o/r");
    for heading in ["## Identity", "## Persona", "## Self-model", "## Relationships", "## Core loop"] {
        assert_eq!(section(&brief, heading), section(&full, heading));
    }
    assert!(!brief.contains("Blocker 9") && !brief.contains("Ready task 14"), "{brief}");
    position(&brief, "… more: sense_checkpoint without arguments");
    position(&brief, "… more: sense_task action=ready limit=100");
}

#[tokio::test]
async fn the_brief_cuts_its_work_then_its_core_loop_then_the_relationships_the_self_model_and_the_persona() {
    let (_, items) = crowded().await;
    let card = identity::card(&*items).await.unwrap();
    let within = async |budget| compose(&*items, &context(THREAD), card.as_ref(), budget).await.unwrap();
    let full = within(64_000).await;
    assert_eq!(within(full.len()).await, full, "a brief that fits is unchanged");
    let persona = section(&full, "## Persona").to_owned();
    let work = ["Blocker 0", "- Lexer", "Ready task 0", "Send the notes", "Short commits"];
    let mut reached = [false; 5];
    for budget in (0..full.len()).rev().step_by(23) {
        let brief = within(budget).await;
        assert!(brief.len() <= budget, "{} > {budget}", brief.len());
        let has = |text: &str| brief.contains(text);
        let cuts = ["sense_checkpoint without", "sense_task action", "sense_promise action", "sense_remind action"];
        let work_cut = cuts.iter().any(|call| has(&format!("{MORE}{call}")));
        let core_whole = has("- sense_checkpoint: after each step");
        let persona_whole = has(&persona);
        if work_cut || !core_whole {
            assert!(!has("Keep each commit small."), "the memories shorten first: {brief}");
        }
        if !core_whole {
            assert!(work.iter().all(|text| !has(text)), "the work goes before the core loop: {brief}");
        }
        if !has("patient") {
            assert!(!core_whole, "the core loop goes before the relationships: {brief}");
        }
        if !has("careful") {
            assert!(!has("Works with Alice"), "the relationships go before the self-model: {brief}");
        }
        if !persona_whole {
            assert!(!has("Knows parsers"), "the self-model goes before the persona: {brief}");
        }
        let stages = [
            work_cut && core_whole,
            !core_whole && has("sense_checkpoint, sense_task") && has("patient"),
            !has("patient") && has("Works with Alice") && has("careful"),
            !has("careful") && has("Knows parsers") && persona_whole,
            !persona_whole && has("… more: sense_identity"),
        ];
        reached.iter_mut().zip(stages).for_each(|(reached, stage)| *reached |= stage);
    }
    assert_eq!(reached, [true; 5], "every cut is made");
}
