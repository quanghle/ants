use super::*;

async fn record(sense: &Sense, kind: &str, who: &str, body: &str) {
    call(sense, identity::identity, THREAD, json!({kind: body, "who": who, "title": who})).await;
}

#[tokio::test]
async fn the_self_model_follows_the_identity_and_the_participants_records_precede_the_memories() {
    let (sense, _) = filled().await;
    let brief = session_brief(&sense, 8_000).await;
    position(&brief, "No self-model yet");

    record(&sense, "self_model", "self", "Strong at parsers.").await;
    record(&sense, "relationship", "human:alice", "Alice wants tests first.").await;
    record(&sense, "relationship", "human:bob", "Bob wants docs first.").await;
    let brief = session_brief(&sense, 8_000).await;
    let headings = ["## Identity", "## Self-model", "## This thread", "## Relationships", "## Memories"];
    let at: Vec<_> = headings.iter().map(|heading| position(&brief, heading)).collect();
    assert!(at.is_sorted(), "{brief}");
    let model = &brief[at[1]..at[2]];
    assert!(position(model, RECALLED_BEGIN) < position(model, "(self, revision"), "{model}");
    position(model, "(self, revision 1): self\n  Strong at parsers.");
    let relationships = &brief[at[3]..at[4]];
    position(relationships, "(human:alice, revision 1): human:alice\n  Alice wants tests first.");
    assert!(!brief.contains("Bob wants docs first."), "only the thread's participants: {brief}");
}

#[tokio::test]
async fn records_shorten_to_their_titles_and_cut_naming_the_record_to_read() {
    let (sense, _) = sense().await;
    record(&sense, "self_model", "self", &"careful ".repeat(200)).await;
    record(&sense, "relationship", "human:alice", &"patient ".repeat(200)).await;
    let full = session_brief(&sense, 8_000).await;
    let first_without = async |text: &str| {
        for budget in (0..full.len()).rev().step_by(16) {
            let brief = session_brief(&sense, u32::try_from(budget).unwrap()).await;
            if !brief.contains(text) {
                return brief;
            }
        }
        panic!("{text:?} is never cut");
    };
    let brief = first_without("patient").await;
    position(&brief, "careful");
    position(&brief, "(human:alice, revision 1): human:alice\n");
    position(&brief, &format!("{MORE}sense_recall id=<id>"));
    let brief = first_without("careful").await;
    position(&brief, "(self, revision 1): self\n");
    let id = crate::records::self_model(&*items_of(&sense).await).await.unwrap().unwrap().item.id;
    position(&brief, &format!("{MORE}sense_recall id={id}"));
    position(&brief, &format!("## Relationships\n{MORE}sense_recall id=<id>"));
}

async fn items_of(sense: &Sense) -> Arc<dyn Items> {
    sense.items(&"dev-a".parse().unwrap()).await.unwrap()
}

/// The brief of a review `ant` asks of the sage.
async fn review(sense: &Sense, ant: &str) -> String {
    let origin = Origin { by: Starter::Review { user: ant.parse().unwrap() }, channel: None };
    let context = CallContext { origin, ..context(OTHER) };
    sense.brief(BriefRequest { context, budget: 8_000, reason: BriefReason::Started }).await.unwrap().text
}

#[tokio::test]
async fn a_review_s_brief_shows_what_the_sage_keeps_of_the_ant_under_review_alone() {
    let (sense, _) = sense().await;
    record(&sense, "relationship", "ant:dev-b", "dev-b takes findings citing file and line.").await;
    let fact = json!({"kind": "fact", "scope": "ant:dev-b", "title": "dev-b runs its tests twice", "body": "Flaky."});
    call(&sense, memory::remember, THREAD, fact).await;
    let lesson = json!({"kind": "lesson", "title": "Cite the commit", "body": "Every finding cites the commit."});
    call(&sense, memory::remember, THREAD, lesson).await;

    let of_b = review(&sense, "dev-b").await;
    position(&of_b, "(ant:dev-b, revision 1): ant:dev-b\n  dev-b takes findings citing file and line.");
    position(&of_b, "dev-b runs its tests twice");
    position(&of_b, "Cite the commit");
    let of_c = review(&sense, "dev-c").await;
    assert!(!of_c.contains("dev-b takes findings") && !of_c.contains("dev-b runs its tests"), "{of_c}");
    position(&of_c, "Cite the commit");
}

#[tokio::test]
async fn a_review_s_brief_keeps_room_for_memories_of_the_ant_under_review_among_many_lessons() {
    let (sense, _) = sense().await;
    for n in 0..TOP_MEMORIES + 2 {
        let (title, body) = (format!("Lesson {n}"), format!("Rule {n}."));
        let lesson = json!({"kind": "lesson", "title": title, "body": body, "importance": 5, "force": true});
        call(&sense, memory::remember, THREAD, lesson).await;
    }
    for importance in 1..=REVIEWED_MEMORIES + 1 {
        let title = format!("dev-b note {importance}");
        let mut fact = json!({"kind": "fact", "scope": "ant:dev-b", "title": title, "body": title, "force": true});
        fact["importance"] = json!(importance);
        call(&sense, memory::remember, THREAD, fact).await;
    }

    let brief = review(&sense, "dev-b").await;
    let shown = |prefix: &str| brief.lines().filter(|line| line.starts_with("- ") && line.contains(prefix)).count();
    assert_eq!(shown("): dev-b note"), REVIEWED_MEMORIES, "{brief}");
    assert!(!brief.contains("dev-b note 1"), "only the best of them: {brief}");
    assert_eq!(shown("): Lesson"), TOP_MEMORIES - REVIEWED_MEMORIES, "{brief}");
    let notes = position(&brief, "dev-b note");
    assert!(position(&brief, "): Lesson") < notes, "in rank order: {brief}");
}
