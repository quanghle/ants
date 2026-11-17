use super::*;

async fn record(sense: &Sense, kind: &str, scope: &str, body: &str) {
    call(sense, memory::remember, THREAD, json!({"kind": kind, "scope": scope, "title": scope, "body": body})).await;
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
    let brief = session_brief(&sense, u32::try_from(full.len()).unwrap() - 1).await;
    assert!(!brief.contains("careful") && !brief.contains("patient"), "{brief}");
    position(&brief, "(self, revision 1): self\n");
    position(&brief, "(human:alice, revision 1): human:alice\n");
    let id = crate::records::self_model(&*items_of(&sense).await).await.unwrap().unwrap().item.id;
    position(&brief, &format!("{MORE}sense_recall id={id}"));
    position(&brief, &format!("{MORE}sense_recall id=<id>"));
}

async fn items_of(sense: &Sense) -> Arc<dyn Items> {
    sense.items(&"dev-a".parse().unwrap()).await.unwrap()
}
