//! Tests of the persona and skills in the brief: the persona follows the identity, skills precede the core loop and
//! shorten to their names within the budget.

use super::*;
use crate::items::Values;
use crate::persona::{PERSONA, persona_id};
use crate::skill::{SKILL, skill_id};
use crate::store::Write;

/// The write of skill `name`, confirmed, described by `description`.
fn skill(name: &str, description: &str) -> Write {
    let mut values = Values::new();
    values.insert("name".into(), json!(name));
    values.insert("description".into(), json!(description));
    values.insert("files".into(), json!({"SKILL.md": format!("---\nname: {name}\ndescription: {description}\n---\n")}));
    Write::Create { id: skill_id(name), kind: SKILL.into(), values }
}

#[tokio::test]
async fn the_persona_follows_the_identity_and_the_skills_precede_the_core_loop() {
    let (sense, items) = filled().await;
    let brief = session_brief(&sense, 8_000).await;
    position(&brief, "## Persona\nNo persona yet: propose");
    assert!(!brief.contains("## Skills"), "{brief}");

    let mut persona = Values::new();
    persona.insert("voice".into(), json!("Plain and brief."));
    persona.insert("boundaries".into(), json!("No force pushes."));
    let mut writes = vec![Write::Create { id: persona_id(), kind: PERSONA.into(), values: persona }];
    writes.extend([skill("release", "Cut a release."), skill("notes", "Write release notes.")]);
    items.write(&writes, "test").await.unwrap();
    let brief = session_brief(&sense, 8_000).await;
    let headings = ["## Identity", "## Persona", "## Self-model", "## Memories", "## Skills", "## Core loop"];
    let at: Vec<_> = headings.iter().map(|heading| position(&brief, heading)).collect();
    assert!(at.is_sorted(), "{brief}");
    assert_eq!(&brief[at[1]..at[2]], "## Persona\nVoice: Plain and brief.\nBoundaries: No force pushes.\n\n");
    position(&brief[at[4]..at[5]], "- notes: Write release notes.\n- release: Cut a release.");
}

#[tokio::test]
async fn the_skills_shorten_to_their_names_within_the_budget() {
    let (sense, items) = sense().await;
    let writes: Vec<_> = (0..20).map(|n| skill(&format!("skill-{n:02}"), &"careful ".repeat(40))).collect();
    items.write(&writes, "test").await.unwrap();
    let full = session_brief(&sense, 16_000).await;
    position(&full, "- skill-19: careful");
    let budget = u32::try_from(full.len()).unwrap() - 1;
    let brief = session_brief(&sense, budget).await;
    assert!(brief.len() <= budget as usize);
    assert!(!brief.contains("careful"), "{brief}");
    position(&brief, "- skill-00\n- skill-01");
    position(&brief, &format!("{MORE}sense_skill"));
}
