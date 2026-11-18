//! The memories view of sense ([`ants_proto::provider::method::MEMORIES`]): what `ants ant memories` and the memories
//! pane of the ant view show humans of who an ant has become. In order:
//!
//! 1. its identity: its card and experience;
//! 2. its persona, with its pending proposal;
//! 3. its self-model;
//! 4. each relationship record;
//! 5. its skills, by name and description;
//! 6. its last dream: the title of its diary, with its verdict counts;
//! 7. its core memories, then its top [`DEFAULT_LIMIT`] memories of every scope, as `sense_recall` ranks them without
//!    a query.
//!
//! Each entry's key names its id, kind, scope and trust. The view is read from the ant's cache and writes nothing: it
//! notes no session on the card. Its entries fit [`MEMORIES_CAP`], the ones left out counted by a last entry; an ant
//! sense keeps no data of shows none.

use std::fmt::Write as _;

use ants_proto::provider::{AntRef, MemoriesView, MemoryEntry, RpcError};
use ants_proto::{MEMORIES_CAP, UserId};
use chrono::{DateTime, Utc};

use crate::Sense;
use crate::brief::{TOP_EXPERIENCE, identity_text};
use crate::dream::{self, DREAM, Dream};
use crate::identity::{self, card};
use crate::memory::{self, DEFAULT_LIMIT};
use crate::persona::{self, PERSONA, persona_id};
use crate::proposal::{self, time};
use crate::records;
use crate::skill::{self, SKILL, skill_id};
use crate::store::{Items, StoreError};
use crate::thread::experience::experience;

/// The key of the entry counting the entries left out.
pub const LEFT_OUT: &str = "left out";

/// Bytes of [`MEMORIES_CAP`] kept for the entry counting the ones left out.
const RESERVE: usize = 1024;

/// The memories view of `ant`.
pub async fn memories(sense: &Sense, ant: AntRef) -> Result<MemoriesView, RpcError> {
    let read = async {
        let items = sense.items(&ant.ant).await?;
        entries(&*items, &ant.ant, Utc::now()).await
    };
    match read.await {
        Ok(entries) => Ok(MemoriesView { memories: capped(entries) }),
        Err(StoreError::NotKept(_)) => Ok(MemoriesView::default()),
        Err(e) => Err(RpcError::unavailable(&e.to_string())),
    }
}

/// Every entry of the view of `ant`, its memories ranked at `now`.
async fn entries(items: &dyn Items, ant: &UserId, now: DateTime<Utc>) -> Result<Vec<MemoryEntry>, StoreError> {
    let mut entries: Vec<_> = identity_entry(items, ant).await?.into_iter().collect();
    entries.extend(persona_entry(items).await?);
    let records = records::self_model(items).await?.into_iter().chain(identity::relationships(items).await?);
    entries.extend(records.map(|found| split(&found.full())));
    entries.extend(skill::skills(items).await?.into_iter().map(|skill| MemoryEntry {
        key: format!("{} ({SKILL}, self), trust self, revision {}", skill_id(&skill.name), skill.revision),
        content: format!("{}: {}", skill.name, skill.description),
    }));
    entries.extend(dream_entry(items).await?);
    let core = memory::core(items).await?;
    let top = memory::top_of_all(items, DEFAULT_LIMIT + core.len(), now).await?;
    let top = top.into_iter().map(|ranked| ranked.found).filter(|found| !found.memory.core).take(DEFAULT_LIMIT);
    entries.extend(core.into_iter().chain(top).map(|found| split(&found.full())));
    Ok(entries)
}

/// The card and experience of `ant`, under its sense id; none before either.
async fn identity_entry(items: &dyn Items, ant: &UserId) -> Result<Option<MemoryEntry>, StoreError> {
    let card = card(items).await?;
    let experience = experience(items, ant, TOP_EXPERIENCE).await?;
    if card.is_none() && experience.is_none() {
        return Ok(None);
    }
    Ok(Some(MemoryEntry {
        key: format!("{} (identity, self), trust self", items.sense().await),
        content: identity_text(card.as_ref(), ant, experience.as_ref()),
    }))
}

/// The confirmed persona, then the pending proposal of a new one; none before either.
async fn persona_entry(items: &dyn Items) -> Result<Option<MemoryEntry>, StoreError> {
    let current = persona::current(items).await?;
    let pending = proposal::pending_for(items, &persona_id()).await?;
    let mut key = format!("{} ({PERSONA}, self), trust self", persona_id());
    let mut content = match &current {
        Some(current) => {
            let _ = write!(key, ", revision {}", current.revision);
            format!(
                "Confirmed {}: {}\n{}",
                current.text("confirmed"),
                current.text("reason"),
                persona::text(&current.values)
            )
        }
        None if pending.is_some() => "None confirmed yet.".to_owned(),
        None => return Ok(None),
    };
    if let Some(pending) = &pending {
        let _ = write!(content, "\n{}\n{}", pending.line(), persona::text(&pending.proposal.version));
    }
    Ok(Some(MemoryEntry { key, content }))
}

/// The last dream: the title of its diary, when it began, where it stands; none before the first.
async fn dream_entry(items: &dyn Items) -> Result<Option<MemoryEntry>, StoreError> {
    let Some((dream, revision)) = dream::last(items).await? else { return Ok(None) };
    let mut content = format!("{}\nBegan {}, {}", dream.summary(), time(dream.at), dream.state);
    if let Some(diary) = &dream.diary {
        let _ = write!(content, "; diary {diary}");
    }
    content.push('.');
    Ok(Some(MemoryEntry {
        key: format!("{} ({DREAM}, self), trust self, revision {revision}", Dream::id(dream.number)),
        content,
    }))
}

/// An entry of an item shown in `full`: its first line, naming what it is, is the key.
fn split(full: &str) -> MemoryEntry {
    let (key, content) = full.split_once('\n').unwrap_or((full, ""));
    MemoryEntry { key: key.to_owned(), content: content.to_owned() }
}

/// The first `entries` whose texts fit [`MEMORIES_CAP`] as JSON, then one counting the others, if any.
fn capped(entries: Vec<MemoryEntry>) -> Vec<MemoryEntry> {
    let mut left = MEMORIES_CAP - RESERVE;
    let mut kept = Vec::new();
    let total = entries.len();
    for entry in entries {
        let size = serde_json::to_string(&entry.content).map_or(usize::MAX, |json| json.len());
        if size > left {
            break;
        }
        left -= size;
        kept.push(entry);
    }
    let out = total - kept.len();
    if out > 0 {
        kept.push(MemoryEntry {
            key: LEFT_OUT.to_owned(),
            content: format!(
                "{out} more entries over the size cap of a memories read: `ants ant export` carries all sense data of \
                 the ant."
            ),
        });
    }
    kept
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ants_proto::provider::Provider as _;
    use serde_json::json;

    use super::*;
    use crate::dream::{Recipient, Start};
    use crate::items::Values;
    use crate::memory::tests::{context, events, human, id_of, sense, tool};
    use crate::store::Write;

    fn of(ant: &str) -> AntRef {
        AntRef { ant: ant.parse().unwrap(), identity: "0f1e".into() }
    }

    fn keys(view: &MemoriesView) -> Vec<&str> {
        view.memories.iter().map(|entry| entry.key.as_str()).collect()
    }

    #[tokio::test]
    async fn an_ant_without_sense_data_shows_an_empty_view() {
        assert_eq!(Sense::new().memories(of("dev-a")).await.unwrap(), MemoriesView::default());
        let (sense, _) = sense().await;
        assert_eq!(sense.memories(of("dev-a")).await.unwrap(), MemoriesView::default());
    }

    #[tokio::test]
    async fn the_view_shows_who_the_ant_is_then_its_memories_writing_nothing() {
        let (sense, kept) = sense().await;
        let items: Arc<dyn Items> = kept.clone();
        let call = |handler, arguments| tool(&sense, handler, arguments, context(human()));
        identity::record_session(&*items, &context(human())).await.unwrap();
        let mut persona = Values::new();
        persona.insert("voice".into(), json!("Plain."));
        persona.insert("confirmed".into(), json!("2025-01-01T12:00:00Z"));
        persona.insert("reason".into(), json!("Asked for."));
        let mut skill = Values::new();
        skill.insert("name".into(), json!("release"));
        skill.insert("description".into(), json!("Cut a release."));
        skill.insert("files".into(), json!({"SKILL.md": "---\nname: release\ndescription: Cut a release.\n---\n"}));
        let writes = [
            Write::Create { id: persona_id(), kind: PERSONA.into(), values: persona },
            Write::Create { id: skill_id("release"), kind: SKILL.into(), values: skill },
        ];
        items.write(&writes, "test").await.unwrap();
        for arguments in [
            json!({"voice": "Terse.", "reason": "Shorter."}),
            json!({"self_model": "Careful.", "title": "A careful reviewer"}),
            json!({"relationship": "Wants tests first.", "who": "human:alice", "title": "Alice: tests first"}),
        ] {
            assert!(!call(identity::identity, arguments).await.is_error);
        }
        let writer = context(human());
        let start =
            Start { at: Utc::now(), recipient: Recipient::Ant, threads: vec![], commits: vec![], writer: &writer };
        dream::begin(&*items, start).await.unwrap().unwrap();
        let remember =
            |title: &str, body: &str, core| json!({"kind": "lesson", "title": title, "body": body, "core": core});
        let core = id_of(&call(memory::remember, remember("Always rebase", "Merges hide history.", true)).await);
        let top = id_of(&call(memory::remember, remember("Tests run slow", "The suite takes minutes.", false)).await);

        let logged = events(&kept).await.len();
        let view = sense.memories(of("dev-a")).await.unwrap();
        assert_eq!(events(&kept).await.len(), logged, "the view writes nothing");
        let keys = keys(&view);
        let card = identity::card(&*items).await.unwrap().unwrap();
        let expected = [
            format!("{} (identity, self), trust self", card.sense),
            format!("{} (persona, self), trust self, revision 1", persona_id()),
            "(self_model, self), trust self, revision 1".to_owned(),
            "(relationship, human:alice), trust self, revision 1".to_owned(),
            format!("{} (skill, self), trust self, revision 1", skill_id("release")),
            format!("{} (dream, self), trust self, revision 1", Dream::id(1)),
            format!("{core} (lesson, self, importance 3, core), active, trust self, revision 1"),
            format!("{top} (lesson, self, importance 3), active, trust self, revision 1"),
        ];
        assert_eq!(keys.len(), expected.len(), "{keys:#?}");
        for (key, expected) in keys.iter().zip(&expected) {
            assert!(key.ends_with(expected.as_str()), "{key:?} is not {expected:?}");
        }
        let content = |n: usize| view.memories[n].content.as_str();
        assert!(content(0).starts_with("dev-a (sense identity "), "{}", content(0));
        assert!(content(1).starts_with("Confirmed 2025-01-01T12:00:00Z: Asked for.\nVoice: Plain.\nProposal "));
        assert!(content(1).ends_with("Voice: Terse."), "{}", content(1));
        assert!(content(2).starts_with("A careful reviewer\nCareful.\nWritten "), "{}", content(2));
        assert_eq!(content(4), "release: Cut a release.");
        assert!(
            content(5).starts_with("Dream 1: 1 findings, 0 applied, 0 rejected, 0 deferred, 1 unanswered\nBegan "),
            "{}",
            content(5)
        );
        assert!(content(6).starts_with("Always rebase\nMerges hide history."), "{}", content(6));
    }

    #[test]
    fn entries_past_the_cap_are_left_out_and_counted() {
        let entry = |key: &str| MemoryEntry { key: key.into(), content: "x".repeat(MEMORIES_CAP / 3) };
        let view = capped(vec![entry("a"), entry("b"), entry("c"), entry("d")]);
        assert_eq!(keys(&MemoriesView { memories: view.clone() }), ["a", "b", LEFT_OUT]);
        assert!(view[2].content.starts_with("2 more entries over the size cap"), "{}", view[2].content);
        assert_eq!(capped(vec![entry("a")]).len(), 1);
    }
}
