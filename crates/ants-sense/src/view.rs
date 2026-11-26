//! The memories view of sense ([`ants_proto::provider::method::MEMORIES`]): what `ants ant memories` and the memories
//! pane of the ant view show humans of who an ant has become. In order:
//!
//! 1. its identity: its card and experience, and the tally of the reviews it made as a sage;
//! 2. its growth report (`report`): how its identity grew, in counts;
//! 3. its persona, with its pending proposal;
//! 4. its self-model;
//! 5. each relationship record;
//! 6. its skills, by name and description;
//! 7. its last dream: the title of its diary, with its verdict counts and the vital signs of its period, each with its
//!    change against the mean of the dreams before (`dream::changes`);
//! 8. its open work (`work`): the findings of its last dream unanswered or deferred, its sage's among them, and those
//!    its sage gave late; its open commitments; its active reminders; its tasks not closed;
//! 9. its core memories, then its top [`DEFAULT_LIMIT`] memories of every scope, as `sense_recall` ranks them without
//!    a query.
//!
//! Each entry's key names its id, kind, scope and trust, or, for the growth report and each list of open work, what it
//! holds, and its typed fields ([`MemoryFields`]) tell the same: its section ([`IDENTITY`], [`REPORT_SECTION`],
//! [`PERSONA_SECTION`], [`SELF_MODEL_SECTION`], [`RELATIONSHIPS`], [`SKILLS`], [`DREAM_SECTION`], [`WORK`],
//! [`MEMORIES`], [`LEFT_OUT_SECTION`]), and the id, kind, scope, trust, title, revision and coreness of the item it
//! shows, the entry of the active reminders a row for each reminder and watcher. The view is read from the ant's cache
//! and writes nothing: it notes no session on the card. Its entries fit [`MEMORIES_CAP`] as JSON, their typed fields
//! counted, the ones left out counted by a last entry; an ant sense keeps no data of shows one entry reading
//! [`NO_DATA`].

use std::fmt::Write as _;

use ants_proto::provider::{AntRef, MemoriesView, MemoryEntry, MemoryFields, RpcError};
use ants_proto::{MEMORIES_CAP, UserId};
use chrono::{DateTime, Utc};

use crate::Sense;
use crate::brief::{TOP_EXPERIENCE, identity_text};
use crate::dream::{self, DREAM, Dream};
use crate::identity::{self, card};
use crate::items::stamp;
use crate::memory::{self, DEFAULT_LIMIT, MemoryItem, Scope, Trust};
use crate::persona::{self, PERSONA, persona_id};
use crate::proposal::{self, Confirmation, time};
use crate::records::{self, RecordItem};
use crate::skill::{self, SKILL, skill_id};
use crate::store::{Items, StoreError};
use crate::thread::experience::experience;

mod report;
mod work;

pub use report::{KEY as REPORT, NO_DATA};
pub use work::REMINDERS;

/// The key of the entry counting the entries left out.
pub const LEFT_OUT: &str = "left out";

/// The sections of the view, in its order, as [`MemoryFields::section`] names them.
pub const IDENTITY: &str = "identity";
pub const REPORT_SECTION: &str = "report";
pub const PERSONA_SECTION: &str = "persona";
pub const SELF_MODEL_SECTION: &str = "self_model";
pub const RELATIONSHIPS: &str = "relationships";
pub const SKILLS: &str = "skills";
pub const DREAM_SECTION: &str = "dream";
pub const WORK: &str = "work";
pub const MEMORIES: &str = "memories";
pub const LEFT_OUT_SECTION: &str = "left_out";

/// Bytes of [`MEMORIES_CAP`] kept for the entry counting the ones left out.
const RESERVE: usize = 1024;

/// The memories view of `ant`.
pub async fn memories(sense: &Sense, ant: AntRef) -> Result<MemoriesView, RpcError> {
    let read = async {
        let items = sense.synced(&ant.ant).await?;
        entries(&*items, &ant.ant, Utc::now()).await
    };
    match read.await {
        Ok(entries) => Ok(MemoriesView { memories: capped(entries) }),
        Err(StoreError::NotKept(_)) => Ok(MemoriesView { memories: vec![no_data()] }),
        Err(e) => Err(RpcError::unavailable(&e.to_string())),
    }
}

/// Every entry of the view of `ant`, its memories ranked at `now`.
async fn entries(items: &dyn Items, ant: &UserId, now: DateTime<Utc>) -> Result<Vec<MemoryEntry>, StoreError> {
    let Some(report) = report::report(items, ant, now).await? else { return Ok(vec![no_data()]) };
    let mut entries: Vec<_> = identity_entry(items, ant).await?.into_iter().collect();
    entries.push(MemoryEntry { key: REPORT.to_owned(), content: report, fields: section(REPORT_SECTION) });
    entries.extend(persona_entry(items).await?);
    let records = records::self_model(items).await?.into_iter().chain(identity::relationships(items).await?);
    entries.extend(records.map(|found| record_entry(&found)));
    entries.extend(skill::skills(items).await?.into_iter().map(|skill| {
        let id = skill_id(&skill.name);
        MemoryEntry {
            key: format!("{id} ({SKILL}, self), trust self, revision {}", skill.revision),
            content: format!("{}: {}", skill.name, skill.description),
            fields: MemoryFields {
                title: Some(skill.name.clone()),
                revision: Some(skill.revision),
                ..own(SKILLS, id.to_string(), SKILL)
            },
        }
    }));
    entries.extend(dream_entry(items).await?);
    entries.extend(work::entries(items, now).await?);
    let core = memory::core(items).await?;
    let top = memory::top_of_all(items, DEFAULT_LIMIT + core.len(), now).await?;
    let top = top.into_iter().map(|ranked| ranked.found).filter(|found| !found.memory.core).take(DEFAULT_LIMIT);
    entries.extend(core.into_iter().chain(top).map(|found| memory_entry(&found)));
    Ok(entries)
}

/// The one entry of an ant sense keeps no data of.
fn no_data() -> MemoryEntry {
    MemoryEntry { key: REPORT.to_owned(), content: NO_DATA.to_owned(), fields: section(REPORT_SECTION) }
}

/// The typed fields of an entry of `section` alone.
fn section(section: &str) -> MemoryFields {
    MemoryFields { section: Some(section.to_owned()), ..MemoryFields::default() }
}

/// The typed fields of an entry of `section` showing the item `id` of `kind`, the ant's own and about itself.
fn own(section: &str, id: String, kind: &str) -> MemoryFields {
    MemoryFields {
        id: Some(id),
        kind: Some(kind.to_owned()),
        scope: Some(Scope::Own.to_string()),
        trust: Some(Trust::Own.name().to_owned()),
        ..self::section(section)
    }
}

/// The entry of the self-model or a relationship record `found`.
fn record_entry(found: &RecordItem) -> MemoryEntry {
    let section = if found.item.kind == records::SELF_MODEL { SELF_MODEL_SECTION } else { RELATIONSHIPS };
    let record = &found.record;
    let fields = MemoryFields {
        id: Some(found.id().to_string()),
        kind: Some(found.item.kind.clone()),
        scope: Some(record.scope.to_string()),
        trust: Some(record.trust.name().to_owned()),
        title: Some(record.title.clone()),
        revision: Some(found.item.revision),
        ..self::section(section)
    };
    MemoryEntry { fields, ..split(&found.full()) }
}

/// The entry of the memory `found`.
fn memory_entry(found: &MemoryItem) -> MemoryEntry {
    let memory = &found.memory;
    let fields = MemoryFields {
        id: Some(found.id().to_string()),
        kind: Some(memory.kind.to_string()),
        scope: Some(memory.scope.to_string()),
        trust: Some(memory.trust.name().to_owned()),
        title: Some(memory.title.clone()),
        revision: Some(found.item.revision),
        core: memory.core,
        ..section(MEMORIES)
    };
    MemoryEntry { fields, ..split(&found.full()) }
}

/// The card and experience of `ant`, and the tally of its reviews, under its sense id; none before any.
async fn identity_entry(items: &dyn Items, ant: &UserId) -> Result<Option<MemoryEntry>, StoreError> {
    let card = card(items).await?;
    let experience = experience(items, ant, TOP_EXPERIENCE).await?;
    let reviews = dream::review_tally(items, TOP_EXPERIENCE).await?;
    if card.is_none() && experience.is_none() && reviews.is_none() {
        return Ok(None);
    }
    let sense = items.sense().await;
    Ok(Some(MemoryEntry {
        key: format!("{sense} (identity, self), trust self"),
        content: identity_text(card.as_ref(), ant, experience.as_ref(), reviews.as_ref()),
        fields: own(IDENTITY, sense.to_string(), "identity"),
    }))
}

/// The confirmed persona, then the pending proposal of a new one; none before either.
async fn persona_entry(items: &dyn Items) -> Result<Option<MemoryEntry>, StoreError> {
    let current = persona::current(items).await?;
    let pending = proposal::pending_for(items, &persona_id()).await?;
    let mut key = format!("{} ({PERSONA}, self), trust self", persona_id());
    let mut fields = own(PERSONA_SECTION, persona_id().to_string(), PERSONA);
    let mut content = match &current {
        Some(current) => {
            let _ = write!(key, ", revision {}", current.revision);
            fields.revision = Some(current.revision);
            let confirmation = Confirmation::of(&current.values);
            format!(
                "Confirmed {}: {}\n{}",
                confirmation.confirmed.map_or_default(stamp),
                confirmation.reason,
                persona::text(&current.values)
            )
        }
        None if pending.is_some() => "None confirmed yet.".to_owned(),
        None => return Ok(None),
    };
    if let Some(pending) = &pending {
        let _ = write!(content, "\n{}\n{}", pending.line(), persona::text(&pending.proposal.version));
    }
    Ok(Some(MemoryEntry { key, content, fields }))
}

/// The last dream: the title of its diary, when it began, where it stands, and the vital signs of its period with their
/// change against the dreams before; none before the first.
async fn dream_entry(items: &dyn Items) -> Result<Option<MemoryEntry>, StoreError> {
    let Some((dream, revision)) = dream::last(items).await? else { return Ok(None) };
    let mut content = format!("{}\nBegan {}, {}", dream.summary(), time(dream.at), dream.state);
    if let Some(diary) = &dream.diary {
        let _ = write!(content, "; diary {diary}");
    }
    content.push('.');
    if let Some(vitals) = &dream.vitals {
        let _ = write!(content, "\nVital signs: {}.", vitals.line());
        if let Some(changes) = dream::changes(vitals, &dream::trend(items, dream.number).await?) {
            let _ = write!(content, "\nAgainst {changes}.");
        }
    }
    let id = Dream::id(dream.number);
    let fields = MemoryFields {
        title: Some(dream.summary()),
        revision: Some(revision),
        ..own(DREAM_SECTION, id.to_string(), DREAM)
    };
    Ok(Some(MemoryEntry { key: format!("{id} ({DREAM}, self), trust self, revision {revision}"), content, fields }))
}

/// An entry of an item shown in `full`: its first line, naming what it is, is the key.
fn split(full: &str) -> MemoryEntry {
    let (key, content) = full.split_once('\n').unwrap_or((full, ""));
    MemoryEntry::new(key, content)
}

/// The first `entries` fitting [`MEMORIES_CAP`] as JSON, their keys and typed fields counted, then one counting the
/// others, if any.
fn capped(entries: Vec<MemoryEntry>) -> Vec<MemoryEntry> {
    let mut left = MEMORIES_CAP - RESERVE;
    let mut kept = Vec::new();
    let total = entries.len();
    for entry in entries {
        let size = serde_json::to_vec(&entry).map_or(usize::MAX, |json| json.len());
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
            fields: section(LEFT_OUT_SECTION),
        });
    }
    kept
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ants_proto::provider::Provider as _;
    use serde_json::json;

    use super::work::FINDINGS;
    use super::*;
    use crate::dream::{ColonyTrails, Recipient, Start};
    use crate::items::{ItemId, Values};
    use crate::memory::tests::id_of;
    use crate::store::Write;
    use crate::testing::{context, events, human, sense, tool};

    fn of(ant: &str) -> AntRef {
        AntRef { ant: ant.parse().unwrap(), identity: "0f1e".into() }
    }

    fn keys(view: &MemoriesView) -> Vec<&str> {
        view.memories.iter().map(|entry| entry.key.as_str()).collect()
    }

    #[tokio::test]
    async fn an_ant_without_sense_data_reads_no_sense_data() {
        let none = MemoriesView { memories: vec![no_data()] };
        assert_eq!(Sense::new().memories(of("dev-a")).await.unwrap(), none);
        let (sense, _) = sense().await;
        assert_eq!(sense.memories(of("dev-a")).await.unwrap(), none);
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
        let mut signs = dream::Signs { threads: 1, ..dream::Signs::default() };
        signs.prompts.replied = 2;
        let vitals = vec![(ants_proto::ThreadId::generate(), signs)];
        let start = Start {
            at: Utc::now(),
            recipient: Recipient::Ant,
            threads: vec![],
            commits: vec![],
            writer: &writer,
            vitals,
            trails: &ColonyTrails::UNREAD,
        };
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
            REPORT.to_owned(),
            format!("{} (persona, self), trust self, revision 1", persona_id()),
            "(self_model, self), trust self, revision 1".to_owned(),
            "(relationship, human:alice), trust self, revision 1".to_owned(),
            format!("{} (skill, self), trust self, revision 1", skill_id("release")),
            format!("{} (dream, self), trust self, revision 1", Dream::id(1)),
            FINDINGS.to_owned(),
            format!("{core} (lesson, self, importance 3, core), active, trust self, revision 1"),
            format!("{top} (lesson, self, importance 3), active, trust self, revision 1"),
        ];
        assert_eq!(keys.len(), expected.len(), "{keys:#?}");
        for (key, expected) in keys.iter().zip(&expected) {
            assert!(key.ends_with(expected.as_str()), "{key:?} is not {expected:?}");
        }
        assert_typed(&view, &card.sense.to_string(), &core, &top);
        let content = |n: usize| view.memories[n].content.as_str();
        assert!(content(0).starts_with("dev-a (sense identity "), "{}", content(0));
        assert!(content(1).starts_with("Threads: none recorded.\nBy week from Monday (UTC)"), "{}", content(1));
        assert!(content(2).starts_with("Confirmed 2025-01-01T12:00:00Z: Asked for.\nVoice: Plain.\nProposal "));
        assert!(content(2).ends_with("Voice: Terse."), "{}", content(2));
        assert!(content(3).starts_with("A careful reviewer\nCareful.\nWritten "), "{}", content(3));
        assert_eq!(content(5), "release: Cut a release.");
        assert!(
            content(6).starts_with("Dream 1: 1 findings, 0 applied, 0 rejected, 0 deferred, 1 unanswered\nBegan "),
            "{}",
            content(6)
        );
        assert!(content(6).ends_with(".\nVital signs: 2 prompts (2 replied)."), "{}", content(6));
        assert!(content(7).starts_with("Dream 1 (fallback): 1 of 1 findings unanswered or deferred"), "{}", content(7));
        assert!(content(8).starts_with("Always rebase\nMerges hide history."), "{}", content(8));
    }

    /// The typed fields of the entries of `view`, which the test above reads, `sense` its card's sense id and `core`
    /// and `top` its memories.
    fn assert_typed(view: &MemoriesView, sense: &str, core: &ItemId, top: &ItemId) {
        let sections: Vec<_> = view.memories.iter().map(|entry| entry.fields.section.as_deref().unwrap()).collect();
        let expected = [IDENTITY, REPORT_SECTION, PERSONA_SECTION, SELF_MODEL_SECTION, RELATIONSHIPS, SKILLS];
        assert_eq!(sections[..6], expected);
        assert_eq!(sections[6..], [DREAM_SECTION, WORK, MEMORIES, MEMORIES]);
        let fields = |n: usize| &view.memories[n].fields;
        let typed = |n: usize| {
            let fields = fields(n);
            let text = |field: &Option<String>| field.clone().unwrap_or_default();
            [&fields.id, &fields.kind, &fields.scope, &fields.trust].map(text)
        };
        assert_eq!(typed(0), [sense.to_owned(), "identity".into(), "self".into(), "self".into()]);
        assert_eq!((typed(2)[1].as_str(), fields(2).revision), (PERSONA, Some(1)));
        assert_eq!(typed(4)[1..], ["relationship", "human:alice", "self"]);
        assert_eq!(fields(4).title.as_deref(), Some("Alice: tests first"));
        assert_eq!((fields(5).title.as_deref(), typed(5)[1].as_str()), (Some("release"), SKILL));
        assert!(fields(6).title.as_deref().unwrap().starts_with("Dream 1: 1 findings"), "{:?}", fields(6));
        assert_eq!(typed(8), [core.to_string(), "lesson".into(), "self".into(), "self".into()]);
        assert_eq!(
            (fields(8).title.as_deref(), fields(8).revision, fields(8).core),
            (Some("Always rebase"), Some(1), true)
        );
        assert_eq!((typed(9)[0].as_str(), fields(9).core), (top.as_str(), false));
        assert!(fields(1).id.is_none() && fields(7).id.is_none());
    }

    #[tokio::test]
    async fn the_last_dream_shows_its_vital_signs_against_the_dreams_before() {
        let (_sense, kept) = sense().await;
        let items: Arc<dyn Items> = kept.clone();
        let writer = context(human());
        let replied = |replied| {
            let mut signs = dream::Signs { threads: 1, ..dream::Signs::default() };
            signs.prompts.replied = replied;
            vec![(ants_proto::ThreadId::generate(), signs)]
        };
        let start = |at, vitals| Start {
            at,
            recipient: Recipient::Ant,
            threads: vec![],
            commits: vec![],
            writer: &writer,
            vitals,
            trails: &ColonyTrails::UNREAD,
        };
        let yesterday = Utc::now() - chrono::Duration::hours(25);
        dream::begin(&*items, start(yesterday, replied(1))).await.unwrap().unwrap();
        let first = dream_entry(&*items).await.unwrap().unwrap().content;
        assert!(first.ends_with(".\nVital signs: 1 prompt (1 replied)."), "no dream before: {first}");
        dream::begin(&*items, start(Utc::now(), replied(2))).await.unwrap().unwrap();
        let second = dream_entry(&*items).await.unwrap().unwrap().content;
        let vitals = ".\nVital signs: 2 prompts (2 replied).\nAgainst the mean of the dream before: threads worked 1 \
                      (mean 1, +0%); prompts replied 2 (mean 1, +100%).";
        assert!(second.ends_with(vitals), "{second}");
    }

    #[test]
    fn entries_past_the_cap_are_left_out_and_counted() {
        let entry = |key: &str| MemoryEntry::new(key, "x".repeat(MEMORIES_CAP / 3));
        let view = capped(vec![entry("a"), entry("b"), entry("c"), entry("d")]);
        assert_eq!(keys(&MemoriesView { memories: view.clone() }), ["a", "b", LEFT_OUT]);
        assert!(view[2].content.starts_with("2 more entries over the size cap"), "{}", view[2].content);
        assert_eq!(capped(vec![entry("a")]).len(), 1);
        let title = MemoryFields { title: Some("x".repeat(MEMORIES_CAP / 3)), ..MemoryFields::default() };
        let typed = MemoryEntry { fields: title, ..MemoryEntry::new("t", "") };
        let view = capped(vec![entry("a"), typed, entry("b")]);
        assert_eq!(keys(&MemoriesView { memories: view.clone() }), ["a", "t", LEFT_OUT], "the typed fields count");
        assert_eq!(view[2].fields.section.as_deref(), Some(LEFT_OUT_SECTION));
    }
}
