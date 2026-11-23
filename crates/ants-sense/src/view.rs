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
//! holds. The view is read from the ant's cache and writes nothing: it notes no session on the card. Its entries fit [`MEMORIES_CAP`], the ones left out counted by a last entry; an ant
//! sense keeps no data of shows one entry reading [`NO_DATA`].

use std::fmt::Write as _;

use ants_proto::provider::{AntRef, MemoriesView, MemoryEntry, RpcError};
use ants_proto::{MEMORIES_CAP, UserId};
use chrono::{DateTime, Utc};

use crate::Sense;
use crate::brief::{TOP_EXPERIENCE, identity_text};
use crate::dream::{self, DREAM, Dream};
use crate::identity::{self, card};
use crate::items::stamp;
use crate::memory::{self, DEFAULT_LIMIT};
use crate::persona::{self, PERSONA, persona_id};
use crate::proposal::{self, Confirmation, time};
use crate::records;
use crate::skill::{self, SKILL, skill_id};
use crate::store::{Items, StoreError};
use crate::thread::experience::experience;

mod report;
mod work;

pub use report::{KEY as REPORT, NO_DATA};

/// The key of the entry counting the entries left out.
pub const LEFT_OUT: &str = "left out";

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
    entries.push(MemoryEntry { key: REPORT.to_owned(), content: report });
    entries.extend(persona_entry(items).await?);
    let records = records::self_model(items).await?.into_iter().chain(identity::relationships(items).await?);
    entries.extend(records.map(|found| split(&found.full())));
    entries.extend(skill::skills(items).await?.into_iter().map(|skill| MemoryEntry {
        key: format!("{} ({SKILL}, self), trust self, revision {}", skill_id(&skill.name), skill.revision),
        content: format!("{}: {}", skill.name, skill.description),
    }));
    entries.extend(dream_entry(items).await?);
    entries.extend(work::entries(items, now).await?);
    let core = memory::core(items).await?;
    let top = memory::top_of_all(items, DEFAULT_LIMIT + core.len(), now).await?;
    let top = top.into_iter().map(|ranked| ranked.found).filter(|found| !found.memory.core).take(DEFAULT_LIMIT);
    entries.extend(core.into_iter().chain(top).map(|found| split(&found.full())));
    Ok(entries)
}

/// The one entry of an ant sense keeps no data of.
fn no_data() -> MemoryEntry {
    MemoryEntry { key: REPORT.to_owned(), content: NO_DATA.to_owned() }
}

/// The card and experience of `ant`, and the tally of its reviews, under its sense id; none before any.
async fn identity_entry(items: &dyn Items, ant: &UserId) -> Result<Option<MemoryEntry>, StoreError> {
    let card = card(items).await?;
    let experience = experience(items, ant, TOP_EXPERIENCE).await?;
    let reviews = dream::review_tally(items, TOP_EXPERIENCE).await?;
    if card.is_none() && experience.is_none() && reviews.is_none() {
        return Ok(None);
    }
    Ok(Some(MemoryEntry {
        key: format!("{} (identity, self), trust self", items.sense().await),
        content: identity_text(card.as_ref(), ant, experience.as_ref(), reviews.as_ref()),
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
    Ok(Some(MemoryEntry { key, content }))
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

    use super::work::FINDINGS;
    use super::*;
    use crate::dream::{ColonyTrails, Recipient, Start};
    use crate::items::Values;
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
        let none = MemoriesView { memories: vec![MemoryEntry { key: REPORT.into(), content: NO_DATA.into() }] };
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
        let entry = |key: &str| MemoryEntry { key: key.into(), content: "x".repeat(MEMORIES_CAP / 3) };
        let view = capped(vec![entry("a"), entry("b"), entry("c"), entry("d")]);
        assert_eq!(keys(&MemoriesView { memories: view.clone() }), ["a", "b", LEFT_OUT]);
        assert!(view[2].content.starts_with("2 more entries over the size cap"), "{}", view[2].content);
        assert_eq!(capped(vec![entry("a")]).len(), 1);
    }
}
