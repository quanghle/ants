//! Tests of the import of an ant's harness memories: files split by section and into parts, bd memories, the source
//! and trust each memory keeps, duplicates left out, the cap, and one import an ant.

use ants_proto::provider::Starter;
use serde_json::json;

use super::*;
use crate::memory::{CORE_BUDGET, Status, core, remember};
use crate::testing::{self, human};

fn writer() -> CallContext {
    testing::context(Starter::Provider { name: crate::NAME.into() })
}

fn file(path: &str, scope: Scope, text: &str) -> Source {
    Source { from: Imported::File { path: path.into() }, scope, text: text.into() }
}

fn bd(key: &str, text: &str) -> Source {
    Source {
        from: Imported::Bd { db: "/work/.bd/bd.db".into(), key: key.into() },
        scope: Scope::Own,
        text: text.into(),
    }
}

async fn memories(items: &dyn Items) -> Vec<Memory> {
    let mut found: Vec<_> =
        items.list(MEMORY, usize::MAX).await.unwrap().into_iter().filter_map(MemoryItem::of).collect();
    found.sort_by(|a, b| a.memory.title.cmp(&b.memory.title));
    found.into_iter().map(|found| found.memory).collect()
}

const AGENTS: &str = "Preamble of the guide.

# Guide

## Build

Run `make check` before each commit.

```sh
# not a heading
make check
```

### Release

Tag v1.2 then push.

## Empty

# Notes ##

Keep commits small.
";

#[tokio::test]
async fn each_section_of_a_file_and_each_bd_memory_is_an_untrusted_fact_naming_its_source() {
    let (_, items) = testing::sense().await;
    let repo = Scope::Repo("github.com/o/r".into());
    let sources = [file("AGENTS.md", repo.clone(), AGENTS), bd("deploy", "Deploy on Tuesdays with ants-deploy.")];
    let record = import(&*items, &sources, &writer()).await.unwrap().unwrap();
    assert_eq!(record, Import { at: writer().at, sources: 2, imported: 5, duplicates: 0, over: 0 });

    let found = memories(&*items).await;
    let titles: Vec<_> = found.iter().map(|memory| memory.title.as_str()).collect();
    assert_eq!(
        titles,
        [
            "AGENTS.md",
            "AGENTS.md: Guide > Build",
            "AGENTS.md: Guide > Build > Release",
            "AGENTS.md: Notes",
            "bd memory deploy"
        ]
    );
    assert!(found[1].body.contains("# not a heading"), "a fenced block keeps its lines: {}", found[1].body);
    assert_eq!(found[2].body, "Tag v1.2 then push.");
    for memory in &found {
        assert_eq!(
            (memory.kind, memory.trust, memory.core, memory.status),
            (MemoryKind::Fact, Trust::Untrusted, false, Status::Active)
        );
        assert_eq!(memory.writer, writer());
    }
    assert_eq!(found[0].scope, repo);
    assert_eq!(found[0].imported, Some(Imported::File { path: "AGENTS.md".into() }));
    assert_eq!((&found[4].scope, &found[4].body), (&Scope::Own, &"Deploy on Tuesdays with ants-deploy.".to_owned()));
    let full = MemoryItem::of(items.list(MEMORY, usize::MAX).await.unwrap().remove(0)).unwrap().full();
    assert!(full.contains("\nImported from "), "{full}");
    assert!(core(&*items).await.unwrap().is_empty(), "none takes of the core cap of {CORE_BUDGET} bytes");
}

#[tokio::test]
async fn an_ant_is_imported_once_and_a_halted_import_again_without_what_it_wrote() {
    let (_, items) = testing::sense().await;
    let sources = [bd("a", "Alpha bravo charlie delta."), bd("b", "Echo foxtrot golf hotel.")];
    // As an import halted after its first batch: a memory written, no record.
    let first = memories_of(&sources[..1]);
    items.write(&[first[0].create(ItemId::new())], BY).await.unwrap();
    assert_eq!(imported(&*items).await.unwrap(), None);

    let record = import(&*items, &sources, &writer()).await.unwrap().unwrap();
    assert_eq!((record.imported, record.duplicates), (1, 1));
    assert_eq!(imported(&*items).await.unwrap(), Some(record));
    assert_eq!(import(&*items, &[bd("c", "India juliet kilo lima.")], &writer()).await.unwrap(), None, "once");
    assert_eq!(memories(&*items).await.len(), 2);
}

fn memories_of(sources: &[Source]) -> Vec<Memory> {
    sources.iter().flat_map(|source| super::memories(source, &writer())).collect()
}

#[tokio::test]
async fn exact_and_near_duplicates_of_live_memories_of_the_scope_and_of_each_other_are_left_out() {
    let (sense, items) = testing::sense().await;
    let given = json!({"kind": "fact", "title": "Make check", "body": "Run make check before each commit, always."});
    let answer = testing::tool(&sense, remember, given, testing::context(human())).await;
    assert!(!answer.is_error, "{answer:?}");
    let sources = [
        bd("check", "Run make check before each commit, always."),
        bd("check-again", "run   MAKE check before each commit, always!"),
        bd("near", "Run make check before each commit, always, please."),
        file("CLAUDE.md", Scope::Repo("github.com/o/r".into()), "Run make check before each commit, always."),
        bd("one", "Squash merges keep main linear and bisectable."),
        bd("two", "Squash merges keep main linear and bisectable."),
    ];
    let record = import(&*items, &sources, &writer()).await.unwrap().unwrap();
    assert_eq!((record.imported, record.duplicates), (2, 4), "{:?}", memories(&*items).await);
    let titles: Vec<_> = memories(&*items).await.into_iter().map(|memory| memory.title).collect();
    assert_eq!(titles, ["CLAUDE.md", "Make check", "bd memory one"], "another scope is no duplicate");
}

#[tokio::test]
async fn a_long_text_is_split_into_parts_and_the_import_stops_at_its_cap() {
    let paragraph = "word ".repeat(300);
    let long = [paragraph.as_str(); 5].join("\n\n");
    let parts = super::memories(&bd("long", &long), &writer());
    let titles: Vec<_> = parts.iter().map(|memory| memory.title.as_str()).collect();
    let expected: Vec<_> = (1..=3).map(|part| format!("bd memory long (part {part} of 3)")).collect();
    assert_eq!(titles, expected);
    assert!(parts.iter().all(|memory| memory.body.len() <= MAX_BODY_BYTES));
    let unbroken = "x".repeat(MAX_BODY_BYTES * 2 + 10);
    assert_eq!(super::memories(&bd("unbroken", &unbroken), &writer()).len(), 3);

    let (_, items) = testing::sense().await;
    let many: Vec<_> = (0..MAX_IMPORTED + 3)
        .map(|n| bd(&format!("k{n}"), &format!("Fact number {n}: {}", "zulu ".repeat(n % 7))))
        .map(|source| Source { text: format!("{} {}", source.text, uuid::Uuid::new_v4()), ..source })
        .collect();
    let record = import(&*items, &many, &writer()).await.unwrap().unwrap();
    assert_eq!((record.imported, record.over), (MAX_IMPORTED, 3), "{record:?}");
    assert_eq!(memories(&*items).await.len(), MAX_IMPORTED);
}

#[tokio::test]
async fn secrets_are_redacted_as_in_any_memory() {
    let (_, items) = testing::sense().await;
    let source = bd("token", "The deploy token is ghp_abcdefghijklmnopqrstuvwxyz0123456789.");
    import(&*items, &[source], &writer()).await.unwrap();
    let body = &memories(&*items).await[0].body;
    assert!(body.contains("[redacted:github-token]") && !body.contains("ghp_"), "{body}");
}
