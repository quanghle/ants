//! Tests of the import of an ant's harness memories into its sense data: the files of its workdir as the repository's
//! memories, those under the home directory as its own, its bd memories, and one import an ant.

use ants_proto::SenseId;
use ants_sense::memory::import::imported;
use ants_sense::memory::{MEMORY, MemoryItem, Trust};
use ants_sense::testing::SharedLog;

use super::*;

fn ant() -> UserId {
    "dev-imports".parse().unwrap()
}

async fn memories(items: &dyn Items) -> Vec<ants_sense::memory::Memory> {
    let found = items.list(MEMORY, usize::MAX).await.unwrap();
    let mut found: Vec<_> = found.into_iter().filter_map(MemoryItem::of).map(|found| found.memory).collect();
    found.sort_by(|a, b| a.title.cmp(&b.title));
    found
}

#[cfg(unix)]
#[tokio::test]
async fn the_memory_files_and_bd_memories_are_imported_once_with_the_scope_of_where_they_lie() {
    let root = ants_test_support::scratch_dir("sense-imports").canonicalize().unwrap();
    let (home, workdir) = (root.join("home"), root.join("home/src/repo"));
    std::fs::create_dir_all(workdir.join(".git")).unwrap();
    std::fs::create_dir_all(home.join(".codex")).unwrap();
    std::fs::write(workdir.join(".git/config"), "[remote \"origin\"]\n\turl = git@github.com:o/r.git\n").unwrap();
    std::fs::write(workdir.join("AGENTS.md"), "# Checks\n\nRun the checks before each commit.\n").unwrap();
    std::fs::write(home.join(".codex/AGENTS.md"), "Answer in short sentences.\n").unwrap();
    let bd = root.join("bd");
    let script = "#!/bin/sh\n[ \"$1 $2\" = 'memories --json' ] || exit 2\n\
                  printf '[{\"key\":\"deploy\",\"content\":\"Deploy from %s.\",\"revision\":1}]' \"$BD_DB\"\n";
    ants_test_support::executable(&bd, script);
    let from = HarnessMemories::new(&["AGENTS.md", "~/.codex/AGENTS.md"], &workdir, &home, (&bd, Some("/srv/bd.db")));
    let items = ants_sense::testing::store(SharedLog::default(), SenseId::new(), ant()).await;

    import(&items, &from, &ant(), "m1").await.unwrap();
    let repo = Scope::Repo("github.com/o/r".into());
    let found: Vec<_> = memories(&items).await.into_iter().map(|m| (m.title, m.scope, m.imported, m.trust)).collect();
    assert_eq!(
        found,
        [
            (
                "AGENTS.md: Checks".to_owned(),
                repo.clone(),
                Some(Imported::File { path: "AGENTS.md".into() }),
                Trust::Untrusted
            ),
            (
                "bd memory deploy".to_owned(),
                repo,
                Some(Imported::Bd { db: "/srv/bd.db".into(), key: "deploy".into() }),
                Trust::Untrusted
            ),
            (
                "~/.codex/AGENTS.md".to_owned(),
                Scope::Own,
                Some(Imported::File { path: "~/.codex/AGENTS.md".into() }),
                Trust::Untrusted
            ),
        ]
    );
    let record = imported(&items).await.unwrap().expect("the import is recorded");
    assert_eq!((record.sources, record.imported), (3, 3));

    std::fs::write(workdir.join("AGENTS.md"), "# Later\n\nA rule written after the import.\n").unwrap();
    import(&items, &from, &ant(), "m1").await.unwrap();
    assert_eq!(memories(&items).await.len(), 3, "an ant is imported once");
}
