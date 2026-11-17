use std::path::PathBuf;

use super::*;
use crate::Workdirs;

struct Repository(PathBuf);

impl Repository {
    fn new() -> Self {
        let repository = Self(ants_test_support::scratch_dir("brief-citations"));
        for arguments in [&["init", "--quiet"][..], &["remote", "add", "origin", "https://github.com/ants/brief"]] {
            let output = std::process::Command::new("git").current_dir(&repository.0).args(arguments).output().unwrap();
            assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        }
        repository
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

impl Workdirs for Repository {
    fn workdir(&self, _context: &CallContext) -> Option<PathBuf> {
        Some(self.0.clone())
    }
}

#[tokio::test]
async fn both_brief_routes_recheck_cited_lines_and_keep_warnings_when_memories_shorten() {
    let repository = Arc::new(Repository::new());
    let source = repository.0.join("source.rs");
    std::fs::write(&source, "original\n").unwrap();
    let (sense, items) = sense().await;
    let sense = sense.with_workdirs(repository.clone());
    let answer = call(
        &sense,
        memory::remember,
        THREAD,
        json!({"kind":"fact","title":"Cited parser behavior","body":"A long documented fact. ".repeat(30),
               "core":true,"citations":["source.rs:1"]}),
    )
    .await;
    let id: ItemId = answer.json["id"].as_str().unwrap().parse().unwrap();
    std::fs::write(&source, "session edit\n").unwrap();
    let session = session_brief(&sense, 8_000).await;
    position(&session, "needs review");
    position(&session, "Warning: github.com/ants/brief:source.rs:1: cited lines changed.");
    std::fs::write(&source, "original\n").unwrap();
    let session = session_brief(&sense, 8_000).await;
    assert!(!session.contains("cited lines changed"), "{session}");
    std::fs::write(&source, "changed\n").unwrap();
    let answer = call(&sense, brief, THREAD, json!({})).await;
    position(&answer.text, "needs review");
    position(&answer.text, "Warning: github.com/ants/brief:source.rs:1: cited lines changed.");
    let saved = MemoryItem::of(items.get(&id).await.unwrap().unwrap()).unwrap();
    assert_eq!(saved.memory.status, Status::NeedsReview);
    assert_eq!(saved.memory.citation_warnings[0].reason, memory::CitationProblem::Changed);
    let sections = sections(&*items, &context(THREAD), None).await.unwrap();
    let total = fit(sections.clone(), usize::MAX).len();
    let shortened = fit(sections, total - 1);
    assert!(!shortened.contains("A long documented fact."), "{shortened}");
    position(&shortened, "cited lines changed");
    std::fs::remove_file(&source).unwrap();
    let answer = call(&sense, brief, THREAD, json!({})).await;
    position(&answer.text, "cited file is missing");
}
