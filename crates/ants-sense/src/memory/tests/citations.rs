use std::path::{Path, PathBuf};
use std::process::Command;

use super::*;
use crate::Workdirs;

struct Repository {
    dir: PathBuf,
}

impl Repository {
    fn new() -> Self {
        let dir = ants_test_support::scratch_dir("sense-citations");
        let repository = Self { dir };
        repository.git(&["init", "--quiet"]);
        repository.git(&["remote", "add", "origin", "git@github.com:ants/citations.git"]);
        repository
    }

    fn git(&self, arguments: &[&str]) {
        let output = Command::new("git").current_dir(&self.dir).args(arguments).output().unwrap();
        assert!(output.status.success(), "git {arguments:?}: {}", String::from_utf8_lossy(&output.stderr));
    }

    fn write(&self, path: &str, body: impl AsRef<[u8]>) {
        let file = self.dir.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, body).unwrap();
    }

    fn sense(&self, sense: Sense) -> Sense {
        sense.with_workdirs(Arc::new(Workdir(self.dir.clone())))
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

struct Workdir(PathBuf);

impl Workdirs for Workdir {
    fn workdir(&self, _context: &CallContext) -> Option<PathBuf> {
        Some(self.0.clone())
    }
}

fn cited(citations: Value) -> Value {
    json!({"kind": "fact", "title": "Cited source", "body": "The checked source establishes a fact.", "citations": citations})
}

async fn recheck(store: &Kept, root: &Path) -> CitationRecheck {
    recheck_citations(store, root).await.unwrap()
}

#[tokio::test]
async fn remember_records_the_real_repository_and_exact_lines_for_strings_and_objects() {
    let repository = Repository::new();
    let contents = "first\r\nβeta\r\nlast";
    repository.write("src/lib.rs", contents);
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let mut context = context(human());
    context.repo = Some("github.com/not/the-caller".into());
    let answer = tool(
        &sense,
        remember,
        json!({
            "kind": "fact", "title": "Exact lines", "body": "Both citation forms refer to identical bytes.",
            "scope": "repo:github.com/not/the-caller",
            "citations": [
                "./src/lib.rs:2",
                {"path": "src/lib.rs", "start": 2, "hash": crate::log::sha256("βeta\r\n".as_bytes()).to_uppercase()},
                "src/lib.rs"
            ],
        }),
        context,
    )
    .await;
    let memory = memory(&store, &id_of(&answer)).await;
    assert_eq!(memory.status, Status::Active);
    assert!(memory.citation_warnings.is_empty());
    assert_eq!(memory.citations[0], memory.citations[1]);
    let citation = &memory.citations[0];
    assert_eq!(citation.path, "src/lib.rs");
    assert_eq!((citation.start, citation.end), (Some(2), Some(2)));
    assert_eq!(citation.remote.as_deref(), Some("github.com/ants/citations"));
    assert_eq!(citation.hash, Some(crate::log::sha256("βeta\r\n".as_bytes())));
    assert_eq!((memory.citations[2].start, memory.citations[2].end), (Some(1), Some(3)));
    assert_eq!(memory.citations[2].hash, Some(crate::log::sha256(contents.as_bytes())));
    assert_eq!(answer.json["citations"], json!(memory.citations));
}

#[tokio::test]
async fn linked_worktrees_read_the_common_remote_but_hash_only_their_own_files() {
    let repository = Repository::new();
    repository.git(&["remote", "rename", "origin", "upstream"]);
    repository.write(".git/worktrees/linked/commondir", "../..\n");
    repository.write("linked/.git", "gitdir: ../.git/worktrees/linked\n");
    repository.write("linked/source.rs", "worktree bytes\n");
    repository.write("source.rs", "main repository bytes\n");
    let (sense, store) = sense().await;
    let sense = sense.with_workdirs(Arc::new(Workdir(repository.dir.join("linked"))));
    let id = ok(&sense, cited(json!(["source.rs:1"]))).await;
    let memory = memory(&store, &id).await;
    assert_eq!(memory.citations[0].remote.as_deref(), Some("github.com/ants/citations"));
    assert_eq!(memory.citations[0].hash, Some(crate::log::sha256(b"worktree bytes\n")));
    assert_eq!(recheck(store.as_ref(), &repository.dir.join("linked")).await.unchanged, 1);
}

#[tokio::test]
async fn unchanged_lines_do_not_write_and_changed_lines_preserve_the_baseline_with_a_warning() {
    let repository = Repository::new();
    repository.write("source.rs", "before\nimportant\n");
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let id = ok(&sense, cited(json!(["source.rs:2"]))).await;
    let baseline = memory(&store, &id).await.citations;
    let before = events(&store).await.len();
    repository.write("source.rs", "unrelated edit\nimportant\n");
    std::fs::create_dir(repository.dir.join("nested")).unwrap();
    let same = recheck(store.as_ref(), &repository.dir.join("nested")).await;
    assert_eq!((same.checked, same.unchanged, same.marked), (1, 1, 0));
    assert!(same.needs_review.is_empty());
    assert_eq!(events(&store).await.len(), before, "unchanged cited lines append nothing");

    repository.write("source.rs", "unrelated edit\ndifferent\n");
    let changed = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!((changed.checked, changed.unchanged, changed.marked), (1, 0, 1));
    assert_eq!(changed.needs_review.as_slice(), std::slice::from_ref(&id));
    let memory = memory(&store, &id).await;
    assert_eq!(memory.status, Status::NeedsReview);
    assert_eq!(memory.citations, baseline);
    assert_eq!(memory.citation_warnings, [CitationWarning { citation: 0, reason: CitationProblem::Changed }]);
    let after = events(&store).await.len();
    assert_eq!(after, before + 1);
    let again = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!(again.marked, 0);
    assert_eq!(again.needs_review.as_slice(), std::slice::from_ref(&id));
    assert_eq!(events(&store).await.len(), after, "an existing warning is not repeatedly written");
    repository.write("source.rs", "unrelated edit\nimportant\n");
    let restored = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!(restored.unchanged, 1);
    let saved = super::memory(&store, &id).await;
    assert_eq!(saved.status, Status::NeedsReview, "restoring a file does not vote a memory helpful");
    assert!(saved.citation_warnings.is_empty());
}

#[tokio::test]
async fn recall_rechecks_before_ranking_or_showing_a_memory_and_explains_the_warning() {
    let repository = Repository::new();
    repository.write("source.rs", "original\n");
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let id = ok(&sense, cited(json!(["source.rs:1"]))).await;
    repository.write("source.rs", "replacement\n");
    let answer = tool(&sense, recall, json!({}), context(human())).await;
    assert!(!answer.is_error, "{answer:?}");
    assert_eq!(answer.json["memories"][0]["status"], "needs_review");
    assert!(answer.text.contains("cited lines changed") && answer.text.contains("needs review"), "{}", answer.text);
    assert_eq!(memory(&store, &id).await.status, Status::NeedsReview);

    let answer = tool(&sense, recall, json!({"id": id}), context(human())).await;
    assert_eq!(answer.json["memory"]["citation_warnings"][0]["reason"], "changed");
    assert!(answer.text.contains("Warning: github.com/ants/citations:source.rs:1: cited lines changed."));
}

#[tokio::test]
async fn supplied_hashes_are_compared_and_other_repositories_are_not_mistaken_for_the_caller() {
    let repository = Repository::new();
    repository.write("source.rs", "present\n");
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let baseline = "ab".repeat(32);
    let answer = call(
        &sense,
        human(),
        cited(json!([
            {"path": "source.rs", "start": 1, "hash": baseline},
            {"path": "source.rs", "start": 1, "remote": "https://elsewhere.org/other/repo", "hash": baseline}
        ])),
    )
    .await;
    let id = id_of(&answer);
    let memory = memory(&store, &id).await;
    assert_eq!(memory.status, Status::NeedsReview);
    assert_eq!(memory.citations[0].hash.as_deref(), Some(baseline.as_str()));
    assert_eq!(memory.citations[1].remote.as_deref(), Some("elsewhere.org/other/repo"));
    assert_eq!(memory.citations[1].hash.as_deref(), Some(baseline.as_str()));
    assert_eq!(memory.citation_warnings, [CitationWarning { citation: 0, reason: CitationProblem::Changed }]);
    assert!(answer.text.contains("cited lines changed") && answer.text.contains("needs_review"));
    let checked = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!((checked.checked, checked.skipped), (1, 1));
}

#[tokio::test]
async fn missing_files_and_shortened_ranges_need_review_without_affecting_retired_memories() {
    let repository = Repository::new();
    repository.write("source.rs", "one\ntwo\n");
    repository.write("retired.rs", "still cited\n");
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let id = ok(&sense, cited(json!(["source.rs:2"]))).await;
    let mut retired = cited(json!(["retired.rs:1"]));
    retired["body"] = json!("An independent finding later voted wrong.");
    let retired = ok(&sense, retired).await;
    Items::change(store.as_ref(), &retired, json!({"status": "wrong"}).as_object().unwrap().clone(), None, "test")
        .await
        .unwrap();
    repository.write("source.rs", "one\n");
    std::fs::remove_file(repository.dir.join("retired.rs")).unwrap();
    let checked = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!(checked.checked, 1);
    assert_eq!(memory(&store, &id).await.citation_warnings[0].reason, CitationProblem::InvalidRange);
    assert!(memory(&store, &retired).await.citation_warnings.is_empty());
    assert_eq!(memory(&store, &retired).await.status, Status::Wrong);
    std::fs::remove_file(repository.dir.join("source.rs")).unwrap();
    recheck(store.as_ref(), &repository.dir).await;
    assert_eq!(memory(&store, &id).await.citation_warnings[0].reason, CitationProblem::Missing);
}

#[tokio::test]
async fn unavailable_content_does_not_invent_ranges_or_hashes() {
    let repository = Repository::new();
    repository.write("binary", [0xff]);
    repository.write("empty", "");
    repository.write("one-line", "one\n");
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let id = ok(
        &sense,
        cited(json!(["missing", "binary", "empty", "one-line:2", "one-line:1-2", {"path": "missing", "start": 1, "hash": "ab".repeat(32)}])),
    )
    .await;
    let memory = memory(&store, &id).await;
    assert_eq!(memory.status, Status::NeedsReview);
    assert_eq!(
        memory.citation_warnings.iter().map(|warning| warning.reason).collect::<Vec<_>>(),
        [
            CitationProblem::Missing,
            CitationProblem::NotUtf8,
            CitationProblem::InvalidRange,
            CitationProblem::InvalidRange,
            CitationProblem::InvalidRange,
            CitationProblem::Missing,
        ]
    );
    assert!(memory.citations[..5].iter().all(|citation| citation.hash.is_none()));
    assert!(memory.citations[..3].iter().all(|citation| citation.start.is_none() && citation.end.is_none()));
    assert_eq!(memory.citations[5].hash, Some("ab".repeat(32)));
}

#[tokio::test]
async fn malformed_ranges_and_unsafe_paths_are_refused_before_any_file_access() {
    let (sense, store) = sense().await;
    for citation in [
        json!("../outside.rs:1"),
        json!("/outside.rs:1"),
        json!("src/../../outside.rs"),
        json!("C:\\outside.rs"),
        json!({"path": "bad\u{0}path", "start": 1}),
        json!("source.rs:0"),
        json!("source.rs:3-1"),
        json!("source.rs:1-"),
        json!("source.rs:4294967296"),
        json!({"path": "source.rs", "end": 1}),
    ] {
        let answer = refused(&sense, cited(json!([citation]))).await;
        assert_eq!(answer.json["error"], "invalid", "{citation}");
    }
    assert!(events(&store).await.is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_outside_the_repository_never_receive_a_hash() {
    let repository = Repository::new();
    let outside = Repository::new();
    outside.write("secret", "outside content\n");
    repository.write("tracked", "original\n");
    std::os::unix::fs::symlink(outside.dir.join("secret"), repository.dir.join("escape")).unwrap();
    let (sense, store) = sense().await;
    let sense = repository.sense(sense);
    let id = ok(&sense, cited(json!(["escape:1", "tracked:1"]))).await;
    let memory = memory(&store, &id).await;
    assert_eq!(memory.status, Status::NeedsReview);
    assert!(memory.citations[0].hash.is_none());
    assert_eq!(memory.citation_warnings[0].reason, CitationProblem::OutsideRoot);
    std::fs::remove_file(repository.dir.join("tracked")).unwrap();
    std::os::unix::fs::symlink(outside.dir.join("secret"), repository.dir.join("tracked")).unwrap();
    let summary = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!(summary.checked, 2);
    let saved = super::memory(&store, &id).await;
    assert_eq!(saved.citation_warnings.len(), 2);
    assert_eq!(saved.citation_warnings[1].reason, CitationProblem::OutsideRoot);
    assert_eq!(saved.citations[1].hash, Some(crate::log::sha256(b"original\n")));
}

#[tokio::test]
async fn recheck_validates_legacy_or_replicated_citations_without_trusting_their_paths_or_ranges() {
    let repository = Repository::new();
    repository.write("source.rs", "one\n");
    let (_sense, store) = sense().await;
    let mut saved = Memory::new(
        MemoryKind::Fact,
        Scope::Own,
        "Legacy citations".into(),
        "Replicated values need validation when read from disk.".into(),
        &context(human()),
    );
    saved.citations = [
        ("../outside", Some(1), Some(1), "ab".repeat(32)),
        ("source.rs", Some(0), Some(1), "ab".repeat(32)),
        ("source.rs", Some(1), Some(1), "not-a-hash".into()),
    ]
    .into_iter()
    .map(|(path, start, end, hash)| Citation {
        path: path.into(),
        start,
        end,
        remote: Some("github.com/ants/citations".into()),
        hash: Some(hash),
    })
    .collect();
    let id = ItemId::new();
    Items::write(store.as_ref(), &[saved.create(id.clone())], "test").await.unwrap();
    let summary = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!((summary.checked, summary.marked), (3, 1));
    let saved = memory(&store, &id).await;
    assert_eq!(
        saved.citation_warnings.iter().map(|warning| warning.reason).collect::<Vec<_>>(),
        [CitationProblem::InvalidPath, CitationProblem::InvalidRange, CitationProblem::InvalidHash]
    );
}

#[tokio::test]
async fn baselines_are_not_guessed_without_a_workdir_or_remote() {
    let repository = Repository::new();
    repository.write("source.rs", "available\n");
    let (sense, store) = sense().await;
    let id = ok(&sense, cited(json!(["source.rs:1"]))).await;
    let saved = memory(&store, &id).await;
    assert!(saved.citations[0].hash.is_none() && saved.citations[0].remote.is_none());
    let summary = recheck(store.as_ref(), &repository.dir).await;
    assert_eq!((summary.checked, summary.skipped, summary.marked), (0, 1, 0));
    assert_eq!(memory(&store, &id).await.status, Status::Active);
    repository.git(&["remote", "remove", "origin"]);
    let sense = repository.sense(sense);
    let mut no_remote = cited(json!(["source.rs:1"]));
    no_remote["body"] = json!("A separate memory in a repository without a remote.");
    let id = ok(&sense, no_remote).await;
    let saved = memory(&store, &id).await;
    assert!(saved.citations[0].hash.is_none() && saved.citations[0].remote.is_none());
}
