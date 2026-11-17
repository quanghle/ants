//! Test helpers shared by the test suites of this crate: git repositories to check out worktrees of.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A repository with one commit and a `sub` directory, or None when git is missing.
pub(crate) fn repo(name: &str) -> Option<PathBuf> {
    if !ants_test_support::have("git") {
        return None;
    }
    let dir = ants_test_support::scratch_dir(name).canonicalize().unwrap();
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("sub")).unwrap();
    std::fs::write(repo.join("sub/file"), "one").unwrap();
    run(&repo, &["init", "--quiet", "--initial-branch=main"]);
    run(&repo, &["add", "."]);
    commit(&repo, "first");
    Some(repo)
}

pub(crate) fn run(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git").args(args).current_dir(dir).output().unwrap();
    assert!(output.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&output.stderr));
    String::from_utf8(output.stdout).unwrap()
}

pub(crate) fn commit(dir: &Path, message: &str) {
    run(dir, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "--allow-empty", "-am", message]);
}
