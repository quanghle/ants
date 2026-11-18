use std::path::Path;

use ants_proto::{Envelope, ReviewCommit, ThreadId, UserId};

use super::*;
use crate::test_support::{commit, repo, run};

fn review(commits: Vec<ReviewCommit>) -> Envelope {
    let ant: UserId = "dev-a".parse().unwrap();
    let mut envelope = Envelope::new(ant, "sage".parse().unwrap(), "review this");
    envelope.review_commits = commits;
    envelope
}

fn commit_of(repo: &str, id: &str, local: Option<&Path>) -> ReviewCommit {
    ReviewCommit {
        repo: repo.to_owned(),
        commit: id.to_owned(),
        thread: None,
        local: local.map(|path| path.display().to_string()),
    }
}

#[tokio::test]
async fn checks_out_a_commit_from_the_senders_repository_until_dropped() {
    let Some(repo) = repo("checkouts-local") else { return };
    run(&repo, &["remote", "add", "origin", "https://github.com/acme/app.git"]);
    let first = run(&repo, &["rev-parse", "HEAD"]).trim().to_owned();
    std::fs::write(repo.join("sub/file"), "two").unwrap();
    commit(&repo, "second");
    let checkouts = Checkouts::new(&repo.parent().unwrap().join("sage"));
    let envelope = review(vec![commit_of("github.com/acme/app", &first, Some(&repo.join(".git")))]);

    let prepared = checkouts.prepare(&envelope).await;

    let workdir = checkouts.workdir(&envelope.thread_id).expect("the review has a workdir");
    let path = workdir.join(&first[..12]);
    assert_eq!(std::fs::read_to_string(path.join("sub/file")).unwrap(), "one");
    assert!(prepared.note().contains(&format!("github.com/acme/app at {first}, in the workdir: {}", path.display())));
    drop(prepared);
    assert!(!workdir.exists());
    assert_eq!(checkouts.workdir(&envelope.thread_id), None);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_commit_out_of_reach_has_no_checkout_and_the_rest_do() {
    let Some(repo) = repo("checkouts-unreachable") else { return };
    run(&repo, &["remote", "add", "origin", "https://github.com/acme/app.git"]);
    let head = run(&repo, &["rev-parse", "HEAD"]).trim().to_owned();
    let checkouts = Checkouts::new(&repo.parent().unwrap().join("sage"));
    let missing = "0123456789abcdef0123456789abcdef01234567";
    let thread: ThreadId = "019a7d2c-0000-7000-8000-000000000002".parse().unwrap();
    let local = repo.join(".git");
    let envelope = review(vec![
        // Another repository's local path is not fetched from, and `invalid` makes no URL.
        commit_of("invalid", missing, Some(&local)),
        commit_of("github.com/acme/app", "HEAD", Some(&local)),
        ReviewCommit { thread: Some(thread.clone()), ..commit_of("github.com/acme/app", &head, Some(&local)) },
    ]);

    let prepared = checkouts.prepare(&envelope).await;

    let note = prepared.note();
    assert!(note.contains(&format!("invalid at {missing}, in the workdir: no checkout available")), "{note}");
    assert!(note.contains("github.com/acme/app at HEAD, in the workdir: no checkout available, not a full commit id"));
    let path = checkouts.workdir(&envelope.thread_id).unwrap().join(&head[..12]);
    assert!(note.contains(&format!("in the worktree of thread {thread}: {}", path.display())), "{note}");
    assert!(path.join("sub/file").is_file());
    drop(prepared);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_review_naming_no_commits_says_so_and_clear_removes_what_is_left() {
    let Some(repo) = repo("checkouts-none") else { return };
    let checkouts = Checkouts::new(&repo.parent().unwrap().join("sage"));
    let envelope = review(Vec::new());

    let prepared = checkouts.prepare(&envelope).await;

    assert!(prepared.note().contains("None: the review names no commits."));
    assert!(checkouts.workdir(&envelope.thread_id).is_some());
    std::mem::forget(prepared);
    checkouts.clear();
    assert_eq!(checkouts.workdir(&envelope.thread_id), None);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[test]
fn only_plain_remote_names_make_urls() {
    assert!(is_remote_name("github.com/acme/app"));
    assert!(is_remote_name("git.example.org/team/sub/app.v2"));
    for bad in ["app", "github.com/../x", "github.com/acme/", "-x.com/a", "github.com/a b", "host:22/a", "u@h/a"] {
        assert!(!is_remote_name(bad), "{bad}");
    }
}
