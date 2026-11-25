//! A git worktree per thread: made, reused, pruned with its branch, started from HEAD or the remote, by ants sharing
//! a repository at once, registered by hand or by git, made again when cut short, and whether an ant gets them at all.

use std::process::Command;

use super::repository::Changes;
use super::*;
use crate::test_support::{commit, repo, run};

fn ant() -> UserId {
    "dev-a".parse().unwrap()
}

fn thread(name: &str) -> ThreadId {
    name.parse().unwrap()
}

#[tokio::test]
async fn each_thread_gets_its_own_worktree_and_keeps_it() {
    let Some(repo) = repo("worktrees-own") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&ant(), &repo.join("sub"), &home).await.unwrap();
    let a = worktrees.checkout(&thread("a")).await.unwrap();
    let b = worktrees.checkout(&thread("b")).await.unwrap();
    assert_eq!((a.clone(), b.clone()), (home.join("worktrees/a/sub"), home.join("worktrees/b/sub")));
    assert_eq!(std::fs::read_to_string(a.join("file")).unwrap(), "one");
    assert_eq!(run(&a, &["branch", "--show-current"]).trim(), "ants/dev-a/a");
    std::fs::write(a.join("file"), "edited").unwrap();
    assert_eq!(worktrees.checkout(&thread("a")).await.unwrap(), a);
    assert_eq!(std::fs::read_to_string(a.join("file")).unwrap(), "edited", "the worktree is reused");
    assert_eq!(std::fs::read_to_string(b.join("file")).unwrap(), "one");
    assert_eq!(std::fs::read_to_string(repo.join("sub/file")).unwrap(), "one");
    assert_eq!(worktrees.threads(), [thread("a"), thread("b")]);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn expired_threads_lose_their_worktree_and_merged_branch() {
    let Some(repo) = repo("worktrees-prune") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    let (a, b, c) = (thread("a"), thread("b"), thread("c"));
    for t in [&a, &b, &c] {
        worktrees.checkout(t).await.unwrap();
    }
    std::fs::write(home.join("worktrees/a/sub/file"), "uncommitted").unwrap();
    commit(&home.join("worktrees/b"), "work of b");
    let ahead = Branch { thread: b.clone(), name: "ants/dev-a/b".to_owned(), commits: 1, upstream: None };
    assert_eq!(worktrees.ahead(&b).await, Some(ahead.clone()));
    assert_eq!(worktrees.ahead(&a).await, None, "uncommitted work is no commit");
    assert_eq!(worktrees.ahead(&thread("d")).await, None, "no branch");
    let pruned = worktrees.prune(async |t: &ThreadId| *t != c).await;
    assert_eq!(pruned, Pruned { removed: 2, kept: vec![ahead] });
    assert_eq!(worktrees.threads(), std::slice::from_ref(&c));
    let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
    assert_eq!(branches.lines().collect::<Vec<_>>(), ["ants/dev-a/b", "ants/dev-a/c"], "unmerged work is kept");
    assert_eq!(run(&repo, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 2);
    let again = worktrees.checkout(&b).await.unwrap();
    assert_eq!(run(&again, &["log", "-1", "--format=%s"]).trim(), "work of b", "a kept branch is checked out again");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn worktrees_left_behind_are_found_from_their_repository() {
    let Some(repo) = repo("worktrees-left") else { return };
    let home = repo.parent().unwrap().join("home");
    assert!(Worktrees::left_behind(&ant(), &home).await.is_none(), "no worktrees");
    let worktrees = Worktrees::find(&ant(), &repo.join("sub"), &home).await.unwrap();
    let (a, b) = (thread("a"), thread("b"));
    worktrees.checkout(&a).await.unwrap();
    worktrees.checkout(&b).await.unwrap();
    // Not a worktree: skipped when looking for the repository, and never mistaken for a thread of the enclosing one.
    std::fs::remove_file(home.join("worktrees/a/.git")).unwrap();
    let moved = repo.parent().unwrap().join("moved");
    std::fs::rename(&repo, &moved).unwrap();
    run(&moved, &["worktree", "repair", home.join("worktrees/b").to_str().unwrap()]);
    let left = Worktrees::left_behind(&ant(), &home).await.unwrap();
    assert_eq!(left.prune(async |_: &ThreadId| true).await, Pruned { removed: 2, kept: Vec::new() });
    assert!(left.threads().is_empty());
    assert_eq!(run(&moved, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 1);
    let branches = run(&moved, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
    assert_eq!(branches, "", "merged branches are deleted");
    std::fs::remove_dir_all(moved.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_missing_or_broken_worktree_is_created_again() {
    let Some(repo) = repo("worktrees-broken") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    let a = thread("a");
    let path = worktrees.checkout(&a).await.unwrap();
    std::fs::remove_dir_all(&path).unwrap();
    assert_eq!(worktrees.checkout(&a).await.unwrap(), path, "a registered worktree whose directory is gone");
    std::fs::remove_file(path.join(".git")).unwrap();
    assert_eq!(worktrees.checkout(&a).await.unwrap(), path, "a directory that is no worktree");
    assert_eq!(std::fs::read_to_string(path.join("sub/file")).unwrap(), "one");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn ants_sharing_a_repository_check_out_and_prune_at_once() {
    let Some(repo) = repo("worktrees-shared-repo") else { return };
    let root = repo.parent().unwrap();
    let mut all = Vec::new();
    for i in 0..24 {
        let ant: UserId = format!("dev-{i}").parse().unwrap();
        let workdir = if i % 2 == 0 { repo.clone() } else { repo.join("sub") };
        all.push(Worktrees::find(&ant, &workdir, &root.join(format!("home-{i}"))).await.unwrap());
    }
    let threads: Vec<ThreadId> = (0..3).map(|j| thread(&format!("t{j}"))).collect();
    let checkouts = all.iter().flat_map(|w| threads.iter().map(move |t| w.checkout(t)));
    for dir in futures::future::join_all(checkouts).await {
        let dir = dir.unwrap();
        assert!(dir.join("file").is_file() || dir.join("sub/file").is_file(), "{}", dir.display());
    }
    let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
    assert_eq!(branches.lines().count(), 72);
    let pruned = futures::future::join_all(all.iter().map(|w| w.prune(async |_: &ThreadId| true))).await;
    assert!(pruned.iter().all(|p| *p == Pruned { removed: 3, kept: Vec::new() }), "{pruned:?}");
    assert_eq!(run(&repo, &["branch", "--list", "ants/*"]), "");
    assert_eq!(run(&repo, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 1);
    std::fs::remove_dir_all(root).unwrap();
}

/// Make `script` the `post-checkout` hook of `repo`, which git runs at the end of each `git worktree add`.
fn hook(repo: &Path, script: &str) {
    let hook = repo.join(".git/hooks/post-checkout");
    std::fs::write(&hook, format!("#!/bin/sh\n{script}\n")).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}

#[tokio::test]
async fn a_failed_checkout_leaves_no_branch() {
    let Some(repo) = repo("worktrees-failed") else { return };
    let home = repo.parent().unwrap().join("home");
    hook(&repo, "exit 1");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    assert!(worktrees.checkout(&thread("a")).await.is_err());
    assert_eq!(run(&repo, &["branch", "--list", "ants/*"]), "", "the branch the add made is gone");
    assert!(!home.join("worktrees/a").exists());
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn an_add_git_does_not_end_in_time_fails_and_leaves_nothing() {
    let Some(repo) = repo("worktrees-slow") else { return };
    let home = repo.parent().unwrap().join("home");
    hook(&repo, "sleep 30");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    let hurried = Worktrees { add_timeout: Duration::from_secs(2), ..worktrees };
    let e = hurried.checkout(&thread("a")).await.unwrap_err();
    assert!(format!("{e:#}").contains("no answer within 2s"), "{e:#}");
    assert_eq!(run(&repo, &["branch", "--list", "ants/*"]), "", "the branch the add made is gone");
    assert!(!home.join("worktrees/a").exists());
    std::fs::remove_file(repo.join(".git/hooks/post-checkout")).unwrap();
    let worktrees = Worktrees { add_timeout: ADD_TIMEOUT, ..hurried };
    assert_eq!(worktrees.checkout(&thread("a")).await.unwrap(), home.join("worktrees/a"), "tried again later");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn adds_run_beside_each_other_and_any_other_change_alone() {
    use futures::FutureExt as _;
    let changes = Changes::default();
    let adding = futures::future::join_all((0..ADDS_AT_ONCE).map(|_| changes.adding())).await;
    assert!(changes.adding().now_or_never().is_none(), "no more adds at once");
    let mut alone = std::pin::pin!(changes.alone());
    assert!(futures::poll!(&mut alone).is_pending(), "not beside an add");
    drop(adding);
    assert!(changes.adding().now_or_never().is_none(), "an add asked for later waits for the change");
    let held = alone.await;
    assert!(changes.adding().now_or_never().is_none() && changes.alone().now_or_never().is_none());
    drop(held);
    assert!(changes.adding().now_or_never().is_some());
}

#[tokio::test]
async fn ants_of_one_repository_share_its_changes() {
    let Some(repo) = repo("worktrees-one-repository") else { return };
    let root = repo.parent().unwrap();
    let a = Worktrees::find(&ant(), &repo, &root.join("home-a")).await.unwrap();
    let b = Worktrees::find(&"dev-b".parse().unwrap(), &repo.join("sub"), &root.join("home-b")).await.unwrap();
    assert!(Arc::ptr_eq(&a.repository, &b.repository));
    let _alone = a.repository.changes.alone().await;
    let waiting = tokio::time::timeout(Duration::from_millis(300), b.checkout(&thread("t"))).await;
    assert!(waiting.is_err(), "no add beside another ant's removal");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn a_branch_left_without_a_worktree_goes_once_its_thread_has_no_session() {
    let Some(repo) = repo("worktrees-orphans") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    // As left by a daemon killed between git making the branch and the worktree.
    for t in ["a", "b", "c"] {
        run(&repo, &["branch", &format!("ants/dev-a/{t}")]);
    }
    run(&repo, &["branch", "ants/dev-b/a"]);
    run(&repo, &["switch", "--quiet", "ants/dev-a/c"]);
    commit(&repo, "work of c");
    run(&repo, &["switch", "--quiet", "main"]);
    let mut asked = Vec::new();
    let pruned = worktrees
        .prune(async |t: &ThreadId| {
            asked.push(t.clone());
            *t != thread("b")
        })
        .await;
    assert_eq!(pruned, Pruned::default(), "no worktree removed");
    assert_eq!(asked, [thread("a"), thread("b")], "a branch with commits is not taken for an expired thread");
    let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
    assert_eq!(
        branches.lines().collect::<Vec<_>>(),
        ["ants/dev-a/b", "ants/dev-a/c", "ants/dev-b/a"],
        "a's goes; b's thread has a session, c's holds a commit, and another ant's is not this ant's"
    );
    let path = worktrees.checkout(&thread("b")).await.unwrap();
    assert_eq!(run(&path, &["branch", "--show-current"]).trim(), "ants/dev-a/b", "the branch is checked out");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_workdir_outside_a_work_tree_is_shared() {
    if !ants_test_support::have("git") {
        return;
    }
    let dir = ants_test_support::scratch_dir("worktrees-bare").canonicalize().unwrap();
    run(&dir, &["init", "--quiet", "--bare"]);
    assert!(Worktrees::find(&ant(), &dir, &dir.join("home")).await.is_none());
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_repository_without_commits_is_shared_until_it_has_one() {
    if !ants_test_support::have("git") {
        return;
    }
    let dir = ants_test_support::scratch_dir("worktrees-empty").canonicalize().unwrap();
    let (repo, home) = (dir.join("repo"), dir.join("home"));
    std::fs::create_dir_all(repo.join("sub")).unwrap();
    std::fs::write(repo.join("sub/file"), "one").unwrap();
    run(&repo, &["init", "--quiet", "--initial-branch=main"]);
    let top = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    let below = Worktrees::find(&"dev-b".parse().unwrap(), &repo.join("sub"), &dir.join("home-b")).await.unwrap();
    let (a, b, c) = (thread("a"), thread("b"), thread("c"));
    assert_eq!(top.checkout(&a).await.unwrap(), repo, "an unborn HEAD");
    assert_eq!(below.checkout(&a).await.unwrap(), repo.join("sub"), "below the top level");
    assert!(!home.join("worktrees").exists() && run(&repo, &["branch", "--list", "ants/*"]).is_empty());
    assert_eq!(top.prune(async |_: &ThreadId| true).await, Pruned::default());

    run(&repo, &["add", "."]);
    commit(&repo, "first");
    assert_eq!(top.checkout(&b).await.unwrap(), home.join("worktrees/b"), "a new thread, once there is a commit");
    assert_eq!(top.checkout(&c).await.unwrap(), home.join("worktrees/c"));
    assert_eq!(top.threads(), [b, c]);
    assert_eq!(below.checkout(&thread("d")).await.unwrap(), dir.join("home-b/worktrees/d/sub"));
    assert_eq!(top.checkout(&a).await.unwrap(), home.join("worktrees/a"), "only its stored session keeps it out");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn worktrees_default_on_for_concurrent_threads_in_a_git_workdir() {
    let Some(repo) = repo("worktrees-default") else { return };
    let sub = repo.join("sub");
    let outside = std::env::temp_dir();
    assert!(per_thread(&sub, 2, None).await, "unset, concurrent threads in a git workdir");
    assert!(!per_thread(&sub, 1, None).await, "unset, one thread at a time");
    assert!(!per_thread(Path::new("/"), 4, None).await, "unset, no git work tree");
    assert!(!per_thread(&sub, 2, Some(false)).await, "turned off");
    assert!(per_thread(&sub, 1, Some(true)).await && per_thread(&outside, 1, Some(true)).await, "turned on");
    std::fs::create_dir(repo.join("build")).unwrap();
    std::fs::write(repo.join(".gitignore"), "/build\n").unwrap();
    assert!(!per_thread(&repo.join("build"), 2, None).await, "unset, a workdir git ignores");
    let scratch = ants_test_support::scratch_dir("worktrees-default-scratch");
    assert!(!per_thread(&scratch, 4, None).await, "unset, the scratch dirs of tests in ants' ignored target/");
    std::fs::remove_dir_all(scratch).unwrap();
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[test]
fn concurrent_threads_share_a_git_workdir_only_with_worktrees_turned_off() {
    let dir = ants_test_support::scratch_dir("worktrees-shared");
    std::fs::create_dir_all(dir.join(".git")).unwrap();
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    assert!(shares_git_workdir(&dir.join("sub"), 2, Some(false)));
    assert!(!shares_git_workdir(&dir.join("sub"), 2, None));
    assert!(!shares_git_workdir(&dir.join("sub"), 2, Some(true)));
    assert!(!shares_git_workdir(&dir.join("sub"), 1, Some(false)));
    std::fs::remove_dir(dir.join(".git")).unwrap();
    std::fs::write(dir.join(".git"), "gitdir: elsewhere").unwrap();
    assert!(shares_git_workdir(&dir, 4, Some(false)), "a worktree's .git is a file");
    assert!(!shares_git_workdir(Path::new("/"), 4, Some(false)));
    std::fs::remove_dir_all(dir).unwrap();
}

/// A bare repository `remote.git` beside `repo`, its `origin` with `main` pushed, the remote's `HEAD` set unless
/// `head` is false, and a clone `other` of it.
fn with_remote(repo: &Path, head: bool) -> (PathBuf, PathBuf) {
    let dir = repo.parent().unwrap();
    let remote = dir.join("remote.git");
    run(dir, &["init", "--quiet", "--bare", "--initial-branch=main", remote.to_str().unwrap()]);
    run(repo, &["remote", "add", "origin", remote.to_str().unwrap()]);
    if head {
        run(repo, &["push", "--quiet", "origin", "main"]);
        run(repo, &["remote", "set-head", "origin", "main"]);
    } else {
        // Since git 2.48 a fetch otherwise sets the remote's HEAD.
        run(repo, &["config", "remote.origin.followRemoteHEAD", "never"]);
        run(repo, &["push", "--quiet", "origin", "main"]);
    }
    run(dir, &["clone", "--quiet", remote.to_str().unwrap(), dir.join("other").to_str().unwrap()]);
    (remote, dir.join("other"))
}

#[tokio::test]
async fn an_upstream_base_starts_the_branch_from_the_fetched_remote_default_branch() {
    let Some(repo) = repo("worktrees-upstream") else { return };
    let home = repo.parent().unwrap().join("home");
    let (_, other) = with_remote(&repo, true);
    commit(&other, "upstream work");
    run(&other, &["push", "--quiet", "origin", "main"]);
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap().with_base(Base::Upstream);
    assert_eq!(worktrees.remote().await.as_deref(), Some("origin"));
    let (a, b) = (thread("a"), thread("b"));
    let path = worktrees.checkout(&a).await.unwrap();
    assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "upstream work");
    assert_eq!(run(&repo, &["log", "-1", "--format=%s"]).trim(), "first", "the workdir's checkout is untouched");
    assert!(
        Command::new("git")
            .args(["config", "branch.ants/dev-a/a.remote"])
            .current_dir(&repo)
            .status()
            .is_ok_and(|s| !s.success())
    );
    assert_eq!(worktrees.ahead(&a).await, None, "the upstream commit is no commit of the thread");
    commit(&path, "work of a");
    let ahead = worktrees.ahead(&a).await.unwrap();
    assert_eq!((ahead.commits, ahead.upstream.as_deref()), (1, Some("origin/main")));
    assert_eq!(ahead.outside(), "the repository's HEAD or origin/main");
    worktrees.checkout(&b).await.unwrap();
    let pruned = worktrees.prune(async |_: &ThreadId| true).await;
    assert_eq!(pruned, Pruned { removed: 2, kept: vec![ahead] });
    let branches = run(&repo, &["branch", "--list", "ants/*", "--format=%(refname:short)"]);
    assert_eq!(branches.trim(), "ants/dev-a/a", "a branch with only upstream commits is deleted");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn ants_sharing_a_repository_fetch_one_at_a_time() {
    let Some(repo) = repo("worktrees-upstream-shared") else { return };
    let root = repo.parent().unwrap().to_owned();
    let (_, other) = with_remote(&repo, true);
    commit(&other, "upstream work");
    run(&other, &["push", "--quiet", "origin", "main"]);
    let mut all = Vec::new();
    for i in 0..16 {
        let ant: UserId = format!("dev-{i}").parse().unwrap();
        let home = root.join(format!("home-{i}"));
        all.push(Worktrees::find(&ant, &repo, &home).await.unwrap().with_base(Base::Upstream));
    }
    let t = thread("t");
    let paths = futures::future::join_all(all.iter().map(|w| w.checkout(&t))).await;
    for path in paths {
        let path = path.unwrap();
        assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "upstream work", "{}", path.display());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn an_upstream_base_falls_back_to_head() {
    let Some(repo) = repo("worktrees-upstream-fallback") else { return };
    let home = repo.parent().unwrap().join("home");
    commit(&repo, "local work");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap().with_base(Base::Upstream);
    let path = worktrees.checkout(&thread("a")).await.unwrap();
    assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "local work", "no remote");

    let (remote, other) = with_remote(&repo, false);
    commit(&other, "upstream work");
    run(&other, &["push", "--quiet", "origin", "main"]);
    let path = worktrees.checkout(&thread("b")).await.unwrap();
    let shown = "the remote has no HEAD and main no upstream";
    assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "local work", "{shown}");

    run(&repo, &["branch", "--quiet", "--set-upstream-to=origin/main"]);
    let path = worktrees.checkout(&thread("c")).await.unwrap();
    assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "upstream work", "main's upstream instead");

    std::fs::remove_dir_all(&remote).unwrap();
    let path = worktrees.checkout(&thread("d")).await.unwrap();
    assert_eq!(run(&path, &["log", "-1", "--format=%s"]).trim(), "local work", "the fetch fails");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_worktree_registered_by_hand_is_one_to_git() {
    let Some(repo) = repo("worktrees-by-hand") else { return };
    let home = repo.parent().unwrap().join("home");
    let log = repo.parent().unwrap().join("hook.log");
    hook(&repo, &format!("echo \"$* $(pwd)\" >> '{}'", log.display()));
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    assert!(worktrees.by_hand);
    let path = worktrees.checkout(&thread("a")).await.unwrap();
    worktrees.checkout(&thread("b")).await.unwrap();
    let head = run(&repo, &["rev-parse", "HEAD"]);
    let zero = "0".repeat(head.trim().len());
    assert_eq!(
        std::fs::read_to_string(&log).unwrap().lines().next().unwrap(),
        format!("{zero} {} 1 {}", head.trim(), path.display()),
        "the post-checkout hook runs as for git worktree add"
    );
    assert_eq!(run(&path, &["status", "--porcelain"]), "", "checked out");
    assert_eq!(run(&path, &["rev-parse", "--show-toplevel"]).trim(), path.to_str().unwrap());
    let listed = run(&repo, &["worktree", "list", "--porcelain"]);
    let entry = format!("worktree {}\nHEAD {}branch refs/heads/ants/dev-a/a\n", path.display(), head);
    assert!(listed.contains(&entry), "{listed}");
    assert!(!listed.contains("locked") && !listed.contains("prunable"), "{listed}");
    run(&repo, &["worktree", "prune"]);
    assert_eq!(run(&repo, &["worktree", "list", "--porcelain"]), listed, "a prune keeps it");
    run(&repo, &["worktree", "remove", path.to_str().unwrap()]);
    assert!(!path.exists());
    std::fs::remove_dir_all(home.join("worktrees/b")).unwrap();
    assert!(run(&repo, &["worktree", "list", "--porcelain"]).contains("prunable"));
    run(&repo, &["worktree", "prune"]);
    assert_eq!(run(&repo, &["worktree", "list", "--porcelain"]).matches("worktree ").count(), 1);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_worktree_cut_short_while_made_is_made_again() {
    let Some(repo) = repo("worktrees-cut-short") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    let a = thread("a");
    let path = worktrees.checkout(&a).await.unwrap();
    // As left by a daemon killed before the worktree was unlocked: no .git file yet, which git would not add again.
    std::fs::remove_file(path.join(".git")).unwrap();
    std::fs::write(repo.join(".git/worktrees/a/locked"), "initializing").unwrap();
    assert!(run(&repo, &["worktree", "list", "--porcelain"]).contains("locked initializing"));
    assert_eq!(worktrees.checkout(&a).await.unwrap(), path);
    assert_eq!(run(&path, &["branch", "--show-current"]).trim(), "ants/dev-a/a");
    let listed = run(&repo, &["worktree", "list", "--porcelain"]);
    assert_eq!(listed.matches("worktree ").count(), 2, "{listed}");
    assert!(!listed.contains("locked"), "{listed}");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_sparse_checkout_gets_its_worktrees_from_git() {
    let Some(repo) = repo("worktrees-sparse") else { return };
    let home = repo.parent().unwrap().join("home");
    std::fs::create_dir(repo.join("other")).unwrap();
    std::fs::write(repo.join("other/file"), "two").unwrap();
    run(&repo, &["add", "."]);
    commit(&repo, "other");
    run(&repo, &["sparse-checkout", "set", "sub"]);
    let worktrees = Worktrees::find(&ant(), &repo, &home).await.unwrap();
    assert!(!worktrees.by_hand, "git worktree add copies the sparse checkout");
    let path = worktrees.checkout(&thread("a")).await.unwrap();
    assert!(path.join("sub/file").is_file() && !path.join("other").exists(), "sparse as the repository");
    assert_eq!(run(&path, &["branch", "--show-current"]).trim(), "ants/dev-a/a");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}
