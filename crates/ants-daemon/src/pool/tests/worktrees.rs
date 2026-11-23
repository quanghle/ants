//! Each thread works in a worktree of its own where the workdir is a repository, a review among its checkouts.

use super::*;

#[tokio::test]
async fn each_thread_works_in_its_own_worktree_until_its_session_expires() {
    let Some(repo) = crate::test_support::repo("pool-worktrees") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees(worktrees);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    pool.evict_idle(Instant::now() + IDLE * 2);
    pool.prompt(&thread(1), "c").await.unwrap();
    let (t1, t2) = (home.join("worktrees/t1"), home.join("worktrees/t2"));
    assert_eq!(log.locked().cwds, [t1.clone(), t2.clone(), t1.clone()], "session/load in the same worktree");
    crate::test_support::commit(&t2, "work of t2");
    assert_eq!(pool.branch_ahead(&thread(1)).await, None);
    assert_eq!(pool.branch_ahead(&thread(2)).await.map(|b| (b.name, b.commits)), Some(("ants/dev-a/t2".to_owned(), 1)));
    assert_eq!(pool.prune_worktrees().await.removed, 0, "sessions still stored");
    let SessionStore::Memory(sessions) = &store else { unreachable!() };
    sessions.locked().clear();
    let pruned = pool.prune_worktrees().await;
    assert_eq!(pruned.removed, 1, "only the thread without a live harness");
    assert_eq!(pruned.kept.iter().map(|b| &b.thread).collect::<Vec<_>>(), [&thread(2)], "its branch holds a commit");
    assert!(t1.is_dir() && !t2.exists());
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn worktrees_left_behind_are_pruned_while_threads_share_the_workdir() {
    let Some(repo) = crate::test_support::repo("pool-worktrees-off") else { return };
    let home = repo.parent().unwrap().join("home");
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    worktrees.checkout(&thread(1)).await.unwrap();
    worktrees.checkout(&thread(2)).await.unwrap();
    let (launch, log, _) = fake::launch();
    let store = SessionStore::memory();
    let left = Worktrees::left_behind(&"dev-a".parse().unwrap(), &home).await.unwrap();
    let pool = Pool::new(launch.in_dir(repo.clone()), store.clone(), 4, IDLE).with_worktrees_left_behind(left);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(3), "b").await.unwrap();
    assert_eq!(log.locked().cwds, [repo.clone(), repo.clone()], "threads share the workdir");
    crate::test_support::commit(&home.join("worktrees/t1"), "work of t1");
    assert_eq!(pool.branch_ahead(&thread(1)).await, None, "the thread works in the workdir");
    assert_eq!(pool.prune_worktrees().await.removed, 1, "only the thread with no session and no live harness");
    let SessionStore::Memory(sessions) = &store else { unreachable!() };
    sessions.locked().clear();
    pool.evict_idle(Instant::now() + IDLE * 2);
    assert_eq!(pool.prune_worktrees().await.removed, 1);
    assert!(!home.join("worktrees/t1").exists() && !home.join("worktrees/t3").exists());
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_thread_whose_worktree_fails_waits_rather_than_share_the_workdir() {
    let Some(repo) = crate::test_support::repo("pool-worktrees-fail") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    crate::test_support::run(&repo, &["branch", "ants/dev-a/t1"]);
    crate::test_support::run(&repo, &["switch", "--quiet", "ants/dev-a/t1"]);
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 4, IDLE).with_worktrees(worktrees);
    pool.prompt(&thread(2), "b").await.unwrap();
    let e = pool.prompt(&thread(1), "a").await.unwrap_err();
    assert!(e.is::<WorktreeFailed>(), "its branch is checked out elsewhere: {e:#}");
    assert_eq!(log.locked().cwds, [home.join("worktrees/t2")], "no harness started in the shared workdir");
    assert_eq!(pool.live(), 1, "the failed start holds no slot");
    crate::test_support::run(&repo, &["switch", "--quiet", "main"]);
    assert_eq!(pool.prompt(&thread(1), "a").await.unwrap(), "s2: a", "tried again, in its own worktree");
    assert_eq!(log.locked().cwds, [home.join("worktrees/t2"), home.join("worktrees/t1")]);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn threads_get_worktrees_once_the_repository_has_a_commit_and_those_sharing_the_workdir_stay() {
    if !ants_test_support::have("git") {
        return;
    }
    let dir = ants_test_support::scratch_dir("pool-worktrees-unborn").canonicalize().unwrap();
    let (repo, home) = (dir.join("repo"), dir.join("home"));
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("file"), "one").unwrap();
    crate::test_support::run(&repo, &["init", "--quiet", "--initial-branch=main"]);
    let (launch, log, _) = fake::launch();
    let launch = launch.in_dir(repo.clone());
    let store = SessionStore::memory();
    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE).with_worktrees(worktrees);
    pool.prompt(&thread(1), "a").await.unwrap();
    crate::test_support::run(&repo, &["add", "."]);
    crate::test_support::commit(&repo, "first");
    pool.prompt(&thread(2), "b").await.unwrap();
    pool.evict_idle(Instant::now() + IDLE * 2);
    pool.prompt(&thread(1), "c").await.unwrap();
    let t2 = home.join("worktrees/t2");
    let cwds = [repo.clone(), t2.clone(), repo.clone()];
    assert_eq!(log.locked().cwds, cwds, "no restart; session/load of the first thread where it started");
    assert_eq!(pool.branch_ahead(&thread(1)).await, None, "no branch");
    assert_eq!(pool.prune_worktrees().await.removed, 0);
    pool.shutdown().await;

    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    let pool = Pool::new(launch, store.clone(), 4, IDLE).with_worktrees(worktrees);
    assert_eq!(pool.prompt(&thread(1), "d").await.unwrap(), "s1: d", "its session, after a daemon restart");
    pool.prompt(&thread(2), "e").await.unwrap();
    assert_eq!(log.locked().cwds[3..], [repo.clone(), t2], "each where its session is stored to work");
    assert_eq!(log.locked().loaded, ["s1", "s1", "s2"]);
    assert!(!home.join("worktrees/t1").exists());
    pool.evict_idle(Instant::now() + IDLE * 2);
    store.delete(&thread(1)).await.unwrap();
    assert_eq!(pool.prompt(&thread(1), "f").await.unwrap(), "s3: f", "a new session, not told of a move");
    assert_eq!(log.locked().cwds[5..], [home.join("worktrees/t1")], "its session expired: a worktree as any thread");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_thread_with_a_session_from_before_worktrees_were_turned_on_stays_in_the_workdir() {
    let Some(repo) = crate::test_support::repo("pool-worktrees-on") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let launch = launch.in_dir(repo.clone());
    let store = SessionStore::memory();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.shutdown().await;

    let worktrees = Worktrees::find(&"dev-a".parse().unwrap(), &repo, &home).await.unwrap();
    let pool = Pool::new(launch, store.clone(), 4, IDLE).with_worktrees(worktrees);
    assert_eq!(pool.run(&thread(1), with_history("b")).await.unwrap().text, "s1: b", "resumed, where it was created");
    pool.prompt(&thread(2), "c").await.unwrap();
    assert_eq!(log.locked().cwds, [repo.clone(), repo.clone(), home.join("worktrees/t2")]);
    assert_eq!(store.get(&thread(1)).await.unwrap().map(|s| s.cwd), Some(repo.clone()));
    assert_eq!(store.get(&thread(2)).await.unwrap().map(|s| s.cwd), Some(home.join("worktrees/t2")));
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_session_whose_directory_is_no_longer_the_thread_s_starts_over_told_where_it_worked() {
    let Some(repo) = crate::test_support::repo("pool-worktrees-moved") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let launch = launch.in_dir(repo.clone());
    let store = SessionStore::memory();
    let ant: UserId = "dev-a".parse().unwrap();
    let worktrees = Worktrees::find(&ant, &repo, &home).await.unwrap();
    let pool = Pool::new(launch.clone(), store.clone(), 4, IDLE).with_worktrees(worktrees);
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.evict_idle(Instant::now() + IDLE * 2);
    let t1 = home.join("worktrees/t1");
    std::fs::remove_dir_all(&t1).unwrap();
    assert_eq!(pool.prompt(&thread(1), "b").await.unwrap(), "s1: b", "its worktree gone, made again at its place");
    assert_eq!(log.locked().cwds, [t1.clone(), t1.clone()]);
    pool.shutdown().await;

    let left = Worktrees::left_behind(&ant, &home).await.unwrap();
    let pool = Pool::new(launch, store.clone(), 4, IDLE).with_worktrees_left_behind(left);
    let reply = pool.run(&thread(1), with_history("c")).await.unwrap();
    let told = format!(
        "s2: [ants] The earlier session of this thread worked in another directory, {}, so this one starts over, its \
         context rebuilt from the thread's history.\n\nHISTORY 65536\n\n[ants message]\nc",
        t1.display()
    );
    assert_eq!((reply.text, reply.rebuilt), (told, false), "worktrees turned off: not a session lost");
    assert_eq!(pool.run(&thread(1), with_history("d")).await.unwrap().text, "s2: d", "told once");
    let (cwds, loaded) = {
        let log = log.locked();
        (log.cwds[2..].to_vec(), log.loaded.len())
    };
    assert_eq!((cwds, loaded), (vec![repo.clone()], 1), "the session of the worktree is not loaded in the workdir");
    assert_eq!(store.get(&thread(1)).await.unwrap().map(|s| s.cwd), Some(repo.clone()));
    assert_eq!(pool.prune_worktrees().await.removed, 0, "its session is stored still");
    pool.evict_idle(Instant::now() + IDLE * 2);
    store.delete(&thread(1)).await.unwrap();
    assert_eq!(pool.prune_worktrees().await.removed, 1, "the worktree left behind goes with the session");
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}

#[tokio::test]
async fn a_thread_under_review_works_among_its_checkouts() {
    let Some(repo) = crate::test_support::repo("pool-checkouts") else { return };
    let home = repo.parent().unwrap().join("home");
    let (launch, log, _) = fake::launch();
    let worktrees = Worktrees::find(&"sage".parse().unwrap(), &repo, &home).await.unwrap();
    let checkouts = Checkouts::new(&home);
    let pool = Pool::new(launch.in_dir(repo.clone()), SessionStore::memory(), 4, IDLE)
        .with_worktrees(worktrees)
        .with_checkouts(checkouts.clone());
    let review = ants_proto::Envelope::on_thread(thread(1), "dev-a".parse().unwrap(), "sage".parse().unwrap(), "r");
    let prepared = pool.checkouts().unwrap().prepare(&review).await;
    pool.prompt(&thread(1), "a").await.unwrap();
    pool.prompt(&thread(2), "b").await.unwrap();
    assert_eq!(log.locked().cwds, [home.join("reviews/t1"), home.join("worktrees/t2")]);
    drop(prepared);
    if home.join("reviews/t1").exists() {
        std::fs::remove_dir_all(home.join("reviews/t1")).unwrap();
    }
    pool.evict_idle(Instant::now() + IDLE * 2);
    assert_eq!(pool.prompt(&thread(1), "c").await.unwrap(), "s1: c", "its session goes on, the checkouts gone");
    assert_eq!(log.locked().cwds[2..], [home.join("worktrees/t1")]);
    std::fs::remove_dir_all(repo.parent().unwrap()).unwrap();
}
