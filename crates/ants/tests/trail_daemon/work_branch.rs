//! The branch a run's work so far is on: an agent() step's reply naming the branch of its thread's commits sets the
//! run's work branch, stored with its header, as the branch of the thread a run is started from with `run_trail` does
//! when it holds commits, and the agent() steps sent after it name that branch to merge.

use std::process::Command;

use ants_daemon::worktrees::Worktrees;
use ants_proto::WorkBranch;

use super::nested::{channel_outbox, serve, started};
use super::tip_tools::ant_mcp_sending;
use super::*;

/// The `[ants]` line naming `branch`, in the repository of `by`, to merge.
fn work_line(branch: &str, by: &str) -> String {
    format!(
        "[ants] The work so far is on branch {branch}, in the repository of {by}: this step works in a checkout of its \
         own, so merge {branch} to build on it."
    )
}

/// Run git with `args` in `dir`, as a test's committer.
fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(["-c", "user.name=t", "-c", "user.email=t@t"])
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&output.stderr));
}

#[tokio::test]
async fn a_step_after_a_step_whose_reply_names_a_branch_names_it_to_merge() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = "a = await agent(\"build\", ant=\"dev\")\nb = await agent(\"test\", ant=\"dev\")\n\
                  c = await agent(\"ship\", ant=\"dev\")\n[a, b, c]\n";
    let run = lead.start(script, TrailLimits::default()).await;

    let build = lead.step().await;
    assert_eq!(build.text, "build", "no work so far: no branch named");
    let branch = "ants/dev/T";
    let built = format!("built\n\n[ants] Branch {branch} holds 2 commits not in the repository's HEAD.");
    assert!(lead.reply(&build, &built).await);

    let test = lead.step().await;
    let line = work_line(branch, "dev");
    assert_eq!(test.text, format!("test\n\n{line}"));
    let stored = lead.header(&run).await.work_branch;
    assert_eq!(stored, Some(WorkBranch { name: branch.to_owned(), by: ant("dev") }));
    assert!(lead.reply(&test, "tested").await);

    let ship = lead.step().await;
    assert_eq!(ship.text, format!("ship\n\n{line}"), "a reply naming no branch leaves the run's");
    assert!(lead.reply(&ship, "shipped").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!([built, "tested", "shipped"]) });
}

#[tokio::test]
async fn a_run_started_from_a_thread_whose_branch_holds_commits_names_it_to_its_first_steps() {
    let Some(server) = NatsServer::start() else { return };
    if !ants_test_support::have("git") {
        return;
    }
    init(&server).await;
    let dir = scratch_dir("trail-work-branch").canonicalize().unwrap();
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "--initial-branch=main"]);
    git(&repo, &["commit", "--quiet", "--allow-empty", "-m", "first"]);
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    let (outbox, mut sent) = channel_outbox();
    let (trails, dev) = ant_mcp_sending(&server, "dev", &dir, &saved, outbox).await;
    let serving = serve(&trails);
    let worktrees = Arc::new(Worktrees::find(&ant("dev"), &repo, &dir.join("trees")).await.unwrap());
    dev.with_worktrees(worktrees.clone());
    let asked =
        Envelope::new(ants_test_support::human(), ant("dev"), "fan out").with_owner(Some(ants_test_support::human()));
    let thread = &asked.thread_id;
    let _asked = dev.begin(&asked);
    let work = worktrees.checkout(thread).await.unwrap();
    let client = mcp_client(&dev, thread).await;
    let script = "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": []}\n\
                  await agent(\"check\", ant=\"qa\")\n";

    let (ok, text) = call(&client, "run_trail", json!({"script": script})).await;
    assert!(ok, "{text}");
    let step = tokio::time::timeout(Duration::from_secs(20), sent.recv()).await.unwrap().unwrap();
    assert_eq!(step.text, "check", "no commits on the thread's branch: none named");
    assert_eq!(trails.run(&started(&text)).await.unwrap().unwrap().work_branch, None);

    git(&work, &["commit", "--quiet", "--allow-empty", "-m", "the work so far"]);
    let (ok, text) = call(&client, "run_trail", json!({"script": script})).await;
    assert!(ok, "{text}");
    let step = tokio::time::timeout(Duration::from_secs(20), sent.recv()).await.unwrap().unwrap();
    let branch = format!("ants/dev/{thread}");
    assert_eq!(step.text, format!("check\n\n{}", work_line(&branch, "dev")));
    let header = trails.run(&started(&text)).await.unwrap().unwrap();
    assert_eq!(header.work_branch, Some(WorkBranch { name: branch, by: ant("dev") }), "stored with the run");
    serving.abort();
}
