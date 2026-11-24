//! The branch a run's work so far is on: an agent() step's reply naming the branch of its thread's commits sets the
//! run's work branch, stored with its header, and the agent() steps sent after it name that branch to merge.

use ants_proto::WorkBranch;

use super::*;

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
    let line = format!(
        "[ants] The work of this run's steps so far is on branch {branch}, in the repository of dev: this step works \
         in a checkout of its own, so merge {branch} to build on it."
    );
    assert_eq!(test.text, format!("test\n\n{line}"));
    let stored = lead.header(&run).await.work_branch;
    assert_eq!(stored, Some(WorkBranch { name: branch.to_owned(), by: ant("dev") }));
    assert!(lead.reply(&test, "tested").await);

    let ship = lead.step().await;
    assert_eq!(ship.text, format!("ship\n\n{line}"), "a reply naming no branch leaves the run's");
    assert!(lead.reply(&ship, "shipped").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!([built, "tested", "shipped"]) });
}
