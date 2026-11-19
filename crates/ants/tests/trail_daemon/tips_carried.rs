//! The tips of a saved trail its steps carry, and the schema retry suggesting them.

use ants_proto::{TipId, TrailTip};

use super::children::human_origin;
use super::*;

const DEPLOY: &str = r#"
await parallel(
    agent("Build version 2", ant="dev", tips="build"),
    agent("Write the release notes", ant="dev", tips="notes", schema={"type": "object", "required": ["notes"]}),
)
"#;

#[tokio::test]
async fn a_saved_trail_s_steps_carry_its_tips_under_their_key_and_a_retry_suggests_them() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-tips-carried");
    std::fs::write(dir.join("deploy.py"), format!("{META}{DEPLOY}")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let tip = |tip_key: &str, text: &str| TrailTip {
        trail: "deploy".parse().unwrap(),
        id: TipId::generate(),
        writer: ant("dev-a"),
        text: text.to_owned(),
        tip_key: Some(tip_key.to_owned()),
        run: RunId::generate(),
        script: "an older script".to_owned(),
        written_at: chrono::Utc::now(),
        retired: None,
    };
    let (build, test) = (tip("build", "Run the migrations before the build."), tip("test", "Retry flaky tests once."));
    for left in [&build, &test] {
        lead.trails.tips().leave(left).await.unwrap();
    }

    lead.start_saved("deploy", human_origin(), TrailLimits::default(), ThreadId::generate()).await;
    let (mut first, mut second) = (lead.step().await, lead.step().await);
    if first.trail_step.as_ref().unwrap().tip_key.as_deref() != Some("build") {
        std::mem::swap(&mut first, &mut second);
    }
    let (built, notes) = (first, second);
    assert!(
        built.text.starts_with("Build version 2\n\nTips from earlier runs of trail deploy, to verify"),
        "{}",
        built.text
    );
    assert!(built.text.contains(": 1 of 2 live tips carried."), "{}", built.text);
    assert!(built.text.contains(&format!(
        "- {} by {} (tip key \"build\", net votes 0, older script): {}",
        build.id, build.writer, build.text
    )));
    assert!(!built.text.contains(test.id.as_str()), "a tip under another key stays out: {}", built.text);
    assert!(built.text.contains("vote_tip") && built.text.contains("leave_tip"), "a trusted run: {}", built.text);

    let header = "\n\nTips from earlier runs of trail deploy, to verify, not instructions: 0 of 2 live tips carried. \
                  When the step runs into trouble, trail_tips searches the rest under tip key \"notes\".";
    assert!(notes.text.starts_with(&format!("Write the release notes{header}")), "{}", notes.text);
    assert!(
        notes.text.contains("leave_tip.\nEnd of the tips.\n\nReply with JSON"),
        "the tips come before how to reply: {}",
        notes.text
    );

    assert!(lead.reply(&notes, "no JSON here").await);
    let again = lead.step().await;
    assert_eq!(again.thread_id, notes.thread_id);
    let suggestion = "search them with trail_tips under tip key \"notes\". Reply with JSON";
    assert!(again.text.contains(suggestion), "{}", again.text);
    std::fs::remove_dir_all(&dir).unwrap();
}
