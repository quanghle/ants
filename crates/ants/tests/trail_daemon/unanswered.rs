//! Steps their recipients settle without a reply: the notice of the recipient's daemon on the step's thread, replying
//! to the step ([`Envelope::step_notice`]), fails the call, raising in the script, and is noted on the run's thread.

use super::*;

/// A script catching what its one step to dev raises, returning the error's text.
const CAUGHT: &str = r#"
async def go():
    try:
        return await agent("go", ant="dev")
    except RuntimeError as e:
        return "raised: " + str(e)
await go()
"#;

/// The note on the run's thread of `notice`, of `kind`, from dev on the thread of `step` of `run`.
fn noted(run: &RunId, step: &Envelope, kind: NoticeKind, text: &str) -> String {
    let call = step.trail_step.as_ref().unwrap().call;
    format!(
        "A {} notice from dev on the thread {} of step {call} to dev, run {run}:\n{text}",
        kind.as_str(),
        step.thread_id
    )
}

#[tokio::test]
async fn a_step_its_ant_turns_away_at_a_cap_or_out_of_turns_raises_in_the_script() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    for why in [
        "Daily cost cap of dev reached (5.00 of 5.00 today; resets at 00:00 UTC)",
        "Turn budget spent: the step's thread has no turns left",
    ] {
        let thread = ThreadId::generate();
        let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
        let run = lead.start_on(CAUGHT, TrailLimits::default(), thread.clone()).await;
        let step = lead.step().await;
        let text = format!("[ants] {why}; dev does not answer the step.");
        let other = step.step_notice(ant("ops"), NoticeKind::Unanswered, &text);
        assert!(lead.trails.take(&other).await.unwrap(), "another's notice is noted");
        assert!(!lead.header(&run).await.finished(), "but fails nothing");
        let notice = step.step_notice(ant("dev"), NoticeKind::Unanswered, &text);
        assert!(lead.trails.take(&notice).await.unwrap(), "a notice prompts no conductor");
        let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
        assert_eq!(result, json!(format!("raised: dev never answered: {why}; dev does not answer the step.")));
        assert_eq!(lead.notes(&thread, 3).await[2], noted(&run, &step, NoticeKind::Unanswered, &text));
        assert_eq!(step_status(&mut status).await.1, StepStage::Started);
        assert_eq!(step_status(&mut status).await.1, StepStage::Failed);
    }
}

#[tokio::test]
async fn a_step_a_human_steered_raises_in_the_script_naming_the_human() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let thread = ThreadId::generate();
    let run = lead.start_on(CAUGHT, TrailLimits::default(), thread.clone()).await;
    let step = lead.step().await;
    let steered = format!(
        "{} steered the prompt answering the step with message {}, which takes the answer.",
        ants_test_support::human(),
        ants_proto::Uuid::now_v7()
    );
    let text = format!("[ants] {steered}");
    let notice = step.step_notice(ant("dev"), NoticeKind::Steered, &text);
    assert!(lead.trails.take(&notice).await.unwrap(), "a notice prompts no conductor");
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert_eq!(result, json!(format!("raised: dev never answered: {steered}")));
    assert_eq!(lead.notes(&thread, 2).await[1], noted(&run, &step, NoticeKind::Steered, &text));
}
