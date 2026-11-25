//! Steps their recipients settle without a reply: the notice of the recipient's daemon on the step's thread, replying
//! to the step ([`Envelope::step_notice`]), fails the call, raising in the script, and is noted on the run's thread;
//! but one telling that the recipient asked a human leaves the call waiting on the human, for the recipient's reply.

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

#[tokio::test]
async fn a_dead_lettered_step_raises_its_error_and_its_replay_s_reply_is_late() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let thread = ThreadId::generate();
    let run = lead.start_on(CAUGHT, TrailLimits::default(), thread.clone()).await;
    let step = lead.step().await;
    let error = "Gave up on the message from lead after 5 deliveries: the harness failed. Retry it with `ants dead \
                 replay 7`.";
    let notice = step.step_notice(ant("dev"), NoticeKind::GaveUp, error);
    assert!(lead.trails.take(&notice).await.unwrap(), "a notice prompts no conductor");
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert_eq!(result, json!(format!("raised: dev never answered: {error}")));
    assert_eq!(lead.notes(&thread, 2).await[1], noted(&run, &step, NoticeKind::GaveUp, error));

    // The step replayed from its dead letter: its reply answers nothing, and is noted.
    assert!(lead.reply(&step, "done after all").await, "the late reply prompts no conductor");
    let call = step.trail_step.as_ref().unwrap().call;
    let late = format!(
        "A reply from dev on the thread {} of step {call} to dev, which the run no longer waits on, run {run}:\ndone \
         after all",
        step.thread_id
    );
    let notes = lead.notes(&thread, 3).await;
    assert!(notes.contains(&late), "{notes:?}");
    let RunState::Done { result: still } = lead.header(&run).await.state else { panic!("not done") };
    assert_eq!(still, result);
}

#[tokio::test]
async fn a_step_whose_ant_asked_a_human_waits_on_the_human_and_takes_the_ant_s_answer_after() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let thread = ThreadId::generate();
    let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let run = lead.start_on(CAUGHT, TrailLimits::default(), thread.clone()).await;
    let step = lead.step().await;
    let text = "[ants] dev asked human and answers the step once answered.";
    let notice = step.step_notice(ant("dev"), NoticeKind::Asked, text);
    assert!(lead.trails.take(&notice).await.unwrap(), "a notice prompts no conductor");
    eventually("the run to wait on a human", async || lead.header(&run).await.state == RunState::Waiting).await;
    assert_eq!(lead.notes(&thread, 2).await[1], noted(&run, &step, NoticeKind::Asked, text));
    assert_eq!(step_status(&mut status).await.1, StepStage::Started);
    assert_eq!(step_status(&mut status).await.1, StepStage::Asked, "the step's status tells it waiting on the human");

    // The ant's answer after the human's replies to the step.
    assert!(lead.reply(&step, "deployed").await, "the reply prompts no conductor");
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert_eq!(result, json!("deployed"));
    assert_eq!(step_status(&mut status).await.1, StepStage::Answered);
}
