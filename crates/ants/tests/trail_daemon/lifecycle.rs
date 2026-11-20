//! Stops and resumes asked at once, or not stored, and the steps a run counts across them.

use super::*;

const FAN_OUT: &str = r#"await parallel(agent("a", ant="dev"), agent("b", ant="ops"))"#;

/// The trail runs bucket taking puts of at most `max` bytes, or any size for -1.
async fn runs_take_at_most(js: &async_nats::jetstream::Context, max: i32) {
    let mut stream = js.get_stream(format!("KV_{TRAIL_RUNS_BUCKET}")).await.unwrap();
    let mut config = stream.info().await.unwrap().config.clone();
    config.max_message_size = max;
    js.update_stream(config).await.unwrap();
}

/// Once the conductor answers on its control subject: the answers on `answers`.
async fn hearing(lead: &Conductor) -> async_nats::Subscriber {
    let mut answers = lead.client.subscribe(subject::trailed(&ant("lead"))).await.unwrap();
    loop {
        let request = TrailRequest::new(ant("lead"), UserId::human(), TrailAction::Stop { run: RunId::generate() });
        lead.client.publish(request.subject(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
        if answers_to(&mut answers, request.id, Duration::from_millis(200)).await.is_some() {
            return answers;
        }
    }
}

/// Publish `actions` on the control subject, back to back; their last answers, in order, a stop's pending one passed.
async fn ask_at_once(
    lead: &Conductor,
    answers: &mut async_nats::Subscriber,
    actions: Vec<TrailAction>,
) -> Vec<TrailOutcome> {
    let requests: Vec<TrailRequest> =
        actions.into_iter().map(|action| TrailRequest::new(ant("lead"), UserId::human(), action)).collect();
    for request in &requests {
        lead.client.publish(request.subject(), serde_json::to_vec(request).unwrap().into()).await.unwrap();
    }
    let mut outcomes: Vec<Option<TrailOutcome>> = vec![None; requests.len()];
    while outcomes.iter().any(Option::is_none) {
        let answer = tokio::time::timeout(Duration::from_secs(30), answers.next()).await.expect("an answer").unwrap();
        let answer: TrailReply = serde_json::from_slice(&answer.payload).unwrap();
        if matches!(answer.outcome, TrailOutcome::Stopping { .. }) {
            continue;
        }
        if let Some(at) = requests.iter().position(|request| request.id == answer.request_id) {
            outcomes[at] = Some(answer.outcome);
        }
    }
    outcomes.into_iter().map(Option::unwrap).collect()
}

/// Whether `outcomes`, of resumes of `run` asked at once, resumed it once: the others resumed it too or were refused as
/// it was running.
fn resumed_once(run: &RunId, outcomes: &[Result<(), String>]) -> bool {
    let resumed = outcomes.iter().filter(|outcome| outcome.is_ok()).count();
    let refused = outcomes.iter().filter_map(|outcome| outcome.as_ref().err());
    resumed >= 1 && refused.into_iter().all(|error| error == &format!("run {run} is not stopped"))
}

fn as_result(outcome: TrailOutcome) -> Result<(), String> {
    match outcome {
        TrailOutcome::Resumed { .. } => Ok(()),
        TrailOutcome::Refused { error } => Err(error),
        outcome => panic!("not a resume's answer: {outcome:?}"),
    }
}

/// The two steps of [`FAN_OUT`] the conductor sends, and no more within a second.
async fn fan_out_steps(lead: &mut Conductor) -> (Envelope, Envelope) {
    let (first, second) = (lead.step().await, lead.step().await);
    let mut to = [first.to.as_str(), second.to.as_str()];
    to.sort_unstable();
    assert_eq!(to, ["dev", "ops"], "each call goes out once");
    assert!(lead.sends_no_step(Duration::from_secs(1)).await, "a call went out twice");
    (first, second)
}

#[tokio::test]
async fn resumes_of_one_run_asked_at_once_resume_it_once_and_a_stop_beside_one_stops_the_resumed_run() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let mut answers = hearing(&lead).await;
    let human = UserId::human();
    let run = lead.start(FAN_OUT, TrailLimits::default()).await;
    fan_out_steps(&mut lead).await;
    lead.trails.stop(&run, &human).await.unwrap();

    // Two requests at once.
    let resume = TrailAction::Resume { run: run.clone(), limits: None };
    let outcomes = ask_at_once(&lead, &mut answers, vec![resume.clone(), resume.clone()]).await;
    let outcomes: Vec<_> = outcomes.into_iter().map(as_result).collect();
    assert!(resumed_once(&run, &outcomes), "{outcomes:?}");
    fan_out_steps(&mut lead).await;
    assert_eq!(lead.header(&run).await.steps, 2, "the steps sent anew count once");

    // A request beside the resume an admin's order, or a parent run's resume, makes.
    lead.trails.stop(&run, &human).await.unwrap();
    let request = TrailRequest::new(ant("lead"), human.clone(), resume.clone());
    lead.client.publish(request.subject(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
    let direct = lead.trails.resume(&run, &human, None).await;
    let requested = answers_to(&mut answers, request.id, Duration::from_secs(30)).await.expect("an answer");
    assert!(resumed_once(&run, &[direct.clone(), as_result(requested.clone())]), "{direct:?}, {requested:?}");
    fan_out_steps(&mut lead).await;
    assert_eq!(lead.header(&run).await.steps, 2);

    // A stop asked while a resume is under way stops the resumed run.
    lead.trails.stop(&run, &human).await.unwrap();
    let stop = TrailAction::Stop { run: run.clone() };
    let outcomes = ask_at_once(&lead, &mut answers, vec![resume.clone(), stop]).await;
    assert_eq!(outcomes[0], TrailOutcome::Resumed { run: run.clone() });
    assert!(matches!(&outcomes[1], TrailOutcome::Stopped { run: stopped, .. } if *stopped == run), "{outcomes:?}");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: human.clone() });
    while !lead.sends_no_step(Duration::from_millis(500)).await {}
    assert!(lead.trails.status(&run).await.unwrap().header.state == RunState::Stopped { by: human.clone() });

    lead.trails.resume(&run, &human, None).await.unwrap();
    let (first, second) = fan_out_steps(&mut lead).await;
    assert!(lead.reply(&first, &first.text.to_uppercase()).await);
    assert!(lead.reply(&second, &second.text.to_uppercase()).await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
    assert_eq!(lead.header(&run).await.steps, 2);
}

#[tokio::test]
async fn a_stop_that_cannot_be_stored_is_refused_and_a_stopped_run_lists_only_the_steps_sent() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let humans = Registry::new(UserId::human());
    let mut lead = conductor_refusing(&server, &ants_binary(), Vec::new(), humans, None, Some(ant("ops"))).await;
    let mut answers = hearing(&lead).await;
    let human = UserId::human();
    let run = lead.start(FAN_OUT, TrailLimits::default()).await;
    // The step to ops never goes out.
    let a = lead.step().await;
    assert_eq!(a.to, ant("dev"));

    runs_take_at_most(&lead.js, 64).await;
    let refused = lead.trails.stop(&run, &human).await.unwrap_err();
    assert!(refused.starts_with("storing the run: "), "{refused}");
    let stop = TrailAction::Stop { run: run.clone() };
    let outcomes = ask_at_once(&lead, &mut answers, vec![stop.clone()]).await;
    let [TrailOutcome::Refused { error }] = &outcomes[..] else { panic!("not refused: {outcomes:?}") };
    assert!(error.starts_with("storing the run: "), "{error}");
    assert_eq!(lead.header(&run).await.state, RunState::Running, "the record is left running");
    let status = lead.trails.status(&run).await.unwrap();
    assert_eq!(status.header.state, RunState::Running, "the run is left running in memory");

    runs_take_at_most(&lead.js, -1).await;
    let waiting = lead.trails.stop(&run, &human).await.unwrap();
    let sent = vec![WaitingStep { to: ant("dev"), message: a.id }];
    assert_eq!(waiting, sent, "the step sent alone is for the human to withdraw");
    let header = lead.header(&run).await;
    assert_eq!(header.state, RunState::Stopped { by: human.clone() });
    assert_eq!(header.steps, 1, "the step never sent is given back, to count as it goes out on resuming");
    let outcomes = ask_at_once(&lead, &mut answers, vec![stop]).await;
    assert_eq!(outcomes, [TrailOutcome::Stopped { run: run.clone(), waiting: sent }], "asked again");
}

#[tokio::test]
async fn a_run_failed_on_its_turns_keeps_the_count_of_the_steps_sent_and_resumes_within_its_steps() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
x = await agent("x", ant="dev")
r = await parallel(human([{"question": "ok?", "options": ["yes", "no"]}]), agent("a", ant="ops"))
[x, r]
"#;
    let run = lead.start(script, TrailLimits { steps: 3, turns: 1, ..TrailLimits::default() }).await;
    let x = lead.step().await;
    assert!(lead.reply(&x, "X").await);
    let RunState::Failed { error, limit } = lead.ended(&run).await else { panic!("not failed") };
    assert_eq!(limit, Some(RunLimit::Turns), "{error}");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await, "a step of the failed pass went out");
    assert_eq!(lead.header(&run).await.steps, 1, "the step sent alone counts");

    let limits = TrailLimits { steps: 3, turns: 10, ..TrailLimits::default() };
    lead.trails.resume(&run, &UserId::human(), Some(limits)).await.unwrap();
    let (first, second) = (lead.step().await, lead.step().await);
    let (asked, a) = if first.to == ant("ops") { (second, first) } else { (first, second) };
    assert!(lead.reply(&asked, "yes").await);
    assert!(lead.reply(&a, "A").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["X", ["yes", "A"]]) });
    assert_eq!(lead.header(&run).await.steps, 3);
}

#[tokio::test]
async fn a_resume_tried_again_after_its_records_were_stored_half_way_counts_each_step_once() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-resume-half-way");
    let meta = json!({"name": "fan", "description": "a test", "phases": []});
    std::fs::write(dir.join("fan.py"), format!("meta = {meta}\n{FAN_OUT}\n")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let human = UserId::human();
    // The first leg's script, pinned in the run's header alone once the run went on to the next, makes the header
    // outweigh the session.
    let script = format!("# {}\nawait goto(\"fan\")\n", "x".repeat(60_000));
    let run = lead.start(&script, TrailLimits::default()).await;
    fan_out_steps(&mut lead).await;
    lead.trails.stop(&run, &human).await.unwrap();

    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let session = runs.get(trail_session_key(&ant("lead"), &run)).await.unwrap().unwrap().len();
    let header = runs.get(trail_run_key(&ant("lead"), &run)).await.unwrap().unwrap().len();
    assert!(header > session + 20_000, "header {header}, session {session}");
    // The session is put, the header not.
    runs_take_at_most(&lead.js, i32::try_from(session + 10_000).unwrap()).await;
    let refused = lead.trails.resume(&run, &human, None).await.unwrap_err();
    assert!(refused.starts_with("storing the run: "), "{refused}");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: human.clone() });

    runs_take_at_most(&lead.js, -1).await;
    lead.trails.resume(&run, &human, None).await.unwrap();
    let (first, second) = fan_out_steps(&mut lead).await;
    assert_eq!(lead.header(&run).await.steps, 2, "the steps sent anew count once");
    assert!(lead.reply(&first, &first.text.to_uppercase()).await);
    assert!(lead.reply(&second, &second.text.to_uppercase()).await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
    assert_eq!(lead.header(&run).await.steps, 2);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Leave `run` of `lead`, its two steps stored sent, stored but not listed, as a daemon stopped between storing its
/// header and listing it leaves it, and restart the conductor.
async fn unlisted(lead: Conductor, run: &RunId, server: &NatsServer, dir: &Path) -> Conductor {
    eventually("the steps are stored sent", async || {
        let pending = lead.session(run).await.0.pending;
        pending.len() == 2 && pending.iter().all(|call| call.sent)
    })
    .await;
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    drop(lead);
    let index = serde_json::to_vec(&TrailRunIndex { runs: Vec::new() }).unwrap();
    runs.put(trail_runs_index_key(&ant("lead")), index).await.unwrap();
    let again = conductor_saving(server, &ants_binary(), Vec::new(), Some(dir.to_owned())).await;
    assert!(again.trails.unfinished().is_empty(), "the index lists no run");
    again
}

#[tokio::test]
async fn a_run_its_firing_continues_beside_a_resume_or_a_stop_of_it_is_continued_once_and_keeps_the_stop() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-fired-beside");
    let meta = json!({"name": "fan", "description": "a test", "phases": []});
    std::fs::write(dir.join("fan.py"), format!("meta = {meta}\n{FAN_OUT}\n")).unwrap();
    let start = TrailStart { trail: "fan".parse().unwrap(), args: Value::Null, limits: TrailLimits::DEFAULT };
    let firing = Envelope::new(ant("nightly"), ant("lead"), "Scheduled run of trail.")
        .with_owner(Some(UserId::human()))
        .from_webhook()
        .starting(Some(start.clone()));
    let human = UserId::human();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let run = lead.trails.start_fired(&firing, &start).await.unwrap();
    fan_out_steps(&mut lead).await;
    lead.trails.stop(&run, &human).await.unwrap();

    // The firing, delivered again, continues the stopped run while a resume of it is under way.
    let mut again = unlisted(lead, &run, &server, &dir).await;
    let (fired, resumed) =
        tokio::join!(again.trails.start_fired(&firing, &start), again.trails.resume(&run, &human, None));
    assert_eq!(fired, Ok(run.clone()));
    assert_eq!(resumed, Ok(()));
    assert_eq!(again.trails.unfinished().len(), 1);
    assert_eq!(again.trails.status(&run).await.unwrap().header.state, RunState::Running);
    fan_out_steps(&mut again).await;
    assert_eq!(again.header(&run).await.steps, 2, "the steps sent anew count once");

    // The firing, delivered again, continues the running run while a stop of it is under way: the stop is refused, or
    // the run stays stopped.
    let mut third = unlisted(again, &run, &server, &dir).await;
    let stop = async {
        tokio::task::yield_now().await;
        third.trails.stop(&run, &human).await
    };
    let (fired, stopped) = tokio::join!(third.trails.start_fired(&firing, &start), stop);
    assert_eq!(fired, Ok(run.clone()));
    assert_eq!(third.trails.unfinished().len(), 1);
    let state = third.trails.status(&run).await.unwrap().header.state;
    match stopped {
        Ok(_) => {
            assert_eq!(state, RunState::Stopped { by: human.clone() });
            assert_eq!(third.header(&run).await.state, state, "stored stopped");
            assert!(third.sends_no_step(Duration::from_secs(1)).await, "a stopped run sends no step");
        }
        Err(error) => {
            assert_eq!(error, format!("lead conducts no run {run}"));
            assert_eq!(state, RunState::Running);
            third.trails.stop(&run, &human).await.unwrap();
        }
    }
    third.trails.resume(&run, &human, None).await.unwrap();
    let (first, second) = fan_out_steps(&mut third).await;
    assert!(third.reply(&first, &first.text.to_uppercase()).await);
    assert!(third.reply(&second, &second.text.to_uppercase()).await);
    assert_eq!(third.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
    std::fs::remove_dir_all(&dir).unwrap();
}
