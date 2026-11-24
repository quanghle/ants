//! A run from its start to its end: its steps to ants and humans, the reply to one answering it, a human's own or
//! their assistant's, a re-ask for a reply matching a schema; started by a human or a schedule's firing; stopped and
//! resumed, by a human or an admin order, asked at once or not stored, and the steps a run counts across them; ended
//! as its conductor is retired; a step to a team going to its lead; a paused conductor starting none and holding its
//! steps.

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
        let request =
            TrailRequest::new(ant("lead"), ants_test_support::human(), TrailAction::Stop { run: RunId::generate() });
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
        actions.into_iter().map(|action| TrailRequest::new(ant("lead"), ants_test_support::human(), action)).collect();
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
    let human = ants_test_support::human();
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
    let humans = Registry::new(ants_test_support::human());
    let mut lead = conductor_refusing(&server, &ants_binary(), Vec::new(), humans, None, Some(ant("ops"))).await;
    let mut answers = hearing(&lead).await;
    let human = ants_test_support::human();
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
    lead.trails.resume(&run, &ants_test_support::human(), Some(limits)).await.unwrap();
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
    let human = ants_test_support::human();
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
        .with_owner(Some(ants_test_support::human()))
        .from_webhook()
        .starting(Some(start.clone()));
    let human = ants_test_support::human();
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

/// agent(team=) sends the step to the team's lead, marked as addressed to the team; a team with no such name, or named
/// beside an ant, raises in the script.
#[tokio::test]
async fn a_step_to_a_team_goes_to_its_lead() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (_, js) =
        ants_daemon::bus::connect(&server.url, None, None, &ants_test_support::human(), "teams").await.unwrap();
    js.create_stream(hub::teams_config()).await.unwrap();
    let kv = ants_daemon::teams::open(&js, None).await.unwrap();
    let mut directory = ants_proto::TeamDirectory::default();
    directory
        .create(ants_proto::Team::new(ant("reviewers"), ant("reviewer"), ants_test_support::human()), None)
        .unwrap();
    assert!(ants_daemon::teams::write(&kv, &directory, 0).await.unwrap());
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
out = []
try:
    out.append(await agent("x", team="nobody"))
except RuntimeError as e:
    out.append(str(e))
try:
    out.append(await agent("x", team="reviewers", ant="dev"))
except RuntimeError as e:
    out.append(str(e))
out.append(await agent("review it", team="reviewers"))
out
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let step = lead.step().await;
    assert_eq!((step.to.as_str(), step.text.as_str()), ("reviewer", "review it"));
    assert_eq!(step.team, Some(ant("reviewers")), "the step says it came through the team");
    assert!(lead.reply(&step, "fine").await);
    let refused = json!(["no team nobody", "agent() takes team= alone, without ant= or capability=", "fine"]);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: refused });
}

#[tokio::test]
async fn a_human_starts_a_run_fanning_out_to_two_ants_whose_replies_finish_it() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), vec![colony_entry("reviewer", &["review"])]).await;
    let mut answers = lead.client.subscribe(subject::trailed(&ant("lead"))).await.unwrap();
    let mut ask = async |action: TrailAction, wait: Duration| {
        let request = TrailRequest::new(ant("lead"), ants_test_support::human(), action);
        lead.client.publish(request.subject(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
        let answer = tokio::time::timeout(wait, answers.next()).await.ok()?.unwrap();
        let answer: TrailReply = serde_json::from_slice(&answer.payload).unwrap();
        assert_eq!(answer.request_id, request.id);
        Some(answer.outcome)
    };
    let missing = RunId::generate();
    let refused = loop {
        let stop = TrailAction::Stop { run: missing.clone() };
        if let Some(outcome) = ask(stop, Duration::from_millis(200)).await {
            break outcome;
        }
    };
    let TrailOutcome::Refused { error } = refused else { panic!("not refused: {refused:?}") };
    assert!(error.contains("conducts no run"), "{error}");
    let script = r#"
await parallel(agent("build it", ant="dev", tips="build"), agent("review it", capability="review"))
"#;
    let start = TrailAction::Start {
        trail: TrailSource::Script(format!("{META}{script}")),
        args: Value::Null,
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
    };
    let started = ask(start, Duration::from_secs(10)).await.expect("an answer");
    let TrailOutcome::Started { run } = started else { panic!("not started: {started:?}") };

    let (first, second) = (lead.step().await, lead.step().await);
    let mut steps = [first, second];
    steps.sort_by_key(|step| step.to.to_string());
    let [dev, reviewer] = steps;
    assert_eq!((dev.to.as_str(), reviewer.to.as_str()), ("dev", "reviewer"));
    assert_ne!(dev.thread_id, reviewer.thread_id, "each step has a thread of its own");
    assert_eq!(dev.text, "build it");
    let marks = dev.trail_step.as_ref().unwrap();
    assert_eq!(marks.run, run);
    assert_eq!(marks.tip_key.as_deref(), Some("build"), "the step carries its tip key");
    assert_eq!((&marks.trail, &marks.script), (&None, &None), "an inline script is no saved trail");
    assert_eq!(reviewer.trail_step.as_ref().unwrap().tip_key, None);
    assert_eq!(dev.owner, Some(ants_test_support::human()), "the step's thread keeps the run's owner");

    let unrelated = Envelope::new(ant("dev"), ant("lead"), "hello");
    assert!(!lead.trails.take(&unrelated).await.unwrap(), "a message on no step's thread is prompted as ever");
    assert!(lead.reply(&reviewer, "looks good").await);
    assert!(lead.reply(&dev, "built").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["built", "looks good"]) });
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.turns), (2, 2), "each reply spent a turn of the run");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await, "no step went twice");
}

#[tokio::test]
async fn a_reply_not_matching_the_schema_is_asked_again_and_raises_after_two_retries() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
async def ask(prompt):
    try:
        return await agent(prompt, ant="dev", schema=schema)
    except RuntimeError as e:
        return str(e)
await parallel(ask("count"), ask("guess"))
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let (first, second) = (lead.step().await, lead.step().await);
    let (count, guess) = if first.text.starts_with("count") { (first, second) } else { (second, first) };
    assert!(count.text.contains("JSON Schema"), "{}", count.text);

    assert!(lead.reply(&count, "about seven").await);
    let again = lead.step().await;
    assert_eq!(again.thread_id, count.thread_id, "asked again on the step's thread");
    assert!(again.text.contains("no JSON"), "{}", again.text);
    assert!(lead.reply(&again, "Here:\n```json\n{\"n\": 7}\n```").await);

    let mut asked = guess;
    for _ in 0..2 {
        assert!(lead.reply(&asked, "{\"n\": \"many\"}").await);
        asked = lead.step().await;
        assert!(asked.text.contains("$.n"), "the error is named: {}", asked.text);
    }
    assert!(lead.reply(&asked, "{\"m\": 1}").await);
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    assert_eq!(result[0], json!({"n": 7}));
    let error = result[1].as_str().unwrap();
    assert!(error.contains("did not match the schema, asked 3 times") && error.contains(r#"has no "n""#), "{error}");
}

#[tokio::test]
async fn a_human_step_asks_the_runs_owner_and_its_answer_comes_later() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
await human([{"question": "Ship it?", "options": ["yes", "no"]}])
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let asked = lead.step().await;
    assert_eq!(asked.to, ants_test_support::human());
    assert!(asked.asks().is_some(), "the step asks its questions as ask_human does");
    eventually("the run waits for its human", async || lead.header(&run).await.state == RunState::Waiting).await;

    tokio::time::sleep(Duration::from_millis(500)).await;
    let answer = Envelope::answering(&asked, ants_test_support::human(), vec![Pick::option(1)]).unwrap();
    assert!(lead.trails.take(&answer).await.unwrap());
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("yes") });
}

#[tokio::test]
async fn a_stopped_run_gives_up_its_steps_and_resumes_with_fresh_ones() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run =
        lead.start(r#"await parallel(agent("a", ant="dev"), agent("b", ant="ops"))"#, TrailLimits::default()).await;
    let (first, second) = (lead.step().await, lead.step().await);

    let human = ants_test_support::human();
    let mut waiting = lead.trails.stop(&run, &human).await.unwrap();
    waiting.sort_by_key(|step| step.to.to_string());
    let messages: Vec<_> = waiting.iter().map(|step| (step.to.as_str(), step.message)).collect();
    let mut sent = vec![(first.to.as_str(), first.id), (second.to.as_str(), second.id)];
    sent.sort_unstable();
    assert_eq!(messages, sent, "the steps waited on are for the human to withdraw");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: human.clone() });
    let told = lead.told().await;
    assert_eq!((&told.to, &told.thread_id), (&human, &lead.header(&run).await.thread), "{told:?}");
    assert!(told.text.contains("was stopped by human") && told.text.contains("ants trail resume"), "{}", told.text);
    assert!(lead.reply(&first, "late").await, "a stopped step's reply is not prompted");
    assert!(lead.sends_no_step(Duration::from_secs(1)).await);
    assert!(lead.trails.stop(&run, &human).await.is_err(), "a run stops once");

    lead.trails.resume(&run, &human, None).await.unwrap();
    let (first, second) = (lead.step().await, lead.step().await);
    assert!(![&first, &second].iter().any(|step| step.id == messages[0].1 || step.id == messages[1].1));
    assert!(lead.reply(&first, &first.text.to_uppercase()).await);
    assert!(lead.reply(&second, &second.text.to_uppercase()).await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
}

#[tokio::test]
async fn a_retired_conductor_ends_its_running_and_stopped_runs_each_owner_told_why_on_its_thread() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let running = lead.start(r#"await agent("a", ant="dev")"#, TrailLimits::default()).await;
    lead.step().await;
    let stopped = lead.start(r#"await agent("b", ant="ops")"#, TrailLimits::default()).await;
    lead.step().await;
    let human = ants_test_support::human();
    lead.trails.stop(&stopped, &human).await.unwrap();
    lead.told().await;

    let why = "its conductor lead was retired, so no daemon conducts it any more";
    assert_eq!(lead.trails.end_all(why).await, 2);
    assert!(lead.trails.unfinished().is_empty(), "no run is left running, waiting or stopped");
    let told = [lead.told().await, lead.told().await];
    for run in [&running, &stopped] {
        let header = lead.header(run).await;
        assert_eq!(header.state, RunState::Failed { error: why.to_owned(), limit: None });
        let told = told.iter().find(|told| told.thread_id == header.thread).expect("the owner is told");
        assert_eq!(told.to, human);
        assert!(told.text.contains(&format!("{run}")) && told.text.contains(why), "{}", told.text);
    }
    assert_eq!(lead.trails.end_all(why).await, 0, "an ended run ends once");
}

#[tokio::test]
async fn a_stop_while_the_script_computes_is_told_pending_and_a_start_asked_again_starts_one_run() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-slow-worker");
    let (worker, computing, go) = (dir.join("worker"), dir.join("computing"), dir.join("go"));
    let wrapper = format!(
        "#!/bin/sh\ntouch {computing:?}\nwhile [ ! -e {go:?} ]; do sleep 0.05; done\nexec {:?} \"$@\"\n",
        ants_binary()
    );
    std::fs::write(&worker, wrapper).unwrap();
    std::process::Command::new("chmod").arg("+x").arg(&worker).status().unwrap();
    let mut lead = conductor(&server, &worker, Vec::new()).await;
    let mut answers = lead.client.subscribe(subject::trailed(&ant("lead"))).await.unwrap();
    let client = lead.client.clone();
    let publish = async |request: &TrailRequest| {
        client.publish(request.subject(), serde_json::to_vec(request).unwrap().into()).await.unwrap();
    };

    let script = format!("{META}await agent(\"a\", ant=\"dev\")\n");
    let action = TrailAction::Start {
        trail: TrailSource::Script(script),
        args: Value::Null,
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
    };
    let start = TrailRequest::new(ant("lead"), ants_test_support::human(), action);
    let run: RunId = start.id.to_string().parse().unwrap();
    loop {
        let heard =
            TrailRequest::new(ant("lead"), ants_test_support::human(), TrailAction::Stop { run: RunId::generate() });
        publish(&heard).await;
        if answers_to(&mut answers, heard.id, Duration::from_millis(200)).await.is_some() {
            break;
        }
    }
    publish(&start).await;
    let started = answers_to(&mut answers, start.id, Duration::from_secs(5)).await;
    assert_eq!(started, Some(TrailOutcome::Started { run: run.clone() }), "the run takes the request's id");
    eventually("the script computes", async || computing.exists()).await;

    let stop = TrailRequest::new(ant("lead"), ants_test_support::human(), TrailAction::Stop { run: run.clone() });
    publish(&stop).await;
    let pending = answers_to(&mut answers, stop.id, Duration::from_secs(5)).await;
    assert_eq!(pending, Some(TrailOutcome::Stopping { run: run.clone() }), "the stop waits for the pass");
    publish(&start).await;
    let again = answers_to(&mut answers, start.id, Duration::from_secs(5)).await;
    assert_eq!(again, Some(TrailOutcome::Started { run: run.clone() }), "answered while the stop waits");
    assert_eq!(lead.trails.unfinished().len(), 1, "a start asked again starts no second run");

    std::fs::write(&go, "").unwrap();
    let step = lead.step().await;
    let waiting = vec![WaitingStep { to: ant("dev"), message: step.id }];
    let stopped = answers_to(&mut answers, stop.id, Duration::from_secs(30)).await;
    assert_eq!(stopped, Some(TrailOutcome::Stopped { run: run.clone(), waiting: waiting.clone() }));
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: ants_test_support::human() });

    let again = TrailRequest::new(ant("lead"), ants_test_support::human(), TrailAction::Stop { run: run.clone() });
    publish(&again).await;
    let listed = answers_to(&mut answers, again.id, Duration::from_secs(5)).await;
    assert_eq!(listed, Some(TrailOutcome::Stopped { run, waiting }), "a stopped run lists the steps to withdraw");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_resumed_run_counts_the_steps_it_sends_anew_once() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let limits = TrailLimits { steps: 2, ..TrailLimits::default() };
    let run = lead.start(r#"await parallel(agent("a", ant="dev"), agent("b", ant="ops"))"#, limits).await;
    lead.step().await;
    lead.step().await;
    lead.trails.stop(&run, &ants_test_support::human()).await.unwrap();
    assert_eq!(lead.header(&run).await.steps, 2);

    lead.trails.resume(&run, &ants_test_support::human(), None).await.unwrap();
    let (first, second) = (lead.step().await, lead.step().await);
    assert!(lead.reply(&first, &first.text.to_uppercase()).await);
    assert!(lead.reply(&second, &second.text.to_uppercase()).await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["A", "B"]) });
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.leg().unwrap().steps), (2, 2), "the steps sent anew count once");
}

/// A user JWT for `name`, tagged `tag`, signed by `account` for the key `key`.
pub(super) fn user_jwt(account: &nkeys::KeyPair, key: &nkeys::KeyPair, name: &UserId, tag: &str) -> String {
    use data_encoding::BASE64URL_NOPAD;
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let claims = json!({
        "sub": key.public_key(), "iss": account.public_key(), "name": name,
        "nats": { "type": "user", "tags": [tag] },
    });
    let body = BASE64URL_NOPAD.encode(claims.to_string().as_bytes());
    let signature = BASE64URL_NOPAD.encode(&account.sign(format!("{header}.{body}").as_bytes()).unwrap());
    format!("{header}.{body}.{signature}")
}

#[tokio::test]
async fn the_owners_assistant_answers_a_human_step_while_the_grant_names_it() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (human, pa) = (ants_test_support::human(), ant("pa"));
    let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &human, "grant").await.unwrap();
    let bucket = js.create_key_value(ants_daemon::hub::humans_config()).await.unwrap();
    let (account, device) = (nkeys::KeyPair::new_account(), nkeys::KeyPair::new_user());
    let jwt = user_jwt(&account, &device, &human, &DeviceId::generate().tag());
    let mut grant = AssistantGrant::new(human.clone(), Some(pa.clone()), chrono::Utc::now(), jwt);
    grant.signature = data_encoding::BASE64URL_NOPAD.encode(&device.sign(&grant.signed()).unwrap());
    bucket.put(grant.key(), serde_json::to_vec(&grant).unwrap().into()).await.unwrap();
    let humans = Registry::new(human.clone()).verifying_grants(Some(account.public_key()));
    let _watching = humans.watch(js, Duration::from_secs(1));
    eventually("the grant is read", async || humans.assistant(&human).as_ref() == Some(&pa)).await;

    let mut lead = conductor_with(&server, &ants_binary(), Vec::new(), humans).await;
    let run =
        lead.start(r#"await human([{"question": "Ship it?", "options": ["yes", "no"]}])"#, TrailLimits::default());
    let run = run.await;
    let asked = lead.step().await;
    assert_eq!(asked.to, human);
    let answer = |by: &str, pick: u32| {
        Envelope::answering(&asked, ant(by), vec![Pick::option(pick)]).unwrap().on_behalf(Some(human.clone()))
    };
    assert!(!lead.trails.take(&answer("dev", 2)).await.unwrap(), "the grant names no other ant");
    assert!(lead.trails.take(&answer("pa", 1)).await.unwrap(), "the assistant answers for its human");
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("yes") });
}

#[tokio::test]
async fn a_schedule_s_firing_starts_its_saved_trail_once_on_its_thread() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-fired");
    std::fs::write(dir.join("count.py"), format!("{META}args[\"n\"] + 1\n")).unwrap();
    let lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let fire = |start: &TrailStart| {
        Envelope::new(ant("nightly"), ant("lead"), "Scheduled run of trail.")
            .with_owner(Some(ants_test_support::human()))
            .from_webhook()
            .starting(Some(start.clone()))
    };
    let limits = TrailLimits { steps: 7, wall_secs: 3600, ..TrailLimits::DEFAULT };
    let start = TrailStart { trail: "count".parse().unwrap(), args: json!({"n": 1}), limits };
    let firing = fire(&start);

    let run = lead.trails.start_fired(&firing, &start).await.unwrap();
    assert_eq!(run.as_str(), firing.id.to_string(), "the run takes the firing's id");
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(2) });
    let header = lead.header(&run).await;
    assert_eq!((&header.thread, header.owner.as_ref()), (&firing.thread_id, Some(&ants_test_support::human())));
    assert_eq!((header.trail(), header.limits), (Some(&start.trail), limits), "under the limits asked for");
    assert!(!header.origin.trusted(), "a schedule's run is as untrusted as a webhook's: {:?}", header.origin);
    assert_eq!(lead.trails.start_fired(&firing, &start).await.unwrap(), run, "delivered again");
    assert_eq!(lead.header(&run).await.started_at, header.started_at, "no run starts again");

    let missing = TrailStart { trail: "gone".parse().unwrap(), args: Value::Null, limits: TrailLimits::DEFAULT };
    let error = lead.trails.start_fired(&fire(&missing), &missing).await.unwrap_err();
    assert!(error.contains("no trail is saved as gone"), "{error}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The outcome of `order` once its machine's daemon recorded it.
async fn order_outcome(js: &async_nats::jetstream::Context, order: &AdminOrder) -> AdminOutcome {
    let machines = DirectKv::new(js, None, MACHINES_BUCKET).await.unwrap();
    let mut decided = None;
    eventually("the order is decided", async || {
        decided = machines.get(&order.outcome_key()).await.unwrap();
        decided.is_some()
    })
    .await;
    serde_json::from_slice(&decided.unwrap()).unwrap()
}

#[tokio::test]
async fn admin_orders_stop_and_resume_a_run_once_the_conductor_s_daemon_is_back() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (_, js) =
        ants_daemon::bus::connect(&server.url, None, None, &ants_test_support::human(), "admin").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(r#"await agent("go", ant="dev")"#, TrailLimits::default()).await;
    let step = lead.step().await;

    let (account, key, alice) = (nkeys::KeyPair::new_account(), nkeys::KeyPair::new_user(), ant("alice"));
    let jwt = user_jwt(&account, &key, &alice, ants_proto::ADMIN_TAG);
    let m1: Machine = "m1".parse().unwrap();
    let order = |op| {
        let mut order = AdminOrder::new(alice.clone(), m1.clone(), ant("lead"), op, jwt.clone());
        order.signature = data_encoding::BASE64URL_NOPAD.encode(&key.sign(&order.signed()).unwrap());
        order
    };
    let publish = async |order: &AdminOrder| {
        let message = async_nats::jetstream::message::PublishMessage::build()
            .payload(serde_json::to_vec(order).unwrap().into())
            .message_id(order.id.to_string());
        js.send_publish(order.subject(), message).await.unwrap().await.unwrap();
    };

    // m1's daemon is away: the order waits in the stream.
    let stop = order(AdminOp::StopTrail { run: run.clone() });
    publish(&stop).await;
    // Back, it takes the order while the daemon serving lead connects, and applies it once lead conducts its runs.
    let restarts = Restarts::default();
    let _served = restarts.serve(&ant("lead"));
    let orders = ants_daemon::admin::Orders {
        machine: m1.clone(),
        nats: server.url.clone(),
        remote: true,
        accounts: vec![account.public_key()],
        config: None,
        creds: None,
        js_domain: None,
        restarts: restarts.clone(),
        providers: Default::default(),
        leaf: None,
        hold: ants_daemon::admin::held::Hold::new(Registry::new(ants_test_support::human())),
    };
    let taking = tokio::spawn(ants_daemon::admin::serve(js.clone(), None, orders));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(lead.header(&run).await.state, RunState::Running, "not stopped before lead conducts its runs");
    let conducted = restarts.conduct(&ant("lead"), &lead.trails);
    let stopped = order_outcome(&js, &stop).await;
    assert_eq!((stopped.state, stopped.admin.as_ref()), (OrderState::Applied, Some(&alice)), "{stopped:?}");
    let waiting = vec![WaitingStep { to: ant("dev"), message: step.id }];
    let answer: TrailOutcome = serde_json::from_value(stopped.result.unwrap()).unwrap();
    assert_eq!(answer, TrailOutcome::Stopped { run: run.clone(), waiting }, "the steps for the admin to withdraw");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: alice.clone() });
    assert!(lead.told().await.text.contains("was stopped by alice"));

    let limits = TrailLimits { steps: 9, turns: 40, ..TrailLimits::DEFAULT };
    let resume = order(AdminOp::ResumeTrail { run: run.clone(), limits: Some(limits) });
    publish(&resume).await;
    let resumed = order_outcome(&js, &resume).await;
    assert_eq!(resumed.state, OrderState::Applied, "{resumed:?}");
    assert_eq!(
        serde_json::from_value::<TrailOutcome>(resumed.result.unwrap()).unwrap(),
        TrailOutcome::Resumed { run: run.clone() }
    );
    assert_eq!(lead.header(&run).await.limits, limits, "the run's record keeps the limits the order gave");
    let step = lead.step().await;
    assert!(lead.reply(&step, "went").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("went") });

    let refusals = [
        (AdminOp::StopTrail { run: run.clone() }, format!("run {run} has ended")),
        (AdminOp::ResumeTrail { run: run.clone(), limits: None }, format!("run {run} is done")),
        (AdminOp::StopTrail { run: RunId::generate() }, "lead conducts no run".to_owned()),
    ];
    for (op, why) in refusals {
        let refused = order(op);
        publish(&refused).await;
        let refused = order_outcome(&js, &refused).await;
        assert_eq!(refused.state, OrderState::Refused, "{refused:?}");
        assert!(refused.reason.as_deref().unwrap().contains(&why), "{why}: {refused:?}");
    }
    drop(conducted);
    taking.abort();
}

/// A paused conductor starts no run, naming how to resume it, and holds the steps of its runs until it resumes.
#[tokio::test]
async fn a_paused_conductor_starts_no_run_and_holds_its_runs_steps_until_resumed() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"[await agent("one", ant="dev"), await agent("two", ant="dev")]"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let first = lead.step().await;
    lead.pause.replace(&ants_daemon::pause::Pause::new(true));
    assert!(lead.reply(&first, "1").await, "a reply waiting is taken");
    assert!(lead.sends_no_step(Duration::from_millis(500)).await, "no step goes out while paused");
    let human = ants_test_support::human();
    let start = StartRun {
        trail: TrailSource::Script(format!("{META}{script}")),
        args: Value::Null,
        owner: Some(human.clone()),
        origin: Origin { by: Starter::Human { user: human }, channel: None },
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
        spends_from: None,
    };
    let refused = lead.trails.start(start).await.unwrap_err();
    assert!(refused.contains("is paused") && refused.contains("'ants resume lead'"), "{refused}");
    lead.pause.replace(&ants_daemon::pause::Pause::new(false));
    let second = lead.step().await;
    assert_eq!(second.text, "two");
    assert!(lead.reply(&second, "2").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(["1", "2"]) });
}
