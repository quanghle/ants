//! trail(): a call answered by a child run of a saved trail, which the conductor runs within its parent's limits.

use super::*;

/// A conductor with the trails `saved`, by name and script, saved on its machine in `dir`.
async fn conductor_with_saved(server: &NatsServer, dir: &Path, saved: &[(&str, &str)]) -> Conductor {
    for (name, script) in saved {
        std::fs::write(dir.join(format!("{name}.py")), format!("{META}{script}")).unwrap();
    }
    conductor_saving(server, &ants_binary(), Vec::new(), Some(dir.to_owned())).await
}

impl Conductor {
    /// Start a run of the saved trail `name` for `origin`, owned by the human, reporting on `thread`.
    pub(super) async fn start_saved(&self, name: &str, origin: Origin, limits: TrailLimits, thread: ThreadId) -> RunId {
        let start = StartRun {
            trail: TrailSource::Saved(name.parse().unwrap()),
            args: Value::Null,
            owner: Some(UserId::human()),
            origin,
            thread,
            limits,
        };
        self.trails.start(start).await.unwrap()
    }

    /// The result of `run`, which ends done.
    pub(super) async fn result(&self, run: &RunId) -> Value {
        match self.ended(run).await {
            RunState::Done { result } => result,
            state => panic!("not done: {state:?}"),
        }
    }
}

pub(super) fn human_origin() -> Origin {
    Origin { by: Starter::Human { user: UserId::human() }, channel: None }
}

const ECHO: &str = "await agent(args[\"say\"], ant=\"dev\")\n";

#[tokio::test]
async fn a_child_run_s_result_answers_the_call_and_its_failure_raises_there() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child");
    let saved = [("echo", ECHO), ("broken", "raise ValueError(\"bad input\")\n")];
    let mut lead = conductor_with_saved(&server, &dir, &saved).await;
    let thread = ThreadId::generate();
    let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let script = r#"
a = await trail("echo", args={"say": "a"})
try:
    b = await trail("broken")
except RuntimeError as e:
    b = str(e)
{"a": a, "b": b}
"#;
    let run = lead.start_on(script, TrailLimits::default(), thread.clone()).await;
    let (started, stage) = step_status(&mut status).await;
    assert_eq!(stage, StepStage::Started);
    let StatusUpdate::Step { run: of, label, to, step_thread, .. } = started.update else { unreachable!() };
    assert_eq!((&of, label.as_deref(), &to), (&run, Some("trail echo"), &ant("lead")));

    let step = lead.step().await;
    let child = step.trail_step.as_ref().unwrap().run.clone();
    assert_ne!(child, run, "the step is the child run's");
    let header = lead.header(&child).await;
    let parent = header.parent.as_ref().expect("a parent");
    assert_eq!((&parent.run, parent.call, &parent.origin_thread), (&run, 0, &thread));
    assert_eq!((&header.thread, header.trail().map(ToString::to_string)), (&step_thread, Some("echo".into())));
    let marks = step.trail_step.as_ref().unwrap();
    let script = header.leg().map(|leg| ants_proto::script_sha256(&leg.script));
    assert_eq!((marks.trail.as_ref(), &marks.script), (header.trail(), &script), "the step names its trail's script");
    assert!(marks.script.is_some());
    assert_eq!((header.owner.as_ref(), &header.origin), (Some(&UserId::human()), &human_origin()));
    assert_eq!(step.origin_thread.as_ref(), Some(&thread), "the child's steps link to the root run's thread");
    assert_eq!(step.owner, Some(UserId::human()));
    assert!(lead.reply(&step, "A").await);
    assert_eq!(step_status(&mut status).await.1, StepStage::Answered);

    let result = lead.result(&run).await;
    assert_eq!(result["a"], "A");
    let failed = result["b"].as_str().unwrap();
    assert!(failed.starts_with("trail broken failed: ") && failed.contains("bad input"), "{failed}");
    assert_eq!(lead.ended(&child).await, RunState::Done { result: json!("A") });
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let marker = runs.get(trail_thread_step_key(&ant("lead"), &thread, &step_thread)).await.unwrap();
    let marker: TrailStepThread = serde_json::from_slice(&marker.expect("kept under the run's thread")).unwrap();
    let kept = (&marker.run, marker.call, &marker.to, &marker.run_thread, marker.label.as_deref());
    assert_eq!(kept, (&run, 0, &ant("lead"), &thread, Some("trail echo")), "the trail() step's marker");
    let own = runs.get(ants_proto::trail_step_key(&ant("lead"), &step_thread)).await.unwrap();
    assert!(own.is_none(), "the child run's thread is no step thread");
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.turns), (3, 1), "the children's steps and turns count as the parent's");
    let told = lead.told().await;
    assert_eq!(told.thread_id, thread, "only the parent tells its owner its end: {told:?}");
    assert!(lead.sends_no_step(Duration::from_millis(300)).await && lead.told.is_empty(), "{:?}", lead.told);
    let notes = lead.notes(&step_thread, 2).await;
    let start = format!("Run {child} of trail t started, answering call 0 of run {run}: a test");
    assert_eq!(notes[0], start, "the child's start is noted on its thread");
    assert!(notes.last().unwrap().contains(" is done "), "the child's end is noted on its thread: {notes:?}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_trail_calling_itself_an_ancestor_or_no_saved_trail_is_refused() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-recursion");
    let caught = |call: &str| format!("try:\n    r = await {call}\nexcept RuntimeError as e:\n    r = str(e)\nr\n");
    let saved = [
        ("itself", caught("trail(\"itself\")")),
        ("ping", "await trail(\"pong\")\n".to_owned()),
        ("pong", caught("trail(\"ping\")")),
        ("unparsable", "def (\n".to_owned()),
    ];
    let saved: Vec<(&str, &str)> = saved.iter().map(|(name, script)| (*name, script.as_str())).collect();
    let lead = conductor_with_saved(&server, &dir, &saved).await;

    let run = lead.start_saved("itself", human_origin(), TrailLimits::default(), ThreadId::generate()).await;
    let refused = lead.result(&run).await;
    let refused = refused.as_str().unwrap();
    assert!(
        refused.contains(&format!("trail itself is on the route of run {run}, among the calling runs")),
        "{refused}"
    );

    let run = lead.start_saved("ping", human_origin(), TrailLimits::default(), ThreadId::generate()).await;
    let refused = lead.result(&run).await;
    let refused = refused.as_str().unwrap();
    assert!(refused.contains(&format!("trail ping is on the route of run {run}, among the calling runs")), "{refused}");
    assert_eq!(lead.header(&run).await.steps, 1, "the refused call counts as no step");

    let script = r#"
out = []
for name in ["gone", "unparsable"]:
    try:
        out.append(await trail(name))
    except RuntimeError as e:
        out.append(str(e))
out
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let refused = lead.result(&run).await;
    assert!(refused[0].as_str().unwrap().contains("no trail is saved as gone"), "{refused}");
    assert!(refused[1].as_str().unwrap().to_lowercase().contains("syntax"), "{refused}");
    assert_eq!(lead.header(&run).await.steps, 0, "calls starting no child run count as no step");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_trail_any_leg_of_a_calling_run_went_through_is_refused() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-recursion-route");
    let caught = |call: &str| format!("try:\n    r = await {call}\nexcept RuntimeError as e:\n    r = str(e)\nr\n");
    let saved = [
        ("outer", "await trail(\"inner\")\n".to_owned()),
        ("inner", "await goto(\"onward\")\n".to_owned()),
        ("onward", caught("trail(\"outer\")")),
        ("first", "await goto(\"second\")\n".to_owned()),
        ("second", caught("trail(\"first\")")),
    ];
    let saved: Vec<(&str, &str)> = saved.iter().map(|(name, script)| (*name, script.as_str())).collect();
    let lead = conductor_with_saved(&server, &dir, &saved).await;

    let run = lead.start_saved("outer", human_origin(), TrailLimits::default(), ThreadId::generate()).await;
    let refused = lead.result(&run).await;
    let refused = refused.as_str().unwrap();
    let recursion = format!("trail outer is on the route of run {run}, among the calling runs: no trail calls itself");
    assert!(refused.contains(&recursion), "an ancestor's trail, called from a leg a goto() went to: {refused}");
    assert_eq!(lead.header(&run).await.steps, 1, "the refused call counts as no step");

    let run = lead.start_saved("first", human_origin(), TrailLimits::default(), ThreadId::generate()).await;
    let refused = lead.result(&run).await;
    let refused = refused.as_str().unwrap();
    assert!(
        refused.contains(&format!("trail first is on the route of run {run}")),
        "the run's own earlier leg: {refused}"
    );
    assert_eq!(lead.header(&run).await.steps, 0);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn three_child_runs_go_in_parallel() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-children");
    let mut lead = conductor_with_saved(&server, &dir, &[("echo", ECHO)]).await;
    let script = r#"
await parallel(trail("echo", args={"say": "a"}), trail("echo", args={"say": "b"}), trail("echo", args={"say": "c"}))
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let steps = [lead.step().await, lead.step().await, lead.step().await];
    let children: std::collections::HashSet<_> =
        steps.iter().map(|step| step.trail_step.as_ref().unwrap().run.clone()).collect();
    assert_eq!(children.len(), 3, "each step is a child run's of its own");
    for step in &steps {
        assert!(lead.reply(step, &step.text.to_uppercase()).await);
    }
    assert_eq!(lead.result(&run).await, json!(["A", "B", "C"]));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_run_s_limits_cover_the_steps_and_turns_of_its_children() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-limits");
    let many = r#"
out = []
for say in ["a", "b", "c"]:
    try:
        out.append(await agent(say, ant="dev"))
    except RuntimeError as e:
        out.append(str(e))
out
"#;
    let mut lead = conductor_with_saved(&server, &dir, &[("many", many)]).await;

    let run = lead.start("await trail(\"many\")", TrailLimits { steps: 3, ..TrailLimits::default() }).await;
    for _ in 0..2 {
        let step = lead.step().await;
        assert!(lead.reply(&step, &step.text.to_uppercase()).await);
    }
    assert_eq!(lead.result(&run).await, json!(["A", "B", "the run reached its limit of 3 steps"]));
    assert_eq!(lead.header(&run).await.steps, 3);

    let run = lead.start("await trail(\"many\")", TrailLimits { turns: 2, ..TrailLimits::default() }).await;
    for _ in 0..2 {
        let step = lead.step().await;
        assert!(lead.reply(&step, &step.text.to_uppercase()).await);
    }
    let RunState::Failed { error, limit } = lead.ended(&run).await else { panic!("not failed") };
    assert!(error.contains("trail many failed: the run reached its limit of 2 turns"), "{error}");
    assert_eq!(limit, Some(RunLimit::Turns), "failed on its child's turns, its own");
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.turns), (3, 2));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn stopping_a_run_stops_its_child_and_resuming_it_resumes_the_child() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-stop");
    let mut lead = conductor_with_saved(&server, &dir, &[("echo", ECHO)]).await;
    let run = lead.start("await trail(\"echo\", args={\"say\": \"a\"})", TrailLimits::default()).await;
    let step = lead.step().await;
    let child = step.trail_step.as_ref().unwrap().run.clone();

    let human = UserId::human();
    let waiting = lead.trails.stop(&run, &human).await.unwrap();
    let waiting: Vec<_> = waiting.iter().map(|step| (step.to.clone(), step.message)).collect();
    assert_eq!(waiting, [(ant("dev"), step.id)], "the child's step is for the human to withdraw");
    assert_eq!(lead.header(&child).await.state, RunState::Stopped { by: human.clone() });
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: human.clone() });
    let told = lead.told().await;
    assert!(told.text.starts_with(&format!("Run {run} was stopped by human")), "{}", told.text);
    let ended = lead.notes(&lead.header(&child).await.thread, 2).await.pop().unwrap();
    assert!(ended.ends_with(&format!("; `ants trail resume {run}` continues it.")), "the parent's resume: {ended}");
    assert!(lead.reply(&step, "late").await, "the stopped child's step's reply is not prompted");
    assert!(lead.sends_no_step(Duration::from_secs(1)).await);

    lead.trails.resume(&run, &human, None).await.unwrap();
    let again = lead.step().await;
    assert_ne!(again.id, step.id, "the child sends its step anew");
    assert_eq!(again.trail_step.as_ref().unwrap().run, child, "the same child run resumes");
    assert!(lead.reply(&again, "A").await);
    assert_eq!(lead.result(&run).await, json!("A"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn resuming_a_run_failed_on_its_child_s_limit_resumes_the_child() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-limit-resume");
    let each = "out = []\nfor say in [\"a\", \"b\", \"c\"]:\n    out.append(await agent(say, ant=\"dev\"))\nout\n";
    let mut lead = conductor_with_saved(&server, &dir, &[("each", each)]).await;
    let limits = TrailLimits { steps: 3, ..TrailLimits::default() };
    let run = lead.start("await trail(\"each\")", limits).await;
    let mut child = None;
    for _ in 0..2 {
        let step = lead.step().await;
        child = step.trail_step.as_ref().map(|step| step.run.clone());
        assert!(lead.reply(&step, &step.text.to_uppercase()).await);
    }
    let child = child.unwrap();
    let RunState::Failed { error, limit } = lead.ended(&run).await else { panic!("not failed") };
    assert!(
        error.contains("trail each failed: ") && error.ends_with("the run reached its limit of 3 steps"),
        "{error}"
    );
    assert_eq!(limit, Some(RunLimit::Steps));
    assert_eq!(lead.header(&child).await.state.failed_on(), Some(RunLimit::Steps));

    let raised = TrailLimits { steps: 10, ..limits };
    lead.trails.resume(&run, &UserId::human(), Some(raised)).await.unwrap();
    let step = lead.step().await;
    assert_eq!((step.text.as_str(), &step.trail_step.as_ref().unwrap().run), ("c", &child), "the child resumes");
    assert!(lead.reply(&step, "C").await);
    assert_eq!(lead.result(&run).await, json!(["A", "B", "C"]));
    assert_eq!(lead.ended(&child).await, RunState::Done { result: json!(["A", "B", "C"]) });
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn resuming_a_run_failed_on_its_child_s_limit_starts_the_call_anew_when_the_child_left_no_record() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-limit-gone");
    let each = "out = []\nfor say in [\"a\", \"b\"]:\n    out.append(await agent(say, ant=\"dev\"))\nout\n";
    let mut lead = conductor_with_saved(&server, &dir, &[("each", each)]).await;
    let limits = TrailLimits { steps: 2, ..TrailLimits::default() };
    let run = lead.start("await trail(\"each\")", limits).await;
    let a = lead.step().await;
    let child = a.trail_step.as_ref().unwrap().run.clone();
    assert!(lead.reply(&a, "A").await);
    assert_eq!(lead.ended(&run).await.failed_on(), Some(RunLimit::Steps));
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    runs.delete(trail_run_key(&ant("lead"), &child)).await.unwrap();

    lead.trails.resume(&run, &UserId::human(), Some(TrailLimits { steps: 10, ..limits })).await.unwrap();
    let again = lead.step().await;
    assert_eq!((again.text.as_str(), &again.trail_step.as_ref().unwrap().run), ("a", &child), "the child starts anew");
    assert!(lead.reply(&again, "A").await);
    let b = lead.step().await;
    assert!(lead.reply(&b, "B").await);
    assert_eq!(lead.result(&run).await, json!(["A", "B"]));
    assert_eq!(lead.header(&run).await.steps, 4, "the call's own step is counted once");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_daemon_restarted_after_resuming_a_child_failed_on_its_steps_finishes_both() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-limit-restart");
    let each = "out = []\nfor say in [\"a\", \"b\"]:\n    out.append(await agent(say, ant=\"dev\"))\nout\n";
    let mut lead = conductor_with_saved(&server, &dir, &[("each", each)]).await;
    let limits = TrailLimits { steps: 2, ..TrailLimits::default() };
    let run = lead.start("await trail(\"each\")", limits).await;
    let a = lead.step().await;
    let child = a.trail_step.as_ref().unwrap().run.clone();
    assert!(lead.reply(&a, "A").await);
    assert_eq!(lead.ended(&run).await.failed_on(), Some(RunLimit::Steps));
    assert_eq!(lead.header(&child).await.state.failed_on(), Some(RunLimit::Steps));

    lead.trails.resume(&run, &UserId::human(), Some(TrailLimits { steps: 3, ..limits })).await.unwrap();
    let b = lead.step().await;
    assert_eq!((b.text.as_str(), &b.trail_step.as_ref().unwrap().run), ("b", &child), "the child resumes");
    drop(lead);

    let again = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    assert!(again.reply(&b, "B").await);
    assert_eq!(again.result(&run).await, json!(["A", "B"]), "the reply answers the child's call");
    assert_eq!(again.ended(&child).await, RunState::Done { result: json!(["A", "B"]) });
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_daemon_restarted_while_a_child_runs_finishes_both() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-restart");
    let mut lead = conductor_with_saved(&server, &dir, &[("echo", ECHO)]).await;
    let script = r#"
first = await trail("echo", args={"say": "a"})
second = await trail("echo", args={"say": "b"})
[first, second]
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let a = lead.step().await;
    drop(lead);

    let mut again = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    assert!(again.sends_no_step(Duration::from_secs(2)).await, "no step is sent twice");
    assert!(again.reply(&a, "A").await, "the restored child takes the reply of its step sent before the restart");
    let b = again.step().await;
    assert_eq!(b.text, "b");
    assert!(again.reply(&b, "B").await);
    assert_eq!(again.result(&run).await, json!(["A", "B"]));
    assert!(again.sends_no_step(Duration::from_millis(300)).await, "no child runs twice");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn the_child_of_an_untrusted_run_is_untrusted() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-child-untrusted");
    let saved = [("echo", ECHO), ("relay", "await trail(\"echo\", args={\"say\": \"a\"})\n")];
    let mut lead = conductor_with_saved(&server, &dir, &saved).await;
    let origin = Origin { by: Starter::Webhook { user: ant("github") }, channel: None };
    let run = lead.start_saved("relay", origin.clone(), TrailLimits::default(), ThreadId::generate()).await;
    let step = lead.step().await;
    let child = step.trail_step.as_ref().unwrap().run.clone();
    assert_eq!(lead.header(&child).await.origin, origin);
    assert!(step.allow.is_some(), "the child's step is as untrusted as the run: {step:?}");
    assert!(lead.reply(&step, "A").await);
    assert_eq!(lead.result(&run).await, json!("A"));
    std::fs::remove_dir_all(&dir).unwrap();
}
