//! A run failing on compute, session or memory, named in its record, and resumed from it where a resume may continue
//! it; a stop arriving while the script computes, and a resume after the conductor's daemon restarted while the run was
//! stopped.

use super::*;

/// Counts to the number its first step answers before its second step.
const COUNTING: &str = r#"
a = await agent("a", ant="dev")
n = 0
for i in range(int(a)):
    n += 1
b = await agent(f"b {n}", ant="ops")
b
"#;

/// Iterations of [`COUNTING`]'s loop that take a worker well over 300 ms and well under its 10 s compute limit.
const COUNT: &str = "3000000";

/// The state `run` ended in, failed, and the error and limit of it.
async fn failed(lead: &Conductor, run: &RunId) -> (String, Option<RunLimit>) {
    match lead.ended(run).await {
        RunState::Failed { error, limit } => (error, limit),
        state => panic!("not failed: {state:?}"),
    }
}

#[tokio::test]
async fn a_run_past_its_compute_limit_fails_naming_it_and_a_resume_computes_again() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let limits = TrailLimits { compute_ms: 100, ..TrailLimits::default() };
    let run = lead.start(COUNTING, limits).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, COUNT).await);
    let (error, limit) = failed(&lead, &run).await;
    assert_eq!(limit, Some(RunLimit::Compute), "{error}");
    assert!(error.contains("TimeoutError"), "{error}");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await);

    // The resume computes again from the answer its journal holds, and fails the same way.
    lead.trails.resume(&run, &UserId::human(), None).await.unwrap();
    let (again, limit) = failed(&lead, &run).await;
    assert_eq!(limit, Some(RunLimit::Compute), "{again}");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await, "the answered step is not asked again");
}

#[tokio::test]
async fn a_run_past_its_compute_limit_resumes_under_a_raised_one_to_its_end() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(COUNTING, TrailLimits { compute_ms: 100, ..TrailLimits::default() }).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, COUNT).await);
    let (error, limit) = failed(&lead, &run).await;
    assert_eq!(limit, Some(RunLimit::Compute), "{error}");

    // The raised limit reaches the restored session, dumped under the one the run started with.
    lead.trails.resume(&run, &UserId::human(), Some(TrailLimits::default())).await.unwrap();
    let b = lead.step().await;
    assert_eq!(b.text, format!("b {COUNT}"));
    assert!(lead.reply(&b, "B").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("B") });
}

#[tokio::test]
async fn a_run_whose_session_outgrows_its_limit_fails_naming_it_and_resumes_under_a_raised_one() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
a = await agent("a", ant="dev")
big = "x" * int(a)
b = await agent("b", ant="ops")
[len(big), b]
"#;
    let run = lead.start(script, TrailLimits { session_bytes: 50_000, ..TrailLimits::default() }).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, "100000").await);
    let (error, limit) = failed(&lead, &run).await;
    assert_eq!(limit, Some(RunLimit::Session), "{error}");
    assert!(error.contains("passes the limit of 50000 bytes"), "{error}");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await);

    lead.trails.resume(&run, &UserId::human(), Some(TrailLimits::default())).await.unwrap();
    let b = lead.step().await;
    assert!(lead.reply(&b, "B").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!([100_000, "B"]) });
}

#[tokio::test]
async fn a_run_past_its_memory_fails_naming_it_and_takes_no_resume() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
a = await agent("a", ant="dev")
x = []
for i in range(int(a)):
    x.append("a" * 100000)
len(x)
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, "1000").await);
    let (error, limit) = failed(&lead, &run).await;
    assert_eq!(limit, Some(RunLimit::Memory), "{error}");
    assert!(error.contains("MemoryError: memory limit exceeded"), "{error}");
    let told = lead.told().await;
    assert!(told.text.contains(" failed (") && told.text.contains("MemoryError"), "{}", told.text);

    assert!(told.text.contains("No resume continues it: no flag raises its memory limit"), "{}", told.text);

    // No limit raises a script's memory, so a resume, which would fail the same way, is refused.
    let refused = lead.trails.resume(&run, &UserId::human(), Some(TrailLimits::MAX)).await.unwrap_err();
    assert!(refused.contains("failed on its memory limit, which no resume raises"), "{refused}");
    assert!(matches!(lead.header(&run).await.state, RunState::Failed { .. }), "the run stays failed");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await, "the answered step is not asked again");
}

#[tokio::test]
async fn a_run_out_of_turns_fails_naming_it_and_resumes_under_more() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(TWO_STEPS, TrailLimits { turns: 1, ..TrailLimits::default() }).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, "A").await);
    let (error, limit) = failed(&lead, &run).await;
    assert_eq!(limit, Some(RunLimit::Turns), "{error}");
    assert!(error.contains("limit of 1 turns"), "{error}");

    lead.trails
        .resume(&run, &UserId::human(), Some(TrailLimits { turns: 10, ..TrailLimits::default() }))
        .await
        .unwrap();
    let b = lead.step().await;
    assert_eq!(b.to, ant("ops"));
    assert!(lead.reply(&b, "B").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("AB") });
    assert_eq!(lead.header(&run).await.turns, 2);
}

#[tokio::test]
async fn a_stop_while_the_script_computes_takes_effect_once_it_ends_listing_the_steps_it_then_waits_on() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(COUNTING, TrailLimits::default()).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, COUNT).await);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let started = std::time::Instant::now();
    let waiting = lead.trails.stop(&run, &UserId::human()).await.unwrap();
    let b = lead.step().await;
    assert!(started.elapsed() > Duration::from_millis(100), "the stop waited for the computation to end");
    assert_eq!(b.text, format!("b {COUNT}"));
    let waiting: Vec<_> = waiting.iter().map(|step| (step.to.clone(), step.message)).collect();
    assert_eq!(waiting, [(ant("ops"), b.id)], "the step sent as the computation ended is for the human to withdraw");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: UserId::human() });
    assert!(lead.reply(&b, "late").await, "the stopped step's reply is not prompted");
    assert!(lead.sends_no_step(Duration::from_millis(500)).await);

    lead.trails.resume(&run, &UserId::human(), None).await.unwrap();
    let again = lead.step().await;
    assert_ne!(again.id, b.id);
    assert!(lead.reply(&again, "B").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("B") });
}

#[tokio::test]
async fn a_run_stopped_before_its_daemon_restarted_resumes_from_its_record() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let run = lead.start(TWO_STEPS, TrailLimits::default()).await;
    let a = lead.step().await;
    assert!(lead.reply(&a, "A").await);
    let b = lead.step().await;
    lead.trails.stop(&run, &UserId::human()).await.unwrap();
    drop(lead);

    let mut again = conductor(&server, &ants_binary(), Vec::new()).await;
    assert!(again.sends_no_step(Duration::from_secs(1)).await, "a stopped run stays stopped");
    assert!(again.reply(&b, "late").await, "the stopped step's reply is not prompted after the restart");
    assert_eq!(again.header(&run).await.state, RunState::Stopped { by: UserId::human() });
    again.trails.resume(&run, &UserId::human(), None).await.unwrap();
    let fresh = again.step().await;
    assert_eq!((fresh.to.as_str(), fresh.text.as_str()), ("ops", "b"));
    assert!(fresh.id != b.id && fresh.thread_id != b.thread_id, "sent anew on a fresh thread");
    assert!(again.reply(&fresh, "B").await);
    assert_eq!(again.ended(&run).await, RunState::Done { result: json!("AB") });
}
