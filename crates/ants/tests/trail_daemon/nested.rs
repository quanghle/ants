//! Runs an ant starts with `run_trail` while answering a step: they spend from the step's grant, take no trail on its
//! route, and what they spent goes back to the step's conductor with the reply to the step and as each of them ends.

use ants_proto::TrailSpent;

use super::tip_tools::ant_mcp_sending;
use super::*;

/// The saved trail `deploy`: two steps to `dev`, one after the other.
const DEPLOY: &str = "await agent(\"build it\", ant=\"dev\")\nawait agent(\"check it\", ant=\"dev\")\n";

/// A script asking `x` four steps, one after the other.
const FOUR: &str = "for prompt in [\"a\", \"b\", \"c\", \"d\"]:\n    await agent(prompt, ant=\"x\")\n";

/// The run `run_trail` says it started.
fn started(text: &str) -> RunId {
    text.strip_prefix("Started run ").unwrap().split_once(' ').unwrap().0.parse().unwrap()
}

/// An outbox handing what it sends to the receiver.
fn channel_outbox() -> (ants_daemon::mcp::Outbox, mpsc::UnboundedReceiver<Envelope>) {
    let (tx, rx) = mpsc::unbounded_channel::<Envelope>();
    let outbox: ants_daemon::mcp::Outbox = Arc::new(move |envelope| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(envelope).unwrap();
            Ok(())
        })
    });
    (outbox, rx)
}

fn serve(trails: &Trails) -> JoinHandle<()> {
    let trails = trails.clone();
    tokio::spawn(async move { trails.serve().await })
}

/// The session `lead` keeps of `run`.
async fn session(lead: &Conductor, run: &RunId) -> TrailSession {
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    serde_json::from_slice(&runs.get(trail_session_key(&ant("lead"), run)).await.unwrap().unwrap()).unwrap()
}

/// Answers the steps the two runs send `x` until both ended and each told the step's conductor what was spent; returns
/// those notices.
async fn answer_until_ended(
    trails: &Trails,
    sent: &mut mpsc::UnboundedReceiver<Envelope>,
    step: &Envelope,
    runs: [&RunId; 2],
) -> Vec<Envelope> {
    let mut spent_notices = Vec::new();
    let mut answered = 0;
    let ended = |run: &RunId| {
        let trails = trails.clone();
        let run = run.clone();
        async move { trails.run(&run).await.unwrap().is_some_and(|header| header.finished()) }
    };
    while answered < 3 || !ended(runs[0]).await || !ended(runs[1]).await || spent_notices.len() < 2 {
        let out = tokio::time::timeout(Duration::from_secs(30), sent.recv()).await.unwrap().unwrap();
        if out.trail_step.is_some() {
            assert!(trails.take(&out.reply(ant("x"), "done", Role::Ant).unwrap()).await.unwrap());
            answered += 1;
        } else if out.notice == Some(NoticeKind::RunSpent) {
            assert_eq!((&out.to, &out.thread_id), (&ant("lead"), &step.thread_id), "on the step's thread");
            spent_notices.push(out);
        }
    }
    assert_eq!(answered, 3, "two steps of the first run, one of the second, each failing past its steps");
    spent_notices
}

#[tokio::test]
async fn runs_started_answering_a_step_share_its_grant_and_tell_all_they_spent_across_a_restart() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-nested");
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    std::fs::write(saved.join("deploy.py"), format!("{META}{DEPLOY}")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved.clone())).await;
    let (outbox, mut dev_sent) = channel_outbox();
    let (dev_trails, dev) = ant_mcp_sending(&server, "dev", &dir, &saved, outbox.clone()).await;
    let serving = serve(&dev_trails);

    let start = StartRun {
        trail: TrailSource::Saved("deploy".parse().unwrap()),
        args: Value::Null,
        owner: Some(UserId::human()),
        origin: Origin { by: Starter::Human { user: UserId::human() }, channel: None },
        thread: ThreadId::generate(),
        limits: TrailLimits { steps: 4, ..TrailLimits::default() },
        spends_from: None,
    };
    let root = lead.trails.start(start).await.unwrap();
    let step = lead.step().await;
    let bounds = step.trail_step.as_deref().and_then(|step| step.bounds.clone()).expect("the step tells its grant");
    assert_eq!(
        (bounds.steps, bounds.turns, bounds.route.len()),
        (3, 192, 1),
        "a lone step is granted what is left but itself and its turn budget, its route deploy: {bounds:?}"
    );
    let _held = dev.begin(&step);
    let client = mcp_client(&dev, &step.thread_id).await;

    let (ok, text) = call(&client, "run_trail", json!({"name": "deploy"})).await;
    assert!(!ok && text.contains(&format!("on the route of run {root}")) && text.contains("(deploy)"), "{text}");
    let four = format!("{META}{FOUR}");
    let (ok, text) = call(&client, "run_trail", json!({"script": four, "limits": {"steps": 2, "turns": 10}})).await;
    assert!(ok && text.contains(&format!("It spends from run {root} of lead")), "{text}");
    let first = started(&text);
    let (ok, text) = call(&client, "run_trail", json!({"script": four, "limits": {"steps": 100}})).await;
    assert!(ok, "{text}");
    let second = started(&text);
    let header = dev_trails.run(&second).await.unwrap().unwrap();
    assert_eq!(
        (header.limits.steps, header.limits.turns),
        (1, 182),
        "cut to what the grant leaves after the limits of the run still running"
    );
    let from = header.spends_from.expect("the run spends from the step");
    assert_eq!((&from.conductor, &from.run, from.call), (&ant("lead"), &root, step.trail_step.as_ref().unwrap().call));
    let (ok, text) = call(&client, "run_trail", json!({"script": four})).await;
    assert!(!ok && text.contains("no steps left"), "the grant is all taken: {text}");
    let (ok, text) = call(&client, "trail_status", json!({"run": first.as_str()})).await;
    assert!(ok && text.contains(&format!("Started answering call {} of run {root} of lead", from.call)), "{text}");

    let spent_notices = answer_until_ended(&dev_trails, &mut dev_sent, &step, [&first, &second]).await;
    let last = spent_notices.last().unwrap().trail_spent;
    assert_eq!(last, Some(TrailSpent { steps: 3, turns: 3, running: 0 }), "the last run's end tells all, none running");
    for notice in &spent_notices {
        assert!(lead.trails.take(notice).await.unwrap(), "a notice on the step's thread is the conductor's");
    }
    let header = lead.header(&root).await;
    assert_eq!((header.steps, header.turns), (4, 3), "charged as told, stored before the notices are acked");
    let grant = &header.grants[0];
    assert_eq!(grant.spent, TrailSpent { steps: 3, turns: 3, running: 0 }, "{grant:?}");

    serving.abort();
    drop((dev_trails, dev));
    let (dev_trails, dev) = ant_mcp_sending(&server, "dev", &dir, &saved, outbox).await;
    dev_trails.restore().await;
    let serving = serve(&dev_trails);
    let mut reply = step.reply(ant("dev"), "built", Role::Ant).unwrap();
    reply.trail_spent = dev.trail_spent(&step, &reply).await;
    assert_eq!(
        reply.trail_spent,
        Some(TrailSpent { steps: 3, turns: 3, running: 0 }),
        "the restarted daemon still tells all the runs answering the step spent"
    );
    assert!(lead.trails.take(&reply).await.unwrap());
    let RunState::Failed { error, limit } = lead.ended(&root).await else { panic!("the root run did not fail") };
    assert_eq!(limit, Some(RunLimit::Steps), "its second step is past its steps: {error}");
    let header = lead.header(&root).await;
    assert_eq!((header.steps, header.turns), (4, 4), "nothing told twice: its step, the nested three, a reply turn");

    let asked = Envelope::new(UserId::human(), ant("dev"), "fan out").with_owner(Some(UserId::human()));
    let _asked = dev.begin(&asked);
    let client = mcp_client(&dev, &asked.thread_id).await;
    let (ok, text) = call(&client, "run_trail", json!({"script": four, "limits": {"steps": 7}})).await;
    assert!(ok && !text.contains("spends from"), "{text}");
    let header = dev_trails.run(&started(&text)).await.unwrap().unwrap();
    assert_eq!((header.limits.steps, header.spends_from), (7, None), "a thread answering no step keeps its limits");
    serving.abort();
}

#[tokio::test]
async fn steps_out_at_once_share_what_is_left_and_a_grant_comes_back_less_what_was_spent() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
async def first():
    await agent("a", ant="dev")
    return await agent("c", ant="dev")
await parallel(first(), agent("b", ant="dev"))
"#;
    let run = lead.start(script, TrailLimits { steps: 9, ..TrailLimits::default() }).await;
    let (one, two) = (lead.step().await, lead.step().await);
    let (a, b) = if one.text == "a" { (one, two) } else { (two, one) };
    let grant = |step: &Envelope| {
        let bounds = step.trail_step.as_deref().unwrap().bounds.clone().unwrap();
        (bounds.steps, bounds.turns)
    };
    assert_eq!(grant(&a), (3, 92), "half of the 7 steps and 184 turns left once both went");
    assert_eq!(grant(&b), (3, 92));

    let mut reply = a.reply(ant("dev"), "done", Role::Ant).unwrap();
    reply.trail_spent = Some(TrailSpent { steps: 2, turns: 2, running: 0 });
    assert!(lead.trails.take(&reply).await.unwrap());
    assert!(lead.trails.take(&reply).await.unwrap(), "delivered again, reported");
    let c = lead.step().await;
    assert_eq!(c.text, "c");
    assert_eq!(
        grant(&c),
        (1, 89),
        "a's grant given back less what it spent, b's still held: 9 - 5 sent or told - 3 held; 200 - 3 - 16 - 92"
    );
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.turns), (5, 3), "what a reply told is charged once, however often delivered");

    let lean =
        lead.start("await agent(\"x\", ant=\"dev\")\n", TrailLimits { turns: 8, ..TrailLimits::default() }).await;
    let step = lead.step().await;
    assert_eq!(step.trail_step.as_deref().unwrap().run, lean);
    assert_eq!(grant(&step).1, 0, "the step's own turn budget holds the run's 8 turns: none to grant");
}

#[tokio::test]
async fn a_reply_whose_re_ask_could_not_go_out_is_asked_about_once_when_delivered_again() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let script = r#"
schema = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
try:
    await agent("count", ant="dev", schema=schema)
except RuntimeError as e:
    result = str(e)
result
"#;
    let run = lead.start(script, TrailLimits::default()).await;
    let step = lead.step().await;
    let mut reply = step.reply(ant("dev"), "about seven", Role::Ant).unwrap();
    reply.trail_spent = Some(TrailSpent { steps: 1, turns: 1, running: 0 });

    lead.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(lead.trails.take(&reply).await.is_err(), "the re-ask did not go out");
    let stored = session(&lead, &run).await;
    let call = &stored.pending[0];
    assert_eq!((call.retries, call.message, call.turn_budget, call.sent), (0, step.id, step.turn_budget, true));
    let header = lead.header(&run).await;
    assert_eq!((header.steps, header.turns), (1, 0), "nothing of the reply stored");

    assert!(lead.trails.take(&reply).await.unwrap(), "delivered again");
    let again = lead.step().await;
    assert_eq!(again.thread_id, step.thread_id);
    assert_eq!(
        again.trail_step.as_deref().unwrap().bounds,
        step.trail_step.as_deref().unwrap().bounds,
        "the re-ask carries the step's grant, which the recipient's daemon cuts by what its runs spent"
    );
    let stored = session(&lead, &run).await;
    assert_eq!((stored.pending[0].retries, stored.pending[0].message), (1, again.id));
    let header = lead.header(&run).await;
    assert_eq!(
        (header.steps, header.turns),
        (2, 3),
        "the nested step and turn once, the reply's turn and the re-ask's"
    );

    assert!(lead.reply(&again, "{\"n\": \"many\"}").await);
    let last = lead.step().await;
    assert!(lead.reply(&last, "{\"m\": 1}").await);
    let RunState::Done { result } = lead.ended(&run).await else { panic!("not done") };
    let error = result.as_str().unwrap();
    assert!(error.contains("did not match the schema, asked 3 times"), "asked again twice in all: {error}");
}
