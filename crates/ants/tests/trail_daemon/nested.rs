//! Runs an ant starts with `run_trail` while answering a step: they spend from the step's run, take no trail on its
//! route, and what they spent comes back with the reply to the step.

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

#[tokio::test]
async fn a_run_started_answering_a_step_spends_from_the_step_s_run_and_takes_no_trail_on_its_route() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-nested");
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    std::fs::write(saved.join("deploy.py"), format!("{META}{DEPLOY}")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved.clone())).await;
    let (tx, mut dev_sent) = mpsc::unbounded_channel::<Envelope>();
    let outbox: ants_daemon::mcp::Outbox = Arc::new(move |envelope| {
        let tx = tx.clone();
        Box::pin(async move {
            tx.send(envelope).unwrap();
            Ok(())
        })
    });
    let (dev_trails, dev) = ant_mcp_sending(&server, "dev", &dir, &saved, outbox).await;
    let serving = tokio::spawn({
        let trails = dev_trails.clone();
        async move { trails.serve().await }
    });

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
    let bounds = step.trail_step.as_deref().and_then(|step| step.bounds.clone()).expect("the step tells its bounds");
    assert_eq!((bounds.steps, bounds.route.len()), (3, 1), "3 steps left, its route deploy: {bounds:?}");
    let _held = dev.begin(&step);
    let client = mcp_client(&dev, &step.thread_id).await;

    let (ok, text) = call(&client, "run_trail", json!({"name": "deploy"})).await;
    assert!(!ok && text.contains(&format!("on the route of run {root}")) && text.contains("(deploy)"), "{text}");
    let (ok, text) =
        call(&client, "run_trail", json!({"script": format!("{META}{FOUR}"), "limits": {"steps": 100}})).await;
    assert!(ok && text.contains(&format!("It spends from run {root} of lead")), "{text}");
    let nested = started(&text);
    let header = dev_trails.run(&nested).await.unwrap().unwrap();
    assert_eq!(header.limits.steps, 3, "cut to what the step's run has left");
    let from = header.spends_from.expect("the run spends from the step");
    assert_eq!((&from.conductor, &from.run, from.call), (&ant("lead"), &root, step.trail_step.as_ref().unwrap().call));
    let (ok, text) = call(&client, "trail_status", json!({"run": nested.as_str()})).await;
    assert!(ok && text.contains(&format!("Started answering call {} of run {root} of lead", from.call)), "{text}");

    for _ in 0..3 {
        let sent = loop {
            let sent = tokio::time::timeout(Duration::from_secs(30), dev_sent.recv()).await.unwrap().unwrap();
            if sent.trail_step.is_some() {
                break sent;
            }
        };
        assert!(dev_trails.take(&sent.reply(ant("x"), "done", Role::Ant).unwrap()).await.unwrap());
    }
    let mut ended = None;
    eventually("the nested run fails on its steps", async || {
        ended = dev_trails.run(&nested).await.unwrap().filter(TrailRun::finished);
        ended.is_some()
    })
    .await;
    let ended = ended.unwrap();
    let RunState::Failed { error, limit } = &ended.state else { panic!("not failed: {:?}", ended.state) };
    assert_eq!(*limit, Some(RunLimit::Steps), "{error}");
    assert!(error.contains("limit of 3 steps"), "the fourth step raises: {error}");
    assert_eq!((ended.steps, ended.turns), (3, 3));

    let mut reply = step.reply(ant("dev"), "built", Role::Ant).unwrap();
    reply.trail_spent = dev.trail_spent(&step, &reply).await;
    assert_eq!(reply.trail_spent, Some(TrailSpent { steps: 3, turns: 3 }));
    assert_eq!(dev.trail_spent(&step, &reply).await, None, "what was told is not told again");
    assert!(lead.trails.take(&reply).await.unwrap());
    let RunState::Failed { error, limit } = lead.ended(&root).await else { panic!("the root run did not fail") };
    assert_eq!(limit, Some(RunLimit::Steps), "its second step is past its steps: {error}");
    let header = lead.header(&root).await;
    assert_eq!((header.steps, header.turns), (4, 4), "its step and the nested run's three, and their turns");

    let asked = Envelope::new(UserId::human(), ant("dev"), "fan out").with_owner(Some(UserId::human()));
    let _asked = dev.begin(&asked);
    let client = mcp_client(&dev, &asked.thread_id).await;
    let (ok, text) =
        call(&client, "run_trail", json!({"script": format!("{META}{FOUR}"), "limits": {"steps": 7}})).await;
    assert!(ok && !text.contains("spends from"), "{text}");
    let header = dev_trails.run(&started(&text)).await.unwrap().unwrap();
    assert_eq!((header.limits.steps, header.spends_from), (7, None), "a thread answering no step keeps its limits");
    serving.abort();
}
