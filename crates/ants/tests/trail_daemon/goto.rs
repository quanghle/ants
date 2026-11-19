//! goto(): a run leaves the leg it is on for another trail's waypoint, never one it entered before.

use ants_proto::{MessageMark, TrailLeg, message_mark_key};

use super::children::human_origin;
use super::*;

/// Save `script` as the trail `name` with `waypoints` in `dir`, the conductor's saved trails.
fn save(dir: &Path, name: &str, waypoints: &[&str], script: &str) {
    let meta = json!({"name": name, "description": "a test", "phases": [], "waypoints": waypoints});
    std::fs::write(dir.join(format!("{name}.py")), format!("meta = {meta}\n{script}")).unwrap();
}

/// The trail and waypoint of each leg of `header`'s route.
fn route(header: &TrailRun) -> Vec<(String, Option<String>)> {
    header.route.iter().map(|leg| (leg.trail.as_ref().map_or_default(ToString::to_string), leg.at.clone())).collect()
}

fn entered(trail: &str, at: Option<&str>) -> (String, Option<String>) {
    (trail.to_owned(), at.map(str::to_owned))
}

const TRIAGE: &str = r#"
async def intake(args):
    bug = await agent("what broke?", ant="dev")
    await goto("fix", state={"bug": bug})

async def report(state):
    return {"bug": state["bug"], "released": state["version"]}
"#;

const FIX: &str = r#"
patch = await agent(f"fix {args['bug']}", ant="dev")
await goto("release", state={"bug": args["bug"], "patch": patch})
"#;

const RELEASE: &str = r#"
version = await agent(f"release {args['patch']}", ant="ops")
await goto("triage", at="report", state={"bug": args["bug"], "version": version})
"#;

#[tokio::test]
async fn a_run_goes_from_trail_to_trail_and_back_to_a_later_waypoint_of_the_first() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-goto-route");
    save(&dir, "triage", &["intake", "report"], TRIAGE);
    save(&dir, "fix", &[], FIX);
    save(&dir, "release", &[], RELEASE);
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let thread = ThreadId::generate();
    let run = lead.start_saved("triage", human_origin(), TrailLimits::default(), thread.clone()).await;

    let mut calls = Vec::new();
    for (asked, answer) in [("what broke?", "crash"), ("fix crash", "patched"), ("release patched", "v2")] {
        let step = lead.step().await;
        assert_eq!(step.text, asked);
        let trail_step = step.trail_step.as_ref().unwrap();
        assert_eq!(trail_step.run, run, "every leg's steps are the run's");
        calls.push(trail_step.call);
        assert!(lead.reply(&step, answer).await);
    }
    assert_eq!(lead.result(&run).await, json!({"bug": "crash", "released": "v2"}));
    assert!(calls.is_sorted() && calls.windows(2).all(|pair| pair[0] < pair[1]), "unique across legs: {calls:?}");
    let header = lead.header(&run).await;
    let expected = [
        entered("triage", Some("intake")),
        entered("fix", None),
        entered("release", None),
        entered("triage", Some("report")),
    ];
    assert_eq!(route(&header), expected);
    let first_calls: Vec<u32> = header.route.iter().map(|leg| leg.first_call).collect();
    assert!(first_calls.windows(2).all(|pair| pair[0] < pair[1]), "{first_calls:?}");
    assert_eq!(header.route[3].args, json!({"bug": "crash", "version": "v2"}), "the goto's state is the leg's input");
    assert_eq!((&header.thread, header.steps, header.limits), (&thread, 3, TrailLimits::default()));
    assert!(lead.sends_no_step(Duration::from_millis(300)).await);
    std::fs::remove_dir_all(&dir).unwrap();
}

const LOOP: &str = r#"meta = {"name": "loop", "description": "a test", "phases": [], "waypoints": ["first", "second"]}

async def tried(trail=None, at=None):
    try:
        await goto(trail, at=at)
    except RuntimeError as e:
        return str(e)

async def first(args):
    again = await tried(at="first")
    gone = await tried(trail="gone")
    nowhere = await tried(at="nowhere")
    await goto(at="second", state=[again, gone, nowhere])

async def second(refused):
    return refused
"#;

#[tokio::test]
async fn a_goto_the_conductor_cannot_follow_raises_where_it_is_called() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let start = StartRun {
        trail: TrailSource::Script(LOOP.to_owned()),
        args: Value::Null,
        owner: Some(UserId::human()),
        origin: human_origin(),
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
    };
    let run = lead.trails.start(start).await.unwrap();
    let refused = lead.result(&run).await;
    let [again, gone, nowhere] = [0, 1, 2].map(|at| refused[at].as_str().unwrap().to_owned());
    assert!(again.contains("the run entered its inline script at waypoint first before"), "{again}");
    assert!(gone.contains("no trail is saved as gone"), "{gone}");
    assert!(nowhere.contains("has no waypoint \"nowhere\""), "{nowhere}");
    let header = lead.header(&run).await;
    assert_eq!(route(&header), [entered("", Some("first")), entered("", Some("second"))], "the inline script's own");
    assert_eq!(header.steps, 0, "a goto is no step");
}

const SPLIT: &str = r#"
async def go_when_told():
    told = await agent("go?", ant="ops")
    await parallel(agent("never", ant="dev"), goto(at="after", state=told))

async def fan(args):
    await parallel(agent("a", ant="dev"), go_when_told())

async def after(told):
    return await agent(f"after {told}", ant="dev")
"#;

#[tokio::test]
async fn a_goto_inside_parallel_drops_its_sibling_steps() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-goto-parallel");
    save(&dir, "split", &["fan", "after"], SPLIT);
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let thread = ThreadId::generate();
    let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let run = lead.start_saved("split", human_origin(), TrailLimits::default(), thread).await;
    let (first, second) = (lead.step().await, lead.step().await);
    let (a, go) = if first.text == "a" { (first, second) } else { (second, first) };
    assert!(lead.reply(&go, "yes").await);

    let after = lead.step().await;
    assert_eq!(after.text, "after yes", "the sibling steps are dropped, the next leg's sent");
    let dropped = a.trail_step.as_ref().unwrap().call;
    loop {
        let (event, stage) = step_status(&mut status).await;
        let StatusUpdate::Step { call, .. } = event.update else { unreachable!() };
        if stage == StepStage::Dropped {
            assert_eq!(call, dropped, "only the sent sibling step is dropped");
            break;
        }
    }
    assert!(lead.reply(&a, "late").await, "a dropped step's reply is not prompted");
    assert!(lead.reply(&after, "done").await);
    assert_eq!(lead.result(&run).await, json!("done"));
    assert!(lead.sends_no_step(Duration::from_millis(300)).await, "{:?}", lead.told);
    std::fs::remove_dir_all(&dir).unwrap();
}

const SECOND: &str = r#"
b = await agent(f"b {args}", ant="ops")
c = await agent(f"c {b}", ant="dev")
args + b + c
"#;

#[tokio::test]
async fn a_leg_goes_on_from_its_pinned_script_after_a_restart_or_a_replay() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    for replayed in [false, true] {
        let dir = scratch_dir("trail-goto-pinned");
        save(&dir, "first", &[], "a = await agent(\"a\", ant=\"dev\")\nawait goto(\"second\", state=a)\n");
        save(&dir, "second", &[], SECOND);
        let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
        let run = lead.start_saved("first", human_origin(), TrailLimits::default(), ThreadId::generate()).await;
        for (asked, answer) in [("a", "A"), ("b A", "B")] {
            let step = lead.step().await;
            assert_eq!(step.text, asked);
            assert!(lead.reply(&step, answer).await);
        }
        let c = lead.step().await;
        assert_eq!(c.text, "c B");
        eventually("the step is stored sent", async || {
            lead.session(&run).await.0.pending.iter().any(|call| call.message == c.id && call.sent)
        })
        .await;
        lead.serving.abort();
        save(&dir, "second", &[], "await agent(\"changed\", ant=\"ops\")\n");
        if replayed {
            let (stored, mut state) = lead.session(&run).await;
            state["snapshot"] = json!({"ResolveFutures": {"dump_version": 99}});
            lead.put_session(&run, stored, &state).await;
        }
        drop(lead);

        let mut again = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
        assert!(again.sends_no_step(Duration::from_secs(2)).await, "no step is sent twice, replayed: {replayed}");
        assert!(again.reply(&c, "C").await);
        assert_eq!(again.result(&run).await, json!("ABC"), "replayed: {replayed}");
        let header = again.header(&run).await;
        assert_eq!(route(&header), [entered("first", None), entered("second", None)]);
        assert!(header.route[1].script.ends_with(SECOND), "the leg's script is pinned: {}", header.route[1].script);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

#[tokio::test]
async fn a_restart_after_a_goto_stored_its_next_leg_gives_up_the_steps_of_the_leg_left() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-goto-crash");
    save(&dir, "first", &[], "await parallel(agent(\"mine\"), agent(\"yours\", ant=\"dev\"))\n");
    save(&dir, "second", &[], "await agent(f\"next {args}\", ant=\"dev\")\n");
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let thread = ThreadId::generate();
    let run = lead.start_saved("first", human_origin(), TrailLimits::default(), thread.clone()).await;
    let (first, second) = (lead.step().await, lead.step().await);
    let (mine, yours) = if first.text == "mine" { (first, second) } else { (second, first) };
    assert_eq!((&mine.to, &yours.to), (&ant("lead"), &ant("dev")));
    eventually("the steps are stored sent", async || {
        lead.session(&run).await.0.pending.iter().filter(|call| call.sent).count() == 2
    })
    .await;
    lead.serving.abort();
    // The daemon stopped as a goto() stored the run's header with the next leg, before it gave up the leg's steps.
    let mut header = lead.header(&run).await;
    let script = std::fs::read_to_string(dir.join("second.py")).unwrap();
    let started_at = chrono::Utc::now();
    let (trail, args) = (Some("second".parse().unwrap()), json!("moved"));
    header.route.push(TrailLeg { trail, at: None, script, args, first_call: 10, started_at, steps: 0 });
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    runs.put(trail_run_key(&ant("lead"), &run), serde_json::to_vec(&header).unwrap()).await.unwrap();
    drop(lead);

    let (watcher, _) = ants_daemon::bus::connect(&server.url, None, None, &UserId::human(), "watch").await.unwrap();
    let mut status = watcher.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let mut again = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let next = again.step().await;
    assert_eq!(next.text, "next moved", "the next leg goes on");
    let mut dropped = Vec::new();
    while dropped.len() < 2 {
        let (event, stage) = step_status(&mut status).await;
        let StatusUpdate::Step { step_thread, .. } = event.update else { unreachable!() };
        if stage == StepStage::Dropped {
            dropped.push(step_thread);
        }
    }
    dropped.sort();
    let mut left = vec![mine.thread_id.clone(), yours.thread_id.clone()];
    left.sort();
    assert_eq!(dropped, left, "the steps of the leg left are told dropped");
    let attempts = ants_daemon::bus::ensure_attempts(&again.js, None, &ant("lead")).await.unwrap();
    let mark = attempts.get(message_mark_key(&ant("lead"), &mine.id)).await.unwrap().expect("a mark");
    let mark: MessageMark = serde_json::from_slice(&mark).unwrap();
    assert!(matches!(&mark, MessageMark::Withdrawn { by, .. } if *by == ant("lead")), "withdrawn: {mark:?}");
    assert!(again.reply(&yours, "late").await, "a reply to a step given up is not prompted");
    assert!(again.reply(&next, "done").await);
    assert_eq!(again.result(&run).await, json!("done"));
    assert!(again.sends_no_step(Duration::from_millis(300)).await);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn an_untrusted_run_s_child_goes_on_untrusted_and_its_parent_takes_its_end() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-goto-untrusted");
    save(&dir, "relay", &[], "await trail(\"hop\")\n");
    save(&dir, "hop", &[], "await goto(\"echo\", state={\"say\": \"a\"})\n");
    save(&dir, "echo", &[], "await agent(args[\"say\"], ant=\"dev\")\n");
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let origin = Origin { by: Starter::Webhook { user: ant("github") }, channel: None };
    let run = lead.start_saved("relay", origin.clone(), TrailLimits::default(), ThreadId::generate()).await;
    let step = lead.step().await;
    assert!(step.allow.is_some(), "the next leg's step is as untrusted as the run: {step:?}");
    let child = step.trail_step.as_ref().unwrap().run.clone();
    assert_ne!(child, run, "the goto moved the child run");
    let header = lead.header(&child).await;
    assert_eq!((route(&header), &header.origin), (vec![entered("hop", None), entered("echo", None)], &origin));
    assert!(lead.reply(&step, "A").await);
    assert_eq!(lead.result(&run).await, json!("A"), "the parent takes the result of the child's last leg");
    assert_eq!(route(&lead.header(&run).await), [entered("relay", None)]);
    std::fs::remove_dir_all(&dir).unwrap();
}

const INTAKE: &str = r#"
async def intake(args):
    review = await trail("review")
    await goto("fix", state=review)

async def report(state):
    return state
"#;

/// The leg and step events of the run on `thread`, in short, until `count` are in.
async fn legs_and_steps(status: &mut async_nats::Subscriber, run: &RunId, count: usize) -> Vec<String> {
    let mut seen = Vec::new();
    while seen.len() < count {
        let event = tokio::time::timeout(Duration::from_secs(10), status.next()).await.expect("a status").unwrap();
        let event: StatusEvent = serde_json::from_slice(&event.payload).unwrap();
        match event.update {
            StatusUpdate::Leg { run: of, leg, trail, waypoint } => {
                assert_eq!(&of, run);
                seen.push(format!("leg {leg}: {}", ants_proto::leg_entry(trail.as_ref(), waypoint.as_deref())));
            }
            StatusUpdate::Step { label, to, stage, .. } => {
                seen.push(format!("step {} to {to}: {}", label.unwrap_or_default(), stage.as_str()));
            }
            _ => {}
        }
    }
    seen
}

#[tokio::test]
async fn a_run_tells_its_route_on_its_thread_in_its_end_and_in_ants_trail_show() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-goto-told");
    save(&dir, "triage", &["intake", "report"], INTAKE);
    save(&dir, "review", &[], "await agent(\"review it\", ant=\"dev\")\n");
    save(
        &dir,
        "fix",
        &[],
        "patch = await agent(f\"fix {args}\", ant=\"dev\")\nawait goto(\"triage\", at=\"report\", state=patch)\n",
    );
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.clone())).await;
    let thread = ThreadId::generate();
    let mut status = lead.client.subscribe(subject::status(&ant("lead"), &thread)).await.unwrap();
    let run = lead.start_saved("triage", human_origin(), TrailLimits::default(), thread.clone()).await;
    let reviewed = lead.step().await;
    let child = reviewed.trail_step.as_ref().unwrap().run.clone();
    assert!(lead.reply(&reviewed, "fine").await);
    let fixed = lead.step().await;
    assert_eq!(fixed.text, "fix fine");
    assert!(lead.reply(&fixed, "patched").await);
    assert_eq!(lead.result(&run).await, json!("patched"));

    let route = "triage@intake -> fix -> triage@report";
    let events = [
        "leg 0: triage@intake",
        "step trail review to lead: started",
        "step trail review to lead: answered",
        "leg 1: fix",
        "step  to dev: started",
        "step  to dev: answered",
        "leg 2: triage@report",
    ];
    assert_eq!(legs_and_steps(&mut status, &run, events.len()).await, events);
    let notes = lead.notes(&thread, 3).await;
    let started = format!("Run {run} of trail triage started: a test");
    assert_eq!(notes, [started, "route: triage@intake -> fix".to_owned(), format!("route: {route}")]);
    let told = lead.told().await;
    assert!(told.text.starts_with(&format!("Run {run} of trail triage is done (3 steps, ")), "{}", told.text);
    assert!(told.text.ends_with(&format!("patched\nroute: {route}")), "{}", told.text);
    let header = lead.header(&run).await;
    let steps: Vec<u32> = header.route.iter().map(|leg| leg.steps).collect();
    assert_eq!((header.route_text(), steps), (route.to_owned(), vec![2, 1, 0]), "the child's step is its leg's");

    let show = |json: bool| {
        let mut command = tokio::process::Command::new(ants_binary());
        command.args(["--nats", &server.url, "trail", "show", run.as_str()]).env("HOME", &dir).env_remove("ANTS_CREDS");
        command.env_remove("ANTS_CONFIG").args(json.then_some("--json"));
        command.output()
    };
    let shown = show(false).await.unwrap();
    let text = String::from_utf8_lossy(&shown.stdout);
    assert!(shown.status.success(), "{}", String::from_utf8_lossy(&shown.stderr));
    let (_, legs) = text.split_once(&format!("\nroute: {route}\n")).unwrap_or_else(|| panic!("{text}"));
    let legs: Vec<&str> = legs.lines().take(3).collect();
    assert!(legs[0].starts_with("  0  triage  intake  started ") && legs[0].ends_with("  steps 2"), "{text}");
    assert!(legs[1].starts_with("  1  fix     start   started ") && legs[1].contains("  steps 1  state \"fine\""));
    assert!(legs[2].starts_with("  2  triage  report  started ") && legs[2].contains("  steps 0  state "), "{text}");
    assert!(text.contains(&format!("\nchild runs:\n  {child}  review  done  call 0\n")), "{text}");
    let shown = show(true).await.unwrap();
    let json: Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(json["children"][0]["run"], child.as_str(), "{json}");
    std::fs::remove_dir_all(&dir).unwrap();
}
