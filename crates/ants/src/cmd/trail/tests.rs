mod ratings;
mod run;
mod tips;

use std::path::Path;

use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, Role, RunId, RunState, SavedTrail, Saver, StepStage,
    TRAIL_RUNS_BUCKET, ThreadId, TrailFunction, TrailLeg, TrailLimits, TrailParent, TrailRun, TrailRunIndex,
    TrailSession, TrailStepThread, UserId, Uuid, trail_call_key, trail_runs_index_key, trail_session_key,
    trail_thread_step_key,
};
use async_nats::jetstream;
use chrono::{TimeDelta, Utc};
use clap::Parser;
use serde_json::json;

use super::saved::{self, Source};
use super::{Shown, TrailCommand, runs};
use crate::cli::{Cli, Command};
use crate::connection::Nats;
use crate::test_support::Server;

fn p(id: &str) -> UserId {
    id.parse().unwrap()
}

fn script(name: &str) -> String {
    format!(
        "meta = {{\"name\": \"{name}\", \"description\": \"Ship {name}\", \"phases\": [\"build\", \"test\"]}}\n\
         agent(\"build {name}\")\n"
    )
}

fn write(dir: &Path, file: &str, text: &str) -> std::path::PathBuf {
    let path = dir.join(file);
    std::fs::write(&path, text).unwrap();
    path
}

fn shown(arg: &str) -> Shown {
    let cli = Cli::try_parse_from(["ants", "trail", "show", arg]).unwrap();
    let Some(Command::Trail(TrailCommand::Show { what, .. })) = cli.command else { panic!("not a show") };
    what
}

#[test]
fn show_takes_a_uuid_for_a_run_and_anything_else_for_a_trail() {
    let run = RunId::generate();
    assert_eq!(shown(run.as_str()), Shown::Run(run.clone()));
    assert_eq!(shown(&run.as_str().to_ascii_uppercase()), Shown::Run(run));
    assert_eq!(shown("deploy"), Shown::Trail("deploy".parse().unwrap()));
    assert_eq!(shown("0190AB"), Shown::Pick("0190ab".parse().unwrap()));
    assert!(Cli::try_parse_from(["ants", "trail", "show", "Not_A_Name"]).is_err());
}

#[test]
fn a_file_saves_under_its_meta_name_or_the_one_given() {
    let dir = ants_test_support::scratch_dir("trail-read");
    let path = write(&dir, "deploy.py", &script("deploy"));
    let now = Utc::now();
    let trail = saved::read(&path, None, &p("alice"), now).unwrap();
    assert_eq!(trail.name.as_str(), "deploy");
    assert_eq!(
        (trail.description.as_str(), trail.phases.as_slice()),
        ("Ship deploy", &["build".into(), "test".into()][..])
    );
    assert_eq!((&trail.saved_by, trail.saved_at), (&Saver::Human { user: p("alice") }, now));
    assert_eq!(trail.script, script("deploy"));
    let renamed = saved::read(&path, Some("ship".parse().unwrap()), &p("alice"), now).unwrap();
    assert_eq!(renamed.name.as_str(), "ship");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_file_without_meta_or_compiling_or_a_usable_name_saves_nothing() {
    let dir = ants_test_support::scratch_dir("trail-refused");
    let refused = |file: &str, text: &str, name: Option<&str>| {
        let path = write(&dir, file, text);
        let error = saved::read(&path, name.map(|n| n.parse().unwrap()), &p("alice"), Utc::now()).unwrap_err();
        format!("{error:#}")
    };
    let missing = format!("{:#}", saved::read(&dir.join("absent.py"), None, &p("alice"), Utc::now()).unwrap_err());
    assert!(missing.contains("absent.py"), "{missing}");
    let error = refused("bare.py", "agent('build')\n", None);
    assert!(error.contains("bare.py: ") && error.contains("meta"), "{error}");
    let error = refused("broken.py", &format!("{}x = (\n", script("broken")), None);
    assert!(error.contains("broken.py does not compile") && error.contains("line 4"), "{error}");
    let error = refused("upper.py", &script("Deploy_It"), None);
    assert!(error.contains("\"Deploy_It\" is no trail name") && error.contains("--name"), "{error}");
    let uuid = Uuid::now_v7().to_string();
    let error = refused("uuid.py", &script(&uuid), None);
    assert!(error.contains("reads as a run's id"), "{error}");
    let error = refused("upper.py", &script("Deploy_It"), Some(&uuid));
    assert!(error.contains("reads as a run's id"), "{error}");
    assert!(saved::read(&dir.join("upper.py"), Some("deploy-it".parse().unwrap()), &p("alice"), Utc::now()).is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn local_trails_are_named_by_their_files_skipping_bad_ones_and_the_colonys_shadow_them() {
    let dir = ants_test_support::scratch_dir("trail-local");
    write(&dir, "deploy.py", &script("deploy-v2"));
    write(&dir, "Deploy_It.py", &script("deploy"));
    write(&dir, "bare.py", "agent('x')\n");
    write(&dir, "notes.txt", &script("notes"));
    write(&dir, "review.py", &script("review"));
    let local = saved::local(&dir);
    let names: Vec<(&str, &Source)> = local.iter().map(|t| (t.name.as_str(), &t.source)).collect();
    assert_eq!(
        names,
        [
            ("deploy", &Source::Local { path: dir.join("deploy.py") }),
            ("review", &Source::Local { path: dir.join("review.py") })
        ]
    );
    assert!(saved::local(&dir.join("absent")).is_empty());

    let now = Utc::now();
    let colony = saved::read(&dir.join("review.py"), None, &p("alice"), now - TimeDelta::hours(2)).unwrap();
    let listed = saved::listing(vec![colony], local);
    let flags: Vec<(&str, bool)> = listed.iter().map(|l| (l.trail.name.as_str(), l.shadowed)).collect();
    assert_eq!(flags, [("review", false), ("deploy", false), ("review", true)]);
    let table = saved::table(&listed, now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].starts_with("NAME") && lines[0].contains("SOURCE"), "{table}");
    assert!(!lines[0].contains("WAYPOINTS"), "{table}");
    assert!(lines[1].contains("colony") && lines[1].contains("alice") && lines[1].contains("2h ago"), "{table}");
    assert!(lines[3].contains("review.py (the colony's runs)"), "{table}");
    assert!(lines[2].contains("build, test") && lines[2].contains("Ship deploy"), "{table}");
    let json = serde_json::to_value(&listed[2]).unwrap();
    assert_eq!(json["source"], "local");
    assert_eq!(json["shadowed"], true);
    assert!(json.get("script").is_none(), "{json}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_trails_waypoints_are_saved_listed_and_shown() {
    let dir = ants_test_support::scratch_dir("trail-waypoints");
    let text = r#"meta = {"name": "fix", "description": "Fix it", "phases": [], "waypoints": ["triage", "patch"]}
async def triage(state):
    return await agent("triage")
async def patch(state):
    return await agent("patch")
"#;
    let now = Utc::now();
    let trail = saved::read(&write(&dir, "fix.py", text), None, &p("alice"), now).unwrap();
    assert_eq!(trail.waypoints, ["triage", "patch"]);
    let listed = saved::listing(vec![trail], Vec::new());
    let table = saved::table(&listed, now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].contains("PHASES") && lines[0].contains("WAYPOINTS"), "{table}");
    assert!(lines[1].contains("triage, patch"), "{table}");
    let found = saved::Found { trail: listed[0].trail.clone(), shadows: None, ratings: None };
    let text = saved::render(&found, now);
    assert!(text.contains("description: Fix it\nwaypoints: triage, patch\n"), "{text}");

    let stepping = format!("{}await triage(args)\n", found.trail.script);
    let error = saved::read(&write(&dir, "stepping.py", &stepping), None, &p("alice"), now).unwrap_err();
    let error = format!("{error:#}");
    assert!(error.contains("stepping.py: line 6: a trail with waypoints holds only"), "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_trail_an_ant_saved_shows_the_ant_and_its_run() {
    let now = Utc::now();
    let run = RunId::generate();
    let trail = |name: &str, saved_by, saved_from| SavedTrail {
        name: name.parse().unwrap(),
        description: format!("Ship {name}"),
        phases: Vec::new(),
        waypoints: Vec::new(),
        script: script(name),
        saved_by,
        saved_from,
        saved_at: now - TimeDelta::days(3),
    };
    let by_ant = trail("deploy", Saver::Ant { user: p("dev-a") }, Some(run.clone()));
    let by_human = trail("review", Saver::Human { user: p("alice") }, None);
    let listed = saved::listing(vec![by_ant.clone(), by_human], Vec::new());
    let table = saved::table(&listed, now);
    let lines: Vec<&str> = table.lines().collect();
    let words = |line: &str| line.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(words(lines[0]).starts_with("NAME SOURCE SAVED BY SAVED FROM RUN PHASES"), "{table}");
    assert!(words(lines[1]).starts_with(&format!("deploy colony ant dev-a 3d ago {run} -")), "{table}");
    assert!(lines[2].starts_with("review") && lines[2].contains("alice"), "{table}");
    let json = serde_json::to_value(&listed[0]).unwrap();
    assert_eq!(json["saved_by"], json!({"kind": "ant", "user": "dev-a"}));
    assert_eq!(json["saved_from"], run.as_str());
    assert!(serde_json::to_value(&listed[1]).unwrap().get("saved_from").is_none());

    let found = saved::Found { trail: by_ant.into(), shadows: None, ratings: None };
    let text = saved::render(&found, now);
    let first = text.lines().nth(1).unwrap();
    assert!(
        first.starts_with("saved for the colony by ant dev-a 3d ago (")
            && first.ends_with(&format!(", from run {run}")),
        "{text}"
    );
}

/// The JetStream context of a human's connection, which creates the colony's missing buckets.
async fn human(server: &Server, user: &UserId) -> jetstream::Context {
    let creds = server.creds_as(user, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    nats.connect("test").await.unwrap().1
}

#[tokio::test]
async fn humans_save_list_show_replace_and_remove_trails_and_ants_save_none_from_the_command_line() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("trail-saved");
    let local = dir.join("trails");
    std::fs::create_dir(&local).unwrap();
    write(&local, "deploy.py", &script("deploy"));
    let file = write(&dir, "deploy.py", &script("deploy"));
    let (alice, bob) = (p("alice"), p("bob"));
    let js = human(&server, &alice).await;

    let error = saved::find(&js, &local, &"absent".parse().unwrap()).await.unwrap_err().to_string();
    assert!(error.starts_with("no trail absent, either in the colony or in"), "{error}");
    let found = saved::find(&js, &local, &"deploy".parse().unwrap()).await.unwrap();
    assert!(matches!(found.trail.source, Source::Local { .. }) && found.shadows.is_none());

    let first = saved::read(&file, None, &alice, Utc::now()).unwrap();
    assert_eq!(saved::put(&js, &first).await.unwrap(), None);
    let colony = saved::colony(&js).await.unwrap();
    assert_eq!(colony, std::slice::from_ref(&first));
    let found = saved::find(&js, &local, &first.name).await.unwrap();
    let saved_by = Saver::Human { user: alice.clone() };
    assert_eq!(found.trail.source, Source::Colony { saved_by, saved_from: None, saved_at: first.saved_at });
    assert_eq!(found.shadows, Some(local.join("deploy.py")));
    let shown = serde_json::to_value(saved::Shown::from(&found)).unwrap();
    assert_eq!((shown["name"].as_str(), shown["script"].as_str()), (Some("deploy"), Some(script("deploy").as_str())));
    let text = saved::render(&found, Utc::now());
    assert!(text.starts_with("trail deploy\nsaved for the colony by alice"), "{text}");
    assert!(text.contains("phases: build, test\n") && text.ends_with(&script("deploy")), "{text}");

    let bob_js = human(&server, &bob).await;
    let second = saved::read(&file, None, &bob, Utc::now()).unwrap();
    assert_eq!(saved::put(&bob_js, &second).await.unwrap(), Some(first));
    assert_eq!(saved::colony(&js).await.unwrap(), [second]);

    let dev_a = p("dev-a");
    let creds = server.creds_as(&dev_a, Role::Ant);
    let ant = Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, ant_js) = ant.connect("test").await.unwrap();
    assert_eq!(saved::colony(&ant_js).await.unwrap().len(), 1, "an ant reads the colony's trails");
    let save = TrailCommand::Save { file: file.clone(), name: None };
    let error = super::run(&ant, save).await.unwrap_err().to_string();
    assert_eq!(error, "saving a trail needs a human's credentials; those of dev-a are an ant's");
    let error = super::run(&ant, TrailCommand::Rm { name: "deploy".parse().unwrap() }).await.unwrap_err();
    assert!(error.to_string().starts_with("removing a trail needs a human's"), "{error}");

    saved::remove(&js, &local, &"deploy".parse().unwrap()).await.unwrap();
    assert!(saved::colony(&js).await.unwrap().is_empty());
    let error = saved::remove(&js, &local, &"deploy".parse().unwrap()).await.unwrap_err().to_string();
    let file = local.join("deploy.py");
    assert_eq!(
        error,
        format!("the colony saved no trail deploy; the local one goes with its file, {}", file.display())
    );
    let error = saved::remove(&js, &local, &"absent".parse().unwrap()).await.unwrap_err().to_string();
    assert_eq!(error, "the colony saved no trail absent");
    std::fs::remove_dir_all(dir).unwrap();
}

fn record(conductor: &UserId, trail: Option<&str>, state: RunState, started: chrono::DateTime<Utc>) -> TrailRun {
    TrailRun {
        run: RunId::generate(),
        conductor: conductor.clone(),
        route: vec![TrailLeg {
            trail: trail.map(|t| t.parse().unwrap()),
            local: false,
            at: None,
            script: script(trail.unwrap_or("inline")),
            args: json!({"branch": "main"}),
            first_call: 0,
            started_at: started,
            steps: 2,
        }],
        owner: Some(p("alice")),
        thread: ThreadId::generate(),
        parent: None,
        spends_from: None,
        origin: Origin { by: Starter::Human { user: p("alice") }, channel: None },
        limits: TrailLimits::default(),
        state,
        phase: Some("build".to_owned()),
        steps: 2,
        turns: 7,
        started_at: started,
        updated_at: started,
        ended_at: None,
    }
}

#[test]
fn a_child_run_shows_the_call_it_answers_and_a_trail_call_its_child() {
    let now = Utc::now();
    let parent = record(&p("dev-a"), Some("release"), RunState::Waiting, now);
    let mut child = record(&p("dev-a"), Some("review"), RunState::Running, now);
    child.parent = Some(TrailParent { run: parent.run.clone(), call: 1, origin_thread: parent.thread.clone() });
    let call = PendingCall {
        call: 1,
        function: TrailFunction::Trail,
        args: json!({"name": "review", "args": {"pr": 7}}),
        to: p("dev-a"),
        thread: child.thread.clone(),
        message: child.run.as_str().parse().unwrap(),
        sent_at: now,
        sent: true,
        turn_budget: 0,
        retries: 0,
    };
    let shown = |run: &TrailRun, pending: Vec<PendingCall>, children: Vec<TrailRun>| {
        let detail = runs::Detail {
            run: run.clone(),
            pending,
            queued: Vec::new(),
            journal: Vec::new(),
            children,
            ratings: Vec::new(),
        };
        runs::render(&detail, now)
    };
    let text = shown(&child, Vec::new(), Vec::new());
    assert!(text.contains(&format!("\nchild run answering call 1 of run {}\n", parent.run)), "{text}");
    assert!(!text.contains("child runs:"), "{text}");
    let text = shown(&parent, vec![call], vec![child.clone()]);
    let row = format!("  1  trail  run {}  sent 0s ago  thread {}  trail review\n", child.run, child.thread);
    assert!(text.contains(&row) && !text.contains("child run answering"), "{text}");
    assert!(text.contains(&format!("\nchild runs:\n  {}  review  running  call 1\n", child.run)), "{text}");
}

#[test]
fn a_run_started_answering_a_step_shows_the_run_it_spends_from() {
    let now = Utc::now();
    let mut run = record(&p("dev-a"), None, RunState::Running, now);
    let from = RunId::generate();
    let route = vec!["deploy".parse().unwrap()];
    run.spends_from = Some(ants_proto::SpendsFrom { conductor: p("lead"), run: from.clone(), call: 2, route });
    let detail = runs::Detail {
        run,
        pending: Vec::new(),
        queued: Vec::new(),
        journal: Vec::new(),
        children: Vec::new(),
        ratings: Vec::new(),
    };
    let text = runs::render(&detail, now);
    let line = format!("\nstarted answering call 2 of run {from} of lead, spending from that run's limits\n");
    assert!(text.contains(&line), "{text}");
}

#[test]
fn a_run_shows_the_legs_of_its_route_and_the_gotos_it_made() {
    let now = Utc::now();
    let mut run = record(&p("dev-a"), Some("triage"), RunState::Running, now);
    let next = TrailLeg {
        trail: Some("release".parse().unwrap()),
        local: false,
        at: Some("review".to_owned()),
        script: script("release"),
        args: json!({"version": "1.2"}),
        first_call: 2,
        started_at: now,
        steps: 1,
    };
    run.route.push(next);
    let goto = |args, answer| JournalEntry { call: 1, function: TrailFunction::Goto, args, answer };
    let refused = goto(json!({"trail": "triage"}), CallAnswer::Error("the run entered trail triage before".into()));
    let detail = runs::Detail {
        run: run.clone(),
        pending: Vec::new(),
        queued: Vec::new(),
        journal: vec![refused],
        children: Vec::new(),
        ratings: Vec::new(),
    };
    let text = runs::render(&detail, now);
    assert!(text.starts_with(&format!("run {} of trail triage\n", run.run)), "the trail it started: {text}");
    let (_, route) = text.split_once("\nroute: triage -> release@review\n").unwrap_or_else(|| panic!("{text}"));
    let legs: Vec<&str> = route.lines().take(2).collect();
    assert!(legs[0].starts_with("  0  triage   start   started ") && legs[0].ends_with("  steps 2"), "{text}");
    assert!(legs[1].starts_with("  1  release  review  started ") && legs[1].contains("  steps 1  "), "{text}");
    assert!(legs[1].ends_with("  state {\"version\":\"1.2\"}"), "{text}");
    assert!(text.contains("args: {\"branch\":\"main\"}\n"), "the run's input is its first leg's: {text}");
    let row = "  1  goto  dev-a  trail triage at its start  error: the run entered trail triage before\n";
    assert!(text.contains(row), "{text}");
    let table = runs::table(std::slice::from_ref(&run), &runs::Rated::default(), now);
    let row = table.lines().nth(1).unwrap_or_else(|| panic!("{table}"));
    assert!(
        row.starts_with(&format!("{}  triage -> release@review  dev-a  ", run.run)),
        "the trail it started: {table}"
    );
    let header =
        serde_json::from_str::<serde_json::Value>(&runs::header_json(&run, &runs::Rated::default()).unwrap()).unwrap();
    let route = header["route"].as_array().unwrap();
    assert!(route.len() == 2 && route.iter().all(|leg| leg.get("script").is_none()), "{header}");
    let inline = runs::render(&runs::Detail { run: record(&p("dev-a"), None, RunState::Running, now), ..detail }, now);
    assert!(inline.contains("\nroute: inline\n  0  inline  start  started "), "{inline}");
}

#[test]
fn a_run_failed_on_a_limit_shows_which() {
    let now = Utc::now();
    let shown = |state| {
        let run = record(&p("dev-a"), Some("release"), state, now);
        runs::render(
            &runs::Detail {
                run,
                pending: Vec::new(),
                queued: Vec::new(),
                journal: Vec::new(),
                children: Vec::new(),
                ratings: Vec::new(),
            },
            now,
        )
    };
    let error = "the run reached its limit of 200 turns".to_owned();
    let text = shown(RunState::Failed { error: error.clone(), limit: Some(ants_proto::RunLimit::Turns) });
    assert!(text.contains(&format!("\nstate: failed on its turns limit: {error}\n")), "{text}");
    let text = shown(RunState::Failed { error: error.clone(), limit: None });
    assert!(text.contains(&format!("\nstate: failed: {error}\n")), "{text}");
}

/// Put `value` under `key` of [`TRAIL_RUNS_BUCKET`], as the conductor owning the key does.
async fn put(js: &jetstream::Context, key: &str, value: &impl serde::Serialize) {
    let payload = serde_json::to_vec(value).unwrap();
    js.publish(format!("$KV.{TRAIL_RUNS_BUCKET}.{key}"), payload.into()).await.unwrap().await.unwrap();
}

#[tokio::test]
async fn humans_list_and_show_the_runs_conductors_keep() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let js = human(&server, &alice).await;
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let ((_a, a_js), (_b, b_js)) = (server.connect(&dev_a).await, server.connect(&dev_b).await);
    let now = Utc::now();
    let waiting = record(&dev_a, Some("deploy"), RunState::Waiting, now - TimeDelta::minutes(5));
    let mut done = record(&dev_b, None, RunState::Done { result: json!("shipped") }, now - TimeDelta::hours(1));
    done.ended_at = Some(now - TimeDelta::minutes(30));
    put(&a_js, &waiting.key(), &waiting).await;
    put(&a_js, &trail_runs_index_key(&dev_a), &TrailRunIndex { runs: vec![waiting.run.clone()] }).await;
    let step = ThreadId::generate();
    let session = TrailSession {
        session: "opaque".to_owned(),
        pending: vec![PendingCall {
            call: 2,
            function: TrailFunction::Human,
            args: json!({"questions": "Ship it?"}),
            to: alice.clone(),
            thread: step.clone(),
            message: Uuid::now_v7(),
            sent_at: now - TimeDelta::minutes(1),
            sent: true,
            turn_budget: 8,
            retries: 0,
        }],
        queued: vec![QueuedCall { call: 3, function: TrailFunction::Agent, args: json!({"prompt": "announce"}) }],
    };
    put(&a_js, &trail_session_key(&dev_a, &waiting.run), &session).await;
    let built = JournalEntry {
        call: 0,
        function: TrailFunction::Agent,
        args: json!({"prompt": "build the release", "ant": "dev-c", "label": "build"}),
        answer: CallAnswer::Ok(json!("built")),
    };
    let tested = JournalEntry {
        call: 1,
        function: TrailFunction::Agent,
        args: json!({"prompt": "run the tests", "capability": "test"}),
        answer: CallAnswer::Error("tests failed".to_owned()),
    };
    put(&a_js, &trail_call_key(&dev_a, &waiting.run, 1), &tested).await;
    put(&a_js, &trail_call_key(&dev_a, &waiting.run, 0), &built).await;
    put(&b_js, &done.key(), &done).await;

    assert_eq!(runs::list(&js, None).await.unwrap(), [done.clone(), waiting.clone()]);
    assert_eq!(runs::list(&js, Some(&dev_a)).await.unwrap(), std::slice::from_ref(&waiting));
    assert!(runs::list(&js, Some(&p("dev-c"))).await.unwrap().is_empty());
    let header =
        serde_json::from_str::<serde_json::Value>(&runs::header_json(&waiting, &runs::Rated::default()).unwrap())
            .unwrap();
    assert!(header["route"][0].get("script").is_none() && header["run"] == waiting.run.as_str(), "{header}");
    let table = runs::table(&[done.clone(), waiting.clone()], &runs::Rated::default(), now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].starts_with("RUN") && lines[0].contains("ENDED") && lines[0].ends_with("RATED"), "{table}");
    for cell in [done.run.as_str(), "inline", "dev-b", "alice", "done", "2/", "1h ago", "30m ago"] {
        assert!(lines[1].contains(cell), "{cell}: {table}");
    }
    assert!(lines[2].contains("deploy") && lines[2].contains("waiting") && lines[2].contains("5m ago"), "{table}");

    let detail = runs::detail(&js, &waiting.run).await.unwrap();
    assert_eq!(detail.run, waiting);
    assert_eq!((detail.pending, detail.queued.clone()), (session.pending.clone(), session.queued.clone()));
    assert_eq!(detail.journal, [built, tested]);
    let detail = runs::detail(&js, &waiting.run).await.unwrap();
    let json = serde_json::to_value(&detail).unwrap();
    assert!(json.get("session").is_none() && json["journal"].as_array().unwrap().len() == 2, "{json}");
    assert!(detail.children.is_empty() && json.get("children").is_none(), "{json}");
    let text = runs::render(&detail, now);
    for line in [
        format!("run {} of trail deploy", waiting.run),
        format!("conductor dev-a, owner alice, on thread {} started by human alice", waiting.thread),
        "state: waiting".to_owned(),
        "phase: build".to_owned(),
        "args: {\"branch\":\"main\"}".to_owned(),
    ] {
        assert!(text.contains(&format!("{line}\n")), "{line}: {text}");
    }
    assert!(text.contains("limits: steps 2 of "), "{text}");
    assert!(text.contains(&format!("  2  human  to alice  sent 1m ago  thread {step}  Ship it?\n")), "{text}");
    assert!(text.contains("queued calls:\n  3  agent  announce\n"), "{text}");
    assert!(text.contains("  0  agent  dev-c            build          ok: built\n"), "{text}");
    assert!(text.contains("  1  agent  capability test  run the tests  error: tests failed\n"), "{text}");
    let text = runs::render(&runs::detail(&js, &done.run).await.unwrap(), now);
    assert!(text.starts_with(&format!("run {} of an inline script\nconductor dev-b", done.run)), "{text}");
    assert!(text.contains("state: done, result: \"shipped\"\n") && !text.contains("journal"), "{text}");

    let absent = RunId::generate();
    let error = runs::detail(&js, &absent).await.unwrap_err().to_string();
    assert_eq!(error, format!("no run {absent}: `ants trail runs` lists the runs kept"));
    let creds = server.creds_as(&dev_a, Role::Ant);
    let ant = Nats::new(&server.url, Some(&creds), None).unwrap();
    let error = super::run(&ant, TrailCommand::Runs { ant: None, json: false }).await.unwrap_err();
    assert!(error.to_string().starts_with("reading the runs of trails needs a human's"), "{error}");
    let show = TrailCommand::Show { what: Shown::Run(waiting.run), json: false, follow: false };
    assert!(super::run(&ant, show).await.is_err());
}

fn journal(call: u32, label: &str, answer: CallAnswer) -> JournalEntry {
    JournalEntry { call, function: TrailFunction::Agent, args: json!({"prompt": "do it", "label": label}), answer }
}

fn waited(call: u32, thread: &ThreadId) -> PendingCall {
    PendingCall {
        call,
        function: TrailFunction::Agent,
        args: json!({"prompt": "review", "label": "review"}),
        to: p("dev-b"),
        thread: thread.clone(),
        message: Uuid::now_v7(),
        sent_at: Utc::now(),
        sent: true,
        turn_budget: 8,
        retries: 0,
    }
}

#[test]
fn a_step_is_started_until_answered_failed_or_dropped_as_the_records_tell() {
    let (run, run_thread) = (RunId::generate(), ThreadId::generate());
    let threads: Vec<ThreadId> = (0..5).map(|_| ThreadId::generate()).collect();
    let now = Utc::now();
    let marked: Vec<runs::Marked> = [(0, 0), (1, 1), (2, 2), (2, 3), (3, 4)]
        .into_iter()
        .map(|(call, at)| {
            let marker =
                TrailStepThread { run: run.clone(), call, to: p("dev-b"), run_thread: run_thread.clone(), label: None };
            (threads[at].clone(), marker, now + TimeDelta::seconds(i64::try_from(at).unwrap()))
        })
        .collect();
    let answers =
        [journal(0, "build", CallAnswer::Ok(json!("built"))), journal(1, "test", CallAnswer::Error("no".into()))];
    let stages = |pending: Option<&[PendingCall]>| {
        let steps = runs::staged(&p("lead"), marked.clone(), &answers, pending);
        steps.into_iter().map(|s| (s.call, s.label, s.stage)).collect::<Vec<_>>()
    };
    let label = |l: &str| Some(l.to_owned());
    let going = [waited(2, &threads[3])];
    assert_eq!(
        stages(Some(&going)),
        [
            (0, label("build"), StepStage::Answered),
            (1, label("test"), StepStage::Failed),
            (2, label("review"), StepStage::Dropped),
            (2, label("review"), StepStage::Started),
            (3, None, StepStage::Dropped),
        ],
        "a call sent anew drops its older thread, and one the run no longer waits on is dropped"
    );
    let ended = stages(Some(&[]));
    assert_eq!(ended[3], (2, None, StepStage::Dropped), "an ended or stopped run waits on none");
    let unknown = stages(None);
    assert_eq!((unknown[3].2, unknown[4].2), (StepStage::Started, StepStage::Started), "a session not read");
}

#[test]
fn a_step_keeps_the_label_its_marker_carries_once_its_run_ended() {
    let (run, run_thread) = (RunId::generate(), ThreadId::generate());
    let (child, asked, older) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let now = Utc::now();
    let mark = |call: u32, to: &str, label: Option<&str>| TrailStepThread {
        run: run.clone(),
        call,
        to: p(to),
        run_thread: run_thread.clone(),
        label: label.map(str::to_owned),
    };
    let marked = vec![
        (child, mark(0, "lead", Some("trail echo")), now),
        (asked, mark(1, "dev-b", Some("review")), now + TimeDelta::seconds(1)),
        (older, mark(2, "dev-b", None), now + TimeDelta::seconds(2)),
    ];
    let echoed = JournalEntry {
        call: 0,
        function: TrailFunction::Trail,
        args: json!({"name": "echo", "args": {}}),
        answer: CallAnswer::Ok(json!("A")),
    };
    let child_failed = JournalEntry {
        call: 2,
        function: TrailFunction::Trail,
        args: json!({"name": "broken"}),
        answer: CallAnswer::Error("bad input".into()),
    };
    let steps = runs::staged(&p("lead"), marked, &[echoed, child_failed], Some(&[]));
    let shown: Vec<_> = steps.iter().map(|s| (s.call, s.to.as_str(), s.label.as_deref(), s.stage)).collect();
    assert_eq!(
        shown,
        [
            (0, "lead", Some("trail echo"), StepStage::Answered),
            (1, "dev-b", Some("review"), StepStage::Dropped),
            (2, "dev-b", Some("trail broken"), StepStage::Failed),
        ],
        "a trail() step reads back with its label and stage, an unanswered one of a stopped run keeps its label, and a \
         marker without one takes it from its call's arguments"
    );
}

#[tokio::test]
async fn humans_read_the_steps_a_conductor_recorded_on_the_thread_of_its_runs() {
    let Some(server) = Server::start() else { return };
    let js = human(&server, &p("alice")).await;
    let lead = p("lead");
    let (_lead, lead_js) = server.connect(&lead).await;
    let now = Utc::now();
    let running = record(&lead, Some("review"), RunState::Running, now);
    let mut done = record(&lead, Some("review"), RunState::Done { result: json!("ok") }, now);
    done.thread = running.thread.clone();
    let elsewhere = record(&lead, Some("review"), RunState::Running, now);
    for run in [&running, &done, &elsewhere] {
        put(&lead_js, &run.key(), run).await;
    }
    let (answered, waiting, left) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let other = ThreadId::generate();
    let mark = |run: &TrailRun, call: u32, to: &str| TrailStepThread {
        run: run.run.clone(),
        call,
        to: p(to),
        run_thread: run.thread.clone(),
        label: None,
    };
    put(&lead_js, &trail_thread_step_key(&lead, &running.thread, &answered), &mark(&running, 0, "dev-c")).await;
    put(&lead_js, &trail_thread_step_key(&lead, &running.thread, &waiting), &mark(&running, 1, "dev-b")).await;
    put(&lead_js, &trail_thread_step_key(&lead, &done.thread, &left), &mark(&done, 0, "dev-b")).await;
    put(&lead_js, &trail_thread_step_key(&lead, &elsewhere.thread, &other), &mark(&elsewhere, 0, "dev-b")).await;
    put(&lead_js, &trail_call_key(&lead, &running.run, 0), &journal(0, "build", CallAnswer::Ok(json!("built")))).await;
    let session = TrailSession { session: "opaque".to_owned(), pending: vec![waited(1, &waiting)], queued: Vec::new() };
    put(&lead_js, &trail_session_key(&lead, &running.run), &session).await;

    let steps = runs::steps_on(&js, &lead, &running.thread).await.unwrap();
    let shown: Vec<_> = steps.iter().map(|s| (&s.thread, s.call, s.to.as_str(), s.label.as_deref(), s.stage)).collect();
    assert_eq!(
        shown,
        [
            (&answered, 0, "dev-c", Some("build"), StepStage::Answered),
            (&waiting, 1, "dev-b", Some("review"), StepStage::Started),
            (&left, 0, "dev-b", None, StepStage::Dropped),
        ]
    );
    assert!(steps.iter().all(|s| s.conductor == lead) && steps[0].run == running.run && steps[2].run == done.run);
    assert!(steps.windows(2).all(|w| w[0].at <= w[1].at), "by when each started");
    assert!(runs::steps_on(&js, &p("dev-a"), &running.thread).await.unwrap().is_empty(), "another conductor's");
}
