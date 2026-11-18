mod run;

use std::path::Path;

use ants_proto::provider::{Origin, Starter};
use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, QueuedCall, Role, RunId, RunState, TRAIL_RUNS_BUCKET, ThreadId,
    TrailFunction, TrailLimits, TrailRun, TrailRunIndex, TrailSession, UserId, Uuid, trail_call_key,
    trail_runs_index_key, trail_session_key,
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
    assert_eq!(shown(run.as_str()), Shown::Run(run));
    assert_eq!(shown("deploy"), Shown::Trail("deploy".parse().unwrap()));
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
    assert_eq!((trail.saved_by.as_str(), trail.saved_at), ("alice", now));
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
    let found = saved::Found { trail: listed[0].trail.clone(), shadows: None };
    let text = saved::render(&found, now);
    assert!(text.contains("description: Fix it\nwaypoints: triage, patch\n"), "{text}");

    let stepping = format!("{}await triage(args)\n", found.trail.script);
    let error = saved::read(&write(&dir, "stepping.py", &stepping), None, &p("alice"), now).unwrap_err();
    let error = format!("{error:#}");
    assert!(error.contains("stepping.py: line 6: a trail with waypoints holds only"), "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

/// The JetStream context of a human's connection, which creates the colony's missing buckets.
async fn human(server: &Server, user: &UserId) -> jetstream::Context {
    let creds = server.creds_as(user, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    nats.connect("test").await.unwrap().1
}

#[tokio::test]
async fn humans_save_list_show_replace_and_remove_trails_and_ants_only_read_them() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("trail-saved");
    let local = dir.join("trails");
    std::fs::create_dir(&local).unwrap();
    write(&local, "deploy.py", &script("deploy"));
    let file = write(&dir, "deploy.py", &script("deploy"));
    let (alice, bob) = (p("alice"), p("bob"));
    let js = human(&server, &alice).await;

    let error = saved::find(&js, &local, &"absent".parse().unwrap()).await.unwrap_err().to_string();
    assert!(error.starts_with("no trail absent, neither in the colony nor in"), "{error}");
    let found = saved::find(&js, &local, &"deploy".parse().unwrap()).await.unwrap();
    assert!(matches!(found.trail.source, Source::Local { .. }) && found.shadows.is_none());

    let first = saved::read(&file, None, &alice, Utc::now()).unwrap();
    assert_eq!(saved::put(&js, &first).await.unwrap(), None);
    let colony = saved::colony(&js).await.unwrap();
    assert_eq!(colony, std::slice::from_ref(&first));
    let found = saved::find(&js, &local, &first.name).await.unwrap();
    assert_eq!(found.trail.source, Source::Colony { saved_by: alice.clone(), saved_at: first.saved_at });
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
        trail: trail.map(|t| t.parse().unwrap()),
        script: script(trail.unwrap_or("inline")),
        args: json!({"branch": "main"}),
        owner: Some(p("alice")),
        thread: ThreadId::generate(),
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
    let header = serde_json::from_str::<serde_json::Value>(&runs::header_json(&waiting).unwrap()).unwrap();
    assert!(header.get("script").is_none() && header["run"] == waiting.run.as_str(), "{header}");
    let table = runs::table(&[done.clone(), waiting.clone()], now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].starts_with("RUN") && lines[0].ends_with("ENDED"), "{table}");
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
