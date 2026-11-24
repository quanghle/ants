//! Saved trails: `ants trail save`, `list`, `show` and `remove`, and this machine's local trails.

use ants_proto::{Role, RunId, SavedTrail, Saver, Uuid};
use chrono::{TimeDelta, Utc};
use clap::Parser;
use serde_json::json;

use super::super::Shown;
use super::super::saved::{self, Source};
use super::{human, p, script, write};
use crate::cli::{Cli, Command};
use crate::cmd::trail::TrailCommand;
use crate::connection::Nats;
use crate::test_support::Server;

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
    let ant = Nats::new(&server.url, Some(&creds), None, None).unwrap();
    let (_, ant_js) = ant.connect("test").await.unwrap();
    assert_eq!(saved::colony(&ant_js).await.unwrap().len(), 1, "an ant reads the colony's trails");
    let save = TrailCommand::Save { file: file.clone(), name: None };
    let error = crate::cmd::trail::run(&ant, save).await.unwrap_err().to_string();
    assert_eq!(error, "saving a trail needs a human's credentials; those of dev-a are an ant's");
    let error = crate::cmd::trail::run(&ant, TrailCommand::Rm { name: "deploy".parse().unwrap() }).await.unwrap_err();
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
