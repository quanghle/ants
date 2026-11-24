//! Tests of teams as humans run them: the forest as `ants team list` and `ants team show` print it, the team named
//! by `ants ant show`, the checks of a change, and `ants team` and a retirement changing the directory of a bus.

use std::collections::BTreeSet;

use ants_proto::{AntIdentity, IDENTITIES_BUCKET, Role, Team, TeamDirectory, UserId};
use async_nats::jetstream;
use chrono::Utc;

use super::*;
use crate::cmd::team::{TeamCommand, run};
use crate::connection::Nats;
use crate::test_support::Server;

fn p(id: &str) -> UserId {
    id.parse().unwrap()
}

/// eng (lead `boss`): boss, backend, web; backend (lead `dev-a`): dev-a, dev-b; web (lead `dev-c`): dev-c.
fn forest() -> TeamDirectory {
    let mut directory = TeamDirectory::default();
    directory.create(Team::new(p("eng"), p("boss"), p("alice")), None).unwrap();
    let mut backend = Team::new(p("backend"), p("dev-a"), p("alice"));
    backend.members.push(p("dev-b"));
    directory.create(backend, Some(&p("eng"))).unwrap();
    directory.create(Team::new(p("web"), p("dev-c"), p("alice")), Some(&p("eng"))).unwrap();
    directory
}

#[test]
fn the_forest_lists_each_team_under_the_team_holding_it() {
    let directory = forest();
    let order: Vec<(usize, &str)> = forest_of(&directory);
    assert_eq!(order, [(0, "eng"), (1, "backend"), (1, "web")]);
    let table = table(&directory);
    let lines: Vec<&str> = table.lines().map(str::trim_end).collect();
    assert!(lines[0].starts_with("TEAM") && !lines[0].contains("DESCRIPTION"), "{table}");
    assert!(lines[1].starts_with("eng ") && lines[1].contains("boss"), "{table}");
    assert!(lines[2].starts_with("  backend ") && lines[2].contains("dev-a"), "{table}");

    let mut described = directory.clone();
    described.teams[1].description = Some("the services".to_owned());
    assert!(super::table(&described).lines().next().unwrap().contains("DESCRIPTION"));
}

fn forest_of(directory: &TeamDirectory) -> Vec<(usize, &str)> {
    super::forest(directory).into_iter().map(|(depth, team)| (depth, team.team.as_str())).collect()
}

#[test]
fn a_team_is_described_and_an_ant_s_team_named() {
    let directory = forest();
    let eng = directory.team(&p("eng")).unwrap();
    assert_eq!(describe(&directory, eng), "team eng: lead boss; members boss, team backend, team web");
    let mut backend = directory.team(&p("backend")).unwrap().clone();
    backend.owner = Some(p("alice"));
    backend.description = Some("the services".to_owned());
    assert_eq!(
        describe(&directory, &backend),
        "team backend, within team eng: lead dev-a; members dev-a, dev-b; owner alice\ndescription: the services"
    );
    assert_eq!(team_line(&directory, &p("dev-a")), "backend (lead), within eng");
    assert_eq!(team_line(&directory, &p("dev-b")), "backend, within eng");
    assert_eq!(team_line(&directory, &p("boss")), "eng (lead)");
    assert_eq!(team_line(&directory, &p("dev-z")), "none");
}

#[test]
fn a_team_s_members_are_a_tree_with_its_lead_marked() {
    let directory = forest();
    let eng = directory.team(&p("eng")).unwrap();
    let tree = members(&directory, eng, &|ant| if *ant == p("dev-b") { "stopped".into() } else { "idle".into() });
    let lines = tree_lines(&tree, &|ant| ant.to_string());
    assert_eq!(
        lines,
        [
            "  boss ★ lead: idle",
            "  team backend (lead dev-a)",
            "    dev-a ★ lead: idle",
            "    dev-b: stopped",
            "  team web (lead dev-c)",
            "    dev-c ★ lead: idle",
        ]
    );
    let json = serde_json::to_value(&tree[0]).unwrap();
    assert_eq!(json, serde_json::json!({"kind": "ant", "name": "boss", "lead": true, "state": "idle"}));
    let shown = Shown::of(&directory, directory.team(&p("web")).unwrap());
    assert_eq!(serde_json::to_value(&shown).unwrap()["parent"], "eng");
}

#[test]
fn a_member_must_be_known() {
    let directory = forest();
    let known = BTreeSet::from([p("dev-x")]);
    check_known(&directory, &known, &[p("dev-x"), p("web")]).unwrap();
    assert_eq!(
        check_known(&directory, &known, &[p("dev-x"), p("dev-y")]),
        Err(TeamRefusal::UnknownAnt { name: p("dev-y") })
    );
    assert_eq!(parse_name("@backend").unwrap(), p("backend"));
}

/// A human's connection to `server` with `creds`, which keeps the buckets of the hub.
async fn human<'a>(server: &'a Server, creds: &'a std::path::Path) -> (Nats<'a>, jetstream::Context) {
    let nats = Nats::new(&server.url, Some(creds), None, None).unwrap();
    let js = nats.connect("test").await.unwrap().1;
    (nats, js)
}

/// Keep the hub's identities and team directory, with an identity record for each of `ants`.
async fn identities(js: &jetstream::Context, ants: &[&str]) {
    ants_daemon::hub::create_identities(js).await.unwrap();
    ants_daemon::hub::ensure_bucket(js, ants_daemon::hub::Object::Teams).await.unwrap();
    let kv = DirectKv::whole(js, None, IDENTITIES_BUCKET).await.unwrap();
    for ant in ants {
        let record = AntIdentity::new(p(ant), format!("{ant}-id"), Utc::now());
        kv.put(ant, serde_json::to_vec(&record).unwrap()).await.unwrap();
    }
}

async fn directory_of(js: &jetstream::Context) -> TeamDirectory {
    read_of(js, None).await.unwrap().unwrap()
}

#[tokio::test]
async fn a_human_creates_and_changes_a_team_of_known_ants_only() {
    let Some(server) = Server::start() else { return };
    let creds = server.creds_as(&p("alice"), Role::Human);
    let (nats, js) = human(&server, &creds).await;
    identities(&js, &["dev-a", "dev-b", "dev-c"]).await;
    let create = |team: &str, lead: &str, members: &[&str]| TeamCommand::Create {
        team: p(team),
        lead: (!lead.is_empty()).then(|| p(lead)),
        members: members.iter().map(|m| p(m)).collect(),
        owner: None,
        description: Some("the services".to_owned()),
    };

    let unknown = run(&nats, create("backend", "dev-a", &["dev-z"])).await.unwrap_err();
    assert_eq!(unknown.to_string(), TeamRefusal::UnknownAnt { name: p("dev-z") }.to_string());
    let taken = run(&nats, create("dev-c", "dev-a", &[])).await.unwrap_err();
    assert_eq!(taken.to_string(), TeamRefusal::NameTaken { name: p("dev-c") }.to_string());
    let a_human = run(&nats, create("backend", "dev-a", &["alice"])).await.unwrap_err();
    assert!(a_human.to_string().starts_with("alice is a human"), "{a_human}");

    run(&nats, create("backend", "dev-a", &["dev-b"])).await.unwrap();
    run(&nats, create("eng", "dev-c", &[])).await.unwrap();
    run(&nats, TeamCommand::Add { team: p("eng"), names: vec![p("backend")] }).await.unwrap();
    let directory = directory_of(&js).await;
    assert_eq!(forest_of(&directory), [(0, "eng"), (1, "backend")]);
    assert_eq!(directory.team(&p("backend")).unwrap().description.as_deref(), Some("the services"));

    let set = |lead: Option<&str>, no_description: bool| TeamCommand::Set {
        team: p("backend"),
        lead: lead.filter(|l| !l.is_empty()).map(p),
        no_lead: lead == Some(""),
        owner: None,
        no_owner: false,
        description: None,
        no_description,
    };
    assert!(run(&nats, set(None, false)).await.unwrap_err().to_string().starts_with("nothing to change"));
    run(&nats, set(Some(""), false)).await.unwrap();
    let directory = directory_of(&js).await;
    let backend = directory.team(&p("backend")).unwrap();
    assert_eq!((&backend.lead, directory.lead_of(&p("backend"))), (&None, Ok(&p("dev-c"))));
    assert_eq!(lead_label(&directory, backend), "dev-c (from team eng)");
    run(&nats, set(Some("dev-b"), true)).await.unwrap();
    run(&nats, TeamCommand::Remove { team: p("backend"), names: vec![p("dev-a")] }).await.unwrap();
    let backend = directory_of(&js).await.team(&p("backend")).unwrap().clone();
    assert_eq!((backend.lead, backend.members, backend.description), (Some(p("dev-b")), vec![p("dev-b")], None));
    run(&nats, TeamCommand::Remove { team: p("backend"), names: vec![p("dev-b")] }).await.unwrap();
    assert_eq!(directory_of(&js).await.team(&p("backend")).unwrap().lead, None, "removing the lead clears it");
    run(&nats, create("ops", "", &[])).await.unwrap();
    let directory = directory_of(&js).await;
    let ops = directory.team(&p("ops")).unwrap();
    assert_eq!((&ops.lead, lead_label(&directory, ops)), (&None, "none".to_owned()));

    run(&nats, TeamCommand::Delete { team: p("eng") }).await.unwrap();
    assert_eq!(forest_of(&directory_of(&js).await), [(0, "backend"), (0, "ops")]);
    assert!(holds_team(&js, None, &p("backend")).await);
    assert!(check_no_team(&js, None, &p("backend")).await.is_err());
}

#[tokio::test]
async fn an_ant_dropped_leaves_its_team_and_a_lead_leaves_it_to_the_lead_above() {
    let Some(server) = Server::start() else { return };
    let creds = server.creds_as(&p("alice"), Role::Human);
    let (nats, js) = human(&server, &creds).await;
    identities(&js, &["boss", "dev-a", "dev-b"]).await;
    let create = |team: &str, lead: &str, members: Vec<UserId>| TeamCommand::Create {
        team: p(team),
        lead: Some(p(lead)),
        members,
        owner: None,
        description: None,
    };
    run(&nats, create("backend", "dev-a", vec![p("dev-b")])).await.unwrap();
    run(&nats, create("eng", "boss", vec![p("backend")])).await.unwrap();

    assert_eq!(drop_ant_or_say(&js, None, &p("dev-b")).await.as_deref(), Some("took dev-b off team backend"));
    assert_eq!(drop_ant_or_say(&js, None, &p("dev-b")).await, None);
    let line = drop_ant_or_say(&js, None, &p("dev-a")).await.unwrap();
    assert_eq!(
        line,
        "took dev-a off team backend, which has no lead of its own now: it takes boss, the lead of team eng"
    );
    let backend = directory_of(&js).await.team(&p("backend")).unwrap().clone();
    assert!(backend.lead.is_none() && backend.members.is_empty(), "{backend:?}");
    let line = drop_ant_or_say(&js, None, &p("boss")).await.unwrap();
    assert!(line.starts_with("took boss off team eng, which has no lead now, nor has any team above it"), "{line}");
}
