//! Tests of the doctor's team checks: a team with no lead, nor any above it, a lead not live, an owner no human, an
//! unknown member and a team taking the name of an ant or a human; and whether `ants signer` answers each ant's team
//! changes.

use std::collections::BTreeSet;

use ants_proto::{Humans, Team, TeamDirectory};

use super::super::teams::{check_team_changes, findings};
use super::*;
use crate::test_support::colony::entry;

fn p(id: &str) -> UserId {
    id.parse().unwrap()
}

/// backend (lead `dev-a`): dev-a, dev-b.
fn backend() -> TeamDirectory {
    let mut directory = TeamDirectory::default();
    let mut backend = Team::new(p("backend"), p("dev-a"), p("alice"));
    backend.members.push(p("dev-b"));
    directory.create(backend, None).unwrap();
    directory
}

#[test]
fn a_sound_directory_is_ok_and_an_empty_one_too() {
    let now = chrono::Utc::now();
    let known = BTreeSet::from([p("dev-a"), p("dev-b")]);
    let colony = [entry("dev-a", 60, 2, now)];
    let checks = findings(&backend(), &known, Some(&Humans::default()), Some(&colony), now);
    assert_eq!(checks.len(), 1, "{checks:?}");
    assert_eq!(checks[0].status, Status::Ok, "{}", checks[0]);
    assert_eq!(checks[0].detail, "1 team, with a lead, every member an ant of the bus or a team");
    let checks = findings(&TeamDirectory::default(), &BTreeSet::new(), None, None, now);
    assert_eq!((checks[0].status, checks[0].detail.as_str()), (Status::Ok, "no teams"));
}

#[test]
fn an_unknown_member_and_a_taken_name_fail_and_a_team_with_no_lead_and_a_lead_not_live_warn() {
    let now = chrono::Utc::now();
    let mut directory = backend();
    directory.create(Team::new(p("human"), p("dev-c"), p("alice")), None).unwrap();
    directory.drop_ant(&p("dev-a"));
    let known = BTreeSet::from([p("dev-c")]);
    let colony = [entry("dev-c", 60, 600, now)];
    let checks = findings(&directory, &known, Some(&Humans::of([p("human")])), Some(&colony), now);
    let details: Vec<(Status, &str)> = checks.iter().map(|c| (c.status, c.detail.as_str())).collect();
    assert_eq!(
        details,
        [
            (Status::Warn, "team backend has no lead, nor has any team above it: messages to it are refused"),
            (Status::Fail, "team backend holds dev-b, neither an ant of the bus nor a team"),
            (Status::Fail, "the name human is both a team and a human: messages to human go to the team's lead"),
            (Status::Warn, "dev-c, the lead of team human, is not live in the colony: messages to human wait"),
        ]
    );
    assert_eq!(checks[0].fix, ["ants team set backend --lead ANT"]);
    assert_eq!(checks[1].fix, ["ants team remove backend dev-b"]);
}

#[test]
fn an_owner_no_human_warns_unless_the_humans_are_unread() {
    let now = chrono::Utc::now();
    let mut directory = backend();
    directory.teams[0].owner = Some(p("dev-b"));
    let known = BTreeSet::from([p("dev-a"), p("dev-b")]);
    let checks = findings(&directory, &known, Some(&Humans::of([p("alice")])), None, now);
    let details: Vec<(Status, &str)> = checks.iter().map(|c| (c.status, c.detail.as_str())).collect();
    assert_eq!(
        details,
        [(Status::Warn, "dev-b, the owner of team backend, is no human of the bus: no human answers for the team")]
    );
    assert_eq!(checks[0].fix, ["ants team set backend --owner HUMAN, or ants team set backend --no-owner"]);
    directory.teams[0].owner = Some(p("alice"));
    let checks = findings(&directory, &known, Some(&Humans::of([p("alice")])), None, now);
    assert_eq!(checks[0].status, Status::Ok, "{}", checks[0]);
    directory.teams[0].owner = Some(p("dev-b"));
    assert_eq!(findings(&directory, &known, None, None, now)[0].status, Status::Ok, "the humans unread");
}

#[test]
fn a_team_taking_the_lead_above_is_sound() {
    let now = chrono::Utc::now();
    let mut directory = backend();
    directory.create(Team::unled(p("db"), p("alice")), Some(&p("backend"))).unwrap();
    let known = BTreeSet::from([p("dev-a"), p("dev-b")]);
    let colony = [entry("dev-a", 60, 2, now)];
    let checks = findings(&directory, &known, None, Some(&colony), now);
    assert_eq!(checks.len(), 1, "{checks:?}");
    assert_eq!(
        (checks[0].status, checks[0].detail.as_str()),
        (Status::Ok, "2 teams, each with a lead, its own or one above it, every member an ant of the bus or a team")
    );
}

#[test]
fn a_lead_unread_in_the_colony_is_not_warned_of() {
    let now = chrono::Utc::now();
    let known = BTreeSet::from([p("dev-a"), p("dev-b")]);
    let checks = findings(&backend(), &known, None, None, now);
    assert_eq!(checks[0].status, Status::Ok, "{}", checks[0]);
}

#[tokio::test]
async fn the_team_changes_check_tells_whether_the_signer_answers() {
    let Some(server) = Server::start() else { return };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let creds = server.creds_as(&dev_a, Role::Ant);
    let config = server.dir().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nharness = \"copilot\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\ncreds = {creds:?}\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n"
        ),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = setup(&server, None);
    let wait = ants_proto::TEAMADM_REPEAT + Duration::from_millis(500);
    let checks = check_team_changes(&loaded.ants, &setup, wait).await;
    let open = named(&checks, "ant dev-b team changes")[0];
    assert_eq!(open.status, Status::Ok, "{open}");
    let none = named(&checks, "ant dev-a team changes")[0];
    assert_eq!(none.status, Status::Warn, "{none}");
    assert!(none.detail.starts_with("nothing answers its team changes"), "{none}");

    let (human, js) = server.connect_as(&"alice".parse().unwrap(), Role::Human).await;
    js.create_stream(ants_daemon::hub::teams_config()).await.unwrap();
    drop(human);
    let approver =
        crate::credentials::approver::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, wait)
            .await
            .unwrap();
    let keeping = ants_daemon::teams::keeper::serve(approver.client.clone(), approver.hub().clone(), None);
    let signer = tokio::spawn(keeping);
    let checks = check_team_changes(&loaded.ants, &setup, wait).await;
    let answered = named(&checks, "ant dev-a team changes")[0];
    assert_eq!((answered.status, answered.detail.as_str()), (Status::Ok, "ants signer answers manage_team"));
    signer.abort();
}
