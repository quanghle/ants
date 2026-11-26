//! Tests of the teams as a client derives them: the forest, a team's members tree with each ant's state, the lead
//! shown and the team named for an ant; and the changes a human makes, checked and applied to a directory.

use std::collections::BTreeSet;

use ants_proto::{Humans, Team, TeamDirectory, TeamRefusal};
use chrono::{TimeDelta, Utc};

use super::change::{self, Change, Create};
use super::*;
use crate::p;

/// eng (lead `boss`): boss, backend, web; backend (lead `dev-a`): dev-a, dev-b; web (lead `dev-c`): dev-c.
pub(super) fn forest_of_three() -> TeamDirectory {
    let mut directory = TeamDirectory::default();
    directory.create(Team::new(p("eng"), p("boss"), p("alice")), None).unwrap();
    let mut backend = Team::new(p("backend"), p("dev-a"), p("alice"));
    backend.members.push(p("dev-b"));
    directory.create(backend, Some(&p("eng"))).unwrap();
    directory.create(Team::new(p("web"), p("dev-c"), p("alice")), Some(&p("eng"))).unwrap();
    directory
}

/// The colony entry of `ant`, idle, its last heartbeat at `at`.
fn entry(ant: &str, at: DateTime<Utc>) -> ants_proto::ColonyEntry {
    let entry = serde_json::json!({
        "ant": ant, "machine": "m1", "instance": "run-1", "harness": "claude", "started_at": at, "heartbeat_at": at,
        "activity": "idle", "handling": 0, "harnesses": 0, "version": "0", "perms": 1, "capabilities": [],
        "escalate_to": [],
    });
    serde_json::from_value(entry).unwrap()
}

fn order(directory: &TeamDirectory) -> Vec<(usize, String, Option<String>)> {
    let name = |id: &UserId| id.to_string();
    forest(directory).iter().map(|b| (b.depth, name(&b.team.team), b.parent.as_ref().map(name))).collect()
}

#[test]
fn the_forest_lists_each_team_under_the_team_holding_it() {
    let directory = forest_of_three();
    let o = |depth, team: &str, parent: Option<&str>| (depth, team.to_owned(), parent.map(str::to_owned));
    assert_eq!(order(&directory), [o(0, "eng", None), o(1, "backend", Some("eng")), o(1, "web", Some("eng"))]);
    let branch = serde_json::to_value(&forest(&directory)[1]).unwrap();
    assert_eq!((branch["depth"].as_u64(), branch["parent"].as_str()), (Some(1), Some("eng")));
    assert_eq!(serde_json::from_value::<Branch>(branch).unwrap(), forest(&directory)[1]);
}

#[test]
fn a_team_s_lead_and_an_ant_s_team_are_named() {
    let mut directory = forest_of_three();
    assert_eq!(team_line(&directory, &p("dev-a")), "backend (lead), within eng");
    assert_eq!(team_line(&directory, &p("dev-b")), "backend, within eng");
    assert_eq!(team_line(&directory, &p("boss")), "eng (lead)");
    assert_eq!(team_line(&directory, &p("dev-z")), "none");
    directory.clear_lead(&p("backend")).unwrap();
    let backend = directory.team(&p("backend")).unwrap();
    assert_eq!(lead_label(&directory, backend), "boss (from team eng)");
    directory.clear_lead(&p("eng")).unwrap();
    assert_eq!(lead_label(&directory, directory.team(&p("backend")).unwrap()), "none");
    assert_eq!(lead_label(&directory, directory.team(&p("web")).unwrap()), "dev-c");
}

#[test]
fn a_team_s_members_are_a_tree_with_its_lead_marked_and_each_ant_s_state() {
    let directory = forest_of_three();
    let eng = directory.team(&p("eng")).unwrap();
    let now = Utc::now();
    let colony = [entry("boss", now), entry("dev-b", now - TimeDelta::minutes(5))];
    let tree = tree(&directory, eng, &colony, now);
    let ant = |name: &str, lead, state: &str| Member::Ant { name: p(name), lead, state: state.to_owned() };
    let team =
        |name: &str, lead: &str, members| Member::Team { name: p(name), lead: Some(p(lead)), lead_from: None, members };
    assert_eq!(
        tree,
        [
            ant("boss", true, "live, idle"),
            team("backend", "dev-a", vec![ant("dev-a", true, "not in the colony"), ant("dev-b", false, "stale, idle")]),
            team("web", "dev-c", vec![ant("dev-c", true, "not in the colony")]),
        ]
    );
    let json = serde_json::to_value(&tree[0]).unwrap();
    assert_eq!(json, serde_json::json!({"kind": "ant", "name": "boss", "lead": true, "state": "live, idle"}));
    assert_eq!(serde_json::from_value::<Vec<Member>>(serde_json::to_value(&tree).unwrap()).unwrap(), tree);
    let shown = Shown::of(&directory, directory.team(&p("web")).unwrap());
    assert_eq!(serde_json::to_value(&shown).unwrap()["parent"], "eng");
}

#[test]
fn a_team_created_is_checked_against_the_humans_then_the_directory() {
    let humans = Humans::of([p("alice"), p("bob")]);
    let create = |lead: Option<&str>, members: &[&str], owner: Option<&str>| Create {
        team: p("ops"),
        lead: lead.map(p),
        members: members.iter().map(|m| p(m)).collect(),
        owner: owner.map(p),
        description: Some("  the pager  ".to_owned()),
    };
    let created = create(Some("dev-x"), &["dev-x", "dev-y"], Some("bob")).check(&p("alice"), &humans).unwrap();
    assert_eq!((created.lead.as_ref(), &created.members[..]), (Some(&p("dev-x")), &[p("dev-x"), p("dev-y")][..]));
    assert_eq!((created.owner.as_ref(), created.description.as_deref()), (Some(&p("bob")), Some("the pager")));
    let a_human = create(Some("dev-x"), &["bob"], None).check(&p("alice"), &humans).unwrap_err();
    assert!(matches!(&a_human, TeamError::Invalid(why) if why.starts_with("bob is a human")), "{a_human}");
    let no_owner = create(None, &[], Some("dev-x")).check(&p("alice"), &humans).unwrap_err();
    assert!(no_owner.to_string().starts_with("dev-x is no human of the bus"), "{no_owner}");

    let known = BTreeSet::from([p("dev-x"), p("dev-y")]);
    assert_eq!(change::check_name(&p("ops"), &humans, &known), Ok(()));
    assert_eq!(
        change::check_name(&p("dev-x"), &humans, &known),
        Err(TeamRefusal::NameTaken { name: p("dev-x") }.into())
    );
    assert!(matches!(change::check_name(&p("bob"), &humans, &known), Err(TeamError::Refused(_))));
    assert!(matches!(change::check_name(&UserId::messenger(), &humans, &known), Err(TeamError::Invalid(_))));

    let mut directory = forest_of_three();
    change::create(&mut directory, &created, &known).unwrap();
    assert_eq!(directory.holder(&p("dev-y")).map(|t| &t.team), Some(&p("ops")));
    let unknown = create(None, &["dev-z"], None).check(&p("alice"), &humans).unwrap();
    assert_eq!(
        change::create(&mut forest_of_three(), &unknown, &known),
        Err(TeamRefusal::UnknownAnt { name: p("dev-z") })
    );
    let led_by_a_team = create(Some("web"), &[], None).check(&p("alice"), &humans).unwrap();
    assert_eq!(
        change::create(&mut forest_of_three(), &led_by_a_team, &known),
        Err(TeamRefusal::NotLeadable { ant: p("web"), team: p("ops") })
    );
}

#[test]
fn a_change_adds_before_the_lead_is_set_and_removes_after() {
    let humans = Humans::of([p("alice"), p("bob")]);
    let backend = p("backend");
    assert!(matches!(Change::default().check(&backend, &humans), Err(TeamError::Invalid(_))));
    let add_human = Change { add: vec![p("bob")], ..Change::default() };
    assert!(add_human.check(&backend, &humans).is_err());
    let long = Change { description: Some(Some("x".repeat(1001))), ..Change::default() };
    assert!(long.check(&backend, &humans).is_err());

    let mut directory = forest_of_three();
    let known = BTreeSet::from([p("dev-d")]);
    let change = Change {
        add: vec![p("dev-d")],
        remove: vec![p("dev-a")],
        lead: Some(Some(p("dev-d"))),
        owner: Some(Some(p("bob"))),
        ..Change::default()
    };
    change.check(&backend, &humans).unwrap();
    let changed = change.apply(&mut directory, &backend, &known).unwrap();
    assert_eq!((&changed.added[..], &changed.removed[..]), (&[p("dev-d")][..], &[p("dev-a")][..]));
    let team = changed.team.clone().unwrap();
    assert_eq!((team.lead, team.owner, team.members), (Some(p("dev-d")), Some(p("bob")), vec![p("dev-b"), p("dev-d")]));
    assert_eq!(
        change.line(&backend, &changed),
        "team backend: added dev-d, removed dev-a, lead now dev-d, owner now bob"
    );
    let unknown = Change { add: vec![p("dev-z")], ..Change::default() };
    assert_eq!(unknown.apply(&mut directory, &backend, &known), Err(TeamRefusal::UnknownAnt { name: p("dev-z") }));
    let on_a_team = Change { add: vec![p("dev-c")], ..Change::default() };
    let known = BTreeSet::from([p("dev-c")]);
    assert_eq!(
        on_a_team.apply(&mut directory, &backend, &known),
        Err(TeamRefusal::OnTeam { member: p("dev-c"), team: p("web") })
    );
}

#[test]
fn a_team_error_says_the_refusal_and_serializes_by_kind() {
    let refused = TeamError::from(TeamRefusal::NoSuchTeam { team: p("ops") });
    assert_eq!(refused.to_string(), "no team ops");
    let json = serde_json::to_value(&refused).unwrap();
    assert_eq!(json, serde_json::json!({"kind": "refused", "detail": {"refusal": "no_such_team", "team": "ops"}}));
    assert_eq!(serde_json::to_value(TeamError::Conflict).unwrap(), serde_json::json!({"kind": "conflict"}));
}
