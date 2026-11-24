//! The team directory: the forest rule and the lead rule on every change and every read, leads inherited from the
//! teams above, the changes a lead may make of the teams it manages, the requests asking for them and the team note of
//! a member's prompt.

use super::*;
use crate::{TeamAdminRequest, p};

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

fn colony(name: &UserId) -> bool {
    name.as_str().starts_with("dev-") || name.as_str() == "boss"
}

#[test]
fn the_forest_reads_back_and_its_shape_is_known() {
    let directory = forest();
    assert_eq!(directory.check(), Ok(()));
    let bytes = serde_json::to_vec(&directory).unwrap();
    assert_eq!(TeamDirectory::read(&bytes).unwrap(), directory);
    assert_eq!(directory.roots().iter().map(|t| t.team.as_str()).collect::<Vec<_>>(), ["eng"]);
    assert_eq!(directory.holder(&p("dev-b")).unwrap().team, p("backend"));
    let up: Vec<_> = directory.ancestors(&p("dev-b")).iter().map(|t| t.team.to_string()).collect();
    assert_eq!(up, ["backend", "eng"]);
    assert_eq!(directory.ants_under(&p("eng")), [p("boss"), p("dev-a"), p("dev-b"), p("dev-c")]);
    assert!(directory.manages(&p("boss"), &p("backend")) && directory.manages(&p("dev-a"), &p("backend")));
    assert!(!directory.manages(&p("dev-a"), &p("eng")) && !directory.manages(&p("dev-c"), &p("backend")));
    assert!(directory.is_beneath(&p("web"), &p("eng")) && !directory.is_beneath(&p("eng"), &p("web")));
    assert_eq!(directory.led_by(&p("dev-a")).len(), 1);
}

#[test]
fn no_ant_or_team_is_on_two_teams_and_no_team_holds_itself() {
    let mut directory = forest();
    assert_eq!(
        directory.add(&p("web"), &p("dev-b")),
        Err(TeamRefusal::OnTeam { member: p("dev-b"), team: p("backend") })
    );
    assert_eq!(directory.add(&p("backend"), &p("dev-b")), Ok(false), "already a member");
    assert_eq!(
        directory.add(&p("backend"), &p("eng")),
        Err(TeamRefusal::HoldsItself { team: p("backend"), member: p("eng") })
    );
    let mut top = TeamDirectory::default();
    top.create(Team::new(p("a"), p("x"), p("alice")), None).unwrap();
    top.create(Team::new(p("b"), p("y"), p("alice")), Some(&p("a"))).unwrap();
    assert_eq!(top.add(&p("b"), &p("a")), Err(TeamRefusal::HoldsItself { team: p("b"), member: p("a") }));
    assert_eq!(top.add(&p("a"), &p("a")), Err(TeamRefusal::HoldsItself { team: p("a"), member: p("a") }));
    assert!(directory.add(&p("web"), &p("dev-z")).unwrap());

    // A directory written by hand that breaks the rule is unreadable.
    let mut broken = forest();
    broken.teams[2].members.push(p("dev-a"));
    assert_eq!(broken.check(), Err(TeamRefusal::OnTeam { member: p("dev-a"), team: p("backend") }));
    assert!(matches!(TeamDirectory::read(&serde_json::to_vec(&broken).unwrap()), Err(TeamRefusal::Unreadable { .. })));
    let mut cycle = forest();
    cycle.teams[1].members.push(p("eng"));
    assert!(matches!(cycle.check(), Err(TeamRefusal::HoldsItself { .. })));
    let mut twice = forest();
    twice.teams.push(twice.teams[0].clone());
    assert_eq!(twice.check(), Err(TeamRefusal::TeamTwice { team: p("eng") }));
}

#[test]
fn a_lead_is_one_of_its_team_s_direct_ants_and_removing_it_clears_it() {
    let mut directory = forest();
    let mut outside = Team::new(p("ops"), p("dev-e"), p("alice"));
    outside.members.clear();
    assert_eq!(directory.create(outside, None), Err(TeamRefusal::NotLeadable { ant: p("dev-e"), team: p("ops") }));
    assert_eq!(
        directory.create(Team::new(p("ops"), p("web"), p("alice")), None),
        Err(TeamRefusal::NotLeadable { ant: p("web"), team: p("ops") }),
        "a team leads nothing"
    );
    assert_eq!(
        directory.create(Team::new(p("eng"), p("dev-e"), p("alice")), None),
        Err(TeamRefusal::TeamExists { team: p("eng") })
    );
    assert_eq!(
        directory.set_lead(&p("backend"), &p("dev-c")),
        Err(TeamRefusal::NotLeadable { ant: p("dev-c"), team: p("backend") })
    );
    assert_eq!(directory.set_lead(&p("backend"), &p("dev-b")), Ok(true));
    assert_eq!(directory.remove(&p("backend"), &p("dev-b")), Ok(true));
    assert_eq!(directory.team(&p("backend")).unwrap().lead, None, "removing the lead clears it");
    assert_eq!(directory.lead_of(&p("backend")), Ok(&p("boss")), "inherited from team eng");
    assert_eq!(directory.check(), Ok(()));
    let mut hand = forest();
    hand.teams[1].lead = Some(p("dev-c"));
    assert_eq!(hand.check(), Err(TeamRefusal::NotLeadable { ant: p("dev-c"), team: p("backend") }));
}

#[test]
fn retiring_revoking_and_deleting_keep_the_forest() {
    let mut directory = forest();
    assert_eq!(directory.lead_of(&p("backend")), Ok(&p("dev-a")));
    assert_eq!(directory.lead_of(&p("nobody")), Err(TeamRefusal::NoSuchTeam { team: p("nobody") }));
    assert_eq!(directory.drop_ant(&p("dev-b")), Some(p("backend")));
    assert_eq!(directory.drop_ant(&p("dev-b")), None);
    assert_eq!(directory.drop_ant(&p("dev-a")), Some(p("backend")), "a revoked lead");
    let backend = directory.team(&p("backend")).unwrap();
    assert!(backend.lead.is_none() && backend.members.is_empty());
    assert_eq!(directory.check(), Ok(()));
    assert_eq!(directory.lead_of(&p("backend")), Ok(&p("boss")), "a revoked lead's team inherits");
    assert_eq!(directory.drop_ant(&p("boss")), Some(p("eng")));
    assert_eq!(directory.lead_of(&p("backend")), Err(TeamRefusal::NoLead { team: p("backend") }));
    assert_eq!(directory.check(), Ok(()), "no lead anywhere breaks nothing");
    let changed = directory.delete(&p("eng")).unwrap();
    assert_eq!(changed, ["deleted team eng"]);
    assert_eq!(directory.roots().len(), 2, "its member teams are on no team");
    assert!(directory.holder(&p("boss")).is_none());
    assert_eq!(directory.delete(&p("eng")), Err(TeamRefusal::NoSuchTeam { team: p("eng") }));
}

#[test]
fn a_lead_changes_the_teams_it_manages_only() {
    let mut directory = forest();
    let before = directory.clone();
    let add = [TeamOp::AddMember { member: p("dev-e") }];
    assert_eq!(
        directory.apply_for_ant(&p("dev-c"), &p("backend"), &add, colony),
        Err(TeamRefusal::NotManager { ant: p("dev-c"), team: p("backend") })
    );
    assert_eq!(
        directory.apply_for_ant(&p("dev-a"), &p("eng"), &add, colony),
        Err(TeamRefusal::NotManager { ant: p("dev-a"), team: p("eng") }),
        "never a team above its own"
    );
    assert_eq!(
        directory.apply_for_ant(&p("dev-a"), &p("backend"), &[TeamOp::AddMember { member: p("ghost") }], colony),
        Err(TeamRefusal::UnknownAnt { name: p("ghost") })
    );
    let ops = [
        TeamOp::AddMember { member: p("dev-e") },
        TeamOp::SetDescription { description: "Services.".to_owned() },
        TeamOp::RemoveMember { member: p("dev-a") },
    ];
    let wrong = [ops[0].clone(), TeamOp::SetLead { ant: p("dev-c") }];
    assert_eq!(
        directory.apply_for_ant(&p("dev-a"), &p("backend"), &wrong, colony),
        Err(TeamRefusal::NotLeadable { ant: p("dev-c"), team: p("backend") })
    );
    assert_eq!(directory, before, "a refusal changes nothing");
    let changed = directory.apply_for_ant(&p("boss"), &p("backend"), &ops[..2], colony).unwrap();
    assert_eq!(changed, ["added dev-e to team backend", "described team backend"]);
    assert!(directory.apply_for_ant(&p("boss"), &p("backend"), &ops[..2], colony).unwrap().is_empty());

    // Teams beneath: created with an ant on no team or a member moved down, deleted only from the team holding them.
    let create = [TeamOp::CreateTeam { team: p("db"), lead: Some(p("dev-b")), description: None }];
    let changed = directory.apply_for_ant(&p("dev-a"), &p("backend"), &create, colony).unwrap();
    assert_eq!(changed[1], "moved dev-b from team backend to team db");
    assert_eq!(directory.holder(&p("db")).unwrap().team, p("backend"));
    assert_eq!(directory.team(&p("db")).unwrap().created_by, p("dev-a"));
    let unled = [TeamOp::CreateTeam { team: p("cache"), lead: None, description: None }];
    let changed = directory.apply_for_ant(&p("dev-a"), &p("backend"), &unled, colony).unwrap();
    assert_eq!(changed[0], "created team cache, without a lead of its own");
    assert_eq!(directory.lead_of(&p("cache")), Ok(&p("dev-a")), "a team created without a lead takes its creator's");
    assert_eq!(directory.inherited_by(&p("dev-a")).len(), 1);
    assert!(
        directory.apply_for_ant(&p("dev-a"), &p("cache"), &[TeamOp::AddMember { member: p("dev-f") }], colony).is_ok()
    );
    let elsewhere = [TeamOp::CreateTeam { team: p("cache2"), lead: Some(p("dev-c")), description: None }];
    assert_eq!(
        directory.apply_for_ant(&p("dev-a"), &p("backend"), &elsewhere, colony),
        Err(TeamRefusal::OnTeam { member: p("dev-c"), team: p("web") })
    );
    assert_eq!(
        directory.apply_for_ant(&p("dev-a"), &p("backend"), &[TeamOp::DeleteTeam { team: p("web") }], colony),
        Err(TeamRefusal::NotBeneath { team: p("web"), parent: p("backend") })
    );
    assert_eq!(
        directory.apply_for_ant(&p("boss"), &p("eng"), &[TeamOp::DeleteTeam { team: p("db") }], colony),
        Err(TeamRefusal::NotBeneath { team: p("db"), parent: p("eng") })
    );
    assert!(
        directory.apply_for_ant(&p("dev-a"), &p("backend"), &[TeamOp::DeleteTeam { team: p("db") }], colony).is_ok()
    );
    assert!(directory.team(&p("db")).is_none() && directory.holder(&p("dev-b")).is_none());
    assert_eq!(
        directory.apply_for_ant(&p("dev-a"), &p("backend"), &[TeamOp::SetLead { ant: p("dev-e") }], colony),
        Ok(vec!["dev-e leads team backend".to_owned()])
    );
    assert!(!directory.manages(&p("dev-a"), &p("backend")));
    assert_eq!(
        directory.apply_for_ant(&p("boss"), &p("backend"), &[TeamOp::ClearLead], colony),
        Ok(vec!["team backend has no lead of its own".to_owned()])
    );
    assert_eq!(directory.lead_of(&p("backend")), Ok(&p("boss")));
}

#[test]
fn a_team_without_a_lead_takes_the_nearest_lead_above() {
    let mut directory = forest();
    directory.create(Team::unled(p("ops"), p("alice")), Some(&p("backend"))).unwrap();
    directory.add(&p("ops"), &p("dev-o")).unwrap();
    assert_eq!(directory.lead_of(&p("ops")), Ok(&p("dev-a")));
    assert_eq!(directory.lead_source(&p("ops")).unwrap().team, p("backend"));
    directory.clear_lead(&p("backend")).unwrap();
    assert_eq!(directory.lead_of(&p("ops")), Ok(&p("boss")), "past a team without one either");
    assert_eq!(
        directory.inherited_by(&p("boss")).iter().map(|t| t.team.as_str()).collect::<Vec<_>>(),
        ["backend", "ops"]
    );
    let mut top = TeamDirectory::default();
    top.create(Team::unled(p("solo"), p("alice")), None).unwrap();
    assert_eq!(top.lead_of(&p("solo")), Err(TeamRefusal::NoLead { team: p("solo") }));
    assert_eq!(top.lead_of(&p("ghost")), Err(TeamRefusal::NoSuchTeam { team: p("ghost") }));
    // The parent's lead moved down into a new team leaves the parent without one.
    let changed = directory.create(Team::new(p("infra"), p("boss"), p("alice")), Some(&p("eng"))).unwrap();
    assert_eq!(
        changed[1..],
        ["moved boss from team eng to team infra", "team eng has no lead of its own", "added team infra to team eng"]
    );
    assert_eq!(directory.lead_of(&p("ops")), Err(TeamRefusal::NoLead { team: p("ops") }));
}

#[test]
fn team_change_requests_match_their_subject() {
    let request = TeamAdminRequest::new(p("lead"), p("backend"), vec![TeamOp::ClearDescription], 7);
    assert_eq!(request.subject(), "ants.teamadm.lead");
    assert_eq!(request.reply_subject(), "_INBOX.lead.teamadm");
    assert!(request.check_subject("ants.teamadm.lead").is_ok());
    assert!(request.check_subject("ants.teamadm.dev-a").is_err());
    assert!(!request.is_expired(request.requested_at) && request.is_expired(request.expires_at));
    assert_eq!(request.check_ops(), Ok(()));
    assert!(TeamAdminRequest { ops: Vec::new(), ..request.clone() }.check_ops().is_err());
    let reply = TeamAdminReply { request_id: request.id, ant: p("lead"), outcome: TeamAdminOutcome::Changed };
    assert!(reply.check_subject("_INBOX.lead.teamadm").is_ok() && reply.check_subject("_INBOX.x.teamadm").is_err());
}

#[test]
fn a_member_s_note_names_its_team_and_the_teams_above() {
    let mut directory = forest();
    directory.team_mut(&p("backend")).unwrap().description = Some("Services and their APIs.".to_owned());
    directory.team_mut(&p("eng")).unwrap().owner = Some(p("alice"));
    let note = directory.note_for(&p("dev-a")).unwrap();
    assert!(note.starts_with("[ants] You are a member of team backend, within team eng."), "{note}");
    assert!(note.contains("Team backend (you lead it"), "{note}");
    assert!(note.contains("Services and their APIs.") && note.contains("- owner: alice (a human)"), "{note}");
    assert!(note.contains("- members: boss, team backend, team web"), "{note}");
    assert!(directory.note_for(&p("dev-z")).is_none());
    directory.create(Team::unled(p("db"), p("alice")), Some(&p("backend"))).unwrap();
    directory.add(&p("db"), &p("dev-d")).unwrap();
    let note = directory.note_for(&p("dev-d")).unwrap();
    assert!(note.contains("Team db:\n- lead: dev-a, the lead of team backend above it"), "{note}");
    let lead = directory.note_for(&p("dev-a")).unwrap();
    assert!(lead.ends_with("without a lead of their own (messages to them come to you): db"), "{lead}");
}
