//! Tests of the team keeper: it changes only the teams the asking ant manages, on the directory as the ant read it,
//! answers a repeated request with its first answer, and a daemon without credentials changes them itself the same way.

use ants_proto::{AntIdentity, Team, TeamDirectory, TeamOp};
use ants_test_support::NatsServer;

use super::*;
use crate::bus;
use ants_test_support::user as p;

/// eng (lead `boss`) holding backend (lead `dev-a`).
fn forest() -> TeamDirectory {
    let mut directory = TeamDirectory::default();
    directory.create(Team::new(p("eng"), p("boss"), ants_test_support::human()), None).unwrap();
    directory.create(Team::new(p("backend"), p("dev-a"), ants_test_support::human()), Some(&p("eng"))).unwrap();
    directory
}

fn refusal(outcome: Option<TeamAdminOutcome>) -> TeamRefusal {
    match outcome {
        Some(TeamAdminOutcome::Refused { refusal }) => refusal,
        other => panic!("{other:?} rather than a refusal"),
    }
}

#[tokio::test]
async fn the_keeper_changes_only_the_teams_an_ant_manages() {
    let Some(server) = NatsServer::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "test").await.unwrap();
    js.create_stream(crate::hub::teams_config()).await.unwrap();
    let identities = js.create_key_value(crate::hub::identities_config()).await.unwrap();
    for ant in ["boss", "dev-a", "dev-b"] {
        let record = AntIdentity::new(p(ant), format!("{ant}-id"), Utc::now());
        identities.put(ant, serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    }
    let kv = super::super::open(&js, None).await.unwrap();
    assert!(super::super::write(&kv, &forest(), 0).await.unwrap());
    let serving = tokio::spawn(serve(client, js.clone(), None));
    let (a, _) = bus::connect(&server.url, None, None, &p("dev-a"), "test").await.unwrap();
    let last = super::super::read(&kv).await.unwrap().unwrap().last;

    let add = |member: &str| vec![TeamOp::AddMember { member: p(member) }];
    let ask = TeamAdminRequest::new(p("dev-a"), p("backend"), add("dev-b"), last);
    let outcome = request(&a, &ask).await.unwrap();
    let Some(TeamAdminOutcome::Applied { last: now, changed }) = outcome.clone() else { panic!("{outcome:?}") };
    assert_eq!(changed, ["added dev-b to team backend"]);
    let (directory, stored) = super::super::read_checked(&kv).await.unwrap();
    assert_eq!(stored, now);
    assert!(directory.team(&p("backend")).unwrap().is_member(&p("dev-b")));
    assert_eq!(request(&a, &ask).await.unwrap(), outcome, "a repeat gets the first answer");

    let stale = TeamAdminRequest::new(p("dev-a"), p("backend"), add("boss"), last);
    assert_eq!(request(&a, &stale).await.unwrap(), Some(TeamAdminOutcome::Changed));
    let above = TeamAdminRequest::new(p("dev-a"), p("eng"), add("dev-c"), now);
    assert_eq!(
        refusal(request(&a, &above).await.unwrap()),
        TeamRefusal::NotManager { ant: p("dev-a"), team: p("eng") }
    );
    let ghost = TeamAdminRequest::new(p("dev-a"), p("backend"), add("ghost"), now);
    assert_eq!(refusal(request(&a, &ghost).await.unwrap()), TeamRefusal::UnknownAnt { name: p("ghost") });
    let forged = TeamAdminRequest::new(p("boss"), p("eng"), add("dev-c"), now);
    a.publish(subject::teamadm(&p("dev-a")), serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
    assert_eq!(super::super::read(&kv).await.unwrap().unwrap().last, now, "a request on another's subject is dropped");

    // A daemon without credentials changes the directory itself, with the same checks.
    let identities = DirectKv::whole(&js, None, IDENTITIES_BUCKET).await.unwrap();
    let changer = Changer { client: a.clone(), kv: kv.clone(), identities: Some(identities), signer: false };
    let create = vec![TeamOp::CreateTeam { team: p("db"), lead: Some(p("dev-b")), description: None }];
    let outcome = changer.change(&p("boss"), &p("backend"), create, None).await.unwrap();
    assert!(matches!(outcome, Some(TeamAdminOutcome::Applied { .. })), "{outcome:?}");
    let (directory, _) = super::super::read_checked(&kv).await.unwrap();
    assert_eq!(directory.holder(&p("db")).unwrap().team, p("backend"));
    assert_eq!(directory.team(&p("db")).unwrap().created_by, p("boss"));
    let gone = vec![TeamOp::DeleteTeam { team: p("db") }];
    assert!(matches!(
        changer.change(&p("dev-b"), &p("db"), gone, None).await.unwrap(),
        Some(TeamAdminOutcome::Refused { .. })
    ));
    serving.abort();
}
