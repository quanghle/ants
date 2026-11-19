use ants_proto::UserId;
use ants_test_support::NatsServer;
use chrono::Utc;

use super::*;
use crate::bus;

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

fn trail(name: &str, by: &str) -> SavedTrail {
    SavedTrail {
        name: name.parse().unwrap(),
        description: String::new(),
        phases: Vec::new(),
        waypoints: Vec::new(),
        script: "agent('build')".to_owned(),
        saved_by: Saver::Ant { user: p(by) },
        saved_from: None,
        saved_at: Utc::now(),
    }
}

fn save(ant: &str, trail: &SavedTrail, last: Option<u64>) -> SaveTrailRequest {
    SaveTrailRequest::new(p(ant), TrailChange::Save { trail: trail.clone(), last })
}

fn remove(ant: &str, name: &str, last: u64) -> SaveTrailRequest {
    SaveTrailRequest::new(p(ant), TrailChange::Remove { name: name.parse().unwrap(), last })
}

fn refused(outcome: Option<SaveTrailOutcome>, says: &str) {
    match outcome {
        Some(SaveTrailOutcome::Refused { why }) => assert!(why.contains(says), "{why}"),
        other => panic!("{other:?} rather than a refusal saying {says}"),
    }
}

/// An ant saves, replaces and removes only its own trails, as itself, and asks on its own subject only.
#[tokio::test]
async fn the_keeper_writes_an_ant_s_own_trails_only() {
    let Some(server) = NatsServer::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    js.create_key_value(crate::hub::trails_config()).await.unwrap();
    let kv = DirectKv::whole(&js, None, TRAILS_BUCKET).await.unwrap();
    let serving = tokio::spawn(serve(client, js.clone(), None));
    let (a, _) = bus::connect(&server.url, None, None, &p("dev-a"), "test").await.unwrap();
    let (b, _) = bus::connect(&server.url, None, None, &p("dev-b"), "test").await.unwrap();
    let last = async |name: &str| kv.lookup_last(name).await.unwrap().1;

    let deploy = trail("deploy", "dev-a");
    let saving = save("dev-a", &deploy, None);
    assert_eq!(request(&a, &saving).await.unwrap(), Some(SaveTrailOutcome::Done));
    assert_eq!(request(&a, &saving).await.unwrap(), Some(SaveTrailOutcome::Done), "a repeat");
    let stored: SavedTrail = serde_json::from_slice(&kv.get("deploy").await.unwrap().unwrap()).unwrap();
    assert_eq!(stored, deploy);
    let seq = last("deploy").await;

    let as_a = trail("deploy", "dev-a");
    refused(request(&b, &save("dev-b", &as_a, Some(seq))).await.unwrap(), "as its own");
    let over = trail("deploy", "dev-b");
    refused(request(&b, &save("dev-b", &over, Some(seq))).await.unwrap(), "saved by ant dev-a");
    refused(request(&b, &save("dev-b", &over, None)).await.unwrap(), "saved by ant dev-a");
    refused(request(&b, &remove("dev-b", "deploy", seq)).await.unwrap(), "saved by ant dev-a");
    let invalid = SavedTrail { script: " ".to_owned(), ..trail("blank", "dev-a") };
    refused(request(&a, &save("dev-a", &invalid, None)).await.unwrap(), "script");

    // A request naming another ant than its subject does goes unanswered, and changes nothing.
    let mut replies = b.subscribe(subject::save_trail_reply(&p("dev-a"))).await.unwrap();
    let forged = remove("dev-a", "deploy", seq);
    b.publish(subject::save_trail(&p("dev-b")), serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(500), replies.next()).await.is_err());
    assert_eq!(last("deploy").await, seq, "unchanged");

    let replaced = SavedTrail { script: "agent('ship')".to_owned(), ..trail("deploy", "dev-a") };
    assert_eq!(request(&a, &save("dev-a", &replaced, Some(seq - 1))).await.unwrap(), Some(SaveTrailOutcome::Changed));
    assert_eq!(request(&a, &save("dev-a", &replaced, Some(seq))).await.unwrap(), Some(SaveTrailOutcome::Done));
    assert_eq!(request(&a, &remove("dev-a", "deploy", seq)).await.unwrap(), Some(SaveTrailOutcome::Changed), "stale");
    let removing = remove("dev-a", "deploy", last("deploy").await);
    assert_eq!(request(&a, &removing).await.unwrap(), Some(SaveTrailOutcome::Done));
    assert_eq!(request(&a, &removing).await.unwrap(), Some(SaveTrailOutcome::Done), "a repeat");
    assert!(kv.get("deploy").await.unwrap().is_none());

    // A human's trail is no ant's to replace or remove.
    let theirs = SavedTrail { saved_by: Saver::Human { user: UserId::human() }, ..trail("release", "dev-a") };
    kv.put("release", serde_json::to_vec(&theirs).unwrap()).await.unwrap();
    let seq = last("release").await;
    refused(request(&a, &remove("dev-a", "release", seq)).await.unwrap(), "saved by");
    refused(request(&a, &save("dev-a", &trail("release", "dev-a"), Some(seq))).await.unwrap(), "saved by");
    serving.abort();
}
