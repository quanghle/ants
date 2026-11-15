use ants_test_support::NatsServer;

use super::*;
use crate::{bus, hub};

fn p(s: &str) -> UserId {
    s.parse().unwrap()
}

/// The first daemon to start an ant creates its record; one starting it later, here or elsewhere, keeps it.
#[tokio::test]
async fn a_record_is_created_only_when_absent() {
    let Some(server) = NatsServer::start() else { return };
    let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    let dev = Identity::new(&js, None, &p("dev-a")).await.unwrap();
    assert_eq!(dev.ensure().await.unwrap(), None, "no bucket yet");
    js.create_key_value(hub::identities_config()).await.unwrap();

    let born = dev.ensure().await.unwrap().unwrap();
    assert_eq!((born.ant.as_str(), born.prompts, born.id.len()), ("dev-a", 0, 32));
    assert!(born.id.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()), "{}", born.id);
    let elsewhere = Identity::new(&js, None, &p("dev-a")).await.unwrap();
    assert_eq!(elsewhere.ensure().await.unwrap().unwrap(), born, "the first creator wins");
    let other = Identity::new(&js, None, &p("dev-b")).await.unwrap().ensure().await.unwrap().unwrap();
    assert_ne!(other.id, born.id, "each ant has its own id");

    // A human's colour stays through a restart.
    let kv = js.get_key_value(IDENTITIES_BUCKET).await.unwrap();
    let painted = AntIdentity { color: Some("rose".into()), ..born.clone() };
    kv.put("dev-a", serde_json::to_vec(&painted).unwrap().into()).await.unwrap();
    assert_eq!(dev.ensure().await.unwrap().unwrap(), painted);
}

/// The daemon serving an ant counts its prompts by compare-and-set: a standby taking the ant over carries on the count,
/// and a human's change in between is kept.
#[tokio::test]
async fn prompts_are_counted_across_a_standby_takeover() {
    let Some(server) = NatsServer::start() else { return };
    let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    js.create_key_value(hub::identities_config()).await.unwrap();
    let (_, standby_js) = bus::connect(&server.url, None, None, &UserId::human(), "standby").await.unwrap();
    let serving = Identity::new(&js, None, &p("dev-a")).await.unwrap();
    let standby = Identity::new(&standby_js, None, &p("dev-a")).await.unwrap();

    let born = serving.ensure().await.unwrap().unwrap();
    for _ in 0..3 {
        serving.count_prompt().await.unwrap();
    }
    assert_eq!(standby.ensure().await.unwrap().unwrap().prompts, 3, "the standby takes the record as it is");
    let kv = js.get_key_value(IDENTITIES_BUCKET).await.unwrap();
    let mut record: AntIdentity = serde_json::from_slice(&kv.get("dev-a").await.unwrap().unwrap()).unwrap();
    record.avatar = Some("lucky".into());
    kv.put("dev-a", serde_json::to_vec(&record).unwrap().into()).await.unwrap();
    let counted = standby.count_prompt().await.unwrap().unwrap();
    assert_eq!((counted.prompts, counted.avatar.as_deref(), &counted.id), (4, Some("lucky"), &born.id));

    // Both counting at once, as while a takeover overlaps: no prompt is lost.
    for _ in 0..5 {
        let (a, b) = tokio::join!(serving.count_prompt(), standby.count_prompt());
        a.unwrap();
        b.unwrap();
    }
    let record: AntIdentity = serde_json::from_slice(&kv.get("dev-a").await.unwrap().unwrap()).unwrap();
    assert_eq!(record.prompts, 14);
}
