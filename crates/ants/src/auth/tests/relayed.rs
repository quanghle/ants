//! Messages a stream relayed: an ant stores a message under a key of its own and asks for it by a direct get naming a
//! reply subject it may not publish to, where the stream sends it with the headers the ant stored. Each reader of such
//! a subject refuses it. Likewise what the server itself answers such a subject with: the ack of a publish into a
//! stream, or the status of a direct get finding nothing.

use ants_daemon::bus::relayed;
use ants_daemon::hub;
use ants_daemon::records::direct_kv::Lookup;
use ants_daemon::stream::reading::{self, Reading};
use ants_daemon::trail::keeper;
use ants_proto::{
    RatingOutcome, Retirement, RunId, SaveTrailRequest, SavedTrail, Saver, TRAIL_RATINGS_BUCKET, TRAIL_RUNS_BUCKET,
    TRAIL_TIPS_BUCKET, TRAILS_BUCKET, TipBoard, TipRetired, TipStatus, TrailAction, TrailChange, TrailRating,
    TrailRequest, TrailRunIndex, TrailTip, WEBHOOKS_BUCKET, inbox_consumer, trail_run_key, trail_runs_index_key,
    trail_runs_reader,
};
use async_nats::jetstream::consumer::DeliverPolicy;

use super::*;

/// What `ant` stores under `key` of its own runs and has the stream send to `onto`, by a direct get naming it as its
/// reply subject; returns once the server handled the get.
async fn relay(ant: &Client, key: &str, payload: Vec<u8>, onto: &str) {
    let stored = format!("$KV.{TRAIL_RUNS_BUCKET}.{key}");
    let ack = jetstream::new(ant.clone()).publish(stored.clone(), payload.into()).await.unwrap();
    ack.await.unwrap();
    let get = format!("$JS.API.DIRECT.GET.KV_{TRAIL_RUNS_BUCKET}.{stored}");
    ant.publish_with_reply(get, onto.to_owned(), "".into()).await.unwrap();
    ant.flush().await.unwrap();
}

/// Returns once `stream` stored a message on `subject`, as a human gets it.
async fn stored(human: &Client, stream: &str, subject: &str) {
    let request = serde_json::json!({ "last_by_subj": subject }).to_string();
    let get = format!("$JS.API.DIRECT.GET.{stream}");
    crate::test_support::eventually(&format!("{stream} stores {subject}"), async || {
        human.request(get.clone(), request.clone().into()).await.unwrap().status.is_none()
    })
    .await;
}

/// What `ant` relays onto `key` of `bucket`, three messages; returns once the stream stored the last, as a human gets
/// it.
async fn relays_onto(ant: &Client, human: &Client, bucket: &str, key: &str) {
    let onto = format!("$KV.{bucket}.{key}");
    for n in 0..3 {
        relay(ant, &format!("dev-a.relay{n}"), format!("{{\"relay\":{n}}}").into_bytes(), &onto).await;
    }
    let request = serde_json::json!({ "last_by_subj": onto }).to_string();
    let get = format!("$JS.API.DIRECT.GET.KV_{bucket}");
    crate::test_support::eventually(&format!("the last relay onto {onto}"), async || {
        human.request(get.clone(), request.clone().into()).await.unwrap().payload.as_ref() == br#"{"relay":2}"#
    })
    .await;
}

fn trail(script: &str, saved_by: Saver) -> SavedTrail {
    SavedTrail {
        name: "deploy".parse().unwrap(),
        description: String::new(),
        phases: Vec::new(),
        waypoints: Vec::new(),
        script: script.to_owned(),
        saved_by,
        saved_from: None,
        saved_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn a_message_a_stream_relayed_onto_a_subject_an_ant_may_not_publish_is_refused_by_its_reader() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (h, h_js) = server.connect(&human).await;
    let (a, _) = server.connect(&dev_a).await;
    let (b, b_js) = server.connect(&dev_b).await;
    h_js.create_stream(ants_daemon::hub::trails_config()).await.unwrap();
    h_js.create_stream(ants_daemon::hub::trail_runs_config()).await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    bus::open_stream(&b_js, &dev_b).await.unwrap();
    let saved = DirectKv::whole(&h_js, None, TRAILS_BUCKET).await.unwrap();

    // A saved trail: neither `ants trail show` nor a conductor's daemon takes the relayed one as the human's.
    let forged = trail("agent('rm -rf')", Saver::Human { user: human.clone() });
    relay(&a, "dev-a.trail", serde_json::to_vec(&forged).unwrap(), &format!("$KV.{TRAILS_BUCKET}.deploy")).await;
    stored(&h, &format!("KV_{TRAILS_BUCKET}"), &format!("$KV.{TRAILS_BUCKET}.deploy")).await;
    let shown = crate::machines::latest_in(&h_js, TRAILS_BUCKET, &format!("$KV.{TRAILS_BUCKET}.deploy")).await;
    assert!(shown.unwrap().is_empty(), "a human's CLI skips it");
    let conductor = DirectKv::whole(&b_js, None, TRAILS_BUCKET).await.unwrap();
    assert!(conductor.get("deploy").await.is_err(), "a conductor's daemon refuses it");
    let genuine = trail("agent('build')", Saver::Human { user: human.clone() });
    saved.put("deploy", serde_json::to_vec(&genuine).unwrap()).await.unwrap();
    let read: SavedTrail = serde_json::from_slice(&conductor.get("deploy").await.unwrap().unwrap()).unwrap();
    assert_eq!(read, genuine, "a human's put reads as before");

    // Another conductor's run records: neither that conductor's daemon nor a human's CLI reads the relayed one.
    let run = RunId::generate();
    let theirs = trail_run_key(&dev_b, &run);
    relay(&a, "dev-a.run", b"{}".to_vec(), &format!("$KV.{TRAIL_RUNS_BUCKET}.{theirs}")).await;
    let stream = format!("KV_{TRAIL_RUNS_BUCKET}");
    stored(&h, &stream, &format!("$KV.{TRAIL_RUNS_BUCKET}.{theirs}")).await;
    let runs = DirectKv::new(&b_js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    assert!(runs.get(&theirs).await.is_err(), "dev-b's daemon refuses it");
    let filter = format!("$KV.{TRAIL_RUNS_BUCKET}.{theirs}");
    let listed = crate::machines::latest_in(&h_js, TRAIL_RUNS_BUCKET, &filter).await.unwrap();
    assert!(listed.is_empty(), "a human's CLI skips it");
    runs.put(&theirs, b"{\"genuine\":true}".to_vec()).await.unwrap();
    assert_eq!(runs.get(&theirs).await.unwrap().as_deref(), Some(&b"{\"genuine\":true}"[..]), "dev-b's own put");

    // A conductor's control subject: the request heard there is no human's.
    let mut requests = b.subscribe(subject::trail(&dev_b)).await.unwrap();
    b.flush().await.unwrap();
    let request = TrailRequest::new(dev_b.clone(), human.clone(), TrailAction::Stop { run: run.clone() });
    relay(&a, "dev-a.control", serde_json::to_vec(&request).unwrap(), &subject::trail(&dev_b)).await;
    let heard = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    assert!(relayed::heard_relayed(&heard), "the conductor drops it: {:?}", heard.headers);
    // The same through a consumer: dev-a's notes reader, taking its next message on that subject.
    let (a_js, notes) = (jetstream::new(a.clone()), notes_consumer(&dev_a));
    let note = subject::notes(&ThreadId::generate(), &dev_a);
    a_js.publish(note, serde_json::to_vec(&request).unwrap().into()).await.unwrap().await.unwrap();
    let config = serde_json::json!({ "stream_name": STREAM, "config": {
        "durable_name": notes, "filter_subject": subject::ALL_NOTES, "ack_policy": "none", "deliver_policy": "last",
    }});
    let create = format!("$JS.API.CONSUMER.CREATE.{STREAM}.{notes}.{}", subject::ALL_NOTES);
    a.request(create, config.to_string().into()).await.unwrap();
    let next = format!("$JS.API.CONSUMER.MSG.NEXT.{STREAM}.{notes}");
    a.publish_with_reply(next, subject::trail(&dev_b), r#"{"batch":1,"no_wait":true}"#.into()).await.unwrap();
    let heard = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    assert!(relayed::heard_relayed(&heard), "the conductor drops it: {:?}", heard.reply);
    h.publish(subject::trail(&dev_b), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
    let heard = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    assert!(!relayed::heard_relayed(&heard), "a human's request is heard as published");

    // The signer's requests: a relayed request to remove dev-b's trail changes nothing.
    let serving = tokio::spawn(keeper::serve(h.clone(), h_js.clone(), None));
    let theirs = trail("agent('build')", Saver::Ant { user: dev_b.clone() });
    saved.put("deploy", serde_json::to_vec(&theirs).unwrap()).await.unwrap();
    let seq = saved.lookup_last("deploy").await.unwrap().1;
    let removing = SaveTrailRequest::new(dev_b.clone(), TrailChange::Remove { name: theirs.name.clone(), last: seq });
    relay(&a, "dev-a.save", serde_json::to_vec(&removing).unwrap(), &subject::save_trail(&dev_b)).await;
    let renaming = SaveTrailRequest::new(
        dev_b.clone(),
        TrailChange::Save { trail: SavedTrail { name: "release".parse().unwrap(), ..theirs.clone() }, last: None },
    );
    b.publish(subject::save_trail(&dev_b), serde_json::to_vec(&renaming).unwrap().into()).await.unwrap();
    crate::test_support::eventually("the signer saves dev-b's own request", async || {
        saved.get("release").await.unwrap().is_some()
    })
    .await;
    assert_eq!(saved.lookup_last("deploy").await.unwrap().1, seq, "the relayed removal changed nothing");
    serving.abort();

    // An inbox: dev-b's live inbox and every read back of it skip the relayed message from the human.
    let mut live = bus::live_inbox(&b, &dev_b).await.unwrap();
    let relayed_message = Envelope::new(human.clone(), dev_b.clone(), "relayed");
    let onto = relayed_message.subject();
    relay(&a, "dev-a.inbox", serde_json::to_vec(&relayed_message).unwrap(), &onto).await;
    stored(&h, STREAM, &onto).await;
    let genuine = Envelope::new(human.clone(), dev_b.clone(), "genuine");
    bus::publish(&h_js, &genuine).await.unwrap();
    let first = timeout(WAIT, live.next()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<Envelope>(&first.payload).unwrap().id, genuine.id, "live");
    let inbox = subject::inbox_of(&dev_b);
    for (js, reader, reading) in
        [(&b_js, inbox_consumer(&dev_b), Reading::Spare), (&h_js, String::new(), Reading::Gets)]
    {
        let mut read = Vec::new();
        reading::read_back(js, &reader, reading, &inbox, DeliverPolicy::All, |_, _, _, payload| {
            read.push(serde_json::from_slice::<Envelope>(payload).unwrap().id);
            std::ops::ControlFlow::Continue(())
        })
        .await
        .unwrap();
        assert_eq!(read, [genuine.id], "read back by {reading:?}");
    }
}

#[tokio::test]
async fn an_entry_put_before_messages_a_stream_relayed_onto_its_key_is_still_read() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (h, h_js) = server.connect(&human).await;
    let (a, _) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;
    for config in
        [hub::trails_config(), hub::trail_runs_config(), hub::trail_tips_config(), hub::trail_ratings_config()]
    {
        h_js.create_stream(config).await.unwrap();
    }
    let latest = async |bucket: &str, filter: &str| {
        crate::machines::latest_in(&h_js, bucket, &format!("$KV.{bucket}.{filter}")).await.unwrap()
    };

    // A saved trail: a conductor's daemon and a human's CLI read the human's, and an ant's relay onto a name saved
    // none keeps no one from saving it.
    let saved = DirectKv::whole(&h_js, None, TRAILS_BUCKET).await.unwrap().past_relays();
    let genuine = serde_json::to_vec(&trail("agent('build')", Saver::Human { user: human.clone() })).unwrap();
    saved.put("deploy", genuine.clone()).await.unwrap();
    relays_onto(&a, &h, TRAILS_BUCKET, "deploy").await;
    // And the ack of a publish of the ant's onto it, which the server sends with no header.
    let deploy = format!("$KV.{TRAILS_BUCKET}.deploy");
    ack_onto(&a, &format!("$KV.{TRAIL_RUNS_BUCKET}.dev-a.x"), &deploy).await;
    let request = serde_json::json!({ "last_by_subj": deploy }).to_string();
    let get = format!("$JS.API.DIRECT.GET.KV_{TRAILS_BUCKET}");
    crate::test_support::eventually("the ack stored on the trail", async || {
        h.request(get.clone(), request.clone().into()).await.unwrap().payload.starts_with(br#"{"stream""#)
    })
    .await;
    let conductor = DirectKv::whole(&b_js, None, TRAILS_BUCKET).await.unwrap().past_relays();
    assert_eq!(conductor.get("deploy").await.unwrap(), Some(genuine.clone()), "a conductor's daemon");
    assert_eq!(latest(TRAILS_BUCKET, "deploy").await, [("deploy".to_owned(), genuine.clone())], "a human's CLI");
    let mut scanned = Vec::new();
    conductor.scan(">", |key, value| scanned.push((key.to_owned(), value.to_vec()))).await.unwrap();
    assert_eq!(scanned, [("deploy".to_owned(), genuine.clone())], "a conductor's listing");
    relays_onto(&a, &h, TRAILS_BUCKET, "release").await;
    assert_eq!(conductor.get("release").await.unwrap(), None, "relayed messages alone are no trail");
    assert!(saved.create("release", genuine.clone()).await.unwrap(), "the name is free to save");
    assert_eq!(conductor.get("release").await.unwrap(), Some(genuine.clone()));

    // A tip and a human's retirement of it: the tip stays retired.
    let tip = TrailTip {
        trail: "deploy".parse().unwrap(),
        id: "aaaaaaaa".parse().unwrap(),
        writer: dev_b.clone(),
        text: "pin the base image".to_owned(),
        tip_key: None,
        run: RunId::generate(),
        script: "s".to_owned(),
        written_at: chrono::Utc::now(),
        retired: None,
    };
    let tips = DirectKv::whole(&b_js, None, TRAIL_TIPS_BUCKET).await.unwrap().past_relays();
    tips.put(tip.key(), serde_json::to_vec(&tip).unwrap()).await.unwrap();
    let retired = TipRetired { by: human.clone(), at: chrono::Utc::now() };
    let human_tips = DirectKv::whole(&h_js, None, TRAIL_TIPS_BUCKET).await.unwrap();
    human_tips.put(tip.retired_key(), serde_json::to_vec(&retired).unwrap()).await.unwrap();
    relays_onto(&a, &h, TRAIL_TIPS_BUCKET, &tip.key()).await;
    relays_onto(&a, &h, TRAIL_TIPS_BUCKET, &tip.retired_key()).await;
    let status = |entries: Vec<(String, Vec<u8>)>| {
        let board = TipBoard::new(tip.trail.clone(), entries);
        board.tip(&tip.id, None, chrono::Utc::now(), None).map(|ranked| ranked.status).ok()
    };
    let by_human = TipStatus::Retired(Retirement::ByHuman { by: human.clone(), at: retired.at });
    assert_eq!(status(latest(TRAIL_TIPS_BUCKET, "deploy.>").await), Some(by_human.clone()), "a human's CLI");
    let mut scanned = Vec::new();
    tips.scan("deploy.>", |key, value| scanned.push((key.to_owned(), value.to_vec()))).await.unwrap();
    assert_eq!(status(scanned), Some(by_human), "a conductor's daemon");

    // A conductor's rating of its run.
    let rating = TrailRating {
        trail: "deploy".parse().unwrap(),
        conductor: dev_b.clone(),
        run: RunId::generate(),
        outcome: RatingOutcome::Worked,
        task: "ship".to_owned(),
        why: "it shipped".to_owned(),
        script: "s".to_owned(),
        at: chrono::Utc::now(),
        untrusted: false,
    };
    let ratings = DirectKv::whole(&b_js, None, TRAIL_RATINGS_BUCKET).await.unwrap().past_relays();
    let rated = serde_json::to_vec(&rating).unwrap();
    ratings.put(rating.key(), rated.clone()).await.unwrap();
    relays_onto(&a, &h, TRAIL_RATINGS_BUCKET, &rating.key()).await;
    assert_eq!(ratings.get(rating.key()).await.unwrap(), Some(rated.clone()), "a conductor's daemon");
    assert_eq!(latest(TRAIL_RATINGS_BUCKET, ">").await, [(rating.key(), rated)], "a human's CLI");

    // Another conductor's index of its runs: that conductor's daemon reads it through its own reader, and writes it
    // on.
    let index = trail_runs_index_key(&dev_b);
    let runs =
        DirectKv::new(&b_js, None, TRAIL_RUNS_BUCKET).await.unwrap().past_relays_through(trail_runs_reader(&dev_b));
    let listed = serde_json::to_vec(&TrailRunIndex { runs: vec![RunId::generate()] }).unwrap();
    runs.put(&index, listed.clone()).await.unwrap();
    relays_onto(&a, &h, TRAIL_RUNS_BUCKET, &index).await;
    let (found, last) = runs.lookup_last(&index).await.unwrap();
    assert!(matches!(found, Lookup::Value(value) if value == listed), "dev-b's daemon");
    assert_eq!(latest(TRAIL_RUNS_BUCKET, &index).await, [(index.clone(), listed)], "a human's CLI");
    let relisted = serde_json::to_vec(&TrailRunIndex::default()).unwrap();
    assert!(runs.update(&index, relisted.clone(), last).await.unwrap(), "its next write expects the relayed one");
    assert_eq!(runs.get(&index).await.unwrap(), Some(relisted));
}

/// What `client` has the server send onto `onto`: the ack of its publish under `own`, one of its own subjects, as
/// `{"stream":…,"seq":…}`; returns once the server handled the publish.
async fn ack_onto(client: &Client, own: &str, onto: &str) {
    client.publish_with_reply(own.to_owned(), onto.to_owned(), b"{}".to_vec().into()).await.unwrap();
    client.flush().await.unwrap();
}

#[tokio::test]
async fn an_answer_the_server_sent_onto_a_subject_an_ant_or_a_webhook_may_not_publish_is_refused_by_its_reader() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b, hook) = (UserId::human(), p("dev-a"), p("dev-b"), p("github"));
    let (h, h_js) = server.connect(&human).await;
    let (a, _) = server.connect(&dev_a).await;
    let (_, b_js) = server.connect(&dev_b).await;
    h_js.create_stream(ants_daemon::hub::trails_config()).await.unwrap();
    h_js.create_stream(ants_daemon::hub::trail_runs_config()).await.unwrap();
    crate::webhook::ensure_bucket(&h_js).await.unwrap();
    bus::ensure_stream(&h_js).await.unwrap();
    bus::open_stream(&b_js, &dev_b).await.unwrap();
    let creds = server.dir().join("github.creds");
    let permissions = Permissions::for_webhook(&hook, &[Destination::Ant(dev_b.clone())]);
    let kind = crate::credentials::revoke::Kind::Webhook;
    issue_with(&server.dir().join("auth"), &hook, kind, permissions, &creds).unwrap();
    let (w, _) = server.watched_with(&hook, &creds).await;
    let (ant_own, hook_own) = (format!("$KV.{TRAIL_RUNS_BUCKET}.dev-a.x"), format!("$KV.{WEBHOOKS_BUCKET}.github.x"));

    // Another conductor's index of its runs, by an ant: its daemon reads no index of no runs.
    let index = format!("$KV.{TRAIL_RUNS_BUCKET}.{}", trail_runs_index_key(&dev_b));
    ack_onto(&a, &ant_own, &index).await;
    stored(&h, &format!("KV_{TRAIL_RUNS_BUCKET}"), &index).await;
    let runs = DirectKv::new(&b_js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    assert!(runs.get(trail_runs_index_key(&dev_b)).await.is_err(), "dev-b's daemon refuses it");
    let listed = crate::machines::latest_in(&h_js, TRAIL_RUNS_BUCKET, &index).await.unwrap();
    assert!(listed.is_empty(), "a human's CLI skips it");

    // A saved trail, by a webhook: neither a conductor's daemon nor a human's CLI reads one.
    let saved = format!("$KV.{TRAILS_BUCKET}.deploy");
    ack_onto(&w, &hook_own, &saved).await;
    stored(&h, &format!("KV_{TRAILS_BUCKET}"), &saved).await;
    assert!(DirectKv::whole(&b_js, None, TRAILS_BUCKET).await.unwrap().get("deploy").await.is_err());
    assert!(crate::machines::latest_in(&h_js, TRAILS_BUCKET, &saved).await.unwrap().is_empty());

    // An inbox, by a webhook: no read back of dev-b's inbox takes the ack for a message from the human.
    let onto = Envelope::new(human.clone(), dev_b.clone(), "relayed").subject();
    ack_onto(&w, &hook_own, &onto).await;
    stored(&h, STREAM, &onto).await;
    let genuine = Envelope::new(human.clone(), dev_b.clone(), "genuine");
    bus::publish(&h_js, &genuine).await.unwrap();
    let inbox = subject::inbox_of(&dev_b);
    for (js, reader, reading) in
        [(&b_js, inbox_consumer(&dev_b), Reading::Spare), (&h_js, String::new(), Reading::Gets)]
    {
        let mut read = Vec::new();
        reading::read_back(js, &reader, reading, &inbox, DeliverPolicy::All, |_, _, _, payload| {
            read.push(serde_json::from_slice::<Envelope>(payload).map(|envelope| envelope.id).ok());
            std::ops::ControlFlow::Continue(())
        })
        .await
        .unwrap();
        assert_eq!(read, [Some(genuine.id)], "read back by {reading:?}");
    }
}
