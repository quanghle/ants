//! Messages a stream relayed: an ant stores a message under a key of its own and asks for it by a direct get naming a
//! reply subject it may not publish to, where the stream sends it with the headers the ant stored. Each reader of such
//! a subject refuses it.

use ants_daemon::bus::relayed;
use ants_daemon::stream::reading::{self, Reading};
use ants_daemon::trail::keeper;
use ants_proto::{
    RunId, SaveTrailRequest, SavedTrail, Saver, TRAIL_RUNS_BUCKET, TRAILS_BUCKET, TrailAction, TrailChange,
    TrailRequest, inbox_consumer, trail_run_key,
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
    h_js.create_key_value(ants_daemon::hub::trails_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::trail_runs_config()).await.unwrap();
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
