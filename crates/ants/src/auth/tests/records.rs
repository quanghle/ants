//! What ants and humans record in the team's buckets: colony entries, storage warnings, escalation chains,
//! links, attachments, reactions, saved trails and trail runs.

use super::*;

#[tokio::test]
async fn ants_write_their_own_colony_entry_and_everyone_reads_the_colony() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a) = (ants_test_support::human(), p("dev-a"));
    let ttl = ants_proto::COLONY_TTL;

    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_colony(&a_js, ttl).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&human).await;
    let h_kv = bus::ensure_colony(&h_js, ttl).await.unwrap();

    let a_kv = bus::ensure_colony(&a_js, ttl).await.unwrap();
    let entry = ColonyEntry { capabilities: vec!["rust".to_owned()], ..ants_test_support::colony_entry("dev-a", "m1") };
    let registration = Registration::start(a_kv.clone(), entry.clone(), ants_proto::COLONY_HEARTBEAT).await.unwrap();
    assert!(a_kv.put("dev-b", "spoof".into()).await.is_err(), "dev-a cannot list dev-b");
    // Ants read the colony, to route requests by capability.
    let (_b, b_js) = server.connect(&p("dev-b")).await;
    let b_kv = bus::ensure_colony(&b_js, ttl).await.unwrap();
    // dev-b may write its own key only, and an entry there naming dev-a is skipped.
    let forged = ColonyEntry { capabilities: vec!["deploy".to_owned()], ..entry.clone() };
    b_kv.put("dev-b", serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
    for kv in [&b_kv, &h_kv] {
        assert_eq!(ants_daemon::colony::list_entries(kv).await.unwrap(), std::slice::from_ref(&entry));
    }
    b_kv.delete("dev-b").await.unwrap();

    let listed: ColonyEntry = serde_json::from_slice(&h_kv.get("dev-a").await.unwrap().unwrap()).unwrap();
    assert_eq!(listed, entry);
    let keys: Vec<String> = h_kv.keys().await.unwrap().try_collect().await.unwrap();
    assert_eq!(keys, ["dev-a"]);
    let mut watch = h_kv.watch_with_history(">").await.unwrap();
    let seen = timeout(WAIT, watch.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(seen.key, "dev-a");
    assert!(h_kv.put("dev-a", "spoof".into()).await.is_err(), "the human does not write the colony");

    registration.deregister().await;
    assert!(h_kv.get("dev-a").await.unwrap().is_none());
}

#[tokio::test]
async fn ants_warn_a_human_once_per_crossing_of_a_storage_limit() {
    use ants_daemon::monitoring::storage::{self, Settings, Watcher};
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    bus::init_own_buckets(&h_js).await.unwrap();
    h_js.publish(subject::inbox(&dev_a, &human), vec![b'x'; 4096].into()).await.unwrap().await.unwrap();
    let used = storage::measure(&h_js, None, &Settings::default()).await.unwrap()[0].used;
    let watcher = async |ant: &UserId, max_file_store: u64| {
        let (_, js) = server.connect(ant).await;
        Watcher {
            notes: bus::ensure_sessions(&js, None, ant).await.unwrap(),
            js,
            hub_domain: None,
            ant: ant.clone(),
            human: human.clone(),
            settings: Settings { max_file_store: Some(max_file_store), ..Settings::default() },
            server: String::new(),
        }
    };
    let (a, b) = (watcher(&dev_a, used + used / 10).await, watcher(&dev_b, used + used / 10).await);
    let mut warnings = h_client.subscribe(subject::inbox_of(&human)).await.unwrap();
    subscribed(&h_js, &human).await;
    let note = ants_proto::storage_warning_key(None, "file");

    tokio::join!(a.check(), b.check());
    a.check().await;
    let got = timeout(WAIT, warnings.next()).await.unwrap().unwrap();
    let warning: Envelope = serde_json::from_slice(&got.payload).unwrap();
    assert_eq!(warning.escalation.map(|e| e.reason), Some(ants_proto::EscalationReason::Storage));
    assert!(warning.text.contains("JetStream file store of the hub") && warning.text.contains("Using most: "));
    assert!(timeout(Duration::from_millis(300), warnings.next()).await.is_err(), "one warning per crossing");
    assert!(a.notes.get(&note).await.unwrap().is_some());

    // Well under the share the note goes, and the next crossing warns again.
    watcher(&dev_a, used * 100).await.check().await;
    assert!(a.notes.get(&note).await.unwrap().is_none());
    b.check().await;
    timeout(WAIT, warnings.next()).await.expect("a warning of the next crossing");
}

#[tokio::test]
async fn ants_keep_their_own_escalation_chain_and_read_everyone_s() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, lead) = (ants_test_support::human(), p("dev-a"), p("lead"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_chains(&a_js).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&human).await;
    let h_kv = bus::ensure_chains(&h_js).await.unwrap();

    let (_l, l_js) = server.connect(&lead).await;
    ants_daemon::records::chains::publish(&l_js, &lead, std::slice::from_ref(&dev_a)).await.unwrap();
    let a_kv = bus::ensure_chains(&a_js).await.unwrap();
    assert_eq!(ants_daemon::records::chains::read(&a_kv, &lead).await.unwrap(), Some(vec![dev_a.clone()]));
    assert_eq!(ants_daemon::records::chains::read(&h_kv, &lead).await.unwrap(), Some(vec![dev_a.clone()]));

    // dev-a writes its own key only, and an entry there naming lead is skipped.
    assert!(a_kv.put("lead", "spoof".into()).await.is_err(), "dev-a cannot rewrite lead's chain");
    let forged = ants_proto::ChainEntry::new(lead.clone(), vec![human.clone()]);
    a_kv.put("dev-a", serde_json::to_vec(&forged).unwrap().into()).await.unwrap();
    assert_eq!(ants_daemon::records::chains::read(&h_kv, &dev_a).await.unwrap(), None);
    assert!(h_kv.put("lead", "spoof".into()).await.is_err(), "the human does not write chains");
}

#[tokio::test]
async fn ants_record_the_links_they_pass_on_and_read_everyone_s() {
    let Some(server) = Server::start() else { return };
    let (dev_a, dev_b, lead) = (p("dev-a"), p("dev-b"), p("lead"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(bus::ensure_links(&a_js).await.is_err(), "only the human creates the bucket");
    let (_h, h_js) = server.connect(&ants_test_support::human()).await;
    let h_kv = bus::ensure_links(&h_js).await.unwrap();

    let origin = ThreadId::generate();
    let spin = |from: &UserId, to: &UserId| {
        Envelope::on_thread(origin.clone(), from.clone(), to.clone(), "")
            .with_creator(Some(lead.clone()))
            .spun_off(ThreadId::generate())
    };
    let spun = spin(&dev_a, &dev_b);
    ants_daemon::records::links::pass_on(&a_js, &spun).await.unwrap();
    let a_kv = a_js.get_key_value(ants_proto::LINKS_BUCKET).await.unwrap();
    for forged in [link_key(&lead, &origin, &dev_b), link_key(&dev_b, &origin, &dev_a)] {
        assert!(a_kv.put(forged, "spoof".into()).await.is_err(), "dev-a records its own links only");
    }
    assert!(h_kv.put(link_key(&lead, &origin, &dev_a), "spoof".into()).await.is_err(), "the human records none");

    let (_b, b_js) = server.connect(&dev_b).await;
    for js in [&a_js, &b_js, &h_js] {
        assert_eq!(
            ants_daemon::records::links::passed_on(js, &origin).await.unwrap(),
            [(dev_a.clone(), dev_b.clone())]
        );
    }
    assert!(ants_daemon::records::links::passed_on(&b_js, &ThreadId::generate()).await.unwrap().is_empty());
    let passed = ants_daemon::records::links::Passed { from: dev_a, to: dev_b, on: Some(spun.thread_id) };
    assert_eq!(
        ants_daemon::records::links::passed(&h_js, &origin).await.unwrap(),
        [passed],
        "with the thread it went on"
    );
}

#[tokio::test]
async fn senders_store_their_own_attachments_and_ants_read_those_of_the_messages_they_read() {
    use ants_daemon::files;
    use ants_proto::{AttachmentHandle, AttachmentScope};

    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b, dev_c) = (ants_test_support::human(), p("dev-a"), p("dev-b"), p("dev-c"));
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(a_js.create_stream(ants_daemon::hub::files_config()).await.is_err(), "only a human creates it");
    let (_h, h_js) = server.connect(&human).await;
    h_js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
    let (_b, b_js) = server.connect(&dev_b).await;
    let (_c, c_js) = server.connect(&dev_c).await;

    let store = async |js: &jetstream::Context, from: &UserId, scope: AttachmentScope| {
        let attachment = files::reference("report.txt", "text/plain", b"findings").unwrap();
        let handle = AttachmentHandle { from: from.clone(), scope, id: attachment.id };
        files::put(js, &handle, &attachment, b"findings").await.unwrap();
        handle
    };
    let to_b = store(&a_js, &dev_a, AttachmentScope::Inbox(dev_b.clone())).await;
    let notes = store(&a_js, &dev_a, AttachmentScope::Notes(ThreadId::generate())).await;
    let channel: ChannelId = "ops".parse().unwrap();
    let chan = store(&b_js, &dev_b, AttachmentScope::Chan(channel)).await;
    let from_human = store(&h_js, &human, AttachmentScope::Inbox(dev_a.clone())).await;
    for (js, handle) in [(&b_js, &to_b), (&a_js, &to_b), (&h_js, &to_b), (&c_js, &notes), (&c_js, &chan)] {
        assert_eq!(files::get(js, handle, None).await.unwrap().1, b"findings", "{handle}");
    }
    assert_eq!(files::get(&a_js, &from_human, None).await.unwrap().1, b"findings");
    assert!(files::get(&c_js, &to_b, None).await.is_err(), "dev-c reads no messages between dev-a and dev-b");

    let (raw, mut errors) = server.watched(&dev_c).await;
    let theirs = format!("$JS.API.DIRECT.GET.{}.{}", ants_proto::FILES_STREAM, to_b.chunk_subject(0));
    raw.publish(theirs.clone(), "".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&theirs), "reading others' attachments is denied");
    let forged = AttachmentHandle { from: dev_a.clone(), ..to_b.clone() }.chunk_subject(1);
    raw.publish(forged.clone(), "x".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&forged), "storing as another sender is denied");
}

#[tokio::test]
async fn humans_write_only_their_own_reactions_and_ants_change_theirs_through_ants_signer() {
    use ants_daemon::records::reactions::{self, Answering, Reacted};
    use ants_proto::{ReactionChange, ReactionsOutcome, ReactionsRequest};

    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (eyes, check): (ants_proto::Reaction, ants_proto::Reaction) =
        ("eyes".parse().unwrap(), "check".parse().unwrap());
    let (a_client, a_js) = server.connect(&dev_a).await;
    assert!(a_js.create_stream(ants_daemon::hub::reactions_config()).await.is_err(), "only a human creates it");
    let (h_client, h_js) = server.connect(&human).await;
    bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_stream(ants_daemon::hub::reactions_config()).await.unwrap();
    let (to_a, between) = (
        Envelope::new(human.clone(), dev_a.clone(), "for dev-a"),
        Envelope::new(human.clone(), dev_b.clone(), "for dev-b"),
    );
    for envelope in [&to_a, &between] {
        bus::publish(&h_js, envelope).await.unwrap();
    }
    let message = to_a.id;
    let mut heard = h_client.subscribe(subject::ALL).await.unwrap();
    h_client.flush().await.unwrap();

    assert!(reactions::add(&h_js, None, &human, &message, &eyes).await.unwrap());
    assert!(reactions::add(&h_js, None, &dev_a, &message, &check).await.is_err(), "a human reacts as nobody else");
    // dev-a neither writes reactions, not even its own, nor reads them.
    assert!(reactions::add(&a_js, None, &dev_a, &message, &eyes).await.is_err());
    assert!(reactions::load(&a_js, &[message]).await.is_err(), "an ant reads no reactions");
    let (raw, mut errors) = server.watched(&dev_a).await;
    for key in [format!("dev-a.{message}.x"), format!("dev-b.{message}.x")] {
        let forged = format!("$KV.ants_reactions.{key}");
        raw.publish(forged.clone(), "{}".into()).await.unwrap();
        assert!(next_violation(&raw, &mut errors).await.contains(&forged), "a direct reaction is denied");
    }
    let theirs = format!("$KV.ants_reactions.{human}.{message}.eyes");
    let mut delete = async_nats::HeaderMap::new();
    delete.insert("KV-Operation", "DEL");
    raw.publish_with_headers(theirs.clone(), delete, "".into()).await.unwrap();
    assert!(next_violation(&raw, &mut errors).await.contains(&theirs), "removing another's reaction is denied");
    let quiet = timeout(Duration::from_millis(300), heard.next()).await;
    assert!(quiet.is_err(), "a reaction is no message: {quiet:?}");

    // `ants signer`'s credentials answer an ant's requests, for the messages it reads only.
    let approver =
        crate::credentials::approver::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, WAIT)
            .await
            .unwrap();
    let serving = tokio::spawn(reactions::serve(
        approver.client.clone(),
        approver.hub().clone(),
        None,
        Answering::ListsAndChanges,
    ));
    let change = |message, reaction: &ants_proto::Reaction, remove| {
        ReactionsRequest::change(dev_a.clone(), message, ReactionChange::new(reaction.clone(), remove))
    };
    let applied = |changed| Some(ReactionsOutcome::Applied { changed });
    assert_eq!(reactions::request(&a_client, &change(message, &eyes, false)).await.unwrap(), applied(true));
    assert_eq!(reactions::request(&a_client, &change(message, &check, false)).await.unwrap(), applied(true));
    let unread = reactions::request(&a_client, &change(between.id, &eyes, false)).await.unwrap();
    assert_eq!(unread, Some(ReactionsOutcome::Unknown), "dev-a reads no message to dev-b");
    let expected = vec![
        Reacted::new(check.clone(), vec![dev_a.clone()]),
        Reacted::new(eyes.clone(), vec![dev_a.clone(), human.clone()]),
    ];
    let listed = reactions::ask(&a_client, &dev_a, message).await.unwrap();
    assert_eq!(listed, Some(ReactionsOutcome::Listed { reactions: expected.clone() }));
    assert_eq!(reactions::load(&h_js, &[message]).await.unwrap()[&message], expected);
    assert!(!reactions::load(&h_js, &[between.id]).await.unwrap().contains_key(&between.id));
    assert_eq!(reactions::request(&a_client, &change(message, &eyes, true)).await.unwrap(), applied(true));
    assert_eq!(reactions::request(&a_client, &change(message, &eyes, true)).await.unwrap(), applied(false));
    let all = reactions::load(&h_js, &[message]).await.unwrap();
    assert_eq!(all[&message][1], Reacted::new(eyes, vec![human]));
    serving.abort();
}

/// `ants signer` reads every run's records, to check the run an ant names as a trail's source, and writes none.
#[tokio::test]
async fn ants_signer_reads_the_runs_ants_save_trails_from() {
    use ants_proto::{RunId, TRAIL_RUNS_BUCKET, trail_session_key};
    let Some(server) = Server::start() else { return };
    let dev_a = p("dev-a");
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_h, h_js) = server.connect(&ants_test_support::human()).await;
    h_js.create_stream(ants_daemon::hub::trail_runs_config()).await.unwrap();
    let key = trail_session_key(&dev_a, &RunId::generate());
    DirectKv::new(&a_js, None, TRAIL_RUNS_BUCKET).await.unwrap().put(&key, b"{}".to_vec()).await.unwrap();
    let approver =
        crate::credentials::approver::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, WAIT)
            .await
            .unwrap();
    let keeper = DirectKv::whole(approver.hub(), None, TRAIL_RUNS_BUCKET).await.unwrap().past_relays();
    assert_eq!(keeper.get(&key).await.unwrap().as_deref(), Some(&b"{}"[..]));
    assert!(keeper.put(&key, b"{}".to_vec()).await.is_err(), "the signer writes no run");
}

#[tokio::test]
async fn humans_save_the_trails_and_each_ant_keeps_only_the_runs_it_conducts() {
    use ants_proto::{
        RunId, SavedTrail, Saver, TRAIL_RATINGS_BUCKET, TRAIL_RUNS_BUCKET, TRAIL_TIPS_BUCKET, TRAILS_BUCKET, ThreadId,
        TrailStepThread, trail_rating_key, trail_run_key, trail_session_key, trail_thread_step_key,
        trail_thread_steps_prefix, trail_tip_key, trail_tip_retired_key, trail_tip_vote_key,
    };
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_h, h_js) = server.connect(&human).await;
    let configs = [
        ants_daemon::hub::trails_config(),
        ants_daemon::hub::trail_runs_config(),
        ants_daemon::hub::trail_tips_config(),
        ants_daemon::hub::trail_ratings_config(),
    ];
    for config in configs {
        assert!(a_js.create_stream(config.clone()).await.is_err(), "only a human creates {}", config.name);
        h_js.create_stream(config).await.unwrap();
    }

    let trail = SavedTrail {
        name: "deploy".parse().unwrap(),
        description: "Ship it".to_owned(),
        phases: vec!["build".to_owned()],
        waypoints: Vec::new(),
        script: "agent('build')".to_owned(),
        saved_by: Saver::Human { user: human.clone() },
        saved_from: None,
        saved_at: chrono::Utc::now(),
    };
    let saved = DirectKv::whole(&h_js, None, TRAILS_BUCKET).await.unwrap();
    saved.put(trail.key(), serde_json::to_vec(&trail).unwrap()).await.unwrap();
    let read = DirectKv::whole(&a_js, None, TRAILS_BUCKET).await.unwrap();
    let got: SavedTrail = serde_json::from_slice(&read.get("deploy").await.unwrap().unwrap()).unwrap();
    assert_eq!(got, trail);
    let theirs = SavedTrail { saved_by: Saver::Ant { user: dev_a.clone() }, ..trail.clone() };
    assert!(read.put("release", serde_json::to_vec(&theirs).unwrap()).await.is_err(), "an ant's credentials save none");
    assert!(read.delete("deploy").await.is_err(), "an ant's credentials delete none");
    saved.delete("deploy").await.unwrap();
    assert!(read.get("deploy").await.unwrap().is_none(), "a human deletes any");

    let run = RunId::generate();
    let runs = DirectKv::new(&a_js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    runs.put(trail_session_key(&dev_a, &run), b"{}".to_vec()).await.unwrap();
    assert_eq!(runs.get(trail_session_key(&dev_a, &run)).await.unwrap().as_deref(), Some(&b"{}"[..]));
    assert!(runs.put(trail_run_key(&dev_b, &run), b"{}".to_vec()).await.is_err(), "dev-a writes no run of dev-b's");
    let (_b, b_js) = server.connect(&dev_b).await;
    let theirs = DirectKv::new(&b_js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    assert!(theirs.get(trail_session_key(&dev_a, &run)).await.is_err(), "dev-b reads none of dev-a's");
    let all = DirectKv::whole(&h_js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    assert_eq!(all.get(trail_session_key(&dev_a, &run)).await.unwrap().as_deref(), Some(&b"{}"[..]));
    assert!(all.put(trail_session_key(&dev_a, &run), b"{}".to_vec()).await.is_err(), "a human writes no run");

    let (thread, other, step) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let marker = |run_thread: &ThreadId| {
        let (run, to, run_thread) = (run.clone(), dev_b.clone(), run_thread.clone());
        serde_json::to_vec(&TrailStepThread { run, call: 1, to, run_thread, label: None }).unwrap()
    };
    let key = trail_thread_step_key(&dev_a, &thread, &step);
    runs.put(&key, marker(&thread)).await.unwrap();
    runs.put(trail_thread_step_key(&dev_a, &other, &ThreadId::generate()), marker(&other)).await.unwrap();
    let foreign = trail_thread_step_key(&dev_b, &thread, &step);
    assert!(runs.put(&foreign, marker(&thread)).await.is_err(), "dev-a keeps no step under dev-b's runs");
    assert!(theirs.get(&key).await.is_err(), "dev-b reads none of dev-a's steps");
    let filter = format!("$KV.{TRAIL_RUNS_BUCKET}.{}*", trail_thread_steps_prefix(&dev_a, &thread));
    let read = crate::buckets::latest_in(&h_js, TRAIL_RUNS_BUCKET, &filter).await.unwrap();
    assert_eq!(read, [(key.clone(), marker(&thread))], "a human reads the steps on one thread alone");
    assert!(all.put(&key, marker(&thread)).await.is_err(), "a human keeps no step");

    let (deploy, tip) = ("deploy".parse().unwrap(), ants_proto::TipId::generate());
    let tips = DirectKv::whole(&a_js, None, TRAIL_TIPS_BUCKET).await.unwrap();
    assert!(tips.create(trail_tip_key(&deploy, &dev_a, &tip), b"{}".to_vec()).await.unwrap());
    tips.put(trail_tip_vote_key(&deploy, &dev_b, &tip, &dev_a), b"{}".to_vec()).await.unwrap();
    let forged = [
        trail_tip_key(&deploy, &dev_b, &tip),
        trail_tip_vote_key(&deploy, &dev_a, &tip, &dev_b),
        trail_tip_retired_key(&deploy, &dev_a, &tip),
    ];
    for key in forged {
        assert!(tips.put(&key, b"{}".to_vec()).await.is_err(), "dev-a writes no tip or vote of dev-b's: {key}");
    }
    let filter = format!("$KV.{TRAIL_TIPS_BUCKET}.deploy.>");
    assert_eq!(crate::buckets::latest_in(&b_js, TRAIL_TIPS_BUCKET, &filter).await.unwrap().len(), 2, "ants read all");
    let stream = h_js.get_stream(format!("KV_{TRAIL_TIPS_BUCKET}")).await.unwrap();
    stream.purge().filter(&filter).await.unwrap();
    assert!(crate::buckets::latest_in(&b_js, TRAIL_TIPS_BUCKET, &filter).await.unwrap().is_empty(), "a human purges");

    let ratings = DirectKv::whole(&a_js, None, TRAIL_RATINGS_BUCKET).await.unwrap();
    ratings.put(trail_rating_key(&deploy, &dev_a, &run), b"{}".to_vec()).await.unwrap();
    let forged = trail_rating_key(&deploy, &dev_b, &run);
    assert!(ratings.put(&forged, b"{}".to_vec()).await.is_err(), "dev-a rates no run of dev-b's");
    let as_human = DirectKv::whole(&h_js, None, TRAIL_RATINGS_BUCKET).await.unwrap();
    assert!(as_human.put(&forged, b"{}".to_vec()).await.is_err(), "a human writes no rating");
    let filter = format!("$KV.{TRAIL_RATINGS_BUCKET}.deploy.>");
    assert_eq!(
        crate::buckets::latest_in(&b_js, TRAIL_RATINGS_BUCKET, &filter).await.unwrap().len(),
        1,
        "ants read all"
    );
    let stream = h_js.get_stream(format!("KV_{TRAIL_RATINGS_BUCKET}")).await.unwrap();
    stream.purge().filter(&filter).await.unwrap();
    assert!(
        crate::buckets::latest_in(&b_js, TRAIL_RATINGS_BUCKET, &filter).await.unwrap().is_empty(),
        "a human purges"
    );
}

/// One message with a `Nats-Rollup` header to a key an ant may write would remove every other entry of the bucket:
/// the trail buckets `ants init` creates refuse it, under an ant's rights or a human's, and keep the entries of others.
#[tokio::test]
async fn a_rollup_under_an_ant_s_rights_leaves_the_trail_entries_of_others() {
    use ants_proto::{
        RunId, TRAIL_RATINGS_BUCKET, TRAIL_RUNS_BUCKET, TRAIL_TIPS_BUCKET, TRAILS_BUCKET, TipId, trail_rating_key,
        trail_session_key, trail_tip_key,
    };
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (ants_test_support::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    let rights = crate::people::humans::Rights::from_creds(Some(&server.creds(&human))).unwrap();
    ants_daemon::hub::init(&h_js, |s| rights.may_publish(s)).await.unwrap();
    let (_a, a_js) = server.connect(&dev_a).await;
    let (_b, b_js) = server.connect(&dev_b).await;

    let deploy = "deploy".parse().unwrap();
    let (run, tip) = (RunId::generate(), TipId::generate());
    let (rating_a, rating_b) = (trail_rating_key(&deploy, &dev_a, &run), trail_rating_key(&deploy, &dev_b, &run));
    let keys = [
        (TRAIL_RUNS_BUCKET, trail_session_key(&dev_a, &run), trail_session_key(&dev_b, &run)),
        (TRAIL_TIPS_BUCKET, trail_tip_key(&deploy, &dev_a, &tip), trail_tip_key(&deploy, &dev_b, &tip)),
        (TRAIL_RATINGS_BUCKET, rating_a, rating_b),
    ];
    let rollup = async |js: &jetstream::Context, subject: String| {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Rollup", "all");
        js.publish_with_headers(subject, headers, "{}".into()).await.unwrap().await
    };
    for (bucket, own, others) in keys {
        js_put(&b_js, bucket, &others).await;
        js_put(&a_js, bucket, &own).await;
        assert!(rollup(&a_js, format!("$KV.{bucket}.{own}")).await.is_err(), "{bucket} refuses dev-a's rollup");
        let stream = h_js.get_stream(format!("KV_{bucket}")).await.unwrap();
        assert_eq!(stream.cached_info().state.messages, 2, "{bucket} keeps dev-b's entry and dev-a's");
    }
    let saved = DirectKv::whole(&h_js, None, TRAILS_BUCKET).await.unwrap();
    saved.put("deploy", b"{}".to_vec()).await.unwrap();
    saved.put("release", b"{}".to_vec()).await.unwrap();
    assert!(rollup(&h_js, format!("$KV.{TRAILS_BUCKET}.deploy")).await.is_err(), "a saved trail's rollup is refused");
    assert!(saved.get("release").await.unwrap().is_some(), "the other saved trail stays");
}

/// Put `{}` under `key` of `bucket` as the user of `js`, by plain KV subject.
async fn js_put(js: &jetstream::Context, bucket: &str, key: &str) {
    js.publish(format!("$KV.{bucket}.{key}"), "{}".into()).await.unwrap().await.unwrap();
}
