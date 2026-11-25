//! Delivering answers: the title and files a turn attached, what the runs started answering a step spent, a webhook's
//! answer, replies on channel threads, and what a review's thread spent noted with the answer in the sage's sense.

use std::collections::BTreeMap;

use super::*;

#[tokio::test]
async fn the_reply_carries_the_title_suggested_during_its_turn() {
    use agent_client_protocol::v1::schema::McpServer;
    use rmcp::ServiceExt as _;
    use rmcp::model::{CallToolRequestParams, ClientConfig};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("title");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let (titled, plain) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&titled, "hold on").await;

    let suggest = async |thread: &ThreadId, title: &str| {
        let servers = bus.mcp.as_ref().unwrap().servers()(&crate::harness::McpSession::test(
            thread,
            std::path::Path::new("/"),
            true,
        ));
        let [McpServer::Http(server)] = &servers[..] else { panic!("one HTTP server") };
        let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
        let config = StreamableHttpClientTransportConfig::with_uri(server.url.as_str()).auth_header(token);
        let client = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap();
        let serde_json::Value::Object(args) = serde_json::json!({"title": title}) else { unreachable!() };
        let call = CallToolRequestParams::new("suggest_thread_title").with_arguments(args);
        let result = client.call_tool(call).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
    };
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        suggest(&titled, "  Fix the build ").await;
        gate.add_permits(1);
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!((&reply.thread_id, reply.title.as_deref()), (&titled, Some("Fix the build")));

        bus.send(&titled, "again").await;
        bus.send(&plain, "plain").await;
        for _ in 0..2 {
            let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
            let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
            assert_eq!(reply.title, None, "the suggestion went with its turn's reply");
        }

        // A reply posted to a channel thread carries it too.
        let ask = ChannelPost::new(ops.clone(), ants_test_support::human(), "hold: @dev-a the deploy failed");
        bus.post(&ask).await;
        until("the post to be prompted", || log.locked().prompted == 4).await;
        suggest(&ask.thread_id, "Fix the deploy").await;
        gate.add_permits(1);
        let reply = next_post(&mut posts).await;
        assert_eq!((&reply.thread_id, reply.title.as_deref()), (&ask.thread_id, Some("Fix the deploy")));

        // And so does a member's answer posted to the thread notes of an escalation.
        let [carol, lead] = ants(["carol", "lead"]);
        let member = ThreadId::generate();
        let escalation = escalated(&carol, &member, &[&[lead, bus.dev.clone()]]);
        let help =
            Envelope::on_thread(member.clone(), carol, bus.dev.clone(), "hold: help").with_escalation(Some(escalation));
        bus::publish_json(&bus.js, help.subject(), &help, None).await.unwrap();
        until("the member to be prompted", || log.locked().prompted == 5).await;
        suggest(&member, "Unstick carol").await;
        gate.add_permits(1);
        let posted = timeout(WAIT, async {
            loop {
                if let Some(post) = read_notes(&bus, &member).await.pop() {
                    break post;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the member's answer in the thread notes");
        assert_eq!((&posted.from, posted.title.as_deref()), (&bus.dev, Some("Unstick carol")));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn the_answer_carries_the_files_attached_during_its_turn() {
    use agent_client_protocol::v1::schema::McpServer;
    use rmcp::ServiceExt as _;
    use rmcp::model::{CallToolRequestParams, ClientConfig};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

    let Some((mut bus, mut replies)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::files_config()).await.unwrap();
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    // The server's directory is this run's own: test runs at once delete and attach each other's files otherwise.
    let home = bus._server.dir().join("reply-files");
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("report.md"), "# done").unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await.unwrap();
    mcp.with_files(crate::mcp::jetstream_files(bus.js.clone()), home.clone());
    bus.mcp = Some(mcp);
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let thread = ThreadId::generate();
    bus.send(&thread, "hold on").await;

    let attach_to_reply = async |thread: &ThreadId, files: &[&str]| {
        let servers = bus.mcp.as_ref().unwrap().servers()(&crate::harness::McpSession::test(thread, &work, true));
        let [McpServer::Http(server)] = &servers[..] else { panic!("one HTTP server") };
        let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
        let config = StreamableHttpClientTransportConfig::with_uri(server.url.as_str()).auth_header(token);
        let client = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await.unwrap();
        let serde_json::Value::Object(args) = serde_json::json!({"attach": files}) else { unreachable!() };
        let result = client.call_tool(CallToolRequestParams::new("attach_to_reply").with_arguments(args)).await;
        assert_ne!(result.unwrap().is_error, Some(true));
    };
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the prompt to run", || log.locked().prompted == 1).await;
        attach_to_reply(&thread, &["report.md"]).await;
        gate.add_permits(1);
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        let [report] = &reply.attachments[..] else { panic!("{reply:?}") };
        assert_eq!((report.name.as_str(), report.mime.as_str()), ("report.md", "text/markdown"));
        let (_, bytes) = crate::files::get(&bus.js, &reply.attachment_handle(report.id), Some(report)).await.unwrap();
        assert_eq!(bytes, b"# done");

        // A channel post carries them too; one gone by the end of the turn leaves them out, the answer saying why.
        let ask = ChannelPost::new(ops.clone(), ants_test_support::human(), "hold: @dev-a the report?");
        bus.post(&ask).await;
        until("the post to be prompted", || log.locked().prompted == 2).await;
        attach_to_reply(&ask.thread_id, &["report.md"]).await;
        gate.add_permits(1);
        let post = next_post(&mut posts).await;
        let [report] = &post.attachments[..] else { panic!("{post:?}") };
        let (_, bytes) = crate::files::get(&bus.js, &post.attachment_handle(report.id), Some(report)).await.unwrap();
        assert_eq!(bytes, b"# done");

        bus.send(&thread, "hold again").await;
        until("the next prompt", || log.locked().prompted == 3).await;
        attach_to_reply(&thread, &["report.md"]).await;
        std::fs::remove_file(work.join("report.md")).unwrap();
        gate.add_permits(1);
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let reply: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        assert!(reply.attachments.is_empty());
        let left_out = "\n\n[ants] The files attached to this answer are left out: cannot attach \"report.md\"";
        assert!(reply.text.contains(left_out), "{}", reply.text);
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_mentioned_member_answers_on_the_thread_after_the_posts_it_missed() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let root = ChannelPost::new(ops.clone(), ants_test_support::human(), "deploy friday?");
    bus.post(&root).await;
    let ask = root.reply("dev-b".parse().unwrap(), "@dev-a agree?", &[], &Humans::default()).unwrap();
    let ask_seq = bus.post(&ask).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_post(&mut replies).await;
        assert_eq!((&reply.channel, &reply.thread_id, reply.in_reply_to), (&ops, &root.thread_id, Some(ask.id)));
        assert_eq!(reply.turn_budget, DEFAULT_TURN_BUDGET - 2, "the ask spent one turn, the reply another");
        bus.until_posts_acked(ask_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let first = log.locked().answered.clone();
    assert_eq!(first.len(), 1, "the post that mentions nobody is not prompted: {first:?}");
    assert!(first[0].contains("human: deploy friday?\n") && first[0].ends_with("nobody."), "{}", first[0]);
    assert!(first[0].contains("Post from dev-b mentioning dev-a:\n@dev-a agree?"), "{}", first[0]);

    // After a restart, only what was posted since shows, without the ant's own reply. Owning the channel now,
    // the ant is told so.
    let mut owned: ChannelEntry = serde_json::from_slice(&bus.channels.get("ops").await.unwrap().unwrap()).unwrap();
    owned.owner = Some(bus.dev.clone());
    bus.channels.put(owned.key(), serde_json::to_vec(&owned).unwrap().into()).await.unwrap();
    let more = root.reply(ants_test_support::human(), "rollback plan is ready", &[], &Humans::default()).unwrap();
    bus.post(&more).await;
    let again = root.reply(ants_test_support::human(), "@dev-a and now?", &[], &Humans::default()).unwrap();
    let again_seq = bus.post(&again).await;
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_post(&mut replies).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(again.id), DEFAULT_TURN_BUDGET - 1));
        bus.until_posts_acked(again_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let second = log.locked().answered[1].clone();
    let shown = "Earlier posts on the thread not shown before, oldest first:\nhuman: rollback plan is ready\n\n";
    assert!(second.contains(shown), "{second}");
    assert!(!second.contains("deploy friday") && !second.contains("dev-a: "), "{second}");
    assert!(second.ends_with(&format!("\n\n{}", channel::owner_note(&ops))), "{second}");
}

#[tokio::test]
async fn a_member_s_session_on_a_channel_thread_starts_with_the_channel_s_description() {
    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a"]).await;
    let mut entry: ChannelEntry = serde_json::from_slice(&bus.channels.get("ops").await.unwrap().unwrap()).unwrap();
    entry.set_description(Some("Incidents and deploys of the shop.")).unwrap();
    bus.channels.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let root = ChannelPost::new(ops.clone(), ants_test_support::human(), "@dev-a deploy friday?");
    bus.post(&root).await;
    let again = root.reply(ants_test_support::human(), "@dev-a and now?", &[], &Humans::default()).unwrap();
    let again_seq = bus.post(&again).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        next_post(&mut replies).await;
        next_post(&mut replies).await;
        bus.until_posts_acked(again_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    let note = channel::description_note(&ops, "Incidents and deploys of the shop.");
    assert_eq!(
        note,
        "[ants] #ops, the channel of this thread, is described by a human as:\nIncidents and deploys of the shop."
    );
    assert!(answered[0].starts_with(&format!("{note}\n\n")), "{}", answered[0]);
    assert!(!answered[1].contains("described by a human"), "only the first prompt of a session: {}", answered[1]);
}

#[tokio::test]
async fn an_answer_to_a_webhook_goes_to_the_thread_s_owner_or_nowhere() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (hook, human) = ("github".parse::<UserId>().unwrap(), ants_test_support::human());
    let mut to_hook = bus.client.subscribe(subject::inbox_of(&hook)).await.unwrap();
    bus.client.flush().await.unwrap();
    let owned = Envelope::new(hook.clone(), bus.dev.clone(), "build red").with_owner(Some(human.clone()));
    let owned = owned.from_webhook();
    bus::publish_json(&bus.js, owned.subject(), &owned, None).await.unwrap();
    let unowned = Envelope::new(hook.clone(), bus.dev.clone(), "build green").from_webhook();
    let seq = bus::publish_json(&bus.js, unowned.subject(), &unowned, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_human).await;
        assert_eq!((&reply.from, &reply.to, reply.in_reply_to), (&bus.dev, &human, Some(owned.id)));
        assert_eq!(reply.turn_budget, owned.turn_budget, "an answer to a human is free");
        assert_eq!(reply.allow, Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()), "the thread is narrowed");
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    // Different threads, so answered in either order.
    let answered = log.locked().answered.clone();
    let has = |text: &str| answered.iter().any(|prompt| prompt.contains(text));
    assert!(answered.len() == 2 && has("build red") && has("build green"), "{answered:?}");
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_human.next()).await.is_err(), "the unowned answer goes nowhere");
    assert!(timeout(quiet, to_hook.next()).await.is_err(), "nothing goes to the webhook's inbox");
    let kept = bus.ledger.narrowed(&unowned.thread_id, None).await.unwrap();
    assert_eq!(kept, Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()), "until a human messages the ant on it");
}

/// A webhook's own tool rules, leaving out rate_trail, keep narrowing its thread, yet the turn of the conductor's
/// invitation to rate the run reporting there lets rate_trail through; a message only claiming to be one does not.
#[cfg(unix)]
#[tokio::test]
async fn a_run_s_rating_invitation_rates_it_under_a_webhook_s_own_rules() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let trails = bus.conduct().await;
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch.with_policy(PermissionPolicy::allow_all()), SessionStore::memory(), 2, IDLE);
    let (hook, human) = ("github".parse::<UserId>().unwrap(), ants_test_support::human());
    let hooked = Envelope::new(hook.clone(), bus.dev.clone(), "build red: ask rate_trail")
        .with_owner(Some(human.clone()))
        .with_allow(Some(vec!["read:*".to_owned()]))
        .from_webhook();
    bus::publish_json(&bus.js, hooked.subject(), &hooked, None).await.unwrap();

    let at = chrono::Utc::now();
    let done = ants_proto::RunState::Done { result: serde_json::json!("green") };
    let run = ants_proto::TrailRun {
        route: Vec::new(),
        thread: hooked.thread_id.clone(),
        origin: ants_proto::provider::Origin {
            by: ants_proto::provider::Starter::Webhook { user: hook },
            channel: None,
        },
        ..ants_test_support::trail_run(&bus.dev, None, done, at)
    };
    let runs = bus.js.get_key_value(ants_proto::TRAIL_RUNS_BUCKET).await.unwrap();
    runs.put(run.key(), serde_json::to_vec(&run).unwrap().into()).await.unwrap();
    let invited = crate::trail::invitation(&bus.dev, &run).unwrap();
    let invited = Envelope { text: format!("{}\nask rate_trail", invited.text), ..invited };
    assert!(trails.invites(&invited).await);
    let forged = Envelope { id: Uuid::now_v7(), ..invited.clone() };
    assert!(!trails.invites(&forged).await, "not under the id of the run's end");

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_human).await;
        assert!(reply.text.ends_with("asked rate_trail: reject"), "narrowed by its own rules: {}", reply.text);
        for (message, picked) in [(&forged, "reject"), (&invited, "allow")] {
            bus::publish_json(&bus.js, message.subject(), message, None).await.unwrap();
            let reply = next_envelope(&mut to_human).await;
            assert_eq!(reply.in_reply_to, Some(message.id));
            assert!(reply.text.ends_with(&format!("asked rate_trail: {picked}")), "{}", reply.text);
        }
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let kept = bus.ledger.narrowed(&hooked.thread_id, None).await.unwrap();
    assert_eq!(kept, Some(vec!["read:*".to_owned()]), "the thread keeps its webhook's rules");
}

/// The reply answering a step of another conductor's run tells what the runs started answering it spent.
#[tokio::test]
async fn the_reply_to_a_step_tells_what_the_runs_started_answering_it_spent() {
    use ants_proto::{RunId, StepBounds, TrailStep};

    let Some((mut bus, _to_human)) = Bus::start().await else { return };
    let trails = bus.conduct().await;
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("spent");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await.unwrap();
    mcp.with_trails(trails);
    bus.mcp = Some(mcp);
    let [lead] = ants(["lead"]);
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let bounds =
        StepBounds { steps: 5, turns: 50, until: chrono::Utc::now() + chrono::TimeDelta::hours(1), route: vec![] };
    let step = TrailStep {
        run: RunId::generate(),
        call: 1,
        label: None,
        schema: None,
        trail: None,
        script: None,
        tip_key: None,
        bounds: Some(bounds),
    };
    let asked = Envelope::new(lead.clone(), bus.dev.clone(), "hold: build it")
        .with_owner(Some(ants_test_support::human()))
        .as_step(step);
    bus::publish_json(&bus.js, asked.subject(), &asked, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the step's prompt to run", || log.locked().prompted == 1).await;
        let script =
            "meta = {\"name\": \"t\", \"description\": \"a test\", \"phases\": []}\nawait agent(\"x\", ant=\"x\")\n";
        let started =
            call_tool(bus.mcp.as_ref().unwrap(), &asked.thread_id, "run_trail", serde_json::json!({"script": script}))
                .await;
        assert_ne!(started.is_error, Some(true), "{started:?}");
        gate.add_permits(1);
        let reply = next_envelope(&mut to_lead).await;
        assert_eq!((&reply.thread_id, reply.in_reply_to), (&asked.thread_id, Some(asked.id)));
        let spent = reply.trail_spent.expect("the reply tells what was spent");
        assert_eq!((spent.steps, spent.turns), (0, 0), "the run sent no step yet: {spent:?}");
        assert!(spent.running <= 1, "one run started answering the step: {spent:?}");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn the_answer_to_work_an_ant_handed_itself_goes_to_a_human() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let mut to_self = bus.client.subscribe(subject::inbox_of(&bus.dev)).await.unwrap();
    bus.client.flush().await.unwrap();
    let handed = Envelope::new(bus.dev.clone(), bus.dev.clone(), "split this out");
    let seq = bus::publish_json(&bus.js, handed.subject(), &handed, None).await.unwrap();
    let _ = to_self.next().await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_human).await;
        assert_eq!(
            (&reply.from, &reply.to, reply.in_reply_to),
            (&bus.dev, &ants_test_support::human(), Some(handed.id))
        );
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().answered.len(), 1);
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_self.next()).await.is_err(), "nothing goes back to the ant's own inbox");
}

#[tokio::test]
async fn members_woken_by_one_post_spend_the_thread_s_turns_together() {
    let Some((bus, mut human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b", "dev-c"]).await;
    let human_id = ants_test_support::human();
    let mut review = ChannelPost::new(ops.clone(), human_id.clone(), "@dev-a @dev-b @dev-c review");
    review.turn_budget = 3;
    bus.post(&review).await;
    // The other members answered first, each spending one of the thread's turns.
    let b = review.reply("dev-b".parse().unwrap(), "fine", &[], &Humans::default()).unwrap();
    bus.post(&b).await;
    let c = review.reply("dev-c".parse().unwrap(), "fine", std::slice::from_ref(&b), &Humans::default()).unwrap();
    let reviewed = bus.post(&c).await;
    let mut ship = ChannelPost::new(ops.clone(), human_id, "@dev-a @dev-b ship it");
    ship.turn_budget = 1;
    bus.post(&ship).await;
    let shipped = bus.post(&ship.reply("dev-b".parse().unwrap(), "shipped", &[], &Humans::default()).unwrap()).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let answer = next_post(&mut replies).await;
        assert_eq!((answer.in_reply_to, answer.turn_budget), (Some(review.id), 0), "the last turn of the thread");
        let escalation = next_envelope(&mut human).await;
        assert_eq!((&escalation.thread_id, escalation.in_reply_to), (&ship.thread_id, Some(ship.id)));
        assert!(escalation.text.starts_with("Turn budget spent; needs a decision."), "{}", escalation.text);
        bus.until_posts_acked(reviewed.max(shipped)).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let answered = log.locked().answered.clone();
    assert_eq!(answered.len(), 1, "no prompt once the thread is out of turns: {answered:?}");
    assert!(answered[0].contains("(0 left after it)"), "{}", answered[0]);
}

#[tokio::test]
async fn a_reply_spending_a_turn_is_stored_only_while_no_other_post_reached_the_channel() {
    use crate::channel::Earlier;

    let Some((bus, _)) = Bus::start().await else { return };
    let (ops, _posts) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let humans = Humans::default();
    let mut review = ChannelPost::new(ops.clone(), ants_test_support::human(), "@dev-a @dev-b review");
    review.turn_budget = 3;
    let at = bus.post(&review).await;
    let filter = subject::chan_room(&ops);
    let read = bus.threads.context(&review, at, Earlier::Omitted, &humans).await.unwrap();
    assert_eq!((read.later.len(), read.last_seq), (0, at), "nothing after the post yet");

    // dev-b's answer lands between dev-a's read and its store: dev-a's, from the same turns, is turned away.
    let b = review.reply("dev-b".parse().unwrap(), "fine", &[], &humans).unwrap();
    assert_eq!(b.turn_budget, 2);
    bus.post(&b).await;
    let a = review.reply(bus.dev.clone(), "fine too", &read.later, &humans).unwrap();
    let store = async |post: &ChannelPost, last_seq: u64| {
        bus::store_expecting(&bus.js, post.subject(), post, "reply-a", &filter, last_seq).await.unwrap()
    };
    assert_eq!(store(&a, read.last_seq).await, bus::Expected::Moved);

    // Read again, it spends the next turn.
    let read = bus.threads.context(&review, at, Earlier::Omitted, &humans).await.unwrap();
    assert_eq!(read.later, std::slice::from_ref(&b));
    let a = review.reply(bus.dev.clone(), "fine too", &read.later, &humans).unwrap();
    assert_eq!(a.turn_budget, 1, "a turn of its own");
    assert_eq!(store(&a, read.last_seq).await, bus::Expected::Stored(bus::Stored::Hub));
    // Stored again from the same read, as by a retry, it finds itself the post that came after, and is stored once.
    assert_eq!(store(&a, read.last_seq).await, bus::Expected::Moved);
    let read = bus.threads.context(&review, at, Earlier::Omitted, &humans).await.unwrap();
    assert_eq!(read.later, [b.clone(), a.clone()]);
    assert_eq!(bus.stored(&subject::chan(&ops, &bus.dev)).await, 1);
    // A post on another thread of the channel moves it on too.
    let other = ChannelPost::new(ops.clone(), ants_test_support::human(), "unrelated");
    let last = bus.post(&other).await;
    let read = bus.threads.context(&review, at, Earlier::Omitted, &humans).await.unwrap();
    assert_eq!((read.later.len(), read.last_seq), (2, last));
}

#[tokio::test]
async fn an_ant_s_reply_to_the_ant_s_own_message_is_prompted_but_not_answered_back_even_out_of_turns() {
    use super::super::prompt::text::own_reply_note;
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [dev_b] = ants(["dev-b"]);
    let [mut to_dev_b] = inboxes(&bus, std::slice::from_ref(&dev_b).try_into().unwrap()).await;
    let (asked, spent) = (ThreadId::generate(), ThreadId::generate());
    let (hello, _) = bus.send_from(dev_b.clone(), &asked, "hello").await;
    // dev-b's reply to a message dev-a sent on a thread with no turns left.
    let mut thanks = Envelope::on_thread(spent.clone(), dev_b.clone(), bus.dev.clone(), "thanks");
    (thanks.in_reply_to, thanks.turn_budget) = (Some(Uuid::now_v7()), 0);
    let thanks_seq = bus::publish_json(&bus.js, thanks.subject(), &thanks, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let (message, entry) = no_reply_on(&bus, &spent).await;
        let TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, reason: None, text } = entry else {
            panic!("{entry:?}")
        };
        // The fake harness answers with its session's name and the prompt.
        let prompt = format!("{}\n\nthanks", own_reply_note(&dev_b, false, true));
        assert!(message == thanks.id && text.ends_with(&format!(": {prompt}")), "{text}");
        bus.until_acked(thanks_seq).await;

        // dev-a answers dev-b's message, spending a turn; dev-b's reply to that answer goes no further.
        let answer = next_envelope(&mut to_dev_b).await;
        assert_eq!((answer.in_reply_to, answer.turn_budget), (Some(hello.id), DEFAULT_TURN_BUDGET - 1));
        let reply = answer.reply(dev_b.clone(), "noted", Role::Ant).unwrap();
        let seq = bus::publish_json(&bus.js, reply.subject(), &reply, None).await.unwrap();
        let (message, entry) = no_reply_on(&bus, &asked).await;
        assert_eq!(message, reply.id);
        assert!(matches!(entry, TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, .. }), "{entry:?}");
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    let prompts = log.locked().prompts.clone();
    assert_eq!(prompts.len(), 3, "{prompts:?}");
    assert!(prompts.contains(&format!("{}\n\nnoted", own_reply_note(&dev_b, false, false))), "{prompts:?}");
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_dev_b.next()).await.is_err(), "nothing goes back to dev-b");
    assert!(timeout(quiet, to_human.next()).await.is_err(), "no turns left is no escalation then");
}

#[tokio::test]
async fn complete_ends_the_turn_on_an_ant_s_message_with_no_reply_but_not_on_a_human_s() {
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("complete");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let [dev_b] = ants(["dev-b"]);
    let [mut to_dev_b] = inboxes(&bus, std::slice::from_ref(&dev_b).try_into().unwrap()).await;
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let (sent, seq) = bus.send_from(dev_b.clone(), &t1, "hold: merged").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mcp = bus.mcp.as_ref().unwrap();
        until("the prompt to run", || log.locked().prompted == 1).await;
        let done = call_tool(mcp, &t1, "complete", serde_json::json!({"reason": " only confirms "})).await;
        assert_ne!(done.is_error, Some(true), "{done:?}");
        gate.add_permits(1);
        let (message, entry) = no_reply_on(&bus, &t1).await;
        assert_eq!(message, sent.id);
        let (why, reason, text) = (NoReplyWhy::Completed, Some("only confirms".to_owned()), "s1: hold: merged");
        assert_eq!(entry, TimelineEntry::NoReply { why, reason, text: text.to_owned() });
        bus.until_acked(seq).await;

        // A human's message is answered all the same.
        bus.send(&t2, "hold: status?").await;
        until("the human's prompt to run", || log.locked().prompted == 2).await;
        let refused = call_tool(mcp, &t2, "complete", serde_json::json!({})).await;
        assert_eq!(refused.is_error, Some(true), "{refused:?}");
        gate.add_permits(1);
        assert_eq!(next_reply(&mut to_human).await, (t2.clone(), "hold: status?".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_dev_b.next()).await.is_err(), "nothing goes to dev-b");
}

#[tokio::test]
async fn a_member_s_answers_to_an_ant_s_replies_to_its_own_post_are_not_posted() {
    use super::super::prompt::text::own_reply_note;
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((bus, _)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut posts) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let [dev_b] = ants(["dev-b"]);
    let humans = Humans::default();
    let own = ChannelPost::new(ops.clone(), bus.dev.clone(), "@dev-b check the build");
    bus.post(&own).await;
    let green = own.reply(dev_b.clone(), "@dev-a green", &[], &humans).unwrap();
    bus.post(&green).await;
    // Read after `green`, its answer, moved the ant's last post seen past its own.
    let tests = own.reply(dev_b.clone(), "@dev-a tests pass too", &[], &humans).unwrap();
    bus.post(&tests).await;
    let human = tests.reply(ants_test_support::human(), "@dev-b and the deploy?", &[], &humans).unwrap();
    bus.post(&human).await;
    let deploy = human.reply(dev_b.clone(), "@dev-a can you deploy?", &[], &humans).unwrap();
    let deploy_seq = bus.post(&deploy).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        assert_eq!(next_post(&mut posts).await.id, own.id);
        let reply = next_post(&mut posts).await;
        assert_eq!(reply.in_reply_to, Some(deploy.id), "dev-b's answer to a human's post is answered");
        let (message, entry) = no_reply_on(&bus, &own.thread_id).await;
        assert_eq!(message, tests.id);
        assert!(matches!(entry, TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, .. }), "{entry:?}");
        bus.until_posts_acked(deploy_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(200), posts.next()).await.is_err(), "one post only");
    let answered = log.locked().answered.clone();
    let [first, again, second] = &answered[..] else { panic!("{answered:?}") };
    assert!(first.starts_with(&own_reply_note(&dev_b, true, false)), "{first}");
    assert!(first.contains("post_channel with on_thread on this thread"), "{first}");
    assert!(first.contains("@dev-a green") && !first.contains("is posted to #ops"), "{first}");
    assert!(again.starts_with(&own_reply_note(&dev_b, true, false)), "{again}");
    assert!(!second.contains("[ants] dev-b replied") && second.contains("is posted to #ops"), "{second}");
}

/// The answer to another ant's reply to the ant's own post is not posted, but post_channel with on_thread posts on the
/// thread as a reply does, spending one of its turns, and is refused with none left (requirement ants-2mfe.67).
#[tokio::test]
async fn post_channel_on_thread_continues_the_exchange_after_a_reply_to_the_ant_s_own_post() {
    use super::super::prompt::text::own_reply_note;
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((mut bus, _)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("on-thread");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await.unwrap();
    mcp.with_channels(crate::mcp::jetstream_channels(bus.js.clone(), None));
    let sessions = bus::ensure_sessions(&bus.js, None, &bus.dev).await.unwrap();
    let threads = Arc::new(Threads::new(bus.js.clone(), bus.dev.clone(), sessions));
    let redactor = Arc::new(Redactor::default());
    let budget = ants_proto::DEFAULT_TURN_BUDGET;
    let poster = super::super::delivery::thread_poster(bus.js.clone(), threads, bus.humans.clone(), budget, redactor);
    mcp.with_thread_posts(poster);
    bus.mcp = Some(mcp);
    let (ops, mut posts) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let _ = bus.channel("dev", &["dev-a", "dev-b"]).await;
    let [dev_b] = ants(["dev-b"]);
    let humans = Humans::default();
    let allow = Some(vec!["read:*".to_owned()]);
    let own = ChannelPost::new(ops.clone(), bus.dev.clone(), "@dev-b review this").with_allow(allow.clone());
    let own = own.with_creator(Some(bus.dev.clone()));
    bus.post(&own).await;
    let review = own.reply(dev_b.clone(), "@dev-a looks good\nhold: on", &[], &humans).unwrap();
    bus.post(&review).await;
    let mut last = ChannelPost::new(ops.clone(), bus.dev.clone(), "@dev-b one more");
    last.turn_budget = 1;
    bus.post(&last).await;
    let spent = last.reply(dev_b.clone(), "@dev-a done\nhold: on", &[], &humans).unwrap();
    let spent_seq = bus.post(&spent).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mcp = bus.mcp.as_ref().unwrap();
        assert_eq!(next_post(&mut posts).await.id, own.id);
        assert_eq!(next_post(&mut posts).await.id, last.id);
        until("both replies to be prompted", || log.locked().prompted == 2).await;
        let on_thread =
            |channel: &str, text: &str| serde_json::json!({"channel": channel, "text": text, "on_thread": true});
        let wrong = call_tool(mcp, &own.thread_id, "post_channel", on_thread("dev", "@dev-b hi")).await;
        assert_eq!(wrong.is_error, Some(true), "another channel's thread: {wrong:?}");
        let signed = call_tool(mcp, &own.thread_id, "post_channel", on_thread("#ops", "@dev-b thanks, merging")).await;
        assert_ne!(signed.is_error, Some(true), "{signed:?}");
        let said = format!("{:?}", signed.content);
        assert!(said.contains(&format!("on thread {} (6 turns left); it wakes dev-b", own.thread_id)), "{said}");
        let posted = next_post(&mut posts).await;
        assert_eq!((&posted.thread_id, posted.in_reply_to, posted.turn_budget), (&own.thread_id, Some(review.id), 6));
        assert_eq!((&posted.creator, &posted.owner, &posted.allow), (&Some(bus.dev.clone()), &None, &allow));
        assert_eq!(posted.text, "@dev-b thanks, merging");

        let refused = call_tool(mcp, &last.thread_id, "post_channel", on_thread("ops", "@dev-b thanks")).await;
        assert_eq!(refused.is_error, Some(true), "{refused:?}");
        let said = format!("{:?}", refused.content);
        assert!(said.contains("has no turns left") && said.contains("escalate"), "{said}");
        gate.add_permits(2);
        for (thread, message) in [(&own.thread_id, review.id), (&last.thread_id, spent.id)] {
            let (answered, entry) = no_reply_on(&bus, thread).await;
            assert_eq!(answered, message);
            assert!(matches!(entry, TimelineEntry::NoReply { why: NoReplyWhy::ReplyToOwn, .. }), "{entry:?}");
        }
        bus.until_posts_acked(spent_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(timeout(Duration::from_millis(200), posts.next()).await.is_err(), "the answers are not posted");
    let answered = log.locked().answered.clone();
    assert!(answered.iter().any(|prompt| prompt.starts_with(&own_reply_note(&dev_b, true, false))), "{answered:?}");
    assert!(answered.iter().any(|prompt| prompt.starts_with(&own_reply_note(&dev_b, true, true))), "{answered:?}");
}

/// The answer to a review notes in the sage's record of it all its thread spent by the ledger, over every prompt on the
/// thread, though a human's message on it before the review gave it a fresh allowance.
#[tokio::test]
async fn a_review_s_record_keeps_all_its_thread_spent_though_a_human_s_message_started_its_allowance_over() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let _hosting = bus.keep_sense();
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let thread = ThreadId::generate();
    bus.send_from(dev_b.clone(), &thread, "tokens 300 cost 0.25").await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        next_envelope(&mut to_dev_b).await;
        bus.send(&thread, "tokens 50").await;
        next_envelope(&mut to_human).await;
        // The session's cost is a running total: the review adds 0.5.
        let review = Envelope::on_thread(thread.clone(), dev_b.clone(), bus.dev.clone(), "tokens 200 cost 0.75")
            .with_allow(Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec()));
        let told = ants_proto::ReviewOutcomes { dream: 1, findings: Vec::new(), earlier_findings: Vec::new() };
        let review = Envelope { review_outcomes: Some(told), ..review };
        let seq = bus::publish_json(&bus.js, review.subject(), &review, None).await.unwrap();
        assert_eq!(next_envelope(&mut to_dev_b).await.thread_id, thread);
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompted, 3);
    let record = bus.sense.as_ref().unwrap().review_record(&bus.dev, &dev_b, 1).await;
    assert!(record.answered.is_some(), "the review was answered");
    assert_eq!(record.spent.map(|spent| (spent.tokens, spent.cost_micros)), Some((550, 750_000)));
}

#[tokio::test]
async fn a_human_joining_a_running_review_gets_the_answer_and_the_sage_records_it_once() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let _hosting = bus.keep_sense();
    let (launch, log, gate) = fake::launch();
    log.locked().knobs.steers = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let dev_b: UserId = "dev-b".parse().unwrap();
    let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    bus.client.flush().await.unwrap();
    let thread = ThreadId::generate();
    bus.send_from(dev_b.clone(), &thread, "tokens 300 cost 0.25").await;

    let sense = bus.sense.as_ref().unwrap();
    let review = |text| {
        let envelope = Envelope::on_thread(thread.clone(), dev_b.clone(), bus.dev.clone(), text)
            .with_allow(Some(ants_proto::REVIEW_ALLOW.map(str::to_owned).to_vec()));
        let told = ants_proto::ReviewOutcomes { dream: 1, findings: Vec::new(), earlier_findings: Vec::new() };
        Envelope { review_outcomes: Some(told), ..envelope }
    };
    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        next_envelope(&mut to_dev_b).await;
        let asked = review("hold: review the dream");
        bus::publish_json(&bus.js, asked.subject(), &asked, None).await.unwrap();
        until("the review prompt to run", || log.locked().prompted == 2).await;
        let findings = serde_json::json!({"findings": [{
            "kind": "lesson", "finding": "Pin the toolchain", "evidence": {"threads": ["worked-thread"]},
            "action": "Use the pinned version"
        }]});
        let steer = format!("tokens 200 cost 0.75\n\n```json\n{findings}\n```");
        let (human, seq) = bus.send(&thread, &steer).await;
        until("the human's message to join the review", || log.locked().steered.len() == 1).await;
        gate.add_permits(1);

        let notice = next_envelope(&mut to_dev_b).await;
        assert_eq!(notice.in_reply_to, Some(asked.id));
        assert_eq!(notice.notice, Some(ants_proto::NoticeKind::Steered));
        assert!(!notice.text.contains("Pin the toolchain"));
        let answer = next_envelope(&mut to_human).await;
        assert_eq!(answer.in_reply_to, Some(human.id));
        assert_eq!(answer.notice, None);
        assert!(answer.text.contains("Pin the toolchain"));
        bus.until_acked(seq).await;

        let record = sense.review_record(&bus.dev, &dev_b, 1).await;
        assert_eq!(record.thread, thread);
        assert!(record.answered.is_some(), "the review was answered");
        let findings =
            record.findings.iter().map(|finding| (finding.id.as_str(), finding.kind.as_str())).collect::<Vec<_>>();
        assert_eq!(findings, [("1.1", "lesson")]);
        assert_eq!(record.spent.map(|spent| (spent.tokens, spent.cost_micros)), Some((500, 750_000)));

        let repeated = review("tokens 50 cost 1\n\n```json\n{\"findings\": []}\n```");
        let seq = bus::publish_json(&bus.js, repeated.subject(), &repeated, None).await.unwrap();
        assert_eq!(next_envelope(&mut to_dev_b).await.in_reply_to, Some(repeated.id));
        bus.until_acked(seq).await;
        assert_eq!(sense.review_record(&bus.dev, &dev_b, 1).await, record, "the first answer and spend are kept");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    let log = log.locked();
    assert_eq!((log.prompted, log.cancelled), (3, 0), "the steer joined rather than starting a new prompt");
}

#[tokio::test]
async fn a_human_s_or_webhook_s_message_or_post_gives_the_thread_the_configured_turn_budget() {
    for turn_budget in [3, 20] {
        Box::pin(fresh_budget_of(turn_budget)).await;
    }
}

/// A human's and a webhook's message and channel post, each stamped with the default budget by its client, give their
/// threads dev-a's `turn_budget`.
async fn fresh_budget_of(turn_budget: u32) {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.chain.turn_budget = turn_budget;
    let (launch, _log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a"]).await;
    let (human, hook) = (ants_test_support::human(), "github".parse::<UserId>().unwrap());
    let ask = Envelope::new(human.clone(), bus.dev.clone(), "hello");
    assert_eq!(ask.turn_budget, DEFAULT_TURN_BUDGET, "stamped by the client");
    bus::publish_json(&bus.js, ask.subject(), &ask, None).await.unwrap();
    let hooked = Envelope::new(hook.clone(), bus.dev.clone(), "build red").with_owner(Some(human.clone()));
    let hooked = hooked.from_webhook();
    bus::publish_json(&bus.js, hooked.subject(), &hooked, None).await.unwrap();
    let mut root = ChannelPost::new(ops.clone(), human.clone(), "@dev-a deploy friday?");
    root.mentions = vec![bus.dev.clone()];
    bus.post(&root).await;
    let alert = ChannelPost::new(ops.clone(), hook, "@dev-a deploy failed").from_webhook(vec![bus.dev.clone()]);
    let seq = bus.post(&alert).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mut answers = BTreeMap::new();
        for _ in 0..2 {
            let reply = next_envelope(&mut to_human).await;
            answers.insert(reply.in_reply_to.unwrap(), reply.turn_budget);
        }
        let want = BTreeMap::from([(ask.id, turn_budget), (hooked.id, turn_budget)]);
        assert_eq!(answers, want, "an answer to a human spends none of the receiving ant's budget");
        let mut posts = BTreeMap::new();
        for _ in 0..2 {
            let reply = next_post(&mut replies).await;
            posts.insert(reply.in_reply_to.unwrap(), reply.turn_budget);
        }
        let want = BTreeMap::from([(root.id, turn_budget - 1), (alert.id, turn_budget - 1)]);
        assert_eq!(posts, want, "a post's reply spends one of the receiving ant's budget");
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn an_ant_s_message_or_post_keeps_its_budget_whatever_the_receiver_s_turn_budget() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.chain.turn_budget = 3;
    let dev_b: UserId = "dev-b".parse().unwrap();
    let [mut to_dev_b] = inboxes(&bus, std::slice::from_ref(&dev_b).try_into().unwrap()).await;
    let (launch, _log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b"]).await;
    let (ask, _) = bus.send_ant(&ThreadId::generate(), "review?").await;
    let mut post = ChannelPost::new(ops.clone(), dev_b.clone(), "@dev-a agree?");
    post.mentions = vec![bus.dev.clone()];
    let seq = bus.post(&post).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_dev_b).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(ask.id), DEFAULT_TURN_BUDGET - 1));
        let reply = next_post(&mut replies).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(post.id), DEFAULT_TURN_BUDGET - 1));
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_later_human_post_on_a_channel_thread_gives_it_the_configured_turn_budget() {
    let Some((mut bus, _)) = Bus::start().await else { return };
    bus.chain.turn_budget = 20;
    let (launch, _log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a"]).await;
    let mut root = ChannelPost::new(ops.clone(), ants_test_support::human(), "@dev-a deploy friday?");
    root.mentions = vec![bus.dev.clone()];
    bus.post(&root).await;
    // Mentioning nobody, it wakes the channel's messenger only; dev-a reads it among the thread's later posts.
    let more = root.reply(ants_test_support::human(), "or monday", &[], &bus.humans.humans()).unwrap();
    assert_eq!(more.turn_budget, DEFAULT_TURN_BUDGET, "stamped by the client");
    let seq = bus.post(&more).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_post(&mut replies).await;
        assert_eq!((reply.in_reply_to, reply.turn_budget), (Some(root.id), 19), "not the 8 the later post carries");
        bus.until_posts_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}

#[tokio::test]
async fn a_team_an_answer_posted_to_a_channel_mentions_stands_for_its_lead_or_wakes_nobody_with_a_note() {
    let Some((bus, _)) = Bus::start().await else { return };
    bus.js.create_stream(crate::hub::teams_config()).await.unwrap();
    let kv = crate::teams::open(&bus.js, None).await.unwrap();
    let mut directory = ants_proto::TeamDirectory::default();
    let human = ants_test_support::human();
    directory
        .create(ants_proto::Team::new("eng".parse().unwrap(), "boss".parse().unwrap(), human.clone()), None)
        .unwrap();
    directory.create(ants_proto::Team::unled("web".parse().unwrap(), human.clone()), None).unwrap();
    assert!(crate::teams::write(&kv, &directory, 0).await.unwrap());
    let (launch, _, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "boss"]).await;
    let entry: ChannelEntry = serde_json::from_slice(&bus.channels.get("ops").await.unwrap().unwrap()).unwrap();
    // The fake harness answers with the prompt, which holds the post: dev-a's answer mentions what the post does.
    let asked = ChannelPost::new(ops.clone(), human.clone(), "@dev-a tell @eng and @web");
    let asked_seq = bus.post(&asked).await;

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let note = "[ants] A team this answer mentions wakes nobody: team web has no lead, nor has any team above it";
        let boss: UserId = "boss".parse().unwrap();
        let reply = next_post(&mut replies).await;
        assert!(reply.mentions(&boss) && !reply.mentions(&"eng".parse().unwrap()), "{:?}", reply.mentions);
        assert!(reply.mentions(&"web".parse().unwrap()), "a team refused stays as it is: {:?}", reply.mentions);
        let teams: Vec<_> = reply.teams.iter().map(|t| (t.team.as_str(), t.lead.as_str())).collect();
        assert_eq!(teams, [("eng", "boss")]);
        assert!(reply.text.contains(&format!("\n\n{note}")), "{}", reply.text);
        assert!(entry.woken(&reply, &bus.humans.humans()).contains(&boss));
        bus.until_posts_acked(asked_seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
}
