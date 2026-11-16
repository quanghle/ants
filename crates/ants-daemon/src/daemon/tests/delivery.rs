//! Delivering answers: the title and files a turn attached, a webhook's answer, and replies on channel threads.

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
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/daemon-t/title");
    let _ = std::fs::remove_dir_all(&home);
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let (titled, plain) = (ThreadId::generate(), ThreadId::generate());
    bus.send(&titled, "hold on").await;

    let suggest = async |thread: &ThreadId, title: &str| {
        let servers = bus.mcp.as_ref().unwrap().servers()(thread, std::path::Path::new("/"), true);
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
        let ask = ChannelPost::new(ops.clone(), UserId::human(), "hold: @dev-a the deploy failed");
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
    let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/daemon-t/reply-files");
    let _ = std::fs::remove_dir_all(&home);
    let work = home.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("report.md"), "# done").unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await.unwrap();
    mcp.with_files(crate::mcp::jetstream_files(bus.js.clone()), home.clone());
    bus.mcp = Some(mcp);
    let (ops, mut posts) = bus.channel("ops", &["dev-a"]).await;
    let thread = ThreadId::generate();
    bus.send(&thread, "hold on").await;

    let attach_to_reply = async |thread: &ThreadId, files: &[&str]| {
        let servers = bus.mcp.as_ref().unwrap().servers()(thread, &work, true);
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
        let ask = ChannelPost::new(ops.clone(), UserId::human(), "hold: @dev-a the report?");
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
    let root = ChannelPost::new(ops.clone(), UserId::human(), "deploy friday?");
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
    let more = root.reply(UserId::human(), "rollback plan is ready", &[], &Humans::default()).unwrap();
    bus.post(&more).await;
    let again = root.reply(UserId::human(), "@dev-a and now?", &[], &Humans::default()).unwrap();
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
async fn an_answer_to_a_webhook_goes_to_the_thread_s_owner_or_nowhere() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (hook, human) = ("github".parse::<UserId>().unwrap(), UserId::human());
    bus.mark_webhook(&hook).await;
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

#[tokio::test]
async fn members_woken_by_one_post_spend_the_thread_s_turns_together() {
    let Some((bus, mut human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let (ops, mut replies) = bus.channel("ops", &["dev-a", "dev-b", "dev-c"]).await;
    let human_id = UserId::human();
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
