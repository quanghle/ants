//! A daemon run whole ([`run_until`]) against nats-server, its harness an in-process fake ([`crate::harness::fakes`]).

use std::path::Path;
use std::sync::Mutex as StdMutex;

use agent_client_protocol::v1::schema::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse, LoadSessionRequest,
    LoadSessionResponse, McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse, PromptRequest,
    PromptResponse, SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, DynConnectTo};
use rmcp::ServiceExt as _;
use rmcp::model::{CallToolRequestParams, ClientConfig};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;

use super::*;
use crate::harness::Connector;

/// Fake harnesses taking HTTP MCP servers that, on a prompt with a line `ask ANT QUESTION`, call `ask_ant` with it
/// through the session's server and reply with what the call returns; `prompts` gets every prompt's text.
fn asking(prompts: Arc<StdMutex<Vec<String>>>) -> Connector {
    let servers: Arc<StdMutex<Vec<McpServer>>> = Arc::default();
    Arc::new(move || {
        let (on_new, on_load, on_prompt, prompts) =
            (servers.clone(), servers.clone(), servers.clone(), prompts.clone());
        DynConnectTo::new(
            Agent
                .v1()
                .name("asking")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        let caps = AgentCapabilities::new()
                            .load_session(true)
                            .mcp_capabilities(McpCapabilities::new().http(true));
                        responder.respond(InitializeResponse::new(init.protocol_version).agent_capabilities(caps))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |new: NewSessionRequest, responder, _cx| {
                        *on_new.locked() = new.mcp_servers;
                        responder.respond(NewSessionResponse::new("s1"))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |load: LoadSessionRequest, responder, _cx| {
                        *on_load.locked() = load.mcp_servers;
                        responder.respond(LoadSessionResponse::new())
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| {
                        let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                        prompts.locked().push(t.text.clone());
                        let asked = t.text.lines().find_map(|line| line.strip_prefix("ask "));
                        let (to, question) = asked.and_then(|a| a.split_once(' ')).expect("a line `ask ANT QUESTION`");
                        let [McpServer::Http(server)] = &on_prompt.locked().clone()[..] else {
                            panic!("one HTTP MCP server")
                        };
                        let token = server.headers[0].value.strip_prefix("Bearer ").unwrap().to_owned();
                        let (url, args) = (server.url.clone(), serde_json::json!({"to": to, "text": question}));
                        let serde_json::Value::Object(args) = args else { unreachable!() };
                        // Answer from a task, so the call waiting does not stall the connection.
                        cx.clone().spawn(async move {
                            let config = StreamableHttpClientTransportConfig::with_uri(url).auth_header(token);
                            let transport = StreamableHttpClientTransport::from_config(config);
                            let client = ClientConfig::default().serve(transport).await.unwrap();
                            let call = CallToolRequestParams::new("ask_ant").with_arguments(args);
                            let result = client.call_tool(call).await.unwrap();
                            let text: String =
                                result.content.iter().filter_map(|c| c.as_text()).map(|t| t.text.as_str()).collect();
                            let text = ContentBlock::Text(TextContent::new(text));
                            let chunk = SessionUpdate::AgentMessageChunk(ContentChunk::new(text));
                            cx.send_notification(SessionNotification::new(prompt.session_id, chunk))?;
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        })
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    })
}

/// Fake harnesses replying to each prompt with `echo: ` and its last line.
fn echoing() -> Connector {
    Arc::new(|| {
        DynConnectTo::new(
            Agent
                .v1()
                .name("echoing")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        responder.respond(InitializeResponse::new(init.protocol_version))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |_: NewSessionRequest, responder, _cx| responder.respond(NewSessionResponse::new("s1")),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| {
                        let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                        let last = t.text.lines().last().unwrap_or_default();
                        let text = ContentBlock::Text(TextContent::new(format!("echo: {last}")));
                        let chunk = SessionUpdate::AgentMessageChunk(ContentChunk::new(text));
                        cx.send_notification(SessionNotification::new(prompt.session_id, chunk))?;
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    })
}

/// The daemon config of `ants`, served on `url` by the harness `harness`, working and keeping their homes in `dir`.
fn configs(url: &str, harness: &str, dir: &Path, ants: &[&str]) -> Vec<Config> {
    let entries = ants.iter().map(|ant| {
        let home = dir.join(ant);
        format!("[[ant]]\nname = {ant:?}\nworkdir = {dir:?}\nhome = {home:?}\n")
    });
    let text = format!(
        "nats = {url:?}\nharness = {harness:?}\nmessenger = false\nsage = \"none\"\n[harnesses.{harness}]\n\
         command = {harness:?}\n{}",
        entries.collect::<String>()
    );
    let loaded = crate::config::parse(&text, &dir.join("config.toml"), dir).unwrap();
    let names: Vec<String> = loaded.ants.iter().map(|ant| ant.name.to_string()).collect();
    assert_eq!(names, ants, "the ants of the config");
    loaded.ants.into_iter().map(|ant| ant.into_config(url, &loaded.harnesses).unwrap()).collect()
}

/// A harness calling `ask_ant` mid-prompt gets the reply through the live inbox subscription, and the inbox consumer's
/// copy of it, queued on the thread busy with the prompt, is claimed and acked rather than prompted.
#[tokio::test]
async fn a_harness_asking_an_ant_mid_prompt_gets_its_reply_through_the_daemon() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    crate::hub::init(&js, |_| true).await.unwrap();
    let prompts = Arc::new(StdMutex::new(Vec::new()));
    let _fake = crate::harness::fakes::install("asking", asking(prompts.clone()));
    let [dev_a, dev_b] = ants(["dev-a", "dev-b"]);
    let [config, peer] = <[Config; 2]>::try_from(configs(&server.url, "asking", server.dir(), &["dev-a", "dev-b"]))
        .unwrap_or_else(|_| panic!("two ants"));
    // dev-b is in the colony, its inbox heard here.
    let colony = bus::ensure_colony(&js, ants_proto::COLONY_TTL).await.unwrap();
    let entry = colony::entry(&peer).await;
    colony.put(entry.key(), serde_json::to_vec(&entry).unwrap().into()).await.unwrap();
    let mut asked = client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
    let mut replies = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
    client.flush().await.unwrap();

    let (stop, restarts, providers) = (Stop::default(), Restarts::default(), crate::provider::Providers::default());
    let daemon = run_until(config, &stop, &restarts, &providers);
    tokio::pin!(daemon);
    let stream = bus::open_stream(&js, &dev_a).await.unwrap();
    let talk = async {
        // Until the daemon reads its inbox.
        while stream
            .get_consumer::<jetstream::consumer::pull::Config>(&ants_proto::inbox_consumer(&dev_a))
            .await
            .is_err()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let message = Envelope::new(UserId::human(), dev_a.clone(), "ask dev-b 6*7?");
        bus::publish_json(&js, message.subject(), &message, None).await.unwrap();
        let ask = next_envelope(&mut asked).await;
        assert_eq!((&ask.from, &ask.thread_id, ask.text.as_str()), (&dev_a, &message.thread_id, "6*7?"));
        let reply = ask.reply(dev_b.clone(), "42", Role::Ant).unwrap();
        let seq = bus::publish_json(&js, reply.subject(), &reply, None).await.unwrap();
        let answer = next_envelope(&mut replies).await;
        assert_eq!(answer.thread_id, message.thread_id);
        assert!(answer.text.ends_with("42"), "{}", answer.text);
        // Acked once claimed: had it been prompted instead, the ack would follow a second prompt.
        timeout(WAIT, async {
            loop {
                let mut consumer: PullConsumer =
                    stream.get_consumer(&ants_proto::inbox_consumer(&dev_a)).await.unwrap();
                if consumer.info().await.unwrap().ack_floor.stream_sequence >= seq {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the reply is acked");
        assert_eq!(prompts.locked().len(), 1, "the reply is no prompt: {:?}", prompts.locked());
    };
    tokio::select! {
        served = &mut daemon => panic!("the daemon stopped: {served:?}"),
        talked = timeout(WAIT, talk) => talked.expect("the ask is answered"),
    }
    stop.request();
    timeout(WAIT, daemon).await.expect("the daemon stops").unwrap();
}

/// An ant whose start fails because the `ANTS` stream holds as many consumers as it may, the error naming the stream
/// and its limit, is served once consumers are freed, without a reload, and answers its messages.
#[tokio::test]
async fn an_ant_started_while_the_stream_is_full_is_served_once_consumers_are_freed() {
    const ROOM: i32 = 4;
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
    js.create_stream(jetstream::stream::Config { max_consumers: ROOM, ..crate::hub::stream_config() }).await.unwrap();
    crate::hub::init(&js, |_| true).await.unwrap();
    let stream = js.get_stream(ants_proto::STREAM).await.unwrap();
    let mut held = Vec::new();
    for n in 0..ROOM {
        let name = format!("held-{n}");
        let config = jetstream::consumer::pull::Config { durable_name: Some(name.clone()), ..Default::default() };
        stream.create_consumer(config).await.unwrap();
        held.push(name);
    }
    let [dev_a] = ants(["dev-a"]);
    let full = bus::inbox_consumer(&stream, &dev_a, ACK_WAIT, MAX_DELIVER, 1).await.err().expect("no room");
    let full = format!("{full:#}");
    assert!(full.contains(&format!("the ANTS stream holds its {ROOM} consumers, its limit")), "{full}");
    assert!(full.contains("`ants doctor`") && full.contains("maximum consumers limit reached"), "{full}");

    let _fake = crate::harness::fakes::install("echoing", echoing());
    let [config] = <[Config; 1]>::try_from(configs(&server.url, "echoing", server.dir(), &["dev-a"]))
        .unwrap_or_else(|_| panic!("one ant"));
    let mut replies = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
    client.flush().await.unwrap();
    let (stop, restarts, providers) = (Stop::default(), Restarts::default(), crate::provider::Providers::default());
    let daemon = run_until(config, &stop, &restarts, &providers);
    tokio::pin!(daemon);
    let message = Envelope::new(UserId::human(), dev_a.clone(), "hello");
    let talk = async {
        tokio::time::sleep(RETRY_FIRST.saturating_mul(2)).await;
        let inbox = ants_proto::inbox_consumer(&dev_a);
        let consumer = stream.get_consumer::<jetstream::consumer::pull::Config>(&inbox).await;
        assert!(consumer.is_err(), "no inbox consumer while the stream is full");
        for name in &held {
            stream.delete_consumer(name).await.unwrap();
        }
        bus::publish_json(&js, message.subject(), &message, None).await.unwrap();
        next_envelope(&mut replies).await
    };
    let answer = tokio::select! {
        served = &mut daemon => panic!("the daemon stopped: {served:?}"),
        talked = timeout(WAIT, talk) => talked.expect("the ant answers once consumers are freed"),
    };
    assert_eq!(answer.thread_id, message.thread_id);
    assert!(answer.text.ends_with("echo: hello"), "{}", answer.text);
    stop.request();
    timeout(WAIT, daemon).await.expect("the daemon stops").unwrap();
}
