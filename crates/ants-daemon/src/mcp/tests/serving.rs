//! How sessions reach the tools: over HTTP or the socket with a token, or through the stdio bridge.

use super::*;

#[tokio::test]
async fn http_needs_a_known_token() {
    let home = home("auth");
    let (mcp, _sent) = start(&home).await;
    let (url, _) = http_server(&mcp, &from_human().thread_id);
    let config = StreamableHttpClientTransportConfig::with_uri(url).auth_header("nope");
    let refused = ClientConfig::default().serve(StreamableHttpClientTransport::from_config(config)).await;
    assert!(refused.is_err());
}

#[tokio::test]
async fn the_socket_serves_the_tools_after_the_token() {
    let home = home("sock");
    let (mcp, mut sent) = start(&home).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let [McpServer::Stdio(server)] =
        &mcp.servers()(&crate::harness::McpSession::test(&current.thread_id, Path::new("/"), false))[..]
    else {
        panic!("stdio")
    };
    assert_eq!(server.args[..2], ["mcp", "--socket"]);
    let token = &server.env.iter().find(|e| e.name == TOKEN_ENV).unwrap().value;

    let mut stream = ipc::connect(Path::new(&server.args[2])).await.unwrap();
    stream.write_all(format!("{token}\n").as_bytes()).await.unwrap();
    let client = ClientConfig::default().serve(stream).await.unwrap();
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "human", "text": "done"})).await;
    assert!(ok, "{text}");
    assert!(sent.recv().await.unwrap().to == ants_test_support::human());

    let mut stranger = ipc::connect(Path::new(&server.args[2])).await.unwrap();
    stranger.write_all(b"nope\n").await.unwrap();
    assert!(ClientConfig::default().serve(stranger).await.is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn a_home_too_long_for_a_socket_serves_the_tools_from_the_runtime_directory() {
    let home = home("long");
    let long = home.join("a-home-nested-deep-enough-that-its-mcp-socket-path-is-past-sun-len".repeat(2));
    std::fs::create_dir_all(&long).unwrap();
    assert!(long.join("mcp.sock").as_os_str().len() > ipc::MAX_PATH);
    let (mcp, mut sent) = start(&long).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let [McpServer::Stdio(server)] =
        &mcp.servers()(&crate::harness::McpSession::test(&current.thread_id, Path::new("/"), false))[..]
    else {
        panic!("stdio")
    };
    let socket = Path::new(&server.args[2]);
    assert!(socket.as_os_str().len() <= ipc::MAX_PATH, "{}", socket.display());
    assert!(!socket.starts_with(&long), "{}", socket.display());
    let token = &server.env.iter().find(|e| e.name == TOKEN_ENV).unwrap().value;
    let mut stream = ipc::connect(socket).await.unwrap();
    stream.write_all(format!("{token}\n").as_bytes()).await.unwrap();
    let client = ClientConfig::default().serve(stream).await.unwrap();
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "human", "text": "done"})).await;
    assert!(ok, "{text}");
    assert!(sent.recv().await.unwrap().to == ants_test_support::human());
    drop(client);
    drop(mcp);
    assert!(!socket.exists(), "the socket is removed with the server");
}

/// Fake harnesses that note the MCP servers each session gets and, on a prompt, send the prompt's text to dev-b
/// through the HTTP one.
fn fake_pool(mcp: &Mcp, http: bool, seen: Arc<StdMutex<Vec<Vec<McpServer>>>>) -> Pool {
    let connect = Arc::new(move || {
        let (on_new, on_load, on_prompt) = (seen.clone(), seen.clone(), seen.clone());
        DynConnectTo::new(
            Agent
                .v1()
                .name("fake")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        let caps = AgentCapabilities::new()
                            .load_session(true)
                            .mcp_capabilities(McpCapabilities::new().http(http));
                        responder.respond(InitializeResponse::new(init.protocol_version).agent_capabilities(caps))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |new: NewSessionRequest, responder, _cx| {
                        let mut seen = on_new.locked();
                        seen.push(new.mcp_servers);
                        responder.respond(NewSessionResponse::new(format!("s{}", seen.len())))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |load: LoadSessionRequest, responder, _cx| {
                        on_load.locked().push(load.mcp_servers);
                        responder.respond(LoadSessionResponse::new())
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| {
                        let AcpContent::Text(t) = &prompt.prompt[0] else { unreachable!() };
                        let server = on_prompt.locked().last().cloned().unwrap_or_default();
                        let answer = match server.first() {
                            Some(McpServer::Http(server)) => {
                                let token = server.headers[0].value.strip_prefix("Bearer ").unwrap();
                                let client = http_client(&server.url, token).await;
                                let args = serde_json::json!({"to": "dev-b", "text": t.text});
                                call(&client, "send_message", args).await.1
                            }
                            _ => "no tools".to_owned(),
                        };
                        cx.send_notification(SessionNotification::new(
                            prompt.session_id,
                            SessionUpdate::AgentMessageChunk(ContentChunk::new(AcpContent::Text(TextContent::new(
                                answer,
                            )))),
                        ))?;
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    let launch = Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_mcp(mcp.servers());
    Pool::new(launch, SessionStore::memory(), 1, Duration::from_secs(60))
}

#[tokio::test]
async fn sessions_get_the_tools_of_their_thread() {
    let home = home("pool");
    let (mcp, mut sent) = start(&home).await;
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let pool = fake_pool(&mcp, true, seen.clone());
    let (one, two) = (from_human(), from_human());

    for envelope in [&one, &two, &one] {
        let _turn = mcp.begin(envelope);
        let answer = pool.prompt(&envelope.thread_id, &envelope.text).await.unwrap();
        assert!(answer.starts_with("Sent to dev-b"), "{answer}");
        assert_eq!(sent.recv().await.unwrap().thread_id, envelope.thread_id);
    }
    let seen = seen.locked();
    assert_eq!(seen.len(), 3, "new, new, then load with the cap of one");
    let token = |servers: &[McpServer]| match servers {
        [McpServer::Http(http)] => http.headers[0].value.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(token(&seen[0]), token(&seen[2]), "a thread keeps its token across a load");
    assert_ne!(token(&seen[0]), token(&seen[1]));
}

#[tokio::test]
async fn harnesses_without_http_get_the_stdio_bridge() {
    let home = home("stdio");
    let (mcp, _sent) = start(&home).await;
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let pool = fake_pool(&mcp, false, seen.clone());
    let current = from_human();
    assert_eq!(pool.prompt(&current.thread_id, "x").await.unwrap(), "no tools");
    let seen = seen.locked();
    let [McpServer::Stdio(stdio)] = &seen[0][..] else { panic!("{:?}", seen[0]) };
    assert_eq!(stdio.command, std::env::current_exe().unwrap());
}
