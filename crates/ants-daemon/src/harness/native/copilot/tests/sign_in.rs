//! A copilot-sdk session signs in to the definition's `mcp_sign_in` servers as they need it (`copilot/mcp_sign_in.rs`).

use super::*;

/// The servers each sign-in to an MCP server of `played` named, by the session signing in.
fn signed_in(played: &Played, session: &str) -> Vec<Value> {
    let logins = played.requests("session.mcp.oauth.login");
    logins.iter().filter(|login| login["sessionId"] == session).map(|login| login["serverName"].clone()).collect()
}

/// The answer to `session.mcp.list` of session `params` listing `servers` as pending, then their status changing to
/// each of `statuses`, as the CLI starts them.
fn listing(params: &Value, servers: &[&str], statuses: &[(&str, Value)]) -> Answer {
    let id = &params["sessionId"];
    let listed: Vec<_> = servers.iter().map(|name| json!({ "name": name, "status": "pending" })).collect();
    let changed = statuses.iter().map(|(server, status)| {
        let mut data = json!({ "serverName": server });
        data.as_object_mut().unwrap().extend(status.as_object().unwrap().clone());
        event(id, "session.mcp_server_status_changed", data)
    });
    Answer::ok(json!({ "servers": listed })).then(changed.collect())
}

/// A launch of `played` for a thread, and the sign-ins its sessions report.
fn reporting(played: &Played) -> (Launch, Arc<Mutex<Vec<McpSignIn>>>) {
    let told = Arc::new(Mutex::new(Vec::new()));
    let sink = told.clone();
    let thread: ThreadId = "t1".parse().unwrap();
    let launch = played.launch(ModelSettings::default()).for_thread(&thread).with_mcp_sign_ins(Arc::new(
        move |on: &ThreadId, store: &str, sign_ins| {
            assert!(*on == thread && store.ends_with("/copilot"), "{on} {store}");
            sink.lock().unwrap().extend(sign_ins);
        },
    ));
    (launch, told)
}

#[tokio::test]
async fn copilot_sessions_sign_in_to_the_listed_mcp_servers_needing_it_once_they_settle() {
    // icm signs in with the credentials the CLI keeps, ado needs a browser, kusto fails to start and bluebird is no
    // server of the session.
    let table = r#"mcp_sign_in = ["icm", "ado", "kusto", "bluebird"]"#;
    let played = Played::new("copilot-mcp-sign-in", table, |method, params| match method {
        "session.mcp.list" => Some(listing(
            params,
            &["icm", "ado", "kusto"],
            &[
                ("icm", json!({ "status": "connected" })),
                ("ado", json!({ "status": "needs-auth" })),
                (
                    "kusto",
                    json!({ "status": "failed", "error": "no kusto-mcp", "errorClassification": "os_not_found" }),
                ),
            ],
        )),
        "session.mcp.oauth.login" => Some(Answer::ok(json!({ "authorizationUrl": "https://login.example/ado" }))),
        "session.send" => {
            let id = &params["sessionId"];
            let mut events = vec![event(
                id,
                "session.mcp_server_status_changed",
                json!({ "serverName": "ado", "status": "connected" }),
            )];
            events.extend(idle(id, false));
            Some(default_answer(method, params).then(events))
        }
        _ => None,
    });
    let (launch, told) = reporting(&played);
    let ado = || McpSignIn::Browser { server: "ado".to_owned(), url: "https://login.example/ado".to_owned() };
    let failed = |server: &str, error: &str| McpSignIn::Failed { server: server.to_owned(), error: error.to_owned() };

    let first = Session::start(&launch, None).await.expect("opens whatever its sign-ins come to");
    assert_eq!(signed_in(&played, &first.id().0), [json!("ado")], "only the server needing it");
    let mut opened = std::mem::take(&mut *told.lock().unwrap());
    opened.sort_by(|a, b| a.server().cmp(b.server()));
    assert_eq!(
        opened,
        [
            ado(),
            failed("bluebird", "not configured in the session"),
            McpSignIn::SignedIn { server: "icm".to_owned() },
            failed("kusto", "no kusto-mcp (os_not_found)"),
        ]
    );

    // While ado's browser sign-in waits, sessions of other CLIs answer it rather than starting another; a fork signs
    // in to none, the session/load following it does.
    let loaded = Session::start(&launch, Some(SessionId::new("s1"))).await.unwrap();
    assert_eq!(signed_in(&played, "s1"), [] as [Value; 0]);
    assert!(told.lock().unwrap().contains(&ado()));
    fork(&launch, &SessionId::new("s1")).await.unwrap().expect("the CLI forks");
    assert!(played.requests("session.mcp.list").iter().all(|list| list["sessionId"] != "forked"));

    // ado connects once the browser came back to the first session: the session still open is told.
    first.run(Prompt::new("go")).await.unwrap();
    let changed = || played.requests("session.mcp.oauth.authenticationStateChanged");
    until(|| !changed().is_empty()).await;
    assert_eq!(changed(), [json!({ "sessionId": "s1", "serverName": "ado" })]);
    let again = Session::start(&launch, None).await.unwrap();
    assert_eq!(signed_in(&played, &again.id().0), [json!("ado")], "no browser waits any more");
    for session in [first, loaded, again] {
        session.close().await;
    }
}

#[tokio::test]
async fn a_copilot_sign_in_its_mcp_server_was_reconfigured_during_runs_again_once_it_settles() {
    let logins = Arc::new(Mutex::new(0));
    let counted = logins.clone();
    let played = Played::new("copilot-mcp-reconfigured", r#"mcp_sign_in = ["ado"]"#, move |method, params| {
        let id = &params["sessionId"];
        match method {
            "session.mcp.list" => Some(listing(params, &["ado"], &[("ado", json!({ "status": "needs-auth" }))])),
            "session.mcp.oauth.login" => {
                let mut logins = counted.lock().unwrap();
                *logins += 1;
                let answer = Answer::ok(json!({ "authorizationUrl": format!("https://login.example/{logins}") }));
                // The session's MCP configuration changes under the first, restarting the server.
                Some(if *logins > 1 {
                    answer
                } else {
                    answer.then(vec![
                        event(id, "session.mcp_server_needs_reconnect", json!({ "serverName": "ado" })),
                        event(
                            id,
                            "session.mcp_server_status_changed",
                            json!({ "serverName": "ado", "status": "pending" }),
                        ),
                        event(
                            id,
                            "session.mcp_server_status_changed",
                            json!({ "serverName": "ado", "status": "needs-auth" }),
                        ),
                    ])
                })
            }
            _ => None,
        }
    });
    let (launch, told) = reporting(&played);
    let session = Session::start(&launch, None).await.unwrap();
    assert_eq!(*logins.lock().unwrap(), 2);
    let browser = McpSignIn::Browser { server: "ado".to_owned(), url: "https://login.example/2".to_owned() };
    assert_eq!(*told.lock().unwrap(), [browser], "the cancelled sign-in neither failed nor handed out");
    session.close().await;
}

#[tokio::test]
async fn a_copilot_sign_in_whose_browser_came_back_at_once_signs_in_without_a_notice() {
    // s1 has ado connected; in later sessions it needs authentication, the identity provider's live session completing
    // the browser's redirect as soon as the sign-in answers its address.
    let played = Played::new("copilot-mcp-at-once", r#"mcp_sign_in = ["ado"]"#, |method, params| {
        let id = &params["sessionId"];
        match method {
            "session.mcp.list" if *id == "s1" => {
                Some(listing(params, &["ado"], &[("ado", json!({ "status": "connected" }))]))
            }
            "session.mcp.list" => Some(listing(params, &["ado"], &[("ado", json!({ "status": "needs-auth" }))])),
            "session.mcp.oauth.login" => {
                Some(Answer::ok(json!({ "authorizationUrl": "https://login.example/ado" })).then(vec![event(
                    id,
                    "session.mcp_server_status_changed",
                    json!({ "serverName": "ado", "status": "connected" }),
                )]))
            }
            _ => None,
        }
    });
    let (launch, told) = reporting(&played);
    let s1 = Session::start(&launch, Some(SessionId::new("s1"))).await.unwrap();
    let other = Session::start(&launch, None).await.unwrap();
    assert_eq!(signed_in(&played, &other.id().0), [json!("ado")]);
    let changed = || played.requests("session.mcp.oauth.authenticationStateChanged");
    until(|| !changed().is_empty()).await;
    assert_eq!(changed(), [json!({ "sessionId": "s1", "serverName": "ado" })]);
    let signed = McpSignIn::SignedIn { server: "ado".to_owned() };
    assert!(told.lock().unwrap().iter().all(|sign_in| *sign_in == signed), "{:?}", told.lock().unwrap());

    // No browser sign-in is left waiting for a third session to be handed.
    let third = Session::start(&launch, None).await.unwrap();
    assert_eq!(signed_in(&played, &third.id().0), [json!("ado")]);
    for session in [s1, other, third] {
        session.close().await;
    }
}

#[tokio::test]
async fn a_listed_mcp_server_needing_authentication_after_its_session_opened_signs_in() {
    let played = Played::new("copilot-mcp-later", r#"mcp_sign_in = ["icm"]"#, |method, params| match method {
        "session.mcp.list" => Some(listing(params, &["icm"], &[("icm", json!({ "status": "connected" }))])),
        "session.mcp.oauth.login" => Some(Answer::ok(json!({ "authorizationUrl": "https://login.example/icm" }))),
        "session.send" => {
            // Its credentials could not be renewed.
            let id = &params["sessionId"];
            let mut events = vec![event(
                id,
                "session.mcp_server_status_changed",
                json!({ "serverName": "icm", "status": "needs-auth" }),
            )];
            events.extend(idle(id, false));
            Some(default_answer(method, params).then(events))
        }
        _ => None,
    });
    let (launch, told) = reporting(&played);
    let session = Session::start(&launch, None).await.unwrap();
    assert_eq!(signed_in(&played, &session.id().0), [] as [Value; 0], "connected");
    assert_eq!(*told.lock().unwrap(), [McpSignIn::SignedIn { server: "icm".to_owned() }]);
    session.run(Prompt::new("go")).await.unwrap();
    let browser = McpSignIn::Browser { server: "icm".to_owned(), url: "https://login.example/icm".to_owned() };
    until(|| told.lock().unwrap().contains(&browser)).await;
    assert_eq!(signed_in(&played, &session.id().0), [json!("icm")]);
    session.close().await;
}
