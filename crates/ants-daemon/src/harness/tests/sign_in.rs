//! A harness is told ready, signed out, missing or failing by its handshake.

use super::*;

/// A fake harness offering to sign in on a terminal with `--login`, whose session/new fails with `refusal` when
/// there is one.
fn signing(refusal: Option<agent_client_protocol::Error>) -> Launch {
    let connect: Connector = Arc::new(move || {
        let refusal = refusal.clone();
        let hello = |hello: InitializeResponse| {
            let login = AuthMethodTerminal::new("login", "Log in").args(vec!["--login".to_owned()]);
            hello.auth_methods(vec![AuthMethod::Terminal(login)]).agent_info(Implementation::new("fake", "1.0"))
        };
        let opened = move |_| match refusal.clone() {
            Some(e) => Err(e),
            None => Ok(NewSessionResponse::new("s1")),
        };
        DynConnectTo::new(agent("fake", hello, opened))
    });
    Launch::with_transport(connect, PathBuf::from("/"), Default::default())
}

#[tokio::test]
async fn a_signed_out_harness_is_told_apart_from_a_failing_one() {
    let ready = handshake(&signing(None)).await;
    assert_eq!(ready.readiness, Readiness::Ready { agent: "fake 1.0".to_owned() });
    assert_eq!(ready.sign_in.as_deref(), Some("test --login"), "the terminal auth method it offers");
    let session = Session::start(&signing(None), None).await.unwrap();
    let named = HarnessAgent { name: "fake".to_owned(), version: "1.0".to_owned() };
    assert_eq!(session.agent(), Some(&named), "the agent as it named itself");
    session.close().await;

    let required = signing(Some(agent_client_protocol::Error::auth_required()));
    assert_matches!(handshake(&required).await.readiness, Readiness::SignedOut { .. });
    let err = Session::start(&required, None).await.err().expect("a signed-out session");
    let signed_out = err.downcast_ref::<SignedOut>().expect("a sign-in error");
    assert_eq!((signed_out.harness.as_str(), signed_out.sign_in.as_deref()), ("test", Some("test --login")));
    let named = required.with_harness(custom("command = \"custom-acp\"\nsign_in = \"custom-acp login\""));
    assert_eq!(handshake(&named).await.sign_in.as_deref(), Some("custom-acp login"), "the definition's wins");

    // An adapter that exits on start: its stderr comes with the error.
    let exited = |stderr: &str| {
        agent_client_protocol::Error::internal_error().data(format!("Process exited with exit status: 1: {stderr}"))
    };
    let stderr = signing(Some(exited("Error: No authentication information found.")));
    assert_matches!(handshake(&stderr).await.readiness, Readiness::SignedOut { .. });
    let proxy = signing(Some(exited("Proxy authentication required")));
    assert_matches!(handshake(&proxy).await.readiness, Readiness::Failed { .. }, "no sign-in of the harness");
    let err = Session::start(&proxy, None).await.err().expect("a failing session");
    assert!(err.downcast_ref::<SignedOut>().is_none(), "{err:#}");
}

#[tokio::test]
async fn a_harness_not_installed_is_missing() {
    let on_path = |command: &str| {
        let harness = custom(&format!("command = \"{command}\""));
        let env = vec![("PATH".to_owned(), "/nonexistent-ants-bin".to_owned())];
        Launch::new(harness, PathBuf::from("/"), env, Default::default(), ModelSettings::default())
    };
    assert_eq!(handshake(&on_path("no-such-acp")).await.readiness, Readiness::Missing);
    assert_eq!(handshake(&on_path("/nonexistent-ants-bin/acp")).await.readiness, Readiness::Missing);
}

/// An adapter that opens a session, then, once its stdin closes, writes a session update as claude-agent-acp does
/// after answering session/new, and exits 1 on the broken pipe, as Node does on an unhandled EPIPE.
#[cfg(unix)]
const LATE_WRITER: &str = r#"#!/bin/sh
trap '' PIPE
answer() {
    id=$(printf '%s\n' "$1" | grep -o '"id":"[^"]*"' | head -n 1)
    printf '{"jsonrpc":"2.0",%s,"result":%s}\n' "$id" "$2"
}
read -r line
answer "$line" '{"protocolVersion":1,"agentInfo":{"name":"late-writer","version":"1.0"}}'
read -r line
answer "$line" '{"sessionId":"s1"}'
while read -r line; do :; done
sleep 0.2
printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s1","update":{"sessionUpdate":"available_commands_update","availableCommands":[]}}}' || exit 1
"#;

#[cfg(unix)]
#[tokio::test]
async fn a_harness_failing_after_it_opened_the_session_is_ready() {
    let dir = ants_test_support::scratch_dir("late-writer");
    let command = dir.join("late-writer");
    ants_test_support::executable(&command, LATE_WRITER);
    let harness = custom(&format!("command = {:?}", command.display().to_string()));
    let launch = Launch::new(harness, dir.clone(), Vec::new(), Default::default(), ModelSettings::default());
    let checked = handshake(&launch).await;
    assert_eq!(checked.readiness, Readiness::Ready { agent: "late-writer 1.0".to_owned() });
    std::fs::remove_dir_all(dir).unwrap();
}
