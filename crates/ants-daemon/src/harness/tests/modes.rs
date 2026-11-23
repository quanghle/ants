//! A session is switched to the session mode its harness definition names.

use super::*;

/// A fake harness starting its sessions in `auto`, offering `modes`; returns the modes it was switched to.
fn launch(modes: &'static [&'static str]) -> (Launch, Arc<Mutex<Vec<String>>>) {
    let set = Arc::new(Mutex::new(Vec::new()));
    let log = set.clone();
    let connect: Connector = Arc::new(move || {
        let log = log.clone();
        let opened = move |_| {
            let offered = modes.iter().map(|m| SessionMode::new(*m, *m)).collect();
            Ok(NewSessionResponse::new("s1").modes(SessionModeState::new("auto", offered)))
        };
        DynConnectTo::new(agent("fake", |hello| hello, opened).on_receive_request(
            async move |req: SetSessionModeRequest, responder, _cx| {
                log.locked().push(req.mode_id.0.to_string());
                responder.respond(SetSessionModeResponse::new())
            },
            agent_client_protocol::on_receive_request!(),
        ))
    });
    let custom = custom("command = \"custom-acp\"\nmode = \"default\"");
    (Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_harness(custom), set)
}

#[tokio::test]
async fn sessions_are_switched_to_the_harness_mode() {
    let (launch, set) = launch(&["auto", "default"]);
    Session::start(&launch, None).await.unwrap().close().await;
    assert_eq!(*set.locked(), ["default"]);
}

#[tokio::test]
async fn a_harness_without_the_mode_fails_the_session() {
    let (launch, set) = launch(&["auto"]);
    let err = Session::start(&launch, None).await.err().expect("session should fail").to_string();
    assert!(err.contains("does not offer session mode default"), "{err}");
    assert!(set.locked().is_empty());
}
