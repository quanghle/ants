//! A reply made only of the error lines a harness prints of a provider limit is a limit, not an answer.

use super::*;

/// A fake harness that streams each `|`-separated piece of a prompt as a chunk of its reply, then ends the turn.
fn echoing() -> Launch {
    let connect: Connector = Arc::new(|| {
        DynConnectTo::new(plain("echo").on_receive_request(
            async move |prompt: PromptRequest, responder, cx| {
                let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                for piece in t.text.split('|') {
                    let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(piece)));
                    cx.send_notification(SessionNotification::new(
                        prompt.session_id.clone(),
                        SessionUpdate::AgentMessageChunk(chunk),
                    ))?;
                }
                responder.respond(PromptResponse::new(StopReason::EndTurn))
            },
            agent_client_protocol::on_receive_request!(),
        ))
    });
    Launch::with_transport(connect, PathBuf::from("/"), Default::default())
}

#[tokio::test]
async fn a_reply_of_error_lines_reporting_a_limit_is_no_answer() {
    let session = Session::start(&echoing(), None).await.unwrap();
    let limited = |reply: Result<String>| reply.err().and_then(|e| e.downcast_ref::<Limited>().map(|l| l.kind));
    let overloaded = "Info: Request failed due to a transient API error. Retrying...|Error: Failed to get response \
                          from the AI model; retried 5 times (total retry wait time: 16.07 seconds) Last error: 529 \
                          Overloaded";
    assert_eq!(limited(session.prompt(overloaded).await), Some(LimitKind::RateLimit));
    let answers = [
        "A provider |answers 429 |when a rate limit is hit.",
        "Error:| handling in Rust uses Result.",
        "Error: Failed to get response from the AI model; retried 5 times Last error: 500 Internal Server Error",
    ];
    for answer in answers {
        assert_eq!(session.prompt(answer).await.unwrap(), answer.replace('|', ""), "the session lives on");
    }
    // Retry notices before an attempt that succeeds, as Copilot CLI and Antigravity stream them, are no part of it.
    let retried = [
        "Info: Request failed due to a transient API error. Retrying...|Hello| there.",
        "\n⚠ API error (attempt 1): Error 429, Message: Resource exhausted. Please retry in 1.0s.\n|Hello| there.",
    ];
    for reply in retried {
        assert_eq!(session.prompt(reply).await.unwrap(), "Hello there.", "{reply}");
    }
    session.close().await;
}
