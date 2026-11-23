//! Permission requests of a harness are answered by the ant's allow rules, and by a review's.

use super::*;

/// A fake harness asking permission as Codex does for an MCP tool: it reports the call, then asks by the call's
/// id alone. Replies with the option chosen.
fn codex(policy: PermissionPolicy) -> Launch {
    let connect: Connector = Arc::new(|| {
        DynConnectTo::new(plain("codex").on_receive_request(
            async move |prompt: PromptRequest, responder, cx| {
                let sender = cx.clone();
                cx.spawn(async move {
                    let session = prompt.session_id;
                    let input = json!({ "server": "ants", "tool": "send_message", "arguments": {} });
                    let call = ToolCall::new("c1", "mcp.ants.send_message").kind(ToolKind::Execute).raw_input(input);
                    sender
                        .send_notification(SessionNotification::new(session.clone(), SessionUpdate::ToolCall(call)))?;
                    let fields = ToolCallUpdateFields::new().kind(ToolKind::Execute).status(ToolCallStatus::Pending);
                    let options = vec![
                        PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
                        PermissionOption::new("cancel", "Cancel", PermissionOptionKind::RejectOnce),
                    ];
                    let ask =
                        RequestPermissionRequest::new(session.clone(), ToolCallUpdate::new("c1", fields), options);
                    let chosen = match sender.send_request(ask).block_task().await?.outcome {
                        RequestPermissionOutcome::Selected(s) => s.option_id.0.to_string(),
                        _ => "cancelled".to_owned(),
                    };
                    sender.send_notification(SessionNotification::new(
                        session,
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(TextContent::new(
                            chosen,
                        )))),
                    ))?;
                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                })
            },
            agent_client_protocol::on_receive_request!(),
        ))
    });
    Launch::with_transport(connect, PathBuf::from("/"), policy)
}

#[tokio::test]
async fn permission_requests_are_matched_with_the_tool_call_they_name() {
    for (rule, chosen) in [("other:send_message", "allow"), ("execute", "cancel"), ("other:check_inbox", "cancel")] {
        let policy = PermissionPolicy::new(Allowlist::parse(&[rule]).unwrap(), None);
        let session = Session::start(&codex(policy), None).await.unwrap();
        assert_eq!(session.prompt("go").await.unwrap(), chosen, "{rule}");
        session.close().await;
    }
}

#[tokio::test]
async fn a_review_session_rejects_tools_outside_the_review_unasked() {
    let launch = codex(PermissionPolicy::allow_all()).for_review().unwrap();
    let session = Session::start(&launch, None).await.unwrap();
    assert_eq!(session.prompt("go").await.unwrap(), "cancel", "send_message is not read only");
    session.close().await;
}
