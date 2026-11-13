//! ACP client side: start a harness over stdio and run prompts through it.

use std::fmt;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    ContentBlock, InitializeRequest, NewSessionRequest, PermissionOptionKind, PromptRequest, RequestPermissionOutcome,
    RequestPermissionRequest, RequestPermissionResponse, SelectedPermissionOutcome, SessionNotification, SessionUpdate,
    TextContent,
};
use agent_client_protocol::{AcpAgent, Agent, Client};
use anyhow::{Result, anyhow};

/// An agent harness ants can drive over ACP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HarnessKind {
    /// GitHub Copilot CLI, `copilot --acp`. Uses the sign-in from `copilot login`.
    #[default]
    Copilot,
}

impl HarnessKind {
    pub const ALL: [HarnessKind; 1] = [HarnessKind::Copilot];

    pub fn name(self) -> &'static str {
        match self {
            HarnessKind::Copilot => "copilot",
        }
    }

    fn command(self) -> &'static [&'static str] {
        match self {
            HarnessKind::Copilot => &["copilot", "--acp"],
        }
    }

    fn agent(self) -> Result<AcpAgent> {
        AcpAgent::from_args(self.command().iter().copied()).map_err(|e| anyhow!("{e}"))
    }
}

impl fmt::Display for HarnessKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for HarnessKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Self::ALL.into_iter().find(|h| h.name() == s).ok_or_else(|| anyhow!("unknown harness {s:?}"))
    }
}

/// What to answer when a harness asks permission to use a tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PermissionPolicy {
    /// Reject every request. Safe default until approvals reach the human.
    #[default]
    Reject,
    /// Allow each request once. Only for trusted, sandboxed workdirs.
    AllowOnce,
}

impl PermissionPolicy {
    fn answer(self, request: &RequestPermissionRequest) -> RequestPermissionResponse {
        let preferred: &[PermissionOptionKind] = match self {
            PermissionPolicy::Reject => &[PermissionOptionKind::RejectOnce, PermissionOptionKind::RejectAlways],
            PermissionPolicy::AllowOnce => &[PermissionOptionKind::AllowOnce],
        };
        // Pick by option kind, never by position or label.
        let chosen = preferred
            .iter()
            .find_map(|kind| request.options.iter().find(|o| o.kind == *kind).map(|o| o.option_id.clone()));
        RequestPermissionResponse::new(match chosen {
            Some(id) => RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(id)),
            None => RequestPermissionOutcome::Cancelled,
        })
    }
}

/// Start the harness, complete the ACP handshake and describe the agent.
pub async fn check(kind: HarnessKind) -> Result<String> {
    Client
        .v1()
        .name("ants")
        .connect_with(kind.agent()?, async |cx: ConnectionTo<Agent>| {
            let init = cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
            Ok(format!("{:?}", init.agent_info))
        })
        .await
        .map_err(|e| anyhow!("{kind} ACP handshake: {e}"))
}

/// Run one prompt in a fresh session rooted at `cwd`, and return the agent's reply text.
pub async fn prompt_once(kind: HarnessKind, cwd: &Path, text: &str, policy: PermissionPolicy) -> Result<String> {
    let reply = Arc::new(Mutex::new(String::new()));
    let sink = reply.clone();
    Client
        .v1()
        .name("ants")
        .on_receive_notification(
            async move |n: SessionNotification, _cx| {
                if let SessionUpdate::AgentMessageChunk(chunk) = n.update
                    && let ContentBlock::Text(t) = chunk.content
                {
                    sink.lock().expect("reply buffer").push_str(&t.text);
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _cx| {
                tracing::info!(?policy, tool = ?request.tool_call, "harness asked for permission");
                responder.respond(policy.answer(&request))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(kind.agent()?, async |cx: ConnectionTo<Agent>| {
            cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
            let session = cx.send_request(NewSessionRequest::new(cwd.to_path_buf())).block_task().await?;
            let done = cx
                .send_request(PromptRequest::new(
                    session.session_id,
                    vec![ContentBlock::Text(TextContent::new(text.to_owned()))],
                ))
                .block_task()
                .await?;
            tracing::debug!(stop_reason = ?done.stop_reason, "prompt finished");
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("{kind} prompt: {e}"))?;
    Ok(std::mem::take(&mut *reply.lock().expect("reply buffer")))
}
