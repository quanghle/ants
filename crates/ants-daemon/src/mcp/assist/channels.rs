//! Acting on channels for the human: `describe_channel_for_human` sets or clears a channel's description, asking the
//! channel manager ([`crate::chanadm`]), which checks that the human's grant names the ant.

use ants_proto::{CHANADM_WAIT, ChannelAdminOutcome, ChannelAdminRequest, ChannelId, ChannelOp, ThreadId};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use crate::mcp::Hub;
use crate::mcp::channels::describe_channel;
use crate::mcp::tools::{Tools, text};

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct DescribeChannelForHuman {
    /// The channel, such as `ops`.
    channel: String,
    /// What the channel is for, up to 1000 characters; empty clears it.
    description: String,
}

impl Hub {
    async fn describe_channel_for_human(
        &self,
        thread: &ThreadId,
        args: DescribeChannelForHuman,
    ) -> Result<String, String> {
        let (human, _) = self.acting_for()?;
        let admin = self.admin.get().ok_or("channels cannot be managed from this daemon")?;
        let channel: ChannelId = args.channel.trim().trim_start_matches('#').parse().map_err(|e| format!("{e}"))?;
        let description = args.description.trim();
        let op = if description.is_empty() {
            ChannelOp::ClearDescription
        } else {
            ChannelOp::SetDescription { description: description.to_owned() }
        };
        let request = ChannelAdminRequest::new(channel.clone(), self.ant.clone(), thread.clone(), vec![op])
            .on_behalf(Some(human.clone()));
        let reply = admin.request(request).await.map_err(|e| format!("cannot ask the channel manager: {e:#}"))?;
        let Some(reply) = reply else {
            return Err(format!(
                "no channel manager answered in {}s, so the description may not have been changed; {human} can change \
                 it with `ants channel set {channel} --description`",
                CHANADM_WAIT.as_secs()
            ));
        };
        match reply.outcome {
            ChannelAdminOutcome::Applied { entry, changed, .. } if changed.is_empty() => {
                Ok(format!("Nothing to change. {}", describe_channel(&entry)))
            }
            ChannelAdminOutcome::Applied { entry, changed, .. } => {
                let told = self.tell(&human, format!("changed #{channel} for {human}: {}.", changed.join("; "))).await;
                Ok(format!(
                    "Changed #{channel} for {human}: {}; sessions started from now on take it in. {}{told}",
                    changed.join("; "),
                    describe_channel(&entry)
                ))
            }
            ChannelAdminOutcome::Refused { refusal } => Err(format!("#{channel} was not changed: {refusal}")),
        }
    }
}

#[tool_router(router = assist_channels_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "As the assistant of a human, set the description of a channel for it, or clear it with an \
        empty `description`: what the channel is for, up to 1000 characters, which every member ant's session on a \
        thread of the channel starts with; sessions already running keep the one they started with. The channel \
        manager applies it and announces it in the channel, and the human is told. Only a human, or its assistant for \
        it, sets a channel's description.")]
    async fn describe_channel_for_human(
        &self,
        Parameters(args): Parameters<DescribeChannelForHuman>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.describe_channel_for_human(&thread, args).await))
    }
}
