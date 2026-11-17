//! `react`, adding or removing a reaction of the ant to a message, and `list_reactions`, listing those to a message
//! the ant reads, both asked of whatever answers the ant's reactions requests (see [`crate::records::reactions`]).

use std::sync::Arc;

use ants_proto::{JsDomain, Reaction, ReactionChange, ReactionsOutcome, ReactionsRequest, ThreadId, UserId, Uuid};
use anyhow::Result;
use futures::future::BoxFuture;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::tools::{Tools, text};
use crate::records::reactions;
use crate::sync::MutexExt as _;

/// Answers the ant's reactions requests, to list the reactions to a message or change its own; None when nothing
/// answered.
pub type ReactionAsker =
    Arc<dyn Fn(ReactionsRequest) -> BoxFuture<'static, Result<Option<ReactionsOutcome>>> + Send + Sync>;

/// Asks over `client`, the ant's own connection, where `ants signer` or `ants webhook serve` answers
/// ([`reactions::request`]).
pub fn nats_reaction_asker(client: async_nats::Client) -> ReactionAsker {
    Arc::new(move |request| {
        let client = client.clone();
        Box::pin(async move { reactions::request(&client, &request).await })
    })
}

/// Answers in the daemon itself ([`reactions::answer`]), for a server without credentials, where its connection reads
/// and writes everything as a human's or `ants signer`'s does.
pub fn local_reaction_asker(js: async_nats::jetstream::Context, js_domain: Option<JsDomain>) -> ReactionAsker {
    Arc::new(move |request| {
        let (js, js_domain) = (js.clone(), js_domain.clone());
        Box::pin(async move { reactions::answer(&js, js_domain.as_ref(), &request).await.map(Some) })
    })
}

/// Why the answer to a reactions request about `message` is [`ReactionsOutcome::Unknown`].
fn unread(message: Uuid) -> String {
    format!("Message {message} is none this ant reads, or the stream no longer keeps it")
}

#[derive(Deserialize, schemars::JsonSchema)]
pub(super) struct React {
    /// The reaction: 1-64 of lowercase letters, digits and '-', such as `eyes`, `check`, `thumbsup`, `x`, `heart` or
    /// `rocket`.
    reaction: String,
    /// The id of the message to react to; omitted for the message being handled on this thread.
    #[serde(default)]
    message_id: Option<String>,
    /// Remove the ant's reaction instead of adding it.
    #[serde(default)]
    remove: bool,
}

impl React {
    /// Adding `reaction` to the message being handled.
    pub(super) fn current(reaction: &str) -> Self {
        Self { reaction: reaction.to_owned(), message_id: None, remove: false }
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ListReactions {
    /// The id of the message whose reactions to list; omitted for the message being handled on this thread.
    #[serde(default)]
    message_id: Option<String>,
}

impl Hub {
    /// Add or remove the ant's reaction to the message `args` names, by default the one `thread` is handling. A
    /// reaction is no message: it reaches no inbox and spends no turn.
    pub(super) async fn react(&self, thread: &ThreadId, args: React) -> Result<String, String> {
        let asker = self.reaction_asker.get().ok_or("reactions are not available here")?;
        let reaction: Reaction = args.reaction.trim().parse().map_err(|e| format!("{e}"))?;
        let message = self.message(thread, args.message_id.as_deref())?;
        let change = ReactionChange::new(reaction.clone(), args.remove);
        let outcome = asker(ReactionsRequest::change(self.ant.clone(), message, change))
            .await
            .map_err(|e| format!("{e:#}"))?
            .ok_or(
                "no answer about the reaction, so it may not be made: `ants signer`, on the machine with the account \
                 key, makes such changes, and none answered",
            )?;
        let changed = match outcome {
            ReactionsOutcome::Applied { changed } => changed,
            ReactionsOutcome::Unknown => return Err(format!("{}: nothing changed", unread(message))),
            ReactionsOutcome::Listed { .. } => return Err("an answer to another request".to_owned()),
        };
        Ok(match (args.remove, changed) {
            (false, true) => format!("Reacted {reaction} to message {message}."),
            (false, false) => format!("Already reacted {reaction} to message {message}."),
            (true, true) => format!("Removed the reaction {reaction} to message {message}."),
            (true, false) => format!("No reaction {reaction} to message {message} to remove."),
        })
    }

    /// The message `id` names, else the one `thread` is handling.
    fn message(&self, thread: &ThreadId, id: Option<&str>) -> Result<Uuid, String> {
        match id.map(str::trim).filter(|m| !m.is_empty()) {
            Some(id) => id.parse().map_err(|e| format!("message_id {id:?}: {e}")),
            None => {
                let current = self.sessions.locked().current.get(thread).map(|e| e.id);
                current.ok_or_else(|| {
                    "no message is being handled on this thread; name the message by message_id".to_owned()
                })
            }
        }
    }

    /// The reactions to the message `args` names, by default the one `thread` is handling, if the ant reads it.
    async fn list_reactions(&self, thread: &ThreadId, args: ListReactions) -> Result<String, String> {
        let asker = self.reaction_asker.get().ok_or("reactions are not available here")?;
        let message = self.message(thread, args.message_id.as_deref())?;
        let outcome =
            asker(ReactionsRequest::new(self.ant.clone(), message)).await.map_err(|e| format!("{e:#}"))?.ok_or(
                "no answer about the reactions: `ants signer`, on the machine with the account key, or an \
             `ants webhook serve` with a [[human]] entry answers such requests, and none did",
            )?;
        Ok(match outcome {
            ReactionsOutcome::Unknown => format!("{}.", unread(message)),
            ReactionsOutcome::Applied { .. } => return Err("an answer to another request".to_owned()),
            ReactionsOutcome::Listed { reactions } if reactions.is_empty() => {
                format!("No reactions to message {message}.")
            }
            ReactionsOutcome::Listed { reactions } => {
                let shown: Vec<String> = reactions
                    .iter()
                    .map(|r| {
                        let by: Vec<&str> = r.by.iter().map(UserId::as_str).collect();
                        format!("{} by {}", r.reaction, by.join(", "))
                    })
                    .collect();
                format!("Reactions to message {message}: {}.", shown.join("; "))
            }
        })
    }
}

#[tool_router(router = reactions_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "React to a message, such as `eyes` for seen or `check` for done, or remove the ant's \
        reaction with `remove`. Reacts to the message being handled unless `message_id` names another this ant reads \
        (ids come with sent messages, replies and check_inbox). A reaction is no message: it wakes nobody, spends no turn and \
        answers no ask; humans see it beneath the message. Reacting or removing again changes nothing.")]
    async fn react(
        &self,
        Parameters(args): Parameters<React>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.react(&thread, args).await))
    }

    #[tool(description = "List the reactions to a message, such as `eyes` or `check`, and who reacted: of the \
        message being handled unless `message_id` names another this ant reads (one sent to it or by it, a notes post \
        or a channel post). Reactions are no messages, so they never arrive otherwise.")]
    async fn list_reactions(
        &self,
        Parameters(args): Parameters<ListReactions>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.list_reactions(&thread, args).await))
    }
}
