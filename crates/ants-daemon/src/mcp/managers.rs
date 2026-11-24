//! The tools of a manager ant: `request_ant` adds a worker ant from an ant template listing the manager among its
//! managers, and `retire_ant` retires one it added, both through the [`AntRequester`] the daemon gives a manager.

use std::sync::Arc;
use std::time::Duration;

use ants_proto::{ThreadId, UserId};
use futures::future::BoxFuture;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;

use super::Hub;
use super::messages::ask_wait;
use super::routing::LOOKUP_TIMEOUT;
use super::tools::{Tools, text};

/// How long `request_ant` waits for the new ant's credentials by default, a human's approval included.
pub(super) const DEFAULT_REQUEST_WAIT: Duration = Duration::from_mins(15);
/// How long `request_ant` waits for the new ant to be in the colony once listed; the config file is reread within
/// seconds of a change.
const LISTED_WAIT: Duration = Duration::from_secs(60);
const LISTED_POLL: Duration = Duration::from_secs(1);

/// What a manager ant's `request_ant` asks for: ant `name` from `template`, set as `request` asks, for `reason`, on
/// the manager's `thread`, waiting at most `wait` for its credentials.
#[derive(Debug, Clone)]
pub struct AntAsk {
    pub manager: UserId,
    pub thread: ThreadId,
    pub name: UserId,
    pub template: String,
    pub request: crate::profile::template::Request,
    pub reason: String,
    pub wait: Duration,
}

/// What a manager ant's `retire_ant` asks for: retire ant `name`, one it added, on the manager's `thread`.
#[derive(Debug, Clone)]
pub struct RetireAsk {
    pub manager: UserId,
    pub thread: ThreadId,
    pub name: UserId,
}

/// What a manager ant asks for.
#[derive(Debug, Clone)]
pub enum ManagerAsk {
    Add(Box<AntAsk>),
    Retire(RetireAsk),
}

/// Adds the ant a manager asks for ([`AntAsk`]) once its credentials are signed, listing it in the config file for the
/// daemon to serve, or retires one it added ([`RetireAsk`]); returns what it did, or why not.
pub type AntRequester = Arc<dyn Fn(ManagerAsk) -> BoxFuture<'static, Result<String, String>> + Send + Sync>;

/// The [`AntRequester`] of a manager ant, which only managers have. Two compare equal when both have one or neither
/// has, so a reload does not restart a manager for a new one.
#[derive(Clone, Default)]
pub struct Requester(pub Option<AntRequester>);

impl PartialEq for Requester {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_some() == other.0.is_some()
    }
}

impl std::fmt::Debug for Requester {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "Requester(Some(..))" } else { "Requester(None)" })
    }
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RequestAnt {
    /// The new ant's name, fitting the template's name pattern (by default `<manager>-*`, such as `lead-docs`).
    name: String,
    /// The ant template to add it from.
    template: String,
    /// Why the ant is needed; a human deciding on a request outside the template's policy reads it.
    reason: String,
    /// Settings within the template's bounds, each left out taking the template's: workdir, harness, model,
    /// reasoning_effort, context, whoami, memory, tasks, capabilities, allow_sets, max_thread_tokens,
    /// max_thread_cost, max_daily_tokens, max_daily_cost, max_harnesses, prompt_timeout (such as `10m`),
    /// max_tool_calls.
    #[serde(default)]
    overrides: Option<serde_json::Map<String, serde_json::Value>>,
    /// How long to wait for the credentials, a human's approval included: a duration such as `10m` (default 15m,
    /// at most 30m).
    #[serde(default)]
    timeout: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RetireAnt {
    /// The ant to retire, one this ant added with request_ant.
    name: String,
}

impl Hub {
    /// Add the ant `args` asks for through the requester ([`AntRequester`]), then wait up to [`LISTED_WAIT`] for the
    /// daemon to serve it.
    async fn request_ant(&self, thread: &ThreadId, args: RequestAnt) -> Result<String, String> {
        let requester = self.requester.get().ok_or("request_ant is for the managers of an ant template")?;
        let name: UserId = args.name.trim().parse().map_err(|e| format!("name {:?}: {e}", args.name))?;
        let request = match args.overrides {
            Some(overrides) => {
                serde_json::from_value(serde_json::Value::Object(overrides)).map_err(|e| format!("overrides: {e}"))?
            }
            None => crate::profile::template::Request::default(),
        };
        let wait = match args.timeout.as_deref() {
            None => DEFAULT_REQUEST_WAIT,
            Some(timeout) => ask_wait(Some(timeout))?,
        };
        let ask = AntAsk {
            manager: self.ant.clone(),
            thread: thread.clone(),
            name: name.clone(),
            template: args.template.trim().to_owned(),
            request,
            reason: args.reason,
            wait,
        };
        let added = requester(ManagerAsk::Add(Box::new(ask))).await?;
        let Some(lookup) = self.lookup.get() else { return Ok(added) };
        let deadline = tokio::time::Instant::now() + LISTED_WAIT;
        loop {
            if let Ok(Ok(Some(_))) = tokio::time::timeout(LOOKUP_TIMEOUT, lookup(name.clone())).await {
                return Ok(format!("{added} {name} is in the colony: send_message and ask_ant reach it."));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(format!(
                    "{added} {name} is not in the colony yet; messages to it wait in its inbox until it is served."
                ));
            }
            tokio::time::sleep(LISTED_POLL).await;
        }
    }

    /// Retire the ant `args` names through the requester ([`AntRequester`]).
    async fn retire_ant(&self, thread: &ThreadId, args: RetireAnt) -> Result<String, String> {
        let requester = self.requester.get().ok_or("retire_ant is for the managers of an ant template")?;
        let name: UserId = args.name.trim().parse().map_err(|e| format!("name {:?}: {e}", args.name))?;
        requester(ManagerAsk::Retire(RetireAsk { manager: self.ant.clone(), thread: thread.clone(), name })).await
    }
}

#[tool_router(router = managers_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = "Add a worker ant from an ant template that lists this ant among its managers: `name` \
        fitting the template's name pattern, `reason` saying why it is needed, and `overrides` choosing settings \
        within the template's bounds. It is checked against the template here first and refused at once outside \
        it. Its key is made on this machine and its credentials signed where the bus's account key is: at once \
        within the template's policy, else once a human approves, which can take a while. Then it is listed in \
        the config file and served by this machine's daemon; it returns once the new ant is in the colony, to be \
        reached with send_message and ask_ant, or says why it was refused. Ants added this way never add ants.")]
    async fn request_ant(
        &self,
        Parameters(args): Parameters<RequestAnt>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.request_ant(&thread, args).await))
    }

    #[tool(description = "Retire a worker ant this ant added with request_ant, once its work is done: its entry \
        leaves the config file, this machine's daemon stops it once its prompts in flight end (waiting up to 5 \
        minutes, past which they are cut off), and its credentials are revoked where the bus's account key is. \
        Its home stays, memory, task database and worktrees included: a later request_ant of the same name from \
        the same template brings it back with them. A retired ant no longer counts toward the template's max_ants \
        or its pool's daily spend. Only ants this ant added can be retired.")]
    async fn retire_ant(
        &self,
        Parameters(args): Parameters<RetireAnt>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.retire_ant(&thread, args).await))
    }
}
