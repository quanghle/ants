//! `run_trail`, starting a trail run the ant conducts ([`crate::trail`]) on the current thread, its owner the run's.

use std::time::Duration;

use ants_proto::{RunId, RunState, ThreadId, TrailLimits, TrailName, TrailSource};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, schemars, tool, tool_router};
use serde::Deserialize;
use serde_json::Value;

use super::Hub;
use super::messages::ask_wait;
use super::tools::{Tools, text};
use crate::provider::context::{origin, settled};
use crate::sync::MutexExt as _;
use crate::trail::StartRun;

/// How often a waiting call looks for the run's end.
const POLL: Duration = Duration::from_millis(500);

#[derive(Deserialize, schemars::JsonSchema)]
struct RunTrail {
    /// A saved trail's name; give it or `script`.
    #[serde(default)]
    name: Option<String>,
    /// A trail script to run once, starting with its `meta` literal; give it or `name`.
    #[serde(default)]
    script: Option<String>,
    /// The run's JSON input, the script's `args`.
    #[serde(default)]
    args: Value,
    /// Wait for the run's end and return its result or error; by default the call returns once the run starts.
    #[serde(default)]
    wait: bool,
    /// How long `wait` waits, such as `90s` or `10m`: default `5m`, at most `30m`.
    #[serde(default)]
    timeout: Option<String>,
}

/// What `run_trail` teaches: how to call it, then how to write a trail.
fn description() -> String {
    format!(
        "Start a trail run conducted by this ant: a saved trail by `name`, or a `script` of its own (refused on a \
         thread a webhook started), with `args` as its JSON input. The run reports on the current thread and answers \
         to the thread's owner, who gets its end there. Returns the run id at once, or with `wait` the run's result \
         or error (waiting at most `timeout`). Write a trail for work whose steps are known in code: fan-outs, \
         pipelines, verification rounds.\n\n{}",
        crate::trail::GUIDE
    )
}

impl Hub {
    /// Start the run `args` asks for on `thread`, and wait for its end when they say so.
    async fn run_trail(
        &self,
        thread: &ThreadId,
        args: RunTrail,
        cancelled: impl Future<Output = ()>,
    ) -> Result<String, String> {
        let trails = self.trails.get().ok_or("this ant conducts no trails: the bus keeps no trail runs")?;
        let trail = match (args.name, args.script) {
            (Some(name), None) => {
                TrailSource::Saved(name.trim().parse::<TrailName>().map_err(|e| format!("name {name:?}: {e}"))?)
            }
            (None, Some(script)) => TrailSource::Script(script),
            _ => return Err("give either `name` or `script`".to_owned()),
        };
        let wait = if args.wait { Some(ask_wait(args.timeout.as_deref())?) } else { None };
        let current = self.sessions.locked().current.get(thread).cloned();
        let current = current.ok_or("no message is being handled on this thread; runs start only while one is")?;
        let stored = self.origin_of(thread).await.map_err(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's origin for a trail run");
            "the thread's origin cannot be read".to_owned()
        })?;
        let origin = settled(stored, origin(&current, None, &self.humans.humans()));
        let start = StartRun {
            trail,
            args: args.args,
            owner: current.owner.clone(),
            origin,
            thread: thread.clone(),
            limits: TrailLimits::default(),
        };
        let run = trails.start(start).await?;
        tracing::info!(%thread, %run, "trail run started from a tool call");
        let started = format!("Started run {run} on thread {thread}; its end goes to the thread's owner there.");
        let Some(wait) = wait else { return Ok(started) };
        let ended = tokio::select! {
            ended = ended(trails, &run) => ended?,
            () = tokio::time::sleep(wait) => return Ok(format!("{started} It had not ended after {}.", ants_proto::duration::format(wait))),
            () = cancelled => return Ok(format!("{started} The wait was cancelled.")),
        };
        Ok(match ended {
            RunState::Done { result } => format!("Run {run} is done. Result:\n{result}"),
            RunState::Failed { error } => format!("Run {run} failed: {error}"),
            RunState::Stopped { by } => format!("Run {run} was stopped by {by}."),
            RunState::Running | RunState::Waiting => format!("{started} It is still running."),
        })
    }
}

/// The state `run` ended in, once it ends or is stopped.
async fn ended(trails: &crate::trail::Trails, run: &RunId) -> Result<RunState, String> {
    loop {
        let header = trails.run(run).await?.ok_or_else(|| format!("run {run} is gone"))?;
        if header.state.finished() || matches!(header.state, RunState::Stopped { .. }) {
            return Ok(header.state);
        }
        tokio::time::sleep(POLL).await;
    }
}

#[tool_router(router = trails_router, vis = "pub(super)")]
impl Tools {
    #[tool(description = description())]
    async fn run_trail(
        &self,
        Parameters(args): Parameters<RunTrail>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let thread = self.thread(&cx)?;
        Ok(text(self.hub.run_trail(&thread, args, cx.ct.cancelled()).await))
    }
}
