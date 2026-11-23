//! The trail runs an ant conducts ([`crate::trail`]): `run_trail` starts one on the current thread, its owner the run's;
//! `trail_status` reads how one stands, or lists those not ended; `stop_trail` stops one. An ant reaches only the runs
//! it conducts, which its daemon keeps under its own keys.

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{
    Envelope, RunId, RunState, SpendsFrom, ThreadId, TrailFunction, TrailName, TrailRun, TrailSource, TrailSpent,
};
use chrono::{DateTime, Utc};
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
use crate::trail::{AskedLimits, RunStatus, StartRun, Trails, rating_hint};

/// Most saved trails a run of a script points to ([`well_rated`]).
const WELL_RATED: usize = 3;
/// Most characters of a result `trail_status` shows.
const MAX_RESULT: usize = 4000;
/// Why the trail tools are refused when the ant conducts no runs.
const NO_TRAILS: &str = "this ant conducts no trails: the bus keeps no trail runs";

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
    /// What the run may spend; each limit left out takes its default, and one over its maximum is refused.
    #[serde(default)]
    limits: Option<AskedLimits>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TrailStatus {
    /// The run's id, as `run_trail` returned it; without it, the runs this ant conducts that have not ended.
    #[serde(default)]
    run: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct StopTrail {
    /// The run's id, as `run_trail` returned it.
    run: String,
}

/// What `run_trail` teaches: how to call it, then how to write a trail.
fn description() -> String {
    format!(
        "Start a trail run conducted by this ant: a saved trail by `name`, or a `script` of its own (refused on a \
         thread a webhook started), with `args` as its JSON input and `limits` raising or lowering what it may spend \
         (`steps`, `at_once`, `turns`, `wall`). The run reports on the current thread and answers to the thread's \
         owner, who gets its end there. Returns the run id at once, or with `wait` the run's result or error (waiting \
         at most `timeout`). Started while answering a trail step, the run spends from the step's run: its limits are \
         cut to what the step's grant leaves after the runs already started answering it, what it spends counts \
         against that run, and a trail on that run's route is \
         refused, as no trail calls itself. trail_status tells how a run stands and stop_trail stops one. Before writing a script, \
         find_trails finds the saved trails for the task; once a run ends, rate_trail rates it, as its end invites. \
         Once an inline script works and its steps will recur, save_trail saves it for every ant to run by name. Write \
         a trail for work \
         whose steps are known in code: fan-outs, pipelines, verification rounds, routes through saved trails.\n\n{}",
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
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let trail = match (args.name, args.script) {
            (Some(name), None) => {
                TrailSource::Saved(name.trim().parse::<TrailName>().map_err(|e| format!("name {name:?}: {e}"))?)
            }
            (None, Some(script)) => TrailSource::Script(script),
            _ => return Err("give either `name` or `script`".to_owned()),
        };
        let wait = if args.wait { Some(ask_wait(args.timeout.as_deref())?) } else { None };
        let limits = args.limits.unwrap_or_default().limits().map_err(|e| format!("limits: {e}"))?;
        let current = self.sessions.locked().current.get(thread).cloned();
        let current = current.ok_or("no message is being handled on this thread; runs start only while one is")?;
        let spends_from = spending(&current, &trail)?;
        let stored = self.origin_of(thread).await.map_err(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's origin for a trail run");
            "the thread's origin cannot be read".to_owned()
        })?;
        let origin = settled(stored, origin(&current, None, &self.humans.humans()));
        let described = match &trail {
            TrailSource::Script(script) => crate::trail::script_description(script),
            TrailSource::Saved(_) => None,
        };
        let spends = async |run: &RunId| {
            let Some(from) = &spends_from else { return String::new() };
            let limits = match trails.run(run).await {
                Ok(Some(header)) => format!("its limits are {}", header.limits),
                _ => "its limits are cut to the step's grant".to_owned(),
            };
            format!(
                " It spends from run {} of {}, whose step this thread answers: {limits}, within what the step's grant \
                 leaves, and what it spends counts against that run.",
                from.run, from.conductor
            )
        };
        let start = StartRun {
            trail,
            args: args.args,
            owner: current.owner.clone(),
            origin,
            thread: thread.clone(),
            limits,
            spends_from: spends_from.clone(),
        };
        let Some(wait) = wait else {
            let run = trails.start(start).await?;
            let spends = spends(&run).await;
            tracing::info!(%thread, %run, "trail run started from a tool call");
            let started = format!(
                "Started run {run} on thread {thread}; its end goes to the thread's owner there, and invites this \
                 ant's rating of it.{spends}"
            );
            return Ok(format!("{started}{}", well_rated(trails, described.as_deref()).await));
        };
        let mut waiting = trails.start_waited(start).await?;
        let run = waiting.run().clone();
        tracing::info!(%thread, %run, "trail run started from a tool call");
        let spends = spends(&run).await;
        let started =
            format!("Started run {run} on thread {thread}; its end goes to the thread's owner there.{spends}");
        let suggested = well_rated(trails, described.as_deref()).await;
        let unended = |why: String| format!("{started} {why} Its end invites this ant's rating of it.{suggested}");
        let ended = tokio::select! {
            biased;
            ended = waiting.ended() => ended,
            () = tokio::time::sleep(wait) => {
                return Ok(unended(format!("It had not ended after {}.", ants_proto::duration::format(wait))));
            }
            () = cancelled => return Ok(unended("The wait was cancelled.".to_owned())),
        };
        let Some(header) = ended else { return Ok(unended("Its end cannot be waited on.".to_owned())) };
        Ok(match &header.state {
            RunState::Done { result } => {
                format!("Run {run} is done. Result:\n{result}\n{}{suggested}", rating_hint(&header))
            }
            RunState::Failed { error, .. } => format!("Run {run} failed: {error}\n{}{suggested}", rating_hint(&header)),
            RunState::Stopped { by } => format!("Run {run} was stopped by {by}.{suggested}"),
            RunState::Running | RunState::Waiting => unended("It is still running.".to_owned()),
        })
    }

    /// How run `run` stands, or without one the runs the ant conducts that have not ended.
    async fn trail_status(&self, run: Option<&str>) -> Result<String, String> {
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let now = Utc::now();
        let Some(run) = run else { return Ok(listing(&trails.unfinished(), now)) };
        Ok(status_text(&trails.status(&run_id(run)?).await?, now))
    }

    /// Stop run `run` as its conductor, as a human's request to it does ([`Trails::stop`]).
    async fn stop_trail(&self, run: &str) -> Result<String, String> {
        let trails = self.trails.get().ok_or(NO_TRAILS)?;
        let run = run_id(run)?;
        let waiting = trails.stop(&run, &self.ant).await?;
        tracing::info!(%run, "trail run stopped from a tool call");
        let mut out = format!(
            "Stopped run {run}: it sends no further step, and the replies of the steps it waited on answer nothing, \
             until a human resumes it (`ants trail resume {run}`), which sends those steps anew."
        );
        if !waiting.is_empty() {
            let steps: Vec<String> =
                waiting.iter().map(|step| format!("{} message {}", step.to, step.message)).collect();
            let _ = write!(
                out,
                " Steps still in the queues of other ants, which only a human withdraws (`ants withdraw ANT MESSAGE`): \
                 {}.",
                steps.join(", ")
            );
        }
        Ok(out)
    }
}

/// The step a run started with `trail` on a thread handling `current` spends from: `current` when it is a step telling
/// its grant, whose daemon cuts the run's limits to what the grant leaves; refused for a trail on the step's route.
fn spending(current: &Envelope, trail: &TrailSource) -> Result<Option<SpendsFrom>, String> {
    let Some(step) = current.trail_step.as_deref() else { return Ok(None) };
    let Some(bounds) = &step.bounds else { return Ok(None) };
    if let TrailSource::Saved(name) = trail
        && let Some(refused) = crate::trail::refused_on_route(name, step, bounds)
    {
        return Err(refused);
    }
    Ok(Some(SpendsFrom {
        conductor: current.from.clone(),
        run: step.run.clone(),
        call: step.call,
        bounds: bounds.clone(),
    }))
}

impl Hub {
    /// What `reply`, a message going out while a message `current` is handled, tells the runs started answering
    /// `current` spent in all: something only when `current` is a trail step the ant started runs answering and `reply`
    /// goes back to its conductor on the step's thread.
    pub(super) async fn trail_spent(&self, current: Option<&Envelope>, reply: &Envelope) -> Option<TrailSpent> {
        let (current, trails) = (current?, self.trails.get()?);
        let step = current.trail_step.as_deref()?;
        let answers = reply.thread_id == current.thread_id
            && reply.to == current.from
            && reply.trail_step.is_none()
            && reply.notice.is_none();
        if !answers {
            return None;
        }
        trails.spent_answering(&current.from, &current.thread_id, step).await
    }
}

impl super::Mcp {
    /// What `reply`, the final answer to `incoming`, tells the runs started answering it, a trail step, spent
    /// (`Hub::trail_spent`).
    pub async fn trail_spent(&self, incoming: &Envelope, reply: &Envelope) -> Option<TrailSpent> {
        self.hub.trail_spent(Some(incoming), reply).await
    }
}

fn run_id(run: &str) -> Result<RunId, String> {
    run.trim().parse().map_err(|e| format!("run {run:?}: {e}"))
}

/// The run's name: the trail it started, or an inline script.
fn named(header: &TrailRun) -> String {
    match header.first_trail() {
        Some(trail) => format!("Run {} of trail {trail}", header.run),
        None => format!("Run {} of an inline script", header.run),
    }
}

fn state(header: &TrailRun) -> String {
    match &header.state {
        RunState::Waiting => "waiting on humans".to_owned(),
        RunState::Stopped { by } => format!("stopped by {by}"),
        RunState::Failed { error, limit: Some(limit) } => format!("failed on its {limit} limit: {error}"),
        RunState::Failed { error, limit: None } => format!("failed: {error}"),
        other @ (RunState::Running | RunState::Done { .. }) => other.name().to_owned(),
    }
}

/// How long `header`'s run has run, stopped time included, to the second.
fn ran(header: &TrailRun, now: DateTime<Utc>) -> String {
    let ran = (header.ended_at.unwrap_or(now) - header.started_at).to_std().unwrap_or_default();
    ants_proto::duration::format(Duration::from_secs(ran.as_secs()))
}

/// What `trail_status` tells of one run: its state and phase, what it spent of its limits, the steps it waits on and
/// its result.
fn status_text(status: &RunStatus, now: DateTime<Utc>) -> String {
    let RunStatus { header, waiting, queued } = status;
    let limits = &header.limits;
    let mut out = format!("{}: {}.", named(header), state(header));
    if let Some(phase) = &header.phase {
        let _ = write!(out, " Phase: {phase}.");
    }
    let _ = write!(
        out,
        "\nSpent {} of {} steps, {} of {} turns and {} of {} wall time; at most {} steps wait at once.",
        header.steps,
        limits.steps,
        header.turns,
        limits.turns,
        ran(header, now),
        ants_proto::duration::format(limits.wall_time()),
        limits.concurrency,
    );
    if !waiting.is_empty() {
        let steps: Vec<String> = waiting
            .iter()
            .map(|step| {
                let whom = if step.function == TrailFunction::Human { "human" } else { "ant" };
                match &step.label {
                    Some(label) => format!("call {} to {whom} {} ({label})", step.call, step.to),
                    None => format!("call {} to {whom} {}", step.call, step.to),
                }
            })
            .collect();
        let stopped = matches!(header.state, RunState::Stopped { .. });
        let what = if stopped { "To send anew on resuming" } else { "Waiting on" };
        let _ = write!(out, "\n{what}: {}.", steps.join(", "));
    }
    if *queued > 0 {
        let _ = write!(out, "\n{queued} more calls of the script wait for room to go out.");
    }
    let owner = header.owner.as_ref().map_or_else(|| "no one".to_owned(), ToString::to_string);
    let _ = write!(
        out,
        "\nStarted {} ago, reporting on thread {}, owned by {owner}.",
        ran_since(header, now),
        header.thread
    );
    if let Some(from) = &header.spends_from {
        let _ = write!(
            out,
            "\nStarted answering call {} of run {} of {}: it spends from that run, within the step's grant, and tells \
             that run what it spent with each reply to the step and as it ends or stops.",
            from.call, from.run, from.conductor
        );
    }
    if let RunState::Stopped { .. } = header.state {
        let _ = write!(out, "\nA human resumes it with `ants trail resume {}`.", header.run);
    }
    if let RunState::Done { result } = &header.state {
        let result = match result {
            serde_json::Value::String(text) => text.clone(),
            result => result.to_string(),
        };
        let shown: String = result.chars().take(MAX_RESULT).collect();
        let cut = if shown.len() < result.len() { "…" } else { "" };
        let _ = write!(out, "\nResult:\n{shown}{cut}");
    }
    if header.finished() {
        let _ = write!(out, "\n{}", rating_hint(header));
    }
    out
}

/// How long ago `header`'s run started, to the second.
fn ran_since(header: &TrailRun, now: DateTime<Utc>) -> String {
    let since = (now - header.started_at).to_std().unwrap_or_default();
    ants_proto::duration::format(Duration::from_secs(since.as_secs()))
}

/// What `trail_status` tells of the runs not ended: a line each.
fn listing(runs: &[TrailRun], now: DateTime<Utc>) -> String {
    if runs.is_empty() {
        return "This ant conducts no run that has not ended.".to_owned();
    }
    let mut out = "Runs this ant conducts that have not ended:".to_owned();
    for header in runs {
        let _ = write!(
            out,
            "\n- {}: {}; {} of {} steps, {} of {} turns; started {} ago",
            named(header),
            state(header),
            header.steps,
            header.limits.steps,
            header.turns,
            header.limits.turns,
            ran_since(header, now),
        );
    }
    out
}

/// The line naming the saved trails with good records for tasks as `described`, a script's description, best effort:
/// nothing without a description, a trail matching it or one reading.
async fn well_rated(trails: &Trails, described: Option<&str>) -> String {
    let Some(described) = described else { return String::new() };
    let found = match trails.well_rated(described, WELL_RATED).await {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!(error = e, "finding the saved trails with good records for a script");
            return String::new();
        }
    };
    if found.is_empty() {
        return String::new();
    }
    let named: Vec<String> = found.iter().map(|(trail, counts)| format!("{trail} ({counts})")).collect();
    format!("\nSaved trails with good records for such tasks: {}; run_trail(name: …) runs one.", named.join(", "))
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

    #[tool(
        description = "Tell how a trail run this ant conducts stands: its state and phase, the steps and turns it spent \
                       of its limits and its wall time, the steps it waits on, and its result or error once over. \
                       Without `run`, list the runs this ant conducts that have not ended. Runs of other conductors \
                       are not this ant's to read."
    )]
    async fn trail_status(
        &self,
        Parameters(args): Parameters<TrailStatus>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.thread(&cx)?;
        Ok(text(self.hub.trail_status(args.run.as_deref()).await))
    }

    #[tool(
        description = "Stop a trail run this ant conducts, by its id: it sends no further step and the replies of the \
                       steps it waited on answer nothing, while its wall time runs on. Only a human resumes it. Steps \
                       to other ants stay in their queues, listed for a human to withdraw. Runs of other conductors are \
                       not this ant's to stop."
    )]
    async fn stop_trail(
        &self,
        Parameters(args): Parameters<StopTrail>,
        cx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        self.thread(&cx)?;
        Ok(text(self.hub.stop_trail(&args.run).await))
    }
}
