//! Asking a run's conductor to start, stop or resume it: a [`TrailRequest`] on the conductor's control subject, which
//! its daemon answers with a [`TrailReply`].

use ants_daemon::bus::relayed;
use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{
    RunId, TrailAction, TrailLimits, TrailOutcome, TrailReply, TrailRequest, TrailRun, Unresumable, UserId, WaitingStep,
};
use anyhow::{Result, bail};
use futures::StreamExt as _;

/// How long the CLI waits for the conductor's daemon to answer.
pub(super) const ANSWER_WAIT: Duration = Duration::from_secs(15);

/// Ask `ant`'s daemon, as human `by`, for `action`, and wait up to `wait` for its answer: refused when the daemon
/// refuses, or when none answers. A stop the daemon told pending, as the run's script computes, is
/// [`TrailOutcome::Stopping`] when it did not take effect within `wait`.
pub(super) async fn ask(
    client: &async_nats::Client,
    ant: &UserId,
    by: &UserId,
    action: TrailAction,
    wait: Duration,
) -> Result<TrailOutcome> {
    let request = TrailRequest::new(ant.clone(), by.clone(), action);
    let mut answers = relayed::subscribe(client, request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    let deadline = tokio::time::Instant::now() + wait;
    let mut pending = None;
    while let Ok(Some(message)) = tokio::time::timeout_at(deadline, answers.next()).await {
        let Ok(reply) = serde_json::from_slice::<TrailReply>(&message.payload) else { continue };
        if reply.request_id != request.id || reply.check_subject(&message.subject).is_err() {
            continue;
        }
        match reply.outcome {
            TrailOutcome::Refused { error } => bail!("{ant} refused:{}", crate::text::after_colon(&error)),
            stopping @ TrailOutcome::Stopping { .. } => pending = Some(stopping),
            outcome => return Ok(outcome),
        }
    }
    if let Some(stopping) = pending {
        return Ok(stopping);
    }
    if let TrailAction::Start { .. } = request.action {
        bail!(
            "no daemon serving {ant} answered within {}s; a run it starts later is run {}, which `ants trail show \
             {}` shows",
            wait.as_secs(),
            request.id,
            request.id
        );
    }
    bail!("no daemon serving {ant} answered within {}s", wait.as_secs())
}

/// Stop `run` as human `by`; what `ants trail stop` prints.
pub(crate) async fn stop(client: &async_nats::Client, by: &UserId, run: &TrailRun) -> Result<String> {
    stop_within(client, by, run, ANSWER_WAIT).await
}

/// Stop `run` as [`stop`] does, waiting at most `wait` for the conductor's answer.
pub(super) async fn stop_within(
    client: &async_nats::Client,
    by: &UserId,
    run: &TrailRun,
    wait: Duration,
) -> Result<String> {
    if run.finished() {
        bail!("run {} has ended: {}", run.run, run.state.name());
    }
    let action = TrailAction::Stop { run: run.run.clone() };
    match ask(client, &run.conductor, by, action, wait).await? {
        TrailOutcome::Stopped { waiting, .. } => Ok(stopped(&run.run, &run.conductor, &waiting)),
        TrailOutcome::Stopping { .. } => Ok(stopping(&run.run, &run.conductor)),
        other => bail!("{} answered {other:?}", run.conductor),
    }
}

/// What `ants trail stop` prints of `run`, whose stop `conductor` applies once the script's pass ends.
pub(super) fn stopping(run: &RunId, conductor: &UserId) -> String {
    format!(
        "stop of run {run} pending on {conductor}: its script computes, and the run stops once that pass ends; `ants \
         trail stop {run}` then lists the steps still to withdraw\n"
    )
}

/// What `ants trail stop` prints of `run`, stopped by `conductor` with `waiting` steps.
pub(super) fn stopped(run: &RunId, conductor: &UserId, waiting: &[WaitingStep]) -> String {
    let mut out = format!(
        "stopped run {run} on {conductor}: no further step goes out until `ants trail resume {run}`, which sends the \
         steps waited on anew\n"
    );
    if !waiting.is_empty() {
        out.push_str("steps still waiting in the queues of other ants (withdrawing one not started yet):\n");
        for step in waiting {
            let _ = writeln!(out, "  {} message {}: ants withdraw {} {}", step.to, step.message, step.to, step.message);
        }
    }
    out
}

/// How `run` ended with nothing to resume ([`Unresumable::ended`]): done, or failed on no limit.
pub(crate) fn ended(run: &TrailRun) -> Option<&'static str> {
    match run.state.resumable(&run.limits, None) {
        Err(Unresumable::Done) => Some("done"),
        Err(Unresumable::Failed(_)) => Some("failed, not on a limit"),
        _ => None,
    }
}

/// Refuse to resume `run` when its record shows it ended with nothing to resume ([`ended`]).
pub(crate) fn resumable(run: &TrailRun) -> Result<()> {
    match ended(run) {
        Some(ended) => bail!("run {} has ended: {ended}", run.run),
        None => Ok(()),
    }
}

/// Resume `run` as human `by`, under `limits` when given; what `ants trail resume` prints. Refused at once for a run
/// ended with nothing to resume.
pub(crate) async fn resume(
    client: &async_nats::Client,
    by: &UserId,
    run: &TrailRun,
    limits: Option<TrailLimits>,
) -> Result<String> {
    resumable(run)?;
    let action = TrailAction::Resume { run: run.run.clone(), limits };
    match ask(client, &run.conductor, by, action, ANSWER_WAIT).await? {
        TrailOutcome::Resumed { run: resumed } => {
            let mut out = format!("resumed run {resumed} on {}", run.conductor);
            if let Some(limits) = limits {
                let _ = write!(out, " under {}", super::start::limits_line(&limits));
            }
            Ok(out)
        }
        other => bail!("{} answered {other:?}", run.conductor),
    }
}
