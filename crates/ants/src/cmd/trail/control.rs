//! Asking a run's conductor to start, stop or resume it: a [`TrailRequest`] on the conductor's control subject, which
//! its daemon answers with a [`TrailReply`].

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{
    RunId, TrailAction, TrailLimits, TrailOutcome, TrailReply, TrailRequest, TrailRun, UserId, WaitingStep,
};
use anyhow::{Result, bail};
use futures::StreamExt as _;

/// How long the CLI waits for the conductor's daemon to answer.
pub(super) const ANSWER_WAIT: Duration = Duration::from_secs(15);

/// Ask `ant`'s daemon, as human `by`, for `action`, and wait up to `wait` for its answer: refused when the daemon
/// refuses, or when none answers.
pub(super) async fn ask(
    client: &async_nats::Client,
    ant: &UserId,
    by: &UserId,
    action: TrailAction,
    wait: Duration,
) -> Result<TrailOutcome> {
    let request = TrailRequest::new(ant.clone(), by.clone(), action);
    let mut answers = client.subscribe(request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    let deadline = tokio::time::Instant::now() + wait;
    while let Ok(Some(message)) = tokio::time::timeout_at(deadline, answers.next()).await {
        let Ok(reply) = serde_json::from_slice::<TrailReply>(&message.payload) else { continue };
        if reply.request_id != request.id || reply.check_subject(&message.subject).is_err() {
            continue;
        }
        if let TrailOutcome::Refused { error } = reply.outcome {
            bail!("{ant} refused: {error}");
        }
        return Ok(reply.outcome);
    }
    bail!("no daemon serving {ant} answered within {}s", wait.as_secs())
}

/// Stop `run` as human `by`; what `ants trail stop` prints.
pub(crate) async fn stop(client: &async_nats::Client, by: &UserId, run: &TrailRun) -> Result<String> {
    if run.finished() {
        bail!("run {} has ended: {}", run.run, super::runs::state(&run.state));
    }
    let action = TrailAction::Stop { run: run.run.clone() };
    match ask(client, &run.conductor, by, action, ANSWER_WAIT).await? {
        TrailOutcome::Stopped { waiting, .. } => Ok(stopped(&run.run, &run.conductor, &waiting)),
        other => bail!("{} answered {other:?}", run.conductor),
    }
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

/// Resume `run` as human `by`, under `limits` when given; what `ants trail resume` prints.
pub(crate) async fn resume(
    client: &async_nats::Client,
    by: &UserId,
    run: &TrailRun,
    limits: Option<TrailLimits>,
) -> Result<String> {
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
