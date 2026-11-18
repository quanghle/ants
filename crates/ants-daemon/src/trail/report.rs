//! Where a run reports what it does: each step sent and answered, what the script reports, and the run's end. Only
//! logged for now; the run's thread is where these are to show.

use ants_proto::{PendingCall, TrailRun};
use ants_trail::{Answer, Event};

pub(super) fn sent(run: &TrailRun, step: &PendingCall) {
    tracing::info!(run = %run.run, call = step.call, to = %step.to, thread = %step.thread, "trail step sent");
}

pub(super) fn answered(run: &TrailRun, step: &PendingCall, answer: &Answer) {
    let failed = matches!(answer, Answer::Error(_));
    tracing::info!(run = %run.run, call = step.call, from = %step.to, failed, "trail step answered");
}

pub(super) fn reported(run: &TrailRun, event: &Event) {
    match event {
        Event::Phase(phase) => tracing::info!(run = %run.run, phase, "trail phase"),
        Event::Log(text) => tracing::debug!(run = %run.run, text, "trail log"),
    }
}

pub(super) fn ended(run: &TrailRun) {
    tracing::info!(run = %run.run, state = ?run.state, steps = run.steps, turns = run.turns, "trail run ended");
}
