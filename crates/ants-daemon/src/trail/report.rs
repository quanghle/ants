//! Where a run reports what it does, on its own thread: its start, what its script reports (phase(), log()) and what
//! arrives on its steps' threads answering no call, as thread notes of the conductor; each step starting and ending, as
//! status events of the conductor on the run's thread ([`StatusUpdate::Step`]); and its end, as a message to its
//! owner, or, for a child run, whose parent reports, as a note. Reports are best effort: one that fails is logged,
//! never failing the run.

use std::fmt::Write as _;

use ants_proto::{
    DEFAULT_TURN_BUDGET, Envelope, MAX_TITLE, NotesPost, PendingCall, RunId, RunState, StatusUpdate, StepStage,
    ThreadId, TrailFunction, TrailRun, UserId,
};
use ants_trail::{Answer, Event, Meta};

use super::{Trails, steps};

/// Reads of busy notes before a report to them is given up.
const NOTE_TRIES: usize = 5;
/// Most characters of one note, and of a message quoted in one.
const MAX_NOTE: usize = 4000;
const MAX_QUOTE: usize = 1000;
/// Most characters of a result the end's message shows.
const MAX_RESULT: usize = 8000;

impl Trails {
    /// Tell the start of `run`, a run of the trail `meta` describes, in the notes of its thread, suggesting a title
    /// for the thread.
    pub(super) async fn report_start(&self, run: &TrailRun, meta: &Meta) {
        let answering = run
            .parent
            .as_ref()
            .map_or_else(String::new, |parent| format!(", answering call {} of run {}", parent.call, parent.run));
        let text = format!("Run {} of trail {} started{answering}: {}", run.run, meta.name, meta.description);
        let title: String = format!("trail {}: {}", meta.name, meta.description)
            .chars()
            .map(|c| if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') { ' ' } else { c })
            .collect();
        self.note(run, &text, Some(clip(title.trim(), MAX_TITLE))).await;
    }

    pub(super) async fn report_resumed(&self, run: &TrailRun, by: &UserId) {
        self.note(run, &format!("Run {} resumed by {by}.", run.run), None).await;
    }

    /// Tell what the script reported in one resume, in one note.
    pub(super) async fn report_events(&self, run: &TrailRun, events: &[Event]) {
        for event in events {
            match event {
                Event::Phase(phase) => tracing::info!(run = %run.run, phase, "trail phase"),
                Event::Log(text) => tracing::debug!(run = %run.run, text, "trail log"),
            }
        }
        if events.is_empty() {
            return;
        }
        let mut text = String::new();
        for (at, event) in events.iter().enumerate() {
            let line = match event {
                Event::Phase(phase) => format!("phase: {phase}"),
                Event::Log(log) => format!("log: {log}"),
            };
            if text.chars().count() + line.chars().count() > MAX_NOTE {
                let _ = write!(text, "… {} more", events.len() - at);
                break;
            }
            text.push_str(&line);
            text.push('\n');
        }
        self.note(run, text.trim_end(), None).await;
    }

    /// Tell `step` of `run` at `stage` on the run's thread.
    pub(super) fn report_step(&self, run: &TrailRun, step: &PendingCall, stage: StepStage) {
        tracing::info!(run = %run.run, call = step.call, to = %step.to, thread = %step.thread, stage = stage.as_str(),
            "trail step");
        let label = match step.function {
            TrailFunction::Agent => steps::AgentArgs::of(&step.args).ok().and_then(|args| args.label),
            TrailFunction::Trail => steps::TrailArgs::of(&step.args).ok().map(|args| format!("trail {}", args.name)),
            TrailFunction::Human => None,
        };
        let update = StatusUpdate::Step {
            run: run.run.clone(),
            call: step.call,
            label,
            to: step.to.clone(),
            step_thread: step.thread.clone(),
            stage,
        };
        crate::monitoring::status::publish(&self.inner.client, &self.inner.ant, &run.thread, update);
    }

    /// Tell `step` answered with `answer`.
    pub(super) fn report_answered(&self, run: &TrailRun, step: &PendingCall, answer: &Answer) {
        let stage = if matches!(answer, Answer::Error(_)) { StepStage::Failed } else { StepStage::Answered };
        self.report_step(run, step, stage);
    }

    /// Tell `message`, which arrived on a step's thread and answers no call, in the notes of the run's thread.
    pub(super) async fn report_stray(&self, stray: Stray<'_>, message: &Envelope) {
        let Stray { run, run_thread, call, to, waited } = stray;
        let what = match (&message.notice, &message.escalation) {
            (Some(kind), _) => format!("A {} notice", kind.as_str()),
            (None, Some(escalation)) => format!("A {} escalation", escalation.reason.as_str()),
            (None, None) => "A reply".to_owned(),
        };
        let from = &message.from;
        let thread = &message.thread_id;
        let step = call.map_or_else(|| "a step".to_owned(), |call| format!("step {call}"));
        let state = if waited { "" } else { ", which the run no longer waits on" };
        let text = format!(
            "{what} from {from} on the thread {thread} of {step} to {to}{state}, run {run}:\n{}",
            clip(&message.text, MAX_QUOTE)
        );
        tracing::info!(%run, call, %from, %thread, "a message answering no trail call, reported");
        self.note_on(run, run_thread, &text, None).await;
    }

    /// Tell the end of `run` to its owner, else the default human, on the run's thread: its result, its error, or who
    /// stopped it; a child run's in the notes of its thread, its parent telling the owner.
    pub(super) async fn report_end(&self, run: &TrailRun) {
        tracing::info!(run = %run.run, state = ?run.state, steps = run.steps, turns = run.turns, "trail run ended");
        let Some(text) = end_text(run) else { return };
        if run.parent.is_some() {
            return self.note(run, &clip(&text, MAX_NOTE), None).await;
        }
        let to = self.inner.humans.owner_or_default(run.owner.as_ref());
        let message =
            Envelope::on_thread(run.thread.clone(), self.inner.ant.clone(), to, text).with_owner(run.owner.clone());
        if let Err(e) = (self.inner.outbox)(message).await {
            tracing::warn!(run = %run.run, error = format!("{e:#}"), "telling the end of a trail run");
        }
    }

    async fn note(&self, run: &TrailRun, text: &str, title: Option<String>) {
        self.note_on(&run.run, &run.thread, text, title).await;
    }

    /// Post `text` to the notes of the thread of `run` with the turns they have left: a report spends none.
    async fn note_on(&self, run: &RunId, thread: &ThreadId, text: &str, title: Option<String>) {
        let notes = &self.inner.notes;
        for _ in 0..NOTE_TRIES {
            let read = match notes.read(thread).await {
                Ok(read) => read,
                Err(e) => return tracing::warn!(%run, error = format!("{e:#}"), "reading the run's notes"),
            };
            let mut post = NotesPost::new(thread.clone(), self.inner.ant.clone(), text);
            post.turn_budget = read.last().map_or(DEFAULT_TURN_BUDGET, |last| last.turn_budget);
            post.title.clone_from(&title);
            match notes.post(&post, read.last_seq).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(e) => return tracing::warn!(%run, error = format!("{e:#}"), "posting to the run's notes"),
            }
        }
        tracing::warn!(%run, "the run's notes stayed busy; a report to them is dropped");
    }
}

/// A message on a step's thread answering no call, as [`Trails::report_stray`] tells it.
pub(super) struct Stray<'a> {
    pub(super) run: &'a RunId,
    pub(super) run_thread: &'a ThreadId,
    /// The step's call, when known.
    pub(super) call: Option<u32>,
    pub(super) to: &'a UserId,
    /// Whether the run still waits on the call.
    pub(super) waited: bool,
}

/// The message telling the end of `run`; None while it runs.
fn end_text(run: &TrailRun) -> Option<String> {
    let name = match &run.trail {
        Some(trail) => format!("Run {} of trail {trail}", run.run),
        None => format!("Run {}", run.run),
    };
    let spent = format!("{} steps, {} turns", run.steps, run.turns);
    let resume = format!("`ants trail resume {}` continues it", run.run);
    Some(match &run.state {
        RunState::Running | RunState::Waiting => return None,
        RunState::Stopped { by } => format!("{name} was stopped by {by} ({spent}); {resume}."),
        RunState::Failed { error } if error.contains("limit") => {
            format!("{name} failed ({spent}): {error}\n{resume}, under raised limits.")
        }
        RunState::Failed { error } => format!("{name} failed ({spent}): {error}"),
        RunState::Done { result } => {
            let (text, fence) = match result {
                serde_json::Value::Null => return Some(format!("{name} is done ({spent}).")),
                serde_json::Value::String(text) => (text.clone(), false),
                result => (serde_json::to_string_pretty(result).unwrap_or_else(|_| result.to_string()), true),
            };
            let mut shown = clip(&text, MAX_RESULT);
            if fence {
                shown = format!("```json\n{shown}\n```");
            }
            if text.chars().count() > MAX_RESULT {
                let _ = write!(shown, "\n`ants trail show {}` shows the whole result.", run.run);
            }
            format!("{name} is done ({spent}):\n\n{shown}")
        }
    })
}

/// `text`, cut to at most `max` characters, the last an ellipsis when cut.
fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut clipped: String = text.chars().take(max.saturating_sub(1)).collect();
    clipped.push('…');
    clipped
}
