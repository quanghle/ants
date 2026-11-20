//! Where a run reports what it does, on its own thread: its start, what its script reports (phase(), log()), its route
//! at each goto() and what arrives on its steps' threads answering no call, as thread notes of the conductor; each step
//! starting and ending and each leg starting, as status events of the conductor on the run's thread
//! ([`StatusUpdate::Step`], [`StatusUpdate::Leg`]); and its end, as a message to its owner, or, for a child run, whose
//! parent reports, as a note. Reports are best effort: one that fails is logged, never failing the run.

use std::fmt::Write as _;

use ants_proto::{
    CallAnswer, DEFAULT_TURN_BUDGET, Envelope, MAX_TITLE, NotesPost, PendingCall, RunId, RunLimit, RunState,
    StatusUpdate, StepStage, ThreadId, TrailLimits, TrailRun, UserId,
};
use ants_trail::{Event, Meta};

use super::Trails;
use super::invite::end_id;

/// Reads of busy notes before a report to them is given up.
const NOTE_TRIES: usize = 5;
/// Most characters of one note, and of a message quoted in one.
const MAX_NOTE: usize = 4000;
const MAX_QUOTE: usize = 1000;
/// Most characters of a result the end's message shows.
const MAX_RESULT: usize = 8000;

impl Trails {
    /// Tell the start of `run`, whose script `meta` describes, in the notes of its thread, suggesting a title for the
    /// thread: the run named by the trail it started as, else as an inline script, the meta's name told when it differs.
    pub(super) async fn report_start(&self, run: &TrailRun, meta: &Meta) {
        let answering = run
            .parent
            .as_ref()
            .map_or_else(String::new, |parent| format!(", answering call {} of run {}", parent.call, parent.run));
        let (what, title) = match run.first_trail() {
            Some(trail) if trail.as_str() == meta.name => (format!("trail {trail}"), format!("trail {trail}")),
            Some(trail) => (format!("trail {trail} (meta name {})", meta.name), format!("trail {trail}")),
            None => (format!("an inline script (meta name {})", meta.name), format!("inline {}", meta.name)),
        };
        let text = format!("Run {} of {what} started{answering}: {}", run.run, meta.description);
        let title: String = format!("{title}: {}", meta.description)
            .chars()
            .map(|c| if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') { ' ' } else { c })
            .collect();
        self.note(run, &text, Some(clip(title.trim(), MAX_TITLE))).await;
    }

    /// Tell the leg `run` entered last, by a goto(), in the notes of its thread as its route so far, and as the leg's
    /// start.
    pub(super) async fn report_route(&self, run: &TrailRun) {
        self.note(run, &clip(&format!("route: {}", run.route_text()), MAX_NOTE), None).await;
        self.report_leg(run);
    }

    /// Tell the start of the leg `run` entered last on the run's thread.
    pub(super) fn report_leg(&self, run: &TrailRun) {
        let Some(leg) = run.leg() else { return };
        let index = u32::try_from(run.route.len() - 1).unwrap_or(u32::MAX);
        tracing::info!(run = %run.run, leg = index, trail = ?leg.trail, at = ?leg.at, "trail leg");
        let update =
            StatusUpdate::Leg { run: run.run.clone(), leg: index, trail: leg.trail.clone(), waypoint: leg.at.clone() };
        crate::monitoring::status::publish(&self.inner.client, &self.inner.ant, &run.thread, update);
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
        let update = StatusUpdate::Step {
            run: run.run.clone(),
            call: step.call,
            label: step.function.label(&step.args),
            to: step.to.clone(),
            step_thread: step.thread.clone(),
            stage,
        };
        crate::monitoring::status::publish(&self.inner.client, &self.inner.ant, &run.thread, update);
    }

    /// Tell `step` answered with `answer`.
    pub(super) fn report_answered(&self, run: &TrailRun, step: &PendingCall, answer: &CallAnswer) {
        let stage = if matches!(answer, CallAnswer::Error(_)) { StepStage::Failed } else { StepStage::Answered };
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
    /// stopped it and which run to resume; a child run's in the notes of its thread, its parent telling the owner. True
    /// once told: the message stored, under an id derived from the run's end, which a restart sends again as the same
    /// message, for a run ended done or failed.
    pub(super) async fn report_end(&self, run: &TrailRun) -> bool {
        tracing::info!(run = %run.run, state = ?run.state, steps = run.steps, turns = run.turns, "trail run ended");
        // A child run's error answers its parent's call: only its parent's resume resumes it, once failed on a limit.
        let resume = match (&run.parent, &run.state) {
            (None, _) => Some(run.run.clone()),
            (Some(_), RunState::Stopped { by }) => self.stopped_root(run, by),
            (Some(_), _) => None,
        };
        let Some(text) = end_text(run, resume.as_ref()) else { return true };
        if run.parent.is_some() {
            self.note(run, &clip(&text, MAX_NOTE), None).await;
            return true;
        }
        let to = self.inner.humans.owner_or_default(run.owner.as_ref());
        let mut message =
            Envelope::on_thread(run.thread.clone(), self.inner.ant.clone(), to, text).with_owner(run.owner.clone());
        if let Some(ended_at) = run.ended_at.filter(|_| run.finished()) {
            message.id = end_id("trail-end", &run.run, ended_at);
        }
        match (self.inner.outbox)(message).await {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(run = %run.run, error = format!("{e:#}"), "telling the end of a trail run");
                false
            }
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

/// The message telling the end of `run`, naming `resume` as the run whose resume continues it; None while it runs. Past
/// one leg, its last line is the run's route.
fn end_text(run: &TrailRun, resume: Option<&RunId>) -> Option<String> {
    let mut text = end_state(run, resume)?;
    if run.route.len() > 1 {
        let _ = write!(text, "\nroute: {}", run.route_text());
    }
    Some(text)
}

fn end_state(run: &TrailRun, resume: Option<&RunId>) -> Option<String> {
    let name = match run.first_trail() {
        Some(trail) => format!("Run {} of trail {trail}", run.run),
        None => format!("Run {} of an inline script", run.run),
    };
    let spent = format!("{} steps, {} turns", run.steps, run.turns);
    Some(match (&run.state, resume) {
        (RunState::Running | RunState::Waiting, _) => return None,
        (RunState::Stopped { by }, Some(resume)) => {
            format!("{name} was stopped by {by} ({spent}); `ants trail resume {resume}` continues it.")
        }
        (RunState::Stopped { by }, None) => format!("{name} was stopped by {by} ({spent})."),
        (RunState::Failed { error, limit: Some(limit) }, Some(resume)) => {
            format!("{name} failed ({spent}): {error}\n{}", resume_after(&run.limits, *limit, resume))
        }
        (RunState::Failed { error, .. }, _) => format!("{name} failed ({spent}): {error}"),
        (RunState::Done { result }, _) => {
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

/// What a resume of `resume` does for a run under `limits` failed on `limit`: the flag raising a limit a flag raises,
/// unless at its maximum; a compute limit's computation run again; none for the session and memory, which a resume
/// would break the same way, but the session under a limit below its maximum, which only a request's limits raise.
fn resume_after(limits: &TrailLimits, limit: RunLimit, resume: &RunId) -> String {
    let max = TrailLimits::MAX;
    let raise = |flag: &str, value: String, most: String, at_most: bool| {
        if at_most {
            format!("Its {value} are the most a run may take: no resume continues it.")
        } else {
            format!("`ants trail resume {resume} {flag}` continues it, raising its {value} (at most {most}).")
        }
    };
    let wall = ants_proto::duration::format;
    if let Some(flag) = limit.raised_by() {
        let (value, most, at_most, takes) = match limit {
            RunLimit::Steps => {
                (format!("{} steps", limits.steps), max.steps.to_string(), limits.steps >= max.steps, "N")
            }
            RunLimit::Turns => {
                (format!("{} turns", limits.turns), max.turns.to_string(), limits.turns >= max.turns, "N")
            }
            _ => (
                format!("{} of wall time", wall(limits.wall_time())),
                wall(max.wall_time()),
                limits.wall_secs >= max.wall_secs,
                "DUR",
            ),
        };
        return raise(&format!("{flag} {takes}"), value, most, at_most);
    }
    match limit {
        RunLimit::Compute => format!(
            "`ants trail resume {resume}` runs the script's computation again under the same compute limit, which no \
             flag raises: it fails again unless the machine was only busy."
        ),
        RunLimit::Session if limits.session_bytes < max.session_bytes => format!(
            "No flag of `ants trail resume` raises its session size limit of {} bytes (at most {}), and a resume \
             under the same one would restore the same session and take the same answers.",
            limits.session_bytes, max.session_bytes
        ),
        _ => format!(
            "No resume continues it: no flag raises its {limit} limit, and a resume would restore the same session \
             and take the same answers."
        ),
    }
}

/// `text`, cut to at most `max` characters, the last an ellipsis when cut.
pub(crate) fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    let mut clipped: String = text.chars().take(max.saturating_sub(1)).collect();
    clipped.push('…');
    clipped
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::{Origin, Starter};
    use ants_proto::{TrailLeg, UserId};
    use chrono::Utc;

    use super::*;

    /// A run of trail deploy failed on `limit` under `limits`, having spent 2 steps and 7 turns.
    fn failed(limit: RunLimit, limits: TrailLimits) -> TrailRun {
        let human = UserId::human();
        let now = Utc::now();
        TrailRun {
            run: RunId::generate(),
            conductor: "lead".parse().unwrap(),
            route: vec![TrailLeg {
                trail: Some("deploy".parse().unwrap()),
                local: false,
                at: None,
                script: "agent('build')\n".to_owned(),
                args: serde_json::Value::Null,
                first_call: 0,
                started_at: now,
                steps: 2,
            }],
            owner: Some(human.clone()),
            thread: ThreadId::generate(),
            parent: None,
            spends_from: None,
            grants: Vec::new(),
            origin: Origin { by: Starter::Human { user: human }, channel: None },
            limits,
            state: RunState::Failed { error: "the limit".to_owned(), limit: Some(limit) },
            phase: None,
            steps: 2,
            turns: 7,
            started_at: now,
            updated_at: now,
            ended_at: Some(now),
        }
    }

    /// The last line of the end's message of `run`, resumed as itself.
    fn resume_line(run: &TrailRun) -> String {
        let text = end_text(run, Some(&run.run)).unwrap();
        assert!(text.starts_with(&format!("Run {} of trail deploy failed (2 steps, 7 turns): the limit\n", run.run)));
        text.lines().last().unwrap().to_owned()
    }

    #[test]
    fn the_end_of_a_run_failed_on_steps_turns_or_wall_time_names_the_flag_raising_it() {
        let limits = TrailLimits::default();
        let run = failed(RunLimit::Steps, limits);
        let resume = format!("`ants trail resume {}", run.run);
        assert_eq!(resume_line(&run), format!("{resume} --steps N` continues it, raising its 50 steps (at most 500)."));
        let run = failed(RunLimit::Turns, limits);
        let resume = format!("`ants trail resume {}", run.run);
        assert_eq!(
            resume_line(&run),
            format!("{resume} --turns N` continues it, raising its 200 turns (at most 2000).")
        );
        let run = failed(RunLimit::Wall, limits);
        let resume = format!("`ants trail resume {}", run.run);
        let raise = " --wall DUR` continues it, raising its 1d of wall time (at most 7d).";
        assert_eq!(resume_line(&run), format!("{resume}{raise}"));
    }

    #[test]
    fn the_end_of_a_run_at_its_most_steps_turns_or_wall_time_names_no_resume() {
        let run = failed(RunLimit::Steps, TrailLimits::MAX);
        assert_eq!(resume_line(&run), "Its 500 steps are the most a run may take: no resume continues it.");
        let run = failed(RunLimit::Turns, TrailLimits::MAX);
        assert_eq!(resume_line(&run), "Its 2000 turns are the most a run may take: no resume continues it.");
        let run = failed(RunLimit::Wall, TrailLimits::MAX);
        assert_eq!(resume_line(&run), "Its 7d of wall time are the most a run may take: no resume continues it.");
    }

    #[test]
    fn the_end_of_a_run_failed_on_compute_says_a_resume_computes_again() {
        let run = failed(RunLimit::Compute, TrailLimits::default());
        let line = resume_line(&run);
        assert!(line.starts_with(&format!("`ants trail resume {}` runs the script's computation again", run.run)));
        assert!(line.contains("no flag raises") && line.ends_with("unless the machine was only busy."), "{line}");
    }

    #[test]
    fn the_end_of_a_run_failed_on_its_session_or_memory_names_no_resume() {
        for (limit, name) in [(RunLimit::Session, "session size"), (RunLimit::Memory, "memory")] {
            let line = resume_line(&failed(limit, TrailLimits::default()));
            let none = format!("No resume continues it: no flag raises its {name} limit, and a resume would restore");
            assert!(line.starts_with(&none), "{line}");
            assert!(!line.contains("ants trail resume"), "{line}");
        }
    }

    #[test]
    fn the_end_of_a_run_failed_on_a_session_limit_below_its_maximum_names_no_flag() {
        let run = failed(RunLimit::Session, TrailLimits { session_bytes: 1024, ..TrailLimits::default() });
        let line = resume_line(&run);
        let none = "No flag of `ants trail resume` raises its session size limit of 1024 bytes (at most 786432)";
        assert!(line.starts_with(none), "{line}");
    }

    #[test]
    fn the_end_of_a_child_run_failed_on_a_limit_names_no_resume() {
        let run = failed(RunLimit::Steps, TrailLimits::default());
        assert_eq!(
            end_text(&run, None).unwrap(),
            format!("Run {} of trail deploy failed (2 steps, 7 turns): the limit", run.run)
        );
    }

    #[test]
    fn the_end_of_an_inline_run_names_it_an_inline_script() {
        let mut run = failed(RunLimit::Steps, TrailLimits::default());
        run.route[0].trail = None;
        assert_eq!(
            end_text(&run, None).unwrap(),
            format!("Run {} of an inline script failed (2 steps, 7 turns): the limit", run.run)
        );
    }
}
