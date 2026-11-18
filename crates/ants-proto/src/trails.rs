//! Trails: scripts an ant, the conductor, runs to conduct other ants and humans, one step at a time. A step is a
//! message carrying its [`TrailStep`]; a saved trail is a [`SavedTrail`] in [`TRAILS_BUCKET`]; a run is kept in
//! [`TRAIL_RUNS_BUCKET`] under its conductor's keys: its [`TrailRun`], its [`TrailSession`], one [`JournalEntry`] per
//! answered call, the conductor's [`TrailRunIndex`] and a [`TrailStepThread`] per step's thread.
//!
//! A step goes on a thread of its own, which the conductor's [`TrailSession`] maps to the call
//! ([`PendingCall::thread`]). The reply resolving the call carries nothing of the step: the first message on that
//! thread from the step's recipient ([`PendingCall::to`]), or from the assistant of a human recipient answering for it,
//! to the conductor that carries no [`TrailStep`], no notice and no escalation answers it ([`PendingCall::answered_by`]). Only the conductor's own records say which threads are
//! steps, so no other user can answer a call by naming it. A step the conductor sends itself tells its reply apart the
//! same way: the step carries its [`TrailStep`], the reply none.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::provider::Origin;
use crate::{Envelope, Error, MAX_TITLE, RunId, ThreadId, TrailName, UserId, subject};
#[cfg(doc)]
use crate::{
    NoticeKind, TRAIL_RUNS_BUCKET, TRAILS_BUCKET, trail_call_key, trail_run_key, trail_runs_index_key,
    trail_session_key, trail_step_key,
};

/// Most bytes a trail's script has: a saved trail's and a run's.
pub const MAX_TRAIL_SCRIPT: usize = 256 * 1024;
/// Most bytes a run's arguments have, as JSON.
pub const MAX_TRAIL_ARGS: usize = 64 * 1024;
/// Most characters a saved trail's description has.
pub const MAX_TRAIL_DESCRIPTION: usize = 1000;
/// Most phases a saved trail names.
pub const MAX_TRAIL_PHASES: usize = 32;
/// How long [`TRAIL_RUNS_BUCKET`] keeps a finished run, at least.
pub const TRAIL_RUN_RETENTION: Duration = Duration::from_hours(30 * 24);

/// What makes a message a trail's step: the run and call it serves ([`Envelope::trail_step`]). Its recipient answers
/// it with an ordinary reply on the step's thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailStep {
    pub run: RunId,
    /// The script's call the step answers, unique within the run.
    pub call: u32,
    /// What the script names the step, for showing it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The JSON Schema the answer is to match: the recipient answers with JSON matching it, and the conductor asks
    /// again, with the step on the same thread, for an answer that does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
}

/// A saved trail in [`TRAILS_BUCKET`], under the key of its name. Humans put and delete them; every ant and human reads
/// them. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedTrail {
    pub name: TrailName,
    #[serde(default)]
    pub description: String,
    /// The phases the script reports, in order, for showing a run's progress.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub phases: Vec<String>,
    /// The script's source text.
    pub script: String,
    /// The human who saved it.
    pub saved_by: UserId,
    pub saved_at: DateTime<Utc>,
}

impl SavedTrail {
    pub fn key(&self) -> &str {
        self.name.as_str()
    }

    /// Check what a reader takes from the bucket: the trail is stored under its own name and within the bounds of
    /// [`SavedTrail::check`].
    pub fn check_key(&self, key: &str) -> Result<(), Error> {
        if key != self.key() {
            return Err(Error::ForeignTrail { name: self.name.clone(), at: key.to_owned() });
        }
        self.check()
    }

    /// Check the trail's bounds: a script of 1 to [`MAX_TRAIL_SCRIPT`] bytes, a description of at most
    /// [`MAX_TRAIL_DESCRIPTION`] characters, and at most [`MAX_TRAIL_PHASES`] phases, each one line of 1 to
    /// [`MAX_TITLE`] characters.
    pub fn check(&self) -> Result<(), Error> {
        let invalid = |why: String| Err(Error::InvalidTrail { name: self.name.clone(), why });
        if self.script.trim().is_empty() || self.script.len() > MAX_TRAIL_SCRIPT {
            return invalid(format!("a script has 1-{MAX_TRAIL_SCRIPT} bytes"));
        }
        if self.description.chars().count() > MAX_TRAIL_DESCRIPTION {
            return invalid(format!("a description has at most {MAX_TRAIL_DESCRIPTION} characters"));
        }
        if self.phases.len() > MAX_TRAIL_PHASES {
            return invalid(format!("a trail has at most {MAX_TRAIL_PHASES} phases"));
        }
        let bad_phase = |p: &str| p.trim().is_empty() || p.contains(['\n', '\r']) || p.chars().count() > MAX_TITLE;
        if self.phases.iter().any(|p| bad_phase(p)) {
            return invalid(format!("a phase is one line of 1-{MAX_TITLE} characters"));
        }
        Ok(())
    }
}

/// What a run may spend, as its start asks and its [`TrailRun`] keeps. Missing fields take their defaults
/// ([`TrailLimits::DEFAULT`]); none may exceed [`TrailLimits::MAX`] ([`TrailLimits::check`]). Each step also counts
/// against its ant's own spending caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TrailLimits {
    /// Steps the run sends in all, re-asks for an answer matching a step's schema not counted.
    pub steps: u32,
    /// Steps waiting for an answer at once; the script's further calls wait their turn ([`TrailSession::queued`]).
    pub concurrency: u32,
    /// Turns the run's steps spend in all, as ant-to-ant messages spend them ([`Envelope::turn_budget`]).
    pub turns: u32,
    /// Seconds from the run's start to its end, stopped time included: a run still unfinished then fails.
    pub wall_secs: u64,
    /// Milliseconds the script computes for at most between two suspensions.
    pub compute_ms: u64,
    /// Most bytes of the dumped session ([`TrailSession::session`]).
    pub session_bytes: u32,
}

impl TrailLimits {
    /// What a run may spend when its start asks for no more. A step's answer spends a turn, a re-ask for an answer
    /// matching its schema another, and a step's ant that consults others on the step's thread more: four turns per
    /// step of the default steps.
    pub const DEFAULT: Self = Self {
        steps: 50,
        concurrency: 4,
        turns: 200,
        wall_secs: 24 * 3600,
        compute_ms: 10_000,
        session_bytes: 768 * 1024,
    };
    /// The most any run may spend. A run waiting for humans may take days, so a week of wall time, which bounds how
    /// long [`TRAIL_RUNS_BUCKET`] keeps a run's entries ([`TRAIL_RUN_RETENTION`]). The dumped session must leave room
    /// in its record, under NATS's 1 MiB payload, for the calls it waits on.
    pub const MAX: Self = Self {
        steps: 500,
        concurrency: 16,
        turns: 2000,
        wall_secs: 7 * 24 * 3600,
        compute_ms: 10_000,
        session_bytes: 768 * 1024,
    };

    pub fn wall_time(&self) -> Duration {
        Duration::from_secs(self.wall_secs)
    }

    pub fn compute(&self) -> Duration {
        Duration::from_millis(self.compute_ms)
    }

    /// Check each limit is at least 1 and at most its [`TrailLimits::MAX`].
    pub fn check(&self) -> Result<(), Error> {
        let max = Self::MAX;
        let limits = [
            ("steps", u64::from(self.steps), u64::from(max.steps)),
            ("concurrency", u64::from(self.concurrency), u64::from(max.concurrency)),
            ("turns", u64::from(self.turns), u64::from(max.turns)),
            ("wall_secs", self.wall_secs, max.wall_secs),
            ("compute_ms", self.compute_ms, max.compute_ms),
            ("session_bytes", u64::from(self.session_bytes), u64::from(max.session_bytes)),
        ];
        match limits.into_iter().find(|&(_, value, max)| value == 0 || value > max) {
            Some((limit, value, max)) => Err(Error::TrailLimit { limit, value, max }),
            None => Ok(()),
        }
    }
}

impl Default for TrailLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// A run's header in [`TRAIL_RUNS_BUCKET`], under [`trail_run_key`]: what it runs, for whom and where, and how far it
/// got. Its conductor's daemon puts it at the start and whenever its state or counters change; humans read it.
/// Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRun {
    pub run: RunId,
    /// The ant running the script, under whose keys the run is kept.
    pub conductor: UserId,
    /// The saved trail it runs; none for a script given inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trail: Option<TrailName>,
    pub script: String,
    /// The arguments the script got, JSON of at most [`MAX_TRAIL_ARGS`] bytes.
    pub args: Value,
    /// The human the run answers to, the owner of the thread that started it: its steps' threads carry it on. None when
    /// that thread had none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<UserId>,
    /// The thread the run reports on, where its start was asked and its result goes.
    pub thread: ThreadId,
    /// Where that thread came from, as the conductor's daemon settled it: a run from a webhook's thread is untrusted.
    pub origin: Origin,
    pub limits: TrailLimits,
    #[serde(flatten)]
    pub state: RunState,
    /// The phase the script reported last.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Steps sent so far, re-asks not counted.
    #[serde(default)]
    pub steps: u32,
    /// Turns the steps spent so far.
    #[serde(default)]
    pub turns: u32,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
}

impl TrailRun {
    pub fn key(&self) -> String {
        crate::trail_run_key(&self.conductor, &self.run)
    }

    /// Whether the run is over: done or failed. A stopped run may still resume within its wall time.
    pub fn finished(&self) -> bool {
        self.state.finished()
    }
}

/// How far a run got ([`TrailRun::state`]), with its result or error once over.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunState {
    /// The script computes, or waits for steps an ant answers.
    Running,
    /// The script waits only for steps humans answer.
    Waiting,
    /// A human, or its conductor, stopped it: it sends no step until a human resumes it, while its wall time runs on.
    Stopped { by: UserId },
    /// The script returned `result`.
    Done { result: Value },
    /// The script raised, or the run broke a limit or could not go on.
    Failed { error: String },
}

impl RunState {
    pub fn finished(&self) -> bool {
        matches!(self, Self::Done { .. } | Self::Failed { .. })
    }
}

/// Who answers a call of the script: an ant, by a step sent to it, or a human, by a question on a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrailFunction {
    Agent,
    Human,
}

/// A run's suspended session in [`TRAIL_RUNS_BUCKET`], under [`trail_session_key`]: the interpreter's dumped state and
/// the calls it waits on. The conductor's daemon puts it after each suspension, before acking the answer that resumed
/// it, and only it reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailSession {
    /// The dumped session, opaque JSON of at most the run's [`TrailLimits::session_bytes`].
    pub session: String,
    /// The calls sent as steps, waiting for their answers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<PendingCall>,
    /// The calls made beyond the run's concurrency, in order, waiting to be sent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued: Vec<QueuedCall>,
}

impl TrailSession {
    /// The pending call `message`, by `answerer`, answers, if any ([`PendingCall::answered_by`]).
    pub fn answered_by(&self, message: &Envelope, answerer: &UserId) -> Option<&PendingCall> {
        self.pending.iter().find(|call| call.answered_by(message, answerer))
    }
}

/// A call of the script sent as a step, waiting for its answer ([`TrailSession::pending`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCall {
    pub call: u32,
    pub function: TrailFunction,
    pub args: Value,
    /// The ant, or human, the step went to.
    pub to: UserId,
    /// The step's own thread.
    pub thread: ThreadId,
    /// The step's last message on that thread: the step, or its last re-ask.
    pub message: Uuid,
    pub sent_at: DateTime<Utc>,
    /// Whether that message is published: the conductor's daemon puts the call before publishing its message, under the
    /// message's id, which the stream deduplicates, and marks it sent after, so its restarted daemon publishes again
    /// only what it never did. A stopped run's steps are unsent again, to go out anew on resuming.
    #[serde(default)]
    pub sent: bool,
    /// The turns the step's thread had left as its last message on it went: what the step spent shows in the answer's
    /// [`Envelope::turn_budget`].
    #[serde(default)]
    pub turn_budget: u32,
    /// The re-asks for an answer matching the step's schema so far.
    #[serde(default)]
    pub retries: u32,
}

impl PendingCall {
    /// Whether `message`, which passed [`Envelope::check_subject`], answers the call: `answerer` is the step's
    /// recipient, the message is on the step's thread, and it is neither a step, a notice ([`NoticeKind`]) nor an
    /// escalation. `answerer` is the message's sender, or the human its [`Envelope::on_behalf_of`] names once the
    /// conductor honours the sender as that human's assistant, as for an answer to ask_human.
    pub fn answered_by(&self, message: &Envelope, answerer: &UserId) -> bool {
        message.thread_id == self.thread
            && *answerer == self.to
            && message.trail_step.is_none()
            && message.notice.is_none()
            && message.escalation.is_none()
    }
}

/// A call of the script waiting for room to be sent ([`TrailSession::queued`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueuedCall {
    pub call: u32,
    pub function: TrailFunction,
    pub args: Value,
}

/// An answered call in [`TRAIL_RUNS_BUCKET`], under [`trail_call_key`], put by the conductor's daemon before it acks
/// the answer: replaying the journal in call order rebuilds a session its dump no longer restores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalEntry {
    pub call: u32,
    pub function: TrailFunction,
    pub args: Value,
    pub answer: CallAnswer,
}

/// What a call returned to the script: `{"ok": <JSON>}`, or `{"error": "<message>"}`, which it raises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallAnswer {
    Ok(Value),
    Error(String),
}

/// The runs of a conductor not yet finished, in [`TRAIL_RUNS_BUCKET`] under [`trail_runs_index_key`], so its restarted
/// daemon finds them by reading its own keys alone. Its daemon adds a run before the run's first step and removes it
/// once finished.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRunIndex {
    #[serde(default)]
    pub runs: Vec<RunId>,
}

/// A step's thread, as its conductor keeps it in [`TRAIL_RUNS_BUCKET`] under [`trail_step_key`] from before the step is
/// sent, so a message on it after the run ended, or after a restart, is still told to be the step's and reported on the
/// run's thread rather than prompting the conductor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailStepThread {
    pub run: RunId,
    pub call: u32,
    /// The step's recipient ([`PendingCall::to`]).
    pub to: UserId,
    /// The run's thread ([`TrailRun::thread`]).
    pub run_thread: ThreadId,
}

/// A human asking `ant`'s daemon, the conductor of the run, to start, stop or resume one, on [`subject::trail`] with core
/// NATS. The daemon answers with a [`TrailReply`] on [`subject::trailed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailRequest {
    pub id: Uuid,
    pub ant: UserId,
    /// Who asks: a human, as only human credentials publish to [`subject::trail`].
    pub by: UserId,
    pub sent_at: DateTime<Utc>,
    #[serde(flatten)]
    pub action: TrailAction,
}

/// What a [`TrailRequest`] asks of a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum TrailAction {
    /// Start a run of `trail` with `args`, reporting on `thread`, which the asking human owns: its owner record names
    /// the human for the conductor ([`crate::owner_key`]) before the request goes, so the steps' recipients trust it.
    Start {
        trail: TrailSource,
        #[serde(default)]
        args: Value,
        thread: ThreadId,
        #[serde(default)]
        limits: TrailLimits,
    },
    /// Stop a run: it sends no step until resumed, and the steps it waits on are withdrawn.
    Stop { run: RunId },
    /// Resume a stopped run, or one failed on a limit given `limits` raising it.
    Resume {
        run: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limits: Option<TrailLimits>,
    },
}

/// The run a schedule's message asks its recipient to start, as the run's conductor ([`Envelope::trail_start`]): the
/// saved trail `trail` with `args`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailStart {
    pub trail: TrailName,
    #[serde(default)]
    pub args: Value,
}

/// The script a run starts: a saved trail's, by name, or one given inline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrailSource {
    Saved(TrailName),
    Script(String),
}

impl TrailRequest {
    pub fn new(ant: UserId, by: UserId, action: TrailAction) -> Self {
        Self { id: Uuid::now_v7(), ant, by, sent_at: Utc::now(), action }
    }

    pub fn subject(&self) -> String {
        subject::trail(&self.ant)
    }

    pub fn reply_subject(&self) -> String {
        subject::trailed(&self.ant)
    }

    /// Check that the request arrived on its own [`TrailRequest::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "trail request", user: self.ant.clone(), at: subject.to_owned() })
    }

    pub fn answer(&self, outcome: TrailOutcome) -> TrailReply {
        TrailReply { request_id: self.id, ant: self.ant.clone(), outcome }
    }
}

/// The answer of the conductor's daemon to a [`TrailRequest`], on [`subject::trailed`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailReply {
    pub request_id: Uuid,
    pub ant: UserId,
    #[serde(flatten)]
    pub outcome: TrailOutcome,
}

impl TrailReply {
    pub fn subject(&self) -> String {
        subject::trailed(&self.ant)
    }

    /// Check that the answer arrived on its own [`TrailReply::subject`], which credentials pin to the ant's own.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "trail answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// What came of a [`TrailRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum TrailOutcome {
    Started {
        run: RunId,
    },
    /// The run stopped. Its steps to other ants that waited for an answer are for the asking human to withdraw
    /// ([`crate::message_mark_key`]), as an ant marks no other ant's messages; the conductor withdrew its own.
    Stopped {
        run: RunId,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        waiting: Vec<WaitingStep>,
    },
    Resumed {
        run: RunId,
    },
    Refused {
        error: String,
    },
}

/// A step a stopped run waited on: its message, to `to`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitingStep {
    pub to: UserId,
    pub message: Uuid,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::p;

    fn saved() -> SavedTrail {
        SavedTrail {
            name: "release".parse().unwrap(),
            description: String::new(),
            phases: vec!["build".to_owned()],
            script: "agent('dev-a', 'build')".to_owned(),
            saved_by: p("alice"),
            saved_at: Utc::now(),
        }
    }

    #[test]
    fn a_message_is_no_step_unless_the_conductor_makes_it_one() {
        let message = Envelope::new(p("planner"), p("dev-a"), "build it");
        let json = serde_json::to_value(&message).unwrap();
        assert!(json.get("trail_step").is_none(), "a plain message serializes as before: {json}");
        let step = TrailStep { run: RunId::generate(), call: 1, label: None, schema: None };
        let sent = message.as_step(step.clone());
        let back: Envelope = serde_json::from_value(serde_json::to_value(&sent).unwrap()).unwrap();
        assert_eq!(back.trail_step.as_deref(), Some(&step));
        let reply = sent.reply(p("dev-a"), "built", crate::Role::Ant).unwrap();
        assert_eq!(reply.trail_step, None);
        assert_eq!(sent.follow_up(p("planner"), p("dev-a"), "again").trail_step, None);
    }

    #[test]
    fn only_the_recipients_reply_on_the_steps_thread_answers_a_call() {
        let step = Envelope::new(p("planner"), p("dev-a"), "build it").as_step(TrailStep {
            run: RunId::generate(),
            call: 1,
            label: None,
            schema: None,
        });
        let call = PendingCall {
            call: 1,
            function: TrailFunction::Agent,
            args: json!(["dev-a", "build it"]),
            to: p("dev-a"),
            thread: step.thread_id.clone(),
            message: step.id,
            sent_at: step.sent_at,
            sent: true,
            turn_budget: step.turn_budget,
            retries: 0,
        };
        let session = TrailSession { session: "{}".to_owned(), pending: vec![call], queued: Vec::new() };
        let reply = step.reply(p("dev-a"), "built", crate::Role::Ant).unwrap();
        assert_eq!(session.answered_by(&reply, &reply.from).map(|c| c.call), Some(1));
        let by_another = step.follow_up(p("dev-b"), p("planner"), "built too");
        let elsewhere = Envelope::new(p("dev-a"), p("planner"), "built");
        let notice = reply.clone().as_notice(crate::NoticeKind::SignedOut);
        let forged = reply.as_step(*step.trail_step.unwrap());
        for other in [by_another, elsewhere, notice, forged] {
            assert!(session.answered_by(&other, &other.from).is_none(), "{other:?}");
        }
    }

    #[test]
    fn a_self_step_is_told_from_its_reply() {
        let step = Envelope::new(p("planner"), p("planner"), "think").as_step(TrailStep {
            run: RunId::generate(),
            call: 2,
            label: None,
            schema: None,
        });
        let call = PendingCall {
            call: 2,
            function: TrailFunction::Agent,
            args: json!(null),
            to: p("planner"),
            thread: step.thread_id.clone(),
            message: step.id,
            sent_at: step.sent_at,
            sent: true,
            turn_budget: step.turn_budget,
            retries: 0,
        };
        assert!(!call.answered_by(&step, &step.from), "the step itself answers nothing");
        let reply = step.reply(p("planner"), "thought", crate::Role::Ant).unwrap();
        assert!(call.answered_by(&reply, &reply.from));
    }

    #[test]
    fn a_trail_request_and_its_answer_go_on_the_conductors_subjects() {
        let action = TrailAction::Start {
            trail: TrailSource::Saved("release".parse().unwrap()),
            args: json!({"version": "1.2"}),
            thread: ThreadId::generate(),
            limits: TrailLimits::DEFAULT,
        };
        let request = TrailRequest::new(p("planner"), p("alice"), action);
        assert_eq!(
            (request.subject(), request.reply_subject()),
            ("ants.trail.planner".into(), "ants.trailed.planner".into())
        );
        assert!(request.check_subject("ants.trail.dev-a").is_err());
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!((json["action"].as_str(), json["trail"]["saved"].as_str()), (Some("start"), Some("release")));
        let back: TrailRequest = serde_json::from_value(json).unwrap();
        assert_eq!(back, request);
        let stopped = request.answer(TrailOutcome::Stopped {
            run: RunId::generate(),
            waiting: vec![WaitingStep { to: p("dev-a"), message: Uuid::now_v7() }],
        });
        assert_eq!(stopped.check_subject("ants.trailed.planner"), Ok(()));
        let json = serde_json::to_value(&stopped).unwrap();
        assert_eq!(json["outcome"], "stopped");
        assert_eq!(serde_json::from_value::<TrailReply>(json).unwrap(), stopped);
    }

    #[test]
    fn a_journal_entry_has_the_shape_replay_reads() {
        let ok = JournalEntry {
            call: 1,
            function: TrailFunction::Human,
            args: json!(["ship?"]),
            answer: CallAnswer::Ok(json!("yes")),
        };
        assert_eq!(
            serde_json::to_value(&ok).unwrap(),
            json!({"call": 1, "function": "human", "args": ["ship?"], "answer": {"ok": "yes"}})
        );
        let failed = JournalEntry { answer: CallAnswer::Error("no such ant".to_owned()), ..ok };
        assert_eq!(serde_json::to_value(&failed).unwrap()["answer"], json!({"error": "no such ant"}));
    }

    #[test]
    fn a_runs_state_sits_beside_its_other_fields() {
        let state = RunState::Failed { error: "steps limit reached".to_owned() };
        let json = json!({"state": "failed", "error": "steps limit reached"});
        assert_eq!(serde_json::to_value(&state).unwrap(), json);
        assert!(state.finished() && !RunState::Stopped { by: p("alice") }.finished());
    }

    #[test]
    fn limits_hold_between_one_and_their_maximum() {
        assert_eq!(TrailLimits::default().check(), Ok(()));
        assert_eq!(TrailLimits::MAX.check(), Ok(()));
        let over = TrailLimits { concurrency: 17, ..TrailLimits::DEFAULT };
        assert_eq!(over.check(), Err(Error::TrailLimit { limit: "concurrency", value: 17, max: 16 }));
        assert!(TrailLimits { steps: 0, ..TrailLimits::DEFAULT }.check().is_err());
        assert!(TrailLimits::MAX.wall_time() >= TrailLimits::DEFAULT.wall_time());
    }

    #[test]
    fn a_saved_trail_is_checked_against_its_key_and_bounds() {
        let trail = saved();
        assert_eq!(trail.check_key("release"), Ok(()));
        assert!(matches!(trail.check_key("other"), Err(Error::ForeignTrail { .. })));
        let bad = [
            SavedTrail { script: " ".to_owned(), ..saved() },
            SavedTrail { script: "x".repeat(MAX_TRAIL_SCRIPT + 1), ..saved() },
            SavedTrail { description: "x".repeat(MAX_TRAIL_DESCRIPTION + 1), ..saved() },
            SavedTrail { phases: vec!["a\nb".to_owned()], ..saved() },
            SavedTrail { phases: vec!["p".to_owned(); MAX_TRAIL_PHASES + 1], ..saved() },
        ];
        for trail in bad {
            assert!(matches!(trail.check(), Err(Error::InvalidTrail { .. })), "{trail:?}");
        }
    }
}
