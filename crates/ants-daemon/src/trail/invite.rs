//! The conductor's invitation to rate a run it conducted, as the run ends done or failed: the answer of the run_trail
//! call waiting on the run carries it when that call takes the end ([`Trails::start_waited`]); else the conductor's
//! daemon sends the conductor itself a message on the run's thread, a child run's included ([`Trails::invite`]). The
//! run stays in the conductor's index of runs until the message is stored, so a daemon stopped before sends it on its
//! restart, under the same id, which the bus keeps once, as the message telling its owner the end is. A stopped run
//! invites no rating.

use std::fmt::Write as _;

use ants_proto::{Envelope, RunId, RunState, TrailRun, UserId, WEBHOOK_ALLOW};
use tokio::sync::oneshot;
use uuid::Uuid;

use super::report::clip;
use super::{StartRun, Trails, locked};

/// Most characters of a run's result or error an invitation quotes.
const MAX_RESULT: usize = 1000;
/// Most characters of a run's arguments, as JSON, an invitation quotes.
const MAX_ARGS: usize = 500;

/// A run_trail call's wait for the end of the run it started, which takes the run's invitation to rate it with the
/// end. Dropped before the end, it leaves the invitation to a message.
pub struct EndWait {
    trails: Trails,
    run: RunId,
    ended: Option<oneshot::Receiver<TrailRun>>,
}

impl EndWait {
    pub fn run(&self) -> &RunId {
        &self.run
    }

    /// The run once it ends or is stopped; None if the trails gave the wait up.
    pub async fn ended(&mut self) -> Option<TrailRun> {
        let ended = self.ended.as_mut()?;
        let header = ended.await.ok();
        self.ended = None;
        header
    }
}

impl Drop for EndWait {
    fn drop(&mut self) {
        locked(&self.trails.inner.waiters).remove(&self.run);
        // An end handed over as the wait was given up: its invitation goes as a message after all.
        let Some(mut ended) = self.ended.take() else { return };
        if let Ok(header) = ended.try_recv()
            && header.finished()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let trails = self.trails.clone();
            runtime.spawn(async move {
                trails.invite(&header).await;
            });
        }
    }
}

impl Trails {
    /// Start a run as [`Trails::start`] does, with a wait for its end, which takes the invitation to rate it.
    pub async fn start_waited(&self, start: StartRun) -> Result<EndWait, String> {
        let run = RunId::generate();
        let (tx, rx) = oneshot::channel();
        locked(&self.inner.waiters).insert(run.clone(), tx);
        // Made first, so a refused start drops it, and its waiter with it.
        let wait = EndWait { trails: self.clone(), run: run.clone(), ended: Some(rx) };
        self.start_as(run, start).await?;
        Ok(wait)
    }

    /// Hand `header`, a run ended or stopped, to the run_trail call waiting on it: true when one takes it.
    pub(super) fn hand_end(&self, header: &TrailRun) -> bool {
        let waiter = locked(&self.inner.waiters).remove(&header.run);
        waiter.is_some_and(|waiter| waiter.send(header.clone()).is_ok())
    }

    /// Ask the conductor itself, on the thread of `run`, ended done or failed, to rate it: true once the message is
    /// stored, which a run ended again (resumed) sends anew under another id.
    pub(super) async fn invite(&self, run: &TrailRun) -> bool {
        let Some(message) = invitation(&self.inner.ant, run) else { return true };
        match (self.inner.outbox)(message).await {
            Ok(()) => {
                tracing::info!(run = %run.run, thread = %run.thread, "inviting the conductor to rate a trail run");
                true
            }
            Err(e) => {
                tracing::warn!(run = %run.run, error = format!("{e:#}"), "inviting the conductor to rate a trail run");
                false
            }
        }
    }

    /// Whether `message` is an invitation this conductor's daemon sent to rate the run it names
    /// ([`Envelope::rating_of`]): from the conductor to itself, on the run's thread, under the id of the run's end.
    pub async fn invites(&self, message: &Envelope) -> bool {
        let Some(run) = &message.rating_of else { return false };
        let ant = &self.inner.ant;
        if message.from != *ant || message.to != *ant {
            return false;
        }
        let header = match self.run(run).await {
            Ok(Some(header)) => header,
            Ok(None) => return false,
            Err(e) => {
                tracing::warn!(%run, error = e, "reading the run a rating invitation names");
                return false;
            }
        };
        header.thread == message.thread_id && header.ended_at.is_some_and(|at| invitation_id(run, at) == message.id)
    }
}

/// The line inviting the conductor to rate `run`, with, for a failed run, the one pointing to other trails.
pub fn rating_hint(run: &TrailRun) -> String {
    let mut hint = format!(
        "rate_trail(run: \"{}\", outcome, task, why) records whether it served the task, for ants choosing trails.",
        run.run
    );
    if matches!(run.state, RunState::Failed { .. }) {
        hint.push_str("\nfind_trails(query) finds other saved trails for the task.");
    }
    hint
}

/// The message from `ant` to itself inviting it to rate `run`, its run ended done or failed; None for a run not
/// ended so. On the run's thread, answering to its owner; narrowed as a webhook's thread is for a run an untrusted
/// thread started, so that it is rated only there; naming the run ([`Envelope::rating_of`]), so that its turn may
/// rate it whatever tool rules narrow the thread.
pub(crate) fn invitation(ant: &UserId, run: &TrailRun) -> Option<Envelope> {
    let ended_at = run.ended_at?;
    let name = match run.first_trail() {
        Some(trail) => format!("Run {} of trail {trail}", run.run),
        None => format!("Run {} of an inline script", run.run),
    };
    let spent = format!("{} steps, {} turns", run.steps, run.turns);
    let mut text = match &run.state {
        RunState::Done { result } => {
            let result = match result {
                serde_json::Value::String(text) => text.clone(),
                result => result.to_string(),
            };
            format!("[ants] {name} ended done ({spent}). Result:\n{}", clip(&result, MAX_RESULT))
        }
        RunState::Failed { error, .. } => {
            format!("[ants] {name} ended failed ({spent}): {}", clip(error, MAX_RESULT))
        }
        RunState::Running | RunState::Waiting | RunState::Stopped { .. } => return None,
    };
    if let Some(leg) = run.route.first()
        && !leg.args.is_null()
    {
        let _ = write!(text, "\nArgs: {}", clip(&leg.args.to_string(), MAX_ARGS));
    }
    if run.route.len() > 1 {
        let _ = write!(text, "\nRoute: {}", run.route_text());
    }
    if let Some(parent) = &run.parent {
        let _ = write!(text, "\nA child run of run {}, answering its call {}.", parent.run, parent.call);
    }
    let _ = write!(text, "\n{}\nAfter rating, end the turn with complete: nothing needs a reply.", rating_hint(run));
    let allow = (!run.origin.trusted()).then(|| WEBHOOK_ALLOW.map(str::to_owned).to_vec());
    let message = Envelope::on_thread(run.thread.clone(), ant.clone(), ant.clone(), text)
        .with_owner(run.owner.clone())
        .with_origin_thread(run.parent.as_ref().map(|parent| parent.origin_thread.clone()))
        .with_allow(allow);
    Some(Envelope { id: invitation_id(&run.run, ended_at), rating_of: Some(run.run.clone()), ..message })
}

/// The id of the invitation to rate `run`, ended at `ended_at`.
fn invitation_id(run: &RunId, ended_at: chrono::DateTime<chrono::Utc>) -> Uuid {
    end_id("trail-rating", run, ended_at)
}

/// The id of a message of `kind` telling the end of `run` at `ended_at`: the same each time it is sent for that end, so
/// the bus keeps one, and a UUID v7 of the end's time, as the bus orders messages by.
pub(super) fn end_id(kind: &str, run: &RunId, ended_at: chrono::DateTime<chrono::Utc>) -> Uuid {
    let seed = format!("{kind}\0{run}\0{}", ended_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
    let hash = ring::digest::digest(&ring::digest::SHA256, seed.as_bytes());
    let mut random = [0; 10];
    random.copy_from_slice(&hash.as_ref()[..10]);
    let millis = u64::try_from(ended_at.timestamp_millis()).unwrap_or_default();
    uuid::Builder::from_unix_timestamp_millis(millis, &random).into_uuid()
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::{Origin, Starter};
    use ants_proto::{ThreadId, TrailLeg, TrailParent};
    use ants_test_support::{trail_leg, trail_run};
    use chrono::{TimeZone as _, Utc};
    use serde_json::json;

    use super::*;

    fn lead() -> UserId {
        "lead".parse().unwrap()
    }

    fn ended(state: RunState) -> TrailRun {
        let at = Utc.with_ymd_and_hms(2026, 11, 19, 8, 0, 0).unwrap();
        let leg = TrailLeg { args: json!({"pr": 7}), steps: 3, ..trail_leg(Some("review"), at) };
        TrailRun {
            route: vec![leg],
            steps: 3,
            turns: 4,
            ended_at: Some(at + chrono::Duration::milliseconds(1500)),
            ..trail_run(&lead(), Some("review"), state, at)
        }
    }

    #[test]
    fn a_run_ended_done_invites_its_conductor_on_its_thread_to_rate_it() {
        let run = ended(RunState::Done { result: json!("all fine") });
        let message = invitation(&lead(), &run).unwrap();
        assert_eq!((&message.from, &message.to, &message.thread_id), (&lead(), &lead(), &run.thread));
        assert_eq!(message.owner, run.owner);
        assert_eq!(message.allow, None, "a trusted run's invitation is not narrowed");
        assert_eq!(message.rating_of.as_ref(), Some(&run.run));
        assert_eq!(
            message.text,
            format!(
                "[ants] Run {r} of trail review ended done (3 steps, 4 turns). Result:
all fine
Args: {{\"pr\":7}}
\
                 rate_trail(run: \"{r}\", outcome, task, why) records whether it served the task, for ants choosing \
                 trails.
After rating, end the turn with complete: nothing needs a reply.",
                r = run.run
            )
        );
        assert_eq!(message.id, invitation(&lead(), &run).unwrap().id, "sent again under the same id");
        assert_eq!(message.id.get_version_num(), 7);
        let again = TrailRun { ended_at: run.ended_at.map(|at| at + chrono::Duration::seconds(1)), ..run };
        assert_ne!(message.id, invitation(&lead(), &again).unwrap().id, "a later end is another invitation");
    }

    #[test]
    fn a_failed_child_run_started_untrusted_invites_a_narrowed_rating_pointing_to_other_trails() {
        let mut run = ended(RunState::Failed { error: "no reviewer".to_owned(), limit: None });
        let root = ThreadId::generate();
        run.parent = Some(TrailParent { run: RunId::generate(), call: 2, origin_thread: root.clone() });
        run.origin = Origin { by: Starter::Webhook { user: "ci".parse().unwrap() }, channel: None };
        let message = invitation(&lead(), &run).unwrap();
        assert_eq!(message.allow, Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()));
        assert_eq!(message.origin_thread, Some(root));
        let parent = run.parent.as_ref().unwrap();
        assert!(message.text.starts_with(&format!(
            "[ants] Run {} of trail review ended failed (3 steps, 4 turns): no reviewer",
            run.run
        )));
        assert!(message.text.contains(&format!("\nA child run of run {}, answering its call 2.\n", parent.run)));
        assert!(message.text.contains("\nfind_trails(query) finds other saved trails for the task.\n"));
    }

    #[test]
    fn a_stopped_or_unended_run_invites_no_rating() {
        assert!(invitation(&lead(), &ended(RunState::Stopped { by: ants_test_support::human() })).is_none());
        assert!(invitation(&lead(), &TrailRun { ended_at: None, ..ended(RunState::Running) }).is_none());
        assert!(!rating_hint(&ended(RunState::Done { result: json!(null) })).contains("find_trails"));
    }
}
