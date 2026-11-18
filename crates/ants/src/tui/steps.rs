//! The steps of the trail runs reporting on a thread, as its thread view lists them: heard as the conductor's status
//! updates tell each start and end ([`StatusUpdate::Step`]), and read from the records of the runs in
//! [`ants_proto::TRAIL_RUNS_BUCKET`] when the thread shows ([`Io`]), as status updates are never stored: a run's
//! thread opened after its steps ran shows them all the same. Each conductor keeps its steps under the thread of their
//! run ([`ants_proto::trail_thread_step_key`]), so a thread's read takes its own steps alone.

use std::collections::HashMap;

use ants_proto::{StatusEvent, StatusUpdate, StepStage, ThreadId, UserId};
use chrono::{DateTime, Utc};
use futures::FutureExt as _;
use futures::future::LocalBoxFuture;

pub(crate) use crate::cmd::trail::runs::Step;
use crate::live::maybe;

/// How many threads' steps are kept, those heard of least recently dropped first.
const MAX_THREADS: usize = 64;

/// The steps of each run's thread, by when each started, and when that thread's were last heard of or read.
#[derive(Debug, Default)]
pub struct Steps(HashMap<ThreadId, (DateTime<Utc>, Vec<Step>)>);

impl Steps {
    /// The steps of the runs reporting on `thread`, by when each started.
    pub fn of(&self, thread: &ThreadId) -> &[Step] {
        self.0.get(thread).map_or(&[][..], |(_, steps)| steps)
    }

    fn on(&mut self, thread: &ThreadId, at: DateTime<Utc>) -> &mut Vec<Step> {
        if !self.0.contains_key(thread) && self.0.len() >= MAX_THREADS {
            let stalest = self.0.iter().min_by_key(|(_, (touched, _))| *touched).map(|(t, _)| t.clone());
            stalest.map(|t| self.0.remove(&t));
        }
        let (touched, steps) = self.0.entry(thread.clone()).or_insert_with(|| (at, Vec::new()));
        *touched = (*touched).max(at);
        steps
    }

    /// Take a status update: a step update starts the step of its thread, or moves it to its stage.
    pub fn status(&mut self, event: &StatusEvent) {
        let StatusUpdate::Step { run, call, label, to, step_thread, stage } = &event.update else { return };
        let steps = self.on(&event.thread_id, event.at);
        if let Some(step) = steps.iter_mut().find(|s| s.thread == *step_thread) {
            step.stage = *stage;
            if label.is_some() {
                step.label.clone_from(label);
            }
            return;
        }
        let step = Step {
            run: run.clone(),
            conductor: event.ant.clone(),
            call: *call,
            label: label.clone(),
            to: to.clone(),
            thread: step_thread.clone(),
            stage: *stage,
            at: event.at,
        };
        insert(steps, step);
    }

    /// Take the steps read of the runs reporting on `thread` at `now`: each replaces the one on its thread, but for a
    /// stage heard ended that the records, read before, do not have yet.
    pub fn read(&mut self, thread: &ThreadId, read: Vec<Step>, now: DateTime<Utc>) {
        let steps = self.on(thread, now);
        for step in read {
            let Some(had) = steps.iter_mut().find(|s| s.thread == step.thread) else {
                insert(steps, step);
                continue;
            };
            let stage = if step.stage == StepStage::Started { had.stage } else { step.stage };
            let label = step.label.clone().or_else(|| had.label.take());
            *had = Step { stage, label, at: had.at.min(step.at), ..step };
        }
        steps.sort_by_key(|s| (s.at, s.call));
    }
}

/// Keep `step` among `steps` by when it started.
fn insert(steps: &mut Vec<Step>, step: Step) {
    let at = steps.partition_point(|s| (s.at, s.call) <= (step.at, step.call));
    steps.insert(at, step);
}

/// The steps read of the runs reporting on a thread, or why they could not be.
pub struct Event(pub ThreadId, pub anyhow::Result<Vec<Step>>);

/// The reads of the steps of the runs reporting on the thread shown: whenever it, or the ants known on it, change,
/// and when asked again.
pub struct Io {
    js: async_nats::jetstream::Context,
    /// The thread shown, and the ants on it, which may conduct runs reporting there.
    watched: Option<(ThreadId, Vec<UserId>)>,
    read: Option<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(js: async_nats::jetstream::Context) -> Self {
        Self { js, watched: None, read: None }
    }

    /// The next read of the thread watched. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            let event = maybe(&mut self.read).await;
            self.read = None;
            if self.watched.as_ref().is_some_and(|(thread, _)| *thread == event.0) {
                return event;
            }
        }
    }

    /// Read the steps of the runs the ants of `shown` conduct reporting on its thread, unless watched already.
    pub fn watch(&mut self, shown: Option<(ThreadId, Vec<UserId>)>) {
        if shown != self.watched {
            self.watched = shown;
            self.refresh();
        }
    }

    /// Read the steps of the thread watched again now.
    pub fn refresh(&mut self) {
        let Some((thread, ants)) = self.watched.clone() else {
            self.read = None;
            return;
        };
        let js = self.js.clone();
        self.read = Some(
            async move {
                let mut steps = Vec::new();
                for ant in &ants {
                    match crate::cmd::trail::runs::steps_on(&js, ant, &thread).await {
                        Ok(read) => steps.extend(read),
                        Err(e) => return Event(thread, Err(e)),
                    }
                }
                Event(thread, Ok(steps))
            }
            .boxed_local(),
        );
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{RunId, TRAIL_RUNS_BUCKET, TrailStepThread, trail_step_key, trail_thread_step_key};

    use super::*;
    use crate::connection::Nats;
    use crate::test_support::Server;
    use crate::test_support::live::{at, user};

    fn step(thread: &ThreadId, step_thread: &ThreadId, call: u32, stage: StepStage, secs: i64) -> StatusEvent {
        let update = StatusUpdate::Step {
            run: "0198c4ab-0000-7000-8000-000000000000".parse().unwrap(),
            call,
            label: (stage == StepStage::Started).then(|| format!("review {call}")),
            to: user("dev-b"),
            step_thread: step_thread.clone(),
            stage,
        };
        StatusEvent { at: at(secs), ..StatusEvent::new(user("lead"), thread.clone(), update) }
    }

    #[test]
    fn a_step_heard_starts_and_ends_on_its_own_thread_in_order_of_start() {
        let (thread, first, second) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut steps = Steps::default();
        steps.status(&step(&thread, &second, 2, StepStage::Started, 5));
        steps.status(&step(&thread, &first, 1, StepStage::Started, 3));
        steps.status(&step(&thread, &first, 1, StepStage::Answered, 9));
        let shown: Vec<_> = steps.of(&thread).iter().map(|s| (s.call, s.stage, s.label.as_deref(), s.at)).collect();
        assert_eq!(
            shown,
            [(1, StepStage::Answered, Some("review 1"), at(3)), (2, StepStage::Started, Some("review 2"), at(5))],
            "the label heard at the start stays"
        );
        assert_eq!(steps.of(&thread)[0].conductor, user("lead"));
        assert!(steps.of(&ThreadId::generate()).is_empty());
    }

    #[test]
    fn steps_read_join_those_heard_and_never_take_back_an_end_heard() {
        let (thread, heard, older) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let mut steps = Steps::default();
        steps.status(&step(&thread, &heard, 2, StepStage::Started, 6));
        steps.status(&step(&thread, &heard, 2, StepStage::Failed, 8));
        let run: RunId = "0198c4ab-0000-7000-8000-000000000000".parse().unwrap();
        let stored = |thread: &ThreadId, call, stage, secs| Step {
            run: run.clone(),
            conductor: user("lead"),
            call,
            label: None,
            to: user("dev-b"),
            thread: thread.clone(),
            stage,
            at: at(secs),
        };
        let read = vec![stored(&older, 1, StepStage::Answered, 2), stored(&heard, 2, StepStage::Started, 5)];
        steps.read(&thread, read, at(10));
        let shown: Vec<_> = steps.of(&thread).iter().map(|s| (s.call, s.stage, s.at)).collect();
        assert_eq!(shown, [(1, StepStage::Answered, at(2)), (2, StepStage::Failed, at(5))]);
        assert_eq!(steps.of(&thread)[1].label.as_deref(), Some("review 2"), "the label heard stays");
        steps.read(&thread, vec![stored(&heard, 2, StepStage::Dropped, 5)], at(11));
        assert_eq!(steps.of(&thread)[1].stage, StepStage::Dropped, "an end read replaces another heard");
    }

    #[tokio::test]
    async fn a_thread_reads_only_the_steps_kept_under_it() {
        let Some(server) = Server::start() else { return };
        let creds = server.creds_as(&user("alice"), ants_proto::Role::Human);
        let (_alice, js) = Nats::new(&server.url, Some(&creds), None).unwrap().connect("test").await.unwrap();
        let lead = user("lead");
        let (_lead, lead_js) = server.connect(&lead).await;
        let run: RunId = "0198c4ab-0000-7000-8000-000000000000".parse().unwrap();
        let (thread, other) = (ThreadId::generate(), ThreadId::generate());
        let (own, unindexed, elsewhere) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let put = async |key: String, call: u32, run_thread: &ThreadId| {
            let marker = TrailStepThread { run: run.clone(), call, to: user("dev-b"), run_thread: run_thread.clone() };
            let payload = serde_json::to_vec(&marker).unwrap().into();
            lead_js.publish(format!("$KV.{TRAIL_RUNS_BUCKET}.{key}"), payload).await.unwrap().await.unwrap();
        };
        put(trail_thread_step_key(&lead, &thread, &own), 1, &thread).await;
        put(trail_step_key(&lead, &unindexed), 2, &thread).await;
        put(trail_thread_step_key(&lead, &other, &elsewhere), 3, &other).await;

        let mut io = Io::new(js);
        io.watch(Some((thread.clone(), vec![lead.clone()])));
        let Event(read, steps) = io.next().await;
        assert_eq!(read, thread);
        let steps: Vec<_> = steps.unwrap().into_iter().map(|s| (s.thread, s.call, s.conductor)).collect();
        assert_eq!(steps, [(own, 1, lead)], "neither a step kept under another thread nor one kept by its own alone");
    }

    #[test]
    fn only_so_many_threads_are_kept() {
        let mut steps = Steps::default();
        let first = ThreadId::generate();
        steps.status(&step(&first, &ThreadId::generate(), 1, StepStage::Started, 0));
        for n in 1..=MAX_THREADS {
            let secs = i64::try_from(n).unwrap();
            steps.status(&step(&ThreadId::generate(), &ThreadId::generate(), 1, StepStage::Started, secs));
        }
        assert!(steps.of(&first).is_empty(), "the stalest went");
        assert_eq!(steps.0.len(), MAX_THREADS);
    }
}
