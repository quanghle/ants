//! The examples of the trail authoring guide and of docs/trails.md, saved on the conductor's machine and each run by
//! name to its end, the conductor following their gotos and trail() calls and refusing those it cannot follow, the
//! fake host of `trail_guide` answering their steps.

use ants_proto::{TrailFunction, TrailName, trail_call_key};
use ants_trail::{Answer, Call, Function, JournalEntry};

use super::guide_host::{DOCS, GUIDE, answer, examples};
use super::*;

/// What running every example of a document through a conductor showed.
#[derive(Default)]
struct Ran {
    /// The gotos the conductor refused.
    refused: usize,
    /// The legs of each run's route.
    legs: Vec<usize>,
    /// The child runs started.
    children: usize,
}

impl Conductor {
    /// The call `step` makes, as the stored session of its run lists it.
    async fn step_call(&self, step: &Envelope) -> Call {
        let marks = step.trail_step.as_ref().unwrap();
        let pending = self.session(&marks.run).await.0.pending;
        let call = pending.into_iter().find(|call| call.call == marks.call).expect("the step's call is pending");
        let function = match call.function {
            TrailFunction::Agent => Function::Agent,
            TrailFunction::Human => Function::Human,
            function => panic!("a step of {function:?}"),
        };
        Call { call: call.call, function, args: call.args }
    }

    /// The step's answer from the fake host: its recipient's reply, or the human's.
    async fn answer_step(&self, example: &str, step: &Envelope) {
        let call = self.step_call(step).await;
        let Answer::Ok(value) = answer(example, &call) else { unreachable!("the fake host fails no step") };
        let taken = if call.function == Function::Human {
            let asked = step.asks().expect("a human step asks questions");
            let picks = asked.questions.iter().map(|_| Pick::option(1)).collect();
            self.trails.take(&Envelope::answering(step, UserId::human(), picks).unwrap()).await.unwrap()
        } else {
            let text = value.as_str().map_or_else(|| value.to_string(), str::to_owned);
            self.reply(step, &text).await
        };
        assert!(taken, "{example}: the conductor took the answer to {}", call.args);
    }

    /// The goto() calls of `run` the conductor refused, as its journal holds them.
    async fn refused_gotos(&self, run: &RunId) -> usize {
        let runs = self.js.get_key_value(TRAIL_RUNS_BUCKET).await.unwrap();
        let prefix = trail_call_key(&ant("lead"), run, 0);
        let prefix = prefix.trim_end_matches('0');
        let mut keys = runs.keys().await.unwrap();
        let mut refused = 0;
        while let Some(key) = keys.next().await {
            let key = key.unwrap();
            if !key.starts_with(prefix) {
                continue;
            }
            let entry: JournalEntry = serde_json::from_slice(&runs.get(&key).await.unwrap().unwrap()).unwrap();
            refused += usize::from(entry.function == Function::Goto && matches!(entry.answer, Answer::Error(_)));
        }
        refused
    }
}

/// Runs every example of `markdown`, saved in `dir`, to its end through a conductor.
async fn run_examples(markdown: &str, dir: &str) -> Ran {
    let Some(server) = NatsServer::start() else { return Ran::default() };
    init(&server).await;
    let dir = scratch_dir(dir);
    let examples = examples(markdown);
    for (script, _) in &examples {
        let meta = ants_trail::check(script).unwrap();
        std::fs::write(dir.join(format!("{}.py", meta.name)), script).unwrap();
    }
    let colony = vec![colony_entry("worker", &["ops", "review", "rust", "security"])];
    let mut lead = conductor_saving(&server, &ants_binary(), colony, Some(dir.clone())).await;
    let mut ran = Ran::default();
    for (script, args) in examples {
        let name: TrailName = ants_trail::check(&script).unwrap().name.parse().unwrap();
        let start = StartRun {
            trail: TrailSource::Saved(name.clone()),
            args,
            owner: Some(UserId::human()),
            origin: children::human_origin(),
            thread: ThreadId::generate(),
            limits: TrailLimits::default(),
            spends_from: None,
        };
        let run = lead.trails.start(start).await.unwrap();
        let mut runs = vec![run.clone()];
        while !lead.header(&run).await.finished() {
            let Ok(sent) = tokio::time::timeout(Duration::from_millis(200), lead.sent.recv()).await else { continue };
            let sent = sent.unwrap();
            let Some(marks) = &sent.trail_step else {
                lead.aside(sent);
                continue;
            };
            if !runs.contains(&marks.run) {
                runs.push(marks.run.clone());
            }
            lead.answer_step(name.as_str(), &sent).await;
        }
        let state = lead.header(&run).await.state;
        let RunState::Done { result } = state else { panic!("{name}: {state:?}") };
        assert!(!result.is_null(), "{name}: no result");
        for run in &runs {
            ran.refused += lead.refused_gotos(run).await;
            ran.legs.push(lead.header(run).await.route.len());
        }
        ran.children += runs.len() - 1;
    }
    std::fs::remove_dir_all(&dir).unwrap();
    ran
}

#[tokio::test]
async fn every_example_of_the_guide_runs_to_its_end_in_a_conductor() {
    let Ran { refused, legs, children } = run_examples(GUIDE, "trail-guide-examples").await;
    if legs.is_empty() {
        return;
    }
    assert!(refused > 0, "the conductor refused no goto: {legs:?}");
    assert!(legs.iter().any(|legs| *legs >= 4), "no route of 4 legs: {legs:?}");
    assert!(children > 0, "no example starts a child run");
}

#[tokio::test]
async fn every_example_of_the_trails_docs_runs_to_its_end_in_a_conductor() {
    run_examples(DOCS, "trail-docs-examples").await;
}
