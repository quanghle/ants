//! The examples of the trail authoring guide, contrib/skills/ants-trails/SKILL.md, and of docs/trails.md, each run to
//! its end with the real worker and a fake host answering its steps, its gotos and trail() steps leading to the same
//! document's other examples. The fake refuses gotos and trail() calls as the conductor does; the trail_daemon tests
//! run the same examples through a conductor, which refuses them itself. The watcher trail of
//! contrib/skills/ants-pr-watch/SKILL.md runs as a watcher's check ([`pr_watch`]).

use std::collections::{HashMap, VecDeque};

use ants_proto::{CallAnswer, TrailFunction};
use ants_trail::{Call, Event, Limits, Progress, Run, Status, Worker};
use serde_json::Value;

use host::{DOCS, GUIDE, answer, examples, hash};

#[path = "trail_guide/host.rs"]
mod host;
#[path = "trail_guide/pr_watch.rs"]
mod pr_watch;

const FAILED: &str = "the fake host failed this step";

/// Runs examples to their ends, each `goto()` and `trail()` leading to the example of the trail it names as a
/// conductor's daemon leads to a saved trail; with `faults`, a seed, about one step in three fails.
struct Drive<'a> {
    worker: &'a Worker,
    /// The script of each example, by its meta's name.
    trails: &'a HashMap<String, String>,
    faults: Option<u64>,
    /// The `agent()` and `human()` steps answered.
    steps: usize,
    /// The gotos refused for a waypoint entered before.
    refused: usize,
    /// The route of each run ended, as its thread tells it.
    routes: Vec<String>,
    /// The child runs started.
    children: usize,
    events: Vec<Event>,
}

impl<'a> Drive<'a> {
    fn new(worker: &'a Worker, trails: &'a HashMap<String, String>, faults: Option<u64>) -> Self {
        Self { worker, trails, faults, steps: 0, refused: 0, routes: Vec::new(), children: 0, events: Vec::new() }
    }

    /// Runs `trail` with `args` to its end, `callers` the trails on the routes of the runs whose trail() steps it
    /// answers.
    async fn run(&mut self, trail: &str, args: Value, callers: &[String]) -> Status {
        let mut leg = trail.to_owned();
        let mut run = self.leg(trail, None, args, 0);
        let mut route = vec![(leg.clone(), run.state().entry().map(str::to_owned))];
        let progress = run.start(self.worker).await.unwrap();
        let mut calls = VecDeque::new();
        self.take(&run, progress, &mut calls);
        // A goto first, as the conductor takes them before sending steps; then the newest step first: an example must
        // not depend on the order its steps are answered in.
        while let Some(call) = match calls.iter().position(|call| call.function == TrailFunction::Goto) {
            Some(goto) => calls.remove(goto),
            None => calls.pop_back(),
        } {
            let answer = match call.function {
                TrailFunction::Goto => match self.goto(&leg, &route, &run, &call) {
                    Ok((trail, next)) => {
                        route.push((trail.clone(), next.state().entry().map(str::to_owned)));
                        (leg, run) = (trail, next);
                        calls.clear();
                        let progress = run.start(self.worker).await.unwrap();
                        self.take(&run, progress, &mut calls);
                        continue;
                    }
                    Err(why) => CallAnswer::Error(why),
                },
                TrailFunction::Trail => {
                    let mut callers = callers.to_vec();
                    callers.extend(route.iter().map(|(trail, _)| trail.clone()));
                    Box::pin(self.child(&callers, &call)).await
                }
                TrailFunction::Agent | TrailFunction::Human => {
                    self.steps += 1;
                    if self.faults.is_some_and(|seed| hash(seed, &call.args.to_string()).is_multiple_of(3)) {
                        CallAnswer::Error(FAILED.to_owned())
                    } else {
                        answer(&leg, &call)
                    }
                }
                TrailFunction::Command | TrailFunction::ReadFile | TrailFunction::Bd | TrailFunction::Thread => {
                    unreachable!("{leg}: a watcher's read in a run")
                }
            };
            let progress = run.resolve(self.worker, call.call, answer).await.unwrap();
            self.take(&run, progress, &mut calls);
        }
        let legs: Vec<String> = route
            .iter()
            .map(|(trail, at)| at.as_ref().map_or_else(|| trail.clone(), |at| format!("{trail}@{at}")))
            .collect();
        self.routes.push(legs.join(" -> "));
        run.status().clone()
    }

    /// A run of the example `trail` entered at `at`, its calls numbered from `first`.
    fn leg(&self, trail: &str, at: Option<&str>, args: Value, first: u32) -> Run {
        let script = self.trails.get(trail).unwrap_or_else(|| panic!("no example of the document is trail {trail}"));
        Run::with_clock(script, at, args, Limits::default(), 1_700_000_000, 7)
            .unwrap_or_else(|e| panic!("entering trail {trail} at {at:?}: {e}"))
            .numbered_from(first)
    }

    /// The calls and events `progress` brings to the run of `run`, its phases checked against its meta.
    fn take(&mut self, run: &Run, progress: Progress, calls: &mut VecDeque<Call>) {
        let meta = run.state().meta();
        for event in &progress.events {
            if let Event::Phase(title) = event {
                assert!(meta.phases.contains(title), "{}: phase {title:?} is not in its meta", meta.name);
            }
        }
        calls.retain(|call| !progress.cancelled.contains(&call.call));
        calls.extend(progress.calls);
        self.events.extend(progress.events);
    }

    /// The trail and run of the leg `call`, a goto() of the leg `leg` of a run whose route is `route`, leads to; or
    /// why the conductor refuses it: a waypoint the route entered before.
    fn goto(
        &mut self,
        leg: &str,
        route: &[(String, Option<String>)],
        run: &Run,
        call: &Call,
    ) -> Result<(String, Run), String> {
        let args = call.args.as_object().unwrap_or_else(|| panic!("{leg}: goto args {}", call.args));
        for key in args.keys() {
            assert!(["trail", "at", "state"].contains(&key.as_str()), "{leg}: goto {key}");
        }
        let text = |key: &str| args.get(key).map(|v| v.as_str().unwrap_or_else(|| panic!("{leg}: goto {key} {v}")));
        let trail = text("trail").unwrap_or(leg);
        let state = args.get("state").cloned().unwrap_or(Value::Null);
        let next = self.leg(trail, text("at"), state, run.state().next_call());
        let at = next.state().entry().map(str::to_owned);
        if route.iter().any(|entered| entered.0 == trail && entered.1 == at) {
            self.refused += 1;
            let at = at.map_or_else(|| "its start".to_owned(), |at| format!("waypoint {at}"));
            return Err(format!(
                "the run entered trail {trail} at {at} before; a run enters a trail at each waypoint only once"
            ));
        }
        Ok((trail.to_owned(), next))
    }

    /// The answer to `call`, a trail() of the run of the last of `callers`: the result of a child run of the example it
    /// names, or why it fails.
    async fn child(&mut self, callers: &[String], call: &Call) -> CallAnswer {
        let caller = callers.last().map_or("", String::as_str);
        let args = call.args.as_object().unwrap_or_else(|| panic!("{caller}: trail args {}", call.args));
        for key in args.keys() {
            assert!(["name", "args"].contains(&key.as_str()), "{caller}: trail {key}");
        }
        let name = args["name"].as_str().unwrap_or_else(|| panic!("{caller}: trail name {}", call.args));
        if callers.iter().any(|caller| caller == name) {
            return CallAnswer::Error(format!("trail {name} is on the route of a calling run: no trail calls itself"));
        }
        self.children += 1;
        match self.run(name, args.get("args").cloned().unwrap_or(Value::Null), callers).await {
            Status::Done { result } => CallAnswer::Ok(result),
            Status::Failed { error, .. } => CallAnswer::Error(format!("trail {name} failed: {error}")),
            Status::Running => panic!("{name}: a child run that never ended"),
        }
    }
}

/// What running every example of a document showed: the gotos refused, the routes taken and the child runs started.
struct Ran {
    refused: usize,
    routes: Vec<String>,
    children: usize,
}

/// Runs every example of `markdown` to its end, once with every step answered, then with failing steps.
async fn run_examples(markdown: &str, at_least: usize) -> Ran {
    let worker = Worker::new(env!("CARGO_BIN_EXE_ants"));
    let examples = examples(markdown);
    assert!(examples.len() >= at_least, "the document has {} examples", examples.len());
    let mut trails = HashMap::new();
    let mut checked = Vec::new();
    for (script, args) in examples {
        let meta = ants_trail::check(&script).unwrap_or_else(|e| panic!("{script}\n{e}"));
        assert!(trails.insert(meta.name.clone(), script).is_none(), "two examples are trail {}", meta.name);
        checked.push((meta.name, args));
    }
    let (mut refused, mut routes, mut children) = (0, Vec::new(), 0);
    for (name, args) in &checked {
        let mut drive = Drive::new(&worker, &trails, None);
        let status = drive.run(name, args.clone(), &[]).await;
        let events = &drive.events;
        let Status::Done { result } = status else { panic!("{name}: {status:?}\nevents: {events:?}") };
        assert!(drive.steps > 0 && !result.is_null(), "{name}: {} steps, result {result}", drive.steps);
        (refused, children) = (refused + drive.refused, children + drive.children);
        routes.extend(drive.routes);

        // With failing steps, an example either handles them or fails on one it lets raise, never on a bug of its
        // own in the code handling them.
        for seed in 1..=4 {
            let mut drive = Drive::new(&worker, &trails, Some(seed));
            match drive.run(name, args.clone(), &[]).await {
                Status::Done { .. } => {}
                Status::Failed { error, .. } if error.ends_with(&format!("RuntimeError: {FAILED}")) => {}
                other => panic!("{name} with failing steps ({seed}): {other:?}\nevents: {:?}", drive.events),
            }
        }
    }
    Ran { refused, routes, children }
}

#[tokio::test]
async fn every_example_of_the_guide_runs_to_its_end() {
    let Ran { refused, routes, children } = run_examples(GUIDE, 5).await;
    assert!(refused > 0, "no example has a goto refused: {routes:#?}");
    assert!(routes.iter().any(|route| route.matches(" -> ").count() >= 3), "no route of 4 legs: {routes:#?}");
    assert!(children > 0, "no example starts a child run: {routes:#?}");
}

#[tokio::test]
async fn every_example_of_the_trails_docs_runs_to_its_end() {
    run_examples(DOCS, 2).await;
}
