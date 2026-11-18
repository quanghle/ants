//! The examples of the trail authoring guide, contrib/skills/ants-trails/SKILL.md, each run to its end with the real
//! worker and a fake host answering its steps, its gotos and trail() steps leading to the guide's other examples.

use std::collections::{HashMap, VecDeque};

use ants_trail::{Answer, Call, Event, Function, Limits, Progress, Run, Status, Worker};
use serde_json::{Map, Value, json};

const GUIDE: &str = include_str!("../../../contrib/skills/ants-trails/SKILL.md");

/// A fenced block of the guide: its language and its text.
fn blocks(markdown: &str) -> Vec<(&str, String)> {
    let mut blocks = Vec::new();
    let mut open: Option<(&str, String)> = None;
    for line in markdown.lines() {
        match (open.take(), line.strip_prefix("```")) {
            (None, Some(lang)) => open = Some((lang.trim(), String::new())),
            (Some(block), Some(_)) => blocks.push(block),
            (Some((lang, mut text)), None) => {
                text.push_str(line);
                text.push('\n');
                open = Some((lang, text));
            }
            (None, None) => {}
        }
    }
    assert!(open.is_none(), "the guide has an unclosed fenced block");
    blocks
}

/// Each python block with its args, the json block right after it, if any.
fn examples() -> Vec<(String, Value)> {
    let blocks = blocks(GUIDE);
    let mut examples = Vec::new();
    for (i, (lang, script)) in blocks.iter().enumerate() {
        if *lang != "python" {
            continue;
        }
        let args = match blocks.get(i + 1) {
            Some(("json", args)) => serde_json::from_str(args).unwrap_or_else(|e| panic!("args of {script}: {e}")),
            _ => Value::Null,
        };
        examples.push((script.clone(), args));
    }
    examples
}

/// FNV-1a: the fake host's answers depend only on what a step asks.
fn hash(seed: u64, text: &str) -> u64 {
    text.bytes().fold(seed ^ 0xcbf2_9ce4_8422_2325, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3))
}

/// A value of the shape `schema` describes; arrays hold 0 to 2 items, and strings come from a small set, so a loop
/// asking for new ones runs dry.
fn fake(schema: &Value, name: &str, seed: u64) -> Value {
    if let Some(choices) = schema["enum"].as_array() {
        return choices[usize::try_from(seed).unwrap_or_default() % choices.len()].clone();
    }
    let kind = match &schema["type"] {
        Value::String(kind) => kind.as_str(),
        Value::Array(kinds) => kinds.first().and_then(Value::as_str).unwrap_or("string"),
        _ => "string",
    };
    match kind {
        "object" => {
            let properties = schema["properties"].as_object().cloned().unwrap_or_default();
            let object: Map<String, Value> =
                properties.iter().map(|(key, property)| (key.clone(), fake(property, key, hash(seed, key)))).collect();
            Value::Object(object)
        }
        "array" => (0..seed % 3).map(|i| fake(&schema["items"], name, hash(seed, &i.to_string()))).collect(),
        "integer" => json!(seed % 100),
        "number" => json!(f64::from(u32::try_from(seed % 1000).unwrap_or_default()) / 10.0),
        "boolean" => json!(seed.is_multiple_of(2)),
        "null" => Value::Null,
        _ => json!(format!("{name} {}", seed % 4)),
    }
}

/// The fake host's answer to a step, after checking the step is one the real host takes.
fn answer(example: &str, call: &Call) -> Answer {
    let args = call.args.as_object().unwrap_or_else(|| panic!("{example}: step args {}", call.args));
    match call.function {
        Function::Agent => {
            for key in args.keys() {
                assert!(["prompt", "ant", "capability", "schema", "label"].contains(&key.as_str()), "{example}: {key}");
            }
            let prompt = args["prompt"].as_str().unwrap_or_else(|| panic!("{example}: prompt {}", call.args));
            assert!(!prompt.trim().is_empty(), "{example}: an empty prompt");
            for key in ["ant", "capability", "label"] {
                assert!(args.get(key).is_none_or(Value::is_string), "{example}: {key} {}", call.args);
            }
            let label = args.get("label").and_then(Value::as_str).unwrap_or("step");
            Answer::Ok(match args.get("schema") {
                Some(schema) => {
                    assert!(schema.is_object(), "{example}: schema {schema}");
                    fake(schema, label, hash(0, prompt))
                }
                None => json!(format!("reply to {label}")),
            })
        }
        Function::Human => {
            let questions = args["questions"].as_array().unwrap_or_else(|| panic!("{example}: {}", call.args));
            assert!((1..=4).contains(&questions.len()), "{example}: {} questions", questions.len());
            let lines: Vec<(String, String)> = questions
                .iter()
                .map(|q| {
                    let text = q["question"].as_str().unwrap_or_else(|| panic!("{example}: question {q}"));
                    assert!((1..=500).contains(&text.chars().count()), "{example}: question {text:?}");
                    let options = q["options"].as_array().unwrap_or_else(|| panic!("{example}: options {q}"));
                    assert!((2..=9).contains(&options.len()), "{example}: {} options", options.len());
                    let labels: Vec<&str> = options
                        .iter()
                        .map(|o| o.as_str().or_else(|| o["label"].as_str()).unwrap_or_else(|| panic!("{example}: {o}")))
                        .collect();
                    let header = q.get("header").map(|h| h.as_str().unwrap_or_else(|| panic!("{example}: {h}")));
                    assert!(header.is_none_or(|h| h.chars().count() <= 12), "{example}: header {header:?}");
                    let title = header.unwrap_or(text).to_owned();
                    (title, labels[0].to_owned())
                })
                .collect();
            Answer::Ok(json!(match lines.as_slice() {
                [(_, label)] => label.clone(),
                lines => lines.iter().map(|(title, label)| format!("{title}: {label}")).collect::<Vec<_>>().join("\n"),
            }))
        }
        Function::Trail | Function::Goto => unreachable!("{example}: the route answers {:?}", call.function),
    }
}

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

    /// Runs `trail` with `args` to its end, `callers` the trails of the runs whose trail() steps it answers.
    async fn run(&mut self, trail: &str, args: Value, callers: &[String]) -> Status {
        let mut leg = trail.to_owned();
        let mut run = self.leg(trail, None, args, 0);
        let mut route = vec![(leg.clone(), run.state().entry().map(str::to_owned))];
        let progress = run.start(self.worker).await.unwrap();
        let mut calls = VecDeque::new();
        self.take(&run, progress, &mut calls);
        // A goto first, as the conductor takes them before sending steps; then the newest step first: an example must
        // not depend on the order its steps are answered in.
        while let Some(call) = match calls.iter().position(|call| call.function == Function::Goto) {
            Some(goto) => calls.remove(goto),
            None => calls.pop_back(),
        } {
            let answer = match call.function {
                Function::Goto => match self.goto(&leg, &route, &run, &call) {
                    Ok((trail, next)) => {
                        route.push((trail.clone(), next.state().entry().map(str::to_owned)));
                        (leg, run) = (trail, next);
                        calls.clear();
                        let progress = run.start(self.worker).await.unwrap();
                        self.take(&run, progress, &mut calls);
                        continue;
                    }
                    Err(why) => Answer::Error(why),
                },
                Function::Trail => {
                    let mut callers = callers.to_vec();
                    callers.push(leg.clone());
                    Box::pin(self.child(&callers, &call)).await
                }
                Function::Agent | Function::Human => {
                    self.steps += 1;
                    if self.faults.is_some_and(|seed| hash(seed, &call.args.to_string()).is_multiple_of(3)) {
                        Answer::Error(FAILED.to_owned())
                    } else {
                        answer(&leg, &call)
                    }
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
        let script = self.trails.get(trail).unwrap_or_else(|| panic!("no example of the guide is trail {trail}"));
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
    async fn child(&mut self, callers: &[String], call: &Call) -> Answer {
        let caller = callers.last().map_or("", String::as_str);
        let args = call.args.as_object().unwrap_or_else(|| panic!("{caller}: trail args {}", call.args));
        for key in args.keys() {
            assert!(["name", "args"].contains(&key.as_str()), "{caller}: trail {key}");
        }
        let name = args["name"].as_str().unwrap_or_else(|| panic!("{caller}: trail name {}", call.args));
        if callers.iter().any(|caller| caller == name) {
            return Answer::Error(format!("trail {name} already runs among the calling runs: no trail calls itself"));
        }
        self.children += 1;
        match self.run(name, args.get("args").cloned().unwrap_or(Value::Null), callers).await {
            Status::Done { result } => Answer::Ok(result),
            Status::Failed { error } => Answer::Error(format!("trail {name} failed: {error}")),
            Status::Running => panic!("{name}: a child run that never ended"),
        }
    }
}

#[tokio::test]
async fn every_example_of_the_guide_runs_to_its_end() {
    let worker = Worker::new(env!("CARGO_BIN_EXE_ants"));
    let examples = examples();
    assert!(examples.len() >= 5, "the guide has {} examples", examples.len());
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
                Status::Failed { error } if error.ends_with(&format!("RuntimeError: {FAILED}")) => {}
                other => panic!("{name} with failing steps ({seed}): {other:?}\nevents: {:?}", drive.events),
            }
        }
    }
    assert!(refused > 0, "no example has a goto refused: {routes:#?}");
    assert!(routes.iter().any(|route| route.matches(" -> ").count() >= 3), "no route of 4 legs: {routes:#?}");
    assert!(children > 0, "no example starts a child run: {routes:#?}");
}
