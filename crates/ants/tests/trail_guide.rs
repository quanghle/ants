//! The examples of the trail authoring guide, contrib/skills/ants-trails/SKILL.md, each run to its end with the real
//! worker and a fake host answering its steps.

use std::collections::VecDeque;

use ants_trail::{Answer, Call, Event, Function, Limits, Run, Status, Worker};
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
    }
}

const FAILED: &str = "the fake host failed this step";

/// Runs an example to its end; with `faults`, a seed, about one step in three fails. Returns the steps answered and
/// the events.
async fn drive(worker: &Worker, run: &mut Run, name: &str, faults: Option<u64>) -> (usize, Vec<Event>) {
    let progress = run.start(worker).await.unwrap();
    let (mut events, mut steps) = (progress.events, 0);
    let mut calls: VecDeque<Call> = progress.calls.into();
    // The newest step first: an example must not depend on the order its steps are answered in.
    while let Some(call) = calls.pop_back() {
        steps += 1;
        let answer = if faults.is_some_and(|seed| hash(seed, &call.args.to_string()).is_multiple_of(3)) {
            Answer::Error(FAILED.to_owned())
        } else {
            answer(name, &call)
        };
        let progress = run.resolve(worker, call.call, answer).await.unwrap();
        calls.retain(|call| !progress.cancelled.contains(&call.call));
        calls.extend(progress.calls);
        events.extend(progress.events);
    }
    (steps, events)
}

#[tokio::test]
async fn every_example_of_the_guide_runs_to_its_end() {
    let worker = Worker::new(env!("CARGO_BIN_EXE_ants"));
    let examples = examples();
    assert!(examples.len() >= 5, "the guide has {} examples", examples.len());
    for (script, args) in examples {
        let meta = ants_trail::check(&script).unwrap_or_else(|e| panic!("{script}\n{e}"));
        let name = meta.name.as_str();
        let mut run = Run::with_clock(&script, None, args.clone(), Limits::default(), 1_700_000_000, 7).unwrap();
        let (steps, events) = drive(&worker, &mut run, name, None).await;
        let Status::Done { result } = run.status() else { panic!("{name}: {:?}\nevents: {events:?}", run.status()) };
        assert!(steps > 0 && !result.is_null(), "{name}: {steps} steps, result {result}");
        for event in &events {
            if let Event::Phase(title) = event {
                assert!(meta.phases.contains(title), "{name}: phase {title:?} is not in its meta");
            }
        }

        // With failing steps, an example either handles them or fails on one it lets raise, never on a bug of its
        // own in the code handling them.
        for seed in 1..=4 {
            let mut run = Run::with_clock(&script, None, args.clone(), Limits::default(), 1_700_000_000, 7).unwrap();
            let (_, events) = drive(&worker, &mut run, name, Some(seed)).await;
            match run.status() {
                Status::Done { .. } => {}
                Status::Failed { error } if error.ends_with(&format!("RuntimeError: {FAILED}")) => {}
                other => panic!("{name} with failing steps ({seed}): {other:?}\nevents: {events:?}"),
            }
        }
    }
}
