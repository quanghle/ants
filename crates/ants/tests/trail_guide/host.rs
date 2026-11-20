//! What the tests running the trail examples of contrib/skills/ants-trails/SKILL.md and docs/trails.md share: the
//! examples, and a fake host's answers to their steps.

use ants_proto::{CallAnswer, TrailFunction};
use ants_trail::Call;
use serde_json::{Map, Value, json};

pub const GUIDE: &str = include_str!("../../../../contrib/skills/ants-trails/SKILL.md");
pub const DOCS: &str = include_str!("../../../../docs/trails.md");

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
    assert!(open.is_none(), "a document has an unclosed fenced block");
    blocks
}

/// Each python block that is a whole trail, its first statement its meta, with its args, the json block right after
/// it, if any; a block of a few lines quoted from a trail is left out.
pub fn examples(markdown: &str) -> Vec<(String, Value)> {
    let blocks = blocks(markdown);
    let mut examples = Vec::new();
    for (i, (lang, script)) in blocks.iter().enumerate() {
        if *lang != "python" || !script.starts_with("meta = ") {
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
pub fn hash(seed: u64, text: &str) -> u64 {
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
pub fn answer(example: &str, call: &Call) -> CallAnswer {
    let args = call.args.as_object().unwrap_or_else(|| panic!("{example}: step args {}", call.args));
    match call.function {
        TrailFunction::Agent => {
            for key in args.keys() {
                assert!(
                    ["prompt", "ant", "capability", "schema", "label", "tips"].contains(&key.as_str()),
                    "{example}: {key}"
                );
            }
            let prompt = args["prompt"].as_str().unwrap_or_else(|| panic!("{example}: prompt {}", call.args));
            assert!(!prompt.trim().is_empty(), "{example}: an empty prompt");
            for key in ["ant", "capability", "label", "tips"] {
                assert!(args.get(key).is_none_or(Value::is_string), "{example}: {key} {}", call.args);
            }
            if let Some(key) = args.get("tips").and_then(Value::as_str) {
                ants_proto::check_tip_key(key).unwrap_or_else(|e| panic!("{example}: tips {key:?}: {e}"));
            }
            let label = args.get("label").and_then(Value::as_str).unwrap_or("step");
            CallAnswer::Ok(match args.get("schema") {
                Some(schema) => {
                    assert!(schema.is_object(), "{example}: schema {schema}");
                    fake(schema, label, hash(0, prompt))
                }
                None => json!(format!("reply to {label}")),
            })
        }
        TrailFunction::Human => {
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
            CallAnswer::Ok(json!(match lines.as_slice() {
                [(_, label)] => label.clone(),
                lines => lines.iter().map(|(title, label)| format!("{title}: {label}")).collect::<Vec<_>>().join("\n"),
            }))
        }
        TrailFunction::Trail | TrailFunction::Goto => unreachable!("{example}: the route answers {:?}", call.function),
    }
}
