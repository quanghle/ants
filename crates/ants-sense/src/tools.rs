//! sense's tools: one [`ToolSpec`] each, in [`TOOLS`].

use ants_proto::provider::{BoxFuture, Format, Output, Tool, ToolCall, ToolResult};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::Sense;
use crate::memory::{
    CORE_BUDGET, DEFAULT_IMPORTANCE, DEFAULT_LIMIT, MAX_BODY_BYTES, MAX_CITATIONS, MAX_IMPORTANCE, MAX_LIMIT,
    MAX_TITLE_CHARS, MAX_WITH,
};
use crate::store::StoreError;

mod arguments;

/// The most tools sense may serve: each costs every session of every ant the room its schema takes.
pub const MAX_TOOLS: usize = 12;

/// The most characters of the reason a call gives for a vote, a proposal or a dream's verdict.
pub const MAX_REASON_CHARS: usize = 500;

/// The arguments of `call` as a tool's request `T`, an argument given as null taken as not given; refused as invalid
/// arguments naming the argument at fault.
pub(crate) fn arguments<'a, T: Deserialize<'a>>(call: &'a ToolCall) -> Result<T, Answer> {
    T::deserialize(arguments::Arguments(&call.arguments))
        .map_err(|e| Answer::invalid(format!("Invalid arguments: {e}.")))
}

/// A tool of sense: its name without the `sense_` prefix, what the ant reads of it, and what answers its calls.
#[derive(Debug, Clone, Copy)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    /// The JSON Schema of its arguments.
    pub schema: fn() -> Value,
    pub handler: Handler,
    /// How its calls on a dream's thread count toward the dream's [`crate::dream::MAX_DREAM_WRITES`]; none when
    /// they do not.
    pub dream: Option<DreamWrite>,
}

/// How a tool's calls count as writes of a dream, made on its thread (`crate::dream::counted`).
#[derive(Debug, Clone, Copy)]
pub struct DreamWrite {
    /// Whether a call writes what counts: a memory or a record.
    pub writes: fn(&ToolCall) -> bool,
    /// Whether what it writes is stored as quoting outside content while a finding of the dream resting on untrusted
    /// text is open.
    pub quotes: bool,
}

/// Every call writes.
fn always(_: &ToolCall) -> bool {
    true
}

/// What answers a tool's calls, given the ant's call ([`ToolCall::context`] tells whose).
pub type Handler = for<'a> fn(&'a Sense, &'a ToolCall) -> BoxFuture<'a, Answer>;

impl ToolSpec {
    pub fn tool(&self) -> Tool {
        Tool { name: self.name.to_owned(), description: self.description.to_owned(), input_schema: (self.schema)() }
    }
}

/// A tool's answer in both its forms, the call's format picking one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    /// Brief text, for the ant to read.
    pub text: String,
    pub json: Value,
    pub is_error: bool,
}

impl Answer {
    /// A refusal: `message` as text, `{"error": code, "message": message}` as JSON.
    pub fn error(code: &str, message: String) -> Self {
        Self { json: json!({"error": code, "message": message}), text: message, is_error: true }
    }

    /// The refusal of arguments missing, malformed or out of their bounds.
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::error("invalid_arguments", message.into())
    }

    pub fn render(self, format: Format) -> ToolResult {
        let output = match format {
            Format::Text => Output::Text(self.text),
            Format::Json => Output::Json(self.json),
        };
        ToolResult { output, is_error: self.is_error }
    }
}

/// The refusal of a call its store failed: an item missing, at another revision than expected or changing under
/// every try, or the data not available.
impl From<StoreError> for Answer {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(id) => Self::error("not_found", format!("No item {id}.")),
            StoreError::Conflict { expected, current } => {
                let message = format!(
                    "Item {} is at revision {}, not {expected}: read it again before retrying.",
                    current.id, current.revision
                );
                let mut answer = Self::error("conflict", message);
                answer.json["current"] = json!(current);
                answer
            }
            StoreError::Contended(id) => {
                Self::error("conflict", format!("Item {id} kept changing meanwhile: try again."))
            }
            error @ (StoreError::Unavailable(_) | StoreError::NotKept(_)) => {
                Self::error("unavailable", format!("{error}."))
            }
            error => Self::error("unavailable", format!("sense's data is not available: {error}.")),
        }
    }
}

/// The refusal of a call, `error`: one showing an item of `kind` found at another revision shows it as `name` too.
pub(crate) fn refused_as(error: impl Into<Answer>, kind: &str, name: &str) -> Answer {
    let mut answer = error.into();
    if answer.json["current"]["kind"] == kind {
        answer.json[name] = answer.json["current"].clone();
    }
    answer
}

pub const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "brief",
        description: "Show the brief of this thread again: identity, open work and the memories that matter.",
        schema: || object(&json!({})),
        handler: crate::brief::brief,
        dream: None,
    },
    ToolSpec {
        name: "remember",
        description: "Store a memory: a fact, lesson, decision, preference, gotcha or episode, with citations.",
        schema: remember,
        handler: crate::memory::remember,
        dream: Some(DreamWrite { writes: always, quotes: true }),
    },
    ToolSpec {
        name: "recall",
        description: "Search the memories of self, the thread's repository (and the one repo names) and the thread's \
                      people, ranked by relevance, recency, importance and votes; or read one in full by id, with its \
                      provenance.",
        schema: recall,
        handler: crate::memory::recall,
        dream: None,
    },
    ToolSpec {
        name: "vote",
        description: "Say whether a memory helped, is stale (to review) or is wrong (retired).",
        schema: || {
            let mut schema = object(&json!({
                "id": {"type": "string"},
                "vote": {"enum": ["helpful", "stale", "wrong"]},
                "reason": {"type": "string", "maxLength": MAX_REASON_CHARS},
            }));
            schema["required"] = json!(["id", "vote"]);
            schema
        },
        handler: crate::memory::vote,
        dream: Some(DreamWrite { writes: always, quotes: false }),
    },
    ToolSpec {
        name: "checkpoint",
        description: "Replace this thread's resume register (goal, current step, next action, blockers, branch, open \
                      questions) from the revision read, acknowledging this thread's handoffs. Without arguments, \
                      show the whole register and every unacknowledged handoff.",
        schema: checkpoint,
        handler: crate::thread::checkpoint,
        dream: None,
    },
    ToolSpec {
        name: "handoff",
        description: "Hand the work over as the thread's last write: what was done, is in flight and comes next, \
                      promises, decisions, files.",
        schema: handoff,
        handler: crate::thread::handoff,
        dream: None,
    },
    ToolSpec {
        name: "task",
        description: "Keep personal tasks, list ready work and claim a task for this thread under a fenced lease.",
        schema: crate::task::schema,
        handler: crate::task::task,
        dream: None,
    },
    ToolSpec {
        name: "promise",
        description: "Record a commitment, its promisee and due time; keep, break, release or renegotiate it.",
        schema: crate::promise::schema,
        handler: crate::promise::promise,
        dream: None,
    },
    ToolSpec {
        name: "remind",
        description: "Set a timed or conditional reminder; acknowledge, postpone, renew or drop it.",
        schema: crate::remind::schema,
        handler: crate::remind::remind,
        dream: None,
    },
    ToolSpec {
        name: "identity",
        description: concat!(
            "Show who the ant is: card, experience, persona and its pending proposal, self-model, relationship \
             records and skills. With self_model, or relationship and who, replace that record from the revision \
             read. With persona fields and a reason, propose a persona change, which a dream at least ",
            crate::proposal::confirm_after_hours!(),
            " hours later confirms or rejects. With history, show every persona version; with history and who, \
             every revision of the self-model (who=self) or of that relationship record."
        ),
        schema: crate::identity::schema,
        handler: crate::identity::identity,
        dream: Some(DreamWrite { writes: crate::identity::writes_record, quotes: true }),
    },
    ToolSpec {
        name: "skill",
        description: "Propose a new, changed or removed skill (SKILL.md with name and description, plus files) with \
                      its reason: a later dream confirms or rejects it. Without a name, list the skills; with a name \
                      alone, read one.",
        schema: skill,
        handler: crate::skill::skill,
        dream: None,
    },
    ToolSpec {
        name: "dream",
        description: "On a dream thread: answer each finding of the dream as applied (naming the items written or \
                      the trails saved), rejected or deferred, with a reason, and finish the dream with done. Without \
                      arguments, show the dream's findings and verdicts.",
        schema: crate::dream::schema,
        handler: crate::dream::dream,
        dream: None,
    },
];

const _: () = assert!(TOOLS.len() <= MAX_TOOLS, "sense serves at most 12 tools");

fn object(properties: &Value) -> Value {
    json!({"type": "object", "properties": properties})
}

fn strings() -> Value {
    json!({"type": "array", "items": {"type": "string"}})
}

fn remember() -> Value {
    let citation = json!({
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "start": {"type": "integer", "minimum": 1},
            "end": {"type": "integer", "minimum": 1},
            "remote": {"type": "string", "description": "The repository's git remote."},
            "hash": {"type": "string", "description": "The SHA-256 of the cited lines, in hex."},
        },
        "required": ["path"],
    });
    let mut schema = object(&json!({
        "kind": {"enum": ["fact", "lesson", "decision", "preference", "gotcha", "episode"]},
        "scope": {
            "type": "string",
            "description": "Whom it is about: self (default), repo:<git remote>, human:<name> or ant:<name>.",
        },
        "title": {"type": "string", "maxLength": MAX_TITLE_CHARS},
        "body": {"type": "string", "description": format!("At most {MAX_BODY_BYTES} bytes.")},
        "importance": {
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_IMPORTANCE,
            "description": format!("{DEFAULT_IMPORTANCE} by default."),
        },
        "citations": {
            "type": "array",
            "maxItems": MAX_CITATIONS,
            "items": {"anyOf": [{"type": "string", "description": "path, path:line or path:start-end"}, citation]},
        },
        "core": {
            "type": "boolean",
            "description": format!("Part of the identity, in every brief ({CORE_BUDGET} bytes for all)."),
        },
        "supersedes": {"type": "string", "description": "The id of the memory this one replaces."},
        "quotes": {
            "enum": ["recalled", "external"],
            "description": "Set when the text quotes recalled memories or outside content: stored as untrusted.",
        },
        "force": {"type": "boolean", "description": "Store it even when a near duplicate exists."},
    }));
    schema["required"] = json!(["kind", "title", "body"]);
    schema
}

fn recall() -> Value {
    object(&json!({
        "query": {"type": "string", "description": "Words to search for; none for the top memories."},
        "id": {"type": "string", "description": "A memory to read in full, with its provenance."},
        "history": {"type": "boolean", "description": "With id: show the chain of memories it superseded."},
        "repo": {"type": "string", "description": "The git remote of another repository, to include its memories too."},
        "with": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": MAX_WITH,
            "description": "More people of the thread, to include their memories: name, human:<name> or ant:<name>.",
        },
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_LIMIT,
            "description": format!("{DEFAULT_LIMIT} by default."),
        },
    }))
}

fn skill() -> Value {
    object(&json!({
        "name": {"type": "string", "description": "The skill's name: lowercase letters, digits and hyphens."},
        "content": {
            "type": "string",
            "description": "The whole SKILL.md: front matter (--- name: and description: ---), then instructions.",
        },
        "files": {
            "type": "object",
            "additionalProperties": {"type": ["string", "null"]},
            "description": "Other files by relative path, null removing one; files not named are kept.",
        },
        "remove": {"type": "boolean", "description": "Propose removing the skill."},
        "reason": {"type": "string", "maxLength": MAX_REASON_CHARS, "description": "Why: kept with the version."},
        "finding": {"type": "string", "description": "The dream finding this answers, such as 3.1."},
        "path": {"type": "string", "description": "With name or proposal: read this file."},
        "proposal": {"type": "string", "description": "Show this proposal in full."},
        "history": {"type": "boolean", "description": "With name: show every version and its reason."},
    }))
}

fn checkpoint() -> Value {
    object(&json!({
        "goal": {"type": "string"},
        "step": {"type": "string", "description": "The current step."},
        "next": {"type": "string", "description": "The next action."},
        "blockers": strings(),
        "branch": {"type": "string"},
        "questions": strings(),
        "revision": {
            "type": "integer",
            "minimum": 0,
            "description": "The revision this replaces, from the brief or the last checkpoint; none for the first.",
        },
        "acknowledges": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Handoffs of other threads to acknowledge, by id.",
        },
    }))
}

fn handoff() -> Value {
    object(&json!({
        "done": strings(),
        "in_flight": strings(),
        "next": strings(),
        "promises": strings(),
        "decisions": strings(),
        "files": strings(),
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use ants_proto::provider::valid_tool_name;

    use super::*;

    #[test]
    fn tools_are_named_once_validly_with_object_schemas() {
        let names: BTreeSet<_> = TOOLS.iter().map(|spec| spec.name).collect();
        assert_eq!(names.len(), TOOLS.len(), "a tool is named twice");
        for spec in TOOLS {
            assert!(valid_tool_name(spec.name), "{}", spec.name);
            assert!(!spec.description.is_empty());
            let schema = (spec.schema)();
            assert_eq!(schema["type"], "object", "{}", spec.name);
            let properties = schema["properties"].as_object().expect("properties");
            for required in schema["required"].as_array().into_iter().flatten() {
                assert!(properties.contains_key(required.as_str().unwrap()), "{}: {required}", spec.name);
            }
        }
    }
}
