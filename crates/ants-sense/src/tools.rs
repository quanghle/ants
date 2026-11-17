//! sense's tools: one [`ToolSpec`] each, in [`TOOLS`].

use ants_proto::provider::{BoxFuture, Format, Output, Tool, ToolCall, ToolResult};
use serde_json::{Value, json};

use crate::Sense;

/// The most tools sense may serve: each costs every session of every ant the room its schema takes.
pub const MAX_TOOLS: usize = 12;

/// A tool of sense: its name without the `sense_` prefix, what the ant reads of it, and what answers its calls.
#[derive(Debug, Clone, Copy)]
pub struct ToolSpec {
    pub name: &'static str,
    pub description: &'static str,
    /// The JSON Schema of its arguments.
    pub schema: fn() -> Value,
    pub handler: Handler,
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

    pub fn render(self, format: Format) -> ToolResult {
        let output = match format {
            Format::Text => Output::Text(self.text),
            Format::Json => Output::Json(self.json),
        };
        ToolResult { output, is_error: self.is_error }
    }
}

pub const TOOLS: &[ToolSpec] = &[
    ToolSpec {
        name: "brief",
        description: "Show the brief of this thread again: identity, open work and the memories that matter.",
        schema: || object(&json!({})),
        handler: crate::brief::brief,
    },
    ToolSpec {
        name: "remember",
        description: "Store a memory: a fact, lesson, decision, preference, gotcha or episode, with citations. Or, \
                      with kind self_model or relationship, replace the self-model or the relationship record of a \
                      human or ant from the revision read.",
        schema: remember,
        handler: crate::memory::remember,
    },
    ToolSpec {
        name: "recall",
        description: "Search the memories of self, the thread's repository (and the one repo names) and the thread's \
                      people, ranked by relevance, recency, importance and votes; or read one in full by id, with its \
                      provenance.",
        schema: recall,
        handler: crate::memory::recall,
    },
    ToolSpec {
        name: "vote",
        description: "Say whether a memory helped, is stale (to review) or is wrong (retired).",
        schema: || {
            let mut schema = object(&json!({
                "id": {"type": "string"},
                "vote": {"enum": ["helpful", "stale", "wrong"]},
                "reason": {"type": "string", "maxLength": 500},
            }));
            schema["required"] = json!(["id", "vote"]);
            schema
        },
        handler: crate::memory::vote,
    },
    ToolSpec {
        name: "checkpoint",
        description: "Replace this thread's resume register (goal, current step, next action, blockers, branch, open \
                      questions) from the revision read, acknowledging this thread's handoffs. Without arguments, \
                      show the whole register and every unacknowledged handoff.",
        schema: checkpoint,
        handler: crate::thread::checkpoint,
    },
    ToolSpec {
        name: "handoff",
        description: "Hand the work over as the thread's last write: what was done, is in flight and comes next, \
                      promises, decisions, files.",
        schema: handoff,
        handler: crate::thread::handoff,
    },
    ToolSpec {
        name: "task",
        description: "Keep personal tasks, list ready work and claim a task for this thread under a fenced lease.",
        schema: crate::task::schema,
        handler: crate::task::task,
    },
    ToolSpec {
        name: "promise",
        description: "Record a commitment, its promisee and due time; keep, break, release or renegotiate it.",
        schema: crate::promise::schema,
        handler: crate::promise::promise,
    },
    ToolSpec {
        name: "remind",
        description: "Set a timed or conditional reminder; acknowledge, postpone, renew or drop it.",
        schema: crate::remind::schema,
        handler: crate::remind::remind,
    },
    ToolSpec {
        name: "dream",
        description: "On a dream thread: answer each finding of the dream as applied (naming the items written), \
                      rejected or deferred, with a reason, and finish the dream with done. Without arguments, show \
                      the dream's findings and verdicts.",
        schema: crate::dream::schema,
        handler: crate::dream::dream,
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
        "kind": {
            "enum": ["fact", "lesson", "decision", "preference", "gotcha", "episode", "self_model", "relationship"],
        },
        "scope": {
            "type": "string",
            "description": "Whom it is about: self (default), repo:<git remote>, human:<name> or ant:<name>; \
                            human:<name> or ant:<name> for a relationship.",
        },
        "title": {"type": "string", "maxLength": 200},
        "body": {"type": "string", "description": "At most 4 KiB; 2,000 bytes for a self-model or relationship."},
        "revision": {
            "type": "integer",
            "minimum": 0,
            "description": "For a self-model or relationship: the revision replaced, none for the first.",
        },
        "importance": {"type": "integer", "minimum": 1, "maximum": 5, "description": "3 by default."},
        "citations": {
            "type": "array",
            "maxItems": 20,
            "items": {"anyOf": [{"type": "string", "description": "path, path:line or path:start-end"}, citation]},
        },
        "core": {"type": "boolean", "description": "Part of the identity, in every brief (2,000 bytes for all)."},
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
            "maxItems": 20,
            "description": "More people of the thread, to include their memories: name, human:<name> or ant:<name>.",
        },
        "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "10 by default."},
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
