//! What every protocol keeps of a turn between the CLI's events and the session updates they become: the reply and
//! thoughts streamed in pieces and not repeated whole ([`Reply`]), the tool calls announced ([`ToolCalls`]), the
//! tokens counted ([`Tokens`]) and the context window and cost reported ([`Spent`]).

use std::collections::HashSet;

use agent_client_protocol::v1::schema::{ToolCall, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, Usage};
use serde_json::Value;

use super::Updates;

/// A turn's reply and thoughts: a message or thought whose text came in pieces is not sent again whole, and each
/// message of the reply is set apart from the one before by a blank line.
#[derive(Debug, Default)]
pub(crate) struct Reply {
    /// The messages and thoughts whose text came in pieces, by id.
    streamed: HashSet<String>,
    /// The message the reply last came from, if any.
    last: Option<String>,
}

impl Reply {
    /// Note that the text of message or thought `id` comes in pieces.
    pub(crate) fn streams(&mut self, id: &str) {
        self.streamed.insert(id.to_owned());
    }

    /// Whether the text of message or thought `id` came in pieces.
    pub(crate) fn streamed(&self, id: &str) -> bool {
        self.streamed.contains(id)
    }

    /// Add `text` of message `id` to the reply, after a blank line when it starts another message.
    pub(crate) fn say(&mut self, updates: &Updates, id: &str, text: &str) {
        if text.is_empty() {
            return;
        }
        if self.last.as_deref() != Some(id) {
            if self.last.is_some() {
                updates.message("\n\n");
            }
            self.last = Some(id.to_owned());
        }
        updates.message(text);
    }

    /// Whether the reply said anything yet.
    pub(crate) fn said(&self) -> bool {
        self.last.is_some()
    }

    /// A piece of the text of message `id`.
    pub(crate) fn delta(&mut self, updates: &Updates, id: &str, text: &str) {
        self.streams(id);
        self.say(updates, id, text);
    }

    /// The whole text of message `id`, unless it came in pieces.
    pub(crate) fn whole(&mut self, updates: &Updates, id: &str, text: &str) {
        if !self.streamed(id) {
            self.say(updates, id, text);
        }
    }

    /// A piece of the text of thought `id`.
    pub(crate) fn thought_delta(&mut self, updates: &Updates, id: &str, text: &str) {
        self.streams(id);
        updates.thought(text);
    }

    /// The whole text of thought `id`, unless it came in pieces.
    pub(crate) fn whole_thought(&self, updates: &Updates, id: &str, text: impl Into<String>) {
        if !self.streamed(id) {
            updates.thought(text);
        }
    }
}

/// The tool calls a turn announced, by id.
#[derive(Debug, Default)]
pub(crate) struct ToolCalls(HashSet<String>);

impl ToolCalls {
    /// Announce `call`, as the CLI says it started.
    pub(crate) fn started(&mut self, updates: &Updates, call: ToolCall) {
        self.0.insert(call.tool_call_id.0.to_string());
        updates.tool_call(call);
    }

    /// Report how `call` stands, with `output` if it gave any: the tool call when first seen, else what changed of it.
    pub(crate) fn report(&mut self, updates: &Updates, call: ToolCall, status: ToolCallStatus, output: Option<Value>) {
        if self.0.insert(call.tool_call_id.0.to_string()) {
            updates.tool_call(call.status(status).raw_output(output));
        } else {
            let fields = ToolCallUpdateFields::new().status(status).raw_output(output);
            updates.tool_update(ToolCallUpdate::new(call.tool_call_id, fields));
        }
    }
}

/// Token counts, of a model call, a turn or a session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Tokens {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub thought: u64,
}

impl Tokens {
    /// Count `more` too.
    pub(crate) fn add(&mut self, more: Self) {
        self.input += more.input;
        self.output += more.output;
        self.cache_read += more.cache_read;
        self.cache_write += more.cache_write;
        self.thought += more.thought;
    }

    /// What was counted since `before`.
    pub(crate) fn minus(self, before: Self) -> Self {
        Self {
            input: self.input.saturating_sub(before.input),
            output: self.output.saturating_sub(before.output),
            cache_read: self.cache_read.saturating_sub(before.cache_read),
            cache_write: self.cache_write.saturating_sub(before.cache_write),
            thought: self.thought.saturating_sub(before.thought),
        }
    }

    /// The usage a prompt answers with, of `total` tokens as its CLI counts them: the daemon's spending caps count
    /// the total. A cache or thought count of zero is left out.
    pub(crate) fn usage(self, total: u64) -> Usage {
        let some = |count: u64| (count > 0).then_some(count);
        Usage::new(total, self.input, self.output)
            .cached_read_tokens(some(self.cache_read))
            .cached_write_tokens(some(self.cache_write))
            .thought_tokens(some(self.thought))
    }
}

/// What a session reports of its context window and cost.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct Spent {
    /// The cost in US dollars the CLI carried over from the session's parent, which its totals include.
    pub base: f64,
    /// The session's own cost so far in US dollars, once reported.
    pub cost: Option<f64>,
    /// Tokens in the context window as last reported, and the window's size.
    pub used: u64,
    pub size: u64,
}

impl Spent {
    /// The spending of a session whose totals start at `base` US dollars.
    pub(crate) fn from(base: f64) -> Self {
        Self { base, ..Self::default() }
    }

    pub(crate) fn report(self, updates: &Updates) {
        updates.usage(self.used, self.size, self.cost);
    }
}
