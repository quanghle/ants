//! The turns a harness runs of its own between prompts, such as Claude Code's as a background command it started ends
//! after the turn that started it: its translator reports each as it starts ([`OwnTurns`]), the daemon prompts the
//! ant on the thread with a message saying why, and that prompt takes the turn the harness runs rather than prompting
//! anew, its key `ants/own_turn` in the `_meta` of `session/prompt` naming the turn ([`meta`]). The daemon may refuse
//! a turn, as it does past a cap, the translator then dropping its events.

use std::fmt;
use std::sync::Arc;

use agent_client_protocol::v1::schema::Meta;
use ants_proto::ThreadId;
use serde_json::Value;

/// The key of a `session/prompt`'s `_meta` naming the turn of its own the harness runs that the prompt is to take.
const META_KEY: &str = "ants/own_turn";

/// A turn a harness started of its own between prompts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnTurn {
    /// What the prompt taking it names it by, in its `_meta` under `ants/own_turn`.
    pub id: String,
    /// Why it started, for the thread and for a harness that no longer runs it: the background tasks that ended.
    pub text: String,
}

/// Where the turns of its own a thread's harness starts go: the thread, and the turn; whether a prompt is to take it.
pub type OwnTurns = Arc<dyn Fn(&ThreadId, OwnTurn) -> bool + Send + Sync>;

/// Where the turns of its own the harness of one thread starts go ([`OwnTurns`]).
#[derive(Clone)]
pub(crate) struct OwnTurnReport {
    thread: ThreadId,
    sink: OwnTurns,
}

impl OwnTurnReport {
    pub(crate) fn new(thread: ThreadId, sink: OwnTurns) -> Self {
        Self { thread, sink }
    }

    /// Hand over `turn`: whether a prompt is to take it, else its events are for no one.
    pub(crate) fn send(&self, turn: OwnTurn) -> bool {
        tracing::info!(thread = %self.thread, turn = turn.id, why = turn.text, "the harness runs a turn of its own");
        (self.sink)(&self.thread, turn)
    }
}

impl PartialEq for OwnTurnReport {
    fn eq(&self, other: &Self) -> bool {
        self.thread == other.thread && Arc::ptr_eq(&self.sink, &other.sink)
    }
}

impl Eq for OwnTurnReport {}

impl fmt::Debug for OwnTurnReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnTurnReport").field("thread", &self.thread).finish_non_exhaustive()
    }
}

/// The `_meta` of a prompt taking the harness's own turn `id`.
pub(crate) fn meta(id: &str) -> Meta {
    [(META_KEY.to_owned(), Value::from(id))].into_iter().collect()
}

/// The own turn a prompt with `meta` takes, if any.
pub(crate) fn of(meta: Option<&Meta>) -> Option<&str> {
    meta?.get(META_KEY)?.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prompt_names_the_own_turn_it_takes_in_its_meta() {
        assert_eq!(of(Some(&meta("t1"))), Some("t1"));
        assert_eq!(of(None), None);
        let odd: Meta = [(META_KEY.to_owned(), Value::from(1))].into_iter().collect();
        assert_eq!(of(Some(&odd)), None, "an id not in this form is none");
    }
}
