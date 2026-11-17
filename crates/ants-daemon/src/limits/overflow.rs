//! Telling a prompt that outgrew the model's context window from other harness errors.

use std::fmt;

use crate::harness::Harness;

/// Phrases of a prompt error that mean the session outgrew the model's context window, lowercase, whatever the harness:
/// those of the model APIs harnesses pass on (OpenAI's `context_length_exceeded`, Anthropic's "prompt is too long")
/// and of the harnesses themselves (Codex: "ran out of room in the model's context window").
const CONTEXT_OVERFLOW_TEXTS: &[&str] = &[
    "context_length_exceeded",
    "maximum context length",
    "context window exceeded",
    "exceeds the context window",
    "ran out of room in the model's context window",
    "prompt is too long",
];

/// Whether `error`, a prompt error from `harness`, says the session outgrew the model's context window: one of
/// [`CONTEXT_OVERFLOW_TEXTS`] or of the harness's own `context_overflow` phrases, in any case.
pub(crate) fn overflows(harness: &Harness, error: &str) -> bool {
    let error = error.to_lowercase();
    let own = harness.context_overflow.iter().map(|phrase| phrase.to_lowercase());
    CONTEXT_OVERFLOW_TEXTS.iter().map(|phrase| (*phrase).to_owned()).chain(own).any(|phrase| error.contains(&phrase))
}

/// A prompt that outgrew the model's context window even in a new session, its context rebuilt from the thread's
/// history: retrying it cannot help.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextOverflow {
    pub harness: String,
    /// What the harness said.
    pub detail: String,
}

impl fmt::Display for ContextOverflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let harness = &self.harness;
        write!(
            f,
            "The prompt does not fit the context window of {harness}'s model, even in a new session: {}",
            self.detail
        )
    }
}

impl std::error::Error for ContextOverflow {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Harnesses;

    #[test]
    fn overflow_errors_of_each_harness_are_told_from_others() {
        let harnesses = Harnesses::builtin();
        let harness = |name: &str| harnesses.get(name).unwrap();
        for (name, error) in [
            ("claude", "Internal error: Prompt is too long"),
            ("copilot", "CAPIError: 400 prompt token count of 140213 exceeds the limit of 128000"),
            ("copilot", r#"Internal error: {"code": "model_max_prompt_tokens_exceeded"}"#),
            ("antigravity", "The input token count (1100000) exceeds the maximum number of tokens allowed (1048576)."),
            ("antigravity", "This model's maximum context length is 128000 tokens."),
        ] {
            assert!(overflows(&harness(name), error), "{name}: {error}");
        }
        for (name, error) in [
            ("claude", "Internal error: Not logged in"),
            ("copilot", "CAPIError: 429 rate limit exceeded"),
            ("claude", "The input token count (1100000) exceeds the maximum number of tokens allowed (1048576)."),
        ] {
            assert!(!overflows(&harness(name), error), "{name}: {error}");
        }
    }
}
