//! What a harness left running in the background as its turn ended, such as a long check or a watcher Claude Code
//! runs as a background command: ants' `ants/background` key of a prompt response's `_meta`, listing those processes,
//! which a native protocol's translator sets ([`super::native`]) and the pool keeps the harness for while one of them
//! runs (`crate::pool`).

use agent_client_protocol::v1::schema::PromptResponse;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The key of a prompt response's `_meta` listing the processes the turn left running in the background.
const META_KEY: &str = "ants/background";

/// A process a harness's turn left running in the background.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Background {
    pub pid: i32,
    /// When it started, in the platform's unit: with the pid, which process it is.
    pub start: u64,
    /// Its command line, shortened.
    pub command: String,
}

impl Background {
    /// Whether it still runs.
    pub fn runs(&self) -> bool {
        super::native::runs(self.pid, self.start)
    }
}

/// `response` listing `background`, unless that is empty.
pub(crate) fn with(response: PromptResponse, background: &[Background]) -> PromptResponse {
    if background.is_empty() {
        return response;
    }
    let Ok(listed) = serde_json::to_value(background) else { return response };
    let mut meta = response.meta.clone().unwrap_or_default();
    meta.insert(META_KEY.to_owned(), listed);
    response.meta(meta)
}

/// What `response` lists left running in the background; nothing when it lists nothing or not in this form.
pub(crate) fn of(response: &PromptResponse) -> Vec<Background> {
    let listed = response.meta.as_ref().and_then(|meta| meta.get(META_KEY)).cloned().unwrap_or(Value::Null);
    serde_json::from_value(listed).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::v1::schema::StopReason;

    use super::*;

    #[test]
    fn a_prompt_response_lists_what_its_turn_left_running() {
        let none = PromptResponse::new(StopReason::EndTurn);
        assert_eq!(with(none.clone(), &[]), none, "nothing listed leaves the response as it was");
        assert_eq!(of(&none), Vec::new());
        let left = vec![Background { pid: 812, start: 1_764_000_000_123_456, command: "sh scripts/check.sh".into() }];
        let listed = with(none, &left);
        assert_eq!(of(&listed), left);
        let mut odd = PromptResponse::new(StopReason::EndTurn);
        odd.meta = Some([(META_KEY.to_owned(), Value::from("812"))].into_iter().collect());
        assert_eq!(of(&odd), Vec::new(), "a listing not in this form is none");
    }
}
