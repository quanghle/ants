//! What a condition trigger waits for, which the daemon probes with local files and command-line tools.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::items::ItemId;

/// The most bytes of each text of a condition.
pub const MAX_CONDITION_BYTES: usize = 4096;

/// A condition, as the ant gives it and its reminder keeps it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Condition {
    /// A GitHub pull request of `repo`, `owner/repository`, reaching `state`.
    GhPr { repo: String, number: u64, state: PrState },
    /// A GitHub workflow run of `repo` finishing.
    GhWorkflow { repo: String, run: u64 },
    /// A bd issue reaching `state`.
    Bd { id: String, state: IssueState },
    /// `commit` reaching the git `reference`.
    Git { commit: String, reference: String },
    /// A file at `path` within the working directory appearing or changing.
    File { path: String, event: FileEvent },
    /// A shell command exiting successfully.
    Command { command: String },
    /// An event of sense's own items: of item `id` alone, when given.
    Sense {
        event: SenseEvent,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        id: Option<ItemId>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrState {
    Merged,
    Closed,
    Reviewed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueState {
    Closed,
    Ready,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileEvent {
    Appeared,
    Changed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SenseEvent {
    /// An open commitment past its due time.
    CommitmentOverdue,
    /// A task in progress past its lease.
    LeaseExpired,
}

impl Condition {
    /// Whether the texts it gives are fit to keep and pass to a command.
    pub(crate) fn validate(&self) -> Result<(), String> {
        match self {
            Self::GhPr { repo, number, .. } => {
                validate_repo(repo)?;
                positive("number", *number)
            }
            Self::GhWorkflow { repo, run } => {
                validate_repo(repo)?;
                positive("run", *run)
            }
            Self::Bd { id, .. } => text("id", id),
            Self::Git { commit, reference } => {
                text("commit", commit)?;
                text("reference", reference)
            }
            Self::File { path, .. } => text("path", path),
            Self::Command { command } => text("command", command),
            Self::Sense { .. } => Ok(()),
        }
    }

    /// Whether it asks a remote service, so is checked less often.
    pub(crate) fn remote(&self) -> bool {
        matches!(self, Self::GhPr { .. } | Self::GhWorkflow { .. } | Self::Bd { .. })
    }
}

fn validate_repo(repo: &str) -> Result<(), String> {
    text("repo", repo)?;
    if repo.split('/').count() != 2 || repo.split('/').any(|part| part.is_empty() || part.starts_with('-')) {
        return Err("condition.repo must be owner/repository.".into());
    }
    Ok(())
}

fn positive(name: &str, number: u64) -> Result<(), String> {
    if number == 0 {
        return Err(format!("condition.{name} must be positive."));
    }
    Ok(())
}

fn text(name: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > MAX_CONDITION_BYTES || value.chars().any(char::is_control) {
        return Err(format!(
            "condition.{name} must be a nonempty single-line string up to {MAX_CONDITION_BYTES} bytes."
        ));
    }
    let clean = crate::sanitize::sanitize(
        value,
        crate::sanitize::Lines::Single,
        crate::sanitize::Limit::Bytes(MAX_CONDITION_BYTES),
    )
    .map_err(|error| error.to_string())?;
    if clean.text != value {
        return Err(format!("condition.{name} contains hidden characters or secrets."));
    }
    Ok(())
}

pub(super) fn schema() -> Value {
    let text = json!({"type":"string","minLength":1,"maxLength":MAX_CONDITION_BYTES});
    let condition = |kind: &str, fields: Value, required: &[&str]| {
        let mut properties = fields.as_object().unwrap().clone();
        properties.insert("kind".into(), json!({"const":kind}));
        let required = std::iter::once("kind").chain(required.iter().copied()).collect::<Vec<_>>();
        json!({"type":"object","additionalProperties":false,"properties":properties,"required":required})
    };
    json!({"oneOf":[
        condition("gh_pr",json!({"repo":text,"number":{"type":"integer","minimum":1},
            "state":{"enum":["merged","closed","reviewed"]}}),&["repo","number","state"]),
        condition("gh_workflow",json!({"repo":text,"run":{"type":"integer","minimum":1}}),&["repo","run"]),
        condition("bd",json!({"id":text,"state":{"enum":["closed","ready"]}}),&["id","state"]),
        condition("git",json!({"commit":text,"reference":text}),&["commit","reference"]),
        condition("file",json!({"path":text,"event":{"enum":["appeared","changed"]}}),&["path","event"]),
        condition("command",json!({"command":text}),&["command"]),
        condition("sense",json!({"event":{"enum":["commitment_overdue","lease_expired"]},"id":text}),&["event"])
    ]})
}
