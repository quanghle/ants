//! Publishing what a thread committed. With `publish = "draft-pr"`, a prompt that ends with new commits on the thread's
//! worktree branch pushes the branch to the repository's default remote and opens a draft pull request for it with
//! gh, or finds the one open, which the push updated. The push is a tool call like the harness's own: run unasked when
//! an allow rule matches `execute:git push <remote> <branch>`, else asked through the approvers like any other tool
//! request. The reply tells what happened, failures included; nothing is retried until the branch moves again.

use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use agent_client_protocol::v1::schema::{ToolCallUpdateFields, ToolKind};
use ants_proto::{ThreadId, ThreadTitle, UserId, Verdict, title_key};
use anyhow::{Context as _, Result};
use serde::Deserialize;
use serde_json::json;

use crate::permission::PermissionPolicy;
use crate::records::direct_kv::{DirectKv, Lookup};
use crate::secrets::redact::Redactor;
use crate::worktrees::{self, Branch, Worktrees};

/// Longest a push may take.
pub const PUSH_TIMEOUT: Duration = Duration::from_secs(120);
/// Longest a gh command may take.
pub const GH_TIMEOUT: Duration = Duration::from_secs(60);
/// Longest reading a thread's title may take.
const TITLE_TIMEOUT: Duration = Duration::from_secs(2);
/// Most characters of a pull request title taken from the thread's opening line.
const MAX_TITLE: usize = 120;
/// Most characters of an error a reply tells.
const MAX_ERROR: usize = 500;

/// What becomes of a thread's branch holding new commits: the ant config key `publish`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Publish {
    /// Nothing: the reply names the branch.
    #[default]
    Off,
    /// Pushed to the repository's default remote, with a draft pull request.
    DraftPr,
}

impl Publish {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::DraftPr => "draft-pr",
        }
    }
}

/// A pull request of a thread's branch.
enum PullRequest {
    /// Opened now, at this URL.
    Opened(String),
    /// Already open, at this URL.
    Open(String),
}

/// Pushes the branches of an ant's threads, opening a draft pull request for each.
pub struct Publisher {
    ant: UserId,
    /// The gh program.
    gh: OsString,
    push_timeout: Duration,
    gh_timeout: Duration,
    /// Where thread titles are, when the ant's credentials may read them.
    titles: Option<DirectKv>,
    /// Set once reading a title failed, so no later prompt waits on it again.
    no_titles: AtomicBool,
    /// What pull request titles and bodies hide.
    redactor: Arc<Redactor>,
}

impl Publisher {
    pub fn new(ant: UserId) -> Self {
        Self {
            ant,
            gh: "gh".into(),
            push_timeout: PUSH_TIMEOUT,
            gh_timeout: GH_TIMEOUT,
            titles: None,
            no_titles: AtomicBool::new(false),
            redactor: Arc::default(),
        }
    }

    /// The same, titling a pull request with its thread's title in `titles` ([`ants_proto::THREADS_BUCKET`]) when
    /// there is one and it can be read.
    #[must_use]
    pub fn with_titles(self, titles: DirectKv) -> Self {
        Self { titles: Some(titles), ..self }
    }

    /// The same, running `gh` for gh.
    #[cfg(all(test, unix))]
    #[must_use]
    pub fn with_gh(self, gh: impl Into<OsString>) -> Self {
        Self { gh: gh.into(), ..self }
    }

    /// The same, hiding what `redactor` does in pull request titles and bodies.
    #[must_use]
    pub fn with_redactor(self, redactor: Arc<Redactor>) -> Self {
        Self { redactor, ..self }
    }

    /// Keep the first line of `text`, a message prompted on `thread`, as the thread's opening line, unless the
    /// thread's branch has one already: it titles the branch's pull request when the thread's title cannot be read.
    pub async fn note_opening(&self, worktrees: &Worktrees, thread: &ThreadId, text: &str) {
        let Some(line) = opening_line(text) else { return };
        if let Err(e) = worktrees.describe(thread, &line).await {
            tracing::debug!(%thread, error = format!("{e:#}"), "keeping the thread's opening line");
        }
    }

    /// Push `branch` and open its draft pull request, unless the remote has it as it is, asking `policy` first. What
    /// happened, for the reply.
    pub async fn publish(&self, worktrees: &Worktrees, branch: &Branch, policy: &PermissionPolicy) -> Option<String> {
        let (name, thread) = (&branch.name, &branch.thread);
        let Some(remote) = worktrees.remote().await else {
            return Some(format!("[ants] Branch {name} was not pushed: the repository has no remote."));
        };
        if worktrees.pushed(branch, &remote).await {
            return None;
        }
        let command = format!("git push {remote} {name}");
        let fields = ToolCallUpdateFields::new()
            .kind(ToolKind::Execute)
            .title(format!("Push branch {name} to {remote} and open a draft pull request"))
            .raw_input(json!({ "command": command }));
        if policy.decide(&fields, thread, worktrees.repo()).await == Verdict::Reject {
            tracing::info!(%thread, branch = %name, "the push was rejected");
            return Some(format!(
                "[ants] Branch {name} was not pushed: `{command}` was rejected. An allow rule such as \
                 `execute:git push {remote} ants/{}/*` lets it run unasked.",
                self.ant
            ));
        }
        if let Err(e) = worktrees.push(branch, &remote, self.push_timeout).await {
            tracing::warn!(%thread, branch = %name, error = format!("{e:#}"), "pushing the thread's branch");
            return Some(format!("[ants] Pushing branch {name} to {remote} failed: {}", shortened(&e)));
        }
        Some(match self.pull_request(worktrees, branch).await {
            Ok(PullRequest::Opened(url)) => {
                tracing::info!(%thread, branch = %name, %url, "opened a draft pull request");
                format!("[ants] Branch {name} was pushed to {remote}, with draft pull request {url}.")
            }
            Ok(PullRequest::Open(url)) => {
                tracing::info!(%thread, branch = %name, %url, "updated the pull request");
                format!("[ants] Branch {name} was pushed to {remote}, updating pull request {url}.")
            }
            Err(e) => {
                tracing::warn!(%thread, branch = %name, error = format!("{e:#}"), "opening a draft pull request");
                format!(
                    "[ants] Branch {name} was pushed to {remote}, but opening a draft pull request failed: {}",
                    shortened(&e)
                )
            }
        })
    }

    /// The open pull request of `branch`, else a draft one opened now.
    async fn pull_request(&self, worktrees: &Worktrees, branch: &Branch) -> Result<PullRequest> {
        let (dir, name, thread) = (worktrees.repo(), branch.name.as_str(), &branch.thread);
        let list = ["pr", "list", "--head", name, "--state", "open", "--json", "url", "--jq", ".[].url"];
        if let Some(url) = first_line(&self.gh(dir, list).await?) {
            return Ok(PullRequest::Open(url));
        }
        let title = match self.thread_title(thread).await {
            Some(title) => title,
            None => match worktrees.description(thread).await {
                Some(line) => line,
                None => format!("Work of {} on thread {thread}", self.ant),
            },
        };
        let title = self.redactor.redact(&title).into_owned();
        let body = format!(
            "Work of ants {} on thread {thread}, which `ants thread show {thread}` shows.",
            self.redactor.redact(self.ant.as_str())
        );
        let create = ["pr", "create", "--draft", "--head", name, "--title", &title, "--body", &body];
        let created = self.gh(dir, create).await?;
        let url = created.lines().rev().find_map(first_line).context("gh pr create printed no URL")?;
        Ok(PullRequest::Opened(url))
    }

    /// Run gh in `dir`, prompting for nothing.
    async fn gh<const N: usize>(&self, dir: &Path, args: [&str; N]) -> Result<String> {
        let env = [
            ("GH_PROMPT_DISABLED", "1"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GH_NO_UPDATE_NOTIFIER", "1"),
            ("NO_COLOR", "1"),
        ];
        worktrees::run(&self.gh, dir, args, &env, Some(self.gh_timeout)).await
    }

    /// The title of `thread`, if it has one the ant's credentials may read, not sealed between humans.
    async fn thread_title(&self, thread: &ThreadId) -> Option<String> {
        let titles = self.titles.as_ref().filter(|_| !self.no_titles.load(Ordering::Relaxed))?;
        let read = tokio::time::timeout(TITLE_TIMEOUT, titles.lookup(title_key(thread))).await;
        let failed = match read {
            Ok(Ok(Lookup::Value(value))) => {
                let title = serde_json::from_slice::<ThreadTitle>(&value).ok().filter(|t| t.thread_id == *thread);
                return title.and_then(plain_title);
            }
            Ok(Ok(Lookup::NoValue)) => return None,
            Ok(Ok(Lookup::NoBucket)) => "there is no bucket".to_owned(),
            Ok(Err(e)) => format!("{e:#}"),
            Err(_) => "no answer in time".to_owned(),
        };
        tracing::info!(error = failed, "thread titles cannot be read; pull requests take the opening line");
        self.no_titles.store(true, Ordering::Relaxed);
        None
    }
}

/// The text of `title` for a pull request: none when sealed between humans, whose text never leaves their devices.
fn plain_title(title: ThreadTitle) -> Option<String> {
    Some(title.title).filter(|text| title.sealed.is_none() && !text.trim().is_empty())
}

/// The first line of `text` holding more than blanks, trimmed and shortened to [`MAX_TITLE`] characters.
fn opening_line(text: &str) -> Option<String> {
    let line = text.lines().map(str::trim).find(|l| !l.is_empty())?;
    Some(match line.char_indices().nth(MAX_TITLE) {
        Some((end, _)) => format!("{}…", line[..end].trim_end()),
        None => line.to_owned(),
    })
}

fn first_line(text: &str) -> Option<String> {
    text.lines().map(str::trim).find(|l| !l.is_empty()).map(str::to_owned)
}

/// `e` on one line of at most [`MAX_ERROR`] characters.
fn shortened(e: &anyhow::Error) -> String {
    let text = format!("{e:#}").split_whitespace().collect::<Vec<_>>().join(" ");
    match text.char_indices().nth(MAX_ERROR) {
        Some((end, _)) => format!("{}…", &text[..end]),
        None => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_opening_line_is_the_first_line_with_text_shortened() {
        assert_eq!(opening_line("\n  Fix the build \nthen test"), Some("Fix the build".to_owned()));
        assert_eq!(opening_line(" \n\t"), None);
        let long = opening_line(&"é".repeat(MAX_TITLE + 5)).unwrap();
        assert_eq!((long.chars().count(), long.ends_with('…')), (MAX_TITLE + 1, true));
        assert_eq!(opening_line(&"é".repeat(MAX_TITLE)).unwrap().chars().count(), MAX_TITLE);
    }
}
