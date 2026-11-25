//! Condition probes use local files and the existing command-line tools, never another network client.

use std::collections::BTreeMap;
use std::path::{Component, Path};
use std::process::Stdio;
use std::time::Duration;

use ants_proto::ToolCall;
use ants_sense::remind::{self, Condition, FileEvent, IssueState, PrState};
use ants_sense::store::Items;
use anyhow::{Context as _, Result, bail};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt as _;
use tokio::process::Command;

const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OUTPUT: u64 = 64 * 1024;

pub(super) struct Context<'a> {
    pub workdir: &'a Path,
    pub env: &'a BTreeMap<String, String>,
    pub allow: &'a [String],
    /// Whether a command runs unasked, as the ant's harness runs its tools, its allow rules not deciding.
    pub unasked: bool,
    pub items: &'a dyn Items,
    pub now: DateTime<Utc>,
}

impl Context<'_> {
    /// Whether a check may run `command`: unasked, or as the ant's tool allow rules permit.
    pub(super) fn runs(&self, command: &str) -> bool {
        self.unasked || allowed(command, self.allow)
    }
}

pub(super) struct Observation {
    pub fired: bool,
    pub value: Value,
    pub reason: &'static str,
}

pub(super) async fn evaluate(
    condition: &Condition,
    previous: Option<&Value>,
    context: &Context<'_>,
) -> Result<Observation> {
    tokio::time::timeout(TIMEOUT, probe(condition, previous, context))
        .await
        .context("condition check exceeded 30 seconds")?
}

async fn probe(condition: &Condition, previous: Option<&Value>, context: &Context<'_>) -> Result<Observation> {
    let (fired, value, reason) = match condition {
        Condition::GhPr { repo, number, state } => {
            let repo = arg(repo, "repo")?;
            let number = positive(*number, "number")?.to_string();
            let output =
                output("gh", &["pr", "view", &number, "--repo", repo, "--json", "state,mergedAt,reviews"], context)
                    .await?;
            let value: Value = serde_json::from_slice(&output).context("reading gh's pull request state")?;
            let fired = match state {
                PrState::Merged => value["mergedAt"].as_str().is_some_and(|at| !at.is_empty()),
                PrState::Closed => value["state"] == "CLOSED" || value["state"] == "MERGED",
                PrState::Reviewed => value["reviews"].as_array().is_some_and(|reviews| {
                    reviews.iter().any(|review| {
                        matches!(
                            review["state"].as_str(),
                            Some("APPROVED" | "CHANGES_REQUESTED" | "COMMENTED" | "DISMISSED")
                        )
                    })
                }),
            };
            (fired, json!(fired), "GitHub pull request condition matched")
        }
        Condition::GhWorkflow { repo, run } => {
            let repo = arg(repo, "repo")?;
            let run = positive(*run, "run")?.to_string();
            let output = output("gh", &["run", "view", &run, "--repo", repo, "--json", "status"], context).await?;
            let value: Value = serde_json::from_slice(&output).context("reading gh's workflow state")?;
            let fired = value["status"] == "completed";
            (fired, value["status"].clone(), "GitHub workflow run finished")
        }
        Condition::Bd { id, state } => {
            let id = arg(id, "id")?;
            let fired = match state {
                IssueState::Closed => {
                    let output = output("bd", &["show", id, "--json"], context).await?;
                    let value: Value = serde_json::from_slice(&output).context("reading bd's issue state")?;
                    value["status"] == "closed"
                }
                IssueState::Ready => {
                    let output = output("bd", &["ready", "--json"], context).await?;
                    let value: Value = serde_json::from_slice(&output).context("reading bd's ready issues")?;
                    value.as_array().is_some_and(|issues| issues.iter().any(|issue| issue["id"] == id))
                }
            };
            (fired, json!(fired), "bd issue condition matched")
        }
        Condition::Git { commit, reference } => {
            let commit = arg(commit, "commit")?;
            let reference = arg(reference, "reference")?;
            let fired = status("git", &["merge-base", "--is-ancestor", commit, reference], context).await?;
            (fired, json!(fired), "commit reached the git reference")
        }
        Condition::File { path, event } => {
            let value = file(arg(path, "path")?, context.workdir).await?;
            let fired = match event {
                FileEvent::Appeared => !value.is_null(),
                FileEvent::Changed => previous.is_some_and(|previous| previous != &value),
            };
            (fired, value, "file condition matched")
        }
        Condition::Command { command } => {
            let command = arg(command, "command")?;
            if !context.runs(command) {
                bail!("command trigger is outside the ant's tool allow rules");
            }
            let (program, args) = shell(command);
            let (status, digest) = digested(program, &args, context).await?;
            if status != Some(0) {
                tracing::debug!(command, ?status, "a condition's command did not exit 0");
            }
            (status == Some(0), digest, "command exited successfully")
        }
        Condition::Sense { event, id } => {
            let fired = remind::sense_condition(context.items, *event, id.as_ref(), context.now).await?;
            (fired, json!(fired), "sense event condition matched")
        }
    };
    Ok(Observation { fired, value, reason })
}

fn arg<'a>(text: &'a str, field: &str) -> Result<&'a str> {
    if text.is_empty() || text.starts_with('-') || text.contains('\0') {
        bail!("condition needs a valid {field}");
    }
    Ok(text)
}

fn positive(number: u64, field: &str) -> Result<u64> {
    if number == 0 {
        bail!("condition needs {field}");
    }
    Ok(number)
}

/// Whether the ant's tool allow rules `allow` cover running `command`.
pub(super) fn allowed(command: &str, allow: &[String]) -> bool {
    let tool = ToolCall {
        kind: "execute".into(),
        title: command.into(),
        locations: Vec::new(),
        input: Some(json!({"command": command}).to_string()),
    };
    allow.iter().any(|rule| crate::permission::covers(rule, &tool))
}

/// The program and arguments running `command` in the platform's shell.
pub(super) fn shell(command: &str) -> (&'static str, Vec<&str>) {
    #[cfg(unix)]
    return ("sh", vec!["-c", command]);
    #[cfg(windows)]
    return ("cmd.exe", vec!["/D", "/S", "/C", command]);
}

fn command(program: &str, args: &[&str], context: &Context<'_>, capture: bool) -> Command {
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(context.workdir)
        .envs(context.env)
        .stdin(Stdio::null())
        .stdout(if capture { Stdio::piped() } else { Stdio::null() })
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    command
}

struct Running(tokio::process::Child);

impl Drop for Running {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(pid) = self.0.id().and_then(|pid| rustix::process::Pid::from_raw(pid.cast_signed())) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        }
        let _ = self.0.start_kill();
    }
}

async fn status(program: &str, args: &[&str], context: &Context<'_>) -> Result<bool> {
    let mut child = Running(command(program, args, context, false).spawn().context("starting a condition check")?);
    Ok(crate::process::wait(&mut child.0).await.context("waiting for a condition check")?.success())
}

/// The exit status of `program` run with `args`, none when a signal ended it, and the observation of a command
/// condition: the digest of its stdout, read whatever its length, and that status.
async fn digested(program: &str, args: &[&str], context: &Context<'_>) -> Result<(Option<i32>, Value)> {
    let mut child = Running(command(program, args, context, true).spawn().context("starting a condition check")?);
    let mut stdout = child.0.stdout.take().context("condition stdout missing")?;
    let mut digest = remind::CommandDigest::default();
    let mut buffer = [0; 8192];
    loop {
        let read = stdout.read(&mut buffer).await.context("reading a condition check's stdout")?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let status = crate::process::wait(&mut child.0).await.context("waiting for a condition check")?.code();
    Ok((status, digest.finish(status)))
}

/// The exit status of `program` run with `args` and its stdout, failing past [`MAX_OUTPUT`]; no status when a signal
/// ended it. Takes at most 30 seconds.
pub(super) async fn captured(program: &str, args: &[&str], context: &Context<'_>) -> Result<(Option<i32>, Vec<u8>)> {
    tokio::time::timeout(TIMEOUT, async {
        let mut child = Running(command(program, args, context, true).spawn().context("starting the command")?);
        let mut bytes = Vec::new();
        child
            .0
            .stdout
            .take()
            .context("the command's stdout is missing")?
            .take(MAX_OUTPUT + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() as u64 > MAX_OUTPUT {
            bail!("the command wrote over 64 KiB");
        }
        Ok((crate::process::wait(&mut child.0).await.context("waiting for the command")?.code(), bytes))
    })
    .await
    .context("the command took over 30 seconds")?
}

async fn output(program: &str, args: &[&str], context: &Context<'_>) -> Result<Vec<u8>> {
    let mut child = Running(command(program, args, context, true).spawn().context("starting a condition check")?);
    let mut bytes = Vec::new();
    child.0.stdout.take().context("condition stdout missing")?.take(MAX_OUTPUT + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > MAX_OUTPUT {
        bail!("condition response exceeds 64 KiB");
    }
    let status = crate::process::wait(&mut child.0).await?;
    if !status.success() {
        bail!("{program} answering the condition failed ({status})");
    }
    Ok(bytes)
}

async fn file(path: &str, workdir: &Path) -> Result<Value> {
    let path = Path::new(path);
    if path.is_absolute() || path.components().any(|part| matches!(part, Component::ParentDir | Component::Prefix(_))) {
        bail!("file condition must stay within the workdir");
    }
    let root = tokio::fs::canonicalize(workdir).await?;
    let path = match tokio::fs::canonicalize(root.join(path)).await {
        Ok(path) => path,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Value::Null),
        Err(e) => return Err(e.into()),
    };
    if !path.starts_with(&root) {
        bail!("file condition leaves the workdir");
    }
    let mut file = tokio::fs::File::open(path).await?;
    if !file.metadata().await?.is_file() {
        bail!("file condition names no regular file");
    }
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buffer = [0; 8192];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(json!(data_encoding::HEXLOWER.encode(digest.finish().as_ref())))
}

#[cfg(test)]
mod tests;
