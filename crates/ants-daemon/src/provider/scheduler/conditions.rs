//! Condition probes use local files and the existing command-line tools, never another network client.

use std::collections::BTreeMap;
use std::path::{Component, Path};
use std::process::Stdio;
use std::time::Duration;

use ants_proto::ToolCall;
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
    pub items: &'a dyn Items,
    pub now: DateTime<Utc>,
}

pub(super) struct Observation {
    pub fired: bool,
    pub value: Value,
    pub reason: &'static str,
}

pub(super) async fn evaluate(
    condition: &Value,
    previous: Option<&Value>,
    context: &Context<'_>,
) -> Result<Observation> {
    tokio::time::timeout(TIMEOUT, probe(condition, previous, context))
        .await
        .context("condition check exceeded 30 seconds")?
}

async fn probe(condition: &Value, previous: Option<&Value>, context: &Context<'_>) -> Result<Observation> {
    let (fired, value, reason) = match text(condition, "kind")? {
        "gh_pr" => {
            let repo = text(condition, "repo")?;
            let number = positive(condition, "number")?.to_string();
            let output =
                output("gh", &["pr", "view", &number, "--repo", repo, "--json", "state,mergedAt,reviews"], context)
                    .await?;
            let value: Value = serde_json::from_slice(&output).context("reading gh's pull request state")?;
            let fired = match text(condition, "state")? {
                "merged" => value["mergedAt"].as_str().is_some_and(|at| !at.is_empty()),
                "closed" => value["state"] == "CLOSED" || value["state"] == "MERGED",
                "reviewed" => value["reviews"].as_array().is_some_and(|reviews| {
                    reviews.iter().any(|review| {
                        matches!(
                            review["state"].as_str(),
                            Some("APPROVED" | "CHANGES_REQUESTED" | "COMMENTED" | "DISMISSED")
                        )
                    })
                }),
                _ => bail!("unknown pull request condition"),
            };
            (fired, json!(fired), "GitHub pull request condition matched")
        }
        "gh_workflow" => {
            let repo = text(condition, "repo")?;
            let run = positive(condition, "run")?.to_string();
            let output = output("gh", &["run", "view", &run, "--repo", repo, "--json", "status"], context).await?;
            let value: Value = serde_json::from_slice(&output).context("reading gh's workflow state")?;
            let fired = value["status"] == "completed";
            (fired, value["status"].clone(), "GitHub workflow run finished")
        }
        "bd" => {
            let id = text(condition, "id")?;
            let fired = match text(condition, "state")? {
                "closed" => {
                    let output = output("bd", &["show", id, "--json"], context).await?;
                    let value: Value = serde_json::from_slice(&output).context("reading bd's issue state")?;
                    value["status"] == "closed"
                }
                "ready" => {
                    let output = output("bd", &["ready", "--json"], context).await?;
                    let value: Value = serde_json::from_slice(&output).context("reading bd's ready issues")?;
                    value.as_array().is_some_and(|issues| issues.iter().any(|issue| issue["id"] == id))
                }
                _ => bail!("unknown bd condition"),
            };
            (fired, json!(fired), "bd issue condition matched")
        }
        "git" => {
            let commit = text(condition, "commit")?;
            let reference = text(condition, "reference")?;
            let fired = status("git", &["merge-base", "--is-ancestor", commit, reference], context).await?;
            (fired, json!(fired), "commit reached the git reference")
        }
        "file" => {
            let value = file(text(condition, "path")?, context.workdir).await?;
            let fired = match text(condition, "event")? {
                "appeared" => !value.is_null(),
                "changed" => previous.is_some_and(|previous| previous != &value),
                _ => bail!("unknown file condition"),
            };
            (fired, value, "file condition matched")
        }
        "command" => {
            let command = text(condition, "command")?;
            let tool = ToolCall {
                kind: "execute".into(),
                title: command.into(),
                locations: Vec::new(),
                input: Some(json!({"command": command}).to_string()),
            };
            if !context.allow.iter().any(|rule| crate::permission::covers(rule, &tool)) {
                bail!("command trigger is outside the ant's tool allow rules");
            }
            #[cfg(unix)]
            let fired = status("sh", &["-c", command], context).await?;
            #[cfg(windows)]
            let fired = status("cmd.exe", &["/D", "/S", "/C", command], context).await?;
            (fired, json!(fired), "command exited successfully")
        }
        "sense" => {
            let fired = sense_event(condition, context).await?;
            (fired, json!(fired), "sense event condition matched")
        }
        _ => bail!("unknown condition kind"),
    };
    Ok(Observation { fired, value, reason })
}

async fn sense_event(condition: &Value, context: &Context<'_>) -> Result<bool> {
    let kind = match text(condition, "event")? {
        "commitment_overdue" => "promise",
        "lease_expired" => "task",
        _ => bail!("unknown sense event condition"),
    };
    let items = context.items.list(kind, usize::MAX).await?;
    let id = condition.get("id").and_then(Value::as_str);
    Ok(items.iter().any(|item| {
        if id.is_some_and(|id| item.id.as_str() != id) {
            return false;
        }
        if kind == "promise" {
            item.text("status") == "open" && due(item.values.get("due"), context.now)
        } else {
            item.text("status") == "in_progress"
                && due(item.values.get("lease").and_then(|lease| lease.get("expires_at")), context.now)
        }
    }))
}

fn due(value: Option<&Value>, now: DateTime<Utc>) -> bool {
    value.and_then(Value::as_str).and_then(|at| at.parse::<DateTime<Utc>>().ok()).is_some_and(|at| at <= now)
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    let text = value.get(field).and_then(Value::as_str).filter(|text| !text.is_empty());
    match text {
        Some(text) if !text.starts_with('-') && !text.contains('\0') => Ok(text),
        _ => bail!("condition needs a valid {field}"),
    }
}

fn positive(value: &Value, field: &str) -> Result<u64> {
    value.get(field).and_then(Value::as_u64).filter(|number| *number > 0).context(format!("condition needs {field}"))
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
    Ok(child.0.wait().await.context("waiting for a condition check")?.success())
}

async fn output(program: &str, args: &[&str], context: &Context<'_>) -> Result<Vec<u8>> {
    let mut child = Running(command(program, args, context, true).spawn().context("starting a condition check")?);
    let mut bytes = Vec::new();
    child.0.stdout.take().context("condition stdout missing")?.take(MAX_OUTPUT + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > MAX_OUTPUT {
        bail!("condition response exceeds 64 KiB");
    }
    if !child.0.wait().await?.success() {
        bail!("condition command did not succeed");
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
