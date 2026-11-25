//! The control channel of a Claude Code process: the control requests sent to it, each awaiting its response, and
//! its own, a tool request among them, answered; and the turns it runs of its own between prompts, reported as they
//! start ([`Own`]).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use agent_client_protocol::Error;
use agent_client_protocol::v1::schema::{ToolCallLocation, ToolCallUpdate, ToolCallUpdateFields};
use serde_json::{Value, json};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{mpsc, oneshot};

use super::turn::describe;
use crate::harness::native::io::forward;
use crate::harness::native::{Cli, JsonLines, JsonWriter, Process, Updates, approve, failed};
use crate::harness::own_turn::{OwnTurn, OwnTurnReport};
use crate::sync::MutexExt as _;

/// What a tool request Claude Code asks about is answered with, unless ants allow it.
const DENIED: &str = "Denied: this harness runs only the tools its permission settings allow.";

/// What a tool request ants were asked about and did not allow is answered with.
const REJECTED: &str = "Rejected: the ant's allow rules and approvers did not allow this tool call.";

/// What a session's process and its reader share.
pub(super) struct Shared {
    pub(super) cli: Cli,
    /// The session's working directory, which tool titles are relative to.
    pub(super) cwd: PathBuf,
    /// Where the tool requests of the turn running go, when the ant asks ants about its tools.
    pub(super) asking: Mutex<Option<Updates>>,
    pub(super) writer: JsonWriter<ChildStdin>,
    /// Taken when the session closes.
    pub(super) process: tokio::sync::Mutex<Option<Process>>,
    /// Control requests awaiting their response, by id; None once the CLI's output ended.
    pub(super) pending: Mutex<Option<HashMap<String, oneshot::Sender<Value>>>>,
    pub(super) requests: AtomicU64,
    pub(super) own: Mutex<Own>,
    /// Where the turns the CLI runs of its own go: a session serving a thread.
    pub(super) own_turns: Option<OwnTurnReport>,
}

/// The type of the line the reader puts before the events of a turn Claude Code runs of its own, naming it by `id`.
pub(super) const OWN_TURN: &str = "ants_own_turn";

/// The turns Claude Code runs of its own between prompts, as it does once a background task it started ends after the
/// turn that started it (`system/task_notification`) or a wakeup it scheduled comes: the reader reports each as it
/// starts ([`OwnTurnReport`]) and puts a line of [`OWN_TURN`] before its events, for the prompt that takes it, marked
/// `dropped` when the report refused it.
#[derive(Debug, Default)]
pub(super) struct Own {
    /// Whether a prompt runs, or a fork's first message: the turns are then the prompt's.
    prompting: bool,
    /// The background tasks whose end the CLI told since the last turn, as their notices say it.
    notices: Vec<String>,
    /// The turn the CLI runs of its own, until it ends.
    open: Option<String>,
    /// Whether the CLI reports its state (`session_state_changed`): a turn then ends once it is idle, else at its
    /// `result`.
    states: bool,
}

impl Own {
    /// Whether `event` ends a turn.
    pub(super) fn ends(&self, event: &Value) -> bool {
        match (event["type"].as_str(), event["subtype"].as_str()) {
            (Some("system"), Some("session_state_changed")) => event["state"] == "idle",
            (Some("result"), _) => !self.states,
            _ => false,
        }
    }
}

/// A prompt running ([`Own::prompting`]), until dropped.
pub(super) struct Prompting<'a>(&'a Shared);

impl Drop for Prompting<'_> {
    fn drop(&mut self) {
        self.0.own.locked().prompting = false;
    }
}

impl Shared {
    /// Count the turns the CLI runs as a prompt's until the guard drops.
    pub(super) fn prompting(&self) -> Prompting<'_> {
        self.own.locked().prompting = true;
        Prompting(self)
    }

    /// Whether `event` ends a turn ([`Own::ends`]).
    pub(super) fn ends_turn(&self, event: &Value) -> bool {
        self.own.locked().ends(event)
    }

    /// Note what `event`, about to go to `events`, says of a turn of the CLI's own: the notice of a background task
    /// that ended, or the start of a turn while no prompt runs, which is reported and marked in `events` before it.
    fn own_turn(&self, event: &Value, events: &mpsc::UnboundedSender<Value>) {
        let mut own = self.own.locked();
        let kind = (event["type"].as_str(), event["subtype"].as_str());
        if kind == (Some("system"), Some("session_state_changed")) {
            own.states = true;
        }
        if own.open.is_some() {
            if own.ends(event) {
                own.open = None;
            }
            return;
        }
        if own.prompting {
            return;
        }
        let starts = match kind {
            (Some("system"), Some("task_notification")) => {
                own.notices.push(notice(event));
                return;
            }
            (Some("system"), Some("session_state_changed")) => event["state"] == "running",
            (Some("system"), Some("init")) => true,
            _ => false,
        };
        if !starts {
            return;
        }
        let id = uuid::Uuid::new_v4().to_string();
        own.open = Some(id.clone());
        let notices = std::mem::take(&mut own.notices);
        drop(own);
        let text = if notices.is_empty() {
            "[ants] Claude Code started a turn of its own.".to_owned()
        } else {
            notices.iter().map(|notice| format!("[ants] {notice}")).collect::<Vec<_>>().join("\n")
        };
        let taken = match &self.own_turns {
            Some(report) => report.send(OwnTurn { id: id.clone(), text }),
            None => {
                tracing::debug!(harness = self.cli.name(), why = text, "Claude Code runs a turn of its own");
                true
            }
        };
        forward(events, json!({ "type": OWN_TURN, "id": id, "dropped": !taken }));
    }

    /// Send control request `request` and await its response.
    pub(super) async fn control(&self, request: Value) -> Result<Value, Error> {
        let id = format!("ants-{}", self.requests.fetch_add(1, Ordering::Relaxed) + 1);
        let (tx, rx) = oneshot::channel();
        let open = self.pending.locked().as_mut().map(|pending| pending.insert(id.clone(), tx)).is_some();
        if !open {
            return Err(self.exited().await);
        }
        let subtype = request["subtype"].as_str().unwrap_or_default().to_owned();
        if self.writer.send(&control_request(&id, &request)).await.is_err() {
            return Err(self.exited().await);
        }
        let Ok(response) = rx.await else { return Err(self.exited().await) };
        if response["subtype"] == "success" {
            Ok(response["response"].clone())
        } else {
            Err(failed(format!("{}: {subtype}: {}", self.cli.name(), response["error"].as_str().unwrap_or("failed"))))
        }
    }

    /// Answer a control request of the CLI: a tool request as ants decide ([`approve`]), refusing anything else.
    pub(super) async fn answer(&self, message: &Value) {
        let (id, request) = (&message["request_id"], &message["request"]);
        let response = if request["subtype"] == "can_use_tool" {
            let tool = request["tool_name"].as_str().unwrap_or("a tool");
            let input = &request["input"];
            let (kind, title) = describe(tool, input, &self.cwd);
            let path = ["file_path", "notebook_path", "path"].iter().find_map(|key| input[key].as_str());
            let fields = ToolCallUpdateFields::new()
                .kind(kind)
                .title(title)
                .raw_input(input.clone())
                .locations(path.map(|path| vec![ToolCallLocation::new(path)]));
            let call_id = request["tool_use_id"].as_str().or(id.as_str()).unwrap_or_default();
            let call = ToolCallUpdate::new(call_id.to_owned(), fields);
            let updates = self.asking.locked().clone();
            let decided = if approve(&self.cli, updates, call, &format!("{tool} {input}")).await {
                json!({ "behavior": "allow", "updatedInput": input })
            } else {
                json!({ "behavior": "deny", "message": if self.cli.asks() { REJECTED } else { DENIED } })
            };
            json!({ "subtype": "success", "request_id": id, "response": decided })
        } else {
            let subtype = request["subtype"].as_str().unwrap_or_default();
            json!({ "subtype": "error", "request_id": id, "error": format!("Unsupported control request subtype: {subtype}") })
        };
        if let Err(e) = self.writer.send(&json!({ "type": "control_response", "response": response })).await {
            tracing::debug!(error = %e, "answering a control request of Claude Code");
        }
    }

    /// The error of the CLI's output ending.
    pub(super) async fn exited(&self) -> Error {
        match self.process.lock().await.as_mut() {
            Some(process) => process.exited().await,
            None => failed(format!("{} stopped", self.cli.name())),
        }
    }
}

/// What the notice of a background task's end (`system/task_notification`) says of it: its summary, such as
/// `Background command "…" completed (exit code 0)`, and where its output is.
fn notice(event: &Value) -> String {
    let summary = match event["summary"].as_str().filter(|summary| !summary.is_empty()) {
        Some(summary) => summary.to_owned(),
        None => {
            let (task, status) =
                (event["task_id"].as_str().unwrap_or("?"), event["status"].as_str().unwrap_or("ended"));
            format!("Background task {task} {status}")
        }
    };
    match event["output_file"].as_str().filter(|file| !file.is_empty()) {
        Some(file) => format!("{summary}; its output is in {file}."),
        None => format!("{summary}."),
    }
}

/// Control request `request` as Claude Code reads it, the response coming under `id`.
pub(super) fn control_request(id: &str, request: &Value) -> Value {
    json!({ "type": "control_request", "request_id": id, "request": request })
}

/// Read the CLI's output: responses to control requests to whoever awaits them, its own control requests answered,
/// everything else to `events`.
pub(super) async fn read(mut lines: JsonLines<ChildStdout>, shared: Arc<Shared>, events: mpsc::UnboundedSender<Value>) {
    loop {
        let message = match lines.next().await {
            Ok(Some(message)) => message,
            Ok(None) => break,
            Err(e) => {
                tracing::warn!(harness = shared.cli.name(), error = %e, "reading Claude Code");
                break;
            }
        };
        match message["type"].as_str() {
            Some("control_response") => {
                let response = &message["response"];
                let id = response["request_id"].as_str().unwrap_or_default();
                let waiting = shared.pending.locked().as_mut().and_then(|pending| pending.remove(id));
                if let Some(waiting) = waiting {
                    let _ = waiting.send(response.clone());
                }
            }
            // Answered off the loop, as ants may take a while to decide on a tool request.
            Some("control_request") => {
                let shared = shared.clone();
                tokio::spawn(async move { shared.answer(&message).await });
            }
            Some("control_cancel_request") => {}
            _ => {
                shared.own_turn(&message, &events);
                forward(&events, message);
            }
        }
    }
    shared.pending.locked().take();
}
