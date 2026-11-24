//! The handler answering the permission requests of a Copilot CLI session.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::{ToolCallLocation, ToolCallUpdate, ToolCallUpdateFields, ToolKind};
use copilot_sdk as sdk;
use sdk::handler::PermissionResult;
use sdk::types::{PermissionRequestKind, RequestId};
use sdk::{PermissionRequestData, SessionId};

use crate::harness::native::{Cli, Updates, approve, deny_approval};
use crate::sync::MutexExt as _;

/// Where the permission requests of a session go: the prompt running, if any.
pub(super) type Asking = Arc<Mutex<Option<Updates>>>;

/// The handler answering a session's permission requests: one that managed policy says a human must decide is
/// denied; else every one is approved, as the tools left to the model are the ones the definition enables, unless the
/// ant asks ants about its tools, when ants decide ([`approve`]).
pub(super) struct Permissions {
    pub(super) cli: Cli,
    pub(super) asking: Asking,
}

impl sdk::handler::PermissionHandler for Permissions {
    fn handle<'a, 'b>(
        &'a self,
        _session: SessionId,
        _request: RequestId,
        data: PermissionRequestData,
    ) -> Pin<Box<dyn Future<Output = PermissionResult> + Send + 'b>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            let described = data.extra.get("permissionRequest").unwrap_or(&data.extra).to_string();
            if data.managed_approval_required == Some(true) {
                deny_approval(&self.cli, &described);
                return PermissionResult::reject(None);
            }
            if !self.cli.asks() {
                tracing::debug!(
                    harness = self.cli.name(),
                    request = described,
                    "approved a tool request of the harness"
                );
                return PermissionResult::approve_once();
            }
            let updates = self.asking.locked().clone();
            if approve(&self.cli, updates, asked_call(&data), &described).await {
                PermissionResult::approve_once()
            } else {
                PermissionResult::reject(Some("The ant's tool permissions did not allow it.".to_owned()))
            }
        })
    }
}

/// The tool call a permission request of the CLI asks to run, by the id of the one it reported.
fn asked_call(data: &PermissionRequestData) -> ToolCallUpdate {
    let request = data.extra.get("permissionRequest").unwrap_or(&data.extra);
    let text = |key: &str| request[key].as_str().unwrap_or_default().to_owned();
    let (kind, title, path) = match data.kind {
        Some(PermissionRequestKind::Shell) => (ToolKind::Execute, text("fullCommandText"), None),
        Some(PermissionRequestKind::Write) => {
            (ToolKind::Edit, format!("Write {}", text("fileName")), Some(text("fileName")))
        }
        Some(PermissionRequestKind::Read) => (ToolKind::Read, format!("Read {}", text("path")), Some(text("path"))),
        Some(PermissionRequestKind::Url) => (ToolKind::Fetch, format!("Fetch {}", text("url")), None),
        Some(PermissionRequestKind::Mcp) => {
            (ToolKind::Other, format!("{}/{}", text("serverName"), text("toolName")), None)
        }
        _ => (ToolKind::Other, text("intention"), None),
    };
    let id = data.tool_call_id.clone().or_else(|| request["toolCallId"].as_str().map(str::to_owned));
    let fields = ToolCallUpdateFields::new()
        .kind(kind)
        .title(title)
        .raw_input(request.clone())
        .locations(path.filter(|path| !path.is_empty()).map(|path| vec![ToolCallLocation::new(path)]));
    ToolCallUpdate::new(id.unwrap_or_else(|| "permission".to_owned()), fields)
}
