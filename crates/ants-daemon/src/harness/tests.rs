//! Tests of the ACP client against fake ACP agents ([`agent`]) and definitions, one submodule for each behaviour.

use std::assert_matches;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use agent_client_protocol::v1::schema::{
    AgentCapabilities, AuthMethod, AuthMethodTerminal, ContentBlock, ContentChunk, Implementation, InitializeRequest,
    InitializeResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest, NewSessionResponse,
    PermissionOption, PermissionOptionKind, PromptRequest, PromptResponse, RequestPermissionOutcome,
    RequestPermissionRequest, SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectGroup, SessionConfigSelectOption, SessionMode, SessionModeState, SessionNotification,
    SessionUpdate, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse, SetSessionModeRequest,
    SetSessionModeResponse, StopReason, TextContent, ToolCall, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
    ToolKind,
};
use agent_client_protocol::{Agent, Client, DynConnectTo, HandleDispatchFrom};
use ants_proto::HarnessAgent;
use anyhow::Result;
use serde_json::json;

use super::launch::Connector;
use super::*;
use crate::limits::ratelimit::Limited;
use crate::permission::{Allowlist, PermissionPolicy};
use crate::sync::MutexExt as _;
use ants_proto::LimitKind;

mod definitions;
mod modes;
mod permissions;
mod replies;
mod settings;
mod sign_in;

/// How a fake ACP agent answers `session/new`.
type Opened = Result<NewSessionResponse, agent_client_protocol::Error>;

/// A fake ACP agent named `name`, answering `initialize` with what `hello` makes of the plain answer and
/// `session/new` as `opened` says; a fake adds the handlers of what else it plays.
fn agent(
    name: &'static str,
    hello: impl Fn(InitializeResponse) -> InitializeResponse + Send + Sync + 'static,
    opened: impl Fn(NewSessionRequest) -> Opened + Send + Sync + 'static,
) -> agent_client_protocol::v1::Builder<Agent, impl HandleDispatchFrom<Client>> {
    Agent
        .v1()
        .name(name)
        .on_receive_request(
            async move |init: InitializeRequest, responder, _cx| {
                responder.respond(hello(InitializeResponse::new(init.protocol_version)))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async move |request: NewSessionRequest, responder, _cx| responder.respond_with_result(opened(request)),
            agent_client_protocol::on_receive_request!(),
        )
}

/// A fake ACP agent named `name` with nothing to say of itself, opening session `s1`.
fn plain(name: &'static str) -> agent_client_protocol::v1::Builder<Agent, impl HandleDispatchFrom<Client>> {
    agent(name, |hello| hello, |_| Ok(NewSessionResponse::new("s1")))
}

/// What a fake ACP agent that resumes sessions makes of its plain answer to `initialize`.
fn loading(hello: InitializeResponse) -> InitializeResponse {
    hello.agent_capabilities(AgentCapabilities::new().load_session(true))
}

/// The harness a config file defines as `[harnesses.custom]` with `table`.
fn custom(table: &str) -> Arc<Harness> {
    let definitions = parse_definitions(&format!("[harnesses.custom]\n{table}")).unwrap();
    Harnesses::builtin().define(definitions).unwrap().get("custom").unwrap()
}

fn builtin(name: &str) -> Arc<Harness> {
    Harnesses::builtin().get(name).unwrap()
}
