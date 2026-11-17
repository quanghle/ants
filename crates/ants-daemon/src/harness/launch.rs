//! How to start a harness and open sessions in it, for an ant and the thread its sessions serve.

#[cfg(test)]
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_client_protocol::v1::schema::McpServer;
use agent_client_protocol::{Client, DynConnectTo};
use ants_proto::ThreadId;

#[cfg(test)]
use super::Protocol;
use super::{Harness, ModelSettings};
use crate::permission::PermissionPolicy;

/// Makes the transport to a fresh harness: a process, or an in-process agent in tests.
pub(super) type Connector = Arc<dyn Fn() -> DynConnectTo<Client> + Send + Sync>;

/// The MCP servers a session gets.
pub type McpServers = Arc<dyn Fn(&McpSession<'_>) -> Vec<McpServer> + Send + Sync>;

/// A session MCP servers are made for.
#[derive(Debug, Clone, Copy)]
pub struct McpSession<'a> {
    pub thread: &'a ThreadId,
    /// The directory the session works in.
    pub dir: &'a Path,
    /// The harness running it, by its name in the config.
    pub harness: &'a str,
    /// The model the config sets for it, if any.
    pub model: Option<&'a str>,
    /// Whether the harness takes HTTP MCP servers.
    pub http: bool,
}

impl<'a> McpSession<'a> {
    /// A session of `thread` working in `dir`, of a harness taking HTTP MCP servers when `http`.
    #[cfg(test)]
    pub(crate) fn test(thread: &'a ThreadId, dir: &'a Path, http: bool) -> Self {
        Self { thread, dir, harness: "test", model: None, http }
    }
}

/// How to start a harness and open sessions in it.
#[derive(Clone)]
pub struct Launch {
    pub(super) harness: Arc<Harness>,
    pub(super) settings: ModelSettings,
    pub(super) connect: Connector,
    pub(super) cwd: PathBuf,
    pub(super) policy: PermissionPolicy,
    /// The thread the sessions serve, so a permission request can be escalated on it.
    pub(super) thread: Option<ThreadId>,
    pub(super) mcp: Option<McpServers>,
    pub(super) instructions: String,
    /// The environment the harness process gets over the daemon's, to find its executable; None for an in-process
    /// agent.
    pub(super) process_env: Option<Vec<(String, String)>>,
}

impl Launch {
    /// `harness` working in `cwd`, with its own environment and then `env` added to the daemon's.
    pub fn new(
        harness: Arc<Harness>,
        cwd: PathBuf,
        env: Vec<(String, String)>,
        policy: PermissionPolicy,
        settings: ModelSettings,
    ) -> Self {
        let (process, process_settings, process_env) = (harness.clone(), settings.clone(), env.clone());
        let connect: Connector = Arc::new(move || process.agent(&process_env, &process_settings));
        let process_env = Some(env);
        Self {
            harness,
            settings,
            connect,
            cwd,
            policy,
            thread: None,
            mcp: None,
            instructions: String::new(),
            process_env,
        }
    }

    /// The same launch, running `fallback` instead of the ant's own harness and model.
    #[must_use]
    pub fn with_fallback(&self, fallback: &crate::limits::fallback::Fallback) -> Self {
        let (process, settings) = (fallback.harness.clone(), fallback.settings.clone());
        let env = self.process_env.clone().unwrap_or_default();
        let connect: Connector = Arc::new(move || process.agent(&env, &settings));
        Self { harness: fallback.harness.clone(), settings: fallback.settings.clone(), connect, ..self.clone() }
    }

    /// The name of the harness it starts.
    pub fn harness_name(&self) -> &str {
        &self.harness.name
    }

    /// Whether the harness it starts runs its tools without asking ([`Harness::bypasses_permissions`]).
    pub fn bypasses_permissions(&self) -> bool {
        self.harness.bypasses_permissions()
    }

    /// The same launch, for sessions serving `thread`.
    #[must_use]
    pub fn for_thread(&self, thread: &ThreadId) -> Self {
        Self { thread: Some(thread.clone()), ..self.clone() }
    }

    /// The same launch, with sessions working in `cwd`.
    #[must_use]
    pub fn in_dir(self, cwd: PathBuf) -> Self {
        Self { cwd, ..self }
    }

    /// The same launch, offering `mcp` to the sessions of each thread.
    #[must_use]
    pub fn with_mcp(self, mcp: McpServers) -> Self {
        Self { mcp: Some(mcp), ..self }
    }

    /// Prepend the ant's instructions to every prompt, including after session/load.
    #[must_use]
    pub fn with_instructions(self, instructions: String) -> Self {
        Self { instructions, ..self }
    }

    pub(super) fn mcp_servers(&self, http: bool) -> Vec<McpServer> {
        match (&self.mcp, &self.thread) {
            (Some(mcp), Some(thread)) => mcp(&McpSession {
                thread,
                dir: &self.cwd,
                harness: &self.harness.name,
                model: self.settings.model.as_deref(),
                http,
            }),
            _ => Vec::new(),
        }
    }

    /// What answers the permission requests of the harness's sessions; its clones share narrowed threads.
    pub(crate) fn policy(&self) -> &PermissionPolicy {
        &self.policy
    }

    /// The same launch, answering permission requests with `policy`.
    #[cfg(all(test, unix))]
    #[must_use]
    pub(crate) fn with_policy(self, policy: PermissionPolicy) -> Self {
        Self { policy, ..self }
    }

    #[cfg(test)]
    pub(crate) fn with_transport(connect: Connector, cwd: PathBuf, policy: PermissionPolicy) -> Self {
        let harness = Harness {
            name: "test".to_owned(),
            file: None,
            command: "test".into(),
            protocol: Protocol::Acp,
            args: Vec::new(),
            env: BTreeMap::new(),
            mode: None,
            bypass_permissions: false,
            context: BTreeMap::new(),
            sign_in: None,
            entra_client_id: None,
            rate_limited: Vec::new(),
            quota_exhausted: Vec::new(),
            context_overflow: Vec::new(),
            window: None,
            history_budget: None,
            memory_files: None,
        };
        Self {
            harness: Arc::new(harness),
            settings: ModelSettings::default(),
            connect,
            cwd,
            policy,
            thread: None,
            mcp: None,
            instructions: String::new(),
            process_env: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_harness(self, harness: Arc<Harness>) -> Self {
        Self { harness, ..self }
    }

    /// The same launch, its harness named `name`.
    #[cfg(test)]
    pub(crate) fn named(self, name: &str) -> Self {
        let harness = Arc::new(Harness { name: name.to_owned(), ..(*self.harness).clone() });
        self.with_harness(harness)
    }
}

impl fmt::Debug for Launch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launch")
            .field("harness", &self.harness.name)
            .field("settings", &self.settings)
            .field("cwd", &self.cwd)
            .field("policy", &self.policy)
            .field("thread", &self.thread)
            .finish()
    }
}
