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
use super::mcp_sign_in::Report;
use super::{Harness, McpSignIns, ModelSettings, Skills, skills};
use crate::permission::PermissionPolicy;

/// Makes the transport to a fresh harness: a process, or an in-process agent in tests.
pub(crate) type Connector = Arc<dyn Fn() -> DynConnectTo<Client> + Send + Sync>;

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
    /// Where the sign-ins to MCP servers of the thread's sessions go.
    pub(super) mcp_sign_ins: Option<McpSignIns>,
    pub(super) instructions: String,
    /// The environment the harness process gets over the daemon's, to find its executable; None for an in-process
    /// agent.
    pub(super) process_env: Option<Vec<(String, String)>>,
    /// The ant's confirmed sense skills, offered to the harness once their directory holds one ([`skills`]).
    pub(super) skills: Option<Skills>,
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
        let connect = connector(&harness, &settings, env.clone(), None, None);
        let process_env = Some(env);
        Self {
            harness,
            settings,
            connect,
            cwd,
            policy,
            thread: None,
            mcp: None,
            mcp_sign_ins: None,
            instructions: String::new(),
            process_env,
            skills: None,
        }
    }

    /// The same launch, running `fallback` instead of the ant's own harness and model.
    #[must_use]
    pub fn with_fallback(&self, fallback: &crate::limits::fallback::Fallback) -> Self {
        let env = self.process_env.clone().unwrap_or_default();
        let connect = connector(&fallback.harness, &fallback.settings, env, self.report(), self.skills.clone());
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

    /// The same launch, for sessions reviewing for a sage: on the harness's review variant ([`Harness::reviewing`]),
    /// answering permission requests held to [`ants_proto::REVIEW_ALLOW`] ([`PermissionPolicy::for_review`]). None
    /// when the harness runs its tools unasked and sets no review.
    pub fn for_review(&self) -> Option<Self> {
        let harness = Arc::new(self.harness.reviewing()?);
        let policy = self.policy.for_review(self.harness.bypasses_permissions());
        Some(Self { harness, policy, ..self.clone() }.reporting())
    }

    /// Why a review is declined on the harness it starts ([`Harness::unheld_review`]).
    pub fn unheld_review(&self) -> Option<String> {
        self.harness.unheld_review()
    }

    /// The same launch, for sessions serving `thread`.
    #[must_use]
    pub fn for_thread(&self, thread: &ThreadId) -> Self {
        Self { thread: Some(thread.clone()), ..self.clone() }.reporting()
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

    /// The same launch, sending the sign-ins to MCP servers its sessions serving a thread make to `sign_ins`.
    #[must_use]
    pub fn with_mcp_sign_ins(self, sign_ins: McpSignIns) -> Self {
        Self { mcp_sign_ins: Some(sign_ins), ..self }.reporting()
    }

    /// Where the sign-ins to MCP servers of the thread's sessions go, once both are known: sessions of no thread, such
    /// as the probes of a harness, sign in to none.
    fn report(&self) -> Option<Report> {
        Some(Report::new(self.thread.clone()?, self.mcp_sign_ins.clone()?))
    }

    /// The same launch, its harness processes reporting the sign-ins to MCP servers of their sessions ([`Self::report`]).
    fn reporting(self) -> Self {
        let Some(env) = self.process_env.clone() else { return self };
        Self { connect: connector(&self.harness, &self.settings, env, self.report(), self.skills.clone()), ..self }
    }

    /// The same launch, offering its harness `skills` through the harness's own skills mechanism.
    #[must_use]
    pub fn with_skills(self, skills: Skills) -> Self {
        Self { skills: Some(skills), ..self }.reporting()
    }

    /// The skills to offer a session opening now: None until their directory holds one.
    pub(super) fn skills(&self) -> Option<&Skills> {
        skills::offered(self.skills.as_ref()?)
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
            review: None,
            tools: Default::default(),
            ask: None,
            context: BTreeMap::new(),
            sign_in: None,
            entra_client_id: None,
            mcp_sign_in: Vec::new(),
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
            mcp_sign_ins: None,
            instructions: String::new(),
            process_env: None,
            skills: None,
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

/// A connector starting `harness` with the ant's `env` and `settings`, reporting sign-ins to MCP servers to `report`
/// and offering `skills` when it starts.
fn connector(
    harness: &Arc<Harness>,
    settings: &ModelSettings,
    env: Vec<(String, String)>,
    report: Option<Report>,
    skills: Option<Skills>,
) -> Connector {
    #[cfg(test)]
    if let Some(fake) = fakes::of(&harness.name) {
        return fake;
    }
    let (harness, settings) = (harness.clone(), settings.clone());
    Arc::new(move || {
        let offered = skills.as_ref().and_then(skills::offered);
        match offered.and_then(|offered| skills::env(&harness, &env, &offered.dir)) {
            Some(skills) => harness.agent(&[env.as_slice(), &[skills]].concat(), &settings, report.clone()),
            None => harness.agent(&env, &settings, report.clone()),
        }
    })
}

/// In-process harnesses that tests start in place of the processes of the harnesses they name, so a daemon run whole
/// ([`crate::daemon::run_until`]) prompts them.
#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    use super::Connector;
    use crate::sync::MutexExt as _;

    static FAKES: LazyLock<Mutex<HashMap<String, Connector>>> = LazyLock::new(Mutex::default);

    /// Start `connect` for the harness named `name` until the guard drops.
    pub(crate) fn install(name: &str, connect: Connector) -> Installed {
        FAKES.locked().insert(name.to_owned(), connect);
        Installed(name.to_owned())
    }

    pub(super) fn of(name: &str) -> Option<Connector> {
        FAKES.locked().get(name).cloned()
    }

    pub(crate) struct Installed(String);

    impl Drop for Installed {
        fn drop(&mut self) {
            FAKES.locked().remove(&self.0);
        }
    }
}
