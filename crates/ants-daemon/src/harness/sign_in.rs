//! Whether a harness is installed and signed in, and what its errors mean: signed out, a provider limit or another
//! failure.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{AuthMethod, InitializeRequest, NewSessionRequest};
use agent_client_protocol::{Agent, Client};
use anyhow::{Result, anyhow, bail};

use super::{Harness, Launch, ModelSettings};
use crate::permission::PermissionPolicy;
use crate::sync::MutexExt as _;

/// How long a [`handshake`] may take.
pub(super) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(60);

/// Phrases of a harness's error or stderr that mean it needs signing in, lowercase. Kept to those that cannot mean
/// anything else, such as an MCP server's or a proxy's authentication.
const SIGNED_OUT_TEXTS: &[&str] = &[
    "no authentication information found",
    "not logged in",
    "not signed in",
    "please log in",
    "please login",
    "please sign in",
    "please run /login",
    "login required",
];

/// A harness that cannot work until a human signs it in, as its ACP handshake or a prompt said: ACP's
/// `auth_required` error, or an error or stderr that says so (`SIGNED_OUT_TEXTS`). Nothing ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedOut {
    pub harness: String,
    /// The command that signs it in, if known ([`Harness::sign_in`], else a terminal auth method it offers).
    pub sign_in: Option<String>,
    /// What the harness said.
    pub detail: String,
}

impl fmt::Display for SignedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} is not signed in: {}", self.harness, self.detail)?;
        match &self.sign_in {
            Some(command) => write!(f, "; sign in with `{command}`"),
            None => Ok(()),
        }
    }
}

impl std::error::Error for SignedOut {}

/// Whether an ACP error from a harness means it needs signing in.
fn needs_sign_in(e: &agent_client_protocol::Error) -> bool {
    if e.code == agent_client_protocol::ErrorCode::AUTH_REQUIRED {
        return true;
    }
    let text = e.to_string().to_lowercase();
    SIGNED_OUT_TEXTS.iter().any(|phrase| text.contains(phrase))
}

/// `e`, from the harness of `launch` while doing `what`, as an error: a [`SignedOut`] one when it says the harness
/// needs signing in, given the auth methods the harness offered, else a [`crate::limits::ratelimit::Limited`] one when it
/// reports a provider limit.
pub(super) fn harness_error(
    launch: &Launch,
    what: &str,
    e: &agent_client_protocol::Error,
    methods: &[AuthMethod],
) -> anyhow::Error {
    if needs_sign_in(e) {
        let harness = &launch.harness;
        let signed_out = SignedOut {
            harness: harness.name.clone(),
            sign_in: harness.sign_in_command(methods),
            detail: e.to_string(),
        };
        return signed_out.into();
    }
    if let Some(limited) = crate::limits::ratelimit::classify(&launch.harness, &e.to_string()) {
        return limited.into();
    }
    anyhow!("{} {what}: {e}", launch.harness)
}

impl Harness {
    /// The command that signs the harness in: the definition's, else the one of a terminal auth method it offers, run
    /// as the harness with that method's arguments.
    fn sign_in_command(&self, methods: &[AuthMethod]) -> Option<String> {
        if let Some(command) = &self.sign_in {
            return Some(command.clone());
        }
        methods.iter().find_map(|method| match method {
            AuthMethod::Terminal(terminal) => {
                let args = self.args.iter().chain(&terminal.args);
                Some(
                    std::iter::once(self.command.display().to_string())
                        .chain(args.cloned())
                        .collect::<Vec<_>>()
                        .join(" "),
                )
            }
            _ => None,
        })
    }
}

/// What an ACP handshake with a harness found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    /// It answered `initialize`, describing itself, and opened a session: installed and signed in.
    Ready { agent: String },
    /// Its executable is neither at the path its command names nor on `PATH`.
    Missing,
    /// It needs a human to sign it in ([`SignedOut`]).
    SignedOut { detail: String },
    /// It failed otherwise, or took longer than a minute.
    Failed { detail: String },
}

/// The outcome of a [`handshake`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub harness: String,
    /// The command that signs it in, if known: the definition's, else a terminal auth method it offered.
    pub sign_in: Option<String>,
    pub readiness: Readiness,
}

/// Check that the harness of `launch` is installed and signed in: start it, complete the ACP handshake and open a
/// session in the launch's directory, with no MCP servers or settings, then stop it. Prompts nothing.
pub async fn handshake(launch: &Launch) -> Handshake {
    let harness = &launch.harness;
    let mut checked =
        Handshake { harness: harness.name.clone(), sign_in: harness.sign_in.clone(), readiness: Readiness::Missing };
    if launch.process_env.as_ref().is_some_and(|env| harness.locate(env).is_none()) {
        return checked;
    }
    let methods = Mutex::new(Vec::new());
    // The agent, once it opened the session. Stopping it afterwards closes its stdout: an adapter still writing then,
    // as claude-agent-acp writes its commands right after answering session/new, dies of the broken pipe, which fails
    // the connection although the harness passed.
    let opened = Mutex::new(None);
    let transport = launch.transport().await;
    let connect = Client.v1().name("ants").connect_with(transport, async |cx: ConnectionTo<Agent>| {
        let init = cx.send_request(InitializeRequest::new(ProtocolVersion::V1)).block_task().await?;
        methods.locked().clone_from(&init.auth_methods);
        cx.send_request(NewSessionRequest::new(launch.cwd.clone())).block_task().await?;
        let agent = match &init.agent_info {
            Some(info) => format!("{} {}", info.name, info.version),
            None => "an agent that does not name itself".to_owned(),
        };
        *opened.locked() = Some(agent.clone());
        Ok(agent)
    });
    let result = tokio::time::timeout(HANDSHAKE_TIMEOUT, connect).await;
    let methods = methods.into_inner().unwrap_or_else(PoisonError::into_inner);
    checked.sign_in = harness.sign_in_command(&methods);
    let opened = opened.into_inner().unwrap_or_else(PoisonError::into_inner);
    checked.readiness = match (opened, result) {
        (Some(agent), Ok(Err(e))) => {
            tracing::debug!(harness = %harness.name, error = %e, "the harness failed as it was stopped after its handshake");
            Readiness::Ready { agent }
        }
        (Some(agent), _) | (None, Ok(Ok(agent))) => Readiness::Ready { agent },
        (None, Ok(Err(e))) if needs_sign_in(&e) => Readiness::SignedOut { detail: e.to_string() },
        (None, Ok(Err(e))) => Readiness::Failed { detail: e.to_string() },
        (None, Err(_)) => Readiness::Failed { detail: format!("no handshake within {HANDSHAKE_TIMEOUT:?}") },
    };
    checked
}

/// Check `harness` as [`handshake`] does, in the current directory, and describe the agent.
pub async fn check(harness: &Arc<Harness>) -> Result<String> {
    let launch = Launch::new(
        harness.clone(),
        std::env::current_dir()?,
        Vec::new(),
        PermissionPolicy::reject_all(),
        ModelSettings::default(),
    );
    let checked = handshake(&launch).await;
    let harness = &launch.harness;
    match checked.readiness {
        Readiness::Ready { agent } => Ok(agent),
        Readiness::Missing => bail!("{harness}: {} is not installed (not found on PATH)", harness.command.display()),
        Readiness::SignedOut { detail } => {
            Err(SignedOut { harness: checked.harness, sign_in: checked.sign_in, detail }.into())
        }
        Readiness::Failed { detail } => bail!("{harness} ACP handshake: {detail}"),
    }
}
