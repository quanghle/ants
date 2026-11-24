//! A native CLI as an ant runs it: its definition with the ant's environment and model settings, and the command
//! that starts it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use agent_client_protocol::Error;
use tokio::process::{ChildStdin, ChildStdout, Command};

use super::Process;
use crate::harness::mcp_sign_in::Report;
use crate::harness::{Harness, ModelSettings, Tools};

/// A native CLI as an ant runs it: the definition with the ant's environment and model settings.
#[derive(Debug, Clone)]
pub(crate) struct Cli {
    harness: Arc<Harness>,
    /// The executable: the command, else on Windows the file found on `PATH`.
    program: PathBuf,
    /// The definition's arguments and those of the ant's `context`.
    pub(super) args: Vec<String>,
    /// The definition's environment with the ant's over it, added to the daemon's.
    env: BTreeMap<String, String>,
    /// The ant's model, with the suffix its `context` selects.
    model: Option<String>,
    effort: Option<String>,
    /// Where its sessions serving a thread report their sign-ins to MCP servers.
    report: Option<Report>,
}

impl Cli {
    pub(super) fn new(harness: &Harness, env: &[(String, String)], settings: &ModelSettings) -> Self {
        Self {
            harness: Arc::new(harness.clone()),
            program: harness.executable(env),
            args: harness.launch_args(settings),
            env: harness.launch_env(env),
            model: settings.model_id(harness),
            effort: settings.reasoning_effort.clone(),
            report: None,
        }
    }

    /// The CLI with its sessions serving a thread reporting their sign-ins to MCP servers to `report`, if any.
    pub(super) fn reporting(self, report: Option<Report>) -> Self {
        Self { report, ..self }
    }

    /// The CLI with `value` for environment variable `name`, unless the definition or the ant set it.
    pub(crate) fn with_env_default(mut self, name: &str, value: &str) -> Self {
        self.env.entry(name.to_owned()).or_insert_with(|| value.to_owned());
        self
    }

    /// The CLI with `args` for the definition's arguments and those of the ant's `context`.
    pub(crate) fn with_args(&self, args: Vec<String>) -> Self {
        Self { args, ..self.clone() }
    }

    /// Where its sessions serving a thread report their sign-ins to MCP servers, if anywhere.
    pub(super) fn report(&self) -> Option<Report> {
        self.report.clone()
    }

    /// The definition's `sign_in`: the command signing the CLI in, if it names one.
    pub(super) fn sign_in(&self) -> Option<&str> {
        self.harness.sign_in()
    }

    /// The harness's name.
    pub(crate) fn name(&self) -> &str {
        self.harness.name()
    }

    /// The executable started.
    pub(crate) fn program(&self) -> &std::path::Path {
        &self.program
    }

    /// The definition's arguments and those of the ant's `context`, which [`Self::command`] passes after the
    /// protocol's own.
    pub(crate) fn args(&self) -> &[String] {
        &self.args
    }

    /// The value environment variable `name` has for the CLI, unless unset or empty.
    pub(crate) fn var(&self, name: &str) -> Option<String> {
        let value = self.env.get(name).cloned().or_else(|| std::env::var(name).ok());
        value.filter(|value| !value.is_empty())
    }

    /// The definition's `mode`: for a native protocol, the CLI's own permission mode.
    pub(crate) fn mode(&self) -> Option<&str> {
        self.harness.mode.as_deref()
    }

    /// Whether the ant asks ants about its tools (`tools = "ask"`): what the CLI would ask a human about goes to the
    /// client ([`super::approve`]) rather than being denied.
    pub(crate) fn asks(&self) -> bool {
        self.harness.tools == Tools::Ask
    }

    /// The definition's `mcp_sign_in`: the MCP servers a session serving a thread signs in to as they need it.
    pub(crate) fn mcp_sign_in(&self) -> &[String] {
        &self.harness.mcp_sign_in
    }

    /// The definition's `entra_client_id`: the Microsoft Entra app registration for MCP servers signing in through Entra.
    pub(crate) fn entra_client_id(&self) -> Option<&str> {
        self.harness.entra_client_id.as_deref()
    }

    /// The model the ant runs, if it sets one, its `context`'s suffix included.
    pub(crate) fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// The reasoning effort the ant runs at, if it sets one.
    pub(crate) fn effort(&self) -> Option<&str> {
        self.effort.as_deref()
    }

    /// The CLI's command: its executable, with `protocol_args` before the definition's arguments and those of the ant's
    /// `context`, the definition's environment with the ant's over it, added to the daemon's. Its stdio is piped, it
    /// runs without a shell, in a process group of its own on Unix (so a stop reaches what it starts) and without a
    /// console window on Windows, and it is killed when dropped.
    pub(crate) fn command<I, S>(&self, protocol_args: I) -> Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut command = Command::new(&self.program);
        command
            .args(protocol_args)
            .args(&self.args)
            .envs(&self.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(windows)]
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
        command
    }

    /// Start the CLI ([`Self::command`]) with `protocol_args`, working in `cwd`, else in the daemon's directory: the
    /// process, and its stdin and stdout to speak to it through.
    pub(crate) fn spawn<I, S>(
        &self,
        protocol_args: I,
        cwd: Option<&Path>,
    ) -> Result<(Process, ChildStdin, ChildStdout), Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<std::ffi::OsStr>,
    {
        let mut command = self.command(protocol_args);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        Process::start(self.name(), &self.program, command)
    }
}

/// Whether argument `arg` of a definition gives `flag`, alone or with its `=value`.
pub(crate) fn is_flag(arg: &str, flag: &str) -> bool {
    arg.strip_prefix(flag).is_some_and(|rest| rest.is_empty() || rest.starts_with('='))
}
