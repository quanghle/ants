//! An external provider kept running ([`Supervised`]): started once for all the ants it serves, initialized, and
//! restarted with a backoff each time it ends. Its tools are offered only while it is up; a call meanwhile fails
//! saying why.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use ants_proto::provider::{
    Ack, AntRef, BoxFuture, Brief, BriefRequest, Capabilities, Capability, Connected, Event, Export, Host,
    ImportRequest, Info, InitializeParams, InitializeResult, MemoriesView, PROTOCOL_VERSION, Provider, RpcError,
    ToolCall, ToolList, ToolResult, WakeRequest, Woken,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::watch;
use tokio::time::Instant;

use super::definition::{Definition, Protocol};
use super::link::Link;
use super::process::{self, Process};
use crate::sync::RwLockExt as _;

/// The first wait before a restart, doubled after each restart that did not last [`HEALTHY`].
const FIRST_BACKOFF: Duration = Duration::from_secs(1);

/// The longest wait before a restart.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// How long a provider stays up for its next restart to wait [`FIRST_BACKOFF`] again.
const HEALTHY: Duration = Duration::from_secs(60);

/// How long connecting waits for a provider starting to be up, before going on with what it may declare.
const FIRST_START: Duration = Duration::from_secs(5);

/// A provider's stdio, and its process unless it runs in this one, as tests' do.
pub(super) struct Spawned {
    pub(super) reader: Box<dyn AsyncRead + Send + Unpin>,
    pub(super) writer: Box<dyn AsyncWrite + Send + Unpin>,
    pub(super) process: Option<Process>,
}

/// What starts a provider, once per run.
pub(super) type Spawn = Arc<dyn Fn() -> std::io::Result<Spawned> + Send + Sync>;

/// Where a provider stands.
#[derive(Clone)]
enum State {
    Starting,
    Up(Connected),
    /// Ended `why`, to start again at `until`.
    Down {
        why: String,
        until: Instant,
    },
    Stopped,
}

/// What the provider's runs share with its handle.
struct Shared {
    state: watch::Sender<State>,
    /// The host of the last connection to the provider, which its wake requests go to.
    host: RwLock<Option<Arc<dyn Host>>>,
}

impl Host for Shared {
    fn wake(&self, request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
        let host = self.host.read_locked().clone();
        match host {
            Some(host) => Box::pin(async move { host.wake(request).await }),
            None => Box::pin(std::future::ready(Err(RpcError::unavailable("ants is not connected to the provider")))),
        }
    }
}

/// An external provider, kept running until stopped or dropped; a [`Provider`] answering for whichever run is up.
pub struct Supervised {
    name: String,
    protocol: Protocol,
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
}

impl Supervised {
    /// Start the provider `definition` defines, and keep it running.
    pub fn start(definition: &Definition) -> Self {
        let spawn_definition = definition.clone();
        let spawn: Spawn = Arc::new(move || {
            let (process, stdout, stdin) = process::spawn(&spawn_definition)?;
            Ok(Spawned { reader: Box::new(stdout), writer: Box::new(stdin), process: Some(process) })
        });
        Self::with_spawn(&definition.name, definition.protocol, spawn)
    }

    /// Start the provider `name`, speaking `protocol`, with `spawn`, and keep it running.
    pub(super) fn with_spawn(name: &str, protocol: Protocol, spawn: Spawn) -> Self {
        let shared = Arc::new(Shared { state: watch::Sender::new(State::Starting), host: RwLock::new(None) });
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(supervise(name.to_owned(), protocol, spawn, shared.clone(), stopped));
        Self { name: name.to_owned(), protocol, shared, stop }
    }

    /// Stop it for good, killing its process.
    pub fn stop(&self) {
        self.stop.send_replace(true);
    }

    /// What it may declare while not up: all a provider may, or tools alone of an MCP server.
    fn declared(&self) -> Capabilities {
        match self.protocol {
            Protocol::Provider => Capabilities::ALL,
            Protocol::Mcp => Capabilities { tools: true, ..Capabilities::NONE },
        }
    }

    /// The run up now, else the error a call fails with.
    fn up(&self) -> Result<Connected, RpcError> {
        match &*self.shared.state.borrow() {
            State::Up(connected) => Ok(connected.clone()),
            State::Starting => Err(RpcError::unavailable(&format!("{} is starting", self.name))),
            State::Down { why, until } => {
                let left = until.saturating_duration_since(Instant::now());
                let left = left.as_secs() + u64::from(left.subsec_nanos() > 0);
                Err(RpcError::unavailable(&format!("{} is down: {why}; restarting in {left} s", self.name)))
            }
            State::Stopped => Err(RpcError::unavailable(&format!("{} is stopped", self.name))),
        }
    }
}

impl Drop for Supervised {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Provider for Supervised {
    /// Wait up to `FIRST_START` for a provider starting, then answer with what its run up agreed, else with what
    /// it may declare (`Supervised::declared`), calls then failing until it is up.
    fn initialize(
        &self,
        params: InitializeParams,
        host: Arc<dyn Host>,
    ) -> BoxFuture<'_, Result<InitializeResult, RpcError>> {
        *self.shared.host.write_locked() = Some(host);
        Box::pin(async move {
            let mut state = self.shared.state.subscribe();
            let _ = tokio::time::timeout(FIRST_START, state.wait_for(|s| !matches!(s, State::Starting))).await;
            let (version, capabilities) = match self.up() {
                Ok(up) => (up.agreed().provider.version.clone(), up.capabilities()),
                Err(_) => (String::new(), self.declared()),
            };
            params.accept(&[PROTOCOL_VERSION], Info { name: self.name.clone(), version }, capabilities)
        })
    }

    fn list_tools(&self) -> BoxFuture<'_, Result<ToolList, RpcError>> {
        Box::pin(async move {
            match self.up() {
                Ok(up) if up.has(Capability::Tools) => up.list_tools().await,
                _ => Ok(ToolList::default()),
            }
        })
    }

    fn call_tool(&self, call: ToolCall) -> BoxFuture<'_, Result<ToolResult, RpcError>> {
        Box::pin(async move { self.up()?.call_tool(call).await })
    }

    fn brief(&self, request: BriefRequest) -> BoxFuture<'_, Result<Brief, RpcError>> {
        Box::pin(async move { self.up()?.brief(request).await })
    }

    fn event(&self, event: Event) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(async move { self.up()?.event(event).await })
    }

    fn memories(&self, ant: AntRef) -> BoxFuture<'_, Result<MemoriesView, RpcError>> {
        Box::pin(async move { self.up()?.memories(ant).await })
    }

    fn export(&self, ant: AntRef) -> BoxFuture<'_, Result<Export, RpcError>> {
        Box::pin(async move { self.up()?.export(ant).await })
    }

    fn import(&self, request: ImportRequest) -> BoxFuture<'_, Result<Ack, RpcError>> {
        Box::pin(async move { self.up()?.import(request).await })
    }
}

/// The wait before each restart: from [`FIRST_BACKOFF`], doubling up to [`MAX_BACKOFF`], from the first again after a
/// run that lasted [`HEALTHY`].
struct Backoff(Duration);

impl Backoff {
    fn after(&mut self, up_for: Duration) -> Duration {
        if up_for >= HEALTHY {
            self.0 = FIRST_BACKOFF;
        }
        let wait = self.0;
        self.0 = (wait * 2).min(MAX_BACKOFF);
        wait
    }
}

/// Run the provider `name` with `spawn` and restart it each time it ends, until `stopped`.
async fn supervise(
    name: String,
    protocol: Protocol,
    spawn: Spawn,
    shared: Arc<Shared>,
    mut stopped: watch::Receiver<bool>,
) {
    let mut backoff = Backoff(FIRST_BACKOFF);
    loop {
        shared.state.send_replace(State::Starting);
        let (why, up_for) = tokio::select! {
            ended = run(&name, protocol, &spawn, &shared) => ended,
            _ = stopped.wait_for(|stop| *stop) => break,
        };
        let wait = backoff.after(up_for);
        tracing::warn!(provider = name, why, "provider down; restarting in {} s", wait.as_secs());
        let until = Instant::now() + wait;
        shared.state.send_replace(State::Down { why, until });
        tokio::select! {
            () = tokio::time::sleep_until(until) => {}
            _ = stopped.wait_for(|stop| *stop) => break,
        }
    }
    shared.state.send_replace(State::Stopped);
    tracing::info!(provider = name, "provider stopped");
}

/// Start the provider, initialize it and serve it while it is up; how it ended and for how long it was up.
async fn run(name: &str, protocol: Protocol, spawn: &Spawn, shared: &Arc<Shared>) -> (String, Duration) {
    let Spawned { reader, writer, mut process } = match spawn() {
        Ok(spawned) => spawned,
        Err(e) => return (format!("could not start: {e}"), Duration::ZERO),
    };
    let (client, link) = match protocol {
        Protocol::Provider => super::external::client(name, reader, writer),
        Protocol::Mcp => super::mcp::client(name, reader, writer),
    };
    let params = InitializeParams::new(super::client(), super::USES);
    let host: Arc<dyn Host> = shared.clone();
    let connected = tokio::select! {
        connected = Connected::connect(client, params, host) => connected,
        why = ended(&link, &mut process) => return (why, Duration::ZERO),
    };
    let connected = match connected {
        Ok(connected) => connected,
        Err(e) => return (format!("failed to initialize: {e}"), Duration::ZERO),
    };
    tracing::info!(provider = name, capabilities = ?connected.capabilities(), "provider up");
    let up = Instant::now();
    shared.state.send_replace(State::Up(connected));
    (ended(&link, &mut process).await, up.elapsed())
}

/// Wait for the provider to end, its process exiting or its output closing; how it did.
async fn ended(link: &Link, process: &mut Option<Process>) -> String {
    let Some(process) = process else { return link.closed().await };
    let closed = tokio::select! {
        why = process.exited() => return why,
        why = link.closed() => why,
    };
    tokio::time::timeout(process::EXIT_GRACE, process.exited()).await.unwrap_or(closed)
}

#[cfg(test)]
mod tests;
