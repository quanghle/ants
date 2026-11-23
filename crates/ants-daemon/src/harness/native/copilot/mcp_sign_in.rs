//! Signing a session serving a thread in to the MCP servers the definition lists (`mcp_sign_in`) that need it.
//!
//! The CLI starts a session's MCP servers once asked of them: a watcher asks with `session.mcp.list` as the session
//! opens, then follows each listed server's status in the session's events (`session.mcp_server_status_changed` and
//! kin, sent only live, never replayed on resume). A server settles once its status is not pending and nothing more
//! came of it for [`QUIET`]. One settled needing authentication is signed in with `session.mcp.oauth.login`: the CLI
//! signs in with the credentials it keeps, renewing them itself, else answers the address a browser opens, its
//! loopback listener waiting for the browser to come back. A sign-in its server was reconfigured during (removed, or
//! needing to reconnect) was cancelled, so runs again once the server settles, up to [`ATTEMPTS`] times.
//!
//! The session opens once each listed server settled and signed in, or after [`OPEN_WAIT`], telling of the servers
//! not configured in it, failing or not ready yet; the watcher goes on signing in to those later needing it for the
//! session's life. Each outcome goes to the thread's [`Report`].
//!
//! While a sign-in waits for a browser, no other session of a CLI keeping its credentials in the same place starts
//! another to the server: each answers the waiting one's address. Once the server connects after it, the other
//! sessions open with the server are told its credentials changed (`session.mcp.oauth.authenticationStateChanged`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use github_copilot_sdk as sdk;
use sdk::SessionEvent;
use serde_json::{Value, json};
use tokio::sync::{OwnedMutexGuard, mpsc, oneshot};
use tokio::time::Instant;

use super::Shared;
use super::turn::text;
use crate::harness::McpSignIn;
use crate::harness::mcp_sign_in::Report;
use crate::harness::native::Cli;
use crate::sync::MutexExt as _;

/// How long a server's status must stay put to count as settled: the CLI sends a server's changes in a burst, within
/// milliseconds of each other, so a second of nothing ends one.
const QUIET: Duration = Duration::from_secs(1);

/// The longest a session's opening waits for its listed servers to settle and sign in, well within the minute the
/// daemon gives a harness to start and open a session.
const OPEN_WAIT: Duration = Duration::from_secs(30);

/// The longest a sign-in may take to answer, and to wait for an earlier one to the same server.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(30);

/// The most sign-ins to a server in a row its reconfiguration cancels before telling of it.
const ATTEMPTS: u32 = 3;

/// How long a sign-in waiting for a browser holds off others to its server, about as long as the CLI waits for it.
const BROWSER_WAIT: Duration = Duration::from_mins(10);

/// The MCP servers signed in to, by where their credentials are kept.
static SERVERS: Mutex<BTreeMap<Key, Server>> = Mutex::new(BTreeMap::new());

/// Where a CLI keeps a server's credentials: by its executable and `COPILOT_HOME`, and the server's name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    program: PathBuf,
    home: Option<String>,
    server: String,
}

impl Key {
    fn new(cli: &Cli, server: &str) -> Self {
        Self { program: cli.program().to_owned(), home: cli.var("COPILOT_HOME"), server: server.to_owned() }
    }
}

/// Where `cli` keeps its credentials, as the daemon tells harnesses apart by ([`Report::send`]).
fn store(cli: &Cli) -> String {
    let program = cli.program().display();
    cli.var("COPILOT_HOME").map_or_else(|| program.to_string(), |home| format!("{program} COPILOT_HOME={home}"))
}

#[derive(Default)]
struct Server {
    /// Held by the sign-in running, so they run one at a time.
    turn: Arc<tokio::sync::Mutex<()>>,
    browser: Option<Browser>,
    /// The sessions open with the server listed.
    sessions: Vec<Weak<Opened>>,
}

/// A sign-in waiting for a browser to open `url`, on the listener of the CLI `cli` for its session `session`.
struct Browser {
    url: String,
    since: Instant,
    cli: Weak<Shared>,
    session: sdk::SessionId,
}

impl Browser {
    fn waits(&self) -> bool {
        self.since.elapsed() < BROWSER_WAIT && self.cli.upgrade().is_some_and(|cli| !*cli.closed.borrow())
    }

    fn of(&self, session: &Opened) -> bool {
        self.session == session.session && std::ptr::eq(self.cli.as_ptr(), Arc::as_ptr(&session.shared))
    }
}

/// A session of a CLI, to sign in and tell of credentials changing.
pub(super) struct Opened {
    pub(super) shared: Arc<Shared>,
    pub(super) session: sdk::SessionId,
}

impl Opened {
    async fn call(&self, method: &str, mut params: Value) -> Result<Value, String> {
        params["sessionId"] = json!(self.session);
        self.shared.client.call(method, Some(params)).await.map_err(|e| e.to_string())
    }
}

/// The address of the browser sign-in a `session.mcp.oauth.login` answer starts, if it needs one.
pub(super) fn browser_url(login: &Value) -> Option<String> {
    login["authorizationUrl"].as_str().filter(|url| !url.is_empty()).map(str::to_owned)
}

/// Note that a sign-in to `server` of session `of` waits for a browser to open `url`.
pub(super) fn waiting(of: &Opened, server: &str, url: &str) {
    let browser = Browser {
        url: url.to_owned(),
        since: Instant::now(),
        cli: Arc::downgrade(&of.shared),
        session: of.session.clone(),
    };
    SERVERS.locked().entry(Key::new(&of.shared.cli, server)).or_default().browser = Some(browser);
}

/// The address a sign-in to `key`'s server waits for a browser at, if one does.
fn waiting_at(key: &Key) -> Option<String> {
    let mut servers = SERVERS.locked();
    let browser = &mut servers.get_mut(key)?.browser;
    if browser.as_ref().is_some_and(|browser| !browser.waits()) {
        *browser = None;
    }
    browser.as_ref().map(|browser| browser.url.clone())
}

/// Note what event `event` of session `from` says of its MCP servers: one connecting ends a sign-in waiting for a
/// browser, telling the other sessions open with it that its credentials changed; one reconfigured ends the sign-in
/// its session started, the listener gone.
pub(super) fn observe(from: &Arc<Opened>, event: &SessionEvent) {
    for change in changes(event) {
        if change.status == Status::Connected {
            connected(from, &change.server);
        } else if change.reconfigured {
            forget(from, &change.server);
        }
    }
}

/// End the sign-in to `server` that session `of` started waiting for a browser, if one waits.
fn forget(of: &Opened, server: &str) {
    let mut servers = SERVERS.locked();
    if let Some(entry) = servers.get_mut(&Key::new(&of.shared.cli, server))
        && entry.browser.as_ref().is_some_and(|browser| browser.of(of))
    {
        entry.browser = None;
    }
}

fn connected(from: &Arc<Opened>, server: &str) {
    let others: Vec<_> = {
        let mut servers = SERVERS.locked();
        let Some(entry) = servers.get_mut(&Key::new(&from.shared.cli, server)) else { return };
        if entry.browser.take().is_none() {
            return;
        }
        entry.sessions.retain(|session| session.strong_count() > 0);
        entry.sessions.iter().filter_map(Weak::upgrade).filter(|other| !Arc::ptr_eq(other, from)).collect()
    };
    let harness = from.shared.cli.name();
    tracing::info!(harness, server, others = others.len(), "MCP server signed in after a browser sign-in");
    for other in others {
        let server = server.to_owned();
        tokio::spawn(async move {
            let told =
                other.call("session.mcp.oauth.authenticationStateChanged", json!({ "serverName": server })).await;
            if let Err(error) = told {
                let harness = other.shared.cli.name();
                tracing::warn!(harness, server, error, "telling a session of the MCP server's new credentials");
            }
        });
    }
}

/// An MCP server's status in a session.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Status {
    Pending,
    Connected,
    NeedsAuth,
    /// Failed to connect, as the error says.
    Failed(String),
    Disabled,
    /// Not configured in the session, or removed from it.
    Absent,
}

impl Status {
    fn new(data: &Value) -> Self {
        match data["status"].as_str().unwrap_or_default() {
            "connected" => Self::Connected,
            "needs-auth" => Self::NeedsAuth,
            "pending" => Self::Pending,
            "disabled" | "stopped" => Self::Disabled,
            "not_configured" => Self::Absent,
            word => {
                let error = data["error"].as_str().filter(|error| !error.is_empty());
                let mut error = error.map_or_else(|| format!("its status is {word}"), str::to_owned);
                if let Some(class) = data["errorClassification"].as_str().filter(|class| *class != "unclassified") {
                    error = format!("{error} ({class})");
                }
                Self::Failed(error)
            }
        }
    }

    /// Why a server of this status cannot sign in, if it cannot.
    fn error(&self) -> Option<String> {
        match self {
            Self::Pending | Self::Connected | Self::NeedsAuth => None,
            Self::Failed(error) => Some(error.clone()),
            Self::Disabled => Some("disabled in the session".to_owned()),
            Self::Absent => Some("not configured in the session".to_owned()),
        }
    }
}

/// What an event says of an MCP server: its status, and whether the session reconfigured it, which cancels a sign-in
/// to it.
struct Change {
    server: String,
    status: Status,
    reconfigured: bool,
}

fn changes(event: &SessionEvent) -> Vec<Change> {
    let data = &event.data;
    let change =
        |status: Status, reconfigured| Change { server: text(data, "serverName").to_owned(), status, reconfigured };
    match event.event_type.as_str() {
        "session.mcp_servers_loaded" => {
            let servers = data["servers"].as_array().into_iter().flatten();
            let change = |server: &Value| {
                let status = Status::new(server);
                let reconfigured = matches!(status, Status::Disabled | Status::Absent);
                Change { server: text(server, "name").to_owned(), status, reconfigured }
            };
            servers.map(change).collect()
        }
        "session.mcp_server_status_changed" => {
            let status = Status::new(data);
            let reconfigured = matches!(status, Status::Disabled | Status::Absent);
            vec![change(status, reconfigured)]
        }
        "session.mcp_server_removed" => vec![change(Status::Absent, true)],
        "session.mcp_server_needs_reconnect" => vec![change(Status::Pending, true)],
        _ => Vec::new(),
    }
}

/// How a sign-in came out.
enum Login {
    /// An earlier sign-in to the server waits for a browser to open this address.
    Earlier(String),
    /// This one waits for a browser to open this address, noted as waiting ([`waiting`]).
    Browser(String),
    SignedIn,
    Failed(String),
}

/// What happened to a server while a sign-in to it ran.
#[derive(Debug, Default, Clone, Copy)]
struct Running {
    /// The session reconfigured it, cancelling the sign-in.
    disturbed: bool,
    connected: bool,
}

type Logging = Pin<Box<dyn Future<Output = (String, Option<OwnedMutexGuard<()>>, Login)> + Send>>;

/// A listed server, as its session's events tell of it.
#[derive(Default)]
struct Watched {
    /// Its status, and when it came.
    status: Option<(Status, Instant)>,
    /// Whether its status was dealt with: signed in to, or told of.
    handled: bool,
    /// The sign-ins in a row its reconfiguration cancelled.
    cancelled: u32,
    /// While a sign-in runs, what happened to the server since it started.
    login: Option<Running>,
}

impl Watched {
    /// When its status settles, if it is to be dealt with.
    fn due(&self) -> Option<Instant> {
        match &self.status {
            Some((status, since)) if *status != Status::Pending && !self.handled && self.login.is_none() => {
                Some(*since + QUIET)
            }
            _ => None,
        }
    }
}

/// Sign session `opened` in to `servers` as each needs it, reporting to `report`: once each settled and signed in, or
/// after [`OPEN_WAIT`], `ready` is sent, and the session opens.
pub(super) async fn watch(
    opened: Arc<Opened>,
    servers: Vec<String>,
    report: Report,
    events: mpsc::UnboundedReceiver<SessionEvent>,
    ready: oneshot::Sender<()>,
) {
    {
        let mut registry = SERVERS.locked();
        for server in &servers {
            let entry = registry.entry(Key::new(&opened.shared.cli, server)).or_default();
            entry.sessions.retain(|session| session.strong_count() > 0);
            entry.sessions.push(Arc::downgrade(&opened));
        }
    }
    let store = store(&opened.shared.cli);
    let servers = servers.into_iter().map(|server| (server, Watched::default())).collect();
    let deadline = Instant::now() + OPEN_WAIT;
    Watch { opened, store, report, servers, opening: Some((deadline, ready)) }.run(events).await;
}

struct Watch {
    opened: Arc<Opened>,
    /// Where the CLI keeps its credentials.
    store: String,
    report: Report,
    servers: BTreeMap<String, Watched>,
    /// While the session opens, when it opens at the latest and what tells it to.
    opening: Option<(Instant, oneshot::Sender<()>)>,
}

impl Watch {
    async fn run(mut self, mut events: mpsc::UnboundedReceiver<SessionEvent>) {
        // Asking of the servers starts them; its answer stands for their status until an event tells it.
        let opened = self.opened.clone();
        let list = opened.call("session.mcp.list", json!({}));
        tokio::pin!(list);
        let mut listing = true;
        let mut logins = FuturesUnordered::<Logging>::new();
        loop {
            let wake = self.settle(&mut logins);
            tokio::select! {
                listed = &mut list, if listing => {
                    listing = false;
                    self.listed(listed);
                }
                Some(event) = events.recv() => self.changed(&event),
                Some((server, turn, login)) = logins.next() => {
                    self.logged_in(&server, login);
                    drop(turn);
                }
                () = tokio::time::sleep_until(wake.unwrap_or_else(Instant::now)), if wake.is_some() => {}
                else => break,
            }
        }
    }

    /// Seed the status of the servers no event told of yet from the session's list of them: one missing from it is
    /// not configured in the session.
    fn listed(&mut self, listed: Result<Value, String>) {
        let listed = match listed {
            Ok(listed) => listed,
            Err(error) => {
                let harness = self.opened.shared.cli.name();
                tracing::warn!(harness, error, "listing a session's MCP servers to sign in to");
                Value::Null
            }
        };
        let listed = listed["servers"].as_array().cloned().unwrap_or_default();
        for (server, watched) in &mut self.servers {
            if watched.status.is_none() {
                let status =
                    listed.iter().find(|listed| listed["name"] == **server).map_or(Status::Absent, Status::new);
                watched.status = Some((status, Instant::now()));
            }
        }
    }

    fn changed(&mut self, event: &SessionEvent) {
        for Change { server, status, reconfigured } in changes(event) {
            let Some(watched) = self.servers.get_mut(&server) else { continue };
            let before = watched.status.replace((status.clone(), Instant::now())).map(|(status, _)| status);
            match &mut watched.login {
                Some(running) => {
                    running.disturbed |= reconfigured;
                    running.connected |= status == Status::Connected;
                }
                None if before.as_ref() != Some(&status) => watched.handled = false,
                None => {}
            }
            if status == Status::Connected && before != Some(Status::Connected) {
                watched.cancelled = 0;
                self.report.send(&self.store, vec![McpSignIn::SignedIn { server }]);
            }
        }
    }

    /// Deal with the servers whose status settled: sign in to those needing it and, as the session opens, tell of
    /// those that cannot; open the session once each is dealt with or its time is up. When to look again.
    fn settle(&mut self, logins: &mut FuturesUnordered<Logging>) -> Option<Instant> {
        let now = Instant::now();
        let mut failed = Vec::new();
        for (server, watched) in &mut self.servers {
            if watched.due().is_none_or(|due| due > now) {
                continue;
            }
            let Some((status, _)) = &watched.status else { continue };
            match status.error() {
                None if *status == Status::NeedsAuth => {
                    watched.login = Some(Running::default());
                    logins.push(Box::pin(login(self.opened.clone(), server.clone())));
                }
                None => watched.handled = true,
                Some(error) => {
                    watched.handled = true;
                    if self.opening.is_some() {
                        failed.push(McpSignIn::Failed { server: server.clone(), error });
                    }
                }
            }
        }
        if let Some((deadline, _)) = &self.opening {
            let done = self.servers.values().all(|watched| watched.handled && watched.login.is_none());
            if done || *deadline <= now {
                for (server, watched) in &self.servers {
                    if !watched.handled && watched.login.is_none() {
                        let error = format!("not ready within {OPEN_WAIT:?}");
                        failed.push(McpSignIn::Failed { server: server.clone(), error });
                    }
                }
                if let Some((_, ready)) = self.opening.take() {
                    let _ = ready.send(());
                }
            }
        }
        self.tell(failed);
        let due = self.servers.values().filter_map(Watched::due);
        due.chain(self.opening.as_ref().map(|(deadline, _)| *deadline)).min()
    }

    /// Deal with how a sign-in to `server` came out: one the server connected during signed in, even if it answered a
    /// browser's address; one its reconfiguration cancelled runs again once it settles, its address forgotten.
    fn logged_in(&mut self, server: &str, login: Login) {
        let Some(watched) = self.servers.get_mut(server) else { return };
        let running = watched.login.take().unwrap_or_default();
        let harness = self.opened.shared.cli.name();
        if running.connected || matches!(watched.status, Some((Status::Connected, _))) {
            if matches!(login, Login::Browser(_)) {
                // The browser came back at once, as an identity provider's live session may have it: the sign-in
                // waits no more, and the other sessions are told, unless the server's event did both already.
                connected(&self.opened, server);
            }
            watched.handled = true;
            watched.cancelled = 0;
            self.tell(vec![McpSignIn::SignedIn { server: server.to_owned() }]);
            return;
        }
        let cancelled = match &login {
            Login::Earlier(_) | Login::SignedIn => false,
            Login::Browser(_) => running.disturbed,
            Login::Failed(error) => running.disturbed || error.to_lowercase().contains("cancel"),
        };
        if cancelled && matches!(login, Login::Browser(_)) {
            forget(&self.opened, server);
        }
        if cancelled {
            watched.cancelled += 1;
            if watched.cancelled < ATTEMPTS {
                tracing::debug!(harness, server, "the MCP server was reconfigured during a sign-in to it");
                if let Some((_, since)) = &mut watched.status {
                    *since = Instant::now();
                }
                watched.handled = false;
                return;
            }
        }
        watched.handled = true;
        let server = server.to_owned();
        let sign_in = match login {
            _ if cancelled => {
                let error = format!("the server was reconfigured during each of {ATTEMPTS} sign-ins");
                McpSignIn::Failed { server, error }
            }
            Login::Earlier(url) | Login::Browser(url) => McpSignIn::Browser { server, url },
            Login::SignedIn => McpSignIn::SignedIn { server },
            Login::Failed(error) => McpSignIn::Failed { server, error },
        };
        self.tell(vec![sign_in]);
    }

    /// Log and report `sign_ins`.
    fn tell(&self, sign_ins: Vec<McpSignIn>) {
        let harness = self.opened.shared.cli.name();
        for sign_in in &sign_ins {
            match sign_in {
                McpSignIn::SignedIn { server } => tracing::debug!(harness, server, "signed in to the MCP server"),
                McpSignIn::Browser { server, url } => {
                    tracing::warn!(harness, server, url, "the MCP server needs a browser sign-in on this machine");
                }
                McpSignIn::Failed { server, error } => {
                    tracing::warn!(harness, server, error, "signing in to an MCP server");
                }
            }
        }
        self.report.send(&self.store, sign_ins);
    }
}

/// Sign session `opened` in to `server`, once no other sign-in to it runs, unless one waits for a browser already.
/// The turn held, for the outcome to be noted before the next runs.
async fn login(opened: Arc<Opened>, server: String) -> (String, Option<OwnedMutexGuard<()>>, Login) {
    let key = Key::new(&opened.shared.cli, &server);
    let turn = SERVERS.locked().entry(key.clone()).or_default().turn.clone();
    let Ok(turn) = tokio::time::timeout(LOGIN_TIMEOUT, turn.lock_owned()).await else {
        let error = format!("an earlier sign-in to it did not end within {LOGIN_TIMEOUT:?}");
        return (server, None, Login::Failed(error));
    };
    if let Some(url) = waiting_at(&key) {
        return (server, Some(turn), Login::Earlier(url));
    }
    let login = opened.call("session.mcp.oauth.login", json!({ "serverName": server }));
    let login = match tokio::time::timeout(LOGIN_TIMEOUT, login).await {
        // Noted at once, for the server connecting meanwhile to end it and tell the other sessions.
        Ok(Ok(answer)) => browser_url(&answer).map_or(Login::SignedIn, |url| {
            waiting(&opened, &server, &url);
            Login::Browser(url)
        }),
        Ok(Err(error)) => Login::Failed(error),
        Err(_) => Login::Failed(format!("no answer within {LOGIN_TIMEOUT:?}")),
    };
    // Events of the server reconfigured during the sign-in come within this, so it counts as cancelled.
    tokio::time::sleep(QUIET).await;
    (server, Some(turn), login)
}
