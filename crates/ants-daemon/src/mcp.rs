//! MCP tools for the ant's harness sessions: `send_message`, `ask_ant`, `check_inbox`, `list_ants` and
//! `route` to find ants by their colony capabilities, `post_notes` and `read_notes` for the thread's notes
//! (see [`crate::stream::notes`]), and `escalate` to send the thread up the ant's escalation chain (see
//! [`crate::escalation`]). The tools of the ant's provider, what holds its identity, memory and work, come beside them,
//! named with the provider's name as prefix (`sense_recall`), each call carrying the ant, its identity, the thread and
//! where the thread came from (see [`provider`]).
//!
//! `decide_approval` answers another ant's tool request that came up its chain (see [`crate::approvals`]).
//!
//! `post_channel` starts a channel thread the ant creates: its escalations go up the ant's chain, whichever ant
//! gets stuck on it (see [`ants_proto::Escalation::new_from`]).
//! `manage_channel` asks the channel manager to change a channel the ant owns (see [`crate::chanadm`]).
//! `react` adds or removes the ant's reaction to a message, by default the one being handled, and `list_reactions`
//! lists the reactions to a message the ant reads (see [`crate::records::reactions`]).
//! `suggest_thread_title` has the turn's reply on the thread suggest a title for it ([`Envelope::title`],
//! [`ants_proto::ChannelPost::title`], [`ants_proto::NotesPost::title`]).
//! `complete` ends the turn with no reply to the message being handled, which gets a `check` reaction
//! ([`Mcp::take_completed`]); a human's, a webhook's, an escalation's or an approval request's message is always
//! answered.
//! `ask_human` asks the thread's owner, else the default human, a question with options ([`Envelope::question`]) and
//! returns at once; the answer arrives as a later message ([`Envelope::answer`]).
//! `send_message`, `ask_ant`, `route`, `post_channel` and `post_notes` attach files from the directory the thread's
//! session works in, `attach_to_reply` attaches them to the turn's final answer ([`Mcp::reply_files`]), and
//! `read_attachment` reads an attachment of a message the ant reads (see [`attach`]).
//!
//! An assistant also gets the `*_for_human` tools, which act for its human while the human's grant names it: read the
//! human's home queue, answer questions, send, post in channels and thread notes, react, title, mark done and decide
//! tool requests (see [`assist`]).
//!
//! One server per ant, with two ways in: streamable HTTP on a loopback port, for harnesses that take HTTP MCP
//! servers, and a local socket that `ants mcp` bridges stdio to, for the others (see [`crate::ipc`]). The sessions of each thread get their
//! own bearer token, which tells the tools which thread calls them.
//!
//! A reply to `ask_ant` lands in the ant's inbox like any message. It reaches the waiting call through a core NATS
//! subscription to the inbox ([`Mcp::listen`]), so it never waits behind the prompt that asked, and [`Mcp::claim`] keeps
//! the inbox consumer from prompting with it. A reply that comes after the call gave up is kept for `check_inbox`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use agent_client_protocol::v1::schema::{EnvVariable, HttpHeader, McpServer, McpServerHttp, McpServerStdio};
use ants_proto::provider::{Connected, Origin};
use ants_proto::{AttachmentHandle, Envelope, ThreadId, UserId, Uuid};
use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use hyper::http::header;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::chanadm::Admin;
use crate::escalation::Escalator;
use crate::harness::{McpServers, McpSession};
use crate::people::humans::Registry;
use crate::provider::context::repository;
use crate::records::links;
use crate::secrets::redact::Redactor;
use crate::stream::notes::NotesStore;
use crate::sync::MutexExt as _;
use crate::{bus, ipc};

mod asks;
mod assist;
mod attach;
mod channels;
mod escalation;
mod managers;
mod messages;
mod notes;
mod provider;
mod ratings;
mod reactions;
mod routing;
mod saving;
mod serve;
mod tips;
mod tools;
mod trails;
mod turn;

pub use assist::{HumanMessage, HumanSealing, HumanStore, jetstream_store};
pub use attach::{FileAccess, jetstream_files};
pub use channels::{ChannelAccess, jetstream_channels};
pub use managers::{AntAsk, AntRequester, ManagerAsk, Requester, RetireAsk};
pub(crate) use messages::asked_questions;
pub(crate) use provider::Directories;
pub use reactions::{ReactionAsker, local_reaction_asker, nats_reaction_asker};
pub(crate) use routing::why_unserved;
pub use routing::{Colony, ColonyLookup, jetstream_colony, jetstream_lookup};
pub use serve::bridge_stdio;
pub use tools::own_names;
pub use turn::{Completed, Turn};

use asks::{Asks, Stored};
use serve::{serve_http, serve_socket};

/// Environment variable carrying the session's token to `ants mcp`.
pub const TOKEN_ENV: &str = "ANTS_MCP_TOKEN";
/// Name the harness shows the tools under.
const SERVER_NAME: &str = "ants";

/// Publishes an envelope and resolves once JetStream has stored it.
pub type Outbox = Arc<dyn Fn(Envelope) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Publishes through JetStream, first recording the link to a channel thread the envelope passes on, if any
/// ([`links::pass_on`]).
pub fn jetstream_outbox(js: async_nats::jetstream::Context) -> Outbox {
    Arc::new(move |envelope| {
        let js = js.clone();
        Box::pin(async move {
            links::passing_on(&js, &envelope).await;
            bus::publish(&js, &envelope).await.map(drop)
        })
    })
}

/// The MCP server of one ant. Dropping it stops the listeners.
pub struct Mcp {
    hub: Arc<Hub>,
    url: String,
    socket: Option<PathBuf>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Mcp {
    fn drop(&mut self) {
        self.tasks.iter().for_each(JoinHandle::abort);
        if let Some(socket) = &self.socket {
            ipc::remove(socket);
        }
    }
}

impl Mcp {
    /// Listen on a loopback port and on a local socket ([`ipc::Listener`]). What must survive a restart (asks waiting for a reply, late
    /// replies) lives in `<home>/mcp.json`. `humans` tells the humans the tools reach. Thread notes out of turns are
    /// escalated through `escalator`, without one straight to the thread's owner.
    pub async fn start(
        ant: UserId,
        home: &Path,
        outbox: Outbox,
        colony: Colony,
        notes: Arc<dyn NotesStore>,
        humans: Registry,
        escalator: Option<Arc<Escalator>>,
    ) -> Result<Self> {
        let state_path = home.join("mcp.json");
        let stored = match tokio::fs::read(&state_path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("reading {}", state_path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Stored::default(),
            Err(e) => return Err(e).with_context(|| format!("reading {}", state_path.display())),
        };
        let hub = Arc::new(Hub {
            ant,
            outbox,
            colony,
            notes,
            humans,
            escalator,
            channels: std::sync::OnceLock::new(),
            lookup: OnceLock::new(),
            admin: OnceLock::new(),
            requester: OnceLock::new(),
            reaction_asker: OnceLock::new(),
            assisting: OnceLock::new(),
            human_store: OnceLock::new(),
            sealing: OnceLock::new(),
            files: OnceLock::new(),
            redactor: OnceLock::new(),
            provider: OnceLock::new(),
            trails: OnceLock::new(),
            dreams: OnceLock::new(),
            served: Mutex::default(),
            callers: OnceLock::new(),
            origin_store: OnceLock::new(),
            settling: tokio::sync::Mutex::default(),
            state_path,
            sessions: Mutex::default(),
            asks: tokio::sync::Mutex::new(Asks { stored, waiting: HashMap::new() }),
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.context("binding the MCP port")?;
        let url = format!("http://{}/mcp", listener.local_addr()?);
        let mut tasks = vec![tokio::spawn(serve_http(listener, hub.clone()))];

        // The stdio fallback is optional: without the socket, harnesses lacking HTTP MCP just get no tools.
        let socket = match ipc::Listener::bind(home) {
            Ok((listener, address)) => {
                tasks.push(tokio::spawn(serve_socket(listener, hub.clone())));
                Some(address)
            }
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "no MCP socket for stdio harnesses");
                None
            }
        };
        tracing::info!(%url, "serving MCP tools");
        Ok(Self { hub, url, socket, tasks })
    }

    /// Serve `post_channel` through `channels`; without them it is refused.
    pub fn with_channels(&self, channels: ChannelAccess) {
        let _ = self.hub.channels.set(channels);
    }

    /// Look recipients up through `lookup` before `send_message` and `ask_ant` reach them; without it no
    /// recipient is looked up.
    pub fn with_lookup(&self, lookup: ColonyLookup) {
        let _ = self.hub.lookup.set(lookup);
    }

    /// Serve `request_ant` and `retire_ant` through `requester`, for a manager ant; without one neither tool is
    /// offered.
    pub fn with_requester(&self, requester: AntRequester) {
        let _ = self.hub.requester.set(requester);
    }

    /// Serve `react` and `list_reactions` through `asker`; without it both are refused.
    pub fn with_reaction_asker(&self, asker: ReactionAsker) {
        let _ = self.hub.reaction_asker.set(asker);
    }

    /// What asks for reactions and changes them for the ant, once set.
    pub fn reaction_asker(&self) -> Option<ReactionAsker> {
        self.hub.reaction_asker.get().cloned()
    }

    /// Take the ant for the assistant of `human`, as its credentials' assistant_of tag says.
    pub fn with_assisting(&self, human: UserId) {
        let _ = self.hub.assisting.set(human);
    }

    /// The human the tools may act for, setting `on_behalf_of` on what they send ([`Envelope::on_behalf`]): the one
    /// the ant's credentials make it the assistant of, while that human's grant names the ant. None otherwise, so
    /// whatever the tools send then is the ant's own.
    pub fn assisting(&self) -> Option<UserId> {
        self.hub.assisting_now()
    }

    /// Serve the tools acting for the human the ant assists ([`Mcp::with_assisting`]) through `store`; without it
    /// they are refused.
    pub fn with_human_store(&self, store: Arc<dyn HumanStore>) {
        let _ = self.hub.human_store.set(store);
    }

    /// Open the human's sealed messages and seal those sent for it to humans through `sealing`, which does so only
    /// while the human enrolled the ant as one of its devices; without it none is opened or sealed.
    pub fn with_sealing(&self, sealing: Arc<dyn HumanSealing>) {
        let _ = self.hub.sealing.set(sealing);
    }

    /// Store and read attachments through `files`, attaching and saving files in `workdir`, or the directory a
    /// thread's sessions work in when they were given one ([`Mcp::servers`]); without it `attach` and
    /// `read_attachment` are refused.
    pub fn with_files(&self, files: FileAccess, workdir: PathBuf) {
        let _ = self.hub.files.set((files, workdir));
    }

    /// Redact with `redactor` the files the tools attach whose bytes are UTF-8 text, before they are stored.
    pub fn with_redactor(&self, redactor: Arc<Redactor>) {
        let _ = self.hub.redactor.set(redactor);
    }

    /// Take `provider` for the ant's provider, which [`Mcp::provider`] gives.
    pub fn with_provider(&self, provider: Connected) {
        let _ = self.hub.provider.set(provider);
    }

    /// Reach the ant's trail runs through `trails` for `run_trail`, `trail_status` and `stop_trail`, the trails' tips
    /// for `leave_tip`, `vote_tip` and `trail_tips`, the colony's saved trails for `save_trail` and `remove_trail`,
    /// and their ratings for `rate_trail` and `find_trails`, offered only then.
    pub fn with_trails(&self, trails: crate::trail::Trails) {
        let _ = self.hub.trails.set(trails);
    }

    /// Check through `dreams`, the ant's dreams as sense keeps them, the trails `save_trail` saves on a dream's thread
    /// or in answer to a dream finding ([`crate::sense::Dreams::ground`]).
    pub fn with_dreams(&self, dreams: crate::sense::Dreams) {
        let _ = self.hub.dreams.set(dreams);
    }

    /// The ant's provider, connected: none when the ant has none.
    pub fn provider(&self) -> Option<&Connected> {
        self.hub.provider.get()
    }

    /// The MCP servers each harness session gets: HTTP where the harness takes it, else `ants mcp` over the socket.
    /// The directory the session works in is where its tools attach and save files; the harness and model running it,
    /// and the repository of the directory ([`repository`]), are those the calls of the provider's tools carry.
    pub fn servers(&self) -> McpServers {
        let (hub, url, socket) = (self.hub.clone(), self.url.clone(), self.socket.clone());
        let exe = std::env::current_exe().ok();
        Arc::new(move |session: &McpSession<'_>| {
            let thread = session.thread;
            let running = Running {
                harness: session.harness.to_owned(),
                model: session.model.map(str::to_owned),
                repo: repository(session.dir),
            };
            {
                let mut sessions = hub.sessions.locked();
                sessions.dirs.insert(thread.clone(), session.dir.to_owned());
                sessions.running.insert(thread.clone(), running);
            }
            let token = hub.token(thread);
            if session.http {
                let auth = HttpHeader::new(header::AUTHORIZATION.as_str(), format!("Bearer {token}"));
                return vec![McpServer::Http(McpServerHttp::new(SERVER_NAME, &url).headers(vec![auth]))];
            }
            match (&exe, socket.as_ref().and_then(|s| s.to_str())) {
                (Some(exe), Some(socket)) => vec![McpServer::Stdio(
                    McpServerStdio::new(SERVER_NAME, exe)
                        .args(vec!["mcp".to_owned(), "--socket".to_owned(), socket.to_owned()])
                        .env(vec![EnvVariable::new(TOKEN_ENV, token)]),
                )],
                _ => Vec::new(),
            }
        })
    }

    /// Hand replies to waiting `ask_ant` calls as they arrive on `inbox`, the ant's ([`crate::bus::live_inbox`]).
    pub fn listen(&mut self, mut inbox: crate::bus::LiveInbox) {
        let hub = self.hub.clone();
        self.tasks.push(tokio::spawn(async move {
            while let Some(msg) = inbox.next().await {
                let Ok(envelope) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
                // The inbox consumer dead-letters a forged sender; it must not answer an ask meanwhile.
                match envelope.check_subject(&msg.subject) {
                    Ok(()) => hub.route(envelope).await,
                    Err(e) => tracing::warn!(error = %e, "ignoring a reply with a forged sender"),
                }
            }
        }));
    }

    /// Whether `envelope`, from the inbox consumer, answers an `ask_ant` call; then it is no prompt but the call's
    /// reply, or kept for `check_inbox` when the call gave up. True means it is handled and may be acked.
    pub async fn claim(&self, envelope: &Envelope) -> Result<bool> {
        self.hub.claim(envelope).await
    }

    /// Let `manage_channel` reach the channels through `admin`; without it the tool is refused.
    pub fn manage_channels(&self, admin: Arc<dyn Admin>) {
        let _ = self.hub.admin.set(admin);
    }

    /// How many late replies on `thread` wait for `check_inbox`.
    pub async fn late_on(&self, thread: &ThreadId) -> usize {
        self.hub.asks.lock().await.stored.late.iter().filter(|e| &e.thread_id == thread).count()
    }
}

/// A session opened: the harness running it, the model the config sets for it, and the repository it works in
/// ([`repository`]), read once as it opens.
struct Running {
    harness: String,
    model: Option<String>,
    repo: Option<String>,
}

#[derive(Default)]
struct Sessions {
    tokens: HashMap<String, ThreadId>,
    by_thread: HashMap<ThreadId, String>,
    /// The message each thread is handling.
    current: HashMap<ThreadId, Envelope>,
    /// The title each thread's session suggested during its turn, until a message on the thread carries it.
    titles: HashMap<ThreadId, String>,
    /// Whether each thread's session called `complete` during its turn, and why.
    completed: HashMap<ThreadId, turn::Completed>,
    /// The directory each thread's sessions work in.
    dirs: HashMap<ThreadId, PathBuf>,
    /// Each thread's last session opened.
    running: HashMap<ThreadId, Running>,
    /// Where each thread came from, as the messages it handled tell, settled ([`Mcp::settle_origin`]) or read back.
    origins: HashMap<ThreadId, Origin>,
    /// The paths of the files each thread's session attached to the final answer of its turn.
    reply_files: HashMap<ThreadId, Vec<String>>,
    /// The handles of the files attached to the message each thread is handling, and when the hub stored it.
    carried: HashMap<ThreadId, (Vec<AttachmentHandle>, DateTime<Utc>)>,
}

struct Hub {
    ant: UserId,
    outbox: Outbox,
    colony: Colony,
    notes: Arc<dyn NotesStore>,
    humans: Registry,
    escalator: Option<Arc<Escalator>>,
    channels: std::sync::OnceLock<ChannelAccess>,
    lookup: OnceLock<ColonyLookup>,
    admin: OnceLock<Arc<dyn Admin>>,
    requester: OnceLock<AntRequester>,
    reaction_asker: OnceLock<ReactionAsker>,
    /// The human whose assistant the ant's credentials make it ([`ants_proto::AssistantOf`]).
    assisting: OnceLock<UserId>,
    /// What the tools acting for that human read and write through.
    human_store: OnceLock<Arc<dyn HumanStore>>,
    /// What opens and seals that human's sealed messages.
    sealing: OnceLock<Arc<dyn HumanSealing>>,
    /// The attachments' store and the ant's workdir.
    files: OnceLock<(FileAccess, PathBuf)>,
    /// What redacts the text files the tools attach; none redacts nothing.
    redactor: OnceLock<Arc<Redactor>>,
    /// The ant's provider ([`crate::provider`]).
    provider: OnceLock<Connected>,
    /// The runs the ant conducts ([`Mcp::with_trails`]).
    trails: OnceLock<crate::trail::Trails>,
    /// The ant's dreams, when its provider is sense ([`Mcp::with_dreams`]).
    dreams: OnceLock<crate::sense::Dreams>,
    /// The provider's tools as listed last.
    served: Mutex<provider::Served>,
    /// Who the provider calls are for ([`Mcp::with_callers`]).
    callers: OnceLock<crate::provider::context::Callers>,
    /// Where the threads' origins are kept ([`Mcp::with_origins`]).
    origin_store: OnceLock<crate::provider::origins::OriginStore>,
    /// Held while a thread's origin is settled or read back, so none is lost to another.
    settling: tokio::sync::Mutex<()>,
    state_path: PathBuf,
    sessions: Mutex<Sessions>,
    asks: tokio::sync::Mutex<Asks>,
}

impl Hub {
    /// The token of `thread`'s sessions, made on first use.
    fn token(&self, thread: &ThreadId) -> String {
        let mut sessions = self.sessions.locked();
        if let Some(token) = sessions.by_thread.get(thread) {
            return token.clone();
        }
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        sessions.tokens.insert(token.clone(), thread.clone());
        sessions.by_thread.insert(thread.clone(), token.clone());
        token
    }

    fn caller(&self, token: &str) -> Option<ThreadId> {
        self.sessions.locked().tokens.get(token).cloned()
    }

    async fn send(&self, envelope: &Envelope) -> Result<(), String> {
        let current = self.sessions.locked().current.get(&envelope.thread_id).cloned();
        let trail_spent = self.trail_spent(current.as_ref(), envelope).await;
        (self.outbox)(Envelope { trail_spent, ..envelope.clone() }).await.map_err(|e| format!("not sent: {e:#}"))
    }
}

#[cfg(test)]
mod tests;
