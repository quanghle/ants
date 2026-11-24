//! MCP tools for the ant's harness sessions: `send_message`, `ask_ant`, `check_inbox`, `list_ants` and
//! `route` to find ants by their colony capabilities, `post_notes` and `read_notes` for the thread's notes
//! (see [`crate::stream::notes`]), and `escalate` to send the thread up the ant's escalation chain (see
//! [`crate::escalation`]). The tools of the ant's provider, what holds its identity, memory and work, come beside them,
//! named with the provider's name as prefix (`sense_recall`), each call carrying the ant, its identity, the thread and
//! where the thread came from (see `provider`).
//!
//! `decide_approval` answers another ant's tool request that came up its chain (see `crate::approvals`).
//!
//! `post_channel` starts a channel thread the ant creates: its escalations go up the ant's chain, whichever ant
//! gets stuck on it (see [`ants_proto::Escalation::new_from`]); with `on_thread` it posts on the thread of the
//! channel post being handled instead, as the ant's reply there does, spending one of the thread's turns.
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
//! session works in, `attach_to_reply` attaches them to the turn's final answer ([`Mcp::take_reply_files`]), and
//! `read_attachment` reads an attachment of a message the ant reads (see `attach`).
//!
//! An assistant also gets the `*_for_human` tools, which act for its human while the human's grant names it: read the
//! human's home queue, answer questions, send, post in channels and thread notes, react, title, mark done and decide
//! tool requests (see `assist`).
//!
//! One server per ant, with two ways in: streamable HTTP on a loopback port, for harnesses that take HTTP MCP
//! servers, and a local socket that `ants mcp` bridges stdio to, for the others (see `crate::ipc`). The sessions of each thread get their
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
use ants_proto::{AttachmentHandle, ChannelPost, Envelope, ThreadId, UserId, Uuid};
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

/// Where the MCP socket of the ant at a home goes: `<home>/mcp.sock`, else, that path too long for a Unix socket, one in
/// the runtime directory.
#[cfg(unix)]
pub use crate::ipc::address as socket_address;
pub use asks::Claim;
pub use assist::{HumanMessage, HumanSealing, HumanStore, jetstream_store};
pub use attach::{FileAccess, jetstream_files};
pub use channels::{ChannelAccess, ThreadPoster, jetstream_channels};
pub use managers::{AntAsk, AntRequester, ManagerAsk, Requester, RetireAsk};
pub(crate) use messages::asked_questions;
pub(crate) use provider::Directories;
pub use ratings::RATE_TRAIL;
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

/// Room a message the tools send leaves for its headers within the server's `max_payload`.
pub const HEADER_ROOM: usize = 1024;

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
    /// Listen on a loopback port and on a local socket (`ipc::Listener`). What must survive a restart (asks waiting for a reply, late
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
            channels: OnceLock::new(),
            thread_posts: OnceLock::new(),
            lookup: OnceLock::new(),
            admin: OnceLock::new(),
            requester: OnceLock::new(),
            reaction_asker: OnceLock::new(),
            assist: Assist::default(),
            attach: Attach::default(),
            provided: Provided::default(),
            trails: OnceLock::new(),
            dreams: OnceLock::new(),
            served: Mutex::default(),
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

    /// Serve `post_channel`'s `on_thread` through `poster`, which stores a post on the channel thread a turn handles;
    /// without it such a post is refused.
    pub fn with_thread_posts(&self, poster: ThreadPoster) {
        let _ = self.hub.thread_posts.set(poster);
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
        let _ = self.hub.assist.human.set(human);
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
        let _ = self.hub.assist.store.set(store);
    }

    /// Open the human's sealed messages and seal those sent for it to humans through `sealing`, which does so only
    /// while the human enrolled the ant as one of its devices; without it none is opened or sealed.
    pub fn with_sealing(&self, sealing: Arc<dyn HumanSealing>) {
        let _ = self.hub.assist.sealing.set(sealing);
    }

    /// Store and read attachments through `files`, attaching and saving files in `workdir`, or the directory a
    /// thread's sessions work in when they were given one ([`Mcp::servers`]); without it `attach` and
    /// `read_attachment` are refused.
    pub fn with_files(&self, files: FileAccess, workdir: PathBuf) {
        let _ = self.hub.attach.files.set((files, workdir));
    }

    /// Refuse a message or post the tools send whose JSON is over `max_payload` (the server's) less [`HEADER_ROOM`],
    /// before any of its attachments is stored; without it the server alone refuses one.
    pub fn with_max_payload(&self, max_payload: usize) {
        let _ = self.hub.attach.max_payload.set(max_payload);
    }

    /// Redact with `redactor` the files the tools attach whose bytes are UTF-8 text, before they are stored.
    pub fn with_redactor(&self, redactor: Arc<Redactor>) {
        let _ = self.hub.attach.redactor.set(redactor);
    }

    /// Take `provider` for the ant's provider, which [`Mcp::provider`] gives.
    pub fn with_provider(&self, provider: Connected) {
        let _ = self.hub.provided.provider.set(provider);
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
        self.hub.provided.provider.get()
    }

    /// The MCP servers each harness session gets: HTTP where the harness takes it, else `ants mcp` over the socket.
    /// The directory the session works in is where its tools attach and save files; the harness and model running it,
    /// and the repository of the directory ([`repository`]), are those the calls of the provider's tools carry.
    pub fn servers(&self) -> McpServers {
        let (hub, url, socket) = (self.hub.clone(), self.url.clone(), self.socket.clone());
        let exe = std::env::current_exe().ok();
        Arc::new(move |session: &McpSession<'_>| {
            let thread = session.thread;
            let token = hub.open(thread, session);
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

    /// Whether `envelope`, from the inbox consumer, answers an `ask_ant` call: then it is no prompt but the call's
    /// reply, or, when the call gave up, kept for `check_inbox` and prompted once its turn comes on its thread.
    pub async fn claim(&self, envelope: &Envelope) -> Result<Claim> {
        self.hub.claim(envelope).await
    }

    /// Whether `envelope`, a late reply ([`Claim::Late`]) whose turn came, is to be prompted: not when `check_inbox`
    /// took it first.
    pub async fn late_turn(&self, envelope: &Envelope) -> Result<bool> {
        self.hub.late_turn(envelope).await
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

/// What a thread's sessions were opened with: their token, and of the last one opened the directory it works in, the
/// harness running it, the model the config sets for it, and the repository it works in ([`repository`]), read once
/// as it opens.
struct Opened {
    token: String,
    dir: PathBuf,
    harness: String,
    model: Option<String>,
    repo: Option<String>,
}

/// The turn of a thread's session, from [`Mcp::begin`] until its guard drops.
struct TurnState {
    /// The message the thread is handling.
    current: Envelope,
    /// The title the session suggested, until a message on the thread carries it.
    title: Option<String>,
    /// Whether the session called `complete`, and why.
    completed: Option<turn::Completed>,
    /// The paths of the files the session attached to the final answer.
    reply_files: Option<Vec<String>>,
    /// The handles of the files attached to the message being handled, and when the hub stored it.
    carried: Option<(Vec<AttachmentHandle>, DateTime<Utc>)>,
    /// The channel post the message being handled was made of, with its stream sequence in the ant's inbox stream.
    post: Option<(ChannelPost, u64)>,
}

impl TurnState {
    /// The turn that handles `current`.
    fn of(current: Envelope) -> Self {
        Self { current, title: None, completed: None, reply_files: None, carried: None, post: None }
    }
}

#[derive(Default)]
struct Sessions {
    tokens: HashMap<String, ThreadId>,
    /// What each thread's sessions were opened with.
    opened: HashMap<ThreadId, Opened>,
    /// The turn each thread is in.
    turns: HashMap<ThreadId, TurnState>,
    /// Where each thread came from, as the messages it handled tell, settled ([`Mcp::settle_origin`]) or read back.
    origins: HashMap<ThreadId, Origin>,
}

impl Sessions {
    /// The message `thread` is handling.
    fn current(&self, thread: &ThreadId) -> Option<&Envelope> {
        self.turns.get(thread).map(|turn| &turn.current)
    }
}

/// Acting for the human the ant assists.
#[derive(Default)]
struct Assist {
    /// The human whose assistant the ant's credentials make it ([`ants_proto::AssistantOf`]).
    human: OnceLock<UserId>,
    /// What the tools acting for that human read and write through.
    store: OnceLock<Arc<dyn HumanStore>>,
    /// What opens and seals that human's sealed messages.
    sealing: OnceLock<Arc<dyn HumanSealing>>,
}

/// Attaching files to what the tools send, and what one message carries.
#[derive(Default)]
struct Attach {
    /// The attachments' store and the ant's workdir.
    files: OnceLock<(FileAccess, PathBuf)>,
    /// What redacts the text files the tools attach; none redacts nothing.
    redactor: OnceLock<Arc<Redactor>>,
    /// The server's `max_payload` ([`Mcp::with_max_payload`]).
    max_payload: OnceLock<usize>,
}

/// The ant's provider and what the calls of its tools carry.
#[derive(Default)]
struct Provided {
    /// The ant's provider ([`crate::provider`]).
    provider: OnceLock<Connected>,
    /// Who the provider calls are for ([`Mcp::with_callers`]).
    callers: OnceLock<crate::provider::context::Callers>,
    /// Where the threads' origins are kept ([`Mcp::with_origins`]).
    origins: OnceLock<crate::provider::origins::OriginStore>,
}

struct Hub {
    ant: UserId,
    outbox: Outbox,
    colony: Colony,
    notes: Arc<dyn NotesStore>,
    humans: Registry,
    escalator: Option<Arc<Escalator>>,
    channels: OnceLock<ChannelAccess>,
    /// What stores the posts `post_channel` makes on the channel thread a turn handles ([`Mcp::with_thread_posts`]).
    thread_posts: OnceLock<ThreadPoster>,
    lookup: OnceLock<ColonyLookup>,
    admin: OnceLock<Arc<dyn Admin>>,
    requester: OnceLock<AntRequester>,
    reaction_asker: OnceLock<ReactionAsker>,
    assist: Assist,
    attach: Attach,
    provided: Provided,
    /// The runs the ant conducts ([`Mcp::with_trails`]).
    trails: OnceLock<crate::trail::Trails>,
    /// The ant's dreams, when its provider is sense ([`Mcp::with_dreams`]).
    dreams: OnceLock<crate::sense::Dreams>,
    /// The provider's tools as listed last.
    served: Mutex<provider::Served>,
    /// Held while a thread's origin is settled or read back, so none is lost to another.
    settling: tokio::sync::Mutex<()>,
    state_path: PathBuf,
    sessions: Mutex<Sessions>,
    asks: tokio::sync::Mutex<Asks>,
}

impl Hub {
    /// Note what `session` of `thread` is opened with, and return the token of the thread's sessions, made on first
    /// use.
    fn open(&self, thread: &ThreadId, session: &McpSession<'_>) -> String {
        let (harness, model) = (session.harness.to_owned(), session.model.map(str::to_owned));
        let (dir, repo) = (session.dir.to_owned(), repository(session.dir));
        let mut sessions = self.sessions.locked();
        let token = match sessions.opened.get(thread) {
            Some(opened) => opened.token.clone(),
            None => {
                let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
                sessions.tokens.insert(token.clone(), thread.clone());
                token
            }
        };
        sessions.opened.insert(thread.clone(), Opened { token: token.clone(), dir, harness, model, repo });
        token
    }

    fn caller(&self, token: &str) -> Option<ThreadId> {
        self.sessions.locked().tokens.get(token).cloned()
    }

    /// Refuse `message`, a message or post the tools send, when its JSON is longer than one NATS message carries
    /// ([`Mcp::with_max_payload`]), naming its size and the limit.
    fn check_fits(&self, message: &impl serde::Serialize) -> Result<(), String> {
        let Some(&max_payload) = self.attach.max_payload.get().filter(|&&max| max > 0) else { return Ok(()) };
        let len = serde_json::to_vec(message).map_or(0, |json| json.len());
        let max = max_payload.saturating_sub(HEADER_ROOM);
        if len <= max {
            return Ok(());
        }
        Err(format!(
            "the message is {len} bytes; one message carries at most {max} bytes: write the text to a file in the \
             working directory and send it with `attach` instead; nothing sent"
        ))
    }

    async fn send(&self, envelope: &Envelope) -> Result<(), String> {
        let current = self.sessions.locked().current(&envelope.thread_id).cloned();
        let trail_spent = self.trail_spent(current.as_ref(), envelope).await;
        (self.outbox)(Envelope { trail_spent, ..envelope.clone() }).await.map_err(|e| format!("not sent: {e:#}"))
    }
}

#[cfg(test)]
mod tests;
