//! `ants`: talk to agent teams over NATS from the terminal, and run the persona daemon.

mod approve;
mod auth;
mod bridge;
mod channel;
mod humans;
mod inbox;
mod leaf;
mod outbox;
mod roster;
mod serve;

use std::path::{Path, PathBuf};

use ants_daemon::{HarnessKind, Launch, PermissionPolicy, bus, dead, harness, plan};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, Envelope, Escalation, JsDomain, Machine,
    PersonaId, PlanPost, StatusEvent, StatusUpdate, ThreadId, subject,
};
use anyhow::{Context as _, Result, bail};
use clap::{Parser, Subcommand};
use futures::StreamExt as _;

#[derive(Parser)]
#[command(name = "ants", version, about = "Agent teams messaging over NATS")]
struct Cli {
    /// NATS server: nats://, tls://, ws:// or wss://. Point it at the local leaf node where there is one.
    #[arg(long, global = true, env = "ANTS_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats: String,

    /// NATS credentials file (`ants auth user` writes one). Needed when the server enforces per-persona permissions.
    /// The CLI acts as the human they name, and registers it in `ants_humans` on connecting; without them, as `human`.
    #[arg(long, global = true, env = "ANTS_CREDS")]
    creds: Option<PathBuf>,

    /// JetStream domain of the hub (its `jetstream { domain }`), needed to reach JetStream from a leaf node without
    /// JetStream. Set it for every ants process, and when issuing credentials with `ants auth`.
    #[arg(long, global = true, env = "ANTS_JS_DOMAIN")]
    js_domain: Option<JsDomain>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send a message as the CLI's human, who owns the thread. Starts with @persona to pick the recipient; the
    /// messenger gets it otherwise.
    Ask {
        /// Continue an existing thread.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// Wait for the first reply on the thread, print it and mark it read.
        #[arg(long)]
        wait: bool,
        /// `@persona` (optional), then the message.
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
    /// Print the replies to the CLI's human not read yet, including those that arrived while no CLI was running, and
    /// mark them read.
    Inbox {
        /// Show without marking read.
        #[arg(long)]
        peek: bool,
        /// Only replies on this thread; the others stay unread.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// At most this many replies; the rest stay unread.
        #[arg(long)]
        limit: Option<usize>,
        /// Keep waiting for new replies until Ctrl-C, meanwhile dead-lettering the messages JetStream stops
        /// delivering without the daemon having stored them as dead letters (as `ants dead watch` does).
        #[arg(long, short)]
        follow: bool,
        /// One JSON envelope per line.
        #[arg(long)]
        json: bool,
    },
    /// List the personas being served: harness, machine, capabilities, escalation chain, last heartbeat, and whether it
    /// is recent.
    Roster {
        /// One JSON entry per line, with its state.
        #[arg(long)]
        json: bool,
    },
    /// Post to a channel as the CLI's human. An @persona in the text wakes only that member; a post that @mentions
    /// nobody goes to the channel's messenger, which wakes the members the post concerns. A new thread belongs to the
    /// human who posts it, and the turn budget it runs out of comes back to that human.
    Post {
        /// The channel: `ops` or `'#ops'` (quoted, as shells take an unquoted `#` for a comment).
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Post on an existing thread of the channel instead of starting one; the thread keeps its owner.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// Print the posts on the thread as each arrives, until the timeout or Ctrl-C.
        #[arg(long)]
        wait: bool,
        /// Seconds to wait for posts with `--wait`.
        #[arg(long, value_name = "SECS", default_value_t = 600, requires = "wait")]
        timeout: u64,
        /// The post; each @persona mention wakes that member.
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
    /// Channels: named groups of personas that humans post to with `ants post`.
    #[command(subcommand)]
    Channel(ChannelCommand),
    /// Show the plan room of a thread, or post to it as the CLI's human, which gives the room a fresh turn budget.
    Plan {
        thread: ThreadId,
        /// The post. Without it, show the room.
        words: Vec<String>,
    },
    /// Print ants traffic as it happens, escalations with their level, leader and personas visited, or the live
    /// progress of a persona: state, reply text, tool calls and plan.
    Watch {
        /// Subject filter, or a persona to follow its progress on every thread.
        #[arg(default_value = subject::ALL)]
        subject: String,
        /// With a persona, follow only this thread.
        thread: Option<ThreadId>,
    },
    /// Create the ANTS stream and the roster, sessions, attempts, channels, humans and chains buckets on the server, or
    /// add the stream subjects and direct gets an older version lacks. Needs a human's credentials.
    Init,
    /// Tool requests personas escalate: list the pending ones, show one, or answer it. Every human sees every
    /// request; the first decision wins, a human's or that of an agent of the persona's escalation chain.
    Approve {
        /// Request as `<persona>/<id>`, or a bare id when only one persona has it; a unique start of the id will do.
        /// Without it, list the pending requests.
        id: Option<String>,
        /// Answer the request. Without it, show the request.
        #[arg(requires = "id")]
        verdict: Option<approve::Answer>,
        /// After listing, keep printing new requests until Ctrl-C.
        #[arg(long, short, conflicts_with = "id")]
        follow: bool,
        /// One JSON request per line.
        #[arg(long)]
        json: bool,
    },
    /// Messages personas gave up on: inspect, replay or drop them.
    #[command(subcommand)]
    Dead(DeadCommand),
    /// Serve persona inboxes through agent harnesses: those of the config file, or one described by flags.
    Daemon(Box<serve::DaemonArgs>),
    /// Bridge stdio to a daemon's MCP tools, for harnesses without HTTP MCP. The daemon configures this itself.
    Mcp {
        /// The daemon's MCP socket.
        #[arg(long)]
        socket: PathBuf,
        /// The session's token.
        #[arg(long, env = ants_daemon::mcp::TOKEN_ENV, hide_env_values = true)]
        token: String,
    },
    /// Talk to a harness directly, without NATS.
    #[command(subcommand)]
    Harness(HarnessCommand),
    /// Issue NATS credentials: one operator and account for the team, one user per persona or human.
    #[command(subcommand)]
    Auth(AuthCommand),
    /// The machine's local NATS leaf node, which every ants process on it connects to.
    #[command(subcommand)]
    Leaf(LeafCommand),
    /// Carry a leaf node's uplink over plain HTTPS requests, for networks whose proxies block WebSocket.
    #[command(subcommand)]
    Bridge(BridgeCommand),
}

#[derive(Subcommand)]
enum ChannelCommand {
    /// Create a channel; fails when the channel exists.
    Create {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Member persona (repeatable, or comma-separated).
        #[arg(long = "member", value_name = "PERSONA", value_delimiter = ',', value_parser = channel::parse_persona)]
        members: Vec<PersonaId>,
        /// Persona that triages the posts of humans that @mention nobody (default: `messenger`).
        #[arg(long, value_parser = channel::parse_persona)]
        messenger: Option<PersonaId>,
    },
    /// Replace the members or the messenger of a channel; what is not given stays.
    Set {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Member persona (repeatable, or comma-separated): together, the new members.
        #[arg(long = "member", value_name = "PERSONA", value_delimiter = ',', value_parser = channel::parse_persona)]
        members: Option<Vec<PersonaId>>,
        /// Persona that triages the posts of humans that @mention nobody.
        #[arg(long, value_parser = channel::parse_persona, conflicts_with = "no_messenger")]
        messenger: Option<PersonaId>,
        /// Go back to the default messenger, `messenger`.
        #[arg(long)]
        no_messenger: bool,
    },
    /// Add members to a channel.
    Add {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Personas: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_parser = channel::parse_persona)]
        personas: Vec<PersonaId>,
    },
    /// Remove members from a channel.
    Remove {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Personas: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_parser = channel::parse_persona)]
        personas: Vec<PersonaId>,
    },
    /// List the channels with the members and the messenger of each.
    List,
    /// Print the posts of a channel kept in the stream, by thread: the post that started each thread, then the
    /// replies on the thread, indented.
    Read {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Only this thread.
        #[arg(long)]
        thread: Option<ThreadId>,
        /// Only the last this many threads.
        #[arg(long, conflicts_with = "thread")]
        limit: Option<usize>,
    },
    /// Delete a channel. The posts to the channel stay in the stream.
    Delete {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
    },
}

#[derive(Subcommand)]
enum BridgeCommand {
    /// On the hub, behind the TLS-terminating reverse proxy: open a connection to the upstream (the hub's leafnode
    /// port) for each session `ants bridge connect` starts.
    Serve {
        /// Address for the reverse proxy.
        #[arg(long, default_value = bridge::DEFAULT_SERVE_LISTEN)]
        listen: String,
        /// Where sessions connect to: the hub's leafnode port.
        #[arg(long, default_value = bridge::DEFAULT_UPSTREAM)]
        upstream: String,
        /// Bearer token every request must carry. Without it, anyone reaching the bridge may open sessions, which
        /// NATS authentication then checks as for any leaf node.
        #[arg(long, env = bridge::TOKEN_ENV, hide_env_values = true)]
        token: Option<String>,
    },
    /// On a worker: listen locally and carry each connection to the hub's `ants bridge serve`. The machine's leaf
    /// node then uses the uplink nats-leaf://127.0.0.1:7422.
    Connect {
        /// The bridge's URL, https://host[:port]/path, as the hub's reverse proxy mounts it.
        url: String,
        /// Local address for the leaf node's uplink; loopback only.
        #[arg(long, default_value = bridge::DEFAULT_CONNECT_LISTEN)]
        listen: String,
        /// Bearer token, as given to `ants bridge serve`.
        #[arg(long, env = bridge::TOKEN_ENV, hide_env_values = true)]
        token: Option<String>,
        /// Extra request header, `Name: value` (repeatable), for a gateway in front of the hub.
        #[arg(long = "header", value_name = "HEADER")]
        headers: Vec<String>,
        /// HTTP CONNECT forward proxy, http://[user:password@]host:port.
        #[arg(long, value_name = "URL")]
        proxy: Option<String>,
    },
}

#[derive(Subcommand)]
enum DeadCommand {
    /// List dead letters, oldest first.
    List,
    /// Send a dead letter's message to its inbox again, as it was, and remove the dead letter.
    Replay { seq: u64 },
    /// Remove a dead letter without replaying it.
    Drop { seq: u64 },
    /// Keep dead-lettering, until Ctrl-C, the messages JetStream stops delivering without the daemon having stored
    /// them as dead letters, and print each one. `ants inbox --follow` does the same while it runs.
    Watch,
}

#[derive(Subcommand)]
enum AuthCommand {
    /// Create the team operator and account, and the server.conf that makes nats-server trust them.
    Init {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Issue credentials for a persona, allowing only the subjects of its role, or with --human for a human, a CLI
    /// user. `human` always gets a human's. Give the hub's --js-domain, if it has one, so they work with it.
    User {
        persona: PersonaId,
        /// Issue a human's credentials, with the same rights as every other human. The human joins `ants_humans`,
        /// the team's humans, on its first CLI connection.
        #[arg(long)]
        human: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/<persona>.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Issue the uplink credentials of a machine's leaf node (leafnodes.remotes[].credentials), allowing what the
    /// personas and humans on the machine may do and usable only by a leaf node connection.
    Leaf {
        /// Machine name, for the credentials' name and file name, and the leaf's JetStream domain: letters, digits,
        /// '-' and '_'.
        machine: Machine,
        /// Persona served on the machine (repeatable). `human` counts as a human.
        #[arg(long = "persona", value_name = "PERSONA", required_unless_present = "humans")]
        personas: Vec<PersonaId>,
        /// Human running the CLI on the machine (repeatable), as issued with `ants auth user NAME --human`.
        #[arg(long = "human", value_name = "NAME")]
        humans: Vec<PersonaId>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/leaf-<machine>.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum LeafCommand {
    /// Write the nats-server configuration of the machine's leaf node: listening on localhost only, with one uplink
    /// to the hub, and a JetStream of its own (domain: the machine name) for the outbox. Run it again to switch the
    /// uplink, then reload the leaf (SIGHUP).
    Config {
        /// Machine name, as given to `ants auth leaf`.
        machine: Machine,
        /// Hub URL: nats-leaf:// or tls:// for its leafnode port (also through `devtunnel connect`), ws:// or wss://
        /// for its websocket listener (a leaf node asks for <path>/leafnode). Repeatable, all of one scheme.
        #[arg(long = "uplink", value_name = "URL", required = true)]
        uplinks: Vec<String>,
        /// Uplink credentials from `ants auth leaf` (default: <dir>/leaf-<machine>.creds).
        #[arg(long)]
        uplink_creds: Option<PathBuf>,
        /// The team's server.conf from `ants auth init` (default: <dir>/server.conf).
        #[arg(long)]
        server_conf: Option<PathBuf>,
        /// Team directory holding server.conf and the credentials (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// CA certificate of a hub the platform does not trust (tls:// and wss:// uplinks).
        #[arg(long)]
        ca_file: Option<PathBuf>,
        /// HTTP CONNECT forward proxy for the uplink, http://host:port or https://host:port.
        #[arg(long, value_name = "URL")]
        proxy: Option<String>,
        /// Address for the machine's ants processes; loopback only.
        #[arg(long, default_value = leaf::DEFAULT_LISTEN)]
        listen: String,
        /// Where to write the configuration (default: ~/.config/ants/leaf.conf).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// As a human, with the leaf node of the machine up and connected: create the machine's outbox, which keeps
    /// what its personas and humans send while the hub is out of reach, and have the hub's stream take from it what
    /// they may send once the uplink is back. Run it again when the machine's personas or humans change.
    Outbox {
        /// Machine name, as given to `ants leaf config`.
        machine: Machine,
        /// Persona served on the machine (repeatable), as given to `ants auth leaf`. `human` counts as a human.
        #[arg(long = "persona", value_name = "PERSONA", required_unless_present_any = ["humans", "remove"])]
        personas: Vec<PersonaId>,
        /// Human running the CLI on the machine (repeatable), as given to `ants auth leaf`.
        #[arg(long = "human", value_name = "NAME")]
        humans: Vec<PersonaId>,
        /// Stop taking anything from the machine's outbox instead.
        #[arg(long, conflicts_with_all = ["personas", "humans"])]
        remove: bool,
    },
}

#[derive(Subcommand)]
enum HarnessCommand {
    /// Start the harness and complete the ACP handshake.
    Check {
        #[arg(long, default_value_t = HarnessKind::Copilot)]
        harness: HarnessKind,
    },
    /// Run one prompt in the current directory and print the reply. Tool requests are rejected.
    Prompt {
        #[arg(long, default_value_t = HarnessKind::Copilot)]
        harness: HarnessKind,
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("ANTS_LOG").unwrap_or_else(|_| "info,rmcp=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let domain = cli.js_domain.as_ref();
    let nats = || Nats::new(&cli.nats, cli.creds.as_deref(), domain);
    match cli.command {
        Command::Ask { thread, wait, words } => ask(&nats()?, thread, wait, words).await,
        Command::Inbox { peek, thread, limit, follow, json } => {
            let select = inbox::Select { thread, limit, peek };
            show_inbox(&nats()?, &select, follow, json).await
        }
        Command::Roster { json } => roster::show(&nats()?, json).await,
        Command::Post { channel, thread, wait, timeout, words } => {
            let wait = wait.then(|| std::time::Duration::from_secs(timeout));
            post(&nats()?, channel, thread, wait, words).await
        }
        Command::Channel(command) => channels(&nats()?, command).await,
        Command::Plan { thread, words } => plan_room(&nats()?, thread, words).await,
        Command::Watch { subject, thread } => {
            let filter = watch_filter(&subject, thread.as_ref())?;
            watch(&nats()?, filter).await
        }
        Command::Init => {
            let nats = nats()?;
            let (_, js) = nats.connect_only("ants-cli").await?;
            bus::init_stream(&js).await?;
            bus::ensure_roster(&js, ants_proto::ROSTER_TTL).await?;
            bus::ensure_sessions(&js).await?;
            bus::ensure_attempts(&js).await?;
            bus::ensure_channels(&js).await?;
            bus::ensure_chains(&js).await?;
            humans::ensure(&js).await?;
            humans::register_or_warn(&js, &nats.me).await;
            println!(
                "stream {} and buckets {}, {}, {}, {}, {} and {} ready",
                ants_proto::STREAM,
                ants_proto::ROSTER_BUCKET,
                ants_proto::SESSIONS_BUCKET,
                ants_proto::ATTEMPTS_BUCKET,
                ants_proto::CHANNELS_BUCKET,
                ants_proto::HUMANS_BUCKET,
                ants_proto::CHAINS_BUCKET
            );
            Ok(())
        }
        Command::Approve { id, verdict, follow, json } => {
            let args = approve::Args { id, answer: verdict, follow, json };
            approve::run(&nats()?, args).await
        }
        Command::Dead(command) => dead_letters(&nats()?, command).await,
        Command::Daemon(args) => {
            let personas = serve::personas(&args, &ants_daemon::config::default_path()?)?;
            serve::run(serve::configs(personas, &cli.nats, cli.creds, domain.cloned())?).await
        }
        Command::Mcp { socket, token } => ants_daemon::mcp::bridge_stdio(&socket, &token).await,
        Command::Harness(HarnessCommand::Check { harness }) => {
            println!("{harness}: {}", harness::check(harness).await?);
            Ok(())
        }
        Command::Harness(HarnessCommand::Prompt { harness, words }) => {
            let launch = Launch::new(
                harness,
                std::env::current_dir()?,
                Vec::new(),
                PermissionPolicy::reject_all(),
                Default::default(),
            );
            let reply = harness::prompt_once(&launch, &words.join(" ")).await?;
            println!("{reply}");
            Ok(())
        }
        Command::Auth(AuthCommand::Init { dir }) => {
            let dir = team_dir(dir)?;
            auth::init(&dir)?;
            println!(
                "team keys in {0}\ncopy {0}/{1} next to the nats-server configuration and include \"{1}\" from it (include paths are relative)",
                dir.display(),
                auth::SERVER_CONF
            );
            Ok(())
        }
        Command::Auth(AuthCommand::User { persona, human, dir, out }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("{persona}.creds")));
            auth::issue(&dir, &persona, auth::role(&persona, human), domain, &out)?;
            println!("{}", out.display());
            Ok(())
        }
        Command::Auth(AuthCommand::Leaf { machine, personas, humans, dir, out }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
            auth::issue_leaf(&dir, &machine, &auth::members(&personas, &humans), domain, &out)?;
            println!("{}", out.display());
            Ok(())
        }
        Command::Leaf(LeafCommand::Config {
            machine,
            uplinks,
            uplink_creds,
            server_conf,
            dir,
            ca_file,
            proxy,
            listen,
            out,
        }) => {
            let dir = team_dir(dir)?;
            let creds = uplink_creds.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
            let server_conf = server_conf.unwrap_or_else(|| dir.join(auth::SERVER_CONF));
            let out = match out {
                Some(out) => out,
                None => auth::default_dir()?.with_file_name("leaf.conf"),
            };
            let config = leaf::Leaf {
                machine: &machine,
                hub_domain: domain,
                uplinks: &uplinks,
                creds: &creds,
                server_conf: &server_conf,
                ca_file: ca_file.as_deref(),
                proxy: proxy.as_deref(),
                listen: &listen,
            };
            leaf::write(&config, &out)?;
            println!(
                "{0}\nstart: nats-server -c {0} (scripts/install-units.sh installs it as a service)\n\
                 switch the uplink: run this again, then send the running leaf SIGHUP\n\
                 clients on this machine: nats://{listen} (the default) with the hub's --js-domain\n\
                 once the leaf is up: ants leaf outbox {machine} --persona ... --human ...",
                out.display()
            );
            Ok(())
        }
        Command::Bridge(BridgeCommand::Serve { listen, upstream, token }) => {
            let addr = bridge::listen_addr(&listen, false)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            tracing::info!("bridge on http://{addr}, sessions connect to {upstream}");
            bridge::Server::new(upstream, token).run(listener).await
        }
        Command::Bridge(BridgeCommand::Connect { url, listen, token, headers, proxy }) => {
            let remote = bridge::Remote::new(&url, token.as_deref(), &headers, proxy.as_deref())?;
            let addr = bridge::listen_addr(&listen, true)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            tracing::info!("bridging {addr} to {url}; leaf uplink: nats-leaf://{addr}");
            remote.run(listener).await
        }
        Command::Leaf(LeafCommand::Outbox { machine, personas, humans, remove }) => {
            let nats = nats()?;
            let (_, js) = nats.connect("ants-cli").await?;
            if remove {
                outbox::remove(&js, &machine).await?;
                println!("the hub no longer takes anything from the outbox of {machine}");
            } else {
                let leaf = outbox::connect(&cli.nats, cli.creds.as_deref(), &nats.me.id, &machine).await?;
                outbox::admit(&leaf, &js, &machine, &auth::members(&personas, &humans)).await?;
                println!("outbox of {machine} ready; the hub takes from it what its personas and humans send");
            }
            Ok(())
        }
    }
}

/// How the CLI reaches NATS, and as whom: the human its credentials name ([`humans::Me`]).
pub struct Nats<'a> {
    url: &'a str,
    creds: Option<&'a Path>,
    domain: Option<&'a JsDomain>,
    pub me: humans::Me,
}

impl<'a> Nats<'a> {
    fn new(url: &'a str, creds: Option<&'a Path>, domain: Option<&'a JsDomain>) -> Result<Self> {
        Ok(Self { url, creds, domain, me: humans::Me::from_creds(creds)? })
    }

    /// Connect as [`Nats::me`], and register it in the humans bucket ([`humans::register_or_warn`]).
    pub async fn connect(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let (client, js) = self.connect_only(name).await?;
        humans::register_or_warn(&js, &self.me).await;
        Ok((client, js))
    }

    /// Connect as [`Nats::me`] without registering it.
    async fn connect_only(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        bus::connect(self.url, self.creds, self.domain, &self.me.id, name).await
    }
}

fn team_dir(dir: Option<PathBuf>) -> Result<PathBuf> {
    match dir {
        Some(dir) => Ok(dir),
        None => auth::default_dir(),
    }
}

/// Split a leading `@persona` off the words.
fn recipient(mut words: Vec<String>) -> Result<(PersonaId, String)> {
    let to = match words.first().and_then(|w| w.strip_prefix('@')) {
        Some(name) => {
            let to = name.parse()?;
            words.remove(0);
            to
        }
        None => PersonaId::messenger(),
    };
    if words.is_empty() {
        bail!("nothing to send");
    }
    Ok((to, words.join(" ")))
}

async fn ask(nats: &Nats<'_>, thread: Option<ThreadId>, wait: bool, words: Vec<String>) -> Result<()> {
    let envelope = ask_envelope(&nats.me, thread, words)?;
    let (client, js) = nats.connect("ants-cli").await?;
    // Subscribe first so a fast reply is not missed.
    let mut replies = client.subscribe(subject::inbox_of(&nats.me.id)).await?;
    if bus::publish(&js, &envelope).await? == bus::Stored::Outbox {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    println!("thread {}", envelope.thread_id);
    if !wait {
        return Ok(());
    }
    while let Some(msg) = replies.next().await {
        let Ok(reply) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
        if let Err(e) = reply.check_subject(&msg.subject) {
            tracing::warn!("ignoring a reply with a forged sender: {e}");
            continue;
        }
        if reply.thread_id == envelope.thread_id {
            println!("{}: {}", reply.from, reply.text);
            if !nats.me.role.is_human() {
                // A persona's inbox consumer is its daemon's.
                return Ok(());
            }
            // Seen here, so `ants inbox` need not show it again. The other replies stay unread.
            match inbox::mark_read(&js, &nats.me.id, &reply).await {
                Ok(true) => {}
                Ok(false) => tracing::debug!("reply already read"),
                Err(e) => tracing::warn!("could not mark the reply read: {e:#}"),
            }
            return Ok(());
        }
    }
    bail!("connection closed before a reply arrived")
}

/// What `ants ask` sends as `me`: a thread `me` owns, as it starts it or continues it, when `me` is a human. A thread
/// a persona starts has no owner: only a human gets a thread's escalations, with a fresh turn budget.
fn ask_envelope(me: &humans::Me, thread: Option<ThreadId>, words: Vec<String>) -> Result<Envelope> {
    let (to, text) = recipient(words)?;
    let envelope = match thread {
        Some(thread) => Envelope::on_thread(thread, me.id.clone(), to, text),
        None => Envelope::new(me.id.clone(), to, text),
    };
    Ok(envelope.with_owner(me.role.is_human().then(|| me.id.clone())))
}

async fn post(
    nats: &Nats<'_>,
    channel: ChannelId,
    thread: Option<ThreadId>,
    wait: Option<std::time::Duration>,
    words: Vec<String>,
) -> Result<()> {
    let text = words.join(" ");
    if text.trim().is_empty() {
        bail!("nothing to post");
    }
    let (client, js) = nats.connect("ants-cli").await?;
    let humans = humans::load(&js, &nats.me).await;
    let post = channel::compose(&js, &nats.me, &humans, channel, thread, text).await;
    let entry = channel::get(&channel::open(&js).await?, &post.channel).await?;
    if entry.is_none() {
        tracing::warn!("no channel #{0} (`ants channel create {0}` makes one): the post wakes nobody", post.channel);
    }
    for persona in channel::idle_mentions(&post, entry.as_ref(), &humans) {
        tracing::warn!("@{persona} is no member of #{}: the post does not wake {persona}", post.channel);
    }
    // Subscribe first so a fast reply is not missed.
    let mut replies = match wait {
        Some(_) => Some(client.subscribe(subject::chan_room(&post.channel)).await?),
        None => None,
    };
    if bus::store(&js, post.subject(), &post, &post.id.to_string()).await? == bus::Stored::Outbox {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    println!("thread {}", post.thread_id);
    let (Some(timeout), Some(replies)) = (wait, replies.as_mut()) else { return Ok(()) };
    let stop = async { _ = tokio::signal::ctrl_c().await };
    let shown = channel::follow_thread(replies, &post, timeout, stop, |reply| {
        println!("{}: {}", reply.from, reply.text);
    })
    .await?;
    if shown == 0 {
        println!("no post on the thread within {}s", timeout.as_secs());
    }
    Ok(())
}

async fn channels(nats: &Nats<'_>, command: ChannelCommand) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let kv = channel::open(&js).await?;
    let humans = humans::load(&js, &nats.me).await;
    match command {
        ChannelCommand::Create { channel, members, messenger } => {
            channel::check_not_human(members.iter().chain(&messenger), &humans)?;
            let mut entry = ants_proto::ChannelEntry::new(channel);
            for member in members {
                entry.add_member(member);
            }
            entry.messenger = messenger;
            channel::create(&kv, &entry).await?;
            warn_unserved(&js, entry.members.iter().chain(&entry.messenger)).await;
            println!("created {}", channel::describe(&entry));
        }
        ChannelCommand::Set { channel, members, messenger, no_messenger } => {
            if members.is_none() && messenger.is_none() && !no_messenger {
                bail!("nothing to change: give --member, --messenger or --no-messenger");
            }
            channel::check_not_human(members.iter().flatten().chain(&messenger), &humans)?;
            let (entry, ()) = channel::update(&kv, &channel, |entry| {
                if let Some(members) = &members {
                    entry.members.clear();
                    for member in members {
                        entry.add_member(member.clone());
                    }
                }
                if messenger.is_some() || no_messenger {
                    entry.messenger.clone_from(&messenger);
                }
            })
            .await?;
            warn_unserved(&js, members.iter().flatten().chain(&messenger)).await;
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::Add { channel, personas } => {
            channel::check_not_human(&personas, &humans)?;
            let (entry, added) = channel::update(&kv, &channel, |entry| {
                personas.iter().filter(|p| entry.add_member((*p).clone())).cloned().collect::<Vec<_>>()
            })
            .await?;
            for persona in personas.iter().filter(|p| !added.contains(p)) {
                tracing::warn!("{persona} is already a member of #{channel}");
            }
            warn_unserved(&js, &added).await;
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::Remove { channel, personas } => {
            let (entry, removed) = channel::update(&kv, &channel, |entry| {
                personas.iter().filter(|p| entry.remove_member(p)).cloned().collect::<Vec<_>>()
            })
            .await?;
            for persona in personas.iter().filter(|p| !removed.contains(p)) {
                tracing::warn!("{persona} is no member of #{channel}");
            }
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::List => {
            let entries = channel::list(&kv).await?;
            if entries.is_empty() {
                println!("no channels");
            } else {
                println!("{}", channel::table(&entries));
            }
        }
        ChannelCommand::Read { channel, thread, limit } => {
            if channel::get(&kv, &channel).await?.is_none() {
                tracing::warn!("no channel #{channel}; showing the posts kept for #{channel} anyway");
            }
            let mut threads = channel::threads(channel::read(&js, &nats.me.id, &channel).await?);
            if let Some(thread) = &thread {
                threads.retain(|t| t.id == *thread);
            }
            if let Some(limit) = limit {
                threads.drain(..threads.len().saturating_sub(limit));
            }
            if threads.is_empty() {
                match thread {
                    Some(thread) => println!("no posts on thread {thread} of #{channel}"),
                    None => println!("no posts in #{channel}"),
                }
            }
            print!("{}", channel::render(&threads));
        }
        ChannelCommand::Delete { channel } => {
            channel::delete(&kv, &channel).await?;
            println!("deleted #{channel}; the posts to #{channel} stay in the stream");
        }
    }
    Ok(())
}

/// Warn about members and messengers no daemon serves now, rather than refuse them: one may start later.
async fn warn_unserved<'a>(js: &async_nats::jetstream::Context, personas: impl IntoIterator<Item = &'a PersonaId>) {
    let personas: Vec<&PersonaId> = personas.into_iter().collect();
    if personas.is_empty() {
        return;
    }
    match roster::list(js).await {
        Ok(roster) => {
            for persona in channel::unserved(personas, &roster) {
                tracing::warn!("{persona} is not in the roster: no daemon serves {persona} now");
            }
        }
        Err(e) => tracing::debug!("not checking the roster: {e:#}"),
    }
}

async fn show_inbox(nats: &Nats<'_>, select: &inbox::Select, follow: bool, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are a persona's, whose inbox its daemon reads; `ants inbox` reads a human's");
    }
    let (client, js) = nats.connect("ants-inbox").await?;
    let mut inbox = inbox::Inbox::open(&js, me).await?;
    let show = |e: &Envelope| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(e)?);
        } else {
            println!("{}", inbox::render(e));
        }
        Ok(())
    };
    let result = async {
        let unread = inbox.unread(select).await?;
        if unread.is_empty() && !follow && !json {
            println!("no unread replies");
        }
        unread.iter().try_for_each(show)?;
        if follow {
            // A dead letter stored here tells the human through the inbox, like one the daemon stores.
            let watch = async {
                if let Err(e) = dead::watch(&client, &js, |_, _| {}).await {
                    tracing::warn!("not watching for messages JetStream stopped delivering: {e:#}");
                }
                std::future::pending::<()>().await
            };
            tokio::select! {
                followed = inbox.follow(select, show, async { _ = tokio::signal::ctrl_c().await }) => followed?,
                () = watch => {}
            }
        }
        anyhow::Ok(())
    }
    .await;
    // Hand back what was not taken even after an error, so it is not hidden until the ack wait runs out.
    let released = inbox.release().await;
    result.and(released)
}

async fn dead_letters(nats: &Nats<'_>, command: DeadCommand) -> Result<()> {
    let (client, js) = nats.connect("ants-cli").await?;
    let stream = || async { js.get_stream(ants_proto::STREAM).await.context("opening the ANTS stream") };
    match command {
        DeadCommand::List => {
            let letters = dead::list(&stream().await?).await?;
            if letters.is_empty() {
                println!("no dead letters");
            }
            for (seq, letter) in &letters {
                println!("{}", dead_row(*seq, letter));
            }
        }
        DeadCommand::Replay { seq } => {
            let envelope = dead::replay(&js, &stream().await?, seq).await?;
            println!("replayed to {} on thread {}", envelope.to, envelope.thread_id);
        }
        DeadCommand::Drop { seq } => {
            dead::discard(&stream().await?, seq).await?;
            println!("dropped dead letter {seq}");
        }
        DeadCommand::Watch => {
            let watch = dead::watch(&client, &js, |seq, letter| println!("{}", dead_row(seq, letter)));
            tokio::select! {
                watched = watch => watched?,
                _ = tokio::signal::ctrl_c() => {}
            }
        }
    }
    Ok(())
}

/// One line of `ants dead list`: sequence, persona, thread, sender, deliveries, time, error and the start of the text.
fn dead_row(seq: u64, letter: &DeadLetter) -> String {
    let time = letter.died_at.format("%Y-%m-%d %H:%M:%S");
    let (thread, from, text) = match &letter.envelope {
        Some(e) => (e.thread_id.as_str(), e.from.as_str(), e.text.as_str()),
        None => ("-", "-", letter.payload.as_deref().unwrap_or("")),
    };
    format!(
        "{seq}\t{}\t{thread}\t{from}\t{}x\t{time}\t{}\t{}",
        letter.persona,
        letter.deliveries,
        excerpt(&letter.error, 60),
        excerpt(text, 60)
    )
}

/// The first line of `text`, cut to `max` characters.
fn excerpt(text: &str, max: usize) -> String {
    let line = text.lines().next().unwrap_or("");
    let mut out: String = line.chars().take(max).collect();
    if out.len() < text.len() {
        out.push('…');
    }
    out
}

async fn plan_room(nats: &Nats<'_>, thread: ThreadId, words: Vec<String>) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    if !words.is_empty() {
        let post = PlanPost::new(thread, nats.me.id.clone(), words.join(" "));
        plan::publish(&js, &post, None).await?;
        println!("posted to the plan room of thread {} ({} turns left)", post.thread_id, post.turn_budget);
        return Ok(());
    }
    let room = plan::read(&js, &nats.me.id, &thread).await?;
    if room.posts.is_empty() {
        println!("the plan room of thread {thread} is empty");
    }
    for post in &room.posts {
        let time = post.sent_at.format("%Y-%m-%d %H:%M:%S");
        println!("{time} {} ({} turns left): {}", post.from, post.turn_budget, post.text);
    }
    Ok(())
}

/// What `ants watch <subject> [thread]` subscribes to: a subject filter as given, or, for a persona (one token), its
/// status subjects.
fn watch_filter(target: &str, thread: Option<&ThreadId>) -> Result<String> {
    match target.parse::<PersonaId>().ok() {
        Some(persona) => Ok(thread.map_or_else(|| subject::status_of(&persona), |t| subject::status(&persona, t))),
        None if thread.is_some() => bail!("a thread can only follow a persona, not the subject filter {target:?}"),
        None => Ok(target.to_owned()),
    }
}

async fn watch(nats: &Nats<'_>, filter: String) -> Result<()> {
    use std::io::Write as _;

    let (client, _) = nats.connect("ants-watch").await?;
    let mut sub = client.subscribe(filter).await?;
    let mut printer = Printer::default();
    while let Some(msg) = sub.next().await {
        let forged = |ok: bool| if ok { String::new() } else { format!(" (forged: sent on {})", msg.subject) };
        let out = if let Ok(event) = serde_json::from_slice::<StatusEvent>(&msg.payload) {
            printer.status(&event, &forged(event.check_subject(&msg.subject).is_ok()))
        } else if let Ok(e) = serde_json::from_slice::<Envelope>(&msg.payload) {
            printer.line(&envelope_line(&e, &forged(e.check_subject(&msg.subject).is_ok())))
        } else if let Ok(post) = serde_json::from_slice::<ChannelPost>(&msg.payload) {
            // Before plan posts: a channel post has every field of one.
            printer.line(&channel_line(&post, &forged(post.check_subject(&msg.subject).is_ok())))
        } else if let Ok(post) = serde_json::from_slice::<PlanPost>(&msg.payload) {
            let forged = forged(post.check_subject(&msg.subject).is_ok());
            let (thread, from, left) = (&post.thread_id, &post.from, post.turn_budget);
            printer.line(&format!("[{thread}] {from} -> plan room ({left} turns left){forged}: {}", post.text))
        } else if let Ok(d) = serde_json::from_slice::<DeadLetter>(&msg.payload) {
            printer.line(&format!("dead letter of {} after {} deliveries: {}", d.persona, d.deliveries, d.error))
        } else if let Ok(r) = serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
            printer.line(&request_line(&r, &forged(r.check_subject(&msg.subject).is_ok())))
        } else if let Ok(d) = serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
            let forged = forged(d.subject() == msg.subject.as_str());
            printer.line(&format!("{}/{}{forged}: answered {}", d.persona, d.request_id, approve::spelling(d.verdict)))
        } else {
            printer.line(&format!("{} {}", msg.subject, String::from_utf8_lossy(&msg.payload)))
        };
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(out.as_bytes())?;
        stdout.flush()?;
    }
    Ok(())
}

/// An envelope as `ants watch` prints it: an escalation it carries with its level, reason, stuck persona and thread,
/// leader and the personas visited; an agent's approval decision in place of its text.
fn envelope_line(e: &Envelope, forged: &str) -> String {
    let head = format!("[{}] {} -> {}{forged}", e.thread_id, e.from, e.to);
    if let Some(d) = &e.approval {
        return format!(
            "{head} decides {} on approval request {}/{}",
            approve::spelling(d.verdict),
            d.persona,
            d.request_id
        );
    }
    match &e.escalation {
        Some(x) => format!("{head} [{}]: {}", escalation_note(x), e.text),
        None => format!("{head}: {}", e.text),
    }
}

/// An escalation in one line: `escalation level 1, turn_budget: dev-a stuck on t1, leader lead, visited dev-a > lead`.
fn escalation_note(x: &Escalation) -> String {
    let reason = match x.request_id {
        Some(id) => format!("{} {}/{id}", x.reason.as_str(), x.stuck),
        None => x.reason.as_str().to_owned(),
    };
    let visited = x.visited.iter().map(PersonaId::as_str).collect::<Vec<_>>().join(" > ");
    let (level, stuck, thread, leader) = (x.level, &x.stuck, &x.thread_id, &x.leader);
    format!("escalation level {level}, {reason}: {stuck} stuck on {thread}, leader {leader}, visited {visited}")
}

/// A tool request as `ants watch` prints it.
fn request_line(r: &ApprovalRequest, forged: &str) -> String {
    let options = r.options.iter().map(|v| approve::spelling(*v)).collect::<Vec<_>>().join("|");
    let (thread, persona, id) = (&r.thread_id, &r.persona, r.id);
    format!("[{thread}] {persona}{forged} asks approval {persona}/{id} ({options}): {}: {}", r.tool.kind, r.tool.title)
}

/// A channel post as `ants watch` prints it.
fn channel_line(post: &ChannelPost, forged: &str) -> String {
    let (thread, from, channel, left) = (&post.thread_id, &post.from, &post.channel, post.turn_budget);
    format!("[{thread}] {from} -> #{channel} ({left} turns left){forged}: {}", post.text)
}

/// Renders watched traffic, continuing a persona's streamed text on one line until something else arrives.
#[derive(Default)]
struct Printer {
    /// Whose text the output ends with, unterminated.
    streaming: Option<(PersonaId, ThreadId, &'static str)>,
}

impl Printer {
    /// A whole line, after ending any streamed text.
    fn line(&mut self, line: &str) -> String {
        let end = if self.streaming.take().is_some() { "\n" } else { "" };
        format!("{end}{line}\n")
    }

    fn status(&mut self, event: &StatusEvent, forged: &str) -> String {
        let (who, thread) = (&event.persona, &event.thread_id);
        let (verb, text) = match &event.update {
            StatusUpdate::Message { text } => ("says", text),
            StatusUpdate::Thought { text } => ("thinks", text),
            StatusUpdate::State { state } => {
                return self.line(&format!("[{thread}] {who}{forged}: {}", state.as_str()));
            }
            StatusUpdate::Tool { id, title, tool_kind, status } => {
                let kind = tool_kind.as_deref().map(|k| format!(" ({k})")).unwrap_or_default();
                let status = status.as_deref().map(|s| format!(": {s}")).unwrap_or_default();
                let title = title.as_deref().unwrap_or(id);
                return self.line(&format!("[{thread}] {who}{forged} tool {title}{kind}{status}"));
            }
            StatusUpdate::Plan { steps } => {
                let steps: Vec<String> = steps
                    .iter()
                    .map(|s| {
                        let mark = match s.status.as_str() {
                            "completed" => "x",
                            "in_progress" => ">",
                            _ => " ",
                        };
                        format!("[{mark}] {}", s.content)
                    })
                    .collect();
                return self.line(&format!("[{thread}] {who}{forged} plan: {}", steps.join("; ")));
            }
        };
        let key = (who.clone(), thread.clone(), verb);
        if !forged.is_empty() {
            return self.line(&format!("[{thread}] {who} {verb}{forged}: {text}"));
        }
        if self.streaming.as_ref() == Some(&key) {
            return text.clone();
        }
        let start = self.line(&format!("[{thread}] {who} {verb}: "));
        self.streaming = Some(key);
        format!("{}{text}", start.trim_end_matches('\n'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(s: &str) -> Vec<String> {
        s.split(' ').map(str::to_owned).collect()
    }

    #[test]
    fn leading_at_picks_the_recipient() {
        let (to, text) = recipient(words("@dev-b check the build")).unwrap();
        assert_eq!((to.as_str(), text.as_str()), ("dev-b", "check the build"));
    }

    #[test]
    fn messenger_is_the_default_recipient() {
        let (to, text) = recipient(words("who owns billing?")).unwrap();
        assert_eq!((to, text.as_str()), (PersonaId::messenger(), "who owns billing?"));
    }

    #[test]
    fn an_ask_comes_from_and_belongs_to_the_cli_human() {
        let alice: PersonaId = "alice".parse().unwrap();
        let me = humans::Me { id: alice.clone(), role: ants_proto::Role::Human };
        let ask = ask_envelope(&me, None, words("@dev-b check the build")).unwrap();
        assert_eq!((&ask.from, ask.to.as_str(), ask.owner.as_ref()), (&alice, "dev-b", Some(&alice)));
        let thread: ThreadId = "t1".parse().unwrap();
        let follow_up = ask_envelope(&me, Some(thread.clone()), words("and the docs")).unwrap();
        assert_eq!((follow_up.thread_id, follow_up.owner), (thread, Some(alice)));
    }

    #[test]
    fn an_ask_with_a_persona_s_credentials_has_no_owner() {
        let me = humans::Me { id: "dev-a".parse().unwrap(), role: ants_proto::Role::Persona };
        let ask = ask_envelope(&me, None, words("@dev-b check the build")).unwrap();
        assert_eq!((&ask.from, ask.owner), (&me.id, None));
    }

    #[test]
    fn dead_rows_show_the_message_or_the_bad_payload() {
        let mut ask = Envelope::new(PersonaId::human(), "dev-a".parse().unwrap(), "build it\nand test it");
        ask.thread_id = "t1".parse().unwrap();
        let letter = DeadLetter::of_envelope("dev-a".parse().unwrap(), 3, 5, "harness crashed", ask);
        let row = dead_row(12, &letter);
        assert!(row.starts_with("12\tdev-a\tt1\thuman\t5x\t"), "{row}");
        assert!(row.ends_with("\tharness crashed\tbuild it…"), "{row}");

        let letter = DeadLetter::of_payload("dev-a".parse().unwrap(), 4, 1, "not an envelope", b"{oops");
        assert!(dead_row(13, &letter).ends_with("\tnot an envelope\t{oops"));
    }

    #[test]
    fn watch_follows_a_persona_or_a_subject_filter() {
        let t1: ThreadId = "t1".parse().unwrap();
        assert_eq!(watch_filter("dev-a", None).unwrap(), "ants.status.dev-a.*");
        assert_eq!(watch_filter("dev-a", Some(&t1)).unwrap(), "ants.status.dev-a.t1");
        assert_eq!(watch_filter("ants.plan.>", None).unwrap(), "ants.plan.>");
        assert_eq!(watch_filter(">", None).unwrap(), ">");
        assert!(watch_filter("ants.>", Some(&t1)).is_err());
    }

    #[test]
    fn streamed_text_continues_on_one_line() {
        let (a, t1): (PersonaId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let event = |update| StatusEvent::new(a.clone(), t1.clone(), update);
        let says = |text: &str| event(StatusUpdate::Message { text: text.to_owned() });
        let mut printer = Printer::default();
        let out: String = [
            printer.status(&says("Hel"), ""),
            printer.status(&says("lo"), ""),
            printer.status(&event(StatusUpdate::Thought { text: "hmm".to_owned() }), ""),
            printer.status(
                &event(StatusUpdate::Tool {
                    id: "c1".to_owned(),
                    title: Some("cargo test".to_owned()),
                    tool_kind: Some("execute".to_owned()),
                    status: Some("in_progress".to_owned()),
                }),
                "",
            ),
            printer.status(&says("done"), ""),
            printer.status(&event(StatusUpdate::State { state: ants_proto::State::Idle }), ""),
        ]
        .concat();
        assert_eq!(
            out,
            "[t1] dev-a says: Hello\n[t1] dev-a thinks: hmm\n[t1] dev-a tool cargo test (execute): in_progress\n\
             [t1] dev-a says: done\n[t1] dev-a: idle\n"
        );
    }

    #[test]
    fn watched_channel_posts_name_the_channel_and_turns_left() {
        let mut post = ChannelPost::new("ops".parse().unwrap(), "dev-a".parse().unwrap(), "@dev-b ready?");
        (post.thread_id, post.turn_budget) = ("t1".parse().unwrap(), 3);
        assert_eq!(channel_line(&post, ""), "[t1] dev-a -> #ops (3 turns left): @dev-b ready?");
        let forged = " (forged: sent on ants.chan.ops.dev-b)";
        assert_eq!(channel_line(&post, forged), format!("[t1] dev-a -> #ops (3 turns left){forged}: @dev-b ready?"));
    }

    #[test]
    fn watched_escalations_show_level_reason_leader_and_visited() {
        let p = |s: &str| -> PersonaId { s.parse().unwrap() };
        let t1: ThreadId = "t1".parse().unwrap();
        let start = Escalation::new(p("dev-a"), t1.clone(), ants_proto::EscalationReason::TurnBudget);
        let ants_proto::Hop::Chain { escalation, .. } = start.up(&[p("lead"), p("dev-b")], 3) else { panic!("up") };
        let notice =
            Envelope::on_thread(t1.clone(), p("dev-a"), p("lead"), "[ants] stuck").with_escalation(Some(escalation));
        assert_eq!(
            envelope_line(&notice, ""),
            "[t1] dev-a -> lead [escalation level 1, turn_budget: dev-a stuck on t1, leader lead, \
             visited dev-a > lead > dev-b]: [ants] stuck"
        );

        let tool = ants_proto::ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() };
        let options = vec![ants_proto::Verdict::AllowOnce];
        let request = ApprovalRequest::new(p("dev-a"), t1.clone(), tool, options, std::time::Duration::from_secs(60));
        let id = request.id;
        let ants_proto::Hop::Chain { escalation, .. } = Escalation::of_request(&request).up(&[p("lead")], 3) else {
            panic!("up")
        };
        assert_eq!(
            escalation_note(&escalation),
            format!("escalation level 1, approval dev-a/{id}: dev-a stuck on t1, leader lead, visited dev-a > lead")
        );
        assert_eq!(
            request_line(&request, ""),
            format!("[t1] dev-a asks approval dev-a/{id} (allow-once|reject): execute: git push")
        );
        let decision = ApprovalDecision::new(id, p("dev-a"), ants_proto::Verdict::AllowOnce);
        let decided = Envelope::decision(t1, p("lead"), decision);
        assert_eq!(
            envelope_line(&decided, ""),
            format!("[t1] lead -> dev-a decides allow-once on approval request dev-a/{id}")
        );
    }

    #[test]
    fn a_recipient_alone_is_an_error() {
        assert!(recipient(words("@dev-b")).is_err());
        assert!(recipient(words("@Bad.Name hi")).is_err());
    }
}
