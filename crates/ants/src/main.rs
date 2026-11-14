//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

mod add;
mod auth;
mod bridge;
mod channel;
mod enroll;
mod humans;
mod inbox;
mod leaf;
mod outbox;
mod roster;
mod serve;
mod tools;
mod webhook;

use std::path::{Path, PathBuf};
use std::time::Duration;

use ants_daemon::reading::Reading;
use ants_daemon::{Launch, PermissionPolicy, bus, dead, harness, notes, owners};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, ChannelId, ChannelPost, DeadLetter, Destination, Envelope, Escalation, JsDomain,
    Machine, NotesPost, Permissions, StatusEvent, StatusUpdate, ThreadId, UserId, Verdict, subject,
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

    /// NATS credentials file (`ants auth user` writes one). Needed when the server enforces per-user permissions.
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
    /// Send a message as the CLI's human, who owns the thread. Starts with @ant to pick the recipient; the
    /// messenger gets it otherwise. With --wait or --follow, print the replies to the CLI's human on the thread and
    /// mark them read.
    Ask {
        /// Continue an existing thread. The message still prompts only its recipient (@ant, else the
        /// messenger), not the thread's other ants.
        #[arg(long)]
        thread: Option<ThreadId>,
        #[command(flatten)]
        replies: Replies,
        /// `@ant` (optional), then the message.
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
    /// List the ants being served: harness, machine, capabilities, escalation chain, last heartbeat, and whether it
    /// is recent.
    Roster {
        /// One JSON entry per line, with its state.
        #[arg(long)]
        json: bool,
    },
    /// Post to a channel as the CLI's human. An @mention in the text wakes only that member, if an ant, or puts a
    /// notice in the member's inbox, if a human; a post that @mentions nobody goes to the channel's messenger, which
    /// wakes the members the post concerns. A new thread belongs to the
    /// human who posts it, and the turn budget it runs out of comes back to that human. With --wait, the first post
    /// on the thread by someone other than the poster ends the wait; --follow prints every later post on it.
    Post {
        /// The channel: `ops` or `'#ops'` (quoted, as shells take an unquoted `#` for a comment).
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Post on an existing thread of the channel instead of starting one; the thread keeps its owner.
        #[arg(long)]
        thread: Option<ThreadId>,
        #[command(flatten)]
        replies: Replies,
        /// The post; each @ant mention wakes that member.
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
    /// Channels: named groups of ants that humans post to with `ants post`.
    #[command(subcommand)]
    Channel(ChannelCommand),
    /// Show the notes of a thread, or post TEXT to them as the CLI's human, which gives them a fresh turn budget.
    /// Thread notes are a passive board, one per thread: a post wakes nobody, and agents read the notes with
    /// read_notes. They are distinct from the execution plan an ant reports, which `ants watch <ant>` shows.
    Notes {
        thread: ThreadId,
        /// The post. Without it, show the notes.
        #[arg(value_name = "TEXT")]
        text: Vec<String>,
    },
    /// Print ants traffic as it happens, escalations with their level, leader and ants visited, or the live
    /// progress of an ant: state, reply text, tool calls and execution plan.
    Watch {
        /// Subject filter, or an ant to follow its progress on every thread.
        #[arg(default_value = subject::ALL)]
        subject: String,
        /// With an ant, follow only this thread.
        thread: Option<ThreadId>,
    },
    /// Create the ANTS stream and the roster, sessions, attempts, channels, humans and chains buckets on the server, or
    /// add the stream subjects and direct gets an older version lacks. Needs a human's credentials.
    Init,
    /// Tool requests ants escalate to humans: list the pending ones, show one, approve or reject it. Every human
    /// sees every request; the first decision wins, a human's or that of an agent of the ant's escalation chain.
    #[command(subcommand)]
    Tools(ToolsCommand),
    /// Messages ants gave up on: inspect, replay or drop them.
    #[command(subcommand)]
    Dead(DeadCommand),
    /// Serve ant inboxes through agent harnesses: those of the config file, or one described by flags.
    Daemon(Box<serve::DaemonArgs>),
    /// Add an ant to the machine: list it in the config file and give it credentials. Its user NKey is generated here
    /// and the seed stays here. Without --jwt or --enroll, print the public key for the machine holding the account
    /// key to sign; run again with --jwt or --enroll to complete the credentials. Reload the daemon (SIGHUP) to serve the ant.
    Add(Box<add::AddArgs>),
    /// Enrollment requests (credentials asked for with `ants add --enroll`), on the machine holding the account key:
    /// list them, approve or reject one.
    #[command(subcommand)]
    Enroll(EnrollCommand),
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
    /// Issue NATS credentials: one operator and account for the team, one user per ant or human.
    #[command(subcommand)]
    Auth(AuthCommand),
    /// The machine's local NATS leaf node, which every ants process on it connects to.
    #[command(subcommand)]
    Leaf(LeafCommand),
    /// Carry a leaf node's uplink over plain HTTPS requests, for networks whose proxies block WebSocket.
    #[command(subcommand)]
    Bridge(BridgeCommand),
    /// Take events from outside services over HTTP and deliver each to the ant or channel its webhook names.
    #[command(subcommand)]
    Webhook(WebhookCommand),
}

/// What `ants ask` and `ants post` do once sent: return at once, wait for the first reply, or follow the thread.
#[derive(clap::Args, Debug, Default)]
struct Replies {
    /// Wait for the first reply on the thread, print it and exit; exit with status 1 if none comes within
    /// --timeout.
    #[arg(long, group = "awaiting")]
    wait: bool,
    /// Print every reply on the thread as it arrives, until --timeout or Ctrl-C.
    #[arg(long, short, group = "awaiting")]
    follow: bool,
    /// How long --wait or --follow lasts, such as 90s, 10m or 1h30m. Without it, until a reply (--wait) or Ctrl-C.
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, requires = "awaiting")]
    timeout: Option<Duration>,
}

impl Replies {
    /// Hand the replies on `thread` to `show` as they come: the first one with `--wait`, each one with `--follow`,
    /// until the timeout or, following, Ctrl-C. A `--wait` without a reply in time is an error.
    async fn show<T>(
        &self,
        thread: &ThreadId,
        replies: impl futures::Stream<Item = T>,
        mut show: impl AsyncFnMut(T) -> Result<()>,
    ) -> Result<()> {
        let deadline = async {
            match self.timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => std::future::pending().await,
            }
        };
        // Only a follow catches Ctrl-C, to end cleanly; a wait is left to end at once as interrupted.
        let stop = async {
            if self.follow {
                _ = tokio::signal::ctrl_c().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(replies, deadline, stop);
        let mut shown = 0;
        loop {
            let reply = tokio::select! {
                () = &mut deadline => break,
                () = &mut stop => return Ok(()),
                reply = replies.next() => match reply {
                    Some(reply) => reply,
                    None => bail!("connection closed while waiting for replies on thread {thread}"),
                },
            };
            show(reply).await?;
            shown += 1;
            if self.wait {
                return Ok(());
            }
        }
        let within = self.timeout.map(ants_proto::duration::format).unwrap_or_default();
        if self.wait {
            bail!("no reply on thread {thread} within {within}");
        }
        if shown == 0 {
            eprintln!("no reply on thread {thread} within {within}");
        }
        Ok(())
    }
}

#[derive(Subcommand)]
enum ChannelCommand {
    /// Create a channel; fails when the channel exists.
    Create {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Member, an ant or a human (repeatable, or comma-separated).
        #[arg(long = "member", value_name = "USER", value_delimiter = ',', value_parser = channel::parse_ant)]
        members: Vec<UserId>,
        /// Ant that triages the posts of humans that @mention nobody (default: `messenger`).
        #[arg(long, value_parser = channel::parse_ant)]
        messenger: Option<UserId>,
        /// Ant that owns the channel: it changes the members and the messenger through `ants channel serve`.
        #[arg(long, value_name = "ANT", value_parser = channel::parse_ant)]
        owner: Option<UserId>,
    },
    /// Replace the members or the messenger of a channel; what is not given stays.
    Set {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Member, an ant or a human (repeatable, or comma-separated): together, the new members.
        #[arg(long = "member", value_name = "USER", value_delimiter = ',', value_parser = channel::parse_ant)]
        members: Option<Vec<UserId>>,
        /// Ant that triages the posts of humans that @mention nobody.
        #[arg(long, value_parser = channel::parse_ant, conflicts_with = "default_messenger")]
        messenger: Option<UserId>,
        /// Hand the triage back to the default messenger, the ant `messenger` (triage stays on).
        #[arg(long)]
        default_messenger: bool,
        /// Ant that owns the channel: it changes the members and the messenger through `ants channel serve`.
        #[arg(long, value_name = "ANT", value_parser = channel::parse_ant, conflicts_with = "no_owner")]
        owner: Option<UserId>,
        /// Leave the channel without an owner.
        #[arg(long)]
        no_owner: bool,
    },
    /// Add members, ants or humans, to a channel. An @mention of a human member puts a notice in the human's inbox.
    Add {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Ants or humans: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "USER", value_parser = channel::parse_ant)]
        users: Vec<UserId>,
    },
    /// Remove members from a channel.
    Remove {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Ants or humans: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "USER", value_parser = channel::parse_ant)]
        users: Vec<UserId>,
    },
    /// List the channels with the members, the messenger and the owner of each.
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
    /// Run the channel manager, where no ant runs (on the hub, say), with the credentials of
    /// `ants auth channel-manager` as --creds: it applies the changes owners ask for, refuses the others, and posts
    /// each change in its channel.
    Serve,
}

#[derive(Subcommand)]
enum WebhookCommand {
    /// On the hub, behind the TLS-terminating reverse proxy: take `POST <path>/<webhook>` for each webhook of the
    /// config file and deliver its events, each webhook as itself with its own credentials.
    Serve {
        /// The webhooks' config file (default: ~/.ants/webhooks.toml).
        #[arg(long)]
        config: Option<PathBuf>,
        /// Address for the reverse proxy (default: the file's `listen`, else 127.0.0.1:8424).
        #[arg(long)]
        listen: Option<String>,
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
    /// Issue credentials for an ant, allowing only the subjects of its role, with --human for a human, a CLI user,
    /// or with --webhook for a webhook of `ants webhook serve`. `human` always gets a human's. Give the hub's
    /// --js-domain, if it has one, so they work with it.
    #[command(group = clap::ArgGroup::new("targets").args(["to", "channels"]).multiple(true))]
    User {
        /// The ant, with --human the human, or with --webhook the webhook.
        user: UserId,
        /// Issue a human's credentials, with the same rights as every other human. The human joins `ants_humans`,
        /// the team's humans, on its first CLI connection.
        #[arg(long, conflicts_with = "webhook")]
        human: bool,
        /// Issue a webhook's credentials: it may only message the ants of --to and post to the channels of
        /// --channel, as itself, and record the events it delivered.
        #[arg(long, requires = "targets")]
        webhook: bool,
        /// With --webhook: an ant the webhook may message (repeatable).
        #[arg(long, value_name = "ANT", requires = "webhook")]
        to: Vec<UserId>,
        /// With --webhook: a channel the webhook may post to (repeatable).
        #[arg(long = "channel", value_name = "CHANNEL", value_parser = channel::parse_channel, requires = "webhook")]
        channels: Vec<ChannelId>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/<user>.creds), or with --public-key the JWT (default:
        /// print it).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Sign this public user NKey (U...), from `ants add` on the ant's machine, instead of generating a key:
        /// the JWT, given to `ants add --jwt` there, completes the credentials.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
    },
    /// Issue enrollment credentials for a new ant: they may only ask for the ant's credentials, with
    /// `ants add NAME --enroll FILE` on its machine, and hear the answer `ants enroll approve` or `reject`
    /// gives here. They expire after --ttl.
    Enroll {
        ant: UserId,
        /// How long the enrollment credentials stay valid (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "1h")]
        ttl: std::time::Duration,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the enrollment credentials (default: <dir>/enroll-<ant>.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Issue the credentials of the channel manager, `ants channel serve`: it hears channel management requests and
    /// answers each once, reads the roster and the humans, writes the channels and posts in them.
    ChannelManager {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/channel-manager.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Issue the uplink credentials of a machine's leaf node (leafnodes.remotes[].credentials), allowing what the
    /// ants and humans on the machine may do and usable only by a leaf node connection.
    Leaf {
        /// Machine name, for the credentials' name and file name, and the leaf's JetStream domain: letters, digits,
        /// '-' and '_'.
        machine: Machine,
        /// Ant served on the machine (repeatable). `human` counts as a human.
        #[arg(long = "ant", value_name = "ANT", required_unless_present_any = ["humans", "webhooks"])]
        ants: Vec<UserId>,
        /// Human running the CLI on the machine (repeatable), as issued with `ants auth user HUMAN --human`.
        #[arg(long = "human", value_name = "HUMAN")]
        humans: Vec<UserId>,
        /// Webhook `ants webhook serve` runs on the machine (repeatable): the uplink also allows what its credentials,
        /// <dir>/<WEBHOOK>.creds from `ants auth user WEBHOOK --webhook`, allow.
        #[arg(long = "webhook", value_name = "WEBHOOK")]
        webhooks: Vec<UserId>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/leaf-<machine>.creds).
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ToolsCommand {
    /// List the pending tool requests: `<ant>/<id>`, thread, tool, files, options offered and time left.
    List {
        /// After listing, keep printing new requests until Ctrl-C.
        #[arg(long, short)]
        follow: bool,
        /// One JSON request per line.
        #[arg(long)]
        json: bool,
    },
    /// Show a tool request in full: its tool input, where it stands, and the commands answering it.
    Show {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        id: String,
        /// The request as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Allow the tool call of a pending request, once or with --always for the rest of the ant's session.
    Approve {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        id: String,
        /// Allow this tool for the rest of the session (allow-always), when the request offers it.
        #[arg(long)]
        always: bool,
    },
    /// Reject the tool call of a pending request.
    Reject {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        id: String,
    },
}

#[derive(Subcommand)]
enum EnrollCommand {
    /// List the enrollment requests heard, the latest of each ant and key; machines repeat theirs every few
    /// seconds while they wait.
    List {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Sign the credentials an ant asks for, with the rights `ants auth user` gives, for the hub's --js-domain if
    /// it has one, and send them to the asking machine.
    Approve {
        ant: UserId,
        /// The asking machine's public key, needed when several ask for the ant.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Refuse the credentials an ant asks for, to every machine asking for it or the one of --public-key.
    Reject {
        ant: UserId,
        /// The asking machine's public key.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
        /// Reason shown to the asking machine.
        #[arg(long)]
        reason: Option<String>,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
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
    /// what its ants and humans send while the hub is out of reach, and have the hub's stream take from it what
    /// they may send once the uplink is back. Run it again when the machine's ants or humans change. Fails if
    /// the hub does not hear from the outbox within 10 s, as without `feature_flags { js_api_reply_v2: true }` on the
    /// hub.
    Outbox {
        /// Machine name, as given to `ants leaf config`.
        machine: Machine,
        /// Ant served on the machine (repeatable), as given to `ants auth leaf`. `human` counts as a human.
        #[arg(long = "ant", value_name = "ANT", required_unless_present_any = ["humans", "remove"])]
        ants: Vec<UserId>,
        /// Human running the CLI on the machine (repeatable), as given to `ants auth leaf`.
        #[arg(long = "human", value_name = "HUMAN")]
        humans: Vec<UserId>,
        /// Stop taking anything from the machine's outbox instead.
        #[arg(long, conflicts_with_all = ["ants", "humans"])]
        remove: bool,
    },
}

#[derive(Subcommand)]
enum HarnessCommand {
    /// Start the harness and complete the ACP handshake.
    Check {
        #[command(flatten)]
        pick: HarnessPick,
    },
    /// Run one prompt in the current directory and print the reply. Tool requests are rejected.
    Prompt {
        #[command(flatten)]
        pick: HarnessPick,
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
}

#[derive(clap::Args)]
struct HarnessPick {
    /// Harness name: built in (`copilot`, `claude`, `antigravity`) or defined in the config file (default: the
    /// file's top-level `harness`, else `copilot`).
    #[arg(long)]
    harness: Option<String>,
    /// Config file defining harnesses (default: ~/.ants/config.toml, if it exists).
    #[arg(long, env = "ANTS_CONFIG")]
    config: Option<PathBuf>,
}

impl HarnessPick {
    fn resolve(&self) -> Result<std::sync::Arc<ants_daemon::Harness>> {
        let path = match &self.config {
            Some(path) => path.clone(),
            None => ants_daemon::config::default_path()?,
        };
        ants_daemon::config::Summary::load(&path)?.harness(self.harness.as_deref())
    }
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
        Command::Ask { thread, replies, words } => ask(&nats()?, thread, &replies, words).await,
        Command::Inbox { peek, thread, limit, follow, json } => {
            let select = inbox::Select { thread, limit, peek };
            show_inbox(&nats()?, &select, follow, json).await
        }
        Command::Roster { json } => roster::show(&nats()?, json).await,
        Command::Post { channel, thread, replies, words } => post(&nats()?, channel, thread, &replies, words).await,
        Command::Channel(ChannelCommand::Serve) => channel::serve(&cli.nats, cli.creds.as_deref(), domain).await,
        Command::Channel(command) => channels(&nats()?, command).await,
        Command::Notes { thread, text } => thread_notes(&nats()?, thread, text).await,
        Command::Watch { subject, thread } => {
            let filter = watch_filter(&subject, thread.as_ref())?;
            watch(&nats()?, filter).await
        }
        Command::Init => {
            let nats = nats()?;
            let (_, js) = nats.connect_only("ants-cli").await?;
            bus::init_stream(&js).await?;
            bus::ensure_roster(&js, ants_proto::ROSTER_TTL).await?;
            bus::init_own_buckets(&js).await?;
            bus::ensure_channels(&js).await?;
            bus::ensure_chains(&js).await?;
            bus::ensure_links(&js).await?;
            bus::ensure_owners(&js).await?;
            humans::ensure(&js).await?;
            webhook::ensure_bucket(&js).await?;
            humans::register_or_warn(&js, &nats.me).await;
            println!(
                "stream {} and buckets {}, {}, {}, {}, {}, {}, {}, {} and {} ready",
                ants_proto::STREAM,
                ants_proto::ROSTER_BUCKET,
                ants_proto::SESSIONS_BUCKET,
                ants_proto::ATTEMPTS_BUCKET,
                ants_proto::CHANNELS_BUCKET,
                ants_proto::HUMANS_BUCKET,
                ants_proto::CHAINS_BUCKET,
                ants_proto::LINKS_BUCKET,
                ants_proto::OWNERS_BUCKET,
                ants_proto::WEBHOOKS_BUCKET
            );
            Ok(())
        }
        Command::Tools(ToolsCommand::List { follow, json }) => tools::list(&nats()?, follow, json).await,
        Command::Tools(ToolsCommand::Show { id, json }) => tools::show(&nats()?, &id, json).await,
        Command::Tools(ToolsCommand::Approve { id, always }) => {
            let verdict = if always { Verdict::AllowAlways } else { Verdict::AllowOnce };
            tools::decide_request(&nats()?, &id, verdict).await
        }
        Command::Tools(ToolsCommand::Reject { id }) => tools::decide_request(&nats()?, &id, Verdict::Reject).await,
        Command::Dead(command) => dead_letters(&nats()?, command).await,
        Command::Daemon(args) => {
            let default_path = ants_daemon::config::default_path()?;
            let configs = || {
                let (ants, harnesses) = serve::ants(&args, &default_path)?;
                serve::configs(ants, &harnesses, &cli.nats, cli.creds.clone(), domain.cloned())
            };
            serve::run(configs()?, configs).await
        }
        Command::Mcp { socket, token } => ants_daemon::mcp::bridge_stdio(&socket, &token).await,
        Command::Harness(HarnessCommand::Check { pick }) => {
            let harness = pick.resolve()?;
            println!("{harness}: {}", harness::check(&harness).await?);
            Ok(())
        }
        Command::Harness(HarnessCommand::Prompt { pick, words }) => {
            let launch = Launch::new(
                pick.resolve()?,
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
        Command::Auth(AuthCommand::User { user, human, webhook, to, channels, dir, out, public_key }) => {
            let permissions = match webhook {
                true => {
                    if user.is_human() {
                        bail!("{user} is the default human, not a webhook");
                    }
                    let to = to.into_iter().map(Destination::Ant);
                    let destinations: Vec<Destination> =
                        to.chain(channels.into_iter().map(Destination::Channel)).collect();
                    Permissions::for_webhook(&user, &destinations)
                }
                false => Permissions::for_user(&user, auth::role(&user, human)),
            }
            .with_js_domain(domain);
            let dir = team_dir(dir)?;
            let Some(key) = public_key else {
                let out = out.unwrap_or_else(|| dir.join(format!("{user}.creds")));
                auth::issue_with(&dir, &user, permissions, &out)?;
                println!("{}", out.display());
                return Ok(());
            };
            let jwt = auth::sign_with(&dir, &user, permissions, &key)?;
            match out {
                Some(out) => {
                    std::fs::write(&out, format!("{jwt}\n")).with_context(|| format!("writing {}", out.display()))?;
                    println!("{}", out.display());
                }
                None => println!("{jwt}"),
            }
            Ok(())
        }
        Command::Auth(AuthCommand::Enroll { ant, ttl, dir, out }) => {
            if ant.is_human() {
                bail!("{ant} is the default human, not an ant");
            }
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("{}.creds", ants_proto::enrollment_user(&ant))));
            let expires = auth::issue_enrollment(&dir, &ant, ttl, &out)?;
            let expires = chrono::DateTime::from_timestamp(i64::try_from(expires)?, 0).context("expiry")?;
            println!(
                "{}\nvalid until {expires}; on the ant's machine: ants add {ant} --enroll FILE ...\n\
                 then here: ants enroll approve {ant}",
                out.display()
            );
            Ok(())
        }
        Command::Add(args) => add::add(*args, &cli.nats, domain).await,
        Command::Enroll(command) => {
            let dir = |dir| team_dir(dir);
            match command {
                EnrollCommand::List { dir: d } => {
                    enroll::Approver::connect(&cli.nats, &dir(d)?, enroll::LISTEN).await?.list().await
                }
                EnrollCommand::Approve { ant, public_key, dir: d } => {
                    let dir = dir(d)?;
                    let approver = enroll::Approver::connect(&cli.nats, &dir, enroll::LISTEN).await?;
                    let key = approver.approve(&dir, &ant, public_key.as_deref(), domain).await?;
                    println!("{ant} approved for {key}");
                    Ok(())
                }
                EnrollCommand::Reject { ant, public_key, reason, dir: d } => {
                    let approver = enroll::Approver::connect(&cli.nats, &dir(d)?, enroll::LISTEN).await?;
                    for key in approver.reject(&ant, public_key.as_deref(), reason.as_deref()).await? {
                        println!("{ant} rejected for {key}");
                    }
                    Ok(())
                }
            }
        }
        Command::Auth(AuthCommand::ChannelManager { dir, out }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("{}.creds", UserId::channel_manager())));
            auth::issue_channel_manager(&dir, domain, &out)?;
            println!("{}\nrun: ants --creds {} channel serve", out.display(), out.display());
            Ok(())
        }
        Command::Auth(AuthCommand::Leaf { machine, ants, humans, webhooks, dir, out }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
            auth::issue_leaf(&dir, &machine, &auth::members(&ants, &humans), &webhooks, domain, &out)?;
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
                 once the leaf is up: ants leaf outbox {machine} --ant ... --human ...",
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
        Command::Webhook(WebhookCommand::Serve { config, listen }) => {
            let path = match config {
                Some(path) => path,
                None => webhook::default_path()?,
            };
            let config = webhook::Config::load(&path)?;
            let listen = listen.or_else(|| config.listen.clone()).unwrap_or_else(|| webhook::DEFAULT_LISTEN.to_owned());
            let addr = bridge::listen_addr(&listen, false)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            let nats = config.nats.clone().unwrap_or_else(|| cli.nats.clone());
            let domain = config.js_domain.clone().or_else(|| domain.cloned());
            tracing::info!("webhooks on http://{addr}{}/<webhook>", config.path);
            webhook::serve(config, &nats, domain.as_ref(), listener).await
        }
        Command::Bridge(BridgeCommand::Connect { url, listen, token, headers, proxy }) => {
            let remote = bridge::Remote::new(&url, token.as_deref(), &headers, proxy.as_deref())?;
            let addr = bridge::listen_addr(&listen, true)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            tracing::info!("bridging {addr} to {url}; leaf uplink: nats-leaf://{addr}");
            remote.run(listener).await
        }
        Command::Leaf(LeafCommand::Outbox { machine, ants, humans, remove }) => {
            let nats = nats()?;
            let (_, js) = nats.connect("ants-cli").await?;
            if remove {
                outbox::remove(&js, &machine).await?;
                println!("the hub no longer takes anything from the outbox of {machine}");
            } else {
                let leaf = outbox::connect(&cli.nats, cli.creds.as_deref(), &nats.me.id, &machine).await?;
                outbox::admit(&leaf, &js, &machine, &auth::members(&ants, &humans)).await?;
                outbox::sourced(&js, &machine, outbox::SOURCE_WAIT).await?;
                println!("outbox of {machine} ready; the hub takes from it what its ants and humans send");
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

/// Split a leading `@ant` off the words.
fn recipient(mut words: Vec<String>) -> Result<(UserId, String)> {
    let to = match words.first().and_then(|w| w.strip_prefix('@')) {
        Some(name) => {
            let to = name.parse()?;
            words.remove(0);
            to
        }
        None => UserId::messenger(),
    };
    if words.is_empty() {
        bail!("nothing to send");
    }
    Ok((to, words.join(" ")))
}

async fn ask(nats: &Nats<'_>, thread: Option<ThreadId>, replies: &Replies, words: Vec<String>) -> Result<()> {
    let envelope = ask_envelope(&nats.me, thread, words)?;
    let (client, js) = nats.connect("ants-cli").await?;
    // Subscribe first so a fast reply is not missed. A waiter also reads back what the hub stored while out of its
    // reach.
    let inbox = match replies.wait || replies.follow {
        true => bus::caught_up_inbox(&client, &js, &nats.me.id, nats.me.role).await?,
        false => bus::live_inbox(&client, &nats.me.id).await?,
    };
    if let Some(owner) = &envelope.owner {
        owners::recording(&js, nats.domain, &envelope.thread_id, owner).await;
    }
    if bus::publish(&js, &envelope).await? == bus::Stored::Outbox {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    println!("thread {}", envelope.thread_id);
    if !replies.wait && !replies.follow {
        return Ok(());
    }
    let thread = envelope.thread_id.clone();
    let on_thread = inbox.filter_map(move |msg| {
        let reply = match serde_json::from_slice::<Envelope>(&msg.payload) {
            Ok(reply) => match reply.check_subject(&msg.subject) {
                Ok(()) => (reply.thread_id == thread).then_some(reply),
                Err(e) => {
                    tracing::warn!("ignoring a reply with a forged sender: {e}");
                    None
                }
            },
            Err(_) => None,
        };
        std::future::ready(reply)
    });
    replies
        .show(&envelope.thread_id, on_thread, async |reply: Envelope| {
            println!("{}: {}", reply.from, reply.text);
            // Seen here, so `ants inbox` need not show it again. The other replies stay unread; an ant's inbox
            // consumer is its daemon's.
            if nats.me.role.is_human() {
                match inbox::mark_read(&js, &nats.me.id, &reply).await {
                    Ok(true) => {}
                    Ok(false) => tracing::debug!("reply already read"),
                    Err(e) => tracing::warn!("could not mark the reply read: {e:#}"),
                }
            }
            Ok(())
        })
        .await
}

/// What `ants ask` sends as `me`: a thread `me` owns, as it starts it or continues it, when `me` is a human. A thread
/// an ant starts has no owner: only a human gets a thread's escalations, with a fresh turn budget. A human's owner is
/// recorded for the thread before the message goes out ([`owners::record`]); the first record of a thread stays.
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
    replies: &Replies,
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
    for user in channel::idle_mentions(&post, entry.as_ref()) {
        tracing::warn!("@{user} is no member of #{}: the post neither wakes nor notifies {user}", post.channel);
    }
    // Subscribe first so a fast reply is not missed.
    let room = if replies.wait || replies.follow {
        Some(client.subscribe(subject::chan_room(&post.channel)).await?)
    } else {
        None
    };
    let stored = bus::store_acked(&js, post.subject(), &post, &post.id.to_string()).await?.map(|ack| ack.sequence);
    if stored.is_none() {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    // An ant's daemon notifies the humans its posts mention.
    if nats.me.role.is_human() {
        let notified = channel::notify_mentioned(&js, &post, entry.as_ref(), &humans).await;
        if !notified.is_empty() {
            let names: Vec<&str> = notified.iter().map(UserId::as_str).collect();
            println!("notified {}", names.join(", "));
        }
    }
    println!("thread {}", post.thread_id);
    let Some(room) = room else { return Ok(()) };
    // Waiting is for an answer, so the poster's own posts from elsewhere do not count; following shows them all. What
    // never comes live, as while the leaf's uplink is down, is read back from the hub's stream.
    let posts = channel::caught_up_posts(room, &js, &nats.me, &post, stored);
    let on_thread = channel::thread_posts(posts, &post, replies.wait);
    replies
        .show(&post.thread_id, on_thread, async |reply: ChannelPost| {
            println!("{}: {}", reply.from, reply.text);
            Ok(())
        })
        .await
}

async fn channels(nats: &Nats<'_>, command: ChannelCommand) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let kv = channel::open(&js).await?;
    let humans = humans::load(&js, &nats.me).await;
    match command {
        ChannelCommand::Create { channel, members, messenger, owner } => {
            channel::check_messenger_not_human(messenger.as_ref(), &humans)?;
            channel::check_owner_not_human(owner.as_ref(), &humans)?;
            let mut entry = ants_proto::ChannelEntry::new(channel);
            for member in members {
                entry.add_member(member);
            }
            entry.messenger = messenger;
            entry.owner = owner;
            channel::create(&kv, &entry).await?;
            warn_unserved(&js, entry.members.iter().chain(&entry.messenger).chain(&entry.owner), &humans).await;
            println!("created {}", channel::describe(&entry));
        }
        ChannelCommand::Set { channel, members, messenger, default_messenger, owner, no_owner } => {
            if members.is_none() && messenger.is_none() && !default_messenger && owner.is_none() && !no_owner {
                bail!("nothing to change: give --member, --messenger, --default-messenger, --owner or --no-owner");
            }
            channel::check_messenger_not_human(messenger.as_ref(), &humans)?;
            channel::check_owner_not_human(owner.as_ref(), &humans)?;
            let (entry, ()) = channel::update(&kv, &channel, |entry| {
                if let Some(members) = &members {
                    entry.members.clear();
                    for member in members {
                        entry.add_member(member.clone());
                    }
                }
                if messenger.is_some() || default_messenger {
                    entry.messenger.clone_from(&messenger);
                }
                if owner.is_some() || no_owner {
                    entry.owner.clone_from(&owner);
                }
            })
            .await?;
            warn_unserved(&js, members.iter().flatten().chain(&messenger).chain(&owner), &humans).await;
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::Add { channel, users } => {
            let (entry, added) = channel::update(&kv, &channel, |entry| {
                users.iter().filter(|p| entry.add_member((*p).clone())).cloned().collect::<Vec<_>>()
            })
            .await?;
            for user in users.iter().filter(|p| !added.contains(p)) {
                tracing::warn!("{user} is already a member of #{channel}");
            }
            warn_unserved(&js, &added, &humans).await;
            println!("{}", channel::describe(&entry));
        }
        ChannelCommand::Remove { channel, users } => {
            let (entry, removed) = channel::update(&kv, &channel, |entry| {
                users.iter().filter(|p| entry.remove_member(p)).cloned().collect::<Vec<_>>()
            })
            .await?;
            for user in users.iter().filter(|p| !removed.contains(p)) {
                tracing::warn!("{user} is no member of #{channel}");
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
            let mut threads = channel::threads(channel::read(&js, &nats.me, &channel).await?);
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
        ChannelCommand::Serve => unreachable!("main runs the channel manager"),
    }
    Ok(())
}

/// Warn about members, messengers and owners no daemon serves now, rather than refuse them: one may start later.
/// `humans` need none.
async fn warn_unserved<'a>(
    js: &async_nats::jetstream::Context,
    users: impl IntoIterator<Item = &'a UserId>,
    humans: &ants_proto::Humans,
) {
    let users: Vec<&UserId> = users.into_iter().filter(|u| !humans.contains(u)).collect();
    if users.is_empty() {
        return;
    }
    match roster::list(js).await {
        Ok(roster) => {
            for ant in channel::unserved(users, &roster, humans) {
                tracing::warn!("{ant} is not in the roster: no daemon serves {ant} now");
            }
        }
        Err(e) => tracing::debug!("not checking the roster: {e:#}"),
    }
}

async fn show_inbox(nats: &Nats<'_>, select: &inbox::Select, follow: bool, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's, whose inbox its daemon reads; `ants inbox` reads a human's");
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
                if let Err(e) = dead::watch(&client, &js, me, |_, _| {}).await {
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
            let watch = dead::watch(&client, &js, &nats.me.id, |seq, letter| println!("{}", dead_row(seq, letter)));
            tokio::select! {
                watched = watch => watched?,
                _ = tokio::signal::ctrl_c() => {}
            }
        }
    }
    Ok(())
}

/// One line of `ants dead list`: sequence, ant, thread, sender, deliveries, time, error and the start of the text.
fn dead_row(seq: u64, letter: &DeadLetter) -> String {
    let time = letter.died_at.format("%Y-%m-%d %H:%M:%S");
    let (thread, from, text) = match &letter.envelope {
        Some(e) => (e.thread_id.as_str(), e.from.as_str(), e.text.as_str()),
        None => ("-", "-", letter.payload.as_deref().unwrap_or("")),
    };
    format!(
        "{seq}\t{}\t{thread}\t{from}\t{}x\t{time}\t{}\t{}",
        letter.ant,
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

async fn thread_notes(nats: &Nats<'_>, thread: ThreadId, text: Vec<String>) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    if !text.is_empty() {
        let post = NotesPost::new(thread, nats.me.id.clone(), text.join(" "));
        notes::publish(&js, &post, None).await?;
        println!("posted to the notes of thread {} ({} turns left)", post.thread_id, post.turn_budget);
        return Ok(());
    }
    let read = notes::read(&js, &nats.me.id, Reading::cli(nats.me.role), &thread).await?;
    if read.posts.is_empty() {
        println!("thread {thread} has no notes");
    }
    for post in &read.posts {
        let time = post.sent_at.format("%Y-%m-%d %H:%M:%S");
        println!("{time} {} ({} turns left): {}", post.from, post.turn_budget, post.text);
    }
    Ok(())
}

/// What `ants watch <subject> [thread]` subscribes to: a subject filter as given, or, for an ant (one token), its
/// status subjects.
fn watch_filter(target: &str, thread: Option<&ThreadId>) -> Result<String> {
    match target.parse::<UserId>().ok() {
        Some(ant) => Ok(thread.map_or_else(|| subject::status_of(&ant), |t| subject::status(&ant, t))),
        None if thread.is_some() => bail!("a thread can only follow an ant, not the subject filter {target:?}"),
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
            // Before notes posts: a channel post has every field of one.
            printer.line(&channel_line(&post, &forged(post.check_subject(&msg.subject).is_ok())))
        } else if let Ok(post) = serde_json::from_slice::<NotesPost>(&msg.payload) {
            let forged = forged(post.check_subject(&msg.subject).is_ok());
            let (thread, from, left) = (&post.thread_id, &post.from, post.turn_budget);
            printer.line(&format!("[{thread}] {from} -> thread notes ({left} turns left){forged}: {}", post.text))
        } else if let Ok(d) = serde_json::from_slice::<DeadLetter>(&msg.payload) {
            printer.line(&format!("dead letter of {} after {} deliveries: {}", d.ant, d.deliveries, d.error))
        } else if let Ok(r) = serde_json::from_slice::<ApprovalRequest>(&msg.payload) {
            printer.line(&request_line(&r, &forged(r.check_subject(&msg.subject).is_ok())))
        } else if let Ok(d) = serde_json::from_slice::<ApprovalDecision>(&msg.payload) {
            let forged = forged(d.subject() == msg.subject.as_str());
            printer.line(&format!("{}/{}{forged}: answered {}", d.ant, d.request_id, tools::spelling(d.verdict)))
        } else {
            printer.line(&format!("{} {}", msg.subject, String::from_utf8_lossy(&msg.payload)))
        };
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(out.as_bytes())?;
        stdout.flush()?;
    }
    Ok(())
}

/// An envelope as `ants watch` prints it: an escalation it carries with its level, reason, stuck ant and thread,
/// leader and the ants visited; an agent's approval decision in place of its text.
fn envelope_line(e: &Envelope, forged: &str) -> String {
    let head = format!("[{}] {} -> {}{forged}", e.thread_id, e.from, e.to);
    if let Some(d) = &e.approval {
        return format!("{head} decides {} on approval request {}/{}", tools::spelling(d.verdict), d.ant, d.request_id);
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
    let visited = x.visited.iter().map(UserId::as_str).collect::<Vec<_>>().join(" > ");
    let (level, stuck, thread, leader) = (x.level, &x.stuck, &x.thread_id, &x.leader);
    format!("escalation level {level}, {reason}: {stuck} stuck on {thread}, leader {leader}, visited {visited}")
}

/// A tool request as `ants watch` prints it.
fn request_line(r: &ApprovalRequest, forged: &str) -> String {
    let options = r.options.iter().map(|v| tools::spelling(*v)).collect::<Vec<_>>().join("|");
    let (thread, ant, id) = (&r.thread_id, &r.ant, r.id);
    format!("[{thread}] {ant}{forged} asks approval {ant}/{id} ({options}): {}: {}", r.tool.kind, r.tool.title)
}

/// A channel post as `ants watch` prints it.
fn channel_line(post: &ChannelPost, forged: &str) -> String {
    let (thread, from, channel, left) = (&post.thread_id, &post.from, &post.channel, post.turn_budget);
    let created = post.creator.as_ref().map(|c| format!(", created by {c}")).unwrap_or_default();
    format!("[{thread}] {from} -> #{channel} ({left} turns left{created}){forged}: {}", post.text)
}

/// Renders watched traffic, continuing an ant's streamed text on one line until something else arrives.
#[derive(Default)]
struct Printer {
    /// Whose text the output ends with, unterminated.
    streaming: Option<(UserId, ThreadId, &'static str)>,
}

impl Printer {
    /// A whole line, after ending any streamed text.
    fn line(&mut self, line: &str) -> String {
        let end = if self.streaming.take().is_some() { "\n" } else { "" };
        format!("{end}{line}\n")
    }

    fn status(&mut self, event: &StatusEvent, forged: &str) -> String {
        let (who, thread) = (&event.ant, &event.thread_id);
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
        assert_eq!((to, text.as_str()), (UserId::messenger(), "who owns billing?"));
    }

    #[test]
    fn an_ask_comes_from_and_belongs_to_the_cli_human() {
        let alice: UserId = "alice".parse().unwrap();
        let me = humans::Me { id: alice.clone(), role: ants_proto::Role::Human };
        let ask = ask_envelope(&me, None, words("@dev-b check the build")).unwrap();
        assert_eq!((&ask.from, ask.to.as_str(), ask.owner.as_ref()), (&alice, "dev-b", Some(&alice)));
        let thread: ThreadId = "t1".parse().unwrap();
        let follow_up = ask_envelope(&me, Some(thread.clone()), words("and the docs")).unwrap();
        assert_eq!((follow_up.thread_id, follow_up.owner), (thread, Some(alice)));
    }

    #[test]
    fn an_ask_with_an_ant_s_credentials_has_no_owner() {
        let me = humans::Me { id: "dev-a".parse().unwrap(), role: ants_proto::Role::Ant };
        let ask = ask_envelope(&me, None, words("@dev-b check the build")).unwrap();
        assert_eq!((&ask.from, ask.owner), (&me.id, None));
    }

    #[test]
    fn dead_rows_show_the_message_or_the_bad_payload() {
        let mut ask = Envelope::new(UserId::human(), "dev-a".parse().unwrap(), "build it\nand test it");
        ask.thread_id = "t1".parse().unwrap();
        let letter = DeadLetter::of_envelope("dev-a".parse().unwrap(), 3, 5, "harness crashed", ask);
        let row = dead_row(12, &letter);
        assert!(row.starts_with("12\tdev-a\tt1\thuman\t5x\t"), "{row}");
        assert!(row.ends_with("\tharness crashed\tbuild it…"), "{row}");

        let letter = DeadLetter::of_payload("dev-a".parse().unwrap(), 4, 1, "not an envelope", b"{oops");
        assert!(dead_row(13, &letter).ends_with("\tnot an envelope\t{oops"));
    }

    #[test]
    fn watch_follows_an_ant_or_a_subject_filter() {
        let t1: ThreadId = "t1".parse().unwrap();
        assert_eq!(watch_filter("dev-a", None).unwrap(), "ants.status.dev-a.*");
        assert_eq!(watch_filter("dev-a", Some(&t1)).unwrap(), "ants.status.dev-a.t1");
        assert_eq!(watch_filter("ants.notes.>", None).unwrap(), "ants.notes.>");
        assert_eq!(watch_filter(">", None).unwrap(), ">");
        assert!(watch_filter("ants.>", Some(&t1)).is_err());
    }

    #[test]
    fn streamed_text_continues_on_one_line() {
        let (a, t1): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
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
        let created = post.with_creator(Some("lead".parse().unwrap()));
        assert_eq!(channel_line(&created, ""), "[t1] dev-a -> #ops (3 turns left, created by lead): @dev-b ready?");
    }

    #[test]
    fn watched_escalations_show_level_reason_leader_and_visited() {
        let p = |s: &str| -> UserId { s.parse().unwrap() };
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
    fn notes_shows_without_text_and_posts_with_it() {
        let cli = Cli::try_parse_from(["ants", "notes", "t1", "split", "the", "work"]).unwrap();
        let Command::Notes { thread, text } = cli.command else { panic!("not notes") };
        assert_eq!((thread.as_str(), text), ("t1", vec!["split".to_owned(), "the".to_owned(), "work".to_owned()]));
        let cli = Cli::try_parse_from(["ants", "notes", "t1"]).unwrap();
        assert!(matches!(cli.command, Command::Notes { text, .. } if text.is_empty()));
        assert!(Cli::try_parse_from(["ants", "plan", "t1"]).is_err());
    }

    #[test]
    fn add_is_a_top_level_command() {
        let cli = Cli::try_parse_from(["ants", "add", "dev-a", "--enroll", "enroll-dev-a.creds"]).unwrap();
        assert!(matches!(cli.command, Command::Add(args) if args.name.as_str() == "dev-a"));
        assert!(Cli::try_parse_from(["ants", "ant", "add", "dev-a"]).is_err());
    }

    #[test]
    fn tool_requests_are_listed_shown_approved_and_rejected() {
        let parse = |line: &str| Cli::try_parse_from(line.split(' ')).map(|cli| cli.command);
        assert!(matches!(
            parse("ants tools approve dev-a/019a --always"),
            Ok(Command::Tools(ToolsCommand::Approve { id, always: true })) if id == "dev-a/019a"
        ));
        assert!(matches!(
            parse("ants tools approve 019a"),
            Ok(Command::Tools(ToolsCommand::Approve { id, always: false })) if id == "019a"
        ));
        assert!(
            matches!(parse("ants tools reject 019a"), Ok(Command::Tools(ToolsCommand::Reject { id })) if id == "019a")
        );
        assert!(matches!(
            parse("ants tools show 019a --json"),
            Ok(Command::Tools(ToolsCommand::Show { id, json: true })) if id == "019a"
        ));
        assert!(matches!(
            parse("ants tools list -f"),
            Ok(Command::Tools(ToolsCommand::List { follow: true, json: false }))
        ));
        for line in
            ["ants approve", "ants approve 019a allow-once", "ants tools approve", "ants tools reject 019a --always"]
        {
            assert!(parse(line).is_err(), "{line}");
        }
    }

    #[test]
    fn wait_and_follow_conflict_and_a_timeout_needs_one_of_them() {
        let parse = |args: &str| Cli::try_parse_from(["ants"].into_iter().chain(args.split(' ')));
        let replies = |args: &str| match parse(args).unwrap().command {
            Command::Ask { replies, .. } | Command::Post { replies, .. } => replies,
            _ => panic!("{args}"),
        };
        let r = replies("ask --wait --timeout 1h30m hi");
        assert_eq!((r.wait, r.follow, r.timeout), (true, false, Some(Duration::from_secs(5400))));
        let r = replies("post -f ops hi");
        assert_eq!((r.wait, r.follow, r.timeout), (false, true, None));
        for args in
            ["ask --wait --follow hi", "post --wait -f ops hi", "ask --timeout 10m hi", "post --timeout 10m ops hi"]
        {
            assert!(parse(args).is_err(), "{args}");
        }
        let bare = parse("ask --wait --timeout 600 hi").err().unwrap().to_string();
        assert!(bare.contains("without a unit"), "{bare}");
    }

    #[test]
    fn the_default_messenger_conflicts_with_a_named_one() {
        let parse = |args: &[&str]| Cli::try_parse_from(["ants", "channel", "set", "ops"].iter().chain(args));
        let Command::Channel(ChannelCommand::Set { default_messenger, messenger, .. }) =
            parse(&["--default-messenger"]).unwrap().command
        else {
            panic!("channel set")
        };
        assert!(default_messenger && messenger.is_none());
        assert!(parse(&["--default-messenger", "--messenger", "triage"]).is_err());
        assert!(parse(&["--no-messenger"]).is_err());
    }

    #[test]
    fn an_owner_is_set_or_cleared_and_never_the_channel_manager() {
        let parse = |args: &[&str]| Cli::try_parse_from(["ants", "channel", "set", "ops"].iter().chain(args));
        let Command::Channel(ChannelCommand::Set { owner, no_owner, .. }) =
            parse(&["--owner", "@lead"]).unwrap().command
        else {
            panic!("channel set")
        };
        assert_eq!((owner, no_owner), (Some("lead".parse().unwrap()), false));
        assert!(parse(&["--owner", "lead", "--no-owner"]).is_err());
        assert!(parse(&["--owner", "channel-manager"]).is_err());
    }

    fn thread() -> ThreadId {
        "t1".parse().unwrap()
    }

    async fn shown(
        replies: Replies,
        posts: impl futures::Stream<Item = &'static str>,
    ) -> (Result<()>, Vec<&'static str>) {
        let mut seen = Vec::new();
        let result = replies
            .show(&thread(), posts, async |p| {
                seen.push(p);
                Ok(())
            })
            .await;
        (result, seen)
    }

    #[tokio::test]
    async fn waiting_ends_at_the_first_reply_or_fails_at_the_timeout() {
        let timeout = Some(Duration::from_millis(50));
        let wait = || Replies { wait: true, timeout, ..Default::default() };
        let later = futures::stream::iter(["one", "two"]).chain(futures::stream::pending());
        let (result, seen) = shown(wait(), later).await;
        assert_eq!((result.is_ok(), seen), (true, vec!["one"]));

        let (result, seen) = shown(wait(), futures::stream::pending()).await;
        assert_eq!(result.unwrap_err().to_string(), "no reply on thread t1 within 0");
        assert!(seen.is_empty());

        let follow = Replies { follow: true, timeout, ..Default::default() };
        let later = futures::stream::iter(["one", "two"]).chain(futures::stream::pending());
        let (result, seen) = shown(follow, later).await;
        assert_eq!((result.is_ok(), seen), (true, vec!["one", "two"]), "following ends at the timeout, cleanly");
    }

    #[test]
    fn a_recipient_alone_is_an_error() {
        assert!(recipient(words("@dev-b")).is_err());
        assert!(recipient(words("@Bad.Name hi")).is_err());
    }
}
