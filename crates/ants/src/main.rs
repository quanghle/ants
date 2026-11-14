//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

mod add;
mod auth;
mod bridge;
mod channel;
mod completions;
mod defaults;
mod doctor;
mod enroll;
mod hook;
mod humans;
mod inbox;
mod keys;
mod leaf;
mod manager;
mod members;
mod notify;
mod outbox;
mod presence;
mod progress;
mod reload;
mod renew;
mod retire;
mod revoke;
mod roster;
mod scheduler;
mod serve;
mod service;
mod setup;
mod signer;
mod spend;
mod thread;
mod thread_meta;
mod tools;
mod upgrade;
mod versions;
mod webhook;
mod webhook_add;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_daemon::reading::Reading;
use ants_daemon::{Launch, PermissionPolicy, bus, dead, harness, hub, notes, owners, reactions};
use ants_proto::{
    ApprovalDecision, ApprovalRequest, CancelReply, CancelRequest, ChannelId, ChannelPost, DeadLetter, Destination,
    Envelope, Escalation, Following, JsDomain, Machine, NotesPost, Permissions, Reaction, StatusEvent, StatusUpdate,
    ThreadId, UserId, Uuid, Verdict, subject,
};
use anyhow::{Context as _, Result, bail};
use clap::{Parser, Subcommand};
use clap_complete::engine::ArgValueCandidates;
use futures::StreamExt as _;

#[derive(Parser)]
#[command(name = "ants", version, about = "Agent teams messaging over NATS")]
struct Cli {
    /// NATS server: nats://, tls://, ws:// or wss://. Point it at the local leaf node where there is one.
    /// Unset, the config file's `[cli]` or top-level `nats` applies, except to `ants daemon` and `ants add`.
    #[arg(long, global = true, env = "ANTS_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats: String,

    /// NATS credentials file (`ants auth user` writes one). Needed when the server enforces per-user permissions.
    /// The CLI acts as the human they name, and registers it in `ants_humans` on connecting; without them, as `human`.
    /// Unset, the config file's `[cli]` `creds` apply, except to `ants daemon` and `ants add`.
    /// A human's also bring the hub's stream and buckets up to date on connecting, as `ants init` does.
    #[arg(long, global = true, env = "ANTS_CREDS")]
    creds: Option<PathBuf>,

    /// JetStream domain of the hub (its `jetstream { domain }`), needed to reach JetStream from a leaf node without
    /// JetStream. Set it for every ants process, and when issuing credentials with `ants auth`.
    /// Unset, the config file's `[cli]` or top-level `js_domain` applies, except to `ants daemon` and `ants add`.
    #[arg(long, global = true, env = "ANTS_JS_DOMAIN")]
    js_domain: Option<JsDomain>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Send a message as the CLI's human, who owns the thread. Starts with @ant to pick the recipient; the
    /// messenger gets it otherwise. With --wait or --follow, print the replies to the CLI's human on the thread and
    /// mark them read.
    Ask {
        /// Continue an existing thread: its id, a unique start of one, or `last`. The message still prompts only
        /// its recipient (@ant, else the messenger), not the thread's other ants.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        #[command(flatten)]
        replies: Replies,
        /// `@ant` (optional), then the message.
        #[arg(required = true, num_args = 1.., add = ArgValueCandidates::new(completions::mentions))]
        words: Vec<String>,
    },
    /// Print the replies to the CLI's human not read yet, including those that arrived while no CLI was running, and
    /// mark them read.
    Inbox {
        /// Show without marking read.
        #[arg(long)]
        peek: bool,
        /// Only replies on this thread (its id, a unique start of one, or `last`); the others stay unread.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        /// At most this many replies; the rest stay unread.
        #[arg(long)]
        limit: Option<usize>,
        /// Keep waiting for new replies until Ctrl-C, meanwhile dead-lettering the messages JetStream stops
        /// delivering without the daemon having stored them as dead letters (as `ants dead watch` does). Without
        /// --thread and --json, also print the pending tool requests, new ones and how each ends.
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
    /// Report the tokens and cost ants spent, as their daemons keep them: per ant, today and over the last --days UTC
    /// days; for one ANT, each day and each thread since a human's last message on it; with --thread, that thread.
    /// Caps come from the local config file for the ants it lists. Needs human credentials.
    Spend {
        /// Only this ant.
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// What the ant, or each ant on the thread, spent on it since a human's last message there: its id, a unique
        /// start of one, or `last`.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        /// UTC days to report, today included.
        #[arg(long, default_value_t = spend::DEFAULT_DAYS, value_parser = clap::value_parser!(u32).range(1..=366),
              conflicts_with = "thread")]
        days: u32,
        /// One JSON object per line: per ant, or with --thread per ant on the thread.
        #[arg(long)]
        json: bool,
        /// Config file with the ants' caps (default: ~/.ants/config.toml, if it exists).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Post to a channel as the CLI's human. An @mention in the text wakes only that member, if an ant, or puts a
    /// notice in the member's inbox, if a human; a post that @mentions nobody goes to the channel's messenger, which
    /// wakes the members the post concerns. A new thread belongs to the
    /// human who posts it, and the turn budget it runs out of comes back to that human. With --wait, the first post
    /// on the thread by someone other than the poster ends the wait; --follow prints every later post on it.
    Post {
        /// The channel: `ops` or `'#ops'` (quoted, as shells take an unquoted `#` for a comment).
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Post on an existing thread of the channel instead of starting one (its id, a unique start of one, or
        /// `last`); the thread keeps its owner.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        #[command(flatten)]
        replies: Replies,
        /// The post; each @ant mention wakes that member.
        #[arg(required = true, num_args = 1.., add = ArgValueCandidates::new(completions::mentions))]
        words: Vec<String>,
    },
    /// Channels: named groups of ants that humans post to with `ants post`.
    #[command(subcommand)]
    Channel(ChannelCommand),
    /// Show the notes of a thread, or post TEXT to them as the CLI's human, which gives them a fresh turn budget.
    /// Thread notes are a passive board, one per thread: a post wakes nobody, and agents read the notes with
    /// read_notes. They are distinct from the execution plan an ant reports, which `ants watch <ant>` shows.
    Notes {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        /// The post. Without it, show the notes.
        #[arg(value_name = "TEXT")]
        text: Vec<String>,
    },
    /// React to a message as the CLI's user, such as `eyes` for seen or `check` for done, or remove the reaction with
    /// --remove. A reaction is no message: it wakes nobody, marks nothing read and ends no wait for a reply. Humans see
    /// reactions beneath the messages in `ants inbox`, `ants thread show` and `ants watch`. Message ids come from
    /// `ants ask`, `ants post`, `ants inbox` and `ants thread show`.
    React {
        /// The message's id.
        message: Uuid,
        /// The reaction: 1-64 of lowercase letters, digits and '-', such as eyes, check, thumbsup, x, heart or rocket.
        #[arg(add = ArgValueCandidates::new(completions::reactions))]
        reaction: Reaction,
        /// Remove the reaction instead of adding it.
        #[arg(long)]
        remove: bool,
    },
    /// Threads read back from the stream across inboxes (both ways), channel posts, notes, escalations, tool requests
    /// and decisions, and dead letters: list the recent ones, find those mentioning a text, or show one's whole
    /// history; title, pin or archive one. Nothing is consumed or marked read. Needs a human's credentials.
    #[command(subcommand)]
    Thread(ThreadCommand),
    /// Print ants traffic as it happens, escalations with their level, leader and ants visited, or the live
    /// progress of an ant: state, reply text, tool calls and execution plan.
    Watch {
        /// Subject filter, or an ant to follow its progress on every thread.
        #[arg(default_value = subject::ALL, add = ArgValueCandidates::new(completions::ants))]
        subject: String,
        /// With an ant, follow only this thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
    },
    /// Stop the prompt ANT is running on THREAD, as a prompt limit does: its daemon cancels the prompt and replies on
    /// the thread with the partial output and an [ants] notice, and the message counts as answered. Messages waiting
    /// behind it on the thread still run. Needs a human's credentials.
    Cancel {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Create the ANTS stream and the roster, sessions, attempts, channels, humans, chains, links, owners, webhooks,
    /// reactions, threads, read and presence buckets on the server, or upgrade those an older version set up (stream subjects, direct gets, the
    /// republishing of inbox messages, the stream's and the owners bucket's duplicate windows), keeping the rest of
    /// their settings. Needs a human's credentials. Every other command run with them does the same on connecting.
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
    /// key to sign; run again with --jwt or --enroll to complete the credentials. Completed credentials reload the
    /// daemon service of the config file to serve the ant, when it runs (scripts/install-units.sh).
    Add(Box<add::AddArgs>),
    /// Retire an ant added from a template (by a manager's `request_ant`): unlist it from the config file, so the
    /// daemon stops it as on any reload, its prompts in flight ending first; wait up to 5m for that; revoke its
    /// credentials, when the account key is in --dir, and push the account; remove its credentials file. Its home
    /// stays: a later `request_ant` of the same name from the same template brings it back with its memory, task
    /// database and worktrees. Other ants are refused: edit the config file and run `ants auth revoke NAME` instead.
    Retire(Box<retire::RetireArgs>),
    /// Enrollment requests (credentials asked for with `ants add --enroll`), on the machine holding the account key:
    /// list them, approve or reject one; and renewal requests (`ants renew`): list them, renew one or all.
    #[command(subcommand)]
    Enroll(EnrollCommand),
    /// On the machine holding the account key, sign the credentials of the ants manager ants ask for (their
    /// `request_ant` tool), until stopped: at once for a request within the policy of the [[ant_template]] tables of
    /// --config (the template lists the manager, has room under its max_ants among the live keys the team issued,
    /// and bounds the request), on a human's approval of a tool request of the manager otherwise (`ants tools`, ntfy,
    /// Slack, Telegram). Names taken, ants added by added ants and forged requests are refused.
    Signer {
        /// Config file whose [[ant_template]] tables are the policy; the rest of it is ignored
        /// (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
        /// How long a human has to decide on a request outside the policy, at most until the manager stops waiting.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "15m")]
        approval_timeout: Duration,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
    /// Renew credentials over NATS: generate a new user NKey here, ask for the credentials of the file renewed for it,
    /// signed with their key, and wait for `ants enroll renew NAME` on the machine holding the account key. Then
    /// replace the file and reload the daemon serving an ant with it, or restart the leaf node whose uplink it is.
    /// Credentials expired, or older than renewal over NATS, are renewed with `ants auth` instead.
    Renew(renew::RenewArgs),
    /// Keep the NKey seeds of credentials files in the OS credential store (macOS keychain, Linux Secret Service,
    /// Windows Credential Manager) instead of the files, move them back, or remove credentials from the machine.
    #[command(subcommand)]
    Keys(keys::KeysCommand),
    /// Bridge stdio to a daemon's MCP tools, for harnesses without HTTP MCP. The daemon configures this itself.
    Mcp {
        /// The daemon's MCP socket (a named pipe on Windows).
        #[arg(long)]
        socket: PathBuf,
        /// The session's token.
        #[arg(long, env = ants_daemon::mcp::TOKEN_ENV, hide_env_values = true)]
        token: String,
    },
    /// Talk to a harness directly, without NATS.
    #[command(subcommand)]
    Harness(HarnessCommand),
    /// Check the machine's ants setup end to end: the config file, credentials, the server and JetStream, the hub's
    /// stream and buckets, each ant's harness, whether the hub keeps pushed accounts across restarts (with the team
    /// keys), the leaf node and its outbox, the installed services, the webhooks' config file and its secrets, and the
    /// Dev Tunnel and HTTPS bridge services installed. Prints one line per check (ok, warn, fail or skip) with the
    /// command fixing each warning or failure, and exits with status 1 when one fails. Changes nothing.
    Doctor {
        /// Config file of the machine's ants (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
        /// Leaf node configuration (default: the one `ants leaf config` writes, ~/.config/ants/leaf.conf).
        #[arg(long)]
        leaf_config: Option<PathBuf>,
        /// Skip starting each ant's harness for an ACP handshake, which may take up to a minute.
        #[arg(long)]
        no_harness: bool,
        /// Print one JSON object per check.
        #[arg(long)]
        json: bool,
        /// Team directory of `ants auth` (default: ~/.config/ants/auth); where it holds the team keys, the hub is asked
        /// whether it keeps pushed accounts across restarts.
        #[arg(long = "dir", env = "ANTS_AUTH_DIR")]
        team: Option<PathBuf>,
        /// Config file of `ants webhook serve` (default: ~/.ants/webhooks.toml), checked when it is there.
        #[arg(long)]
        webhook_config: Option<PathBuf>,
    },
    /// Bring up ants on this machine, each step skipped when already done: write a local nats-server configuration
    /// with JetStream (~/.config/ants/nats.conf) and install its service, run `ants init`, write the config file with
    /// the messenger and the ant of --ant, check the harness, install the daemon service and the shell completions,
    /// then run `ants doctor`. Never overwrites a file; one already there is reported and kept. Exits with status 1
    /// when a step or a check fails.
    Setup(Box<setup::SetupArgs>),
    /// Print the script registering shell completions of ants for SHELL, or write it where the shell loads it from.
    /// Besides commands and flags, it completes ants, channels, recent threads, pending tool requests and dead
    /// letters, looked up over NATS as the command line being completed would connect, for up to 2 seconds.
    Completions {
        /// The shell (default with --install: the one $SHELL names).
        #[arg(required_unless_present = "install")]
        shell: Option<completions::Shell>,
        /// Write the script where the shell loads it from: bash-completion's directory
        /// (~/.local/share/bash-completion/completions), zsh's site-functions in ~/.local/share/zsh, which fpath
        /// needs to name, or ~/.config/fish/completions.
        #[arg(long)]
        install: bool,
    },
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
    /// Run commands on bus events, with observer credentials (`ants auth user NAME --observer`).
    #[command(subcommand)]
    Hook(HookCommand),
}

#[derive(Subcommand, Debug)]
enum HookCommand {
    /// Run CMD once per event of one kind, with the event's JSON document on its stdin, until stopped. Stored
    /// events (tool requests, verdicts, dead letters) are read through the credentials' own consumer, from where it
    /// left off, and acked once CMD ends, whatever its exit; live ones (escalations, status, roster) from now on.
    /// Failures and timeouts are logged.
    Run {
        /// The kind of event.
        #[arg(long, value_enum)]
        on: hook::Event,
        /// Most runs of CMD at once.
        #[arg(long, default_value_t = 4, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
        concurrency: usize,
        /// How long one run of CMD may take before it is killed (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "60s")]
        timeout: Duration,
        /// The command and its arguments.
        #[arg(last = true, required = true, value_name = "CMD")]
        command: Vec<String>,
    },
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
            progress::clear();
            eprintln!("no reply on thread {thread} within {within}");
        }
        Ok(())
    }
}

#[derive(Subcommand, Debug)]
enum ChannelCommand {
    /// Create a channel; fails when the channel exists.
    Create {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        channel: ChannelId,
        /// Member, an ant or a human (repeatable, or comma-separated).
        #[arg(long = "member", value_name = "USER", value_delimiter = ',', value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        members: Vec<UserId>,
        /// Ant that triages the posts of humans that @mention nobody (default: `messenger`).
        #[arg(long, value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        messenger: Option<UserId>,
        /// Ant that owns the channel: it changes the members and the messenger through `ants channel serve`.
        #[arg(long, value_name = "ANT", value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        owner: Option<UserId>,
    },
    /// Replace the members or the messenger of a channel; what is not given stays.
    Set {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Member, an ant or a human (repeatable, or comma-separated): together, the new members.
        #[arg(long = "member", value_name = "USER", value_delimiter = ',', value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        members: Option<Vec<UserId>>,
        /// Ant that triages the posts of humans that @mention nobody.
        #[arg(long, value_parser = channel::parse_ant, conflicts_with = "default_messenger", add = ArgValueCandidates::new(completions::ants))]
        messenger: Option<UserId>,
        /// Hand the triage back to the default messenger, the ant `messenger` (triage stays on).
        #[arg(long)]
        default_messenger: bool,
        /// Ant that owns the channel: it changes the members and the messenger through `ants channel serve`.
        #[arg(long, value_name = "ANT", value_parser = channel::parse_ant, conflicts_with = "no_owner", add = ArgValueCandidates::new(completions::ants))]
        owner: Option<UserId>,
        /// Leave the channel without an owner.
        #[arg(long)]
        no_owner: bool,
    },
    /// Add members, ants or humans, to a channel. An @mention of a human member puts a notice in the human's inbox.
    Add {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Ants or humans: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "USER", value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        users: Vec<UserId>,
    },
    /// Remove members from a channel.
    Remove {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Ants or humans: `dev-a` or `@dev-a`.
        #[arg(required = true, num_args = 1.., value_name = "USER", value_parser = channel::parse_ant, add = ArgValueCandidates::new(completions::ants))]
        users: Vec<UserId>,
    },
    /// List the channels with the members, the messenger and the owner of each.
    List,
    /// Print the posts of a channel kept in the stream, by thread: the post that started each thread, then the
    /// replies on the thread, indented.
    Read {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
        /// Only this thread: its id, a unique start of one, or `last`.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        /// Only the last this many threads.
        #[arg(long, conflicts_with = "thread")]
        limit: Option<usize>,
    },
    /// Delete a channel. The posts to the channel stay in the stream.
    Delete {
        /// The channel: `ops` or `'#ops'`.
        #[arg(value_parser = channel::parse_channel)]
        #[arg(add = ArgValueCandidates::new(completions::channels))]
        channel: ChannelId,
    },
    /// Run the channel manager, where no ant runs (on the hub, say), with the credentials of
    /// `ants auth channel-manager` as --creds: it applies the changes owners ask for, refuses the others, and posts
    /// each change in its channel.
    Serve {
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
}

#[derive(Subcommand, Debug)]
enum WebhookCommand {
    /// On the hub, behind the TLS-terminating reverse proxy: take `POST <path>/<webhook>` for each webhook of the
    /// config file and deliver its events, each webhook as itself with its own credentials. Also push the tool
    /// requests and escalations of each `[[human]]` to its ntfy, Slack, Telegram, command (exec) or signed HTTP
    /// endpoint (http) sinks, with approve and reject links answered at `<path>/_approval`.
    Serve {
        /// The webhooks' config file (default: ~/.ants/webhooks.toml).
        #[arg(long)]
        config: Option<PathBuf>,
        /// Address for the reverse proxy (default: the file's `listen`, else 127.0.0.1:8424).
        #[arg(long)]
        listen: Option<String>,
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: the file's `metrics`, else off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
    /// Send a test notice through each sink (ntfy, slack, telegram, exec, http) of HUMAN in the config file, those of
    /// a device too, and print how each went; exits with status 1 when one fails. Needs no NATS.
    NotifyTest {
        /// A `[[human]]` of the config file.
        human: UserId,
        /// The webhooks' config file (default: ~/.ants/webhooks.toml).
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// List a new webhook in the config file in one step: issue its credentials (or ask for them over NATS with
    /// --enroll), write a new secret to <name>.secret beside the config file (mode 0600), add its [[webhook]] entry,
    /// reload the webhook service, and print the URL and secret to give the sender.
    Add(Box<webhook_add::AddArgs>),
}

#[derive(Subcommand, Debug)]
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
        /// File holding the bearer token, read at start, instead of --token or ANTS_BRIDGE_TOKEN: keeps it out of the
        /// command line and environment.
        #[arg(long, value_name = "FILE", conflicts_with = "token")]
        token_file: Option<PathBuf>,
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
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
        /// File holding the bearer token, read at start, instead of --token or ANTS_BRIDGE_TOKEN: keeps it out of the
        /// command line and environment.
        #[arg(long, value_name = "FILE", conflicts_with = "token")]
        token_file: Option<PathBuf>,
        /// Extra request header, `Name: value` (repeatable), for a gateway in front of the hub.
        #[arg(long = "header", value_name = "HEADER")]
        headers: Vec<String>,
        /// HTTP CONNECT forward proxy, http://[user:password@]host:port.
        #[arg(long, value_name = "URL")]
        proxy: Option<String>,
        /// Serve GET /healthz and GET /metrics (Prometheus text format) on this address, such as 127.0.0.1:9464
        /// (default: off).
        #[arg(long, value_name = "ADDR")]
        metrics: Option<SocketAddr>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
}

#[derive(Subcommand, Debug)]
enum ThreadCommand {
    /// List the threads with messages stored within --since, one row each: id, owner, users and channels, first and
    /// last time, messages, pinned or archived and title when any is, and the first line of the first message; the
    /// most recently active last, and the pinned after the others. Archived threads are left out, but for those with
    /// a message stored since they were archived (`archived+new`).
    List {
        /// Only threads this ant (or human) sent or received a message on.
        #[arg(long, value_name = "ANT", add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// Only threads this human owns.
        #[arg(long, value_name = "HUMAN")]
        owner: Option<UserId>,
        /// Only threads with a message whose text contains TEXT, whatever the case; its first matching line is shown
        /// under the thread.
        #[arg(long, value_name = "TEXT")]
        grep: Option<String>,
        /// Only threads whose escalation reached a human with no human's message on the thread since, other than a
        /// tool request's; the escalation is shown under the thread. One sent before --since is not seen.
        #[arg(long)]
        escalated: bool,
        /// Only the threads pinned (`ants thread pin`), by default read from the earliest one's start.
        #[arg(long, conflicts_with = "archived")]
        pinned: bool,
        /// Only the threads archived (`ants thread archive`), by default read from the earliest one's start.
        #[arg(long)]
        archived: bool,
        /// How far back to read, such as 90m, 24h or 7d; 0 for everything the stream keeps. Each message in it is
        /// read, so a longer one takes longer. By default 24h.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
        since: Option<Duration>,
        /// At most this many threads, the most recently active.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// One JSON thread per line.
        #[arg(long)]
        json: bool,
    },
    /// Show a thread's whole history in the order the stream stored it: kind, time, sender and recipient, and text.
    Show {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        /// How far back to read; by default from the thread's start when its id is time-ordered (as generated ids
        /// are), else everything the stream keeps.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse)]
        since: Option<Duration>,
        /// One JSON entry per line.
        #[arg(long)]
        json: bool,
    },
    /// Print a thread's title, or with TEXT title it, for every human: any human titles or retitles any thread.
    Title {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        /// The title: one line of up to 120 characters.
        #[arg(conflicts_with = "clear")]
        text: Vec<String>,
        /// Remove the title.
        #[arg(long)]
        clear: bool,
    },
    /// Pin a thread, unarchiving it, for every device of the human: `ants thread list` lists it after the others.
    Pin {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Unpin a thread.
    Unpin {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Archive a thread, unpinning it, for every device of the human: `ants thread list` leaves it out until a
    /// message is stored on it. Its messages stay, and its work goes on.
    Archive {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
    /// Unarchive a thread.
    Unarchive {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
    },
}

#[derive(Subcommand, Debug)]
enum DeadCommand {
    /// List dead letters, oldest first.
    List,
    /// Send a dead letter's message to its inbox again, as it was, and remove the dead letter.
    Replay {
        #[arg(add = ArgValueCandidates::new(completions::dead_letters))]
        seq: u64,
    },
    /// Remove a dead letter without replaying it.
    Drop {
        #[arg(add = ArgValueCandidates::new(completions::dead_letters))]
        seq: u64,
    },
    /// Keep dead-lettering, until Ctrl-C, the messages JetStream stops delivering without the daemon having stored
    /// them as dead letters, and print each one. `ants inbox --follow` does the same while it runs.
    Watch,
}

#[derive(Subcommand, Debug)]
enum AuthCommand {
    /// Create the team operator and account, and the server.conf that makes nats-server trust them.
    Init {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Issue credentials for an ant, allowing only the subjects of its role, with --human for a human, a CLI user,
    /// with --webhook for a webhook of `ants webhook serve`, or with --observer for a hook observing the team's
    /// events. `human` always gets a human's. Give the hub's --js-domain, if it has one, so they work with it.
    #[command(group = clap::ArgGroup::new("targets").args(["to", "channels"]).multiple(true))]
    User {
        /// The ant, with --human the human, with --webhook the webhook, or with --observer the observer.
        user: UserId,
        /// Issue a human's credentials, with the same rights as every other human. The human joins `ants_humans`,
        /// the team's humans, on its first CLI connection.
        #[arg(long, conflicts_with = "webhook")]
        human: bool,
        /// Issue an observer's read-only credentials, for hooks (`ants hook run`, docs/hooks.md): it reads approval
        /// requests and decisions, dead letters, every ant's status (escalations among it) and the roster, and
        /// publishes nothing.
        #[arg(long, conflicts_with_all = ["human", "webhook", "label"])]
        observer: bool,
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
        /// With --human (or for `human`): a label for the device the credentials are for, such as "Quang's laptop",
        /// shown by `ants auth devices`. Each human's credentials are for a new device with a generated id.
        #[arg(long, conflicts_with = "webhook")]
        label: Option<String>,
        /// Keep the NKey seed in the OS credential store (macOS keychain, Linux Secret Service, Windows Credential
        /// Manager), never in a file, for credentials used on this machine: the file names the stored key, written
        /// only once the seed reads back and authenticates at --nats, unless no server answers there.
        #[arg(long, conflicts_with = "public_key")]
        store_key: bool,
    },
    /// Issue enrollment credentials for a new ant: they may only ask for the ant's credentials, with
    /// `ants add NAME --enroll FILE` on its machine, and hear the answer `ants enroll approve` or `reject`
    /// gives here. They expire after --ttl, or never with --ttl 0.
    Enroll {
        ant: UserId,
        /// How long the enrollment credentials stay valid (a duration such as 90s, 10m or 1h; 0 for no expiry).
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
        /// Keep the NKey seed in the OS credential store, never in a file, as `ants auth user --store-key` does.
        #[arg(long)]
        store_key: bool,
    },
    /// Issue the uplink credentials of a machine's leaf node (leafnodes.remotes[].credentials), allowing what the
    /// ants and humans on the machine may do and usable only by a leaf node connection, and the system account
    /// credentials of its second uplink, which only hears the account updates `ants auth push` sends the hub.
    /// Without --ant, --human or --webhook, it reissues them for the members of the last run, changed by --add-* and
    /// --remove-*.
    Leaf {
        /// Machine name, for the credentials' name and file name, and the leaf's JetStream domain: letters, digits,
        /// '-' and '_'.
        machine: Machine,
        #[command(flatten)]
        members: members::UplinkArgs,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        /// Where to write the credentials (default: <dir>/leaf-<machine>.creds); the system account credentials of
        /// the leaf's second uplink go beside them, as <stem>.system.creds.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Revoke credentials before they expire: every key issued to NAME (an ant, human, webhook or observer,
    /// `leaf-<machine>`, `channel-manager` or `enroll-<ant>`) and each --key. The team's account in server.conf
    /// refuses them from then on, and the account is pushed to the server of --nats, which closes their connections,
    /// as do the leaf nodes connected to it through a system account uplink.
    Revoke {
        /// Name the credentials carry; every key issued to it is revoked.
        #[arg(required_unless_present = "keys")]
        name: Option<String>,
        /// Public user NKey to revoke (repeatable), as `ants auth revoked` or the credentials' JWT shows it.
        #[arg(long = "key", value_name = "KEY")]
        keys: Vec<String>,
        /// Revoke only this device of the human NAME (repeatable): its id, its label when no other device of NAME
        /// has it, or its key, as `ants auth devices NAME` lists them.
        #[arg(long = "device", value_name = "DEVICE", requires = "name")]
        devices: Vec<String>,
        /// Only record the revocation and write server.conf; push later with `ants auth push`.
        #[arg(long)]
        no_push: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the devices of a human, one per credentials issued to it with `ants auth user NAME --human`: id
    /// (`legacy` for credentials issued before devices had ids), label, key, when issued, and whether revoked.
    Devices {
        human: UserId,
        /// One JSON object per line.
        #[arg(long)]
        json: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Label a device of a human anew. Labels need not be unique; an id tells devices with the same label apart.
    Rename {
        human: UserId,
        /// The device: its id, its label when no other device of the human has it, or its key.
        device: String,
        /// The new label.
        label: String,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the revoked keys: key, name, when revoked and when their credentials expire. Revocations of expired
    /// credentials are dropped on the next revoke or push.
    Revoked {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// List the keys issued here: key, name, kind, when issued, expiry, permissions version (`-` for credentials
    /// issued before versions), and whether revoked or reissued since. Credentials in use that are outdated or expire
    /// within 7 days get a line naming the command that renews them.
    Issued {
        /// One JSON object per line.
        #[arg(long)]
        json: bool,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Push the team's account, with its revocations, to the server of --nats (the hub) and through it to the leaf
    /// nodes connected with a system account uplink, as `ants auth revoke` does.
    Push {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
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
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// The request as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Allow the tool call of a pending request, once or with --always for the rest of the ant's session.
    Approve {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// Allow this tool for the rest of the session (allow-always), when the request offers it.
        #[arg(long)]
        always: bool,
        /// Also add the allow rule fitting the request exactly to the ant's allow list in the config file, then
        /// reload the daemon serving it. An ant added from a template takes its allow rules from the template:
        /// nothing is added to its entry.
        #[arg(long)]
        rule: bool,
        /// Config file listing the ant, for --rule (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Reject the tool call of a pending request.
    Reject {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
    },
}

#[derive(Subcommand, Debug)]
enum EnrollCommand {
    /// List the enrollment requests heard, the latest of each ant and key; machines repeat theirs every few
    /// seconds while they wait.
    List {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Sign the credentials an ant asks for, with the rights `ants auth user` gives, for the hub's --js-domain if
    /// it has one, and send them to the asking machine. With --webhook, those of a webhook asking with
    /// `ants webhook add --enroll`.
    #[command(group = clap::ArgGroup::new("targets").args(["to", "channels"]).multiple(true))]
    Approve {
        /// The ant, or with --webhook the webhook.
        ant: UserId,
        /// The asking machine's public key, needed when several ask for the ant.
        #[arg(long, value_name = "KEY")]
        public_key: Option<String>,
        /// Sign a webhook's credentials, with the rights `ants auth user --webhook` gives: it may only message the
        /// ants of --to and post to the channels of --channel.
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
    },
    /// List the renewal requests heard (`ants renew`), the latest of each name and key.
    Renewals {
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
    },
    /// Renew the credentials NAME asks for (`ants renew`): valid, unrevoked credentials of the team, for the new key,
    /// with the same name, kind, members, device and --js-domain, and the permissions their kind has in this version.
    /// With --auto, renew every credentials asking, as they ask, until stopped.
    Renew {
        /// The name the credentials carry: an ant, a human, a webhook, an observer, channel-manager or leaf-<machine>.
        #[arg(required_unless_present = "auto")]
        name: Option<String>,
        /// The asking holder's new public key, needed when several ask for NAME.
        #[arg(long, value_name = "KEY", conflicts_with = "auto")]
        public_key: Option<String>,
        /// Keep running and renew every credentials whose holder asks, without an approval each.
        #[arg(long, conflicts_with = "name")]
        auto: bool,
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

#[derive(Subcommand, Debug)]
enum LeafCommand {
    /// Write the nats-server configuration of the machine's leaf node: listening on localhost only, with one uplink
    /// to the hub, and a JetStream of its own (domain: the machine name) for the outbox. Run it again to switch the
    /// uplink: it reloads the leaf node service (ants.nats, ants-nats) running the file, or says how.
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
        /// System account credentials from `ants auth leaf`, for a second uplink bringing the account updates
        /// `ants auth push` sends the hub, revocations included (default: leaf-<machine>.system.creds beside the
        /// uplink credentials, when there).
        #[arg(long)]
        system_creds: Option<PathBuf>,
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
    /// they may send once the uplink is back. Run it again when the machine's ants or humans change; without
    /// members, it takes those its uplink credentials carry. Fails if the hub does not hear from the outbox within
    /// 10 s, as without `feature_flags { js_api_reply_v2: true }` on the hub.
    Outbox {
        /// Machine name, as given to `ants leaf config`.
        machine: Machine,
        #[command(flatten)]
        members: members::OutboxArgs,
        /// Leaf node configuration whose uplink credentials name the members when --ant and --human do not
        /// (default: ~/.config/ants/leaf.conf).
        #[arg(long, value_name = "FILE")]
        leaf_config: Option<PathBuf>,
        /// Stop taking anything from the machine's outbox instead.
        #[arg(long, conflicts_with_all = ["ants", "humans", "add_ants", "add_humans", "remove_ants", "remove_humans", "leaf_config"])]
        remove: bool,
    },
}

#[derive(Subcommand, Debug)]
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

#[derive(clap::Args, Debug)]
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

fn main() -> Result<()> {
    completions::answer();
    run()
}

#[tokio::main(flavor = "current_thread")]
async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("ANTS_LOG").unwrap_or_else(|_| "info,rmcp=warn".into()),
        )
        .with_writer(progress::log_writer)
        .init();
    let cli = defaults::parse()?;
    let domain = cli.js_domain.as_ref();
    let nats = || Nats::new(&cli.nats, cli.creds.as_deref(), domain);
    match cli.command {
        Command::Ask { thread, replies, words } => ask(&nats()?, thread, &replies, words).await,
        Command::Inbox { peek, thread, limit, follow, json } => {
            let select = inbox::Select { thread: None, limit, peek };
            Box::pin(show_inbox(&nats()?, select, thread, follow, json)).await
        }
        Command::Roster { json } => roster::show(&nats()?, json).await,
        Command::Spend { ant, thread, days, json, config } => {
            let path = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            let caps = ants_daemon::config::Summary::load(&path)?.caps;
            spend::show(&nats()?, &spend::Select { ant, thread, days }, &caps, json).await
        }
        Command::Post { channel, thread, replies, words } => {
            Box::pin(post(&nats()?, channel, thread, &replies, words)).await
        }
        Command::Channel(ChannelCommand::Serve { upgrade }) => {
            Box::pin(upgrade::or_exit(channel::serve(&cli.nats, cli.creds.as_deref(), domain), &upgrade)).await
        }
        Command::Channel(command) => channels(&nats()?, command).await,
        Command::Notes { thread, text } => thread_notes(&nats()?, thread, text).await,
        Command::React { message, reaction, remove } => react(&nats()?, message, reaction, remove).await,
        Command::Thread(ThreadCommand::List { ant, owner, grep, escalated, pinned, archived, since, limit, json }) => {
            let select = thread::Select { ant, owner, grep, limit: Some(limit), escalated, pinned, archived };
            thread::list(&nats()?, since, &select, json).await
        }
        Command::Thread(ThreadCommand::Title { thread, text, clear }) => {
            thread_meta::title(&nats()?, thread, &text, clear).await
        }
        Command::Thread(ThreadCommand::Pin { thread }) => {
            thread_meta::mark(&nats()?, thread, thread_meta::Marking::Pin).await
        }
        Command::Thread(ThreadCommand::Unpin { thread }) => {
            thread_meta::mark(&nats()?, thread, thread_meta::Marking::Unpin).await
        }
        Command::Thread(ThreadCommand::Archive { thread }) => {
            thread_meta::mark(&nats()?, thread, thread_meta::Marking::Archive).await
        }
        Command::Thread(ThreadCommand::Unarchive { thread }) => {
            thread_meta::mark(&nats()?, thread, thread_meta::Marking::Unarchive).await
        }
        Command::Thread(ThreadCommand::Show { thread, since, json }) => {
            thread::show(&nats()?, thread, since, json).await
        }
        Command::Watch { subject, thread } => watch(&nats()?, &subject, thread).await,
        Command::Cancel { ant, thread } => cancel(&nats()?, ant, thread).await,
        Command::Init => {
            for line in init_hub(&nats()?).await? {
                println!("{line}");
            }
            Ok(())
        }
        Command::Setup(args) => {
            let connection = setup::Connection { nats: &cli.nats, creds: cli.creds.as_deref(), js_domain: domain };
            Box::pin(setup::run(&args, &connection)).await
        }
        Command::Completions { shell, install } => {
            let shell = match shell.or_else(completions::Shell::login) {
                Some(shell) => shell,
                None => bail!("$SHELL is not bash, zsh or fish: name the shell"),
            };
            match install {
                true => println!(
                    "{}",
                    completions::install(shell, &std::env::home_dir().context("no home directory")?, false)?
                ),
                false => print!("{}", shell.script(&completions::bin())?),
            }
            Ok(())
        }
        Command::Tools(ToolsCommand::List { follow, json }) => tools::list(&nats()?, follow, json).await,
        Command::Tools(ToolsCommand::Show { id, json }) => tools::show(&nats()?, &id, json).await,
        Command::Tools(ToolsCommand::Approve { id, always, rule, config }) => {
            let verdict = if always { Verdict::AllowAlways } else { Verdict::AllowOnce };
            let config = match (rule, config) {
                (false, _) => None,
                (true, Some(path)) => Some(path),
                (true, None) => Some(ants_daemon::config::default_path()?),
            };
            tools::decide_request(&nats()?, &id, verdict, config.as_deref()).await
        }
        Command::Tools(ToolsCommand::Reject { id }) => {
            tools::decide_request(&nats()?, &id, Verdict::Reject, None).await
        }
        Command::Dead(command) => dead_letters(&nats()?, command).await,
        Command::Daemon(args) => {
            let default_path = ants_daemon::config::default_path()?;
            let served = || {
                let listed = serve::ants(&args, &default_path)?;
                let mut configs =
                    serve::configs(listed.ants, &listed.harnesses, &cli.nats, cli.creds.clone(), domain.cloned())?;
                if let Some((managers, path)) = &listed.managers {
                    for config in configs.iter_mut().filter(|c| managers.contains(&c.ant)) {
                        let (nats, creds, js_domain) =
                            (config.nats_url.clone(), config.creds.clone(), config.js_domain.clone());
                        let manager = manager::Manager::new(path.clone(), nats, creds, js_domain)?;
                        config.requester = ants_daemon::mcp::Requester(Some(manager.requester()));
                    }
                }
                let schedules = scheduler::resolve(listed.schedules, &cli.nats, domain)?;
                Ok(serve::Served { configs, schedules, metrics: listed.metrics })
            };
            let config = serve::config_file(&args, &default_path)?;
            let dropins = ants_daemon::config::dropin_dir(&std::path::absolute(&config)?);
            let triggers = reload::Triggers::new(config, Some(dropins), reload::CHECK_EVERY)?;
            match serve::run(served()?, served, triggers, upgrade::replaced(args.upgrade.upgrade_check)).await? {
                serve::Ended::Stopped => Ok(()),
                serve::Ended::Replaced => upgrade::exit(),
            }
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
        Command::Doctor { config, leaf_config, no_harness, json, team, webhook_config } => {
            let setup = doctor::Setup {
                nats: cli.nats.clone(),
                creds: cli.creds.clone(),
                js_domain: cli.js_domain.clone(),
                config: match config {
                    Some(path) => path,
                    None => ants_daemon::config::default_path()?,
                },
                leaf_config: match leaf_config {
                    Some(path) => path,
                    None => auth::default_dir()?.with_file_name("leaf.conf"),
                },
                harnesses: !no_harness,
                team: team_dir(team)?,
                webhooks: match webhook_config {
                    Some(path) => path,
                    None => webhook::default_path()?,
                },
                home: std::env::home_dir(),
            };
            let checks = Box::pin(doctor::run(&setup)).await;
            for check in &checks {
                match json {
                    true => println!("{}", serde_json::to_string(check)?),
                    false => println!("{check}"),
                }
            }
            let failed = checks.iter().filter(|c| c.status == doctor::Status::Fail).count();
            match failed {
                0 => Ok(()),
                _ => anyhow::bail!("{failed} of {} checks failed", checks.len()),
            }
        }
        Command::Auth(AuthCommand::Init { dir }) => {
            let dir = team_dir(dir)?;
            auth::init(&dir)?;
            println!(
                "team keys in {0}\ncopy {0}/{1} next to the hub's nats-server configuration and include \"{1}\" from it \
                 (include paths are relative), followed by `resolver {{ type: full, dir: \"/ABSOLUTE/DIR\" }}` so the \
                 hub keeps the accounts `ants auth push` sends it across restarts",
                dir.display(),
                auth::SERVER_CONF
            );
            Ok(())
        }
        Command::Auth(AuthCommand::User {
            user,
            human,
            observer,
            webhook,
            to,
            channels,
            dir,
            out,
            public_key,
            label,
            store_key,
        }) => {
            let kind = match (webhook, observer) {
                (true, _) => revoke::Kind::Webhook,
                (_, true) => revoke::Kind::Observer,
                _ => auth::role(&user, human).into(),
            };
            let permissions = match (webhook, observer) {
                (_, true) => {
                    if user.is_human() {
                        bail!("{user} is the default human, not an observer");
                    }
                    Permissions::for_observer(&user)
                }
                (true, _) => {
                    if user.is_human() {
                        bail!("{user} is the default human, not a webhook");
                    }
                    let to = to.into_iter().map(Destination::Ant);
                    let destinations: Vec<Destination> =
                        to.chain(channels.into_iter().map(Destination::Channel)).collect();
                    Permissions::for_webhook(&user, &destinations)
                }
                _ => Permissions::for_user(&user, auth::role(&user, human)),
            }
            .with_js_domain(domain);
            let dir = team_dir(dir)?;
            let label = label.as_deref();
            if store_key {
                let out = out.unwrap_or_else(|| dir.join(format!("{user}.creds")));
                let key = nkeys::KeyPair::new_user();
                let (jwt, device) = auth::sign_device(&dir, &user, kind, permissions, label, &key.public_key())?;
                keys::issue_stored(&out, &jwt, &key, &cli.nats).await?;
                println!("{}", out.display());
                if let Some(device) = device {
                    eprintln!("device {device}");
                }
                return Ok(());
            }
            let Some(key) = public_key else {
                let out = out.unwrap_or_else(|| dir.join(format!("{user}.creds")));
                let device = auth::issue_device(&dir, &user, kind, permissions, label, &out)?;
                println!("{}", out.display());
                if let Some(device) = device {
                    eprintln!("device {device}");
                }
                return Ok(());
            };
            let (jwt, device) = auth::sign_device(&dir, &user, kind, permissions, label, &key)?;
            if let Some(device) = device {
                eprintln!("device {device}");
            }
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
            let expires = match auth::issue_enrollment(&dir, &ant, ttl, &out)? {
                Some(exp) => {
                    let exp = chrono::DateTime::from_timestamp(i64::try_from(exp)?, 0).context("expiry")?;
                    format!("valid until {exp}")
                }
                None => "valid with no expiry".to_owned(),
            };
            println!(
                "{}\n{expires}; on the ant's machine: ants add {ant} --enroll FILE ...\n\
                 then here: ants enroll approve {ant}",
                out.display()
            );
            Ok(())
        }
        Command::Add(args) => add::add(*args, &cli.nats, domain).await,
        Command::Retire(args) => {
            let connected = match nats() {
                Ok(nats) => nats.connect("ants-retire").await,
                Err(e) => Err(e),
            };
            let roster = match connected {
                Ok((_, js)) => Some(js),
                Err(e) => {
                    println!("cannot read the roster ({e:#}): not waiting for {} to stop", args.name);
                    None
                }
            };
            retire::retire(&args, roster.as_ref(), retire::STOP_WAIT, &cli.nats).await
        }
        Command::Renew(args) => renew::run(args, &cli.nats, cli.creds.as_deref()).await,
        Command::Keys(command) => keys::run(command, &cli.nats, cli.creds.as_deref()).await,
        Command::Signer { config, approval_timeout, dir, upgrade } => {
            let dir = team_dir(dir)?;
            let config = match config {
                Some(path) => path,
                None => ants_daemon::config::default_path()?,
            };
            let templates = ants_daemon::config::signer_templates(&config)?;
            let names: Vec<_> = templates.iter().map(|t| t.name.as_str()).collect();
            eprintln!(
                "signing the ants manager ants ask for, within the templates of {} ({}); Ctrl-C stops",
                config.display(),
                if names.is_empty() { "none yet: every request goes to a human".to_owned() } else { names.join(", ") }
            );
            let options = signer::Options {
                config: &config,
                approval_timeout,
                reconnect: renew::AUTO_RECONNECT,
                tick: signer::TICK,
            };
            let stop = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            let serving = signer::serve(&cli.nats, &dir, domain, &options, signer::print, stop);
            Box::pin(upgrade::or_exit(serving, &upgrade)).await
        }
        Command::Enroll(command) => {
            let dir = |dir| team_dir(dir);
            match command {
                EnrollCommand::List { dir: d } => {
                    enroll::Approver::connect(&cli.nats, &dir(d)?, domain, enroll::LISTEN).await?.list().await
                }
                EnrollCommand::Approve { ant, public_key, webhook: true, to, channels, dir: d } => {
                    let dir = dir(d)?;
                    let approver = enroll::Approver::connect(&cli.nats, &dir, domain, enroll::LISTEN).await?;
                    let destinations: Vec<Destination> = to
                        .into_iter()
                        .map(Destination::Ant)
                        .chain(channels.into_iter().map(Destination::Channel))
                        .collect();
                    let key = approver.approve_webhook(&dir, &ant, &destinations, public_key.as_deref()).await?;
                    println!("webhook {ant} approved for {key}");
                    Ok(())
                }
                EnrollCommand::Approve { ant, public_key, dir: d, .. } => {
                    let dir = dir(d)?;
                    let approver = enroll::Approver::connect(&cli.nats, &dir, domain, enroll::LISTEN).await?;
                    let approved = approver.approve(&dir, &ant, public_key.as_deref()).await?;
                    println!("{ant} approved for {}", approved.public_key);
                    if let Some(leaf) = &approved.leaf {
                        println!("the uplink credentials of {} renewed to carry {ant}", leaf.machine);
                        match leaf.outbox {
                            true => {
                                println!("the hub's stream takes {ant}'s messages from the outbox of {}", leaf.machine)
                            }
                            false => {
                                println!("{} has no outbox (`ants leaf outbox`) to admit {ant} from", leaf.machine)
                            }
                        }
                    }
                    Ok(())
                }
                EnrollCommand::Renewals { dir: d } => {
                    let approver = enroll::Approver::connect(&cli.nats, &dir(d)?, domain, enroll::LISTEN).await?;
                    approver.list_renewals().await
                }
                EnrollCommand::Renew { name: None, auto: true, dir: d, .. } => {
                    let dir = dir(d)?;
                    eprintln!("renewing credentials as their holders ask (Ctrl-C stops)");
                    let stop = async {
                        let _ = tokio::signal::ctrl_c().await;
                    };
                    let (listen, reconnect) = (enroll::LISTEN, renew::AUTO_RECONNECT);
                    renew::auto(&cli.nats, &dir, domain, listen, reconnect, renew::print_renewed, stop).await
                }
                EnrollCommand::Renew { name, public_key, dir: d, .. } => {
                    let dir = dir(d)?;
                    let name = name.context("name the credentials to renew, or give --auto")?;
                    let approver = enroll::Approver::connect(&cli.nats, &dir, domain, enroll::LISTEN).await?;
                    let (request, renewed) = approver.renew(&dir, &name, public_key.as_deref()).await?;
                    renew::print_renewed(&request, &renewed);
                    Ok(())
                }
                EnrollCommand::Reject { ant, public_key, reason, dir: d } => {
                    let approver = enroll::Approver::connect(&cli.nats, &dir(d)?, domain, enroll::LISTEN).await?;
                    for key in approver.reject(&ant, public_key.as_deref(), reason.as_deref()).await? {
                        println!("{ant} rejected for {key}");
                    }
                    Ok(())
                }
            }
        }
        Command::Auth(AuthCommand::ChannelManager { dir, out, store_key }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("{}.creds", UserId::channel_manager())));
            match store_key {
                true => {
                    let key = nkeys::KeyPair::new_user();
                    let jwt = auth::sign_channel_manager(&dir, domain, &key.public_key())?;
                    keys::issue_stored(&out, &jwt, &key, &cli.nats).await?;
                }
                false => auth::issue_channel_manager(&dir, domain, &out)?,
            }
            println!("{}\nrun: ants --creds {} channel serve", out.display(), out.display());
            Ok(())
        }
        Command::Auth(AuthCommand::Leaf { machine, members, dir, out }) => {
            let dir = team_dir(dir)?;
            let out = out.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
            let kept = members::issued(&dir, &machine, &out)?;
            let set = members::Change::from(members).apply(&machine, kept, || {
                anyhow::anyhow!(
                    "{} keeps no members of {machine}: name them with --ant, --human or --webhook",
                    dir.display()
                )
            })?;
            auth::issue_leaf(&dir, &machine, &set.roles(), &set.webhooks, set.enroll_approver, domain, &out)?;
            println!("{}\n{}\nmembers of {machine}: {set}", out.display(), auth::leaf_system_creds(&out).display());
            Ok(())
        }
        Command::Auth(AuthCommand::Revoke { name, mut keys, devices, no_push, dir }) => {
            let dir = team_dir(dir)?;
            let now = auth::now()?;
            let mut name = name;
            if let (Some(human), false) = (&name, devices.is_empty()) {
                keys.extend(revoke::device_keys(&dir, human, &devices)?);
                name = None;
            }
            for r in revoke::revoke(&dir, name.as_deref(), &keys)? {
                let expired = if r.expires_at.is_some_and(|exp| exp <= now) { " (already expired)" } else { "" };
                println!("revoked {} ({}){expired}", r.key, r.name.as_deref().unwrap_or("name unknown"));
            }
            println!("{} carries the revocations", dir.join(auth::SERVER_CONF).display());
            if no_push {
                println!(
                    "push them to the hub with: ants auth push --nats URL (leaf nodes with a system account uplink get \
                     them with the push and keep them)"
                );
                return Ok(());
            }
            push(&cli.nats, &dir).await
        }
        Command::Auth(AuthCommand::Revoked { dir }) => revoke::list(&team_dir(dir)?),
        Command::Auth(AuthCommand::Issued { json, dir }) => revoke::list_issued(&team_dir(dir)?, json),
        Command::Auth(AuthCommand::Devices { human, json, dir }) => {
            revoke::list_devices(&team_dir(dir)?, human.as_str(), json)
        }
        Command::Auth(AuthCommand::Rename { human, device, label, dir }) => {
            let renamed = revoke::rename(&team_dir(dir)?, human.as_str(), &device, &label)?;
            let id = renamed.device.map_or_else(|| format!("legacy device {}", renamed.key), |id| id.to_string());
            println!("{id} of {human} is now labelled {label:?}");
            Ok(())
        }
        Command::Auth(AuthCommand::Push { dir }) => push(&cli.nats, &team_dir(dir)?).await,
        Command::Leaf(LeafCommand::Config {
            machine,
            uplinks,
            uplink_creds,
            system_creds,
            server_conf,
            dir,
            ca_file,
            proxy,
            listen,
            out,
        }) => {
            let dir = team_dir(dir)?;
            let creds = uplink_creds.unwrap_or_else(|| dir.join(format!("leaf-{machine}.creds")));
            let system_creds = system_creds.or_else(|| Some(auth::leaf_system_creds(&creds)).filter(|p| p.is_file()));
            if system_creds.is_none() {
                println!(
                    "no system account credentials at {}: revocations pushed to the hub do not reach this leaf; issue \
                     them with `ants auth leaf` and run this again",
                    auth::leaf_system_creds(&creds).display()
                );
            }
            let server_conf = server_conf.unwrap_or_else(|| dir.join(auth::SERVER_CONF));
            let default_out = out.is_none();
            let out = match out {
                Some(out) => out,
                None => auth::default_dir()?.with_file_name("leaf.conf"),
            };
            let config = leaf::Leaf {
                machine: &machine,
                hub_domain: domain,
                uplinks: &uplinks,
                creds: &creds,
                system_creds: system_creds.as_deref(),
                server_conf: &server_conf,
                ca_file: ca_file.as_deref(),
                proxy: proxy.as_deref(),
                listen: &listen,
            };
            let old = std::fs::read_to_string(&out).ok();
            leaf::write(&config, &out)?;
            ants_daemon::creds::warn(&creds);
            system_creds.as_deref().map(ants_daemon::creds::warn);
            let new = std::fs::read_to_string(&out).with_context(|| format!("reading {}", out.display()))?;
            let taken = leaf::take(&out, old.as_deref(), &new, |action| {
                service::Services::local().apply(service::Service::Nats, action, &out)
            });
            println!(
                "{}\n{taken}\n\
                 switch the uplink: run this again\n\
                 clients on this machine: nats://{listen} (the default) with the hub's --js-domain\n\
                 once the leaf is up: ants leaf outbox {machine}{1}",
                out.display(),
                if default_out { String::new() } else { format!(" --leaf-config {}", out.display()) }
            );
            Ok(())
        }
        Command::Bridge(BridgeCommand::Serve { listen, upstream, token, token_file, metrics, upgrade }) => {
            let token = bridge::token(token, token_file.as_deref())?;
            let addr = bridge::listen_addr(&listen, false)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            let server = bridge::Server::new(upstream.clone(), token);
            let _metrics = serve_metrics(metrics, server.traffic()).await?;
            tracing::info!("bridge on http://{addr}, sessions connect to {upstream}");
            upgrade::or_exit(server.run(listener), &upgrade).await
        }
        Command::Hook(HookCommand::Run { on, concurrency, timeout, command }) => {
            let nats = nats()?;
            let (_, js) = nats.connect_only("ants-hook").await?;
            hook::Hook { event: on, command, timeout, concurrency }.run(&js, &nats.me.id).await
        }
        Command::Webhook(WebhookCommand::Add(args)) => webhook_add::add(*args, &cli.nats, domain).await,
        Command::Webhook(WebhookCommand::Serve { config, listen, metrics, upgrade }) => {
            let path = match config {
                Some(path) => path,
                None => webhook::default_path()?,
            };
            let load = || {
                let mut config = webhook::Config::load(&path)?;
                config.metrics = metrics.or(config.metrics);
                config.listen = listen.clone().or(config.listen);
                Ok(config)
            };
            let config = load()?;
            let addr = config.listen.as_deref().unwrap_or(webhook::DEFAULT_LISTEN);
            let addr = bridge::listen_addr(addr, false)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            tracing::info!("webhooks on http://{addr}{}/<webhook>", config.path);
            if config.links.is_some() {
                tracing::info!("approval links on http://{addr}{}/_approval", config.path);
            }
            let reload =
                webhook::Reload { triggers: reload::Triggers::new(path.clone(), None, reload::CHECK_EVERY)?, load };
            upgrade::or_exit(webhook::serve(config, &cli.nats, domain, listener, reload), &upgrade).await
        }
        Command::Webhook(WebhookCommand::NotifyTest { human, config }) => {
            let path = match config {
                Some(path) => path,
                None => webhook::default_path()?,
            };
            let config = webhook::Config::read(&path)?;
            let Some(entry) = config.humans.iter().find(|h| h.name == human) else {
                let names: Vec<&str> = config.humans.iter().map(|h| h.name.as_str()).collect();
                match names.is_empty() {
                    true => bail!("{} lists no [[human]]", path.display()),
                    false => bail!("{} lists no human {human}, only {}", path.display(), names.join(", ")),
                }
            };
            let machine = ants_daemon::roster::machine().await;
            let results = notify::test(entry, &machine).await;
            for (sink, sent) in &results {
                match sent {
                    Ok(()) => println!("ok    {sink}"),
                    Err(e) => println!("fail  {sink}: {e:#}"),
                }
            }
            let failed = results.iter().filter(|(_, sent)| sent.is_err()).count();
            match failed {
                0 => Ok(()),
                _ => bail!("{failed} of {} sinks of {human} failed", results.len()),
            }
        }
        Command::Bridge(BridgeCommand::Connect {
            url,
            listen,
            token,
            token_file,
            headers,
            proxy,
            metrics,
            upgrade,
        }) => {
            let token = bridge::token(token, token_file.as_deref())?;
            let remote = bridge::Remote::new(&url, token.as_deref(), &headers, proxy.as_deref())?;
            let addr = bridge::listen_addr(&listen, true)?;
            let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
            let _metrics = serve_metrics(metrics, remote.traffic()).await?;
            tracing::info!("bridging {addr} to {url}; leaf uplink: nats-leaf://{addr}");
            upgrade::or_exit(remote.run(listener), &upgrade).await
        }
        Command::Leaf(LeafCommand::Outbox { machine, members, leaf_config, remove }) => {
            if remove {
                let (_, js) = nats()?.connect("ants-cli").await?;
                outbox::remove(&js, &machine).await?;
                println!("the hub no longer takes anything from the outbox of {machine}");
                return Ok(());
            }
            let change = members::Change::from(members);
            let kept = match change.lists() {
                true => None,
                false => {
                    let conf = match leaf_config {
                        Some(path) => path,
                        None => auth::default_dir()?.with_file_name("leaf.conf"),
                    };
                    let members = members::uplink(&conf, &machine).with_context(|| {
                        format!("no --ant or --human given: reading the members of {machine} from {}", conf.display())
                    })?;
                    Some(members)
                }
            };
            let set = change.apply(&machine, kept, || anyhow::anyhow!("no members of {machine}"))?;
            let nats = nats()?;
            let (_, js) = nats.connect("ants-cli").await?;
            let leaf = outbox::connect(&cli.nats, cli.creds.as_deref(), &nats.me.id, &machine).await?;
            outbox::admit(&leaf, &js, &machine, &set.roles()).await?;
            outbox::sourced(&js, &machine, outbox::SOURCE_WAIT).await?;
            println!("outbox of {machine} ready; the hub takes from it what its members send ({set})");
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
    /// The device of the human the credentials were issued for.
    pub device: humans::Device,
    rights: humans::Rights,
    /// Whether a connection checked the hub's stream and buckets already ([`upgrade_hub`]).
    hub_checked: AtomicBool,
}

impl<'a> Nats<'a> {
    fn new(url: &'a str, creds: Option<&'a Path>, domain: Option<&'a JsDomain>) -> Result<Self> {
        let me = humans::Me::from_creds(creds)?;
        let device = humans::Device::from_creds(creds, &me.id)?;
        let rights = humans::Rights::from_creds(creds)?;
        Ok(Self { url, creds, domain, me, device, rights, hub_checked: AtomicBool::new(false) })
    }

    /// Connect as [`Nats::me`], and register it in the humans bucket ([`humans::register_or_warn`]). The first
    /// connection of a human also brings the hub's stream and buckets up to date ([`upgrade_hub`]).
    pub async fn connect(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let (client, js) = self.connect_only(name).await?;
        let upgrade = async {
            if self.me.role.is_human() && !self.hub_checked.swap(true, Ordering::Relaxed) {
                upgrade_hub(&js, &self.rights).await;
            }
        };
        tokio::join!(humans::register_or_warn(&js, &self.me), upgrade);
        Ok((client, js))
    }

    /// Connect as [`Nats::me`] without registering it, naming the connection after its device, if any, too. A human's
    /// credentials known to predate batched direct gets read the stream one message at a time from the start.
    async fn connect_only(&self, name: &str) -> Result<(async_nats::Client, async_nats::jetstream::Context)> {
        let name = match &self.device.id {
            Some(device) => format!("{name} {device}"),
            None => name.to_owned(),
        };
        let (client, js) = bus::connect(self.url, self.creds, self.domain, &self.me.id, &name).await?;
        if self.me.role.is_human() && self.rights.older_than(ants_daemon::reading::BATCHES_SINCE) {
            ants_daemon::reading::without_batches(&client);
        }
        Ok((client, js))
    }
}

/// `ants init`: create the hub's stream and buckets, or upgrade those an older version set up, and register the human
/// of the credentials. Returns what was done, a line each.
async fn init_hub(nats: &Nats<'_>) -> Result<Vec<String>> {
    let (_, js) = nats.connect_only("ants-cli").await?;
    let fixed = hub::init(&js, |subject| nats.rights.may_publish(subject)).await;
    let fixed = fixed.context("`ants init` needs a human's credentials (`ants auth user NAME --human`)")?;
    let mut lines: Vec<String> = fixed.iter().map(|finding| finding.fixed()).collect();
    humans::register_or_warn(&js, &nats.me).await;
    lines.push(format!(
        "stream {} and buckets {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} and {} ready",
        ants_proto::STREAM,
        ants_proto::ROSTER_BUCKET,
        ants_proto::SESSIONS_BUCKET,
        ants_proto::ATTEMPTS_BUCKET,
        ants_proto::CHANNELS_BUCKET,
        ants_proto::HUMANS_BUCKET,
        ants_proto::CHAINS_BUCKET,
        ants_proto::LINKS_BUCKET,
        ants_proto::OWNERS_BUCKET,
        ants_proto::WEBHOOKS_BUCKET,
        ants_proto::REACTIONS_BUCKET,
        ants_proto::THREADS_BUCKET,
        ants_proto::READ_BUCKET,
        ants_proto::PRESENCE_BUCKET
    ));
    Ok(lines)
}

/// Create the hub's stream and buckets that are missing and upgrade those out of date, as `ants init` does
/// ([`hub::check`]), saying what was done. Those the credentials may not fix are reported as needing `ants init`;
/// those whose stream info the credentials do not get, or that cannot be checked (the hub out of reach), are left
/// alone. Returns what was fixed.
/// Serve health and metrics from `source` on `addr`, if given, for as long as the result is kept.
async fn serve_metrics(
    addr: Option<SocketAddr>,
    source: Arc<dyn ants_daemon::metrics::Source>,
) -> Result<Option<ants_daemon::metrics::Listening>> {
    match addr {
        Some(addr) => Ok(Some(ants_daemon::metrics::start(addr, source).await?)),
        None => Ok(None),
    }
}

async fn upgrade_hub(js: &async_nats::jetstream::Context, rights: &humans::Rights) -> Vec<hub::Finding> {
    let mut fixed = Vec::new();
    for finding in hub::check(js, |subject| rights.may_publish(subject)).await {
        if let hub::Problem::Unchecked(_) = finding.problem {
            tracing::debug!("{finding}");
        } else if !finding.rights().iter().all(|subject| rights.may_publish(subject)) {
            tracing::warn!("{finding}; `ants init` with a human's current credentials (`ants auth user`) fixes it");
        } else {
            match hub::apply(js, &finding).await {
                Ok(()) => {
                    tracing::info!("{}", finding.fixed());
                    fixed.push(finding);
                }
                Err(e) => tracing::warn!("{finding}; `ants init` as a human fixes it: {e:#}"),
            }
        }
    }
    fixed
}

/// Push the team's account to the server at `url` ([`revoke::push`]), saying how to apply it otherwise on failure.
async fn push(url: &str, dir: &Path) -> Result<()> {
    match revoke::push(url, dir).await {
        Ok(pushed) => {
            println!(
                "account pushed to server {} ({url}); connections of revoked credentials are closed",
                pushed.server
            );
            let applied: Vec<&str> =
                pushed.others.iter().filter(|(_, error)| error.is_none()).map(|(name, _)| name.as_str()).collect();
            if !applied.is_empty() {
                println!("also applied by: {}", applied.join(", "));
            }
            for (name, error) in &pushed.others {
                if let Some(error) = error {
                    println!("not applied by {name}: {error}");
                }
            }
            let conf = dir.join(auth::SERVER_CONF);
            match pushed.kept {
                Some(true) => println!(
                    "server {} keeps it across restarts (full resolver): copy {} to it only for changes besides the \
                     account's (operator, system account, max_control_line)",
                    pushed.server,
                    conf.display()
                ),
                Some(false) => println!(
                    "server {0} keeps it in memory only: a restart brings back the account of its server.conf, so \
                     copy {1} to it and reload it (SIGHUP); for it to keep pushed accounts, add `resolver {{ type: \
                     full, dir: \"/ABSOLUTE/DIR\" }}` after its `include \"{2}\"` and restart it",
                    pushed.server,
                    conf.display(),
                    auth::SERVER_CONF
                ),
                None => println!(
                    "server {} did not tell whether it keeps pushed accounts across restarts (`ants doctor` on this \
                     machine checks it); without a full resolver, copy {} to it and reload it (SIGHUP)",
                    pushed.server,
                    conf.display()
                ),
            }
            println!(
                "leaf nodes not listed (offline, or with no system account uplink) did not apply it: run `ants auth \
                 push` again once they are connected, unless the hub keeps it (they then catch up within 10 s of \
                 reconnecting)"
            );
            Ok(())
        }
        Err(e) => Err(e.context(format!(
            "the account was not pushed to {url}; the revocations stay in {}: push them with `ants auth push \
             --nats URL` once the hub is in reach, or copy server.conf to the hub and reload it (SIGHUP)",
            dir.display()
        ))),
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

async fn ask(nats: &Nats<'_>, thread: Option<thread::Pick>, replies: &Replies, words: Vec<String>) -> Result<()> {
    // Refuse a message with nothing to send before connecting.
    recipient(words.clone())?;
    let (client, js) = nats.connect("ants-cli").await?;
    let thread = thread::resolve(thread, &js, &nats.me).await?;
    let envelope = ask_envelope(&nats.me, thread, words)?;
    // Subscribe first so a fast reply is not missed. A waiter also reads back what the hub stored while out of its
    // reach.
    let inbox = match replies.wait || replies.follow {
        true => bus::caught_up_inbox(&client, &js, &nats.me.id, nats.me.role).await?,
        false => bus::live_inbox(&client, &nats.me.id).await?,
    };
    // Warned rather than refused: a daemon may start later and take the message, which waits in the stream.
    let warnings = roster::unserved_warnings(&js, &[&envelope.to]).await;
    let to_human = !warnings.is_empty() && humans::load(&js, &nats.me).await.contains(&envelope.to);
    let unserved = !warnings.is_empty() && !to_human;
    if unserved {
        for warning in warnings {
            tracing::warn!("{warning}");
        }
        if replies.wait || replies.follow {
            let flag = if replies.wait { "--wait" } else { "--follow" };
            tracing::warn!("a reply waits for a daemon to serve {}: {flag} may not end until one does", envelope.to);
        }
    }
    let status = match (replies.wait || replies.follow) && !to_human && progress::wanted(nats.me.role.is_human()) {
        true => client.subscribe(subject::status(&envelope.to, &envelope.thread_id)).await.ok(),
        false => None,
    };
    if let Some(owner) = &envelope.owner {
        owners::recording(&js, nats.domain, &envelope.thread_id, &envelope.to, owner).await;
    }
    if bus::publish(&js, &envelope).await? == bus::Stored::Outbox {
        println!("kept in this machine's outbox until the hub is reachable");
    }
    println!("thread {}", envelope.thread_id);
    println!("message {}", envelope.id);
    if !replies.wait && !replies.follow {
        return Ok(());
    }
    let _shown = status.map(|sub| {
        let stand = if unserved { progress::Stand::Unserved } else { progress::Stand::Waiting };
        let status = progress::Status::new(vec![envelope.id]).with(envelope.to.clone(), stand);
        progress::start(sub, status, replies.wait)
    });
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
            progress::clear();
            println!("{}: {}", reply.from, reply.text);
            // Seen here, so `ants inbox` need not show it again. The other replies stay unread; an ant's inbox
            // consumer is its daemon's.
            if nats.me.role.is_human() && !nats.device.marks_read {
                tracing::warn!("{REISSUE_HUMAN}; the reply stays unread");
            } else if nats.me.role.is_human() {
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
/// recorded for the thread and the recipient before the message goes out ([`owners::record`]); the first record of a
/// thread to an ant stays.
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
    thread: Option<thread::Pick>,
    replies: &Replies,
    words: Vec<String>,
) -> Result<()> {
    let text = words.join(" ");
    if text.trim().is_empty() {
        bail!("nothing to post");
    }
    let (client, js) = nats.connect("ants-cli").await?;
    let thread = thread::resolve(thread, &js, &nats.me).await?;
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
    let status = match room.is_some() && progress::wanted(nats.me.role.is_human()) {
        true => client.subscribe(subject::status_on(&post.thread_id)).await.ok(),
        false => None,
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
    println!("message {}", post.id);
    let Some(room) = room else { return Ok(()) };
    let _shown = status.map(|sub| progress::start(sub, progress::Status::new(vec![post.id]), replies.wait));
    // Waiting is for an answer, so the poster's own posts from elsewhere do not count; following shows them all. What
    // never comes live, as while the leaf's uplink is down, is read back from the hub's stream.
    let posts = channel::caught_up_posts(room, &js, &nats.me, &post, stored);
    let on_thread = channel::thread_posts(posts, &post, replies.wait);
    replies
        .show(&post.thread_id, on_thread, async |reply: ChannelPost| {
            progress::clear();
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
            let thread = thread::resolve(thread, &js, &nats.me).await?;
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
        ChannelCommand::Serve { .. } => unreachable!("main runs the channel manager"),
    }
    Ok(())
}

/// Warn about members, messengers and owners no daemon serves now ([`roster::unserved`]), rather than refuse them:
/// one may start later. `humans` need none.
async fn warn_unserved<'a>(
    js: &async_nats::jetstream::Context,
    users: impl IntoIterator<Item = &'a UserId>,
    humans: &ants_proto::Humans,
) {
    let users: Vec<&UserId> = users.into_iter().filter(|u| !humans.contains(u)).collect();
    for warning in roster::unserved_warnings(js, &users).await {
        tracing::warn!("{warning}");
    }
}

/// Why credentials of a human from before the read marker cannot mark replies read.
const REISSUE_HUMAN: &str = "these credentials predate the read marker every device of a human shares: issue new ones \
                             with `ants auth user NAME --human` (and a leaf's with `ants auth leaf`)";

async fn show_inbox(
    nats: &Nats<'_>,
    mut select: inbox::Select,
    thread: Option<thread::Pick>,
    follow: bool,
    json: bool,
) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's, whose inbox its daemon reads; `ants inbox` reads a human's");
    }
    if !nats.device.marks_read {
        bail!("{REISSUE_HUMAN}");
    }
    let (client, js) = nats.connect("ants-inbox").await?;
    select.thread = thread::resolve(thread, &js, &nats.me).await?;
    let select = &select;
    let mut inbox = inbox::Inbox::open(&js, me).await?;
    let show_with = |e: &Envelope, reactions: &[reactions::Reacted]| -> Result<()> {
        if json {
            println!("{}", serde_json::to_string(&inbox::Shown { envelope: e, reactions })?);
        } else {
            println!("{}", inbox::render(e, reactions));
        }
        Ok(())
    };
    let show = |e: &Envelope| show_with(e, &[]);
    let unread = inbox.unread(select).await?;
    if unread.is_empty() && !follow && !json {
        println!("no unread replies");
    }
    let ids: Vec<Uuid> = unread.iter().map(|e| e.id).collect();
    let reactions = inbox::reactions_or_none(&js, &ids).await;
    unread.iter().try_for_each(|e| show_with(e, reactions.get(&e.id).map_or(&[], Vec::as_slice)))?;
    if follow {
        // A dead letter stored here tells the human through the inbox, like one the daemon stores.
        let watch = async {
            if let Err(e) = dead::watch(&client, &js, me, |_, _| {}).await {
                tracing::warn!("not watching for messages JetStream stopped delivering: {e:#}");
            }
            std::future::pending::<()>().await
        };
        let followed = inbox.follow(select, show, async { _ = tokio::signal::ctrl_c().await });
        // Following one thread shows no escalation on the others, so only following them all counts as reading.
        let followed = async {
            match select.thread {
                None => {
                    let device = nats.device.id;
                    Box::pin(presence::while_following(&js, nats.domain, me, device, Following::Inbox, followed)).await
                }
                Some(_) => followed.await,
            }
        };
        // Following every thread as text shows the tool requests too, until the replies end or showing them fails.
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let followed = async {
            let followed = followed.await;
            drop(stop);
            followed
        };
        let requests = async {
            if select.thread.is_some() || json {
                return;
            }
            let report = |update: &tools::Update| {
                println!("{}", tools::render(update, chrono::Utc::now()));
                Ok(())
            };
            let shown = async {
                tokio::select! {
                    shown = Box::pin(tools::follow(&client, &js, report)) => {
                        if let Err(e) = shown {
                            tracing::warn!("not showing tool requests: {e:#}");
                        }
                    }
                    _ = stopped => {}
                }
            };
            let device = nats.device.id;
            Box::pin(presence::while_following(&js, nats.domain, me, device, Following::Approvals, shown)).await;
        };
        let followed = async { tokio::join!(followed, requests).0 };
        tokio::select! {
            followed = followed => followed?,
            () = watch => {}
        }
    }
    Ok(())
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

async fn thread_notes(nats: &Nats<'_>, thread: thread::Pick, text: Vec<String>) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let thread = thread.resolve(&js, &nats.me).await?;
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

async fn react(nats: &Nats<'_>, message: Uuid, reaction: Reaction, remove: bool) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let me = &nats.me.id;
    let line = if remove {
        match reactions::remove(&js, nats.domain, me, &message, &reaction).await? {
            true => format!("removed {reaction} from message {message}"),
            false => format!("no {reaction} reaction to message {message} to remove"),
        }
    } else {
        match reactions::add(&js, nats.domain, me, &message, &reaction).await? {
            true => format!("reacted {reaction} to message {message}"),
            false => format!("already reacted {reaction} to message {message}"),
        }
    };
    println!("{line}");
    Ok(())
}

/// How long `ants cancel` waits for a daemon of the ant to answer.
const CANCEL_WAIT: Duration = Duration::from_secs(5);
/// How long `ants cancel` waits on, after a daemon of the ant runs no prompt on the thread, for another daemon of the
/// same ant that may run it.
const CANCEL_GRACE: Duration = Duration::from_millis(500);

async fn cancel(nats: &Nats<'_>, ant: UserId, thread: thread::Pick) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; only a human cancels a prompt");
    }
    let (client, js) = nats.connect("ants-cancel").await?;
    let thread = thread.resolve(&js, &nats.me).await?;
    let request = CancelRequest::new(ant, thread, me.clone());
    let answers = client.subscribe(request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    let (ant, thread) = (&request.ant, &request.thread_id);
    match cancel_outcome(&request, answers, CANCEL_WAIT, CANCEL_GRACE).await {
        Some(true) => {
            println!("cancelled the prompt of {ant} on thread {thread}; the partial reply follows on the thread");
            Ok(())
        }
        Some(false) => bail!("{ant} has no prompt running on thread {thread}"),
        None => bail!("no daemon serving {ant} answered within {}s", CANCEL_WAIT.as_secs()),
    }
}

/// What the daemons of the ant answer to `request`: whether one cancelled the prompt, or `None` when none answers
/// within `wait`. That no prompt runs is the answer once `grace` passes with no daemon cancelling it.
async fn cancel_outcome(
    request: &CancelRequest,
    mut answers: impl futures::Stream<Item = async_nats::Message> + Unpin,
    wait: Duration,
    grace: Duration,
) -> Option<bool> {
    let mut deadline = tokio::time::Instant::now() + wait;
    let mut outcome = None;
    while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, answers.next()).await {
        let Ok(answer) = serde_json::from_slice::<CancelReply>(&msg.payload) else { continue };
        if answer.request_id != request.id || answer.check_subject(&msg.subject).is_err() {
            continue;
        }
        if answer.cancelled {
            return Some(true);
        }
        if outcome.is_none() {
            outcome = Some(false);
            deadline = deadline.min(tokio::time::Instant::now() + grace);
        }
    }
    outcome
}

/// What `ants watch <subject> [thread]` subscribes to: a subject filter as given, or, for an ant (one token), its
/// status subjects.
fn watch_filter(target: &str, thread: Option<&ThreadId>) -> Result<String> {
    match watched_ant(target, thread.is_some())? {
        Some(ant) => Ok(thread.map_or_else(|| subject::status_of(&ant), |t| subject::status(&ant, t))),
        None => Ok(target.to_owned()),
    }
}

/// The ant `target` names for `ants watch`, None for a subject filter, which follows no `thread`.
fn watched_ant(target: &str, thread: bool) -> Result<Option<UserId>> {
    match target.parse::<UserId>().ok() {
        None if thread => bail!("a thread can only follow an ant, not the subject filter {target:?}"),
        ant => Ok(ant),
    }
}

async fn watch(nats: &Nats<'_>, target: &str, thread: Option<thread::Pick>) -> Result<()> {
    use std::io::Write as _;

    watched_ant(target, thread.is_some())?;
    let (client, js) = nats.connect("ants-watch").await?;
    let filter = watch_filter(target, thread::resolve(thread, &js, &nats.me).await?.as_ref())?;
    let mut sub = client.subscribe(filter).await?;
    let mut printer = Printer::default();
    let mut reacted = watched_reactions(&js, target, nats.me.role.is_human()).await;
    loop {
        let msg = tokio::select! {
            msg = sub.next() => match msg {
                Some(msg) => msg,
                None => break,
            },
            Some(change) = reacted.next() => {
                let mut stdout = std::io::stdout().lock();
                stdout.write_all(printer.line(&reaction_line(&change)).as_bytes())?;
                stdout.flush()?;
                continue;
            }
        };
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
        } else if let Ok(r) = serde_json::from_slice::<CancelRequest>(&msg.payload) {
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            printer.line(&format!("[{}] {} asks to cancel the prompt of {}{forged}", r.thread_id, r.by, r.ant))
        } else if let Ok(r) = serde_json::from_slice::<CancelReply>(&msg.payload) {
            let forged = forged(r.check_subject(&msg.subject).is_ok());
            let outcome = if r.cancelled { "prompt cancelled" } else { "no prompt to cancel" };
            printer.line(&format!("[{}] {}{forged}: {outcome}", r.thread_id, r.ant))
        } else {
            printer.line(&format!("{} {}", msg.subject, String::from_utf8_lossy(&msg.payload)))
        };
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(out.as_bytes())?;
        stdout.flush()?;
    }
    Ok(())
}

/// The reactions `ants watch` follows besides `target`: all of them for a human taking every message (`ants.>` or
/// `>`), else none, as when the bucket cannot be read.
async fn watched_reactions(
    js: &async_nats::jetstream::Context,
    target: &str,
    human: bool,
) -> futures::stream::BoxStream<'static, reactions::Change> {
    let none = || futures::stream::pending().boxed();
    if !human || ![subject::ALL, ">"].contains(&target) {
        return none();
    }
    match reactions::watch(js).await {
        Ok(changes) => changes.chain(futures::stream::pending()).boxed(),
        Err(e) => {
            tracing::warn!("not showing reactions: {e:#}");
            none()
        }
    }
}

/// A reaction added or removed as `ants watch` prints it.
fn reaction_line(change: &reactions::Change) -> String {
    let verb = if change.removed { "removed the reaction" } else { "reacted" };
    format!("{} {verb} {} to message {}", change.by, change.reaction, change.message)
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
            StatusUpdate::Queued { message, ahead: 0 } => {
                return self.line(&format!("[{thread}] {who}{forged}: message {message} starts"));
            }
            StatusUpdate::Queued { message, ahead } => {
                return self.line(&format!("[{thread}] {who}{forged}: message {message} queued behind {ahead}"));
            }
            StatusUpdate::Approval { request_id } => {
                return self.line(&format!("[{thread}] {who}{forged} waits for approval {who}/{request_id}"));
            }
            StatusUpdate::Escalated { to, escalation } => {
                let (stuck, level, reason) = (&escalation.stuck, escalation.level, escalation.reason.as_str());
                return self
                    .line(&format!("[{thread}] {who}{forged} escalated {stuck}'s {reason} to {to} (level {level})"));
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
    use std::assert_matches;

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
        assert!(watched_ant("ants.>", true).is_err());
    }

    #[test]
    fn watch_spend_and_channel_read_take_a_start_of_a_thread_id_or_last() {
        let start = Some(thread::Pick::Prefix("0199".to_owned()));
        let cli = Cli::try_parse_from(["ants", "watch", "dev-a", "0199"]).unwrap();
        assert_matches!(cli.command, Command::Watch { thread, .. } if thread == start);
        let cli = Cli::try_parse_from(["ants", "spend", "--thread", "0199"]).unwrap();
        assert_matches!(cli.command, Command::Spend { thread, .. } if thread == start);
        let cli = Cli::try_parse_from(["ants", "channel", "read", "ops", "--thread", "last"]).unwrap();
        let Command::Channel(ChannelCommand::Read { thread, .. }) = cli.command else { panic!("not channel read") };
        assert_eq!(thread, Some(thread::Pick::Last));
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

    #[tokio::test]
    async fn cancel_waits_for_any_daemon_of_the_ant_to_cancel_the_prompt() {
        let request = CancelRequest::new("dev-a".parse().unwrap(), "t1".parse().unwrap(), UserId::human());
        let msg = |answer: &CancelReply| async_nats::Message {
            subject: answer.subject().into(),
            reply: None,
            payload: serde_json::to_vec(answer).unwrap().into(),
            headers: None,
            status: None,
            description: None,
            length: 0,
        };
        let other = CancelRequest::new(request.ant.clone(), request.thread_id.clone(), UserId::human());
        let (idle, busy) = (msg(&request.answer(false)), msg(&request.answer(true)));
        let (long, grace) = (Duration::from_secs(60), Duration::from_millis(50));
        let answers = |msgs: Vec<async_nats::Message>| futures::stream::iter(msgs).chain(futures::stream::pending());

        let outcome = cancel_outcome(&request, answers(vec![idle.clone(), busy]), long, grace).await;
        assert_eq!(outcome, Some(true), "another daemon of the ant runs the prompt");
        let alone = vec![msg(&other.answer(true)), idle];
        let outcome =
            tokio::time::timeout(Duration::from_secs(5), cancel_outcome(&request, answers(alone), long, grace)).await;
        assert_eq!(outcome, Ok(Some(false)), "no prompt, known once the grace passes");
        let outcome = cancel_outcome(&request, answers(vec![msg(&other.answer(true))]), grace, grace).await;
        assert_eq!(outcome, None, "no daemon answered this request");
    }

    #[test]
    fn notes_shows_without_text_and_posts_with_it() {
        let cli = Cli::try_parse_from(["ants", "notes", "t1", "split", "the", "work"]).unwrap();
        let Command::Notes { thread, text } = cli.command else { panic!("not notes") };
        let words = vec!["split".to_owned(), "the".to_owned(), "work".to_owned()];
        assert_eq!((thread, text), (thread::Pick::Prefix("t1".to_owned()), words));
        let cli = Cli::try_parse_from(["ants", "notes", "t1"]).unwrap();
        assert_matches!(cli.command, Command::Notes { text, .. } if text.is_empty());
        assert!(Cli::try_parse_from(["ants", "plan", "t1"]).is_err());
    }

    #[test]
    fn react_takes_a_message_id_a_reaction_and_remove() {
        let id = Uuid::now_v7().to_string();
        let cli = Cli::try_parse_from(["ants", "react", &id, "--remove", "eyes"]).unwrap();
        assert_matches!(cli.command, Command::React { message, reaction, remove: true }
            if message.to_string() == id && reaction.as_str() == "eyes");
        assert!(Cli::try_parse_from(["ants", "react", &id, "Eyes!"]).is_err(), "no subject token");
        assert!(Cli::try_parse_from(["ants", "react", "t1", "eyes"]).is_err(), "no message id");
    }

    #[test]
    fn add_is_a_top_level_command() {
        let cli = Cli::try_parse_from(["ants", "add", "dev-a", "--enroll", "enroll-dev-a.creds"]).unwrap();
        assert_matches!(cli.command, Command::Add(args) if args.name.as_str() == "dev-a");
        assert!(Cli::try_parse_from(["ants", "ant", "add", "dev-a"]).is_err());
    }

    #[test]
    fn tool_requests_are_listed_shown_approved_and_rejected() {
        let parse = |line: &str| Cli::try_parse_from(line.split(' ')).map(|cli| cli.command);
        assert_matches!(
            parse("ants tools approve dev-a/019a --always"),
            Ok(Command::Tools(ToolsCommand::Approve { id, always: true, rule: false, .. })) if id == "dev-a/019a"
        );
        assert_matches!(
            parse("ants tools approve 019a --rule"),
            Ok(Command::Tools(ToolsCommand::Approve { id, always: false, rule: true, .. })) if id == "019a"
        );
        assert_matches!(parse("ants tools reject 019a"), Ok(Command::Tools(ToolsCommand::Reject { id })) if id == "019a");
        assert_matches!(
            parse("ants tools show 019a --json"),
            Ok(Command::Tools(ToolsCommand::Show { id, json: true })) if id == "019a"
        );
        assert_matches!(
            parse("ants tools list -f"),
            Ok(Command::Tools(ToolsCommand::List { follow: true, json: false }))
        );
        for line in [
            "ants approve",
            "ants approve 019a allow-once",
            "ants tools approve",
            "ants tools reject 019a --always",
            "ants tools reject 019a --rule",
        ] {
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
        assert_eq!((r.wait, r.follow, r.timeout), (true, false, Some(Duration::from_mins(90))));
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

    /// A human's credentials bring the hub up to date; an ant's ask for nothing they are denied, which would only time
    /// out, and fix nothing.
    #[tokio::test]
    async fn a_human_brings_the_hub_up_to_date() {
        let Some(server) = auth::tests::Server::start() else { return };
        let connect = async |user: &UserId, role| {
            let creds = server.creds_as(user, role);
            let (_, js) = bus::connect(&server.url, Some(&creds), None, user, "test").await.unwrap();
            (js, humans::Rights::from_creds(Some(&creds)).unwrap())
        };
        let (a_js, a_rights) = connect(&"dev-a".parse().unwrap(), ants_proto::Role::Ant).await;
        assert!(!a_rights.may_publish(&hub::Object::Sessions.info_subject()));
        let started = tokio::time::Instant::now();
        assert!(upgrade_hub(&a_js, &a_rights).await.is_empty());
        assert!(started.elapsed() < Duration::from_secs(2), "no request was denied");

        let (h_js, h_rights) = connect(&"alice".parse().unwrap(), ants_proto::Role::Human).await;
        h_js.create_stream(async_nats::jetstream::stream::Config {
            name: ants_proto::STREAM.to_owned(),
            subjects: vec![subject::ALL_NOTES.to_owned()],
            ..Default::default()
        })
        .await
        .unwrap();
        // As created before, with the 1000 consumers nats-server gives a stream by default.
        h_js.create_key_value(hub::channels_config()).await.unwrap();
        let fixed: Vec<_> = upgrade_hub(&h_js, &h_rights).await.into_iter().map(|f| f.object).collect();
        let seen: Vec<_> = hub::Object::ALL.into_iter().filter(|o| *o != hub::Object::Presence).collect();
        assert_eq!(fixed, seen, "the old stream and bucket upgraded, every bucket a human may look up created");
        for watched in [hub::Object::Channels, hub::Object::Humans, hub::Object::Roster] {
            let config = h_js.get_stream(watched.stream_name()).await.unwrap().cached_info().config.clone();
            assert_eq!(config.max_consumers, hub::MAX_CONSUMERS, "{watched}");
        }
        assert!(hub::check(&h_js, |s| h_rights.may_publish(s)).await.is_empty());
        assert!(upgrade_hub(&h_js, &h_rights).await.is_empty());
    }

    #[test]
    fn a_recipient_alone_is_an_error() {
        assert!(recipient(words("@dev-b")).is_err());
        assert!(recipient(words("@Bad.Name hi")).is_err());
    }
}
