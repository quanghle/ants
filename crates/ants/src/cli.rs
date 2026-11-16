//! The command line: the global options and the commands.

use std::path::PathBuf;
use std::time::Duration;

use ants_proto::{ChannelId, JsDomain, Reaction, ThreadId, UserId, Uuid, subject};
use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use clap_complete::engine::{ArgValueCandidates, ArgValueCompleter};
use futures::StreamExt as _;

use crate::cmd::{
    ant::AntCommand, assistant::AssistantCommand, attachment::AttachmentCommand, auth::AuthCommand,
    bridge::BridgeCommand, channel::ChannelCommand, dead::DeadCommand, enroll::EnrollCommand, harness::HarnessCommand,
    hook::HookCommand, keys::KeysCommand, leaf::LeafCommand, login, notify::NotifyCommand, setup,
    thread::ThreadCommand, tools::ToolsCommand, webhook::WebhookCommand,
};
use crate::{
    add, attachments, channel, completions, compose, progress, renew, retire, serve, spend, thread, update, upgrade,
};

/// The whole command line.
pub(crate) type Cli = Ants<Command>;

/// The global options and the commands of `C`: [`Command`] for all of them, or [`DaemonCommand`] alone, which spares
/// `ants daemon` building every other command at startup.
#[derive(Parser)]
#[command(name = "ants", version, about = "Agent teams messaging over NATS")]
pub(crate) struct Ants<C: Subcommand> {
    /// NATS server: nats://, tls://, ws:// or wss://. Point it at the local leaf node where there is one.
    /// Unset, the config file's `[cli]` or top-level `nats` applies, except to `ants daemon` and `ants add`.
    #[arg(long, global = true, env = "ANTS_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    pub(crate) nats: String,

    /// NATS credentials file (`ants auth user` writes one). Needed when the server enforces per-user permissions.
    /// The CLI acts as the human they name, and registers it in `ants_humans` on connecting; without them, as `human`.
    /// Unset, the config file's `[cli]` `creds` apply, except to `ants daemon` and `ants add`.
    /// A human's also create the hub's stream and buckets that are missing on connecting, as `ants init` does.
    #[arg(long, global = true, env = "ANTS_CREDS")]
    pub(crate) creds: Option<PathBuf>,

    /// JetStream domain of the hub (its `jetstream { domain }`), needed to reach JetStream from a leaf node without
    /// JetStream. Set it for every ants process, and when issuing credentials with `ants auth`.
    /// Unset, the config file's `[cli]` or top-level `js_domain` applies, except to `ants daemon` and `ants add`.
    #[arg(long, global = true, env = "ANTS_JS_DOMAIN")]
    pub(crate) js_domain: Option<JsDomain>,

    /// With bare `ants`: the two-line pane of `ants chat --compact`.
    #[arg(long)]
    pub(crate) compact: bool,

    // None for bare `ants`, which runs `ants chat` on a terminal.
    #[command(subcommand)]
    pub(crate) command: Option<C>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Send a message as the CLI's human, who owns the thread. Starts with @ant to pick the recipient; otherwise the
    /// human's assistant gets it while its grant stands, else the messenger. With --wait or --follow, print the
    /// replies to the CLI's human on the thread and mark them read.
    Ask {
        /// Continue an existing thread: its id, a unique start of one, or `last`. The message still prompts only
        /// its recipient (@ant, else the human's assistant or the messenger), not the thread's other ants.
        /// A human's message interrupts a running prompt on that thread and prompts the same session next, unless
        /// sent with --queue.
        #[arg(long, value_name = "THREAD")]
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: Option<thread::Pick>,
        /// Reply to this message or post, quoting its author and an excerpt of it: its id, or with a human's
        /// credentials a unique start of one, as `ants react` takes. The message goes on that message's thread and,
        /// without @ant, to its author unless the CLI's human wrote it.
        #[arg(long, value_name = "MESSAGE", add = ArgValueCompleter::new(completions::messages))]
        reply_to: Option<thread::MessagePick>,
        /// On an existing thread (--thread or --reply-to), wait behind the prompt running there rather than
        /// interrupting it, as a human's message does by default.
        #[arg(long)]
        queue: bool,
        /// Hand this message or post on to @ant (else the human's assistant or the messenger) on a new thread: its
        /// id, or a unique start of one, as --reply-to takes. The new message quotes it with its author and the
        /// thread it is on, carries its attachments, and the words, if any, as a note before it; the new thread
        /// links to the source thread, as a spin-off does. Needs human credentials.
        #[arg(long, value_name = "MESSAGE", conflicts_with_all = ["thread", "reply_to"])]
        #[arg(add = ArgValueCompleter::new(completions::messages))]
        forward: Option<thread::MessagePick>,
        #[command(flatten)]
        sending: Sending,
        #[command(flatten)]
        replies: Replies,
        #[command(flatten)]
        input: compose::Input,
        #[command(flatten)]
        attach: attachments::Attach,
        /// `@ant` (optional), then the message, unless --file or --edit gives it.
        #[arg(num_args = 1.., add = ArgValueCandidates::new(completions::mentions))]
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
        /// delivering without the daemon having stored them as dead letters (as `ants dead watch` does) and printing
        /// the reactions added to or removed from the replies shown. Without --thread and --json, also print the
        /// pending tool requests, new ones and how each ends.
        #[arg(long, short)]
        follow: bool,
        /// One JSON envelope per line; following, a reaction added or removed as one of message, by, reaction and
        /// removed.
        #[arg(long)]
        json: bool,
        /// Mark unread again, on all the human's devices, a message to the human and the later ones on its thread
        /// (a message id or a unique start of one), or the latest message to the human on a thread (its id, a unique
        /// start of one, or `last`); prints nothing else.
        #[arg(long, value_name = "MESSAGE|THREAD", conflicts_with_all = ["peek", "thread", "limit", "follow", "json"])]
        #[arg(add = ArgValueCompleter::new(completions::messages))]
        unread: Option<String>,
    },
    /// List the ants being served: harness, machine, capabilities, escalation chain, last heartbeat, and whether it
    /// is recent.
    Colony {
        /// One JSON entry per line, with its state.
        #[arg(long)]
        json: bool,
    },
    /// One ant at a glance, as the ant view of `ants chat` shows it.
    #[command(subcommand)]
    Ant(AntCommand),
    /// List the machines whose daemons recorded the ants they serve, with host credentials (`ants auth host`): kept
    /// when an ant goes offline and its colony entry expires. With ANT, the machine serving it, or that last did.
    Machines {
        /// Only the machines of this ant.
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// One JSON record per line; with ANT, one object with its machines.
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
        /// Reply to this post or message on a thread of the channel, quoting its author and an excerpt of it: its id,
        /// or with a human's credentials a unique start of one, as `ants react` takes. The post goes on that thread;
        /// an @mention in the text still picks whom it wakes.
        #[arg(long, value_name = "MESSAGE", add = ArgValueCompleter::new(completions::messages))]
        reply_to: Option<thread::MessagePick>,
        /// On an existing thread (--thread or --reply-to), wait behind the prompt running there rather than
        /// interrupting it, as a human's post does by default.
        #[arg(long)]
        queue: bool,
        #[command(flatten)]
        replies: Replies,
        #[command(flatten)]
        input: compose::Input,
        #[command(flatten)]
        attach: attachments::Attach,
        /// The post, unless --file or --edit gives it; each @ant mention wakes that member.
        #[arg(num_args = 1.., add = ArgValueCandidates::new(completions::mentions))]
        words: Vec<String>,
    },
    /// Channels: named groups of ants that humans post to with `ants post`.
    #[command(subcommand)]
    Channel(ChannelCommand),
    /// Show the notes of a thread, or post TEXT to them as the CLI's human, which gives them a fresh turn budget.
    /// Thread notes are a passive board, one per thread: a post wakes nobody, and ants read the notes with
    /// read_notes. They are distinct from the execution plan an ant reports, which `ants watch <ant>` shows.
    Notes {
        /// The thread: its id, a unique start of one, or `last`.
        #[arg(add = ArgValueCandidates::new(completions::threads))]
        thread: thread::Pick,
        #[command(flatten)]
        attach: attachments::Attach,
        /// The post. Without it, show the notes.
        #[arg(value_name = "TEXT")]
        text: Vec<String>,
    },
    /// React to a message as the CLI's user, such as `eyes` for seen or `check` for done, or remove the reaction with
    /// --remove. A reaction is no message: it wakes nobody, marks nothing read and ends no wait for a reply. Humans see
    /// reactions beneath the messages in `ants inbox`, `ants thread show` and `ants watch`. Message ids come from
    /// `ants ask`, `ants post`, `ants inbox` and `ants thread show`.
    React {
        /// The message's id, or with a human's credentials a unique start of one among the messages to or from the
        /// CLI's human stored in the last 7 days, else any stored then.
        #[arg(add = ArgValueCompleter::new(completions::messages))]
        message: thread::MessagePick,
        /// The reaction: 1-64 of lowercase letters, digits and '-', such as eyes, check, thumbsup, x, heart or rocket.
        #[arg(add = ArgValueCandidates::new(completions::reactions))]
        reaction: Reaction,
        /// Remove the reaction instead of adding it.
        #[arg(long)]
        remove: bool,
    },
    /// Answer a question an ant asked the CLI's human (with ask_human), shown with its options numbered in `ants inbox`
    /// and `ants thread show`: a number picks that option, any other text is a typed answer, which only a question
    /// taking one accepts. The answer goes to the asking ant on the question's thread, as a message from the human.
    /// Needs a human's credentials.
    Answer {
        /// The question's message id, or a unique start of one among the messages to or from the CLI's human stored in
        /// the last 7 days, else any stored then.
        #[arg(add = ArgValueCompleter::new(completions::messages))]
        message: thread::MessagePick,
        /// An option's number, or the typed answer.
        #[arg(value_name = "N|TEXT", required = true, num_args = 1..)]
        answer: Vec<String>,
    },
    /// Threads read back from the stream across inboxes (both ways), channel posts, notes, escalations, tool requests
    /// and decisions, and dead letters: list the recent ones, find those mentioning a text, show one's whole history,
    /// or search messages by sender, recipient, channel, kind, time and text; title, pin, archive or summarise one.
    /// Nothing is consumed or marked read. Needs a human's credentials.
    #[command(subcommand)]
    Thread(ThreadCommand),
    /// Ask the config file's `[cli]` `summary_ant` (by default the messenger) for a digest of the threads with entries
    /// within --since: what finished, what is stuck, and what waits on the human. Kept nowhere; the request opens a
    /// thread of its own, left out of `ants thread list`, `ants inbox` and `ants dash`. Needs a human's credentials.
    Digest {
        /// How far back, such as 90m, 24h or 7d; 0 for everything the stream keeps. Only the newest entries within
        /// 16 KiB are sent.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "24h")]
        since: Duration,
    },
    /// Files attached to messages (`--attach` of `ants ask`, `ants post` and `ants notes`): save one.
    #[command(subcommand)]
    Attachment(AttachmentCommand),
    /// Hold back the notifications `ants webhook serve` pushes to the CLI's human for a while, on every device of the
    /// human: snooze them, resume them, or show what is held. Needs a human's credentials.
    #[command(subcommand)]
    Notify(NotifyCommand),
    /// Create the CLI's human's dedicated assistant ant, designate an existing ant as its assistant, withdraw or
    /// retire it, show a human's, or enroll it as a device of the human for sealed messages. The human's device signs
    /// the designation. Needs a human device's credentials, but for `show` and `key`.
    #[command(subcommand)]
    Assistant(AssistantCommand),
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
    /// The full-screen chat UI, also what bare `ants` opens on a terminal: home (what waits on the human), work (the
    /// threads by state), the human's channels, the ants and the people, kept live as messages, posts, progress and
    /// tool requests come. Needs a terminal and a human's credentials.
    Chat {
        /// How far back to read threads, such as 90m, 24h or 7d; 0 for everything the stream keeps.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "7d")]
        since: Duration,
        /// Draw a two-line pane instead, as for a tmux split: what waits on the human by kind, the ants working and on
        /// what, the spend that UTC day, and the top home item with its keys; Enter opens the full view.
        #[arg(long)]
        compact: bool,
    },
    /// One line of what waits on the human by kind, the ants working and the ants' spend that UTC day, without a
    /// terminal: for a status bar or a script. Reads for at most 3s and prints ? for what it could not read in time.
    /// Needs a human's credentials.
    Status {
        /// One short line for tmux's status-right: `⚑3 ◐2 $4.12`, the items waiting, the ants working, the spend.
        #[arg(long, conflicts_with = "json")]
        tmux: bool,
        /// One JSON object, null for what was not read in time.
        #[arg(long)]
        json: bool,
    },
    /// Interactive terminal dashboard: recent threads and their history, the ants' live progress and the colony, and
    /// the pending tool requests, each shown in full and answered after a confirmation. Needs a terminal and a human's
    /// credentials.
    Dash {
        /// How far back to read threads, such as 90m, 24h or 7d; 0 for everything the stream keeps.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "24h")]
        since: Duration,
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
    /// List the messages waiting for ANT, oldest first: those its daemon holds, running, waiting their turn on their
    /// thread or held for a sign-in or a provider limit, then those awaiting redelivery (one started in an attempt that
    /// did not end is not withdrawable), then the inbox messages and the channel posts waking ANT still in the stream,
    /// with id, thread, sender, age and place in the thread's queue. Without ANT, lists those of every ant live in the
    /// colony, with an ANT column, leaving out the ants with nothing waiting. Needs a human's credentials, and a daemon
    /// of the ant to answer.
    Queue {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: Option<UserId>,
        /// One JSON object per message instead of a table, naming its ant when listing every ant's.
        #[arg(long)]
        json: bool,
    },
    /// Withdraw MESSAGE (an id `ants queue` lists) from the queue of ANT before ANT is prompted with it: its daemon
    /// acks it unprompted when its turn comes, and tells the thread it was withdrawn. The prompt running and the other
    /// messages go on. A message awaiting a retry after a failed attempt, or held for a sign-in or a provider limit
    /// after its tools ran, is withdrawn too, unless its retry starts first. A message ANT started on otherwise is not
    /// withdrawn: `ants cancel` stops its prompt. Needs a human's credentials.
    Withdraw {
        #[arg(add = ArgValueCandidates::new(completions::ants))]
        ant: UserId,
        message: Uuid,
    },
    /// Create the ANTS stream and the colony, sessions, attempts, channels, humans, chains, links, owners, webhooks,
    /// reactions, threads, read, presence and notify buckets on the server, those missing; one already there stays as
    /// it is. Needs a human's credentials. Every other command run with them does the same on connecting. Also writes
    /// the incarnation marker of the identities bucket when absent, which every ant's shared settings are signed for.
    Init {
        /// After restoring the identities bucket (stream KV_ants_identities) from a backup: write its incarnation
        /// marker anew, so that no shared settings record signed before verifies, the backup's included. Each daemon
        /// holding an ant's credentials signs the ant's settings anew within seconds: those it kept, else its
        /// config's.
        #[arg(long)]
        identities_restored: bool,
    },
    /// Tool requests ants escalate to humans: list the pending ones, show one, approve or reject it. Every human
    /// sees every request; the first decision wins, a human's or that of an ant of the ant's escalation chain.
    #[command(subcommand)]
    Tools(ToolsCommand),
    /// Messages ants gave up on: inspect, replay or drop them.
    #[command(subcommand)]
    Dead(DeadCommand),
    #[command(flatten)]
    Daemon(DaemonCommand),
    /// Add an ant to the machine: list it in the config file and give it credentials. Its user NKey is generated here
    /// and the seed stays here. Without --jwt or --enroll, print the public key for the machine holding the account
    /// key to sign; run again with --jwt or --enroll to complete the credentials. Completed credentials reload the
    /// daemon service of the config file to serve the ant, when it runs (scripts/install-units.sh).
    Add(Box<add::AddArgs>),
    /// Retire an ant added from a template (by a manager's `request_ant`): unlist it from the config file, so the
    /// daemon stops it as on any reload, its prompts in flight ending first; wait up to 5m for that; revoke its
    /// credentials, when the account key is in --dir, drop it from the uplink of this machine's leaf node, and push the
    /// account; remove its credentials file. Its home
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
    /// Slack, Telegram). Names taken, ants added by added ants and forged requests are refused. Also answers ants'
    /// reactions requests (their `react` and `list_reactions` tools, `ants react` with an ant's credentials) for the
    /// messages each ant reads, and the renewal requests (`ants renew`) of devices registered with `ants login`,
    /// renewing them while their grant holds; every [registration] sweep, revokes those whose GitHub users left the
    /// policy's organizations and teams.
    Signer {
        /// Config file whose [[ant_template]] tables are the policy; the rest of it is ignored
        /// (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
        /// How long a human has to decide on a request outside the policy, at most until the manager stops waiting,
        /// or on a registration (`ants login`) outside the [registration] table, at most until its request expires.
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "15m")]
        approval_timeout: Duration,
        /// Team directory holding the signing keys (default: ~/.config/ants/auth).
        #[arg(long, env = "ANTS_AUTH_DIR")]
        dir: Option<PathBuf>,
        #[command(flatten)]
        upgrade: upgrade::UpgradeArgs,
    },
    /// Register a device of a human with a GitHub sign-in: generate its NKey here, ask `ants signer` on the hub with
    /// the team's bootstrap credentials, show GitHub's device login page and the code to enter there, and wait for
    /// the credentials or a refusal (repeating the request every 5 s, for up to 15 min, until Ctrl-C). Then write
    /// ~/.config/ants/auth/NAME.creds and set `creds` and `nats` of [cli] in the config file when [cli] names no
    /// credentials. A refusal, an expiry or Ctrl-C leaves no key and no file.
    Login(login::LoginArgs),
    /// Renew credentials over NATS: generate a new user NKey here, ask for the credentials of the file renewed for it,
    /// signed with their key, and wait for `ants enroll renew NAME` on the machine holding the account key (or, for
    /// a device registered with `ants login`, for `ants signer` there, which may refuse). Then
    /// replace the file and reload the daemon serving an ant with it, or restart the leaf node whose uplink it is.
    /// Expired credentials are renewed with `ants auth` instead.
    Renew(renew::RenewArgs),
    /// Keep the NKey seeds of credentials files in the OS credential store (macOS keychain, Linux Secret Service,
    /// Windows Credential Manager) instead of the files, move them back, or remove credentials from the machine.
    #[command(subcommand)]
    Keys(KeysCommand),
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
    /// Replace this ants executable with a release published on GitHub: the latest, when newer than this one, or the
    /// one of --version. Checks the archive against the release's SHA256SUMS, runs the new binary with --version, then
    /// renames it over the executable at once; services running ants restart onto it through their upgrade check.
    /// Refuses an executable installed by cargo install, or in a directory not writable, printing the command to run.
    Update(update::UpdateArgs),
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

/// How `ants ask` sends.
#[derive(clap::Args, Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Sending {
    /// Send a message from a human to another human in the clear: by default it is sealed so only the two humans'
    /// devices read it.
    #[arg(long)]
    pub(crate) plaintext: bool,
}

/// What `ants ask` and `ants post` do once sent: return at once, wait for the first reply, or follow the thread.
#[derive(clap::Args, Debug, Default)]
pub(crate) struct Replies {
    /// Wait for the first reply on the thread, print it and exit; exit with status 1 if none comes within
    /// --timeout.
    #[arg(long, group = "awaiting")]
    pub(crate) wait: bool,
    /// Print every reply on the thread as it arrives, and the reactions added to or removed from the message and
    /// the replies, until --timeout or Ctrl-C.
    #[arg(long, short, group = "awaiting")]
    pub(crate) follow: bool,
    /// How long --wait or --follow lasts, such as 90s, 10m or 1h30m. Without it, until a reply (--wait) or Ctrl-C.
    #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, requires = "awaiting")]
    pub(crate) timeout: Option<Duration>,
}

impl Replies {
    /// Hand the replies on `thread` to `show` as they come: the first one with `--wait`, each one with `--follow`,
    /// until the timeout or, following, Ctrl-C. A `--wait` without a reply in time is an error.
    pub(crate) async fn show<T>(
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
        let within = self.timeout.map_or_default(ants_proto::duration::format);
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
pub(crate) enum DaemonCommand {
    /// Serve ant inboxes through agent harnesses: those of the config file, or one described by flags.
    Daemon(Box<serve::DaemonArgs>),
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use crate::cmd::{channel::ChannelCommand, tools::ToolsCommand};

    #[test]
    fn watch_spend_and_channel_read_take_a_start_of_a_thread_id_or_last() {
        let start = Some(thread::Pick::Prefix("0199".to_owned()));
        let cli = Cli::try_parse_from(["ants", "watch", "dev-a", "0199"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Watch { thread, .. } if thread == start);
        let cli = Cli::try_parse_from(["ants", "spend", "--thread", "0199"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Spend { thread, .. } if thread == start);
        let cli = Cli::try_parse_from(["ants", "channel", "read", "ops", "--thread", "last"]).unwrap();
        let Command::Channel(ChannelCommand::Read { thread, .. }) = cli.command.unwrap() else {
            panic!("not channel read")
        };
        assert_eq!(thread, Some(thread::Pick::Last));
    }

    #[test]
    fn bare_ants_names_no_command_and_chat_reads_a_week_by_default() {
        let cli = Cli::try_parse_from(["ants", "--nats", "nats://hub:4222"]).unwrap();
        assert!(cli.command.is_none());
        let cli = Cli::try_parse_from(["ants", "chat"]).unwrap();
        assert_matches!(cli.command, Some(Command::Chat { since, compact: false }) if since == Duration::from_hours(7 * 24));
        let cli = Cli::try_parse_from(["ants", "chat", "--since", "90m"]).unwrap();
        assert_matches!(cli.command, Some(Command::Chat { since, .. }) if since == Duration::from_mins(90));
        let cli = Cli::try_parse_from(["ants", "--compact"]).unwrap();
        assert!(cli.compact && cli.command.is_none());
        let cli = Cli::try_parse_from(["ants", "chat", "--compact"]).unwrap();
        assert_matches!(cli.command, Some(Command::Chat { compact: true, .. }));
        assert!(Cli::try_parse_from(["ants", "status", "--tmux", "--json"]).is_err());
    }

    #[test]
    fn ask_on_a_thread_needs_no_steering_command() {
        let cli = Cli::try_parse_from(["ants", "ask", "--thread", "last", "@dev-a", "go"]).unwrap();
        assert_matches!(cli.command, Some(Command::Ask { thread: Some(thread::Pick::Last), .. }));
        assert!(Cli::try_parse_from(["ants", "ask", "--steer", "--thread", "last", "@dev-a", "go"]).is_err());
        assert!(Cli::try_parse_from(["ants", "steer", "dev-a", "last", "go"]).is_err());
    }

    #[test]
    fn ask_and_post_queue_behind_a_running_prompt() {
        let cli = Cli::try_parse_from(["ants", "ask", "--queue", "--thread", "last", "@dev-a", "then"]).unwrap();
        assert_matches!(cli.command, Some(Command::Ask { queue: true, .. }));
        let cli = Cli::try_parse_from(["ants", "post", "ops", "--queue", "--thread", "last", "then"]).unwrap();
        assert_matches!(cli.command, Some(Command::Post { queue: true, .. }));
    }

    #[test]
    fn notes_shows_without_text_and_posts_with_it() {
        let cli = Cli::try_parse_from(["ants", "notes", "t1", "split", "the", "work"]).unwrap();
        let Command::Notes { thread, text, .. } = cli.command.unwrap() else { panic!("not notes") };
        let words = vec!["split".to_owned(), "the".to_owned(), "work".to_owned()];
        assert_eq!((thread, text), (thread::Pick::Prefix("t1".to_owned()), words));
        let cli = Cli::try_parse_from(["ants", "notes", "t1"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Notes { text, .. } if text.is_empty());
        assert!(Cli::try_parse_from(["ants", "plan", "t1"]).is_err());
    }

    #[test]
    fn ask_and_post_reply_to_a_message() {
        let id = Uuid::now_v7();
        let cli = Cli::try_parse_from(["ants", "ask", "--reply-to", &id.to_string(), "ship", "it"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Ask { reply_to: Some(thread::MessagePick::Id(m)), .. } if m == id);
        let cli = Cli::try_parse_from(["ants", "post", "--reply-to", "019a-", "ops", "deploy?"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Post { reply_to: Some(thread::MessagePick::Prefix(p)), .. }
            if p == "019a-");
        assert!(Cli::try_parse_from(["ants", "ask", "--reply-to", "t1", "hi"]).is_err(), "no message id");
    }

    #[test]
    fn answer_takes_a_message_and_an_option_or_words() {
        let id = Uuid::now_v7();
        let cli = Cli::try_parse_from(["ants", "answer", &id.to_string(), "2"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Answer { message, answer }
            if message == thread::MessagePick::Id(id) && answer == ["2"]);
        let cli = Cli::try_parse_from(["ants", "answer", "019a-", "after", "lunch"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Answer { answer, .. } if answer == ["after", "lunch"]);
        assert!(Cli::try_parse_from(["ants", "answer", &id.to_string()]).is_err(), "no answer");
    }

    #[test]
    fn react_takes_a_message_id_a_reaction_and_remove() {
        let id = Uuid::now_v7();
        let cli = Cli::try_parse_from(["ants", "react", &id.to_string().to_uppercase(), "--remove", "eyes"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::React { message, reaction, remove: true }
            if message == thread::MessagePick::Id(id) && reaction.as_str() == "eyes");
        let cli = Cli::try_parse_from(["ants", "react", "019A-", "eyes"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::React { message: thread::MessagePick::Prefix(p), .. } if p == "019a-");
        assert!(Cli::try_parse_from(["ants", "react", &id.to_string(), "Eyes!"]).is_err(), "no subject token");
        assert!(Cli::try_parse_from(["ants", "react", "t1", "eyes"]).is_err(), "no message id");
    }

    #[test]
    fn add_is_a_top_level_command() {
        let cli = Cli::try_parse_from(["ants", "add", "dev-a", "--enroll", "enroll-dev-a.creds"]).unwrap();
        assert_matches!(cli.command.unwrap(), Command::Add(args) if args.name.as_str() == "dev-a");
        assert!(Cli::try_parse_from(["ants", "ant", "add", "dev-a"]).is_err());
    }

    #[test]
    fn tool_requests_are_listed_shown_approved_and_rejected() {
        let parse = |line: &str| Cli::try_parse_from(line.split(' ')).map(|cli| cli.command.unwrap());
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
        let replies = |args: &str| match parse(args).unwrap().command.unwrap() {
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
            parse(&["--default-messenger"]).unwrap().command.unwrap()
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
            parse(&["--owner", "@lead"]).unwrap().command.unwrap()
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
}
