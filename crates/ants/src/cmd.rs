//! What the commands do, with the arguments of each group of subcommands.
//!
//! [`run`] dispatches every command; `main.rs` only parses the command line and calls it, so adding a command
//! touches [`crate::cli`] and this module, not `main.rs`. Each arm of [`run`] hands its command to one function:
//!
//! - in `cmd/NAME.rs` for `ants NAME`, which holds the clap enum of its subcommands, if it has some, and the
//!   handler: what turns the arguments into calls, connects and prints. `cmd/send.rs` serves the commands that
//!   send (`ask`, `post`, `notes`, `react`, `answer`). A module only one command uses lives in `cmd/`
//!   whole (`cmd/setup.rs`, `cmd/webhook/add.rs`);
//! - or in a top-level module, when one function there is the whole command (`colony::show`).
//!
//! Top-level modules hold the logic commands share with each other, the terminal UI, the daemon's services and the
//! assistant. The terminal UI ([`crate::tui`]: `ants`, `ants chat`, `ants status`) stays top-level for its size.

pub(crate) mod ant;
pub(crate) mod assistant;
pub(crate) mod attachment;
pub(crate) mod auth;
pub(crate) mod bridge;
pub(crate) mod cancel;
pub(crate) mod channel;
pub(crate) mod daemon;
pub(crate) mod dash;
pub(crate) mod dead;
pub(crate) mod doctor;
pub(crate) mod enroll;
pub(crate) mod harness;
pub(crate) mod hook;
pub(crate) mod inbox;
pub(crate) mod init;
pub(crate) mod keys;
pub(crate) mod leaf;
pub(crate) mod login;
pub(crate) mod notify;
pub(crate) mod remind;
pub(crate) mod retire;
pub(crate) mod send;
pub(crate) mod setup;
pub(crate) mod signer;
pub(crate) mod spend;
pub(crate) mod thread;
pub(crate) mod tools;
pub(crate) mod watch;
pub(crate) mod webhook;

use anyhow::Result;

use crate::cli::{Cli, Command, DaemonCommand};
use crate::connection::{Connection, Listing};
use crate::{add, colony, completions, machines, queue, renew, summary, tui, update};

/// Run the command `cli` names, or the terminal UI without one.
pub(crate) async fn run(cli: Cli) -> Result<()> {
    let domain = cli.js_domain.as_ref();
    let conn = Connection { nats: &cli.nats, creds: cli.creds.as_deref(), js_domain: domain };
    let nats = || conn.nats();
    let Some(command) = cli.command else {
        tui::usage_without_terminal();
        return Box::pin(tui::run(&nats()?, tui::SINCE, cli.compact)).await;
    };
    match command {
        Command::Ask { thread, reply_to, queue, forward, forward_thread, sending, replies, input, attach, words } => {
            let forward =
                forward.map(send::Forwarding::Message).or_else(|| forward_thread.map(send::Forwarding::Thread));
            let target = send::Target { thread, reply_to, queue, forward };
            Box::pin(send::ask(&nats()?, target, sending, &replies, &input, &attach, words)).await
        }
        Command::Inbox { unread: Some(target), .. } => inbox::mark_unread(&nats()?, &target).await,
        Command::Inbox { peek, thread, limit, follow, json, activity, unread: None } => {
            let select = crate::inbox::Select { thread: None, limit, peek, summaries: false, activity };
            Box::pin(inbox::show(&nats()?, select, thread, Listing { follow, json })).await
        }
        Command::Colony { json } => colony::show(&nats()?, json).await,
        Command::Ant(command) => Box::pin(ant::run(&nats()?, command)).await,
        Command::Machines { ant, json } => machines::show(&nats()?, ant.as_ref(), json).await,
        Command::Spend { ant, thread, days, json, config } => {
            spend::run(&conn, &crate::spend::Select { ant, thread, days }, config, json).await
        }
        Command::Post { channel, thread, reply_to, queue, replies, input, attach, words } => {
            let target = send::Target { thread, reply_to, queue, forward: None };
            Box::pin(send::post(&nats()?, channel, target, &replies, &input, &attach, words)).await
        }
        Command::Channel(channel::ChannelCommand::Serve { upgrade }) => channel::serve(&conn, &upgrade).await,
        Command::Channel(command) => channel::run(&nats()?, command).await,
        Command::Notes { thread, attach, text } => send::thread_notes(&nats()?, thread, &attach, text).await,
        Command::React { message, reaction, remove } => send::react(&nats()?, message, reaction, remove).await,
        Command::Answer { message, answer } => send::answer(&nats()?, message, answer).await,
        Command::Thread(command) => thread::run(&nats()?, command).await,
        Command::Digest { since } => summary::digest(&nats()?, since).await,
        Command::Attachment(command) => Box::pin(attachment::run(&nats()?, command)).await,
        Command::Notify(command) => notify::run(&nats()?, command).await,
        Command::Remind(command) => remind::run(&nats()?, command).await,
        Command::Assistant(command) => assistant::run(command, nats).await,
        Command::Watch { subject, thread } => watch::watch(&nats()?, &subject, thread).await,
        Command::Chat { since, compact } => Box::pin(tui::run(&nats()?, since, compact)).await,
        Command::Status { tmux, json } => Box::pin(tui::status(&nats()?, tmux, json)).await,
        Command::Dash { since } => Box::pin(dash::run(&nats()?, since)).await,
        Command::Cancel { ant, thread } => cancel::cancel(&nats()?, ant, thread).await,
        Command::Queue { ant, json } => queue::list(&nats()?, ant, json).await,
        Command::Withdraw { ant, message } => queue::withdraw(&nats()?, ant, message).await,
        Command::Init { identities_restored } => init::run(&nats()?, identities_restored).await,
        Command::Setup(args) => Box::pin(setup::run(&args, &conn)).await,
        Command::Completions { shell, install } => completions::run(shell, install),
        Command::Update(args) => update::run(args).await,
        Command::Tools(command) => tools::run(command, &conn).await,
        Command::Dead(command) => Box::pin(dead::run(&nats()?, command)).await,
        Command::Daemon(DaemonCommand::Daemon(args)) => daemon::run(&args, &conn).await,
        Command::Mcp { socket, token } => ants_daemon::mcp::bridge_stdio(&socket, &token).await,
        Command::Harness(command) => harness::run(command).await,
        Command::Doctor { config, leaf_config, no_harness, json, team, webhook_config } => {
            doctor::run(config, leaf_config, no_harness, json, team, webhook_config, &conn).await
        }
        Command::Auth(command) => auth::run(command, &conn).await,
        Command::Add(args) => Box::pin(add::add(*args, conn.nats, domain)).await,
        Command::Retire(args) => retire::run(&args, &conn).await,
        Command::Login(args) => login::run(args).await,
        Command::Renew(args) => renew::run(args, conn.nats, conn.creds).await,
        Command::Keys(command) => keys::run(command, conn.nats, conn.creds).await,
        Command::Signer { config, approval_timeout, dir, upgrade } => {
            signer::run(config, approval_timeout, dir, upgrade, &conn).await
        }
        Command::Enroll(command) => enroll::run(command, &conn).await,
        Command::Leaf(command) => leaf::run(command, &conn).await,
        Command::Bridge(command) => bridge::run(command).await,
        Command::Hook(command) => hook::run(&nats()?, command).await,
        Command::Webhook(command) => webhook::run(command, &conn).await,
    }
}
