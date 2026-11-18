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
//! Every handler has one shape: `handler(context, arguments)`. The context comes first: `&Nats` for a command that
//! acts as the CLI's user, `&Connection` for one that needs the connection options or connects only for some of its
//! subcommands, none for one that does not connect. The arguments follow, by value: the clap enum of the
//! subcommands, the command's `clap::Args` struct, or the fields of a variant with three at most; a command with more
//! gets an `Args` struct in its `cmd/NAME.rs`. The handler in `cmd/NAME.rs` is `run`; `cmd/send.rs` names one
//! function after each command, and a top-level module (or `ants_daemon`, for `ants mcp`) names it for what it does
//! (`queue::withdraw`).
//!
//! An arm boxes its handler's future (`Box::pin`) when clippy's `large_futures` flags awaiting it, either here or
//! where `main.rs` awaits [`run`]: that future is at least as large as the largest one an arm awaits unboxed.
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
pub(crate) mod trail;
pub(crate) mod watch;
pub(crate) mod webhook;

use anyhow::Result;

use crate::cli::{Cli, Command, DaemonCommand};
use crate::connection::Connection;
use crate::credentials::renew;
use crate::release::update;
use crate::threads::summary;
use crate::{add, colony, completions, machines, queue, tui};

/// Run the command `cli` names, or the terminal UI without one.
pub(crate) async fn run(cli: Cli) -> Result<()> {
    let conn = Connection { nats: &cli.nats, creds: cli.creds.as_deref(), js_domain: cli.js_domain.as_ref() };
    let nats = || conn.nats();
    let Some(command) = cli.command else {
        tui::usage_without_terminal();
        return Box::pin(tui::run(&nats()?, tui::SINCE, cli.compact)).await;
    };
    match command {
        Command::Ask(args) => Box::pin(send::ask(&nats()?, args)).await,
        Command::Inbox(args) => Box::pin(inbox::run(&nats()?, args)).await,
        Command::Colony { json } => colony::show(&nats()?, json).await,
        Command::Ant(command) => ant::run(&nats()?, command).await,
        Command::Machines { ant, json } => machines::show(&nats()?, ant, json).await,
        Command::Spend(args) => spend::run(&conn, args).await,
        Command::Post(args) => send::post(&nats()?, args).await,
        Command::Channel(command) => channel::run(&conn, command).await,
        Command::Notes { thread, attach, text } => send::notes(&nats()?, thread, attach, text).await,
        Command::React { message, reaction, remove } => send::react(&nats()?, message, reaction, remove).await,
        Command::Answer { message, answer } => send::answer(&nats()?, message, answer).await,
        Command::Thread(command) => thread::run(&nats()?, command).await,
        Command::Trail(command) => trail::run(&nats()?, command).await,
        Command::Digest { since } => summary::digest(&nats()?, since).await,
        Command::Attachment(command) => attachment::run(&nats()?, command).await,
        Command::Notify(command) => notify::run(&nats()?, command).await,
        Command::Remind(command) => remind::run(&nats()?, command).await,
        Command::Assistant(command) => assistant::run(&conn, command).await,
        Command::Watch { subject, thread } => watch::run(&nats()?, subject, thread).await,
        Command::Chat { since, compact } => Box::pin(tui::run(&nats()?, since, compact)).await,
        Command::Status { tmux, json } => tui::status(&nats()?, tmux, json).await,
        Command::Dash { since } => dash::run(&nats()?, since).await,
        Command::Cancel { ant, thread } => cancel::run(&nats()?, ant, thread).await,
        Command::Queue { ant, json } => queue::list(&nats()?, ant, json).await,
        Command::Withdraw { ant, message } => queue::withdraw(&nats()?, ant, message).await,
        Command::Init { identities_restored } => init::run(&nats()?, identities_restored).await,
        Command::Setup(args) => setup::run(&conn, *args).await,
        Command::Completions { shell, install } => completions::run(shell, install),
        Command::Update(args) => update::run(args).await,
        Command::Tools(command) => tools::run(&conn, command).await,
        Command::Dead(command) => Box::pin(dead::run(&nats()?, command)).await,
        Command::Daemon(DaemonCommand::Daemon(args)) => Box::pin(daemon::run(&conn, *args)).await,
        Command::Mcp { socket, token } => ants_daemon::mcp::bridge_stdio(&socket, &token).await,
        Command::Harness(command) => harness::run(command).await,
        Command::Doctor(args) => doctor::run(&conn, args).await,
        Command::Auth(command) => auth::run(&conn, command).await,
        Command::Add(args) => Box::pin(add::add(&conn, *args)).await,
        Command::Retire(args) => retire::run(&conn, *args).await,
        Command::Login(args) => login::run(args).await,
        Command::Renew(args) => renew::run(&conn, args).await,
        Command::Keys(command) => keys::run(&conn, command).await,
        Command::Signer(args) => signer::run(&conn, args).await,
        Command::Enroll(command) => enroll::run(&conn, command).await,
        Command::Leaf(command) => leaf::run(&conn, command).await,
        Command::Bridge(command) => bridge::run(command).await,
        Command::Hook(command) => hook::run(&nats()?, command).await,
        Command::Webhook(command) => webhook::run(&conn, command).await,
    }
}
