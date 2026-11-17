//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

mod add;
mod age;
mod ant;
mod assistant;
mod attachments;
mod auth;
mod avatars;
mod backup;
mod behalf;
mod binding;
mod bridge;
mod channel;
mod cli;
mod cmd;
mod colony;
mod completions;
mod compose;
mod connection;
mod defaults;
mod devices;
mod doctor;
mod enroll;
mod export;
mod forward;
mod github;
mod http;
mod humans;
mod inbox;
mod keys;
mod leaf;
mod live;
mod machines;
mod manager;
mod mcp;
mod members;
mod memories;
mod natsd;
mod notify;
mod orders;
mod outbox;
mod pause;
mod presence;
mod progress;
mod queue;
mod quiet;
mod reacted;
mod registrar;
mod reload;
mod remind;
mod renew;
mod restart;
mod retire;
mod revoke;
mod scheduler;
mod sealed;
mod search;
mod serve;
mod service;
mod settings;
mod signer;
mod spend;
mod summary;
mod text;
mod thread;
mod thread_meta;
mod tools;
mod tui;
mod update;
mod upgrade;
mod versions;
mod webhook;

#[cfg(test)]
mod test_support;

use anyhow::Result;

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
    #[cfg(windows)]
    update::remove_old();
    cmd::run(defaults::parse()?).await
}
