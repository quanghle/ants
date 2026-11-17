//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

#![cfg_attr(
    all(test, target_os = "macos"),
    allow(linker_messages, reason = "the test binary's unwind tables outgrow what ld64 encodes compactly (16 MB)")
)]

// The command line, and how it reaches NATS.
mod cli;
mod cmd;
mod completions;
mod connection;
mod defaults;
mod http;
mod text;

// Ants, channels and the colony.
mod add;
mod ant;
mod channel;
mod colony;
mod export;
mod machines;
mod queue;
mod spend;
mod tools;

// The full-screen views.
mod avatars;
mod live;
mod tui;

// Services: `ants daemon` and its schedules, the services' units and reloads, webhooks and notifications.
mod notify;
mod reload;
mod scheduler;
mod serve;
mod service;
mod webhook;

// Setting up and checking a team.
mod auth;
mod doctor;

mod admin;
mod credentials;
mod messages;
mod node;
mod people;
mod release;
mod signing;
mod threads;

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
        .with_writer(messages::progress::log_writer)
        .init();
    #[cfg(windows)]
    release::update::remove_old();
    cmd::run(defaults::parse()?).await
}
