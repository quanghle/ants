//! `ants`: talk to agent teams over NATS from the terminal, and run the ant daemon.

#![cfg_attr(
    all(any(test, debug_assertions), target_os = "macos"),
    allow(linker_messages, reason = "a debug binary's unwind tables outgrow what ld64 encodes compactly (16 MB)")
)]
#![expect(rustdoc::invalid_html_tags, reason = "clap help text names placeholders as <dir>")]
#![cfg_attr(test, expect(clippy::disallowed_methods, reason = "tests read and hear as they please"))]

// The command line, and how it reaches NATS.
mod buckets;
mod cas;
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
mod sense_id;
mod spend;
mod tools;

// The full-screen views.
mod avatars;
mod live;
mod tty;
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
mod trails;

#[cfg(test)]
mod test_support;

use anyhow::Result;

/// Counts live bytes for a trail worker's memory limit.
#[global_allocator]
static ALLOCATOR: ants_trail::Allocator = ants_trail::Allocator;

fn main() -> Result<()> {
    // A trail worker, `ants subprocess`, serves one request before anything else starts.
    if ants_trail::worker::asked() {
        std::process::exit(ants_trail::worker::main());
    }
    completions::answer();
    let result = run();
    if let Err(e) = &result
        && e.downcast_ref::<trails::follow::Unread>().is_some()
    {
        eprintln!("Error: {e:?}");
        std::process::exit(trails::follow::UNREAD_STATUS);
    }
    result
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
