//! `ants notify`: snooze, resume and show the notifications `ants webhook serve` pushes to the CLI's human.

use std::time::Duration;

use anyhow::Result;
use clap::Subcommand;

use crate::connection::Nats;
use crate::quiet;

#[derive(Subcommand, Debug)]
pub(crate) enum NotifyCommand {
    /// Hold back every notice for DURATION (such as 30m, 1h or 2d), but those the human's `bypass` in the webhook
    /// config lets through. Held notices are dropped, not sent later; nothing is marked read or decided.
    Snooze {
        #[arg(value_name = "DURATION", value_parser = ants_proto::duration::parse)]
        duration: Duration,
    },
    /// End a snooze.
    Resume,
    /// Show the snooze, the muted threads and the channels' notify levels (quiet hours are in the webhook config).
    Status {
        /// One JSON object.
        #[arg(long)]
        json: bool,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: NotifyCommand) -> Result<()> {
    match command {
        NotifyCommand::Snooze { duration } => quiet::snooze(nats, duration).await,
        NotifyCommand::Resume => quiet::resume(nats).await,
        NotifyCommand::Status { json } => quiet::status(nats, json).await,
    }
}
