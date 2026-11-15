//! `ants attachment`: the files attached to messages.

use std::path::PathBuf;

use ants_proto::AttachmentHandle;
use anyhow::Result;
use clap::Subcommand;

use crate::{Nats, attachments};

#[derive(Subcommand, Debug)]
pub(crate) enum AttachmentCommand {
    /// Save the file attached to a message under HANDLE, as `ants inbox`, `ants thread show`, `ants notes` and `ants
    /// channel read` list it (`📎 name (size) HANDLE`): to PATH, into PATH under its name when PATH is a directory, or
    /// to ./name. An existing file is not overwritten, and nothing is written when the bytes stored do not match the
    /// attachment's reference. The credentials read only the attachments of messages they read.
    Get {
        /// `<from>.<inbox|notes|chan>.<to, thread or channel>.<id>`.
        handle: AttachmentHandle,
        /// Where to write it.
        #[arg(long, short, value_name = "PATH")]
        output: Option<PathBuf>,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: AttachmentCommand) -> Result<()> {
    match command {
        AttachmentCommand::Get { handle, output } => attachments::get(nats, &handle, output.as_deref()).await,
    }
}
