//! The ant's redactor and the redacting wrappers of what it publishes ([`crate::secrets::redact`]).

use ants_proto::{ChannelPost, Envelope, ThreadId};
use anyhow::Result;

use crate::escalation;
use crate::secrets::redact::Redactor;
use crate::stream::notes::{self, NotesStore};

use super::settings::HarnessSettings;

/// What hides the ant's secrets: the values of its env and its harness's, the daemon's environment variables named
/// like secrets, and the common patterns; nothing with `redact = false`.
pub(super) fn redactor(redact: bool, harness: &HarnessSettings) -> Redactor {
    if !redact {
        return Redactor::off();
    }
    Redactor::default().with_env(&harness.env).with_env(&harness.harness.env).with_process_env()
}

/// `outbox`, redacting the text of what the ant sends with its MCP tools.
pub(super) fn redacting_outbox(outbox: crate::mcp::Outbox, redactor: std::sync::Arc<Redactor>) -> crate::mcp::Outbox {
    std::sync::Arc::new(move |mut envelope: Envelope| {
        redactor.redact_string(&mut envelope.text);
        outbox(envelope)
    })
}

/// `channels`, redacting the text of what the ant posts with its MCP tools.
pub(super) fn redacting_posts(
    channels: crate::mcp::ChannelAccess,
    redactor: std::sync::Arc<Redactor>,
) -> crate::mcp::ChannelAccess {
    let post = channels.post;
    crate::mcp::ChannelAccess {
        post: std::sync::Arc::new(move |mut posted: ChannelPost, source| {
            redactor.redact_string(&mut posted.text);
            post(posted, source)
        }),
        ..channels
    }
}

/// `sender`, redacting the text of the escalations it stores.
pub(super) fn redacting_sender(sender: escalation::Sender, redactor: std::sync::Arc<Redactor>) -> escalation::Sender {
    std::sync::Arc::new(move |mut envelope: Envelope, id| {
        redactor.redact_string(&mut envelope.text);
        sender(envelope, id)
    })
}

/// `sender`, redacting the text of the notes posts it stores.
pub(super) fn redacting_notes_sender(
    sender: escalation::NotesSender,
    redactor: std::sync::Arc<Redactor>,
) -> escalation::NotesSender {
    std::sync::Arc::new(move |mut post: ants_proto::NotesPost, id| {
        redactor.redact_string(&mut post.text);
        sender(post, id)
    })
}

/// Thread notes whose posts are redacted before they are stored.
pub(super) struct RedactingNotes<N> {
    pub(super) notes: N,
    pub(super) redactor: std::sync::Arc<Redactor>,
}

impl<N: NotesStore> NotesStore for RedactingNotes<N> {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> futures::future::BoxFuture<'a, Result<notes::Notes>> {
        self.notes.read(thread)
    }

    fn post<'a>(
        &'a self,
        post: &'a ants_proto::NotesPost,
        last_seq: u64,
    ) -> futures::future::BoxFuture<'a, Result<bool>> {
        match self.redactor.redact(&post.text) {
            std::borrow::Cow::Borrowed(_) => self.notes.post(post, last_seq),
            std::borrow::Cow::Owned(text) => {
                let post = ants_proto::NotesPost { text, ..post.clone() };
                Box::pin(async move { self.notes.post(&post, last_seq).await })
            }
        }
    }
}
