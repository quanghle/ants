//! The messages the inbox answers, as it holds them.

use ants_proto::{Attachment, AttachmentHandle, ChannelEntry, ChannelId, ChannelPost, Envelope};
use async_nats::jetstream;

pub(super) type InHand = (Incoming, jetstream::Message);

/// A message being handled: what it says, as it arrived on the stream, and its stream sequence.
#[derive(Clone, Copy)]
pub(super) struct Handling<'a> {
    pub(super) incoming: &'a Incoming,
    pub(super) msg: &'a jetstream::Message,
    pub(super) seq: u64,
}

/// A message to answer: one in the inbox, or a channel post that wakes the ant, read as a message from its author
/// ([`crate::channel::as_envelope`]).
#[derive(Clone)]
pub(super) struct Incoming {
    pub(super) envelope: Envelope,
    pub(super) post: Option<Box<Posted>>,
    /// The channel the answer goes to instead of the sender's inbox: that of the thread, when the message is the
    /// escalation leader's reply that renewed it ([`super::Inbox::arrived`]).
    pub(super) resume: Option<ChannelId>,
    /// How the message steered its thread.
    pub(super) steered: Steered,
}

/// Whether a message steers its thread: one that does goes ahead of the messages waiting there, after those that
/// steered it before.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum Steered {
    /// An ordinary message, waiting its turn.
    #[default]
    No,
    /// It cancelled the prompt running on its thread as it arrived.
    Interrupted,
    /// It arrived behind another steer that cancelled the running prompt already, or the prompt it was to join ended
    /// before taking it.
    Ahead,
    /// It joins the prompt running on its thread, whose harness takes it into its turn ([`super::steering`]).
    Joining,
}

impl Incoming {
    /// The channel of the thread the message came on, as its origin names it: a post's, or the channel of a thread it
    /// renewed ([`Incoming::resume`]).
    pub(super) fn channel(&self) -> Option<&ChannelId> {
        self.post.as_ref().map(|posted| &posted.post.channel).or(self.resume.as_ref())
    }

    /// The files attached to the message, each with its handle: a channel post's under the post's channel, as the
    /// envelope made of it carries none ([`crate::channel::as_envelope`]).
    pub(super) fn attached(&self) -> Vec<(AttachmentHandle, Attachment)> {
        match &self.post {
            Some(posted) => {
                let post = &posted.post;
                post.attachments.iter().map(|a| (post.attachment_handle(a.id), a.clone())).collect()
            }
            None => {
                let envelope = &self.envelope;
                envelope.attachments.iter().map(|a| (envelope.attachment_handle(a.id), a.clone())).collect()
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct Posted {
    pub(super) post: ChannelPost,
    /// The channel's entry, when the ant is to triage the post as its messenger.
    pub(super) triage: Option<ChannelEntry>,
    /// Whether the ant owns the channel.
    pub(super) owned: bool,
    /// What the channel is for, as its entry said when the post arrived.
    pub(super) description: Option<String>,
}
