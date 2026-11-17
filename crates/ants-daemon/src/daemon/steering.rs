//! Messages joining the prompt running on their thread, for a harness that takes them into its turn (`_ants/steer`):
//! the last message to join gets the prompt's answer, and those before it, the one the prompt started with first, a
//! notice of where the answer went.

use std::collections::HashMap;
use std::fmt;

use ants_proto::{AttachmentHandle, Envelope, ThreadId, UserId, Uuid};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};

use crate::sync::MutexExt as _;

use super::prompt::Ran;

/// The prompts running on threads whose harness takes steers, and where to send the messages joining each.
#[derive(Default)]
pub(super) struct Steers(std::sync::Mutex<HashMap<ThreadId, mpsc::UnboundedSender<Join>>>);

/// A message joining the prompt running on its thread.
pub(super) struct Join {
    pub(super) envelope: Envelope,
    /// The handles of the files attached to it, which the hub stored at `stored`.
    pub(super) carried: Vec<AttachmentHandle>,
    pub(super) stored: Option<DateTime<Utc>>,
    /// What the prompt is steered with.
    pub(super) text: String,
    /// What came of it; dropped when the prompt failed, the message then waiting its turn again.
    pub(super) joined: oneshot::Sender<Joined>,
}

/// What came of a message joining a running prompt.
pub(super) enum Joined {
    /// The prompt did not take it: it waits for a prompt of its own.
    Refused,
    /// The prompt's answer is its own, to send while the turn of the message the prompt started with lasts: until the
    /// sender drops.
    Answer(Box<Ran>, oneshot::Sender<()>),
    /// A later message joined the prompt too: the sender and id of the next one, which the answer went towards.
    Superseded(UserId, Uuid),
}

impl Steers {
    /// Take the messages joining the prompt about to run on `thread` until the guard drops; they arrive on the receiver.
    pub(super) fn register<'a>(&'a self, thread: &ThreadId) -> (Joinable<'a>, mpsc::UnboundedReceiver<Join>) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.locked().insert(thread.clone(), tx);
        (Joinable { steers: self, thread: thread.clone() }, rx)
    }

    /// Whether the prompt running on `thread` takes messages joining it.
    pub(super) fn takes(&self, thread: &ThreadId) -> bool {
        self.0.locked().get(thread).is_some_and(|tx| !tx.is_closed())
    }

    /// Send `join` to the prompt running on its thread: false when none takes it.
    pub(super) fn join(&self, join: Join) -> bool {
        let steers = self.0.locked();
        steers.get(&join.envelope.thread_id).is_some_and(|tx| tx.send(join).is_ok())
    }
}

pub(super) struct Joinable<'a> {
    steers: &'a Steers,
    thread: ThreadId,
}

impl Drop for Joinable<'_> {
    fn drop(&mut self) {
        self.steers.0.locked().remove(&self.thread);
    }
}

/// A message that steered its thread's prompt but did not join it: it waits for a prompt of its own, as it arrived.
#[derive(Debug)]
pub(super) struct Requeue;

impl fmt::Display for Requeue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the running prompt did not take the message; it waits its turn")
    }
}

impl std::error::Error for Requeue {}
