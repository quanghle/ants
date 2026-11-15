use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::{Envelope, REACTIONS_BUCKET};
use crate::{Reaction, UserId, reaction_key};

/// A reaction in [`REACTIONS_BUCKET`], under its [`reaction_key`]: `by` reacted `reaction` to message `message`
/// ([`Envelope::id`]). Readers take the reactor, message and reaction from the key, which credentials pin to the
/// reactor, not from the entry, which says when as well.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionEntry {
    pub message: Uuid,
    pub by: UserId,
    pub reaction: Reaction,
    pub at: DateTime<Utc>,
}

impl ReactionEntry {
    /// `by`'s `reaction` to `message`, made now.
    pub fn new(by: UserId, message: Uuid, reaction: Reaction) -> Self {
        Self { message, by, reaction, at: Utc::now() }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> String {
        reaction_key(&self.by, &self.message, &self.reaction)
    }
}
