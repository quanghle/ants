use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use crate::{Envelope, REACTIONS_BUCKET, REACTIONS_REPEAT};
use crate::{Error, Reaction, UserId, reaction_key, subject};

/// A reaction in [`REACTIONS_BUCKET`], under its [`reaction_key`]: `by` reacted `reaction` to message `message`
/// ([`Envelope::id`]). Readers take the reactor, message and reaction from the key, which credentials pin to the
/// reactor, not from the entry, which says when as well.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionEntry {
    pub message: Uuid,
    pub by: UserId,
    pub reaction: Reaction,
    pub at: DateTime<Utc>,
    /// The human whose assistant, `by`, reacted for it ([`crate::Acting`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
}

impl ReactionEntry {
    /// `by`'s `reaction` to `message`, made now.
    pub fn new(by: UserId, message: Uuid, reaction: Reaction) -> Self {
        Self { message, by, reaction, at: Utc::now(), on_behalf_of: None }
    }

    /// The same, made for `human` by its assistant ([`ReactionEntry::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
    }

    /// The KV key of the entry.
    pub fn key(&self) -> String {
        reaction_key(&self.by, &self.message, &self.reaction)
    }
}

/// One reaction to a message and who reacted so, in order of name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reacted {
    pub reaction: Reaction,
    pub by: Vec<UserId>,
}

/// An ant's request for the reactions to message `message`, on [`subject::reactions`], or, with a `change`, to add or
/// remove its own reaction to it, on [`subject::react`]: core NATS, not stored. Only humans read the reactions bucket,
/// and no ant writes it, so a human's `ants webhook serve` or `ants signer` answers a request for the reactions, and
/// only `ants signer`, whose credentials write any user's reactions, a change; each only after checking that the ant
/// reads the message: its first copy in the stream is on a subject the ant's credentials read
/// ([`subject::readable_by`]). The ant repeats the request with the same id while no answer arrives
/// ([`REACTIONS_REPEAT`]); the answer goes to [`subject::reactions_reply`], which only the ant reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionsRequest {
    pub id: Uuid,
    /// The ant asking.
    pub ant: UserId,
    /// The message whose reactions it asks for ([`Envelope::id`], or that of a notes or channel post).
    pub message: Uuid,
    pub requested_at: DateTime<Utc>,
    /// The change to the ant's own reactions to the message it asks for; none to only list them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change: Option<ReactionChange>,
}

/// A reaction an ant adds to a message, or removes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionChange {
    pub reaction: Reaction,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub remove: bool,
    /// The human the ant, its assistant, reacts for; the signer keeps it in the entry ([`ReactionEntry::on_behalf_of`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
}

impl ReactionChange {
    /// Add `reaction`, or with `remove` remove it.
    pub fn new(reaction: Reaction, remove: bool) -> Self {
        Self { reaction, remove, on_behalf_of: None }
    }

    /// The same, made for `human` by its assistant ([`ReactionChange::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
    }
}

impl ReactionsRequest {
    /// `ant`'s request for the reactions to `message`.
    pub fn new(ant: UserId, message: Uuid) -> Self {
        Self { id: Uuid::now_v7(), ant, message, requested_at: Utc::now(), change: None }
    }

    /// `ant`'s request to make `change` to its reactions to `message`.
    pub fn change(ant: UserId, message: Uuid, change: ReactionChange) -> Self {
        Self { change: Some(change), ..Self::new(ant, message) }
    }

    /// Where the request goes: [`subject::react`] with a change, else [`subject::reactions`].
    pub fn subject(&self) -> String {
        match self.change {
            Some(_) => subject::react(&self.ant),
            None => subject::reactions(&self.ant),
        }
    }

    /// Where the answer goes.
    pub fn reply_subject(&self) -> String {
        subject::reactions_reply(&self.ant)
    }

    /// Check that the request arrived on its own [`ReactionsRequest::subject`]. Credentials pin each ant to its own,
    /// while the body is whatever the publisher wrote, so a request naming another ant is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "reactions request", user: self.ant.clone(), at: subject.to_owned() })
    }
}

/// The answer to a [`ReactionsRequest`], on [`subject::reactions_reply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactionsReply {
    pub request_id: Uuid,
    pub ant: UserId,
    pub message: Uuid,
    pub outcome: ReactionsOutcome,
    /// The kind of process that answered; absent from the answers of older ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responder: Option<ReactionsResponder>,
}

/// The kind of process answering a [`ReactionsRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReactionsResponder {
    /// `ants signer`, which answers changes as well.
    Signer,
    /// A `[[human]]` connection of `ants webhook serve`, which answers only requests for the reactions.
    Webhook,
}

impl ReactionsReply {
    pub fn subject(&self) -> String {
        subject::reactions_reply(&self.ant)
    }

    /// Check that the answer arrived on its own [`ReactionsReply::subject`].
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::ForeignUser { kind: "reactions answer", user: self.ant.clone(), at: subject.to_owned() })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ReactionsOutcome {
    /// The reactions to the message, in order of reaction; none when nobody reacted.
    Listed { reactions: Vec<Reacted> },
    /// The change asked for is made: `changed` says whether it changed anything, adding a reaction the ant made
    /// already or removing one it did not make changing nothing.
    Applied { changed: bool },
    /// No message the ant reads has that id, as far as the stream keeps it. The same whether there is no such
    /// message or the ant may not read it, so the answer tells nothing of messages the ant does not read. A change
    /// asked for is not made.
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    #[test]
    fn reactions_requests_and_answers_match_the_ants_own_subjects() {
        let request = ReactionsRequest::new(p("dev-a"), Uuid::now_v7());
        assert_eq!(request.subject(), "ants.reactions.dev-a");
        assert_eq!(request.reply_subject(), "_INBOX.dev-a.reactions");
        assert_eq!(request.check_subject("ants.reactions.dev-a"), Ok(()));
        assert!(request.check_subject("ants.reactions.dev-b").is_err());
        let reply = ReactionsReply {
            request_id: request.id,
            ant: request.ant.clone(),
            message: request.message,
            outcome: ReactionsOutcome::Unknown,
            responder: None,
        };
        assert_eq!(reply.check_subject(&request.reply_subject()), Ok(()));
        assert!(reply.check_subject("_INBOX.dev-b.reactions").is_err());
        assert_eq!(serde_json::to_value(&reply.outcome).unwrap(), serde_json::json!({"outcome": "unknown"}));

        let change = ReactionChange::new("eyes".parse().unwrap(), false);
        let react = ReactionsRequest::change(p("dev-a"), Uuid::now_v7(), change);
        assert_eq!(react.subject(), "ants.reactions.dev-a.react");
        assert_eq!(react.reply_subject(), request.reply_subject());
        assert!(react.check_subject("ants.reactions.dev-a").is_err(), "a change goes on its own subject");
        assert!(request.check_subject("ants.reactions.dev-a.react").is_err(), "and a listing on its own");
    }
}
