//! The [`NotesPost`] on a thread's shared notes ([`subject::notes`]).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(doc)]
use super::Envelope;
use crate::{Attachment, DEFAULT_TURN_BUDGET, Error, ThreadId, UserId, subject};

/// A post to the notes of a thread, on [`subject::notes`]: one shared board per thread, where the ants on it coordinate
/// together; a post wakes nobody, and each of them can read the whole board.
///
/// Posts spend the thread's turn budget like messages between ants do, and each carries what is left. The post that
/// spends the last turn hands the thread to the human, and the notes take no more ant posts until the human posts
/// there with a fresh budget.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotesPost {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    pub text: String,
    /// Turns left in the notes after this post.
    pub turn_budget: u32,
    /// The title the posting ant suggests for the thread, as [`Envelope::title`]: an ant's reply posted to the notes
    /// carries the one its MCP tool suggest_thread_title took.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The files attached to the post, stored before it under its [`NotesPost::attachment_handle`]s.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<Attachment>,
    /// The human the author, its assistant, posts for, as [`Envelope::on_behalf_of`]: a reader honours it only as
    /// [`crate::Acting`] tells.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<UserId>,
    pub sent_at: DateTime<Utc>,
}

impl NotesPost {
    /// A post with a fresh budget, as the human posts.
    pub fn new(thread_id: ThreadId, from: UserId, text: impl Into<String>) -> Self {
        Self {
            id: Uuid::now_v7(),
            thread_id,
            from,
            text: text.into(),
            turn_budget: DEFAULT_TURN_BUDGET,
            title: None,
            attachments: Vec::new(),
            on_behalf_of: None,
            sent_at: Utc::now(),
        }
    }

    /// The same, posted for `human` by its assistant ([`NotesPost::on_behalf_of`]).
    #[must_use]
    pub fn on_behalf(self, human: Option<UserId>) -> Self {
        Self { on_behalf_of: human, ..self }
    }

    /// An ant's post, spending one turn of what is left: the lower of `budget`, the turns of the message the ant
    /// is handling, and those the notes' `last` post left. Fails when none are left.
    pub fn spend(
        thread_id: ThreadId,
        from: UserId,
        text: impl Into<String>,
        budget: u32,
        last: Option<&NotesPost>,
    ) -> Result<Self, Error> {
        let left = last.map_or(budget, |last| last.turn_budget.min(budget));
        let turn_budget = left.checked_sub(1).ok_or_else(|| Error::BudgetExhausted(thread_id.clone()))?;
        Ok(Self { turn_budget, ..Self::new(thread_id, from, text) })
    }

    pub fn subject(&self) -> String {
        subject::notes(&self.thread_id, &self.from)
    }

    /// Check that the post arrived on its own [`NotesPost::subject`]. Credentials pin the sender token of a notes
    /// subject, so a `from` that disagrees with it is forged.
    pub fn check_subject(&self, subject: &str) -> Result<(), Error> {
        if subject == self.subject() {
            return Ok(());
        }
        Err(Error::WrongNotesSubject {
            subject: subject.to_owned(),
            from: self.from.clone(),
            thread: self.thread_id.clone(),
        })
    }
}
