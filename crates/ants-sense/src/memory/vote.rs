//! `sense_vote`: a thread's feedback on a memory, helpful, stale or wrong, weighing in its ranking
//! ([`super::score`]). A stale vote marks the memory [`Status::NeedsReview`], still recalled; a wrong one retires it as
//! [`Status::Wrong`]; a helpful one on a memory needing review makes it active again and counts its recency from then.
//!
//! Each thread's vote on a memory is an item of kind [`VOTE`], named after the memory and the thread, holding the
//! vote, its reason and its writer; the memory counts the votes ([`Votes`]). Both are written in one event, at the
//! revisions read, so concurrent votes are counted again rather than lost. A thread voting again replaces its vote:
//! each thread counts once. Threads a webhook started may not vote.

use std::fmt::{self, Write as _};
use std::str::FromStr;

use ants_proto::ThreadId;
use ants_proto::provider::{BoxFuture, CallContext, ToolCall};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{MemoryItem, Status};
use crate::Sense;
use crate::items::{Item, ItemId, Values, values_of};
use crate::sanitize::{Limit, sanitize_title};
use crate::store::{Write, read_modify_write};
use crate::tools::{Answer, MAX_REASON_CHARS};

/// The kind of the items holding votes.
pub const VOTE: &str = "vote";

/// A thread's feedback on a memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Vote {
    Helpful,
    Stale,
    Wrong,
}

impl Vote {
    pub const ALL: [Self; 3] = [Self::Helpful, Self::Stale, Self::Wrong];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Helpful => "helpful",
            Self::Stale => "stale",
            Self::Wrong => "wrong",
        }
    }
}

impl FromStr for Vote {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        Self::ALL.into_iter().find(|vote| vote.name() == s).ok_or_else(|| format!("no vote {s:?}"))
    }
}

impl fmt::Display for Vote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The votes on a memory, each thread's last one counted.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Votes {
    #[serde(default, skip_serializing_if = "is_zero")]
    pub helpful: u32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub stale: u32,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub wrong: u32,
    /// When it was last voted helpful, from when its recency counts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed: Option<DateTime<Utc>>,
}

impl Votes {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn count(&mut self, vote: Vote) -> &mut u32 {
        match vote {
            Vote::Helpful => &mut self.helpful,
            Vote::Stale => &mut self.stale,
            Vote::Wrong => &mut self.wrong,
        }
    }
}

fn is_zero(count: &u32) -> bool {
    *count == 0
}

/// A thread's vote on a memory, as its item holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Ballot {
    memory: ItemId,
    vote: Vote,
    reason: Option<String>,
    thread: ThreadId,
    writer: CallContext,
}

impl Ballot {
    fn of(item: &Item) -> Option<Self> {
        serde_json::from_value(Value::Object(item.values.clone())).ok()
    }
}

/// Answers a `sense_vote` call.
pub fn vote<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { vote_in(sense, call).await.unwrap_or_else(|refused| refused) })
}

/// The arguments of a `sense_vote` call.
#[derive(Deserialize)]
struct Request {
    id: ItemId,
    vote: Vote,
    reason: Option<String>,
}

async fn vote_in(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let Request { id, vote, reason } = crate::tools::arguments(call)?;
    let reason = match reason {
        Some(reason) => {
            sanitize_title(reason.trim(), Limit::Chars(MAX_REASON_CHARS))
                .map_err(|refused| Answer::error("too_long", format!("The reason is too long: {refused}.")))?
                .text
        }
        None => String::new(),
    };
    let context = &call.context;
    if !context.trusted() {
        let message = "Votes are not taken in a thread a webhook started: outside content may steer them.";
        return Err(Answer::error("untrusted", message.into()));
    }
    let items = sense.items(&context.ant).await?;
    let (items, id, reason) = (&*items, &id, &reason);
    let ballot = &ItemId::named(VOTE, &format!("{id}/{}", context.thread));
    read_modify_write(|| async move {
        let Some(found) = items.get(id).await?.and_then(MemoryItem::of) else {
            return Ok(Err(Answer::error("not_found", format!("No memory {id}."))));
        };
        let memory = &found.memory;
        if !memory.status.live() {
            let by = memory.superseded_by.as_ref().map_or_else(String::new, |by| format!(" by {by}"));
            let message = format!("Memory {id} is {}{by}: vote on a live one.", memory.status.name());
            return Ok(Err(Answer::error("conflict", message)));
        }
        let cast = items.get(ballot).await?;
        let previous = cast.as_ref().and_then(Ballot::of).map(|ballot| ballot.vote);
        let mut votes = memory.votes.clone();
        if let Some(previous) = previous {
            let count = votes.count(previous);
            *count = count.saturating_sub(1);
        }
        *votes.count(vote) += 1;
        let status = match vote {
            Vote::Wrong => Status::Wrong,
            Vote::Stale => Status::NeedsReview,
            Vote::Helpful => {
                votes.confirmed = Some(context.at);
                Status::Active
            }
        };
        let record = Ballot {
            memory: id.clone(),
            vote,
            reason: (!reason.is_empty()).then(|| reason.clone()),
            thread: context.thread.clone(),
            writer: context.clone(),
        };
        let mut changed = Values::new();
        changed.insert("votes".into(), json!(votes));
        changed.insert("status".into(), json!(status));
        let writes = [
            Write::Put {
                id: ballot.clone(),
                kind: VOTE.into(),
                values: values_of(&record),
                expected: Some(cast.as_ref().map_or(0, |cast| cast.revision)),
            },
            Write::Change { id: id.clone(), values: changed, expected: Some(found.item.revision) },
        ];
        let written = items.write(&writes, context.thread.as_str()).await?;
        Ok(Ok(voted(&found, vote, previous, status, &votes, written[1].revision)))
    })
    .await?
}

fn voted(
    found: &MemoryItem,
    vote: Vote,
    previous: Option<Vote>,
    status: Status,
    votes: &Votes,
    revision: u64,
) -> Answer {
    let (id, title) = (found.id(), &found.memory.title);
    let mut text = format!(
        "Voted {vote} on {id} \"{title}\": {} helpful, {} stale, {} wrong.",
        votes.helpful, votes.stale, votes.wrong
    );
    match previous {
        Some(previous) if previous == vote => text.push_str("\nThis thread voted so already: it counts once."),
        Some(previous) => {
            let _ = write!(text, "\nReplaces this thread's {previous} vote.");
        }
        None => {}
    }
    match (found.memory.status, status) {
        (_, Status::Wrong) => text.push_str("\nRetired as wrong: recall leaves it out. Remember what holds instead."),
        (Status::Active, Status::NeedsReview) => text.push_str(
            "\nMarked needs_review: check it against the current code, then supersede it or vote it helpful.",
        ),
        (Status::NeedsReview, Status::Active) => text.push_str("\nActive again."),
        _ => {}
    }
    let json = json!({
        "id": id,
        "vote": vote,
        "replaced": previous,
        "status": status,
        "votes": votes,
        "revision": revision,
    });
    Answer { text, json, is_error: false }
}

#[cfg(test)]
mod tests;
