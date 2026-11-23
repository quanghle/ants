//! Reviews an ant asks of its sage: the checkouts of the commits they name, and declining one nothing would hold to
//! read-only tools.

use ants_proto::UserId;
use anyhow::Result;
use async_nats::jetstream::{self};

use crate::channel::Context;
use crate::checkouts::Prepared;

use super::{Handling, Inbox, Incoming, Steered};

impl Inbox<'_> {
    /// Prompt `text` telling of the checkouts of the commits `incoming` names, with the checkouts, to keep while the
    /// prompt lasts, when it is a review an ant asked for: a message, not a post or a webhook's, narrowed to
    /// [`ants_proto::REVIEW_ALLOW`] ([`crate::checkouts`]). `text` alone for one `joining` a running prompt.
    pub(super) async fn review_checkouts(
        &self,
        incoming: &Incoming,
        msg: &jetstream::Message,
        joining: bool,
        text: String,
    ) -> (String, Option<Prepared>) {
        let review = !joining && self.asks_review(incoming);
        let Some(checkouts) = self.pool.checkouts().filter(|_| review) else { return (text, None) };
        let prepared = self.acking(msg, checkouts.prepare(&incoming.envelope)).await;
        (format!("{text}\n\n{}", prepared.note()), Some(prepared))
    }

    /// Whether `incoming` is a review an ant asked for: a message, not a post or a webhook's, narrowed to
    /// [`ants_proto::REVIEW_ALLOW`].
    pub(super) fn asks_review(&self, incoming: &Incoming) -> bool {
        let envelope = &incoming.envelope;
        incoming.post.is_none()
            && !envelope.webhook
            && !self.humans.sender_role(envelope).is_human()
            && ants_proto::is_review(envelope.allow.as_deref())
    }

    /// The live ant naming `ant` as its sage, if any: `ant` is a sage then. None when the colony cannot be read.
    pub(super) async fn sage_named(&self, ant: &UserId) -> Option<UserId> {
        let entries = (self.colony)()
            .await
            .inspect_err(|e| tracing::warn!(error = format!("{e:#}"), "reading the colony; the review goes on"))
            .ok()?;
        crate::colony::naming_as_sage(ant, &entries, chrono::Utc::now()).map(|entry| entry.ant.clone())
    }

    /// Why `incoming` is a review ([`ants_proto::REVIEW_ALLOW`]) that nothing would hold to read-only tools, unless it
    /// joins a prompt running.
    pub(super) fn unheld_review(&self, incoming: &Incoming) -> Option<String> {
        let review = incoming.steered != Steered::Joining && ants_proto::is_review(incoming.envelope.allow.as_deref());
        review.then(|| self.pool.launch().unheld_review()).flatten()
    }

    /// Answer a review with a decline explaining `why`, unprompted: its sender then goes on without the review.
    pub(super) async fn decline_review(
        &self,
        h @ Handling { incoming, seq, .. }: Handling<'_>,
        read: Option<Context>,
        why: &str,
    ) -> Result<()> {
        let envelope = &incoming.envelope;
        tracing::warn!(thread = %envelope.thread_id, from = %envelope.from, why, "declining a review");
        let text = format!("[ants] {} declines the review: {why}.", self.ant);
        let Some(reply) = Box::pin(self.reply_for(h, read, (text, None), &mut None, None)).await? else {
            return Ok(());
        };
        if let Err(e) = self.marks.attempts.replied(seq, envelope, &reply).await {
            tracing::warn!(thread = %envelope.thread_id, error = format!("{e:#}"), "recording the reply");
        }
        self.deliver(h, &reply).await
    }
}
