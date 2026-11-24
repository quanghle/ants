//! A turn's answer that goes as a message, outside a channel, has one recipient: one @mentioning several users is not
//! sent, but prompted once more, the session told why ([`ants_proto::one_recipient`]); a second answer that still
//! @mentions several goes with a notice saying who alone gets it.

use ants_proto::{PromptEnd, UserId};

use super::Ran;
use crate::daemon::{Handling, Inbox};

impl Inbox<'_> {
    /// `ran`, the prompt run on the message of `h`, or the prompt run once more on its turn when its answer would go
    /// as a message @mentioning several users, its session told why the answer was not sent: the spend and end of the
    /// first are recorded as any prompt's are. Never for an answer going nowhere (`own`, or `complete` called), a
    /// cancelled one, one cut or at a cap, nor one a message joined. With the run, the notice the answer carries
    /// when it still @mentions several.
    pub(super) async fn one_recipient(&self, h: Handling<'_>, own: bool, mut ran: Ran) -> (Ran, Option<String>) {
        let envelope = &h.incoming.envelope;
        let thread = &envelope.thread_id;
        let Some(to) = self.answered_to(h.incoming) else { return (ran, None) };
        let Ok(reply) = &ran.reply else { return (ran, None) };
        let completing = self.mcp.is_some_and(|mcp| mcp.completing(thread));
        let settled = own || completing || ran.cancelled_by.is_some() || ran.reached.is_some() || reply.cut.is_some();
        if settled || !ran.joined.is_empty() {
            return (ran, None);
        }
        let Err(refused) = ants_proto::one_recipient(self.ant, &to, &reply.text, crate::mcp::POST_WITH) else {
            return (ran, None);
        };
        let rebuilt = reply.rebuilt;
        let spend = std::mem::replace(&mut ran.spend, tokio::sync::mpsc::unbounded_channel().1);
        self.record_ended(envelope, spend, &mut ran.reached, &mut ran.measured).await;
        if ran.reached.is_some() {
            let notice = still_several(self.ant, &to, ran.reply.as_ref().ok().map(|r| r.text.as_str()));
            return (ran, notice);
        }
        self.count_prompt().await;
        self.note_end(thread, &ran.measured, PromptEnd::Replied, rebuilt).await;
        tracing::info!(%thread, %to, "prompting again: the answer @mentions several users");
        let text = format!(
            "[ants] The answer of this turn was not sent. {refused} The answer goes to {to} alone: give it again \
             with no mention of any other user, once the others are reached as needed."
        );
        let flush = tokio::time::interval_at(
            tokio::time::Instant::now() + crate::monitoring::status::FLUSH_EVERY,
            crate::monitoring::status::FLUSH_EVERY,
        );
        let again = Box::pin(self.run_prompt(h, text, flush)).await;
        let notice = still_several(self.ant, &to, again.reply.as_ref().ok().map(|r| r.text.as_str()));
        (again, notice)
    }
}

/// The notice an answer `text` from `ant` to `to` carries when it still @mentions several users.
fn still_several(ant: &UserId, to: &UserId, text: Option<&str>) -> Option<String> {
    let mentioned = ants_proto::several_mentions(ant, text?)?;
    let others: Vec<String> = mentioned.iter().filter(|m| *m != to).map(|m| format!("@{m}")).collect();
    let others = match others.split_last() {
        Some((last, rest)) if !rest.is_empty() => format!("{} and {last}", rest.join(", ")),
        _ => others.concat(),
    };
    Some(format!("[ants] Only {to} receives this message, not {others}, though mentioned."))
}
