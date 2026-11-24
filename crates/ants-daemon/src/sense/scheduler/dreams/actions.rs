//! What was done with the ant's threads besides writing on them, for its review packets ([`Action`]): a thread's
//! marking done or explicitly undone, read by direct get as the ant's credentials allow, and the first message, note
//! or channel post stored after it was marked done ([`history::Active::reopenings`]), which reopened it; the ant's
//! tool requests rejected, from the timeline it reads back ([`history::Active::rejections`]); and the reactions to its
//! messages and channel posts, asked as `list_reactions` asks them ([`ReactionAsker`]). Each is read within [`TIMEOUT`]
//! in all, and left out when it fails.

use ants_proto::{
    DecidedBy, JsDomain, MergedRejection, ReactionsOutcome, ReactionsRequest, THREAD_UNDONE, THREADS_BUCKET,
    ThreadDone, ThreadId, UserId, done_key,
};
use ants_sense::dream::Action;
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use tokio::time::{Duration, Instant, timeout_at};

use crate::mcp::ReactionAsker;
use crate::records::direct_kv::{DirectKv, Lookup};
#[cfg(doc)]
use crate::stream::history;
use crate::stream::history::{Reopening, Sent};

/// How long reading what was done with the threads of a packet may take in all.
const TIMEOUT: Duration = Duration::from_secs(30);
/// The most of the ant's latest messages and channel posts on a thread whose reactions are asked for.
const REACTED: usize = 8;

/// Reads what was done with each thread of a packet. A source that fails, answers nothing or runs out of time is read
/// no more: its lines are left out of the threads after as well.
pub(super) struct Actions<'a> {
    ant: &'a UserId,
    /// The threads' done states; none once a read failed.
    done: Option<DirectKv>,
    /// Asks for the reactions to a message; none once a request failed.
    reactions: Option<&'a ReactionAsker>,
    deadline: Instant,
}

impl<'a> Actions<'a> {
    /// Reading as `ant` on `js`, made for `js_domain`, and asking `reactions`.
    pub(super) async fn new(
        js: &jetstream::Context,
        js_domain: Option<&JsDomain>,
        ant: &'a UserId,
        reactions: Option<&'a ReactionAsker>,
    ) -> Self {
        let deadline = Instant::now() + TIMEOUT;
        let done = within(deadline, DirectKv::new(js, js_domain, THREADS_BUCKET)).await;
        let done = done.inspect_err(|e| tracing::warn!(error = e, "reading when threads were marked done")).ok();
        Self { ant, done, reactions, deadline }
    }

    /// What was done with thread `id` from `since` to `until` besides writing on it: the tool requests `rejections` of
    /// the ant rejected, the reactions to `sent`, its messages and channel posts on it, and its marking done, then the
    /// first of `reopenings`, the entries read back on it, stored after it.
    pub(super) async fn of(
        &mut self,
        id: &ThreadId,
        sent: &[Sent],
        rejections: &[MergedRejection],
        reopenings: &[Reopening],
        (since, until): (DateTime<Utc>, DateTime<Utc>),
    ) -> Vec<Action> {
        let between = |at: &DateTime<Utc>| (since..=until).contains(at);
        let mut actions: Vec<Action> = rejections.iter().filter(|r| between(&r.at)).map(rejected).collect();
        actions.extend(self.reactions(sent).await);
        if let Some(mark) = self.done(id).await {
            match mark {
                Mark::Done(done) if between(&done.at) => {
                    actions.push(done_by(&done));
                    if let Some(reopening) = reopenings.iter().find(|reopening| !done.holds(reopening.seq)) {
                        let article = if reopening.kind.starts_with("unreadable ") { "an" } else { "a" };
                        let by = format!("{article} {} from {}", reopening.kind, reopening.from);
                        actions.push(Action::Reopened { at: reopening.at.into(), by });
                    }
                }
                Mark::Undone(at) if between(&at) => actions.push(Action::Undone { at }),
                _ => {}
            }
        }
        actions
    }

    /// The reactions to the latest of `sent` but the ant's own, oldest first.
    async fn reactions(&mut self, sent: &[Sent]) -> Vec<Action> {
        let mut actions = Vec::new();
        for message in &sent[sent.len().saturating_sub(REACTED)..] {
            let Some(asker) = self.reactions else { break };
            let asked = within(self.deadline, asker(ReactionsRequest::new(self.ant.clone(), message.id))).await;
            let reactions = match asked {
                Ok(Some(ReactionsOutcome::Listed { reactions })) => reactions,
                Ok(Some(ReactionsOutcome::Unknown | ReactionsOutcome::Applied { .. })) => continue,
                Ok(None) => {
                    tracing::warn!("no answer about the reactions to the ant's messages: none in its review packet");
                    self.reactions = None;
                    continue;
                }
                Err(error) => {
                    tracing::warn!(error, "asking for the reactions to the ant's messages: none in its review packet");
                    self.reactions = None;
                    continue;
                }
            };
            for reacted in reactions {
                for (by, label) in reacted.by.iter().zip(reacted.labels()) {
                    if by != self.ant {
                        let (reaction, opening) = (reacted.reaction.to_string(), message.opening.clone());
                        actions.push(Action::Reaction { reaction, by: label, message: message.id, opening });
                    }
                }
            }
        }
        actions
    }

    /// The latest done or explicit undo mark of thread `id`, if it has one.
    async fn done(&mut self, id: &ThreadId) -> Option<Mark> {
        let kv = self.done.as_ref()?;
        match within(self.deadline, kv.lookup_marked(done_key(id), THREAD_UNDONE)).await {
            Ok((Lookup::Value(value), _)) => {
                serde_json::from_slice::<ThreadDone>(&value).ok().filter(|d| d.thread_id == *id).map(Mark::Done)
            }
            Ok((Lookup::NoValue | Lookup::NoBucket, undone)) => undone.map(|at| Mark::Undone(at.into())),
            Err(error) => {
                tracing::warn!(thread = %id, error, "reading when a thread was marked done: none in its review packet");
                self.done = None;
                None
            }
        }
    }
}

enum Mark {
    Done(ThreadDone),
    Undone(DateTime<Utc>),
}

/// What `read` comes to by `deadline`, its error or lateness as a reason.
async fn within<T>(deadline: Instant, read: impl Future<Output = Result<T>>) -> Result<T, String> {
    match timeout_at(deadline, read).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => Err(format!("{e:#}")),
        Err(_) => Err("out of time".to_owned()),
    }
}

/// The ant's tool request `rejection` rejected, and by whom: none when it expired unanswered or its prompt was
/// cancelled.
fn rejected(rejection: &MergedRejection) -> Action {
    let MergedRejection { rejection, at, .. } = rejection;
    let by = match (rejection.by, &rejection.decider, &rejection.via) {
        (DecidedBy::Expiry | DecidedBy::Cancelled, _, _) => None,
        (DecidedBy::Human, Some(human), Some(assistant)) => Some(format!("human {human} via {assistant}")),
        (DecidedBy::Human, Some(human), None) => Some(format!("human {human}")),
        (DecidedBy::Human, None, _) => Some("a human".to_owned()),
        (DecidedBy::Chain, Some(ant), _) => Some(format!("ant {ant}")),
        (DecidedBy::Chain, None, _) => Some("an ant of its escalation chain".to_owned()),
    };
    let tool = match rejection.kind.as_str() {
        "" => rejection.title.clone(),
        kind => format!("{kind}: {}", rejection.title),
    };
    Action::Rejected { at: *at, tool: tool.replace(['\n', '\r'], " "), by }
}

/// The marking done `done`, by its marker, for the human an assistant claims to have marked it for.
fn done_by(done: &ThreadDone) -> Action {
    let by = match &done.on_behalf_of {
        Some(human) => format!("{} for {human}", done.by),
        None => done.by.to_string(),
    };
    Action::Done { at: done.at, by }
}

#[cfg(test)]
mod tests;
