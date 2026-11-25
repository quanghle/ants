//! The turns the ant's harness runs of its own between prompts ([`crate::harness::OwnTurn`]), as Claude Code does once
//! a background command it started ends after the turn that started it: each is sent as a message from the ant to
//! itself on the thread, saying why, marked to wait behind a prompt running there rather than steer it, and the prompt
//! of that message takes the turn, its answer going to the thread's owner, else to the default human, as for any
//! message the ant sends itself. A message taken by a daemon that did not see the turn start, or tried again, is
//! prompted as it is. At most [`OWN_TURNS_PER_HOUR`] such messages are sent for an ant within an hour: the harness
//! drops the events of a turn past them, logged at WARN.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ants_proto::{Envelope, ThreadId, UserId, Uuid};

use crate::harness::OwnTurn;
use crate::mcp::Outbox;
use crate::records::owners::Owners;
use crate::sync::MutexExt as _;

/// The most messages of turns kept for their prompts.
const KEPT: usize = 64;

/// The most turns of their own an ant's harnesses start within an hour that it is sent a message for.
const OWN_TURNS_PER_HOUR: usize = 60;

const HOUR: Duration = Duration::from_hours(1);

/// The messages sent for the harness's own turns, with the turn each is for, until their prompt takes it, and when
/// the ones within the hour were sent; clones share them.
#[derive(Clone)]
pub(super) struct OwnTurns {
    kept: Arc<Mutex<VecDeque<(Uuid, String)>>>,
    sent: Arc<Mutex<VecDeque<Instant>>>,
    per_hour: usize,
}

impl Default for OwnTurns {
    fn default() -> Self {
        Self::capped(OWN_TURNS_PER_HOUR)
    }
}

impl OwnTurns {
    /// Sending at most `per_hour` messages within an hour.
    pub(super) fn capped(per_hour: usize) -> Self {
        Self { kept: Arc::default(), sent: Arc::default(), per_hour }
    }

    /// Where a launch's sessions hand the turns they start of their own: sent as `ant` through `outbox`, to the
    /// thread's owner as `owners` records it, unless past the cap, the turn then refused.
    pub(super) fn sink(&self, ant: UserId, outbox: Outbox, owners: Owners) -> crate::harness::OwnTurns {
        let (own, owners) = (self.clone(), Arc::new(owners));
        Arc::new(move |thread: &ThreadId, turn: OwnTurn| {
            if !own.count() {
                tracing::warn!(
                    %thread,
                    turn = turn.id,
                    per_hour = own.per_hour,
                    "dropping a turn the harness started of its own: too many within the hour"
                );
                return false;
            }
            let (own, ant, outbox, owners, thread) =
                (own.clone(), ant.clone(), outbox.clone(), owners.clone(), thread.clone());
            tokio::spawn(async move { own.send(&ant, &outbox, &owners, thread, turn).await });
            true
        })
    }

    /// Count a message against the cap, refusing it past it.
    fn count(&self) -> bool {
        let now = Instant::now();
        let mut sent = self.sent.locked();
        while sent.front().is_some_and(|at| now.duration_since(*at) >= HOUR) {
            sent.pop_front();
        }
        if sent.len() >= self.per_hour {
            return false;
        }
        sent.push_back(now);
        true
    }

    async fn send(&self, ant: &UserId, outbox: &Outbox, owners: &Owners, thread: ThreadId, turn: OwnTurn) {
        let owner = owners.recorded(&thread, ant).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner for the harness's own turn");
            None
        });
        let envelope = Envelope::on_thread(thread.clone(), ant.clone(), ant.clone(), turn.text).with_owner(owner);
        let envelope = Envelope { queue: true, ..envelope };
        let id = envelope.id;
        {
            let mut kept = self.kept.locked();
            if kept.len() >= KEPT {
                kept.pop_front();
            }
            kept.push_back((id, turn.id));
        }
        if let Err(e) = outbox(envelope).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "sending the message of the harness's own turn");
            self.kept.locked().retain(|(kept, _)| *kept != id);
        }
    }

    /// The turn of its own the harness runs that the prompt of `envelope`, a message to `ant`, takes: the one the
    /// message was sent for, by this daemon and from the ant itself, once.
    pub(super) fn take(&self, envelope: &Envelope, ant: &UserId) -> Option<String> {
        if envelope.from != *ant || envelope.to != *ant {
            return None;
        }
        let mut kept = self.kept.locked();
        let at = kept.iter().position(|(id, _)| *id == envelope.id)?;
        kept.remove(at).map(|(_, turn)| turn)
    }
}
