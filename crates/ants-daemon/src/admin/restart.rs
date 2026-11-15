//! [`AdminOp::Restart`]: stop the ant's harnesses, the idle ones at once and the working ones once their current prompt
//! ends ([`Pool::restart`]), or with `now` cancelling those prompts as `ants cancel` does, each message answered with
//! what its prompt wrote so far. A thread's next prompt starts a new harness, which resumes its session.
//!
//! The daemon serving the ant carries the order out ([`Restarts::listen`]) and the outcome, applied with what it did
//! ([`Restarted`]), is recorded once the restart is under way: the idle harnesses stopped, the working ones marked to
//! stop and with `now` their prompts cancelled, not once every one of them stopped. An ant the daemon serves but whose
//! inbox it does not serve now, as while it connects, runs no harness: the order is applied with nothing to stop. An
//! ant the daemon does not serve is refused.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, UserId};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::{Orders, applied, refused};
#[cfg(doc)]
use crate::pool::Pool;
use crate::sync::MutexExt as _;

/// What a restart did, the `result` of its outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Restarted {
    /// The idle harnesses stopped at once.
    pub stopped: usize,
    /// The working harnesses, which stop once their current prompt ends.
    pub after_prompt: usize,
    /// The prompts of those cancelled, with `now`.
    #[serde(default)]
    pub cancelled: usize,
}

/// A restart asked of the daemon serving an ant.
#[derive(Debug)]
pub struct Request {
    /// The admin, whom the notice of a cancelled prompt names.
    pub by: UserId,
    /// Cancel the prompts running too.
    pub now: bool,
    /// Where to tell what the restart did.
    pub done: oneshot::Sender<Restarted>,
}

/// The ants a process serves, each with where its daemon hears restarts while it serves the ant's inbox; clones share
/// it.
#[derive(Clone, Default)]
pub struct Restarts(Arc<Mutex<HashMap<UserId, Entry>>>);

/// An ant a process serves.
#[derive(Default)]
struct Entry {
    /// Its daemons, more than one while one stopping overlaps the next one, after a reload.
    served: usize,
    /// Where the daemon serving its inbox hears restarts.
    listening: Option<mpsc::UnboundedSender<Request>>,
    /// Where it hears the snapshots clones of it ask for ([`crate::clone`]).
    cloning: Option<mpsc::UnboundedSender<crate::clone::Request>>,
}

impl std::fmt::Debug for Restarts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.locked().keys()).finish()
    }
}

impl PartialEq for Restarts {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Restarts {}

impl Restarts {
    /// Note `ant` as served until the guard drops.
    #[must_use]
    pub fn serve(&self, ant: &UserId) -> Served {
        self.0.locked().entry(ant.clone()).or_default().served += 1;
        Served { restarts: self.clone(), ant: ant.clone() }
    }

    /// Hear the restarts of `ant` until the guard drops.
    #[must_use]
    pub fn listen(&self, ant: &UserId) -> Listening {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.locked().entry(ant.clone()).or_default().listening = Some(tx.clone());
        Listening { restarts: self.clone(), ant: ant.clone(), tx, rx }
    }

    /// Whether the daemon serving `ant` serves its inbox, hearing its restarts.
    #[cfg(test)]
    pub(crate) fn is_listening(&self, ant: &UserId) -> bool {
        self.0.locked().get(ant).is_some_and(|entry| entry.listening.is_some())
    }

    /// Restart the harnesses of `ant` for `by`, cancelling their prompts if `now`, returning what that did once it is
    /// under way; none when the process does not serve `ant`.
    pub async fn restart(&self, ant: &UserId, by: UserId, now: bool) -> Option<Restarted> {
        let tx = match self.0.locked().get(ant) {
            Some(Entry { served: 1.., listening: Some(tx), .. }) => tx.clone(),
            Some(Entry { served: 1.., listening: None, .. }) => return Some(Restarted::default()),
            _ => return None,
        };
        let (done, answer) = oneshot::channel();
        if tx.send(Request { by, now, done }).is_err() {
            return Some(Restarted::default());
        }
        // Unanswered, the daemon stopped serving the inbox, its harnesses with it.
        Some(answer.await.unwrap_or_default())
    }
}

impl Restarts {
    /// Hear the snapshots clones of `ant` ask for until the guard drops.
    #[must_use]
    pub fn listen_clones(&self, ant: &UserId) -> crate::clone::Listening {
        let (tx, rx) = mpsc::unbounded_channel();
        self.0.locked().entry(ant.clone()).or_default().cloning = Some(tx.clone());
        crate::clone::Listening { restarts: self.clone(), ant: ant.clone(), tx, rx }
    }

    /// Whether the daemon serving `ant` serves its inbox, hearing the snapshots clones of it ask for.
    pub(crate) fn hears_clones(&self, ant: &UserId) -> bool {
        matches!(self.0.locked().get(ant), Some(Entry { served: 1.., cloning: Some(_), .. }))
    }

    /// The snapshot of `ant` for a clone named `new`, once the daemon serving its inbox read it; none when the process
    /// serves no inbox of `ant`.
    pub async fn snapshot(&self, ant: &UserId, new: UserId) -> Option<anyhow::Result<crate::clone::Snapshot>> {
        let tx = match self.0.locked().get(ant) {
            Some(Entry { served: 1.., cloning: Some(tx), .. }) => tx.clone(),
            _ => return None,
        };
        let (done, answer) = oneshot::channel();
        tx.send(crate::clone::Request { new, done }).ok()?;
        answer.await.ok()
    }
}

impl Drop for crate::clone::Listening {
    fn drop(&mut self) {
        if let Some(entry) = self.restarts.0.locked().get_mut(&self.ant)
            && entry.cloning.as_ref().is_some_and(|tx| tx.same_channel(&self.tx))
        {
            entry.cloning = None;
        }
    }
}

/// An ant noted as served ([`Restarts::serve`]).
pub struct Served {
    restarts: Restarts,
    ant: UserId,
}

impl Drop for Served {
    fn drop(&mut self) {
        let mut restarts = self.restarts.0.locked();
        if let Some(entry) = restarts.get_mut(&self.ant) {
            entry.served -= 1;
            if entry.served == 0 {
                restarts.remove(&self.ant);
            }
        }
    }
}

/// The restarts of an ant, heard while its daemon serves its inbox ([`Restarts::listen`]).
pub struct Listening {
    restarts: Restarts,
    ant: UserId,
    tx: mpsc::UnboundedSender<Request>,
    rx: mpsc::UnboundedReceiver<Request>,
}

impl Listening {
    /// The next restart asked for.
    pub async fn next(&mut self) -> Request {
        match self.rx.recv().await {
            Some(request) => request,
            None => std::future::pending().await,
        }
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        if let Some(entry) = self.restarts.0.locked().get_mut(&self.ant)
            && entry.listening.as_ref().is_some_and(|tx| tx.same_channel(&self.tx))
        {
            entry.listening = None;
        }
    }
}

pub(super) async fn apply(orders: &Orders, order: &AdminOrder, now: bool) -> AdminOutcome {
    match orders.restarts.restart(&order.ant, order.admin.clone(), now).await {
        Some(restarted) => applied(order, Some(serde_json::to_value(restarted).expect("a restart serializes"))),
        None => refused(order, &format!("machine {} serves no ant {}", orders.machine, order.ant)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_restart_reaches_the_daemon_serving_the_ant_s_inbox() {
        let (restarts, dev_a, alice) = (Restarts::default(), "dev-a".parse().unwrap(), "alice".parse().unwrap());
        assert_eq!(restarts.restart(&dev_a, alice, false).await, None, "not served");
        let served = restarts.serve(&dev_a);
        let alice: UserId = "alice".parse().unwrap();
        let idle = restarts.restart(&dev_a, alice.clone(), true).await;
        assert_eq!(idle, Some(Restarted::default()), "no inbox served, no harness");
        let mut listening = restarts.listen(&dev_a);
        let daemon = async {
            let request = listening.next().await;
            assert_eq!((&request.by, request.now), (&alice, true));
            request.done.send(Restarted { stopped: 1, after_prompt: 2, cancelled: 2 }).unwrap();
        };
        let (restarted, ()) = tokio::join!(restarts.restart(&dev_a, alice.clone(), true), daemon);
        assert_eq!(restarted, Some(Restarted { stopped: 1, after_prompt: 2, cancelled: 2 }));
        drop(listening);
        assert_eq!(restarts.restart(&dev_a, alice.clone(), false).await, Some(Restarted::default()));
        let next = restarts.serve(&dev_a);
        drop(served);
        assert_eq!(restarts.restart(&dev_a, alice.clone(), false).await, Some(Restarted::default()), "still served");
        drop(next);
        assert_eq!(restarts.restart(&dev_a, alice, false).await, None);
    }
}
