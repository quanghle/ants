//! What the daemon serving an ant hands over to a clone of it ([`AdminOp::Clone`], carried out by the host connection
//! in `admin::clone`): once the ant is paused and its last prompt ended, a [`Snapshot`] of its identity record, how
//! its harness starts, and each thread it has a session of, with that session and the thread's history read back.
//!
//! An ant cannot list its keys of [`ants_proto::SESSIONS_BUCKET`], so its threads are those of the messages it
//! received and sent that the ANTS stream still holds, and those it has a worktree of; a thread it only posted in
//! within a channel, with neither, is missed.

use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::path::Path;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AntIdentity, ChannelId, Envelope, ThreadId, UserId, inbox_consumer, sent_reader, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer::DeliverPolicy};
use chrono::Utc;
use tokio::sync::{mpsc, oneshot};

use crate::harness::Launch;
use crate::identity::Identity;
use crate::pool::{SessionStore, Stored};
use crate::reading::{self, Reading};

/// A snapshot asked of the daemon serving the ant to clone.
pub struct Request {
    /// The clone's name.
    pub new: UserId,
    pub done: oneshot::Sender<Result<Snapshot>>,
}

/// What a clone takes of the ant it is made from, read while that ant is paused and runs no prompt.
pub struct Snapshot {
    /// The ant's identity record; none when absent or unreadable.
    pub identity: Option<AntIdentity>,
    /// The record of the clone's name, which must not exist unless left by an earlier attempt at the same clone.
    pub new_identity: Option<AntIdentity>,
    /// How the ant starts its harness, to fork its sessions with; none for a daemon running no harness.
    pub launch: Option<Launch>,
    pub threads: Vec<ThreadCopy>,
}

/// A thread the ant has a session of.
#[derive(Debug, Clone)]
pub struct ThreadCopy {
    pub thread: ThreadId,
    pub stored: Stored,
    /// The thread's history, as the lines of the clone's copy ([`crate::history::cloned`]).
    pub history: String,
}

/// The snapshots of an ant asked while its daemon serves its inbox ([`crate::admin::restart::Restarts::listen_clones`]).
pub struct Listening {
    pub(crate) restarts: crate::admin::restart::Restarts,
    pub(crate) ant: UserId,
    pub(crate) tx: mpsc::UnboundedSender<Request>,
    pub(crate) rx: mpsc::UnboundedReceiver<Request>,
}

impl Listening {
    /// The next snapshot asked for.
    pub async fn next(&mut self) -> Request {
        match self.rx.recv().await {
            Some(request) => request,
            None => std::future::pending().await,
        }
    }
}

/// Where the daemon serving an ant reads its snapshot.
pub struct Source<'a> {
    /// A connection as the ant.
    pub js: &'a jetstream::Context,
    pub ant: &'a UserId,
    pub home: &'a Path,
    pub store: &'a SessionStore,
    pub identity: Option<&'a Identity>,
    pub launch: Option<Launch>,
}

impl Source<'_> {
    /// The snapshot for a clone named `new`; `root` tells the channel whose top-level post started a thread.
    pub async fn snapshot(self, new: &UserId, root: impl AsyncFn(&ThreadId) -> Option<ChannelId>) -> Result<Snapshot> {
        let (identity, new_identity) = match self.identity {
            Some(identity) => (identity.of(self.ant).await?, identity.of(new).await?),
            None => (None, None),
        };
        let at = Utc::now();
        let mut threads = Vec::new();
        for thread in self.threads().await? {
            let Some(stored) = self.store.get(&thread).await? else { continue };
            let history = crate::history::Thread {
                js: self.js.clone(),
                ant: self.ant.clone(),
                thread: thread.clone(),
                channel: root(&thread).await,
                before: u64::MAX,
                home: Some(self.home.to_owned()),
            };
            let history = history.snapshot(at).await?;
            threads.push(ThreadCopy { thread, stored, history });
        }
        Ok(Snapshot { identity, new_identity, launch: self.launch, threads })
    }

    /// The threads of the messages the ant received and sent that the stream holds, and of its worktrees.
    async fn threads(&self) -> Result<BTreeSet<ThreadId>> {
        let mut threads = BTreeSet::new();
        let mut take = |_, _, subject: &str, payload: &[u8]| {
            if let Ok(envelope) = serde_json::from_slice::<Envelope>(payload)
                && envelope.check_subject(subject).is_ok()
            {
                threads.insert(envelope.thread_id);
            }
            ControlFlow::Continue(())
        };
        let (reader, filter) = (inbox_consumer(self.ant), subject::inbox_of(self.ant));
        reading::read_back(self.js, &reader, Reading::Spare, &filter, DeliverPolicy::All, &mut take)
            .await
            .context("reading back the messages received")?;
        {
            let _one = crate::history::SENT_READS.lock().await;
            let (reader, filter) = (sent_reader(self.ant), subject::inbox_from(self.ant));
            reading::read_back(self.js, &reader, Reading::Consumer, &filter, DeliverPolicy::All, &mut take)
                .await
                .context("reading back the messages sent")?;
        }
        if let Ok(dirs) = std::fs::read_dir(self.home.join("worktrees")) {
            threads.extend(dirs.flatten().filter_map(|dir| dir.file_name().to_str()?.parse().ok()));
        }
        Ok(threads)
    }
}
