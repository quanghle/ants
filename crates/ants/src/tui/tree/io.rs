//! The reads of the shown tree's links and spend, every while, and the cancels asked of its ants.

use std::collections::BTreeMap;
use std::time::Duration;

use ants_daemon::bus::relayed;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::records::links::{self, Passed};
use ants_proto::{CancelRequest, JsDomain, SESSIONS_BUCKET, ThreadId, UserId};
use anyhow::Result;
use async_nats::jetstream;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};

use crate::cmd::cancel::{CANCEL_GRACE, CANCEL_WAIT, cancel_outcome};
use crate::live::maybe;
use crate::spend::{self, ThreadReport};

/// How often the shown tree's links and spend are read.
const READ_EVERY: Duration = Duration::from_secs(10);

/// What the tree's reads and cancels brought.
pub enum Event {
    /// The links to the root and the spend on the tree's threads; None for what could not be read.
    Read(ThreadId, Option<Vec<Passed>>, Option<Vec<ThreadReport>>),
    /// How each cancel asked went: whether the ant's daemon cancelled a prompt, None when none answered.
    Cancelled(Vec<(UserId, ThreadId, Result<Option<bool>>)>),
}

/// The reads of the shown tree, and the cancels asked.
pub struct Io {
    js: jetstream::Context,
    domain: Option<JsDomain>,
    me: UserId,
    /// The root of the tree shown, and the threads known of it.
    watched: Option<(ThreadId, Vec<ThreadId>)>,
    every: tokio::time::Interval,
    read: Option<LocalBoxFuture<'static, Event>>,
    cancels: FuturesUnordered<LocalBoxFuture<'static, Event>>,
}

impl Io {
    pub fn new(js: jetstream::Context, domain: Option<JsDomain>, me: UserId) -> Self {
        let every = tokio::time::interval(READ_EVERY);
        Self { js, domain, me, watched: None, every, read: None, cancels: FuturesUnordered::new() }
    }

    /// The next thing read or cancelled. Cancel-safe, so it can race the terminal's keys and the live feed.
    pub async fn next(&mut self) -> Event {
        loop {
            tokio::select! {
                _ = self.every.tick() => {
                    if self.read.is_none() {
                        self.start_read();
                    }
                }
                event = maybe(&mut self.read) => {
                    self.read = None;
                    if let Event::Read(root, ..) = &event
                        && self.watched.as_ref().is_some_and(|(watched, _)| watched == root)
                    {
                        return event;
                    }
                }
                Some(event) = self.cancels.next() => return event,
            }
        }
    }

    /// Read the links and spend of the tree of the root of `shown` now and every while, with its threads known; with
    /// None, of none.
    pub fn watch(&mut self, shown: Option<(ThreadId, Vec<ThreadId>)>) {
        let again = self.watched.as_ref().map(|(root, _)| root) != shown.as_ref().map(|(root, _)| root);
        self.watched = shown;
        if again {
            self.start_read();
        }
    }

    /// Read everything again now.
    pub fn refresh(&mut self) {
        self.start_read();
    }

    fn start_read(&mut self) {
        let Some((root, threads)) = self.watched.clone() else {
            self.read = None;
            return;
        };
        let (js, domain) = (self.js.clone(), self.domain.clone());
        self.read = Some(
            async move {
                let links =
                    links::passed(&js, &root).await.inspect_err(|e| tracing::debug!("reading the links: {e:#}")).ok();
                let mut threads = threads;
                for on in links.iter().flatten().filter_map(|l| l.on.as_ref()) {
                    if !threads.contains(on) {
                        threads.push(on.clone());
                    }
                }
                let spend = async {
                    let kv = DirectKv::new(&js, domain.as_ref(), SESSIONS_BUCKET).await?;
                    let mut reports = Vec::new();
                    for thread in &threads {
                        reports.extend(spend::thread(&js, &kv, None, thread, &BTreeMap::new()).await?);
                    }
                    anyhow::Ok(reports)
                };
                let spend = spend.await.inspect_err(|e| tracing::debug!("reading the spend: {e:#}")).ok();
                Event::Read(root, links, spend)
            }
            .boxed_local(),
        );
    }

    /// Cancel the prompt of each ant on its thread, as `ants cancel` does, all at once.
    pub fn cancel(&mut self, prompts: Vec<(UserId, ThreadId)>) {
        let (client, me) = (self.js.client(), self.me.clone());
        let cancels = prompts.into_iter().map(move |(ant, thread)| {
            let (client, me) = (client.clone(), me.clone());
            async move {
                let outcome = cancel(&client, &me, &ant, &thread).await;
                (ant, thread, outcome)
            }
        });
        self.cancels.push(async move { Event::Cancelled(futures::future::join_all(cancels).await) }.boxed_local());
    }
}

/// Ask the daemons of `ant` to cancel its prompt on `thread`: whether one did, None when none answered.
async fn cancel(client: &async_nats::Client, me: &UserId, ant: &UserId, thread: &ThreadId) -> Result<Option<bool>> {
    let request = CancelRequest::new(ant.clone(), thread.clone(), me.clone());
    let answers = relayed::subscribe(client, request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    Ok(cancel_outcome(&request, answers, CANCEL_WAIT, CANCEL_GRACE).await)
}
