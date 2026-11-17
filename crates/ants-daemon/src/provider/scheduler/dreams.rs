//! The daemon's part of the ant's dreams ([`ants_sense::dream`]): when one is due, the ant has had no prompt running
//! for [`IDLE_MINUTES`] and its threads were active since the last dream, it gathers them, with the timeline the ant
//! stored on them, and the commits they left, begins the dream and wakes the ant on a new thread with its packet.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use ants_proto::provider::{BoxFuture, CallContext, Host, Origin, Starter, WakeRequest};
use ants_proto::{ThreadId, UserId};
use ants_sense::dream::{self, Commit, Covered, Dream, IDLE_MINUTES, PACKET_BUDGET, Start, Thread};
use ants_sense::store::Items;
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Duration, Utc};

use super::Settings;
use crate::colony::Workload;
use crate::provider::origins::OriginStore;
use crate::stream::history;
use crate::stream::reading::Reading;
use crate::sync::MutexExt as _;

/// How long a git command naming a commit may take.
const GIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// What a dream needs of the daemon serving the ant.
pub(crate) trait Dreaming: Send + Sync {
    /// Since when no prompt has run; none while one runs.
    fn idle_since(&self) -> Option<DateTime<Utc>>;
    /// The ant's threads active from `since` to `until`, the most recently active first.
    fn threads(&self, since: DateTime<Utc>, until: DateTime<Utc>) -> BoxFuture<'_, Result<Vec<Thread>>>;
    /// The commits the ant's workdir and the branches of `threads` are at.
    fn commits<'a>(&'a self, threads: &'a [ThreadId]) -> BoxFuture<'a, Vec<Commit>>;
}

pub(super) struct Dreams {
    daemon: Arc<dyn Dreaming>,
    /// The idle time from which an earlier look found no thread active: none is until a prompt runs again.
    quiet: Mutex<Option<DateTime<Utc>>>,
}

impl Dreams {
    pub(super) fn new(daemon: Arc<dyn Dreaming>) -> Self {
        Self { daemon, quiet: Mutex::new(None) }
    }
}

/// Take the ant's dreams a step at `now`: wake it for a dream begun, or begin the next one when due. Returns when to
/// look again.
pub(super) async fn step(
    items: &dyn Items,
    host: &dyn Host,
    settings: &Settings,
    dreams: &Dreams,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    let idle = Duration::minutes(IDLE_MINUTES);
    let last = dream::last(items).await?.map(|(last, _)| last);
    if let Some(current) = &last {
        if current.wake_due(now) {
            let mut threads = dreams.daemon.threads(current.since, current.at).await?;
            threads.retain(|thread| current.threads.iter().any(|covered| covered.thread == thread.id));
            wake(items, host, &settings.ant, current, &threads).await?;
            return Ok(current.next());
        }
        if let Some(deadline) = current.deadline() {
            return Ok(deadline);
        }
        if current.next() > now {
            return Ok(current.next());
        }
    }
    let Some(idle_since) = dreams.daemon.idle_since() else { return Ok(now + idle) };
    if idle_since + idle > now {
        return Ok(idle_since + idle);
    }
    if *dreams.quiet.locked() == Some(idle_since) {
        return Ok(now + idle);
    }
    let since = dream::since(last.as_ref(), now);
    let mut threads = dreams.daemon.threads(since, now).await?;
    // The last dream's own thread is no work to review.
    threads.retain(|thread| last.as_ref().and_then(|last| last.thread.as_ref()) != Some(&thread.id));
    if threads.is_empty() {
        *dreams.quiet.locked() = Some(idle_since);
        return Ok(now + idle);
    }
    let ids: Vec<_> = threads.iter().map(|thread| thread.id.clone()).collect();
    let commits = dreams.daemon.commits(&ids).await;
    let covered = threads.iter().map(|thread| Covered { thread: thread.id.clone(), untrusted: thread.untrusted });
    let writer = writer(items, settings, last.as_ref(), now).await?;
    // The sage, when the ant has one, reads the packet first with ants-thy3p.11; until then the ant reads it alone.
    let recipient = dream::recipient(None);
    let start = Start { at: now, recipient, threads: covered.collect(), commits, writer: &writer };
    let Some(begun) = dream::begin(items, start).await? else {
        // Another daemon began one meanwhile.
        return Ok(now);
    };
    let findings = begun.findings.len();
    tracing::info!(ant = %settings.ant, dream = begun.number, threads = threads.len(), findings, "sense dream began");
    if begun.wake_due(now) {
        wake(items, host, &settings.ant, &begun, &threads).await?;
    }
    Ok(begun.deadline().unwrap_or_else(|| begun.next()))
}

/// Wake `ant` on a new thread with the packet of `dream`, covering `threads`, and note the thread.
async fn wake(items: &dyn Items, host: &dyn Host, ant: &UserId, dream: &Dream, threads: &[Thread]) -> Result<()> {
    let text = dream::packet(items, dream, threads).await?;
    // The beginning names the dream apart from a dream of the same number of another sense identity of the ant.
    let id = format!("dream-{}-{}", dream.number, dream.at.timestamp());
    let woken = host.wake(WakeRequest { ant: ant.clone(), thread: None, text, id }).await?;
    dream::woken(items, dream.number, &woken.thread).await?;
    Ok(())
}

/// The context sense writes in for the ant's dreams at `now`: the diary of the `last` dream left unfinished.
async fn writer(
    items: &dyn Items,
    settings: &Settings,
    last: Option<&Dream>,
    now: DateTime<Utc>,
) -> Result<CallContext> {
    let identity = ants_sense::identity::card(items).await?.map_or_default(|card| card.identity);
    Ok(CallContext {
        ant: settings.ant.clone(),
        identity,
        thread: last.and_then(|last| last.thread.clone()).unwrap_or_else(ThreadId::generate),
        origin: Origin { by: Starter::Provider { name: ants_sense::NAME.to_owned() }, channel: None },
        harness: String::new(),
        model: None,
        machine: settings.machine.clone(),
        repo: None,
        at: now,
    })
}

/// The daemon serving the ant, as its dreams need it.
pub(crate) struct Daemon {
    pub js: jetstream::Context,
    pub ant: UserId,
    /// How the daemon reads through the ant's own readers ([`Reading::daemon`]).
    pub reading: Reading,
    pub origins: Option<OriginStore>,
    pub workload: Arc<Workload>,
    /// When the daemon began serving: idle from then until the first prompt.
    pub started: DateTime<Utc>,
    pub workdir: PathBuf,
}

impl Dreaming for Daemon {
    fn idle_since(&self) -> Option<DateTime<Utc>> {
        (self.workload.handling() == 0).then(|| self.workload.worked().unwrap_or(self.started))
    }

    fn threads(&self, since: DateTime<Utc>, until: DateTime<Utc>) -> BoxFuture<'_, Result<Vec<Thread>>> {
        Box::pin(async move {
            let (from, to) = (SystemTime::from(since), SystemTime::from(until));
            let active = history::active(&self.js, &self.ant, self.reading, from, to, PACKET_BUDGET).await?;
            let mut threads = Vec::with_capacity(active.len());
            for history::Active { thread: id, history: text, timeline } in active {
                let origin = match &self.origins {
                    Some(origins) => origins.get(&id).await.unwrap_or_else(|e| {
                        tracing::warn!(thread = %id, error = format!("{e:#}"), "reading the origin of a thread");
                        None
                    }),
                    None => None,
                };
                let untrusted = origin.as_ref().is_some_and(|origin| !origin.trusted());
                let started_by = origin.map_or_else(|| "someone not known".to_owned(), |origin| starter(&origin.by));
                threads.push(Thread { id, untrusted, started_by, text, timeline });
            }
            Ok(threads)
        })
    }

    fn commits<'a>(&'a self, threads: &'a [ThreadId]) -> BoxFuture<'a, Vec<Commit>> {
        Box::pin(async move {
            let Some(repo) = crate::provider::context::repository(&self.workdir) else { return Vec::new() };
            let at = async |name: &str| {
                let args = ["rev-parse", "--verify", "--quiet", name];
                let commit = crate::worktrees::run("git", &self.workdir, args, &[], Some(GIT_TIMEOUT)).await.ok()?;
                Some(commit.trim().to_owned()).filter(|commit| !commit.is_empty())
            };
            let mut commits = Vec::new();
            if let Some(commit) = at("HEAD").await {
                commits.push(Commit { repo: repo.clone(), commit, thread: None });
            }
            for thread in threads {
                let branch = format!("refs/heads/{}", crate::worktrees::branch_name(&self.ant, thread));
                if let Some(commit) = at(&branch).await {
                    commits.push(Commit { repo: repo.clone(), commit, thread: Some(thread.clone()) });
                }
            }
            commits
        })
    }
}

fn starter(by: &Starter) -> String {
    match by {
        Starter::Human { user } => format!("human {user}"),
        Starter::Ant { user } => format!("ant {user}"),
        Starter::Webhook { user } => format!("webhook {user}"),
        Starter::Provider { name } => format!("provider {name}"),
    }
}
