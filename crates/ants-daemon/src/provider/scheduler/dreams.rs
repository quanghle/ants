//! The daemon's part of the ant's dreams ([`ants_sense::dream`]): when one is due, the ant has had no prompt running
//! for [`IDLE_MINUTES`] and its threads were active since the last dream, it gathers them and the commits they left,
//! and begins the dream. It sends the packet to the ant's sage when one serves it, as a message from the ant on a new
//! thread narrowed to a review ([`ants_proto::REVIEW_ALLOW`]), and takes the sage's answer there as it arrives
//! ([`review_answer`]); without a sage, or once the sage leaves the review unanswered or answers with nothing
//! readable, it wakes the ant on a new thread with the packet alone.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use ants_proto::provider::{BoxFuture, CallContext, Host, Origin, Starter, WakeRequest};
use ants_proto::{Envelope, NoticeKind, REVIEW_ALLOW, ThreadId, UserId};
use ants_sense::dream::{
    self, Commit, Covered, Dream, IDLE_MINUTES, PACKET_BUDGET, Reader, Review, Reviewed, SAGE_TIMEOUT_HOURS, Start,
    Thread,
};
use ants_sense::store::Items;
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Duration, Utc};

use super::Settings;
use crate::colony::Workload;
use crate::mcp::{ColonyLookup, Outbox};
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
    /// The ant's sage, when it has one and a daemon serves it now.
    fn sage(&self) -> BoxFuture<'_, Result<Option<UserId>>>;
    /// Send the review packet `text` to `sage` as message `message` on `thread`.
    fn review<'a>(&'a self, sage: &'a UserId, review: &'a Review, text: String) -> BoxFuture<'a, Result<()>>;
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
            let threads = covered(dreams, current).await?;
            wake(items, host, &settings.ant, current, &threads).await?;
            return Ok(current.next());
        }
        if current.review_due(now).is_some() {
            let threads = covered(dreams, current).await?;
            review(items, dreams, &settings.ant, current, &threads).await?;
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
    // The last dream's own threads are no work to review.
    threads.retain(|thread| last.as_ref().is_none_or(|last| last.own_threads().all(|own| *own != thread.id)));
    if threads.is_empty() {
        *dreams.quiet.locked() = Some(idle_since);
        return Ok(now + idle);
    }
    let ids: Vec<_> = threads.iter().map(|thread| thread.id.clone()).collect();
    let commits = dreams.daemon.commits(&ids).await;
    let covered = threads.iter().map(|thread| Covered { thread: thread.id.clone(), untrusted: thread.untrusted });
    let writer = writer(items, settings, last.as_ref(), now).await?;
    // A sage reviews others, never itself.
    let sage = dreams.daemon.sage().await?.filter(|sage| *sage != settings.ant);
    let recipient = dream::recipient(sage);
    let start = Start { at: now, recipient, threads: covered.collect(), commits, writer: &writer };
    let Some(begun) = dream::begin(items, start).await? else {
        // Another daemon began one meanwhile.
        return Ok(now);
    };
    let findings = begun.findings.len();
    tracing::info!(ant = %settings.ant, dream = begun.number, threads = threads.len(), findings, "sense dream began");
    if begun.wake_due(now) {
        wake(items, host, &settings.ant, &begun, &threads).await?;
    } else if begun.review_due(now).is_some() {
        review(items, dreams, &settings.ant, &begun, &threads).await?;
    }
    Ok(begun.deadline().unwrap_or_else(|| begun.next()))
}

/// The threads `dream` covers, as they read now.
async fn covered(dreams: &Dreams, dream: &Dream) -> Result<Vec<Thread>> {
    let mut threads = dreams.daemon.threads(dream.since, dream.at).await?;
    threads.retain(|thread| dream.threads.iter().any(|covered| covered.thread == thread.id));
    Ok(threads)
}

/// Send the review packet of `dream` of `ant`, covering `threads`, to its sage, and note it sent. Sent again after a
/// failure to note it, it keeps its message id, so the stream keeps one.
async fn review(items: &dyn Items, dreams: &Dreams, ant: &UserId, dream: &Dream, threads: &[Thread]) -> Result<()> {
    let (Some(sage), Some(review)) = (&dream.sage, &dream.review) else { return Ok(()) };
    let text = dream::packet(items, dream, &Reader::Sage(ant.clone()), threads).await?;
    dreams.daemon.review(sage, review, text).await?;
    dream::review_sent(items, dream.number).await?;
    tracing::info!(%ant, %sage, dream = dream.number, thread = %review.thread, "sense dream review sent to the sage");
    Ok(())
}

/// Take the message `envelope` that arrived for the ant, whose sense data `items` holds, when it is its sage's answer
/// to the review of its last dream: none when it is not; true to prompt the ant with it, as it was made the sage's
/// findings in their marked block; false to ack it unprompted, as an answer without findings, a late one or one that
/// leaves the ant to be woken with the packet alone. A notice that the sage gave up on the review or had it withdrawn
/// leaves the ant to the packet alone; any other one waits for the answer.
pub(crate) async fn review_answer(items: &dyn Items, envelope: &mut Envelope) -> Result<Option<bool>> {
    let Some((last, _)) = dream::last(items).await? else { return Ok(None) };
    let reviewing = last.review.as_ref().is_some_and(|review| review.thread == envelope.thread_id);
    if !reviewing || last.sage.as_ref() != Some(&envelope.from) {
        return Ok(None);
    }
    let text = match envelope.notice {
        None => Some(envelope.text.as_str()),
        Some(NoticeKind::GaveUp | NoticeKind::NoFallback | NoticeKind::Withdrawn) => None,
        Some(_) => return Ok(Some(false)),
    };
    let (sage, thread) = (&envelope.from, &envelope.thread_id);
    let Some(reviewed) = dream::answered(items, sage, thread, envelope.id, text).await? else { return Ok(None) };
    let dream = last.number;
    match reviewed {
        Reviewed::Prompt(findings) => {
            tracing::info!(%sage, dream, "the sage's findings prompt the ant");
            envelope.text = findings;
            envelope.quote = None;
            envelope.allow = None;
            Ok(Some(true))
        }
        Reviewed::Quiet => {
            tracing::info!(%sage, dream, "the sage's answer brings no findings: nobody woken");
            Ok(Some(false))
        }
        Reviewed::Fallback => {
            tracing::warn!(%sage, dream, "the sage's answer holds no readable findings: the ant dreams on the packet alone");
            Ok(Some(false))
        }
    }
}

/// Wake `ant` on a new thread with the packet of `dream`, covering `threads`, and note the thread.
async fn wake(items: &dyn Items, host: &dyn Host, ant: &UserId, dream: &Dream, threads: &[Thread]) -> Result<()> {
    let text = dream::packet(items, dream, &Reader::Ant, threads).await?;
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
    /// The ant reviewing this one ([`crate::daemon::Config::policy`]).
    pub sage: Option<UserId>,
    pub lookup: ColonyLookup,
    /// Sends the review packets, redacted.
    pub outbox: Outbox,
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
            for (id, text) in active {
                let origin = match &self.origins {
                    Some(origins) => origins.get(&id).await.unwrap_or_else(|e| {
                        tracing::warn!(thread = %id, error = format!("{e:#}"), "reading the origin of a thread");
                        None
                    }),
                    None => None,
                };
                let untrusted = origin.as_ref().is_some_and(|origin| !origin.trusted());
                let started_by = origin.map_or_else(|| "someone not known".to_owned(), |origin| starter(&origin.by));
                threads.push(Thread { id, untrusted, started_by, text });
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

    fn sage(&self) -> BoxFuture<'_, Result<Option<UserId>>> {
        Box::pin(async move {
            let Some(sage) = self.sage.clone() else { return Ok(None) };
            let entry = (self.lookup)(sage.clone()).await?;
            let wait = std::time::Duration::from_secs(SAGE_TIMEOUT_HOURS.unsigned_abs() * 3600);
            match crate::mcp::why_unserved(&sage, entry.as_ref(), Utc::now(), wait) {
                None => Ok(Some(sage)),
                Some(why) => {
                    tracing::info!(ant = %self.ant, %sage, why, "the ant dreams without its sage");
                    Ok(None)
                }
            }
        })
    }

    fn review<'a>(&'a self, sage: &'a UserId, review: &'a Review, text: String) -> BoxFuture<'a, Result<()>> {
        let mut envelope = Envelope::on_thread(review.thread.clone(), self.ant.clone(), sage.clone(), text)
            .with_allow(Some(REVIEW_ALLOW.map(str::to_owned).to_vec()));
        envelope.id = review.message;
        (self.outbox)(envelope)
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
