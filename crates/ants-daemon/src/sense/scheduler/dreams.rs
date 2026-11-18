//! The daemon's part of the ant's dreams ([`ants_sense::dream`]): when one is due, the ant has had no prompt running
//! for [`IDLE_MINUTES`] and its threads were active since the last dream, it gathers them, with the timeline the ant
//! stored on them, what was done with them besides writing on them ([`actions`]) and the vital signs of the work on
//! them ([`vitals`]), and the commits they left, and begins the dream. It sends the packet to the ant's sage when one
//! serves it and the ant is no live ant's sage ([`serving_sage`]), as a message from the ant on a new thread narrowed
//! to a review ([`ants_proto::REVIEW_ALLOW`]) naming the commits for the sage's daemon to check out
//! ([`crate::checkouts`]), and takes the sage's answer there as it arrives ([`review_answer`]); without a sage, or once
//! the sage leaves the review unanswered or answers with nothing readable, it wakes the ant on a new thread with the
//! packet alone. A packet sent again, after a failure, checks the colony again: no further packet goes out if the ant
//! became a sage or its sage is no longer served, and the ant is woken with the packet alone.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use ants_proto::provider::{BoxFuture, CallContext, Host, Origin, Starter, WakeRequest};
use ants_proto::{ColonyEntry, Envelope, JsDomain, NoticeKind, REVIEW_ALLOW, ReviewCommit, ThreadId, UserId};
use ants_sense::dream::{
    self, Commit, Covered, Dream, IDLE_MINUTES, PACKET_BUDGET, Reader, ReviewSpend, Reviewed, SAGE_TIMEOUT_HOURS,
    Start, Thread,
};
use ants_sense::store::Items;
use anyhow::Result;
use async_nats::jetstream;
use chrono::{DateTime, Duration, Utc};

use super::Settings;
use crate::colony::Workload;
use crate::mcp::{Colony, Outbox, ReactionAsker};
use crate::provider::origins::OriginStore;
use crate::stream::history;
use crate::stream::reading::Reading;
use crate::sync::MutexExt as _;

mod actions;
mod vitals;

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
    /// The ant's configured sage, if any.
    fn sage(&self) -> Option<UserId>;
    /// The colony entries for checking both the ant's and the sage's eligibility for a review.
    fn colony(&self) -> BoxFuture<'_, Result<Vec<ColonyEntry>>>;
    /// Send the review packet `text` of `dream` to `sage` as its review's message on its thread, naming the dream's
    /// commits for the sage's daemon to check out and telling what is new in the verdicts and outcomes of the sage's
    /// earlier findings ([`Dream::review_outcomes`]).
    fn review<'a>(&'a self, sage: &'a UserId, dream: &'a Dream, text: String) -> BoxFuture<'a, Result<()>>;
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
        if let Some(sage) = current.sage.as_ref().filter(|_| current.review_due(now).is_some()) {
            let threads = covered(dreams, current).await?;
            let entries = dreams.daemon.colony().await?;
            if !serving_sage(&settings.ant, sage, &entries, now) {
                wake(items, host, &settings.ant, current, &threads).await?;
                return Ok(current.next());
            }
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
    // A proposal ripe for confirmation calls for a dream even without new work.
    let due = dream::proposal_due(items, last.as_ref(), now).await?;
    if !due && *dreams.quiet.locked() == Some(idle_since) {
        return Ok(now + idle);
    }
    let since = dream::since(last.as_ref(), now);
    let mut threads = dreams.daemon.threads(since, now).await?;
    // The last dream's own threads are no work to review.
    threads.retain(|thread| last.as_ref().is_none_or(|last| last.own_threads().all(|own| *own != thread.id)));
    if threads.is_empty() && !due {
        *dreams.quiet.locked() = Some(idle_since);
        return Ok(now + idle);
    }
    let ids: Vec<_> = threads.iter().map(|thread| thread.id.clone()).collect();
    let commits = dreams.daemon.commits(&ids).await;
    let covered = threads.iter().map(|thread| Covered {
        thread: thread.id.clone(),
        untrusted: thread.untrusted,
        review: thread.review,
    });
    let writer = writer(items, settings, last.as_ref(), now).await?;
    let sage = match dreams.daemon.sage() {
        Some(sage) if serving_sage(&settings.ant, &sage, &dreams.daemon.colony().await?, now) => Some(sage),
        _ => None,
    };
    let recipient = dream::recipient(sage);
    let vitals = threads.iter().map(|thread| (thread.id.clone(), thread.signs.clone())).collect();
    let start = Start { at: now, recipient, threads: covered.collect(), commits, writer: &writer, vitals };
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
    dreams.daemon.review(sage, dream, text).await?;
    dream::review_sent(items, dream.number).await?;
    tracing::info!(%ant, %sage, dream = dream.number, thread = %review.thread, "sense dream review sent to the sage");
    Ok(())
}

/// Take the message `envelope` that arrived for the ant, whose sense data `items` holds, when it is its sage's answer
/// to the review of its last dream: none when it is not; true to prompt the ant with it, as it was made the sage's
/// findings in their marked block; false to ack it unprompted, as an answer without findings, a late one, whose
/// findings the next dream offers, or one that leaves the ant to be woken with the packet alone. A notice that the
/// sage gave up on the review or had it withdrawn leaves the ant to the packet alone; any other one waits for the
/// answer. A reply of the sage to the review of the dream before the last, come after the next dream began, is dropped:
/// acked unprompted.
pub(crate) async fn review_answer(
    sense: &ants_sense::Sense,
    items: &dyn Items,
    envelope: &mut Envelope,
) -> Result<Option<bool>> {
    let Some((last, _)) = dream::last(items).await? else { return Ok(None) };
    let reviewing = |dream: &Dream| {
        dream.sage.as_ref() == Some(&envelope.from)
            && dream.review.as_ref().is_some_and(|review| review.thread == envelope.thread_id)
    };
    if !reviewing(&last) {
        let before = match last.number - 1 {
            number if number > 0 && envelope.in_reply_to.is_some() => dream::get(items, number).await?,
            _ => None,
        };
        if let Some((before, _)) = before.filter(|(before, _)| reviewing(before)) {
            tracing::info!(sage = %envelope.from, dream = before.number, "the sage's answer came after the next dream began: dropped");
            return Ok(Some(false));
        }
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
        Reviewed::Late(findings) => {
            tracing::info!(%sage, dream, findings, "the sage's answer came after the ant dreamed on the packet alone: the next dream offers its findings");
            Ok(Some(false))
        }
        Reviewed::Fallback => {
            tracing::warn!(%sage, dream, "the sage's answer holds no readable findings: the ant dreams on the packet alone");
            sense.changed(&envelope.to);
            Ok(Some(false))
        }
    }
}

/// Note in `items`, a sage's sense data, the review message `envelope` that arrived from an ant ([`dream::review_received`]):
/// the review's record made, and what is new in the verdicts and outcomes of the sage's earlier findings applied to
/// the records of the reviews that made them. Nothing when the message tells no dream; nothing new when it arrives again.
pub(crate) async fn review_received(items: &dyn Items, envelope: &Envelope) -> Result<()> {
    let Some(told) = &envelope.review_outcomes else { return Ok(()) };
    let (ant, thread) = (&envelope.from, &envelope.thread_id);
    if dream::review_received(items, ant, thread, told, Utc::now()).await? {
        tracing::info!(%ant, dream = told.dream, outcomes = told.findings.len(), "sense noted a review asked of the sage");
    }
    Ok(())
}

/// Note in `items`, a sage's sense data, its answer `text` to the review message `request` asked
/// ([`dream::review_answered`]): the findings it gives, by kind, and what its thread `spent`, when known. Nothing when
/// the message tells no dream; once answered, a review keeps its first answer.
pub(crate) async fn review_given(
    items: &dyn Items,
    request: &Envelope,
    text: &str,
    spent: Option<ReviewSpend>,
) -> Result<()> {
    let Some(told) = &request.review_outcomes else { return Ok(()) };
    let (ant, thread) = (&request.from, &request.thread_id);
    if dream::review_answered(items, ant, thread, told, text, spent, Utc::now()).await? {
        tracing::info!(%ant, dream = told.dream, "sense noted the sage's answer to a review");
    }
    Ok(())
}

/// Wake `ant` on a new thread with the packet of `dream`, covering `threads`, and note the thread.
async fn wake(items: &dyn Items, host: &dyn Host, ant: &UserId, dream: &Dream, threads: &[Thread]) -> Result<()> {
    let text = dream::packet(items, dream, &Reader::Ant(ant.clone()), threads).await?;
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
    /// The domain `js` was made for.
    pub js_domain: Option<JsDomain>,
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
    /// Reads the colony: the sage's entry, and the entries naming this ant as their sage.
    pub colony: Colony,
    /// Sends the review packets, redacted.
    pub outbox: Outbox,
    /// Asks for the reactions to the ant's messages, as `list_reactions` does.
    pub reactions: Option<ReactionAsker>,
}

impl Dreaming for Daemon {
    fn idle_since(&self) -> Option<DateTime<Utc>> {
        (self.workload.handling() == 0).then(|| self.workload.worked().unwrap_or(self.started))
    }

    fn threads(&self, since: DateTime<Utc>, until: DateTime<Utc>) -> BoxFuture<'_, Result<Vec<Thread>>> {
        Box::pin(async move {
            let (from, to) = (SystemTime::from(since), SystemTime::from(until));
            let records = history::ReviewRecords { js_domain: self.js_domain.as_ref(), origins: self.origins.as_ref() };
            let active = history::active(&self.js, &self.ant, self.reading, records, (from, to), PACKET_BUDGET).await?;
            let mut threads = Vec::with_capacity(active.len());
            let mut actions =
                actions::Actions::new(&self.js, self.js_domain.as_ref(), &self.ant, self.reactions.as_ref()).await;
            for history::Active {
                thread: id,
                origin,
                history: text,
                timeline,
                sent,
                reopenings,
                rejections,
                ends,
                escalations,
            } in active
            {
                let untrusted = origin.as_ref().is_some_and(|origin| !origin.trusted());
                let review = origin.as_ref().is_some_and(|origin| matches!(origin.by, Starter::Review { .. }));
                let started_by = origin.map_or_else(|| "someone not known".to_owned(), |origin| starter(&origin.by));
                let actions = actions.of(&id, &sent, &rejections, &reopenings, (since, until)).await;
                let signs = vitals::signs(&ends, escalations, &actions);
                threads.push(Thread { id, untrusted, review, started_by, text, timeline, actions, signs });
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

    fn sage(&self) -> Option<UserId> {
        self.sage.clone()
    }

    fn colony(&self) -> BoxFuture<'_, Result<Vec<ColonyEntry>>> {
        (self.colony)()
    }

    fn review<'a>(&'a self, sage: &'a UserId, dream: &'a Dream, text: String) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let Some(review) = &dream.review else { return Ok(()) };
            let mut envelope = Envelope::on_thread(review.thread.clone(), self.ant.clone(), sage.clone(), text)
                .with_allow(Some(REVIEW_ALLOW.map(str::to_owned).to_vec()));
            envelope.id = review.message;
            envelope.review_commits = review_commits(&self.workdir, &dream.commits).await;
            envelope.review_outcomes = dream.review_outcomes();
            (self.outbox)(envelope).await
        })
    }
}

/// Whether `ant` sends `sage` its review packets, given the colony's `entries` at `now`: never to itself or while
/// a live entry of another ant names `ant` as its sage, nor while no daemon serves `sage` ([`crate::mcp::why_unserved`]).
pub(super) fn serving_sage(ant: &UserId, sage: &UserId, entries: &[ColonyEntry], now: DateTime<Utc>) -> bool {
    if ant == sage {
        return false;
    }
    if let Some(reviewed) = crate::colony::naming_as_sage(ant, entries, now) {
        tracing::debug!(
            %ant, %sage, reviewed = %reviewed.ant,
            "the ant dreams without its sage: a live ant names it as its sage, and a sage is reviewed by no one"
        );
        return false;
    }
    let entry = entries.iter().find(|entry| &entry.ant == sage);
    let wait = std::time::Duration::from_secs(SAGE_TIMEOUT_HOURS.unsigned_abs() * 3600);
    match crate::mcp::why_unserved(sage, entry, now, wait) {
        None => true,
        Some(why) => {
            tracing::info!(%ant, %sage, why, "the ant dreams without its sage");
            false
        }
    }
}

/// `commits`, of the repository of `workdir`, as a review message names them: with the git directory they are in, for
/// a sage's daemon on this machine to fetch them from.
pub(super) async fn review_commits(workdir: &Path, commits: &[Commit]) -> Vec<ReviewCommit> {
    let local = if commits.is_empty() {
        None
    } else {
        let args = ["rev-parse", "--path-format=absolute", "--git-common-dir"];
        let dir = crate::worktrees::run("git", workdir, args, &[], Some(GIT_TIMEOUT)).await;
        dir.inspect_err(|e| tracing::info!(error = format!("{e:#}"), "the review names no local repository"))
            .ok()
            .map(|dir| dir.trim().to_owned())
    };
    let named = |commit: &Commit| ReviewCommit {
        repo: commit.repo.clone(),
        commit: commit.commit.clone(),
        thread: commit.thread.clone(),
        local: local.clone(),
    };
    commits.iter().map(named).collect()
}

fn starter(by: &Starter) -> String {
    match by {
        Starter::Human { user } => format!("human {user}"),
        Starter::Ant { user } => format!("ant {user}"),
        Starter::Webhook { user } => format!("webhook {user}"),
        Starter::Review { user } => format!("ant {user}, asking a review"),
        Starter::Provider { name } => format!("provider {name}"),
        Starter::Unknown => "someone of a kind not known".to_owned(),
    }
}
