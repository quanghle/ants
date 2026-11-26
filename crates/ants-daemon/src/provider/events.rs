//! The lifecycle events of the ants a process serves ([`Event`]), told to each ant's provider when it agreed on
//! [`Capability::Events`]: in order per ant and at least once, a repeat carrying the same id. An event says what
//! happened, to which ant and identity, on which thread and when: never what a message, prompt or reply said, a
//! thread planned carrying only the plan the harness reported ([`Lifecycle::thread_planned`]).
//!
//! An ant's events wait in its home, one JSON [`Event`] a line in [`QUEUE`], from when they happen until its provider
//! acks them, so a restarted daemon tells those left. Each home's queue has a lock, a file and, while it holds
//! events, a delivering task of its own ([`Outbox`]), so storing or telling one ant's events waits for no other
//! ant's; its file is read and written whole in the blocking pool, an event stored before whoever tells of it goes
//! on. The task tells the ant's next event only once its provider acked the one before, retrying with a backoff while
//! the provider fails or is not connected, and holds the queue's lock between the calls only. Past [`CAP`] waiting,
//! an ant's oldest go, with a warning. An ant renamed takes the events queued under its earlier names, as its
//! provider serves it under its new one.
//!
//! Beside the queue, [`IDENTITY`] holds the id of the identity record the provider was told of, so a daemon tells of an
//! ant new to its home ([`EventKind::AntAdded`]) once; and [`RETIRED`] marks an ant retired ([`mark_retired`]) until
//! a daemon serves it again ([`EventKind::AntRevived`]).

use std::collections::{HashMap, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ants_proto::provider::{BoxFuture, Capability, Connected, Event, EventKind, Origin, RpcError};
use ants_proto::{PlanStep, ThreadId, TimelineEntry, UserId};
use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::sync::MutexExt as _;

pub mod done;

/// The directory of an ant's home holding its lifecycle files, which neither a clone nor an export copies.
pub const DIR: &str = "lifecycle";
/// The queue of an ant's events not yet acked, under [`DIR`].
pub const QUEUE: &str = "events.jsonl";
/// The id of the identity record told of, under [`DIR`].
pub const IDENTITY: &str = "identity";
/// The mark of a retired ant, under [`DIR`].
pub const RETIRED: &str = "retired";
/// How many events an ant's queue keeps at most.
pub const CAP: usize = 1_000;
/// How long a provider has to ack an event before it is sent again.
const CALL_WAIT: Duration = Duration::from_secs(30);
/// The backoff after a first failure, doubling with each next one up to [`LAST_RETRY`].
const FIRST_RETRY: Duration = Duration::from_secs(1);
const LAST_RETRY: Duration = Duration::from_mins(5);
/// How long a stopping daemon waits for the provider to ack an ant's retirement before the ant leaves the colony.
pub const RETIRED_WAIT: Duration = Duration::from_secs(10);

/// What connects the provider of an ant: [`super::Providers::of`] in the daemon.
pub type Resolve = Arc<dyn Fn(UserId) -> BoxFuture<'static, Option<Connected>> + Send + Sync>;

/// The queues of the ants' events, each delivered by a task of its own while it holds any: one a process
/// ([`super::Providers::outbox`]).
#[derive(Clone)]
pub struct Outbox(Arc<Inner>);

struct Inner {
    resolve: Resolve,
    /// The queues by their file, held only to find one: no ant's queue waits for another's.
    queues: std::sync::Mutex<HashMap<PathBuf, Arc<Queue>>>,
    retry: (Duration, Duration),
}

/// The queue of the ant of one home: its file is read and written only under `state`.
struct Queue {
    file: PathBuf,
    state: tokio::sync::Mutex<State>,
    /// Notified each time a sent event settles.
    settled: Notify,
}

/// An ant's events not yet acked, as its queue file holds them.
#[derive(Default)]
struct State {
    /// The name the ant is served under: none until the file is read.
    ant: Option<UserId>,
    events: VecDeque<Event>,
    /// The task delivering the events, none once it found no event left.
    task: Option<JoinHandle<()>>,
}

/// What came of sending an event.
enum Sent {
    Acked,
    /// The provider did not agree on events: the ant's queue goes.
    NotAgreed,
    Failed(String),
}

impl Outbox {
    pub fn new(resolve: Resolve) -> Self {
        Self::with_retry(resolve, FIRST_RETRY, LAST_RETRY)
    }

    /// An outbox retrying after `first`, doubling up to `last`.
    pub fn with_retry(resolve: Resolve, first: Duration, last: Duration) -> Self {
        Self(Arc::new(Inner { resolve, queues: std::sync::Mutex::default(), retry: (first, last) }))
    }

    /// Whether `ant`'s provider may take events: false only when it is connected without agreeing on them.
    pub async fn accepts(&self, ant: &UserId) -> bool {
        (self.0.resolve)(ant.clone()).await.is_none_or(|provider| provider.has(Capability::Events))
    }

    /// Read the queue of `ant`, whose home is `home`, from its file, and deliver what it holds: the events queued
    /// under an earlier name of the ant as its own ([`State::read`]), also when the queue was read under that name.
    pub(crate) async fn open(&self, ant: &UserId, home: &Path) {
        let queue = self.queue(home);
        let mut state = queue.state.lock().await;
        state.read(&queue.file, home, ant).await;
        self.deliver(&queue, &mut state);
    }

    /// Queue `event` for its ant, whose home is `home`, stored before it returns, and deliver it.
    pub async fn push(&self, home: &Path, event: Event) {
        if let Err(e) = self.store(home, event).await {
            tracing::warn!(error = format!("{e:#}"), "storing a lifecycle event");
        }
    }

    /// [`Outbox::push`], failing when the event could not be stored: it is delivered all the same while the process
    /// runs.
    pub async fn store(&self, home: &Path, event: Event) -> Result<()> {
        let queue = self.queue(home);
        let mut state = queue.state.lock().await;
        if state.ant.is_none() {
            state.read(&queue.file, home, &event.ant).await;
        }
        let stored = state.push(&queue.file, event).await;
        self.deliver(&queue, &mut state);
        stored
    }

    /// The events not yet acked of the ant whose home is `home`.
    pub async fn pending(&self, home: &Path) -> Vec<Event> {
        match self.known(home) {
            Some(queue) => queue.state.lock().await.events.iter().cloned().collect(),
            None => Vec::new(),
        }
    }

    /// Wait up to `wait` for the event `id` of the ant whose home is `home` to leave its queue, acked or dropped:
    /// whether it did.
    pub async fn delivered(&self, home: &Path, id: Uuid, wait: Duration) -> bool {
        let Some(queue) = self.known(home) else { return true };
        let waiting = async {
            loop {
                let settled = queue.settled.notified();
                let mut settled = std::pin::pin!(settled);
                settled.as_mut().enable();
                let state = queue.state.lock().await;
                if state.events.iter().all(|event| event.id != id) {
                    return;
                }
                drop(state);
                settled.await;
            }
        };
        tokio::time::timeout(wait, waiting).await.is_ok()
    }

    /// The queue of the ant whose home is `home`, its file not read yet when new.
    fn queue(&self, home: &Path) -> Arc<Queue> {
        let file = queue_file(home);
        let mut queues = self.0.queues.locked();
        let queue = queues.entry(file).or_insert_with_key(|file| {
            Arc::new(Queue { file: file.clone(), state: tokio::sync::Mutex::default(), settled: Notify::new() })
        });
        queue.clone()
    }

    /// The queue of the ant whose home is `home`, if any was read.
    fn known(&self, home: &Path) -> Option<Arc<Queue>> {
        self.0.queues.locked().get(&queue_file(home)).cloned()
    }

    /// Start the task delivering `queue`, whose `state` the caller holds, when it has events and no task: on its
    /// first event, or after the runtime its task ran on stopped.
    fn deliver(&self, queue: &Arc<Queue>, state: &mut State) {
        if !state.events.is_empty() && state.task.as_ref().is_none_or(JoinHandle::is_finished) {
            state.task = Some(tokio::spawn(deliver(self.0.clone(), queue.clone())));
        }
    }
}

/// Deliver the events of `queue` until none is left: its first, the next once the provider acked it, sent again after
/// a backoff while it fails. The queue is held between the calls only, never across one.
async fn deliver(inner: Arc<Inner>, queue: Arc<Queue>) {
    let mut backoff = inner.retry.0;
    loop {
        let mut state = queue.state.lock().await;
        let (Some(ant), Some(event)) = (state.ant.clone(), state.events.front().cloned()) else {
            state.task = None;
            return;
        };
        drop(state);
        let id = event.id;
        let sent = match (inner.resolve)(ant.clone()).await {
            None => Sent::Failed("no provider connected".to_owned()),
            Some(provider) => match tokio::time::timeout(CALL_WAIT, provider.event(event)).await {
                Ok(Ok(_)) => Sent::Acked,
                Ok(Err(e)) if e.code == RpcError::NOT_AGREED => Sent::NotAgreed,
                Ok(Err(e)) => Sent::Failed(e.to_string()),
                Err(_) => Sent::Failed(format!("no ack within {}s", CALL_WAIT.as_secs())),
            },
        };
        let mut state = queue.state.lock().await;
        let mut retry_in = None;
        match sent {
            Sent::Acked => {
                // An event told under a name the ant has left since stays, to be told under its current one.
                if state.events.front().is_some_and(|first| first.id == id && first.ant == ant) {
                    state.events.pop_front();
                    state.store(&queue.file).await;
                }
                backoff = inner.retry.0;
            }
            Sent::NotAgreed => {
                tracing::info!(%ant, dropped = state.events.len(), "the provider takes no events; dropping the ant's");
                state.events.clear();
                state.store(&queue.file).await;
            }
            Sent::Failed(why) => {
                tracing::warn!(
                    %ant,
                    event = %id,
                    error = why,
                    retry_in = backoff.as_secs_f64(),
                    "telling the provider of a lifecycle event"
                );
                retry_in = Some(backoff);
                backoff = (backoff * 2).min(inner.retry.1);
            }
        }
        drop(state);
        queue.settled.notify_waiters();
        if let Some(retry_in) = retry_in {
            tokio::time::sleep(retry_in).await;
        }
    }
}

impl State {
    /// Read the events of `ant` from `file`, the queue of its home `home`: those queued under an earlier name of the
    /// ant ([`crate::sense::earlier_names`]) told as its own, as the name they carry is no longer served, the file
    /// written again when any was.
    async fn read(&mut self, file: &Path, home: &Path, ant: &UserId) {
        self.events = blocking(read(file.to_owned())).await;
        self.ant = Some(ant.clone());
        let renamed = rename(home, ant, &mut self.events);
        if self.trim(file) > 0 || renamed {
            self.store(file).await;
        }
    }

    /// Add `event`, appended to `file`, or the file written again when the oldest went past [`CAP`].
    async fn push(&mut self, file: &Path, event: Event) -> Result<()> {
        let line = serde_json::to_string(&event).context("writing a lifecycle event")? + "\n";
        self.events.push_back(event);
        if self.trim(file) > 0 {
            return self.rewrite(file).await.with_context(|| format!("writing {}", file.display()));
        }
        blocking(append(file.to_owned(), line)).await.with_context(|| format!("appending to {}", file.display()))
    }

    /// Drop the oldest events past [`CAP`]: how many.
    fn trim(&mut self, file: &Path) -> usize {
        let over = self.events.len().saturating_sub(CAP);
        if over > 0 {
            self.events.drain(..over);
            tracing::warn!(file = %file.display(), dropped = over, "dropping the oldest lifecycle events past the cap");
        }
        over
    }

    /// Write `file` again with the events left, removed when none is.
    async fn store(&self, file: &Path) {
        if let Err(e) = self.rewrite(file).await {
            tracing::warn!(file = %file.display(), error = format!("{e:#}"), "storing the lifecycle events");
        }
    }

    async fn rewrite(&self, file: &Path) -> Result<()> {
        let mut text = String::new();
        for event in &self.events {
            text += &serde_json::to_string(event)?;
            text.push('\n');
        }
        blocking(rewrite(file.to_owned(), text)).await
    }
}

/// Give `ant` the events of `events` carrying an earlier name of it, as `home` lists them: whether any carried one.
fn rename(home: &Path, ant: &UserId, events: &mut VecDeque<Event>) -> bool {
    if events.iter().all(|event| event.ant == *ant) {
        return false;
    }
    let earlier = match crate::sense::earlier_names(home) {
        Ok(earlier) => earlier,
        Err(e) => {
            tracing::warn!(
                %ant,
                error = format!("{e:#}"),
                "reading the earlier names of an ant for its lifecycle events"
            );
            return false;
        }
    };
    let mut renamed = false;
    for event in events.iter_mut().filter(|event| earlier.contains(&event.ant)) {
        event.ant = ant.clone();
        renamed = true;
    }
    renamed
}

/// Run `work`, the whole of one read or write of a queue file, off the runtime's thread: done once this returns, and
/// done all the same when the caller stops waiting.
async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    match tokio::task::spawn_blocking(work).await {
        Ok(done) => done,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

/// The events `file` holds.
fn read(file: PathBuf) -> impl FnOnce() -> VecDeque<Event> {
    move || {
        let mut events = VecDeque::new();
        match std::fs::read_to_string(&file) {
            Ok(text) => {
                for line in text.lines().filter(|line| !line.trim().is_empty()) {
                    match serde_json::from_str(line) {
                        Ok(event) => events.push_back(event),
                        Err(e) => tracing::warn!(file = %file.display(), error = %e, "skipping an unreadable event"),
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(file = %file.display(), error = %e, "reading the queue of lifecycle events"),
        }
        events
    }
}

fn append(file: PathBuf, line: String) -> impl FnOnce() -> Result<()> {
    move || {
        if let Some(dir) = file.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut out = std::fs::OpenOptions::new().create(true).append(true).open(&file)?;
        out.write_all(line.as_bytes())?;
        out.sync_data()?;
        Ok(())
    }
}

/// Write `file` again as `text`, removed when that is empty.
fn rewrite(file: PathBuf, text: String) -> impl FnOnce() -> Result<()> {
    move || {
        if text.is_empty() {
            return match std::fs::remove_file(&file) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
                _ => Ok(()),
            };
        }
        let tmp = file.with_extension("jsonl.tmp");
        let mut out = std::fs::File::create(&tmp)?;
        out.write_all(text.as_bytes())?;
        out.sync_all()?;
        std::fs::rename(&tmp, &file)?;
        Ok(())
    }
}

fn queue_file(home: &Path) -> PathBuf {
    home.join(DIR).join(QUEUE)
}

/// An event of `ant`, whose identity id is `identity`, happening now.
pub fn event(id: Uuid, ant: &UserId, identity: &str, thread: Option<&ThreadId>, kind: EventKind) -> Event {
    Event { id, ant: ant.clone(), identity: identity.to_owned(), thread: thread.cloned(), at: Utc::now(), kind }
}

/// An id derived from `parts`, the same each time an event is told again for the same cause.
pub fn derived(parts: &[&str]) -> Uuid {
    let digest = ring::digest::digest(&ring::digest::SHA256, parts.join("\0").as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_ref()[..16]);
    uuid::Builder::from_custom_bytes(bytes).into_uuid()
}

/// The lifecycle of one ant a daemon serves: its events go to `outbox`.
#[derive(Clone)]
pub struct Lifecycle {
    ant: UserId,
    identity: String,
    home: PathBuf,
    outbox: Outbox,
    /// The ant's working directory, whose repository its threads start in ([`Lifecycle::with_workdir`]).
    workdir: Option<PathBuf>,
}

/// The mark of a retired ant ([`RETIRED`]): the id and time of its [`EventKind::AntRetired`], and whether it was told.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Retired {
    id: Uuid,
    at: DateTime<Utc>,
    #[serde(default)]
    told: bool,
}

impl Lifecycle {
    /// The lifecycle of `ant`, whose identity id is `identity` and home `home`, as a daemon starts serving it: its
    /// queue read, then told of an ant new to its home, or of one served again after its retirement. None when its
    /// provider takes no events.
    pub async fn open(outbox: &Outbox, ant: &UserId, identity: &str, home: &Path) -> Option<Self> {
        if !outbox.accepts(ant).await {
            return None;
        }
        outbox.open(ant, home).await;
        let (identity, home) = (identity.to_owned(), home.to_owned());
        let lifecycle = Self { ant: ant.clone(), identity, home, outbox: outbox.clone(), workdir: None };
        lifecycle.started().await;
        Some(lifecycle)
    }

    /// Tell of an ant new to its home, its identity not the one told of there, or else of one marked retired served
    /// again, its retirement first when that was not told.
    async fn started(&self) {
        let (ant, identity, home) = (&self.ant, self.identity.as_str(), &self.home);
        let known = tokio::fs::read_to_string(home.join(DIR).join(IDENTITY)).await.ok();
        let retired = read_retired(home).await;
        if known.as_deref().map(str::trim) != Some(identity) {
            let id = derived(&[ant.as_str(), identity, "ant_added"]);
            self.push(event(id, ant, identity, None, EventKind::AntAdded)).await;
            if let Err(e) = write_identity(home, identity) {
                tracing::warn!(error = format!("{e:#}"), "noting the identity told of the ant");
            }
        } else if let Some(retired) = &retired {
            if !retired.told {
                self.retired(retired).await;
            }
            let id = derived(&[ant.as_str(), identity, &retired.id.to_string(), "ant_revived"]);
            self.push(event(id, ant, identity, None, EventKind::AntRevived)).await;
        }
        if retired.is_some() {
            unmark_retired(home);
        }
    }

    /// Tell of `kind`, now, on `thread` when there is one.
    pub async fn emit(&self, kind: EventKind, thread: Option<&ThreadId>) {
        self.push(event(Uuid::new_v4(), &self.ant, &self.identity, thread, kind)).await;
    }

    /// The lifecycle telling of threads started in the repository of `workdir`, the ant's working directory.
    #[must_use]
    pub fn with_workdir(self, workdir: PathBuf) -> Self {
        Self { workdir: Some(workdir), ..self }
    }

    /// Tell of `thread` started by message `message` from `origin`, in the repository of the ant's working directory:
    /// told again with the same id when the message is.
    pub async fn thread_started(&self, thread: &ThreadId, message: &str, origin: Origin) {
        let id = derived(&[self.ant.as_str(), thread.as_str(), message, "thread_started"]);
        let repo = self.workdir.as_deref().and_then(super::context::repository);
        self.push(event(id, &self.ant, &self.identity, Some(thread), EventKind::ThreadStarted { origin, repo })).await;
    }

    /// Tell of the plan `steps` a prompt on `thread` ended with, as the harness last reported it, cut to what a
    /// timeline record keeps.
    pub async fn thread_planned(&self, thread: &ThreadId, steps: &[PlanStep]) {
        let TimelineEntry::Plan { steps } = TimelineEntry::plan(steps) else { return };
        self.emit(EventKind::ThreadPlanned { steps }, Some(thread)).await;
    }

    /// Tell of `thread` marked done at `at`, by the done state stored at sequence `seq` of the stream of
    /// [`ants_proto::THREADS_BUCKET`]: told again with the same id when that one is. Fails when the event could not be
    /// stored.
    pub async fn thread_done(&self, thread: &ThreadId, seq: u64, at: DateTime<Utc>) -> Result<()> {
        let id = derived(&[self.ant.as_str(), thread.as_str(), &seq.to_string(), "thread_done"]);
        let told = event(id, &self.ant, &self.identity, Some(thread), EventKind::ThreadDone);
        self.outbox.store(&self.home, Event { at, ..told }).await
    }

    /// Tell of a retirement once the daemon stopped serving the ant: when its home is marked retired
    /// ([`mark_retired`]) and that was not told yet. Waits up to `wait` for the provider's ack, so it records the
    /// retirement before the ant leaves the colony and its credentials are revoked.
    pub async fn stopped(&self, wait: Duration) {
        let Some(retired) = read_retired(&self.home).await.filter(|retired| !retired.told) else { return };
        self.retired(&retired).await;
        let told = Retired { told: true, ..retired };
        if let Err(e) = write_retired(&self.home, &told) {
            tracing::warn!(error = format!("{e:#}"), "noting the retirement of the ant told");
        }
        if !self.outbox.delivered(&self.home, told.id, wait).await {
            tracing::warn!(waited = wait.as_secs_f64(), "the provider has not acked the retirement of the ant yet");
        }
    }

    async fn retired(&self, retired: &Retired) {
        let told = event(retired.id, &self.ant, &self.identity, None, EventKind::AntRetired);
        self.push(Event { at: retired.at, ..told }).await;
    }

    async fn push(&self, event: Event) {
        self.outbox.push(&self.home, event).await;
    }
}

/// Mark the ant whose home is `home` retired, before its retirement stops it: the daemon serving it tells of it once
/// it stopped ([`Lifecycle::stopped`]), the one serving it again of its revival ([`Lifecycle::open`]).
pub fn mark_retired(home: &Path) -> Result<()> {
    write_retired(home, &Retired { id: Uuid::new_v4(), at: Utc::now(), told: false })
}

/// The retirement the ant whose home is `home` is marked with ([`mark_retired`]), if any: each marks it anew.
pub async fn retirement(home: &Path) -> Option<Uuid> {
    read_retired(home).await.map(|retired| retired.id)
}

/// Take back [`mark_retired`]: for a retirement that failed, or an ant served again.
pub fn unmark_retired(home: &Path) {
    if let Err(e) = std::fs::remove_file(home.join(DIR).join(RETIRED))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %e, "removing the mark of a retired ant");
    }
}

/// Queue `kinds`, in order, in the home `home` of `ant`, whose identity id is `identity`, before any daemon serves it
/// there, and note the identity told of: for an ant cloned or imported, which is then not told of as added.
pub fn arrived(home: &Path, ant: &UserId, identity: &str, kinds: Vec<EventKind>) -> Result<()> {
    let file = queue_file(home);
    let dir = home.join(DIR);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut text = String::new();
    for kind in kinds {
        text += &serde_json::to_string(&event(Uuid::new_v4(), ant, identity, None, kind))?;
        text.push('\n');
    }
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file)
        .with_context(|| format!("opening {}", file.display()))?;
    std::io::Write::write_all(&mut out, text.as_bytes()).with_context(|| format!("writing {}", file.display()))?;
    out.sync_data().with_context(|| format!("writing {}", file.display()))?;
    write_identity(home, identity)
}

fn write_identity(home: &Path, identity: &str) -> Result<()> {
    write_marker(home, IDENTITY, identity.as_bytes())
}

async fn read_retired(home: &Path) -> Option<Retired> {
    let bytes = tokio::fs::read(home.join(DIR).join(RETIRED)).await.ok()?;
    serde_json::from_slice(&bytes).inspect_err(|e| tracing::warn!(error = %e, "reading the mark of a retired ant")).ok()
}

fn write_retired(home: &Path, retired: &Retired) -> Result<()> {
    write_marker(home, RETIRED, &serde_json::to_vec(retired)?)
}

fn write_marker(home: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let dir = home.join(DIR);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let file = dir.join(name);
    std::fs::write(&file, bytes).with_context(|| format!("writing {}", file.display()))
}

#[cfg(test)]
pub(crate) mod tests;
