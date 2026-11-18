//! The lifecycle events of the ants a process serves ([`Event`]), told to each ant's provider when it agreed on
//! [`Capability::Events`]: in order per ant and at least once, a repeat carrying the same id. An event says what
//! happened, to which ant and identity, on which thread and when: never what a message, prompt or reply said.
//!
//! An ant's events wait in its home, one JSON [`Event`] a line in [`QUEUE`], from when they happen until its provider
//! acks them, so a restarted daemon tells those left. One task delivers the events of every ant ([`Outbox`]), an ant's
//! next only once its provider acked the one before, retrying with a backoff while the provider fails or is not
//! connected. Past [`CAP`] waiting, an ant's oldest go, with a warning.
//!
//! Beside the queue, [`IDENTITY`] holds the id of the identity record the provider was told of, so a daemon tells of an
//! ant new to its home ([`EventKind::AntAdded`]) once; and [`RETIRED`] marks an ant retired ([`mark_retired`]) until
//! a daemon serves it again ([`EventKind::AntRevived`]).

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ants_proto::provider::{BoxFuture, Capability, Connected, Event, EventKind, Origin, RpcError};
use ants_proto::{ThreadId, UserId};
use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
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

/// What connects the provider of an ant: [`super::of`] in the daemon.
pub type Resolve = Arc<dyn Fn(UserId) -> BoxFuture<'static, Option<Connected>> + Send + Sync>;

/// The outbox of the process, delivering to each ant's provider ([`super::of`]).
pub fn outbox() -> &'static Outbox {
    static OUTBOX: OnceLock<Outbox> = OnceLock::new();
    OUTBOX.get_or_init(|| Outbox::new(Arc::new(|ant: UserId| Box::pin(async move { super::of(&ant).await }))))
}

/// The queues of the ants' events and the one task delivering them, started on the first event.
#[derive(Clone)]
pub struct Outbox(Arc<Inner>);

struct Inner {
    resolve: Resolve,
    queues: tokio::sync::Mutex<HashMap<UserId, Queue>>,
    wake: Notify,
    /// Notified each time a sent event settles ([`Inner::settle`]).
    settled: Notify,
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
    retry: (Duration, Duration),
}

/// An ant's events not yet acked, as its queue file holds them.
struct Queue {
    file: PathBuf,
    events: VecDeque<Event>,
    sending: bool,
    retry_at: Option<Instant>,
    backoff: Duration,
}

/// What came of sending an event.
enum Sent {
    Acked,
    /// The provider did not agree on events: the ant's queue goes.
    NotAgreed,
    Failed(String),
}

type Sending = BoxFuture<'static, (UserId, Uuid, Sent)>;

impl Outbox {
    pub fn new(resolve: Resolve) -> Self {
        Self::with_retry(resolve, FIRST_RETRY, LAST_RETRY)
    }

    /// An outbox retrying after `first`, doubling up to `last`.
    pub fn with_retry(resolve: Resolve, first: Duration, last: Duration) -> Self {
        Self(Arc::new(Inner {
            resolve,
            queues: tokio::sync::Mutex::default(),
            wake: Notify::new(),
            settled: Notify::new(),
            task: std::sync::Mutex::default(),
            retry: (first, last),
        }))
    }

    /// Whether `ant`'s provider may take events: false only when it is connected without agreeing on them.
    pub async fn accepts(&self, ant: &UserId) -> bool {
        (self.0.resolve)(ant.clone()).await.is_none_or(|provider| provider.has(Capability::Events))
    }

    /// Read the queue of `ant`, whose home is `home`, from its file, unless read already, and deliver what it holds.
    pub async fn open(&self, ant: &UserId, home: &Path) {
        let mut queues = self.0.queues.lock().await;
        if queues.contains_key(ant) {
            return;
        }
        let queue = Queue::read(queue_file(home), self.0.retry.0).await;
        let pending = !queue.events.is_empty();
        queues.insert(ant.clone(), queue);
        drop(queues);
        if pending {
            self.wake();
        }
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
        let mut queues = self.0.queues.lock().await;
        let queue = match queues.entry(event.ant.clone()) {
            std::collections::hash_map::Entry::Occupied(queue) => queue.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Queue::read(queue_file(home), self.0.retry.0).await)
            }
        };
        let stored = queue.push(event).await;
        drop(queues);
        self.wake();
        stored
    }

    /// The events of `ant` not yet acked.
    pub async fn pending(&self, ant: &UserId) -> Vec<Event> {
        self.0.queues.lock().await.get(ant).map_or_else(Vec::new, |queue| queue.events.iter().cloned().collect())
    }

    /// Wait up to `wait` for `ant`'s event `id` to leave its queue, acked or dropped: whether it did.
    pub async fn delivered(&self, ant: &UserId, id: Uuid, wait: Duration) -> bool {
        let waiting = async {
            loop {
                let settled = self.0.settled.notified();
                let mut settled = std::pin::pin!(settled);
                settled.as_mut().enable();
                let queues = self.0.queues.lock().await;
                if !queues.get(ant).is_some_and(|queue| queue.events.iter().any(|event| event.id == id)) {
                    return;
                }
                drop(queues);
                settled.await;
            }
        };
        tokio::time::timeout(wait, waiting).await.is_ok()
    }

    /// Wake the delivering task, starting it when not running: on the first event, or after the runtime it ran on
    /// stopped.
    fn wake(&self) {
        let mut task = self.0.task.locked();
        if task.as_ref().is_none_or(JoinHandle::is_finished) {
            *task = Some(tokio::spawn(deliver(self.0.clone())));
        }
        drop(task);
        self.0.wake.notify_one();
    }
}

/// Deliver the events of every ant for good: each ant's first, once due, while none of its is in flight.
async fn deliver(inner: Arc<Inner>) {
    let mut sending: FuturesUnordered<Sending> = FuturesUnordered::new();
    loop {
        let next = inner.send_due(&mut sending).await;
        tokio::select! {
            Some((ant, id, sent)) = sending.next() => inner.settle(&ant, id, sent).await,
            () = inner.wake.notified() => {}
            () = tokio::time::sleep_until(next.unwrap_or_else(Instant::now)), if next.is_some() => {}
        }
    }
}

impl Inner {
    /// Send the first event of each ant that has none in flight and is due, returning when the next is due.
    async fn send_due(&self, sending: &mut FuturesUnordered<Sending>) -> Option<Instant> {
        let now = Instant::now();
        let mut next: Option<Instant> = None;
        for (ant, queue) in self.queues.lock().await.iter_mut() {
            let Some(event) = queue.events.front().filter(|_| !queue.sending) else { continue };
            if let Some(at) = queue.retry_at.filter(|at| *at > now) {
                next = Some(next.map_or(at, |next| next.min(at)));
                continue;
            }
            queue.sending = true;
            let (resolve, ant, event) = (self.resolve.clone(), ant.clone(), event.clone());
            sending.push(Box::pin(async move {
                let id = event.id;
                let sent = match resolve(ant.clone()).await {
                    None => Sent::Failed("no provider connected".to_owned()),
                    Some(provider) => match tokio::time::timeout(CALL_WAIT, provider.event(event)).await {
                        Ok(Ok(_)) => Sent::Acked,
                        Ok(Err(e)) if e.code == RpcError::NOT_AGREED => Sent::NotAgreed,
                        Ok(Err(e)) => Sent::Failed(e.to_string()),
                        Err(_) => Sent::Failed(format!("no ack within {}s", CALL_WAIT.as_secs())),
                    },
                };
                (ant, id, sent)
            }));
        }
        next
    }

    /// Take what came of sending `ant`'s event `id`: drop it once acked, else retry it after the backoff.
    async fn settle(&self, ant: &UserId, id: Uuid, sent: Sent) {
        self.settle_queue(ant, id, sent).await;
        self.settled.notify_waiters();
    }

    async fn settle_queue(&self, ant: &UserId, id: Uuid, sent: Sent) {
        let mut queues = self.queues.lock().await;
        let Some(queue) = queues.get_mut(ant) else { return };
        queue.sending = false;
        match sent {
            Sent::Acked => {
                if queue.events.front().is_some_and(|first| first.id == id) {
                    queue.events.pop_front();
                    queue.store().await;
                }
                (queue.retry_at, queue.backoff) = (None, self.retry.0);
            }
            Sent::NotAgreed => {
                tracing::info!(%ant, dropped = queue.events.len(), "the provider takes no events; dropping the ant's");
                queue.events.clear();
                queue.store().await;
            }
            Sent::Failed(why) => {
                let backoff = queue.backoff;
                tracing::warn!(
                    %ant,
                    event = %id,
                    error = why,
                    retry_in = backoff.as_secs_f64(),
                    "telling the provider of a lifecycle event"
                );
                queue.retry_at = Some(Instant::now() + backoff);
                queue.backoff = (backoff * 2).min(self.retry.1);
            }
        }
    }
}

impl Queue {
    async fn read(file: PathBuf, backoff: Duration) -> Self {
        let mut events = VecDeque::new();
        match tokio::fs::read_to_string(&file).await {
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
        let mut queue = Self { file, events, sending: false, retry_at: None, backoff };
        if queue.trim() > 0 {
            queue.store().await;
        }
        queue
    }

    /// Add `event`, appended to the file, or the file written again when the oldest went past [`CAP`].
    async fn push(&mut self, event: Event) -> Result<()> {
        let line = serde_json::to_string(&event).context("writing a lifecycle event")? + "\n";
        self.events.push_back(event);
        if self.trim() > 0 {
            let file = self.file.display();
            return rewrite(&self.file, &self.events).await.with_context(|| format!("writing {file}"));
        }
        let file = self.file.display();
        append(&self.file, line.as_bytes()).await.with_context(|| format!("appending to {file}"))
    }

    /// Drop the oldest events past [`CAP`]: how many.
    fn trim(&mut self) -> usize {
        let over = self.events.len().saturating_sub(CAP);
        if over > 0 {
            self.events.drain(..over);
            tracing::warn!(file = %self.file.display(), dropped = over, "dropping the oldest lifecycle events past the cap");
        }
        over
    }

    /// Write the file again with the events left, removed when none is.
    async fn store(&self) {
        if let Err(e) = rewrite(&self.file, &self.events).await {
            tracing::warn!(file = %self.file.display(), error = format!("{e:#}"), "storing the lifecycle events");
        }
    }
}

async fn append(file: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(dir) = file.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let mut out = tokio::fs::OpenOptions::new().create(true).append(true).open(file).await?;
    out.write_all(bytes).await?;
    out.sync_data().await?;
    Ok(())
}

async fn rewrite(file: &Path, events: &VecDeque<Event>) -> Result<()> {
    if events.is_empty() {
        return match tokio::fs::remove_file(file).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        };
    }
    let mut text = String::new();
    for event in events {
        text += &serde_json::to_string(event)?;
        text.push('\n');
    }
    let tmp = file.with_extension("jsonl.tmp");
    let mut out = tokio::fs::File::create(&tmp).await?;
    out.write_all(text.as_bytes()).await?;
    out.sync_all().await?;
    tokio::fs::rename(&tmp, file).await?;
    Ok(())
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
        if !self.outbox.delivered(&self.ant, told.id, wait).await {
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
