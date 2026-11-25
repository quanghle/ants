//! Telling when the NATS server a daemon's connections go to, or the JetStream behind it, stays out of reach. Each
//! connection made with [`super::connect_retrying`], and the host connection, reports when it goes down, as a failed
//! attempt or a disconnection, and when it is up again; each ant's colony heartbeats report whether JetStream answered
//! them, which it does not through a leaf node whose uplink to the hub is down while the leaf node itself answers
//! ([`Reporter`]). The process's [`watch`] logs one ERROR for a server, or its JetStream, that some reporter has been
//! unable to reach for longer than its threshold, naming its address and what runs the server, or the uplink, there,
//! and, once every reporter reaches it again, an INFO saying how long it was out. Reporters of the same server make
//! one line between them, however many ants the daemon serves; while the server is out of reach, its JetStream's
//! outage waits to be told until the server is back.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;

use super::connect::redacted;
use crate::sync::MutexExt as _;

/// How long a daemon's connections stay unable to reach their NATS server, or its JetStream, before the ERROR saying
/// so.
pub const OUT_OF_REACH: Duration = Duration::from_mins(1);

/// What tells what runs the server, or the way to its JetStream, at an address ([`watch`]).
type Runs = Arc<dyn Fn(&str, Of) -> Option<String> + Send + Sync>;

/// The process's watch, with its id, if one runs.
static WATCH: Mutex<Option<(u64, UnboundedSender<Report>)>> = Mutex::new(None);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// What is out of reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Of {
    /// The NATS server.
    Server,
    /// The JetStream reached through a server that answers, as the hub's through a leaf node.
    JetStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    Down,
    Up,
    /// The reporter is gone: its connection closed or never made, its heartbeats stopped.
    Gone,
}

#[derive(Debug)]
struct Report {
    url: Arc<str>,
    of: Of,
    reporter: u64,
    change: Change,
}

/// What one connection to `url`, or one ant's heartbeats over it, tells the process's watch, if one runs; dropping it
/// tells it is gone.
#[derive(Debug)]
pub struct Reporter {
    url: Arc<str>,
    of: Of,
    id: u64,
}

impl Reporter {
    /// The reporter of a connection to the server at `url`.
    pub fn new(url: &str) -> Arc<Self> {
        Arc::new(Self::of(url, Of::Server))
    }

    /// The reporter of whether JetStream answers through the server at `url` ([`Self::answered`]).
    pub fn jetstream(url: &str) -> Self {
        Self::of(url, Of::JetStream)
    }

    fn of(url: &str, of: Of) -> Self {
        Self { url: url.into(), of, id: NEXT_ID.fetch_add(1, Ordering::Relaxed) }
    }

    /// An attempt to connect failed, the connection was lost, or JetStream did not answer.
    pub fn down(&self) {
        self.send(Change::Down);
    }

    /// The connection is up, or JetStream answered.
    pub fn up(&self) {
        self.send(Change::Up);
    }

    /// Report what the connection's `event` tells.
    pub fn event(&self, event: &async_nats::Event) {
        match event {
            async_nats::Event::Disconnected => self.down(),
            async_nats::Event::Connected => self.up(),
            async_nats::Event::Closed => self.send(Change::Gone),
            _ => {}
        }
    }

    /// Report what the outcome of a JetStream request tells: answered when it succeeded, unanswered when it got no
    /// responders or no answer in time ([`super::unanswered`]), nothing on another error, which JetStream may have
    /// answered or not.
    pub fn answered(&self, outcome: Result<(), &anyhow::Error>) {
        match outcome {
            Ok(()) => self.up(),
            Err(e) if super::unanswered(e) => self.down(),
            Err(_) => {}
        }
    }

    fn send(&self, change: Change) {
        if let Some((_, watch)) = &*WATCH.locked() {
            let _ = watch.send(Report { url: self.url.clone(), of: self.of, reporter: self.id, change });
        }
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        self.send(Change::Gone);
    }
}

/// The process's watch ([`watch`]), until dropped.
pub struct Watching {
    id: u64,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Watching {
    fn drop(&mut self) {
        let mut watch = WATCH.locked();
        if watch.as_ref().is_some_and(|(id, _)| *id == self.id) {
            *watch = None;
        }
        self.task.abort();
    }
}

/// Watch the reporters of the process, in place of any watch before, for as long as the result is kept: a server, or
/// its JetStream, that some reporter has been unable to reach for longer than `after` gets one ERROR naming its address
/// and what `runs` tells about what runs the server, or the way to its JetStream, there, if anything, asked then, apart
/// from the runtime, as it may run commands. Needs a tokio runtime.
pub fn watch(after: Duration, runs: impl Fn(&str, Of) -> Option<String> + Send + Sync + 'static) -> Watching {
    let (tx, rx) = unbounded_channel();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    *WATCH.locked() = Some((id, tx));
    Watching { id, task: tokio::spawn(run(after, Arc::new(runs), rx)) }
}

/// A server, or its JetStream, some reporter cannot reach.
struct Outage {
    /// The reporters that cannot.
    down: HashSet<u64>,
    since: Instant,
    told: bool,
}

type Outages = HashMap<(Arc<str>, Of), Outage>;

async fn run(after: Duration, runs: Runs, mut reports: UnboundedReceiver<Report>) {
    let mut outages: Outages = HashMap::new();
    loop {
        let due = outages
            .iter()
            .filter(|((url, of), o)| {
                !o.told && !(*of == Of::JetStream && outages.contains_key(&(url.clone(), Of::Server)))
            })
            .min_by_key(|(_, o)| o.since)
            .map(|(key, o)| (key.clone(), o.since + after));
        let overdue = async {
            match &due {
                Some((_, at)) => tokio::time::sleep_until(*at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            report = reports.recv() => match report {
                Some(report) => take(&mut outages, report),
                None => return,
            },
            () = overdue => {
                let Some(((url, of), _)) = due else { continue };
                let asked = url.clone();
                let runs = runs.clone();
                let what = tokio::task::spawn_blocking(move || runs(&asked, of)).await.ok().flatten();
                let shown = redacted(&url);
                match of {
                    Of::Server => {
                        let what = what.unwrap_or_else(|| "nothing on this machine is known to run it".to_owned());
                        tracing::error!(url = %shown, "the NATS server is out of reach for over {after:?}: {what}");
                    }
                    Of::JetStream => {
                        let what = what.unwrap_or_else(|| "`ants doctor` checks the leaf node and its uplink".to_owned());
                        tracing::error!(
                            url = %shown,
                            "JetStream does not answer through the NATS server for over {after:?}, as when a leaf \
                             node's uplink to the hub is down: {what}"
                        );
                    }
                }
                if let Some(outage) = outages.get_mut(&(url, of)) {
                    outage.told = true;
                }
            }
        }
    }
}

/// Take `report` into `outages`, logging an outage told of as over once every reporter reaches it again.
fn take(outages: &mut Outages, Report { url, of, reporter, change }: Report) {
    let key = (url, of);
    if change == Change::Down {
        let outage =
            outages.entry(key).or_insert_with(|| Outage { down: HashSet::new(), since: Instant::now(), told: false });
        outage.down.insert(reporter);
        return;
    }
    let Some(outage) = outages.get_mut(&key) else { return };
    outage.down.remove(&reporter);
    if !outage.down.is_empty() {
        return;
    }
    let outage = outages.remove(&key).expect("just found");
    if outage.told && change == Change::Up {
        let out_for = Duration::from_secs(outage.since.elapsed().as_secs());
        let url = redacted(&key.0);
        match of {
            Of::Server => tracing::info!(%url, ?out_for, "the NATS server answers again"),
            Of::JetStream => tracing::info!(%url, ?out_for, "JetStream answers again"),
        }
    }
}

#[cfg(test)]
mod tests;
