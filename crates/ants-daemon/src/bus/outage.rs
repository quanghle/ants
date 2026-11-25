//! Telling when the NATS server a daemon's connections go to stays out of reach. Each connection made with
//! [`super::connect_retrying`], and the host connection, reports when it goes down, as a failed attempt or a
//! disconnection, and when it is up again ([`Reporter`]). The process's [`watch`] logs one ERROR for a server that some
//! connection has been unable to reach for longer than its threshold, naming its address and what runs it there, and,
//! once every connection to it is up again, an INFO saying how long it was out. Connections to the same server make
//! one line between them, however many ants the daemon serves.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::Instant;

use super::connect::redacted;
use crate::sync::MutexExt as _;

/// How long a daemon's connections stay unable to reach their NATS server before the ERROR saying so.
pub const OUT_OF_REACH: Duration = Duration::from_mins(1);

/// What tells what runs the server at an address ([`watch`]).
type Runs = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The process's watch, with its id, if one runs.
static WATCH: Mutex<Option<(u64, UnboundedSender<Report>)>> = Mutex::new(None);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Change {
    Down,
    Up,
    /// The connection is closed, or was never made.
    Gone,
}

#[derive(Debug)]
struct Report {
    url: Arc<str>,
    connection: u64,
    change: Change,
}

/// What one connection to `url` tells the process's watch, if one runs; dropping it tells the connection is gone.
#[derive(Debug)]
pub struct Reporter {
    url: Arc<str>,
    id: u64,
}

impl Reporter {
    pub fn new(url: &str) -> Arc<Self> {
        Arc::new(Self { url: url.into(), id: NEXT_ID.fetch_add(1, Ordering::Relaxed) })
    }

    /// An attempt to connect failed, or the connection was lost.
    pub fn down(&self) {
        self.send(Change::Down);
    }

    /// The connection is up.
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

    fn send(&self, change: Change) {
        if let Some((_, watch)) = &*WATCH.locked() {
            let _ = watch.send(Report { url: self.url.clone(), connection: self.id, change });
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

/// Watch the connections of the process, in place of any watch before, for as long as the result is kept: a server
/// some connection has been unable to reach for longer than `after` gets one ERROR naming its address and what
/// `runs` tells about what runs the server there, if anything, asked then, apart from the runtime, as it may run
/// commands. Needs a tokio runtime.
pub fn watch(after: Duration, runs: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Watching {
    let (tx, rx) = unbounded_channel();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    *WATCH.locked() = Some((id, tx));
    Watching { id, task: tokio::spawn(run(after, Arc::new(runs), rx)) }
}

/// A server some connection cannot reach.
struct Outage {
    /// The connections to it that are down.
    down: HashSet<u64>,
    since: Instant,
    told: bool,
}

async fn run(after: Duration, runs: Runs, mut reports: UnboundedReceiver<Report>) {
    let mut outages: HashMap<Arc<str>, Outage> = HashMap::new();
    loop {
        let due = outages
            .iter()
            .filter(|(_, o)| !o.told)
            .min_by_key(|(_, o)| o.since)
            .map(|(url, o)| (url.clone(), o.since + after));
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
                let Some((url, _)) = due else { continue };
                let asked = url.clone();
                let runs = runs.clone();
                let what = tokio::task::spawn_blocking(move || runs(&asked)).await.ok().flatten();
                let what = what.unwrap_or_else(|| "nothing on this machine is known to run it".to_owned());
                tracing::error!(url = %redacted(&url), "the NATS server is out of reach for over {after:?}: {what}");
                if let Some(outage) = outages.get_mut(&url) {
                    outage.told = true;
                }
            }
        }
    }
}

/// Take `report` into `outages`, logging an outage told of as over once every connection is up again.
fn take(outages: &mut HashMap<Arc<str>, Outage>, Report { url, connection, change }: Report) {
    if change == Change::Down {
        let outage =
            outages.entry(url).or_insert_with(|| Outage { down: HashSet::new(), since: Instant::now(), told: false });
        outage.down.insert(connection);
        return;
    }
    let Some(outage) = outages.get_mut(&url) else { return };
    outage.down.remove(&connection);
    if !outage.down.is_empty() {
        return;
    }
    let outage = outages.remove(&url).expect("just found");
    if outage.told && change == Change::Up {
        let out_for = Duration::from_secs(outage.since.elapsed().as_secs());
        tracing::info!(url = %redacted(&url), ?out_for, "the NATS server answers again");
    }
}

#[cfg(test)]
mod tests;
