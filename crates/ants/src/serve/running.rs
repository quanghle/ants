//! What the daemon keeps running and reconciles with the ants and schedules listed on a reload: the ants' tasks, the
//! schedules, the host connection, sense's link of its own and the follower of the shared settings.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use ants_daemon::host::{self, Host};
use ants_daemon::profile::shared::{self, Shared, Via};
use ants_daemon::provider::Providers;
use ants_daemon::schedule::Schedule;
use ants_daemon::{Config, Stop};
use ants_proto::{Machine, UserId};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tracing::Instrument as _;

use super::{SenseLink, Served};

/// The schedules running, each in its task, which stops with them.
#[derive(Default)]
pub(super) struct Schedules(pub(super) HashMap<UserId, ScheduleRun>);

pub(super) struct ScheduleRun {
    schedule: Schedule,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    pub(super) task: JoinHandle<()>,
}

impl Schedules {
    /// Run the schedules `wanted`, started with `spawn`. One no longer listed stops at once, between firings or in
    /// the middle of one, which another daemon or a restart sends as it was not marked sent; one whose entry or
    /// credentials file changed, or which stopped, starts again; the others carry on untouched.
    pub(super) fn reconcile(&mut self, wanted: Vec<Schedule>, spawn: impl Fn(Schedule) -> JoinHandle<()>) {
        let listed: HashSet<UserId> = wanted.iter().map(|s| s.name.clone()).collect();
        for (name, ScheduleRun { task, .. }) in self.0.extract_if(|name, _| !listed.contains(name)) {
            tracing::info!(schedule = %name, "stopping the schedule no longer listed");
            task.abort();
        }
        for schedule in wanted {
            let name = schedule.name.clone();
            let creds = creds_snapshot(schedule.creds.as_deref());
            match self.0.get(&name) {
                Some(run) if run.schedule == schedule && run.creds == creds && !run.task.is_finished() => continue,
                Some(_) => tracing::info!(
                    schedule = %name,
                    "restarting the schedule: its entry or credentials changed, or it stopped"
                ),
                None => tracing::info!(schedule = %name, "starting the schedule"),
            }
            let task = spawn(schedule.clone());
            if let Some(old) = self.0.insert(name, ScheduleRun { schedule, creds, task }) {
                old.task.abort();
            }
        }
    }
}

impl Drop for Schedules {
    fn drop(&mut self) {
        for run in self.0.values() {
            run.task.abort();
        }
    }
}

/// The machine's links to the bus besides its ants' own: its host connection, or else sense's link of its own and the
/// reading of the shared settings without one.
pub(super) struct Links {
    host: HostRun,
    sense: SenseRun,
    follower: Follower,
    /// The providers of the process's ants, whose sense the links reach their data for.
    providers: Providers,
}

impl Links {
    pub(super) fn new(providers: Providers) -> Self {
        Self { host: HostRun::default(), sense: SenseRun::default(), follower: Follower::default(), providers }
    }

    /// Run the host connection `host`, and sense's link of its own and the reading of the shared settings `served`
    /// lists; with neither, all stop. Sense is told first why it keeps none of its ants' data where `served` says so,
    /// for each of its ants to say so as it starts; with a link of its own, the link tells it ([`SenseRun`]).
    pub(super) fn reconcile(&mut self, host: Option<(Host, Vec<UserId>)>, served: Option<&Served>) {
        if let Some(served) = served {
            match &served.sense {
                Some(SenseLink::Off(why)) => self.providers.sense().unkept(Some(why.clone())),
                Some(SenseLink::Own(_)) => {}
                None => self.providers.sense().unkept(None),
            }
        }
        self.host.reconcile(host);
        self.sense.reconcile(match served.and_then(|s| s.sense.as_ref()) {
            Some(SenseLink::Own(host)) => Some(Host { providers: self.providers.clone(), ..Host::clone(host) }),
            _ => None,
        });
        let machine = served.and_then(|s| s.machine.clone());
        self.follower.reconcile(served.and_then(Served::vias), served.and_then(|s| s.shared.as_ref()), machine);
    }
}

/// The machine's host connection, kept up ([`host::keep`]), if any, in its task.
#[derive(Default)]
pub(super) struct HostRun(Option<HostTask>);

pub(super) struct HostTask {
    host: Host,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    ants: watch::Sender<Vec<UserId>>,
    /// Tells the connection to try again at once, if down.
    again: Arc<Notify>,
    task: JoinHandle<()>,
}

impl HostRun {
    /// Run the host connection `wanted` recording its ants: the one running is told the ants, trying again at once if
    /// down, unless it changed, its credentials file changed or it stopped, when it starts again; with none wanted, it
    /// stops.
    fn reconcile(&mut self, wanted: Option<(Host, Vec<UserId>)>) {
        let Some((wanted, ants)) = wanted else {
            if let Some(old) = self.0.take() {
                tracing::info!("stopping the host connection");
                old.task.abort();
            }
            return;
        };
        let creds = creds_snapshot(wanted.creds.as_deref());
        if let Some(run) = self.0.as_ref().filter(|r| r.host == wanted && r.creds == creds && !r.task.is_finished()) {
            run.ants.send_replace(ants);
            run.again.notify_waiters();
            return;
        }
        let (sender, receiver) = watch::channel(ants);
        let (started, again) = (wanted.clone(), Arc::new(Notify::new()));
        let nudged = again.clone();
        let task = tokio::spawn(
            async move { Box::pin(host::keep(started, receiver, &nudged)).await }
                .instrument(tracing::info_span!("host")),
        );
        if let Some(old) = self.0.replace(HostTask { host: wanted, creds, ants: sender, again, task }) {
            old.task.abort();
        }
    }
}

/// Sense's link of its own without a host connection, kept up ([`host::keep_sense`]), if any, in its task.
#[derive(Default)]
pub(super) struct SenseRun(Option<SenseTask>);

pub(super) struct SenseTask {
    host: Host,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    /// Tells the link to try again at once, if down.
    again: Arc<Notify>,
    task: JoinHandle<()>,
}

impl SenseRun {
    /// Run the link `wanted`: the one running carries on, trying again at once if down, unless it changed, its
    /// credentials file changed or it stopped, when it starts again, no longer saying why sense keeps none of its ants'
    /// data unless it fails again; with none wanted, it stops.
    fn reconcile(&mut self, wanted: Option<Host>) {
        let Some(wanted) = wanted else {
            if let Some(old) = self.0.take() {
                tracing::info!("stopping sense's link");
                old.task.abort();
            }
            return;
        };
        let creds = creds_snapshot(wanted.creds.as_deref());
        // Each reload makes the ants' restarts afresh, which sense's link does not use.
        let same = |r: &SenseTask| r.host == Host { restarts: r.host.restarts.clone(), ..wanted.clone() };
        if let Some(run) = self.0.as_ref().filter(|r| same(r) && r.creds == creds && !r.task.is_finished()) {
            run.again.notify_waiters();
            return;
        }
        wanted.providers.sense().unkept(None);
        let (started, again) = (wanted.clone(), Arc::new(Notify::new()));
        let nudged = again.clone();
        let task = tokio::spawn(
            async move { Box::pin(host::keep_sense(started, &nudged)).await }.instrument(tracing::info_span!("sense")),
        );
        if let Some(old) = self.0.replace(SenseTask { host: wanted, creds, again, task }) {
            old.task.abort();
        }
    }
}

impl Drop for SenseRun {
    fn drop(&mut self) {
        if let Some(run) = &self.0 {
            run.task.abort();
        }
    }
}

/// The reading of the shared settings without a host connection ([`shared::follow`]), if any, in its task.
#[derive(Default)]
pub(super) struct Follower(Option<Following>);

pub(super) struct Following {
    vias: watch::Sender<Vec<Via>>,
    task: JoinHandle<()>,
    /// The machine it names as the writer of the records it creates.
    machine: Option<Machine>,
}

impl Follower {
    /// Read the shared settings into `shared` where `wanted` says, as `machine` ([`shared::follow`]): the reading
    /// running is told unless it stopped or the machine changed, when it starts again; with none wanted, it stops.
    fn reconcile(&mut self, wanted: Option<Vec<Via>>, shared: Option<&Shared>, machine: Option<Machine>) {
        let (Some(wanted), Some(shared)) = (wanted, shared) else {
            if let Some(old) = self.0.take() {
                old.task.abort();
            }
            return;
        };
        if let Some(running) = &self.0
            && !running.task.is_finished()
            && running.machine == machine
        {
            running.vias.send_replace(wanted);
            return;
        }
        let (vias, receiver) = watch::channel(wanted);
        let following = shared::follow(shared.clone(), receiver, machine.clone());
        let task = tokio::spawn(following.instrument(tracing::info_span!("shared")));
        if let Some(old) = self.0.replace(Following { vias, task, machine }) {
            old.task.abort();
        }
    }
}

impl Drop for Follower {
    fn drop(&mut self) {
        if let Some(running) = &self.0 {
            running.task.abort();
        }
    }
}

impl Drop for HostRun {
    fn drop(&mut self) {
        if let Some(run) = &self.0 {
            run.task.abort();
        }
    }
}

/// The contents of the credentials file at `path`, to tell when it changed (`ants renew`): a NATS connection reads it
/// only when it connects.
pub(crate) fn creds_snapshot(path: Option<&Path>) -> Option<Vec<u8>> {
    std::fs::read(path?).ok()
}

/// An ant being served.
#[derive(Debug)]
pub(super) struct Running {
    config: Config,
    /// The contents of its credentials file when it started ([`creds_snapshot`]).
    creds: Option<Vec<u8>>,
    stop: Stop,
    /// Asked to stop, as it is no longer listed or its entry changed.
    pub(super) stopping: bool,
    /// What to serve the ant with once it stopped.
    pub(super) next: Option<Config>,
}

impl Running {
    /// Note `config` as served, returning it with the stop to serve it until.
    pub(super) fn insert(running: &mut HashMap<UserId, Running>, config: Config) -> (Config, Stop) {
        let stop = Stop::default();
        let creds = creds_snapshot(config.connection.creds.as_deref());
        let entry = Running { config: config.clone(), creds, stop: stop.clone(), stopping: false, next: None };
        running.insert(config.ant.clone(), entry);
        (config, stop)
    }

    pub(super) fn ask_to_stop(&mut self) {
        if !self.stopping {
            self.stop.request();
            self.stopping = true;
        }
    }
}

/// Bring the ants `running` in line with those `wanted`, returning the ants to start: those newly listed. An ant no
/// longer listed is asked to stop, finishing its prompts in flight; one whose config or credentials file changed is
/// asked to stop and is served again with its new config once it stopped; the others carry on untouched.
pub(super) fn reconcile(running: &mut HashMap<UserId, Running>, wanted: Vec<Config>) -> Vec<(Config, Stop)> {
    let listed: HashSet<UserId> = wanted.iter().map(|c| c.ant.clone()).collect();
    for (ant, r) in running.iter_mut().filter(|(ant, _)| !listed.contains(*ant)) {
        if !r.stopping || r.next.is_some() {
            tracing::info!(%ant, "stopping the ant no longer listed");
        }
        r.next = None;
        r.ask_to_stop();
    }
    let mut start = Vec::new();
    for config in wanted {
        let ant = config.ant.clone();
        let Some(r) = running.get_mut(&ant) else {
            tracing::info!(%ant, "serving the ant newly listed");
            start.push(Running::insert(running, config));
            continue;
        };
        config.pause.carry(&r.config.pause);
        if r.stopping {
            r.next = Some(config);
        } else if r.config != config {
            tracing::info!(%ant, "restarting the ant: its entry changed");
            r.next = Some(config);
            r.ask_to_stop();
        } else if r.creds != creds_snapshot(config.connection.creds.as_deref()) {
            tracing::info!(%ant, "restarting the ant: its credentials changed");
            r.next = Some(config);
            r.ask_to_stop();
        } else {
            // Pool members, the pause and the drift are left out of the comparison: the running ant takes them as it
            // is.
            if let (Some(serving), Some(listed)) = (&r.config.spending.pool, &config.spending.pool) {
                serving.members.replace(&listed.members);
            }
            r.config.pause.replace(&config.pause);
            r.config.drift.replace(&config.drift);
        }
    }
    start
}
