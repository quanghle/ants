//! The active daemon owns sense's durable schedule. A firing is stored before its wake, and confirmed only after
//! delivery; a replacement daemon therefore resumes pending work with the same wake id. Nothing fires while the ant's
//! sense identity is dormant ([`ants_sense::identity::Card::active`]) or the ant is paused ([`crate::pause`]); its
//! revival changes the card and its resuming the pause, either waking the scheduler, so each trigger due meanwhile
//! fires once. As it first reads the ant's items, it imports the ant's harness memories into them unless imported
//! before ([`super::imports`]). It also runs sense's daily memory maintenance and begins the ant's dreams
//! ([`dreams`]), whose last ones are kept in sense data, so a replacement daemon runs them no sooner either. Each read
//! of the ant's items also publishes the profile of its confirmed persona ([`super::published::profile`]), dormant or
//! not.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::provider::{Host, RpcError, WakeRequest};
use ants_proto::{UserId, WatchWake};
use ants_sense::Sense;
use ants_sense::memory;
use ants_sense::remind::{self, ConditionCheck, Firing, WatchCheck};
use ants_sense::store::{Items, StoreError};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

mod conditions;
mod dreams;
mod watchers;

pub(crate) use dreams::{Daemon, Dreaming, review_answer, review_given, review_received};
pub(crate) use watchers::TrailChecks;

const RETRY: Duration = Duration::from_secs(60);

struct Settings {
    ant: UserId,
    workdir: PathBuf,
    machine: String,
    env: BTreeMap<String, String>,
    allow: Vec<String>,
    /// Whether the ant's harness runs its tools unasked: then so do its triggers' commands, but for a trigger made on
    /// an untrusted thread, its allow rules deciding otherwise.
    unasked: bool,
    /// None in tests that leave dreams out.
    dreams: Option<dreams::Dreams>,
    /// What the ant's colony entry shows, its profile among it.
    workload: Arc<crate::colony::Workload>,
    /// What runs trail watchers; none where the bus keeps no runs.
    trails: Option<Arc<dyn TrailChecks>>,
    /// Hides secrets in a check's output before a wake carries it.
    redactor: crate::secrets::redact::Redactor,
    /// The ant's home, where the watchers' status is kept for `ants doctor`; none in tests leaving it out.
    home: Option<PathBuf>,
    /// The output of each watcher firing prepared and not delivered yet, by firing id, redacted and cut.
    outputs: std::sync::Mutex<HashMap<String, watchers::Output>>,
    /// Whether the ant is paused: then nothing fires, as for a dormant ant.
    pause: crate::pause::Pause,
    /// Where the ant's harness memories are read from, imported once as the schedule first reads its items; none in
    /// tests leaving them out.
    harness_memories: Option<super::imports::HarnessMemories>,
}

/// Whether the commands of an ant's triggers run, as its scheduler decides it: unasked on a harness running its tools
/// so, else as the ant's tool allow rules permit.
#[derive(Debug, Clone)]
pub(crate) struct TriggerCommands {
    allow: Vec<String>,
    unasked: bool,
}

impl TriggerCommands {
    /// As the ant `config` serves runs them.
    pub(crate) fn of(config: &crate::daemon::Config) -> Self {
        Self { allow: config.policy.allow.clone(), unasked: config.harness.harness.bypasses_permissions() }
    }

    /// Why a call of sense's tool `tool` with `arguments` is refused: one creating a command trigger or watcher whose
    /// command the scheduler would refuse to run, naming the rule that lets it. A trigger from an untrusted thread
    /// sense refuses itself.
    pub(crate) fn refusal(&self, tool: &str, arguments: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
        let action = arguments.get("action").and_then(serde_json::Value::as_str);
        if self.unasked || tool != "remind" || action.is_some_and(|action| action != "create") {
            return None;
        }
        let trigger: remind::Trigger = serde_json::from_value(arguments.get("trigger")?.clone()).ok()?;
        let command = match (trigger.condition(), trigger.watch()) {
            (Some(remind::Condition::Command { command }), _) => command,
            (_, Some(remind::Watch { run: remind::Watched::Command { command }, .. })) => command,
            _ => return None,
        };
        (!conditions::allowed(command, &self.allow)).then(|| {
            format!(
                "the command {command:?} is outside the ant's tool allow rules, which decide what its triggers run as \
                 its harness asks about its tools, so each check would be refused: a rule such as \
                 \"execute:{command}\" in the ant's allow lets it run"
            )
        })
    }
}

/// What a watcher's check takes beside the ant's config.
pub(crate) struct Watching {
    pub trails: Option<Arc<dyn TrailChecks>>,
    pub redactor: crate::secrets::redact::Redactor,
}

/// Dropping the active daemon's guard cancels its scheduler, including an in-flight probe.
pub(crate) struct Serving(Mutex<Option<JoinHandle<()>>>);

impl Serving {
    pub(crate) async fn stop(&self) {
        let mut running = self.0.lock().await;
        if let Some(task) = running.as_mut() {
            task.abort();
            let _ = task.await;
        }
        running.take();
    }
}

impl Drop for Serving {
    fn drop(&mut self) {
        if let Some(task) = self.0.get_mut().take() {
            task.abort();
        }
    }
}

/// Start only after the ant holds its colony registration and its wake service is installed.
pub(super) fn serve(
    sense: Arc<Sense>,
    wakers: crate::provider::wake::Wakers,
    config: &crate::daemon::Config,
    machine: &str,
    dreaming: Arc<dyn Dreaming>,
    workload: Arc<crate::colony::Workload>,
    watching: Watching,
) -> Serving {
    let host = crate::provider::wake::Ants::new(wakers);
    host.named(ants_sense::NAME);
    let settings = Settings {
        ant: config.ant.clone(),
        workdir: config.harness.workdir.clone(),
        machine: machine.to_owned(),
        env: config.harness.env.clone(),
        allow: config.policy.allow.clone(),
        unasked: config.harness.harness.bypasses_permissions(),
        dreams: Some(dreams::Dreams::new(dreaming)),
        workload,
        trails: watching.trails,
        redactor: watching.redactor,
        home: Some(config.storage.home.clone()),
        outputs: std::sync::Mutex::default(),
        pause: config.pause.clone(),
        harness_memories: super::imports::HarnessMemories::of(config),
    };
    start(sense, Arc::new(host), settings)
}

fn start(sense: Arc<Sense>, host: Arc<dyn Host>, settings: Settings) -> Serving {
    Serving(Mutex::new(Some(tokio::spawn(async move {
        // Subscribe first: a call changing the ant's items during a schedule read must wake the following sleep.
        let mut changes = sense.changes(&settings.ant);
        let mut paused = settings.pause.subscribe();
        let mut imported = false;
        loop {
            paused.mark_unchanged();
            let result = async {
                let items = sense.synced(&settings.ant).await?;
                if let Some(from) = settings.harness_memories.as_ref().filter(|_| !imported) {
                    super::imports::import(&*items, from, &settings.ant, &settings.machine).await?;
                    imported = true;
                }
                super::published::profile(&*items, &settings.ant, &settings.workload).await;
                cycle(&sense, &*items, &*host, &settings, Utc::now()).await
            }
            .await;
            match result {
                Ok(next) => {
                    if !wait(&mut changes, &mut paused, next).await {
                        return;
                    }
                }
                Err(error)
                    if error.downcast_ref::<StoreError>().is_some_and(|e| matches!(e, StoreError::NotKept(_))) =>
                {
                    // A fixture without a host keeps no data. It needs no retries or warnings.
                    if !wait(&mut changes, &mut paused, None).await {
                        return;
                    }
                }
                Err(error) => {
                    tracing::warn!(ant = %settings.ant, error = format!("{error:#}"), "retrying sense's schedule in a minute");
                    // Changes cannot turn a delivery error or wake throttle into a busy retry loop.
                    tokio::time::sleep(RETRY).await;
                }
            }
        }
    }))))
}

async fn cycle(
    sense: &Sense,
    items: &dyn Items,
    host: &dyn Host,
    settings: &Settings,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>> {
    if ants_sense::identity::card(items).await?.is_some_and(|card| !card.active) {
        // A dormant ant's reminders wait for its revival, which changes the card.
        return Ok(None);
    }
    if settings.pause.get().is_some() {
        // A paused ant's triggers wait for its resuming, which changes the pause.
        return Ok(None);
    }
    let started = tokio::time::Instant::now();
    // A failed wake ends the firings but holds back neither maintenance nor dreams, the cycle ending in its error.
    let mut undelivered = None;
    for firing in remind::pending_work(items).await? {
        if let Err(error) = deliver(items, host, settings, &firing).await {
            undelivered = Some(error);
            break;
        }
    }
    let due = if undelivered.is_none() { remind::due(items, now).await? } else { Vec::new() };
    let mut watched = false;
    for due in due {
        let at = since(now, started);
        let firing = if due.watch.is_some() {
            watched = true;
            match remind::reserve_watch(items, &due.id, at).await? {
                Some(check) => watch(sense, items, settings, &check).await?,
                None => None,
            }
        } else if due.condition.is_some() {
            match remind::reserve_condition(items, &due.id, at).await? {
                Some(check) => probe(sense, items, settings, &check).await?,
                None => None,
            }
        } else {
            remind::prepare_firing(items, &due.id, at, "reminder time reached").await?
        };
        if let Some(firing) = firing
            && let Err(error) = deliver(items, host, settings, &firing).await
        {
            undelivered = Some(error);
            break;
        }
    }
    let now = since(now, started);
    if watched && let Some(home) = &settings.home {
        let status = remind::watcher_status(items, now).await?;
        if let Err(error) = remind::watch::write_status(home, &status) {
            tracing::warn!(ant = %settings.ant, error = %error, "the watchers' status for `ants doctor` is not kept");
        }
    }
    if let Some(run) = memory::maintain(items, Some(&settings.workdir), now).await? {
        let changes = run.changes.len();
        tracing::info!(ant = %settings.ant, changes, clusters = run.duplicates.len(), "sense maintenance ran");
    }
    let dream = match &settings.dreams {
        Some(dreams) => Some(dreams::step(items, host, settings, dreams, now).await.unwrap_or_else(|error| {
            tracing::warn!(ant = %settings.ant, error = format!("{error:#}"), "retrying the dream in a minute");
            now
        })),
        None => None,
    };
    if let Some(error) = undelivered {
        return Err(error);
    }
    let next = [memory::next_maintenance(items).await?, remind::next_due(items, now).await?, dream];
    let next = next.into_iter().flatten().min();
    // A concurrent schedule update or a stale reservation must not turn a due record into a busy loop.
    Ok(next
        .map(|next| if next <= now { now + chrono::Duration::from_std(RETRY).expect("a bounded delay") } else { next }))
}

async fn probe(
    sense: &Sense,
    items: &dyn Items,
    settings: &Settings,
    check: &ConditionCheck,
) -> Result<Option<Firing>> {
    if items.get(&check.id).await?.is_none_or(|item| item.revision != check.revision) {
        return Ok(None);
    }
    let workdir =
        workdir(sense, settings, check.source.clone(), check.workdir.as_ref(), check.workdir_machine.as_ref());
    let context = conditions::Context {
        workdir: &workdir,
        env: &settings.env,
        allow: &settings.allow,
        unasked: unasked(settings, check.source.as_ref()),
        items,
        now: check.at,
    };
    let started = tokio::time::Instant::now();
    let result = conditions::evaluate(&check.condition, check.observation.as_ref(), &context).await;
    let now = since(check.at, started);
    let (matched, reason, observation) = match result {
        Ok(observed) => (observed.fired, observed.reason, Some(observed.value)),
        Err(error) => {
            tracing::warn!(ant = %settings.ant, reminder = %check.id, error = format!("{error:#}"), "sense condition check failed");
            (false, "condition check failed", None)
        }
    };
    Ok(remind::complete_condition(items, check, now, matched, reason, observation).await?)
}

/// Run a reserved watcher check and apply what it found, keeping the output of a firing for its wake.
async fn watch(sense: &Sense, items: &dyn Items, settings: &Settings, check: &WatchCheck) -> Result<Option<Firing>> {
    if items.get(&check.id).await?.is_none_or(|item| item.revision != check.revision) {
        return Ok(None);
    }
    let workdir =
        workdir(sense, settings, check.source.clone(), check.workdir.as_ref(), check.workdir_machine.as_ref());
    let context = conditions::Context {
        workdir: &workdir,
        env: &settings.env,
        allow: &settings.allow,
        unasked: unasked(settings, check.source.as_ref()),
        items,
        now: check.at,
    };
    let reads = watchers::Reads { context, thread: check.source.as_ref().map(|source| source.thread.clone()) };
    let started = tokio::time::Instant::now();
    let seen = watchers::run(check, &reads, settings.trails.as_deref()).await;
    if let remind::Checked::Failed(error) = &seen.checked {
        tracing::warn!(ant = %settings.ant, watcher = %check.id, error, "a watcher's check failed");
    }
    let now = since(check.at, started);
    let firing = remind::complete_watch(items, check, now, seen.checked).await?;
    if let (Some(firing), Some(output)) = (&firing, seen.output) {
        let output = watchers::Output::of(&settings.redactor.redact(&output));
        settings.outputs.lock().expect("unpoisoned").insert(firing.id.clone(), output);
    }
    Ok(firing)
}

/// The workdir a check of the reminder from `source` runs in: the source thread's, else the one it was set in on this
/// machine, else the ant's.
fn workdir(
    sense: &Sense,
    settings: &Settings,
    source: Option<ants_proto::provider::CallContext>,
    workdir: Option<&PathBuf>,
    machine: Option<&String>,
) -> PathBuf {
    let source = source.map(|mut context| {
        context.ant = settings.ant.clone();
        context.machine.clone_from(&settings.machine);
        context
    });
    source
        .as_ref()
        .and_then(|context| sense.workdir(context))
        .or_else(|| {
            workdir
                .filter(|workdir| {
                    workdir.is_absolute() && machine.map(String::as_str) == Some(settings.machine.as_str())
                })
                .cloned()
        })
        .unwrap_or_else(|| settings.workdir.clone())
}

/// Whether the commands of a trigger made in `source` run unasked: when the ant's harness runs its tools so and the
/// trigger comes from a trusted thread, none narrowed to tool rules.
fn unasked(settings: &Settings, source: Option<&ants_proto::provider::CallContext>) -> bool {
    settings.unasked && source.is_some_and(ants_proto::provider::CallContext::trusted)
}

/// `at`, moved on by the time elapsed since `started`.
fn since(at: DateTime<Utc>, started: tokio::time::Instant) -> DateTime<Utc> {
    at + chrono::Duration::from_std(started.elapsed()).expect("a bounded check")
}

async fn deliver(items: &dyn Items, host: &dyn Host, settings: &Settings, firing: &Firing) -> Result<()> {
    let mut text = format!("{}\n\n{}", firing.text, firing.reason);
    let mut watcher = None;
    if firing.watched() {
        let output = settings.outputs.lock().expect("unpoisoned").get(&firing.id).cloned();
        text.push_str(
            output.as_ref().map_or_else(|| watchers::OUTPUT_LOST.to_owned(), watchers::Output::wake_text).as_str(),
        );
        watcher = Some(WatchWake {
            id: firing.reminder.to_string(),
            title: firing.text.clone(),
            done: firing.last,
            cut: output.as_ref().is_some_and(watchers::Output::cut),
            output: output.map(|output| output.shown),
            replaces: None,
        });
    }
    let mut request = WakeRequest {
        ant: settings.ant.clone(),
        thread: firing.thread.clone(),
        text,
        id: firing.id.clone(),
        replaces: firing.replaces.clone(),
        watcher,
    };
    match host.wake(request.clone()).await {
        Err(error) if request.thread.is_some() && error.code == RpcError::UNKNOWN_THREAD => {
            // A source session may be absent after a restart. Keep the durable firing identity on the new thread.
            request.thread = None;
            host.wake(request).await?;
        }
        result => {
            result?;
        }
    }
    remind::confirm_delivered(items, &firing.reminder, &firing.id, Utc::now()).await?;
    settings.outputs.lock().expect("unpoisoned").remove(&firing.id);
    Ok(())
}

/// Until `next`, a change of the ant's items or a pause or resume; false once the items' changes end.
async fn wait(
    changes: &mut watch::Receiver<()>,
    paused: &mut watch::Receiver<Option<ants_proto::Paused>>,
    next: Option<DateTime<Utc>>,
) -> bool {
    let delay = next.map(|next| (next - Utc::now()).to_std().unwrap_or_default());
    tokio::select! {
        () = async { tokio::time::sleep(delay.unwrap_or_default()).await }, if delay.is_some() => true,
        changed = changes.changed() => changed.is_ok(),
        Ok(()) = paused.changed() => true,
    }
}

#[cfg(test)]
mod tests;
