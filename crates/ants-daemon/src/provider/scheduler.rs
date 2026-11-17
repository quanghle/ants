//! The active daemon owns sense's durable schedule. A firing is stored before its wake, and confirmed only after
//! delivery; a replacement daemon therefore resumes pending work with the same wake id. Nothing fires while the ant's
//! sense identity is dormant ([`ants_sense::identity::Card::active`]); its revival changes the card, which wakes the
//! scheduler. It also runs sense's daily memory maintenance and begins the ant's dreams ([`dreams`]), whose last ones
//! are kept in sense data, so a replacement daemon runs them no sooner either.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::UserId;
use ants_proto::provider::{CallContext, Host, RpcError, WakeRequest};
use ants_sense::Sense;
use ants_sense::memory;
use ants_sense::remind::{self, ConditionCheck, Firing};
use ants_sense::store::{Items, StoreError};
use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

mod conditions;
mod dreams;

pub(crate) use dreams::{Daemon, Dreaming};

const RETRY: Duration = Duration::from_secs(60);

struct Settings {
    ant: UserId,
    workdir: PathBuf,
    machine: String,
    env: BTreeMap<String, String>,
    allow: Vec<String>,
    /// None in tests that leave dreams out.
    dreams: Option<dreams::Dreams>,
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
pub(crate) fn serve(config: &crate::daemon::Config, machine: &str, dreaming: Arc<dyn Dreaming>) -> Option<Serving> {
    if !matches!(config.provider, super::Setting::Sense) {
        return None;
    }
    let host = super::wake::Ants::default();
    host.named(ants_sense::NAME);
    let settings = Settings {
        ant: config.ant.clone(),
        workdir: config.harness.workdir.clone(),
        machine: machine.to_owned(),
        env: config.harness.env.clone(),
        allow: config.policy.allow.clone(),
        dreams: Some(dreams::Dreams::new(dreaming)),
    };
    Some(start(super::built_in().clone(), Arc::new(host), settings))
}

fn start(sense: Arc<Sense>, host: Arc<dyn Host>, settings: Settings) -> Serving {
    Serving(Mutex::new(Some(tokio::spawn(async move {
        // Subscribe first: a successful tool call during a schedule read must wake the following sleep.
        let mut changes = sense.changes();
        loop {
            let result = async {
                let items = sense.items(&settings.ant).await?;
                cycle(&sense, &*items, &*host, &settings, Utc::now()).await
            }
            .await;
            match result {
                Ok(next) => {
                    if !wait(&mut changes, next).await {
                        return;
                    }
                }
                Err(error)
                    if error.downcast_ref::<StoreError>().is_some_and(|e| matches!(e, StoreError::NotKept(_))) =>
                {
                    // A fixture without a host keeps no data. It needs no retries or warnings.
                    if !wait(&mut changes, None).await {
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
    let started = tokio::time::Instant::now();
    for firing in remind::pending_work(items).await? {
        deliver(items, host, &settings.ant, &firing).await?;
    }
    for due in remind::due(items, now).await? {
        let at = now + chrono::Duration::from_std(started.elapsed()).expect("a bounded check");
        let firing = if due.condition.is_some() {
            match remind::reserve_condition(items, &due.id, at).await? {
                Some(check) => probe(sense, items, settings, &check).await?,
                None => None,
            }
        } else {
            remind::prepare_firing(items, &due.id, at, "reminder time reached").await?
        };
        if let Some(firing) = firing {
            deliver(items, host, &settings.ant, &firing).await?;
        }
    }
    let now = now + chrono::Duration::from_std(started.elapsed()).expect("a bounded check");
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
    let Some(item) = items.get(&check.id).await?.filter(|item| item.revision == check.revision) else {
        return Ok(None);
    };
    let source = item
        .values
        .get("source_context")
        .or_else(|| item.values.get(ants_sense::memory::WRITER))
        .and_then(|value| serde_json::from_value::<CallContext>(value.clone()).ok())
        .and_then(|mut context| {
            context.ant = settings.ant.clone();
            context.thread = item.text("thread").parse().ok()?;
            context.machine.clone_from(&settings.machine);
            Some(context)
        });
    let workdir = source
        .as_ref()
        .and_then(|context| sense.workdir(context))
        .or_else(|| {
            check
                .workdir
                .as_ref()
                .filter(|workdir| workdir.is_absolute() && item.text("workdir_machine") == settings.machine)
                .cloned()
        })
        .unwrap_or_else(|| settings.workdir.clone());
    let context =
        conditions::Context { workdir: &workdir, env: &settings.env, allow: &settings.allow, items, now: check.at };
    let started = tokio::time::Instant::now();
    let result = conditions::evaluate(&check.condition, check.observation.as_ref(), &context).await;
    let now = check.at + chrono::Duration::from_std(started.elapsed()).expect("a bounded check");
    let (matched, reason, observation) = match result {
        Ok(observed) => (observed.fired, observed.reason, Some(observed.value)),
        Err(error) => {
            tracing::warn!(ant = %settings.ant, reminder = %check.id, error = format!("{error:#}"), "sense condition check failed");
            (false, "condition check failed", None)
        }
    };
    Ok(remind::complete_condition(items, check, now, matched, reason, observation).await?)
}

async fn deliver(items: &dyn Items, host: &dyn Host, ant: &UserId, firing: &Firing) -> Result<()> {
    let mut request = WakeRequest {
        ant: ant.clone(),
        thread: firing.thread.clone(),
        text: format!("{}\n\n{}", firing.text, firing.reason),
        id: firing.id.clone(),
    };
    match host.wake(request.clone()).await {
        Err(error) if request.thread.is_some() && unknown_thread(&error) => {
            // A source session may be absent after a restart. Keep the durable firing identity on the new thread.
            request.thread = None;
            host.wake(request).await?;
        }
        result => {
            result?;
        }
    }
    remind::confirm_delivered(items, &firing.reminder, &firing.id, Utc::now()).await?;
    Ok(())
}

fn unknown_thread(error: &RpcError) -> bool {
    error.code == RpcError::INVALID_PARAMS && error.message.contains(" is no thread of ant ")
}

async fn wait(changes: &mut watch::Receiver<()>, next: Option<DateTime<Utc>>) -> bool {
    match next {
        Some(next) => {
            let delay = (next - Utc::now()).to_std().unwrap_or_default();
            tokio::select! {
                _ = tokio::time::sleep(delay) => true,
                changed = changes.changed() => changed.is_ok(),
            }
        }
        None => changes.changed().await.is_ok(),
    }
}

#[cfg(test)]
mod tests;
