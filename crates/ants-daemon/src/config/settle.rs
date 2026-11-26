//! The checks of a parsed config file's top-level settings, and its `[[ant]]` and `[[schedule]]` entries settled
//! ([`super::parse`]): each given the top-level settings it sets none of, its paths resolved.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};

use super::ant::fallbacks;
use super::{Ant, File, resolve};
use crate::harness::{self, Harnesses, ModelSettings};
use crate::provider::{Definition, Setting};
use crate::schedule::{self, Schedule};

/// Check the top-level settings of the config `file` at `path`: its `harness` among the `harnesses`, its `provider`
/// among the `providers`, its `[cli]`, `[storage]`, `[memory_pressure]`, `[trails]` and `machine`.
pub(super) fn check_top_level(
    file: &File,
    harnesses: &Harnesses,
    providers: &BTreeMap<String, Arc<Definition>>,
    path: &Path,
) -> Result<()> {
    if let Some(harness) = &file.harness {
        harnesses.get(harness).with_context(|| format!("config {}", path.display()))?;
    }
    if let Some(provider) = &file.provider {
        Setting::named(provider, providers).with_context(|| format!("config {}", path.display()))?;
    }
    if let Some(url) = &file.cli.nats {
        crate::bus::check_url(url).with_context(|| format!("config {}: [cli]", path.display()))?;
    }
    file.cli.summary().with_context(|| format!("config {}", path.display()))?;
    file.storage.validate().with_context(|| format!("config {}", path.display()))?;
    file.memory_pressure.validate().with_context(|| format!("config {}", path.display()))?;
    file.trails.validate().with_context(|| format!("config {}", path.display()))?;
    if file.turn_budget == Some(0) {
        bail!("config {}: turn_budget must be at least 1", path.display());
    }
    if let Some(machine) = &file.machine {
        check_machine(machine).with_context(|| format!("config {}", path.display()))?;
    }
    Ok(())
}

/// Check `ant` of the config file at `path`, give it the top-level settings of the `file` it sets none of, its
/// harness and provider among those defined, and resolve its paths against `base` and `home` unless it is built in.
pub(super) fn settle_ant(
    mut ant: Ant,
    file: &File,
    (harnesses, providers): (&Harnesses, &BTreeMap<String, Arc<Definition>>),
    base: &Path,
    home: &Path,
    path: &Path,
) -> Result<Ant> {
    harness::validate_env(&ant.env)?;
    ant.validate().with_context(|| format!("config {}", path.display()))?;
    if ant.harness.is_none() {
        ant.harness.clone_from(&file.harness);
    }
    // A built-in ant without one needs none unless served here, which `builtins::leave` settles.
    if ant.builtin.is_none() || ant.harness.is_some() {
        check_harnesses(&ant, harnesses).with_context(|| format!("config {}: ant {}", path.display(), ant.name))?;
    }
    if ant.provider.is_none() {
        ant.provider.clone_from(&file.provider);
    }
    if let Some(provider) = &ant.provider {
        ant.provider_setting = Setting::named(provider, providers)
            .with_context(|| format!("config {}: ant {}", path.display(), ant.name))?;
    }
    if ant.builtin.is_none() {
        let abs = |dir: &Path| resolve(dir, base, home);
        ant.workdir = abs(&ant.workdir).canonicalize().with_context(|| {
            format!("config {}: ant {}: workdir {}", path.display(), ant.name, ant.workdir.display())
        })?;
        ant.home = ant.home.as_deref().map(abs);
        ant.creds = ant.creds.as_deref().map(abs);
    }
    if ant.nats.is_none() {
        ant.nats.clone_from(&file.nats);
    }
    if let Some(url) = &ant.nats {
        crate::bus::check_url(url).with_context(|| format!("config {}: ant {}", path.display(), ant.name))?;
    }
    if ant.js_domain.is_none() {
        ant.js_domain.clone_from(&file.js_domain);
    }
    if ant.human.is_none() {
        ant.human.clone_from(&file.human);
    }
    if ant.turn_budget.is_none() {
        ant.turn_budget = file.turn_budget;
    }
    ant.redact = file.redact;
    ant.machine.clone_from(&file.machine);
    ant.storage = file.storage;
    ant.memory_pressure = file.memory_pressure;
    ant.trail_workers = file.trails.workers;
    ant.remote_admin = file.admin.remote;
    Ok(ant)
}

/// The schedules of the config file at `path` from their `entries`, each named once and after none of the ants'
/// `names`, with the top-level `timezone`, and the `nats` and `js_domain` of the `file`, where they set none.
pub(super) fn schedules(
    entries: Vec<schedule::Entry>,
    file: &File,
    timezone: Option<&jiff::tz::TimeZone>,
    names: &BTreeSet<UserId>,
    base: &Path,
    home: &Path,
    path: &Path,
) -> Result<Vec<Schedule>> {
    let mut schedules: Vec<Schedule> = Vec::with_capacity(entries.len());
    for entry in entries {
        let name = entry.name.clone();
        let mut schedule = Schedule::from_entry(entry, timezone, |p| resolve(p, base, home))
            .with_context(|| format!("config {}: schedule {name}", path.display()))?;
        let name = &schedule.name;
        if schedules.iter().any(|s| s.name == *name) {
            bail!("config {}: schedule {name} is listed twice", path.display());
        }
        if names.contains(name) {
            bail!(
                "config {}: schedule {name} has the name of an ant; a schedule sends as a user of its own",
                path.display()
            );
        }
        if schedule.nats.is_none() {
            schedule.nats.clone_from(&file.nats);
        }
        if schedule.js_domain.is_none() {
            schedule.js_domain.clone_from(&file.js_domain);
        }
        schedules.push(schedule);
    }
    Ok(schedules)
}

/// The most characters of the top-level `machine`.
const MAX_MACHINE: usize = 64;

/// Check the top-level `machine`: not blank, at most [`MAX_MACHINE`] characters, none of them a control character.
fn check_machine(machine: &str) -> Result<()> {
    if machine.trim().is_empty() {
        bail!("machine is blank");
    }
    if machine.chars().count() > MAX_MACHINE {
        bail!("machine {machine:?} is longer than {MAX_MACHINE} characters");
    }
    if machine.chars().any(char::is_control) {
        bail!("machine {machine:?} holds a control character");
    }
    Ok(())
}

/// Check that `ant` names a harness, and that it and those of its fallbacks are defined and approve tools as its
/// `tools` says.
fn check_harnesses(ant: &Ant, harnesses: &Harnesses) -> Result<()> {
    let own = harnesses.named(ant.harness.as_deref())?;
    own.with_tools(ant.tools)?;
    let settings = ModelSettings { model: ant.model.clone(), ..ModelSettings::default() };
    fallbacks(ant.template.is_some(), &ant.fallback, &own, &settings, harnesses, ant.tools).map(drop)
}
