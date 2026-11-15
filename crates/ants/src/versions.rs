//! The versions of ants that running components publish: each daemon in its roster entry, `ants webhook serve` and
//! each schedule in their webhook's marker, `ants channel serve` under its own key of [`WEBHOOKS_BUCKET`]
//! ([`Serving`]). `ants roster` flags mixed versions and `ants doctor` tells whether every component runs a version
//! that allows renewing credentials to the permissions of this one. Both also flag a harness agent that live ants run
//! at different versions on different machines ([`harness_drift`]).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ants_daemon::direct_kv::DirectKv;
use ants_daemon::roster;
use ants_proto::{
    PERMISSIONS_VERSION, PermissionsVersion, ROSTER_BUCKET, RosterEntry, SERVING_EVERY, Serving, WEBHOOKS_BUCKET,
    channel_manager_serving_subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, stream::DirectGetErrorKind};
use chrono::{DateTime, Utc};
use futures::TryStreamExt as _;

/// A [`Serving`] put longer ago than this is of a process that stopped.
pub const SERVING_STALE_AFTER: Duration = SERVING_EVERY.saturating_mul(2);

/// A running component and the version it publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Component {
    /// What it is, in words: `the daemon of dev-a on m1`, `webhook github on m2`, `the channel manager on m1`.
    pub what: String,
    pub version: String,
    pub perms: PermissionsVersion,
}

impl Component {
    pub fn of_entry(entry: &RosterEntry) -> Self {
        Self {
            what: format!("the daemon of {} on {}", entry.ant, entry.machine),
            version: entry.version.clone(),
            perms: entry.perms,
        }
    }

    fn of_serving(what: &str, serving: Serving) -> Self {
        Self { what: format!("{what} on {}", serving.machine), version: serving.version, perms: serving.perms }
    }

    /// It, with its version: `the daemon of dev-a on m1 (ants 0.2.0, permissions 2)`.
    fn described(&self) -> String {
        format!("{} (ants {}, permissions {})", self.what, self.version, self.perms)
    }
}

/// What every running component publishes, and what could not be read.
#[derive(Debug, Default)]
pub struct Running {
    pub components: Vec<Component>,
    /// Why some components could not be looked at, one line each.
    pub unseen: Vec<String>,
}

/// The ants versions in `components`, with how many run each: `0.2.0 (3), 0.1.0 (1)`, newest first; `None` unless
/// they run more than one.
pub fn mixed(components: &[Component]) -> Option<String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for component in components {
        *counts.entry(component.version.as_str()).or_default() += 1;
    }
    if counts.len() < 2 {
        return None;
    }
    let mut versions: Vec<(&str, usize)> = counts.into_iter().collect();
    versions.sort_by(|(a, _), (b, _)| newest_first(a, b));
    let named: Vec<String> = versions.into_iter().map(|(version, n)| format!("{version} ({n})")).collect();
    Some(named.join(", "))
}

/// Versions compared by their numeric parts, newest first.
fn newest_first(a: &str, b: &str) -> std::cmp::Ordering {
    let parts = |v: &str| -> Vec<u64> {
        v.split(|c: char| !c.is_ascii_digit()).filter(|p| !p.is_empty()).filter_map(|p| p.parse().ok()).collect()
    };
    parts(b).cmp(&parts(a)).then_with(|| b.cmp(a))
}

/// The line `ants roster` adds under its table when the live daemons listed run more than one version at `now`.
pub fn roster_note(entries: &[RosterEntry], now: DateTime<Utc>) -> Option<String> {
    let live = entries.iter().filter(|e| roster::is_live(e, now));
    let components: Vec<Component> = live.map(Component::of_entry).collect();
    let versions = mixed(&components)?;
    Some(format!("mixed ants versions: {versions}; `ants doctor` tells whether credentials may be renewed"))
}

/// One line per harness agent the live ants of `entries` run at more than one version, across more than one machine,
/// as their roster entries name it ([`RosterEntry::agent`]): `copilot at 1.0.3 (dev-a on m1), 1.0.2 (dev-b on m2)`,
/// newest first. Different versions on one machine only are a harness upgraded there while an older one still runs.
pub fn harness_drift(entries: &[RosterEntry], now: DateTime<Utc>) -> Vec<String> {
    let mut agents: BTreeMap<&str, BTreeMap<&str, Vec<&RosterEntry>>> = BTreeMap::new();
    for entry in entries.iter().filter(|e| roster::is_live(e, now)) {
        if let Some(agent) = &entry.agent {
            agents.entry(&agent.name).or_default().entry(&agent.version).or_default().push(entry);
        }
    }
    agents
        .into_iter()
        .filter(|(_, versions)| {
            let machines: BTreeSet<&str> = versions.values().flatten().map(|e| e.machine.as_str()).collect();
            versions.len() > 1 && machines.len() > 1
        })
        .map(|(name, versions)| {
            let mut versions: Vec<_> = versions.into_iter().collect();
            versions.sort_by(|(a, _), (b, _)| newest_first(a, b));
            let described: Vec<String> = versions
                .into_iter()
                .map(|(version, ants)| {
                    let ants: Vec<String> = ants.iter().map(|e| format!("{} on {}", e.ant, e.machine)).collect();
                    format!("{version} ({})", ants.join(", "))
                })
                .collect();
            format!("{name} at {}", described.join(", "))
        })
        .collect()
}

/// The line `ants roster` adds under its table when live ants run a harness agent at different versions on different
/// machines at `now` ([`harness_drift`]).
pub fn harness_note(entries: &[RosterEntry], now: DateTime<Utc>) -> Option<String> {
    let drift = harness_drift(entries, now);
    (!drift.is_empty()).then(|| format!("harness versions differ across machines: {}", drift.join("; ")))
}

/// Whether credentials renewed with this ants suit every running component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Renewal {
    /// Every component runs this permissions version or a newer one.
    Allowed,
    /// Some run an older permissions version: renewed credentials may lack rights they use.
    Behind(Vec<String>),
    /// Some run a newer one than this ants: credentials it renews may lack rights they use.
    Ahead(Vec<String>),
}

/// How `components` stand with renewing credentials to [`PERMISSIONS_VERSION`].
pub fn renewal(components: &[Component]) -> Renewal {
    let described = |keep: &dyn Fn(PermissionsVersion) -> bool| -> Vec<String> {
        components.iter().filter(|c| keep(c.perms)).map(Component::described).collect()
    };
    let behind = described(&|perms| perms < PERMISSIONS_VERSION);
    if !behind.is_empty() {
        return Renewal::Behind(behind);
    }
    let ahead = described(&|perms| perms > PERMISSIONS_VERSION);
    if !ahead.is_empty() {
        return Renewal::Ahead(ahead);
    }
    Renewal::Allowed
}

/// What the running components publish, read with a human's credentials, whose rights `may` tells: the live roster
/// entries, the markers of webhooks and schedules, and the channel manager's entry, each put within
/// [`SERVING_STALE_AFTER`] of `now`.
pub async fn running(js: &jetstream::Context, may: impl Fn(&str) -> bool, now: DateTime<Utc>) -> Result<Running> {
    let mut running = Running::default();
    match js.get_key_value(ROSTER_BUCKET).await {
        Ok(roster) => {
            let entries = roster::list_entries(&roster).await?;
            running.components.extend(entries.iter().filter(|e| roster::is_live(e, now)).map(Component::of_entry));
        }
        Err(e) => running.unseen.push(format!("no roster read ({e}): daemons not looked at")),
    }

    let stream_name = format!("KV_{WEBHOOKS_BUCKET}");
    if !may(&format!("$JS.API.STREAM.INFO.{stream_name}")) {
        running.unseen.push(format!("the credentials get no stream info of {WEBHOOKS_BUCKET}: webhooks not looked at"));
        return Ok(running);
    }
    let stream = match js.get_stream(&stream_name).await {
        Ok(stream) => Some(stream),
        Err(e) if crate::outbox::not_found(&e) => None,
        Err(e) => return Err(e).context(format!("looking up {WEBHOOKS_BUCKET}")),
    };
    let Some(stream) = stream else { return Ok(running) };
    let prefix = format!("$KV.{WEBHOOKS_BUCKET}.");
    let markers: Vec<(String, usize)> = stream
        .info_with_subjects(format!("{prefix}*.webhook"))
        .await
        .context("listing the webhooks' markers")?
        .try_collect()
        .await
        .context("listing the webhooks' markers")?;
    let mut markers: Vec<String> = markers.into_iter().map(|(subject, _)| subject).collect();
    markers.sort();
    for subject in markers {
        let Some((value, at)) = last(&stream, &subject).await? else { continue };
        if !fresh(at, now) {
            continue;
        }
        let webhook = subject.strip_prefix(&prefix).and_then(|k| k.strip_suffix(".webhook")).unwrap_or(&subject);
        let what = format!("webhook {webhook}");
        match serde_json::from_slice::<Serving>(&value) {
            Ok(serving) => running.components.push(Component::of_serving(&what, serving)),
            Err(e) => running.unseen.push(format!("the marker of {what} is malformed: {e}")),
        }
    }

    let manager = channel_manager_serving_subject();
    if !may(&format!("$JS.API.DIRECT.GET.{stream_name}.{manager}")) {
        running.unseen.push(
            "the credentials may not read the channel manager's version: renew them (`ants auth user NAME --human`)"
                .to_owned(),
        );
        return Ok(running);
    }
    match last(&stream, &manager).await? {
        Some((value, at)) if fresh(at, now) => match serde_json::from_slice::<Serving>(&value) {
            Ok(serving) => running.components.push(Component::of_serving("the channel manager", serving)),
            Err(e) => running.unseen.push(format!("the channel manager's version is malformed: {e}")),
        },
        _ => running.unseen.push(format!(
            "no channel manager published its version in the last {}h: none runs",
            SERVING_STALE_AFTER.as_secs() / 3600
        )),
    }
    Ok(running)
}

/// The last value on `subject` and when the server stored it; none when there is none, or it was deleted.
async fn last(stream: &jetstream::stream::Stream, subject: &str) -> Result<Option<(Vec<u8>, DateTime<Utc>)>> {
    match stream.direct_get_last_for_subject(subject).await {
        Ok(msg) if msg.payload.is_empty() => Ok(None),
        Ok(msg) => {
            let at = DateTime::from_timestamp(msg.time.unix_timestamp(), msg.time.nanosecond()).unwrap_or_default();
            Ok(Some((msg.payload.to_vec(), at)))
        }
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {subject}")),
    }
}

/// Whether a [`Serving`] stored `at` is of a process still running at `now`.
fn fresh(at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    (now - at).to_std().unwrap_or_default() <= SERVING_STALE_AFTER
}

/// The [`Serving`] of this process, as stored.
pub async fn serving() -> Vec<u8> {
    serde_json::to_vec(&Serving::here(roster::machine().await)).expect("a serving entry serializes")
}

/// Put `value` at `key` of `kv` now and every [`SERVING_EVERY`], calling `put` after each put, until dropped. A put
/// that fails is logged and tried again at the next.
pub async fn keep_putting(kv: DirectKv, key: String, value: Vec<u8>, put: impl Fn()) {
    let mut every = tokio::time::interval(SERVING_EVERY);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        every.tick().await;
        match kv.put(&key, value.clone()).await {
            Ok(()) => put(),
            Err(e) => tracing::warn!(key, error = format!("{e:#}"), "publishing the version of ants this runs"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn component(what: &str, version: &str, perms: u32) -> Component {
        Component { what: what.to_owned(), version: version.to_owned(), perms: PermissionsVersion(perms) }
    }

    #[test]
    fn mixed_versions_are_counted_newest_first() {
        let now = PERMISSIONS_VERSION.0;
        let same = [component("a", "0.2.0", now), component("b", "0.2.0", now)];
        assert_eq!(mixed(&same), None);
        assert_eq!(mixed(&[]), None);
        let mut all = same.to_vec();
        all.extend([component("c", "0.10.0", now), component("e", "0.9.1", now)]);
        assert_eq!(mixed(&all).as_deref(), Some("0.10.0 (1), 0.9.1 (1), 0.2.0 (2)"));
    }

    #[test]
    fn a_harness_at_different_versions_across_machines_drifts() {
        let now = Utc::now();
        let entry = |ant: &str, machine: &str, agent: Option<(&str, &str)>| RosterEntry {
            ant: ant.parse().unwrap(),
            machine: machine.to_owned(),
            instance: "run-1".to_owned(),
            harness: "copilot".to_owned(),
            agent: agent
                .map(|(name, version)| ants_proto::HarnessAgent { name: name.to_owned(), version: version.to_owned() }),
            capabilities: Vec::new(),
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: ants_proto::Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: Vec::new(),
            version: ants_proto::ANTS_VERSION.to_owned(),
            perms: ants_proto::PERMISSIONS_VERSION,
            signed_out: None,
            limited: None,
            failing: None,
            fallback: None,
            memory_low: None,
            model: None,
        };
        let same = [entry("dev-a", "m1", Some(("copilot", "1.0.3"))), entry("dev-b", "m2", Some(("copilot", "1.0.3")))];
        assert!(harness_drift(&same, now).is_empty());
        assert_eq!(harness_note(&same, now), None);
        let one_machine =
            [entry("dev-a", "m1", Some(("copilot", "1.0.3"))), entry("dev-b", "m1", Some(("copilot", "1.0.2")))];
        assert!(harness_drift(&one_machine, now).is_empty(), "an upgrade on one machine while a harness still runs");

        let mut stale = entry("dev-e", "m3", Some(("copilot", "0.9.0")));
        stale.heartbeat_at = now - chrono::Duration::hours(1);
        let mut all = same.to_vec();
        all.extend([
            entry("dev-c", "m2", Some(("copilot", "1.0.10"))),
            entry("dev-d", "m1", None),
            stale,
            entry("rev-a", "m1", Some(("codex-acp", "0.3.0"))),
            entry("rev-b", "m2", Some(("codex-acp", "0.3.0"))),
        ]);
        assert_eq!(harness_drift(&all, now), ["copilot at 1.0.10 (dev-c on m2), 1.0.3 (dev-a on m1, dev-b on m2)"]);
        assert_eq!(
            harness_note(&all, now).as_deref(),
            Some(
                "harness versions differ across machines: copilot at 1.0.10 (dev-c on m2), 1.0.3 (dev-a on m1, dev-b on m2)"
            )
        );
    }

    #[test]
    fn renewal_waits_for_every_component() {
        let now = PERMISSIONS_VERSION.0;
        let current = component("the daemon of dev-a on m1", "0.2.0", now);
        assert_eq!(renewal(&[]), Renewal::Allowed);
        assert_eq!(renewal(std::slice::from_ref(&current)), Renewal::Allowed);

        let old = component("webhook github on m2", "0.1.0", now - 1);
        let Renewal::Behind(behind) = renewal(&[current.clone(), old]) else { panic!("behind") };
        assert_eq!(behind, [format!("webhook github on m2 (ants 0.1.0, permissions {})", now - 1)]);

        let newer = component("the channel manager on m1", "9.0.0", now + 1);
        let Renewal::Ahead(ahead) = renewal(&[current, newer]) else { panic!("ahead") };
        assert_eq!(ahead, [format!("the channel manager on m1 (ants 9.0.0, permissions {})", now + 1)]);
    }
}
