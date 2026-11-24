//! What the colony records: the versions of ants and harnesses running across the team, avatars, and the machines'
//! drift.

use ants_proto::{ColonyEntry, PERMISSIONS_VERSION};
use async_nats::jetstream;

use super::{Check, Identity, NETWORK_TIMEOUT};
use crate::ants::avatars::{Depth, Faces};
use crate::release::versions;

/// The versions of ants every daemon, webhook, schedule and channel manager runs ([`versions::running`]), and whether
/// credentials renewed with this ants suit them all.
pub(super) async fn check_versions(js: &jetstream::Context, identity: &Identity) -> Check {
    let check = "versions";
    if !identity.me.role.is_human() {
        return Check::skip(check, "an ant's credentials read no versions: run `ants doctor` as a human");
    }
    let may = |subject: &str| identity.rights.may_publish(subject);
    let now = chrono::Utc::now();
    let running = match tokio::time::timeout(NETWORK_TIMEOUT * 2, versions::running(js, may, now)).await {
        Ok(Ok(running)) => running,
        Ok(Err(e)) => return Check::warn(check, format!("{e:#}"), Vec::new()),
        Err(_) => return Check::warn(check, "no versions read within 10s", Vec::new()),
    };
    let mut notes: Vec<String> =
        versions::mixed(&running.components).map(|v| format!("mixed: {v}")).into_iter().collect();
    notes.extend(running.unseen);
    let notes = if notes.is_empty() { String::new() } else { format!(" ({})", notes.join("; ")) };
    let renewing = "renewing ant, webhook, channel manager and leaf credentials";
    match versions::renewal(&running.components) {
        versions::Renewal::Allowed => Check::ok(
            check,
            format!(
                "{} running components at permissions version {PERMISSIONS_VERSION} or newer: {renewing} is safe{notes}",
                running.components.len()
            ),
        ),
        versions::Renewal::Behind(behind) => Check::warn(
            check,
            format!("older than permissions version {PERMISSIONS_VERSION}: {}{notes}", behind.join(", ")),
            vec![format!("upgrade ants where they run and restart them before {renewing}")],
        ),
        versions::Renewal::Ahead(ahead) => Check::warn(
            check,
            format!("newer than permissions version {PERMISSIONS_VERSION} of this ants: {}{notes}", ahead.join(", ")),
            vec![format!("upgrade ants here and on the machine holding the team keys before {renewing}")],
        ),
    }
}

/// The colony's entries, read within twice [`NETWORK_TIMEOUT`], else why not.
pub(super) async fn read_colony(js: &jetstream::Context) -> Result<Vec<ColonyEntry>, String> {
    use anyhow::Context as _;
    let read = async {
        let kv = js.get_key_value(ants_proto::COLONY_BUCKET).await.context("opening the colony")?;
        ants_daemon::colony::list_entries(&kv).await
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok(entries)) => Ok(entries),
        Ok(Err(e)) => Err(format!("no colony read: {e:#}")),
        Err(_) => Err("no colony read within 10s".to_owned()),
    }
}

/// Whether live ants run the same harness agent at different versions on different machines, as their colony entries
/// `colony` name it ([`versions::harness_drift`]).
pub(super) fn check_harness_versions(colony: &Result<Vec<ColonyEntry>, String>) -> Check {
    match colony {
        Ok(entries) => harness_versions(entries, chrono::Utc::now()),
        Err(why) => Check::skip("harness versions", why.clone()),
    }
}

/// Whether two ants in the colony look alike: the same face in the same colour ([`Faces::collisions`]).
pub(super) async fn check_avatars(js: &jetstream::Context, identity: &Identity) -> [Check; 2] {
    let (check, moved_check) = ("avatars", "moved ants");
    if !identity.me.role.is_human() {
        let why = "checked with a human's credentials, which list the colony";
        return [Check::skip(check, why), Check::skip(moved_check, why)];
    }
    let read = async {
        let entries = crate::ants::colony::list(js).await?;
        anyhow::Ok((entries, crate::ants::avatars::read(js).await?))
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok((entries, identities))) => {
            let moved = moved(&identities, chrono::Utc::now());
            [avatars(&entries, Faces::new(identities, Depth::Plain, ants_proto::Theme::Dark)), moved]
        }
        Ok(Err(e)) => {
            let why = format!("no colony or identities read: {e:#}");
            [Check::skip(check, why.clone()), Check::skip(moved_check, why)]
        }
        Err(_) => {
            let why = "no colony or identities read within 10s";
            [Check::skip(check, why), Check::skip(moved_check, why)]
        }
    }
}

/// The ants whose machines' configs set some of their shared settings otherwise, as the machines' records tell.
pub(super) async fn check_drift(js: &jetstream::Context, identity: &Identity) -> Check {
    let check = "shared settings";
    if !identity.me.role.is_human() {
        return Check::skip(check, "checked with a human's credentials, which list the machines");
    }
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, crate::ants::machines::list(js)).await {
        Ok(Ok(records)) => drift(&records),
        Ok(Err(e)) => Check::skip(check, format!("no machine records read: {e:#}")),
        Err(_) => Check::skip(check, "no machine records read within 10s"),
    }
}

pub(super) fn drift(records: &[ants_proto::MachineRecord]) -> Check {
    let check = "shared settings";
    let lines: Vec<String> = records
        .iter()
        .flat_map(|r| r.drift.iter().map(move |(ant, fields)| format!("{ant}: {} on {}", fields.join(", "), r.machine)))
        .collect();
    if lines.is_empty() {
        return Check::ok(check, format!("no drift on {} machines", records.len()));
    }
    Check::warn(
        check,
        format!("machines' configs set ants' shared settings otherwise, which they ignore: {}", lines.join("; ")),
        vec![
            "`ants ant show ANT` lists the shared settings that serve; remove the fields from the machine's \
             config.toml, or set them on every machine with `ants ant set ANT FIELD=VALUE`"
                .to_owned(),
        ],
    )
}

/// The ants `ants ant export --move` marked moved to another bus, which no daemon here serves.
pub(super) fn moved(identities: &crate::ants::avatars::Identities, now: chrono::DateTime<chrono::Utc>) -> Check {
    let check = "moved ants";
    let lines: Vec<String> = identities
        .values()
        .filter_map(|i| i.moved.as_ref().map(|mark| crate::ants::export::moved_line(&i.ant, mark, now)))
        .collect();
    if lines.is_empty() {
        return Check::ok(check, "no ant is marked moved to another bus");
    }
    Check::warn(check, format!("{} ants marked moved to another bus, served by no daemon here", lines.len()), lines)
}

pub(super) fn avatars(entries: &[ants_proto::ColonyEntry], faces: Faces) -> Check {
    let check = "avatars";
    let collisions = faces.collisions(entries.iter().map(|e| &e.ant));
    if collisions.is_empty() {
        return Check::ok(check, format!("{} ants in the colony, each with a face of its own", entries.len()));
    }
    Check::warn(check, "ants look alike", collisions)
}

pub(super) fn harness_versions(entries: &[ColonyEntry], now: chrono::DateTime<chrono::Utc>) -> Check {
    let check = "harness versions";
    let drift = versions::harness_drift(entries, now);
    if !drift.is_empty() {
        return Check::warn(
            check,
            format!("different versions across machines: {}", drift.join("; ")),
            vec![
                "upgrade the older harness agents where they run; an ant's colony entry shows the new version once its \
                 harness starts again"
                    .to_owned(),
            ],
        );
    }
    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    let named = entries.iter().filter(|e| ants_daemon::colony::is_live(e, now)).filter_map(|e| e.agent.as_ref());
    for agent in named {
        *counts.entry(agent.to_string()).or_default() += 1;
    }
    if counts.is_empty() {
        return Check::ok(
            check,
            "no live ant names its harness agent yet: none started a harness since its daemon did",
        );
    }
    let running: Vec<String> = counts.into_iter().map(|(agent, n)| format!("{agent} ({n})")).collect();
    Check::ok(check, format!("live ants run {}", running.join(", ")))
}
