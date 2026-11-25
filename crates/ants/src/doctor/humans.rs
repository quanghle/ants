//! The humans the config file names: its top-level `human`, an ant's own `human` and each `stand_in`, each of which
//! escalations and tool requests fall back on, read from the file, not the hub. A human revoked whole
//! (`ants auth revoke`) is out of `ants_humans`, yet still named there until the file changes.

use std::path::Path;

use ants_daemon::config::{self, Ant};
use ants_proto::{HUMANS_BUCKET, Humans, UserId};

use super::{Check, Connected, NETWORK_TIMEOUT};

const CHECK: &str = "config humans";

/// The humans the config file at `path` names ([`named_humans`]) against the bus's humans, as the CLI's credentials read
/// `ants_humans` ([`findings`]); nothing without a config file read, a skip while the bucket is unread.
pub(super) async fn check_config_humans(
    connected: Option<&Connected>,
    loaded: Option<&config::Loaded>,
    path: &Path,
) -> Vec<Check> {
    let Some(loaded) = loaded else { return Vec::new() };
    let top = config::Summary::load(path).ok().and_then(|summary| summary.human);
    let named = named_humans(top.as_ref(), &loaded.ants);
    if named.is_empty() {
        return Vec::new();
    }
    let Some(connected) = connected else { return vec![Check::skip(CHECK, "the hub is not reached")] };
    let read = async {
        let kv = connected.js.get_key_value(HUMANS_BUCKET).await?;
        crate::people::humans::read_from(&kv).await
    };
    match tokio::time::timeout(NETWORK_TIMEOUT * 2, read).await {
        Ok(Ok(humans)) => findings(&named, &humans, path),
        Ok(Err(e)) => vec![Check::skip(CHECK, format!("{HUMANS_BUCKET} unread: {e:#}"))],
        Err(_) => vec![Check::skip(CHECK, format!("{HUMANS_BUCKET} unread within 10s"))],
    }
}

/// The humans a config file names, each with the key naming it: its top-level `human` (`top`), the `human` of an
/// ant of `ants` setting another, and the `stand_in` of each ant setting one.
pub(super) fn named_humans(top: Option<&UserId>, ants: &[Ant]) -> Vec<(String, UserId)> {
    let mut named: Vec<(String, UserId)> =
        top.map(|human| ("the top-level `human`".to_owned(), human.clone())).into_iter().collect();
    for ant in ants {
        if let Some(human) = ant.human.as_ref().filter(|human| Some(*human) != top) {
            named.push((format!("`human` of the [[ant]] {}", ant.name), human.clone()));
        }
        if let Some(stand_in) = &ant.stand_in {
            named.push((format!("`stand_in` of the [[ant]] {}", ant.name), stand_in.clone()));
        }
    }
    named
}

/// One warning for each of `named` not among `humans`, naming the key of the config file at `path` to change; else
/// one check telling they all are.
pub(super) fn findings(named: &[(String, UserId)], humans: &Humans, path: &Path) -> Vec<Check> {
    let config = path.display();
    let mut checks = Vec::new();
    for (key, human) in named.iter().filter(|(_, human)| !humans.contains(human)) {
        let detail = format!(
            "{key} in {config} names {human}, not a human of the bus ({HUMANS_BUCKET} has no entry, as after `ants auth \
             revoke {human}`): the escalations and tool requests falling back on {human} reach no human"
        );
        let fix = vec![
            format!("change {key} in {config} to a human of the bus"),
            format!(
                "or, should {human} still be one, connect once with {human}'s credentials, which registers {human}"
            ),
        ];
        checks.push(Check::warn(CHECK, detail, fix));
    }
    if checks.is_empty() {
        let mut names: Vec<&str> = named.iter().map(|(_, human)| human.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        let what = if names.len() == 1 { "a human" } else { "humans" };
        checks.push(Check::ok(CHECK, format!("{config} names {}, {what} of the bus", names.join(", "))));
    }
    checks
}
