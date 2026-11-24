//! What `ants auth issued`, `ants auth devices`, `ants auth admin list` and `ants auth revoked` print of the registry
//! of issued keys and the revocations.

use std::fs;
use std::path::Path;

use ants_proto::{DeviceKey, PERMISSIONS_VERSION};
use anyhow::{Context as _, Result};
use serde::Serialize;

use super::devices::{admins, assistants, devices, marks};
use super::issued::{Issued, Kind, latest_lines};
use super::revoke::revoked;
use crate::auth;

/// One key of the registry, as `ants auth issued` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listed {
    /// Its latest line in [`ISSUED`](super::issued::ISSUED).
    #[serde(flatten)]
    pub issued: Issued,
    pub revoked: bool,
    /// A later key was issued for the same name and kind; for a human's, only a later key of the same device (renewed
    /// credentials), as each other one is another device.
    pub reissued: bool,
    /// Its permissions version is not [`PERMISSIONS_VERSION`].
    pub outdated: bool,
    /// It expires within [`ants_daemon::secrets::creds::EXPIRY_WARNING`].
    pub expiring: bool,
    /// The commands that renew it, when it is in use (neither revoked, reissued, expired nor an enrollment's) and
    /// outdated or expiring: from its `.creds` file in the auth directory when there is one
    /// ([`ants_daemon::secrets::creds::Finding::renew`]), else with placeholders for what only that file tells.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub renew: Vec<String>,
}

/// The keys of the registry of `dir` at `now`, oldest first, each once.
pub fn listing(dir: &Path, now: u64) -> Result<Vec<Listed>> {
    let revoked = revoked(dir)?;
    let latest = latest_lines(dir)?;
    let files = team_findings(dir)?;
    let mut listed = Vec::new();
    for (n, one) in latest.iter().enumerate() {
        let device = |later: &Issued| one.kind != Kind::Human || later.device == one.device;
        let reissued =
            latest[n + 1..].iter().any(|later| later.name == one.name && later.kind == one.kind && device(later));
        let revoked = revoked.iter().any(|r| r.key == one.key);
        let expired = one.expires_at.is_some_and(|exp| exp <= now);
        let expiring = !expired
            && one.expires_at.is_some_and(|exp| exp - now <= ants_daemon::secrets::creds::EXPIRY_WARNING.as_secs());
        let outdated = one.perms != PERMISSIONS_VERSION;
        let in_use = !revoked && !reissued && !expired && one.kind != Kind::Enrollment;
        let renew = match (in_use && (outdated || expiring), files.iter().find(|f| f.key == one.key)) {
            (false, _) => Vec::new(),
            (true, Some(file)) => file.renew(),
            (true, None) => renew_without_file(one),
        };
        listed.push(Listed { issued: one.clone(), revoked, reissued, outdated, expiring, renew });
    }
    Ok(listed)
}

/// The command renewing `issued` when no `.creds` file tells more: placeholders stand for a webhook's destinations
/// and the hub's JetStream domain; `ants auth leaf` takes a leaf node's members from the registry.
fn renew_without_file(issued: &Issued) -> Vec<String> {
    let name = &issued.name;
    let domain = "[--js-domain DOMAIN]";
    vec![match issued.kind {
        Kind::Ant if let Some(builtin) = ants_proto::BuiltinAnt::from_id(name) => {
            format!("ants auth builtin {} {domain}", builtin.name())
        }
        Kind::Ant => format!("ants auth user {name} {domain}"),
        Kind::Human if issued.admin => format!("ants auth user {name} --human --admin {domain}"),
        Kind::Human if issued.web => format!("ants auth user {name} --human --web --public-key KEY {domain}"),
        Kind::Human => format!("ants auth user {name} --human {domain}"),
        Kind::Webhook => format!("ants auth user {name} --webhook --to ANT... --channel CHANNEL... {domain}"),
        Kind::Leaf | Kind::LeafStore | Kind::LeafSystem => {
            format!("ants auth leaf {} {domain}", name.trim_prefix("leaf-"))
        }
        Kind::ChannelManager => format!("ants auth channel-manager {domain}"),
        Kind::Host => format!("ants auth host {} {domain}", name.trim_prefix("host-")),
        Kind::Observer => format!("ants auth user {name} --observer {domain}"),
        Kind::Enrollment => format!("ants auth enroll {}", name.trim_prefix("enroll-")),
        Kind::Bootstrap => "ants auth bootstrap".to_owned(),
        Kind::Assistant => {
            let of = issued.assistant_of.as_ref().map_or_else(|| "HUMAN".to_owned(), ToString::to_string);
            format!("ants auth user {name} --assistant-of {of} {domain}")
        }
    }]
}

/// What [`ants_daemon::secrets::creds`] finds in the `.creds` files in `dir` that the bus's account signed.
fn team_findings(dir: &Path) -> Result<Vec<ants_daemon::secrets::creds::Finding>> {
    let account = auth::account_key(dir)?;
    let entries = fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    let mut found = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "creds") {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else { continue };
        let Some(jwt) = auth::creds_jwt(&text) else { continue };
        if auth::verified(jwt).is_ok_and(|claims| claims.iss == account)
            && let Ok(finding) = ants_daemon::secrets::creds::Finding::from_jwt(jwt)
        {
            found.push(finding);
        }
    }
    Ok(found)
}

/// Print the keys of the registry of `dir` ([`listing`]): key, name, kind, when issued, expiry, permissions version,
/// whether revoked or reissued, and under each one to renew, why and how; with `json`, one JSON object per line.
pub fn list_issued(dir: &Path, json: bool) -> Result<()> {
    let now = auth::now()?;
    let listed = listing(dir, now)?;
    if listed.is_empty() && !json {
        println!("no credentials issued in {}", dir.display());
    }
    for l in listed {
        if json {
            println!("{}", serde_json::to_string(&l)?);
            continue;
        }
        let i = &l.issued;
        let mut kind = serde_json::to_value(i.kind)?.as_str().unwrap_or("?").to_owned();
        if let Some(of) = &i.assistant_of {
            kind = format!("{kind} of {of}");
        }
        if i.admin {
            kind.push_str(" (admin)");
        }
        if i.web {
            kind.push_str(" (web)");
        }
        let expiry = match i.expires_at {
            Some(exp) if exp <= now => format!("expired {}", time(exp)),
            Some(exp) => format!("expires {}", time(exp)),
            None => "no expiry".to_owned(),
        };
        let perms = i.perms;
        let revoked = if l.revoked { "  revoked" } else { "" };
        let reissued = if l.reissued { "  reissued" } else { "" };
        println!(
            "{}  {}  {kind}  issued {}  {expiry}  perms {perms}{revoked}{reissued}",
            i.key,
            i.name,
            time(i.issued_at)
        );
        if !l.renew.is_empty() {
            let why: Vec<&str> = [(l.outdated, "outdated permissions"), (l.expiring, "expires soon")]
                .into_iter()
                .filter_map(|(yes, why)| yes.then_some(why))
                .collect();
            println!("  renew ({}): {}", why.join(", "), l.renew.join(", then "));
        }
    }
    Ok(())
}

/// Print the devices of the human `name` in `dir`: id, label, key, encryption key, when issued, whether an admin's,
/// whether a web device and whether revoked; with `json`, one JSON object per line. Then, but for `json`, the keys of its assistant's
/// credentials ([`Kind::Assistant`]).
pub fn list_devices(dir: &Path, name: &str, json: bool) -> Result<()> {
    let devices = devices(dir, name)?;
    let assistants = if json { Vec::new() } else { assistants(dir, name)? };
    if devices.is_empty() && !json {
        println!("no devices of {name} known in {}", dir.display());
    }
    for d in devices {
        if json {
            println!("{}", serde_json::to_string(&d)?);
            continue;
        }
        let id = d.device;
        let label = d.label.as_deref().unwrap_or("-");
        let revoked = if d.revoked { "  revoked" } else { "" };
        let xkey = d.xkey.as_ref().map_or("-", DeviceKey::as_str);
        let github = match (&d.github, &d.grant) {
            (Some(login), Some(grant)) => format!("  github {login} ({grant})"),
            (Some(login), None) => format!("  github {login}"),
            (None, _) => String::new(),
        };
        println!("{id}  {label}  {}  {xkey}  issued {}{github}{}{revoked}", d.key, time(d.issued_at), marks(&d));
    }
    for (one, revoked) in assistants {
        let revoked = if revoked { "  revoked" } else { "" };
        println!("assistant {}  {}  issued {}{revoked}", one.name, one.key, time(one.issued_at));
    }
    Ok(())
}

/// Print [`admins`]: human, device, label, key, when issued and expiry; with `json`, one JSON object per line.
pub fn list_admins(dir: &Path, json: bool) -> Result<()> {
    let now = auth::now()?;
    let admins = admins(dir, now)?;
    if admins.is_empty() && !json {
        println!("no admin credentials in {}", dir.display());
    }
    for a in admins {
        if json {
            let admin = serde_json::json!({
                "name": a.name, "device": a.device, "label": a.label, "key": a.key, "issued_at": a.issued_at,
                "expires_at": a.expires_at,
            });
            println!("{admin}");
            continue;
        }
        let device = a.device.map_or_else(|| "-".to_owned(), |d| d.to_string());
        let label = a.label.as_deref().unwrap_or("-");
        let expiry = a.expires_at.map_or_else(|| "no expiry".to_owned(), |exp| format!("expires {}", time(exp)));
        println!("{}  {device}  {label}  {}  issued {}  {expiry}", a.name, a.key, time(a.issued_at));
    }
    Ok(())
}

/// Print the revocations of `dir`: key, name if known, when revoked and until when it matters.
pub fn list(dir: &Path) -> Result<()> {
    let revoked = revoked(dir)?;
    if revoked.is_empty() {
        println!("no credentials revoked");
    }
    let now = auth::now()?;
    for r in revoked {
        let until = match r.expires_at {
            Some(exp) if exp <= now => format!("expired {}", time(exp)),
            Some(exp) => format!("expires {}", time(exp)),
            None => "no expiry".to_owned(),
        };
        println!("{}  {}  revoked {}  {until}", r.key, r.name.as_deref().unwrap_or("-"), time(r.revoked_at));
    }
    Ok(())
}

fn time(secs: u64) -> String {
    ants_daemon::secrets::creds::time(secs)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::credentials::issued::{ISSUED, issued};

    #[test]
    fn the_listing_names_the_renewal_of_credentials_in_use_that_are_outdated_or_expire_soon() {
        let dir = ants_test_support::scratch_dir("issued");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let ant = |name: &str, out: &Path| {
            crate::test_support::issue(&team, &name.parse().unwrap(), ants_proto::Role::Ant, None, out).unwrap();
        };
        ant("dev-a", &dir.join("dev-a-old.creds"));
        ant("dev-a", &team.join("dev-a.creds"));
        ant("dev-b", &dir.join("dev-b.creds"));
        ant("dev-c", &team.join("dev-c.creds"));
        auth::issue_enrollment(&team, &"dev-d".parse().unwrap(), Duration::from_secs(60), &dir.join("e.creds"))
            .unwrap();
        let now = auth::now().unwrap();
        // dev-a's keys carry an older permissions version; dev-b's expires in a day.
        let lines: String = issued(&team)
            .unwrap()
            .into_iter()
            .map(|i| match i.name.as_str() {
                "dev-a" => Issued { perms: ants_proto::PermissionsVersion(0), ..i },
                "dev-b" => Issued { expires_at: Some(now + 86_400), ..i },
                _ => i,
            })
            .map(|i| serde_json::to_string(&i).unwrap() + "\n")
            .collect();
        fs::write(team.join(ISSUED), lines).unwrap();

        let listed = listing(&team, now).unwrap();
        let summary: Vec<(&str, bool, bool, bool, Vec<String>)> = listed
            .iter()
            .map(|l| (l.issued.name.as_str(), l.reissued, l.outdated, l.expiring, l.renew.clone()))
            .collect();
        assert_eq!(
            summary,
            [
                ("dev-a", true, true, false, vec![]),
                ("dev-a", false, true, false, vec!["ants auth user dev-a".to_owned()]),
                ("dev-b", false, false, true, vec!["ants auth user dev-b [--js-domain DOMAIN]".to_owned()]),
                ("dev-c", false, false, false, vec![]),
                ("enroll-dev-d", false, false, true, vec![]),
            ]
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
