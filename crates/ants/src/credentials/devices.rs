//! What the registry of issued keys tells of who holds them: the devices of a human, selected, relabelled and listed
//! with what marks them, the credentials of a human's assistant, and the admins', each with whether it is revoked.

use std::path::Path;

use ants_daemon::config::registration::Grant;
use ants_proto::{DeviceId, DeviceKey};
use anyhow::{Context as _, Result, bail};
use serde::Serialize;

use super::issued::{ISSUED, Issued, Kind, issued, latest_lines, record};
use super::revoke::revoked;

/// Longest device label.
const MAX_LABEL: usize = 64;

/// Fail unless `label` can label a device: 1 to 64 characters, none of them control characters.
pub fn check_label(label: &str) -> Result<()> {
    if label.trim().is_empty() || label.chars().count() > MAX_LABEL || label.chars().any(char::is_control) {
        bail!("invalid device label {label:?}: use 1-{MAX_LABEL} characters, no control characters");
    }
    Ok(())
}

/// One device of a human: the latest line of [`ISSUED`] for one of its keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Device {
    pub device: DeviceId,
    pub label: Option<String>,
    pub key: String,
    /// The encryption key its latest credentials bind, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xkey: Option<DeviceKey>,
    pub issued_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    pub revoked: bool,
    /// The login of the GitHub user the device was signed for, if bound.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub github: Option<String>,
    /// Why `ants signer` signed the device, if it registered it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grant: Option<Grant>,
    /// Whether its credentials carry the admin role ([`ants_proto::ADMIN_TAG`]).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub admin: bool,
    /// Whether it is a web device, a page of the web client ([`ants_proto::WEB_TAG`]).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub web: bool,
}

/// The devices of the human `name` in the registry of `dir`, oldest first: one per key issued to it as a human, but one
/// per device id, with its latest key, for a device whose credentials were renewed (`ants renew`).
pub fn devices(dir: &Path, name: &str) -> Result<Vec<Device>> {
    let revoked = revoked(dir)?;
    let mut found: Vec<Device> = Vec::new();
    for one in issued(dir)?.into_iter().filter(|i| i.name == name && i.kind == Kind::Human) {
        let id =
            one.device.with_context(|| format!("{ISSUED}: the key {} of the human {name} names no device", one.key))?;
        let device = Device {
            device: id,
            label: one.label,
            revoked: revoked.iter().any(|r| r.key == one.key),
            key: one.key,
            xkey: one.xkey,
            issued_at: one.issued_at,
            expires_at: one.expires_at,
            github: one.github.map(|g| g.login),
            grant: one.grant,
            admin: one.admin,
            web: one.web,
        };
        let same = |d: &Device| d.key == device.key || d.device == device.device;
        match found.iter_mut().find(|d| same(d)) {
            Some(known) => *known = Device { issued_at: known.issued_at, ..device },
            None => found.push(device),
        }
    }
    Ok(found)
}

/// The device of `devices` that `selector` names: its id, its public key, or its label when no other device of the
/// human has that label.
pub fn select<'a>(devices: &'a [Device], name: &str, selector: &str) -> Result<&'a Device> {
    let by_id_or_key = devices.iter().find(|d| d.key == selector || d.device.to_string() == selector);
    if let Some(device) = by_id_or_key {
        return Ok(device);
    }
    let labelled: Vec<&Device> = devices.iter().filter(|d| d.label.as_deref() == Some(selector)).collect();
    match labelled.as_slice() {
        [device] => Ok(device),
        [] => bail!("{name} has no device {selector:?}; `ants auth devices {name}` lists them"),
        several => {
            let ids: Vec<String> = several.iter().map(|d| d.device.to_string()).collect();
            bail!("{name} has {} devices labelled {selector:?}; name one by id: {}", several.len(), ids.join(", "))
        }
    }
}

/// The keys of the devices of the human `name` in `dir` that `selectors` name ([`select`]), to revoke them alone: each
/// key a device had, its renewed credentials' and the earlier ones.
pub fn device_keys(dir: &Path, name: &str, selectors: &[String]) -> Result<Vec<String>> {
    let devices = devices(dir, name)?;
    let issued = issued(dir)?;
    let mut keys = Vec::new();
    for selector in selectors {
        let device = select(&devices, name, selector)?;
        let of_device = |i: &&Issued| i.name == name && i.kind == Kind::Human && i.device == Some(device.device);
        keys.push(device.key.clone());
        keys.extend(issued.iter().filter(of_device).map(|i| i.key.clone()));
    }
    let mut seen = std::collections::HashSet::new();
    keys.retain(|k| seen.insert(k.clone()));
    Ok(keys)
}

/// Label the device of the human `name` that `selector` names ([`select`]) `label`, appending its line to the registry
/// of `dir` again with the new label. Returns the device as relabelled.
pub fn rename(dir: &Path, name: &str, selector: &str, label: &str) -> Result<Device> {
    check_label(label)?;
    let devices = devices(dir, name)?;
    let device = select(&devices, name, selector)?;
    let latest = issued(dir)?.into_iter().rev().find(|i| i.key == device.key && i.kind == Kind::Human);
    let latest = latest.context("the device's registry line")?;
    record(dir, &Issued { label: Some(label.to_owned()), ..latest })?;
    Ok(Device { label: Some(label.to_owned()), ..device.clone() })
}

/// What marks `device` in the list of `ants auth devices`: `admin` for an admin's, `web` for a web device.
pub(crate) fn marks(device: &Device) -> String {
    let marks = [(device.admin, "  admin"), (device.web, "  web")];
    marks.into_iter().filter_map(|(marked, mark)| marked.then_some(mark)).collect()
}

/// The keys of the credentials of the assistant of the human `name` in `dir` ([`Kind::Assistant`]), oldest first,
/// each with whether it is revoked.
pub fn assistants(dir: &Path, name: &str) -> Result<Vec<(Issued, bool)>> {
    let revoked = revoked(dir)?;
    let of = |i: &Issued| i.kind == Kind::Assistant && i.assistant_of.as_ref().is_some_and(|h| h.as_str() == name);
    let lines = latest_lines(dir)?.into_iter().filter(of);
    Ok(lines.map(|i| (revoked.iter().any(|r| r.key == i.key), i)).map(|(r, i)| (i, r)).collect())
}

/// The keys of the credentials in `dir` carrying the admin role ([`Issued::admin`]) neither revoked nor expired at
/// `now`, oldest first.
pub fn admins(dir: &Path, now: u64) -> Result<Vec<Issued>> {
    let revoked = revoked(dir)?;
    let live = |i: &Issued| !revoked.iter().any(|r| r.key == i.key) && i.expires_at.is_none_or(|exp| exp > now);
    Ok(latest_lines(dir)?.into_iter().filter(|i| i.admin && live(i)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth;

    #[test]
    fn a_github_binding_and_its_grant_are_listed_with_the_device_and_kept_on_renewal() {
        use ants_daemon::config::registration::{GithubAccount, Grant};
        let dir = ants_test_support::scratch_dir("binding");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let alice: ants_proto::UserId = "alice".parse().unwrap();
        let new = auth::Registered {
            label: Some("Laptop"),
            github: Some(GithubAccount { id: 42, login: "octocat".into() }),
            grant: Some(Grant::Org("acme".into())),
            ..auth::Registered::default()
        };
        let key = nkeys::KeyPair::new_user();
        let permissions = ants_proto::Permissions::for_user(&alice, ants_proto::Role::Human);
        let (jwt, device) = auth::Issue::new(&team, &alice, Kind::Human, permissions)
            .with(&new)
            .for_key(&key.public_key(), None)
            .unwrap();

        let listed = devices(&team, "alice").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!((listed[0].github.as_deref(), listed[0].grant.as_ref()), (Some("octocat"), new.grant.as_ref()));
        auth::renew(&team, &jwt, &nkeys::KeyPair::new_user().public_key(), None).unwrap();
        let renewed = issued(&team).unwrap().pop().unwrap();
        assert_eq!(renewed.device, device);
        assert_eq!((renewed.github, renewed.grant), (new.github, new.grant));
    }
}
