//! The registry of the user keys the team issued, and the devices of humans it tells.
//!
//! Every user key the team directory issues or signs is recorded in [`ISSUED`], one JSON object per line, so that
//! `ants auth revoke NAME` finds all the keys of a name: a human may hold credentials on several devices, and
//! enrollment and re-issued credentials add more.

use std::collections::BTreeSet;
use std::fs;
use std::io::Write as _;
use std::path::Path;

use ants_daemon::config::registration::{GithubAccount, Grant};
use ants_proto::{DeviceId, DeviceKey, PERMISSIONS_VERSION, PermissionsVersion};
use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

use super::revoke::revoked;
use crate::auth;

/// The registry of issued user keys in the team directory.
pub const ISSUED: &str = "issued.jsonl";

/// What credentials an issued key is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    Ant,
    Human,
    Webhook,
    Leaf,
    /// The store account uplink of a leaf node, `leaf-<machine>` like its uplink: issued, and revoked by name, with
    /// it, in the store account.
    LeafStore,
    /// The system account uplink of a leaf node, `leaf-<machine>` like its uplink: issued, and revoked by name, with
    /// it, in the system account.
    LeafSystem,
    ChannelManager,
    /// A machine's host credentials, `host-<machine>`.
    Host,
    Enrollment,
    Observer,
    /// The team's public bootstrap credentials, with which `ants login` asks to register a human's device.
    Bootstrap,
    /// An ant that is a human's assistant ([`Issued::assistant_of`]).
    Assistant,
}

impl From<ants_proto::Role> for Kind {
    fn from(role: ants_proto::Role) -> Self {
        match role {
            ants_proto::Role::Human => Self::Human,
            ants_proto::Role::Ant => Self::Ant,
        }
    }
}

/// One user key the team issued, a line of [`ISSUED`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Issued {
    /// The user's public NKey, `U...`.
    pub key: String,
    /// The name its JWT carries: the ant, human, webhook or observer, `leaf-<machine>`, `channel-manager` or
    /// `enroll-<ant>`.
    pub name: String,
    pub kind: Kind,
    /// Seconds since the epoch.
    pub issued_at: u64,
    /// Seconds since the epoch; none for credentials that never expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    /// A human's device the credentials are for, whose user JWT carries it ([`DeviceId::tag`]); none for an ant's, a
    /// webhook's and so on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
    /// The label of a human's device, for display: a later line of the same key relabels it ([`rename`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The encryption key of a human's device that its user JWT binds ([`DeviceKey::tag`]), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xkey: Option<DeviceKey>,
    /// The permissions version its JWT carries ([`PERMISSIONS_VERSION`]).
    pub perms: PermissionsVersion,
    /// The key of the credentials these renew (`ants renew`), of the same name, kind and device, which stays valid
    /// until it expires or is revoked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renewed_from: Option<String>,
    /// The members of a leaf node's uplink ([`crate::node::members`]); none for other credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<crate::node::members::Members>,
    /// The template of an ant a manager added (`ants signer`), whose `max_ants` the signer counts its live keys
    /// against; none for other credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// The manager ant that added an ant from `template`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_by: Option<ants_proto::UserId>,
    /// The GitHub user a human's name is bound to ([`crate::signing::binding`]), on the credentials of each of its devices
    /// signed since.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github: Option<GithubAccount>,
    /// Why `ants signer` signed a registered device; none for credentials `ants auth` issued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<Grant>,
    /// When revoking every key of the name withdrew the approval grant of a registered device (seconds since the
    /// epoch), on a later line of its key ([`super::revoke::revoke`]): the binding stays, the approval no longer
    /// stands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawn: Option<u64>,
    /// Whether a human's credentials carry the admin role ([`ants_proto::ADMIN_TAG`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub admin: bool,
    /// Whether a human's credentials are a web device's ([`ants_proto::WEB_TAG`]).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub web: bool,
    /// The human an assistant's credentials make the ant the assistant of ([`ants_proto::AssistantOf`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assistant_of: Option<ants_proto::UserId>,
    /// The admin order that added the ant on its machine (`ants ant add`), which `ants signer` signed the key on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<ants_proto::Uuid>,
}

impl Issued {
    /// A key issued now.
    pub fn new(key: &str, name: &str, kind: Kind, expires_at: Option<u64>) -> Result<Self> {
        let issued_at = auth::now()?;
        Ok(Self {
            key: key.to_owned(),
            name: name.to_owned(),
            kind,
            issued_at,
            expires_at,
            device: None,
            label: None,
            xkey: None,
            perms: PERMISSIONS_VERSION,
            renewed_from: None,
            members: None,
            template: None,
            added_by: None,
            github: None,
            grant: None,
            withdrawn: None,
            admin: false,
            web: false,
            assistant_of: None,
            order: None,
        })
    }

    /// The key, for `device` labelled `label`, whose encryption key is `xkey`.
    pub fn for_device(self, device: DeviceId, label: Option<String>, xkey: Option<DeviceKey>) -> Self {
        Self { device: Some(device), label, xkey, ..self }
    }
}

/// Record `issued` in the registry of `dir`.
pub fn record(dir: &Path, issued: &Issued) -> Result<()> {
    let path = dir.join(ISSUED);
    let mut line = serde_json::to_string(issued)?;
    line.push('\n');
    // One append of a whole line, so concurrent issuers do not interleave.
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    file.write_all(line.as_bytes()).with_context(|| format!("writing {}", path.display()))
}

/// The keys recorded in the registry of `dir`, oldest first.
pub fn issued(dir: &Path) -> Result<Vec<Issued>> {
    let path = dir.join(ISSUED);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let lines = text.lines().enumerate().filter(|(_, line)| !line.trim().is_empty());
    lines.map(|(n, line)| serde_json::from_str(line).with_context(|| format!("{}:{}", path.display(), n + 1))).collect()
}

/// The latest line of each key of `issued`, oldest key first.
pub(super) fn latest(issued: &[Issued]) -> Vec<&Issued> {
    let mut latest: Vec<&Issued> = Vec::new();
    for one in issued {
        match latest.iter_mut().find(|known| known.key == one.key) {
            Some(known) => *known = one,
            None => latest.push(one),
        }
    }
    latest
}

/// The latest line of each key of the registry of `dir`, oldest first, keeping when its key was first issued.
pub(super) fn latest_lines(dir: &Path) -> Result<Vec<Issued>> {
    let mut latest: Vec<Issued> = Vec::new();
    for one in issued(dir)? {
        match latest.iter_mut().find(|known| known.key == one.key) {
            Some(known) => *known = Issued { issued_at: known.issued_at, ..one },
            None => latest.push(one),
        }
    }
    Ok(latest)
}

/// The latest line of the key `key` in the registry `issued`, if a registration signed it: one with a grant
/// ([`Issued::grant`]), whose renewals `ants signer` answers.
pub fn registered<'a>(issued: &'a [Issued], key: &str) -> Option<&'a Issued> {
    issued.iter().rev().find(|i| i.key == key).filter(|i| i.grant.is_some())
}

/// The approval grant that stands for the GitHub user `github` (a GitHub user id) on the name `name` in the registry
/// `issued`: a human's approval that registered a key of the name for the user, not withdrawn since
/// ([`Issued::withdrawn`]).
pub fn standing_approval(issued: &[Issued], name: &str, github: u64) -> Option<Grant> {
    latest(issued)
        .into_iter()
        .filter(|i| i.name == name && i.withdrawn.is_none() && i.github.as_ref().is_some_and(|g| g.id == github))
        .find_map(|i| i.grant.clone().filter(|grant| matches!(grant, Grant::Approval(_))))
}

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

/// The live keys in `dir` that `keys` were renewed from ([`Issued::renewed_from`]), and those they were renewed from in
/// turn, back to the first: what `ants auth revoke --key K --earlier` revokes besides K, so that no earlier
/// credentials of the holder stay valid.
pub fn renewed_from(dir: &Path, keys: &[String]) -> Result<Vec<String>> {
    let issued = issued(dir)?;
    let live: BTreeSet<String> = crate::signing::signer::live(dir)?.into_iter().map(|i| i.key).collect();
    let mut seen: BTreeSet<&str> = keys.iter().map(String::as_str).collect();
    let mut todo: Vec<&str> = keys.iter().map(String::as_str).collect();
    let mut earlier = Vec::new();
    while let Some(key) = todo.pop() {
        for from in issued.iter().filter(|i| i.key == key).filter_map(|i| i.renewed_from.as_deref()) {
            if seen.insert(from) {
                todo.push(from);
                if live.contains(from) {
                    earlier.push(from.to_owned());
                }
            }
        }
    }
    Ok(earlier)
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
    use crate::credentials::revoke::revoke;

    #[test]
    fn a_github_binding_and_its_grant_are_listed_with_the_device_and_kept_on_renewal() {
        use ants_daemon::config::registration::{GithubAccount, Grant};
        let dir = ants_test_support::scratch_dir("binding");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let alice: ants_proto::UserId = "alice".parse().unwrap();
        let new = auth::NewDevice {
            label: Some("Laptop"),
            github: Some(GithubAccount { id: 42, login: "octocat".into() }),
            grant: Some(Grant::Org("acme".into())),
            ..auth::NewDevice::default()
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

    /// `--earlier` follows a key back through the keys it was renewed from, live ones only, and none of another holder's.
    #[test]
    fn the_keys_a_key_was_renewed_from_are_its_live_predecessors() {
        let dir = ants_test_support::scratch_dir("renewed-from");
        let team = dir.join("auth");
        auth::init(&team).unwrap();
        let ant = |name: &str| {
            let out = dir.join(format!("{name}.creds"));
            crate::test_support::issue(&team, &name.parse().unwrap(), ants_proto::Role::Ant, None, &out).unwrap();
            auth::read_creds(&out).unwrap()
        };
        let renewed = |jwt: &str| {
            let key = nkeys::KeyPair::new_user();
            (auth::renew(&team, jwt, &key.public_key(), None).unwrap().jwt, key.public_key())
        };
        let (jwt, first) = ant("dev-a");
        let first = first.public_key();
        let (jwt, second) = renewed(&jwt);
        let (_, third) = renewed(&jwt);
        ant("dev-b");

        let mut earlier = renewed_from(&team, std::slice::from_ref(&third)).unwrap();
        earlier.sort();
        let mut expected = vec![first.clone(), second.clone()];
        expected.sort();
        assert_eq!(earlier, expected);
        assert!(renewed_from(&team, std::slice::from_ref(&first)).unwrap().is_empty());
        // What a renewal's hint revokes for the key it replaced reaches back, never forward to the new key.
        assert_eq!(renewed_from(&team, std::slice::from_ref(&second)).unwrap(), std::slice::from_ref(&first));
        revoke(&team, None, std::slice::from_ref(&first)).unwrap();
        assert_eq!(renewed_from(&team, std::slice::from_ref(&third)).unwrap(), [second]);

        let mut keys = vec![third];
        keys.extend(renewed_from(&team, &keys).unwrap());
        revoke(&team, None, &keys).unwrap();
        let live = crate::signing::signer::live(&team).unwrap();
        assert!(!live.iter().any(|i| i.name == "dev-a"), "{live:?}");
        assert!(live.iter().any(|i| i.name == "dev-b"));
        fs::remove_dir_all(dir).unwrap();
    }
}
