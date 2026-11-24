//! The registry of the user keys the team issued.
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
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};

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
    /// The label of a human's device, for display: a later line of the same key relabels it ([`super::devices::rename`]).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::revoke::revoke;

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
