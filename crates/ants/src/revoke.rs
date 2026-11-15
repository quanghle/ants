//! Revoking issued credentials before they expire.
//!
//! Every user key the team directory issues or signs is recorded in [`ISSUED`], one JSON object per line, so that
//! `ants auth revoke NAME` finds all the keys of a name: a human may hold credentials on several devices, and
//! enrollment and re-issued credentials add more. Revocations live in [`REVOKED`]; the account JWT in `server.conf`
//! carries them as NATS revocations (a user key and a time: JWTs of that key issued at or before it are refused),
//! and [`push`] sends that account JWT to a running server through the system account, which closes the revoked
//! users' connections at once. The update reaches every leaf node connected to that server through a system account
//! uplink (`ants leaf config`), which applies it too and keeps it across restarts.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use ants_proto::{DeviceId, PERMISSIONS_VERSION, PermissionsVersion};
use anyhow::{Context as _, Result, bail};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};

use crate::auth;

/// The registry of issued user keys in the team directory.
pub const ISSUED: &str = "issued.jsonl";
/// The revocations in the team directory.
pub const REVOKED: &str = "revoked.json";

const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// What credentials an issued key is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    Ant,
    Human,
    Webhook,
    Leaf,
    ChannelManager,
    Enrollment,
    Observer,
}

impl From<ants_proto::Role> for Kind {
    fn from(role: ants_proto::Role) -> Self {
        match role {
            ants_proto::Role::Human => Self::Human,
            ants_proto::Role::Ant => Self::Ant,
        }
    }
}

/// One user key the team issued, a line of [`ISSUED`]. Fields added later are optional, so older lines still read.
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
    /// webhook's and so on, and for a human's issued before devices had ids.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
    /// The label of a human's device, for display: a later line of the same key relabels it ([`rename`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The permissions version its JWT carries ([`PERMISSIONS_VERSION`]); none for keys issued before versions were
    /// stamped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub perms: Option<PermissionsVersion>,
    /// The key of the credentials these renew (`ants renew`), of the same name, kind and device, which stays valid
    /// until it expires or is revoked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renewed_from: Option<String>,
    /// The members of a leaf node's uplink ([`crate::members`]); none for other credentials, and for an uplink issued
    /// before the registry kept them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<crate::members::Members>,
    /// The template of an ant a manager added (`ants signer`), whose `max_ants` the signer counts its live keys
    /// against; none for other credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// The manager ant that added an ant from `template`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub added_by: Option<ants_proto::UserId>,
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
            perms: Some(PERMISSIONS_VERSION),
            renewed_from: None,
            members: None,
            template: None,
            added_by: None,
        })
    }

    /// The key, for `device` labelled `label`.
    pub fn for_device(self, device: DeviceId, label: Option<String>) -> Self {
        Self { device: Some(device), label, ..self }
    }
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
    /// None for a legacy device, whose credentials carry no id.
    pub device: Option<DeviceId>,
    pub label: Option<String>,
    pub key: String,
    pub issued_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    pub revoked: bool,
}

/// The devices of the human `name` in the registry of `dir`, oldest first: one per key issued to it as a human, but one
/// per device id, with its latest key, for a device whose credentials were renewed (`ants renew`).
pub fn devices(dir: &Path, name: &str) -> Result<Vec<Device>> {
    let revoked = revoked(dir)?;
    let mut found: Vec<Device> = Vec::new();
    for one in issued(dir)?.into_iter().filter(|i| i.name == name && i.kind == Kind::Human) {
        let device = Device {
            device: one.device,
            label: one.label,
            revoked: revoked.iter().any(|r| r.key == one.key),
            key: one.key,
            issued_at: one.issued_at,
            expires_at: one.expires_at,
        };
        let same = |d: &Device| d.key == device.key || (device.device.is_some() && d.device == device.device);
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
    let by_id_or_key =
        devices.iter().find(|d| d.key == selector || d.device.is_some_and(|id| id.to_string() == selector));
    if let Some(device) = by_id_or_key {
        return Ok(device);
    }
    let labelled: Vec<&Device> = devices.iter().filter(|d| d.label.as_deref() == Some(selector)).collect();
    match labelled.as_slice() {
        [device] => Ok(device),
        [] => bail!("{name} has no device {selector:?}; `ants auth devices {name}` lists them"),
        several => {
            let ids: Vec<String> = several
                .iter()
                .map(|d| d.device.map_or_else(|| format!("legacy device {}", d.key), |id| id.to_string()))
                .collect();
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
        let of_device = |i: &&Issued| {
            i.name == name && i.kind == Kind::Human && device.device.is_some() && i.device == device.device
        };
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

/// Print the devices of the human `name` in `dir`: id (`legacy` for one without), label, key, when issued, and
/// whether revoked; with `json`, one JSON object per line.
pub fn list_devices(dir: &Path, name: &str, json: bool) -> Result<()> {
    let devices = devices(dir, name)?;
    if devices.is_empty() && !json {
        println!("no devices of {name} known in {}", dir.display());
    }
    for d in devices {
        if json {
            println!("{}", serde_json::to_string(&d)?);
            continue;
        }
        let id = d.device.map_or_else(|| "legacy".to_owned(), |id| id.to_string());
        let label = d.label.as_deref().unwrap_or("-");
        let revoked = if d.revoked { "  revoked" } else { "" };
        println!("{id}  {label}  {}  issued {}{revoked}", d.key, time(d.issued_at));
    }
    Ok(())
}

/// One revoked user key, an entry of [`REVOKED`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revoked {
    pub key: String,
    /// Seconds since the epoch: JWTs of the key issued at or before it are refused.
    pub revoked_at: u64,
    /// The name its credentials carry, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// When the latest credentials known for the key expire; none when they never do, or are unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

impl Revoked {
    /// Whether every known credential of the key has expired at `now`, so the revocation is no longer needed.
    fn expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now)
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

/// The revocations of `dir`, oldest first.
pub fn revoked(dir: &Path) -> Result<Vec<Revoked>> {
    let path = dir.join(REVOKED);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| format!("reading {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn write_revoked(dir: &Path, revoked: &[Revoked]) -> Result<()> {
    let path = dir.join(REVOKED);
    let mut text = serde_json::to_string_pretty(revoked)?;
    text.push('\n');
    fs::write(&path, text).with_context(|| format!("writing {}", path.display()))
}

/// Revoke every key known for `name`, in the registry or in a `.creds` file of `dir` the team's account signed, and
/// each of `keys`, then write `server.conf` again ([`apply`]). Returns the keys revoked, some perhaps already expired;
/// fails without a change when `name` has no known key.
pub fn revoke(dir: &Path, name: Option<&str>, keys: &[String]) -> Result<Vec<Revoked>> {
    let now = auth::now()?;
    let issued = issued(dir)?;
    let mut targets: Vec<Revoked> = Vec::new();
    let add = |targets: &mut Vec<Revoked>, key: &str, name: Option<&str>, expires_at: Option<u64>| {
        match targets.iter_mut().find(|t| t.key == key) {
            // Of several credentials of one key, the revocation lasts until the last of them expires.
            Some(known) => known.expires_at = known.expires_at.zip(expires_at).map(|(a, b)| a.max(b)),
            None => targets.push(Revoked {
                key: key.to_owned(),
                revoked_at: now,
                name: name.map(str::to_owned),
                expires_at,
            }),
        }
    };
    if let Some(name) = name {
        for one in issued.iter().filter(|i| i.name == name) {
            add(&mut targets, &one.key, Some(name), one.expires_at);
        }
        for claims in team_creds(dir)?.into_iter().filter(|c| c.name == name) {
            add(&mut targets, &claims.sub, Some(name), claims.exp);
        }
        if targets.is_empty() {
            bail!(
                "no credentials of {name} known in {} ({ISSUED}, or a .creds file there); revoke them by public key \
                 with --key U...",
                dir.display()
            );
        }
    }
    for key in keys {
        auth::check_user_key(key)?;
        let known: Vec<&Issued> = issued.iter().filter(|i| i.key == *key).collect();
        match known.as_slice() {
            [] => add(&mut targets, key, None, None),
            known => {
                for one in known {
                    add(&mut targets, key, Some(&one.name), one.expires_at);
                }
            }
        }
    }
    let mut all = revoked(dir)?;
    all.retain(|r| !targets.iter().any(|t| t.key == r.key));
    all.extend(targets.iter().cloned());
    write_revoked(dir, &all)?;
    apply(dir)?;
    Ok(targets)
}

/// Drop the revocations whose credentials have all expired, and write the `server.conf` of `dir` with the rest.
/// Returns the account's public key and JWT.
pub fn apply(dir: &Path) -> Result<(String, String)> {
    let now = auth::now()?;
    let mut all = revoked(dir)?;
    let before = all.len();
    all.retain(|r| !r.expired(now));
    if all.len() != before {
        write_revoked(dir, &all)?;
    }
    let revocations: BTreeMap<String, u64> = all.into_iter().map(|r| (r.key, r.revoked_at)).collect();
    auth::write_server_conf(dir, &revocations)
}

/// The claims of the `.creds` files in `dir` that the team's account signed.
fn team_creds(dir: &Path) -> Result<Vec<auth::Claims>> {
    let account = auth::account_key(dir)?;
    let entries = fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))?;
    let mut found = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().is_none_or(|ext| ext != "creds") {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else { continue };
        let claims = auth::creds_jwt(&text).and_then(|jwt| auth::verified(jwt).ok());
        found.extend(claims.filter(|c| c.iss == account));
    }
    Ok(found)
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
    ants_daemon::creds::time(secs)
}

/// One key of the registry, as `ants auth issued` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listed {
    /// Its latest line in [`ISSUED`].
    #[serde(flatten)]
    pub issued: Issued,
    pub revoked: bool,
    /// A later key was issued for the same name and kind; for a human's, only a later key of the same device (renewed
    /// credentials), as each other one is another device.
    pub reissued: bool,
    /// Its permissions version is not [`PERMISSIONS_VERSION`].
    pub outdated: bool,
    /// It expires within [`ants_daemon::creds::EXPIRY_WARNING`].
    pub expiring: bool,
    /// The commands that renew it, when it is in use (neither revoked, reissued, expired nor an enrollment's) and
    /// outdated or expiring: from its `.creds` file in the team directory when there is one
    /// ([`ants_daemon::creds::Finding::renew`]), else with placeholders for what only that file tells.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub renew: Vec<String>,
}

/// The keys of the registry of `dir` at `now`, oldest first, each once.
pub fn listing(dir: &Path, now: u64) -> Result<Vec<Listed>> {
    let revoked = revoked(dir)?;
    let mut latest: Vec<Issued> = Vec::new();
    for one in issued(dir)? {
        match latest.iter_mut().find(|known| known.key == one.key) {
            Some(known) => *known = Issued { issued_at: known.issued_at, ..one },
            None => latest.push(one),
        }
    }
    let files = team_findings(dir)?;
    let mut listed = Vec::new();
    for (n, one) in latest.iter().enumerate() {
        let device = |later: &Issued| one.kind != Kind::Human || (one.device.is_some() && later.device == one.device);
        let reissued =
            latest[n + 1..].iter().any(|later| later.name == one.name && later.kind == one.kind && device(later));
        let revoked = revoked.iter().any(|r| r.key == one.key);
        let expired = one.expires_at.is_some_and(|exp| exp <= now);
        let expiring =
            !expired && one.expires_at.is_some_and(|exp| exp - now <= ants_daemon::creds::EXPIRY_WARNING.as_secs());
        let outdated = one.perms != Some(PERMISSIONS_VERSION);
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

/// The command renewing `issued` when no `.creds` file tells more: placeholders stand for a webhook's destinations,
/// a leaf node's members when the registry does not keep them, and the hub's JetStream domain.
fn renew_without_file(issued: &Issued) -> Vec<String> {
    let name = &issued.name;
    let domain = "[--js-domain DOMAIN]";
    vec![match issued.kind {
        Kind::Ant => format!("ants auth user {name} {domain}"),
        Kind::Human => format!("ants auth user {name} --human {domain}"),
        Kind::Webhook => format!("ants auth user {name} --webhook --to ANT... --channel CHANNEL... {domain}"),
        Kind::Leaf if issued.members.is_some() => {
            format!("ants auth leaf {} {domain}", name.trim_prefix("leaf-"))
        }
        Kind::Leaf => format!(
            "ants auth leaf {} --ant ANT... --human HUMAN... --webhook WEBHOOK... {domain}",
            name.trim_prefix("leaf-")
        ),
        Kind::ChannelManager => format!("ants auth channel-manager {domain}"),
        Kind::Observer => format!("ants auth user {name} --observer {domain}"),
        Kind::Enrollment => format!("ants auth enroll {}", name.trim_prefix("enroll-")),
    }]
}

/// What [`ants_daemon::creds`] finds in the `.creds` files in `dir` that the team's account signed.
fn team_findings(dir: &Path) -> Result<Vec<ants_daemon::creds::Finding>> {
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
            && let Ok(finding) = ants_daemon::creds::Finding::from_jwt(jwt)
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
        let kind = serde_json::to_value(i.kind)?.as_str().unwrap_or("?").to_owned();
        let expiry = match i.expires_at {
            Some(exp) if exp <= now => format!("expired {}", time(exp)),
            Some(exp) => format!("expires {}", time(exp)),
            None => "no expiry".to_owned(),
        };
        let perms = i.perms.map_or_else(|| "-".to_owned(), |v| v.to_string());
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

/// The inbox prefix of [`push`], where the leaf nodes' system account uplinks may answer it.
pub const PUSH_INBOX: &str = "_INBOX.ants-push";

/// How long [`push`] waits for more servers to answer once the one it pushed to has.
const PUSH_SETTLE: Duration = Duration::from_secs(1);

/// The servers that applied a pushed account.
#[derive(Debug)]
pub struct Pushed {
    /// The server pushed to.
    pub server: String,
    /// Other servers that answered, mostly leaf nodes connected to it through a system account uplink, each with the
    /// error it gave, if any.
    pub others: Vec<(String, Option<String>)>,
    /// Whether the server pushed to keeps the accounts pushed to it across restarts ([`keeps_accounts`]); none when
    /// it did not tell.
    pub kept: Option<bool>,
}

/// Send the team's account JWT, with its revocations, to the server at `url`, as a user of the system account signed
/// with its key in `dir`; the server, and every leaf node connected to it through a system account uplink, closes the
/// connections of users revoked since. Fails unless the server applies it.
pub async fn push(url: &str, dir: &Path) -> Result<Pushed> {
    ants_daemon::bus::check_url(url)?;
    let (account, jwt) = apply(dir)?;
    let client = connect_system(url, dir).await?;
    let server = client.server_info().server_name;
    let inbox = client.new_inbox();
    let mut answers = client.subscribe(inbox.clone()).await?;
    let subject = format!("$SYS.REQ.ACCOUNT.{account}.CLAIMS.UPDATE");
    client.publish_with_reply(subject, inbox, jwt.into()).await.context("asking the server to update the account")?;
    client.flush().await?;
    let mut until = tokio::time::Instant::now() + PUSH_TIMEOUT;
    let (mut pushed, mut others) = (None, Vec::new());
    while let Ok(Some(message)) = tokio::time::timeout_at(until, answers.next()).await {
        let (name, outcome) = answer(&message.payload);
        if name == server && pushed.is_none() {
            pushed = Some(outcome);
            until = tokio::time::Instant::now() + PUSH_SETTLE;
        } else {
            others.push((name, outcome));
        }
    }
    match pushed {
        None => bail!("no answer from server {server}"),
        Some(Some(error)) => bail!("server {server}: {error}"),
        Some(None) => Ok(Pushed { kept: keeps_accounts(&client).await, server, others }),
    }
}

/// A connection to the server at `url` as a short-lived user of the team's system account, signed with its key in
/// `dir`. A refusal leaves an [`async_nats::ConnectError`] in the error's chain.
pub async fn connect_system(url: &str, dir: &Path) -> Result<async_nats::Client> {
    ants_daemon::bus::check_url(url)?;
    let creds = auth::system_creds(dir)?;
    let connect = async_nats::ConnectOptions::with_credentials(&creds)?
        .name("ants-auth")
        .custom_inbox_prefix(PUSH_INBOX)
        .connection_timeout(PUSH_TIMEOUT)
        .connect(url);
    let client = tokio::time::timeout(PUSH_TIMEOUT, connect).await.context("no answer from the server")?;
    client.context("connecting to NATS")
}

/// Whether the server `client` is connected to, as a system account user, keeps the accounts pushed to it across
/// restarts: only a `full` resolver answers `$SYS.REQ.CLAIMS.LIST`, which no leaf node's system account uplink may
/// hear. The `MEMORY` resolver of `server.conf` has none answer it, so a restart brings back the account of the
/// server's `server.conf`. None without an answer of that server.
pub async fn keeps_accounts(client: &async_nats::Client) -> Option<bool> {
    let server = client.server_info().server_name;
    match client.request(LIST_REQUEST, "".into()).await {
        Ok(answer) => {
            let answer: serde_json::Value = serde_json::from_slice(&answer.payload).ok()?;
            (answer["server"]["name"] == server.as_str() && answer["data"].is_array()).then_some(true)
        }
        Err(e) if e.kind() == async_nats::RequestErrorKind::NoResponders => Some(false),
        Err(_) => None,
    }
}

const LIST_REQUEST: &str = "$SYS.REQ.CLAIMS.LIST";

/// The server answering an account update, and the error it gave, if any.
fn answer(payload: &[u8]) -> (String, Option<String>) {
    let Ok(answer) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return ("?".to_owned(), Some("unreadable answer".to_owned()));
    };
    let server = answer["server"]["name"].as_str().unwrap_or("?").to_owned();
    let error = match (answer["error"]["description"].as_str(), answer["data"]["message"].as_str()) {
        (Some(description), _) => Some(description.to_owned()),
        (None, Some("jwt updated")) => None,
        (None, Some(message)) => Some(format!("{message} (the account is not loaded there)")),
        (None, None) => Some(format!("unexpected answer {answer}")),
    };
    (server, error)
}

#[cfg(test)]
mod tests;
