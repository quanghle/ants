//! Team credentials for NATS decentralized auth: one operator, one account holding the ants stream, and one user per
//! ant or human whose JWT carries the permissions of [`Permissions::for_user`] in its role.
//!
//! The operator and account seeds stay in the team directory on the machine that issues credentials. The server
//! needs only `server.conf`; each machine gets the `.creds` file of the ants it runs, and its leaf node the uplink
//! credentials of [`issue_leaf`].
//!
//! When the hub has a JetStream domain, credentials are issued for it: they allow the JetStream API as
//! `$JS.<domain>.API`, the prefix every ants process then uses, besides the plain `$JS.API` the hub maps it to
//! ([`Permissions::with_js_domain`]).

use std::collections::BTreeMap;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ants_daemon::config::registration::{GithubAccount, Grant};
use ants_daemon::creds::{Finding, Kind as CredsKind, Seed};
use ants_proto::{Destination, DeviceId, DeviceKey, JsDomain, Machine, PERMISSIONS_VERSION, Permissions, Role, UserId};
use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use nkeys::{KeyPair, XKey};
use serde_json::{Value, json};

use crate::members::EnrollApprover;
use crate::revoke::{self, Issued, Kind};

pub(crate) const OPERATOR_SEED: &str = "operator.nk";
pub(crate) const ACCOUNT_SEED: &str = "account.nk";
pub(crate) const SYSTEM_SEED: &str = "system.nk";
pub const SERVER_CONF: &str = "server.conf";
const TEAM: &str = "ants";
/// `max_control_line` of the servers that include [`SERVER_CONF`], hub and leaf nodes alike.
const MAX_CONTROL_LINE: u32 = 16 * 1024;
/// The longest uplink user JWT of a leaf node ([`issue_leaf`]): nats-server closes a leaf node connection whose
/// CONNECT line is longer than 16 times the hub's `max_control_line`, and the line holds the JWT's signature and the
/// leaf's names besides.
const MAX_UPLINK_JWT: usize = 16 * MAX_CONTROL_LINE as usize - 4 * 1024;

/// The default team directory, `~/.config/ants/auth`.
pub fn default_dir() -> Result<PathBuf> {
    Ok(std::env::home_dir().context("no home directory")?.join(".config/ants/auth"))
}

/// Create the operator and account in `dir` and write the `server.conf` that trusts them.
pub fn init(dir: &Path) -> Result<()> {
    if dir.join(ACCOUNT_SEED).exists() {
        bail!("{} already holds a team", dir.display());
    }
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let operator = KeyPair::new_operator();
    let account = KeyPair::new_account();
    let system = KeyPair::new_account();
    write_secret(&dir.join(OPERATOR_SEED), &seed(&operator)?)?;
    write_secret(&dir.join(SYSTEM_SEED), &seed(&system)?)?;
    write_secret(&dir.join(ACCOUNT_SEED), &seed(&account)?)?;
    fs::write(dir.join(SERVER_CONF), server_conf(&operator, None, &system, &account, &BTreeMap::new())?)?;
    Ok(())
}

/// Write the `server.conf` of the team in `dir` again, its account revoking each user key of `revocations` issued
/// at or before its time (seconds since the epoch). Returns the account's public key and JWT.
pub fn write_server_conf(dir: &Path, revocations: &BTreeMap<String, u64>) -> Result<(String, String)> {
    let operator = load(&dir.join(OPERATOR_SEED))?;
    let system = load(&dir.join(SYSTEM_SEED))?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let path = dir.join(SERVER_CONF);
    // nats-server reloads no change of the trusted operator, so the operator JWT already there stays.
    let earlier = fs::read_to_string(&path).unwrap_or_default();
    let operator_jwt = earlier.lines().find_map(|line| line.strip_prefix("operator: ")).filter(|jwt| {
        verified(jwt).is_ok_and(|claims| claims.sub == operator.public_key() && claims.iss == operator.public_key())
    });
    fs::write(&path, server_conf(&operator, operator_jwt, &system, &account, revocations)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok((account.public_key(), account_jwt(&operator, &account, revocations)?))
}

/// Whether `dir` holds the keys of a team, those of its system account included.
pub fn holds_team(dir: &Path) -> bool {
    dir.join(SYSTEM_SEED).is_file()
}

/// The public key of the team's account in `dir`.
pub fn account_key(dir: &Path) -> Result<String> {
    Ok(load(&dir.join(ACCOUNT_SEED))?.public_key())
}

/// The text of short-lived credentials of the team's system account, for `ants auth push` to update the account on a
/// server.
pub fn system_creds(dir: &Path) -> Result<String> {
    let system = load(&dir.join(SYSTEM_SEED))?;
    let user = KeyPair::new_user();
    let exp = now()? + APPROVER_TTL.as_secs();
    let all = Permissions { publish: vec![">".to_owned()], subscribe: vec![">".to_owned()] };
    let jwt = user_jwt(&system, &user.public_key(), "ants-auth", all, json!({}), Some(exp))?;
    Ok(creds(&jwt, &seed(&user)?))
}

/// The role `ants auth user` issues: a human's with `--human`, and for the default human whatever the flag; else an
/// ant's.
pub fn role(user: &UserId, human: bool) -> Role {
    if human { Role::Human } else { Role::by_default(user) }
}

/// The ants and humans served on a machine, each in its role, without repeats: a name given as both is a human.
pub fn members(ants: &[UserId], humans: &[UserId]) -> Vec<(UserId, Role)> {
    let mut members: Vec<(UserId, Role)> = humans.iter().map(|h| (h.clone(), Role::Human)).collect();
    for ant in ants {
        if !members.iter().any(|(m, _)| m == ant) {
            members.push((ant.clone(), Role::by_default(ant)));
        }
    }
    members
}

/// Fail when `user` is a name the team reserves: [`UserId::channel_manager`], which no ant or human may take.
pub fn check_not_reserved(user: &UserId) -> Result<()> {
    if *user == UserId::channel_manager() {
        bail!("{user} is reserved for the channel manager (`ants auth channel-manager`); choose another name");
    }
    Ok(())
}

/// Issue a new NATS user for `user`, an ant or a human, in `role`, for the hub's JetStream domain if it has one, and
/// write its `.creds` file to `out`. Earlier credentials stay valid.
#[cfg(test)]
pub fn issue(dir: &Path, user: &UserId, role: Role, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    issue_with(dir, user, role.into(), Permissions::for_user(user, role).with_js_domain(js_domain), out)
}

/// Issue a new NATS user for `user`, of `kind`, with `permissions`, write its `.creds` file to `out` and record its
/// key ([`revoke::record`]). A human's are for a new device ([`issue_device`]).
#[cfg(test)]
pub fn issue_with(dir: &Path, user: &UserId, kind: Kind, permissions: Permissions, out: &Path) -> Result<()> {
    issue_device(dir, user, kind, permissions, None, out).map(drop)
}

/// [`issue_with`], with a human's credentials for a new device labelled `label`, whose id and new encryption key their
/// user JWT carries ([`DeviceId::tag`], [`DeviceKey::tag`]) and the registry records, the key's seed in the file after
/// the NKey seed. Returns the device id, for a human's.
pub fn issue_device(
    dir: &Path,
    user: &UserId,
    kind: Kind,
    permissions: Permissions,
    label: Option<&str>,
    out: &Path,
) -> Result<Option<DeviceId>> {
    issue_device_with(dir, user, kind, permissions, &NewDevice { label, ..NewDevice::default() }, out)
}

/// What the registry records of a human's new device besides its key, and when its credentials expire.
#[derive(Debug, Clone, Default)]
pub struct NewDevice<'a> {
    pub label: Option<&'a str>,
    /// The GitHub user the human's name is bound to ([`crate::binding`]).
    pub github: Option<GithubAccount>,
    /// Why `ants signer` signed it.
    pub grant: Option<Grant>,
    /// Seconds since the epoch; none for credentials that never expire.
    pub expires_at: Option<u64>,
}

impl NewDevice<'_> {
    fn record(&self, issued: Issued, device: Option<DeviceId>, xkey: Option<DeviceKey>) -> Issued {
        let issued = with_device(issued, device, self.label, xkey);
        Issued { github: self.github.clone(), grant: self.grant.clone(), ..issued }
    }
}

/// [`issue_device`], recording `new` with the device.
pub fn issue_device_with(
    dir: &Path,
    user: &UserId,
    kind: Kind,
    permissions: Permissions,
    new: &NewDevice<'_>,
    out: &Path,
) -> Result<Option<DeviceId>> {
    check_not_reserved(user)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let key = KeyPair::new_user();
    let xkey = (kind == Kind::Human).then(XKey::new);
    let bound = xkey.as_ref().map(device_key).transpose()?;
    let (device, extra) = device_claims(kind, new.label, bound.as_ref())?;
    let jwt = user_jwt(&account, &key.public_key(), user.as_str(), permissions, extra, new.expires_at)?;
    write_secret(out, &creds_with(&jwt, &seed(&key)?, xkey.as_ref())?)?;
    let issued = Issued::new(&key.public_key(), user.as_str(), kind, new.expires_at)?;
    revoke::record(dir, &new.record(issued, device, bound))?;
    Ok(device)
}

/// The public key of `xkey`, as a user JWT binds it.
pub(crate) fn device_key(xkey: &XKey) -> Result<DeviceKey> {
    Ok(xkey.public_key().parse()?)
}

/// A new device for credentials of `kind`, if a human's, and the claims carrying it with its encryption key `xkey`.
fn device_claims(kind: Kind, label: Option<&str>, xkey: Option<&DeviceKey>) -> Result<(Option<DeviceId>, Value)> {
    if kind != Kind::Human {
        if label.is_some() {
            bail!("only a human's credentials are for a device with a label");
        }
        if xkey.is_some() {
            bail!("only a human's credentials bind an encryption key");
        }
        return Ok((None, json!({})));
    }
    label.map(revoke::check_label).transpose()?;
    let device = DeviceId::generate();
    let tags: Vec<String> = std::iter::once(device.tag()).chain(xkey.map(DeviceKey::tag)).collect();
    Ok((Some(device), json!({ "tags": tags })))
}

fn with_device(issued: Issued, device: Option<DeviceId>, label: Option<&str>, xkey: Option<DeviceKey>) -> Issued {
    match device {
        Some(device) => issued.for_device(device, label.map(str::to_owned), xkey),
        None => issued,
    }
}

/// Issue the uplink user of `machine`'s leaf node and write its `.creds` file to `out`. It may carry only what
/// `members`, ants and humans each in its role ([`members`]), may do and the sourcing of the machine's outbox
/// ([`Permissions::for_leaf`]), plus what the credentials of `webhooks` allow, for an `ants webhook serve` on the
/// machine ([`webhook_permissions`]), plus [`EnrollApprover::With`] what `ants enroll` and `ants signer` on the machine
/// need ([`Permissions::for_enroll_approver`], [`Permissions::for_reactions_responder`]), and connect only as a leaf
/// node, not as a client. Also writes the system account credentials of the leaf's second uplink beside them
/// ([`leaf_system_creds`]).
///
/// nats-server applies a leaf user's permissions from the leaf's side, as for a client: publish limits what the leaf
/// sends to the hub, subscribe what the hub sends to the leaf (the hub swaps them internally).
pub fn issue_leaf(
    dir: &Path,
    machine: &Machine,
    members: &[(UserId, Role)],
    webhooks: &[UserId],
    approver: EnrollApprover,
    js_domain: Option<&JsDomain>,
    out: &Path,
) -> Result<()> {
    if members.is_empty() && webhooks.is_empty() {
        bail!("a leaf node needs at least one ant, human or webhook");
    }
    members.iter().map(|(member, _)| member).chain(webhooks).try_for_each(check_not_reserved)?;
    if js_domain.is_some_and(|domain| domain.as_str() == machine.as_str()) {
        bail!("machine {machine} would take the hub's JetStream domain; name it otherwise");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let roles = members.iter().map(|(member, role)| (member, *role));
    let mut permissions = Permissions::for_leaf(machine, roles);
    for webhook in webhooks {
        merge(&mut permissions, webhook_permissions(dir, webhook)?);
    }
    if approver == EnrollApprover::With {
        merge(&mut permissions, signer_permissions());
    }
    let mut permissions = permissions.with_js_domain(js_domain);
    dedup(&mut permissions);
    let only_leaf = json!({ "allowed_connection_types": LEAF_CONNECTIONS });
    let name = leaf_user(machine);
    let jwt = user_jwt(&account, &user.public_key(), &name, permissions, only_leaf, None)?;
    check_uplink_size(&jwt, machine)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))?;
    let issued = Issued::new(&user.public_key(), &name, Kind::Leaf, None)?;
    let members = crate::members::Members::new(members, webhooks, approver);
    revoke::record(dir, &Issued { members: Some(members), ..issued })?;
    issue_leaf_system(dir, machine, &account.public_key(), &leaf_system_creds(out))
}

/// Where [`issue_leaf`] writes the system account credentials of a leaf node whose uplink credentials are at
/// `uplink`: beside them, `leaf-<machine>.system.creds` for `leaf-<machine>.creds`.
pub fn leaf_system_creds(uplink: &Path) -> PathBuf {
    uplink.with_extension("system.creds")
}

/// Write to `out` the credentials of the system account for the second uplink of `machine`'s leaf node, through which
/// the account updates `ants auth push` sends the hub reach the leaf: they hear those of `account` only, answer only
/// the push ([`revoke::PUSH_INBOX`]), and connect only as a leaf node. They also let the leaf's `full` resolver ask
/// the hub's for the accounts it holds ([`PACK_REQUEST`]) and hear the answers on the leaf's own system inbox only
/// ([`system_inbox`]), which catches a leaf up on the pushes it missed while offline.
fn issue_leaf_system(dir: &Path, machine: &Machine, account: &str, out: &Path) -> Result<()> {
    let system = load(&dir.join(SYSTEM_SEED))?;
    let user = KeyPair::new_user();
    let permissions = Permissions {
        publish: vec![format!("{}.>", revoke::PUSH_INBOX), PACK_REQUEST.to_owned()],
        subscribe: vec![format!("$SYS.REQ.ACCOUNT.{account}.CLAIMS.UPDATE"), format!("{}.*", system_inbox(machine))],
    };
    let only_leaf = json!({ "allowed_connection_types": LEAF_CONNECTIONS });
    let jwt = user_jwt(&system, &user.public_key(), &leaf_user(machine), permissions, only_leaf, None)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))
}

/// What a `full` resolver periodically sends the other servers of its system account, with the hash of the accounts
/// it holds; those with a `full` resolver too answer with theirs when they differ, and it keeps the newer ones.
const PACK_REQUEST: &str = "$SYS.REQ.CLAIMS.PACK";

/// The inbox prefix of the system requests of `machine`'s leaf node, named after the machine (`ants leaf config`):
/// nats-server takes `$SYS._INBOX.` and 8 base-62 digits of the SHA-256 of its server name.
fn system_inbox(machine: &Machine) -> String {
    const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let sha = ring::digest::digest(&ring::digest::SHA256, machine.as_str().as_bytes());
    let hash: String = sha.as_ref()[..8].iter().map(|b| char::from(DIGITS[usize::from(b % 62)])).collect();
    format!("$SYS._INBOX.{hash}")
}

const LEAF_CONNECTIONS: [&str; 2] = ["LEAFNODE", "LEAFNODE_WS"];

/// The `name` of the uplink user of `machine`'s leaf node.
pub fn leaf_user(machine: &Machine) -> String {
    format!("leaf-{machine}")
}

/// Refuse `jwt`, uplink credentials of `machine`'s leaf node, if the hub would not take it in the leaf's CONNECT.
/// Each ant adds about 4.7 KB to them, 8.7 KB with a JetStream domain.
fn check_uplink_size(jwt: &str, machine: &Machine) -> Result<()> {
    if jwt.len() > MAX_UPLINK_JWT {
        bail!(
            "the uplink credentials of {machine} would take {} bytes, over the {MAX_UPLINK_JWT} a hub takes in a leaf \
             node's CONNECT (16 times max_control_line {MAX_CONTROL_LINE}, less room for the rest of the line): run \
             some of its ants, humans or webhooks behind another leaf node, under another machine name",
            jwt.len()
        );
    }
    Ok(())
}

/// Add `more` to `permissions`, keeping each subject once, in the order first seen.
fn merge(permissions: &mut Permissions, more: Permissions) {
    permissions.publish.extend(more.publish);
    permissions.subscribe.extend(more.subscribe);
    dedup(permissions);
}

fn dedup(permissions: &mut Permissions) {
    for all in [&mut permissions.publish, &mut permissions.subscribe] {
        let mut seen = std::collections::HashSet::new();
        all.retain(|s| seen.insert(s.clone()));
    }
}

/// The publish and subscribe allow lists of a user JWT's claims.
fn allowed(claims: &Value) -> Permissions {
    let list = |kind: &str| -> Vec<String> {
        let list = claims["nats"][kind]["allow"].as_array().map_or_default(Vec::as_slice);
        list.iter().filter_map(|s| s.as_str().map(str::to_owned)).collect()
    };
    Permissions { publish: list("pub"), subscribe: list("sub") }
}

/// Check that `jwt` is the uplink user JWT of `machine`'s leaf node ([`issue_leaf`]), issued by `account` and not
/// expired; returns its claims. Only a leaf node may connect with it, so an ant or human named `leaf-<machine>` does
/// not pass.
pub fn check_leaf(account: &str, jwt: &str, machine: &Machine) -> Result<Claims> {
    leaf_claims(account, jwt, machine).map(|(claims, _)| claims)
}

fn leaf_claims(account: &str, jwt: &str, machine: &Machine) -> Result<(Claims, Value)> {
    let (claims, body) = verified_body(jwt)?;
    if claims.iss != account {
        bail!("issued by {}, not by the team's account {account}", claims.iss);
    }
    if claims.name != leaf_user(machine) {
        bail!("issued to {}, not to the leaf node of {machine}", claims.name);
    }
    let types = body["nats"]["allowed_connection_types"].as_array().map_or_default(Vec::as_slice);
    if types.is_empty() || !types.iter().all(|t| t.as_str().is_some_and(|t| LEAF_CONNECTIONS.contains(&t))) {
        bail!("not the uplink credentials of a leaf node");
    }
    if claims.exp.is_some_and(|exp| exp <= now().unwrap_or(u64::MAX)) {
        bail!("expired");
    }
    Ok((claims, body))
}

/// Renew `jwt`, the uplink user JWT of `machine`'s leaf node issued from the account key in `dir`, for the same key,
/// allowing `more` besides what it allows. Everything else in it stays as it is, its permissions version
/// ([`PERMISSIONS_VERSION`]) too: the rights of its other users are still those of that version. Refused if the hub
/// would not take the renewed JWT ([`MAX_UPLINK_JWT`]).
pub fn extend_leaf(dir: &Path, jwt: &str, machine: &Machine, more: Permissions) -> Result<String> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let (claims, body) = leaf_claims(&account.public_key(), jwt, machine)
        .with_context(|| format!("the uplink credentials of {machine}"))?;
    // A JWT signed now would outlive a revocation of its key, which covers only JWTs issued before it.
    if revoke::revoked(dir)?.iter().any(|r| r.key == claims.sub) {
        bail!("the uplink credentials of {machine} are revoked");
    }
    let mut permissions = allowed(&body);
    merge(&mut permissions, more);
    let mut nats = body["nats"].clone();
    nats["pub"]["allow"] = json!(permissions.publish);
    nats["sub"]["allow"] = json!(permissions.subscribe);
    let renewed = self::jwt(&account, &claims.sub, &claims.name, nats, claims.exp)?;
    check_uplink_size(&renewed, machine)?;
    Ok(renewed)
}

/// Whether the user JWT `jwt` is for the key of `other` and allows everything `other` allows.
pub fn allows_all(jwt: &str, other: &str) -> Result<bool> {
    let ((claims, body), (other_claims, other_body)) = (verified_body(jwt)?, verified_body(other)?);
    let (have, need) = (allowed(&body), allowed(&other_body));
    let covers = |have: &[String], need: &[String]| need.iter().all(|s| have.contains(s));
    Ok(claims.sub == other_claims.sub
        && covers(&have.publish, &need.publish)
        && covers(&have.subscribe, &need.subscribe))
}

/// The user JWT and key of the `.creds` file at `path`, whose seed is in the file or in the OS credential store.
pub fn read_creds(path: &Path) -> Result<(String, KeyPair)> {
    ants_daemon::creds::load(path, ants_daemon::keystore::os())
}

/// The signature of `data` by `key`, base64url without padding.
pub fn sign(key: &KeyPair, data: &[u8]) -> Result<String> {
    Ok(BASE64URL_NOPAD.encode(&key.sign(data).map_err(|e| anyhow!("signing: {e}"))?))
}

/// Check that `signature`, from [`sign`], is the signature of `data` by the key `public_key`.
pub fn check_signature(public_key: &str, data: &[u8], signature: &str) -> Result<()> {
    let key = KeyPair::from_public_key(public_key).map_err(|e| anyhow!("public key {public_key}: {e}"))?;
    let signature = BASE64URL_NOPAD.decode(signature.as_bytes()).context("a signature that is not base64url")?;
    key.verify(data, &signature).map_err(|_| anyhow!("not signed by {public_key}"))
}

/// The permissions of the credentials of `webhook` in `dir`, `<webhook>.creds` as `ants auth user WEBHOOK --webhook`
/// writes them ([`Permissions::for_webhook`]): issued by the team's account, for `webhook`, hearing only its own reply
/// inboxes as a webhook's do.
pub fn webhook_permissions(dir: &Path, webhook: &UserId) -> Result<Permissions> {
    let path = dir.join(format!("{webhook}.creds"));
    let text = fs::read_to_string(&path).with_context(|| {
        format!(
            "reading {} (issue them first: `ants auth user {webhook} --webhook --to ANT` or `--channel CHANNEL`)",
            path.display()
        )
    })?;
    let jwt = creds_jwt(&text).with_context(|| format!("{}: no user JWT", path.display()))?;
    let (claims, body) = verified_body(jwt).with_context(|| path.display().to_string())?;
    if claims.iss != load(&dir.join(ACCOUNT_SEED))?.public_key() {
        bail!("{} were not issued by the team in {}", path.display(), dir.display());
    }
    if claims.name != webhook.as_str() {
        bail!("{} are {}'s, not {webhook}'s", path.display(), claims.name);
    }
    let permissions = allowed(&body);
    let (inbox, renewal) =
        (format!("{}.>", ants_proto::inbox_prefix(webhook)), ants_proto::subject::renew_reply(webhook.as_str()));
    if !permissions.subscribe.contains(&inbox) || permissions.subscribe.iter().any(|s| *s != inbox && *s != renewal) {
        bail!("{} are no webhook's: issue them with `ants auth user {webhook} --webhook`", path.display());
    }
    Ok(permissions)
}

/// Issue the credentials of the channel manager, `ants channel serve`, for the hub's JetStream domain if it has one,
/// and write its `.creds` file to `out`: it hears channel management requests and answers each once, reads the roster
/// and the humans, writes the channels bucket and posts in channels as [`UserId::channel_manager`]
/// ([`Permissions::for_channel_manager`]).
pub fn issue_channel_manager(dir: &Path, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    let key = KeyPair::new_user();
    let jwt = sign_channel_manager(dir, js_domain, &key.public_key())?;
    write_secret(out, &creds(&jwt, &seed(&key)?))
}

/// Sign the user JWT of the channel manager for `public_key`, with the permissions [`issue_channel_manager`] gives,
/// and record its key.
pub fn sign_channel_manager(dir: &Path, js_domain: Option<&JsDomain>, public_key: &str) -> Result<String> {
    check_user_key(public_key)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let permissions = Permissions::for_channel_manager().with_js_domain(js_domain);
    let ttl = u64::try_from(ants_proto::CHANADM_TTL.as_nanos())?;
    let answer_once = json!({ "resp": { "max": 1, "ttl": ttl } });
    let name = UserId::channel_manager();
    let jwt = user_jwt(&account, public_key, name.as_str(), permissions, answer_once, None)?;
    revoke::record(dir, &Issued::new(public_key, name.as_str(), Kind::ChannelManager, None)?)?;
    Ok(jwt)
}

/// Sign the user JWT of `user` in `role` for `public_key`, a user NKey generated on another machine, with the
/// permissions [`issue`] gives. The seed stays where it was generated; its `.creds` file is the JWT plus that seed.
pub fn sign_user(
    dir: &Path,
    user: &UserId,
    role: Role,
    js_domain: Option<&JsDomain>,
    public_key: &str,
) -> Result<String> {
    sign_with(dir, user, role.into(), Permissions::for_user(user, role).with_js_domain(js_domain), public_key)
}

/// Sign the user JWT of `user`, of `kind`, with `permissions` for `public_key`, a user NKey generated on another
/// machine, and record the key ([`revoke::record`]). A human's are for a new device ([`sign_device`]).
pub fn sign_with(dir: &Path, user: &UserId, kind: Kind, permissions: Permissions, public_key: &str) -> Result<String> {
    sign_device(dir, user, kind, permissions, None, public_key, None).map(|(jwt, _)| jwt)
}

/// [`sign_with`], with a human's for a new device labelled `label`, as [`issue_device`], binding the encryption key
/// `xkey` generated on the device, if given; only a human's bind one. Returns the JWT and the device id, for a
/// human's.
pub fn sign_device(
    dir: &Path,
    user: &UserId,
    kind: Kind,
    permissions: Permissions,
    label: Option<&str>,
    public_key: &str,
    xkey: Option<&DeviceKey>,
) -> Result<(String, Option<DeviceId>)> {
    sign_device_with(dir, user, kind, permissions, &NewDevice { label, ..NewDevice::default() }, public_key, xkey)
}

/// [`sign_device`], recording `new` with the device.
pub fn sign_device_with(
    dir: &Path,
    user: &UserId,
    kind: Kind,
    permissions: Permissions,
    new: &NewDevice<'_>,
    public_key: &str,
    xkey: Option<&DeviceKey>,
) -> Result<(String, Option<DeviceId>)> {
    check_user_key(public_key)?;
    check_not_reserved(user)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let (device, extra) = device_claims(kind, new.label, xkey)?;
    let jwt = user_jwt(&account, public_key, user.as_str(), permissions, extra, new.expires_at)?;
    let issued = Issued::new(public_key, user.as_str(), kind, new.expires_at)?;
    revoke::record(dir, &new.record(issued, device, xkey.cloned()))?;
    Ok((jwt, device))
}

/// Sign the user JWT of `ant`, added by `manager` from `template`, for `public_key`, with the permissions of an ant,
/// and record the key with its template and manager, which the signer counts against the template's `max_ants`.
pub fn sign_added(
    dir: &Path,
    ant: &UserId,
    js_domain: Option<&JsDomain>,
    public_key: &str,
    template: &str,
    manager: &UserId,
) -> Result<String> {
    check_user_key(public_key)?;
    check_not_reserved(ant)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let permissions = Permissions::for_user(ant, Role::Ant).with_js_domain(js_domain);
    let jwt = user_jwt(&account, public_key, ant.as_str(), permissions, json!({}), None)?;
    let issued = Issued {
        template: Some(template.to_owned()),
        added_by: Some(manager.clone()),
        ..Issued::new(public_key, ant.as_str(), Kind::Ant, None)?
    };
    revoke::record(dir, &issued)?;
    Ok(jwt)
}

/// Credentials renewed by [`renew`].
#[derive(Debug)]
pub struct Renewed {
    /// The user JWT for the new key.
    pub jwt: String,
    /// What the renewed credentials are, as the earlier ones: same name, kind, device and JetStream domain.
    pub finding: Finding,
    /// The public key of the earlier credentials.
    pub previous: String,
}

/// Renew `current`, the user JWT of credentials the account key in `dir` issued, for `public_key`, a new user NKey
/// generated where they are used: a JWT with the same name, kind, members, device, encryption key and JetStream
/// domain, permissions as this version of ants gives them (so an upgrade applies), and as long a lifetime as `current`
/// had, if it had an expiry. A human's credentials without an encryption key bind `xkey`, generated on the device,
/// when given; credentials of others, or with a key already, refuse one. Recorded in the registry as renewed from the earlier key ([`Issued::renewed_from`]), with a leaf node's
/// members ([`crate::members`]), so a later `ants auth leaf` keeps them.
///
/// Refused for credentials of another account, expired or revoked ones, enrollment and leaf system credentials, and
/// for a new key revoked or already issued to other credentials. Proving that the holder has the seed of `current` is
/// the caller's part.
pub fn renew(dir: &Path, current: &str, public_key: &str, xkey: Option<&DeviceKey>) -> Result<Renewed> {
    check_user_key(public_key)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let (claims, body) = verified_body(current)?;
    if claims.iss != account.public_key() {
        bail!("issued by {}, not by the team's account {}", claims.iss, account.public_key());
    }
    if claims.sub == public_key {
        bail!("the new key {public_key} is the key of the credentials to renew; generate a new one");
    }
    let now = now()?;
    if claims.exp.is_some_and(|exp| exp <= now) {
        bail!("the credentials of {} expired; issue new ones with `ants auth`", claims.name);
    }
    let revoked = revoke::revoked(dir)?;
    if revoked.iter().any(|r| r.key == claims.sub) {
        bail!("the credentials of {} ({}) are revoked", claims.name, claims.sub);
    }
    if revoked.iter().any(|r| r.key == public_key) {
        bail!("the new key {public_key} is revoked");
    }
    let finding = Finding::from_claims(&body)?;
    if let Some(xkey) = xkey {
        if finding.kind != CredsKind::Human {
            bail!("{} bind no encryption key, as only a human's do: {xkey} refused", finding.holder());
        }
        if let Some(bound) = &finding.xkey {
            bail!("the credentials of {} bind the encryption key {bound} already: {xkey} refused", claims.name);
        }
    }
    let xkey = finding.xkey.clone().or_else(|| xkey.cloned());
    let issued = revoke::issued(dir)?;
    let kind = registry_kind(&finding.kind)
        .with_context(|| format!("{} cannot be renewed: issue new ones with `ants auth`", finding.holder()))?;
    if issued.iter().any(|i| i.key == public_key && (i.name != claims.name || i.kind != kind)) {
        bail!("the new key {public_key} was issued to other credentials; generate a new one");
    }
    let name = &claims.name;
    let user = || name.parse::<UserId>().map_err(|e| anyhow!("credentials named {name:?}: {e}"));
    let (permissions, extra) = match &finding.kind {
        CredsKind::Ant => (Permissions::for_user(&user()?, Role::Ant), json!({})),
        CredsKind::Human => {
            let tags = finding.device.iter().map(DeviceId::tag).chain(xkey.iter().map(DeviceKey::tag));
            let tags: Vec<String> = tags.collect();
            (Permissions::for_user(&user()?, Role::Human), json!({ "tags": tags }))
        }
        CredsKind::Webhook { to, channels } => {
            let to = to.iter().cloned().map(Destination::Ant);
            let destinations: Vec<Destination> = to.chain(channels.iter().cloned().map(Destination::Channel)).collect();
            (Permissions::for_webhook(&user()?, &destinations), json!({}))
        }
        CredsKind::Observer => (Permissions::for_observer(&user()?), json!({})),
        CredsKind::ChannelManager => {
            let ttl = u64::try_from(ants_proto::CHANADM_TTL.as_nanos())?;
            (Permissions::for_channel_manager(), json!({ "resp": { "max": 1, "ttl": ttl } }))
        }
        CredsKind::Leaf { machine, ants, humans, webhooks, enroll_approver } => {
            let members = members(ants, humans);
            let mut permissions = Permissions::for_leaf(machine, members.iter().map(|(m, r)| (m, *r)));
            let publish = allowed(&body).publish;
            for webhook in webhooks {
                let to = publish.iter().filter_map(|s| ants_proto::subject::parse_inbox(s));
                let to = to.filter(|(_, from)| from == webhook).map(|(to, _)| Destination::Ant(to));
                let channels = publish.iter().filter_map(|s| ants_proto::subject::parse_chan(s));
                let channels = channels.filter(|(_, from)| from == webhook).map(|(c, _)| Destination::Channel(c));
                let destinations: Vec<Destination> = to.chain(channels).collect();
                merge(&mut permissions, Permissions::for_webhook(webhook, &destinations));
            }
            if *enroll_approver {
                merge(&mut permissions, signer_permissions());
            }
            (permissions, json!({ "allowed_connection_types": LEAF_CONNECTIONS }))
        }
        CredsKind::LeafSystem { .. } | CredsKind::Enrollment { .. } | CredsKind::Bootstrap => {
            unreachable!("no registry kind")
        }
    };
    let mut permissions = permissions.with_js_domain(finding.js_domain.as_ref());
    dedup(&mut permissions);
    let lifetime = claims.exp.map(|exp| exp.saturating_sub(body["iat"].as_u64().unwrap_or(now)));
    let exp = lifetime.map(|lifetime| now + lifetime.max(1));
    let jwt = user_jwt(&account, public_key, name, permissions, extra, exp)?;
    if let CredsKind::Leaf { machine, .. } = &finding.kind {
        check_uplink_size(&jwt, machine)?;
    }
    let renewed = Finding::from_jwt(&jwt)?;
    if !finding.same_credentials(&renewed) || renewed.xkey != xkey {
        bail!("renewing {} would change what they are: {:?} into {:?}", finding.holder(), finding.kind, renewed.kind);
    }
    let previous = issued.iter().rev().find(|i| i.key == claims.sub);
    let mut record = inherited(Issued::new(public_key, name, kind, exp)?, previous);
    record.renewed_from = Some(claims.sub.clone());
    if let Some(device) = finding.device {
        record = record.for_device(device, previous.and_then(|p| p.label.clone()), xkey);
    }
    if let CredsKind::Leaf { ants, humans, webhooks, enroll_approver, .. } = &renewed.kind {
        let (ants, humans, webhooks) = (ants.clone(), humans.clone(), webhooks.clone());
        record.members = Some(crate::members::Members { ants, humans, webhooks, enroll_approver: *enroll_approver });
    }
    revoke::record(dir, &record)?;
    Ok(Renewed { jwt, finding: renewed, previous: claims.sub })
}

/// `record` of renewed credentials, with what the registry recorded of the `previous` ones beyond their key: the
/// template and manager of an added ant, and the GitHub binding and grant of a registered device.
fn inherited(record: Issued, previous: Option<&Issued>) -> Issued {
    let Some(p) = previous else { return record };
    let (template, added_by) = (p.template.clone(), p.added_by.clone());
    Issued { template, added_by, github: p.github.clone(), grant: p.grant.clone(), ..record }
}

/// The registry's kind of credentials of `kind`, if they may be renewed ([`renew`]).
fn registry_kind(kind: &CredsKind) -> Option<Kind> {
    match kind {
        CredsKind::Ant => Some(Kind::Ant),
        CredsKind::Human => Some(Kind::Human),
        CredsKind::Webhook { .. } => Some(Kind::Webhook),
        CredsKind::ChannelManager => Some(Kind::ChannelManager),
        CredsKind::Observer => Some(Kind::Observer),
        CredsKind::Leaf { .. } => Some(Kind::Leaf),
        CredsKind::LeafSystem { .. } | CredsKind::Enrollment { .. } | CredsKind::Bootstrap => None,
    }
}

/// Fail unless `public_key` is a user's public NKey, `U...`.
pub fn check_user_key(public_key: &str) -> Result<()> {
    let key = KeyPair::from_public_key(public_key).map_err(|e| anyhow!("public key {public_key}: {e}"))?;
    if key.key_pair_type() != nkeys::KeyPairType::User {
        bail!("{public_key} is not the public key of a user (U...)");
    }
    Ok(())
}

/// Issue the enrollment credentials of `ant`, valid for `ttl` (zero: with no expiry), and write their `.creds` file
/// to `out`: they may only ask for the ant's credentials and hear the answer ([`Permissions::for_enrollment`]).
/// Returns when they expire, in seconds since the epoch.
pub fn issue_enrollment(dir: &Path, ant: &UserId, ttl: Duration, out: &Path) -> Result<Option<u64>> {
    check_not_reserved(ant)?;
    if !ttl.is_zero() && ttl < Duration::from_secs(1) {
        bail!("a ttl of {ttl:?} is under a second: give at least 1s, or 0 for no expiry");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = if ttl.is_zero() { None } else { Some(now()? + ttl.as_secs()) };
    let name = ants_proto::enrollment_user(ant);
    let jwt = user_jwt(&account, &user.public_key(), &name, Permissions::for_enrollment(ant), json!({}), exp)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))?;
    revoke::record(dir, &Issued::new(&user.public_key(), &name, Kind::Enrollment, exp)?)?;
    Ok(exp)
}

/// Issue the team's public bootstrap credentials, valid for `ttl` (zero: with no expiry), and write their `.creds`
/// file to `out`, readable by all as they are meant to be shared: they may only ask `ants signer` to register a
/// human's device and hear its answers ([`Permissions::for_bootstrap`]), with messages of at most
/// [`ants_proto::BOOTSTRAP_MAX_PAYLOAD`] bytes. Returns their public key and when they expire, in seconds since the
/// epoch.
pub fn issue_bootstrap(dir: &Path, ttl: Duration, out: &Path) -> Result<(String, Option<u64>)> {
    if !ttl.is_zero() && ttl < Duration::from_secs(1) {
        bail!("a ttl of {ttl:?} is under a second: give at least 1s, or 0 for no expiry");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = if ttl.is_zero() { None } else { Some(now()? + ttl.as_secs()) };
    let name = ants_proto::BOOTSTRAP_USER;
    let limits = json!({ "payload": ants_proto::BOOTSTRAP_MAX_PAYLOAD });
    let jwt = user_jwt(&account, &user.public_key(), name, Permissions::for_bootstrap(), limits, exp)?;
    fs::write(out, creds(&jwt, &seed(&user)?)).with_context(|| format!("writing {}", out.display()))?;
    #[cfg(unix)]
    fs::set_permissions(out, std::os::unix::fs::PermissionsExt::from_mode(0o644))?;
    revoke::record(dir, &Issued::new(&user.public_key(), name, Kind::Bootstrap, exp)?)?;
    Ok((user.public_key(), exp))
}

/// The text of short-lived credentials for `ants enroll` and `ants signer`, which hear enrollment requests and answer
/// them ([`Permissions::for_enroll_approver`]), for the hub's JetStream domain if it has one, issued afresh from the
/// account key of each run.
pub fn approver_creds(dir: &Path, js_domain: Option<&JsDomain>) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_enroll_approver())
}

/// [`approver_creds`] for `ants signer`, which also answers ants' reactions requests
/// ([`Permissions::for_reactions_responder`]) and humans' registration requests ([`Permissions::for_registrar`]).
pub fn signer_creds(dir: &Path, js_domain: Option<&JsDomain>) -> Result<String> {
    let mut permissions = signer_permissions();
    // Registration happens on the hub only: a leaf uplink with signer_permissions never carries it.
    merge(&mut permissions, Permissions::for_registrar());
    short_lived_creds(dir, js_domain, permissions)
}

/// What `ants enroll` and `ants signer` need: [`Permissions::for_enroll_approver`] and
/// [`Permissions::for_reactions_responder`].
fn signer_permissions() -> Permissions {
    let mut permissions = Permissions::for_enroll_approver();
    merge(&mut permissions, Permissions::for_reactions_responder());
    permissions
}

fn short_lived_creds(dir: &Path, js_domain: Option<&JsDomain>, permissions: Permissions) -> Result<String> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = now()? + APPROVER_TTL.as_secs();
    let permissions = permissions.with_js_domain(js_domain);
    let jwt = user_jwt(&account, &user.public_key(), "enroll", permissions, json!({}), Some(exp))?;
    Ok(creds(&jwt, &seed(&user)?))
}

const APPROVER_TTL: Duration = Duration::from_mins(10);

/// The claims of a user JWT that matter to enrollment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    /// The user's public key.
    pub sub: String,
    /// The account that signed it.
    pub iss: String,
    pub name: String,
    /// Expiry, in seconds since the epoch.
    pub exp: Option<u64>,
}

/// Decode a JWT and check that its issuer signed it.
pub fn verified(jwt: &str) -> Result<Claims> {
    verified_body(jwt).map(|(claims, _)| claims)
}

/// [`verified`], with all of the JWT's claims.
fn verified_body(jwt: &str) -> Result<(Claims, Value)> {
    let jwt = jwt.trim();
    let (signed, signature) = jwt.rsplit_once('.').context("not a JWT")?;
    let (_, body) = signed.split_once('.').context("not a JWT")?;
    let decode = |part: &str| BASE64URL_NOPAD.decode(part.as_bytes()).context("not a JWT");
    let claims: Value = serde_json::from_slice(&decode(body)?).context("not a JWT")?;
    let text = |field: &str| claims[field].as_str().map(str::to_owned).with_context(|| format!("JWT without {field}"));
    let iss = text("iss")?;
    let issuer = KeyPair::from_public_key(&iss).map_err(|e| anyhow!("JWT issuer {iss}: {e}"))?;
    issuer.verify(signed.as_bytes(), &decode(signature)?).map_err(|_| anyhow!("JWT not signed by its issuer {iss}"))?;
    let verified = Claims { sub: text("sub")?, iss, name: text("name")?, exp: claims["exp"].as_u64() };
    Ok((verified, claims))
}

/// The user JWT of a `.creds` file's text: the line after the `BEGIN NATS USER JWT` marker.
pub fn creds_jwt(creds: &str) -> Option<&str> {
    creds_block(creds, "BEGIN NATS USER JWT")
}

fn creds_block<'a>(creds: &'a str, marker: &str) -> Option<&'a str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains(marker))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

/// Write the `.creds` file of `jwt` and the seed of `user` to `out`, readable by its owner only.
pub fn write_creds(out: &Path, jwt: &str, user: &KeyPair) -> Result<()> {
    write_secret(out, &creds(jwt, &seed(user)?))
}

/// Write `user`'s seed alone to `out`, readable by its owner only.
pub fn write_seed(out: &Path, user: &KeyPair) -> Result<()> {
    write_secret(out, &format!("{}\n", seed(user)?))
}

/// Read a seed written by [`write_seed`].
pub fn read_seed(path: &Path) -> Result<KeyPair> {
    let seed = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{}: {e}", path.display()))
}

pub(crate) fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

/// The `server.conf` of the team, with `operator_jwt` if given, else a new one.
fn server_conf(
    operator: &KeyPair,
    operator_jwt: Option<&str>,
    system: &KeyPair,
    account: &KeyPair,
    revocations: &BTreeMap<String, u64>,
) -> Result<String> {
    let operator_jwt = match operator_jwt {
        Some(jwt) => jwt.to_owned(),
        None => jwt(
            operator,
            &operator.public_key(),
            TEAM,
            json!({ "system_account": system.public_key(), "type": "operator", "version": 2 }),
            None,
        )?,
    };
    // JetStream needs a system account in operator mode; `ants auth push` connects to it, and leaf nodes join it to
    // hear the pushes.
    let system_limits = json!({ "conn": -1, "leaf": -1, "subs": -1, "data": -1, "payload": -1, "wildcards": true });
    let system_claims = json!({ "limits": system_limits, "type": "account", "version": 2 });
    let system_jwt = jwt(operator, &system.public_key(), "SYS", system_claims, None)?;
    let account_jwt = account_jwt(operator, account, revocations)?;
    Ok(format!(
        "# Written by `ants auth init`. Include it from the server configuration: include \"{SERVER_CONF}\"\n\
         # Credentials carry their permissions in the CONNECT line, longer than the 4 KB nats-server allows by default.\n\
         max_control_line: {MAX_CONTROL_LINE}\n\
         operator: {operator_jwt}\n\
         system_account: {system}\n\
         resolver: MEMORY\n\
         resolver_preload: {{\n  {system}: {system_jwt}\n  {account}: {account_jwt}\n}}\n",
        system = system.public_key(),
        account = account.public_key(),
    ))
}

/// The JWT of the team's account, revoking the user keys of `revocations` issued at or before their time.
fn account_jwt(operator: &KeyPair, account: &KeyPair, revocations: &BTreeMap<String, u64>) -> Result<String> {
    let mut nats = json!({
        "limits": {
            "subs": -1, "data": -1, "payload": -1, "imports": -1, "exports": -1, "wildcards": true,
            "conn": -1, "leaf": -1, "mem_storage": -1, "disk_storage": -1, "streams": -1, "consumer": -1,
        },
        "default_permissions": { "pub": {}, "sub": {} },
        "type": "account",
        "version": 2,
    });
    if !revocations.is_empty() {
        nats["revocations"] = json!(revocations);
    }
    jwt(operator, &account.public_key(), TEAM, nats, None)
}

/// A user JWT for the public key `user` with `permissions`, plus the `extra` claims, expiring at `exp` (seconds since
/// the epoch) if given. Its tags carry [`PERMISSIONS_VERSION`].
fn user_jwt(
    account: &KeyPair,
    user: &str,
    name: &str,
    permissions: Permissions,
    extra: Value,
    exp: Option<u64>,
) -> Result<String> {
    let Permissions { publish, subscribe } = permissions;
    let mut nats = json!({
        "pub": { "allow": publish },
        "sub": { "allow": subscribe },
        "subs": -1, "data": -1, "payload": -1,
        "type": "user",
        "version": 2,
    });
    if let (Value::Object(nats), Value::Object(extra)) = (&mut nats, extra) {
        nats.extend(extra);
    }
    let mut tags = nats["tags"].as_array().cloned().unwrap_or_default();
    tags.push(PERMISSIONS_VERSION.tag().into());
    nats["tags"] = tags.into();
    jwt(account, user, name, nats, exp)
}

/// A NATS v2 JWT for `subject`, signed by `issuer`, expiring at `exp` if given.
fn jwt(issuer: &KeyPair, subject: &str, name: &str, nats: Value, exp: Option<u64>) -> Result<String> {
    let mut claims = json!({ "iat": now()?, "iss": issuer.public_key(), "name": name, "sub": subject, "nats": nats });
    if let Some(exp) = exp {
        claims["exp"] = exp.into();
    }
    // As in nats-io/jwt: the id is the hash of the claims without it.
    let id = ring::digest::digest(&ring::digest::SHA512_256, &serde_json::to_vec(&claims)?);
    claims["jti"] = BASE32_NOPAD.encode(id.as_ref()).into();
    let header = BASE64URL_NOPAD.encode(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let signed = format!("{header}.{}", BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims)?));
    let signature = issuer.sign(signed.as_bytes()).map_err(|e| anyhow!("signing: {e}"))?;
    Ok(format!("{signed}.{}", BASE64URL_NOPAD.encode(&signature)))
}

fn creds(jwt: &str, seed: &str) -> String {
    ants_daemon::creds::text(jwt, seed)
}

/// The text of a `.creds` file holding `jwt` and `seed`, and the seed of the encryption key `xkey` if given.
pub(crate) fn creds_with(jwt: &str, seed: &str, xkey: Option<&XKey>) -> Result<String> {
    let mut text = creds(jwt, seed);
    if let Some(xkey) = xkey {
        text += &ants_daemon::creds::xkey_block(Seed::File(&xseed(xkey)?));
    }
    Ok(text)
}

pub(crate) fn xseed(xkey: &XKey) -> Result<String> {
    xkey.seed().map_err(|e| anyhow!("{e}"))
}

pub(crate) fn seed(key: &KeyPair) -> Result<String> {
    key.seed().map_err(|e| anyhow!("{e}"))
}

fn load(path: &Path) -> Result<KeyPair> {
    let seed =
        fs::read_to_string(path).with_context(|| format!("reading {} (run `ants auth init`)", path.display()))?;
    KeyPair::from_seed(seed.trim()).map_err(|e| anyhow!("{}: {e}", path.display()))
}

/// Replace the `.creds` file at `path` with that of `jwt` and the seed of `user`, and of the encryption key `xkey` if
/// given, at once: written beside it, readable by its owner only, then renamed over it, so what reads it finds either
/// the earlier credentials or the new ones.
pub fn replace_creds(path: &Path, jwt: &str, user: &KeyPair, xkey: Option<&XKey>) -> Result<()> {
    replace_secret(path, &creds_with(jwt, &seed(user)?, xkey)?)
}

/// Replace the file at `path` with `contents` at once: written beside it, readable by its owner only, then renamed
/// over it.
pub(crate) fn replace_secret(path: &Path, contents: &str) -> Result<()> {
    let name = path.file_name().with_context(|| format!("{} names no file", path.display()))?;
    let mut temporary = name.to_owned();
    temporary.push(format!(".{}.tmp", std::process::id()));
    let temporary = path.with_file_name(temporary);
    let written = write_secret(&temporary, contents)
        .and_then(|()| fs::rename(&temporary, path).with_context(|| format!("replacing {}", path.display())));
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    written
}

/// Write a file only its owner can read, replacing any earlier one.
pub(crate) fn write_secret(path: &Path, contents: &str) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(path).with_context(|| format!("writing {}", path.display()))?;
    // `mode` applies only on creation; tighten a file that already existed too.
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    file.write_all(contents.as_bytes())?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
