//! Team credentials for NATS decentralized auth: one operator, the team's account, and one user per ant or human whose
//! JWT carries the permissions of [`Permissions::for_user`] in its role.
//!
//! The streams, KV buckets and object stores live in an account of their own, the store account, the only one with
//! JetStream. It exports as services the JetStream API and every subject they store, and as streams the reply inboxes
//! JetStream delivers to; the team's account imports them under the same names ([`store`]). So no subject a user
//! names as a reply, which the server answers in the team's account, is a stored one.
//!
//! The operator and account seeds stay in the team directory on the machine that issues credentials. The server
//! needs only `server.conf`; each machine gets the `.creds` file of the ants it runs, and its leaf node the uplink
//! credentials of [`issue_leaf`].
//!
//! When the hub has a JetStream domain, credentials are issued for it: they allow the JetStream API as
//! `$JS.<domain>.API`, the prefix every ants process then uses, besides the plain `$JS.API` the hub maps it to
//! ([`Permissions::with_js_domain`]); a leaf node's uplink credentials allow the domain form only
//! ([`Permissions::with_uplink_js_domain`]).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use ants_proto::{BuiltinAnt, DEVICES_BUCKET, LeafRole, Permissions, REVOKED_DEVICES, Role, UserId};
use anyhow::{Context as _, Result, bail};
use nkeys::KeyPair;
use serde_json::json;

use self::claims::{jwt, user_jwt};
use self::files::{creds, load};
use self::short_lived::APPROVER_TTL;
use crate::credentials::revoke::{self, Issued, Kind};

pub(crate) use self::claims::now;
#[cfg(test)]
pub(crate) use self::claims::verified_body;
pub use self::claims::{Claims, allows_all, check_signature, check_user_key, sign, verified};
pub use self::files::{creds_jwt, read_creds, read_seed, replace_creds, write_creds, write_seed};
pub(crate) use self::files::{replace_secret, seed, write_secret, xseed};
pub(crate) use self::issuing::device_key;
pub use self::issuing::{
    NewDevice, issue_builtin, issue_channel_manager, issue_device, issue_device_with, issue_host, sign_added,
    sign_builtin, sign_channel_manager, sign_device_with, sign_host, sign_ordered, sign_user, sign_with,
};
#[cfg(test)]
pub use self::issuing::{issue, issue_with, sign_device};
pub use self::relocate::move_stores;
pub use self::renewal::{Renewed, renew, renew_registered};
pub(crate) use self::short_lived::retired_consumer_creds;
pub use self::short_lived::{approver_creds, issue_bootstrap, issue_enrollment, revoker_creds, signer_creds};
pub use self::uplink::{
    check_leaf, extend_leaf, issue_leaf, leaf_store_creds, leaf_system_creds, leaf_user, shrink_leaf,
};

mod claims;
mod files;
mod issuing;
mod relocate;
mod renewal;
mod short_lived;
mod store;
mod uplink;

pub(crate) const OPERATOR_SEED: &str = "operator.nk";
pub(crate) const ACCOUNT_SEED: &str = "account.nk";
pub(crate) const STORE_SEED: &str = "store.nk";
pub(crate) const SYSTEM_SEED: &str = "system.nk";
pub const SERVER_CONF: &str = "server.conf";
const TEAM: &str = "ants";
const STORE: &str = "ants-store";
/// `max_control_line` of the servers that include [`SERVER_CONF`], hub and leaf nodes alike: a user JWT grows with the
/// length of its user's name and of the JetStream domain, which may each take 64 characters, and the longest, an
/// ant's, takes about 29 KB then.
const MAX_CONTROL_LINE: u32 = 64 * 1024;
/// The longest uplink user JWT of a leaf node ([`issue_leaf`]): within the 16 times the hub's `max_control_line`
/// nats-server takes in a leaf node's CONNECT line, and small enough for the replies carrying a renewed one (enrollment
/// approval, `ants signer`) to stay well within NATS's 1 MB `max_payload`.
const MAX_UPLINK_JWT: usize = 252 * 1024;
const _: () = assert!(MAX_UPLINK_JWT + 4 * 1024 <= 16 * MAX_CONTROL_LINE as usize);

/// The default team directory, `~/.config/ants/auth`.
pub fn default_dir() -> Result<PathBuf> {
    Ok(std::env::home_dir().context("no home directory")?.join(".config/ants/auth"))
}

/// `dir`, else [`default_dir`].
pub fn team_dir(dir: Option<PathBuf>) -> Result<PathBuf> {
    match dir {
        Some(dir) => Ok(dir),
        None => default_dir(),
    }
}

/// The keys of a team: its operator, system account, store account and the team's account.
struct Keys {
    operator: KeyPair,
    system: KeyPair,
    store: KeyPair,
    account: KeyPair,
}

impl Keys {
    fn load(dir: &Path) -> Result<Self> {
        if holds_team(dir) && !holds_store(dir) {
            bail!("the team in {} has no store account: `ants auth init` adds it", dir.display());
        }
        Ok(Self {
            operator: load(&dir.join(OPERATOR_SEED))?,
            system: load(&dir.join(SYSTEM_SEED))?,
            store: load(&dir.join(STORE_SEED))?,
            account: load(&dir.join(ACCOUNT_SEED))?,
        })
    }
}

/// Create the operator and accounts in `dir` and write the `server.conf` that trusts them.
pub fn init(dir: &Path) -> Result<()> {
    if dir.join(ACCOUNT_SEED).exists() {
        bail!("{} already holds a team", dir.display());
    }
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let keys = Keys {
        operator: KeyPair::new_operator(),
        system: KeyPair::new_account(),
        store: KeyPair::new_account(),
        account: KeyPair::new_account(),
    };
    write_secret(&dir.join(OPERATOR_SEED), &seed(&keys.operator)?)?;
    write_secret(&dir.join(SYSTEM_SEED), &seed(&keys.system)?)?;
    write_secret(&dir.join(STORE_SEED), &seed(&keys.store)?)?;
    write_secret(&dir.join(ACCOUNT_SEED), &seed(&keys.account)?)?;
    fs::write(dir.join(SERVER_CONF), server_conf(&keys, None, &BTreeMap::new())?)?;
    Ok(())
}

/// Add the store account to the team in `dir`, made before there was one, and write its `server.conf` again with the
/// revocations still in effect ([`revoke::apply`]), the operator JWT kept.
pub fn add_store(dir: &Path) -> Result<()> {
    if !holds_team(dir) || !dir.join(ACCOUNT_SEED).is_file() {
        bail!("no team in {}", dir.display());
    }
    if holds_store(dir) {
        bail!("{} already holds a team with a store account", dir.display());
    }
    write_secret(&dir.join(STORE_SEED), &seed(&KeyPair::new_account())?)?;
    revoke::apply(dir)?;
    Ok(())
}

/// An account's public key and JWT, as `ants auth push` sends it.
pub type AccountJwt = (String, String);

/// Write the `server.conf` of the team in `dir` again, its account revoking each user key of `revocations` issued
/// at or before its time (seconds since the epoch). Returns the store account's public key and JWT, then the team
/// account's, the order in which a server takes them: the team's account imports from the store's.
pub fn write_server_conf(dir: &Path, revocations: &BTreeMap<String, u64>) -> Result<[AccountJwt; 2]> {
    let keys = Keys::load(dir)?;
    let path = dir.join(SERVER_CONF);
    // nats-server reloads no change of the trusted operator, so the operator JWT already there stays.
    let earlier = fs::read_to_string(&path).unwrap_or_default();
    let operator_jwt = earlier.lines().find_map(|line| line.strip_prefix("operator: ")).filter(|jwt| {
        let operator = keys.operator.public_key();
        verified(jwt).is_ok_and(|claims| claims.sub == operator && claims.iss == operator)
    });
    fs::write(&path, server_conf(&keys, operator_jwt, revocations)?)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok([(keys.store.public_key(), store_jwt(&keys)?), (keys.account.public_key(), account_jwt(&keys, revocations)?)])
}

/// Whether `dir` holds the keys of a team, those of its system account included.
pub fn holds_team(dir: &Path) -> bool {
    dir.join(SYSTEM_SEED).is_file()
}

/// Whether the team in `dir` has its store account.
pub fn holds_store(dir: &Path) -> bool {
    dir.join(STORE_SEED).is_file()
}

/// The public key of the store account in `dir`.
pub fn store_key(dir: &Path) -> Result<String> {
    Ok(load(&dir.join(STORE_SEED))?.public_key())
}

/// The public keys of the team's account and of the store account a `server.conf` of `ants auth init` preloads, by
/// their names; the store account's None in one written before there was one.
pub fn preloaded_accounts(server_conf: &str) -> Result<(String, Option<String>)> {
    let (mut account, mut store) = (None, None);
    for line in server_conf.lines() {
        let Some((key, jwt)) = line.trim().split_once(": ") else { continue };
        let Ok(claims) = verified(jwt) else { continue };
        if claims.sub != key {
            continue;
        }
        match claims.name.as_str() {
            TEAM => account = Some(claims.sub),
            STORE => store = Some(claims.sub),
            _ => {}
        }
    }
    Ok((account.context("no account of the team preloaded: not a server.conf of `ants auth init`")?, store))
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

/// Fail unless the ant `user` may get the credentials of the assistant of `human` in the team in `dir`: `human` is the
/// default human or one the registry knows, `user` is neither a human nor an ant a manager added from a template, and
/// no credentials neither revoked nor expired make another ant `human`'s assistant or `user` another human's.
pub fn check_assistant(dir: &Path, user: &UserId, human: &UserId) -> Result<()> {
    if user == human {
        bail!("{user} cannot be its own assistant");
    }
    if user.is_human() {
        bail!("{user} is a human; an assistant is an ant");
    }
    let issued = revoke::issued(dir)?;
    let live = live_in(dir)?;
    let named = |i: &&revoke::Issued, name: &UserId| i.name == name.as_str();
    if !human.is_human() && !issued.iter().any(|i| named(&i, human) && i.kind == revoke::Kind::Human) {
        bail!("{human} is not a human of the team: issue its credentials with `ants auth user {human} --human` first");
    }
    let mine = issued.iter().filter(|i| named(i, user));
    if let Some(i) = mine.clone().find(|i| i.kind == revoke::Kind::Human) {
        bail!("{user} holds a human's credentials ({}); an assistant is an ant", i.key);
    }
    if mine.clone().any(|i| i.template.is_some()) {
        bail!("{user} is an ant a manager added from a template; such an ant cannot be an assistant");
    }
    let assisting = |i: &&revoke::Issued| i.kind == revoke::Kind::Assistant && live(i);
    if let Some(i) = mine.filter(assisting).find(|i| i.assistant_of.as_ref() != Some(human)) {
        let of = i.assistant_of.as_ref().map_or("another human", UserId::as_str);
        bail!("{user} is the assistant of {of} ({}): revoke those credentials first", i.key);
    }
    let mut others = issued.iter().filter(|i| !named(i, user) && i.assistant_of.as_ref() == Some(human));
    if let Some(i) = others.find(assisting) {
        bail!(
            "{} is already the assistant of {human} ({}); a human has one: revoke those credentials first",
            i.name,
            i.key
        );
    }
    Ok(())
}

/// Whether credentials the registry of `dir` records are neither revoked nor expired.
fn live_in(dir: &Path) -> Result<impl Fn(&Issued) -> bool> {
    let revoked = revoke::revoked(dir)?;
    let now = now()?;
    Ok(move |i: &Issued| !revoked.iter().any(|r| r.key == i.key) && i.expires_at.is_none_or(|exp| exp > now))
}

/// The ants of the team in `dir` that are a human's assistant, as their credentials neither revoked nor expired make
/// them, each with its human.
fn assistants(dir: &Path) -> Result<BTreeMap<UserId, UserId>> {
    let live = live_in(dir)?;
    let issued = revoke::issued(dir)?;
    let assisting = issued.into_iter().filter(|i| i.kind == Kind::Assistant && live(i));
    Ok(assisting.filter_map(|i| Some((i.name.parse().ok()?, i.assistant_of?))).collect())
}

/// The role of `member` of a leaf node, in `role` by the members kept, as [`Permissions::for_leaf`] takes it: an ant
/// among `assistants` is its human's assistant.
fn leaf_role<'a>(assistants: &'a BTreeMap<UserId, UserId>, member: &UserId, role: Role) -> LeafRole<'a> {
    match assistants.get(member) {
        Some(human) if !role.is_human() => LeafRole::AssistantOf(human),
        _ => role.into(),
    }
}

/// Fail when `user` is a name the team reserves: [`UserId::channel_manager`], which no ant or human may take,
/// [`REVOKED_DEVICES`], under which the devices bucket keeps its revocation records, [`UserId::signer`], and
/// [`ants_proto::BOOTSTRAP_USER`], the name of the public bootstrap credentials, which `ants auth revoke bootstrap`
/// revokes.
pub fn check_not_reserved(user: &UserId) -> Result<()> {
    if let Some(builtin) = BuiltinAnt::from_id(user.as_str()) {
        bail!("{user} is reserved for a built-in ant (`ants auth builtin {}`); choose another name", builtin.name());
    }
    if *user == UserId::channel_manager() {
        bail!("{user} is reserved for the channel manager (`ants auth channel-manager`); choose another name");
    }
    if user.as_str() == REVOKED_DEVICES {
        bail!("{user} is reserved for the revocation records of {DEVICES_BUCKET}; choose another name");
    }
    if *user == UserId::signer() {
        bail!("{user} is reserved for the approval requests of `ants signer`; choose another name");
    }
    if user.as_str() == ants_proto::BOOTSTRAP_USER {
        bail!("{user} is reserved for the public bootstrap credentials (`ants auth bootstrap`); choose another name");
    }
    Ok(())
}

/// The `server.conf` of the team, with `operator_jwt` if given, else a new one.
fn server_conf(keys: &Keys, operator_jwt: Option<&str>, revocations: &BTreeMap<String, u64>) -> Result<String> {
    let Keys { operator, system, store, account } = keys;
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
    Ok(format!(
        "# Written by `ants auth init`. Include it from the server configuration: include \"{SERVER_CONF}\"\n\
         # Credentials carry their permissions in the CONNECT line, longer than the 4 KB nats-server allows by default.\n\
         max_control_line: {MAX_CONTROL_LINE}\n\
         operator: {operator_jwt}\n\
         system_account: {system}\n\
         resolver: MEMORY\n\
         resolver_preload: {{\n  {system}: {system_jwt}\n  {store}: {store_jwt}\n  {account}: {account_jwt}\n}}\n",
        system = system.public_key(),
        store = store.public_key(),
        store_jwt = store_jwt(keys)?,
        account = account.public_key(),
        account_jwt = account_jwt(keys, revocations)?,
    ))
}

/// The limits of an account, with JetStream if `jetstream`, else without.
fn limits(jetstream: bool) -> serde_json::Value {
    let storage = if jetstream { -1 } else { 0 };
    json!({
        "subs": -1, "data": -1, "payload": -1, "imports": -1, "exports": -1, "wildcards": true,
        "conn": -1, "leaf": -1,
        "mem_storage": storage, "disk_storage": storage, "streams": storage, "consumer": storage,
    })
}

/// The JWT of the store account: JetStream, exporting what the team's account imports ([`store`]).
fn store_jwt(keys: &Keys) -> Result<String> {
    let nats = json!({
        "limits": limits(true),
        "exports": store::exports(),
        "default_permissions": { "pub": {}, "sub": {} },
        "type": "account",
        "version": 2,
    });
    jwt(&keys.operator, &keys.store.public_key(), STORE, nats, None)
}

/// The JWT of the team's account, without JetStream of its own, importing the store account's, and revoking the user
/// keys of `revocations` issued at or before their time.
fn account_jwt(keys: &Keys, revocations: &BTreeMap<String, u64>) -> Result<String> {
    let mut nats = json!({
        "limits": limits(false),
        "imports": store::imports(&keys.store.public_key()),
        "default_permissions": { "pub": {}, "sub": {} },
        "type": "account",
        "version": 2,
    });
    if !revocations.is_empty() {
        nats["revocations"] = json!(revocations);
    }
    jwt(&keys.operator, &keys.account.public_key(), TEAM, nats, None)
}

/// Write the `server.conf` of the team in `dir` as `ants auth init` wrote it before the store account: the team's
/// account holding JetStream itself, and no store account.
#[cfg(test)]
pub(crate) fn write_server_conf_before_store(dir: &Path) -> Result<()> {
    let keys = Keys::load(dir)?;
    let nats = json!({
        "limits": limits(true),
        "default_permissions": { "pub": {}, "sub": {} },
        "type": "account",
        "version": 2,
    });
    let account_jwt = jwt(&keys.operator, &keys.account.public_key(), TEAM, nats, None)?;
    let conf = server_conf(&keys, None, &BTreeMap::new())?;
    let conf: String = conf
        .lines()
        .filter(|line| !line.trim_start().starts_with(&keys.store.public_key()))
        .map(|line| match line.trim_start().strip_prefix(&format!("{}: ", keys.account.public_key())) {
            Some(_) => format!("  {}: {account_jwt}\n", keys.account.public_key()),
            None => format!("{line}\n"),
        })
        .collect();
    fs::write(dir.join(SERVER_CONF), conf)?;
    Ok(())
}

#[cfg(test)]
mod tests;
