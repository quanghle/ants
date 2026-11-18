//! A leaf node's uplink credentials: issued for the machine's ants and humans, checked, extended to an ant added and
//! shrunk to one removed, with the system account credentials of the leaf's second uplink.

use std::path::{Path, PathBuf};

use ants_daemon::secrets::creds::{Finding, Kind as CredsKind};
use ants_proto::{JsDomain, LeafRole, Machine, Permissions, Role, UserId};
use anyhow::{Context as _, Result, bail};
use nkeys::KeyPair;
use serde_json::{Value, json};

use super::claims::{Claims, now, user_jwt, verified_body};
use super::files::{creds, load, seed, write_secret};
use super::issuing::webhook_permissions;
use super::short_lived::signer_permissions;
use super::{
    ACCOUNT_SEED, MAX_CONTROL_LINE, MAX_UPLINK_JWT, SYSTEM_SEED, assistants, check_not_reserved, leaf_role, members,
};
use crate::credentials::revoke::{self, Issued, Kind};
use crate::node::members::EnrollApprover;

/// Issue the uplink user of `machine`'s leaf node and write its `.creds` file to `out`. It may carry only what
/// `members`, ants and humans each in its role ([`members`]), an ant the registry makes a human's assistant as one
/// ([`assistants`]), may do and the sourcing of the machine's outbox
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
    let assistants = assistants(dir)?;
    let roles = members.iter().map(|(member, role)| (member, leaf_role(&assistants, member, *role)));
    let mut permissions = Permissions::for_leaf(machine, roles);
    for webhook in webhooks {
        merge(&mut permissions, webhook_permissions(dir, webhook)?);
    }
    if approver == EnrollApprover::With {
        merge(&mut permissions, signer_permissions());
    }
    let mut permissions = permissions.with_uplink_js_domain(js_domain);
    dedup(&mut permissions);
    let only_leaf = json!({ "allowed_connection_types": LEAF_CONNECTIONS });
    let name = leaf_user(machine);
    let jwt = user_jwt(&account, &user.public_key(), &name, permissions, only_leaf, None)?;
    check_uplink_size(&jwt, machine)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))?;
    let issued = Issued::new(&user.public_key(), &name, Kind::Leaf, None)?;
    let members = crate::node::members::Members::new(members, webhooks, approver);
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
pub(super) fn issue_leaf_system(dir: &Path, machine: &Machine, account: &str, out: &Path) -> Result<()> {
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
pub(super) const PACK_REQUEST: &str = "$SYS.REQ.CLAIMS.PACK";

/// The inbox prefix of the system requests of `machine`'s leaf node, named after the machine (`ants leaf config`):
/// nats-server takes `$SYS._INBOX.` and 8 base-62 digits of the SHA-256 of its server name.
pub(super) fn system_inbox(machine: &Machine) -> String {
    const DIGITS: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let sha = ring::digest::digest(&ring::digest::SHA256, machine.as_str().as_bytes());
    let hash: String = sha.as_ref()[..8].iter().map(|b| char::from(DIGITS[usize::from(b % 62)])).collect();
    format!("$SYS._INBOX.{hash}")
}

pub(super) const LEAF_CONNECTIONS: [&str; 2] = ["LEAFNODE", "LEAFNODE_WS"];

/// The `name` of the uplink user of `machine`'s leaf node.
pub fn leaf_user(machine: &Machine) -> String {
    format!("leaf-{machine}")
}

/// Refuse `jwt`, uplink credentials of `machine`'s leaf node, if over [`MAX_UPLINK_JWT`]. Each ant adds about 4.8 KB
/// to them, 5.1 KB with a JetStream domain.
pub(super) fn check_uplink_size(jwt: &str, machine: &Machine) -> Result<()> {
    if jwt.len() > MAX_UPLINK_JWT {
        bail!(
            "the uplink credentials of {machine} would take {} bytes, over the {MAX_UPLINK_JWT} allowed (within the 16 \
             times max_control_line {MAX_CONTROL_LINE} a hub takes in a leaf node's CONNECT, and small enough to send \
             renewed): run some of its ants, humans or webhooks behind another leaf node, under another machine name",
            jwt.len()
        );
    }
    Ok(())
}

/// Add `more` to `permissions`, keeping each subject once, in the order first seen.
pub(super) fn merge(permissions: &mut Permissions, more: Permissions) {
    permissions.publish.extend(more.publish);
    permissions.subscribe.extend(more.subscribe);
    dedup(permissions);
}

pub(super) fn dedup(permissions: &mut Permissions) {
    for all in [&mut permissions.publish, &mut permissions.subscribe] {
        let mut seen = std::collections::HashSet::new();
        all.retain(|s| seen.insert(s.clone()));
    }
}

/// The publish and subscribe allow lists of a user JWT's claims.
pub(super) fn allowed(claims: &Value) -> Permissions {
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

pub(super) fn leaf_claims(account: &str, jwt: &str, machine: &Machine) -> Result<(Claims, Value)> {
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
///
/// [`PERMISSIONS_VERSION`]: ants_proto::PERMISSIONS_VERSION
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
    let renewed = super::claims::jwt(&account, &claims.sub, &claims.name, nats, claims.exp)?;
    check_uplink_size(&renewed, machine)?;
    Ok(renewed)
}

/// Renew `jwt`, the uplink user JWT of `machine`'s leaf node issued from the account key in `dir`, for the same key,
/// without what it allows only for `ant`, retired: what [`Permissions::for_leaf`] gives the ant in `role`, in the
/// JetStream domain of the uplink, that neither the other members it carries nor its webhooks and enrollment approver
/// need. Everything else in it stays as it is ([`extend_leaf`]). None when it allows nothing only for `ant`.
pub fn shrink_leaf(dir: &Path, jwt: &str, machine: &Machine, ant: &UserId, role: LeafRole) -> Result<Option<String>> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let (claims, body) = leaf_claims(&account.public_key(), jwt, machine)
        .with_context(|| format!("the uplink credentials of {machine}"))?;
    if revoke::revoked(dir)?.iter().any(|r| r.key == claims.sub) {
        bail!("the uplink credentials of {machine} are revoked");
    }
    let finding = Finding::from_claims(&body)?;
    let domain = finding.js_domain.as_ref();
    let CredsKind::Leaf { ants, humans, webhooks, enroll_approver, .. } = finding.kind else {
        bail!("the uplink credentials of {machine} carry no members");
    };
    let others: Vec<(UserId, Role)> = members(&ants, &humans).into_iter().filter(|(member, _)| member != ant).collect();
    let assistants = assistants(dir)?;
    let roles = others.iter().map(|(member, role)| (member, leaf_role(&assistants, member, *role)));
    let mut kept = Permissions::for_leaf(machine, roles);
    for webhook in &webhooks {
        if let Ok(permissions) = webhook_permissions(dir, webhook) {
            merge(&mut kept, permissions);
        }
    }
    if enroll_approver {
        merge(&mut kept, signer_permissions());
    }
    let kept = kept.with_uplink_js_domain(domain);
    let only = Permissions::for_leaf(machine, [(ant, role)]).with_uplink_js_domain(domain);
    let mut permissions = allowed(&body);
    let before = permissions.publish.len() + permissions.subscribe.len();
    for (all, only, kept) in [
        (&mut permissions.publish, &only.publish, &kept.publish),
        (&mut permissions.subscribe, &only.subscribe, &kept.subscribe),
    ] {
        all.retain(|s| !only.contains(s) || kept.contains(s));
    }
    if permissions.publish.len() + permissions.subscribe.len() == before {
        return Ok(None);
    }
    let mut nats = body["nats"].clone();
    nats["pub"]["allow"] = json!(permissions.publish);
    nats["sub"]["allow"] = json!(permissions.subscribe);
    super::claims::jwt(&account, &claims.sub, &claims.name, nats, claims.exp).map(Some)
}
