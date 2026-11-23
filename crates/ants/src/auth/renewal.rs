//! Renewing credentials for a new key: the rights they carry, reread from the team's records, and the record of
//! the renewed ones.

use std::collections::BTreeMap;
use std::path::Path;

use ants_daemon::config::registration::{GithubAccount, Grant};
use ants_daemon::secrets::creds::{Finding, Kind as CredsKind};
use ants_proto::{AssistantOf, Destination, DeviceKey, Permissions, Role, UserId};
#[cfg(doc)]
use ants_proto::{REGISTERED_TAG, WEB_TAG};
use anyhow::{Context as _, Result, anyhow, bail, ensure};
use serde_json::{Value, json};

use super::claims::{check_user_key, now, user_jwt, verified_body};
use super::files::load;
use super::issuing::{human_tags, in_domain};
use super::short_lived::signer_permissions;
use super::uplink::{LEAF_CONNECTIONS, allowed, check_uplink_size, dedup, merge};
use super::{ACCOUNT_SEED, assistants, leaf_role, members};
use crate::credentials::revoke::{self, Issued, Kind};

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
/// when given; credentials of others, a web device's, or those with a key already, refuse one. A web device's stay a
/// web device's ([`WEB_TAG`], [`Permissions::for_web`]). Recorded in the registry as renewed from the earlier key
/// ([`Issued::renewed_from`]), with a leaf node's members ([`crate::node::members`]), so a later `ants auth leaf`
/// keeps them.
///
/// Refused for credentials of another account, expired or revoked ones, enrollment and leaf system credentials, and
/// for a new key revoked or already issued to other credentials. Proving that the holder has the seed of `current` is
/// the caller's part.
pub fn renew(dir: &Path, current: &str, public_key: &str, xkey: Option<&DeviceKey>) -> Result<Renewed> {
    renew_as(dir, current, public_key, xkey, None)
}

/// [`renew`] the credentials of a device a registration granted, as `ants signer` does once their grant still holds:
/// recorded as now bound to `github` (its current login) and granted by `grant`, and marked [`REGISTERED_TAG`], which
/// [`renew`] leaves out.
pub fn renew_registered(
    dir: &Path,
    current: &str,
    public_key: &str,
    xkey: Option<&DeviceKey>,
    github: GithubAccount,
    grant: Grant,
) -> Result<Renewed> {
    renew_as(dir, current, public_key, xkey, Some((github, grant)))
}

pub(super) fn renew_as(
    dir: &Path,
    current: &str,
    public_key: &str,
    xkey: Option<&DeviceKey>,
    registered: Option<(GithubAccount, Grant)>,
) -> Result<Renewed> {
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
    ensure!(!revoked.iter().any(|r| r.key == public_key), "the new key {public_key} is revoked");
    let finding = Finding::from_claims(&body)?;
    if let Some(xkey) = xkey {
        if finding.kind != CredsKind::Human {
            bail!("{} bind no encryption key, as only a human's do: {xkey} refused", finding.holder());
        }
        if finding.web {
            bail!(
                "the credentials of {} are a web device's, which bind no encryption key: {xkey} refused",
                claims.name
            );
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
    let assistants = assistants(dir)?;
    let (permissions, extra) = renewed_rights(&finding, &body, name, xkey.as_ref(), registered.is_some(), &assistants)?;
    let mut permissions = in_domain(permissions, &finding.kind, finding.js_domain.as_ref());
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
    let mut record = inherited(Issued::new(public_key, name, kind, exp)?, previous, registered);
    record.renewed_from = Some(claims.sub.clone());
    record.admin = renewed.admin;
    record.web = renewed.web;
    if let CredsKind::Assistant { of } = &renewed.kind {
        record.assistant_of = Some(of.clone());
    }
    if let Some(device) = finding.device {
        record = record.for_device(device, previous.and_then(|p| p.label.clone()), xkey);
    }
    if let CredsKind::Leaf { ants, humans, webhooks, enroll_approver, .. } = &renewed.kind {
        let (ants, humans, webhooks) = (ants.clone(), humans.clone(), webhooks.clone());
        record.members =
            Some(crate::node::members::Members { ants, humans, webhooks, enroll_approver: *enroll_approver });
    }
    revoke::record(dir, &record)?;
    Ok(Renewed { jwt, finding: renewed, previous: claims.sub })
}

/// The rights and the rest of the claims (`nats` beyond them) of the renewal of credentials found as `finding`, whose
/// claims are `body`, named `name`, binding `xkey`, and `registered` by a registration; a leaf node's member among
/// `assistants` gets its human's assistant's rights.
pub(super) fn renewed_rights(
    finding: &Finding,
    body: &Value,
    name: &str,
    xkey: Option<&DeviceKey>,
    registered: bool,
    assistants: &BTreeMap<UserId, UserId>,
) -> Result<(Permissions, Value)> {
    let user = || name.parse::<UserId>().map_err(|e| anyhow!("credentials named {name:?}: {e}"));
    Ok(match &finding.kind {
        CredsKind::Ant => (Permissions::for_user(&user()?, Role::Ant), json!({})),
        CredsKind::Human => {
            let tags = human_tags(finding.device.as_ref(), xkey, registered, finding.admin, finding.web);
            (Permissions::for_human_device(&user()?, finding.web), json!({ "tags": tags }))
        }
        CredsKind::Assistant { of } => {
            (Permissions::for_assistant(&user()?, of), json!({ "tags": [AssistantOf::tag(of)] }))
        }
        CredsKind::Webhook { to, channels } => {
            let to = to.iter().cloned().map(Destination::Ant);
            let destinations: Vec<Destination> = to.chain(channels.iter().cloned().map(Destination::Channel)).collect();
            (Permissions::for_webhook(&user()?, &destinations), json!({}))
        }
        CredsKind::Observer => (Permissions::for_observer(&user()?), json!({})),
        CredsKind::Host { machine } => (Permissions::for_host(machine), json!({})),
        CredsKind::ChannelManager => {
            let ttl = u64::try_from(ants_proto::CHANADM_TTL.as_nanos())?;
            (Permissions::for_channel_manager(), json!({ "resp": { "max": 1, "ttl": ttl } }))
        }
        CredsKind::Leaf { machine, ants, humans, webhooks, enroll_approver } => {
            let members = members(ants, humans);
            let roles = members.iter().map(|(m, r)| (m, leaf_role(assistants, m, *r)));
            let mut permissions = Permissions::for_leaf(machine, roles);
            let publish = allowed(body).publish;
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
        CredsKind::LeafSystem { .. }
        | CredsKind::LeafStore { .. }
        | CredsKind::Enrollment { .. }
        | CredsKind::Bootstrap => {
            unreachable!("no registry kind")
        }
    })
}

/// `record` of renewed credentials, with what the registry recorded of the `previous` ones beyond their key: the
/// template and manager of an added ant, and the GitHub binding and grant of a registered device, unless `registered`
/// gives them anew.
pub(super) fn inherited(
    record: Issued,
    previous: Option<&Issued>,
    registered: Option<(GithubAccount, Grant)>,
) -> Issued {
    let Some(p) = previous else { return record };
    let (template, added_by) = (p.template.clone(), p.added_by.clone());
    let (github, grant) = match registered {
        Some((github, grant)) => (Some(github), Some(grant)),
        None => (p.github.clone(), p.grant.clone()),
    };
    Issued { template, added_by, github, grant, ..record }
}

/// The registry's kind of credentials of `kind`, if they may be renewed ([`renew`]).
pub(super) fn registry_kind(kind: &CredsKind) -> Option<Kind> {
    match kind {
        CredsKind::Ant => Some(Kind::Ant),
        CredsKind::Human => Some(Kind::Human),
        CredsKind::Assistant { .. } => Some(Kind::Assistant),
        CredsKind::Webhook { .. } => Some(Kind::Webhook),
        CredsKind::ChannelManager => Some(Kind::ChannelManager),
        CredsKind::Host { .. } => Some(Kind::Host),
        CredsKind::Observer => Some(Kind::Observer),
        CredsKind::Leaf { .. } => Some(Kind::Leaf),
        CredsKind::LeafSystem { .. }
        | CredsKind::LeafStore { .. }
        | CredsKind::Enrollment { .. }
        | CredsKind::Bootstrap => None,
    }
}
