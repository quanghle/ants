//! Issuing credentials: a user's or a device's, written as a `.creds` file or signed for a key made elsewhere, and
//! the services' (webhooks, the channel manager, a machine's host connection).

use std::fs;
use std::path::Path;

use ants_daemon::config::registration::{GithubAccount, Grant};
use ants_daemon::secrets::creds::Kind as CredsKind;
use ants_proto::{
    ADMIN_TAG, AssistantOf, BuiltinAnt, DeviceId, DeviceKey, JsDomain, Machine, Permissions, REGISTERED_TAG, Role,
    UserId, WEB_TAG,
};
use anyhow::{Context as _, Result, bail};
use nkeys::{KeyPair, XKey};
use serde_json::{Value, json};

use super::claims::{check_user_key, user_jwt, verified_body};
use super::files::{creds, creds_jwt, creds_with, load, seed, write_secret};
use super::uplink::allowed;
use super::{ACCOUNT_SEED, check_not_reserved};
use crate::credentials::devices;
use crate::credentials::issued::{self, Issued, Kind};

/// Credentials to issue, signed with the account key in the auth directory `dir`: those of `user`, of `kind`, with
/// `permissions`. A human's are for a new device, whose id their user JWT carries ([`DeviceId::tag`]) and the
/// registry records with `new`.
pub struct Issue<'a> {
    pub dir: &'a Path,
    pub user: &'a UserId,
    pub kind: Kind,
    pub permissions: Permissions,
    pub new: Registered<'a>,
}

impl<'a> Issue<'a> {
    /// Credentials recording nothing more than their key.
    pub fn new(dir: &'a Path, user: &'a UserId, kind: Kind, permissions: Permissions) -> Self {
        Self { dir, user, kind, permissions, new: Registered::default() }
    }

    /// A human's, for a new device labelled `label`.
    #[cfg(test)]
    pub fn labelled(self, label: Option<&'a str>) -> Self {
        Self { new: Registered { label, ..self.new }, ..self }
    }

    /// Recording `new` with the device.
    pub fn with(self, new: &Registered<'a>) -> Self {
        Self { new: new.clone(), ..self }
    }

    /// Issue them for a new user NKey, write their `.creds` file to `out` and record the key ([`issued::record`]). A
    /// human's bind a new encryption key ([`DeviceKey::tag`]), but a web device's, its seed in the file after the
    /// NKey seed. Returns the device id, for a human's.
    pub fn into_file(self, out: &Path) -> Result<Option<DeviceId>> {
        let Self { dir, user, kind, permissions, new } = self;
        check_not_reserved(user)?;
        let account = load(&dir.join(ACCOUNT_SEED))?;
        let key = KeyPair::new_user();
        let xkey = (kind == Kind::Human && !new.web).then(XKey::new);
        let bound = xkey.as_ref().map(device_key).transpose()?;
        let (device, extra) = device_claims(kind, &new, bound.as_ref())?;
        let jwt = user_jwt(&account, &key.public_key(), user.as_str(), permissions, extra, new.expires_at)?;
        write_secret(out, &creds_with(&jwt, &seed(&key)?, xkey.as_ref())?)?;
        let issued = Issued::new(&key.public_key(), user.as_str(), kind, new.expires_at)?;
        issued::record(dir, &new.record(issued, device, bound))?;
        Ok(device)
    }

    /// Sign their user JWT for `public_key`, a user NKey generated on another machine, and record the key
    /// ([`issued::record`]), binding the encryption key `xkey` generated on the device, if given; only a human's bind
    /// one. The seed stays where it was generated. Returns the JWT and the device id, for a human's.
    pub fn for_key(self, public_key: &str, xkey: Option<&DeviceKey>) -> Result<(String, Option<DeviceId>)> {
        let Self { dir, user, kind, permissions, new } = self;
        check_user_key(public_key)?;
        check_not_reserved(user)?;
        let account = load(&dir.join(ACCOUNT_SEED))?;
        let (device, extra) = device_claims(kind, &new, xkey)?;
        let jwt = user_jwt(&account, public_key, user.as_str(), permissions, extra, new.expires_at)?;
        let issued = Issued::new(public_key, user.as_str(), kind, new.expires_at)?;
        issued::record(dir, &new.record(issued, device, xkey.cloned()))?;
        Ok((jwt, device))
    }
}

/// What the registry records of new credentials besides their key, a human's new device above all, and when they
/// expire.
#[derive(Debug, Clone, Default)]
pub struct Registered<'a> {
    pub label: Option<&'a str>,
    /// The GitHub user the human's name is bound to ([`crate::signing::binding`]).
    pub github: Option<GithubAccount>,
    /// Why `ants signer` signed it.
    pub grant: Option<Grant>,
    /// Seconds since the epoch; none for credentials that never expire.
    pub expires_at: Option<u64>,
    /// Whether a human's credentials carry the admin role ([`ADMIN_TAG`]).
    pub admin: bool,
    /// Whether a human's credentials are a web device's ([`WEB_TAG`]), with [`Permissions::for_web`]: never an
    /// admin's, and binding no encryption key.
    pub web: bool,
    /// The human whose assistant an ant's credentials ([`Kind::Assistant`]) make it ([`AssistantOf`]).
    pub assistant_of: Option<UserId>,
    /// The template a manager added an ant from ([`Issued::template`]).
    pub template: Option<&'a str>,
    /// The manager ant that added an ant from `template`.
    pub added_by: Option<UserId>,
    /// The admin order that added an ant on its machine ([`Issued::order`]).
    pub order: Option<ants_proto::Uuid>,
}

impl Registered<'_> {
    fn record(&self, issued: Issued, device: Option<DeviceId>, xkey: Option<DeviceKey>) -> Issued {
        let issued = with_device(issued, device, self.label, xkey);
        Issued {
            github: self.github.clone(),
            grant: self.grant.clone(),
            admin: self.admin,
            web: self.web,
            assistant_of: self.assistant_of.clone(),
            template: self.template.map(str::to_owned),
            added_by: self.added_by.clone(),
            order: self.order,
            ..issued
        }
    }
}

/// The public key of `xkey`, as a user JWT binds it.
pub(crate) fn device_key(xkey: &XKey) -> Result<DeviceKey> {
    Ok(xkey.public_key().parse()?)
}

/// A new device for credentials of `kind`, if a human's, and the claims carrying it with its encryption key `xkey`,
/// marked [`REGISTERED_TAG`] when a registration grants it (`new.grant`), [`ADMIN_TAG`] for an admin (`new.admin`)
/// and [`WEB_TAG`] for a web device (`new.web`), which is never an admin's and binds no encryption key; an
/// assistant's carry its human's [`AssistantOf::tag`] instead.
fn device_claims(kind: Kind, new: &Registered<'_>, xkey: Option<&DeviceKey>) -> Result<(Option<DeviceId>, Value)> {
    if new.admin && kind != Kind::Human {
        bail!("only a human's credentials carry the admin role");
    }
    if new.web {
        if kind != Kind::Human {
            bail!("only a human's device is a web device");
        }
        if new.admin {
            bail!("a web device's credentials never carry the admin role");
        }
        if let Some(xkey) = xkey {
            bail!("a web device's credentials bind no encryption key: {xkey} refused");
        }
    }
    match (kind, &new.assistant_of) {
        (Kind::Assistant, None) => bail!("an assistant's credentials name the human it assists"),
        (Kind::Assistant, Some(_)) | (_, None) => {}
        (_, Some(_)) => bail!("only an assistant's credentials name a human it assists"),
    }
    if kind != Kind::Human {
        if new.label.is_some() {
            bail!("only a human's credentials are for a device with a label");
        }
        if xkey.is_some() {
            bail!("only a human's credentials bind an encryption key");
        }
        let tags: Vec<String> = new.assistant_of.iter().map(AssistantOf::tag).collect();
        return Ok((None, if tags.is_empty() { json!({}) } else { json!({ "tags": tags }) }));
    }
    new.label.map(devices::check_label).transpose()?;
    let device = DeviceId::generate();
    let tags = human_tags(Some(&device), xkey, new.grant.is_some(), new.admin, new.web);
    Ok((Some(device), json!({ "tags": tags })))
}

/// The tags of a human's credentials: its `device` if it has one and the encryption key `xkey` if it binds one, then
/// [`REGISTERED_TAG`] when a registration granted them, [`ADMIN_TAG`] for an admin's and [`WEB_TAG`] for a web
/// device's.
pub(super) fn human_tags(
    device: Option<&DeviceId>,
    xkey: Option<&DeviceKey>,
    registered: bool,
    admin: bool,
    web: bool,
) -> Vec<String> {
    let tags = device.map(DeviceId::tag).into_iter().chain(xkey.map(DeviceKey::tag));
    let tags = tags.chain(registered.then(|| REGISTERED_TAG.to_owned()));
    let tags = tags.chain(admin.then(|| ADMIN_TAG.to_owned()));
    tags.chain(web.then(|| WEB_TAG.to_owned())).collect()
}

fn with_device(issued: Issued, device: Option<DeviceId>, label: Option<&str>, xkey: Option<DeviceKey>) -> Issued {
    match device {
        Some(device) => issued.for_device(device, label.map(str::to_owned), xkey),
        None => issued,
    }
}

/// The permissions of the credentials of `webhook` in `dir`, `<webhook>.creds` as `ants auth user WEBHOOK --webhook`
/// writes them ([`Permissions::for_webhook`]): issued by the bus's account, for `webhook`, hearing only its own reply
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
        bail!("{} were not issued with the account keys in {}", path.display(), dir.display());
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

/// Issue ordinary ant rights for a reserved built-in ID.
pub fn issue_builtin(dir: &Path, builtin: BuiltinAnt, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    let key = KeyPair::new_user();
    let jwt = sign_builtin(dir, builtin, js_domain, &key.public_key())?;
    write_secret(out, &creds(&jwt, &seed(&key)?))
}

pub fn sign_builtin(dir: &Path, builtin: BuiltinAnt, js_domain: Option<&JsDomain>, public_key: &str) -> Result<String> {
    check_user_key(public_key)?;
    let name = builtin.id();
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let permissions = Permissions::for_user(&name, Role::Ant).with_js_domain(js_domain);
    let jwt = user_jwt(&account, public_key, name.as_str(), permissions, json!({}), None)?;
    issued::record(dir, &Issued::new(public_key, name.as_str(), Kind::Ant, None)?)?;
    Ok(jwt)
}

/// Issue the credentials of the channel manager, `ants channel serve`, for the hub's JetStream domain if it has one,
/// and write its `.creds` file to `out`: it hears channel management requests and answers each once, reads the colony
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
    issued::record(dir, &Issued::new(public_key, name.as_str(), Kind::ChannelManager, None)?)?;
    Ok(jwt)
}

/// Issue the host credentials of `machine`, for the hub's JetStream domain if it has one, and write their `.creds`
/// file to `out`: with them the daemon serving the machine's ants records them in [`ants_proto::MACHINES_BUCKET`] and
/// takes their admin orders ([`Permissions::for_host`]). They never expire.
pub fn issue_host(dir: &Path, machine: &Machine, js_domain: Option<&JsDomain>, out: &Path) -> Result<()> {
    let key = KeyPair::new_user();
    let jwt = sign_host(dir, machine, js_domain, &key.public_key())?;
    write_secret(out, &creds(&jwt, &seed(&key)?))
}

/// Sign the user JWT of the host of `machine` for `public_key`, with the permissions [`issue_host`] gives, and record
/// its key.
pub fn sign_host(dir: &Path, machine: &Machine, js_domain: Option<&JsDomain>, public_key: &str) -> Result<String> {
    check_user_key(public_key)?;
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let permissions = Permissions::for_host(machine).with_js_domain(js_domain);
    let name = ants_proto::host_user(machine);
    let jwt = user_jwt(&account, public_key, &name, permissions, json!({}), None)?;
    issued::record(dir, &Issued::new(public_key, &name, Kind::Host, None)?)?;
    Ok(jwt)
}

/// Sign the user JWT of `user` in `role` for `public_key`, a user NKey generated on another machine, with the
/// permissions `issue` gives. The seed stays where it was generated; its `.creds` file is the JWT plus that seed.
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
/// machine, and record the key ([`issued::record`]). A human's are for a new device ([`Issue::for_key`]).
pub fn sign_with(dir: &Path, user: &UserId, kind: Kind, permissions: Permissions, public_key: &str) -> Result<String> {
    Issue::new(dir, user, kind, permissions).for_key(public_key, None).map(|(jwt, _)| jwt)
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
    let permissions = Permissions::for_user(ant, Role::Ant).with_js_domain(js_domain);
    let new = Registered { template: Some(template), added_by: Some(manager.clone()), ..Registered::default() };
    Issue { dir, user: ant, kind: Kind::Ant, permissions, new }.for_key(public_key, None).map(|(jwt, _)| jwt)
}

/// Sign the user JWT of `ant`, added on its machine by the admin order `order`, for `public_key`, with the permissions
/// of an ant, or, with `assistant_of`, of the assistant of that human ([`AssistantOf`]), and record the key with the
/// order ([`Issued::order`]).
pub fn sign_ordered(
    dir: &Path,
    ant: &UserId,
    js_domain: Option<&JsDomain>,
    public_key: &str,
    order: ants_proto::Uuid,
    assistant_of: Option<&UserId>,
) -> Result<String> {
    let (permissions, kind) = match assistant_of {
        Some(human) => (Permissions::for_assistant(ant, human), Kind::Assistant),
        None => (Permissions::for_user(ant, Role::Ant), Kind::Ant),
    };
    let permissions = permissions.with_js_domain(js_domain);
    let new = Registered { order: Some(order), assistant_of: assistant_of.cloned(), ..Registered::default() };
    Issue { dir, user: ant, kind, permissions, new }.for_key(public_key, None).map(|(jwt, _)| jwt)
}

/// `permissions` of credentials of `kind` for the hub's JetStream domain `domain`, if it has one: a leaf node's uplink
/// allows the domain form only ([`Permissions::with_uplink_js_domain`]), any other credentials both.
pub(super) fn in_domain(permissions: Permissions, kind: &CredsKind, domain: Option<&JsDomain>) -> Permissions {
    match kind {
        CredsKind::Leaf { .. } => permissions.with_uplink_js_domain(domain),
        _ => permissions.with_js_domain(domain),
    }
}
