//! Checking the credentials an ants process uses: whether `ants auth` issued them with the permissions of this
//! version ([`PERMISSIONS_VERSION`]) and whether they expire soon, each with the command that renews them: `ants renew`
//! over NATS for credentials that may ask for it ([`Finding::renewable`]), else `ants auth` on the machine holding the
//! team keys.
//!
//! The kind of credentials, and what renewing them takes, comes from the permissions their user JWT carries.

use std::path::Path;
use std::time::Duration;

use ants_proto::{
    AssistantOf, ChannelId, DeviceId, DeviceKey, ENROLL_INBOX, JsDomain, Machine, PERMISSIONS_VERSION,
    PermissionsVersion, UserId, WEBHOOKS_BUCKET, inbox_prefix, is_admin, subject, tagged_registered,
};
use anyhow::{Context as _, Result, anyhow, bail};
use data_encoding::BASE64URL_NOPAD;
use nkeys::{KeyPair, XKey};
use serde::Serialize;
use serde_json::Value;

use crate::keystore::{self, Store};

/// Credentials expiring within this long get a warning.
pub const EXPIRY_WARNING: Duration = Duration::from_hours(7 * 24);

/// How often a daemon checks its credentials again ([`recheck`]).
pub const RECHECK_EVERY: Duration = Duration::from_hours(24);

/// What credentials are for, with what renewing them takes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Kind {
    Ant,
    Human,
    /// An ant that is the assistant `of` a human ([`ants_proto::AssistantOf`]), with
    /// [`ants_proto::Permissions::for_assistant`].
    Assistant {
        of: UserId,
    },
    /// A webhook of `ants webhook serve`, delivering to the ants of `to` and the channels of `channels`.
    Webhook {
        to: Vec<UserId>,
        channels: Vec<ChannelId>,
    },
    ChannelManager,
    /// Read-only credentials of a hook observing the team's events ([`ants_proto::Permissions::for_observer`]).
    Observer,
    /// The uplink of `machine`'s leaf node, carrying what its members may do.
    Leaf {
        machine: Machine,
        ants: Vec<UserId>,
        humans: Vec<UserId>,
        webhooks: Vec<UserId>,
        enroll_approver: bool,
    },
    /// The system account uplink of `machine`'s leaf node, which `ants auth leaf` writes beside its uplink's
    /// credentials and through which `ants auth push` reaches the leaf.
    LeafSystem {
        machine: Machine,
    },
    /// The host credentials of `machine`, with which its daemon writes the machine's record and takes its admin
    /// orders ([`ants_proto::Permissions::for_host`]).
    Host {
        machine: Machine,
    },
    /// Enrollment credentials of `ant`, for `ants add --enroll`.
    Enrollment {
        ant: UserId,
    },
    /// The team's public bootstrap credentials, for `ants login` ([`ants_proto::Permissions::for_bootstrap`]).
    Bootstrap,
}

/// How the permissions version of credentials compares with [`PERMISSIONS_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum VersionStatus {
    Current,
    /// Issued with an older version: what the role allows since may be refused.
    Outdated,
    /// Issued by a newer ants.
    Newer,
}

/// When credentials expire, seen at some time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Expiry {
    Never,
    Later,
    /// Within [`EXPIRY_WARNING`].
    Soon,
    Expired,
}

/// What a check of credentials found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// The name their user JWT carries.
    pub name: String,
    /// Their public key.
    pub key: String,
    #[serde(flatten)]
    pub kind: Kind,
    /// The permissions version they carry.
    pub version: PermissionsVersion,
    /// Seconds since the epoch; none for credentials that never expire.
    pub expires_at: Option<u64>,
    /// The hub's JetStream domain they were issued for, if any.
    pub js_domain: Option<JsDomain>,
    /// The human's device they are for ([`DeviceId::tag`]), which a human's user JWT names; none for others.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
    /// The encryption key of the human's device ([`DeviceKey::tag`]), if its user JWT binds one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xkey: Option<DeviceKey>,
    /// Whether a registration granted them (`ants login`), as their user JWT says
    /// ([`ants_proto::REGISTERED_TAG`]): `ants signer` renews them, not `ants enroll renew`.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub registered: bool,
    /// Whether they are a human's carrying the admin role ([`ants_proto::ADMIN_TAG`]).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub admin: bool,
}

impl Finding {
    /// The finding of the user JWT in a `.creds` file's text.
    pub fn from_creds(text: &str) -> Result<Self> {
        Self::from_jwt(creds_jwt(text).context("no user JWT")?)
    }

    /// The finding of a user JWT.
    pub fn from_jwt(jwt: &str) -> Result<Self> {
        Self::from_claims(&claims(jwt)?)
    }

    /// The finding of a user JWT's claims.
    pub fn from_claims(claims: &Value) -> Result<Self> {
        let text = |field: &str| claims[field].as_str().map(str::to_owned).with_context(|| format!("no {field}"));
        let (name, key) = (text("name")?, text("sub")?);
        let nats = &claims["nats"];
        let list = |value: &Value| -> Vec<String> {
            value.as_array().into_iter().flatten().filter_map(|s| s.as_str().map(str::to_owned)).collect()
        };
        let (publish, subscribe) = (list(&nats["pub"]["allow"]), list(&nats["sub"]["allow"]));
        let tags = list(&nats["tags"]);
        let leaf_only = {
            let types = list(&nats["allowed_connection_types"]);
            !types.is_empty() && types.iter().all(|t| t.starts_with("LEAFNODE"))
        };
        let mut kind = kind(&name, &publish, &subscribe, leaf_only)?;
        let tag_list = || tags.iter().map(String::as_str);
        match (AssistantOf::from_tags(tag_list()), &kind) {
            // An ant's rights and the traffic of the whole team to watch, as a human.
            (Some(of), Kind::Ant) if subscribe.iter().any(|s| s == subject::ALL) => kind = Kind::Assistant { of },
            (Some(of), _) => bail!("{name}'s credentials say it assists {of} without an assistant's permissions"),
            (None, _) => {}
        }
        let device = DeviceId::from_tags(tag_list());
        if kind == Kind::Human && device.is_none() {
            bail!("a human's credentials without a device id");
        }
        let registered = kind == Kind::Human && tagged_registered(tag_list());
        let admin = kind == Kind::Human && is_admin(tag_list());
        Ok(Self {
            kind,
            version: PermissionsVersion::from_tags(tags.iter().map(String::as_str))
                .context("no permissions version")?,
            expires_at: claims["exp"].as_u64(),
            js_domain: js_domain(&publish),
            device,
            xkey: DeviceKey::from_tags(tags.iter().map(String::as_str)),
            registered,
            admin,
            name,
            key,
        })
    }

    pub fn version_status(&self) -> VersionStatus {
        match self.version.cmp(&PERMISSIONS_VERSION) {
            std::cmp::Ordering::Equal => VersionStatus::Current,
            std::cmp::Ordering::Greater => VersionStatus::Newer,
            std::cmp::Ordering::Less => VersionStatus::Outdated,
        }
    }

    /// When they expire, seen at `now` (seconds since the epoch).
    pub fn expiry(&self, now: u64) -> Expiry {
        match self.expires_at {
            None => Expiry::Never,
            Some(exp) if exp <= now => Expiry::Expired,
            Some(exp) if exp - now <= EXPIRY_WARNING.as_secs() => Expiry::Soon,
            Some(_) => Expiry::Later,
        }
    }

    /// Who they are for, in words: `ant dev-a`, `the leaf node uplink of m1`, ...
    pub fn holder(&self) -> String {
        match &self.kind {
            Kind::Ant => format!("ant {}", self.name),
            Kind::Human if self.admin => format!("human {} (admin)", self.name),
            Kind::Human => format!("human {}", self.name),
            Kind::Assistant { of } => format!("ant {}, assistant of {of}", self.name),
            Kind::Webhook { .. } => format!("webhook {}", self.name),
            Kind::ChannelManager => "the channel manager".to_owned(),
            Kind::Observer => format!("observer {}", self.name),
            Kind::Leaf { machine, .. } => format!("the leaf node uplink of {machine}"),
            Kind::LeafSystem { machine } => format!("the system account uplink of the leaf node of {machine}"),
            Kind::Host { machine } => format!("the host of {machine}"),
            Kind::Enrollment { ant } => format!("the enrollment of {ant}"),
            Kind::Bootstrap => "the bootstrap credentials".to_owned(),
        }
    }

    /// The commands that renew them, in order, all but `ants leaf outbox` on the machine holding the team keys.
    pub fn renew(&self) -> Vec<String> {
        let domain = self.js_domain.as_ref().map_or_default(|d| format!(" --js-domain {d}"));
        let flags = |flag: &str, ids: &[UserId]| ids.iter().map(|id| format!(" --{flag} {id}")).collect::<String>();
        match &self.kind {
            Kind::Ant => vec![format!("ants auth user {}{domain}", self.name)],
            Kind::Human => {
                let admin = if self.admin { " --admin" } else { "" };
                vec![format!("ants auth user {} --human{admin}{domain}", self.name)]
            }
            Kind::Assistant { of } => vec![format!("ants auth user {} --assistant-of {of}{domain}", self.name)],
            Kind::Webhook { to, channels } => {
                let channels: String = channels.iter().map(|c| format!(" --channel {c}")).collect();
                vec![format!("ants auth user {} --webhook{}{channels}{domain}", self.name, flags("to", to))]
            }
            Kind::ChannelManager => vec![format!("ants auth channel-manager{domain}")],
            Kind::Observer => vec![format!("ants auth user {} --observer{domain}", self.name)],
            Kind::Leaf { machine, ants, humans, webhooks, enroll_approver } => {
                let members = format!("{}{}", flags("ant", ants), flags("human", humans));
                let approver = if *enroll_approver { " --enroll-approver" } else { "" };
                let mut renew =
                    vec![format!("ants auth leaf {machine}{members}{}{approver}{domain}", flags("webhook", webhooks))];
                if !members.is_empty() {
                    renew.push(format!("ants leaf outbox {machine}{members}{domain}"));
                }
                renew
            }
            // They carry nothing of the members: those are in the uplink's credentials, leaf-<machine>.creds.
            Kind::LeafSystem { machine } => vec![format!(
                "ants auth leaf {machine} --ant ANT... --human HUMAN... --webhook WEBHOOK... [--js-domain DOMAIN]"
            )],
            Kind::Host { machine } => vec![format!("ants auth host {machine}{domain}")],
            Kind::Enrollment { ant } => vec![format!("ants auth enroll {ant}")],
            Kind::Bootstrap => vec!["ants auth bootstrap".to_owned()],
        }
    }

    /// Whether `other` are the same credentials as these, for another key or with other permissions: the same name,
    /// kind (members in any order, the human of an assistant), device, admin role and JetStream domain, as renewing
    /// keeps them.
    pub fn same_credentials(&self, other: &Finding) -> bool {
        fn sorted(kind: &Kind) -> Kind {
            let mut kind = kind.clone();
            match &mut kind {
                Kind::Webhook { to, channels } => {
                    to.sort();
                    channels.sort();
                }
                Kind::Leaf { ants, humans, webhooks, .. } => {
                    ants.sort();
                    humans.sort();
                    webhooks.sort();
                }
                _ => {}
            }
            kind
        }
        self.name == other.name
            && sorted(&self.kind) == sorted(&other.kind)
            && self.device == other.device
            && self.admin == other.admin
            && self.js_domain == other.js_domain
    }

    /// Whether they may ask for their own renewal over NATS at `now` (`ants renew`): those of an ant, a human, a
    /// webhook, the channel manager or a leaf node's uplink, not expired, as the approver renews only credentials still
    /// valid.
    pub fn renewable(&self, now: u64) -> bool {
        let kind = !matches!(
            self.kind,
            Kind::LeafSystem { .. } | Kind::Host { .. } | Kind::Enrollment { .. } | Kind::Bootstrap
        );
        kind && self.expiry(now) != Expiry::Expired
    }

    /// The commands renewing them over NATS when they are at `path`: `ants renew` where they are used, then
    /// `ants enroll renew` on the machine holding the account key, which approves it, but for credentials a
    /// registration granted ([`Finding::registered`]), which `ants signer` there renews without a command.
    pub fn renew_over_nats(&self, path: &Path) -> Vec<String> {
        let renew = format!("ants renew {}", path.display());
        if self.registered {
            return vec![renew];
        }
        vec![renew, format!("ants enroll renew {}", self.name)]
    }

    /// How to renew them, in words: over NATS ([`Finding::renew_over_nats`]) when they are at `path` and
    /// [`Finding::renewable`] at `now`, else naming [`Finding::renew`], and `ants login` for credentials a registration
    /// granted.
    pub fn renew_hint(&self, path: Option<&Path>, now: u64) -> String {
        if let Some(path) = path.filter(|_| self.renewable(now)) {
            if self.registered {
                return format!(
                    "renew them with `ants renew {}`, which `ants signer` answers for a device registered with \
                     `ants login`",
                    path.display()
                );
            }
            return format!(
                "renew them with `ants renew {}`, approved by `ants enroll renew {}` on the machine holding the account \
                 key",
                path.display(),
                self.name
            );
        }
        let renew = self.renew();
        let quoted: Vec<String> = renew.iter().map(|c| format!("`{c}`")).collect();
        match (&self.kind, quoted.as_slice()) {
            (Kind::Leaf { .. }, [issue, outbox]) => format!(
                "renew them on the machine holding the team keys with {issue}, put the new file in their place, \
                 restart the leaf node, then run {outbox} as a human"
            ),
            (Kind::LeafSystem { machine }, _) => format!(
                "renew them on the machine holding the team keys with {}, naming the members of the uplink \
                 credentials leaf-{machine}.creds, which it renews too, put both new files in their place and restart \
                 the leaf node",
                quoted.join(" and ")
            ),
            (Kind::Leaf { .. }, _) => format!(
                "renew them on the machine holding the team keys with {}, put the new file in their place and \
                 restart the leaf node",
                quoted.join(" and ")
            ),
            _ if self.registered => format!(
                "register the device again with `ants login --hub URL --name {}`, or renew them on the machine holding \
                 the team keys with {} and put the new file in their place",
                self.name,
                quoted.join(" and ")
            ),
            _ => format!(
                "renew them on the machine holding the team keys with {} and put the new file in their place",
                quoted.join(" and ")
            ),
        }
    }

    /// When to renew outdated credentials of a service, which other machines may still run an older ants depending on
    /// rights the renewal withdraws: empty for a human's and enrollment credentials.
    pub fn renew_when(&self) -> &'static str {
        match self.kind {
            Kind::Human | Kind::Enrollment { .. } | Kind::Bootstrap => "",
            _ => {
                ", once `ants doctor` run as a human finds every daemon, `ants webhook serve` and `ants channel \
                 serve` at this permissions version (its `versions` check)"
            }
        }
    }

    /// What deserves a warning at `now`: an outdated or newer permissions version, and an expiry passed or within
    /// [`EXPIRY_WARNING`], except for enrollment credentials, short-lived by design, which only expire. One line each,
    /// with the [`Finding::renew_hint`] of credentials at `path`.
    pub fn warnings(&self, path: Option<&Path>, now: u64) -> Vec<String> {
        let mut warnings = Vec::new();
        let holder = self.holder();
        let version = self.version;
        match self.version_status() {
            VersionStatus::Outdated => warnings.push(format!(
                "the credentials of {holder} carry permissions version {version}, older than version \
                 {PERMISSIONS_VERSION} of this ants: requests their role allows now may be refused; {}{}",
                self.renew_hint(path, now),
                self.renew_when()
            )),
            VersionStatus::Newer => warnings.push(format!(
                "the credentials of {holder} carry permissions version {version}, newer than version \
                 {PERMISSIONS_VERSION} of this ants: upgrade ants"
            )),
            VersionStatus::Current => {}
        }
        let at = || self.expires_at.map_or_default(time);
        match self.expiry(now) {
            Expiry::Expired => warnings.push(format!(
                "the credentials of {holder} expired at {}; {}",
                at(),
                self.renew_hint(path, now)
            )),
            Expiry::Soon if !matches!(self.kind, Kind::Enrollment { .. }) => {
                warnings.push(format!(
                    "the credentials of {holder} expire at {}; {}",
                    at(),
                    self.renew_hint(path, now)
                ));
            }
            _ => {}
        }
        warnings
    }
}

/// Check the credentials file at `path` ([`Finding`]).
pub fn check(path: &Path) -> Result<Finding> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    Finding::from_creds(&text).with_context(|| format!("credentials {}", path.display()))
}

/// Whether the credentials file at `path` holds an assistant's credentials ([`Kind::Assistant`]); false without one,
/// or for one that does not check.
pub fn is_assistant(path: Option<&Path>) -> bool {
    path.is_some_and(|path| check(path).is_ok_and(|finding| matches!(finding.kind, Kind::Assistant { .. })))
}

/// The account that signed the credentials file at `path`: the issuer of its user JWT.
pub fn account(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let jwt = creds_jwt(&text).with_context(|| format!("credentials {} without a user JWT", path.display()))?;
    let claims = claims(jwt).with_context(|| format!("credentials {}", path.display()))?;
    claims["iss"]
        .as_str()
        .map(str::to_owned)
        .with_context(|| format!("credentials {} without an issuer", path.display()))
}

/// The claims of a user JWT.
fn claims(jwt: &str) -> Result<Value> {
    let payload = jwt.trim().split('.').nth(1).context("a malformed user JWT")?;
    let payload = BASE64URL_NOPAD
        .decode(payload.trim_end_matches('=').as_bytes())
        .map_err(|e| anyhow!("a malformed user JWT: {e}"))?;
    serde_json::from_slice(&payload).context("a malformed user JWT")
}

/// Log a warning for each of [`Finding::warnings`] of the credentials file at `path`; one that cannot be read is
/// left for the connection to report.
pub fn warn(path: &Path) {
    match (check(path), now()) {
        (Ok(finding), Some(now)) => {
            for warning in finding.warnings(Some(path), now) {
                tracing::warn!(creds = %path.display(), "{warning}");
            }
        }
        (Err(e), _) => tracing::debug!("checking credentials: {e:#}"),
        (_, None) => {}
    }
}

/// [`warn`] about the credentials file at `path` every [`RECHECK_EVERY`], for as long as it runs; never, without
/// one.
pub async fn recheck(path: Option<&Path>) {
    let Some(path) = path else { return std::future::pending().await };
    loop {
        tokio::time::sleep(RECHECK_EVERY).await;
        warn(path);
    }
}

/// A time in seconds since the epoch, as RFC 3339.
pub fn time(secs: u64) -> String {
    i64::try_from(secs)
        .ok()
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .map_or_else(|| secs.to_string(), |t| t.to_rfc3339())
}

fn now() -> Option<u64> {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_secs())
}

/// The user JWT of the credentials at `path`.
pub fn jwt(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    creds_jwt(&text).map(str::to_owned).with_context(|| format!("no user JWT in {}", path.display()))
}

/// The user JWT of a `.creds` file's text: the line after the `BEGIN NATS USER JWT` marker.
fn creds_jwt(creds: &str) -> Option<&str> {
    creds_block(creds, "BEGIN NATS USER JWT")
}

const SEED_MARKER: &str = "BEGIN USER NKEY SEED";
const STORED_MARKER: &str = "BEGIN ANTS STORED NKEY";
const XKEY_SEED_MARKER: &str = "BEGIN ANTS XKEY SEED";
const STORED_XKEY_MARKER: &str = "BEGIN ANTS STORED XKEY";

fn creds_block<'a>(creds: &'a str, marker: &str) -> Option<&'a str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains(marker))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
}

/// Where the NKey seed of a `.creds` file is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seed<'a> {
    /// In the file, as written by `ants auth` and nats tools.
    File(&'a str),
    /// In the OS credential store ([`keystore`]), under this public key.
    Stored(&'a str),
}

/// Where the seed of a `.creds` file's text is, if it says.
pub fn seed_of(creds: &str) -> Option<Seed<'_>> {
    creds_block(creds, SEED_MARKER).map(Seed::File).or_else(|| creds_block(creds, STORED_MARKER).map(Seed::Stored))
}

/// Where the encryption key seed of a human's `.creds` file text is, if it says: in an `ANTS XKEY SEED` block after
/// the NKey seed, or in the OS credential store under the public key of an `ANTS STORED XKEY` block.
pub fn xkey_of(creds: &str) -> Option<Seed<'_>> {
    creds_block(creds, XKEY_SEED_MARKER)
        .map(Seed::File)
        .or_else(|| creds_block(creds, STORED_XKEY_MARKER).map(Seed::Stored))
}

/// The block keeping an encryption key seed, or the public key of the one stored, appended to the text of a `.creds`
/// file. nats tools read only a file's first two blocks, the user JWT and the NKey seed, so they ignore it.
pub fn xkey_block(seed: Seed<'_>) -> String {
    match seed {
        Seed::File(seed) => format!(
            "\n************************* IMPORTANT *************************\n\
             The XKEY seed below opens the messages sealed for this device: only ants reads it.\n\n\
             -----BEGIN ANTS XKEY SEED-----\n{seed}\n------END ANTS XKEY SEED------\n"
        ),
        Seed::Stored(key) => format!(
            "\n************************* IMPORTANT *************************\n\
             The XKEY seed of this device is in the OS credential store under the public key below.\n\n\
             -----BEGIN ANTS STORED XKEY-----\n{key}\n------END ANTS STORED XKEY------\n"
        ),
    }
}

/// The text of a `.creds` file holding `jwt` and `seed`, as nats tools write it.
pub fn text(jwt: &str, seed: &str) -> String {
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
         ************************* IMPORTANT *************************\n\
         NKEY Seed printed below can be used to sign and prove identity.\n\
         NKEYs are sensitive and should be treated as secrets.\n\n\
         -----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n\n\
         *************************************************************\n"
    )
}

/// The text of a `.creds` file holding `jwt`, whose seed is in the OS credential store under the public key `key`.
pub fn stored_text(jwt: &str, key: &str) -> String {
    format!(
        "-----BEGIN NATS USER JWT-----\n{jwt}\n------END NATS USER JWT------\n\n\
         ************************* IMPORTANT *************************\n\
         The NKEY seed of this user is not in this file but in the OS credential store,\n\
         under the service \"{service}\" and the public key below: only ants reads it.\n\
         `ants keys unstore FILE` moves it back into the file.\n\n\
         -----BEGIN ANTS STORED NKEY-----\n{key}\n------END ANTS STORED NKEY------\n\n\
         *************************************************************\n",
        service = keystore::SERVICE
    )
}

/// The user JWT and key of the `.creds` file at `path`, whose seed is in the file or in `store`. A seed stored for
/// another key than the JWT's, or none, is an error, as is a store that cannot be read; none names the seed.
pub fn load(path: &Path, store: &dyn Store) -> Result<(String, KeyPair)> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let jwt = creds_jwt(&text).with_context(|| format!("no user JWT in {}", path.display()))?;
    let key = match seed_of(&text) {
        Some(Seed::File(seed)) => {
            KeyPair::from_seed(seed).map_err(|e| anyhow!("the seed in {}: {e}", path.display()))?
        }
        Some(Seed::Stored(public)) => {
            let named = Finding::from_jwt(jwt).with_context(|| format!("credentials {}", path.display()))?.key;
            if named != public {
                bail!("{} name the stored key {public}, not the key {named} of their user JWT", path.display());
            }
            let seed = store
                .get(public)
                .with_context(|| {
                    format!("reading the seed of {} ({public}) from the OS credential store", path.display())
                })?
                .with_context(|| {
                    format!(
                        "the OS credential store holds no seed for {public}, which {} name: restore it from a backup \
                         of the store, or have the credentials issued again",
                        path.display()
                    )
                })?;
            let key = KeyPair::from_seed(seed.trim())
                .map_err(|e| anyhow!("the seed stored for {public} ({}): {e}", path.display()))?;
            if key.public_key() != public {
                bail!("the OS credential store holds the seed of another key under {public} ({})", path.display());
            }
            key
        }
        None => bail!("no seed in {}, nor the key of one in the OS credential store", path.display()),
    };
    Ok((jwt.to_owned(), key))
}

/// The encryption key of the `.creds` file at `path`, whose seed is in the file or in `store`, if its user JWT binds
/// one ([`DeviceKey::tag`]). A seed of another key than the JWT's, a missing seed, a seed the JWT does not bind and a
/// store that cannot be read are errors; none names the seed.
pub fn load_xkey(path: &Path, store: &dyn Store) -> Result<Option<XKey>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let bound = Finding::from_creds(&text).with_context(|| format!("credentials {}", path.display()))?.xkey;
    let (seed, bound) = match (xkey_of(&text), bound) {
        (None, None) => return Ok(None),
        (None, Some(bound)) => bail!(
            "no seed of the encryption key {bound} in {}, nor the key of one in the OS credential store",
            path.display()
        ),
        (Some(_), None) => bail!("{} hold an encryption key their user JWT does not bind", path.display()),
        (Some(Seed::File(seed)), Some(bound)) => (seed.to_owned(), bound),
        (Some(Seed::Stored(public)), Some(bound)) => {
            let seed = store
                .get(public)
                .with_context(|| format!("reading the encryption key seed of {} ({public})", path.display()))?
                .with_context(|| {
                    format!("the OS credential store holds no seed for {public}, which {} name", path.display())
                })?;
            (seed, bound)
        }
    };
    let key =
        XKey::from_seed(seed.trim()).map_err(|e| anyhow!("the encryption key seed of {}: {e}", path.display()))?;
    if key.public_key() != bound.as_str() {
        bail!("the encryption key seed of {} is not that of the key {bound} their user JWT binds", path.display());
    }
    Ok(Some(key))
}

/// The kind of credentials named `name` with these permissions; `leaf_only` when only a leaf node may connect with
/// them.
fn kind(name: &str, publish: &[String], subscribe: &[String], leaf_only: bool) -> Result<Kind> {
    let has = |list: &[String], s: &str| list.iter().any(|one| one == s);
    if leaf_only {
        let machine = name.trim_prefix("leaf-");
        let machine: Machine = machine.parse().map_err(|e| anyhow!("a leaf node's credentials named {name}: {e}"))?;
        if !subscribe.is_empty() && subscribe.iter().all(|s| s.starts_with("$SYS.")) {
            return Ok(Kind::LeafSystem { machine });
        }
        let users = subscribe.iter().filter_map(|s| s.strip_prefix("_INBOX.")?.strip_suffix(".>")?.parse().ok());
        // With a JetStream domain, an uplink allows KV writes in their domain form only.
        let domain = js_domain(publish);
        let has_kv = |kv: &str| {
            has(publish, kv) || domain.as_ref().is_some_and(|domain| has(publish, &format!("$JS.{domain}.API.{kv}")))
        };
        let (mut ants, mut humans, mut webhooks) = (Vec::new(), Vec::new(), Vec::new());
        for user in users {
            let user: UserId = user;
            if has_kv(&subject::colony(&user)) {
                ants.push(user);
            } else if has_kv(&format!("$KV.{WEBHOOKS_BUCKET}.{user}.>")) {
                webhooks.push(user);
            } else {
                humans.push(user);
            }
        }
        let enroll_approver = has(subscribe, &format!("{ENROLL_INBOX}.>"));
        return Ok(Kind::Leaf { machine, ants, humans, webhooks, enroll_approver });
    }
    if let Some(machine) = name.strip_prefix("host-").and_then(|machine| machine.parse::<Machine>().ok())
        && has(publish, &subject::machine_record(&machine))
    {
        return Ok(Kind::Host { machine });
    }
    let user: UserId = name.parse().map_err(|e| anyhow!("credentials named {name:?}: {e}"))?;
    if user == UserId::channel_manager() {
        return Ok(Kind::ChannelManager);
    }
    if name == ants_proto::BOOTSTRAP_USER && publish == [subject::ALL_LOGIN_REQUESTS] {
        return Ok(Kind::Bootstrap);
    }
    if let Some(ant) = name.strip_prefix("enroll-")
        && !publish.is_empty()
        && publish.iter().all(|s| s.starts_with("ants.enroll."))
    {
        return Ok(Kind::Enrollment { ant: ant.parse().map_err(|e| anyhow!("enrollment credentials {name}: {e}"))? });
    }
    if ants_proto::writes_human_entries(publish.iter().map(String::as_str)) {
        return Ok(Kind::Human);
    }
    if has(subscribe, subject::DEAD_ALL) {
        return Ok(Kind::Observer);
    }
    let (inbox, renewal) = (format!("{}.>", inbox_prefix(&user)), subject::renew_reply(name));
    if has(subscribe, &inbox) && subscribe.iter().all(|s| *s == inbox || *s == renewal) {
        let to = publish.iter().filter_map(|s| subject::parse_inbox(s).filter(|(_, from)| *from == user));
        let channels = publish.iter().filter_map(|s| subject::parse_chan(s).filter(|(_, from)| *from == user));
        return Ok(Kind::Webhook { to: to.map(|(to, _)| to).collect(), channels: channels.map(|(c, _)| c).collect() });
    }
    Ok(Kind::Ant)
}

/// The JetStream domain the publish allowances name, as [`ants_proto::Permissions::with_js_domain`] adds them.
fn js_domain(publish: &[String]) -> Option<JsDomain> {
    publish.iter().find_map(|s| {
        let (domain, rest) = s.strip_prefix("$JS.")?.split_once('.')?;
        if domain == "API" || domain == "*" || !rest.starts_with("API.") {
            return None;
        }
        domain.parse().ok()
    })
}

#[cfg(test)]
mod tests {
    use ants_proto::{Permissions, Role};
    use serde_json::json;

    use super::*;

    fn claims(name: &str, permissions: &Permissions, tags: &[String], exp: Option<u64>) -> Value {
        let mut claims = json!({
            "name": name,
            "sub": "UKEY",
            "nats": { "pub": { "allow": permissions.publish }, "sub": { "allow": permissions.subscribe }, "tags": tags },
        });
        if let Some(exp) = exp {
            claims["exp"] = exp.into();
        }
        claims
    }

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[test]
    fn a_stored_seed_loads_only_for_the_key_its_jwt_names() {
        let dir = ants_test_support::scratch_dir("creds-load");
        let (path, store) = (dir.join("alice.creds"), keystore::Memory::default());
        let (user, other) = (KeyPair::new_user(), KeyPair::new_user());
        let (key, seed) = (user.public_key(), user.seed().unwrap());
        let tags = [PERMISSIONS_VERSION.tag(), DeviceId::generate().tag()];
        let mut named = claims("alice", &Permissions::for_user(&p("alice"), Role::Human), &tags, None);
        named["sub"] = key.clone().into();
        let jwt = format!("e30.{}.sig", BASE64URL_NOPAD.encode(named.to_string().as_bytes()));
        let load = |text: String| {
            std::fs::write(&path, text).unwrap();
            load(&path, &store).map(|(jwt, key)| (jwt, key.public_key())).map_err(|e| format!("{e:#}"))
        };

        assert_eq!(load(text(&jwt, &seed)), Ok((jwt.clone(), key.clone())));
        let err = load(stored_text(&jwt, &key)).unwrap_err();
        assert!(err.contains("holds no seed") && err.contains("backup"), "{err}");
        store.set(&key, &seed).unwrap();
        assert_eq!(load(stored_text(&jwt, &key)), Ok((jwt.clone(), key.clone())));
        let err = load(stored_text(&jwt, &other.public_key())).unwrap_err();
        assert!(err.contains("not the key"), "{err}");
        store.lock();
        let err = load(stored_text(&jwt, &key)).unwrap_err();
        assert!(err.contains("locked") && !err.contains(&seed), "{err}");
        store.unlock();
        store.set(&key, &other.seed().unwrap()).unwrap();
        let err = load(stored_text(&jwt, &key)).unwrap_err();
        assert!(err.contains("another key"), "{err}");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_encryption_key_loads_only_as_the_jwt_binds_it() {
        let dir = ants_test_support::scratch_dir("creds-xkey");
        let (path, store) = (dir.join("alice.creds"), keystore::Memory::default());
        let (user, xkey, other) = (KeyPair::new_user(), XKey::new(), XKey::new());
        let (seed, xseed) = (user.seed().unwrap(), xkey.seed().unwrap());
        let jwt = |xkey: Option<&XKey>| {
            let mut tags = vec![PERMISSIONS_VERSION.tag(), DeviceId::generate().tag()];
            tags.extend(xkey.map(|x| x.public_key().parse::<DeviceKey>().unwrap().tag()));
            let mut named = claims("alice", &Permissions::for_user(&p("alice"), Role::Human), &tags, None);
            named["sub"] = user.public_key().into();
            format!("e30.{}.sig", BASE64URL_NOPAD.encode(named.to_string().as_bytes()))
        };
        let (bound, unbound) = (jwt(Some(&xkey)), jwt(None));
        let load = |text: String| {
            std::fs::write(&path, text).unwrap();
            load_xkey(&path, &store).map(|key| key.map(|key| key.public_key())).map_err(|e| format!("{e:#}"))
        };

        let file = text(&bound, &seed) + &xkey_block(Seed::File(&xseed));
        assert_eq!(xkey_of(&file), Some(Seed::File(xseed.as_str())));
        assert_eq!(seed_of(&file), Some(Seed::File(seed.as_str())), "the NKey seed stays the one nats tools read");
        assert_eq!(load(file), Ok(Some(xkey.public_key())));
        assert_eq!(load(text(&unbound, &seed)), Ok(None));
        let err = load(text(&bound, &seed)).unwrap_err();
        assert!(err.contains("no seed of the encryption key"), "{err}");
        let err = load(text(&unbound, &seed) + &xkey_block(Seed::File(&xseed))).unwrap_err();
        assert!(err.contains("does not bind"), "{err}");
        let err = load(text(&bound, &seed) + &xkey_block(Seed::File(&other.seed().unwrap()))).unwrap_err();
        assert!(err.contains("not that of the key") && !err.contains(&xseed), "{err}");

        let stored = stored_text(&bound, &user.public_key()) + &xkey_block(Seed::Stored(&xkey.public_key()));
        assert!(load(stored.clone()).unwrap_err().contains("holds no seed"));
        store.set(&xkey.public_key(), &xseed).unwrap();
        assert_eq!(load(stored), Ok(Some(xkey.public_key())));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn credentials_without_the_current_version_are_outdated_and_name_their_renewal() {
        let dev_a = p("dev-a");
        let domain: JsDomain = "hub".parse().unwrap();
        let ant = Permissions::for_user(&dev_a, Role::Ant).with_js_domain(Some(&domain));
        let current = PERMISSIONS_VERSION.tag();
        let older = PermissionsVersion(0).tag();
        let newer = PermissionsVersion(PERMISSIONS_VERSION.0 + 1).tag();
        let finding = |tags: &[String]| Finding::from_claims(&claims("dev-a", &ant, tags, None)).unwrap();

        let fresh = finding(std::slice::from_ref(&current));
        assert_eq!((fresh.kind.clone(), fresh.version_status()), (Kind::Ant, VersionStatus::Current));
        assert!(fresh.warnings(None, 0).is_empty());
        assert_eq!(fresh.renew(), ["ants auth user dev-a --js-domain hub"]);

        let outdated = finding(std::slice::from_ref(&older));
        assert_eq!(outdated.version_status(), VersionStatus::Outdated);
        let warnings = outdated.warnings(None, 0);
        assert!(warnings.len() == 1 && warnings[0].contains("`ants auth user dev-a --js-domain hub`"), "{warnings:?}");
        assert!(
            outdated.warnings(None, 0)[0].ends_with("(its `versions` check)"),
            "renew once every component allows it"
        );
        let human = Permissions::for_user(&p("alice"), Role::Human);
        let tags = [older, DeviceId::generate().tag()];
        let human = Finding::from_claims(&claims("alice", &human, &tags, None)).unwrap();
        assert!(
            !human.warnings(None, 0)[0].contains("ants doctor"),
            "a human's renewal withdraws no right a service uses"
        );
        let ahead = finding(&[newer]);
        assert_eq!(ahead.version_status(), VersionStatus::Newer);
        assert!(ahead.warnings(None, 0)[0].contains("upgrade ants"), "{:?}", ahead.warnings(None, 0));
    }

    #[test]
    fn credentials_expiring_within_a_week_get_a_warning_and_enrollments_only_once_expired() {
        let dev_a = p("dev-a");
        let tags = [PERMISSIONS_VERSION.tag()];
        let ant = Permissions::for_user(&dev_a, Role::Ant);
        let now = 1_000_000_000;
        let week = EXPIRY_WARNING.as_secs();
        let at = |exp| Finding::from_claims(&claims("dev-a", &ant, &tags, exp)).unwrap();
        assert_eq!(at(None).expiry(now), Expiry::Never);
        assert_eq!(at(Some(now + week + 1)).expiry(now), Expiry::Later);
        assert!(at(Some(now + week + 1)).warnings(None, now).is_empty());
        let soon = at(Some(now + week));
        assert_eq!(soon.expiry(now), Expiry::Soon);
        let warnings = soon.warnings(None, now);
        assert!(
            warnings.len() == 1 && warnings[0].contains("expire at") && warnings[0].contains("`ants auth user dev-a`")
        );
        let expired = at(Some(now));
        assert_eq!(expired.expiry(now), Expiry::Expired);
        assert!(expired.warnings(None, now)[0].contains("expired at"));

        let enrollment = Permissions::for_enrollment(&dev_a);
        let enroll = |exp| Finding::from_claims(&claims("enroll-dev-a", &enrollment, &tags, Some(exp))).unwrap();
        assert_eq!(enroll(now + 60).kind, Kind::Enrollment { ant: dev_a });
        assert!(enroll(now + 60).warnings(None, now).is_empty(), "enrollment credentials are short-lived by design");
        assert_eq!(enroll(now).renew(), ["ants auth enroll dev-a"]);
        assert_eq!(enroll(now).warnings(None, now).len(), 1);
    }

    #[test]
    fn credentials_name_ants_renew_until_they_expire() {
        let now = 1_000_000_000;
        let path = Path::new("/home/a/dev-a.creds");
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        let at = |version: u32, exp| {
            let tags = [PermissionsVersion(version).tag()];
            Finding::from_claims(&claims("dev-a", &ant, &tags, exp)).unwrap()
        };
        let soon = at(PERMISSIONS_VERSION.0, Some(now + 60));
        assert!(soon.renewable(now));
        let warning = &soon.warnings(Some(path), now)[0];
        assert!(
            warning.contains("`ants renew /home/a/dev-a.creds`, approved by `ants enroll renew dev-a`"),
            "{warning}"
        );
        assert!(soon.warnings(None, now)[0].contains("`ants auth user dev-a`"), "no path, no `ants renew`");
        assert_eq!(soon.renew_over_nats(path), ["ants renew /home/a/dev-a.creds", "ants enroll renew dev-a"]);
        let expired = at(PERMISSIONS_VERSION.0, Some(now));
        assert!(!expired.renewable(now) && expired.warnings(Some(path), now)[0].contains("`ants auth user dev-a`"));
        let older = at(0, None);
        assert!(older.renewable(now), "any version renews over NATS");
        assert!(older.warnings(Some(path), now)[0].contains("`ants renew /home/a/dev-a.creds`"));
        let enrollment = Permissions::for_enrollment(&p("dev-a"));
        let tags = [PERMISSIONS_VERSION.tag()];
        assert!(!Finding::from_claims(&claims("enroll-dev-a", &enrollment, &tags, None)).unwrap().renewable(now));
    }

    #[test]
    fn registered_credentials_name_ants_signer_not_ants_enroll_renew() {
        let now = 1_000_000_000;
        let path = Path::new("/home/a/alice.creds");
        let human = Permissions::for_user(&p("alice"), Role::Human);
        let at = |registered: bool, exp| {
            let mut tags = vec![PERMISSIONS_VERSION.tag(), DeviceId::generate().tag()];
            tags.extend(registered.then(|| ants_proto::REGISTERED_TAG.to_owned()));
            Finding::from_claims(&claims("alice", &human, &tags, Some(exp))).unwrap()
        };
        let registered = at(true, now + 60);
        assert!(registered.registered);
        assert_eq!(registered.renew_over_nats(path), ["ants renew /home/a/alice.creds"]);
        let warning = &registered.warnings(Some(path), now)[0];
        assert!(warning.contains("`ants renew /home/a/alice.creds`, which `ants signer` answers"), "{warning}");
        assert!(!warning.contains("ants enroll renew"), "{warning}");
        let expired = &at(true, now).warnings(Some(path), now)[0];
        assert!(expired.contains("`ants login --hub URL --name alice`"), "{expired}");

        let issued = at(false, now + 60);
        assert!(!issued.registered, "issued with `ants auth user`");
        assert_eq!(issued.renew_over_nats(path), ["ants renew /home/a/alice.creds", "ants enroll renew alice"]);
        assert!(!at(false, now).warnings(Some(path), now)[0].contains("ants login"));

        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        let tags = [PERMISSIONS_VERSION.tag(), ants_proto::REGISTERED_TAG.to_owned()];
        assert!(!Finding::from_claims(&claims("dev-a", &ant, &tags, None)).unwrap().registered, "only a human's");
    }

    #[test]
    fn only_the_user_jwt_makes_an_assistant_or_an_admin() {
        let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
        let tags = [PERMISSIONS_VERSION.tag(), "ants-assistant-of:alice".to_owned()];
        let finding = Finding::from_claims(&claims("pa", &assistant, &tags, None)).unwrap();
        assert_eq!((&finding.kind, finding.device, finding.admin), (&Kind::Assistant { of: p("alice") }, None, false));
        assert_eq!(finding.holder(), "ant pa, assistant of alice");
        assert_eq!(finding.renew(), ["ants auth user pa --assistant-of alice"]);
        assert!(finding.same_credentials(&finding.clone()));
        let ant = Permissions::for_user(&p("pa"), Role::Ant);
        assert!(Finding::from_claims(&claims("pa", &ant, &tags, None)).is_err(), "the tag without the permissions");
        let human = Permissions::for_user(&p("pa"), Role::Human);
        assert!(Finding::from_claims(&claims("pa", &human, &tags, None)).is_err(), "nor with a human's");

        let human = Permissions::for_user(&p("alice"), Role::Human);
        let tags = [PERMISSIONS_VERSION.tag(), DeviceId::generate().tag(), "ANTS-ADMIN".to_owned()];
        let admin = Finding::from_claims(&claims("alice", &human, &tags, None)).unwrap();
        assert!(admin.admin);
        assert_eq!(admin.holder(), "human alice (admin)");
        assert_eq!(admin.renew(), ["ants auth user alice --human --admin"]);
        assert!(serde_json::to_value(&admin).unwrap()["admin"] == true);
        let tags = [PERMISSIONS_VERSION.tag(), "ants-admin".to_owned()];
        assert!(!Finding::from_claims(&claims("dev-a", &ant, &tags, None)).unwrap().admin, "only a human's");
    }

    #[test]
    fn a_webhook_asking_for_its_renewal_is_still_a_webhook() {
        let hook = p("github");
        let destinations = [ants_proto::Destination::Ant(p("dev-a"))];
        let webhook = Permissions::for_webhook(&hook, &destinations);
        let tags = [PERMISSIONS_VERSION.tag()];
        let finding = Finding::from_claims(&claims("github", &webhook, &tags, None)).unwrap();
        assert_eq!(finding.kind, Kind::Webhook { to: vec![p("dev-a")], channels: vec![] });
        let ant = Permissions::for_user(&hook, Role::Ant);
        assert_eq!(Finding::from_claims(&claims("github", &ant, &tags, None)).unwrap().kind, Kind::Ant);
    }

    #[test]
    fn host_credentials_name_their_machine_and_its_renewal() {
        let tags = [PERMISSIONS_VERSION.tag()];
        let machine: Machine = "Mac_mini".parse().unwrap();
        let domain: JsDomain = "hub".parse().unwrap();
        let host = Permissions::for_host(&machine).with_js_domain(Some(&domain));
        let finding = Finding::from_claims(&claims("host-Mac_mini", &host, &tags, None)).unwrap();
        assert_eq!(finding.kind, Kind::Host { machine });
        assert_eq!(finding.holder(), "the host of Mac_mini");
        assert_eq!(finding.renew(), ["ants auth host Mac_mini --js-domain hub"]);
        assert!(!finding.renewable(0), "only `ants auth host` renews them");
        let ant = Permissions::for_user(&p("host-m1"), Role::Ant);
        assert_eq!(Finding::from_claims(&claims("host-m1", &ant, &tags, None)).unwrap().kind, Kind::Ant);
    }

    #[test]
    fn a_creds_file_s_user_jwt_is_decoded() {
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        let text = |tags: &[String]| {
            let body = BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims("dev-a", &ant, tags, Some(5))).unwrap());
            format!("-----BEGIN NATS USER JWT-----\nhead.{body}.sig\n------END NATS USER JWT------\n")
        };
        let finding = Finding::from_creds(&text(&[PERMISSIONS_VERSION.tag()])).unwrap();
        assert_eq!((finding.name.as_str(), finding.expires_at), ("dev-a", Some(5)));
        assert!(Finding::from_creds("no jwt").is_err());
        assert!(Finding::from_creds(&text(&[])).is_err(), "no permissions version");
        let human = Permissions::for_user(&p("alice"), Role::Human);
        let tags = [PERMISSIONS_VERSION.tag()];
        assert!(Finding::from_claims(&claims("alice", &human, &tags, None)).is_err(), "no device id");
    }
}
