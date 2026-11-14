//! Checking the credentials an ants process uses: whether `ants auth` issued them with the permissions of this
//! version ([`PERMISSIONS_VERSION`]) and whether they expire soon, each with the command that renews them: `ants renew`
//! over NATS for credentials that may ask for it ([`Finding::renewable`]), else `ants auth` on the machine holding the
//! team keys.
//!
//! The kind of credentials, and what renewing them takes, comes from the permissions their user JWT carries, so
//! credentials issued before the version was stamped are told apart too.

use std::path::Path;
use std::time::Duration;

use ants_proto::{
    ChannelId, DeviceId, ENROLL_INBOX, HUMANS_BUCKET, JsDomain, Machine, PERMISSIONS_VERSION, PermissionsVersion,
    RENEW_SINCE, UserId, WEBHOOKS_BUCKET, inbox_prefix, subject,
};
use anyhow::{Context as _, Result, anyhow};
use data_encoding::BASE64URL_NOPAD;
use serde::Serialize;
use serde_json::Value;

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
    /// A webhook of `ants webhook serve`, delivering to the ants of `to` and the channels of `channels`.
    Webhook {
        to: Vec<UserId>,
        channels: Vec<ChannelId>,
    },
    ChannelManager,
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
    /// Enrollment credentials of `ant`, for `ants add --enroll`.
    Enrollment {
        ant: UserId,
    },
}

/// How the permissions version of credentials compares with [`PERMISSIONS_VERSION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum VersionStatus {
    Current,
    /// Issued with an older version, or before versions were stamped: what the role allows since may be refused.
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
    /// The permissions version they carry; none for credentials issued before versions were stamped.
    pub version: Option<PermissionsVersion>,
    /// Seconds since the epoch; none for credentials that never expire.
    pub expires_at: Option<u64>,
    /// The hub's JetStream domain they were issued for, if any.
    pub js_domain: Option<JsDomain>,
    /// The human's device they are for, if their user JWT names one ([`DeviceId::tag`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<DeviceId>,
}

impl Finding {
    /// The finding of the user JWT in a `.creds` file's text.
    pub fn from_creds(text: &str) -> Result<Self> {
        Self::from_jwt(creds_jwt(text).context("no user JWT")?)
    }

    /// The finding of a user JWT.
    pub fn from_jwt(jwt: &str) -> Result<Self> {
        let payload = jwt.trim().split('.').nth(1).context("a malformed user JWT")?;
        let payload = BASE64URL_NOPAD
            .decode(payload.trim_end_matches('=').as_bytes())
            .map_err(|e| anyhow!("a malformed user JWT: {e}"))?;
        Self::from_claims(&serde_json::from_slice(&payload).context("a malformed user JWT")?)
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
        Ok(Self {
            kind: kind(&name, &publish, &subscribe, leaf_only)?,
            version: PermissionsVersion::from_tags(tags.iter().map(String::as_str)),
            expires_at: claims["exp"].as_u64(),
            js_domain: js_domain(&publish),
            device: DeviceId::from_tags(tags.iter().map(String::as_str)),
            name,
            key,
        })
    }

    pub fn version_status(&self) -> VersionStatus {
        match self.version {
            Some(version) if version == PERMISSIONS_VERSION => VersionStatus::Current,
            Some(version) if version > PERMISSIONS_VERSION => VersionStatus::Newer,
            _ => VersionStatus::Outdated,
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
            Kind::Human => format!("human {}", self.name),
            Kind::Webhook { .. } => format!("webhook {}", self.name),
            Kind::ChannelManager => "the channel manager".to_owned(),
            Kind::Leaf { machine, .. } => format!("the leaf node uplink of {machine}"),
            Kind::LeafSystem { machine } => format!("the system account uplink of the leaf node of {machine}"),
            Kind::Enrollment { ant } => format!("the enrollment of {ant}"),
        }
    }

    /// The commands that renew them, in order, all but `ants leaf outbox` on the machine holding the team keys.
    pub fn renew(&self) -> Vec<String> {
        let domain = self.js_domain.as_ref().map(|d| format!(" --js-domain {d}")).unwrap_or_default();
        let flags = |flag: &str, ids: &[UserId]| ids.iter().map(|id| format!(" --{flag} {id}")).collect::<String>();
        match &self.kind {
            Kind::Ant => vec![format!("ants auth user {}{domain}", self.name)],
            Kind::Human => vec![format!("ants auth user {} --human{domain}", self.name)],
            Kind::Webhook { to, channels } => {
                let channels: String = channels.iter().map(|c| format!(" --channel {c}")).collect();
                vec![format!("ants auth user {} --webhook{}{channels}{domain}", self.name, flags("to", to))]
            }
            Kind::ChannelManager => vec![format!("ants auth channel-manager{domain}")],
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
            Kind::Enrollment { ant } => vec![format!("ants auth enroll {ant}")],
        }
    }

    /// Whether `other` are the same credentials as these, for another key or with other permissions: the same name,
    /// kind (members in any order), device and JetStream domain, as renewing keeps them.
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
            && self.js_domain == other.js_domain
    }

    /// Whether they may ask for their own renewal over NATS at `now` (`ants renew`): those of an ant, a human, a
    /// webhook, the channel manager or a leaf node's uplink, issued with permissions version [`RENEW_SINCE`] or later,
    /// whose subjects they need, and not expired, as the approver renews only credentials still valid.
    pub fn renewable(&self, now: u64) -> bool {
        let kind = !matches!(self.kind, Kind::LeafSystem { .. } | Kind::Enrollment { .. });
        kind && self.version.is_some_and(|v| v >= RENEW_SINCE) && self.expiry(now) != Expiry::Expired
    }

    /// The commands renewing them over NATS when they are at `path`: `ants renew` where they are used, then
    /// `ants enroll renew` on the machine holding the account key, which approves it.
    pub fn renew_over_nats(&self, path: &Path) -> Vec<String> {
        vec![format!("ants renew {}", path.display()), format!("ants enroll renew {}", self.name)]
    }

    /// How to renew them, in words: over NATS ([`Finding::renew_over_nats`]) when they are at `path` and
    /// [`Finding::renewable`] at `now`, else naming [`Finding::renew`].
    pub fn renew_hint(&self, path: Option<&Path>, now: u64) -> String {
        if let Some(path) = path.filter(|_| self.renewable(now)) {
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
            Kind::Human | Kind::Enrollment { .. } => "",
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
        match (self.version_status(), self.version) {
            (VersionStatus::Outdated, None) => warnings.push(format!(
                "the credentials of {holder} predate permission versions, and this ants gives version \
                 {PERMISSIONS_VERSION}: requests their role allows now may be refused; {}{}",
                self.renew_hint(path, now),
                self.renew_when()
            )),
            (VersionStatus::Outdated, Some(version)) => warnings.push(format!(
                "the credentials of {holder} carry permissions version {version}, older than version \
                 {PERMISSIONS_VERSION} of this ants: requests their role allows now may be refused; {}{}",
                self.renew_hint(path, now),
                self.renew_when()
            )),
            (VersionStatus::Newer, Some(version)) => warnings.push(format!(
                "the credentials of {holder} carry permissions version {version}, newer than version \
                 {PERMISSIONS_VERSION} of this ants: upgrade ants"
            )),
            _ => {}
        }
        let at = || self.expires_at.map(time).unwrap_or_default();
        match self.expiry(now) {
            Expiry::Expired => warnings.push(format!(
                "the credentials of {holder} expired at {}; {}",
                at(),
                self.renew_hint(path, now)
            )),
            Expiry::Soon if !matches!(self.kind, Kind::Enrollment { .. }) => {
                warnings.push(format!("the credentials of {holder} expire at {}; {}", at(), self.renew_hint(path, now)))
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

/// The user JWT of a `.creds` file's text: the line after the `BEGIN NATS USER JWT` marker.
fn creds_jwt(creds: &str) -> Option<&str> {
    let mut lines = creds.lines().map(str::trim);
    lines.find(|line| line.starts_with("---") && line.contains("BEGIN NATS USER JWT"))?;
    lines.find(|line| !line.is_empty()).filter(|line| !line.starts_with("---"))
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
        let (mut ants, mut humans, mut webhooks) = (Vec::new(), Vec::new(), Vec::new());
        for user in users {
            let user: UserId = user;
            if has(publish, &subject::roster(&user)) {
                ants.push(user);
            } else if has(publish, &format!("$KV.{WEBHOOKS_BUCKET}.{user}.>")) {
                webhooks.push(user);
            } else {
                humans.push(user);
            }
        }
        let enroll_approver = has(subscribe, &format!("{ENROLL_INBOX}.>"));
        return Ok(Kind::Leaf { machine, ants, humans, webhooks, enroll_approver });
    }
    let user: UserId = name.parse().map_err(|e| anyhow!("credentials named {name:?}: {e}"))?;
    if user == UserId::channel_manager() {
        return Ok(Kind::ChannelManager);
    }
    if let Some(ant) = name.strip_prefix("enroll-")
        && !publish.is_empty()
        && publish.iter().all(|s| s.starts_with("ants.enroll."))
    {
        return Ok(Kind::Enrollment { ant: ant.parse().map_err(|e| anyhow!("enrollment credentials {name}: {e}"))? });
    }
    if has(publish, &format!("$KV.{HUMANS_BUCKET}.>")) {
        return Ok(Kind::Human);
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
    fn credentials_without_the_current_version_are_outdated_and_name_their_renewal() {
        let dev_a = p("dev-a");
        let domain: JsDomain = "hub".parse().unwrap();
        let ant = Permissions::for_user(&dev_a, Role::Ant).with_js_domain(Some(&domain));
        let current = PERMISSIONS_VERSION.tag();
        let older = PermissionsVersion(PERMISSIONS_VERSION.0 - 1).tag();
        let newer = PermissionsVersion(PERMISSIONS_VERSION.0 + 1).tag();
        let finding = |tags: &[String]| Finding::from_claims(&claims("dev-a", &ant, tags, None)).unwrap();

        let fresh = finding(std::slice::from_ref(&current));
        assert_eq!((fresh.kind.clone(), fresh.version_status()), (Kind::Ant, VersionStatus::Current));
        assert!(fresh.warnings(None, 0).is_empty());
        assert_eq!(fresh.renew(), ["ants auth user dev-a --js-domain hub"]);

        let unstamped = finding(&[]);
        assert_eq!((unstamped.version, unstamped.version_status()), (None, VersionStatus::Outdated));
        let warnings = unstamped.warnings(None, 0);
        assert!(warnings.len() == 1 && warnings[0].contains("`ants auth user dev-a --js-domain hub`"), "{warnings:?}");
        let outdated = finding(std::slice::from_ref(&older));
        assert_eq!(outdated.version_status(), VersionStatus::Outdated);
        assert!(
            outdated.warnings(None, 0)[0].ends_with("(its `versions` check)"),
            "renew once every component allows it"
        );
        let human = Permissions::for_user(&p("alice"), Role::Human);
        let human = Finding::from_claims(&claims("alice", &human, &[older], None)).unwrap();
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
    fn credentials_since_renewal_over_nats_name_ants_renew_until_they_expire() {
        let now = 1_000_000_000;
        let path = Path::new("/home/a/dev-a.creds");
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        let at = |version: u32, exp| {
            let tags = [PermissionsVersion(version).tag()];
            Finding::from_claims(&claims("dev-a", &ant, &tags, exp)).unwrap()
        };
        let soon = at(RENEW_SINCE.0, Some(now + 60));
        assert!(soon.renewable(now));
        let warning = &soon.warnings(Some(path), now)[0];
        assert!(
            warning.contains("`ants renew /home/a/dev-a.creds`, approved by `ants enroll renew dev-a`"),
            "{warning}"
        );
        assert!(soon.warnings(None, now)[0].contains("`ants auth user dev-a`"), "no path, no `ants renew`");
        assert_eq!(soon.renew_over_nats(path), ["ants renew /home/a/dev-a.creds", "ants enroll renew dev-a"]);
        let expired = at(RENEW_SINCE.0, Some(now));
        assert!(!expired.renewable(now) && expired.warnings(Some(path), now)[0].contains("`ants auth user dev-a`"));
        let older = at(RENEW_SINCE.0 - 1, None);
        assert!(!older.renewable(now), "their subjects came with {RENEW_SINCE}");
        assert!(older.warnings(Some(path), now)[0].contains("`ants auth user dev-a`"));
        let enrollment = Permissions::for_enrollment(&p("dev-a"));
        let tags = [PERMISSIONS_VERSION.tag()];
        assert!(!Finding::from_claims(&claims("enroll-dev-a", &enrollment, &tags, None)).unwrap().renewable(now));
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
    fn a_creds_file_s_user_jwt_is_decoded() {
        let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
        let body = BASE64URL_NOPAD.encode(&serde_json::to_vec(&claims("dev-a", &ant, &[], Some(5))).unwrap());
        let text = format!("-----BEGIN NATS USER JWT-----\nhead.{body}.sig\n------END NATS USER JWT------\n");
        let finding = Finding::from_creds(&text).unwrap();
        assert_eq!((finding.name.as_str(), finding.expires_at), ("dev-a", Some(5)));
        assert!(Finding::from_creds("no jwt").is_err());
    }
}
