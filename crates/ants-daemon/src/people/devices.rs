//! The directory of the encryption keys of humans' devices as a reader sees it ([`Directory`]): the user JWTs that
//! humans' CLIs put in [`DEVICES_BUCKET`], taken only once verified ([`verify`]) and while no revocation record names
//! their key, and the assistants their humans enrolled as devices by their grants in [`HUMANS_BUCKET`]
//! ([`ants_proto::AssistantGrant::sealing`]), taken only from a grant standing that verifies.

use crate::bus::relayed::GuardedKv as _;
use std::collections::HashSet;
use std::fmt;

use ants_proto::{
    ASSISTANT_RECORD, AssistantGrant, DEVICES_BUCKET, DeviceId, DeviceKey, HUMANS_BUCKET, REVOKED_DEVICES, UserId,
    UserJwt, VerifiedGrant, assistant_grant_key, assistant_grant_of, device_entry_key, device_entry_of,
    revoked_device_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::context::GetStreamErrorKind;
use async_nats::jetstream::{self, ErrorCode, kv};
use futures::TryStreamExt as _;

use crate::secrets::creds::{Finding, Kind};

/// A device's encryption key, taken from a user JWT the reader verified ([`verify`]), or from the grant of a human
/// enrolling its assistant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDevice {
    pub human: UserId,
    pub device: DeviceId,
    pub key: DeviceKey,
    /// The public key of the credentials whose user JWT binds it, `U...`: of an assistant's, those that signed the
    /// grant.
    pub user_key: String,
    /// When those credentials expire, in seconds since the epoch; none when they never do, or for an assistant's.
    pub expires_at: Option<u64>,
    /// The assistant ant holding the key, when the human enrolled its assistant as this device: a message it seals
    /// names it as its sender, not the human.
    pub assistant: Option<UserId>,
}

impl VerifiedDevice {
    /// The device of the assistant that `grant` enrolls, if it does.
    pub fn enrolled(grant: &VerifiedGrant) -> Option<Self> {
        let sealing = grant.sealing.as_ref()?;
        Some(Self {
            human: grant.human.clone(),
            device: sealing.device,
            key: sealing.xkey.clone(),
            user_key: grant.signer.clone(),
            expires_at: None,
            assistant: Some(grant.assistant.clone()?),
        })
    }

    /// Who sends with the device: its assistant, else its human.
    pub fn sender(&self) -> &UserId {
        self.assistant.as_ref().unwrap_or(&self.human)
    }
}

/// An entry of the directory a reader took no key from, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// Its key in the bucket.
    pub entry: String,
    pub reason: String,
    /// Whether it is skipped only as its key has a revocation record, which `ants auth push` clears it for.
    pub revoked: bool,
}

impl Skipped {
    /// Log that its key is not used: a revoked device's entry at debug, as its revocation is recorded, any other as a
    /// warning.
    fn log(&self) {
        if self.revoked {
            tracing::debug!("{self}; its key is not used");
        } else {
            tracing::warn!("{self}; its key is not used");
        }
    }
}

impl fmt::Display for Skipped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{DEVICES_BUCKET} entry {}: {}", self.entry, self.reason)
    }
}

/// The devices of some humans in the directory: the keys verified, and the entries skipped.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Devices {
    pub verified: Vec<VerifiedDevice>,
    pub skipped: Vec<Skipped>,
}

impl Devices {
    /// The verified devices of `human`, its assistant's among them.
    pub fn of<'a>(&'a self, human: &'a UserId) -> impl Iterator<Item = &'a VerifiedDevice> {
        self.verified.iter().filter(move |d| d.human == *human)
    }
}

/// What the directory says of one device ([`Directory::device`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    Verified(VerifiedDevice),
    /// No entry of the device.
    Missing,
    Skipped(Skipped),
}

/// Take a device's encryption key from `jwt`, the entry of `device` of `human`, only if it is a user JWT signed by
/// `account`, the account that signed the reader's own credentials, named for `human`, for a human's device `device`
/// that it binds a key to, and unexpired at `now` (seconds since the epoch). Whether its key is revoked the directory
/// tells ([`Directory`]). Else, the reason.
pub fn verify(account: &str, human: &UserId, device: &DeviceId, jwt: &str, now: u64) -> Result<VerifiedDevice, String> {
    let jwt = UserJwt::verify(jwt).map_err(|e| e.to_string())?;
    if jwt.iss != account {
        return Err(format!("signed by {}, not by the bus's account {account}", jwt.iss));
    }
    if jwt.name != human.as_str() {
        return Err(format!("names {}, not {human}", jwt.name));
    }
    let finding = Finding::from_claims(&jwt.claims).map_err(|e| format!("{e:#}"))?;
    if finding.kind != Kind::Human {
        return Err("not a human's credentials".to_owned());
    }
    if finding.device != Some(*device) {
        return Err(format!("for device {}", finding.device.map_or_default(|d| d.to_string())));
    }
    let Some(key) = finding.xkey else { return Err("binds no encryption key".to_owned()) };
    let expires_at = jwt.exp.map(|exp| u64::try_from(exp).unwrap_or_default());
    if let Some(exp) = expires_at.filter(|exp| *exp <= now) {
        return Err(format!("expired at {exp}"));
    }
    Ok(VerifiedDevice { human: human.clone(), device: *device, key, user_key: jwt.sub, expires_at, assistant: None })
}

/// The directory as a reader sees it: the keys it takes are only those of user JWTs that the account `account` signed
/// ([`verify`]), and whose user key has no revocation record, and those of the assistants that grants signed by devices
/// of the account enroll.
pub struct Directory {
    /// None while the bucket does not exist: no human put a key yet.
    kv: Option<kv::Store>,
    /// [`HUMANS_BUCKET`], where the grants are; none while it does not exist. Boxed, as a reader holds the directory
    /// across awaits.
    grants: Option<Box<kv::Store>>,
    account: String,
}

impl Directory {
    /// The directory on the hub `js` reaches, read for the bus of the account `account` (the issuer of the reader's
    /// credentials).
    pub async fn open(js: &jetstream::Context, account: &str) -> Result<Self> {
        let kv = match js.get_key_value(DEVICES_BUCKET).await {
            Ok(kv) => Some(kv),
            Err(e) => match js.get_stream(subject::kv_stream(DEVICES_BUCKET)).await {
                Err(missing) if matches!(missing.kind(), GetStreamErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND) => {
                    None
                }
                _ => return Err(e).with_context(|| format!("opening {DEVICES_BUCKET}")),
            },
        };
        let grants = Box::pin(js.get_key_value(HUMANS_BUCKET))
            .await
            .inspect_err(|e| tracing::debug!("no assistant enrolled as a device, {HUMANS_BUCKET} unread: {e}"))
            .ok()
            .map(Box::new);
        Ok(Self { kv, grants, account: account.to_owned() })
    }

    /// The devices of `humans` whose keys the directory holds, reporting each entry of theirs it skips: one listing of
    /// the bucket's keys, a get of each entry of those humans, and a get of each one's grant.
    pub async fn devices(&self, humans: &[UserId]) -> Result<Devices> {
        let mut devices = self.devices_where(|h| humans.iter().any(|x| x.as_str() == h)).await?;
        for human in humans {
            devices
                .verified
                .extend(self.grant(&assistant_grant_key(human)).await.as_ref().and_then(VerifiedDevice::enrolled));
        }
        Ok(devices)
    }

    /// The devices of every human whose keys the directory holds, as [`Directory::devices`] reads them.
    pub async fn all(&self) -> Result<Devices> {
        let mut devices = self.devices_where(|h| h != REVOKED_DEVICES).await?;
        devices.verified.extend(self.grants().await.iter().filter_map(VerifiedDevice::enrolled));
        Ok(devices)
    }

    async fn devices_where(&self, human: impl Fn(&str) -> bool) -> Result<Devices> {
        let Some(kv) = &self.kv else { return Ok(Devices::default()) };
        let keys: Vec<String> = kv
            .keys()
            .await
            .with_context(|| format!("listing {DEVICES_BUCKET}"))?
            .try_collect()
            .await
            .with_context(|| format!("listing {DEVICES_BUCKET}"))?;
        let revoked: HashSet<&str> =
            keys.iter().filter_map(|k| k.strip_prefix(REVOKED_DEVICES)?.strip_prefix('.')).collect();
        let theirs = keys
            .iter()
            .filter(|k| k.split('.').next().is_some_and(&human))
            .filter(|k| k.split('.').nth(1) != Some(ASSISTANT_RECORD));
        let gets = theirs.map(|entry| async move {
            let jwt = kv.guarded_get(entry).await.with_context(|| format!("reading {entry} in {DEVICES_BUCKET}"))?;
            anyhow::Ok((entry, jwt))
        });
        let now = now();
        let mut devices = Devices::default();
        for (entry, jwt) in futures::future::try_join_all(gets).await? {
            // Deleted since listed.
            let Some(jwt) = jwt else { continue };
            match self.check(entry, &jwt, now, |user_key| revoked.contains(user_key)) {
                Ok(device) => devices.verified.push(device),
                Err(skipped) => {
                    skipped.log();
                    devices.skipped.push(skipped);
                }
            }
        }
        Ok(devices)
    }

    /// What the directory says of `device` of `sender`: two direct gets, of its entry and of the revocation record of
    /// the key of its credentials; when it has no entry, the device a grant standing enrolls `sender` as, if `sender`
    /// is an assistant. A skipped entry is reported.
    pub async fn device(&self, sender: &UserId, device: &DeviceId) -> Result<Lookup> {
        let Some(kv) = &self.kv else { return Ok(Box::pin(self.enrolled(sender, device)).await) };
        let entry = device_entry_key(sender, device);
        let Some(jwt) = kv.guarded_get(&entry).await.with_context(|| format!("reading {entry} in {DEVICES_BUCKET}"))?
        else {
            return Ok(Box::pin(self.enrolled(sender, device)).await);
        };
        let verified = match self.check(&entry, &jwt, now(), |_| false) {
            Ok(verified) => verified,
            Err(skipped) => return Ok(self.skipped(skipped)),
        };
        let record = revoked_device_key(&verified.user_key);
        if kv.guarded_get(&record).await.with_context(|| format!("reading {record} in {DEVICES_BUCKET}"))?.is_some() {
            return Ok(self.skipped(revoked_entry(&entry, &verified.user_key)));
        }
        Ok(Lookup::Verified(verified))
    }

    /// The device `assistant` is enrolled as by a grant standing, if it is `device`.
    async fn enrolled(&self, assistant: &UserId, device: &DeviceId) -> Lookup {
        let enrolled = self
            .grants()
            .await
            .iter()
            .filter_map(VerifiedDevice::enrolled)
            .find(|d| d.assistant.as_ref() == Some(assistant) && d.device == *device);
        enrolled.map_or(Lookup::Missing, Lookup::Verified)
    }

    /// Every grant standing that verifies, the others warned of.
    async fn grants(&self) -> Vec<VerifiedGrant> {
        let Some(kv) = &self.grants else { return Vec::new() };
        let keys: Result<Vec<String>> = async { Ok(kv.keys().await?.try_collect().await?) }.await;
        let keys = keys.unwrap_or_else(|e| {
            tracing::warn!("no assistant taken as a device, {HUMANS_BUCKET} not listed: {e:#}");
            Vec::new()
        });
        let grants = keys.iter().filter(|k| assistant_grant_of(k).is_some()).map(|key| self.grant(key));
        futures::future::join_all(grants).await.into_iter().flatten().collect()
    }

    /// The grant under `key` in [`HUMANS_BUCKET`], if it verifies; why not is warned of.
    async fn grant(&self, key: &str) -> Option<VerifiedGrant> {
        let kv = self.grants.as_ref()?;
        let value = kv
            .guarded_get(key)
            .await
            .inspect_err(|e| tracing::warn!("not reading {key} in {HUMANS_BUCKET}: {e}"))
            .ok()??;
        let verified = serde_json::from_slice::<AssistantGrant>(&value)
            .map_err(|e| e.to_string())
            .and_then(|grant| grant.verify(key, &self.account, chrono::Utc::now()).map_err(|e| e.to_string()));
        verified.inspect_err(|e| tracing::warn!("{HUMANS_BUCKET} entry {key}: {e}; its assistant is not used")).ok()
    }

    fn skipped(&self, skipped: Skipped) -> Lookup {
        skipped.log();
        Lookup::Skipped(skipped)
    }

    fn check(
        &self,
        entry: &str,
        jwt: &[u8],
        now: u64,
        revoked: impl Fn(&str) -> bool,
    ) -> Result<VerifiedDevice, Skipped> {
        let skip = |reason: String| Skipped { entry: entry.to_owned(), reason, revoked: false };
        let (human, device) =
            device_entry_of(entry).ok_or_else(|| skip("not the key of a device's entry".to_owned()))?;
        let jwt = std::str::from_utf8(jwt).map_err(|_| skip("not a user JWT".to_owned()))?;
        let verified = verify(&self.account, &human, &device, jwt, now).map_err(skip)?;
        if revoked(&verified.user_key) {
            return Err(revoked_entry(entry, &verified.user_key));
        }
        Ok(verified)
    }
}

fn revoked_entry(entry: &str, user_key: &str) -> Skipped {
    Skipped { entry: entry.to_owned(), reason: format!("its credentials {user_key} are revoked"), revoked: true }
}

/// Seconds since the epoch.
fn now() -> u64 {
    u64::try_from(chrono::Utc::now().timestamp()).unwrap_or_default()
}
