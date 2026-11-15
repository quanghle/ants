//! The directory of human devices' encryption keys, the KV bucket [`DEVICES_BUCKET`]: each human's CLI puts there the
//! user JWT of its device, which binds the device's encryption key ([`DeviceKey`]), on connecting ([`publish_or_warn`]);
//! `ants auth revoke` and `ants auth push` put a record of each revoked key of a human's credentials
//! ([`record_revocations`]); and a sender finds there the keys of the devices it seals for ([`Directory`]), taking each
//! only from a JWT it verifies itself ([`verify`]), so no human puts a key for another's device.
#![cfg_attr(not(test), expect(dead_code, reason = "the sealing of human messages (ants-wk3mo.4) reads the directory"))]

use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::creds::{Finding, Kind};
use ants_proto::{
    DEVICES_BUCKET, DeviceId, DeviceKey, DeviceRevocation, ENROLL_INBOX, JsDomain, REVOKED_DEVICES, UserId,
    device_entry_key, device_entry_of, revoked_device_key,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::context::GetStreamErrorKind;
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, ErrorCode, kv};
use futures::TryStreamExt as _;

use crate::{auth, revoke};

/// How long `ants auth revoke` and `ants auth push` wait for the hub to take the revocation records.
const REVOKER_TIMEOUT: Duration = Duration::from_secs(10);

/// The device of a human the CLI's credentials are for, with the user JWT binding its encryption key, which its CLI
/// puts in the directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Own {
    pub human: UserId,
    pub device: DeviceId,
    /// The user JWT of the credentials.
    pub jwt: String,
}

impl Own {
    /// The device of the credentials file `creds`, if a human's credentials binding an encryption key; none for any
    /// other, and without credentials.
    pub fn from_creds(creds: Option<&Path>) -> Result<Option<Self>> {
        let Some(path) = creds else { return Ok(None) };
        let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
        Ok(Self::from_creds_text(&text))
    }

    fn from_creds_text(text: &str) -> Option<Self> {
        let jwt = auth::creds_jwt(text)?;
        let finding = Finding::from_jwt(jwt).ok()?;
        let (Kind::Human, Some(device), Some(_)) = (&finding.kind, finding.device, &finding.xkey) else { return None };
        Some(Self { human: finding.name.parse().ok()?, device, jwt: jwt.to_owned() })
    }
}

/// The account that signed the credentials file `creds`, whose devices' keys alone a reader of the directory takes
/// ([`Directory::open`]); none without credentials.
pub fn account_of(creds: Option<&Path>) -> Result<Option<String>> {
    let Some(path) = creds else { return Ok(None) };
    let text = std::fs::read_to_string(path).with_context(|| format!("reading credentials {}", path.display()))?;
    let claims = crate::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
    let iss = claims["iss"].as_str().with_context(|| format!("credentials {} without an issuer", path.display()))?;
    Ok(Some(iss.to_owned()))
}

/// The devices bucket, created when missing.
async fn ensure(js: &jetstream::Context) -> Result<kv::Store> {
    if let Ok(store) = js.get_key_value(DEVICES_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(ants_daemon::hub::devices_config())
        .await
        .with_context(|| format!("creating the bucket {DEVICES_BUCKET}"))
}

/// Put the user JWT of `own` in the directory, creating the bucket when missing, unless it is there already: one
/// direct get then. Returns whether it put it.
pub async fn publish(js: &jetstream::Context, own: &Own) -> Result<bool> {
    let key = device_entry_key(&own.human, &own.device);
    let bucket = js.get_stream_no_info(format!("KV_{DEVICES_BUCKET}")).await?;
    match bucket.direct_get_last_for_subject(format!("$KV.{DEVICES_BUCKET}.{key}")).await {
        Ok(there) if *there.payload == *own.jwt.as_bytes() => return Ok(false),
        Ok(_) => {}
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => {}
        // No bucket, or one without direct gets: putting tells.
        Err(e) if matches!(e.kind(), DirectGetErrorKind::ErrorResponse(..)) => {}
        Err(e) => return Err(e).with_context(|| format!("reading {key} in {DEVICES_BUCKET}")),
    }
    let kv = ensure(js).await?;
    kv.put(&key, own.jwt.clone().into()).await.with_context(|| format!("putting {key} in {DEVICES_BUCKET}"))?;
    Ok(true)
}

/// [`publish`], warning on failure: the directory lacking the key only keeps senders from sealing for the device.
pub async fn publish_or_warn(js: &jetstream::Context, own: &Own) {
    match publish(js, own).await {
        Ok(true) => {
            tracing::debug!("put the encryption key of device {} of {} in {DEVICES_BUCKET}", own.device, own.human);
        }
        Ok(false) => {}
        Err(e) => tracing::warn!(
            "the encryption key of device {} is not in the directory, so no message is sealed for it yet: {e:#}",
            own.device
        ),
    }
}

/// A device's encryption key, taken from a user JWT the reader verified ([`verify`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedDevice {
    pub human: UserId,
    pub device: DeviceId,
    pub key: DeviceKey,
    /// The public key of the credentials whose user JWT binds it, `U...`.
    pub user_key: String,
    /// When those credentials expire, in seconds since the epoch; none when they never do.
    pub expires_at: Option<u64>,
}

/// An entry of the directory a reader took no key from, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    /// Its key in the bucket.
    pub entry: String,
    pub reason: String,
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
    /// The verified devices of `human`.
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
    let (claims, body) = auth::verified_body(jwt).map_err(|e| format!("{e:#}"))?;
    if claims.iss != account {
        return Err(format!("signed by {}, not by the team's account {account}", claims.iss));
    }
    if body["nats"]["type"] != "user" {
        return Err("not a user JWT".to_owned());
    }
    auth::check_user_key(&claims.sub).map_err(|e| format!("{e:#}"))?;
    if claims.name != human.as_str() {
        return Err(format!("names {}, not {human}", claims.name));
    }
    let finding = Finding::from_claims(&body).map_err(|e| format!("{e:#}"))?;
    if finding.kind != Kind::Human {
        return Err("not a human's credentials".to_owned());
    }
    if finding.device != Some(*device) {
        return Err(format!("for device {}", finding.device.map_or_default(|d| d.to_string())));
    }
    let Some(key) = finding.xkey else { return Err("binds no encryption key".to_owned()) };
    if let Some(exp) = claims.exp.filter(|exp| *exp <= now) {
        return Err(format!("expired at {exp}"));
    }
    Ok(VerifiedDevice { human: human.clone(), device: *device, key, user_key: claims.sub, expires_at: claims.exp })
}

/// The directory as a reader sees it: the keys it takes are only those of user JWTs that the account `account` signed
/// ([`verify`]), and whose user key has no revocation record.
pub struct Directory {
    /// None while the bucket does not exist: no human put a key yet.
    kv: Option<kv::Store>,
    account: String,
}

impl Directory {
    /// The directory on the hub `js` reaches, read for the team of the account `account` ([`account_of`]).
    pub async fn open(js: &jetstream::Context, account: &str) -> Result<Self> {
        let kv = match js.get_key_value(DEVICES_BUCKET).await {
            Ok(kv) => Some(kv),
            Err(e) => match js.get_stream(format!("KV_{DEVICES_BUCKET}")).await {
                Err(missing) if matches!(missing.kind(), GetStreamErrorKind::JetStream(e) if e.error_code() == ErrorCode::STREAM_NOT_FOUND) => {
                    None
                }
                _ => return Err(e).with_context(|| format!("opening {DEVICES_BUCKET}")),
            },
        };
        Ok(Self { kv, account: account.to_owned() })
    }

    /// The devices of `humans` whose keys the directory holds, reporting each entry of theirs it skips: one listing of
    /// the bucket's keys, and a get of each entry of those humans.
    pub async fn devices(&self, humans: &[UserId]) -> Result<Devices> {
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
        let theirs =
            keys.iter().filter(|k| k.split('.').next().is_some_and(|h| humans.iter().any(|x| x.as_str() == h)));
        let gets = theirs.map(|entry| async move {
            let jwt = kv.get(entry).await.with_context(|| format!("reading {entry} in {DEVICES_BUCKET}"))?;
            anyhow::Ok((entry, jwt))
        });
        let now = auth::now()?;
        let mut devices = Devices::default();
        for (entry, jwt) in futures::future::try_join_all(gets).await? {
            // Deleted since listed.
            let Some(jwt) = jwt else { continue };
            match self.check(entry, &jwt, now, |user_key| revoked.contains(user_key)) {
                Ok(device) => devices.verified.push(device),
                Err(skipped) => {
                    tracing::warn!("{skipped}; its key is not used");
                    devices.skipped.push(skipped);
                }
            }
        }
        Ok(devices)
    }

    /// What the directory says of `device` of `human`: two direct gets, of its entry and of the revocation record of
    /// the key of its credentials. A skipped entry is reported.
    pub async fn device(&self, human: &UserId, device: &DeviceId) -> Result<Lookup> {
        let Some(kv) = &self.kv else { return Ok(Lookup::Missing) };
        let entry = device_entry_key(human, device);
        let Some(jwt) = kv.get(&entry).await.with_context(|| format!("reading {entry} in {DEVICES_BUCKET}"))? else {
            return Ok(Lookup::Missing);
        };
        let now = auth::now()?;
        let verified = match self.check(&entry, &jwt, now, |_| false) {
            Ok(verified) => verified,
            Err(skipped) => return Ok(self.skipped(skipped)),
        };
        let record = revoked_device_key(&verified.user_key);
        if kv.get(&record).await.with_context(|| format!("reading {record} in {DEVICES_BUCKET}"))?.is_some() {
            return Ok(self.skipped(revoked_entry(&entry, &verified.user_key)));
        }
        Ok(Lookup::Verified(verified))
    }

    fn skipped(&self, skipped: Skipped) -> Lookup {
        tracing::warn!("{skipped}; its key is not used");
        Lookup::Skipped(skipped)
    }

    fn check(
        &self,
        entry: &str,
        jwt: &[u8],
        now: u64,
        revoked: impl Fn(&str) -> bool,
    ) -> Result<VerifiedDevice, Skipped> {
        let skip = |reason: String| Skipped { entry: entry.to_owned(), reason };
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
    Skipped { entry: entry.to_owned(), reason: format!("its credentials {user_key} are revoked") }
}

/// The revocations of the team in `dir` that the devices bucket keeps a record of: those of every key of a human's
/// credentials, and of every key the registry does not know, which may be one.
fn device_revocations(dir: &Path) -> Result<Vec<DeviceRevocation>> {
    let issued = revoke::issued(dir)?;
    let human_or_unknown = |key: &str| {
        let mut known = issued.iter().filter(|i| i.key == key).peekable();
        known.peek().is_none() || known.any(|i| i.kind == revoke::Kind::Human)
    };
    let revoked = revoke::revoked(dir)?.into_iter().filter(|r| human_or_unknown(&r.key));
    Ok(revoked.map(|r| DeviceRevocation { key: r.key, revoked_at: r.revoked_at }).collect())
}

/// Put in the devices bucket on the hub at `url`, with credentials signed by the account key in `dir`
/// ([`auth::revoker_creds`]), a record of each revoked key of a human's credentials it lacks, so senders stop sealing
/// for the devices they are for. Returns the keys recorded now. Fails if the hub is out of reach: the next `ants auth
/// push` records them.
pub async fn record_revocations(url: &str, js_domain: Option<&JsDomain>, dir: &Path) -> Result<Vec<String>> {
    let revocations = device_revocations(dir)?;
    if revocations.is_empty() {
        return Ok(Vec::new());
    }
    bus::check_url(url)?;
    let creds = auth::revoker_creds(dir, js_domain)?;
    let connect = async_nats::ConnectOptions::with_credentials(&creds)?
        .name("ants-auth")
        .custom_inbox_prefix(ENROLL_INBOX)
        .connection_timeout(REVOKER_TIMEOUT)
        .connect(url);
    let client = tokio::time::timeout(REVOKER_TIMEOUT, connect).await.context("no answer from the server")??;
    let mut js = bus::jetstream(client, js_domain);
    js.set_timeout(REVOKER_TIMEOUT);
    let kv = ensure(&js).await?;
    let mut recorded = Vec::new();
    for revocation in revocations {
        let key = revoked_device_key(&revocation.key);
        if kv.get(&key).await.with_context(|| format!("reading {key} in {DEVICES_BUCKET}"))?.is_some() {
            continue;
        }
        let record = serde_json::to_vec(&revocation)?;
        kv.put(&key, record.into()).await.with_context(|| format!("putting {key} in {DEVICES_BUCKET}"))?;
        recorded.push(revocation.key);
    }
    Ok(recorded)
}

#[cfg(test)]
mod tests;
