//! The directory of human devices' encryption keys, the KV bucket [`DEVICES_BUCKET`]: each human's CLI puts there the
//! user JWT of its device, which binds the device's encryption key ([`ants_proto::DeviceKey`]), on connecting ([`publish_or_warn`]);
//! `ants auth revoke` and `ants auth push` put a record of each revoked key of a human's credentials
//! ([`record_revocations`]); and a sender finds there the keys of the devices it seals for ([`Directory`]), taking each
//! only from a JWT it verifies itself ([`ants_daemon::people::devices::verify`]), so no human puts a key for another's device, besides the assistants
//! the humans enrolled as devices by their grants.

use ants_daemon::bus::relayed::GuardedKv as _;
use std::path::Path;
use std::time::Duration;

use ants_daemon::bus;
pub use ants_daemon::people::devices::{Devices, Directory, Lookup, VerifiedDevice};
use ants_daemon::profile::identity;
use ants_daemon::secrets::creds::{Finding, Kind};
use ants_proto::{
    DEVICES_BUCKET, DeviceId, DeviceRevocation, ENROLL_INBOX, IDENTITIES_BUCKET, JsDomain, UserId, device_entry_key,
    revoked_device_key, subject,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::stream::DirectGetErrorKind;
use async_nats::jetstream::{self, kv};

use crate::auth;
use crate::credentials::issued;
use crate::credentials::revoke;

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
    let claims =
        crate::people::humans::creds_claims(&text).with_context(|| format!("credentials {}", path.display()))?;
    let iss = claims["iss"].as_str().with_context(|| format!("credentials {} without an issuer", path.display()))?;
    Ok(Some(iss.to_owned()))
}

/// The devices bucket, created as `ants init` does when missing.
async fn ensure(js: &jetstream::Context) -> Result<kv::Store> {
    Box::pin(ants_daemon::hub::ensure_bucket(js, ants_daemon::hub::Object::Devices))
        .await
        .with_context(|| format!("opening the bucket {DEVICES_BUCKET}"))
}

/// Put the user JWT of `own` in the directory, creating the bucket when missing, unless it is there already: one
/// direct get then. Returns whether it put it.
pub async fn publish(js: &jetstream::Context, own: &Own) -> Result<bool> {
    let key = device_entry_key(&own.human, &own.device);
    let bucket = js.get_stream_no_info(subject::kv_stream(DEVICES_BUCKET)).await?;
    match bucket.direct_get_last_for_subject(subject::kv(DEVICES_BUCKET, &key)).await {
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

/// The revocations of the bus in `dir` that the devices bucket keeps a record of: those of every key of a human's
/// credentials (an admin's among them) and of an assistant's, whose roles readers honour only from a key without one,
/// of an ant's, whose shared settings readers take only signed with a key without one, and of every key the registry
/// does not know, which may be one.
fn device_revocations(dir: &Path) -> Result<Vec<DeviceRevocation>> {
    let issued = issued::issued(dir)?;
    let human_or_unknown = |key: &str| {
        let mut known = issued.iter().filter(|i| i.key == key).peekable();
        known.peek().is_none()
            || known.any(|i| matches!(i.kind, issued::Kind::Human | issued::Kind::Assistant | issued::Kind::Ant))
    };
    let revoked = revoke::revoked(dir)?.into_iter().filter(|r| human_or_unknown(&r.key));
    Ok(revoked.map(|r| DeviceRevocation { key: r.key, revoked_at: r.revoked_at }).collect())
}

/// What [`record_revocations`] did: the revoked keys it recorded now, and the device entries of revoked keys it
/// deleted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Recorded {
    pub keys: Vec<String>,
    /// `<human>.<device>`.
    pub cleared: Vec<String>,
}

/// How many device entries one set of short-lived credentials deletes, keeping its user JWT short.
const ENTRIES_A_REMOVER: usize = 16;

/// Put in the devices bucket on the hub at `url`, with credentials signed by the account key in `dir`
/// ([`auth::revoker_creds`]), a record of each revoked key of a human's or an ant's credentials it lacks, so senders
/// stop sealing for the devices they are for and readers refuse the shared settings they signed; then delete each
/// human's device entry still holding the user JWT of a revoked key the registry knows, recorded now or earlier, with
/// credentials allowed only those entries ([`auth::device_entry_remover_creds`]), so no reader is left skipping it. An
/// entry a renewal replaced in between is kept. Fails if the hub is out of reach, or has no devices bucket, which these
/// credentials cannot create as `ants init` does: the next `ants auth push` records them.
pub async fn record_revocations(url: &str, js_domain: Option<&JsDomain>, dir: &Path) -> Result<Recorded> {
    let revocations = device_revocations(dir)?;
    if revocations.is_empty() {
        return Ok(Recorded::default());
    }
    let js = connect(url, js_domain, &auth::revoker_creds(dir, js_domain)?).await?;
    let kv = js
        .get_key_value(DEVICES_BUCKET)
        .await
        .with_context(|| format!("opening the bucket {DEVICES_BUCKET} (`ants init` as a human creates it)"))?;
    let mut recorded = Recorded::default();
    for revocation in &revocations {
        let key = revoked_device_key(&revocation.key);
        if kv.guarded_get(&key).await.with_context(|| format!("reading {key} in {DEVICES_BUCKET}"))?.is_some() {
            continue;
        }
        let record = serde_json::to_vec(revocation)?;
        kv.put(&key, record.into()).await.with_context(|| format!("putting {key} in {DEVICES_BUCKET}"))?;
        recorded.keys.push(revocation.key.clone());
    }
    recorded.cleared = Box::pin(clear(url, js_domain, dir, &kv, &revocations)).await?;
    Ok(recorded)
}

/// Delete the device entries [`stale_entries`] finds, returning those deleted.
async fn clear(
    url: &str,
    js_domain: Option<&JsDomain>,
    dir: &Path,
    kv: &kv::Store,
    revocations: &[DeviceRevocation],
) -> Result<Vec<String>> {
    let stale = stale_entries(kv, dir, revocations).await?;
    let mut cleared = Vec::new();
    for chunk in stale.chunks(ENTRIES_A_REMOVER) {
        let entries: Vec<String> = chunk.iter().map(|(entry, _)| entry.clone()).collect();
        let creds = auth::device_entry_remover_creds(dir, js_domain, &entries)?;
        let kv = connect(url, js_domain, &creds)
            .await?
            .get_key_value(DEVICES_BUCKET)
            .await
            .with_context(|| format!("opening the bucket {DEVICES_BUCKET}"))?;
        for (entry, revision) in chunk {
            match kv.delete_expect_revision(entry, Some(*revision)).await {
                Ok(()) => cleared.push(entry.clone()),
                // Put again since read, by a renewal of the device: its key is no longer the revoked one.
                Err(e) if e.kind() == kv::DeleteErrorKind::WrongLastRevision => {}
                Err(e) => return Err(e).with_context(|| format!("deleting {entry} in {DEVICES_BUCKET}")),
            }
        }
    }
    Ok(cleared)
}

/// The device entries of humans' devices in `kv`, with their revisions, still holding the user JWT of a key of
/// `revocations` that the registry of `dir` knows as a device's.
async fn stale_entries(kv: &kv::Store, dir: &Path, revocations: &[DeviceRevocation]) -> Result<Vec<(String, u64)>> {
    let issued = issued::issued(dir)?;
    let mut entries: Vec<(String, &str)> = issued
        .iter()
        .filter(|i| i.kind == issued::Kind::Human && revocations.iter().any(|r| r.key == i.key))
        .filter_map(|i| Some((device_entry_key(&i.name.parse().ok()?, &i.device?), i.key.as_str())))
        .collect();
    entries.sort_unstable();
    entries.dedup();
    let mut stale = Vec::new();
    for (entry, key) in entries {
        let Some(found) =
            kv.guarded_entry(&entry).await.with_context(|| format!("reading {entry} in {DEVICES_BUCKET}"))?
        else {
            continue;
        };
        let holds = found.operation == kv::Operation::Put
            && std::str::from_utf8(&found.value)
                .ok()
                .and_then(|jwt| auth::verified(jwt).ok())
                .is_some_and(|c| c.sub == key);
        if holds {
            stale.push((entry, found.revision));
        }
    }
    Ok(stale)
}

/// Delete `ant`'s identity record and shared settings from the hub at `url`, keeping its sense identity's growth.
pub async fn forget_identity(url: &str, js_domain: Option<&JsDomain>, dir: &Path, ant: &UserId) -> Result<()> {
    let js = connect(url, js_domain, &auth::revoker_creds(dir, js_domain)?).await?;
    identity::forget(&js, js_domain, ant).await.with_context(|| {
        format!("the credentials were revoked, but clearing {ant}'s identity and settings in {IDENTITIES_BUCKET} did not finish: revoke them again by name")
    })
}

/// A JetStream context, in domain `js_domain` if the hub has one, of the short-lived credentials `creds` signed with
/// the account key, connected to the server at `url`.
async fn connect(url: &str, js_domain: Option<&JsDomain>, creds: &str) -> Result<jetstream::Context> {
    bus::check_url(url)?;
    let connect = async_nats::ConnectOptions::with_credentials(creds)?
        .name("ants-auth")
        .custom_inbox_prefix(ENROLL_INBOX)
        .connection_timeout(REVOKER_TIMEOUT)
        .connect(url);
    let client = tokio::time::timeout(REVOKER_TIMEOUT, connect).await.context("no answer from the server")??;
    let mut js = bus::jetstream(client, js_domain);
    js.set_timeout(REVOKER_TIMEOUT);
    Ok(js)
}

#[cfg(test)]
mod tests;
