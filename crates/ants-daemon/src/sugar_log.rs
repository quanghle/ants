//! Sugar's event logs in the hub's [`SUGAR_STREAM`], which the daemon reaches with its host credentials alone
//! ([`ants_proto::Permissions::for_host`]): the stream ([`sugar_config`]), the [`EventLog`] over it ([`JetStreamLog`]),
//! the key of an ant's credentials signing its events ([`AntSigner`]) and the revocation records events are checked
//! against ([`DeviceRevocations`]).

use std::path::Path;
use std::time::{Duration, SystemTime};

use ants_proto::{
    DEVICES_BUCKET, DeviceRevocation, JsDomain, SUGAR_STREAM, SugarId, UserId, revoked_device_key, subject,
};
use ants_sugar::log::{AppendError, EventLog, LogError, Revocations, Signature, Signer, Stored};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::stream::{self, DirectGetErrorKind};
use async_nats::jetstream::{self, message::StreamMessage};
use async_nats::{HeaderMap, StatusCode};
use chrono::{DateTime, SecondsFormat, Utc};
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::records::direct_kv::{DirectKv, Lookup};

/// The header making the stream refuse a message unless the last on its subject has the given sequence (0: none).
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";
/// The header making the server refuse a message unless [`SUGAR_STREAM`] is the stream storing it.
const EXPECTED_STREAM: &str = "Nats-Expected-Stream";
/// Messages asked for at most by a direct get, and the bytes they come to at most (a larger message comes alone).
const DIRECT_BATCH: usize = 256;
const DIRECT_BATCH_BYTES: usize = 4 << 20;
/// How long a read waits for each message of a batch.
const DIRECT_WAIT: Duration = Duration::from_secs(10);

/// The stream of sugar's event logs, one subject per sugar identity ([`subject::sugar`]): file-backed, keeping every
/// event for good, denying deletes and purges, and serving direct gets. Every host creates it with this config alone,
/// and writes only once its own create succeeded, which nats-server refuses for an existing stream of another config:
/// no stream another created otherwise, such as one republishing what it stores, ever gets an event.
pub fn sugar_config() -> stream::Config {
    stream::Config {
        name: SUGAR_STREAM.to_owned(),
        description: Some("Sugar's event log of each ant, one subject per sugar identity".into()),
        subjects: vec![subject::ALL_SUGAR.to_owned()],
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    }
}

/// The event logs in [`SUGAR_STREAM`], over a host connection. Its incarnation is the stream's creation time, which a
/// stream created again, after one deleted, has anew.
#[derive(Clone)]
pub struct JetStreamLog {
    js: jetstream::Context,
    stream: stream::Stream<()>,
    incarnation: String,
}

impl JetStreamLog {
    /// The logs, over `js`, a host connection's JetStream context ([`crate::bus::jetstream`]), creating the stream
    /// when missing ([`sugar_config`]). Fails when the stream exists with another config.
    pub async fn open(js: &jetstream::Context) -> Result<Self> {
        let created = js.create_stream(sugar_config()).await.with_context(|| {
            format!("creating the {SUGAR_STREAM} stream, or finding it with the config every host creates it with")
        })?;
        let created: SystemTime = created.cached_info().created.into();
        let incarnation = DateTime::<Utc>::from(created).to_rfc3339_opts(SecondsFormat::Nanos, true);
        let stream = js.get_stream_no_info(SUGAR_STREAM).await?;
        Ok(Self { js: js.clone(), stream, incarnation })
    }

    async fn last(&self, sugar: &SugarId) -> Result<u64, LogError> {
        match self.stream.direct_get_last_for_subject(subject::sugar(sugar)).await {
            Ok(msg) => Ok(msg.sequence),
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(0),
            Err(e) => Err(LogError(format!("reading the last event of sugar identity {sugar}: {e}"))),
        }
    }

    async fn read(&self, sugar: &SugarId, after: u64, limit: usize) -> Result<Vec<Stored>> {
        let client = self.js.client();
        let inbox = client.new_inbox();
        let mut replies = client.subscribe(inbox.clone()).await?;
        let subject = subject::sugar(sugar);
        let request = serde_json::json!({
            "seq": after + 1,
            "next_by_subj": subject,
            "batch": limit.clamp(1, DIRECT_BATCH),
            "max_bytes": DIRECT_BATCH_BYTES,
        });
        let request = async_nats::Request::new().inbox(inbox).payload(serde_json::to_vec(&request)?.into());
        self.js.send_request(format!("DIRECT.GET.{SUGAR_STREAM}"), request).await?;
        let mut read = Vec::new();
        loop {
            let Ok(Some(msg)) = tokio::time::timeout(DIRECT_WAIT, replies.next()).await else {
                bail!("no answer to a direct get of {SUGAR_STREAM}");
            };
            match msg.status {
                None => {}
                Some(StatusCode::NOT_FOUND) => return Ok(read),
                Some(status) if status.as_u16() == 204 => return Ok(read),
                Some(status) => bail!("a direct get of {SUGAR_STREAM} answered {status} {:?}", msg.description),
            }
            let msg = StreamMessage::try_from(msg).map_err(|e| anyhow!(e))?;
            if msg.subject.as_str() != subject {
                bail!("a direct get of {subject} answered with a message on {}", msg.subject);
            }
            let stored_at: SystemTime = msg.time.into();
            read.push(Stored { sequence: msg.sequence, stored_at: stored_at.into(), payload: msg.payload.to_vec() });
        }
    }

    async fn publish(&self, sugar: &SugarId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        let mut headers = HeaderMap::new();
        headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, expected_last.to_string().as_str());
        headers.insert(EXPECTED_STREAM, SUGAR_STREAM);
        let failed = |e: &dyn std::fmt::Display| LogError(format!("appending to sugar identity {sugar}: {e}"));
        let ack = self.js.publish_with_headers(subject::sugar(sugar), headers, payload.into()).await;
        match ack.map_err(|e| failed(&e))?.await {
            Ok(ack) => Ok(ack.sequence),
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {
                Err(AppendError::Conflict { last: self.last(sugar).await? })
            }
            Err(e) => Err(AppendError::Failed(failed(&e))),
        }
    }
}

impl EventLog for JetStreamLog {
    async fn append(&self, sugar: &SugarId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.publish(sugar, payload, expected_last).await
    }

    async fn read_after(&self, sugar: &SugarId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        let read = self.read(sugar, after, limit).await;
        read.map_err(|e| LogError(format!("reading sugar identity {sugar}: {e:#}")))
    }

    async fn last_sequence(&self, sugar: &SugarId) -> Result<u64, LogError> {
        self.last(sugar).await
    }

    fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

/// What signs an ant's events: the key of the ant's credentials, with their JWT; nothing on a server without
/// authentication.
pub struct AntSigner(Option<(String, KeyPair)>);

impl AntSigner {
    /// The ant's credentials `jwt`, whose key is `key`.
    pub fn new(jwt: &str, key: KeyPair) -> Self {
        Self(Some((jwt.to_owned(), key)))
    }

    /// The credentials of `ant` at `creds`; without any, nothing where `accounts` is empty (a server without
    /// authentication), else fail.
    pub fn load(ant: &UserId, creds: Option<&Path>, accounts: &[String]) -> Result<Self> {
        match creds {
            Some(creds) => {
                let (jwt, key) = crate::secrets::creds::load(creds, crate::secrets::keystore::os())?;
                Ok(Self::new(&jwt, key))
            }
            None if accounts.is_empty() => Ok(Self(None)),
            None => bail!("no credentials of {ant} on this machine sign its sugar events"),
        }
    }
}

impl Signer for AntSigner {
    fn sign(&self, bytes: &[u8]) -> Result<Option<Signature>, String> {
        let Some((jwt, key)) = &self.0 else { return Ok(None) };
        let signature = key.sign(bytes).map_err(|e| e.to_string())?;
        Ok(Some(Signature { credentials: jwt.clone(), signature: BASE64URL_NOPAD.encode(&signature) }))
    }
}

/// The revocation records of credentials in [`DEVICES_BUCKET`], which host credentials read by direct gets.
pub struct DeviceRevocations(DirectKv);

impl DeviceRevocations {
    /// Over `js`, made for `js_domain` ([`crate::bus::jetstream`]).
    pub async fn new(js: &jetstream::Context, js_domain: Option<&JsDomain>) -> Result<Self> {
        Ok(Self(DirectKv::new(js, js_domain, DEVICES_BUCKET).await?))
    }
}

impl Revocations for DeviceRevocations {
    async fn revoked_at(&self, key: &str) -> Result<Option<u64>, String> {
        match self.0.lookup(revoked_device_key(key)).await.map_err(|e| format!("{e:#}"))? {
            Lookup::Value(value) => {
                let revocation: DeviceRevocation =
                    serde_json::from_slice(&value).map_err(|e| format!("the revocation record of {key}: {e}"))?;
                Ok(Some(revocation.revoked_at))
            }
            Lookup::NoValue | Lookup::NoBucket => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests;
