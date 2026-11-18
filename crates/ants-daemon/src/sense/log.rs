//! Sense's event logs in the hub's [`SENSE_STREAM`], which the daemon reaches with its host credentials alone
//! ([`ants_proto::Permissions::for_host`]): the stream ([`sense_config`]), the [`EventLog`] over it ([`JetStreamLog`]),
//! the key of an ant's credentials signing its events ([`AntSigner`]) and the revocation records events are checked
//! against ([`DeviceRevocations`]).

use std::path::Path;
use std::time::{Duration, SystemTime};

use ants_proto::{
    DEVICES_BUCKET, DeviceRevocation, JsDomain, SENSE_STREAM, SenseId, UserId, revoked_device_key, subject,
};
use ants_sense::log::{AppendError, EventLog, LogError, LogErrorKind, Revocations, Signature, Signer, Stored};
use anyhow::{Result, bail};
use async_nats::jetstream::context::{CreateStreamErrorKind, PublishError, PublishErrorKind};
use async_nats::jetstream::stream::{self, DirectGetError, DirectGetErrorKind};
use async_nats::jetstream::{self, message::StreamMessage};
use async_nats::{HeaderMap, StatusCode};
use chrono::{DateTime, SecondsFormat, Utc};
use data_encoding::BASE64URL_NOPAD;
use futures::StreamExt as _;
use nkeys::KeyPair;

use crate::records::direct_kv::{DirectKv, Lookup};

/// The header making the stream refuse a message unless the last on its subject has the given sequence (0: none).
const EXPECTED_LAST_SUBJECT_SEQUENCE: &str = "Nats-Expected-Last-Subject-Sequence";
/// The header making the server refuse a message unless [`SENSE_STREAM`] is the stream storing it.
const EXPECTED_STREAM: &str = "Nats-Expected-Stream";
/// Messages asked for at most by a direct get, and the bytes they come to at most (a larger message comes alone).
const DIRECT_BATCH: usize = 256;
const DIRECT_BATCH_BYTES: usize = 4 << 20;
/// How long a read waits for each message of a batch.
const DIRECT_WAIT: Duration = Duration::from_secs(10);

/// The stream of sense's event logs, one subject per sense identity ([`subject::sense`]): file-backed, keeping every
/// event for good, denying deletes and purges, and serving direct gets. Every host creates it with this config alone,
/// and writes only once its own create succeeded, which nats-server refuses for an existing stream of another config:
/// no stream another created otherwise, such as one republishing what it stores, ever gets an event.
pub fn sense_config() -> stream::Config {
    stream::Config {
        name: SENSE_STREAM.to_owned(),
        description: Some("Sense's event log of each ant, one subject per sense identity".into()),
        subjects: vec![subject::ALL_SENSE.to_owned()],
        storage: stream::StorageType::File,
        num_replicas: 1,
        allow_direct: true,
        deny_delete: true,
        deny_purge: true,
        ..Default::default()
    }
}

/// The event logs in [`SENSE_STREAM`], over a host connection. Its incarnation is the stream's creation time, which a
/// stream created again, after one deleted, has anew.
#[derive(Clone)]
pub struct JetStreamLog {
    js: jetstream::Context,
    stream: stream::Stream<()>,
    incarnation: String,
}

impl JetStreamLog {
    /// The logs, over `js`, a host connection's JetStream context ([`crate::bus::jetstream`]), creating the stream
    /// when missing ([`sense_config`]). Fails when the stream exists with another config.
    pub async fn open(js: &jetstream::Context) -> Result<Self, LogError> {
        let created = js.create_stream(sense_config()).await.map_err(|e| {
            let kind = match e.kind() {
                CreateStreamErrorKind::TimedOut
                | CreateStreamErrorKind::JetStreamUnavailable
                | CreateStreamErrorKind::Response => LogErrorKind::Unreachable,
                _ => LogErrorKind::Refused,
            };
            let creating =
                format!("creating the {SENSE_STREAM} stream, or finding it with the config every host creates it with");
            LogError::new(kind, format!("{creating}: {e}"))
        })?;
        let created: SystemTime = created.cached_info().created.into();
        let incarnation = DateTime::<Utc>::from(created).to_rfc3339_opts(SecondsFormat::Nanos, true);
        let stream = js.get_stream_no_info(SENSE_STREAM).await.map_err(|e| LogError::refused(e.to_string()))?;
        Ok(Self { js: js.clone(), stream, incarnation })
    }

    async fn last(&self, sense: &SenseId) -> Result<u64, LogError> {
        match self.stream.direct_get_last_for_subject(subject::sense(sense)).await {
            Ok(msg) => Ok(msg.sequence),
            Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(0),
            Err(e) => {
                let kind = if unanswered(&e) { LogErrorKind::Unreachable } else { LogErrorKind::Refused };
                Err(LogError::new(kind, format!("reading the last event of sense identity {sense}: {e}")))
            }
        }
    }

    async fn read(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        let failed =
            |kind, e: &dyn std::fmt::Display| LogError::new(kind, format!("reading sense identity {sense}: {e}"));
        let client = self.js.client();
        let inbox = client.new_inbox();
        let mut replies = client.subscribe(inbox.clone()).await.map_err(|e| failed(LogErrorKind::Unreachable, &e))?;
        let subject = subject::sense(sense);
        let request = serde_json::json!({
            "seq": after + 1,
            "next_by_subj": subject,
            "batch": limit.clamp(1, DIRECT_BATCH),
            "max_bytes": DIRECT_BATCH_BYTES,
        });
        let request = async_nats::Request::new().inbox(inbox).payload(request.to_string().into());
        self.js.send_request(format!("DIRECT.GET.{SENSE_STREAM}"), request).await.map_err(|e| {
            let kind = match e.kind() {
                async_nats::PublishErrorKind::Send => LogErrorKind::Unreachable,
                _ => LogErrorKind::Refused,
            };
            failed(kind, &e)
        })?;
        let mut read = Vec::new();
        loop {
            let Ok(Some(msg)) = tokio::time::timeout(DIRECT_WAIT, replies.next()).await else {
                let none = format!("no answer to a direct get of {SENSE_STREAM}");
                return Err(failed(LogErrorKind::Unreachable, &none));
            };
            match msg.status {
                None => {}
                Some(StatusCode::NOT_FOUND) => return Ok(read),
                Some(status) if status.as_u16() == 204 => return Ok(read),
                Some(status) => {
                    let kind = if status == StatusCode::NO_RESPONDERS {
                        LogErrorKind::Unreachable
                    } else {
                        LogErrorKind::Refused
                    };
                    let answered = format!("a direct get of {SENSE_STREAM} answered {status} {:?}", msg.description);
                    return Err(failed(kind, &answered));
                }
            }
            let msg = StreamMessage::try_from(msg).map_err(|e| failed(LogErrorKind::Integrity, &e))?;
            if msg.subject.as_str() != subject {
                let other = format!("a direct get of {subject} answered with a message on {}", msg.subject);
                return Err(failed(LogErrorKind::Integrity, &other));
            }
            let stored_at: SystemTime = msg.time.into();
            read.push(Stored { sequence: msg.sequence, stored_at: stored_at.into(), payload: msg.payload.to_vec() });
        }
    }

    async fn publish(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        let mut headers = HeaderMap::new();
        headers.insert(EXPECTED_LAST_SUBJECT_SEQUENCE, expected_last.to_string().as_str());
        headers.insert(EXPECTED_STREAM, SENSE_STREAM);
        let failed = |e: &PublishError| {
            let kind = match e.kind() {
                // The ack's no responders: JetStream, on the hub, is out of reach.
                PublishErrorKind::TimedOut | PublishErrorKind::BrokenPipe | PublishErrorKind::StreamNotFound => {
                    LogErrorKind::Unreachable
                }
                _ => LogErrorKind::Refused,
            };
            LogError::new(kind, format!("appending to sense identity {sense}: {e}"))
        };
        let ack = self.js.publish_with_headers(subject::sense(sense), headers, payload.into()).await;
        match ack.map_err(|e| failed(&e))?.await {
            Ok(ack) => Ok(ack.sequence),
            Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {
                Err(AppendError::Conflict { last: self.last(sense).await? })
            }
            Err(e) => Err(AppendError::Failed(failed(&e))),
        }
    }
}

/// Whether the direct get `e` failed for want of an answer: timed out, no responders, or the connection gone.
fn unanswered(e: &DirectGetError) -> bool {
    match e.kind() {
        DirectGetErrorKind::TimedOut | DirectGetErrorKind::Request => true,
        DirectGetErrorKind::ErrorResponse(status, _) => status == StatusCode::NO_RESPONDERS,
        DirectGetErrorKind::Other => std::error::Error::source(e)
            .and_then(|source| source.downcast_ref::<async_nats::RequestError>())
            .is_some_and(|request| request.kind() == async_nats::RequestErrorKind::Other),
        DirectGetErrorKind::NotFound | DirectGetErrorKind::InvalidSubject => false,
    }
}

impl EventLog for JetStreamLog {
    async fn append(&self, sense: &SenseId, payload: Vec<u8>, expected_last: u64) -> Result<u64, AppendError> {
        self.publish(sense, payload, expected_last).await
    }

    async fn read_after(&self, sense: &SenseId, after: u64, limit: usize) -> Result<Vec<Stored>, LogError> {
        self.read(sense, after, limit).await
    }

    async fn last_sequence(&self, sense: &SenseId) -> Result<u64, LogError> {
        self.last(sense).await
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
            None => bail!("no credentials of {ant} on this machine sign its sense events"),
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
