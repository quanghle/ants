//! Delivering events to the hub ([`Bus`]): each webhook's connection, deduplicated by event id in
//! [`WEBHOOKS_BUCKET`], to its ant or to the members of its channel.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ants_daemon::bus;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::records::owners;
use ants_daemon::schedule::Mentions;
use ants_daemon::sync::MutexExt as _;
use ants_proto::{
    CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, Destination, Envelope, JsDomain, SERVING_EVERY, ThreadId,
    TrailStart, UserId, WEBHOOKS_BUCKET, subject, webhook_marker_key,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use async_nats::jetstream::stream::DirectGetErrorKind;
use serde::{Deserialize, Serialize};

use super::ACK_TIMEOUT;

/// The bucket of delivered events ([`WEBHOOKS_BUCKET`]), created when missing and the credentials allow it (a
/// human's), as `ants init` does.
#[cfg(test)]
pub async fn ensure_bucket(js: &jetstream::Context) -> Result<jetstream::kv::Store> {
    if let Ok(store) = js.get_key_value(WEBHOOKS_BUCKET).await {
        return Ok(store);
    }
    js.create_key_value(ants_daemon::hub::webhooks_config())
        .await
        .context("creating the webhooks bucket (with credentials, `ants init` as a human creates it)")
}

/// What [`WEBHOOKS_BUCKET`] keeps of a delivered event.
#[derive(Serialize, Deserialize)]
pub(super) struct Seen {
    thread: ThreadId,
    seq: u64,
}

/// A webhook's, or a schedule's, connection to the hub, as itself.
pub(crate) struct Bus {
    pub(crate) client: async_nats::Client,
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    /// Without stream info, which would list the events of every webhook.
    seen: DirectKv,
    /// What its marker holds: the version of ants this runs ([`ants_proto::Serving`]).
    marker: Vec<u8>,
    /// When it last put its marker.
    marked: Arc<Mutex<Option<Instant>>>,
    /// Puts the marker every [`SERVING_EVERY`].
    keeper: tokio::task::JoinHandle<()>,
}

impl Drop for Bus {
    fn drop(&mut self) {
        self.keeper.abort();
    }
}

/// An event to deliver, once.
pub(crate) struct Delivery<'a> {
    pub(crate) from: &'a UserId,
    pub(crate) destination: &'a Destination,
    pub(crate) mentions: &'a Mentions,
    pub(crate) owner: Option<&'a UserId>,
    /// The tool rules the thread is narrowed to ([`Envelope::allow`]).
    pub(crate) allow: Option<&'a [String]>,
    /// Its key in [`WEBHOOKS_BUCKET`] ([`webhook_seen_key`]).
    ///
    /// [`webhook_seen_key`]: ants_proto::webhook_seen_key
    pub(crate) key: String,
    /// The stream's `Nats-Msg-Id` of the message.
    pub(crate) message_id: String,
    pub(crate) text: String,
    /// The trail run the ant is to start ([`Envelope::trail_start`]); for an ant only.
    pub(crate) trail: Option<&'a TrailStart>,
}

/// The thread an event went to, if known, and whether it went there before.
pub(crate) struct Delivered {
    pub(crate) thread: Option<ThreadId>,
    pub(crate) duplicate: bool,
}

impl Bus {
    /// Connect as `name`, and open the bucket of delivered events, waiting for both as long as it takes.
    pub(crate) async fn connect(
        name: &UserId,
        creds: Option<&Path>,
        nats: &str,
        js_domain: Option<&JsDomain>,
        client_name: &str,
    ) -> Result<Self> {
        let pending = std::future::pending();
        let (client, mut js) =
            bus::connect_retrying(nats, creds, js_domain, name, client_name, pending).await?.context("stopped")?;
        js.set_timeout(ACK_TIMEOUT);
        let seen = DirectKv::new(&js, js_domain, WEBHOOKS_BUCKET).await?;
        let mut delay = Duration::from_secs(1);
        loop {
            let error = match seen.exists(name).await {
                Ok(true) => break,
                Ok(false) => format!("no bucket {WEBHOOKS_BUCKET}"),
                Err(e) => format!("{e:#}"),
            };
            tracing::warn!(user = %name, error, retry_in = ?delay, "no bucket {WEBHOOKS_BUCKET} yet: `ants init` creates it");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
        let (marker, marked) = (crate::release::versions::serving().await, Arc::new(Mutex::new(None)));
        let keeping = (seen.clone(), webhook_marker_key(name), marker.clone(), marked.clone());
        let keeper = tokio::spawn(async move {
            let (seen, key, marker, marked) = keeping;
            let put = move || *marked.locked() = Some(Instant::now());
            crate::release::versions::keep_putting(seen, key, marker, put).await;
        });
        Ok(Self { client, js, js_domain: js_domain.cloned(), seen, marker, marked, keeper })
    }

    /// Deliver `event` as a new thread unless it was delivered already, and return once the hub's stream stored it.
    pub(crate) async fn deliver(&self, event: Delivery<'_>) -> Result<Delivered> {
        let Delivery { from, destination, mentions, owner, allow, key, message_id, text, trail } = event;
        let allow = allow.map(<[String]>::to_vec);
        if let Some(seen) = self.seen(&key).await? {
            return Ok(Delivered { thread: Some(seen.thread), duplicate: true });
        }
        self.mark(from).await?;
        let (subject, payload, thread) = match destination {
            Destination::Ant(ant) => {
                let message = Envelope::new(from.clone(), ant.clone(), text)
                    .with_owner(owner.cloned())
                    .with_allow(allow)
                    .starting(trail.cloned())
                    .from_webhook();
                if let Some(owner) = owner {
                    owners::recording(&self.js, self.js_domain.as_ref(), &message.thread_id, ant, owner).await;
                }
                (message.subject(), serde_json::to_vec(&message)?, message.thread_id)
            }
            Destination::Channel(channel) => {
                let mentions = match mentions {
                    Mentions::Nobody => Vec::new(),
                    Mentions::Listed(mentions) => mentions.clone(),
                    Mentions::Members => members(&self.js, channel).await?,
                };
                let post = ChannelPost::new(channel.clone(), from.clone(), text)
                    .with_owner(owner.cloned())
                    .with_allow(allow)
                    .from_webhook(mentions);
                (post.subject(), serde_json::to_vec(&post)?, post.thread_id)
            }
        };
        let message = jetstream::message::PublishMessage::build().payload(payload.into()).message_id(message_id);
        let ack = match self.js.send_publish(subject.clone(), message).await?.await {
            Ok(ack) => ack,
            Err(e) => return Err(bus::explain(&self.js, &subject, e).await).context("waiting for the hub to store it"),
        };
        if ack.duplicate {
            let seen = self.seen(&key).await.inspect_err(|e| tracing::debug!("the duplicate's thread unknown: {e:#}"));
            let thread = seen.ok().flatten().map(|seen| seen.thread);
            return Ok(Delivered { thread, duplicate: true });
        }
        let seen = serde_json::to_vec(&Seen { thread: thread.clone(), seq: ack.sequence })?;
        for attempt in 1..=3 {
            match self.seen.create(&key, seen.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    tracing::warn!(user = %from, error = format!("{e:#}"), attempt, "recording a delivered event");
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                }
            }
        }
        Ok(Delivered { thread: Some(thread), duplicate: false })
    }

    /// Put the marker of `name` ([`webhook_marker_key`]), unless put in the last [`SERVING_EVERY`]: without it, daemons
    /// take its messages for an ant's, and answer it in its own inbox.
    async fn mark(&self, name: &UserId) -> Result<()> {
        if self.marked.locked().is_some_and(|at| at.elapsed() < SERVING_EVERY) {
            return Ok(());
        }
        self.seen.put(webhook_marker_key(name), self.marker.clone()).await.context("putting the marker")?;
        *self.marked.locked() = Some(Instant::now());
        Ok(())
    }

    async fn seen(&self, key: &str) -> Result<Option<Seen>> {
        let found = self.seen.get(key).await.context("reading the webhooks bucket")?;
        Ok(found.and_then(|value| serde_json::from_slice(&value).ok()))
    }
}

/// The members of `channel` now; none when it does not exist.
pub(super) async fn members(js: &jetstream::Context, channel: &ChannelId) -> Result<Vec<UserId>> {
    let bucket = js.get_stream_no_info(format!("KV_{CHANNELS_BUCKET}")).await?;
    match bucket.direct_get_last_for_subject(subject::channel_entry(channel)).await {
        Ok(entry) if entry.payload.is_empty() => Ok(Vec::new()),
        Ok(entry) => {
            let entry: ChannelEntry = serde_json::from_slice(&entry.payload).context("reading the channel")?;
            Ok(entry.members)
        }
        Err(e) if e.kind() == DirectGetErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(e).context("reading the channel"),
    }
}
