//! The owners of the threads humans and webhooks message ants on. Before its message to an ant starts or joins a
//! thread, a human (`ants ask`) or a webhook (`ants webhook serve`) records the thread's owner for that ant in
//! [`OWNERS_BUCKET`] ([`record`]), which no ant's credentials write and whose first record of a thread to an ant stays.
//! While the hub is out of reach, a human's record waits in the machine's outbox until the bucket sources it
//! ([`owner_outbox`]). A daemon takes the owner of an ant's message on a thread no channel post started only from
//! there ([`Owners::recorded`]), for an ant the record went to or one such an ant passed the link to the thread on to,
//! so no ant names the owner of such a thread. An assistant records its human as the owner of a thread it messages an
//! ant on for the human under a key naming both ([`assisted_owner_key`], [`record_for_human`]), which a daemon takes
//! only while the human's grant names that assistant ([`Owners::assisted`]).

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ants_proto::{
    JsDomain, OWNERS_BUCKET, ThreadId, UserId, assisted_owner_key, owner_key, owner_outbox, owner_record_id,
};
use anyhow::{Context as _, Result};
use async_nats::HeaderMap;
use async_nats::jetstream;

use crate::bus;
use crate::records::direct_kv::{DirectKv, Lookup};
use crate::sync::MutexExt as _;

/// Most owners kept in memory; past it they are read again.
const MAX_KEPT: usize = 4096;

/// How often [`Owners::wait_for`] reads the bucket again.
const RECHECK: Duration = Duration::from_millis(200);

/// What a put of a second record of a thread to an ant fails with: the bucket's stream discards new messages per subject.
const RECORDED: &str = "maximum messages per subject";

/// The owners recorded so far, read from [`OWNERS_BUCKET`]. A record never changes, so those found are kept, by key.
pub struct Owners {
    js: jetstream::Context,
    found: Mutex<HashMap<String, UserId>>,
    /// Whether the missing bucket was reported.
    missing: AtomicBool,
}

/// What a read of the record of a thread to an ant found.
enum Read {
    Owner(UserId),
    NoOwner,
    NoBucket,
}

impl Owners {
    pub fn new(js: jetstream::Context) -> Self {
        Self { js, found: Mutex::default(), missing: AtomicBool::new(false) }
    }

    /// The owner a human's or webhook's message to `to` on `thread` named; None when no record of it is there, or the
    /// bucket does not exist.
    pub async fn recorded(&self, thread: &ThreadId, to: &UserId) -> Result<Option<UserId>> {
        Ok(match self.read(thread, to).await? {
            Read::Owner(owner) => Some(owner),
            Read::NoOwner | Read::NoBucket => None,
        })
    }

    /// [`Owners::recorded`], read again until a record of `thread` to `to` shows up or `within` runs out; None at
    /// once when the bucket does not exist.
    pub async fn wait_for(&self, thread: &ThreadId, to: &UserId, within: Duration) -> Result<Option<UserId>> {
        let until = tokio::time::Instant::now() + within;
        loop {
            match self.read(thread, to).await? {
                Read::Owner(owner) => return Ok(Some(owner)),
                Read::NoBucket => return Ok(None),
                Read::NoOwner if tokio::time::Instant::now() >= until => return Ok(None),
                Read::NoOwner => tokio::time::sleep(RECHECK.min(until - tokio::time::Instant::now())).await,
            }
        }
    }

    /// `human`, when `assistant`, its assistant, recorded it as the owner its message for it to `to` on `thread` names
    /// ([`record_for_human`]). The caller checks that the human's grant names `assistant`.
    pub async fn assisted(
        &self,
        thread: &ThreadId,
        to: &UserId,
        human: &UserId,
        assistant: &UserId,
    ) -> Result<Option<UserId>> {
        Ok(match self.read_key(thread, assisted_owner_key(thread, to, human, assistant)).await? {
            Read::Owner(owner) => Some(owner).filter(|owner| owner == human),
            Read::NoOwner | Read::NoBucket => None,
        })
    }

    async fn read(&self, thread: &ThreadId, to: &UserId) -> Result<Read> {
        self.read_key(thread, owner_key(thread, to)).await
    }

    async fn read_key(&self, thread: &ThreadId, key: String) -> Result<Read> {
        if let Some(owner) = self.found.locked().get(&key) {
            return Ok(Read::Owner(owner.clone()));
        }
        // A direct get alone, so no stream info lists the threads of every owner.
        let kv = DirectKv::new(&self.js, None, OWNERS_BUCKET).await?;
        let value = match kv.lookup(key.clone()).await {
            Ok(Lookup::Value(value)) => value,
            Ok(Lookup::NoValue) => return Ok(Read::NoOwner),
            Err(e) if crate::bus::hub_out_of_reach(&e) => return Err(e),
            // A missing bucket, or credentials not reading it, leave a direct get through a leaf node unanswered.
            missing @ (Ok(Lookup::NoBucket) | Err(_)) => {
                if !self.missing.swap(true, Ordering::Relaxed) {
                    let error = missing.err().map(|e| format!("{e:#}"));
                    tracing::warn!(
                        %thread,
                        ?error,
                        "reading the owners bucket; until `ants init` creates it and `ants auth user` issues \
                         credentials reading it, ants' messages on threads no channel post started have no owner"
                    );
                }
                return Ok(Read::NoBucket);
            }
        };
        let Some(owner) = std::str::from_utf8(&value).ok().and_then(|v| v.parse::<UserId>().ok()) else {
            return Ok(Read::NoOwner);
        };
        let mut found = self.found.locked();
        if found.len() >= MAX_KEPT {
            found.clear();
        }
        found.insert(key, owner.clone());
        Ok(Read::Owner(owner))
    }
}

/// Record `owner` as the owner of `thread` that a message to `to` names, unless a record of it is there already: in
/// the owners bucket or, while the hub is out of reach, in the machine's outbox, which the bucket sources once the
/// uplink is back ([`owner_outbox`]). The first record of a thread to an ant stays: the bucket refuses another, or
/// drops it as a repeat of the first's message id ([`owner_record_id`]). Resolves once either holds it, so the message
/// sent after it never outruns it. Needs a human's credentials, or a webhook's with `to` among its destinations; only
/// a human's reach the outbox. `js_domain` is the domain `js` was made for.
pub async fn record(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    thread: &ThreadId,
    to: &UserId,
    owner: &UserId,
) -> Result<()> {
    let key = owner_key(thread, to);
    let (id, value) = (owner_record_id(&key), owner.to_string());
    let in_hub = async {
        let kv = DirectKv::new(js, js_domain, OWNERS_BUCKET).await?;
        match kv.put_as(&key, value.clone(), &id).await {
            Err(e) if bus::is_store_full(e.as_ref()) => {
                Err(bus::explain_full(e, &format!("$KV.{OWNERS_BUCKET}.{key}")))
            }
            Err(e) if !format!("{e:#}").contains(RECORDED) => {
                Err(e).context("recording it (with credentials, `ants init` as the human creates the bucket)")
            }
            _ => Ok(()),
        }
    };
    let unreachable = match in_hub.await {
        Ok(()) => return Ok(()),
        // The hub is in reach: the outbox would only pile up what its full store refuses.
        Err(e) if bus::is_store_full(e.as_ref()) => return Err(e),
        Err(e) => e,
    };
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", id.as_str());
    let kept = match js.publish_with_headers(owner_outbox(thread, to), headers, value.into()).await {
        Ok(ack) => ack.await,
        Err(e) => Err(e),
    };
    if let Err(e) = kept {
        return Err(anyhow::anyhow!(e)).with_context(|| {
            format!(
                "the hub did not record the owner of thread {thread} ({unreachable:#}), nor did this machine's outbox"
            )
        });
    }
    tracing::warn!(
        %thread,
        error = format!("{unreachable:#}"),
        "the hub is out of reach; the thread's owner is kept in this machine's outbox"
    );
    Ok(())
}

/// Record `human` as the owner of `thread` that a message of `assistant`, its assistant, for it to `to` names, in the
/// owners bucket under [`assisted_owner_key`], unless a record of it is there already: never in the machine's outbox,
/// which the bucket sources only a human's records from. Needs the credentials of `assistant` as the assistant of
/// `human`. `js_domain` is the domain `js` was made for.
pub async fn record_for_human(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    thread: &ThreadId,
    to: &UserId,
    human: &UserId,
    assistant: &UserId,
) -> Result<()> {
    let key = assisted_owner_key(thread, to, human, assistant);
    let kv = DirectKv::new(js, js_domain, OWNERS_BUCKET).await?;
    match kv.put_as(&key, human.to_string(), &owner_record_id(&key)).await {
        Err(e) if bus::is_store_full(e.as_ref()) => Err(bus::explain_full(e, &format!("$KV.{OWNERS_BUCKET}.{key}"))),
        Err(e) if !format!("{e:#}").contains(RECORDED) => Err(e).context("recording the thread's owner"),
        _ => Ok(()),
    }
}

/// [`record`], logging a failure: the message goes out anyway, and ants' messages on its thread have no owner.
pub async fn recording(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    thread: &ThreadId,
    to: &UserId,
    owner: &UserId,
) {
    if let Err(e) = record(js, js_domain, thread, to, owner).await {
        tracing::warn!(
            %thread,
            %to,
            error = format!("{e:#}"),
            "recording the thread's owner; ants' messages on the thread name no owner"
        );
    }
}

/// The stream of [`OWNERS_BUCKET`].
pub fn owners_stream() -> String {
    format!("KV_{OWNERS_BUCKET}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus;
    use ants_test_support::NatsServer;

    #[tokio::test]
    async fn the_first_record_of_a_thread_stays() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let owners = Owners::new(js.clone());
        let thread = ThreadId::generate();
        let [dev, other]: [UserId; 2] = ["dev".parse().unwrap(), "other".parse().unwrap()];
        assert_eq!(owners.recorded(&thread, &dev).await.unwrap(), None, "no bucket yet");
        let started = tokio::time::Instant::now();
        assert_eq!(owners.wait_for(&thread, &dev, Duration::from_secs(5)).await.unwrap(), None);
        assert!(started.elapsed() < Duration::from_secs(1), "no bucket to wait on");
        bus::ensure_owners(&js).await.unwrap();
        let [alice, bob]: [UserId; 2] = ["alice".parse().unwrap(), "bob".parse().unwrap()];
        record(&js, None, &thread, &dev, &alice).await.unwrap();
        record(&js, None, &thread, &dev, &bob).await.unwrap();
        let key = owner_key(&thread, &dev);
        let subject = format!("$KV.{OWNERS_BUCKET}.{key}");
        // As the bucket sources from a machine's outbox: no check first, nor a refusal.
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", owner_record_id(&key).as_str());
        let repeat = js.publish_with_headers(subject.clone(), headers, "bob".into()).await.unwrap().await.unwrap();
        assert!(repeat.duplicate, "dropped as a repeat");
        assert!(js.publish(subject, "bob".into()).await.unwrap().await.is_err(), "refused without the id");
        let mut stream = js.get_stream(owners_stream()).await.unwrap();
        assert_eq!(stream.info().await.unwrap().state.messages, 1);
        assert!(stream.delete_message(1).await.is_err(), "nor is it deleted");
        assert!(stream.purge().await.is_err(), "nor purged");
        assert_eq!(Owners::new(js.clone()).recorded(&thread, &dev).await.unwrap(), Some(alice.clone()));
        assert_eq!(owners.recorded(&thread, &other).await.unwrap(), None, "no message to it on the thread");

        // A record on its way.
        let later = ThreadId::generate();
        let waited = tokio::spawn({
            let (js, later) = (js.clone(), later.clone());
            let dev = dev.clone();
            async move { Owners::new(js).wait_for(&later, &dev, Duration::from_secs(5)).await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        record(&js, None, &later, &dev, &bob).await.unwrap();
        assert_eq!(waited.await.unwrap(), Some(bob));
        let none = ThreadId::generate();
        let started = tokio::time::Instant::now();
        assert_eq!(owners.wait_for(&none, &dev, Duration::from_millis(600)).await.unwrap(), None);
        assert!(started.elapsed() >= Duration::from_millis(600));
    }

    #[tokio::test]
    async fn the_bucket_drops_a_repeated_record() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        bus::ensure_owners(&js).await.unwrap();
        let config = js.get_stream(owners_stream()).await.unwrap().cached_info().config.clone();
        assert_eq!(config.duplicate_window, config.max_age, "as long as a record is kept");
        let thread = ThreadId::generate();
        let [dev, alice]: [UserId; 2] = ["dev".parse().unwrap(), "alice".parse().unwrap()];
        record(&js, None, &thread, &dev, &alice).await.unwrap();
        let key = owner_key(&thread, &dev);
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", owner_record_id(&key).as_str());
        let subject = format!("$KV.{OWNERS_BUCKET}.{key}");
        let repeat = js.publish_with_headers(subject, headers, "bob".into()).await.unwrap().await.unwrap();
        assert!(repeat.duplicate);
        assert_eq!(Owners::new(js).recorded(&thread, &dev).await.unwrap(), Some(alice));
    }
}
