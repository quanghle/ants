//! What ants serves its providers: wake requests ([`ants_proto::provider::method::WAKE`]), each a message to the ant
//! from its provider. The ant's daemon sends it as the ant, to the ant itself, marked with the provider's name
//! ([`Envelope::provider`]): on the thread the request names, which must be one the ant has a session for, else on a
//! new one. A request's id is remembered for [`WAKE_DEDUP`] in [`ants_proto::ATTEMPTS_BUCKET`], so a repeat sends
//! nothing and names the same thread, across restarts and the machines serving the ant. The message carries a fresh
//! turn budget, so it spends none of its thread's turns; it renews no spending allowance, and an ant is woken at most
//! [`WAKES_PER_HOUR`] times an hour.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use ants_proto::provider::{BoxFuture, Host, MAX_WAKE_TEXT, RpcError, WAKE_DEDUP, WakeRequest, Woken};
use ants_proto::{Envelope, ThreadId, UserId};
use async_nats::jetstream;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::mcp::Outbox;
use crate::pool::SessionStore;
use crate::records::direct_kv::{DirectKv, Lookup};
use crate::records::owners::Owners;
use crate::sync::MutexExt as _;

/// The most wake requests that wake an ant within an hour; repeats of one already sent do not count.
pub const WAKES_PER_HOUR: usize = 60;

const HOUR: Duration = Duration::from_hours(1);

/// The ants this process serves, each with what wakes it.
static SERVED: Mutex<BTreeMap<UserId, Arc<Waker>>> = Mutex::new(BTreeMap::new());

/// What ants serves a provider: wake requests for the ants of this process that have it as their provider.
#[derive(Default)]
pub struct Ants {
    /// The provider's name, once connected.
    name: OnceLock<String>,
}

impl Ants {
    /// Take the provider, connected, as named `name`.
    pub fn named(&self, name: &str) {
        let _ = self.name.set(name.to_owned());
    }
}

impl Host for Ants {
    fn wake(&self, request: WakeRequest) -> BoxFuture<'_, Result<Woken, RpcError>> {
        Box::pin(async move {
            let Some(provider) = self.name.get() else {
                return Err(unavailable("the provider is not connected yet"));
            };
            let waker = SERVED.locked().get(&request.ant).cloned();
            let Some(waker) = waker else {
                return Err(unavailable(&format!("ant {} is not served here", request.ant)));
            };
            waker.wake(provider, request).await
        })
    }
}

/// While it lives, the ant's daemon takes wake requests from the ant's provider ([`serve`]).
pub struct Serving {
    waker: Arc<Waker>,
}

impl Drop for Serving {
    fn drop(&mut self) {
        let mut served = SERVED.locked();
        if served.get(&self.waker.ant).is_some_and(|w| Arc::ptr_eq(w, &self.waker)) {
            served.remove(&self.waker.ant);
        }
    }
}

/// Take the wake requests of `ant`'s provider, if it has one, with what its daemon serves it with: the outbox it sends
/// as the ant through, its sessions, and `marks`, its keys of [`ants_proto::ATTEMPTS_BUCKET`].
pub async fn serve(
    ant: &UserId,
    js: &jetstream::Context,
    outbox: Outbox,
    sessions: SessionStore,
    marks: DirectKv,
) -> Option<Serving> {
    let provider = super::of(ant).await?;
    let waker = Waker::new(ant.clone(), provider.name(), outbox, sessions, marks, Owners::new(js.clone()));
    Some(register(waker))
}

fn register(waker: Waker) -> Serving {
    let waker = Arc::new(waker);
    SERVED.locked().insert(waker.ant.clone(), waker.clone());
    Serving { waker }
}

/// What wakes one ant.
struct Waker {
    ant: UserId,
    /// The name of the ant's provider, the one provider that wakes it.
    provider: String,
    outbox: Outbox,
    sessions: SessionStore,
    marks: DirectKv,
    owners: Owners,
    /// When the ant was woken within the last hour.
    woken: Mutex<VecDeque<Instant>>,
}

/// A wake request's record in [`ants_proto::ATTEMPTS_BUCKET`], under [`mark_key`].
#[derive(Debug, Serialize, Deserialize)]
struct Mark {
    thread: ThreadId,
    /// The id of its message ([`Envelope::id`]).
    message: Uuid,
    /// Whether the message was stored: until it is, a repeat of the request sends it again, under the same id.
    #[serde(default)]
    sent: bool,
}

impl Waker {
    fn new(
        ant: UserId,
        provider: &str,
        outbox: Outbox,
        sessions: SessionStore,
        marks: DirectKv,
        owners: Owners,
    ) -> Self {
        Self { ant, provider: provider.to_owned(), outbox, sessions, marks, owners, woken: Mutex::default() }
    }

    /// Wake the ant as `provider` asks with `request`.
    async fn wake(&self, provider: &str, request: WakeRequest) -> Result<Woken, RpcError> {
        if provider != self.provider {
            return Err(invalid(&format!("ant {} has another provider than {provider}", self.ant)));
        }
        if request.id.is_empty() {
            return Err(invalid("a wake request needs an id"));
        }
        if request.text.trim().is_empty() {
            return Err(invalid("a wake request needs a text"));
        }
        if request.text.len() > MAX_WAKE_TEXT {
            return Err(invalid(&format!("the text of a wake request is limited to {MAX_WAKE_TEXT} bytes")));
        }
        let key = mark_key(&self.ant, provider, &request.id);
        // A second try finds the mark of a request with the same id that took the key meanwhile.
        for _ in 0..2 {
            let (mark, last) = self.mark(&key).await?;
            if let Some(mark) = mark {
                return self.send(&key, mark, &request).await;
            }
            if let Some(thread) = &request.thread {
                self.check_thread(thread).await?;
            }
            self.count()?;
            let thread = request.thread.clone().unwrap_or_else(ThreadId::generate);
            let mark = Mark { thread, message: Uuid::now_v7(), sent: false };
            let value = serde_json::to_vec(&mark).map_err(|e| RpcError::internal(&e.to_string()))?;
            if self.marks.update(&key, value, last).await.map_err(storing)? {
                return self.send(&key, mark, &request).await;
            }
        }
        Err(unavailable("the request's record kept changing"))
    }

    /// The mark under `key`, unless older than [`WAKE_DEDUP`], and the stream sequence of the key's last message.
    async fn mark(&self, key: &str) -> Result<(Option<Mark>, u64), RpcError> {
        let (found, last, stored) = self.marks.lookup_stored(key).await.map_err(storing)?;
        let value = match found {
            Lookup::Value(value) => value,
            Lookup::NoValue => return Ok((None, last)),
            Lookup::NoBucket => return Err(unavailable("the attempts bucket is missing")),
        };
        let age = stored.and_then(|at| SystemTime::now().duration_since(at).ok()).unwrap_or_default();
        Ok((serde_json::from_slice(&value).ok().filter(|_| age < WAKE_DEDUP), last))
    }

    /// Refuse `thread` unless the ant has a session for it.
    async fn check_thread(&self, thread: &ThreadId) -> Result<(), RpcError> {
        match self.sessions.get(thread).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(invalid(&format!("thread {thread} is no thread of ant {}", self.ant))),
            Err(e) => Err(storing(e)),
        }
    }

    /// Count a wake against [`WAKES_PER_HOUR`], refusing it past them.
    fn count(&self) -> Result<(), RpcError> {
        let now = Instant::now();
        let mut woken = self.woken.locked();
        while woken.front().is_some_and(|at| now.duration_since(*at) >= HOUR) {
            woken.pop_front();
        }
        if woken.len() >= WAKES_PER_HOUR {
            return Err(unavailable(&format!("ant {} was woken {WAKES_PER_HOUR} times within the hour", self.ant)));
        }
        woken.push_back(now);
        Ok(())
    }

    /// Send the message of `mark`, the request's under `key`, unless it was sent, and record it sent.
    async fn send(&self, key: &str, mark: Mark, request: &WakeRequest) -> Result<Woken, RpcError> {
        let thread = mark.thread.clone();
        if mark.sent {
            return Ok(Woken { thread });
        }
        let owner = match &request.thread {
            Some(thread) => self.owners.recorded(thread, &self.ant).await.unwrap_or_else(|e| {
                tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner for a wake");
                None
            }),
            None => None,
        };
        let envelope = Envelope::on_thread(thread.clone(), self.ant.clone(), self.ant.clone(), request.text.clone())
            .with_owner(owner)
            .woken_by(&self.provider);
        // Under the mark's id, the stream drops it as a duplicate if an earlier try stored it.
        let envelope = Envelope { id: mark.message, ..envelope };
        (self.outbox)(envelope).await.map_err(|e| unavailable(&format!("sending the wake: {e:#}")))?;
        tracing::info!(%thread, provider = self.provider, id = request.id, "woke the ant");
        let sent = serde_json::to_vec(&Mark { sent: true, ..mark }).map_err(|e| RpcError::internal(&e.to_string()))?;
        if let Err(e) = self.marks.put(key, sent).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "recording the wake sent");
        }
        Ok(Woken { thread })
    }
}

/// Key in [`ants_proto::ATTEMPTS_BUCKET`] of the mark of `provider`'s wake request `id` for `ant`: under the ant's own
/// keys, the provider and id hashed into one token.
fn mark_key(ant: &UserId, provider: &str, id: &str) -> String {
    let named = [provider.as_bytes(), b"\0", id.as_bytes()].concat();
    let hash = ring::digest::digest(&ring::digest::SHA256, &named);
    format!("{ant}.wake.{}", data_encoding::HEXLOWER.encode(hash.as_ref()))
}

fn invalid(detail: &str) -> RpcError {
    RpcError::invalid_params(detail)
}

fn unavailable(why: &str) -> RpcError {
    RpcError::new(RpcError::UNAVAILABLE, format!("unavailable: {why}"))
}

fn storing(e: anyhow::Error) -> RpcError {
    unavailable(&format!("{e:#}"))
}

#[cfg(test)]
mod tests;
