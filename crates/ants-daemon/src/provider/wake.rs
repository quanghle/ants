//! What ants serves its providers: wake requests ([`ants_proto::provider::method::WAKE`]), each a message to the ant
//! from its provider. The ant's daemon sends it as the ant, to the ant itself, marked with the provider's name
//! ([`Envelope::provider`]): on the thread the request names, which must be one the ant has a session for, else on a
//! new one. A request's id is remembered for [`WAKE_DEDUP`] in [`ants_proto::ATTEMPTS_BUCKET`], so a repeat sends
//! nothing and names the same thread, across restarts and the machines serving the ant. The message carries a fresh
//! turn budget, so it spends none of its thread's turns; it renews no spending allowance, and an ant is woken at most
//! [`WAKES_PER_HOUR`] times an hour. A request replacing an earlier one withdraws that one's message first, and a
//! watcher's wake names the message only when the withdrawal won ([`ants_proto::WatchWake::replaces`]).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use ants_proto::provider::{BoxFuture, Host, MAX_WAKE_TEXT, RpcError, WAKE_DEDUP, WakeRequest, Woken};
use ants_proto::{Envelope, MAX_WATCH_OUTPUT, MessageMark, ThreadId, UserId, WatchWake};
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

/// The ants a process serves, each with what wakes it ([`serve`]); clones share them.
#[derive(Clone, Default)]
pub struct Wakers(Arc<Mutex<BTreeMap<UserId, Arc<Waker>>>>);

/// What ants serves a provider: wake requests for the ants of the process that have it as their provider.
pub struct Ants {
    /// The provider's name, once connected.
    name: OnceLock<String>,
    served: Wakers,
}

impl Ants {
    /// Wake requests for the ants `served` lists.
    pub fn new(served: Wakers) -> Self {
        Self { name: OnceLock::new(), served }
    }

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
            let waker = self.served.0.locked().get(&request.ant).cloned();
            let Some(waker) = waker else {
                return Err(unavailable(&format!("ant {} is not served here", request.ant)));
            };
            waker.wake(provider, request).await
        })
    }
}

/// While it lives, the ant's daemon takes wake requests from the ant's provider ([`serve`]).
pub struct Serving {
    served: Wakers,
    waker: Arc<Waker>,
}

impl Drop for Serving {
    fn drop(&mut self) {
        let mut served = self.served.0.locked();
        if served.get(&self.waker.ant).is_some_and(|w| Arc::ptr_eq(w, &self.waker)) {
            served.remove(&self.waker.ant);
        }
    }
}

/// Take the wake requests of `ant`'s provider among `providers`, if it has one, with what its daemon serves it with: the
/// outbox it sends as the ant through, its sessions, and `marks`, its keys of [`ants_proto::ATTEMPTS_BUCKET`].
pub async fn serve(
    providers: &super::Providers,
    ant: &UserId,
    js: &jetstream::Context,
    outbox: Outbox,
    sessions: SessionStore,
    marks: DirectKv,
) -> Option<Serving> {
    let provider = providers.of(ant).await?;
    let waker = Waker::new(ant.clone(), provider.name(), outbox, sessions, marks, Owners::new(js.clone()));
    Some(register(providers.wakers(), waker))
}

fn register(served: &Wakers, waker: Waker) -> Serving {
    let waker = Arc::new(waker);
    served.0.locked().insert(waker.ant.clone(), waker.clone());
    Serving { served: served.clone(), waker }
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
    /// What withdrawing the message of the request it replaces did, once tried, so a repeat sends the same message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replaced: Option<Replaced>,
}

/// What became of the message of the request a wake request replaces ([`WakeRequest::replaces`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Replaced {
    /// This daemon withdrew it, message `message`, from the ant's queue before the ant took it up.
    Withdrawn { message: Uuid },
    /// It goes on: taken up already, withdrawn by someone else, or no message of the provider's known.
    Kept,
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
        if let Some(watcher) = &request.watcher {
            check_watcher(watcher)?;
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
            let mark = Mark { thread, message: Uuid::now_v7(), sent: false, replaced: None };
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
            Ok(None) => Err(RpcError::new(
                RpcError::UNKNOWN_THREAD,
                format!("thread {thread} is no thread of ant {}", self.ant),
            )),
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

    /// Send the message of `mark`, the request's under `key`, unless it was sent, and record it sent: after
    /// withdrawing the message of the request it replaces, unless a try before did, the outcome recorded with it.
    async fn send(&self, key: &str, mut mark: Mark, request: &WakeRequest) -> Result<Woken, RpcError> {
        let thread = mark.thread.clone();
        if mark.sent {
            return Ok(Woken { thread });
        }
        let mut replaced = mark.replaced;
        if let (None, Some(earlier)) = (replaced, &request.replaces) {
            replaced = self.withdraw(earlier).await;
            if replaced.is_some() {
                mark.replaced = replaced;
                if let Err(e) = self.put_mark(key, &mark).await {
                    tracing::warn!(%thread, error = e.message, "recording the replaced wake");
                }
            }
        }
        let watcher = request.watcher.clone().map(|watcher| WatchWake {
            replaces: match replaced {
                Some(Replaced::Withdrawn { message }) => Some(message),
                Some(Replaced::Kept) | None => None,
            },
            ..watcher
        });
        let owner = match &request.thread {
            Some(thread) => self.owners.recorded(thread, &self.ant).await.unwrap_or_else(|e| {
                tracing::warn!(%thread, error = format!("{e:#}"), "reading the thread's owner for a wake");
                None
            }),
            None => None,
        };
        let envelope = Envelope::on_thread(thread.clone(), self.ant.clone(), self.ant.clone(), request.text.clone())
            .with_owner(owner)
            .woken_by(&self.provider)
            .watching(watcher);
        // Under the mark's id, the stream drops it as a duplicate if an earlier try stored it.
        let envelope = Envelope { id: mark.message, ..envelope };
        (self.outbox)(envelope).await.map_err(|e| unavailable(&format!("sending the wake: {e:#}")))?;
        tracing::info!(%thread, provider = self.provider, id = request.id, "woke the ant");
        crate::monitoring::metrics::ant(&self.ant).woken();
        if let Err(e) = self.put_mark(key, &Mark { sent: true, ..mark }).await {
            tracing::warn!(%thread, error = e.message, "recording the wake sent");
        }
        Ok(Woken { thread })
    }

    async fn put_mark(&self, key: &str, mark: &Mark) -> Result<(), RpcError> {
        let value = serde_json::to_vec(mark).map_err(|e| RpcError::internal(&e.to_string()))?;
        self.marks.put(key, value).await.map_err(storing)
    }
}

impl Waker {
    /// Withdraw the message of the provider's wake request `id` while it waits in the ant's queue, as a later wake
    /// replacing it is about to be sent: one taken up already goes on. Withdrawn only when this daemon's mark won
    /// (or, on a repeat, had won: the mark it put is found); none when it could not tell.
    async fn withdraw(&self, id: &str) -> Option<Replaced> {
        let key = mark_key(&self.ant, &self.provider, id);
        let earlier = match self.mark(&key).await {
            Ok((Some(mark), _)) if mark.sent => mark,
            Ok(_) => return Some(Replaced::Kept),
            Err(e) => {
                tracing::warn!(id, error = e.message, "reading the replaced wake");
                return None;
            }
        };
        let withdrawn = Replaced::Withdrawn { message: earlier.message };
        match crate::delivery::attempts::withdraw(&self.marks, &self.ant, &earlier.message, &self.ant, None).await {
            Ok(None) => {
                tracing::info!(thread = %earlier.thread, id, "withdrew the replaced wake");
                Some(withdrawn)
            }
            Ok(Some(MessageMark::Withdrawn { by, on_behalf_of: None, .. })) if by == self.ant => Some(withdrawn),
            Ok(Some(_)) => Some(Replaced::Kept),
            Err(e) => {
                tracing::warn!(id, error = format!("{e:#}"), "withdrawing the replaced wake");
                None
            }
        }
    }
}

/// Refuse a watcher's part of a wake request that is malformed or carries more output than a wake does.
fn check_watcher(watcher: &WatchWake) -> Result<(), RpcError> {
    if watcher.id.is_empty() || watcher.title.trim().is_empty() {
        return Err(invalid("a watcher's wake needs the watcher's id and title"));
    }
    if watcher.output.as_ref().is_some_and(|output| output.len() > MAX_WATCH_OUTPUT) {
        return Err(invalid(&format!("a watcher's output in a wake is limited to {MAX_WATCH_OUTPUT} bytes")));
    }
    if watcher.id.len() + watcher.title.len() > MAX_WAKE_TEXT {
        return Err(invalid(&format!("a watcher's id and title are limited to {MAX_WAKE_TEXT} bytes")));
    }
    Ok(())
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
