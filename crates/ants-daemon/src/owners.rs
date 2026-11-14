//! The owners of the threads humans and webhooks message ants on. Before its message starts or joins a thread, a human
//! (`ants ask`) or a webhook (`ants webhook serve`) records the thread's owner in [`OWNERS_BUCKET`] ([`record`]), which
//! no ant's credentials write and whose first record of a thread stays. A daemon takes the owner of an ant's message on
//! a thread no channel post started only from there ([`Owners::owner_of`]), so no ant names the owner of such a thread.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use ants_proto::{JsDomain, OWNERS_BUCKET, ThreadId, UserId, owner_key};
use anyhow::{Context as _, Result};
use async_nats::jetstream;

use crate::direct_kv::{DirectKv, Lookup};

/// Most owners kept in memory; past it they are read again.
const MAX_KEPT: usize = 4096;

/// What a put of a second record of a thread fails with: the bucket's stream discards new messages per subject.
const RECORDED: &str = "maximum messages per subject";

/// The owners recorded so far, read from [`OWNERS_BUCKET`]. A record never changes, so those found are kept.
pub struct Owners {
    js: jetstream::Context,
    found: Mutex<HashMap<ThreadId, UserId>>,
    /// Whether the missing bucket was reported.
    missing: AtomicBool,
}

impl Owners {
    pub fn new(js: jetstream::Context) -> Self {
        Self { js, found: Mutex::default(), missing: AtomicBool::new(false) }
    }

    /// The owner recorded for `thread`; None when none is, or the bucket does not exist.
    pub async fn owner_of(&self, thread: &ThreadId) -> Result<Option<UserId>> {
        if let Some(owner) = self.found.lock().expect("owners").get(thread) {
            return Ok(Some(owner.clone()));
        }
        // A direct get alone, so no stream info lists the threads of every owner.
        let kv = DirectKv::new(&self.js, None, OWNERS_BUCKET).await?;
        let value = match kv.lookup(owner_key(thread)).await {
            Ok(Lookup::Value(value)) => value,
            Ok(Lookup::NoValue) => return Ok(None),
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
                return Ok(None);
            }
        };
        let Some(owner) = std::str::from_utf8(&value).ok().and_then(|v| v.parse::<UserId>().ok()) else {
            return Ok(None);
        };
        let mut found = self.found.lock().expect("owners");
        if found.len() >= MAX_KEPT {
            found.clear();
        }
        found.insert(thread.clone(), owner.clone());
        Ok(Some(owner))
    }
}

/// Record `owner` as the owner of `thread`, unless a record of the thread is there already: the first one stays.
/// Needs a human's or a webhook's credentials. `js_domain` is the domain `js` was made for.
pub async fn record(
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    thread: &ThreadId,
    owner: &UserId,
) -> Result<()> {
    let kv = DirectKv::new(js, js_domain, OWNERS_BUCKET).await?;
    match kv.put(owner_key(thread), owner.to_string()).await {
        Ok(()) => Ok(()),
        Err(e) if format!("{e:#}").contains(RECORDED) => Ok(()),
        Err(e) => Err(e).with_context(|| {
            format!(
                "recording the owner of thread {thread} (with credentials, `ants init` as the human creates the bucket)"
            )
        }),
    }
}

/// [`record`], logging a failure: the message goes out anyway, and ants' messages on its thread have no owner.
pub async fn recording(js: &jetstream::Context, js_domain: Option<&JsDomain>, thread: &ThreadId, owner: &UserId) {
    if let Err(e) = record(js, js_domain, thread, owner).await {
        tracing::warn!(
            %thread,
            error = format!("{e:#}"),
            "recording the thread's owner; ants' messages on the thread name no owner"
        );
    }
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
        assert_eq!(owners.owner_of(&thread).await.unwrap(), None, "no bucket yet");
        bus::ensure_owners(&js).await.unwrap();
        let [alice, bob]: [UserId; 2] = ["alice".parse().unwrap(), "bob".parse().unwrap()];
        record(&js, None, &thread, &alice).await.unwrap();
        record(&js, None, &thread, &bob).await.unwrap();
        let kv = js.get_key_value(OWNERS_BUCKET).await.unwrap();
        assert!(kv.delete(owner_key(&thread)).await.is_err(), "nor is it deleted");
        assert!(kv.purge(owner_key(&thread)).await.is_err(), "nor purged");
        assert_eq!(Owners::new(js).owner_of(&thread).await.unwrap(), Some(alice));
    }
}
