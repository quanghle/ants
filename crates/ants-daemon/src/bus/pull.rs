//! The messages of a pull consumer, pulled anew once its request through a leaf node's uplink went silent.
//!
//! A leaf client's pull request to the hub's domain crosses the team account's service import on the hub, which maps
//! its reply subject (`_R_.…`) into the store account. A reconnect of the leaf node's uplink leaves the request waiting
//! and its mapping in place, but what the stream sends through it no longer reaches the client: the request goes
//! silent, and a message the stream delivers to it comes again only after its ack wait. A [`Pull`] makes a new request,
//! on a new reply subject, once two idle heartbeats in a row went missing, and at once when the daemon has its leaf
//! node reconnect the uplink ([`pull_anew`]). Dropping the old reply subject's subscription withdraws its interest: the
//! stream's next heartbeat to the old request then reaches nobody, which drops the mapping, and the stream drops the
//! request ([`settled`]).

use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker, ready};
use std::time::{Duration, Instant};

use async_nats::jetstream::consumer::StreamError;
use async_nats::jetstream::consumer::pull::{MessagesError, MessagesErrorKind, Stream};
use async_nats::jetstream::{self, consumer::PullConsumer};
use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt as _};

use crate::sync::MutexExt as _;

/// How often the stream tells an idle pull request it is still there: two missed in a row make a new request.
pub const IDLE_HEARTBEAT: Duration = Duration::from_secs(2);

/// Every [`Pull`] of the process, for [`pull_anew`].
static PULLS: Mutex<Vec<Weak<Mutex<Reading>>>> = Mutex::new(Vec::new());
/// When [`pull_anew`] last ran.
static ANEW: Mutex<Option<Instant>> = Mutex::new(None);

/// The messages of a pull consumer ([`pull`]), as [`Stream`] yields them, but for missed heartbeats, after which it
/// pulls anew.
pub struct Pull {
    consumer: PullConsumer,
    reading: Arc<Mutex<Reading>>,
    opening: Option<BoxFuture<'static, Result<Stream, StreamError>>>,
    ended: bool,
}

/// What [`pull_anew`] drops and wakes.
struct Reading {
    /// The current request's messages; none until the next one is made.
    messages: Option<Stream>,
    /// The waker of the last poll left pending.
    waker: Option<Waker>,
}

/// The messages of `consumer`, each pull request expiring after 30 seconds, an idle one heard from every
/// [`IDLE_HEARTBEAT`].
pub async fn pull(consumer: PullConsumer) -> Result<Pull, StreamError> {
    let messages = open(consumer.clone()).await?;
    let reading = Arc::new(Mutex::new(Reading { messages: Some(messages), waker: None }));
    let mut pulls = PULLS.locked();
    pulls.retain(|pull| pull.strong_count() > 0);
    pulls.push(Arc::downgrade(&reading));
    drop(pulls);
    Ok(Pull { consumer, reading, opening: None, ended: false })
}

/// Have every [`Pull`] of the process drop its request and make a new one, as after the daemon had its leaf node
/// reconnect the uplink: a reader busy with a message makes it once it asks for the next.
pub fn pull_anew() {
    *ANEW.locked() = Some(Instant::now());
    let pulls: Vec<_> = PULLS.locked().iter().filter_map(Weak::upgrade).collect();
    for pull in pulls {
        let mut reading = pull.locked();
        let dropped = reading.messages.take();
        let waker = reading.waker.take();
        drop(reading);
        drop(dropped);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Wait until the requests [`pull_anew`] dropped last are gone from their streams, two heartbeats after: a message
/// stored meanwhile may still be delivered to one, and come again only after its ack wait. A reader holding back the
/// next message until it acks one, as the admin consumer does, waits for this before acking.
pub async fn settled() {
    let Some(anew) = *ANEW.locked() else { return };
    tokio::time::sleep_until((anew + 2 * IDLE_HEARTBEAT).into()).await;
}

async fn open(consumer: PullConsumer) -> Result<Stream, StreamError> {
    consumer.stream().heartbeat(IDLE_HEARTBEAT).messages().await
}

impl futures::Stream for Pull {
    type Item = Result<jetstream::Message, MessagesError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if this.ended {
                return Poll::Ready(None);
            }
            if let Some(opening) = &mut this.opening {
                let opened = ready!(opening.poll_unpin(cx));
                this.opening = None;
                match opened {
                    Ok(messages) => this.reading.locked().messages = Some(messages),
                    // Only a closed connection fails a subscription: nothing more comes.
                    Err(e) => {
                        this.ended = true;
                        return Poll::Ready(Some(Err(MessagesError::with_source(MessagesErrorKind::Pull, e))));
                    }
                }
            }
            let mut reading = this.reading.locked();
            let Some(messages) = &mut reading.messages else {
                drop(reading);
                this.opening = Some(open(this.consumer.clone()).boxed());
                continue;
            };
            match messages.poll_next_unpin(cx) {
                Poll::Pending => {
                    reading.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(Some(Err(e))) if e.kind() == MessagesErrorKind::MissingHeartbeat => {
                    let silent = reading.messages.take();
                    drop(reading);
                    drop(silent);
                    tracing::debug!(consumer = this.consumer.cached_info().name, "missed heartbeats; pulling anew");
                }
                Poll::Ready(item) => return Poll::Ready(item),
            }
        }
    }
}
