//! Core NATS subscriptions and requests that hear only what clients published: a message a stream sent onto the
//! subject ([`heard_relayed`]) is dropped, with a warning naming the subject.

use std::pin::Pin;
use std::task::{Context, Poll};

use async_nats::subject::ToSubject;
use async_nats::{
    Client, Message, Request, RequestError, RequestErrorKind, SubscribeError, Subscriber, UnsubscribeError,
};
use futures::{Stream, StreamExt as _};
use hyper::body::Bytes;

use super::heard_relayed;

/// A subscription that yields only the messages clients published on its subject.
pub struct Heard(Subscriber);

impl Heard {
    pub async fn unsubscribe(&mut self) -> Result<(), UnsubscribeError> {
        self.0.unsubscribe().await
    }
}

/// For a subscription made otherwise, a test's say.
impl From<Subscriber> for Heard {
    fn from(subscriber: Subscriber) -> Self {
        Self(subscriber)
    }
}

impl Stream for Heard {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        loop {
            match self.0.poll_next_unpin(cx) {
                Poll::Ready(Some(msg)) if heard_relayed(&msg) => warn(&msg),
                polled => return polled,
            }
        }
    }
}

fn warn(msg: &Message) {
    tracing::warn!(subject = %msg.subject, "dropping a message a stream relayed onto the subject");
}

/// [`Client::subscribe`], hearing only what clients published.
#[expect(clippy::disallowed_methods, reason = "the guarded subscription itself")]
pub async fn subscribe(client: &Client, subject: impl ToSubject) -> Result<Heard, SubscribeError> {
    client.subscribe(subject).await.map(Heard)
}

/// [`Client::queue_subscribe`], hearing only what clients published.
#[expect(clippy::disallowed_methods, reason = "the guarded subscription itself")]
pub async fn queue_subscribe(client: &Client, subject: impl ToSubject, queue: String) -> Result<Heard, SubscribeError> {
    client.queue_subscribe(subject, queue).await.map(Heard)
}

/// [`Client::request`], failing on an answer a stream relayed onto the request's inbox.
pub async fn request(client: &Client, subject: impl ToSubject, payload: Bytes) -> Result<Message, RequestError> {
    let answer = client.send_request(subject, Request::new().payload(payload)).await?;
    if heard_relayed(&answer) {
        warn(&answer);
        return Err(RequestError::with_source(RequestErrorKind::Other, "the answer is a message a stream relayed"));
    }
    Ok(answer)
}
