//! The connection a client's reads and writes run over ([`Transport`]): four calls of core NATS, connect, publish,
//! subscribe and request, which name no runtime. A browser's binding implements them over nats.js, a native client
//! could over async-nats; [`crate::client::Client`] is all that calls them.
//!
//! The futures are not `Send`: a browser's connection is not, and a native client awaits them on the thread that
//! holds its transport.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// What a call of a [`Transport`] returns: a future the caller's own runtime drives.
pub type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A core NATS message, as published or as received.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub subject: String,
    /// The subject answers go to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply: Option<String>,
    /// The headers, a name once per value, the values of one name in the order the message carries them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub payload: Vec<u8>,
    /// The status of a message the server sent in place of an answer (`404`, `408`, a batch's `204`), with its
    /// description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Message {
    /// A message of `payload` to `subject`.
    pub fn new(subject: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self { subject: subject.into(), payload: payload.into(), ..Self::default() }
    }

    /// The message with the header `name` added.
    #[must_use]
    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_owned(), value.into()));
        self
    }

    /// The last value of the header named exactly `name`. nats-server tells header names apart by case and adds its
    /// own headers after those a message was stored with, so this is the server's value of a header it adds, never
    /// one a writer stored under the name or under another case of it.
    pub fn header_value(&self, name: &str) -> Option<&str> {
        self.headers.iter().rfind(|(header, _)| header == name).map(|(_, value)| value.as_str())
    }

    /// The names of its headers, one per value.
    pub fn header_names(&self) -> impl Iterator<Item = &str> + Clone {
        self.headers.iter().map(|(name, _)| name.as_str())
    }
}

/// How a client asks its transport to connect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Connect {
    /// The name the connection tells the server.
    pub name: String,
    /// The prefix of the connection's reply subjects ([`ants_proto::inbox_prefix`]): a user's rights let it hear
    /// answers under its own only, so a request under any other is refused.
    pub inbox_prefix: String,
}

/// Why a call of a [`Transport`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportError {
    /// Nothing listens on the subject of a request: no stream captures it, or the server has no JetStream.
    #[error("nobody answers")]
    NoResponders,
    #[error("no answer in time")]
    TimedOut,
    /// The connection, or the subscription, is closed.
    #[error("the connection is closed")]
    Closed,
    /// The server refused the call for want of a right.
    #[error("not permitted: {0}")]
    NotPermitted(String),
    #[error("{0}")]
    Other(String),
}

/// A connection speaking core NATS. Where the server is and what authenticates the connection, the transport knows
/// from whoever made it.
pub trait Transport {
    /// Connect as `options` says, resolving once connected; a transport that reconnects on its own keeps to them.
    fn connect(&self, options: Connect) -> Pending<'_, Result<(), TransportError>>;

    /// Publish `message`, with its headers and its reply subject if it names one.
    fn publish(&self, message: Message) -> Pending<'_, Result<(), TransportError>>;

    /// Subscribe to `subject`, resolving once the server has the subscription. Dropping the subscription ends it.
    fn subscribe(&self, subject: String) -> Pending<'_, Result<Box<dyn Subscription>, TransportError>>;

    /// Publish `message` under a reply subject of the connection's own, and resolve with the first answer, a status
    /// in place of one included, or fail after `wait`.
    fn request(&self, message: Message, wait: Duration) -> Pending<'_, Result<Message, TransportError>>;
}

/// What [`Transport::subscribe`] gives: the messages on a subject, in the order they came.
pub trait Subscription {
    /// The next message, waited for no longer than `wait` when given ([`TransportError::TimedOut`]);
    /// [`TransportError::Closed`] once the subscription has ended.
    fn next(&mut self, wait: Option<Duration>) -> Pending<'_, Result<Message, TransportError>>;
}
