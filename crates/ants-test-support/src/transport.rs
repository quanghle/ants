//! A client's [`Transport`] over async-nats ([`NatsTransport`]), so a test runs `ants_client::client::Client` against
//! a real nats-server, with the credentials of a device, and sees what the server refused
//! ([`NatsTransport::server_errors`]).

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ants_client::transport::{Connect, Message, Pending, Subscription, Transport, TransportError};
use async_nats::client::RequestErrorKind;
use async_nats::{Event, HeaderMap, Request, Subscriber};
use futures::StreamExt as _;

/// A connection to the nats-server at a URL, made by [`Transport::connect`] on a tokio runtime.
///
/// Two things differ from what the trait asks. A subscription resolves once written out, which the server may not
/// have read yet: a test that waits for another connection's publish makes a round trip first. And a message's
/// headers come in no order, as async-nats keeps them in a map.
pub struct NatsTransport {
    url: String,
    creds: Option<PathBuf>,
    client: RefCell<Option<async_nats::Client>>,
    errors: Arc<Mutex<Vec<String>>>,
}

impl NatsTransport {
    /// A transport to the server at `url`, authenticating with the credentials file `creds` if given.
    pub fn new(url: &str, creds: Option<&Path>) -> Self {
        Self {
            url: url.to_owned(),
            creds: creds.map(Path::to_owned),
            client: RefCell::default(),
            errors: Arc::default(),
        }
    }

    /// The errors the server sent the connection so far, permission violations among them, in the order they came.
    /// The server handles a connection's operations in order, so the refusal of a publish is here by the time the
    /// refusal of a later one is.
    pub fn server_errors(&self) -> Vec<String> {
        self.errors.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    fn client(&self) -> Result<async_nats::Client, TransportError> {
        self.client.borrow().clone().ok_or(TransportError::Closed)
    }

    async fn connected(&self, options: Connect) -> Result<async_nats::Client, TransportError> {
        let errors = self.errors.clone();
        let mut connect = async_nats::ConnectOptions::new()
            .name(options.name)
            .custom_inbox_prefix(options.inbox_prefix)
            .event_callback(move |event| {
                let errors = errors.clone();
                async move {
                    if let Event::ServerError(e) = event {
                        errors.lock().unwrap_or_else(PoisonError::into_inner).push(e.to_string());
                    }
                }
            });
        if let Some(creds) = &self.creds {
            connect = connect.credentials_file(creds).await.map_err(other)?;
        }
        connect.connect(&self.url).await.map_err(other)
    }
}

fn other(error: impl std::fmt::Display) -> TransportError {
    TransportError::Other(error.to_string())
}

fn headers_of(message: &Message) -> Option<HeaderMap> {
    if message.headers.is_empty() {
        return None;
    }
    let mut headers = HeaderMap::new();
    for (name, value) in &message.headers {
        headers.append(name.as_str(), value.as_str());
    }
    Some(headers)
}

fn received(message: async_nats::Message) -> Message {
    let headers = message.headers.iter().flat_map(HeaderMap::iter);
    Message {
        subject: message.subject.to_string(),
        reply: message.reply.map(|reply| reply.to_string()),
        headers: headers
            .flat_map(|(name, values)| values.iter().map(move |value| (name.to_string(), value.to_string())))
            .collect(),
        payload: message.payload.to_vec(),
        status: message.status.map(|status| status.as_u16()),
        description: message.description,
    }
}

impl Transport for NatsTransport {
    fn connect(&self, options: Connect) -> Pending<'_, Result<(), TransportError>> {
        Box::pin(async move {
            let client = self.connected(options).await?;
            *self.client.borrow_mut() = Some(client);
            Ok(())
        })
    }

    fn publish(&self, message: Message) -> Pending<'_, Result<(), TransportError>> {
        Box::pin(async move {
            let client = self.client()?;
            let headers = headers_of(&message).unwrap_or_default();
            let Message { subject, reply, payload, .. } = message;
            let published = match reply {
                Some(reply) => client.publish_with_reply_and_headers(subject, reply, headers, payload.into()).await,
                None => client.publish_with_headers(subject, headers, payload.into()).await,
            };
            published.map_err(other)
        })
    }

    fn subscribe(&self, subject: String) -> Pending<'_, Result<Box<dyn Subscription>, TransportError>> {
        Box::pin(async move {
            let client = self.client()?;
            #[expect(clippy::disallowed_methods, reason = "the client refuses what a stream relayed")]
            let subscriber = client.subscribe(subject).await.map_err(other)?;
            client.flush().await.map_err(other)?;
            Ok(Box::new(Subscribed(subscriber)) as Box<dyn Subscription>)
        })
    }

    fn request(&self, message: Message, wait: Duration) -> Pending<'_, Result<Message, TransportError>> {
        Box::pin(async move {
            let client = self.client()?;
            let mut request = Request::new().payload(message.payload.clone().into()).timeout(Some(wait));
            if let Some(headers) = headers_of(&message) {
                request = request.headers(headers);
            }
            match client.send_request(message.subject, request).await {
                Ok(answer) => Ok(received(answer)),
                Err(e) => Err(match e.kind() {
                    RequestErrorKind::NoResponders => TransportError::NoResponders,
                    RequestErrorKind::TimedOut => TransportError::TimedOut,
                    _ => other(e),
                }),
            }
        })
    }
}

struct Subscribed(Subscriber);

impl Subscription for Subscribed {
    fn next(&mut self, wait: Option<Duration>) -> Pending<'_, Result<Message, TransportError>> {
        Box::pin(async move {
            let next = match wait {
                Some(wait) => tokio::time::timeout(wait, self.0.next()).await.map_err(|_| TransportError::TimedOut)?,
                None => self.0.next().await,
            };
            next.map(received).ok_or(TransportError::Closed)
        })
    }
}
