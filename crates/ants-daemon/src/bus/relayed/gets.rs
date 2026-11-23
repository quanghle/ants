//! A stream read by batched direct gets (`$JS.API.DIRECT.GET.<stream>`), one request per batch, its answers on one
//! reply subject ([`Gets`]). The request and the reading of each answer are those of a human's client of any kind
//! ([`ants_client::requests`]); here they are sent and awaited over async-nats.

use std::time::Duration;

use ants_client::requests::{self, BatchGet, DIRECT_BATCH, Position, REQUEST_WAIT, Stored};
use async_nats::jetstream;
use async_nats::{StatusCode, Subscriber};
use futures::StreamExt as _;

const SUBJECT: &str = "Nats-Subject";

/// What a read by batched direct gets comes to next.
#[derive(Debug)]
pub enum Got {
    /// A message as its writer stored it.
    Stored(Stored),
    /// A message a stream relayed ([`super::stored_relayed`]), which a reader refuses, and the subject it is on.
    Relayed { seq: u64, subject: String },
    /// A batch ended with messages left after it: the next batch is asked for by reading on.
    More,
    /// Nothing more is stored on the filter.
    End,
}

/// Why a read by batched direct gets failed.
#[derive(Debug)]
pub enum GetsError {
    /// The request could not be sent.
    Send(async_nats::PublishError),
    /// No answer came in time.
    Unanswered,
    /// The server answered with a status in place of a message.
    Status { status: StatusCode, description: Option<String> },
    /// An answer that is no stored message.
    Malformed(String),
}

/// A read of the messages of a stream on a filter, oldest first, from a position on.
pub struct Gets {
    js: jetstream::Context,
    get: String,
    filter: String,
    at: Position,
    inbox: String,
    replies: Subscriber,
    wait: Duration,
    size: usize,
    /// The answers read of the batch asked for; none while no batch is.
    batch: Option<usize>,
}

impl Gets {
    /// A read of `stream` on `filter` from `at` on, over `js`, waiting for each answer as long as its client waits
    /// for one.
    pub async fn open(
        js: &jetstream::Context,
        stream: &str,
        filter: impl Into<String>,
        at: Position,
    ) -> Result<Self, async_nats::SubscribeError> {
        let client = js.client();
        let inbox = client.new_inbox();
        #[expect(clippy::disallowed_methods, reason = "direct gets' answers, which the stream sends")]
        let replies = client.subscribe(inbox.clone()).await?;
        Ok(Self {
            js: js.clone(),
            get: format!("DIRECT.GET.{stream}"),
            filter: filter.into(),
            at,
            inbox,
            replies,
            wait: client.timeout().unwrap_or(REQUEST_WAIT),
            size: DIRECT_BATCH,
            batch: None,
        })
    }

    /// The read waiting `wait` for each answer.
    #[must_use]
    pub fn waiting(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// The read asking for up to `size` messages at once, no more than a human's client does.
    #[must_use]
    pub fn in_batches_of(mut self, size: usize) -> Self {
        self.size = size.clamp(1, DIRECT_BATCH);
        self
    }

    /// Where the read goes on from: after the last message it came to.
    pub fn at(&self) -> &Position {
        &self.at
    }

    /// What the read comes to next, asking for a batch when none is open.
    pub async fn next(&mut self) -> Result<Got, GetsError> {
        if self.batch.is_none() {
            let request = BatchGet { batch: self.size, ..self.at.request(&self.filter) };
            let payload = serde_json::to_vec(&request).unwrap_or_default();
            let request = async_nats::Request::new().inbox(self.inbox.clone()).payload(payload.into());
            self.js.send_request(self.get.clone(), request).await.map_err(GetsError::Send)?;
        }
        let got = self.batch.get_or_insert(0);
        let Ok(Some(msg)) = tokio::time::timeout(self.wait, self.replies.next()).await else {
            return Err(GetsError::Unanswered);
        };
        let (status, description) = (msg.status, msg.description.clone());
        let answer = super::message(msg);
        let subject = answer.header_value(SUBJECT).unwrap_or_default().to_owned();
        match requests::batch_answer(answer) {
            Ok(requests::Got::Stored(stored)) => {
                *got += 1;
                self.at.after(stored.seq);
                Ok(Got::Stored(stored))
            }
            Ok(requests::Got::Relayed(seq)) => {
                *got += 1;
                self.at.after(seq);
                Ok(Got::Relayed { seq, subject })
            }
            Ok(requests::Got::End { pending, .. }) => {
                let more = *got > 0 && pending != Some(0);
                self.batch = None;
                Ok(if more { Got::More } else { Got::End })
            }
            Ok(requests::Got::Nothing) => {
                self.batch = None;
                Ok(Got::End)
            }
            Err(e) => Err(match status {
                Some(status) => GetsError::Status { status, description },
                None => GetsError::Malformed(e.to_string()),
            }),
        }
    }
}
