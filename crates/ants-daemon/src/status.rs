//! Live progress on `ants.status.<ant>.<thread>`: core NATS, best effort.
//!
//! Nothing here waits on the server. An update the client cannot take at once is dropped, so a slow or lost link
//! never holds up a prompt, an ack or the harness.
//!
//! What the harness streams is redacted ([`Redactor`]) before it goes out; text joined over a flush period goes out
//! only up to a point where no secret is cut in two ([`Redactor::settled`]), the rest waiting for more.

use std::time::Duration;

use ants_proto::{Envelope, State, StatusEvent, StatusUpdate, ThreadId, UserId};
use futures::FutureExt as _;

use crate::redact::Redactor;

/// How long streamed text is joined before it goes out.
pub const FLUSH_EVERY: Duration = Duration::from_millis(250);
/// Text joined past this many bytes goes out without waiting for the next flush.
const MAX_JOINED: usize = 4096;
/// Updates a prompt may have waiting between the harness and the publisher; more are dropped.
pub const BUFFER: usize = 256;

/// Publish `update` for `ant` on `thread`, or drop it.
pub fn publish(client: &async_nats::Client, ant: &UserId, thread: &ThreadId, update: StatusUpdate) {
    let event = StatusEvent::new(ant.clone(), thread.clone(), update);
    let payload = match serde_json::to_vec(&event) {
        Ok(payload) => payload,
        Err(e) => return tracing::debug!(error = %e, "encoding a status update"),
    };
    match client.publish(event.subject(), payload.into()).now_or_never() {
        Some(Ok(())) => {}
        Some(Err(e)) => tracing::debug!(error = %e, "dropping a status update"),
        None => tracing::debug!("dropping a status update: the NATS client is busy"),
    }
}

/// Publish the thread's state.
pub fn state(client: &async_nats::Client, ant: &UserId, thread: &ThreadId, state: State) {
    publish(client, ant, thread, StatusUpdate::State { state });
}

/// Tell that `envelope`, stored, escalates what its sender could not settle, if it carries an escalation
/// ([`StatusUpdate::Escalated`]), on the sender's status of the envelope's thread.
pub fn escalated(client: &async_nats::Client, envelope: &Envelope) {
    if let Some(escalation) = envelope.escalation.as_deref() {
        let update = StatusUpdate::Escalated { to: envelope.to.clone(), escalation: escalation.clone() };
        publish(client, &envelope.from, &envelope.thread_id, update);
    }
}

/// The status of one prompt, joining streamed text so a reply goes out in a few updates rather than one per token,
/// redacted.
pub struct Status<'a> {
    client: &'a async_nats::Client,
    ant: &'a UserId,
    thread: &'a ThreadId,
    redactor: &'a Redactor,
    /// Text not sent yet: message or thought.
    joined: Option<StatusUpdate>,
}

impl<'a> Status<'a> {
    pub fn new(client: &'a async_nats::Client, ant: &'a UserId, thread: &'a ThreadId, redactor: &'a Redactor) -> Self {
        Self { client, ant, thread, redactor, joined: None }
    }

    fn send(&self, mut update: StatusUpdate) {
        self.redactor.redact_status(&mut update);
        publish(self.client, self.ant, self.thread, update);
    }

    /// Add an update. Text joins the text before it of the same type; anything else sends that text first, so the
    /// order holds.
    pub fn push(&mut self, update: StatusUpdate) {
        match (&mut self.joined, update) {
            (Some(StatusUpdate::Message { text: joined }), StatusUpdate::Message { text }) => joined.push_str(&text),
            (Some(StatusUpdate::Thought { text: joined }), StatusUpdate::Thought { text }) => joined.push_str(&text),
            (_, update @ (StatusUpdate::Message { .. } | StatusUpdate::Thought { .. })) => {
                self.flush();
                self.joined = Some(update);
            }
            (_, update) => {
                self.flush();
                self.send(update);
            }
        }
        if let Some(StatusUpdate::Message { text } | StatusUpdate::Thought { text }) = &self.joined
            && text.len() >= MAX_JOINED
        {
            self.flush_settled();
        }
    }

    /// Send the text joined so far.
    pub fn flush(&mut self) {
        if let Some(update) = self.joined.take() {
            self.send(update);
        }
    }

    /// Send the text joined so far up to where no secret is cut in two, keeping the rest until more comes or
    /// [`MAX_JOINED`] bytes of it wait.
    pub fn flush_settled(&mut self) {
        let Some(StatusUpdate::Message { text } | StatusUpdate::Thought { text }) = &mut self.joined else { return };
        let settled = self.redactor.settled(text);
        if text.len() - settled >= MAX_JOINED {
            return self.flush();
        }
        if settled == 0 {
            return;
        }
        let rest = text.split_off(settled);
        let update = match &self.joined {
            Some(StatusUpdate::Thought { .. }) => StatusUpdate::Thought { text: rest },
            _ => StatusUpdate::Message { text: rest },
        };
        let sent = self.joined.replace(update).expect("joined text");
        self.send(sent);
    }
}

#[cfg(test)]
mod tests {
    use ants_test_support::NatsServer as Server;
    use futures::StreamExt as _;

    use super::*;

    #[tokio::test]
    async fn text_is_joined_until_something_else_comes() {
        let Some(server) = Server::start() else { return };
        let client = async_nats::connect(&server.url).await.unwrap();
        let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let mut sub = client.subscribe(ants_proto::subject::status(&ant, &thread)).await.unwrap();
        client.flush().await.unwrap();

        let says = |text: &str| StatusUpdate::Message { text: text.to_owned() };
        let thinks = StatusUpdate::Thought { text: "hmm".to_owned() };
        let tool = StatusUpdate::Tool { id: "c1".to_owned(), title: None, tool_kind: None, status: None, paths: None };
        let redactor = Redactor::default();
        let mut status = Status::new(&client, &ant, &thread, &redactor);
        for update in [says("a"), says("b"), thinks.clone(), tool.clone(), says("c")] {
            status.push(update);
        }
        status.flush();
        status.push(says(&"x".repeat(MAX_JOINED)));
        client.flush().await.unwrap();

        let mut got = Vec::new();
        for _ in 0..5 {
            let msg = tokio::time::timeout(Duration::from_secs(5), sub.next()).await.unwrap().unwrap();
            got.push(serde_json::from_slice::<StatusEvent>(&msg.payload).unwrap().update);
        }
        assert_eq!(got, [says("ab"), thinks, tool, says("c"), says(&"x".repeat(MAX_JOINED))]);
    }

    #[tokio::test]
    async fn secrets_are_redacted_even_when_streamed_across_flushes() {
        let Some(server) = Server::start() else { return };
        let client = async_nats::connect(&server.url).await.unwrap();
        let (ant, thread): (UserId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let mut sub = client.subscribe(ants_proto::subject::status(&ant, &thread)).await.unwrap();
        client.flush().await.unwrap();

        let says = |text: &str| StatusUpdate::Message { text: text.to_owned() };
        let tool = |title: &str| StatusUpdate::Tool {
            id: "c1".to_owned(),
            title: Some(title.to_owned()),
            tool_kind: None,
            status: None,
            paths: Some(vec![format!("/work/{title}")]),
        };
        let redactor = Redactor::default().with_env([("API_TOKEN", "tok-12345678")]);
        let mut status = Status::new(&client, &ant, &thread, &redactor);
        status.push(says("pushing with ghp_0123"));
        status.flush_settled();
        status.push(says("456789abcdefghijABCDEFGHIJ now"));
        status.flush_settled();
        status.push(tool("curl -H 'X-Token: tok-12345678'"));
        status.flush();
        client.flush().await.unwrap();

        let mut got = Vec::new();
        for _ in 0..4 {
            let msg = tokio::time::timeout(Duration::from_secs(5), sub.next()).await.unwrap().unwrap();
            got.push(serde_json::from_slice::<StatusEvent>(&msg.payload).unwrap().update);
        }
        assert_eq!(
            got,
            [says("pushing with "), says("[redacted] "), says("now"), tool("curl -H 'X-Token: [redacted]'")]
        );
    }
}
