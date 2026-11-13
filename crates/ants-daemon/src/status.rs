//! Live progress on `ants.status.<persona>.<thread>`: core NATS, best effort.
//!
//! Nothing here waits on the server. An update the client cannot take at once is dropped, so a slow or lost link
//! never holds up a prompt, an ack or the harness.

use std::time::Duration;

use ants_proto::{PersonaId, State, StatusEvent, StatusUpdate, ThreadId};
use futures::FutureExt as _;

/// How long streamed text is joined before it goes out.
pub const FLUSH_EVERY: Duration = Duration::from_millis(250);
/// Text joined past this many bytes goes out without waiting for the next flush.
const MAX_JOINED: usize = 4096;
/// Updates a prompt may have waiting between the harness and the publisher; more are dropped.
pub const BUFFER: usize = 256;

/// Publish `update` for `persona` on `thread`, or drop it.
pub fn publish(client: &async_nats::Client, persona: &PersonaId, thread: &ThreadId, update: StatusUpdate) {
    let event = StatusEvent::new(persona.clone(), thread.clone(), update);
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
pub fn state(client: &async_nats::Client, persona: &PersonaId, thread: &ThreadId, state: State) {
    publish(client, persona, thread, StatusUpdate::State { state });
}

/// The status of one prompt, joining streamed text so a reply goes out in a few updates rather than one per token.
pub struct Status<'a> {
    client: &'a async_nats::Client,
    persona: &'a PersonaId,
    thread: &'a ThreadId,
    /// Text not sent yet: message or thought.
    joined: Option<StatusUpdate>,
}

impl<'a> Status<'a> {
    pub fn new(client: &'a async_nats::Client, persona: &'a PersonaId, thread: &'a ThreadId) -> Self {
        Self { client, persona, thread, joined: None }
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
                publish(self.client, self.persona, self.thread, update);
            }
        }
        if let Some(StatusUpdate::Message { text } | StatusUpdate::Thought { text }) = &self.joined
            && text.len() >= MAX_JOINED
        {
            self.flush();
        }
    }

    /// Send the text joined so far.
    pub fn flush(&mut self) {
        if let Some(update) = self.joined.take() {
            publish(self.client, self.persona, self.thread, update);
        }
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
        let (persona, thread): (PersonaId, ThreadId) = ("dev-a".parse().unwrap(), "t1".parse().unwrap());
        let mut sub = client.subscribe(ants_proto::subject::status(&persona, &thread)).await.unwrap();
        client.flush().await.unwrap();

        let says = |text: &str| StatusUpdate::Message { text: text.to_owned() };
        let thinks = StatusUpdate::Thought { text: "hmm".to_owned() };
        let tool = StatusUpdate::Tool { id: "c1".to_owned(), title: None, tool_kind: None, status: None };
        let mut status = Status::new(&client, &persona, &thread);
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
}
