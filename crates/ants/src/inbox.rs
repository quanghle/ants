//! A human's inbox: replies kept in the human's durable consumer, `inbox-<name>`, until `ants inbox` shows them.
//!
//! A reply is read once its message is acked. Messages fetched but not wanted (peeked, on another thread, past the
//! limit) are held and handed back with a nak on [`Inbox::release`], so they stay unread; if the process dies first,
//! they come back after [`ACK_WAIT`].

use std::collections::BTreeMap;
use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{Envelope, ThreadId, UserId};
use anyhow::{Context as _, Result, anyhow};
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt as _;

/// How long a fetched message stays with this CLI before the server offers it again.
pub const ACK_WAIT: Duration = Duration::from_secs(60);
const BATCH: usize = 256;

/// Which unread messages to take, and whether taking them marks them read.
#[derive(Debug, Clone, Default)]
pub struct Select {
    pub thread: Option<ThreadId>,
    pub limit: Option<usize>,
    /// Show without marking read.
    pub peek: bool,
}

enum Fetched {
    /// A message seen for the first time, and its envelope unless malformed.
    New(u64, Option<Box<Envelope>>),
    /// A held message the server offered again.
    Again,
}

pub struct Inbox {
    client: async_nats::Client,
    consumer: PullConsumer,
    /// Messages fetched and left unread, by stream sequence.
    held: BTreeMap<u64, jetstream::Message>,
    taken: usize,
}

impl Inbox {
    /// The inbox of `human`.
    pub async fn open(js: &jetstream::Context, human: &UserId) -> Result<Self> {
        let stream = bus::ensure_stream(js).await?;
        let consumer = bus::human_inbox_consumer(&stream, human, ACK_WAIT).await?;
        Ok(Self { client: js.client(), consumer, held: BTreeMap::new(), taken: 0 })
    }

    fn left(&self, select: &Select) -> usize {
        select.limit.map_or(usize::MAX, |limit| limit.saturating_sub(self.taken))
    }

    /// The selected unread messages available now, oldest first.
    pub async fn unread(&mut self, select: &Select) -> Result<Vec<Envelope>> {
        self.unread_where(select, |_| true).await
    }

    async fn unread_where(&mut self, select: &Select, wanted: impl Fn(&Envelope) -> bool) -> Result<Vec<Envelope>> {
        let mut found = Vec::new();
        while self.left(select) > 0 {
            let mut batch = self.consumer.fetch().max_messages(BATCH).messages().await.context("fetching")?;
            let mut fresh = false;
            while let Some(msg) = batch.next().await {
                if let Fetched::New(sequence, envelope) = self.hold(msg.map_err(|e| anyhow!(e))?, select.peek).await? {
                    fresh = true;
                    found.extend(envelope.map(|e| (sequence, *e)));
                }
            }
            // Held messages come back only after ACK_WAIT, so a batch with nothing new means nothing is left.
            if !fresh {
                break;
            }
        }
        // Messages handed back come again before the rest, so restore the order they were sent in.
        found.sort_by_key(|(sequence, _)| *sequence);
        found.retain(|(_, e)| select.thread.as_ref().is_none_or(|t| *t == e.thread_id) && wanted(e));
        found.truncate(self.left(select));
        for (sequence, _) in &found {
            self.take(*sequence, select.peek).await?;
        }
        Ok(found.into_iter().map(|(_, e)| e).collect())
    }

    /// Wait for new selected messages and hand each to `show`, until `stop` resolves or the limit is reached.
    pub async fn follow(
        &mut self,
        select: &Select,
        mut show: impl FnMut(&Envelope) -> Result<()>,
        stop: impl Future<Output = ()>,
    ) -> Result<()> {
        tokio::pin!(stop);
        let mut messages = self.consumer.messages().await.context("following the inbox")?;
        while self.left(select) > 0 {
            let msg = tokio::select! {
                _ = &mut stop => break,
                msg = messages.next() => match msg {
                    None => break,
                    Some(msg) => msg.map_err(|e| anyhow!(e)).context("following the inbox")?,
                },
            };
            if let Fetched::New(sequence, Some(envelope)) = self.hold(msg, select.peek).await?
                && select.thread.as_ref().is_none_or(|t| *t == envelope.thread_id)
            {
                self.take(sequence, select.peek).await?;
                show(&envelope)?;
            }
        }
        Ok(())
    }

    /// Hold `msg` unread. A malformed one, or one whose sender is forged, is dropped instead, unless peeking; either
    /// way it is not shown.
    async fn hold(&mut self, msg: jetstream::Message, peek: bool) -> Result<Fetched> {
        let sequence = msg.info().map_err(|e| anyhow!(e))?.stream_sequence;
        if self.held.insert(sequence, msg).is_some() {
            return Ok(Fetched::Again);
        }
        let msg = &self.held[&sequence];
        let envelope = serde_json::from_slice::<Envelope>(&msg.payload)
            .map_err(|e| e.to_string())
            .and_then(|e| e.check_subject(&msg.subject).map(|()| e).map_err(|e| format!("forged sender: {e}")));
        match envelope {
            Ok(envelope) => Ok(Fetched::New(sequence, Some(Box::new(envelope)))),
            Err(e) => {
                if !peek {
                    tracing::warn!(error = %e, sequence, "dropping a malformed message");
                    let msg = self.held.remove(&sequence).expect("held");
                    msg.ack_with(AckKind::Term).await.map_err(|e| anyhow!(e))?;
                }
                Ok(Fetched::New(sequence, None))
            }
        }
    }

    /// Count a held message as shown, and mark it read unless peeking.
    async fn take(&mut self, sequence: u64, peek: bool) -> Result<()> {
        self.taken += 1;
        if !peek && let Some(msg) = self.held.remove(&sequence) {
            msg.double_ack().await.map_err(|e| anyhow!(e)).context("marking read")?;
        }
        Ok(())
    }

    /// Hand the held messages back, still unread, so the next `ants inbox` shows them at once.
    pub async fn release(self) -> Result<()> {
        for msg in self.held.into_values() {
            msg.ack_with(AckKind::Nak(None)).await.map_err(|e| anyhow!(e))?;
        }
        self.client.flush().await?;
        Ok(())
    }
}

/// Mark `reply`, already shown by `ants ask --wait`, read in the inbox of `human`. JetStream may store it a moment
/// after it was delivered, so a miss is retried briefly. Returns whether it was found unread.
pub async fn mark_read(js: &jetstream::Context, human: &UserId, reply: &Envelope) -> Result<bool> {
    let mut inbox = Inbox::open(js, human).await?;
    let select = Select { thread: Some(reply.thread_id.clone()), limit: Some(1), peek: false };
    let mut found = false;
    for _ in 0..5 {
        found = !inbox.unread_where(&select, |e| e.id == reply.id).await?.is_empty();
        if found {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    inbox.release().await?;
    Ok(found)
}

/// A message for the terminal: a header line, then the text indented.
pub fn render(e: &Envelope) -> String {
    let at = e.sent_at.with_timezone(&chrono::Local).format("%Y-%m-%d %H:%M");
    let text: Vec<String> = e.text.lines().map(|line| format!("  {line}")).collect();
    format!("{at}  {} on thread {}\n{}\n", e.from, e.thread_id, text.join("\n"))
}

#[cfg(test)]
mod tests {
    use ants_proto::Role;

    use super::*;
    use crate::auth::tests::Server;

    fn texts(envelopes: &[Envelope]) -> Vec<&str> {
        envelopes.iter().map(|e| e.text.as_str()).collect()
    }

    async fn read(js: &jetstream::Context, select: Select) -> Vec<Envelope> {
        let mut inbox = Inbox::open(js, &UserId::human()).await.unwrap();
        let unread = inbox.unread(&select).await.unwrap();
        inbox.release().await.unwrap();
        unread
    }

    /// Runs as the human and dev-a with their own credentials, so it also checks the human's permissions.
    #[tokio::test]
    async fn replies_stay_unread_until_shown() {
        let Some(server) = Server::start() else { return };
        let dev_a: UserId = "dev-a".parse().unwrap();
        let (_, human) = server.connect(&UserId::human()).await;
        bus::ensure_stream(&human).await.unwrap();
        let (_, agent) = server.connect(&dev_a).await;
        let asks =
            [Envelope::new(UserId::human(), dev_a.clone(), "a"), Envelope::new(UserId::human(), dev_a.clone(), "b")];
        for (ask, text) in [(&asks[0], "one"), (&asks[1], "two"), (&asks[0], "three")] {
            bus::publish(&agent, &ask.reply(dev_a.clone(), text, Role::Human).unwrap()).await.unwrap();
        }
        let forged = asks[0].reply(dev_a.clone(), "forged", Role::Human).unwrap();
        let subject = ants_proto::subject::inbox(&UserId::human(), &UserId::human());
        bus::publish_json(&human, subject, &forged, None).await.unwrap();

        let peek = Select { peek: true, ..Default::default() };
        assert_eq!(texts(&read(&human, peek.clone()).await), ["one", "two", "three"]);
        assert_eq!(texts(&read(&human, peek).await), ["one", "two", "three"], "peeking marks nothing read");

        let thread_b = Select { thread: Some(asks[1].thread_id.clone()), ..Default::default() };
        assert_eq!(texts(&read(&human, thread_b).await), ["two"]);
        let first = Select { limit: Some(1), ..Default::default() };
        assert_eq!(texts(&read(&human, first).await), ["one"]);

        let last = asks[0].reply(dev_a.clone(), "four", Role::Human).unwrap();
        bus::publish(&agent, &last).await.unwrap();
        assert!(mark_read(&human, &UserId::human(), &last).await.unwrap(), "ask --wait marks its reply read");
        assert_eq!(texts(&read(&human, Select::default()).await), ["three"]);
        assert!(read(&human, Select::default()).await.is_empty());
        assert!(!mark_read(&human, &UserId::human(), &last).await.unwrap());
    }
}
