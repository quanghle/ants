//! Thread notes (plan rooms in the code): a thread's posts live in the ANTS stream on `ants.plan.<thread>.<from>`.
//!
//! A reader reads a room back through its own consumer `plan-<persona>`, created for the read and deleted after it, so
//! its credentials need not let it touch any other consumer. Posts whose sender or thread disagrees with their subject
//! are dropped. A post states the stream sequence of the room's last post it saw, so two agents spending the same turn
//! cannot both succeed: the second one reads the room again.

use std::time::Duration;

use ants_proto::{PersonaId, PlanPost, STREAM, ThreadId, plan_consumer, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream::context::PublishErrorKind;
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;
use futures::future::BoxFuture;

use crate::bus;

/// How long a consumer left behind by an interrupted read lives on.
const INACTIVE: Duration = Duration::from_secs(30);
/// Messages asked for at once.
const BATCH: u64 = 256;
/// Reads of a busy room before [`spend`] gives up.
const SPEND_TRIES: usize = 5;
/// Header naming the subjects `Nats-Expected-Last-Subject-Sequence` applies to (nats-server 2.11).
const EXPECTED_LAST_SUBJECT: &str = "Nats-Expected-Last-Subject-Sequence-Subject";

/// The posts of one room, oldest first.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Room {
    pub posts: Vec<PlanPost>,
    /// Stream sequence of the room's last message, 0 for an empty room.
    pub last_seq: u64,
}

impl Room {
    pub fn last(&self) -> Option<&PlanPost> {
        self.posts.last()
    }
}

/// Reads and writes plan rooms.
pub trait Rooms: Send + Sync {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> BoxFuture<'a, Result<Room>>;

    /// Store `post` unless its room got another message after `last_seq`; false then.
    fn post<'a>(&'a self, post: &'a PlanPost, last_seq: u64) -> BoxFuture<'a, Result<bool>>;
}

/// Plan rooms in the ANTS stream, as `reader` sees them.
pub struct JetStreamRooms {
    js: jetstream::Context,
    reader: PersonaId,
    /// One read at a time: they share the reader's consumer.
    reading: tokio::sync::Mutex<()>,
}

impl JetStreamRooms {
    pub fn new(js: jetstream::Context, reader: PersonaId) -> Self {
        Self { js, reader, reading: tokio::sync::Mutex::new(()) }
    }
}

impl Rooms for JetStreamRooms {
    fn read<'a>(&'a self, thread: &'a ThreadId) -> BoxFuture<'a, Result<Room>> {
        Box::pin(async move {
            let _one = self.reading.lock().await;
            read(&self.js, &self.reader, thread).await
        })
    }

    fn post<'a>(&'a self, post: &'a PlanPost, last_seq: u64) -> BoxFuture<'a, Result<bool>> {
        Box::pin(publish(&self.js, post, Some(last_seq)))
    }
}

/// The room of `thread`, read through the consumer of `reader`. Only one read per reader may run at a time.
pub async fn read(js: &jetstream::Context, reader: &PersonaId, thread: &ThreadId) -> Result<Room> {
    let name = plan_consumer(reader);
    // A consumer left by an interrupted read would carry on where it stopped.
    let _ = js.delete_consumer_from_stream(&name, STREAM).await;
    let config = consumer::pull::Config {
        name: Some(name.clone()),
        filter_subject: subject::plan_room(thread),
        ack_policy: consumer::AckPolicy::None,
        inactive_threshold: INACTIVE,
        ..Default::default()
    };
    let consumer: consumer::PullConsumer = js
        .create_consumer_on_stream(config, STREAM)
        .await
        .with_context(|| format!("reading the notes of thread {thread}"))?;
    let room = collect(&consumer, thread).await;
    if let Err(e) = js.delete_consumer_from_stream(&name, STREAM).await {
        tracing::debug!(error = %e, consumer = name, "deleting the plan consumer");
    }
    room
}

async fn collect(consumer: &consumer::PullConsumer, thread: &ThreadId) -> Result<Room> {
    let mut room = Room::default();
    let mut left = consumer.cached_info().num_pending;
    while left > 0 {
        let mut batch = consumer.fetch().max_messages(left.min(BATCH) as usize).messages().await?;
        let mut got = 0;
        while let Some(msg) = batch.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!(e))?;
            got += 1;
            if let Ok(info) = msg.info() {
                room.last_seq = info.stream_sequence;
            }
            let post = serde_json::from_slice::<PlanPost>(&msg.payload)
                .map_err(|e| e.to_string())
                .and_then(|post| post.check_subject(&msg.subject).map(|()| post).map_err(|e| e.to_string()));
            match post {
                Ok(post) => room.posts.push(post),
                Err(error) => tracing::warn!(%thread, subject = %msg.subject, error, "dropping a plan post"),
            }
        }
        if got == 0 {
            break;
        }
        left = left.saturating_sub(got);
    }
    Ok(room)
}

/// Store `post` in its room once JetStream has it. With `last_seq`, only if the room's last message is still that one
/// (0: the room is empty); false otherwise. Without, the post may wait in the machine's outbox while the hub is out of
/// reach ([`bus::store`]); a post that takes a turn cannot, as only the hub knows whose turn it is.
pub async fn publish(js: &jetstream::Context, post: &PlanPost, last_seq: Option<u64>) -> Result<bool> {
    let Some(seq) = last_seq else {
        bus::store(js, post.subject(), post, &post.id.to_string()).await?;
        return Ok(true);
    };
    let message = jetstream::message::PublishMessage::build()
        .payload(serde_json::to_vec(post)?.into())
        .expected_last_subject_sequence(seq)
        .header(EXPECTED_LAST_SUBJECT, subject::plan_room(&post.thread_id).as_str());
    let ack = js.send_publish(post.subject(), message).await.context("posting")?;
    match ack.await {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => Ok(false),
        Err(e) => Err(e).context("waiting for JetStream to store the post"),
    }
}

/// Post `text` from `from` to the plan room of `thread`, spending one of its turns, at most `budget` left: the room is
/// read again when another post took the turn first. None when the room has no turns left.
pub async fn spend(
    rooms: &dyn Rooms,
    thread: &ThreadId,
    from: &PersonaId,
    text: &str,
    budget: u32,
) -> Result<Option<PlanPost>> {
    for _ in 0..SPEND_TRIES {
        let room = rooms.read(thread).await?;
        let Ok(post) = PlanPost::spend(thread.clone(), from.clone(), text, budget, room.last()) else {
            return Ok(None);
        };
        if rooms.post(&post, room.last_seq).await? {
            return Ok(Some(post));
        }
    }
    anyhow::bail!("the notes of thread {thread} kept changing")
}
