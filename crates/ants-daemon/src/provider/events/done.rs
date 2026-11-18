//! The threads marked done ([`EventKind::ThreadDone`](ants_proto::provider::EventKind::ThreadDone)): a human, or an
//! assistant for one, puts a thread's done state ([`ThreadDone`]) in [`THREADS_BUCKET`]. The daemon serving an ant
//! hears every done state put there through the ant's durable consumer ([`done_reader`]), one at a time, and tells
//! the ant's provider of those on a thread the ant holds a session of. It acks a done state once its event is stored,
//! so one put while the daemon was down is told once it is back.

use std::time::Duration;

use ants_proto::{THREADS_BUCKET, ThreadDone, ThreadId, UserId, done_reader};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;

use super::Lifecycle;

/// How long a done state waits for its ack before it comes again.
const ACK_WAIT: Duration = Duration::from_secs(30);
/// The backoff after the consumer failed, doubling with each next failure up to [`LAST_RETRY`].
const FIRST_RETRY: Duration = Duration::from_secs(5);
const LAST_RETRY: Duration = Duration::from_mins(10);

/// Hear of the threads marked done for good, telling `lifecycle` of those `has_session` says the ant `ant` holds a
/// session of; pending for good without a lifecycle.
pub async fn hear(
    js: &jetstream::Context,
    ant: &UserId,
    lifecycle: Option<&Lifecycle>,
    has_session: impl AsyncFn(&ThreadId) -> bool,
) {
    let Some(lifecycle) = lifecycle else { return std::future::pending().await };
    let mut backoff = FIRST_RETRY;
    loop {
        match consumer(js, ant).await {
            Ok(mut dones) => loop {
                match dones.next().await {
                    Some(Ok(message)) => {
                        backoff = FIRST_RETRY;
                        take(&message, lifecycle, &has_session).await;
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, retry_in = backoff.as_secs(), "hearing of the threads marked done");
                        break;
                    }
                    None => break,
                }
            },
            Err(e) => {
                let error = format!("{e:#}");
                tracing::warn!(error, retry_in = backoff.as_secs(), "hearing of the threads marked done");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(LAST_RETRY);
    }
}

/// The done states put from now on, from the durable consumer of `ant`, created if missing: one in hand at a time, so
/// they are told in the order they were put.
pub async fn consumer(js: &jetstream::Context, ant: &UserId) -> Result<consumer::pull::Stream> {
    let stream = js.get_stream_no_info(format!("KV_{THREADS_BUCKET}")).await?;
    let name = done_reader(ant);
    let consumer: consumer::PullConsumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: format!("$KV.{THREADS_BUCKET}.done.*"),
            deliver_policy: consumer::DeliverPolicy::New,
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: ACK_WAIT,
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .with_context(|| {
            format!("creating consumer {name} (`ants init` as a human creates bucket {THREADS_BUCKET})")
        })?;
    Ok(consumer.messages().await?)
}

/// Tell of the thread `message` marks done, when the ant holds a session of it, then ack the message: not when the
/// event could not be stored, so the message comes again.
async fn take(message: &jetstream::Message, lifecycle: &Lifecycle, has_session: &impl AsyncFn(&ThreadId) -> bool) {
    if let Some((thread, done)) = marked(message)
        && has_session(&thread).await
    {
        let seq = match message.info() {
            Ok(info) => info.stream_sequence,
            Err(e) => return tracing::warn!(%thread, error = %e, "reading a done state's sequence"),
        };
        if let Err(e) = lifecycle.thread_done(&thread, seq, done.at).await {
            return tracing::warn!(%thread, error = format!("{e:#}"), "telling of a thread marked done");
        }
    }
    if let Err(e) = message.ack().await {
        tracing::warn!(error = %e, "acking a done state");
    }
}

/// The thread a message of the bucket's stream marks done, and its done state: None for a deleted one, or one that
/// does not parse or names another thread than its key.
fn marked(message: &jetstream::Message) -> Option<(ThreadId, ThreadDone)> {
    let removed = message.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
    if removed {
        return None;
    }
    let thread: ThreadId =
        message.subject.as_str().strip_prefix(&format!("$KV.{THREADS_BUCKET}.done."))?.parse().ok()?;
    let done: ThreadDone = serde_json::from_slice(&message.payload).ok()?;
    (done.thread_id == thread).then_some((thread, done))
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::{Event, EventKind};
    use ants_proto::{UserId, done_key};
    use ants_test_support::NatsServer;

    use super::*;
    use crate::bus;
    use crate::provider::events::tests::{Recording, outbox_to};
    use crate::sync::MutexExt as _;

    #[tokio::test]
    async fn a_thread_marked_done_is_told_to_the_provider_of_an_ant_holding_its_session_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let stream = js.create_stream(crate::hub::threads_config()).await.unwrap();
        let threads = js.get_key_value(THREADS_BUCKET).await.unwrap();
        let ant: UserId = "dev-a".parse().unwrap();
        let (held, other, later) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
        let provider = Recording::taking_events();
        let outbox = outbox_to(provider.clone()).await;
        let home = server.dir().join("lifecycle");
        let lifecycle = Lifecycle::open(&outbox, &ant, "id-1", &home).await.unwrap();
        let has_session = async |thread: &ThreadId| *thread != other;
        let mark = async |thread: &ThreadId| {
            let done = ThreadDone::new(thread.clone(), UserId::human(), 7);
            let seq = threads.put(done_key(thread), serde_json::to_vec(&done).unwrap().into()).await.unwrap();
            (done, seq)
        };
        let acked = async |what: &str, seq| {
            ants_test_support::eventually(what, async || {
                stream.consumer_info(done_reader(&ant)).await.unwrap().ack_floor.stream_sequence >= seq
            })
            .await;
        };
        let drained = async || {
            ants_test_support::eventually("the provider outbox drained", async || {
                outbox.pending(&ant).await.is_empty()
            })
            .await;
        };
        let thread_done = |thread: &ThreadId, done: &ThreadDone, told: &Event| {
            assert_eq!(told.kind, EventKind::ThreadDone);
            assert_eq!((&told.ant, told.identity.as_str()), (&ant, "id-1"));
            assert_eq!((told.thread.as_ref(), told.at), (Some(thread), done.at));
        };

        let marked = tokio::select! {
            () = hear(&js, &ant, Some(&lifecycle), has_session) => unreachable!(),
            marked = async {
                ants_test_support::eventually("the consumer", async || {
                    stream.consumer_info(done_reader(&ant)).await.is_ok()
                })
                .await;
                mark(&other).await;
                let (marked, seq) = mark(&held).await;
                // The bucket keeps only one revision per key: deleting before the ack can replace an unread mark.
                acked("the held thread's done mark acked", seq).await;
                let key = done_key(&held);
                threads.delete(&key).await.unwrap();
                let deleted = threads.entry(&key).await.unwrap().unwrap();
                assert_eq!(deleted.operation, jetstream::kv::Operation::Delete);
                acked("the done mark's tombstone acked", deleted.revision).await;
                marked
            } => marked,
        };
        drained().await;
        let got = provider.got.locked().clone();
        assert_eq!(got.len(), 2, "the ant told added, then of the one thread it holds a session of: {got:?}");
        assert_eq!(got[0].kind, EventKind::AntAdded);
        thread_done(&held, &marked, &got[1]);

        let (marked, seq) = mark(&later).await;
        tokio::select! {
            () = hear(&js, &ant, Some(&lifecycle), has_session) => unreachable!(),
            () = acked("the done mark put while the reader was stopped acked", seq) => {}
        }
        drained().await;
        let got = provider.got.locked();
        assert_eq!(got.len(), 3, "only the new mark told after the reader resumed: {got:?}");
        thread_done(&later, &marked, &got[2]);
    }
}
