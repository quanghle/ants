//! The threads marked done ([`EventKind::ThreadDone`](ants_proto::provider::EventKind::ThreadDone)): a human, or an
//! assistant for one, marks a thread done ([`mark`]), putting its marking under a key of its own ([`done_mark_key`])
//! before the thread's done state ([`ThreadDone`]) in [`THREADS_BUCKET`]. The bucket keeps one message per key, so an
//! undo or a newer mark replaces a done state; a marking stays for [`MARKING_KEPT`], so no later change of the
//! thread's done state takes it before each ant has heard of it. The daemon serving an ant hears every marking put
//! there through the ant's durable consumer ([`done_reader`]), one at a time, and tells the ant's provider of those on
//! a thread the ant holds a session of. It acks a marking once its event is stored, so one put while the daemon was
//! down is told once it is back.

use crate::bus::relayed::{self, Read};
use std::time::Duration;

use ants_proto::{THREADS_BUCKET, ThreadDone, ThreadId, UserId, done_key, done_mark_key, done_reader};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;
use uuid::Uuid;

use super::Lifecycle;
use crate::records::direct_kv::DirectKv;

/// How long a marking waits for its ack before it comes again.
const ACK_WAIT: Duration = Duration::from_secs(30);
/// The backoff after the consumer failed, doubling with each next failure up to [`LAST_RETRY`].
const FIRST_RETRY: Duration = Duration::from_secs(5);
const LAST_RETRY: Duration = Duration::from_mins(10);
/// How long a marking of a thread done stays for the daemons of ants not served meanwhile to hear of.
pub const MARKING_KEPT: Duration = crate::bus::RETENTION;

/// Mark the thread of `done` done in `threads`: its marking, for the ants' daemons to hear of, then its done state.
/// A marking stored without its done state, as the second put failed, is told all the same, at least once.
pub async fn mark(threads: &DirectKv, done: &ThreadDone) -> Result<()> {
    let value = serde_json::to_vec(done)?;
    let marking = done_mark_key(&done.thread_id, &Uuid::new_v4());
    threads.put_for(&marking, value.clone(), MARKING_KEPT).await.context("storing the marking done")?;
    threads.put(done_key(&done.thread_id), value).await.context("storing the done state")
}

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

/// The markings put from now on, from the durable consumer of `ant`, created if missing: one in hand at a time, so
/// they are told in the order they were put.
pub async fn consumer(js: &jetstream::Context, ant: &UserId) -> Result<crate::bus::Pull> {
    let stream = js.get_stream_no_info(format!("KV_{THREADS_BUCKET}")).await?;
    let name = done_reader(ant);
    let consumer: consumer::PullConsumer = stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(name.clone()),
            filter_subject: format!("$KV.{THREADS_BUCKET}.done.*.*"),
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
    Ok(crate::bus::pull(consumer).await?)
}

/// Tell of the thread `message` marks done, when the ant holds a session of it, then ack the message: not when the
/// event could not be stored, so the message comes again.
async fn take(message: &jetstream::Message, lifecycle: &Lifecycle, has_session: &impl AsyncFn(&ThreadId) -> bool) {
    if let Some((thread, done)) = marked(message)
        && has_session(&thread).await
    {
        let seq = match message.info() {
            Ok(info) => info.stream_sequence,
            Err(e) => return tracing::warn!(%thread, error = %e, "reading the sequence of a marking done"),
        };
        if let Err(e) = lifecycle.thread_done(&thread, seq, done.at).await {
            return tracing::warn!(%thread, error = format!("{e:#}"), "telling of a thread marked done");
        }
    }
    if let Err(e) = message.ack().await {
        tracing::warn!(error = %e, "acking a marking done");
    }
}

/// The thread a message of the bucket's stream marks done, and its done state: None for a deleted one, one a stream
/// relayed, or one that does not parse or names another thread than its key.
fn marked(message: &jetstream::Message) -> Option<(ThreadId, ThreadDone)> {
    if relayed::stored_relayed(message.headers.as_ref(), &message.payload, Read::Consumer) {
        tracing::warn!(subject = %message.subject, "skipping a done state a stream relayed");
        return None;
    }
    let removed = message.headers.as_ref().and_then(|h| h.get("KV-Operation")).is_some_and(|op| op.as_str() != "PUT");
    if removed {
        return None;
    }
    let marking = message.subject.as_str().strip_prefix(&format!("$KV.{THREADS_BUCKET}.done."))?;
    let thread: ThreadId = marking.split_once('.')?.0.parse().ok()?;
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
    use crate::records::direct_kv::Lookup;
    use crate::sync::MutexExt as _;

    #[tokio::test]
    async fn each_marking_of_a_thread_done_is_told_to_the_provider_of_an_ant_holding_its_session_once() {
        let Some(server) = NatsServer::start() else { return };
        let (_, js) = bus::connect(&server.url, None, None, &UserId::human(), "test").await.unwrap();
        let stream = js.create_stream(crate::hub::threads_config()).await.unwrap();
        let threads = DirectKv::new(&js, None, THREADS_BUCKET).await.unwrap();
        let ant: UserId = "dev-a".parse().unwrap();
        let [held, undone, other, later] = std::array::from_fn(|_| ThreadId::generate());
        let provider = Recording::taking_events();
        let outbox = outbox_to(provider.clone()).await;
        let home = server.dir().join("lifecycle");
        let lifecycle = Lifecycle::open(&outbox, &ant, "id-1", &home).await.unwrap();
        let has_session = async |thread: &ThreadId| *thread != other;
        let done_as_of = async |thread: &ThreadId, after| {
            let done = ThreadDone::new(thread.clone(), UserId::human(), after);
            super::mark(&threads, &done).await.unwrap();
            done
        };
        let clear = async |thread: &ThreadId| threads.delete(done_key(thread)).await.unwrap();
        let heard = async || {
            ants_test_support::eventually("every marking acked", async || {
                let info = stream.consumer_info(done_reader(&ant)).await.unwrap();
                info.num_pending == 0 && info.num_ack_pending == 0
            })
            .await;
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

        // Every change lands before the reader reads any: an undo or a newer mark replaces the done state only.
        drop(consumer(&js, &ant).await.unwrap());
        done_as_of(&other, 7).await;
        let first = done_as_of(&held, 7).await;
        clear(&held).await;
        let second = done_as_of(&held, 9).await;
        let cleared = done_as_of(&undone, 7).await;
        clear(&undone).await;
        tokio::select! {
            () = hear(&js, &ant, Some(&lifecycle), has_session) => unreachable!(),
            () = heard() => {}
        }
        let got = provider.got.locked().clone();
        assert_eq!(
            got.len(),
            4,
            "the ant told added, then of each marking of the threads it holds a session of: {got:?}"
        );
        assert_eq!(got[0].kind, EventKind::AntAdded);
        thread_done(&held, &first, &got[1]);
        thread_done(&held, &second, &got[2]);
        thread_done(&undone, &cleared, &got[3]);
        let Lookup::Value(state) = threads.lookup(done_key(&held)).await.unwrap() else { panic!("held is done") };
        assert_eq!(serde_json::from_slice::<ThreadDone>(&state).unwrap(), second, "the latest done state stays");
        assert!(matches!(threads.lookup(done_key(&undone)).await.unwrap(), Lookup::NoValue), "the undo stays");

        let marked = done_as_of(&later, 7).await;
        tokio::select! {
            () = hear(&js, &ant, Some(&lifecycle), has_session) => unreachable!(),
            () = heard() => {}
        }
        let got = provider.got.locked();
        assert_eq!(got.len(), 5, "only the new mark told after the reader resumed: {got:?}");
        thread_done(&later, &marked, &got[4]);
    }
}
