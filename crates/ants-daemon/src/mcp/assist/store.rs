//! The [`HumanStore`] on the hub's JetStream: the human's messages and the tool requests waiting for it read back
//! from the streams, its threads' titles, done marks, pins, archives and snoozes (kept with its reminders) in the
//! threads bucket, and the requests to the daemons of an ant answered over core NATS.

use crate::bus::relayed;
use std::collections::{BTreeMap, HashSet};
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use ants_proto::{
    ATTEMPTS_BUCKET, AdminOrder, ApprovalDecision, ApprovalRequest, CancelReply, CancelRequest, Envelope, Humans,
    JsDomain, QueueReply, QueueRequest, READ_BUCKET, Reaction, ReadMarker, Reminders, THREADS_BUCKET, ThreadDone,
    ThreadId, ThreadMarks, ThreadSnoozes, ThreadTitle, UserId, Uuid, assisted_marks_key, assisted_reminders_key,
    done_key, marks_key, reminders_key, subject, title_key,
};
use anyhow::{Context as _, Result};
use async_nats::jetstream::{self, consumer};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::{ANSWER_GRACE, ANSWER_WAIT, Change, HumanMessage, HumanStore};
use crate::delivery::attempts::{self, Recorded};
use crate::delivery::queue;
use crate::records::direct_kv::DirectKv;
use crate::records::{owners, reactions};
use crate::stream::reading::{self, Reading};

/// How many times a change of the human's marks or snoozes is tried again when its devices changed them meanwhile.
const CONFLICTS: usize = 16;

/// The [`HumanStore`] on the hub's JetStream, read as a human reads it, by direct gets; `js_domain` is the domain `js`
/// was made for, and `creds` the ant's credentials, which sign the orders it sends.
pub fn jetstream_store(js: jetstream::Context, js_domain: Option<JsDomain>, creds: PathBuf) -> Arc<dyn HumanStore> {
    Arc::new(JetStreamStore { js, js_domain, creds })
}

struct JetStreamStore {
    js: jetstream::Context,
    js_domain: Option<JsDomain>,
    creds: PathBuf,
}

impl JetStreamStore {
    async fn read(&self, filter: &str, since: SystemTime, mut take: impl FnMut(u64, &str, &[u8])) -> Result<()> {
        let start = consumer::DeliverPolicy::ByStartTime { start_time: since.into() };
        reading::read_back(&self.js, "", Reading::Gets, filter, start, |seq, _, subject, payload| {
            take(seq, subject, payload);
            ControlFlow::Continue(())
        })
        .await
    }

    async fn threads(&self) -> Result<DirectKv> {
        DirectKv::new(&self.js, self.js_domain.as_ref(), THREADS_BUCKET).await
    }

    /// The entry under `key` of [`THREADS_BUCKET`], and its stream sequence (0 for none).
    async fn entry<T: Default + DeserializeOwned>(&self, threads: &DirectKv, key: &str) -> Result<(T, u64)> {
        Ok(match threads.lookup_last(key).await? {
            (crate::records::direct_kv::Lookup::NoBucket, _) => anyhow::bail!("there is no bucket {THREADS_BUCKET}"),
            (crate::records::direct_kv::Lookup::NoValue, last) => (T::default(), last),
            (crate::records::direct_kv::Lookup::Value(value), last) => {
                (serde_json::from_slice(&value).unwrap_or_default(), last)
            }
        })
    }

    /// Change the entry under `key` of [`THREADS_BUCKET`] with `change`, by compare-and-set.
    async fn change<T: Default + Serialize + DeserializeOwned>(&self, key: String, change: Change<T>) -> Result<bool> {
        let threads = self.threads().await?;
        for _ in 0..CONFLICTS {
            let (mut value, last) = self.entry::<T>(&threads, &key).await?;
            if !change(&mut value) {
                return Ok(false);
            }
            if threads.update(&key, serde_json::to_vec(&value)?, last).await? {
                return Ok(true);
            }
        }
        anyhow::bail!("storing {key}: it kept changing")
    }

    /// Publish `payload` on `subject` with core NATS, and gather the answers on `reply` that `keep` takes.
    async fn ask<T: DeserializeOwned>(
        &self,
        subject: String,
        reply: String,
        payload: Vec<u8>,
        keep: impl Fn(&T, &str) -> bool,
    ) -> Result<Vec<T>> {
        let client = self.js.client();
        let mut answers = relayed::subscribe(&client, reply).await?;
        client.publish(subject, payload.into()).await?;
        client.flush().await?;
        let mut deadline = tokio::time::Instant::now() + ANSWER_WAIT;
        let mut found = Vec::new();
        while let Ok(Some(msg)) = tokio::time::timeout_at(deadline, answers.next()).await {
            let Ok(answer) = serde_json::from_slice::<T>(&msg.payload) else { continue };
            if !keep(&answer, &msg.subject) {
                continue;
            }
            if found.is_empty() {
                deadline = deadline.min(tokio::time::Instant::now() + ANSWER_GRACE);
            }
            found.push(answer);
        }
        Ok(found)
    }
}

impl HumanStore for JetStreamStore {
    fn messages(
        &self,
        human: UserId,
        assistant: UserId,
        since: SystemTime,
    ) -> BoxFuture<'_, Result<Vec<HumanMessage>>> {
        Box::pin(async move {
            let mut found = BTreeMap::new();
            for filter in [subject::inbox_of(&human), subject::inbox_from(&human), subject::inbox_from(&assistant)] {
                self.read(&filter, since, |seq, subject, payload| {
                    let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) else { return };
                    let for_human = envelope.from != assistant || envelope.on_behalf_of.as_ref() == Some(&human);
                    if for_human && envelope.check_subject(subject).is_ok() {
                        found.insert(seq, envelope);
                    }
                })
                .await
                .with_context(|| format!("reading {filter}"))?;
            }
            Ok(found.into_iter().map(|(seq, envelope)| HumanMessage { seq, envelope }).collect())
        })
    }

    fn approvals(&self, since: SystemTime) -> BoxFuture<'_, Result<(Vec<ApprovalRequest>, HashSet<Uuid>)>> {
        Box::pin(async move {
            let (mut requests, mut decided) = (Vec::new(), HashSet::new());
            self.read(subject::ALL_APPROVALS, since, |_, subject, payload| {
                if let Ok(request) = serde_json::from_slice::<ApprovalRequest>(payload)
                    && request.check_subject(subject).is_ok()
                {
                    // A cancelled copy settles the request it repeats.
                    match request.cancelled_at {
                        Some(_) => drop(decided.insert(request.id)),
                        None => requests.push(request),
                    }
                } else if let Ok(decision) = serde_json::from_slice::<ApprovalDecision>(payload)
                    && subject == decision.subject()
                {
                    decided.insert(decision.request_id);
                }
            })
            .await
            .context("reading the tool requests")?;
            Ok((requests, decided))
        })
    }

    fn read_marker(&self, human: UserId) -> BoxFuture<'_, Result<ReadMarker>> {
        Box::pin(async move {
            let read = DirectKv::new(&self.js, self.js_domain.as_ref(), READ_BUCKET).await?;
            match read.lookup(human.as_str()).await? {
                crate::records::direct_kv::Lookup::Value(value) => {
                    serde_json::from_slice(&value).with_context(|| format!("reading the read marker of {human}"))
                }
                _ => Ok(ReadMarker::default()),
            }
        })
    }

    fn done(&self, thread: ThreadId) -> BoxFuture<'_, Result<Option<ThreadDone>>> {
        Box::pin(async move {
            match self.threads().await?.lookup(done_key(&thread)).await? {
                crate::records::direct_kv::Lookup::Value(value) => Ok(serde_json::from_slice(&value).ok()),
                _ => Ok(None),
            }
        })
    }

    fn last_sequence(&self) -> BoxFuture<'_, Result<u64>> {
        Box::pin(reading::last_sequence(&self.js, "", Reading::Gets, ""))
    }

    fn put_title(&self, title: ThreadTitle) -> BoxFuture<'_, Result<()>> {
        Box::pin(
            async move { self.threads().await?.put(title_key(&title.thread_id), serde_json::to_vec(&title)?).await },
        )
    }

    fn put_done(&self, done: ThreadDone) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move { crate::provider::events::done::mark(&self.threads().await?, &done).await })
    }

    fn marks(&self, human: UserId) -> BoxFuture<'_, Result<ThreadMarks>> {
        Box::pin(async move { Ok(self.entry(&self.threads().await?, &marks_key(&human)).await?.0) })
    }

    fn snoozes(&self, human: UserId) -> BoxFuture<'_, Result<ThreadSnoozes>> {
        Box::pin(async move {
            let reminders: Reminders = self.entry(&self.threads().await?, &reminders_key(&human)).await?.0;
            Ok(reminders.snoozed)
        })
    }

    fn reminders(&self, human: UserId, assistant: UserId) -> BoxFuture<'_, Result<(Reminders, ThreadSnoozes)>> {
        Box::pin(async move {
            let threads = self.threads().await?;
            let own: Reminders = self.entry(&threads, &reminders_key(&human)).await?.0;
            let assisted: Reminders = self.entry(&threads, &assisted_reminders_key(&human, &assistant)).await?.0;
            Ok((own, assisted.snoozed))
        })
    }

    fn change_marks(
        &self,
        human: UserId,
        assistant: UserId,
        change: Change<ThreadMarks>,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(self.change(assisted_marks_key(&human, &assistant), change))
    }

    fn change_snoozes(
        &self,
        human: UserId,
        assistant: UserId,
        change: Change<ThreadSnoozes>,
    ) -> BoxFuture<'_, Result<bool>> {
        let change: Change<Reminders> = Box::new(move |r| change(&mut r.snoozed));
        Box::pin(self.change(assisted_reminders_key(&human, &assistant), change))
    }

    fn react(
        &self,
        by: UserId,
        message: Uuid,
        reaction: Reaction,
        remove: bool,
        human: UserId,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            let domain = self.js_domain.as_ref();
            if remove {
                reactions::remove(&self.js, domain, &by, &message, &reaction).await
            } else {
                reactions::add_for(&self.js, domain, &by, &message, &reaction, Some(human)).await
            }
        })
    }

    fn record_owner(
        &self,
        thread: ThreadId,
        to: UserId,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<()>> {
        let domain = self.js_domain.as_ref();
        Box::pin(async move { owners::record_for_human(&self.js, domain, &thread, &to, &human, &assistant).await })
    }

    fn cancel(&self, request: CancelRequest) -> BoxFuture<'_, Result<Vec<CancelReply>>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&request)?;
            let keep = |a: &CancelReply, at: &str| a.request_id == request.id && a.check_subject(at).is_ok();
            self.ask(request.subject(), request.reply_subject(), payload, keep).await
        })
    }

    fn queue(
        &self,
        request: QueueRequest,
        humans: Humans,
        shown: usize,
    ) -> BoxFuture<'_, Result<Option<queue::Listed>>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&request)?;
            let keep = |a: &QueueReply, at: &str| a.request_id == request.id && a.check_subject(at).is_ok();
            let answers = self.ask(request.subject(), request.reply_subject(), payload, keep).await?;
            if answers.is_empty() {
                return Ok(None);
            }
            let kv = DirectKv::new(&self.js, self.js_domain.as_ref(), ATTEMPTS_BUCKET).await?;
            let (mut waiting, more) = queue::waiting(&self.js, &kv, &request.ant, humans, &answers, shown).await?;
            queue::withdrawals(&kv, &request.ant, &mut waiting).await?;
            Ok(Some((waiting, more)))
        })
    }

    fn record_withdrawal(
        &self,
        ant: UserId,
        message: Uuid,
        human: UserId,
        assistant: UserId,
    ) -> BoxFuture<'_, Result<Recorded>> {
        Box::pin(async move {
            let kv = DirectKv::new(&self.js, self.js_domain.as_ref(), ATTEMPTS_BUCKET).await?;
            attempts::record_withdrawal(&kv, &ant, &message, &assistant, &human).await
        })
    }

    fn send_order(&self, mut order: AdminOrder) -> BoxFuture<'_, Result<AdminOrder>> {
        Box::pin(async move {
            let (jwt, key) = crate::secrets::creds::load(&self.creds, crate::secrets::keystore::os())?;
            order.credentials = jwt;
            order.signature = data_encoding::BASE64URL_NOPAD.encode(&key.sign(&order.signed())?);
            let message = jetstream::message::PublishMessage::build()
                .payload(serde_json::to_vec(&order)?.into())
                .message_id(order.id.to_string());
            let subject = order.subject();
            let stored = async { self.js.send_publish(subject.clone(), message).await?.await };
            stored.await.with_context(|| format!("sending the admin order on {subject}"))?;
            Ok(order)
        })
    }

    fn identity_id(&self, ant: UserId) -> BoxFuture<'_, Option<String>> {
        Box::pin(async move { crate::profile::identity::id_of(&self.js, self.js_domain.as_ref(), &ant).await })
    }
}
