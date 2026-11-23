//! The messages in hand: the inbox loop that takes them in, queues them on their threads and runs each thread's
//! next one, and lists them for `ants queue`.

use crate::bus::{self, relayed};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::time::{Duration, Instant};

use ants_proto::{ConsumerPlace, QueueRequest, QueuedMessage, State, StatusUpdate, ThreadId, Uuid, subject};
use anyhow::{Context as _, Result};
use async_nats::jetstream;
use futures::future::{FusedFuture as _, FutureExt as _};
use futures::stream::{FuturesUnordered, Stream, StreamExt as _};

use crate::channel::Channels;
use crate::monitoring::status;

use super::control::{Cancel, Why};
use super::handling::Handled;
use super::{EVICT_EVERY, InHand, Inbox, Incoming, Steered, is_post, sleep_until, stream_seq};

/// How long [`Inbox::answer_queue`] waits for each consumer's info, well inside the 5 s `ants queue` waits for an
/// answer.
const QUEUE_READ: Duration = Duration::from_secs(2);

/// The messages of one thread in hand: one in flight at most, and those joining its prompt, the rest waiting their
/// turn.
#[derive(Default)]
pub(super) struct Thread {
    waiting: VecDeque<InHand>,
    busy: bool,
    /// The messages joining the prompt in flight ([`Steered::Joining`]), until they are answered or wait again.
    joining: Vec<Joiner>,
    /// Stream sequence of the message in flight.
    in_flight: Option<u64>,
    /// The message in flight, as `ants queue` lists it.
    running: Option<QueuedMessage>,
    /// A failed message that is to be redelivered. Until it is back (or the deadline passes) the rest of the thread
    /// waits rather than overtake it.
    retry: Option<Retry>,
    /// The messages told they wait behind others ([`StatusUpdate::Queued`]), to tell when their turn comes.
    queued: HashSet<Uuid>,
}

/// A message joining the prompt in flight on its thread.
struct Joiner {
    id: Uuid,
    seq: Option<u64>,
    /// The message, as `ants queue` lists it.
    queued: Option<QueuedMessage>,
}

pub(super) struct Retry {
    pub(super) seq: u64,
    pub(super) until: tokio::time::Instant,
    /// The message, as `ants queue` lists it.
    pub(super) queued: Option<QueuedMessage>,
}

/// What a message handed back waits for, first on its thread.
enum Wait {
    SignIn,
    Limit,
    Repair,
    Hub,
}

impl Thread {
    /// Whether the message with stream sequence `seq` is in flight or waiting. The server redelivers one whose progress
    /// acks were lost, as on a link that went silent for longer than the ack wait.
    fn holds(&self, seq: Option<u64>) -> bool {
        seq.is_some()
            && (self.in_flight == seq
                || self.joining.iter().any(|joiner| joiner.seq == seq)
                || self.waiting.iter().any(|(_, msg)| stream_seq(msg) == seq))
    }

    /// How many messages one arriving now waits behind: the one in flight or to be redelivered, those joining it and
    /// those waiting.
    fn ahead(&self) -> usize {
        usize::from(self.busy) + usize::from(self.retry.is_some()) + self.joining.len() + self.waiting.len()
    }

    /// Where a message steering the thread goes among those waiting: after those that steered it before, which `seq`
    /// follows.
    fn steered_at(&self, seq: Option<u64>) -> usize {
        let before = |msg| seq.is_none() || stream_seq(msg) < seq;
        self.waiting.iter().take_while(|(waiting, msg)| waiting.steered != Steered::No && before(msg)).count()
    }
}

/// Where a message in hand stands on its thread.
#[derive(Clone, Copy)]
pub(super) enum Place {
    Running,
    /// Waiting with `ahead` messages of its thread before it.
    Waiting {
        ahead: usize,
    },
    /// Awaiting its redelivery after a failed attempt.
    Retry,
}

impl Inbox<'_> {
    /// The requests for the ant's queue, those naming their sender, an assistant's, apart.
    async fn queue_asks(&self) -> impl Stream<Item = async_nats::Message> + Unpin {
        let subjects = [subject::queue(self.ant), subject::queues_by_of(self.ant)];
        match relayed::subscribe_all(self.client, subjects).await {
            Ok(asks) => asks.left_stream(),
            Err(e) => {
                tracing::warn!(error = %e, "serving without ants queue");
                futures::stream::pending().right_stream()
            }
        }
    }

    /// Handle messages, from the inbox and channel posts, until they end or `stop` resolves, then finish the prompts in
    /// flight unless it resolves again. `channels` says which posts are meant for the ant.
    pub(super) async fn serve<S, E, F>(
        &self,
        mut messages: S,
        mut channels: Channels,
        idle_timeout: Duration,
        mut stop: impl FnMut() -> F,
    ) where
        S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
        E: Display,
        F: Future<Output = ()>,
    {
        let mut threads: HashMap<ThreadId, Thread> = HashMap::new();
        let mut running = FuturesUnordered::new();
        let mut evict = tokio::time::interval(EVICT_EVERY.min(idle_timeout).max(Duration::from_secs(1)));
        let mut progress = self.progress();
        let shutdown = stop();
        let finishing = self.finish_buried();
        let signing_in = futures::future::Fuse::terminated();
        let lifting = futures::future::Fuse::terminated();
        let repairing = futures::future::Fuse::terminated();
        let stalling = futures::future::Fuse::terminated();
        tokio::pin!(shutdown, finishing, signing_in, lifting, repairing, stalling);
        let mut asks = self.queue_asks().await;
        let mut answering = FuturesUnordered::new();
        let mut paused = self.pause.subscribe();
        let mut drift = self.drift.subscribe();
        loop {
            let retry_deadline = threads.values().filter_map(|t| t.retry.as_ref().map(|r| r.until)).min();
            // Every fallback was turned away lately: the first to be tried again lets the messages on.
            let now = tokio::time::Instant::now();
            let fallback_up =
                self.holds.failover.next_up(now).filter(|_| self.outage().is_some() && self.fallback(now).is_none());
            let mut ready = None;
            tokio::select! {
                () = &mut shutdown => break,
                () = &mut finishing => {}
                () = channels.changed() => {}
                _ = evict.tick() => {
                    self.pool.evict_idle(Instant::now());
                }
                _ = progress.tick() => ack_progress(&threads).await,
                () = sleep_until(retry_deadline), if retry_deadline.is_some() => {
                    give_up_retries(&mut threads);
                    ready = Some(threads.keys().cloned().collect());
                }
                () = &mut signing_in => ready = Some(threads.keys().cloned().collect()),
                () = &mut lifting => ready = Some(threads.keys().cloned().collect()),
                () = &mut repairing => ready = Some(threads.keys().cloned().collect()),
                () = &mut stalling => ready = Some(threads.keys().cloned().collect()),
                Ok(()) = paused.changed() => ready = Some(threads.keys().cloned().collect()),
                Ok(()) = drift.changed() => self.workload.set_drift(drift.borrow_and_update().clone()),
                () = sleep_until(fallback_up), if fallback_up.is_some() => ready = Some(threads.keys().cloned().collect()),
                Some(ask) = asks.next() => {
                    if let Some(request) = queue_request(&ask) {
                        answering.push(self.answer_queue(request, in_hand(&threads)));
                    }
                }
                Some(()) = answering.next(), if !answering.is_empty() => {}
                Some((thread, handled, joiner)) = running.next(), if !running.is_empty() => {
                    match handed_back(&mut threads, &thread, handled, joiner, &self.holds.stall) {
                        Some(Wait::SignIn) if signing_in.is_terminated() => signing_in.set(self.signed_in().fuse()),
                        Some(Wait::Limit) if lifting.is_terminated() => lifting.set(self.limit_passed().fuse()),
                        Some(Wait::Repair) if repairing.is_terminated() => repairing.set(self.repaired().fuse()),
                        Some(Wait::Hub) if stalling.is_terminated() => stalling.set(self.hub_back().fuse()),
                        _ => {}
                    }
                    ready = Some(vec![thread]);
                }
                next = messages.next() => match next {
                    None => break,
                    Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                    Some(Ok(msg)) => {
                        let taken = Box::pin(self.take_in(msg, &mut channels, &mut threads)).await;
                        let Some(thread) = taken else { continue };
                        ready = Some(vec![thread]);
                    }
                }
            }
            // While the harness is signed out, a provider limit holds or the harness keeps failing to start, messages
            // wait in hand, unless a fallback answers them: the consumer's limit on them stops deliveries. A rate limit
            // is waited out, as is a hub full or out of reach. Paused, they wait in hand, the consumers not pulled from.
            let now = tokio::time::Instant::now();
            let pause = paused.borrow_and_update().clone();
            let held = pause.is_some()
                || self.holds.throttle.is_held() && !self.holds.throttle.is_quota_held()
                || self.holds.stall.is_on()
                || self.outage().is_some() && self.fallback(now).is_none();
            self.workload.set_paused(pause);
            for thread in ready.into_iter().flatten().filter(|_| !held) {
                while let Some((incoming, msg)) = next_turn(&mut threads, thread.clone()) {
                    if let Some(t) = threads.get_mut(&thread) {
                        self.queue_moved(&thread, t, incoming.envelope.id);
                    }
                    let joiner = (incoming.steered == Steered::Joining).then_some(incoming.envelope.id);
                    running.push(self.handle(incoming, msg).map(move |(thread, handled)| (thread, handled, joiner)));
                }
            }
            // Not those awaiting a redelivery, which may take a while.
            let in_hand = threads.iter().filter(|(_, t)| t.retry.is_none()).map(|(thread, _)| thread.clone());
            self.pool.set_in_hand(in_hand.collect());
            self.workload.set_harnesses(self.pool.live());
        }

        drop(messages);
        hand_back_waiting(threads).await;
        if !running.is_empty() {
            tracing::info!(prompts = running.len(), "finishing prompts in flight; Ctrl-C again stops now");
            let drain = async { while running.next().await.is_some() {} };
            tokio::select! {
                () = drain => {}
                () = stop() => tracing::warn!("stopping mid-prompt; those messages are redelivered"),
            }
        }
    }

    /// Admit `msg` and queue it on its thread: the thread, or `None` when the message is not to be handled now.
    async fn take_in(
        &self,
        msg: jetstream::Message,
        channels: &mut Channels,
        threads: &mut HashMap<ThreadId, Thread>,
    ) -> Option<ThreadId> {
        let incoming =
            if is_post(&msg) { self.admit_post(&msg, channels).await } else { Box::pin(self.admit(&msg)).await };
        let incoming = incoming?;
        let thread = incoming.envelope.thread_id.clone();
        let t = threads.entry(thread.clone()).or_default();
        if t.holds(stream_seq(&msg)) {
            // The copy in hand gets the ack: the server tracks a message by its stream sequence.
            tracing::debug!(%thread, "ignoring a redelivery of a message in hand");
            return None;
        }
        if t.retry.as_ref().is_some_and(|r| Some(r.seq) == stream_seq(&msg)) {
            t.retry = None;
            t.waiting.push_front((incoming, msg));
        } else if let Some(steered) = self.steer(&incoming, &thread, t) {
            let at = t.steered_at(None);
            let joins = steered == Steered::Joining;
            tracing::info!(%thread, by = %incoming.envelope.from, ahead = at, joins, "a message steers the thread");
            t.waiting.insert(at, (Incoming { steered, ..incoming }, msg));
            let behind = t.waiting.iter().map(|(waiting, _)| waiting.envelope.id).enumerate().skip(at + 1);
            for (before, message) in behind.filter(|(_, message)| t.queued.contains(message)) {
                status::publish(self.client, self.ant, &thread, StatusUpdate::Queued { message, ahead: before + 1 });
            }
        } else {
            let (message, ahead) = (incoming.envelope.id, t.ahead());
            t.waiting.push_back((incoming, msg));
            if ahead > 0 {
                t.queued.insert(message);
                status::publish(self.client, self.ant, &thread, StatusUpdate::Queued { message, ahead });
            }
            if self.holds.sign_in.is_out() && self.fallback(tokio::time::Instant::now()).is_none() {
                status::state(self.client, self.ant, &thread, State::SignedOut);
            }
        }
        Some(thread)
    }

    /// A message that steers ([`Inbox::steers`]) joins the prompt on its thread when the harness takes it into its
    /// turn, else interrupts it, unless an earlier one already did.
    fn steer(&self, incoming: &Incoming, thread: &ThreadId, t: &Thread) -> Option<Steered> {
        let envelope = &incoming.envelope;
        if !t.busy || !self.steers(incoming) {
            return None;
        }
        let first = t.waiting.front().map(|(waiting, _)| waiting.steered);
        if matches!(first, Some(Steered::Interrupted | Steered::Ahead)) {
            return Some(Steered::Ahead);
        }
        if self.running.steers.takes(thread) {
            return Some(Steered::Joining);
        }
        let cancel = Cancel { by: envelope.from.clone(), why: Why::Steer };
        self.running.cancels.cancel(thread, cancel).then_some(Steered::Interrupted)
    }

    /// Whether `incoming` steers the prompt running on its thread rather than wait its turn: a human's message, or
    /// another ant's that starts something rather than answers (no reply, escalation, notice, triage or webhook),
    /// unless marked to wait ([`ants_proto::Envelope::queue`]).
    fn steers(&self, incoming: &Incoming) -> bool {
        let envelope = &incoming.envelope;
        if envelope.queue {
            return false;
        }
        if self.humans.sender_role(envelope).is_human() {
            return true;
        }
        let answers = match &incoming.post {
            Some(posted) => posted.post.in_reply_to.is_some() || posted.triage.is_some(),
            None => envelope.in_reply_to.is_some(),
        };
        !answers
            && !envelope.webhook
            && envelope.escalation.is_none()
            && envelope.notice.is_none()
            && envelope.from != *self.ant
    }

    /// Tell the messages waiting on `thread` where they stand now that `started` left the queue, and `started` that
    /// its turn came if it was told it waits.
    fn queue_moved(&self, thread: &ThreadId, t: &mut Thread, started: Uuid) {
        if t.queued.remove(&started) {
            status::publish(self.client, self.ant, thread, StatusUpdate::Queued { message: started, ahead: 0 });
        }
        for (before, (incoming, _)) in t.waiting.iter().enumerate() {
            let message = incoming.envelope.id;
            t.queued.insert(message);
            status::publish(self.client, self.ant, thread, StatusUpdate::Queued { message, ahead: before + 1 });
        }
    }

    /// Answer a human's [`QueueRequest`], or an assistant's for its human, with `messages`, those in hand, and where the
    /// inbox and channel consumers stand: the messages they have not delivered yet wait in the stream. Each consumer is
    /// read within [`QUEUE_READ`], well inside the wait of `ants queue`, so an unread one leaves its place out of the
    /// answer rather than delay it.
    async fn answer_queue(&self, request: QueueRequest, messages: Vec<QueuedMessage>) {
        let assisted = request.on_behalf_of.as_ref();
        if assisted.is_some() && self.assisted(&request.by, assisted, "queue request").await.is_none() {
            return;
        }
        let place = async |name: String| {
            let read = async {
                let stream = self.js.get_stream_no_info(ants_proto::STREAM).await?;
                let info = stream.consumer_info(&name).await?;
                anyhow::Ok(ConsumerPlace {
                    acked: info.ack_floor.stream_sequence,
                    delivered: info.delivered.stream_sequence,
                    undelivered: info.num_pending,
                    ack_pending: u64::try_from(info.num_ack_pending).unwrap_or(u64::MAX),
                })
            };
            tokio::time::timeout(QUEUE_READ, read)
                .await
                .unwrap_or_else(|_| Err(anyhow::anyhow!("no answer within {QUEUE_READ:?}")))
                .with_context(|| format!("reading {name}"))
        };
        let (inbox, posts) =
            futures::join!(place(ants_proto::inbox_consumer(self.ant)), place(ants_proto::chan_consumer(self.ant)));
        let inbox = inbox.inspect_err(|e| {
            tracing::warn!(error = format!("{e:#}"), "reading the inbox consumer for ants queue");
        });
        // Missing while the daemon serves no channel posts.
        let posts = posts.inspect_err(|e| {
            tracing::debug!(error = format!("{e:#}"), "reading the channel consumer for ants queue");
        });
        let answer = request.answer(messages, inbox.ok(), posts.ok());
        let payload = serde_json::to_vec(&answer).expect("a queue answer serializes");
        if let Err(e) = self.client.publish(answer.subject(), payload.into()).await {
            tracing::warn!(error = %e, "answering a queue request");
        }
    }
}

/// Tell the server the messages waiting in hand are still being worked on, so it does not redeliver them.
async fn ack_progress(threads: &HashMap<ThreadId, Thread>) {
    for (_, msg) in threads.values().flat_map(|t| &t.waiting) {
        bus::progress(msg).await;
    }
}

/// Stop waiting for the failed messages not redelivered by their deadline.
fn give_up_retries(threads: &mut HashMap<ThreadId, Thread>) {
    let now = tokio::time::Instant::now();
    for (thread, t) in threads {
        if t.retry.as_ref().is_some_and(|r| r.until <= now) {
            tracing::warn!(%thread, "failed message not redelivered in time; going on with the thread");
            t.retry = None;
        }
    }
}

/// Note how handling the message in flight on `thread`, or the `joiner` of its prompt, ended. Returns what it waits
/// for, first on its thread, if it was handed back.
fn handed_back(
    threads: &mut HashMap<ThreadId, Thread>,
    thread: &ThreadId,
    handled: Handled,
    joiner: Option<Uuid>,
    stall: &super::holds::Stall,
) -> Option<Wait> {
    stall.settled(&handled);
    let t = threads.entry(thread.clone()).or_default();
    match joiner {
        Some(id) => t.joining.retain(|joiner| joiner.id != id),
        None => {
            t.busy = false;
            t.in_flight = None;
            t.running = None;
            t.retry = None;
        }
    }
    match handled {
        Handled::Done => None,
        Handled::Retry(retry) => {
            t.retry.get_or_insert(retry);
            None
        }
        Handled::Requeued(held) => {
            let (incoming, msg) = *held;
            let at = t.steered_at(stream_seq(&msg));
            t.waiting.insert(at, (Incoming { steered: Steered::Ahead, ..incoming }, msg));
            None
        }
        Handled::SignedOut(held) => {
            t.waiting.push_front(*held);
            Some(Wait::SignIn)
        }
        Handled::Limited(held) => {
            t.waiting.push_front(*held);
            Some(Wait::Limit)
        }
        Handled::Broken(held) => {
            t.waiting.push_front(*held);
            Some(Wait::Repair)
        }
        Handled::Failover(held) => {
            t.waiting.push_front(*held);
            None
        }
        Handled::Stalled(held) => {
            t.waiting.push_front(*held);
            Some(Wait::Hub)
        }
    }
}

/// The thread's next message to prompt, unless one is in flight, being answered by the prompt it joined, or waiting
/// for a redelivery; a message joining the prompt in flight starts at once. Forgets a thread with nothing left in hand.
fn next_turn(threads: &mut HashMap<ThreadId, Thread>, thread: ThreadId) -> Option<InHand> {
    let Entry::Occupied(mut entry) = threads.entry(thread) else { return None };
    let t = entry.get_mut();
    if t.retry.is_some() {
        return None;
    }
    let joins = t.waiting.front().is_some_and(|(waiting, _)| waiting.steered == Steered::Joining);
    if t.busy && joins {
        let (incoming, msg) = t.waiting.pop_front()?;
        let queued = queued_message(&incoming, &msg, Place::Running);
        t.joining.push(Joiner { id: incoming.envelope.id, seq: stream_seq(&msg), queued });
        return Some((incoming, msg));
    }
    if t.busy || !t.joining.is_empty() {
        return None;
    }
    let next = t.waiting.pop_front();
    match next {
        Some((incoming, msg)) => {
            // The prompt it was to join ended before it started.
            let steered = if incoming.steered == Steered::Joining { Steered::Ahead } else { incoming.steered };
            let incoming = Incoming { steered, ..incoming };
            t.busy = true;
            t.in_flight = stream_seq(&msg);
            t.running = queued_message(&incoming, &msg, Place::Running);
            Some((incoming, msg))
        }
        None => {
            entry.remove();
            None
        }
    }
}

/// The messages in hand on every thread, as `ants queue` lists them, by stream sequence.
fn in_hand(threads: &HashMap<ThreadId, Thread>) -> Vec<QueuedMessage> {
    let mut messages: Vec<_> = threads
        .values()
        .flat_map(|t| {
            let before = usize::from(t.busy) + usize::from(t.retry.is_some()) + t.joining.len();
            let waiting = t.waiting.iter().enumerate();
            let waiting = waiting.filter_map(move |(at, (incoming, msg))| {
                queued_message(incoming, msg, Place::Waiting { ahead: before + at })
            });
            let retry = t.retry.as_ref().and_then(|r| r.queued.clone());
            let retry = retry.map(|queued| QueuedMessage { ahead: usize::from(t.busy), ..queued });
            let joining = t.joining.iter().filter_map(|joiner| joiner.queued.clone());
            t.running.clone().into_iter().chain(joining).chain(retry).chain(waiting)
        })
        .collect();
    messages.sort_by_key(|m| m.seq);
    messages
}

pub(super) fn queued_message(incoming: &Incoming, msg: &jetstream::Message, place: Place) -> Option<QueuedMessage> {
    let envelope = &incoming.envelope;
    let (running, ahead) = match place {
        Place::Running => (true, 0),
        Place::Waiting { ahead } => (false, ahead),
        Place::Retry => (false, 0),
    };
    Some(QueuedMessage {
        id: envelope.id,
        thread_id: envelope.thread_id.clone(),
        from: envelope.from.clone(),
        channel: incoming.post.as_ref().map(|posted| posted.post.channel.clone()),
        sent_at: envelope.sent_at,
        seq: stream_seq(msg)?,
        running,
        ahead,
        redelivery: matches!(place, Place::Retry),
    })
}

/// The queue request in `msg`, unless it is malformed or names another ant than its subject.
fn queue_request(msg: &async_nats::Message) -> Option<QueueRequest> {
    let request: QueueRequest = match serde_json::from_slice(&msg.payload) {
        Ok(request) => request,
        Err(e) => {
            tracing::warn!(subject = %msg.subject, error = %e, "ignoring a malformed queue request");
            return None;
        }
    };
    if let Err(e) = request.check_subject(&msg.subject) {
        tracing::warn!(error = %e, "ignoring a forged queue request");
        return None;
    }
    tracing::info!(by = %request.by, "listing the messages in hand");
    Some(request)
}

/// Hand the messages waiting on `threads` back at once rather than after the ack wait, oldest first.
async fn hand_back_waiting(threads: HashMap<ThreadId, Thread>) {
    let mut waiting: Vec<_> = threads.into_values().flat_map(|t| t.waiting).map(|(_, msg)| msg).collect();
    waiting.sort_by_key(stream_seq);
    for msg in &waiting {
        bus::nak(msg, None).await;
    }
}
