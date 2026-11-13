//! The inbox loop of one persona.
//!
//! Messages of one thread are handled one at a time, in order; different threads run concurrently, each in its own
//! harness from the pool.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Display;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ants_proto::{Envelope, Error as ProtoError, JsDomain, PersonaId, ThreadId, subject};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::stream::{FuturesUnordered, Stream, StreamExt as _};

use crate::approvals::NatsApprover;
use crate::attempts::{Answered, Attempts, Marker};
use crate::caps::{Caps, Ledger};
use crate::dead::{self, Attempt};
use crate::harness::{HarnessKind, Launch, ModelSettings, Prompt, PromptLimits, Spend};
use crate::mcp::Mcp;
use crate::permission::{Allowlist, PermissionPolicy};
use crate::pool::{Pool, SessionStore};
use crate::{bus, roster};

/// Period of the progress acks sent for every message in hand, from its arrival to its ack: waiting for its thread,
/// and prompting, which covers waiting for a harness, starting it and a tool request waiting for the human.
const PROGRESS_EVERY: Duration = Duration::from_secs(20);
/// Redelivery window: a message in hand of a daemon that crashed, or sent into a dead link, comes back this long after
/// its last progress ack.
const ACK_WAIT: Duration = ack_wait(PROGRESS_EVERY);
/// Attempts a message gets before it becomes a dead letter on `ants.dead.<persona>`.
const MAX_ATTEMPTS: i64 = 5;
/// The consumer allows one more delivery, to dead-letter a message whose last attempt never finished.
const MAX_DELIVER: i64 = MAX_ATTEMPTS + 1;
const RETRY_DELAY: Duration = Duration::from_secs(30);
/// How often idle harnesses are looked for.
const EVICT_EVERY: Duration = Duration::from_secs(30);
/// Inbox messages in hand per harness: prompting, or waiting for their thread or for a free harness.
const IN_HAND_PER_HARNESS: usize = 2;

/// Put before the text of a message whose earlier attempt started and stored no reply.
const CUT_OFF: &str = "[ants] An earlier attempt at this message was cut off before its reply was stored, so tools it \
ran may already have taken effect. The session history may show what ran. Check the current state before repeating \
any action.";

/// A few progress periods, so one late or lost progress ack does not redeliver a message still in hand.
const fn ack_wait(progress_every: Duration) -> Duration {
    progress_every.saturating_mul(3)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub persona: PersonaId,
    pub nats_url: String,
    /// Credentials file of the persona, when the server requires them.
    pub creds: Option<PathBuf>,
    /// JetStream domain of the hub, if it has one.
    pub js_domain: Option<JsDomain>,
    /// Directory the harness works in.
    pub workdir: PathBuf,
    /// The persona's own files (usually `~/.ants/<persona>`).
    pub home: PathBuf,
    /// Persona, memory and task instructions prepended to every prompt.
    pub instructions: String,
    /// The persona's role instructions, shared in the roster.
    pub whoami: Option<String>,
    pub env: BTreeMap<String, String>,
    pub harness: HarnessKind,
    pub model_settings: ModelSettings,
    /// Tool requests the harness may run without asking (see [`crate::permission`] for the rules). Every other one
    /// goes to the human on `ants.approvals.<persona>`.
    pub allow: Vec<String>,
    /// How long a tool request waits for the human before it is rejected.
    pub approval_timeout: Duration,
    /// Most harness processes alive at once; the least recently used idle one stops to make room.
    pub max_harnesses: usize,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// Capabilities the persona advertises in the roster.
    pub capabilities: Vec<String>,
    /// Limits on every prompt.
    pub limits: PromptLimits,
    /// Token and cost caps of each thread and of the persona.
    pub caps: Caps,
}

/// Serve the persona's inbox until Ctrl-C. Prompts in flight are finished first; a second Ctrl-C stops at once. Until
/// the NATS server can be reached the daemon keeps trying ([`bus::connect_retrying`]).
pub async fn run(config: Config) -> Result<()> {
    let allow = Allowlist::parse(&config.allow)?;
    let Some((client, js)) = bus::connect_retrying(
        &config.nats_url,
        config.creds.as_deref(),
        config.js_domain.as_ref(),
        &config.persona,
        &format!("antsd-{}", config.persona),
        async {
            if stop_signal().await.is_err() {
                std::future::pending::<()>().await;
            }
        },
    )
    .await?
    else {
        tracing::info!("stopped before connecting");
        return Ok(());
    };
    let approver = NatsApprover::new(client.clone(), js.clone(), config.persona.clone(), config.approval_timeout);
    let policy = PermissionPolicy::new(allow, Some(std::sync::Arc::new(approver)));
    let (outbox, roster) = (crate::mcp::jetstream_outbox(js.clone()), crate::mcp::jetstream_roster(js.clone()));
    let rooms = std::sync::Arc::new(crate::plan::JetStreamRooms::new(js.clone(), config.persona.clone()));
    let mut mcp = Mcp::start(config.persona.clone(), &config.home, outbox, roster, rooms).await?;
    mcp.listen(client.subscribe(subject::inbox_of(&config.persona)).await?);
    let launch = Launch::new(
        config.harness,
        config.workdir.clone(),
        config.env.clone().into_iter().collect(),
        policy,
        config.model_settings.clone(),
    )
    .with_instructions(config.instructions.clone())
    .with_mcp(mcp.servers());
    let stream = bus::ensure_stream(&js).await?;
    let sessions_kv = bus::ensure_sessions(&js).await?;
    let ledger = Ledger::kv(sessions_kv.clone(), config.persona.clone(), config.caps);
    let sessions = SessionStore::kv(sessions_kv, config.persona.clone());
    let attempts = Attempts::new(bus::ensure_attempts(&js).await?, config.persona.clone());
    let answered = Answered::new(&config.home);
    answered.prune(bus::RETENTION).await;
    let pool = Pool::new(launch, sessions, config.max_harnesses, config.idle_timeout);
    let in_hand = i64::try_from(config.max_harnesses.max(1) * IN_HAND_PER_HARNESS)?;
    let messages = bus::inbox_consumer(&stream, &config.persona, ACK_WAIT, MAX_DELIVER, in_hand).await?;
    let registration = roster::register(&js, &config).await?;
    tracing::info!(
        persona = %config.persona,
        harness = %config.harness,
        model_settings = ?config.model_settings,
        workdir = %config.workdir.display(),
        max_harnesses = config.max_harnesses,
        idle_timeout = ?config.idle_timeout,
        capabilities = ?config.capabilities,
        allow = ?config.allow,
        approval_timeout = ?config.approval_timeout,
        limits = ?config.limits,
        caps = ?config.caps,
        "serving inbox"
    );

    let inbox = Inbox {
        client: &client,
        js: &js,
        persona: &config.persona,
        pool: &pool,
        attempts: &attempts,
        answered: &answered,
        mcp: Some(&mcp),
        limits: config.limits,
        ledger: &ledger,
        retry_delay: RETRY_DELAY,
        progress_every: PROGRESS_EVERY,
    };
    let ctrl_c = || async {
        if let Err(e) = stop_signal().await {
            tracing::warn!(error = %e, "waiting for Ctrl-C or SIGTERM");
            std::future::pending::<()>().await;
        }
    };
    inbox.serve(messages, config.idle_timeout, ctrl_c).await;
    registration.deregister().await;
    pool.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

/// Ctrl-C, or SIGTERM, which launchd and systemd send to stop a service.
async fn stop_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

type InHand = (Envelope, jetstream::Message);

/// The messages of one thread in hand: one in flight at most, the rest waiting their turn.
#[derive(Default)]
struct Thread {
    waiting: VecDeque<InHand>,
    busy: bool,
    /// Stream sequence of the message in flight.
    in_flight: Option<u64>,
    /// A failed message that is to be redelivered. Until it is back (or the deadline passes) the rest of the thread
    /// waits rather than overtake it.
    retry: Option<Retry>,
}

#[derive(Clone, Copy)]
struct Retry {
    seq: u64,
    until: tokio::time::Instant,
}

struct Inbox<'a> {
    client: &'a async_nats::Client,
    js: &'a jetstream::Context,
    persona: &'a PersonaId,
    pool: &'a Pool,
    attempts: &'a Attempts,
    answered: &'a Answered,
    mcp: Option<&'a Mcp>,
    limits: PromptLimits,
    ledger: &'a Ledger,
    retry_delay: Duration,
    progress_every: Duration,
}

impl Inbox<'_> {
    /// Handle messages until they end or `stop` resolves, then finish the prompts in flight unless it resolves again.
    async fn serve<S, E, F>(&self, mut messages: S, idle_timeout: Duration, mut stop: impl FnMut() -> F)
    where
        S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
        E: Display,
        F: Future<Output = ()>,
    {
        let mut threads: HashMap<ThreadId, Thread> = HashMap::new();
        let mut running = FuturesUnordered::new();
        let mut evict = tokio::time::interval(EVICT_EVERY.min(idle_timeout).max(Duration::from_secs(1)));
        let mut progress = self.progress();
        let shutdown = stop();
        tokio::pin!(shutdown);
        loop {
            let retry_deadline = threads.values().filter_map(|t| t.retry.map(|r| r.until)).min();
            let mut ready = None;
            tokio::select! {
                () = &mut shutdown => break,
                _ = evict.tick() => {
                    self.pool.evict_idle(Instant::now());
                }
                _ = progress.tick() => {
                    for (_, msg) in threads.values().flat_map(|t| &t.waiting) {
                        if let Err(e) = msg.ack_with(AckKind::Progress).await {
                            tracing::warn!(error = %e, "progress ack");
                        }
                    }
                }
                () = sleep_until(retry_deadline), if retry_deadline.is_some() => {
                    let now = tokio::time::Instant::now();
                    for (thread, t) in &mut threads {
                        if t.retry.is_some_and(|r| r.until <= now) {
                            tracing::warn!(%thread, "failed message not redelivered in time; going on with the thread");
                            t.retry = None;
                        }
                    }
                    ready = Some(threads.keys().cloned().collect());
                }
                Some((thread, retry)) = running.next(), if !running.is_empty() => {
                    if let Some(t) = threads.get_mut(&thread) {
                        t.busy = false;
                        t.in_flight = None;
                        t.retry = retry;
                    }
                    ready = Some(vec![thread]);
                }
                next = messages.next() => match next {
                    None => break,
                    Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                    Some(Ok(msg)) => {
                        let envelope: Envelope = match serde_json::from_slice(&msg.payload) {
                            Ok(e) => e,
                            Err(e) => {
                                self.bury_unanswerable(&msg, &format!("not an envelope: {e}")).await;
                                continue;
                            }
                        };
                        // A forged sender is buried without its envelope, so a replay cannot pass it on as genuine.
                        if let Err(e) = envelope.check_subject(&msg.subject) {
                            self.bury_unanswerable(&msg, &format!("sender mismatch: {e}")).await;
                            continue;
                        }
                        if self.claim(&envelope, &msg).await {
                            continue;
                        }
                        let thread = envelope.thread_id.clone();
                        let t = threads.entry(thread.clone()).or_default();
                        if t.holds(stream_seq(&msg)) {
                            // The copy in hand gets the ack: the server tracks a message by its stream sequence.
                            tracing::debug!(%thread, "ignoring a redelivery of a message in hand");
                            continue;
                        }
                        if t.retry.is_some_and(|r| Some(r.seq) == stream_seq(&msg)) {
                            t.retry = None;
                            t.waiting.push_front((envelope, msg));
                        } else {
                            t.waiting.push_back((envelope, msg));
                        }
                        ready = Some(vec![thread]);
                    }
                }
            }
            for thread in ready.into_iter().flatten() {
                if let Some((envelope, msg)) = next_turn(&mut threads, thread) {
                    running.push(self.handle(envelope, msg));
                }
            }
        }

        drop(messages);
        // Hand waiting messages back at once rather than after the ack wait, oldest first.
        let mut waiting: Vec<_> = threads.into_values().flat_map(|t| t.waiting).map(|(_, msg)| msg).collect();
        waiting.sort_by_key(stream_seq);
        for msg in &waiting {
            nak(msg, None).await;
        }
        if !running.is_empty() {
            tracing::info!(prompts = running.len(), "finishing prompts in flight; Ctrl-C again stops now");
            let drain = async { while running.next().await.is_some() {} };
            tokio::select! {
                () = drain => {}
                () = stop() => tracing::warn!("stopping mid-prompt; those messages are redelivered"),
            }
        }
    }

    /// Whether `envelope` is a reply to an `ask_agent` call, now handed over and acked. It must not wait in its
    /// thread's queue: the prompt that asked is in flight on that thread.
    async fn claim(&self, envelope: &Envelope, msg: &jetstream::Message) -> bool {
        let Some(mcp) = self.mcp else { return false };
        match mcp.claim(envelope).await {
            Ok(false) => false,
            Ok(true) => {
                if let Err(e) = bus::double_ack(self.client, msg).await {
                    tracing::warn!(error = %e, "acking a reply to ask_agent");
                }
                true
            }
            Err(e) => {
                tracing::warn!(error = format!("{e:#}"), "keeping a reply to ask_agent");
                nak(msg, Some(self.retry_delay)).await;
                true
            }
        }
    }

    /// Dead-letter a message that is never to be prompted with, keeping only an excerpt of its payload.
    async fn bury_unanswerable(&self, msg: &jetstream::Message, error: &str) {
        tracing::warn!(error, subject = %msg.subject, "dead-lettering a message without answering it");
        if let Err(e) = dead::bury(self.js, self.persona, msg, None, error).await {
            // Never redelivered, dead letter or not.
            tracing::warn!(error = format!("{e:#}"), "dead-lettering");
            if let Err(e) = msg.ack_with(AckKind::Term).await {
                tracing::warn!(error = %e, "terminating the message");
            }
        }
    }

    /// Answer one message. A failed attempt is redelivered after a delay, and the last one becomes a dead letter.
    /// Returns its thread, and the redelivery to wait for when there is one.
    async fn handle(&self, envelope: Envelope, msg: jetstream::Message) -> (ThreadId, Option<Retry>) {
        let thread = envelope.thread_id.clone();
        let (delivered, seq) = match msg.info() {
            Ok(info) => (info.delivered, info.stream_sequence),
            Err(e) => {
                self.bury_unanswerable(&msg, &format!("no delivery info: {e}")).await;
                return (thread, None);
            }
        };
        if self.answered.has(&envelope).await {
            // Its ack failed, as while a leaf node's uplink is down; its reply is in the stream or on its way there.
            tracing::info!(%thread, "acking a message whose reply is stored already");
            match bus::double_ack(self.client, &msg).await {
                Ok(()) => {
                    self.attempts.forget(seq).await;
                    self.answered.forget(&envelope).await;
                }
                Err(e) => tracing::warn!(%thread, error = %e, "acking a message whose reply is stored"),
            }
            return (thread, None);
        }
        let attempt = Attempt::of(delivered, MAX_ATTEMPTS);
        let marker = self.attempts.get(seq, &envelope).await;
        if attempt == Attempt::Overdue && !matches!(marker, Ok(Some(Marker::Replied(_)))) {
            let error = format!("no outcome after {MAX_ATTEMPTS} attempts: the daemon stopped or the ack wait ran out");
            match dead::bury(self.js, self.persona, &msg, Some(&envelope), &error).await {
                Ok(_) => self.attempts.forget(seq).await,
                Err(e) => tracing::error!(%thread, error = format!("{e:#}"), "dead-lettering"),
            }
            return (thread, None);
        }
        let result = match marker {
            Ok(marker) => self.answer(&envelope, &msg, seq, marker).await,
            Err(e) => Err(e),
        };
        let Err(e) = result else { return (thread, None) };
        tracing::error!(%thread, error = format!("{e:#}"), "handling message");
        if let Err(e) = self.client.publish(subject::status(self.persona, &thread), "failed".into()).await {
            tracing::warn!(error = %e, "publishing status");
        }
        // A failed last attempt, or an overdue one that failed to send its recorded reply: no delivery follows.
        if attempt != Attempt::Early {
            match dead::bury(self.js, self.persona, &msg, Some(&envelope), &format!("{e:#}")).await {
                Ok(_) => {
                    self.attempts.forget(seq).await;
                    return (thread, None);
                }
                // The overdue delivery tries again.
                Err(bury) => tracing::warn!(error = format!("{bury:#}"), "dead-lettering"),
            }
        }
        nak(&msg, Some(self.retry_delay)).await;
        let retry = (delivered < MAX_DELIVER).then(|| Retry {
            seq,
            // Redelivery may lag behind the delay; past this the thread goes on without the message.
            until: tokio::time::Instant::now() + self.retry_delay * 3,
        });
        (thread, retry)
    }

    /// Answer the message at stream sequence `seq`, given what an earlier attempt at it left. Every attempt after one
    /// that started, whether it failed or was cut off, is told that tools may already have run; one whose reply was
    /// recorded is not prompted again.
    async fn answer(
        &self,
        envelope: &Envelope,
        msg: &jetstream::Message,
        seq: u64,
        marker: Option<Marker>,
    ) -> Result<()> {
        let thread = &envelope.thread_id;
        if let Err(ProtoError::BudgetExhausted(_)) = envelope.reply(self.persona.clone(), "") {
            // Out of turns: hand the thread to the human instead of spending another prompt.
            self.escalate(envelope, "Turn budget spent", seq).await?;
            return bus::double_ack(self.client, msg).await;
        }
        let cut_off = match marker {
            Some(Marker::Replied(reply)) => {
                tracing::info!(%thread, "sending the reply an earlier attempt recorded");
                return self.deliver(envelope, &reply, msg, seq).await;
            }
            Some(Marker::Started) => true,
            None => false,
        };
        if envelope.from.is_human()
            && let Err(e) = self.ledger.fresh_allowance(thread).await
        {
            tracing::warn!(%thread, error = format!("{e:#}"), "renewing the thread's allowance");
        }
        match self.ledger.reached(thread).await {
            Ok(Some(cap)) => {
                // Out of allowance: the same, until the human answers or the day ends.
                self.escalate(envelope, &cap, seq).await?;
                bus::double_ack(self.client, msg).await?;
                self.attempts.forget(seq).await;
                return Ok(());
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(%thread, error = format!("{e:#}"), "reading what the thread spent"),
        }
        if !cut_off {
            // Durable before the harness can run anything.
            self.attempts.start(seq, envelope).await?;
        }

        let status = subject::status(self.persona, thread);
        tracing::info!(%thread, from = %envelope.from, cut_off, "prompting");
        self.client.publish(status, "working".into()).await?;

        let _turn = self.mcp.map(|mcp| mcp.begin(envelope));
        let (spend_tx, mut spend) = tokio::sync::mpsc::unbounded_channel();
        let (stop_tx, stop) = tokio::sync::oneshot::channel();
        let mut stop_tx = Some(stop_tx);
        let mut reached = None;
        let text = if cut_off { warned(&envelope.text) } else { envelope.text.clone() };
        let prompt = Prompt { text, limits: self.limits, spend: Some(spend_tx), stop: Some(stop) };
        let prompt = self.pool.run(thread, prompt);
        tokio::pin!(prompt);
        let mut progress = self.progress();
        let reply = loop {
            tokio::select! {
                r = &mut prompt => break r?,
                Some(spent) = spend.recv() => {
                    if let Some(cap) = self.record(thread, &spent).await {
                        if let Some(stop) = stop_tx.take() {
                            let _ = stop.send(cap.clone());
                        }
                        reached = Some(cap);
                    }
                }
                _ = progress.tick() => {
                    if let Err(e) = msg.ack_with(AckKind::Progress).await {
                        tracing::warn!(error = %e, "progress ack");
                    }
                }
            }
        };
        // Spend reported as the prompt ended, such as its tokens.
        while let Ok(spent) = spend.try_recv() {
            if let Some(cap) = self.record(thread, &spent).await {
                reached.get_or_insert(cap);
            }
        }

        let cut = reply.cut.is_some();
        let limit = reply.cut.or(reached);
        let text = match &limit {
            None => reply.text,
            Some(limit) => {
                let notice = format!("[ants] {limit}{}", if cut { "; the prompt was cancelled." } else { "." });
                [reply.text, notice].into_iter().filter(|t| !t.is_empty()).collect::<Vec<_>>().join("\n\n")
            }
        };
        let reply = envelope.reply(self.persona.clone(), text)?;
        // Before the reply is recorded, so a redelivery that only resends the reply still escalates; its message id
        // keeps a second escalation out of the stream.
        if let Some(limit) = limit
            && !envelope.from.is_human()
        {
            self.escalate(envelope, &limit, seq).await?;
        }
        // From here on a redelivery sends this reply rather than prompting again. Out of the hub's reach, as while a
        // leaf node's uplink is down, the reply still goes to the machine's outbox, and `answered` keeps it from being
        // prompted again.
        if let Err(e) = self.attempts.replied(seq, envelope, &reply).await {
            tracing::warn!(%thread, error = format!("{e:#}"), "recording the reply");
        }
        self.deliver(envelope, &reply, msg, seq).await
    }

    /// Store `reply` to `request`, the message at stream sequence `seq`, in the hub's stream or, out of its reach, in
    /// the machine's outbox, then ack the message and forget its marker. The message id, from the request's, makes
    /// JetStream drop a copy an earlier attempt stored within the stream's duplicate window. When the ack fails, the
    /// note in `answered` has the redelivery acked without answering again, even by a restarted daemon.
    async fn deliver(&self, request: &Envelope, reply: &Envelope, msg: &jetstream::Message, seq: u64) -> Result<()> {
        // Store the reply before acking: a crash in between redelivers rather than loses the message.
        bus::store(self.js, reply.subject(), reply, &reply_id(self.persona, request)).await?;
        let noted = self
            .answered
            .add(request, reply)
            .await
            .inspect_err(|e| tracing::warn!(error = format!("{e:#}"), "noting the stored reply"));
        match bus::double_ack(self.client, msg).await {
            Ok(()) => {
                self.attempts.forget(seq).await;
                self.answered.forget(request).await;
            }
            Err(e) if noted.is_ok() => {
                tracing::warn!(thread = %reply.thread_id, error = %e, "acking; the redelivery is acked unanswered");
            }
            Err(e) => return Err(e),
        }
        self.client.publish(subject::status(self.persona, &reply.thread_id), "idle".into()).await?;
        Ok(())
    }

    /// Count what the harness spent on the thread. Returns the cap reached, if any.
    async fn record(&self, thread: &ThreadId, spent: &Spend) -> Option<String> {
        self.ledger.record(thread, spent).await.unwrap_or_else(|e| {
            tracing::warn!(%thread, error = format!("{e:#}"), "storing what the thread spent");
            None
        })
    }

    /// Hand the thread to the human: `why` it needs a decision, and the message it was on.
    /// Its message id, from the stream sequence `seq` of the message, keeps a redelivery from escalating twice.
    async fn escalate(&self, envelope: &Envelope, why: &str, seq: u64) -> Result<()> {
        tracing::info!(thread = %envelope.thread_id, why, "escalating to the human");
        let mut escalation = Envelope::on_thread(
            envelope.thread_id.clone(),
            self.persona.clone(),
            PersonaId::human(),
            format!("{why}; needs a decision. Last message from {}:\n{}", envelope.from, envelope.text),
        );
        escalation.in_reply_to = Some(envelope.id);
        let id = format!("escalation-{}-{seq}", self.persona);
        bus::store(self.js, escalation.subject(), &escalation, &id).await?;
        Ok(())
    }

    fn progress(&self) -> tokio::time::Interval {
        tokio::time::interval_at(tokio::time::Instant::now() + self.progress_every, self.progress_every)
    }
}

impl Thread {
    /// Whether the message with stream sequence `seq` is in flight or waiting. The server redelivers one whose progress
    /// acks were lost, as on a link that went silent for longer than the ack wait.
    fn holds(&self, seq: Option<u64>) -> bool {
        seq.is_some() && (self.in_flight == seq || self.waiting.iter().any(|(_, msg)| stream_seq(msg) == seq))
    }
}

/// The thread's next message to prompt, unless one is in flight or it waits for a redelivery. Forgets a thread with
/// nothing left in hand.
fn next_turn(threads: &mut HashMap<ThreadId, Thread>, thread: ThreadId) -> Option<InHand> {
    let Entry::Occupied(mut entry) = threads.entry(thread) else { return None };
    let t = entry.get_mut();
    if t.busy || t.retry.is_some() {
        return None;
    }
    let next = t.waiting.pop_front();
    match &next {
        Some((_, msg)) => {
            t.busy = true;
            t.in_flight = stream_seq(msg);
        }
        None => {
            entry.remove();
        }
    }
    next
}

/// The prompt for a message whose earlier attempt was cut off.
fn warned(text: &str) -> String {
    format!("{CUT_OFF}\n\n{text}")
}

/// The message id of `persona`'s reply to `request`: the same for every attempt at it.
fn reply_id(persona: &PersonaId, request: &Envelope) -> String {
    format!("reply-{persona}-{}", request.id)
}

fn stream_seq(msg: &jetstream::Message) -> Option<u64> {
    msg.info().ok().map(|info| info.stream_sequence)
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn nak(msg: &jetstream::Message, delay: Option<Duration>) {
    if let Err(e) = msg.ack_with(AckKind::Nak(delay)).await {
        tracing::warn!(error = %e, "nak");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ants_test_support::{NatsServer as Server, free_port};
    use async_nats::jetstream::consumer::PullConsumer;
    use tokio::sync::Semaphore;
    use tokio::time::timeout;

    use super::*;
    use crate::pool::fake;

    const WAIT: Duration = Duration::from_secs(10);
    const IDLE: Duration = Duration::from_secs(60);

    /// dev-a's side of a server: it serves its inbox, and the human sends to it.
    struct Bus {
        _server: Server,
        client: async_nats::Client,
        js: jetstream::Context,
        stream: jetstream::stream::Stream,
        attempts: Attempts,
        answered: Answered,
        markers: jetstream::kv::Store,
        dev: PersonaId,
        limits: PromptLimits,
        ledger: Ledger,
    }

    impl Bus {
        /// The bus, and the replies to the human.
        async fn start() -> Option<(Self, async_nats::Subscriber)> {
            let server = Server::start()?;
            let url = server.url.clone();
            Some(Self::connect(server, &url, None).await)
        }

        /// dev-a's side of `server`, connected through `url`, with the hub's JetStream in `js_domain`.
        async fn connect(server: Server, url: &str, js_domain: Option<&JsDomain>) -> (Self, async_nats::Subscriber) {
            let dev: PersonaId = "dev-a".parse().unwrap();
            let (client, js) = bus::connect(url, None, js_domain, &dev, "test").await.unwrap();
            let stream = bus::ensure_stream(&js).await.unwrap();
            let markers = bus::ensure_attempts(&js).await.unwrap();
            let attempts = Attempts::new(markers.clone(), dev.clone());
            let answered = Answered::new(&server.dir().join("dev-a"));
            let replies = client.subscribe(subject::inbox_of(&PersonaId::human())).await.unwrap();
            client.flush().await.unwrap();
            let ledger = Ledger::memory(dev.clone(), Caps::default());
            let limits = PromptLimits::default();
            let bus = Self { _server: server, client, js, stream, attempts, answered, markers, dev, limits, ledger };
            (bus, replies)
        }

        /// Send `text` from the human, returning the envelope and its stream sequence.
        async fn send(&self, thread: &ThreadId, text: &str) -> (Envelope, u64) {
            self.send_from(PersonaId::human(), thread, text).await
        }

        async fn send_from(&self, from: PersonaId, thread: &ThreadId, text: &str) -> (Envelope, u64) {
            let envelope = Envelope::on_thread(thread.clone(), from, self.dev.clone(), text);
            let seq = bus::publish_json(&self.js, envelope.subject(), &envelope, None).await.unwrap();
            (envelope, seq)
        }

        async fn marked(&self, seq: u64) -> bool {
            self.markers.get(ants_proto::attempt_key(&self.dev, seq)).await.unwrap().is_some()
        }

        /// Serve the inbox until a permit is added to `ctrl_c` (twice to stop mid-prompt).
        async fn serve(&self, pool: &Pool, ctrl_c: &Arc<Semaphore>) {
            self.serve_timed(pool, ctrl_c, PROGRESS_EVERY, ACK_WAIT).await;
        }

        async fn serve_timed(
            &self,
            pool: &Pool,
            ctrl_c: &Arc<Semaphore>,
            progress_every: Duration,
            ack_wait: Duration,
        ) {
            let messages = bus::inbox_consumer(&self.stream, &self.dev, ack_wait, MAX_DELIVER, 4).await.unwrap();
            let inbox = Inbox {
                client: &self.client,
                js: &self.js,
                persona: &self.dev,
                pool,
                attempts: &self.attempts,
                answered: &self.answered,
                mcp: None,
                limits: self.limits,
                ledger: &self.ledger,
                retry_delay: Duration::from_millis(300),
                progress_every,
            };
            let stop = || {
                let ctrl_c = ctrl_c.clone();
                async move { ctrl_c.acquire().await.unwrap().forget() }
            };
            inbox.serve(messages, IDLE, stop).await;
        }

        async fn until_acked(&self, seq: u64) {
            timeout(WAIT, async {
                let name = ants_proto::inbox_consumer(&self.dev);
                loop {
                    // The consumer may not be there yet.
                    if let Ok(mut consumer) = self.stream.get_consumer::<jetstream::consumer::pull::Config>(&name).await
                        && consumer.info().await.unwrap().ack_floor.stream_sequence >= seq
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("no ack");
        }

        async fn unacked(&self) -> usize {
            self.consumer().await.num_ack_pending
        }

        async fn consumer(&self) -> jetstream::consumer::Info {
            let mut consumer: PullConsumer =
                self.stream.get_consumer(&ants_proto::inbox_consumer(&self.dev)).await.unwrap();
            consumer.info().await.unwrap().clone()
        }
    }

    async fn next_reply(replies: &mut async_nats::Subscriber) -> (ThreadId, String) {
        let msg = timeout(WAIT, replies.next()).await.expect("no reply").unwrap();
        let envelope: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        let text = envelope.text.split_once(": ").expect("fake reply").1.to_owned();
        (envelope.thread_id, text)
    }

    async fn next_envelope(subscriber: &mut async_nats::Subscriber) -> Envelope {
        let msg = timeout(WAIT, subscriber.next()).await.expect("no message").unwrap();
        serde_json::from_slice(&msg.payload).unwrap()
    }

    async fn until(what: &str, done: impl Fn() -> bool) {
        timeout(WAIT, async {
            while !done() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    #[tokio::test]
    async fn threads_run_concurrently_and_each_stays_in_order() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, gate) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;
        bus.send(&t2, "c").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t2.clone(), "c".to_owned()), "t2 does not wait for t1");
            assert_eq!(log.lock().unwrap().answered, ["c"], "b waits for the message before it on t1");
            gate.add_permits(1);
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_failed_message_is_retried_before_the_rest_of_its_thread() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "flaky a").await;
        bus.send(&t1, "b").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            // The failed attempt may have run tools too, so the retry carries the warning.
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("flaky a")));
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, [warned("flaky a"), "b".to_owned()]);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_forged_sender_is_dead_lettered_unanswered() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let thread = ThreadId::generate();
        // dev-b sends on its own subject, but the envelope claims the human sent it.
        let forged = Envelope::on_thread(thread.clone(), PersonaId::human(), bus.dev.clone(), "forged");
        let dev_b: PersonaId = "dev-b".parse().unwrap();
        crate::bus::publish_json(&bus.js, subject::inbox(&bus.dev, &dev_b), &forged, None).await.unwrap();
        bus.send(&thread, "genuine").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (thread.clone(), "genuine".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["genuine"]);
        let letters = dead::list(&bus.stream).await.unwrap();
        let [(_, letter)] = letters.as_slice() else { panic!("{letters:?}") };
        assert!(letter.error.contains("sender mismatch"), "{}", letter.error);
        assert_eq!(letter.envelope, None, "a forged envelope is not replayable");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn inbox_consumers_from_before_sender_subjects_get_the_new_filter() {
        let Some((bus, _)) = Bus::start().await else { return };
        let human = PersonaId::human();
        for persona in [&bus.dev, &human] {
            let name = ants_proto::inbox_consumer(persona);
            let old = jetstream::consumer::pull::Config {
                durable_name: Some(name.clone()),
                filter_subject: format!("ants.inbox.{persona}"),
                ..Default::default()
            };
            bus.stream.create_consumer(old).await.unwrap();
        }
        drop(crate::bus::inbox_consumer(&bus.stream, &bus.dev, ACK_WAIT, MAX_DELIVER, 4).await.unwrap());
        crate::bus::human_inbox_consumer(&bus.stream, ACK_WAIT).await.unwrap();
        for persona in [&bus.dev, &human] {
            let mut consumer: PullConsumer =
                bus.stream.get_consumer(&ants_proto::inbox_consumer(persona)).await.unwrap();
            assert_eq!(consumer.info().await.unwrap().config.filter_subject, subject::inbox_of(persona));
        }
    }

    #[tokio::test]
    async fn ctrl_c_finishes_prompts_in_flight_and_a_restart_resumes() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, gate) = fake::launch(true);
        let store = SessionStore::memory();
        let t1 = ThreadId::generate();
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;

        let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let stop_mid_prompt = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            ctrl_c.add_permits(1);
            gate.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), stop_mid_prompt);
        pool.shutdown().await;
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
        assert_eq!(log.lock().unwrap().answered, ["hold a"], "b was handed back, not prompted");

        let pool = Pool::new(launch, store, 2, IDLE);
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().loaded, ["s1"], "the restarted daemon resumed the thread's session");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn progress_acks_keep_messages_in_hand_past_the_ack_wait() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));
        // A consumer left by an older daemon with a longer ack wait takes the new one.
        drop(bus::inbox_consumer(&bus.stream, &bus.dev, ack_wait * 100, MAX_DELIVER, 4).await.unwrap());
        let (launch, log, gate) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            assert_eq!(bus.consumer().await.config.ack_wait, ack_wait);
            tokio::time::sleep(ack_wait * 4).await;
            let consumer = bus.consumer().await;
            assert_eq!((consumer.num_redelivered, consumer.num_ack_pending), (0, 2), "neither message came back");
            gate.add_permits(1);
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_redelivery_of_a_message_in_hand_is_not_prompted_again() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, gate) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "hold a").await;
        bus.send(&t1, "b").await;

        // Progress acks lost, as on a silent link: both messages come back while still in hand.
        let ack_wait = Duration::from_millis(300);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            timeout(WAIT, async {
                while bus.consumer().await.num_redelivered < 2 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("no redelivery");
            gate.add_permits(10);
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "hold a".to_owned()));
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "b".to_owned()));
            assert!(timeout(ack_wait * 3, replies.next()).await.is_err(), "no duplicate reply");
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, Duration::from_secs(3600), ack_wait), check);
        assert_eq!(log.lock().unwrap().answered, ["hold a", "b"]);
        assert_eq!(bus.unacked().await, 0, "acks of the first delivery count");
    }

    #[tokio::test]
    async fn a_prompt_past_its_limit_is_cancelled_and_the_human_told() {
        let Some((mut bus, mut replies)) = Bus::start().await else { return };
        bus.limits.timeout = Some(Duration::from_millis(100));
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        bus.send(&t1, "stall").await;
        bus.send(&t1, "after").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            let notice = next_envelope(&mut replies).await;
            let text = "partial\n\n[ants] Prompt time limit reached (100ms); the prompt was cancelled.";
            assert_eq!(notice.text, text);
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "after".to_owned()), "and nothing else");
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().cancelled, 1);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_first_delivery_is_prompted_as_sent_and_leaves_no_marker() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        let (_, seq) = bus.send(&t1, "a").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), "a".to_owned()));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().answered, ["a"]);
        assert!(!bus.marked(seq).await, "the marker is removed with the ack");
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_spent_cap_hands_the_thread_to_the_human_until_the_human_answers() {
        let Some((mut bus, mut to_human)) = Bus::start().await else { return };
        bus.ledger = Ledger::memory(bus.dev.clone(), Caps { thread_cost: Some(1.0), ..Caps::default() });
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let dev_b: PersonaId = "dev-b".parse().unwrap();
        let mut to_dev_b = bus.client.subscribe(subject::inbox_of(&dev_b)).await.unwrap();
        bus.client.flush().await.unwrap();
        let t1 = ThreadId::generate();
        bus.send_from(dev_b.clone(), &t1, "cost 2 stall").await;
        bus.send_from(dev_b.clone(), &t1, "more").await;
        bus.send(&t1, "go on").await;

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            let cap = "Thread cost cap reached (2.00 of 1.00 since the human's last message)";
            let reply = next_envelope(&mut to_dev_b).await;
            assert_eq!(reply.text, format!("partial\n\n[ants] {cap}; the prompt was cancelled."));
            let escalation = next_envelope(&mut to_human).await;
            assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\ncost 2 stall"));
            let escalation = next_envelope(&mut to_human).await;
            assert_eq!(escalation.text, format!("{cap}; needs a decision. Last message from dev-b:\nmore"));
            assert_eq!(next_reply(&mut to_human).await, (t1.clone(), "go on".to_owned()), "a fresh allowance");
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(bus.unacked().await, 0);
        let log = log.lock().unwrap();
        assert_eq!((log.cancelled, log.answered.as_slice()), (1, &["go on".to_owned()][..]));
    }

    #[tokio::test]
    async fn a_message_cut_off_mid_prompt_is_prompted_again_with_a_warning() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (progress_every, ack_wait) = (Duration::from_millis(100), ack_wait(Duration::from_millis(100)));
        let (launch, log, _) = fake::launch(true);
        let store = SessionStore::memory();
        let t1 = ThreadId::generate();
        let (_, seq) = bus.send(&t1, "hold a").await;

        // Stopped mid-prompt, as by a crash: the harness may have run tools, and the message was not acked.
        let pool = Pool::new(launch.clone(), store.clone(), 2, IDLE);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let crash = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            ctrl_c.add_permits(2);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), crash);
        // Its harness is still held mid-prompt, so it is dropped rather than shut down.
        drop(pool);
        assert!(bus.marked(seq).await, "the attempt is marked as started");
        assert!(log.lock().unwrap().answered.is_empty());

        let pool = Pool::new(launch, store, 2, IDLE);
        let check = async {
            assert_eq!(next_reply(&mut replies).await, (t1.clone(), warned("hold a")));
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        {
            let log = log.lock().unwrap();
            assert_eq!(log.answered, [warned("hold a")]);
            assert_eq!(log.loaded, ["s1"], "the session resumes, with the history of the cut-off attempt");
        }
        assert!(!bus.marked(seq).await);
        assert_eq!(bus.unacked().await, 0);
    }

    #[tokio::test]
    async fn a_reply_recorded_before_a_crash_is_stored_once_without_a_prompt() {
        let Some((bus, mut replies)) = Bus::start().await else { return };
        let (launch, log, _) = fake::launch(true);
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let t1 = ThreadId::generate();
        let (ask, seq) = bus.send(&t1, "a").await;

        // The crashed attempt recorded its reply and stored it, but never acked.
        let reply = ask.reply(bus.dev.clone(), "s1: done before").unwrap();
        bus.attempts.start(seq, &ask).await.unwrap();
        bus.attempts.replied(seq, &ask, &reply).await.unwrap();
        bus::publish_json(&bus.js, reply.subject(), &reply, Some(&reply_id(&bus.dev, &ask))).await.unwrap();
        assert_eq!(next_reply(&mut replies).await, (t1.clone(), "done before".to_owned()));

        let ctrl_c = Arc::new(Semaphore::new(0));
        let check = async {
            bus.until_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve(&pool, &ctrl_c), check);
        assert_eq!(log.lock().unwrap().starts, 0, "not prompted again");
        let state = bus.stream.get_info().await.unwrap().state;
        assert_eq!(state.messages, 2, "the message and one copy of its reply");
        assert!(!bus.marked(seq).await);
        assert_eq!(bus.unacked().await, 0);
    }

    /// dev-a's daemon on a leaf node replies while the leaf's uplink is down: the reply goes to the machine's outbox
    /// and the ack fails. Once the uplink is back, the redelivery to a restarted daemon is acked, not answered again.
    #[tokio::test]
    async fn a_reply_kept_in_the_outbox_answers_its_message_once() {
        let (uplink_port, dead_port) = (free_port(), free_port());
        let Some(hub) = Server::spawn_with(|dir| {
            format!(
                "jetstream {{ store_dir: {:?}, domain: hub }}\nleafnodes {{ listen: \"127.0.0.1:{uplink_port}\" }}\n",
                dir.join("js")
            )
        }) else {
            return;
        };
        let uplink = |dir: &std::path::Path, port: u16| {
            let remote = format!("leafnodes {{ remotes: [{{ url: \"nats-leaf://127.0.0.1:{port}\" }}] }}\n");
            std::fs::write(dir.join("uplink.conf"), remote).unwrap();
        };
        let leaf = Server::spawn_with(|dir| {
            uplink(dir, uplink_port);
            format!("jetstream {{ store_dir: {:?}, domain: m1 }}\ninclude \"uplink.conf\"\n", dir.join("js"))
        })
        .unwrap();
        let switch_uplink = |port: u16| {
            uplink(leaf.dir(), port);
            let reload = std::process::Command::new("kill").arg("-HUP").arg(leaf.pid().to_string()).status();
            assert!(reload.unwrap().success());
        };
        let human = PersonaId::human();
        let (_h, h_js) = bus::connect(&hub.url, None, None, &human, "test").await.unwrap();
        bus::ensure_stream(&h_js).await.unwrap();
        let (local, local_js) = bus::connect(&leaf.url, None, None, &human, "test").await.unwrap();
        let mut outbox = local_js
            .create_stream(jetstream::stream::Config {
                name: ants_proto::OUTBOX_STREAM.to_owned(),
                subjects: vec![ants_proto::OUTBOX_SUBJECTS.to_owned()],
                ..Default::default()
            })
            .await
            .unwrap();
        let hub_domain: JsDomain = "hub".parse().unwrap();
        let mut through_leaf = bus::jetstream(local, Some(&hub_domain));
        through_leaf.set_timeout(Duration::from_secs(1));
        let uplink_is = async |up: bool| {
            timeout(WAIT * 2, async {
                while bus::ensure_stream(&through_leaf).await.is_ok() != up {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the uplink is not {}", if up { "up" } else { "down" }));
        };
        uplink_is(true).await;
        let leaf_url = leaf.url.clone();
        let (bus, _) = Bus::connect(hub, &leaf_url, Some(&hub_domain)).await;
        let (launch, log, gate) = fake::launch(true);
        let t1 = ThreadId::generate();
        let ask = Envelope::on_thread(t1.clone(), human.clone(), bus.dev.clone(), "hold a");
        let seq = bus::publish_json(&h_js, ask.subject(), &ask, None).await.unwrap();
        let (progress_every, ack_wait) = (Duration::from_millis(200), ack_wait(Duration::from_millis(200)));

        let pool = Pool::new(launch.clone(), SessionStore::memory(), 2, IDLE);
        let ctrl_c = Arc::new(Semaphore::new(0));
        let down = async {
            until("the prompt to start", || log.lock().unwrap().starts == 1).await;
            switch_uplink(dead_port);
            uplink_is(false).await;
            gate.add_permits(1);
            until("the answer", || log.lock().unwrap().answered.len() == 1).await;
            // Stopped, as for a restart, after the prompt in flight is handled.
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), down);
        drop(pool);
        assert_eq!(outbox.info().await.unwrap().state.messages, 1, "the reply is kept in the outbox");

        switch_uplink(uplink_port);
        uplink_is(true).await;
        let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
        let check = async {
            bus.until_acked(seq).await;
            ctrl_c.add_permits(1);
        };
        tokio::join!(bus.serve_timed(&pool, &ctrl_c, progress_every, ack_wait), check);
        assert_eq!(log.lock().unwrap().answered, ["hold a"], "answered once");
        assert_eq!(
            h_js.get_stream(ants_proto::STREAM).await.unwrap().info().await.unwrap().state.messages,
            1,
            "no second reply"
        );
        assert_eq!(bus.unacked().await, 0);
        assert!(!bus.answered.has(&ask).await, "forgotten once acked");
    }
}
