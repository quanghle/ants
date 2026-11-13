//! The inbox loop of one persona.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use ants_proto::{Envelope, Error as ProtoError, PersonaId, subject};
use anyhow::Result;
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt as _;

use crate::harness::{HarnessKind, Launch, PermissionPolicy};
use crate::pool::Pool;
use crate::{bus, roster, workspace};

/// Redelivery window. The daemon extends it with progress acks while a prompt runs, so a crash mid-task redelivers.
const ACK_WAIT: Duration = Duration::from_secs(5 * 60);
const PROGRESS_EVERY: Duration = Duration::from_secs(60);
/// Deliveries before a message is given up on.
const MAX_DELIVER: i64 = 5;
const RETRY_DELAY: Duration = Duration::from_secs(30);
/// How often idle harnesses are looked for.
const EVICT_EVERY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct Config {
    pub persona: PersonaId,
    pub nats_url: String,
    /// Credentials file of the persona, when the server requires them.
    pub creds: Option<PathBuf>,
    /// Directory the harness works in.
    pub workdir: PathBuf,
    /// The persona's own files, its bd workspace among them (usually `~/.ants/<persona>`).
    pub home: PathBuf,
    pub harness: HarnessKind,
    pub permissions: PermissionPolicy,
    /// Most harness processes alive at once; the least recently used stops to make room.
    pub max_harnesses: usize,
    /// A thread's harness stops after this long without a message, and resumes its session on the next one.
    pub idle_timeout: Duration,
    /// Tool allowlist for the harness's permission requests.
    pub allow: Vec<String>,
    /// Capabilities the persona advertises in the roster.
    pub capabilities: Vec<String>,
}

/// Serve the persona's inbox until Ctrl-C. A message in hand is finished first.
pub async fn run(config: Config) -> Result<()> {
    let db = workspace::ensure(&config.home, &config.persona).await?;
    let launch = Launch::new(config.harness, config.workdir.clone(), workspace::harness_env(&db)?, config.permissions);
    let mut pool = Pool::new(launch, config.max_harnesses, config.idle_timeout);
    let (client, js) =
        bus::connect(&config.nats_url, config.creds.as_deref(), &config.persona, &format!("antsd-{}", config.persona))
            .await?;
    let stream = bus::ensure_stream(&js).await?;
    let mut messages = bus::inbox_consumer(&stream, &config.persona, ACK_WAIT, MAX_DELIVER).await?;
    let registration = roster::register(&js, &config).await?;
    tracing::info!(
        persona = %config.persona,
        harness = %config.harness,
        workdir = %config.workdir.display(),
        workspace = %db.display(),
        max_harnesses = config.max_harnesses,
        idle_timeout = ?config.idle_timeout,
        capabilities = ?config.capabilities,
        "serving inbox"
    );

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut evict = tokio::time::interval(EVICT_EVERY.min(config.idle_timeout).max(Duration::from_secs(1)));
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = evict.tick() => {
                pool.evict_idle(Instant::now());
            }
            next = messages.next() => match next {
                None => break,
                Some(Err(e)) => tracing::warn!(error = %e, "inbox stream"),
                Some(Ok(msg)) => {
                    if let Err(e) = handle(&client, &js, &config, &mut pool, msg).await {
                        tracing::error!(error = format!("{e:#}"), "handling message");
                    }
                }
            }
        }
    }
    registration.deregister().await;
    pool.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

async fn handle(
    client: &async_nats::Client,
    js: &jetstream::Context,
    config: &Config,
    pool: &mut Pool,
    msg: jetstream::Message,
) -> Result<()> {
    let envelope: Envelope = match serde_json::from_slice(&msg.payload) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(error = %e, subject = %msg.subject, "dropping malformed message");
            return msg.ack_with(AckKind::Term).await.map_err(|e| anyhow::anyhow!(e));
        }
    };
    if let Err(ProtoError::BudgetExhausted(_)) = envelope.reply(config.persona.clone(), "") {
        // Out of turns: hand the thread to the human instead of spending another prompt.
        let mut escalation = Envelope::on_thread(
            envelope.thread_id.clone(),
            config.persona.clone(),
            PersonaId::human(),
            format!("Turn budget spent; needs a decision. Last message from {}:\n{}", envelope.from, envelope.text),
        );
        escalation.in_reply_to = Some(envelope.id);
        bus::publish(js, &escalation).await?;
        return msg.double_ack().await.map_err(|e| anyhow::anyhow!(e));
    }

    let status = subject::status(&config.persona, &envelope.thread_id);
    tracing::info!(thread = %envelope.thread_id, from = %envelope.from, "prompting {}", config.harness);
    client.publish(status.clone(), "working".into()).await?;

    let prompt = pool.prompt(&envelope.thread_id, &envelope.text);
    tokio::pin!(prompt);
    let mut progress = tokio::time::interval(PROGRESS_EVERY);
    progress.tick().await;
    let result = loop {
        tokio::select! {
            r = &mut prompt => break r,
            _ = progress.tick() => {
                if let Err(e) = msg.ack_with(AckKind::Progress).await {
                    tracing::warn!(error = %e, "progress ack");
                }
            }
        }
    };

    let text = match result {
        Ok(text) => text,
        Err(e) => {
            client.publish(status, "failed".into()).await?;
            msg.ack_with(AckKind::Nak(Some(RETRY_DELAY))).await.map_err(|e| anyhow::anyhow!(e))?;
            return Err(e);
        }
    };

    let reply = envelope.reply(config.persona.clone(), text)?;
    // Store the reply before acking: a crash in between redelivers rather than loses the message.
    bus::publish(js, &reply).await?;
    msg.double_ack().await.map_err(|e| anyhow::anyhow!(e))?;
    client.publish(status, "idle".into()).await?;
    Ok(())
}
