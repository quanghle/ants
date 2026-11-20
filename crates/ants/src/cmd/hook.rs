//! `ants hook run`: hand each event of one kind an observer reads ([`Permissions::for_observer`]) to a command, one
//! process per event with the event's JSON document on its stdin.
//!
//! [`Permissions::for_observer`]: ants_proto::Permissions::for_observer

use ants_daemon::bus::relayed::{self, GuardedKv as _};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use ants_proto::{
    ApprovalDecision, ApprovalRequest, COLONY_BUCKET, ColonyEntry, DeadLetter, Feed, STREAM, StatusEvent, StatusUpdate,
    UserId, subject,
};
use anyhow::{Context as _, Result, anyhow, bail};
use async_nats::jetstream::{self, consumer, kv};
use clap::Subcommand;
use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::Semaphore;

use crate::connection::Nats;

#[derive(Subcommand, Debug)]
pub(crate) enum HookCommand {
    /// Run CMD once per event of one kind, with the event's JSON document on its stdin, until stopped. Stored
    /// events (tool requests, verdicts, dead letters) are read through the credentials' own consumer, from where it
    /// left off, and acked once CMD ends, whatever its exit; live ones (escalations, status, colony) from now on.
    /// Failures and timeouts are logged.
    Run {
        /// The kind of event.
        #[arg(long, value_enum)]
        on: Event,
        /// Most runs of CMD at once.
        #[arg(long, default_value_t = 4, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..))]
        concurrency: usize,
        /// How long one run of CMD may take before it is killed (a duration such as 90s, 10m or 1h).
        #[arg(long, value_name = "DURATION", value_parser = ants_proto::duration::parse, default_value = "60s")]
        timeout: Duration,
        /// The command and its arguments.
        #[arg(last = true, required = true, value_name = "CMD")]
        command: Vec<String>,
    },
}

pub(crate) async fn run(nats: &Nats<'_>, command: HookCommand) -> Result<()> {
    let HookCommand::Run { on, concurrency, timeout, command } = command;
    let (_, js) = nats.connect_only("ants-hook").await?;
    Hook { event: on, command, timeout, concurrency }.run(&js, &nats.me.id).await
}

/// The events a hook may run on.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// A tool request waiting for a verdict (an `ApprovalRequest`).
    ApprovalRequest,
    /// A verdict on a tool request (an `ApprovalDecision`).
    ApprovalDecision,
    /// A message an ant gave up on (a `DeadLetter`).
    DeadLetter,
    /// An escalation sent (a `StatusEvent` of type `escalated`).
    Escalation,
    /// A question an ant asked a human with ask_human (a `StatusEvent` of type `asked`).
    Question,
    /// A tool request settled by a human, an ant of the requester's chain or its expiry (a `StatusEvent` of type
    /// `decided`).
    Decided,
    /// Every live status update of every ant (a `StatusEvent`).
    Status,
    /// A colony entry written: an ant starting, stopping or reporting in (a `ColonyEntry`).
    Colony,
}

impl Event {
    /// The stored feed the event is read from, if it is stored.
    fn feed(self) -> Option<Feed> {
        match self {
            Self::ApprovalRequest => Some(Feed::ApprovalRequests),
            Self::ApprovalDecision => Some(Feed::ApprovalDecisions),
            Self::DeadLetter => Some(Feed::DeadLetters),
            Self::Escalation | Self::Question | Self::Decided | Self::Status | Self::Colony => None,
        }
    }

    /// Whether `payload`, read on `subject` (a key for the colony), is an event of this kind to hand on: `Ok(false)`
    /// for another kind, an error for one forged or unreadable.
    fn accepts(self, subject: &str, payload: &[u8]) -> Result<bool> {
        match self {
            Self::ApprovalRequest => serde_json::from_slice::<ApprovalRequest>(payload)?.check_subject(subject)?,
            Self::ApprovalDecision => {
                let decision: ApprovalDecision = serde_json::from_slice(payload)?;
                if decision.subject() != subject {
                    bail!("a decision on {} read on {subject}", decision.subject());
                }
            }
            Self::DeadLetter => {
                let dead: DeadLetter = serde_json::from_slice(payload)?;
                if dead.subject() != subject {
                    bail!("a dead letter of {} read on {subject}", dead.ant);
                }
            }
            Self::Escalation | Self::Question | Self::Decided | Self::Status => {
                let event: StatusEvent = serde_json::from_slice(payload)?;
                event.check_subject(subject)?;
                let kind = match event.update {
                    StatusUpdate::Escalated { .. } => Self::Escalation,
                    StatusUpdate::Asked { .. } => Self::Question,
                    StatusUpdate::Decided { .. } => Self::Decided,
                    _ => Self::Status,
                };
                return Ok(self == kind || self == Self::Status);
            }
            Self::Colony => serde_json::from_slice::<ColonyEntry>(payload)?.check_key(subject)?,
        }
        Ok(true)
    }
}

/// The command a hook runs for each event, and how.
pub struct Hook {
    pub event: Event,
    pub command: Vec<String>,
    /// How long one run may take before it is killed.
    pub timeout: Duration,
    /// Most runs at once.
    pub concurrency: usize,
}

impl Hook {
    /// Run the hook as `me` on every event until the connection's feed ends: stored events from where the observer's
    /// consumer of the feed left off (the next one on its first run), live ones from now.
    pub async fn run(self, js: &jetstream::Context, me: &UserId) -> Result<()> {
        let hook = Arc::new(self);
        let slots = Arc::new(Semaphore::new(hook.concurrency.max(1)));
        if let Some(feed) = hook.event.feed() {
            let config = consumer::pull::Config {
                durable_name: Some(feed.consumer(me)),
                filter_subject: feed.filter().to_owned(),
                deliver_policy: consumer::DeliverPolicy::New,
                ack_wait: hook.timeout + Duration::from_secs(30),
                max_ack_pending: i64::try_from(hook.concurrency.max(1)).unwrap_or(i64::MAX),
                inactive_threshold: Duration::from_hours(7 * 24),
                ..Default::default()
            };
            let stream = js.get_stream_no_info(STREAM).await?;
            let reader = stream.create_consumer(config).await.with_context(|| {
                format!(
                    "creating consumer {} (observer credentials: `ants auth user NAME --observer`)",
                    feed.consumer(me)
                )
            })?;
            let mut messages = reader.messages().await?;
            while let Some(msg) = messages.next().await {
                let msg = match msg {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!("reading {}: {e}", feed.consumer(me));
                        continue;
                    }
                };
                if relayed::stored_relayed(msg.headers.as_ref(), &msg.payload, relayed::Read::Consumer) {
                    tracing::warn!(subject = %msg.subject, "skipping a message a stream relayed");
                    if let Err(e) = msg.ack().await {
                        tracing::warn!("acking {}: {e}", msg.subject);
                    }
                    continue;
                }
                let slot = slots.clone().acquire_owned().await?;
                let hook = hook.clone();
                tokio::spawn(async move {
                    hook.handle(&msg.subject, &msg.payload).await;
                    if let Err(e) = msg.ack().await {
                        tracing::warn!("acking {}: {e}", msg.subject);
                    }
                    drop(slot);
                });
            }
        } else if hook.event == Event::Colony {
            let colony = js.get_key_value(COLONY_BUCKET).await?;
            let mut entries = colony.guarded_watch(">").await?;
            while let Some(entry) = entries.next().await {
                let entry = entry?;
                if entry.operation != kv::Operation::Put {
                    continue;
                }
                let slot = slots.clone().acquire_owned().await?;
                let hook = hook.clone();
                tokio::spawn(async move {
                    hook.handle(&entry.key, &entry.value).await;
                    drop(slot);
                });
            }
        } else {
            let mut updates = relayed::subscribe(&js.client(), subject::ALL_STATUS).await?;
            while let Some(msg) = updates.next().await {
                let slot = slots.clone().acquire_owned().await?;
                let hook = hook.clone();
                tokio::spawn(async move {
                    hook.handle(&msg.subject, &msg.payload).await;
                    drop(slot);
                });
            }
        }
        Err(anyhow!("the {:?} feed ended", hook.event))
    }

    /// Run the command on `payload` if it is an event of the hook's kind, logging what went wrong.
    async fn handle(&self, at: &str, payload: &[u8]) {
        match self.event.accepts(at, payload) {
            Ok(true) => {}
            Ok(false) => return,
            Err(e) => {
                tracing::warn!("skipping what was read on {at}: {e}");
                return;
            }
        }
        if let Err(e) = self.deliver(payload).await {
            tracing::warn!("hook {:?} on {at}: {e:#}", self.command.join(" "));
        }
    }

    /// Run the command once with `event` and a newline on its stdin, killing it after the timeout.
    async fn deliver(&self, event: &[u8]) -> Result<()> {
        let (program, args) = self.command.split_first().context("no command")?;
        let mut child = tokio::process::Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting {program}"))?;
        let mut stdin = child.stdin.take().context("no stdin")?;
        let feed = async move {
            // A command that does not read its stdin is no failure.
            let _ = stdin.write_all(event).await;
            let _ = stdin.write_all(b"\n").await;
        };
        let (_, status) = match tokio::time::timeout(self.timeout, async { tokio::join!(feed, child.wait()) }).await {
            Ok(done) => done,
            Err(_) => {
                child.kill().await?;
                bail!("killed after {:?}", self.timeout);
            }
        };
        let status = status?;
        if !status.success() {
            bail!("{status}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ants_proto::{ThreadId, Verdict};

    fn hook(event: Event, script: &str, timeout: Duration) -> Hook {
        Hook { event, command: vec!["sh".into(), "-c".into(), script.into()], timeout, concurrency: 1 }
    }

    #[tokio::test]
    async fn a_run_reads_the_event_on_stdin_and_fails_on_a_bad_exit_or_the_timeout() {
        let dir = ants_test_support::scratch_dir("hook-run");
        let out = dir.join("event.json");
        let copy = hook(Event::DeadLetter, &format!("cat > '{}'", out.display()), Duration::from_secs(10));
        copy.deliver(b"{\"a\":1}").await.unwrap();
        assert_eq!(std::fs::read_to_string(&out).unwrap(), "{\"a\":1}\n");

        let ignoring = hook(Event::DeadLetter, "exit 0", Duration::from_secs(10));
        ignoring.deliver(&vec![b'x'; 1 << 20]).await.unwrap();
        let failing = hook(Event::DeadLetter, "exit 3", Duration::from_secs(10));
        assert!(format!("{}", failing.deliver(b"{}").await.unwrap_err()).contains('3'));
        let slow = hook(Event::DeadLetter, "sleep 10", Duration::from_millis(200));
        let started = std::time::Instant::now();
        assert!(format!("{}", slow.deliver(b"{}").await.unwrap_err()).contains("killed"));
        assert!(started.elapsed() < Duration::from_secs(5));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn events_are_checked_against_where_they_were_read() {
        let (dev_a, dev_b) = ("dev-a".parse().unwrap(), "dev-b".parse::<UserId>().unwrap());
        let decision = ApprovalDecision::new(ants_proto::Uuid::now_v7(), dev_a, Verdict::Reject);
        let json = serde_json::to_vec(&decision).unwrap();
        assert!(Event::ApprovalDecision.accepts(&decision.subject(), &json).unwrap());
        let elsewhere = subject::approval_decision(&dev_b, &decision.request_id);
        assert!(Event::ApprovalDecision.accepts(&elsewhere, &json).is_err());
        assert!(Event::ApprovalDecision.accepts(&decision.subject(), b"not json").is_err());

        let thread = ThreadId::generate();
        let working = StatusEvent::new(dev_b, thread.clone(), StatusUpdate::Message { text: "hi".into() });
        let json = serde_json::to_vec(&working).unwrap();
        assert!(Event::Status.accepts(&working.subject(), &json).unwrap());
        assert!(!Event::Escalation.accepts(&working.subject(), &json).unwrap());
        assert!(!Event::Decided.accepts(&working.subject(), &json).unwrap());
        let decided = StatusUpdate::Decided {
            request_id: decision.request_id,
            verdict: Verdict::Reject,
            by: ants_proto::DecidedBy::Expiry,
            decider: None,
            via: None,
        };
        let decided = serde_json::to_vec(&StatusEvent::new(working.ant.clone(), thread.clone(), decided)).unwrap();
        assert!(Event::Decided.accepts(&working.subject(), &decided).unwrap());
        assert!(Event::Status.accepts(&working.subject(), &decided).unwrap());
        assert!(!Event::Escalation.accepts(&working.subject(), &decided).unwrap());
        assert!(!Event::Question.accepts(&working.subject(), &decided).unwrap());
        let options = ["yes".to_owned(), "no".to_owned()];
        let question = ants_proto::Question::new("Ship it?", &options, true).unwrap();
        let (to, message) = ("alice".parse().unwrap(), ants_proto::Uuid::now_v7());
        let asked = StatusUpdate::Asked { to, message, question: question.into() };
        let asked = serde_json::to_vec(&StatusEvent::new(working.ant.clone(), thread.clone(), asked)).unwrap();
        assert!(Event::Question.accepts(&working.subject(), &asked).unwrap());
        assert!(Event::Status.accepts(&working.subject(), &asked).unwrap());
        assert!(!Event::Escalation.accepts(&working.subject(), &asked).unwrap());
        assert!(!Event::Question.accepts(&working.subject(), &json).unwrap());
        assert!(Event::Status.accepts(&subject::status(&"dev-c".parse().unwrap(), &thread), &json).is_err());
    }
}
