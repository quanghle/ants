//! `ants queue` and `ants withdraw`: the messages waiting for an ant, and withdrawing one before the ant is prompted
//! with it.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::time::Duration;

use ants_daemon::direct_kv::DirectKv;
use ants_daemon::reading::{self, Reading};
use ants_proto::{
    ATTEMPTS_BUCKET, ChannelId, Envelope, MessageMark, QueueReply, QueueRequest, QueuedMessage, ThreadId, UserId, Uuid,
    message_mark_key, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::consumer::DeliverPolicy;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::Serialize;

/// How long `ants queue` and `ants withdraw` wait for a daemon of the ant to answer.
const QUEUE_WAIT: Duration = Duration::from_secs(5);
/// How long they wait on after a first answer, for another daemon of the same ant.
const QUEUE_GRACE: Duration = Duration::from_millis(500);
/// The most messages `ants queue` lists of those its daemon has not taken yet.
const UNDELIVERED_SHOWN: usize = 100;

/// A message waiting for an ant, as `ants queue` lists it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Waiting {
    pub id: Uuid,
    pub thread_id: ThreadId,
    pub from: UserId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    pub sent_at: DateTime<Utc>,
    pub seq: u64,
    pub running: bool,
    /// How many messages of its thread go before it.
    pub ahead: usize,
    /// Whether the ant's daemon took it from the stream already.
    pub delivered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn_by: Option<UserId>,
}

/// The messages waiting for `ant`, as `ants queue` lists them, and how many more wait in the stream unlisted; None
/// when no daemon serving the ant answered within [`QUEUE_WAIT`]. Asked as `me`, a human.
pub async fn read(
    js: &async_nats::jetstream::Context,
    domain: Option<&ants_proto::JsDomain>,
    ant: &UserId,
    me: &UserId,
) -> Result<Option<(Vec<Waiting>, usize)>> {
    let Some(answers) = ask(&js.client(), ant, me).await? else { return Ok(None) };
    let (mut waiting, more) = waiting(js, ant, &answers, UNDELIVERED_SHOWN).await?;
    let kv = DirectKv::new(js, domain, ATTEMPTS_BUCKET).await?;
    for message in waiting.iter_mut().filter(|m| !m.running) {
        if let Some(MessageMark::Withdrawn { by, .. }) = mark(&kv, ant, &message.id).await? {
            message.withdrawn_by = Some(by);
        }
    }
    Ok(Some((waiting, more)))
}

/// Why [`read`] found no queue.
pub fn unanswered(ant: &UserId) -> String {
    format!("no daemon serving {ant} answered within {}s", QUEUE_WAIT.as_secs())
}

/// Print the messages waiting for `ant`: a table, or with `json` one per line.
pub async fn list(nats: &crate::Nats<'_>, ant: UserId, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; only a human lists an ant's queue");
    }
    let (_, js) = nats.connect("ants-queue").await?;
    let Some((waiting, more)) = read(&js, nats.domain, &ant, me).await? else { bail!(unanswered(&ant)) };
    if json {
        for message in &waiting {
            println!("{}", serde_json::to_string(message)?);
        }
    } else if waiting.is_empty() {
        println!("no messages waiting for {ant}");
    } else {
        println!("{}", table(&waiting, Utc::now()));
    }
    if more > 0 {
        eprintln!("{more} more messages wait in the stream, not listed");
    }
    Ok(())
}

/// Withdraw `message` from the queue of `ant`, so that the ant is never prompted with it.
pub async fn withdraw(nats: &crate::Nats<'_>, ant: UserId, message: Uuid) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; only a human withdraws a message");
    }
    let (client, js) = nats.connect("ants-withdraw").await?;
    let kv = DirectKv::new(&js, nats.domain, ATTEMPTS_BUCKET).await?;
    if let Some(found) = mark(&kv, &ant, &message).await? {
        return refused(&ant, &message, found);
    }
    // A message not waiting is refused, as one named by a mistyped id or ant would be; with no daemon to ask, the
    // withdrawal waits for one to take the message up.
    let answered = ask(&client, &ant, me).await?;
    if let Some(answers) = &answered {
        let (waiting, _) = waiting(&js, &ant, answers, usize::MAX).await?;
        if !waiting.iter().any(|m| m.id == message) {
            bail!("message {message} is not waiting for {ant}; ants queue {ant} lists the messages that are");
        }
    }
    match ants_daemon::attempts::withdraw(&kv, &ant, &message, me).await? {
        Some(found) => refused(&ant, &message, found),
        None if answered.is_none() => {
            println!(
                "withdrew message {message} from the queue of {ant}; no daemon serving {ant} answered, so none \
                 checked that it waits there"
            );
            Ok(())
        }
        None => {
            println!("withdrew message {message} from the queue of {ant}; the thread is told once its turn comes");
            Ok(())
        }
    }
}

/// The outcome of withdrawing `message` when it has a mark already.
fn refused(ant: &UserId, message: &Uuid, found: MessageMark) -> Result<()> {
    match found {
        MessageMark::Withdrawn { by, .. } => {
            println!("message {message} was withdrawn already, by {by}");
            Ok(())
        }
        MessageMark::Started { thread_id, .. } => bail!(
            "{ant} started on message {message} already; ants cancel {ant} {thread_id} stops the prompt running on its \
             thread"
        ),
        MessageMark::Finished { .. } => bail!("{ant} finished message {message} already"),
    }
}

async fn mark(kv: &DirectKv, ant: &UserId, message: &Uuid) -> Result<Option<MessageMark>> {
    let Some(value) = kv.get(message_mark_key(ant, message)).await? else { return Ok(None) };
    serde_json::from_slice(&value).map(Some).context("reading the message's mark")
}

/// The answers of the daemons of `ant` to a queue request from `me`, None when none answers.
async fn ask(client: &async_nats::Client, ant: &UserId, me: &UserId) -> Result<Option<Vec<QueueReply>>> {
    let request = QueueRequest::new(ant.clone(), me.clone());
    let answers = client.subscribe(request.reply_subject()).await?;
    client.publish(request.subject(), serde_json::to_vec(&request)?.into()).await?;
    client.flush().await?;
    let answers = answers.map(|msg| (msg.subject.to_string(), msg.payload.to_vec()));
    let answers = collect(&request, answers, QUEUE_WAIT, QUEUE_GRACE).await;
    Ok((!answers.is_empty()).then_some(answers))
}

/// The answers to `request` that come within `wait`, and within `grace` of the first.
async fn collect(
    request: &QueueRequest,
    mut answers: impl futures::Stream<Item = (String, Vec<u8>)> + Unpin,
    wait: Duration,
    grace: Duration,
) -> Vec<QueueReply> {
    let mut deadline = tokio::time::Instant::now() + wait;
    let mut collected: Vec<QueueReply> = Vec::new();
    while let Ok(Some((subject, payload))) = tokio::time::timeout_at(deadline, answers.next()).await {
        let Ok(answer) = serde_json::from_slice::<QueueReply>(&payload) else { continue };
        if answer.request_id != request.id || answer.check_subject(&subject).is_err() {
            continue;
        }
        if collected.is_empty() {
            deadline = deadline.min(tokio::time::Instant::now() + grace);
        }
        collected.push(answer);
    }
    collected
}

/// The messages waiting for `ant`: those its daemons hold, as `answers` list them, then up to `undelivered` of those
/// they have not taken from the stream yet. Returns how many more of those there are.
async fn waiting(
    js: &async_nats::jetstream::Context,
    ant: &UserId,
    answers: &[QueueReply],
    undelivered: usize,
) -> Result<(Vec<Waiting>, usize)> {
    let mut held: Vec<QueuedMessage> = answers.iter().flat_map(|a| a.messages.iter().cloned()).collect();
    held.sort_by_key(|m| m.seq);
    held.dedup_by_key(|m| m.seq);
    let mut waiting: Vec<Waiting> = held
        .into_iter()
        .map(|m| Waiting {
            id: m.id,
            thread_id: m.thread_id,
            from: m.from,
            channel: m.channel,
            sent_at: m.sent_at,
            seq: m.seq,
            running: m.running,
            ahead: m.ahead,
            delivered: true,
            withdrawn_by: None,
        })
        .collect();
    let delivered = answers.iter().filter_map(|a| a.delivered).max();
    let (Some(delivered), Some(pending)) = (delivered, answers.iter().filter_map(|a| a.undelivered).max()) else {
        return Ok((waiting, 0));
    };
    if pending == 0 {
        return Ok((waiting, 0));
    }
    let mut ahead: HashMap<ThreadId, usize> = HashMap::new();
    for m in &waiting {
        *ahead.entry(m.thread_id.clone()).or_default() += 1;
    }
    let (mut stored, mut more) = (Vec::new(), 0);
    let start = DeliverPolicy::ByStartSequence { start_sequence: delivered + 1 };
    reading::read_back(js, "", Reading::Gets, &subject::inbox_of(ant), start, |seq, _, subject, payload| {
        let Ok(envelope) = serde_json::from_slice::<Envelope>(payload) else { return ControlFlow::Continue(()) };
        // What the daemon takes up: no forged sender, no decision on a tool request.
        if envelope.check_subject(subject).is_err() || envelope.approval.is_some() {
            return ControlFlow::Continue(());
        }
        if stored.len() < undelivered {
            stored.push((seq, envelope));
        } else {
            more += 1;
        }
        ControlFlow::Continue(())
    })
    .await
    .context("reading the messages waiting in the stream")?;
    for (seq, envelope) in stored {
        let before = ahead.entry(envelope.thread_id.clone()).or_default();
        waiting.push(Waiting {
            id: envelope.id,
            thread_id: envelope.thread_id,
            from: envelope.from,
            channel: None,
            sent_at: envelope.sent_at,
            seq,
            running: false,
            ahead: *before,
            delivered: false,
            withdrawn_by: None,
        });
        *before += 1;
    }
    Ok((waiting, more))
}

pub fn table(waiting: &[Waiting], now: DateTime<Utc>) -> String {
    let header = ["MESSAGE", "THREAD", "FROM", "AGE", "STATE"].map(str::to_owned);
    let rows: Vec<[String; 5]> = std::iter::once(header)
        .chain(waiting.iter().map(|m| {
            let from = match &m.channel {
                Some(channel) => format!("{} in #{channel}", m.from),
                None => m.from.to_string(),
            };
            let age = crate::roster::short((now - m.sent_at).to_std().unwrap_or_default());
            [m.id.to_string(), m.thread_id.to_string(), from, age, state(m)]
        }))
        .collect();
    let mut widths = [0; 5];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines = rows.iter().map(|row| {
        let cells: Vec<String> = row.iter().zip(widths).map(|(cell, width)| format!("{cell:width$}")).collect();
        cells.join("  ").trim_end().to_owned()
    });
    lines.collect::<Vec<_>>().join("\n")
}

/// Where a message stands: running, withdrawn, or waiting behind others of its thread, in the daemon's hands or still
/// in the stream.
pub fn state(m: &Waiting) -> String {
    if m.running {
        return "running".to_owned();
    }
    if let Some(by) = &m.withdrawn_by {
        return format!("withdrawn by {by}");
    }
    let place = if m.ahead == 0 { "next".to_owned() } else { format!("{} ahead", m.ahead) };
    if m.delivered { place } else { format!("{place}, in the stream") }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waiting(id: u128, thread: &str, running: bool, ahead: usize, delivered: bool) -> Waiting {
        Waiting {
            id: Uuid::from_u128(id),
            thread_id: thread.parse().unwrap(),
            from: UserId::human(),
            channel: None,
            sent_at: Utc::now() - chrono::Duration::minutes(3),
            seq: 1,
            running,
            ahead,
            delivered,
            withdrawn_by: None,
        }
    }

    #[test]
    fn the_table_shows_where_each_message_stands() {
        let mut withdrawn = waiting(3, "t1", false, 2, true);
        withdrawn.withdrawn_by = Some("alice".parse().unwrap());
        let rows = [
            waiting(1, "t1", true, 0, true),
            waiting(2, "t1", false, 1, true),
            withdrawn,
            waiting(4, "t2", false, 0, false),
        ];
        let table = table(&rows, Utc::now());
        let states: Vec<&str> = table.lines().skip(1).map(|l| l.split("  ").last().unwrap().trim()).collect();
        assert!(table.starts_with("MESSAGE"), "{table}");
        assert!(table.lines().nth(1).unwrap().contains(" 3m "), "{table}");
        assert_eq!(states, ["running", "1 ahead", "withdrawn by alice", "next, in the stream"]);
    }

    #[tokio::test]
    async fn answers_to_other_requests_or_forged_ones_are_ignored() {
        let ant: UserId = "dev-a".parse().unwrap();
        let request = QueueRequest::new(ant.clone(), UserId::human());
        let other = QueueRequest::new(ant.clone(), UserId::human());
        let payload = |r: &QueueRequest| serde_json::to_vec(&r.answer(Vec::new(), None, None)).unwrap();
        let ours = (request.reply_subject(), payload(&request));
        let answers = futures::stream::iter([
            (other.reply_subject(), payload(&other)),
            (subject::queued(&"dev-b".parse().unwrap()), payload(&request)),
            ours.clone(),
            ours,
        ])
        .chain(futures::stream::pending());
        let collected = collect(&request, Box::pin(answers), QUEUE_WAIT, Duration::from_millis(10)).await;
        assert_eq!(collected.len(), 2, "{collected:?}");
        assert!(collected.iter().all(|a| a.request_id == request.id));
    }
}
