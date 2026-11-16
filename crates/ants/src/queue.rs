//! `ants queue` and `ants withdraw`: the messages waiting for an ant, and withdrawing one before the ant is prompted
//! with it.

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use std::time::Duration;

use ants_daemon::channel::{self, Stored, Wake};
use ants_daemon::direct_kv::DirectKv;
use ants_daemon::reading::{self, Reading};
use ants_daemon::webhooks::Webhooks;
use ants_proto::{
    ATTEMPTS_BUCKET, CHANNELS_BUCKET, ChannelEntry, ChannelId, ChannelPost, ConsumerPlace, Envelope, Humans,
    MessageMark, QueueReply, QueueRequest, QueuedMessage, ThreadId, UserId, Uuid, message_mark_key, subject,
};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::kv;
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
    /// Whether the ant's daemon holds it, taken from the stream.
    pub delivered: bool,
    /// Whether it awaits redelivery: delivered, not acked, and back with the server.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub redelivery: bool,
    /// Whether the ant started on it already, in an attempt that failed: it awaits a retry and cannot be withdrawn.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub started: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn_by: Option<UserId>,
    /// The human whose assistant `withdrawn_by` withdrew it, as its daemon checked.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn_for: Option<UserId>,
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
    let kv = DirectKv::new(js, domain, ATTEMPTS_BUCKET).await?;
    let (mut waiting, more) = waiting(js, &kv, ant, &answers, UNDELIVERED_SHOWN).await?;
    for message in waiting.iter_mut().filter(|m| !m.running && !m.started && m.withdrawn_by.is_none()) {
        if let Some(MessageMark::Withdrawn { by, on_behalf_of, .. }) = mark(&kv, ant, &message.id).await? {
            (message.withdrawn_by, message.withdrawn_for) = (Some(by), on_behalf_of);
        }
    }
    Ok(Some((waiting, more)))
}

/// Why [`read`] found no queue.
pub fn unanswered(ant: &UserId) -> String {
    format!("no daemon serving {ant} answered within {}s", QUEUE_WAIT.as_secs())
}

/// Print the messages waiting for `ant`, or without one for every ant live in the colony: a table, or with `json` one
/// per line.
pub async fn list(nats: &crate::Nats<'_>, ant: Option<UserId>, json: bool) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; only a human lists an ant's queue");
    }
    let (_, js) = nats.connect("ants-queue").await?;
    let Some(ant) = ant else { return list_live(&js, nats.domain, me, json).await };
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

/// A message waiting for an ant, as `ants queue --json` without an ant lists it.
#[derive(Serialize)]
struct AntWaiting<'a> {
    ant: &'a UserId,
    #[serde(flatten)]
    message: &'a Waiting,
}

/// Print the messages waiting for every ant live in the colony, asking their daemons at once. An ant whose daemon does
/// not answer is told on stderr; only when none answers does the listing fail.
async fn list_live(
    js: &async_nats::jetstream::Context,
    domain: Option<&ants_proto::JsDomain>,
    me: &UserId,
    json: bool,
) -> Result<()> {
    let now = Utc::now();
    let mut ants: Vec<UserId> = crate::colony::list(js)
        .await?
        .into_iter()
        .filter(|entry| ants_daemon::colony::is_live(entry, now))
        .map(|entry| entry.ant)
        .collect();
    ants.sort();
    ants.dedup();
    if ants.is_empty() {
        println!("no live ants in the colony");
        return Ok(());
    }
    let reads = futures::future::join_all(ants.iter().map(|ant| read(js, domain, ant, me))).await;
    let mut queues = Vec::new();
    for (ant, queue) in ants.iter().zip(reads) {
        match queue {
            Ok(Some((waiting, more))) => queues.push((ant.clone(), waiting, more)),
            Ok(None) => eprintln!("{}", unanswered(ant)),
            Err(e) => eprintln!("{ant}: {e:#}"),
        }
    }
    if queues.is_empty() {
        bail!("none of the {} live ants answered within {}s", ants.len(), QUEUE_WAIT.as_secs());
    }
    if json {
        for (ant, waiting, _) in &queues {
            for message in waiting {
                println!("{}", serde_json::to_string(&AntWaiting { ant, message })?);
            }
        }
    } else if queues.iter().all(|(_, waiting, _)| waiting.is_empty()) {
        println!("no messages waiting for any live ant ({} of {} answered)", queues.len(), ants.len());
    } else {
        println!("{}", table_by_ant(&queues, Utc::now()));
    }
    for (ant, _, more) in queues.iter().filter(|(_, _, more)| *more > 0) {
        eprintln!("{more} more messages wait in the stream for {ant}, not listed");
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
    let found = mark(&kv, &ant, &message).await?;
    if found.as_ref().is_some_and(|found| !matches!(found, MessageMark::Started { .. })) {
        return refused(&ant, &message, found.expect("a mark"), false);
    }
    // A message not waiting is refused, as one named by a mistyped id or ant would be; with no daemon to ask, the
    // withdrawal waits for one to take the message up.
    let answered = ask(&client, &ant, me).await?;
    let listed = match &answered {
        Some(answers) => waiting(&js, &kv, &ant, answers, usize::MAX).await?.0.into_iter().find(|m| m.id == message),
        None => None,
    };
    let retried = listed.as_ref().is_some_and(|m| m.started);
    if let Some(found) = found {
        return refused(&ant, &message, found, retried);
    }
    if answered.is_some() && listed.is_none() {
        bail!("message {message} is not waiting for {ant}; ants queue {ant} lists the messages that are");
    }
    match ants_daemon::attempts::withdraw(&kv, &ant, &message, me, None).await? {
        Some(found) => refused(&ant, &message, found, false),
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

/// The outcome of withdrawing `message` when it has a mark already; `retried` when it awaits a retry after a failed
/// attempt.
fn refused(ant: &UserId, message: &Uuid, found: MessageMark, retried: bool) -> Result<()> {
    match found {
        MessageMark::Withdrawn { by, on_behalf_of, .. } => {
            println!("message {message} was withdrawn already, by {}", withdrawer(&by, on_behalf_of.as_ref()));
            Ok(())
        }
        MessageMark::Started { .. } if retried => bail!(
            "{ant} started on message {message} already; the attempt failed and the message awaits a retry, so it \
             cannot be withdrawn"
        ),
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

/// The messages waiting for `ant`: those its daemons hold, as `answers` list them; those awaiting redelivery, back
/// with the server after a delivery not acked; then up to `undelivered` of the inbox messages and channel posts for it
/// the daemons have not taken from the stream yet. Returns how many more of those there are. `kv` holds the messages'
/// marks.
async fn waiting(
    js: &async_nats::jetstream::Context,
    kv: &DirectKv,
    ant: &UserId,
    answers: &[QueueReply],
    undelivered: usize,
) -> Result<(Vec<Waiting>, usize)> {
    let mut held: Vec<QueuedMessage> = answers.iter().flat_map(|a| a.messages.iter().cloned()).collect();
    held.sort_by_key(|m| m.seq);
    held.dedup_by_key(|m| m.seq);
    let held_seqs: HashSet<u64> = held.iter().map(|m| m.seq).collect();
    let held_posts = held.iter().filter(|m| m.channel.is_some()).count() as u64;
    let held_inbox = held.len() as u64 - held_posts;
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
            delivered: !m.redelivery,
            redelivery: m.redelivery,
            // A daemon awaits the redelivery of a message only after a failed attempt at it.
            started: m.redelivery,
            withdrawn_by: None,
            withdrawn_for: None,
        })
        .collect();
    let mut ahead: HashMap<ThreadId, usize> = HashMap::new();
    for m in &waiting {
        *ahead.entry(m.thread_id.clone()).or_default() += 1;
    }
    let inbox = place(answers, |a| a.inbox);
    let posts = place(answers, |a| a.posts);
    let mut wakes = Wakes::new(js, ant).await?;
    let mut found = Vec::new();
    if let Some(inbox) = inbox {
        let missing = inbox.ack_pending.saturating_sub(held_inbox);
        let candidates =
            redeliveries(js, &subject::inbox_of(ant), inbox, missing, &held_seqs, async |msg| inbox_message(&msg))
                .await?;
        found.extend(candidates);
    }
    if let Some(posts) = posts {
        let missing = posts.ack_pending.saturating_sub(held_posts);
        let wakes = &mut wakes;
        let candidates =
            redeliveries(js, subject::ALL_CHANNELS, posts, missing, &held_seqs, async |msg| wakes.post(&msg).await)
                .await?;
        found.extend(candidates);
    }
    found.sort_by_key(|(seq, _)| *seq);
    for (seq, message) in found {
        let found = mark(kv, ant, &message.id).await?;
        // Acked, or about to be.
        if matches!(found, Some(MessageMark::Finished { .. })) {
            continue;
        }
        let mut redelivered = to_waiting(seq, message, &mut ahead);
        redelivered.redelivery = true;
        match found {
            Some(MessageMark::Started { .. }) => redelivered.started = true,
            Some(MessageMark::Withdrawn { by, on_behalf_of, .. }) => {
                (redelivered.withdrawn_by, redelivered.withdrawn_for) = (Some(by), on_behalf_of);
            }
            _ => {}
        }
        waiting.push(redelivered);
    }
    let mut stored = Vec::new();
    let mut more = 0;
    if let Some(inbox) = inbox.filter(|p| p.undelivered > 0) {
        let (shown, unshown) =
            in_stream(js, &subject::inbox_of(ant), inbox, undelivered, async |msg| inbox_message(&msg)).await?;
        stored.extend(shown);
        more += unshown;
    }
    if let Some(posts) = posts.filter(|p| p.undelivered > 0) {
        let wakes = &mut wakes;
        let (shown, unshown) =
            in_stream(js, subject::ALL_CHANNELS, posts, undelivered, async |msg| wakes.post(&msg).await).await?;
        stored.extend(shown);
        more += unshown;
    }
    stored.sort_by_key(|(seq, _)| *seq);
    more += stored.len().saturating_sub(undelivered);
    stored.truncate(undelivered);
    waiting.extend(stored.into_iter().map(|(seq, message)| to_waiting(seq, message, &mut ahead)));
    Ok((waiting, more))
}

/// Where a consumer of the ant stands, as the furthest of the `answers` tells.
fn place(answers: &[QueueReply], of: impl Fn(&QueueReply) -> Option<ConsumerPlace>) -> Option<ConsumerPlace> {
    answers.iter().filter_map(of).reduce(|a, b| ConsumerPlace {
        acked: a.acked.max(b.acked),
        delivered: a.delivered.max(b.delivered),
        undelivered: a.undelivered.max(b.undelivered),
        ack_pending: a.ack_pending.max(b.ack_pending),
    })
}

/// A message for an ant, read from the stream: an inbox message, or a channel post as one.
struct Unheld {
    id: Uuid,
    thread_id: ThreadId,
    from: UserId,
    channel: Option<ChannelId>,
    sent_at: DateTime<Utc>,
}

/// `message`, at `seq`, as a message not held by a daemon, behind the others of its thread counted in `ahead`.
fn to_waiting(seq: u64, message: Unheld, ahead: &mut HashMap<ThreadId, usize>) -> Waiting {
    let before = ahead.entry(message.thread_id.clone()).or_default();
    let waiting = Waiting {
        id: message.id,
        thread_id: message.thread_id,
        from: message.from,
        channel: message.channel,
        sent_at: message.sent_at,
        seq,
        running: false,
        ahead: *before,
        delivered: false,
        redelivery: false,
        started: false,
        withdrawn_by: None,
        withdrawn_for: None,
    };
    *before += 1;
    waiting
}

/// The inbox message in `msg`, when the ant's daemon takes it up: no forged sender, no decision on a tool request.
fn inbox_message(msg: &async_nats::Message) -> Option<Unheld> {
    let envelope = serde_json::from_slice::<Envelope>(&msg.payload).ok()?;
    if envelope.check_subject(&msg.subject).is_err() || envelope.approval.is_some() {
        return None;
    }
    let Envelope { id, thread_id, from, sent_at, .. } = envelope;
    Some(Unheld { id, thread_id, from, channel: None, sent_at })
}

/// The messages on `filter` a consumer at `place` delivered and has not had acked, up to `missing` of them, but those
/// at `held`: what awaits redelivery. `take` tells which messages the consumer's daemon keeps unacked.
async fn redeliveries(
    js: &async_nats::jetstream::Context,
    filter: &str,
    place: ConsumerPlace,
    missing: u64,
    held: &HashSet<u64>,
    mut take: impl AsyncFnMut(async_nats::Message) -> Option<Unheld>,
) -> Result<Vec<(u64, Unheld)>> {
    let mut found = Vec::new();
    if missing == 0 || place.acked >= place.delivered {
        return Ok(found);
    }
    let start = DeliverPolicy::ByStartSequence { start_sequence: place.acked + 1 };
    reading::read_back_awaiting(js, "", Reading::Gets, filter, start, async |seq, _, msg| {
        if seq > place.delivered {
            return ControlFlow::Break(());
        }
        if !held.contains(&seq)
            && let Some(message) = take(msg).await
        {
            found.push((seq, message));
            if found.len() as u64 >= missing {
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    })
    .await
    .context("reading the messages awaiting redelivery")?;
    Ok(found)
}

/// The first `shown` of the messages on `filter` a consumer at `place` has not delivered yet, which `take` keeps, and
/// how many more it keeps.
async fn in_stream(
    js: &async_nats::jetstream::Context,
    filter: &str,
    place: ConsumerPlace,
    shown: usize,
    mut take: impl AsyncFnMut(async_nats::Message) -> Option<Unheld>,
) -> Result<(Vec<(u64, Unheld)>, usize)> {
    let (mut stored, mut more) = (Vec::new(), 0);
    let start = DeliverPolicy::ByStartSequence { start_sequence: place.delivered + 1 };
    reading::read_back_awaiting(js, "", Reading::Gets, filter, start, async |seq, _, msg| {
        if let Some(message) = take(msg).await {
            if stored.len() < shown {
                stored.push((seq, message));
            } else {
                more += 1;
            }
        }
        ControlFlow::Continue(())
    })
    .await
    .context("reading the messages waiting in the stream")?;
    Ok((stored, more))
}

/// Which channel posts wake an ant, as its daemon tells them ([`channel::wake`]), with the channels' entries and the
/// webhooks read once.
struct Wakes {
    ant: UserId,
    humans: Humans,
    channels: Option<kv::Store>,
    entries: HashMap<ChannelId, Option<ChannelEntry>>,
    webhooks: Webhooks,
    checked: HashMap<UserId, bool>,
}

impl Wakes {
    async fn new(js: &async_nats::jetstream::Context, ant: &UserId) -> Result<Self> {
        Ok(Self {
            ant: ant.clone(),
            humans: crate::humans::read(js).await?,
            channels: js.get_key_value(CHANNELS_BUCKET).await.ok(),
            entries: HashMap::new(),
            webhooks: Webhooks::new(js.clone()),
            checked: HashMap::new(),
        })
    }

    /// The channel post in `msg`, when it wakes the ant.
    async fn post(&mut self, msg: &async_nats::Message) -> Option<Unheld> {
        let mut post = serde_json::from_slice::<ChannelPost>(&msg.payload).ok()?;
        post.check_subject(&msg.subject).ok()?;
        if post.webhook && !self.humans.contains(&post.from) {
            post.webhook = match self.checked.get(&post.from) {
                Some(&webhook) => webhook,
                None => *self.checked.entry(post.from.clone()).or_insert(self.webhooks.is_webhook(&post.from).await),
            };
        }
        if !self.entries.contains_key(&post.channel) {
            let entry = match &self.channels {
                Some(kv) => match channel::load(kv, &post.channel).await {
                    Ok(Stored::Entry(entry, _)) => Some(entry),
                    _ => None,
                },
                None => None,
            };
            self.entries.insert(post.channel.clone(), entry);
        }
        let entry = self.entries.get(&post.channel).and_then(Option::as_ref);
        if channel::wake(&self.ant, entry, &post, &self.humans) == Wake::Skip {
            return None;
        }
        let ChannelPost { id, thread_id, from, channel, sent_at, .. } = post;
        Some(Unheld { id, thread_id, from, channel: Some(channel), sent_at })
    }
}

pub fn table(waiting: &[Waiting], now: DateTime<Utc>) -> String {
    let header = ["MESSAGE", "THREAD", "FROM", "AGE", "STATE"].map(str::to_owned).to_vec();
    grid(std::iter::once(header).chain(waiting.iter().map(|m| cells(m, now))))
}

/// The messages waiting for each ant of `queues` (the ant, its messages, and how many more are unlisted), with the
/// ant in a column of its own; ants with nothing waiting are left out.
fn table_by_ant(queues: &[(UserId, Vec<Waiting>, usize)], now: DateTime<Utc>) -> String {
    let header = ["ANT", "MESSAGE", "THREAD", "FROM", "AGE", "STATE"].map(str::to_owned).to_vec();
    let rows = queues.iter().flat_map(|(ant, waiting, _)| {
        waiting.iter().map(move |m| std::iter::once(ant.to_string()).chain(cells(m, now)).collect())
    });
    grid(std::iter::once(header).chain(rows))
}

/// The cells of `m` in a table row: its id, thread, sender, age and state.
fn cells(m: &Waiting, now: DateTime<Utc>) -> Vec<String> {
    let from = match &m.channel {
        Some(channel) => format!("{} in #{channel}", m.from),
        None => m.from.to_string(),
    };
    let age = crate::colony::short((now - m.sent_at).to_std().unwrap_or_default());
    vec![m.id.to_string(), m.thread_id.to_string(), from, age, state(m)]
}

/// `rows`, the first a header, in columns as wide as their widest cell.
fn grid(rows: impl Iterator<Item = Vec<String>>) -> String {
    let rows: Vec<Vec<String>> = rows.collect();
    let mut widths = vec![0; rows.first().map_or(0, Vec::len)];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let lines = rows.iter().map(|row| {
        let cells: Vec<String> = row.iter().zip(&widths).map(|(cell, &width)| format!("{cell:width$}")).collect();
        cells.join("  ").trim_end().to_owned()
    });
    lines.collect::<Vec<_>>().join("\n")
}

/// Where a message stands: running, withdrawn, or waiting behind others of its thread, in the daemon's hands, awaiting
/// redelivery (or a retry, not withdrawable, after a failed attempt) or still in the stream.
pub fn state(m: &Waiting) -> String {
    if m.running {
        return "running".to_owned();
    }
    if let Some(by) = &m.withdrawn_by {
        return format!("withdrawn by {}", withdrawer(by, m.withdrawn_for.as_ref()));
    }
    let place = if m.ahead == 0 { "next".to_owned() } else { format!("{} ahead", m.ahead) };
    if m.started {
        format!("{place}, awaiting a retry (started; not withdrawable)")
    } else if m.redelivery {
        format!("{place}, awaiting redelivery")
    } else if m.delivered {
        place
    } else {
        format!("{place}, in the stream")
    }
}

/// Who withdrew a message: `by`, or the human `on_behalf_of` via its assistant `by`.
fn withdrawer(by: &UserId, on_behalf_of: Option<&UserId>) -> String {
    match on_behalf_of {
        Some(human) => format!("{human} via {by}"),
        None => by.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: u128, thread: &str, running: bool, ahead: usize, delivered: bool) -> Waiting {
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
            redelivery: false,
            started: false,
            withdrawn_by: None,
            withdrawn_for: None,
        }
    }

    #[test]
    fn the_table_shows_where_each_message_stands() {
        let mut withdrawn = row(3, "t1", false, 2, true);
        withdrawn.withdrawn_by = Some("alice".parse().unwrap());
        let mut assisted = row(5, "t2", false, 1, false);
        (assisted.withdrawn_by, assisted.withdrawn_for) = (Some("pa".parse().unwrap()), Some("bob".parse().unwrap()));
        let mut retried = row(6, "t3", false, 0, false);
        (retried.redelivery, retried.started) = (true, true);
        let mut redelivered = row(7, "t3", false, 1, false);
        redelivered.redelivery = true;
        let rows = [
            row(1, "t1", true, 0, true),
            row(2, "t1", false, 1, true),
            withdrawn,
            row(4, "t2", false, 0, false),
            assisted,
            retried,
            redelivered,
        ];
        let table = table(&rows, Utc::now());
        let states: Vec<&str> = table.lines().skip(1).map(|l| l.split("  ").last().unwrap().trim()).collect();
        assert!(table.starts_with("MESSAGE"), "{table}");
        assert!(table.lines().nth(1).unwrap().contains(" 3m "), "{table}");
        assert_eq!(
            states,
            [
                "running",
                "1 ahead",
                "withdrawn by alice",
                "next, in the stream",
                "withdrawn by bob via pa",
                "next, awaiting a retry (started; not withdrawable)",
                "1 ahead, awaiting redelivery",
            ]
        );
    }

    #[test]
    fn every_ant_with_messages_waiting_is_listed_with_its_own() {
        let (a, b, c): (UserId, UserId, UserId) =
            ("dev-a".parse().unwrap(), "dev-b".parse().unwrap(), "dev-c".parse().unwrap());
        let queues = [
            (a, vec![row(1, "t1", true, 0, true), row(2, "t1", false, 1, false)], 0),
            (b, Vec::new(), 0),
            (c, vec![row(3, "t2", false, 0, false)], 4),
        ];
        let table = table_by_ant(&queues, Utc::now());
        let ants: Vec<&str> = table.lines().map(|l| l.split_whitespace().next().unwrap()).collect();
        assert_eq!(ants, ["ANT", "dev-a", "dev-a", "dev-c"], "{table}");
        assert!(table.lines().nth(3).unwrap().ends_with("next, in the stream"), "{table}");
    }

    #[tokio::test]
    async fn redeliveries_and_posts_waking_the_ant_are_listed_after_the_messages_held() {
        let Some(server) = crate::auth::tests::Server::start() else { return };
        let (alice, ant): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = server.connect_as(&alice, ants_proto::Role::Human).await;
        ants_daemon::bus::ensure_stream(&js).await.unwrap();
        ants_daemon::bus::init_own_buckets(&js).await.unwrap();
        let mut ops = ChannelEntry::new("ops".parse().unwrap());
        ops.members.push(ant.clone());
        let channels = ants_daemon::bus::ensure_channels(&js).await.unwrap();
        channels.put(ops.key(), serde_json::to_vec(&ops).unwrap().into()).await.unwrap();
        let kv = DirectKv::new(&js, None, ATTEMPTS_BUCKET).await.unwrap();

        let running = Envelope::new(alice.clone(), ant.clone(), "running");
        let on_thread = |text| Envelope::on_thread(running.thread_id.clone(), alice.clone(), ant.clone(), text);
        let (failed, retried, finished, later) =
            (on_thread("failed"), on_thread("retried"), on_thread("done"), on_thread("later"));
        let woken = ChannelPost::new(ops.channel.clone(), alice.clone(), "@dev-a look");
        let unwoken = ChannelPost::new("lobby".parse().unwrap(), alice.clone(), "@dev-a look");
        for envelope in [&running, &failed, &retried, &finished] {
            js.publish(envelope.subject(), serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
        }
        for post in [&woken, &unwoken] {
            js.publish(post.subject(), serde_json::to_vec(post).unwrap().into()).await.unwrap().await.unwrap();
        }
        js.publish(later.subject(), serde_json::to_vec(&later).unwrap().into()).await.unwrap().await.unwrap();
        let thread_id = running.thread_id.clone();
        for (envelope, mark) in [
            (&retried, MessageMark::Started { thread_id: thread_id.clone(), at: Utc::now() }),
            (&finished, MessageMark::Finished { thread_id: thread_id.clone(), at: Utc::now() }),
        ] {
            kv.put(message_mark_key(&ant, &envelope.id), serde_json::to_vec(&mark).unwrap()).await.unwrap();
        }

        let held = QueuedMessage {
            id: running.id,
            thread_id: thread_id.clone(),
            from: alice.clone(),
            channel: None,
            sent_at: running.sent_at,
            seq: 1,
            running: true,
            ahead: 0,
            redelivery: false,
        };
        // The messages at 2 to 4 are delivered and not acked; the channel consumer has delivered no post yet.
        let inbox = ConsumerPlace { acked: 0, delivered: 4, undelivered: 1, ack_pending: 4 };
        let posts = ConsumerPlace { acked: 0, delivered: 0, undelivered: 2, ack_pending: 0 };
        let answer = QueueRequest::new(ant.clone(), alice.clone()).answer(vec![held], Some(inbox), Some(posts));
        let (listed, more) = waiting(&js, &kv, &ant, std::slice::from_ref(&answer), 10).await.unwrap();
        let ids: Vec<Uuid> = listed.iter().map(|m| m.id).collect();
        assert_eq!(ids, [running.id, failed.id, retried.id, woken.id, later.id]);
        let states: Vec<String> = listed.iter().map(state).collect();
        assert_eq!(
            states,
            [
                "running",
                "1 ahead, awaiting redelivery",
                "2 ahead, awaiting a retry (started; not withdrawable)",
                "next, in the stream",
                "3 ahead, in the stream",
            ]
        );
        assert_eq!(listed[3].channel, Some(ops.channel.clone()));
        assert_eq!(more, 0);

        let (listed, more) = waiting(&js, &kv, &ant, &[answer], 1).await.unwrap();
        assert_eq!(listed.last().map(|m| m.id), Some(woken.id));
        assert_eq!(more, 1, "the later inbox message waits unlisted");
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
