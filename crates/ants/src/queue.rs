//! `ants queue` and `ants withdraw`: the messages waiting for an ant, and withdrawing one before the ant is prompted
//! with it.

use std::time::Duration;

pub use ants_daemon::delivery::queue::{Waiting, state};
use ants_daemon::delivery::queue::{mark, waiting, withdrawals, withdrawer};
use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{ATTEMPTS_BUCKET, MessageMark, QueueReply, QueueRequest, UserId, Uuid};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use serde::Serialize;

/// How long `ants queue` and `ants withdraw` wait for a daemon of the ant to answer.
const QUEUE_WAIT: Duration = Duration::from_secs(5);
/// How long they wait on after a first answer, for another daemon of the same ant.
const QUEUE_GRACE: Duration = Duration::from_millis(500);
/// The most messages `ants queue` lists of those its daemon has not taken yet.
const UNDELIVERED_SHOWN: usize = 100;

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
    let humans = crate::people::humans::read(js).await?;
    let (mut waiting, more) = waiting(js, &kv, ant, humans, &answers, UNDELIVERED_SHOWN).await?;
    withdrawals(&kv, ant, &mut waiting).await?;
    Ok(Some((waiting, more)))
}

/// Why [`read`] found no queue.
pub fn unanswered(ant: &UserId) -> String {
    format!("no daemon serving {ant} answered within {}s", QUEUE_WAIT.as_secs())
}

/// Print the messages waiting for `ant`, or without one for every ant live in the colony: a table, or with `json` one
/// per line.
pub async fn list(nats: &crate::connection::Nats<'_>, ant: Option<UserId>, json: bool) -> Result<()> {
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
pub async fn withdraw(nats: &crate::connection::Nats<'_>, ant: UserId, message: Uuid) -> Result<()> {
    let me = &nats.me.id;
    if !nats.me.role.is_human() {
        bail!("the credentials of {me} are an ant's; only a human withdraws a message");
    }
    let (_client, js) = nats.connect("ants-withdraw").await?;
    println!("{}", withdrawn(&js, nats.domain, me, &ant, message).await?);
    Ok(())
}

/// Withdraw `message` from the queue of `ant` as `me`, a human: what was done, or why it was refused.
pub async fn withdrawn(
    js: &async_nats::jetstream::Context,
    domain: Option<&ants_proto::JsDomain>,
    me: &UserId,
    ant: &UserId,
    message: Uuid,
) -> Result<String> {
    let ant = ant.clone();
    let client = js.client();
    let kv = DirectKv::new(js, domain, ATTEMPTS_BUCKET).await?;
    let found = mark(&kv, &ant, &message).await?;
    if let Some(found @ (MessageMark::Withdrawn { .. } | MessageMark::Finished { .. })) = found {
        return refused(&ant, &message, found, false);
    }
    // A message not waiting is refused, as one named by a mistyped id or ant would be; with no daemon to ask, the
    // withdrawal waits for one to take the message up.
    let answered = ask(&client, &ant, me).await?;
    let listed = match &answered {
        Some(answers) => {
            let humans = crate::people::humans::read(js).await?;
            let (waiting, _) = waiting(js, &kv, &ant, humans, answers, usize::MAX).await?;
            waiting.into_iter().find(|m| m.id == message)
        }
        None => None,
    };
    let redelivery = listed.as_ref().is_some_and(|m| m.started);
    if let Some(found @ MessageMark::Started { .. }) = found {
        return refused(&ant, &message, found, redelivery);
    }
    if answered.is_some() && listed.is_none() {
        bail!("message {message} is not waiting for {ant}; ants queue {ant} lists the messages that are");
    }
    let retry = match found {
        Some(MessageMark::Retrying { held: Some(held), .. }) => format!(", where it was held for {held}"),
        Some(MessageMark::Retrying { held: None, .. }) => {
            ", where it awaited a retry after a failed attempt".to_owned()
        }
        _ => String::new(),
    };
    match ants_daemon::delivery::attempts::withdraw(&kv, &ant, &message, me, None).await? {
        Some(found) => refused(&ant, &message, found, false),
        None if answered.is_none() => Ok(format!(
            "withdrew message {message} from the queue of {ant}; no daemon serving {ant} answered, so none checked \
             that it waits there"
        )),
        None => Ok(format!(
            "withdrew message {message} from the queue of {ant}{retry}; the thread is told once its turn comes"
        )),
    }
}

/// The outcome of withdrawing `message` when it has a mark already; `redelivery` when it awaits redelivery started.
fn refused(ant: &UserId, message: &Uuid, found: MessageMark, redelivery: bool) -> Result<String> {
    match found {
        MessageMark::Withdrawn { by, on_behalf_of, .. } => {
            Ok(format!("message {message} was withdrawn already, by {}", withdrawer(&by, on_behalf_of.as_ref())))
        }
        MessageMark::Started { .. } if redelivery => bail!(
            "{ant} started on message {message} already, in an attempt that did not end (its daemon stopped, or its \
             reply is stored); the message awaits redelivery to finish it, so it cannot be withdrawn"
        ),
        MessageMark::Started { thread_id, .. } | MessageMark::Retrying { thread_id, .. } => bail!(
            "{ant} started on message {message} already; ants cancel {ant} {thread_id} stops the prompt running on its \
             thread"
        ),
        MessageMark::Finished { .. } => bail!("{ant} finished message {message} already"),
    }
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

#[cfg(test)]
mod tests {
    use ants_proto::subject;

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
            retry: false,
            held: None,
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
        (retried.redelivery, retried.retry) = (true, true);
        let mut started = row(8, "t3", false, 2, false);
        (started.redelivery, started.started) = (true, true);
        let mut redelivered = row(7, "t3", false, 1, false);
        redelivered.redelivery = true;
        let mut signed_out = row(9, "t4", false, 0, true);
        (signed_out.retry, signed_out.held) = (true, Some(ants_proto::Hold::SignIn));
        let rows = [
            row(1, "t1", true, 0, true),
            row(2, "t1", false, 1, true),
            withdrawn,
            row(4, "t2", false, 0, false),
            assisted,
            retried,
            redelivered,
            started,
            signed_out,
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
                "next, awaiting a retry after a failed attempt",
                "1 ahead, awaiting redelivery",
                "2 ahead, awaiting redelivery (started; not withdrawable)",
                "next, held for a sign-in",
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
