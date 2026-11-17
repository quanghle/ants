//! Catch-up summaries ([`SummaryRequest`]): a TL;DR of one thread, stored for every human under the thread's
//! [`ants_proto::summary_key`] in [`ants_proto::THREADS_BUCKET`] ([`ThreadSummary`]), or a digest of the threads with
//! entries since a time, stored nowhere. Either is asked of `[cli]` `summary_ant` by a message carrying the request on
//! a thread of its own, its text the entries; the ant's reply is the summary. Such threads are left out of the lists
//! of threads and of the inbox.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::records::owners;
use ants_daemon::stream::reading::{self, Reading};
use ants_proto::{Envelope, SummaryRequest, ThreadId, ThreadSummary, UserId, subject};
use anyhow::{Result, bail};
use async_nats::jetstream::{self, consumer};
use chrono::{DateTime, Utc};
use futures::StreamExt as _;

use crate::colony;
use crate::connection::Nats;
use crate::messages::{inbox, progress};
use crate::threads::thread::{self, Entry, Pick};
use crate::threads::thread_meta;

/// Most bytes of entries a request carries: the newest that fit.
pub const ENTRIES_MAX: usize = 16 * 1024;
/// Most bytes of one entry's text a request carries.
pub const ENTRY_MAX: usize = 1024;

/// `text` cut to at most `max` bytes, on a character boundary, with an ellipsis when cut.
fn cut(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max.saturating_sub('…'.len_utf8());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// The entries as a request's text carries them, oldest first, one line each (its text's line breaks kept): time,
/// kind, route, with `threads` the thread, and the text cut to [`ENTRY_MAX`]; only the newest within [`ENTRIES_MAX`].
pub fn entries_text(entries: &[Entry], threads: bool) -> String {
    let mut lines = Vec::new();
    let mut size = 0;
    for e in entries.iter().rev() {
        let on = if threads { format!(" on thread {}", e.thread_id) } else { String::new() };
        let line = format!(
            "{} {} {}{on}: {}\n",
            thread::time(e.stored_at),
            e.kind.as_str(),
            e.route(),
            cut(&e.text, ENTRY_MAX)
        );
        if size + line.len() > ENTRIES_MAX {
            break;
        }
        size += line.len();
        lines.push(line);
    }
    lines.reverse();
    lines.concat()
}

/// The text and request asking for the TL;DR of `thread`, up to its last entry; None when it has none.
pub async fn tldr_request(js: &jetstream::Context, thread: &ThreadId) -> Result<Option<(String, SummaryRequest)>> {
    let entries: Vec<Entry> = thread::history(js, thread, None, thread::Activity::None, None)
        .await?
        .into_iter()
        .filter(|e| e.summary.is_none())
        .collect();
    let Some(last) = entries.last() else { return Ok(None) };
    let request = SummaryRequest::thread(thread.clone(), last.seq);
    Ok(Some((entries_text(&entries, false), request)))
}

/// The text and request asking for a digest of the threads with entries since `since`, threads opened by summary
/// requests left out; None when there is no entry.
pub async fn digest_request(js: &jetstream::Context, since: DateTime<Utc>) -> Result<Option<(String, SummaryRequest)>> {
    let start = consumer::DeliverPolicy::ByStartTime { start_time: std::time::SystemTime::from(since).into() };
    let mut entries = Vec::new();
    thread::scan(js, start, |entry| entries.push(entry)).await?;
    let catch_up: HashSet<ThreadId> =
        entries.iter().filter(|e| e.summary.is_some()).map(|e| e.thread_id.clone()).collect();
    entries.retain(|e| !catch_up.contains(&e.thread_id));
    let Some(last) = entries.last() else { return Ok(None) };
    let request = SummaryRequest::since(since, last.seq);
    Ok(Some((entries_text(&entries, true), request)))
}

/// What `me` sends `ant` to ask for `request`: a message on a thread of its own, which `me` owns.
pub fn request_envelope(me: &UserId, ant: UserId, text: String, request: SummaryRequest) -> Envelope {
    let envelope = Envelope::new(me.clone(), ant, text).with_owner(Some(me.clone()));
    Envelope { summary: Some(request), ..envelope }
}

/// Send `envelope`, a summary request of the CLI's human ([`request_envelope`]), and wait for its ant's reply,
/// showing the ant's progress meanwhile; the reply is marked read. Fails on a reply that carries no summary, such as
/// an ant's notice of a failed prompt.
pub async fn ask(nats: &Nats<'_>, js: &jetstream::Context, envelope: &Envelope) -> Result<Envelope> {
    let me = &nats.me;
    let client = js.client();
    // Subscribe first so a fast reply is not missed.
    let mut replies = bus::caught_up_inbox(&client, js, &me.id, me.reading()).await?;
    let unserved = colony::unserved_warnings(js, &[&envelope.to]).await;
    for warning in &unserved {
        tracing::warn!("{warning}");
    }
    let status = if progress::wanted(me.role.is_human()) {
        client
            .subscribe(subject::status(&envelope.to, &envelope.thread_id))
            .await
            .inspect_err(|e| tracing::debug!("showing no progress: {e}"))
            .ok()
    } else {
        None
    };
    if let Some(owner) = &envelope.owner {
        owners::recording(js, nats.domain, &envelope.thread_id, &envelope.to, owner).await;
    }
    if bus::publish(js, envelope).await? == bus::Stored::Outbox {
        eprintln!("kept in this machine's outbox until the hub is reachable");
    }
    let _shown = status.map(|sub| {
        let stand = if unserved.is_empty() { progress::Stand::Waiting } else { progress::Stand::Unserved };
        progress::start(sub, progress::Status::new(vec![envelope.id]).with(envelope.to.clone(), stand), true)
    });
    let reply = reply(&mut replies, envelope).await?;
    progress::clear();
    taken(js, &me.id, reply).await
}

/// The reply to `envelope`, a summary request, among `replies`, the inbox of its sender.
pub async fn reply(replies: &mut bus::LiveInbox, envelope: &Envelope) -> Result<Envelope> {
    loop {
        let Some(msg) = replies.next().await else { bail!("the connection closed before {} replied", envelope.to) };
        let Ok(reply) = serde_json::from_slice::<Envelope>(&msg.payload) else { continue };
        if reply.thread_id != envelope.thread_id || reply.from != envelope.to {
            continue;
        }
        match reply.check_subject(&msg.subject) {
            Ok(()) => return Ok(reply),
            Err(e) => tracing::warn!("ignoring a reply with a forged sender: {e}"),
        }
    }
}

/// `reply`, to a summary request of `me`, marked read; fails when it carries no summary, such as an ant's notice of a
/// failed prompt.
pub async fn taken(js: &jetstream::Context, me: &UserId, reply: Envelope) -> Result<Envelope> {
    if let Err(e) = inbox::mark_read(js, me, &reply).await {
        tracing::warn!("could not mark the reply read: {e:#}");
    }
    if reply.summary.is_none() {
        bail!("{} answered with no summary: {}", reply.from, reply.text);
    }
    Ok(reply)
}

/// The TL;DR `reply` gives for `request`, asked by `asker`: the request's thread and sequence, the reply's text and
/// tokens. None for a digest.
pub fn tldr_of(request: &SummaryRequest, reply: &Envelope, asker: &UserId) -> Option<ThreadSummary> {
    Some(ThreadSummary {
        thread_id: request.thread_id()?.clone(),
        text: reply.text.trim().to_owned(),
        by: reply.from.clone(),
        asker: asker.clone(),
        at: reply.sent_at,
        upto: request.upto,
        tokens: reply.summary.as_ref().and_then(|s| s.tokens).unwrap_or_default(),
    })
}

/// The tokens the summaries of `human` spent the UTC day of `now`: those the replies to its summary requests stored
/// since midnight carry.
pub async fn spent_today(js: &jetstream::Context, human: &UserId, now: DateTime<Utc>) -> Result<u64> {
    let midnight = now.date_naive().and_time(chrono::NaiveTime::MIN).and_utc();
    let start = consumer::DeliverPolicy::ByStartTime { start_time: std::time::SystemTime::from(midnight).into() };
    let mut spent = 0;
    let filter = subject::inbox_of(human);
    reading::read_back(js, human.as_str(), Reading::Gets, &filter, start, |_, _, subject, payload| {
        if let Ok(e) = serde_json::from_slice::<Envelope>(payload)
            && e.check_subject(subject).is_ok()
        {
            spent += e.summary.and_then(|s| s.tokens).unwrap_or_default();
        }
        ControlFlow::Continue(())
    })
    .await?;
    Ok(spent)
}

/// `ants thread tldr THREAD`: the TL;DR of the thread `pick` picks, kept while no entry came after it unless `new`,
/// else asked of `[cli]` `summary_ant` and kept for every human.
pub async fn tldr(nats: &Nats<'_>, pick: Pick, new: bool, json: bool) -> Result<()> {
    let config = crate::defaults::summaries()?;
    let js = thread::connect(nats).await?;
    let thread = pick.resolve(nats, &js).await?;
    let Some((text, request)) = tldr_request(&js, &thread).await? else {
        bail!("no messages on thread {thread} to summarise")
    };
    let meta = thread_meta::open(nats, &js)
        .await
        .inspect_err(|e| tracing::warn!("not reading or keeping the TL;DR: {e:#}"))
        .ok();
    if !new && let Some(meta) = &meta {
        match meta.summary(&thread).await {
            Ok(Some(kept)) if kept.is_current(request.upto) => return print_tldr(&kept, json),
            Ok(_) => {}
            Err(e) => tracing::warn!("not reading the TL;DR kept: {e:#}"),
        }
    }
    let envelope = request_envelope(&nats.me.id, config.ant, text, request.clone());
    let reply = ask(nats, &js, &envelope).await?;
    let Some(tldr) = tldr_of(&request, &reply, &nats.me.id) else { bail!("a TL;DR request with no thread") };
    if let Some(meta) = &meta
        && let Err(e) = meta.put_summary(&tldr).await
    {
        tracing::warn!("not keeping the TL;DR: {e:#}");
    }
    print_tldr(&tldr, json)?;
    spent_note(&js, &nats.me.id, json).await;
    Ok(())
}

fn print_tldr(tldr: &ThreadSummary, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(tldr)?);
    } else {
        let (thread, by, at) = (&tldr.thread_id, &tldr.by, thread::time(tldr.at));
        println!("TL;DR of thread {thread} by {by}, {at} UTC, {} tokens:\n{}", tldr.tokens, tldr.text);
    }
    Ok(())
}

/// `ants digest`: a digest of the threads with entries within `since` (everything the stream keeps for 0), asked of
/// `[cli]` `summary_ant` and kept nowhere.
pub async fn digest(nats: &Nats<'_>, since: Duration) -> Result<()> {
    let config = crate::defaults::summaries()?;
    let js = thread::connect(nats).await?;
    let since = match chrono::TimeDelta::from_std(since) {
        Ok(since) if !since.is_zero() => Utc::now() - since,
        _ => DateTime::<Utc>::UNIX_EPOCH,
    };
    let Some((text, request)) = digest_request(&js, since).await? else {
        println!("no entries since {} UTC", thread::time(since));
        return Ok(());
    };
    let envelope = request_envelope(&nats.me.id, config.ant, text, request);
    let reply = ask(nats, &js, &envelope).await?;
    let tokens = reply.summary.as_ref().and_then(|s| s.tokens).unwrap_or_default();
    println!(
        "digest of the threads with entries since {} UTC by {}, {tokens} tokens:\n{}",
        thread::time(since),
        reply.from,
        reply.text.trim()
    );
    spent_note(&js, &nats.me.id, false).await;
    Ok(())
}

/// Tell on stderr, but for JSON, what the summaries of `human` spent this UTC day.
async fn spent_note(js: &jetstream::Context, human: &UserId, json: bool) {
    if json {
        return;
    }
    match spent_today(js, human, Utc::now()).await {
        Ok(spent) => eprintln!("summaries spent {spent} tokens today (UTC)"),
        Err(e) => tracing::debug!("not reading the tokens summaries spent today: {e:#}"),
    }
}

#[cfg(test)]
mod tests;
