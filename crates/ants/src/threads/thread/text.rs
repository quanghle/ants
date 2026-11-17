//! Threads and their entries as text: the table of `ants thread list`, and the entries of `ants thread show`.

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{Answered, NoReplyWhy, UserId};
use chrono::{DateTime, Utc};

use super::NoReply;
use super::entry::Entry;
use super::summaries::Summary;
use crate::messages::attachments;

pub(crate) fn time(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// The threads as a table: one row per thread under a header, columns padded to line up, and under a thread
/// matching `--grep` its matching line, under one with an open escalation that escalation. A STATE column (pinned,
/// archived, archived+new for one with a message stored since, or auto-archived for one idle, then done for one
/// marked done, comma-separated) and a TITLE column show only when a thread listed has one.
pub fn table(threads: &[Summary]) -> String {
    let marked = threads.iter().any(|t| t.pinned || t.archived || t.done.is_some());
    let titled = threads.iter().any(|t| t.title.is_some());
    let mut header = vec!["THREAD", "OWNER", "USERS", "FIRST", "LAST", "MSGS"];
    header.extend(marked.then_some("STATE"));
    header.extend(titled.then_some("TITLE"));
    header.push("OPENING");
    let rows: Vec<Vec<String>> = std::iter::once(header.into_iter().map(str::to_owned).collect())
        .chain(threads.iter().map(|t| {
            let users = t.users.iter().map(UserId::as_str);
            let channels = t.channels.iter().map(|c| format!("#{c}"));
            let mut row = vec![
                t.thread_id.to_string(),
                t.owner.as_ref().map_or("-", UserId::as_str).to_owned(),
                users.map(str::to_owned).chain(channels).collect::<Vec<_>>().join(","),
                time(t.first_at),
                time(t.last_at),
                t.messages.to_string(),
            ];
            if marked {
                let states: Vec<&str> = state(t).into_iter().chain(t.done.as_ref().map(|_| "done")).collect();
                row.push(if states.is_empty() { "-".to_owned() } else { states.join(",") });
            }
            if titled {
                row.push(t.title.clone().unwrap_or_else(|| "-".to_owned()));
            }
            row.push(t.opening.clone());
            row
        }))
        .collect();
    let mut widths = vec![0; rows[0].len()];
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let mut lines = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let cells: Vec<String> = row.iter().zip(&widths).map(|(cell, &width)| format!("{cell:width$}")).collect();
        lines.push(cells.join("  ").trim_end().to_owned());
        if let Some(thread) = i.checked_sub(1).map(|i| &threads[i]) {
            for line in thread.matched.iter().chain(&thread.escalated) {
                lines.push(format!("    {line}"));
            }
        }
    }
    lines.join("\n")
}

/// How the CLI's human marked a thread listed, as the STATE column says it.
pub(super) fn state(thread: &Summary) -> Option<&'static str> {
    match (thread.pinned, thread.archived, thread.new_activity) {
        (true, ..) => Some("pinned"),
        (_, true, _) if thread.auto_archived => Some("auto-archived"),
        (_, true, true) => Some("archived+new"),
        (_, true, false) => Some("archived"),
        _ => None,
    }
}

/// The entries of a thread as `ants thread show` prints them: time, kind, route, note, id and text, one entry per
/// line, its text's further lines, its attachments and the reactions to it indented under it. A question shows its options numbered,
/// whether a typed answer is taken, and the answers to it among `entries` or how to answer it; an answer, the question
/// and the option it picks.
pub fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    for e in entries {
        let text = body(e, entries);
        let mut lines = text.lines();
        let first = lines.next().unwrap_or("");
        let _ = writeln!(out, "{}: {first}", heading(e));
        for line in lines {
            let _ = writeln!(out, "    {line}");
        }
        out.push_str(&attachments::render(&e.attachments, "    "));
        out.push_str(&crate::messages::inbox::render_reactions(&e.reactions, "    "));
    }
    out
}

/// How long a tool call took: `840ms`, `2.1s`, `3m`.
pub(crate) fn took(ms: u64) -> String {
    match ms {
        0..1000 => format!("{ms}ms"),
        1000..60_000 => format!("{:.1}s", Duration::from_millis(ms).as_secs_f64()),
        _ => ants_proto::duration::format(Duration::from_millis(ms)),
    }
}

/// What [`render`] shows of an entry before its text: time, kind, route, note, answer and id.
pub fn heading(e: &Entry) -> String {
    let forged = if e.forged { " (forged)" } else { "" };
    let note = e.note.as_ref().map_or_default(|n| format!(" [{n}]"));
    let answering = e.answer.as_ref().map_or_default(|a| match a.option {
        Some(n) => format!(" [answers {} with option {n}]", a.question),
        None => format!(" [answers {} as typed]", a.question),
    });
    let kind = e.kind.as_str();
    let id = e.id.map_or_default(|id| format!(" (message {id})"));
    format!("{}  {kind:<10}  {}{forged}{note}{answering}{id}", time(e.stored_at), e.route())
}

/// The text [`render`] shows of an entry among `entries`: a question with its options and its answers among them, why
/// a prompt ended with no reply sent and its final text, else the entry's text; after the line on the message it
/// replies to, if it quotes one ([`Quote::line`]).
///
/// [`Quote::line`]: ants_proto::Quote::line
pub fn body(e: &Entry, entries: &[Entry]) -> String {
    if let Some(end) = &e.no_reply {
        return no_reply(end, &e.text);
    }
    let text = match (&e.question, e.id) {
        (Some(question), Some(id)) => {
            let answers = answers_to(e, entries).map(|(by, answered)| answer_line(by, answered)).collect();
            crate::messages::inbox::render_question(id, question, answers)
        }
        _ => e.text.clone(),
    };
    match &e.quote {
        Some(quote) => format!("{}\n{text}", quote.line()),
        None => text,
    }
}

/// Why a prompt ended with no reply sent, then its final `text`, which went to nobody.
fn no_reply(end: &NoReply, text: &str) -> String {
    let why = match (end.why, &end.reason) {
        (NoReplyWhy::ReplyToOwn, _) => "reply to own message, answer not sent".to_owned(),
        (NoReplyWhy::Completed, Some(reason)) => format!("completed ({reason})"),
        (NoReplyWhy::Completed, None) => "completed".to_owned(),
        (NoReplyWhy::Steered, Some(reason)) => format!("steered ({reason})"),
        (NoReplyWhy::Steered, None) => "steered".to_owned(),
    };
    if text.is_empty() { why } else { format!("{why}: {text}") }
}

/// The answers among `entries` to `question`, an entry asking one, oldest first, each with who answered: an option
/// picked or a typed answer, the answer's text.
pub fn answers_to<'a>(question: &'a Entry, entries: &'a [Entry]) -> impl Iterator<Item = (&'a Entry, Answered<'a>)> {
    entries.iter().filter_map(move |e| Some((e, e.answers(question)?)))
}

/// How a question's answer is shown beneath it: who answered with which option, or typed what.
pub(crate) fn answer_line(by: &Entry, answered: Answered<'_>) -> String {
    let from = by.from.as_ref().map_or("(a human)", UserId::as_str);
    match answered {
        Answered::Option(n, option) => format!("answered by {from}: option {n}, {option}"),
        Answered::Typed => format!("answered by {from}, typed: {}", crate::text::excerpt(&by.text, 72)),
    }
}
