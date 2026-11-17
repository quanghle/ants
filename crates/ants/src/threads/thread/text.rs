//! Threads and their entries as text: the table of `ants thread list`, and the entries of `ants thread show`.

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{Answered, NoReplyWhy, Pick, UserId};
use chrono::{DateTime, Utc};

use super::entry::{Entry, Kind};
use super::summaries::Summary;
use super::{NoReply, ToolCall};
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

/// The entries of a thread as `ants thread show` prints them: each message a block, its heading ([`heading`]) then
/// its text, attachments and the reactions to it indented under it, a blank line between blocks; the tool calls and
/// the prompts ended with no reply sent after a message as compact lines under it, the first joined to it by `⎿`:
/// time, of day only on the day of the line before, ant, the call as `Name(args)` and how it ended, or why no reply
/// was sent. A question shows its options numbered, whether a typed answer is taken, and the answers to it among
/// `entries` or how to answer it; an answer, the question and the option it picks.
pub fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    let (mut under, mut day) = (false, None);
    for e in entries {
        let text = body(e, entries);
        let mut lines = text.lines();
        let first = lines.next().unwrap_or("");
        let at = match day {
            Some(day) if day == e.stored_at.date_naive() => e.stored_at.format("%H:%M:%S").to_string(),
            _ => time(e.stored_at),
        };
        day = Some(e.stored_at.date_naive());
        let ant = e.from.as_ref().map_or("(an ant)", UserId::as_str);
        let lead = if under { "    " } else { "  ⎿ " };
        if let Some(tool) = &e.tool {
            under = true;
            let (name, args) = call_label(tool.tool_kind.as_deref(), first);
            let args = args.map_or_default(|args| format!("({args})"));
            let _ = writeln!(out, "{lead}{at} {ant}  {name}{args}{}", ended(tool));
            continue;
        }
        if e.no_reply.is_some() {
            under = true;
            let _ = writeln!(out, "{lead}{at} {ant}  no reply: {first}");
            for line in lines {
                let _ = writeln!(out, "{}", format!("      {line}").trim_end());
            }
            continue;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        under = false;
        let _ = writeln!(out, "{}", heading(e));
        for line in text.lines() {
            let _ = writeln!(out, "{}", format!("  {line}").trim_end());
        }
        out.push_str(&attachments::render(&e.attachments, "  "));
        out.push_str(&crate::messages::inbox::render_reactions(&e.reactions, "  "));
    }
    out
}

/// How a tool call ended after its label: ` · 35ms`, ` · failed after 4.0s`, ` · in progress`.
fn ended(tool: &ToolCall) -> String {
    let took = tool.duration_ms.map(took);
    match (tool.status.as_deref(), took) {
        (Some("completed"), Some(took)) => format!(" · {took}"),
        (Some("completed"), None) => String::new(),
        (Some("failed"), Some(took)) => format!(" · failed after {took}"),
        (Some(status), _) => format!(" · {}", status.replace('_', " ")),
        (None, _) => " · started".to_owned(),
    }
}

/// How long a tool call took: `840ms`, `2.1s`, `3m`.
pub(crate) fn took(ms: u64) -> String {
    match ms {
        0..1000 => format!("{ms}ms"),
        1000..60_000 => format!("{:.1}s", Duration::from_millis(ms).as_secs_f64()),
        _ => ants_proto::duration::format(Duration::from_millis(ms)),
    }
}

/// A tool call of ACP kind `kind` titled `title` as a name and what it takes, shown `Name(args)`: `Run` and the
/// command for one running a command, the verb its title starts with and the rest, else the name of its kind and its
/// title; a call of no kind named so, only its title.
pub(crate) fn call_label(kind: Option<&str>, title: &str) -> (String, Option<String>) {
    const VERBS: [&str; 16] = [
        "Read", "View", "Edit", "Write", "Create", "Delete", "Remove", "Move", "Rename", "Find", "Search", "Grep",
        "Glob", "List", "Fetch", "Think",
    ];
    let title = title.trim();
    let name = match kind {
        Some("execute") => {
            let command = title.strip_prefix("Run ").unwrap_or(title);
            let command = command.strip_prefix('`').and_then(|c| c.strip_suffix('`')).unwrap_or(command);
            return ("Run".to_owned(), (!command.is_empty()).then(|| command.to_owned()));
        }
        Some("read") => "Read",
        Some("edit") => "Edit",
        Some("delete") => "Delete",
        Some("move") => "Move",
        Some("search") => "Search",
        Some("think") => "Think",
        Some("fetch") => "Fetch",
        Some("switch_mode") => "Mode",
        _ => return (title.to_owned(), None),
    };
    let (name, args) = match title.split_once(' ') {
        Some((verb, args)) if VERBS.contains(&verb) => (verb, args.trim()),
        None if VERBS.contains(&title) => (title, ""),
        _ => (name, title),
    };
    (name.to_owned(), (!args.is_empty()).then(|| args.to_owned()))
}

/// What [`render`] shows of a message before its text: a bullet, its route, whether forged, its kind unless a
/// message, post or note, its note, what it answers, its time and its id.
pub fn heading(e: &Entry) -> String {
    let forged = if e.forged { " (forged)" } else { "" };
    let note = e.note.as_ref().map_or_default(|n| format!(" [{n}]"));
    let answering = e.answer.as_ref().map_or_default(|a| {
        let picks: Vec<String> = a
            .picks
            .iter()
            .map(|p| match p {
                Pick::Options(numbers) if numbers.len() == 1 => format!("option {}", numbers[0]),
                Pick::Options(numbers) => {
                    let numbers: Vec<String> = numbers.iter().map(u32::to_string).collect();
                    format!("options {}", numbers.join(", "))
                }
                Pick::Typed(_) => "typed".to_owned(),
            })
            .collect();
        match picks.as_slice() {
            [typed] if typed == "typed" => format!(" [answers {} as typed]", a.question),
            _ => format!(" [answers {} with {}]", a.question, picks.join("; ")),
        }
    });
    let kind = match e.kind {
        Kind::Message | Kind::Post | Kind::Note => String::new(),
        kind => format!(" · {}", kind.as_str()),
    };
    let id = e.id.map_or_default(|id| format!(" · message {id}"));
    format!("● {}{forged}{kind}{note}{answering} · {}{id}", e.route(), time(e.stored_at))
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

/// The answers among `entries` to `question`, an entry asking questions, oldest first, each with who answered and
/// its pick for each question: options picked or a typed answer.
pub fn answers_to<'a>(
    question: &'a Entry,
    entries: &'a [Entry],
) -> impl Iterator<Item = (&'a Entry, Vec<Answered<'a>>)> {
    entries.iter().filter_map(move |e| Some((e, e.answers(question)?)))
}

/// How an answer is shown beneath the questions it answers: who answered with which options, or typed what; among
/// several questions, each named by its header, else its number.
pub(crate) fn answer_line(by: &Entry, answered: Vec<Answered<'_>>) -> String {
    let from = by.from.as_ref().map_or("(a human)", UserId::as_str);
    let picked = |a: &Answered<'_>| match a.pick {
        Pick::Typed(text) => format!("typed: {}", crate::text::excerpt(text, 72)),
        Pick::Options(_) => a.picked(),
    };
    match answered.as_slice() {
        [one] if matches!(one.pick, Pick::Typed(_)) => format!("answered by {from}, {}", picked(one)),
        [one] => format!("answered by {from}: {}", picked(one)),
        several => {
            let each: Vec<String> =
                (1..).zip(several).map(|(n, a)| format!("{} {}", a.question.title(n), picked(a))).collect();
            format!("answered by {from}: {}", each.join("; "))
        }
    }
}
