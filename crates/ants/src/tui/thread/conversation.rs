//! The conversation of an open thread, row by row: its messages, its ants' tool calls and the prompts they ended with
//! no reply in the order stored, how deep each nests, the tree drawn before them and the lines each row draws as.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as _;

use ants_proto::{NoticeKind, UserId, Uuid};
use chrono::{DateTime, Local, TimeDelta, Utc};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};

use crate::sealed;
use crate::thread::{Entry, Kind};
use crate::tui::model::Data;
use crate::tui::timeline::{Call, End, Tools};
use crate::tui::{attach, frame, images, markdown, react};

/// How deep delegations nest, deeper ones shown at this depth.
const MAX_DEPTH: usize = 4;
const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// What a row of the conversation is.
#[derive(Debug, Clone)]
pub(super) enum Item<'a> {
    Message(&'a Entry),
    Call(&'a Call),
    End(&'a End),
    /// An ant's tool calls and ends with no reply between two messages, on one line.
    Turn(Turn<'a>),
}

/// An ant's turn collapsed: its tool calls and the prompts it ended with no reply between two messages.
#[derive(Debug, Clone)]
pub(super) struct Turn<'a> {
    pub(super) ant: &'a UserId,
    pub(super) calls: Vec<&'a Call>,
    pub(super) ends: Vec<&'a End>,
}

impl Turn<'_> {
    /// When it started.
    fn started(&self) -> Option<DateTime<Utc>> {
        let calls = self.calls.iter().map(|c| c.started);
        calls.chain(self.ends.iter().map(|e| e.at)).min()
    }

    /// From when its first call started to when its last one ended.
    fn took(&self) -> Option<TimeDelta> {
        let first = self.calls.iter().map(|c| c.started).min()?;
        let last = self.calls.iter().map(|c| c.started + c.duration.unwrap_or_default()).max()?;
        Some(last - first)
    }
}

/// A row of the conversation, and how deep it nests: 0 for the top level.
#[derive(Debug)]
pub(super) struct Row<'a> {
    pub(super) item: Item<'a>,
    pub(super) depth: usize,
}

/// The conversation of `entries`, in the order stored, with `calls`, by when they started, and `ends`, by when they
/// ended, each before the first message stored after it, shown as `tools` has them; `busy` tells whether an ant runs
/// a prompt on the thread, the calls after the last message then the running turn's, and `human` tells the humans.
pub(super) fn rows<'a>(
    entries: &'a [Entry],
    calls: &'a [Call],
    ends: &'a [End],
    tools: Tools,
    busy: bool,
    human: impl Fn(&UserId) -> bool,
) -> Vec<Row<'a>> {
    let mut levels: HashMap<&UserId, usize> = HashMap::new();
    let mut timeline: Vec<(DateTime<Utc>, &UserId, Item)> = Vec::new();
    if tools != Tools::None {
        timeline.extend(calls.iter().map(|c| (c.started, &c.ant, Item::Call(c))));
        timeline.extend(ends.iter().map(|e| (e.at, &e.ant, Item::End(e))));
    }
    timeline.sort_by_key(|(at, ..)| *at);
    let mut timeline = timeline.into_iter().peekable();
    let mut rows = Vec::new();
    for entry in entries.iter().filter(|e| !e.kind.on_timeline()) {
        let stretch = std::iter::from_fn(|| timeline.next_if(|(at, ..)| *at < entry.stored_at));
        let stretch: Vec<_> = stretch.map(|(_, ant, item)| (ant, item)).collect();
        timed(&mut rows, &levels, stretch, tools == Tools::Collapsed);
        let ant = |user: &&UserId| !human(user);
        let depth = match (entry.kind, entry.from.as_ref(), entry.to.as_ref()) {
            // A daemon's notice nests nothing: it shows where its ant does.
            (Kind::Message, Some(from), _) if entry.notice.is_some() => level(&levels, from),
            (Kind::Message | Kind::Escalation, Some(from), Some(to)) if ant(&from) && ant(&to) => {
                let depth = level(&levels, from);
                // A reply to an ant nesting it leaves that ant where it was.
                if levels.get(to).is_none_or(|l| *l > depth) {
                    levels.insert(to, (depth + 1).min(MAX_DEPTH));
                }
                depth
            }
            (Kind::Note | Kind::Approval, Some(from), _) if ant(&from) => level(&levels, from),
            (Kind::Decision | Kind::Dead, _, Some(to)) if ant(&to) => level(&levels, to),
            _ => {
                for user in entry.from.iter().chain(&entry.to).filter(ant) {
                    levels.insert(user, 1);
                }
                0
            }
        };
        rows.push(Row { item: Item::Message(entry), depth });
    }
    let stretch: Vec<_> = timeline.map(|(_, ant, item)| (ant, item)).collect();
    // The running turn shows in full.
    let collapse = tools == Tools::Collapsed && !busy;
    timed(&mut rows, &levels, stretch, collapse);
    rows
}

fn level(levels: &HashMap<&UserId, usize>, user: &UserId) -> usize {
    levels.get(user).copied().unwrap_or(1)
}

/// Add the rows of the calls and ends of `stretch`, between two messages: each on its own, or with `collapse` one
/// [`Turn`] for each ant, where its first one was.
fn timed<'a>(
    rows: &mut Vec<Row<'a>>,
    levels: &HashMap<&UserId, usize>,
    stretch: Vec<(&'a UserId, Item<'a>)>,
    collapse: bool,
) {
    if !collapse {
        rows.extend(stretch.into_iter().map(|(ant, item)| Row { item, depth: level(levels, ant) }));
        return;
    }
    let mut turns: Vec<Turn> = Vec::new();
    for (ant, item) in stretch {
        let at = turns.iter().position(|t| t.ant == ant).unwrap_or_else(|| {
            turns.push(Turn { ant, calls: Vec::new(), ends: Vec::new() });
            turns.len() - 1
        });
        match item {
            Item::Call(call) => turns[at].calls.push(call),
            Item::End(end) => turns[at].ends.push(end),
            Item::Message(_) | Item::Turn(_) => {}
        }
    }
    rows.extend(turns.into_iter().map(|turn| Row { depth: level(levels, turn.ant), item: Item::Turn(turn) }));
}

/// The tree drawn before each of `rows`: before its first line, and before the others.
pub(super) fn glyphs(rows: &[Row]) -> Vec<(String, String)> {
    // Whether a row at each depth comes later, before any shallower one.
    let mut open = [false; MAX_DEPTH + 1];
    let mut out = vec![(String::new(), String::new()); rows.len()];
    for (at, row) in rows.iter().enumerate().rev() {
        let depth = row.depth.min(MAX_DEPTH);
        if depth > 0 {
            let (mut first, mut rest) = ("  ".to_owned(), "  ".to_owned());
            for on in &open[1..depth] {
                let bar = if *on { "│ " } else { "  " };
                first.push_str(bar);
                rest.push_str(bar);
            }
            first.push_str(if open[depth] { "├ " } else { "└ " });
            rest.push_str(if open[depth] { "│ " } else { "  " });
            out[at] = (first, rest);
        }
        open[depth] = true;
        open[depth + 1..].fill(false);
    }
    out
}

/// What the rows are drawn with.
pub(super) struct Shown<'a> {
    pub(super) data: &'a Data,
    pub(super) now: DateTime<Utc>,
    /// Whether an ant is busy on the thread: a call not ended is running only then.
    pub(super) busy: bool,
    pub(super) width: usize,
    /// The message whose attachments `o` opens, by its sequence, and the one marked among them.
    pub(super) marked: Option<(u64, usize)>,
    /// Where the images drawn go among the lines.
    pub(super) placed: RefCell<Vec<images::Placed>>,
}

/// Whether message `id` was withdrawn: here, or as the daemon's notice answering it among `entries` tells.
pub(super) fn withdrawn(data: &Data, entries: &[Entry], id: Uuid) -> bool {
    data.withdrawn.contains(&id)
        || entries.iter().any(|e| e.in_reply_to == Some(id) && e.notice == Some(NoticeKind::Withdrawn))
}

/// Whether the ant `ant` withdrew message `id`, as its daemon's notice answering it among `entries` tells.
pub(super) fn withdrawn_from(entries: &[Entry], ant: &UserId, id: Uuid) -> bool {
    entries
        .iter()
        .any(|e| e.in_reply_to == Some(id) && e.from.as_ref() == Some(ant) && e.notice == Some(NoticeKind::Withdrawn))
}

/// The ants channel post `e` woke, as [`ChannelEntry::woken`] tells by the channel as the human knows it now: the
/// members it @mentions other than humans, or, mentioning nobody, the channel's messenger. Of a channel unknown, the
/// ants it @mentions.
///
/// [`ChannelEntry::woken`]: ants_proto::ChannelEntry::woken
pub(super) fn woken(data: &Data, e: &Entry) -> Vec<UserId> {
    let channel = data.channels.iter().find(|c| Some(&c.channel) == e.channel.as_ref());
    if e.mentions.is_empty() {
        return channel.map(ants_proto::ChannelEntry::messenger).into_iter().collect();
    }
    let ant = |m: &&UserId| Some(*m) != e.from.as_ref() && !data.is_human(m) && channel.is_none_or(|c| c.is_member(m));
    e.mentions.iter().filter(ant).cloned().collect()
}

/// Whether the ant `ant` started on message `id`: a reply to it from the ant, other than a withdrawal's notice, is on
/// `entries`.
pub(super) fn started(entries: &[Entry], ant: &UserId, id: Uuid) -> bool {
    entries
        .iter()
        .any(|e| e.in_reply_to == Some(id) && e.from.as_ref() == Some(ant) && e.notice != Some(NoticeKind::Withdrawn))
}

impl Shown<'_> {
    /// What became of message `e` while it waits for its ant: withdrawn, or queued behind others.
    fn waiting(&self, e: &Entry, entries: &[Entry]) -> Option<Span<'static>> {
        let id = e.id?;
        if withdrawn(self.data, entries, id) {
            return Some(Span::styled("  withdrawn", Style::new().fg(Color::Yellow)));
        }
        let ahead = self.data.live.queued.get(&id)?;
        Some(Span::styled(format!("  queued · {ahead} ahead"), Style::new().fg(Color::Cyan)))
    }

    /// Add the lines of `row`, which `first` and `rest` begin; `entries` are the thread's, for a question's answers.
    pub(super) fn row(&self, lines: &mut Vec<Line<'static>>, row: &Row, entries: &[Entry], first: &str, rest: &str) {
        match &row.item {
            Item::Call(call) => self.call(lines, call, first),
            Item::End(end) => {
                let why = end.why.as_ref().map_or_default(|why| format!(": {why}"));
                let line = format!("{first}{} {} ended without a reply{why}", hm(end.at), end.ant);
                lines.push(Line::styled(cut(&line, self.width), frame::dim()));
            }
            Item::Message(e) if e.notice.is_some() => {
                let from = e.from.as_ref().map_or("ants", UserId::as_str);
                let text = e.text.strip_prefix("[ants] ").unwrap_or(&e.text).replace('\n', " ");
                let style = if e.notice.is_some_and(ants_proto::NoticeKind::needs_action) {
                    Style::new().fg(Color::Yellow)
                } else {
                    frame::dim()
                };
                let line = format!("{first}{} {from} · {text}", hm(e.stored_at));
                push(lines, &line, &format!("{rest}      "), style, self.width);
            }
            Item::Turn(turn) => self.turn(lines, turn, first),
            Item::Message(e) if row.depth == 0 => self.block(lines, e, entries),
            Item::Message(e) => {
                let (head, style) = nested(e);
                let face = e.from.as_ref().filter(|_| e.kind != Kind::Dead).and_then(|from| self.data.face(from));
                let mut first = Line::styled(format!("{first}{} ", hm(e.stored_at)), style);
                first.spans.extend(face.into_iter().flatten());
                first.spans.push(Span::styled(head, style));
                first.spans.extend(self.waiting(e, entries));
                let under = format!("{rest}      ");
                let mut text = text(e, entries, self.width.saturating_sub(under.chars().count()));
                let room = self.width.saturating_sub(first.width() + 2);
                // A text of one line short enough goes on the line of the heading.
                if let [line] = &mut text[..]
                    && line.width() <= room
                {
                    first.spans.push(Span::raw("  "));
                    first.spans.append(&mut line.spans);
                    text.clear();
                }
                lines.push(first);
                lines.extend(text.into_iter().map(|line| indented(&under, line).patch_style(style)));
                self.attachments(lines, e, &under);
                lines.extend(self.reactions(e).map(|line| indented(&under, line)));
            }
        }
    }

    /// The line of the reactions to `e`, if any.
    fn reactions(&self, e: &Entry) -> Option<Line<'static>> {
        let reacted = self.data.reactions.get(&e.id?)?;
        react::line(reacted, self.data.me())
    }

    /// A message at the top level: who sent it to whom and when, then its text under it.
    fn block(&self, lines: &mut Vec<Line<'static>>, e: &Entry, entries: &[Entry]) {
        let from = e.acting.label(e.from.as_ref().map_or("a human", UserId::as_str));
        let to = match (e.kind, &e.channel, &e.to) {
            (Kind::Post, Some(channel), _) => format!(" → #{channel}"),
            (Kind::Note, ..) => " → notes".to_owned(),
            (Kind::Approval, ..) => " → humans".to_owned(),
            (_, _, Some(to)) => format!(" → {to}"),
            _ => String::new(),
        };
        let from_style = if self.data.is_human(e.from.as_ref().unwrap_or(&UserId::human())) {
            Style::new().fg(Color::Cyan).bold()
        } else {
            Style::new().fg(Color::Magenta).bold()
        };
        let face = e.from.as_ref().and_then(|from| self.data.face(from));
        let mut head: Vec<Span<'static>> = face.into_iter().flatten().collect();
        head.extend([
            Span::styled(format!("{from}{to}"), from_style),
            Span::styled(format!("  {}", hm(e.stored_at)), frame::dim()),
        ]);
        let (what, style) = match e.kind {
            Kind::Escalation => {
                (format!("escalation: {}", e.note.as_deref().unwrap_or("")), Style::new().fg(Color::Red))
            }
            Kind::Mention => ("mention".to_owned(), frame::dim()),
            Kind::Approval => ("tool request".to_owned(), Style::new().fg(Color::Yellow)),
            Kind::Dead => (format!("gave up: {}", e.note.as_deref().unwrap_or("")), Style::new().fg(Color::Red)),
            _ if e.question.is_some() => ("asks".to_owned(), Style::new().fg(Color::Yellow)),
            _ => (String::new(), Style::new()),
        };
        if !what.is_empty() {
            head.push(Span::styled(format!("  {what}"), style));
        }
        head.extend(self.waiting(e, entries));
        if e.sealed.is_some() {
            head.push(Span::raw(format!("  {}", crate::tui::dm::LOCK)));
        }
        if e.forged || e.sealed == Some(sealed::State::Forged) {
            head.push(Span::styled("  forged", Style::new().fg(Color::Red).bold()));
        }
        lines.push(Line::from(head));
        lines.extend(text(e, entries, self.width.saturating_sub(2)).into_iter().map(|line| indented("  ", line)));
        self.attachments(lines, e, "  ");
        lines.extend(self.reactions(e).map(|line| indented("  ", line)));
    }

    /// The lines of `e`'s attachments, begun with `indent`, each image's drawn under its line.
    fn attachments(&self, lines: &mut Vec<Line<'static>>, e: &Entry, indent: &str) {
        let shown = attach::lines(&e.attachments, self.marked_of(e), indent);
        let mut placed = self.placed.borrow_mut();
        for (line, listed) in shown.into_iter().zip(&e.attachments) {
            lines.push(line);
            images::place(lines, &mut placed, &self.data.images, listed, indent.chars().count(), self.width);
        }
    }

    /// Which of `e`'s attachments is marked, when `o` opens one of them.
    fn marked_of(&self, e: &Entry) -> Option<usize> {
        self.marked.filter(|(seq, _)| *seq == e.seq).map(|(_, at)| at)
    }

    /// A turn collapsed: when it started, its ant, how many tool calls it made, how long they took and how many
    /// failed, and why it ended with no reply if it did.
    fn turn(&self, lines: &mut Vec<Line<'static>>, turn: &Turn, first: &str) {
        let at = turn.started().map_or_default(hm);
        let mut said = match turn.calls.len() {
            0 => String::new(),
            1 => "1 tool call".to_owned(),
            n => format!("{n} tool calls"),
        };
        if let Some(took) = turn.took().and_then(|d| u64::try_from(d.num_milliseconds()).ok()) {
            let _ = write!(said, ", {}", crate::thread::took(took));
        }
        let failed = turn.calls.iter().filter(|c| c.status.as_deref() == Some("failed")).count();
        let mut ended = String::new();
        if let Some(end) = turn.ends.last() {
            ended = if said.is_empty() { "ended without a reply" } else { ", ended without a reply" }.to_owned();
            if let Some(why) = &end.why {
                let _ = write!(ended, ": {why}");
            }
        }
        let head = format!("{first}{at} {} {said}", turn.ant);
        let failed = if failed > 0 { format!(", {failed} failed") } else { String::new() };
        let room = self.width.saturating_sub(head.chars().count() + failed.chars().count());
        lines.push(Line::from(vec![
            Span::styled(cut(&head, self.width), frame::dim()),
            Span::styled(cut(&failed, self.width.saturating_sub(head.chars().count())), Style::new().fg(Color::Red)),
            Span::styled(cut(&ended, room), frame::dim()),
        ]));
    }

    /// A tool call: when it started, its kind, its title, and how it ended or how long it has run.
    fn call(&self, lines: &mut Vec<Line<'static>>, call: &Call, first: &str) {
        let kind = call.kind.as_deref().unwrap_or("tool");
        let (status, style) = match call.status.as_deref() {
            Some("completed") => (format!("ok {}", took(call)), Style::new().fg(Color::Green)),
            Some("failed") => (format!("failed {}", took(call)), Style::new().fg(Color::Red)),
            _ if self.busy => {
                let elapsed = crate::colony::short(crate::colony::since(call.started, self.now));
                let spin = SPINNER[usize::try_from(self.now.timestamp()).unwrap_or(0) % SPINNER.len()];
                (format!("{spin} running {elapsed}"), busy())
            }
            Some(status) => (status.replace('_', " "), frame::dim()),
            None => ("started".to_owned(), frame::dim()),
        };
        let head = format!("{first}{} {kind:<7} ", hm(call.started));
        let room = self.width.saturating_sub(head.chars().count() + status.chars().count() + 2);
        lines.push(Line::from(vec![
            Span::styled(head, frame::dim()),
            Span::raw(format!("{:<room$}  ", cut(&call.title, room))),
            Span::styled(status, style),
        ]));
    }
}

/// How a message nested under another shows after its time: its route and what it is, and its style.
fn nested(e: &Entry) -> (String, Style) {
    let from = e.acting.label(e.from.as_ref().map_or("a human", UserId::as_str));
    let to = e.to.as_ref().map_or("", UserId::as_str);
    let lock = if e.sealed.is_some() { format!(" {}", crate::tui::dm::LOCK) } else { String::new() };
    let forged = if e.forged || e.sealed == Some(sealed::State::Forged) { " (forged)" } else { "" };
    let forged = format!("{lock}{forged}");
    match e.kind {
        Kind::Note => (format!("{from} → notes{forged}"), Style::new()),
        Kind::Approval => (format!("{from} asks to run{forged}"), Style::new().fg(Color::Yellow)),
        Kind::Decision => (format!("{from} decided for {to}"), frame::dim()),
        Kind::Dead => (format!("{to} gave up"), Style::new().fg(Color::Red)),
        Kind::Escalation => (format!("{from} → {to} escalation{forged}"), Style::new().fg(Color::Red)),
        _ => (format!("{from} → {to}{forged}"), Style::new()),
    }
}

/// A message's text: a question's with its options numbered and its answers.
pub(super) fn body(e: &Entry, entries: &[Entry]) -> String {
    if let Some(unopened) = crate::tui::dm::unopened(e.sealed) {
        return unopened.to_owned();
    }
    let Some(question) = &e.question else {
        return match (&e.note, e.kind) {
            (Some(note), Kind::Approval | Kind::Decision) => format!("{} [{note}]", e.text),
            _ => e.text.clone(),
        };
    };
    let mut text = question.text.clone();
    for (n, option) in question.options.iter().enumerate() {
        let _ = write!(text, "\n{} {option}", n + 1);
    }
    if question.free_text {
        text.push_str("\nor type an answer");
    }
    for (by, answered) in crate::thread::answers_to(e, entries) {
        text.push('\n');
        text.push_str(&crate::thread::answer_line(by, answered));
    }
    text
}

/// A message's text in lines at most `width` wide: as markdown for a message, post or note, else as it is; after a
/// dim line on the message it replies to, if it quotes one.
fn text(e: &Entry, entries: &[Entry], width: usize) -> Vec<Line<'static>> {
    let text = body(e, entries);
    let mut lines: Vec<_> = e.quote.iter().map(|q| Line::styled(cut(&q.line(), width), frame::dim())).collect();
    match e.kind {
        Kind::Message | Kind::Escalation | Kind::Mention | Kind::Post | Kind::Note if e.question.is_none() => {
            lines.extend(markdown::render(&text, width));
        }
        _ => {
            for line in text.lines() {
                push(&mut lines, line, "", Style::new(), width);
            }
        }
    }
    lines
}

/// `line` begun with `indent`, its style kept.
fn indented(indent: &str, line: Line<'static>) -> Line<'static> {
    let mut spans = vec![Span::raw(indent.to_owned())];
    spans.extend(line.spans);
    Line::from(spans).style(line.style)
}

/// Add `line`, cut to `width` characters a line, those after the first begun with `rest`.
fn push(lines: &mut Vec<Line<'static>>, line: &str, rest: &str, style: Style, width: usize) {
    let mut chars = line.chars().peekable();
    lines.push(Line::styled(chars.by_ref().take(width.max(1)).collect::<String>(), style));
    let room = width.saturating_sub(rest.chars().count()).max(1);
    while chars.peek().is_some() {
        lines.push(Line::styled(format!("{rest}{}", chars.by_ref().take(room).collect::<String>()), style));
    }
}

pub(super) fn busy() -> Style {
    Style::new().fg(Color::Yellow)
}

/// How long a call that ended took.
fn took(call: &Call) -> String {
    call.duration.and_then(|d| u64::try_from(d.num_milliseconds()).ok()).map_or_default(crate::thread::took)
}

/// The local hour and minute of `at`.
pub(super) fn hm(at: DateTime<Utc>) -> String {
    at.with_timezone(&Local).format("%H:%M").to_string()
}

/// `text` in at most `width` characters, an ellipsis ending it when cut.
pub(super) fn cut(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_owned();
    }
    let mut cut: String = text.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}
