//! The conversation of an open thread, row by row: its messages, its ants' tool calls, the prompts they ended with no
//! reply and the steps of the trail runs reporting on it in the order stored, how deep each nests, the indent and `⎿`
//! drawn before them and the lines each row draws as.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::Write as _;

use ants_proto::{NoticeKind, StepStage, UserId, Uuid};
use chrono::{DateTime, Local, TimeDelta, Utc};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use unicode_width::UnicodeWidthChar as _;

use crate::people::sealed;
use crate::threads::thread::{Entry, Kind};
use crate::tui::model::Data;
use crate::tui::steps::Step;
use crate::tui::timeline::{Call, End, Tools};
use crate::tui::{attach, images, markdown, react, style};

/// How deep delegations nest, deeper ones shown at this depth.
const MAX_DEPTH: usize = 4;
pub(super) const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
/// How long a spinner shows each frame, in milliseconds: 8 a second, or one a second with `[cli] animate = false`.
pub const SPIN_MS: i64 = 125;
/// Before a message, a turn or a tool call.
const BULLET: &str = "●";
/// Before a step of a trail run.
const STEP: &str = "◆";
/// Before a human's message at the top level.
const PROMPT: &str = "❯";
/// Joins the first of rows nested deeper to the row above them.
const UNDER: &str = "⎿ ";
/// How many of a running turn's latest calls show under it.
const LATEST: usize = 3;
/// The columns a time at the right edge takes, with the gap before it.
const STAMP: usize = 7;
/// The narrowest a line goes to leave room for its time.
const STAMPED_FROM: usize = 24;

/// What a row of the conversation is.
#[derive(Debug, Clone)]
pub(super) enum Item<'a> {
    Message(&'a Entry),
    Call(&'a Call),
    End(&'a End),
    /// An ant's tool calls and ends with no reply between two messages, on one line.
    Turn(Turn<'a>),
    /// A step of a trail run reporting on the thread, where its conductor's calls go.
    Step(&'a Step),
}

/// An ant's turn collapsed: its tool calls and the prompts it ended with no reply between two messages.
#[derive(Debug, Clone)]
pub(super) struct Turn<'a> {
    pub(super) ant: &'a UserId,
    pub(super) calls: Vec<&'a Call>,
    pub(super) ends: Vec<&'a End>,
    /// Whether it runs: after the last message, while an ant is busy on the thread, not ended.
    pub(super) running: bool,
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

/// What the thread's conversation interleaves with its messages: its ants' tool calls, by when they started, the
/// prompts they ended with no reply, by when they ended, and the steps of the trail runs reporting on it, by when they
/// started.
pub(super) struct Timed<'a> {
    pub(super) calls: &'a [Call],
    pub(super) ends: &'a [End],
    pub(super) steps: &'a [Step],
}

/// The conversation of `entries`, in the order stored, `with` what is timed, each before the first message stored
/// after it, the calls and ends shown as `tools` has them; `busy` tells whether an ant runs a prompt on the thread,
/// the turns after the last message then running, and `human` tells the humans.
pub(super) fn rows<'a>(
    entries: &'a [Entry],
    with: Timed<'a>,
    tools: Tools,
    busy: bool,
    human: impl Fn(&UserId) -> bool,
) -> Vec<Row<'a>> {
    let Timed { calls, ends, steps } = with;
    let mut levels: HashMap<&UserId, usize> = HashMap::new();
    let mut timeline: Vec<(DateTime<Utc>, &UserId, Item)> = Vec::new();
    if tools != Tools::None {
        timeline.extend(calls.iter().map(|c| (c.started, &c.ant, Item::Call(c))));
        timeline.extend(ends.iter().map(|e| (e.at, &e.ant, Item::End(e))));
    }
    timeline.extend(steps.iter().map(|s| (s.at, &s.conductor, Item::Step(s))));
    timeline.sort_by_key(|(at, ..)| *at);
    let mut timeline = timeline.into_iter().peekable();
    let mut rows = Vec::new();
    for entry in entries.iter().filter(|e| !e.kind.on_timeline()) {
        let stretch = std::iter::from_fn(|| timeline.next_if(|(at, ..)| *at < entry.stored_at));
        let stretch: Vec<_> = stretch.map(|(_, ant, item)| (ant, item)).collect();
        timed(&mut rows, &levels, stretch, tools == Tools::Collapsed, false);
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
    timed(&mut rows, &levels, stretch, tools == Tools::Collapsed, busy);
    rows
}

fn level(levels: &HashMap<&UserId, usize>, user: &UserId) -> usize {
    levels.get(user).copied().unwrap_or(1)
}

/// Add the rows of the calls, ends and steps of `stretch`, between two messages: each on its own, or with `collapse`
/// the calls and ends in one [`Turn`] for each ant, where its first one was, `running` when the stretch is the last
/// one and an ant is busy; a step always on its own.
fn timed<'a>(
    rows: &mut Vec<Row<'a>>,
    levels: &HashMap<&UserId, usize>,
    stretch: Vec<(&'a UserId, Item<'a>)>,
    collapse: bool,
    running: bool,
) {
    if !collapse {
        rows.extend(stretch.into_iter().map(|(ant, item)| Row { item, depth: level(levels, ant) }));
        return;
    }
    // Each ant's turn where its first call or end was, each step where it was.
    let mut order: Vec<Result<usize, (&UserId, Item)>> = Vec::new();
    let mut turns: Vec<Turn> = Vec::new();
    for (ant, item) in stretch {
        if let Item::Step(_) = item {
            order.push(Err((ant, item)));
            continue;
        }
        let at = turns.iter().position(|t| t.ant == ant).unwrap_or_else(|| {
            turns.push(Turn { ant, calls: Vec::new(), ends: Vec::new(), running: false });
            order.push(Ok(turns.len() - 1));
            turns.len() - 1
        });
        match item {
            Item::Call(call) => turns[at].calls.push(call),
            Item::End(end) => turns[at].ends.push(end),
            Item::Message(_) | Item::Turn(_) | Item::Step(_) => {}
        }
    }
    let mut turns: Vec<Option<Turn>> = turns.into_iter().map(Some).collect();
    rows.extend(order.into_iter().filter_map(|row| match row {
        Ok(at) => {
            let mut turn = turns[at].take()?;
            turn.running = running && turn.ends.is_empty();
            Some(Row { depth: level(levels, turn.ant), item: Item::Turn(turn) })
        }
        Err((ant, item)) => Some(Row { depth: level(levels, ant), item }),
    }));
}

/// What goes before each of `rows`: before its first line its bullet's indent, `⎿` joining it to the row above when
/// that one nests less deep; before its others the indent of its text.
pub(super) fn glyphs(rows: &[Row]) -> Vec<(String, String)> {
    let mut above = None;
    rows.iter()
        .map(|row| {
            let depth = row.depth.min(MAX_DEPTH);
            let first = match depth {
                0 => String::new(),
                _ if above.is_some_and(|above| above < depth) => format!("{}{UNDER}", " ".repeat(4 * depth - 2)),
                _ => " ".repeat(4 * depth),
            };
            above = Some(depth);
            (first, " ".repeat(4 * depth + 2))
        })
        .collect()
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
    /// The message whose card shows under the conversation, by its sequence: the conversation gives it one line.
    pub(super) carded: Option<u64>,
    /// Whether a spinner was drawn.
    pub(super) spun: Cell<bool>,
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

    /// Add the lines of `row`: `first` begins its first, `rest`, the indent of its text, the others; `entries` are the
    /// thread's, for a question's answers.
    pub(super) fn row(&self, lines: &mut Vec<Line<'static>>, row: &Row, entries: &[Entry], first: &str, rest: &str) {
        match &row.item {
            Item::Call(call) => {
                let mut line = Line::raw(first.to_owned());
                line.spans.extend(self.call(call, true));
                lines.push(self.stamped(line, call.started));
            }
            Item::End(end) => {
                let why = end.why.as_ref().map_or_default(|why| format!(": {why}"));
                let said = format!("{first}{BULLET} {}  ended without a reply{why}", end.ant);
                lines.push(self.stamped(Line::from(Span::styled(said, style::dim())), end.at));
            }
            Item::Step(step) => {
                let mut line = Line::raw(first.to_owned());
                line.spans.extend(self.step(step));
                lines.push(self.stamped(line, step.at));
            }
            Item::Message(e) if e.notice.is_some() => self.notice(lines, e, first, rest),
            Item::Message(e) if self.carded == Some(e.seq) => self.carded(lines, e, entries, first, row.depth == 0),
            Item::Turn(turn) => self.turn(lines, turn, first, rest),
            Item::Message(e) if row.depth == 0 => self.block(lines, e, entries),
            Item::Message(e) => self.nested(lines, e, entries, first, rest),
        }
    }

    /// The line of the reactions to `e`, if any.
    fn reactions(&self, e: &Entry) -> Option<Line<'static>> {
        let reacted = self.data.reactions.get(&e.id?)?;
        react::line(reacted, self.data.me())
    }

    /// The heading of message `e`: its bullet, a prompt for a human's at the `top` level, its sender's face, who sent
    /// it to whom, what it is and what became of it.
    fn heading(&self, e: &Entry, entries: &[Entry], top: bool) -> Vec<Span<'static>> {
        let from = e.acting.label(e.from.as_ref().map_or("a human", UserId::as_str));
        let to = match (e.kind, &e.channel, &e.to) {
            (Kind::Post, Some(channel), _) => format!(" → #{channel}"),
            (Kind::Note, ..) => " → notes".to_owned(),
            (Kind::Approval, ..) => " → humans".to_owned(),
            (_, _, Some(to)) => format!(" → {to}"),
            _ => String::new(),
        };
        let human = self.data.is_human(e.from.as_ref().unwrap_or(&UserId::human()));
        let who = Style::new().fg(if human { Color::Cyan } else { Color::Magenta });
        let (what, style) = match e.kind {
            Kind::Escalation => {
                (format!("escalation: {}", e.note.as_deref().unwrap_or("")), Style::new().fg(Color::Red))
            }
            Kind::Mention => ("mention".to_owned(), style::dim()),
            Kind::Approval => ("tool request".to_owned(), Style::new().fg(style::PENDING)),
            Kind::Decision => ("decision".to_owned(), style::dim()),
            Kind::Dead => (format!("gave up: {}", e.note.as_deref().unwrap_or("")), Style::new().fg(style::ERROR)),
            _ if e.question.is_some() => ("asks".to_owned(), Style::new().fg(Color::Yellow)),
            _ if let Some(provider) = &e.provider => (format!("wake from {provider}"), style::dim()),
            _ => (String::new(), Style::new()),
        };
        let tone = match e.kind {
            Kind::Escalation | Kind::Approval | Kind::Decision | Kind::Dead => style,
            _ if e.question.is_some() => style,
            _ => who,
        };
        let bullet = if top && human { PROMPT } else { BULLET };
        let face = e.from.as_ref().filter(|_| e.kind != Kind::Dead).and_then(|from| self.data.face(from));
        let mut head = vec![Span::styled(format!("{bullet} "), tone)];
        head.extend(face.into_iter().flatten());
        head.push(Span::styled(format!("{from}{to}"), who.bold()));
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
        head
    }

    /// A message at the top level: its heading and time, then its text under it.
    fn block(&self, lines: &mut Vec<Line<'static>>, e: &Entry, entries: &[Entry]) {
        lines.push(self.stamped(Line::from(self.heading(e, entries, true)), e.stored_at));
        lines.extend(text(e, entries, self.width.saturating_sub(2)).into_iter().map(|line| indented("  ", line)));
        self.attachments(lines, e, "  ");
        lines.extend(self.reactions(e).map(|line| indented("  ", line)));
    }

    /// A message whose card shows under the conversation: its heading after `first`, at the `top` level or not, and its
    /// text's first line, cut, on one line.
    fn carded(&self, lines: &mut Vec<Line<'static>>, e: &Entry, entries: &[Entry], first: &str, top: bool) {
        const BELOW: &str = " · card below";
        let mut head = Line::raw(first.to_owned());
        head.spans.extend(self.heading(e, entries, top));
        let text = e.text.lines().find(|l| !l.trim().is_empty()).unwrap_or_default();
        let room = self.width.saturating_sub(head.width() + 2 + BELOW.len() + STAMP);
        head.spans.push(Span::raw(format!("  {}", cut(text, room))));
        head.spans.push(Span::styled(BELOW, style::dim()));
        lines.push(self.stamped(head, e.stored_at));
    }

    /// A message nested under another: its heading after `first` and its time, and its text on the heading's line
    /// when one line short enough, else under it at `rest`.
    fn nested(&self, lines: &mut Vec<Line<'static>>, e: &Entry, entries: &[Entry], first: &str, rest: &str) {
        let tone = match e.kind {
            Kind::Approval => Style::new().fg(style::PENDING),
            Kind::Decision => style::dim(),
            Kind::Dead => Style::new().fg(style::ERROR),
            Kind::Escalation => Style::new().fg(Color::Red),
            _ => Style::new(),
        };
        let mut head = Line::raw(first.to_owned());
        head.spans.extend(self.heading(e, entries, false));
        let mut text = text(e, entries, self.width.saturating_sub(rest.chars().count()));
        let room = self.width.saturating_sub(head.width() + 2 + STAMP);
        if let [line] = &mut text[..]
            && line.width() <= room
        {
            head.spans.push(Span::raw("  "));
            head.spans.extend(line.spans.drain(..).map(|mut span| {
                span.style = tone.patch(span.style);
                span
            }));
            text.clear();
        }
        lines.push(self.stamped(head, e.stored_at));
        lines.extend(text.into_iter().map(|line| indented(rest, line).patch_style(tone)));
        self.attachments(lines, e, rest);
        lines.extend(self.reactions(e).map(|line| indented(rest, line)));
    }

    /// A daemon's notice where its ant is, dim, or yellow when the human has to act on it: on the line of its
    /// heading when short enough, else under it at `rest`.
    fn notice(&self, lines: &mut Vec<Line<'static>>, e: &Entry, first: &str, rest: &str) {
        let from = e.from.as_ref().map_or("ants", UserId::as_str);
        let text = e.text.strip_prefix("[ants] ").unwrap_or(&e.text).replace('\n', " ");
        let style =
            if e.notice.is_some_and(NoticeKind::needs_action) { Style::new().fg(Color::Yellow) } else { style::dim() };
        let head = format!("{first}{BULLET} {from}");
        if head.chars().count() + 2 + text.chars().count() + STAMP <= self.width {
            lines.push(self.stamped(Line::from(Span::styled(format!("{head}  {text}"), style)), e.stored_at));
        } else {
            lines.push(self.stamped(Line::from(Span::styled(head, style)), e.stored_at));
            push(lines, &text, rest, style, self.width);
        }
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

    /// A turn collapsed: its ant, what its calls did and how long they took, how many failed and why it ended with no
    /// reply if it did; a running one after a spinner, its latest calls under it at `rest`.
    fn turn(&self, lines: &mut Vec<Line<'static>>, turn: &Turn, first: &str, rest: &str) {
        let failed = turn.calls.iter().filter(|c| c.status.as_deref() == Some("failed")).count();
        let (bullet, tone) = if turn.running {
            (self.spinner(), busy())
        } else if failed > 0 {
            (BULLET, Style::new().fg(style::ERROR))
        } else if turn.calls.is_empty() {
            (BULLET, style::dim())
        } else {
            (BULLET, Style::new().fg(style::CONNECTED))
        };
        let mut said = Vec::new();
        if !turn.calls.is_empty() {
            let took = if turn.running {
                turn.started().map(|at| crate::colony::short(crate::colony::since(at, self.now)))
            } else {
                turn.took().and_then(|d| u64::try_from(d.num_milliseconds()).ok()).map(crate::threads::thread::took)
            };
            let took = took.map_or_default(|took| format!(" · {took}"));
            said.push(Span::styled(format!("{}{took}", summary(&turn.calls)), style::dim()));
        }
        if failed > 0 {
            said.push(Span::styled(format!("{failed} failed"), Style::new().fg(style::ERROR)));
        }
        if let Some(end) = turn.ends.last() {
            let why = end.why.as_ref().map_or_default(|why| format!(": {why}"));
            said.push(Span::styled(format!("ended without a reply{why}"), style::dim()));
        }
        let mut line = Line::from(vec![
            Span::raw(first.to_owned()),
            Span::styled(format!("{bullet} "), tone),
            Span::styled(turn.ant.to_string(), Style::new().fg(Color::Magenta)),
            Span::raw("  "),
        ]);
        for (n, span) in said.into_iter().enumerate() {
            if n > 0 {
                line.spans.push(Span::styled(" · ", style::dim()));
            }
            line.spans.push(span);
        }
        lines.push(match turn.started() {
            Some(at) => self.stamped(line, at),
            None => clip(line, self.width),
        });
        if turn.running {
            let latest = &turn.calls[turn.calls.len().saturating_sub(LATEST)..];
            for (n, call) in latest.iter().enumerate() {
                let mut line = Line::raw(if n == 0 { format!("{rest}{UNDER}") } else { format!("{rest}  ") });
                line.spans.extend(self.call(call, false));
                lines.push(clip(line, self.width));
            }
        }
    }

    /// A tool call as `Name(args)` and how it ended or how long it has run: after its bullet when `bullet`, and a
    /// running one after its spinner always.
    fn call(&self, call: &Call, bold: bool) -> Vec<Span<'static>> {
        let took = took(call);
        let (glyph, tone, status) = match call.status.as_deref() {
            Some("completed") => (BULLET, Style::new().fg(style::CONNECTED), (!took.is_empty()).then_some(took)),
            Some("failed") if took.is_empty() => (BULLET, Style::new().fg(style::ERROR), Some("failed".to_owned())),
            Some("failed") => (BULLET, Style::new().fg(style::ERROR), Some(format!("failed after {took}"))),
            _ if self.busy => {
                let elapsed = crate::colony::short(crate::colony::since(call.started, self.now));
                (self.spinner(), busy(), Some(format!("running {elapsed}")))
            }
            Some(status) => (BULLET, style::dim(), Some(status.replace('_', " "))),
            None => (BULLET, style::dim(), Some("started".to_owned())),
        };
        let mut spans = vec![Span::styled(format!("{glyph} "), tone)];
        let (name, args) = crate::threads::thread::call_label(call.kind.as_deref(), &call.title);
        spans.push(Span::styled(name, if bold { Style::new().bold() } else { Style::new() }));
        spans.extend(args.map(|args| Span::raw(format!("({args})"))));
        if let Some(status) = status {
            // A call that ended well shows how long it took dim; one that failed or runs, in its colour.
            let style = if call.status.as_deref() == Some("completed") { style::dim() } else { tone };
            spans.push(Span::styled(format!(" · {status}"), style));
        }
        spans
    }

    /// A step of a trail run: its call and label, the ant it went to, its stage in its colour, and its own thread.
    fn step(&self, step: &Step) -> Vec<Span<'static>> {
        let tone = match step.stage {
            StepStage::Started => Style::new().fg(Color::Cyan),
            StepStage::Answered => Style::new().fg(style::CONNECTED),
            StepStage::Failed => Style::new().fg(style::ERROR),
            StepStage::Dropped => style::dim(),
        };
        let mut spans = vec![
            Span::styled(format!("{STEP} "), tone),
            Span::styled(format!("step {}", step.call), Style::new().bold()),
        ];
        if let Some(label) = &step.label {
            spans.push(Span::raw(format!(" {}", label.split_whitespace().collect::<Vec<_>>().join(" "))));
        }
        spans.push(Span::styled(format!(" → {}", step.to), Style::new().fg(Color::Magenta)));
        spans.push(Span::styled(format!(" · {}", step.stage.as_str()), tone));
        spans.push(Span::styled(format!(" · thread {}", step.thread), style::dim()));
        spans
    }

    /// `line` with the local time of `at` dim at its right edge, cut to leave room for it; on a narrow view only cut.
    fn stamped(&self, line: Line<'static>, at: DateTime<Utc>) -> Line<'static> {
        if self.width < STAMPED_FROM + STAMP {
            return clip(line, self.width);
        }
        let mut line = clip(line, self.width - STAMP);
        let time = hm(at);
        let pad = self.width.saturating_sub(line.width() + time.chars().count());
        line.spans.push(Span::raw(" ".repeat(pad)));
        line.spans.push(Span::styled(time, style::dim()));
        line
    }

    fn spinner(&self) -> &'static str {
        self.spun.set(true);
        let frame =
            if self.data.looks.animates() { self.now.timestamp_millis() / SPIN_MS } else { self.now.timestamp() };
        SPINNER[usize::try_from(frame).unwrap_or(0) % SPINNER.len()]
    }
}

/// What `calls` did, by kind in the order each first came, other tools last: `Read 2 files, ran 1 command`.
pub(super) fn summary(calls: &[&Call]) -> String {
    const DID: [(&str, &str, &str); 7] = [
        ("read", "read", "file"),
        ("edit", "edited", "file"),
        ("delete", "deleted", "file"),
        ("move", "moved", "file"),
        ("search", "searched for", "pattern"),
        ("execute", "ran", "command"),
        ("fetch", "fetched", "page"),
    ];
    let mut counts: Vec<(Option<usize>, usize)> = Vec::new();
    for call in calls {
        let kind = DID.iter().position(|(kind, ..)| call.kind.as_deref() == Some(*kind));
        match counts.iter_mut().find(|(k, _)| *k == kind) {
            Some((_, n)) => *n += 1,
            None => counts.push((kind, 1)),
        }
    }
    counts.sort_by_key(|(kind, _)| kind.is_none());
    let alone = counts.len() == 1;
    let said: Vec<String> = counts
        .iter()
        .map(|&(kind, n)| {
            let (verb, what) = match kind {
                Some(at) => (DID[at].1, DID[at].2),
                None if alone => ("used", "tool"),
                None => ("used", "other tool"),
            };
            let s = if n == 1 { "" } else { "s" };
            format!("{verb} {n} {what}{s}")
        })
        .collect();
    let said = said.join(", ");
    let mut chars = said.chars();
    chars.next().map_or_default(|c| c.to_uppercase().chain(chars).collect())
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
    let several = question.questions.len() > 1;
    let mut text = String::new();
    for (n, asked) in (1..).zip(&question.questions) {
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        if several {
            let _ = write!(text, "{}: ", asked.title(n));
        }
        text.push_str(&asked.text);
        for (n, option) in (1..).zip(&asked.options) {
            let _ = write!(text, "\n{n} {}", option.line());
        }
        match (asked.multi_select, asked.free_text) {
            (true, true) => text.push_str("\npick one or more, or type an answer"),
            (true, false) => text.push_str("\npick one or more"),
            (false, true) => text.push_str("\nor type an answer"),
            (false, false) => {}
        }
    }
    for (by, answered) in crate::threads::thread::answers_to(e, entries) {
        text.push('\n');
        text.push_str(&crate::threads::thread::answer_line(by, answered));
    }
    text
}

/// A message's text in lines at most `width` wide: as markdown for a message, post or note, else as it is; after a
/// dim line on the message it replies to, if it quotes one.
fn text(e: &Entry, entries: &[Entry], width: usize) -> Vec<Line<'static>> {
    let text = body(e, entries);
    let mut lines: Vec<_> = e.quote.iter().map(|q| Line::styled(cut(&q.line(), width), style::dim())).collect();
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

/// Add `line`, wrapped at words into lines `width` columns wide begun with `indent`, a word too long cut.
fn push(lines: &mut Vec<Line<'static>>, line: &str, indent: &str, style: Style, width: usize) {
    let room = width.saturating_sub(indent.chars().count()).max(1);
    for row in markdown::wrap(&[Span::raw(line.to_owned())], room) {
        lines.push(indented(indent, Line::from(row)).style(style));
    }
}

pub(super) fn busy() -> Style {
    Style::new().fg(Color::Yellow)
}

/// How long a call that ended took.
fn took(call: &Call) -> String {
    call.duration.and_then(|d| u64::try_from(d.num_milliseconds()).ok()).map_or_default(crate::threads::thread::took)
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

/// `line` in at most `width` columns, an ellipsis ending it when cut.
fn clip(line: Line<'static>, width: usize) -> Line<'static> {
    if line.width() <= width {
        return line;
    }
    let style = line.style;
    let mut left = width.saturating_sub(1);
    let mut spans = Vec::new();
    for span in line.spans {
        let wide = span.width();
        if wide <= left {
            left -= wide;
            spans.push(span);
            continue;
        }
        let mut kept = String::new();
        for c in span.content.chars() {
            let wide = c.width().unwrap_or(0);
            if wide > left {
                break;
            }
            left -= wide;
            kept.push(c);
        }
        kept.push('…');
        spans.push(Span::styled(kept, span.style));
        break;
    }
    Line::from(spans).style(style)
}
