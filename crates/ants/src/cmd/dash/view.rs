//! How `ants dash` draws its [`Model`]: a header with the connection and the age of what is shown, the threads, the
//! ants and the pending tool requests on the left, the selected thread's live progress and history on the right, a
//! footer with the keys or the latest notice, and an opened tool request over it all.

use ants_proto::{ApprovalRequest, ColonyEntry, ThreadId, Verdict};
use chrono::{DateTime, TimeDelta, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, List, ListItem, ListState, Paragraph, Wrap};

use super::model::{Detail, Level, Model, Pane};
use crate::live::Link;
use crate::threads::thread;
use crate::tui::style::{self, ago, block, dim, highlight};
use crate::{colony, tools};

/// How old the last read of the stream may be before it shows as stale: a few of the dashboard's periodic reads.
const STALE: TimeDelta = TimeDelta::seconds(15);
/// The most rows the ants and the tool requests each take.
const MOST_ROWS: u16 = 8;

/// `at` as `ants thread list` shows it, in UTC, without the year and seconds.
fn short_time(at: DateTime<Utc>) -> String {
    at.format("%m-%d %H:%M").to_string()
}

pub fn draw(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    let [header, body, footer] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(3), Constraint::Length(1)]).areas(frame.area());
    frame.render_widget(Paragraph::new(header_line(model, now)), header);
    let [left, right] = Layout::horizontal([Constraint::Percentage(40), Constraint::Percentage(60)]).areas(body);
    let rows = |n: usize| u16::try_from(n).unwrap_or(u16::MAX).clamp(1, MOST_ROWS) + 2;
    let [threads, ants, approvals] = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(rows(model.live.colony.len())),
        Constraint::Length(rows(model.live.approvals.len())),
    ])
    .areas(left);
    draw_threads(frame, model, threads, now);
    draw_ants(frame, model, ants, now);
    draw_approvals(frame, model, approvals, now);
    draw_thread(frame, model, right, now);
    frame.render_widget(Paragraph::new(footer_line(model, now)), footer);
    if let Some(detail) = &model.detail {
        draw_detail(frame, detail, now);
    }
}

/// Who the dashboard runs as, the connection, and how fresh the stream and colony read are.
fn header_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    let mut spans = vec![Span::styled(format!(" ants dash · {} · ", model.live.me), Style::new().bold())];
    spans.push(style::link(model.live.link, now, ": what shows may be stale"));
    let stored = match model.live.stored_at {
        Some(at) => {
            let stale = model.live.link != Link::Connected || now - at > STALE;
            let style = if stale { Style::new().fg(style::WARNING) } else { dim() };
            Span::styled(format!(" · stream read {}", ago(at, now)), style)
        }
        None => Span::styled(" · reading the stream…", dim()),
    };
    spans.push(stored);
    if let Some(at) = model.live.colony_at {
        spans.push(Span::styled(format!(" · colony read {}", ago(at, now)), dim()));
    }
    Line::from(spans)
}

fn footer_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    if let Some(notice) = model.notice_at(now) {
        return style::notice(&notice.text, notice.level == Level::Error);
    }
    let keys = match (&model.detail, model.focus) {
        (Some(_), _) => " a allow once · A allow always · r reject · Esc back · Ctrl-C quit",
        (None, Pane::Threads) => {
            " ↑↓ thread · PgUp/PgDn scroll · Enter reread · Tab tool requests · R refresh · q quit"
        }
        (None, Pane::Approvals) => " ↑↓ request · Enter open to decide · Tab threads · R refresh · q quit",
    };
    Line::styled(keys, dim())
}

fn draw_threads(frame: &mut Frame, model: &Model, area: Rect, now: DateTime<Utc>) {
    let since = match model.live.since.as_secs() {
        0 => "all kept".to_owned(),
        secs => format!("last {}", colony::short(std::time::Duration::from_secs(secs))),
    };
    let focused = model.focus == Pane::Threads && model.detail.is_none();
    let title = format!(" Threads ({}, {since}) ", model.live.order.len());
    let items: Vec<ListItem> = model.live.order.iter().map(|t| ListItem::new(thread_row(model, t, now))).collect();
    let selected = model.selected.as_ref().and_then(|s| model.live.order.iter().position(|t| t == s));
    let list = List::new(items).block(block(title, focused)).highlight_style(highlight(focused));
    if model.live.order.is_empty() {
        let text = if model.live.stored_at.is_some() { "no threads" } else { "reading the stream…" };
        frame.render_widget(Paragraph::new(text).block(block(" Threads ".to_owned(), focused)), area);
        return;
    }
    frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(selected));
}

/// A thread in the list: whether an ant is busy on it, its id's start, last activity, users, whether it is done and
/// its opening line.
fn thread_row(model: &Model, thread: &ThreadId, _now: DateTime<Utc>) -> Line<'static> {
    let busy = if model.live.progress.contains_key(thread) { "▶ " } else { "  " };
    let short: String = thread.as_str().chars().take(8).collect();
    let Some(summary) = model.live.summary(thread) else { return Line::raw(format!("{busy}{short}")) };
    let users: Vec<String> =
        summary.users.iter().map(ToString::to_string).chain(summary.channels.iter().map(|c| format!("#{c}"))).collect();
    let done = if model.done(thread).is_some() { " done" } else { "" };
    Line::from(vec![
        Span::styled(busy, Style::new().fg(Color::Green)),
        Span::raw(format!("{short} {} ", short_time(summary.last_at))),
        Span::styled(users.join(","), Style::new().fg(Color::Blue)),
        Span::styled(done, Style::new().fg(Color::Magenta)),
        Span::raw(format!(" {}", summary.opening)),
    ])
}

fn draw_ants(frame: &mut Frame, model: &Model, area: Rect, now: DateTime<Utc>) {
    let title = format!(" Ants ({}) ", model.live.colony.len());
    let lines: Vec<Line> = match model.live.colony_at {
        None => vec![Line::raw("reading the colony…")],
        Some(_) if model.live.colony.is_empty() => vec![Line::raw("no ants registered")],
        Some(_) => model.live.colony.iter().map(|e| ant_row(e, now)).collect(),
    };
    frame.render_widget(Paragraph::new(lines).block(block(title, false)), area);
}

fn ant_row(entry: &ColonyEntry, now: DateTime<Utc>) -> Line<'static> {
    let (state, color) = match colony::State::of(entry, now) {
        colony::State::Live => ("live ", style::CONNECTED),
        colony::State::Stale => ("stale", style::WARNING),
    };
    Line::from(vec![
        Span::raw(format!("{:<12} ", entry.ant)),
        Span::styled(state, Style::new().fg(color)),
        Span::raw(format!(" {} on {}", colony::activity(entry), entry.machine)),
    ])
}

fn draw_approvals(frame: &mut Frame, model: &Model, area: Rect, now: DateTime<Utc>) {
    let focused = model.focus == Pane::Approvals && model.detail.is_none();
    let title = format!(" Tool requests ({}) ", model.live.approvals.len());
    let mut block = block(title, focused);
    if !model.live.approvals.is_empty() {
        block = block.title_style(Style::new().fg(style::PENDING).bold());
    }
    if model.live.approvals.is_empty() {
        frame.render_widget(Paragraph::new("none pending").block(block), area);
        return;
    }
    let items: Vec<ListItem> = model.live.approvals.iter().map(|r| ListItem::new(approval_row(r, now))).collect();
    let list = List::new(items).block(block).highlight_style(highlight(focused));
    frame.render_stateful_widget(list, area, &mut ListState::default().with_selected(Some(model.approval)));
}

fn approval_row(request: &ApprovalRequest, now: DateTime<Utc>) -> String {
    let id: String = request.id.to_string().chars().take(8).collect();
    let left = tools::left(request.expires_at, now);
    format!("{}/{id} {}: {} ({left} left)", request.ant, request.tool.kind, request.tool.title.replace('\n', " "))
}

/// The selected thread: the ants' progress on it, then its messages, the latest at the bottom unless scrolled up.
fn draw_thread(frame: &mut Frame, model: &Model, area: Rect, now: DateTime<Utc>) {
    let Some(thread) = &model.selected else {
        frame.render_widget(Paragraph::new("no thread selected").block(block(" Thread ".to_owned(), false)), area);
        return;
    };
    let summary = model.live.summary(thread);
    let title = match summary.and_then(|s| s.owner.as_ref()) {
        Some(owner) => format!(" Thread {thread}, owned by {owner} "),
        None => format!(" Thread {thread} "),
    };
    let width = usize::from(area.width.saturating_sub(2));
    let mut lines = Vec::new();
    match model.live.progress_of(thread, width.saturating_sub(16)) {
        Some((line, at)) => {
            lines.push(Line::styled(format!("▶ {line} ({})", ago(at, now)), Style::new().fg(Color::Green)));
        }
        None => lines.push(Line::styled("no ant busy on it", dim())),
    }
    let mut scroll = 0;
    match &model.history {
        Some(history) if history.thread == *thread => {
            if history.loading {
                lines.push(Line::styled("reading the thread…", dim()));
            }
            let rendered = thread::render(&history.entries);
            lines.extend(rendered.lines().flat_map(|l| wrap(l, width)).map(Line::raw));
            scroll = model.scroll;
        }
        _ => {}
    }
    let height = usize::from(area.height.saturating_sub(2));
    let top = lines.len().saturating_sub(height).saturating_sub(scroll);
    let paragraph =
        Paragraph::new(lines).block(block(title, false)).scroll((u16::try_from(top).unwrap_or(u16::MAX), 0));
    frame.render_widget(paragraph, area);
}

/// `line` cut into lines of at most `width` characters, those after the first indented.
fn wrap(line: &str, width: usize) -> Vec<String> {
    const INDENT: &str = "    ";
    let mut chars = line.chars().peekable();
    let mut out = vec![chars.by_ref().take(width.max(1)).collect::<String>()];
    while chars.peek().is_some() {
        let rest: String = chars.by_ref().take(width.saturating_sub(INDENT.len()).max(1)).collect();
        out.push(format!("{INDENT}{rest}"));
    }
    out
}

/// An opened tool request: all of it as `ants tools show` prints it, and the keys deciding it or that its decision is
/// being stored.
fn draw_detail(frame: &mut Frame, detail: &Detail, now: DateTime<Utc>) {
    let area = frame.area();
    let [_, area, _] =
        Layout::vertical([Constraint::Percentage(15), Constraint::Percentage(70), Constraint::Percentage(15)])
            .areas(area);
    let [_, area, _] =
        Layout::horizontal([Constraint::Percentage(10), Constraint::Percentage(80), Constraint::Percentage(10)])
            .areas(area);
    let request = &detail.request;
    let mut lines: Vec<Line> =
        tools::details(request, &detail.state, now).lines().map(|l| Line::raw(l.to_owned())).collect();
    lines.push(Line::raw(""));
    let id = tools::qualified(request);
    let prompt = Style::new().fg(style::PENDING).bold();
    if detail.deciding {
        lines.push(Line::styled("storing the decision…", prompt));
    } else if detail.state == tools::State::Pending {
        let keys: Vec<&str> = [(Verdict::AllowOnce, "a allow once"), (Verdict::AllowAlways, "A allow always")]
            .into_iter()
            .filter(|(v, _)| request.options.contains(v))
            .map(|(_, key)| key)
            .chain(["r reject", "Esc back"])
            .collect();
        lines.push(Line::styled(keys.join(" · "), prompt));
    } else {
        lines.push(Line::styled("answered already · Esc back", prompt));
    }
    frame.render_widget(Clear, area);
    let block = Block::bordered().title(format!(" Tool request {id} ")).border_style(Style::new().fg(style::PENDING));
    frame.render_widget(Paragraph::new(lines).block(block).wrap(Wrap { trim: false }), area);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::cmd::dash::model::{Action, Key};
    use crate::live::State;
    use crate::test_support::live::{at, message, request, user};

    fn screen(model: &Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(140, 30, |frame| draw(frame, model, now))
    }

    #[test]
    fn a_thread_done_is_marked_until_a_message_reopens_it_and_a_failed_read_tells_once() {
        let (done, open) = (ThreadId::generate(), ThreadId::generate());
        let mut m = Model::new(State::new(user("alice"), Duration::from_hours(24)));
        m.stored(
            vec![message(1, &done, "alice", "dev-a", "deploy"), message(2, &open, "alice", "dev-b", "test")],
            at(2),
        );
        let read = [(done.clone(), ants_proto::ThreadDone::new(done.clone(), user("alice"), 1))].into();
        m.dones_read(Ok(read), at(3));
        let shown = screen(&m, at(3));
        assert!(shown.contains("alice,dev-a done deploy"), "{shown}");
        assert!(shown.contains("alice,dev-b test"), "{shown}");

        m.dones_read(Err(anyhow::anyhow!("timed out")), at(4));
        assert_eq!(m.notice_at(at(4)).map(|n| n.text.as_str()), Some("reading the done states: timed out"));
        m.notice = None;
        m.dones_read(Err(anyhow::anyhow!("timed out")), at(5));
        assert!(m.notice.is_none(), "told once");

        m.stored(vec![message(3, &done, "dev-a", "alice", "deployed")], at(6));
        assert!(screen(&m, at(6)).contains("alice,dev-a deploy"), "reopened");
    }

    #[test]
    fn long_lines_wrap_indented() {
        assert_eq!(wrap("abcdefghij", 6), ["abcdef", "    gh", "    ij"]);
        assert_eq!(wrap("", 6), [""]);
    }

    #[test]
    fn the_screen_shows_threads_history_requests_and_staleness() {
        let thread = ThreadId::generate();
        let mut m = Model::new(State::new(user("alice"), Duration::from_hours(24)));
        let start = m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(0));
        assert_eq!(start, Some(Action::LoadHistory(thread.clone())));
        m.history_loaded(&thread, Ok(vec![message(1, &thread, "alice", "dev-a", "deploy the site")]), at(1));
        m.approval(tools::Update::Pending(request("dev-a", "git push", vec![Verdict::AllowOnce])));
        let shown = screen(&m, at(2));
        assert!(shown.contains("● connected"), "{shown}");
        assert!(shown.contains("stream read 2s ago"), "{shown}");
        assert!(shown.contains("Threads (1, last 1d)"), "{shown}");
        assert!(shown.contains(&format!("Thread {thread}")), "{shown}");
        assert!(shown.contains("alice -> dev-a"), "{shown}");
        assert!(shown.contains("│  deploy the site "), "{shown}");
        assert!(shown.contains("Tool requests (1)"), "{shown}");
        assert!(shown.contains("dev-a/") && shown.contains(" execute: git push ("), "{shown}");

        m.link(false, at(3));
        let shown = screen(&m, at(30));
        assert!(shown.contains("○ disconnected 27s, reconnecting"), "{shown}");

        m.key(Key::Tab, at(30));
        m.key(Key::Enter, at(30));
        let shown = screen(&m, at(30));
        assert!(shown.contains("tool     execute: git push"), "{shown}");
        assert!(shown.contains("a allow once · r reject · Esc back"), "{shown}");
        assert!(matches!(m.key(Key::Char('a'), at(30)), Some(Action::Decide(_, Verdict::AllowOnce))));
        let shown = screen(&m, at(30));
        assert!(shown.contains("storing the decision…"), "{shown}");
    }
}
