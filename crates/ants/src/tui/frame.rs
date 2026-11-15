//! The frame around the views: a header with the human, the server and the connection, the nav beside the shown view,
//! a footer with the view's keys, the latest notice or the search prompt, and the help over it all. Also the styles the views share.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};

use super::model::{Focus, Level, Model, Page};
use super::{channel, home, nav, search, thread, work};
use crate::live::Link;
use crate::roster;

/// How wide the nav is.
const NAV_WIDTH: u16 = 24;

/// `then` from `now`, such as `5s ago`.
pub fn ago(then: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!("{} ago", roster::short(roster::since(then, now)))
}

pub fn dim() -> Style {
    Style::new().add_modifier(Modifier::DIM)
}

/// A bordered block titled `title`, its border bright when `focused`.
pub fn block(title: String, focused: bool) -> Block<'static> {
    let style = if focused { Style::new().fg(Color::Cyan) } else { dim() };
    Block::bordered().title(title).border_style(style)
}

/// The selected row's style.
pub fn highlight(focused: bool) -> Style {
    if focused { Style::new().add_modifier(Modifier::REVERSED) } else { Style::new().add_modifier(Modifier::BOLD) }
}

/// `line` cut into lines of at most `width` characters, those after the first indented.
pub fn wrap(line: &str, width: usize) -> Vec<String> {
    const INDENT: &str = "    ";
    let mut chars = line.chars().peekable();
    let mut out = vec![chars.by_ref().take(width.max(1)).collect::<String>()];
    while chars.peek().is_some() {
        let rest: String = chars.by_ref().take(width.saturating_sub(INDENT.len()).max(1)).collect();
        out.push(format!("{INDENT}{rest}"));
    }
    out
}

pub fn draw(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    let [header, body, footer] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(3), Constraint::Length(1)]).areas(frame.area());
    frame.render_widget(Paragraph::new(header_line(model, now)), header);
    let [left, right] = Layout::horizontal([Constraint::Length(NAV_WIDTH), Constraint::Min(20)]).areas(body);
    nav::draw(frame, left, &model.data, &model.nav, now, model.focus == Focus::Nav && !model.bar.focused);
    let [view, bar] =
        Layout::vertical([Constraint::Min(3), Constraint::Length(model.bar.height(right.width))]).areas(right);
    let focused = model.focus == Focus::View && !model.bar.focused;
    let data = &model.data;
    match model.page() {
        Page::Home(page) => page.draw(frame, view, data, now, focused),
        Page::Work(page) => page.draw(frame, view, data, now, focused),
        Page::Channel(page) => page.draw(frame, view, data, now, focused),
        Page::Thread(page) => page.draw(frame, view, data, now, focused),
        Page::Dm(page) => page.draw(frame, view, data, now, focused),
        Page::Search(page) => page.draw(frame, view, data, now, focused),
    }
    model.bar.draw(frame, bar, &model.completions());
    frame.render_widget(Paragraph::new(footer_line(model, now)), footer);
    if model.help {
        draw_help(frame);
    }
}

/// Who the UI runs as, on which server, the view shown, and the connection.
fn header_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    let data = &model.data;
    let mut spans = vec![
        Span::styled(format!(" ants · {}@{}", data.me(), data.server), Style::new().bold()),
        Span::styled(format!(" · {} · ", model.page().name()), dim()),
    ];
    match data.live.link {
        Link::Connected => spans.push(Span::styled("● connected", Style::new().fg(Color::Green))),
        Link::Down(since) => spans.push(Span::styled(
            format!("○ disconnected {}, reconnecting", roster::short(roster::since(since, now))),
            Style::new().fg(Color::Red).bold(),
        )),
    }
    Line::from(spans)
}

fn footer_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    if let Some(prompt) = &model.prompt {
        return prompt.line();
    }
    if let Some(notice) = model.notice_at(now) {
        let style = match notice.level {
            Level::Error => Style::new().fg(Color::Red),
            Level::Info => Style::new().fg(Color::Cyan),
        };
        return Line::styled(format!(" {}", notice.text), style);
    }
    if model.bar.focused && !model.completions().is_empty() {
        return Line::styled(" ↑↓ select · Tab/Enter complete · Esc close the list", dim());
    }
    if model.bar.focused {
        return Line::styled(" Enter send · Alt-Enter new line · Ctrl-E editor · Ctrl-K commands · Esc leave", dim());
    }
    let keys = match (model.focus, model.page()) {
        (Focus::Nav, _) => "↑↓ select · Enter view",
        (Focus::View, Page::Home(_)) => home::Page::KEYS,
        (Focus::View, Page::Work(_)) => work::Page::KEYS,
        (Focus::View, Page::Channel(_)) => channel::Page::KEYS,
        (Focus::View, Page::Thread(_)) => thread::Page::KEYS,
        (Focus::View, Page::Dm(page)) => page.keys(),
        (Focus::View, Page::Search(_)) => search::Page::KEYS,
    };
    Line::styled(format!(" {keys} · Tab nav/view · / search · ? help · q quit"), dim())
}

/// The keys, over the middle of the screen.
const HELP: &[(&str, &str)] = &[
    ("Tab", "move between the nav and the view"),
    ("↑↓ j k", "move the selection, or scroll a thread"),
    ("PgUp PgDn Home End", "move further"),
    ("Enter", "open the selection"),
    ("Esc", "back to the previous view, else to the nav"),
    ("w", "in a channel: its threads in work"),
    ("Ctrl-K @ #", "the command bar: @ant asks, #channel posts, else reply"),
    ("/", "in the bar: a command on the selection (Ctrl-K lists them)"),
    ("Alt-Enter Ctrl-E", "in the bar: a new line, the text in the editor"),
    ("↑↓ Tab", "in the bar: pick and complete an @ant, #channel or /command"),
    ("n", "with a person: write a message, starting a thread"),
    ("/", "search messages, as in: deploy from:dev-a"),
    ("R", "read everything again"),
    ("?", "this help"),
    ("q Ctrl-C", "quit"),
];

fn draw_help(frame: &mut Frame) {
    let lines: Vec<Line> = HELP
        .iter()
        .map(|(keys, what)| {
            Line::from(vec![Span::styled(format!("{keys:>20}  "), Style::new().bold()), Span::raw(*what)])
        })
        .chain([Line::raw(""), Line::styled("any key closes this help", dim())])
        .collect();
    let area = centered(frame.area(), 68, u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2);
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(lines).block(block(" Keys ".to_owned(), true)), area);
}

/// An area of at most `width` by `height` in the middle of `area`.
fn centered(area: Rect, width: u16, height: u16) -> Rect {
    let (width, height) = (width.min(area.width), height.min(area.height));
    Rect { x: area.x + (area.width - width) / 2, y: area.y + (area.height - height) / 2, width, height }
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::live::tests::{at, message, user};
    use crate::roster::tests::entry;
    use crate::tui::model::Key;
    use crate::tui::model::tests::model;
    use crate::tui::people;

    fn screen(m: &Model, now: DateTime<Utc>) -> String {
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|f| draw(f, m, now)).unwrap();
        let buffer = terminal.backend().buffer();
        let width = usize::from(buffer.area.width);
        let cells: Vec<&str> = buffer.content.iter().map(ratatui::buffer::Cell::symbol).collect();
        cells.chunks(width).map(|row| row.concat().trim_end().to_owned()).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn the_screen_shows_who_where_the_nav_the_view_and_its_keys() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.data.live.roster(vec![entry("dev-a", 60, 5, at(0))], at(0));
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(1));
        let shown = screen(&m, at(2));
        assert!(shown.contains(" ants · alice@hub:4222 · home · ● connected"), "{shown}");
        assert!(shown.contains("Work 1"), "{shown}");
        assert!(shown.contains("waiting on ant 1"), "{shown}");
        assert!(shown.contains("● dev-a") && shown.contains("PEOPLE") && shown.contains("bob"), "{shown}");
        assert!(shown.contains("nothing waits on a human"), "{shown}");
        assert!(shown.contains("Enter open its thread · Tab nav/view · / search · ? help · q quit"), "{shown}");

        m.select(nav::Item::Work(None));
        m.key(Key::Enter, at(2));
        let shown = screen(&m, at(2));
        assert!(shown.contains("· thread ·"), "{shown}");
        assert!(shown.contains("waiting on ant") && shown.contains("reading the thread…"), "{shown}");

        m.data.live.link(false, at(3));
        m.key(Key::Char('?'), at(3));
        let shown = screen(&m, at(30));
        assert!(shown.contains("○ disconnected 27s, reconnecting"), "{shown}");
        assert!(shown.contains("any key closes this help"), "{shown}");
    }

    #[test]
    fn the_command_bar_shows_its_hint_then_its_text_status_and_keys() {
        let mut m = model();
        let shown = screen(&m, at(0));
        assert!(shown.contains("Ctrl-K commands · @ ant · # channel"), "{shown}");
        m.key(Key::Char('@'), at(0));
        let shown = screen(&m, at(0));
        assert!(shown.contains("@bob") && shown.contains("human"), "{shown}");
        assert!(shown.contains("↑↓ select · Tab/Enter complete · Esc close the list"), "{shown}");
        for c in "dev-a".chars() {
            m.key(Key::Char(c), at(0));
        }
        m.key(Key::Enter, at(0));
        let shown = screen(&m, at(0));
        assert!(shown.contains("@dev-a"), "{shown}");
        assert!(shown.contains("nothing to send to @dev-a"), "{shown}");
        assert!(shown.contains("Enter send · Alt-Enter new line"), "{shown}");
        m.key(Key::Ctrl('k'), at(0));
        let shown = screen(&m, at(0));
        assert!(shown.contains("/approve   [ID] [--always]   ") && shown.contains("/quit"), "{shown}");
    }

    #[test]
    fn a_person_shows_online_and_opens_the_dm_with_its_keys() {
        let mut m = model();
        let bob = user("bob");
        m.stored(vec![message(1, &ThreadId::generate(), "alice", "dev-a", "deploy the site")], at(0));
        m.people(people::Event::Online(Ok([bob.clone()].into())), at(0));
        assert!(screen(&m, at(1)).contains("● bob"));

        m.select(nav::Item::Person(bob));
        let shown = screen(&m, at(1));
        assert!(shown.contains(" bob · online "), "{shown}");
        assert!(shown.contains("no messages with bob yet: n writes one"), "{shown}");
        assert!(shown.contains("Enter open · n new message"), "{shown}");
    }
}
