//! The frame around the views: a header with the human, the server and the connection, the nav beside the shown view
//! with the command bar under it, a footer with the view's keys, the latest notice or the command bar's keys, and the
//! help over it all. Also the styles the views share.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};

use super::model::{Focus, Level, Model, Page};
use super::{approvals, catchup, channel, compact, home, nav, search, thread, work};
use crate::colony;
use crate::live::Link;

/// How wide the nav is.
const NAV_WIDTH: u16 = 24;

/// `then` from `now`, such as `5s ago`.
pub fn ago(then: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!("{} ago", colony::short(colony::since(then, now)))
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

pub fn draw(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    if model.compact {
        return compact::draw(frame, model, now);
    }
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
        Page::Home(page) => page.draw(frame, view, model, now, focused),
        Page::Work(page) => page.draw(frame, view, data, now, focused),
        Page::Channel(page) => page.draw(frame, view, data, now, focused),
        Page::Thread(page) => page.draw(frame, view, model, now, focused),
        Page::Dm(page) => page.draw(frame, view, data, now, focused),
        Page::Search(page) => page.draw(frame, view, data, now, focused),
        Page::Tree(page) => page.draw(frame, view, data, now, focused),
        Page::Ant(page) => page.draw(frame, view, data, now, focused),
        Page::Credentials(page) => page.draw(frame, view, data, now, focused),
        Page::Held(page) => page.draw(frame, view, data, now, focused),
    }
    model.bar.draw(frame, bar, &model.completions());
    frame.render_widget(Paragraph::new(footer_line(model, now)), footer);
    if let Some(asking) = model.asking.as_ref().filter(|a| a.full) {
        approvals::draw_full(frame, asking, now);
    }
    catchup::draw_digest(frame, model, now);
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
            format!("○ disconnected {}, reconnecting", colony::short(colony::since(since, now))),
            Style::new().fg(Color::Red).bold(),
        )),
    }
    Line::from(spans)
}

fn footer_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    if let Some(picker) = &model.picking {
        return picker.line();
    }
    if let Some(confirm) = &model.interrupting {
        return confirm.line();
    }
    if let Some(prompt) = &model.snoozing {
        return prompt.line(&model.data);
    }
    if let Some(line) = model.asking.as_ref().and_then(approvals::Asking::footer) {
        return line;
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
        (Focus::View, Page::Tree(page)) => page.keys(),
        (Focus::View, Page::Ant(page)) => page.keys(),
        (Focus::View, Page::Credentials(page)) => page.keys(),
        (Focus::View, Page::Held(page)) => page.keys(),
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
    ("w ←→ a", "in a channel or on an ant: its threads in work; on an ant: pane, ask"),
    ("Ctrl-K @ #", "the command bar: @ant asks, #channel posts, else reply"),
    ("/command", "in the bar: a command on the selection (Ctrl-K lists them)"),
    ("Alt-Enter Ctrl-E", "in the bar: a new line, the text in the editor"),
    ("↑↓ Tab", "in the bar: pick and complete an @ant, #channel or /command"),
    ("y p m e d z", "in a thread: copy, pin, mute, archive, done, snooze (again to undo)"),
    ("a A r, y", "on a tool request: allow once, always or reject, then confirm"),
    ("1-9 r", "on a question: answer with that option, or type an answer"),
    ("g r f x d z s c o", "on home: grant, reply, forward, cancel, done, snooze, digest, credentials, held orders"),
    ("i > t S", "in a thread: reply, quote the selected, delegation tree, TL;DR"),
    ("+ f u b T", "in a thread: react, forward, unread from, remind; tool calls"),
    ("o ]", "in a thread or channel: open the marked attachment, mark the next"),
    ("x Ctrl-X", "in a thread or work: cancel the prompt running there"),
    ("W E", "in a thread: withdraw a waiting message or post, or to edit it"),
    ("x y", "in a delegation tree: cancel every prompt in the selected subtree"),
    ("n", "with a person: write a message; in a channel: its next notify level"),
    ("/", "the bar on /search, as in: /search deploy from:dev-a"),
    ("R ?", "read everything again, this help"),
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
    let area = centered(frame.area(), 90, u16::try_from(lines.len()).unwrap_or(u16::MAX) + 2);
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
    use crate::colony::tests::entry;
    use crate::live::tests::{at, message, user};
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
    fn the_help_covers_every_key_of_the_thread_view() {
        for key in thread::Page::KEYS.split(" · ").filter_map(|hint| hint.split(' ').next()) {
            let covered = HELP.iter().any(|(keys, _)| keys.split(' ').any(|k| k.trim_end_matches(',') == key));
            assert!(covered, "the help lacks {key}");
        }
    }

    #[test]
    fn the_screen_shows_who_where_the_nav_the_view_and_its_keys() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.data.live.colony(vec![entry("dev-a", 60, 5, at(0))], at(0));
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(1));
        let shown = screen(&m, at(2));
        assert!(shown.contains(" ants · alice@hub:4222 · home · ● connected"), "{shown}");
        assert!(shown.contains("Work 1"), "{shown}");
        assert!(shown.contains("waiting on ant 1"), "{shown}");
        assert!(shown.contains("● {.~.} dev-a") && shown.contains("PEOPLE") && shown.contains("bob"), "{shown}");
        assert!(shown.contains("nothing waits on a human"), "{shown}");
        let keys = "j/k move · Enter open · f forward · d done · z snooze · u unread · b remind · t tree · s digest · c \
                    creds · Tab";
        assert!(shown.contains(keys), "{shown}");

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
    fn a_tool_request_shows_in_full_over_the_view_and_its_verdict_waits_in_the_footer() {
        let mut m = model();
        let push = crate::live::tests::request("dev-a", "git push", vec![ants_proto::Verdict::AllowOnce]);
        m.data.live.approvals = vec![push.clone()];
        let shown = screen(&m, at(0));
        assert!(shown.contains("approve") && shown.contains("asks to run execute: git push"), "{shown}");
        m.key(Key::Enter, at(0));
        let shown = screen(&m, at(0));
        assert!(shown.contains(&format!(" Tool request {} ", crate::tools::qualified(&push))), "{shown}");
        assert!(shown.contains("a allow · r reject · Enter open its thread · Esc back"), "{shown}");
        m.key(Key::Esc, at(0));
        m.key(Key::Char('a'), at(0));
        let shown = screen(&m, at(0));
        assert!(shown.contains("with allow-once? y confirms, any other key cancels"), "{shown}");
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
        assert!(shown.contains("/approve   [ID] [--always]   ") && shown.contains("/reject"), "{shown}");
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
