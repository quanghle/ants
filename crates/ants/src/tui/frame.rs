//! The frame around the views: a header with the human, the server, the place shown and the connection, the nav beside
//! the shown view (over it when folded and focused, [`super::fold`]) with the command bar under it, a footer with the
//! view's keys, the latest notice or the command bar's keys, and the help over it all.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};
use unicode_width::UnicodeWidthStr;

use super::model::{Focus, Level, Model, Page};
use super::style::{self, block, dim};
use super::{approvals, catchup, compact, home, nav, search, thread, work};

/// How wide the nav is.
const NAV_WIDTH: u16 = 24;

pub fn draw(frame: &mut Frame, model: &Model, now: DateTime<Utc>) {
    if model.compact {
        return compact::draw(frame, model, now);
    }
    let [header, body, footer] =
        Layout::vertical([Constraint::Length(1), Constraint::Min(3), Constraint::Length(1)]).areas(frame.area());
    frame.render_widget(Paragraph::new(header_line(model, now, header.width)), header);
    let width = frame.area().width;
    model.fold.drawn(width);
    let nav_focused = model.focus == Focus::Nav && !model.bar.focused;
    let right = if model.fold.nav(width) {
        let [left, right] = Layout::horizontal([Constraint::Length(NAV_WIDTH), Constraint::Min(20)]).areas(body);
        nav::draw(frame, left, &model.data, &model.nav, now, nav_focused);
        right
    } else {
        body
    };
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
    if nav_focused && !model.fold.nav(width) {
        let over = Rect { width: NAV_WIDTH.min(view.width), ..view };
        frame.render_widget(Clear, over);
        nav::draw(frame, over, &model.data, &model.nav, now, true);
    }
    frame.render_widget(Paragraph::new(footer_line(model, now)), footer);
    if let Some(asking) = model.asking.as_ref().filter(|a| a.full) {
        approvals::draw_full(frame, asking, now);
    }
    catchup::draw_digest(frame, model, now);
    if model.help {
        draw_help(frame);
    }
}

/// Who the UI runs as, on which server, the place shown, and the connection.
/// The header, its place cut first so the connection's state stays on screen.
fn header_line(model: &Model, now: DateTime<Utc>, width: u16) -> Line<'static> {
    let data = &model.data;
    let place = match model.page() {
        Page::Thread(page) => page.place(data),
        page => page.name().to_owned(),
    };
    let who = format!(" ants · {}@{}", data.me(), data.server);
    let link = style::link(data.live.link, now, "");
    let room = usize::from(width).saturating_sub(who.width() + link.width() + 6);
    let spans = vec![
        Span::styled(who, Style::new().bold()),
        Span::styled(format!(" · {} · ", style::fit(&place, room.max(1))), dim()),
        link,
    ];
    style::clip(Line::from(spans), usize::from(width))
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
    if let Some(notice) = model.notice_at(now) {
        return style::notice(&notice.text, notice.level == Level::Error);
    }
    if model.bar.focused && !model.completions().is_empty() {
        return Line::styled(" ↑↓ select · Tab/Enter complete · Esc close the list", dim());
    }
    if model.bar.focused {
        return Line::styled(" Enter send · Alt-Enter new line · Ctrl-E editor · Ctrl-K commands · Esc leave", dim());
    }
    let card = match model.page() {
        Page::Home(page) => page.card(&model.data, now).map(|(cursor, _)| cursor),
        Page::Thread(page) => page.card(&model.data, now),
        _ => None,
    };
    let keys = match (model.focus, model.page(), card) {
        (Focus::Nav, _, _) => "↑↓ select · Enter view",
        (Focus::View, _, Some(cursor)) => cursor.keys(),
        (Focus::View, Page::Home(_), _) => home::Page::KEYS,
        (Focus::View, Page::Work(_), _) => work::Page::KEYS,
        (Focus::View, Page::Channel(page), _) => page.keys(),
        (Focus::View, Page::Thread(_), _) => thread::Page::KEYS,
        (Focus::View, Page::Dm(page), _) => page.keys(),
        (Focus::View, Page::Search(_), _) => search::Page::KEYS,
        (Focus::View, Page::Tree(page), _) => page.keys(),
        (Focus::View, Page::Ant(page), _) => page.keys(),
        (Focus::View, Page::Credentials(page), _) => page.keys(),
        (Focus::View, Page::Held(page), _) => page.keys(),
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
    ("Ctrl-B Ctrl-\\", "fold or show the nav; a thread's side panel (again: as the width has it)"),
    ("w ←→ a", "on an ant: its threads in work, pane, ask; in a channel: ←→ its state tab"),
    ("[ ] \\", "on an ant: scroll its about or now column up, down; switch between the two"),
    ("Ctrl-K @ #", "the command bar: @ant asks, #channel posts, else reply"),
    ("/command", "in the bar: a command on the selection (Ctrl-K lists them)"),
    ("Alt-Enter Ctrl-E", "in the bar: a new line, the text in the editor"),
    ("↑↓ Tab", "in the bar: pick and complete an @ant, #channel or /command"),
    ("y p m e d z", "in a thread: copy, pin, mute, archive, done, snooze (again to undo)"),
    ("a A r n", "on a tool request: allow once, always, reject at once, or reject with a note to the ant"),
    ("1-9 Enter", "on a question: answer by number, or focus its card: ↑↓ ←→ Space Enter Esc"),
    ("g r f x d z s c o", "on home: grant, reply, forward, cancel, done, snooze, digest, credentials, held orders"),
    ("i > t S", "in a thread: reply, quote the selected, delegation tree, TL;DR"),
    ("+ f u b T", "in a thread: react, forward, unread from, remind; tool calls"),
    ("o ]", "in a thread or channel: open the marked attachment, mark the next"),
    ("x Ctrl-X", "in a thread or work: cancel the prompt running there"),
    ("W E", "in a thread: withdraw a waiting message or post, or to edit it"),
    ("x y", "in a delegation tree: cancel every prompt in the selected subtree"),
    ("n M", "with a person: write a message; in a channel: notify level, members"),
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

    use super::*;
    use crate::test_support::colony::entry;
    use crate::test_support::live::{at, message, user};
    use crate::tui::fold;
    use crate::tui::model::{Action, Key};
    use crate::tui::people;
    use crate::tui::test_support::model;

    fn screen(m: &Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(120, 32, |f| draw(f, m, now))
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
        assert!(shown.contains(" · deploy the site · ● connected"), "the header names the thread: {shown}");
        assert!(shown.contains("waiting on ant") && shown.contains("reading the thread…"), "{shown}");

        m.data.live.link(false, at(3));
        m.key(Key::Char('?'), at(3));
        let shown = screen(&m, at(30));
        assert!(shown.contains("○ disconnected 27s, reconnecting"), "{shown}");
        assert!(shown.contains("any key closes this help"), "{shown}");
    }

    /// How wide the open thread's conversation is on a screen `width` columns wide.
    fn conversation(m: &Model, width: u16) -> (usize, String) {
        let shown = crate::test_support::render::rendered(width, 24, |f| draw(f, m, at(2)));
        let row: Vec<char> = shown.lines().find(|l| l.contains("reading the thread…")).unwrap().chars().collect();
        let borders: Vec<usize> = row.iter().enumerate().filter(|(_, c)| **c == '│').map(|(at, _)| at).collect();
        let (left, right) = (borders[borders.len() - 2], borders[borders.len() - 1]);
        let left = if row[right - 1] == '│' { borders[borders.len() - 3] } else { left };
        (right - left - 1, shown)
    }

    #[test]
    fn the_conversation_takes_the_width_and_the_nav_and_the_side_panel_fold() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(1));
        m.select(nav::Item::Work(None));
        screen(&m, at(2));
        m.key(Key::Enter, at(2));
        let (wide, shown) = conversation(&m, 80);
        assert_eq!(wide, 78, "{shown}");
        assert!(!shown.contains("Work 1"), "the nav folds: {shown}");
        assert!(shown.contains("│waiting on ant · dev-a"), "the panel's state and ants on a line: {shown}");
        assert!(conversation(&m, 120).0 >= 94);
        let (_, shown) = conversation(&m, 140);
        assert!(shown.contains("STATE") && shown.contains("OWNER"), "the panel from 140 columns: {shown}");

        m.key(Key::Tab, at(2));
        let shown = crate::test_support::render::rendered(80, 24, |f| draw(f, &m, at(2)));
        assert!(shown.contains("│Work 1") && !shown.contains("reading the thread…"), "the nav over the view: {shown}");
        m.key(Key::Tab, at(2));

        let folds = |nav, panel| Some(Action::KeepFolds(fold::Folds { nav, panel }));
        assert_eq!(m.key(Key::Ctrl('b'), at(2)), folds(Some(true), None));
        let (wide, shown) = conversation(&m, 80);
        assert!(shown.contains("Work 1") && wide == 54, "{shown}");
        conversation(&m, 120);
        assert_eq!(m.key(Key::Ctrl('\\'), at(2)), folds(Some(true), Some(true)));
        let (_, shown) = conversation(&m, 120);
        assert!(shown.contains("STATE"), "the panel shown by key: {shown}");
        conversation(&m, 80);
        assert_eq!(m.key(Key::Ctrl('b'), at(2)), folds(None, Some(true)), "again: as the width has it");
    }

    #[test]
    fn no_text_ends_mid_word_at_the_edge_without_an_ellipsis() {
        let ops: ants_proto::ChannelId = "ops".parse().unwrap();
        let long = "deploy the site to production then update the docs and tell the whole team about it today";
        let opening = ants_proto::ChannelPost::new(ops.clone(), user("alice"), long);
        let thread = opening.thread_id.clone();
        let mut m = model();
        let asked = message(2, &thread, "dev-a", "alice", &format!("which region? {long}"));
        let mut first = crate::tui::test_support::post(1, &opening);
        first.owner = Some(user("alice"));
        m.stored(vec![first, asked], at(1));
        let cut = |shown: &str| {
            assert!(shown.lines().next().is_some_and(|l| l.ends_with("● connected")), "{shown}");
            let long = shown.lines().skip(1).filter(|l| l.contains("deploy") || l.contains("which"));
            long.map(|l| l.chars().collect::<Vec<_>>())
                .any(|row| row.len() >= 80 && if row[79] == '│' { row[78] } else { row[79] }.is_alphanumeric())
        };
        for item in [nav::Item::Home, nav::Item::Work(None), nav::Item::Channel(ops)] {
            m.select(item);
            let shown = crate::test_support::render::rendered(80, 24, |f| draw(f, &m, at(2)));
            assert!(!cut(&shown) && shown.contains('…'), "{shown}");
        }
        m.select(nav::Item::Work(None));
        screen(&m, at(2));
        m.key(Key::Enter, at(2));
        let shown = crate::test_support::render::rendered(80, 24, |f| draw(f, &m, at(2)));
        assert!(
            !cut(&shown)
                && shown.contains("┌ deploy the site to production then update the docs and tell the whol… · #ops ┐"),
            "{shown}"
        );
    }

    #[test]
    fn a_tool_request_shows_in_full_over_the_view_and_its_card_says_its_decision_is_being_stored() {
        let mut m = model();
        let push = crate::test_support::live::request("dev-a", "git push", vec![ants_proto::Verdict::AllowOnce]);
        m.data.live.approvals = vec![push.clone()];
        let shown = screen(&m, at(0));
        assert!(shown.contains("approve") && shown.contains("asks to run execute: git push"), "{shown}");
        m.key(Key::Enter, at(0));
        let shown = screen(&m, at(0));
        assert!(shown.contains(&format!(" Tool request {} ", crate::tools::qualified(&push))), "{shown}");
        assert!(
            shown.contains("a allow · r reject · n reject with a note · Enter open its thread · Esc back"),
            "{shown}"
        );
        m.key(Key::Esc, at(0));
        let decide = crate::tui::model::Action::Decide(Box::new(push), ants_proto::Verdict::AllowOnce);
        assert_eq!(m.key(Key::Char('a'), at(0)), Some(decide));
        let shown = screen(&m, at(0));
        assert!(shown.contains("storing the decision…"), "{shown}");
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
