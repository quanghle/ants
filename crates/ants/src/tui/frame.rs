//! The frame around the views: a header with the human, the server and the connection, the nav beside the shown view
//! with the command bar under it, a footer with the view's keys, the latest notice or the command bar's keys, and the
//! help over it all.

use chrono::{DateTime, Utc};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use super::model::{Focus, Level, Model, Page};
use super::style::{self, dim};
use super::{approvals, catchup, compact, keymap, nav};

/// How wide the nav is.
const NAV_WIDTH: u16 = 24;
/// The footer after a first Ctrl-C with nothing open and the bar empty.
pub const QUIT_AGAIN: &str = "Ctrl-C again to quit";

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
    frame.render_widget(Paragraph::new(footer_line(model, now, footer.width)), footer);
    if let Some(asking) = model.asking.as_ref().filter(|a| a.full) {
        approvals::draw_full(frame, asking, now);
    }
    catchup::draw_digest(frame, model, now);
    if let Some(help) = &model.help {
        help.draw(frame, model, now);
    }
}

/// Who the UI runs as, on which server, the view shown, and the connection.
fn header_line(model: &Model, now: DateTime<Utc>) -> Line<'static> {
    let data = &model.data;
    let mut spans = vec![
        Span::styled(format!(" ants · {}@{}", data.me(), data.server), Style::new().bold()),
        Span::styled(format!(" · {} · ", model.page().name()), dim()),
    ];
    spans.push(style::link(data.live.link, now, ""));
    Line::from(spans)
}

/// The footer at most `width` wide: a prompt taking the keys, the latest notice, the keys of the bar or of a focused
/// question card, a confirmation or draft of the view, else the hints of the key table ([`keymap::footer`]).
fn footer_line(model: &Model, now: DateTime<Utc>, width: u16) -> Line<'static> {
    if model.quitting_at(now) {
        return style::notice(QUIT_AGAIN, false);
    }
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
        return keymap::prompt("↑↓ select · Tab/Enter complete · Esc close the list", width, false);
    }
    if model.bar.focused {
        let keys = "Enter send · Alt-Enter new line · Ctrl-E editor · Ctrl-K commands · Esc leave";
        return keymap::prompt(keys, width, false);
    }
    if model.focus == Focus::View {
        let card = match model.page() {
            Page::Home(page) => page.card(&model.data, now).map(|(cursor, _)| cursor),
            Page::Thread(page) => page.card(&model.data, now),
            _ => None,
        };
        if let Some(cursor) = card {
            return keymap::prompt(cursor.keys(), width, false);
        }
        let (prompt, help) = match model.page() {
            Page::Channel(page) => (page.prompt(), false),
            Page::Dm(page) => (page.prompt(), false),
            Page::Ant(page) => (page.prompt(), page.armed()),
            Page::Tree(page) => (page.prompt(), true),
            Page::Credentials(page) => (page.prompt(), true),
            Page::Held(page) => (page.prompt(), true),
            _ => (None, false),
        };
        if let Some(keys) = prompt {
            return keymap::prompt(keys, width, help);
        }
    }
    keymap::footer(model, now, width)
}

#[cfg(test)]
mod tests {
    use ants_proto::ThreadId;

    use super::*;
    use crate::test_support::colony::entry;
    use crate::test_support::live::{at, message, user};
    use crate::tui::model::Key;
    use crate::tui::test_support::model;
    use crate::tui::{ant, people};

    fn screen(m: &Model, now: DateTime<Utc>) -> String {
        crate::test_support::render::rendered(120, 32, |f| draw(f, m, now))
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
        assert!(shown.contains(" Esc back · ↑↓ select · s digest · c creds · o held · Tab nav/view"), "{shown}");

        m.select(nav::Item::Work(None));
        m.key(Key::Enter, at(2));
        let shown = screen(&m, at(2));
        assert!(shown.contains("· thread ·"), "{shown}");
        assert!(shown.contains("waiting on ant") && shown.contains("reading the thread…"), "{shown}");

        m.data.live.link(false, at(3));
        m.key(Key::Char('?'), at(3));
        let shown = screen(&m, at(30));
        assert!(shown.contains("○ disconnected 27s, reconnecting"), "{shown}");
        assert!(shown.contains(crate::tui::help::CLOSE), "{shown}");
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

    fn text(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    /// A model showing each view in turn, the nav focused first, with a thread `thread` on it.
    fn every_view(thread: &ThreadId) -> Vec<Model> {
        let base = || {
            let mut m = model();
            m.data.live.colony(vec![entry("dev-a", 60, 5, at(0))], at(0));
            m.stored(vec![message(1, thread, "alice", "dev-a", "deploy the site")], at(1));
            m
        };
        let open = |page: Page| {
            let mut m = base();
            m.pages.push(page);
            m
        };
        let shown = |item: nav::Item| {
            let mut m = base();
            m.select(item);
            m.focus = Focus::View;
            m
        };
        let mut nav = base();
        nav.focus = Focus::Nav;
        let search = crate::tui::search::parse("deploy", at(1)).unwrap();
        let mut views = vec![
            nav,
            base(),
            shown(nav::Item::Work(None)),
            shown(nav::Item::Channel("ops".parse().unwrap())),
            shown(nav::Item::Person(user("bob"))),
            open(Page::Thread(crate::tui::thread::Page::new(thread.clone()))),
            open(Page::Search(crate::tui::search::Page::new("deploy".to_owned(), &search))),
            open(Page::Tree(crate::tui::tree::Page::new(thread.clone()))),
            open(Page::Credentials(crate::tui::credentials::Page::default())),
            open(Page::Held(crate::tui::held::Page::default())),
        ];
        for pane in [ant::Pane::Threads, ant::Pane::Queue, ant::Pane::Memories, ant::Pane::Mcp, ant::Pane::Settings] {
            let mut m = shown(nav::Item::Ant(user("dev-a")));
            let Some(Page::Ant(page)) = m.pages.last_mut() else { panic!("no ant view") };
            page.pane = pane;
            views.push(m);
        }
        views
    }

    #[test]
    fn every_footer_ends_in_help_and_cuts_no_hint() {
        for m in every_view(&ThreadId::generate()) {
            let rows: Vec<&keymap::Entry> =
                keymap::views(&m).into_iter().flat_map(keymap::entries).chain(keymap::GLOBAL).collect();
            let hints: Vec<String> = rows.iter().filter(|e| e.applies(&m, at(2)).is_ok()).map(|e| e.hint()).collect();
            for width in [80, 100, 120] {
                let footer = text(&footer_line(&m, at(2), width));
                assert!(Line::raw(footer.as_str()).width() <= usize::from(width), "{footer}");
                let shown = footer.strip_suffix(" · ? help").unwrap_or_else(|| panic!("{width}: {footer}"));
                for hint in shown.trim_start().split(" · ") {
                    assert!(hints.iter().any(|h| h == hint), "{}: {hint} cut in {footer}", m.page().name());
                }
            }
        }
    }

    #[test]
    fn a_first_ctrl_c_on_an_empty_bar_asks_for_a_second_for_a_second() {
        let mut m = model();
        m.key(Key::Interrupt, at(0));
        assert_eq!(text(&footer_line(&m, at(0), 100)).trim(), QUIT_AGAIN);
        assert!(text(&footer_line(&m, at(1), 100)).ends_with("? help"));
    }

    #[test]
    fn a_thread_shows_the_keys_of_a_tool_request_only_while_one_waits() {
        let thread = ThreadId::generate();
        let mut m = model();
        m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(1));
        m.pages.push(Page::Thread(crate::tui::thread::Page::new(thread.clone())));
        let footer = text(&footer_line(&m, at(2), 120));
        for hint in ["a allow", "A always", "r reject", "n note", "Enter details"] {
            assert!(!footer.contains(hint), "{footer}");
        }
        let push = crate::test_support::live::request("dev-a", "git push", vec![ants_proto::Verdict::AllowOnce]);
        m.data.live.approvals = vec![ants_proto::ApprovalRequest { thread_id: thread, ..push }];
        let footer = text(&footer_line(&m, at(2), 80));
        assert!(footer.starts_with(" a allow · A always · r reject · n note · Enter details · "), "{footer}");
        assert!(footer.ends_with(" · ? help"), "{footer}");
    }
}
