use ants_proto::ThreadId;

use super::*;
use crate::test_support::colony::entry;
use crate::test_support::live::{at, message, user};
use crate::tui::fold;
use crate::tui::model::{Action, Key};
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
    assert!(!shown.contains("waiting on ant 1"), "the states fold off work: {shown}");
    assert!(shown.contains("● {.~.} dev-a") && shown.contains("PEOPLE") && shown.contains("bob"), "{shown}");
    assert!(shown.contains("nothing waits on a human"), "{shown}");
    assert!(shown.contains(" Esc back · ↑↓ select · s digest · c creds · o held · Tab nav/view"), "{shown}");

    m.select(nav::Item::Work(None));
    assert!(screen(&m, at(2)).contains("waiting on ant 1"), "and show on it");
    m.key(Key::Enter, at(2));
    let shown = screen(&m, at(2));
    assert!(shown.contains(" · deploy the site · ● connected"), "the header names the thread: {shown}");
    assert!(shown.contains("waiting on ant") && shown.contains("reading the thread…"), "{shown}");

    m.data.live.link(false, at(3));
    let shown = screen(&m, at(30));
    assert!(shown.contains("○ disconnected 27s, reconnecting"), "{shown}");
    m.key(Key::Char('?'), at(3));
    let shown = screen(&m, at(30));
    assert!(shown.contains(crate::tui::help::CLOSE), "{shown}");
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
    assert_eq!(m.key(Key::Ctrl('o'), at(2)), folds(Some(true), None));
    let (wide, shown) = conversation(&m, 80);
    assert!(shown.contains("Work 1") && wide == 54, "{shown}");
    conversation(&m, 120);
    assert_eq!(m.key(Key::Ctrl('\\'), at(2)), folds(Some(true), Some(true)));
    let (_, shown) = conversation(&m, 120);
    assert!(shown.contains("STATE"), "the panel shown by key: {shown}");
    conversation(&m, 80);
    assert_eq!(m.key(Key::Ctrl('o'), at(2)), folds(None, Some(true)), "again: as the width has it");
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
        // A row cut with an ellipsis may still end at the edge with what follows the cut, as home's age.
        let long = shown.lines().skip(1).filter(|l| (l.contains("deploy") || l.contains("which")) && !l.contains('…'));
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
    assert!(
        shown.contains("approve") && shown.contains("dev-a asks to run:") && shown.contains("execute: git push"),
        "{shown}"
    );
    m.key(Key::Enter, at(0));
    let shown = screen(&m, at(0));
    assert!(shown.contains(&format!(" Tool request {} ", crate::tools::qualified(&push))), "{shown}");
    assert!(shown.contains("a allow · r reject · n reject with a note · Enter open its thread · Esc back"), "{shown}");
    m.key(Key::Esc, at(0));
    let decide = crate::tui::model::Action::Decide(Box::new(push), ants_proto::Verdict::AllowOnce);
    assert_eq!(m.key(Key::Char('a'), at(0)), Some(decide));
    let shown = screen(&m, at(0));
    assert!(shown.contains("storing the decision…"), "{shown}");
}

#[test]
fn with_no_thread_read_home_says_what_to_type_and_the_bar_what_enter_does() {
    let mut m = model();
    m.stored(vec![], at(0));
    let shown = screen(&m, at(0));
    assert!(shown.contains("@ant and a text") && shown.contains("Ctrl-K and a text"), "{shown}");
    assert!(shown.contains("Enter asks the assistant or the messenger · / commands · @ ant"), "{shown}");
    let thread = ThreadId::generate();
    m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(1));
    let shown = screen(&m, at(2));
    assert!(shown.contains("nothing waits on a human") && !shown.contains("@ant and a text"), "{shown}");
    m.select(nav::Item::Work(None));
    screen(&m, at(2));
    m.key(Key::Enter, at(2));
    assert!(screen(&m, at(2)).contains("Enter replies on the thread · / commands"));
    m.select(nav::Item::Channel("ops".parse().unwrap()));
    assert!(screen(&m, at(2)).contains("Enter starts a thread in #ops · / commands"));
}

#[test]
fn the_command_bar_shows_its_hint_then_its_text_status_and_keys() {
    let mut m = model();
    let shown = screen(&m, at(0));
    assert!(shown.contains("/ commands · @ ant · # channel"), "{shown}");
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
    assert!(shown.contains("Enter send · Ctrl-J new line"), "{shown}");
    m.key(Key::Ctrl('u'), at(0));
    m.key(Key::Char('/'), at(0));
    let shown = screen(&m, at(0));
    assert!(shown.contains("/goto       THREAD|#channel|@user"), "{shown}");
    assert!(shown.contains("↑↓ select · Enter run · Tab complete · Esc close"), "{shown}");
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
    assert!(shown.contains("no messages with bob yet: type one"), "{shown}");
    assert!(shown.contains("a-z write · ↑ select · Enter open"), "{shown}");
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
fn an_error_stays_until_a_key_wrapped_over_three_lines_at_most_after_its_mark() {
    let mut m = model();
    let reason = "the daemon refused the order because ".to_owned() + &"of a reason far too long ".repeat(12);
    m.notify(reason.trim_end(), Level::Error, at(1));
    m.notify("reconnected", Level::Info, at(2));
    let shown = crate::test_support::render::rendered(80, 24, |f| draw(f, &m, at(60)));
    let footer: Vec<&str> = shown.lines().skip(21).collect();
    assert!(footer[0].starts_with(" ✗ the daemon refused the order"), "{shown}");
    assert!(footer[2].trim_end().ends_with('…'), "cut on its third line: {shown}");
    assert!(!shown.contains("reconnected"), "a notice waits behind the error: {shown}");
    m.key(Key::Down, at(61));
    let shown = crate::test_support::render::rendered(80, 24, |f| draw(f, &m, at(61)));
    assert!(!shown.contains("refused") && shown.lines().nth(23).unwrap().contains("? help"), "{shown}");
}

#[test]
fn a_notice_cut_at_the_width_ends_in_an_ellipsis() {
    let mut m = model();
    m.notify("notices far too long to fit ".repeat(4), Level::Info, at(1));
    let line = text(&footer_line(&m, at(2), 40));
    assert_eq!((line.chars().count(), line.ends_with('…')), (40, true), "{line}");
    m.notify("short", Level::Error, at(3));
    assert_eq!(text(&footer_line(&m, at(3), 40)), " ✗ short");
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
    assert!(footer.starts_with(" a-z write · ↑ select · Enter details · "), "letters written: {footer}");
    m.key(Key::Up, at(2));
    let footer = text(&footer_line(&m, at(2), 80));
    assert!(footer.starts_with(" a allow · A always · r reject · n note · Enter details · "), "{footer}");
    assert!(footer.ends_with(" · ? help"), "{footer}");
}
