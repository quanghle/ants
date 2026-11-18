use ants_proto::ThreadId;

use super::*;
use crate::test_support::live::{at, message};
use crate::tui::model::{Focus, Page};
use crate::tui::test_support::model;
use crate::tui::{fold, frame};

/// The keys taken in the body of the function `name` of `source`: each `Key::` variant matched or compared, the
/// characters of `Key::Char` and `Key::Ctrl`, `'1'..='9'` as the nine digits.
fn taken(source: &str, name: &str) -> Vec<Key> {
    let start = source.find(&format!("fn {name}(")).unwrap_or_else(|| panic!("no fn {name}"));
    let open = start + source[start..].find('{').expect("a body");
    let mut depth = 0;
    let end = open
        + source[open..]
            .char_indices()
            .find(|&(_, c)| {
                depth += match c {
                    '{' => 1,
                    '}' => -1,
                    _ => 0,
                };
                depth == 0
            })
            .expect("the end of the body")
            .0;
    let body = &source[open..end];
    let mut keys = Vec::new();
    for (at, _) in body.match_indices("Key::") {
        let rest = &body[at + 5..];
        let name: String = rest.chars().take_while(char::is_ascii_alphanumeric).collect();
        let named = match name.as_str() {
            "Up" => Key::Up,
            "Down" => Key::Down,
            "PageUp" => Key::PageUp,
            "PageDown" => Key::PageDown,
            "Home" => Key::Home,
            "End" => Key::End,
            "Tab" => Key::Tab,
            "Enter" => Key::Enter,
            "Esc" => Key::Esc,
            "Backspace" => Key::Backspace,
            "Delete" => Key::Delete,
            "Left" => Key::Left,
            "Right" => Key::Right,
            "Newline" => Key::Newline,
            "Interrupt" => Key::Interrupt,
            "AltBackspace" => Key::AltBackspace,
            "WordLeft" => Key::WordLeft,
            "WordRight" => Key::WordRight,
            "CtrlHome" => Key::CtrlHome,
            "CtrlEnd" => Key::CtrlEnd,
            "Char" | "Ctrl" | "Alt" => {
                let args = parenthesized(&rest[name.len()..]);
                let chars = quoted(args);
                let chars = if args.contains("..=") { ('1'..='9').collect() } else { chars };
                let key = match name.as_str() {
                    "Char" => Key::Char,
                    "Ctrl" => Key::Ctrl,
                    _ => Key::Alt,
                };
                keys.extend(chars.into_iter().map(key));
                continue;
            }
            _ => panic!("Key::{name}"),
        };
        keys.push(named);
    }
    keys
}

/// What the parentheses `text` begins with hold, quoted characters skipped.
fn parenthesized(text: &str) -> &str {
    let (mut depth, mut quoted) = (0, false);
    let mut chars = text.char_indices();
    while let Some((at, c)) = chars.next() {
        match c {
            '\\' if quoted => _ = chars.next(),
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth -= 1;
                if depth == 0 {
                    return &text[1..at];
                }
            }
            _ => {}
        }
    }
    panic!("unclosed: {text}")
}

/// The characters quoted in `text`: 'a', '\\'.
fn quoted(text: &str) -> Vec<char> {
    text.split('\'')
        .skip(1)
        .step_by(2)
        .map(|c| if c == "\\\\" { '\\' } else { c.chars().next().expect("a char") })
        .collect()
}

#[test]
fn the_fold_keys_have_rows_in_the_table() {
    let rows = |entries: &[keymap::Entry], key| entries.iter().any(|e| e.keys.contains(&key));
    assert!(rows(keymap::GLOBAL, fold::NAV_KEY), "the nav's fold key acts everywhere");
    assert!(rows(keymap::entries(View::Thread), fold::PANEL_KEY), "the side panel's fold key acts in a thread");
}

#[test]
fn every_key_a_view_takes_has_a_row_in_the_table() {
    let views: [(&str, &str, &[View]); 22] = [
        ("model/keys.rs", "nav_key", &[View::Nav]),
        ("thread.rs", "find_key", &[View::Thread]),
        ("command/bar.rs", "key", &[View::Bar]),
        ("input.rs", "key", &[View::Bar]),
        ("command/history.rs", "history_key", &[View::Bar]),
        ("command/history.rs", "search_key", &[View::Bar]),
        ("complete.rs", "complete_key", &[View::Bar]),
        ("approvals.rs", "key", &[View::Request]),
        ("approvals/full.rs", "scrolled", &[View::Request]),
        ("home.rs", "key", &[View::Home]),
        ("work.rs", "key", &[View::Work]),
        ("channel.rs", "key", &[View::Channel]),
        ("thread.rs", "key", &[View::Thread]),
        ("dm.rs", "key", &[View::Dm]),
        ("search.rs", "key", &[View::Search]),
        ("tree.rs", "key", &[View::Tree]),
        ("ant.rs", "key", &[View::Ant, View::AntQueue]),
        ("memories.rs", "key", &[View::Memories]),
        ("mcp.rs", "key", &[View::Mcp]),
        ("settings.rs", "key", &[View::Settings]),
        ("credentials.rs", "key", &[View::Credentials]),
        ("held.rs", "key", &[View::Held]),
    ];
    let source = |file: &str| match file {
        "model/keys.rs" => include_str!("../model/keys.rs"),
        "command/bar.rs" => include_str!("../command/bar.rs"),
        "input.rs" => include_str!("../input.rs"),
        "command/history.rs" => include_str!("../command/history.rs"),
        "complete.rs" => include_str!("../complete.rs"),
        "approvals.rs" => include_str!("../approvals.rs"),
        "approvals/full.rs" => include_str!("../approvals/full.rs"),
        "home.rs" => include_str!("../home.rs"),
        "work.rs" => include_str!("../work.rs"),
        "channel.rs" => include_str!("../channel.rs"),
        "thread.rs" => include_str!("../thread.rs"),
        "dm.rs" => include_str!("../dm.rs"),
        "search.rs" => include_str!("../search.rs"),
        "tree.rs" => include_str!("../tree.rs"),
        "ant.rs" => include_str!("../ant.rs"),
        "memories.rs" => include_str!("../memories.rs"),
        "mcp.rs" => include_str!("../mcp.rs"),
        "settings.rs" => include_str!("../settings.rs"),
        "credentials.rs" => include_str!("../credentials.rs"),
        "held.rs" => include_str!("../held.rs"),
        _ => unreachable!(),
    };
    for (file, name, views) in views {
        let keys = taken(source(file), name);
        assert!(!keys.is_empty(), "{file}: no keys read");
        for key in keys {
            let row = views.iter().flat_map(|v| keymap::entries(*v)).any(|e| e.keys.contains(&key));
            assert!(row, "{file}: {key:?} has no row in the key table for {views:?}");
        }
    }
}

#[test]
fn the_digit_range_and_quoted_backslash_are_read() {
    let source =
        r"fn key() { match key { Key::Char('1'..='9') => 1, Key::Char(c @ ('[' | '\\')) => 2, Key::Ctrl('x') => 3 } }";
    let mut want: Vec<Key> = ('1'..='9').map(Key::Char).collect();
    want.extend([Key::Char('['), Key::Char('\\'), Key::Ctrl('x')]);
    assert_eq!(taken(source, "key"), want);
}

fn screen(m: &Model) -> String {
    crate::test_support::render::rendered(80, 24, |f| frame::draw(f, m, at(2)))
}

#[test]
fn at_80x24_every_row_scrolls_into_view_over_how_to_close_the_help() {
    let thread = ThreadId::generate();
    let mut m = model();
    m.stored(vec![message(1, &thread, "alice", "dev-a", "deploy the site")], at(1));
    m.pages.push(Page::Thread(crate::tui::thread::Page::new(thread)));
    m.focus = Focus::View;
    m.key(Key::Char('?'), at(2));
    let rows = lines(&m, at(2), 78);
    let mut seen = Vec::new();
    for _ in 0..rows.len() {
        let shown = screen(&m);
        let close = shown.lines().nth(21).expect("the help's last row");
        assert!(close.contains(CLOSE), "{shown}");
        seen.push(shown);
        m.key(Key::Down, at(2));
    }
    for row in &rows {
        let text: String = row.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(seen.iter().any(|s| s.contains(text.trim_end())), "never shown: {text}");
    }
    assert!(rows.iter().any(|r| r.to_string().contains("allow the oldest tool request waiting once")));
    assert!(rows.iter().any(|r| r.to_string().contains(" Everywhere")));
}

#[test]
fn esc_q_or_question_mark_close_the_help_and_other_keys_do_nothing() {
    let mut m = model();
    for close in [Key::Esc, Key::Char('q'), Key::Char('?')] {
        m.key(Key::Char('?'), at(0));
        for key in [Key::Char('x'), Key::Enter, Key::Tab, Key::Char('R'), Key::Down, Key::PageDown] {
            assert_eq!(m.key(key, at(0)), None, "{key:?}");
            assert!(m.help.is_some(), "{key:?} keeps the help");
        }
        assert_eq!(m.key(close, at(0)), None);
        assert!(m.help.is_none(), "{close:?} closes the help");
    }
}

#[test]
fn on_an_ant_the_help_lists_the_keys_of_every_pane() {
    let mut m = model();
    m.select(crate::tui::nav::Item::Ant(crate::test_support::live::user("dev-a")));
    m.focus = Focus::View;
    let rows: Vec<String> = lines(&m, at(0), 98).iter().map(ToString::to_string).collect();
    for label in ["r", "p", "X", "u", "s", "v", "n"] {
        let row = format!(" {label:>LABEL$}  ");
        assert!(rows.iter().any(|r| r.starts_with(&row)), "no {label}: {rows:#?}");
    }
}
