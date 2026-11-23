//! Tests of scrolling the thread view: a message found by search selected at the top, messages taller than the view,
//! and messages arriving while scrolled up.

use super::*;

#[test]
fn opened_from_a_search_the_message_found_is_selected_at_the_top_until_moved() {
    let thread = ThreadId::generate();
    let m = model();
    let entries: Vec<Entry> = (1..=30)
        .map(|n| {
            let (from, to) = if n % 2 == 0 { ("dev-a", "alice") } else { ("alice", "dev-a") };
            message(n, &thread, from, to, &format!("line {n}"))
        })
        .collect();
    let mut page = Page::at(thread.clone(), entries[4].id.unwrap());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let rows = |page: &Page| screen(page, &m, 80, at(40)).lines().map(str::to_owned).collect::<Vec<_>>();
    let shown = rows(&page);
    assert!(shown[2].starts_with("│▌❯ alice → dev-a"), "selected: {shown:#?}");
    assert_eq!(shown[3].trim_end_matches('│').trim_end(), "│▌  line 5");
    assert_eq!(page.key(Key::Char('y'), &m.data, at(40)), Some(To::Do(Action::Copy("line 5".to_owned()))));
    assert_eq!(rows(&page)[3], shown[3], "held until moved");
    page.key(Key::Up, &m.data, at(40));
    assert_eq!(rows(&page)[3].trim_end_matches('│').trim_end(), "│▌  line 4", "moving starts from it");
    page.key(Key::End, &m.data, at(40));
    assert!(rows(&page)[22].contains("line 30"), "the latest at the bottom");
}

/// A thread of a short question, an answer drawn on 241 lines (its heading and 240 rows) and a short reply.
fn tall(thread: &ThreadId) -> Page {
    let rows: Vec<String> = (1..=240).map(|n| format!("- row {n}.")).collect();
    let entries = vec![
        message(1, thread, "alice", "dev-a", "how did the deploy go?"),
        message(2, thread, "dev-a", "alice", &rows.join("\n")),
        message(3, thread, "alice", "dev-a", "thanks"),
    ];
    let mut page = Page::new(thread.clone());
    page.history.loaded(thread, Ok(entries)).unwrap();
    page
}

/// The rows of the tall answer `shown` shows.
fn rows_shown(shown: &str) -> Vec<usize> {
    let rows = shown.split("row ").skip(1);
    rows.filter_map(|r| r.split_once('.').and_then(|(n, _)| n.parse().ok())).collect()
}

#[test]
fn page_down_alone_brings_every_line_of_a_message_taller_than_the_view_into_it() {
    let thread = ThreadId::generate();
    let m = model();
    let mut page = tall(&thread);
    let draw =
        |page: &Page| crate::test_support::render::rendered(120, 30, |f| page.draw(f, f.area(), &m, at(9), true));
    draw(&page);
    page.key(Key::Home, &m.data, at(9));
    let mut seen = std::collections::BTreeSet::new();
    let mut shown = draw(&page);
    for _ in 0..40 {
        seen.extend(rows_shown(&shown));
        page.key(Key::PageDown, &m.data, at(9));
        let next = draw(&page);
        if next == shown {
            break;
        }
        shown = next;
    }
    assert_eq!(seen, (1..=240).collect(), "{shown}");
    assert!(shown.contains("thanks"), "{shown}");
    assert_eq!(page.chosen().map(|e| e.text.as_str()), Some("how did the deploy go?"), "PgDn keeps the selection");
    page.key(Key::PageUp, &m.data, at(9));
    let back = rows_shown(&draw(&page));
    assert!(back.contains(&220) && !back.contains(&240), "PgUp scrolls back by half a view: {back:?}");
}

#[test]
fn up_and_down_scroll_inside_a_selected_message_taller_than_the_view_before_moving_on() {
    let thread = ThreadId::generate();
    let m = model();
    let mut page = tall(&thread);
    let draw =
        |page: &Page| crate::test_support::render::rendered(120, 30, |f| page.draw(f, f.area(), &m, at(9), true));
    draw(&page);
    page.key(Key::Home, &m.data, at(9));
    draw(&page);
    page.key(Key::Down, &m.data, at(9));
    let mut shown = draw(&page);
    assert_eq!(rows_shown(&shown).first(), Some(&1), "from its first line: {shown}");
    let answer = |page: &Page| page.chosen().is_some_and(|e| e.text.starts_with("- row 1."));
    let mut seen = std::collections::BTreeSet::new();
    while answer(&page) {
        seen.extend(rows_shown(&shown));
        page.key(Key::Down, &m.data, at(9));
        shown = draw(&page);
    }
    assert_eq!(seen, (1..=240).collect());
    assert_eq!(page.chosen().map(|e| e.text.as_str()), Some("thanks"));
    page.key(Key::Up, &m.data, at(9));
    let shown = draw(&page);
    assert!(answer(&page) && rows_shown(&shown).contains(&240), "from its last line: {shown}");
    page.key(Key::Up, &m.data, at(9));
    assert!(answer(&page), "Up scrolls inside it");
    let text = page.chosen().unwrap().text.clone();
    assert_eq!(page.key(Key::Char('v'), &m.data, at(9)), Some(To::Do(Action::Page(text))));
}

#[test]
fn a_message_arriving_while_scrolled_up_leaves_the_view_and_shows_in_the_count() {
    let thread = ThreadId::generate();
    let m = model();
    let entries: Vec<Entry> = (1..=30).map(|n| message(n, &thread, "dev-a", "alice", &format!("line {n}"))).collect();
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let draw =
        |page: &Page| crate::test_support::render::rendered(80, 24, |f| page.draw(f, f.area(), &m, at(40), true));
    let newer = |shown: &str| {
        let border = shown.lines().last().unwrap_or_default();
        border.split_once("↓ ").and_then(|(_, n)| n.split_once(" newer · End shows them")).map(|(n, _)| n.to_owned())
    };
    assert_eq!(newer(&draw(&page)), None, "following the latest");
    page.key(Key::PageUp, &m.data, at(40));
    let before = draw(&page);
    let count: usize = newer(&before).unwrap().parse().unwrap();
    page.history.stored(vec![message(31, &thread, "dev-a", "alice", "line 31")]);
    let after = draw(&page);
    let body = |shown: &str| shown.lines().take(23).map(str::to_owned).collect::<Vec<_>>();
    assert_eq!(body(&after), body(&before), "the view stays");
    assert_eq!(newer(&after), Some((count + 1).to_string()), "{after}");
    page.key(Key::End, &m.data, at(40));
    let latest = draw(&page);
    assert!(latest.contains("line 31") && newer(&latest).is_none(), "{latest}");
}
