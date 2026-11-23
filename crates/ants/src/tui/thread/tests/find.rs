//! Tests of finding in the thread view: Ctrl-F marks the matches and steps to them, scrolling by line.

use ratatui::style::Modifier;

use super::*;

#[test]
fn ctrl_f_marks_the_matches_and_steps_to_them_scrolling_by_line() {
    let thread = ThreadId::generate();
    let m = model();
    let entries: Vec<Entry> = (1..=30)
        .map(|n| {
            message(
                n,
                &thread,
                "alice",
                "dev-a",
                &if n % 10 == 4 { format!("the Needle {n}") } else { format!("line {n}") },
            )
        })
        .collect();
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let shown = screen(&page, &m, 80, at(40));
    assert!(!shown.contains("Needle 4 ") && shown.contains("line 30"), "{shown}");
    assert!(page.find_key(Key::Ctrl('f')) && page.finding());
    "needle".chars().for_each(|c| assert!(page.find_key(Key::Char(c))));
    screen(&page, &m, 80, at(40));
    assert_eq!(
        page.find_footer().unwrap().to_string(),
        " find: needle_ · 3 found · Enter next · Shift-Enter previous · Esc end"
    );
    assert!(page.find_key(Key::Enter));
    screen(&page, &m, 80, at(40));
    assert!(page.find_footer().unwrap().to_string().contains("3 of 3"), "the first match in view");
    assert!(page.find_key(Key::Newline), "Shift-Enter: the previous match");
    let shown = screen(&page, &m, 80, at(40));
    assert!(shown.contains("the Needle 14 ") && !shown.contains("line 30"), "scrolled to it: {shown}");
    assert!(page.find_footer().unwrap().to_string().contains("2 of 3"));
    assert!(!page.find_key(Key::Up) && !page.finding(), "another key stops asking");
    assert!(page.find_key(Key::Char('N')));
    let shown = screen(&page, &m, 80, at(40));
    assert!(shown.contains("the Needle 4 ") && page.find_footer().unwrap().to_string().contains("1 of 3"), "{shown}");
    assert!(page.find_key(Key::Char('n')));
    screen(&page, &m, 80, at(40));
    assert!(page.find_footer().unwrap().to_string().contains("2 of 3"));
    assert_eq!(page.selected, None, "the view scrolls, the selection stays");
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| page.draw(f, f.area(), &m, at(40), true)).unwrap();
    let buffer = terminal.backend().buffer();
    let (x, y) = (0..24)
        .flat_map(|y| (0..80).map(move |x| (x, y)))
        .find(|&(x, y)| buffer[(x, y)].symbol() == "N" && buffer[(x + 1, y)].symbol() == "e")
        .unwrap();
    assert!(buffer[(x, y)].modifier.contains(Modifier::REVERSED), "the match marked");
    assert!(page.find_key(Key::Esc) && page.find_footer().is_none(), "Esc ends it");
    assert!(!page.find_key(Key::Char('n')), "n is no longer taken");
}
