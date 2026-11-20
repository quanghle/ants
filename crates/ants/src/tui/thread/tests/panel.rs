use super::*;

#[test]
fn the_view_shows_the_timeline_with_running_calls_and_a_panel_beside_it() {
    let thread = ThreadId::generate();
    let mut m = model();
    let (entries, calls) = delegated(&thread);
    m.stored(entries[..3].to_vec(), at(5));
    calls.iter().for_each(|c| m.data.calls.status(c));
    let busy = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
    m.data
        .live
        .status(&ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), busy), at(6));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries[..3].to_vec())).unwrap();
    page.about = Some(About { spend: Some(Vec::new()), ..About::default() });

    m.data.live.colony(vec![crate::test_support::colony::entry("dev-a", 3600, 2, at(5))], at(5));
    let shown = screen(&page, &m, 110, at(47));
    assert!(shown.contains("alice → dev-a"), "{shown}");
    assert!(shown.contains("    ● {.~.} dev-a → dev-b"), "an ant's face before its name: {shown}");
    assert!(shown.contains("  deploy the site"), "{shown}");
    assert!(shown.contains("dev-a → dev-b  is jobs.retries read anywhere?"), "{shown}");
    assert!(shown.contains("dev-a  Ran 1 command · 41s"), "the running turn on a line: {shown}");
    assert!(shown.contains("Run(cargo test) · running 41s"), "its latest call under it: {shown}");
    assert!(shown.contains("      ⎿ ● dev-b  Ran 1 command · 0ms "), "a finished turn on a line: {shown}");
    assert!(!shown.contains("read migrations"), "{shown}");
    m.ui.tools = Tools::All;
    let shown = screen(&page, &m, 110, at(47));
    assert!(shown.contains("  ⎿ ● Run(read migrations) · 0ms "), "{shown}");
    m.ui.tools = Tools::None;
    let shown = screen(&page, &m, 110, at(47));
    assert!(!shown.contains("Ran 1") && !shown.contains("running 41s"), "{shown}");
    assert!(shown.contains("only in worker/retry.rs"), "{shown}");
    let brief = "│working · dev-a dev-b · 8 turns left · $0.00 · 0 tokens";
    assert!(shown.contains(brief) && !shown.contains("STATE"), "the panel folded on a line: {shown}");
    m.ui.tools = Tools::Collapsed;
    let shown = screen(&page, &m, 140, at(47));
    for field in ["STATE", "OWNER", "ANTS", "dev-a ◐", "dev-b", "SPEND  $0.00 · 0 tokens", "FROM   ask by alice"] {
        assert!(shown.contains(field), "{field}: {shown}");
    }
    assert!(shown.contains("FILES") && shown.contains("src/c3.rs"), "{shown}");
    assert!(!screen(&page, &m, 80, at(47)).contains("STATE"), "no panel on a narrow view");
}

#[test]
fn keys_move_the_selection_copy_it_and_toggle_how_the_human_holds_the_thread() {
    let thread = ThreadId::generate();
    let mut m = model();
    let (entries, _) = delegated(&thread);
    m.stored(entries.clone(), at(7));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let data = &mut m.data;
    let copy = |text: &str| Some(To::Do(Action::Copy(text.to_owned())));
    assert_eq!(page.key(Key::Char('y'), data, at(8)), copy("deployed"), "the latest without a selection");
    page.key(Key::Up, data, at(8));
    page.key(Key::Char('k'), data, at(8));
    assert_eq!(page.key(Key::Char('y'), data, at(8)), copy("only in worker/retry.rs"));
    page.key(Key::Home, data, at(8));
    assert_eq!(page.key(Key::Char('y'), data, at(8)), copy("deploy the site"));
    page.key(Key::End, data, at(8));
    assert_eq!(page.selected, None, "End follows the latest again");

    let change = |change| Some(To::Do(Action::Change(thread.clone(), change, 7)));
    for (key, on) in [('p', Change::Pin(true)), ('m', Change::Mute(true)), ('e', Change::Archive(true))] {
        assert_eq!(page.key(Key::Char(key), data, at(8)), change(on));
    }
    assert_eq!(page.key(Key::Char('d'), data, at(8)), change(Change::Done(true)));
    data.live.marks.pin(&thread);
    page.about = Some(About {
        muted: Some(at(100)),
        done: Some(ThreadDone::new(thread.clone(), crate::test_support::live::user("bob"), 7)),
        ..About::default()
    });
    assert_eq!(page.key(Key::Char('p'), data, at(8)), change(Change::Pin(false)), "pressed again, undone");
    assert_eq!(page.key(Key::Char('m'), data, at(8)), change(Change::Mute(false)));
    assert_eq!(page.key(Key::Char('d'), data, at(8)), change(Change::Done(false)));
    assert_eq!(page.key(Key::Char('m'), data, at(200)), change(Change::Mute(true)), "the mute ended");
    data.live.marks.archive(&thread, 7);
    assert_eq!(page.key(Key::Char('e'), data, at(8)), change(Change::Archive(false)));
}
