use super::*;
use crate::tui::images;

#[test]
fn reactions_show_under_each_message_and_plus_reacts_or_takes_one_back() {
    use ants_daemon::records::reactions::Reacted;

    let thread = ThreadId::generate();
    let mut m = model();
    let entries = vec![
        message(1, &thread, "alice", "dev-a", "deploy the site"),
        message(2, &thread, "dev-a", "dev-b", "check the logs"),
    ];
    let (first, nested) = (entries[0].id.unwrap(), entries[1].id.unwrap());
    m.stored(entries.clone(), at(1));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let reacted = |name: &str, by: &[&str]| {
        Reacted::new(name.parse().unwrap(), by.iter().map(|b| crate::test_support::live::user(b)).collect())
    };
    m.data.reactions.insert(first, vec![reacted("eyes", &["alice", "bob"]), reacted("shipit", &["dev-a"])]);
    m.data.reactions.insert(nested, vec![reacted("check", &["alice"])]);
    let shown = screen(&page, &m, 100, at(2));
    // A wide glyph's second cell shows as a space on the test screen.
    assert!(shown.contains("│   👀  2  :shipit: 1"), "{shown}");
    assert!(shown.contains("│       ✅  1"), "under a nested message too: {shown}");

    m.pages.push(crate::tui::model::Page::Thread(page));
    m.key(Key::Home, at(2));
    assert_eq!(m.key(Key::Char('+'), at(2)), None);
    let footer = m.prompt_line().unwrap().to_string();
    assert!(footer.contains("👍 thumbsup") && footer.contains("Esc close"), "{footer}");
    for c in "eyes".chars() {
        m.key(Key::Char(c), at(2));
    }
    let eyes: ants_proto::Reaction = "eyes".parse().unwrap();
    assert_eq!(m.key(Key::Enter, at(2)), Some(Action::React(first, eyes.clone(), true)), "alice takes it back");
    assert!(m.prompt.is_none());
    m.key(Key::Char('+'), at(2));
    assert_eq!(m.key(Key::Enter, at(2)), Some(Action::React(first, "thumbsup".parse().unwrap(), false)));

    m.read(crate::tui::reads::Read::Reacted(first, eyes, true, Ok(true)), at(3));
    let notice = m.notice_at(at(3)).map(|n| n.text.clone());
    assert_eq!(notice, Some(format!("removed eyes from message {first}")));
}

#[test]
fn messages_render_as_markdown_at_the_top_level_and_nested() {
    let thread = ThreadId::generate();
    let m = model();
    let mut entries = vec![
        message(1, &thread, "alice", "dev-a", "**done**:\n\n- one\n- two"),
        message(2, &thread, "dev-a", "dev-b", "check `retry.rs`"),
        message(3, &thread, "dev-b", "dev-a", "# Found\n\nin *two* places"),
    ];
    entries[2].kind = Kind::Note;
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let shown = screen(&page, &m, 80, at(40));
    let shown: Vec<&str> = shown.lines().map(|l| l.trim_end_matches('│').trim_end()).collect();
    let from = shown.iter().position(|l| l.contains("alice → dev-a")).unwrap();
    assert_eq!(shown[from + 1..from + 5], ["│   done:", "│", "│   • one", "│   • two"], "{shown:#?}");
    assert!(shown[from + 5].starts_with("│   ⎿ ● dev-a → dev-b  check retry.rs  "), "one line on its heading");
    assert!(shown[from + 6].starts_with("│       ⎿ ● dev-b → notes  "), "{shown:#?}");
    assert_eq!(shown[from + 7..from + 10], ["│           Found", "│", "│           in two places"]);
}

#[test]
fn a_quoted_reply_shows_the_message_it_replies_to_above_its_text() {
    let thread = ThreadId::generate();
    let m = model();
    let mut reply = message(2, &thread, "alice", "dev-b", "why **that** one?");
    reply.quote = Some(Quote::new(crate::test_support::live::user("dev-a"), "picked dev-b for the deploy"));
    let entries = vec![message(1, &thread, "dev-a", "alice", "picked dev-b for the deploy"), reply];
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let shown = screen(&page, &m, 80, at(40));
    let shown: Vec<&str> = shown.lines().map(|l| l.trim_end_matches('│').trim_end()).collect();
    let from = shown.iter().position(|l| l.contains("alice → dev-b")).unwrap();
    let quoted = "│   ↪ replying to dev-a: picked dev-b for the deploy";
    assert_eq!(shown[from + 1..from + 3], [quoted, "│   why that one?"], "{shown:#?}");
}

#[test]
fn sealed_messages_show_a_lock_opened_or_why_not() {
    let thread = ThreadId::generate();
    let m = model();
    let sealed = |seq, from, to, text: &str, state| {
        let mut e = message(seq, &thread, from, to, text);
        e.sealed = Some(state);
        e
    };
    let entries = vec![
        sealed(1, "alice", "bob", "lunch at noon?", sealed::State::Opened),
        sealed(2, "bob", "alice", "[sealed]", sealed::State::Unreadable),
        sealed(3, "bob", "alice", "[forged]", sealed::State::Forged),
    ];
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let shown = screen(&page, &m, 80, at(40));
    let shown: Vec<&str> = shown.lines().map(|l| l.trim_end_matches('│').trim_end()).collect();
    let from = shown.iter().position(|l| l.contains("alice → bob")).unwrap();
    assert!(shown[from].contains("alice → bob  🔒 "), "{shown:#?}");
    assert_eq!(shown[from + 1], "│   lunch at noon?");
    assert!(shown[from + 3].contains("🔒") && !shown[from + 3].contains("forged"), "{shown:#?}");
    assert_eq!(shown[from + 4], "│   [sealed] sent before this device had a key");
    assert!(shown[from + 6].contains("🔒") && shown[from + 6].contains("  forged  "), "{shown:#?}");
    assert!(shown[from + 7].starts_with("│   [forged] "), "{shown:#?}");
}

#[test]
fn attachments_list_under_their_message_and_o_opens_the_marked_one() {
    let thread = ThreadId::generate();
    let m = model();
    let mut sent = message(1, &thread, "dev-a", "alice", "the screenshots");
    let shot = |name: &str| ants_daemon::files::reference(name, "image/png", &[0; 1536]).unwrap();
    let envelope = ants_proto::Envelope::new(
        crate::test_support::live::user("dev-a"),
        crate::test_support::live::user("alice"),
        "x",
    );
    sent.attachments = crate::messages::attachments::listed(&[shot("before.png"), shot("after.png")], |id| {
        envelope.attachment_handle(id)
    });
    let entries = vec![sent, message(2, &thread, "alice", "dev-a", "thanks")];
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let shown = screen(&page, &m, 80, at(3));
    // The paperclip takes two cells.
    assert!(shown.contains("│   📎  before.png (1.5 KiB) image/png ◂"), "{shown}");
    assert!(shown.contains("│   📎  after.png (1.5 KiB) image/png  "), "{shown}");

    let open = |page: &mut Page| match page.key(Key::Char('o'), &m.data, at(3)) {
        Some(To::Do(Action::Run(Job::Open(listed), text))) if text.is_empty() => listed.attachment.name,
        other => panic!("{other:?}"),
    };
    assert_eq!(open(&mut page), "before.png", "following the latest, the latest message with files");
    page.key(Key::Char(']'), &m.data, at(3));
    assert_eq!(open(&mut page), "after.png");
    assert!(screen(&page, &m, 80, at(3)).contains("after.png (1.5 KiB) image/png ◂"));
    page.key(Key::Char(']'), &m.data, at(3));
    assert_eq!(open(&mut page), "before.png", "past the last, the first");
    page.key(Key::Home, &m.data, at(3));
    page.key(Key::Down, &m.data, at(3));
    assert_eq!(page.key(Key::Char('o'), &m.data, at(3)), None, "the selected message has no files");
}

#[test]
fn an_image_encoded_draws_under_its_line_and_the_others_keep_theirs() {
    let thread = ThreadId::generate();
    let mut m = model();
    let mut sent = message(1, &thread, "dev-a", "alice", "the screenshot");
    let png = crate::tui::test_support::png(40, 40);
    let shot = ants_daemon::files::reference("shot.png", "image/png", &png).unwrap();
    let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
    let envelope = ants_proto::Envelope::new(
        crate::test_support::live::user("dev-a"),
        crate::test_support::live::user("alice"),
        "x",
    );
    sent.attachments = crate::messages::attachments::listed(&[shot, log], |id| envelope.attachment_handle(id));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(vec![sent.clone()])).unwrap();
    let before = screen(&page, &m, 80, at(3));

    m.ui.inline_images = true;
    assert_eq!(images::wanted(&m), Vec::new(), "only for the open thread");
    let image = crate::tui::test_support::halfblocks(&png);
    m.data.images.insert(sent.attachments[0].handle.clone(), Some(image));
    let shown = screen(&page, &m, 80, at(3));
    let rows: Vec<&str> = shown.lines().collect();
    let line = rows.iter().position(|r| r.contains("shot.png")).unwrap();
    assert!(rows[line + 3].contains("build.log"), "two rows for the image:\n{shown}");
    assert_eq!(before.lines().filter(|r| r.contains("📎")).count(), 2);
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| page.draw(f, f.area(), &m, at(3), true)).unwrap();
    let red = ratatui::style::Color::Rgb(200, 40, 40);
    let drawn: Vec<(u16, u16)> = (0..24)
        .flat_map(|y| (0..80).map(move |x| (x, y)))
        .filter(|&(x, y)| terminal.backend().buffer()[(x, y)].bg == red)
        .collect();
    let y = u16::try_from(line).unwrap() + 1;
    assert_eq!(drawn, [(4, y), (5, y), (6, y), (7, y), (4, y + 1), (5, y + 1), (6, y + 1), (7, y + 1)]);
}
