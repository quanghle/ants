//! Tests of the thread view's timeline: delegations nested, prompts ended with no reply, turns' calls cycled and
//! summed, the spinner, and trail runs' steps.

use super::*;
use crate::tui::thread::conversation::{Item, Timed, glyphs, hm, rows};

#[test]
fn a_delegation_nests_the_other_ants_calls_and_reply_under_its_message() {
    let thread = ThreadId::generate();
    let mut m = model();
    let (entries, calls) = delegated(&thread);
    calls.iter().for_each(|c| m.data.calls.status(c));
    let calls = m.data.calls.get(&thread).unwrap().calls();
    let with = Timed { calls, ends: &[], steps: &[] };
    let rows = rows(&entries, with, Tools::All, false, |u| m.data.is_human(u));
    let shown: Vec<(String, usize)> = rows
        .iter()
        .map(|r| match &r.item {
            Item::Message(e) => (e.text.clone(), r.depth),
            Item::Call(c) => (c.title.clone(), r.depth),
            Item::End(e) => (e.ant.to_string(), r.depth),
            Item::Turn(t) => (t.ant.to_string(), r.depth),
            Item::Step(s) => (s.thread.to_string(), r.depth),
        })
        .collect();
    let expected = [
        ("deploy the site", 0),
        ("read migrations", 1),
        ("is jobs.retries read anywhere?", 1),
        ("grep retries", 2),
        ("only in worker/retry.rs", 2),
        ("cargo test", 1),
        ("deployed", 0),
    ];
    assert_eq!(shown, expected.map(|(t, d)| (t.to_owned(), d)));
    let first: Vec<String> = glyphs(&rows).into_iter().map(|(first, _)| first).collect();
    assert_eq!(first, ["", "  ⎿ ", "    ", "      ⎿ ", "        ", "    ", ""]);
}

#[test]
fn a_prompt_ended_with_no_reply_shows_dim_where_it_ended() {
    let thread = ThreadId::generate();
    let mut m = model();
    let entries =
        vec![message(1, &thread, "dev-a", "dev-b", "check the logs"), message(3, &thread, "dev-b", "dev-a", "clean")];
    let end = ants_proto::TimelineEntry::NoReply {
        why: ants_proto::NoReplyWhy::ReplyToOwn,
        reason: None,
        text: "\nThanks, noted.\nMore.".to_owned(),
    };
    let user = crate::test_support::live::user;
    let record = ants_proto::Timeline {
        at: at(4),
        ..ants_proto::Timeline::new(user("dev-a"), thread.clone(), entries[1].id.unwrap(), vec![end], 0)
    };
    let ended = crate::threads::thread::timeline_entries(9, &record.subject(), &serde_json::to_vec(&record).unwrap());
    m.stored(entries.iter().cloned().chain(ended).collect(), at(5));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    assert_eq!(page.messages().count(), 2, "an end is no message");

    let shown = screen(&page, &m, 120, at(6));
    let ended =
        shown.lines().find(|l| l.contains("● dev-a  ended without a reply: Thanks, noted.")).unwrap_or_default();
    assert!(ended.split('│').nth(1).is_some_and(|l| l.ends_with(&hm(at(4)))), "{shown}");
    assert!(!shown.contains("More."), "{shown}");
}

#[test]
fn t_cycles_a_finished_turn_s_calls_from_one_line_to_each_to_none() {
    let thread = ThreadId::generate();
    let mut m = model();
    let entries = vec![
        message(1, &thread, "alice", "dev-a", "run the tests"),
        message(30, &thread, "dev-a", "alice", "two pass"),
    ];
    m.stored(entries.clone(), at(30));
    for (id, status, secs) in
        [("c1", "in_progress", 10), ("c1", "failed", 14), ("c2", "in_progress", 15), ("c2", "completed", 20)]
    {
        m.data.calls.status(&tool(&thread, "dev-a", id, Some(id), status, secs));
    }
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    m.pages.push(crate::tui::model::Page::Thread(page));
    let shown = |m: &Model| match m.pages.last() {
        Some(crate::tui::model::Page::Thread(page)) => screen(page, m, 100, at(40)),
        _ => unreachable!(),
    };
    assert!(shown(&m).contains("  ⎿ ● dev-a  Ran 2 commands · 10.0s · 1 failed"), "{}", shown(&m));

    m.selecting = Some(m.conversation());
    assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::None)));
    assert_eq!(m.notice_at(at(40)).map(|n| n.text.as_str()), Some("tool calls: none"));
    let none = shown(&m);
    assert!(!none.contains("Run(c1)") && !none.contains("Ran 2") && none.contains("two pass"), "{none}");

    assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::All)));
    let all = shown(&m);
    assert!(all.contains("  ⎿ ● Run(c1) · failed after 4.0s") && all.contains("    ● Run(c2) · 5.0s"), "{all}");
    assert!(!all.contains("Ran 2"), "{all}");
    assert_eq!(m.key(Key::Char('T'), at(40)), Some(Action::KeepTools(Tools::Collapsed)));
}

#[test]
fn a_running_turn_sums_its_calls_by_kind_and_lists_only_its_latest_three() {
    let thread = ThreadId::generate();
    let mut m = model();
    let entries = vec![message(1, &thread, "alice", "dev-a", "fix the build")];
    m.stored(entries.clone(), at(1));
    let calls = [
        ("c1", "read", "src/a.rs", "completed", 2),
        ("c2", "execute", "cargo build", "completed", 3),
        ("c3", "other", "browser_snapshot", "completed", 4),
        ("c4", "read", "Read src/b.rs", "completed", 5),
        ("c5", "execute", "Run `cargo test`", "in_progress", 6),
    ];
    for (id, kind, title, status, secs) in calls {
        let mut event = tool(&thread, "dev-a", id, Some(title), status, secs);
        if let ants_proto::StatusUpdate::Tool { tool_kind, .. } = &mut event.update {
            *tool_kind = Some(kind.to_owned());
        }
        m.data.calls.status(&event);
    }
    let busy = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
    m.data
        .live
        .status(&ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), busy), at(6));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();

    let shown = screen(&page, &m, 80, at(10));
    assert!(shown.contains("dev-a  Read 2 files, ran 2 commands, used 1 other tool · 8s"), "{shown}");
    assert!(shown.contains("      ⎿ ● browser_snapshot · "), "{shown}");
    assert!(shown.contains("        ● Read(src/b.rs) · "), "{shown}");
    assert!(shown.contains("Run(cargo test) · running 4s"), "{shown}");
    assert!(!shown.contains("src/a.rs") && !shown.contains("cargo build"), "only the latest three: {shown}");
}

#[test]
fn a_spinner_turns_eight_times_a_second_unless_animate_is_off() {
    let thread = ThreadId::generate();
    let mut m = model();
    let entries = vec![message(1, &thread, "alice", "dev-a", "fix the build")];
    m.stored(entries.clone(), at(1));
    m.data.calls.status(&tool(&thread, "dev-a", "c1", Some("Run `cargo test`"), "in_progress", 2));
    let busy = ants_proto::StatusUpdate::State { state: ants_proto::State::Working };
    m.data
        .live
        .status(&ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), busy), at(2));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let spinner = |m: &Model, now| {
        let shown = screen(&page, m, 80, now);
        shown.chars().find(|c| conversation::SPINNER.iter().any(|s| s.starts_with(*c))).unwrap()
    };
    let later = at(10) + chrono::TimeDelta::milliseconds(125);
    assert_ne!(spinner(&m, at(10)), spinner(&m, later));
    assert!(page.spinning.get());
    m.data.looks = crate::tui::faces::Looks::new(crate::avatars::Depth::Plain, ants_proto::Theme::Dark, false);
    assert_eq!(spinner(&m, at(10)), spinner(&m, later), "a frame a second");
}

/// What lead tells of step `call` of a run reporting on `thread`, to `to` on `step_thread`, at second `secs`.
fn step(
    thread: &ThreadId,
    step_thread: &ThreadId,
    call: u32,
    to: &str,
    stage: ants_proto::StepStage,
    secs: i64,
) -> ants_proto::StatusEvent {
    let update = ants_proto::StatusUpdate::Step {
        run: "0198c4ab-0000-7000-8000-000000000000".parse().unwrap(),
        call,
        label: Some(format!("review  part\n{call}")),
        to: crate::test_support::live::user(to),
        step_thread: step_thread.clone(),
        stage,
    };
    let lead = crate::test_support::live::user("lead");
    ants_proto::StatusEvent { at: at(secs), ..ants_proto::StatusEvent::new(lead, thread.clone(), update) }
}

#[test]
fn a_runs_steps_show_where_they_started_and_enter_opens_the_thread_of_the_one_selected() {
    use ants_proto::StepStage;
    let (thread, first, second) = (ThreadId::generate(), ThreadId::generate(), ThreadId::generate());
    let mut m = model();
    let entries = vec![
        message(1, &thread, "lead", "alice", "Run 0198c4ab started"),
        message(9, &thread, "lead", "alice", "Run 0198c4ab is done"),
    ];
    m.stored(entries.clone(), at(9));
    for event in [
        step(&thread, &first, 1, "dev-b", StepStage::Started, 3),
        step(&thread, &second, 2, "dev-c", StepStage::Started, 4),
        step(&thread, &first, 1, "dev-b", StepStage::Answered, 6),
        step(&thread, &second, 2, "dev-c", StepStage::Failed, 7),
    ] {
        m.take(crate::live::Event::Status(event), at(7));
    }
    assert_eq!(m.data.live.progress_of(&thread, 80), None, "a step tells nothing of where its conductor stands");
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    let shown = screen(&page, &m, 110, at(10));
    let lines: Vec<&str> = shown.lines().collect();
    let row = |text: &str| lines.iter().position(|l| l.contains(text)).unwrap_or_else(|| panic!("{text}: {shown}"));
    let answered = format!("◆ step 1 review part 1 → dev-b · answered · thread {first}");
    let failed = format!("◆ step 2 review part 2 → dev-c · failed · thread {second}");
    assert!(row("│   Run 0198c4ab started") < row(&answered) && row(&answered) < row(&failed), "{shown}");
    assert!(row(&failed) < row("│   Run 0198c4ab is done"), "each before the next message stored: {shown}");
    m.ui.tools = Tools::None;
    assert!(screen(&page, &m, 110, at(10)).contains(&failed), "steps are no tool calls");

    assert_eq!(page.key(Key::Up, &m.data, at(10)), None);
    assert_eq!(page.selection().map(|e| e.seq), Some(9));
    screen(&page, &m, 110, at(10));
    page.key(Key::Up, &m.data, at(10));
    assert_eq!((page.selection(), page.step_selected()), (None, Some(&second)), "a step, as a message is");
    let shown = screen(&page, &m, 110, at(10));
    assert!(shown.contains("│▌    ◆ step 2"), "the step selected marked: {shown}");
    assert_eq!(page.key(Key::Enter, &m.data, at(10)), Some(To::Thread(second.clone())));
    assert_eq!(page.key(Key::Char('y'), &m.data, at(10)), None, "no message to copy");
    page.key(Key::Up, &m.data, at(10));
    assert_eq!(page.key(Key::Enter, &m.data, at(10)), Some(To::Thread(first)));
    page.key(Key::Up, &m.data, at(10));
    assert_eq!(page.selection().map(|e| e.seq), Some(1));
    page.mouse_select_step(second.clone());
    assert_eq!(page.step_selected(), Some(&second));

    m.pages.push(crate::tui::model::Page::Thread(page));
    m.focus = crate::tui::model::Focus::View;
    m.selecting = Some(m.conversation());
    let footer = crate::tui::keymap::footer(&m, at(10), 200).to_string();
    assert!(footer.contains("Enter step thread"), "{footer}");
    let (open, ants) = m.steps_wanted().unwrap();
    assert_eq!((open, ants), (thread, vec![crate::test_support::live::user("lead")]));
}
