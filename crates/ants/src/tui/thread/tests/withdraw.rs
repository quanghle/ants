//! Tests of withdrawing in the thread view: a waiting message's place in line, and `w` and `e` withdrawing a waiting
//! message or a channel post from the ants not yet started.

use super::*;

#[test]
fn a_message_waiting_its_turn_shows_how_many_go_before_it() {
    let thread = ThreadId::generate();
    let mut m = model();
    let entries = vec![
        message(1, &thread, "alice", "dev-a", "deploy the site"),
        message(2, &thread, "alice", "dev-a", "then the docs"),
    ];
    m.stored(entries.clone(), at(2));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries.clone())).unwrap();
    let queued = ants_proto::StatusUpdate::Queued { message: entries[1].id.unwrap(), ahead: 1 };
    let event = ants_proto::StatusEvent::new(crate::test_support::live::user("dev-a"), thread.clone(), queued);
    m.data.live.status(&event, at(3));
    let shown = screen(&page, &m, 100, at(4));
    let line = shown.lines().find(|l| l.contains("queued")).unwrap_or_default();
    assert!(line.contains("alice → dev-a") && line.contains("queued · 1 ahead"), "{shown}");
    assert_eq!(shown.matches("queued").count(), 1, "{shown}");
}

#[test]
fn w_and_e_withdraw_a_waiting_message_of_the_human_to_an_ant_and_withdrawn_ones_are_marked() {
    let thread = ThreadId::generate();
    let mut m = model();
    let mut entries = vec![
        message(1, &thread, "alice", "dev-a", "deploy the site"),
        message(2, &thread, "dev-a", "alice", "deploying"),
        message(3, &thread, "alice", "dev-a", "then the docs"),
    ];
    let (first, last) = (entries[0].id.unwrap(), entries[2].id.unwrap());
    entries[1].in_reply_to = Some(first);
    m.stored(entries.clone(), at(3));
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries.clone())).unwrap();
    let withdraw = |edit| {
        Some(To::Do(Action::Run(
            Job::Withdraw { ants: vec![crate::test_support::live::user("dev-a")], message: last, edit },
            String::new(),
        )))
    };
    assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), withdraw(None));
    let edit = Edit { text: "then the docs".into(), files: Vec::new() };
    assert_eq!(page.key(Key::Char('E'), &m.data, at(4)), withdraw(Some(Box::new(edit))));
    page.key(Key::Up, &m.data, at(4));
    page.key(Key::Up, &m.data, at(4));
    let refused = |why: &str| Some(To::Refused(why.to_owned()));
    assert_eq!(
        page.key(Key::Char('W'), &m.data, at(4)),
        refused("withdraw: select a message of alice to an ant, or a post of alice waking one")
    );
    page.key(Key::Up, &m.data, at(4));
    let started = format!("withdraw unavailable: dev-a started on message {first} already; x cancels it");
    assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), refused(&started));

    let mut notice =
        message(4, &thread, "dev-a", "alice", "[ants] Withdrawn by alice before dev-a was prompted with it");
    notice.in_reply_to = Some(last);
    notice.notice = Some(ants_proto::NoticeKind::Withdrawn);
    entries.push(notice);
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    page.key(Key::Up, &m.data, at(5));
    page.key(Key::Up, &m.data, at(5));
    assert_eq!(page.key(Key::Char('W'), &m.data, at(5)), refused(&format!("message {last} is withdrawn already")));
    m.data.withdrawn.insert(first);
    let shown = screen(&page, &m, 100, at(5));
    assert_eq!(shown.matches("  withdrawn").count(), 2, "{shown}");
}

#[test]
fn w_and_e_withdraw_a_channel_post_of_the_human_from_the_ants_it_woke_that_have_not_started() {
    use ants_proto::{ChannelEntry, ChannelPost, Humans, NoticeKind};
    let user = crate::test_support::live::user;
    let (ops, mut m) = ("ops".parse::<ants_proto::ChannelId>().unwrap(), model());
    let mut channel = ChannelEntry::new(ops.clone());
    channel.members = ["alice", "dev-a", "dev-b"].map(user).to_vec();
    channel.messenger = Some(user("triage"));
    m.data.channels.push(channel);
    let parse = |seq: u64, post: &ChannelPost| {
        let payload = serde_json::to_vec(post).unwrap();
        crate::threads::thread::Parser::default().entry(seq, at(seq.cast_signed()), &post.subject(), &payload).unwrap()
    };
    let top = ChannelPost::new(ops.clone(), user("alice"), "@dev-a @dev-b @bob ship it");
    let thread = top.thread_id.clone();
    let shipping = top.reply(user("dev-a"), "shipping", &[], &Humans::default()).unwrap();
    let later = ChannelPost::on_thread(ops, thread.clone(), user("alice"), "and the docs");
    let mut entries = vec![parse(1, &top), parse(2, &shipping), parse(3, &later)];
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries.clone())).unwrap();
    let withdraw = |ants: &[&str], message, edit| {
        let ants = ants.iter().map(|a| user(a)).collect();
        Some(To::Do(Action::Run(Job::Withdraw { ants, message, edit }, String::new())))
    };
    let refused = |why: String| Some(To::Refused(why));
    assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), withdraw(&["triage"], later.id, None));
    page.key(Key::Home, &m.data, at(4));
    assert_eq!(page.key(Key::Char('W'), &m.data, at(4)), withdraw(&["dev-b"], top.id, None));
    let edit = format!("edit unavailable: dev-a started on message {} already; x cancels it", top.id);
    assert_eq!(page.key(Key::Char('E'), &m.data, at(4)), refused(edit));

    let mut notice = message(4, &thread, "dev-b", "alice", "[ants] Withdrawn by alice before dev-b was prompted");
    (notice.in_reply_to, notice.notice) = (Some(top.id), Some(NoticeKind::Withdrawn));
    entries.push(notice);
    let mut page = Page::new(thread.clone());
    page.history.loaded(&thread, Ok(entries)).unwrap();
    page.key(Key::Home, &m.data, at(5));
    let started = format!("withdraw unavailable: dev-a started on message {} already; x cancels it", top.id);
    assert_eq!(page.key(Key::Char('W'), &m.data, at(5)), refused(started));
}
