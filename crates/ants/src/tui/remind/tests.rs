//! Tests of reminders in the views: `b` asks when and sets the reminder with its note, and `/reminders` lists them as
//! `ants remind list` does.

use super::*;
use crate::test_support::live::{at, user};
use crate::tui::exec::{Exec, Job, Outcome};
use crate::tui::model::Key;
use crate::tui::test_support::model;

#[test]
fn b_asks_when_and_sets_the_reminder_with_its_note_at_once() {
    let (thread, message) = (ThreadId::generate(), Uuid::now_v7());
    let mut m = model();
    assert_eq!(m.remind(thread.clone(), Some(message)), None);
    for c in "2h look again".chars() {
        m.key(Key::Char(c), at(0));
    }
    let line = m.prompt_line().unwrap().to_string();
    assert!(line.contains("remind of") && line.contains("2h look again"), "{line}");
    let reminder =
        Reminder { at: at(0), due: at(7200), message: Some(message), note: "look again".into(), pushed: false };
    let set = Action::Change(thread.clone(), Change::Remind(Some(reminder.clone())), 0);
    assert_eq!(m.key(Key::Enter, at(0)), Some(set));
    assert!(m.prompt.is_none());
    assert_eq!(m.data.cards.reminders.remind.get(&thread), Some(&reminder), "set at once");
    assert_eq!(m.set_reminder(thread.clone(), None), Action::Change(thread, Change::Remind(None), 0));
    assert!(m.data.cards.reminders.remind.is_empty(), "cleared at once");
}

#[test]
fn reminders_lists_them_in_the_pager_from_any_view() {
    let mut m = model();
    assert!(m.target().is_err() && m.unfit("reminders", at(1)).is_none(), "no thread needed");
    m.follow_drafts();
    m.ui.bar.focused = true;
    m.ui.bar.set_text("/reminders");
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(Job::Reminders, "/reminders".to_owned())));
    let line = "2 reminders, 1 due: the pager lists them".to_owned();
    let listed = Done { line: line.clone(), page: Some("TABLE".to_owned()), ..Done::default() };
    let outcome = Outcome { result: Ok(listed), text: "/reminders".to_owned(), quoting: None };
    assert_eq!(m.outcome(outcome), Some(Action::Page("TABLE".to_owned())));
    assert_eq!(m.ui.bar.status.as_ref().map(|s| s.text.as_str()), Some(line.as_str()));
}

/// The job lists every reminder of the human, those not due yet too, soonest due first, with the lines of `ants
/// remind list`.
#[tokio::test]
async fn the_reminders_job_lists_every_reminder_as_ants_remind_list_does() {
    use ants_proto::{Role, ThreadTitle};

    use crate::connection::Nats;
    use crate::threads::thread_meta;

    let Some(server) = crate::test_support::Server::start() else { return };
    let alice = user("alice");
    let creds = server.creds_as(&alice, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let (client, js) = nats.connect("test").await.unwrap();
    let meta = thread_meta::open(&nats, &js).await.unwrap();
    let ctx = Ctx {
        client,
        js: js.clone(),
        me: nats.me.clone(),
        domain: None,
        meta: thread_meta::open(&nats, &js).await.map_err(|e| format!("{e:#}")),
        sealing: crate::people::sealed::Sealing::of(&nats),
        account: None,
        keyring: None,
        admin: Err("unused".into()),
    };
    let mut exec = Exec::new(ctx);
    exec.run(Job::Reminders, String::new());
    let none = exec.next().await.result.unwrap();
    assert_eq!((none.line.as_str(), none.page), ("no reminders", None));

    let (due, later) = (ThreadId::generate(), ThreadId::generate());
    let now = Utc::now();
    let reminder = |due, note: &str| Reminder { at: now, due, message: None, note: note.into(), pushed: false };
    meta.change_reminders(|reminders| {
        reminders.remind(&later, reminder(now + chrono::TimeDelta::hours(2), ""));
        reminders.remind(&due, reminder(now - chrono::TimeDelta::minutes(1), "check CI"));
        true
    })
    .await
    .unwrap();
    meta.put_title(&ThreadTitle::new(later.clone(), "Deploy", alice).unwrap()).await.unwrap();
    exec.run(Job::Reminders, String::new());
    let listed = exec.next().await.result.unwrap();
    assert_eq!(listed.line, "2 reminders, 1 due: the pager lists them");
    let page = listed.page.unwrap();
    let lines: Vec<&str> = page.lines().collect();
    assert!(lines[0].ends_with(&format!(" due thread {due}: check CI")), "{page}");
    assert!(lines[1].ends_with(&format!(" thread {later}: Deploy")) && !lines[1].contains("due"), "{page}");
    assert_eq!(lines.len(), 2, "{page}");
}
