use std::time::Duration;

use ants_proto::{ChannelId, ChannelNotify, Role, ThreadId};

use super::*;
use crate::connection::Nats;
use crate::test_support::live::{at, user};
use crate::tui::exec::{Exec, Job};
use crate::tui::model::{Action, Key, Model};
use crate::tui::test_support::model;

/// Send `text` from the bar of the view shown.
fn run(m: &mut Model, text: &str) -> Option<Action> {
    m.follow_drafts();
    m.ui.bar.focused = true;
    m.ui.bar.set_text(text);
    m.key(Key::Enter, at(1))
}

#[test]
fn notify_snoozes_resumes_and_shows_the_holds_from_any_view_the_level_on_a_channel() {
    let mut m = model();
    assert!(m.unfit("notify", at(1)).is_none(), "no channel needed");
    let ran = |job, text: &str| Some(Action::Run(job, text.to_owned()));
    let hour = Duration::from_hours(1);
    assert_eq!(run(&mut m, "/notify snooze 1h"), ran(Job::NotifySnooze(hour), "/notify snooze 1h"));
    assert_eq!(run(&mut m, "/notify resume"), ran(Job::NotifyResume, "/notify resume"));
    assert_eq!(run(&mut m, "/notify status"), ran(Job::NotifyStatus, "/notify status"));
    assert_eq!(run(&mut m, "/notify mentions"), None);
    let status = m.ui.bar.status.as_ref().map(|s| s.text.as_str());
    assert_eq!(status, Some("/notify sets a channel's notify level: show the channel first"));
}

#[test]
fn the_status_line_counts_what_the_pager_lists() {
    let mut prefs = NotifyPrefs::default();
    assert_eq!(shown(&prefs), Done { line: "not snoozed".to_owned(), ..Done::default() });
    prefs.snoozed_until = Some(at(3600));
    let snoozed = shown(&prefs);
    assert!(snoozed.line.starts_with("snoozed until ") && snoozed.page.is_none(), "{snoozed:?}");
    let (deploy, ops) = (ThreadId::generate(), "ops".parse::<ChannelId>().unwrap());
    prefs.muted.insert(deploy.clone(), at(7200));
    prefs.set_channel(&ops, ChannelNotify::Mentions);
    let held = shown(&prefs);
    assert_eq!(held.line, format!("{}, 1 muted thread, 1 channel's notify level: the pager lists them", snoozed.line));
    let page = held.page.unwrap();
    let lines: Vec<&str> = page.lines().collect();
    assert_eq!(lines[0], snoozed.line);
    assert!(lines[1].starts_with(&format!("muted thread {deploy} until ")), "{page}");
    assert_eq!(lines[2..], ["#ops notifies mentions (@mentions only)"]);
    prefs.snoozed_until = None;
    prefs.muted.insert(ThreadId::generate(), at(7200));
    prefs.set_channel(&ops, ChannelNotify::Humans);
    assert_eq!(shown(&prefs).line, "not snoozed, 2 muted threads: the pager lists them");
}

/// The jobs snooze, show and resume the human's notifications on every device as `ants notify` does, through the
/// same store and lines.
#[tokio::test]
async fn the_notify_jobs_snooze_show_and_resume_as_ants_notify_does() {
    let Some(server) = crate::test_support::Server::start() else { return };
    let alice = user("alice");
    let creds = server.creds_as(&alice, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None).unwrap();
    let (client, js) = nats.connect("test").await.unwrap();
    let ctx = Ctx {
        client,
        js: js.clone(),
        me: nats.me.clone(),
        domain: None,
        meta: Err("unused".into()),
        sealing: crate::people::sealed::Sealing::of(&nats),
        account: None,
        keyring: None,
        admin: Err("unused".into()),
    };
    let mut exec = Exec::new(ctx);
    let mut done = async |job| {
        exec.run(job, String::new());
        exec.next().await.result
    };

    assert_eq!(done(Job::NotifyStatus).await.unwrap().line, "not snoozed");
    let snoozed = done(Job::NotifySnooze(Duration::from_hours(1))).await.unwrap().line;
    assert!(
        snoozed.starts_with("notifications of alice snoozed until ") && snoozed.ends_with(", on every device"),
        "{snoozed}"
    );
    let prefs = crate::people::quiet::prefs(&js, &alice).await.unwrap();
    assert!(prefs.snoozed(chrono::Utc::now() + chrono::TimeDelta::minutes(59)), "stored for every device");
    let thread = ThreadId::generate();
    done(Job::Mute { thread: thread.clone(), hold: Duration::from_hours(2) }).await.unwrap();
    let held = done(Job::NotifyStatus).await.unwrap();
    assert!(held.line.starts_with("snoozed until ") && held.line.ends_with(", 1 muted thread: the pager lists them"));
    assert!(held.page.unwrap().contains(&format!("muted thread {thread} until ")));
    assert_eq!(done(Job::NotifyResume).await.unwrap().line, "notifications of alice resumed");
    assert_eq!(done(Job::NotifyResume).await.unwrap().line, "notifications of alice were not snoozed");
    let refused = done(Job::NotifySnooze(Duration::ZERO)).await.unwrap_err();
    assert_eq!(format!("{refused:#}"), "snooze: from 1s to 365d");
    assert_eq!(done(Job::NotifyStatus).await.unwrap().line, "not snoozed, 1 muted thread: the pager lists them");
}
