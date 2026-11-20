//! The table's [`When`](super::When)s: whether a row's keys can act on the model now, or why not.

use chrono::{DateTime, Utc};

use crate::tui::model::{Focus, Model, Page};
use crate::tui::{home, question};

pub(super) fn always(_: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    Ok(())
}

pub(super) fn bar_empty(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.bar.is_empty(), "the bar holds text: Ctrl-C empties it")
}

pub(super) fn check(holds: bool, why: &'static str) -> Result<(), &'static str> {
    if holds { Ok(()) } else { Err(why) }
}

pub(super) fn no_thread_open(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(!matches!(model.page(), Page::Thread(_)), "Ctrl-F finds in the thread: /search searches every thread")
}

pub(super) fn on_view(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.focus == Focus::View, "the nav is focused")
}

pub(super) fn selecting(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.selecting.is_some() && !model.typing(), "nothing selected: a printable key is written already")
}

pub(super) fn not_selecting(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.selecting.is_none() || model.typing(), "Esc goes back to writing first")?;
    on_view(model, now)
}

pub(super) fn thread_selected(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.target().is_ok(), "no thread selected")
}

pub(super) fn home_item(model: &Model, now: DateTime<Utc>) -> Option<home::Item> {
    match model.page() {
        Page::Home(page) => page.selected(&model.data, now),
        _ => None,
    }
}

pub(super) fn home_selected(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some(), "nothing selected")
}

pub(super) fn home_pending(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.pending().is_some()), "no tool request waits")
}

pub(super) fn home_group(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.of_kind().is_some()), "no group of tool requests selected")
}

pub(super) fn home_reply(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    match home_item(model, now) {
        None => Err("nothing selected"),
        Some(item) if item.pending().is_some() => Err("r rejects the tool request selected"),
        Some(_) => Ok(()),
    }
}

pub(super) fn home_question(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.question.is_some()), "no question waits")
}

pub(super) fn home_stuck(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(home_item(model, now).is_some_and(|i| i.why == home::Why::Stuck), "no stuck thread selected")
}

pub(super) fn thread_pending(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.pending(&model.data).is_some(), "no tool request waits"),
        _ => Err("no thread open"),
    }
}

/// Whether Enter opens a selected step's thread rather than acting on a card.
pub(super) fn step_selected(model: &Model) -> bool {
    matches!(model.page(), Page::Thread(page) if page.step_selected().is_some())
}

pub(super) fn thread_step(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(step_selected(model), "no step of a trail run selected")
}

pub(super) fn thread_pending_enter(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(!step_selected(model), "Enter opens the selected step's thread")?;
    thread_pending(model, now)
}

pub(super) fn thread_question_enter(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(!step_selected(model), "Enter opens the selected step's thread")?;
    thread_question(model, now)
}

pub(super) fn thread_waiting(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.waiting(&model.data, now) > 0, "no tool request or question waits"),
        _ => Err("no thread open"),
    }
}

pub(super) fn finding(model: &Model) -> bool {
    model.focus == Focus::View && matches!(model.page(), Page::Thread(page) if page.find_footer().is_some())
}

pub(super) fn thread_finding(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(finding(model), "no find in the thread: Ctrl-F starts one")
}

pub(super) fn not_finding(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(!finding(model), "N steps back through the find")
}

pub(super) fn thread_note(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    check(!finding(model), "n steps through the find")?;
    thread_pending(model, now)
}

pub(super) fn thread_question(model: &Model, now: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => {
            let asked = question::keyed(&model.data, &page.history.entries, now).is_some();
            check(asked && page.pending(&model.data).is_none(), "no question waits")
        }
        _ => Err("no thread open"),
    }
}

pub(super) fn thread_message(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.selection().is_some(), "no message selected"),
        _ => Err("no thread open"),
    }
}

pub(super) fn thread_attachment(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.attachment().is_some(), "no attachment marked"),
        _ => Err("no thread open"),
    }
}

pub(super) fn thread_attachments(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Thread(page) => check(page.attached().is_some_and(|e| e.attachments.len() > 1), "no other attachment"),
        _ => Err("no thread open"),
    }
}

pub(super) fn post_selected(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Channel(page) => check(page.post(&model.data).is_some(), "no post selected"),
        _ => Err("no channel open"),
    }
}

pub(super) fn post_attachment(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Channel(page) => check(page.attachment(&model.data).is_some(), "no attachment marked"),
        _ => Err("no channel open"),
    }
}

pub(super) fn post_attachments(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    match model.page() {
        Page::Channel(page) => {
            let several = page.post(&model.data).is_some_and(|p| p.attachments.len() > 1);
            check(several, "no other attachment")
        }
        _ => Err("no channel open"),
    }
}

pub(super) fn ant_restart_armed(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    let armed = matches!(model.page(), Page::Ant(page) if page.armed() == Some(super::ant::Armed::Restart));
    check(armed, "no restart asked for")
}

pub(super) fn mcp_signing(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(matches!(model.page(), Page::Ant(page) if page.mcp.signing()), "no sign-in under way")
}

/// Whether the view waits for the `y` confirming what one of its keys asked for.
pub(super) fn armed(model: &Model, _: DateTime<Utc>) -> Result<(), &'static str> {
    check(model.page().armed(), "nothing asked for")
}
