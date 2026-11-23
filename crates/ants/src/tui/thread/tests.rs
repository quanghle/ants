//! Tests of the thread view: the shared fixtures drawing it, and its behaviours, one submodule each.

use ants_proto::{Answer, Pick, Question, ThreadDone, Verdict};
use ratatui::Terminal;
use ratatui::backend::TestBackend;

use super::conversation::body;
use super::*;
use crate::test_support::live::{at, message};
use crate::tui::model::Model;
use crate::tui::test_support::model;
use crate::tui::test_support::tool;
use crate::tui::timeline::Tools;

/// A thread where alice asks dev-a, which asks dev-b, which answers it, then answers alice; each ant making a
/// call before it speaks, dev-a one more after hearing back.
fn delegated(thread: &ThreadId) -> (Vec<Entry>, Vec<ants_proto::StatusEvent>) {
    let entries = vec![
        message(1, thread, "alice", "dev-a", "deploy the site"),
        message(3, thread, "dev-a", "dev-b", "is jobs.retries read anywhere?"),
        message(5, thread, "dev-b", "dev-a", "only in worker/retry.rs"),
        message(7, thread, "dev-a", "alice", "deployed"),
    ];
    let calls = vec![
        tool(thread, "dev-a", "c1", Some("read migrations"), "completed", 2),
        tool(thread, "dev-b", "c2", Some("grep retries"), "completed", 4),
        tool(thread, "dev-a", "c3", Some("cargo test"), "in_progress", 6),
    ];
    (entries, calls)
}

fn screen(page: &Page, m: &Model, width: u16, now: DateTime<Utc>) -> String {
    crate::test_support::render::rendered(width, 24, |f| page.draw(f, f.area(), m, now, true))
}

mod cards;
mod find;
mod messages;
mod panel;
mod scrolling;
mod timeline;
mod waiting;
mod withdraw;
