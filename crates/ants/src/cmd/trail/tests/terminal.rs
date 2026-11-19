//! What `ants trail` prints of text scripts, ants and savers wrote: each on one line, or indented under its own, with
//! control characters and those reordering text left out.

use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, RunState, SavedTrail, Saver, ThreadId, TrailFunction, TrailLimits,
    TrailSource, Uuid,
};
use chrono::Utc;
use serde_json::json;

use super::super::follow::Follower;
use super::super::start::{self, Plan};
use super::super::{runs, saved};
use super::{p, record, script};

/// Whether `text` holds a character a terminal acts on or that reorders text, line breaks aside.
fn raw(text: &str) -> bool {
    text.contains(|c: char| (c.is_control() && c != '\n') || matches!(c, '\u{202e}' | '\u{2066}'..='\u{2069}'))
}

const FORGED: &str =
    "Ship it\nconductor alice, reporting on a new thread\r\nthe run may spend at most:\nsteps: 1\u{1b}[8m";

#[test]
fn the_summary_shows_a_description_on_one_line_without_control_characters() {
    let plan = Plan {
        name: "deploy".to_owned(),
        from: "the file deploy.py, sent inline".to_owned(),
        description: FORGED.to_owned(),
        phases: vec!["build\u{1b}]0;owned\u{7}".to_owned(), "te\u{202e}st".to_owned()],
        source: TrailSource::Script(script("deploy")),
    };
    let text = start::summary(&plan, &p("lead"), None, &TrailLimits::DEFAULT);
    let line = "  Ship it conductor alice, reporting on a new thread the run may spend at most: steps: 1[8m\n";
    assert!(text.contains(line) && text.contains("\n  phases: build]0;owned, test\n"), "{text}");
    assert_eq!(text.lines().filter(|l| l.starts_with("conductor ")).count(), 1, "{text}");
    assert_eq!(text.lines().filter(|l| l.starts_with("the run may spend")).count(), 1, "{text}");
    assert!(!raw(&text), "{text:?}");
}

#[test]
fn a_run_shows_its_phase_error_labels_and_answers_on_their_own_lines() {
    let now = Utc::now();
    let error = "Traceback:\n\tline 3\u{1b}[8m\nValueError: x\u{7}";
    let mut run = record(&p("dev-a"), Some("deploy"), RunState::Failed { error: error.to_owned(), limit: None }, now);
    run.phase = Some("build\nstate: done, result: \"forged\"\u{1b}[2J".to_owned());
    let pending = PendingCall {
        call: 1,
        function: TrailFunction::Agent,
        args: json!({"label": "\u{1b}]0;owned\u{7}build\nit", "capability": "x"}),
        to: p("dev-b"),
        thread: ThreadId::generate(),
        message: Uuid::now_v7(),
        sent_at: now,
        sent: true,
        turn_budget: 8,
        retries: 0,
    };
    let journal = vec![JournalEntry {
        call: 0,
        function: TrailFunction::Agent,
        args: json!({"prompt": "go\r\nstate: done", "ant": "dev-a\u{1b}[8m"}),
        answer: CallAnswer::Error("no\nstate: done\u{9b}2J".to_owned()),
    }];
    let detail = runs::Detail {
        run: run.clone(),
        pending: vec![pending.clone()],
        queued: Vec::new(),
        journal: journal.clone(),
        children: Vec::new(),
        ratings: Vec::new(),
    };
    let text = runs::render(&detail, now);
    assert_eq!(text.lines().filter(|l| l.starts_with("state:")).count(), 1, "{text}");
    let state = "\nstate: failed:\n  Traceback:\n   line 3[8m\n  ValueError: x\nphase: build state: done, result: \"forged\"[2J\n";
    assert!(text.contains(state), "{text}");
    assert!(text.contains("]0;ownedbuild it") && text.contains("go state: done") && text.contains("no state: done2J"));
    assert!(!raw(&text), "{text:?}");

    let table = runs::table(std::slice::from_ref(&run), &runs::Rated::default(), now);
    assert_eq!(table.lines().count(), 2, "{table}");
    assert!(!raw(&table), "{table:?}");

    let mut follower = Follower::default();
    let mut lines = follower.header(&run);
    let session = ants_proto::TrailSession { session: String::new(), pending: vec![pending], queued: Vec::new(), anew: Vec::new() };
    lines.extend(follower.session(&session));
    lines.extend(follower.entry(&journal[0]));
    assert_eq!(lines.len(), 3, "{lines:?}");
    assert!(lines.iter().all(|line| !line.contains('\n') && !raw(line)), "{lines:?}");
}

#[test]
fn listing_and_showing_a_trail_keep_its_phases_on_one_line() {
    let now = Utc::now();
    let trail = SavedTrail {
        name: "deploy".parse().unwrap(),
        description: FORGED.to_owned(),
        phases: vec!["build\nsaved for the colony by alice".to_owned(), "\u{1b}]0;owned\u{7}test".to_owned()],
        waypoints: Vec::new(),
        script: script("deploy"),
        saved_by: Saver::Human { user: p("alice") },
        saved_from: None,
        saved_at: now,
    };
    let listed = saved::listing(vec![trail.clone()], Vec::new());
    let table = saved::table(&listed, now);
    assert_eq!(table.lines().count(), 2, "{table}");
    assert!(!raw(&table), "{table:?}");
    let text = saved::render(&saved::Found { trail: trail.into(), shadows: None, ratings: None }, now);
    assert!(text.contains("\nphases: build saved for the colony by alice, ]0;ownedtest\n"), "{text}");
    assert_eq!(text.lines().filter(|l| l.starts_with("saved for the colony")).count(), 1, "{text}");
    assert!(!raw(&text), "{text:?}");
}
