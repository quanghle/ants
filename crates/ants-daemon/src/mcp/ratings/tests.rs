use ants_proto::provider::{Origin, Starter};
use ants_proto::{RatingSummary, RunId, Saver, TrailLeg, TrailLimits, TrailName, TrailRating, UserId};
use chrono::{Duration, TimeZone as _};
use serde_json::json;

use super::*;

fn name(trail: &str) -> TrailName {
    trail.parse().unwrap()
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 19, 8, 0, 0).unwrap()
}

/// A run, done, of `trail`, or of an inline script for none.
fn run(trail: Option<&str>) -> TrailRun {
    let leg = TrailLeg {
        trail: trail.map(name),
        at: None,
        script: "meta = {}".to_owned(),
        args: json!(null),
        first_call: 0,
        started_at: now(),
        steps: 1,
    };
    TrailRun {
        run: RunId::generate(),
        conductor: "lead".parse().unwrap(),
        route: vec![leg],
        owner: Some(UserId::human()),
        thread: ThreadId::generate(),
        parent: None,
        origin: Origin { by: Starter::Human { user: UserId::human() }, channel: None },
        limits: TrailLimits::default(),
        state: RunState::Done { result: json!(1) },
        phase: None,
        steps: 1,
        turns: 1,
        started_at: now(),
        updated_at: now(),
        ended_at: Some(now()),
    }
}

fn rated(trail: &str, replaced: Option<RatingOutcome>, saved_from: bool) -> RatedTrail {
    RatedTrail { trail: name(trail), replaced, saved_from }
}

#[test]
fn a_rating_names_the_trails_it_rated_and_the_ratings_it_replaced() {
    let run = run(Some("review"));
    let r = &run.run;
    let text = rated_text(&run, RatingOutcome::Worked, "review a PR", &[rated("review", None, false)]);
    assert_eq!(text, format!("Rated run {r} worked for trail review (task: \"review a PR\")."));

    let both = [rated("review", Some(RatingOutcome::Failed), false), rated("fix", Some(RatingOutcome::Failed), true)];
    let text = rated_text(&run, RatingOutcome::Partly, "review a PR", &both);
    assert_eq!(
        text,
        format!(
            "Rated run {r} partly for trails review, fix (saved from its script) (task: \"review a PR\"). It replaces \
             the earlier rating (failed).\nOther saved trails for this task: find_trails(query: \"review a PR\")."
        )
    );

    let mixed = [rated("review", Some(RatingOutcome::Partly), false), rated("fix", None, false)];
    let text = rated_text(&run, RatingOutcome::Worked, "t", &mixed);
    assert!(text.ends_with("It replaces the earlier ratings of review partly."), "{text}");
}

#[test]
fn an_inline_run_has_no_trail_to_rate() {
    let run = run(None);
    let text = rated_text(&run, RatingOutcome::Failed, "say \"hi\"", &[]);
    assert_eq!(
        text,
        format!(
            "Run {} ran an inline script: an inline script has no saved trail to rate.\nOther saved trails for this \
             task: find_trails(query: \"say \\\"hi\\\"\").",
            run.run
        )
    );
}

fn entry(trail: &str, saver: Option<Saver>) -> SavedEntry {
    SavedEntry {
        name: name(trail),
        description: format!("{trail} the changes"),
        script: "0".repeat(64),
        local: saver.is_none(),
        saver,
        saved_from: None,
    }
}

fn rating(trail: &str, outcome: RatingOutcome, why: &str, days: i64) -> TrailRating {
    TrailRating {
        trail: name(trail),
        conductor: "dev-a".parse().unwrap(),
        run: RunId::generate(),
        outcome,
        task: "review a PR touching auth".to_owned(),
        why: why.to_owned(),
        script: "0".repeat(64),
        at: now() - Duration::days(days),
    }
}

#[test]
fn found_trails_show_their_record_on_matching_tasks_or_that_they_are_worth_exploring() {
    let mut matching = RatingSummary::default();
    matching.add(rating("review", RatingOutcome::Worked, "caught the missing permission check", 3));
    matching.add(rating("review", RatingOutcome::Failed, &"x".repeat(300), 5));
    let found = [
        Found {
            name: name("review"),
            item: entry("review", Some(Saver::Human { user: "alice".parse().unwrap() })),
            draw: 0.7,
            matching,
        },
        Found { name: name("upgrade"), item: entry("upgrade", None), draw: 0.4, matching: RatingSummary::default() },
    ];
    let text = found_text("review rust changes", &found, true, now());
    assert_eq!(
        text,
        format!(
            "Saved trails for \"review rust changes\", ranked by a draw from each one's ratings on matching tasks:\n\
             1. review: review the changes\n   saved by alice; success 50% over 2 ratings on matching tasks (1 worked, \
             0 partly, 1 failed)\n   - worked, 3d ago, \"review a PR touching auth\": caught the missing permission \
             check\n   - failed, 5d ago, \"review a PR touching auth\": {}…\n\
             2. upgrade (local to this machine): upgrade the changes\n   worth exploring: no rating on a matching task \
             yet\nA script of this ant's own runs with run_trail(script: …); once it works, save_trail saves it for \
             the colony.",
            "x".repeat(MAX_REASON_SHOWN - 1)
        )
    );
    assert_eq!(found_text("deploy", &[], false, now()), "No saved trail matches \"deploy\".");
}
