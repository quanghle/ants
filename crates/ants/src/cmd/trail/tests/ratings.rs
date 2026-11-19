//! The ratings `ants trail list`, `show` and `runs` show, and `ants trail rm` purging a trail's ratings.

use ants_proto::{RatingOutcome, RunState, TRAIL_RATINGS_BUCKET, TrailName, TrailRating, script_sha256};
use async_nats::jetstream;
use chrono::{TimeDelta, Utc};

use super::super::{ratings, runs, saved};
use super::{human, p, record, script, write};
use crate::test_support::Server;

/// Put `rating` in [`TRAIL_RATINGS_BUCKET`], as its conductor's daemon does.
async fn put(js: &jetstream::Context, rating: &TrailRating) {
    let payload = serde_json::to_vec(rating).unwrap();
    js.publish(format!("$KV.{TRAIL_RATINGS_BUCKET}.{}", rating.key()), payload.into()).await.unwrap().await.unwrap();
}

#[test]
fn the_cells_and_lines_of_ratings_count_each_outcome_and_name_the_latest_reason() {
    assert_eq!(ratings::cells(None), ["-", "-"]);
    assert_eq!(runs::table(&[], &runs::Rated::default(), Utc::now()).lines().count(), 1);
    assert_eq!(ratings::purged(0, true), "");
    assert_eq!(ratings::purged(1, false), " and its 1 rating");
    assert_eq!(ratings::purged(2, true), ", its 2 ratings");
}

#[test]
fn every_listing_shows_a_rating_s_text_on_one_line_without_control_characters() {
    let now = Utc::now();
    let rating = TrailRating {
        trail: "deploy".parse().unwrap(),
        conductor: "dev-a".parse().unwrap(),
        run: ants_proto::RunId::generate(),
        outcome: RatingOutcome::Failed,
        task: "ship\r\nthe \u{1b}]0;owned\u{7}app".to_owned(),
        why: "it broke\u{2028}- worked, 1m ago: \u{1b}[31mfine\u{85}\u{202e}really".to_owned(),
        script: script_sha256(&script("deploy")),
        at: now - TimeDelta::hours(1),
    };
    let (task, why) = ("ship the ]0;ownedapp", "it broke - worked, 1m ago: [31mfine really");
    let summary = ratings::summaries(vec![rating.clone()]).remove(&rating.trail).unwrap();
    let [_, latest] = ratings::cells(Some(&summary));
    assert_eq!(latest, "it broke - worked, 1m ago: [31mfine real…");
    for shown in [ratings::section(&summary, now), ratings::of_run_section(std::slice::from_ref(&rating), now)] {
        let line = shown.lines().find(|line| line.contains("task: ")).unwrap_or_else(|| panic!("{shown}"));
        assert!(line.ends_with(&format!("task: {task}  why: {why}")), "{shown:?}");
        assert!(!shown.contains(|c: char| c.is_control() && c != '\n') && !shown.contains('\u{202e}'), "{shown:?}");
    }
}

#[tokio::test]
async fn humans_see_the_ratings_of_trails_and_runs_and_removing_a_trail_purges_them() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("trail-ratings");
    let alice = p("alice");
    let js = human(&server, &alice).await;
    let now = Utc::now();
    let deploy = saved::read(&write(&dir, "deploy.py", &script("deploy")), None, &alice, now).unwrap();
    saved::put(&js, &deploy).await.unwrap();
    let release = saved::read(&write(&dir, "release.py", &script("release")), None, &alice, now).unwrap();
    saved::put(&js, &release).await.unwrap();
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let ((_a, a_js), (_b, b_js)) = (server.connect(&dev_a).await, server.connect(&dev_b).await);
    let mut first = record(&dev_a, Some("deploy"), RunState::Done { result: "shipped".into() }, now);
    first.ended_at = Some(now);
    let second = record(&dev_b, Some("deploy"), RunState::Failed { error: "no".to_owned(), limit: None }, now);
    let rating = |trail: &str, run: &ants_proto::TrailRun, outcome, why: &str, ago| TrailRating {
        trail: trail.parse().unwrap(),
        conductor: run.conductor.clone(),
        run: run.run.clone(),
        outcome,
        task: "ship the web app".to_owned(),
        why: why.to_owned(),
        script: script_sha256(&script(trail)),
        at: now - TimeDelta::hours(ago),
    };
    let worked = rating("deploy", &first, RatingOutcome::Worked, "it shipped without a hitch", 2);
    let release_rated = rating("release", &first, RatingOutcome::Partly, "the notes needed a hand", 2);
    let failed = rating("deploy", &second, RatingOutcome::Failed, "the migrations broke", 1);
    for (js, rating) in [(&a_js, &worked), (&a_js, &release_rated), (&b_js, &failed)] {
        put(js, rating).await;
    }

    let listed = super::super::saved_trails(&js).await.unwrap();
    let table = saved::table(&listed, now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[0].ends_with("RATINGS  LATEST"), "{table}");
    let row = lines.iter().find(|line| line.starts_with("deploy")).unwrap();
    assert!(row.ends_with("1/0/1    the migrations broke"), "the latest reason: {table}");
    let unrated = saved::table(&saved::listing(vec![deploy.clone()], Vec::new()), now);
    assert!(!unrated.contains("RATINGS"), "no columns while no trail has ratings: {unrated}");
    let json = serde_json::to_value(listed.iter().find(|l| l.trail.name.as_str() == "deploy").unwrap()).unwrap();
    assert_eq!((json["ratings"]["worked"].as_u64(), json["ratings"]["failed"].as_u64()), (Some(1), Some(1)));

    let name: TrailName = "deploy".parse().unwrap();
    let super::super::Found::Trail(found) =
        super::super::find(&js, super::super::Shown::Trail(name.clone()), true).await.unwrap()
    else {
        panic!("not a trail")
    };
    let text = saved::render(&found, now);
    let (_, section) = text.split_once("ratings: 1 worked, 0 partly, 1 failed\n").unwrap_or_else(|| panic!("{text}"));
    let latest: Vec<&str> = section.lines().take(2).collect();
    assert!(latest[0].starts_with(&format!("  failed run {} by dev-b 1h ago  task: ", second.run)), "{text}");
    assert!(latest[1].starts_with("  worked run ") && latest[1].ends_with("why: it shipped without a hitch"), "{text}");

    let rated = ratings::of_runs(&ratings::read(&js, None).await.unwrap());
    let table = runs::table(&[first.clone(), second.clone()], &rated, now);
    let lines: Vec<&str> = table.lines().collect();
    assert!(lines[1].ends_with("deploy worked, release partly"), "each trail of a run: {table}");
    assert!(lines[2].ends_with("failed"), "{table}");
    let header = serde_json::from_str::<serde_json::Value>(&runs::header_json(&first, &rated).unwrap()).unwrap();
    let expected =
        serde_json::json!([{"trail": "deploy", "outcome": "worked"}, {"trail": "release", "outcome": "partly"}]);
    assert_eq!(header["ratings"], expected);
    let by_b = ratings::of_runs(&ratings::read(&js, Some(&dev_b)).await.unwrap());
    assert_eq!(by_b.keys().collect::<Vec<_>>(), [&second.run], "the ratings of one conductor");
    super::put(&a_js, &first.key(), &first).await;
    let detail = runs::detail(&js, &first.run).await.unwrap();
    assert_eq!(detail.ratings, [worked.clone(), release_rated.clone()]);
    let text = runs::render(&detail, now);
    let line = "\nratings:\n  deploy worked 2h ago  task: ship the web app  why: it shipped without a hitch\n";
    assert!(text.contains(line), "{text}");

    let purged = saved::remove(&js, &dir.join("trails"), &name).await.unwrap();
    assert_eq!((purged.tips, purged.ratings), (0, 2));
    let left = ratings::read(&js, None).await.unwrap();
    assert_eq!(left, [release_rated], "another trail's ratings stay");
}
