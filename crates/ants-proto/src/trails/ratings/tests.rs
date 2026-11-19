use chrono::TimeDelta;

use super::*;
use crate::p;

const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

fn day(n: i64) -> DateTime<Utc> {
    "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap() + TimeDelta::days(n)
}

fn name(trail: &str) -> TrailName {
    trail.parse().unwrap()
}

fn rating(trail: &str, outcome: RatingOutcome, task: &str, at: i64) -> TrailRating {
    TrailRating {
        trail: name(trail),
        conductor: p("dev-a"),
        run: RunId::generate(),
        outcome,
        task: task.to_owned(),
        why: "it did".to_owned(),
        script: SHA.to_owned(),
        at: day(at),
    }
}

fn finder(query: &str, trails: &[(&str, &str)]) -> Finder<()> {
    let mut finder = Finder::new(Query::new(query), day(10));
    for (trail, description) in trails {
        finder.trail(name(trail), description, Some(SHA.to_owned()), ());
    }
    finder
}

fn names<T>(found: &[Found<T>]) -> Vec<&str> {
    found.iter().map(|found| found.name.as_str()).collect()
}

#[test]
fn a_rating_is_checked_against_its_key_and_bounds() {
    let good = rating("review-changes", RatingOutcome::Worked, "review a PR", 0);
    let key = good.key();
    assert_eq!(key, format!("review-changes.dev-a.{}", good.run));
    good.check_key(&key).unwrap();
    assert!(matches!(good.check_key(&format!("review-changes.dev-b.{}", good.run)), Err(Error::ForeignRating { .. })));
    let broken = [
        TrailRating { task: " ".to_owned(), ..good.clone() },
        TrailRating { task: "t".repeat(MAX_RATING_TASK + 1), ..good.clone() },
        TrailRating { why: String::new(), ..good.clone() },
        TrailRating { why: "w".repeat(MAX_RATING_WHY + 1), ..good.clone() },
        TrailRating { script: SHA.to_uppercase(), ..good.clone() },
        TrailRating { script: SHA[1..].to_owned(), ..good },
    ];
    for rating in broken {
        assert!(matches!(rating.check(), Err(Error::InvalidRating(_))), "{rating:?}");
    }
}

#[test]
fn outcomes_read_and_write_as_lowercase_words() {
    for outcome in RatingOutcome::ALL {
        assert_eq!(outcome.to_string().parse::<RatingOutcome>().unwrap(), outcome);
        assert_eq!(serde_json::to_value(outcome).unwrap(), outcome.as_str());
    }
    assert!("good".parse::<RatingOutcome>().is_err());
}

#[test]
fn an_old_rating_or_one_of_another_script_weighs_half() {
    let rating = rating("t", RatingOutcome::Worked, "x", 0);
    assert_eq!(rating.weight(Some(SHA), day(89)), 1.0);
    assert_eq!(rating.weight(Some(SHA), day(91)), 0.5);
    assert_eq!(rating.weight(Some("other"), day(1)), 0.5);
    assert_eq!(rating.weight(Some("other"), day(91)), 0.5);
    assert_eq!(rating.weight(None, day(1)), 0.5);
}

#[test]
fn a_query_matches_texts_sharing_two_of_its_stemmed_words_or_all_of_fewer() {
    let query = Query::new("Reviewing the Rust changes");
    assert!(query.matches("review a change"));
    assert!(query.matches("rust reviews"));
    assert!(!query.matches("review a pull request"));
    assert!(query.matches_trail(&name("review-changes"), "every changed file"));
    assert!(Query::new("deploys").matches("deploying the site"));
    assert!(!Query::new("deploys").matches("the site"));
    assert!(Query::new("the").is_empty());
    assert!(Query::new("the").matches("anything"));
}

#[test]
fn evidence_adds_worked_to_alpha_failed_to_beta_and_partly_half_to_each() {
    let mut finder = finder("review rust changes", &[("review-changes", "")]);
    finder.rating(rating("review-changes", RatingOutcome::Worked, "review rust changes", 9));
    finder.rating(rating("review-changes", RatingOutcome::Failed, "review rust changes", 9));
    finder.rating(rating("review-changes", RatingOutcome::Partly, "review rust changes", 9));
    finder.rating(TrailRating {
        script: "f".repeat(64),
        ..rating("review-changes", RatingOutcome::Worked, "rust changes", 9)
    });
    finder.rating(rating("review-changes", RatingOutcome::Failed, "write the docs", 9));
    let evidence = &finder.trails[&name("review-changes")];
    assert_eq!((evidence.alpha, evidence.beta), (3.0, 2.5));
    assert_eq!(evidence.matching.counts, Counts { worked: 2, partly: 1, failed: 1 });
}

#[test]
fn a_summary_keeps_the_latest_ratings_newest_first() {
    let mut summary = RatingSummary::default();
    for at in [3, 1, 5, 2, 4] {
        summary.add(rating("t", RatingOutcome::Worked, &format!("task {at}"), at));
    }
    let ats: Vec<_> = summary.latest.iter().map(|rating| rating.at).collect();
    assert_eq!(ats, [day(5), day(4), day(3)]);
    assert_eq!(summary.counts.total(), 5);
    let json = serde_json::to_value(&summary).unwrap();
    assert_eq!((json["worked"].as_u64(), json["latest"].as_array().map(Vec::len)), (Some(5), Some(3)));
}

#[test]
fn trails_match_by_name_description_or_a_rated_task() {
    let mut search =
        finder("rust review", &[("rust-review", ""), ("checker", "Review Rust code"), ("lint", ""), ("docs", "")]);
    search.rating(rating("lint", RatingOutcome::Worked, "a rust review of the parser", 1));
    search.rating(rating("docs", RatingOutcome::Worked, "write the docs", 1));
    search.rating(rating("gone", RatingOutcome::Worked, "rust review", 1));
    let found = search.rank(&ThreadId::generate(), MAX_FIND_TRAILS);
    let mut found = names(&found);
    found.sort_unstable();
    assert_eq!(found, ["checker", "lint", "rust-review"]);
    let every = finder("", &[("a", ""), ("b", "")]).rank(&ThreadId::generate(), MAX_FIND_TRAILS);
    assert_eq!(every.len(), 2);
}

#[test]
fn a_trail_with_no_rating_on_a_matching_task_is_worth_exploring() {
    let mut finder = finder("rust review", &[("rust-review", ""), ("rust-check", "review")]);
    finder.rating(rating("rust-check", RatingOutcome::Worked, "rust review", 1));
    finder.rating(rating("rust-review", RatingOutcome::Worked, "deploy", 1));
    let found = finder.rank(&ThreadId::generate(), MAX_FIND_TRAILS);
    let exploring: Vec<_> = found.iter().map(|found| (found.name.as_str(), found.exploring())).collect();
    assert!(exploring.contains(&("rust-review", true)) && exploring.contains(&("rust-check", false)));
}

#[test]
fn a_search_ranks_the_same_on_a_thread_and_a_new_trail_leaves_the_draws_of_others() {
    let thread = ThreadId::generate();
    let trails = [("a-review", ""), ("b-review", ""), ("c-review", ""), ("d-review", "")];
    let draws = |found: Vec<Found<()>>| -> BTreeMap<String, f64> {
        found.into_iter().map(|found| (found.name.to_string(), found.draw)).collect()
    };
    let first = draws(finder("review", &trails).rank(&thread, MAX_FIND_TRAILS));
    assert_eq!(first, draws(finder("reviews", &trails).rank(&thread, MAX_FIND_TRAILS)));
    let mut more = trails.to_vec();
    more.push(("e-review", ""));
    let with_more = draws(finder("review", &more).rank(&thread, MAX_FIND_TRAILS));
    assert!(first.iter().all(|(trail, draw)| with_more[trail] == *draw));
    let limited = finder("review", &trails).rank(&thread, 2);
    assert_eq!(limited.len(), 2);
    assert!(limited[0].draw >= limited[1].draw);
}

#[test]
fn a_trail_that_mostly_worked_outranks_one_that_mostly_failed() {
    let mut wins = 0;
    for _ in 0..200 {
        let mut finder = finder("rust review", &[("good-review", "rust"), ("bad-review", "rust")]);
        for n in 0..10 {
            let (good, bad) = if n == 0 {
                (RatingOutcome::Failed, RatingOutcome::Worked)
            } else {
                (RatingOutcome::Worked, RatingOutcome::Failed)
            };
            finder.rating(rating("good-review", good, "rust review", 9));
            finder.rating(rating("bad-review", bad, "rust review", 9));
        }
        if finder.rank(&ThreadId::generate(), 1)[0].name.as_str() == "good-review" {
            wins += 1;
        }
    }
    assert!(wins >= 190, "{wins} of 200");
}

#[test]
fn a_trail_serves_a_task_its_description_matches_with_more_worked_than_failed() {
    assert!(serves(true, &Counts { worked: 2, partly: 0, failed: 1 }));
    assert!(!serves(true, &Counts { worked: 1, partly: 3, failed: 1 }));
    assert!(!serves(false, &Counts { worked: 5, partly: 0, failed: 0 }));
    assert_eq!(Counts { worked: 3, partly: 1, failed: 0 }.rate(), Some(0.875));
    assert_eq!(Counts::default().rate(), None);
}
