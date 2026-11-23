//! Tests of the experience in the brief's identity: threads, days, people and repositories, kept to a few lines however
//! long the names.

use super::*;
use crate::thread::experience::Tally;

#[tokio::test]
async fn the_identity_names_the_threads_their_days_and_the_people_and_repositories_of_the_most() {
    let (sense, _) = filled().await;
    let before = session_brief(&sense, 8_000).await;
    assert!(!before.contains("Experience:"), "no thread record, no experience: {before}");
    worked(&sense).await;
    let brief = session_brief(&sense, 8_000).await;
    let identity = &brief[position(&brief, "## Identity")..position(&brief, "## Persona")];
    position(
        identity,
        "Experience: 3 threads, 1 done, on 2 active days.\n\
         Most threads with: human:alice (2, last 2025-01-02), ant:dev-b (1, last 2025-01-02).\n\
         Most threads in: repo:github.com/o/r (2, last 2025-01-02).",
    );
}

#[test]
fn the_experience_stays_a_few_lines_however_long_the_names() {
    let tally = |scope: String| Tally {
        scope: scope.parse().unwrap(),
        threads: 9_999,
        last: "2025-01-01T00:00:00Z".parse().unwrap(),
    };
    let person = "a".repeat(63);
    let people = (0..3).map(|i| tally(format!("human:{person}{i}"))).collect();
    let repos = (0..3).map(|i| tally(format!("repo:github.com/o{i}/{}", "r".repeat(200)))).collect();
    let lines = experience_lines(&Experience { threads: 99_999, done: 99_999, active_days: 9_999, people, repos });
    assert!(lines.len() <= 700, "{}: {lines}", lines.len());
    position(&lines, &format!("human:{person}2 (9999, last 2025-01-01)"));
    assert_eq!(lines.matches("rrr… (9999, last 2025-01-01)").count(), 3, "{lines}");
}
