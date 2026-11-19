use std::collections::BTreeMap;

use ants_proto::{Counts, FindingOutcome, FindingVerdict, TrailName};

use super::track_record::{advised, keep, part, reviewed, user, verdict};
use super::*;

fn name(name: &str) -> TrailName {
    name.parse().unwrap()
}

fn saved(trail: &str, description: &str, (worked, partly, failed): (u32, u32, u32), at: DateTime<Utc>) -> ColonyTrail {
    let counts = Counts { worked, partly, failed };
    ColonyTrail { name: name(trail), description: description.into(), counts, saved_at: at }
}

#[tokio::test]
async fn a_sage_s_packet_lists_the_colony_s_saved_trails_the_best_rated_first_the_ant_s_none() {
    let (_, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let current = reviewed(1, now, "sage", Vec::new(), Vec::new());
    let trails = ColonyTrails::read(vec![
        saved("flaky-deploy", "Deploy\n  the site.", (0, 1, 3), now),
        saved("old-notes", "", (0, 0, 0), now - Duration::days(9)),
        saved("new-notes", &"Draft notes. ".repeat(40), (0, 0, 0), now),
        saved("release", "Cut a release.", (4, 0, 1), now - Duration::days(30)),
    ]);

    let read = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &trails).await.unwrap();
    let listed = part(&read, "\n## Saved trails of the colony\n");
    let order: Vec<_> = listed.lines().filter_map(|line| line.strip_prefix("- ")?.split(' ').next()).collect();
    assert_eq!(order, ["release", "new-notes", "old-notes", "flaky-deploy"], "{listed}");
    assert!(listed.contains("- flaky-deploy (rated 0 worked, 1 partly, 3 failed): Deploy the site.\n"), "{listed}");
    assert!(listed.contains("- old-notes (rated 0 worked, 0 partly, 0 failed): no description\n"), "{listed}");
    let cut = format!("- new-notes (rated 0 worked, 0 partly, 0 failed): {}…\n", &"Draft notes. ".repeat(40)[..300]);
    assert!(listed.contains(&cut), "{listed}");
    assert!(read.contains("suggests none that a trail the colony saved, listed below, already serves"), "{read}");

    let unread = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &ColonyTrails::UNREAD).await.unwrap();
    let listed = part(&unread, "\n## Saved trails of the colony\n");
    assert!(listed.contains("Not known: the colony's saved trails could not be read"), "{listed}");
    let sage = Reader::Sage(user("dev-a"));
    let none = packet(&*items, &current, &sage, &[], &ColonyTrails::read(Vec::new())).await.unwrap();
    assert!(none.contains("None: the colony saved no trail.\n"), "{none}");

    let alone = packet(&*items, &current, &ant(), &[], &trails).await.unwrap();
    assert!(!alone.contains("## Saved trails"), "{alone}");
}

#[tokio::test]
async fn the_saved_trails_take_a_share_of_the_packet_and_are_left_out_for_room_before_the_earlier_findings() {
    let (_, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let current = reviewed(1, now, "sage", Vec::new(), Vec::new());
    let many: Vec<_> =
        (0..40).map(|n| saved(&format!("trail-{n:02}"), &"Steps. ".repeat(40), (40 - n, 0, 0), now)).collect();
    let read = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &ColonyTrails::read(many)).await.unwrap();
    let listed = part(&read, "\n## Saved trails of the colony\n");
    assert!(listed.len() <= PACKET_BUDGET / TRAILS_SHARE, "{}", listed.len());
    let kept_count = listed.lines().filter(|line| line.starts_with("- ")).count();
    assert!(listed.contains("- trail-00 ") && !listed.contains("- trail-39 "), "the worst rated left out: {listed}");
    assert!(listed.contains(&format!("\n{} saved trails left out for room.\n", 40 - kept_count)), "{listed}");

    let dream = Dream { sage: Some(user("sage")), ..reviewing(0, 2) };
    let record = Section {
        head: "\n## Earlier findings of sage\n\nThe findings.\n".into(),
        entries: (1..=3).map(|n| format!("- 0.{n} (lesson): Earlier.\n")).collect(),
        tally: String::new(),
        noun: "earlier finding",
    };
    let trails = Section {
        head: "\n## Saved trails of the colony\n\n".into(),
        entries: (1..=3).map(|n| format!("- trail-{n} (rated 1 worked, 0 partly, 0 failed): Steps.\n")).collect(),
        tally: String::new(),
        noun: "saved trail",
    };
    let reader = Reader::Sage(user("dev-a"));
    let within =
        |budget| compose(&dream, Earlier::Sage(&record, &trails), &[], &reader, &Identity::default(), &[], budget);
    let whole = within(PACKET_BUDGET);
    assert!(whole.contains("- trail-3 ") && !whole.contains("left out for room"), "{whole}");
    let shrunk = within(whole.len() - 1);
    assert!(shrunk.contains("- trail-2 ") && !shrunk.contains("- trail-3 "), "{shrunk}");
    assert!(shrunk.contains("1 saved trail left out for room.") && shrunk.contains("- 0.3 (lesson)"), "{shrunk}");
    assert!(!shrunk.contains("earlier finding left out"), "{shrunk}");
}

#[tokio::test]
async fn an_applied_trail_finding_holds_while_its_trail_is_saved_and_rated_failed_no_more_often_than_worked() {
    let (_, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let mut findings = Vec::new();
    let mut verdicts = Vec::new();
    for (n, trail) in ["release-notes", "flaky-deploy", "gone"].into_iter().enumerate() {
        let id = format!("1.{}", n + 1);
        findings.push(advised(&id, TRAIL, &format!("Done by hand thrice: {trail}.")));
        let mut applied = verdict(&id, Judgement::Applied, "Saved it.", &[], now - Duration::hours(23));
        applied.trails = vec![name(trail)];
        verdicts.push(applied);
    }
    keep(&*items, &reviewed(1, now - Duration::hours(24), "sage", findings, verdicts)).await;
    let trails = ColonyTrails::read(vec![
        saved("release-notes", "Draft the release notes.", (2, 1, 2), now),
        saved("flaky-deploy", "Deploy the site.", (1, 0, 2), now),
    ]);

    let current = reviewed(2, now, "sage", Vec::new(), Vec::new());
    let read = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &trails).await.unwrap();
    let record = part(&read, "\n## Earlier findings of sage\n");
    for shown in [
        "- 1.1 (trail): Done by hand thrice: release-notes.\n  Suggested action: Act on 1.1.\n  Verdict: applied: Saved \
         it.\n  Saved trails: release-notes (rated 2 worked, 1 partly, 2 failed).\n",
        "  Saved trails: flaky-deploy (rated 1 worked, 0 partly, 2 failed).\n",
        "  Saved trails: gone (no longer saved).\n",
        "- trail: 3 made, 3 applied (1 still holding), 0 rejected, 0 deferred, 0 unanswered.",
    ] {
        assert!(record.contains(shown), "{shown} in {record}");
    }
    assert!(read.contains("A trail finding also holds \"trail\": {\"name\": \"release-notes\""), "{read}");

    let ages = memory::Ages::new(now, std::iter::empty());
    let (told, _) = packet::review_history(&*items, 2, &ages, &user("sage"), &trails).await.unwrap();
    let holds: Vec<_> = told.iter().map(|outcome| (outcome.finding.as_str(), outcome.holds)).collect();
    assert_eq!(holds, [("1.1", true), ("1.2", false), ("1.3", false)]);

    let unknown = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &ColonyTrails::UNREAD).await.unwrap();
    let record = part(&unknown, "\n## Earlier findings of sage\n");
    assert!(
        record.contains("  Saved trails: gone (its ratings not known: the colony's saved trails could not be read).\n")
    );
    assert!(record.contains("- trail: 3 made, 3 applied (3 still holding)"), "unknown holds: {record}");
    let (fresh, _) = packet::review_history(&*items, 2, &ages, &user("sage"), &ColonyTrails::UNREAD).await.unwrap();
    assert!(fresh.iter().all(|outcome| outcome.holds), "never told, by its items alone: {fresh:?}");

    let mut review = Review::new(told, BTreeMap::new());
    review.sent = true;
    keep(&*items, &Dream { review: Some(review), ..reviewed(2, now, "sage", Vec::new(), Vec::new()) }).await;
    let (again, _) = packet::review_history(&*items, 3, &ages, &user("sage"), &ColonyTrails::UNREAD).await.unwrap();
    assert!(again.is_empty(), "not known, as last told: {again:?}");
    let undone = FindingOutcome {
        finding: "1.1".into(),
        verdict: FindingVerdict::Applied,
        holds: false,
        reason: "Saved it.".into(),
    };
    let removed = ColonyTrails::read(Vec::new());
    let (changed, _) = packet::review_history(&*items, 3, &ages, &user("sage"), &removed).await.unwrap();
    assert_eq!(changed, [undone], "its trail no longer saved");
}

#[tokio::test]
async fn a_trail_finding_told_deferred_then_applied_holds_while_the_colony_s_trails_cannot_be_read() {
    let (_, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let findings = vec![advised("1.1", TRAIL, "Done by hand thrice.")];
    let mut applied = verdict("1.1", Judgement::Applied, "Saved it.", &[], now - Duration::hours(23));
    applied.trails = vec![name("release-notes")];
    keep(&*items, &reviewed(1, now - Duration::hours(24), "sage", findings, vec![applied])).await;
    let deferred = FindingOutcome {
        finding: "1.1".into(),
        verdict: FindingVerdict::Deferred,
        holds: false,
        reason: "Later.".into(),
    };
    let mut review = Review::new(vec![deferred], BTreeMap::new());
    review.sent = true;
    keep(&*items, &Dream { review: Some(review), ..reviewed(2, now, "sage", Vec::new(), Vec::new()) }).await;

    let ages = memory::Ages::new(now, std::iter::empty());
    let (told, _) = packet::review_history(&*items, 3, &ages, &user("sage"), &ColonyTrails::UNREAD).await.unwrap();
    let told: Vec<_> = told.iter().map(|outcome| (outcome.finding.as_str(), outcome.verdict, outcome.holds)).collect();
    assert_eq!(told, [("1.1", FindingVerdict::Applied, true)], "a deferral told says nothing of holding");
}
