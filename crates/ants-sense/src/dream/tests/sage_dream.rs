use ants_proto::FindingVerdict::{Applied, Rejected};
use ants_proto::{FindingOutcome, FindingVerdict};

use super::reviews::findings as answer_of;
use super::*;

/// Note the review of dream `dream` of `ant` on thread `thread`, received at `at` telling `told` of the findings of
/// its earlier reviews, and answered with a finding of each of `kinds`.
async fn review(
    items: &dyn Items,
    review: (&str, u64, u8),
    told: &[(String, FindingVerdict, bool)],
    kinds: &[&str],
    at: DateTime<Utc>,
) {
    review_spending(items, review, told, kinds, None, at).await;
}

/// [`review`], the sage's review thread having spent `spent`.
async fn review_spending(
    items: &dyn Items,
    (ant, dream, thread): (&str, u64, u8),
    told: &[(String, FindingVerdict, bool)],
    kinds: &[&str],
    spent: Option<ReviewSpend>,
    at: DateTime<Utc>,
) {
    let (ant, thread) = (ant.parse().unwrap(), on(thread).thread);
    let findings = told
        .iter()
        .map(|(finding, verdict, holds)| FindingOutcome { finding: finding.clone(), verdict: *verdict, holds: *holds })
        .collect();
    let message = ReviewOutcomes { dream, findings };
    assert!(review_received(items, &ant, &thread, &message, at).await.unwrap());
    assert!(review_answered(items, &ant, &thread, dream, &answer_of(kinds), spent, at).await.unwrap());
}

/// Verdicts on findings `dream`.1 on, one per outcome.
fn told(dream: u64, outcomes: &[(FindingVerdict, bool)]) -> Vec<(String, FindingVerdict, bool)> {
    outcomes.iter().zip(1..).map(|((verdict, holds), n)| (format!("{dream}.{n}"), *verdict, *holds)).collect()
}

/// The practice findings gathered at `at` whose key starts with `kind`, by key with their counts.
async fn found(items: &dyn Items, kind: &str, at: DateTime<Utc>) -> Vec<(String, Option<u64>)> {
    let reviews = review_records(items, usize::MAX).await.unwrap();
    let ages = memory::Ages::new(at, std::iter::empty());
    let gathered = crate::dream::practice_findings::gather(items, &reviews, &ages).await.unwrap();
    gathered.into_iter().map(|found| (found.key(), found.count)).filter(|(key, _)| key.starts_with(kind)).collect()
}

fn keyed(found: &[(&str, u64)]) -> Vec<(String, Option<u64>)> {
    found.iter().map(|(key, count)| ((*key).to_owned(), Some(*count))).collect()
}

/// A sage that gave dev-b `kinds` findings and was told `outcomes` on them at `at`, by the practice findings of `kind`
/// it gathers now.
async fn judged(kinds: &[&str], outcomes: &[(FindingVerdict, bool)], at: DateTime<Utc>, kind: &str) -> Vec<String> {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    review(&*items, ("dev-b", 1, 1), &[], kinds, at - Duration::hours(24)).await;
    review(&*items, ("dev-b", 2, 2), &told(1, outcomes), &[], at).await;
    found(&*items, kind, Utc::now()).await.into_iter().map(|(key, _)| key).collect()
}

#[tokio::test]
async fn a_kind_of_finding_the_ants_reject_seven_times_in_ten_over_fourteen_active_days_is_found() {
    let now = Utc::now();
    let mut kinds = vec!["lesson"; JUDGED_FINDINGS as usize];
    kinds.extend(["mistake"; 3]);
    let verdicts = |rejected: usize| {
        let lessons =
            (0..JUDGED_FINDINGS as usize).map(move |n| if n < rejected { (Rejected, false) } else { (Applied, true) });
        lessons.chain([(Rejected, false); 3]).collect::<Vec<_>>()
    };
    let rejected = REJECTED_FINDINGS as usize;

    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    review(&*items, ("dev-b", 1, 1), &[], &kinds, now - Duration::hours(25)).await;
    review(&*items, ("dev-b", 2, 2), &told(1, &verdicts(rejected)), &[], now - Duration::hours(1)).await;
    assert_eq!(found(&*items, "", now).await, keyed(&[("rejected_kind:lesson", 7)]), "3 mistakes judged are too few");

    assert!(judged(&kinds, &verdicts(rejected - 1), now, "rejected_kind").await.is_empty(), "6 in 10");
    assert!(judged(&kinds[..9], &verdicts(9)[..9], now, "rejected_kind").await.is_empty(), "9 judged");
    let long_ago = now - Duration::days(PRACTICE_DAYS);
    assert!(judged(&kinds, &verdicts(rejected), long_ago, "rejected_kind").await.is_empty(), "told 14 days ago");
}

#[tokio::test]
async fn three_applied_findings_undone_since_are_found_naming_them() {
    let now = Utc::now();
    let kinds = ["lesson", "mistake", "persona", "lesson"];
    let undone = [(Applied, false), (Applied, false), (Applied, false), (Rejected, false)];
    assert_eq!(judged(&kinds, &undone, now, "").await, ["undone_findings:applied"]);

    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    review(&*items, ("dev-b", 1, 1), &[], &kinds, now - Duration::hours(25)).await;
    review(&*items, ("dev-b", 2, 2), &told(1, &undone), &[], now - Duration::hours(1)).await;
    let reviews = review_records(&*items, usize::MAX).await.unwrap();
    let gathered = crate::dream::practice_findings::gather(&*items, &reviews, &memory::Ages::new(now, [])).await;
    let text = &gathered.unwrap()[0].text;
    assert!(text.contains("3 findings given other ants were applied and then undone"), "{text}");
    assert!(text.contains("1.1 lesson of dev-b, 1.2 mistake of dev-b, 1.3 persona of dev-b."), "{text}");

    let holding = [(Applied, false), (Applied, false), (Applied, true), (Rejected, false)];
    assert!(judged(&kinds, &holding, now, "").await.is_empty(), "2 undone");
    let long_ago = now - Duration::days(PRACTICE_DAYS);
    assert!(judged(&kinds, &undone, long_ago, "").await.is_empty(), "told 14 days ago");
}

/// A sage that reviewed `reviews` dreams of dev-b, two findings each, the last at `at`, each review telling the
/// verdicts on the first `answered[n]` findings of the one before.
async fn reviewed(reviews: u64, answered: &[usize], at: DateTime<Utc>) -> Vec<(String, Option<u64>)> {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    for dream in 1..=reviews {
        let before = answered.get(usize::try_from(dream).unwrap().wrapping_sub(2)).copied().unwrap_or(0);
        let told = told(dream - 1, &[(Applied, true); 2][..before]);
        let thread = u8::try_from(dream).unwrap();
        let received = at - Duration::hours(i64::try_from(24 * (reviews - dream)).unwrap());
        review(&*items, ("dev-b", dream, thread), &told, &["lesson", "mistake"], received).await;
    }
    found(&*items, "unanswered_reviews", Utc::now()).await
}

#[tokio::test]
async fn an_ant_leaving_half_the_findings_of_its_last_three_reviews_unanswered_is_found() {
    let now = Utc::now();
    let left_three = [2, 1, 0];
    assert_eq!(reviewed(4, &left_three, now).await, keyed(&[("unanswered_reviews:ant:dev-b", 2)]));
    assert_eq!(reviewed(4, &[0, 0, 0], now).await, keyed(&[("unanswered_reviews:ant:dev-b", 4)]));
    assert!(reviewed(4, &[2, 1, 1], now).await.is_empty(), "2 of 6 left");
    assert!(reviewed(3, &[0, 0], now).await.is_empty(), "the latest review's verdicts come with the next");
    assert!(reviewed(5, &[0, 2, 1, 1], now).await.is_empty(), "the first review is not among the last three");
    assert!(reviewed(4, &left_three, now - Duration::days(PRACTICE_DAYS)).await.is_empty(), "reviewed 14 days ago");
}

/// A sage that reviewed dreams of dev-b once a day, the last at `at`, review `n` spending `spent[n - 1]` (tokens, cost
/// in millionths) and giving a finding, and each telling the verdict on the finding of the one before: applied for
/// the review `applied`, rejected for the others.
async fn spending(spent: &[Option<(u64, u64)>], applied: Option<u64>, at: DateTime<Utc>) -> Vec<(String, Option<u64>)> {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let reviews = spent.len() as u64;
    for (dream, spent) in (1..).zip(spent) {
        let verdict = if applied == Some(dream - 1) { Applied } else { Rejected };
        let told = if dream > 1 { told(dream - 1, &[(verdict, false)]) } else { Vec::new() };
        let spent = spent.map(|(tokens, cost_micros)| ReviewSpend { tokens, cost_micros });
        let received = at - Duration::hours(i64::try_from(24 * (reviews - dream)).unwrap());
        let review = ("dev-b", dream, u8::try_from(dream).unwrap());
        review_spending(&*items, review, &told, &["lesson"], spent, received).await;
    }
    found(&*items, "costly_reviews", Utc::now()).await
}

#[tokio::test]
async fn an_ant_whose_last_five_reviews_cost_too_much_with_no_finding_applied_is_found() {
    let now = Utc::now();
    let tokens = Some((COSTLY_REVIEW_TOKENS + 100_000, 0));
    let costly = keyed(&[("costly_reviews:ant:dev-b", 1)]);
    assert_eq!(spending(&[tokens; 6], None, now).await, costly, "the latest review left out");
    let cost = Some((0, 2 * COSTLY_REVIEW_COST_MICROS));
    assert_eq!(spending(&[cost; 6], None, now).await, keyed(&[("costly_reviews:ant:dev-b", 2)]), "twice the cost");
    let unknown = [tokens, tokens, None, tokens, tokens, tokens, tokens];
    assert_eq!(spending(&unknown, None, now).await, costly, "5 of known spend");
    assert_eq!(spending(&[tokens; 7], Some(1), now).await, costly, "the finding applied was of the first review");

    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let spent = Some(ReviewSpend { tokens: 1_100_000, cost_micros: 1_500_000 });
    for dream in 1..=6 {
        review_spending(&*items, ("dev-b", dream, dream as u8), &[], &[], spent, now).await;
    }
    let reviews = review_records(&*items, usize::MAX).await.unwrap();
    let gathered = crate::dream::practice_findings::gather(&*items, &reviews, &memory::Ages::new(now, [])).await;
    let costly = gathered.unwrap().into_iter().find(|found| found.kind == FindingKind::CostlyReviews).unwrap();
    let text = costly.text;
    assert!(text.contains("of its dreams 1, 2, 3, 4, 5, spent 5500000 tokens and cost 7.50 in all"), "{text}");

    let limit = Some((COSTLY_REVIEW_TOKENS, COSTLY_REVIEW_COST_MICROS));
    assert!(spending(&[limit; 6], None, now).await.is_empty(), "no more than the limit");
    assert!(spending(&[tokens; 6], Some(3), now).await.is_empty(), "a finding applied");
    assert!(spending(&[tokens; 5], None, now).await.is_empty(), "4 reviews before the latest");
    assert!(spending(&[tokens, tokens, None, tokens, tokens, tokens], None, now).await.is_empty(), "4 of known spend");
    let long_ago = now - Duration::days(PRACTICE_DAYS);
    assert!(spending(&[tokens; 6], None, long_ago).await.is_empty(), "reviewed 14 days ago");
}

#[tokio::test]
async fn a_relationship_record_is_asked_of_each_ant_reviewed_three_times_until_written() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    for (dream, ant) in [(1, "dev-b"), (2, "dev-b"), (1, "dev-c"), (2, "dev-c"), (3, "dev-b")] {
        review(&*items, (ant, dream, dream as u8), &[], &[], now).await;
    }
    let relationship = "no_reviewed_relationship";
    assert_eq!(found(&*items, relationship, now).await, keyed(&[("no_reviewed_relationship:ant:dev-b", 3)]));
    review(&*items, ("dev-c", 3, 3), &[], &[], now).await;
    review(&*items, ("dev-c", 4, 4), &[], &[], now).await;
    let both = [("no_reviewed_relationship:ant:dev-c", 3), ("no_reviewed_relationship:ant:dev-b", 3)];
    assert_eq!(found(&*items, relationship, now).await, keyed(&both), "the most reviewed first");

    let record = json!({"relationship": "Cites file and line.", "who": "ant:dev-c", "title": "dev-c"});
    assert!(!tool(&sense, crate::identity::identity, record, context(human())).await.is_error);
    assert_eq!(found(&*items, relationship, now).await, keyed(&both[1..]));
}

#[tokio::test]
async fn a_finding_on_the_practice_applied_names_what_it_asks_and_rests_while_its_ground_is_unchanged() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let writer = on(90);
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    for dream in 1..=4 {
        review(&*items, ("dev-b", dream, dream as u8), &[], &["lesson"], day(-1)).await;
    }
    let lesson = store(&kept, &lesson("Review dev-b by its commits")).await;

    let first = begin(&*items, start(day(0), &writer)).await.unwrap().unwrap();
    let found: Vec<_> = first.findings.iter().map(|f| (f.id.as_str(), f.kind)).collect();
    assert_eq!(found, [("1.1", FindingKind::UnansweredReviews), ("1.2", FindingKind::NoReviewedRelationship)]);
    woken(&*items, 1, &on(91).thread).await.unwrap();
    let applied = |finding: &str, item: &serde_json::Value| json!({"verdicts": [{"finding": finding, "verdict": "applied", "reason": "Kept.", "items": [item]}]});
    let refused = tool(&sense, dream, applied("1.2", &json!(lesson)), on(91)).await;
    assert_eq!(error(&refused), "invalid_arguments");
    assert_eq!(
        refused.text,
        "Finding 1.2 (no_reviewed_relationship): applied names the relationship record of ant:dev-b written; none of \
         the items given is."
    );
    let other = json!({"relationship": "Terse.", "who": "ant:dev-c", "title": "dev-c"});
    let other = tool(&sense, crate::identity::identity, other, context(human())).await.json["id"].clone();
    assert_eq!(error(&tool(&sense, dream, applied("1.1", &other), on(91)).await), "invalid_arguments", "not dev-b");
    assert!(!tool(&sense, dream, applied("1.1", &json!(lesson)), on(91)).await.is_error, "a lesson answers it");

    let second = begin(&*items, start(day(1), &writer)).await.unwrap().unwrap();
    let found: Vec<_> = second.findings.iter().map(|f| (f.id.as_str(), f.kind)).collect();
    assert_eq!(found, [("1.2", FindingKind::NoReviewedRelationship)], "applied, 1.1 rests though its ground holds");
    for dream in 5..=7 {
        review(&*items, ("dev-b", dream, dream as u8), &[], &["lesson", "mistake"], day(1)).await;
    }
    let third = begin(&*items, start(day(2), &writer)).await.unwrap().unwrap();
    let found: Vec<_> = third.findings.iter().map(|f| (f.kind, f.count)).collect();
    assert!(found.contains(&(FindingKind::UnansweredReviews, Some(4))), "5 left unanswered, it changed: {found:?}");
}

#[tokio::test]
async fn a_sage_s_dream_lists_its_reviews_by_their_records_and_holds_none_of_their_text() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    review(&*items, ("dev-b", 1, 1), &[], &["lesson", "mistake"], now - Duration::hours(30)).await;
    let undone = told(1, &[(Applied, false), (Rejected, false)]);
    review(&*items, ("dev-b", 2, 2), &undone, &["persona"], now - Duration::hours(2)).await;
    review(&*items, ("dev-c", 1, 3), &[], &[], now - Duration::hours(1)).await;
    let covered = |n: u8, untrusted: bool, review: bool| Covered { thread: on(n).thread, untrusted, review };
    let writer = on(9);
    let threads = vec![covered(2, true, false), covered(3, true, true), covered(4, false, false)];
    let dream = begin(&*items, Start { threads, ..start(now, &writer) }).await.unwrap().unwrap();
    let reviews: Vec<_> = dream.threads.iter().map(|covered| covered.review).collect();
    assert_eq!(reviews, [true, true, false], "a thread a review record names is a review, whatever its origin");

    let threads = [thread(2, 200, true), thread(3, 200, true), thread(4, 200, false)];
    let text = packet(&*items, &dream, &ant(), &threads).await.unwrap();
    assert!(text.contains(", 1 threads, and the reviews it made of other ants, listed below."), "{text}");
    assert!(text.contains("Ask how the findings of those reviews fared"), "{text}");
    let shown = [
        "## Reviews of other ants",
        "### Dream 1 of dev-c, asked",
        "No findings.\n",
        "### Dream 2 of dev-b, asked",
        "- 2.1 persona: unanswered\n",
        "### Dream 1 of dev-b, asked",
        "- 1.1 lesson: applied, since undone\n- 1.2 mistake: rejected\n",
        "Reviews of other ants: 3 reviews of 2 ants",
    ];
    let at: Vec<_> = shown.iter().map(|line| text.find(line).unwrap_or_else(|| panic!("{line}: {text}"))).collect();
    assert!(at.is_sorted(), "{text}");
    assert!(!text.contains("message 2 of a thread") && !text.contains("message 3 of a thread"), "{text}");
    assert!(text.contains("message 4 of a thread") && !text.contains("(untrusted"), "{text}");

    woken(&*items, 1, &on(10).thread).await.unwrap();
    assert_eq!(untrusted(&*items, &on(10), None).await.unwrap(), None, "the reviews' text is left out");
}
