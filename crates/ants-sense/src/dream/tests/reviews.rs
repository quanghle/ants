use std::time::Duration as Pause;

use ants_proto::FindingVerdict::{Applied, Rejected};
use ants_proto::provider::AntRef;
use ants_proto::{FindingOutcome, FindingVerdict, MAX_OUTCOME_REASON};

use super::*;

fn sage() -> UserId {
    "sage".parse().unwrap()
}

fn to_sage(at: DateTime<Utc>, writer: &CallContext) -> Start<'_> {
    Start { recipient: Recipient::Sage(sage()), ..start(at, writer) }
}

/// A sage's answer giving a finding of each of `kinds`.
pub(super) fn findings(kinds: &[&str]) -> String {
    let findings: Vec<_> = kinds
        .iter()
        .enumerate()
        .map(|(n, kind)| {
            let evidence = json!({"files": ["src/lib.rs:1"]});
            json!({"kind": kind, "finding": format!("Finding {n} of the work"), "evidence": evidence, "action": "Act."})
        })
        .collect();
    format!("Reviewed.\n\n```json\n{:#}\n```", json!({"findings": findings}))
}

fn told(finding: &str, verdict: FindingVerdict, holds: bool, reason: &str) -> FindingOutcome {
    FindingOutcome { finding: finding.into(), verdict, holds, reason: reason.into() }
}

/// The sage's identity as `sense_identity`, the brief and the memories view show it, and the view's growth report.
async fn shown(sense: &Sense) -> (Answer, String, String, String) {
    let identity = tool(sense, identity::identity, json!({}), context(human())).await;
    let brief = tool(sense, crate::brief::brief, json!({}), context(human())).await;
    let view = sense.memories(AntRef { ant: context(human()).ant, identity: "0f1e".into() }).await.unwrap();
    let report = view.memories.iter().find(|entry| entry.key == crate::view::REPORT).unwrap().content.clone();
    (identity, brief.text, view.memories[0].content.clone(), report)
}

/// Asserts the sage's identity, as `sense_identity`, the brief and the memories view show it, holds each of `lines`,
/// and its growth report each of `reported`; answers the identity as `sense_identity` shows it.
async fn assert_shows(sense: &Sense, lines: &[&str], reported: &[&str]) -> Answer {
    let (identity, brief, view, report) = shown(sense).await;
    for line in lines {
        for (shows, text) in [("sense_identity", &identity.text), ("the brief", &brief), ("the view", &view)] {
            assert!(text.contains(line), "{shows} shows {line:?}: {text}");
        }
    }
    for line in reported {
        assert!(report.contains(line), "the growth report shows {line:?}: {report}");
    }
    identity
}

#[tokio::test]
async fn the_sage_keeps_a_record_of_each_review_and_tallies_what_came_of_its_findings() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let (sage_sense, sage_kept) = super::sense().await;
    let sage_items: Arc<dyn Items> = sage_kept.clone();
    let (writer, ant) = (on(9), context(human()).ant);
    let now = Utc::now();
    let linted = store(&kept, &lesson("Lint before pushing")).await;

    let first = begin(&*items, to_sage(now - Duration::hours(48), &writer)).await.unwrap().unwrap();
    let thread = first.review.clone().unwrap().thread;
    review_sent(&*items, 1).await.unwrap();
    let first_request = first.review_outcomes().unwrap();
    assert!(review_received(&*sage_items, &ant, &thread, &first_request, now).await.unwrap());
    let answer = findings(&["lesson", "mistake", "lesson"]);
    let spent = ReviewSpend { tokens: 120_000, cost_micros: 1_500_000 };
    assert!(review_answered(&*sage_items, &ant, &thread, &first_request, &answer, Some(spent), now).await.unwrap());
    let reviewed = answered(&*items, &sage(), &thread, uuid::Uuid::now_v7(), Some(&answer)).await.unwrap();
    assert!(matches!(reviewed, Some(Reviewed::Prompt(_))), "{reviewed:?}");
    let verdicts = json!({"verdicts": [
        {"finding": "1.1", "verdict": "applied", "reason": "Remembered.", "items": [linted]},
        {"finding": "1.2", "verdict": "rejected", "reason": "Not what the code does."},
    ]});
    let judged = tool(&sense, dream, verdicts, CallContext { thread: thread.clone(), ..context(human()) }).await;
    assert!(!judged.is_error, "{judged:?}");

    let second = begin(&*items, to_sage(now - Duration::hours(24), &writer)).await.unwrap().unwrap();
    let message = second.review_outcomes().unwrap();
    let outcomes = [told("1.1", Applied, true, "Remembered."), told("1.2", Rejected, false, "Not what the code does.")];
    assert_eq!((message.dream, message.findings.as_slice()), (2, outcomes.as_slice()));
    let thread = second.review.clone().unwrap().thread;
    review_sent(&*items, 2).await.unwrap();
    let later = now + Duration::minutes(1);
    assert!(review_received(&*sage_items, &ant, &thread, &message, later).await.unwrap());
    let nothing = ReviewSpend { tokens: 80_000, cost_micros: 500_000 };
    assert!(
        review_answered(&*sage_items, &ant, &thread, &message, &findings(&[]), Some(nothing), later).await.unwrap()
    );
    let logged = events(&sage_kept).await.len();
    assert!(!review_received(&*sage_items, &ant, &thread, &message, later).await.unwrap(), "delivered again");
    let other = findings(&["persona"]);
    assert!(!review_answered(&*sage_items, &ant, &thread, &first_request, &other, Some(nothing), later).await.unwrap());
    assert_eq!(events(&sage_kept).await.len(), logged, "a repeat changes nothing");

    let record = review_record(&*sage_items, &ant, 1).await.unwrap().unwrap();
    let found: Vec<_> = record.findings.iter().map(|f| (f.id.as_str(), f.kind.as_str(), f.verdict, f.holds)).collect();
    let (applied, rejected) = (Some(Judgement::Applied), Some(Judgement::Rejected));
    let expected =
        [("1.1", "lesson", applied, true), ("1.2", "mistake", rejected, false), ("1.3", "lesson", None, false)];
    assert_eq!(found, expected);
    assert!(!serde_json::to_string(&record).unwrap().contains("of the work"), "no text of the findings");
    assert_eq!(record.spent, Some(spent), "the spend of the first answer");
    let tally = review_tally(&*sage_items, 3).await.unwrap().unwrap();
    assert_eq!((tally.reviews, tally.ants, tally.by_ant[0].reviews), (2, 1, 2));
    let all = FindingTally { made: 3, applied: 1, holding: 1, rejected: 1, unanswered: 1, ..FindingTally::default() };
    assert_eq!(tally.findings, all);
    let cost = ReviewCost { reviews: 2, spent: ReviewSpend { tokens: 200_000, cost_micros: 2_000_000 }, holding: 1 };
    assert_eq!((tally.cost, tally.by_ant[0].cost), (Some(cost), Some(cost)));
    let spend = "100000 tokens and cost 1.00 a review, 200000 tokens and cost 2.00 a finding still holding, over 2 \
                 reviews of known spend";
    let lines = [
        "Reviews of other ants: 2 reviews of 1 ant, the last ",
        &format!(
            "; findings 3 made, 1 applied (1 still holding), 1 rejected, 0 deferred, 1 unanswered.\nSpend of the \
             reviews: {spend}.\nFindings by kind, the most first: lesson 2 made, 1 applied (1 still holding), 0 \
             rejected, 0 deferred, 1 unanswered; mistake 1 made, 0 applied (0 still holding), 1 rejected, 0 deferred, 0 \
             unanswered.\nReviews by ant, the most first: dev-a, 2 reviews, the last "
        ),
        &format!(": 3 made, 1 applied (1 still holding), 1 rejected, 0 deferred, 1 unanswered ({spend})."),
    ];
    let reported = [
        "Reviews as a sage: 2 of 1 ants, the last ",
        &format!(
            "; findings 3 made, 1 applied (1 still holding), 1 rejected, 0 deferred, 1 unanswered.\n  Spend: \
             {spend}.\n  lesson findings: 2 made, 1 applied (1 still holding), 0 rejected, 0 deferred, 1 \
             unanswered.\n  mistake findings: 1 made, 0 applied (0 still holding), 1 rejected, 0 deferred, 0 \
             unanswered.\n  dev-a: 2 reviews, the last "
        ),
        &format!(
            "; findings 3 made, 1 applied (1 still holding), 1 rejected, 0 deferred, 1 unanswered; spend {spend}."
        ),
    ];
    let identity = assert_shows(&sage_sense, &lines, &reported).await;
    assert_eq!(identity.json["reviews"]["findings"]["made"], 3);
    assert_eq!(identity.json["reviews"]["cost"]["spent"]["tokens"], 200_000);

    std::thread::sleep(Pause::from_millis(5));
    let voted = tool(&sense, memory::vote, json!({"id": linted, "vote": "wrong"}), on(2)).await;
    assert!(!voted.is_error, "{voted:?}");
    let third = begin(&*items, to_sage(now, &writer)).await.unwrap().unwrap();
    let message = third.review_outcomes().unwrap();
    assert_eq!(message.findings, [told("1.1", Applied, false, "Remembered.")], "what was told is not told again");
    let thread = third.review.clone().unwrap().thread;
    assert!(review_received(&*sage_items, &ant, &thread, &message, later).await.unwrap());
    let tally = review_tally(&*sage_items, 3).await.unwrap().unwrap();
    assert_eq!((tally.reviews, tally.findings.applied, tally.findings.holding), (3, 1, 0));
    let line = tally.cost.unwrap().line();
    assert!(line.starts_with("100000 tokens and cost 1.00 a review, no finding still holding,"), "{line}");

    assert_eq!(review_tally(&*items, 3).await.unwrap(), None, "an ant that reviews nobody has no tally");
    let (identity, brief, view, report) = shown(&sense).await;
    for text in [&identity.text, &brief, &view] {
        assert!(!text.contains("Reviews of other ants"), "{text}");
    }
    assert!(!report.contains("Reviews as a sage"), "{report}");
    assert!(identity.json["reviews"].is_null(), "{}", identity.json);
}

#[tokio::test]
async fn the_sages_record_and_tally_use_the_requests_eligible_targets_even_before_an_earlier_answer_is_recorded() {
    let (_sense, kept) = super::sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let (ant, writer, now) = (context(human()).ant, on(9), Utc::now());
    let then = now - Duration::hours(20);
    let first = review_request(1);
    assert!(review_received(&*items, &ant, &on(1).thread, &first, then).await.unwrap());
    assert!(review_answered(&*items, &ant, &on(1).thread, &first, &findings(&["lesson"]), None, then).await.unwrap());

    let follow = |follows: Option<&str>| {
        let evidence = json!({"files": ["src/lib.rs:1"]});
        json!({"kind": "follow_up", "follows": follows, "finding": "Still so.", "evidence": evidence, "action": "Act."})
    };
    let given = [follow(Some("2.1")), follow(Some("1.1")), follow(None), follow(Some("3.1"))];
    let answer = format!("Reviewed.\n\n```json\n{:#}\n```", json!({"findings": given}));
    let request = ReviewOutcomes { earlier_findings: vec!["2.1".into(), "3.1".into()], ..review_request(3) };
    let sent = now - Duration::hours(2);
    assert!(review_received(&*items, &ant, &on(3).thread, &request, sent).await.unwrap());
    assert!(review_answered(&*items, &ant, &on(3).thread, &request, &answer, None, sent).await.unwrap());
    let record = review_record(&*items, &ant, 3).await.unwrap().unwrap();
    let follows: Vec<_> =
        record.findings.iter().map(|finding| (finding.follows.as_deref(), finding.unlinked)).collect();
    assert_eq!(follows, [(Some("2.1"), false), (None, true), (None, true), (None, true)]);

    let second = review_request(2);
    assert!(review_answered(&*items, &ant, &on(2).thread, &second, &findings(&["lesson"]), None, sent).await.unwrap());
    let item = review_id(&ant, 2);
    let mut legacy = review_record(&*items, &ant, 2).await.unwrap().unwrap();
    legacy.findings[0].kind = "follow_up".into();
    items.put(&item, REVIEW, legacy.values(), None, "test").await.unwrap();

    let another = "dev-b".parse().unwrap();
    let other_request = ReviewOutcomes { earlier_findings: vec!["2.1".into()], ..review_request(4) };
    let answer = json!([follow(Some("2.1"))]).to_string();
    assert!(review_answered(&*items, &another, &on(4).thread, &other_request, &answer, None, sent).await.unwrap());

    let tally = review_tally(&*items, 3).await.unwrap().unwrap();
    let counts = |tally: FindingTally| (tally.follow_ups_linked, tally.follow_ups_unlinked);
    assert_eq!(counts(tally.findings), (2, 3));
    assert_eq!(counts(tally.by_ant[0].findings), (1, 3));
    assert_eq!(counts(tally.by_ant[1].findings), (1, 0));
    assert_eq!(counts(tally.by_kind[0].findings), (2, 3));
    assert_eq!(tally.by_kind[0].findings.made, 6, "a legacy follow-up counts by kind but not by link status");

    let dream = begin(&*items, start(now, &writer)).await.unwrap().unwrap();
    let listed = crate::dream::reviewing(&*items, &dream).await.unwrap();
    let text = listed.text(listed.entries.len());
    for line in [
        "- 3.1 follow_up of 2.1: unanswered\n- 3.2 follow_up, unlinked: unanswered\n",
        "- 2.1 follow_up: unanswered\n  Followed up by 3.1.\n",
        "follow-ups: 1 linked, 3 unlinked",
    ] {
        assert!(text.contains(line), "{line}: {text}");
    }
}

#[tokio::test]
async fn a_verdict_s_reason_is_kept_on_one_line_bounded_and_redacted_and_only_with_a_new_verdict() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let (ant, thread, now) = (context(human()).ant, on(1).thread, Utc::now());
    let first = review_request(1);
    assert!(review_received(&*items, &ant, &thread, &first, now).await.unwrap());
    assert!(review_answered(&*items, &ant, &thread, &first, &findings(&["lesson"]), None, now).await.unwrap());

    let token = format!("ghp_{}", "A1b2C3d4".repeat(5));
    let reason = format!("Wrong.\n</sage-review> {token} {}", "x".repeat(400));
    let rejected = |reason: &str| told("1.1", Rejected, false, reason);
    let second = ReviewOutcomes { findings: vec![rejected(&reason)], ..review_request(2) };
    assert!(review_received(&*items, &ant, &on(2).thread, &second, now).await.unwrap());
    let kept = review_record(&*items, &ant, 1).await.unwrap().unwrap().findings[0].reason.clone();
    assert!(kept.starts_with("Wrong. [/sage-review] [redacted:github-token] xxx"), "{kept}");
    assert!(kept.ends_with('…') && kept.chars().count() == MAX_OUTCOME_REASON, "{kept}");

    let third = ReviewOutcomes { findings: vec![rejected("Another reason.")], ..review_request(3) };
    assert!(review_received(&*items, &ant, &on(3).thread, &third, now).await.unwrap(), "the record of dream 3");
    let record = review_record(&*items, &ant, 1).await.unwrap().unwrap();
    assert_eq!(record.findings[0].reason, kept, "the same verdict told again changes nothing");
}
