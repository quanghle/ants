use std::time::Duration as Pause;

use ants_proto::provider::AntRef;
use ants_proto::{FindingOutcome, FindingVerdict};

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

fn told(finding: &str, verdict: FindingVerdict, holds: bool) -> FindingOutcome {
    FindingOutcome { finding: finding.into(), verdict, holds }
}

/// The sage's identity as `sense_identity`, the brief and the memories view show it, and the view's growth report.
async fn shown(sense: &Sense) -> (Answer, String, String, String) {
    let identity = tool(sense, identity::identity, json!({}), context(human())).await;
    let brief = tool(sense, crate::brief::brief, json!({}), context(human())).await;
    let view = sense.memories(AntRef { ant: context(human()).ant, identity: "0f1e".into() }).await.unwrap();
    let report = view.memories.iter().find(|entry| entry.key == crate::view::REPORT).unwrap().content.clone();
    (identity, brief.text, view.memories[0].content.clone(), report)
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
    assert_eq!(first.review_outcomes(), Some(ReviewOutcomes { dream: 1, findings: Vec::new() }));
    let thread = first.review.clone().unwrap().thread;
    review_sent(&*items, 1).await.unwrap();
    let message = first.review_outcomes().unwrap();
    assert!(review_received(&*sage_items, &ant, &thread, &message, now).await.unwrap());
    let answer = findings(&["lesson", "mistake", "lesson"]);
    assert!(review_answered(&*sage_items, &ant, &thread, 1, &answer, now).await.unwrap());
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
    let outcomes = [told("1.1", FindingVerdict::Applied, true), told("1.2", FindingVerdict::Rejected, false)];
    assert_eq!((message.dream, message.findings.as_slice()), (2, outcomes.as_slice()));
    let thread = second.review.clone().unwrap().thread;
    review_sent(&*items, 2).await.unwrap();
    let later = now + Duration::minutes(1);
    assert!(review_received(&*sage_items, &ant, &thread, &message, later).await.unwrap());
    let logged = events(&sage_kept).await.len();
    assert!(!review_received(&*sage_items, &ant, &thread, &message, later).await.unwrap(), "delivered again");
    let other = findings(&["persona"]);
    assert!(!review_answered(&*sage_items, &ant, &thread, 1, &other, later).await.unwrap(), "the first answer stays");
    assert_eq!(events(&sage_kept).await.len(), logged, "a repeat changes nothing");

    assert_eq!(review_records(&*sage_items, 10).await.unwrap().len(), 2);
    let record = review_record(&*sage_items, &ant, 1).await.unwrap().unwrap();
    let found: Vec<_> = record.findings.iter().map(|f| (f.id.as_str(), f.kind.as_str(), f.verdict, f.holds)).collect();
    assert_eq!(
        found,
        [
            ("1.1", "lesson", Some(Judgement::Applied), true),
            ("1.2", "mistake", Some(Judgement::Rejected), false),
            ("1.3", "lesson", None, false)
        ]
    );
    assert!(!serde_json::to_string(&record).unwrap().contains("of the work"), "no text of the findings");
    let tally = review_tally(&*sage_items, 3).await.unwrap().unwrap();
    assert_eq!((tally.reviews, tally.ants, tally.by_ant[0].reviews), (2, 1, 2));
    let all = FindingTally { made: 3, applied: 1, holding: 1, rejected: 1, deferred: 0, unanswered: 1 };
    assert_eq!(tally.findings, all);
    let (identity, brief, view, report) = shown(&sage_sense).await;
    let lines = [
        "Reviews of other ants: 2 reviews of 1 ant, the last ",
        "; findings 3 made, 1 applied (1 still holding), 1 rejected, 0 deferred, 1 unanswered.\nFindings by kind, the \
         most first: lesson 2 made, 1 applied (1 still holding), 0 rejected, 0 deferred, 1 unanswered; mistake 1 \
         made, 0 applied (0 still holding), 1 rejected, 0 deferred, 0 unanswered.\nReviews by ant, the most first: \
         dev-a, 2 reviews, the last ",
    ];
    for line in lines {
        for (shows, text) in [("sense_identity", &identity.text), ("the brief", &brief), ("the view", &view)] {
            assert!(text.contains(line), "{shows} shows {line:?}: {text}");
        }
    }
    assert_eq!(identity.json["reviews"]["findings"]["made"], 3);
    let lines = [
        "Reviews as a sage: 2 of 1 ants, the last ",
        "; findings 3 made, 1 applied (1 still holding), 1 rejected, 0 deferred, 1 unanswered.\n  lesson findings: 2 \
         made, 1 applied (1 still holding), 0 rejected, 0 deferred, 1 unanswered.\n  mistake findings: 1 made, 0 \
         applied (0 still holding), 1 rejected, 0 deferred, 0 unanswered.\n  dev-a: 2 reviews, the last ",
    ];
    for line in lines {
        assert!(report.contains(line), "the growth report shows {line:?}: {report}");
    }

    std::thread::sleep(Pause::from_millis(5));
    let voted = tool(&sense, memory::vote, json!({"id": linted, "vote": "wrong"}), on(2)).await;
    assert!(!voted.is_error, "{voted:?}");
    let third = begin(&*items, to_sage(now, &writer)).await.unwrap().unwrap();
    let message = third.review_outcomes().unwrap();
    assert_eq!(message.findings, [told("1.1", FindingVerdict::Applied, false)], "what was told is not told again");
    let thread = third.review.clone().unwrap().thread;
    assert!(review_received(&*sage_items, &ant, &thread, &message, later).await.unwrap());
    let tally = review_tally(&*sage_items, 3).await.unwrap().unwrap();
    assert_eq!((tally.reviews, tally.findings.applied, tally.findings.holding), (3, 1, 0));

    assert_eq!(review_tally(&*items, 3).await.unwrap(), None, "an ant that reviews nobody has no tally");
    let (identity, brief, view, report) = shown(&sense).await;
    for text in [&identity.text, &brief, &view] {
        assert!(!text.contains("Reviews of other ants"), "{text}");
    }
    assert!(!report.contains("Reviews as a sage"), "{report}");
    assert!(identity.json["reviews"].is_null(), "{}", identity.json);
}
