use std::collections::BTreeMap;

use super::*;

fn calls(kinds: &[(&str, u32, u32)]) -> BTreeMap<String, Tally> {
    kinds.iter().map(|(kind, calls, failed)| ((*kind).to_owned(), Tally { calls: *calls, failed: *failed })).collect()
}

fn replied(replied: u32) -> Signs {
    Signs { threads: 1, prompts: Ends { replied, ..Ends::default() }, ..Signs::default() }
}

async fn begun(items: &dyn Items, at: DateTime<Utc>, vitals: Vec<(ThreadId, Signs)>) -> Dream {
    let writer = on(9);
    begin(items, Start { vitals, ..start(at, &writer) }).await.unwrap().unwrap()
}

fn keys(dream: &Dream) -> Vec<(&str, Option<&str>)> {
    dream.findings.iter().map(|finding| (finding.id.as_str(), finding.key.as_deref())).collect()
}

#[tokio::test]
async fn a_dream_keeps_the_vital_signs_of_its_period_and_its_packet_shows_them_beside_the_dreams_before() {
    let (_sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let first = Signs { calls: calls(&[("execute", 10, 1)]), prompt_ms: 60_000, ..replied(4) };
    begun(&*items, day(0), vec![(on(1).thread, first)]).await;
    let second = Signs { done: 1, calls: calls(&[("read", 10, 0)]), ..replied(3) };
    begun(&*items, day(1), vec![(on(2).thread, second)]).await;

    let worked = Signs {
        threads: 1,
        prompts: Ends { replied: 2, cancelled: 1, capped: 1, ..Ends::default() },
        calls: calls(&[("execute", 8, 3), ("read", 4, 0)]),
        prompt_ms: 754_000,
        tokens: 12_345,
        cost_micros: 1_700_000,
        asked: 2,
        rejected: 1,
        rebuilt: 1,
        sense_calls: 2,
        ..Signs::default()
    };
    let finished = Signs { done: 1, escalations: 1, ..replied(1) };
    let vitals = vec![(on(3).thread, worked.clone()), (on(4).thread, finished.clone())];
    let third = begun(&*items, day(2), vitals).await;
    let kept = third.vitals.clone().unwrap();
    assert_eq!((kept.threads, kept.done), (2, 1));
    assert_eq!(kept.prompts, Ends { replied: 3, cancelled: 1, capped: 1, ..Ends::default() });
    assert_eq!(kept.all_calls(), Tally { calls: 12, failed: 3 });
    assert_eq!((kept.rejected, kept.asked, kept.escalations), (1, 2, 1));
    assert!(third.findings.is_empty(), "no sign turned sharply: {:?}", third.findings);
    assert_eq!(get(&*items, 3).await.unwrap().unwrap().0.vitals, Some(kept), "the dream's item keeps them");

    let threads = [
        packet::Thread { signs: worked, ..thread(3, 200, false) },
        packet::Thread { signs: finished, ..thread(4, 200, false) },
    ];
    for reader in [ant(), Reader::Sage(on(1).ant)] {
        let shown = packet(&*items, &third, &reader, &threads, &ColonyTrails::UNREAD).await.unwrap();
        let rows = [
            "| | dream 1 | dream 2 | dream 3 |",
            "| threads worked | 1 | 1 | 2 |",
            "| prompts replied | 4 | 3 | 3 |",
            "| prompts cancelled by a human | 0 | 0 | 1 |",
            "| prompts cut by a cap | 0 | 0 | 1 |",
            "| time in prompts | 1m 00s | 0 | 12m 34s |",
            "| tool calls | 10 | 10 | 12 |",
            "| tool calls failed | 1 (10%) | 0 | 3 (25%) |",
            "| cost | 0 | 0 | 1.70 |",
            "| tool requests rejected | 0 | 0 | 1 |",
            "\nTool calls by kind: execute 8 (3 failed), read 4.\n",
        ];
        let lines = [
            format!(
                "- {}: 4 prompts (2 replied, 1 cancelled by a human, 1 cut by a cap), 12 tool calls (3 failed), \
                 12m 34s in prompts, 12345 tokens, cost 1.70, 2 tool requests asked, 1 rejected, 1 session rebuilt, \
                 2 sense tool calls.\n",
                on(3).thread
            ),
            format!("- {}: 1 prompt (1 replied), 1 escalation, marked done.\n", on(4).thread),
        ];
        for part in rows.iter().map(|row| (*row).to_owned()).chain(lines) {
            assert!(shown.contains(&part), "{part} in {shown}");
        }
        assert!(!shown.contains("tool requests expired"), "rows with nothing to count are left out: {shown}");
    }
}

#[test]
fn a_packet_leaves_out_the_vital_signs_by_thread_after_the_threads() {
    let mut dream = reviewing(2, 0);
    dream.vitals = Some(replied(2));
    let threads = [
        packet::Thread { signs: replied(1), ..thread(1, 3_000, false) },
        packet::Thread { signs: replied(1), ..thread(2, 3_000, false) },
    ];
    let compose = |budget| compose(&dream, Earlier::Last(None), &[], &ant(), &Identity::default(), &threads, budget);
    let whole = compose(PACKET_BUDGET);
    assert!(whole.contains("\nBy thread:\n") && whole.contains("| prompts replied | 2 |"), "{whole}");
    let least = compose(0);
    assert!(!least.contains("By thread:") && least.contains("| prompts replied | 2 |"), "{least}");
    let bare = compose(least.len() + 200);
    assert!(!bare.contains("message 1 of a thread") && bare.contains("By thread:"), "{bare}");
}

#[test]
fn each_sign_changes_against_its_mean_in_the_dreams_before() {
    let signs = Signs { calls: calls(&[("execute", 30, 6)]), prompt_ms: 90_000, escalations: 2, ..replied(3) };
    let trend = [
        (1, Signs { calls: calls(&[("execute", 20, 1)]), prompt_ms: 60_000, cost_micros: 500_000, ..replied(2) }),
        (2, Signs { rejected: 1, ..replied(1) }),
    ];
    let changed = "the mean of the 2 dreams before: threads worked 1 (mean 1, +0%); prompts replied 3 (mean 1.5, \
                   +100%); time in prompts 1m 30s (mean 30s, +200%); tool calls 30 (mean 10, +200%); tool calls \
                   failed 20% (mean 5%, +15 points); cost 0.00 (mean 0.25, -100%); tool requests rejected 0 (mean \
                   0.5, -100%); escalations 2 (mean 0)";
    assert_eq!(changes(&signs, &trend).as_deref(), Some(changed), "the share's mean is of the dreams with calls");
    let after_none = changes(&signs, &trend[1..]).unwrap();
    assert!(after_none.starts_with("the mean of the dream before: "), "{after_none}");
    assert!(after_none.contains("; tool calls failed 20%; "), "no dream before with calls: {after_none}");
    assert_eq!(changes(&signs, &[]), None, "no dream before");
    assert_eq!(changes(&Signs::default(), &[(1, Signs::default())]), None, "nothing counts");
}

#[test]
fn each_vital_finding_is_gathered_when_its_ground_holds_and_not_otherwise() {
    let found = |threads: &[(ThreadId, Signs)], trend: &[(u64, Signs)]| -> Vec<(String, String)> {
        crate::dream::vitals::gather(threads, trend)
            .into_iter()
            .map(|gathered| (gathered.key(), gathered.text))
            .collect()
    };
    let trend = [
        (1, Signs { calls: calls(&[("execute", 20, 1)]), ..Signs::default() }),
        (2, Signs { calls: calls(&[("read", 20, 3)]), ..Signs::default() }),
        (3, replied(1)),
    ];
    let ending = |cancelled, capped| Ends { cancelled, capped, ..Ends::default() };
    let below = Signs {
        prompts: ending(CANCELLED_FINDING - 1, CAPPED_FINDING - 1),
        rejected: REJECTED_FINDING - 1,
        expired: 5,
        calls: calls(&[("execute", FAILED_MIN_CALLS - 1, FAILED_MIN_CALLS - 1)]),
        ..Signs::default()
    };
    assert_eq!(found(&[(on(1).thread, below)], &trend), []);

    // The mean share of failed calls before is 10%: 5% and 15%, the dream without calls aside.
    let over = |failed| Signs {
        prompts: ending(2, 1),
        rejected: 2,
        calls: calls(&[("execute", 12, failed), ("read", 8, 0)]),
        ..Signs::default()
    };
    let threads = [
        (on(1).thread, over(5)),
        (on(2).thread, Signs { prompts: ending(1, 1), rejected: 1, ..Signs::default() }),
        (on(3).thread, replied(4)),
    ];
    let all = found(&threads, &trend);
    let keys: Vec<_> = all.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["vitals:cancelled", "vitals:capped", "vitals:rejected", "vitals:failed_calls"]);
    let (one, two) = (on(1).thread, on(2).thread);
    assert!(
        all[0].1.starts_with(&format!("3 prompts were cancelled by a human (2 on thread {one}, 1 on thread {two})"))
    );
    assert!(all[1].1.starts_with(&format!("2 prompts were cut by a cap (1 on thread {one}, 1 on thread {two})")));
    assert!(all[2].1.starts_with(&format!("3 tool requests were rejected (2 on thread {one}, 1 on thread {two})")));
    let failed = format!("5 of 20 tool calls failed, 25%, against 10% in the 2 dreams before (5 on thread {one})");
    assert!(all[3].1.starts_with(&failed), "{}", all[3].1);

    let share = |threads: &[(ThreadId, Signs)], trend: &[(u64, Signs)]| {
        found(threads, trend).iter().any(|(key, _)| key == "vitals:failed_calls")
    };
    assert!(!share(&[(one.clone(), over(3))], &trend), "15% is under twice 10%");
    let fewer = Signs { calls: calls(&[("execute", FAILED_MIN_CALLS - 1, 10)]), ..Signs::default() };
    assert!(!share(&[(one.clone(), fewer)], &trend), "too few calls");
    assert!(!share(&[(one.clone(), over(5))], &trend[2..]), "no dream before with calls");
    assert!(!share(&[(one.clone(), over(5))], &[]), "no dream before");

    let clean = [(1, Signs { calls: calls(&[("execute", 20, 0)]), ..Signs::default() })];
    let failing = |failed| Signs { calls: calls(&[("execute", 20, failed)]), ..Signs::default() };
    assert!(!share(&[(one.clone(), failing(1))], &clean), "5% is under the floor, though the mean before is zero");
    let floor = (FAILED_SHARE_FLOOR * 20.0).ceil() as u32;
    assert!(share(&[(one, failing(floor))], &clean), "the floor makes a finding over a mean of zero");
}

#[tokio::test]
async fn a_vital_finding_comes_again_while_its_ground_holds_and_rests_once_rejected() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let cancelled = |n| Signs { prompts: Ends { cancelled: n, ..Ends::default() }, ..replied(1) };

    let first = begun(&*items, day(0), vec![(on(1).thread, cancelled(3))]).await;
    assert_eq!(keys(&first), [("1.1", Some("vitals:cancelled"))]);
    assert_eq!(first.findings[0].kind, FindingKind::Vitals);
    assert!(first.findings[0].text.contains(&format!("3 on thread {}", on(1).thread)), "{:?}", first.findings);
    let shown = packet(&*items, &first, &ant(), &[], &ColonyTrails::UNREAD).await.unwrap();
    assert!(shown.contains("- 1.1 (vitals): 3 prompts were cancelled by a human"), "{shown}");

    let second = begun(&*items, day(1), vec![(on(2).thread, cancelled(4))]).await;
    assert_eq!(keys(&second), [("1.1", Some("vitals:cancelled"))], "unanswered, it comes again");
    assert!(second.findings[0].text.starts_with("4 prompts"), "with the count of now: {:?}", second.findings);
    assert!(second.findings[0].text.contains(&on(2).thread.to_string()), "{:?}", second.findings);

    let third = begun(&*items, day(2), vec![(on(3).thread, cancelled(2))]).await;
    assert_eq!(keys(&third), [], "its ground gone, it goes");

    let fourth = begun(&*items, day(3), vec![(on(4).thread, cancelled(3))]).await;
    assert_eq!(keys(&fourth), [("4.1", Some("vitals:cancelled"))]);
    woken(&*items, 4, &on(4).thread).await.unwrap();
    let verdicts = json!({"verdicts": [{"finding": "4.1", "verdict": "rejected", "reason": "Cancelled on purpose."}]});
    let answered = answer(&sense, 4, verdicts).await;
    assert!(!answered.is_error, "{answered:?}");
    let fifth = begun(&*items, day(4), vec![(on(5).thread, cancelled(5))]).await;
    assert_eq!(keys(&fifth), [], "rejected, it rests");
}
