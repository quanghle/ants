//! Tests of a sage's track record: earlier findings with their verdicts and what became of them, the signs before and
//! since, active-day windows, and shrinking over the budget.

use std::collections::BTreeMap;
use std::time::Duration as Pause;

use ants_proto::{FindingOutcome, FindingVerdict};

use super::*;
use crate::proposal::{PROPOSAL, Proposal};

pub(super) fn user(name: &str) -> UserId {
    name.parse().unwrap()
}

/// A finding of a sage, of `kind`, first offered as `id`.
pub(super) fn advised(id: &str, kind: &str, text: &str) -> Finding {
    let action = format!("Act on {id}.");
    let advice = Advice { kind: kind.into(), evidence: Evidence::default(), action, follows: None, trail: None };
    Finding { id: id.into(), kind: FindingKind::Sage, text: text.into(), advice: Some(advice), ..finding(1) }
}

pub(super) fn verdict(
    finding: &str,
    verdict: Judgement,
    reason: &str,
    items: &[&ItemId],
    at: DateTime<Utc>,
) -> Verdict {
    let items = items.iter().map(|&id| id.clone()).collect();
    Verdict { finding: finding.into(), verdict, reason: reason.into(), items, trails: Vec::new(), at }
}

/// Dream `number`, begun at `at` with `sage` reviewing, offering `findings` and judged by `verdicts`.
pub(super) fn reviewed(
    number: u64,
    at: DateTime<Utc>,
    sage: &str,
    findings: Vec<Finding>,
    verdicts: Vec<Verdict>,
) -> Dream {
    let since = at - Duration::hours(24);
    Dream { number, at, since, sage: Some(user(sage)), findings, verdicts, ..reviewing(0, 0) }
}

pub(super) async fn keep(items: &dyn Items, dream: &Dream) {
    let put = Write::Put { id: Dream::id(dream.number), kind: DREAM.into(), values: dream.values(), expected: None };
    items.write(&[put], "test").await.unwrap();
}

/// A persona proposal, `status`.
async fn proposed(items: &dyn Items, status: proposal::Status) -> ItemId {
    let proposal = Proposal {
        target: ItemId::new(),
        kind: "persona".into(),
        summary: "A plainer voice.".into(),
        version: Values::new(),
        base: 1,
        reason: "The sage found it.".into(),
        status,
        writer: context(human()),
        finding: None,
        superseded_by: None,
        decided: None,
    };
    let values = serde_json::to_value(&proposal).unwrap().as_object().unwrap().clone();
    items.create(PROPOSAL, values, "test").await.unwrap().id
}

/// The part of `packet` from `heading` to the next heading of its level.
pub(super) fn part<'a>(packet: &'a str, heading: &str) -> &'a str {
    let start = packet.find(heading).unwrap_or_else(|| panic!("{heading:?} in {packet}"));
    let rest = &packet[start + heading.len()..];
    &rest[..rest.find("\n## ").unwrap_or(rest.len())]
}

#[tokio::test]
async fn a_sage_reads_its_earlier_findings_with_their_verdicts_and_what_became_of_what_was_written() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let doubted = store(&kept, &lesson("Retry the flaky job")).await;
    let used = store(&kept, &lesson("Run the linter first")).await;
    let persona = proposed(&*items, proposal::Status::Confirmed).await;
    std::thread::sleep(Pause::from_millis(5));
    let judged = Utc::now();
    std::thread::sleep(Pause::from_millis(5));
    let voted = tool(&sense, memory::vote, json!({"id": doubted, "vote": "wrong"}), on(2)).await;
    assert!(!voted.is_error, "{voted:?}");
    let read = tool(&sense, memory::recall, json!({"id": used}), on(1)).await;
    assert!(!read.is_error, "{read:?}");

    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let elder = advised("1.1", "lesson", "The elder's own finding.");
    let first =
        reviewed(1, day(-3), "elder", vec![elder], vec![verdict("1.1", Judgement::Applied, "Done.", &[], judged)]);
    let second = reviewed(
        2,
        day(-2),
        "sage",
        vec![
            advised("2.1", "lesson", "Flaky jobs pass on a retry."),
            advised("2.2", "mistake", "The parser drops the last line."),
            advised("2.3", "persona", "The voice is too formal."),
        ],
        vec![
            verdict("2.1", Judgement::Applied, "Worth keeping.", &[&doubted], judged),
            verdict("2.2", Judgement::Rejected, "Not what the code does.", &[], judged),
            verdict("2.3", Judgement::Deferred, "Later.", &[], judged),
        ],
    );
    let third = reviewed(
        3,
        day(-1),
        "sage",
        vec![
            advised("2.3", "persona", "The voice is too formal."),
            advised("3.1", "lesson", "Lint before pushing."),
            advised("3.2", "skill", "A release skill would help."),
            advised("3.3", "no_persona", "No persona after 12 threads."),
        ],
        vec![
            verdict("2.3", Judgement::Applied, "Proposed a plainer voice.", &[&persona], judged),
            verdict("3.1", Judgement::Applied, "Remembered.", &[&used], judged),
            verdict("3.3", Judgement::Deferred, "After the release.", &[], judged),
        ],
    );
    for dream in [&first, &second, &third] {
        keep(&*items, dream).await;
    }
    let current = reviewed(4, now, "sage", vec![advised("3.2", "skill", "A release skill would help.")], Vec::new());

    let read = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &ColonyTrails::UNREAD).await.unwrap();
    let earlier = part(&read, "\n## Earlier findings of sage\n");
    assert!(!earlier.contains("elder") && !earlier.contains("1.1"), "another sage's findings stay out: {earlier}");
    let entries = [
        "### Review of dream 3, ",
        "- 3.1 (lesson): Lint before pushing.\n  Suggested action: Act on 3.1.\n  Verdict: applied: Remembered.\n  \
         Written: memory ",
        "\"Run the linter first\": active, read on 1 day, last ",
        "- 3.2 (skill): A release skill would help.\n  Suggested action: Act on 3.2.\n  Verdict: unanswered (offered \
         again below)\n",
        "- 3.3 (no_persona): No persona after 12 threads.\n  Suggested action: Act on 3.3.\n  Verdict: deferred: \
         After the release.\n",
        "### Review of dream 2, ",
        "- 2.1 (lesson): Flaky jobs pass on a retry.\n  Suggested action: Act on 2.1.\n  Verdict: applied: Worth \
         keeping.\n  Written: memory ",
        "\"Retry the flaky job\": wrong since the verdict, voted wrong 1\n",
        "- 2.2 (mistake): The parser drops the last line.\n  Suggested action: Act on 2.2.\n  Verdict: rejected: Not \
         what the code does.\n",
        "- 2.3 (persona): The voice is too formal.\n  Suggested action: Act on 2.3.\n  Verdict: applied: Proposed a \
         plainer voice.\n  Written: proposal ",
        " (persona): confirmed\n",
        "### Tally of the last 30 active days\n\n- lesson: 2 made, 2 applied (1 still holding), 0 rejected, 0 \
         deferred, 0 unanswered.\n- mistake: 1 made, 0 applied (0 still holding), 1 rejected, 0 deferred, 0 \
         unanswered.\n- no_persona: 1 made, 0 applied (0 still holding), 0 rejected, 1 deferred, 0 unanswered.\n- \
         persona: 1 made, 1 applied (1 still holding), 0 rejected, 0 deferred, 0 unanswered.\n- skill: 1 made, 0 \
         applied (0 still holding), 0 rejected, 0 deferred, 1 unanswered.\n- all: 6 made, 3 applied (2 still \
         holding), 1 rejected, 1 deferred, 1 unanswered.\n",
    ];
    let mut rest = earlier;
    for entry in entries {
        let found = rest.find(entry).unwrap_or_else(|| panic!("{entry:?} in {earlier}"));
        rest = &rest[found + entry.len()..];
    }
    assert!(read.contains("finding of kind follow_up"), "{read}");
    assert!(!read.contains("## Verdicts on dream"), "{read}");

    let alone = packet(&*items, &current, &ant(), &[], &ColonyTrails::UNREAD).await.unwrap();
    assert!(alone.contains("## Verdicts on dream 3\n\n- 2.3 (sage): applied: Proposed a plainer voice."), "{alone}");
    assert!(!alone.contains("Earlier findings") && !alone.contains("follow_up"), "{alone}");
}

/// Vital signs of `threads` threads: `cancelled` prompts, `calls` tool calls `failed` of them, `rejected` tool
/// requests and `tokens`.
fn signs(threads: u32, cancelled: u32, (calls, failed): (u32, u32), rejected: u32, tokens: u64) -> Signs {
    let calls = if calls == 0 { BTreeMap::new() } else { BTreeMap::from([("edit".into(), Tally { calls, failed })]) };
    let prompts = Ends { replied: threads, cancelled, ..Ends::default() };
    Signs { threads, prompts, calls, rejected, tokens, ..Signs::default() }
}

#[tokio::test]
async fn a_finding_applied_three_dreams_ago_shows_the_signs_of_the_dreams_before_and_since_side_by_side() {
    let (_, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let hours = |n: i64| now - Duration::hours(n);
    let judged = hours(47);
    let dream =
        |number, at, vitals, findings, verdicts| Dream { vitals, ..reviewed(number, at, "sage", findings, verdicts) };
    let earlier = [
        // Beyond the 30 active days the track record covers, yet among the dreams before the finding.
        dream(1, now - Duration::days(40), Some(signs(2, 3, (10, 5), 2, 20_000)), Vec::new(), Vec::new()),
        dream(2, hours(96), None, Vec::new(), Vec::new()),
        dream(3, hours(72), Some(signs(4, 1, (0, 0), 0, 40_000)), Vec::new(), Vec::new()),
        dream(
            4,
            hours(48),
            Some(signs(1, 2, (4, 1), 1, 5_000)),
            vec![advised("4.1", "lesson", "Ask before a long run."), advised("4.2", "mistake", "Wrong branch.")],
            vec![
                verdict("4.1", Judgement::Applied, "Remembered.", &[], judged),
                verdict("4.2", Judgement::Rejected, "It was right.", &[], judged),
            ],
        ),
        dream(5, hours(36), Some(signs(3, 0, (20, 1), 0, 30_000)), Vec::new(), Vec::new()),
        dream(6, hours(24), Some(Signs::default()), Vec::new(), Vec::new()),
    ];
    for dream in &earlier {
        keep(&*items, dream).await;
    }
    let current = dream(7, now, Some(signs(2, 1, (10, 0), 0, 8_000)), Vec::new(), Vec::new());

    let read = packet(&*items, &current, &Reader::Sage(user("dev-a")), &[], &ColonyTrails::UNREAD).await.unwrap();
    let record = part(&read, "\n## Earlier findings of sage\n");
    let applied = "- 4.1 (lesson): Ask before a long run.\n  Suggested action: Act on 4.1.\n  Verdict: applied: \
                   Remembered.\n  Signs of dreams 1, 3, 4 before it was applied | 5, 6, 7 since: threads 2, 4, 1 | 3, \
                   0, 2; prompts cancelled 3, 1, 2 | 0, 0, 1; tool calls failed 50%, -, 25% | 5%, -, 0%; tool requests \
                   rejected 2, 0, 1 | 0, 0, 0; tokens a thread 10000, 10000, 5000 | 10000, -, 4000\n";
    assert!(record.contains(applied), "{record}");
    assert_eq!(record.matches("  Signs of dreams").count(), 1, "only the applied finding shows signs: {record}");
    assert!(read.contains("a few threads make weak evidence"), "the instructions say so: {read}");

    let early = dream(5, hours(36), None, Vec::new(), Vec::new());
    let record = track_record(&*items, &early, &user("sage"), &user("dev-a"), &ColonyTrails::UNREAD).await.unwrap();
    let text = record.text(record.entries.len());
    assert!(text.contains("before it was applied | none since: threads 2, 4, 1 | none;"), "{text}");
}

#[tokio::test]
async fn the_earlier_findings_reach_back_thirty_active_days_the_dormant_ones_not_counted() {
    let (_, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let lifecycle = |n: i64, kind: EventKind| Event {
        id: uuid::Uuid::now_v7(),
        ant: "dev-a".parse().unwrap(),
        identity: "id1".into(),
        thread: None,
        at: day(n),
        kind,
    };
    for (n, kind) in [(-100, EventKind::AntAdded), (-35, EventKind::AntRetired), (-5, EventKind::AntRevived)] {
        identity::record_event(&*items, &lifecycle(n, kind)).await.unwrap();
    }
    let old = reviewed(1, day(-70), "sage", vec![advised("1.1", "lesson", "Forty active days ago.")], Vec::new());
    let kept_one = reviewed(2, day(-40), "sage", vec![advised("2.1", "lesson", "Ten active days ago.")], Vec::new());
    keep(&*items, &old).await;
    keep(&*items, &kept_one).await;

    let current = reviewed(3, now, "sage", Vec::new(), Vec::new());
    let record = track_record(&*items, &current, &user("sage"), &user("dev-a"), &ColonyTrails::UNREAD).await.unwrap();
    let text = record.text(record.entries.len());
    assert!(text.contains("Ten active days ago.") && !text.contains("Forty active days ago."), "{text}");
    assert!(text.contains("- lesson: 1 made, 0 applied (0 still holding), 0 rejected, 0 deferred, 1 unanswered."));

    let card = identity::card(&*items).await.unwrap().unwrap();
    let ages = memory::Ages::new(now, card.dormant(now));
    let (_, targets) = packet::review_history(&*items, 3, &ages, &user("sage"), &ColonyTrails::UNREAD).await.unwrap();
    assert_eq!(targets, BTreeMap::from([("2.1".into(), "2.1".into())]));

    let none = track_record(&*items, &current, &user("elder"), &user("dev-a"), &ColonyTrails::UNREAD).await.unwrap();
    assert!(none.is_empty(), "{none:?}");
    assert_eq!(none.tally, "\nNo findings of elder in the last 30 active days of dev-a.\n");
}

#[tokio::test]
async fn the_outcomes_told_the_sage_reach_back_ninety_active_days_the_track_record_thirty() {
    let (sense, kept) = sense().await;
    let items: Arc<dyn Items> = kept.clone();
    let now = Utc::now();
    let day = |n: i64| now + Duration::days(n);
    let written = store(&kept, &lesson("Pin the toolchain")).await;
    let applied = |id: &str, items: &[&ItemId], at| vec![verdict(id, Judgement::Applied, "Done.", items, at)];
    let beyond = day(-OUTCOME_DAYS - 1);
    let gone = reviewed(1, beyond, "sage", vec![advised("1.1", "lesson", "Long gone.")], applied("1.1", &[], beyond));
    let far = day(-(TRACK_RECORD_DAYS + OUTCOME_DAYS) / 2);
    let followed = applied("2.1", &[&written], far);
    keep(&*items, &gone).await;
    keep(&*items, &reviewed(2, far, "sage", vec![advised("2.1", "lesson", "Sixty active days ago.")], followed)).await;
    std::thread::sleep(Pause::from_millis(5));
    let voted = tool(&sense, memory::vote, json!({"id": written, "vote": "wrong"}), on(2)).await;
    assert!(!voted.is_error, "{voted:?}");

    let ages = memory::Ages::new(now, std::iter::empty());
    let (told, targets) =
        packet::review_history(&*items, 3, &ages, &user("sage"), &ColonyTrails::UNREAD).await.unwrap();
    assert!(targets.is_empty(), "reporting a verdict beyond the window does not grant a follow-up link");
    let undone = FindingOutcome {
        finding: "2.1".into(),
        verdict: FindingVerdict::Applied,
        holds: false,
        reason: "Done.".into(),
    };
    assert_eq!(told, [undone], "followed for {OUTCOME_DAYS} active days, the older not told");
    let current = reviewed(3, now, "sage", Vec::new(), Vec::new());
    let record = track_record(&*items, &current, &user("sage"), &user("dev-a"), &ColonyTrails::UNREAD).await.unwrap();
    assert!(record.is_empty(), "the packet shows {TRACK_RECORD_DAYS} active days: {record:?}");
}

#[test]
fn over_its_budget_the_earlier_findings_shrink_to_their_tally_before_the_identity_is_cut() {
    let dream = Dream { sage: Some(user("sage")), ..reviewing(3, 5) };
    let threads = [thread(1, 3_000, false), thread(2, 3_000, true), thread(3, 3_000, false)];
    let identity = Identity {
        whole: format!("\n### Persona\n\n{}\n", "Voice: plain.\n".repeat(300)),
        titles: "\nCut to titles for room.\n\n### Persona\n\nFields: Voice.\n".into(),
    };
    let record = |findings: usize, bytes: usize| Section {
        head: "\n## Earlier findings of sage\n\nThe findings.\n".into(),
        entries: (1..=findings).map(|n| format!("- 0.{n} (lesson): {}\n", "x".repeat(bytes))).collect(),
        tally: "\n### Tally of the last 30 active days\n\n- lesson: 10 made.\n".into(),
        noun: "earlier finding",
    };
    let reader = Reader::Sage(user("dev-a"));

    let long = record(30, 400);
    let shared =
        compose(&dream, Earlier::Sage(&long, &Section::default()), &[], &reader, &identity, &threads, PACKET_BUDGET);
    assert!(shared.contains("- 0.19 (lesson)") && !shared.contains("- 0.20 (lesson)"), "an eighth: {shared}");
    assert!(shared.contains("11 earlier findings left out for room.") && shared.contains("message 3"), "{shared}");

    let earlier = record(10, 30);
    let within = |budget| {
        compose(&dream, Earlier::Sage(&earlier, &Section::default()), &[], &reader, &identity, &threads, budget)
    };
    let whole = within(PACKET_BUDGET);
    assert!(whole.contains("- 0.10 (lesson)") && !whole.contains("earlier findings left out"), "{whole}");
    assert!(whole.find("## Earlier findings").unwrap() < whole.find("## Findings").unwrap(), "{whole}");

    let bare = within(whole.len() - 8_500);
    assert!(!bare.contains(" of a thread") && bare.contains(&identity.whole), "{bare}");
    assert!(bare.contains("- 0.10 (lesson)") && !bare.contains("earlier finding left out"), "{bare}");

    let shrunk = within(bare.len() - 1);
    assert!(shrunk.contains("- 0.9 (lesson)") && !shrunk.contains("- 0.10 (lesson)"), "{shrunk}");
    assert!(shrunk.contains("1 earlier finding left out for room.") && shrunk.contains(&identity.whole), "{shrunk}");

    let left_out = "\n10 earlier findings left out for room.\n";
    let tally = within(bare.len() - 10 * "- 0.1 (lesson): \n".len() - 30 * 10 - 1 + left_out.len());
    assert!(tally.contains(left_out) && tally.contains("10 made") && tally.contains(&identity.whole), "{tally}");

    let titles = within(tally.len() - 1);
    assert!(titles.contains(&identity.titles) && titles.contains(left_out), "{titles}");
    assert!(titles.contains("10 made") && titles.contains("Finding number 5"), "{titles}");
}
