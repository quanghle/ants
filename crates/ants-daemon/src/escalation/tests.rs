//! Tests of escalations up the origin chain: where they start and climb, leaders' answers, notices, reminders backing
//! off, snoozes, and assistants passing them on to their humans.

use super::*;
use crate::records::heed::Heeded;
use ants_test_support::user as p;

fn t() -> ThreadId {
    "t1".parse().unwrap()
}

fn pending(escalation: Escalation, to: &[&str], renewed: bool) -> Pending {
    Pending {
        escalation,
        to: to.iter().map(|s| p(s)).collect(),
        renewed_by: renewed.then(Uuid::new_v4),
        deadline: None,
        owner: None,
        creator: None,
        origin_thread: None,
        summary: String::new(),
        channel: None,
        question: None,
        sent: None,
    }
}

fn level(e: &Escalation, chain: &[&str]) -> Escalation {
    let chain: Vec<_> = chain.iter().map(|s| p(s)).collect();
    match e.up(&chain, 3) {
        Hop::Chain { escalation, .. } => *escalation,
        Hop::Owner(because) => panic!("to the owner: {because:?}"),
    }
}

fn step(ant: &str, carried: Option<&Escalation>, pending: Option<&Pending>) -> Step {
    step_from(ant, None, carried, pending)
}

fn step_from(ant: &str, origin: Option<&str>, carried: Option<&Escalation>, pending: Option<&Pending>) -> Step {
    let (ant, origin, thread) = (p(ant), origin.map(p), t());
    let fresh = Fresh { ant: &ant, origin: origin.as_ref(), thread: &thread, reason: EscalationReason::Cap };
    next_step(fresh, None, carried, pending)
}

fn climbs(from: Escalation) -> Step {
    Step::Climb(Box::new(from))
}

#[test]
fn a_notice_past_max_payload_is_cut_on_a_char_boundary_with_a_line_telling_so() {
    let notice = Envelope::new(p("dev-a"), p("human"), "short");
    assert_eq!(fit(notice.clone(), 4096), notice, "one that fits is left as it is");
    let long = Envelope::new(p("dev-a"), p("human"), format!("{}\"", "é".repeat(4000)));
    let cut = fit(long, 4096);
    assert!(serde_json::to_vec(&cut).unwrap().len() <= 4096 - 1024);
    let (kept, line) = cut.text.split_once("\n\n[ants] ").expect("a line telling of the cut");
    assert!(kept.chars().all(|c| c == 'é') && !kept.is_empty(), "{kept}");
    assert_eq!(
        line,
        format!("This notice was cut to fit the bus's max_payload: {} of its 8001 bytes are kept.", kept.len())
    );
}

#[test]
fn a_first_escalation_starts_at_the_stuck_ant() {
    let new = Escalation::new(p("dev-a"), t(), EscalationReason::Cap);
    assert_eq!(step("dev-a", None, None), climbs(new));
}

#[test]
fn one_answered_by_its_leader_climbs_and_one_waiting_goes_again() {
    let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
    let one = level(&start, &["lead", "dev-b"]);
    let waiting = pending(one.clone(), &["lead", "dev-b"], false);
    assert_eq!(step("dev-a", None, Some(&waiting)), Step::Again(Box::new(waiting.clone())));
    let climb = step("dev-a", Some(&one), Some(&pending(one.clone(), &["lead"], true)));
    assert_eq!(climb, climbs(Escalation { reason: EscalationReason::Cap, ..one.clone() }));
    let owner = pending(one, &[], false);
    assert_eq!(
        step("dev-a", None, Some(&owner)),
        Step::Again(Box::new(owner.clone())),
        "a human has it until answering"
    );
}

#[test]
fn a_member_stuck_on_an_escalation_climbs_from_it() {
    let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
    let one = level(&start, &["lead", "dev-b"]);
    let expected = Escalation { reason: EscalationReason::Cap, ..one.clone() };
    assert_eq!(step("lead", Some(&one), None), climbs(expected.clone()));
    assert_eq!(step("dev-b", Some(&one), None), climbs(expected));
    let two = level(&one, &["boss"]);
    let lower = pending(one.clone(), &["lead"], false);
    assert_eq!(step("dev-a", Some(&two), Some(&lower)), climbs(Escalation { reason: EscalationReason::Cap, ..two }));
    let other: ThreadId = "t2".parse().unwrap();
    let elsewhere = Escalation { thread_id: other, ..one };
    assert_eq!(step("lead", Some(&elsewhere), None), step("lead", None, None), "another thread's is no record");
}

#[test]
fn a_carried_escalation_a_human_ended_starts_afresh() {
    let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
    let one = level(&start, &["lead"]);
    assert_eq!(step("dev-a", Some(&one), None), climbs(Escalation::new(p("dev-a"), t(), EscalationReason::Cap)));
}

#[test]
fn on_a_thread_another_ant_created_a_fresh_escalation_starts_at_the_creator() {
    let from_lead = Escalation::new_from(p("dev-a"), t(), EscalationReason::Cap, Some(&p("lead")));
    assert_eq!(step_from("dev-a", Some("lead"), None, None), climbs(from_lead.clone()));
    let own = Escalation::new(p("lead"), t(), EscalationReason::Cap);
    assert_eq!(step_from("lead", Some("lead"), None, None), climbs(own), "the creator stuck on its own");

    // Climbing on, and starting at the creator again once a human ended one.
    let one = level(&from_lead, &["boss"]);
    assert_eq!(members_at(&from_lead), [p("lead")]);
    let climb = step_from("boss", Some("lead"), Some(&one), None);
    assert_eq!(climb, climbs(Escalation { reason: EscalationReason::Cap, ..one.clone() }));
    assert_eq!(step_from("dev-a", Some("lead"), Some(&one), None), climbs(from_lead.clone()));

    let text = owner_text(&from_lead, &OwnerBecause::NoChain).unwrap();
    assert_eq!(text, "[ants] The escalation of dev-a starts at lead, which created thread t1 and has no escalate_to.");
}

#[test]
fn only_the_leader_answers_the_stuck_ant() {
    let one = level(&Escalation::new(p("dev-a"), t(), EscalationReason::Asked), &["lead", "dev-b", "dev-c"]);
    let to = |from: &str, to: &str| {
        let envelope = Envelope::on_thread(t(), p(from), p(to), "help").with_escalation(Some(one.clone()));
        route(&p(to), &envelope).map(|(_, route)| route)
    };
    assert_eq!(to("dev-a", "lead"), Some(Route::Stuck(p("dev-a"))));
    assert_eq!(to("dev-a", "dev-b"), Some(Route::Notes));
    assert_eq!(to("dev-b", "dev-c"), Some(Route::Sender), "between members, as any message");
    assert_eq!(to("dev-b", "lead"), Some(Route::Sender));
    assert_eq!(to("lead", "dev-a"), None, "the stuck ant's own escalation");
    assert_eq!(members_at(&one), [p("lead"), p("dev-b"), p("dev-c")]);

    // Any member decides an approval request: no group, no leader.
    let approval = Escalation { level: 1, ..one.clone() };
    let approval = Escalation { reason: EscalationReason::Approval, ..approval };
    let asked = Envelope::on_thread(t(), p("dev-a"), p("dev-b"), "may it?").with_escalation(Some(approval));
    assert_eq!(route(&p("dev-b"), &asked), None);
    assert_eq!(group_note(&p("dev-b"), &asked), None);
}

#[test]
fn notices_say_who_leads_and_why_the_owner_has_it() {
    let start = Escalation::new(p("dev-a"), t(), EscalationReason::TurnBudget);
    let one = level(&start, &["lead", "dev-b"]);
    let text = chain_text(&one, &[p("lead"), p("dev-b")], "Turn budget spent; needs a decision.");
    assert!(text.starts_with("[ants] Escalation on thread t1, level 1: dev-a is stuck (turn budget spent)."));
    assert!(text.contains("Sent to lead, dev-b; lead leads."), "{text}");
    assert!(text.contains("Only lead resolves it, by answering dev-a on thread t1"), "{text}");
    assert!(text.ends_with("\n\nTurn budget spent; needs a decision."));
    assert_eq!(owner_text(&start, &OwnerBecause::NoChain), None, "as before chains");
    assert!(owner_text(&one, &OwnerBecause::HopLimit(1)).unwrap().contains("hop limit (max_escalation_hops = 1)"));
    assert!(owner_text(&one, &OwnerBecause::Visited(p("dev-a"))).unwrap().contains("dev-a had it already"));
}

#[tokio::test]
async fn reminders_back_off_and_stop_after_the_third() {
    let sent = Arc::new(std::sync::Mutex::new(Vec::<Envelope>::new()));
    let into = sent.clone();
    let send: Sender = Arc::new(move |envelope, _id| {
        into.lock().unwrap().push(envelope);
        Box::pin(async { Ok(None) })
    });
    let colony: Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let humans = Registry::with(UserId::human(), &["alice"]);
    let ledger = Arc::new(Ledger::memory(p("dev-a"), crate::limits::caps::Caps::default()));
    let chain = Chain { remind: Some(Duration::from_millis(1)), ..Default::default() };
    let escalator = Escalator::new(p("dev-a"), chain, colony, humans, ledger, send);
    let notice = Envelope::on_thread(t(), p("dev-a"), p("alice"), "stuck").with_owner(Some(p("alice")));
    escalator.escalate(Stuck::new(EscalationReason::Cap, notice, "s-1".to_owned(), None)).await.unwrap();

    let mut waits = Vec::new();
    while let Some(reminder) = escalator.ledger.reminder(&t()).await.unwrap() {
        let now = reminder.at;
        escalator.remind(&t(), reminder, now).await.unwrap();
        waits.push(escalator.ledger.reminder(&t()).await.unwrap().map(|r| r.at - now));
    }
    let day = millis(Duration::from_hours(24));
    assert_eq!(waits, [Some(day), Some(3 * day), None]);
    let sent = sent.lock().unwrap();
    let reminders: Vec<_> = sent.iter().filter(|e| e.text.starts_with("[ants] Reminder:")).collect();
    assert_eq!(reminders.len(), 3);
    assert!(reminders[2].text.contains("This is the last reminder."), "{}", reminders[2].text);
    assert!(!reminders[1].text.contains("last reminder"), "{}", reminders[1].text);
}

#[tokio::test]
async fn a_snooze_holds_the_reminder_back_and_a_read_escalation_ends_them() {
    let stored = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let next = stored.clone();
    let send: Sender = Arc::new(move |_envelope, _id| {
        let seq = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        Box::pin(async move { Ok(Some(seq)) })
    });
    let colony: Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let humans = Registry::with(UserId::human(), &["alice"]);
    let ledger = Arc::new(Ledger::memory(p("dev-a"), crate::limits::caps::Caps::default()));
    let chain = Chain { remind: Some(Duration::from_millis(1)), ..Default::default() };
    let until = chrono::Utc::now() + chrono::TimeDelta::hours(1);
    let state = Arc::new(std::sync::Mutex::new((Heeded::Snoozed(until), Vec::new())));
    let looked = state.clone();
    let heed: Heed = Arc::new(move |human, _thread, seqs| {
        let mut looked = looked.lock().unwrap();
        assert_eq!(human, p("alice"));
        looked.1.push(seqs);
        let heeded = looked.0;
        Box::pin(async move { Ok(heeded) })
    });
    let escalator = Escalator::new(p("dev-a"), chain, colony, humans, ledger, send).with_heed(heed);
    let notice = Envelope::on_thread(t(), p("dev-a"), p("alice"), "stuck").with_owner(Some(p("alice")));
    escalator.escalate(Stuck::new(EscalationReason::Cap, notice, "s-1".to_owned(), None)).await.unwrap();

    let reminder = escalator.ledger.reminder(&t()).await.unwrap().unwrap();
    escalator.remind(&t(), reminder.clone(), reminder.at).await.unwrap();
    let held = escalator.ledger.reminder(&t()).await.unwrap().unwrap();
    assert_eq!((held.at, held.sent), (until.timestamp_millis(), 0), "held back until the snooze ends, uncounted");
    assert_eq!(stored.load(std::sync::atomic::Ordering::Relaxed), 1, "only the escalation went out");

    state.lock().unwrap().0 = Heeded::No;
    escalator.remind(&t(), held.clone(), held.at).await.unwrap();
    state.lock().unwrap().0 = Heeded::Read;
    let reminder = escalator.ledger.reminder(&t()).await.unwrap().unwrap();
    escalator.remind(&t(), reminder.clone(), reminder.at).await.unwrap();
    assert_eq!(escalator.ledger.reminder(&t()).await.unwrap(), None, "no more reminders");
    assert_eq!(stored.load(std::sync::atomic::Ordering::Relaxed), 2, "one reminder went out");
    assert_eq!(state.lock().unwrap().1, [vec![1], vec![1], vec![1, 2]], "checked against what reached alice");
    let ended = escalator.ledger.reset(&t()).await.unwrap();
    assert_eq!(ended.reached, Some(p("alice")), "still telling alice of another human's answer");
}

/// A stuck ant's escalator up `chain`, with an empty colony, and what it sends under which ids.
/// What an escalator sent, under which message ids.
type Recorded = Arc<std::sync::Mutex<Vec<(Envelope, String)>>>;

fn recording(chain: &[&str]) -> (Escalator, Recorded) {
    let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
    let into = sent.clone();
    let send: Sender = Arc::new(move |envelope, id| {
        into.lock().unwrap().push((envelope, id));
        Box::pin(async { Ok(None) })
    });
    let colony: Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let humans = Registry::with(UserId::human(), &["alice"]);
    let ledger = Arc::new(Ledger::memory(p("dev-a"), crate::limits::caps::Caps::default()));
    let chain = Chain { escalate_to: chain.iter().map(|s| p(s)).collect(), after: None, ..Default::default() };
    (Escalator::new(p("dev-a"), chain, colony, humans, ledger, send), sent)
}

/// dev-a stuck on thread t1 for `reason`, on the message escalated under `id`.
fn stuck_on(reason: EscalationReason, id: &str) -> Stuck {
    let notice = Envelope::on_thread(t(), p("dev-a"), p("alice"), "Turn budget spent").with_owner(Some(p("alice")));
    Stuck::new(reason, notice, id.to_owned(), None)
}

#[tokio::test]
async fn a_thread_out_of_turns_escalates_once_per_level_whatever_wakes_the_ant() {
    for chain in [&[][..], &["lead"]] {
        let (escalator, sent) = recording(chain);
        let first = escalator.escalate(stuck_on(EscalationReason::TurnBudget, "e-1")).await.unwrap();
        let sends = sent.lock().unwrap().len();
        assert!(sends >= 1, "{chain:?}");
        // Every later message the ant is woken by, out of turns or allowance, finds the escalation waiting.
        let reasons = [EscalationReason::TurnBudget, EscalationReason::Cap, EscalationReason::Notes];
        for (i, reason) in reasons.into_iter().cycle().take(6).enumerate() {
            let again = escalator.escalate(stuck_on(reason, &format!("e-{}", i + 2))).await.unwrap();
            assert_eq!(again, first, "{chain:?}");
        }
        assert_eq!(sent.lock().unwrap().len(), sends, "nothing sent again up {chain:?}");
        // A retry of the first, as after a failed send, sends it again under the same ids.
        escalator.escalate(stuck_on(EscalationReason::TurnBudget, "e-1")).await.unwrap();
        {
            let sent = sent.lock().unwrap();
            assert_eq!(sent.len(), 2 * sends, "{chain:?}");
            assert_eq!(sent[sends].1, sent[0].1, "{chain:?}");
        }
        // Stuck for another reason, the ant still sends it.
        escalator.escalate(stuck_on(EscalationReason::ContextWindow, "e-9")).await.unwrap();
        assert_eq!(sent.lock().unwrap().len(), 3 * sends, "{chain:?}");
    }

    // Once the leader answered, the next one climbs, to alice, and the ones after wait for her.
    let (escalator, sent) = recording(&["lead"]);
    escalator.escalate(stuck_on(EscalationReason::TurnBudget, "e-1")).await.unwrap();
    let carried = escalator.ledger.pending(&t()).await.unwrap().unwrap().escalation;
    let mut answer = Envelope::on_thread(t(), p("lead"), p("dev-a"), "go on").with_escalation(Some(carried));
    assert!(escalator.arrived(&mut answer, Role::Ant).await.unwrap());
    sent.lock().unwrap().clear();
    for id in ["e-2", "e-3", "e-4"] {
        let up = escalator.escalate(stuck_on(EscalationReason::TurnBudget, id)).await.unwrap();
        assert_eq!(up, Escalated::Owner(p("alice")));
    }
    let sent = sent.lock().unwrap();
    assert_eq!(sent.iter().map(|(e, _)| &e.to).collect::<Vec<_>>(), [&p("alice")], "{sent:?}");
}

#[tokio::test]
async fn a_cap_escalation_left_unanswered_goes_out_again_the_next_utc_day() {
    for chain in [&[][..], &["lead"]] {
        let (escalator, sent) = recording(chain);
        let first = escalator.escalate(stuck_on(EscalationReason::Cap, "e-1")).await.unwrap();
        let sends = sent.lock().unwrap().len();
        assert!(sends >= 1, "{chain:?}");
        escalator.escalate(stuck_on(EscalationReason::Cap, "e-2")).await.unwrap();
        assert_eq!(sent.lock().unwrap().len(), sends, "the same day it waits, up {chain:?}");

        // A day later, still unanswered: the allowance renewed at 00:00 UTC was spent again.
        let mut pending = escalator.ledger.pending(&t()).await.unwrap().unwrap();
        pending.sent.as_mut().unwrap().day = "2000-01-01".to_owned();
        escalator.ledger.set_pending(&t(), Some(pending.clone())).await.unwrap();
        // Out of turns rather than allowance, nothing came back at midnight: it still waits.
        escalator.escalate(stuck_on(EscalationReason::TurnBudget, "e-3")).await.unwrap();
        assert_eq!(sent.lock().unwrap().len(), sends, "{chain:?}");
        let again = escalator.escalate(stuck_on(EscalationReason::Cap, "e-4")).await.unwrap();
        assert_eq!(again, first, "the same members, at the same level, up {chain:?}");
        {
            let sent = sent.lock().unwrap();
            assert_eq!(sent.len(), 2 * sends, "sent again up {chain:?}");
            assert_eq!(sent[sends].0.to, sent[0].0.to, "{chain:?}");
        }
        // And once that day.
        escalator.escalate(stuck_on(EscalationReason::Cap, "e-5")).await.unwrap();
        assert_eq!(sent.lock().unwrap().len(), 2 * sends, "{chain:?}");
        let stored = escalator.ledger.pending(&t()).await.unwrap().unwrap();
        assert_eq!(stored.escalation.level, pending.escalation.level, "{chain:?}");
        assert_ne!(stored.sent.unwrap().day, "2000-01-01", "{chain:?}");
    }
}

mod assistant_first {
    use std::sync::Mutex as StdMutex;

    use ants_proto::{AssistantGrant, ColonyEntry, Question};

    use super::*;
    use crate::people::humans::TestDevice;

    type Sent = Arc<StdMutex<Vec<Envelope>>>;

    fn entry(ant: &str) -> ColonyEntry {
        crate::people::assistant_first::live(&p(ant))
    }

    /// alice's grant naming `assistant`, or withdrawing the one standing with None.
    fn grant(humans: &Registry, device: &TestDevice, assistant: Option<&str>) {
        let mut grant = AssistantGrant::new(p("alice"), assistant.map(p), chrono::Utc::now(), device.jwt(&p("alice")));
        grant.signature = device.sign(&grant.signed());
        humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    }

    /// dev-a's escalator with no chain, alice's assistant pa listed as `colony` has it, and what it sends.
    fn escalator(humans: &Registry, colony: Vec<ColonyEntry>, after: Option<Duration>) -> (Escalator, Sent) {
        let sent = Sent::default();
        let into = sent.clone();
        let send: Sender = Arc::new(move |envelope, _id| {
            into.lock().unwrap().push(envelope);
            Box::pin(async { Ok(None) })
        });
        let colony: Colony = Arc::new(move || {
            let colony = colony.clone();
            Box::pin(async move { Ok(colony) })
        });
        let record: crate::people::assistant_first::RecordOf = Arc::new(|_| Box::pin(async { Some(p("alice")) }));
        let assistants = AssistantFirst::new(humans.clone(), colony.clone(), record);
        let chain = Chain { after, ..Default::default() };
        let ledger = Arc::new(Ledger::memory(p("dev-a"), crate::limits::caps::Caps::default()));
        let escalator = Escalator::new(p("dev-a"), chain, colony, humans.clone(), ledger, send);
        (escalator.with_assistants(assistants), sent)
    }

    fn humans() -> (Registry, TestDevice) {
        let device = TestDevice::default();
        let humans = Registry::with(UserId::human(), &["alice"]).verifying_grants(Some(device.account()));
        grant(&humans, &device, Some("pa"));
        (humans, device)
    }

    fn stuck() -> Stuck {
        let notice = Envelope::on_thread(t(), p("dev-a"), p("alice"), "turn budget spent").with_owner(Some(p("alice")));
        Stuck::new(EscalationReason::TurnBudget, notice, "stuck-1".to_owned(), None)
    }

    fn take(sent: &Sent) -> Vec<Envelope> {
        std::mem::take(&mut *sent.lock().unwrap())
    }

    #[tokio::test]
    async fn a_stuck_escalation_goes_to_the_assistant_which_passes_it_on() {
        let (humans, _device) = humans();
        let (escalator, sent) = escalator(&humans, vec![entry("pa")], None);
        let escalated = escalator.escalate(stuck()).await.unwrap();
        assert_eq!(escalated, Escalated::Assistant { assistant: p("pa"), human: p("alice") });
        let [asked] = take(&sent).try_into().unwrap();
        let carried = asked.escalation.as_deref().unwrap().clone();
        assert_eq!((&asked.to, &carried.leader, &carried.assistant_of), (&p("pa"), &p("pa"), &Some(p("alice"))));

        // Only the assistant passes it on, and only once.
        let passed = carried.passed_on().unwrap();
        let by = |from: &str| {
            Envelope::on_thread(t(), p(from), p("dev-a"), "a human must decide").with_escalation(Some(passed.clone()))
        };
        assert!(!escalator.took_climb(&by("dev-b")).await.unwrap(), "not the assistant");
        assert!(escalator.took_climb(&by("pa")).await.unwrap());
        let [told] = take(&sent).try_into().unwrap();
        assert_eq!(told.to, p("alice"));
        assert!(told.text.contains("pa, the assistant of alice, passes the escalation of dev-a"), "{}", told.text);
        assert!(told.text.contains("a human must decide"), "{}", told.text);
        assert_eq!(told.escalation.as_deref().unwrap().assistant_of, None);
        assert!(escalator.ledger.pending(&t()).await.unwrap().unwrap().at_owner());
        assert!(escalator.took_climb(&by("pa")).await.unwrap());
        assert!(take(&sent).is_empty(), "passed on already");
    }

    #[tokio::test]
    async fn one_the_assistant_does_not_answer_in_time_goes_on_to_its_human() {
        let (humans, _device) = humans();
        let (escalator, sent) = escalator(&humans, vec![entry("pa")], Some(Duration::from_millis(1)));
        escalator.escalate(stuck()).await.unwrap();
        assert!(escalator.ledger.pending(&t()).await.unwrap().unwrap().deadline.is_some());
        take(&sent);
        tokio::time::sleep(Duration::from_millis(5)).await;
        escalator.overdue().await.unwrap();
        let [told] = take(&sent).try_into().unwrap();
        assert_eq!(told.to, p("alice"));
        assert!(told.text.starts_with("[ants] No answer from pa, the assistant of alice"), "{}", told.text);
    }

    #[tokio::test]
    async fn while_the_assistant_cannot_act_it_goes_to_the_human() {
        let paused =
            ColonyEntry { paused: Some(ants_proto::Paused { since: chrono::Utc::now(), by: None }), ..entry("pa") };
        let stale = ColonyEntry { heartbeat_at: chrono::Utc::now() - chrono::Duration::hours(1), ..entry("pa") };
        for colony in [vec![], vec![paused], vec![stale]] {
            let (humans, _device) = humans();
            let (escalator, _sent) = escalator(&humans, colony, None);
            assert_eq!(escalator.escalate(stuck()).await.unwrap(), Escalated::Owner(p("alice")));
        }
        let (humans, device) = humans();
        grant(&humans, &device, None);
        let (escalator, sent) = escalator(&humans, vec![entry("pa")], None);
        assert_eq!(escalator.escalate(stuck()).await.unwrap(), Escalated::Owner(p("alice")), "withdrawn");
        assert_eq!(take(&sent)[0].to, p("alice"));
    }

    #[tokio::test]
    async fn a_question_goes_to_the_assistant_then_to_its_human_as_the_question() {
        let (humans, _device) = humans();
        let (escalator, sent) = escalator(&humans, vec![entry("pa")], Some(Duration::from_millis(1)));
        let mut notice = Envelope::on_thread(t(), p("dev-a"), p("alice"), "which one?");
        notice.question = Some(Question::new("which one?", &["a".to_owned(), "b".to_owned()], true).unwrap().into());
        assert_eq!(escalator.ask_first(&notice).await.unwrap(), Some(p("pa")));
        let [asked] = take(&sent).try_into().unwrap();
        assert_eq!((&asked.to, &asked.question), (&p("pa"), &None));
        let escalation = asked.escalation.as_deref().unwrap();
        assert_eq!((escalation.reason, &escalation.assistant_of), (EscalationReason::Question, &Some(p("alice"))));

        tokio::time::sleep(Duration::from_millis(5)).await;
        escalator.overdue().await.unwrap();
        let [told] = take(&sent).try_into().unwrap();
        assert_eq!((&told.to, &told.question, &told.escalation), (&p("alice"), &notice.question, &None));
        assert!(told.text.contains("the question goes on to alice"), "{}", told.text);
    }

    #[tokio::test]
    async fn an_ant_stuck_after_a_question_to_the_assistant_escalates_as_stuck() {
        let (humans, _device) = humans();
        let (escalator, sent) = escalator(&humans, vec![entry("pa")], Some(Duration::from_millis(1)));
        let mut notice = Envelope::on_thread(t(), p("dev-a"), p("alice"), "which one?");
        notice.question = Some(Question::new("which one?", &["a".to_owned(), "b".to_owned()], true).unwrap().into());
        escalator.ask_first(&notice).await.unwrap();
        take(&sent);

        let escalated = escalator.escalate(stuck()).await.unwrap();
        assert_eq!(escalated, Escalated::Assistant { assistant: p("pa"), human: p("alice") });
        let [asked] = take(&sent).try_into().unwrap();
        assert_eq!(asked.escalation.as_deref().unwrap().reason, EscalationReason::TurnBudget);
        assert!(asked.text.contains("dev-a is stuck (turn budget spent) and would reach alice"), "{}", asked.text);

        tokio::time::sleep(Duration::from_millis(5)).await;
        escalator.overdue().await.unwrap();
        let [told] = take(&sent).try_into().unwrap();
        assert_eq!((&told.to, &told.question), (&p("alice"), &None));
        assert_eq!(told.escalation.as_deref().unwrap().reason, EscalationReason::TurnBudget);
        assert!(told.text.contains("the escalation goes on to alice"), "{}", told.text);
        assert!(told.text.contains("Asked before: which one?"), "{}", told.text);
    }
}
