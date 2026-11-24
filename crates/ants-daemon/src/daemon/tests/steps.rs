//! Trail steps on the inbox of the ant answering them: a step the ant does not answer with a reply ends with a notice
//! to its conductor on the step's thread, replying to the step, so that the step's call raises in the script; one
//! whose turn asked a human tells the conductor so, and the answer after the human's is the step's reply. A re-ask is
//! answered as the step is.

use super::*;

/// A step of a run `lead` conducts, asking `text` of dev-a on a fresh thread owned by the human.
fn step_of(bus: &Bus, lead: &UserId, text: &str) -> Envelope {
    let step = ants_proto::TrailStep {
        run: ants_proto::RunId::generate(),
        call: 1,
        label: None,
        schema: None,
        trail: None,
        script: None,
        tip_key: None,
        bounds: None,
    };
    Envelope::new(lead.clone(), bus.dev.clone(), text).with_owner(Some(ants_test_support::human())).as_step(step)
}

/// The notice `notice` is, from dev-a to `lead` on the thread of `step` replying to it, of `kind`.
fn assert_step_notice(bus: &Bus, notice: &Envelope, step: &Envelope, kind: ants_proto::NoticeKind) {
    assert_eq!(
        (&notice.from, &notice.to, &notice.thread_id, notice.in_reply_to, notice.notice),
        (&bus.dev, &step.from, &step.thread_id, Some(step.id), Some(kind)),
        "{notice:?}"
    );
    assert!(notice.trail_step.is_none() && notice.escalation.is_none(), "{notice:?}");
}

#[tokio::test]
async fn a_step_to_a_capped_ant_or_out_of_turns_is_turned_away_with_a_notice_to_its_conductor() {
    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    bus.ledger = Arc::new(Ledger::memory(bus.dev.clone(), Caps { daily_tokens: Some(1), ..Caps::default() }));
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [lead] = ants(["lead"]);
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let mut spent = step_of(&bus, &lead, "no turns");
    spent.turn_budget = 0;
    let spent_seq = bus::publish_json(&bus.js, spent.subject(), &spent, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut to_lead).await;
        assert_step_notice(&bus, &notice, &spent, ants_proto::NoticeKind::Unanswered);
        assert_eq!(
            notice.text,
            "[ants] Turn budget spent: the step's thread has no turns left; dev-a does not answer the step."
        );
        assert_eq!(notice.turn_budget, 0, "the notice spends no turn");
        bus.until_acked(spent_seq).await;

        // A day's spend past the cap: the next step is turned away, not escalated.
        let spend = crate::harness::Spend::Tokens(2);
        bus.ledger.record(&ThreadId::generate(), &spend).await.unwrap();
        let capped = step_of(&bus, &lead, "capped");
        let seq = bus::publish_json(&bus.js, capped.subject(), &capped, None).await.unwrap();
        let notice = next_envelope(&mut to_lead).await;
        assert_step_notice(&bus, &notice, &capped, ants_proto::NoticeKind::Unanswered);
        let cap = "Daily token cap of dev-a reached (2 of 1 tokens today; resets at 00:00 UTC)";
        assert_eq!(notice.text, format!("[ants] {cap}; dev-a does not answer the step."));
        bus.until_acked(seq).await;
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert!(log.locked().prompts.is_empty(), "neither step prompted the harness");
    let quiet = Duration::from_millis(200);
    assert!(timeout(quiet, to_human.next()).await.is_err(), "nothing escalated to the human");
    assert!(timeout(quiet, to_lead.next()).await.is_err(), "one notice each");
}

#[tokio::test]
async fn a_re_ask_replying_to_the_ant_s_answer_is_prompted_and_answered() {
    let Some((bus, _to_human)) = Bus::start().await else { return };
    let (launch, log, _) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [lead] = ants(["lead"]);
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let mut again = step_of(&bus, &lead, "again");
    again.in_reply_to = Some(Uuid::now_v7());
    bus::publish_json(&bus.js, again.subject(), &again, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let reply = next_envelope(&mut to_lead).await;
        assert_eq!((&reply.from, reply.in_reply_to, reply.notice), (&bus.dev, Some(again.id), None), "{reply:?}");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().prompts.len(), 1);
}

#[tokio::test]
async fn a_dead_lettered_step_tells_its_conductor_with_the_dead_letter_s_error() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [lead] = ants(["lead"]);
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let step = step_of(&bus, &lead, "doomed");
    bus::publish_json(&bus.js, step.subject(), &step, None).await.unwrap();
    gate.add_permits(MAX_ATTEMPTS as usize);

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let notice = next_envelope(&mut to_lead).await;
        assert_step_notice(&bus, &notice, &step, ants_proto::NoticeKind::GaveUp);
        let gave_up = format!("Gave up on the message from lead after {MAX_ATTEMPTS} deliveries: ");
        assert!(notice.text.starts_with(&gave_up) && notice.text.contains("doomed"), "{}", notice.text);
        assert!(notice.text.contains("`ants dead replay "), "{}", notice.text);
        let owner = next_envelope(&mut to_human).await;
        assert_eq!((owner.notice, owner.text), (notice.notice, notice.text), "the owner is told as well");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(log.locked().doomed_failed, MAX_ATTEMPTS as usize);
    assert_eq!(dead::list(&bus.stream).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_step_whose_prompt_a_human_steered_ends_with_a_steered_notice_to_its_conductor() {
    let Some((bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    log.locked().knobs.steers = true;
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let [lead] = ants(["lead"]);
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let step = step_of(&bus, &lead, "hold a");
    bus::publish_json(&bus.js, step.subject(), &step, None).await.unwrap();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        until("the step's prompt to run", || log.locked().prompted == 1).await;
        let (steer, _) = bus.send(&step.thread_id, "b").await;
        until("the human's message to join", || log.locked().steered.len() == 1).await;
        gate.add_permits(1);
        let notice = next_envelope(&mut to_lead).await;
        assert_step_notice(&bus, &notice, &step, ants_proto::NoticeKind::Steered);
        let by = ants_test_support::human();
        let id = steer.id;
        assert_eq!(
            notice.text,
            format!("[ants] {by} steered the prompt answering the step with message {id}, which takes the answer.")
        );
        let answer = next_envelope(&mut to_human).await;
        assert_eq!(answer.in_reply_to, Some(steer.id), "the answer goes to the human");
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
}

#[tokio::test]
async fn a_step_whose_turn_asked_a_human_is_answered_by_the_ant_s_answer_after_the_human_s() {
    use ants_proto::{NoReplyWhy, TimelineEntry};

    let Some((mut bus, mut to_human)) = Bus::start().await else { return };
    let (launch, log, gate) = fake::launch();
    let pool = Pool::new(launch, SessionStore::memory(), 2, IDLE);
    let home = bus._server.dir().join("asked");
    std::fs::create_dir_all(&home).unwrap();
    let (outbox, colony) = (crate::mcp::jetstream_outbox(bus.js.clone()), crate::mcp::jetstream_colony(bus.js.clone()));
    let notes = Arc::new(crate::stream::notes::JetStreamNotes::new(bus.js.clone(), bus.dev.clone()));
    let mcp = Mcp::start(bus.dev.clone(), &home, outbox, colony, notes, bus.humans.clone(), None).await;
    bus.mcp = Some(mcp.unwrap());
    let [lead] = ants(["lead"]);
    let [mut to_lead] = inboxes(&bus, std::slice::from_ref(&lead).try_into().unwrap()).await;
    let step = step_of(&bus, &lead, "hold: deploy?");
    bus::publish_json(&bus.js, step.subject(), &step, None).await.unwrap();
    let human = ants_test_support::human();

    let ctrl_c = Arc::new(Semaphore::new(0));
    let check = async {
        let mcp = bus.mcp.as_ref().unwrap();
        until("the step's prompt to run", || log.locked().prompted == 1).await;
        let questions = serde_json::json!({"questions": [{"question": "Ship it?", "options": ["yes", "no"]}]});
        let asked = call_tool(mcp, &step.thread_id, "ask_human", questions).await;
        assert_ne!(asked.is_error, Some(true), "{asked:?}");
        assert!(format!("{asked:?}").contains("This turn sends the trail step no reply"), "{asked:?}");
        let question = next_envelope(&mut to_human).await;
        assert!(question.question.is_some(), "{question:?}");
        gate.add_permits(1);
        let notice = next_envelope(&mut to_lead).await;
        assert_step_notice(&bus, &notice, &step, ants_proto::NoticeKind::Asked);
        assert_eq!(notice.text, format!("[ants] dev-a asked {human} and answers the step once answered."));
        let (message, entry) = no_reply_on(&bus, &step.thread_id).await;
        assert_eq!(message, step.id);
        let TimelineEntry::NoReply { why, reason, .. } = entry else { panic!("{entry:?}") };
        assert_eq!((why, reason), (NoReplyWhy::Asked, Some(format!("asked {human}"))));

        // The human's answer: the ant's answer to it is the step's reply, to the conductor.
        bus.send(&step.thread_id, "hold: yes").await;
        until("the answer's prompt to run", || log.locked().prompted == 2).await;
        gate.add_permits(1);
        let reply = next_envelope(&mut to_lead).await;
        assert_eq!(
            (&reply.from, &reply.thread_id, reply.in_reply_to, reply.notice, reply.turn_budget),
            (&bus.dev, &step.thread_id, Some(step.id), None, step.turn_budget - 1),
            "{reply:?}"
        );
        assert!(reply.text.ends_with("hold: yes"), "{}", reply.text);

        // Answered, the step waits no more: the human's next message is answered to the human.
        bus.send(&step.thread_id, "hold: thanks").await;
        until("the next prompt to run", || log.locked().prompted == 3).await;
        gate.add_permits(1);
        assert_eq!(next_reply(&mut to_human).await, (step.thread_id.clone(), "hold: thanks".to_owned()));
        ctrl_c.add_permits(1);
    };
    tokio::join!(bus.serve(&pool, &ctrl_c), check);
    assert_eq!(bus.unacked().await, 0);
    assert!(timeout(Duration::from_millis(200), to_lead.next()).await.is_err(), "the step has one reply");
}
