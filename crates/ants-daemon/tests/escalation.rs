//! Escalations against a real nats-server: kept in `ants_sessions` across a restart, up the chains the colony
//! publishes or the chains bucket keeps, and to the human once the chains loop.

use std::sync::Arc;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::escalation::{self, Chain, Escalated, Escalator, Stuck};
use ants_daemon::limits::caps::{Caps, Ledger};
use ants_daemon::mcp::Colony;
use ants_daemon::people::humans::Registry;
use ants_daemon::records::chains;
use ants_daemon::records::direct_kv::DirectKv;
use ants_proto::{ColonyEntry, Envelope, EscalationReason, Role, StatusEvent, StatusUpdate, ThreadId, UserId, subject};
use ants_test_support::NatsServer as Server;
use async_nats::jetstream;
use futures::StreamExt as _;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);

fn ant(name: &str) -> UserId {
    name.parse().unwrap()
}

/// A colony where lead escalates to boss, and boss back to dev-a.
fn colony() -> Colony {
    Arc::new(|| {
        let now = chrono::Utc::now();
        let entry = move |name: &str, chain: &str| ColonyEntry {
            ant: ant(name),
            machine: "test".to_owned(),
            instance: "run-1".to_owned(),
            harness: "copilot".to_owned(),
            agent: None,
            capabilities: Vec::new(),
            whoami: None,
            profile: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: ants_proto::Activity::Idle,
            handling: 0,
            harnesses: 0,
            escalate_to: vec![ant(chain)],
            sage: None,
            version: ants_proto::ANTS_VERSION.to_owned(),
            perms: ants_proto::PERMISSIONS_VERSION,
            signed_out: None,
            limited: None,
            failing: None,
            fallback: None,
            memory_low: None,
            model: None,
            paused: None,
            drift: Vec::new(),
            mcp: false,
        };
        Box::pin(async move { Ok(vec![entry("lead", "boss"), entry("boss", "dev-a")]) })
    })
}

fn escalator(js: &jetstream::Context, sessions: &DirectKv) -> Escalator {
    let chain = Chain { escalate_to: vec![ant("lead")], after: Some(Duration::from_millis(300)), ..Chain::default() };
    let ledger = Arc::new(Ledger::kv(sessions.clone(), ant("dev-a"), Caps::default()));
    let send = escalation::jetstream_sender(js.clone());
    Escalator::new(ant("dev-a"), chain, colony(), Registry::default(), ledger, send)
}

async fn next(subscriber: &mut async_nats::Subscriber) -> Envelope {
    let msg = timeout(WAIT, subscriber.next()).await.expect("no message").unwrap();
    serde_json::from_slice(&msg.payload).unwrap()
}

#[tokio::test]
async fn an_escalation_climbs_past_its_deadline_after_a_restart_and_goes_to_the_human_once_the_chains_loop() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ant("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let sessions = bus::ensure_sessions(&js, None, &ant("dev-a")).await.unwrap();
    let mut to_lead = client.subscribe(subject::inbox_of(&ant("lead"))).await.unwrap();
    let mut to_boss = client.subscribe(subject::inbox_of(&ant("boss"))).await.unwrap();
    let mut to_human = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
    let thread = ThreadId::generate();
    let mut status = client.subscribe(subject::status(&ant("dev-a"), &thread)).await.unwrap();
    client.flush().await.unwrap();
    let stuck = |text: &str, id: &str| {
        let notice = Envelope::on_thread(thread.clone(), ant("dev-a"), UserId::human(), text);
        Stuck::new(EscalationReason::TurnBudget, notice, id.to_owned(), None)
    };

    let first = escalator(&js, &sessions);
    let escalated = first.escalate(stuck("Turn budget spent.", "e1")).await.unwrap();
    assert_eq!(escalated, Escalated::Chain { level: 1, leader: ant("lead"), to: vec![ant("lead")] });
    let one = next(&mut to_lead).await;
    assert_eq!(one.escalation.as_deref().map(|e| e.level), Some(1));
    // Observers hear it on the ant's status, without the message's text.
    let told = timeout(WAIT, status.next()).await.expect("no status").unwrap();
    let told: StatusEvent = serde_json::from_slice(&told.payload).unwrap();
    let StatusUpdate::Escalated { to, escalation } = told.update else { panic!("{told:?}") };
    assert_eq!((to, escalation.level, escalation.reason), (ant("lead"), 1, EscalationReason::TurnBudget));
    drop(first);

    // Restarted, the ant still escalates past the deadline it stored.
    let escalator = escalator(&js, &sessions);
    let check = async {
        let two = next(&mut to_boss).await;
        let level = two.escalation.as_deref().expect("the escalation").clone();
        assert_eq!((level.level, &level.leader), (2, &ant("boss")));
        assert!(two.text.contains("[ants] No answer from lead on thread"), "{}", two.text);
        assert!(two.text.ends_with("Turn budget spent."), "{}", two.text);

        // Stuck again before boss answers: the same members get it again, at the same level.
        let again = escalator.escalate(stuck("Still stuck.", "e2")).await.unwrap();
        assert_eq!(again, Escalated::Chain { level: 2, leader: ant("boss"), to: vec![ant("boss")] });
        assert!(next(&mut to_boss).await.text.ends_with("Still stuck."));

        // boss answers: a fresh budget once, and no more deadline.
        let mut answer = two.reply(ant("boss"), "go on", ants_proto::Role::Ant).unwrap();
        answer = answer.with_escalation(Some(level));
        answer.turn_budget = 0;
        assert!(escalator.arrived(&mut answer, Role::Ant).await.unwrap());
        assert_eq!(answer.turn_budget, ants_proto::DEFAULT_TURN_BUDGET);
        let mut other = answer.clone();
        other.id = ants_proto::Uuid::now_v7();
        other.turn_budget = 0;
        assert!(!escalator.arrived(&mut other, Role::Ant).await.unwrap(), "once per level");
        assert!(escalator.ledger().deadlines().await.unwrap().is_empty());

        // boss's chain leads back to dev-a, so the human gets the thread.
        let escalated = escalator.escalate(stuck("Spent again.", "e3")).await.unwrap();
        assert_eq!(escalated, Escalated::Owner(UserId::human()));
        let notice = next(&mut to_human).await;
        assert!(notice.text.starts_with("Spent again.\n\n[ants] The escalation of dev-a reached level 2"));
        assert!(notice.text.ends_with("dev-a had it already, so the chains loop."), "{}", notice.text);
        assert_eq!(notice.escalation.as_deref().map(|e| e.level), Some(2));

        let mut human = Envelope::on_thread(thread.clone(), UserId::human(), ant("dev-a"), "carry on");
        assert!(!escalator.arrived(&mut human, Role::Human).await.unwrap());
        assert_eq!(escalator.ledger().pending(&thread).await.unwrap(), None, "a human ends it");
    };
    tokio::select! {
        () = check => {}
        () = escalator.deadlines() => unreachable!(),
    }
    assert!(timeout(Duration::from_millis(500), to_lead.next()).await.is_err(), "lead got level 1 only");
}

#[tokio::test]
async fn an_escalation_climbs_past_a_leader_gone_from_the_colony_through_the_chain_kept_for_it() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ant("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let sessions = bus::ensure_sessions(&js, None, &ant("dev-a")).await.unwrap();
    let kept = bus::ensure_chains(&js).await.unwrap();
    chains::publish(&js, &ant("lead"), &[ant("boss")]).await.unwrap();
    let revision = kept.entry("lead").await.unwrap().unwrap().revision;
    chains::publish(&js, &ant("lead"), &[ant("boss")]).await.unwrap();
    assert_eq!(kept.entry("lead").await.unwrap().unwrap().revision, revision, "kept already");
    chains::publish(&js, &ant("dev-b"), &[]).await.unwrap();
    assert!(kept.get("dev-b").await.unwrap().is_none(), "no chain, no entry");

    let mut to_lead = client.subscribe(subject::inbox_of(&ant("lead"))).await.unwrap();
    let mut to_boss = client.subscribe(subject::inbox_of(&ant("boss"))).await.unwrap();
    client.flush().await.unwrap();
    let chain = Chain { escalate_to: vec![ant("lead")], after: Some(Duration::from_millis(300)), ..Chain::default() };
    let ledger = Arc::new(Ledger::kv(sessions, ant("dev-a"), Caps::default()));
    let offline: Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let escalator = Escalator::new(
        ant("dev-a"),
        chain,
        offline,
        Registry::default(),
        ledger,
        escalation::jetstream_sender(js.clone()),
    )
    .with_chains(chains::jetstream_chains(js.clone()));
    let thread = ThreadId::generate();
    let notice = Envelope::on_thread(thread, ant("dev-a"), UserId::human(), "Turn budget spent.");
    let stuck = Stuck::new(EscalationReason::TurnBudget, notice, "e1".to_owned(), None);
    escalator.escalate(stuck).await.unwrap();
    assert_eq!(next(&mut to_lead).await.escalation.as_deref().map(|e| e.level), Some(1));

    // lead never answers and is not in the colony: its kept chain takes the escalation to boss.
    let climbed = tokio::select! {
        two = next(&mut to_boss) => two,
        () = escalator.deadlines() => unreachable!(),
    };
    let level = climbed.escalation.as_deref().expect("the escalation");
    assert_eq!((level.level, &level.leader), (2, &ant("boss")));
}

#[tokio::test]
async fn an_escalation_on_a_thread_spun_off_a_channel_thread_passes_the_link_on() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ant("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let sessions = bus::ensure_sessions(&js, None, &ant("dev-a")).await.unwrap();
    bus::ensure_links(&js).await.unwrap();
    let mut to_boss = client.subscribe(subject::inbox_of(&ant("boss"))).await.unwrap();
    let mut to_human = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
    client.flush().await.unwrap();
    // lead created the channel thread; dev-a got stuck on a thread spun off it.
    let (channel_thread, thread) = (ThreadId::generate(), ThreadId::generate());
    let notice = Envelope::on_thread(channel_thread.clone(), ant("dev-a"), UserId::human(), "Turn budget spent.")
        .with_creator(Some(ant("lead")))
        .spun_off(thread.clone());
    let stuck = Stuck::new(EscalationReason::TurnBudget, notice, "e1".to_owned(), None);

    let escalator = escalator(&js, &sessions);
    let escalated = escalator.escalate(stuck).await.unwrap();
    assert_eq!(escalated, Escalated::Chain { level: 1, leader: ant("boss"), to: vec![ant("boss")] });
    let linked = |e: &Envelope| (e.thread_id.clone(), e.creator.clone(), e.origin_thread.clone());
    let expected = (thread.clone(), Some(ant("lead")), Some(channel_thread.clone()));
    assert_eq!(linked(&next(&mut to_boss).await), expected);
    let passed = ants_daemon::records::links::passed_on(&js, &channel_thread).await.unwrap();
    assert!(passed.contains(&(ant("dev-a"), ant("boss"))), "{passed:?}");

    // Past the deadline, boss's chain loops back to dev-a: the human's notice keeps the link the escalation kept.
    let notice = tokio::select! {
        notice = next(&mut to_human) => notice,
        () = escalator.deadlines() => unreachable!(),
    };
    assert_eq!(linked(&notice), expected);
}

#[tokio::test]
async fn an_escalation_reaching_the_human_is_pushed_and_reminded_after_a_restart_until_a_human_answers() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ant("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let sessions = bus::ensure_sessions(&js, None, &ant("dev-a")).await.unwrap();
    let mut to_human = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
    client.flush().await.unwrap();
    let escalator = || {
        let chain = Chain { remind: Some(Duration::from_millis(300)), stand_in: Some(ant("bob")), ..Chain::default() };
        let ledger = Arc::new(Ledger::kv(sessions.clone(), ant("dev-a"), Caps::default()));
        let send = escalation::jetstream_sender(js.clone());
        Escalator::new(ant("dev-a"), chain, colony(), Registry::default(), ledger, send)
    };
    let thread = ThreadId::generate();
    let notice = Envelope::on_thread(thread.clone(), ant("dev-a"), UserId::human(), "Turn budget spent.");
    let stuck = Stuck::new(EscalationReason::TurnBudget, notice, "e1".to_owned(), None);

    let first = escalator();
    assert_eq!(first.escalate(stuck).await.unwrap(), Escalated::Owner(UserId::human()));
    let told = next(&mut to_human).await;
    let escalation = told.escalation.as_deref().expect("an escalation, for push sinks").clone();
    assert_eq!((escalation.level, escalation.reason), (0, EscalationReason::TurnBudget));
    assert_eq!(escalation.stand_in, Some(ant("bob")), "for the human's notifications to relay while it is away");
    drop(first);

    // Restarted, the ant still reminds the human of it, on the thread, carrying it.
    let escalator = escalator();
    let check = async {
        let reminded = next(&mut to_human).await;
        assert_eq!((&reminded.thread_id, reminded.escalation.as_deref()), (&thread, Some(&escalation)));
        let start =
            format!("[ants] Reminder: the escalation of dev-a on thread {thread} (turn budget spent) has waited");
        assert!(reminded.text.starts_with(&start), "{}", reminded.text);
        assert!(reminded.text.contains(&format!("`ants ask --thread {thread} @dev-a TEXT`")), "{}", reminded.text);
        assert!(reminded.text.ends_with("\n\nTurn budget spent."), "{}", reminded.text);
        let reminder = loop {
            match escalator.ledger().reminder(&thread).await.unwrap() {
                Some(reminder) if reminder.sent == 0 => tokio::time::sleep(Duration::from_millis(20)).await,
                reminder => break reminder.expect("the next reminder"),
            }
        };
        let tomorrow = chrono::Utc::now().timestamp_millis() + 23 * 3_600_000;
        assert!(reminder.sent == 1 && reminder.at > tomorrow, "daily after the first: {reminder:?}");
        assert_eq!(escalator.ledger().deadlines().await.unwrap().get(&thread), Some(&reminder.at));

        let mut stand_in = Envelope::on_thread(thread.clone(), ant("bob"), ant("dev-a"), "carry on");
        assert!(!escalator.arrived(&mut stand_in, Role::Human).await.unwrap());
        assert_eq!(escalator.ledger().reminder(&thread).await.unwrap(), None, "a human ends it");
        assert!(escalator.ledger().deadlines().await.unwrap().is_empty());
        let answered = next(&mut to_human).await;
        let expected =
            format!("[ants] bob answered the escalation of dev-a on thread {thread} in place of human: carry on");
        assert_eq!((answered.text.as_str(), &answered.escalation), (expected.as_str(), &None));
        assert_ne!(answered.thread_id, thread, "on a thread of its own");
        let mut again = Envelope::on_thread(thread.clone(), UserId::human(), ant("dev-a"), "thanks");
        assert!(!escalator.arrived(&mut again, Role::Human).await.unwrap());
    };
    tokio::select! {
        done = timeout(WAIT, check) => done.expect("checked in time"),
        () = escalator.deadlines() => unreachable!(),
    }
    assert!(timeout(Duration::from_millis(500), to_human.next()).await.is_err(), "one reminder, one answer told");
}

#[tokio::test]
async fn a_stand_in_s_answer_to_an_escalation_reaching_the_human_without_reminders_is_told_to_the_human() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ant("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let sessions = bus::ensure_sessions(&js, None, &ant("dev-a")).await.unwrap();
    let mut to_human = client.subscribe(subject::inbox_of(&UserId::human())).await.unwrap();
    client.flush().await.unwrap();
    let chain = Chain { remind: None, stand_in: Some(ant("bob")), ..Chain::default() };
    let ledger = Arc::new(Ledger::kv(sessions, ant("dev-a"), Caps::default()));
    let send = escalation::jetstream_sender(js.clone());
    let escalator = Escalator::new(ant("dev-a"), chain, colony(), Registry::default(), ledger, send);
    let thread = ThreadId::generate();
    let escalate = |id: &str| {
        let notice = Envelope::on_thread(thread.clone(), ant("dev-a"), UserId::human(), "Turn budget spent.");
        escalator.escalate(Stuck::new(EscalationReason::TurnBudget, notice, id.to_owned(), None))
    };
    let human_says = |from: UserId, text: &str| Envelope::on_thread(thread.clone(), from, ant("dev-a"), text);

    assert_eq!(escalate("e1").await.unwrap(), Escalated::Owner(UserId::human()));
    next(&mut to_human).await;
    assert_eq!(escalator.ledger().pending(&thread).await.unwrap(), None, "no chain record");
    assert_eq!(escalator.ledger().reminder(&thread).await.unwrap(), None, "no reminder");
    assert!(escalator.ledger().deadlines().await.unwrap().is_empty(), "nothing due");

    assert!(!escalator.arrived(&mut human_says(ant("bob"), "carry on"), Role::Human).await.unwrap());
    let answered = next(&mut to_human).await;
    let expected =
        format!("[ants] bob answered the escalation of dev-a on thread {thread} in place of human: carry on");
    assert_eq!(answered.text, expected);
    // The answer ended it: the human's own message next tells nothing.
    assert!(!escalator.arrived(&mut human_says(UserId::human(), "thanks"), Role::Human).await.unwrap());

    // Answered by the human it reached, it ends untold, and a later message of the stand-in tells nothing either.
    escalate("e2").await.unwrap();
    next(&mut to_human).await;
    assert!(!escalator.arrived(&mut human_says(UserId::human(), "on it"), Role::Human).await.unwrap());
    assert!(!escalator.arrived(&mut human_says(ant("bob"), "noted"), Role::Human).await.unwrap());
    assert!(timeout(Duration::from_millis(500), to_human.next()).await.is_err(), "one answer told");
}

#[tokio::test]
async fn a_stored_question_is_told_on_the_ant_s_status_without_the_message_s_text() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &ant("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let thread = ThreadId::generate();
    let mut status = client.subscribe(subject::status(&ant("dev-a"), &thread)).await.unwrap();
    client.flush().await.unwrap();
    let options = ["yes".to_owned(), "not yet".to_owned()];
    let question = ants_proto::Question::new("Ship it?", &options, false).unwrap();
    let asking = Envelope::asking(thread, ant("dev-a"), UserId::human(), question.clone());
    escalation::jetstream_sender(js)(asking.clone(), asking.id.to_string()).await.unwrap();
    let told = timeout(WAIT, status.next()).await.expect("no status").unwrap();
    let told: StatusEvent = serde_json::from_slice(&told.payload).unwrap();
    assert_eq!(told.update, StatusUpdate::Asked { to: UserId::human(), message: asking.id, question: question.into() });
    assert!(!String::from_utf8_lossy(&serde_json::to_vec(&told).unwrap()).contains("1. yes"));
}
