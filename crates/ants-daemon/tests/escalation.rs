//! Escalations against a real nats-server: kept in `ants_sessions` across a restart, up the chains the roster
//! publishes, and to the human once the chains loop.

use std::sync::Arc;
use std::time::Duration;

use ants_daemon::bus;
use ants_daemon::caps::{Caps, Ledger};
use ants_daemon::escalation::{self, Chain, Escalated, Escalator, Stuck};
use ants_daemon::humans::Registry;
use ants_daemon::mcp::Roster;
use ants_proto::{Envelope, EscalationReason, PersonaId, RosterEntry, ThreadId, subject};
use ants_test_support::NatsServer as Server;
use async_nats::jetstream::{self, kv};
use futures::StreamExt as _;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);

fn persona(name: &str) -> PersonaId {
    name.parse().unwrap()
}

/// A roster where lead escalates to boss, and boss back to dev-a.
fn roster() -> Roster {
    Arc::new(|| {
        let now = chrono::Utc::now();
        let entry = move |name: &str, chain: &str| RosterEntry {
            persona: persona(name),
            machine: "test".to_owned(),
            harness: "copilot".to_owned(),
            capabilities: Vec::new(),
            whoami: None,
            workdir: None,
            started_at: now,
            heartbeat_at: now,
            activity: None,
            handling: None,
            harnesses: None,
            escalate_to: vec![persona(chain)],
        };
        Box::pin(async move { Ok(vec![entry("lead", "boss"), entry("boss", "dev-a")]) })
    })
}

fn escalator(js: &jetstream::Context, sessions: &kv::Store) -> Escalator {
    let chain =
        Chain { escalate_to: vec![persona("lead")], after: Some(Duration::from_millis(300)), ..Chain::default() };
    let ledger = Arc::new(Ledger::kv(sessions.clone(), persona("dev-a"), Caps::default()));
    let send = escalation::jetstream_sender(js.clone());
    Escalator::new(persona("dev-a"), chain, roster(), Registry::default(), ledger, send)
}

async fn next(subscriber: &mut async_nats::Subscriber) -> Envelope {
    let msg = timeout(WAIT, subscriber.next()).await.expect("no message").unwrap();
    serde_json::from_slice(&msg.payload).unwrap()
}

#[tokio::test]
async fn an_escalation_climbs_past_its_deadline_after_a_restart_and_goes_to_the_human_once_the_chains_loop() {
    let Some(server) = Server::start() else { return };
    let (client, js) = bus::connect(&server.url, None, None, &persona("dev-a"), "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let sessions = bus::ensure_sessions(&js).await.unwrap();
    let mut to_lead = client.subscribe(subject::inbox_of(&persona("lead"))).await.unwrap();
    let mut to_boss = client.subscribe(subject::inbox_of(&persona("boss"))).await.unwrap();
    let mut to_human = client.subscribe(subject::inbox_of(&PersonaId::human())).await.unwrap();
    client.flush().await.unwrap();
    let thread = ThreadId::generate();
    let stuck = |text: &str, id: &str| {
        let notice = Envelope::on_thread(thread.clone(), persona("dev-a"), PersonaId::human(), text);
        Stuck::new(EscalationReason::TurnBudget, notice, id.to_owned(), None)
    };

    let first = escalator(&js, &sessions);
    let escalated = first.escalate(stuck("Turn budget spent.", "e1")).await.unwrap();
    assert_eq!(escalated, Escalated::Chain { level: 1, leader: persona("lead"), to: vec![persona("lead")] });
    let one = next(&mut to_lead).await;
    assert_eq!(one.escalation.as_deref().map(|e| e.level), Some(1));
    drop(first);

    // Restarted, the persona still escalates past the deadline it stored.
    let escalator = escalator(&js, &sessions);
    let check = async {
        let two = next(&mut to_boss).await;
        let level = two.escalation.as_deref().expect("the escalation").clone();
        assert_eq!((level.level, &level.leader), (2, &persona("boss")));
        assert!(two.text.contains("[ants] No answer from lead on thread"), "{}", two.text);
        assert!(two.text.ends_with("Turn budget spent."), "{}", two.text);

        // Stuck again before boss answers: the same members get it again, at the same level.
        let again = escalator.escalate(stuck("Still stuck.", "e2")).await.unwrap();
        assert_eq!(again, Escalated::Chain { level: 2, leader: persona("boss"), to: vec![persona("boss")] });
        assert!(next(&mut to_boss).await.text.ends_with("Still stuck."));

        // boss answers: a fresh budget once, and no more deadline.
        let mut answer = two.reply(persona("boss"), "go on", ants_proto::Role::Persona).unwrap();
        answer = answer.with_escalation(Some(level));
        answer.turn_budget = 0;
        assert!(escalator.arrived(&mut answer, false).await.unwrap());
        assert_eq!(answer.turn_budget, ants_proto::DEFAULT_TURN_BUDGET);
        let mut other = answer.clone();
        other.id = ants_proto::Uuid::now_v7();
        other.turn_budget = 0;
        assert!(!escalator.arrived(&mut other, false).await.unwrap(), "once per level");
        assert!(escalator.ledger().deadlines().await.unwrap().is_empty());

        // boss's chain leads back to dev-a, so the human gets the thread.
        let escalated = escalator.escalate(stuck("Spent again.", "e3")).await.unwrap();
        assert_eq!(escalated, Escalated::Owner(PersonaId::human()));
        let notice = next(&mut to_human).await;
        assert!(notice.text.starts_with("Spent again.\n\n[ants] The escalation of dev-a reached level 2"));
        assert!(notice.text.ends_with("dev-a had it already, so the chains loop."), "{}", notice.text);
        assert_eq!(notice.escalation.as_deref().map(|e| e.level), Some(2));

        let mut human = Envelope::on_thread(thread.clone(), PersonaId::human(), persona("dev-a"), "carry on");
        assert!(!escalator.arrived(&mut human, true).await.unwrap());
        assert_eq!(escalator.ledger().pending(&thread).await.unwrap(), None, "a human ends it");
    };
    tokio::select! {
        () = check => {}
        () = escalator.deadlines() => unreachable!(),
    }
    assert!(timeout(Duration::from_millis(500), to_lead.next()).await.is_err(), "lead got level 1 only");
}
