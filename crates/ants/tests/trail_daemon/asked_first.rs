//! A human() step asked of the run's owner as ask_human asks: of the owner's assistant first, whose answer answers
//! the step for the owner; passed on by the assistant, or left unanswered past escalate_after, the question reaches
//! the owner, whose answer then answers the step.

use ants_daemon::escalation::{Chain, Escalator, Sender};
use ants_daemon::limits::caps::{Caps, Ledger};
use ants_daemon::people::assistant_first::{AssistantFirst, RecordOf};
use ants_proto::EscalationReason;

use super::lifecycle::user_jwt;
use super::*;

const ASK: &str = r#"await human([{"question": "Ship it?", "options": ["yes", "no"]}])"#;

/// The humans the conductor knows, the default human's grant naming pa as its assistant read in, and their watch.
async fn granted(server: &NatsServer) -> (Registry, ants_daemon::people::humans::Watching) {
    let (human, pa) = (ants_test_support::human(), ant("pa"));
    let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &human, "grant").await.unwrap();
    let bucket = js.create_key_value(ants_daemon::hub::humans_config()).await.unwrap();
    let (account, device) = (nkeys::KeyPair::new_account(), nkeys::KeyPair::new_user());
    let jwt = user_jwt(&account, &device, &human, &DeviceId::generate().tag());
    let mut grant = AssistantGrant::new(human.clone(), Some(pa.clone()), chrono::Utc::now(), jwt);
    grant.signature = data_encoding::BASE64URL_NOPAD.encode(&device.sign(&grant.signed()).unwrap());
    bucket.put(grant.key(), serde_json::to_vec(&grant).unwrap().into()).await.unwrap();
    let humans = Registry::new(human.clone()).verifying_grants(Some(account.public_key()));
    let watching = humans.watch(js, Duration::from_secs(1));
    eventually("the grant is read", async || humans.assistant(&human).as_ref() == Some(&pa)).await;
    (humans, watching)
}

/// lead's escalator, sending what would reach a human to pa first, live in the colony, and waiting `after` for its
/// answer; what it sends comes on the receiver.
fn escalator(humans: &Registry, after: Option<Duration>) -> (Arc<Escalator>, mpsc::UnboundedReceiver<Envelope>) {
    let (tx, sent) = mpsc::unbounded_channel();
    let send: Sender = Arc::new(move |envelope, _id| {
        tx.send(envelope).unwrap();
        Box::pin(async { Ok(None) })
    });
    let colony: ants_daemon::mcp::Colony =
        Arc::new(|| Box::pin(async { Ok(vec![ants_test_support::colony_entry("pa", "m1")]) }));
    let record: RecordOf = Arc::new(|_| Box::pin(async { Some(ants_test_support::human()) }));
    let assistants = AssistantFirst::new(humans.clone(), colony.clone(), record);
    let chain = Chain { after, ..Chain::default() };
    let ledger = Arc::new(Ledger::memory(ant("lead"), Caps::default()));
    let escalator =
        Escalator::new(ant("lead"), chain, colony, humans.clone(), ledger, send).with_assistants(assistants);
    (Arc::new(escalator), sent)
}

async fn next(sent: &mut mpsc::UnboundedReceiver<Envelope>) -> Envelope {
    tokio::time::timeout(Duration::from_secs(30), sent.recv()).await.expect("a message").unwrap()
}

/// A run of [`ASK`] whose question went to pa first: the run and the message asking pa.
async fn asked_first(lead: &mut Conductor, sent: &mut mpsc::UnboundedReceiver<Envelope>) -> (RunId, Envelope) {
    let run = lead.start(ASK, TrailLimits::default()).await;
    let asked = next(sent).await;
    assert_eq!(asked.to, ant("pa"));
    let carried = asked.escalation.as_deref().expect("the question's escalation");
    assert_eq!(carried.reason, EscalationReason::Question);
    assert_eq!((&carried.leader, &carried.assistant_of), (&ant("pa"), &Some(ants_test_support::human())));
    assert!(asked.text.contains("Ship it?"), "{}", asked.text);
    assert!(asked.trail_step.is_none(), "the assistant gets no step");
    assert!(lead.sends_no_step(Duration::from_millis(200)).await, "the question went to the human directly");
    (run, asked)
}

#[tokio::test]
async fn a_human_step_goes_to_the_owners_assistant_first_whose_answer_answers_it() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (humans, _watching) = granted(&server).await;
    let mut lead = conductor_with(&server, &ants_binary(), Vec::new(), humans.clone()).await;
    let (escalator, mut sent) = escalator(&humans, None);
    lead.trails.with_escalator(escalator.clone());
    let (run, asked) = asked_first(&mut lead, &mut sent).await;

    let reply = |by: &str| {
        let reply = asked.reply(ant(by), "yes, ship it", Role::Ant).unwrap();
        reply.with_escalation(asked.escalation.as_deref().cloned())
    };
    assert!(lead.trails.take(&reply("dev")).await.unwrap(), "reported, as another's escalation on the step");
    assert!(escalator.ledger().pending(&asked.thread_id).await.unwrap().is_some(), "dev's answer settles nothing");
    assert!(lead.trails.take(&reply("pa")).await.unwrap());
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("yes, ship it") });
    assert!(escalator.ledger().pending(&asked.thread_id).await.unwrap().is_none(), "the question goes on to nobody");
}

#[tokio::test]
async fn a_human_step_the_assistant_passes_on_is_answered_by_the_owner() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (humans, _watching) = granted(&server).await;
    let mut lead = conductor_with(&server, &ants_binary(), Vec::new(), humans.clone()).await;
    let (escalator, mut sent) = escalator(&humans, None);
    lead.trails.with_escalator(escalator.clone());
    let (run, asked) = asked_first(&mut lead, &mut sent).await;

    let passed = asked.escalation.as_deref().unwrap().passed_on().unwrap();
    let pass = Envelope::on_thread(asked.thread_id.clone(), ant("pa"), ant("lead"), "for the human to decide")
        .with_escalation(Some(passed));
    assert!(!lead.trails.take(&pass).await.unwrap(), "the escalator's to take");
    assert!(escalator.took_climb(&pass).await.unwrap());
    let question = next(&mut sent).await;
    assert_eq!((&question.to, &question.thread_id), (&ants_test_support::human(), &asked.thread_id));
    assert!(question.question.is_some(), "asked as the question it is");
    assert!(question.text.contains("for the human to decide"), "{}", question.text);

    let answer = Envelope::answering(&question, ants_test_support::human(), vec![Pick::option(2)]).unwrap();
    assert!(lead.trails.take(&answer).await.unwrap());
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("no") });
}

#[tokio::test]
async fn a_human_step_the_assistant_leaves_unanswered_reaches_the_owner_after_escalate_after() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (humans, _watching) = granted(&server).await;
    let mut lead = conductor_with(&server, &ants_binary(), Vec::new(), humans.clone()).await;
    let (escalator, mut sent) = escalator(&humans, Some(Duration::from_millis(300)));
    lead.trails.with_escalator(escalator.clone());
    let deadlines = tokio::spawn({
        let escalator = escalator.clone();
        async move { escalator.deadlines().await }
    });
    let (run, asked) = asked_first(&mut lead, &mut sent).await;

    let question = next(&mut sent).await;
    assert_eq!((&question.to, &question.thread_id), (&ants_test_support::human(), &asked.thread_id));
    assert!(question.question.is_some(), "asked as the question it is");
    let answer = Envelope::answering(&question, ants_test_support::human(), vec![Pick::option(1)]).unwrap();
    assert!(lead.trails.take(&answer).await.unwrap());
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("yes") });
    deadlines.abort();
}
