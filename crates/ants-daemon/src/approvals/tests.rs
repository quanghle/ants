//! Tests of tool request escalation: decisions by the humans, the chain and assistants, the first decision winning,
//! silence rejecting, and expired requests told to the owner or chain.

use std::collections::BTreeMap;

use ants_proto::{EscalationReason, subject};
use ants_test_support::NatsServer as Server;
use tokio::time::timeout;

use super::*;
use crate::bus;

/// Long enough for a busy machine running the whole suite.
const WAIT: Duration = Duration::from_secs(30);

/// Returns once the server has registered the subscriptions of `js`'s client: async-nats' flush only writes them
/// out, so another client's publish could otherwise get there first, and be missed.
async fn subscribed(js: &jetstream::Context) {
    js.get_stream(ants_proto::STREAM).await.unwrap();
}

/// The next `decided` update heard on `statuses`, after which the ant goes on working: its verdict, who gave it and
/// the deciding ant.
async fn decided_update(statuses: &mut async_nats::Subscriber) -> (Verdict, DecidedBy, Option<UserId>) {
    let (verdict, by, decider, _) = decided_via(statuses).await;
    (verdict, by, decider)
}

/// [`decided_update`], with the assistant that decided for the human.
async fn decided_via(statuses: &mut async_nats::Subscriber) -> (Verdict, DecidedBy, Option<UserId>, Option<UserId>) {
    let mut next = async || {
        let msg = timeout(WAIT, statuses.next()).await.unwrap().unwrap();
        serde_json::from_slice::<ants_proto::StatusEvent>(&msg.payload).unwrap().update
    };
    loop {
        if let StatusUpdate::Decided { verdict, by, decider, via, .. } = next().await {
            assert_eq!(next().await, StatusUpdate::State { state: State::Working });
            return (verdict, by, decider, via);
        }
    }
}

fn tool() -> ToolCall {
    ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() }
}

/// The tool request rejection last kept on the timeline of `ant` on `thread`, and the message it is kept under.
async fn rejection(stream: &jetstream::stream::Stream, thread: &ThreadId, ant: &UserId) -> (Uuid, TimelineRejection) {
    let stored = stream.direct_get_last_for_subject(subject::timeline(thread, ant)).await.unwrap();
    let record: Timeline = serde_json::from_slice(&stored.payload).unwrap();
    let [TimelineEntry::Rejected(rejected)] = record.entries.as_slice() else { panic!("{record:?}") };
    (record.message, rejected.clone())
}

#[tokio::test]
async fn the_human_decides_and_silence_rejects() {
    let Some(server) = Server::start() else { return };
    let ant: UserId = "dev-a".parse().unwrap();
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    let stream = bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
    subscribed(&human_js).await;

    let approver = NatsApprover::new(
        client.clone(),
        js.clone(),
        ant.clone(),
        Duration::from_secs(10),
        Registry::new(ants_test_support::human()),
    );
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let always = "Yes, and don't ask again for git push commands in /work";
    let labels = [(Verdict::AllowAlways, always.to_owned())].into();
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (options, labels)));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((&request.ant, &request.thread_id, &request.tool), (&ant, &thread, &tool()));
    assert_eq!(request.options, [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);
    assert_eq!(request.label(Verdict::AllowAlways), Some(always), "the harness's name, stored with the request");
    for expected in [
        ants_proto::StatusUpdate::Approval { request_id: request.id },
        ants_proto::StatusUpdate::State { state: State::WaitingApproval },
    ] {
        let status = timeout(WAIT, statuses.next()).await.unwrap().unwrap();
        let status: ants_proto::StatusEvent = serde_json::from_slice(&status.payload).unwrap();
        assert_eq!(status.update, expected);
    }

    let decision = request.decide(Verdict::AllowAlways).by(Some(ants_test_support::human()));
    human_js.publish(decision.subject(), serde_json::to_vec(&decision).unwrap().into()).await.unwrap().await.unwrap();
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);
    let told = (Verdict::AllowAlways, DecidedBy::Human, Some(ants_test_support::human()));
    assert_eq!(decided_update(&mut statuses).await, told, "the human the decision names");

    // Both the request and the decision stay in the stream for a late `ants tools list`.
    assert_eq!(stream.get_last_raw_message_by_subject(&request.subject()).await.unwrap().sequence, 1);
    assert!(stream.get_last_raw_message_by_subject(&decision.subject()).await.is_ok());
    let timeline = subject::timeline(&thread, &ant);
    assert!(stream.direct_get_last_for_subject(&timeline).await.is_err(), "nothing kept of an allowed request");

    // A human's rejection is kept on the thread's timeline: with no message being answered, under the nil id.
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    let decision = request.decide(Verdict::Reject).by(Some(ants_test_support::human()));
    human_js.publish(decision.subject(), serde_json::to_vec(&decision).unwrap().into()).await.unwrap().await.unwrap();
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::Reject);
    assert_eq!(
        decided_update(&mut statuses).await,
        (Verdict::Reject, DecidedBy::Human, Some(ants_test_support::human()))
    );
    let (message, rejected) = rejection(&stream, &thread, &ant).await;
    assert_eq!(message, Uuid::nil());
    let expected = TimelineRejection {
        request: request.id,
        kind: "execute".into(),
        title: "git push".into(),
        by: DecidedBy::Human,
        decider: Some(ants_test_support::human()),
        via: None,
    };
    assert_eq!(rejected, expected);

    // Silence rejects, and the thread's owner is told before the ant goes on.
    let alice: UserId = "alice".parse().unwrap();
    let humans = Registry::with(ants_test_support::human(), &["alice"]);
    let impatient = NatsApprover::new(client, js, ant.clone(), Duration::from_millis(300), humans.clone());
    let asked = Envelope::on_thread(thread.clone(), alice.clone(), ant.clone(), "push").with_owner(Some(alice.clone()));
    let turn = impatient.begin(&asked);
    assert_eq!(
        impatient.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())).await,
        Verdict::Reject
    );
    assert_eq!(decided_update(&mut statuses).await, (Verdict::Reject, DecidedBy::Expiry, None));
    let (message, rejected) = rejection(&stream, &thread, &ant).await;
    assert_eq!(
        (message, rejected.by, rejected.decider),
        (asked.id, DecidedBy::Expiry, None),
        "under the message answered"
    );
    let stored = stream.direct_get_last_for_subject(subject::inbox(&alice, &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!((&told.from, &told.owner), (&ant, &Some(alice.clone())));
    assert_ne!(told.thread_id, thread, "on a thread of its own, for `ants ask --wait` to wait on");
    assert!(told.text.contains("expired unanswered") && told.text.contains("git push"), "{}", told.text);
    let escalation = told.escalation.as_deref().map(|e| (e.reason, &e.thread_id));
    assert_eq!(escalation, Some((EscalationReason::Approval, &thread)), "pushed as escalations are");

    assert_eq!(impatient.asked(&thread), 1, "the turn's requests are counted");

    // With the turn over, the default human is told.
    drop(turn);
    assert_eq!(
        impatient.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())).await,
        Verdict::Reject
    );
    assert_eq!(impatient.asked(&thread), 0, "counted only while a turn lasts");
    let stored = stream.direct_get_last_for_subject(subject::inbox(&ants_test_support::human(), &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!((&told.to, &told.owner), (&ants_test_support::human(), &None));
}

#[tokio::test]
async fn a_request_names_its_human_and_stand_in_who_is_told_of_another_human_deciding() {
    let Some(server) = Server::start() else { return };
    let (ant, alice, bob): (UserId, UserId, UserId) =
        ("dev-a".parse().unwrap(), "alice".parse().unwrap(), "bob".parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    let stream = bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &bob, "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    subscribed(&human_js).await;

    let chain = Chain { stand_in: Some(bob.clone()), ..Chain::default() };
    let humans = Registry::with(ants_test_support::human(), &["alice", "bob"]);
    let approver = NatsApprover::new(client, js, ant.clone(), Duration::from_secs(10), humans).with_chain(chain);
    let asked = Envelope::on_thread(thread.clone(), alice.clone(), ant.clone(), "push").with_owner(Some(alice.clone()));
    let _turn = approver.begin(&asked);
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((&request.human, &request.stand_in), (&Some(alice.clone()), &Some(bob.clone())));

    let decision = request.decide(Verdict::AllowOnce).by(Some(bob.clone()));
    human_js.publish(decision.subject(), serde_json::to_vec(&decision).unwrap().into()).await.unwrap().await.unwrap();
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    let stored = stream.direct_get_last_for_subject(subject::inbox(&alice, &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!((&told.from, &told.owner, &told.escalation), (&ant, &Some(alice.clone()), &None));
    let expected = format!("bob decided tool request dev-a/{} on thread t1 in place of alice: allow_once", request.id);
    assert!(told.text.contains(&expected), "{}", told.text);

    assert_eq!(decided_notice(&request, &decision.clone().by(Some(alice))), None, "the human's own decision");
    assert_eq!(decided_notice(&request, &decision.by(None)), None, "a decider unknown");
}

#[test]
fn an_expired_request_is_told_to_the_owner_else_the_default_human() {
    let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
    let humans = Registry::with(ants_test_support::human(), &["alice"]);
    let thread: ThreadId = "t1".parse().unwrap();
    let tool = ToolCall { locations: vec!["src/main.rs".into()], ..tool() };
    let request = ApprovalRequest::new(ant.clone(), thread.clone(), tool, vec![Verdict::AllowOnce], DEFAULT_TIMEOUT);
    let asked = Envelope::on_thread(thread.clone(), lead.clone(), ant.clone(), "push")
        .with_owner(Some("alice".parse().unwrap()))
        .with_creator(Some(lead.clone()))
        .with_origin_thread(Some("t0".parse().unwrap()));
    let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, Some(&asked), &humans);
    assert_eq!((told.to.as_str(), &told.owner, &told.creator), ("alice", &asked.owner, &Some(lead)));
    assert_ne!(told.thread_id, thread);
    assert_eq!((&told.origin_thread, told.in_reply_to), (&asked.origin_thread, None));
    for part in [
        format!("Tool request dev-a/{} on thread t1 expired unanswered after 5m", request.id),
        "git push (execute)".into(),
        "Files: src/main.rs".into(),
        "`ants ask --thread t1 @dev-a TEXT`".into(),
        "approval_timeout".into(),
        "allow rule".into(),
    ] {
        assert!(told.text.contains(&part), "{part}: {}", told.text);
    }
    let by_ant = asked.with_owner(Some("dev-c".parse().unwrap()));
    let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, Some(&by_ant), &humans);
    assert_eq!((&told.to, &told.owner), (&ants_test_support::human(), &None), "an ant owns no thread");
    let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, None, &humans);
    assert_eq!((&told.to, &told.owner, &told.creator), (&ants_test_support::human(), &None, &None));
}

/// A decision the stream refuses, as `ants tools approve` loses to another human's, still reaches the ant.
async fn publish_refused(human: &async_nats::Client, decision: &ApprovalDecision) {
    let mut headers = async_nats::HeaderMap::new();
    // Nothing is stored on the subject yet, so the stream refuses this.
    headers.insert(async_nats::header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, "1");
    let payload = serde_json::to_vec(decision).unwrap().into();
    human.publish_with_headers(decision.subject(), headers, payload).await.unwrap();
    human.flush().await.unwrap();
}

#[tokio::test]
async fn the_ant_acts_on_the_stored_decision_not_the_first_heard() {
    let Some(server) = Server::start() else { return };
    let ant: UserId = "dev-a".parse().unwrap();
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    let stream = bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    subscribed(&human_js).await;
    let mut next_request = async || {
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        serde_json::from_slice::<ApprovalRequest>(&msg.payload).unwrap()
    };

    let approver =
        NatsApprover::new(client.clone(), js.clone(), ant.clone(), WAIT, Registry::new(ants_test_support::human()));
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (options.clone(), BTreeMap::new())));
    let request = next_request().await;
    let mut heard = client.subscribe(request.decision_subject()).await.unwrap();
    subscribed(&js).await;
    publish_refused(&human, &request.decide(Verdict::AllowOnce)).await;
    timeout(WAIT, heard.next()).await.unwrap().unwrap();
    tokio::time::sleep(RECHECK * 2).await;
    assert!(!decided.is_finished(), "a decision heard but not stored decides nothing");
    let winner = request.decide(Verdict::AllowAlways);
    human_js.publish(winner.subject(), serde_json::to_vec(&winner).unwrap().into()).await.unwrap().await.unwrap();
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);
    let stored = stream.direct_get_last_for_subject(winner.subject()).await.unwrap();
    assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), winner);

    // A decision that is never stored leaves the request to expire.
    let impatient = NatsApprover::new(
        client.clone(),
        js.clone(),
        ant.clone(),
        Duration::from_secs(1),
        Registry::new(ants_test_support::human()),
    );
    let decided = tokio::spawn(impatient.decide(thread, tool(), (options, BTreeMap::new())));
    publish_refused(&human, &next_request().await.decide(Verdict::AllowOnce)).await;
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::Reject);
}

fn chain(escalate_to: &[&UserId]) -> Chain {
    Chain { escalate_to: escalate_to.iter().map(|p| (*p).clone()).collect(), ..Chain::default() }
}

/// Publish `envelope` on `subject` and wait for the stream to store it.
async fn send(js: &jetstream::Context, subject: String, envelope: &Envelope) {
    js.publish(subject, serde_json::to_vec(envelope).unwrap().into()).await.unwrap().await.unwrap();
}

#[tokio::test]
async fn the_chain_hears_the_request_and_only_its_decision_counts() {
    let Some(server) = Server::start() else { return };
    let (ant, lead, other): (UserId, UserId, UserId) =
        ("dev-a".parse().unwrap(), "lead".parse().unwrap(), "dev-c".parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut asked = human.subscribe(subject::inbox_of(&lead)).await.unwrap();
    let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
    subscribed(&human_js).await;

    let approver = NatsApprover::new(client, js, ant.clone(), WAIT, Registry::new(ants_test_support::human()))
        .with_chain(chain(&[&lead]));
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    let msg = timeout(WAIT, asked.next()).await.unwrap().unwrap();
    let notice: Envelope = serde_json::from_slice(&msg.payload).unwrap();
    notice.check_subject(&msg.subject).unwrap();
    assert_eq!((&notice.from, &notice.thread_id), (&ant, &thread));
    let escalation = notice.escalation.as_deref().unwrap();
    assert_eq!(
        (escalation.reason, escalation.request_id, escalation.level),
        (EscalationReason::Approval, Some(request.id), 1)
    );
    assert!(notice.text.contains("decide_approval") && notice.text.contains("git push"), "{}", notice.text);

    // Outside the chain, forged, or for another request: ignored.
    let decide = |from: &UserId, decision: ApprovalDecision| Envelope::decision(thread.clone(), from.clone(), decision);
    let foreign = decide(&other, request.decide(Verdict::AllowOnce));
    send(&human_js, foreign.subject(), &foreign).await;
    let forged = decide(&lead, request.decide(Verdict::AllowOnce));
    send(&human_js, foreign.subject(), &forged).await;
    let elsewhere = decide(&lead, ApprovalDecision::new(Uuid::new_v4(), ant.clone(), Verdict::AllowOnce));
    send(&human_js, elsewhere.subject(), &elsewhere).await;
    tokio::time::sleep(RECHECK * 2).await;
    assert!(!decided.is_finished(), "no decision from the chain yet");

    let lead_says = decide(&lead, request.decide(Verdict::AllowOnce));
    send(&human_js, lead_says.subject(), &lead_says).await;
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    let told = decided_update(&mut statuses).await;
    assert_eq!(told, (Verdict::AllowOnce, DecidedBy::Chain, Some(lead.clone())), "observers learn who decided");
    assert!(approver.waiting.locked().is_empty(), "nothing waits once decided");
    approver.offer(&lead_says);
}

/// A team in escalate_to stands for its lead as the request is sent, and the lead's decision counts.
#[tokio::test]
async fn a_team_in_the_chain_is_asked_through_its_lead_whose_decision_counts() {
    let Some(server) = Server::start() else { return };
    let [ant, lead, backend]: [UserId; 3] = ["dev-a", "lead", "backend"].map(|u| u.parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut asked = human.subscribe(subject::inbox_of(&lead)).await.unwrap();
    subscribed(&human_js).await;

    let mut directory = ants_proto::TeamDirectory::default();
    directory.create(ants_proto::Team::new(backend.clone(), lead.clone(), ants_test_support::human()), None).unwrap();
    let teams: Teams = Arc::new(move || {
        let directory = directory.clone();
        Box::pin(async move { Some(directory) })
    });
    let approver = NatsApprover::new(client, js, ant.clone(), WAIT, Registry::new(ants_test_support::human()))
        .with_chain(chain(&[&backend]))
        .with_teams(teams);
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    let msg = timeout(WAIT, asked.next()).await.unwrap().unwrap();
    let notice: Envelope = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(notice.escalation.as_deref().unwrap().request_id, Some(request.id), "the lead is asked");

    let decide = |from: &UserId| Envelope::decision(thread.clone(), from.clone(), request.decide(Verdict::AllowOnce));
    let team_says = decide(&backend);
    send(&human_js, team_says.subject(), &team_says).await;
    tokio::time::sleep(RECHECK * 2).await;
    assert!(!decided.is_finished(), "the team's name decides nothing");
    let lead_says = decide(&lead);
    send(&human_js, lead_says.subject(), &lead_says).await;
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
}

#[tokio::test]
async fn an_expired_request_is_told_to_the_chain_asked_and_pushed_to_the_human_once() {
    let Some(server) = Server::start() else { return };
    let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    let mut stream = bus::ensure_stream(&js).await.unwrap();

    // A human in the chain, as a misconfigured escalate_to may hold, is told once, as the default human.
    let human = ants_test_support::human();
    let approver = NatsApprover::new(
        client,
        js,
        ant.clone(),
        Duration::from_millis(300),
        Registry::new(ants_test_support::human()),
    )
    .with_chain(chain(&[&lead, &human]));
    assert_eq!(
        approver.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())).await,
        Verdict::Reject
    );

    let stored = stream.direct_get_last_for_subject(subject::inbox(&lead, &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!((&told.thread_id, &told.escalation), (&thread, &None), "on the request's thread, nothing to decide");
    assert!(told.text.contains("expired unanswered") && told.text.contains("git push"), "{}", told.text);

    let to_human = stream.get_last_raw_message_by_subject(&subject::inbox(&human, &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&to_human.payload).unwrap();
    assert!(told.text.contains("expired unanswered"), "{}", told.text);
    let escalation = told.escalation.as_deref().expect("an escalation, for push sinks");
    assert_eq!((escalation.reason, &escalation.thread_id), (EscalationReason::Approval, &thread));
    let info = stream.info().await.unwrap();
    let (first, last) = (info.state.first_sequence, info.state.last_sequence);
    let mut to_human = 0;
    for seq in first..=last {
        let msg = stream.get_raw_message(seq).await.unwrap();
        to_human += usize::from(msg.subject.as_str() == subject::inbox(&human, &ant));
    }
    assert_eq!(to_human, 2, "the chain's request and the expiry notice, not one more");
}

#[tokio::test]
async fn the_first_decision_wins_whoever_makes_it() {
    let Some(server) = Server::start() else { return };
    let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &ants_test_support::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
    subscribed(&human_js).await;
    let approver = NatsApprover::new(client, js, ant.clone(), WAIT, Registry::new(ants_test_support::human()))
        .with_chain(chain(&[&lead]));
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let decide =
        |request: &ApprovalRequest, verdict| Envelope::decision(thread.clone(), lead.clone(), request.decide(verdict));

    // The inbox consumer hands over the lead's decision, and a human's comes after it.
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (options.clone(), BTreeMap::new())));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    approver.offer(&Envelope::decision(thread.clone(), "dev-c".parse().unwrap(), request.decide(Verdict::AllowAlways)));
    approver.offer(&decide(&request, Verdict::AllowOnce));
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    assert_eq!(decided_update(&mut statuses).await, (Verdict::AllowOnce, DecidedBy::Chain, Some(lead.clone())));
    let late = request.decide(Verdict::Reject);
    human_js.publish(late.subject(), serde_json::to_vec(&late).unwrap().into()).await.unwrap().await.unwrap();

    // A human's decision stored by the time the lead's arrives wins. The runtime is blocked while another stores
    // it, so the approver gets both at once.
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), (options, BTreeMap::new())));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    let (url, human_says) = (server.url.clone(), request.decide(Verdict::AllowAlways));
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (_human, js) = bus::connect(&url, None, None, &ants_test_support::human(), "human-2").await.unwrap();
            js.publish(human_says.subject(), serde_json::to_vec(&human_says).unwrap().into())
                .await
                .unwrap()
                .await
                .unwrap();
        });
    })
    .join()
    .unwrap();
    approver.offer(&decide(&request, Verdict::AllowOnce));
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);
    let told = decided_update(&mut statuses).await;
    assert_eq!(told, (Verdict::AllowAlways, DecidedBy::Human, None), "the human's, though the lead's came with it");
}

#[tokio::test]
async fn an_assistant_s_decision_takes_effect_only_with_its_human_s_consent() {
    use ants_proto::{Confirmations, ConfirmedAct, PreAllow};

    use crate::people::humans::TestDevice;

    let Some(server) = Server::start() else { return };
    let [ant, alice, pa]: [UserId; 3] = ["dev-a", "alice", "pa"].map(|u| u.parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &alice, "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
    subscribed(&human_js).await;

    let device = TestDevice::default();
    let humans = Registry::with(ants_test_support::human(), &["alice"]).verifying_grants(Some(device.account()));
    let at = || chrono::Utc::now();
    let mut grant = ants_proto::AssistantGrant::new(alice.clone(), Some(pa.clone()), at(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    assert_eq!(humans.assistant(&alice), Some(pa.clone()));
    let confirm = |request: &ApprovalRequest, verdict, answer| {
        let act = Act::Decision { ant: ant.clone(), request_id: request.id, verdict };
        let acts = vec![ConfirmedAct { assistant: pa.clone(), act, answer, until: request.expires_at }];
        let mut record = Confirmations::new(alice.clone(), acts, at(), device.jwt(&alice));
        record.signature = device.sign(&record.signed());
        humans.put_record(&ants_proto::confirmations_key(&alice), Some(&serde_json::to_vec(&record).unwrap()));
    };

    let approver = NatsApprover::new(client, js, ant.clone(), WAIT, humans.clone());
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let ask = async |requests: &mut async_nats::Subscriber| {
        let pushing = ToolCall { input: Some(r#"{"command":"git push"}"#.into()), ..tool() };
        let decided = tokio::spawn(approver.decide(thread.clone(), pushing, (options.clone(), BTreeMap::new())));
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        (decided, serde_json::from_slice::<ApprovalRequest>(&msg.payload).unwrap())
    };
    let for_alice = |request: &ApprovalRequest, verdict| {
        Envelope::decision(thread.clone(), pa.clone(), request.decide(verdict)).on_behalf(Some(alice.clone()))
    };
    let quiet = RECHECK * 2;

    // Held until the human confirms that very act: a confirmation of another verdict does not do.
    let (decided, request) = ask(&mut requests).await;
    approver.offer(&for_alice(&request, Verdict::AllowOnce));
    tokio::time::sleep(quiet).await;
    assert!(!decided.is_finished(), "held");
    confirm(&request, Verdict::AllowAlways, ActAnswer::Confirm);
    tokio::time::sleep(quiet).await;
    assert!(!decided.is_finished(), "another act confirmed");
    confirm(&request, Verdict::AllowOnce, ActAnswer::Confirm);
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    let told = (Verdict::AllowOnce, DecidedBy::Human, Some(alice.clone()), Some(pa.clone()));
    assert_eq!(decided_via(&mut statuses).await, told, "the human's, via its assistant");

    // A denied act is dropped; the request waits for another decision.
    let (decided, request) = ask(&mut requests).await;
    approver.offer(&for_alice(&request, Verdict::AllowOnce));
    confirm(&request, Verdict::AllowOnce, ActAnswer::Deny);
    tokio::time::sleep(quiet).await;
    assert!(!decided.is_finished(), "denied");
    let human_says = request.decide(Verdict::Reject).by(Some(alice.clone()));
    human_js.publish(human_says.subject(), serde_json::to_vec(&human_says).unwrap().into()).await.unwrap();
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::Reject);
    assert_eq!(decided_via(&mut statuses).await, (Verdict::Reject, DecidedBy::Human, Some(alice.clone()), None));

    // A pre-allow rule covering the call lets a decision take effect unconfirmed, but not one allowing always.
    let rules = vec![format!("{ant}/execute:git push")];
    let mut pre_allow = PreAllow::new(alice.clone(), pa.clone(), rules, at(), device.jwt(&alice));
    pre_allow.signature = device.sign(&pre_allow.signed());
    humans.put_record(&ants_proto::pre_allow_key(&alice), Some(&serde_json::to_vec(&pre_allow).unwrap()));
    let (decided, request) = ask(&mut requests).await;
    approver.offer(&for_alice(&request, Verdict::AllowAlways));
    tokio::time::sleep(quiet).await;
    assert!(!decided.is_finished(), "allow_always needs a confirmation");
    approver.offer(&for_alice(&request, Verdict::AllowOnce));
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    assert_eq!(decided_via(&mut statuses).await.3, Some(pa.clone()));

    // Withdrawing the grant drops what the assistant did unconfirmed: a confirmation coming after it does nothing.
    humans.put_record(&ants_proto::pre_allow_key(&alice), None);
    let (decided, request) = ask(&mut requests).await;
    approver.offer(&for_alice(&request, Verdict::AllowOnce));
    tokio::time::sleep(quiet).await;
    let mut withdrawal = ants_proto::AssistantGrant::new(alice.clone(), None, at(), device.jwt(&alice));
    withdrawal.signature = device.sign(&withdrawal.signed());
    humans.put_record(&withdrawal.key(), Some(&serde_json::to_vec(&withdrawal).unwrap()));
    humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    confirm(&request, Verdict::AllowOnce, ActAnswer::Confirm);
    tokio::time::sleep(quiet).await;
    assert!(!decided.is_finished(), "dropped on withdrawal");
    decided.abort();

    // The live inbox, which cannot verify the claim, never takes such a decision.
    let msg = async_nats::Message {
        subject: for_alice(&request, Verdict::AllowOnce).subject().into(),
        reply: None,
        payload: serde_json::to_vec(&for_alice(&request, Verdict::AllowOnce)).unwrap().into(),
        headers: None,
        status: None,
        description: None,
        length: 0,
    };
    assert!(approver.chain_decision(&request, &approver.chain.escalate_to, &msg).is_none());
}

#[tokio::test]
async fn a_request_goes_to_the_human_s_assistant_too_while_it_can_act() {
    use crate::people::humans::TestDevice;

    let Some(server) = Server::start() else { return };
    let [ant, alice, pa]: [UserId; 3] = ["dev-a", "alice", "pa"].map(|u| u.parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &alice, "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut assistant = human.subscribe(subject::inbox_of(&pa)).await.unwrap();
    subscribed(&human_js).await;

    let device = TestDevice::default();
    let humans = Registry::with(alice.clone(), &["alice"]).verifying_grants(Some(device.account()));
    let mut grant =
        ants_proto::AssistantGrant::new(alice.clone(), Some(pa.clone()), chrono::Utc::now(), device.jwt(&alice));
    grant.signature = device.sign(&grant.signed());
    humans.put_record(&grant.key(), Some(&serde_json::to_vec(&grant).unwrap()));
    let listed = Arc::new(Mutex::new(vec![crate::people::assistant_first::live(&pa)]));
    let colony: crate::mcp::Colony = {
        let listed = listed.clone();
        Arc::new(move || {
            let colony = listed.lock().unwrap().clone();
            Box::pin(async move { Ok(colony) })
        })
    };
    let record: crate::people::assistant_first::RecordOf = {
        let alice = alice.clone();
        Arc::new(move |_| {
            let alice = alice.clone();
            Box::pin(async move { Some(alice) })
        })
    };
    let assistants = AssistantFirst::new(humans.clone(), colony, record);
    let approver = NatsApprover::new(client, js, ant.clone(), WAIT, humans.clone()).with_assistants(assistants);
    let ask = async |requests: &mut async_nats::Subscriber| {
        let decided =
            tokio::spawn(approver.decide(thread.clone(), tool(), (vec![Verdict::AllowOnce], BTreeMap::new())));
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        (decided, serde_json::from_slice::<ApprovalRequest>(&msg.payload).unwrap())
    };

    // The human has the request, and its assistant is asked to decide it for the human.
    let (decided, request) = ask(&mut requests).await;
    assert_eq!(request.human, Some(alice.clone()));
    let msg = timeout(WAIT, assistant.next()).await.unwrap().unwrap();
    let asked: Envelope = serde_json::from_slice(&msg.payload).unwrap();
    let escalation = asked.escalation.as_deref().unwrap();
    assert_eq!((escalation.reason, escalation.request_id), (EscalationReason::Approval, Some(request.id)));
    assert_eq!((&escalation.leader, &escalation.assistant_of), (&pa, &Some(alice.clone())));
    assert!(asked.text.contains(&format!("decide_for_human {ant}/{}", request.id)), "{}", asked.text);
    decided.abort();

    // A paused assistant is not asked.
    listed.lock().unwrap()[0].paused = Some(ants_proto::Paused { since: chrono::Utc::now(), by: None });
    let (decided, _) = ask(&mut requests).await;
    assert!(timeout(RECHECK * 4, assistant.next()).await.is_err(), "the human alone");
    decided.abort();
}
