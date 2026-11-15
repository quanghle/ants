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

fn tool() -> ToolCall {
    ToolCall { kind: "execute".into(), title: "git push".into(), ..Default::default() }
}

#[tokio::test]
async fn the_human_decides_and_silence_rejects() {
    let Some(server) = Server::start() else { return };
    let ant: UserId = "dev-a".parse().unwrap();
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    let stream = bus::ensure_stream(&js).await.unwrap();
    let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut statuses = human.subscribe(subject::status(&ant, &thread)).await.unwrap();
    subscribed(&human_js).await;

    let approver = NatsApprover::new(client.clone(), js.clone(), ant.clone(), Duration::from_secs(10));
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!((&request.ant, &request.thread_id, &request.tool), (&ant, &thread, &tool()));
    assert_eq!(request.options, [Verdict::AllowOnce, Verdict::AllowAlways, Verdict::Reject]);
    for expected in [
        ants_proto::StatusUpdate::Approval { request_id: request.id },
        ants_proto::StatusUpdate::State { state: State::WaitingApproval },
    ] {
        let status = timeout(WAIT, statuses.next()).await.unwrap().unwrap();
        let status: ants_proto::StatusEvent = serde_json::from_slice(&status.payload).unwrap();
        assert_eq!(status.update, expected);
    }

    let decision = request.decide(Verdict::AllowAlways);
    human_js.publish(decision.subject(), serde_json::to_vec(&decision).unwrap().into()).await.unwrap().await.unwrap();
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);

    // Both the request and the decision stay in the stream for a late `ants tools list`.
    assert_eq!(stream.get_last_raw_message_by_subject(&request.subject()).await.unwrap().sequence, 1);
    assert!(stream.get_last_raw_message_by_subject(&decision.subject()).await.is_ok());

    // Silence rejects, and the thread's owner is told before the ant goes on.
    let alice: UserId = "alice".parse().unwrap();
    let humans = Registry::with(UserId::human(), &["alice"]);
    let impatient = NatsApprover::new(client, js, ant.clone(), Duration::from_millis(300)).with_humans(humans.clone());
    let asked = Envelope::on_thread(thread.clone(), alice.clone(), ant.clone(), "push").with_owner(Some(alice.clone()));
    let turn = impatient.begin(&asked);
    assert_eq!(impatient.decide(thread.clone(), tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);
    let stored = stream.direct_get_last_for_subject(subject::inbox(&alice, &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!((&told.from, &told.owner), (&ant, &Some(alice.clone())));
    assert_ne!(told.thread_id, thread, "on a thread of its own, for `ants ask --wait` to wait on");
    assert!(told.text.contains("expired unanswered") && told.text.contains("git push"), "{}", told.text);
    let escalation = told.escalation.as_deref().map(|e| (e.reason, &e.thread_id));
    assert_eq!(escalation, Some((EscalationReason::Approval, &thread)), "pushed as escalations are");

    // With the turn over, the default human is told.
    drop(turn);
    assert_eq!(impatient.decide(thread, tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);
    let stored = stream.direct_get_last_for_subject(subject::inbox(&UserId::human(), &ant)).await.unwrap();
    let told: Envelope = serde_json::from_slice(&stored.payload).unwrap();
    assert_eq!((&told.to, &told.owner), (&UserId::human(), &None));
}

#[test]
fn an_expired_request_is_told_to_the_owner_else_the_default_human() {
    let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
    let humans = Registry::with(UserId::human(), &["alice"]);
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
    assert_eq!((&told.to, &told.owner), (&UserId::human(), &None), "an ant owns no thread");
    let told = expired_notice(&ant, &request, DEFAULT_TIMEOUT, None, &humans);
    assert_eq!((&told.to, &told.owner, &told.creator), (&UserId::human(), &None, &None));
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
    let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    subscribed(&human_js).await;
    let mut next_request = async || {
        let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
        serde_json::from_slice::<ApprovalRequest>(&msg.payload).unwrap()
    };

    let approver = NatsApprover::new(client.clone(), js.clone(), ant.clone(), WAIT);
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options.clone()));
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
    let impatient = NatsApprover::new(client.clone(), js.clone(), ant.clone(), Duration::from_secs(1));
    let decided = tokio::spawn(impatient.decide(thread.clone(), tool(), options.clone()));
    publish_refused(&human, &next_request().await.decide(Verdict::AllowOnce)).await;
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::Reject);

    // A stream from before direct gets leaves only the first decision heard.
    let mut config = stream.cached_info().config.clone();
    config.allow_direct = false;
    human_js.update_stream(&config).await.unwrap();
    let decided = tokio::spawn(approver.decide(thread, tool(), options));
    publish_refused(&human, &next_request().await.decide(Verdict::AllowOnce)).await;
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
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
    let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    let mut asked = human.subscribe(subject::inbox_of(&lead)).await.unwrap();
    subscribed(&human_js).await;

    let approver = NatsApprover::new(client, js, ant.clone(), WAIT).with_chain(chain(&[&lead]));
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), vec![Verdict::AllowOnce]));
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
    assert!(approver.waiting.lock().unwrap().is_empty(), "nothing waits once decided");
    approver.offer(&lead_says);
}

#[tokio::test]
async fn an_expired_request_is_told_to_the_chain_asked_and_pushed_to_the_human_once() {
    let Some(server) = Server::start() else { return };
    let (ant, lead): (UserId, UserId) = ("dev-a".parse().unwrap(), "lead".parse().unwrap());
    let thread: ThreadId = "t1".parse().unwrap();
    let (client, js) = bus::connect(&server.url, None, None, &ant, "test").await.unwrap();
    let mut stream = bus::ensure_stream(&js).await.unwrap();

    // A human in the chain, as a misconfigured escalate_to may hold, is told once, as the default human.
    let human = UserId::human();
    let approver =
        NatsApprover::new(client, js, ant.clone(), Duration::from_millis(300)).with_chain(chain(&[&lead, &human]));
    assert_eq!(approver.decide(thread.clone(), tool(), vec![Verdict::AllowOnce]).await, Verdict::Reject);

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
    let (human, human_js) = bus::connect(&server.url, None, None, &UserId::human(), "human").await.unwrap();
    let mut requests = human.subscribe(subject::approvals(&ant)).await.unwrap();
    subscribed(&human_js).await;
    let approver = NatsApprover::new(client, js, ant.clone(), WAIT).with_chain(chain(&[&lead]));
    let options = vec![Verdict::AllowOnce, Verdict::AllowAlways];
    let decide =
        |request: &ApprovalRequest, verdict| Envelope::decision(thread.clone(), lead.clone(), request.decide(verdict));

    // The inbox consumer hands over the lead's decision, and a human's comes after it.
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options.clone()));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    approver.offer(&Envelope::decision(thread.clone(), "dev-c".parse().unwrap(), request.decide(Verdict::AllowAlways)));
    approver.offer(&decide(&request, Verdict::AllowOnce));
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowOnce);
    let late = request.decide(Verdict::Reject);
    human_js.publish(late.subject(), serde_json::to_vec(&late).unwrap().into()).await.unwrap().await.unwrap();

    // A human's decision stored by the time the lead's arrives wins. The runtime is blocked while another stores
    // it, so the approver gets both at once.
    let decided = tokio::spawn(approver.decide(thread.clone(), tool(), options));
    let msg = timeout(WAIT, requests.next()).await.unwrap().unwrap();
    let request: ApprovalRequest = serde_json::from_slice(&msg.payload).unwrap();
    let (url, human_says) = (server.url.clone(), request.decide(Verdict::AllowAlways));
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let (_human, js) = bus::connect(&url, None, None, &UserId::human(), "human-2").await.unwrap();
            js.publish(human_says.subject(), serde_json::to_vec(&human_says).unwrap().into())
                .await
                .unwrap()
                .await
                .unwrap();
        })
    })
    .join()
    .unwrap();
    approver.offer(&decide(&request, Verdict::AllowOnce));
    assert_eq!(timeout(WAIT, decided).await.unwrap().unwrap(), Verdict::AllowAlways);
}
