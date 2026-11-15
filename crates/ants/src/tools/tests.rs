use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{Role, ThreadId, ToolCall};

use super::*;
use crate::auth::tests::Server;

fn request(ant: &UserId, title: &str, offered: Vec<Verdict>, timeout: Duration) -> ApprovalRequest {
    let tool = ToolCall { kind: "execute".into(), title: title.into(), ..Default::default() };
    ApprovalRequest::new(ant.clone(), ThreadId::generate(), tool, offered, timeout)
}

fn titles(requests: &[ApprovalRequest]) -> Vec<&str> {
    requests.iter().map(|r| r.tool.title.as_str()).collect()
}

/// Runs as the human and dev-a with their own credentials, so it also checks their permissions.
#[tokio::test]
async fn pending_requests_are_listed_and_answered() {
    let Some(server) = Server::start() else { return };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let (_, human) = server.connect(&UserId::human()).await;
    let stream = bus::ensure_stream(&human).await.unwrap();
    let (_, ant_js) = server.connect(&dev_a).await;
    let minute = Duration::from_secs(60);
    let push = request(&dev_a, "git push", vec![Verdict::AllowOnce, Verdict::AllowAlways], minute);
    let rm = request(&dev_a, "rm -rf build", vec![Verdict::AllowOnce], minute);
    let late = request(&dev_a, "curl example.com", vec![Verdict::AllowOnce], Duration::ZERO);
    for r in [&push, &rm, &late] {
        bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
    }
    // dev-b publishes only on its own subject, so a request there naming dev-a is forged.
    let dev_b: UserId = "dev-b".parse().unwrap();
    let (_, intruder) = server.connect(&dev_b).await;
    let forged = request(&dev_a, "git push --force", vec![Verdict::AllowAlways], minute);
    bus::publish_json(&intruder, subject::approvals(&dev_b), &forged, None).await.unwrap();
    assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["git push", "rm -rf build"]);
    let err = answer(&human, &stream, &forged.id.to_string(), Verdict::AllowAlways, Utc::now()).await.unwrap_err();
    assert!(err.to_string().contains("no tool request"), "{err:#}");

    let id = push.id.to_string();
    let decision = answer(&human, &stream, &id[..id.len() - 4], Verdict::AllowAlways, Utc::now()).await.unwrap();
    assert_eq!((decision.request_id, decision.verdict), (push.id, Verdict::AllowAlways));
    assert_eq!(decision.by, Some(UserId::human()), "the decider, told to the human the request waited for");
    let stored = stream.get_last_raw_message_by_subject(&push.decision_subject()).await.unwrap();
    assert_eq!(stored.subject.as_str(), format!("ants.approvals.dev-a.{}", push.id));
    assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), decision);
    assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["rm -rf build"]);

    let again = answer(&human, &stream, &id, Verdict::Reject, Utc::now()).await.unwrap_err();
    assert!(again.to_string().contains("already answered: allow-always"), "{again:#}");
    let expired = answer(&human, &stream, &late.id.to_string(), Verdict::AllowOnce, Utc::now()).await.unwrap_err();
    assert!(expired.to_string().contains("expired"), "{expired:#}");
    let unoffered = answer(&human, &stream, &rm.id.to_string(), Verdict::AllowAlways, Utc::now()).await.unwrap_err();
    let rm_id = qualified(&rm);
    assert_eq!(
        unoffered.to_string(),
        format!(
            "allow-always is not offered for request {rm_id}; answer ants tools approve {rm_id} | \
                 ants tools reject {rm_id}"
        )
    );
    assert_eq!(super::decision(&stream, &rm).await.unwrap(), None, "an unoffered verdict publishes nothing");
    let (seq, _) = find(&stream, &rm.id.to_string()).await.unwrap();
    assert_eq!(state(&human, &stream, seq, &rm, rm.expires_at).await.unwrap(), State::Expired);
}

/// dev-b reuses the id of dev-a's request under its own name: a bare id is refused with both qualified ids, and
/// `<ant>/<id>` answers the right one, on that ant's subject.
#[tokio::test]
async fn an_ant_qualified_id_picks_among_colliding_ids() {
    let Some(server) = Server::start() else { return };
    let (dev_a, dev_b): (UserId, UserId) = ("dev-a".parse().unwrap(), "dev-b".parse().unwrap());
    let (_, human) = server.connect(&UserId::human()).await;
    let stream = bus::ensure_stream(&human).await.unwrap();
    let minute = Duration::from_secs(60);
    let real = request(&dev_a, "git push", vec![Verdict::AllowOnce], minute);
    let mut copy = request(&dev_b, "rm -rf /", vec![Verdict::AllowOnce], minute);
    copy.id = real.id;
    for (ant, r) in [(&dev_a, &real), (&dev_b, &copy)] {
        let (_, ant_js) = server.connect(ant).await;
        bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
    }

    let id = real.id.to_string();
    let err = answer(&human, &stream, &id, Verdict::AllowOnce, Utc::now()).await.unwrap_err();
    assert!(err.to_string().contains(&format!("2 tool requests match {id}: dev-a/{id}, dev-b/{id};")), "{err:#}");

    let decision =
        answer(&human, &stream, &format!("dev-a/{}", &id[..8]), Verdict::AllowOnce, Utc::now()).await.unwrap();
    assert_eq!((&decision.ant, decision.request_id), (&dev_a, real.id));
    let stored = stream.get_last_raw_message_by_subject(&real.decision_subject()).await.unwrap();
    assert_eq!(stored.subject.as_str(), format!("ants.approvals.dev-a.{id}"));
    assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["rm -rf /"]);
    assert_eq!(find(&stream, &format!("dev-b/{id}")).await.unwrap().1, copy);
}

/// Two humans with their own credentials see the same pending request; when both answer it, the first decision
/// is the one stored and the second is refused, even when both checked it was pending before either answered.
#[tokio::test]
async fn every_human_sees_every_request_and_the_first_decision_wins() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a): (UserId, UserId, UserId) =
        ("alice".parse().unwrap(), "bob".parse().unwrap(), "dev-a".parse().unwrap());
    let (_, a_js) = server.connect_as(&alice, Role::Human).await;
    let (_, b_js) = server.connect_as(&bob, Role::Human).await;
    let a_stream = bus::ensure_stream(&a_js).await.unwrap();
    let b_stream = bus::ensure_stream(&b_js).await.unwrap();
    let (ant_client, ant_js) = server.connect(&dev_a).await;
    let push = request(&dev_a, "git push", vec![Verdict::AllowOnce, Verdict::Reject], Duration::from_secs(60));
    let mut heard = ant_client.subscribe(push.decision_subject()).await.unwrap();
    ant_client.flush().await.unwrap();
    bus::publish_json(&ant_js, push.subject(), &push, None).await.unwrap();
    for (js, stream) in [(&a_js, &a_stream), (&b_js, &b_stream)] {
        assert_eq!(titles(&pending(js, stream, Utc::now()).await.unwrap()), ["git push"]);
    }

    let (seq, _) = find(&a_stream, &push.id.to_string()).await.unwrap();
    let a_last = check(&a_js, &a_stream, seq, &push, Verdict::AllowOnce, Utc::now()).await.unwrap();
    let b_last = check(&b_js, &b_stream, seq, &push, Verdict::Reject, Utc::now()).await.unwrap();
    let first = decide(&a_js, &a_stream, &push, push.decide(Verdict::AllowOnce), a_last).await.unwrap();
    let late = decide(&b_js, &b_stream, &push, push.decide(Verdict::Reject), b_last).await.unwrap_err();
    assert!(late.to_string().contains("already answered: allow-once"), "{late:#}");
    let again = answer(&b_js, &b_stream, &push.id.to_string(), Verdict::Reject, Utc::now()).await.unwrap_err();
    assert!(again.to_string().contains("already answered: allow-once"), "{again:#}");
    for (js, stream) in [(&a_js, &a_stream), (&b_js, &b_stream)] {
        assert_eq!(state(js, stream, seq, &push, Utc::now()).await.unwrap(), State::Decided(first.clone()));
        assert!(pending(js, stream, Utc::now()).await.unwrap().is_empty());
    }
    // The ant hears alice's decision, and acts on the one stored, which is alice's too.
    let msg = tokio::time::timeout(Duration::from_secs(5), heard.next()).await.unwrap().unwrap();
    assert_eq!(serde_json::from_slice::<ApprovalDecision>(&msg.payload).unwrap(), first);
    let ant_stream = ant_js.get_stream_no_info(STREAM).await.unwrap();
    let stored = ant_stream.direct_get_last_for_subject(push.decision_subject()).await.unwrap();
    assert_eq!(serde_json::from_slice::<ApprovalDecision>(&stored.payload).unwrap(), first);
}

/// lead is in dev-a's escalate_to, kept in `ants_chains` as dev-a's roster entry expired; dev-c is not. Each sends
/// decisions with its own credentials to dev-a's inbox.
#[tokio::test]
async fn a_request_an_ant_of_the_chain_decided_is_no_longer_pending() {
    let Some(server) = Server::start() else { return };
    let [dev_a, lead, dev_c]: [UserId; 3] = ["dev-a", "lead", "dev-c"].map(|p| p.parse().unwrap());
    let (_, human) = server.connect(&UserId::human()).await;
    let stream = bus::ensure_stream(&human).await.unwrap();
    bus::ensure_chains(&human).await.unwrap();
    let (_, ant_js) = server.connect(&dev_a).await;
    ants_daemon::chains::publish(&ant_js, &dev_a, std::slice::from_ref(&lead)).await.unwrap();
    let minute = Duration::from_secs(60);
    let offered = || vec![Verdict::AllowOnce, Verdict::Reject];
    let [push, rm, cat] = ["git push", "rm -rf build", "cat secrets"].map(|t| request(&dev_a, t, offered(), minute));
    for r in [&push, &rm, &cat] {
        bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
    }
    let decide_as = async |js: &jetstream::Context, from: &UserId, r: &ApprovalRequest, verdict| {
        let envelope =
            Envelope::decision(r.thread_id.clone(), from.clone(), ApprovalDecision::new(r.id, r.ant.clone(), verdict));
        bus::publish_json(js, envelope.subject(), &envelope, None).await.unwrap();
    };
    let (_, lead_js) = server.connect(&lead).await;
    let (_, intruder) = server.connect(&dev_c).await;
    decide_as(&lead_js, &lead, &push, Verdict::AllowOnce).await;
    decide_as(&intruder, &dev_c, &rm, Verdict::AllowOnce).await;
    answer(&human, &stream, &cat.id.to_string(), Verdict::Reject, Utc::now()).await.unwrap();
    decide_as(&lead_js, &lead, &cat, Verdict::AllowOnce).await;
    assert_eq!(titles(&pending(&human, &stream, Utc::now()).await.unwrap()), ["rm -rf build"]);

    let err = answer(&human, &stream, &push.id.to_string(), Verdict::Reject, Utc::now()).await.unwrap_err();
    assert_eq!(err.to_string(), format!("request dev-a/{} was already answered by lead: allow-once", push.id));
    assert_eq!(super::decision(&stream, &push).await.unwrap(), None, "nothing stored for the human");
    let state_of = async |r: &ApprovalRequest| {
        let (seq, _) = find(&stream, &qualified(r)).await.unwrap();
        state(&human, &stream, seq, r, Utc::now()).await.unwrap()
    };
    let State::DecidedByAnt(by, decision) = state_of(&push).await else { panic!("decided by lead") };
    assert_eq!((by, decision.request_id, decision.verdict), (lead.clone(), push.id, Verdict::AllowOnce));
    let State::Decided(first) = state_of(&cat).await else { panic!("the human's came first") };
    assert_eq!(first.verdict, Verdict::Reject);
    assert_eq!(state_of(&rm).await, State::Pending, "dev-c is not in dev-a's chain");
}

/// Earlier approvals of dev-a for the same command make the rule suggested; rejected ones, other commands and
/// later requests do not count.
#[tokio::test]
async fn repeated_approvals_suggest_an_allow_rule() {
    let Some(server) = Server::start() else { return };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let (_, human) = server.connect(&UserId::human()).await;
    let stream = bus::ensure_stream(&human).await.unwrap();
    let (_, ant_js) = server.connect(&dev_a).await;
    let mut stored = Vec::new();
    for (command, verdict) in [
        ("cargo test", Some(Verdict::AllowOnce)),
        ("cargo test", Some(Verdict::Reject)),
        ("git push", Some(Verdict::AllowOnce)),
        ("cargo test", Some(Verdict::AllowAlways)),
        ("cargo test", None),
        ("cargo test", None),
    ] {
        let mut r = request(&dev_a, command, vec![Verdict::AllowOnce, Verdict::AllowAlways], Duration::from_secs(60));
        r.tool.input = Some(serde_json::json!({ "command": command }).to_string());
        bus::publish_json(&ant_js, r.subject(), &r, None).await.unwrap();
        let (seq, _) = find(&stream, &qualified(&r)).await.unwrap();
        if let Some(verdict) = verdict {
            answer_request(&human, &stream, seq, &r, verdict, &UserId::human(), Utc::now()).await.unwrap();
        }
        stored.push((seq, r));
    }
    let suggested = async |i: usize| suggestion(&stream, stored[i].0, &stored[i].1).await.unwrap();
    assert_eq!(suggested(3).await, None, "the second approval");
    let third = Suggestion { rule: "execute:cargo test".to_owned(), approved_before: 2 };
    assert_eq!(suggested(4).await.as_ref(), Some(&third));
    assert_eq!(suggested(5).await, Some(third), "a request still pending is no approval");
    assert_eq!(suggested(2).await, None, "git push was allowed once");
    assert!(suggestion_text(&stored[4].1, &suggested(4).await.unwrap()).contains("allowed this 3 times"));
}

/// Following reports the pending request, then a new one, and how each ends: answered by the human, by lead of
/// dev-a's escalate_to, or expired.
#[tokio::test]
async fn following_reports_new_requests_and_how_each_ends() {
    let Some(server) = Server::start() else { return };
    let [dev_a, lead]: [UserId; 2] = ["dev-a", "lead"].map(|p| p.parse().unwrap());
    let (client, human) = server.connect(&UserId::human()).await;
    let stream = bus::ensure_stream(&human).await.unwrap();
    bus::ensure_chains(&human).await.unwrap();
    let (_, ant_js) = server.connect(&dev_a).await;
    ants_daemon::chains::publish(&ant_js, &dev_a, std::slice::from_ref(&lead)).await.unwrap();
    let (_, lead_js) = server.connect(&lead).await;
    let offered = || vec![Verdict::AllowOnce, Verdict::Reject];
    let minute = Duration::from_secs(60);
    let push = request(&dev_a, "git push", offered(), minute);
    bus::publish_json(&ant_js, push.subject(), &push, None).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let following = follow(&client, &human, |update| {
        tx.send(update.clone()).unwrap();
        Ok(())
    });
    let script = async {
        let mut next = async || tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!(next().await, Update::Pending(push.clone()));
        let [rm, cat] = [("rm -rf build", minute), ("cat secrets", Duration::from_secs(3))]
            .map(|(title, timeout)| request(&dev_a, title, offered(), timeout));
        for r in [&rm, &cat] {
            bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
            assert_eq!(next().await, Update::Pending(r.clone()));
        }
        let shown = render(&Update::Pending(rm.clone()), Utc::now());
        let id = qualified(&rm);
        assert!(shown.contains(&format!("answer with ants tools approve {id} | ants tools reject {id}")), "{shown}");

        let answered = answer(&human, &stream, &qualified(&push), Verdict::AllowOnce, Utc::now()).await.unwrap();
        assert_eq!(next().await, Update::Resolved(push.clone(), State::Decided(answered)));
        let decision = ApprovalDecision::new(rm.id, dev_a.clone(), Verdict::Reject);
        let envelope = Envelope::decision(rm.thread_id.clone(), lead.clone(), decision);
        bus::publish_json(&lead_js, envelope.subject(), &envelope, None).await.unwrap();
        let Update::Resolved(r, State::DecidedByAnt(by, d)) = next().await else { panic!("decided by lead") };
        assert_eq!((r, by, d.verdict), (rm, lead.clone(), Verdict::Reject));
        assert_eq!(next().await, Update::Resolved(cat.clone(), State::Expired));
    };
    tokio::select! {
        ended = following => panic!("following ended: {ended:?}"),
        () = script => {}
    }
}

#[test]
fn a_query_names_a_known_ant_and_a_request_id() {
    let minute = Duration::from_secs(60);
    let dev_a: UserId = "dev-a".parse().unwrap();
    let known = vec![request(&dev_a, "git push", vec![], minute)];
    let id = known[0].id.to_string();
    let pick = |text: &str| Query::parse(text).and_then(|q| select(known.clone(), &q)).map_err(|e| e.to_string());
    assert_eq!(
        pick(&format!(" DEV-A/{} ", id.to_uppercase())).unwrap_err(),
        "invalid user id \"DEV-A\": use 1-64 of a-z, 0-9 and '-', starting with a letter or digit"
    );
    assert_eq!(pick(&format!("dev-a/{}", id.to_uppercase())), Ok(known[0].clone()));
    assert_eq!(pick(&format!("dev-c/{id}")).unwrap_err(), format!("no tool request dev-c/{id}"));
    assert_eq!(pick("0000").unwrap_err(), "no tool request 0000");
    assert_eq!(pick("dev-a/").unwrap_err(), "no request id given");
    assert!(pick("dev-a/dev-b/0").unwrap_err().contains("is not a request id"));
    assert!(pick("/0").unwrap_err().contains("invalid user id"));
}

#[test]
fn time_left_reads_at_a_glance() {
    let now = Utc::now();
    let secs = |s| left(now + TimeDelta::seconds(s), now);
    assert_eq!([secs(-5), secs(45), secs(245), secs(3 * 3600 + 120)], ["0s", "45s", "4m05s", "3h02m"]);
}
