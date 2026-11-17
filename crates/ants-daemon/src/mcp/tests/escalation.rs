//! Escalating up a chain and deciding approval requests from it.

use super::*;

#[tokio::test]
async fn decide_approval_answers_a_request_from_the_chain() {
    let home = home("decide");
    let (mcp, mut sent) = start(&home).await;
    let tool = ants_proto::ToolCall {
        kind: "execute".to_owned(),
        title: "rm -rf target".to_owned(),
        locations: Vec::new(),
        input: None,
    };
    let request = ants_proto::ApprovalRequest::new(
        user("dev-b"),
        ThreadId::generate(),
        tool,
        vec![Verdict::AllowOnce, Verdict::Reject],
        Duration::from_secs(60),
    );
    let ants_proto::Hop::Chain { escalation, .. } =
        ants_proto::Escalation::of_request(&request).up(&[user("dev-a")], 3)
    else {
        panic!("a chain hop")
    };
    let asked = Envelope::on_thread(request.thread_id.clone(), user("dev-b"), user("dev-a"), "may it?")
        .with_owner(Some(UserId::human()))
        .with_escalation(Some(*escalation));
    let client = {
        let (url, token) = http_server(&mcp, &asked.thread_id);
        http_client(&url, &token).await
    };

    let plain = Envelope::on_thread(asked.thread_id.clone(), user("dev-b"), user("dev-a"), "hi");
    let turn = mcp.begin(&plain);
    let (ok, text) = call(&client, "decide_approval", serde_json::json!({"verdict": "reject"})).await;
    assert!(!ok && text.contains("no approval request"), "{text}");
    drop(turn);

    let _turn = mcp.begin(&asked);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "unsure"})).await;
    assert!(!ok && text.contains("dev-b's approval request, which goes no higher"), "{text}");
    let (ok, text) = call(&client, "decide_approval", serde_json::json!({"verdict": "maybe"})).await;
    assert!(!ok && text.contains("unknown verdict"), "{text}");
    let (ok, text) = call(&client, "decide_approval", serde_json::json!({"verdict": "allow-once"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.from, &out.to, &out.thread_id), (&user("dev-a"), &user("dev-b"), &request.thread_id));
    assert_eq!(out.owner, Some(UserId::human()));
    assert_eq!(out.chain_decision(&request, &[user("dev-a")]).unwrap().verdict, Verdict::AllowOnce);
    assert!(sent.try_recv().is_err(), "one decision");
}

/// An MCP server for `dev-a` escalating up `escalate_to`, whose sent envelopes come out of the receiver.
async fn start_escalating(home: &Path, escalate_to: &[&str]) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    start_escalating_with(home, escalate_to, Vec::new()).await
}

async fn start_escalating_with(
    home: &Path,
    escalate_to: &[&str],
    colony: Vec<ColonyEntry>,
) -> (Mcp, mpsc::UnboundedReceiver<Envelope>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let (outbox_tx, send_tx) = (tx.clone(), tx);
    let outbox: Outbox = Arc::new(move |envelope| {
        let _ = outbox_tx.send(envelope);
        Box::pin(async { Ok(()) })
    });
    let send: crate::escalation::Sender = Arc::new(move |envelope, _id| {
        let _ = send_tx.send(envelope);
        Box::pin(async { Ok(None) })
    });
    let colony: Colony = Arc::new(move || {
        let colony = colony.clone();
        Box::pin(async move { Ok(colony) })
    });
    let chain =
        crate::escalation::Chain { escalate_to: escalate_to.iter().map(|p| user(p)).collect(), ..Default::default() };
    let ledger = Arc::new(crate::caps::Ledger::memory(user("dev-a"), crate::caps::Caps::default()));
    let humans = Registry::default();
    let escalator = Escalator::new(user("dev-a"), chain, colony.clone(), humans.clone(), ledger, send);
    let notes = Arc::new(MemoryNotes::default());
    let mcp = Mcp::start(user("dev-a"), home, outbox, colony, notes, humans, Some(Arc::new(escalator)));
    (mcp.await.unwrap(), rx)
}

#[tokio::test]
async fn escalate_goes_up_the_chain_and_only_the_leader_sends_it_higher() {
    let home = home("escalate");
    let (mcp, mut sent) = start_escalating(&home, &["lead", "dev-b"]).await;
    let current = from_human();
    let thread = current.thread_id.clone();
    let client = {
        let (url, token) = http_server(&mcp, &thread);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "the schema is unclear"})).await;
    assert!(ok && text.contains("to level 1: sent to lead, dev-b; lead leads"), "{text}");
    for to in ["lead", "dev-b"] {
        let envelope = sent.try_recv().unwrap();
        assert_eq!(envelope.to, user(to));
        let escalation = envelope.escalation.as_deref().unwrap();
        assert_eq!((escalation.level, escalation.reason), (1, EscalationReason::Asked));
        assert!(envelope.text.contains("dev-a asks for help: the schema is unclear"), "{}", envelope.text);
    }
    assert!(sent.try_recv().is_err());

    // On dev-c's escalation, led by lead, dev-a is a member: it neither escalates nor answers dev-c.
    let start = ants_proto::Escalation::new(user("dev-c"), thread.clone(), EscalationReason::TurnBudget);
    let ants_proto::Hop::Chain { escalation: theirs, .. } = start.up(&[user("lead"), user("dev-a")], 3) else {
        panic!("a chain")
    };
    let member = Envelope::on_thread(thread.clone(), user("dev-c"), user("dev-a"), "help dev-c")
        .with_escalation(Some(*theirs.clone()));
    drop(_turn);
    let _turn = mcp.begin(&member);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "no idea"})).await;
    assert!(!ok && text.contains("only lead escalates the escalation of dev-c's thread"), "{text}");
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "try X"})).await;
    assert!(!ok && text.contains("only lead answers dev-c"), "{text}");
    assert!(sent.try_recv().is_err());

    // Led by dev-a, it goes up dev-a's chain, one level above, and dev-c hears of it.
    let other = ThreadId::generate();
    let start = ants_proto::Escalation::new(user("dev-c"), other.clone(), EscalationReason::TurnBudget);
    let ants_proto::Hop::Chain { escalation: led, .. } = start.up(&[user("dev-a")], 3) else { panic!("a chain") };
    let leading =
        Envelope::on_thread(other.clone(), user("dev-c"), user("dev-a"), "help dev-c").with_escalation(Some(*led));
    let client = {
        let (url, token) = http_server(&mcp, &other);
        http_client(&url, &token).await
    };
    let _other = mcp.begin(&leading);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "beyond dev-a"})).await;
    assert!(ok && text.contains("to level 2: sent to lead, dev-b"), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!(notice.to, user("dev-c"));
    assert_eq!(notice.escalation.as_deref().map(|e| (e.level, &e.leader)), Some((2, &user("lead"))));
    let to: Vec<_> = std::iter::from_fn(|| sent.try_recv().ok()).map(|e| e.to).collect();
    assert_eq!(to, [user("lead"), user("dev-b")]);
}

#[tokio::test]
async fn escalate_on_a_thread_another_ant_created_goes_up_the_creator_s_chain() {
    let lead = ColonyEntry { escalate_to: vec![user("boss")], ..listed("lead", &[]) };
    let home = home("escalate-creator");
    let (mcp, mut sent) = start_escalating_with(&home, &["dev-b"], vec![lead]).await;
    let current = Envelope::new(user("lead"), user("dev-a"), "fix it").with_creator(Some(user("lead")));
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(ok && text.contains("to level 1: sent to boss; boss leads"), "{text}");
    let envelope = sent.try_recv().unwrap();
    assert_eq!((&envelope.to, &envelope.creator), (&user("boss"), &Some(user("lead"))));
    let escalation = envelope.escalation.as_deref().unwrap();
    assert_eq!((escalation.level, &escalation.stuck), (1, &user("dev-a")));
    assert_eq!(escalation.visited, [user("dev-a"), user("lead"), user("boss")]);
    assert!(sent.try_recv().is_err(), "dev-a's own chain is not used");

    // A creator with no chain sends it to the owner, else the default human.
    let other_home = super::home("escalate-creator-none");
    let (mcp, mut sent) = start_escalating_with(&other_home, &["dev-b"], vec![]).await;
    let current = Envelope::new(user("lead"), user("dev-a"), "fix it").with_creator(Some(user("lead")));
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(ok && text.contains("to its owner human"), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!(notice.to, UserId::human());
    assert!(notice.text.contains("starts at lead, which created thread"), "{}", notice.text);
}

#[tokio::test]
async fn escalate_without_a_chain_goes_to_the_owner() {
    let home = home("escalate-owner");
    let (mcp, mut sent) = start_escalating(&home, &[]).await;
    let current = from_human();
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "stuck"})).await;
    assert!(ok && text.ends_with("to its owner human: dev-a has no escalate_to chain."), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!((notice.to, notice.in_reply_to), (UserId::human(), Some(current.id)));
}

#[tokio::test]
async fn an_assistant_passes_an_escalation_on_to_its_human() {
    let home = home("escalate-assistant");
    let (mcp, mut sent) = start_escalating(&home, &["lead"]).await;
    let stuck = ants_proto::Escalation::new(user("dev-c"), ThreadId::generate(), EscalationReason::TurnBudget);
    let led = stuck.to_assistant(user("dev-a"), user("alice"));
    let current = Envelope::on_thread(led.thread_id.clone(), user("dev-c"), user("dev-a"), "help dev-c")
        .with_escalation(Some(led.clone()));
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "escalate", serde_json::json!({"issue": "alice must pick"})).await;
    assert!(ok && text.contains("Passed the escalation of dev-c") && text.contains("on to alice"), "{text}");
    let out = sent.try_recv().unwrap();
    assert_eq!(
        (&out.to, &out.text, out.in_reply_to),
        (&user("dev-c"), &"alice must pick".to_owned(), Some(current.id))
    );
    assert_eq!(out.escalation.as_deref(), led.passed_on().as_ref());
    assert!(sent.try_recv().is_err(), "not up the assistant's own chain");
}
