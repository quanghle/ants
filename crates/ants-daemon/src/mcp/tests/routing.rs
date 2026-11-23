//! Whom requests go to: humans, ants by colony capabilities, ants requested and retired.

use super::*;

#[tokio::test]
async fn registered_humans_are_humans_to_the_tools() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let home = home("humans");
    let (mcp, mut sent) = start_with(&home, Vec::new(), Arc::default(), humans).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q").with_owner(Some(user("alice")));
    current.turn_budget = 0;
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "alice", "text": "?"})).await;
    assert!(!ok && text.contains("send_message reaches alice"), "{text}");
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "alice", "text": "stuck"})).await;
    assert!(ok, "a message to a human spends no turn: {text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((out.to.as_str(), out.turn_budget, &out.owner), ("alice", 0, &current.owner));

    // A registered human's message makes messages to ants free, and its owner goes on to threads started from it.
    let mut current = Envelope::new(user("alice"), user("dev-a"), "go").with_owner(Some(user("alice")));
    current.turn_budget = 1;
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) =
        call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "x", "thread": "new"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_ne!(out.thread_id, current.thread_id);
    assert_eq!((out.turn_budget, &out.owner), (1, &current.owner));
}

#[tokio::test]
async fn requests_are_routed_by_colony_capabilities() {
    let mut billing = listed("dev-c", &["Rust", "billing"]);
    billing.whoami = Some("Maintain billing services.\nReview invoices.".to_owned());
    billing.profile = Some("Good for invoices and payment providers.".to_owned());
    let mut docs = listed("docs", &[]);
    docs.activity = ants_proto::Activity::Working;
    docs.agent = Some(ants_proto::HarnessAgent { name: "copilot".to_owned(), version: "1.0.3".to_owned() });
    let mut busy = listed("dev-b", &["rust"]);
    busy.activity = ants_proto::Activity::Working;
    busy.handling = 1;
    let mut out = listed("ops", &["billing"]);
    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    out.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: Some("copilot login".to_owned()) });
    let mut limited = listed("dev-q", &[]);
    let until = "2026-01-01T00:10:00Z".parse().unwrap();
    limited.limited = Some(ants_proto::HarnessLimit { kind: ants_proto::LimitKind::Quota, since, until });
    limited.memory_low = Some(ants_proto::MemoryLow { since, available: 512 << 20, max_harnesses: 1 });
    let mut flaky = listed("dev-d", &["rust"]);
    flaky.profile = Some("Good for flaky tests.".to_owned());
    let colony = vec![listed("dev-a", &["rust"]), billing, out, busy, docs, limited, flaky];
    let home = home("route");
    let (mcp, mut sent) = start_with_colony(&home, colony).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;

    let (ok, text) = call(&client, "list_ants", serde_json::json!({})).await;
    assert!(
        ok && !text.contains("dev-a:")
            && text.contains("docs: no capabilities (copilot running copilot 1.0.3 on m1, working)"),
        "{text}"
    );
    let described = "whoami (advertised):\nMaintain billing services.\nReview invoices.\n  profile (the ant's own \
                     words):\nGood for invoices and payment providers.";
    assert!(text.contains(described), "{text}");
    assert!(
        text.contains(
            "ops: billing (copilot on m1, signed out)\n  harness signed out since 2026-01-01T00:00:00Z: messages \
                 to ops wait unanswered until a human signs it in (`copilot login` on m1)"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "dev-q: no capabilities (copilot on m1, out-of-quota)\n  harness held back by a provider limit \
                 (out-of-quota): messages to dev-q wait until about 2026-01-01T00:10:00Z\n  memory low on m1 since \
                 2026-01-01T00:00:00Z: at most 1 harness runs at once, so messages may wait longer"
        ),
        "{text}"
    );
    let (ok, text) = call(&client, "list_ants", serde_json::json!({"capability": "billing"})).await;
    assert!(ok && text.starts_with("dev-c: Rust, billing (copilot on m1, idle)") && !text.contains("dev-b"), "{text}");
    assert!(text.contains("Maintain billing services.\nReview invoices."), "{text}");

    // dev-a has the capability too, but does not route to itself; dev-b comes before dev-c by name, but works.
    let (ok, text) =
        call(&client, "route", serde_json::json!({"capability": "RUST", "text": "fix it", "wait": false})).await;
    assert!(ok && text.starts_with("Routed to dev-c for RUST."), "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.to, out.text.as_str(), &out.thread_id), (&user("dev-c"), "fix it", &current.thread_id));
    // dev-d's profile shares words with the request, which breaks its tie with dev-c, and is not echoed.
    let request = serde_json::json!({"capability": "rust", "text": "The tests are flaky.", "wait": false});
    let (ok, text) = call(&client, "route", request).await;
    assert!(ok && text.starts_with("Routed to dev-d for rust.") && !text.contains("Good for"), "{text}");
    assert_eq!(sent.recv().await.unwrap().to, user("dev-d"));

    let asking = tokio::spawn(async move {
        call(&client, "route", serde_json::json!({"capability": "billing", "text": "invoice?"})).await
    });
    let ask = sent.recv().await.unwrap();
    assert_eq!(ask.to, user("dev-c"), "not ops: idle, but signed out");
    let reply = ask.reply(user("dev-c"), "paid", Role::Ant).unwrap();
    mcp.hub.route(reply.clone()).await;
    let (ok, text) = asking.await.unwrap();
    let routed = format!("Routed to dev-c for billing. dev-c replied (message {}):\npaid", reply.id);
    assert!(ok && text == routed, "{text}");

    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "route", serde_json::json!({"capability": "go", "text": "?"})).await;
    assert!(!ok && text.contains("Capabilities there: billing, rust."), "{text}");
    assert!(sent.try_recv().is_err(), "nothing is sent without an ant to take it");
}

/// Looks ants up in `colony`.
fn lookup_in(colony: Vec<ColonyEntry>) -> ColonyLookup {
    Arc::new(move |ant| {
        let entry = colony.iter().find(|e| e.ant == ant).cloned();
        Box::pin(async move { Ok(entry) })
    })
}

#[tokio::test]
async fn request_and_retire_ant_are_offered_to_managers_and_request_returns_once_the_ant_is_in_the_colony() {
    let home = home("request");
    let (mcp, _sent) = start(&home).await;
    let thread = ThreadId::generate();
    let (url, token) = http_server(&mcp, &thread);
    let names = async |client: &Client| -> Vec<String> {
        client.list_all_tools().await.unwrap().into_iter().map(|t| t.name.to_string()).collect()
    };
    let offered = names(&http_client(&url, &token).await).await;
    assert!(!offered.contains(&"request_ant".to_owned()) && !offered.contains(&"retire_ant".to_owned()));

    let asked = Arc::new(StdMutex::new(Vec::<AntAsk>::new()));
    let retired = Arc::new(StdMutex::new(Vec::<RetireAsk>::new()));
    let (seen, seen_retired) = (asked.clone(), retired.clone());
    mcp.with_requester(Arc::new(move |ask: ManagerAsk| {
        let ask = match ask {
            ManagerAsk::Add(ask) => *ask,
            ManagerAsk::Retire(ask) => {
                seen_retired.locked().push(ask);
                return Box::pin(async { Ok("Retired dev-a-docs.".to_owned()) });
            }
        };
        seen.locked().push(ask.clone());
        Box::pin(async move {
            match ask.name.as_str() {
                "dev-a-docs" => Ok("Added dev-a-docs.".to_owned()),
                _ => Err("the signer refused it".to_owned()),
            }
        })
    }));
    mcp.with_lookup(lookup_in(vec![listed("dev-a-docs", &[])]));
    let client = http_client(&url, &token).await;
    let offered = names(&client).await;
    assert!(offered.contains(&"request_ant".to_owned()) && offered.contains(&"retire_ant".to_owned()));

    let args = serde_json::json!({
        "name": "dev-a-docs", "template": "worker", "reason": "docs",
        "overrides": {"model": "gpt-5", "prompt_timeout": "10m"},
    });
    let (ok, text) = call(&client, "request_ant", args).await;
    assert!(ok && text.starts_with("Added dev-a-docs.") && text.contains("is in the colony"), "{text}");
    let ask = asked.locked()[0].clone();
    assert_eq!(
        (ask.manager, ask.thread, ask.template, ask.wait),
        (user("dev-a"), thread.clone(), "worker".to_owned(), DEFAULT_REQUEST_WAIT)
    );
    assert_eq!(ask.request.model.as_deref(), Some("gpt-5"));
    assert_eq!(ask.request.prompt_timeout, Some(Duration::from_mins(10)));

    let args = serde_json::json!({"name": "dev-a-x", "template": "worker", "reason": "x", "timeout": "1m"});
    let (ok, text) = call(&client, "request_ant", args).await;
    assert!(!ok && text.contains("the signer refused it"), "{text}");
    assert_eq!(asked.locked()[1].wait, Duration::from_mins(1));
    let args = serde_json::json!({"name": "dev-a-y", "template": "worker", "reason": "y", "overrides": {"x": 1}});
    let (ok, text) = call(&client, "request_ant", args).await;
    assert!(!ok && text.contains("overrides"), "{text}");
    assert_eq!(asked.locked().len(), 2, "a malformed request goes nowhere");

    let (ok, text) = call(&client, "retire_ant", serde_json::json!({"name": " dev-a-docs "})).await;
    assert!(ok && text == "Retired dev-a-docs.", "{text}");
    let ask = retired.locked()[0].clone();
    assert_eq!((ask.manager, ask.thread, ask.name), (user("dev-a"), thread, user("dev-a-docs")));
}

#[test]
fn a_provider_limit_counts_only_while_it_outlasts_the_wait() {
    let now = chrono::Utc::now();
    let mut entry = listed("dev-q", &[]);
    let until = now + chrono::Duration::minutes(10);
    entry.limited = Some(ants_proto::HarnessLimit { kind: ants_proto::LimitKind::Quota, since: now, until });
    let ant = user("dev-q");
    let why = why_unserved(&ant, Some(&entry), now, Duration::ZERO).expect("held now");
    assert!(why.starts_with("the harness of dev-q (copilot on m1) is out of quota: messages to dev-q wait"), "{why}");
    assert!(why_unserved(&ant, Some(&entry), now, Duration::from_mins(5)).is_some(), "past a 5m ask");
    assert_eq!(why_unserved(&ant, Some(&entry), now, Duration::from_mins(30)), None, "over within a 30m ask");
    entry.limited = None;
    assert_eq!(why_unserved(&ant, Some(&entry), now, Duration::ZERO), None);
    entry.failing = Some(ants_proto::HarnessFailing { since: now, error: "not found".to_owned() });
    let why = why_unserved(&ant, Some(&entry), now, Duration::from_mins(30)).expect("held until fixed");
    assert!(why.starts_with("the harness of dev-q (copilot on m1) fails to start since"), "{why}");

    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    entry.fallback = Some(ants_proto::ServingFallback { name: "claude".to_owned(), since });
    assert_eq!(why_unserved(&ant, Some(&entry), now, Duration::ZERO), None, "a fallback answers");
    assert!(
        describe(&[&entry]).contains(
            "\n  fallback claude answers messages to dev-q since 2026-01-01T00:00:00Z, until its own harness can again"
        ),
        "{}",
        describe(&[&entry])
    );
    entry.paused = Some(ants_proto::Paused { since, by: None });
    let why = why_unserved(&ant, Some(&entry), now, Duration::ZERO).expect("a fallback answers no paused ant");
    assert_eq!(
        why,
        "dev-q is paused since 2026-01-01T00:00:00Z: messages to dev-q wait queued, unanswered until a human resumes it"
    );
    let described = describe(&[&entry]);
    assert!(described.contains(", failing, paused)\n  paused since 2026-01-01T00:00:00Z: dev-q takes"), "{described}");
}

#[test]
fn an_ant_its_daemon_cannot_serve_is_told_with_why_and_when_it_is_tried_again() {
    let now = chrono::Utc::now();
    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    let retry_at = "2026-01-01T00:01:00Z".parse().unwrap();
    let mut entry = listed("dev-q", &["rust"]);
    entry.paused = Some(ants_proto::Paused { since, by: None });
    entry.unserved = Some(ants_proto::Unserved { since, error: "a server error".to_owned(), retry_at });
    let why = why_unserved(&user("dev-q"), Some(&entry), now, Duration::from_mins(30)).expect("not served");
    assert_eq!(
        why,
        "the daemon of dev-q on m1 cannot serve it since 2026-01-01T00:00:00Z (a server error): messages to dev-q \
         wait queued; serving is tried again at 2026-01-01T00:01:00Z"
    );
    let described = describe(&[&entry]);
    assert!(
        described.contains(
            "(copilot on m1, unserved)\n  not served since 2026-01-01T00:00:00Z (a server error): messages to dev-q \
             wait queued; serving is tried again at 2026-01-01T00:01:00Z"
        ),
        "{described}"
    );
    let served = listed("dev-r", &["rust"]);
    let entries = [ants_proto::ColonyEntry { paused: None, ..entry }, served];
    let picked = crate::colony::pick(&entries, "rust", "", &user("dev-a"), now).map(|e| e.ant.as_str());
    assert_eq!(picked, Some("dev-r"), "a served ant first");
}

#[tokio::test]
async fn messages_to_an_ant_no_daemon_serves_say_so() {
    let mut stale = listed("dev-c", &["rust"]);
    stale.heartbeat_at -= chrono::Duration::minutes(10);
    let mut out = listed("ops", &["billing"]);
    let since = "2026-01-01T00:00:00Z".parse().unwrap();
    out.signed_out = Some(ants_proto::SignInNeeded { since, sign_in: None });
    let colony = vec![listed("dev-a", &["rust"]), listed("dev-b", &["rust"]), stale, out];
    let humans = Registry::with(UserId::human(), &["alice"]);
    let home = home("unserved");
    let (mcp, mut sent) = start_with(&home, colony.clone(), Arc::default(), humans).await;
    mcp.with_lookup(lookup_in(colony));
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;

    let ask = |to: &str| serde_json::json!({"to": to, "text": "?"});
    let (ok, text) = call(&client, "ask_ant", ask("dev-bb")).await;
    assert!(!ok, "{text}");
    assert_eq!(
        text,
        "Not sent, as dev-bb is not in the colony: no daemon serves dev-bb now. Closest names in the colony: \
             dev-b, dev-c. send_message leaves a message waiting in the inbox of dev-bb instead."
    );
    let (ok, text) = call(&client, "ask_ant", ask("zed")).await;
    assert!(!ok && text.contains("now. In the colony: dev-b, dev-c, ops."), "{text}");
    let (ok, text) = call(&client, "ask_ant", ask("dev-c")).await;
    assert!(!ok && text.contains("dev-c is stale in the colony: the daemon on m1 last beat 10m"), "{text}");
    assert!(text.contains("Served ants sharing a capability of dev-c: dev-b."), "{text}");
    let (ok, text) = call(&client, "ask_ant", ask("ops")).await;
    assert!(
        !ok && text.contains(
            "the harness of ops (copilot on m1) is signed out since 2026-01-01T00:00:00Z: messages to ops wait \
                 unanswered until a human signs the harness in. No served ant shares a capability of ops"
        ),
        "{text}"
    );
    assert!(sent.try_recv().is_err(), "an ask no daemon can answer is not sent");

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-bb", "text": "hi"})).await;
    assert!(ok, "{text}");
    assert!(
        text.ends_with(
            "turns left). Warning: the message waits in the inbox of dev-bb, as dev-bb is not in the colony: no \
                 daemon serves dev-bb now. Closest names in the colony: dev-b, dev-c."
        ),
        "{text}"
    );
    assert_eq!(sent.recv().await.unwrap().to, user("dev-bb"), "sent all the same");
    for to in ["dev-b", "alice", "human"] {
        let (ok, text) = call(&client, "send_message", serde_json::json!({"to": to, "text": "hi"})).await;
        assert!(ok && !text.contains("Warning"), "{to}: {text}");
        assert_eq!(sent.recv().await.unwrap().to, user(to));
    }

    // A colony that cannot be read holds nothing back.
    let other_home = super::home("unserved-unread");
    let (mcp, mut sent) = start(&other_home).await;
    mcp.with_lookup(Arc::new(|_| Box::pin(async { Err(anyhow::anyhow!("no colony")) })));
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-x", "text": "hi"})).await;
    assert!(ok && !text.contains("Warning"), "{text}");
    assert_eq!(sent.recv().await.unwrap().to, user("dev-x"));
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "dev-x", "text": "?", "timeout": "1s"})).await;
    assert!(!ok && text.starts_with("no reply from dev-x within 1s"), "{text}");
    assert_eq!(sent.recv().await.unwrap().to, user("dev-x"));
}
