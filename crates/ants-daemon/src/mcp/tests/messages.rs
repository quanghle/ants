//! Sending, reacting, titling and asking the human from a turn, and the budget messages spend.

use super::*;

#[tokio::test]
async fn send_message_follows_on_from_the_current_message() {
    let home = home("send");
    let (mcp, mut sent) = start(&home).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let tools: Vec<_> = client.list_all_tools().await.unwrap().into_iter().map(|t| t.name).collect();
    assert_eq!(tools.len(), 21, "{tools:?}");

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "hi"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "@dev-b", "text": "hi"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.from, &out.to, &out.thread_id), (&user("dev-a"), &user("dev-b"), &current.thread_id));
    assert_eq!((out.turn_budget, out.in_reply_to), (current.turn_budget, None), "free after the human's message");

    let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "x", "thread": "new"})).await;
    assert!(ok);
    assert_ne!(sent.recv().await.unwrap().thread_id, current.thread_id);

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-a", "text": "me"})).await;
    assert!(!ok && text.contains("this ant"), "{text}");
    drop(turn);
    let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "late"})).await;
    assert!(!ok, "nothing goes out once the message is handled");
}

#[tokio::test]
async fn handoff_sends_to_the_ant_itself_on_another_thread() {
    let home = home("handoff");
    let (mcp, mut sent) = start(&home).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "go").with_owner(Some(UserId::human()));
    current.turn_budget = 4;
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let _turn = mcp.begin(&current);

    let (ok, text) = call(&client, "handoff", serde_json::json!({"text": "split this out"})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.from, &out.to), (&user("dev-a"), &user("dev-a")));
    assert_ne!(out.thread_id, current.thread_id);
    assert_eq!((out.turn_budget, out.owner.as_ref()), (3, Some(&UserId::human())), "one turn, the owner kept");

    let other = ants_proto::ThreadId::generate();
    let (ok, _) = call(&client, "handoff", serde_json::json!({"text": "x", "thread": other.to_string()})).await;
    assert!(ok);
    assert_eq!(sent.recv().await.unwrap().thread_id, other);

    let here = current.thread_id.to_string();
    let (ok, text) = call(&client, "handoff", serde_json::json!({"text": "x", "thread": here})).await;
    assert!(!ok && text.contains("current one"), "{text}");
}

#[tokio::test]
async fn react_takes_the_current_message_or_a_named_one_and_removes() {
    let home = home("react");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(!ok && text.contains("not available"), "{text}");

    let stored: Arc<StdMutex<std::collections::BTreeSet<(Uuid, String)>>> = Arc::default();
    let kept = stored.clone();
    let (unread, silent) = (Uuid::now_v7(), Uuid::now_v7());
    mcp.with_reaction_asker(Arc::new(move |request: ants_proto::ReactionsRequest| {
        assert_eq!(request.ant.as_str(), "dev-a");
        let change = request.change.expect("a change");
        let mut stored = kept.locked();
        let entry = (request.message, change.reaction.as_str().to_owned());
        let outcome = if request.message == unread {
            Some(ants_proto::ReactionsOutcome::Unknown)
        } else if request.message == silent {
            None
        } else {
            let changed = if change.remove { stored.remove(&entry) } else { stored.insert(entry) };
            Some(ants_proto::ReactionsOutcome::Applied { changed })
        };
        Box::pin(async move { Ok(outcome) })
    }));
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(!ok && text.contains("name the message by message_id"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(ok && text == format!("Reacted eyes to message {}.", current.id), "{text}");
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "eyes"})).await;
    assert!(ok && text.starts_with("Already reacted eyes"), "{text}");
    let other = Uuid::now_v7();
    let args = serde_json::json!({"reaction": "check", "message_id": other.to_string()});
    assert!(call(&client, "react", args).await.0);
    let (ok, text) = call(&client, "react", serde_json::json!({"reaction": "Eyes!"})).await;
    assert!(!ok, "{text}");
    let expected = std::collections::BTreeSet::from([(current.id, "eyes".to_owned()), (other, "check".to_owned())]);
    assert_eq!(*stored.locked(), expected);

    let remove = serde_json::json!({"reaction": "eyes", "remove": true});
    let (ok, text) = call(&client, "react", remove.clone()).await;
    assert!(ok && text.starts_with("Removed the reaction eyes"), "{text}");
    let (ok, text) = call(&client, "react", remove).await;
    assert!(ok && text.starts_with("No reaction eyes"), "{text}");
    assert_eq!(*stored.locked(), std::collections::BTreeSet::from([(other, "check".to_owned())]));

    let named = |id: Uuid| serde_json::json!({"reaction": "eyes", "message_id": id.to_string()});
    let (ok, text) = call(&client, "react", named(unread)).await;
    assert!(!ok && text.contains("none this ant reads") && text.contains("nothing changed"), "{text}");
    let (ok, text) = call(&client, "react", named(silent)).await;
    assert!(!ok && text.contains("ants signer"), "{text}");
}

#[tokio::test]
async fn list_reactions_shows_those_to_the_current_message_or_a_named_one() {
    let home = home("list-reactions");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({})).await;
    assert!(!ok && text.contains("not available"), "{text}");

    let (unread, silent) = (Uuid::now_v7(), Uuid::now_v7());
    let quiet = current.id;
    mcp.with_reaction_asker(Arc::new(move |request: ants_proto::ReactionsRequest| {
        assert!(request.change.is_none(), "only listing");
        let message = request.message;
        let outcome = if message == unread {
            Some(ants_proto::ReactionsOutcome::Unknown)
        } else if message == silent {
            None
        } else if message == quiet {
            Some(ants_proto::ReactionsOutcome::Listed { reactions: Vec::new() })
        } else {
            let reacted = |reaction: &str, by: &[&str]| {
                ants_proto::Reacted::new(reaction.parse().unwrap(), by.iter().map(|b| b.parse().unwrap()).collect())
            };
            let reactions = vec![reacted("check", &["dev-b"]), reacted("eyes", &["alice", "dev-b"])];
            Some(ants_proto::ReactionsOutcome::Listed { reactions })
        };
        Box::pin(async move { Ok(outcome) })
    }));
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({})).await;
    assert!(!ok && text.contains("name the message by message_id"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({})).await;
    assert!(ok && text == format!("No reactions to message {quiet}."), "{text}");
    let named = |id: Uuid| serde_json::json!({"message_id": id.to_string()});
    let other = Uuid::now_v7();
    let (ok, text) = call(&client, "list_reactions", named(other)).await;
    assert!(ok && text == format!("Reactions to message {other}: check by dev-b; eyes by alice, dev-b."), "{text}");
    let (ok, text) = call(&client, "list_reactions", named(unread)).await;
    assert!(ok && text.contains("none this ant reads"), "{text}");
    let (ok, text) = call(&client, "list_reactions", named(silent)).await;
    assert!(!ok && text.contains("ants signer") && text.contains("ants webhook serve"), "{text}");
    let (ok, text) = call(&client, "list_reactions", serde_json::json!({"message_id": "nope"})).await;
    assert!(!ok && text.contains("message_id"), "{text}");
}

#[tokio::test]
async fn a_suggested_title_waits_for_the_turn_s_reply_and_goes_with_the_turn() {
    let home = home("title");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let thread = &current.thread_id;
    let (url, token) = http_server(&mcp, thread);
    let client = http_client(&url, &token).await;
    let suggest = |title: &str| call(&client, "suggest_thread_title", serde_json::json!({ "title": title }));
    let (ok, text) = suggest("Fix the build").await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    for bad in ["  ", "two\nlines", &"x".repeat(MAX_TITLE + 1)] {
        let (ok, text) = suggest(bad).await;
        assert!(!ok && text.contains("invalid thread title"), "{text}");
    }
    assert!(suggest("First idea").await.0);
    let (ok, text) = suggest("  Fix the build ").await;
    assert!(ok && text.contains("\"Fix the build\""), "{text}");
    assert_eq!(mcp.take_title(thread).as_deref(), Some("Fix the build"), "the latest, trimmed");
    assert_eq!(mcp.take_title(thread), None, "taken once");
    assert!(suggest("Unsent").await.0);
    drop(turn);
    let _turn = mcp.begin(&current);
    assert_eq!(mcp.take_title(thread), None, "a suggestion ends with its turn");
}

#[tokio::test]
async fn complete_ends_an_ant_s_message_turn_with_no_reply_and_a_check_reaction() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let home = home("complete");
    let (mcp, mut sent) = start_with(&home, Vec::new(), Arc::default(), humans).await;
    let reacted: Arc<StdMutex<Vec<(Uuid, String)>>> = Arc::default();
    let kept = reacted.clone();
    mcp.with_reaction_asker(Arc::new(move |request: ants_proto::ReactionsRequest| {
        let change = request.change.expect("a change");
        kept.locked().push((request.message, change.reaction.as_str().to_owned()));
        Box::pin(async move { Ok(Some(ants_proto::ReactionsOutcome::Applied { changed: true })) })
    }));
    let current = Envelope::new(user("dev-b"), user("dev-a"), "thanks, done");
    let thread = &current.thread_id;
    let client = {
        let (url, token) = http_server(&mcp, thread);
        http_client(&url, &token).await
    };
    let complete = |args: serde_json::Value| call(&client, "complete", args);
    let (ok, text) = complete(serde_json::json!({})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let turn = mcp.begin(&current);
    for bad in ["two\nlines", &"x".repeat(501)] {
        let (ok, text) = complete(serde_json::json!({ "reason": bad })).await;
        assert!(!ok && text.contains("invalid reason"), "{text}");
        assert_eq!(mcp.take_completed(thread), None, "nothing recorded");
    }
    assert!(reacted.locked().is_empty(), "no reaction for a refused call");
    let (ok, text) = complete(serde_json::json!({})).await;
    assert!(ok && text.contains("no reply") && !text.contains("not made"), "{text}");
    assert!(complete(serde_json::json!({ "reason": "first" })).await.0);
    assert!(complete(serde_json::json!({ "reason": format!("  {}  ", "y".repeat(500)) })).await.0);
    let expected = Completed { reason: Some("y".repeat(500)) };
    assert_eq!(mcp.take_completed(thread), Some(expected), "the latest, trimmed");
    assert_eq!(mcp.take_completed(thread), None, "taken once");
    assert!(complete(serde_json::json!({ "reason": "   " })).await.0);
    assert_eq!(mcp.take_completed(thread), Some(Completed { reason: None }), "a blank reason is none");
    assert_eq!(*reacted.locked(), vec![(current.id, "check".to_owned()); 4]);
    assert!(complete(serde_json::json!({ "reason": "unsent" })).await.0);
    drop(turn);
    let turn = mcp.begin(&current);
    assert_eq!(mcp.take_completed(thread), None, "a completion ends with its turn");

    drop(turn);

    let other_home = super::home("complete-failing");
    let (failing, _) = start(&other_home).await;
    failing.with_reaction_asker(Arc::new(|_| Box::pin(async { Err(anyhow::anyhow!("hub unreachable")) })));
    let client = {
        let (url, token) = http_server(&failing, thread);
        http_client(&url, &token).await
    };
    let _turn = failing.begin(&current);
    let (ok, text) = call(&client, "complete", serde_json::json!({ "reason": "acknowledged" })).await;
    assert!(ok && text.contains("check reaction to the message was not made: hub unreachable"), "{text}");
    assert_eq!(failing.take_completed(thread), Some(Completed { reason: Some("acknowledged".to_owned()) }));
    assert!(sent.try_recv().is_err(), "nothing is sent");
}

#[tokio::test]
async fn complete_is_refused_for_a_message_always_answered() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let home = home("complete-refused");
    let (mcp, mut sent) = start_with(&home, Vec::new(), Arc::default(), humans).await;
    let from_ant = || Envelope::new(user("dev-b"), user("dev-a"), "x");
    let mut for_human = from_ant();
    for_human.on_behalf_of = Some(user("alice"));
    let mut webhook = Envelope::new(user("github"), user("dev-a"), "push");
    webhook.webhook = true;
    let mut escalation = from_ant();
    let stuck = ants_proto::Escalation::new(user("dev-b"), escalation.thread_id.clone(), EscalationReason::TurnBudget);
    escalation.escalation = Some(Box::new(stuck));
    let cases = [
        (Envelope::new(user("alice"), user("dev-a"), "x"), "a human's"),
        (for_human, "a human's"),
        (webhook, "a webhook's"),
        (escalation, "an escalation"),
    ];
    for (current, why) in cases {
        let client = {
            let (url, token) = http_server(&mcp, &current.thread_id);
            http_client(&url, &token).await
        };
        let _turn = mcp.begin(&current);
        let (ok, text) = call(&client, "complete", serde_json::json!({ "reason": "seen" })).await;
        assert!(!ok && text.contains(why) && text.contains("nothing recorded"), "{why}: {text}");
        assert_eq!(mcp.take_completed(&current.thread_id), None, "{why}");
    }

    let post = |from: &str| {
        let post = ChannelPost::new("general".parse().unwrap(), user(from), "@dev-a ok");
        crate::channel::as_envelope(&post, &user("dev-a"))
    };
    for (current, allowed) in [(post("alice"), false), (post("dev-b"), true)] {
        let client = {
            let (url, token) = http_server(&mcp, &current.thread_id);
            http_client(&url, &token).await
        };
        let _turn = mcp.begin(&current);
        let (ok, text) = call(&client, "complete", serde_json::json!({})).await;
        assert_eq!(ok, allowed, "{text}");
        assert_eq!(mcp.take_completed(&current.thread_id).is_some(), allowed);
    }
    assert!(sent.try_recv().is_err(), "nothing is sent");
}

#[tokio::test]
async fn ask_human_asks_the_thread_s_owner_without_spending_a_turn() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let home = home("ask-human");
    let (mcp, mut sent) = start_with(&home, Vec::new(), Arc::default(), humans).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q").with_owner(Some(user("alice")));
    current.turn_budget = 0;
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let ask = |args: serde_json::Value| call(&client, "ask_human", args);
    let one = |question: serde_json::Value| serde_json::json!({"questions": [question]});
    let (ok, text) = ask(one(serde_json::json!({"question": "Ship?", "options": ["yes", "no"]}))).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = ask(one(serde_json::json!({"question": "Ship?", "options": ["yes"]}))).await;
    assert!(!ok && text.contains("invalid question"), "{text}");
    let (ok, text) =
        ask(one(serde_json::json!({"question": " Ship? ", "options": ["yes", "not yet"], "free_text": false}))).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert!(text.contains(&out.id.to_string()) && text.contains("alice"), "{text}");
    assert_eq!((&out.to, &out.thread_id, out.turn_budget), (&user("alice"), &current.thread_id, 0));
    let question = &out.asks().unwrap().questions[0];
    assert_eq!((question.text.as_str(), question.free_text), ("Ship?", false));
    assert_eq!(out.text, out.asks().unwrap().render());
    assert_eq!(out.in_reply_to, None);

    let plain = Envelope::new(user("dev-b"), user("dev-a"), "q");
    let _turn = mcp.begin(&plain);
    let client = {
        let (url, token) = http_server(&mcp, &plain.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) =
        call(&client, "ask_human", one(serde_json::json!({"question": "Ship?", "options": ["yes", "no"]}))).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!(out.to, UserId::human(), "no owner: the default human");
    assert!(out.asks().unwrap().questions[0].free_text, "typed answers by default");
}

#[tokio::test]
async fn ask_human_asks_several_questions_in_one_message() {
    let home = home("ask-several");
    let (mcp, mut sent) = start(&home).await;
    let current = Envelope::new(user("dev-b"), user("dev-a"), "q");
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let regions = serde_json::json!({
        "question": "Which regions?",
        "header": "Regions",
        "options": [{"label": "eu", "description": "Frankfurt"}, "us"],
        "multi_select": true,
    });
    let ship = serde_json::json!({"question": "Ship it?", "options": ["yes", "no"], "free_text": false});
    let (ok, text) = call(&client, "ask_human", serde_json::json!({"questions": [regions, ship]})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    let asked = &out.asks().unwrap().questions;
    assert_eq!(asked.len(), 2);
    assert_eq!((asked[0].header.as_deref(), asked[0].multi_select, asked[0].free_text), (Some("Regions"), true, true));
    assert_eq!(asked[0].options[0].description.as_deref(), Some("Frankfurt"));
    assert_eq!((asked[1].multi_select, asked[1].free_text), (false, false));
    assert!(out.text.contains("Question 1 of 2, Regions: Which regions?") && out.text.contains("Question 2 of 2"));

    let five = serde_json::json!({"questions": vec![ship.clone(); 5]});
    let (ok, text) = call(&client, "ask_human", five).await;
    assert!(!ok && text.contains("invalid question"), "{text}");
    let long = serde_json::json!({"question": "Ship?", "header": "Thirteen char", "options": ["yes", "no"]});
    let (ok, text) = call(&client, "ask_human", serde_json::json!({"questions": [long]})).await;
    assert!(!ok && text.contains("header"), "{text}");
    assert!(sent.try_recv().is_err(), "nothing more is sent");
}

#[tokio::test]
async fn messages_between_ants_spend_the_budget() {
    let home = home("budget");
    let (mcp, mut sent) = start(&home).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q");
    current.turn_budget = 1;
    let _turn = mcp.begin(&current);
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "dev-c", "text": "?"})).await;
    assert!(!ok && text.contains("no turns left"), "{text}");
    let (ok, _) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "fyi"})).await;
    assert!(ok);
    assert_eq!(sent.recv().await.unwrap().turn_budget, 0);

    current.turn_budget = 0;
    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-c", "text": "more"})).await;
    assert!(!ok && text.contains("no turns left"), "{text}");
}

#[tokio::test]
async fn notes_posts_spend_the_budget_then_the_thread_goes_to_its_owner() {
    let notes = Arc::new(MemoryNotes::default());
    let humans = Registry::with(UserId::human(), &["alice"]);
    let home = home("notes");
    let (mcp, mut sent) = start_with(&home, Vec::new(), notes.clone(), humans).await;
    let mut current =
        Envelope::new(user("dev-b"), user("dev-a"), "plan the release with me").with_owner(Some(user("alice")));
    current.turn_budget = 5;
    let thread = current.thread_id.clone();
    let client = {
        let (url, token) = http_server(&mcp, &thread);
        http_client(&url, &token).await
    };
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "early"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = call(&client, "read_notes", serde_json::json!({})).await;
    assert!(ok && text.ends_with("has no notes yet."), "{text}");
    // dev-b posted already and left two turns, fewer than the message has.
    let theirs = NotesPost::spend(thread.clone(), user("dev-b"), "I take the docs", 3, None).unwrap();
    notes.0.locked().insert(thread.clone(), vec![theirs]);
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "I take the code"})).await;
    assert!(ok && text.contains("(1 turns left)"), "{text}");
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "ship friday?"})).await;
    assert!(ok && text.contains("; alice has the thread now"), "{text}");
    let notice = sent.try_recv().unwrap();
    assert_eq!((&notice.from, &notice.to, &notice.thread_id), (&user("dev-a"), &user("alice"), &thread));
    assert_eq!(notice.owner, Some(user("alice")));
    assert!(
        notice.text.contains("spent their turn budget") && notice.text.ends_with("a fresh budget."),
        "{}",
        notice.text
    );
    assert!(notice.text.contains("Last post from dev-a:\nship friday?"), "{}", notice.text);

    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "one more"})).await;
    assert!(!ok && text.contains("no turns left"), "{text}");
    assert!(sent.try_recv().is_err(), "the owner is told once");
    let (ok, text) = call(&client, "read_notes", serde_json::json!({"thread": thread.as_str()})).await;
    assert!(ok && text.starts_with(&format!("Notes of thread {thread}: 3 posts; 0 turns left.")), "{text}");
    assert!(text.contains("dev-b at ") && text.ends_with("ship friday?"), "{text}");

    // The human's post gives the notes a fresh budget, within the message's.
    notes.0.locked().get_mut(&thread).unwrap().push(NotesPost::new(thread.clone(), UserId::human(), "go"));
    let (ok, text) = call(&client, "post_notes", serde_json::json!({"text": "back to it"})).await;
    assert!(ok && text.contains("(4 turns left)"), "{text}");
}
