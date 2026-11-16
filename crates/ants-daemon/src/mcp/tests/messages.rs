//! Sending, reacting, titling and asking the human from a turn, and the budget messages spend.

use super::*;

#[tokio::test]
async fn send_message_follows_on_from_the_current_message() {
    let (mcp, mut sent) = start(&home("send")).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let tools: Vec<_> = client.list_all_tools().await.unwrap().into_iter().map(|t| t.name).collect();
    assert_eq!(tools.len(), 17, "{tools:?}");

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
async fn react_takes_the_current_message_or_a_named_one_and_removes() {
    let (mcp, _sent) = start(&home("react")).await;
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
    let (mcp, _sent) = start(&home("list-reactions")).await;
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
    let (mcp, _sent) = start(&home("title")).await;
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
async fn ask_human_asks_the_thread_s_owner_without_spending_a_turn() {
    let humans = Registry::with(UserId::human(), &["alice"]);
    let (mcp, mut sent) = start_with(&home("ask-human"), Vec::new(), Arc::default(), humans).await;
    let mut current = Envelope::new(user("dev-b"), user("dev-a"), "q").with_owner(Some(user("alice")));
    current.turn_budget = 0;
    let client = {
        let (url, token) = http_server(&mcp, &current.thread_id);
        http_client(&url, &token).await
    };
    let ask = |args: serde_json::Value| call(&client, "ask_human", args);
    let (ok, text) = ask(serde_json::json!({"question": "Ship?", "options": ["yes", "no"]})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let _turn = mcp.begin(&current);
    let (ok, text) = ask(serde_json::json!({"question": "Ship?", "options": ["yes"]})).await;
    assert!(!ok && text.contains("invalid question"), "{text}");
    let (ok, text) =
        ask(serde_json::json!({"question": " Ship? ", "options": ["yes", "not yet"], "free_text": false})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert!(text.contains(&out.id.to_string()) && text.contains("alice"), "{text}");
    assert_eq!((&out.to, &out.thread_id, out.turn_budget), (&user("alice"), &current.thread_id, 0));
    let question = out.asks().unwrap();
    assert_eq!((question.text.as_str(), question.free_text), ("Ship?", false));
    assert_eq!(out.text, question.render());
    assert_eq!(out.in_reply_to, None);

    let plain = Envelope::new(user("dev-b"), user("dev-a"), "q");
    let _turn = mcp.begin(&plain);
    let client = {
        let (url, token) = http_server(&mcp, &plain.thread_id);
        http_client(&url, &token).await
    };
    let (ok, text) =
        call(&client, "ask_human", serde_json::json!({"question": "Ship?", "options": ["yes", "no"]})).await;
    assert!(ok, "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!(out.to, UserId::human(), "no owner: the default human");
    assert!(out.asks().unwrap().free_text, "typed answers by default");
}

#[tokio::test]
async fn messages_between_ants_spend_the_budget() {
    let (mcp, mut sent) = start(&home("budget")).await;
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
    let (mcp, mut sent) = start_with(&home("notes"), Vec::new(), notes.clone(), humans).await;
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
