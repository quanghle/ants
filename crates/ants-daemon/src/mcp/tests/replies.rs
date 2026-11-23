//! `ask_ant` and the replies it waits for, live or from the inbox.

use super::*;

#[tokio::test]
async fn a_bad_timeout_is_refused_before_anything_is_sent() {
    let home = home("timeout");
    let (mcp, mut sent) = start(&home).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    for (timeout, why) in [("300", "without a unit"), ("0", "out of range"), ("31m", "at most 30m")] {
        for (tool, args) in [
            ("ask_ant", serde_json::json!({"to": "dev-b", "text": "?", "timeout": timeout})),
            ("route", serde_json::json!({"capability": "rust", "text": "?", "timeout": timeout})),
        ] {
            let (ok, text) = call(&client, tool, args).await;
            assert!(!ok && text.contains("timeout") && text.contains(why), "{tool} {timeout}: {text}");
        }
    }
    assert!(sent.try_recv().is_err(), "nothing sent");
}

#[tokio::test]
async fn ask_ant_gets_the_reply_while_the_inbox_consumer_holds_it() {
    let home = home("ask");
    let (mcp, mut sent) = start(&home).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let asking =
        tokio::spawn(async move { call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "6*7?"})).await });
    let ask = sent.recv().await.unwrap();
    assert_eq!((&ask.to, &ask.thread_id), (&user("dev-b"), &current.thread_id));

    // Someone else answering does not count.
    let mut forged = ask.reply(user("dev-c"), "13", Role::Ant).unwrap();
    forged.in_reply_to = Some(ask.id);
    mcp.hub.route(forged.clone()).await;
    assert_eq!(mcp.claim(&forged).await.unwrap(), Claim::No, "a forged reply is an ordinary message");

    let reply = ask.reply(user("dev-b"), "42", Role::Ant).unwrap();
    mcp.hub.route(reply.clone()).await;
    let (ok, text) = asking.await.unwrap();
    assert!(ok && text.ends_with("42"), "{text}");
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Taken, "the consumer's copy is acked, not prompted");
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::No, "the ask is settled");
}

#[tokio::test]
async fn the_consumer_can_deliver_the_reply_too() {
    let home = home("ask-js");
    let (mcp, mut sent) = start(&home).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let asking =
        tokio::spawn(async move { call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "?"})).await });
    let reply = sent.recv().await.unwrap().reply(user("dev-b"), "yes", Role::Ant).unwrap();
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Taken);
    assert!(asking.await.unwrap().1.ends_with("yes"));
    mcp.hub.route(reply).await;
}

#[tokio::test]
async fn a_late_reply_waits_in_check_inbox_across_restarts() {
    let dir = home("late");
    let (mcp, mut sent) = start(&dir).await;
    let current = from_human();
    let turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "?", "timeout": "1s"})).await;
    assert!(!ok && text.contains("check_inbox") && text.contains("prompted on this thread"), "{text}");
    let ask = sent.recv().await.unwrap();
    drop((turn, client, mcp));

    // The reply comes after a restart: it is still known as a reply, not a new message.
    let (mcp, _sent) = start(&dir).await;
    let reply = ask.reply(user("dev-b"), "late answer", Role::Ant).unwrap();
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Late);
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Late, "kept once, still to prompt");
    drop(mcp);

    let (mcp, _sent) = start(&dir).await;
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    assert_eq!(mcp.late_on(&current.thread_id).await, 1);
    assert_eq!(mcp.late_on(&ThreadId::generate()).await, 0, "only its own thread's");
    let (ok, text) = call(&client, "check_inbox", serde_json::json!({})).await;
    assert!(ok && text.contains("From dev-b") && text.contains("late answer"), "{text}");
    assert_eq!(mcp.late_on(&current.thread_id).await, 0, "taken");
    let (_, text) = call(&client, "check_inbox", serde_json::json!({"all_threads": true})).await;
    assert_eq!(text, "No messages.");
    drop((client, mcp));

    // Taken, its copy in the inbox is acked unprompted, after a restart too.
    let (mcp, _sent) = start(&dir).await;
    assert!(!mcp.late_turn(&reply).await.unwrap(), "check_inbox took it");
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Taken);
}

#[tokio::test]
async fn a_late_reply_no_turn_took_is_prompted_and_leaves_check_inbox() {
    let dir = home("late-prompt");
    let (mcp, mut sent) = start(&dir).await;
    let current = from_human();
    let turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, _) = call(&client, "ask_ant", serde_json::json!({"to": "dev-b", "text": "?", "timeout": "1s"})).await;
    assert!(!ok);
    let ask = sent.recv().await.unwrap();
    drop(turn);

    let reply = ask.reply(user("dev-b"), "late answer", Role::Ant).unwrap();
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Late);
    assert!(mcp.late_turn(&reply).await.unwrap(), "prompted as its turn comes");
    assert_eq!(mcp.late_on(&current.thread_id).await, 0, "no longer waiting for check_inbox");
    let _turn = mcp.begin(&current);
    let (_, text) = call(&client, "check_inbox", serde_json::json!({})).await;
    assert_eq!(text, "No messages.");
    drop((client, mcp));

    // A redelivery, the prompt cut off by a restart, is prompted again.
    let (mcp, _sent) = start(&dir).await;
    assert_eq!(mcp.claim(&reply).await.unwrap(), Claim::Late);
    assert!(mcp.late_turn(&reply).await.unwrap());
}
