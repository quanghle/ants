//! Tests of `ants ask` on a thread's messages: replies to one, and forwards of one or of a thread's TL;DR.

use super::*;

#[tokio::test]
async fn ask_reply_to_answers_the_message_s_author_on_its_thread_quoting_it() {
    use crate::cmd::send::{AskArgs, PostArgs, ask, post};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let built = asked.reply(dev_a.clone(), "built,\n  all green", ants_proto::Role::Human).unwrap();
    for e in [&asked, &built] {
        js.publish(e.subject(), json(e).into()).await.unwrap().await.unwrap();
    }

    let send = |reply_to: Uuid, thread: Option<Pick>, words: &[&str]| {
        let words = words.iter().map(|w| (*w).to_owned()).collect();
        Box::pin(ask(&nats, AskArgs { thread, reply_to: Some(MessagePick::Id(reply_to)), words, ..Default::default() }))
    };
    send(built.id, None, &["ship", "it"]).await.unwrap();
    let elsewhere = send(built.id, Some(Pick::Id(ThreadId::generate())), &["ship"]).await.unwrap_err().to_string();
    assert!(elsewhere.contains(&format!("is on thread {}, not ", asked.thread_id)), "{elsewhere}");
    let args = PostArgs {
        channel: "ops".parse().unwrap(),
        thread: None,
        reply_to: Some(MessagePick::Id(built.id)),
        queue: false,
        replies: Default::default(),
        input: Default::default(),
        attach: Default::default(),
        words: vec!["deploy?".to_owned()],
    };
    Box::pin(post(&nats, args)).await.unwrap();

    let shown = history(&js, &asked.thread_id, None, Activity::None, None).await.unwrap();
    let replied: Vec<_> = shown[2..].iter().map(|e| (e.to.clone(), e.channel.clone(), e.text.as_str())).collect();
    assert_eq!(replied, [(Some(dev_a.clone()), None, "ship it"), (None, Some("ops".parse().unwrap()), "deploy?")]);
    for e in &shown[2..] {
        assert_eq!(e.quote, Some(ants_proto::Quote::new(dev_a.clone(), "built, all green")), "{e:?}");
    }
}

#[tokio::test]
async fn ask_forward_hands_a_message_and_its_files_to_another_ant_on_a_thread_linked_to_its_own() {
    use crate::cmd::send::{AskArgs, ask};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "build it");
    let mut built = asked.reply(dev_a.clone(), "built, all green", ants_proto::Role::Human).unwrap();
    let log = ants_daemon::files::reference("build.log", "text/plain", b"ok").unwrap();
    ants_daemon::files::put(&js, &built.attachment_handle(log.id), &log, b"ok").await.unwrap();
    built.attachments = vec![log];
    for e in [&asked, &built] {
        js.publish(e.subject(), json(e).into()).await.unwrap().await.unwrap();
    }

    let words = ["@dev-b", "ship", "it"].map(str::to_owned).to_vec();
    Box::pin(ask(&nats, AskArgs { forward: Some(MessagePick::Id(built.id)), words, ..Default::default() }))
        .await
        .unwrap();

    let sent = recent_messages(&js, &alice).await.unwrap().into_iter().find(|e| e.to.as_ref() == Some(&dev_b));
    let sent = sent.expect("a message to dev-b");
    assert_ne!(sent.thread_id, asked.thread_id, "on a new thread");
    assert_eq!(sent.origin.as_ref(), Some(&asked.thread_id), "linked to the source thread");
    assert!(sent.text.starts_with("ship it\n\nForwarded from dev-a on thread "), "{}", sent.text);
    assert!(sent.text.ends_with("> built, all green\n> 📎 build.log (attached)"), "{}", sent.text);
    let [file] = sent.attachments.as_slice() else { panic!("{:?}", sent.attachments) };
    assert!(file.handle.readable_by(&dev_b), "stored again for dev-b");
    let (_, bytes) =
        crate::messages::attachments::fetch(&js, &file.handle, Some(&file.attachment), None).await.unwrap();
    assert_eq!(bytes, b"ok");
}

#[tokio::test]
async fn ask_forward_thread_hands_its_kept_tldr_to_another_ant_and_refuses_without_one() {
    use crate::cmd::send::{AskArgs, ask};

    let Some(server) = crate::test_support::Server::start() else { return };
    let (alice, dev_a, dev_b) = (p("alice"), p("dev-a"), p("dev-b"));
    let creds = server.creds_as(&alice, ants_proto::Role::Human);
    let nats = crate::connection::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    ants_daemon::bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let asked = Envelope::new(alice.clone(), dev_a.clone(), "fix the flaky test");
    js.publish(asked.subject(), json(&asked).into()).await.unwrap().await.unwrap();

    let send = || {
        let forward_thread = Some(Pick::Id(asked.thread_id.clone()));
        let words = ["@dev-b", "follow", "up"].map(str::to_owned).to_vec();
        Box::pin(ask(&nats, AskArgs { forward_thread, words, ..Default::default() }))
    };
    let refused = send().await.unwrap_err().to_string();
    assert!(refused.starts_with(&format!("no TL;DR of thread {} is kept", asked.thread_id)), "{refused}");

    let tldr = ants_proto::ThreadSummary {
        thread_id: asked.thread_id.clone(),
        text: "dev-a fixed the flaky test".to_owned(),
        by: p("messenger"),
        asker: alice.clone(),
        at: chrono::Utc::now(),
        upto: 1,
        tokens: 40,
    };
    crate::threads::thread_meta::open(&nats, &js).await.unwrap().put_summary(&tldr).await.unwrap();
    send().await.unwrap();

    let sent = recent_messages(&js, &alice).await.unwrap().into_iter().find(|e| e.to.as_ref() == Some(&dev_b));
    let sent = sent.expect("a message to dev-b");
    assert_ne!(sent.thread_id, asked.thread_id, "on a new thread");
    assert_eq!(sent.origin.as_ref(), Some(&asked.thread_id), "linked to the source thread");
    let head = format!("follow up\n\nForwarded thread {}, its TL;DR by messenger, ", asked.thread_id);
    assert!(sent.text.starts_with(&head), "{}", sent.text);
    assert!(sent.text.ends_with("UTC:\n> dev-a fixed the flaky test"), "{}", sent.text);
}
