use std::time::Duration;

use ants_daemon::bus;
use ants_proto::{Envelope, StatusUpdate, Uuid};

use super::*;
use crate::test_support::Server;
use crate::test_support::live::{request, user};
use crate::thread::Kind;

#[test]
fn a_verdict_answers_only_a_pending_request_offering_it() {
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
    let id = tools::qualified(&push);
    assert_eq!(refusal(&push, &tools::State::Pending, Verdict::AllowOnce), None);
    assert_eq!(refusal(&push, &tools::State::Pending, Verdict::Reject), None, "a reject is always offered");
    assert_eq!(
        refusal(&push, &tools::State::Pending, Verdict::AllowAlways),
        Some(format!("allow-always is not offered for request {id}"))
    );
    assert_eq!(
        refusal(&push, &tools::State::Expired, Verdict::AllowOnce),
        Some(format!("request {id} is answered already"))
    );
}

/// The next event of `feed` that `pick` takes, the others skipped.
async fn until<T>(feed: &mut Feed, mut pick: impl FnMut(Event) -> Option<T>) -> T {
    let wait = async {
        loop {
            if let Some(picked) = pick(feed.next().await) {
                return picked;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), wait).await.expect("the feed told nothing wanted")
}

/// Sends `text` from dev-a to the human on `thread`.
async fn tell(js: &jetstream::Context, thread: &ThreadId, text: &str) {
    let envelope = Envelope { thread_id: thread.clone(), ..Envelope::new(user("dev-a"), UserId::human(), text) };
    bus::publish_json(js, subject::inbox(&envelope.to, &envelope.from), &envelope, None).await.unwrap();
}

/// The texts of the messages on `thread` among `entries`.
fn texts<'a>(entries: &'a [Entry], thread: &ThreadId) -> Vec<&'a str> {
    entries.iter().filter(|e| e.thread_id == *thread).map(|e| e.text.as_str()).collect()
}

#[tokio::test]
async fn the_feed_reads_follows_and_answers_what_the_human_sees() {
    let Some(server) = Server::start() else { return };
    let (client, js) = server.connect(&UserId::human()).await;
    bus::ensure_stream(&js).await.unwrap();
    let (ant, ant_js) = server.connect(&user("dev-a")).await;
    let thread = ThreadId::generate();
    tell(&ant_js, &thread, "first").await;
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
    bus::publish_json(&ant_js, push.subject(), &push, None).await.unwrap();

    let mut feed = Feed::new(client, js.clone(), Duration::from_hours(1)).await.unwrap().with_timelines();
    assert!(feed.connected());
    assert!(matches!(feed.next().await, Event::Following), "the tool requests are followed first");
    // The stream read and the pending requests come in either order.
    let (mut stored, mut pending) = (None, None);
    until(&mut feed, |e| {
        match e {
            Event::Stored(read) => stored = stored.take().or(Some(read.unwrap())),
            Event::Approval(update) => pending = Some(update),
            _ => {}
        }
        (stored.is_some() && pending.is_some()).then_some(())
    })
    .await;
    assert_eq!(texts(&stored.unwrap(), &thread), ["first"]);
    assert_eq!(pending, Some(Update::Pending(push.clone())));

    // What is heard after is read from where the last read ended, and progress is told as heard.
    tell(&ant_js, &thread, "second").await;
    let stored = until(&mut feed, |e| match e {
        Event::Stored(read) => Some(read.unwrap()).filter(|entries| !entries.is_empty()),
        _ => None,
    })
    .await;
    assert_eq!(texts(&stored, &thread), ["second"]);
    let working =
        StatusEvent::new(user("dev-a"), thread.clone(), StatusUpdate::State { state: ants_proto::State::Working });
    let payload = serde_json::to_vec(&working).unwrap();
    ant.publish(subject::status(&working.ant, &thread), payload.into()).await.unwrap();
    ant.flush().await.unwrap();
    let heard = until(&mut feed, |e| match e {
        Event::Status(event) => Some(event),
        _ => None,
    })
    .await;
    assert_eq!(heard, working);

    // A tool call stored on the ant's timeline is read after the first read, and with the thread's history.
    let call = ants_proto::TimelineCall {
        id: "c1".to_owned(),
        tool_kind: Some("execute".to_owned()),
        title: "cargo test".to_owned(),
        status: Some("completed".to_owned()),
        started: Utc::now(),
        duration_ms: Some(3100),
        paths: Vec::new(),
    };
    let record = ants_proto::Timeline {
        ant: user("dev-a"),
        thread_id: thread.clone(),
        message: Uuid::new_v4(),
        entries: vec![ants_proto::TimelineEntry::Tool(call)],
        omitted: 0,
        at: Utc::now(),
    };
    bus::publish_json(&ant_js, subject::timeline(&thread, &record.ant), &record, None).await.unwrap();
    let tools = until(&mut feed, |e| match e {
        Event::Stored(read) => Some(read.unwrap()).filter(|entries| !entries.is_empty()),
        _ => None,
    })
    .await;
    assert_eq!(tools.iter().map(|e| (e.kind, e.text.as_str())).collect::<Vec<_>>(), [(Kind::Tool, "cargo test")]);
    // The first read brings the ends with no reply only.
    let why = ants_proto::NoReplyWhy::Completed;
    let end = ants_proto::TimelineEntry::NoReply { why, reason: None, text: "noted".to_owned() };
    let ended = ants_proto::Timeline { entries: vec![end], ..record.clone() };
    bus::publish_json(&ant_js, subject::timeline(&thread, &record.ant), &ended, None).await.unwrap();
    let (_, first) = tail(js.clone(), Parser::default(), None, Duration::from_hours(1), true).await;
    let timeline: Vec<_> = first.unwrap().0.into_iter().filter(|e| e.kind.on_timeline()).collect();
    assert_eq!(timeline.iter().map(|e| (e.kind, e.text.as_str())).collect::<Vec<_>>(), [(Kind::NoReply, "noted")]);

    feed.load_history(thread.clone(), true);
    let (loaded, read) = until(&mut feed, |e| match e {
        Event::History(thread, read) => Some((thread, read.unwrap())),
        _ => None,
    })
    .await;
    assert_eq!((&loaded, texts(&read, &thread)), (&thread, vec!["first", "second", "cargo test", "noted"]));

    feed.decide(push.clone(), Verdict::AllowOnce, UserId::human());
    let (mut decided, mut resolved) = (None, None);
    until(&mut feed, |e| {
        match e {
            Event::Decided(outcome) => decided = Some(outcome.unwrap()),
            Event::Approval(Update::Resolved(request, state)) => resolved = Some((request, state)),
            _ => {}
        }
        (decided.is_some() && resolved.is_some()).then_some(())
    })
    .await;
    let decided = decided.unwrap();
    assert!(decided.starts_with(&format!("answered request {}: allow-once", tools::qualified(&push))), "{decided}");
    let (request, state) = resolved.unwrap();
    assert_eq!(request, push);
    assert!(matches!(state, tools::State::Decided(d) if d.verdict == Verdict::AllowOnce));

    // A refresh follows the tool requests again, telling the pending ones anew.
    feed.refresh();
    assert!(matches!(feed.next().await, Event::Following));
}

#[tokio::test]
async fn the_feed_opens_a_sealed_message_from_a_device_added_since_its_keyring_was_loaded() {
    use ants_proto::Role;

    let Some(server) = Server::start() else { return };
    let (alice, bob) = (user("alice"), user("bob"));
    let bob_creds = server.creds_as(&bob, Role::Human);
    let bob_nats = crate::connection::Nats::new(&server.url, Some(&bob_creds), None).unwrap();
    let (_, bob_js) = bob_nats.connect("test").await.unwrap();
    let keyring = crate::sealed::Keyring::load(&bob_nats, &bob_js).await.map(Arc::new);
    assert!(keyring.is_some());

    // A device of alice's registered after bob's keyring was loaded.
    let alice_creds = server.dir().join("alice-laptop.creds");
    crate::auth::issue(&server.dir().join("auth"), &alice, Role::Human, None, &alice_creds).unwrap();
    let alice_nats = crate::connection::Nats::new(&server.url, Some(&alice_creds), None).unwrap();
    alice_nats.connect("test").await.unwrap();
    let words = vec!["@bob".to_owned(), "lunch at noon?".to_owned()];
    let args = crate::cmd::send::AskArgs { words, ..Default::default() };
    Box::pin(crate::cmd::send::ask(&alice_nats, args)).await.unwrap();

    let (_, read) = tail(bob_js, Parser::opening(keyring), None, Duration::from_hours(1), false).await;
    let entries = read.unwrap().0;
    let [entry] = entries.as_slice() else { panic!("not one message: {entries:?}") };
    assert_eq!((entry.text.as_str(), entry.sealed), ("lunch at noon?", Some(crate::sealed::State::Opened)));
}
