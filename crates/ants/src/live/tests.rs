//! Tests of the live view: verdicts and allow-all on tool requests, the feed reading, following and answering, and
//! sealed messages from a device added since.

use std::time::Duration;

use ants_client::entry::Kind;
use ants_daemon::bus;
use ants_proto::{Envelope, StatusUpdate, Uuid};

use super::*;
use crate::test_support::Server;
use crate::test_support::live::{request, user};

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

#[test]
fn an_allow_all_says_in_one_line_how_many_were_allowed_answered_already_or_failed() {
    let push = request("dev-a", "git push", vec![Verdict::AllowOnce]);
    let line = |tally| allowed_line(Some(&push), tally);
    assert_eq!(line((3, 0, Vec::new())).unwrap(), "allowed once 3 of 3 execute requests of dev-a");
    assert_eq!(line((1, 2, Vec::new())).unwrap(), "allowed once 1 of 3 execute requests of dev-a; 2 answered already");
    let failed = vec!["dev-a/1: down".to_owned()];
    assert_eq!(
        line((1, 0, failed.clone())).unwrap(),
        "allowed once 1 of 2 execute requests of dev-a; 1 failed: dev-a/1: down"
    );
    let none = line((0, 1, failed)).unwrap_err().to_string();
    assert_eq!(none, "allowed once 0 of 2 execute requests of dev-a; 1 answered already; 1 failed: dev-a/1: down");
}

#[tokio::test]
async fn an_allow_all_allows_once_each_request_in_turn_counting_those_answered_elsewhere() {
    let Some(server) = Server::start() else { return };
    let (client, js) = server.connect(&ants_test_support::human()).await;
    bus::ensure_stream(&js).await.unwrap();
    let (_ant, ant_js) = server.connect(&user("dev-a")).await;
    let thread = ThreadId::generate();
    let on = |title: &str| ApprovalRequest {
        thread_id: thread.clone(),
        ..request("dev-a", title, vec![Verdict::AllowOnce])
    };
    let (push, test, build) = (on("git push"), on("cargo test"), on("cargo build"));
    for r in [&push, &test, &build] {
        bus::publish_json(&ant_js, r.subject(), r, None).await.unwrap();
    }
    let mut feed = Feed::new(client, js.clone(), Duration::from_hours(1)).await.unwrap();
    feed.decide(test.clone(), Verdict::Reject, ants_test_support::human());
    let rejected = until(&mut feed, |e| match e {
        Event::Decided(outcome) => Some(outcome),
        _ => None,
    })
    .await;
    assert!(rejected.is_ok(), "{rejected:?}");

    feed.allow_all(vec![push.clone(), test.clone(), build.clone()], ants_test_support::human());
    let line = until(&mut feed, |e| match e {
        Event::Decided(outcome) => Some(outcome),
        _ => None,
    })
    .await
    .unwrap();
    assert_eq!(line, "allowed once 2 of 3 execute requests of dev-a; 1 answered already");
    let stream = js.get_stream(STREAM).await.unwrap();
    for (r, verdict) in [(&push, Verdict::AllowOnce), (&test, Verdict::Reject), (&build, Verdict::AllowOnce)] {
        let (seq, r) = tools::find(&stream, &tools::qualified(r)).await.unwrap();
        let state = tools::state(&js, &stream, seq, &r, Utc::now()).await.unwrap();
        assert!(matches!(state, tools::State::Decided(ref d) if d.verdict == verdict), "{state:?}");
    }
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
    let envelope =
        Envelope { thread_id: thread.clone(), ..Envelope::new(user("dev-a"), ants_test_support::human(), text) };
    bus::publish_json(js, subject::inbox(&envelope.to, &envelope.from), &envelope, None).await.unwrap();
}

/// The texts of the messages on `thread` among `entries`.
fn texts<'a>(entries: &'a [Entry], thread: &ThreadId) -> Vec<&'a str> {
    entries.iter().filter(|e| e.thread_id == *thread).map(|e| e.text.as_str()).collect()
}

#[tokio::test]
async fn the_feed_reads_follows_and_answers_what_the_human_sees() {
    let Some(server) = Server::start() else { return };
    let (client, js) = server.connect(&ants_test_support::human()).await;
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

    feed.decide(push.clone(), Verdict::AllowOnce, ants_test_support::human());
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
    let bob_nats = crate::connection::Nats::new(&server.url, Some(&bob_creds), None, None).unwrap();
    let (_, bob_js) = bob_nats.connect("test").await.unwrap();
    let keyring = crate::people::sealed::Keyring::load(&bob_nats, &bob_js).await.map(Arc::new);
    assert!(keyring.is_some());

    // A device of alice's registered after bob's keyring was loaded.
    let alice_creds = server.dir().join("alice-laptop.creds");
    crate::test_support::issue(&server.dir().join("auth"), &alice, Role::Human, None, &alice_creds).unwrap();
    let alice_nats = crate::connection::Nats::new(&server.url, Some(&alice_creds), None, None).unwrap();
    alice_nats.connect("test").await.unwrap();
    let words = vec!["@bob".to_owned(), "lunch at noon?".to_owned()];
    let args = crate::cmd::send::AskArgs { words, ..Default::default() };
    Box::pin(crate::cmd::send::ask(&alice_nats, args)).await.unwrap();

    let (_, read) = tail(bob_js, Parser::opening(keyring), None, Duration::from_hours(1), false).await;
    let entries = read.unwrap().0;
    let [entry] = entries.as_slice() else { panic!("not one message: {entries:?}") };
    assert_eq!((entry.text.as_str(), entry.sealed), ("lunch at noon?", Some(ants_client::sealed::State::Opened)));
}
