//! Tests of catch-up summaries: a request's entries and their bounds, and a TL;DR asked, kept while current and its
//! thread left out.

use std::time::Duration;

use ants_proto::{Role, ThreadMarks};
use ants_test_support::user as p;

use super::*;
use crate::test_support::Server;
use crate::threads::thread::{Parser, Select, Threads};

#[test]
fn a_request_carries_the_newest_entries_within_16_kib_each_cut_to_1_kib() {
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let mut parser = Parser::default();
    let entries: Vec<Entry> = (0..40)
        .map(|i| {
            let e = Envelope::new(alice.clone(), dev_a.clone(), format!("{i:02}{}", "é".repeat(1000)));
            parser.entry(i + 1, Utc::now(), &e.subject(), &serde_json::to_vec(&e).unwrap()).unwrap()
        })
        .collect();
    let text = entries_text(&entries, false);
    assert!(text.len() <= ENTRIES_MAX, "{}", text.len());
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines.last().unwrap().contains("alice -> dev-a: 39é"), "the newest last");
    assert!(lines.len() < 40 && lines.len() >= 15, "only the newest fit: {}", lines.len());
    let cut = lines[0].split_once(": ").unwrap().1;
    assert!(cut.len() <= ENTRY_MAX && cut.ends_with('…'), "{}", cut.len());
    let short = &entries[..1];
    assert!(entries_text(short, true).contains(&format!(" on thread {}: 00", short[0].thread_id)));
}

/// A human asks an ant for a TL;DR, which the CLI keeps until the thread has a newer entry; the request's thread is
/// left out of the threads listed and of the human's recent threads, and its tokens count for the day.
#[tokio::test]
async fn a_tldr_is_asked_kept_while_current_and_its_thread_left_out() {
    let Some(server) = Server::start() else { return };
    let (alice, dev_a) = (p("alice"), p("dev-a"));
    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let started = Utc::now();
    bus::ensure_stream(&js).await.unwrap();
    js.create_stream(ants_daemon::hub::threads_config()).await.unwrap();
    let (ant, ant_js) = server.connect(&dev_a).await;
    let work = Envelope::new(alice.clone(), dev_a.clone(), "fix the build");
    bus::publish(&js, &work).await.unwrap();
    let done = work.reply(dev_a.clone(), "fixed", Role::Human).unwrap();
    bus::publish(&ant_js, &done).await.unwrap();

    // The ant answers a summary request with its TL;DR and the tokens spent.
    let mut asked = bus::live_inbox(&ant, &dev_a).await.unwrap();
    let answering = tokio::spawn(async move {
        let msg = asked.next().await.unwrap();
        let request: Envelope = serde_json::from_slice(&msg.payload).unwrap();
        let summary = request.summary.clone().map(|s| SummaryRequest { tokens: Some(42), ..s });
        let reply = Envelope { summary, ..request.reply(dev_a.clone(), " The build is fixed. ", Role::Human).unwrap() };
        bus::publish(&ant_js, &reply).await.unwrap();
        request
    });
    let (text, request) = tldr_request(&js, &work.thread_id).await.unwrap().unwrap();
    assert!(text.contains("alice -> dev-a: fix the build") && text.contains("dev-a -> alice: fixed"), "{text}");
    let creds = server.creds_as(&alice, Role::Human);
    let nats = Nats::new(&server.url, Some(&creds), None, None).unwrap();
    let envelope = request_envelope(&alice, p("dev-a"), text, request.clone());
    let reply = tokio::time::timeout(Duration::from_secs(10), ask(&nats, &js, &envelope)).await.unwrap().unwrap();
    let sent = answering.await.unwrap();
    assert_eq!((sent.summary, sent.owner), (Some(request.clone()), Some(alice.clone())));
    let tldr = tldr_of(&request, &reply, &alice).unwrap();
    assert_eq!((tldr.text.as_str(), &tldr.by, tldr.tokens), ("The build is fixed.", &p("dev-a"), 42));

    let meta = thread_meta::open(&nats, &js).await.unwrap();
    meta.put_summary(&tldr).await.unwrap();
    let kept = meta.summary(&work.thread_id).await.unwrap().unwrap();
    let (_, now) = tldr_request(&js, &work.thread_id).await.unwrap().unwrap();
    assert!(kept.is_current(now.upto), "no entry since");
    bus::publish(&js, &Envelope::on_thread(work.thread_id.clone(), alice.clone(), p("dev-a"), "thanks")).await.unwrap();
    let (_, later) = tldr_request(&js, &work.thread_id).await.unwrap().unwrap();
    assert!(!kept.is_current(later.upto), "stale once a newer entry is stored");

    let mut threads = Threads::new(None);
    thread::scan(&js, consumer::DeliverPolicy::All, |e| threads.add(&e)).await.unwrap();
    let (listed, _) = threads.finish(&Select::default(), &ThreadMarks::default());
    let listed: Vec<&ThreadId> = listed.iter().map(|t| &t.thread_id).collect();
    assert_eq!(listed, [&work.thread_id]);
    let recent = thread::recent_threads(&js, &alice).await.unwrap();
    assert!(recent.iter().all(|e| e.thread_id == work.thread_id), "{recent:?}");
    let unread = inbox::Inbox::open(&js, &alice).await.unwrap().unread(&inbox::Select::default()).await.unwrap();
    assert_eq!(unread.iter().map(|e| e.id).collect::<Vec<_>>(), [done.id], "the TL;DR reply is read");
    assert_eq!(spent_today(&js, &alice, started).await.unwrap(), 42);
}
