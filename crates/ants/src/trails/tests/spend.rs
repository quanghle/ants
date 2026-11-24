//! What a run spent, as `ants trail show RUN` tells it: the spend of its step threads, in all and by ant, and the
//! ants its steps went to that counted none.

use std::collections::{BTreeSet, HashMap};

use ants_daemon::bus;
use ants_daemon::harness::Spend;
use ants_daemon::limits::caps::{Caps, Ledger, Spent};
use ants_proto::{
    CallAnswer, JournalEntry, PendingCall, RunId, RunState, ThreadId, TrailFunction, TrailSession, TrailStepThread,
    Uuid, trail_call_key, trail_session_key, trail_thread_step_key,
};
use chrono::Utc;
use serde_json::json;

use super::super::runs;
use super::{human, p, put, record};
use crate::test_support::Server;

fn spent(tokens: u64, cost: f64) -> Spent {
    Spent { tokens, cost }
}

#[test]
fn a_run_adds_up_its_step_threads_by_ant_and_names_the_ants_that_counted_none() {
    let (t1, t2) = (ThreadId::generate(), ThreadId::generate());
    let on = HashMap::from([
        (t1.clone(), vec![(p("dev-a"), spent(100, 0.5)), (p("dev-c"), spent(20, 0.0))]),
        (t2, vec![(p("dev-a"), spent(50, 0.25)), (p("dev-b"), spent(0, 0.0))]),
    ]);
    let asked = BTreeSet::from([p("dev-a"), p("dev-b")]);
    let spend = runs::run_spend(&on, &asked);
    assert_eq!(spend.total, spent(170, 0.75));
    let by: Vec<(&str, Spent)> = spend.by_ant.iter().map(|a| (a.ant.as_str(), a.spent)).collect();
    assert_eq!(by, [("dev-a", spent(150, 0.75)), ("dev-c", spent(20, 0.0))], "an ant dev-a asked counts too");
    assert_eq!(spend.uncounted, [p("dev-b")], "spent nothing on its thread");
    let json = serde_json::to_value(&spend).unwrap();
    assert_eq!(
        json,
        json!({"tokens": 170, "cost": 0.75, "by_ant": [{"ant": "dev-a", "tokens": 150, "cost": 0.75},
            {"ant": "dev-c", "tokens": 20, "cost": 0.0}], "uncounted": ["dev-b"]})
    );

    let now = Utc::now();
    let shown = |spend| {
        let run = record(&p("lead"), Some("release"), RunState::Running, now);
        let detail = runs::Detail {
            run,
            pending: Vec::new(),
            queued: Vec::new(),
            journal: Vec::new(),
            children: Vec::new(),
            ratings: Vec::new(),
            spend,
        };
        runs::render(&detail, now)
    };
    let text = shown(Some(spend));
    let line = "\nspent on its step threads: 170 tokens, cost 0.75 (dev-a 150 tokens, cost 0.75; dev-c 20 tokens, cost \
                0.00)\ncounted no spend, as a harness reporting none: dev-b\n";
    assert!(text.contains(line), "{text}");
    let one = runs::run_spend(&HashMap::from([(t1, vec![(p("dev-a"), spent(9, 0.0))])]), &BTreeSet::new());
    assert!(shown(Some(one)).contains("\nspent on its step threads: 9 tokens, cost 0.00, all by dev-a\n"));
    assert!(!shown(None).contains("spent on"), "not read");
}

#[tokio::test]
async fn a_human_reads_what_the_step_threads_of_a_run_spent() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let js = human(&server, &alice).await;
    bus::init_own_buckets(&js).await.unwrap();
    let lead = p("lead");
    let (_, lead_js) = server.connect(&lead).await;
    let now = Utc::now();
    let run = record(&lead, Some("release"), RunState::Waiting, now);
    put(&lead_js, &run.key(), &run).await;
    let threads: Vec<ThreadId> = (0..4).map(|_| ThreadId::generate()).collect();
    let marker = |call: u32, to: &str| TrailStepThread {
        run: run.run.clone(),
        call,
        to: p(to),
        run_thread: run.thread.clone(),
        label: None,
    };
    let other = TrailStepThread { run: RunId::generate(), ..marker(0, "dev-x") };
    for (thread, marker) in [
        (&threads[0], marker(0, "dev-b")),
        (&threads[1], marker(1, "dev-d")),
        (&threads[2], marker(2, "alice")),
        (&threads[3], other),
    ] {
        put(&lead_js, &trail_thread_step_key(&lead, &run.thread, thread), &marker).await;
    }
    let answered = |call: u32, ant: &str| JournalEntry {
        call,
        function: TrailFunction::Agent,
        args: json!({"prompt": "build", "ant": ant}),
        answer: CallAnswer::Ok(json!("built")),
    };
    put(&lead_js, &trail_call_key(&lead, &run.run, 0), &answered(0, "dev-b")).await;
    put(&lead_js, &trail_call_key(&lead, &run.run, 1), &answered(1, "dev-d")).await;
    let asking = PendingCall {
        call: 2,
        function: TrailFunction::Human,
        args: json!({"questions": "Ship it?"}),
        to: alice.clone(),
        thread: threads[2].clone(),
        message: Uuid::now_v7(),
        sent_at: now,
        sent: true,
        turn_budget: 8,
        retries: 0,
        asked: false,
    };
    let session =
        TrailSession { session: "opaque".to_owned(), pending: vec![asking], queued: Vec::new(), anew: Vec::new() };
    put(&lead_js, &trail_session_key(&lead, &run.run), &session).await;

    let today = Utc::now().date_naive().format("%Y-%m-%d").to_string();
    for (ant, thread, tokens) in [("dev-b", 0, 100), ("dev-c", 0, 20), ("dev-c", 3, 5), ("dev-x", 3, 7)] {
        let ant = p(ant);
        let (_, ant_js) = server.connect(&ant).await;
        let kv = bus::ensure_sessions(&ant_js, None, &ant).await.unwrap();
        let ledger = Ledger::kv(kv, ant, Caps::default());
        ledger.record_on(&threads[thread], &Spend::Tokens(tokens), &today).await.unwrap();
    }

    let detail = runs::detail(&js, &run.run).await.unwrap();
    let spend = detail.spend.clone().expect("the spend is read");
    assert_eq!(spend.total, spent(120, 0.0), "the other run's step thread left out");
    let by: Vec<(&str, u64)> = spend.by_ant.iter().map(|a| (a.ant.as_str(), a.spent.tokens)).collect();
    assert_eq!(by, [("dev-b", 100), ("dev-c", 20)], "dev-c, asked on dev-b's step thread, counts");
    assert_eq!(spend.uncounted, [p("dev-d")], "the owner asked by human() is no ant");
    let json = serde_json::to_value(&detail).unwrap();
    assert_eq!((&json["spend"]["tokens"], &json["spend"]["uncounted"]), (&json!(120), &json!(["dev-d"])), "{json}");
    let text = runs::render(&detail, now);
    assert!(text.contains("spent on its step threads: 120 tokens, cost 0.00 (dev-b 100 tokens"), "{text}");
    assert!(text.contains("counted no spend, as a harness reporting none: dev-d\n"), "{text}");
}
