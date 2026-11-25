//! Tests of the outage watch: one ERROR per server, or JetStream behind it, out of reach for longer than the
//! threshold, however many connections or heartbeats report it, naming what runs it, an INFO once every reporter
//! reaches it again, nothing for a shorter outage or one ending with its reporters gone, and nothing for JetStream
//! while the server itself is out of reach.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use super::*;

const AFTER: Duration = Duration::from_secs(60);
const URL: &str = "nats://127.0.0.1:4222";

/// The events logged, as `LEVEL message url=…`.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<String>>>);

impl Logged {
    fn lines(&self) -> Vec<String> {
        self.0.locked().clone()
    }
}

impl tracing::Subscriber for Logged {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::INFO
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut line = event.metadata().level().to_string();
        event.record(&mut |field: &tracing::field::Field, value: &dyn std::fmt::Debug| match field.name() {
            "message" => _ = write!(line, " {value:?}"),
            name => _ = write!(line, " {name}={value:?}"),
        });
        self.0.locked().push(line);
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn report(reports: &UnboundedSender<Report>, connection: u64, change: Change) {
    reports.send(Report { url: URL.into(), of: Of::Server, reporter: connection, change }).unwrap();
}

fn heartbeat(reports: &UnboundedSender<Report>, ant: u64, change: Change) {
    reports.send(Report { url: URL.into(), of: Of::JetStream, reporter: ant, change }).unwrap();
}

/// Wait until something more than `seen` lines is logged, as the question of what runs the server goes to a
/// blocking thread, which paused time does not wait for.
async fn logged_past(logged: &Logged, seen: usize) {
    for _ in 0..200 {
        if logged.lines().len() > seen {
            return;
        }
        tokio::task::yield_now().await;
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Let the watch take what was sent, and wait `wait`.
async fn settle(wait: Duration) {
    tokio::time::sleep(wait).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_server_out_of_reach_past_the_threshold_gets_one_error_naming_what_runs_it_then_an_info() {
    let logged = Logged::default();
    let _logging = tracing::subscriber::set_default(logged.clone());
    let (reports, rx) = unbounded_channel();
    let asked = Arc::new(Mutex::new(Vec::new()));
    let runs = {
        let asked = asked.clone();
        move |url: &str, of: Of| {
            assert_eq!(of, Of::Server);
            asked.locked().push(url.to_owned());
            Some("start its service with `launchctl kickstart gui/501/ants.nats`".to_owned())
        }
    };
    let _watch = tokio::spawn(run(AFTER, Arc::new(runs), rx));
    for connection in 1..=13 {
        report(&reports, connection, Change::Down);
    }
    settle(AFTER - Duration::from_secs(1)).await;
    assert_eq!(logged.lines(), Vec::<String>::new(), "not yet");
    settle(Duration::from_secs(2)).await;
    logged_past(&logged, 0).await;
    assert_eq!(
        logged.lines(),
        [format!(
            "ERROR the NATS server is out of reach for over 60s: start its service with `launchctl kickstart \
             gui/501/ants.nats` url={URL}"
        )]
    );
    assert_eq!(*asked.locked(), [URL]);
    settle(AFTER * 3).await;
    assert_eq!(logged.lines().len(), 1, "one ERROR per outage: {:?}", logged.lines());

    for connection in 1..13 {
        report(&reports, connection, Change::Up);
    }
    settle(Duration::ZERO).await;
    assert_eq!(logged.lines().len(), 1, "a connection is still down: {:?}", logged.lines());
    report(&reports, 13, Change::Up);
    settle(Duration::ZERO).await;
    assert_eq!(logged.lines()[1], format!("INFO the NATS server answers again url={URL} out_for=241s"));
}

#[tokio::test(start_paused = true)]
async fn a_shorter_outage_or_one_whose_connections_are_gone_logs_nothing() {
    let logged = Logged::default();
    let _logging = tracing::subscriber::set_default(logged.clone());
    let (reports, rx) = unbounded_channel();
    let _watch = tokio::spawn(run(AFTER, Arc::new(|_: &str, _| None), rx));
    report(&reports, 1, Change::Down);
    report(&reports, 2, Change::Down);
    settle(AFTER / 2).await;
    report(&reports, 1, Change::Up);
    report(&reports, 2, Change::Up);
    settle(AFTER * 2).await;
    assert_eq!(logged.lines(), Vec::<String>::new());

    report(&reports, 1, Change::Down);
    settle(AFTER / 2).await;
    report(&reports, 1, Change::Gone);
    settle(AFTER * 2).await;
    assert_eq!(logged.lines(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn a_server_nothing_is_known_to_run_is_named_by_its_address_alone() {
    let logged = Logged::default();
    let _logging = tracing::subscriber::set_default(logged.clone());
    let (reports, rx) = unbounded_channel();
    let _watch = tokio::spawn(run(AFTER, Arc::new(|_: &str, _| None), rx));
    report(&reports, 1, Change::Down);
    settle(AFTER * 2).await;
    logged_past(&logged, 0).await;
    assert_eq!(
        logged.lines(),
        [format!(
            "ERROR the NATS server is out of reach for over 60s: nothing on this machine is known to run it url={URL}"
        )]
    );
    report(&reports, 1, Change::Gone);
    settle(Duration::ZERO).await;
    assert_eq!(logged.lines().len(), 1, "gone, not answering: {:?}", logged.lines());
}

#[tokio::test(start_paused = true)]
async fn jetstream_unanswered_through_a_server_that_answers_gets_its_own_error_held_while_the_server_is_out() {
    let logged = Logged::default();
    let _logging = tracing::subscriber::set_default(logged.clone());
    let (reports, rx) = unbounded_channel();
    let runs = |_: &str, of: Of| match of {
        Of::Server => None,
        Of::JetStream => Some("start the uplink's service with `launchctl kickstart gui/501/ants.bridge`".to_owned()),
    };
    let _watch = tokio::spawn(run(AFTER, Arc::new(runs), rx));

    // The server is out of reach too: its ERROR alone.
    report(&reports, 1, Change::Down);
    heartbeat(&reports, 10, Change::Down);
    heartbeat(&reports, 11, Change::Down);
    settle(AFTER + Duration::from_secs(1)).await;
    logged_past(&logged, 0).await;
    settle(Duration::from_secs(5)).await;
    let lines = logged.lines();
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(lines[0].starts_with("ERROR the NATS server is out of reach"), "{lines:?}");

    // Back, with JetStream still unanswered: its ERROR, overdue already.
    report(&reports, 1, Change::Up);
    settle(Duration::ZERO).await;
    logged_past(&logged, 2).await;
    let lines = logged.lines();
    assert_eq!(lines[1], format!("INFO the NATS server answers again url={URL} out_for=66s"));
    assert_eq!(
        lines[2],
        format!(
            "ERROR JetStream does not answer through the NATS server for over 60s, as when a leaf node's uplink to the \
             hub is down: start the uplink's service with `launchctl kickstart gui/501/ants.bridge` url={URL}"
        )
    );
    heartbeat(&reports, 10, Change::Up);
    settle(AFTER).await;
    assert_eq!(logged.lines().len(), 3, "a heartbeat is still unanswered: {:?}", logged.lines());
    heartbeat(&reports, 11, Change::Up);
    settle(Duration::ZERO).await;
    assert_eq!(logged.lines()[3], format!("INFO JetStream answers again url={URL} out_for=126s"));

    // Unanswered alone, from the start.
    heartbeat(&reports, 10, Change::Down);
    settle(AFTER + Duration::from_secs(1)).await;
    logged_past(&logged, 4).await;
    assert!(logged.lines()[4].contains("`launchctl kickstart gui/501/ants.bridge`"), "{:?}", logged.lines());
}
