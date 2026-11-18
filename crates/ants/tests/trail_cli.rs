//! `ants trail run`, `stop`, `resume` and `show --follow` against a conductor's daemon running trails
//! ([`ants_daemon::trail`]): the ants a run's steps go to are played by the test, which reads the steps the conductor
//! sends and hands it their replies, as its inbox would.

use std::path::Path;
use std::process::{Output, Stdio};
use std::sync::Arc;
use std::time::Duration;

use ants_daemon::people::humans::Registry;
use ants_daemon::trail::{Conducting, Trails};
use ants_proto::{Envelope, Role, UserId};
use ants_test_support::{NatsServer, scratch_dir};
use tokio::io::{AsyncBufReadExt as _, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

const ANTS: &str = env!("CARGO_BIN_EXE_ants");

const SCRIPT: &str = r#"meta = {"name": "ship", "description": "Build then ship", "phases": ["build", "ship"]}
phase("build")
built = await agent("build it", ant="dev")
phase("ship")
await agent("ship " + built, ant="ops")
"#;

/// The conductor `lead`'s daemon serving its runs, its sent steps on `sent`.
struct Conductor {
    trails: Trails,
    sent: mpsc::UnboundedReceiver<Envelope>,
    serving: JoinHandle<()>,
}

impl Drop for Conductor {
    fn drop(&mut self) {
        self.serving.abort();
    }
}

impl Conductor {
    async fn start(server: &NatsServer) -> Self {
        let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &UserId::human(), "init").await.unwrap();
        for config in [ants_daemon::hub::trails_config(), ants_daemon::hub::trail_runs_config()] {
            js.create_key_value(config).await.unwrap();
        }
        let lead: UserId = "lead".parse().unwrap();
        let (client, js) = ants_daemon::bus::connect(&server.url, None, None, &lead, "conductor").await.unwrap();
        let attempts = ants_daemon::bus::ensure_attempts(&js, None, &lead).await.unwrap();
        let (tx, sent) = mpsc::unbounded_channel();
        let outbox: ants_daemon::mcp::Outbox = Arc::new(move |envelope| {
            let tx = tx.clone();
            Box::pin(async move {
                tx.send(envelope).unwrap();
                Ok(())
            })
        });
        let colony: ants_daemon::mcp::Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
        let conducting = Conducting {
            js,
            js_domain: None,
            client,
            ant: lead,
            worker: ANTS.into(),
            trails_dir: None,
            humans: Registry::new(UserId::human()),
            colony,
            outbox,
            attempts,
            workers: ants_daemon::trail::DEFAULT_WORKERS,
        };
        let trails = Trails::new(conducting).await.unwrap();
        trails.restore().await;
        let serving = tokio::spawn({
            let trails = trails.clone();
            async move { trails.serve().await }
        });
        Self { trails, sent, serving }
    }

    /// The next step the conductor sends.
    async fn step(&mut self) -> Envelope {
        tokio::time::timeout(Duration::from_secs(30), self.sent.recv()).await.expect("a step").unwrap()
    }

    /// `step`'s recipient replying `text`.
    async fn reply(&self, step: &Envelope, text: &str) {
        let reply = step.reply(step.to.clone(), text, Role::Ant).unwrap();
        assert!(self.trails.take(&reply).await.unwrap(), "the conductor takes the reply to a step");
    }
}

/// `ants --nats URL ARGS…` as the human, with a home of its own.
fn ants(server: &NatsServer, home: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(ANTS);
    command
        .args(["--nats", &server.url])
        .args(args)
        .env_remove("ANTS_CREDS")
        .env_remove("ANTS_CONFIG")
        .env("HOME", home)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    command
}

async fn output(mut command: Command) -> Output {
    tokio::time::timeout(Duration::from_secs(60), command.output()).await.expect("the command ends").unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A command running, its output read a line at a time.
struct Following {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    read: Vec<String>,
}

impl Following {
    fn spawn(mut command: Command) -> Self {
        let mut child = command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        Self { child, lines, read: Vec::new() }
    }

    /// Read up to the first line holding `part`, and return it.
    async fn line(&mut self, part: &str) -> String {
        loop {
            let next = tokio::time::timeout(Duration::from_secs(30), self.lines.next_line()).await;
            let Ok(Ok(Some(line))) = next else { panic!("no line holding {part:?} in {:#?}", self.read) };
            self.read.push(line.clone());
            if line.contains(part) {
                return line;
            }
        }
    }

    /// Wait for the command to end: its success and its lines.
    async fn end(mut self) -> (bool, Vec<String>) {
        while let Some(line) = self.lines.next_line().await.unwrap() {
            self.read.push(line);
        }
        let status = tokio::time::timeout(Duration::from_secs(30), self.child.wait()).await.unwrap().unwrap();
        (status.success(), self.read)
    }
}

#[tokio::test]
async fn a_run_started_and_followed_from_the_cli_ends_with_its_result() {
    let Some(server) = NatsServer::start() else { return };
    let mut lead = Conductor::start(&server).await;
    let dir = scratch_dir("trail-cli-run");
    let file = dir.join("ship.py");
    std::fs::write(&file, SCRIPT).unwrap();
    let file = file.to_str().unwrap();

    let refused = output(ants(&server, &dir, &["trail", "run", file, "--ant", "lead"])).await;
    assert!(!refused.status.success());
    assert!(text(&refused.stdout).contains("the run may spend at most:\n"), "{}", text(&refused.stdout));
    assert!(text(&refused.stderr).contains("no terminal to confirm the run on: --yes starts it"));
    let over = output(ants(&server, &dir, &["trail", "run", file, "--yes", "--ant", "lead", "--steps", "501"])).await;
    assert!(text(&over.stderr).contains("--steps is over the most a run may take, 500"), "{}", text(&over.stderr));
    let args = output(ants(&server, &dir, &["trail", "run", file, "--yes", "--args", "{oops"])).await;
    assert!(!args.status.success() && text(&args.stderr).contains("not JSON"), "{}", text(&args.stderr));

    let run = ["trail", "run", file, "--yes", "--follow", "--ant", "lead", "--steps", "9", "--args", "{\"v\": 1}"];
    let mut following = Following::spawn(ants(&server, &dir, &run));
    following.line("  steps:      9,").await;
    let started = following.line("started run ").await;
    assert!(started.contains(" on lead, reporting on thread "), "{started}");
    following.line("phase: build").await;
    let build = lead.step().await;
    assert_eq!((build.to.as_str(), build.text.as_str()), ("dev", "build it"));
    following.line(" sent to dev: build it").await;
    lead.reply(&build, "v1").await;
    following.line(" answered: v1").await;
    let ship = lead.step().await;
    assert_eq!((ship.to.as_str(), ship.text.as_str()), ("ops", "ship v1"));
    lead.reply(&ship, "shipped v1").await;
    let (success, lines) = following.end().await;
    assert!(success, "{lines:#?}");
    assert_eq!(lines.last().map(String::as_str), Some("done: shipped v1"), "{lines:#?}");
    assert!(lines.iter().any(|line| line.ends_with(" answered: shipped v1")), "{lines:#?}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_run_stopped_from_the_cli_resumes_with_raised_limits_and_is_followed_to_its_end() {
    let Some(server) = NatsServer::start() else { return };
    let mut lead = Conductor::start(&server).await;
    let dir = scratch_dir("trail-cli-stop");
    let file = dir.join("wait.py");
    let script =
        "meta = {\"name\": \"wait\", \"description\": \"\", \"phases\": []}\nawait agent(\"build it\", ant=\"dev\")\n";
    std::fs::write(&file, script).unwrap();

    let started =
        output(ants(&server, &dir, &["trail", "run", file.to_str().unwrap(), "--yes", "--ant", "lead"])).await;
    assert!(started.status.success(), "{}", text(&started.stderr));
    let stdout = text(&started.stdout);
    let run = stdout.lines().find_map(|line| line.strip_prefix("started run ")).unwrap().split(' ').next().unwrap();
    let run = run.to_owned();
    let first = lead.step().await;
    let prefix = &run[..13];

    let unknown = output(ants(&server, &dir, &["trail", "stop", "fff"])).await;
    assert!(!unknown.status.success());
    assert!(text(&unknown.stderr).contains("no run's id starts with fff"), "{}", text(&unknown.stderr));
    let resumed = output(ants(&server, &dir, &["trail", "resume", prefix])).await;
    assert!(!resumed.status.success());
    assert!(
        text(&resumed.stderr).contains(&format!("lead refused: run {run} is not stopped")),
        "{}",
        text(&resumed.stderr)
    );

    let stopped = output(ants(&server, &dir, &["trail", "stop", prefix])).await;
    assert!(stopped.status.success(), "{}", text(&stopped.stderr));
    let stdout = text(&stopped.stdout);
    assert!(stdout.starts_with(&format!("stopped run {run} on lead: ")), "{stdout}");
    assert!(stdout.contains(&format!("  dev message {0}: ants withdraw dev {0}\n", first.id)), "{stdout}");
    let followed = output(ants(&server, &dir, &["trail", "show", &run, "--follow"])).await;
    assert!(!followed.status.success());
    let stderr = text(&followed.stderr);
    assert!(
        stderr.contains(&format!("run {run} was stopped by human: `ants trail resume {run}` resumes it")),
        "{stderr}"
    );

    let resumed = output(ants(&server, &dir, &["trail", "resume", prefix, "--steps", "60"])).await;
    assert!(resumed.status.success(), "{}", text(&resumed.stderr));
    assert!(text(&resumed.stdout).starts_with(&format!("resumed run {run} on lead under 60 steps, ")));
    let again = lead.step().await;
    assert_ne!(again.id, first.id, "the step goes out anew");
    let mut following = Following::spawn(ants(&server, &dir, &["trail", "show", prefix, "--follow"]));
    following.line(" sent to dev: build it").await;
    lead.reply(&again, "built").await;
    let (success, lines) = following.end().await;
    assert!(success, "{lines:#?}");
    assert_eq!(lines.last().map(String::as_str), Some("done: built"), "{lines:#?}");
    std::fs::remove_dir_all(dir).unwrap();
}
