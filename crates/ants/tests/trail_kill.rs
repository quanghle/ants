//! A worker killed mid-run. Alone in its test binary: it kills the one child process the test has.

use std::process::Command;
use std::time::Duration;

use ants_proto::CallAnswer;
use ants_trail::{Limits, Run, RunError, Status, Worker, WorkerError};
use serde_json::json;

const SCRIPT: &str = r#"meta = {"name": "busy", "description": "computes after its step", "phases": []}
x = await agent("go")
n = 0
for i in range(1000000):
    n += 1
[x, n]
"#;

/// Kills the first child process of this test with SIGKILL.
async fn kill_the_worker() {
    let me = std::process::id().to_string();
    for _ in 0..500 {
        let children = Command::new("pgrep").args(["-P", &me]).output().unwrap();
        let children = String::from_utf8(children.stdout).unwrap();
        if let Some(pid) = children.lines().next() {
            assert!(Command::new("kill").args(["-KILL", pid]).status().unwrap().success());
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no worker to kill");
}

#[tokio::test]
async fn a_run_whose_worker_was_killed_resumes_from_its_last_snapshot() {
    let worker = Worker::new(env!("CARGO_BIN_EXE_ants"));
    let mut run = Run::new(SCRIPT, None, json!(null), Limits::default()).unwrap();
    let call = run.start(&worker).await.unwrap().calls[0].call;
    run.answer(call, CallAnswer::Ok(json!("went"))).unwrap();
    let before = serde_json::to_string(run.state()).unwrap();

    let (resumed, ()) = tokio::join!(run.resume(&worker), kill_the_worker());
    let Err(RunError::Worker(WorkerError::Crashed { status, .. })) = resumed else {
        panic!("the killed worker crashed: {resumed:?}");
    };
    assert!(status.contains("signal: 9"), "{status}");
    assert_eq!(serde_json::to_string(run.state()).unwrap(), before);
    assert_eq!(run.journal().len(), 1);

    run.resume(&worker).await.unwrap();
    assert_eq!(*run.status(), Status::Done { result: json!(["went", 1_000_000]) });
}
