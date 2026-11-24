//! The trail tools of an ant's MCP server: run_trail, trail_status and stop_trail.

use super::*;

#[tokio::test]
async fn run_trail_starts_a_saved_or_inline_trail_and_a_webhook_s_thread_runs_only_saved_ones() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-mcp");
    std::fs::create_dir_all(dir.join("saved")).unwrap();
    std::fs::write(dir.join("saved/count.py"), format!("{META}args[\"n\"] + 1\n")).unwrap();
    let lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(dir.join("saved"))).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;

    let asked =
        Envelope::new(ants_test_support::human(), ant("lead"), "count").with_owner(Some(ants_test_support::human()));
    let _turn = mcp.begin(&asked);
    let client = mcp_client(&mcp, &asked.thread_id).await;
    let script = format!("{META}args[\"n\"] * 10\n");
    let (ok, text) = run_trail(&client, json!({"script": script, "args": {"n": 4}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n40\nrate_trail(run: "), "{text}");
    let (ok, text) = run_trail(&client, json!({"name": "count", "args": {"n": 4}})).await;
    assert!(ok && text.starts_with("Started run "), "{text}");
    let run = started(&text);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(5) });
    let header = lead.header(&run).await;
    assert_eq!((&header.thread, header.owner.as_ref()), (&asked.thread_id, Some(&ants_test_support::human())));
    let (ok, text) = run_trail(&client, json!({"name": "count", "script": script})).await;
    assert!(!ok && text.contains("either `name` or `script`"), "{text}");
    let limits = json!({"steps": 9, "at_once": 2, "turns": 30, "wall": "2h"});
    let (ok, text) = run_trail(&client, json!({"name": "count", "args": {"n": 1}, "limits": limits})).await;
    assert!(ok, "{text}");
    let asked = TrailLimits { steps: 9, concurrency: 2, turns: 30, wall_secs: 7200, ..TrailLimits::DEFAULT };
    assert_eq!(lead.header(&started(&text)).await.limits, asked, "the run's record keeps the limits asked for");
    for (limits, why) in [
        (json!({"steps": 501}), "limits: steps is over the most a run may take, 500"),
        (json!({"wall": "8d"}), "limits: wall is over the most a run may take, 7d"),
        (json!({"at_once": 0}), "limits: at_once must be more than 0"),
    ] {
        let (ok, text) = run_trail(&client, json!({"name": "count", "limits": limits})).await;
        assert!(!ok && text.contains(why), "{text}");
    }

    let hooked = Envelope::new(ant("github"), ant("lead"), "an issue").from_webhook();
    let _turn = mcp.begin(&hooked);
    let client = mcp_client(&mcp, &hooked.thread_id).await;
    let (ok, text) = run_trail(&client, json!({"script": script, "args": {"n": 1}})).await;
    assert!(!ok && text.contains("only saved trails"), "{text}");
    let (ok, text) = run_trail(&client, json!({"name": "count", "args": {"n": 1}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n2\n"), "{text}");

    let idle = ThreadId::generate();
    let (ok, text) = run_trail(&mcp_client(&mcp, &idle).await, json!({"name": "count"})).await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn trail_status_and_stop_trail_reach_only_the_runs_the_ant_conducts() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-mcp-status");
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;
    let asked =
        Envelope::new(ants_test_support::human(), ant("lead"), "review").with_owner(Some(ants_test_support::human()));
    let _turn = mcp.begin(&asked);
    let client = mcp_client(&mcp, &asked.thread_id).await;
    let script = format!("{META}phase(\"one\")\nawait agent(\"go\", ant=\"dev\", label=\"the go\")\n");
    let (ok, text) = run_trail(&client, json!({"script": script})).await;
    assert!(ok, "{text}");
    let run = started(&text);
    let step = lead.step().await;

    let (ok, status) = call(&client, "trail_status", json!({"run": run.as_str()})).await;
    assert!(ok, "{status}");
    for told in [": running.", "Spent 1 of 50 steps, 0 of 200 turns", "of 1d wall time", "to ant dev (the go)"] {
        assert!(status.contains(told), "{told:?} in {status}");
    }
    let (ok, listed) = call(&client, "trail_status", json!({})).await;
    assert!(ok && listed.contains(run.as_str()) && listed.contains("1 of 50 steps"), "{listed}");

    let mut other = lead.header(&run).await;
    (other.conductor, other.run) = (ant("dev"), RunId::generate());
    let runs = DirectKv::new(&lead.js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    runs.put(other.key(), serde_json::to_vec(&other).unwrap()).await.unwrap();
    for tool in ["trail_status", "stop_trail"] {
        let (ok, text) = call(&client, tool, json!({"run": other.run.as_str()})).await;
        assert!(!ok && text.contains(&format!("lead conducts no run {}", other.run)), "{tool}: {text}");
    }
    let (ok, text) = call(&client, "stop_trail", json!({"run": "nonsense"})).await;
    assert!(!ok && text.contains("nonsense"), "{text}");

    let (ok, text) = call(&client, "stop_trail", json!({"run": run.as_str()})).await;
    assert!(ok && text.starts_with(&format!("Stopped run {run}")), "{text}");
    assert!(text.contains(&format!("dev message {}", step.id)), "the step for a human to withdraw: {text}");
    assert_eq!(lead.header(&run).await.state, RunState::Stopped { by: ant("lead") });
    assert!(lead.told().await.text.contains("was stopped by lead"));
    let (ok, status) = call(&client, "trail_status", json!({"run": run.as_str()})).await;
    assert!(ok && status.contains("stopped by lead") && status.contains("ants trail resume"), "{status}");
    let (ok, text) = call(&client, "stop_trail", json!({"run": run.as_str()})).await;
    assert!(!ok && text.contains("already stopped"), "{text}");
    assert!(lead.reply(&step, "late").await, "a stopped step's reply is not prompted");
    assert!(lead.sends_no_step(Duration::from_millis(500)).await);

    lead.trails.resume(&run, &ants_test_support::human(), None).await.unwrap();
    let step = lead.step().await;
    assert!(lead.reply(&step, "went").await);
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!("went") });
    let (ok, status) = call(&client, "trail_status", json!({"run": run.as_str()})).await;
    assert!(ok && status.contains(": done.") && status.contains("Result:\nwent\nrate_trail(run: "), "{status}");
    let (ok, listed) = call(&client, "trail_status", json!({})).await;
    assert!(ok && listed == "This ant conducts no run that has not ended.", "{listed}");
    std::fs::remove_dir_all(&dir).unwrap();
}
