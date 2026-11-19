//! The MCP tools saving trails for the colony: `save_trail`, saving a script or a finished inline run's as the ant's
//! own, and `remove_trail`, removing one the ant saved, both refused on untrusted threads.

use ants_proto::{MAX_TRAIL_SCRIPT, SavedTrail, Saver, TRAILS_BUCKET, TipId, TrailTip, script_sha256};

use super::tip_tools::ant_mcp;
use super::*;

type Client = RunningService<RoleClient, ClientConfig>;

/// The colony's saved trail `name`, if any.
async fn colony_trail(js: &async_nats::jetstream::Context, name: &str) -> Option<SavedTrail> {
    let kv = DirectKv::new(js, None, TRAILS_BUCKET).await.unwrap();
    kv.get(name).await.unwrap().map(|value| serde_json::from_slice(&value).unwrap())
}

/// The id of the run `run_trail` says ended, with `wait`.
fn ended_run(text: &str) -> RunId {
    text.strip_prefix("Run ").unwrap().split_once(' ').unwrap().0.parse().unwrap()
}

/// An MCP client of `mcp` on a thread a human started, handling its message.
async fn trusted(mcp: &Mcp) -> (Client, ants_daemon::mcp::Turn) {
    let asked = Envelope::new(UserId::human(), ant("lead"), "save it").with_owner(Some(UserId::human()));
    let turn = mcp.begin(&asked);
    (mcp_client(mcp, &asked.thread_id).await, turn)
}

async fn save(client: &Client, args: Value) -> (bool, String) {
    call(client, "save_trail", args).await
}

#[tokio::test]
async fn an_ant_saves_a_script_or_a_finished_inline_run_s_for_every_ant_to_run_by_name() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("save-tools");
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    std::fs::write(saved.join("local.py"), format!("{META}1\n")).unwrap();
    let lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved.clone())).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;
    let (client, _turn) = trusted(&mcp).await;

    let tenfold = format!("{META}args[\"n\"] * 10\n");
    let (ok, text) = save(&client, json!({"name": "tenfold", "script": tenfold})).await;
    assert!(ok && text.starts_with("Saved trail tenfold for the colony as this ant's."), "{text}");
    let trail = colony_trail(&lead.js, "tenfold").await.unwrap();
    assert_eq!((&trail.saved_by, &trail.saved_from), (&Saver::Ant { user: ant("lead") }, &None));
    assert_eq!((trail.script.as_str(), trail.description.as_str()), (tenfold.as_str(), "a test"));
    assert_eq!(trail.phases, ["one"]);

    let (other_trails, other) = ant_mcp(&server, "dev-b", &dir, &dir.join("none")).await;
    let serving = tokio::spawn({
        let trails = other_trails.clone();
        async move { trails.serve().await }
    });
    let (other_client, _other_turn) = trusted(&other).await;
    let (ok, text) = run_trail(&other_client, json!({"name": "tenfold", "args": {"n": 4}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n40\n"), "another ant runs it by name: {text}");

    let plus_one = format!("{META}args[\"n\"] + 1\n");
    let (ok, text) = run_trail(&client, json!({"script": plus_one, "args": {"n": 1}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n2\n"), "{text}");
    let run = ended_run(&text);
    let (ok, text) = save(&client, json!({"name": "plus-one", "run": run.as_str()})).await;
    assert!(ok && text.starts_with(&format!("Saved trail plus-one for the colony as this ant's, from run {run}.")));
    let trail = colony_trail(&lead.js, "plus-one").await.unwrap();
    assert_eq!((trail.script.as_str(), trail.saved_from.as_ref()), (plus_one.as_str(), Some(&run)));

    let failing = format!("{META}def f():\n    raise ValueError('bad')\nf()\n");
    let (ok, text) = run_trail(&client, json!({"script": failing, "wait": true})).await;
    assert!(ok && text.contains("failed"), "{text}");
    let failed = ended_run(&text);
    let (ok, text) = save(&client, json!({"name": "failing", "run": failed.as_str()})).await;
    assert!(ok && text.contains(&format!("from run {failed} (which failed: ")), "{text}");

    let (ok, text) = run_trail(&client, json!({"script": format!("{META}await agent(\"go\", ant=\"dev\")\n")})).await;
    assert!(ok, "{text}");
    let running = started(&text);
    let (ok, text) = run_trail(&client, json!({"name": "tenfold", "args": {"n": 1}, "wait": true})).await;
    assert!(ok, "{text}");
    let of_saved = ended_run(&text);
    let (ok, text) = run_trail(&other_client, json!({"script": plus_one, "args": {"n": 1}, "wait": true})).await;
    assert!(ok, "{text}");
    let others = ended_run(&text);
    let unknown = RunId::generate();
    for (args, why) in [
        (json!({"name": "x", "run": running.as_str()}), format!("run {running} has not finished (running)")),
        (json!({"name": "x", "run": of_saved.as_str()}), format!("run {of_saved} ran saved trail tenfold")),
        (json!({"name": "x", "run": unknown.as_str()}), format!("no run {unknown} conducted by this ant")),
        (json!({"name": "x", "run": others.as_str()}), format!("no run {others} conducted by this ant")),
        (json!({"name": "x", "run": "Not A Run"}), "\"Not A Run\" is no run id".to_owned()),
        (json!({"name": "x", "script": tenfold, "run": run.as_str()}), "give `script` or `run`, not both".to_owned()),
        (json!({"name": "x"}), "give `script` or `run`".to_owned()),
        (json!({"name": "Bad Name", "script": tenfold}), "\"Bad Name\" is no trail name".to_owned()),
        (json!({"name": RunId::generate().as_str(), "script": tenfold}), "reads as a run's id".to_owned()),
        (json!({"name": "x", "script": "1\n"}), "meta".to_owned()),
        (json!({"name": "x", "script": format!("{META}def (\n")}), "the script does not compile".to_owned()),
        (
            json!({"name": "x", "script": format!("{META}#{}\n", "a".repeat(MAX_TRAIL_SCRIPT))}),
            format!("a script has 1-{MAX_TRAIL_SCRIPT} bytes"),
        ),
        (json!({"name": "local", "script": tenfold}), "this machine's trails hold one named local".to_owned()),
    ] {
        let (ok, text) = save(&client, args.clone()).await;
        assert!(!ok && text.contains(&why), "{args}: {why:?} in {text}");
    }
    for name in ["x", "local"] {
        assert_eq!(colony_trail(&lead.js, name).await, None, "{name} saved nothing");
    }
    serving.abort();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn an_ant_replaces_and_removes_only_the_trails_it_saved_on_trusted_threads_leaving_their_tips() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("save-tools-own");
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    std::fs::write(saved.join("local.py"), format!("{META}1\n")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved.clone())).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;
    let (client, _turn) = trusted(&mcp).await;
    let (_, other) = ant_mcp(&server, "dev-b", &dir, &dir.join("none")).await;
    let (other_client, _other_turn) = trusted(&other).await;
    let first = format!("{META}1\n");
    let second = format!("{META}2\n");

    let (ok, text) = save(&client, json!({"name": "deploy", "script": first})).await;
    assert!(ok, "{text}");
    let (ok, text) = save(&client, json!({"name": "deploy", "script": second})).await;
    assert!(!ok && text.contains("this ant saved trail deploy 1m ago: pass `replace: true`"), "{text}");
    let (ok, text) = save(&client, json!({"name": "deploy", "script": second, "replace": true})).await;
    assert!(ok && text.starts_with("Replaced trail deploy, which this ant saved 1m ago, with the script given."));
    assert_eq!(colony_trail(&lead.js, "deploy").await.unwrap().script, second);
    let (ok, text) = save(&other_client, json!({"name": "deploy", "script": first, "replace": true})).await;
    assert!(!ok && text.contains("trail deploy was saved by ant lead; an ant replaces only the trails it saved"));
    assert_eq!(colony_trail(&lead.js, "deploy").await.unwrap().script, second, "another's save changed nothing");

    let kv = DirectKv::new(&lead.js, None, TRAILS_BUCKET).await.unwrap();
    let mut human = colony_trail(&lead.js, "deploy").await.unwrap();
    (human.name, human.saved_by) = ("release".parse().unwrap(), Saver::Human { user: UserId::human() });
    kv.put("release", serde_json::to_vec(&human).unwrap()).await.unwrap();
    kv.put("broken", b"not a trail".to_vec()).await.unwrap();
    let (ok, text) = save(&client, json!({"name": "release", "script": first, "replace": true})).await;
    assert!(!ok && text.contains("trail release was saved by human; an ant replaces only"), "{text}");
    let (ok, text) = save(&client, json!({"name": "broken", "script": first, "replace": true})).await;
    assert!(!ok && text.contains("trail broken is saved but unreadable"), "{text}");

    let remove = async |client: &Client, name: &str| call(client, "remove_trail", json!({"name": name})).await;
    for (client, name, why) in [
        (
            &other_client,
            "deploy",
            "trail deploy was saved by ant lead; an ant removes only the trails it saved: ask a human",
        ),
        (&client, "release", "trail release was saved by human; an ant removes only the trails it saved"),
        (&client, "broken", "trail broken is saved but unreadable"),
        (&client, "absent", "the colony saved no trail absent"),
        (&client, "local", "is this machine's, no colony trail"),
        (&client, "Bad Name", "is no trail name"),
    ] {
        let (ok, text) = remove(client, name).await;
        assert!(!ok && text.contains(why), "{name}: {why:?} in {text}");
    }
    assert!(colony_trail(&lead.js, "release").await.is_some() && saved.join("local.py").exists());

    let asking = format!("{META}await agent(\"go\", ant=\"dev-b\")\n");
    let (ok, text) = save(&client, json!({"name": "ask-b", "script": asking})).await;
    assert!(ok, "{text}");
    let hooked = Envelope::new(ant("github"), ant("lead"), "an issue").from_webhook();
    let _hooked = mcp.begin(&hooked);
    let start = StartRun {
        trail: TrailSource::Saved("ask-b".parse().unwrap()),
        args: Value::Null,
        owner: Some(UserId::human()),
        origin: Origin { by: Starter::Webhook { user: ant("github") }, channel: None },
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
        spends_from: None,
    };
    lead.trails.start(start).await.unwrap();
    let step = lead.step().await;
    let _step = other.begin(&step);
    for (client, name) in
        [(mcp_client(&mcp, &hooked.thread_id).await, "deploy"), (mcp_client(&other, &step.thread_id).await, "mine")]
    {
        let (ok, text) = save(&client, json!({"name": name, "script": first, "replace": true})).await;
        assert!(!ok && text.contains("this thread is untrusted"), "{text}");
        let (ok, text) = remove(&client, name).await;
        assert!(!ok && text.contains("this thread is untrusted"), "{text}");
    }
    assert_eq!(colony_trail(&lead.js, "mine").await, None);
    assert_eq!(colony_trail(&lead.js, "deploy").await.unwrap().script, second);
    let idle = mcp_client(&mcp, &ThreadId::generate()).await;
    let (ok, text) = remove(&idle, "deploy").await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");

    let tip = TrailTip {
        trail: "deploy".parse().unwrap(),
        id: TipId::generate(),
        writer: ant("dev-b"),
        text: "Build before tagging".to_owned(),
        tip_key: None,
        run: RunId::generate(),
        script: script_sha256(&second),
        written_at: chrono::Utc::now(),
        retired: None,
    };
    lead.trails.tips().leave(&tip).await.unwrap();
    let (ok, text) = remove(&client, "deploy").await;
    assert!(ok && text.starts_with("Removed trail deploy, which this ant saved 1m ago. Its tips stay"), "{text}");
    assert_eq!(colony_trail(&lead.js, "deploy").await, None);
    let board = lead.trails.tips().board(&tip.trail).await.unwrap();
    assert!(board.tip(&tip.id, chrono::Utc::now(), None).is_some(), "the trail's tips stay");
    let (ok, text) = save(&client, json!({"name": "deploy", "script": first})).await;
    assert!(ok && text.starts_with("Saved trail deploy"), "a removed name is saved anew: {text}");
    std::fs::remove_dir_all(&dir).unwrap();
}
