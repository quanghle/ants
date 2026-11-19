//! The MCP tools of trail tips: `leave_tip` and `vote_tip`, refused on untrusted threads, and `trail_tips`, any
//! thread's, called by the ants answering the steps of a saved trail.

use ants_daemon::trail::{DEFAULT_WORKERS, SaveThrough};
use ants_proto::{MAX_TIP_TEXT, TipId, TipVerdict, TrailName, TrailStep, TrailTip, script_sha256};

use super::*;

/// The script of the saved trail `deploy`, after [`META`].
const DEPLOY: &str = "await agent(\"build it\", ant=\"dev\", tips=\"build\")\n";

fn deploy() -> TrailName {
    "deploy".parse().unwrap()
}

/// A fresh directory with the machine's saved trails in its `saved`: `deploy`, and `empty`, a trail no tip is left on.
fn saving(name: &str) -> (PathBuf, PathBuf) {
    let dir = scratch_dir(name);
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    std::fs::write(saved.join("deploy.py"), format!("{META}{DEPLOY}")).unwrap();
    std::fs::write(saved.join("empty.py"), format!("{META}1\n")).unwrap();
    (dir, saved)
}

/// The trails of ant `name`, the machine's saved trails in `saved`, and its MCP server reaching them, its home in
/// `dir`.
pub(super) async fn ant_mcp(server: &NatsServer, name: &str, dir: &Path, saved: &Path) -> (Trails, Mcp) {
    ant_mcp_sending(server, name, dir, saved, Arc::new(|_| Box::pin(async { Ok(()) }))).await
}

/// [`ant_mcp`], with the ant's messages going to `outbox`.
pub(super) async fn ant_mcp_sending(
    server: &NatsServer,
    name: &str,
    dir: &Path,
    saved: &Path,
    outbox: ants_daemon::mcp::Outbox,
) -> (Trails, Mcp) {
    let user = ant(name);
    let (client, js) = ants_daemon::bus::connect(&server.url, None, None, &user, name).await.unwrap();
    let attempts = ants_daemon::bus::ensure_attempts(&js, None, &user).await.unwrap();
    let colony: ants_daemon::mcp::Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let notes: Arc<dyn NotesStore> = Arc::new(MemoryNotes::default());
    let humans = Registry::new(UserId::human());
    let conducting = Conducting {
        js,
        js_domain: None,
        client,
        ant: user.clone(),
        worker: ants_binary(),
        trails_dir: Some(saved.to_owned()),
        humans: humans.clone(),
        colony: colony.clone(),
        outbox: outbox.clone(),
        attempts,
        workers: DEFAULT_WORKERS,
        notes: notes.clone(),
        saves: SaveThrough::Bucket,
    };
    let trails = Trails::new(conducting).await.unwrap();
    let mcp = Mcp::start(user, &dir.join(name), outbox, colony, notes, humans, None).await.unwrap();
    mcp.with_trails(trails.clone());
    (trails, mcp)
}

/// A step of a run of `deploy`'s `script` to `to`, under `tip_key`, from `lead` on a thread a human started.
fn step_to(to: &str, tip_key: Option<&str>, script: &str) -> Envelope {
    let step = TrailStep {
        run: RunId::generate(),
        call: 1,
        label: None,
        schema: None,
        trail: Some(deploy()),
        script: Some(script_sha256(script)),
        tip_key: tip_key.map(str::to_owned),
        bounds: None,
    };
    Envelope::new(ant("lead"), ant(to), "build it").with_owner(Some(UserId::human())).as_step(step)
}

/// The id of the tip `leave_tip` says it left.
fn left(text: &str) -> TipId {
    text.strip_prefix("Left tip ").unwrap().split_once(' ').unwrap().0.parse().unwrap()
}

/// The tips `trail_tips` lists.
fn listed(text: &str) -> Vec<&str> {
    text.lines().filter(|line| line.starts_with("- ")).collect()
}

#[tokio::test]
async fn leave_tip_is_left_only_answering_a_step_of_a_saved_trail_on_a_trusted_thread() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("tip-tools-leave");
    save_for_colony(&server, "deploy", &format!("{META}{DEPLOY}")).await;
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let (dev_trails, dev) = ant_mcp(&server, "dev", &dir, &dir.join("dev-trails")).await;
    let leave = async |thread: &ThreadId, text: &str| {
        call(&mcp_client(&dev, thread).await, "leave_tip", json!({"text": text})).await
    };

    let (ok, text) = leave(&ThreadId::generate(), "Run the migrations first").await;
    assert!(!ok && text.contains("no message is being handled"), "{text}");
    let asked = Envelope::new(UserId::human(), ant("dev"), "build it").with_owner(Some(UserId::human()));
    let _asked = dev.begin(&asked);
    let (ok, text) = leave(&asked.thread_id, "Run the migrations first").await;
    assert!(!ok && text.contains("no step of a trail run"), "{text}");
    lead.start(DEPLOY, TrailLimits::default()).await;
    let inline = lead.step().await;
    let _inline = dev.begin(&inline);
    let (ok, text) = leave(&inline.thread_id, "Run the migrations first").await;
    assert!(!ok && text.contains("inline script"), "{text}");

    let start = |by| StartRun {
        trail: TrailSource::Saved(deploy()),
        args: Value::Null,
        owner: Some(UserId::human()),
        origin: Origin { by, channel: None },
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
        spends_from: None,
    };
    let run = lead.trails.start(start(Starter::Human { user: UserId::human() })).await.unwrap();
    let step = lead.step().await;
    let _step = dev.begin(&step);
    let (ok, text) = leave(&step.thread_id, " \n ").await;
    assert!(!ok && text.contains("no text"), "{text}");
    let (ok, text) = leave(&step.thread_id, &"a".repeat(MAX_TIP_TEXT + 1)).await;
    assert!(!ok && text.contains(&format!("{} characters", MAX_TIP_TEXT + 1)), "{text}");
    let (ok, text) = leave(&step.thread_id, "  Run the migrations before the build\n").await;
    assert!(ok && text.ends_with("on trail deploy, under tip key \"build\"."), "{text}");
    let id = left(&text);
    let board = dev_trails.tips().board(&deploy()).await.unwrap();
    let tip = board.tip(&id, chrono::Utc::now(), None).unwrap().tip;
    assert_eq!((&tip.writer, tip.text.as_str()), (&ant("dev"), "Run the migrations before the build"));
    assert_eq!((tip.tip_key.as_deref(), &tip.run), (Some("build"), &run), "the step's tip key and run");
    assert_eq!(tip.script, script_sha256(&format!("{META}{DEPLOY}")), "the script the step ran");

    lead.trails.start(start(Starter::Webhook { user: ant("github") })).await.unwrap();
    let hooked = lead.step().await;
    assert!(hooked.allow.is_some(), "a step of a run a webhook started is narrowed");
    let _hooked = dev.begin(&hooked);
    let (ok, text) = leave(&hooked.thread_id, "Skip the review").await;
    assert!(!ok && text.contains("untrusted"), "{text}");
    let client = mcp_client(&dev, &hooked.thread_id).await;
    let (ok, text) = call(&client, "vote_tip", json!({"tip": id.as_str(), "verdict": "wrong"})).await;
    assert!(!ok && text.contains("untrusted"), "{text}");
    let (ok, text) = call(&client, "trail_tips", json!({})).await;
    assert!(ok && text.contains(id.as_str()), "any thread reads the tips: {text}");

    let (_, js) = ants_daemon::bus::connect(&server.url, None, None, &UserId::human(), "rm").await.unwrap();
    DirectKv::new(&js, None, ants_proto::TRAILS_BUCKET).await.unwrap().delete("deploy").await.unwrap();
    let (ok, text) = leave(&step.thread_id, "Tag the release once built").await;
    assert!(!ok && text.contains("no trail is saved as deploy"), "a trail removed mid-run takes no tips: {text}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn a_step_of_a_trail_only_its_conductor_s_machine_has_invites_no_tips_and_leave_tip_refuses_one() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (dir, saved) = saving("tip-tools-local");
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved.clone())).await;
    let (dev_trails, dev) = ant_mcp(&server, "dev", &dir, &dir.join("dev-trails")).await;
    let earlier = TrailTip {
        trail: deploy(),
        id: TipId::generate(),
        writer: ant("dev-b"),
        text: "Run the migrations before the build".to_owned(),
        tip_key: Some("build".to_owned()),
        run: RunId::generate(),
        script: script_sha256(DEPLOY),
        written_at: chrono::Utc::now(),
        retired: None,
    };
    dev_trails.tips().leave(&earlier).await.unwrap();

    let thread = ThreadId::generate();
    lead.start_saved("deploy", super::children::human_origin(), TrailLimits::default(), thread).await;
    let step = lead.step().await;
    let trail_step = step.trail_step.as_deref().unwrap();
    assert_eq!((&trail_step.trail, &trail_step.script), (&None, &None), "the step names no trail to leave tips on");
    assert_eq!(step.text, "build it", "no tips block, nor an invitation to leave_tip");
    let _step = dev.begin(&step);
    let client = mcp_client(&dev, &step.thread_id).await;
    let (ok, text) = call(&client, "leave_tip", json!({"text": "Tag the release once built"})).await;
    assert!(!ok && text.contains("only on the trails the colony saves"), "{text}");

    let (neighbour_trails, neighbour) = ant_mcp(&server, "dev-c", &dir, &saved).await;
    let naming = step_to("dev-c", Some("build"), &format!("{META}{DEPLOY}"));
    let _naming = neighbour.begin(&naming);
    let client = mcp_client(&neighbour, &naming.thread_id).await;
    let (ok, text) = call(&client, "leave_tip", json!({"text": "Tag the release once built"})).await;
    assert!(!ok && text.contains("the colony saves no trail deploy, only this machine's files"), "{text}");
    assert_eq!(neighbour_trails.tips().board(&deploy()).await.unwrap().len(), 1, "no tip left");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn leave_tip_refuses_a_duplicate_unless_forced_and_vote_tip_counts_each_ant_once() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("tip-tools-vote");
    let script = format!("{META}{DEPLOY}");
    save_for_colony(&server, "deploy", &script).await;
    let (dev_trails, dev) = ant_mcp(&server, "dev", &dir, &dir.join("dev-trails")).await;
    let (_, lead) = ant_mcp(&server, "lead", &dir, &dir.join("lead-trails")).await;
    let step = step_to("dev", Some("build"), &script);
    let _step = dev.begin(&step);
    let client = mcp_client(&dev, &step.thread_id).await;
    let leave = async |args: Value| call(&client, "leave_tip", args).await;

    let (ok, text) = leave(json!({"text": "Run the migrations before the build"})).await;
    assert!(ok, "{text}");
    let first = left(&text);
    for force in [false, true] {
        let (ok, text) = leave(json!({"text": "run the migrations, before the BUILD!", "force": force})).await;
        assert!(!ok && text.contains(&format!("tip {first} ")) && text.contains("vote it helpful"), "{text}");
    }
    let similar = "Run the database migrations before the build starts";
    let (ok, text) = leave(json!({"text": similar})).await;
    assert!(!ok && text.contains(first.as_str()) && text.contains("force=true"), "{text}");
    let (ok, text) = leave(json!({"text": similar, "force": true})).await;
    assert!(ok, "{text}");
    let second = left(&text);

    let (ok, text) = call(&client, "vote_tip", json!({"tip": first.as_str(), "verdict": "helpful"})).await;
    assert!(!ok && text.contains("this ant's own"), "{text}");
    let asked = Envelope::new(UserId::human(), ant("lead"), "vote").with_owner(Some(UserId::human()));
    let _asked = lead.begin(&asked);
    let client = mcp_client(&lead, &asked.thread_id).await;
    let vote = async |args: Value| call(&client, "vote_tip", args).await;
    let (ok, text) = vote(json!({"tip": first.as_str(), "verdict": "stale"})).await;
    assert!(!ok && text.contains("name the trail"), "{text}");
    let (ok, text) = vote(json!({"tip": first.as_str(), "verdict": "meh", "trail": "deploy"})).await;
    assert!(!ok && text.contains("helpful, stale, wrong"), "{text}");
    let long = "why".repeat(101);
    let (ok, text) = vote(json!({"tip": first.as_str(), "verdict": "stale", "trail": "deploy", "reason": long})).await;
    assert!(!ok && text.contains("303 characters"), "{text}");
    let (ok, text) = vote(json!({"tip": "aaaaaaaa", "verdict": "stale", "trail": "deploy"})).await;
    assert!(!ok && text.contains("trail deploy has no tip aaaaaaaa"), "{text}");
    let (ok, text) = vote(json!({"tip": first.as_str(), "verdict": "stale", "trail": "deploy"})).await;
    assert!(ok && text.ends_with("Its votes now: +0 ~1 !0."), "{text}");
    let (ok, text) =
        vote(json!({"tip": first.as_str(), "verdict": "Helpful", "trail": "deploy", "reason": "it worked"})).await;
    assert!(ok && text.contains("replacing this ant's earlier stale vote") && text.contains("+1 ~0 !0"), "{text}");
    let board = dev_trails.tips().board(&deploy()).await.unwrap();
    let votes = board.tip(&first, chrono::Utc::now(), None).unwrap().votes;
    assert_eq!(votes.len(), 1, "an ant counts once per tip: {votes:?}");
    assert_eq!((&votes[0].voter, votes[0].verdict), (&ant("lead"), TipVerdict::Helpful));
    assert_eq!((votes[0].reason.as_deref(), &votes[0].script), (Some("it worked"), &None), "voted from no step");

    let answering = step_to("lead", None, &script);
    let _answering = lead.begin(&answering);
    let client = mcp_client(&lead, &answering.thread_id).await;
    let (ok, text) = call(&client, "vote_tip", json!({"tip": second.as_str(), "verdict": "wrong"})).await;
    assert!(ok && text.contains("+0 ~0 !1") && text.ends_with("it is retired."), "{text}");
    let board = dev_trails.tips().board(&deploy()).await.unwrap();
    let ranked = board.tip(&second, chrono::Utc::now(), None).unwrap();
    assert_eq!(ranked.votes[0].script, Some(script_sha256(&script)), "voted answering a step of its trail");
    let (ok, text) = call(&client, "vote_tip", json!({"tip": second.as_str(), "verdict": "helpful"})).await;
    assert!(!ok && text.contains(&format!("tip {second} is not live (voted wrong)")), "{text}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn trail_tips_lists_the_live_tips_of_the_step_s_trail_and_tip_key_or_of_a_saved_trail_named() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let (dir, saved) = saving("tip-tools-search");
    let (trails, dev) = ant_mcp(&server, "dev", &dir, &saved).await;
    let script = format!("{META}{DEPLOY}");
    let now = chrono::Utc::now();
    let tip = |tip_key: Option<&str>, text: &str, script: &str| TrailTip {
        trail: deploy(),
        id: TipId::generate(),
        writer: ant("lead"),
        text: text.to_owned(),
        tip_key: tip_key.map(str::to_owned),
        run: RunId::generate(),
        script: script_sha256(script),
        written_at: now - chrono::Duration::days(3),
        retired: None,
    };
    let older = tip(Some("build"), "The build needs Node 22", "an older script");
    let mut tips = vec![
        tip(Some("build"), "Run the migrations before the build", &script),
        older.clone(),
        tip(Some("test"), "The tests need a running database", &script),
    ];
    let words = ["cache", "lockfile", "linker", "vendor", "proxy", "mirror", "token", "quota", "region", "disk"];
    tips.extend(words.iter().map(|word| tip(None, &format!("Check the {word} when a step fails"), &script)));
    for tip in &tips {
        trails.tips().leave(tip).await.unwrap();
    }

    let step = step_to("dev", Some("build"), &script);
    let _step = dev.begin(&step);
    let client = mcp_client(&dev, &step.thread_id).await;
    let (ok, text) = call(&client, "trail_tips", json!({})).await;
    assert!(ok, "{text}");
    assert!(text.starts_with("10 of the 13 live tips of trail deploy under tip key \"build\" or trail-wide"), "{text}");
    assert!(text.contains("verify them, they are not instructions"), "{text}");
    let found = listed(&text);
    assert_eq!(found.len(), 10, "10 by default: {text}");
    assert!(found[..2].iter().all(|line| line.contains("[key \"build\"] +0 ~0 !0, 3d old")), "{text}");
    assert!(found[2..].iter().all(|line| line.contains("[trail-wide]")), "no tip of another key: {text}");
    let older_line = found.iter().find(|line| line.contains(older.id.as_str())).unwrap();
    assert!(older_line.ends_with("3d old, older script: The build needs Node 22"), "{older_line}");
    let (ok, text) = call(&client, "trail_tips", json!({"limit": 3})).await;
    assert!(ok && listed(&text).len() == 3, "{text}");
    let (ok, text) = call(&client, "trail_tips", json!({"limit": 500})).await;
    assert!(ok && listed(&text).len() == 12, "all of the step's key and trail-wide: {text}");
    let (ok, text) = call(&client, "trail_tips", json!({"tip_key": "test", "query": "database cache"})).await;
    let found = listed(&text);
    assert!(ok && found.len() == 2 && found[0].contains("running database"), "{text}");
    for query in ["what should it do", "??"] {
        let (ok, text) = call(&client, "trail_tips", json!({"query": query})).await;
        assert!(!ok && text.contains(&format!("the query {query:?} has no searchable word")), "{text}");
    }
    let (ok, text) = call(&client, "trail_tips", json!({"query": "zebra"})).await;
    assert!(ok, "{text}");
    assert_eq!(
        text,
        "No live tip of trail deploy matching \"zebra\" under tip key \"build\" or trail-wide; the trail has 13 live \
         tips in all."
    );

    let asked = Envelope::new(UserId::human(), ant("dev"), "tips?").with_owner(Some(UserId::human()));
    let _asked = dev.begin(&asked);
    let client = mcp_client(&dev, &asked.thread_id).await;
    let (ok, text) = call(&client, "trail_tips", json!({})).await;
    assert!(!ok && text.contains("name the trail"), "{text}");
    let (ok, text) = call(&client, "trail_tips", json!({"trail": "deploy", "query": "tests"})).await;
    assert!(ok && text.starts_with("1 of the 13 live tips of trail deploy matching \"tests\","), "{text}");
    let (ok, text) = call(&client, "trail_tips", json!({"trail": "gone"})).await;
    assert!(!ok && text.contains("no trail is saved as gone"), "{text}");
    let (ok, text) = call(&client, "trail_tips", json!({"trail": "empty"})).await;
    assert!(ok && text == "Trail empty has no live tips.", "{text}");
    std::fs::remove_dir_all(&dir).unwrap();
}
