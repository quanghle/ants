//! Rating trail runs and finding saved trails: a run's end invites its conductor to rate it unless a waited
//! `run_trail` answer told it, `rate_trail` rates the saved trails a run took or was saved as, and `find_trails` ranks
//! the saved trails for a task by their ratings.

use ants_proto::{
    RatingOutcome, TRAIL_RATINGS_BUCKET, TrailRating, TrailRunIndex, WEBHOOK_ALLOW, script_sha256, trail_rating_key,
    trail_runs_index_key,
};

use super::*;

type Client = RunningService<RoleClient, ClientConfig>;

/// A script described as `description`, answering its args plus one.
fn described(description: &str) -> String {
    format!("meta = {{\"name\": \"t\", \"description\": \"{description}\", \"phases\": [\"one\"]}}\nargs[\"n\"] + 1\n")
}

/// The id of the run `run_trail` says ended, with `wait`.
fn ended_run(text: &str) -> RunId {
    text.strip_prefix("Run ").unwrap().split_once(' ').unwrap().0.parse().unwrap()
}

/// An MCP client of `mcp` on the thread `message` starts, handling it.
async fn handling(mcp: &Mcp, message: &Envelope) -> (Client, ants_daemon::mcp::Turn) {
    let turn = mcp.begin(message);
    (mcp_client(mcp, &message.thread_id).await, turn)
}

async fn rate(client: &Client, run: &RunId, outcome: &str, task: &str, why: &str) -> (bool, String) {
    call(client, "rate_trail", json!({"run": run.as_str(), "outcome": outcome, "task": task, "why": why})).await
}

/// The ids of the runs whose end lists `index`, `lead`'s.
async fn listed(js: &async_nats::jetstream::Context) -> Vec<RunId> {
    let kv = DirectKv::new(js, None, TRAIL_RUNS_BUCKET).await.unwrap();
    let index = kv.get(trail_runs_index_key(&ant("lead"))).await.unwrap();
    index.map_or_default(|index| serde_json::from_slice::<TrailRunIndex>(&index).unwrap().runs)
}

impl Conductor {
    /// The next invitation to rate `run`, passing over those of other runs.
    async fn invitation_of(&mut self, run: &RunId) -> Envelope {
        loop {
            let invited = self.invitation().await;
            if invited.text.starts_with(&format!("[ants] Run {run} ")) {
                return invited;
            }
        }
    }

    /// Whether, after `wait`, no invitation to rate `run` came.
    async fn invites_not(&mut self, run: &RunId, wait: Duration) -> bool {
        assert!(self.sends_no_step(wait).await);
        !self.invited.iter().any(|invited| invited.text.starts_with(&format!("[ants] Run {run} ")))
    }
}

#[tokio::test]
async fn a_run_s_end_invites_its_conductor_to_rate_it_once_unless_a_waited_answer_did_or_it_was_stopped() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-invite");
    let saved = dir.join("saved");
    std::fs::create_dir_all(&saved).unwrap();
    std::fs::write(saved.join("deploy.py"), described("Deploy the web app")).unwrap();
    let mut lead = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved.clone())).await;
    let human = UserId::human();
    let start = StartRun {
        trail: TrailSource::Saved("deploy".parse().unwrap()),
        args: json!({"n": 1}),
        owner: Some(human.clone()),
        origin: Origin { by: Starter::Human { user: human.clone() }, channel: None },
        thread: ThreadId::generate(),
        limits: TrailLimits::default(),
    };
    let run = lead.trails.start(start.clone()).await.unwrap();
    assert_eq!(lead.ended(&run).await, RunState::Done { result: json!(2) });
    let invited = lead.invitation_of(&run).await;
    assert_eq!((&invited.from, &invited.to), (&ant("lead"), &ant("lead")), "the conductor invites itself");
    assert_eq!((&invited.thread_id, invited.owner.as_ref()), (&start.thread, Some(&human)), "on the run's thread");
    assert_eq!((invited.allow.as_ref(), invited.in_reply_to), (None, None), "a trusted run's, prompted");
    let text = &invited.text;
    assert!(text.starts_with(&format!("[ants] Run {run} of trail deploy ended done (")), "{text}");
    assert!(text.contains("Result:\n2\nArgs: {\"n\":1}\nrate_trail(run: "), "{text}");
    assert!(text.ends_with("\nAfter rating, end the turn with complete: nothing needs a reply."), "{text}");
    assert!(!text.contains("find_trails"), "a run that went well: {text}");
    assert!(!listed(&lead.js).await.contains(&run), "unlisted once the invitation is sent");

    let failing = lead.start("def f():\n    raise ValueError('bad')\nf()\n", TrailLimits::default()).await;
    assert!(matches!(lead.ended(&failing).await, RunState::Failed { .. }));
    let text = lead.invitation_of(&failing).await.text;
    assert!(text.contains(" of an inline script ended failed (") && text.contains("\nfind_trails(query) "), "{text}");

    let stopped = lead.start("await agent(\"go\", ant=\"dev\")\n", TrailLimits::default()).await;
    lead.step().await;
    lead.trails.stop(&stopped, &ant("lead")).await.unwrap();
    while !lead.told().await.text.contains("was stopped by lead") {}
    assert!(lead.invites_not(&stopped, Duration::from_millis(500)).await, "a stopped run is resumed, not rated");

    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;
    let asked = Envelope::new(human.clone(), ant("lead"), "deploy").with_owner(Some(human.clone()));
    let (client, _turn) = handling(&mcp, &asked).await;
    let (ok, text) = run_trail(&client, json!({"name": "deploy", "args": {"n": 4}, "wait": true})).await;
    assert!(ok && text.contains("is done. Result:\n5\nrate_trail(run: "), "the answer invites the rating: {text}");
    let waited = ended_run(&text);
    assert!(lead.invites_not(&waited, Duration::from_millis(500)).await, "the waited answer told the end");
    assert!(!listed(&lead.js).await.contains(&waited));

    let js = lead.js.clone();
    drop(lead);
    let kv = js.get_key_value(TRAIL_RUNS_BUCKET).await.unwrap();
    let index = TrailRunIndex { runs: vec![run.clone()] };
    kv.put(trail_runs_index_key(&ant("lead")), serde_json::to_vec(&index).unwrap().into()).await.unwrap();
    let mut again = conductor_saving(&server, &ants_binary(), Vec::new(), Some(saved)).await;
    let resent = again.invitation_of(&run).await;
    assert_eq!(
        resent.id, invited.id,
        "a daemon that stopped before unlisting the run invites again, as the same message"
    );
    eventually("the run is unlisted", async || !listed(&again.js).await.contains(&run)).await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn rate_trail_rates_the_saved_trails_a_run_took_and_find_trails_ranks_them_on_matching_tasks() {
    let Some(server) = NatsServer::start() else { return };
    init(&server).await;
    let dir = scratch_dir("trail-ratings");
    let mut lead = conductor(&server, &ants_binary(), Vec::new()).await;
    let mcp = lead_mcp(&dir, &lead.js, &lead.trails).await;
    let human = UserId::human();
    let asked = Envelope::new(human.clone(), ant("lead"), "deploy").with_owner(Some(human.clone()));
    let (client, _turn) = handling(&mcp, &asked).await;
    let web = described("Deploy the web app to staging");
    for (name, script) in [("deploy-web", web.as_str()), ("deploy-db", &described("Migrate the database schema"))] {
        let (ok, text) = call(&client, "save_trail", json!({"name": name, "script": script})).await;
        assert!(ok, "{text}");
    }

    let (ok, text) = run_trail(&client, json!({"name": "deploy-web", "args": {"n": 1}, "wait": true})).await;
    assert!(ok, "{text}");
    let first = ended_run(&text);
    let (ok, text) = rate(&client, &first, "worked", "deploying web apps", "it shipped").await;
    let rated = format!("Rated run {first} worked for trail deploy-web (task: \"deploying web apps\").");
    assert!(ok && text == rated, "{text}");
    let ratings = DirectKv::new(&lead.js, None, TRAIL_RATINGS_BUCKET).await.unwrap();
    let key = trail_rating_key(&"deploy-web".parse().unwrap(), &ant("lead"), &first);
    let stored: TrailRating = serde_json::from_slice(&ratings.get(&key).await.unwrap().unwrap()).unwrap();
    assert_eq!((stored.outcome, stored.why.as_str()), (RatingOutcome::Worked, "it shipped"));
    assert_eq!(stored.script, script_sha256(&web), "the script the run took");
    let (ok, text) = rate(&client, &first, "partly", "deploying web apps", "the notes needed a hand").await;
    assert!(ok && text.contains(" It replaces the earlier rating (worked)."), "{text}");
    assert!(text.ends_with("\nOther saved trails for this task: find_trails(query: \"deploying web apps\")."));

    for (run, outcome, task, why, refused) in [
        (RunId::generate(), "worked", "a task", "a reason", "lead conducts no run "),
        (first.clone(), "great", "a task", "a reason", "no outcome \"great\": use worked, partly or failed"),
        (first.clone(), "worked", " ", "a reason", "the task is empty"),
        (first.clone(), "worked", "a task", &"x".repeat(501), "the reason has 501 characters"),
    ] {
        let (ok, text) = rate(&client, &run, outcome, task, why).await;
        assert!(!ok && text.contains(refused), "{refused}: {text}");
    }
    let going = lead.start("await agent(\"go\", ant=\"dev\")\n", TrailLimits::default()).await;
    lead.step().await;
    let (ok, text) = rate(&client, &going, "worked", "a task", "a reason").await;
    assert!(!ok && text.contains("has not ended"), "{text}");

    let (ok, text) = run_trail(&client, json!({"name": "deploy-web", "args": {"n": 2}, "wait": true})).await;
    assert!(ok, "{text}");
    let second = ended_run(&text);
    assert!(rate(&client, &second, "worked", "deploy a web app", "smooth").await.0);
    let inline = described("Ship a web app to staging");
    let (ok, text) = run_trail(&client, json!({"script": inline, "args": {"n": 1}})).await;
    let well = "\nSaved trails with good records for such tasks: deploy-web (1 worked, 1 partly, 0 failed); \
                run_trail(name: …) runs one.";
    assert!(ok && text.ends_with(well), "a script described as a well rated trail is: {text}");
    let shipped = started(&text);
    lead.ended(&shipped).await;
    let (ok, text) = rate(&client, &shipped, "worked", "deploying web apps", "fine").await;
    assert!(ok && text.starts_with(&format!("Run {shipped} ran an inline script: ")), "{text}");
    let serves = "\nSaved trail deploy-web already serves such tasks (1 worked, 1 partly, 0 failed): \
                  run_trail(name: \"deploy-web\") next time.";
    assert!(text.ends_with(serves), "{text}");
    let (ok, text) = run_trail(&client, json!({"script": described("CI"), "args": {"n": 1}})).await;
    assert!(ok && text.ends_with("invites this ant's rating of it."), "a description without words names none: {text}");
    let wordless = started(&text);
    lead.ended(&wordless).await;
    let (ok, text) = rate(&client, &wordless, "worked", "CI", "green").await;
    assert!(ok && text.contains(&format!("\nsave_trail(run: \"{wordless}\", name: …) saves its script")), "{text}");
    assert!(!text.contains("already serves"), "a task without words is served by none: {text}");

    let (ok, text) = run_trail(&client, json!({"script": described("Count up"), "args": {"n": 1}, "wait": true})).await;
    assert!(ok, "{text}");
    let counted = ended_run(&text);
    let (ok, text) = rate(&client, &counted, "worked", "counting up by one", "right").await;
    assert!(ok && text.contains(&format!("\nsave_trail(run: \"{counted}\", name: …) saves its script")), "{text}");
    let (ok, text) = call(&client, "save_trail", json!({"name": "count-up", "run": counted.as_str()})).await;
    assert!(ok, "{text}");
    let (ok, text) = rate(&client, &counted, "worked", "counting up by one", "right").await;
    let rated = format!("Rated run {counted} worked for trail count-up (saved from its script) (task: ");
    assert!(ok && text.starts_with(&rated) && !text.contains("save_trail"), "{text}");

    let (ok, text) = call(&client, "find_trails", json!({"query": "deploy the web applications"})).await;
    assert!(ok, "{text}");
    let (head, rest) = text.split_once('\n').unwrap();
    assert!(head.starts_with("Saved trails for \"deploy the web applications\", ranked by a draw"), "{text}");
    let entry = "1. deploy-web: \"Deploy the web app to staging\"\n   saved by ant lead; success 75% over 2 ratings on \
                 matching tasks (1 worked, 1 partly, 0 failed)\n   - ";
    assert!(rest.starts_with(entry), "{text}");
    assert!(!text.contains("deploy-db") && !text.contains("count-up"), "only the trails matching: {text}");
    assert!(text.ends_with("once it works, save_trail saves it for the colony."), "{text}");
    let (ok, text) = call(&client, "find_trails", json!({"query": "migrating databases"})).await;
    assert!(
        ok && text.contains("\n1. deploy-db: \"Migrate the database schema\"\n   saved by ant lead; worth exploring")
    );
    for (args, refused) in [
        (json!({"query": "the"}), "the query has no words"),
        (json!({"query": "deploy", "limit": 21}), "limit 21: from 1 to 20"),
    ] {
        let (ok, text) = call(&client, "find_trails", args).await;
        assert!(!ok && text.contains(refused), "{text}");
    }

    untrusted_threads_rate_only_their_run(&mcp, &mut lead, &client, &first).await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// On a thread a webhook started, `lead` rates only the run reporting on it, `first` being another of its runs, which
/// the trusted `client` rates, and while handling the invitation to rate one of the runs reporting there, only that
/// one; `find_trails` reads from there all the same.
async fn untrusted_threads_rate_only_their_run(mcp: &Mcp, lead: &mut Conductor, client: &Client, first: &RunId) {
    let hooked = Envelope::new(ant("github"), ant("lead"), "an issue").with_owner(Some(UserId::human())).from_webhook();
    let (untrusted, _hooked) = handling(mcp, &hooked).await;
    let (ok, text) = run_trail(&untrusted, json!({"name": "deploy-web", "args": {"n": 1}})).await;
    assert!(ok, "{text}");
    let hooked_run = started(&text);
    lead.ended(&hooked_run).await;
    let invited = lead.invitation_of(&hooked_run).await;
    assert_eq!(invited.thread_id, hooked.thread_id);
    assert_eq!(invited.allow, Some(WEBHOOK_ALLOW.map(str::to_owned).to_vec()), "narrowed as the run's thread is");
    let (ok, text) = rate(&untrusted, &hooked_run, "failed", "triaging an issue", "wrong trail").await;
    assert!(ok && text.starts_with(&format!("Rated run {hooked_run} failed for trail deploy-web")), "{text}");
    let (ok, text) = rate(&untrusted, first, "failed", "triaging an issue", "wrong trail").await;
    assert!(!ok && text.contains("this thread is untrusted"), "another run than the thread's: {text}");
    let other = Envelope::new(ant("github"), ant("lead"), "another issue").from_webhook();
    let (elsewhere, _other) = handling(mcp, &other).await;
    let (ok, text) = rate(&elsewhere, &hooked_run, "worked", "triaging an issue", "right").await;
    assert!(!ok && text.contains("this thread is untrusted"), "{text}");
    let (ok, text) = rate(client, &hooked_run, "partly", "triaging an issue", "half right").await;
    assert!(ok && text.contains("It replaces the earlier rating (failed)."), "a trusted thread rates any: {text}");
    let (ok, text) = call(&untrusted, "find_trails", json!({"query": "deploying web apps"})).await;
    assert!(ok && text.contains("1. deploy-web") && !text.contains("save_trail"), "{text}");

    let (ok, text) = run_trail(&untrusted, json!({"name": "deploy-web", "args": {"n": 2}})).await;
    assert!(ok, "{text}");
    let again = started(&text);
    lead.ended(&again).await;
    assert_eq!(invited.rating_of.as_ref(), Some(&hooked_run), "the invitation names its run");
    let (inviting, _invited) = handling(mcp, &invited).await;
    let (ok, text) = rate(&inviting, &again, "worked", "triaging an issue", "right").await;
    assert!(!ok && text.contains("only that one"), "another run reporting on the thread: {text}");
    let (ok, text) = rate(&inviting, &hooked_run, "worked", "triaging an issue", "right").await;
    assert!(ok, "{text}");
}
