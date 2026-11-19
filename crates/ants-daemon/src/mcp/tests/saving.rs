//! `save_trail` on a dream's thread or in answer to a dream finding: checked against the ant's dreams, which sense
//! keeps, counted as one of the dream's writes, and named in the verdict applying the finding.

use ants_proto::{RatingOutcome, RunId, TrailRating, script_sha256};
use ants_test_support::NatsServer;
use serde_json::json;

use super::*;
use crate::provider::context::Callers;
use crate::trail::{Conducting, DEFAULT_WORKERS, Ratings, Trails};

/// A script saved as a trail described as `description`.
fn script(description: &str) -> String {
    format!("meta = {{\"name\": \"t\", \"description\": \"{description}\", \"phases\": [\"one\"]}}\n1\n")
}

/// The trails `dev-a` conducts, its saved trails in the hub `server` runs.
async fn conducting(server: &NatsServer) -> (Trails, async_nats::jetstream::Context) {
    let (client, js) = crate::bus::connect(&server.url, None, None, &user("dev-a"), "test").await.unwrap();
    let configs = [
        crate::hub::trails_config(),
        crate::hub::trail_runs_config(),
        crate::hub::trail_tips_config(),
        crate::hub::trail_ratings_config(),
    ];
    for config in configs {
        js.create_key_value(config).await.unwrap();
    }
    let outbox: Outbox = Arc::new(|_| Box::pin(async { Ok(()) }));
    let colony: Colony = Arc::new(|| Box::pin(async { Ok(Vec::new()) }));
    let conducting = Conducting {
        js: js.clone(),
        js_domain: None,
        client,
        ant: user("dev-a"),
        worker: PathBuf::from("ants"),
        trails_dir: None,
        humans: Registry::default(),
        colony,
        outbox,
        attempts: crate::bus::ensure_attempts(&js, None, &user("dev-a")).await.unwrap(),
        workers: DEFAULT_WORKERS,
        notes: Arc::new(MemoryNotes::default()),
        saves: crate::trail::SaveThrough::Bucket,
    };
    (Trails::new(conducting).await.unwrap(), js)
}

/// A sage's trail finding citing `thread`, suggesting trail `name` described as `description`.
fn trail_finding(thread: &ThreadId, name: &str, description: &str) -> serde_json::Value {
    json!({
        "kind": "trail",
        "finding": format!("{name} was done by hand again and again"),
        "evidence": {"threads": [thread.to_string()]},
        "action": "Save it as a trail.",
        "trail": {"name": name, "description": description, "phases": ["Gather", "Write"]},
    })
}

async fn saved_names(trails: &Trails) -> Vec<String> {
    trails.saved_trails().await.unwrap().into_iter().map(|trail| trail.name.to_string()).collect()
}

#[tokio::test]
async fn a_trail_saved_for_a_dream_finding_stands_on_trusted_ground_and_counts_as_a_dream_write() {
    let Some(server) = NatsServer::start() else { return };
    let home = home("saving");
    let (mcp, _sent) = start(&home).await;
    let (trails, js) = conducting(&server).await;
    mcp.with_trails(trails.clone());
    let sense = crate::sense::Sense::new(crate::provider::wake::Wakers::default());
    let _hosting = sense.keeping(&js, &user("dev-a"), &home, "id-1").await;
    mcp.with_dreams(sense.dreams());
    mcp.with_provider(crate::provider::connect(sense.provider(), &Default::default()).await.unwrap());
    mcp.with_callers(Callers::known(user("dev-a"), "m1", "id-1"));

    let asked = from_human();
    let (url, token) = http_server(&mcp, &asked.thread_id);
    let human = http_client(&url, &token).await;
    mcp.settle_origin(&asked, None).await.unwrap();
    let _asked = mcp.begin(&asked);
    let served = "Draft the release notes of a repository";
    let (ok, text) = call(&human, "save_trail", json!({"name": "notes-draft", "script": script(served)})).await;
    assert!(ok && !text.contains("Dream"), "off a dream's thread: {text}");
    let (ok, text) = call(&human, "save_trail", json!({"name": "x", "script": script("x"), "finding": "1.1"})).await;
    assert!(!ok && text.contains("No dream has a finding \"1.1\"."), "{text}");
    let ratings = Ratings::new(&js, None, user("dev-a")).await.unwrap();
    let rating = TrailRating {
        trail: "notes-draft".parse().unwrap(),
        conductor: user("dev-a"),
        run: RunId::generate(),
        outcome: RatingOutcome::Worked,
        task: served.to_owned(),
        why: "Drafted them.".to_owned(),
        script: script_sha256(&script(served)),
        untrusted: false,
        at: chrono::Utc::now(),
    };
    ratings.put(&rating).await.unwrap();

    let (trusted, webhook) = (ThreadId::generate(), ThreadId::generate());
    let answer = format!(
        "```json\n{}\n```",
        json!({"findings": [
            trail_finding(&trusted, "deploy-check", "Check a deployment of a service before shipping it"),
            trail_finding(&webhook, "triage", "Triage an incoming issue"),
            trail_finding(&trusted, "release-notes", &format!("{served} from its merged pull requests")),
            {
                "kind": "lesson",
                "finding": "Deploys are checked by hand",
                "evidence": {"threads": [trusted.to_string()]},
                "action": "Remember it.",
            },
        ]})
    );
    let dream = sense.reviewed(&user("dev-a"), &user("sage"), &[(trusted, false), (webhook, true)], &answer).await;
    let mut findings = Envelope::new(user("sage"), user("dev-a"), "findings");
    findings.thread_id = dream.clone();
    let (url, token) = http_server(&mcp, &dream);
    let client = http_client(&url, &token).await;
    mcp.settle_origin(&findings, None).await.unwrap();
    let _findings = mcp.begin(&findings);

    let save = async |name: &str, finding: Option<&str>| {
        let mut args = json!({"name": name, "script": script("Check a deployment")});
        if let Some(finding) = finding {
            args["finding"] = json!(finding);
        }
        call(&client, "save_trail", args).await
    };
    let refused = [
        (Some("1.2"), "Refused: finding 1.2 rests on untrusted text; trails are saved only on trusted ground."),
        (None, "dream 1 offers untrusted finding 1.2 and no finding of it is named in `finding`"),
        (Some("1.9"), "No dream has a finding \"1.9\"."),
        (Some("1.4"), "dream 1 offers untrusted finding 1.2 and finding 1.4 is no trail finding"),
        (
            Some("1.3"),
            "Refused: saved trail notes-draft already serves such tasks (1 worked, 0 partly, 0 failed): \
                       reject finding 1.3 with sense_dream naming notes-draft",
        ),
    ];
    for (finding, why) in refused {
        let (ok, text) = save("refused", finding).await;
        assert!(!ok && text.contains(why), "{finding:?}: {why:?} in {text}");
    }
    assert_eq!(saved_names(&trails).await, ["notes-draft"], "nothing saved when refused");

    let (ok, text) = save("deploy-check", Some("1.1")).await;
    assert!(ok, "{text}");
    assert!(
        text.ends_with("\nDream 1: 1 of 20 writes made. Name deploy-check in the trails of finding 1.1's verdict.")
    );
    assert_eq!(saved_names(&trails).await, ["deploy-check", "notes-draft"]);

    let verdicts = json!({"verdicts": [
        {"finding": "1.1", "verdict": "applied", "reason": "Saved it.", "trails": ["deploy-check"]},
        {"finding": "1.3", "verdict": "rejected", "reason": "notes-draft serves it."},
    ]});
    let (ok, text) = call(&client, "sense_dream", verdicts).await;
    assert!(ok, "{text}");
    let (ok, text) = call(&client, "sense_dream", json!({})).await;
    assert!(ok, "{text}");
    assert!(text.contains("1 of 20 writes made"), "{text}");
    assert!(text.contains("applied: Saved it. (trails deploy-check)"), "{text}");
    assert!(text.contains("\nSaved on this thread: deploy-check (for 1.1)."), "{text}");
}
