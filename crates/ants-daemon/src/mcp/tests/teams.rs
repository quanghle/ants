//! Teams in the tools: a message to a team reaches its lead carrying the team, `list_ants` and `route` keep to a
//! team's ants, `list_teams` draws the forest, and `manage_team` asks for the changes of a team the ant manages.

use ants_proto::{Team, TeamAdminOutcome, TeamDirectory};

use super::*;
use crate::mcp::TeamAccess;
use crate::teams::Read;

/// eng (lead boss) holding backend (lead dev-a, with dev-b), and orphans, whose lead was revoked.
fn forest() -> TeamDirectory {
    let mut directory = TeamDirectory::default();
    let mut eng = Team::new(user("eng"), user("boss"), UserId::human());
    eng.set_description(Some("Builds the shop.")).unwrap();
    directory.create(eng, None).unwrap();
    directory.create(Team::new(user("backend"), user("dev-a"), UserId::human()), Some(&user("eng"))).unwrap();
    directory.add(&user("backend"), &user("dev-b")).unwrap();
    directory.create(Team::new(user("orphans"), user("gone"), UserId::human()), None).unwrap();
    directory.drop_ant(&user("gone"));
    directory
}

/// The teams in memory, changed for dev-a as `ants signer` would, any name an ant of the bus; with `silent` no signer
/// answers.
fn memory_teams(directory: TeamDirectory, silent: bool) -> (TeamAccess, Arc<StdMutex<TeamDirectory>>) {
    let shared = Arc::new(StdMutex::new(directory));
    let read = shared.clone();
    let changed = shared.clone();
    let access = TeamAccess {
        read: Arc::new(move || {
            let directory = read.locked().clone();
            Box::pin(async move { Ok(Some(Read { directory: Ok(directory), last: 1 })) })
        }),
        change: Arc::new(move |team, ops, _| {
            let outcome =
                (!silent).then(|| match changed.locked().apply_for_ant(&user("dev-a"), &team, &ops, |_| true) {
                    Ok(changed) => TeamAdminOutcome::Applied { last: 2, changed },
                    Err(refusal) => TeamAdminOutcome::Refused { refusal },
                });
            Box::pin(async move { Ok(outcome) })
        }),
    };
    (access, shared)
}

#[tokio::test]
async fn a_message_to_a_team_goes_to_its_lead_carrying_the_team() {
    let home = home("team-send");
    let (mcp, mut sent) = start(&home).await;
    mcp.with_teams(memory_teams(forest(), false).0);
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "@eng", "text": "ship it"})).await;
    assert!(ok && text.starts_with("Sent to boss, the lead of team eng, on thread"), "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.to, &out.team, out.text.as_str()), (&user("boss"), &Some(user("eng")), "ship it"));
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "dev-b", "text": "hi"})).await;
    assert!(ok, "{text}");
    assert_eq!(sent.recv().await.unwrap().team, None, "an ant is no team");

    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "backend", "text": "?"})).await;
    assert!(!ok && text.starts_with("team backend sends to its lead, this ant;"), "{text}");
    let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "orphans", "text": "?"})).await;
    assert!(!ok && text.starts_with("nothing sent: team orphans has no lead since its lead was revoked"), "{text}");

    let asking = tokio::spawn(async move {
        let (ok, text) = call(&client, "ask_ant", serde_json::json!({"to": "eng", "text": "when?"})).await;
        (client, ok, text)
    });
    let ask = sent.recv().await.unwrap();
    assert_eq!((&ask.to, &ask.team), (&user("boss"), &Some(user("eng"))));
    let reply = ask.reply(user("boss"), "friday", Role::Ant).unwrap();
    assert_eq!(reply.team, None, "a reply is no message to the team");
    mcp.hub.route(reply.clone()).await;
    let (client, ok, text) = asking.await.unwrap();
    assert!(ok && text == format!("boss replied (message {}):\nfriday", reply.id), "{text}");

    mcp.with_lookup(super::routing::lookup_in(Vec::new()));
    let (ok, text) = call(&client, "send_message", serde_json::json!({"to": "engs", "text": "hi"})).await;
    let closest = "Closest teams, which messages reach through their leads: eng.";
    assert!(ok && text.contains("engs is not in the colony") && text.ends_with(closest), "a misspelt team: {text}");
    assert_eq!(sent.recv().await.unwrap().to, user("engs"));
    assert!(sent.try_recv().is_err());
}

#[tokio::test]
async fn list_ants_and_route_keep_to_a_team_s_ants() {
    let colony = vec![listed("dev-a", &["rust"]), listed("aaa", &["rust"]), listed("dev-b", &["rust"])];
    let home = home("team-route");
    let (mcp, mut sent) = start_with_colony(&home, colony).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "list_ants", serde_json::json!({"team": "eng"})).await;
    assert!(!ok && text.contains("the bus keeps no teams"), "{text}");

    mcp.with_teams(memory_teams(forest(), false).0);
    let (ok, text) = call(&client, "list_ants", serde_json::json!({"team": "eng"})).await;
    assert!(ok && text.starts_with("dev-b: rust") && !text.contains("aaa"), "through member team backend: {text}");
    let (ok, text) = call(&client, "list_ants", serde_json::json!({"team": "orphans"})).await;
    assert_eq!((ok, text.as_str()), (true, "No other ant of team orphans is in the colony."));
    let (ok, text) = call(&client, "list_ants", serde_json::json!({"team": "nope"})).await;
    assert!(!ok && text.starts_with("there is no team nope"), "{text}");

    let route =
        |team: Option<&str>| serde_json::json!({"capability": "rust", "text": "fix", "wait": false, "team": team});
    let (ok, text) = call(&client, "route", route(None)).await;
    assert!(ok && text.starts_with("Routed to aaa for rust."), "{text}");
    sent.recv().await.unwrap();
    let (ok, text) = call(&client, "route", route(Some("backend"))).await;
    assert!(ok && text.starts_with("Routed to dev-b for rust."), "{text}");
    let out = sent.recv().await.unwrap();
    assert_eq!((&out.to, &out.team), (&user("dev-b"), &None), "routed to an ant, not to the team");
    let (ok, text) = call(&client, "route", route(Some("orphans"))).await;
    assert!(!ok && text.starts_with("Within team orphans: No other ant in the colony has capability"), "{text}");
}

#[tokio::test]
async fn list_teams_draws_the_forest_with_the_members_states() {
    let home = home("team-list");
    let (mcp, _sent) = start_with_colony(&home, vec![listed("dev-b", &[])]).await;
    let current = from_human();
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    mcp.with_teams(memory_teams(forest(), false).0);

    let (ok, text) = call(&client, "list_teams", serde_json::json!({})).await;
    let forest = "dev-a leads team backend.
team eng (lead boss)
  | Builds the shop.
  - boss (lead): not in the colony
  - team backend (lead dev-a)
    - dev-a (lead): this ant
    - dev-b: idle on m1
team orphans (lead none, its lead was revoked)";
    assert_eq!((ok, text.as_str()), (true, forest));
    let (ok, text) = call(&client, "list_teams", serde_json::json!({"team": "@backend"})).await;
    let backend = "Within team eng.\nteam backend (lead dev-a)\n  - dev-a (lead): this ant\n  - dev-b: idle on m1";
    assert_eq!((ok, text.as_str()), (true, backend));
    let (ok, text) = call(&client, "list_teams", serde_json::json!({"team": "nope"})).await;
    assert!(!ok && text.starts_with("there is no team nope"), "{text}");
}

#[tokio::test]
async fn manage_team_asks_for_the_changes_of_a_team_the_ant_manages() {
    let home = home("team-manage");
    let (mcp, _sent) = start(&home).await;
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "manage_team", serde_json::json!({})).await;
    assert!(!ok && text.contains("cannot be managed"), "{text}");

    let (access, directory) = memory_teams(forest(), false);
    mcp.with_teams(access);
    let (ok, text) = call(&client, "manage_team", serde_json::json!({})).await;
    assert_eq!((ok, text.as_str()), (true, "dev-a manages:\n- team backend"));
    let args = serde_json::json!({"team": "backend", "add": ["dev-c"], "description": "The services."});
    let (ok, text) = call(&client, "manage_team", args).await;
    assert!(
        ok && text.starts_with("Changed team backend: added dev-c to team backend; described team backend.\n"),
        "{text}"
    );
    assert!(text.contains("  | The services.\n") && text.ends_with("  - dev-c: not in the colony"), "{text}");
    assert!(directory.locked().team(&user("backend")).unwrap().is_member(&user("dev-c")));
    let create = serde_json::json!({"team": "backend", "create": {"team": "db", "lead": "dev-c"}});
    let (ok, text) = call(&client, "manage_team", create).await;
    assert!(ok && text.contains("created team db, led by dev-c; moved dev-c from team backend to team db"), "{text}");
    let (ok, text) = call(&client, "manage_team", serde_json::json!({})).await;
    assert_eq!((ok, text.as_str()), (true, "dev-a manages:\n- team backend, and beneath it: db"));

    let (ok, text) = call(&client, "manage_team", serde_json::json!({"team": "eng", "add": ["dev-d"]})).await;
    assert_eq!(
        (ok, text.as_str()),
        (false, "team eng was not changed: dev-a manages neither team eng nor a team above it")
    );
    let (ok, text) = call(&client, "manage_team", serde_json::json!({"team": "backend", "remove": ["dev-a"]})).await;
    assert!(!ok && text.contains("dev-a leads team backend; name another lead first"), "{text}");
    let both = serde_json::json!({"team": "backend", "description": "x", "clear_description": true});
    let (ok, text) = call(&client, "manage_team", both).await;
    assert!(!ok && text.contains("not both"), "{text}");

    let hooked = Envelope::new(user("github"), user("dev-a"), "go").from_webhook();
    let _hooked = mcp.begin(&hooked);
    let (url, token) = http_server(&mcp, &hooked.thread_id);
    let untrusted = http_client(&url, &token).await;
    let (ok, text) = call(&untrusted, "manage_team", serde_json::json!({"team": "backend", "add": ["dev-d"]})).await;
    assert!(!ok && text.starts_with("this thread is untrusted"), "{text}");
    let (ok, text) = call(&untrusted, "manage_team", serde_json::json!({"team": "backend"})).await;
    assert!(ok && text.contains("team backend (lead dev-a)"), "showing a team needs no trust: {text}");
}

#[tokio::test]
async fn manage_team_tells_when_no_signer_answers() {
    let home = home("team-silent");
    let (mcp, _sent) = start(&home).await;
    mcp.with_teams(memory_teams(forest(), true).0);
    let current = from_human();
    let _turn = mcp.begin(&current);
    let (url, token) = http_server(&mcp, &current.thread_id);
    let client = http_client(&url, &token).await;
    let (ok, text) = call(&client, "manage_team", serde_json::json!({"team": "backend", "add": ["dev-c"]})).await;
    assert!(!ok && text.starts_with("no signer answered in 10s (start `ants signer`)"), "{text}");
}
