//! The watcher trail of contrib/skills/ants-pr-watch/SKILL.md, run as a watcher's check with the real worker and a
//! fake host answering its command() reads as gh and a Forgejo API would, for each state of a pull request: what it
//! tells blocks the pull request, when it is done, that every command it runs is one the skill's allow rules let run,
//! and that the skill's watch trigger runs it.

use std::sync::Mutex;

use ants_proto::{CallAnswer, ToolCall, TrailFunction};
use ants_trail::Worker;
use serde_json::{Value, json};

use super::host::{blocks, examples};

const PR_WATCH: &str = include_str!("../../../../contrib/skills/ants-pr-watch/SKILL.md");

/// The text of each fenced block of the skill in `lang`.
fn blocks_in(lang: &str) -> Vec<String> {
    blocks(PR_WATCH).into_iter().filter(|(l, _)| *l == lang).map(|(_, text)| text).collect()
}

/// The skill's trail with its GitHub args, its Forgejo args, and its allow rules.
fn skill() -> (String, Value, Value, Vec<String>) {
    let examples = examples(PR_WATCH);
    let [(script, github)] = examples.as_slice() else { panic!("the skill has {} trails", examples.len()) };
    let meta = ants_trail::check(script).unwrap_or_else(|e| panic!("{script}\n{e}"));
    assert_eq!(meta.name, "pr-blocked");
    let json: Vec<Value> =
        blocks_in("json").iter().map(|b| serde_json::from_str(b).unwrap_or_else(|e| panic!("{b}: {e}"))).collect();
    let forgejo = json.iter().find(|b| b.get("api").is_some()).expect("the skill gives Forgejo args").clone();
    let create = json.iter().find(|b| b["action"] == "create").expect("the skill creates a watcher");
    let run = &create["trigger"]["run"];
    assert_eq!(create["trigger"]["kind"], "watch");
    assert_eq!((&run["kind"], &run["name"], &run["args"]), (&json!("trail"), &json!(meta.name), github));
    let [toml] = blocks_in("toml").try_into().expect("the skill has one toml block");
    let allow: toml::Table = toml::from_str(&toml).unwrap();
    let allow = allow["allow"].as_array().unwrap().iter().map(|r| r.as_str().unwrap().to_owned()).collect();
    (script.clone(), github.clone(), forgejo, allow)
}

/// Runs `script` with `args` as a watcher, each command() answered by `answer` from the command, after checking
/// `allow` lets the command run.
async fn check(
    script: &str,
    args: &Value,
    allow: &[String],
    answer: impl Fn(&str) -> (i32, Value),
) -> (Result<Value, String>, Vec<String>) {
    let worker = Worker::new(env!("CARGO_BIN_EXE_ants"));
    let commands = Mutex::new(Vec::new());
    let read = async |function: TrailFunction, args: Value| {
        assert_eq!(function, TrailFunction::Command, "{args}");
        let command = args["command"].as_str().unwrap().to_owned();
        let tool = ToolCall {
            kind: "execute".into(),
            title: command.clone(),
            locations: Vec::new(),
            input: Some(json!({"command": command}).to_string()),
        };
        assert!(allow.iter().any(|rule| ants_daemon::permission::covers(rule, &tool)), "no rule allows {command}");
        let (status, stdout) = answer(&command);
        commands.lock().unwrap().push(command);
        CallAnswer::Ok(json!({"status": status, "stdout": stdout.to_string()}))
    };
    let result = ants_trail::watch::check(&worker, script, args.clone(), read, async || ()).await;
    (result, commands.into_inner().unwrap())
}

/// The output of a check that succeeded, after checking it is done exactly when `done`.
fn output(result: Result<Value, String>, done: bool) -> String {
    let result = result.unwrap();
    let mut keys: Vec<&String> = result.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(keys, ["done", "output"], "{result}");
    assert_eq!(result["done"], done, "{result}");
    result["output"].as_str().unwrap().to_owned()
}

fn gh(state: &str, merge: &str, decision: &str, checks: Value) -> Value {
    json!({
        "state": state,
        "isDraft": false,
        "mergeStateStatus": merge,
        "reviewDecision": decision,
        "reviews": [{"author": {"login": "rev"}, "state": "COMMENTED"}, {"author": {"login": "rev"}, "state": decision}],
        "statusCheckRollup": checks,
    })
}

fn run(name: &str, status: &str, conclusion: &str) -> Value {
    json!({"__typename": "CheckRun", "name": name, "status": status, "conclusion": conclusion})
}

#[tokio::test]
async fn the_pr_watch_skill_s_trail_tells_what_blocks_a_github_pull_request() {
    let (script, args, _, allow) = skill();
    let status = json!({"__typename": "StatusContext", "context": "deploy", "state": "SUCCESS"});
    let blocked = gh(
        "OPEN",
        "BEHIND",
        "CHANGES_REQUESTED",
        json!([
            run("test", "COMPLETED", "FAILURE"),
            run("clippy", "COMPLETED", "SUCCESS"),
            status,
            run("fmt", "COMPLETED", "TIMED_OUT")
        ]),
    );
    let (result, commands) = check(&script, &args, &allow, |_| (0, blocked.clone())).await;
    assert_eq!(
        output(result, false),
        "acme/widgets#7 is blocked:\n- checks failing: fmt, test\n- changes requested (2 reviews)\n\
         - the base branch moved on: rebase onto it"
    );
    assert_eq!(commands.len(), 1);
    assert!(commands[0].starts_with("gh pr view 7 --repo acme/widgets --json "), "{commands:?}");

    let mut reordered = blocked.clone();
    reordered["statusCheckRollup"].as_array_mut().unwrap().reverse();
    let (again, _) = check(&script, &args, &allow, |_| (0, reordered.clone())).await;
    assert!(output(again, false).contains("checks failing: fmt, test"), "the order of checks wakes no one");

    let running = gh("OPEN", "BLOCKED", "APPROVED", json!([run("test", "IN_PROGRESS", ""), status]));
    let (result, _) = check(&script, &args, &allow, |_| (0, running.clone())).await;
    assert_eq!(output(result, false), "acme/widgets#7 is blocked:\n- checks running");

    let ready = gh("OPEN", "CLEAN", "APPROVED", json!([run("test", "COMPLETED", "SUCCESS"), status]));
    let (result, _) = check(&script, &args, &allow, |_| (0, ready.clone())).await;
    assert_eq!(output(result, false), "acme/widgets#7 is ready to merge");

    for (state, told) in [("MERGED", "merged"), ("CLOSED", "closed")] {
        let ended = gh(state, "UNKNOWN", "", json!([]));
        let (result, _) = check(&script, &args, &allow, |_| (0, ended.clone())).await;
        assert_eq!(output(result, true), format!("acme/widgets#7 is {told}"));
    }

    let (failed, _) = check(&script, &args, &allow, |_| (1, json!("gh: not logged in"))).await;
    assert!(failed.unwrap_err().contains("exited with status 1"), "a failed read fails the check");
}

#[tokio::test]
async fn the_pr_watch_skill_s_trail_tells_what_blocks_a_forgejo_pull_request() {
    let (script, _, private, allow) = skill();
    let mut public = private.clone();
    public.as_object_mut().unwrap().remove("token_env");
    let pull = |state: &str, merged: bool, mergeable: bool| json!({"state": state, "merged": merged, "draft": false, "mergeable": mergeable, "head": {"sha": "abc123"}});
    let review = |state: &str, dismissed: bool| json!({"state": state, "user": {"login": "rev"}, "dismissed": dismissed, "stale": false});
    let forge = |pull: Value, statuses: Value, reviews: Value| {
        move |command: &str| -> (i32, Value) {
            if command.ends_with("/pulls/7") {
                (0, pull.clone())
            } else if command.ends_with("/commits/abc123/status") {
                (0, json!({"state": "failure", "statuses": statuses}))
            } else if command.ends_with("/pulls/7/reviews") {
                (0, reviews.clone())
            } else {
                panic!("an unexpected read: {command}")
            }
        }
    };

    let failing = json!([{"context": "ci/test", "status": "failure"}, {"context": "ci/lint", "status": "success"}]);
    let reviews = json!([review("APPROVED", true), review("REQUEST_CHANGES", false)]);
    let (result, commands) = check(&script, &public, &allow, forge(pull("open", false, false), failing, reviews)).await;
    assert_eq!(
        output(result, false),
        "acme/widgets#7 is blocked:\n- checks failing: ci/test\n- changes requested (2 reviews)\n\
         - conflicts with the base branch"
    );
    assert_eq!(commands[0], "curl -sf https://forge.example.net/api/v1/repos/acme/widgets/pulls/7");

    let passing = json!([{"context": "ci/test", "status": "success"}]);
    let approved = json!([review("APPROVED", false)]);
    let (result, commands) =
        check(&script, &private, &allow, forge(pull("open", false, true), passing.clone(), approved.clone())).await;
    assert_eq!(output(result, false), "acme/widgets#7 is ready to merge");
    assert!(
        commands.iter().all(|c| c.starts_with("curl -sf -H \"Authorization: token $FORGEJO_TOKEN\" ")),
        "{commands:?}"
    );

    let pending = json!([{"context": "ci/test", "status": "pending"}]);
    let (result, _) = check(&script, &public, &allow, forge(pull("open", false, true), pending, json!([]))).await;
    assert_eq!(
        output(result, false),
        "acme/widgets#7 is blocked:\n- checks running\n- waiting for a review (0 reviews)"
    );

    for (state, merged, told) in [("closed", true, "merged"), ("closed", false, "closed")] {
        let reads = forge(pull(state, merged, false), passing.clone(), approved.clone());
        let (result, commands) = check(&script, &public, &allow, reads).await;
        assert_eq!(output(result, true), format!("acme/widgets#7 is {told}"));
        assert_eq!(commands.len(), 1, "an ended pull request needs no other read");
    }
}
