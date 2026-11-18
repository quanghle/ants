use ants_sense::cache::Cache;
use ants_sense::log::{AccountVerifier, AntLog, MemoryLog, NoRevocations, Unsigned};
use ants_sense::store::Store;

use super::*;

fn parse(condition: Value) -> Condition {
    serde_json::from_value(condition).unwrap()
}

async fn items() -> impl Items {
    let log = AntLog::new(
        MemoryLog::default(),
        Unsigned,
        AccountVerifier::new(vec![], NoRevocations),
        ants_proto::SenseId::new(),
        "dev-a".parse().unwrap(),
        "m1".parse().unwrap(),
        None,
    )
    .unwrap();
    tokio::sync::Mutex::new(Store::open(log, Cache::in_memory().unwrap()).await.unwrap())
}

#[tokio::test]
async fn files_are_fingerprinted_and_never_escape_the_workdir() {
    let root = ants_test_support::scratch_dir("sense-file-condition");
    assert_eq!(file("later", &root).await.unwrap(), Value::Null);
    std::fs::write(root.join("later"), "first").unwrap();
    let first = file("later", &root).await.unwrap();
    assert_eq!(first, file("later", &root).await.unwrap());
    std::fs::write(root.join("later"), "other").unwrap();
    assert_ne!(first, file("later", &root).await.unwrap());
    assert!(file("../outside", &root).await.is_err());
    assert!(file(root.to_str().unwrap(), &root).await.is_err());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(std::env::temp_dir(), root.join("outside")).unwrap();
        assert!(file("outside", &root).await.is_err());
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn changed_files_need_a_baseline_and_disappearance_counts_as_a_change() {
    let root = ants_test_support::scratch_dir("sense-file-baseline");
    let items = items().await;
    let env = BTreeMap::new();
    let context = Context { workdir: &root, env: &env, allow: &[], items: &items, now: Utc::now() };
    let condition = parse(json!({"kind":"file","path":"flag","event":"changed"}));
    let first = evaluate(&condition, None, &context).await.unwrap();
    assert!(!first.fired);
    std::fs::write(root.join("flag"), "ready").unwrap();
    let appeared = evaluate(&condition, Some(&first.value), &context).await.unwrap();
    assert!(appeared.fired);
    assert!(!evaluate(&condition, Some(&appeared.value), &context).await.unwrap().fired);
    std::fs::remove_file(root.join("flag")).unwrap();
    assert!(evaluate(&condition, Some(&appeared.value), &context).await.unwrap().fired);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn command_checks_obey_allow_rules_and_use_the_workdir_and_environment() {
    let root = ants_test_support::scratch_dir("sense-command-condition");
    let items = items().await;
    let env = BTreeMap::from([("SENSE_TEST_FLAG".into(), "ready".into())]);
    let mut context = Context { workdir: &root, env: &env, allow: &[], items: &items, now: Utc::now() };
    let command = parse(json!({"kind":"command","command":"test \"$SENSE_TEST_FLAG\" = ready"}));
    assert!(evaluate(&command, None, &context).await.is_err());
    let allow = vec!["execute:test *".into()];
    context.allow = &allow;
    let result = evaluate(&command, None, &context).await.unwrap();
    assert!(result.fired);
    assert_eq!(result.value, json!(true), "command output is never kept");
    std::fs::write(root.join("flag"), "ready").unwrap();
    assert!(evaluate(&parse(json!({"kind":"command","command":"test -f flag"})), None, &context).await.unwrap().fired);
    assert!(
        evaluate(&parse(json!({"kind":"command","command":"test -f flag; touch forbidden"})), None, &context)
            .await
            .is_err()
    );
    assert!(!root.join("forbidden").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test(start_paused = true)]
async fn a_command_timeout_does_not_leave_the_check_running() {
    let root = ants_test_support::scratch_dir("sense-command-timeout");
    let items = items().await;
    let env = BTreeMap::new();
    let allow = vec!["execute:sleep *".into()];
    let context = Context { workdir: &root, env: &env, allow: &allow, items: &items, now: Utc::now() };
    let before = tokio::time::Instant::now();
    assert!(evaluate(&parse(json!({"kind":"command","command":"sleep 300"})), None, &context).await.is_err());
    assert_eq!(before.elapsed(), TIMEOUT);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn github_and_bd_checks_interpret_structured_results_without_saving_output() {
    use std::os::unix::fs::PermissionsExt as _;

    let root = ants_test_support::scratch_dir("sense-external-condition");
    let items = items().await;
    let env = BTreeMap::from([("PATH".into(), root.to_str().unwrap().into())]);
    let context = Context { workdir: &root, env: &env, allow: &[], items: &items, now: Utc::now() };
    let stub = |name: &str, body: &str| {
        let path = root.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    };
    stub(
        "gh",
        r#"printf '%s' '{"state":"MERGED","mergedAt":"2025-01-01T00:00:00Z","reviews":[{"state":"APPROVED"}]}'"#,
    );
    for state in ["merged", "closed", "reviewed"] {
        let condition = parse(json!({"kind":"gh_pr","repo":"o/r","number":42,"state":state}));
        let result = evaluate(&condition, None, &context).await.unwrap();
        assert!(result.fired);
        assert_eq!(result.value, json!(true));
    }
    stub("gh", r#"printf '%s' '{"status":"completed"}'"#);
    assert!(evaluate(&parse(json!({"kind":"gh_workflow","repo":"o/r","run":4})), None, &context).await.unwrap().fired);
    stub("bd", r#"printf '%s' '{"id":"work-1","status":"closed"}'"#);
    assert!(evaluate(&parse(json!({"kind":"bd","id":"work-1","state":"closed"})), None, &context).await.unwrap().fired);
    stub("bd", r#"printf '%s' '[{"id":"work-1","status":"open"}]'"#);
    assert!(evaluate(&parse(json!({"kind":"bd","id":"work-1","state":"ready"})), None, &context).await.unwrap().fired);
    assert!(!evaluate(&parse(json!({"kind":"bd","id":"work-2","state":"ready"})), None, &context).await.unwrap().fired);
    stub("gh", "/bin/cat response");
    std::fs::write(root.join("response"), "x".repeat(65 * 1024)).unwrap();
    assert!(evaluate(&parse(json!({"kind":"gh_workflow","repo":"o/r","run":4})), None, &context).await.is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn git_checks_wait_until_the_commit_reaches_the_named_reference() {
    if !ants_test_support::have("git") {
        return;
    }
    let root = ants_test_support::scratch_dir("sense-git-condition");
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git").args(args).current_dir(&root).output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    git(&["init", "--quiet"]);
    std::fs::write(root.join("work"), "first").unwrap();
    git(&["add", "work"]);
    git(&["-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "--quiet", "-m", "first"]);
    git(&["branch", "waiting"]);
    std::fs::write(root.join("work"), "second").unwrap();
    git(&["add", "work"]);
    git(&["-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "--quiet", "-m", "second"]);
    let commit = git(&["rev-parse", "HEAD"]);
    let items = items().await;
    let env = BTreeMap::new();
    let context = Context { workdir: &root, env: &env, allow: &[], items: &items, now: Utc::now() };
    let condition = parse(json!({"kind":"git","commit":commit,"reference":"waiting"}));
    assert!(!evaluate(&condition, None, &context).await.unwrap().fired);
    git(&["branch", "--force", "waiting", "HEAD"]);
    assert!(evaluate(&condition, None, &context).await.unwrap().fired);
    assert!(
        evaluate(&parse(json!({"kind":"git","commit":"--help","reference":"HEAD"})), None, &context).await.is_err()
    );
    std::fs::remove_dir_all(root).unwrap();
}
