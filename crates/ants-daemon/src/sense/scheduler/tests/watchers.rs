//! Tests of watchers as the scheduler checks them: a wake on each changed result carrying its output and replacing
//! the last, none on an unchanged one, the last on done, failed checks, trail watchers and their reads, and the output
//! lost to a restart.

use ants_sense::remind::{Checked, Status};

use super::*;

/// A trail watcher's stand-in: reads the result of the command `cat result` through the check's reads.
struct CatTrail;

impl TrailChecks for CatTrail {
    fn check<'a>(
        &'a self,
        name: &'a str,
        args: Value,
        reads: &'a crate::sense::scheduler::watchers::Reads<'a>,
    ) -> futures::future::BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            assert_eq!((name, &args), ("pr-blocked", &json!({"pr": 7})));
            let refused = reads.read(ants_proto::TrailFunction::Bd, json!({"args": ["close", "ants-1"]})).await;
            assert!(
                matches!(refused, ants_proto::CallAnswer::Error(e) if e.contains("only the subcommands that read"))
            );
            let escaping = reads.read(ants_proto::TrailFunction::ReadFile, json!({"path": "../x"})).await;
            assert!(matches!(escaping, ants_proto::CallAnswer::Error(_)), "{escaping:?}");
            match reads.read(ants_proto::TrailFunction::ReadFile, json!({"path": "result"})).await {
                ants_proto::CallAnswer::Ok(text) => {
                    let text = text.as_str().unwrap_or("gone").trim().to_owned();
                    Ok(json!({"output": text, "done": text == "merged"}))
                }
                ants_proto::CallAnswer::Error(e) => Err(e),
            }
        })
    }
}

fn command_watcher() -> Value {
    json!({"kind":"watch","run":{"kind":"command","command":"./check"}})
}

#[cfg(unix)]
#[tokio::test]
async fn a_command_watcher_wakes_on_each_change_with_its_output_and_retires_once_done() {
    let root = ants_test_support::scratch_dir("scheduler-watchers");
    let home = root.join("home");
    std::fs::write(root.join("result"), "pending").unwrap();
    let check = root.join("check");
    std::fs::write(&check, "#!/bin/sh\ncat result\ntest \"$(cat result)\" != merged || exit 3\n").unwrap();
    std::fs::set_permissions(&check, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let (sense, stores) = setup().await;
    let items = &*stores.items;
    let host = Wakes::default();
    let config = Settings {
        workdir: root.clone(),
        allow: vec!["execute:./check".into()],
        home: Some(home.clone()),
        redactor: crate::secrets::redact::Redactor::default().with_env([("API_TOKEN", "s3cret-value")]),
        ..settings()
    };
    let id = create(&sense, command_watcher()).await;
    let mut at = Utc::now();
    cycle(&sense, items, &host, &config, at).await.unwrap();
    assert!(host.requests.locked().is_empty(), "the first check sets the baseline");
    let status = remind::watch::read_status(&home).unwrap().unwrap();
    assert_eq!((status.watchers.len(), status.watchers[0].failed_checks), (1, 0), "{status:?}");

    at += chrono::Duration::seconds(61);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    assert!(host.requests.locked().is_empty(), "unchanged");

    std::fs::write(root.join("result"), "failing s3cret-value").unwrap();
    at += chrono::Duration::seconds(61);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    let first = host.requests.locked()[0].clone();
    assert!(first.text.contains("```\nfailing [redacted]\n```"), "{}", first.text);
    assert_eq!(first.replaces, None);
    let watcher = first.watcher.clone().expect("a watcher's wake");
    assert_eq!((watcher.id, watcher.title.as_str()), (id.to_string(), "scheduled work"));
    assert_eq!((watcher.output.as_deref(), watcher.done, watcher.cut), (Some("failing [redacted]"), false, false));
    assert_eq!(watcher.replaces, None, "the daemon's to set");
    assert!(config.outputs.locked().is_empty(), "the output goes with its wake");

    std::fs::write(root.join("result"), "passing").unwrap();
    at += chrono::Duration::seconds(61);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    let second = host.requests.locked()[1].clone();
    assert_eq!(second.replaces.as_deref(), Some(first.id.as_str()));

    std::fs::write(root.join("result"), "merged").unwrap();
    at += chrono::Duration::seconds(61);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    let last = host.requests.locked()[2].clone();
    assert!(last.text.contains("this is its last firing") && last.text.contains("[exit status 3]"), "{}", last.text);
    assert!(last.watcher.as_ref().is_some_and(|watcher| watcher.done), "{last:?}");
    assert_eq!(last.replaces.as_deref(), Some(second.id.as_str()));
    assert_eq!(reminder(items, &id).await.status, Status::Done);
    at += chrono::Duration::hours(1);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    assert_eq!(host.requests.locked().len(), 3);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_watcher_outside_the_allow_rules_fails_its_checks_without_waking() {
    let root = ants_test_support::scratch_dir("scheduler-watchers-refused");
    let (sense, stores) = setup().await;
    let host = Wakes::default();
    let config = Settings { workdir: root.clone(), allow: vec!["execute:./probe".into()], ..settings() };
    let id = create(&sense, command_watcher()).await;
    cycle(&sense, &*stores.items, &host, &config, Utc::now()).await.unwrap();
    assert!(host.requests.locked().is_empty());
    let reminder = reminder(&*stores.items, &id).await;
    assert_eq!(reminder.failed_checks, Some(1));
    assert!(reminder.check_error.unwrap().contains("outside the ant's tool allow rules"));
    std::fs::remove_dir_all(root).unwrap();
}

/// On a harness running its tools unasked, a watcher's command runs unasked too, no allow rule needed; one whose
/// source thread is untrusted is held to the rules.
#[cfg(unix)]
#[tokio::test]
async fn a_watcher_on_a_harness_running_its_tools_unasked_runs_its_command_unasked() {
    let root = ants_test_support::scratch_dir("scheduler-watchers-unasked");
    std::fs::write(root.join("result"), "pending").unwrap();
    let check = root.join("check");
    std::fs::write(&check, "#!/bin/sh\ncat result\n").unwrap();
    std::fs::set_permissions(&check, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let (sense, stores) = setup().await;
    let items = &*stores.items;
    let host = Wakes::default();
    let config = Settings { workdir: root.clone(), unasked: true, ..settings() };
    let id = create(&sense, command_watcher()).await;
    cycle(&sense, items, &host, &config, Utc::now()).await.unwrap();
    let checked = reminder(items, &id).await;
    assert_eq!((checked.failed_checks, checked.check_error.as_deref()), (None, None), "checked unasked");
    assert!(checked.digest().is_some());

    let source = checked.source_context.clone().unwrap();
    let untrusted = ants_proto::provider::Origin {
        by: ants_proto::provider::Starter::Webhook { user: "github".parse().unwrap() },
        channel: None,
    };
    let mut values = ants_sense::items::Values::new();
    values.insert("source_context".into(), json!(CallContext { origin: untrusted, ..source }));
    values.insert("next_due".into(), json!(Utc::now()));
    let item = items.get(&id).await.unwrap().unwrap();
    let change = Write::Change { id: id.clone(), values, expected: Some(item.revision) };
    items.write(&[change], "test").await.unwrap();
    cycle(&sense, items, &host, &config, Utc::now() + chrono::Duration::seconds(61)).await.unwrap();
    let refused = reminder(items, &id).await;
    assert!(refused.check_error.unwrap().contains("outside the ant's tool allow rules"), "an untrusted source");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn sense_remind_is_refused_a_command_its_checks_could_not_run_naming_the_rule() {
    let asked = TriggerCommands { allow: vec!["execute:./probe".into()], unasked: false };
    let create = |trigger: Value| json!({"title": "t", "trigger": trigger}).as_object().unwrap().clone();
    let why = asked.refusal("remind", &create(command_watcher())).unwrap();
    assert!(why.contains("\"execute:./check\""), "{why}");
    let condition = json!({"kind":"condition","condition":{"kind":"command","command":"./check"}});
    assert!(asked.refusal("remind", &create(condition)).is_some());
    let probe = json!({"kind":"watch","run":{"kind":"command","command":"./probe"}});
    assert_eq!(asked.refusal("remind", &create(probe)), None, "inside the rules");
    assert_eq!(asked.refusal("remind", &create(json!({"kind":"after","seconds":300}))), None, "no command");
    let mut snooze = create(command_watcher());
    snooze.insert("action".into(), json!("snooze"));
    assert_eq!(asked.refusal("remind", &snooze), None, "not a creation");
    assert_eq!(asked.refusal("recall", &create(command_watcher())), None);
    let unasked = TriggerCommands { allow: Vec::new(), unasked: true };
    assert_eq!(unasked.refusal("remind", &create(command_watcher())), None);
}

#[tokio::test]
async fn a_trail_watcher_reads_through_its_check_and_a_daemon_without_trails_fails_it() {
    let root = ants_test_support::scratch_dir("scheduler-trail-watchers");
    std::fs::write(root.join("result"), "open").unwrap();
    let (sense, stores) = setup().await;
    let items = &*stores.items;
    let host = Wakes::default();
    let trail = json!({"kind":"watch","run":{"kind":"trail","name":"pr-blocked","args":{"pr":7}},"first":true});
    let id = create(&sense, trail).await;
    let without = Settings { workdir: root.clone(), ..settings() };
    let mut at = Utc::now();
    cycle(&sense, items, &host, &without, at).await.unwrap();
    assert!(reminder(items, &id).await.check_error.unwrap().contains("this daemon runs no trails"));

    let config = Settings { workdir: root.clone(), trails: Some(Arc::new(CatTrail)), ..settings() };
    at += chrono::Duration::seconds(61);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    assert!(host.requests.locked()[0].text.contains("```\nopen\n```"), "{:?}", host.requests.locked());
    std::fs::write(root.join("result"), "merged").unwrap();
    at += chrono::Duration::seconds(61);
    cycle(&sense, items, &host, &config, at).await.unwrap();
    assert!(host.requests.locked()[1].text.contains("this is its last firing"));
    assert_eq!(reminder(items, &id).await.status, Status::Done);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn a_firing_prepared_before_a_restart_says_its_output_was_not_kept() {
    let (sense, stores) = setup().await;
    let items = &*stores.items;
    let id = create(&sense, json!({"kind":"watch","run":{"kind":"command","command":"true"},"first":true})).await;
    let check = remind::reserve_watch(items, &id, Utc::now()).await.unwrap().unwrap();
    let digest = remind::watch::digest(Some(0), b"");
    let firing =
        remind::complete_watch(items, &check, Utc::now(), Checked::Seen { digest, done: false }).await.unwrap();
    assert!(firing.is_some());
    let host = Wakes::default();
    cycle(&sense, items, &host, &settings(), Utc::now()).await.unwrap();
    assert!(
        host.requests.locked()[0].text.ends_with(crate::sense::scheduler::watchers::OUTPUT_LOST),
        "{:?}",
        host.requests.locked()
    );
    let watcher = host.requests.locked()[0].watcher.clone().expect("a watcher's wake");
    assert_eq!((watcher.output, watcher.cut), (None, false), "no output kept");
}
