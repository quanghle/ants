//! Tests of condition probes as the scheduler runs them: source threads and workdirs, sense events, reservations on
//! takeover, command and gh checks, and cancelling a probe with its child process.

use ants_sense::Workdirs;
#[cfg(unix)]
use ants_sense::items::ItemValues;

use super::*;

struct Directories(Mutex<Option<PathBuf>>);

impl Workdirs for Directories {
    fn workdir(&self, context: &CallContext) -> Option<PathBuf> {
        assert_eq!(context.thread.as_str(), "scheduler-thread", "always use the original source thread");
        self.0.locked().clone()
    }
}

fn file_trigger() -> Value {
    json!({"kind":"condition","condition":{"kind":"file","path":"flag","event":"appeared"}})
}

#[tokio::test]
async fn probes_use_the_live_source_thread_then_only_same_machine_captured_paths() {
    let root = ants_test_support::scratch_dir("scheduler-workdirs");
    let local = root.join("local");
    let captured = root.join("captured");
    let live = root.join("live");
    for path in [&local, &captured, &live] {
        std::fs::create_dir(path).unwrap();
    }
    let (sense, stores) = setup().await;
    let resolver = Arc::new(Directories(Mutex::new(Some(captured.clone()))));
    let sense = Arc::new(Arc::try_unwrap(sense).unwrap().with_workdirs(resolver.clone()));
    let host = Wakes::default();
    let config = Settings { workdir: local.clone(), ..settings() };
    let first = create(&sense, file_trigger()).await;
    let mut renew = call(json!({"action":"renew","id":first}));
    renew.context.thread = "another-thread".parse().unwrap();
    assert!(!sense.call_tool(renew).await.unwrap().is_error);
    *resolver.0.locked() = Some(live.clone());
    std::fs::write(live.join("flag"), "ready").unwrap();
    cycle(&sense, &*stores.items, &host, &config, Utc::now()).await.unwrap();
    assert_eq!(host.requests.locked()[0].id, format!("{first}:1"));

    *resolver.0.locked() = Some(captured.clone());
    let same_machine = create(&sense, file_trigger()).await;
    *resolver.0.locked() = None;
    std::fs::write(captured.join("flag"), "ready").unwrap();
    cycle(&sense, &*stores.items, &host, &config, Utc::now()).await.unwrap();
    assert_eq!(host.requests.locked()[1].id, format!("{same_machine}:1"));

    *resolver.0.locked() = Some(captured);
    let moved = create(&sense, file_trigger()).await;
    *resolver.0.locked() = None;
    let mut renew = call(json!({"action":"renew","id":moved}));
    renew.context.machine = "m2".into();
    assert!(!sense.call_tool(renew).await.unwrap().is_error);
    let moved_config = Settings { machine: "m2".into(), ..config };
    cycle(&sense, &*stores.items, &host, &moved_config, Utc::now()).await.unwrap();
    assert_eq!(host.requests.locked().len(), 2, "never probe the old machine's absolute path");
    let at = remind::next_due(&*stores.items, Utc::now()).await.unwrap().unwrap();
    std::fs::write(local.join("flag"), "ready").unwrap();
    cycle(&sense, &*stores.items, &host, &moved_config, at).await.unwrap();
    assert_eq!(host.requests.locked()[2].id, format!("{moved}:1"));
    std::fs::remove_dir_all(root).unwrap();
}

async fn tool(sense: &Sense, name: &str, arguments: Value, at: DateTime<Utc>) -> Value {
    let mut call = call(arguments);
    call.name = name.into();
    call.context.at = at;
    let result = sense.call_tool(call).await.unwrap();
    assert!(!result.is_error, "{result:?}");
    let ants_proto::provider::Output::Json(result) = result.output else { panic!("requested JSON") };
    result
}

#[tokio::test]
async fn sense_events_check_overdue_commitments_and_expired_task_leases() {
    let (sense, stores) = setup().await;
    let at = Utc::now();
    let overdue = at - chrono::Duration::minutes(1);
    let promise = tool(&sense, "promise", json!({"title":"overdue","promisee":"alice","due":overdue}), at).await;
    let task = tool(&sense, "task", json!({"action":"create","title":"held"}), at).await;
    let claim = json!({"action":"claim","id":task["task"]["id"]});
    tool(&sense, "task", claim, at - chrono::Duration::minutes(10)).await;
    for (event, id) in [("commitment_overdue", &promise["promise"]["id"]), ("lease_expired", &task["task"]["id"])] {
        create(&sense, json!({"kind":"condition","condition":{"kind":"sense","event":event,"id":id}})).await;
    }
    let host = Wakes::default();
    cycle(&sense, &*stores.items, &host, &settings(), Utc::now()).await.unwrap();
    let requests = host.requests.locked().clone();
    let matched = requests.iter().filter(|request| request.text.contains("sense event condition matched"));
    assert_eq!(matched.count(), 2, "{requests:?}");
    assert!(remind::pending_work(&*stores.items).await.unwrap().is_empty());
}

#[cfg(unix)]
fn script(root: &std::path::Path, name: &str, body: &str) {
    let path = root.join(name);
    ants_test_support::executable(&path, &format!("#!/bin/sh\n{body}\n"));
}

#[cfg(unix)]
async fn wait_file(root: &std::path::Path, name: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.join(name).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("probe started");
}

#[cfg(unix)]
#[tokio::test]
async fn persisted_reservations_prevent_overlapping_checks_on_takeover() {
    let root = ants_test_support::scratch_dir("scheduler-probe-reservation");
    script(&root, "probe", "printf x >> runs\nsleep 1\nexit 0");
    let log = SharedLog::default();
    let identity = SenseId::new();
    let items = open(log.clone(), identity.clone()).await;
    let sense = Arc::new(Sense::with_stores(Arc::new(One::new(items.clone()))));
    let id = create(&sense, json!({"kind":"condition","condition":{"kind":"command","command":"./probe"}})).await;
    let host = Arc::new(Wakes::default());
    let config = Arc::new(Settings { workdir: root.clone(), allow: vec!["execute:*".into()], ..settings() });
    let first = tokio::spawn({
        let (sense, items, host, config) = (sense.clone(), items.clone(), host.clone(), config.clone());
        async move { cycle(&sense, &*items, &*host, &config, Utc::now()).await }
    });
    wait_file(&root, "runs").await;
    let takeover = open(log, identity).await;
    let next = cycle(&sense, &*takeover, &*host, &config, Utc::now()).await.unwrap().unwrap();
    assert!(next >= Utc::now() + chrono::Duration::seconds(58));
    assert_eq!(std::fs::read_to_string(root.join("runs")).unwrap(), "x");
    assert!(host.requests.locked().is_empty());
    first.await.unwrap().unwrap();
    assert_eq!(host.requests.locked().len(), 1);
    assert_eq!(host.requests.locked()[0].id, format!("{id}:1"));
    assert_eq!(std::fs::read_to_string(root.join("runs")).unwrap(), "x");
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn command_checks_use_current_policy_and_environment_without_storing_output() {
    let root = ants_test_support::scratch_dir("scheduler-command-policy");
    script(&root, "probe", "printf 'private command output'\ntest \"$SENSE_RESULT\" = ready");
    let (sense, stores) = setup().await;
    let id = create(&sense, json!({"kind":"condition","condition":{"kind":"command","command":"./probe"}})).await;
    let denied =
        create(&sense, json!({"kind":"condition","condition":{"kind":"command","command":"touch forbidden"}})).await;
    let config = Settings {
        workdir: root.clone(),
        env: BTreeMap::from([("SENSE_RESULT".into(), "ready".into())]),
        allow: vec!["execute:./probe".into()],
        ..settings()
    };
    let host = Wakes::default();
    let at = Utc::now();
    cycle(&sense, &*stores.items, &host, &config, at).await.unwrap();
    assert_eq!(host.requests.locked().len(), 1);
    assert_eq!(host.requests.locked()[0].id, format!("{id}:1"));
    let item = stores.items.get(&id).await.unwrap().unwrap();
    let observation = remind::Reminder::of(&item).unwrap().observation.unwrap();
    assert_eq!(observation.as_object().unwrap().keys().collect::<Vec<_>>(), ["digest"], "{observation}");
    assert!(!serde_json::to_string(&item).unwrap().contains("private command output"));
    let denied = reminder(&*stores.items, &denied).await;
    assert_eq!(denied.unchanged_checks, Some(1));
    assert!(denied.next_due.unwrap() >= at + chrono::Duration::minutes(1));
    assert!(!root.join("forbidden").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn gh_checks_reserve_five_minutes_and_back_off_after_ten_unchanged_observations() {
    let root = ants_test_support::scratch_dir("scheduler-probe-backoff");
    script(
        &root,
        "gh",
        r#"printf x >> runs
printf '%s' '{"state":"OPEN","mergedAt":null,"reviews":[]}'"#,
    );
    let (sense, stores) = setup().await;
    let id = create(
        &sense,
        json!({"kind":"condition","condition":{"kind":"gh_pr","repo":"o/r","number":42,"state":"merged"}}),
    )
    .await;
    let host = Wakes::default();
    let config = Settings {
        workdir: root.clone(),
        env: BTreeMap::from([("PATH".into(), root.to_str().unwrap().into())]),
        ..settings()
    };
    let mut at = Utc::now();
    for check in 0..=10 {
        let next = cycle(&sense, &*stores.items, &host, &config, at).await.unwrap().unwrap();
        let interval = if check == 10 { 30 } else { 5 };
        assert!(next >= at + chrono::Duration::minutes(interval));
        assert!(next < at + chrono::Duration::minutes(interval) + chrono::Duration::seconds(1));
        cycle(&sense, &*stores.items, &host, &config, next - chrono::Duration::seconds(1)).await.unwrap();
        assert_eq!(std::fs::read_to_string(root.join("runs")).unwrap().len(), check + 1);
        at = next;
    }
    assert_eq!(reminder(&*stores.items, &id).await.unchanged_checks, Some(10));
    assert!(host.requests.locked().is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn stopping_or_dropping_the_scheduler_cancels_the_probe_and_its_child_process() {
    cancels_probe(true).await;
    cancels_probe(false).await;
}

#[cfg(unix)]
async fn cancels_probe(explicit_stop: bool) {
    let root = ants_test_support::scratch_dir("scheduler-probe-stop");
    script(&root, "probe", "printf '%s' \"$$\" > probe.pid\nsleep 300 &\nprintf '%s' \"$!\" > child.pid\nwait");
    let (sense, stores) = setup().await;
    let id = create(&sense, json!({"kind":"condition","condition":{"kind":"command","command":"exec ./probe"}})).await;
    let host = Arc::new(Wakes::default());
    let scheduler =
        start(sense, host.clone(), Settings { workdir: root.clone(), allow: vec!["execute:*".into()], ..settings() });
    wait_file(&root, "child.pid").await;
    if explicit_stop {
        scheduler.stop().await;
    } else {
        drop(scheduler);
    }
    for name in ["probe.pid", "child.pid"] {
        let pid = std::fs::read_to_string(root.join(name)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let output = std::process::Command::new("ps").args(["-p", pid.trim(), "-o", "stat="]).output().unwrap();
                let state = String::from_utf8_lossy(&output.stdout);
                if !output.status.success() || state.trim().starts_with('Z') {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("probe process stopped");
    }
    assert!(host.requests.locked().is_empty());
    assert!(remind::pending_work(&*stores.items).await.unwrap().is_empty());
    let reserved = reminder(&*stores.items, &id).await;
    assert!(reserved.check_started.is_some(), "reservation survives interruption");
    assert!(reserved.next_due.unwrap() > Utc::now());
    std::fs::remove_dir_all(root).unwrap();
}
