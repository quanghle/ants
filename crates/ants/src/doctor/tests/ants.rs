//! Tests of the doctor's checks of the ants: look-alike avatars, harnesses and their versions, shared settings,
//! who answers their requests, workdirs, homes, file limits, sense, watchers, tool rules and sages.

use super::*;

#[test]
fn ants_looking_alike_are_warned_of() {
    let now = chrono::Utc::now();
    let entries: Vec<ants_proto::ColonyEntry> =
        ["dev-a", "dev-b"].map(|ant| crate::test_support::colony::entry(ant, 60, 2, now)).into();
    let check = avatars(&entries, Faces::new(Default::default(), Depth::Plain, ants_proto::Theme::Dark));
    assert_eq!(
        (check.status, check.detail.as_str()),
        (Status::Ok, "2 ants in the colony, each with a face of its own")
    );
    let mut identities = crate::ants::avatars::Identities::new();
    for e in &entries {
        let mut record = ants_proto::AntIdentity::new(e.ant.clone(), format!("{}-id", e.ant), now);
        record.avatar = Some("twin".to_owned());
        identities.insert(e.ant.clone(), record);
    }
    let check = avatars(&entries, Faces::new(identities, Depth::Plain, ants_proto::Theme::Dark));
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.fix[0].starts_with("dev-a and dev-b share the face "), "{check}");
}

#[test]
fn harness_versions_warn_of_drift_across_machines() {
    let now = chrono::Utc::now();
    let entry = |ant: &str, machine: &str, version: Option<&str>| {
        let mut json = serde_json::json!({
            "ant": ant, "machine": machine, "instance": "run-1", "harness": "copilot", "capabilities": [],
            "started_at": now, "heartbeat_at": now, "activity": "idle", "handling": 0, "harnesses": 0,
            "version": ants_proto::ANTS_VERSION, "perms": ants_proto::PERMISSIONS_VERSION,
        });
        if let Some(version) = version {
            json["agent"] = serde_json::json!({"name": "copilot", "version": version});
        }
        serde_json::from_value::<ants_proto::ColonyEntry>(json).unwrap()
    };
    let check = harness_versions(&[entry("dev-a", "m1", None)], now);
    assert_eq!(check.status, Status::Ok, "{check}");
    assert!(check.detail.starts_with("no live ant names its harness agent"), "{check}");
    let mut entries = vec![entry("dev-a", "m1", Some("1.0.3")), entry("dev-b", "m2", Some("1.0.3"))];
    let check = harness_versions(&entries, now);
    assert_eq!((check.status, check.detail.as_str()), (Status::Ok, "live ants run copilot 1.0.3 (2)"));
    entries.push(entry("dev-c", "m2", Some("1.0.2")));
    let check = harness_versions(&entries, now);
    assert_eq!(check.status, Status::Warn, "{check}");
    assert_eq!(
        check.detail,
        "different versions across machines: copilot at 1.0.3 (dev-a on m1, dev-b on m2), 1.0.2 (dev-c on m2)"
    );
}

#[test]
fn drift_of_shared_settings_is_listed_by_field_and_machine() {
    let record = |machine: &str, drift: &[(&str, &[&str])]| ants_proto::MachineRecord {
        machine: machine.parse().unwrap(),
        ants: Vec::new(),
        hostname: machine.to_owned(),
        version: ants_proto::ANTS_VERSION.to_owned(),
        updated_at: chrono::Utc::now(),
        drift: drift
            .iter()
            .map(|(ant, fields)| (ant.parse().unwrap(), fields.iter().map(|f| (*f).to_owned()).collect()))
            .collect(),
    };
    let check = drift(&[record("m1", &[]), record("m2", &[])]);
    assert_eq!((check.status, check.detail.as_str()), (Status::Ok, "no drift on 2 machines"));
    let check = drift(&[record("m1", &[("dev-a", &["model"])]), record("m2", &[("dev-a", &["model", "whoami"])])]);
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.ends_with("which they ignore: dev-a: model on m1; dev-a: model, whoami on m2"), "{check}");
}

#[tokio::test]
async fn a_missing_harness_fails_with_its_fix() {
    let dir = ants_test_support::scratch_dir("doctor-harness").canonicalize().unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\nsage = \"none\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"ghost\"\n\
             [harnesses.ghost]\ncommand = \"/nonexistent-ants-bin/acp\"\n",
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = Setup {
        nats: "nats://127.0.0.1:1".to_owned(),
        creds: None,
        js_domain: None,
        human: None,
        config: config.clone(),
        leaf_config: dir.join("leaf.conf"),
        hub_config: dir.join("no-nats.conf"),
        harnesses: true,
        team: dir.join("no-team"),
        webhooks: dir.join("no-webhooks.toml"),
        home: None,
        worker: None,
    };
    let checks = check_ants(&loaded.ants, &loaded.harnesses, &setup, 0).await;
    let harness = named(&checks, "ant dev-a harness")[0];
    assert_eq!(harness.status, Status::Fail, "{checks:#?}");
    assert!(harness.detail.contains("/nonexistent-ants-bin/acp is not installed"), "{harness}");
    let fix =
        format!("install /nonexistent-ants-bin/acp, or set the `command` of [harnesses.ghost] in {}", config.display());
    assert_eq!(harness.fix, [fix]);
    assert_eq!(named(&checks, "ant dev-a credentials")[0].status, Status::Skip);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Each ant's reactions requests are asked as the ant: answered by nothing, by `ants webhook serve` listing only, or
/// by `ants signer`; an ant without credentials asks no one.
#[tokio::test]
async fn the_reactions_check_names_who_answers_an_ants_requests() {
    use ants_daemon::records::reactions::{Answering, serve};
    let Some(server) = Server::start() else { return };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let creds = server.creds_as(&dev_a, Role::Ant);
    let config = server.dir().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nharness = \"copilot\"\nhuman = \"human\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\ncreds = {creds:?}\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n"
        ),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = setup(&server, None);
    // Past the first repeat of a request, which a responder subscribing late still hears.
    let wait = ants_proto::REACTIONS_REPEAT + Duration::from_millis(500);
    let checked = async || check_reactions(&loaded.ants, &setup, wait).await;

    let checks = checked().await;
    let open = named(&checks, "ant dev-b reactions")[0];
    assert_eq!(open.status, Status::Ok, "{open}");
    assert!(open.detail.contains("the daemon answers list_reactions and react itself"), "{open}");
    let none = named(&checks, "ant dev-a reactions")[0];
    assert_eq!(none.status, Status::Warn, "{none}");
    assert!(none.detail.starts_with("nothing answers its reactions requests within 2s"), "{none}");
    assert!(none.fix[0].starts_with("ants signer") && none.fix[1].contains("ants webhook serve"), "{none}");

    let (human, js) = server.connect_as(&"alice".parse().unwrap(), Role::Human).await;
    let webhook = tokio::spawn(serve(human, js, None, Answering::Lists));
    let checks = checked().await;
    let lists = named(&checks, "ant dev-a reactions")[0];
    assert_eq!(lists.status, Status::Warn, "{lists}");
    assert!(
        lists.detail.starts_with("ants webhook serve answers list_reactions, but nothing answers react"),
        "{lists}"
    );
    webhook.abort();

    let approver =
        crate::credentials::approver::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, wait)
            .await
            .unwrap();
    let signer = tokio::spawn(serve(approver.client.clone(), approver.hub().clone(), None, Answering::ListsAndChanges));
    let checks = checked().await;
    let both = named(&checks, "ant dev-a reactions")[0];
    assert_eq!((both.status, both.detail.as_str()), (Status::Ok, "ants signer answers list_reactions and react"));
    signer.abort();
}

/// Each ant's trail saves are asked of `ants signer` as the ant, changing nothing; an ant without credentials asks no
/// one.
#[tokio::test]
async fn the_trail_saves_check_tells_whether_the_signer_answers() {
    let Some(server) = Server::start() else { return };
    let dev_a: UserId = "dev-a".parse().unwrap();
    let creds = server.creds_as(&dev_a, Role::Ant);
    let config = server.dir().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nharness = \"copilot\"\nhuman = \"human\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\ncreds = {creds:?}\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n"
        ),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let setup = setup(&server, None);
    let wait = ants_proto::SAVE_TRAIL_REPEAT + Duration::from_millis(500);
    let checks = check_trail_saves(&loaded.ants, &setup, wait).await;
    let open = named(&checks, "ant dev-b trail saves")[0];
    assert_eq!(open.status, Status::Ok, "{open}");
    let none = named(&checks, "ant dev-a trail saves")[0];
    assert_eq!(none.status, Status::Warn, "{none}");
    assert!(none.detail.starts_with("nothing answers its trail saves"), "{none}");

    let (human, js) = server.connect_as(&"alice".parse().unwrap(), Role::Human).await;
    js.create_stream(ants_daemon::hub::trails_config()).await.unwrap();
    drop(human);
    let approver =
        crate::credentials::approver::Approver::connect_signer(&server.url, &server.dir().join("auth"), None, wait)
            .await
            .unwrap();
    let keeping = ants_daemon::trail::keeper::serve(approver.client.clone(), approver.hub().clone(), None);
    let signer = tokio::spawn(keeping);
    let checks = check_trail_saves(&loaded.ants, &setup, wait).await;
    let answered = named(&checks, "ant dev-a trail saves")[0];
    assert_eq!(
        (answered.status, answered.detail.as_str()),
        (Status::Ok, "ants signer answers save_trail and remove_trail")
    );
    signer.abort();
}

#[test]
fn ants_sharing_a_git_workdir_with_worktrees_turned_off_are_warned() {
    let dir = ants_test_support::scratch_dir("doctor-shared-workdir").canonicalize().unwrap();
    std::fs::create_dir(dir.join(".git")).unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "messenger = false\nharness = \"copilot\"\nhuman = \"human\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nworktrees = false\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nworktrees = true\n\
             [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nmax_harnesses = 1\nworktrees = false\n\
             [[ant]]\nname = \"dev-d\"\nworkdir = \".\"\n",
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let checks = check_shared_workdirs(&loaded.ants, &config);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status)).collect::<Vec<_>>(),
        [("ant dev-a workdir", Status::Warn)]
    );
    let fix = format!("worktrees = true in the [[ant]] named dev-a of {}", config.display());
    assert!(checks[0].fix[0].starts_with(&fix), "{}", checks[0]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[cfg(unix)]
#[test]
fn a_home_too_long_for_a_socket_fails_no_check_with_a_runtime_directory_to_bind_in() {
    let dir = ants_test_support::scratch_dir("doctor-mcp-socket").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let long = "a-home-nested-deep-enough-that-its-mcp-socket-path-is-past-sun-len".repeat(2);
    std::fs::write(
        &config,
        format!("messenger = false\nsage = \"none\"\nharness = \"copilot\"\nhuman = \"human\"\n[[ant]]\nname = \"dev\"\nworkdir = \".\"\nhome = \"{long}\"\n"),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    assert!(loaded.ants[0].home.as_ref().unwrap().join("mcp.sock").as_os_str().len() > 108);
    assert_eq!(check_mcp_sockets(&loaded.ants, &config), []);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_open_file_limit_below_what_the_ants_need_is_warned() {
    use ants_daemon::limits::files::Limit;

    let dir = ants_test_support::scratch_dir("doctor-open-files").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let ants: String =
        (0..50).map(|i| format!("[[ant]]\nname = \"dev-{i}\"\nworkdir = \".\"\nmax_harnesses = 8\n")).collect();
    std::fs::write(
        &config,
        format!("messenger = false\nsage = \"none\"\nharness = \"copilot\"\nhuman = \"human\"\n{ants}"),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let launchd = Limit { soft: Some(256), hard: Some(256) };
    let low = check_open_files(&loaded.ants, Some(launchd)).unwrap();
    assert_eq!(low.status, Status::Warn, "{low}");
    assert!(low.detail.contains("limit of 256") && low.detail.contains("50 ants"), "{low}");
    assert!(!low.fix.is_empty(), "{low}");
    let raised = Limit { soft: Some(10_240), hard: None };
    assert_eq!(check_open_files(&loaded.ants, Some(raised)).unwrap().status, Status::Ok);
    assert_eq!(check_open_files(&loaded.ants, None), None);
    assert_eq!(check_open_files(&[], Some(launchd)), None);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_break_in_an_ant_s_sense_chain_fails_and_a_log_over_its_size_limit_warns() {
    use ants_sense::log::{Break, ChainStatus, SIZE_LIMIT, write_status};

    let dir = ants_test_support::scratch_dir("doctor-sense-chain").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let ants = ["dev-a", "dev-b", "dev-c"]
        .map(|name| format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\nhome = \"{name}\"\n"));
    std::fs::write(
        &config,
        format!("messenger = false\nsage = \"none\"\nharness = \"copilot\"\nhuman = \"human\"\n{}", ants.concat()),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let checked_at = "2026-01-02T03:04:05Z".parse().unwrap();
    let status = ChainStatus {
        sense: ants_proto::SenseId::new(),
        incarnation: "i".into(),
        checked_at,
        events: 2,
        bytes: SIZE_LIMIT,
        seen: None,
        broken: None,
        ant: None,
    };
    write_status(&dir.join("dev-a"), &status).unwrap();
    let broken = Break { index: 3, sequence: Some(7), reason: "not signed by dev-b's credentials".into() };
    write_status(&dir.join("dev-b"), &ChainStatus { broken: Some(broken), bytes: SIZE_LIMIT + 1, ..status.clone() })
        .unwrap();

    let checks = check_sense_logs(&loaded.ants);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status, c.detail.as_str())).collect::<Vec<_>>(),
        [
            ("ant dev-a sense chain", Status::Ok, "2 events verified at 2026-01-02 03:04:05 UTC"),
            ("ant dev-a sense size", Status::Ok, "32.0 MiB (33554432 bytes) of sense data, within the 32 MiB limit"),
            (
                "ant dev-b sense chain",
                Status::Fail,
                &*format!(
                    "sense identity {} breaks at event 3 (stream sequence 7): not signed by dev-b's credentials, as \
                     verified at 2026-01-02 03:04:05 UTC",
                    status.sense
                )
            ),
            (
                "ant dev-b sense size",
                Status::Warn,
                &*format!(
                    "32.0 MiB (33554433 bytes) of sense data in sense identity {}, over the 32 MiB limit",
                    status.sense
                )
            ),
        ],
        "nothing for an ant whose log was never read"
    );
    assert!(checks[3].fix[0].contains("`ants ant memories dev-b`"), "{}", checks[3]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_watcher_failing_its_checks_warns_naming_why() {
    use ants_sense::remind::watch::{WatcherState, WatcherStatus, write_status};

    let dir = ants_test_support::scratch_dir("doctor-watchers").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let ants = ["dev-a", "dev-b", "dev-c"]
        .map(|name| format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\nhome = \"{name}\"\n"));
    std::fs::write(
        &config,
        format!("messenger = false\nsage = \"none\"\nharness = \"copilot\"\nhuman = \"human\"\n{}", ants.concat()),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let watcher = WatcherState {
        id: "01M876XB7HEVCSRA95C0ACNJPH".parse().unwrap(),
        title: "PR 7".into(),
        runs: "gh pr checks 7".into(),
        failed_checks: 2,
        error: None,
    };
    let status = WatcherStatus { checked_at: "2026-01-02T03:04:05Z".parse().unwrap(), watchers: vec![watcher.clone()] };
    write_status(&dir.join("dev-a"), &status).unwrap();
    let failing = WatcherState {
        failed_checks: 3,
        error: Some("the command is outside the ant's tool allow rules".into()),
        ..watcher
    };
    write_status(&dir.join("dev-b"), &WatcherStatus { watchers: vec![failing], ..status }).unwrap();

    let checks = check_watchers(&loaded.ants);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status, c.detail.as_str())).collect::<Vec<_>>(),
        [
            (
                "ant dev-a watchers",
                Status::Ok,
                "1 active, none failing its checks, as checked at 2026-01-02 03:04:05 UTC"
            ),
            (
                "ant dev-b watchers",
                Status::Warn,
                "01M876XB7HEVCSRA95C0ACNJPH (PR 7, running gh pr checks 7) failed 3 checks in a row, the latest because \
                 the command is outside the ant's tool allow rules, as checked at 2026-01-02 03:04:05 UTC"
            ),
        ],
        "nothing for an ant without watchers checked"
    );
    assert!(checks[1].fix[0].contains("`execute:` rule"), "{}", checks[1]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn sense_keeping_none_of_its_ants_data_fails_naming_the_fixes() {
    let ant = |name: &str, creds: bool, setting| Ant {
        creds: creds.then(|| PathBuf::from(format!("{name}.creds"))),
        provider_setting: setting,
        ..Ant::new(name.parse().unwrap(), PathBuf::from("/w"))
    };
    let (sense, none) = (ants_daemon::provider::Setting::Sense, ants_daemon::provider::Setting::None);
    let mut host = config::HostSettings::default();
    let check =
        |host: &config::HostSettings, ants: &[Ant]| check_sense_kept(host, ants, "box.local", &[], chrono::Utc::now());
    assert!(check(&host, &[ant("dev-a", false, sense.clone())]).is_none(), "no authentication: kept as no one");
    let ants = [ant("dev-a", true, sense.clone()), ant("dev-b", true, none.clone()), ant("dev-c", true, sense)];
    let failed = check(&host, &ants).unwrap();
    assert_eq!((failed.status, failed.check.as_str()), (Status::Fail, "sense data"), "{failed}");
    assert!(failed.detail.contains("none of the data of dev-a, dev-c: no host credentials"), "{failed}");
    assert!(failed.fix[0].starts_with("ants auth host box ") && failed.fix[1].contains("provider = \"none\""));
    assert!(check(&host, &[ant("dev-b", true, none)]).is_none(), "no ant uses sense");
    host.creds = Some(PathBuf::from("host.creds"));
    assert!(check(&host, &ants).is_none(), "sense writes the logs with the host credentials");
}

#[test]
fn sense_keeping_none_of_the_data_of_an_ant_its_colony_entry_names_fails_without_a_config_file() {
    let now = chrono::Utc::now();
    let entry = |ant: &str, machine: &str, unkept: Option<&str>, heartbeat: chrono::DateTime<chrono::Utc>| {
        let json = serde_json::json!({
            "ant": ant, "machine": machine, "instance": "run-1", "harness": "copilot", "capabilities": [],
            "started_at": heartbeat, "heartbeat_at": heartbeat, "activity": "idle", "handling": 0, "harnesses": 0,
            "version": ants_proto::ANTS_VERSION, "perms": PERMISSIONS_VERSION, "sense_unkept": unkept,
        });
        serde_json::from_value::<ants_proto::ColonyEntry>(json).unwrap()
    };
    let colony = [
        entry("dev-a", "box.local", Some("no host credentials on box"), now),
        entry("dev-b", "box.local", None, now),
        entry("dev-c", "other.local", Some("no host credentials on other"), now),
        entry("dev-d", "box.local", Some("no host credentials on box"), now - chrono::Duration::hours(1)),
    ];
    let mut host = config::HostSettings::default();
    let failed = check_sense_kept(&host, &[], "box.local", &colony, now).unwrap();
    assert_eq!((failed.status, failed.check.as_str()), (Status::Fail, "sense data"), "{failed}");
    let told = "the colony entry of dev-a says sense keeps none of its data: no host credentials on box";
    assert_eq!(failed.detail, told, "a live entry of this machine alone");
    assert!(failed.fix[0].starts_with("ants auth host box ") && failed.fix.len() == 1, "{failed}");
    assert!(check_sense_kept(&host, &[], "box.local", &colony[1..], now).is_none());
    host.top_machine = Some("other.local".to_owned());
    let failed = check_sense_kept(&host, &[], "box.local", &colony[1..], now).unwrap();
    assert!(failed.detail.contains("entry of dev-c "), "the config's machine names this one too: {failed}");
    host.creds = Some(PathBuf::from("host.creds"));
    let failed = check_sense_kept(&host, &[], "box.local", &colony[1..], now).unwrap();
    assert!(failed.fix[0].starts_with("reload `ants daemon` with SIGHUP"), "host credentials now here: {failed}");
    assert!(failed.fix[0].ends_with("with the host credentials at host.creds") && failed.fix.len() == 1, "{failed}");
    host.creds = None;
    let ant =
        Ant { creds: Some(PathBuf::from("dev-a.creds")), ..Ant::new("dev-a".parse().unwrap(), PathBuf::from("/w")) };
    let failed = check_sense_kept(&host, &[ant], "box.local", &colony[..1], now).unwrap();
    assert!(failed.detail.starts_with("sense keeps none of the data of dev-a:"), "{failed}");
    assert!(!failed.detail.contains("colony entry"), "an ant named once: {failed}");
}

#[test]
fn ants_whose_harness_runs_tools_unasked_are_warned_of_rules_that_miss_them() {
    let dir = ants_test_support::scratch_dir("doctor-bypassed");
    let config = dir.join("ants.toml");
    let creds = dir.join("nightly.creds");
    std::fs::write(
        &config,
        format!(
            "messenger = false\nharness = \"claude\"\n\
             [harnesses.gated]\ncommand = \"claude-agent-acp\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nallow = [\"read:*\"]\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\n\
             [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nharness = \"gated\"\nallow = [\"read:*\"]\n\
             [[ant]]\nname = \"dev-d\"\nworkdir = \".\"\ntools = \"ask\"\nallow = [\"read:*\"]\n\
             [[schedule]]\nname = \"nightly\"\ncreds = {creds:?}\nevery = \"4h\"\nant = \"dev-b\"\ntext = \"t\"\nallow = [\"read:*\"]\n"
        ),
    )
    .unwrap();
    let loaded = config::load(&config).unwrap();
    let checks = check_bypassed(&loaded, &[]);
    assert_eq!(
        checks.iter().map(|c| (c.check.as_str(), c.status)).collect::<Vec<_>>(),
        [("ant dev-a tool permissions", Status::Warn), ("ant dev-b tool permissions", Status::Warn)]
    );
    assert!(checks[1].detail.contains("schedule nightly narrows its threads to"), "{}", checks[1]);
    assert!(checks[0].fix[0].starts_with("set tools = \"ask\" on the ant or in [harnesses.claude]"), "{}", checks[0]);

    std::fs::write(&config, "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n").unwrap();
    let mut checks = Vec::new();
    assert!(check_config(&config, &mut checks).is_none());
    assert!(checks[0].fix[0].starts_with("add `harness = \"NAME\"` at the top of"), "{}", checks[0]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn caps_a_harness_or_fallback_cannot_count_are_warned_of() {
    let dir = ants_test_support::scratch_dir("doctor-uncounted-caps");
    let config = dir.join("ants.toml");
    std::fs::write(
        &config,
        "messenger = false\nsage = \"none\"\nharness = \"claude\"\n\
         [harnesses.mine]\ncommand = \"mine-acp\"\n\
         [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nmax_daily_cost = 5.0\nmax_thread_tokens = 1000\n\
         fallback = [\"codex\", \"copilot\", \"mine\"]\n\
         [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nmax_daily_cost = 5.0\n\
         [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nharness = \"copilot\"\n",
    )
    .unwrap();
    let checks = check_caps(&config::load(&config).unwrap());
    let found: Vec<_> = checks.iter().map(|c| (c.check.as_str(), c.status, c.detail.as_str())).collect();
    assert_eq!(
        found,
        [
            (
                "ant dev-a spending caps",
                Status::Warn,
                "harness codex reports no cost, so max_daily_cost never trips on it"
            ),
            (
                "ant dev-a spending caps",
                Status::Warn,
                "harness copilot reports neither tokens nor cost, so max_thread_tokens and max_daily_cost never trip on \
                 it"
            ),
        ],
        "claude counts both, mine says nothing of what it reports, and dev-c has no caps"
    );
    assert!(checks[0].fix[0].starts_with("set caps of what harness codex reports"), "{}", checks[0]);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_sage_on_a_harness_that_cannot_hold_reviews_is_warned_of_declining_them() {
    let dir = ants_test_support::scratch_dir("doctor-unheld-reviews");
    let config = dir.join("ants.toml");
    let reviews = |harness: &str| {
        std::fs::write(
            &config,
            format!("messenger = false\nharness = \"{harness}\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n"),
        )
        .unwrap();
        let checks = check_bypassed(&config::load(&config).unwrap(), &[]);
        checks.into_iter().filter(|c| c.check == "ant system-sage reviews").collect::<Vec<_>>()
    };
    let warned = reviews("antigravity");
    assert_eq!(warned.len(), 1);
    assert!(warned[0].detail.contains("harness antigravity runs its tools unasked"), "{}", warned[0]);
    assert!(warned[0].fix[0].starts_with("set review in [harnesses.antigravity]"), "{}", warned[0]);
    assert!(reviews("claude").is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}
