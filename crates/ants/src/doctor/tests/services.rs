//! Tests of the doctor's service checks: the hub service of `ants setup` and the nats-server the daemon runs.

use super::*;

#[cfg(unix)]
#[test]
fn the_hub_service_of_ants_setup_must_be_installed_and_running() {
    let home = ants_test_support::scratch_dir("doctor-hub-service").canonicalize().unwrap();
    let hub_config = home.join("nats.conf");
    let setup = Setup {
        nats: "nats://127.0.0.1:4222".to_owned(),
        creds: None,
        js_domain: None,
        config: home.join("no-config.toml"),
        leaf_config: home.join("leaf.conf"),
        hub_config: hub_config.clone(),
        harnesses: false,
        team: home.join("no-team"),
        webhooks: home.join("no-webhooks.toml"),
        home: Some(home.clone()),
        worker: None,
    };
    let stopped = || {
        Services::answering(&home, |command| {
            let (ok, stdout) = match command {
                "id -u" => (true, "501\n"),
                c if c.starts_with("launchctl print") => (true, "\tstate = not running\n"),
                _ => (false, ""),
            };
            crate::service::Ran { ok, stdout: stdout.to_owned(), stderr: String::new() }
        })
    };
    assert!(
        named(&check_services(&setup, &mut stopped(), None), "hub service").is_empty(),
        "nothing without nats.conf"
    );

    std::fs::write(&hub_config, "listen: 127.0.0.1:4222\n").unwrap();
    let checks = check_services(&setup, &mut stopped(), None);
    let missing = one(&checks, "hub service");
    assert_eq!(missing.status, Status::Warn, "{missing}");
    assert_eq!(missing.fix, [format!("scripts/install-units.sh --nats-conf {}", hub_config.display())]);
    assert_eq!(one(&checks, "leaf node service").status, Status::Skip, "{checks:#?}");

    if !install_units(&home, &["--nats-conf", hub_config.to_str().unwrap()]) {
        return;
    }
    let checks = check_services(&setup, &mut stopped(), None);
    let down = one(&checks, "hub service");
    assert_eq!(down.status, Status::Fail, "{down}");
    assert_eq!(down.detail, format!("installed for {}, not running", hub_config.display()));
    let start = if cfg!(target_os = "macos") {
        "launchctl kickstart gui/501/ants.nats"
    } else {
        "systemctl --user start ants-nats"
    };
    assert_eq!(down.fix, [start]);
    assert_eq!(one(&check_services(&setup, &mut running(&home), None), "hub service").status, Status::Ok);

    std::fs::write(home.join("leaf.conf"), "").unwrap();
    let checks = check_services(&setup, &mut running(&home), None);
    assert!(named(&checks, "hub service").is_empty(), "the leaf node is the machine's nats-server: {checks:#?}");
    assert_eq!(one(&checks, "leaf node service").status, Status::Warn, "{checks:#?}");
    std::fs::remove_dir_all(&home).unwrap();
}

#[cfg(unix)]
#[test]
fn a_nats_server_run_by_the_daemon_has_the_daemon_service_as_its_one_owner() {
    let home = ants_test_support::scratch_dir("doctor-managed-nats").canonicalize().unwrap();
    let (leaf, config) = (home.join("leaf.conf"), home.join("config.toml"));
    std::fs::write(&leaf, "listen: 127.0.0.1:4222\n").unwrap();
    std::fs::write(&config, format!("[nats_server]\nconfig = {leaf:?}\n")).unwrap();
    let setup = Setup {
        nats: "nats://127.0.0.1:4222".to_owned(),
        creds: None,
        js_domain: None,
        config: config.clone(),
        leaf_config: leaf.clone(),
        hub_config: home.join("nats.conf"),
        harnesses: false,
        team: home.join("no-team"),
        webhooks: home.join("no-webhooks.toml"),
        home: Some(home.clone()),
        worker: None,
    };
    let managed = Some(leaf.as_path());
    let run_by = format!("{} is run by `ants daemon` ([nats_server] of {})", leaf.display(), config.display());
    let checks = check_services(&setup, &mut running(&home), managed);
    let missing = one(&checks, "leaf node service");
    assert_eq!((missing.status, &missing.detail), (Status::Warn, &format!("{run_by}; no daemon service installed")));
    assert_eq!(missing.fix, [format!("scripts/install-units.sh --managed-nats --config {}", config.display())]);

    if !install_units_as(&home, &["--managed-nats", "--ants", "/bin/sh", "--config", config.to_str().unwrap()]) {
        return;
    }
    let checks = check_services(&setup, &mut running(&home), managed);
    let owned = one(&checks, "leaf node service");
    assert_eq!((owned.status, &owned.detail), (Status::Ok, &format!("{run_by}, whose service runs")));
    assert_eq!(one(&checks, "daemon service").status, Status::Ok, "{checks:#?}");

    assert!(install_units(&home, &[]));
    let checks = check_services(&setup, &mut running(&home), managed);
    let two = one(&checks, "leaf node service");
    assert_eq!(two.status, Status::Fail, "{two}");
    let table = format!("[nats_server] of {}", config.display());
    let owners =
        format!("two owners of {}: an ants-nats service runs it, and `ants daemon` too, from {table}", leaf.display());
    assert_eq!(two.detail, owners);
    let remove = if cfg!(target_os = "macos") {
        format!(
            "launchctl bootout gui/$(id -u)/ants.nats && rm {}",
            home.join("Library/LaunchAgents/ants.nats.plist").display()
        )
    } else {
        format!(
            "systemctl --user disable --now ants-nats && rm {}",
            home.join(".config/systemd/user/ants-nats.service").display()
        )
    };
    assert_eq!(two.fix, [remove, format!("or remove {table}")]);
    std::fs::remove_dir_all(&home).unwrap();
}
