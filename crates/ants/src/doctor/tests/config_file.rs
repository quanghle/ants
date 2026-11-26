//! Tests of the doctor's config file checks: a machine name at the top level, a config leaving every ant to
//! another machine, and one of the CLI alone.

use super::*;

#[test]
fn a_top_level_machine_no_machine_name_is_warned_of_unless_the_host_table_names_one() {
    let mut host = config::HostSettings::default();
    assert!(check_machine_name(&host, "box.local").is_none());
    host.top_machine = Some("dev-vm1".to_owned());
    assert!(check_machine_name(&host, "box.local").is_none());
    host.top_machine = Some("Dev VM.1".to_owned());
    let check = check_machine_name(&host, "box.local").unwrap();
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.contains("name the machine box ") && check.fix[0].contains("\"Dev-VM-1\""), "{check}");
    host.machine = Some("m1".parse().unwrap());
    assert!(check_machine_name(&host, "box.local").is_none(), "the [host] table names the machine");
}

#[test]
fn a_config_leaving_every_ant_to_another_machine_fails_naming_the_builtin_ones() {
    let auth = std::env::home_dir().unwrap().join(".config/ants/auth");
    if ["system-messenger", "system-sage"].iter().any(|name| auth.join(format!("{name}.creds")).is_file()) {
        return;
    }
    let dir = ants_test_support::scratch_dir("doctor-elsewhere");
    let config = dir.join("ants.toml");
    let check = |text: &str| {
        std::fs::write(
            &config,
            format!("harness = \"copilot\"\nhuman = \"human\"\n[host]\ncreds = \"h.creds\"\n{text}"),
        )
        .unwrap();
        let mut checks = Vec::new();
        assert!(check_config(&config, &mut checks).is_some());
        checks.remove(0)
    };
    let nothing = check("");
    assert_eq!(nothing.status, Status::Fail, "{nothing}");
    let messenger = format!(
        "the built-in system-messenger, without credentials at {} here",
        auth.join("system-messenger.creds").display()
    );
    assert!(nothing.detail.contains(&messenger) && nothing.detail.ends_with("nothing to serve here"), "{nothing}");
    let some = check("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n");
    assert_eq!(some.status, Status::Ok, "{some}");
    assert!(some.detail.contains(" lists dev-a; the built-in system-messenger, without credentials at"), "{some}");
    let served = format!(
        "system-sage.creds here, is left to another machine, unless served with credentials kept elsewhere: ants \
         daemon --config {} --ant system-sage --creds CREDS",
        config.display()
    );
    assert!(some.detail.ends_with(&served), "{some}");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A machine where only humans run the CLI: the file `ants login` writes, without [[ant]] or harness, the CLI's
/// credentials telling the server takes some, so the messenger without its own is left to another machine.
#[tokio::test]
async fn a_cli_only_config_passes_and_its_ants_checks_skip() {
    let auth = std::env::home_dir().unwrap().join(".config/ants/auth");
    if auth.join("system-messenger.creds").is_file() {
        return;
    }
    let dir = ants_test_support::scratch_dir("doctor-cli-only").canonicalize().unwrap();
    let config = dir.join("config.toml");
    std::fs::write(
        &config,
        "js_domain = \"hub\"\nnats = \"nats://127.0.0.1:4222\"\n\n[cli]\ncreds = \"~/.config/ants/auth/alice.creds\"\n",
    )
    .unwrap();
    let mut checks = Vec::new();
    let loaded = check_config(&config, &mut checks).unwrap();
    if loaded.host.creds.is_some() {
        // Host credentials here mean a daemon is meant to run, which a file serving no ants fails.
        assert_eq!(checks[0].status, Status::Fail, "{}", checks[0]);
        return std::fs::remove_dir_all(&dir).unwrap();
    }
    let check = &checks[0];
    assert_eq!(check.status, Status::Ok, "{check}");
    assert!(check.detail.contains("lists no ants to serve here, as on a machine where only humans run the CLI"));
    assert!(check.detail.contains("the built-in system-messenger, without credentials at"), "{check}");
    let setup = Setup {
        nats: "nats://127.0.0.1:1".to_owned(),
        creds: None,
        js_domain: None,
        human: None,
        config: config.clone(),
        leaf_config: dir.join("leaf.conf"),
        hub_config: dir.join("no-nats.conf"),
        nats_conf: None,
        harnesses: true,
        team: dir.join("no-team"),
        webhooks: dir.join("no-webhooks.toml"),
        home: None,
        worker: None,
    };
    let ants = check_ants(&loaded.ants, &loaded.harnesses, &setup, 0).await;
    assert_eq!(ants.iter().map(|c| (c.check.as_str(), c.status)).collect::<Vec<_>>(), [("ants", Status::Skip)]);
    assert!(ants[0].detail.ends_with("no ant credentials or harness sign-in to check"), "{}", ants[0]);
    std::fs::remove_dir_all(&dir).unwrap();
}
