//! The host: its name, and its credentials as the host table names them or as found in the auth directory.

use super::*;

#[test]
fn the_host_table_names_credentials_else_they_are_found_in_the_auth_directory() {
    let home = ants_test_support::scratch_dir("config-host").canonicalize().unwrap();
    let path = home.join("config.toml");
    let host = |text: &str| Summary::parse(text, &path, &home).unwrap().host;
    let top = "nats = \"nats://hub:4222\"\njs_domain = \"hub\"\n";
    let none = host(top);
    assert_eq!((none.creds, none.machine), (None, None));
    assert_eq!(
        (none.nats.as_deref(), none.js_domain.map(|d| d.to_string())),
        (Some("nats://hub:4222"), Some("hub".into()))
    );
    let auth = home.join(".config/ants/auth");
    std::fs::create_dir_all(&auth).unwrap();
    std::fs::write(auth.join("host-box.creds"), "").unwrap();
    assert_eq!(host("[host]\nmachine = \"box\"").creds, Some(auth.join("host-box.creds")));
    std::fs::write(auth.join("host.creds"), "").unwrap();
    assert_eq!(host("[host]\nmachine = \"box\"").creds, Some(auth.join("host.creds")));
    assert_eq!(host("[host]\ncreds = \"h.creds\"").creds, Some(home.join("h.creds")));
    assert!(Summary::parse("[host]\nuser = \"x\"", &path, &home).is_err(), "unknown keys are refused");
    assert!(none.remote_admin, "admin orders are carried out unless the file says otherwise");
    assert!(!host("[admin]\nremote = false").remote_admin);
    assert!(Summary::parse("[admin]\nlocal = false", &path, &home).is_err(), "unknown keys are refused");
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn the_top_level_machine_names_the_host_where_the_host_table_names_none() {
    let home = ants_test_support::scratch_dir("config-host-top").canonicalize().unwrap();
    let path = home.join("config.toml");
    let host = |text: &str| Summary::parse(text, &path, &home).unwrap().host;
    let m = |name: &str| name.parse::<Machine>().unwrap();
    let local = Machine::from_hostname(&hostname().unwrap());
    let top = host("machine = \"dev-vm1\"");
    assert_eq!((top.fallback(), top.machine_or("gen-4f2a.internal")), (Some(m("dev-vm1")), m("dev-vm1")));
    let both = host("machine = \"dev-vm1\"\n[host]\nmachine = \"box\"");
    assert_eq!(both.machine_or("gen-4f2a.internal"), m("box"), "the [host] table's machine comes first");
    let colony_only = host("machine = \"Dev VM 1\"");
    assert_eq!(colony_only.top_machine.as_deref(), Some("Dev VM 1"));
    assert_eq!((colony_only.fallback(), colony_only.machine_or("gen-4f2a.internal")), (None, m("gen-4f2a")));

    let auth = home.join(".config/ants/auth");
    std::fs::create_dir_all(&auth).unwrap();
    std::fs::write(auth.join(format!("host-{local}.creds")), "").unwrap();
    assert_eq!(host("").creds, Some(auth.join(format!("host-{local}.creds"))));
    assert_eq!(
        host("machine = \"dev-vm1\"").creds,
        Some(auth.join(format!("host-{local}.creds"))),
        "the host name's credentials still serve when the top-level machine has none"
    );
    std::fs::write(auth.join("host-dev-vm1.creds"), "").unwrap();
    assert_eq!(host("machine = \"dev-vm1\"").creds, Some(auth.join("host-dev-vm1.creds")));
    std::fs::remove_dir_all(&home).unwrap();
}

#[test]
fn the_host_credentials_candidates_are_those_looked_up_whether_they_exist_or_not() {
    let home = ants_test_support::scratch_dir("config-host-candidates").canonicalize().unwrap();
    let path = home.join("config.toml");
    let candidates = |text: &str| Summary::parse(text, &path, &home).unwrap().host.creds_candidates("gen-4f2a.local");
    let auth = home.join(".config/ants/auth");
    let creds = |names: &[&str]| names.iter().map(|name| auth.join(name)).collect::<Vec<_>>();
    assert_eq!(candidates(""), creds(&["host.creds", "host-gen-4f2a.creds"]), "no auth directory yet");
    assert_eq!(
        candidates("machine = \"dev-vm1\""),
        creds(&["host.creds", "host-dev-vm1.creds", "host-gen-4f2a.creds"])
    );
    assert_eq!(
        candidates("machine = \"dev-vm1\"\n[host]\nmachine = \"box\""),
        creds(&["host.creds", "host-box.creds"])
    );
    assert_eq!(candidates("[host]\ncreds = \"h.creds\""), Vec::<std::path::PathBuf>::new(), "named, none looked up");
    std::fs::remove_dir_all(&home).unwrap();
}
