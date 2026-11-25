//! A bus set up before the store account: `ants auth init` adds it.

use super::*;

/// The account a `server.conf` preloads under `key`, with its claims.
fn preloaded(conf: &str, key: &str) -> Option<serde_json::Value> {
    let jwt = conf.lines().find_map(|line| line.trim().strip_prefix(&format!("{key}: ")))?;
    Some(verified_body(jwt).unwrap().1)
}

#[test]
fn a_team_made_before_the_store_account_gets_one_with_its_operator_and_revocations() {
    let dir = ants_test_support::scratch_dir("add-store");
    init(&dir).unwrap();
    issue(&dir, &p("dev-a"), Role::Ant, None, &dir.join("dev-a.creds")).unwrap();
    let revoked = crate::credentials::revoke::revoke(&dir, Some("dev-a"), &[]).unwrap();
    crate::credentials::revoke::apply(&dir).unwrap();
    write_server_conf_before_store(&dir).unwrap();
    fs::remove_file(dir.join(STORE_SEED)).unwrap();
    let before = fs::read_to_string(dir.join(SERVER_CONF)).unwrap();
    let account = account_key(&dir).unwrap();
    assert_eq!(preloaded_accounts(&before).unwrap(), (account.clone(), None));

    let e = crate::credentials::revoke::apply(&dir).unwrap_err();
    assert!(e.to_string().contains("no store account: `ants auth init` adds it"), "{e:#}");
    assert!(init(&dir).is_err(), "a new team over the old one");
    add_store(&dir).unwrap();

    let conf = fs::read_to_string(dir.join(SERVER_CONF)).unwrap();
    let store = store_key(&dir).unwrap();
    assert_eq!(preloaded_accounts(&conf).unwrap(), (account.clone(), Some(store.clone())));
    let operator = |conf: &str| conf.lines().find(|line| line.starts_with("operator: ")).unwrap().to_owned();
    assert_eq!(operator(&conf), operator(&before), "nats-server reloads no new operator");
    let team = preloaded(&conf, &account).unwrap();
    assert_eq!(team["nats"]["limits"]["disk_storage"], 0, "{team}");
    assert!(team["nats"]["revocations"].get(&revoked[0].key).is_some(), "{team}");
    assert_eq!(preloaded(&conf, &store).unwrap()["nats"]["limits"]["disk_storage"], -1);
    assert!(add_store(&dir).unwrap_err().to_string().contains("already holds account keys with a store account"));
    fs::remove_dir_all(dir).unwrap();
}
