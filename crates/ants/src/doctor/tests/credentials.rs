//! Tests of the doctor's credentials checks: the backup of the keys issued, host credentials, stored seeds and
//! encryption keys.

use super::*;

#[test]
fn the_backup_check_warns_until_a_backup_holds_every_key_issued() {
    let root = ants_test_support::scratch_dir("doctor-backup");
    let team = root.join("auth");
    assert_eq!(check_backup(&team), None, "no account keys here");
    auth::init(&team).unwrap();
    let check = check_backup(&team).unwrap();
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.contains("no backup") && check.fix[0].contains("ants auth backup FILE"), "{check}");
    backup::backup(&team, &root.join("team.age"), "passphrase", 10).unwrap();
    assert_eq!(check_backup(&team).unwrap().status, Status::Ok);
    let key = nkeys::KeyPair::new_user().public_key();
    issued::record(&team, &issued::Issued::new(&key, "dev-a", issued::Kind::Ant, None).unwrap()).unwrap();
    let check = check_backup(&team).unwrap();
    assert_eq!(check.status, Status::Warn, "{check}");
    assert!(check.detail.contains("predates 1 issued.jsonl entry"), "{check}");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn the_host_check_wants_host_credentials_of_the_machine_once_ants_have_credentials() {
    let dir = ants_test_support::scratch_dir("doctor-host");
    let team = dir.join("team");
    auth::init(&team).unwrap();
    let mut ant = Ant::new("dev-a".parse().unwrap(), dir.clone());
    let mut host = config::HostSettings::default();
    let check = |host: &config::HostSettings, ant: &Ant| check_host(host, std::slice::from_ref(ant), "box.local", 0);
    assert_eq!(check(&host, &ant).status, Status::Skip, "no credentials anywhere: the host connects as no one");
    ant.creds = Some(dir.join("dev-a.creds"));
    let off = check(&host, &ant);
    assert!(off.status == Status::Warn && off.to_string().contains("ants auth host box "), "{off}");
    host.top_machine = Some("dev-vm1".to_owned());
    let named = check(&host, &ant);
    assert!(named.to_string().contains("ants auth host dev-vm1 "), "the top-level machine names it: {named}");
    let path = dir.join("host-m1.creds");
    auth::issue_host(&team, &"m1".parse().unwrap(), None, &path).unwrap();
    host.creds = Some(path);
    assert_eq!(check(&host, &ant).status, Status::Ok, "{}", check(&host, &ant));
    host.machine = Some("m2".parse().unwrap());
    let other = check(&host, &ant);
    assert!(other.status == Status::Fail && other.to_string().contains("ants auth host m2 "), "{other}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn the_key_check_finds_a_stored_seed_locked_missing_or_of_another_key() {
    let dir = ants_test_support::scratch_dir("doctor-key");
    let team = dir.join("team");
    auth::init(&team).unwrap();
    let user = nkeys::KeyPair::new_user();
    let (key, seed) = (user.public_key(), user.seed().unwrap());
    let jwt = auth::sign_user(&team, &"dev-a".parse().unwrap(), Role::Ant, None, &key).unwrap();
    let path = dir.join("dev-a.creds");
    let store = keystore::Memory::default();
    let check = |store: &keystore::Memory| {
        let check = check_key("ant dev-a key", &path, vec!["ants add dev-a".to_owned()], store).unwrap();
        let shown = check.to_string();
        assert!(!shown.contains(&seed), "{shown}");
        (check.status, shown)
    };

    auth::write_creds(&path, &jwt, &user).unwrap();
    assert!(check_key("ant dev-a key", &path, Vec::new(), &store).is_none(), "the seed is in the file");
    std::fs::write(&path, creds::stored_text(&jwt, &key)).unwrap();
    store.set(&key, &seed).unwrap();
    assert_eq!(check(&store).0, Status::Ok);
    store.lock();
    let (status, shown) = check(&store);
    assert!(status == Status::Fail && shown.contains("locked") && shown.contains("unlock"), "{shown}");
    store.unlock();
    store.set(&key, &auth::seed(&nkeys::KeyPair::new_user()).unwrap()).unwrap();
    let (status, shown) = check(&store);
    assert!(status == Status::Fail && shown.contains("another key") && shown.contains("ants add"), "{shown}");
    store.delete(&key).unwrap();
    let (status, shown) = check(&store);
    assert!(status == Status::Fail && shown.contains("holds no seed") && shown.contains("backup"), "{shown}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn the_xkey_check_warns_of_a_human_device_binding_none_and_fails_on_a_lost_seed() {
    let dir = ants_test_support::scratch_dir("doctor-xkey");
    let team = dir.join("team");
    auth::init(&team).unwrap();
    let alice: UserId = "alice".parse().unwrap();
    let path = dir.join("alice.creds");
    let store = keystore::Memory::default();
    let rights = || ants_proto::Permissions::for_user(&alice, Role::Human);
    let check = |path: &Path| {
        let text = std::fs::read_to_string(path).unwrap();
        let check = check_xkey("credentials xkey", path, &creds::Finding::from_creds(&text).unwrap(), &store).unwrap();
        let shown = check.to_string();
        if let Some(Seed::File(seed)) = creds::xkey_of(&text) {
            assert!(!shown.contains(seed), "{shown}");
        }
        (check.status, shown)
    };

    auth::Issue::new(&team, &alice, issued::Kind::Human, rights()).into_file(&path).unwrap();
    let (status, shown) = check(&path);
    assert!(status == Status::Ok && shown.contains("in the file"), "{shown}");
    let text = std::fs::read_to_string(&path).unwrap();
    let Some(Seed::File(seed)) = creds::xkey_of(&text) else { panic!("{text}") };
    let other = auth::xseed(&nkeys::XKey::new()).unwrap();
    std::fs::write(&path, text.replace(seed, &other)).unwrap();
    let (status, shown) = check(&path);
    assert!(status == Status::Fail && shown.contains("not that of the key") && shown.contains("backup"), "{shown}");

    let user = nkeys::KeyPair::new_user();
    let (jwt, _) =
        auth::Issue::new(&team, &alice, issued::Kind::Human, rights()).for_key(&user.public_key(), None).unwrap();
    auth::write_creds(&path, &jwt, &user).unwrap();
    let (status, shown) = check(&path);
    assert!(
        status == Status::Warn && shown.contains("bind no encryption key") && shown.contains("ants renew"),
        "{shown}"
    );
    let ant = auth::sign_user(&team, &"dev-a".parse().unwrap(), Role::Ant, None, &user.public_key()).unwrap();
    let finding = creds::Finding::from_jwt(&ant).unwrap();
    assert!(check_xkey("credentials xkey", &path, &finding, &store).is_none(), "an ant's bind none");
    std::fs::remove_dir_all(&dir).unwrap();
}
