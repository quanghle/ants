//! Tests of the doctor's check of the humans the config file names: its top-level `human`, an ant's own `human` and
//! each `stand_in`, warned of, naming the key, when `ants_humans` has no entry of them.

use ants_proto::Humans;

use super::super::humans::{findings, named_humans};
use super::*;

fn p(id: &str) -> UserId {
    id.parse().unwrap()
}

/// A config file in `dir` whose top-level `human` is alice, dev-a's `stand_in` bob and dev-b's own `human` carol.
fn config_in(dir: &Path) -> PathBuf {
    let path = dir.join("ants.toml");
    let text = [
        r#"harness = "copilot""#,
        r#"human = "alice""#,
        r#"messenger = false"#,
        r#"[[ant]]"#,
        r#"name = "dev-a""#,
        r#"workdir = ".""#,
        r#"stand_in = "bob""#,
        r#"[[ant]]"#,
        r#"name = "dev-b""#,
        r#"workdir = ".""#,
        r#"human = "carol""#,
    ]
    .join("\n");
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn a_human_or_stand_in_no_human_of_the_bus_is_warned_of_naming_the_key() {
    let dir = ants_test_support::scratch_dir("doctor-config-humans");
    let path = config_in(&dir);
    let loaded = config::load(&path).unwrap();
    let named = named_humans(Some(&p("alice")), &loaded.ants);
    let keys: Vec<(&str, &str)> = named.iter().map(|(key, human)| (key.as_str(), human.as_str())).collect();
    assert_eq!(
        keys,
        [
            ("the top-level `human`", "alice"),
            ("`stand_in` of the [[ant]] dev-a", "bob"),
            ("`human` of the [[ant]] dev-b", "carol"),
        ]
    );
    let checks = findings(&named, &Humans::of([p("alice"), p("bob"), p("carol")]), &path);
    assert_eq!(checks.len(), 1, "{checks:#?}");
    assert_eq!(checks[0].status, Status::Ok, "{}", checks[0]);
    assert_eq!(checks[0].detail, format!("{} names alice, bob, carol, humans of the bus", path.display()));

    let checks = findings(&named, &Humans::of([p("carol")]), &path);
    let shown: Vec<(Status, &str)> = checks.iter().map(|c| (c.status, c.fix[0].as_str())).collect();
    let change = |key: &str| format!("change {key} in {} to a human of the bus", path.display());
    assert_eq!(
        shown,
        [
            (Status::Warn, change("the top-level `human`").as_str()),
            (Status::Warn, change("`stand_in` of the [[ant]] dev-a").as_str()),
        ]
    );
    assert!(
        checks[1].detail.starts_with(&format!(
            "`stand_in` of the [[ant]] dev-a in {} names bob, not a human of the bus (ants_humans has no entry, as \
             after `ants auth revoke bob`)",
            path.display()
        )),
        "{}",
        checks[1]
    );
    assert!(named_humans(None, &[]).is_empty());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn the_doctor_reads_the_humans_of_the_bus_with_the_cli_s_credentials() {
    let Some(server) = Server::start() else { return };
    let alice = p("alice");
    let mut setup = setup(&server, Some(server.creds_as(&alice, Role::Human)));
    setup.config = config_in(server.dir());
    let humans_check = async |setup: &Setup| {
        let checks = Box::pin(run(setup)).await;
        named(&checks, "config humans").into_iter().cloned().collect::<Vec<_>>()
    };
    let checks = humans_check(&setup).await;
    assert_eq!(checks.len(), 1, "{checks:#?}");
    assert_eq!(checks[0].status, Status::Skip, "no {} before ants init: {}", ants_proto::HUMANS_BUCKET, checks[0]);

    let (_, js) = server.connect_as(&alice, Role::Human).await;
    let rights = Rights::from_creds(setup.creds.as_deref()).unwrap();
    hub::init(&js, |s| rights.may_publish(s)).await.unwrap();
    let me = Me { id: alice.clone(), role: Role::Human, assistant_of: None };
    crate::people::humans::register(&js, &me).await.unwrap();
    let checks = humans_check(&setup).await;
    let warned: Vec<&str> = checks.iter().filter(|c| c.status == Status::Warn).map(|c| c.detail.as_str()).collect();
    assert_eq!(warned.len(), 2, "{checks:#?}");
    assert!(warned[0].starts_with("`stand_in` of the [[ant]] dev-a in ") && warned[1].contains("names carol"));
}
