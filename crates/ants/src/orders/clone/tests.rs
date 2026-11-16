#![allow(clippy::too_many_lines)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use ants_daemon::admin::ConfigFile;
use ants_daemon::admin::restart::Restarts;
use ants_daemon::config::edit::Edit;
use ants_daemon::direct_kv::DirectKv;
use ants_daemon::identity::{self, Identity};
use ants_daemon::pool::{SessionStore, Stored};
use ants_proto::{
    AdminOp, Envelope, IDENTITIES_BUCKET, MovedMark, OrderState, Permissions, Role, SharedSettings, ThreadId, UserId,
};
use serde_json::{Value, json};

use crate::auth::tests::Server;
use crate::auth::{NewDevice, issue_device_with};
use crate::orders::add::tests::{SIGNED, with_signer};
use crate::orders::send;
use crate::orders::tests::{WAIT, host_of_m1_serving, p};
use crate::revoke::Kind;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = std::process::Command::new("git").args(args).current_dir(dir).output().unwrap();
    assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

fn clone_as(new: &str, settings: Value) -> AdminOp {
    let Value::Object(settings) = settings else { unreachable!() };
    AdminOp::Clone { new: p(new), settings }
}

/// A clone order pauses the ant, copies its home, writes each thread's history into the clone's home, adds each
/// worktree again on the clone's branch with its uncommitted changes, has `ants signer` sign the clone's key, stores
/// the sessions marked to start over (the ant's daemon runs no harness that forks), writes the clone's identity with
/// where it came from and the prompts, appends its table with the settings over the ant's, and resumes the ant. A
/// failure undoes it all; a clone into a listed name or one with an identity record, of a moved ant or of one served
/// elsewhere, is refused, the ant resumed.
#[tokio::test]
async fn a_clone_order_copies_an_ant_under_a_new_name_and_undoes_a_failed_one() {
    let Some(server) = Server::start() else { return };
    let (alice, bob, dev_a, dev_c) = (p("alice"), p("bob"), p("dev-a"), p("dev-c"));
    let team = server.dir().join("auth");
    let (_, h_js) = server.connect_as(&bob, Role::Human).await;
    ants_daemon::bus::ensure_stream(&h_js).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::machines_config()).await.unwrap();
    h_js.create_key_value(ants_daemon::hub::devices_config()).await.unwrap();
    h_js.create_stream(ants_daemon::hub::identities_config()).await.unwrap();
    let admin_creds = server.dir().join("alice-admin.creds");
    let admin = NewDevice { label: Some("laptop"), admin: true, ..NewDevice::default() };
    let rights = Permissions::for_user(&alice, Role::Human);
    issue_device_with(&team, &alice, Kind::Human, rights, &admin, &admin_creds).unwrap();
    let admin = crate::Nats::new(&server.url, Some(&admin_creds), None).unwrap();
    let (_, js) = admin.connect("test").await.unwrap();

    let repo = server.dir().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "-b", "main"]);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    git(&repo, &["add", "a.txt"]);
    git(&repo, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "one"]);
    let home_a = server.dir().join("home-a");
    std::fs::create_dir_all(home_a.join("memory")).unwrap();
    std::fs::create_dir_all(home_a.join("answered")).unwrap();
    std::fs::write(home_a.join("memory").join("core.md"), "the build runs with cargo\n").unwrap();
    std::fs::write(home_a.join("answered").join("x"), "").unwrap();

    let (_, a_js) = server.connect_as(&dev_a, Role::Ant).await;
    let asked = Envelope::new(bob.clone(), dev_a.clone(), "port the parser");
    h_js.publish(asked.subject(), serde_json::to_vec(&asked).unwrap().into()).await.unwrap().await.unwrap();
    let (talked, worked): (ThreadId, ThreadId) = (asked.thread_id.clone(), "t-work".parse().unwrap());
    let tree = home_a.join("worktrees").join(worked.as_str());
    git(&repo, &["worktree", "add", "--quiet", "-b", "ants/dev-a/t-work", tree.to_str().unwrap()]);
    std::fs::write(tree.join("b.txt"), "two\n").unwrap();
    git(&tree, &["add", "b.txt"]);
    git(&tree, &["-c", "user.name=t", "-c", "user.email=t@t", "commit", "--quiet", "-m", "two"]);
    std::fs::write(tree.join("a.txt"), "one, changed\n").unwrap();
    std::fs::write(tree.join("new.txt"), "untracked\n").unwrap();
    let head = git(&tree, &["rev-parse", "HEAD"]);
    let store = SessionStore::kv(ants_daemon::bus::ensure_sessions(&a_js, None, &dev_a).await.unwrap(), dev_a.clone());
    for (thread, session) in [(&talked, "s-talk"), (&worked, "s-work")] {
        let stored = Stored { session: session.to_owned().into(), harness: "copilot".to_owned(), cloned: false };
        store.put(thread, stored).await.unwrap();
    }
    let identity = Identity::new(&a_js, None, &dev_a).await.unwrap();
    let mut born = identity::born(&dev_a).unwrap();
    born.prompts = 7;
    identity.replace(&born).await.unwrap();

    let path = server.dir().join("config.toml");
    let text = format!(
        "harness = \"copilot\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\nhome = {:?}\nmodel = \"o3\"\n\
         escalate_to = [\"lead\"]\n\n[[ant]]\nname = \"dev-b\"\nworkdir = {:?}\n",
        repo.display().to_string(),
        home_a.display().to_string(),
        repo.display().to_string(),
    );
    std::fs::write(&path, &text).unwrap();
    let config = ConfigFile::new(path.clone(), Arc::new(tokio::sync::Notify::new()));
    let restarts = Restarts::default();
    let (ants, task) = host_of_m1_serving(&server, &js, true, Some(config), restarts.clone()).await;
    let m1: ants_proto::Machine = "m1".parse().unwrap();
    // dev-a's shared settings, set by an order since its table was written.
    let host = async_nats::ConnectOptions::with_credentials_file(server.dir().join("host-m1.creds"))
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(&m1))
        .connect(&server.url)
        .await
        .unwrap();
    let host_kv = DirectKv::new(&async_nats::jetstream::new(host), None, IDENTITIES_BUCKET).await.unwrap();
    let Value::Object(fields) = json!({ "whoami": "Reviews parsers", "model": "o4", "escalate_to": ["lead"] }) else {
        unreachable!()
    };
    let mut shared = SharedSettings::new(dev_a.clone(), fields, m1.clone(), Some(alice.clone()));
    let a_creds = server.dir().join("dev-a-Ant.creds");
    let signer = ants_daemon::shared::Signer::load(&dev_a, Some(&a_creds), &[]).unwrap();
    assert!(ants_daemon::shared::write(&host_kv, &mut shared, &signer, None).await.unwrap());

    let _served = restarts.serve(&dev_a);
    let mut listening = restarts.listen_clones(&dev_a);
    let paused_then = Arc::new(Mutex::new(Vec::new()));
    let snapshots = async {
        loop {
            let request = listening.next().await;
            let paused = Edit::open(&path).unwrap().get(&dev_a, &["paused".to_owned()]).unwrap();
            paused_then.lock().unwrap().push(paused);
            let source = ants_daemon::clone::Source {
                js: &a_js,
                ant: &dev_a,
                home: &home_a,
                store: &store,
                identity: Some(&identity),
                launch: None,
                reading: ants_daemon::reading::Reading::Consumer,
            };
            _ = request.done.send(source.snapshot(&request.new, async |_| None).await);
        }
    };

    let checks = async {
        let home_c = server.dir().join("home-c");
        let clone = clone_as("dev-c", json!({ "home": home_c, "model": "gpt-5" }));
        let (order, cloned) =
            with_signer(&server, Box::pin(send(&admin, &js, Some(m1.clone()), &dev_a, clone, SIGNED))).await.unwrap();
        let cloned = cloned.expect("the daemon decides once the signer answers");
        assert_eq!((cloned.state, cloned.reason.as_deref()), (OrderState::Applied, None));
        let result = cloned.result.unwrap();
        assert_eq!(result["threads"], json!(2));
        assert_eq!(result["forked"], json!([]));
        assert_eq!(result["worktrees"], json!(["ants/dev-c/t-work"]));
        assert_eq!(*paused_then.lock().unwrap(), [Some(json!(true))], "the snapshot is read while dev-a is paused");

        let core = std::fs::read_to_string(home_c.join("memory").join("core.md")).unwrap();
        assert_eq!(core, "the build runs with cargo\n");
        assert!(!home_c.join("answered").exists(), "answered messages stay dev-a's");
        let history = std::fs::read_to_string(home_c.join("history").join(format!("{talked}.jsonl"))).unwrap();
        assert!(history.contains("port the parser") && history.contains("\"cloned_from\""), "{history}");
        let copy = home_c.join("worktrees").join(worked.as_str());
        assert_eq!(git(&copy, &["branch", "--show-current"]), "ants/dev-c/t-work");
        assert_eq!(git(&copy, &["rev-parse", "HEAD"]), head, "from dev-a's tip");
        assert_eq!(std::fs::read_to_string(copy.join("a.txt")).unwrap(), "one, changed\n");
        assert_eq!(std::fs::read_to_string(copy.join("new.txt")).unwrap(), "untracked\n");
        assert_eq!(std::fs::read_to_string(tree.join("a.txt")).unwrap(), "one, changed\n", "dev-a's stays");

        let creds = server.dir().join("dev-c.creds");
        let (_, c_js) = ants_daemon::bus::connect(&server.url, Some(&creds), None, &dev_c, "test").await.unwrap();
        let c_store =
            SessionStore::kv(ants_daemon::bus::ensure_sessions(&c_js, None, &dev_c).await.unwrap(), dev_c.clone());
        let stored = c_store.get(&worked).await.unwrap().expect("the session is stored for dev-c");
        assert_eq!((stored.session.to_string().as_str(), stored.cloned), ("s-work", true));
        let record = Identity::new(&c_js, None, &dev_c).await.unwrap().of(&dev_c).await.unwrap().unwrap();
        let from = record.cloned_from.as_ref().expect("where dev-c came from");
        assert_eq!((from.ant.as_str(), from.id.as_str(), record.prompts), ("dev-a", born.id.as_str(), 7));
        assert_ne!(record.id, born.id, "a new id");

        let edited = std::fs::read_to_string(&path).unwrap();
        assert!(edited.starts_with(&text), "dev-a resumed, the clone's table appended: {edited}");
        let mark = format!("# cloned from dev-a by alice's order {}\n[[ant]]\nname = \"dev-c\"", order.id);
        assert!(edited.contains(&mark), "{edited}");
        let loaded = ants_daemon::config::load(&path).unwrap();
        let ant = loaded.ants.iter().find(|a| a.name == dev_c).expect("listed");
        assert_eq!((ant.model.as_deref(), ant.home.as_deref()), (Some("gpt-5"), Some(home_c.as_path())));
        assert_eq!((ant.escalate_to.as_slice(), ant.paused), (&[p("lead")][..], false));
        assert_eq!(ant.whoami.as_deref(), Some("Reviews parsers"), "dev-a's shared settings, not its table's");
        let shared = DirectKv::whole(&js, None, IDENTITIES_BUCKET).await.unwrap();
        let record = shared.get(SharedSettings::key_of(&dev_c)).await.unwrap().expect("dev-c's own shared settings");
        let record: SharedSettings = serde_json::from_slice(&record).unwrap();
        let want = json!({ "whoami": "Reviews parsers", "model": "gpt-5", "escalate_to": ["lead"] });
        assert_eq!(Value::Object(record.fields), want, "dev-a's with the order's over them");

        // The signer refuses: the team holds credentials of dev-d already.
        server.creds_as(&p("dev-d"), Role::Ant);
        let home_d = server.dir().join("home-d");
        let clone = clone_as("dev-d", json!({ "home": home_d }));
        let (_, failed) =
            with_signer(&server, Box::pin(send(&admin, &js, Some(m1.clone()), &dev_a, clone, SIGNED))).await.unwrap();
        let failed = failed.expect("refused once the signer answers");
        assert_eq!(failed.state, OrderState::Refused);
        let reason = failed.reason.unwrap();
        assert!(reason.contains("holds credentials named dev-d") && reason.contains("all undone"), "{reason}");
        assert!(!home_d.exists(), "the home goes");
        assert!(git(&repo, &["branch", "--list", "ants/dev-d/*"]).is_empty(), "the branch goes");
        assert!(!server.dir().join("dev-d.nk").exists() && !server.dir().join("dev-d.creds").exists());
        assert!(shared.get(SharedSettings::key_of(&p("dev-d"))).await.unwrap().is_none());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), edited, "dev-a resumed, nothing added");

        let refusal = async |new: &str, ant: &UserId| {
            let clone = clone_as(new, json!({ "home": server.dir().join(format!("home-{new}")) }));
            let (_, outcome) = send(&admin, &js, Some(m1.clone()), ant, clone, WAIT).await.unwrap();
            let outcome = outcome.expect("refused");
            assert_eq!(outcome.state, OrderState::Refused);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), edited, "dev-a resumed");
            outcome.reason.unwrap()
        };
        let reason = refusal("dev-c", &dev_a).await;
        assert!(reason.contains("lists dev-c already"), "{reason}");
        let reason = refusal("dev-e", &p("dev-b")).await;
        assert!(reason.contains("m1 serves no inbox of dev-b"), "{reason}");
        let (_, e_js) = server.connect_as(&p("dev-e"), Role::Ant).await;
        Identity::new(&e_js, None, &p("dev-e")).await.unwrap().ensure().await.unwrap();
        let reason = refusal("dev-e", &dev_a).await;
        assert!(reason.contains("dev-e has an identity record already"), "{reason}");
        born.moved = Some(MovedMark { at: chrono::Utc::now(), by: alice.clone() });
        identity.replace(&born).await.unwrap();
        let reason = refusal("dev-f", &dev_a).await;
        assert!(reason.contains("dev-a was moved"), "{reason}");
        assert!(!server.dir().join("home-dev-f").exists());
    };
    tokio::select! {
        () = snapshots => unreachable!(),
        () = checks => {}
    }
    drop(ants);
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap().unwrap();
}
