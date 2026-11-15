use std::collections::BTreeMap;

use ants_proto::Role;

use super::*;
use crate::auth::tests::Server;

fn p(name: &str) -> UserId {
    name.parse().unwrap()
}

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

/// The files under `dir`, relative to it, with their text.
fn tree(dir: &Path) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();
    let mut dirs = vec![dir.to_owned()];
    while let Some(d) = dirs.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                let rel = path.strip_prefix(dir).unwrap().to_str().unwrap().to_owned();
                files.insert(rel, std::fs::read_to_string(&path).unwrap());
            }
        }
    }
    files
}

#[test]
fn an_export_packs_the_home_s_memories_and_leaves_out_its_work() {
    let dir = ants_test_support::scratch_dir("export-pack").canonicalize().unwrap();
    let home = dir.join("home");
    for (path, text) in [
        ("notes.md", "core"),
        ("memory/deep/fact.md", "deep"),
        ("CLAUDE.md", "claude"),
        (".claude/CLAUDE.md", "claude too"),
        ("worktrees/t1/src.rs", "work"),
        ("answered", "ids"),
        (".git/HEAD", "ref"),
        ("tasks.db", "db"),
        ("tasks.db-wal", "wal"),
        ("tasks.dbx", "not the db"),
    ] {
        write(&home.join(path), text);
    }
    let mut ant = Ant::new(p("dev-a"), dir.clone());
    ant.home = Some(home.clone());
    ant.env.insert("BD_DB".to_owned(), home.join("tasks.db").to_str().unwrap().to_owned());
    let claude = ants_daemon::harness::Harnesses::builtin().get("claude").unwrap();
    let harness = harness_files_of(Some(&claude), &home);
    assert_eq!(harness, [PathBuf::from(".claude/CLAUDE.md"), PathBuf::from("CLAUDE.md")]);
    let core = core_files_of(&ant, &home, &harness).unwrap();
    let core: Vec<&str> = core.iter().map(|p| p.to_str().unwrap()).collect();
    assert_eq!(core, ["memory/deep/fact.md", "notes.md", "tasks.dbx"]);
    assert!(harness_files_of(None, &home).is_empty());
    assert!(core_files_of(&ant, &dir.join("none"), &[]).unwrap().is_empty(), "no home, no files");

    let identity = AntIdentity::new(p("dev-a"), "ab".repeat(16), Utc::now());
    let export = pack_ant(&ant, Some(&claude), None, identity, None).unwrap();
    let restored = dir.join("restored");
    assert_eq!(restore(&restored, &export.files, false).unwrap(), [".claude/CLAUDE.md", "CLAUDE.md"]);
    let files = tree(&restored);
    assert_eq!(files.keys().collect::<Vec<_>>(), ["memory/deep/fact.md", "notes.md", "tasks.dbx"]);
    assert_eq!(files["memory/deep/fact.md"], "deep");

    let outside = Packed { path: "../escape".to_owned(), kind: Kind::Core, data: BASE64.encode(b"x") };
    let err = restore(&dir.join("other"), &[outside], true).unwrap_err().to_string();
    assert!(err.contains("outside the home"), "{err}");
    let absolute = Packed { path: "/etc/x".to_owned(), kind: Kind::Core, data: BASE64.encode(b"x") };
    assert!(restore(&dir.join("other"), &[absolute], true).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[derive(clap::Parser)]
struct Cmd {
    #[command(flatten)]
    args: ImportArgs,
}

async fn import_as(nats: &crate::Nats<'_>, args: &[&str]) -> Result<()> {
    let args = <Cmd as clap::Parser>::try_parse_from(std::iter::once("import").chain(args.iter().copied()))?.args;
    Box::pin(import_with(nats, args, &keystore::Memory::default())).await
}

/// An ant exported with `--move` is marked moved on its bus, and imports on another with its id, birth, avatar,
/// prompts, settings and memories, under its name or another, refused while a record holds its name or its id, into
/// an empty home only, its harness memory left out when it runs another harness there. Unseal clears the mark.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn an_ant_moves_to_another_bus_with_its_identity_and_memories() {
    let Some(server) = Server::start() else { return };
    let dir = ants_test_support::scratch_dir("export-move").canonicalize().unwrap();
    let alice = p("alice");
    let creds = server.creds_as(&alice, Role::Human);
    let nats = crate::Nats::new(&server.url, Some(&creds), None).unwrap();
    let (_, js) = nats.connect("test").await.unwrap();
    js.create_key_value(ants_daemon::hub::identities_config()).await.unwrap();
    let kv = DirectKv::whole(&js, None, IDENTITIES_BUCKET).await.unwrap();

    let home = dir.join("home-a");
    write(&home.join("notes.md"), "remember");
    write(&home.join("CLAUDE.md"), "be brief");
    write(&home.join("worktrees/t1/x"), "work");
    let config = dir.join("config.toml");
    let workdir = dir.to_str().unwrap();
    let listed = format!(
        "harness = \"claude\"\n\n[[ant]]\nname = \"dev-a\"\nworkdir = {workdir:?}\nhome = {:?}\nwhoami = \"Reviewer\"\n\
         model = \"opus\"\nfallback = [\"codex\"]\ncapabilities = [\"review\"]\n",
        home.to_str().unwrap()
    );
    std::fs::write(&config, listed).unwrap();
    let born = Utc::now() - chrono::Duration::days(30);
    let identity = AntIdentity {
        avatar: Some("seed".to_owned()),
        prompts: 42,
        ..AntIdentity::new(p("dev-a"), "cd".repeat(16), born)
    };
    assert!(kv.create("dev-a", serde_json::to_vec(&identity).unwrap()).await.unwrap());
    let (m1, host_creds) = ("m1".parse().unwrap(), server.dir().join("host-m1.creds"));
    crate::auth::issue_host(&server.dir().join("auth"), &m1, None, &host_creds).unwrap();
    let host = async_nats::ConnectOptions::with_credentials_file(&host_creds)
        .await
        .unwrap()
        .custom_inbox_prefix(ants_proto::host_inbox_prefix(&m1))
        .connect(&server.url)
        .await
        .unwrap();
    let host_kv = DirectKv::new(&async_nats::jetstream::new(host), None, IDENTITIES_BUCKET).await.unwrap();
    let serde_json::Value::Object(fields) =
        serde_json::json!({ "whoami": "Lead", "model": "opus", "fallback": ["codex"], "capabilities": ["review"] })
    else {
        unreachable!()
    };
    let shared = SharedSettings { ant: p("dev-a"), fields, machine: m1, by: None, updated_at: Utc::now() };
    assert!(host_kv.create(&shared.key(), serde_json::to_vec(&shared).unwrap()).await.unwrap());

    let file = dir.join("dev-a.ant");
    export(&nats, &p("dev-a"), Some(file.clone()), true, Some(config.clone())).await.unwrap();
    let err = export(&nats, &p("dev-a"), Some(file.clone()), false, Some(config.clone())).await.unwrap_err();
    assert!(err.to_string().contains("not overwriting"), "{err}");
    let (record, _) = read(&kv, &p("dev-a")).await.unwrap();
    let mark = record.unwrap().moved.expect("--move marks the ant moved");
    assert_eq!(mark.by, alice);
    let exported: Export = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    assert_eq!((exported.ants_ant, &exported.identity), (VERSION, &identity), "unmarked");
    let settings = Settings {
        whoami: Some("Lead".to_owned()),
        harness: Some("claude".to_owned()),
        model: Some("opus".to_owned()),
        fallback: vec!["codex".to_owned()],
        ..Settings::default()
    };
    assert_eq!(exported.settings, settings, "only what makes the ant, as shared: no capabilities");
    let paths: Vec<_> = exported.files.iter().map(|f| (f.path.as_str(), f.kind)).collect();
    assert_eq!(paths, [("notes.md", Kind::Core), ("CLAUDE.md", Kind::Harness)]);

    let config_b = dir.join("config-b.toml");
    std::fs::write(&config_b, "harness = \"claude\"\n").unwrap();
    let (file_arg, config_arg) = (file.to_str().unwrap(), config_b.to_str().unwrap());
    let home_b = dir.join("home-b");
    let out_b = dir.join("keys/dev-b.creds");
    let (home_arg, out_arg) = (home_b.to_str().unwrap(), out_b.to_str().unwrap());
    let base = ["--config", config_arg, "--workdir", workdir, "--out", out_arg, "--home", home_arg];
    let import = |extra: &'static [&'static str]| {
        let args: Vec<&str> = [file_arg].into_iter().chain(base).chain(extra.iter().copied()).collect();
        let nats = &nats;
        async move { Box::pin(import_as(nats, &args)).await }
    };

    let err = import(&[]).await.unwrap_err().to_string();
    assert!(err.contains("holds an ant named dev-a"), "{err}");
    let err = import(&["--as", "dev-b"]).await.unwrap_err().to_string();
    assert!(err.contains("holds this ant already, as dev-a"), "{err}");
    write(&home_b.join("old.md"), "x");
    kv.delete("dev-a").await.unwrap();
    let err = import(&["--as", "dev-b"]).await.unwrap_err().to_string();
    assert!(err.contains("is not empty"), "{err}");
    assert_eq!(read(&kv, &p("dev-b")).await.unwrap().0, None, "nothing half-done");
    std::fs::remove_file(home_b.join("old.md")).unwrap();

    import(&["--as", "dev-b"]).await.unwrap();
    let (record, _) = read(&kv, &p("dev-b")).await.unwrap();
    assert_eq!(record, Some(AntIdentity { ant: p("dev-b"), ..identity.clone() }), "the same id, birth and growth");
    assert_eq!(
        tree(&home_b),
        BTreeMap::from([
            ("notes.md".to_owned(), "remember".to_owned()),
            ("CLAUDE.md".to_owned(), "be brief".to_owned())
        ])
    );
    let loaded = config::load(&config_b).unwrap();
    let dev_b = loaded.ants.iter().find(|a| a.name == p("dev-b")).unwrap();
    assert_eq!(settings_of(dev_b, None).unwrap(), settings, "the shared settings, which seed the new bus's");
    assert!(dev_b.capabilities.is_empty());
    let err = import(&["--as", "dev-b"]).await.unwrap_err().to_string();
    assert!(err.contains("lists dev-b already"), "{err}");

    // On another harness, the harness memory stays behind.
    kv.delete("dev-b").await.unwrap();
    let config_c = dir.join("config-c.toml");
    std::fs::write(&config_c, "harness = \"copilot\"\n").unwrap();
    let (home_c, out_c) = (dir.join("home-c"), dir.join("keys/dev-a.creds"));
    let args = [
        file_arg,
        "--config",
        config_c.to_str().unwrap(),
        "--workdir",
        workdir,
        "--out",
        out_c.to_str().unwrap(),
        "--home",
        home_c.to_str().unwrap(),
        "--harness",
        "codex",
        "--model",
        "gpt-5",
    ];
    Box::pin(import_as(&nats, &args)).await.unwrap();
    assert_eq!(tree(&home_c), BTreeMap::from([("notes.md".to_owned(), "remember".to_owned())]));
    let loaded = config::load(&config_c).unwrap();
    let dev_a = &loaded.ants.iter().find(|a| a.name == p("dev-a")).unwrap();
    assert_eq!((dev_a.harness.as_deref(), dev_a.model.as_deref()), (Some("codex"), Some("gpt-5")), "the flags win");
    assert_eq!(dev_a.whoami.as_deref(), Some("Lead"));

    // Unseal clears the mark of a moved ant, and only that.
    let moved = AntIdentity { moved: Some(mark), ..identity.clone() };
    let (_, last) = read(&kv, &p("dev-a")).await.unwrap();
    assert!(kv.update("dev-a", serde_json::to_vec(&moved).unwrap(), last).await.unwrap());
    unseal(&nats, &p("dev-a")).await.unwrap();
    assert_eq!(read(&kv, &p("dev-a")).await.unwrap().0.unwrap().moved, None);
    assert!(!mark_moved(&kv, &p("dev-a"), None).await.unwrap(), "not moved: unchanged");
    std::fs::remove_dir_all(&dir).unwrap();
}
