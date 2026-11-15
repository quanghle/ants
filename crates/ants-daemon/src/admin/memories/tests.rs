use std::sync::Arc;

use ants_proto::{AdminOp, OrderState};
use tokio::sync::Notify;

use super::*;
use crate::admin::ConfigFile;
use crate::admin::restart::Restarts;
use crate::harness::{Harnesses, parse_definitions};

fn write(path: &Path, text: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
}

#[cfg(unix)]
fn link(target: &Path, at: &Path) {
    std::os::unix::fs::symlink(target, at).unwrap();
}

/// A home, an ant's workdir in it and a directory outside both.
fn dirs(name: &str) -> (PathBuf, PathBuf, PathBuf) {
    let root = ants_test_support::scratch_dir(name).canonicalize().unwrap();
    let (home, outside) = (root.join("home"), root.join("outside"));
    let workdir = home.join("src/repo");
    std::fs::create_dir_all(&workdir).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    (home, workdir, outside)
}

fn harness(globs: &[&str]) -> Arc<Harness> {
    let globs: Vec<String> = globs.iter().map(|g| format!("{g:?}")).collect();
    let text = format!("[harnesses.h]\ncommand = \"h\"\nmemory_files = [{}]\n", globs.join(", "));
    Harnesses::builtin().define(parse_definitions(&text).unwrap()).unwrap().get("h").unwrap()
}

fn env(home: &Path) -> Env {
    Env { home: home.to_owned(), bd: "bd".into(), bd_db: None }
}

fn paths(memories: &Memories) -> Vec<String> {
    memories.files.iter().map(|f| f.path.clone()).collect()
}

#[test]
fn globs_match_within_components_and_double_stars_span_directories() {
    assert!(name_matches("*.md", "AGENTS.md") && name_matches("A?ENTS.md", "AGENTS.md") && name_matches("*", ""));
    assert!(!name_matches("*.md", "AGENTS.txt") && !name_matches("?", ""));
    let glob = |g: &str| g.split('/').map(str::to_owned).collect::<Vec<_>>();
    assert!(components_match(&glob("**/*.md"), &["a.md"]) && components_match(&glob("**/*.md"), &["x", "y", "a.md"]));
    assert!(!components_match(&glob("*/a.md"), &["x", "y", "a.md"]) && !components_match(&glob("a.md"), &[]));
}

/// The files of the globs are read, under `~` and the workdir; a link to another memory file is read once, and a
/// link out of the globs, or a glob out of the workdir and homes, is named, not followed; `**` walks no linked
/// directory.
#[cfg(unix)]
#[tokio::test]
async fn reads_only_the_files_of_the_globs_within_the_workdir_and_homes() {
    let (home, workdir, outside) = dirs("memories-confined");
    write(&workdir.join("CLAUDE.md"), "be brief");
    link(&workdir.join("CLAUDE.md"), &workdir.join("AGENTS.md"));
    write(&home.join(".claude/CLAUDE.md"), "global");
    write(&home.join(".ssh/id_ed25519"), "secret");
    link(&home.join(".ssh/id_ed25519"), &workdir.join("GEMINI.md"));
    write(&workdir.join("docs/rules/a.md"), "rule a");
    write(&outside.join("rules/b.md"), "rule b");
    link(&outside.join("rules"), &workdir.join("docs/linked"));
    write(&outside.join("x.md"), "outside");

    let mut ant = Ant::new("dev-a".parse().unwrap(), workdir.clone());
    ant.whoami = Some("reviewer".to_owned());
    ant.memory = Some("notes in NOTES.md".to_owned());
    let globs = ["AGENTS.md", "CLAUDE.md", "GEMINI.md", "docs/**/*.md", "~/.claude/CLAUDE.md"];
    let outside_glob = outside.join("*.md").display().to_string();
    let harness = harness(&[&globs[..], &[outside_glob.as_str()]].concat());
    let memories = read(&ant, Some(&harness), &env(&home)).await;

    assert_eq!(memories.core.whoami.as_deref(), Some("reviewer"));
    assert_eq!((memories.core.memory.as_deref(), memories.core.tasks.as_deref()), (Some("notes in NOTES.md"), None));
    let read: Vec<String> =
        [workdir.join("CLAUDE.md"), workdir.join("docs/rules/a.md"), home.join(".claude/CLAUDE.md")]
            .iter()
            .map(|p| p.display().to_string())
            .collect();
    assert_eq!(paths(&memories), read, "{memories:#?}");
    assert_eq!(memories.files[0].text, "be brief");
    let skipped: Vec<(&str, &str)> = memories.skipped.iter().map(|s| (s.what.as_str(), s.why.as_str())).collect();
    let gemini = workdir.join("GEMINI.md").display().to_string();
    let x = outside.join("x.md").display().to_string();
    assert_eq!(skipped, [(gemini.as_str(), OUTSIDE), (x.as_str(), OUTSIDE)]);
    assert!(memories.bd.is_none(), "no BD_DB, no bd memories");
}

/// Each text is cut at the file cap, and the memories altogether at the total cap, what is left out being named.
#[tokio::test]
async fn memories_are_cut_at_each_file_s_cap_and_at_the_total_cap() {
    let (home, workdir, _) = dirs("memories-capped");
    let big = "x".repeat(MEMORY_FILE_CAP + 10);
    for n in 0..10 {
        write(&workdir.join(format!("m{n}.md")), &big);
    }
    let ant = Ant::new("dev-a".parse().unwrap(), workdir.clone());
    let memories = read(&ant, Some(&harness(&["*.md"])), &env(&home)).await;
    let first = &memories.files[0];
    assert_eq!((first.text.len(), first.size, first.truncated), (MEMORY_FILE_CAP, big.len() as u64, true));
    let shown: usize = memories.files.iter().map(|f| f.text.len()).sum();
    assert!(shown <= MEMORIES_CAP, "{shown}");
    assert!(memories.files.len() < 10, "the total cap leaves some out");
    let last = memories.skipped.last().expect("some left out");
    let m9 = workdir.join("m9.md").display().to_string();
    assert_eq!((last.what.as_str(), last.why.as_str()), (m9.as_str(), OVER_CAP));
    assert!(serde_json::to_vec(&memories).unwrap().len() < MEMORIES_CAP + 8 * 1024, "the result fits a KV value");

    let mut budget = Budget(10);
    assert_eq!(budget.take("a\"b", false), Some(("a\"b".to_owned(), false)), "6 bytes as JSON");
    assert_eq!(budget.take("\n\n\n", false), Some(("\n".to_owned(), true)), "4 bytes left");
    assert_eq!(budget.take("more", false), None);
}

/// The built-in harnesses name their memory files; a config table replacing one keeps them unless it sets its own.
#[test]
fn built_in_harnesses_have_default_memory_files_a_replacement_keeps() {
    let builtin = Harnesses::builtin();
    let claude = builtin.get("claude").unwrap();
    assert!(claude.memory_files().contains(&"~/.claude/CLAUDE.md".to_owned()), "{:?}", claude.memory_files());
    assert!(builtin.get("codex").unwrap().memory_files().contains(&"~/.codex/AGENTS.md".to_owned()));
    for name in builtin.names() {
        assert!(!builtin.get(name).unwrap().memory_files().is_empty(), "{name} names its memory files");
    }
    let kept = builtin.define(parse_definitions("[harnesses.claude]\ncommand = \"my-claude\"\n").unwrap()).unwrap();
    assert_eq!(kept.get("claude").unwrap().memory_files(), claude.memory_files());
    let own = "[harnesses.claude]\ncommand = \"c\"\nmemory_files = [\"NOTES.md\"]\n";
    let own = builtin.define(parse_definitions(own).unwrap()).unwrap();
    assert_eq!(own.get("claude").unwrap().memory_files(), ["NOTES.md"]);
    let parent = "[harnesses.x]\ncommand = \"x\"\nmemory_files = [\"../secrets/*\"]\n";
    assert!(builtin.define(parse_definitions(parent).unwrap()).is_err(), "no glob climbs out");
}

/// With `BD_DB` in the ant's environment, its bd memories are listed by `bd memories --json`.
#[cfg(unix)]
#[tokio::test]
async fn bd_memories_are_listed_when_the_ant_s_environment_names_a_database() {
    use std::os::unix::fs::PermissionsExt as _;

    let (home, workdir, outside) = dirs("memories-bd");
    let bd = outside.join("bd");
    let script = "#!/bin/sh\n[ \"$1 $2\" = 'memories --json' ] || exit 2\n\
                  printf '[{\"key\":\"k1\",\"content\":\"%s\",\"revision\":1}]' \"$BD_DB\"\n";
    write(&bd, script);
    std::fs::set_permissions(&bd, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut ant = Ant::new("dev-a".parse().unwrap(), workdir.clone());
    ant.env.insert("BD_DB".to_owned(), "/srv/bd.db".to_owned());
    let env = Env { home, bd: bd.into(), bd_db: Some("/daemon/bd.db".to_owned()) };
    let memories = read(&ant, None, &env).await;
    let bd = memories.bd.expect("BD_DB is set");
    assert_eq!(bd.db, "/srv/bd.db", "the ant's own env wins");
    assert_eq!(bd.memories, [BdMemory { key: "k1".to_owned(), content: "/srv/bd.db".to_owned(), truncated: false }]);
    assert_eq!(bd.error, None);

    let env = Env { bd: "no-such-bd-command".into(), ..env };
    let failed = read(&ant, None, &env).await.bd.unwrap();
    assert!(failed.error.unwrap().starts_with("running bd"), "a failure is told, not fatal");
}

/// A read is answered from the config file the daemon serves while online, and refused once it waited too long for
/// the daemon, or names an ant the file does not list.
#[tokio::test]
async fn a_read_is_answered_from_the_config_only_while_fresh() {
    let (_, workdir, _) = dirs("memories-apply");
    let path = workdir.join("config.toml");
    let text =
        "harness = \"claude\"\nmessenger = false\n\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\ntasks = \"triage\"\n";
    write(&path, text);
    let config = ConfigFile::new(path, Arc::new(Notify::new()));
    let orders = Orders {
        machine: "m1".parse().unwrap(),
        nats: String::new(),
        remote: true,
        accounts: vec![],
        config: Some(config),
        restarts: Restarts::default(),
        creds: None,
        js_domain: None,
        leaf: None,
    };
    let order = |ant: &str| {
        AdminOrder::new(
            "alice".parse().unwrap(),
            "m1".parse().unwrap(),
            ant.parse().unwrap(),
            AdminOp::ReadMemories,
            "",
        )
    };

    let fresh = apply(&orders, &order("dev-a"), Utc::now()).await;
    assert_eq!(fresh.state, OrderState::Applied, "{fresh:?}");
    let memories: Memories = serde_json::from_value(fresh.result.unwrap()).unwrap();
    assert_eq!((memories.core.tasks.as_deref(), memories.harness.as_deref()), (Some("triage"), Some("claude")));

    let stale = apply(&orders, &order("dev-a"), Utc::now() - MEMORIES_FRESH - chrono::TimeDelta::seconds(1)).await;
    assert_eq!(stale.state, OrderState::Refused);
    assert!(stale.reason.unwrap().starts_with("stale"));
    let unknown = apply(&orders, &order("dev-z"), Utc::now()).await;
    assert!(unknown.reason.unwrap().contains("lists no ant dev-z"));
}
