//! Editing the file in place: entries read back as written, appended, changed and removed, the rest staying as it
//! was.

use super::*;

#[test]
fn entries_read_back_as_written() {
    let workdir = manifest_dir().canonicalize().unwrap();
    let plain = Ant::new("dev-a".parse().unwrap(), workdir.clone());
    let text = entry(&plain).unwrap();
    assert_eq!(text, format!("[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\n", workdir.to_str().unwrap()));
    let read = parse_here(&format!("messenger = false\n{text}")).unwrap();
    assert_eq!(read[0], Ant { harness: Some("copilot".to_owned()), ..plain }, "the file's harness");

    let mut full = Ant::new("dev-b".parse().unwrap(), workdir);
    full.nats = Some("wss://hub.example/nats".to_owned());
    full.creds = Some(PathBuf::from("/keys/dev b.creds"));
    full.js_domain = Some("hub".parse().unwrap());
    full.human = Some("alice".parse().unwrap());
    full.home = Some(PathBuf::from("/srv/ants/dev-b"));
    full.whoami = Some("Say \"hi\".\nThen\ttab \\ back\u{7f}.".to_owned());
    full.env.insert("A B".to_owned(), "x=\"y\"".to_owned());
    full.harness = Some("claude".to_owned());
    full.model = Some("m".to_owned());
    full.fallback = vec!["codex".to_owned(), ":m2".to_owned()];
    full.history_budget = Some(20_000);
    full.allow_tools = true;
    full.allow = vec!["execute:cargo test*".to_owned()];
    full.capabilities = vec!["rust".to_owned(), "nats".to_owned()];
    full.escalate_to = vec!["lead".parse().unwrap()];
    full.stand_in = Some("carol".parse().unwrap());
    full.max_harnesses = 1;
    full.worktrees = Some(false);
    full.prompt_timeout = Duration::ZERO;
    full.idle_timeout = Duration::from_mins(90);
    full.max_thread_cost = Some(2.5);
    full.max_daily_tokens = Some(1000);
    full.spend_warn_percent = 0;
    full.worktree_base = Base::Upstream;
    full.publish = Publish::DraftPr;
    let text = entry(&full).unwrap();
    assert!(text.contains("\nidle_timeout = \"1h30m\"\nprompt_timeout = \"0\"\n"), "{text}");
    let read: Vec<_> = parse_here(&text).unwrap().into_iter().filter(|ant| ant.builtin.is_none()).collect();
    assert_eq!(read, [full]);
}

#[test]
fn appending_keeps_the_file_as_it_was_and_checks_it() {
    let dir = ants_test_support::scratch_dir("config-append").canonicalize().unwrap();
    let path = dir.join("ants/config.toml");
    let ant = |name: &str| Ant { harness: Some("copilot".to_owned()), ..Ant::new(name.parse().unwrap(), dir.clone()) };
    let add = |name: &str| append(&path, &entry(&ant(name)).unwrap());
    let err = format!("{:#}", add("dev-a").unwrap_err());
    assert!(err.contains("ant system-messenger: no harness set"), "a new file needs a top-level harness: {err}");
    assert!(!path.exists(), "nothing written");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    let old = "# the team\nharness = \"copilot\"\nnats = \"nats://hub:4222\" # the hub\n\n[[ant]]\nname = \"dev-a\"\n\
                   workdir = \".\"\ncreds = \"keys/dev-a.creds\"";
    std::fs::write(&path, old).unwrap();
    add("dev-b").unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.starts_with(&format!("{old}\n\n[[ant]]\nname = \"dev-b\"")), "{text}");
    let loaded = load(&path).unwrap().ants;
    let names: Vec<_> = loaded.iter().filter(|ant| ant.builtin.is_none()).map(|ant| ant.name.as_str()).collect();
    assert_eq!(
        names,
        ["dev-a", "dev-b"],
        "besides the built-in ants, the sage among them only where the home holds its credentials"
    );

    let err = format!("{:#}", add("dev-a").unwrap_err());
    assert!(err.contains("ant dev-a is listed twice"), "{err}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "left as it was");

    let summary = Summary::load(&path).unwrap();
    assert_eq!(summary.nats.as_deref(), Some("nats://hub:4222"));
    assert_eq!(summary.creds_of(&"dev-a".parse().unwrap()), Some(Some(dir.join("ants/keys/dev-a.creds").as_path())));
    assert_eq!(summary.creds_of(&"dev-b".parse().unwrap()), Some(None));
    assert_eq!(summary.creds_of(&"dev-c".parse().unwrap()), None);
    let none = Summary::load(&dir.join("none.toml")).unwrap();
    assert!(none.host.looked_in.is_some(), "a missing file looks for the host credentials where they belong");
    assert_eq!(Summary { host: HostSettings { looked_in: None, ..none.host.clone() }, ..none }, Summary::default());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_allow_rule_goes_into_the_ants_list_and_the_rest_stays() {
    let dir = ants_test_support::scratch_dir("config-allow").canonicalize().unwrap();
    let path = dir.join("config.toml");
    let old = "# the team\nharness = \"copilot\"\nnats = \"nats://hub:4222\"\n\n[[ant]]\nname = \"dev-a\" # first\n\
                   workdir = \".\"\n\n\
                   [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nallow = [\n  \"read\", # reading is fine\n]\n\n\
                   [[ant]]\nname = \"dev-c\"\nworkdir = \".\"\nallow = []\n";
    std::fs::write(&path, old).unwrap();
    let add = |ant: &str, rule: &str| {
        let text = with_allow_rule(&path, &ant.parse().unwrap(), rule)?;
        if let Some(text) = &text {
            write(&path, text)?;
        }
        anyhow::Ok(text.is_some())
    };
    assert!(add("dev-a", "execute:cargo test").unwrap());
    assert!(add("dev-b", "edit:/a \"b\"").unwrap());
    assert!(add("dev-c", "read").unwrap());
    assert!(!add("dev-b", "read").unwrap(), "listed already");
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(
        text,
        old.replace("# first\n", "# first\nallow = [\"execute:cargo test\"]\n")
            .replace("\"read\", #", "\"read\", \"edit:/a \\\"b\\\"\", #")
            .replace("[]", "[\"read\"]")
    );
    let allow: Vec<_> = load(&path).unwrap().ants.into_iter().map(|a| a.allow).collect();
    assert_eq!(allow[..3], [vec!["execute:cargo test"], vec!["read", "edit:/a \"b\""], vec!["read"]]);

    let err = format!("{:#}", add("dev-d", "read").unwrap_err());
    assert!(err.contains("lists no ant dev-d"), "{err}");
    std::fs::write(&path, "ant = [{ name = \"dev-a\", workdir = \".\" }]\n").unwrap();
    assert!(add("dev-a", "read").is_err(), "an inline table is left to edit by hand");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_ants_table_leaves_the_file_and_the_rest_stays() {
    let dir = ants_test_support::scratch_dir("config-without").canonicalize().unwrap();
    let path = dir.join("config.toml");
    let top = "# the team\nharness = \"copilot\"\nnats = \"nats://hub:4222\"\n\n";
    let a = format!("{top}[[ant]]\nname = \"dev-a\" # first\nworkdir = \".\"\n\n");
    let b = "[[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nallow = [\n  \"read\",\n]\n\n";
    let c = "# last\n[[ ant ]] # spaced\nworkdir = \".\"\nname = \"dev-c\"\n";
    std::fs::write(&path, format!("{a}{b}{c}")).unwrap();
    let without = |ant: &str| without_ant(&path, &ant.parse().unwrap());
    assert_eq!(without("dev-b").unwrap(), format!("{a}{c}"));
    assert_eq!(without("dev-c").unwrap(), format!("{a}{b}# last\n"), "the comment above stays");
    assert_eq!(without("dev-a").unwrap(), format!("{top}{b}{c}"));
    assert!(format!("{:#}", without("dev-d").unwrap_err()).contains("lists no ant dev-d"));
    std::fs::write(&path, "ant = [{ name = \"dev-a\", workdir = \".\" }]\n").unwrap();
    assert!(format!("{:#}", without("dev-a").unwrap_err()).contains("remove it by hand"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn an_entry_writes_the_turn_budget_only_when_set() {
    let workdir = manifest_dir().canonicalize().unwrap();
    let mut ant = Ant::new("dev-a".parse().unwrap(), workdir);
    assert!(!entry(&ant).unwrap().contains("turn_budget"));
    ant.turn_budget = Some(12);
    let text = entry(&ant).unwrap();
    assert!(text.contains("\nturn_budget = 12\n"), "{text}");
    let read = parse_here(&format!("messenger = false\n{text}")).unwrap();
    assert_eq!(read[0].turn_budget, Some(12));
}
