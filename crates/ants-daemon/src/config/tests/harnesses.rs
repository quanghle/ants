//! Harnesses and the built-in messenger: the top-level harness, harnesses defined in the file or in drop-in files,
//! fallbacks, and the messenger each of them runs.

use super::*;

#[test]
fn the_top_level_harness_is_the_default_one_and_runs_the_builtin_messenger() {
    let ants = parse_here(
        "harness = \"claude\"\njs_domain = \"hub\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nharness = \"antigravity\"",
    )
    .unwrap();
    use ants_proto::BuiltinAnt::{Messenger, Sage};

    let harnesses: Vec<_> = ants.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref(), ant.builtin)).collect();
    assert_eq!(
        harnesses,
        [
            ("dev-a", Some("claude"), None),
            ("dev-b", Some("antigravity"), None),
            ("system-messenger", Some("claude"), Some(Messenger)),
            ("system-sage", Some("claude"), Some(Sage)),
        ]
    );
    let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
    let messenger = &ants[2];
    let dir = root.join(".ants/agents/system-messenger");
    assert_eq!((&messenger.workdir, messenger.home.as_ref()), (&dir, Some(&dir)));
    assert_eq!(messenger.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    let stock = Ant {
        harness: Some("claude".to_owned()),
        home: Some(dir.clone()),
        js_domain: messenger.js_domain.clone(),
        builtin: Some(Messenger),
        human: Some(ants_test_support::human()),
        ..Ant::new(UserId::messenger(), dir)
    };
    assert_eq!(messenger, &stock, "harness defaults for model, reasoning effort and context; stock settings");

    let alone = parse_here("harness = \"antigravity\"").unwrap();
    assert_eq!(alone.len(), 1, "harness alone is a complete file");
    assert_eq!((alone[0].name.as_str(), alone[0].builtin), ("system-messenger", Some(Messenger)));
    assert_eq!(alone[0].harness.as_deref(), Some("antigravity"));
}

#[test]
fn the_builtin_messenger_is_on_unless_turned_off() {
    let empty = parse_here("").unwrap();
    let only: Vec<_> = empty.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref(), ant.builtin)).collect();
    assert_eq!(only, [("system-messenger", Some("copilot"), Some(ants_proto::BuiltinAnt::Messenger))]);

    let ants = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    let names: Vec<_> = ants.iter().map(|ant| ant.name.as_str()).collect();
    assert_eq!(names, ["dev-a", "system-messenger", "system-sage"]);

    let ants = parse_here("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    let names: Vec<_> = ants.iter().map(|ant| ant.name.as_str()).collect();
    assert_eq!(names, ["dev-a", "system-sage"]);

    assert!(parse_here("messenger = false\nharness = \"claude\"").unwrap().is_empty(), "the daemon refuses it");
}

#[test]
fn ordinary_ants_can_use_the_short_names_but_not_the_reserved_ids() {
    let ants = parse_here("[[ant]]\nname = \"messenger\"\nworkdir = \".\"\n[[ant]]\nname = \"sage\"\nworkdir = \".\"")
        .unwrap();
    assert_eq!(
        ants.iter().map(|ant| ant.name.as_str()).collect::<Vec<_>>(),
        ["messenger", "sage", "system-messenger", "system-sage"]
    );
    for ordinary in &ants[..2] {
        assert_eq!(ordinary.builtin, None);
        assert_eq!(ordinary.reviewed_by(), Some(UserId::sage()), "even a regular ant named sage is reviewed");
    }
    for builtin in ants_proto::BuiltinAnt::ALL {
        let err = parse_here(&format!("[[ant]]\nname = {:?}\nworkdir = \".\"", builtin.id().as_str())).unwrap_err();
        assert!(format!("{err:#}").contains("reserved for a built-in ant"), "{err:#}");
    }
}

#[test]
fn harnesses_defined_in_the_file_run_ants_and_the_builtin_messenger() {
    let (ants, harnesses) = parse_with_harnesses(
        r#"
            harness = "my-agent"

            [[ant]]
            name = "dev-a"
            workdir = "."
            env = { SHARED = "ant" }

            [[ant]]
            name = "dev-b"
            workdir = "."
            harness = "claude"

            [harnesses.my-agent]
            command = "bin/my-agent-acp"
            args = ["--stdio"]
            env = { SHARED = "definition", MY_AGENT_LOG = "warn" }
            mode = "default"

            [harnesses.home-agent]
            command = "~/bin/agent"

            [harnesses.path-agent]
            command = "agent-acp"

            [harnesses.claude]
            command = "/opt/claude/claude-agent-acp"
            "#,
    )
    .unwrap();
    let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
    let harness = |name: &str| harnesses.get(name).unwrap();
    assert_eq!(harness("my-agent").command, manifest_dir().join("bin/my-agent-acp"), "relative to the file");
    assert_eq!(harness("home-agent").command, root.join("bin/agent"));
    assert_eq!(harness("path-agent").command, PathBuf::from("agent-acp"), "looked up on PATH");
    assert_eq!(harness("claude").command, PathBuf::from("/opt/claude/claude-agent-acp"));
    assert_eq!(harness("claude").mode, None, "a definition replaces the built-in one whole");
    assert_eq!(harness("antigravity"), Harnesses::builtin().get("antigravity").unwrap());

    let named: Vec<_> = ants.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref())).collect();
    let builtins = [("system-messenger", Some("my-agent")), ("system-sage", Some("my-agent"))];
    assert_eq!(named, [[("dev-a", Some("my-agent")), ("dev-b", Some("claude"))].as_slice(), &builtins].concat());
    let configs: Vec<_> = ants.into_iter().map(|ant| ant.into_config("nats://unused", &harnesses).unwrap()).collect();
    assert_eq!(configs[0].harness.harness, harness("my-agent"));
    assert_eq!(configs[0].harness.harness.mode.as_deref(), Some("default"));
    assert_eq!(configs[1].harness.harness, harness("claude"));
    assert_eq!(configs[2].harness.harness, harness("my-agent"));

    let mut alone = Ant::new("dev-a".parse().unwrap(), manifest_dir());
    alone.harness = Some("my-agent".to_owned());
    let err = config(alone).unwrap_err();
    assert!(format!("{err:#}").contains("ant dev-a: unknown harness \"my-agent\""), "{err:#}");
}

#[test]
fn unknown_harness_references_and_bad_definitions_fail_to_load() {
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    let top = err("harness = \"my-agent\"");
    assert!(top.contains("config.toml") && top.contains("unknown harness \"my-agent\""), "{top}");
    let ant = err("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"my-agent\"");
    assert!(ant.contains("ant dev-a: unknown harness \"my-agent\""), "{ant}");
    let empty = err("[harnesses.my-agent]\ncommand = \"\"");
    assert!(empty.contains("config.toml") && empty.contains("harness my-agent: command must not be empty"), "{empty}");
    assert!(err("[harnesses.my-agent]\ncommand = \"x\"\nshell = \"sh -c\"").contains("unknown field `shell`"));
    assert!(err("[harnesses.my-agent]\nargs = []").contains("missing field `command`"));
    assert!(err("[harnesses.\"My.Agent\"]\ncommand = \"x\"").contains("use lowercase letters"));
    let protocol = err("[harnesses.my-agent]\ncommand = \"x\"\nprotocol = \"vim\"");
    assert!(protocol.contains("config.toml") && protocol.contains("unknown variant `vim`"), "{protocol}");
    let mode = err("[harnesses.my-agent]\ncommand = \"x\"\nprotocol = \"codex\"\nmode = \"default\"");
    assert!(mode.contains("config.toml") && mode.contains("protocol codex takes mode"), "{mode}");
    let summary = Summary::parse("[harnesses.my-agent]\ncommand = \"\"", Path::new("/c/config.toml"), Path::new("/h"));
    assert!(format!("{:#}", summary.unwrap_err()).contains("command must not be empty"));
}

#[test]
fn fallbacks_name_known_harnesses_and_models() {
    let ant = |fallback: &str| {
        format!(
            "messenger = false\nharness = \"my-agent\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nmodel = \"m1\"\n\
             fallback = {fallback}\n[harnesses.my-agent]\ncommand = \"x\""
        )
    };
    let (ants, harnesses) = parse_with_harnesses(&ant(r#"["claude", "copilot:gpt-5", ":m2"]"#)).unwrap();
    let config = ants.into_iter().next().unwrap().into_config("nats://unused", &harnesses).unwrap();
    let names: Vec<_> = config.harness.fallbacks.iter().map(crate::limits::fallback::Fallback::name).collect();
    assert_eq!(names, ["claude", "copilot:gpt-5", "my-agent:m2"]);
    assert_eq!(config.harness.fallbacks[2].harness, config.harness.harness, "another model of the ant's own harness");

    let err = |fallback: &str| format!("{:#}", parse_here(&ant(fallback)).unwrap_err());
    let unknown = err(r#"["vim"]"#);
    assert!(unknown.contains("config.toml: ant dev-a: fallback \"vim\": unknown harness \"vim\""), "{unknown}");
    assert!(err(r#"[":m1"]"#).contains("the ant's own harness and model"));
    assert!(err(r#"["claude:"]"#).contains("names no model"));
    assert!(err(r#""claude""#).contains("invalid type"), "a list");
}

#[test]
fn the_summary_resolves_harnesses_for_direct_use() {
    let text = "harness = \"my-agent\"\n[harnesses.my-agent]\ncommand = \"./my-agent\"";
    let summary = Summary::parse(text, Path::new("/c/config.toml"), Path::new("/h")).unwrap();
    assert_eq!(summary.harness(None).unwrap().command, PathBuf::from("/c/./my-agent"));
    assert_eq!(summary.harness(Some("claude")).unwrap().name(), "claude");
    assert!(summary.harness(Some("vim")).is_err());
    let unset = Summary::default().harness(None).unwrap_err();
    assert!(unset.is::<crate::harness::Unset>(), "no default harness: {unset}");
}

#[test]
fn dropin_files_define_harnesses_the_ants_may_name() {
    let dir = ants_test_support::scratch_dir("config-dropins").canonicalize().unwrap();
    let config = dir.join("config.toml");
    let dropins = dir.join("harnesses.d");
    std::fs::create_dir(&dropins).unwrap();
    let text = "messenger = false\nsage = \"none\"\nhuman = \"human\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"codex\"\n\
                    [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nharness = \"mine\"\n\
                    [harnesses.mine]\ncommand = \"mine-acp\"\n";
    std::fs::write(&config, text).unwrap();
    std::fs::write(dropins.join("codex.toml"), "[harnesses.codex]\ncommand = \"./codex-acp\"\nmode = \"read-only\"")
        .unwrap();
    std::fs::write(dropins.join("claude.toml"), "[harnesses.claude]\ncommand = \"claude-acp\"").unwrap();
    std::fs::write(dropins.join(".hidden.toml"), "not toml").unwrap();
    std::fs::write(dropins.join("notes.txt"), "not toml").unwrap();
    let loaded = load(&config).unwrap();
    let codex = loaded.harnesses.get("codex").unwrap();
    assert_eq!(codex.command, dropins.join("./codex-acp"), "relative to harnesses.d");
    assert_eq!(codex.file(), Some(dropins.join("codex.toml").as_path()));
    assert_eq!(loaded.harnesses.get("claude").unwrap().mode, None, "replaces the built-in one whole");
    assert_eq!(loaded.harnesses.get("mine").unwrap().file(), None);
    let configs: Vec<_> =
        loaded.ants.into_iter().map(|ant| ant.into_config("nats://unused", &loaded.harnesses).unwrap()).collect();
    assert_eq!((configs[0].harness.harness.name(), configs[1].harness.harness.name()), ("codex", "mine"));
    assert_eq!(Summary::load(&config).unwrap().harness(Some("codex")).unwrap(), codex);
    std::fs::remove_file(&config).unwrap();
    assert_eq!(Summary::load(&config).unwrap().harness(Some("codex")).unwrap(), codex, "no config file");

    let err = |file: &str, text: &str| {
        std::fs::write(dropins.join(file), text).unwrap();
        let err = format!("{:#}", Summary::load(&config).unwrap_err());
        std::fs::remove_file(dropins.join(file)).unwrap();
        err
    };
    let twice = err("other.toml", "[harnesses.codex]\ncommand = \"x\"");
    let (first, second) = (dropins.join("codex.toml"), dropins.join("other.toml"));
    assert!(twice.contains(&format!("in both {} and {}", first.display(), second.display())), "{twice}");
    std::fs::write(&config, "[harnesses.codex]\ncommand = \"x\"").unwrap();
    let with_config = format!("{:#}", load(&config).unwrap_err());
    assert!(with_config.contains(&format!("in both {} and {}", config.display(), first.display())), "{with_config}");
    std::fs::write(&config, "").unwrap();
    let path = dropins.join("bad.toml");
    let unknown = err("bad.toml", "harness = \"codex\"");
    assert!(unknown.contains(&path.display().to_string()) && unknown.contains("unknown field `harness`"), "{unknown}");
    let field = err("bad.toml", "[harnesses.bad]\ncommand = \"x\"\nshell = \"sh\"");
    assert!(field.contains(&path.display().to_string()) && field.contains("unknown field `shell`"), "{field}");
    let empty = err("bad.toml", "[harnesses.bad]\ncommand = \"\"");
    assert!(empty.contains(&path.display().to_string()) && empty.contains("must not be empty"), "{empty}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_summary_keeps_each_ant_s_caps() {
    let text = "[[ant]]\nname = \"dev-a\"\nworkdir = \"/nowhere\"\nmax_daily_cost = 2.5\nmax_thread_tokens = 900";
    let summary = Summary::parse(text, Path::new("/c/config.toml"), Path::new("/h")).unwrap();
    let caps = Caps { daily_cost: Some(2.5), thread_tokens: Some(900), ..Caps::default() };
    assert_eq!(summary.caps.get(&"dev-a".parse().unwrap()), Some(&caps));
}

#[test]
fn without_a_top_level_harness_each_ant_and_template_names_its_own() {
    let unset = |text: &str| {
        let e = parse_as_is(text).unwrap_err();
        assert!(harness::Unset::caused(&e), "{e:#}");
        format!("{e:#}")
    };
    let e = unset("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"");
    assert!(e.contains(": ant dev-a: no harness set: add `harness = \"NAME\"`"), "{e}");
    assert!(e.contains("(antigravity, claude, codex, copilot)") && e.contains("`ants setup`"), "{e}");
    assert!(unset("").contains(": ant system-messenger: no harness set"), "nor the built-in messenger");
    let template = "messenger = false\n[[ant_template]]\nname = \"worker\"\nmanagers = [\"lead\"]\nmax_ants = 1\n\
                    workdirs = [\".\"]\n[[ant]]\nname = \"lead\"\nworkdir = \".\"\nharness = \"codex\"";
    let e = unset(template);
    assert!(e.contains(": ant template worker lists no harnesses: no harness set"), "{e}");

    let ants = parse_as_is(
        "messenger = false\nsage = \"none\"\nhuman = \"human\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"codex\"",
    )
    .unwrap()
    .ants;
    let codex = config(ants[0].clone()).unwrap();
    assert_eq!((codex.harness.harness.name(), codex.presence.builtin), ("codex", None));
    let e = config(Ant::new("dev-a".parse().unwrap(), manifest_dir())).unwrap_err();
    assert!(harness::Unset::caused(&e), "{e:#}");
}

#[test]
fn the_builtin_messenger_finds_its_credentials_and_creates_its_workdir() {
    let home = ants_test_support::scratch_dir("config-home").canonicalize().unwrap();
    let auth = home.join(".config/ants/auth");
    assert_eq!(builtin_messenger(Some("copilot"), &home).creds, None, "no credentials issued");
    std::fs::create_dir_all(&auth).unwrap();
    std::fs::write(auth.join("system-messenger.creds"), "").unwrap();
    let messenger = builtin_messenger(Some("copilot"), &home);
    assert_eq!(messenger.creds, Some(auth.join("system-messenger.creds")));
    let config = config(messenger).unwrap();
    assert_eq!(config.presence.builtin, Some(ants_proto::BuiltinAnt::Messenger));
    assert_eq!(config.harness.workdir, home.join(".ants/agents/system-messenger").canonicalize().unwrap());
    assert_eq!(config.harness.model_settings, ModelSettings::default());
    std::fs::remove_dir_all(&home).unwrap();
}
