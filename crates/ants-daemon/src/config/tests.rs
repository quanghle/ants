use super::*;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Parse as if the file sat in this crate's directory, with the workspace root as home and `harness = "copilot"` on
/// top unless the file names its own top-level harness.
fn parse_here(text: &str) -> Result<Vec<Ant>> {
    Ok(parse_with_harnesses(text)?.0)
}

fn parse_with_harnesses(text: &str) -> Result<(Vec<Ant>, Harnesses)> {
    parse_all(text).map(|loaded| (loaded.ants, loaded.harnesses))
}

fn parse_all(text: &str) -> Result<Loaded> {
    parse_as_is(&with_harness(text))
}

/// Parse as [`parse_here`] does, the file as it is.
fn parse_as_is(text: &str) -> Result<Loaded> {
    let dir = manifest_dir();
    parse(text, &dir.join("config.toml"), dir.parent().unwrap().parent().unwrap())
}

/// `text` with `harness = "copilot"` on top unless it names a top-level harness.
fn with_harness(text: &str) -> String {
    let top = text.lines().map(str::trim).take_while(|line| !line.starts_with('['));
    if top.clone().any(|line| line.starts_with("harness")) {
        text.to_owned()
    } else {
        format!("harness = \"copilot\"\n{text}")
    }
}

fn config(ant: Ant) -> Result<Config> {
    ant.into_config("nats://unused", &Harnesses::builtin())
}

#[test]
fn parses_several_ants() {
    let ants = parse_here(
        r#"
            nats = "wss://hub.example/nats"
            js_domain = "hub"
            human = "alice"

            [[ant]]
            name = "dev-a"
            workdir = "~/crates/ants"
            creds = "~/auth/dev-a.creds"
            home = "/srv/ants/dev-a"
            whoami = """Maintain the Rust backend.
Review changes for correctness."""
            memory = "Use the memory MCP server."
            tasks = "Use the tracker CLI."
            env = { TRACKER_DB = "/srv/ants/dev-a/tasks.db" }
            harness = "copilot"
            model = "test-model"
            reasoning_effort = "high"
            context = "long_context"
            history_budget = 131072
            allow = ["read", "search"]
            capabilities = ["rust", "review"]
            max_harnesses = 2
            worktrees = true
            idle_timeout = "1m"
            prompt_timeout = "0"
            max_tool_calls = 50
            max_thread_cost = 2.5
            max_daily_tokens = 1000000
            spend_warn_percent = 90
            escalate_to = ["lead", "dev-b"]
            max_escalation_hops = 2
            escalate_after = "0"
            escalation_reminder = "0"
            stand_in = "carol"

            [[ant]]
            name = "dev-b"
            nats = "nats://127.0.0.1:4222"
            human = "bob"
            workdir = "src"
            "#,
    )
    .unwrap();
    let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
    let a = &ants[0];
    assert_eq!(a.name.as_str(), "dev-a");
    assert_eq!(a.nats.as_deref(), Some("wss://hub.example/nats"));
    assert_eq!(a.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(a.human.as_ref().map(UserId::as_str), Some("alice"));
    assert_eq!(a.workdir, root.join("crates/ants").canonicalize().unwrap());
    assert_eq!(a.creds, Some(root.join("auth/dev-a.creds")));
    assert_eq!(a.home, Some(PathBuf::from("/srv/ants/dev-a")));
    assert_eq!((a.allow.len(), a.capabilities.len()), (2, 2));
    assert_eq!((a.max_harnesses, a.idle_timeout), (2, Duration::from_secs(60)));
    let config = a.clone().into_config("nats://unused", &Harnesses::builtin()).unwrap();
    assert_eq!(
        config.harness.model_settings,
        ModelSettings {
            model: Some("test-model".to_owned()),
            reasoning_effort: Some("high".to_owned()),
            context: Some("long_context".to_owned()),
            history_budget: Some(131072),
        }
    );
    assert_eq!(config.presence.whoami.as_deref(), Some("Maintain the Rust backend.\nReview changes for correctness."));
    assert_eq!(
        config.harness.instructions,
        "[ants role dev-a]\nMaintain the Rust backend.\nReview changes for correctness.\n\n\
             [ants memory]\nUse the memory MCP server.\n\n[ants tasks]\nUse the tracker CLI."
    );
    assert_eq!(config.harness.env.get("TRACKER_DB").map(String::as_str), Some("/srv/ants/dev-a/tasks.db"));
    assert_eq!(config.harness.limits, PromptLimits { timeout: None, max_tool_calls: Some(50) }, "0 is no limit");
    let caps = Caps { thread_cost: Some(2.5), daily_tokens: Some(1_000_000), ..Caps::default() };
    assert_eq!(config.spending.caps, caps);
    assert_eq!(config.spending.warn_percent, 90);
    let escalate_to = vec!["lead".parse().unwrap(), "dev-b".parse().unwrap()];
    let chain = Chain { escalate_to, max_hops: 2, after: None, remind: None, stand_in: Some("carol".parse().unwrap()) };
    assert_eq!(config.policy.escalation, chain, "0 waits for the leader for good and never reminds");
    assert_eq!(config.worktrees.per_thread, Some(true));
    check_dev_b(ants[1].clone());
}

/// Check dev-b of [`parses_several_ants`], which sets only its connection, human and workdir.
fn check_dev_b(b: Ant) {
    assert_eq!(b.nats.as_deref(), Some("nats://127.0.0.1:4222"));
    assert_eq!(b.workdir, manifest_dir().join("src").canonicalize().unwrap());
    let defaults = Ant::new(b.name.clone(), b.workdir.clone());
    let set = Ant { nats: b.nats.clone(), js_domain: b.js_domain.clone(), human: b.human.clone(), ..defaults };
    assert_eq!(b, Ant { harness: Some("copilot".to_owned()), ..set }, "the file's harness");

    let config = b.into_config("nats://unused", &Harnesses::builtin()).unwrap();
    assert_eq!(config.connection.nats_url, "nats://127.0.0.1:4222");
    assert_eq!(config.policy.human.as_str(), "bob", "an ant's own default human comes first");
    assert!(config.policy.allow.is_empty(), "rejects every tool request unless the human allows it");
    assert_eq!(config.policy.approval_timeout, crate::approvals::DEFAULT_TIMEOUT);
    assert_eq!(config.pooling.idle_timeout, DEFAULT_IDLE_TIMEOUT);
    assert_eq!(config.worktrees.per_thread, None, "settled at start by crate::worktrees::per_thread");
    assert_eq!(config.harness.limits, PromptLimits::default());
    assert_eq!(config.spending.caps, Caps::default(), "no spend caps unless set");
    assert_eq!(config.spending.warn_percent, DEFAULT_SPEND_WARN_PERCENT);
    assert_eq!(config.policy.escalation, Chain::default(), "no chain: escalations go to the owner");
    assert_eq!(config.policy.escalation.after, Some(DEFAULT_ESCALATE_AFTER));
    assert_eq!(config.policy.escalation.remind, Some(DEFAULT_ESCALATION_REMINDER));
    assert!(config.storage.home.ends_with(".ants/dev-b"));
    assert_eq!(config.presence.whoami, None);
    assert_eq!(config.harness.model_settings, ModelSettings::default());
    assert!(config.harness.instructions.is_empty(), "no memory or task tool by default");
    assert!(config.harness.env.is_empty(), "no tool-specific environment by default");
}

#[test]
fn whoami_memory_and_tasks_are_independent_and_can_be_cleared() {
    let mut ant = Ant::new("dev-a".parse().unwrap(), manifest_dir());
    ant.whoami = Some("Maintain the backend.".to_owned());
    ant.memory = Some("Use notes.".to_owned());
    assert_eq!(ant.prompt_instructions(), "[ants role dev-a]\nMaintain the backend.\n\n[ants memory]\nUse notes.");
    ant.whoami = Some(String::new());
    assert_eq!(ant.prompt_instructions(), "[ants memory]\nUse notes.");
    ant.memory = Some(String::new());
    ant.tasks = Some("Use tickets.".to_owned());
    assert_eq!(ant.prompt_instructions(), "[ants tasks]\nUse tickets.");
    ant.whoami = Some(" \n".to_owned());
    ant.tasks = Some(" \n".to_owned());
    assert!(ant.prompt_instructions().is_empty());
}

#[test]
fn invalid_native_settings_fail_before_starting_an_ant() {
    for (harness, model, effort, context, error) in [
        ("copilot", Some(" "), None, None, "model must not be blank"),
        ("claude", None, Some(""), None, "reasoning_effort must not be blank"),
        ("copilot", None, None, Some("128000"), "harness copilot: context must be default or long_context"),
        ("claude", None, None, Some("1m"), "harness claude: context 1m requires an explicit model"),
        ("claude", Some("smart[1m]"), None, Some("default"), "conflicts with model smart[1m]"),
        ("antigravity", None, None, Some("long_context"), "harness antigravity has no context setting"),
        ("vim", None, None, None, "unknown harness \"vim\""),
    ] {
        let mut ant = Ant::new("dev-a".parse().unwrap(), manifest_dir());
        ant.harness = Some(harness.to_owned());
        ant.model = model.map(str::to_owned);
        ant.reasoning_effort = effort.map(str::to_owned);
        ant.context = context.map(str::to_owned);
        let err = format!("{:#}", config(ant).unwrap_err());
        assert!(err.contains(error), "{err}");
    }
}

#[test]
fn rejects_invalid_environment_without_exposing_values() {
    for (name, value) in [("", "secret"), ("bad=name", "secret"), ("bad\0name", "secret"), ("NAME", "secret\0")] {
        let mut ant = Ant::new("dev-a".parse().unwrap(), manifest_dir());
        ant.env.insert(name.to_owned(), value.to_owned());
        let err = config(ant).unwrap_err().to_string();
        assert!(err.contains("invalid harness environment variable"), "{err}");
        assert!(!err.contains("secret"), "{err}");
    }
    let err = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nenv = { 'bad=name' = 'secret' }")
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid harness environment variable"), "{err}");
}

#[test]
fn falls_back_to_the_default_server_and_human() {
    let ants = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    assert_eq!((&ants[0].nats, &ants[0].human), (&None, &None));
    let config = ants[0].clone().into_config("nats://leaf:4222", &Harnesses::builtin()).unwrap();
    assert_eq!((config.connection.nats_url.as_str(), config.policy.human), ("nats://leaf:4222", UserId::human()));
    let err = parse_here("human = \"Not A Name\"").unwrap_err();
    assert!(format!("{err:#}").contains("human"), "{err:#}");
}

#[test]
fn metrics_are_off_unless_an_address_is_given() {
    assert_eq!(parse_all("").unwrap().metrics, None);
    let loaded = parse_all("metrics = \"127.0.0.1:9464\"").unwrap();
    assert_eq!(loaded.metrics, Some("127.0.0.1:9464".parse().unwrap()));
    let err = format!("{:#}", parse_all("metrics = \"localhost\"").unwrap_err());
    assert!(err.contains("metrics"), "{err}");
}

#[test]
fn redaction_is_on_unless_turned_off() {
    let ant = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"";
    let redact = |text: &str| parse_here(text).unwrap().iter().map(|ant| ant.redact).collect::<Vec<_>>();
    assert_eq!(redact(ant), [true, true]);
    assert_eq!(redact(&format!("redact = false\n{ant}")), [false, false], "the messenger too");
}

#[test]
fn names_the_machine_as_the_file_does() {
    let ant = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"";
    let machines = |text: &str| parse_here(text).unwrap().into_iter().map(|ant| ant.machine).collect::<Vec<_>>();
    assert_eq!(machines(ant), [None, None], "the host name");
    let named = machines(&format!("machine = \"dev-vm1\"\n{ant}"));
    assert_eq!(named, [Some("dev-vm1".to_owned()), Some("dev-vm1".to_owned())], "the messenger too");
    let mut ants = parse_here(&format!("machine = \"dev-vm1\"\n{ant}")).unwrap();
    assert_eq!(config(ants.remove(0)).unwrap().presence.machine.as_deref(), Some("dev-vm1"));
    let long = "m".repeat(65);
    for (machine, why) in [("\" \"", "blank"), (&format!("\"{long}\""), "longer than 64"), ("\"vm\\n1\"", "control")] {
        let err = format!("{:#}", parse_here(&format!("machine = {machine}\n{ant}")).unwrap_err());
        assert!(err.contains("machine") && err.contains(why), "{err}");
    }
}

#[test]
fn refuses_a_server_with_a_user_and_password() {
    let ant = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"";
    for text in [format!("nats = \"nats://dev:secret@hub\"\n{ant}"), format!("{ant}\nnats = \"wss://secret@hub\"")] {
        let err = format!("{:#}", parse_here(&text).unwrap_err());
        assert!(err.contains("ant dev-a: NATS URL") && err.contains(".creds") && !err.contains("secret"), "{err}");
    }
    let ants = parse_here(ant).unwrap();
    let err = ants[0].clone().into_config("nats://dev:secret@leaf", &Harnesses::builtin()).unwrap_err();
    assert!(format!("{err:#}").contains("ant dev-a: NATS URL nats://***@leaf"), "{err:#}");
}

#[test]
fn the_top_level_harness_is_the_default_one_and_runs_the_builtin_messenger() {
    let ants = parse_here(
        "harness = \"claude\"\njs_domain = \"hub\"\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n\
             [[ant]]\nname = \"dev-b\"\nworkdir = \".\"\nharness = \"antigravity\"",
    )
    .unwrap();
    let harnesses: Vec<_> = ants.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref(), ant.builtin)).collect();
    assert_eq!(
        harnesses,
        [("dev-a", Some("claude"), false), ("dev-b", Some("antigravity"), false), ("messenger", Some("claude"), true),]
    );
    let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
    let messenger = &ants[2];
    let dir = root.join(".ants/messenger");
    assert_eq!((&messenger.workdir, messenger.home.as_ref()), (&dir, Some(&dir)));
    assert_eq!(messenger.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    let stock = Ant {
        harness: Some("claude".to_owned()),
        home: Some(dir.clone()),
        js_domain: messenger.js_domain.clone(),
        builtin: true,
        ..Ant::new(UserId::messenger(), dir)
    };
    assert_eq!(messenger, &stock, "harness defaults for model, reasoning effort and context; stock settings");

    let alone = parse_here("harness = \"antigravity\"").unwrap();
    assert_eq!(alone.len(), 1, "harness alone is a complete file");
    assert_eq!((alone[0].name.as_str(), alone[0].builtin), ("messenger", true));
    assert_eq!(alone[0].harness.as_deref(), Some("antigravity"));
}

#[test]
fn the_builtin_messenger_is_on_unless_turned_off() {
    let empty = parse_here("").unwrap();
    let only: Vec<_> = empty.iter().map(|ant| (ant.name.as_str(), ant.harness.as_deref(), ant.builtin)).collect();
    assert_eq!(only, [("messenger", Some("copilot"), true)], "the file's harness");

    let ants = parse_here("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    let names: Vec<_> = ants.iter().map(|ant| ant.name.as_str()).collect();
    assert_eq!(names, ["dev-a", "messenger"]);

    let ants = parse_here("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    assert_eq!((ants.len(), ants[0].name.as_str()), (1, "dev-a"));

    let err = format!("{:#}", parse_here("messenger = false\nharness = \"claude\"").unwrap_err());
    assert!(err.contains("nothing to serve"), "{err}");
}

#[test]
fn a_listed_messenger_replaces_the_builtin_one() {
    let ants = parse_here("harness = \"claude\"\n[[ant]]\nname = \"messenger\"\nworkdir = \".\"").unwrap();
    assert_eq!(ants.len(), 1);
    assert_eq!((ants[0].harness.as_deref(), ants[0].builtin), (Some("claude"), false));
    assert_eq!(ants[0].workdir, manifest_dir().canonicalize().unwrap());
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
    assert_eq!(named, [("dev-a", Some("my-agent")), ("dev-b", Some("claude")), ("messenger", Some("my-agent"))]);
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
    let names: Vec<_> = config.harness.fallbacks.iter().map(crate::fallback::Fallback::name).collect();
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
    let text = "messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"codex\"\n\
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
    assert!(unset("").contains(": ant messenger: no harness set"), "nor the built-in messenger");
    let template = "messenger = false\n[[ant_template]]\nname = \"worker\"\nmanagers = [\"lead\"]\nmax_ants = 1\n\
                    workdirs = [\".\"]\n[[ant]]\nname = \"lead\"\nworkdir = \".\"\nharness = \"codex\"";
    let e = unset(template);
    assert!(e.contains(": ant template worker lists no harnesses: no harness set"), "{e}");

    let ants =
        parse_as_is("messenger = false\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"codex\"").unwrap().ants;
    let codex = config(ants[0].clone()).unwrap();
    assert_eq!((codex.harness.harness.name(), codex.presence.defer_to_others), ("codex", false));
    let e = config(Ant::new("dev-a".parse().unwrap(), manifest_dir())).unwrap_err();
    assert!(harness::Unset::caused(&e), "{e:#}");
}

#[test]
fn the_builtin_messenger_finds_its_credentials_and_creates_its_workdir() {
    let home = ants_test_support::scratch_dir("config-home").canonicalize().unwrap();
    let auth = home.join(".config/ants/auth");
    assert_eq!(builtin_messenger(Some("copilot"), &home).creds, None, "no credentials issued");
    std::fs::create_dir_all(&auth).unwrap();
    std::fs::write(auth.join("messenger.creds"), "").unwrap();
    let messenger = builtin_messenger(Some("copilot"), &home);
    assert_eq!(messenger.creds, Some(auth.join("messenger.creds")));
    let config = config(messenger).unwrap();
    assert!(config.presence.defer_to_others);
    assert_eq!(config.harness.workdir, home.join(".ants/messenger").canonicalize().unwrap());
    assert_eq!(config.harness.model_settings, ModelSettings::default());
    std::fs::remove_dir_all(&home).unwrap();
}

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
fn rejects_bad_files() {
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    let twice = err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"");
    assert!(twice.contains("ant dev-a is listed twice"), "{twice}");
    assert!(twice.contains("config.toml"), "{twice}");
    let missing = err("[[ant]]\nname = \"dev-a\"\nworkdir = \"no/such/dir\"");
    assert!(missing.contains("workdir no/such/dir"), "{missing}");
    assert!(err("harness = \"vim\"").contains("unknown harness"));
    assert!(err("provider = \"copilot\"").contains("unknown field `provider`"));
    assert!(err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"vim\"").contains("unknown harness"));
    assert!(err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nworkdirs = 1").contains("unknown field"));
    assert!(err("[[ant]]\nname = \"Dev.A\"\nworkdir = \".\"").contains("parsing config"));
    let chain = |names: &str| err(&format!("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nescalate_to = {names}"));
    assert!(chain("[\"dev-a\"]").contains("escalate_to names the ant itself"));
    let stand_in = err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nstand_in = \"dev-a\"");
    assert!(stand_in.contains("stand_in names the ant itself"), "{stand_in}");
    let warn = err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nspend_warn_percent = 100");
    assert!(warn.contains("spend_warn_percent must be below 100"), "{warn}");
    assert!(chain("[\"lead\", \"lead\"]").contains("escalate_to lists lead twice"));
    assert!(chain("[\"Lead!\"]").contains("parsing config"));
}

#[test]
fn the_storage_table_reaches_every_ant() {
    let loaded = parse_all(
        "[storage]\nwarn_percent = 90\nmax_file_store = \"10GB\"\nmax_memory_store = 1024\n\
             [[ant]]\nname = \"dev-a\"\nworkdir = \".\"",
    )
    .unwrap();
    let settings = storage::Settings { warn_percent: 90, max_file_store: Some(10 << 30), max_memory_store: Some(1024) };
    assert_eq!(loaded.storage, settings);
    assert_eq!(loaded.ants.len(), 2, "dev-a and the messenger");
    assert!(loaded.ants.iter().all(|ant| ant.storage == settings));
    assert_eq!(config(loaded.ants[0].clone()).unwrap().storage.jetstream, settings);
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    assert!(err("[storage]\nwarn_percent = 100").contains("warn_percent must be below 100"));
    assert!(err("[storage]\nmax_file_store = \"10 parsecs\"").contains("invalid size"));
    assert!(err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nstorage = {}").contains("unknown field"));
}

#[test]
fn the_memory_pressure_table_reaches_every_ant() {
    let loaded = parse_all(
        "[memory_pressure]\nmin_available = \"10%\"\nmax_harnesses = 2\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"",
    )
    .unwrap();
    let settings = pressure::Settings { min_available: pressure::Threshold::Percent(10), max_harnesses: 2 };
    assert!(loaded.ants.iter().all(|ant| ant.memory_pressure == settings));
    assert_eq!(config(loaded.ants[0].clone()).unwrap().pooling.memory_pressure, settings);
    let default = parse_all("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"").unwrap();
    assert_eq!(default.ants[0].memory_pressure, pressure::Settings::default(), "on unless turned off");
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    assert!(err("[memory_pressure]\nmax_harnesses = 0").contains("max_harnesses must be at least 1"));
    assert!(err("[memory_pressure]\nmin_available = \"100%\"").contains("invalid share"));
}

#[test]
fn schedules_take_the_top_level_server_and_unique_names() {
    let loaded = parse_all(
        "nats = \"wss://hub.example/nats\"\njs_domain = \"hub\"\n\
             [[schedule]]\nname = \"nightly\"\ncreds = \"~/auth/nightly.creds\"\ncron = \"0 3 * * *\"\n\
             ant = \"dev-a\"\nowner = \"alice\"\ntext = \"Audit.\"\n\
             [[schedule]]\nname = \"pulse\"\nnats = \"nats://127.0.0.1:4222\"\njs_domain = \"own\"\nevery = \"15m\"\n\
             channel = \"ops\"\nmention = [\"dev-b\"]\ntext = \"Stuck?\"\n",
    )
    .unwrap();
    let [nightly, pulse] = &loaded.schedules[..] else { panic!("two schedules") };
    let root = manifest_dir().parent().unwrap().parent().unwrap().to_owned();
    assert_eq!(nightly.creds, Some(root.join("auth/nightly.creds")));
    assert_eq!(nightly.nats.as_deref(), Some("wss://hub.example/nats"));
    assert_eq!(nightly.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(nightly.owner.as_ref().map(UserId::as_str), Some("alice"));
    assert_eq!(pulse.nats.as_deref(), Some("nats://127.0.0.1:4222"));
    assert_eq!(pulse.js_domain.as_ref().map(JsDomain::as_str), Some("own"));
    assert_eq!(pulse.mentions, schedule::Mentions::Listed(vec!["dev-b".parse().unwrap()]));

    let err = |text: &str| format!("{:#}", parse_all(text).unwrap_err());
    let one = "[[schedule]]\nname = \"nightly\"\ncron = \"@daily\"\nant = \"dev-a\"\ntext = \"t\"\n";
    assert!(err(&format!("{one}{one}")).contains("schedule nightly is listed twice"));
    let ant = one.replace("nightly", "messenger");
    assert!(err(&ant).contains("schedule messenger has the name of an ant"));
    let bad = err(&one.replace("@daily", "0 25 * * *"));
    assert!(bad.contains("config.toml: schedule nightly: cron \"0 25 * * *\": hour"), "{bad}");
    let bad = err(&format!("timezone = \"Mars/Olympus_Mons\"\n{one}"));
    assert!(bad.contains("config.toml: timezone \"Mars/Olympus_Mons\": unknown time zone"), "{bad}");
    let bad = err(&format!("{one}timezone = \"Europe/Londn\"\n"));
    assert!(bad.contains("schedule nightly: timezone \"Europe/Londn\": unknown time zone"), "{bad}");

    let zoned = |text: &str| parse_all(text).unwrap().schedules[0].when.to_string();
    assert_eq!(zoned(one), "cron \"@daily\"", "UTC by default");
    let tokyo = format!("timezone = \"Asia/Tokyo\"\n{one}");
    assert_eq!(zoned(&tokyo), "cron \"@daily\" in Asia/Tokyo", "the top-level default");
    assert_eq!(zoned(&format!("{tokyo}timezone = \"UTC\"\n")), "cron \"@daily\"", "the schedule's own");
}

#[test]
fn ants_login_sets_the_cli_connection_where_cli_names_no_credentials() {
    let (path, home) = (Path::new("/team/config.toml"), Path::new("/home/alice"));
    let creds = Path::new("/home/alice/.config/ants/auth/alice.creds");
    let set = |text: &str| cli_login_text(text, path, home, "wss://hub.example/nats", creds);
    let (fresh, nats) = set("").unwrap().unwrap();
    assert!(nats);
    assert_eq!(
        fresh,
        "[cli]\ncreds = \"/home/alice/.config/ants/auth/alice.creds\"\nnats = \"wss://hub.example/nats\"\n"
    );

    let top = "nats = \"nats://127.0.0.1:4222\" # the leaf node\n";
    let (appended, nats) = set(top).unwrap().unwrap();
    assert!(nats, "the top-level server is the ants'");
    assert!(
        appended.starts_with(top)
            && appended.ends_with(
                "\n\n[cli]\ncreds = \"/home/alice/.config/ants/auth/alice.creds\"\nnats = \"wss://hub.example/nats\"\n"
            ),
        "{appended}"
    );

    let own = "# mine\n[cli] # the CLI\nnats = \"nats://hub:4222\"\n\n[[ant]]\nname = \"dev-a\"\n";
    let (inserted, nats) = set(own).unwrap().unwrap();
    assert!(!nats, "[cli] names a server");
    assert_eq!(
        inserted,
        "# mine\n[cli] # the CLI\ncreds = \"/home/alice/.config/ants/auth/alice.creds\"\nnats = \"nats://hub:4222\"\n\n[[ant]]\nname = \"dev-a\"\n"
    );
    assert_eq!(set("[cli]\ncreds = \"bob.creds\"\n").unwrap(), None, "the CLI's credentials stay");
    assert!(set("cli.js_domain = \"hub\"\n").is_err(), "no [cli] header to put the keys under");
    assert!(set("[cli\n").is_err());
}

#[test]
fn the_cli_section_overrides_the_top_level_server() {
    let (path, home) = (Path::new("/team/config.toml"), Path::new("/home/alice"));
    let cli = |text: &str| CliDefaults::parse(text, path, home);
    assert_eq!(cli("").unwrap(), CliDefaults::default());
    let top = "nats = \"wss://hub.example/nats\"\njs_domain = \"hub\"\n\
                   [[ant]]\nname = \"dev-a\"\nworkdir = \"/nowhere\"\ncreds = \"dev-a.creds\"\nbogus = 1\n";
    let defaults = cli(top).unwrap();
    assert_eq!(defaults.nats.as_deref(), Some("wss://hub.example/nats"));
    assert_eq!(defaults.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(defaults.creds, None, "an ant's credentials are not the CLI's");

    let own = cli(&format!("{top}[cli]\nnats = \"nats://127.0.0.1:4222\"\ncreds = \"~/alice.creds\"\n")).unwrap();
    assert_eq!(own.nats.as_deref(), Some("nats://127.0.0.1:4222"));
    assert_eq!(own.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(own.creds.as_deref(), Some(Path::new("/home/alice/alice.creds")));
    let relative = cli("[cli]\ncreds = \"auth/alice.creds\"\njs_domain = \"own\"").unwrap();
    assert_eq!(relative.creds.as_deref(), Some(Path::new("/team/auth/alice.creds")));
    assert_eq!(relative.js_domain.as_ref().map(JsDomain::as_str), Some("own"));
    assert_eq!(relative.auto_archive, None, "never by default");
    assert_eq!(cli("[cli]\nauto_archive = \"14d\"").unwrap().auto_archive, Some(Duration::from_hours(14 * 24)));
    assert_eq!(cli("[cli]\nauto_archive = \"0\"").unwrap().auto_archive, None);
    assert!(!relative.inline_images, "off by default");
    assert!(cli("[cli]\ninline_images = true").unwrap().inline_images);
    assert!(parse_all("[cli]\ninline_images = true").is_ok(), "the daemon accepts it");
    assert_eq!((relative.theme, relative.animate), (Theme::Auto, true), "asked of the terminal, moving");
    let still = cli("[cli]\ntheme = \"light\"\nanimate = false").unwrap();
    assert_eq!((still.theme, still.animate), (Theme::Light, false));
    assert!(cli("[cli]\ntheme = \"blue\"").is_err());

    assert_eq!(relative.summaries, SummaryConfig::default(), "on demand, by the messenger");
    let ahead = cli("[cli]\nsummaries = \"ahead\"\nsummary_daily_tokens = 50000\nsummary_ant = \"scribe\"").unwrap();
    let scribe = "scribe".parse().unwrap();
    assert_eq!(ahead.summaries, SummaryConfig { mode: Summaries::Ahead, ant: scribe, daily_tokens: 50000 });
    let unbounded = format!("{:#}", cli("[cli]\nsummaries = \"ahead\"").unwrap_err());
    assert!(unbounded.contains("needs summary_daily_tokens above 0"), "{unbounded}");
    assert!(parse_all("[cli]\nsummaries = \"ahead\"").is_err(), "the daemon refuses it too");

    for bad in [
        "[cli]\ncred = \"a.creds\"",
        "[cli]\nauto_archive = 14",
        "[cli]\nsummaries = \"eager\"",
        "nats = 4222",
        "nats = \"nats://u:secret@hub\"",
        "[cli",
    ] {
        let err = format!("{:#}", cli(bad).unwrap_err());
        assert!(err.contains("/team/config.toml") && !err.contains("secret"), "{bad}: {err}");
    }
    assert!(parse_all("[cli]\nnats = \"nats://hub:4222\"\ncreds = \"alice.creds\"").is_ok(), "daemon accepts it");
    assert!(parse_all("[cli]\nharness = \"claude\"").is_err());
}

#[test]
fn durations_need_a_unit() {
    let idle = |line: &str| parse_here(&format!("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n{line}"));
    assert_eq!(idle("idle_timeout = \"10m\"").unwrap()[0].idle_timeout, Duration::from_secs(600));
    for bad in ["idle_timeout_secs = 600", "idle_timeout = 600", "idle_timeout = \"600\""] {
        assert!(idle(bad).is_err(), "{bad} loaded");
    }
    let err = format!("{:#}", idle("escalate_after = \"30\"").unwrap_err());
    assert!(err.contains("a number without a unit"), "{err}");
}

#[test]
fn entries_read_back_as_written() {
    let workdir = manifest_dir().canonicalize().unwrap();
    let plain = Ant::new("dev-a".parse().unwrap(), workdir.clone());
    let text = entry(&plain).unwrap();
    assert_eq!(text, format!("[[ant]]\nname = \"dev-a\"\nworkdir = {:?}\n", workdir.to_str().unwrap()));
    let read = parse_here(&format!("messenger = false\n{text}")).unwrap();
    assert_eq!(read, [Ant { harness: Some("copilot".to_owned()), ..plain }], "the file's harness");

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
    let read: Vec<_> = parse_here(&text).unwrap().into_iter().filter(|ant| !ant.builtin).collect();
    assert_eq!(read, [full]);
}

#[test]
fn appending_keeps_the_file_as_it_was_and_checks_it() {
    let dir = ants_test_support::scratch_dir("config-append").canonicalize().unwrap();
    let path = dir.join("ants/config.toml");
    let ant = |name: &str| Ant { harness: Some("copilot".to_owned()), ..Ant::new(name.parse().unwrap(), dir.clone()) };
    let add = |name: &str| append(&path, &entry(&ant(name)).unwrap());
    let err = format!("{:#}", add("dev-a").unwrap_err());
    assert!(err.contains("ant messenger: no harness set"), "a new file needs a top-level harness: {err}");
    assert!(!path.exists(), "nothing written");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    let old = "# the team\nharness = \"copilot\"\nnats = \"nats://hub:4222\" # the hub\n\n[[ant]]\nname = \"dev-a\"\n\
                   workdir = \".\"\ncreds = \"keys/dev-a.creds\"";
    std::fs::write(&path, old).unwrap();
    add("dev-b").unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.starts_with(&format!("{old}\n\n[[ant]]\nname = \"dev-b\"")), "{text}");
    let names: Vec<_> = load(&path).unwrap().ants.into_iter().map(|ant| ant.name.to_string()).collect();
    assert_eq!(names, ["dev-a", "dev-b", "messenger"]);

    let err = format!("{:#}", add("dev-a").unwrap_err());
    assert!(err.contains("ant dev-a is listed twice"), "{err}");
    assert_eq!(std::fs::read_to_string(&path).unwrap(), text, "left as it was");

    let summary = Summary::load(&path).unwrap();
    assert_eq!(summary.nats.as_deref(), Some("nats://hub:4222"));
    assert_eq!(summary.creds_of(&"dev-a".parse().unwrap()), Some(Some(dir.join("ants/keys/dev-a.creds").as_path())));
    assert_eq!(summary.creds_of(&"dev-b".parse().unwrap()), Some(None));
    assert_eq!(summary.creds_of(&"dev-c".parse().unwrap()), None);
    assert_eq!(Summary::load(&dir.join("none.toml")).unwrap(), Summary::default());
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

const TEMPLATE: &str = r#"
        nats = "nats://hub:4222"

        [[ant_template]]
        name = "worker"
        managers = ["lead"]
        max_ants = 2
        workdirs = [".", "src"]
        harnesses = ["copilot", "claude"]
        models = { "gpt-5" = "high" }
        max_daily_cost = 10.0
        pool_daily_cost = 15.0
        allow = ["read"]
        allow_sets = { build = ["execute:cargo build*"] }
        env = { A = "b" }

        [[ant]]
        name = "lead"
        workdir = "."
    "#;

fn with_ants(entries: &str) -> String {
    format!("{TEMPLATE}\n{entries}")
}

#[test]
fn an_ant_added_from_a_template_takes_its_settings_and_shares_its_pool() {
    let text = with_ants(
        r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            creds = "a.creds"
            model = "gpt-5"
            reasoning_effort = "medium"
            allow_sets = ["build"]
            max_daily_cost = 4.0

            [[ant]]
            name = "lead-b"
            template = "worker"
            added_by = "lead"
            workdir = "src"
            model = "gpt-5"
        "#,
    );
    let loaded = parse_all(&text).unwrap();
    let a = loaded.ants.iter().find(|a| a.name.as_str() == "lead-a").unwrap();
    assert_eq!(
        (a.workdir.clone(), a.creds.clone()),
        (manifest_dir().canonicalize().unwrap(), Some(manifest_dir().join("a.creds")))
    );
    assert_eq!(
        (a.harness.as_deref(), a.model.as_deref(), a.reasoning_effort.as_deref()),
        (Some("copilot"), Some("gpt-5"), Some("medium"))
    );
    assert_eq!(
        (a.allow.clone(), a.env.len(), a.max_daily_cost),
        (vec!["read".to_owned(), "execute:cargo build*".to_owned()], 1, Some(4.0))
    );
    assert_eq!((a.escalate_to.clone(), a.nats.as_deref()), (vec!["lead".parse().unwrap()], Some("nats://hub:4222")));
    let pool = a.pool.clone().unwrap();
    assert_eq!((pool.template.as_str(), pool.daily_cost, pool.daily_tokens), ("worker", Some(15.0), None));
    assert_eq!(pool.members.get(), ["lead-a".parse::<UserId>().unwrap(), "lead-b".parse().unwrap()]);
    let b = loaded.ants.iter().find(|a| a.name.as_str() == "lead-b").unwrap();
    assert_eq!((b.workdir.clone(), b.max_daily_cost), (manifest_dir().join("src").canonicalize().unwrap(), Some(10.0)));
    assert!(loaded.ants.iter().find(|a| a.name.as_str() == "lead").unwrap().pool.is_none());
    assert_eq!(config(a.clone()).unwrap().spending.pool, Some(pool));

    let summary = Summary::parse(&text, &manifest_dir().join("config.toml"), Path::new("/home")).unwrap();
    assert_eq!(summary.caps[&"lead-b".parse::<UserId>().unwrap()].daily_cost, Some(10.0), "the template's ceiling");
}

#[test]
fn an_ant_added_from_a_template_is_checked_against_it_on_every_load() {
    let entry = r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"
            max_daily_cost = 8.0
        "#;
    parse_all(&with_ants(entry)).unwrap();
    let tightened = with_ants(entry).replace("max_daily_cost = 10.0", "max_daily_cost = 5.0");
    let err = format!("{:#}", parse_all(&tightened).unwrap_err());
    assert!(err.contains("ant lead-a: max_daily_cost 8 is above 5, the ceiling of template worker"), "{err}");

    for (change, error) in [
        (("model = \"gpt-5\"", "model = \"gpt-5\"\nenv = { B = \"c\" }"), "ant lead-a: env comes from template worker"),
        (("model = \"gpt-5\"", "model = \"gpt-5\"\nreasoning_effort = \"max\""), "reasoning_effort max is above high"),
        (("template = \"worker\"", "template = \"other\""), "ant lead-a: no [[ant_template]] named other"),
        (("added_by = \"lead\"", ""), "added_by must name the manager ant"),
        (("added_by = \"lead\"", "added_by = \"boss\""), "name lead-a does not fit name_pattern"),
        (("template = \"worker\"", ""), "added_by and allow_sets are for an ant added from a template"),
    ] {
        let err = format!("{:#}", parse_all(&with_ants(&entry.replace(change.0, change.1))).unwrap_err());
        assert!(err.contains(error), "{err}");
    }
    let err =
        format!("{:#}", parse_all(&with_ants(entry).replace("max_ants = 2", "max_ants = 2\nmax_ant = 3")).unwrap_err());
    assert!(err.contains("unknown field `max_ant`"), "{err}");
    let err = format!("{:#}", parse_all(&with_ants(entry).replace("max_ants = 2", "max_ants = 0")).unwrap_err());
    assert!(err.contains("ant template worker: 1 ants are added from it, more than its max_ants of 0"), "{err}");
    let err = format!("{:#}", parse_here("[[ant]]\nname = \"dev-a\"\n").unwrap_err());
    assert!(err.contains("ant dev-a: workdir is missing"), "{err}");
}

#[test]
fn an_ant_added_from_a_template_takes_its_fallbacks_less_its_own_harness_and_model() {
    let entries = r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"

            [[ant]]
            name = "lead-b"
            template = "worker"
            added_by = "lead"
            harness = "claude"
            model = "gpt-5"
        "#;
    let text = with_ants(entries).replace("env = {", "fallback = [\"copilot\", \":gpt-5\", \"claude:gpt-5\"]\nenv = {");
    let loaded = parse_all(&text).unwrap();
    let names = |ant: &str| {
        let ant = loaded.ants.iter().find(|a| a.name.as_str() == ant).unwrap().clone();
        assert_eq!(ant.fallback, ["copilot", ":gpt-5", "claude:gpt-5"]);
        let config = ant.into_config("nats://unused", &loaded.harnesses).unwrap();
        config.harness.fallbacks.iter().map(crate::fallback::Fallback::name).collect::<Vec<_>>()
    };
    assert_eq!(names("lead-a"), ["copilot", "claude:gpt-5"], ":gpt-5 is copilot:gpt-5, the ant's own");
    assert_eq!(names("lead-b"), ["copilot"], "claude:gpt-5, its own, twice");

    let err = format!("{:#}", parse_all(&text.replace("fallback = [\"copilot\"", "fallback = [\"vim\"")).unwrap_err());
    assert!(err.contains("ant template worker: fallback \"vim\": unknown harness \"vim\""), "{err}");
    let set = with_ants(entries).replace("harness = \"claude\"", "fallback = [\"claude\"]");
    let err = format!("{:#}", parse_all(&set).unwrap_err());
    assert!(err.contains("ant lead-b: fallback comes from template worker"), "{err}");
}

#[test]
fn the_signer_reads_only_the_templates_and_checks_requests_against_its_copy() {
    let dir = ants_test_support::scratch_dir("config-signer").canonicalize().unwrap();
    std::fs::create_dir(dir.join("src")).unwrap();
    let path = dir.join("config.toml");
    // Workdirs of the managers' machines need not exist on the signer's, nor harnesses be defined.
    let text = with_ants("[[ant]]\nname = \"lead-a\"\ntemplate = \"worker\"\nadded_by = \"lead\"\nmodel = \"gpt-5\"\n")
        .replace("workdirs = [\".\", \"src\"]", "workdirs = [\"src\", \"elsewhere\"]")
        .replace("harnesses = [\"copilot\", \"claude\"]", "harnesses = [\"other\"]");
    std::fs::write(&path, &text).unwrap();
    let templates = signer_templates(&path).unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].workdirs, [dir.join("src"), dir.join("elsewhere")]);
    let (lead, name): (UserId, UserId) = ("lead".parse().unwrap(), "lead-b".parse().unwrap());
    let request: Request = serde_json::from_str(r#"{"model": "gpt-5", "prompt_timeout": "10m"}"#).unwrap();
    let same = |dir: &Path| Ok(dir.to_owned());
    let ant = templates[0].resolve_with(&lead, &name, &request, same).unwrap();
    assert_eq!((ant.workdir, ant.prompt_timeout), (dir.join("src"), Duration::from_mins(10)));
    let elsewhere = Request { workdir: Some(dir.join("elsewhere")), ..request.clone() };
    templates[0].resolve_with(&lead, &name, &elsewhere, same).unwrap();
    let back: Request = serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
    assert_eq!(back, request, "a request goes over the wire as JSON");

    assert_eq!(template_of(&path, &"lead-a".parse().unwrap()).unwrap().as_deref(), Some("worker"));
    assert_eq!(template_of(&path, &lead).unwrap(), None);
    assert!(template_of(&path, &name).is_err());
    std::fs::write(&path, format!("{text}\n{}", TEMPLATE.split("[[ant]]").next().unwrap().replace("nats =", "x =")))
        .unwrap();
    let err = format!("{:#}", signer_templates(&path).unwrap_err());
    assert!(err.contains("ant template worker is listed twice"), "{err}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn added_ants_never_add_ants() {
    let entry = r#"
            [[ant]]
            name = "lead-a"
            template = "worker"
            added_by = "lead"
            model = "gpt-5"
        "#;
    let as_manager = with_ants(entry).replace("managers = [\"lead\"]", "managers = [\"lead\", \"lead-a\"]");
    let err = format!("{:#}", parse_all(&as_manager).unwrap_err());
    assert!(err.contains("manager lead-a was added from a template; added ants never add ants"), "{err}");
    let nested = format!(
        "{}\n{}",
        with_ants(entry),
        entry
            .replace("name = \"lead-a\"", "name = \"lead-a-x\"")
            .replace("added_by = \"lead\"", "added_by = \"lead-a\"")
    );
    let err = format!("{:#}", parse_all(&nested).unwrap_err());
    assert!(err.contains("added_by names lead-a, itself added from a template"), "{err}");
}

#[test]
fn a_request_is_checked_and_its_entry_loads_as_the_ant_checked() {
    let loaded = parse_all(&with_ants("")).unwrap();
    let (lead, name): (UserId, UserId) = ("lead".parse().unwrap(), "lead-docs".parse().unwrap());
    let request = Request {
        workdir: Some("src".into()),
        model: Some("gpt-5".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        harness: Some("claude".to_owned()),
        allow_sets: vec!["build".to_owned()],
        capabilities: Some(vec!["docs".to_owned()]),
        whoami: Some("Write \"docs\".".to_owned()),
        max_daily_cost: Some(2.5),
        max_thread_tokens: Some(500),
        max_harnesses: Some(1),
        prompt_timeout: Some(Duration::from_mins(10)),
        max_tool_calls: Some(40),
        ..Request::default()
    };
    let checked = loaded.check_request(&lead, &name, "worker", &request).unwrap();
    let entry = template_entry(&name, "worker", &lead, Some(Path::new("docs.creds")), &request).unwrap();
    let reloaded = parse_all(&with_ants(&entry)).unwrap();
    let ant = reloaded.ants.into_iter().find(|a| a.name == name).unwrap();
    assert_eq!(
        ant,
        Ant {
            creds: Some(manifest_dir().join("docs.creds")),
            nats: Some("nats://hub:4222".to_owned()),
            pool: ant.pool.clone(),
            ..checked
        }
    );

    for (manager, name, template, error) in [
        ("lead", "lead-docs", "other", "no ant template other (the config file has: worker)"),
        ("boss", "boss-docs", "worker", "boss is not among the managers of template worker"),
        ("lead", "lead", "worker", "the config file already lists lead"),
        ("lead", "dev-docs", "worker", "does not fit name_pattern"),
    ] {
        let err =
            loaded.check_request(&manager.parse().unwrap(), &name.parse().unwrap(), template, &request).unwrap_err();
        assert!(err.to_string().contains(error), "{err}");
    }
    let full = parse_all(&format!("{}\n{entry}\n{}", with_ants(""), entry.replace("lead-docs", "lead-more"))).unwrap();
    let err = full.check_request(&lead, &"lead-x".parse().unwrap(), "worker", &request).unwrap_err().to_string();
    assert!(err.contains("template worker has 2 ants already, its max_ants"), "{err}");
    let listed = with_ants(&entry).replace("managers = [\"lead\"]", "managers = [\"lead\", \"lead-docs\"]");
    let err = format!("{:#}", parse_all(&listed).unwrap_err());
    assert!(err.contains("manager lead-docs was added from a template"), "{err}");
}
