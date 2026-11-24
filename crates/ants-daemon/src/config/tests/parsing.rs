//! Parsing the file: several ants with their settings and defaults, the top-level keys every ant takes, and the
//! files and values refused.

use super::*;

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
    let chain = Chain {
        escalate_to,
        max_hops: 2,
        turn_budget: ants_proto::DEFAULT_TURN_BUDGET,
        after: None,
        remind: None,
        stand_in: Some("carol".parse().unwrap()),
    };
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
    assert!(config.storage.home.ends_with(".ants/agents/dev-b"));
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
fn a_managed_nats_server_needs_a_configuration_and_resolves_its_paths() {
    assert_eq!(parse_all("").unwrap().nats_server, None);
    let dir = manifest_dir();
    let home = dir.parent().unwrap().parent().unwrap();
    let managed = parse_all("[nats_server]\nconfig = \"~/leaf.conf\"").unwrap().nats_server.unwrap();
    assert_eq!(
        managed,
        NatsServer {
            command: "nats-server".into(),
            config: home.join("leaf.conf"),
            ready_timeout: DEFAULT_READY_TIMEOUT
        }
    );
    let text = "[nats_server]\nconfig = \"nats/leaf.conf\"\ncommand = \"bin/nats-server\"\nready_timeout = \"5s\"";
    let managed = parse_all(text).unwrap().nats_server.unwrap();
    assert_eq!(managed.command, dir.join("bin/nats-server"));
    assert_eq!(managed.config, dir.join("nats/leaf.conf"));
    assert_eq!(managed.ready_timeout, Duration::from_secs(5));
    for bad in [
        "[nats_server]\ncommand = \"nats-server\"",
        "[nats_server]\nconfig = \"\"",
        "[nats_server]\nconfig = \"a\"\nready_timeout = \"0s\"",
        "[nats_server]\nconfig = \"a\"\nport = 4222",
    ] {
        let err = format!("{:#}", parse_all(bad).unwrap_err());
        assert!(err.contains("nats_server") || err.contains("config") || err.contains("port"), "{bad}: {err}");
    }
}

#[test]
fn auto_update_is_off_unless_set() {
    assert_eq!(parse_all("").unwrap().auto_update, AutoUpdate::Off);
    assert_eq!(parse_all("auto_update = \"check\"").unwrap().auto_update, AutoUpdate::Check);
    assert_eq!(parse_all("auto_update = \"install\"").unwrap().auto_update, AutoUpdate::Install);
    assert!(parse_all("auto_update = \"always\"").is_err());
}

#[test]
fn redaction_is_on_unless_turned_off() {
    let ant = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"";
    let redact = |text: &str| parse_here(text).unwrap().iter().map(|ant| ant.redact).collect::<Vec<_>>();
    assert_eq!(redact(ant), [true, true, true]);
    assert_eq!(redact(&format!("redact = false\n{ant}")), [false, false, false], "the messenger and the sage too");
}

#[test]
fn names_the_machine_as_the_file_does() {
    let ant = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"";
    let machines = |text: &str| parse_here(text).unwrap().into_iter().map(|ant| ant.machine).collect::<Vec<_>>();
    assert_eq!(machines(ant), [None, None, None], "the host name");
    let named = machines(&format!("machine = \"dev-vm1\"\n{ant}"));
    assert_eq!(named, vec![Some("dev-vm1".to_owned()); 3], "the messenger and the sage too");
    let mut ants = parse_here(&format!("machine = \"dev-vm1\"\n{ant}")).unwrap();
    assert_eq!(config(ants.remove(0)).unwrap().presence.machine.as_deref(), Some("dev-vm1"));
    let long = "m".repeat(65);
    for (machine, why) in [("\" \"", "blank"), (&format!("\"{long}\""), "longer than 64"), ("\"vm\\n1\"", "control")] {
        let err = format!("{:#}", parse_here(&format!("machine = {machine}\n{ant}")).unwrap_err());
        assert!(err.contains("machine") && err.contains(why), "{err}");
    }
}

#[test]
fn ants_are_served_by_sense_unless_the_file_or_the_ant_names_another_provider() {
    use crate::provider::{Protocol, Setting};

    let ants = "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n[[ant]]\nname = \"dev-b\"\nworkdir = \".\"";
    let providers = "[providers.memo]\ncommand = \"bin/memo\"\nargs = [\"--stdio\"]\nenv = { MEMO_DB = \"~/memo.db\" }\n\
                     [providers.github]\ncommand = \"github-mcp-server\"\nprotocol = \"mcp\"";
    let settings = |text: &str| -> Vec<Setting> {
        parse_here(text).unwrap().into_iter().map(|ant| config(ant).unwrap().provider).collect()
    };
    assert_eq!(settings(ants), vec![Setting::Sense; 4], "the messenger and the sage too");

    let named = settings(&format!("provider = \"memo\"\n{ants}\nprovider = \"none\"\n{providers}"));
    let Setting::External(memo) = &named[0] else { panic!("{named:?}") };
    assert_eq!(
        (named[1].clone(), named[2].name()),
        (Setting::None, "memo"),
        "dev-b's own, and the messenger the file's"
    );
    assert_eq!((memo.name.as_str(), memo.command.as_path()), ("memo", manifest_dir().join("bin/memo").as_path()));
    assert_eq!((memo.args.as_slice(), memo.env["MEMO_DB"].as_str()), (["--stdio".to_owned()].as_slice(), "~/memo.db"));
    assert_eq!(memo.protocol, Protocol::Provider);
    let github = settings(&format!("provider = \"github\"\n{ants}\n{providers}")).remove(0);
    let Setting::External(github) = github else { panic!("{github:?}") };
    assert_eq!((github.command.as_path(), github.protocol), (Path::new("github-mcp-server"), Protocol::Mcp));

    for (text, why) in [
        (format!("provider = \"other\"\n{ants}\n{providers}"), "provider \"other\" is not defined"),
        (format!("{ants}\nprovider = \"other\""), "ant dev-b: provider \"other\" is not defined"),
        ("[providers.sense]\ncommand = \"x\"".to_owned(), "reserved"),
        ("[providers.send]\ncommand = \"x\"".to_owned(), "read like ants' own send_"),
        ("[providers.x]\ncommand = \"x\"\nprotocol = \"grpc\"".to_owned(), "unknown variant"),
        ("[providers.x]\nargs = []".to_owned(), "missing field `command`"),
    ] {
        let err = format!("{:#}", parse_here(&text).unwrap_err());
        assert!(err.contains(why), "{why}: {err}");
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
fn rejects_bad_files() {
    let err = |text: &str| format!("{:#}", parse_here(text).unwrap_err());
    let twice = err("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n[[ant]]\nname = \"dev-a\"\nworkdir = \".\"");
    assert!(twice.contains("ant dev-a is listed twice"), "{twice}");
    assert!(twice.contains("config.toml"), "{twice}");
    let missing = err("[[ant]]\nname = \"dev-a\"\nworkdir = \"no/such/dir\"");
    assert!(missing.contains("workdir no/such/dir"), "{missing}");
    assert!(err("harness = \"vim\"").contains("unknown harness"));
    assert!(err("provider = \"copilot\"").contains("provider \"copilot\" is not defined"));
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
fn durations_need_a_unit() {
    let idle = |line: &str| parse_here(&format!("[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\n{line}"));
    assert_eq!(idle("idle_timeout = \"10m\"").unwrap()[0].idle_timeout, Duration::from_secs(600));
    for bad in ["idle_timeout_secs = 600", "idle_timeout = 600", "idle_timeout = \"600\""] {
        assert!(idle(bad).is_err(), "{bad} loaded");
    }
    let err = format!("{:#}", idle("escalate_after = \"30\"").unwrap_err());
    assert!(err.contains("a number without a unit"), "{err}");
}
