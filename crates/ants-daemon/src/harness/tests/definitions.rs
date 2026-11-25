//! Harness definitions: how the built-in and custom ones launch, their history budgets, what is checked of them,
//! and their variants asking about tools and for reviews.

use super::*;

fn command_line(harness: &Harness, env: &[(&str, &str)], context: Option<&str>) -> (String, Vec<String>, Vec<String>) {
    let env: Vec<_> = env.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
    let settings = ModelSettings { context: context.map(str::to_owned), ..Default::default() };
    let launched = harness.launch_env(&env).iter().map(|(k, v)| format!("{k}={v}")).collect();
    (harness.executable(&env).display().to_string(), harness.launch_args(&settings), launched)
}

#[test]
fn builtin_harnesses_launch_in_their_most_permissive_setting() {
    let strings = |items: &[&str]| items.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let cases = [
        ("copilot", None, "copilot", &["--acp", "--allow-all"][..], None),
        ("copilot", Some("long_context"), "copilot", &["--acp", "--allow-all", "--context", "long_context"][..], None),
        ("copilot", Some("default"), "copilot", &["--acp", "--allow-all", "--context", "default"][..], None),
        ("claude", Some("1m"), "claude-agent-acp", &[][..], Some("bypassPermissions")),
        ("codex", None, "codex-acp", &[][..], Some("agent-full-access")),
        ("antigravity", None, "agy", &["acp", "--disable-telemetry", "--dangerously-skip-permissions"][..], None),
    ];
    for (name, context, command, args, mode) in cases {
        let harness = builtin(name);
        assert_eq!(command_line(&harness, &[], context), (command.to_owned(), strings(args), Vec::new()), "{name}");
        assert_eq!(harness.mode.as_deref(), mode, "{name}");
        assert!(harness.bypasses_permissions(), "{name}");
    }
    assert_eq!(builtin("codex").sign_in(), Some("codex-acp login"));
    let claude =
        ModelSettings { model: Some("smart".to_owned()), context: Some("1m".to_owned()), ..Default::default() };
    assert_eq!(claude.model_id(&builtin("claude")).as_deref(), Some("smart[1m]"));
    assert_eq!(claude.model_id(&builtin("copilot")).as_deref(), Some("smart"));
}

#[test]
fn a_custom_adapter_launches_with_its_arguments_and_environment() {
    let harness = custom(
        "command = \"/opt/acp/bin/my-adapter\"\nargs = [\"--stdio\", \"--log=warn\"]\n\
             env = { ADAPTER_HOME = \"/opt/acp\", SHARED = \"definition\" }\n\
             [harnesses.custom.context]\nwide = { args = [\"--window\", \"wide\"], model_suffix = \"-wide\" }",
    );
    let (command, args, env) = command_line(&harness, &[("SHARED", "ant"), ("OWN", "1")], None);
    assert_eq!(command, "/opt/acp/bin/my-adapter");
    assert_eq!(args, ["--stdio", "--log=warn"]);
    assert_eq!(env, ["ADAPTER_HOME=/opt/acp", "OWN=1", "SHARED=ant"], "the ant's env wins over the definition's");
    assert_eq!(command_line(&harness, &[], Some("wide")).1, ["--stdio", "--log=warn", "--window", "wide"]);

    let wide = |model: Option<&str>| ModelSettings {
        model: model.map(str::to_owned),
        context: Some("wide".to_owned()),
        ..Default::default()
    };
    assert_eq!(wide(Some("m")).model_id(&harness).as_deref(), Some("m-wide"));
    let err = wide(None).validate(&harness).unwrap_err().to_string();
    assert!(err.contains("harness custom: context wide requires an explicit model"), "{err}");
    let narrow = ModelSettings { context: Some("narrow".to_owned()), ..Default::default() };
    assert!(narrow.validate(&harness).unwrap_err().to_string().contains("context must be wide"));
    let plain = custom("command = \"my-adapter\"");
    let err = wide(Some("m")).validate(&plain).unwrap_err().to_string();
    assert!(err.contains("harness custom has no context setting"), "{err}");
    assert!(ModelSettings::default().validate(&plain).is_ok());
}

#[test]
fn the_history_budget_follows_the_ant_then_its_context_then_the_harness() {
    let context = |value: &str| ModelSettings { context: Some(value.to_owned()), ..Default::default() };
    let own = |budget| ModelSettings { history_budget: Some(budget), ..context("1m") };
    let claude = builtin("claude");
    assert_eq!(builtin("copilot").history_budget(&ModelSettings::default()), crate::stream::history::DEFAULT_BUDGET);
    assert_eq!(claude.history_budget(&context("default")), 66_666, "from the harness's window");
    assert_eq!(claude.history_budget(&context("1m")), 333_333, "from the 1m context's window");
    assert_eq!(claude.history_budget(&own(4096)), 4096);
    assert_eq!(claude.history_budget(&own(usize::MAX)), 1_000_000, "a quarter of the 1m window at most");

    let harness = custom(
        "command = \"x\"\nhistory_budget = 1000\n[harnesses.custom.context]\n\
             plain = {}\nwide = { window = 120000 }\nset = { history_budget = 3000 }",
    );
    assert_eq!(harness.history_budget(&ModelSettings::default()), 1000);
    assert_eq!(harness.history_budget(&context("plain")), 1000);
    assert_eq!(harness.history_budget(&context("wide")), 40_000, "the context's window over the harness's budget");
    assert_eq!(harness.history_budget(&context("set")), 3000);
}

#[test]
fn definitions_are_checked_and_replace_builtins_whole() {
    let define = |text: &str| parse_definitions(text).and_then(|defs| Harnesses::builtin().define(defs));
    let err = |text: &str| format!("{:#}", define(text).unwrap_err());
    assert!(err("[harnesses.custom]\ncommand = \"\"").contains("harness custom: command must not be empty"));
    assert!(err("[harnesses.custom]\nargs = [\"x\"]").contains("missing field `command`"));
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nshell = true").contains("unknown field `shell`"));
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nargs = [\"a\\u0000\"]").contains("must not contain NUL"));
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nmode = \" \"").contains("mode must not be blank"));
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nsign_in = \"\"").contains("sign_in must not be blank"));
    let entra = |protocol: &str, id: &str| {
        err(&format!("[harnesses.custom]\ncommand = \"x\"\nprotocol = \"{protocol}\"\nentra_client_id = \"{id}\""))
    };
    assert!(entra("copilot-sdk", " ").contains("harness custom: entra_client_id must not be blank"));
    let elsewhere = entra("acp", "00000000-1111-2222-3333-444444444444");
    assert!(elsewhere.contains("harness custom: entra_client_id applies only to protocol copilot-sdk, not acp"));
    let sign_in = |protocol: &str, names: &str| {
        err(&format!("[harnesses.custom]\ncommand = \"x\"\nprotocol = \"{protocol}\"\nmcp_sign_in = {names}"))
    };
    assert!(sign_in("acp", r#"["icm"]"#).contains("harness custom: mcp_sign_in applies only to protocol copilot-sdk"));
    assert!(sign_in("copilot-sdk", r#"["icm", ""]"#).contains("harness custom: mcp_sign_in names must not be blank"));
    assert!(sign_in("copilot-sdk", r#"["icm", "icm"]"#).contains("harness custom: mcp_sign_in names icm twice"));
    assert_eq!(builtin("copilot").sign_in(), Some("copilot login"));
    let env = err("[harnesses.custom]\ncommand = \"x\"\nenv = { 'A=B' = \"secret\" }");
    assert!(env.contains("harness custom") && env.contains("invalid harness environment variable"), "{env}");
    assert!(!env.contains("secret"), "{env}");
    assert!(err("[harnesses.\"My Agent\"]\ncommand = \"x\"").contains("use lowercase letters"));
    let suffix = err("[harnesses.custom]\ncommand = \"x\"\ncontext = { big = { model_suffix = \"\" } }");
    assert!(suffix.contains("model_suffix must not be empty"), "{suffix}");
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nwindow = 0").contains("window must be above 0"));

    let harnesses = define("[harnesses.copilot]\ncommand = \"/usr/local/bin/copilot-acp\"").unwrap();
    let copilot = harnesses.get("copilot").unwrap();
    assert_eq!((copilot.name(), copilot.args.is_empty(), copilot.context.is_empty()), ("copilot", true, true));
    assert_eq!(harnesses.get("claude").unwrap(), builtin("claude"));
    let unknown = harnesses.get("vim").unwrap_err().to_string();
    assert!(
        unknown.contains("unknown harness \"vim\", expected one of antigravity, claude, codex, copilot"),
        "{unknown}"
    );
    assert!(!copilot.bypasses_permissions(), "a definition replacing a built-in sets its own");
}

#[test]
fn a_harness_running_its_tools_unasked_tells_which_gating_misses_them() {
    let rules = |rules: &[&str]| rules.iter().map(|rule| (*rule).to_owned()).collect::<Vec<_>>();
    let copilot = builtin("copilot");
    assert_eq!(copilot.unenforced(&[], false), None, "nothing gated");
    assert_eq!(copilot.unenforced(&rules(&["read", "*"]), false), None, "everything allowed anyway");
    assert_eq!(
        copilot.unenforced(&rules(&["read"]), true).as_deref(),
        Some(
            "harness copilot runs its tools unasked, so its allow rules and tool approvals by its escalate_to ants do \
             not apply to them"
        )
    );
    let approvals = copilot.unenforced(&[], true).unwrap();
    assert!(approvals.contains("so tool approvals by its escalate_to ants do not apply"), "{approvals}");
    let asking = parse_definitions("[harnesses.copilot]\ncommand = \"copilot\"\nargs = [\"--acp\"]").unwrap();
    let asking = Harnesses::builtin().define(asking).unwrap();
    assert_eq!(asking.get("copilot").unwrap().unenforced(&rules(&["read"]), true), None);
}

#[test]
fn a_harness_asking_ants_about_its_tools_runs_its_ask_variant() {
    let asking = |harness: &Harness| harness.with_tools(Some(Tools::Ask)).map(|h| (h.args, h.mode));
    let copilot = builtin("copilot").with_tools(Some(Tools::Ask)).unwrap();
    assert!(!copilot.bypasses_permissions());
    assert_eq!(copilot.tools(), Tools::Ask);
    assert_eq!(copilot.unenforced(&["read".to_owned()], true), None);
    assert_eq!(command_line(&copilot, &[], Some("default")).1, ["--acp", "--context", "default"]);
    assert_eq!(asking(&builtin("claude")).unwrap(), (Vec::new(), Some("auto".to_owned())));
    assert_eq!(asking(&builtin("codex")).unwrap(), (Vec::new(), Some("agent".to_owned())));
    assert_eq!(builtin("codex").with_tools(None).unwrap(), *builtin("codex"), "auto-approve by default");
    let refused = format!("{:#}", builtin("antigravity").with_tools(Some(Tools::Ask)).unwrap_err());
    assert!(refused.contains("harness antigravity runs its tools unasked and its definition sets no ask"), "{refused}");

    let gated = custom("command = \"x\"\nargs = [\"--a\"]");
    assert_eq!(asking(&gated).unwrap(), (vec!["--a".to_owned()], None), "itself");
    let unasked = custom("command = \"x\"\nbypass_permissions = true\ntools = \"ask\"\nask = { mode = \"m\" }");
    assert_eq!(unasked.with_tools(Some(Tools::AutoApprove)).unwrap().mode, None, "an ant's auto-approve wins");
    assert_eq!(unasked.with_tools(None).unwrap().mode.as_deref(), Some("m"), "the definition's ask");

    let define = |text: &str| parse_definitions(text).and_then(|defs| Harnesses::builtin().define(defs));
    let err = |text: &str| format!("{:#}", define(text).unwrap_err());
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nask = {}").contains("ask must set args or mode"));
    let unasking = err("[harnesses.antigravity]\ncommand = \"agy\"\nbypass_permissions = true\ntools = \"ask\"");
    assert!(unasking.contains("tools = \"ask\" does not apply to it"), "{unasking}");
    let tools = err("[harnesses.custom]\ncommand = \"x\"\ntools = \"sometimes\"");
    assert!(tools.contains("auto-approve") && tools.contains("ask"), "{tools}");
}

#[test]
fn reviews_run_on_a_variant_asking_before_running_tools() {
    let reviewing = |harness: &Harness| harness.reviewing().map(|h| (h.args, h.mode));
    let strings = |items: &[&str]| items.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let copilot = builtin("copilot").reviewing().unwrap();
    assert!(!copilot.bypasses_permissions());
    assert_eq!(command_line(&copilot, &[], Some("default")).1, ["--acp", "--context", "default"]);
    assert_eq!(reviewing(&builtin("claude")), Some((Vec::new(), Some("default".to_owned()))));
    assert_eq!(reviewing(&builtin("codex")), Some((Vec::new(), Some("read-only".to_owned()))));
    assert_eq!(reviewing(&builtin("antigravity")), None);
    let unheld = builtin("antigravity").unheld_review().unwrap();
    assert!(unheld.contains("harness antigravity runs its tools unasked and its definition sets no review"));
    assert_eq!(builtin("copilot").unheld_review(), None);

    let asking = custom("command = \"x\"\nargs = [\"--a\"]");
    assert_eq!(reviewing(&asking), Some((strings(&["--a"]), None)), "itself");
    assert_eq!(custom("command = \"x\"\nbypass_permissions = true").reviewing(), None);
    let native = custom("command = \"x\"\nprotocol = \"codex\"\nreview = { mode = \"read-only\" }");
    assert_eq!(reviewing(&native), Some((Vec::new(), Some("read-only".to_owned()))));

    let define = |text: &str| parse_definitions(text).and_then(|defs| Harnesses::builtin().define(defs));
    let err = |text: &str| format!("{:#}", define(text).unwrap_err());
    assert!(err("[harnesses.custom]\ncommand = \"x\"\nreview = {}").contains("review must set args or mode"));
    let blank = err("[harnesses.custom]\ncommand = \"x\"\nreview = { mode = \" \" }");
    assert!(blank.contains("harness custom: review: harness custom: mode must not be blank"), "{blank}");
    let replaced =
        "[harnesses.copilot]\ncommand = \"copilot\"\nargs = [\"--acp\", \"--allow-all\"]\nbypass_permissions = true";
    let replaced = define(replaced).unwrap();
    assert_eq!(replaced.get("copilot").unwrap().reviewing(), None, "a definition replacing a built-in sets its own");
}

#[test]
fn a_cost_cap_on_protocol_codex_is_warned_of_as_tripping_only_signed_in_with_chatgpt() {
    let cost = [("max_daily_cost", Reported::Cost)];
    let native = custom("command = \"codex\"\nprotocol = \"codex\"");
    assert_eq!(
        native.uncounted(&cost).as_deref(),
        Some(
            "harness custom reports cost only signed in with ChatGPT, so max_daily_cost never trips on it signed in \
             otherwise"
        )
    );
    let fix = native.uncounted_fix();
    assert!(
        fix.starts_with(
            "signed in with ChatGPT, set reports = [\"tokens\", \"cost\"] in [harnesses.custom]; otherwise set caps"
        ),
        "{fix}"
    );
    let both = [("max_daily_tokens", Reported::Tokens), ("max_daily_cost", Reported::Cost)];
    let none = custom("command = \"codex\"\nprotocol = \"codex\"\nreports = []");
    assert_eq!(
        none.uncounted(&both).as_deref(),
        Some(
            "harness custom reports no tokens and cost only signed in with ChatGPT, so max_daily_tokens never trips \
             on it and max_daily_cost never trips on it signed in otherwise"
        )
    );
    assert!(none.uncounted_fix().starts_with("signed in with ChatGPT, set reports = [\"cost\"]"));
    let chatgpt = custom("command = \"codex\"\nprotocol = \"codex\"\nreports = [\"tokens\", \"cost\"]");
    assert_eq!(chatgpt.uncounted(&both), None, "a definition saying it reports a cost is taken at its word");
    assert!(chatgpt.uncounted_fix().starts_with("set caps of what harness custom reports"));
    assert_eq!(
        builtin("codex").uncounted(&cost).as_deref(),
        Some("harness codex reports no cost, so max_daily_cost never trips on it"),
        "codex-acp reports no cost with any sign-in"
    );
}
