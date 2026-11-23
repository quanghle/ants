//! Templates and the ants added from them: the settings an added ant takes, the checks on every load, and the
//! requests the signer checks against its copy.

use super::*;

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
        config.harness.fallbacks.iter().map(crate::limits::fallback::Fallback::name).collect::<Vec<_>>()
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
fn an_ants_tools_override_its_harness_and_fallbacks() {
    let ant = |tools: &str, fallback: &str| {
        let text = format!(
            "[[ant]]\nname = \"dev-a\"\nworkdir = \".\"\nharness = \"claude\"\n{tools}\nfallback = [{fallback}]"
        );
        let loaded = parse_all(&text)?;
        loaded.ants[0].clone().into_config("nats://unused", &loaded.harnesses)
    };
    let config = ant("tools = \"ask\"", "\"codex\"").unwrap();
    assert_eq!(config.harness.harness.mode.as_deref(), Some("auto"));
    assert_eq!(config.harness.fallbacks[0].harness.mode.as_deref(), Some("agent"));
    assert!(!config.harness.fallbacks[0].harness.bypasses_permissions());
    let config = ant("", "\"codex\"").unwrap();
    assert_eq!(config.harness.harness.mode.as_deref(), Some("bypassPermissions"), "auto-approve by default");

    let err = format!("{:#}", ant("tools = \"ask\"", "\"antigravity\"").unwrap_err());
    assert!(err.contains("ant dev-a: fallback \"antigravity\": harness antigravity runs its tools unasked"), "{err}");
    let err = format!("{:#}", ant("tools = \"never\"", "").unwrap_err());
    assert!(err.contains("tools"), "{err}");
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
