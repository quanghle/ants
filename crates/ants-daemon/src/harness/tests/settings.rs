//! The ant's model, reasoning effort, context and skills are applied to new and resumed sessions.

use super::*;

#[derive(Clone, Copy)]
enum ConfigBehavior {
    Apply,
    NoOptions,
    Ignore,
    ResetModel,
}

type ConfigCalls = Arc<Mutex<Vec<(String, String)>>>;

fn model_options(model: &str, effort: &str) -> Vec<SessionConfigOption> {
    let models =
        ["base", "smart", "smart[1m]"].into_iter().map(|value| SessionConfigSelectOption::new(value, value)).collect();
    let efforts = if model == "base" { vec!["low"] } else { vec!["medium", "high"] };
    vec![
        SessionConfigOption::select(
            "model",
            "Model",
            model.to_owned(),
            vec![SessionConfigSelectGroup::new("available", "Available models", models)],
        )
        .category(SessionConfigOptionCategory::Model),
        SessionConfigOption::select(
            "provider-effort",
            "Thinking level",
            effort.to_owned(),
            efforts.into_iter().map(|value| SessionConfigSelectOption::new(value, value)).collect::<Vec<_>>(),
        )
        .category(SessionConfigOptionCategory::ThoughtLevel),
    ]
}

fn configurable(harness: &str, settings: ModelSettings, behavior: ConfigBehavior) -> (Launch, ConfigCalls) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let log = calls.clone();
    let connect: Connector = Arc::new(move || {
        let options = if matches!(behavior, ConfigBehavior::NoOptions) { vec![] } else { model_options("base", "low") };
        let initial = options.clone();
        let loaded = options.clone();
        let state = Arc::new(Mutex::new(options));
        let log = log.clone();
        let opened = move |_| Ok(NewSessionResponse::new("s1").config_options(initial.clone()));
        DynConnectTo::new(
            agent("configurable", loading, opened)
                .on_receive_request(
                    async move |_: LoadSessionRequest, responder, _cx| {
                        responder.respond(LoadSessionResponse::new().config_options(loaded.clone()))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |request: SetSessionConfigOptionRequest, responder, _cx| {
                        let value = request.value.as_value_id().unwrap().0.to_string();
                        let config_id = request.config_id.0.as_ref();
                        log.locked().push((config_id.to_owned(), value.clone()));
                        let mut options = state.locked();
                        if !matches!(behavior, ConfigBehavior::Ignore) {
                            if config_id == "model" {
                                *options = model_options(&value, "medium");
                            } else {
                                let model = if matches!(behavior, ConfigBehavior::ResetModel) {
                                    "base".to_owned()
                                } else {
                                    let SessionConfigKind::Select(select) = &options[0].kind else { unreachable!() };
                                    select.current_value.0.to_string()
                                };
                                *options = model_options(&model, &value);
                            }
                        }
                        responder.respond(SetSessionConfigOptionResponse::new(options.clone()))
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    let mut launch = Launch::with_transport(connect, PathBuf::from("/"), Default::default());
    // The fake offers no session modes.
    launch.harness = Arc::new(Harness { mode: None, ..(*builtin(harness)).clone() });
    launch.settings = settings;
    (launch, calls)
}

#[tokio::test]
async fn model_then_effort_are_applied_to_new_and_resumed_sessions() {
    let settings = ModelSettings {
        model: Some("smart".to_owned()),
        reasoning_effort: Some("high".to_owned()),
        ..Default::default()
    };
    for harness in ["copilot", "claude", "antigravity"] {
        let (launch, calls) = configurable(harness, settings.clone(), ConfigBehavior::Apply);
        Session::start(&launch, None).await.unwrap().close().await;
        Session::start(&launch, Some("s1".into())).await.unwrap().close().await;
        let calls = calls.locked();
        assert_eq!(
            calls.as_slice(),
            [
                ("model".to_owned(), "smart".to_owned()),
                ("provider-effort".to_owned(), "high".to_owned()),
                ("model".to_owned(), "smart".to_owned()),
                ("provider-effort".to_owned(), "high".to_owned()),
            ]
        );
    }
}

#[tokio::test]
async fn explicit_settings_never_silently_fall_back() {
    for (behavior, model, effort, expected) in [
        (ConfigBehavior::NoOptions, "smart", None, "does not expose model"),
        (ConfigBehavior::Apply, "missing", None, "unsupported model"),
        (ConfigBehavior::Apply, "smart", Some("max"), "unsupported reasoning_effort"),
        (ConfigBehavior::Ignore, "smart", None, "did not apply model"),
        (ConfigBehavior::ResetModel, "smart", Some("high"), "did not apply model"),
    ] {
        let settings = ModelSettings {
            model: Some(model.to_owned()),
            reasoning_effort: effort.map(str::to_owned),
            ..Default::default()
        };
        let (launch, _) = configurable("copilot", settings, behavior);
        for resume in [None, Some("s1".into())] {
            let err = Session::start(&launch, resume).await.err().expect("settings should fail").to_string();
            assert!(err.contains(expected), "{err}");
        }
    }
}

#[tokio::test]
async fn claude_context_selects_only_an_advertised_model_variant() {
    for (model, expected) in [("smart", Some("smart[1m]")), ("smart[1m]", Some("smart[1m]")), ("base", None)] {
        let settings =
            ModelSettings { model: Some(model.to_owned()), context: Some("1m".to_owned()), ..Default::default() };
        let (launch, calls) = configurable("claude", settings, ConfigBehavior::Apply);
        let session = Session::start(&launch, None).await;
        if let Some(expected) = expected {
            session.unwrap().close().await;
            assert_eq!(calls.locked()[0], ("model".to_owned(), expected.to_owned()));
        } else {
            let err = session.err().expect("unavailable context variant must fail").to_string();
            assert!(err.contains("unsupported model") && err.contains("base[1m]"), "{err}");
            assert!(calls.locked().is_empty());
        }
    }
}

#[tokio::test]
async fn sessions_are_offered_the_ants_skills_once_one_is_confirmed() {
    let metas = Arc::new(Mutex::new(Vec::new()));
    let log = metas.clone();
    let connect: Connector = Arc::new(move || {
        let (new, load) = (log.clone(), log.clone());
        let opened = move |request: NewSessionRequest| {
            new.locked().push(request.meta);
            Ok(NewSessionResponse::new("s1"))
        };
        DynConnectTo::new(agent("skilled", loading, opened).on_receive_request(
            async move |request: LoadSessionRequest, responder, _cx| {
                load.locked().push(request.meta);
                responder.respond(LoadSessionResponse::new())
            },
            agent_client_protocol::on_receive_request!(),
        ))
    });
    let home = ants_test_support::scratch_dir("harness-skills");
    let dir = home.join(".sense/skills");
    let skills = Skills { dir: dir.clone(), plugin: home.join(".sense") };
    let launch = Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_skills(skills);
    Session::start(&launch, None).await.unwrap().close().await;
    assert_eq!(metas.locked().pop(), Some(None), "no skill confirmed yet");

    std::fs::create_dir_all(dir.join("deploy")).unwrap();
    std::fs::write(dir.join("deploy/SKILL.md"), "---\nname: deploy\ndescription: Deploy.\n---\n").unwrap();
    Session::start(&launch, None).await.unwrap().close().await;
    Session::start(&launch, Some("s1".into())).await.unwrap().close().await;
    let plugins = json!([{ "type": "local", "path": home.join(".sense"), "skipMcpDiscovery": true }]);
    let offered = json!({ "ants/skills": dir, "claudeCode": { "options": { "plugins": plugins } } });
    let offered = offered.as_object().cloned();
    assert_eq!(*metas.locked(), [offered.clone(), offered], "to new and loaded sessions");
    std::fs::remove_dir_all(&home).unwrap();
}
