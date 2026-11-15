use std::assert_matches;

use agent_client_protocol::v1::schema::{
    AgentCapabilities, AuthMethodTerminal, ContentChunk, Implementation, InitializeResponse, LoadSessionResponse,
    NewSessionResponse, PermissionOption, PermissionOptionKind, SessionConfigSelectGroup, SessionConfigSelectOption,
    SessionMode, SetSessionConfigOptionResponse, SetSessionModeResponse, StopReason, ToolCallStatus,
    ToolCallUpdateFields, ToolKind,
};
use serde_json::json;

use super::*;
use crate::permission::Allowlist;
use crate::ratelimit::Limited;
use ants_proto::LimitKind;

/// A fake harness starting its sessions in `auto`, offering `modes`; returns the modes it was switched to.
fn launch(modes: &'static [&'static str]) -> (Launch, Arc<Mutex<Vec<String>>>) {
    let set = Arc::new(Mutex::new(Vec::new()));
    let log = set.clone();
    let connect: Connector = Arc::new(move || {
        let log = log.clone();
        DynConnectTo::new(
            Agent
                .v1()
                .name("fake")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        responder.respond(InitializeResponse::new(init.protocol_version))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |_: NewSessionRequest, responder, _cx| {
                        let offered = modes.iter().map(|m| SessionMode::new(*m, *m)).collect();
                        responder.respond(NewSessionResponse::new("s1").modes(SessionModeState::new("auto", offered)))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |req: SetSessionModeRequest, responder, _cx| {
                        log.lock().unwrap().push(req.mode_id.0.to_string());
                        responder.respond(SetSessionModeResponse::new())
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    let custom = custom("command = \"custom-acp\"\nmode = \"default\"");
    (Launch::with_transport(connect, PathBuf::from("/"), Default::default()).with_harness(custom), set)
}

/// The harness a config file defines as `[harnesses.custom]` with `table`.
fn custom(table: &str) -> Arc<Harness> {
    let definitions = parse_definitions(&format!("[harnesses.custom]\n{table}")).unwrap();
    Harnesses::builtin().define(definitions).unwrap().get("custom").unwrap()
}

fn builtin(name: &str) -> Arc<Harness> {
    Harnesses::builtin().get(name).unwrap()
}

#[tokio::test]
async fn sessions_are_switched_to_the_harness_mode() {
    let (launch, set) = launch(&["auto", "default"]);
    Session::start(&launch, None).await.unwrap().close().await;
    assert_eq!(*set.lock().unwrap(), ["default"]);
}

#[tokio::test]
async fn a_harness_without_the_mode_fails_the_session() {
    let (launch, set) = launch(&["auto"]);
    let err = Session::start(&launch, None).await.err().expect("session should fail").to_string();
    assert!(err.contains("does not offer session mode default"), "{err}");
    assert!(set.lock().unwrap().is_empty());
}

/// A fake harness offering to sign in on a terminal with `--login`, whose session/new fails with `refusal` when
/// there is one.
fn signing(refusal: Option<agent_client_protocol::Error>) -> Launch {
    let connect: Connector = Arc::new(move || {
        let refusal = refusal.clone();
        DynConnectTo::new(
            Agent
                .v1()
                .name("fake")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        let login = AuthMethodTerminal::new("login", "Log in").args(vec!["--login".to_owned()]);
                        let init = InitializeResponse::new(init.protocol_version)
                            .auth_methods(vec![AuthMethod::Terminal(login)])
                            .agent_info(Implementation::new("fake", "1.0"));
                        responder.respond(init)
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |_: NewSessionRequest, responder, _cx| match refusal.clone() {
                        Some(e) => responder.respond_with_error(e),
                        None => responder.respond(NewSessionResponse::new("s1")),
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    Launch::with_transport(connect, PathBuf::from("/"), Default::default())
}

#[tokio::test]
async fn a_signed_out_harness_is_told_apart_from_a_failing_one() {
    let ready = handshake(&signing(None)).await;
    assert_eq!(ready.readiness, Readiness::Ready { agent: "fake 1.0".to_owned() });
    assert_eq!(ready.sign_in.as_deref(), Some("test --login"), "the terminal auth method it offers");
    let session = Session::start(&signing(None), None).await.unwrap();
    let named = HarnessAgent { name: "fake".to_owned(), version: "1.0".to_owned() };
    assert_eq!(session.agent(), Some(&named), "the agent as it named itself");
    session.close().await;

    let required = signing(Some(agent_client_protocol::Error::auth_required()));
    assert_matches!(handshake(&required).await.readiness, Readiness::SignedOut { .. });
    let err = Session::start(&required, None).await.err().expect("a signed-out session");
    let signed_out = err.downcast_ref::<SignedOut>().expect("a sign-in error");
    assert_eq!((signed_out.harness.as_str(), signed_out.sign_in.as_deref()), ("test", Some("test --login")));
    let named = required.with_harness(custom("command = \"custom-acp\"\nsign_in = \"custom-acp login\""));
    assert_eq!(handshake(&named).await.sign_in.as_deref(), Some("custom-acp login"), "the definition's wins");

    // An adapter that exits on start: its stderr comes with the error.
    let exited = |stderr: &str| {
        agent_client_protocol::Error::internal_error().data(format!("Process exited with exit status: 1: {stderr}"))
    };
    let stderr = signing(Some(exited("Error: No authentication information found.")));
    assert_matches!(handshake(&stderr).await.readiness, Readiness::SignedOut { .. });
    let proxy = signing(Some(exited("Proxy authentication required")));
    assert_matches!(handshake(&proxy).await.readiness, Readiness::Failed { .. }, "no sign-in of the harness");
    let err = Session::start(&proxy, None).await.err().expect("a failing session");
    assert!(err.downcast_ref::<SignedOut>().is_none(), "{err:#}");
}

#[tokio::test]
async fn a_harness_not_installed_is_missing() {
    let on_path = |command: &str| {
        let harness = custom(&format!("command = \"{command}\""));
        let env = vec![("PATH".to_owned(), "/nonexistent-ants-bin".to_owned())];
        Launch::new(harness, PathBuf::from("/"), env, Default::default(), ModelSettings::default())
    };
    assert_eq!(handshake(&on_path("no-such-acp")).await.readiness, Readiness::Missing);
    assert_eq!(handshake(&on_path("/nonexistent-ants-bin/acp")).await.readiness, Readiness::Missing);
}

/// A fake harness asking permission as Codex does for an MCP tool: it reports the call, then asks by the call's
/// id alone. Replies with the option chosen.
fn codex(policy: PermissionPolicy) -> Launch {
    let connect: Connector = Arc::new(|| {
        DynConnectTo::new(
            Agent
                .v1()
                .name("codex")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        responder.respond(InitializeResponse::new(init.protocol_version))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |_: NewSessionRequest, responder, _cx| responder.respond(NewSessionResponse::new("s1")),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| {
                        let sender = cx.clone();
                        cx.spawn(async move {
                            let session = prompt.session_id;
                            let input = json!({ "server": "ants", "tool": "send_message", "arguments": {} });
                            let call =
                                ToolCall::new("c1", "mcp.ants.send_message").kind(ToolKind::Execute).raw_input(input);
                            sender.send_notification(SessionNotification::new(
                                session.clone(),
                                SessionUpdate::ToolCall(call),
                            ))?;
                            let fields =
                                ToolCallUpdateFields::new().kind(ToolKind::Execute).status(ToolCallStatus::Pending);
                            let options = vec![
                                PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
                                PermissionOption::new("cancel", "Cancel", PermissionOptionKind::RejectOnce),
                            ];
                            let ask = RequestPermissionRequest::new(
                                session.clone(),
                                ToolCallUpdate::new("c1", fields),
                                options,
                            );
                            let chosen = match sender.send_request(ask).block_task().await?.outcome {
                                RequestPermissionOutcome::Selected(s) => s.option_id.0.to_string(),
                                _ => "cancelled".to_owned(),
                            };
                            sender.send_notification(SessionNotification::new(
                                session,
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                                    TextContent::new(chosen),
                                ))),
                            ))?;
                            responder.respond(PromptResponse::new(StopReason::EndTurn))
                        })
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    Launch::with_transport(connect, PathBuf::from("/"), policy)
}

#[tokio::test]
async fn permission_requests_are_matched_with_the_tool_call_they_name() {
    for (rule, chosen) in [("other:send_message", "allow"), ("execute", "cancel"), ("other:check_inbox", "cancel")] {
        let policy = PermissionPolicy::new(Allowlist::parse(&[rule]).unwrap(), None);
        let session = Session::start(&codex(policy), None).await.unwrap();
        assert_eq!(session.prompt("go").await.unwrap(), chosen, "{rule}");
        session.close().await;
    }
}

/// A fake harness that streams each `|`-separated piece of a prompt as a chunk of its reply, then ends the turn.
fn echoing() -> Launch {
    let connect: Connector = Arc::new(|| {
        DynConnectTo::new(
            Agent
                .v1()
                .name("echo")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        responder.respond(InitializeResponse::new(init.protocol_version))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |_: NewSessionRequest, responder, _cx| responder.respond(NewSessionResponse::new("s1")),
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |prompt: PromptRequest, responder, cx| {
                        let ContentBlock::Text(t) = &prompt.prompt[0] else { unreachable!() };
                        for piece in t.text.split('|') {
                            let chunk = ContentChunk::new(ContentBlock::Text(TextContent::new(piece)));
                            cx.send_notification(SessionNotification::new(
                                prompt.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(chunk),
                            ))?;
                        }
                        responder.respond(PromptResponse::new(StopReason::EndTurn))
                    },
                    agent_client_protocol::on_receive_request!(),
                ),
        )
    });
    Launch::with_transport(connect, PathBuf::from("/"), Default::default())
}

#[tokio::test]
async fn a_reply_of_error_lines_reporting_a_limit_is_no_answer() {
    let session = Session::start(&echoing(), None).await.unwrap();
    let limited = |reply: Result<String>| reply.err().and_then(|e| e.downcast_ref::<Limited>().map(|l| l.kind));
    let overloaded = "Info: Request failed due to a transient API error. Retrying...|Error: Failed to get response \
                          from the AI model; retried 5 times (total retry wait time: 16.07 seconds) Last error: 529 \
                          Overloaded";
    assert_eq!(limited(session.prompt(overloaded).await), Some(LimitKind::RateLimit));
    let answers = [
        "A provider |answers 429 |when a rate limit is hit.",
        "Info: Request failed due to a transient API error. Retrying...|Hello",
        "Error: Failed to get response from the AI model; retried 5 times Last error: 500 Internal Server Error",
    ];
    for answer in answers {
        assert_eq!(session.prompt(answer).await.unwrap(), answer.replace('|', ""), "the session lives on");
    }
    session.close().await;
}

fn command_line(harness: &Harness, env: &[(&str, &str)], context: Option<&str>) -> (String, Vec<String>, Vec<String>) {
    let env: Vec<_> = env.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
    let settings = ModelSettings { context: context.map(str::to_owned), ..Default::default() };
    let process = harness.process(&env, &settings);
    let env = process.environment().iter().map(|(k, v)| format!("{k}={v}")).collect();
    (process.command().display().to_string(), process.arguments().to_vec(), env)
}

#[test]
fn builtin_harnesses_launch_as_before() {
    let strings = |items: &[&str]| items.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
    let cases = [
        ("copilot", None, "copilot", &["--acp"][..], None),
        ("copilot", Some("long_context"), "copilot", &["--acp", "--context", "long_context"][..], None),
        ("copilot", Some("default"), "copilot", &["--acp", "--context", "default"][..], None),
        ("claude", Some("1m"), "claude-agent-acp", &[][..], Some("default")),
        ("antigravity", None, "agy", &["acp", "--disable-telemetry"][..], Some("default")),
    ];
    for (name, context, command, args, mode) in cases {
        let harness = builtin(name);
        assert_eq!(command_line(&harness, &[], context), (command.to_owned(), strings(args), Vec::new()), "{name}");
        assert_eq!(harness.mode.as_deref(), mode, "{name}");
    }
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
    assert_eq!(builtin("copilot").history_budget(&ModelSettings::default()), crate::history::DEFAULT_BUDGET);
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
    assert!(unknown.contains("unknown harness \"vim\", expected one of antigravity, claude, copilot"), "{unknown}");
}

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
        DynConnectTo::new(
            Agent
                .v1()
                .name("configurable")
                .on_receive_request(
                    async move |init: InitializeRequest, responder, _cx| {
                        responder.respond(
                            InitializeResponse::new(init.protocol_version)
                                .agent_capabilities(AgentCapabilities::new().load_session(true)),
                        )
                    },
                    agent_client_protocol::on_receive_request!(),
                )
                .on_receive_request(
                    async move |_: NewSessionRequest, responder, _cx| {
                        responder.respond(NewSessionResponse::new("s1").config_options(initial.clone()))
                    },
                    agent_client_protocol::on_receive_request!(),
                )
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
                        log.lock().unwrap().push((config_id.to_owned(), value.clone()));
                        let mut options = state.lock().unwrap();
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
        let calls = calls.lock().unwrap();
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
            assert_eq!(calls.lock().unwrap()[0], ("model".to_owned(), expected.to_owned()));
        } else {
            let err = session.err().expect("unavailable context variant must fail").to_string();
            assert!(err.contains("unsupported model") && err.contains("base[1m]"), "{err}");
            assert!(calls.lock().unwrap().is_empty());
        }
    }
}
