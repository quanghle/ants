//! Snapshots of the provider protocol: its JSON-RPC messages, calls and wake refusals, and the admin orders about
//! providers.

use super::*;

fn call_context(by: provider::Starter) -> provider::CallContext {
    provider::CallContext {
        ant: user("dev-a"),
        identity: "0f1e2d3c4b5a69788796a5b4c3d2e1f0".into(),
        thread: thread(),
        origin: provider::Origin { by, channel: Some(channel()) },
        harness: "copilot".into(),
        model: Some("gpt-5".into()),
        machine: "studio".into(),
        repo: Some("github.com/owner/repo".into()),
        at: at(1),
    }
}

#[test]
fn provider_protocol() {
    use provider::jsonrpc::{Message, Notification, Request, RequestId, Response};
    use provider::*;
    use serde_json::json;

    let info = |name: &str| Info { name: name.into(), version: "0.2.0".into() };
    let params = InitializeParams::new(info("ants"), Capabilities { tools: true, wake: true, ..Capabilities::NONE });
    snap("provider_initialize_params", &params);
    let result = params.accept(&[PROTOCOL_VERSION], info("sense"), Capabilities::ALL).unwrap();
    snap("provider_initialize_result", &result);

    // Params are kept as JSON, whose keys keep their order only where serde_json preserves it: these are sorted.
    let ant = AntRef { ant: user("dev-a"), identity: "0f1e".into() };
    snap("provider_request", &Request::new(RequestId::Number(7), method::MEMORIES, &ant).unwrap());
    snap(
        "provider_request_string_id",
        &Request::new(RequestId::String("w-1".into()), method::TOOLS_LIST, &()).unwrap(),
    );
    snap("provider_notification", &Notification::new("log", &json!({"level": "info"})).unwrap());
    snap("provider_response_result", &Response::new(RequestId::Number(7), Ok(Ack {})));
    let refused = RpcError::unsupported_version(&[2], &[PROTOCOL_VERSION]);
    snap("provider_response_error", &Response::new(RequestId::Number(7), Err::<Ack, _>(refused)));
    snap("provider_response_unread", &Response::unread(RpcError::parse_error("expected value")));
    let line = Message::Response(Response::new(RequestId::Number(1), Ok(Woken { thread: thread() }))).to_line();
    snap_line("provider_message_line", &line);

    let tools = ToolList {
        tools: vec![Tool {
            name: "recall".into(),
            description: "Search memories.".into(),
            input_schema: json!({"properties": {"query": {"type": "string"}}, "type": "object"}),
        }],
    };
    snap("provider_tool_list", &tools);
    let call = ToolCall {
        name: "recall".into(),
        arguments: json!({"query": "deploy"}).as_object().unwrap().clone(),
        context: call_context(Starter::Human { user: user("alice") }),
        format: Format::Json,
    };
    snap("provider_tool_call", &call);
    let starters = vec![
        Starter::Human { user: user("alice") },
        Starter::Ant { user: user("dev-b") },
        Starter::Webhook { user: user("github") },
        Starter::Provider { name: "sense".into() },
    ];
    snap("provider_starter_variants", &starters);
    snap("provider_tool_result_text", &ToolResult { output: Output::Text("2 memories".into()), is_error: false });
    snap("provider_tool_result_json", &ToolResult { output: Output::Json(json!({"memories": []})), is_error: false });
    snap("provider_tool_result_error", &ToolResult { output: Output::Text("no such memory".into()), is_error: true });

    let brief = BriefRequest {
        context: call_context(Starter::Webhook { user: user("github") }),
        budget: BRIEF_BUDGET,
        reason: BriefReason::Rebuilt,
    };
    snap("provider_brief_request", &brief);
    snap("provider_brief", &Brief { text: "Goal: ship the release.".into() });

    let kinds = vec![
        EventKind::AntAdded,
        EventKind::AntRenamed { from: user("dev-old") },
        EventKind::AntMoved,
        EventKind::AntCloned { from: user("dev-b"), from_identity: "a1b2".into() },
        EventKind::AntRetired,
        EventKind::AntRevived,
        EventKind::ThreadStarted {
            origin: provider::Origin { by: Starter::Ant { user: user("dev-b") }, channel: None },
            repo: Some("github.com/o/r".into()),
        },
        EventKind::ThreadDone,
        EventKind::ThreadExpired,
        EventKind::SessionLost,
        EventKind::SessionRebuilt { reason: Rebuilt::Overflow },
    ];
    let events: Vec<Event> = kinds
        .into_iter()
        .zip(1..)
        .map(|(kind, n)| Event {
            id: id(n),
            ant: user("dev-a"),
            identity: "0f1e".into(),
            thread: Some(thread()),
            at: at(2),
            kind,
        })
        .collect();
    snap("provider_event_variants", &events);
    snap("provider_ack", &Ack {});

    snap("provider_woken", &Woken { thread: other_thread() });
    snap("provider_ant_ref", &ant);
    let memories = MemoriesView { memories: vec![MemoryEntry::new("lesson/deploy", "Tag first.")] };
    snap("provider_memories_view", &memories);
    snap("provider_memories_view_typed", &MemoriesView { memories: vec![typed_memory(), reminders()] });
    let untyped: MemoriesView = parse(r#"{"memories": [{"key": "lesson/deploy", "content": "Tag first."}]}"#);
    assert_eq!(untyped, memories, "a provider may leave the typed fields out");
    let export = Export { provider: "sense".into(), data: json!({"memories": 1}) };
    snap("provider_export", &export);
    snap("provider_import_request", &ImportRequest { ant: ant.ant, identity: ant.identity, export });
}

#[test]
fn provider_wake_requests() {
    use provider::WakeRequest;

    snap(
        "provider_wake_request",
        &WakeRequest {
            ant: user("dev-a"),
            thread: Some(thread()),
            text: "The reminder is due.".into(),
            id: "rem-1".into(),
            replaces: None,
        },
    );
    snap(
        "provider_wake_request_replacing",
        &WakeRequest {
            ant: user("dev-a"),
            thread: Some(thread()),
            text: "The watcher's result changed.".into(),
            id: "rem-2:3".into(),
            replaces: Some("rem-2:2".into()),
        },
    );
}

#[test]
fn provider_wake_refusals() {
    use provider::Woken;
    use provider::jsonrpc::{RequestId, Response, RpcError};

    let unknown = RpcError::new(RpcError::UNKNOWN_THREAD, format!("thread {} is no thread of ant dev-a", thread()));
    snap("provider_wake_unknown_thread", &Response::new(RequestId::Number(8), Err::<Woken, _>(unknown)));
}

#[test]
fn admin_orders_of_providers() {
    snap("admin_op_read_memories", &AdminOp::ReadMemories { identity: Some("0f1e".into()) });
    let payload = AdminPayload { id: id(9), machine: machine(), size: 300_000, sha256: "ab".repeat(32) };
    snap("admin_op_export_provider", &AdminOp::ExportProvider { identity: "0f1e".into() });
    snap("admin_op_import_provider", &AdminOp::ImportProvider { identity: "0f1e".into(), payload: payload.clone() });
    snap("admin_provider_export", &ProviderExport { provider: Some("sense".into()), payload: Some(payload) });
    let typed = typed_memory();
    let provider = ProviderMemories {
        provider: "sense".into(),
        memories: vec![ProviderMemory {
            key: typed.key,
            content: typed.content,
            fields: typed.fields,
            truncated: true,
        }],
        error: Some("provider sense did not answer within 10s".into()),
    };
    let memories = Memories {
        core: CoreMemories { whoami: Some("A reviewer.".into()), ..CoreMemories::default() },
        provider: Some(provider),
        skipped: vec![Skipped { what: "provider memory big".into(), why: "over 64 KiB".into() }],
        ..Memories::default()
    };
    snap("admin_memories_of_a_provider", &memories);
}

/// A memory of a memories view, with its typed fields.
fn typed_memory() -> provider::MemoryEntry {
    provider::MemoryEntry {
        key: "lesson-1a2b (lesson, self, importance 3, core), active, trust self, revision 2".into(),
        content: "Tag first\nThe release job reads the tag.".into(),
        fields: provider::MemoryFields {
            section: Some("memories".into()),
            id: Some("lesson-1a2b".into()),
            kind: Some("lesson".into()),
            scope: Some("self".into()),
            trust: Some("self".into()),
            title: Some("Tag first".into()),
            revision: Some(2),
            core: true,
            triggers: vec![],
        },
    }
}

/// The active reminders of a memories view: a reminder fired and a watcher failing its checks.
fn reminders() -> provider::MemoryEntry {
    use provider::TriggerRow;

    let reminder = TriggerRow {
        id: "reminder-3c4d".into(),
        revision: Some(4),
        kind: "reminder".into(),
        title: "Ask for the review".into(),
        status: "awaiting_acknowledgement".into(),
        thread: Some(thread()),
        trigger: Some("at 2025-01-01T12:01:00Z".into()),
        next_due: Some(at(59)),
        last_fired: Some(at(1)),
        commitment: Some("promise-5e6f".into()),
        failing_checks: None,
        last_error: None,
    };
    let watcher = TriggerRow {
        id: "reminder-7a8b".into(),
        revision: Some(9),
        kind: "watcher".into(),
        title: "CI of main".into(),
        status: "upcoming".into(),
        thread: Some(other_thread()),
        trigger: Some("watch command gh run list, every 60 s".into()),
        next_due: Some(at(3)),
        last_fired: Some(at(2)),
        commitment: None,
        failing_checks: Some(3),
        last_error: Some("it exited with status 1".into()),
    };
    provider::MemoryEntry {
        key: "active reminders".into(),
        content:
            "- reminder-3c4d (revision 4, fired): Ask for the review; ...\n- reminder-7a8b (revision 9, upcoming): \
                  CI of main; ..."
                .into(),
        fields: provider::MemoryFields {
            section: Some("work".into()),
            kind: Some("reminders".into()),
            triggers: vec![reminder, watcher],
            ..provider::MemoryFields::default()
        },
    }
}
