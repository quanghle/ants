//! Opening a session, through session/load or as a new one, and putting it in the harness's mode and the ant's model
//! settings.

use agent_client_protocol::Agent;
use agent_client_protocol::v1::ConnectionTo;
use agent_client_protocol::v1::schema::{
    LoadSessionRequest, McpServer, NewSessionRequest, SessionConfigKind, SessionConfigOption,
    SessionConfigOptionCategory, SessionConfigSelectOptions, SessionId, SessionModeState,
    SetSessionConfigOptionRequest, SetSessionModeRequest,
};

use super::Launch;

pub(super) async fn open(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    resume: Option<SessionId>,
    can_load: bool,
    mcp: Vec<McpServer>,
) -> Result<SessionId, agent_client_protocol::Error> {
    if let Some(id) = resume {
        if !can_load {
            tracing::warn!(session = %id, "{} cannot load sessions; starting a new one", launch.harness);
        } else {
            let load = LoadSessionRequest::new(id.clone(), launch.cwd.clone()).mcp_servers(mcp.clone());
            match cx.send_request(load).block_task().await {
                Ok(loaded) => {
                    configure(cx, launch, &id, loaded.modes, loaded.config_options.unwrap_or_default()).await?;
                    return Ok(id);
                }
                Err(e) => tracing::warn!(session = %id, error = %e, "session/load failed; starting a new session"),
            }
        }
    }
    new_session(cx, launch, mcp).await
}

pub(super) async fn new_session(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    mcp: Vec<McpServer>,
) -> Result<SessionId, agent_client_protocol::Error> {
    let new = cx.send_request(NewSessionRequest::new(launch.cwd.clone()).mcp_servers(mcp)).block_task().await?;
    configure(cx, launch, &new.session_id, new.modes, new.config_options.unwrap_or_default()).await?;
    Ok(new.session_id)
}

async fn configure(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    id: &SessionId,
    modes: Option<SessionModeState>,
    mut options: Vec<SessionConfigOption>,
) -> Result<(), agent_client_protocol::Error> {
    launch
        .settings
        .validate(&launch.harness)
        .map_err(|e| agent_client_protocol::Error::internal_error().data(e.to_string()))?;
    set_mode(cx, launch, id, modes).await?;
    let model = launch.settings.model_id(&launch.harness);
    for (name, value) in
        [("model", model.as_deref()), ("reasoning_effort", launch.settings.reasoning_effort.as_deref())]
    {
        if let Some(value) = value {
            options = set_option(cx, id, name, value, options).await?;
        }
    }
    // Applying a later option must not silently reset an earlier explicit setting.
    for (name, value) in
        [("model", model.as_deref()), ("reasoning_effort", launch.settings.reasoning_effort.as_deref())]
    {
        if let Some(value) = value {
            check_option(&options, name, value)?;
        }
    }
    Ok(())
}

fn option<'a>(options: &'a [SessionConfigOption], name: &str) -> Option<&'a SessionConfigOption> {
    options.iter().find(|option| match name {
        "model" => option.id.0.as_ref() == "model" || option.category == Some(SessionConfigOptionCategory::Model),
        "reasoning_effort" => {
            matches!(option.id.0.as_ref(), "reasoning_effort" | "effort")
                || option.category == Some(SessionConfigOptionCategory::ThoughtLevel)
        }
        _ => false,
    })
}

fn check_option(options: &[SessionConfigOption], name: &str, value: &str) -> Result<(), agent_client_protocol::Error> {
    if option(options, name).is_some_and(
        |option| matches!(&option.kind, SessionConfigKind::Select(select) if select.current_value.0.as_ref() == value),
    ) {
        return Ok(());
    }
    Err(agent_client_protocol::Error::internal_error().data(format!("harness did not apply {name}={value:?}")))
}

async fn set_option(
    cx: &ConnectionTo<Agent>,
    id: &SessionId,
    name: &str,
    value: &str,
    options: Vec<SessionConfigOption>,
) -> Result<Vec<SessionConfigOption>, agent_client_protocol::Error> {
    let fail = |message: String| agent_client_protocol::Error::internal_error().data(message);
    let option = option(&options, name).ok_or_else(|| fail(format!("harness does not expose {name} configuration")))?;
    let SessionConfigKind::Select(select) = &option.kind else {
        return Err(fail(format!("harness {name} is not a selectable option")));
    };
    let values: Vec<&str> = match &select.options {
        SessionConfigSelectOptions::Ungrouped(values) => values.iter().map(|v| v.value.0.as_ref()).collect(),
        SessionConfigSelectOptions::Grouped(groups) => {
            groups.iter().flat_map(|group| &group.options).map(|v| v.value.0.as_ref()).collect()
        }
        _ => return Err(fail(format!("harness {name} has an unsupported option format"))),
    };
    if !values.contains(&value) {
        return Err(fail(format!("unsupported {name}={value:?}; available values: {}", values.join(", "))));
    }
    let response =
        cx.send_request(SetSessionConfigOptionRequest::new(id.clone(), option.id.clone(), value)).block_task().await?;
    check_option(&response.config_options, name, value)?;
    Ok(response.config_options)
}

/// Put the session in the launch's mode, if it has one; a harness that does not offer it fails the session.
async fn set_mode(
    cx: &ConnectionTo<Agent>,
    launch: &Launch,
    id: &SessionId,
    modes: Option<SessionModeState>,
) -> Result<(), agent_client_protocol::Error> {
    let Some(want) = launch.harness.mode.as_deref() else { return Ok(()) };
    let offered = modes.as_ref().is_some_and(|m| m.available_modes.iter().any(|mode| &*mode.id.0 == want));
    if !offered {
        return Err(agent_client_protocol::Error::internal_error()
            .data(format!("{} does not offer session mode {want}", launch.harness)));
    }
    if modes.is_some_and(|m| &*m.current_mode_id.0 == want) {
        return Ok(());
    }
    cx.send_request(SetSessionModeRequest::new(id.clone(), want.to_owned())).block_task().await?;
    tracing::debug!(session = %id, mode = want, "session mode set");
    Ok(())
}
