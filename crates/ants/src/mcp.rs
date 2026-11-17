//! `ants ant mcp`: the MCP servers of an ant's harness and how each stands, which an admin order asks the daemon of its
//! machine to read ([`AdminOp::ReadMcp`]) with a session of the harness opened for them alone, and their OAuth sign-in
//! ([`AdminOp::McpSignIn`]).

use std::fmt::Write as _;
use std::time::Duration;

use ants_proto::{AdminOp, MCP_FRESH, Machine, McpReport, McpSignIn, McpStatus, OrderState, UserId};
use anyhow::{Result, bail};
use async_nats::jetstream;
use serde::de::DeserializeOwned;

use crate::connection::Nats;
use crate::orders;

/// How long the CLI waits for the daemon's answer: the harness starts, and its servers have some seconds to.
pub const WAIT: Duration = Duration::from_secs(90);

/// Ask the daemon of `machine`, else of the machine that last recorded serving `ant`, for the MCP servers of the
/// harness of `ant`, with the admin credentials of `nats`.
pub async fn read(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
) -> Result<McpReport> {
    ask(nats, js, machine, ant, AdminOp::ReadMcp).await
}

/// Ask the daemon serving `ant` to sign its harness in to MCP server `server`, or with `callback` to hand the harness
/// the address the browser came back to.
pub async fn sign_in(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    server: &str,
    callback: Option<String>,
) -> Result<McpSignIn> {
    ask(nats, js, machine, ant, AdminOp::McpSignIn { server: server.to_owned(), callback }).await
}

async fn ask<T: DeserializeOwned>(
    nats: &Nats<'_>,
    js: &jetstream::Context,
    machine: Option<Machine>,
    ant: &UserId,
    op: AdminOp,
) -> Result<T> {
    let what = op.describe();
    let (order, outcome) = orders::send(nats, js, machine, ant, op, WAIT).await?;
    let Some(outcome) = outcome else {
        bail!(
            "the daemon of {} did not answer within {}s: it reads MCP servers only while online, and the order is \
             refused as stale once it waited {}m",
            order.machine,
            WAIT.as_secs(),
            MCP_FRESH.num_minutes()
        );
    };
    match (outcome.state, outcome.result) {
        (OrderState::Applied, Some(result)) => Ok(serde_json::from_value(result)?),
        (OrderState::Applied, None) => bail!("the daemon of {} answered without a result", order.machine),
        (_, _) => bail!(
            "the daemon of {} refused to {what} of {ant}: {}",
            order.machine,
            outcome.reason.as_deref().unwrap_or("no reason given")
        ),
    }
}

/// `ants ant mcp ANT`: the MCP servers of `ant` as text, or with `json` as one JSON object; with `sign_in`, sign in to
/// that server instead.
pub async fn show(
    nats: &Nats<'_>,
    ant: &UserId,
    machine: Option<Machine>,
    json: bool,
    sign_in: Option<String>,
    callback: Option<String>,
) -> Result<()> {
    let (_, js) = nats.connect("ants-mcp").await?;
    let Some(server) = sign_in else {
        let report = read(nats, &js, machine, ant).await?;
        if json {
            println!("{}", serde_json::to_string(&report)?);
        } else {
            print!("{}", render(ant, &report));
        }
        return Ok(());
    };
    let signing = self::sign_in(nats, &js, machine, ant, &server, callback).await?;
    if json {
        println!("{}", serde_json::to_string(&signing)?);
    } else {
        print!("{}", render_sign_in(ant, &signing));
    }
    Ok(())
}

/// The MCP servers of `ant` as text: one line each, with its status, where the harness took it from and its tools,
/// and why it failed below.
pub fn render(ant: &UserId, report: &McpReport) -> String {
    let at = report.read_at.format("%H:%M:%S UTC");
    let mut out =
        format!("MCP servers of {ant}, harness {} (protocol {}), read {at}\n", report.harness, report.protocol);
    let Some(servers) = &report.servers else {
        out.push_str("  not reported by this harness: only protocols claude, codex and copilot-sdk report them\n");
        return out;
    };
    if servers.is_empty() {
        out.push_str("  none\n");
    }
    let width = servers.iter().map(|s| s.name.chars().count()).max().unwrap_or(0);
    for server in servers {
        let mut line = format!("  {:<width$}  {:<10}", server.name, server.status.name());
        if let Some(tools) = server.tools {
            _ = write!(line, "  {tools} tools");
        }
        if let Some(source) = &server.source {
            _ = write!(line, "  ({source})");
        }
        out.push_str(line.trim_end());
        out.push('\n');
        if let Some(error) = &server.error {
            _ = writeln!(out, "  {:width$}  {}", "", crate::text::excerpt(error, 200));
        }
    }
    if report.truncated {
        out.push_str("  and more, left out\n");
    }
    if servers.iter().any(|s| s.status == McpStatus::NeedsAuth) {
        _ = writeln!(out, "sign in to a server needing it: ants ant mcp {ant} --sign-in SERVER");
    }
    out
}

/// What a sign-in did, as text: where to open a browser, and how to hand back the address it comes back to.
pub fn render_sign_in(ant: &UserId, signing: &McpSignIn) -> String {
    let server = &signing.server;
    let Some(url) = &signing.authorization_url else {
        let mut out = format!("MCP server {} of {ant}: {}\n", server.name, server.status.name());
        if let Some(error) = &server.error {
            _ = writeln!(out, "  {error}");
        }
        return out;
    };
    let until = signing.expires_at.map_or_default(|at| format!(", until {}", at.format("%H:%M UTC")));
    format!(
        "open in a browser to sign in to MCP server {name} of {ant}{until}:\n  {url}\n\
         A browser on the machine of {ant} comes back to its harness on its own. A browser elsewhere ends on an \
         address that fails to load: hand that address over with\n  ants ant mcp {ant} --sign-in {name} --callback \
         'ADDRESS'\n",
        name = server.name
    )
}

#[cfg(test)]
mod tests {
    use ants_proto::McpServerState;
    use chrono::{TimeZone as _, Utc};

    use super::*;

    #[test]
    fn servers_render_with_their_status_and_a_sign_in_hint() {
        let ant: UserId = "dev-a".parse().unwrap();
        let read_at = Utc.with_ymd_and_hms(2026, 5, 1, 9, 30, 0).unwrap();
        let servers = vec![
            McpServerState::new("ants", McpStatus::Connected, None, Some("session")).with_tools(Some(12)),
            McpServerState::new("github", McpStatus::NeedsAuth, Some("unauthorized"), None),
        ];
        let report = McpReport {
            harness: "copilot".to_owned(),
            protocol: "copilot-sdk".to_owned(),
            servers: Some(servers),
            truncated: false,
            read_at,
        };
        assert_eq!(
            render(&ant, &report),
            "MCP servers of dev-a, harness copilot (protocol copilot-sdk), read 09:30:00 UTC\n\
             \x20 ants    connected   12 tools  (session)\n\
             \x20 github  needs_auth\n\
             \x20         unauthorized\n\
             sign in to a server needing it: ants ant mcp dev-a --sign-in SERVER\n"
        );
        let unreported = McpReport { servers: None, protocol: "acp".to_owned(), ..report };
        assert!(render(&ant, &unreported).contains("not reported by this harness"));
    }
}
