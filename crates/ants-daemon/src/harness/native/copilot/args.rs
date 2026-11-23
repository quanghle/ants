//! What a copilot-sdk definition's arguments set of each session rather than of the CLI, whose server ignores them on
//! its command line.

use serde_json::{Map, Value};

/// Permission rules the CLI's server ignores, its sessions taking their tools from the SDK.
const IGNORED_ARGS: [&str; 2] = ["--allow-tool", "--deny-tool"];

/// What the definition's arguments set of each session rather than of the CLI, whose server ignores them on its
/// command line: the tools left to the model (`--available-tools`, `--excluded-tools`, each tool named, or several
/// separated by commas), the context tier (`--context`) and MCP servers added to the session's own
/// (`--additional-mcp-config`, as JSON or `@` a file of it, each naming them in `mcpServers`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct SessionArgs {
    pub(super) available: Option<Vec<String>>,
    pub(super) excluded: Option<Vec<String>>,
    pub(super) context: Option<String>,
    pub(super) mcp: Vec<String>,
}

impl SessionArgs {
    /// Take them out of `cli_args`, leaving the CLI's own; an error names an argument it cannot take.
    pub(super) fn take(cli_args: &mut Vec<String>) -> Result<Self, String> {
        let mut taken = Self::default();
        let mut rest = Vec::new();
        let mut args = std::mem::take(cli_args).into_iter().peekable();
        while let Some(arg) = args.next() {
            let (flag, value) = match arg.split_once('=') {
                Some((flag, value)) if flag.starts_with("--") => (flag.to_owned(), Some(value.to_owned())),
                _ => (arg.clone(), None),
            };
            let tools = match flag.as_str() {
                "--available-tools" => &mut taken.available,
                "--excluded-tools" => &mut taken.excluded,
                "--context" => {
                    let tier = value.or_else(|| args.next_if(|next| !next.starts_with('-')));
                    taken.context = Some(tier.ok_or("--context takes a tier")?);
                    continue;
                }
                "--additional-mcp-config" => {
                    let config = value.or_else(|| args.next()).ok_or("--additional-mcp-config takes JSON or @FILE")?;
                    if !config.starts_with('@') {
                        mcp_config(&config)?;
                    }
                    taken.mcp.push(config);
                    continue;
                }
                flag if IGNORED_ARGS.contains(&flag) => {
                    return Err(format!(
                        "the CLI's server ignores {flag}: choose the tools with --available-tools or --excluded-tools"
                    ));
                }
                _ => {
                    rest.push(arg);
                    continue;
                }
            };
            let mut names = value.into_iter().collect::<Vec<_>>();
            if names.is_empty() {
                while let Some(next) = args.next_if(|next| !next.starts_with('-')) {
                    names.push(next);
                }
            }
            let names = names.iter().flat_map(|names| names.split(',')).map(str::trim).filter(|n| !n.is_empty());
            let tools = tools.get_or_insert_default();
            for name in names {
                if name == "*" {
                    return Err(format!("{flag} takes builtin:*, mcp:* or custom:* rather than *"));
                }
                tools.push(name.to_owned());
            }
        }
        *cli_args = rest;
        Ok(taken)
    }

    /// The servers `--additional-mcp-config` adds, those of a later one over an earlier one's, reading its files.
    pub(super) fn mcp_servers(&self) -> Result<Map<String, Value>, String> {
        let mut servers = Map::new();
        for config in &self.mcp {
            let read = match config.strip_prefix('@') {
                Some(path) => std::fs::read_to_string(path)
                    .map_err(|e| format!("--additional-mcp-config: reading {path}: {e}"))
                    .and_then(|text| mcp_config(&text).map_err(|e| format!("{e} in {path}")))?,
                None => mcp_config(config)?,
            };
            servers.extend(read);
        }
        Ok(servers)
    }
}

/// The servers of an MCP configuration, as `--additional-mcp-config` takes it: `{"mcpServers": {NAME: SERVER}}`, a
/// server without a `type` being `stdio` with a `command`, else `http`.
fn mcp_config(text: &str) -> Result<Map<String, Value>, String> {
    let config: Value = serde_json::from_str(text).map_err(|e| format!("--additional-mcp-config: {e}"))?;
    let Some(Value::Object(servers)) = config.get("mcpServers") else {
        return Err("--additional-mcp-config takes an object of servers in mcpServers".to_owned());
    };
    let mut servers = servers.clone();
    for (name, server) in &mut servers {
        let Value::Object(server) = server else {
            return Err(format!("--additional-mcp-config: server {name} is no object"));
        };
        if !server.contains_key("type") {
            let kind = if server.contains_key("command") { "stdio" } else { "http" };
            server.insert("type".to_owned(), kind.into());
        }
    }
    Ok(servers)
}
