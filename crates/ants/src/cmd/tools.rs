//! `ants tools`: the tool calls ants ask approval for.

use std::path::PathBuf;

use ants_proto::Verdict;
use anyhow::Result;
use clap::Subcommand;
use clap_complete::engine::ArgValueCandidates;

use crate::{Connection, completions, tools};

#[derive(Subcommand, Debug)]
pub(crate) enum ToolsCommand {
    /// List the pending tool requests: `<ant>/<id>`, thread, tool, files, options offered and time left.
    List {
        /// After listing, keep printing new requests until Ctrl-C.
        #[arg(long, short)]
        follow: bool,
        /// One JSON request per line.
        #[arg(long)]
        json: bool,
    },
    /// Show a tool request in full: its tool input, where it stands, and the commands answering it.
    Show {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// The request as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Allow the tool call of a pending request, once or with --always for the rest of the ant's session.
    Approve {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
        /// Allow this tool for the rest of the session (allow-always), when the request offers it.
        #[arg(long)]
        always: bool,
        /// Also add the allow rule fitting the request exactly to the ant's allow list in the config file, then
        /// reload the daemon serving it. An ant added from a template takes its allow rules from the template:
        /// nothing is added to its entry.
        #[arg(long)]
        rule: bool,
        /// Config file listing the ant, for --rule (default: ~/.ants/config.toml).
        #[arg(long, env = "ANTS_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Reject the tool call of a pending request.
    Reject {
        /// Request as `<ant>/<id>`, or a bare id when only one ant has it; a unique start of the id will do.
        #[arg(add = ArgValueCandidates::new(completions::tool_requests))]
        id: String,
    },
}

pub(crate) async fn run(command: ToolsCommand, conn: &Connection<'_>) -> Result<()> {
    let nats = || conn.nats();
    match command {
        ToolsCommand::List { follow, json } => tools::list(&nats()?, crate::Listing { follow, json }).await,
        ToolsCommand::Show { id, json } => tools::show(&nats()?, &id, json).await,
        ToolsCommand::Approve { id, always, rule, config } => {
            let verdict = if always { Verdict::AllowAlways } else { Verdict::AllowOnce };
            let config = match (rule, config) {
                (false, _) => None,
                (true, Some(path)) => Some(path),
                (true, None) => Some(ants_daemon::config::default_path()?),
            };
            tools::decide_request(&nats()?, &id, verdict, config.as_deref()).await
        }
        ToolsCommand::Reject { id } => tools::decide_request(&nats()?, &id, Verdict::Reject, None).await,
    }
}
