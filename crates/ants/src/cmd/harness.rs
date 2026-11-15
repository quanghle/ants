//! `ants harness`: check and prompt a harness.

use std::path::PathBuf;

use ants_daemon::{Launch, PermissionPolicy, harness};
use anyhow::Result;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub(crate) enum HarnessCommand {
    /// Start the harness and complete the ACP handshake.
    Check {
        #[command(flatten)]
        pick: HarnessPick,
    },
    /// Run one prompt in the current directory and print the reply. Tool requests are rejected.
    Prompt {
        #[command(flatten)]
        pick: HarnessPick,
        #[arg(required = true, num_args = 1..)]
        words: Vec<String>,
    },
}

#[derive(clap::Args, Debug)]
pub(crate) struct HarnessPick {
    /// Harness name: built in (`copilot`, `claude`, `antigravity`) or defined in the config file (default: the
    /// file's top-level `harness`, else `copilot`).
    #[arg(long)]
    pub(crate) harness: Option<String>,
    /// Config file defining harnesses (default: ~/.ants/config.toml, if it exists).
    #[arg(long, env = "ANTS_CONFIG")]
    pub(crate) config: Option<PathBuf>,
}

impl HarnessPick {
    pub(crate) fn resolve(&self) -> Result<std::sync::Arc<ants_daemon::Harness>> {
        let path = match &self.config {
            Some(path) => path.clone(),
            None => ants_daemon::config::default_path()?,
        };
        ants_daemon::config::Summary::load(&path)?.harness(self.harness.as_deref())
    }
}

pub(crate) async fn run(command: HarnessCommand) -> Result<()> {
    match command {
        HarnessCommand::Check { pick } => {
            let harness = pick.resolve()?;
            println!("{harness}: {}", harness::check(&harness).await?);
            Ok(())
        }
        HarnessCommand::Prompt { pick, words } => {
            let launch = Launch::new(
                pick.resolve()?,
                std::env::current_dir()?,
                Vec::new(),
                PermissionPolicy::reject_all(),
                Default::default(),
            );
            let reply = harness::prompt_once(&launch, &words.join(" ")).await?;
            println!("{reply}");
            Ok(())
        }
    }
}
