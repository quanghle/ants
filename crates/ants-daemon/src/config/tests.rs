//! Tests of the daemon config file, one submodule per behaviour: parsing ants, harnesses, templates, the host, CLI,
//! storage, trails and schedule tables with their defaults and checks, and editing the file in place. The helpers
//! parsing a file's text are here.

use ants_proto::Machine;

use super::cli::cli_login_text;
use super::host::hostname;
use super::*;
use crate::daemon::Config;
use crate::escalation::Chain;
use crate::harness::{ModelSettings, PromptLimits};
use crate::limits::caps::Caps;
use crate::profile::template::Request;
use crate::publish::Publish;
use crate::worktrees::Base;

mod cli;
mod editing;
mod harnesses;
mod host;
mod parsing;
mod tables;
mod templates;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Parse as if the file sat in this crate's directory, with the workspace root as home, and `harness = "copilot"` and
/// `human = "human"` on top unless the file names its own top-level harness and human.
fn parse_here(text: &str) -> Result<Vec<Ant>> {
    Ok(parse_with_harnesses(text)?.0)
}

fn parse_with_harnesses(text: &str) -> Result<(Vec<Ant>, Harnesses)> {
    parse_all(text).map(|loaded| (loaded.ants, loaded.harnesses))
}

fn parse_all(text: &str) -> Result<Loaded> {
    parse_as_is(&with_harness(text))
}

/// Parse as [`parse_here`] does, the file as it is.
fn parse_as_is(text: &str) -> Result<Loaded> {
    let dir = manifest_dir();
    parse(text, &dir.join("config.toml"), dir.parent().unwrap().parent().unwrap())
}

/// `text` with `harness = "copilot"` and `human = "human"` on top unless it names a top-level harness and human.
fn with_harness(text: &str) -> String {
    let top = || text.lines().map(str::trim).take_while(|line| !line.starts_with('['));
    let names = |key: &str| top().any(|line| line.split('=').next().is_some_and(|k| k.trim() == key));
    let harness = if names("harness") { "" } else { "harness = \"copilot\"\n" };
    let human = if names("human") { "" } else { "human = \"human\"\n" };
    format!("{harness}{human}{text}")
}

/// `ant`'s config, its human defaulting to [`ants_test_support::human`].
fn config(ant: Ant) -> Result<Config> {
    let ant = Ant { human: ant.human.clone().or_else(|| Some(ants_test_support::human())), ..ant };
    ant.into_config("nats://unused", &Harnesses::builtin())
}
