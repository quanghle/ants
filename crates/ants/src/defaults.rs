//! The global `--nats`, `--creds` and `--js-domain`, defaulted from the config file where neither a flag nor the
//! environment sets them.

use std::path::{Path, PathBuf};

use ants_daemon::config::{self, CliDefaults};
use anyhow::Result;
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory as _, FromArgMatches as _};

use crate::{Cli, Command};

/// Parse the command line, then fill in the connection it leaves to the config file.
pub fn parse() -> Result<Cli> {
    let matches = Cli::command().get_matches();
    let mut cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    apply(&mut cli, &matches, &path()?)?;
    Ok(cli)
}

/// The config file `ANTS_CONFIG` names, else `~/.ants/config.toml`.
fn path() -> Result<PathBuf> {
    match std::env::var_os(config::ENV) {
        Some(path) if !path.is_empty() => Ok(path.into()),
        _ => config::default_path(),
    }
}

/// Set the global connection flags that neither the command line nor the environment set from the file at `path`,
/// if it exists, except for `ants daemon` and `ants add`: their flags describe ants, and they read the file's
/// top-level keys themselves.
fn apply(cli: &mut Cli, matches: &ArgMatches, path: &Path) -> Result<()> {
    if matches!(cli.command, Command::Daemon(_) | Command::Add(_)) {
        return Ok(());
    }
    let given = |id| matches!(matches.value_source(id), Some(ValueSource::CommandLine | ValueSource::EnvVariable));
    let (nats, creds, js_domain) = (given("nats"), given("creds"), given("js_domain"));
    if nats && creds && js_domain {
        return Ok(());
    }
    let defaults = CliDefaults::load(path)?;
    if let (false, Some(url)) = (nats, defaults.nats) {
        cli.nats = url;
    }
    if !creds && defaults.creds.is_some() {
        cli.creds = defaults.creds;
    }
    if !js_domain && defaults.js_domain.is_some() {
        cli.js_domain = defaults.js_domain;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(args: &str, file: &Path) -> Result<Cli> {
        let matches = Cli::command().try_get_matches_from(["ants"].into_iter().chain(args.split(' ')))?;
        let mut cli = Cli::from_arg_matches(&matches)?;
        apply(&mut cli, &matches, file)?;
        Ok(cli)
    }

    fn connection(cli: &Cli) -> (&str, Option<&str>, Option<&str>) {
        (&cli.nats, cli.creds.as_deref().and_then(Path::to_str), cli.js_domain.as_ref().map(|d| d.as_str()))
    }

    #[test]
    fn flags_override_the_file_which_overrides_the_built_in_defaults() {
        let dir = ants_test_support::scratch_dir("cli-defaults").canonicalize().unwrap();
        let file = dir.join("config.toml");
        let none = resolved("roster", &file).unwrap();
        assert_eq!(connection(&none), ("nats://127.0.0.1:4222", None, None), "no file");

        std::fs::write(&file, "nats = \"nats://leaf:4222\"\njs_domain = \"hub\"\n[cli]\ncreds = \"alice.creds\"\n")
            .unwrap();
        let creds = dir.join("alice.creds");
        let from_file = resolved("roster", &file).unwrap();
        assert_eq!(connection(&from_file), ("nats://leaf:4222", creds.to_str(), Some("hub")));
        let flags = resolved("roster --nats nats://hub:4222 --creds bob.creds --js-domain own", &file).unwrap();
        assert_eq!(connection(&flags), ("nats://hub:4222", Some("bob.creds"), Some("own")));
        let before = resolved("--js-domain own thread list", &file).unwrap();
        assert_eq!(connection(&before), ("nats://leaf:4222", creds.to_str(), Some("own")));

        let daemon = resolved("daemon", &file).unwrap();
        assert_eq!(connection(&daemon), ("nats://127.0.0.1:4222", None, None), "the daemon reads the file itself");

        std::fs::write(&file, "[cli]\nnats = \"nats://leaf:4222\"\ncred = \"alice.creds\"\n").unwrap();
        let err = format!("{:#}", resolved("roster", &file).err().unwrap());
        assert!(err.contains("config.toml") && err.contains("cred"), "{err}");
        assert!(resolved("roster --nats nats://hub:4222 --creds b.creds --js-domain own", &file).is_ok(), "not read");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
