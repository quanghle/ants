//! The global `--nats`, `--creds` and `--js-domain`, defaulted from the config file where neither a flag nor the
//! environment sets them.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use ants_daemon::config::{self, CliDefaults};
use anyhow::Result;
use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory as _, FromArgMatches as _, Parser as _};

use crate::cli::{Ants, DaemonCommand};
use crate::cli::{Cli, Command};

/// Parse the command line, then fill in the connection it leaves to the config file.
pub fn parse() -> Result<Cli> {
    if is_daemon(std::env::args_os()) {
        return Ok(daemon(Ants::parse()));
    }
    let matches = Cli::command().get_matches();
    let mut cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    if cli.compact && cli.command.is_some() {
        let message = "--compact goes with bare `ants`, or after `ants chat`";
        Cli::command().error(clap::error::ErrorKind::ArgumentConflict, message).exit();
    }
    apply(&mut cli, &matches, &path()?)?;
    Ok(cli)
}

/// Whether the command line is `ants daemon`: its first argument that is no global connection option or value of one.
/// Anything else, `--help` and `--version` included, goes through the whole command line.
fn is_daemon(args: impl IntoIterator<Item = OsString>) -> bool {
    let mut args = args.into_iter().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--nats" | "--creds" | "--js-domain") => {
                args.next();
            }
            Some(arg) if ["--nats=", "--creds=", "--js-domain="].iter().any(|option| arg.starts_with(option)) => {}
            arg => return arg == Some("daemon"),
        }
    }
    false
}

/// The whole command line of a parsed `ants daemon`.
fn daemon(lean: Ants<DaemonCommand>) -> Cli {
    let Ants { nats, creds, js_domain, compact, command } = lean;
    Cli { nats, creds, js_domain, compact, command: command.map(Command::Daemon) }
}

/// The config file `ANTS_CONFIG` names, else `~/.ants/config.toml`.
pub(crate) fn path() -> Result<PathBuf> {
    match std::env::var_os(config::ENV) {
        Some(path) if !path.is_empty() => Ok(path.into()),
        _ => config::default_path(),
    }
}

/// How long a thread goes without a message before it counts as archived for the CLI's human: `auto_archive` of the
/// config file's `[cli]`; None for never.
pub fn auto_archive() -> Result<Option<std::time::Duration>> {
    Ok(CliDefaults::load(&path()?)?.auto_archive)
}

/// When and whom the CLI asks for catch-up summaries: the summary keys of the config file's `[cli]`.
pub fn summaries() -> Result<config::SummaryConfig> {
    Ok(CliDefaults::load(&path()?)?.summaries)
}

/// The `[cli]` defaults of the config file.
pub fn cli() -> Result<CliDefaults> {
    CliDefaults::load(&path()?)
}

/// Whether `ants chat` draws image attachments in the terminal: `inline_images` of the config file's `[cli]`.
pub fn inline_images() -> Result<bool> {
    Ok(CliDefaults::load(&path()?)?.inline_images)
}

/// Set the global connection flags that neither the command line nor the environment set from the file at `path`,
/// if it exists, except for `ants daemon` and `ants add`: their flags describe ants, and they read the file's
/// top-level keys themselves.
fn apply(cli: &mut Cli, matches: &ArgMatches, path: &Path) -> Result<()> {
    if matches!(cli.command, Some(Command::Daemon(_) | Command::Add(_))) {
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
        (
            &cli.nats,
            cli.creds.as_deref().and_then(Path::to_str),
            cli.js_domain.as_ref().map(ants_proto::JsDomain::as_str),
        )
    }

    #[test]
    fn ants_daemon_parses_the_same_without_the_other_commands() {
        let args = |line: &str| line.split(' ').map(OsString::from).collect::<Vec<_>>();
        for line in ["ants daemon", "ants --nats nats://hub:4222 --creds=a.creds daemon --ant a --harness h"] {
            assert!(is_daemon(args(line)), "{line}");
            let lean = daemon(Ants::try_parse_from(args(line)).unwrap());
            let whole = Cli::try_parse_from(args(line)).unwrap();
            assert_eq!(connection(&lean), connection(&whole), "{line}");
            assert_eq!(format!("{:?}", lean.command), format!("{:?}", whole.command), "{line}");
        }
        for line in ["ants", "ants --nats daemon", "ants --compact daemon", "ants --help daemon", "ants ask daemon"] {
            assert!(!is_daemon(args(line)), "{line}");
        }
        for line in ["ants daemon --help", "ants --js-domain hub daemon --ant", "ants daemon --bogus"] {
            let lean = Ants::<DaemonCommand>::try_parse_from(args(line)).err().unwrap();
            let whole = Cli::try_parse_from(args(line)).err().unwrap();
            assert_eq!((lean.kind(), lean.to_string()), (whole.kind(), whole.to_string()), "{line}");
        }
    }

    #[test]
    fn flags_override_the_file_which_overrides_the_built_in_defaults() {
        let dir = ants_test_support::scratch_dir("cli-defaults").canonicalize().unwrap();
        let file = dir.join("config.toml");
        let none = resolved("colony", &file).unwrap();
        assert_eq!(connection(&none), ("nats://127.0.0.1:4222", None, None), "no file");

        std::fs::write(&file, "nats = \"nats://leaf:4222\"\njs_domain = \"hub\"\n[cli]\ncreds = \"alice.creds\"\n")
            .unwrap();
        let creds = dir.join("alice.creds");
        let from_file = resolved("colony", &file).unwrap();
        assert_eq!(connection(&from_file), ("nats://leaf:4222", creds.to_str(), Some("hub")));
        let flags = resolved("colony --nats nats://hub:4222 --creds bob.creds --js-domain own", &file).unwrap();
        assert_eq!(connection(&flags), ("nats://hub:4222", Some("bob.creds"), Some("own")));
        let before = resolved("--js-domain own thread list", &file).unwrap();
        assert_eq!(connection(&before), ("nats://leaf:4222", creds.to_str(), Some("own")));

        let daemon = resolved("daemon", &file).unwrap();
        assert_eq!(connection(&daemon), ("nats://127.0.0.1:4222", None, None), "the daemon reads the file itself");

        std::fs::write(&file, "[cli]\nnats = \"nats://leaf:4222\"\ncred = \"alice.creds\"\n").unwrap();
        let err = format!("{:#}", resolved("colony", &file).err().unwrap());
        assert!(err.contains("config.toml") && err.contains("cred"), "{err}");
        assert!(resolved("colony --nats nats://hub:4222 --creds b.creds --js-domain own", &file).is_ok(), "not read");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
