//! The step of `ants setup` writing the config file of the machine's ants, or adding the ant of `--ant` to the one
//! there.

use std::fmt::Write as _;
use std::path::Path;

use ants_daemon::config::{self, Ant, Summary};
use ants_daemon::harness;
use ants_proto::UserId;
use anyhow::{Context as _, Result};

use super::{Managed, Mark, Steps, pick};
use crate::node::conf::quote as quoted;

impl Steps<'_> {
    /// Write the config file, unless there is one: the messenger, `harness`, the server and the default human given,
    /// the `[nats_server]` of `managed`, and the ant of --ant. To a file there, only add the ant, when not listed; one naming no harness
    /// for its ants fails the step. Returns whether it changed the file.
    pub(super) fn config_file(&mut self, harness: Option<&str>, managed: &Managed) -> bool {
        let (connection, path) = (self.connection, &self.paths.config);
        let step = "config";
        let shown = path.display();
        let summary = if path.exists() {
            match Summary::load(path) {
                Ok(summary) => Some(summary),
                Err(e) => {
                    self.report.step(Mark::Fail, step, format!("{e:#}"));
                    return false;
                }
            }
        } else {
            None
        };
        let entry = match (&self.args.ant, &self.args.workdir) {
            (Some(name), Some(workdir)) => match ant_entry(name, workdir, connection, summary.as_ref(), harness) {
                Ok(entry) => Some((name, entry)),
                Err(e) => {
                    self.report.step(Mark::Fail, step, format!("ant {name}: {e:#}"));
                    return false;
                }
            },
            _ => None,
        };
        let Some(summary) = summary else {
            let mut text = String::from("# The ants of this machine (docs/config.md), written by `ants setup`.\n");
            if let Some(harness) = harness {
                let _ = writeln!(text, "harness = {}", quoted(harness));
            }
            if connection.nats != crate::cli::DEFAULT_NATS {
                let _ = writeln!(text, "nats = {}", quoted(connection.nats));
            }
            if let Some(human) = connection.human {
                let _ = writeln!(text, "human = {}", quoted(human.as_str()));
            }
            text.push_str("messenger = true\n");
            let mut running = String::new();
            if let Managed::New(conf) = managed {
                text.push_str(&nats_server_table(conf, self.args.nats_server.as_deref()));
                running = format!(", running nats-server -c {}", conf.display());
            }
            if let Some((_, entry)) = &entry {
                text.push('\n');
                text.push_str(entry);
            }
            let listed = match &entry {
                Some((name, _)) => format!("the messenger, {name} and the sage"),
                None => "the messenger".to_owned(),
            };
            let written = if self.report.dry_run { Ok(()) } else { super::write_new(path, &text) };
            match written {
                Ok(()) => self.report.step(
                    Mark::Done,
                    step,
                    format!("{shown} serving {listed} on harness {}{running}", harness.unwrap_or("none")),
                ),
                Err(e) => self.report.step(Mark::Fail, step, format!("{e:#}")),
            }
            return !self.report.dry_run;
        };
        if let Some(harness) = self.args.harness.as_ref().filter(|h| summary.harness.as_ref() != Some(*h)) {
            self.report.step(Mark::Skip, step, format!("{shown} kept: its harness stays, not set to {harness}"));
        }
        let Some((name, entry)) = entry else {
            match config::load(path) {
                Err(e) if harness::Unset::caused(&e) => {
                    let installed = pick::installed(&pick::detect(&std::env::var_os("PATH").unwrap_or_default()));
                    let installed = match installed.as_slice() {
                        [] => "none of the built-in ones".to_owned(),
                        names => names.join(", "),
                    };
                    self.report.step(Mark::Fail, step, format!("{shown} kept: {e:#}; installed here: {installed}"));
                }
                _ => self.report.step(Mark::Exists, step, format!("{shown} kept")),
            }
            return false;
        };
        if summary.ants.iter().any(|(ant, _)| ant == name) {
            self.report.step(Mark::Exists, step, format!("{shown} lists {name} already; its entry kept"));
            return false;
        }
        let appended = if self.report.dry_run { Ok(()) } else { config::append(path, &entry) };
        match appended {
            Ok(()) => self.report.step(Mark::Done, step, format!("{shown}: {name} added")),
            Err(e) => self.report.step(Mark::Fail, step, format!("{e:#}")),
        }
        !self.report.dry_run
    }
}

/// The `[nats_server]` table of a new config file: the daemon runs nats-server, `command` when given, with `conf`.
pub(super) fn nats_server_table(conf: &Path, command: Option<&Path>) -> String {
    let mut table = format!(
        "\n# The local nats-server, which `ants daemon` starts and stops itself; no other service runs it.\n\
         [nats_server]\nconfig = {}\n",
        quoted(&conf.to_string_lossy())
    );
    if let Some(command) = command {
        let command = std::path::absolute(command).unwrap_or_else(|_| command.to_owned());
        let _ = writeln!(table, "command = {}", quoted(&command.to_string_lossy()));
    }
    table
}

/// The `[[ant]]` entry of `name` working in `workdir`, checked as the daemon will check it: running the top-level
/// harness of the file there, else `harness`, that of a new file.
fn ant_entry(
    name: &UserId,
    workdir: &Path,
    connection: &crate::connection::Connection<'_>,
    summary: Option<&Summary>,
    harness: Option<&str>,
) -> Result<String> {
    let workdir = workdir.canonicalize().with_context(|| format!("workdir {}", workdir.display()))?;
    let ant = Ant::new(name.clone(), workdir);
    let harnesses = summary.map_or_default(|s| s.harnesses.clone());
    let (harness, human) = match summary {
        Some(summary) => (summary.harness.clone(), summary.human.clone()),
        None => (harness.map(str::to_owned), connection.human.cloned()),
    };
    Ant { harness, human, ..ant.clone() }.into_config(connection.nats, &harnesses)?;
    config::entry(&ant)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{managed_args, paths, steps};
    use super::*;

    #[test]
    fn a_new_config_file_names_the_managed_nats_server_in_a_table_the_daemon_loads() {
        let dir = ants_test_support::scratch_dir("setup-managed-table").canonicalize().unwrap();
        let paths = paths(&dir);
        let (conf, path) = (&paths.leaf_conf, &paths.config);
        let (args, unmanaged) = (managed_args(Some("bin/nats-server")), managed_args(None));
        let mut steps = steps(&args, &paths);
        let managed = Managed::New(conf.clone());
        assert!(steps.config_file(Some("copilot"), &managed));
        let nats_server = config::load(path).unwrap().nats_server.unwrap();
        assert_eq!(&nats_server.config, conf);
        assert_eq!(nats_server.command, std::path::absolute("bin/nats-server").unwrap());

        std::fs::remove_file(path).unwrap();
        steps.args = &unmanaged;
        assert!(steps.config_file(Some("copilot"), &Managed::Off));
        assert_eq!(config::load(path).unwrap().nats_server, None, "without --managed-nats");
        assert_eq!(steps.report.failed, 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
