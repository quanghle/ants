//! Which of the built-in ants a file serves, the messenger ([`super::builtin_messenger`]) and the sage
//! ([`super::builtin_sage`]), the machine serves: on a server taking credentials, only those whose own are on the
//! machine, each other one being left to another machine ([`Elsewhere`]); and whose credentials `ants daemon --creds`
//! stands for ([`Loaded::credit`]).

use std::path::{Path, PathBuf};

use ants_proto::{BuiltinAnt, UserId};
use anyhow::{Result, bail};

use super::ant::builtin_creds;
use super::{Ant, HostSettings, Loaded, home_dir};
use crate::harness;

/// A built-in ant the file serves but the machine leaves to another, the server taking credentials and the ant having
/// none on the machine.
#[derive(Debug, Clone, PartialEq)]
pub struct Elsewhere {
    /// The ant as the machine would serve it, without credentials.
    pub ant: Ant,
    /// Where its credentials belong: `~/.config/ants/auth/<name>.creds`.
    pub creds: PathBuf,
}

/// Where the credentials of each built-in ant belong, which the machine serving it or not depends on.
pub fn creds_files() -> Result<Vec<PathBuf>> {
    let home = home_dir()?;
    Ok(BuiltinAnt::ALL.into_iter().map(|builtin| builtin_creds(&builtin.id(), &home)).collect())
}

/// Whether the server takes credentials, as the machine's `host` credentials, those of one of `ants` or the CLI's
/// (`cli`, set in the `[cli]` table) tell: only `ants auth` issues them, and a server without authentication has its
/// ants connect as no one, as the built-in ants then do too.
pub(super) fn takes_credentials(ants: &[Ant], host: &HostSettings, cli: bool) -> bool {
    cli || host.creds.is_some() || ants.iter().any(|ant| ant.creds.is_some())
}

/// Take out of `ants` the built-in ones the machine leaves to another, those without credentials, where the server
/// takes some (`takes`, [`takes_credentials`]), `home` standing for `~`.
pub(super) fn leave(ants: &mut Vec<Ant>, takes: bool, home: &Path) -> Vec<Elsewhere> {
    if !takes {
        return Vec::new();
    }
    ants.extract_if(.., |ant| ant.builtin.is_some() && ant.creds.is_none())
        .map(|ant| Elsewhere { creds: builtin_creds(&ant.name, home), ant })
        .collect()
}

/// Fails with [`harness::Unset`] where the built-in `ant`, to be served here, names no harness, nor does its file: a
/// built-in ant left to another machine needs none.
pub(super) fn harness_set(ant: &Ant) -> Result<()> {
    if ant.builtin.is_some() && ant.harness.is_none() {
        return Err(anyhow::Error::new(harness::Unset).context(format!("ant {}", ant.name)));
    }
    Ok(())
}

impl Loaded {
    /// Have `creds` (`ants daemon --creds`) stand for the credentials of the ant `named` (`--ant`), else of the file's
    /// one `[[ant]]`, where it has none of its own: never for a built-in ant's unless named, one the machine leaves to
    /// another then served here with them. With them the server takes credentials, so of the whole file's ants the
    /// built-in ones without their own are left to another machine. Fails where none is named and the file lists
    /// several `[[ant]]` entries, or none.
    pub fn credit(&mut self, named: Option<&UserId>, creds: &Path) -> Result<()> {
        let creds = Some(creds.to_owned());
        if let Some(name) = named {
            if let Some(i) = self.elsewhere.iter().position(|left| left.ant.name == *name) {
                harness_set(&self.elsewhere[i].ant)?;
                let ant = self.elsewhere.remove(i).ant;
                self.ants.push(Ant { creds, ..ant });
            } else if let Some(ant) = self.ants.iter_mut().find(|ant| ant.name == *name && ant.creds.is_none()) {
                ant.creds = creds;
            }
            return Ok(());
        }
        let mut listed = self.ants.iter_mut().filter(|ant| ant.builtin.is_none());
        match (listed.next(), listed.count()) {
            (Some(ant), 0) => {
                if ant.creds.is_none() {
                    ant.creds = creds;
                }
            }
            (Some(_), more) => bail!(
                "--creds (ANTS_CREDS) stands for one ant's credentials, and the file lists {} [[ant]] entries: set \
                 creds in each, or serve one with --ant",
                more + 1
            ),
            (None, _) => bail!(
                "--creds (ANTS_CREDS) stands for one ant's credentials, and the file lists no [[ant]]: a built-in \
                 ant takes them from ~/.config/ants/auth/<name>.creds, or from --creds with --ant naming it"
            ),
        }
        self.elsewhere.extend(leave(&mut self.ants, true, &self.home));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
