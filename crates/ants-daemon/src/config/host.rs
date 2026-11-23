//! The `[host]` and `[admin]` tables of the config file, and the machine's host connection they describe
//! ([`HostSettings`]), with the host credentials looked for where the table names none.

use std::path::{Path, PathBuf};

use ants_proto::{JsDomain, Machine};
use serde::Deserialize;

use super::{File, resolve};

/// The `[host]` table: the machine's host connection ([`crate::host`]).
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HostSection {
    creds: Option<PathBuf>,
    machine: Option<Machine>,
}

/// The `[admin]` table: the admin orders to the machine's ants ([`crate::admin`]).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdminSection {
    /// Whether to carry out the orders an admin signed; false refuses every one.
    #[serde(default = "super::yes")]
    remote: bool,
}

impl Default for AdminSection {
    fn default() -> Self {
        Self { remote: true }
    }
}

/// The machine's host connection a config file describes ([`crate::host`]), made only by a daemon serving the whole
/// file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSettings {
    /// The host credentials (`ants auth host`): the `[host]` table's `creds`, else `~/.config/ants/auth/host.creds`,
    /// else `host-<machine>.creds` there for the table's `machine`, else for the top-level `machine` or the host
    /// name's first label, whichever exists first; None for none.
    pub creds: Option<PathBuf>,
    /// The `[host]` table's `machine`.
    pub machine: Option<Machine>,
    /// The file's top-level `machine`, as written.
    pub top_machine: Option<String>,
    /// The file's top-level `nats` and `js_domain`.
    pub nats: Option<String>,
    pub js_domain: Option<JsDomain>,
    /// The `[admin]` table's `remote`: whether the admin orders to the machine's ants may be carried out.
    pub remote_admin: bool,
    /// The auth directory the host credentials were looked for in, where the `[host]` table names none.
    pub looked_in: Option<PathBuf>,
}

impl Default for HostSettings {
    fn default() -> Self {
        Self {
            creds: None,
            machine: None,
            top_machine: None,
            nats: None,
            js_domain: None,
            remote_admin: true,
            looked_in: None,
        }
    }
}

impl HostSettings {
    pub(super) fn resolve(file: &mut File, base: &Path, home: &Path) -> Self {
        let section = std::mem::take(&mut file.host);
        let top_machine = file.machine.clone();
        let fallback = top_machine.as_deref().and_then(|m| m.parse().ok());
        let (creds, looked_in) = match section.creds {
            Some(creds) => (Some(resolve(&creds, base, home)), None),
            None => {
                let dir = home.join(".config/ants/auth");
                (default_host_creds(&dir, section.machine.as_ref(), fallback.as_ref()), Some(dir))
            }
        };
        Self {
            creds,
            machine: section.machine,
            top_machine,
            nats: file.nats.clone(),
            js_domain: file.js_domain.clone(),
            remote_admin: file.admin.remote,
            looked_in,
        }
    }

    /// The files the host credentials are looked for at where the `[host]` table names none, whether they exist or
    /// not: `host.creds`, then `host-<machine>.creds` for the table's `machine`, else for [`Self::fallback`] and the
    /// first label of `hostname` (`default_host_creds`). A reload watches them come, go or change.
    pub fn creds_candidates(&self, hostname: &str) -> Vec<PathBuf> {
        let Some(dir) = &self.looked_in else { return Vec::new() };
        let named = |machine: &Machine| dir.join(format!("{}.creds", ants_proto::host_user(machine)));
        let mut candidates = vec![dir.join("host.creds")];
        match &self.machine {
            Some(machine) => candidates.push(named(machine)),
            None => {
                candidates.extend(self.fallback().as_ref().map(named));
                candidates.push(named(&Machine::from_hostname(hostname)));
            }
        }
        candidates.dedup();
        candidates
    }

    /// The top-level `machine` if it is a machine name ([`Machine`]): the machine's name where neither the `[host]`
    /// table nor the host credentials name one.
    pub fn fallback(&self) -> Option<Machine> {
        self.top_machine.as_deref().and_then(|m| m.parse().ok())
    }

    /// The machine's name, the host credentials left aside: the `[host]` table's `machine`, else [`Self::fallback`],
    /// else the first label of `hostname`.
    pub fn machine_or(&self, hostname: &str) -> Machine {
        self.machine.clone().or_else(|| self.fallback()).unwrap_or_else(|| Machine::from_hostname(hostname))
    }

    /// The machine's name as the host connection takes it ([`crate::host::machine_of`]): the `[host]` table's
    /// `machine`, else the one the host credentials name, else [`Self::machine_or`], which also stands when the
    /// credentials do not load or name another machine than the table.
    pub fn named(&self, hostname: &str) -> Machine {
        let otherwise = self.machine_or(hostname);
        crate::host::machine_of(self.machine.as_ref(), self.creds.as_deref(), otherwise.clone()).unwrap_or(otherwise)
    }
}

/// `host.creds` in the auth directory `dir`, else `host-<machine>.creds` there, `machine` being the one named, else
/// `fallback` or the first label of the host name, whichever's file exists; the host name is looked up only when
/// `dir` holds some host's credentials.
fn default_host_creds(dir: &Path, machine: Option<&Machine>, fallback: Option<&Machine>) -> Option<PathBuf> {
    let file = |name: &str| Some(dir.join(name)).filter(|path| path.is_file());
    let named = |machine: &Machine| file(&format!("{}.creds", ants_proto::host_user(machine)));
    file("host.creds").or_else(|| match machine {
        Some(machine) => named(machine),
        None => {
            let mut entries = std::fs::read_dir(dir).ok()?;
            let any = entries.any(|e| e.is_ok_and(|e| e.file_name().to_string_lossy().starts_with("host-")));
            any.then(|| fallback.and_then(named).or_else(|| named(&Machine::from_hostname(&hostname()?)))).flatten()
        }
    })
}

/// This machine's host name, as `hostname` prints it, read without a runtime ([`crate::colony::machine`] reads it
/// with one).
pub(super) fn hostname() -> Option<String> {
    crate::colony::printed(std::process::Command::new("hostname").output())
}
