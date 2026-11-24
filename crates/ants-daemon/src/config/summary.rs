//! The [`Summary`] of a config file: the little of it that adding an ant, running a harness directly or reporting
//! spend needs, read without checking the rest.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ants_proto::{JsDomain, UserId};
use anyhow::{Context as _, Result};

use super::dropin::define;
use super::{Ant, File, HostSettings, home_dir, resolve};
use crate::harness::{Harness, Harnesses};
use crate::limits::caps::Caps;
use crate::profile::template::AntTemplate;

/// What adding an ant, running a harness directly or reporting spend needs to know of a config file: its top-level
/// `nats`, `js_domain`, `harness` and `human`, its harnesses, and the ants it lists with their credentials, resolved as
/// [`parse`] does, and their spend caps. Nothing else is checked.
///
/// [`parse`]: super::parse
#[derive(Debug, Default, PartialEq)]
pub struct Summary {
    pub nats: Option<String>,
    pub js_domain: Option<JsDomain>,
    pub harness: Option<String>,
    pub human: Option<UserId>,
    pub harnesses: Harnesses,
    pub ants: Vec<(UserId, Option<PathBuf>)>,
    pub caps: BTreeMap<UserId, Caps>,
    /// The machine's host connection.
    pub host: HostSettings,
}

impl Summary {
    /// Read the summary of the file at `path`; an empty one, with the built-in harnesses and those of its drop-in
    /// files, when there is no file.
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::parse(&text, &std::path::absolute(path)?, &home_dir()?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Self::parse("", &std::path::absolute(path)?, &home_dir()?)
            }
            Err(e) => Err(e).with_context(|| format!("reading config {}", path.display())),
        }
    }

    pub(super) fn parse(text: &str, path: &Path, home: &Path) -> Result<Self> {
        let mut file: File = toml::from_str(text).with_context(|| format!("parsing config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        let host = HostSettings::resolve(&mut file, base, home);
        let harnesses = define(file.harnesses, path, home)?;
        let caps = file.ants.iter().map(|ant| (ant.name.clone(), summary_caps(ant, &file.ant_templates))).collect();
        let ants = file.ants.into_iter().map(|ant| (ant.name, ant.creds.map(|c| resolve(&c, base, home)))).collect();
        Ok(Self {
            nats: file.nats,
            js_domain: file.js_domain,
            harness: file.harness,
            human: file.human,
            harnesses,
            ants,
            caps,
            host,
        })
    }

    /// The harness named `name`, else the file's top-level `harness`; with neither, an [`harness::Unset`] error.
    ///
    /// [`harness::Unset`]: crate::harness::Unset
    pub fn harness(&self, name: Option<&str>) -> Result<std::sync::Arc<Harness>> {
        self.harnesses.named(name.or(self.harness.as_deref()))
    }

    /// The credentials the file names for `ant`: `None` when it does not list the ant, `Some(None)` when its
    /// entry names none.
    pub fn creds_of(&self, ant: &UserId) -> Option<Option<&Path>> {
        self.ants.iter().find(|(name, _)| name == ant).map(|(_, creds)| creds.as_deref())
    }
}

/// The caps of `ant`: those of its entry, else of its template, if it names one of `templates`.
fn summary_caps(ant: &Ant, templates: &[AntTemplate]) -> Caps {
    let caps = ant.caps();
    match ant.template.as_ref().and_then(|name| templates.iter().find(|t| t.name == *name)) {
        Some(t) => Caps {
            thread_tokens: caps.thread_tokens.or(t.max_thread_tokens),
            thread_cost: caps.thread_cost.or(t.max_thread_cost),
            daily_tokens: caps.daily_tokens.or(t.max_daily_tokens),
            daily_cost: caps.daily_cost.or(t.max_daily_cost),
        },
        None => caps,
    }
}
