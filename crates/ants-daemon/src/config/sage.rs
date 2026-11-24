//! Who reviews each ant: the `sage` setting of its `[[ant]]` table, one of its shared settings, else the file's
//! top-level `sage`, which names the built-in sage ([`super::builtin_sage`]) unless set ([`Sage`]). A sage has no sage
//! of its own: the built-in ants, and every ant the file names as another's sage, are reviewed by no one ([`settle`],
//! [`Ant::reviewed_by`]). Only an ant served with sense dreams, so only one names a sage: an ant with another provider
//! has none, and its `sage` setting warns. On a server taking credentials, a machine without the built-in sage's leaves
//! it to another ([`super::builtins`]).

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use ants_proto::UserId;
use anyhow::{Result, anyhow, bail};
use serde::Deserialize;

use super::Ant;
use crate::provider::Setting;

/// What a `sage` setting names: the ant that reviews, or none (`"none"`), which turns reviews off.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub enum Sage {
    Ant(UserId),
    None,
}

impl Default for Sage {
    /// The built-in sage.
    fn default() -> Self {
        Self::Ant(UserId::sage())
    }
}

impl FromStr for Sage {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if s == "none" {
            return Ok(Self::None);
        }
        s.parse().map(Self::Ant).map_err(|_| anyhow!("sage {s:?} is neither an ant's name nor none"))
    }
}

impl TryFrom<String> for Sage {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self> {
        s.parse()
    }
}

impl fmt::Display for Sage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ant(ant) => ant.fmt(f),
            Self::None => f.write_str("none"),
        }
    }
}

/// Who reviews the ants of a file, as [`settle`] settles it.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Settled {
    /// Whether an ant names the built-in sage, so that the file serves it.
    pub serves_sage: bool,
    /// One line for each ant whose `sage` setting names an ant while its provider is not sense, which never dreams.
    pub warnings: Vec<String>,
}

/// Settle who reviews each of `ants`, the file's top-level `sage` being `default`: mark the sages, the ants the
/// others served with sense name, and say whether one of them names the built-in sage, so that the file serves it,
/// warning of each ant with another provider naming a sage. Fails where an ant's own setting names the ant itself, or
/// names another while the ant is a sage, which has no sage of its own.
pub(super) fn settle(ants: &mut [Ant], default: &Sage) -> Result<Settled> {
    let mut warnings = Vec::new();
    for ant in ants.iter_mut() {
        ant.file_sage = default.clone();
        if ant.sage.as_ref() == Some(&Sage::Ant(ant.name.clone())) {
            bail!("ant {}: sage names the ant itself", ant.name);
        }
        if let Some(Sage::Ant(sage)) = &ant.sage
            && ant.builtin.is_none()
            && !dreams(ant)
        {
            warnings.push(format!(
                "ant {}: sage names {sage}, but the ant's provider is {}, which never dreams: no sage reviews it;                  remove its sage or serve it with provider = \"sense\"",
                ant.name,
                ant.provider_setting.name()
            ));
        }
    }
    let reviewed = || ants.iter().filter(|ant| ant.builtin.is_none() && dreams(ant));
    let mut sages: BTreeSet<UserId> = reviewed()
        .filter_map(|ant| match &ant.sage {
            Some(Sage::Ant(sage)) => Some(sage.clone()),
            _ => None,
        })
        .collect();
    if let Sage::Ant(sage) = default
        && reviewed().any(|ant| ant.sage.is_none() && ant.name != *sage && !sages.contains(&ant.name))
    {
        sages.insert(sage.clone());
    }
    for ant in ants.iter_mut() {
        ant.is_sage = sages.contains(&ant.name);
        if ant.is_sage
            && let Some(Sage::Ant(own)) = &ant.sage
        {
            bail!(
                "ant {}: sage names {own}, but {} reviews other ants and a sage has no sage of its own; remove its \
                 sage or set it to \"none\"",
                ant.name,
                ant.name
            );
        }
    }
    Ok(Settled { serves_sage: sages.contains(&UserId::sage()), warnings })
}

/// Whether `ant` dreams, so that a sage reviews it: only sense keeps dreams.
pub(super) fn dreams(ant: &Ant) -> bool {
    matches!(ant.provider_setting, Setting::Sense)
}

#[cfg(test)]
mod tests;
