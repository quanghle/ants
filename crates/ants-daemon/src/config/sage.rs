//! Who reviews each ant: the `sage` setting of its `[[ant]]` table, one of its shared settings, else the file's
//! top-level `sage`, which names the built-in sage ([`super::builtin_sage`]) unless set ([`Sage`]). A sage has no sage
//! of its own: the built-in ants, and every ant the file names as another's sage, are reviewed by no one ([`settle`],
//! [`Ant::reviewed_by`]). On a server taking credentials, a machine without the built-in sage's leaves it to another
//! ([`super::builtins`]).

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use ants_proto::UserId;
use anyhow::{Result, anyhow, bail};
use serde::Deserialize;

use super::Ant;

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

/// Settle who reviews each of `ants`, the file's top-level `sage` being `default`: mark the sages, the ants the
/// others name, and return whether one of the others names the built-in sage, so that the file serves it. Fails where
/// an ant's own setting names the ant itself, or names another while the ant is
/// a sage, which has no sage of its own.
pub(super) fn settle(ants: &mut [Ant], default: &Sage) -> Result<bool> {
    for ant in ants.iter_mut() {
        ant.file_sage = default.clone();
        if ant.sage.as_ref() == Some(&Sage::Ant(ant.name.clone())) {
            bail!("ant {}: sage names the ant itself", ant.name);
        }
    }
    let reviewed = || ants.iter().filter(|ant| ant.builtin.is_none());
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
    Ok(sages.contains(&UserId::sage()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::{Loaded, builtin_sage, parse};
    use crate::harness::Harnesses;

    /// `text` with `harness = "copilot"` on top, parsed as if it sat in this crate's directory.
    fn load(text: &str) -> Result<Loaded> {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        parse(&format!("harness = \"copilot\"\n{text}"), &dir.join("config.toml"), dir.parent().unwrap())
    }

    /// Each ant's name, with the ant reviewing it.
    fn reviewers(text: &str) -> Vec<(String, Option<String>)> {
        let loaded = load(text).unwrap();
        loaded.ants.iter().map(|ant| (ant.name.to_string(), ant.reviewed_by().map(|s| s.to_string()))).collect()
    }

    fn err(text: &str) -> String {
        format!("{:#}", load(text).unwrap_err())
    }

    fn ant(name: &str, more: &str) -> String {
        format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\n{more}\n")
    }

    fn pairs(list: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        list.iter().map(|(ant, sage)| ((*ant).to_owned(), sage.map(str::to_owned))).collect()
    }

    #[test]
    fn every_ant_names_the_builtin_sage_by_default_which_the_file_then_serves() {
        let text = format!("{}{}", ant("dev-a", ""), ant("dev-b", ""));
        assert_eq!(
            reviewers(&text),
            pairs(&[
                ("dev-a", Some("system-sage")),
                ("dev-b", Some("system-sage")),
                ("system-messenger", None),
                ("system-sage", None)
            ]),
            "the built-in ants have no sage"
        );
        let ants = load(&text).unwrap().ants;
        let sage = ants.iter().find(|ant| ant.name == UserId::sage()).unwrap();
        assert_eq!(sage.builtin, Some(ants_proto::BuiltinAnt::Sage));
        assert!(sage.is_sage);
        assert_eq!(sage.harness.as_deref(), Some("copilot"), "the file's top-level harness");

        let home = ants_test_support::scratch_dir("sage-home").canonicalize().unwrap();
        let config = builtin_sage(Some("copilot"), &home).into_config("nats://unused", &Harnesses::builtin()).unwrap();
        assert_eq!(config.presence.builtin, Some(ants_proto::BuiltinAnt::Sage));
        assert_eq!(config.harness.workdir, home.join(".ants/agents/system-sage").canonicalize().unwrap());
        assert_eq!(config.policy.sage, None);
        std::fs::remove_dir_all(&home).unwrap();

        let names: Vec<String> = load("").unwrap().ants.iter().map(|ant| ant.name.to_string()).collect();
        assert_eq!(names, ["system-messenger"], "the built-in messenger names no sage");
    }

    #[test]
    fn the_file_serves_the_builtin_sage_only_while_an_ant_names_it() {
        let off = format!("sage = \"none\"\n{}{}", ant("dev-a", ""), ant("dev-b", "sage = \"none\""));
        assert_eq!(reviewers(&off), pairs(&[("dev-a", None), ("dev-b", None), ("system-messenger", None)]));

        let each_off = format!("{}{}", ant("dev-a", "sage = \"none\""), ant("dev-b", "sage = \"none\""));
        assert_eq!(reviewers(&each_off), pairs(&[("dev-a", None), ("dev-b", None), ("system-messenger", None)]));

        let one_on = format!("sage = \"none\"\n{}{}", ant("dev-a", ""), ant("dev-b", "sage = \"system-sage\""));
        assert_eq!(
            reviewers(&one_on),
            pairs(&[
                ("dev-a", None),
                ("dev-b", Some("system-sage")),
                ("system-messenger", None),
                ("system-sage", None)
            ])
        );

        let listed = format!("sage = \"sage\"\n{}{}", ant("dev-a", ""), ant("sage", "harness = \"claude\""));
        assert_eq!(reviewers(&listed), pairs(&[("dev-a", Some("sage")), ("sage", None), ("system-messenger", None)]));
        let ants = load(&listed).unwrap().ants;
        let sage = ants.iter().find(|ant| ant.name.as_str() == "sage").unwrap();
        assert_eq!((sage.builtin, sage.harness.as_deref()), (None, Some("claude")), "an ordinary ant can be assigned");
    }

    #[test]
    fn an_ant_named_as_a_sage_has_none_of_its_own() {
        let other = format!("{}{}{}", ant("dev-a", "sage = \"lead\""), ant("dev-b", ""), ant("lead", ""));
        assert_eq!(
            reviewers(&other),
            pairs(&[
                ("dev-a", Some("lead")),
                ("dev-b", Some("system-sage")),
                ("lead", None),
                ("system-messenger", None),
                ("system-sage", None)
            ])
        );

        let top = format!("sage = \"lead\"\n{}{}", ant("dev-a", ""), ant("lead", ""));
        assert_eq!(reviewers(&top), pairs(&[("dev-a", Some("lead")), ("lead", None), ("system-messenger", None)]));

        let elsewhere = ant("dev-a", "sage = \"reviewer\"");
        assert_eq!(
            reviewers(&elsewhere),
            pairs(&[("dev-a", Some("reviewer")), ("system-messenger", None)]),
            "a sage the file does not list may be served on another machine"
        );

        let chained = format!("{}{}", ant("dev-a", "sage = \"lead\""), ant("lead", "sage = \"sage\""));
        let e = err(&chained);
        assert!(e.contains("ant lead: sage names sage, but lead reviews other ants and a sage has no sage"), "{e}");
        let crossed = format!("sage = \"lead\"\n{}{}", ant("dev-a", ""), ant("lead", "sage = \"dev-a\""));
        assert_eq!(
            reviewers(&crossed),
            pairs(&[("dev-a", None), ("lead", Some("dev-a")), ("system-messenger", None)]),
            "lead's own setting makes dev-a a sage, over the file's naming lead"
        );
        let off = format!("{}{}", ant("dev-a", "sage = \"lead\""), ant("lead", "sage = \"none\""));
        assert_eq!(reviewers(&off), pairs(&[("dev-a", Some("lead")), ("lead", None), ("system-messenger", None)]));
    }

    #[test]
    fn a_sage_setting_names_an_ant_or_none() {
        assert!(err(&ant("dev-a", "sage = \"dev-a\"")).contains("ant dev-a: sage names the ant itself"));
        let e = err(&ant("dev-a", "sage = \"Lead\""));
        assert!(e.contains("sage \"Lead\" is neither an ant's name nor none"), "{e}");
        assert!(err("sage = \"\"").contains("sage \"\" is neither"));
        assert_eq!("none".parse::<Sage>().unwrap().to_string(), "none");
        assert_eq!("lead".parse::<Sage>().unwrap().to_string(), "lead");
    }
}
