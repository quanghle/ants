//! Who reviews each ant: the `sage` setting of its `[[ant]]` table, one of its shared settings, else the file's
//! top-level `sage`, which names the built-in sage ([`super::builtin_sage`]) unless set ([`Sage`]). A sage has no sage
//! of its own: the built-in ants, and every ant the file names as another's sage, are reviewed by no one ([`settle`],
//! [`Ant::reviewed_by`]). On a server taking credentials, a machine without the built-in sage's leaves it to another
//! ([`served_here`]).

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use ants_proto::UserId;
use anyhow::{Result, anyhow, bail};
use serde::Deserialize;

use super::{Ant, HostSettings};

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
/// others name, and return whether one of the others names the built-in sage while no ant of the name is listed, so
/// that the file serves it. Fails where an ant's own setting names the ant itself, or names another while the ant is
/// a sage, which has no sage of its own.
pub(super) fn settle(ants: &mut [Ant], default: &Sage) -> Result<bool> {
    for ant in ants.iter_mut() {
        ant.file_sage = default.clone();
        if ant.sage.as_ref() == Some(&Sage::Ant(ant.name.clone())) {
            bail!("ant {}: sage names the ant itself", ant.name);
        }
    }
    let reviewed = || ants.iter().filter(|ant| !ant.builtin);
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
    let sage = UserId::sage();
    Ok(sages.contains(&sage) && !ants.iter().any(|ant| ant.name == sage))
}

/// Whether the machine serves the built-in `sage` the file names ([`super::builtin_sage`]): unless the server takes
/// credentials and the sage has none on the machine, it being left to another machine then. A server takes
/// credentials where the machine's `host` credentials or one of the file's `ants` has some: only `ants auth` issues
/// them, and a server without authentication has its ants connect as no one, as the sage then does too.
pub(super) fn served_here(sage: &Ant, ants: &[Ant], host: &HostSettings) -> bool {
    sage.creds.is_some() || (host.creds.is_none() && ants.iter().all(|ant| ant.creds.is_none()))
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
            pairs(&[("dev-a", Some("sage")), ("dev-b", Some("sage")), ("messenger", None), ("sage", None)]),
            "the built-in ants have no sage"
        );
        let ants = load(&text).unwrap().ants;
        let sage = ants.iter().find(|ant| ant.name == UserId::sage()).unwrap();
        assert!(sage.builtin && sage.is_sage);
        assert_eq!(sage.harness.as_deref(), Some("copilot"), "the file's top-level harness");

        let home = ants_test_support::scratch_dir("sage-home").canonicalize().unwrap();
        let config = builtin_sage(Some("copilot"), &home).into_config("nats://unused", &Harnesses::builtin()).unwrap();
        assert!(config.presence.defer_to_others, "served only while holding the sage's colony entry");
        assert_eq!(config.harness.workdir, home.join(".ants/sage").canonicalize().unwrap());
        assert_eq!(config.policy.sage, None);
        std::fs::remove_dir_all(&home).unwrap();

        let names: Vec<String> = load("").unwrap().ants.iter().map(|ant| ant.name.to_string()).collect();
        assert_eq!(names, ["messenger"], "the built-in messenger names no sage");
    }

    #[test]
    fn the_file_serves_no_builtin_sage_while_no_ant_names_it_or_it_lists_one() {
        let off = format!("sage = \"none\"\n{}{}", ant("dev-a", ""), ant("dev-b", "sage = \"none\""));
        assert_eq!(reviewers(&off), pairs(&[("dev-a", None), ("dev-b", None), ("messenger", None)]));

        let each_off = format!("{}{}", ant("dev-a", "sage = \"none\""), ant("dev-b", "sage = \"none\""));
        assert_eq!(reviewers(&each_off), pairs(&[("dev-a", None), ("dev-b", None), ("messenger", None)]));

        let one_on = format!("sage = \"none\"\n{}{}", ant("dev-a", ""), ant("dev-b", "sage = \"sage\""));
        assert_eq!(
            reviewers(&one_on),
            pairs(&[("dev-a", None), ("dev-b", Some("sage")), ("messenger", None), ("sage", None)])
        );

        let listed = format!("{}{}", ant("dev-a", ""), ant("sage", "harness = \"claude\""));
        assert_eq!(reviewers(&listed), pairs(&[("dev-a", Some("sage")), ("sage", None), ("messenger", None)]));
        let ants = load(&listed).unwrap().ants;
        let sage = ants.iter().find(|ant| ant.name == UserId::sage()).unwrap();
        assert_eq!((sage.builtin, sage.harness.as_deref()), (false, Some("claude")), "the listed sage, alone");
    }

    #[test]
    fn an_ant_named_as_a_sage_has_none_of_its_own() {
        let other = format!("{}{}{}", ant("dev-a", "sage = \"lead\""), ant("dev-b", ""), ant("lead", ""));
        assert_eq!(
            reviewers(&other),
            pairs(&[
                ("dev-a", Some("lead")),
                ("dev-b", Some("sage")),
                ("lead", None),
                ("messenger", None),
                ("sage", None)
            ])
        );

        let top = format!("sage = \"lead\"\n{}{}", ant("dev-a", ""), ant("lead", ""));
        assert_eq!(reviewers(&top), pairs(&[("dev-a", Some("lead")), ("lead", None), ("messenger", None)]));

        let elsewhere = ant("dev-a", "sage = \"reviewer\"");
        assert_eq!(
            reviewers(&elsewhere),
            pairs(&[("dev-a", Some("reviewer")), ("messenger", None)]),
            "a sage the file does not list may be served on another machine"
        );

        let chained = format!("{}{}", ant("dev-a", "sage = \"lead\""), ant("lead", "sage = \"sage\""));
        let e = err(&chained);
        assert!(e.contains("ant lead: sage names sage, but lead reviews other ants and a sage has no sage"), "{e}");
        let crossed = format!("sage = \"lead\"\n{}{}", ant("dev-a", ""), ant("lead", "sage = \"dev-a\""));
        assert_eq!(
            reviewers(&crossed),
            pairs(&[("dev-a", None), ("lead", Some("dev-a")), ("messenger", None)]),
            "lead's own setting makes dev-a a sage, over the file's naming lead"
        );
        let off = format!("{}{}", ant("dev-a", "sage = \"lead\""), ant("lead", "sage = \"none\""));
        assert_eq!(reviewers(&off), pairs(&[("dev-a", Some("lead")), ("lead", None), ("messenger", None)]));
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

    #[test]
    fn on_a_server_taking_credentials_the_builtin_sage_is_served_only_where_its_own_are() {
        let home = ants_test_support::scratch_dir("sage-creds").canonicalize().unwrap();
        let auth = home.join(".config/ants/auth");
        std::fs::create_dir_all(&auth).unwrap();
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let load = |text: &str| {
            let loaded = parse(&format!("harness = \"copilot\"\n{text}"), &dir.join("config.toml"), &home).unwrap();
            let names: Vec<String> = loaded.ants.iter().map(|ant| ant.name.to_string()).collect();
            (names, loaded)
        };
        let bare = format!("messenger = false\n{}", ant("dev-b", ""));
        let (names, open) = load(&bare);
        assert_eq!(names, ["dev-b", "sage"], "without credentials anywhere the sage connects as no one, as dev-b does");
        assert_eq!(open.sage_elsewhere, None);

        let leaf = format!("messenger = false\n{}", ant("dev-b", "creds = \"~/.config/ants/auth/dev-b.creds\""));
        let (names, elsewhere) = load(&leaf);
        assert_eq!(names, ["dev-b"], "dev-b's credentials tell the server takes some, and the sage has none here");
        assert_eq!(elsewhere.sage_elsewhere, Some(auth.join("sage.creds")));
        assert_eq!(elsewhere.ants[0].reviewed_by(), Some(UserId::sage()), "dev-b names the sage served elsewhere");

        std::fs::write(auth.join("messenger.creds"), "").unwrap();
        assert_eq!(load(&ant("dev-b", "")).0, ["dev-b", "messenger"], "so do the built-in messenger's");
        std::fs::remove_file(auth.join("messenger.creds")).unwrap();
        std::fs::write(auth.join("host.creds"), "").unwrap();
        assert_eq!(load(&bare).0, ["dev-b"], "and the machine's host credentials");

        std::fs::write(auth.join("sage.creds"), "").unwrap();
        let (names, served) = load(&leaf);
        assert_eq!(names, ["dev-b", "sage"]);
        assert_eq!((served.ants[1].creds.as_deref(), served.sage_elsewhere), (Some(&*auth.join("sage.creds")), None));
        std::fs::remove_dir_all(&home).unwrap();
    }
}
