//! What an ant's provider setting stands for ([`Setting`]): sugar, none, or a provider of the config file's
//! `[providers]` table ([`Definition`]), a command speaking the provider protocol or an MCP server serving its tools.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ants_proto::provider::valid_provider_name;
use anyhow::{Result, bail};
use serde::Deserialize;

/// The setting of an ant served by sugar, ants' built-in provider: the default.
pub const SUGAR: &str = "sugar";

/// The setting of an ant served by no provider, its harness's own tools serving its memory and tasks.
pub const NONE: &str = "none";

/// What an external provider's command speaks on stdio.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// The provider protocol ([`ants_proto::provider`]).
    #[default]
    Provider,
    /// MCP, the server's tools serving as a provider of tools alone.
    Mcp,
}

/// An external provider, as a `[providers.<name>]` table of the config file defines it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    /// Its name in the config, which prefixes its tools' names.
    #[serde(skip)]
    pub name: String,
    /// A path, or a bare name looked up on `PATH` when it starts.
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    /// Added to the daemon's environment.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub protocol: Protocol,
}

/// The provider an ant is served with.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Setting {
    #[default]
    Sugar,
    None,
    External(Arc<Definition>),
}

impl Setting {
    /// The setting `name` stands for: [`SUGAR`], [`NONE`], else the provider of that name among `providers`.
    pub fn named(name: &str, providers: &BTreeMap<String, Arc<Definition>>) -> Result<Self> {
        match name {
            SUGAR => Ok(Self::Sugar),
            NONE => Ok(Self::None),
            _ => match providers.get(name) {
                Some(definition) => Ok(Self::External(definition.clone())),
                None => bail!("provider {name:?} is not defined: name {SUGAR}, {NONE} or a [providers] table"),
            },
        }
    }

    /// What the config file calls it.
    pub fn name(&self) -> &str {
        match self {
            Self::Sugar => SUGAR,
            Self::None => NONE,
            Self::External(definition) => &definition.name,
        }
    }
}

/// The providers `definitions` define, each named and checked, its command, when a path, resolved with `resolve`.
pub fn define(
    definitions: BTreeMap<String, Definition>,
    resolve: impl Fn(&Path) -> PathBuf,
) -> Result<BTreeMap<String, Arc<Definition>>> {
    definitions
        .into_iter()
        .map(|(name, mut definition)| {
            check_name(&name)?;
            let command = definition.command.as_os_str();
            if command.is_empty() || command.as_encoded_bytes().contains(&0) {
                bail!("provider {name}: command must not be empty or hold a NUL");
            }
            if definition.args.iter().any(|arg| arg.contains('\0')) {
                bail!("provider {name}: an argument holds a NUL");
            }
            for (key, value) in &definition.env {
                if key.is_empty() || key.contains(['=', '\0']) || value.contains('\0') {
                    bail!("provider {name}: invalid environment variable {key:?}");
                }
            }
            if definition.command.components().count() > 1 {
                definition.command = resolve(&definition.command);
            }
            definition.name.clone_from(&name);
            Ok((name, Arc::new(definition)))
        })
        .collect()
}

/// Check `name` can name a provider of the config: a provider name, neither [`SUGAR`] nor [`NONE`], and no prefix of
/// the tools ants serves itself, which its tools' names would mix with.
fn check_name(name: &str) -> Result<()> {
    if !valid_provider_name(name) {
        bail!("provider name {name:?}: use 1-32 of a-z, 0-9 and '-', starting with a letter");
    }
    if name == SUGAR || name == NONE {
        bail!("provider name {name:?} is reserved");
    }
    if let Some(tool) = crate::mcp::own_names().iter().filter(|tool| tool.split('_').next() == Some(name)).min() {
        bail!("provider name {name:?}: its tools would read like ants' own {tool}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn definition(command: &str) -> Definition {
        Definition {
            name: String::new(),
            command: command.into(),
            args: Vec::new(),
            env: BTreeMap::new(),
            protocol: Protocol::Provider,
        }
    }

    fn define_one(name: &str, definition: Definition) -> Result<BTreeMap<String, Arc<Definition>>> {
        define(BTreeMap::from([(name.to_owned(), definition)]), |p| Path::new("/base").join(p))
    }

    #[test]
    fn names_are_checked_and_paths_resolved() {
        let defined = define_one("memo", definition("bin/memo")).unwrap();
        assert_eq!(defined["memo"].name, "memo");
        assert_eq!(defined["memo"].command, Path::new("/base/bin/memo"));
        assert_eq!(define_one("memo", definition("memo")).unwrap()["memo"].command, Path::new("memo"));
        for bad in ["Memo", "my_memo", "notes", "sugar", "none", "send", "read", ""] {
            assert!(define_one(bad, definition("memo")).is_err(), "{bad}");
        }
        assert!(define_one("memo", definition("")).is_err());
        let mut env = definition("memo");
        env.env.insert("A=B".into(), "x".into());
        assert!(define_one("memo", env).is_err());
    }

    #[test]
    fn a_setting_names_sugar_none_or_a_provider() {
        let providers = define_one("memo", definition("memo")).unwrap();
        assert_eq!(Setting::named("sugar", &providers).unwrap(), Setting::Sugar);
        assert_eq!(Setting::named("none", &providers).unwrap(), Setting::None);
        assert_eq!(Setting::named("memo", &providers).unwrap().name(), "memo");
        assert!(Setting::named("other", &providers).is_err());
    }
}
