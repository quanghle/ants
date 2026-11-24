//! The harnesses an ant can run, by name: the built-in definitions and those a config file adds.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, LazyLock};

use anyhow::{Result, anyhow};
use serde::Deserialize;

use super::Harness;
use super::definition::Variant;

/// The built-in definitions, in the same form as a config file's `[harnesses.<name>]` tables.
pub const BUILTIN_HARNESSES: &str = include_str!("../harnesses.toml");

/// What a definition of a native protocol takes for what it does not set, by protocol name.
const PROTOCOL_DEFAULTS: &str = include_str!("../protocols.toml");

/// A native protocol's defaults ([`PROTOCOL_DEFAULTS`]).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Defaults {
    memory_files: Option<Vec<String>>,
    review: Option<Variant>,
    window: Option<u64>,
}

static DEFAULTS: LazyLock<BTreeMap<String, Defaults>> = LazyLock::new(|| {
    #[derive(Deserialize)]
    struct Tables {
        protocols: BTreeMap<String, Defaults>,
    }
    toml::from_str::<Tables>(PROTOCOL_DEFAULTS).expect("protocol defaults parse").protocols
});

/// The harnesses ants can run, by name: the built-in ones and those a config file defines.
#[derive(Debug, Clone, PartialEq)]
pub struct Harnesses(Arc<BTreeMap<String, Arc<Harness>>>);

static BUILTIN: LazyLock<Harnesses> = LazyLock::new(|| {
    Harnesses(Arc::default())
        .define(parse_definitions(BUILTIN_HARNESSES).expect("built-in harnesses parse"))
        .expect("built-in harnesses are valid")
});

/// `[harnesses.<name>]` tables, as a config file holds them.
pub fn parse_definitions(text: &str) -> Result<BTreeMap<String, Harness>> {
    #[derive(Deserialize)]
    struct Tables {
        harnesses: BTreeMap<String, Harness>,
    }
    Ok(toml::from_str::<Tables>(text)?.harnesses)
}

impl Default for Harnesses {
    fn default() -> Self {
        Self::builtin()
    }
}

impl Harnesses {
    /// The built-in harnesses alone ([`BUILTIN_HARNESSES`]).
    pub fn builtin() -> Self {
        BUILTIN.clone()
    }

    /// These harnesses with `definitions` added, each replacing a harness of the same name whole. A definition of a
    /// native protocol takes that protocol's memory files, review and window for those it does not set; one of ACP
    /// keeps the memory files of the harness it replaces unless it sets them.
    pub fn define(&self, definitions: BTreeMap<String, Harness>) -> Result<Self> {
        let mut all = (*self.0).clone();
        for (name, mut harness) in definitions {
            harness.name.clone_from(&name);
            if let Some(defaults) = DEFAULTS.get(harness.protocol.name()) {
                harness.memory_files = harness.memory_files.or_else(|| defaults.memory_files.clone());
                harness.review = harness.review.or_else(|| defaults.review.clone());
                harness.window = harness.window.or(defaults.window);
            } else if harness.memory_files.is_none() {
                harness.memory_files = all.get(&name).and_then(|h| h.memory_files.clone());
            }
            harness.validate()?;
            all.insert(name, Arc::new(harness));
        }
        Ok(Self(Arc::new(all)))
    }

    /// The names of the harnesses, in name order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// The harness named `name`, else, with none named, an [`Unset`] error.
    pub fn named(&self, name: Option<&str>) -> Result<Arc<Harness>> {
        match name {
            Some(name) => self.get(name),
            None => Err(Unset.into()),
        }
    }

    /// The harness named `name`.
    pub fn get(&self, name: &str) -> Result<Arc<Harness>> {
        self.0.get(name).cloned().ok_or_else(|| {
            let known: Vec<_> = self.0.keys().map(String::as_str).collect();
            anyhow!(
                "unknown harness {name:?}, expected one of {} or a [harnesses.{name}] table in the config file or in \
                 a harnesses.d file beside it",
                known.join(", ")
            )
        })
    }
}

/// An ant names no harness, nor does its config file: ants has no default one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unset;

impl fmt::Display for Unset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let builtin: Vec<_> = BUILTIN.0.keys().map(String::as_str).collect();
        write!(
            f,
            "no harness set: add `harness = \"NAME\"` at the top of the config file or to the ant's entry (`ants \
             daemon --ant` takes --harness NAME), NAME being a built-in harness ({}) or a [harnesses.NAME] table; \
             `ants setup` lists the ones installed",
            builtin.join(", ")
        )
    }
}

impl std::error::Error for Unset {}

impl Unset {
    /// Whether `error` is, or was caused by, an [`Unset`].
    pub fn caused(error: &anyhow::Error) -> bool {
        error.chain().any(<dyn std::error::Error>::is::<Self>)
    }
}
