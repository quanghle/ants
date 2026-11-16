//! The harnesses a config file defines: those of its `[harnesses]` table and of the drop-in files in `harnesses.d`
//! beside it ([`dropin_dir`]), added to the built-in ones, each name defined once across the files.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use super::resolve;
use crate::harness::{Harness, Harnesses};

/// The built-in harnesses with the definitions of the config file at `path`, `definitions`, and those of its drop-in
/// files ([`dropin_files`]), their commands given as paths resolved like other paths of their file. A name defined
/// twice, in two drop-in files or in one and the config file, fails naming both files.
pub(super) fn define(definitions: BTreeMap<String, Harness>, path: &Path, home: &Path) -> Result<Harnesses> {
    let mut files = vec![(path.to_owned(), definitions)];
    let dir = dropin_dir(path);
    for file in dropin_files(&dir).with_context(|| format!("reading harness directory {}", dir.display()))? {
        files.push((file.clone(), read_dropin(&file)?));
    }
    let mut harnesses = Harnesses::builtin();
    let mut defined: BTreeMap<String, &Path> = BTreeMap::new();
    for (file, definitions) in &files {
        for name in definitions.keys() {
            if let Some(first) = defined.insert(name.clone(), file) {
                bail!("harness {name} is defined in both {} and {}", first.display(), file.display());
            }
        }
    }
    for (i, (file, mut definitions)) in files.into_iter().enumerate() {
        let base = file.parent().unwrap_or(Path::new("."));
        for harness in definitions.values_mut() {
            if harness.command_is_path() {
                harness.command = resolve(&harness.command, base, home);
            }
            if i > 0 {
                harness.file = Some(file.clone());
            }
        }
        let what = if i == 0 { "config" } else { "harness file" };
        harnesses = harnesses.define(definitions).with_context(|| format!("{what} {}", file.display()))?;
    }
    Ok(harnesses)
}

/// The directory of drop-in harness files beside the config file at `path`: `harnesses.d`.
pub fn dropin_dir(path: &Path) -> PathBuf {
    path.parent().unwrap_or(Path::new(".")).join("harnesses.d")
}

/// The drop-in harness files in `dir`, sorted by name: its `*.toml` entries other than hidden ones and directories.
/// None when there is no directory.
pub fn dropin_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if !name.starts_with('.') && path.extension().is_some_and(|ext| ext == "toml") && !path.is_dir() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// The `[harnesses.<name>]` tables of the drop-in file at `path`, its only keys.
fn read_dropin(path: &Path) -> Result<BTreeMap<String, Harness>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct DropIn {
        #[serde(default)]
        harnesses: BTreeMap<String, Harness>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading harness file {}", path.display()))?;
    let file: DropIn = toml::from_str(&text).with_context(|| format!("parsing harness file {}", path.display()))?;
    Ok(file.harnesses)
}
