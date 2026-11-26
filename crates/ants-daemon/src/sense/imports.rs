//! The import of an ant's harness memories into its sense data, once per ant ([`ants_sense::memory::import`]): run by
//! the ant's schedule as it first reads the ant's items in a daemon serving it ([`super::scheduler`]), so at the first
//! use of the ant's sense data after it was added, or after its daemon took the import on. An ant whose items record
//! the import is not read again; one that recorded none has its memory files and bd memories read as an admin's read
//! of its memories reads them ([`crate::admin::harness_memories`]), within the same caps.
//!
//! A file under the ant's workdir is named by its path there, and its memories are of the workdir's repository, as are
//! the bd memories; a file under the home directory is named under `~`, and its memories, as all of an ant whose
//! workdir is in no repository with a remote, are the ant's own (`self`).

use std::path::{Path, PathBuf};

use ants_proto::provider::{CallContext, Origin, Starter};
use ants_proto::{MemoryFile, ThreadId, UserId};
use ants_sense::memory::Scope;
use ants_sense::memory::import::{self, Imported, Source};
use ants_sense::store::Items;
use anyhow::Result;
use chrono::Utc;

use crate::admin::{MemoriesEnv, harness_memories};
use crate::daemon::Config;

/// Where the harness memories of an ant are read from.
#[derive(Debug, Clone)]
pub(crate) struct HarnessMemories {
    /// The harness, by its name in the config.
    harness: String,
    globs: Vec<String>,
    workdir: PathBuf,
    home: PathBuf,
    /// The database named by `BD_DB` in the ant's environment: its own `env`, its harness's, the daemon's.
    bd_db: Option<String>,
    env: MemoriesEnv,
}

impl HarnessMemories {
    /// Those of the ant `config` serves; none without a home directory, which `~` stands for.
    pub(crate) fn of(config: &Config) -> Option<Self> {
        let env = MemoriesEnv::current()?;
        let harness = &config.harness.harness;
        let bd_db = config.harness.env.get("BD_DB").or_else(|| harness.env.get("BD_DB")).or(env.bd_db.as_ref());
        Some(Self {
            harness: harness.name().to_owned(),
            globs: harness.memory_files().to_vec(),
            workdir: config.harness.workdir.clone(),
            home: config.storage.home.clone(),
            bd_db: bd_db.cloned(),
            env,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(globs: &[&str], workdir: &Path, home: &Path, bd: (&Path, Option<&str>)) -> Self {
        let env = MemoriesEnv { home: home.to_owned(), bd: bd.0.into(), bd_db: None };
        Self {
            harness: "copilot".into(),
            globs: globs.iter().map(|&glob| glob.to_owned()).collect(),
            workdir: workdir.to_owned(),
            home: home.join("ant"),
            bd_db: bd.1.map(Into::into),
            env,
        }
    }
}

/// Import the harness memories `from` of `ant`, served on `machine`, into `items`, unless they hold the import's
/// record already.
pub(crate) async fn import(items: &dyn Items, from: &HarnessMemories, ant: &UserId, machine: &str) -> Result<()> {
    if import::imported(items).await?.is_some() {
        return Ok(());
    }
    let (files, bd, skipped) =
        harness_memories(&from.globs, &from.workdir, &from.home, &from.env, from.bd_db.as_deref()).await;
    let repo = crate::provider::context::repository(&from.workdir);
    let shared = repo.clone().map_or(Scope::Own, Scope::Repo);
    let mut sources: Vec<Source> = files.into_iter().map(|file| source(file, from, &shared)).collect();
    if let Some(bd) = &bd {
        if let Some(error) = &bd.error {
            tracing::warn!(%ant, db = bd.db, error, "the bd memories are left out of the import into sense");
        }
        sources.extend(bd.memories.iter().map(|memory| Source {
            from: Imported::Bd { db: bd.db.clone(), key: memory.key.clone() },
            scope: shared.clone(),
            text: memory.content.clone(),
        }));
    }
    let identity = ants_sense::identity::card(items).await?.map_or_default(|card| card.identity);
    let writer = CallContext {
        ant: ant.clone(),
        identity,
        thread: ThreadId::generate(),
        origin: Origin { by: Starter::Provider { name: ants_sense::NAME.to_owned() }, channel: None },
        harness: from.harness.clone(),
        model: None,
        machine: machine.to_owned(),
        repo,
        at: Utc::now(),
    };
    if let Some(record) = import::import(items, &sources, &writer).await? {
        let (imported, duplicates, over, left_out) = (record.imported, record.duplicates, record.over, skipped.len());
        tracing::info!(%ant, sources = record.sources, imported, duplicates, over, left_out, "imported the harness memories into sense");
    }
    Ok(())
}

/// The source of `file`, named by its path in the workdir or under `~`, its memories in `shared` when in the workdir.
fn source(file: MemoryFile, from: &HarnessMemories, shared: &Scope) -> Source {
    let path = Path::new(&file.path);
    // The globs match from their bases with links resolved.
    let under = |dir: &Path| {
        let real = dir.canonicalize().unwrap_or_else(|_| dir.to_owned());
        path.strip_prefix(&real).or_else(|_| path.strip_prefix(dir)).ok().map(|rel| rel.display().to_string())
    };
    let (path, scope) = if let Some(rel) = under(&from.workdir) {
        (rel, shared.clone())
    } else if let Some(rel) = under(&from.env.home) {
        (format!("~/{rel}"), Scope::Own)
    } else {
        (file.path.clone(), Scope::Own)
    };
    Source { from: Imported::File { path }, scope, text: file.text }
}

#[cfg(test)]
mod tests;
