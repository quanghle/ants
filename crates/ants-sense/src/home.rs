//! The layout of an ant's sense files in its home, all in its `.sense` directory ([`DIR`]) and named here alone
//! ([`Home`]): the ant's sense id and earlier names, the only ones an export of the home carries
//! ([`Home::left_out`]); the cache of its log and the marker of writes pending in it; the chain status; the skill files;
//! and the marker of an import awaited. The files written whole are written through `write()`.

use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

/// The directory of an ant's home holding its sense files.
pub const DIR: &str = ".sense";
const ID: &str = "id";
const NAMES: &str = "names";
const CACHE: &str = "cache.db";
const PENDING: &str = "cache.pending";
const CHAIN: &str = "chain.json";
const SKILLS: &str = "skills";
const IMPORTING: &str = "importing";

/// The sense files of an ant's home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Home {
    dir: PathBuf,
}

impl Home {
    /// The sense files of the ant whose home is `home`.
    pub fn new(home: &Path) -> Self {
        Self { dir: home.join(DIR) }
    }

    /// The directory holding them, the root of the Claude Code plugin of the ant's skills ([`Home::skills`]).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The file holding the ant's sense id ([`crate::identity`]).
    pub fn id(&self) -> PathBuf {
        self.dir.join(ID)
    }

    /// The file listing the ant's earlier names, one a line, the oldest first ([`crate::identity`]).
    pub fn names(&self) -> PathBuf {
        self.dir.join(NAMES)
    }

    /// The cache of the ant's log ([`crate::cache::Cache`]).
    pub fn cache(&self) -> PathBuf {
        self.dir.join(CACHE)
    }

    /// The marker of writes pending in the cache, there while any is, so a cache that holds them is never replaced.
    pub fn pending(&self) -> PathBuf {
        self.dir.join(PENDING)
    }

    /// The file holding the [`crate::log::ChainStatus`].
    pub fn status(&self) -> PathBuf {
        self.dir.join(CHAIN)
    }

    /// The directory of the ant's skill files, one directory a skill ([`crate::skill`]).
    pub fn skills(&self) -> PathBuf {
        self.dir.join(SKILLS)
    }

    /// The marker of the ant's sense data awaiting their import ([`crate::export::expect_import`]).
    pub fn importing(&self) -> PathBuf {
        self.dir.join(IMPORTING)
    }

    /// Whether the home holds a sense id.
    pub fn holds_sense_id(&self) -> bool {
        self.id().exists()
    }

    /// Whether writes are pending in the cache ([`Home::pending`]).
    pub fn has_pending_writes(&self) -> bool {
        self.pending().exists()
    }

    /// Whether the ant's sense data await their import ([`Home::importing`]).
    pub fn awaits_import(&self) -> bool {
        self.importing().exists()
    }

    /// Whether `rel`, a path relative to an ant's home, is of the sense files an export of the home leaves out: all but
    /// the sense id and the earlier names, which the ant keeps wherever it goes. The cache and the chain status are of
    /// the log on this machine's bus, and the skill files travel in sense's part, with the log ([`crate::export`]).
    pub fn left_out(rel: &Path) -> bool {
        let dir = Path::new(DIR);
        rel != dir && rel.starts_with(dir) && ![ID, NAMES].iter().any(|file| rel == dir.join(file))
    }
}

/// Write `bytes` to `file`, one of a home's sense files, whole: beside it first and synced, then renamed over it, the
/// directory synced too, so neither a crash nor a power loss leaves it part written.
pub(crate) fn write(file: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = file.parent().expect("a file in a directory");
    std::fs::create_dir_all(dir)?;
    let mut beside = file.as_os_str().to_owned();
    beside.push(".new");
    let mut new = std::fs::File::create(&beside)?;
    new.write_all(bytes)?;
    new.sync_all()?;
    std::fs::rename(&beside, file)?;
    std::fs::File::open(dir)?.sync_all()
}
