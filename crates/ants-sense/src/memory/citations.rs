//! Citation baselines and rechecks, confined to the caller's git repository. Hashes cover exact UTF-8 bytes,
//! including line endings; a recheck never replaces a baseline or makes a memory active again.

use std::fmt;
use std::path::{Component, Path};

use data_encoding::HEXLOWER;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncReadExt};

use super::{Citation, MEMORY, MemoryItem, Status, normalize_remote, remote_url};
use crate::items::{ItemId, Values};
use crate::store::{Items, StoreError, read_modify_write};

const RECHECK_BY: &str = "sense:citation-recheck";
const MAX_GIT_METADATA_BYTES: u64 = 64 * 1024;

/// Why cited lines could not be verified against their recorded hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CitationProblem {
    Changed,
    Missing,
    InvalidPath,
    OutsideRoot,
    InvalidRange,
    InvalidHash,
    NotUtf8,
    NotFile,
    Unreadable,
}

impl fmt::Display for CitationProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Changed => "cited lines changed",
            Self::Missing => "cited file is missing",
            Self::InvalidPath => "citation path is not a safe repository-relative path",
            Self::OutsideRoot => "cited file resolves outside the repository",
            Self::InvalidRange => "cited line range is outside the file or malformed",
            Self::InvalidHash => "recorded citation hash is not SHA-256",
            Self::NotUtf8 => "cited file is not UTF-8",
            Self::NotFile => "citation does not name a regular file",
            Self::Unreadable => "cited file could not be read",
        })
    }
}

/// A persistent warning about the zero-based `citation` in [`super::Memory::citations`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CitationWarning {
    pub citation: usize,
    pub reason: CitationProblem,
}

/// The result of [`recheck_citations`]. Counts cover citations of live memories only.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CitationRecheck {
    /// Citations inspected in this repository, including missing or invalid content.
    pub checked: usize,
    /// Inspected citations whose recorded hashes still match.
    pub unchanged: usize,
    /// Citations without a baseline or belonging to an unavailable repository.
    pub skipped: usize,
    /// Memories changed from active to needs_review.
    pub marked: usize,
    /// Memories with citation warnings after this recheck, including those already needing review.
    pub needs_review: Vec<ItemId>,
}

/// Rehash citations of live memories in `items` against the git repository containing `workdir`. Only citations of
/// that repository are checked. Missing, unreadable, unsafe or changed content marks the memory needs_review and
/// records why in [`super::Memory::citation_warnings`], preserving its baseline. Other repositories and citations
/// without recorded hashes are not guessed. Writes use revision guards and retry concurrent changes.
///
/// Run before showing memories and during maintenance. An unavailable git repository leaves its citations alone.
pub async fn recheck_citations(items: &dyn Items, workdir: &Path) -> Result<CitationRecheck, StoreError> {
    let memories: Vec<_> = items
        .list(MEMORY, usize::MAX)
        .await?
        .into_iter()
        .filter_map(MemoryItem::of)
        .filter(|found| found.memory.status.live() && !found.memory.citations.is_empty())
        .collect();
    if memories.is_empty() {
        return Ok(CitationRecheck::default());
    }
    let repository = Repository::at(workdir).await;
    let mut summary = CitationRecheck::default();
    for found in memories {
        let Some(repository) = &repository else {
            summary.skipped += found.memory.citations.len();
            continue;
        };
        let checked = recheck(items, found, repository).await?;
        summary.checked += checked.checked;
        summary.unchanged += checked.unchanged;
        summary.skipped += checked.skipped;
        summary.marked += checked.marked;
        summary.needs_review.extend(checked.needs_review);
    }
    Ok(summary)
}

/// Recheck the citations of `found`, read again when a change of it is refused as another writer changed it meanwhile.
async fn recheck(items: &dyn Items, found: MemoryItem, repository: &Repository) -> Result<CitationRecheck, StoreError> {
    let id = &found.id().clone();
    let mut read = Some(found);
    read_modify_write(|| {
        let read = read.take();
        async move {
            let found = match read {
                Some(found) => found,
                None => match items.get(id).await?.and_then(MemoryItem::of) {
                    Some(found) => found,
                    None => return Ok(CitationRecheck::default()),
                },
            };
            recheck_once(items, &found, repository).await
        }
    })
    .await
}

async fn recheck_once(
    items: &dyn Items,
    found: &MemoryItem,
    repository: &Repository,
) -> Result<CitationRecheck, StoreError> {
    let memory = &found.memory;
    if !memory.status.live() {
        return Ok(CitationRecheck::default());
    }
    let mut summary = CitationRecheck::default();
    let mut warnings = Vec::new();
    let mut problem = false;
    for (index, citation) in memory.citations.iter().enumerate() {
        if !repository.matches(citation) {
            summary.skipped += 1;
            warnings.extend(memory.citation_warnings.iter().filter(|warning| warning.citation == index).cloned());
            continue;
        }
        let result = repository.snapshot(citation).await;
        let reason = match (result, &citation.hash) {
            (Ok(_), None) => {
                summary.skipped += 1;
                warnings.extend(memory.citation_warnings.iter().filter(|warning| warning.citation == index).cloned());
                continue;
            }
            (Ok(_), Some(hash)) if !valid_hash(hash) => Some(CitationProblem::InvalidHash),
            (Ok(snapshot), Some(hash)) if snapshot.hash.eq_ignore_ascii_case(hash) => {
                summary.unchanged += 1;
                None
            }
            (Ok(_), Some(_)) => Some(CitationProblem::Changed),
            (Err(reason), _) => Some(reason),
        };
        summary.checked += 1;
        if let Some(reason) = reason {
            problem = true;
            warnings.push(CitationWarning { citation: index, reason });
        }
    }
    if problem || (!warnings.is_empty() && memory.status == Status::NeedsReview) {
        summary.needs_review.push(found.id().clone());
        summary.marked = usize::from(problem && memory.status == Status::Active);
    }
    let mut values = Values::new();
    if problem && memory.status != Status::NeedsReview {
        values.insert("status".into(), json!(Status::NeedsReview));
    }
    if warnings != memory.citation_warnings {
        values.insert("citation_warnings".into(), if warnings.is_empty() { Value::Null } else { json!(warnings) });
    }
    if values.is_empty() {
        return Ok(summary);
    }
    items.change(found.id(), values, Some(found.item.revision), RECHECK_BY).await?;
    Ok(summary)
}

/// Fill missing metadata from the actual repository, not a caller's claimed scope. Supplied baselines are compared,
/// not silently replaced; unavailable content never receives a fabricated hash or range.
pub(super) async fn record(citations: &mut [Citation], workdir: &Path) -> Vec<CitationWarning> {
    if citations.is_empty() {
        return Vec::new();
    }
    let Some(repository) = Repository::at(workdir).await else { return Vec::new() };
    let mut warnings = Vec::new();
    for (index, citation) in citations.iter_mut().enumerate() {
        if citation.remote.as_ref().is_some_and(|remote| remote != &repository.remote) {
            continue;
        }
        citation.remote = Some(repository.remote.clone());
        let reason = match repository.snapshot(citation).await {
            Ok(snapshot) => {
                citation.start = Some(snapshot.start);
                citation.end = Some(snapshot.end);
                match &citation.hash {
                    Some(hash) if !snapshot.hash.eq_ignore_ascii_case(hash) => Some(CitationProblem::Changed),
                    Some(_) => None,
                    None => {
                        citation.hash = Some(snapshot.hash);
                        None
                    }
                }
            }
            Err(reason) => Some(reason),
        };
        if let Some(reason) = reason {
            warnings.push(CitationWarning { citation: index, reason });
        }
    }
    warnings
}

pub(super) fn safe_path(path: &str) -> Option<String> {
    if path.contains(|c: char| c.is_control() || matches!(c, '\\' | ':')) {
        return None;
    }
    let mut relative = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::Normal(part) => relative.push(part.to_str()?),
            Component::CurDir => {}
            _ => return None,
        }
    }
    (!relative.is_empty()).then(|| relative.join("/"))
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

struct Repository {
    root: std::path::PathBuf,
    remote: String,
}

impl Repository {
    async fn at(workdir: &Path) -> Option<Self> {
        let workdir = fs::canonicalize(workdir).await.ok()?;
        for root in workdir.ancestors() {
            let dot_git = root.join(".git");
            let metadata = match fs::metadata(&dot_git).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(_) => return None,
            };
            let git_dir = if metadata.is_file() {
                let text = git_metadata(&dot_git).await?;
                root.join(text.trim().strip_prefix("gitdir:")?.trim())
            } else if metadata.is_dir() {
                dot_git
            } else {
                return None;
            };
            let common = match git_metadata(&git_dir.join("commondir")).await {
                Some(text) => git_dir.join(text.trim()),
                None => git_dir,
            };
            let config = git_metadata(&common.join("config")).await?;
            let remote = remote_url(&config).and_then(normalize_remote)?;
            return Some(Self { root: root.to_owned(), remote });
        }
        None
    }

    fn matches(&self, citation: &Citation) -> bool {
        citation.remote.as_deref().and_then(normalize_remote).as_ref() == Some(&self.remote)
    }

    async fn snapshot(&self, citation: &Citation) -> Result<Snapshot, CitationProblem> {
        let path = safe_path(&citation.path).ok_or(CitationProblem::InvalidPath)?;
        let path = fs::canonicalize(self.root.join(path)).await.map_err(file_error)?;
        if !path.starts_with(&self.root) {
            return Err(CitationProblem::OutsideRoot);
        }
        let metadata = fs::metadata(&path).await.map_err(file_error)?;
        if !metadata.is_file() {
            return Err(CitationProblem::NotFile);
        }
        let file = File::open(path).await.map_err(file_error)?;
        snapshot(&mut file.take(metadata.len()), citation.start, citation.end).await
    }
}

async fn git_metadata(path: &Path) -> Option<String> {
    let metadata = fs::metadata(path).await.ok()?;
    if !metadata.is_file() || metadata.len() > MAX_GIT_METADATA_BYTES {
        return None;
    }
    let file = File::open(path).await.ok()?;
    let mut text = String::new();
    file.take(MAX_GIT_METADATA_BYTES + 1).read_to_string(&mut text).await.ok()?;
    (text.len() as u64 <= MAX_GIT_METADATA_BYTES).then_some(text)
}

fn file_error(error: std::io::Error) -> CitationProblem {
    match error.kind() {
        std::io::ErrorKind::NotFound => CitationProblem::Missing,
        _ => CitationProblem::Unreadable,
    }
}

struct Snapshot {
    start: u32,
    end: u32,
    hash: String,
}

async fn snapshot(
    file: &mut (impl AsyncRead + Unpin),
    start: Option<u32>,
    end: Option<u32>,
) -> Result<Snapshot, CitationProblem> {
    let end = match (start, end) {
        (Some(0), _) | (None, Some(_)) => return Err(CitationProblem::InvalidRange),
        (Some(start), Some(end)) if end < start => return Err(CitationProblem::InvalidRange),
        (Some(start), None) => Some(start),
        (_, end) => end,
    };
    let start = start.unwrap_or(1);
    let mut hash = Sha256::new();
    let mut buffer = [0; 8 * 1024];
    let mut utf8 = Vec::with_capacity(buffer.len() + 3);
    let mut line = 1_u64;
    let mut seen = false;
    let mut newline = false;
    loop {
        let count = file.read(&mut buffer).await.map_err(file_error)?;
        if count == 0 {
            break;
        }
        utf8.extend_from_slice(&buffer[..count]);
        match std::str::from_utf8(&utf8) {
            Ok(_) => utf8.clear(),
            Err(error) if error.error_len().is_none() => {
                utf8.drain(..error.valid_up_to());
            }
            Err(_) => return Err(CitationProblem::NotUtf8),
        }
        for bytes in buffer[..count].split_inclusive(|byte| *byte == b'\n') {
            if line >= u64::from(start) && end.is_none_or(|end| line <= u64::from(end)) {
                hash.update(bytes);
            }
            seen = true;
            newline = bytes.last() == Some(&b'\n');
            line += u64::from(newline);
        }
    }
    if !utf8.is_empty() {
        return Err(CitationProblem::NotUtf8);
    }
    let lines = if seen { line - u64::from(newline) } else { 0 };
    let lines = u32::try_from(lines).map_err(|_| CitationProblem::InvalidRange)?;
    let end = end.unwrap_or(lines);
    if start > lines || end > lines {
        return Err(CitationProblem::InvalidRange);
    }
    Ok(Snapshot { start, end, hash: HEXLOWER.encode(&hash.finalize()) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn line_hashing_handles_chunk_boundaries_and_last_line_endings() {
        let prefix = "x".repeat(8 * 1024 - 1);
        let text = format!("{prefix}β\nlast\n");
        let first = snapshot(&mut text.as_bytes(), Some(1), Some(1)).await.unwrap();
        let last = snapshot(&mut text.as_bytes(), Some(2), None).await.unwrap();
        let whole = snapshot(&mut text.as_bytes(), None, None).await.unwrap();
        assert_eq!(first.hash, crate::log::sha256(format!("{prefix}β\n").as_bytes()));
        assert_eq!(last.hash, crate::log::sha256(b"last\n"));
        assert_eq!(whole.end, 2);
    }
}
