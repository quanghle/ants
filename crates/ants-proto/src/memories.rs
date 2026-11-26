//! What an [`AdminOp::ReadMemories`] order returns in its outcome's `result`: the ant's memories ([`Memories`]).
//!
//! - Core memories ([`CoreMemories`]): the ant's `whoami`, `memory` and `tasks`, as the config file of its machine
//!   sets them, which an admin edits with a set order.
//! - Harness memories: the files of the ant's harness's `memory_files` globs ([`MemoryFile`]), and the bd memories of
//!   the database `BD_DB` names in the ant's environment ([`BdMemories`]), read-only.
//! - Provider memories ([`ProviderMemories`]): the memories view of the ant's provider, when it serves one and the
//!   order names the ant's identity, read-only, each entry with the typed fields the provider gave it
//!   ([`ProviderMemory`]).
//!
//! A machine's daemon answers such an order only while it is online: one that waited longer than [`MEMORIES_FRESH`]
//! in the stream for it is refused as stale. The outcome stays in a KV bucket, so what it carries is capped: each
//! text at [`MEMORY_FILE_CAP`] bytes, all of them at [`MEMORIES_CAP`] bytes of JSON, and what is left out is named in
//! [`Memories::skipped`].

use chrono::TimeDelta;
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use crate::AdminOp;
use crate::provider::MemoryFields;

/// How long a read of memories may wait in the stream for its machine's daemon before it is refused as stale.
pub const MEMORIES_FRESH: TimeDelta = TimeDelta::minutes(2);

/// The most bytes of one memory shown: of a file, of a bd memory, of a core memory, of a provider's memory with its
/// typed fields as JSON.
pub const MEMORY_FILE_CAP: usize = 32 * 1024;

/// The most bytes of JSON the texts of all memories take together, a provider memory's typed fields among them.
pub const MEMORIES_CAP: usize = 192 * 1024;

/// The memories of an ant. Readers should treat unknown fields as additions, not errors.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memories {
    pub core: CoreMemories,
    /// The harness whose memory files these are.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// The ant's workdir, which relative globs are relative to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<MemoryFile>,
    /// The bd memories, when `BD_DB` is set in the ant's environment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bd: Option<BdMemories>,
    /// The memories view of the ant's provider, when it serves one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderMemories>,
    /// What was found but left out or cut short, and why.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<Skipped>,
}

/// An ant's role, memory and task instructions, which every prompt starts with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoreMemories {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whoami: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tasks: Option<String>,
}

/// A memory file of the ant's harness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryFile {
    /// Its path, symbolic links resolved.
    pub path: String,
    /// Its size in bytes.
    pub size: u64,
    pub text: String,
    /// Whether `text` holds only the start of the file.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

/// The bd memories of the database `BD_DB` names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BdMemories {
    /// `BD_DB`, as the ant's environment sets it.
    pub db: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memories: Vec<BdMemory>,
    /// Why `bd memories` could not list them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A memory of bd, by its key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BdMemory {
    pub key: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

/// The memories view of an ant's provider.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMemories {
    /// The provider's name.
    pub provider: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memories: Vec<ProviderMemory>,
    /// Why the provider's view could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// An entry of the memories view of an ant's provider ([`crate::provider::MemoryEntry`]), with what it is as the
/// provider types it, its typed fields counted within the caps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMemory {
    pub key: String,
    pub content: String,
    #[serde(flatten)]
    pub fields: MemoryFields,
    /// Whether `content` holds only its start, or `fields.triggers` only the first of its rows.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

/// A memory found but left out, or cut short: its path, key or name, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skipped {
    pub what: String,
    pub why: String,
}

mod shown;
pub use shown::{MemoriesOutcome, MemoriesReply, MemoriesRequest, MemoriesShown};
