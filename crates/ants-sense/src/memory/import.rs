//! Importing an ant's harness memories into sense, once per ant: the memory files its harness's `memory_files` globs
//! match and the bd memories of the database its `BD_DB` names, which the daemon reads, within the caps of a memories
//! read, and passes as [`Source`]s ([`import`]).
//!
//! Each file becomes a memory per section of markdown, titled with its path and its headings; each bd memory one,
//! titled with its key; a text over [`MAX_BODY_BYTES`] is split at paragraphs into parts. Every imported memory is a
//! fact of default importance, never core, so the core memories' cap stays the ant's, and untrusted: the text is
//! not the ant's own, but outside content written by whoever wrote the files or the database, to verify before use.
//! It names its source ([`Imported`]) and passes the sanitizer as any memory does. One that is an exact or near
//! duplicate of a live memory of its scope, or of one imported before it, is left out.
//!
//! The import writes its memories in batches, then its record ([`Import`]), an item of kind [`IMPORT`] named
//! [`HARNESS`] that sense alone writes: an ant whose items hold the record is imported never again, and one whose
//! import stopped halfway is imported again, the memories written already left out as duplicates.

use std::collections::HashSet;
use std::fmt;

use ants_proto::provider::CallContext;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    DIGEST, MAX_BODY_BYTES, MAX_TITLE_CHARS, MEMORY, Memory, MemoryItem, MemoryKind, NEAR_DUPLICATE, Scope, Trust,
    similarity,
};
use crate::identity::BY;
use crate::items::{ItemId, ItemValues};
use crate::sanitize::{Limit, sanitize_body, sanitize_title};
use crate::store::{Items, StoreError, Write};

/// The kind of the record of an import.
pub const IMPORT: &str = "import";
/// The name of the record of the import of the ant's harness memories ([`ItemId::named`]).
pub const HARNESS: &str = "harness";
/// The most memories an import writes.
pub const MAX_IMPORTED: usize = 100;

/// The best full-text matches compared for near duplicates.
const CANDIDATES: usize = 20;
/// The most memories one event of the log holds.
const BATCH_ITEMS: usize = 25;
/// What separates the headings of a section in its title.
const TRAIL: &str = " > ";

/// Where an imported memory came from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum Imported {
    /// A memory file of the ant's harness: its path relative to the ant's workdir, or under `~`.
    File { path: String },
    /// A bd memory of the database `db`, by its key.
    Bd { db: String, key: String },
}

impl fmt::Display for Imported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File { path } => write!(f, "the harness's memory file {path}"),
            Self::Bd { db, key } => write!(f, "bd memory {key} of {db}"),
        }
    }
}

/// A text to import: a memory file or a bd memory, as read, with the scope its memories take.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    pub from: Imported,
    pub scope: Scope,
    pub text: String,
}

/// The record of an import: when, from how many sources, and how many memories it wrote and left out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Import {
    pub at: DateTime<Utc>,
    pub sources: usize,
    pub imported: usize,
    /// Memories left out as exact or near duplicates.
    pub duplicates: usize,
    /// Memories left out past [`MAX_IMPORTED`].
    pub over: usize,
}

impl ItemValues for Import {
    const KINDS: &[&str] = &[IMPORT];
}

fn record_id() -> ItemId {
    ItemId::named(IMPORT, HARNESS)
}

/// The record of the import of the ant's harness memories; none before it ran.
pub async fn imported(items: &dyn Items) -> Result<Option<Import>, StoreError> {
    items.get(&record_id()).await?.as_ref().map(Import::of).transpose()
}

/// Import `sources` into `items` as memories written by `writer`, unless imported already: the record of the import,
/// none when one was recorded before.
pub async fn import(items: &dyn Items, sources: &[Source], writer: &CallContext) -> Result<Option<Import>, StoreError> {
    if imported(items).await?.is_some() {
        return Ok(None);
    }
    let mut kept: Vec<Memory> = Vec::new();
    let mut seen = HashSet::new();
    let (mut duplicates, mut over) = (0, 0);
    for source in sources {
        for memory in memories(source, writer) {
            if !seen.insert((memory.scope.to_string(), memory.digest.clone()))
                || kept.iter().any(|other| near(other, &memory))
                || duplicated(items, &memory).await?
            {
                duplicates += 1;
            } else if kept.len() == MAX_IMPORTED {
                over += 1;
            } else {
                kept.push(memory);
            }
        }
    }
    let record = Import { at: writer.at, sources: sources.len(), imported: kept.len(), duplicates, over };
    let mut writes: Vec<Write> = kept.iter().map(|memory| memory.create(ItemId::new())).collect();
    let marker = Write::Put { id: record_id(), kind: IMPORT.into(), values: record.values(), expected: Some(0) };
    while writes.len() > BATCH_ITEMS {
        let batch: Vec<_> = writes.drain(..BATCH_ITEMS).collect();
        items.write(&batch, BY).await?;
    }
    writes.push(marker);
    items.write(&writes, BY).await?;
    Ok(Some(record))
}

/// Whether a live memory of the scope of `memory` holds its body, or nearly its text.
async fn duplicated(items: &dyn Items, memory: &Memory) -> Result<bool, StoreError> {
    let rival = |found: &MemoryItem| found.memory.status.live() && found.memory.scope == memory.scope;
    let same = items.list_by(MEMORY, DIGEST, Some(&json!(memory.digest)), usize::MAX).await?;
    if same.into_iter().filter_map(MemoryItem::of).any(|found| rival(&found)) {
        return Ok(true);
    }
    let text = format!("{}\n{}", memory.title, memory.body);
    let hits = items.search(&text, Some(MEMORY), CANDIDATES).await?;
    Ok(hits
        .into_iter()
        .filter_map(|hit| MemoryItem::of(hit.item))
        .filter(|found| rival(found))
        .any(|found| similarity(&text, &format!("{}\n{}", found.memory.title, found.memory.body)) >= NEAR_DUPLICATE))
}

/// Whether `a` and `b`, of the same scope, are near duplicates.
fn near(a: &Memory, b: &Memory) -> bool {
    a.scope == b.scope
        && similarity(&format!("{}\n{}", a.title, a.body), &format!("{}\n{}", b.title, b.body)) >= NEAR_DUPLICATE
}

/// The memories of `source`, written by `writer`: a file's sections, a bd memory whole, each split into parts that
/// fit a body, sanitized.
fn memories(source: &Source, writer: &CallContext) -> Vec<Memory> {
    let texts = match &source.from {
        Imported::File { path } => sections(&source.text)
            .into_iter()
            .map(|(trail, body)| {
                let title = if trail.is_empty() { path.clone() } else { format!("{path}: {}", trail.join(TRAIL)) };
                (title, body)
            })
            .collect(),
        Imported::Bd { key, .. } => vec![(format!("bd memory {key}"), source.text.trim().to_owned())],
    };
    let mut memories = Vec::new();
    for (title, body) in texts {
        let parts = parts(&body);
        let count = parts.len();
        for (part, body) in parts.into_iter().enumerate() {
            let title = if count > 1 { format!("{title} (part {} of {count})", part + 1) } else { title.clone() };
            let title = cut_chars(&title, MAX_TITLE_CHARS);
            let (Ok(title), Ok(body)) = (
                sanitize_title(&title, Limit::Chars(MAX_TITLE_CHARS)),
                sanitize_body(&body, Limit::Bytes(MAX_BODY_BYTES)),
            ) else {
                continue;
            };
            let (title, body) = (title.text.trim().to_owned(), body.text.trim().to_owned());
            if title.is_empty() || body.is_empty() {
                continue;
            }
            let memory = Memory::new(MemoryKind::Fact, source.scope.clone(), title, body, writer);
            memories.push(Memory { trust: Trust::Untrusted, imported: Some(source.from.clone()), ..memory });
        }
    }
    memories
}

/// The sections of markdown `text`, each with the headings it lies under, the outermost first, and its body; a
/// section of a blank body is left out. A `#` inside a fenced code block starts no section.
fn sections(text: &str) -> Vec<(Vec<String>, String)> {
    let mut sections = Vec::new();
    let mut trail: Vec<(usize, String)> = Vec::new();
    let mut body = String::new();
    let mut fence: Option<&str> = None;
    let flush = |trail: &[(usize, String)], body: &mut String, sections: &mut Vec<_>| {
        let text = std::mem::take(body);
        if !text.trim().is_empty() {
            sections.push((trail.iter().map(|(_, heading)| heading.clone()).collect(), text.trim().to_owned()));
        }
    };
    for line in text.lines() {
        let trimmed = line.trim_start();
        let marker = ["```", "~~~"].into_iter().find(|marker| trimmed.starts_with(marker));
        match (fence, marker) {
            (None, Some(marker)) => fence = Some(marker),
            (Some(open), Some(marker)) if open == marker => fence = None,
            _ => {}
        }
        if fence.is_none()
            && marker.is_none()
            && let Some((level, heading)) = heading(line)
        {
            flush(&trail, &mut body, &mut sections);
            trail.retain(|(outer, _)| *outer < level);
            trail.push((level, heading));
            continue;
        }
        body.push_str(line);
        body.push('\n');
    }
    flush(&trail, &mut body, &mut sections);
    sections
}

/// The level and text of `line` when it is an ATX heading: one to six `#`, then a space.
fn heading(line: &str) -> Option<(usize, String)> {
    let level = line.bytes().take_while(|&b| b == b'#').count();
    let rest = &line[level..];
    if !(1..=6).contains(&level) || !(rest.is_empty() || rest.starts_with([' ', '\t'])) {
        return None;
    }
    let heading = rest.trim().trim_end_matches('#').trim();
    (!heading.is_empty()).then(|| (level, heading.to_owned()))
}

/// `body` in parts of at most [`MAX_BODY_BYTES`], split between paragraphs, and inside one only when it is too long.
fn parts(body: &str) -> Vec<String> {
    if body.len() <= MAX_BODY_BYTES {
        return vec![body.to_owned()];
    }
    let mut parts = Vec::new();
    let mut part = String::new();
    for paragraph in body.split("\n\n").filter(|paragraph| !paragraph.trim().is_empty()) {
        if !part.is_empty() && part.len() + 2 + paragraph.len() > MAX_BODY_BYTES {
            parts.push(std::mem::take(&mut part));
        }
        let mut rest = paragraph;
        while rest.len() > MAX_BODY_BYTES {
            if !part.is_empty() {
                parts.push(std::mem::take(&mut part));
            }
            let end = rest.floor_char_boundary(MAX_BODY_BYTES);
            parts.push(rest[..end].to_owned());
            rest = &rest[end..];
        }
        if !part.is_empty() {
            part.push_str("\n\n");
        }
        part.push_str(rest);
    }
    if !part.trim().is_empty() {
        parts.push(part);
    }
    parts
}

/// `text` cut to `max` characters, the last an ellipsis when cut.
fn cut_chars(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_owned();
    }
    text.chars().take(max - 1).chain(['…']).collect()
}

#[cfg(test)]
mod tests;
