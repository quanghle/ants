//! Files the ant attaches to what it sends with its MCP tools (`attach`), and the attachments it reads
//! (`read_attachment`). Paths stay inside the directory the thread's session works in: the ant's workdir, or the
//! thread's worktree.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ants_proto::{
    ATTACHMENT_CHUNK, Attachment, AttachmentHandle, MAX_ATTACHMENT_SIZE, MAX_ATTACHMENTS, Uuid, guess_mime,
};
use anyhow::Result;
use futures::future::BoxFuture;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Most bytes of a text attachment `read_attachment` returns as text; a longer one is saved as a file.
pub(super) const MAX_TEXT: usize = ATTACHMENT_CHUNK;
/// Names tried when saving an attachment beside different files of its name: `name`, `name-2`, … up to this.
const MAX_SAVE_TRIES: usize = 100;

/// What the tools need of the attachments' store: `put` stores the bytes of an attachment under a handle, resolving
/// once stored; `get` reads those under a handle, with the reference stored with them, checked against it.
#[derive(Clone)]
pub struct FileAccess {
    pub put: Arc<PutFile>,
    pub get: Arc<GetFile>,
}

/// Stores the bytes of an attachment under a handle.
pub type PutFile = dyn Fn(AttachmentHandle, Attachment, Arc<[u8]>) -> BoxFuture<'static, Result<()>> + Send + Sync;
/// Reads the reference and bytes of the attachment under a handle.
pub type GetFile = dyn Fn(AttachmentHandle) -> BoxFuture<'static, Result<(Attachment, Vec<u8>)>> + Send + Sync;

/// Stores and reads in the attachments' store through JetStream ([`crate::files`]), by direct gets only.
pub fn jetstream_files(js: async_nats::jetstream::Context) -> FileAccess {
    let reader = js.clone();
    FileAccess {
        put: Arc::new(move |handle, attachment, bytes| {
            let js = js.clone();
            Box::pin(async move { crate::files::put(&js, &handle, &attachment, &bytes).await })
        }),
        get: Arc::new(move |handle| {
            let js = reader.clone();
            Box::pin(async move { crate::files::get(&js, &handle, None).await })
        }),
    }
}

/// A file read to attach: its reference, with a fresh id, and its bytes.
pub(super) struct Loaded {
    pub attachment: Attachment,
    pub bytes: Arc<[u8]>,
}

/// Read the files at `paths` to attach them, each relative to `dir` unless absolute: at most [`MAX_ATTACHMENTS`],
/// each a regular file of at most [`MAX_ATTACHMENT_SIZE`] whose real path, symlinks and `..` resolved, is inside `dir`.
/// Each is named by the base name of its path, with a media type guessed from its extension.
pub(super) async fn load(dir: &Path, paths: &[String]) -> Result<Vec<Loaded>, String> {
    if paths.len() > MAX_ATTACHMENTS {
        return Err(format!("attach takes at most {MAX_ATTACHMENTS} files, not {}; nothing sent", paths.len()));
    }
    let root = tokio::fs::canonicalize(dir)
        .await
        .map_err(|e| format!("cannot resolve the working directory {}: {e}; nothing sent", dir.display()))?;
    let mut loaded = Vec::with_capacity(paths.len());
    for path in paths {
        let file =
            load_one(&root, path.trim()).await.map_err(|why| format!("cannot attach {path:?}: {why}; nothing sent"))?;
        loaded.push(file);
    }
    Ok(loaded)
}

async fn load_one(root: &Path, path: &str) -> Result<Loaded, String> {
    if path.is_empty() {
        return Err("the path is empty".to_owned());
    }
    let given = Path::new(path);
    let real = tokio::fs::canonicalize(root.join(given)).await.map_err(|e| e.to_string())?;
    if !real.starts_with(root) {
        return Err(format!("it is outside the working directory {}", root.display()));
    }
    let mut file = tokio::fs::File::open(&real).await.map_err(|e| e.to_string())?;
    let metadata = file.metadata().await.map_err(|e| e.to_string())?;
    if !metadata.is_file() {
        return Err("it is no regular file".to_owned());
    }
    let too_big = |size: u64| format!("it is {} long, over the 8 MiB an attachment holds", size_text(size));
    if metadata.len() > MAX_ATTACHMENT_SIZE {
        return Err(too_big(metadata.len()));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    // At most one byte over, so a file growing meanwhile is caught without reading all of it.
    (&mut file).take(MAX_ATTACHMENT_SIZE + 1).read_to_end(&mut bytes).await.map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_ATTACHMENT_SIZE {
        return Err(too_big(bytes.len() as u64));
    }
    let name =
        given.file_name().or_else(|| real.file_name()).and_then(|n| n.to_str()).ok_or("it has no UTF-8 file name")?;
    let attachment = crate::files::reference(name, guess_mime(name), &bytes).map_err(|e| format!("{e:#}"))?;
    Ok(Loaded { attachment, bytes: bytes.into() })
}

/// Store the bytes of `loaded` under the handles `handle_of` gives their ids, returning their references for the
/// message to carry.
pub(super) async fn store(
    files: &FileAccess,
    loaded: Vec<Loaded>,
    handle_of: impl Fn(Uuid) -> AttachmentHandle,
) -> Result<Vec<Attachment>, String> {
    let mut attachments = Vec::with_capacity(loaded.len());
    for Loaded { attachment, bytes } in loaded {
        (files.put)(handle_of(attachment.id), attachment.clone(), bytes)
            .await
            .map_err(|e| format!("cannot store attachment {}: {e:#}; nothing sent", attachment.name))?;
        attachments.push(attachment);
    }
    Ok(attachments)
}

/// Save `bytes`, those of `attachment`, into `dir` under its name, or `name-2`, `name-3`, … when a different file has
/// it; a file there with the same bytes is reused. Returns the path. An existing file, or a symlink, is never written.
pub(super) async fn save(dir: &Path, attachment: &Attachment, bytes: &[u8]) -> Result<PathBuf, String> {
    let root = tokio::fs::canonicalize(dir)
        .await
        .map_err(|e| format!("cannot resolve the working directory {}: {e}", dir.display()))?;
    for n in 1..=MAX_SAVE_TRIES {
        let path = root.join(numbered(&attachment.name, n));
        match tokio::fs::OpenOptions::new().write(true).create_new(true).open(&path).await {
            Ok(mut file) => {
                let written = async {
                    file.write_all(bytes).await?;
                    file.flush().await
                };
                if let Err(e) = written.await {
                    let _ = tokio::fs::remove_file(&path).await;
                    return Err(format!("cannot write {}: {e}", path.display()));
                }
                return Ok(path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if holds(&path, bytes).await {
                    return Ok(path);
                }
            }
            Err(e) => return Err(format!("cannot write {}: {e}", path.display())),
        }
    }
    Err(format!("{} and {MAX_SAVE_TRIES} numbered names beside it are taken in {}", attachment.name, root.display()))
}

/// Whether `path` is a regular file, no symlink, holding `bytes`.
async fn holds(path: &Path, bytes: &[u8]) -> bool {
    let regular = tokio::fs::symlink_metadata(path).await.is_ok_and(|m| m.is_file() && m.len() == bytes.len() as u64);
    regular && tokio::fs::read(path).await.is_ok_and(|held| held == bytes)
}

/// `name` for `n` 1, else with `-n` before its extension: `shot-2.png`.
fn numbered(name: &str, n: usize) -> String {
    if n == 1 {
        return name.to_owned();
    }
    match name.rsplit_once('.').filter(|(stem, _)| !stem.is_empty()) {
        Some((stem, extension)) => format!("{stem}-{n}.{extension}"),
        None => format!("{name}-{n}"),
    }
}

/// Whether `read_attachment` returns `attachment` as text: a text/* or JSON one.
pub(super) fn is_text(attachment: &Attachment) -> bool {
    attachment.mime.starts_with("text/") || attachment.mime == "application/json"
}

/// Lines listing `attachments`, each with the handle `handle_of` gives its id, for `read_attachment`: empty for none.
pub(super) fn listing(attachments: &[Attachment], handle_of: impl Fn(Uuid) -> AttachmentHandle) -> String {
    attachments.iter().map(|a| format!("\n📎 {} handle {}", describe(a), handle_of(a.id))).collect()
}

/// An attachment's name, media type and size.
pub(super) fn describe(attachment: &Attachment) -> String {
    format!("{} ({}, {})", attachment.name, attachment.mime, size_text(attachment.size))
}

/// `size` bytes in B, KiB or MiB.
fn size_text(size: u64) -> String {
    match size {
        0..1024 => format!("{size} B"),
        1024..1_048_576 => format!("{:.1} KiB", size as f64 / 1024.0),
        _ => format!("{:.1} MiB", size as f64 / 1_048_576.0),
    }
}
