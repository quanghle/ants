//! Files attached to messages, from the CLI: reading those `--attach` names ([`Attach`]), storing them before the
//! message carrying them ([`store`]), listing them under a message ([`Listed`]) and saving one ([`save`], `ants
//! attachment get`). The ants TUI attaches, lists and saves through the same functions.

use std::collections::HashSet;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use ants_daemon::files;
use ants_proto::{Attachment, AttachmentHandle, MAX_ATTACHMENT_SIZE, MAX_ATTACHMENTS, Uuid};
use anyhow::{Context as _, Result, bail, ensure};
use async_nats::jetstream;
use serde::{Serialize, Serializer};

/// The `--attach` of `ants ask`, `ants post` and `ants notes`.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct Attach {
    /// Attach the file at PATH (repeat for up to 8, each at most 8 MiB), named by its base name, its media type
    /// guessed from its extension. The files are stored before the message, and nothing is sent when one fails.
    #[arg(long = "attach", value_name = "PATH")]
    pub paths: Vec<PathBuf>,
}

impl Attach {
    /// Read the files to attach ([`read_all`]).
    pub fn read(&self) -> Result<Vec<File>> {
        read_all(&self.paths)
    }
}

/// A file to attach: its reference, with a fresh id, and its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct File {
    pub attachment: Attachment,
    pub bytes: Vec<u8>,
}

/// Read the files at `paths` to attach to one message: at most [`MAX_ATTACHMENTS`], each read with [`read`].
pub fn read_all(paths: &[PathBuf]) -> Result<Vec<File>> {
    ensure!(
        paths.len() <= MAX_ATTACHMENTS,
        "{} files to attach: a message carries at most {MAX_ATTACHMENTS}",
        paths.len()
    );
    paths.iter().map(|path| read(path)).collect()
}

/// Read the file at `path` to attach: a regular file of at most [`MAX_ATTACHMENT_SIZE`] bytes, named by its base name,
/// of the media type its extension gives ([`mime_of`]).
pub fn read(path: &Path) -> Result<File> {
    let shown = path.display();
    let name = path.file_name().with_context(|| format!("{shown} names no file to attach"))?;
    let name = name.to_str().with_context(|| format!("the name of {shown} is not UTF-8: rename it to attach it"))?;
    let file = std::fs::File::open(path).with_context(|| format!("opening {shown} to attach it"))?;
    let meta = file.metadata().with_context(|| format!("reading {shown}"))?;
    ensure!(meta.is_file(), "{shown} is no file: only a file is attached");
    let too_big = |len| format!("{shown} is {}: an attachment is at most {}", size(len), size(MAX_ATTACHMENT_SIZE));
    ensure!(meta.len() <= MAX_ATTACHMENT_SIZE, too_big(meta.len()));
    let mut bytes = Vec::with_capacity(usize::try_from(meta.len()).unwrap_or_default());
    file.take(MAX_ATTACHMENT_SIZE + 1).read_to_end(&mut bytes).with_context(|| format!("reading {shown}"))?;
    ensure!(bytes.len() as u64 <= MAX_ATTACHMENT_SIZE, too_big(bytes.len() as u64));
    let attachment = files::reference(name, mime_of(name), &bytes).with_context(|| format!("attaching {shown}"))?;
    Ok(File { attachment, bytes })
}

/// The references of `files`, as the message carrying them holds them.
pub fn references(files: &[File]) -> Vec<Attachment> {
    files.iter().map(|file| file.attachment.clone()).collect()
}

/// Store `files` under the handles `handle_of` gives their ids, those of the message about to carry them, before it is
/// sent.
pub async fn store(
    js: &jetstream::Context,
    files: &[File],
    handle_of: impl Fn(Uuid) -> AttachmentHandle,
) -> Result<()> {
    for file in files {
        files::put(js, &handle_of(file.attachment.id), &file.attachment, &file.bytes).await?;
    }
    Ok(())
}

/// The media type of a file named `name`, from its extension, whatever the case; [`Attachment::UNKNOWN_MIME`] for
/// one not listed.
pub fn mime_of(name: &str) -> &'static str {
    let Some((_, extension)) = name.rsplit_once('.') else { return Attachment::UNKNOWN_MIME };
    match extension.to_ascii_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "ico" => "image/vnd.microsoft.icon",
        "tif" | "tiff" => "image/tiff",
        "avif" => "image/avif",
        "heic" => "image/heic",
        "txt" | "log" | "text" => "text/plain",
        "md" | "markdown" => "text/markdown",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "text/javascript",
        "diff" | "patch" => "text/x-diff",
        "rs" => "text/x-rust",
        "py" => "text/x-python",
        "go" => "text/x-go",
        "c" | "h" => "text/x-c",
        "sh" => "application/x-sh",
        "json" => "application/json",
        "xml" => "application/xml",
        "yaml" | "yml" => "application/yaml",
        "toml" => "application/toml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "tar" => "application/x-tar",
        "wasm" => "application/wasm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "mp4" => "video/mp4",
        "mov" => "video/quicktime",
        "webm" => "video/webm",
        _ => Attachment::UNKNOWN_MIME,
    }
}

/// A size for people: bytes, KiB or MiB, with one decimal under 10.
pub fn size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    let scaled = |unit: u64, name: &str| {
        let value = bytes as f64 / unit as f64;
        if value < 10.0 { format!("{value:.1} {name}") } else { format!("{value:.0} {name}") }
    };
    if bytes < KIB {
        format!("{bytes} B")
    } else if bytes < KIB * KIB {
        scaled(KIB, "KiB")
    } else {
        scaled(KIB * KIB, "MiB")
    }
}

/// An attachment as a message lists it: its reference, the handle naming where its bytes are, and why it is unavailable
/// when the reference is invalid ([`Attachment::check`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listed {
    #[serde(flatten)]
    pub attachment: Attachment,
    #[serde(serialize_with = "display")]
    pub handle: AttachmentHandle,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
}

fn display<S: Serializer>(handle: &AttachmentHandle, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(handle)
}

impl Listed {
    /// Its line under the message: `📎 name (size) handle`, or with an invalid reference why it is unavailable.
    pub fn line(&self) -> String {
        let Attachment { name, size: bytes, .. } = &self.attachment;
        match &self.unavailable {
            None => format!("📎 {name} ({}) {}", size(*bytes), self.handle),
            // An invalid name may hold control characters, which are escaped rather than sent to the terminal.
            Some(reason) => format!("📎 {:?} (unavailable: {reason}) {}", name, self.handle),
        }
    }
}

/// The attachments a message carries, listed with the handles `handle_of` gives their ids, those of the message's
/// subject. One with an invalid reference, a repeated id or past the [`MAX_ATTACHMENTS`] first is unavailable.
pub fn listed(attachments: &[Attachment], handle_of: impl Fn(Uuid) -> AttachmentHandle) -> Vec<Listed> {
    let mut ids = HashSet::new();
    attachments
        .iter()
        .enumerate()
        .map(|(n, attachment)| {
            let unavailable = if n >= MAX_ATTACHMENTS {
                Some(format!("a message carries at most {MAX_ATTACHMENTS}"))
            } else if let Err(e) = attachment.check() {
                Some(e.to_string())
            } else if !ids.insert(attachment.id) {
                Some("its id is repeated".to_owned())
            } else {
                None
            };
            Listed { attachment: attachment.clone(), handle: handle_of(attachment.id), unavailable }
        })
        .collect()
}

/// The attachments of the message stored on `subject` ([`listed`]); none for a subject carrying none.
pub fn listed_on(subject: &str, attachments: &[Attachment]) -> Vec<Listed> {
    match AttachmentHandle::of_message(subject, Uuid::nil()) {
        Some(handle) if !attachments.is_empty() => listed(attachments, |id| AttachmentHandle { id, ..handle.clone() }),
        _ => Vec::new(),
    }
}

/// The lines of `listed` under a message, each at `indent` and ending in a newline.
pub fn render(listed: &[Listed], indent: &str) -> String {
    listed.iter().map(|l| format!("{indent}{}\n", l.line())).collect()
}

/// Read the attachment under `handle` and check its bytes against `expected`, the reference of the message carrying
/// it, or without one against the reference stored with them.
pub async fn fetch(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
) -> Result<(Attachment, Vec<u8>)> {
    files::get(js, handle, expected).await
}

/// Where an attachment named `name` is saved: `out`, or in it under `name` when it is a directory; `./name` without.
pub fn target(out: Option<&Path>, name: &str) -> PathBuf {
    match out {
        Some(dir) if dir.is_dir() => dir.join(name),
        Some(path) => path.to_owned(),
        None => PathBuf::from(name),
    }
}

/// Write `bytes` to a new file at `path`, refusing to overwrite one; on a failure, nothing is left there.
pub fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            anyhow::anyhow!("{} exists: not overwriting it (-o names another path)", path.display())
        } else {
            anyhow::Error::new(e).context(format!("creating {}", path.display()))
        }
    })?;
    if let Err(e) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(e).with_context(|| format!("writing {}", path.display()));
    }
    Ok(())
}

/// Save the attachment under `handle` where [`target`] puts it, after checking its bytes ([`fetch`]): nothing is
/// written when they do not match. Returns its reference and the path written.
pub async fn save(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
    out: Option<&Path>,
) -> Result<(Attachment, PathBuf)> {
    if let Some(path) = out.filter(|path| !path.is_dir())
        && path.exists()
    {
        bail!("{} exists: not overwriting it (-o names another path)", path.display());
    }
    let (attachment, bytes) = fetch(js, handle, expected).await?;
    let path = target(out, &attachment.name);
    write_new(&path, &bytes)?;
    Ok((attachment, path))
}

/// `ants attachment get HANDLE [-o PATH]`.
pub async fn get(nats: &crate::Nats<'_>, handle: &AttachmentHandle, out: Option<&Path>) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let (attachment, path) = save(&js, handle, None, out).await?;
    println!("saved {} ({}, {}) to {}", attachment.name, size(attachment.size), attachment.mime, path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use ants_proto::{Envelope, Role, UserId};

    use super::*;
    use crate::auth::tests::Server;

    #[test]
    fn media_types_come_from_the_extension() {
        assert_eq!(mime_of("Shot.PNG"), "image/png");
        assert_eq!(mime_of("photo.jpeg"), "image/jpeg");
        assert_eq!(mime_of("build.log"), "text/plain");
        assert_eq!(mime_of("archive.tar.gz"), "application/gzip");
        for unknown in ["Makefile", "data.bin", "trailing."] {
            assert_eq!(mime_of(unknown), Attachment::UNKNOWN_MIME, "{unknown}");
        }
        assert_eq!(
            [0, 1023, 1024, 1536, 240 * 1024, 8 * 1024 * 1024].map(size),
            ["0 B", "1023 B", "1.0 KiB", "1.5 KiB", "240 KiB", "8.0 MiB"]
        );
    }

    #[test]
    fn only_files_within_the_limits_are_attached() {
        let dir = ants_test_support::scratch_dir("attach");
        let shot = dir.join("shot.png");
        std::fs::write(&shot, b"\x89PNG").unwrap();
        let read = read(&shot).unwrap();
        assert_eq!((read.attachment.name.as_str(), read.attachment.mime.as_str()), ("shot.png", "image/png"));
        assert_eq!((read.attachment.size, read.bytes.as_slice()), (4, b"\x89PNG".as_slice()));
        assert_eq!(read.attachment.sha256, files::sha256(b"\x89PNG"));

        let big = dir.join("big.bin");
        std::fs::File::create(&big).unwrap().set_len(MAX_ATTACHMENT_SIZE + 1).unwrap();
        let refused = super::read(&big).unwrap_err().to_string();
        assert!(refused.contains("is 8.0 MiB: an attachment is at most 8.0 MiB"), "{refused}");
        std::fs::File::create(&big).unwrap().set_len(MAX_ATTACHMENT_SIZE).unwrap();
        assert_eq!(super::read(&big).unwrap().attachment.size, MAX_ATTACHMENT_SIZE);

        assert!(super::read(&dir).unwrap_err().to_string().contains("is no file"));
        assert!(super::read(&dir.join("missing")).is_err());
        assert_eq!(read_all(&vec![shot.clone(); MAX_ATTACHMENTS]).unwrap().len(), MAX_ATTACHMENTS);
        let many = read_all(&vec![shot; MAX_ATTACHMENTS + 1]).unwrap_err().to_string();
        assert!(many.contains("9 files to attach: a message carries at most 8"), "{many}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_invalid_reference_is_listed_unavailable() {
        let message = Envelope::new(UserId::human(), "dev-a".parse().unwrap(), "look");
        let good = files::reference("a.txt", "text/plain", b"a").unwrap();
        let bad = Attachment { name: "evil\u{1b}[2J".into(), ..files::reference("b", "text/plain", b"b").unwrap() };
        let shown = listed(&[good.clone(), bad, good.clone()], |id| message.attachment_handle(id));
        let lines: Vec<String> = shown.iter().map(Listed::line).collect();
        assert_eq!(lines[0], format!("📎 a.txt (1 B) human.inbox.dev-a.{}", good.id));
        assert!(lines[1].starts_with("📎 \"evil\\u{1b}[2J\" (unavailable: invalid attachment"), "{}", lines[1]);
        assert!(lines[2].contains("(unavailable: its id is repeated)"), "{}", lines[2]);
        let json = serde_json::to_value(&shown[0]).unwrap();
        assert_eq!(json["handle"], format!("human.inbox.dev-a.{}", good.id));
        assert_eq!((json["name"].as_str(), json["size"].as_u64()), (Some("a.txt"), Some(1)));
        assert!(json.get("unavailable").is_none());
        assert!(listed_on("ants.dead.dev-a", &[good]).is_empty(), "no handle without a message subject");
    }

    #[tokio::test]
    async fn an_attachment_is_saved_once_and_only_as_stored() {
        let Some(server) = Server::start() else { return };
        let (alice, dev_a): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
        let dir = server.dir().join("saved");
        std::fs::create_dir_all(&dir).unwrap();
        let source = dir.join("report.txt");
        std::fs::write(&source, b"findings").unwrap();

        let attached = read_all(std::slice::from_ref(&source)).unwrap();
        let mut message = Envelope::new(alice.clone(), dev_a, "see attached");
        message.attachments = references(&attached);
        store(&js, &attached, |id| message.attachment_handle(id)).await.unwrap();
        let [shown] = listed_on(&message.subject(), &message.attachments).try_into().unwrap();
        assert_eq!(shown.handle, message.attachment_handle(attached[0].attachment.id));

        let out = dir.join("out");
        std::fs::create_dir(&out).unwrap();
        let (saved, path) = save(&js, &shown.handle, Some(&shown.attachment), Some(&out)).await.unwrap();
        assert_eq!((saved, path.clone()), (attached[0].attachment.clone(), out.join("report.txt")));
        assert_eq!(std::fs::read(&path).unwrap(), b"findings");
        let again = save(&js, &shown.handle, None, Some(&out)).await.unwrap_err().to_string();
        assert!(again.contains("exists: not overwriting it"), "{again}");
        assert_eq!(std::fs::read(&path).unwrap(), b"findings");

        let named = out.join("renamed.txt");
        let lying = Attachment { sha256: files::sha256(b"other"), ..shown.attachment.clone() };
        let mismatch = save(&js, &shown.handle, Some(&lying), Some(&named)).await.unwrap_err().to_string();
        assert!(mismatch.contains("does not match its reference"), "{mismatch}");
        assert!(!named.exists(), "nothing written when the bytes do not match");
        save(&js, &shown.handle, None, Some(&named)).await.unwrap();
        assert_eq!(std::fs::read(&named).unwrap(), b"findings");

        let unknown = message.attachment_handle(Uuid::now_v7());
        assert!(save(&js, &unknown, None, Some(&out)).await.unwrap_err().to_string().contains("is not stored"));
    }
}
