//! Files attached to messages, from the CLI: reading those `--attach` names ([`Attach`]), storing them before the
//! message carrying them ([`store`]), listing them under a message ([`Listed`]) and saving one ([`save`], `ants
//! attachment get`), opening one sealed on a message between two humans ([`crate::sealed`]). The ants TUI attaches,
//! lists and saves through the same functions.

use std::collections::HashSet;
use std::io::{Read as _, Write as _};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use ants_daemon::files;
use ants_daemon::reading::{self, Reading};
use ants_proto::{
    Attachment, AttachmentHandle, AttachmentScope, Envelope, MAX_ATTACHMENT_SIZE, MAX_ATTACHMENTS, SealedAttachment,
    Uuid, guess_mime,
};
use anyhow::{Context as _, Result, bail, ensure};
use async_nats::jetstream;
use async_nats::jetstream::consumer::DeliverPolicy;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

use crate::sealed;

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
/// of the media type its extension gives ([`guess_mime`]).
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
    let attachment = files::reference(name, guess_mime(name), &bytes).with_context(|| format!("attaching {shown}"))?;
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
/// when the reference is invalid ([`Attachment::check`]); when the hub stored the message, if known, for the wait for
/// chunks still on their way ([`files::get_stored`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Listed {
    #[serde(flatten)]
    pub attachment: Attachment,
    #[serde(serialize_with = "display")]
    pub handle: AttachmentHandle,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
    #[serde(skip)]
    pub stored: Option<DateTime<Utc>>,
}

fn display<S: Serializer>(handle: &AttachmentHandle, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(handle)
}

impl Listed {
    /// The size of the file, sealed or not.
    pub fn size(&self) -> u64 {
        let Attachment { size, sealed, .. } = &self.attachment;
        if sealed.is_some() { size.saturating_sub(SealedAttachment::OVERHEAD) } else { *size }
    }

    /// Its line under the message: `📎 name (size) handle`, the size that of the file, sealed or not ([`Self::size`]),
    /// or with an invalid reference why it is unavailable.
    pub fn line(&self) -> String {
        let name = &self.attachment.name;
        match &self.unavailable {
            None => format!("📎 {name} ({}) {}", size(self.size()), self.handle),
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
            Listed { attachment: attachment.clone(), handle: handle_of(attachment.id), unavailable, stored: None }
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

/// The attachments of the message the hub stored on `subject` at `stored` ([`listed_on`]).
pub fn stored_on(subject: &str, attachments: &[Attachment], stored: DateTime<Utc>) -> Vec<Listed> {
    let listed = listed_on(subject, attachments);
    listed.into_iter().map(|listed| Listed { stored: Some(stored), ..listed }).collect()
}

/// The lines of `listed` under a message, each at `indent` and ending in a newline.
pub fn render(listed: &[Listed], indent: &str) -> String {
    listed.iter().map(|l| format!("{indent}{}\n", l.line())).collect()
}

/// Read the attachment under `handle` and check its bytes against `expected`, the reference of the message carrying
/// it, or without one against the reference stored with them; missing chunks are waited for as [`files::get_stored`]
/// does for a message the hub stored at `stored`.
pub async fn fetch(
    js: &jetstream::Context,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
    stored: Option<DateTime<Utc>>,
) -> Result<(Attachment, Vec<u8>)> {
    files::get_stored(js, handle, expected, stored).await
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

/// Read the attachment under `handle` and check its bytes ([`fetch`]) against `expected`, the reference of the message
/// carrying it, if given, which the hub stored at `stored`. When the store holds it as an attachment of a sealed message
/// ([`SealedAttachment::STORED_NAME`]), or `expected` is sealed, its reference and content key are those of the sealed
/// message carrying it, which `keyring` opens ([`sealed::carrier`]), and its bytes are opened; refused when that
/// message does not open on this device. Returns its reference and bytes.
pub async fn open(
    js: &jetstream::Context,
    keyring: Option<&sealed::Keyring>,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
    carried: Option<DateTime<Utc>>,
) -> Result<(Attachment, Vec<u8>)> {
    let sealed_expected = expected.is_some_and(|e| e.sealed.is_some());
    let (stored, bytes) = fetch(js, handle, expected.filter(|_| !sealed_expected), carried).await?;
    let stored_sealed = stored.name == SealedAttachment::STORED_NAME
        && stored.mime == Attachment::UNKNOWN_MIME
        && matches!(handle.scope, AttachmentScope::Inbox(_));
    if !stored_sealed && !sealed_expected {
        return Ok((stored, bytes));
    }
    match Box::pin(sealed::carrier(js, keyring, handle)).await? {
        sealed::Carrier::Opened(key, reference) => {
            let bytes = if reference.sealed.is_some() {
                sealed::open_attachment(&key, &reference, &bytes)?
            } else {
                ensure!(
                    bytes.len() as u64 == reference.size && files::sha256(&bytes) == reference.sha256,
                    "attachment {handle} does not match its reference: it changed or is incomplete"
                );
                bytes
            };
            Ok((reference, bytes))
        }
        sealed::Carrier::Unopened(state) => bail!(
            "attachment {handle} is sealed with a message this device does not open ({}): {}",
            state.placeholder(),
            match state {
                sealed::State::Forged => "it is forged",
                _ => "sent before this device had an encryption key, or not sealed for it",
            }
        ),
        sealed::Carrier::None if sealed_expected => bail!("attachment {handle} is on no sealed message stored"),
        sealed::Carrier::None => Ok((stored, bytes)),
    }
}

/// Save the attachment under `handle` where [`target`] puts it, read and opened as [`open`] does with `keyring`,
/// `expected` and `stored`: nothing is written when its bytes do not match. Returns its reference, the size of the file and the path
/// written.
pub async fn save(
    js: &jetstream::Context,
    keyring: Option<&sealed::Keyring>,
    handle: &AttachmentHandle,
    expected: Option<&Attachment>,
    stored: Option<DateTime<Utc>>,
    out: Option<&Path>,
) -> Result<(Attachment, u64, PathBuf)> {
    if let Some(path) = out.filter(|path| !path.is_dir())
        && path.exists()
    {
        bail!("{} exists: not overwriting it (-o names another path)", path.display());
    }
    let (attachment, bytes) = open(js, keyring, handle, expected, stored).await?;
    let path = target(out, &attachment.name);
    write_new(&path, &bytes)?;
    Ok((attachment, bytes.len() as u64, path))
}

/// `ants attachment get HANDLE [-o PATH]`.
pub async fn get(nats: &crate::Nats<'_>, handle: &AttachmentHandle, out: Option<&Path>) -> Result<()> {
    let (_, js) = nats.connect("ants-cli").await?;
    let human = nats.me.role.is_human();
    let keyring = if human { sealed::Keyring::load(nats, &js).await } else { None };
    let (attachment, bytes, path) = save_handle(&js, human, keyring.as_ref(), handle, out).await?;
    println!("saved {} ({}, {}) to {}", attachment.name, size(bytes), attachment.mime, path.display());
    Ok(())
}

/// [`save`] of the attachment under `handle`, with no message at hand. Missing chunks of one made over
/// [`files::RECENT`] ago are waited for when, read with a `human`'s credentials, the message carrying it was stored
/// within that time ([`carried_recently`]), as one from a leaf's outbox after a long outage.
async fn save_handle(
    js: &jetstream::Context,
    human: bool,
    keyring: Option<&sealed::Keyring>,
    handle: &AttachmentHandle,
    out: Option<&Path>,
) -> Result<(Attachment, u64, PathBuf)> {
    match save(js, keyring, handle, None, None, out).await {
        Err(e) if human && files::missing(&e) && !files::recent(handle, None, Utc::now()) => {
            match carried_recently(js, keyring, handle).await? {
                Some(stored) => save(js, keyring, handle, None, Some(stored), out).await,
                None => Err(e),
            }
        }
        saved => saved,
    }
}

/// When the hub stored the message carrying the attachment under `handle`, if within [`files::RECENT`]: looked for by
/// message gets, as a human's credentials read, among the messages stored on its message's subject since, a sealed
/// one opened with `keyring`.
async fn carried_recently(
    js: &jetstream::Context,
    keyring: Option<&sealed::Keyring>,
    handle: &AttachmentHandle,
) -> Result<Option<DateTime<Utc>>> {
    #[derive(Deserialize)]
    struct Carried {
        id: Uuid,
    }
    #[derive(Deserialize)]
    struct Carrying {
        #[serde(default)]
        attachments: Vec<Carried>,
    }
    let carries = |payload: &[u8]| {
        if let Ok(envelope) = serde_json::from_slice::<Envelope>(payload)
            && envelope.sealed.is_some()
        {
            return match keyring.and_then(|k| k.open(&envelope)) {
                Some(sealed::Opening::Opened(body)) => body.attachments.iter().any(|a| a.id == handle.id),
                _ => false,
            };
        }
        serde_json::from_slice::<Carrying>(payload).is_ok_and(|m| m.attachments.iter().any(|a| a.id == handle.id))
    };
    let since = std::time::SystemTime::now() - files::RECENT;
    let start = DeliverPolicy::ByStartTime { start_time: since.into() };
    let mut found = None;
    reading::read_back(js, "", Reading::Gets, &handle.message_subject(), start, |_, stored, _, payload| {
        if !carries(payload) {
            return ControlFlow::Continue(());
        }
        found = Some(stored.into());
        ControlFlow::Break(())
    })
    .await
    .with_context(|| format!("reading the messages that may carry attachment {handle}"))?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ants_proto::{Role, UserId};

    use super::*;
    use crate::auth::tests::Server;

    #[test]
    fn media_types_come_from_the_extension() {
        assert_eq!(guess_mime("Shot.PNG"), "image/png");
        assert_eq!(guess_mime("photo.jpeg"), "image/jpeg");
        assert_eq!(guess_mime("build.log"), "text/plain");
        assert_eq!(guess_mime("archive.tar.gz"), "application/gzip");
        for unknown in ["Makefile", "data.bin", "trailing."] {
            assert_eq!(guess_mime(unknown), Attachment::UNKNOWN_MIME, "{unknown}");
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
        let (saved, _, path) = save(&js, None, &shown.handle, Some(&shown.attachment), None, Some(&out)).await.unwrap();
        assert_eq!((saved, path.clone()), (attached[0].attachment.clone(), out.join("report.txt")));
        assert_eq!(std::fs::read(&path).unwrap(), b"findings");
        let again = save(&js, None, &shown.handle, None, None, Some(&out)).await.unwrap_err().to_string();
        assert!(again.contains("exists: not overwriting it"), "{again}");
        assert_eq!(std::fs::read(&path).unwrap(), b"findings");

        let named = out.join("renamed.txt");
        let lying = Attachment { sha256: files::sha256(b"other"), ..shown.attachment.clone() };
        let mismatch = save(&js, None, &shown.handle, Some(&lying), None, Some(&named)).await.unwrap_err().to_string();
        assert!(mismatch.contains("does not match its reference"), "{mismatch}");
        assert!(!named.exists(), "nothing written when the bytes do not match");
        save(&js, None, &shown.handle, None, None, Some(&named)).await.unwrap();
        assert_eq!(std::fs::read(&named).unwrap(), b"findings");

        let unknown = message.attachment_handle(Uuid::now_v7());
        assert!(
            save(&js, None, &unknown, None, None, Some(&out)).await.unwrap_err().to_string().contains("is not stored")
        );
    }

    #[tokio::test]
    async fn an_old_attachment_on_a_message_stored_just_now_is_waited_for() {
        let Some(server) = Server::start() else { return };
        let (alice, dev_a): (UserId, UserId) = ("alice".parse().unwrap(), "dev-a".parse().unwrap());
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        js.create_stream(ants_daemon::hub::stream_config()).await.unwrap();
        js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
        let out = server.dir().join("got");
        std::fs::create_dir_all(&out).unwrap();
        // UUIDv7 ids made in November 2023.
        let old = |n: u8| -> Uuid { format!("018bcfe5-6800-7000-8000-0000000000{n:02x}").parse().unwrap() };

        // Kept in a leaf's outbox through a long outage: its message is stored now, its chunks later.
        let kept = Attachment { id: old(0), ..files::reference("kept.txt", "text/plain", b"kept").unwrap() };
        let mut message = Envelope::new(alice.clone(), dev_a, "see attached");
        message.attachments = vec![kept.clone()];
        ants_daemon::bus::store(&js, message.subject(), &message, &message.id.to_string()).await.unwrap();
        let handle = message.attachment_handle(kept.id);
        let getting = tokio::spawn({
            let (js, handle, out) = (js.clone(), handle.clone(), out.clone());
            async move { save_handle(&js, true, None, &handle, Some(&out)).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        files::put(&js, &handle, &kept, b"kept").await.unwrap();
        let (saved, _, path) = getting.await.unwrap().unwrap();
        assert_eq!((saved, std::fs::read(path).unwrap()), (kept, b"kept".to_vec()));

        // One no recent message carries is not waited for.
        let started = std::time::Instant::now();
        let gone = save_handle(&js, true, None, &message.attachment_handle(old(1)), Some(&out)).await.unwrap_err();
        assert!(files::missing(&gone), "{gone:#}");
        assert!(started.elapsed() < files::SETTLE / 2, "not waited for");
    }
}
