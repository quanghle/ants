//! Files attached to messages: the reference a message carries ([`Attachment`]) and where the bytes are stored
//! ([`AttachmentHandle`]).

use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ChannelId, ChannelPost, Envelope, Error, NotesPost, ThreadId, UserId, subject};
#[cfg(doc)]
use crate::{Permissions, SealedBody};

/// The store of the bytes of attachments, a JetStream stream named [`FILES_STREAM`] that keeps them as chunks on
/// [`AttachmentHandle::chunk_subject`]s, laid out as an Object Store's chunks but with no object metadata: the reference
/// a message carries is that, and the subject of the metadata, a base64 token, cannot be confined to the readers of a
/// message, as the chunk subjects are ([`Permissions::for_user`]).
pub const FILES_BUCKET: &str = "ants_files";
/// The stream of [`FILES_BUCKET`].
pub const FILES_STREAM: &str = "OBJ_ants_files";
/// Most bytes of an attachment one chunk holds.
pub const ATTACHMENT_CHUNK: usize = 128 * 1024;
/// Room for headers [`FILES_STREAM`] leaves on a chunk, which carry the reference on the first ([`ATTACHMENT_HEADER`]).
pub const ATTACHMENT_HEADROOM: usize = 4 * 1024;
/// Most bytes one attachment has.
pub const MAX_ATTACHMENT_SIZE: u64 = 8 * 1024 * 1024;
/// Most attachments one message carries.
pub const MAX_ATTACHMENTS: usize = 8;
/// Most bytes of an attachment's name.
pub const MAX_ATTACHMENT_NAME: usize = 255;
/// Header of an attachment's first chunk carrying its [`Attachment`] as JSON, so a reader holding only its handle
/// learns its name, type, size and digest.
pub const ATTACHMENT_HEADER: &str = "Ants-Attachment";
/// Most bytes of an attachment's media type.
const MAX_MIME: usize = 127;
/// The media types of attachments shown as images ([`Attachment::is_image`]).
pub const IMAGE_MIMES: [&str; 4] = ["image/png", "image/jpeg", "image/gif", "image/webp"];

/// A file attached to a message: what the message carries of it. Its bytes are stored, before the message, under the
/// [`AttachmentHandle`] the subject of that message and the id give, where only the readers of that message read them.
///
/// `size` and `sha256` describe the bytes as stored. Readers ignore fields they do not know, so later ones, such as how
/// the bytes of an attachment on a sealed message are sealed ([`Attachment::sealed`]), leave them working.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    /// A UUIDv7, unique among the attachments of the message.
    pub id: Uuid,
    /// The file's name, without directories.
    pub name: String,
    /// Its media type, `type/subtype` in lowercase.
    pub mime: String,
    /// Its length in bytes.
    pub size: u64,
    /// The SHA-256 of its bytes, in lowercase hex.
    pub sha256: String,
    /// How its bytes are sealed, on an attachment of a sealed message: its reference then travels only inside the
    /// message's [`SealedBody`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sealed: Option<SealedAttachment>,
}

/// How the bytes of an attachment of a sealed message are sealed: stored as the XSalsa20-Poly1305 encryption
/// (`crypto_secretbox`) of the file's bytes under the message's content key and a random nonce of their own, so the
/// [`Attachment::size`] and [`Attachment::sha256`] of its reference are those of the ciphertext. The first chunk's
/// [`ATTACHMENT_HEADER`] names it [`SealedAttachment::STORED_NAME`], of media type [`Attachment::UNKNOWN_MIME`],
/// without this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedAttachment {
    /// The 24-byte nonce, in standard padded base64.
    pub nonce: String,
}

impl SealedAttachment {
    /// The name the store holds an attachment of a sealed message under.
    pub const STORED_NAME: &str = "sealed";
    /// How many bytes longer sealing makes an attachment: XSalsa20-Poly1305's tag.
    pub const OVERHEAD: u64 = 16;
}

impl Attachment {
    /// The media type of a file whose type is unknown.
    pub const UNKNOWN_MIME: &str = "application/octet-stream";

    /// A new attachment's reference, with a fresh id, checked ([`Attachment::check`]).
    pub fn new(
        name: impl Into<String>,
        mime: impl Into<String>,
        size: u64,
        sha256: impl Into<String>,
    ) -> Result<Self, Error> {
        let attachment = Self {
            id: Uuid::now_v7(),
            name: name.into(),
            mime: mime.into(),
            size,
            sha256: sha256.into(),
            sealed: None,
        };
        attachment.check()?;
        Ok(attachment)
    }

    /// Check the reference: a UUIDv7 id, a name of 1 to [`MAX_ATTACHMENT_NAME`] bytes, neither `.` nor `..`, without
    /// `/`, `\` or control characters, so it names a file in one directory on every system; a lowercase
    /// `type/subtype`; at most [`MAX_ATTACHMENT_SIZE`] bytes; and a SHA-256 in lowercase hex.
    pub fn check(&self) -> Result<(), Error> {
        let invalid = |reason| Err(Error::InvalidAttachment { name: self.name.clone(), reason });
        let name = &self.name;
        if self.id.get_version_num() != 7 {
            return invalid("its id is no UUIDv7");
        }
        if name.is_empty() || name.len() > MAX_ATTACHMENT_NAME || name == "." || name == ".." {
            return invalid("use a file name of 1-255 bytes");
        }
        if name.chars().any(|c| c == '/' || c == '\\' || c.is_control()) {
            return invalid("its name holds '/', '\\' or a control character");
        }
        if !valid_mime(&self.mime) {
            return invalid("its media type is no lowercase type/subtype");
        }
        if self.size > MAX_ATTACHMENT_SIZE {
            return invalid("it is over 8 MiB");
        }
        if self.sha256.len() != 64 || !self.sha256.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return invalid("its sha256 is no lowercase hex SHA-256");
        }
        Ok(())
    }

    /// How many chunks hold its bytes: one at least, empty for an empty file.
    pub fn chunks(&self) -> u64 {
        self.size.div_ceil(ATTACHMENT_CHUNK as u64).max(1)
    }

    /// Whether it is an image of a type harnesses and MCP clients take as image content ([`IMAGE_MIMES`]).
    pub fn is_image(&self) -> bool {
        IMAGE_MIMES.contains(&self.mime.as_str())
    }
}

/// The media type of a file named `name`, guessed from its extension; [`Attachment::UNKNOWN_MIME`] when unknown.
pub fn guess_mime(name: &str) -> &'static str {
    // A leading dot starts a hidden file's name, such as `.bashrc`, not an extension.
    let Some((_, extension)) = name.rsplit_once('.').filter(|(stem, _)| !stem.is_empty()) else {
        return Attachment::UNKNOWN_MIME;
    };
    match extension.to_ascii_lowercase().as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "ico" => "image/vnd.microsoft.icon",
        "tif" | "tiff" => "image/tiff",
        "heic" => "image/heic",
        "avif" => "image/avif",
        "md" | "markdown" => "text/markdown",
        "csv" => "text/csv",
        "tsv" => "text/tab-separated-values",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" | "cjs" => "text/javascript",
        "diff" | "patch" => "text/x-diff",
        "txt" | "text" | "log" | "rs" | "py" | "go" | "ts" | "tsx" | "jsx" | "c" | "h" | "cc" | "cpp" | "hpp"
        | "java" | "kt" | "swift" | "rb" | "sh" | "bash" | "zsh" | "fish" | "ps1" | "toml" | "yaml" | "yml" | "ini"
        | "cfg" | "conf" | "sql" | "lock" => "text/plain",
        "json" => "application/json",
        "xml" => "application/xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" | "tgz" => "application/gzip",
        "tar" => "application/x-tar",
        "wasm" => "application/wasm",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mov" => "video/quicktime",
        _ => Attachment::UNKNOWN_MIME,
    }
}

/// Whether `mime` is a lowercase `type/subtype` of the characters RFC 6838 allows.
fn valid_mime(mime: &str) -> bool {
    let part = |s: &str| {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-".contains(&b))
    };
    mime.len() <= MAX_MIME && mime.split_once('/').is_some_and(|(kind, sub)| part(kind) && part(sub))
}

/// Check the attachments of one message: at most [`MAX_ATTACHMENTS`], with distinct ids, each valid
/// ([`Attachment::check`]).
pub fn check_attachments(attachments: &[Attachment]) -> Result<(), Error> {
    if attachments.len() > MAX_ATTACHMENTS {
        return Err(Error::TooManyAttachments(attachments.len()));
    }
    let mut ids = HashSet::new();
    for attachment in attachments {
        attachment.check()?;
        if !ids.insert(attachment.id) {
            return Err(Error::InvalidAttachment { name: attachment.name.clone(), reason: "its id is repeated" });
        }
    }
    Ok(())
}

/// What a message carrying an attachment was sent to, as its subject says.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AttachmentScope {
    /// An inbox message to this user.
    Inbox(UserId),
    /// A post to the notes of this thread.
    Notes(ThreadId),
    /// A post to this channel.
    Chan(ChannelId),
}

/// Where an attachment's bytes are stored: `<from>.<kind>.<target>.<id>`, its sender, the kind and target of the subject
/// of the message carrying it (`inbox.<to>`, `notes.<thread>` or `chan.<channel>`), and its id. The sender comes first,
/// so credentials confine each user to writing its own, and the rest follows the message's subject, so they confine
/// each user to reading those of the messages it reads ([`subject::readable_by`]). Tools and the CLI name an attachment
/// by its handle.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttachmentHandle {
    pub from: UserId,
    pub scope: AttachmentScope,
    pub id: Uuid,
}

impl AttachmentHandle {
    /// The handle of attachment `id` of the message on `subject`, an inbox, notes or channel subject; `None` for others.
    pub fn of_message(subject: &str, id: Uuid) -> Option<Self> {
        let (from, scope) = if let Some((to, from)) = subject::parse_inbox(subject) {
            (from, AttachmentScope::Inbox(to))
        } else if let Some((thread, from)) = subject::parse_notes(subject) {
            (from, AttachmentScope::Notes(thread))
        } else {
            let (channel, from) = subject::parse_chan(subject)?;
            (from, AttachmentScope::Chan(channel))
        };
        Some(Self { from, scope, id })
    }

    /// The subject of the message carrying the attachment.
    pub fn message_subject(&self) -> String {
        match &self.scope {
            AttachmentScope::Inbox(to) => subject::inbox(to, &self.from),
            AttachmentScope::Notes(thread) => subject::notes(thread, &self.from),
            AttachmentScope::Chan(channel) => subject::chan(channel, &self.from),
        }
    }

    /// Where chunk `n` of the attachment (from 0) is stored in [`FILES_STREAM`].
    pub fn chunk_subject(&self, n: u64) -> String {
        format!("$O.{FILES_BUCKET}.C.{self}.{n}")
    }

    /// Where a client on a leaf node keeps chunk `n` while the hub is out of reach ([`subject::outbox`]).
    pub fn outbox_chunk_subject(&self, n: u64) -> String {
        format!("ants.out.files.{self}.{n}")
    }

    /// Whether the credentials of the ant `ant` read the attachment: whether they read the message carrying it.
    pub fn readable_by(&self, ant: &UserId) -> bool {
        subject::readable_by(ant, &self.message_subject())
    }
}

impl fmt::Display for AttachmentHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, target) = match &self.scope {
            AttachmentScope::Inbox(to) => ("inbox", to.as_str()),
            AttachmentScope::Notes(thread) => ("notes", thread.as_str()),
            AttachmentScope::Chan(channel) => ("chan", channel.as_str()),
        };
        write!(f, "{}.{kind}.{target}.{}", self.from, self.id)
    }
}

impl FromStr for AttachmentHandle {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        let invalid = || Error::InvalidAttachmentHandle(s.to_owned());
        let mut tokens = s.split('.');
        let (Some(from), Some(kind), Some(target), Some(id), None) =
            (tokens.next(), tokens.next(), tokens.next(), tokens.next(), tokens.next())
        else {
            return Err(invalid());
        };
        let subject = format!("ants.{kind}.{target}.{from}");
        let id = id.parse().map_err(|_| invalid())?;
        Self::of_message(&subject, id).ok_or_else(invalid)
    }
}

impl Envelope {
    /// The handle of the attachment `id` of the message.
    pub fn attachment_handle(&self, id: Uuid) -> AttachmentHandle {
        AttachmentHandle { from: self.from.clone(), scope: AttachmentScope::Inbox(self.to.clone()), id }
    }
}

impl NotesPost {
    /// The handle of the attachment `id` of the post.
    pub fn attachment_handle(&self, id: Uuid) -> AttachmentHandle {
        AttachmentHandle { from: self.from.clone(), scope: AttachmentScope::Notes(self.thread_id.clone()), id }
    }
}

impl ChannelPost {
    /// The handle of the attachment `id` of the post.
    pub fn attachment_handle(&self, id: Uuid) -> AttachmentHandle {
        AttachmentHandle { from: self.from.clone(), scope: AttachmentScope::Chan(self.channel.clone()), id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p;

    fn attachment(name: &str) -> Attachment {
        Attachment::new(name, "image/png", 300 * 1024, "ab".repeat(32)).unwrap()
    }

    #[test]
    fn a_reference_names_one_file_of_a_known_size_and_digest() {
        let shot = attachment("shot 1.png");
        assert_eq!(shot.chunks(), 3);
        assert_eq!(Attachment { size: 0, ..shot.clone() }.chunks(), 1, "an empty file has one empty chunk");
        assert_eq!(Attachment { size: 128 * 1024, ..shot.clone() }.chunks(), 1);
        assert!(Attachment { name: "ünïcode.txt".into(), ..shot.clone() }.check().is_ok());
        for name in ["", ".", "..", "a/b", "a\\b", "a\nb", &"x".repeat(256)] {
            assert!(Attachment { name: name.into(), ..shot.clone() }.check().is_err(), "{name:?}");
        }
        for mime in ["image", "Image/png", "image/", "/png", "text/plain; charset=utf-8"] {
            assert!(Attachment { mime: mime.into(), ..shot.clone() }.check().is_err(), "{mime}");
        }
        assert!(Attachment { size: MAX_ATTACHMENT_SIZE + 1, ..shot.clone() }.check().is_err());
        assert!(Attachment { sha256: "AB".repeat(32), ..shot.clone() }.check().is_err());
        assert!(Attachment { sha256: "ab".repeat(31), ..shot.clone() }.check().is_err());
        assert!(Attachment { id: Uuid::nil(), ..shot.clone() }.check().is_err());

        assert!(check_attachments(&vec![shot.clone(); 1]).is_ok());
        assert!(check_attachments(&[shot.clone(), shot]).is_err(), "repeated id");
        let many: Vec<_> = (0..=MAX_ATTACHMENTS).map(|_| attachment("a")).collect();
        assert_eq!(check_attachments(&many), Err(Error::TooManyAttachments(MAX_ATTACHMENTS + 1)));
        assert!(check_attachments(&many[1..]).is_ok());
    }

    #[test]
    fn a_media_type_is_guessed_from_the_extension() {
        for (name, mime) in [
            ("shot.PNG", "image/png"),
            ("a.b.jpeg", "image/jpeg"),
            ("notes.md", "text/markdown"),
            ("main.rs", "text/plain"),
            ("data.json", "application/json"),
            ("icon.ico", "image/vnd.microsoft.icon"),
            (".bashrc", Attachment::UNKNOWN_MIME),
            ("README", Attachment::UNKNOWN_MIME),
            ("x.unknown", Attachment::UNKNOWN_MIME),
        ] {
            assert_eq!(guess_mime(name), mime, "{name}");
            let guessed = Attachment::new(name, guess_mime(name), 1, "ab".repeat(32)).unwrap();
            assert_eq!(guessed.is_image(), mime.starts_with("image/") && mime != "image/vnd.microsoft.icon");
        }
    }

    #[test]
    fn a_handle_follows_the_subject_of_the_message_sender_first() {
        let id = Uuid::now_v7();
        let message = Envelope::new(p("alice"), p("dev-a"), "look");
        let handle = message.attachment_handle(id);
        assert_eq!(handle.to_string(), format!("alice.inbox.dev-a.{id}"));
        assert_eq!(handle.chunk_subject(2), format!("$O.ants_files.C.alice.inbox.dev-a.{id}.2"));
        assert_eq!(handle.outbox_chunk_subject(0), format!("ants.out.files.alice.inbox.dev-a.{id}.0"));
        assert_eq!(handle.message_subject(), message.subject());
        assert_eq!(AttachmentHandle::of_message(&message.subject(), id), Some(handle.clone()));
        assert_eq!(handle.to_string().parse(), Ok(handle.clone()));
        assert!(handle.readable_by(&p("dev-a")) && !handle.readable_by(&p("dev-b")));

        let notes = NotesPost::new("t1".parse().unwrap(), p("dev-a"), "x").attachment_handle(id);
        assert_eq!(notes.to_string(), format!("dev-a.notes.t1.{id}"));
        let post = ChannelPost::new("ops".parse().unwrap(), p("dev-b"), "x").attachment_handle(id);
        assert_eq!(post.to_string(), format!("dev-b.chan.ops.{id}"));
        for readable in [&notes, &post] {
            assert_eq!(readable.to_string().parse().as_ref(), Ok(readable));
            assert!(readable.readable_by(&p("dev-c")));
        }

        assert_eq!(AttachmentHandle::of_message(&subject::dead(&p("dev-a")), id), None);
        for malformed in [
            "alice.inbox.dev-a".to_owned(),
            format!("alice.dead.dev-a.{id}"),
            format!("alice.inbox.dev-a.{id}.0"),
            "a.inbox.b.c".into(),
        ] {
            assert!(malformed.parse::<AttachmentHandle>().is_err(), "{malformed}");
        }
    }
}
