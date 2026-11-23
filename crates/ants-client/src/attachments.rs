//! The files a message carries as a reader lists them under it ([`Listed`]): each reference with the handle naming
//! where its bytes are, and why one is unavailable.

use std::collections::HashSet;

use ants_proto::{Attachment, AttachmentHandle, MAX_ATTACHMENTS, SealedAttachment, Uuid};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

/// `len` and `limit` for people ([`size`]), as exact bytes when they would read the same.
pub fn sizes(len: u64, limit: u64) -> (String, String) {
    let (shown, max) = (size(len), size(limit));
    if shown == max && len != limit { (format!("{len} bytes"), format!("{limit} bytes")) } else { (shown, max) }
}

/// An attachment as a message lists it: its reference, the handle naming where its bytes are, and why it is unavailable
/// when the reference is invalid ([`Attachment::check`]); when the hub stored the message, if known, for the wait for
/// chunks still on their way.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listed {
    #[serde(flatten)]
    pub attachment: Attachment,
    #[serde(serialize_with = "display", deserialize_with = "parsed")]
    pub handle: AttachmentHandle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stored: Option<DateTime<Utc>>,
}

/// A handle as its text, which names an attachment to people and tools.
pub fn display<S: Serializer>(handle: &AttachmentHandle, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(handle)
}

fn parsed<'de, D: Deserializer<'de>>(d: D) -> Result<AttachmentHandle, D::Error> {
    String::deserialize(d)?.parse().map_err(serde::de::Error::custom)
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

#[cfg(test)]
mod tests {
    use ants_proto::{Envelope, UserId};

    use super::*;

    fn reference(name: &str, bytes: &[u8]) -> Attachment {
        let sha256 = "ab".repeat(32);
        Attachment { id: Uuid::now_v7(), ..Attachment::new(name, "text/plain", bytes.len() as u64, sha256).unwrap() }
    }

    #[test]
    fn an_invalid_reference_is_listed_unavailable() {
        let message = Envelope::new(UserId::human(), "dev-a".parse().unwrap(), "look");
        let good = reference("a.txt", b"a");
        let bad = Attachment { name: "evil\u{1b}[2J".into(), ..reference("b", b"b") };
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
}
