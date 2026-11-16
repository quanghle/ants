//! Handing a message, or a thread by its TL;DR, to another user on a new thread: `ants ask --forward` and `/forward`
//! in `ants chat`. The new thread carries the message quoted with its author, its attachments stored again for the
//! recipient, and the source thread's id, plus the human's note; and links to the source thread
//! ([`Envelope::origin_thread`]) as a spin-off does, so the source thread's delegation tree shows it. The recipient
//! gets only what is forwarded: no grant to the source thread comes with it.

use ants_proto::{Envelope, MAX_ATTACHMENTS, ThreadId, ThreadSummary, UserId};
use anyhow::{Result, bail, ensure};
use async_nats::jetstream;

use crate::attachments::{self, File};
use crate::cmd::send;
use crate::thread::{Entry, Kind};
use crate::{humans, sealed};

/// What a forward carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A message, post or note, quoted with its author and attachments.
    Message(Box<Entry>),
    /// A thread, by its TL;DR.
    Thread(Box<ThreadSummary>),
}

impl Source {
    /// The thread forwarded from.
    pub fn thread(&self) -> &ThreadId {
        match self {
            Self::Message(entry) => &entry.thread_id,
            Self::Thread(tldr) => &tldr.thread_id,
        }
    }

    /// Why the source cannot be forwarded, if it cannot: a message needs an author and text that opened on this
    /// device.
    pub fn check(&self) -> Result<()> {
        let Self::Message(entry) = self else { return Ok(()) };
        let what = entry.id.map_or_else(|| format!("entry {}", entry.seq), |id| format!("message {id}"));
        ensure!(entry.kind.reopens() && !entry.forged, "{what} is no message, post or note to forward");
        ensure!(entry.from.is_some(), "{what} names no author");
        if entry.sealed.is_some_and(|s| s != sealed::State::Opened) {
            bail!("{what} is sealed and does not open on this device: there is nothing to forward");
        }
        Ok(())
    }

    /// The text of the new thread's first message: `note`, if any, then the source quoted with where it comes from.
    pub fn text(&self, note: &str) -> String {
        let (head, body, listed) = match self {
            Self::Message(entry) => {
                let from = entry.from.as_ref().map_or("unknown", UserId::as_str);
                let place = match (&entry.channel, entry.kind) {
                    (Some(channel), Kind::Post) => format!(" in #{channel}"),
                    _ => String::new(),
                };
                let at = crate::thread::time(entry.stored_at);
                let head = format!("Forwarded from {from}{place} on thread {}, {at} UTC:", entry.thread_id);
                let listed = entry.attachments.iter().map(|l| format!("📎 {} (attached)", l.attachment.name)).collect();
                (head, entry.text.as_str(), listed)
            }
            Self::Thread(tldr) => {
                let at = crate::thread::time(tldr.at);
                (
                    format!("Forwarded thread {}, its TL;DR by {}, {at} UTC:", tldr.thread_id, tldr.by),
                    tldr.text.as_str(),
                    Vec::new(),
                )
            }
        };
        let quoted: Vec<String> = body
            .trim_end()
            .lines()
            .map(str::to_owned)
            .chain(listed)
            .map(|l| format!("> {l}").trim_end().to_owned())
            .collect();
        let forwarded = format!("{head}\n{}", quoted.join("\n"));
        match note.trim() {
            "" => forwarded,
            note => format!("{note}\n\n{forwarded}"),
        }
    }

    /// The new thread's first message from `me` to `to`, with `note`: owned by `me`, a human, and linked to the source
    /// thread.
    pub fn envelope(&self, me: &humans::Me, to: UserId, note: &str) -> Envelope {
        send::ask_envelope(me, None, to, self.text(note)).with_origin_thread(Some(self.thread().clone()))
    }

    /// The attachments of a forwarded message, read and opened with `keyring` as `ants attachment get` does, to store
    /// again for the new message, with fresh ids; then `extra`, those attached besides. At most [`MAX_ATTACHMENTS`] in
    /// all.
    pub async fn files(
        &self,
        js: &jetstream::Context,
        keyring: Option<&sealed::Keyring>,
        extra: Vec<File>,
    ) -> Result<Vec<File>> {
        let listed = match self {
            Self::Message(entry) => entry.attachments.as_slice(),
            Self::Thread(_) => &[],
        };
        let count = listed.len() + extra.len();
        ensure!(count <= MAX_ATTACHMENTS, "{count} files to attach: a message carries at most {MAX_ATTACHMENTS}");
        let mut files = Vec::with_capacity(count);
        for l in listed {
            if let Some(why) = &l.unavailable {
                bail!("attachment {:?} of the message is unavailable: {why}", l.attachment.name);
            }
            let (attachment, bytes) = attachments::open(js, keyring, &l.handle, Some(&l.attachment), l.stored).await?;
            let attachment = ants_daemon::files::reference(&attachment.name, &attachment.mime, &bytes)?;
            files.push(File { attachment, bytes });
        }
        files.extend(extra);
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::{Attachment, AttachmentHandle, AttachmentScope, Uuid};
    use chrono::TimeZone as _;

    use super::*;
    use crate::thread::Entry;

    fn p(name: &str) -> UserId {
        name.parse().unwrap()
    }

    fn message(text: &str) -> Entry {
        let at = chrono::Utc.with_ymd_and_hms(2026, 11, 16, 9, 30, 0).unwrap();
        let mut entry = Entry::new(7, at, Kind::Message, ThreadId::generate());
        (entry.id, entry.from, entry.to, entry.text) =
            (Some(Uuid::now_v7()), Some(p("dev-a")), Some(p("alice")), text.to_owned());
        entry
    }

    fn me() -> humans::Me {
        humans::Me { id: p("alice"), role: ants_proto::Role::Human, assistant_of: None }
    }

    #[test]
    fn a_forwarded_message_is_quoted_with_its_author_thread_and_files_after_the_note() {
        let mut entry = message("the build fails on arm64\n\nsee the log");
        let attachment = Attachment::new("build.log", "text/plain", 3, "0".repeat(64)).unwrap();
        let handle =
            AttachmentHandle { from: p("dev-a"), scope: AttachmentScope::Inbox(p("alice")), id: attachment.id };
        entry.attachments = attachments::listed(&[attachment], |_| handle.clone());
        let thread = entry.thread_id.clone();
        let source = Source::Message(Box::new(entry));
        assert_eq!(
            source.text("  can you take this?  "),
            format!(
                "can you take this?\n\nForwarded from dev-a on thread {thread}, 2026-11-16 09:30:00 UTC:\n\
                 > the build fails on arm64\n>\n> see the log\n> 📎 build.log (attached)"
            )
        );
        assert!(source.text("").starts_with("Forwarded from dev-a"), "no note, no blank lines before");
    }

    #[test]
    fn a_forwarded_thread_carries_its_tldr_and_the_new_thread_links_to_the_source() {
        let thread = ThreadId::generate();
        let tldr = ThreadSummary {
            thread_id: thread.clone(),
            text: "dev-a fixed the flaky test".to_owned(),
            by: p("messenger"),
            asker: p("alice"),
            at: chrono::Utc.with_ymd_and_hms(2026, 11, 16, 10, 0, 0).unwrap(),
            upto: 9,
            tokens: 40,
        };
        let source = Source::Thread(Box::new(tldr));
        let envelope = source.envelope(&me(), p("dev-b"), "follow up");
        assert_ne!(envelope.thread_id, thread, "a new thread");
        assert_eq!((envelope.origin_thread.as_ref(), envelope.owner.as_ref()), (Some(&thread), Some(&p("alice"))));
        assert_eq!(
            envelope.text,
            format!(
                "follow up\n\nForwarded thread {thread}, its TL;DR by messenger, 2026-11-16 10:00:00 UTC:\n\
                 > dev-a fixed the flaky test"
            )
        );
    }

    #[test]
    fn only_a_message_with_an_author_and_opened_text_is_forwarded() {
        assert!(Source::Message(Box::new(message("x"))).check().is_ok());
        let mut sealed = message("[sealed]");
        sealed.sealed = Some(sealed::State::Unreadable);
        assert!(Source::Message(Box::new(sealed)).check().is_err());
        let mut tool = message("Read file");
        tool.kind = Kind::Tool;
        assert!(Source::Message(Box::new(tool)).check().is_err());
    }
}
