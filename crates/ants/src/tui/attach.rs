//! Attachments in the chat: their lines under a message, a post or a DM, which of a message's attachments `o` opens
//! and `]` moves ([`Mark`]), opening one with the system's viewer ([`open`]) or saving it ([`save`]), and the files
//! `/attach` stages in the command bar for the next message sent from it ([`Staged`]), with the completion of their
//! paths ([`paths`]).

use std::fmt;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use ants_proto::{MAX_ATTACHMENTS, Uuid};
use anyhow::{Context as _, Result, bail};
use async_nats::jetstream;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use super::complete::Candidate;
use super::frame;
use crate::attachments::{self, File, Listed};
use crate::sealed::Keyring;

/// Most paths `/attach` completes.
const MAX_PATHS: usize = 200;

/// Which of a message's attachments keys act on: the first, unless `]` moved on from it on that message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mark<K> {
    on: Option<K>,
    at: usize,
}

impl<K> Default for Mark<K> {
    fn default() -> Self {
        Self { on: None, at: 0 }
    }
}

impl<K: Copy + PartialEq> Mark<K> {
    /// The index marked among the `count` attachments of message `on`.
    pub fn at(&self, on: K, count: usize) -> usize {
        if self.on == Some(on) && count > 0 { self.at % count } else { 0 }
    }

    /// Mark the next of the `count` attachments of message `on`, the first after the last.
    pub fn next(&mut self, on: K, count: usize) {
        let at = self.at(on, count) + 1;
        *self = Self { on: Some(on), at: if count == 0 { 0 } else { at % count } };
    }
}

/// The lines of `listed` under a message, each begun with `indent`; with `marked`, the attachment keys act on is
/// pointed at when there are several.
pub fn lines(listed: &[Listed], marked: Option<usize>, indent: &str) -> Vec<Line<'static>> {
    let several = listed.len() > 1;
    listed
        .iter()
        .enumerate()
        .map(|(at, l)| {
            let mut spans = vec![Span::raw(indent.to_owned())];
            spans.extend(line(l).spans);
            if several && marked == Some(at) {
                spans.push(Span::styled(" ◂", Style::new().add_modifier(Modifier::BOLD)));
            }
            Line::from(spans)
        })
        .collect()
}

/// An attachment's line: `📎 name (size) type`, or with an invalid reference why it is unavailable.
pub fn line(listed: &Listed) -> Line<'static> {
    let attachment = &listed.attachment;
    match &listed.unavailable {
        None => Line::from(vec![
            Span::raw(format!("📎 {} ({}) ", attachment.name, attachments::size(listed.size()))),
            Span::styled(attachment.mime.clone(), frame::dim()),
        ]),
        // An invalid name may hold control characters, which are escaped rather than sent to the terminal.
        Some(reason) => Line::styled(format!("📎 {:?} (unavailable: {reason})", attachment.name), frame::dim()),
    }
}

/// `listed`, refused when unavailable.
fn available(listed: &Listed) -> Result<&Listed> {
    if let Some(reason) = &listed.unavailable {
        bail!("{:?} is unavailable: {reason}", listed.attachment.name);
    }
    Ok(listed)
}

/// Fetch the attachment `listed` into a new directory of its own under the system's temporary one, readable by the
/// user alone, and open it with the system's viewer; one sealed opens with `keyring` first. Returns the line telling
/// so.
pub async fn open(js: &jetstream::Context, keyring: Option<&Keyring>, listed: &Listed) -> Result<String> {
    let path = fetched(js, keyring, listed).await?;
    let opener = opener();
    let status = tokio::process::Command::new(opener)
        .arg(&path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .with_context(|| format!("running {opener} to open {}", path.display()))?;
    if !status.success() {
        bail!("{opener} {status} opening {}", path.display());
    }
    Ok(format!("opened {} ({}) with {opener}", listed.attachment.name, attachments::size(listed.size())))
}

/// Fetch the attachment `listed`, checking its bytes and opening one sealed with `keyring` ([`attachments::open`]),
/// into a new private directory under the system's temporary one: the path of the file written.
pub async fn fetched(js: &jetstream::Context, keyring: Option<&Keyring>, listed: &Listed) -> Result<PathBuf> {
    let listed = available(listed)?;
    let (attachment, bytes) =
        attachments::open(js, keyring, &listed.handle, Some(&listed.attachment), listed.stored).await?;
    let dir = private_dir(&std::env::temp_dir(), attachment.id)?;
    let path = dir.join(&attachment.name);
    attachments::write_new(&path, &bytes)?;
    Ok(path)
}

/// A new directory under `under` for attachment `id`, created rather than reused so that no path made beforehand (a
/// link) is written through.
fn private_dir(under: &Path, id: Uuid) -> Result<PathBuf> {
    let dir = under.join(format!("ants-attachment-{id}-{}", Uuid::now_v7().simple()));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(&dir).with_context(|| format!("creating {}", dir.display()))?;
    Ok(dir)
}

/// The system's viewer of a file: `open` on macOS, `wslview` under WSL, else `xdg-open`.
fn opener() -> &'static str {
    let wsl = || {
        std::env::var_os("WSL_DISTRO_NAME").is_some()
            || std::fs::read_to_string("/proc/sys/kernel/osrelease")
                .is_ok_and(|release| release.to_ascii_lowercase().contains("microsoft"))
    };
    opener_on(std::env::consts::OS, wsl)
}

fn opener_on(os: &str, wsl: impl FnOnce() -> bool) -> &'static str {
    match os {
        "macos" => "open",
        "linux" if wsl() => "wslview",
        _ => "xdg-open",
    }
}

/// Save the attachment `listed` as `ants attachment get -o` does: to `out`, in it when a directory, else to its name in
/// the current directory, one sealed opened with `keyring`. Returns the line telling so.
pub async fn save(
    js: &jetstream::Context,
    keyring: Option<&Keyring>,
    listed: &Listed,
    out: Option<&Path>,
) -> Result<String> {
    let listed = available(listed)?;
    let expected = Some(&listed.attachment);
    let (attachment, size, path) = attachments::save(js, keyring, &listed.handle, expected, listed.stored, out).await?;
    Ok(format!("saved {} ({}, {}) to {}", attachment.name, attachments::size(size), attachment.mime, path.display()))
}

/// `path` with a leading `~/` in the home directory.
pub fn expand(path: &str) -> PathBuf {
    match (path.strip_prefix("~/"), std::env::home_dir()) {
        (Some(rest), Some(home)) => home.join(rest),
        _ if path == "~" => std::env::home_dir().unwrap_or_else(|| PathBuf::from(path)),
        _ => PathBuf::from(path),
    }
}

/// The files `/attach` staged for the next message the bar sends, and those of the messages being sent, to stage
/// again when the send fails.
#[derive(Default)]
pub struct Staged {
    files: Vec<File>,
    /// The files of each message sending, by the bar's text that sends it.
    sending: Vec<(String, Rc<[File]>)>,
}

impl fmt::Debug for Staged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.files.iter().map(|f| f.attachment.name.as_str()).collect();
        f.debug_struct("Staged").field("files", &names).field("sending", &self.sending.len()).finish()
    }
}

impl Staged {
    /// The names of the files staged.
    pub fn names(&self) -> Vec<&str> {
        self.files.iter().map(|f| f.attachment.name.as_str()).collect()
    }

    /// Stage the file at `path`, within the limits of `--attach`: the line telling so, or why it is not staged.
    pub fn add(&mut self, path: &str) -> Result<String> {
        if self.files.len() >= MAX_ATTACHMENTS {
            bail!("{MAX_ATTACHMENTS} files attached already: a message carries at most {MAX_ATTACHMENTS}");
        }
        let file = attachments::read(&expand(path))?;
        let line = format!(
            "attached {} ({}): sent with the next message",
            file.attachment.name,
            attachments::size(file.attachment.size)
        );
        self.files.push(file);
        Ok(line)
    }

    /// Drop the files staged: how many there were.
    pub fn clear(&mut self) -> usize {
        std::mem::take(&mut self.files).len()
    }

    /// Take the files staged for the message the bar's `text` sends, kept until [`Self::sent`] tells how it went; none
    /// when none is staged.
    pub fn take(&mut self, text: &str) -> Option<Rc<[File]>> {
        if self.files.is_empty() {
            return None;
        }
        let files: Rc<[File]> = std::mem::take(&mut self.files).into();
        self.sending.push((text.to_owned(), Rc::clone(&files)));
        Some(files)
    }

    /// Take how the send of the bar's `text` went: when it failed, its files are staged again, unless others are.
    pub fn sent(&mut self, text: &str, failed: bool) {
        let Some(at) = self.sending.iter().position(|(sent, _)| sent == text) else { return };
        let (_, files) = self.sending.remove(at);
        if failed && self.files.is_empty() {
            self.files = files.to_vec();
        }
    }
}

/// The completions of `typed`, the path being typed after `/attach`: the entries of its directory whose names start
/// with what follows its last `/`, directories ending in `/`; hidden ones only when that starts with a `.`.
pub fn paths(typed: &str) -> Vec<Candidate> {
    let (dir, start) = typed.rsplit_once('/').map_or(("", typed), |(dir, start)| (dir, start));
    let listed = match typed.rfind('/') {
        Some(_) if dir.is_empty() => PathBuf::from("/"),
        Some(_) => expand(dir),
        None => PathBuf::from("."),
    };
    let Ok(entries) = std::fs::read_dir(&listed) else { return Vec::new() };
    let lead = typed.rfind('/').map_or("", |at| &typed[..=at]);
    let mut found: Vec<(String, bool, u64)> = entries
        .filter_map(Result::ok)
        .take(MAX_PATHS * 4)
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            if !name.starts_with(start) || (name.starts_with('.') && !start.starts_with('.')) {
                return None;
            }
            // Followed, so that a link to a directory completes as one.
            let meta = std::fs::metadata(entry.path()).ok()?;
            Some((name, meta.is_dir(), meta.len()))
        })
        .collect();
    found.sort();
    found
        .into_iter()
        .take(MAX_PATHS)
        .map(|(name, dir, len)| {
            let (word, detail) = if dir {
                (format!("{lead}{name}/"), "directory".to_owned())
            } else {
                (format!("{lead}{name}"), attachments::size(len))
            };
            Candidate { word, detail }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use ants_daemon::files;
    use ants_proto::{Envelope, Role, UserId};

    use super::*;
    use crate::auth::tests::Server;

    fn listed(name: &str, bytes: &[u8]) -> Listed {
        let message = Envelope::new(UserId::human(), "dev-a".parse().unwrap(), "look");
        let attachment = files::reference(name, ants_proto::guess_mime(name), bytes).unwrap();
        attachments::listed(&[attachment], |id| message.attachment_handle(id)).remove(0)
    }

    #[test]
    fn attachments_show_name_size_and_type_and_the_marked_one_of_several() {
        let (shot, log) = (listed("shot.png", &[0; 2048]), listed("build.log", b"ok"));
        let shown: Vec<String> =
            lines(&[shot.clone(), log.clone()], Some(1), "  ").iter().map(ToString::to_string).collect();
        assert_eq!(shown, ["  📎 shot.png (2.0 KiB) image/png", "  📎 build.log (2 B) text/plain ◂"]);
        assert_eq!(lines(std::slice::from_ref(&shot), Some(0), "")[0].to_string(), "📎 shot.png (2.0 KiB) image/png");
        let bad = Listed { unavailable: Some("its id is repeated".into()), ..log };
        assert_eq!(line(&bad).to_string(), "📎 \"build.log\" (unavailable: its id is repeated)");

        let mut mark = Mark::default();
        assert_eq!(mark.at(7u64, 3), 0);
        mark.next(7, 3);
        mark.next(7, 3);
        assert_eq!((mark.at(7, 3), mark.at(8, 3)), (2, 0), "a mark holds on its message only");
        mark.next(7, 3);
        assert_eq!(mark.at(7, 3), 0, "past the last, the first");
    }

    #[test]
    fn the_viewer_is_that_of_the_system() {
        assert_eq!(opener_on("macos", || true), "open");
        assert_eq!(opener_on("linux", || true), "wslview");
        assert_eq!(opener_on("linux", || false), "xdg-open");
        assert_eq!(opener_on("freebsd", || false), "xdg-open");
    }

    #[test]
    fn staged_files_go_with_one_send_and_come_back_when_it_fails() {
        let dir = ants_test_support::scratch_dir("staged");
        std::fs::write(dir.join("a.txt"), b"a").unwrap();
        let path = dir.join("a.txt").display().to_string();
        let mut staged = Staged::default();
        assert_eq!(staged.take("hi"), None, "nothing to attach");
        assert_eq!(staged.add(&path).unwrap(), "attached a.txt (1 B): sent with the next message");
        assert!(staged.add(&dir.display().to_string()).unwrap_err().to_string().contains("is no file"));
        let files = staged.take("hi").unwrap();
        assert_eq!((files.len(), staged.names().len()), (1, 0));
        staged.sent("hi", true);
        assert_eq!(staged.names(), ["a.txt"], "a failed send stages its files again");
        staged.take("hi").unwrap();
        staged.sent("hi", false);
        assert!(staged.names().is_empty());
        for _ in 0..MAX_ATTACHMENTS {
            staged.add(&path).unwrap();
        }
        assert!(staged.add(&path).unwrap_err().to_string().contains("at most 8"));
        assert_eq!(staged.clear(), MAX_ATTACHMENTS);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn attach_completes_the_paths_of_the_directory_typed() {
        let dir = ants_test_support::scratch_dir("paths");
        std::fs::create_dir(dir.join("shots")).unwrap();
        std::fs::write(dir.join("shot.png"), b"png").unwrap();
        std::fs::write(dir.join(".hidden"), b"").unwrap();
        let root = format!("{}/", dir.display());
        let words = |typed: &str| paths(typed).into_iter().map(|c| c.word).collect::<Vec<_>>();
        assert_eq!(words(&format!("{root}sh")), [format!("{root}shot.png"), format!("{root}shots/")]);
        assert_eq!(words(&root).len(), 2, "hidden files only when asked for");
        assert_eq!(words(&format!("{root}.h")), [format!("{root}.hidden")]);
        assert_eq!(paths(&format!("{root}shot.png"))[0].detail, "3 B");
        assert!(words(&format!("{root}missing/")).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_attachment_opens_from_a_private_copy_and_saves_as_the_cli_does() {
        let Some(server) = Server::start() else { return };
        let alice: UserId = "alice".parse().unwrap();
        let (_, js) = server.connect_as(&alice, Role::Human).await;
        js.create_stream(ants_daemon::hub::files_config()).await.unwrap();
        let source = server.dir().join("report.txt");
        std::fs::write(&source, b"findings").unwrap();
        let files = attachments::read_all(std::slice::from_ref(&source)).unwrap();
        let mut message = Envelope::new(alice, "dev-a".parse().unwrap(), "see attached");
        message.attachments = attachments::references(&files);
        attachments::store(&js, &files, |id| message.attachment_handle(id)).await.unwrap();
        let [shown] = attachments::listed_on(&message.subject(), &message.attachments).try_into().unwrap();

        let path = fetched(&js, None, &shown).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"findings");
        let dir = path.parent().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(std::fs::metadata(dir).unwrap().permissions().mode() & 0o777, 0o700);
        }
        let again = fetched(&js, None, &shown).await.unwrap();
        assert_ne!(again.parent(), Some(dir), "each open has a directory of its own");
        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(again.parent().unwrap());

        let out = server.dir().join("out");
        std::fs::create_dir(&out).unwrap();
        let line = save(&js, None, &shown, Some(&out)).await.unwrap();
        assert_eq!(line, format!("saved report.txt (8 B, text/plain) to {}", out.join("report.txt").display()));
        let gone = Listed { unavailable: Some("its id is repeated".into()), ..shown };
        assert!(save(&js, None, &gone, Some(&out)).await.unwrap_err().to_string().contains("is unavailable"));
    }
}
