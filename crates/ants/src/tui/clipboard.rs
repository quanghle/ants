//! Occasional clipboard commands, with OSC 52 as the remote and commandless fallback.

use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{Context as _, Result, bail};
use pulldown_cmark::{Event, Parser, Tag, TagEnd};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Paragraph;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::model::{Action, Key};
use super::select::step;
use super::{help, style};

const COMMANDS: &[&str] = &["pbcopy", "wl-copy", "xclip", "clip.exe"];

fn executable(path: &Path) -> bool {
    let Ok(meta) = path.metadata() else { return false };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    true
}

fn command(path: Option<&std::ffi::OsStr>, remote: bool) -> Option<(PathBuf, &'static str)> {
    if remote {
        return None;
    }
    let dirs: Vec<_> = std::env::split_paths(path?).collect();
    COMMANDS
        .iter()
        .find_map(|&name| dirs.iter().map(|dir| dir.join(name)).find(|path| executable(path)).map(|path| (path, name)))
}

pub async fn copy(text: String) -> Result<String> {
    let remote = ["SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"].iter().any(|name| std::env::var_os(name).is_some());
    let Some((path, name)) = command(std::env::var_os("PATH").as_deref(), remote) else {
        let mut out = io::stdout();
        out.write_all(super::osc52(&text).as_bytes()).context("sending OSC 52 clipboard request")?;
        out.flush().context("sending OSC 52 clipboard request")?;
        return Ok("OSC 52 clipboard request sent; terminal acknowledgement unavailable".to_owned());
    };
    copy_command(&path, name, &text).await?;
    Ok(format!("copied {} characters via {name}", text.chars().count()))
}

async fn copy_command(path: &Path, name: &str, text: &str) -> Result<()> {
    let mut command = tokio::process::Command::new(path);
    if name == "xclip" {
        command.args(["-selection", "clipboard"]);
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("starting clipboard command {name}"))?;
    let mut input = child.stdin.take().context("opening clipboard command stdin")?;
    let mut errors = child.stderr.take().context("opening clipboard command stderr")?;
    let write = async {
        let result = input.write_all(text.as_bytes()).await;
        drop(input);
        result
    };
    let stderr = async {
        let mut bytes = Vec::new();
        (&mut errors).take(4096).read_to_end(&mut bytes).await?;
        tokio::io::copy(&mut errors, &mut tokio::io::sink()).await?;
        Ok::<_, io::Error>(bytes)
    };
    // Drain stderr while writing; a failed command must still be reaped after a broken pipe.
    let (written, output, stderr) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async { tokio::join!(write, child.wait(), stderr) })
            .await
            .with_context(|| format!("clipboard command {name} timed out"))?;
    let output = output.with_context(|| format!("waiting for clipboard command {name}"))?;
    let stderr = stderr.with_context(|| format!("reading clipboard command {name} stderr"))?;
    if !output.success() {
        let stderr: String =
            String::from_utf8_lossy(&stderr).chars().filter(|c| !c.is_control() || *c == '\n').take(240).collect();
        bail!("clipboard command {name} failed ({output}): {}", stderr.trim());
    }
    written.with_context(|| format!("writing clipboard command {name} stdin"))?;
    Ok(())
}

#[derive(Debug)]
pub struct Picker {
    choices: Vec<(String, String)>,
    pub selected: usize,
}

impl Picker {
    pub fn new(text: &str) -> Option<Self> {
        let mut choices = vec![("Whole message".to_owned(), text.to_owned())];
        let mut code = None;
        let mut language = String::new();
        for event in Parser::new(text) {
            match event {
                Event::Start(Tag::CodeBlock(kind)) => {
                    language = match kind {
                        pulldown_cmark::CodeBlockKind::Fenced(info) => info.to_string(),
                        pulldown_cmark::CodeBlockKind::Indented => String::new(),
                    };
                    code = Some(String::new());
                }
                Event::Text(text) if code.is_some() => code.as_mut().expect("a code block").push_str(&text),
                Event::End(TagEnd::CodeBlock) => {
                    let text = code.take().unwrap_or_default();
                    let label =
                        format!("Code block {} {} ({} characters)", choices.len(), language, text.chars().count());
                    choices.push((label, text));
                }
                _ => {}
            }
        }
        (choices.len() > 1).then_some(Self { choices, selected: 0 })
    }

    pub fn key(&mut self, key: Key) -> Option<Action> {
        match key {
            Key::Enter => return Some(Action::Copy(self.choices[self.selected].1.clone())),
            _ => {
                if let Some(to) = step(key, self.selected, None) {
                    self.selected = to.min(self.choices.len() - 1);
                }
            }
        }
        None
    }

    pub fn draw(&self, frame: &mut Frame, mouse: &super::mouse::Regions) {
        let area = help::centered(frame.area(), 76, u16::try_from(self.choices.len() + 3).unwrap_or(u16::MAX));
        let block = style::block(" Copy: Up/Down select; Enter copy; Esc close ".to_owned(), true);
        let inner = block.inner(area);
        frame.render_widget(ratatui::widgets::Clear, area);
        frame.render_widget(block, area);
        let top = (self.selected + 1).saturating_sub(usize::from(inner.height));
        for (row, (label, _)) in self.choices.iter().enumerate().skip(top).take(usize::from(inner.height)) {
            let line = Line::raw(label.clone());
            let line = if row == self.selected { line.patch_style(style::highlight(true)) } else { line };
            let area = Rect { y: inner.y + (row - top) as u16, height: 1, ..inner };
            mouse.add(area, super::mouse::Target::Copy(row));
            frame.render_widget(Paragraph::new(line), area);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_choices_keep_whole_message_first_and_preserve_code() {
        let text = "Before\n```rust\nlet n = 1;\n```\nAfter\n\n    indented\n";
        let mut picker = Picker::new(text).unwrap();
        assert_eq!(picker.key(Key::Enter), Some(Action::Copy(text.to_owned())));
        picker.key(Key::Down);
        assert_eq!(picker.key(Key::Enter), Some(Action::Copy("let n = 1;\n".to_owned())));
        picker.key(Key::Down);
        assert_eq!(picker.key(Key::Enter), Some(Action::Copy("indented\n".to_owned())));
        assert!(Picker::new("plain `inline`").is_none());
    }

    #[test]
    fn copying_code_opens_a_modal_choice_without_losing_the_bar_draft() {
        let mut model = super::super::test_support::model();
        let id = ants_proto::ThreadId::generate();
        let text = "Before\n```\nexample\n```\nAfter";
        let entry = crate::test_support::live::message(1, &id, "dev-a", "alice", text);
        model.stored(vec![entry.clone()], crate::test_support::live::at(1));
        let mut page = super::super::thread::Page::new(id.clone());
        page.history.loaded(&id, Ok(vec![entry])).unwrap();
        model.pages.push(super::super::model::Page::Thread(page));
        let now = crate::test_support::live::at(1);
        model.key(Key::Up, now);
        model.bar.set_text("unsent draft");
        assert_eq!(model.key(Key::Char('y'), now), None);
        assert!(model.copying().is_some());
        model.paste("not added", now);
        assert_eq!(model.bar.text(), "unsent draft");
        model.key(Key::Down, now);
        assert_eq!(model.key(Key::Enter, now), Some(Action::Copy("example\n".to_owned())));
        assert!(model.copying().is_none());
        model.key(Key::Char('y'), now);
        model.key(Key::Interrupt, now);
        assert!(model.copying().is_none());
        assert_eq!(model.bar.text(), "unsent draft");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_errors_and_broken_stdin_are_reported() {
        let error = copy_command(Path::new("/usr/bin/false"), "false", "text").await.unwrap_err();
        assert!(error.to_string().contains("failed"), "{error:#}");
        let error = copy_command(Path::new("/usr/bin/true"), "true", &"x".repeat(1024 * 1024)).await.unwrap_err();
        assert!(format!("{error:#}").contains("stdin"), "{error:#}");
        let error = copy_command(Path::new("/no-such-clipboard-program"), "missing", "text").await.unwrap_err();
        assert!(error.to_string().contains("starting"), "{error:#}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_copy_supplies_exact_stdin_and_clipboard_selection_and_checks_completion() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = ants_test_support::scratch_dir("clipboard-stdin");
        let path = dir.join("xclip");
        std::fs::write(&path, "#!/bin/sh\n/bin/cat > \"$0.input\"\nprintf '%s' \"$*\" > \"$0.args\"\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        let text = "first\nsecond \u{1f41c}\n";
        copy_command(&path, "xclip", text).await.unwrap();
        assert_eq!(std::fs::read_to_string(dir.join("xclip.input")).unwrap(), text);
        assert_eq!(std::fs::read_to_string(dir.join("xclip.args")).unwrap(), "-selection clipboard");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn first_executable_wins_and_ssh_forces_osc52() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = ants_test_support::scratch_dir("clipboard");
        for name in ["wl-copy", "pbcopy"] {
            let path = dir.join(name);
            std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        assert_eq!(command(Some(dir.as_os_str()), false).unwrap().1, "pbcopy");
        assert!(command(Some(dir.as_os_str()), true).is_none());
        assert!(command(None, false).is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
