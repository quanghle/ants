//! The text of `ants ask` and `ants post`: the words on the command line, a file, stdin or an editor.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context as _, Result, bail};

#[derive(clap::Args, Debug, Default)]
pub struct Input {
    /// Read the message from PATH (`-` for stdin) instead of the command line, keeping its lines as they are but for
    /// the trailing line breaks.
    #[arg(long, value_name = "PATH", value_hint = clap::ValueHint::FilePath, conflicts_with = "edit")]
    pub file: Option<PathBuf>,
    /// Write the message in an editor: the command line in $VISUAL, else $EDITOR, else vi (notepad on Windows), run
    /// through the shell with a new file's path appended. Leaving the file empty sends nothing.
    #[arg(long)]
    pub edit: bool,
}

impl Input {
    /// The message: the words joined by spaces, else the text of --file or --edit, which take no words.
    pub fn text(&self, words: Vec<String>) -> Result<String> {
        self.compose(words, std::io::stdin(), &editor(std::env::var("VISUAL").ok(), std::env::var("EDITOR").ok()))
    }

    fn compose(&self, words: Vec<String>, stdin: impl Read, editor: &str) -> Result<String> {
        if !words.is_empty() && (self.file.is_some() || self.edit) {
            let flag = if self.edit { "--edit" } else { "--file" };
            bail!("give the message either as words or with {flag}, not both");
        }
        let text = match &self.file {
            Some(path) if path == Path::new("-") => read(stdin, "stdin")?,
            Some(path) => {
                let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
                read(file, &path.display().to_string())?
            }
            None if self.edit => edit(editor, "")?,
            None => words.join(" "),
        };
        if text.trim().is_empty() {
            match (&self.file, self.edit) {
                (Some(path), _) if path == Path::new("-") => bail!("stdin is empty: nothing sent"),
                (Some(path), _) => bail!("{} is empty: nothing sent", path.display()),
                (None, true) => bail!("the message is empty: nothing sent"),
                (None, false) => {
                    bail!("nothing to send: give the message as words, with --file PATH (- for stdin) or --edit")
                }
            }
        }
        Ok(text)
    }
}

/// The text in `from`, without a leading byte order mark or trailing line breaks.
fn read(mut from: impl Read, name: &str) -> Result<String> {
    let mut bytes = Vec::new();
    from.read_to_end(&mut bytes).with_context(|| format!("reading {name}"))?;
    let text = String::from_utf8(bytes).map_err(|_| anyhow::anyhow!("{name} is not UTF-8 text"))?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    Ok(text.trim_end_matches(['\r', '\n']).to_owned())
}

/// The editor command line: $VISUAL, else $EDITOR, else the platform's default, as git picks it.
fn editor(visual: Option<String>, editor: Option<String>) -> String {
    let default = if cfg!(windows) { "notepad" } else { "vi" };
    [visual, editor].into_iter().flatten().find(|e| !e.trim().is_empty()).unwrap_or_else(|| default.to_owned())
}

/// A new file only its owner may read, removed when dropped.
struct Draft(PathBuf);

impl Draft {
    fn create() -> Result<Self> {
        let dir = std::env::temp_dir();
        for attempt in 0u32.. {
            let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
            let path = dir.join(format!("ants-message-{}-{}-{attempt}.md", std::process::id(), nanos.as_nanos()));
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
            match options.open(&path) {
                Ok(_) => return Ok(Self(path)),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && attempt < 10 => continue,
                Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
            }
        }
        unreachable!()
    }
}

impl Drop for Draft {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Run the user's editor ($VISUAL, else $EDITOR, else the platform's default) on a new file holding `initial` and
/// return what it saved there.
pub fn edit_text(initial: &str) -> Result<String> {
    edit(&editor(std::env::var("VISUAL").ok(), std::env::var("EDITOR").ok()), initial)
}

/// Show `text` in the pager `PAGER` names, else in the editor, on a file removed once it exits.
pub fn page_text(text: &str) -> Result<()> {
    let pager = std::env::var("PAGER").ok().filter(|p| !p.trim().is_empty());
    let pager = pager.unwrap_or_else(|| editor(std::env::var("VISUAL").ok(), std::env::var("EDITOR").ok()));
    let draft = Draft::create()?;
    std::fs::write(&draft.0, text).with_context(|| format!("writing {}", draft.0.display()))?;
    let status = shell(&pager, &draft.0).status().with_context(|| format!("running `{pager}`"))?;
    if !status.success() {
        bail!("`{pager}` failed ({status}): set PAGER to a pager's command line");
    }
    Ok(())
}

/// Run the `editor` command line on a new file holding `initial` and return what it saved there.
fn edit(editor: &str, initial: &str) -> Result<String> {
    let draft = Draft::create()?;
    if !initial.is_empty() {
        std::fs::write(&draft.0, initial).with_context(|| format!("writing {}", draft.0.display()))?;
    }
    let status = shell(editor, &draft.0).status().with_context(|| format!("running the editor `{editor}`"))?;
    // The shell's status for a command it cannot find.
    let not_found = if cfg!(windows) { 9009 } else { 127 };
    match status.code() {
        Some(0) => {}
        Some(code) if code == not_found => {
            bail!("editor `{editor}` not found: set VISUAL or EDITOR to an editor's command line; nothing sent")
        }
        _ => bail!("editor `{editor}` failed ({status}): nothing sent"),
    }
    let file = std::fs::File::open(&draft.0).with_context(|| format!("opening {}", draft.0.display()))?;
    read(file, "the message")
}

/// The command line `editor` with `path` appended, run as git runs an editor: by `sh -c` (by `cmd /S /C` on Windows).
#[cfg(not(windows))]
fn shell(editor: &str, path: &Path) -> Command {
    let mut command = Command::new("sh");
    command.arg("-c").arg(format!("{editor} \"$@\"")).arg(editor).arg(path);
    command
}

#[cfg(windows)]
fn shell(editor: &str, path: &Path) -> Command {
    use std::os::windows::process::CommandExt as _;
    let mut command = Command::new("cmd");
    command.raw_arg(format!("/S /C \"{editor} \"{}\"\"", path.display()));
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(s: &str) -> Vec<String> {
        s.split(' ').map(str::to_owned).collect()
    }

    fn compose(input: &Input, words: Vec<String>, stdin: &str) -> Result<String> {
        input.compose(words, stdin.as_bytes(), "ants-no-such-editor")
    }

    #[test]
    fn the_message_comes_from_words_a_file_or_stdin() {
        let dir = ants_test_support::scratch_dir("compose");
        let file = dir.join("message.md");
        std::fs::write(&file, "\u{feff}first\n\n  indented\r\nlast \n\n").unwrap();
        let words_only = Input::default();
        assert_eq!(compose(&words_only, words("check the build"), "ignored").unwrap(), "check the build");
        let from_file = Input { file: Some(file.clone()), edit: false };
        assert_eq!(compose(&from_file, vec![], "ignored").unwrap(), "first\n\n  indented\r\nlast ");
        let from_stdin = Input { file: Some("-".into()), edit: false };
        assert_eq!(compose(&from_stdin, vec![], "one\n  two\n").unwrap(), "one\n  two");

        let err =
            |input: &Input, words: Vec<String>, stdin: &str| compose(input, words, stdin).unwrap_err().to_string();
        assert!(err(&from_file, words("more"), "").contains("either as words or with --file"));
        assert!(err(&Input { file: None, edit: true }, words("more"), "").contains("either as words or with --edit"));
        assert!(err(&words_only, vec![], "").starts_with("nothing to send"));
        assert!(err(&words_only, words(" "), "").starts_with("nothing to send"));
        assert_eq!(err(&from_stdin, vec![], " \n\n"), "stdin is empty: nothing sent");
        std::fs::write(&file, "\n").unwrap();
        assert!(err(&from_file, vec![], "").ends_with("message.md is empty: nothing sent"));
        std::fs::write(&file, [0xff, 0xfe, 0xfd]).unwrap();
        assert!(err(&from_file, vec![], "").ends_with("message.md is not UTF-8 text"));
        let missing = Input { file: Some(dir.join("missing.md")), edit: false };
        assert!(err(&missing, vec![], "").contains("missing.md"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn visual_wins_over_editor_over_the_default() {
        let some = |s: &str| Some(s.to_owned());
        assert_eq!(editor(some("code --wait"), some("vim")), "code --wait");
        assert_eq!(editor(some(""), some("vim")), "vim");
        assert_eq!(editor(None, None), if cfg!(windows) { "notepad" } else { "vi" });
    }

    #[cfg(unix)]
    #[test]
    fn an_editor_writes_the_message_in_a_private_file_removed_afterwards() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = ants_test_support::scratch_dir("compose-editor");
        let script = dir.join("fake editor");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf %s \"$2\" > \"$(dirname \"$0\")/path\"\nls -l \"$2\" | cut -c1-10 > \"$(dirname \"$0\")/mode\"\n\
             test \"$1\" = --wait || exit 9\ncase $(basename \"$2\") in ants-message-*.md) ;; *) exit 8 ;; esac\n\
             if [ -n \"$FAKE_TEXT\" ]; then printf \"$FAKE_TEXT\" > \"$2\"; fi\nexit ${FAKE_EXIT:-0}\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        // Quoted as a command line: the path holds a space.
        let line = |text: &str, exit: u8| format!("FAKE_TEXT='{text}' FAKE_EXIT={exit} '{}' --wait", script.display());
        let input = Input { file: None, edit: true };
        let edit = |text: &str, exit: u8| input.compose(vec![], "".as_bytes(), &line(text, exit));

        assert_eq!(edit("line one\\n\\n  line two\\n", 0).unwrap(), "line one\n\n  line two");
        let path = std::fs::read_to_string(dir.join("path")).unwrap();
        assert!(!Path::new(&path).exists(), "{path} left behind");
        assert_eq!(std::fs::read_to_string(dir.join("mode")).unwrap(), "-rw-------\n");

        assert_eq!(edit("", 0).unwrap_err().to_string(), "the message is empty: nothing sent");
        assert!(edit("  \\n", 0).is_err());
        let failed = edit("written anyway", 3).unwrap_err().to_string();
        assert!(failed.contains("failed (exit status: 3): nothing sent"), "{failed}");
        let missing = input.compose(vec![], "".as_bytes(), "ants-no-such-editor --wait").unwrap_err().to_string();
        assert!(missing.starts_with("editor `ants-no-such-editor --wait` not found"), "{missing}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
