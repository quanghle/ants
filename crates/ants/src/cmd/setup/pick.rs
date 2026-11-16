//! The harness `ants setup` writes into a new config file: the built-in harnesses installed here, found on `PATH` as
//! the daemon finds their commands, none of them started.

use std::ffi::OsStr;
use std::fmt::Write as _;
use std::io::{BufRead, Write};

use ants_daemon::harness::{self, Harnesses};
use anyhow::{Result, bail};

/// A built-in harness, as `ants setup` offers it.
struct Builtin {
    name: &'static str,
    /// The provider's own CLI, when the ACP adapter is a separate program, to tell that it alone is missing.
    cli: Option<&'static str>,
    install: &'static str,
}

/// The built-in harnesses, none of them preferred.
const BUILTINS: [Builtin; 4] = [
    Builtin { name: "copilot", cli: None, install: "npm install -g @github/copilot" },
    Builtin { name: "claude", cli: Some("claude"), install: "npm install -g @agentclientprotocol/claude-agent-acp" },
    Builtin { name: "codex", cli: Some("codex"), install: "npm install -g @agentclientprotocol/codex-acp" },
    Builtin { name: "antigravity", cli: None, install: "curl -fsSL https://antigravity.google/cli/install.sh | bash" },
];

/// Whether a built-in harness is installed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found {
    Installed,
    /// The provider's CLI is, its ACP adapter, the harness's command, is not.
    AdapterMissing {
        cli: &'static str,
        adapter: String,
    },
    Missing,
}

/// A built-in harness and whether it is installed.
#[derive(Debug, Clone)]
pub struct Detected {
    pub name: &'static str,
    pub found: Found,
    install: &'static str,
    sign_in: String,
}

impl Detected {
    /// What is installed of it, if anything, in parentheses.
    fn note(&self) -> String {
        match &self.found {
            Found::Installed => " (installed)".to_owned(),
            Found::AdapterMissing { cli, adapter } => format!(" ({cli} found, ACP adapter {adapter} missing)"),
            Found::Missing => String::new(),
        }
    }
}

/// Each built-in harness, and whether its command is in a directory of `path`.
pub fn detect(path: &OsStr) -> Vec<Detected> {
    let harnesses = Harnesses::builtin();
    BUILTINS
        .iter()
        .map(|builtin| {
            let harness = harnesses.get(builtin.name).expect("a built-in harness");
            let command = harness.command();
            let found = if harness::on_path(command.as_os_str(), path).is_some() {
                Found::Installed
            } else {
                match builtin.cli.filter(|cli| harness::on_path(OsStr::new(cli), path).is_some()) {
                    Some(cli) => Found::AdapterMissing { cli, adapter: command.display().to_string() },
                    None => Found::Missing,
                }
            };
            let sign_in = harness.sign_in().unwrap_or("none needed").to_owned();
            Detected { name: builtin.name, found, install: builtin.install, sign_in }
        })
        .collect()
}

/// The names of the harnesses installed.
pub fn installed(detected: &[Detected]) -> Vec<&'static str> {
    detected.iter().filter(|d| d.found == Found::Installed).map(|d| d.name).collect()
}

/// Each harness with its install and sign-in commands, one per line, then where custom ones are described.
pub fn listing(detected: &[Detected]) -> String {
    let mut text = String::new();
    for d in detected {
        let _ = writeln!(text, "  {:<12} install: {}; sign in: {}{}", d.name, d.install, d.sign_in, d.note());
    }
    text.push_str(
        "  another ACP agent: define [harnesses.NAME] in the config file or in harnesses.d \
         (docs/config.md#harnesses), then pass --harness NAME",
    );
    text
}

/// The harness to write into a new config file, and how it was picked: the only one installed, else, when `prompt`
/// gives the human's input and output, the one asked for.
pub fn choose(detected: &[Detected], prompt: Option<(&mut dyn BufRead, &mut dyn Write)>) -> Result<(String, String)> {
    match (installed(detected).as_slice(), prompt) {
        ([name], _) => Ok(((*name).to_owned(), format!("{name}, the only built-in harness installed here"))),
        ([], _) => bail!(
            "no built-in harness is installed here; install one and sign it in, then run `ants setup` again, or pass \
             --harness NAME:\n{}",
            listing(detected)
        ),
        (names, None) => {
            bail!("{} are installed here; pass --harness NAME to pick one:\n{}", names.join(", "), listing(detected))
        }
        (_, Some((input, output))) => {
            let name = ask(detected, input, output)?;
            let how = format!("{name}, as chosen");
            Ok((name, how))
        }
    }
}

/// Ask for a harness by number or name until one is given; fails when input ends.
fn ask(detected: &[Detected], input: &mut dyn BufRead, output: &mut dyn Write) -> Result<String> {
    writeln!(output, "Harness for the config file:")?;
    for (n, d) in detected.iter().enumerate() {
        writeln!(output, "  {}. {}{}", n + 1, d.name, d.note())?;
    }
    loop {
        write!(output, "Number or name [1-{}]: ", detected.len())?;
        output.flush()?;
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            bail!("no harness chosen; pass --harness NAME");
        }
        let answer = line.trim();
        let picked = match answer.parse::<usize>() {
            Ok(n) => n.checked_sub(1).and_then(|i| detected.get(i)),
            Err(_) => detected.iter().find(|d| d.name == answer),
        };
        match picked {
            Some(d) => return Ok(d.name.to_owned()),
            None if answer.is_empty() => {}
            None => writeln!(output, "{answer}: not one of those listed")?,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use super::*;

    /// A `PATH` of one directory holding empty files named `commands`.
    fn path_with(commands: &[&str]) -> (OsString, PathBuf) {
        let dir = ants_test_support::scratch_dir("setup-pick");
        for command in commands {
            let file = if cfg!(windows) { format!("{command}.exe") } else { (*command).to_owned() };
            std::fs::write(dir.join(file), "").unwrap();
        }
        (dir.clone().into_os_string(), dir)
    }

    #[test]
    fn harnesses_are_found_by_their_commands_and_a_cli_without_its_adapter_is_told() {
        let (path, dir) = path_with(&["copilot", "claude", "codex", "codex-acp"]);
        let detected = detect(&path);
        let names: Vec<&str> = detected.iter().map(|d| d.name).collect();
        assert_eq!(names, ["copilot", "claude", "codex", "antigravity"]);
        assert_eq!(installed(&detected), ["copilot", "codex"]);
        let adapter = Found::AdapterMissing { cli: "claude", adapter: "claude-agent-acp".to_owned() };
        assert_eq!(detected[1].found, adapter);
        assert_eq!(detected[3].found, Found::Missing);
        assert!(listing(&detected).contains("claude found, ACP adapter claude-agent-acp missing"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn the_only_harness_installed_is_picked_without_asking() {
        let (path, dir) = path_with(&["agy"]);
        let detected = detect(&path);
        let mut output = Vec::new();
        let (name, how) = choose(&detected, Some((&mut &b""[..], &mut output))).unwrap();
        assert_eq!(name, "antigravity");
        assert!(how.contains("the only built-in harness installed here"), "{how}");
        assert!(output.is_empty(), "nothing asked");
        assert_eq!(choose(&detected, None).unwrap().0, "antigravity", "nor without a terminal");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn with_none_installed_each_is_listed_with_its_install_and_sign_in_commands() {
        let (path, dir) = path_with(&[]);
        let detected = detect(&path);
        let mut output = Vec::new();
        let asked = choose(&detected, Some((&mut &b"1\n"[..], &mut output))).unwrap_err();
        for e in [asked, choose(&detected, None).unwrap_err()] {
            let e = e.to_string();
            assert!(e.starts_with("no built-in harness is installed here"), "{e}");
            for line in [
                "copilot      install: npm install -g @github/copilot; sign in: copilot login",
                "codex        install: npm install -g @agentclientprotocol/codex-acp; sign in: codex-acp login",
                "antigravity  install: curl -fsSL https://antigravity.google/cli/install.sh | bash; sign in: agy",
                "docs/config.md#harnesses",
            ] {
                assert!(e.contains(line), "{line} in {e}");
            }
        }
        assert!(output.is_empty(), "nothing asked");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn with_several_installed_a_terminal_is_asked_and_anything_else_fails() {
        let (path, dir) = path_with(&["copilot", "claude-agent-acp"]);
        let detected = detect(&path);
        let e = choose(&detected, None).unwrap_err().to_string();
        assert!(e.starts_with("copilot, claude are installed here; pass --harness NAME"), "{e}");

        let mut output = Vec::new();
        let chosen = choose(&detected, Some((&mut &b"7\nnope\n\n2\n"[..], &mut output))).unwrap();
        assert_eq!(chosen, ("claude".to_owned(), "claude, as chosen".to_owned()));
        let asked = String::from_utf8(output).unwrap();
        assert!(asked.contains("  1. copilot (installed)\n  2. claude (installed)\n  3. codex\n"), "{asked}");
        assert!(asked.contains("7: not one of those listed") && asked.contains("nope: not one"), "{asked}");

        let mut output = Vec::new();
        let by_name = choose(&detected, Some((&mut &b"codex\n"[..], &mut output))).unwrap();
        assert_eq!(by_name.0, "codex", "by name, installed or not");
        assert!(choose(&detected, Some((&mut &b""[..], &mut output))).is_err(), "input ended");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
